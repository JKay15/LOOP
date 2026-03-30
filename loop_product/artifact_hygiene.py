"""Shared runtime artifact hygiene helpers."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
from pathlib import Path
from typing import Any, Iterable

_NESTED_RUNTIME_HEAVY_DIR_NAMES = frozenset({".lake", ".git", ".venv", ".uv-cache"})
_ARTIFACT_ROOT_RUNTIME_HEAVY_DIR_NAMES = frozenset({"build", "_lake_build"})
_AUTHORITATIVE_RUNTIME_HEAVY_TREE_DIR_NAMES = frozenset({".lake", "build", "_lake_build"})
_PUBLICATION_STAGING_PREFIX = ".primary_artifact.publish."
_HEAVY_OBJECT_STORE_DIRNAME = "heavy_objects"


def _runtime_heavy_name_ignored(*, artifact_root: Path, current_path: Path, name: str) -> bool:
    normalized_name = str(name or "").strip()
    if normalized_name in _NESTED_RUNTIME_HEAVY_DIR_NAMES:
        return True
    if current_path == artifact_root and normalized_name in _ARTIFACT_ROOT_RUNTIME_HEAVY_DIR_NAMES:
        return True
    return False


def iter_runtime_heavy_tree_paths(artifact_root: Path) -> list[Path]:
    matches: list[Path] = []
    for current_root, dirnames, _filenames in os.walk(artifact_root):
        current_path = Path(current_root)
        keep_dirnames: list[str] = []
        for dirname in dirnames:
            candidate = current_path / dirname
            if dirname in _NESTED_RUNTIME_HEAVY_DIR_NAMES:
                matches.append(candidate)
                continue
            if current_path == artifact_root and dirname in _ARTIFACT_ROOT_RUNTIME_HEAVY_DIR_NAMES:
                matches.append(candidate)
                continue
            keep_dirnames.append(dirname)
        dirnames[:] = keep_dirnames
    # Preserve symlink identity so externalized runtime-owned mounts do not
    # collapse into their target directories and get misclassified as local
    # materialized heavy trees.
    return sorted({path.absolute() for path in matches}, key=lambda item: (len(item.parts), str(item)))


def iter_materialized_runtime_heavy_tree_paths(artifact_root: Path) -> list[Path]:
    return [path for path in iter_runtime_heavy_tree_paths(artifact_root) if path.exists() and not path.is_symlink()]


def remove_runtime_heavy_tree(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    shutil.rmtree(path)


def canonicalize_directory_artifact_heavy_trees(artifact_root: Path) -> list[str]:
    removed: list[str] = []
    for heavy_path in iter_runtime_heavy_tree_paths(artifact_root):
        if not heavy_path.exists():
            continue
        remove_runtime_heavy_tree(heavy_path)
        removed.append(str(heavy_path))
    remaining = [str(path) for path in iter_runtime_heavy_tree_paths(artifact_root) if path.exists()]
    if remaining:
        raise ValueError(
            "directory artifact runtime canonicalization could not prune runtime-owned heavy trees: "
            + ", ".join(remaining)
        )
    return removed


def artifact_fingerprint(path: Path, *, ignore_runtime_heavy: bool = False) -> dict[str, str]:
    resolved = path.resolve()
    if not resolved.exists():
        raise ValueError(f"artifact fingerprint target does not exist: {resolved}")
    if resolved.is_file():
        digest = hashlib.sha256()
        with resolved.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return {
            "artifact_kind": "file",
            "fingerprint": digest.hexdigest(),
        }
    if not resolved.is_dir():
        raise ValueError(f"artifact fingerprint target must be a file or directory: {resolved}")

    digest = hashlib.sha256()
    for current_root, dirnames, filenames in os.walk(resolved):
        current_path = Path(current_root)
        keep_dirnames: list[str] = []
        for dirname in dirnames:
            if ignore_runtime_heavy and _runtime_heavy_name_ignored(
                artifact_root=resolved,
                current_path=current_path,
                name=dirname,
            ):
                continue
            keep_dirnames.append(dirname)
        dirnames[:] = sorted(keep_dirnames)
        for filename in sorted(filenames):
            file_path = current_path / filename
            if ignore_runtime_heavy and _runtime_heavy_name_ignored(
                artifact_root=resolved,
                current_path=current_path,
                name=filename,
            ):
                continue
            relative = file_path.relative_to(resolved).as_posix()
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            try:
                with file_path.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
            except FileNotFoundError:
                continue
            digest.update(b"\0")
    return {
        "artifact_kind": "directory",
        "fingerprint": digest.hexdigest(),
    }


def artifact_byte_size(path: Path, *, ignore_runtime_heavy: bool = False) -> int:
    resolved = path.resolve()
    if not resolved.exists():
        raise ValueError(f"artifact byte-size target does not exist: {resolved}")
    if resolved.is_file():
        return int(resolved.stat().st_size)
    if not resolved.is_dir():
        raise ValueError(f"artifact byte-size target must be a file or directory: {resolved}")

    total = 0
    for current_root, dirnames, filenames in os.walk(resolved):
        current_path = Path(current_root)
        keep_dirnames: list[str] = []
        for dirname in dirnames:
            if ignore_runtime_heavy and _runtime_heavy_name_ignored(
                artifact_root=resolved,
                current_path=current_path,
                name=dirname,
            ):
                continue
            keep_dirnames.append(dirname)
        dirnames[:] = sorted(keep_dirnames)
        for filename in sorted(filenames):
            if ignore_runtime_heavy and _runtime_heavy_name_ignored(
                artifact_root=resolved,
                current_path=current_path,
                name=filename,
            ):
                continue
            try:
                total += int((current_path / filename).stat().st_size)
            except FileNotFoundError:
                continue
    return total


def heavy_object_store_root(repo_root: Path) -> Path:
    return (Path(repo_root).resolve() / ".loop" / _HEAVY_OBJECT_STORE_DIRNAME).resolve()


def _normalized_heavy_object_kind_component(object_kind: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", str(object_kind or "").strip()).strip("._-") or "object"


def path_within_heavy_object_store(*, repo_root: Path, candidate: Path) -> bool:
    return _path_within(heavy_object_store_root(repo_root), candidate)


def canonical_heavy_object_store_ref(
    *,
    repo_root: Path,
    object_kind: str,
    object_id: str,
    source_ref: Path,
) -> Path:
    normalized_object_id = str(object_id or "").strip()
    if not normalized_object_id:
        raise ValueError("object_id is required")
    resolved_source = Path(source_ref).expanduser().resolve()
    suffix = "".join(resolved_source.suffixes) if resolved_source.is_file() else ""
    leaf = "tree" if resolved_source.is_dir() else f"object{suffix}"
    digest = normalized_object_id.removeprefix("sha256:").strip() or normalized_object_id
    return (
        heavy_object_store_root(repo_root)
        / _normalized_heavy_object_kind_component(object_kind)
        / digest
        / leaf
    ).resolve()


def _remove_existing_path(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or path.is_file():
        path.unlink()
        return
    shutil.rmtree(path)


def materialize_canonical_heavy_object_store_ref(
    *,
    repo_root: Path,
    object_kind: str,
    object_id: str,
    source_ref: Path,
) -> Path:
    normalized_object_id = str(object_id or "").strip()
    if not normalized_object_id:
        raise ValueError("object_id is required")
    resolved_source = Path(source_ref).expanduser().resolve()
    if not resolved_source.exists():
        raise ValueError(f"source heavy object does not exist: {resolved_source}")
    expected_ref = canonical_heavy_object_store_ref(
        repo_root=repo_root,
        object_kind=object_kind,
        object_id=normalized_object_id,
        source_ref=resolved_source,
    )
    expected_ref.parent.mkdir(parents=True, exist_ok=True)

    if expected_ref.exists():
        try:
            existing_identity = heavy_object_identity(expected_ref, object_kind=object_kind)
        except Exception:
            _remove_existing_path(expected_ref)
        else:
            if str(existing_identity.get("object_id") or "").strip() == normalized_object_id:
                return expected_ref
            _remove_existing_path(expected_ref)

    if resolved_source == expected_ref:
        materialized_ref = expected_ref
    elif resolved_source.is_file():
        shutil.copy2(resolved_source, expected_ref)
        materialized_ref = expected_ref
    elif resolved_source.is_dir():
        shutil.copytree(resolved_source, expected_ref, symlinks=True)
        materialized_ref = expected_ref
    else:
        raise ValueError(f"unsupported heavy object source type: {resolved_source}")

    materialized_identity = heavy_object_identity(materialized_ref, object_kind=object_kind)
    if str(materialized_identity.get("object_id") or "").strip() != normalized_object_id:
        raise ValueError(
            "canonical heavy-object materialization changed object identity: "
            f"expected {normalized_object_id}, got {str(materialized_identity.get('object_id') or '').strip()}"
        )
    return materialized_ref.resolve()


def heavy_object_identity(path: Path, *, object_kind: str) -> dict[str, Any]:
    resolved = path.resolve()
    normalized_object_kind = str(object_kind or "").strip()
    ignore_runtime_heavy = normalized_object_kind == "publish_tree"
    fingerprint = artifact_fingerprint(resolved, ignore_runtime_heavy=ignore_runtime_heavy)
    return {
        "artifact_kind": str(fingerprint.get("artifact_kind") or ""),
        "fingerprint": str(fingerprint.get("fingerprint") or ""),
        "object_id": f"sha256:{str(fingerprint.get('fingerprint') or '')}",
        "byte_size": artifact_byte_size(resolved, ignore_runtime_heavy=ignore_runtime_heavy),
    }


def _is_publication_staging_dir(path: Path) -> bool:
    return path.is_dir() and path.name.startswith(_PUBLICATION_STAGING_PREFIX)


def iter_workspace_publication_staging_roots(workspace_root: Path) -> list[Path]:
    deliverables_root = workspace_root / "deliverables"
    if not deliverables_root.exists():
        return []
    candidates: list[Path] = []
    for child in deliverables_root.iterdir():
        if _is_publication_staging_dir(child):
            candidates.append(child.resolve())
    return sorted({path for path in candidates}, key=lambda item: (len(item.parts), str(item)))


def iter_workspace_artifact_roots(workspace_root: Path) -> list[Path]:
    candidates: list[Path] = []
    explicit_roots = [
        workspace_root / "deliverables" / "primary_artifact",
    ]
    for candidate in explicit_roots:
        if candidate.exists() and candidate.is_dir():
            candidates.append(candidate.resolve())

    deliverables_root = workspace_root / "deliverables"
    if deliverables_root.exists():
        for child in deliverables_root.iterdir():
            if _is_publication_staging_dir(child):
                continue
            if child.is_dir() and "primary_artifact" in child.name:
                candidates.append(child.resolve())

    archive_root = workspace_root / "artifacts" / "archive_deliverables"
    if archive_root.exists():
        for child in archive_root.iterdir():
            if _is_publication_staging_dir(child):
                continue
            if child.is_dir() and "primary_artifact" in child.name:
                candidates.append(child.resolve())

    return sorted({path for path in candidates}, key=lambda item: (len(item.parts), str(item)))


def canonicalize_workspace_artifact_heavy_trees(
    workspace_root: Path,
    *,
    exclude_roots: Iterable[Path] = (),
) -> list[str]:
    removed: list[str] = []
    excluded = {Path(item).resolve() for item in exclude_roots}
    for artifact_root in iter_workspace_artifact_roots(workspace_root):
        if artifact_root.resolve() in excluded:
            continue
        removed.extend(canonicalize_directory_artifact_heavy_trees(artifact_root))
    return removed


def _path_within(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _workspace_publication_staging_holder_root(reference_ref: Path) -> Path | None:
    for candidate in [reference_ref, *reference_ref.parents]:
        if candidate.name != "primary_artifact":
            continue
        staging_root = candidate.parent
        if staging_root.name.startswith(_PUBLICATION_STAGING_PREFIX) and staging_root.parent.name == "deliverables":
            return staging_root.resolve()
    return None


def _evaluator_workspace_root(reference_ref: Path) -> Path | None:
    for candidate in [reference_ref, *reference_ref.parents]:
        if candidate.name != "workspace":
            continue
        parents = list(candidate.parents)
        if len(parents) < 5:
            continue
        if parents[1].name != ".loop":
            continue
        if parents[3].name != "evaluator_runs":
            continue
        if parents[4].name != "artifacts":
            continue
        return candidate.resolve()
    return None


def _workspace_artifact_holder_root(reference_ref: Path) -> Path | None:
    for candidate in [reference_ref, *reference_ref.parents]:
        if candidate.name != "primary_artifact":
            continue
        if candidate.parent.name != "deliverables":
            continue
        return candidate.resolve()
    return None


def _workspace_live_artifact_holder_root(reference_ref: Path) -> Path | None:
    for candidate in [reference_ref, *reference_ref.parents]:
        if candidate.name != ".tmp_primary_artifact":
            continue
        return candidate.resolve()
    return None


def _runtime_live_artifact_holder_root(reference_ref: Path) -> Path | None:
    for candidate in [reference_ref, *reference_ref.parents]:
        parents = list(candidate.parents)
        if len(parents) < 2:
            continue
        if parents[1].name != "live_artifacts":
            continue
        if ".loop" not in candidate.parts:
            continue
        return candidate.resolve()
    return None


def _classify_primary_artifact_reference_holder(reference_ref: Path, *, repo_root: Path) -> dict[str, str]:
    resolved_repo_root = repo_root.resolve()
    resolved_reference_ref = reference_ref.resolve()
    if not _path_within(resolved_repo_root, resolved_reference_ref):
        return {}
    evaluator_workspace_root = _evaluator_workspace_root(resolved_reference_ref)

    publication_staging_root = _workspace_publication_staging_holder_root(resolved_reference_ref)
    if publication_staging_root is not None and _path_within(resolved_repo_root, publication_staging_root):
        holder_kind = "workspace_publication_staging_root"
        if evaluator_workspace_root is not None and _path_within(evaluator_workspace_root, publication_staging_root):
            holder_kind = "evaluator_workspace_publication_staging_root"
        return {
            "reference_ref": str(resolved_reference_ref),
            "reference_holder_kind": holder_kind,
            "reference_holder_ref": str(publication_staging_root),
        }

    workspace_artifact_root = _workspace_artifact_holder_root(resolved_reference_ref)
    if workspace_artifact_root is not None and _path_within(resolved_repo_root, workspace_artifact_root):
        if evaluator_workspace_root is not None and _path_within(evaluator_workspace_root, workspace_artifact_root):
            holder_kind = "evaluator_workspace_artifact_root"
        else:
            holder_kind = "runtime_workspace_artifact_root" if ".loop" in workspace_artifact_root.parts else "workspace_artifact_root"
        return {
            "reference_ref": str(resolved_reference_ref),
            "reference_holder_kind": holder_kind,
            "reference_holder_ref": str(workspace_artifact_root),
        }

    workspace_live_artifact_root = _workspace_live_artifact_holder_root(resolved_reference_ref)
    if workspace_live_artifact_root is not None and _path_within(resolved_repo_root, workspace_live_artifact_root):
        holder_kind = "runtime_workspace_live_artifact_root"
        if evaluator_workspace_root is not None and _path_within(evaluator_workspace_root, workspace_live_artifact_root):
            holder_kind = "evaluator_workspace_live_artifact_root"
        return {
            "reference_ref": str(resolved_reference_ref),
            "reference_holder_kind": holder_kind,
            "reference_holder_ref": str(workspace_live_artifact_root),
        }

    runtime_live_artifact_root = _runtime_live_artifact_holder_root(resolved_reference_ref)
    if runtime_live_artifact_root is not None and _path_within(resolved_repo_root, runtime_live_artifact_root):
        return {
            "reference_ref": str(resolved_reference_ref),
            "reference_holder_kind": "runtime_live_artifact_root",
            "reference_holder_ref": str(runtime_live_artifact_root),
        }
    return {}


def classify_mathlib_pack_reference_holder(reference_ref: Path, *, repo_root: Path) -> dict[str, str]:
    resolved_repo_root = repo_root.resolve()
    resolved_reference_ref = reference_ref.resolve()
    if not resolved_reference_ref.exists() or not resolved_reference_ref.is_file():
        return {}
    if not _path_within(resolved_repo_root, resolved_reference_ref):
        return {}
    if path_within_heavy_object_store(repo_root=resolved_repo_root, candidate=resolved_reference_ref):
        return {}
    return _classify_primary_artifact_reference_holder(resolved_reference_ref, repo_root=resolved_repo_root)


def classify_publish_tree_reference_holder(reference_ref: Path, *, repo_root: Path) -> dict[str, str]:
    resolved_repo_root = repo_root.resolve()
    resolved_reference_ref = reference_ref.resolve()
    if not resolved_reference_ref.exists() or not resolved_reference_ref.is_dir():
        return {}
    if not _path_within(resolved_repo_root, resolved_reference_ref):
        return {}
    if path_within_heavy_object_store(repo_root=resolved_repo_root, candidate=resolved_reference_ref):
        return {}
    return _classify_primary_artifact_reference_holder(resolved_reference_ref, repo_root=resolved_repo_root)


def classify_runtime_heavy_tree_reference_holder(reference_ref: Path, *, repo_root: Path) -> dict[str, str]:
    resolved_repo_root = repo_root.resolve()
    resolved_reference_ref = reference_ref.resolve()
    if not resolved_reference_ref.exists() or not resolved_reference_ref.is_dir():
        return {}
    if resolved_reference_ref.name not in _AUTHORITATIVE_RUNTIME_HEAVY_TREE_DIR_NAMES:
        return {}
    if not _path_within(resolved_repo_root, resolved_reference_ref):
        return {}
    if path_within_heavy_object_store(repo_root=resolved_repo_root, candidate=resolved_reference_ref):
        return {}
    return _classify_primary_artifact_reference_holder(resolved_reference_ref, repo_root=resolved_repo_root)


def iter_publish_tree_candidate_roots(repo_root: Path) -> list[Path]:
    resolved_repo_root = repo_root.resolve()
    candidates: list[Path] = []
    seen: set[str] = set()
    patterns = ("primary_artifact", ".tmp_primary_artifact")
    for pattern in patterns:
        for path in sorted(resolved_repo_root.rglob(pattern)):
            try:
                resolved = path.expanduser().resolve()
            except OSError:
                continue
            if not resolved.is_dir():
                continue
            if path_within_heavy_object_store(repo_root=resolved_repo_root, candidate=resolved):
                continue
            if not classify_publish_tree_reference_holder(resolved, repo_root=resolved_repo_root):
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(resolved)
    return sorted(candidates, key=lambda item: (len(item.parts), str(item)))


def iter_runtime_heavy_tree_candidate_roots(repo_root: Path) -> list[Path]:
    resolved_repo_root = repo_root.resolve()
    candidates: list[Path] = []
    seen: set[str] = set()
    for artifact_root in iter_publish_tree_candidate_roots(resolved_repo_root):
        for candidate in iter_materialized_runtime_heavy_tree_paths(artifact_root):
            if candidate.name not in _AUTHORITATIVE_RUNTIME_HEAVY_TREE_DIR_NAMES:
                continue
            if not classify_runtime_heavy_tree_reference_holder(candidate, repo_root=resolved_repo_root):
                continue
            key = str(candidate.resolve())
            if key in seen:
                continue
            seen.add(key)
            candidates.append(candidate.resolve())
    return sorted(candidates, key=lambda item: (len(item.parts), str(item)))
