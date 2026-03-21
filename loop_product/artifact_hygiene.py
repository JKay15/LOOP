"""Shared runtime artifact hygiene helpers."""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path
from typing import Iterable

_NESTED_RUNTIME_HEAVY_DIR_NAMES = frozenset({".lake", ".git", ".venv", ".uv-cache"})
_ARTIFACT_ROOT_RUNTIME_HEAVY_DIR_NAMES = frozenset({"build", "_lake_build"})


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
    return sorted({path.resolve() for path in matches}, key=lambda item: (len(item.parts), str(item)))


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
            if ignore_runtime_heavy:
                if dirname in _NESTED_RUNTIME_HEAVY_DIR_NAMES:
                    continue
                if current_path == resolved and dirname in _ARTIFACT_ROOT_RUNTIME_HEAVY_DIR_NAMES:
                    continue
            keep_dirnames.append(dirname)
        dirnames[:] = sorted(keep_dirnames)
        for filename in sorted(filenames):
            file_path = current_path / filename
            relative = file_path.relative_to(resolved).as_posix()
            digest.update(relative.encode("utf-8"))
            digest.update(b"\0")
            with file_path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
            digest.update(b"\0")
    return {
        "artifact_kind": "directory",
        "fingerprint": digest.hexdigest(),
    }


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
            if child.is_dir() and "primary_artifact" in child.name:
                candidates.append(child.resolve())

    archive_root = workspace_root / "artifacts" / "archive_deliverables"
    if archive_root.exists():
        for child in archive_root.iterdir():
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
