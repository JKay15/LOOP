"""Shared runtime artifact hygiene helpers."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

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


def iter_workspace_artifact_roots(workspace_root: Path) -> list[Path]:
    candidates: list[Path] = []
    explicit_roots = [
        workspace_root / ".tmp_primary_artifact",
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


def canonicalize_workspace_artifact_heavy_trees(workspace_root: Path) -> list[str]:
    removed: list[str] = []
    for artifact_root in iter_workspace_artifact_roots(workspace_root):
        removed.extend(canonicalize_directory_artifact_heavy_trees(artifact_root))
    return removed
