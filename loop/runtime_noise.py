"""Standalone workspace-noise policy for the router-based LOOP repo."""

from __future__ import annotations

from pathlib import Path

DEFAULT_RUNTIME_NOISE_ROOT_NAMES: tuple[str, ...] = (
    ".lake",
    ".venv",
    ".uv-cache",
    "build",
    "target",
    "node_modules",
    "_lake_build",
)


def runtime_noise_git_exclude_patterns() -> tuple[str, ...]:
    return tuple(f"{root}/**" for root in DEFAULT_RUNTIME_NOISE_ROOT_NAMES)


def is_runtime_noise_path(path: str | Path) -> bool:
    normalized = str(path or "").strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    if not normalized:
        return False
    first = normalized.split("/", 1)[0]
    return first in DEFAULT_RUNTIME_NOISE_ROOT_NAMES


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_shared_uv_cache_dir() -> Path:
    return (repo_root() / ".cache" / "uv").resolve()
