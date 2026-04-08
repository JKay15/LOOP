"""Runtime-owned Codex home helpers for the standalone LOOP repo."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Sequence

DEFAULT_CODEX_MODEL = "gpt-5.4"

_DEFAULT_SHARED_ENTRIES = (
    "auth.json",
    "version.json",
    "models_cache.json",
)

_MUTABLE_RUNTIME_DIRS = (
    "archived_sessions",
    "sessions",
    "shell_snapshots",
    ".tmp",
    "tmp",
    "sqlite",
    "log",
)


def host_codex_home() -> Path:
    return (Path.home() / ".codex").resolve()


def default_codex_model() -> str:
    return DEFAULT_CODEX_MODEL


def _link_or_copy_path(*, source: Path, target: Path) -> None:
    if target.exists() or target.is_symlink():
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.symlink_to(source, target_is_directory=source.is_dir())
        return
    except OSError:
        pass
    if source.is_dir():
        shutil.copytree(source, target, symlinks=True)
    else:
        shutil.copy2(source, target)


def prepare_runtime_owned_codex_home(
    *,
    runtime_home: Path,
    shared_entries: Sequence[str] = _DEFAULT_SHARED_ENTRIES,
) -> Path:
    runtime_home = Path(runtime_home).expanduser().resolve()
    runtime_home.mkdir(parents=True, exist_ok=True)
    for name in _MUTABLE_RUNTIME_DIRS:
        (runtime_home / name).mkdir(parents=True, exist_ok=True)
    host_home = host_codex_home()
    for name in shared_entries:
        source = host_home / str(name)
        if not source.exists():
            continue
        _link_or_copy_path(source=source, target=runtime_home / str(name))
    return runtime_home
