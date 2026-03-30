"""Trusted state-file write helpers."""

from __future__ import annotations

import json
import os
import stat
import uuid
from pathlib import Path
from typing import Any, Mapping

READ_ONLY_FILE_MODE = stat.S_IRUSR | stat.S_IRGRP | stat.S_IROTH


def write_text_read_only(path: Path, text: str, *, encoding: str = "utf-8") -> Path:
    """Atomically replace a file and leave it read-only on disk."""

    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(text, encoding=encoding)
        os.chmod(tmp, READ_ONLY_FILE_MODE)
        os.replace(tmp, path)
        os.chmod(path, READ_ONLY_FILE_MODE)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
    return path


def write_json_read_only(path: Path, payload: Mapping[str, Any] | list[Any]) -> Path:
    """Atomically write JSON and leave it read-only on disk."""

    return write_text_read_only(path, json.dumps(payload, indent=2, sort_keys=True) + "\n")


def is_user_writable(path: Path) -> bool:
    """Return whether the current file mode advertises user-write permissions."""

    return bool(Path(path).stat().st_mode & stat.S_IWUSR)
