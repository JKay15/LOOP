"""Shared completion-record and workspace-fingerprint helpers for router actors."""

from __future__ import annotations

import hashlib
from pathlib import Path

from loop.events import ActorKind
from loop.evaluator_runtime import ROUTER_RUNTIME_ROOT, actor_completion_record_path
from loop.runtime_noise import is_non_substantive_workspace_path

def is_router_runtime_metadata_path(path: str | Path) -> bool:
    """Return whether one relative workspace path belongs to router-owned runtime metadata."""

    normalized = str(path or "").strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized == str(ROUTER_RUNTIME_ROOT) or normalized.startswith(
        f"{ROUTER_RUNTIME_ROOT.as_posix()}/"
    )


def workspace_output_fingerprint(workspace_root: str | Path) -> str:
    """Return a deterministic fingerprint of substantive workspace outputs."""

    resolved_workspace = Path(workspace_root).expanduser().resolve()
    digest = hashlib.sha256()
    if not resolved_workspace.exists():
        digest.update(b"<missing-workspace>")
        return digest.hexdigest()
    for candidate in sorted(resolved_workspace.rglob("*")):
        if not candidate.is_file() or candidate.is_symlink():
            continue
        relpath = candidate.relative_to(resolved_workspace).as_posix()
        if relpath.startswith(".git/"):
            continue
        if is_non_substantive_workspace_path(relpath) or is_router_runtime_metadata_path(relpath):
            continue
        digest.update(relpath.encode("utf-8"))
        digest.update(b"\0")
        digest.update(candidate.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
