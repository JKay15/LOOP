"""Shared completion-record and workspace-fingerprint helpers for router actors."""

from __future__ import annotations

import hashlib
from pathlib import Path

from loop.events import ActorKind
from loop.runtime_noise import is_runtime_noise_path


ROUTER_RUNTIME_ROOT = Path(".loop") / "router_runtime"
COMPLETION_ROOT = ROUTER_RUNTIME_ROOT / "completions"


def _completion_task_suffix(task_id: str) -> str:
    normalized = str(task_id or "").strip()
    if not normalized:
        raise ValueError("task_id is required for evaluator_ai_user completion paths")
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def actor_completion_record_path(
    workspace_root: str | Path,
    actor_kind: ActorKind,
    *,
    task_id: str | None = None,
) -> Path:
    """Return the canonical completion record path for one actor kind in one workspace."""

    resolved_workspace = Path(workspace_root).expanduser().resolve()
    if actor_kind is ActorKind.EVALUATOR_AI_USER:
        suffix = _completion_task_suffix(str(task_id or "").strip())
        return (resolved_workspace / COMPLETION_ROOT / f"{actor_kind.value}.{suffix}.json").resolve()
    return (resolved_workspace / COMPLETION_ROOT / f"{actor_kind.value}.json").resolve()


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
        if is_runtime_noise_path(relpath) or is_router_runtime_metadata_path(relpath):
            continue
        digest.update(relpath.encode("utf-8"))
        digest.update(b"\0")
        digest.update(candidate.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
