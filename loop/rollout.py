"""Helpers for rollout JSONL fallback lookup."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


def _resolved_existing_paths(paths: Iterable[str | Path | None]) -> list[Path]:
    resolved: list[Path] = []
    seen: set[str] = set()
    for raw in paths:
        if raw in (None, ""):
            continue
        path = Path(raw).expanduser().resolve()
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        if path.exists() and path.is_file():
            resolved.append(path)
    return resolved


def _rollout_sort_key(path: Path) -> tuple[int, str]:
    try:
        mtime_ns = int(path.stat().st_mtime_ns)
    except OSError:
        mtime_ns = -1
    return (mtime_ns, str(path))


def fallback_rollout_path_for_session(
    session_id: str,
    *,
    codex_home: str | Path | None = None,
    preferred_paths: Iterable[str | Path | None] = (),
) -> Path | None:
    """Best-effort fallback lookup for the current rollout file of one session."""

    normalized_session_id = str(session_id or "").strip()
    candidates = _resolved_existing_paths(preferred_paths)
    if not normalized_session_id:
        if not candidates:
            return None
        return max(candidates, key=_rollout_sort_key)

    resolved_codex_home = Path(codex_home or (Path.home() / ".codex")).expanduser().resolve()
    sessions_root = resolved_codex_home / "sessions"
    if sessions_root.exists():
        candidates.extend(sessions_root.rglob(f"rollout-*{normalized_session_id}.jsonl"))
    resolved_candidates = _resolved_existing_paths(candidates)
    if not resolved_candidates:
        return None
    return max(resolved_candidates, key=_rollout_sort_key)
