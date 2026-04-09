"""Workspace-local reviewer feedback ledger paths."""

from __future__ import annotations

from pathlib import Path

from loop.evaluator_runtime import (
    active_feedback_path as _active_feedback_path,
    feedback_dir as _feedback_dir,
    feedback_history_path as _feedback_history_path,
    reviewer_feedback_submission_snapshot_path as _reviewer_feedback_submission_snapshot_path,
)


def feedback_runtime_dir(workspace_root: str | Path, node_id: str | None = None) -> Path:
    """Return the workspace-local feedback runtime directory for one node."""

    return _feedback_dir(workspace_root, node_id)


def active_feedback_path(workspace_root: str | Path, node_id: str | None = None) -> Path:
    """Return the canonical active-feedback ledger path."""

    return _active_feedback_path(workspace_root, node_id)


def feedback_history_path(workspace_root: str | Path, node_id: str | None = None) -> Path:
    """Return the append-only feedback history ledger path."""

    return _feedback_history_path(workspace_root, node_id)


def reviewer_feedback_submission_snapshot_path(
    workspace_root: str | Path,
    *,
    node_id: str | None = None,
    attempt_count: int,
) -> Path:
    """Return the canonical workspace path for one reviewer feedback submission snapshot."""

    return _reviewer_feedback_submission_snapshot_path(
        workspace_root,
        node_id=node_id,
        attempt_count=attempt_count,
    )
