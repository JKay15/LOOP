"""Workspace-local reviewer feedback ledger paths."""

from __future__ import annotations

from pathlib import Path


def feedback_runtime_dir(workspace_root: str | Path) -> Path:
    """Return the workspace-local feedback runtime directory."""

    return (
        Path(workspace_root).expanduser().resolve() / ".loop" / "router_runtime" / "feedback"
    ).resolve()


def active_feedback_path(workspace_root: str | Path) -> Path:
    """Return the canonical active-feedback ledger path."""

    return (feedback_runtime_dir(workspace_root) / "active_feedback.json").resolve()


def feedback_history_path(workspace_root: str | Path) -> Path:
    """Return the append-only feedback history ledger path."""

    return (feedback_runtime_dir(workspace_root) / "feedback_history.jsonl").resolve()


def reviewer_feedback_submission_snapshot_path(
    workspace_root: str | Path,
    *,
    node_id: str,
    attempt_count: int,
) -> Path:
    """Return the canonical workspace path for one reviewer feedback submission snapshot."""

    return (
        feedback_runtime_dir(workspace_root)
        / "submissions"
        / f"reviewer-node-{str(node_id).strip()}-attempt-{int(attempt_count)}.json"
    ).resolve()
