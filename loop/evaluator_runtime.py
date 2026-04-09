"""Canonical node-scoped evaluator runtime paths."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from loop.events import ActorKind


ROUTER_RUNTIME_ROOT = Path(".loop") / "router_runtime"
NODES_ROOT = ROUTER_RUNTIME_ROOT / "nodes"


def runtime_root_dir(workspace_root: str | Path) -> Path:
    return (Path(workspace_root).expanduser().resolve() / ROUTER_RUNTIME_ROOT).resolve()


def _default_node_id(actor_kind: ActorKind | None = None) -> str:
    if actor_kind is ActorKind.KERNEL:
        return "0"
    return "1"


def node_runtime_dir(workspace_root: str | Path, node_id: str | None) -> Path:
    normalized_node_id = str(node_id or _default_node_id()).strip()
    if not normalized_node_id:
        raise ValueError("node_id is required")
    return (runtime_root_dir(workspace_root) / "nodes" / f"node-{normalized_node_id}").resolve()


def _task_id_path_component(task_id: str | None) -> str:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise ValueError("task_id is required")
    if re.fullmatch(r"[A-Za-z0-9._-]+", normalized_task_id):
        return normalized_task_id
    digest = hashlib.sha256(normalized_task_id.encode("utf-8")).hexdigest()[:8]
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", normalized_task_id).strip("._-") or "task"
    return f"{slug}-{digest}"


def completions_dir(workspace_root: str | Path, node_id: str) -> Path:
    return (node_runtime_dir(workspace_root, node_id) / "completions").resolve()


def actor_completion_record_path(
    workspace_root: str | Path,
    actor_kind: ActorKind,
    *,
    node_id: str | None = None,
    task_id: str | None = None,
) -> Path:
    base_dir = completions_dir(workspace_root, str(node_id or _default_node_id(actor_kind)))
    if actor_kind is ActorKind.EVALUATOR_AI_USER:
        return (base_dir / f"{actor_kind.value}.{_task_id_path_component(task_id)}.json").resolve()
    return (base_dir / f"{actor_kind.value}.json").resolve()


def checker_dir(workspace_root: str | Path, node_id: str | None) -> Path:
    return (node_runtime_dir(workspace_root, node_id) / "checker").resolve()


def checker_current_tasks_path(workspace_root: str | Path, node_id: str | None) -> Path:
    return (checker_dir(workspace_root, node_id) / "current" / "tasks.json").resolve()


def checker_accepted_tasks_path(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    attempt_count: int,
) -> Path:
    return (
        checker_dir(workspace_root, node_id)
        / "accepted"
        / f"attempt-{int(attempt_count):03d}"
        / "tasks.json"
    ).resolve()


def tester_dir(workspace_root: str | Path, node_id: str | None) -> Path:
    return (node_runtime_dir(workspace_root, node_id) / "tester").resolve()


def tester_current_result_path(workspace_root: str | Path, node_id: str | None) -> Path:
    return (tester_dir(workspace_root, node_id) / "current" / "result.md").resolve()


def tester_accepted_result_path(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    attempt_count: int,
) -> Path:
    return (
        tester_dir(workspace_root, node_id)
        / "accepted"
        / f"attempt-{int(attempt_count):03d}"
        / "result.md"
    ).resolve()


def ai_user_dir(workspace_root: str | Path, node_id: str | None) -> Path:
    return (node_runtime_dir(workspace_root, node_id) / "ai_user").resolve()


def ai_user_task_dir(workspace_root: str | Path, node_id: str | None, task_id: str) -> Path:
    return (ai_user_dir(workspace_root, node_id) / _task_id_path_component(task_id)).resolve()


def ai_user_current_result_path(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    task_id: str,
) -> Path:
    return (ai_user_task_dir(workspace_root, node_id, task_id) / "current" / "result.md").resolve()


def ai_user_accepted_result_path(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    task_id: str,
    attempt_count: int,
) -> Path:
    return (
        ai_user_task_dir(workspace_root, node_id, task_id)
        / "accepted"
        / f"attempt-{int(attempt_count):03d}"
        / "result.md"
    ).resolve()


def ai_user_artifacts_dir(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    task_id: str,
) -> Path:
    return (ai_user_task_dir(workspace_root, node_id, task_id) / "artifacts").resolve()


def reviewer_dir(workspace_root: str | Path, node_id: str | None) -> Path:
    return (node_runtime_dir(workspace_root, node_id) / "reviewer").resolve()


def reviewer_current_report_path(workspace_root: str | Path, node_id: str | None) -> Path:
    return (reviewer_dir(workspace_root, node_id) / "current" / "report.md").resolve()


def reviewer_current_feedback_path(workspace_root: str | Path, node_id: str | None) -> Path:
    return (reviewer_dir(workspace_root, node_id) / "current" / "feedback.json").resolve()


def reviewer_accepted_report_path(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    attempt_count: int,
) -> Path:
    return (
        reviewer_dir(workspace_root, node_id)
        / "accepted"
        / f"attempt-{int(attempt_count):03d}"
        / "report.md"
    ).resolve()


def reviewer_accepted_feedback_path(
    workspace_root: str | Path,
    node_id: str | None,
    *,
    attempt_count: int,
) -> Path:
    return (
        reviewer_dir(workspace_root, node_id)
        / "accepted"
        / f"attempt-{int(attempt_count):03d}"
        / "feedback.json"
    ).resolve()


def feedback_dir(workspace_root: str | Path, node_id: str | None) -> Path:
    return (node_runtime_dir(workspace_root, node_id) / "feedback").resolve()


def active_feedback_path(workspace_root: str | Path, node_id: str | None) -> Path:
    return (feedback_dir(workspace_root, node_id) / "active_feedback.json").resolve()


def feedback_history_path(workspace_root: str | Path, node_id: str | None) -> Path:
    return (feedback_dir(workspace_root, node_id) / "feedback_history.jsonl").resolve()


def reviewer_feedback_submission_snapshot_path(
    workspace_root: str | Path,
    *,
    node_id: str | None,
    attempt_count: int,
) -> Path:
    return (
        feedback_dir(workspace_root, node_id)
        / "submissions"
        / f"reviewer-attempt-{int(attempt_count):03d}.json"
    ).resolve()
