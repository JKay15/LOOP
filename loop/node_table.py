"""Minimal router-owned node-table object definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from loop.events import ActorKind, ActorRef


class ComponentStatus(str, Enum):
    """Minimal component lifecycle states for router v0."""

    INACTIVE = "inactive"
    RUNNING = "running"
    COMPLETED = "completed"
    TERMINAL_FAILED = "terminal_failed"


@dataclass(frozen=True)
class ComponentRuntimeState:
    """Router-visible runtime state for one node component."""

    status: ComponentStatus
    attempt_count: int
    pid: int | None
    process_birth_time: float | None
    session_ids: list[str]
    workspace_fingerprint_before: str
    saw_output_in_attempt: bool
    consecutive_no_progress: int
    consecutive_failed_exits: int
    task_id: str = ""


def component_key(*, actor_kind: ActorKind, task_id: str | None = None) -> str:
    """Return the stable runtime-state key for one component instance."""

    normalized_task_id = str(task_id or "").strip()
    if actor_kind is ActorKind.EVALUATOR_AI_USER:
        if not normalized_task_id:
            raise ValueError("evaluator_ai_user component key requires task_id")
        return f"{actor_kind.value}:{normalized_task_id}"
    return actor_kind.value


def component_key_for_actor(actor: ActorRef) -> str:
    """Return the stable runtime-state key for one running actor instance."""

    return component_key(
        actor_kind=actor.actor_kind,
        task_id=actor.task_id,
    )


def component_keys_for_kind(
    components: dict[str, ComponentRuntimeState],
    actor_kind: ActorKind,
) -> list[str]:
    """List runtime-state keys currently present for one actor kind."""

    if actor_kind is ActorKind.EVALUATOR_AI_USER:
        prefix = f"{actor_kind.value}:"
        return sorted(key for key in components if key.startswith(prefix))
    key = actor_kind.value
    return [key] if key in components else []


@dataclass(frozen=True)
class NodeRuntimeRecord:
    """Router-owned authoritative record for one LOOP node."""

    node_id: str
    parent_node_id: str
    child_node_ids: list[str]
    workspace_root: str
    final_effects_file: str
    split_request: int
    split_approved: int
    evaluator_phase: str
    checker_tasks_ref: str
    task_result_refs: dict[str, dict[str, str]]
    reviewer_verdict_kind: str
    reviewer_report_ref: str
    pending_prelude_lines: list[str]
    current_components: list[ActorRef]
    durable_commit: str
    escalated_to_kernel: bool
    last_rejected_split_diff_fingerprint: str
    approved_split_request_seq: int = 0
    result_commit: str = ""
    components: dict[str, ComponentRuntimeState] = field(default_factory=dict)
