"""Minimal router inbox item objects."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


KERNEL_NODE_ID = "0"
KERNEL_ATTEMPT_COUNT = 1


class ActorKind(str, Enum):
    """Concrete router-visible agent roles."""

    KERNEL = "kernel"
    IMPLEMENTER = "implementer"
    EVALUATOR_CHECKER = "evaluator_checker"
    EVALUATOR_TESTER = "evaluator_tester"
    EVALUATOR_AI_USER = "evaluator_ai_user"
    EVALUATOR_REVIEWER = "evaluator_reviewer"


@dataclass(frozen=True)
class ActorRef:
    """Identify one router-visible actor attempt."""

    node_id: str
    actor_kind: ActorKind
    attempt_count: int
    task_id: str | None = None

    def __post_init__(self) -> None:
        normalized_task_id = str(self.task_id or "").strip()
        if self.actor_kind is not ActorKind.EVALUATOR_AI_USER and normalized_task_id:
            raise ValueError("only evaluator_ai_user actors may carry task_id")
        object.__setattr__(self, "task_id", normalized_task_id or None)
        if self.actor_kind is ActorKind.KERNEL:
            if str(self.node_id) != KERNEL_NODE_ID:
                raise ValueError("kernel actor must use node_id 0")
            if int(self.attempt_count) <= 0:
                raise ValueError("kernel actor must use a positive attempt_count")
        elif str(self.node_id) == KERNEL_NODE_ID and self.actor_kind is ActorKind.IMPLEMENTER:
            raise ValueError("implementer actor must not use node_id 0")


@dataclass(frozen=True)
class OutputWindow:
    """One actor observation window, with or without output progress."""

    actor: ActorRef
    had_output: bool
    observed_at: datetime


@dataclass(frozen=True)
class ProcessExitedObserved:
    """One actor process exited after starting."""

    actor: ActorRef
    pid: int
    process_birth_time: float | None
    exit_code: int | None
    signal_name: str | None
    occurred_at: datetime


RouterEvent = OutputWindow | ProcessExitedObserved


@dataclass(frozen=True)
class RequestSplit:
    """A child actor asks router to submit a split proposal upward."""

    actor: ActorRef
    split_bundle_ref: str
    durable_commit: str
    diff_fingerprint: str
    requested_at: datetime


@dataclass(frozen=True)
class ApproveSplit:
    """Kernel approves one previously submitted split request."""

    actor: ActorRef
    target_node_id: str
    request_seq: int
    approved_at: datetime


@dataclass(frozen=True)
class RejectSplit:
    """Kernel rejects one previously submitted split request."""

    actor: ActorRef
    target_node_id: str
    request_seq: int
    rejected_at: datetime
    reason_ref: str | None


@dataclass(frozen=True)
class TakeoverResolved:
    """Router resolves one takeover request and returns a durable handoff."""

    actor: ActorRef
    target_node_id: str
    takeover_id: str
    resolution_ref: str
    workspace_root: str
    final_effects_file: str
    resolved_at: datetime

RouterControlMessage = (
    RequestSplit
    | ApproveSplit
    | RejectSplit
    | TakeoverResolved
)

RouterInboxItem = RouterEvent | RouterControlMessage
