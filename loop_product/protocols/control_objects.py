"""Structured control payloads carried inside the unified control envelope."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum

from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus


class LocalControlAction(str, Enum):
    """Bounded node-local control decisions."""

    COMPLETE = "COMPLETE"
    RETRY = "RETRY"
    ESCALATE_BUG = "ESCALATE_BUG"
    ESCALATE_STUCK = "ESCALATE_STUCK"
    FAIL_CLOSED = "FAIL_CLOSED"


class DispatchState(str, Enum):
    """Child-dispatch lifecycle facts visible to the kernel."""

    MATERIALIZED = "MATERIALIZED"
    HEARTBEAT = "HEARTBEAT"
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"


class NodeTerminalOutcome(str, Enum):
    """Terminal node outcomes reported through the gateway."""

    PASS = "PASS"
    FAIL = "FAIL"
    BUG = "BUG"
    STUCK = "STUCK"
    FAIL_CLOSED = "FAIL_CLOSED"


@dataclass(slots=True)
class ChildDispatchStatus:
    """Structured child dispatch or heartbeat status."""

    node_id: str
    parent_node_id: str | None
    round_id: str
    generation: int
    dispatch_state: DispatchState
    node_status: str
    summary: str
    active_child_ids: list[str] = field(default_factory=list)
    evidence_refs: list[str] = field(default_factory=list)
    self_attribution: str = ""
    self_repair: str = ""

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["dispatch_state"] = self.dispatch_state.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "ChildDispatchStatus":
        return cls(
            node_id=str(data["node_id"]),
            parent_node_id=data.get("parent_node_id"),
            round_id=str(data["round_id"]),
            generation=int(data["generation"]),
            dispatch_state=DispatchState(str(data["dispatch_state"])),
            node_status=str(data["node_status"]),
            summary=str(data["summary"]),
            active_child_ids=[str(item) for item in (data.get("active_child_ids") or [])],
            evidence_refs=[str(item) for item in (data.get("evidence_refs") or [])],
            self_attribution=str(data.get("self_attribution") or ""),
            self_repair=str(data.get("self_repair") or ""),
        )

    def to_envelope(self, *, status: EnvelopeStatus = EnvelopeStatus.REPORT) -> ControlEnvelope:
        return ControlEnvelope(
            source=self.node_id,
            envelope_type="child_dispatch_status",
            payload_type="dispatch_status",
            round_id=self.round_id,
            generation=self.generation,
            payload=self.to_dict(),
            status=status,
            note=self.summary,
        )


@dataclass(slots=True)
class LocalControlDecision:
    """Node-local decision produced after evaluator feedback."""

    node_id: str
    round_id: str
    generation: int
    action: LocalControlAction
    source_lane: str
    summary: str
    evidence_refs: list[str] = field(default_factory=list)
    retryable: bool = False
    self_attribution: str = ""
    self_repair: str = ""

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["action"] = self.action.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "LocalControlDecision":
        return cls(
            node_id=str(data["node_id"]),
            round_id=str(data["round_id"]),
            generation=int(data["generation"]),
            action=LocalControlAction(str(data["action"])),
            source_lane=str(data["source_lane"]),
            summary=str(data["summary"]),
            evidence_refs=[str(item) for item in (data.get("evidence_refs") or [])],
            retryable=bool(data.get("retryable", False)),
            self_attribution=str(data.get("self_attribution") or ""),
            self_repair=str(data.get("self_repair") or ""),
        )

    def to_envelope(
        self,
        *,
        round_id: str | None = None,
        generation: int | None = None,
        status: EnvelopeStatus = EnvelopeStatus.REPORT,
    ) -> ControlEnvelope:
        return ControlEnvelope(
            source=self.node_id,
            envelope_type="local_control_decision",
            payload_type="local_control_decision",
            round_id=round_id or self.round_id,
            generation=self.generation if generation is None else generation,
            payload=self.to_dict(),
            status=status,
            note=self.summary,
        )


@dataclass(slots=True)
class NodeTerminalResult:
    """Terminal result reported for a node through the gateway."""

    node_id: str
    round_id: str
    generation: int
    outcome: NodeTerminalOutcome
    node_status: str
    summary: str
    evidence_refs: list[str] = field(default_factory=list)
    self_attribution: str = ""
    self_repair: str = ""

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["outcome"] = self.outcome.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "NodeTerminalResult":
        return cls(
            node_id=str(data["node_id"]),
            round_id=str(data["round_id"]),
            generation=int(data["generation"]),
            outcome=NodeTerminalOutcome(str(data["outcome"])),
            node_status=str(data["node_status"]),
            summary=str(data["summary"]),
            evidence_refs=[str(item) for item in (data.get("evidence_refs") or [])],
            self_attribution=str(data.get("self_attribution") or ""),
            self_repair=str(data.get("self_repair") or ""),
        )

    def to_envelope(
        self,
        *,
        round_id: str | None = None,
        generation: int | None = None,
        status: EnvelopeStatus = EnvelopeStatus.REPORT,
    ) -> ControlEnvelope:
        return ControlEnvelope(
            source=self.node_id,
            envelope_type="node_terminal_result",
            payload_type="node_terminal_result",
            round_id=round_id or self.round_id,
            generation=self.generation if generation is None else generation,
            payload=self.to_dict(),
            status=status,
            note=self.summary,
        )
