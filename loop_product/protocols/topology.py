"""Topology protocol surfaces for split, activation, merge, resume, and reap proposals."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus


@dataclass(slots=True)
class TopologyMutation:
    """Structured topology mutation proposal."""

    kind: str
    source_node_id: str
    target_node_ids: list[str] = field(default_factory=list)
    payload: dict[str, Any] = field(default_factory=dict)
    reason: str = ""

    @classmethod
    def split(
        cls,
        source_node_id: str,
        target_node_ids: list[str],
        *,
        split_mode: str = "parallel",
        completed_work: str = "",
        remaining_work: str = "",
        reason: str = "",
        payload: dict[str, Any] | None = None,
    ) -> "TopologyMutation":
        normalized_payload = dict(payload or {})
        normalized_payload.setdefault("split_mode", str(split_mode or "parallel"))
        normalized_payload.setdefault("completed_work", str(completed_work or ""))
        normalized_payload.setdefault("remaining_work", str(remaining_work or ""))
        return cls(
            kind="split",
            source_node_id=source_node_id,
            target_node_ids=target_node_ids,
            payload=normalized_payload,
            reason=reason,
        )

    @classmethod
    def merge(
        cls,
        source_node_id: str,
        target_node_ids: list[str],
        *,
        reason: str = "",
        payload: dict[str, Any] | None = None,
    ) -> "TopologyMutation":
        return cls(
            kind="merge",
            source_node_id=source_node_id,
            target_node_ids=target_node_ids,
            payload=dict(payload or {}),
            reason=reason,
        )

    @classmethod
    def activate(
        cls,
        source_node_id: str,
        *,
        reason: str = "",
        payload: dict[str, Any] | None = None,
    ) -> "TopologyMutation":
        return cls(kind="activate", source_node_id=source_node_id, payload=dict(payload or {}), reason=reason)

    @classmethod
    def resume(cls, source_node_id: str, *, reason: str = "", payload: dict[str, Any] | None = None) -> "TopologyMutation":
        return cls(kind="resume", source_node_id=source_node_id, payload=dict(payload or {}), reason=reason)

    @classmethod
    def retry(cls, source_node_id: str, *, reason: str = "", payload: dict[str, Any] | None = None) -> "TopologyMutation":
        return cls(kind="retry", source_node_id=source_node_id, payload=dict(payload or {}), reason=reason)

    @classmethod
    def relaunch(
        cls, source_node_id: str, *, reason: str = "", payload: dict[str, Any] | None = None
    ) -> "TopologyMutation":
        return cls(kind="relaunch", source_node_id=source_node_id, payload=dict(payload or {}), reason=reason)

    @classmethod
    def reap(cls, source_node_id: str, *, reason: str = "", payload: dict[str, Any] | None = None) -> "TopologyMutation":
        return cls(kind="reap", source_node_id=source_node_id, payload=dict(payload or {}), reason=reason)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_envelope(
        self,
        *,
        round_id: str,
        generation: int,
        source: str | None = None,
        status: EnvelopeStatus = EnvelopeStatus.PROPOSAL,
        note: str = "",
    ) -> ControlEnvelope:
        return ControlEnvelope(
            source=source or self.source_node_id,
            envelope_type=f"{self.kind}_request",
            payload_type="topology_mutation",
            round_id=round_id,
            generation=generation,
            payload=self.to_dict(),
            status=status,
            note=note or self.reason,
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TopologyMutation":
        return cls(
            kind=str(data["kind"]),
            source_node_id=str(data["source_node_id"]),
            target_node_ids=list(data.get("target_node_ids") or []),
            payload=dict(data.get("payload") or {}),
            reason=str(data.get("reason") or ""),
        )
