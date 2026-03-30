"""Structured control envelope used by kernel and gateway surfaces."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
from typing import Any


class EnvelopeStatus(str, Enum):
    """Control envelope status markers."""

    PROPOSAL = "proposal"
    REPORT = "report"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


def infer_payload_type(envelope_type: str) -> str:
    """Infer the structured payload type from an envelope type."""

    normalized = str(envelope_type or "").strip().lower()
    if normalized in {
        "split_request",
        "activate_request",
        "merge_request",
        "resume_request",
        "retry_request",
        "relaunch_request",
        "reap_request",
    }:
        return "topology_mutation"
    if normalized in {
        "repo_tree_cleanup_request",
        "heavy_object_authority_gap_audit_request",
        "heavy_object_authority_gap_repo_remediation_request",
        "heavy_object_discovery_request",
        "heavy_object_registration_request",
        "heavy_object_observation_request",
        "heavy_object_reference_request",
        "heavy_object_reference_release_request",
        "heavy_object_pin_request",
        "heavy_object_pin_release_request",
        "heavy_object_supersession_request",
        "heavy_object_gc_eligibility_request",
        "heavy_object_reclamation_request",
    }:
        return "local_control_decision"
    if normalized in {"child_dispatch_status", "child_heartbeat"}:
        return "dispatch_status"
    if normalized in {"local_control_decision", "local_control"}:
        return "local_control_decision"
    if normalized in {"node_terminal_result", "terminal_result"}:
        return "node_terminal_result"
    if normalized in {"evaluator_result", "evaluator_verdict"}:
        return "evaluator_result"
    if normalized in {"invalid_control_envelope"}:
        return "invalid_control_envelope"
    if "heartbeat" in normalized or "dispatch" in normalized:
        return "dispatch_status"
    if "local_control" in normalized:
        return "local_control_decision"
    if "terminal" in normalized:
        return "node_terminal_result"
    if "evaluator" in normalized:
        return "evaluator_result"
    if any(token in normalized for token in ("split", "activate", "merge", "resume", "retry", "relaunch", "reap")):
        return "topology_mutation"
    return "unknown"


@dataclass(slots=True)
class ControlEnvelope:
    """Unified envelope for proposal, report, and accepted facts."""

    source: str
    envelope_type: str
    round_id: str
    generation: int
    payload_type: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    status: EnvelopeStatus = EnvelopeStatus.PROPOSAL
    note: str = ""
    envelope_id: str = ""
    classification: str = "unknown"
    accepted_at: str = ""

    def with_identity(self) -> "ControlEnvelope":
        digest = hashlib.sha256(
            json.dumps(
                {
                    "source": self.source,
                    "envelope_type": self.envelope_type,
                    "payload_type": self.payload_type or infer_payload_type(self.envelope_type),
                    "round_id": self.round_id,
                    "generation": self.generation,
                    "payload": self.payload,
                },
                ensure_ascii=True,
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()[:16]
        return replace(self, envelope_id=f"env_{digest}")

    def accepted(self) -> "ControlEnvelope":
        return replace(self, status=EnvelopeStatus.ACCEPTED)

    def rejected(self, note: str) -> "ControlEnvelope":
        return replace(self, status=EnvelopeStatus.REJECTED, note=note)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["payload_type"] = self.payload_type or infer_payload_type(self.envelope_type)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ControlEnvelope":
        return cls(
            source=str(data["source"]),
            envelope_type=str(data["envelope_type"]),
            payload_type=str(data.get("payload_type") or infer_payload_type(str(data.get("envelope_type") or ""))),
            round_id=str(data["round_id"]),
            generation=int(data["generation"]),
            payload=dict(data.get("payload") or {}),
            status=EnvelopeStatus(str(data.get("status") or EnvelopeStatus.PROPOSAL.value)),
            note=str(data.get("note") or ""),
            envelope_id=str(data.get("envelope_id") or ""),
            classification=str(data.get("classification") or "unknown"),
            accepted_at=str(data.get("accepted_at") or ""),
        )
