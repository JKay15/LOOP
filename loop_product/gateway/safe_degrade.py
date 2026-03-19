"""Fail-closed envelope construction for evaluator or dispatch errors."""

from __future__ import annotations

from typing import Any

from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus, infer_payload_type


def safe_degrade_envelope(
    *,
    source: str,
    envelope_type: str,
    round_id: str,
    generation: int,
    reason: str,
    payload: dict[str, Any] | None = None,
) -> ControlEnvelope:
    """Build a rejected envelope without pretending acceptance."""

    return ControlEnvelope(
        source=source,
        envelope_type=envelope_type,
        round_id=round_id,
        generation=generation,
        payload_type=infer_payload_type(envelope_type),
        payload=dict(payload or {}),
        status=EnvelopeStatus.REJECTED,
        note=reason,
        classification="rejected",
    )
