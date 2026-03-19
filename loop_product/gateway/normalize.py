"""Normalize raw control objects into a repo-local control envelope."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping

from jsonschema import ValidationError

from loop_product.gateway.classify import classify_envelope
from loop_product.gateway.safe_degrade import safe_degrade_envelope
from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus, infer_payload_type
from loop_product.protocols.schema import validate_repo_object

_KNOWN_PAYLOAD_TYPES = frozenset(
    {
        "topology_mutation",
        "dispatch_status",
        "local_control_decision",
        "node_terminal_result",
        "evaluator_result",
        "invalid_control_envelope",
    }
)


def _coerce_envelope_status(raw_status: object) -> EnvelopeStatus:
    candidate = getattr(raw_status, "value", raw_status)
    return EnvelopeStatus(str(candidate or EnvelopeStatus.PROPOSAL.value))


def _coerce_payload_type(*, envelope_type: object, raw_payload_type: object) -> str:
    normalized_envelope_type = str(envelope_type or "").strip()
    inferred = infer_payload_type(normalized_envelope_type)
    if inferred == "unknown":
        raise ValueError(f"unsupported envelope_type: {normalized_envelope_type or '<empty>'}")
    candidate = str(raw_payload_type or "").strip().lower()
    if not candidate:
        return inferred
    if candidate not in _KNOWN_PAYLOAD_TYPES:
        raise ValueError(f"unsupported payload_type: {candidate}")
    if candidate != inferred:
        raise ValueError(
            f"payload_type {candidate!r} does not match envelope_type {normalized_envelope_type!r}; expected {inferred!r}"
        )
    return candidate


def _degraded_invalid_envelope(*, raw: object, reason: str) -> ControlEnvelope:
    payload: dict[str, Any]
    if isinstance(raw, Mapping):
        payload = {"raw": dict(raw)}
    else:
        payload = {"raw": repr(raw)}
    return safe_degrade_envelope(
        source="gateway",
        envelope_type="invalid_control_envelope",
        round_id="R-invalid",
        generation=0,
        reason=reason,
        payload=payload,
    )


def normalize_control_envelope(raw: ControlEnvelope | Mapping[str, Any]) -> ControlEnvelope:
    """Normalize arbitrary envelope-like input."""

    if isinstance(raw, ControlEnvelope):
        try:
            envelope = ControlEnvelope(
                source=str(raw.source),
                envelope_type=str(raw.envelope_type),
                round_id=str(raw.round_id),
                generation=int(raw.generation),
                payload_type=_coerce_payload_type(
                    envelope_type=raw.envelope_type,
                    raw_payload_type=raw.payload_type,
                ),
                payload=dict(raw.payload or {}),
                status=_coerce_envelope_status(raw.status),
                note=str(raw.note or ""),
                envelope_id=str(raw.envelope_id or ""),
                classification=str(raw.classification or "unknown"),
                accepted_at=str(raw.accepted_at or ""),
            )
        except (TypeError, ValueError) as exc:
            return _degraded_invalid_envelope(raw=raw, reason=f"invalid control envelope object: {exc}")
    else:
        try:
            missing = [field for field in ("source", "envelope_type", "round_id", "generation", "payload", "status") if field not in raw]
            if missing:
                return _degraded_invalid_envelope(
                    raw=raw,
                    reason=f"invalid raw control envelope: missing required fields: {', '.join(missing)}",
                )
            source = str(raw["source"])
            envelope_type = str(raw["envelope_type"])
            round_id = str(raw["round_id"])
            generation = int(raw["generation"])
            envelope = ControlEnvelope(
                source=source,
                envelope_type=envelope_type,
                round_id=round_id,
                generation=generation,
                payload_type=_coerce_payload_type(
                    envelope_type=envelope_type,
                    raw_payload_type=raw.get("payload_type"),
                ),
                payload=dict(raw.get("payload") or {}),
                status=_coerce_envelope_status(raw.get("status")),
                note=str(raw.get("note") or ""),
                envelope_id=str(raw.get("envelope_id") or ""),
                classification=str(raw.get("classification") or "unknown"),
                accepted_at=str(raw.get("accepted_at") or ""),
            )
        except (TypeError, ValueError) as exc:
            return _degraded_invalid_envelope(raw=raw, reason=f"invalid raw control envelope: {exc}")

    if not envelope.payload_type:
        envelope = replace(envelope, payload_type=infer_payload_type(envelope.envelope_type))
    envelope = envelope.with_identity()
    envelope = replace(envelope, classification=classify_envelope(envelope))
    try:
        validate_repo_object("LoopSystemControlEnvelope.schema.json", envelope.to_dict())
    except ValidationError as exc:
        return safe_degrade_envelope(
            source=envelope.source or "gateway",
            envelope_type=envelope.envelope_type or "invalid_control_envelope",
            round_id=envelope.round_id or "R-invalid",
            generation=max(envelope.generation, 0),
            reason=f"control envelope schema violation: {exc.message}",
            payload={"raw_payload": envelope.payload},
        ).with_identity()
    return envelope
