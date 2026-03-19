"""Classify normalized control envelopes."""

from __future__ import annotations

from loop_product.protocols.control_envelope import ControlEnvelope, infer_payload_type


def classify_envelope(envelope: ControlEnvelope) -> str:
    """Return a coarse envelope class for kernel handling."""

    if envelope.status.value == "rejected":
        return "rejected"
    payload_type = str(envelope.payload_type or infer_payload_type(envelope.envelope_type)).strip().lower()
    if payload_type == "dispatch_status":
        return "dispatch"
    if payload_type == "local_control_decision":
        return "local_control"
    if payload_type == "evaluator_result":
        return "evaluator"
    if payload_type == "topology_mutation":
        return "topology"
    if payload_type == "node_terminal_result":
        return "terminal"
    envelope_type = envelope.envelope_type.lower()
    if "control" in envelope_type or "local_control" in envelope_type:
        return "control"
    return "unknown"
