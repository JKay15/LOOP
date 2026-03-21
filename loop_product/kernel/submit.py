"""Kernel submission helpers for control envelopes."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from loop_product.gateway import accept_control_envelope, normalize_control_envelope
from loop_product.kernel.authority import kernel_internal_authority
from loop_product.kernel.topology import apply_accepted_topology_mutation, review_topology_mutation
from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
from loop_product.protocols.topology import TopologyMutation


def _enforce_topology_kernel_review(state_root: Path, kernel_state, envelope: ControlEnvelope) -> ControlEnvelope:
    payload = dict(envelope.payload or {})
    mutation_payload = payload.get("topology_mutation") or payload
    try:
        mutation = TopologyMutation.from_dict(dict(mutation_payload or {}))
    except Exception as exc:  # noqa: BLE001
        return envelope.rejected(f"invalid topology mutation payload: {exc}")
    review = review_topology_mutation(kernel_state, mutation, state_root=state_root)
    summary = str(review.get("summary") or mutation.reason or envelope.note or "kernel reviewed topology mutation")
    reviewed = replace(
        envelope,
        payload={
            "topology_mutation": mutation.to_dict(),
            "review": review,
        },
        note=summary,
    )
    if str(review.get("decision") or "").upper() != "ACCEPT":
        return reviewed.rejected(summary)
    return reviewed


def submit_control_envelope(state_root: Path, envelope: ControlEnvelope) -> ControlEnvelope:
    """Normalize and accept a control envelope."""

    authority = kernel_internal_authority()
    normalized = normalize_control_envelope(envelope)
    kernel_state = None
    if normalized.status.value != "rejected" and normalized.classification == "topology":
        kernel_state = load_kernel_state(state_root)
        normalized = _enforce_topology_kernel_review(state_root, kernel_state, normalized)
    accepted = accept_control_envelope(state_root, normalized)
    if accepted.status.value != "accepted":
        return accepted
    if kernel_state is None:
        kernel_state = load_kernel_state(state_root)
    kernel_state.apply_accepted_envelope(accepted)
    if accepted.classification == "topology":
        apply_accepted_topology_mutation(state_root, kernel_state, accepted, authority=authority)
    elif accepted.classification in {"dispatch", "terminal"}:
        payload = dict(accepted.payload or {})
        node_id = str(payload.get("node_id") or accepted.source or "").strip()
        if node_id and node_id in kernel_state.nodes:
            persist_node_snapshot(state_root, kernel_state.nodes[node_id], authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    return accepted


def submit_topology_mutation(
    state_root: Path,
    mutation: TopologyMutation,
    *,
    round_id: str,
    generation: int,
    source: str | None = None,
) -> ControlEnvelope:
    """Review a topology mutation under kernel authority, then accept or reject it."""

    kernel_state = load_kernel_state(state_root)
    review = review_topology_mutation(kernel_state, mutation, state_root=state_root)
    envelope = mutation.to_envelope(
        round_id=round_id,
        generation=generation,
        source=source or mutation.source_node_id,
        status=EnvelopeStatus.PROPOSAL,
        note=str(review.get("summary") or mutation.reason or ""),
    )
    envelope.payload = {
        "topology_mutation": mutation.to_dict(),
        "review": review,
    }
    if str(review.get("decision") or "").upper() != "ACCEPT":
        envelope = envelope.rejected(str(review.get("summary") or "kernel rejected topology mutation"))
    return submit_control_envelope(state_root, envelope)
