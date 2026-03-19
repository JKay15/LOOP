"""Heartbeat envelopes for child nodes."""

from __future__ import annotations

from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
from loop_product.protocols.control_objects import ChildDispatchStatus, DispatchState
from loop_product.protocols.node import NodeSpec


def build_child_dispatch_status(node: NodeSpec, *, note: str) -> ChildDispatchStatus:
    """Create a structured child dispatch status payload."""

    return ChildDispatchStatus(
        node_id=node.node_id,
        parent_node_id=node.parent_node_id,
        round_id=node.round_id,
        generation=node.generation,
        dispatch_state=DispatchState.HEARTBEAT,
        node_status=node.status.value,
        summary=note,
    )


def build_child_heartbeat(node: NodeSpec, *, note: str) -> ControlEnvelope:
    """Create a child heartbeat report."""

    status = build_child_dispatch_status(node, note=note)
    return ControlEnvelope(
        source=status.node_id,
        envelope_type="child_heartbeat",
        payload_type="dispatch_status",
        round_id=status.round_id,
        generation=status.generation,
        payload=status.to_dict(),
        status=EnvelopeStatus.REPORT,
        note=status.summary,
    )
