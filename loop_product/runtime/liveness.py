"""Liveness helpers for active node visibility."""

from __future__ import annotations

from typing import Any, Mapping

from loop_product.protocols.node import NodeSpec, NodeStatus, RuntimeAttachmentState, normalize_runtime_state


def runtime_attachment_state(node: NodeSpec | Mapping[str, Any]) -> RuntimeAttachmentState:
    payload = node.to_dict() if isinstance(node, NodeSpec) else dict(node)
    runtime_state = normalize_runtime_state(dict(payload.get("runtime_state") or {}))
    return RuntimeAttachmentState(str(runtime_state["attachment_state"]))


def build_runtime_loss_signal(
    *,
    observation_kind: str,
    summary: str,
    evidence_refs: list[str] | tuple[str, ...] = (),
    observed_at: str = "",
) -> dict[str, Any]:
    return normalize_runtime_state(
        {
            "attachment_state": RuntimeAttachmentState.LOST.value,
            "observed_at": observed_at,
            "summary": summary,
            "observation_kind": observation_kind,
            "evidence_refs": [str(item) for item in evidence_refs],
        }
    )


def is_live(node: NodeSpec) -> bool:
    return (
        node.status in {NodeStatus.PLANNED, NodeStatus.ACTIVE, NodeStatus.BLOCKED}
        and runtime_attachment_state(node) is not RuntimeAttachmentState.LOST
    )
