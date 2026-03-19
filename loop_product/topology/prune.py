"""Prune / reap review helpers."""

from __future__ import annotations

from loop_product.kernel.state import KernelState, NON_TERMINAL_NODE_STATUSES
from loop_product.protocols.topology import TopologyMutation


def pruneable(node_status: str) -> bool:
    return node_status in {"COMPLETED", "FAILED"}


def review_reap_request(kernel_state: KernelState, mutation: TopologyMutation) -> dict[str, object]:
    """Review a reap request under current kernel authority."""

    source = dict(kernel_state.nodes.get(mutation.source_node_id) or {})
    source_status = str(source.get("status") or "")
    has_active_children = any(
        str(node.get("parent_node_id") or "") == mutation.source_node_id
        and str(node.get("status") or "") in NON_TERMINAL_NODE_STATUSES
        for node in kernel_state.nodes.values()
    )
    checks = [
        {
            "check_id": "P1_source_node_exists",
            "passed": bool(source) and mutation.source_node_id != str(kernel_state.root_node_id),
            "detail": "reap requires an existing non-root node",
        },
        {
            "check_id": "P2_terminal_node",
            "passed": pruneable(source_status),
            "detail": "reap requires the source node to already be terminal",
        },
        {
            "check_id": "P3_no_active_children",
            "passed": not has_active_children,
            "detail": "reap requires the source node to have no active child dependents",
        },
    ]
    accepted = all(bool(item["passed"]) for item in checks)
    return {
        "decision": "ACCEPT" if accepted else "REJECT",
        "summary": (
            f"accepted reap request for {mutation.source_node_id}"
            if accepted
            else f"rejected reap request for {mutation.source_node_id}: kernel review failed"
        ),
        "checks": checks,
    }
