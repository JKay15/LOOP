"""Merge-review helpers."""

from __future__ import annotations

from typing import Any

from loop_product.kernel.state import KernelState
from loop_product.protocols.topology import TopologyMutation

_MERGE_READY_CHILD_STATUSES = {"COMPLETED"}


def build_merge_request(
    source_node_id: str,
    target_node_ids: list[str],
    *,
    reason: str = "",
    payload: dict[str, Any] | None = None,
) -> TopologyMutation:
    """Build a structured merge request without applying topology changes directly."""

    return TopologyMutation.merge(source_node_id, target_node_ids, reason=reason, payload=dict(payload or {}))


def review_merge_request(kernel_state: KernelState, mutation: TopologyMutation) -> dict[str, Any]:
    """Review a merge request under current kernel authority."""

    source = dict(kernel_state.nodes.get(mutation.source_node_id) or {})
    source_status = str(source.get("status") or "")
    source_allowed_actions = {str(item) for item in source.get("allowed_actions") or []}
    target_ids = [str(item).strip() for item in mutation.target_node_ids]
    target_nodes = [dict(kernel_state.nodes.get(target_id) or {}) for target_id in target_ids]
    normalized_merge_mode = "parallel" if source_status == "BLOCKED" else "deferred" if source_status == "COMPLETED" else ""
    minimum_target_count = 2 if normalized_merge_mode == "parallel" else 1 if normalized_merge_mode == "deferred" else 9999

    checks = [
        {
            "check_id": "M1_source_node_authorized",
            "passed": bool(source) and source_status in {"BLOCKED", "COMPLETED"} and "merge_request" in source_allowed_actions,
            "detail": (
                "merge requires an existing source node that explicitly allows merge_request; "
                "parallel convergence uses a BLOCKED source, while deferred convergence uses a COMPLETED source"
            ),
        },
        {
            "check_id": "M2_target_node_ids",
            "passed": len(target_ids) >= minimum_target_count
            and all(target_ids)
            and len(set(target_ids)) == len(target_ids)
            and all(target_nodes)
            and all(str(node.get("parent_node_id") or "") == mutation.source_node_id for node in target_nodes),
            "detail": (
                "merge targets must be distinct existing child nodes owned by the source node; "
                "parallel merge needs at least two children, deferred merge may converge one or more"
            ),
        },
        {
            "check_id": "M3_merge_ready_children",
            "passed": bool(target_nodes)
            and all(str(node.get("status") or "") in _MERGE_READY_CHILD_STATUSES for node in target_nodes),
            "detail": "merge targets must already be in a completed terminal state",
        },
    ]
    accepted = all(bool(item["passed"]) for item in checks)
    return {
        "decision": "ACCEPT" if accepted else "REJECT",
        "summary": (
            f"accepted {normalized_merge_mode or 'unknown'} merge request for {mutation.source_node_id} from {len(target_ids)} child nodes"
            if accepted
            else f"rejected merge request for {mutation.source_node_id}: kernel review failed"
        ),
        "checks": checks,
        "normalized_target_ids": target_ids if accepted else [],
        "normalized_merge_mode": normalized_merge_mode,
    }
