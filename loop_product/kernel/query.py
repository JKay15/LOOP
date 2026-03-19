"""Read-only authority queries over durable kernel state."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loop_product.kernel.state import ACTIVE_NODE_STATUSES, query_kernel_state
from loop_product.protocols.schema import validate_repo_object


def query_authority_view(state_root: Path) -> dict[str, Any]:
    """Return a simplified read-only authority view for current kernel state."""

    snapshot = query_kernel_state(state_root)
    nodes = dict(snapshot.get("nodes") or {})
    node_graph: list[dict[str, Any]] = []
    node_lifecycle: dict[str, str] = {}
    active_child_nodes: list[str] = []
    planned_child_nodes: list[str] = []

    for node_id, node in sorted(nodes.items(), key=lambda item: (int(item[1].get("generation", 0)), str(item[0]))):
        graph_entry = {
            "node_id": str(node_id),
            "parent_node_id": node.get("parent_node_id"),
            "node_kind": str(node.get("node_kind") or ""),
            "goal_slice": str(node.get("goal_slice") or ""),
            "generation": int(node.get("generation") or 0),
            "round_id": str(node.get("round_id") or ""),
            "status": str(node.get("status") or ""),
            "runtime_state": dict(node.get("runtime_state") or {}),
        }
        node_graph.append(graph_entry)
        node_lifecycle[graph_entry["node_id"]] = graph_entry["status"]
        if (
            graph_entry["node_id"] != str(snapshot.get("root_node_id") or "")
            and graph_entry["status"] in ACTIVE_NODE_STATUSES
        ):
            active_child_nodes.append(graph_entry["node_id"])
        if (
            graph_entry["node_id"] != str(snapshot.get("root_node_id") or "")
            and graph_entry["status"] == "PLANNED"
        ):
            planned_child_nodes.append(graph_entry["node_id"])

    authority_view = {
        "task_id": str(snapshot.get("task_id") or ""),
        "root_goal": str(snapshot.get("root_goal") or ""),
        "root_node_id": str(snapshot.get("root_node_id") or ""),
        "status": str(snapshot.get("status") or ""),
        "node_graph": node_graph,
        "node_lifecycle": node_lifecycle,
        "effective_requirements": [str(item) for item in (snapshot.get("active_requirements") or [])],
        "budget_view": dict(snapshot.get("complexity_budget") or {}),
        "blocked_reasons": dict(snapshot.get("blocked_reasons") or {}),
        "delegation_map": dict(snapshot.get("delegation_map") or {}),
        "active_evaluator_lanes": list(snapshot.get("active_evaluator_lanes") or []),
        "active_child_nodes": active_child_nodes,
        "planned_child_nodes": planned_child_nodes,
    }
    validate_repo_object("LoopSystemAuthorityView.schema.json", authority_view)
    return authority_view
