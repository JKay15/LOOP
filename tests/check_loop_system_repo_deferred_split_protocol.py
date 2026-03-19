#!/usr/bin/env python3
"""Validate deferred split protocol and planned-child authority semantics."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-deferred-split][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_source_node():
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="finish the primary birthday poster flow",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "split_request"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/child-implementer-001/result.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave11-deferred-split-protocol",
        root_goal="persist deferred split children without activating them",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise deferred split protocol",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"thinking_budget": "medium", "role": "kernel"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/kernel.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(_base_source_node())
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_split_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-followup-001",
                    "goal_slice": "prepare the follow-up artifact pack after the primary poster is finished",
                    "depends_on_node_ids": [source_node.node_id],
                    "activation_condition": f"after:{source_node.node_id}:terminal",
                }
            ],
            split_mode="deferred",
            completed_work="the primary poster scope remains with the current implementer",
            remaining_work="the follow-up artifact pack should wait until the source node reaches terminal state",
            reason="persist a deferred follow-up branch without activating it yet",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"deferred split must be accepted, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority["node_graph"]}
        source_graph = node_graph.get(source_node.node_id)
        deferred_child = node_graph.get("child-followup-001")
        if source_graph is None or deferred_child is None:
            return _fail("accepted deferred split must preserve the source node and materialize the deferred child")
        if source_graph["status"] != NodeStatus.ACTIVE.value:
            return _fail("accepted deferred split must keep the source node ACTIVE")
        if deferred_child["status"] != NodeStatus.PLANNED.value:
            return _fail("accepted deferred split must persist deferred children as PLANNED")
        if "child-followup-001" in authority["active_child_nodes"]:
            return _fail("planned deferred children must not appear in active_child_nodes")
        if "child-followup-001" not in authority["planned_child_nodes"]:
            return _fail("authority view must expose planned deferred children separately")

        child_state_path = state_root / "state" / "child-followup-001.json"
        child_delegation_path = state_root / "state" / "delegations" / "child-followup-001.json"
        if not child_state_path.exists() or not child_delegation_path.exists():
            return _fail("accepted deferred split must persist child state and delegation artifacts")
        child_state = json.loads(child_state_path.read_text(encoding="utf-8"))
        child_delegation = json.loads(child_delegation_path.read_text(encoding="utf-8"))
        if child_state.get("depends_on_node_ids") != [source_node.node_id]:
            return _fail("deferred child node state must persist depends_on_node_ids")
        if child_state.get("activation_condition") != f"after:{source_node.node_id}:terminal":
            return _fail("deferred child node state must persist activation_condition")
        if child_delegation.get("depends_on_node_ids") != [source_node.node_id]:
            return _fail("deferred child delegation must persist depends_on_node_ids")
        if child_delegation.get("activation_condition") != f"after:{source_node.node_id}:terminal":
            return _fail("deferred child delegation must persist activation_condition")

    print("[loop-system-deferred-split] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
