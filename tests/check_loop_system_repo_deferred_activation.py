#!/usr/bin/env python3
"""Validate kernel-owned activation of deferred split children."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-deferred-activation][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_source_node():
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="formalize the primary theorem statement from the current paper",
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
        task_id="wave11-deferred-activation",
        root_goal="activate deferred children only after dependency completion",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise deferred activation",
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


def _mark_source_completed(state_root: Path, node_id: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus

    authority = kernel_internal_authority()
    kernel_state = load_kernel_state(state_root)
    kernel_state.nodes[node_id]["status"] = NodeStatus.COMPLETED.value
    persist_node_snapshot(state_root, kernel_state.nodes[node_id], authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)


def _submit_deferred_split(state_root: Path) -> None:
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    source_node = _base_source_node()
    mutation = build_split_request(
        source_node=source_node,
        target_nodes=[
            {
                "node_id": "child-followup-001",
                "goal_slice": "formalize the deferred proof obligations after the primary theorem lands",
                "depends_on_node_ids": [source_node.node_id],
                "activation_condition": f"after:{source_node.node_id}:terminal",
            }
        ],
        split_mode="deferred",
        completed_work="primary theorem statement stays with current node",
        remaining_work="proof obligations defer to a child after the source reaches terminal state",
        reason="persist a deferred follow-up child for proof obligations",
    )
    envelope = submit_topology_mutation(
        state_root,
        mutation,
        round_id=source_node.round_id,
        generation=source_node.generation,
    )
    if envelope.status is not EnvelopeStatus.ACCEPTED:
        raise RuntimeError(f"deferred split setup must be accepted, got {envelope.status.value!r}")


def _accepted_activation_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.activate import build_activate_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_activation_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        _submit_deferred_split(state_root)
        _mark_source_completed(state_root, "child-implementer-001")

        mutation = build_activate_request(
            "child-followup-001",
            reason="source node completed, so the deferred child may now activate",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id="R1",
            generation=2,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"accepted activation path must return ACCEPTED, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority["node_graph"]}
        child_graph = node_graph.get("child-followup-001")
        if child_graph is None:
            return _fail("accepted activation must preserve the deferred child in authority view")
        if child_graph["status"] != NodeStatus.ACTIVE.value:
            return _fail("accepted activation must promote the deferred child to ACTIVE")
        if "child-followup-001" in authority["planned_child_nodes"]:
            return _fail("accepted activation must remove the child from planned_child_nodes")
        if "child-followup-001" not in authority["active_child_nodes"]:
            return _fail("accepted activation must expose the child in active_child_nodes")

        child_state_path = state_root / "state" / "child-followup-001.json"
        child_delegation_path = state_root / "state" / "delegations" / "child-followup-001.json"
        child_state = json.loads(child_state_path.read_text(encoding="utf-8"))
        child_delegation = json.loads(child_delegation_path.read_text(encoding="utf-8"))
        if child_state.get("status") != NodeStatus.ACTIVE.value:
            return _fail("accepted activation must persist ACTIVE node state")
        if child_state.get("depends_on_node_ids") != ["child-implementer-001"]:
            return _fail("accepted activation must preserve deferred dependency metadata")
        if child_state.get("activation_condition") != "after:child-implementer-001:terminal":
            return _fail("accepted activation must preserve deferred activation_condition metadata")
        if child_delegation.get("depends_on_node_ids") != ["child-implementer-001"]:
            return _fail("accepted activation must not rewrite frozen delegation dependency metadata")

    return 0


def _rejected_activation_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.activate import build_activate_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_activation_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        _submit_deferred_split(state_root)

        mutation = build_activate_request(
            "child-followup-001",
            reason="attempt activation before dependency completion",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id="R1",
            generation=2,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"activation before dependency completion must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority["node_graph"]}
        child_graph = node_graph.get("child-followup-001")
        if child_graph is None or child_graph["status"] != "PLANNED":
            return _fail("rejected activation must leave the deferred child PLANNED")

    return 0


def main() -> int:
    try:
        rc = _accepted_activation_case()
        if rc:
            return rc
        rc = _rejected_activation_case()
        if rc:
            return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"deferred activation runtime raised unexpectedly: {exc}")

    print("[loop-system-deferred-activation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
