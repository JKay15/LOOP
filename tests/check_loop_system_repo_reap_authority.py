#!/usr/bin/env python3
"""Validate kernel-approved reap authority for the standalone LOOP product repo."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-reap-authority][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_reap_state(state_root: Path, *, child_status: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave15-reap-authority",
        root_goal="prove kernel can retire terminal nodes from the live authoritative graph",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise reap authority",
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
    parent_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="own a leaf child that may later be reaped",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "merge_request"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/child-implementer-001/result.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-review-001",
        node_kind="reviewer",
        goal_slice="bounded review branch",
        parent_node_id=parent_node.node_id,
        generation=2,
        round_id="R1.S1",
        execution_policy={"sandbox_mode": "read-only"},
        reasoning_profile={"thinking_budget": "medium", "role": "reviewer"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["review", "report"],
        delegation_ref="state/delegations/child-review-001.json",
        result_sink_ref="artifacts/child-review-001/result.json",
        lineage_ref="root-kernel->child-implementer-001->child-review-001",
        status=NodeStatus(child_status),
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(parent_node)
    kernel_state.register_node(child_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _accepted_reap_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.topology import TopologyMutation

    with tempfile.TemporaryDirectory(prefix="loop_system_reap_authority_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_reap_state(state_root, child_status="COMPLETED")
        envelope = submit_topology_mutation(
            state_root,
            TopologyMutation.reap("child-review-001", reason="retire the completed review leaf from the live graph"),
            round_id="R1.S1",
            generation=2,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"terminal leaf reap must be accepted, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if any(node["node_id"] == "child-review-001" for node in authority["node_graph"]):
            return _fail("accepted reap must remove the node from the live authoritative graph")

        archive_path = state_root / "quarantine" / "reaped" / "child-review-001.json"
        if not archive_path.exists():
            return _fail("accepted reap must archive the retired node under .loop/quarantine/reaped/")
        archived = json.loads(archive_path.read_text(encoding="utf-8"))
        if archived.get("status") != "COMPLETED":
            return _fail("reaped node archive must preserve the terminal node status")

    return 0


def _rejected_reap_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.topology import TopologyMutation

    with tempfile.TemporaryDirectory(prefix="loop_system_reap_authority_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_reap_state(state_root, child_status="ACTIVE")
        envelope = submit_topology_mutation(
            state_root,
            TopologyMutation.reap("child-review-001", reason="active nodes must not be reaped"),
            round_id="R1.S1",
            generation=2,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"non-terminal node reap must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if not any(node["node_id"] == "child-review-001" for node in authority["node_graph"]):
            return _fail("rejected reap must leave the node in the live authoritative graph")

    return 0


def main() -> int:
    try:
        for case in (_accepted_reap_case, _rejected_reap_case):
            result = case()
            if result:
                return result
    except Exception as exc:  # noqa: BLE001
        return _fail(f"reap authority runtime raised unexpectedly: {exc}")

    print("[loop-system-reap-authority] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
