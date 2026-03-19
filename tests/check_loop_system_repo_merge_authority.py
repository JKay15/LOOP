#!/usr/bin/env python3
"""Validate kernel-approved merge authority for the standalone LOOP product repo."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-merge-authority][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_merge_ready_state(state_root: Path, *, child_b_status: str = "COMPLETED") -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave14-merge-authority",
        root_goal="prove blocked source nodes can merge completed child branches back into mainline",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise merge authority",
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
    source_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="converge bounded child branches back into the main lane",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "split_request", "merge_request"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/child-implementer-001/result.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.BLOCKED,
    )
    child_a = NodeSpec(
        node_id="child-review-001",
        node_kind="reviewer",
        goal_slice="review branch",
        parent_node_id=source_node.node_id,
        generation=2,
        round_id="R1.S1",
        execution_policy={"sandbox_mode": "read-only"},
        reasoning_profile={"thinking_budget": "medium", "role": "reviewer"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["review", "report"],
        delegation_ref="state/delegations/child-review-001.json",
        result_sink_ref="artifacts/child-review-001/result.json",
        lineage_ref="root-kernel->child-implementer-001->child-review-001",
        status=NodeStatus.COMPLETED,
    )
    child_b = NodeSpec(
        node_id="child-repair-001",
        node_kind="implementer",
        goal_slice="repair branch",
        parent_node_id=source_node.node_id,
        generation=2,
        round_id="R1.S2",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "high", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-repair-001.json",
        result_sink_ref="artifacts/child-repair-001/result.json",
        lineage_ref="root-kernel->child-implementer-001->child-repair-001",
        status=NodeStatus(child_b_status),
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    kernel_state.register_node(child_a)
    kernel_state.register_node(child_b)
    kernel_state.blocked_reasons[source_node.node_id] = "waiting for child branches to converge"
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _accepted_merge_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.merge import build_merge_request

    with tempfile.TemporaryDirectory(prefix="loop_system_merge_authority_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_merge_ready_state(state_root)
        mutation = build_merge_request(
            "child-implementer-001",
            ["child-review-001", "child-repair-001"],
            reason="both child branches completed and can converge back into the source node",
        )
        envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"merge-ready child branches must yield ACCEPTED merge, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get("child-implementer-001") != "ACTIVE":
            return _fail("accepted merge must reactivate the blocked source node")
        if "child-implementer-001" in authority["blocked_reasons"]:
            return _fail("accepted merge must clear the source blocked reason")

        audit_view = query_audit_experience_view(state_root, node_id="child-implementer-001", limit=10)
        if not any("merge" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
            return _fail("accepted merge must appear in recent structural decisions")

    return 0


def _rejected_merge_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.merge import build_merge_request

    with tempfile.TemporaryDirectory(prefix="loop_system_merge_authority_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_merge_ready_state(state_root, child_b_status="ACTIVE")
        mutation = build_merge_request(
            "child-implementer-001",
            ["child-review-001", "child-repair-001"],
            reason="an active child must block merge acceptance",
        )
        envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"non-terminal child branch must reject merge, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get("child-implementer-001") != "BLOCKED":
            return _fail("rejected merge must leave the source node BLOCKED")
        if authority["blocked_reasons"].get("child-implementer-001") != "waiting for child branches to converge":
            return _fail("rejected merge must preserve the existing blocked reason")

    return 0


def main() -> int:
    try:
        for case in (_accepted_merge_case, _rejected_merge_case):
            result = case()
            if result:
                return result
    except Exception as exc:  # noqa: BLE001
        return _fail(f"merge authority runtime raised unexpectedly: {exc}")

    print("[loop-system-merge-authority] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
