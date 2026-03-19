#!/usr/bin/env python3
"""Validate kernel-approved deferred merge authority for completed source lanes."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-deferred-merge-authority][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_deferred_merge_ready_state(state_root: Path, *, child_status: str = "COMPLETED") -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus, RuntimeAttachmentState

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="deferred-merge-authority",
        root_goal="prove completed source lanes can converge completed deferred child work back into the source lane",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise deferred merge authority",
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
        node_id="child-formalizer-001",
        node_kind="implementer",
        goal_slice="formalize the primary theorem statement",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "high", "role": "implementer"},
        budget_profile={"max_rounds": 3},
        allowed_actions=["implement", "evaluate", "report", "split_request", "merge_request"],
        delegation_ref="state/delegations/child-formalizer-001.json",
        result_sink_ref="artifacts/child-formalizer-001/result.json",
        lineage_ref="root-kernel->child-formalizer-001",
        runtime_state={
            "attachment_state": RuntimeAttachmentState.TERMINAL.value,
            "observed_at": "2026-03-19T00:00:00Z",
            "summary": "the source lane already reached a terminal state before deferred merge",
            "observation_kind": "terminal_result",
            "evidence_refs": ["control_envelope:source-terminal"],
        },
        status=NodeStatus.COMPLETED,
    )
    child_node = NodeSpec(
        node_id="child-proof-followup-001",
        node_kind="implementer",
        goal_slice="formalize the deferred proof obligations",
        parent_node_id=source_node.node_id,
        generation=2,
        round_id="R1.S1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "high", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-proof-followup-001.json",
        result_sink_ref="artifacts/child-proof-followup-001/result.json",
        lineage_ref="root-kernel->child-formalizer-001->child-proof-followup-001",
        depends_on_node_ids=[source_node.node_id],
        activation_condition=f"after:{source_node.node_id}:completed",
        status=NodeStatus(child_status),
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    kernel_state.register_node(child_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _accepted_deferred_merge_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.merge import build_merge_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_merge_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_deferred_merge_ready_state(state_root)
        mutation = build_merge_request(
            "child-formalizer-001",
            ["child-proof-followup-001"],
            reason="the completed deferred proof child can now converge back into the completed source lane",
        )
        envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"completed source + completed deferred child must yield ACCEPTED merge, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        source = next(item for item in authority["node_graph"] if item["node_id"] == "child-formalizer-001")
        if authority["node_lifecycle"].get("child-formalizer-001") != "ACTIVE":
            return _fail("accepted deferred merge must reactivate the completed source node")
        if str(source.get("runtime_state", {}).get("attachment_state") or "") != "UNOBSERVED":
            return _fail("accepted deferred merge must reset source runtime attachment to UNOBSERVED")
        if "child-proof-followup-001" in authority["active_child_nodes"]:
            return _fail("completed deferred child must not appear as active after accepted merge")

        audit_view = query_audit_experience_view(state_root, node_id="child-formalizer-001", limit=10)
        if not any("merge" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
            return _fail("accepted deferred merge must appear in recent structural decisions")

    return 0


def _rejected_deferred_merge_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.merge import build_merge_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_merge_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_deferred_merge_ready_state(state_root, child_status="ACTIVE")
        mutation = build_merge_request(
            "child-formalizer-001",
            ["child-proof-followup-001"],
            reason="an active deferred child must still block merge acceptance",
        )
        envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"non-terminal deferred child must reject merge, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get("child-formalizer-001") != "COMPLETED":
            return _fail("rejected deferred merge must leave the source node COMPLETED")

    return 0


def main() -> int:
    try:
        for case in (_accepted_deferred_merge_case, _rejected_deferred_merge_case):
            rc = case()
            if rc:
                return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"deferred merge authority runtime raised unexpectedly: {exc}")

    print("[loop-system-deferred-merge-authority] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
