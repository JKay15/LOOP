#!/usr/bin/env python3
"""Validate kernel-approved split authority for the standalone LOOP product repo."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-split-authority][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_source_node():
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="deliver the current bounded endpoint bundle",
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
        task_id="wave11-split-authority",
        root_goal="prove node proposes and kernel decides split mutations",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise split authority",
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


def _accepted_split_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_split_authority_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-review-001",
                    "node_kind": "reviewer",
                    "goal_slice": "audit the risk-heavy branch in bounded scope",
                    "execution_policy": {"provider_selection": "codex_cli", "sandbox_mode": "read-only"},
                },
                {
                    "node_id": "child-repair-001",
                    "goal_slice": "repair the high-risk implementation branch with a higher thinking budget",
                    "execution_policy": {"provider_selection": "codex_cli", "sandbox_mode": "workspace-write"},
                    "reasoning_profile": {"thinking_budget": "high", "role": "implementer"},
                    "budget_profile": {"max_rounds": 3},
                },
            ],
            reason="split validation and repair into separate bounded child nodes",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"accepted split path must return ACCEPTED, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority["node_graph"]}
        for child_id in ("child-review-001", "child-repair-001"):
            if child_id not in node_graph:
                return _fail(f"accepted split must materialize child node {child_id}")
            if node_graph[child_id]["parent_node_id"] != source_node.node_id:
                return _fail(f"split child {child_id} must inherit source parentage")
            if node_graph[child_id]["generation"] != source_node.generation + 1:
                return _fail(f"split child {child_id} must increment generation")
        if authority["node_lifecycle"].get(source_node.node_id) != NodeStatus.BLOCKED.value:
            return _fail("accepted split must block the source node while child nodes take over")
        if source_node.node_id not in authority["blocked_reasons"]:
            return _fail("accepted split must preserve a blocked reason for the source node")
        if "child-review-001" not in authority["active_child_nodes"] or "child-repair-001" not in authority["active_child_nodes"]:
            return _fail("accepted split must expose active split children in authority view")

        review_node_state = state_root / "state" / "child-review-001.json"
        repair_node_state = state_root / "state" / "child-repair-001.json"
        if not review_node_state.exists() or not repair_node_state.exists():
            return _fail("accepted split must persist split child node snapshots under .loop/state/")
        review_node = json.loads(review_node_state.read_text(encoding="utf-8"))
        repair_node = json.loads(repair_node_state.read_text(encoding="utf-8"))
        if review_node.get("execution_policy", {}).get("agent_provider") != "codex_cli":
            return _fail("accepted split child node state must canonicalize provider_selection to execution_policy.agent_provider")
        if repair_node.get("execution_policy", {}).get("agent_provider") != "codex_cli":
            return _fail("accepted split repair child node state must canonicalize provider_selection to execution_policy.agent_provider")
        if "provider_selection" in review_node.get("execution_policy", {}):
            return _fail("accepted split child node state must not persist legacy provider_selection keys")
        if "provider_selection" in repair_node.get("execution_policy", {}):
            return _fail("accepted split repair child node state must not persist legacy provider_selection keys")
        review_delegation = state_root / "state" / "delegations" / "child-review-001.json"
        repair_delegation = state_root / "state" / "delegations" / "child-repair-001.json"
        if not review_delegation.exists() or not repair_delegation.exists():
            return _fail("accepted split must freeze split child delegations")
        review_delegation_data = json.loads(review_delegation.read_text(encoding="utf-8"))
        repair_delegation_data = json.loads(repair_delegation.read_text(encoding="utf-8"))
        if review_delegation_data.get("execution_policy", {}).get("agent_provider") != "codex_cli":
            return _fail("accepted split child delegation must canonicalize provider_selection to execution_policy.agent_provider")
        if repair_delegation_data.get("execution_policy", {}).get("agent_provider") != "codex_cli":
            return _fail("accepted split repair child delegation must canonicalize provider_selection to execution_policy.agent_provider")
        if "provider_selection" in review_delegation_data.get("execution_policy", {}):
            return _fail("accepted split child delegation must not persist legacy provider_selection keys")
        if "provider_selection" in repair_delegation_data.get("execution_policy", {}):
            return _fail("accepted split repair child delegation must not persist legacy provider_selection keys")

        audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
        if not any("split" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
            return _fail("accepted split must appear in recent structural decisions")

    return 0


def _rejected_split_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_split_authority_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {"node_id": "child-a", "goal_slice": "branch a"},
                {"node_id": "child-b", "goal_slice": "branch b"},
                {"node_id": "child-c", "goal_slice": "branch c"},
            ],
            reason="attempt an over-budget split that should be rejected",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"over-budget split must be rejected, got {envelope.status.value!r}")
        authority = query_authority_view(state_root)
        if any(node_id in authority["node_lifecycle"] for node_id in ("child-a", "child-b", "child-c")):
            return _fail("rejected split must not materialize child nodes")
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("rejected split must leave the source node active")

    return 0


def main() -> int:
    try:
        _accepted_split_case()
        _rejected_split_case()
    except Exception as exc:  # noqa: BLE001
        return _fail(f"split authority runtime raised unexpectedly: {exc}")

    print("[loop-system-split-authority] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
