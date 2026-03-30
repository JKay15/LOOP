#!/usr/bin/env python3
"""Validate read-only relaunch command trace over command, event, and projection history."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-relaunch-command-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _make_source_node(*, node_id: str, status: str):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id=node_id,
        node_kind="implementer",
        goal_slice="recover the current bounded implementation lane",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "resume_request", "retry_request", "relaunch_request"],
        delegation_ref=f"state/delegations/{node_id}.json",
        result_sink_ref=f"artifacts/{node_id}/result.json",
        lineage_ref=f"root-kernel->{node_id}",
        status=NodeStatus(status),
    )


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-relaunch-command-trace",
        root_goal="validate relaunch command trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise relaunch command trace validation",
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
    source_node = _make_source_node(node_id="child-relaunch-001", status="BLOCKED")
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    kernel_state.blocked_reasons[source_node.node_id] = "staged workspace is inconsistent and needs a fresh launch"
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_authority_view, query_topology_command_trace_view
        from loop_product.kernel.submit import submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime.recover import build_relaunch_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_relaunch_command_trace_") as repo_root:
        state_root = repo_root / ".loop"
        source_node = _make_source_node(node_id="child-relaunch-001", status="BLOCKED")
        _persist_base_state(state_root)
        accepted = submit_topology_mutation(
            state_root,
            build_relaunch_request(
                source_node,
                replacement_node_id="child-relaunch-001-v2",
                reason="materialize a fresh replacement node with refreshed execution policy",
                self_attribution="staged_workspace_corruption",
                self_repair="launch a fresh child from durable delegation instead of blaming implementation",
                execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "single_retry"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 3},
            ),
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("relaunch trace scenario requires an accepted relaunch request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        trace = query_topology_command_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("relaunch command trace must stay read-only")
        if str(trace.get("command_kind") or "") != "relaunch":
            return _fail("relaunch trace must expose command_kind=relaunch")
        projection_effect = dict(trace.get("projection_effect") or {})
        source_projection = dict(projection_effect.get("source_node") or {})
        target_nodes = [dict(item) for item in list(projection_effect.get("target_nodes") or [])]
        source_lineage = dict(trace.get("source_node_lineage") or {})
        target_lineages = [dict(item or {}) for item in list(trace.get("target_node_lineages") or [])]
        canonical = dict(trace.get("canonical_event") or {})
        if str(source_projection.get("node_id") or "") != "child-relaunch-001":
            return _fail("relaunch trace must expose the superseded source node")
        if str(source_projection.get("status") or "") != "FAILED":
            return _fail("relaunch trace must expose the superseded source node as FAILED")
        if len(target_nodes) != 1:
            return _fail("relaunch trace must expose exactly one replacement target node")
        replacement = dict(target_nodes[0] or {})
        if str(replacement.get("node_id") or "") != "child-relaunch-001-v2":
            return _fail("relaunch trace must expose the replacement target node id")
        if str(replacement.get("status") or "") != "ACTIVE":
            return _fail("relaunch trace must expose the replacement target node as ACTIVE")
        if list(canonical.get("target_node_ids") or []) != ["child-relaunch-001-v2"]:
            return _fail("relaunch trace canonical event summary must expose the replacement target id")
        if str(canonical.get("self_attribution") or "") != "staged_workspace_corruption":
            return _fail("relaunch trace must expose self-attribution through the canonical event summary")
        if str(canonical.get("self_repair") or "") != "launch a fresh child from durable delegation instead of blaming implementation":
            return _fail("relaunch trace must expose self-repair through the canonical event summary")
        if not bool(source_lineage.get("read_only")):
            return _fail("relaunch trace must expose the superseded source through nested node-lineage")
        if str(source_lineage.get("node_id") or "") != "child-relaunch-001":
            return _fail("relaunch trace nested source lineage must stay anchored to the superseded node")
        if len(target_lineages) != 1 or str(target_lineages[0].get("node_id") or "") != "child-relaunch-001-v2":
            return _fail("relaunch trace must expose one nested node-lineage view for the replacement target")
        if list(trace.get("gaps") or []):
            return _fail("accepted relaunch trace must not report causal gaps")

    print("[loop-system-relaunch-command-trace][OK] relaunch command trace stays read-only and exposes both superseded source and replacement projection")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
