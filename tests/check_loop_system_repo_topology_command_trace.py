#!/usr/bin/env python3
"""Validate read-only topology command trace over command, event, and projection history."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-topology-command-trace][FAIL] {msg}", file=sys.stderr)
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
        task_id="milestone4-topology-command-trace",
        root_goal="validate topology command trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise topology command trace validation",
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


def _build_deferred_split_request():
    from loop_product.topology.split_review import build_split_request

    source_node = _base_source_node()
    return source_node, build_split_request(
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


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_authority_view, query_topology_command_trace_view
        from loop_product.kernel.submit import submit_topology_mutation
        from loop_product.kernel.topology import review_topology_mutation
        from loop_product.kernel.state import load_kernel_state
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_topology_command_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        source_node, mutation = _build_deferred_split_request()
        accepted = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"accepted topology mutation required for trace, got {accepted.status.value!r}")
        _ = query_authority_view(state_root)
        event_count_before = committed_event_count(state_root)
        trace = query_topology_command_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("topology command trace query must stay read-only and not mutate committed event history")
        if not bool(trace.get("read_only")):
            return _fail("topology command trace must declare itself read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("trace must report the canonical topology command event")
        if str(trace.get("command_kind") or "") != "split":
            return _fail("trace must expose the accepted topology command kind")
        if str(trace.get("projection_visibility") or "") != "atomically_visible":
            return _fail("trace must expose the current projection visibility without mutating it")
        projection_effect = dict(trace.get("projection_effect") or {})
        source_projection = dict(projection_effect.get("source_node") or {})
        target_nodes = list(projection_effect.get("target_nodes") or [])
        source_lineage = dict(trace.get("source_node_lineage") or {})
        target_lineages = [dict(item or {}) for item in list(trace.get("target_node_lineages") or [])]
        if str(source_projection.get("status") or "") != "ACTIVE":
            return _fail("trace must expose the visible projected source-node status")
        if len(target_nodes) != 1 or str(dict(target_nodes[0]).get("status") or "") != "PLANNED":
            return _fail("trace must expose the visible projected deferred child status")
        if not bool(source_lineage.get("read_only")):
            return _fail("trace must expose the source node through the nested node-lineage surface")
        if str(source_lineage.get("node_id") or "") != "child-implementer-001":
            return _fail("source node lineage must stay anchored to the visible source node")
        if len(target_lineages) != 1 or str(target_lineages[0].get("node_id") or "") != "child-followup-001":
            return _fail("trace must expose one nested node-lineage view for the visible deferred child")
        if list(trace.get("gaps") or []):
            return _fail("complete topology command trace must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_topology_command_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        source_node, mutation = _build_deferred_split_request()
        kernel_state = load_kernel_state(state_root)
        review = review_topology_mutation(kernel_state, mutation, state_root=state_root)
        envelope = mutation.to_envelope(
            round_id=source_node.round_id,
            generation=source_node.generation,
            status=EnvelopeStatus.PROPOSAL,
            note=str(review.get("summary") or mutation.reason or ""),
        )
        envelope.payload = {
            "topology_mutation": mutation.to_dict(),
            "review": review,
        }
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted topology envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")
        event_count_before = committed_event_count(state_root)
        gap_trace = query_topology_command_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("gap trace query must stay read-only even when canonical facts are missing")
        if bool(gap_trace.get("canonical_event_present")):
            return _fail("gap trace must not invent a canonical topology command event")
        gaps = list(gap_trace.get("gaps") or [])
        if "missing_canonical_topology_command_event" not in gaps:
            return _fail("gap trace must surface missing canonical topology command event")
        if "projection_not_materialized" not in gaps:
            return _fail("gap trace must surface the missing projected topology side effect")

    print("[loop-system-topology-command-trace][OK] topology command trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
