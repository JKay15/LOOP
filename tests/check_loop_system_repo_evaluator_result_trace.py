#!/usr/bin/env python3
"""Validate read-only evaluator-result trace over audit, events, and projection history."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-result-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-evaluator-result-trace",
        root_goal="validate evaluator-result trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise evaluator-result trace validation",
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
    child_node = NodeSpec(
        node_id="child-evaluator-001",
        node_kind="implementer",
        goal_slice="exercise evaluator-result trace coverage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-evaluator-001.json",
        result_sink_ref="artifacts/child-evaluator-001/result.json",
        lineage_ref="root-kernel->child-evaluator-001",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(child_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_authority_view, query_evaluator_result_trace_view
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_evaluator_result_trace_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        evaluator_result = EvaluatorResult(
            verdict=EvaluatorVerdict.FAIL,
            lane="reviewer",
            summary="The evaluator found a retryable implementation gap.",
            evidence_refs=["reviewer:milestone4-evaluator"],
            retryable=True,
            diagnostics={
                "self_attribution": "implementation_gap",
                "self_repair": "repair and rerun the evaluator",
            },
        )
        accepted = submit_control_envelope(
            state_root,
            ControlEnvelope(
                source="child-evaluator-001",
                envelope_type="evaluator_result",
                round_id="R1.eval",
                generation=1,
                payload=evaluator_result.to_dict(),
                status=EnvelopeStatus.REPORT,
                note=evaluator_result.summary,
            ),
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("evaluator trace scenario requires an accepted evaluator result")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        trace = query_evaluator_result_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("evaluator-result trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("evaluator-result trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("evaluator-result trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("evaluator-result trace must report the canonical evaluator-result event")
        source_projection = dict(trace.get("source_node") or {})
        source_lineage = dict(trace.get("source_node_lineage") or {})
        active_lanes = list(trace.get("active_evaluator_lanes") or [])
        canonical = dict(trace.get("canonical_event") or {})
        if str(source_projection.get("node_id") or "") != "child-evaluator-001":
            return _fail("evaluator-result trace must expose the source node id")
        if str(source_projection.get("status") or "") != "ACTIVE":
            return _fail("evaluator-result trace must expose the visible source node status")
        if len(active_lanes) != 1:
            return _fail("evaluator-result trace must expose the visible active evaluator lane effect")
        lane = dict(active_lanes[0] or {})
        if str(lane.get("lane") or "") != "reviewer":
            return _fail("evaluator-result trace must expose the evaluator lane")
        if str(lane.get("verdict") or "") != "FAIL":
            return _fail("evaluator-result trace must expose the evaluator verdict")
        if str(canonical.get("self_attribution") or "") != "implementation_gap":
            return _fail("evaluator-result trace must expose self-attribution through the canonical event summary")
        if str(canonical.get("self_repair") or "") != "repair and rerun the evaluator":
            return _fail("evaluator-result trace must expose self-repair through the canonical event summary")
        if not bool(source_lineage.get("read_only")):
            return _fail("evaluator-result trace must expose the source node through nested node-lineage")
        if str(source_lineage.get("node_id") or "") != "child-evaluator-001":
            return _fail("evaluator-result trace nested source lineage must stay anchored to the visible source node")
        if list(trace.get("gaps") or []):
            return _fail("accepted evaluator-result trace must not report causal gaps")

    print("[loop-system-evaluator-result-trace][OK] evaluator-result trace stays read-only and exposes the visible evaluator projection effect")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
