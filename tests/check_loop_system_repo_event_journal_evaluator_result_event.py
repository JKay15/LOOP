#!/usr/bin/env python3
"""Validate canonical accepted evaluator-result events for Milestone 4."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-evaluator-result-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-evaluator-result-event",
        root_goal="validate canonical accepted evaluator-result events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise evaluator-result event validation",
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
        goal_slice="exercise evaluator-result event coverage",
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
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_evaluator_result_event_") as repo_root:
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
            return _fail(f"evaluator result must be accepted, got {accepted.status.value!r}")

        replayed = submit_control_envelope(state_root, accepted)
        if replayed.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted evaluator envelope must stay transport-accepted")
        if committed_event_count(state_root) != 2:
            return _fail("accepted evaluator result under replay must produce exactly two committed events")

        canonical = [
            dict(event)
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "evaluator_result_recorded"
        ]
        if len(canonical) != 1:
            return _fail("accepted evaluator result must emit exactly one canonical evaluator-result event")
        payload = dict(canonical[0].get("payload") or {})
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical evaluator-result event must preserve the accepted envelope id")
        if str(payload.get("node_id") or "") != "child-evaluator-001":
            return _fail("canonical evaluator-result event must preserve the node id")
        if str(payload.get("lane") or "") != "reviewer":
            return _fail("canonical evaluator-result event must preserve the evaluator lane")
        if str(payload.get("verdict") or "") != "FAIL":
            return _fail("canonical evaluator-result event must preserve the evaluator verdict")
        if bool(payload.get("retryable")) is not True:
            return _fail("canonical evaluator-result event must preserve retryability")
        if str(payload.get("self_attribution") or "") != "implementation_gap":
            return _fail("canonical evaluator-result event must preserve self-attribution evidence")
        if str(payload.get("self_repair") or "") != "repair and rerun the evaluator":
            return _fail("canonical evaluator-result event must preserve self-repair evidence")

    print("[loop-system-event-journal-evaluator-result-event][OK] accepted evaluator results emit one canonical evaluator-result event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
