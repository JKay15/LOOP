#!/usr/bin/env python3
"""Validate kernel accepted-envelope idempotence for replayed accepted envelopes."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-kernel-envelope-idempotence][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, load_kernel_state, persist_kernel_state
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate accepted envelope idempotence",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="exercise accepted envelope replay semantics",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "resume_request"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/child_result.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )

    evaluator_result = EvaluatorResult(
        verdict=EvaluatorVerdict.FAIL,
        lane="reviewer",
        summary="The evaluator found a retryable implementation gap.",
        evidence_refs=["reviewer:round-1"],
        retryable=True,
        diagnostics={
            "self_attribution": "implementation_gap",
            "self_repair": "repair and rerun the evaluator",
        },
    )

    with temporary_repo_root(prefix="loop_system_kernel_envelope_idempotence_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        authority = kernel_internal_authority()
        kernel_state = KernelState(
            task_id="kernel-envelope-idempotence",
            root_goal="validate accepted envelope replay idempotence",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=authority)

        accepted_result = submit_control_envelope(
            state_root,
            ControlEnvelope(
                source=child_node.node_id,
                envelope_type="evaluator_result",
                round_id="R1.1",
                generation=child_node.generation,
                payload=evaluator_result.to_dict(),
                status=EnvelopeStatus.REPORT,
                note=evaluator_result.summary,
            ),
        )
        if accepted_result.status is not EnvelopeStatus.ACCEPTED:
            return _fail("initial evaluator_result must be accepted")

        blocked_state = load_kernel_state(state_root)
        blocked_state.nodes[child_node.node_id]["status"] = NodeStatus.BLOCKED.value
        blocked_state.blocked_reasons[child_node.node_id] = "waiting on repaired implementation"
        persist_kernel_state(state_root, blocked_state, authority=authority)

        accepted_resume = submit_control_envelope(
            state_root,
            ControlEnvelope(
                source=child_node.node_id,
                envelope_type="resume_request",
                payload_type="topology_mutation",
                round_id="R1.2",
                generation=child_node.generation,
                payload=TopologyMutation.resume(
                    child_node.node_id,
                    reason="the same node can continue after a bounded repair",
                    payload={"consistency_signal": "implementation_gap_repaired"},
                ).to_dict(),
                status=EnvelopeStatus.PROPOSAL,
                note="resume after retryable evaluator fail",
            ),
        )
        if accepted_resume.status is not EnvelopeStatus.ACCEPTED:
            return _fail("initial resume_request must be accepted")

        replayed_result = submit_control_envelope(state_root, accepted_result)
        if replayed_result.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted evaluator_result must still be transport-accepted")

        replayed_resume = submit_control_envelope(state_root, accepted_resume)
        if replayed_resume.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted resume_request must still be transport-accepted")

        loaded_state = load_kernel_state(state_root)
        accepted = list(loaded_state.accepted_envelopes)
        accepted_types = [str(item.get("envelope_type") or "") for item in accepted]
        accepted_ids = [str(item.get("envelope_id") or "") for item in accepted]
        if len(accepted) != 2:
            return _fail("replaying accepted envelopes must not append duplicate accepted facts")
        if len(accepted_ids) != len(set(accepted_ids)):
            return _fail("accepted-fact ledger must not contain duplicate envelope ids")
        if accepted_types.count("evaluator_result") != 1:
            return _fail("accepted evaluator_result must persist exactly once even when replayed")
        if accepted_types.count("resume_request") != 1:
            return _fail("accepted resume_request must persist exactly once even when replayed")
        if str(loaded_state.nodes[child_node.node_id].get("status") or "") != NodeStatus.ACTIVE.value:
            return _fail("replayed accepted resume_request must not disturb the already resumed ACTIVE node")
        if loaded_state.blocked_reasons.get(child_node.node_id):
            return _fail("replayed accepted resume_request must not reintroduce the cleared blocked reason")
        if len(loaded_state.active_evaluator_lanes) != 1:
            return _fail("replayed accepted evaluator_result must not duplicate active evaluator lane records")
        if len(loaded_state.experience_assets) != 1:
            return _fail("replayed accepted evaluator_result must not duplicate self-repair experience assets")

        kernel_snapshot = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        if kernel_snapshot.get("accepted_envelope_count") != 2:
            return _fail("accepted_envelope_count must reflect unique accepted facts rather than replay count")

    print("[loop-system-kernel-envelope-idempotence][OK] accepted envelope replay stays idempotent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
