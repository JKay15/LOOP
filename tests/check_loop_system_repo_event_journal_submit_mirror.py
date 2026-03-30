#!/usr/bin/env python3
"""Validate compatibility mirroring from accepted control envelopes into the event journal."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-submit-mirror][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, load_kernel_state, persist_kernel_state
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate event-journal compatibility mirror",
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
        node_id="mirror-child-001",
        node_kind="implementer",
        goal_slice="exercise accepted-envelope journal mirroring",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "resume_request"],
        delegation_ref="state/delegations/mirror-child-001.json",
        result_sink_ref="artifacts/mirror-child-001/result.json",
        lineage_ref="root-kernel->mirror-child-001",
        status=NodeStatus.ACTIVE,
    )

    evaluator_result = EvaluatorResult(
        verdict=EvaluatorVerdict.FAIL,
        lane="reviewer",
        summary="The evaluator found a retryable implementation gap.",
        evidence_refs=["reviewer:mirror"],
        retryable=True,
        diagnostics={
            "self_attribution": "implementation_gap",
            "self_repair": "repair and rerun the evaluator",
        },
    )

    with temporary_repo_root(prefix="loop_system_event_journal_submit_mirror_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-submit-mirror",
            root_goal="validate event-journal compatibility mirror",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        accepted = submit_control_envelope(
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
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("submit_control_envelope must still accept the evaluator result")

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted envelope must stay transport-accepted")

        loaded_state = load_kernel_state(state_root)
        if loaded_state.to_dict().get("accepted_envelope_count") != 1:
            return _fail("compatibility mirroring must not change accepted-envelope count semantics")
        if committed_event_count(state_root) != 2:
            return _fail("accepted evaluator_result under replay must produce exactly two committed events")

        events = iter_committed_events(state_root)
        if len(events) != 2:
            return _fail("one compatibility mirror and one canonical evaluator event are expected")
        compatibility = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "control_envelope_accepted"),
            {},
        )
        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "evaluator_result_recorded"),
            {},
        )
        if not compatibility:
            return _fail("accepted evaluator envelope must still emit the compatibility control_envelope_accepted mirror")
        if not canonical:
            return _fail("accepted evaluator envelope must also emit one canonical evaluator_result_recorded fact")
        payload = dict(compatibility.get("payload") or {})
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("mirrored compatibility payload must reference the accepted envelope identity")
        if str(payload.get("classification") or "") != "evaluator":
            return _fail("mirrored compatibility payload must preserve the accepted classification")
        canonical_payload = dict(canonical.get("payload") or {})
        if str(canonical_payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical evaluator payload must reference the accepted envelope identity")
        if str(canonical_payload.get("verdict") or "") != "FAIL":
            return _fail("canonical evaluator payload must preserve the evaluator verdict")

    print("[loop-system-event-journal-submit-mirror][OK] accepted envelopes mirror into the journal without changing kernel behavior")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
