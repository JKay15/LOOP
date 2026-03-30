#!/usr/bin/env python3
"""Validate canonical accepted recovery-command events for Milestone 4."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-recovery-command-event][FAIL] {msg}", file=sys.stderr)
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
        task_id="milestone4-recovery-command-event",
        root_goal="validate canonical accepted recovery command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise recovery command event validation",
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
    resume_node = _make_source_node(node_id="child-blocked-001", status="BLOCKED")
    retry_node = _make_source_node(node_id="child-retry-001", status="FAILED")
    kernel_state.register_node(root_node)
    kernel_state.register_node(resume_node)
    kernel_state.register_node(retry_node)
    kernel_state.blocked_reasons[resume_node.node_id] = "waiting on an external prerequisite that is now repaired"
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime.recover import build_resume_request, build_retry_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_recovery_command_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        resume_node = _make_source_node(node_id="child-blocked-001", status="BLOCKED")
        retry_node = _make_source_node(node_id="child-retry-001", status="FAILED")

        accepted_resume = submit_topology_mutation(
            state_root,
            build_resume_request(
                resume_node,
                reason="external prerequisite repaired and the same node can continue",
                consistency_signal="dependency_unblocked",
            ),
            round_id=resume_node.round_id,
            generation=resume_node.generation,
        )
        if accepted_resume.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"resume request must be accepted, got {accepted_resume.status.value!r}")

        accepted_retry = submit_topology_mutation(
            state_root,
            build_retry_request(
                retry_node,
                reason="the evaluator found a self-caused path issue and can retry safely",
                self_attribution="evaluator_path_error",
                self_repair="fix the staged command path before retrying the same node",
            ),
            round_id=retry_node.round_id,
            generation=retry_node.generation,
        )
        if accepted_retry.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"retry request must be accepted, got {accepted_retry.status.value!r}")

        replayed_resume = submit_control_envelope(state_root, accepted_resume)
        replayed_retry = submit_control_envelope(state_root, accepted_retry)
        if replayed_resume.status is not EnvelopeStatus.ACCEPTED or replayed_retry.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted recovery envelopes must stay transport-accepted")

        if committed_event_count(state_root) != 4:
            return _fail("accepted resume+retry must produce exactly four committed events under replay")

        canonical = [
            dict(event)
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "topology_command_accepted"
        ]
        if len(canonical) != 2:
            return _fail("accepted resume+retry must emit exactly two canonical topology-command events")

        by_kind = {
            str(dict(event.get("payload") or {}).get("command_kind") or ""): dict(event.get("payload") or {})
            for event in canonical
        }
        if set(by_kind) != {"resume", "retry"}:
            return _fail("canonical recovery command events must cover both resume and retry")

        resume_payload = dict(by_kind["resume"] or {})
        retry_payload = dict(by_kind["retry"] or {})
        if str(resume_payload.get("envelope_id") or "") != str(accepted_resume.envelope_id or ""):
            return _fail("canonical resume event must preserve the accepted envelope id")
        if str(retry_payload.get("envelope_id") or "") != str(accepted_retry.envelope_id or ""):
            return _fail("canonical retry event must preserve the accepted envelope id")
        retry_review = dict(retry_payload.get("review") or {})
        if str(retry_review.get("self_attribution") or "") != "evaluator_path_error":
            return _fail("canonical retry event must preserve recovery self-attribution evidence")
        if str(retry_review.get("self_repair") or "") != "fix the staged command path before retrying the same node":
            return _fail("canonical retry event must preserve recovery self-repair evidence")

    print("[loop-system-event-journal-recovery-command-event][OK] accepted resume and retry commands emit canonical topology-command events without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
