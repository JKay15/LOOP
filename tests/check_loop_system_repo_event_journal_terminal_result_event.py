#!/usr/bin/env python3
"""Validate canonical accepted terminal-result events for Milestone 4."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-terminal-result-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-terminal-result-event",
        root_goal="validate canonical accepted terminal-result events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise terminal-result event validation",
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
        node_id="child-terminal-001",
        node_kind="implementer",
        goal_slice="exercise terminal-result event coverage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-terminal-001.json",
        result_sink_ref="artifacts/child-terminal-001/result.json",
        lineage_ref="root-kernel->child-terminal-001",
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
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.protocols.control_objects import NodeTerminalOutcome
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.runtime.lifecycle import build_node_terminal_result
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_terminal_result_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        child_node = NodeSpec(
            node_id="child-terminal-001",
            node_kind="implementer",
            goal_slice="exercise terminal-result event coverage",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["implement", "evaluate", "report"],
            delegation_ref="state/delegations/child-terminal-001.json",
            result_sink_ref="artifacts/child-terminal-001/result.json",
            lineage_ref="root-kernel->child-terminal-001",
            status=NodeStatus.ACTIVE,
        )
        accepted = submit_control_envelope(
            state_root,
            build_node_terminal_result(
                child_node,
                outcome=NodeTerminalOutcome.FAIL_CLOSED,
                summary="bounded local loop terminated fail-closed after exhausting retries",
                evidence_refs=["roundbook:child-terminal-001"],
                self_attribution="bounded_retry_exhausted",
                self_repair="resume only after a new committed recovery command arrives",
            ).to_envelope(round_id="R1.terminal", generation=child_node.generation),
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"terminal result must be accepted, got {accepted.status.value!r}")

        replayed = submit_control_envelope(state_root, accepted)
        if replayed.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted terminal envelope must stay transport-accepted")
        if committed_event_count(state_root) != 2:
            return _fail("accepted terminal result under replay must produce exactly two committed events")

        canonical = [
            dict(event)
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "terminal_result_recorded"
        ]
        if len(canonical) != 1:
            return _fail("accepted terminal result must emit exactly one canonical terminal-result event")
        payload = dict(canonical[0].get("payload") or {})
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical terminal-result event must preserve the accepted envelope id")
        if str(payload.get("node_id") or "") != "child-terminal-001":
            return _fail("canonical terminal-result event must preserve the node id")
        if str(payload.get("outcome") or "") != "FAIL_CLOSED":
            return _fail("canonical terminal-result event must preserve the terminal outcome")
        if str(payload.get("node_status") or "") != "FAILED":
            return _fail("canonical terminal-result event must preserve the terminal node status")
        if str(payload.get("self_attribution") or "") != "bounded_retry_exhausted":
            return _fail("canonical terminal-result event must preserve self-attribution evidence")
        if str(payload.get("self_repair") or "") != "resume only after a new committed recovery command arrives":
            return _fail("canonical terminal-result event must preserve self-repair evidence")

    print("[loop-system-event-journal-terminal-result-event][OK] accepted terminal results emit one canonical terminal-result event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
