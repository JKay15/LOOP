#!/usr/bin/env python3
"""Validate canonical accepted relaunch-command events for Milestone 4."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-relaunch-command-event][FAIL] {msg}", file=sys.stderr)
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
        task_id="milestone4-relaunch-command-event",
        root_goal="validate canonical accepted relaunch command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise relaunch command event validation",
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
    relaunch_node = _make_source_node(node_id="child-relaunch-001", status="BLOCKED")
    kernel_state.register_node(root_node)
    kernel_state.register_node(relaunch_node)
    kernel_state.blocked_reasons[relaunch_node.node_id] = "staged workspace is inconsistent and needs a fresh launch"
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime.recover import build_relaunch_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_relaunch_command_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        source_node = _make_source_node(node_id="child-relaunch-001", status="BLOCKED")

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
            return _fail(f"relaunch request must be accepted, got {accepted.status.value!r}")

        replayed = submit_control_envelope(state_root, accepted)
        if replayed.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted relaunch envelope must stay transport-accepted")

        if committed_event_count(state_root) != 2:
            return _fail("accepted relaunch under replay must produce exactly two committed events")

        canonical = [
            dict(event)
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "topology_command_accepted"
        ]
        if len(canonical) != 1:
            return _fail("accepted relaunch must emit exactly one canonical topology-command event")

        payload = dict(canonical[0].get("payload") or {})
        if str(payload.get("command_kind") or "") != "relaunch":
            return _fail("canonical relaunch event must expose command_kind=relaunch")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical relaunch event must preserve the accepted envelope id")
        if list(payload.get("target_node_ids") or []) != ["child-relaunch-001-v2"]:
            return _fail("canonical relaunch event must expose the replacement node as target_node_ids")
        review = dict(payload.get("review") or {})
        if str(review.get("self_attribution") or "") != "staged_workspace_corruption":
            return _fail("canonical relaunch event must preserve self-attribution evidence")
        if str(review.get("self_repair") or "") != "launch a fresh child from durable delegation instead of blaming implementation":
            return _fail("canonical relaunch event must preserve self-repair evidence")

    print("[loop-system-event-journal-relaunch-command-event][OK] accepted relaunch emits one canonical topology-command event with replacement-target visibility")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
