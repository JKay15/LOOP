#!/usr/bin/env python3
"""Validate canonical committed rebuild-result facts for explicit projection rebuilds."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-rebuild-result-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _latest_rebuild_event(events: list[dict[str, object]]) -> dict[str, object] | None:
    matches = [dict(event) for event in events if str(event.get("event_type") or "") == "projection_rebuild_performed"]
    if not matches:
        return None
    return matches[-1]


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import iter_committed_events, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel import rebuild_kernel_projections
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_projection_consistency_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate canonical rebuild-result facts",
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
        node_id="projection-rebuild-result-child-001",
        node_kind="implementer",
        goal_slice="exercise explicit projection rebuild result facts",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-rebuild-result-child-001.json",
        result_sink_ref="artifacts/projection-rebuild-result-child-001/result.json",
        lineage_ref="root-kernel->projection-rebuild-result-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_rebuild_result_event_success_") as repo_root:
        state_root = repo_root / ".loop" / "projection_rebuild_result_success_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-rebuild-result-success",
            root_goal="validate canonical rebuild-result facts",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projection rebuild result dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before explicit rebuild")

        summary = rebuild_kernel_projections(state_root, authority=kernel_internal_authority())
        if str(summary.get("projection_state") or "") != "projected":
            return _fail("successful explicit rebuild must report projected state")

        rebuild_event = _latest_rebuild_event(iter_committed_events(state_root))
        if rebuild_event is None:
            return _fail("successful explicit rebuild must leave a canonical projection_rebuild_performed fact")
        payload = dict(rebuild_event.get("payload") or {})
        if str(payload.get("rebuild_scope") or "") != "kernel":
            return _fail("successful rebuild result fact must record rebuild_scope=kernel")
        if str(payload.get("status") or "") != "completed":
            return _fail("successful rebuild result fact must record completed status")
        if str(payload.get("projection_state") or "") != "projected":
            return _fail("successful rebuild result fact must preserve the final projection_state")
        if int(payload.get("projection_committed_seq") or 0) != 1:
            return _fail("successful rebuild result fact must preserve the projection_committed_seq")
        if int(payload.get("replayed_event_count") or 0) != 1:
            return _fail("successful rebuild result fact must preserve the replayed event count")
        if str(payload.get("blocking_reason") or ""):
            return _fail("successful rebuild result fact must not record a blocking reason")
        if str(payload.get("repair_state_before") or "") != "kernel_rebuild_replayable":
            return _fail("successful rebuild result fact must record replayable kernel repair before explicit rebuild")
        if str(payload.get("repair_scope_before") or "") != "kernel":
            return _fail("successful rebuild result fact must record repair_scope_before=kernel")
        if bool(payload.get("repairable_before")) is not True:
            return _fail("successful rebuild result fact must record repairable_before=True")
        if str(payload.get("repair_state_after") or "") != "none":
            return _fail("successful rebuild result fact must record no remaining repair need after explicit rebuild")
        if str(payload.get("repair_scope_after") or "") != "none":
            return _fail("successful rebuild result fact must record repair_scope_after=none")
        if bool(payload.get("repair_cleared")) is not True:
            return _fail("successful rebuild result fact must record repair_cleared=True")

        consistency = query_projection_consistency_view(state_root)
        replay_summary = dict(consistency.get("projection_replay_summary") or {})
        if str(replay_summary.get("mode") or "") != "caught_up":
            return _fail("successful explicit rebuild must still leave projection replay in caught_up mode")
        if int(replay_summary.get("journal_committed_seq") or 0) <= int(replay_summary.get("projection_committed_seq") or 0):
            return _fail("successful rebuild result fact must remain visible as non-projection committed truth")

    with temporary_repo_root(prefix="loop_system_event_journal_projection_rebuild_result_event_blocked_") as repo_root:
        state_root = repo_root / ".loop" / "projection_rebuild_result_blocked_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-rebuild-result-blocked",
            root_goal="validate blocked canonical rebuild-result facts",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                TopologyMutation.resume(
                    root_node.node_id,
                    reason="simulate blocked topology replay for rebuild-result facts",
                    payload={"consistency_signal": "topology_replay_blocked_for_result_event"},
                ).to_envelope(
                    round_id="R0.1",
                    generation=root_node.generation,
                )
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted topology envelope must mirror into the journal before blocked explicit rebuild")

        summary = rebuild_kernel_projections(state_root, authority=kernel_internal_authority())
        if str(summary.get("projection_state") or "") != "kernel_stale":
            return _fail("blocked explicit rebuild must report kernel_stale state")

        rebuild_event = _latest_rebuild_event(iter_committed_events(state_root))
        if rebuild_event is None:
            return _fail("blocked explicit rebuild must still leave a canonical projection_rebuild_performed fact")
        payload = dict(rebuild_event.get("payload") or {})
        if str(payload.get("status") or "") != "blocked":
            return _fail("blocked rebuild result fact must record blocked status")
        if str(payload.get("projection_state") or "") != "kernel_stale":
            return _fail("blocked rebuild result fact must preserve the blocked projection_state")
        if str(payload.get("blocking_reason") or "") != "topology_replay_deferred":
            return _fail("blocked rebuild result fact must preserve topology_replay_deferred as the blocking reason")
        replayed_event_count = payload.get("replayed_event_count")
        if replayed_event_count is None or int(replayed_event_count) < 0:
            return _fail("blocked rebuild result fact must preserve a non-negative replayed event count")
        if str(payload.get("repair_state_before") or "") != "kernel_rebuild_blocked":
            return _fail("blocked rebuild result fact must record blocked kernel repair before explicit rebuild")
        if str(payload.get("repair_scope_before") or "") != "kernel":
            return _fail("blocked rebuild result fact must record repair_scope_before=kernel")
        if bool(payload.get("repairable_before")) is not False:
            return _fail("blocked rebuild result fact must record repairable_before=False")
        if str(payload.get("repair_state_after") or "") != "kernel_rebuild_blocked":
            return _fail("blocked rebuild result fact must preserve blocked repair after a truthful no-op rebuild")
        if str(payload.get("repair_scope_after") or "") != "kernel":
            return _fail("blocked rebuild result fact must preserve repair_scope_after=kernel")
        if bool(payload.get("repair_cleared")) is not False:
            return _fail("blocked rebuild result fact must record repair_cleared=False")

    print("[loop-system-event-journal-projection-rebuild-result-event][OK] explicit rebuild attempts leave canonical committed result facts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
