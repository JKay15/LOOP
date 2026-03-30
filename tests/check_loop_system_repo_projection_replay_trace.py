#!/usr/bin/env python3
"""Validate the read-only projection replay trace surface."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-projection-replay-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel import rebuild_kernel_projections
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_projection_replay_trace_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection replay trace",
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
        node_id="projection-replay-trace-child-001",
        node_kind="implementer",
        goal_slice="exercise projection replay trace",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-replay-trace-child-001.json",
        result_sink_ref="artifacts/projection-replay-trace-child-001/result.json",
        lineage_ref="root-kernel->projection-replay-trace-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_projection_replay_trace_success_") as repo_root:
        state_root = repo_root / ".loop" / "projection_replay_trace_success_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-replay-trace-success",
            root_goal="validate projection replay trace",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        initial = query_projection_replay_trace_view(state_root, node_id=child_node.node_id)
        initial_summary = dict(initial.get("projection_replay_summary") or {})
        initial_crash_summary = dict(initial.get("crash_consistency_summary") or {})
        if str(initial_summary.get("mode") or "") != "caught_up":
            return _fail("fresh replay trace must report caught_up before any committed projection event exists")
        if str(initial_crash_summary.get("matrix_state") or "") != "caught_up":
            return _fail("fresh replay trace must expose crash_consistency_summary=caught_up before any committed projection event exists")
        if initial.get("latest_projection_rebuild_result") is not None:
            return _fail("fresh replay trace must not invent a latest rebuild result")

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projection replay trace dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before replay trace checks")

        stale = query_projection_replay_trace_view(state_root, node_id=child_node.node_id)
        stale_summary = dict(stale.get("projection_replay_summary") or {})
        stale_crash_summary = dict(stale.get("crash_consistency_summary") or {})
        if str(stale_summary.get("mode") or "") != "replayable_now":
            return _fail("replay trace must report replayable_now before trusted rebuild catches up")
        if str(stale_crash_summary.get("matrix_state") or "") != "committed_not_projected_replayable":
            return _fail("replay trace must expose crash_consistency_summary=committed_not_projected_replayable before trusted rebuild catches up")
        if int(stale_summary.get("pending_projection_event_count") or 0) != 1:
            return _fail("replay trace must surface the pending projection event count")
        if len(list(stale.get("pending_projection_event_head") or [])) != 1:
            return _fail("replay trace must expose the pending projection event head before trusted rebuild")

        rebuild_kernel_projections(state_root, authority=kernel_internal_authority())

        healed = query_projection_replay_trace_view(state_root, node_id=child_node.node_id)
        healed_summary = dict(healed.get("projection_replay_summary") or {})
        healed_crash_summary = dict(healed.get("crash_consistency_summary") or {})
        latest_rebuild = dict(healed.get("latest_projection_rebuild_result") or {})
        if str(healed_summary.get("mode") or "") != "caught_up":
            return _fail("replay trace must report caught_up after explicit rebuild")
        if str(healed_crash_summary.get("matrix_state") or "") != "repaired_recently":
            return _fail("replay trace must expose crash_consistency_summary=repaired_recently after explicit rebuild")
        if str(latest_rebuild.get("status") or "") != "completed":
            return _fail("replay trace must surface the latest completed rebuild result")
        if int(healed_summary.get("journal_committed_seq") or 0) <= int(healed_summary.get("projection_committed_seq") or 0):
            return _fail("replay trace must distinguish total journal seq from projection-affecting committed seq")
        if not list(healed.get("applied_projection_event_tail") or []):
            return _fail("replay trace must retain the applied projection event tail after explicit rebuild")

    with temporary_repo_root(prefix="loop_system_projection_replay_trace_blocked_") as repo_root:
        state_root = repo_root / ".loop" / "projection_replay_trace_blocked_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-replay-trace-blocked",
            root_goal="validate blocked projection replay trace",
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
                    reason="simulate blocked topology replay for projection trace",
                    payload={"consistency_signal": "topology_replay_blocked_for_trace"},
                ).to_envelope(
                    round_id="R0.1",
                    generation=root_node.generation,
                )
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted topology envelope must mirror into the journal before blocked trace checks")

        rebuild_kernel_projections(state_root, authority=kernel_internal_authority())
        blocked = query_projection_replay_trace_view(state_root)
        blocked_summary = dict(blocked.get("projection_replay_summary") or {})
        blocked_crash_summary = dict(blocked.get("crash_consistency_summary") or {})
        latest_rebuild = dict(blocked.get("latest_projection_rebuild_result") or {})
        if str(blocked_summary.get("mode") or "") != "blocked":
            return _fail("replay trace must report blocked when topology replay is deferred")
        if str(blocked_crash_summary.get("matrix_state") or "") != "committed_not_projected_blocked":
            return _fail("replay trace must expose crash_consistency_summary=committed_not_projected_blocked when topology replay is deferred")
        if str(blocked_summary.get("blocking_reason") or "") != "topology_replay_deferred":
            return _fail("replay trace must surface topology_replay_deferred as the blocking reason")
        if str(latest_rebuild.get("status") or "") != "blocked":
            return _fail("replay trace must surface the latest blocked rebuild result")

    print("[loop-system-projection-replay-trace][OK] replay trace explains replayable, caught-up, and blocked projection states")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
