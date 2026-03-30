#!/usr/bin/env python3
"""Validate projection consistency query status over the event journal."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-status][FAIL] {msg}", file=sys.stderr)
    return 2


def _seq(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    return -1 if value is None else int(value)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view, query_projection_consistency_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection consistency status",
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
        node_id="projection-status-child-001",
        node_kind="implementer",
        goal_slice="exercise projection consistency query",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-status-child-001.json",
        result_sink_ref="artifacts/projection-status-child-001/result.json",
        lineage_ref="root-kernel->projection-status-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_status_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-status",
            root_goal="validate projection consistency query",
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
                    note="projection consistency pending replay dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the event journal")

        stale_view = query_projection_consistency_view(state_root)
        kernel_projection = dict(stale_view.get("kernel_projection") or {})
        pending = dict(stale_view.get("pending_replay") or {})
        replay_summary = dict(stale_view.get("projection_replay_summary") or {})
        if int(stale_view.get("committed_seq") or 0) != 1:
            return _fail("projection consistency view must expose the committed journal seq")
        if bool(kernel_projection.get("stale")) is not True:
            return _fail("projection consistency view must flag stale kernel projection before self-heal")
        if _seq(kernel_projection, "last_applied_seq") != 0:
            return _fail("projection consistency view must expose the stale kernel projection floor")
        if str(pending.get("mode") or "") != "replayable_now":
            return _fail("projection consistency view must expose replayable pending journal facts before self-heal")
        if int(pending.get("replayable_event_count") or 0) != 1:
            return _fail("projection consistency view must count replayable pending events")
        if list(pending.get("pending_event_types") or []) != ["control_envelope_accepted"]:
            return _fail("projection consistency view must expose the pending replay event types")
        if str(stale_view.get("visibility_state") or "") != "untracked":
            return _fail("projection consistency view must report untracked visibility before a manifest is materialized")
        if str(replay_summary.get("mode") or "") != "replayable_now":
            return _fail("projection consistency view must expose replayable_now through the compact replay summary before self-heal")
        if int(replay_summary.get("journal_committed_seq") or 0) != 1:
            return _fail("projection consistency view must expose the total journal committed seq through the compact replay summary")
        if int(replay_summary.get("projection_committed_seq") or 0) != 1:
            return _fail("projection consistency view must expose the projection-affecting committed seq through the compact replay summary")

        _ = query_authority_view(state_root)

        healed_view = query_projection_consistency_view(state_root)
        healed_kernel_projection = dict(healed_view.get("kernel_projection") or {})
        healed_pending = dict(healed_view.get("pending_replay") or {})
        healed_replay_summary = dict(healed_view.get("projection_replay_summary") or {})
        if bool(healed_kernel_projection.get("stale")):
            return _fail("projection consistency view must stop reporting stale kernel projection after self-heal")
        if _seq(healed_kernel_projection, "last_applied_seq") != int(healed_view.get("committed_seq") or 0):
            return _fail("projection consistency view must show kernel projection caught up after self-heal")
        if _seq(healed_pending, "replayable_event_count") != 0:
            return _fail("projection consistency view must show no replayable gap after self-heal")
        if str(healed_pending.get("mode") or "") != "caught_up":
            return _fail("projection consistency view must report caught_up after self-heal")
        if str(healed_view.get("visibility_state") or "") != "atomically_visible":
            return _fail("projection consistency view must report atomically_visible after trusted rebuild writes a manifest")
        if str(healed_replay_summary.get("mode") or "") != "caught_up":
            return _fail("projection consistency view must expose caught_up through the compact replay summary after self-heal")

    print("[loop-system-event-journal-projection-status][OK] projection consistency view explains stale-vs-caught-up state")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
