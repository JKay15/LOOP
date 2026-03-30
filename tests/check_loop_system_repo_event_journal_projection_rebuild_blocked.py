#!/usr/bin/env python3
"""Validate that explicit projection rebuild truthfully blocks on unsupported topology replay."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-rebuild-blocked][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import mirror_accepted_control_envelope_event
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
        goal_slice="validate blocked explicit projection rebuild",
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

    with temporary_repo_root(prefix="loop_system_event_journal_projection_rebuild_blocked_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-rebuild-blocked",
            root_goal="validate blocked explicit projection rebuild",
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
                    reason="simulate a committed topology event without projection side effects",
                    payload={"consistency_signal": "topology_replay_blocked"},
                ).to_envelope(
                    round_id="R0.1",
                    generation=root_node.generation,
                )
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted topology envelope must mirror into the journal before blocked rebuild")

        rebuild_summary = rebuild_kernel_projections(state_root, authority=kernel_internal_authority())
        if str(rebuild_summary.get("projection_state") or "") != "kernel_stale":
            return _fail("explicit rebuild must report kernel_stale when pending topology replay is deferred")
        if int(rebuild_summary.get("kernel_last_applied_seq", -1)) != 0:
            return _fail("explicit rebuild must not advance the kernel projection floor past an unsupported topology event")
        if int(rebuild_summary.get("replayed_event_count", -1)) != 0:
            return _fail("explicit rebuild must not claim to replay unsupported topology events")
        pending_replay = dict(rebuild_summary.get("pending_replay") or {})
        blocking_event = dict(pending_replay.get("blocking_event") or {})
        if str(pending_replay.get("mode") or "") != "blocked":
            return _fail("explicit rebuild must report blocked pending replay when topology replay is deferred")
        if str(blocking_event.get("reason") or "") != "topology_replay_deferred":
            return _fail("explicit rebuild must surface topology_replay_deferred as the blocking reason")

        consistency = query_projection_consistency_view(state_root)
        if str(dict(consistency.get("pending_replay") or {}).get("mode") or "") != "blocked":
            return _fail("projection consistency view must remain blocked after a truthful no-op rebuild")

    print("[loop-system-event-journal-projection-rebuild-blocked][OK] explicit rebuild reports blocked topology replay honestly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
