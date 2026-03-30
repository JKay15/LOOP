#!/usr/bin/env python3
"""Validate explicit projection rebuild from the committed journal floor."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-rebuild][FAIL] {msg}", file=sys.stderr)
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
        from loop_product.kernel.query import query_commit_state_view, query_projection_consistency_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate explicit projection rebuild",
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
        node_id="projection-rebuild-child-001",
        node_kind="implementer",
        goal_slice="exercise explicit projection rebuild",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-rebuild-child-001.json",
        result_sink_ref="artifacts/projection-rebuild-child-001/result.json",
        lineage_ref="root-kernel->projection-rebuild-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_rebuild_") as repo_root:
        state_root = repo_root / ".loop" / "event_journal_projection_rebuild_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-rebuild",
            root_goal="validate explicit projection rebuild",
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
                    note="projection-rebuild dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before explicit rebuild")

        pre_rebuild_view = query_commit_state_view(state_root)
        if str(pre_rebuild_view.get("phase") or "") != "committed_not_projected":
            return _fail("pre-rebuild commit-state must expose committed_not_projected")

        rebuild_summary = rebuild_kernel_projections(state_root, authority=kernel_internal_authority())
        if str(rebuild_summary.get("projection_state") or "") != "projected":
            return _fail("explicit rebuild must drive projection_state to projected")
        if int(rebuild_summary.get("previous_kernel_last_applied_seq", -1)) != 0:
            return _fail("explicit rebuild must report the old kernel projection floor")
        if int(rebuild_summary.get("kernel_last_applied_seq") or 0) != 1:
            return _fail("explicit rebuild must advance the kernel projection floor to the committed seq")
        if int(rebuild_summary.get("previous_projection_manifest_last_applied_seq", -1)) != 0:
            return _fail("explicit rebuild must report that the manifest was absent before the first rebuild")
        if int(rebuild_summary.get("projection_manifest_last_applied_seq") or 0) != 1:
            return _fail("explicit rebuild must advance the projection manifest to the committed seq")
        if int(rebuild_summary.get("replayed_event_count") or 0) != 1:
            return _fail("explicit rebuild must report the replayed compatibility-mirrored event count")
        updated_node_ids = list(rebuild_summary.get("updated_node_ids") or [])
        if updated_node_ids != [child_node.node_id, root_node.node_id]:
            return _fail("explicit rebuild must report every node projection it rematerialized")

        post_rebuild_view = query_commit_state_view(state_root)
        if str(post_rebuild_view.get("phase") or "") != "projected":
            return _fail("explicit rebuild must leave commit-state in projected phase")
        consistency_view = query_projection_consistency_view(state_root)
        if str(dict(consistency_view.get("pending_replay") or {}).get("mode") or "") != "caught_up":
            return _fail("explicit rebuild must leave projection consistency in caught_up mode")

        second_summary = rebuild_kernel_projections(state_root, authority=kernel_internal_authority())
        if int(second_summary.get("replayed_event_count", -1)) != 0:
            return _fail("explicit rebuild must be idempotent once projections are already caught up")
        if list(second_summary.get("updated_node_ids") or []):
            return _fail("explicit rebuild must not rematerialize node projections when nothing is stale")
        if int(second_summary.get("projection_manifest_last_applied_seq") or 0) != 1:
            return _fail("explicit rebuild must leave the projection manifest at the committed seq when already caught up")

    print("[loop-system-event-journal-projection-rebuild][OK] explicit rebuild catches projections up to the committed journal floor idempotently")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
