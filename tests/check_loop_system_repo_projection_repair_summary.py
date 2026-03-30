#!/usr/bin/env python3
"""Validate compact projection repair-summary truth across replayable, node-lag, and blocked states."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-projection-repair-summary][FAIL] {msg}", file=sys.stderr)
    return 2


def _assert_repair_summary(
    summary: dict[str, object],
    *,
    state: str,
    scope: str,
    repairable_now: bool,
    target_node_ids: list[str],
    blocking_reason: str = "",
) -> str | None:
    if str(summary.get("repair_state") or "") != state:
        return f"expected repair_state={state}, got {summary.get('repair_state')!r}"
    if str(summary.get("repair_scope") or "") != scope:
        return f"expected repair_scope={scope}, got {summary.get('repair_scope')!r}"
    if bool(summary.get("repairable_now")) is not repairable_now:
        return f"expected repairable_now={repairable_now}, got {summary.get('repairable_now')!r}"
    if list(summary.get("repair_target_node_ids") or []) != target_node_ids:
        return (
            f"expected repair_target_node_ids={target_node_ids!r}, "
            f"got {summary.get('repair_target_node_ids')!r}"
        )
    if str(summary.get("blocking_reason") or "") != blocking_reason:
        return f"expected blocking_reason={blocking_reason!r}, got {summary.get('blocking_reason')!r}"
    if bool(summary.get("repair_needed")) is not (state != "none"):
        return f"expected repair_needed={(state != 'none')}, got {summary.get('repair_needed')!r}"
    return None


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import (
            query_commit_state_view,
            query_projection_consistency_view,
            query_projection_replay_trace_view,
        )
        from loop_product.kernel.state import (
            KernelState,
            ensure_runtime_tree,
            persist_kernel_state,
            persist_node_snapshot,
            query_kernel_state_object,
        )
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate compact projection repair-summary truth",
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
        node_id="projection-repair-summary-child-001",
        node_kind="implementer",
        goal_slice="exercise projection repair-summary transitions",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-repair-summary-child-001.json",
        result_sink_ref="artifacts/projection-repair-summary-child-001/result.json",
        lineage_ref="root-kernel->projection-repair-summary-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_projection_repair_summary_") as repo_root:
        state_root = repo_root / ".loop" / "projection_repair_summary_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="projection-repair-summary",
            root_goal="validate projection repair-summary truth",
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
                    note="projection repair summary dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before replayable repair checks")

        consistency = query_projection_consistency_view(state_root)
        replayable_summary = dict(consistency.get("projection_replay_summary") or {})
        err = _assert_repair_summary(
            replayable_summary,
            state="kernel_rebuild_replayable",
            scope="kernel",
            repairable_now=True,
            target_node_ids=[],
        )
        if err:
            return _fail(f"projection consistency summary mismatch in replayable state: {err}")

        commit_state = query_commit_state_view(state_root)
        commit_summary = dict(commit_state.get("projection_replay_summary") or {})
        err = _assert_repair_summary(
            commit_summary,
            state="kernel_rebuild_replayable",
            scope="kernel",
            repairable_now=True,
            target_node_ids=[],
        )
        if err:
            return _fail(f"commit-state summary mismatch in replayable state: {err}")

        replay_trace = query_projection_replay_trace_view(state_root, node_id=child_node.node_id)
        trace_summary = dict(replay_trace.get("projection_replay_summary") or {})
        err = _assert_repair_summary(
            trace_summary,
            state="kernel_rebuild_replayable",
            scope="kernel",
            repairable_now=True,
            target_node_ids=[],
        )
        if err:
            return _fail(f"replay trace summary mismatch in replayable state: {err}")

        _ = query_kernel_state_object(state_root, continue_deferred=False)

        child_snapshot_ref = state_root / "state" / f"{child_node.node_id}.json"
        child_snapshot = json.loads(child_snapshot_ref.read_text(encoding="utf-8"))
        child_snapshot["last_applied_seq"] = 0
        child_snapshot["projection_updated_at"] = "2026-03-27T00:00:00Z"
        os.chmod(child_snapshot_ref, 0o644)
        child_snapshot_ref.write_text(json.dumps(child_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        partial = query_projection_consistency_view(state_root)
        partial_summary = dict(partial.get("projection_replay_summary") or {})
        err = _assert_repair_summary(
            partial_summary,
            state="node_rebuild_required",
            scope="node",
            repairable_now=True,
            target_node_ids=[child_node.node_id],
        )
        if err:
            return _fail(f"projection consistency summary mismatch in node-lag state: {err}")

    with temporary_repo_root(prefix="loop_system_projection_repair_summary_blocked_") as repo_root:
        state_root = repo_root / ".loop" / "projection_repair_summary_blocked_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="projection-repair-summary-blocked",
            root_goal="validate blocked projection repair-summary truth",
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
                    reason="simulate blocked topology replay for repair-summary truth",
                    payload={"consistency_signal": "projection_repair_summary_blocked"},
                ).to_envelope(
                    round_id="R0.1",
                    generation=root_node.generation,
                )
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted topology envelope must mirror into the journal before blocked repair checks")

        blocked = query_projection_replay_trace_view(state_root)
        blocked_summary = dict(blocked.get("projection_replay_summary") or {})
        err = _assert_repair_summary(
            blocked_summary,
            state="kernel_rebuild_blocked",
            scope="kernel",
            repairable_now=False,
            target_node_ids=[],
            blocking_reason="topology_replay_deferred",
        )
        if err:
            return _fail(f"replay trace summary mismatch in blocked state: {err}")

    print("[loop-system-projection-repair-summary][OK] compact replay summaries expose kernel, node, and blocked repair truth")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
