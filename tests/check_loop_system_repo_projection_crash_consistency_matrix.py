#!/usr/bin/env python3
"""Validate the projection crash-consistency matrix across canonical crash windows."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-projection-crash-consistency-matrix][FAIL] {msg}", file=sys.stderr)
    return 2


def _set_required_generation(db_ref: Path, *, required_generation: str) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute("UPDATE writer_fence SET required_producer_generation = ?", (str(required_generation),))
        conn.execute(
            "UPDATE journal_meta SET value_json = ? WHERE key = 'required_producer_generation'",
            (f'"{required_generation}"',),
        )
        conn.commit()
    finally:
        conn.close()


def _assert_matrix_summary(
    summary: dict[str, object],
    *,
    state: str,
    scope: str,
    phase: str,
    replay_mode: str,
    repair_state: str,
    window_open: bool,
    accepted_only_count: int = 0,
    committed_pending_projection_count: int = 0,
    lagging_node_ids: list[str] | None = None,
    latest_repair_status: str = "",
    latest_repair_cleared: bool = False,
    blocking_reason: str = "",
) -> str | None:
    if str(summary.get("matrix_state") or "") != state:
        return f"expected matrix_state={state}, got {summary.get('matrix_state')!r}"
    if str(summary.get("matrix_scope") or "") != scope:
        return f"expected matrix_scope={scope}, got {summary.get('matrix_scope')!r}"
    if str(summary.get("phase") or "") != phase:
        return f"expected phase={phase}, got {summary.get('phase')!r}"
    if str(summary.get("replay_mode") or "") != replay_mode:
        return f"expected replay_mode={replay_mode}, got {summary.get('replay_mode')!r}"
    if str(summary.get("repair_state") or "") != repair_state:
        return f"expected repair_state={repair_state}, got {summary.get('repair_state')!r}"
    if bool(summary.get("window_open")) is not window_open:
        return f"expected window_open={window_open}, got {summary.get('window_open')!r}"
    if int(summary.get("accepted_only_count") or 0) != accepted_only_count:
        return (
            f"expected accepted_only_count={accepted_only_count}, "
            f"got {summary.get('accepted_only_count')!r}"
        )
    if int(summary.get("committed_pending_projection_count") or 0) != committed_pending_projection_count:
        return (
            "expected committed_pending_projection_count="
            f"{committed_pending_projection_count}, got {summary.get('committed_pending_projection_count')!r}"
        )
    if lagging_node_ids is not None and list(summary.get("lagging_node_ids") or []) != list(lagging_node_ids or []):
        return f"expected lagging_node_ids={lagging_node_ids!r}, got {summary.get('lagging_node_ids')!r}"
    if str(summary.get("latest_repair_status") or "") != latest_repair_status:
        return f"expected latest_repair_status={latest_repair_status!r}, got {summary.get('latest_repair_status')!r}"
    if bool(summary.get("latest_repair_cleared")) is not latest_repair_cleared:
        return (
            f"expected latest_repair_cleared={latest_repair_cleared}, "
            f"got {summary.get('latest_repair_cleared')!r}"
        )
    if str(summary.get("blocking_reason") or "") != blocking_reason:
        return f"expected blocking_reason={blocking_reason!r}, got {summary.get('blocking_reason')!r}"
    return None


def _assert_nested_matrix_state(matrix_view: dict[str, object], *, expected_state: str) -> str | None:
    for field in (
        "event_journal_status",
        "commit_state",
        "projection_consistency",
        "projection_explanation",
        "projection_replay_trace",
    ):
        nested = dict(dict(matrix_view.get(field) or {}).get("crash_consistency_summary") or {})
        if str(nested.get("matrix_state") or "") != expected_state:
            return f"{field} crash_consistency_summary must expose matrix_state={expected_state}, got {nested.get('matrix_state')!r}"
    return None


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import ensure_event_journal, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel import rebuild_kernel_projections
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_projection_crash_consistency_matrix_view
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
        goal_slice="validate projection crash-consistency matrix",
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
        node_id="projection-crash-consistency-child-001",
        node_kind="implementer",
        goal_slice="exercise canonical projection crash windows",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-crash-consistency-child-001.json",
        result_sink_ref="artifacts/projection-crash-consistency-child-001/result.json",
        lineage_ref="root-kernel->projection-crash-consistency-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_projection_crash_consistency_matrix_") as repo_root:
        state_root = repo_root / ".loop" / "projection_crash_consistency_matrix_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="projection-crash-consistency-matrix",
            root_goal="validate canonical projection crash windows",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        initial = query_projection_crash_consistency_matrix_view(state_root, node_id=child_node.node_id)
        initial_summary = dict(initial.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            initial_summary,
            state="caught_up",
            scope="none",
            phase="projected",
            replay_mode="caught_up",
            repair_state="none",
            window_open=False,
        )
        if err:
            return _fail(f"initial crash-consistency summary mismatch: {err}")
        err = _assert_nested_matrix_state(initial, expected_state="caught_up")
        if err:
            return _fail(err)

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projection crash-consistency matrix dispatch heartbeat",
                ).to_envelope()
            ),
        )
        accepted_only = query_projection_crash_consistency_matrix_view(state_root, node_id=child_node.node_id)
        accepted_summary = dict(accepted_only.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            accepted_summary,
            state="accepted_not_committed",
            scope="journal",
            phase="accepted_not_committed",
            replay_mode="caught_up",
            repair_state="none",
            window_open=True,
            accepted_only_count=1,
        )
        if err:
            return _fail(f"accepted-not-committed crash summary mismatch: {err}")
        err = _assert_nested_matrix_state(accepted_only, expected_state="accepted_not_committed")
        if err:
            return _fail(err)

        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before replayable matrix checks")

        kernel_snapshot_ref = state_root / "state" / "kernel_state.json"
        kernel_snapshot = json.loads(kernel_snapshot_ref.read_text(encoding="utf-8"))
        kernel_snapshot["last_applied_seq"] = 0
        kernel_snapshot["projection_updated_at"] = "2026-03-27T00:00:00Z"
        os.chmod(kernel_snapshot_ref, 0o644)
        kernel_snapshot_ref.write_text(json.dumps(kernel_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        projection_manifest_ref = state_root / "state" / "ProjectionManifest.json"
        if projection_manifest_ref.exists():
            projection_manifest = json.loads(projection_manifest_ref.read_text(encoding="utf-8"))
            projection_manifest["last_applied_seq"] = 0
            projection_manifest["projection_updated_at"] = "2026-03-27T00:00:00Z"
            os.chmod(projection_manifest_ref, 0o644)
            projection_manifest_ref.write_text(
                json.dumps(projection_manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

        replayable = query_projection_crash_consistency_matrix_view(state_root, node_id=child_node.node_id)
        replayable_summary = dict(replayable.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            replayable_summary,
            state="committed_not_projected_replayable",
            scope="kernel",
            phase="committed_not_projected",
            replay_mode="replayable_now",
            repair_state="kernel_rebuild_replayable",
            window_open=True,
            committed_pending_projection_count=1,
        )
        if err:
            return _fail(f"replayable committed-not-projected crash summary mismatch: {err}")
        err = _assert_nested_matrix_state(replayable, expected_state="committed_not_projected_replayable")
        if err:
            return _fail(err)

        _ = query_kernel_state_object(state_root, continue_deferred=False)
        repaired = query_projection_crash_consistency_matrix_view(state_root, node_id=child_node.node_id)
        repaired_summary = dict(repaired.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            repaired_summary,
            state="repaired_recently",
            scope="none",
            phase="projected",
            replay_mode="caught_up",
            repair_state="none",
            window_open=False,
            latest_repair_status="completed",
            latest_repair_cleared=True,
        )
        if err:
            return _fail(f"repaired-recently crash summary mismatch: {err}")
        err = _assert_nested_matrix_state(repaired, expected_state="repaired_recently")
        if err:
            return _fail(err)

        child_snapshot_ref = state_root / "state" / f"{child_node.node_id}.json"
        child_snapshot = json.loads(child_snapshot_ref.read_text(encoding="utf-8"))
        child_snapshot["last_applied_seq"] = 0
        child_snapshot["projection_updated_at"] = "2026-03-27T00:00:00Z"
        os.chmod(child_snapshot_ref, 0o644)
        child_snapshot_ref.write_text(json.dumps(child_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        node_lag = query_projection_crash_consistency_matrix_view(state_root, node_id=child_node.node_id)
        node_lag_summary = dict(node_lag.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            node_lag_summary,
            state="node_projection_lag",
            scope="node",
            phase="partially_projected",
            replay_mode="caught_up",
            repair_state="node_rebuild_required",
            window_open=True,
            lagging_node_ids=[child_node.node_id],
            latest_repair_status="completed",
            latest_repair_cleared=True,
        )
        if err:
            return _fail(f"node-lag crash summary mismatch: {err}")
        err = _assert_nested_matrix_state(node_lag, expected_state="node_projection_lag")
        if err:
            return _fail(err)

    with temporary_repo_root(prefix="loop_system_projection_crash_consistency_matrix_blocked_") as repo_root:
        state_root = repo_root / ".loop" / "projection_crash_consistency_matrix_blocked_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="projection-crash-consistency-matrix-blocked",
            root_goal="validate blocked projection crash window",
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
                    reason="simulate blocked topology replay for crash-consistency matrix",
                    payload={"consistency_signal": "projection_crash_consistency_matrix_blocked"},
                ).to_envelope(
                    round_id="R0.1",
                    generation=root_node.generation,
                )
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted topology envelope must mirror into the journal before blocked crash-consistency checks")

        kernel_snapshot_ref = state_root / "state" / "kernel_state.json"
        kernel_snapshot = json.loads(kernel_snapshot_ref.read_text(encoding="utf-8"))
        kernel_snapshot["accepted_envelopes"] = []
        kernel_snapshot["last_applied_seq"] = 0
        kernel_snapshot["projection_updated_at"] = "2026-03-27T00:00:00Z"
        os.chmod(kernel_snapshot_ref, 0o644)
        kernel_snapshot_ref.write_text(json.dumps(kernel_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        projection_manifest_ref = state_root / "state" / "ProjectionManifest.json"
        if projection_manifest_ref.exists():
            projection_manifest = json.loads(projection_manifest_ref.read_text(encoding="utf-8"))
            projection_manifest["last_applied_seq"] = 0
            projection_manifest["projection_updated_at"] = "2026-03-27T00:00:00Z"
            os.chmod(projection_manifest_ref, 0o644)
            projection_manifest_ref.write_text(
                json.dumps(projection_manifest, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

        blocked = query_projection_crash_consistency_matrix_view(state_root)
        blocked_summary = dict(blocked.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            blocked_summary,
            state="committed_not_projected_blocked",
            scope="kernel",
            phase="committed_not_projected",
            replay_mode="blocked",
            repair_state="kernel_rebuild_blocked",
            window_open=True,
            committed_pending_projection_count=1,
            latest_repair_status="",
            latest_repair_cleared=False,
            blocking_reason="topology_replay_deferred",
        )
        if err:
            return _fail(f"blocked crash summary mismatch: {err}")
        err = _assert_nested_matrix_state(blocked, expected_state="committed_not_projected_blocked")
        if err:
            return _fail(err)

    with temporary_repo_root(prefix="loop_system_projection_crash_consistency_matrix_incompatible_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        _set_required_generation(db_ref, required_generation="unsupported-generation-v99")

        incompatible = query_projection_crash_consistency_matrix_view(state_root)
        incompatible_summary = dict(incompatible.get("crash_consistency_summary") or {})
        err = _assert_matrix_summary(
            incompatible_summary,
            state="incompatible_journal",
            scope="incompatible",
            phase="incompatible_journal",
            replay_mode="blocked_by_incompatibility",
            repair_state="incompatible",
            window_open=False,
        )
        if err:
            return _fail(f"incompatible crash summary mismatch: {err}")
        err = _assert_nested_matrix_state(incompatible, expected_state="incompatible_journal")
        if err:
            return _fail(err)

    print("[loop-system-projection-crash-consistency-matrix][OK] crash-consistency matrix exposes canonical accepted/commit/replay/repair windows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
