#!/usr/bin/env python3
"""Validate commit-state visibility across audit, journal, and projections."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-commit-state][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _materialize_duplicate_pack_candidates(*, repo_root: Path, state_root: Path, pack_name: str) -> list[Path]:
    pack_paths = [
        state_root
        / "artifacts"
        / "commit_state"
        / ".loop"
        / "ai_user"
        / "workspace"
        / "deliverables"
        / "commit_state_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
        repo_root
        / "workspace"
        / "event-journal-commit-state"
        / "deliverables"
        / ".commit_state_artifact.publish.commitdup"
        / "commit_state_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
    ]
    payload = f"commit-state-pack::{pack_name}\n".encode("utf-8")
    for idx, path in enumerate(pack_paths, start=1):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    return pack_paths


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view, query_commit_state_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate commit-state visibility",
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
        node_id="commit-state-child-001",
        node_kind="implementer",
        goal_slice="exercise commit-state transitions",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/commit-state-child-001.json",
        result_sink_ref="artifacts/commit-state-child-001/result.json",
        lineage_ref="root-kernel->commit-state-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_commit_state_") as repo_root:
        state_root = repo_root / ".loop" / "event_journal_commit_state_runtime"
        anchor_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        pack_name = "pack-event-journal-commit-state.pack"
        pack_paths = _materialize_duplicate_pack_candidates(
            repo_root=repo_root,
            state_root=state_root,
            pack_name=pack_name,
        )
        kernel_state = KernelState(
            task_id="event-journal-commit-state",
            root_goal="validate commit-state visibility",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        def _summary_converged() -> bool:
            summary = dict(query_commit_state_view(state_root).get("heavy_object_authority_gap_summary") or {})
            return (
                str(summary.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(summary.get("filesystem_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("registered_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("discovery_covered_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("unmanaged_candidate_count") or 0) == 0
            )

        if not _wait_until(_summary_converged, timeout_s=8.0):
            return _fail("commit-state view must converge nested heavy-object summaries through repo-anchor authority")

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="commit-state dispatch heartbeat",
                ).to_envelope()
            ),
        )

        accepted_only_view = query_commit_state_view(state_root)
        accepted_replay_summary = dict(accepted_only_view.get("projection_replay_summary") or {})
        accepted_crash_summary = dict(accepted_only_view.get("crash_consistency_summary") or {})
        if str(accepted_only_view.get("phase") or "") != "accepted_not_committed":
            return _fail("commit-state view must expose accepted_not_committed after audit accept but before journal commit")
        if int(accepted_only_view.get("accepted_only_count") or 0) != 1:
            return _fail("commit-state view must count accepted-but-uncommitted envelopes")
        accepted_gap_summary = dict(accepted_only_view.get("heavy_object_authority_gap_summary") or {})
        accepted_pressure_summary = dict(accepted_only_view.get("heavy_object_pressure_summary") or {})
        if int(accepted_gap_summary.get("filesystem_candidate_count") or 0) != len(pack_paths):
            return _fail("commit-state view must expose repo-local heavy-object candidate counts through the generic commit-state surface")
        if str(accepted_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must expose repo-anchor authority_runtime_root for nested heavy-object truth")
        if int(accepted_gap_summary.get("registered_candidate_count") or 0) != len(pack_paths):
            return _fail("commit-state view must expose all duplicate candidates as registration-covered after repo-anchor convergence")
        if int(accepted_gap_summary.get("discovery_covered_candidate_count") or 0) != len(pack_paths):
            return _fail("commit-state view must expose both duplicate refs as discovery-covered after repo-anchor convergence")
        if int(accepted_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("commit-state view must converge to zero unmanaged heavy-object candidates")
        if str(accepted_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must expose repo-anchor authority_runtime_root through the generic pressure summary")
        if int(accepted_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("commit-state view must expose duplicate heavy-object pressure through the generic crash-boundary surface")
        if int(accepted_pressure_summary.get("strongest_duplicate_count") or 0) != len(pack_paths):
            return _fail("commit-state view must expose strongest duplicate evidence through the generic pressure summary")
        lifecycle_pressure_count = (
            int(accepted_pressure_summary.get("shareable_object_count") or 0)
            + int(accepted_pressure_summary.get("converged_object_count") or 0)
            + int(accepted_pressure_summary.get("settling_object_count") or 0)
            + int(accepted_pressure_summary.get("settled_object_count") or 0)
        )
        if lifecycle_pressure_count < 1:
            return _fail("commit-state view must expose at least one registered heavy-object pressure lifecycle state")
        if str(accepted_replay_summary.get("mode") or "") != "caught_up":
            return _fail("commit-state view must expose a compact replay summary even before the journal commit exists")
        if str(accepted_crash_summary.get("matrix_state") or "") != "accepted_not_committed":
            return _fail("commit-state view must expose crash_consistency_summary=accepted_not_committed before the journal commit exists")

        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal")

        committed_only_view = query_commit_state_view(state_root)
        committed_replay_summary = dict(committed_only_view.get("projection_replay_summary") or {})
        committed_crash_summary = dict(committed_only_view.get("crash_consistency_summary") or {})
        if str(committed_only_view.get("phase") or "") != "committed_not_projected":
            return _fail("commit-state view must expose committed_not_projected before projection catches up")
        if int(committed_only_view.get("accepted_only_count", -1)) != 0:
            return _fail("commit-state view must clear accepted-only count once the journal commit exists")
        if int(committed_only_view.get("committed_pending_projection_count") or 0) != 1:
            return _fail("commit-state view must count journal events waiting on projection")
        committed_gap_summary = dict(committed_only_view.get("heavy_object_authority_gap_summary") or {})
        committed_pressure_summary = dict(committed_only_view.get("heavy_object_pressure_summary") or {})
        if str(committed_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must keep surfacing repo-anchor authority_runtime_root while committed events are pending projection")
        if int(committed_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("commit-state view must keep surfacing converged heavy-object authority counts while committed events are pending projection")
        if str(committed_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must keep surfacing repo-anchor authority_runtime_root in the generic pressure summary while committed events are pending projection")
        if int(committed_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("commit-state view must keep surfacing duplicate heavy-object pressure while committed events are pending projection")
        if str(committed_replay_summary.get("mode") or "") != "replayable_now":
            return _fail("commit-state view must surface replayable_now through the compact replay summary while projection is pending")
        if str(committed_crash_summary.get("matrix_state") or "") != "committed_not_projected_replayable":
            return _fail("commit-state view must expose crash_consistency_summary=committed_not_projected_replayable while projection is pending")

        _ = query_authority_view(state_root)

        child_snapshot_ref = state_root / "state" / f"{child_node.node_id}.json"
        child_snapshot = json.loads(child_snapshot_ref.read_text(encoding="utf-8"))
        child_snapshot["last_applied_seq"] = 0
        child_snapshot["projection_updated_at"] = "2026-03-24T00:00:00Z"
        os.chmod(child_snapshot_ref, 0o644)
        child_snapshot_ref.write_text(json.dumps(child_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        partial_view = query_commit_state_view(state_root)
        partial_replay_summary = dict(partial_view.get("projection_replay_summary") or {})
        partial_crash_summary = dict(partial_view.get("crash_consistency_summary") or {})
        if str(partial_view.get("phase") or "") != "partially_projected":
            return _fail("commit-state view must expose partially_projected when kernel projection is current but a node projection lags")
        lagging_nodes = list(partial_view.get("lagging_node_ids") or [])
        if lagging_nodes != [child_node.node_id]:
            return _fail("commit-state view must expose the lagging node ids for partial projection")
        partial_gap_summary = dict(partial_view.get("heavy_object_authority_gap_summary") or {})
        partial_pressure_summary = dict(partial_view.get("heavy_object_pressure_summary") or {})
        if str(partial_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must keep surfacing repo-anchor authority_runtime_root while node projection lags")
        if int(partial_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("commit-state view must keep surfacing converged heavy-object authority counts while node projection lags")
        if str(partial_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must keep surfacing repo-anchor authority_runtime_root in the generic pressure summary while node projection lags")
        if int(partial_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("commit-state view must keep surfacing duplicate heavy-object pressure while node projection lags")
        if str(partial_replay_summary.get("mode") or "") != "caught_up":
            return _fail("commit-state view must treat node-only lag as caught_up at the projection replay summary layer")
        if str(partial_crash_summary.get("matrix_state") or "") != "node_projection_lag":
            return _fail("commit-state view must expose crash_consistency_summary=node_projection_lag while a node projection lags")

        _ = query_authority_view(state_root)

        projected_view = query_commit_state_view(state_root)
        projected_replay_summary = dict(projected_view.get("projection_replay_summary") or {})
        projected_crash_summary = dict(projected_view.get("crash_consistency_summary") or {})
        if str(projected_view.get("phase") or "") != "projected":
            return _fail("commit-state view must expose projected after trusted self-heal catches everything up")
        if int(projected_view.get("committed_pending_projection_count", -1)) != 0:
            return _fail("commit-state view must report zero pending projection count after catch-up")
        if list(projected_view.get("lagging_node_ids") or []):
            return _fail("commit-state view must clear lagging node ids after catch-up")
        projected_gap_summary = dict(projected_view.get("heavy_object_authority_gap_summary") or {})
        projected_pressure_summary = dict(projected_view.get("heavy_object_pressure_summary") or {})
        if str(projected_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must retain repo-anchor authority_runtime_root after projection catch-up")
        if int(projected_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("commit-state view must retain converged heavy-object authority counts after projection catch-up")
        if str(projected_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("commit-state view must retain repo-anchor authority_runtime_root in the generic pressure summary after projection catch-up")
        if int(projected_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("commit-state view must retain duplicate heavy-object pressure visibility after projection catch-up")
        if str(projected_replay_summary.get("mode") or "") != "caught_up":
            return _fail("commit-state view must surface caught_up through the compact replay summary after projection catch-up")
        if str(projected_crash_summary.get("matrix_state") or "") != "repaired_recently":
            return _fail("commit-state view must expose crash_consistency_summary=repaired_recently after projection catch-up")

    print("[loop-system-event-journal-commit-state][OK] commit-state view explains accepted/committed/projected crash boundaries")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
