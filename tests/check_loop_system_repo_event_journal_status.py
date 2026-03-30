#!/usr/bin/env python3
"""Validate the event-journal status query surface."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-status][FAIL] {msg}", file=sys.stderr)
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
        / "evaluator_runs"
        / "lane_status"
        / ".loop"
        / "ai_user"
        / "workspace"
        / "deliverables"
        / "status_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
        repo_root
        / "workspace"
        / "event-journal-status"
        / "deliverables"
        / ".status_artifact.publish.statusdup"
        / "status_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
    ]
    payload = f"status-pack::{pack_name}\n".encode("utf-8")
    for idx, path in enumerate(pack_paths, start=1):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    return pack_paths


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import EVENT_JOURNAL_EVENT_SCHEMA_VERSION, EVENT_JOURNAL_STORE_FORMAT_VERSION
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_authority_view, query_event_journal_status_view
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate event journal status surface",
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
        node_id="event-journal-status-child-001",
        node_kind="implementer",
        goal_slice="exercise event journal status visibility",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/event-journal-status-child-001.json",
        result_sink_ref="artifacts/event-journal-status-child-001/result.json",
        lineage_ref="root-kernel->event-journal-status-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_status_") as repo_root:
        state_root = repo_root / ".loop" / "event_journal_status_runtime"
        anchor_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        pack_name = "pack-event-journal-status.pack"
        pack_paths = _materialize_duplicate_pack_candidates(
            repo_root=repo_root,
            state_root=state_root,
            pack_name=pack_name,
        )
        kernel_state = KernelState(
            task_id="event-journal-status",
            root_goal="validate event-journal status surface",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        def _summary_converged() -> bool:
            summary = dict(query_event_journal_status_view(state_root).get("heavy_object_authority_gap_summary") or {})
            return (
                str(summary.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(summary.get("filesystem_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("registered_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("discovery_covered_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("unmanaged_candidate_count") or 0) == 0
            )

        if not _wait_until(_summary_converged, timeout_s=8.0):
            return _fail("event journal status must converge nested heavy-object summaries through repo-anchor authority")

        pre_status = query_event_journal_status_view(state_root)
        pre_replay_summary = dict(pre_status.get("projection_replay_summary") or {})
        pre_crash_summary = dict(pre_status.get("crash_consistency_summary") or {})
        if str(pre_status.get("compatibility_status") or "") != "compatible":
            return _fail("journal status must report compatible before any committed events")
        if int(pre_status.get("committed_event_count", -1)) != 0:
            return _fail("journal status must report zero committed events for a fresh runtime root")
        if str(dict(pre_status.get("journal_meta") or {}).get("event_schema_version") or "") != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
            return _fail("journal status must surface the current event schema version")
        if str(dict(pre_status.get("journal_meta") or {}).get("store_format_version") or "") != EVENT_JOURNAL_STORE_FORMAT_VERSION:
            return _fail("journal status must surface the current store format version")
        pre_gap_summary = dict(pre_status.get("heavy_object_authority_gap_summary") or {})
        pre_pressure_summary = dict(pre_status.get("heavy_object_pressure_summary") or {})
        if int(pre_gap_summary.get("filesystem_candidate_count") or 0) != len(pack_paths):
            return _fail("journal status must expose repo-local heavy-object candidates through the generic status surface")
        if str(pre_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("journal status must expose repo-anchor authority_runtime_root for nested heavy-object truth")
        if int(pre_gap_summary.get("registered_candidate_count") or 0) != len(pack_paths):
            return _fail("journal status must surface all duplicate candidates as registration-covered after repo-anchor convergence")
        if int(pre_gap_summary.get("discovery_covered_candidate_count") or 0) != len(pack_paths):
            return _fail("journal status must surface both duplicate refs as discovery-covered after repo-anchor convergence")
        if int(pre_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("journal status must converge to zero unmanaged heavy-object candidates")
        if str(pre_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("journal status must expose repo-anchor authority_runtime_root through the generic heavy-object pressure summary")
        if int(pre_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("journal status must expose duplicate heavy-object pressure through the generic status surface")
        if int(pre_pressure_summary.get("strongest_duplicate_count") or 0) != len(pack_paths):
            return _fail("journal status must expose strongest duplicate evidence through the generic pressure summary")
        lifecycle_pressure_count = (
            int(pre_pressure_summary.get("shareable_object_count") or 0)
            + int(pre_pressure_summary.get("converged_object_count") or 0)
            + int(pre_pressure_summary.get("settling_object_count") or 0)
            + int(pre_pressure_summary.get("settled_object_count") or 0)
        )
        if lifecycle_pressure_count < 1:
            return _fail("journal status must expose at least one registered heavy-object pressure lifecycle state")
        if str(pre_replay_summary.get("mode") or "") != "caught_up":
            return _fail("journal status must expose a compact projection replay summary before any committed projection event exists")
        if str(pre_crash_summary.get("matrix_state") or "") != "caught_up":
            return _fail("journal status must expose crash_consistency_summary= caught_up before any committed projection event exists")

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="event-journal-status dispatch heartbeat",
                ).to_envelope()
            ),
        )
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before querying status")

        stale_status = query_event_journal_status_view(state_root)
        stale_replay_summary = dict(stale_status.get("projection_replay_summary") or {})
        stale_crash_summary = dict(stale_status.get("crash_consistency_summary") or {})
        if int(stale_status.get("committed_event_count") or 0) != 1:
            return _fail("journal status must report the mirrored committed event count")
        if int(stale_status.get("committed_seq") or 0) != 1:
            return _fail("journal status must report the current committed seq")
        if str(dict(stale_status.get("projection_consistency") or {}).get("pending_replay", {}).get("mode") or "") != "replayable_now":
            return _fail("journal status must surface replayable pending projection state before self-heal")
        stale_gap_summary = dict(stale_status.get("heavy_object_authority_gap_summary") or {})
        stale_pressure_summary = dict(stale_status.get("heavy_object_pressure_summary") or {})
        if str(stale_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("journal status must keep surfacing repo-anchor authority_runtime_root while replay is pending")
        if int(stale_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("journal status must keep surfacing converged heavy-object authority counts while journal events are pending replay")
        if str(stale_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("journal status must keep surfacing repo-anchor authority_runtime_root in the generic pressure summary while replay is pending")
        if int(stale_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("journal status must keep surfacing duplicate heavy-object pressure while replay is pending")
        if str(stale_replay_summary.get("mode") or "") != "replayable_now":
            return _fail("journal status must expose replayable_now through the compact projection replay summary while replay is pending")
        if str(stale_crash_summary.get("matrix_state") or "") != "committed_not_projected_replayable":
            return _fail("journal status must expose crash_consistency_summary=committed_not_projected_replayable while replay is pending")

        _ = query_authority_view(state_root)
        healed_status = query_event_journal_status_view(state_root)
        healed_replay_summary = dict(healed_status.get("projection_replay_summary") or {})
        healed_crash_summary = dict(healed_status.get("crash_consistency_summary") or {})
        if str(dict(healed_status.get("projection_consistency") or {}).get("pending_replay", {}).get("mode") or "") != "caught_up":
            return _fail("journal status must surface caught_up projection state after self-heal")
        healed_gap_summary = dict(healed_status.get("heavy_object_authority_gap_summary") or {})
        healed_pressure_summary = dict(healed_status.get("heavy_object_pressure_summary") or {})
        if str(healed_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("journal status must retain repo-anchor authority_runtime_root after projection self-heal")
        if int(healed_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("journal status must retain converged heavy-object authority counts after projection self-heal")
        if str(healed_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("journal status must retain repo-anchor authority_runtime_root in the generic pressure summary after projection self-heal")
        if int(healed_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("journal status must retain duplicate heavy-object pressure visibility after projection self-heal")
        if str(healed_replay_summary.get("mode") or "") != "caught_up":
            return _fail("journal status must expose caught_up through the compact projection replay summary after self-heal")
        if str(healed_crash_summary.get("matrix_state") or "") != "repaired_recently":
            return _fail("journal status must expose crash_consistency_summary=repaired_recently after projection self-heal")

    print("[loop-system-event-journal-status][OK] event journal status view exposes meta, commit progress, and projection lag")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
