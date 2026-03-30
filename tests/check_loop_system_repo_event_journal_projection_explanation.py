#!/usr/bin/env python3
"""Validate projection explanation/debug surface over committed event history."""

from __future__ import annotations

import sys
import json
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-explanation][FAIL] {msg}", file=sys.stderr)
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
        / "lane_projection"
        / ".loop"
        / "ai_user"
        / "workspace"
        / "deliverables"
        / "projection_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
        repo_root
        / "workspace"
        / "event-journal-projection-explanation"
        / "deliverables"
        / ".projection_artifact.publish.projdup"
        / "projection_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
    ]
    payload = f"projection-pack::{pack_name}\n".encode("utf-8")
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
        from loop_product.kernel.query import query_authority_view, query_projection_explanation_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection explanation surface",
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
        node_id="projection-explanation-child-001",
        node_kind="implementer",
        goal_slice="exercise projection explanation surface",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-explanation-child-001.json",
        result_sink_ref="artifacts/projection-explanation-child-001/result.json",
        lineage_ref="root-kernel->projection-explanation-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_explanation_") as repo_root:
        state_root = repo_root / ".loop" / "event_journal_projection_explanation_runtime"
        anchor_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        pack_name = "pack-event-journal-projection-explanation.pack"
        pack_paths = _materialize_duplicate_pack_candidates(
            repo_root=repo_root,
            state_root=state_root,
            pack_name=pack_name,
        )
        kernel_state = KernelState(
            task_id="event-journal-projection-explanation",
            root_goal="validate projection explanation surface",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        def _summary_converged() -> bool:
            summary = dict(query_projection_explanation_view(state_root).get("heavy_object_authority_gap_summary") or {})
            return (
                str(summary.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(summary.get("filesystem_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("registered_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("discovery_covered_candidate_count") or 0) == len(pack_paths)
                and int(summary.get("unmanaged_candidate_count") or 0) == 0
            )

        if not _wait_until(_summary_converged, timeout_s=8.0):
            return _fail("projection explanation must converge nested heavy-object summaries through repo-anchor authority")

        initial = query_projection_explanation_view(state_root, node_id=child_node.node_id)
        initial_replay_summary = dict(initial.get("projection_replay_summary") or {})
        initial_crash_summary = dict(initial.get("crash_consistency_summary") or {})
        if str(initial.get("visibility_state") or "") != "untracked":
            return _fail("projection explanation must surface untracked visibility before any manifest exists")
        if str(initial.get("effective_projection_source") or "") != "node_projection":
            return _fail("projection explanation should fall back to node_projection before a bundle manifest exists")
        if list(initial.get("applied_event_tail") or []):
            return _fail("fresh projection explanation must not invent applied events before journal commits exist")
        initial_gap_summary = dict(initial.get("heavy_object_authority_gap_summary") or {})
        initial_pressure_summary = dict(initial.get("heavy_object_pressure_summary") or {})
        if int(initial_gap_summary.get("filesystem_candidate_count") or 0) != len(pack_paths):
            return _fail("projection explanation must expose repo-local heavy-object gap counts even before the first manifest exists")
        if str(initial_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("projection explanation must expose repo-anchor authority_runtime_root for nested heavy-object truth")
        if int(initial_gap_summary.get("registered_candidate_count") or 0) != len(pack_paths):
            return _fail("projection explanation must expose all duplicate candidates as registration-covered after repo-anchor convergence")
        if int(initial_gap_summary.get("discovery_covered_candidate_count") or 0) != len(pack_paths):
            return _fail("projection explanation must expose both duplicate refs as discovery-covered after repo-anchor convergence")
        if int(initial_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("projection explanation must converge to zero unmanaged heavy-object candidates")
        if str(initial_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("projection explanation must expose repo-anchor authority_runtime_root through the generic pressure summary")
        if int(initial_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("projection explanation must expose duplicate heavy-object pressure even before the first manifest exists")
        if int(initial_pressure_summary.get("strongest_duplicate_count") or 0) != len(pack_paths):
            return _fail("projection explanation must expose strongest duplicate evidence through the generic pressure summary")
        lifecycle_pressure_count = (
            int(initial_pressure_summary.get("shareable_object_count") or 0)
            + int(initial_pressure_summary.get("converged_object_count") or 0)
            + int(initial_pressure_summary.get("settling_object_count") or 0)
            + int(initial_pressure_summary.get("settled_object_count") or 0)
        )
        if lifecycle_pressure_count < 1:
            return _fail("projection explanation must expose at least one registered heavy-object pressure lifecycle state")
        if str(initial_replay_summary.get("mode") or "") != "caught_up":
            return _fail("projection explanation must expose a compact replay summary even before any committed projection event exists")
        if str(initial_crash_summary.get("matrix_state") or "") != "caught_up":
            return _fail("projection explanation must expose crash_consistency_summary=caught_up before any committed projection event exists")

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projection explanation dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before projection explanation")

        stale = query_projection_explanation_view(state_root, node_id=child_node.node_id)
        stale_replay_summary = dict(stale.get("projection_replay_summary") or {})
        stale_crash_summary = dict(stale.get("crash_consistency_summary") or {})
        pending_head = list(stale.get("pending_event_head") or [])
        if str(stale.get("visibility_state") or "") != "untracked":
            return _fail("projection explanation must remain untracked before trusted self-heal")
        if len(pending_head) != 1:
            return _fail("projection explanation must expose one pending event before self-heal")
        pending = dict(pending_head[0] or {})
        if int(pending.get("seq") or 0) != 1 or str(pending.get("event_type") or "") != "control_envelope_accepted":
            return _fail("projection explanation must summarize the pending committed event before self-heal")
        stale_gap_summary = dict(stale.get("heavy_object_authority_gap_summary") or {})
        stale_pressure_summary = dict(stale.get("heavy_object_pressure_summary") or {})
        if str(stale_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("projection explanation must keep surfacing repo-anchor authority_runtime_root while replay is pending")
        if int(stale_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("projection explanation must keep surfacing converged heavy-object authority counts while replay is pending")
        if str(stale_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("projection explanation must keep surfacing repo-anchor authority_runtime_root in the generic pressure summary while replay is pending")
        if int(stale_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("projection explanation must keep surfacing duplicate heavy-object pressure while replay is pending")
        if str(stale_replay_summary.get("mode") or "") != "replayable_now":
            return _fail("projection explanation must surface replayable_now through the compact replay summary while replay is pending")
        if str(stale_crash_summary.get("matrix_state") or "") != "committed_not_projected_replayable":
            return _fail("projection explanation must expose crash_consistency_summary=committed_not_projected_replayable while replay is pending")

        _ = query_authority_view(state_root)

        healed = query_projection_explanation_view(state_root, node_id=child_node.node_id)
        healed_replay_summary = dict(healed.get("projection_replay_summary") or {})
        healed_crash_summary = dict(healed.get("crash_consistency_summary") or {})
        applied_tail = list(healed.get("applied_event_tail") or [])
        if str(healed.get("visibility_state") or "") != "atomically_visible":
            return _fail("projection explanation must report atomically visible after trusted self-heal")
        if str(healed.get("effective_projection_source") or "") != "projection_manifest":
            return _fail("projection explanation must attribute a fully visible bundle to the projection manifest")
        if int(healed.get("effective_last_applied_seq") or 0) != 1:
            return _fail("projection explanation must expose the applied manifest seq after self-heal")
        if len(applied_tail) != 1:
            return _fail("projection explanation must retain the applied event tail for the visible bundle")
        applied = dict(applied_tail[0] or {})
        if str(applied.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("projection explanation must retain the accepted envelope identity in the applied tail")
        healed_pending = list(healed.get("pending_event_head") or [])
        if healed_pending and str(dict(healed_pending[0] or {}).get("event_type") or "") != "projection_rebuild_performed":
            return _fail("projection explanation must only leave the non-projection rebuild result fact pending once the bundle is fully visible")
        healed_gap_summary = dict(healed.get("heavy_object_authority_gap_summary") or {})
        healed_pressure_summary = dict(healed.get("heavy_object_pressure_summary") or {})
        if str(healed_gap_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("projection explanation must retain repo-anchor authority_runtime_root after self-heal")
        if int(healed_gap_summary.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("projection explanation must retain converged heavy-object authority counts after self-heal")
        if str(healed_pressure_summary.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("projection explanation must retain repo-anchor authority_runtime_root in the generic pressure summary after self-heal")
        if int(healed_pressure_summary.get("observed_duplicate_object_count") or 0) < 1:
            return _fail("projection explanation must retain duplicate heavy-object pressure visibility after self-heal")
        if str(healed_replay_summary.get("mode") or "") != "caught_up":
            return _fail("projection explanation must surface caught_up through the compact replay summary after self-heal")
        if str(healed_crash_summary.get("matrix_state") or "") != "repaired_recently":
            return _fail("projection explanation must expose crash_consistency_summary=repaired_recently after self-heal")
    print("[loop-system-event-journal-projection-explanation][OK] projection explanation surfaces the visible bundle's applied and pending committed facts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
