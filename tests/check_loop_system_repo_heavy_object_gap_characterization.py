#!/usr/bin/env python3
"""Verify nested runtime heavy-object authority bridging through repo-anchor truth."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-gap][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def main() -> int:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import (
        query_event_journal_status_view,
        query_heavy_object_authority_gap_inventory_view,
        query_projection_explanation_view,
    )
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.runtime.control_plane import live_repo_control_plane_runtime
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_heavy_object_gap_") as repo_root:
        repo_root = repo_root.resolve()
        state_root = repo_root / ".loop" / "heavy-gap-run"
        anchor_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)

        kernel_state = KernelState(
            task_id="heavy-object-gap",
            root_goal="characterize missing heavy-object lifecycle authority",
            root_node_id="root-kernel",
        )
        kernel_state.register_node(
            NodeSpec(
                node_id="root-kernel",
                node_kind="kernel",
                goal_slice="supervise heavy-object gap characterization",
                parent_node_id=None,
                generation=0,
                round_id="R0",
                execution_policy={"mode": "kernel"},
                reasoning_profile={"thinking_budget": "medium", "role": "kernel"},
                budget_profile={"max_rounds": 1},
                allowed_actions=["dispatch", "submit", "audit"],
                delegation_ref="",
                result_sink_ref="artifacts/kernel.json",
                lineage_ref="root-kernel",
                status=NodeStatus.ACTIVE,
            )
        )
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        duplicate_pack_name = "pack-gap-characterization.pack"
        pack_paths = [
            state_root
            / "artifacts"
            / "evaluator_runs"
            / "lane_a"
            / ".loop"
            / "ai_user"
            / "workspace"
            / "deliverables"
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / duplicate_pack_name,
            repo_root
            / "workspace"
            / "heavy-gap-run"
            / "deliverables"
            / ".primary_artifact.publish.gapdup"
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / duplicate_pack_name,
        ]
        payload = f"gap-pack::{duplicate_pack_name}\n".encode("utf-8")
        for idx, path in enumerate(pack_paths, start=1):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)

        def _converged() -> bool:
            control_plane = dict(live_repo_control_plane_runtime(repo_root=repo_root) or {})
            inventory = query_heavy_object_authority_gap_inventory_view(
                state_root,
                object_kind="mathlib_pack",
                repo_root=repo_root,
            )
            journal_status = query_event_journal_status_view(state_root)
            explanation = query_projection_explanation_view(state_root)
            heavy_gap_status = dict(journal_status.get("heavy_object_authority_gap_summary") or {})
            heavy_gap_projection = dict(explanation.get("heavy_object_authority_gap_summary") or {})
            return (
                int(control_plane.get("pid") or 0) > 0
                and str(inventory.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(inventory.get("registered_candidate_count") or 0) == len(pack_paths)
                and int(inventory.get("discovery_covered_candidate_count") or 0) == 2
                and int(inventory.get("unmanaged_candidate_count") or 0) == 0
                and str(heavy_gap_status.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(heavy_gap_status.get("unmanaged_candidate_count") or 0) == 0
                and str(heavy_gap_projection.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(heavy_gap_projection.get("unmanaged_candidate_count") or 0) == 0
            )

        if not _wait_until(_converged, timeout_s=8.0):
            return _fail("nested runtime heavy-object gap characterization must converge through repo-anchor authority")

        journal_status = query_event_journal_status_view(state_root)
        explanation = query_projection_explanation_view(state_root)
        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        blob = json.dumps(
            {
                "journal_status": journal_status,
                "projection_explanation": explanation,
                "inventory": inventory,
            },
            indent=2,
            sort_keys=True,
        )

        heavy_gap_status = dict(journal_status.get("heavy_object_authority_gap_summary") or {})
        heavy_gap_projection = dict(explanation.get("heavy_object_authority_gap_summary") or {})
        if int(heavy_gap_status.get("filesystem_candidate_count") or 0) != len(pack_paths):
            return _fail("generic event-journal status must still surface heavy-object candidate counts")
        if int(heavy_gap_status.get("managed_candidate_count") or 0) != len(pack_paths):
            return _fail("generic event-journal status must surface repo-anchor-managed heavy-object candidates after convergence")
        if int(heavy_gap_status.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("generic event-journal status must converge to zero unmanaged heavy-object candidates")
        if int(heavy_gap_projection.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("projection explanation must converge to zero unmanaged heavy-object candidates")
        if int(inventory.get("registered_candidate_count") or 0) != len(pack_paths):
            return _fail("inventory must expose all duplicate candidates as registration-covered after repo-anchor auto-registration")
        if int(inventory.get("discovery_covered_candidate_count") or 0) != len(pack_paths):
            return _fail("inventory must preserve both duplicate refs as discovery-covered after remediation")
        if str(inventory.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("inventory must expose repo-anchor authority_runtime_root for nested runtime heavy-object truth")
        if str(heavy_gap_status.get("authority_runtime_root") or "") != str(anchor_root.resolve()):
            return _fail("generic event-journal status must expose repo-anchor authority_runtime_root for heavy-object truth")
        if blob.count(duplicate_pack_name) == 0:
            return _fail("post-takeover characterization must keep the duplicate pack paths visible through read-only query surfaces")
        if any(not path.exists() for path in pack_paths):
            return _fail("characterization setup must materialize duplicate heavy-object files")

    print("[loop-system-heavy-object-gap][OK] nested runtime heavy-object gaps converge through repo-anchor authority and remain visible through read-only surfaces")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
