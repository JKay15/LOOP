#!/usr/bin/env python3
"""Validate nested runtime heavy-object queries bridge to repo-anchor authority."""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-nested-runtime-authority-bridge][FAIL] {msg}", file=sys.stderr)
    return 2


def _expected_canonical_store_ref(repo_root: Path, *, object_kind: str, object_id: str, is_dir: bool, suffix: str = "") -> Path:
    digest = str(object_id or "").removeprefix("sha256:").strip()
    leaf = "tree" if is_dir else f"object{suffix}"
    return (repo_root / ".loop" / "heavy_objects" / object_kind / digest / leaf).resolve()


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> Path:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path.resolve()


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-nested-runtime-authority-bridge",
        root_goal="validate nested runtime heavy-object authority bridging",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise nested runtime heavy-object authority bridging",
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
    kernel_state.register_node(root_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    from loop_product.kernel.query import (
        query_event_journal_status_view,
        query_heavy_object_authority_gap_inventory_view,
        query_projection_explanation_view,
    )
    from loop_product.runtime.control_plane import live_repo_control_plane_runtime
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_heavy_object_nested_runtime_authority_bridge_") as repo_root:
        state_root = repo_root / ".loop" / "nested-heavy-authority-bridge"
        anchor_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        payload = b"nested-runtime-heavy-authority-bridge\n"
        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/nested-authority-primary.pack",
            payload,
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/nested-authority-primary.pack",
            payload,
        )
        object_id = f"sha256:{hashlib.sha256(payload).hexdigest()}"

        def _converged() -> bool:
            control_plane = dict(live_repo_control_plane_runtime(repo_root=repo_root) or {})
            inventory = query_heavy_object_authority_gap_inventory_view(
                state_root,
                object_kind="mathlib_pack",
                repo_root=repo_root,
            )
            status = query_event_journal_status_view(state_root)
            projection = query_projection_explanation_view(state_root)
            status_summary = dict(status.get("heavy_object_authority_gap_summary") or {})
            projection_summary = dict(projection.get("heavy_object_authority_gap_summary") or {})
            return (
                int(control_plane.get("pid") or 0) > 0
                and str(inventory.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(inventory.get("registered_candidate_count") or 0) == 2
                and int(inventory.get("discovery_covered_candidate_count") or 0) == 2
                and int(inventory.get("unmanaged_candidate_count") or 0) == 0
                and str(status_summary.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(status_summary.get("unmanaged_candidate_count") or 0) == 0
                and str(projection_summary.get("authority_runtime_root") or "") == str(anchor_root.resolve())
                and int(projection_summary.get("unmanaged_candidate_count") or 0) == 0
            )

        if not _wait_until(_converged, timeout_s=8.0):
            return _fail(
                "nested runtime heavy-object summaries must converge through repo-anchor authority after ambient bootstrap"
            )

        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        managed_refs = {
            str(dict(item).get("object_ref") or "")
            for item in list(inventory.get("managed_candidates") or [])
        }
        if managed_refs != {str(primary_ref), str(duplicate_ref)}:
            return _fail("nested runtime authority bridge must expose both duplicate candidate refs as managed")

        registration_events = _committed_events_by_type(anchor_root, "heavy_object_registered")
        if len(registration_events) != 1:
            return _fail("repo-anchor authority bridge must emit exactly one auto-generated heavy_object_registered fact")
        registration_payload = dict(registration_events[0].get("payload") or {})
        if str(registration_payload.get("object_id") or "") != object_id:
            return _fail("repo-anchor auto-registration must preserve the bridged object id")
        expected_canonical_ref = _expected_canonical_store_ref(
            repo_root,
            object_kind="mathlib_pack",
            object_id=object_id,
            is_dir=False,
            suffix=".pack",
        )
        if str(registration_payload.get("object_ref") or "") != str(expected_canonical_ref):
            return _fail("repo-anchor auto-registration must anchor the canonical content-addressed store ref")
        if str(registration_payload.get("registration_kind") or "") != "authority_gap_auto_registration":
            return _fail("repo-anchor auto-registration must preserve authority_gap_auto_registration kind")

    print(
        "[loop-system-heavy-object-nested-runtime-authority-bridge][OK] "
        "nested runtime heavy-object summaries bridge to repo-anchor authority without query side effects"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
