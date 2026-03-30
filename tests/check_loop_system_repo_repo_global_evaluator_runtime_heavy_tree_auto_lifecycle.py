#!/usr/bin/env python3
"""Validate repo-global auto lifecycle convergence for evaluator runtime-heavy-tree replacements."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-global-evaluator-runtime-heavy-tree-auto-lifecycle][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-evaluator-runtime-heavy-tree-auto-lifecycle",
        root_goal="validate repo-global evaluator runtime-heavy-tree auto lifecycle convergence",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-global evaluator runtime-heavy-tree auto lifecycle validation",
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


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _write_runtime_heavy_tree(artifact_root: Path, *, label: str) -> Path:
    tree_root = artifact_root / ".lake"
    (tree_root / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
    (tree_root / "packages" / "mathlib" / "STATUS.txt").write_text(f"{label}: ready\n", encoding="utf-8")
    return tree_root.resolve()


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    from loop_product.artifact_hygiene import (
        classify_runtime_heavy_tree_reference_holder,
        heavy_object_identity,
    )
    from loop_product.kernel.query import (
        query_heavy_object_gc_eligibility_trace_view,
        query_heavy_object_reference_inventory_view,
        query_heavy_object_reference_release_trace_view,
        query_heavy_object_reclamation_trace_view,
        query_heavy_object_retention_view,
        query_heavy_object_supersession_trace_view,
    )
    from loop_product.kernel.submit import (
        submit_heavy_object_reference_request,
        submit_heavy_object_registration_request,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_evaluator_runtime_heavy_tree_auto_lifecycle_") as repo_root:
        anchor_root = repo_root / ".loop"
        _persist_anchor_state(anchor_root)

        holder_root = (
            repo_root
            / ".loop"
            / "evaluator-runtime-heavy-tree-auto-lifecycle"
            / "artifacts"
            / "evaluator_runs"
            / "lane_a"
            / ".loop"
            / "ai_user"
            / "workspace"
            / "deliverables"
            / "primary_artifact"
        )
        holder_ref = _write_runtime_heavy_tree(holder_root, label="evaluator-runtime-heavy-tree-live")
        holder = classify_runtime_heavy_tree_reference_holder(holder_ref, repo_root=repo_root)
        if str(holder.get("reference_holder_kind") or "") != "evaluator_workspace_artifact_root":
            return _fail("evaluator runtime-heavy-tree auto lifecycle fixture must classify the holder as evaluator_workspace_artifact_root")

        old_store_ref = _write_runtime_heavy_tree(
            repo_root / "artifacts" / "heavy_objects" / "runtime-heavy" / "old-evaluator-auto-lifecycle-tree",
            label="evaluator-runtime-heavy-tree-old",
        )
        old_id = str(heavy_object_identity(old_store_ref, object_kind="runtime_heavy_tree").get("object_id") or "")
        _write_runtime_heavy_tree(holder_root, label="evaluator-runtime-heavy-tree-old")

        submit_heavy_object_registration_request(
            anchor_root,
            object_id=old_id,
            object_kind="runtime_heavy_tree",
            object_ref=old_store_ref,
            byte_size=int(heavy_object_identity(old_store_ref, object_kind="runtime_heavy_tree").get("byte_size") or 0),
            reason="register old canonical evaluator runtime-heavy-tree before replacement",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
            registration_kind="manual_store_seed",
        )
        submit_heavy_object_reference_request(
            anchor_root,
            object_id=old_id,
            object_kind="runtime_heavy_tree",
            object_ref=old_store_ref,
            reference_ref=str(holder_ref),
            reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach old evaluator runtime-heavy-tree to the same holder path",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global evaluator runtime-heavy-tree auto lifecycle requires a live repo control-plane service")

        new_store_ref = _write_runtime_heavy_tree(
            repo_root / "artifacts" / "heavy_objects" / "runtime-heavy" / "new-evaluator-auto-lifecycle-tree",
            label="evaluator-runtime-heavy-tree-new",
        )
        new_id = str(heavy_object_identity(new_store_ref, object_kind="runtime_heavy_tree").get("object_id") or "")
        _write_runtime_heavy_tree(holder_root, label="evaluator-runtime-heavy-tree-new")
        submit_heavy_object_registration_request(
            anchor_root,
            object_id=new_id,
            object_kind="runtime_heavy_tree",
            object_ref=new_store_ref,
            byte_size=int(heavy_object_identity(new_store_ref, object_kind="runtime_heavy_tree").get("byte_size") or 0),
            reason="register replacement canonical evaluator runtime-heavy-tree",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
            registration_kind="manual_store_seed",
        )
        replacement_reference = submit_heavy_object_reference_request(
            anchor_root,
            object_id=new_id,
            object_kind="runtime_heavy_tree",
            object_ref=new_store_ref,
            reference_ref=str(holder_ref),
            reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach replacement evaluator runtime-heavy-tree to the same holder path",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
        )

        ensure_repo_control_plane_services_running(repo_root=repo_root)

        def _auto_lifecycle_ready() -> bool:
            releases = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_released")
                if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
                and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
                and str(dict(event.get("payload") or {}).get("reference_holder_kind") or "")
                == str(holder.get("reference_holder_kind") or "")
                and str(dict(event.get("payload") or {}).get("reference_holder_ref") or "")
                == str(holder.get("reference_holder_ref") or "")
            ]
            supersessions = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_superseded")
                if str(dict(event.get("payload") or {}).get("superseded_object_id") or "") == old_id
                and str(dict(event.get("payload") or {}).get("replacement_object_id") or "") == new_id
            ]
            gc_eligible = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_gc_eligible")
                if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
                and str(dict(event.get("payload") or {}).get("superseded_by_object_id") or "") == new_id
            ]
            reclaimed = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reclaimed")
                if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
            ]
            replacement_reference_inventory = query_heavy_object_reference_inventory_view(
                anchor_root,
                object_id=new_id,
            )
            replacement_retention = query_heavy_object_retention_view(
                anchor_root,
                object_id=new_id,
            )
            return (
                len(releases) >= 1
                and len(supersessions) >= 1
                and len(gc_eligible) >= 1
                and len(reclaimed) >= 1
                and int(replacement_reference_inventory.get("active_reference_count") or 0) == 1
                and list(replacement_reference_inventory.get("active_reference_refs") or []) == [str(holder_ref)]
                and str(replacement_retention.get("retention_state") or "") == "REFERENCED"
            )

        if not _wait_until(_auto_lifecycle_ready, timeout_s=40.0):
            return _fail("repo-global control-plane must auto-converge evaluator runtime-heavy-tree replacement through supersession, GC eligibility, and reclamation")

        supersession_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_superseded")
            if str(dict(event.get("payload") or {}).get("superseded_object_id") or "") == old_id
            and str(dict(event.get("payload") or {}).get("replacement_object_id") or "") == new_id
        ]
        gc_eligibility_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_gc_eligible")
            if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
            and str(dict(event.get("payload") or {}).get("superseded_by_object_id") or "") == new_id
        ]
        reclaimed_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_reclaimed")
            if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
        ]
        release_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_reference_released")
            if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
            and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
            and str(dict(event.get("payload") or {}).get("reference_holder_kind") or "")
            == str(holder.get("reference_holder_kind") or "")
            and str(dict(event.get("payload") or {}).get("reference_holder_ref") or "")
            == str(holder.get("reference_holder_ref") or "")
        ]
        if len(supersession_events) != 1 or len(gc_eligibility_events) != 1 or len(reclaimed_events) != 1:
            return _fail("unchanged evaluator runtime-heavy-tree replacement snapshot must converge through exactly one canonical lifecycle chain")
        if len(release_events) != 1:
            return _fail("unchanged evaluator runtime-heavy-tree replacement snapshot must converge through exactly one canonical handoff release")

        release_event = release_events[0]
        release_payload = dict(release_event.get("payload") or {})
        replacement_reference_event = next(
            (
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(replacement_reference.envelope_id or "")
            ),
            {},
        )
        replacement_reference_event_id = str(replacement_reference_event.get("event_id") or "")
        if not replacement_reference_event_id:
            return _fail("evaluator runtime-heavy-tree auto lifecycle test must locate the canonical replacement reference event")
        if str(release_payload.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("evaluator runtime-heavy-tree auto handoff release must preserve heavy_object_reference_replaced trigger kind")
        if str(release_payload.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("evaluator runtime-heavy-tree auto handoff release must link back to the replacing canonical reference event")

        supersession_payload = dict(supersession_events[0].get("payload") or {})
        gc_payload = dict(gc_eligibility_events[0].get("payload") or {})
        reclaimed_payload = dict(reclaimed_events[0].get("payload") or {})
        if str(supersession_payload.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("evaluator runtime-heavy-tree auto supersession must preserve heavy_object_reference_replaced trigger kind")
        if str(supersession_payload.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("evaluator runtime-heavy-tree auto supersession must link back to the replacing canonical reference event")
        if str(gc_payload.get("trigger_kind") or "") != "heavy_object_auto_supersession":
            return _fail("evaluator runtime-heavy-tree auto GC eligibility must preserve heavy_object_auto_supersession trigger kind")
        if str(reclaimed_payload.get("trigger_kind") or "") != "heavy_object_auto_gc_eligibility":
            return _fail("evaluator runtime-heavy-tree auto reclamation must preserve heavy_object_auto_gc_eligibility trigger kind")

        release_trace = query_heavy_object_reference_release_trace_view(
            anchor_root,
            envelope_id=str(release_payload.get("envelope_id") or ""),
        )
        released_reference = dict(release_trace.get("released_reference") or {})
        if str(released_reference.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("evaluator runtime-heavy-tree auto handoff release trace must preserve the replacement reference event id")

        supersession_trace = query_heavy_object_supersession_trace_view(
            anchor_root,
            envelope_id=str(supersession_payload.get("envelope_id") or ""),
        )
        supersession_summary = dict(supersession_trace.get("supersession") or {})
        if str(supersession_summary.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("evaluator runtime-heavy-tree auto supersession trace must preserve the replacement reference event id")

        gc_trace = query_heavy_object_gc_eligibility_trace_view(
            anchor_root,
            envelope_id=str(gc_payload.get("envelope_id") or ""),
        )
        gc_summary = dict(gc_trace.get("eligible_object") or {})
        if str(gc_summary.get("trigger_kind") or "") != "heavy_object_auto_supersession":
            return _fail("evaluator runtime-heavy-tree auto GC eligibility trace must preserve trigger_kind")

        reclamation_trace = query_heavy_object_reclamation_trace_view(
            anchor_root,
            envelope_id=str(reclaimed_payload.get("envelope_id") or ""),
        )
        reclamation_effect = dict(reclamation_trace.get("reclamation_effect") or {})
        if not bool(reclamation_effect.get("reclaimed")):
            return _fail("evaluator runtime-heavy-tree auto reclamation trace must show reclaimed=true")

        old_reference_inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=old_id,
            object_ref=old_store_ref,
        )
        if int(old_reference_inventory.get("active_reference_count") or 0) != 0:
            return _fail("evaluator runtime-heavy-tree old object must lose active authoritative references after handoff release")
        if int(old_reference_inventory.get("released_reference_count") or 0) != 1:
            return _fail("evaluator runtime-heavy-tree old object must expose one released authoritative reference after handoff release")

        new_reference_inventory = query_heavy_object_reference_inventory_view(anchor_root, object_id=new_id)
        if int(new_reference_inventory.get("active_reference_count") or 0) != 1:
            return _fail("evaluator runtime-heavy-tree replacement identity must retain one active authoritative reference after handoff release")
        if list(new_reference_inventory.get("active_reference_refs") or []) != [str(holder_ref)]:
            return _fail("evaluator runtime-heavy-tree replacement identity must preserve the evaluator holder reference after handoff release")

        new_view = query_heavy_object_retention_view(anchor_root, object_id=new_id)
        if str(new_view.get("retention_state") or "") != "REFERENCED":
            return _fail("evaluator runtime-heavy-tree replacement identity must remain REFERENCED after auto handoff convergence")

        retention_view = query_heavy_object_retention_view(
            anchor_root,
            object_id=old_id,
            object_ref=old_store_ref,
        )
        if str(retention_view.get("retention_state") or "") != "RECLAIMED":
            return _fail("evaluator runtime-heavy-tree retention view must converge old object to RECLAIMED")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(
            [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_released")
                if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
                and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
            ]
        ) != 1:
            return _fail("unchanged evaluator runtime-heavy-tree replacement snapshot must not emit duplicate auto handoff release facts")

    print(
        "[loop-system-repo-global-evaluator-runtime-heavy-tree-auto-lifecycle][OK] "
        "repo-global evaluator runtime-heavy-tree replacement deterministically auto-converges through canonical handoff release and lifecycle events"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
