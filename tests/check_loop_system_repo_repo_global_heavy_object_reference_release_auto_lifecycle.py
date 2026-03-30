#!/usr/bin/env python3
"""Validate repo-global heavy-object auto lifecycle unblocks after authoritative reference release."""

from __future__ import annotations

import hashlib
import shutil
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-global-heavy-object-reference-release-auto-lifecycle][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-repo-global-heavy-object-reference-release-auto-lifecycle",
        root_goal="validate repo-global heavy-object auto lifecycle unblock after reference release",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-global heavy-object reference-release auto lifecycle validation",
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


def _write_bytes(path: Path, payload: bytes) -> tuple[str, Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}", path.resolve()


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    from loop_product.artifact_hygiene import canonical_heavy_object_store_ref, classify_mathlib_pack_reference_holder
    from loop_product.kernel.query import (
        query_heavy_object_reference_release_trace_view,
        query_heavy_object_retention_view,
    )
    from loop_product.kernel.submit import (
        submit_heavy_object_reference_release_request,
        submit_heavy_object_reference_request,
        submit_heavy_object_registration_request,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_heavy_object_reference_release_auto_lifecycle_") as repo_root:
        anchor_root = repo_root / ".loop"
        _persist_anchor_state(anchor_root)
        active_holder_ref = (
            repo_root
            / "workspace"
            / "heavy-object-reference-release-auto-lifecycle"
            / "deliverables"
            / ".primary_artifact.publish.lifecycle"
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / "active.pack"
        )
        old_id, old_seed_ref = _write_bytes(
            repo_root / "artifacts" / "heavy_objects" / "mathlib" / "old-reference-release-auto-lifecycle.pack",
            b"heavy-object-reference-release-auto-lifecycle-old\n",
        )
        old_store_ref = canonical_heavy_object_store_ref(
            repo_root=repo_root,
            object_kind="mathlib_pack",
            object_id=old_id,
            source_ref=old_seed_ref,
        )
        old_store_ref.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old_seed_ref), str(old_store_ref))
        _write_bytes(active_holder_ref, b"heavy-object-reference-release-auto-lifecycle-old\n")
        active_holder = classify_mathlib_pack_reference_holder(active_holder_ref, repo_root=repo_root)
        if str(active_holder.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("reference-release auto lifecycle fixture must classify the active holder as workspace_publication_staging_root")

        blocking_holder_root = repo_root / "workspace" / "heavy-object-reference-release-auto-lifecycle" / "deliverables" / "primary_artifact"
        blocking_reference_ref = (
            blocking_holder_root / ".lake" / "packages" / "mathlib" / ".git" / "objects" / "pack" / old_store_ref.name
        )
        blocking_reference_ref.parent.mkdir(parents=True, exist_ok=True)
        blocking_reference_ref.write_bytes(b"heavy-object-reference-release-auto-lifecycle-old\n")

        submit_heavy_object_registration_request(
            anchor_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_store_ref,
            byte_size=old_store_ref.stat().st_size,
            reason="register old canonical heavy-object store path before replacement",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            registration_kind="manual_store_seed",
        )
        submit_heavy_object_reference_request(
            anchor_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_store_ref,
            reference_ref=str(active_holder_ref),
            reference_holder_kind=str(active_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(active_holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach old store object to the live holder path",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_reference_request(
            anchor_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_store_ref,
            reference_ref=str(blocking_reference_ref),
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=blocking_holder_root,
            reference_kind="manual_store_seed_reference",
            reason="attach one extra authoritative reference that should block auto lifecycle until release",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global reference-release auto lifecycle requires a live repo control-plane service")

        new_id, new_seed_ref = _write_bytes(
            repo_root / "artifacts" / "heavy_objects" / "mathlib" / "new-reference-release-auto-lifecycle.pack",
            b"heavy-object-reference-release-auto-lifecycle-new\n",
        )
        new_store_ref = canonical_heavy_object_store_ref(
            repo_root=repo_root,
            object_kind="mathlib_pack",
            object_id=new_id,
            source_ref=new_seed_ref,
        )
        new_store_ref.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(new_seed_ref), str(new_store_ref))
        _write_bytes(active_holder_ref, b"heavy-object-reference-release-auto-lifecycle-new\n")
        submit_heavy_object_registration_request(
            anchor_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_store_ref,
            byte_size=new_store_ref.stat().st_size,
            reason="register replacement canonical heavy-object store path",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            registration_kind="manual_store_seed",
        )
        submit_heavy_object_reference_request(
            anchor_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_store_ref,
            reference_ref=str(active_holder_ref),
            reference_holder_kind=str(active_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(active_holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach replacement store object to the same live holder path",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        ensure_repo_control_plane_services_running(repo_root=repo_root)

        def _superseded_but_not_gc_or_reclaimed() -> bool:
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
            ]
            reclaimed_events = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reclaimed")
                if str(dict(event.get("payload") or {}).get("object_id") or "") == old_id
            ]
            return len(supersession_events) >= 1 and not gc_eligibility_events and not reclaimed_events

        if not _wait_until(_superseded_but_not_gc_or_reclaimed, timeout_s=8.0):
            return _fail("while the old object still has an authoritative extra reference, repo-global auto lifecycle must stop after canonical supersession")

        blocked_view = query_heavy_object_retention_view(anchor_root, object_id=old_id, object_ref=old_store_ref)
        if str(blocked_view.get("retention_state") or "") != "REFERENCED":
            return _fail("old superseded object must remain REFERENCED while an authoritative extra reference is still active")
        if not old_store_ref.exists():
            return _fail("old canonical store ref must still exist while the authoritative extra reference blocks reclamation")

        released = submit_heavy_object_reference_release_request(
            anchor_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_store_ref,
            reference_ref=str(blocking_reference_ref),
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=blocking_holder_root,
            reference_kind="manual_store_seed_reference",
            reason="release the blocking extra reference so repo-global lifecycle may reclaim the superseded object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        ensure_repo_control_plane_services_running(repo_root=repo_root)

        def _gc_and_reclaimed_after_release() -> bool:
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
            return len(gc_eligibility_events) >= 1 and len(reclaimed_events) >= 1

        if not _wait_until(_gc_and_reclaimed_after_release, timeout_s=8.0):
            return _fail("after authoritative reference release, repo-global auto lifecycle must resume GC eligibility and reclamation")

        release_trace = query_heavy_object_reference_release_trace_view(anchor_root, envelope_id=str(released.envelope_id or ""))
        released_reference = dict(release_trace.get("released_reference") or {})
        if str(released_reference.get("reference_ref") or "") != str(blocking_reference_ref.resolve()):
            return _fail("reference-release trace must preserve the released blocking reference ref")

        released_view = query_heavy_object_retention_view(anchor_root, object_id=old_id, object_ref=old_store_ref)
        if str(released_view.get("retention_state") or "") != "RECLAIMED":
            return _fail("after release, the superseded old object must eventually converge to retention_state=RECLAIMED")
        if old_store_ref.exists():
            return _fail("after release, repo-global auto lifecycle must reclaim the superseded canonical heavy-object store path")

    print(
        "[loop-system-repo-global-heavy-object-reference-release-auto-lifecycle][OK] "
        "repo-global control-plane keeps superseded objects retained while an authoritative extra reference exists "
        "and then resumes lifecycle after reference release"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
