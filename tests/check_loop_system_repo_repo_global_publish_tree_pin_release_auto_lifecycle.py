#!/usr/bin/env python3
"""Validate repo-global publish-tree auto lifecycle unblocks after authoritative pin release."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-global-publish-tree-pin-release-auto-lifecycle][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-repo-global-publish-tree-pin-release-auto-lifecycle",
        root_goal="validate repo-global publish-tree auto lifecycle unblock after pin release",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-global publish-tree pin-release auto lifecycle validation",
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


def _write_publish_tree(root: Path, *, label: str) -> Path:
    (root / "formalization").mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text(f"# {label}\n", encoding="utf-8")
    (root / "formalization" / "STATUS.md").write_text(f"{label}: ready\n", encoding="utf-8")
    return root.resolve()


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    from loop_product.artifact_hygiene import classify_publish_tree_reference_holder, heavy_object_identity
    from loop_product.kernel.query import (
        query_heavy_object_pin_release_trace_view,
        query_heavy_object_retention_view,
    )
    from loop_product.kernel.submit import (
        submit_heavy_object_pin_release_request,
        submit_heavy_object_pin_request,
        submit_heavy_object_reference_request,
        submit_heavy_object_registration_request,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_publish_tree_pin_release_auto_lifecycle_") as repo_root:
        anchor_root = repo_root / ".loop"
        _persist_anchor_state(anchor_root)

        holder_ref = _write_publish_tree(
            repo_root
            / "workspace"
            / "publish-tree-pin-release-auto-lifecycle"
            / "deliverables"
            / ".primary_artifact.publish.lifecycle"
            / "primary_artifact",
            label="publish-tree-live",
        )
        holder = classify_publish_tree_reference_holder(holder_ref, repo_root=repo_root)
        if str(holder.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("publish-tree pin-release auto lifecycle fixture must classify the holder as workspace_publication_staging_root")

        old_store_ref = _write_publish_tree(
            repo_root / "artifacts" / "heavy_objects" / "publish" / "old-pin-release-auto-lifecycle-tree",
            label="publish-tree-old",
        )
        old_id = str(heavy_object_identity(old_store_ref, object_kind="publish_tree").get("object_id") or "")
        _write_publish_tree(holder_ref, label="publish-tree-old")

        submit_heavy_object_registration_request(
            anchor_root,
            object_id=old_id,
            object_kind="publish_tree",
            object_ref=old_store_ref,
            byte_size=int(heavy_object_identity(old_store_ref, object_kind="publish_tree").get("byte_size") or 0),
            reason="register old canonical publish-tree before replacement",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            registration_kind="manual_store_seed",
        )
        submit_heavy_object_reference_request(
            anchor_root,
            object_id=old_id,
            object_kind="publish_tree",
            object_ref=old_store_ref,
            reference_ref=str(holder_ref),
            reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach old publish-tree to the live holder path",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_pin_request(
            anchor_root,
            object_id=old_id,
            object_kind="publish_tree",
            object_ref=old_store_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="keep the old publish-tree retained while replacement is still pinned by policy",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global publish-tree pin-release auto lifecycle requires a live repo control-plane service")

        new_store_ref = _write_publish_tree(
            repo_root / "artifacts" / "heavy_objects" / "publish" / "new-pin-release-auto-lifecycle-tree",
            label="publish-tree-new",
        )
        new_id = str(heavy_object_identity(new_store_ref, object_kind="publish_tree").get("object_id") or "")
        _write_publish_tree(holder_ref, label="publish-tree-new")
        submit_heavy_object_registration_request(
            anchor_root,
            object_id=new_id,
            object_kind="publish_tree",
            object_ref=new_store_ref,
            byte_size=int(heavy_object_identity(new_store_ref, object_kind="publish_tree").get("byte_size") or 0),
            reason="register replacement canonical publish-tree",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            registration_kind="manual_store_seed",
        )
        submit_heavy_object_reference_request(
            anchor_root,
            object_id=new_id,
            object_kind="publish_tree",
            object_ref=new_store_ref,
            reference_ref=str(holder_ref),
            reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach replacement publish-tree to the same live holder path",
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
            return _fail("while the old publish-tree remains pinned, repo-global auto lifecycle must stop after canonical supersession")

        pinned_view = query_heavy_object_retention_view(anchor_root, object_id=old_id, object_ref=old_store_ref)
        if str(pinned_view.get("retention_state") or "") != "PINNED":
            return _fail("old superseded publish-tree must remain PINNED while the authoritative pin is active")
        if not old_store_ref.exists():
            return _fail("old canonical publish-tree ref must still exist while the authoritative pin blocks reclamation")

        released = submit_heavy_object_pin_release_request(
            anchor_root,
            object_id=old_id,
            object_kind="publish_tree",
            object_ref=old_store_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="release the policy pin so repo-global lifecycle may reclaim the superseded publish-tree",
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
            return _fail("after authoritative pin release, repo-global publish-tree lifecycle must resume GC eligibility and reclamation")

        release_trace = query_heavy_object_pin_release_trace_view(anchor_root, envelope_id=str(released.envelope_id or ""))
        released_pin = dict(release_trace.get("released_pin") or {})
        if str(released_pin.get("pin_holder_id") or "") != "publish:artifact-bundle":
            return _fail("publish-tree pin-release trace must preserve the released pin holder id")

        released_view = query_heavy_object_retention_view(anchor_root, object_id=old_id, object_ref=old_store_ref)
        if str(released_view.get("retention_state") or "") != "RECLAIMED":
            return _fail("after release, the superseded old publish-tree must eventually converge to retention_state=RECLAIMED")
        if old_store_ref.exists():
            return _fail("after release, repo-global publish-tree lifecycle must reclaim the superseded canonical directory ref")

    print(
        "[loop-system-repo-global-publish-tree-pin-release-auto-lifecycle][OK] "
        "repo-global control-plane keeps pinned superseded publish trees retained and then resumes authoritative lifecycle after pin release"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
