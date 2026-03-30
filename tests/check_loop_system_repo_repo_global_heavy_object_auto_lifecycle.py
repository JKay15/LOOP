#!/usr/bin/env python3
"""Validate repo-global heavy-object auto lifecycle convergence from reference replacement truth."""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-global-heavy-object-auto-lifecycle][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-repo-global-heavy-object-auto-lifecycle",
        root_goal="validate repo-global heavy-object auto lifecycle convergence",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-global heavy-object auto lifecycle validation",
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
    from loop_product.artifact_hygiene import classify_mathlib_pack_reference_holder
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

    with temporary_repo_root(prefix="loop_system_repo_global_heavy_object_auto_lifecycle_") as repo_root:
        anchor_root = repo_root / ".loop"
        _persist_anchor_state(anchor_root)
        holder_ref = (
            repo_root
            / "workspace"
            / "heavy-object-auto-lifecycle"
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
        ).resolve()
        old_id, old_store_ref = _write_bytes(
            repo_root / "artifacts" / "heavy_objects" / "mathlib" / "old-auto-lifecycle.pack",
            b"heavy-object-auto-lifecycle-old\n",
        )
        _write_bytes(holder_ref, b"heavy-object-auto-lifecycle-old\n")
        holder = classify_mathlib_pack_reference_holder(holder_ref, repo_root=repo_root)
        if str(holder.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("auto lifecycle fixture must classify the holder as workspace_publication_staging_root")

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
            reference_ref=str(holder_ref),
            reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach old store object to the live holder path",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global auto lifecycle requires a live repo control-plane service")

        new_id, new_store_ref = _write_bytes(
            repo_root / "artifacts" / "heavy_objects" / "mathlib" / "new-auto-lifecycle.pack",
            b"heavy-object-auto-lifecycle-new\n",
        )
        _write_bytes(holder_ref, b"heavy-object-auto-lifecycle-new\n")
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
        replacement_reference = submit_heavy_object_reference_request(
            anchor_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_store_ref,
            reference_ref=str(holder_ref),
            reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
            reference_kind="manual_store_seed_reference",
            reason="attach replacement store object to the same live holder path",
            runtime_name="publish-runtime",
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
            return len(releases) >= 1 and len(supersessions) >= 1 and len(gc_eligible) >= 1 and len(reclaimed) >= 1

        if not _wait_until(_auto_lifecycle_ready, timeout_s=8.0):
            return _fail(
                "repo-global control-plane must auto-converge reference replacement through supersession, "
                "GC eligibility, and reclamation"
            )

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
        if (
            len(release_events) != 1
            or len(supersession_events) != 1
            or len(gc_eligibility_events) != 1
            or len(reclaimed_events) != 1
        ):
            return _fail("unchanged replacement snapshot must converge through exactly one canonical release plus one lifecycle chain")

        release_event = release_events[0]
        release_payload = dict(release_event.get("payload") or {})
        supersession_event = supersession_events[0]
        gc_eligibility_event = gc_eligibility_events[0]
        reclaimed_event = reclaimed_events[0]
        supersession_payload = dict(supersession_event.get("payload") or {})
        gc_payload = dict(gc_eligibility_event.get("payload") or {})
        reclaimed_payload = dict(reclaimed_event.get("payload") or {})
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
            return _fail("auto lifecycle test must locate the canonical replacement reference event")
        if str(release_payload.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("auto handoff release must preserve heavy_object_reference_replaced trigger kind")
        if str(release_payload.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("auto handoff release must link back to the replacing canonical reference event")
        if str(supersession_payload.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("auto supersession must preserve heavy_object_reference_replaced trigger kind")
        if str(supersession_payload.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("auto supersession must link back to the replacing canonical reference event")
        if str(gc_payload.get("trigger_kind") or "") != "heavy_object_auto_supersession":
            return _fail("auto GC eligibility must preserve heavy_object_auto_supersession trigger kind")
        if str(gc_payload.get("trigger_ref") or "") != str(supersession_event.get("event_id") or ""):
            return _fail("auto GC eligibility must link back to the canonical supersession event")
        if str(reclaimed_payload.get("trigger_kind") or "") != "heavy_object_auto_gc_eligibility":
            return _fail("auto reclamation must preserve heavy_object_auto_gc_eligibility trigger kind")
        if str(reclaimed_payload.get("trigger_ref") or "") != str(gc_eligibility_event.get("event_id") or ""):
            return _fail("auto reclamation must link back to the canonical GC-eligibility event")

        release_trace = query_heavy_object_reference_release_trace_view(
            anchor_root,
            envelope_id=str(release_payload.get("envelope_id") or ""),
        )
        released_reference = dict(release_trace.get("released_reference") or {})
        if str(released_reference.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("auto handoff release trace must preserve trigger_kind")
        if str(released_reference.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("auto handoff release trace must preserve trigger_ref")

        supersession_trace = query_heavy_object_supersession_trace_view(
            anchor_root,
            envelope_id=str(supersession_payload.get("envelope_id") or ""),
        )
        supersession_summary = dict(supersession_trace.get("supersession") or {})
        if str(supersession_summary.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("auto supersession trace must preserve trigger_kind")
        if str(supersession_summary.get("trigger_ref") or "") != replacement_reference_event_id:
            return _fail("auto supersession trace must preserve trigger_ref")

        gc_trace = query_heavy_object_gc_eligibility_trace_view(
            anchor_root,
            envelope_id=str(gc_payload.get("envelope_id") or ""),
        )
        eligible_object = dict(gc_trace.get("eligible_object") or {})
        if str(eligible_object.get("trigger_kind") or "") != "heavy_object_auto_supersession":
            return _fail("auto GC trace must preserve trigger_kind")
        if str(eligible_object.get("trigger_ref") or "") != str(supersession_event.get("event_id") or ""):
            return _fail("auto GC trace must preserve trigger_ref")

        reclamation_trace = query_heavy_object_reclamation_trace_view(
            anchor_root,
            envelope_id=str(reclaimed_payload.get("envelope_id") or ""),
        )
        reclamation_effect = dict(reclamation_trace.get("reclamation_effect") or {})
        if str(reclamation_effect.get("trigger_kind") or "") != "heavy_object_auto_gc_eligibility":
            return _fail("auto reclamation trace must preserve trigger_kind")
        if str(reclamation_effect.get("trigger_ref") or "") != str(gc_eligibility_event.get("event_id") or ""):
            return _fail("auto reclamation trace must preserve trigger_ref")

        old_reference_inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=old_id,
            object_ref=old_store_ref,
        )
        if int(old_reference_inventory.get("active_reference_count") or 0) != 0:
            return _fail("replaced old object must no longer have active authoritative references after handoff release")
        if int(old_reference_inventory.get("released_reference_count") or 0) != 1:
            return _fail("replaced old object must expose one released authoritative reference after handoff release")

        new_reference_inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=new_id,
            object_ref=new_store_ref,
        )
        if int(new_reference_inventory.get("active_reference_count") or 0) != 1:
            return _fail("replacement object must retain one active authoritative reference after handoff release")

        old_view = query_heavy_object_retention_view(anchor_root, object_id=old_id, object_ref=old_store_ref)
        if str(old_view.get("retention_state") or "") != "RECLAIMED":
            return _fail("auto lifecycle must drive the replaced object to retention_state=RECLAIMED")
        if old_store_ref.exists():
            return _fail("auto lifecycle must reclaim the superseded canonical heavy-object store path")

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
            return _fail("unchanged lifecycle snapshot must not emit duplicate auto handoff release facts")
        if len(
            [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_superseded")
                if str(dict(event.get("payload") or {}).get("superseded_object_id") or "") == old_id
                and str(dict(event.get("payload") or {}).get("replacement_object_id") or "") == new_id
            ]
        ) != 1:
            return _fail("unchanged lifecycle snapshot must not emit duplicate auto supersession facts")

    print(
        "[loop-system-repo-global-heavy-object-auto-lifecycle][OK] "
        "repo-global control-plane deterministically auto-converges reference replacement through canonical handoff release, supersession, GC eligibility, and reclamation"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
