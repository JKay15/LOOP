#!/usr/bin/env python3
"""Shared helpers for managed duplicate heavy-tree convergence tests."""

from __future__ import annotations

import time
from pathlib import Path


def persist_anchor_state(state_root: Path, *, task_id: str, root_goal: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id=task_id,
        root_goal=root_goal,
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice=root_goal,
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


def wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def write_publish_tree(root: Path, *, label: str) -> Path:
    (root / "formalization").mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text(f"# {label}\n", encoding="utf-8")
    (root / "formalization" / "STATUS.md").write_text(f"{label}: ready\n", encoding="utf-8")
    return root.resolve()


def write_runtime_heavy_tree(artifact_root: Path, *, label: str) -> Path:
    tree_root = artifact_root / ".lake"
    (tree_root / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
    (tree_root / "packages" / "mathlib" / "STATUS.txt").write_text(f"{label}: ready\n", encoding="utf-8")
    return tree_root.resolve()


def assert_managed_duplicate_convergence(
    *,
    repo_root: Path,
    anchor_root: Path,
    object_kind: str,
    holder_ref: Path,
    holder_kind: str,
    holder_ref_root: str,
    duplicate_store_ref: Path,
    runtime_name: str,
    label: str,
) -> None:
    from loop_product.artifact_hygiene import heavy_object_identity, materialize_canonical_heavy_object_store_ref
    from loop_product.kernel.query import (
        query_heavy_object_gc_eligibility_trace_view,
        query_heavy_object_observation_view,
        query_heavy_object_reference_inventory_view,
        query_heavy_object_reference_release_trace_view,
        query_heavy_object_reference_trace_view,
        query_heavy_object_reclamation_trace_view,
        query_heavy_object_retention_view,
        query_heavy_object_sharing_view,
        query_heavy_object_supersession_trace_view,
    )
    from loop_product.kernel.submit import (
        submit_heavy_object_reference_request,
        submit_heavy_object_registration_request,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running

    identity = heavy_object_identity(duplicate_store_ref, object_kind=object_kind)
    object_id = str(identity.get("object_id") or "")
    if not object_id:
        raise AssertionError(f"{label}: duplicate heavy object must have a stable object_id")

    canonical_store_ref = materialize_canonical_heavy_object_store_ref(
        repo_root=repo_root,
        object_kind=object_kind,
        object_id=object_id,
        source_ref=duplicate_store_ref,
    )
    if canonical_store_ref == duplicate_store_ref:
        raise AssertionError(f"{label}: fixture must keep duplicate store ref separate from canonical store ref")

    submit_heavy_object_registration_request(
        anchor_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=duplicate_store_ref,
        byte_size=int(identity.get("byte_size") or 0),
        reason=f"{label}: register managed duplicate store ref before convergence",
        runtime_name=runtime_name,
        repo_root=repo_root,
        registration_kind="manual_duplicate_seed",
    )
    duplicate_reference = submit_heavy_object_reference_request(
        anchor_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=duplicate_store_ref,
        reference_ref=str(holder_ref),
        reference_holder_kind=holder_kind,
        reference_holder_ref=str(holder_ref_root),
        reference_kind="manual_duplicate_seed_reference",
        reason=f"{label}: attach managed duplicate store ref to the live holder before convergence",
        runtime_name=runtime_name,
        repo_root=repo_root,
    )
    duplicate_reference_event = next(
        (
            dict(event)
            for event in committed_events_by_type(anchor_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(duplicate_reference.envelope_id or "")
        ),
        {},
    )
    duplicate_reference_event_id = str(duplicate_reference_event.get("event_id") or "")
    if not duplicate_reference_event_id:
        raise AssertionError(f"{label}: managed duplicate seed reference must leave a canonical reference event")

    submit_heavy_object_registration_request(
        anchor_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=canonical_store_ref,
        byte_size=int(heavy_object_identity(canonical_store_ref, object_kind=object_kind).get("byte_size") or 0),
        reason=f"{label}: register canonical store ref before managed duplicate convergence",
        runtime_name=runtime_name,
        repo_root=repo_root,
        registration_kind="manual_store_seed",
    )

    services = ensure_repo_control_plane_services_running(repo_root=repo_root)
    control_plane = dict(services.get("repo_control_plane") or {})
    if int(control_plane.get("pid") or 0) <= 0:
        raise AssertionError(f"{label}: repo-global managed duplicate convergence requires a live repo control-plane")

    def _converged() -> bool:
        replacement_events = [
            dict(event)
            for event in committed_events_by_type(anchor_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(canonical_store_ref)
            and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
            and str(dict(event.get("payload") or {}).get("reference_holder_kind") or "") == holder_kind
            and str(dict(event.get("payload") or {}).get("reference_holder_ref") or "") == str(holder_ref_root)
        ]
        release_events = [
            dict(event)
            for event in committed_events_by_type(anchor_root, "heavy_object_reference_released")
            if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(duplicate_store_ref)
            and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
            and str(dict(event.get("payload") or {}).get("reference_holder_kind") or "") == holder_kind
            and str(dict(event.get("payload") or {}).get("reference_holder_ref") or "") == str(holder_ref_root)
        ]
        supersession_events = [
            dict(event)
            for event in committed_events_by_type(anchor_root, "heavy_object_superseded")
            if str(dict(event.get("payload") or {}).get("superseded_object_ref") or "") == str(duplicate_store_ref)
            and str(dict(event.get("payload") or {}).get("replacement_object_ref") or "") == str(canonical_store_ref)
        ]
        gc_eligibility_events = [
            dict(event)
            for event in committed_events_by_type(anchor_root, "heavy_object_gc_eligible")
            if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(duplicate_store_ref)
            and str(dict(event.get("payload") or {}).get("superseded_by_object_ref") or "") == str(canonical_store_ref)
        ]
        reclaimed_events = [
            dict(event)
            for event in committed_events_by_type(anchor_root, "heavy_object_reclaimed")
            if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(duplicate_store_ref)
        ]
        return (
            len(replacement_events) >= 1
            and len(release_events) >= 1
            and len(supersession_events) >= 1
            and len(gc_eligibility_events) >= 1
            and len(reclaimed_events) >= 1
        )

    if not wait_until(_converged, timeout_s=8.0):
        raise AssertionError(
            f"{label}: repo-global control-plane must hand the managed duplicate holder back to the canonical store ref "
            "and then complete release/supersession/GC/reclamation for the duplicate store ref"
        )

    replacement_events = [
        dict(event)
        for event in committed_events_by_type(anchor_root, "heavy_object_reference_attached")
        if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(canonical_store_ref)
        and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
        and str(dict(event.get("payload") or {}).get("reference_holder_kind") or "") == holder_kind
        and str(dict(event.get("payload") or {}).get("reference_holder_ref") or "") == str(holder_ref_root)
    ]
    release_events = [
        dict(event)
        for event in committed_events_by_type(anchor_root, "heavy_object_reference_released")
        if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(duplicate_store_ref)
        and str(dict(event.get("payload") or {}).get("reference_ref") or "") == str(holder_ref)
        and str(dict(event.get("payload") or {}).get("reference_holder_kind") or "") == holder_kind
        and str(dict(event.get("payload") or {}).get("reference_holder_ref") or "") == str(holder_ref_root)
    ]
    supersession_events = [
        dict(event)
        for event in committed_events_by_type(anchor_root, "heavy_object_superseded")
        if str(dict(event.get("payload") or {}).get("superseded_object_ref") or "") == str(duplicate_store_ref)
        and str(dict(event.get("payload") or {}).get("replacement_object_ref") or "") == str(canonical_store_ref)
    ]
    gc_eligibility_events = [
        dict(event)
        for event in committed_events_by_type(anchor_root, "heavy_object_gc_eligible")
        if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(duplicate_store_ref)
        and str(dict(event.get("payload") or {}).get("superseded_by_object_ref") or "") == str(canonical_store_ref)
    ]
    reclaimed_events = [
        dict(event)
        for event in committed_events_by_type(anchor_root, "heavy_object_reclaimed")
        if str(dict(event.get("payload") or {}).get("object_ref") or "") == str(duplicate_store_ref)
    ]

    if (
        len(replacement_events) != 1
        or len(release_events) != 1
        or len(supersession_events) != 1
        or len(gc_eligibility_events) != 1
        or len(reclaimed_events) != 1
    ):
        raise AssertionError(
            f"{label}: managed duplicate convergence must settle through exactly one handoff attach plus one release/supersession/GC/reclaim chain"
        )

    replacement_event = replacement_events[0]
    replacement_payload = dict(replacement_event.get("payload") or {})
    release_event = release_events[0]
    release_payload = dict(release_event.get("payload") or {})
    supersession_event = supersession_events[0]
    supersession_payload = dict(supersession_event.get("payload") or {})
    gc_event = gc_eligibility_events[0]
    gc_payload = dict(gc_event.get("payload") or {})
    reclaimed_event = reclaimed_events[0]
    reclaimed_payload = dict(reclaimed_event.get("payload") or {})
    replacement_event_id = str(replacement_event.get("event_id") or "")

    if str(replacement_payload.get("reference_kind") or "") != "shared_reference_auto_handoff":
        raise AssertionError(f"{label}: canonical handoff attach must preserve reference_kind=shared_reference_auto_handoff")
    if str(replacement_payload.get("trigger_kind") or "") != "heavy_object_shared_reference_convergence":
        raise AssertionError(f"{label}: canonical handoff attach must preserve trigger_kind=heavy_object_shared_reference_convergence")
    if str(replacement_payload.get("trigger_ref") or "") != duplicate_reference_event_id:
        raise AssertionError(f"{label}: canonical handoff attach must link back to the displaced duplicate reference event")

    if str(release_payload.get("trigger_kind") or "") != "heavy_object_reference_replaced":
        raise AssertionError(f"{label}: managed duplicate release must preserve trigger_kind=heavy_object_reference_replaced")
    if str(release_payload.get("trigger_ref") or "") != replacement_event_id:
        raise AssertionError(f"{label}: managed duplicate release must link back to the canonical handoff attach event")
    if str(supersession_payload.get("trigger_kind") or "") != "heavy_object_reference_replaced":
        raise AssertionError(f"{label}: managed duplicate supersession must preserve trigger_kind=heavy_object_reference_replaced")
    if str(supersession_payload.get("trigger_ref") or "") != replacement_event_id:
        raise AssertionError(f"{label}: managed duplicate supersession must link back to the canonical handoff attach event")
    if str(gc_payload.get("trigger_kind") or "") != "heavy_object_auto_supersession":
        raise AssertionError(f"{label}: managed duplicate GC eligibility must preserve trigger_kind=heavy_object_auto_supersession")
    if str(gc_payload.get("trigger_ref") or "") != str(supersession_event.get("event_id") or ""):
        raise AssertionError(f"{label}: managed duplicate GC eligibility must link back to the supersession event")
    if str(reclaimed_payload.get("trigger_kind") or "") != "heavy_object_auto_gc_eligibility":
        raise AssertionError(f"{label}: managed duplicate reclamation must preserve trigger_kind=heavy_object_auto_gc_eligibility")
    if str(reclaimed_payload.get("trigger_ref") or "") != str(gc_event.get("event_id") or ""):
        raise AssertionError(f"{label}: managed duplicate reclamation must link back to the GC eligibility event")

    replacement_trace = query_heavy_object_reference_trace_view(
        anchor_root,
        envelope_id=str(replacement_payload.get("envelope_id") or ""),
    )
    if str(dict(replacement_trace.get("referenced_object") or {}).get("trigger_kind") or "") != "heavy_object_shared_reference_convergence":
        raise AssertionError(f"{label}: handoff reference trace must preserve shared-reference convergence trigger_kind")

    release_trace = query_heavy_object_reference_release_trace_view(
        anchor_root,
        envelope_id=str(release_payload.get("envelope_id") or ""),
    )
    if str(dict(release_trace.get("released_reference") or {}).get("trigger_ref") or "") != replacement_event_id:
        raise AssertionError(f"{label}: release trace must point back to the canonical handoff attach event")

    supersession_trace = query_heavy_object_supersession_trace_view(
        anchor_root,
        envelope_id=str(supersession_payload.get("envelope_id") or ""),
    )
    if str(dict(supersession_trace.get("supersession") or {}).get("trigger_ref") or "") != replacement_event_id:
        raise AssertionError(f"{label}: supersession trace must point back to the canonical handoff attach event")

    gc_trace = query_heavy_object_gc_eligibility_trace_view(
        anchor_root,
        envelope_id=str(gc_payload.get("envelope_id") or ""),
    )
    if str(dict(gc_trace.get("eligible_object") or {}).get("trigger_ref") or "") != str(supersession_event.get("event_id") or ""):
        raise AssertionError(f"{label}: GC eligibility trace must point back to the supersession event")

    reclamation_trace = query_heavy_object_reclamation_trace_view(
        anchor_root,
        envelope_id=str(reclaimed_payload.get("envelope_id") or ""),
    )
    if str(dict(reclamation_trace.get("reclamation_effect") or {}).get("trigger_ref") or "") != str(gc_event.get("event_id") or ""):
        raise AssertionError(f"{label}: reclamation trace must point back to the GC eligibility event")

    old_reference_inventory = query_heavy_object_reference_inventory_view(
        anchor_root,
        object_ref=duplicate_store_ref,
    )
    new_reference_inventory = query_heavy_object_reference_inventory_view(
        anchor_root,
        object_ref=canonical_store_ref,
    )
    if int(old_reference_inventory.get("active_reference_count") or 0) != 0:
        raise AssertionError(f"{label}: old duplicate store ref must have zero active references after convergence")
    if int(new_reference_inventory.get("active_reference_count") or 0) != 1:
        raise AssertionError(f"{label}: canonical store ref must have exactly one active holder reference after convergence")
    if list(new_reference_inventory.get("active_reference_refs") or []) != [str(holder_ref)]:
        raise AssertionError(f"{label}: canonical store ref must preserve the managed holder reference ref")

    old_retention = query_heavy_object_retention_view(
        anchor_root,
        object_ref=duplicate_store_ref,
    )
    new_retention = query_heavy_object_retention_view(
        anchor_root,
        object_ref=canonical_store_ref,
    )
    if str(old_retention.get("retention_state") or "") != "RECLAIMED":
        raise AssertionError(f"{label}: duplicate store ref must converge to retention_state=RECLAIMED")
    if str(new_retention.get("retention_state") or "") != "REFERENCED":
        raise AssertionError(f"{label}: canonical store ref must remain REFERENCED by the preserved holder")
    if duplicate_store_ref.exists():
        raise AssertionError(f"{label}: duplicate store ref must be reclaimed after convergence")
    if not canonical_store_ref.exists():
        raise AssertionError(f"{label}: canonical store ref must remain present after convergence")

    observation_view = query_heavy_object_observation_view(
        anchor_root,
        object_id=object_id,
        object_ref=canonical_store_ref,
    )
    if int(observation_view.get("matching_observation_count") or 0) < 1:
        raise AssertionError(f"{label}: canonical store ref must gain committed duplicate-observation support during managed duplicate convergence")
    if int(observation_view.get("observed_object_ref_count") or 0) < 2:
        raise AssertionError(f"{label}: managed duplicate convergence observation must preserve both canonical and duplicate refs")

    sharing_view = query_heavy_object_sharing_view(
        anchor_root,
        object_id=object_id,
        object_ref=canonical_store_ref,
    )
    if not bool(sharing_view.get("settled")):
        raise AssertionError(f"{label}: canonical store ref must report settled sharing truth after duplicate reclaim")
    if str(sharing_view.get("sharing_state") or "") != "SETTLED":
        raise AssertionError(f"{label}: canonical store ref must report sharing_state=SETTLED after duplicate reclaim")
    if int(sharing_view.get("retired_observed_ref_count") or 0) < 1:
        raise AssertionError(f"{label}: settled sharing truth must report the reclaimed duplicate ref as retired")
    if int(sharing_view.get("missing_observed_ref_count") or 0) != 0:
        raise AssertionError(f"{label}: settled sharing truth must not leave unresolved observed refs behind")
