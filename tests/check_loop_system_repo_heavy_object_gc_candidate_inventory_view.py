#!/usr/bin/env python3
"""Validate read-only heavy-object GC candidate inventory visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-gc-candidate-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-gc-candidate-inventory-view",
        root_goal="validate read-only heavy-object gc candidate inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object gc candidate inventory validation",
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


def _make_object(repo_root: Path, rel: str, payload: bytes) -> tuple[str, Path]:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}", path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.query import query_heavy_object_gc_candidate_inventory_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_gc_candidate_inventory_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        candidate_old_id, candidate_old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/candidate-old.pack",
            b"heavy-object-gc-candidate-old\n",
        )
        candidate_new_id, candidate_new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/candidate-new.pack",
            b"heavy-object-gc-candidate-new\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=candidate_old_id,
            object_kind="mathlib_pack",
            object_ref=candidate_old_ref,
            byte_size=candidate_old_ref.stat().st_size,
            reason="register reclaimable candidate old object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=candidate_new_id,
            object_kind="mathlib_pack",
            object_ref=candidate_new_ref,
            byte_size=candidate_new_ref.stat().st_size,
            reason="register reclaimable candidate replacement object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=candidate_old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=candidate_old_ref,
            replacement_object_id=candidate_new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=candidate_new_ref,
            supersession_kind="replacement_pack",
            reason="old candidate becomes superseded",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=candidate_old_id,
            object_kind="mathlib_pack",
            object_ref=candidate_old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old candidate becomes reclaimable",
            superseded_by_object_id=candidate_new_id,
            superseded_by_object_ref=candidate_new_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        pinned_old_id, pinned_old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pinned-old.pack",
            b"heavy-object-gc-pinned-old\n",
        )
        pinned_new_id, pinned_new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pinned-new.pack",
            b"heavy-object-gc-pinned-new\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=pinned_old_id,
            object_kind="mathlib_pack",
            object_ref=pinned_old_ref,
            byte_size=pinned_old_ref.stat().st_size,
            reason="register pinned old object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=pinned_new_id,
            object_kind="mathlib_pack",
            object_ref=pinned_new_ref,
            byte_size=pinned_new_ref.stat().st_size,
            reason="register pinned replacement object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=pinned_old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=pinned_old_ref,
            replacement_object_id=pinned_new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=pinned_new_ref,
            supersession_kind="replacement_pack",
            reason="pinned old object becomes superseded",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=pinned_old_id,
            object_kind="mathlib_pack",
            object_ref=pinned_old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="pinned old object becomes gc eligible",
            superseded_by_object_id=pinned_new_id,
            superseded_by_object_ref=pinned_new_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=pinned_old_id,
            object_kind="mathlib_pack",
            object_ref=pinned_old_ref,
            pin_holder_id="publish-staging",
            pin_kind="publish_pin",
            reason="pin blocks immediate reclamation",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        reclaimed_old_id, reclaimed_old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/reclaimed-old.pack",
            b"heavy-object-gc-reclaimed-old\n",
        )
        reclaimed_new_id, reclaimed_new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/reclaimed-new.pack",
            b"heavy-object-gc-reclaimed-new\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=reclaimed_old_id,
            object_kind="mathlib_pack",
            object_ref=reclaimed_old_ref,
            byte_size=reclaimed_old_ref.stat().st_size,
            reason="register reclaimed old object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=reclaimed_new_id,
            object_kind="mathlib_pack",
            object_ref=reclaimed_new_ref,
            byte_size=reclaimed_new_ref.stat().st_size,
            reason="register reclaimed replacement object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=reclaimed_old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=reclaimed_old_ref,
            replacement_object_id=reclaimed_new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=reclaimed_new_ref,
            supersession_kind="replacement_pack",
            reason="reclaimed old object becomes superseded",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=reclaimed_old_id,
            object_kind="mathlib_pack",
            object_ref=reclaimed_old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="reclaimed old object becomes gc eligible",
            superseded_by_object_id=reclaimed_new_id,
            superseded_by_object_ref=reclaimed_new_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_reclamation_request(
            state_root,
            object_id=reclaimed_old_id,
            object_kind="mathlib_pack",
            object_ref=reclaimed_old_ref,
            reclamation_kind="authoritative_gc_reclaim",
            reason="trusted runtime reclaims old object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        settling_old_id, settling_old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settling-old.pack",
            b"heavy-object-gc-settling-old\n",
        )
        settling_new_id, settling_new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settling-new.pack",
            b"heavy-object-gc-settling-old\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settling_old_id,
            object_kind="mathlib_pack",
            object_ref=settling_old_ref,
            byte_size=settling_old_ref.stat().st_size,
            reason="register settling duplicate object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settling_new_id,
            object_kind="mathlib_pack",
            object_ref=settling_new_ref,
            byte_size=settling_new_ref.stat().st_size,
            reason="register settling canonical object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=settling_new_id,
            object_kind="mathlib_pack",
            object_ref=settling_new_ref,
            reference_ref=str(settling_new_ref),
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=str(settling_new_ref.parent),
            reference_kind="settling_reference",
            reason="keep the settling canonical object authoritative",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=settling_old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=settling_old_ref,
            replacement_object_id=settling_new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=settling_new_ref,
            supersession_kind="shared_reference_settling",
            reason="settling duplicate ref is superseded by canonical authority",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=settling_old_id,
            object_kind="mathlib_pack",
            object_ref=settling_old_ref,
            eligibility_kind="shared_reference_settling",
            reason="settling duplicate is reclaimable",
            superseded_by_object_id=settling_new_id,
            superseded_by_object_ref=settling_new_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        blocked_old_id, blocked_old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settling-blocked-old.pack",
            b"heavy-object-gc-settling-blocked-old\n",
        )
        blocked_new_id, blocked_new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settling-blocked-new.pack",
            b"heavy-object-gc-settling-blocked-old\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=blocked_old_id,
            object_kind="mathlib_pack",
            object_ref=blocked_old_ref,
            byte_size=blocked_old_ref.stat().st_size,
            reason="register blocked settling duplicate object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=blocked_new_id,
            object_kind="mathlib_pack",
            object_ref=blocked_new_ref,
            byte_size=blocked_new_ref.stat().st_size,
            reason="register blocked settling canonical object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=blocked_new_id,
            object_kind="mathlib_pack",
            object_ref=blocked_new_ref,
            reference_ref=str(blocked_new_ref),
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=str(blocked_new_ref.parent),
            reference_kind="blocked_settling_reference",
            reason="keep the blocked settling canonical object authoritative",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=blocked_old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=blocked_old_ref,
            replacement_object_id=blocked_new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=blocked_new_ref,
            supersession_kind="shared_reference_settling",
            reason="blocked settling duplicate ref is superseded by canonical authority",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=blocked_old_id,
            object_kind="mathlib_pack",
            object_ref=blocked_old_ref,
            eligibility_kind="shared_reference_settling",
            reason="blocked settling duplicate becomes gc eligible before a pin blocks reclaim",
            superseded_by_object_id=blocked_new_id,
            superseded_by_object_ref=blocked_new_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=blocked_old_id,
            object_kind="mathlib_pack",
            object_ref=blocked_old_ref,
            pin_holder_id="blocked-duplicate-holder",
            pin_kind="duplicate_ref_guard",
            reason="block shared-reference settling duplicate reclaim",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        inventory = query_heavy_object_gc_candidate_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("heavy-object GC candidate inventory must stay read-only")
        if int(inventory.get("registered_object_count") or 0) != 10:
            return _fail("GC candidate inventory must count all matching registered objects")
        if int(inventory.get("gc_eligible_object_count") or 0) != 5:
            return _fail("GC candidate inventory must count all currently gc-eligible objects")
        if int(inventory.get("reclaimable_object_count") or 0) != 2:
            return _fail("GC candidate inventory must expose exactly two reclaimable candidates")
        if int(inventory.get("blocked_gc_eligible_object_count") or 0) != 2:
            return _fail("GC candidate inventory must expose exactly two blocked gc-eligible objects")
        if int(inventory.get("reclaimed_object_count") or 0) != 1:
            return _fail("GC candidate inventory must expose exactly one reclaimed object")
        if int(inventory.get("settlement_reclaimable_object_count") or 0) != 1:
            return _fail("GC candidate inventory must expose exactly one shared-reference settling reclaimable candidate")
        if int(inventory.get("settlement_blocked_object_count") or 0) != 1:
            return _fail("GC candidate inventory must expose exactly one shared-reference settling blocked candidate")

        candidates = list(inventory.get("gc_candidates") or [])
        if len(candidates) != 2:
            return _fail("GC candidate inventory must expose two reclaimable candidate summaries")
        ordinary_candidate = next(
            dict(item or {})
            for item in candidates
            if str(dict(item or {}).get("object_id") or "") == candidate_old_id
        )
        settling_candidate = next(
            dict(item or {})
            for item in candidates
            if str(dict(item or {}).get("object_id") or "") == settling_old_id
        )
        if not bool(ordinary_candidate.get("reclaimable")):
            return _fail("ordinary GC candidate summary must preserve reclaimable=true")
        if str(ordinary_candidate.get("retention_state") or "") != "GC_ELIGIBLE":
            return _fail("ordinary GC candidate summary must preserve retention_state=GC_ELIGIBLE")
        if int(ordinary_candidate.get("active_pin_count") or 0) != 0:
            return _fail("ordinary GC candidate summary must preserve zero active pins")
        if str(settling_candidate.get("gc_eligibility_kind") or "") != "shared_reference_settling":
            return _fail("shared-reference settling candidate summary must expose gc_eligibility_kind=shared_reference_settling")
        if not bool(settling_candidate.get("reclaimable")):
            return _fail("shared-reference settling candidate summary must preserve reclaimable=true")

        blocked = list(inventory.get("blocked_gc_eligible_objects") or [])
        if len(blocked) != 2:
            return _fail("GC candidate inventory must expose two blocked gc-eligible summaries")
        pinned_summary = next(
            dict(item or {})
            for item in blocked
            if str(dict(item or {}).get("object_id") or "") == pinned_old_id
        )
        settling_blocked_summary = next(
            dict(item or {})
            for item in blocked
            if str(dict(item or {}).get("object_id") or "") == blocked_old_id
        )
        if bool(pinned_summary.get("reclaimable")):
            return _fail("blocked pinned summary must preserve reclaimable=false")
        if str(pinned_summary.get("retention_state") or "") != "PINNED":
            return _fail("blocked pinned summary must preserve retention_state=PINNED")
        if int(pinned_summary.get("active_pin_count") or 0) != 1:
            return _fail("blocked pinned summary must preserve active pin count")
        if str(settling_blocked_summary.get("gc_eligibility_kind") or "") != "shared_reference_settling":
            return _fail("blocked settling summary must expose gc_eligibility_kind=shared_reference_settling")
        if str(settling_blocked_summary.get("retention_state") or "") != "PINNED":
            return _fail("blocked settling summary must preserve retention_state=PINNED")
        if int(settling_blocked_summary.get("active_pin_count") or 0) != 1:
            return _fail("blocked settling summary must preserve the blocking pin count")

        reclaimed = list(inventory.get("reclaimed_objects") or [])
        if len(reclaimed) != 1:
            return _fail("GC candidate inventory must expose one reclaimed summary")
        reclaimed_summary = dict(reclaimed[0] or {})
        if str(reclaimed_summary.get("object_id") or "") != reclaimed_old_id:
            return _fail("reclaimed summary must point at the reclaimed old object")
        if str(reclaimed_summary.get("retention_state") or "") != "RECLAIMED":
            return _fail("reclaimed summary must preserve retention_state=RECLAIMED")

        settlement_reclaimable = list(inventory.get("settlement_reclaimable_objects") or [])
        if len(settlement_reclaimable) != 1:
            return _fail("GC candidate inventory must expose one settlement reclaimable summary")
        if str(dict(settlement_reclaimable[0] or {}).get("object_id") or "") != settling_old_id:
            return _fail("settlement reclaimable summary must point at the shared-reference settling duplicate")

        settlement_blocked = list(inventory.get("settlement_blocked_objects") or [])
        if len(settlement_blocked) != 1:
            return _fail("GC candidate inventory must expose one settlement blocked summary")
        if str(dict(settlement_blocked[0] or {}).get("object_id") or "") != blocked_old_id:
            return _fail("settlement blocked summary must point at the blocked shared-reference settling duplicate")

        if inventory.get("gaps"):
            return _fail(f"complete GC candidate inventory must not report gaps, got {inventory['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_gc_candidate_inventory_no_reclaimable_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/no-reclaimable-old.pack",
            b"heavy-object-gc-no-reclaimable-old\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/no-reclaimable-new.pack",
            b"heavy-object-gc-no-reclaimable-new\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register blocked gc object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register blocked gc replacement",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="replacement_pack",
            reason="blocked gc object becomes superseded",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="blocked gc object becomes eligible",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            pin_holder_id="publish-staging",
            pin_kind="publish_pin",
            reason="pin blocks reclaimable candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        blocked_inventory = query_heavy_object_gc_candidate_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(blocked_inventory.get("reclaimable_object_count") or 0) != 0:
            return _fail("blocked inventory must not invent reclaimable candidates")
        gaps = [str(item) for item in list(blocked_inventory.get("gaps") or [])]
        if "missing_current_reclaimable_heavy_object" not in gaps:
            return _fail("GC candidate inventory must surface missing_current_reclaimable_heavy_object when eligibility exists but no reclaimable candidate remains")

    with temporary_repo_root(prefix="loop_system_heavy_object_gc_candidate_inventory_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        gap_inventory = query_heavy_object_gc_candidate_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("registered_object_count") or 0) != 0:
            return _fail("gap GC candidate inventory must not invent matching registrations")
        gaps = [str(item) for item in list(gap_inventory.get("gaps") or [])]
        if "missing_matching_heavy_object_registration" not in gaps:
            return _fail("GC candidate inventory must surface missing_matching_heavy_object_registration when no matching registrations exist")

    print("[loop-system-heavy-object-gc-candidate-inventory-view][OK] heavy-object GC candidate inventory stays read-only and exposes current committed reclaimable candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
