#!/usr/bin/env python3
"""Validate read-only heavy-object sharing inventory visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-sharing-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _inventory_debug(inventory: dict[str, object]) -> str:
    sample_sections: list[str] = []
    for field in (
        "shareable_candidates",
        "converged_objects",
        "settling_objects",
        "settled_objects",
    ):
        entries = []
        for item in list(inventory.get(field) or []):
            if not isinstance(item, dict):
                continue
            entries.append(
                {
                    "object_id": str(item.get("object_id") or ""),
                    "object_ref": str(item.get("object_ref") or ""),
                    "sharing_state": str(item.get("sharing_state") or ""),
                }
            )
        sample_sections.append(f"{field}={entries!r}")
    return (
        f"registered_object_count={int(inventory.get('registered_object_count') or 0)} "
        f"shareable={int(inventory.get('shareable_object_count') or 0)} "
        f"converged={int(inventory.get('converged_object_count') or 0)} "
        f"settling={int(inventory.get('settling_object_count') or 0)} "
        f"settled={int(inventory.get('settled_object_count') or 0)} "
        f"superseded={int(inventory.get('superseded_object_count') or 0)} "
        f"unobserved={int(inventory.get('unobserved_object_count') or 0)} "
        f"gaps={list(inventory.get('gaps') or [])!r} "
        + " ".join(sample_sections)
    )


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-sharing-inventory-view",
        root_goal="validate read-only heavy-object sharing inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object sharing inventory validation",
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
        from loop_product.kernel.query import query_heavy_object_sharing_inventory_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_observation_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_inventory_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        share_payload = b"heavy-object-sharing-inventory-share\n"
        share_id, share_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/share.pack",
            share_payload,
        )
        _, share_dup_ref = _make_object(
            repo_root,
            "workspace/run-share/share-copy.pack",
            share_payload,
        )

        old_payload = b"heavy-object-sharing-inventory-old\n"
        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/old.pack",
            old_payload,
        )
        _, old_dup_ref = _make_object(
            repo_root,
            "workspace/run-old/old-copy.pack",
            old_payload,
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/new.pack",
            b"heavy-object-sharing-inventory-new\n",
        )

        solo_id, solo_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/solo.pack",
            b"heavy-object-sharing-inventory-solo\n",
        )
        converged_payload = b"heavy-object-sharing-inventory-converged\n"
        converged_id, converged_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/converged.pack",
            converged_payload,
        )
        _, converged_dup_ref = _make_object(
            repo_root,
            "workspace/run-converged/converged-copy.pack",
            converged_payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            byte_size=share_ref.stat().st_size,
            reason="register shareable object",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            object_refs=[share_dup_ref],
            duplicate_count=2,
            total_bytes=share_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record shareable duplicate refs",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register soon-to-be-superseded object",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register replacement object",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            object_refs=[old_dup_ref],
            duplicate_count=2,
            total_bytes=old_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record duplicate refs for superseded object",
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
            reason="replacement object supersedes old duplicate object",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=solo_id,
            object_kind="mathlib_pack",
            object_ref=solo_ref,
            byte_size=solo_ref.stat().st_size,
            reason="register unobserved solo object",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=converged_id,
            object_kind="mathlib_pack",
            object_ref=converged_ref,
            byte_size=converged_ref.stat().st_size,
            reason="register converged object",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=converged_id,
            object_kind="mathlib_pack",
            object_ref=converged_ref,
            object_refs=[converged_dup_ref],
            duplicate_count=2,
            total_bytes=converged_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record duplicate refs for converged object",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=converged_id,
            object_kind="mathlib_pack",
            object_ref=converged_ref,
            reference_ref=converged_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=converged_ref.parent,
            reference_kind="converged_reference",
            reason="cover the converged canonical ref",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=converged_id,
            object_kind="mathlib_pack",
            object_ref=converged_ref,
            reference_ref=converged_dup_ref,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=converged_dup_ref.parent,
            reference_kind="converged_reference",
            reason="cover the converged duplicate ref",
        )

        settled_payload = b"heavy-object-sharing-inventory-settled\n"
        settled_id, settled_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settled.pack",
            settled_payload,
        )
        _, settled_dup_ref = _make_object(
            repo_root,
            "workspace/run-settled/settled-copy.pack",
            settled_payload,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_ref,
            byte_size=settled_ref.stat().st_size,
            reason="register settled object",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_dup_ref,
            byte_size=settled_dup_ref.stat().st_size,
            reason="register settled duplicate object",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_ref,
            object_refs=[settled_dup_ref],
            duplicate_count=2,
            total_bytes=settled_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record duplicate refs for settled object",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_ref,
            reference_ref=settled_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=settled_ref.parent,
            reference_kind="settled_reference",
            reason="keep the settled canonical ref authoritative",
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=settled_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=settled_dup_ref,
            replacement_object_id=settled_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=settled_ref,
            supersession_kind="shared_reference_settled",
            reason="duplicate ref is superseded by canonical authority",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_dup_ref,
            eligibility_kind="shared_reference_settled",
            reason="settled duplicate becomes reclaimable",
            superseded_by_object_id=settled_id,
            superseded_by_object_ref=settled_ref,
        )
        submit_heavy_object_reclamation_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_dup_ref,
            reclamation_kind="shared_reference_settled",
            reason="reclaim the settled duplicate ref",
        )
        settling_payload = b"heavy-object-sharing-inventory-settling\n"
        settling_id, settling_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settling.pack",
            settling_payload,
        )
        _, settling_dup_ref = _make_object(
            repo_root,
            "workspace/run-settling/settling-copy.pack",
            settling_payload,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_ref,
            byte_size=settling_ref.stat().st_size,
            reason="register settling object",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_dup_ref,
            byte_size=settling_dup_ref.stat().st_size,
            reason="register settling duplicate object",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_ref,
            object_refs=[settling_dup_ref],
            duplicate_count=2,
            total_bytes=settling_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record duplicate refs for settling object",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_ref,
            reference_ref=settling_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=settling_ref.parent,
            reference_kind="settling_reference",
            reason="keep the settling canonical ref authoritative",
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=settling_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=settling_dup_ref,
            replacement_object_id=settling_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=settling_ref,
            supersession_kind="shared_reference_settling",
            reason="duplicate ref is superseded by canonical authority but reclaim is still blocked",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_dup_ref,
            eligibility_kind="shared_reference_settling",
            reason="settling duplicate becomes gc eligible before reclaim unblocks",
            superseded_by_object_id=settling_id,
            superseded_by_object_ref=settling_ref,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_dup_ref,
            pin_holder_id="settling-duplicate-holder",
            pin_kind="duplicate_ref_guard",
            reason="keep the duplicate blocked so the canonical object reports SETTLING",
        )

        inventory = query_heavy_object_sharing_inventory_view(
            state_root,
            object_kind="mathlib_pack",
        )
        if not bool(inventory.get("read_only")):
            return _fail("heavy-object sharing inventory must stay read-only")
        if int(inventory.get("registered_object_count") or 0) != 9:
            return _fail(
                "sharing inventory must count all matching registered objects, "
                f"including settling and settled duplicate refs; {_inventory_debug(inventory)}"
            )
        if int(inventory.get("shareable_object_count") or 0) != 1:
            return _fail("sharing inventory must report exactly one currently shareable object")
        if int(inventory.get("converged_object_count") or 0) != 1:
            return _fail("sharing inventory must report exactly one fully converged object")
        if int(inventory.get("settling_object_count") or 0) != 1:
            return _fail("sharing inventory must report exactly one settling object")
        if int(inventory.get("settled_object_count") or 0) != 1:
            return _fail("sharing inventory must report exactly one settled object")
        if int(inventory.get("superseded_object_count") or 0) != 3:
            return _fail("sharing inventory must report the superseded objects, including the settling and settled duplicate refs")
        if int(inventory.get("unobserved_object_count") or 0) != 2:
            return _fail("sharing inventory must report the two non-observed objects (replacement and solo)")
        if int(inventory.get("max_duplicate_count") or 0) != 2:
            return _fail("sharing inventory must preserve the strongest duplicate count across matching objects")

        shareable_candidates = list(inventory.get("shareable_candidates") or [])
        if len(shareable_candidates) != 1:
            return _fail("sharing inventory must expose one shareable candidate summary")
        candidate = dict(shareable_candidates[0] or {})
        if str(candidate.get("object_id") or "") != share_id:
            return _fail("shareable candidate summary must point at the shareable object")
        if str(candidate.get("sharing_state") or "") != "SHAREABLE":
            return _fail("shareable candidate summary must preserve sharing_state=SHAREABLE")
        if int(candidate.get("max_duplicate_count") or 0) != 2:
            return _fail("shareable candidate summary must preserve duplicate count")
        candidate_refs = [str(item) for item in list(candidate.get("observed_object_refs") or [])]
        expected_refs = sorted([str(share_ref.resolve()), str(share_dup_ref.resolve())])
        if candidate_refs != expected_refs:
            return _fail("shareable candidate summary must expose normalized observed refs")

        converged_objects = list(inventory.get("converged_objects") or [])
        if len(converged_objects) != 1:
            return _fail("sharing inventory must expose one converged object summary")
        converged = dict(converged_objects[0] or {})
        if str(converged.get("object_id") or "") != converged_id:
            return _fail("converged summary must point at the fully covered object")
        if str(converged.get("sharing_state") or "") != "CONVERGED":
            return _fail("converged summary must preserve sharing_state=CONVERGED")
        if int(converged.get("covered_observed_ref_count") or 0) != 2:
            return _fail("converged summary must report all observed refs as covered")
        if int(converged.get("missing_observed_ref_count") or 0) != 0:
            return _fail("converged summary must report zero missing observed refs")

        settling_objects = list(inventory.get("settling_objects") or [])
        if len(settling_objects) != 1:
            return _fail("sharing inventory must expose one settling object summary")
        settling = dict(settling_objects[0] or {})
        if str(settling.get("object_id") or "") != settling_id:
            return _fail("settling summary must point at the settling canonical object")
        if str(settling.get("sharing_state") or "") != "SETTLING":
            return _fail("settling summary must preserve sharing_state=SETTLING")
        if int(settling.get("blocked_retired_observed_ref_count") or 0) != 1:
            return _fail("settling summary must report one blocked retired duplicate ref")
        if int(settling.get("reclaimable_retired_observed_ref_count") or 0) != 0:
            return _fail("settling summary must not invent reclaimable retired duplicate refs")

        settled_objects = list(inventory.get("settled_objects") or [])
        if len(settled_objects) != 1:
            return _fail("sharing inventory must expose one settled object summary")
        settled = dict(settled_objects[0] or {})
        if str(settled.get("object_id") or "") != settled_id:
            return _fail("settled summary must point at the settled canonical object")
        if str(settled.get("sharing_state") or "") != "SETTLED":
            return _fail("settled summary must preserve sharing_state=SETTLED")
        if int(settled.get("covered_observed_ref_count") or 0) != 1:
            return _fail("settled summary must preserve active canonical coverage")
        if int(settled.get("retired_observed_ref_count") or 0) != 1:
            return _fail("settled summary must report the reclaimed duplicate ref as retired")
        if int(settled.get("missing_observed_ref_count") or 0) != 0:
            return _fail("settled summary must report zero unresolved observed refs")

        if inventory.get("gaps"):
            return _fail(f"complete sharing inventory must not report gaps, got {inventory['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_inventory_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        gap_inventory = query_heavy_object_sharing_inventory_view(
            state_root,
            object_kind="mathlib_pack",
        )
        gaps = [str(item) for item in list(gap_inventory.get("gaps") or [])]
        if int(gap_inventory.get("registered_object_count") or 0) != 0:
            return _fail("gap sharing inventory must not invent registered objects")
        if "missing_matching_heavy_object_registration" not in gaps:
            return _fail("gap sharing inventory must surface missing_matching_heavy_object_registration")

    print("[loop-system-heavy-object-sharing-inventory-view][OK] heavy-object sharing inventory stays read-only and exposes current committed shareable candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
