#!/usr/bin/env python3
"""Validate read-only heavy-object dedup inventory visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-dedup-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-dedup-inventory-view",
        root_goal="validate read-only heavy-object dedup inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object dedup inventory validation",
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
        from loop_product.kernel.query import query_heavy_object_dedup_inventory_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_discovery_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_dedup_inventory_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        share_payload = b"heavy-object-dedup-candidate\n"
        share_id, share_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-share.pack",
            share_payload,
        )
        _, share_dup_a = _make_object(
            repo_root,
            "workspace/publish/share-a/dedup-share.pack",
            share_payload,
        )
        _, share_dup_b = _make_object(
            repo_root,
            "workspace/publish/share-b/dedup-share.pack",
            share_payload,
        )

        solo_payload = b"heavy-object-dedup-solo\n"
        solo_id, solo_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-solo.pack",
            solo_payload,
        )

        old_payload = b"heavy-object-dedup-old\n"
        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-old.pack",
            old_payload,
        )
        _, old_dup_ref = _make_object(
            repo_root,
            "workspace/publish/old-a/dedup-old.pack",
            old_payload,
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-new.pack",
            b"heavy-object-dedup-new\n",
        )

        other_id, other_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-other.pack",
            b"heavy-object-dedup-other\n",
        )
        _, other_dup_ref = _make_object(
            repo_root,
            "workspace/other-runtime/dedup-other.pack",
            b"heavy-object-dedup-other\n",
        )
        converged_payload = b"heavy-object-dedup-converged\n"
        converged_id, converged_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-converged.pack",
            converged_payload,
        )
        _, converged_dup_ref = _make_object(
            repo_root,
            "workspace/publish/converged/dedup-converged.pack",
            converged_payload,
        )
        settled_payload = b"heavy-object-dedup-settled\n"
        settled_id, settled_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-settled.pack",
            settled_payload,
        )
        _, settled_dup_ref = _make_object(
            repo_root,
            "workspace/publish/settled/dedup-settled.pack",
            settled_payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            byte_size=share_ref.stat().st_size,
            reason="register shareable dedup object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate refs for dedup candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            pin_holder_id="publish-staging",
            pin_kind="publish_pin",
            reason="retain the dedup candidate while still allowing sharing",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        # Discovery scans walk repo_root and observe all matching refs for the object payload.
        share_dup_a.write_bytes(share_payload)
        share_dup_b.write_bytes(share_payload)

        submit_heavy_object_registration_request(
            state_root,
            object_id=solo_id,
            object_kind="mathlib_pack",
            object_ref=solo_ref,
            byte_size=solo_ref.stat().st_size,
            reason="register single-ref discovered object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=solo_id,
            object_kind="mathlib_pack",
            object_ref=solo_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover only one ref for non-candidate object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register old object before supersession",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register replacement object before supersession",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover old object before supersession",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        old_dup_ref.write_bytes(old_payload)
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="replacement_pack",
            reason="replacement object supersedes old dedup candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=other_id,
            object_kind="mathlib_pack",
            object_ref=other_ref,
            byte_size=other_ref.stat().st_size,
            reason="register other-runtime object",
            runtime_name="other-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=other_id,
            object_kind="mathlib_pack",
            object_ref=other_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover object in other runtime",
            runtime_name="other-runtime",
            repo_root=repo_root,
        )
        other_dup_ref.write_bytes(b"heavy-object-dedup-other\n")
        submit_heavy_object_registration_request(
            state_root,
            object_id=converged_id,
            object_kind="mathlib_pack",
            object_ref=converged_ref,
            byte_size=converged_ref.stat().st_size,
            reason="register fully converged object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=converged_id,
            object_kind="mathlib_pack",
            object_ref=converged_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate refs for fully converged object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        converged_dup_ref.write_bytes(converged_payload)
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
            runtime_name="publish-runtime",
            repo_root=repo_root,
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
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_ref,
            byte_size=settled_ref.stat().st_size,
            reason="register fully settled object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_dup_ref,
            byte_size=settled_dup_ref.stat().st_size,
            reason="register settled duplicate ref",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate refs for the settled object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        settled_dup_ref.write_bytes(settled_payload)
        submit_heavy_object_reference_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_ref,
            reference_ref=settled_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=settled_ref.parent,
            reference_kind="settled_reference",
            reason="cover the settled canonical ref",
            runtime_name="publish-runtime",
            repo_root=repo_root,
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
            runtime_name="publish-runtime",
            repo_root=repo_root,
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
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_reclamation_request(
            state_root,
            object_id=settled_id,
            object_kind="mathlib_pack",
            object_ref=settled_dup_ref,
            reclamation_kind="shared_reference_settled",
            reason="reclaim the settled duplicate ref",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        settling_payload = b"heavy-object-dedup-settling\n"
        settling_id, settling_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/dedup-settling.pack",
            settling_payload,
        )
        _, settling_dup_ref = _make_object(
            repo_root,
            "workspace/publish/settling/dedup-settling.pack",
            settling_payload,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_ref,
            byte_size=settling_ref.stat().st_size,
            reason="register settling discovered object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_dup_ref,
            byte_size=settling_dup_ref.stat().st_size,
            reason="register settling discovered duplicate object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate refs for the settling object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        settling_dup_ref.write_bytes(settling_payload)
        submit_heavy_object_reference_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_ref,
            reference_ref=settling_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=settling_ref.parent,
            reference_kind="settling_reference",
            reason="cover the settling canonical ref",
            runtime_name="publish-runtime",
            repo_root=repo_root,
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
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_dup_ref,
            eligibility_kind="shared_reference_settling",
            reason="settling duplicate becomes reclaimable before reclaim unblocks",
            superseded_by_object_id=settling_id,
            superseded_by_object_ref=settling_ref,
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=settling_id,
            object_kind="mathlib_pack",
            object_ref=settling_dup_ref,
            pin_holder_id="settling-duplicate-holder",
            pin_kind="duplicate_ref_guard",
            reason="keep the settling duplicate blocked so the canonical object reports SETTLING",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        inventory = query_heavy_object_dedup_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            discovery_kind="matching_filename_sha256_scan",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("heavy-object dedup inventory must stay read-only")
        if int(inventory.get("matching_discovery_observation_count") or 0) != 6:
            return _fail("dedup inventory must count matching committed discovery observations")
        if int(inventory.get("discovered_object_count") or 0) != 6:
            return _fail("dedup inventory must count unique discovered objects")
        if int(inventory.get("dedup_candidate_count") or 0) != 1:
            return _fail("dedup inventory must expose exactly one current dedup candidate")
        if int(inventory.get("converged_discovered_object_count") or 0) != 1:
            return _fail("dedup inventory must expose exactly one fully converged discovered object")
        if int(inventory.get("settling_discovered_object_count") or 0) != 1:
            return _fail("dedup inventory must expose exactly one settling discovered object")
        if int(inventory.get("settled_discovered_object_count") or 0) != 1:
            return _fail("dedup inventory must expose exactly one settled discovered object")
        if int(inventory.get("blocked_discovered_object_count") or 0) != 2:
            return _fail("dedup inventory must expose the blocked discovered objects that are neither converged nor settled")
        if int(inventory.get("strongest_duplicate_count") or 0) != 3:
            return _fail("dedup inventory must preserve strongest duplicate evidence")

        candidates = list(inventory.get("dedup_candidates") or [])
        if len(candidates) != 1:
            return _fail("dedup inventory must expose one candidate summary")
        candidate = dict(candidates[0] or {})
        if str(candidate.get("object_id") or "") != share_id:
            return _fail("candidate summary must point at the shareable discovered object")
        if not bool(candidate.get("shareable")) or str(candidate.get("sharing_state") or "") != "SHAREABLE":
            return _fail("candidate summary must preserve sharing_state=SHAREABLE")
        if str(candidate.get("retention_state") or "") != "PINNED":
            return _fail("candidate summary must preserve retention_state from canonical pin facts")
        if int(candidate.get("active_pin_count") or 0) != 1:
            return _fail("candidate summary must preserve active pin count")
        if int(candidate.get("duplicate_count") or 0) != 3:
            return _fail("candidate summary must preserve duplicate_count")
        candidate_refs = [str(item) for item in list(candidate.get("discovered_object_refs") or [])]
        expected_candidate_refs = sorted([str(share_ref.resolve()), str(share_dup_a.resolve()), str(share_dup_b.resolve())])
        if candidate_refs != expected_candidate_refs:
            return _fail("candidate summary must expose normalized discovered refs")

        blocked = list(inventory.get("blocked_discovered_objects") or [])
        if len(blocked) != 2:
            return _fail("dedup inventory must expose both blocked discovered object summaries")
        first_blocked = dict(blocked[0] or {})
        second_blocked = dict(blocked[1] or {})
        if str(first_blocked.get("object_id") or "") != old_id or str(first_blocked.get("sharing_state") or "") != "SUPERSEDED":
            return _fail("blocked summaries must keep the superseded discovered object with strongest duplicate evidence first")
        if str(second_blocked.get("object_id") or "") != solo_id or str(second_blocked.get("sharing_state") or "") != "SINGLE_REF":
            return _fail("blocked summaries must preserve the single-ref discovered object")

        converged_objects = list(inventory.get("converged_discovered_objects") or [])
        if len(converged_objects) != 1:
            return _fail("dedup inventory must expose one converged discovered object summary")
        converged = dict(converged_objects[0] or {})
        if str(converged.get("object_id") or "") != converged_id:
            return _fail("converged discovered summary must point at the fully covered object")
        if str(converged.get("sharing_state") or "") != "CONVERGED":
            return _fail("converged discovered summary must preserve sharing_state=CONVERGED")
        if int(converged.get("duplicate_count") or 0) != 2:
            return _fail("converged discovered summary must preserve duplicate_count")

        settled_objects = list(inventory.get("settled_discovered_objects") or [])
        if len(settled_objects) != 1:
            return _fail("dedup inventory must expose one settled discovered object summary")
        settled = dict(settled_objects[0] or {})
        if str(settled.get("object_id") or "") != settled_id:
            return _fail("settled discovered summary must point at the settled canonical object")
        if str(settled.get("sharing_state") or "") != "SETTLED":
            return _fail("settled discovered summary must preserve sharing_state=SETTLED")
        if int(settled.get("retired_observed_ref_count") or 0) != 1:
            return _fail("settled discovered summary must report one retired duplicate ref")

        settling_objects = list(inventory.get("settling_discovered_objects") or [])
        if len(settling_objects) != 1:
            return _fail("dedup inventory must expose one settling discovered object summary")
        settling = dict(settling_objects[0] or {})
        if str(settling.get("object_id") or "") != settling_id:
            return _fail("settling discovered summary must point at the settling canonical object")
        if str(settling.get("sharing_state") or "") != "SETTLING":
            return _fail("settling discovered summary must preserve sharing_state=SETTLING")
        if int(settling.get("blocked_retired_observed_ref_count") or 0) != 1:
            return _fail("settling discovered summary must report one blocked retired duplicate ref")
        if int(settling.get("reclaimable_retired_observed_ref_count") or 0) != 0:
            return _fail("settling discovered summary must not invent reclaimable retired duplicate refs")

        if inventory.get("gaps"):
            return _fail(f"complete dedup inventory must not report gaps, got {inventory['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_dedup_inventory_no_candidate_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/no-candidate.pack",
            b"heavy-object-dedup-no-candidate\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="register single-ref discovered object without dedup candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover only one ref",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        no_candidate_inventory = query_heavy_object_dedup_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            discovery_kind="matching_filename_sha256_scan",
            repo_root=repo_root,
        )
        gaps = [str(item) for item in list(no_candidate_inventory.get("gaps") or [])]
        if int(no_candidate_inventory.get("dedup_candidate_count") or 0) != 0:
            return _fail("non-candidate inventory must not invent dedup candidates")
        if "missing_current_heavy_object_dedup_candidate" not in gaps:
            return _fail("dedup inventory must surface missing_current_heavy_object_dedup_candidate when discovery exists but no candidate remains")

    with temporary_repo_root(prefix="loop_system_heavy_object_dedup_inventory_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        gap_inventory = query_heavy_object_dedup_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            discovery_kind="matching_filename_sha256_scan",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("matching_discovery_observation_count") or 0) != 0:
            return _fail("gap dedup inventory must not invent matching discovery observations")
        gaps = [str(item) for item in list(gap_inventory.get("gaps") or [])]
        if "missing_matching_heavy_object_discovery_observation" not in gaps:
            return _fail("dedup inventory must surface missing_matching_heavy_object_discovery_observation when no discovery facts exist")

    print("[loop-system-heavy-object-dedup-inventory-view][OK] heavy-object dedup inventory stays read-only and exposes current committed dedup candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
