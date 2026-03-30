#!/usr/bin/env python3
"""Validate read-only heavy-object sharing visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-sharing-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-sharing-view",
        root_goal="validate read-only heavy-object sharing visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object sharing validation",
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
        from loop_product.kernel.query import query_heavy_object_sharing_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_observation_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_view_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        payload = b"heavy-object-sharing-pack\n"
        object_id, primary_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/shareable-primary.pack",
            payload,
        )
        _, duplicate_ref_a = _make_object(
            repo_root,
            "workspace/shareable-run/duplicate-a.pack",
            payload,
        )
        _, duplicate_ref_b = _make_object(
            repo_root,
            "artifacts/publish/shareable-run/duplicate-b.pack",
            payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            byte_size=primary_ref.stat().st_size,
            reason="register a shareable pack before the sharing view exists",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            object_refs=[duplicate_ref_a, duplicate_ref_b],
            duplicate_count=3,
            total_bytes=primary_ref.stat().st_size * 3,
            observation_kind="discovery_scan",
            reason="record duplicate refs for the shareable pack",
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            pin_holder_id="publish-staging",
            pin_kind="publish_pin",
            reason="keep the shareable pack retained while still allowing sharing",
        )

        share_view = query_heavy_object_sharing_view(state_root, object_id=object_id)
        if not bool(share_view.get("read_only")):
            return _fail("heavy-object sharing view must stay read-only")
        if not bool(share_view.get("registered_present")):
            return _fail("shareable object must report matching registration presence")
        if not bool(share_view.get("observation_present")):
            return _fail("shareable object must report matching heavy-object observation presence")
        if not bool(share_view.get("shareable")):
            return _fail("duplicate-observed object must report shareable=true")
        if str(share_view.get("sharing_state") or "") != "SHAREABLE":
            return _fail("duplicate-observed object must report sharing_state=SHAREABLE")
        if str(share_view.get("retention_state") or "") != "PINNED":
            return _fail("shareability must not erase the retention_state derived from canonical pin facts")
        if int(share_view.get("active_pin_count") or 0) != 1:
            return _fail("shareable pinned object must still expose active_pin_count")
        if int(share_view.get("observed_object_ref_count") or 0) != 3:
            return _fail("shareable object must expose all observed refs")
        if int(share_view.get("max_duplicate_count") or 0) != 3:
            return _fail("shareable object must preserve max_duplicate_count from committed observations")
        observed_refs = [str(item) for item in list(share_view.get("observed_object_refs") or [])]
        expected_refs = sorted(
            [
                str(primary_ref.resolve()),
                str(duplicate_ref_a.resolve()),
                str(duplicate_ref_b.resolve()),
            ]
        )
        if observed_refs != expected_refs:
            return _fail("shareable object must expose the normalized observed ref set")
        if share_view.get("gaps"):
            return _fail(f"complete shareable object must not report gaps, got {share_view['gaps']!r}")
        if bool(share_view.get("converged")):
            return _fail("shareable object must not report converged=true before duplicate refs are fully covered")
        if int(share_view.get("covered_observed_ref_count") or 0) != 0:
            return _fail("shareable object without reference coverage must report zero covered observed refs")
        if int(share_view.get("missing_observed_ref_count") or 0) != 3:
            return _fail("shareable object without reference coverage must report all observed refs as missing")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_view_converged_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        payload = b"heavy-object-sharing-converged-pack\n"
        object_id, primary_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/converged-primary.pack",
            payload,
        )
        _, duplicate_ref_a = _make_object(
            repo_root,
            "workspace/converged-run/duplicate-a.pack",
            payload,
        )
        _, duplicate_ref_b = _make_object(
            repo_root,
            "artifacts/publish/converged-run/duplicate-b.pack",
            payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            byte_size=primary_ref.stat().st_size,
            reason="register a converged pack before sharing convergence validation",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            object_refs=[duplicate_ref_a, duplicate_ref_b],
            duplicate_count=3,
            total_bytes=primary_ref.stat().st_size * 3,
            observation_kind="discovery_scan",
            reason="record duplicate refs for the converged pack",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            reference_ref=primary_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=primary_ref.parent,
            reference_kind="converged_reference",
            reason="cover the canonical heavy-object ref",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            reference_ref=duplicate_ref_a,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=duplicate_ref_a.parent,
            reference_kind="converged_reference",
            reason="cover the first duplicate heavy-object ref",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            reference_ref=duplicate_ref_b,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=duplicate_ref_b.parent,
            reference_kind="converged_reference",
            reason="cover the second duplicate heavy-object ref",
        )

        converged_view = query_heavy_object_sharing_view(state_root, object_id=object_id)
        if bool(converged_view.get("shareable")):
            return _fail("fully covered duplicate refs must no longer report shareable=true")
        if not bool(converged_view.get("converged")):
            return _fail("fully covered duplicate refs must report converged=true")
        if str(converged_view.get("sharing_state") or "") != "CONVERGED":
            return _fail("fully covered duplicate refs must report sharing_state=CONVERGED")
        if int(converged_view.get("active_reference_count") or 0) != 3:
            return _fail("converged sharing view must preserve all active authoritative references")
        if int(converged_view.get("covered_observed_ref_count") or 0) != 3:
            return _fail("converged sharing view must report every observed ref as covered")
        if int(converged_view.get("missing_observed_ref_count") or 0) != 0:
            return _fail("converged sharing view must report zero missing observed refs")
        covered_refs = [str(item) for item in list(converged_view.get("covered_observed_refs") or [])]
        expected_covered_refs = sorted(
            [
                str(primary_ref.resolve()),
                str(duplicate_ref_a.resolve()),
                str(duplicate_ref_b.resolve()),
            ]
        )
        if covered_refs != expected_covered_refs:
            return _fail("converged sharing view must expose the normalized covered observed ref set")
        if list(converged_view.get("missing_observed_refs") or []):
            return _fail("converged sharing view must not leave missing observed refs behind")
        if converged_view.get("gaps"):
            return _fail(f"converged sharing view must not report gaps, got {converged_view['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_view_settled_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        payload = b"heavy-object-sharing-settled-pack\n"
        object_id, canonical_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settled-canonical.pack",
            payload,
        )
        _, duplicate_ref = _make_object(
            repo_root,
            "workspace/settled-run/duplicate.pack",
            payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=canonical_ref,
            byte_size=canonical_ref.stat().st_size,
            reason="register canonical object for settled sharing validation",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=duplicate_ref,
            byte_size=duplicate_ref.stat().st_size,
            reason="register duplicate object for settled sharing validation",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=canonical_ref,
            object_refs=[duplicate_ref],
            duplicate_count=2,
            total_bytes=canonical_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record duplicate refs for settled sharing validation",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=canonical_ref,
            reference_ref=canonical_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=canonical_ref.parent,
            reference_kind="settled_reference",
            reason="keep the canonical ref authoritative while the duplicate retires",
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=object_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=duplicate_ref,
            replacement_object_id=object_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=canonical_ref,
            supersession_kind="shared_reference_settled",
            reason="duplicate ref is superseded by canonical authority",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=duplicate_ref,
            eligibility_kind="shared_reference_settled",
            reason="duplicate ref is now reclaimable after shared-reference settlement",
            superseded_by_object_id=object_id,
            superseded_by_object_ref=canonical_ref,
        )
        submit_heavy_object_reclamation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=duplicate_ref,
            reclamation_kind="shared_reference_settled",
            reason="reclaim the settled duplicate ref",
        )

        settled_view = query_heavy_object_sharing_view(
            state_root,
            object_id=object_id,
            object_ref=canonical_ref,
        )
        if bool(settled_view.get("shareable")):
            return _fail("settled duplicate refs must no longer report shareable=true")
        if bool(settled_view.get("converged")):
            return _fail("settled duplicate refs must not still report converged=true once the duplicate has been retired")
        if not bool(settled_view.get("settled")):
            return _fail("settled duplicate refs must report settled=true")
        if str(settled_view.get("sharing_state") or "") != "SETTLED":
            return _fail("settled duplicate refs must report sharing_state=SETTLED")
        if int(settled_view.get("active_reference_count") or 0) != 1:
            return _fail("settled sharing view must preserve only the canonical active reference")
        if int(settled_view.get("covered_observed_ref_count") or 0) != 1:
            return _fail("settled sharing view must still count the canonical observed ref as actively covered")
        if int(settled_view.get("retired_observed_ref_count") or 0) != 1:
            return _fail("settled sharing view must report the reclaimed duplicate ref as retired")
        if int(settled_view.get("resolved_observed_ref_count") or 0) != 2:
            return _fail("settled sharing view must report every observed ref as resolved")
        if int(settled_view.get("missing_observed_ref_count") or 0) != 0:
            return _fail("settled sharing view must report zero unresolved observed refs")
        retired_refs = [str(item) for item in list(settled_view.get("retired_observed_refs") or [])]
        if retired_refs != [str(duplicate_ref.resolve())]:
            return _fail("settled sharing view must expose the reclaimed duplicate ref as retired")
        if settled_view.get("gaps"):
            return _fail(f"settled sharing view must not report gaps, got {settled_view['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_view_settling_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        payload = b"heavy-object-sharing-settling-pack\n"
        object_id, canonical_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/settling-canonical.pack",
            payload,
        )
        _, duplicate_ref = _make_object(
            repo_root,
            "workspace/settling-run/duplicate.pack",
            payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=canonical_ref,
            byte_size=canonical_ref.stat().st_size,
            reason="register canonical object for settling sharing validation",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=duplicate_ref,
            byte_size=duplicate_ref.stat().st_size,
            reason="register duplicate object for settling sharing validation",
        )
        submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=canonical_ref,
            object_refs=[duplicate_ref],
            duplicate_count=2,
            total_bytes=canonical_ref.stat().st_size * 2,
            observation_kind="discovery_scan",
            reason="record duplicate refs for settling sharing validation",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=canonical_ref,
            reference_ref=canonical_ref,
            reference_holder_kind="canonical_store_root",
            reference_holder_ref=canonical_ref.parent,
            reference_kind="settling_reference",
            reason="keep the canonical ref authoritative while duplicate reclaim remains blocked",
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=object_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=duplicate_ref,
            replacement_object_id=object_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=canonical_ref,
            supersession_kind="shared_reference_settling",
            reason="duplicate ref is superseded by canonical authority",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=duplicate_ref,
            eligibility_kind="shared_reference_settling",
            reason="duplicate ref becomes gc eligible before a blocking pin is considered",
            superseded_by_object_id=object_id,
            superseded_by_object_ref=canonical_ref,
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=duplicate_ref,
            pin_holder_id="settling-duplicate-holder",
            pin_kind="duplicate_ref_guard",
            reason="block duplicate reclaim so the canonical object must report SETTLING",
        )

        settling_view = query_heavy_object_sharing_view(
            state_root,
            object_id=object_id,
            object_ref=canonical_ref,
        )
        if bool(settling_view.get("shareable")):
            return _fail("settling duplicate refs must no longer report shareable=true")
        if bool(settling_view.get("converged")):
            return _fail("settling duplicate refs must not still report converged=true once the duplicate has left active truth")
        if bool(settling_view.get("settled")):
            return _fail("settling duplicate refs must not report settled=true before duplicate reclaim finishes")
        if not bool(settling_view.get("settling")):
            return _fail("settling duplicate refs must report settling=true while duplicate reclaim remains blocked")
        if str(settling_view.get("sharing_state") or "") != "SETTLING":
            return _fail("settling duplicate refs must report sharing_state=SETTLING")
        if int(settling_view.get("covered_observed_ref_count") or 0) != 1:
            return _fail("settling sharing view must still count the canonical observed ref as actively covered")
        if int(settling_view.get("retired_observed_ref_count") or 0) != 1:
            return _fail("settling sharing view must report the duplicate ref as retired from active truth")
        if int(settling_view.get("blocked_retired_observed_ref_count") or 0) != 1:
            return _fail("settling sharing view must report one blocked retired duplicate ref")
        if int(settling_view.get("reclaimable_retired_observed_ref_count") or 0) != 0:
            return _fail("blocked settling sharing view must not invent reclaimable retired duplicate refs")
        if int(settling_view.get("reclaimed_retired_observed_ref_count") or 0) != 0:
            return _fail("blocked settling sharing view must not invent reclaimed retired duplicate refs")
        if int(settling_view.get("missing_observed_ref_count") or 0) != 0:
            return _fail("settling sharing view must not leave unresolved observed refs behind")
        blocked_refs = [str(item) for item in list(settling_view.get("blocked_retired_observed_refs") or [])]
        if blocked_refs != [str(duplicate_ref.resolve())]:
            return _fail("settling sharing view must expose the blocked retired duplicate ref")
        if settling_view.get("gaps"):
            return _fail(f"settling sharing view must not report gaps, got {settling_view['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_view_superseded_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_payload = b"heavy-object-sharing-old-pack\n"
        new_payload = b"heavy-object-sharing-new-pack\n"
        old_id, old_ref = _make_object(repo_root, "artifacts/heavy_objects/mathlib/old.pack", old_payload)
        _, old_dup_ref = _make_object(repo_root, "workspace/share-old/duplicate.pack", old_payload)
        new_id, new_ref = _make_object(repo_root, "artifacts/heavy_objects/mathlib/new.pack", new_payload)

        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register the old object before supersession",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register the replacement object before supersession",
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
            reason="record duplicate refs for the old object before supersession",
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
            reason="replacement object supersedes the old shareable object",
        )

        superseded_view = query_heavy_object_sharing_view(state_root, object_id=old_id)
        if bool(superseded_view.get("shareable")):
            return _fail("superseded object must no longer report shareable=true")
        if str(superseded_view.get("sharing_state") or "") != "SUPERSEDED":
            return _fail("superseded object must report sharing_state=SUPERSEDED")
        latest_superseded_by = dict(superseded_view.get("latest_superseded_by") or {})
        if str(latest_superseded_by.get("replacement_object_id") or "") != new_id:
            return _fail("superseded sharing view must expose the replacement object anchor")
        if superseded_view.get("gaps"):
            return _fail(f"superseded object with complete lifecycle evidence must not report gaps, got {superseded_view['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_sharing_view_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/unobserved.pack",
            b"heavy-object-sharing-gap-pack\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="register an object without any committed observation support",
        )

        gap_view = query_heavy_object_sharing_view(state_root, object_id=object_id)
        gaps = [str(item) for item in list(gap_view.get("gaps") or [])]
        if bool(gap_view.get("shareable")):
            return _fail("unobserved object must not report shareable=true")
        if str(gap_view.get("sharing_state") or "") != "UNOBSERVED":
            return _fail("registration without observation support must report sharing_state=UNOBSERVED")
        if "missing_matching_heavy_object_observation" not in gaps:
            return _fail("sharing gap view must surface missing_matching_heavy_object_observation")

    print("[loop-system-heavy-object-sharing-view][OK] heavy-object sharing view stays read-only and explains current committed shareability")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
