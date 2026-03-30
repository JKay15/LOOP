#!/usr/bin/env python3
"""Validate repo-global authority-gap remediation auto-attaches authoritative heavy-object references."""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(
        f"[loop-system-repo-global-heavy-object-authority-gap-auto-reference][FAIL] {msg}",
        file=sys.stderr,
    )
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _make_pack(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path.resolve()


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    from loop_product.kernel.query import (
        query_heavy_object_dedup_inventory_view,
        query_heavy_object_reference_inventory_view,
        query_heavy_object_reference_trace_view,
        query_heavy_object_sharing_view,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_heavy_object_authority_gap_auto_reference_") as repo_root:
        anchor_root = repo_root / ".loop"
        nested_runtime_root = repo_root / ".loop" / "heavy-object-auto-reference-run"
        duplicate_name = "auto-reference-primary.pack"
        payload = b"repo-global-heavy-object-auto-reference-primary\n"

        runtime_reference = _make_pack(
            nested_runtime_root
            / "artifacts"
            / "evaluator_runs"
            / "lane_a"
            / ".loop"
            / "ai_user"
            / "workspace"
            / "deliverables"
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / duplicate_name,
            payload,
        )
        publish_reference = _make_pack(
            repo_root
            / "workspace"
            / "heavy-object-auto-reference-run"
            / "deliverables"
            / ".primary_artifact.publish.autoref"
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / duplicate_name,
            payload,
        )
        object_id = f"sha256:{hashlib.sha256(payload).hexdigest()}"

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global auto-reference requires a live repo control-plane service")

        def _first_auto_reference_ready() -> bool:
            registrations = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
            ]
            references = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
            ]
            inventory = query_heavy_object_reference_inventory_view(
                anchor_root,
                object_id=object_id,
            )
            return (
                len(registrations) >= 1
                and len(references) >= 2
                and int(inventory.get("active_reference_count") or 0) >= 2
            )

        if not _wait_until(_first_auto_reference_ready, timeout_s=8.0):
            return _fail(
                "repo-global authority-gap remediation must converge to canonical heavy-object references "
                "for the discovered pack holder roots"
            )

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
        ]
        reference_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
        ]
        if len(registration_events) != 1:
            return _fail("initial unchanged gap snapshot must still converge through exactly one auto-generated registration")
        if len(reference_events) != 2:
            return _fail("initial unchanged gap snapshot must converge through exactly two canonical reference facts")

        reference_payloads = [dict(event.get("payload") or {}) for event in reference_events]
        if {
            str(payload.get("reference_ref") or "")
            for payload in reference_payloads
        } != {str(runtime_reference), str(publish_reference)}:
            return _fail("auto-reference events must preserve both discovered pack refs")
        if {
            str(payload.get("reference_holder_kind") or "")
            for payload in reference_payloads
        } != {"evaluator_workspace_artifact_root", "workspace_publication_staging_root"}:
            return _fail("auto-reference events must classify both holder kinds")
        if {
            str(payload.get("reference_kind") or "")
            for payload in reference_payloads
        } != {"authority_gap_auto_reference"}:
            return _fail("auto-reference events must preserve authority_gap_auto_reference kind")

        inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=object_id,
        )
        if int(inventory.get("active_reference_count") or 0) != 2:
            return _fail("reference inventory must report two active refs after auto-reference takeover")
        if {
            str(item)
            for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {"evaluator_workspace_artifact_root", "workspace_publication_staging_root"}:
            return _fail("reference inventory must surface both auto-classified holder kinds")
        if inventory.get("gaps"):
            return _fail(f"post-takeover reference inventory must not report gaps, got {inventory.get('gaps')!r}")

        sharing_view = query_heavy_object_sharing_view(
            anchor_root,
            object_id=object_id,
        )
        if bool(sharing_view.get("shareable")):
            return _fail("fully auto-referenced duplicate pack refs must no longer report shareable=true")
        if not bool(sharing_view.get("converged")):
            return _fail("fully auto-referenced duplicate pack refs must report converged=true")
        if str(sharing_view.get("sharing_state") or "") != "CONVERGED":
            return _fail("fully auto-referenced duplicate pack refs must report sharing_state=CONVERGED")
        if int(sharing_view.get("covered_observed_ref_count") or 0) != 2:
            return _fail("converged pack sharing view must report both observed refs as covered")
        if int(sharing_view.get("missing_observed_ref_count") or 0) != 0:
            return _fail("converged pack sharing view must report zero missing observed refs")

        dedup_inventory = query_heavy_object_dedup_inventory_view(
            anchor_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(dedup_inventory.get("dedup_candidate_count") or 0) != 0:
            return _fail("fully auto-referenced duplicate pack refs must not remain unresolved dedup candidates")
        if int(dedup_inventory.get("converged_discovered_object_count") or 0) < 1:
            return _fail("dedup inventory must count the auto-referenced pack as a converged discovered object")

        trace_envelope_id = str(reference_payloads[0].get("envelope_id") or "")
        trace = query_heavy_object_reference_trace_view(anchor_root, envelope_id=trace_envelope_id)
        if not bool(trace.get("matching_registration_present")):
            return _fail("auto-reference trace must still expose the matching canonical registration")
        referenced_object = dict(trace.get("referenced_object") or {})
        if str(referenced_object.get("reference_kind") or "") != "authority_gap_auto_reference":
            return _fail("auto-reference trace must expose authority_gap_auto_reference kind")
        if str(referenced_object.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("auto-reference trace must preserve the remediation trigger kind")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(
            [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
            ]
        ) != 2:
            return _fail("unchanged gap snapshot must not emit duplicate auto-reference facts")

        third_payload = b"repo-global-heavy-object-auto-reference-third\n"
        third_ref = _make_pack(
            repo_root
            / "workspace"
            / "heavy-object-auto-reference-run"
            / "deliverables"
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / "auto-reference-third.pack",
            third_payload,
        )
        third_object_id = f"sha256:{hashlib.sha256(third_payload).hexdigest()}"

        def _second_auto_reference_ready() -> bool:
            registrations = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
            ]
            references = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
            ]
            return len(registrations) >= 2 and len(references) >= 3

        if not _wait_until(_second_auto_reference_ready, timeout_s=8.0):
            return _fail("changed unmanaged gap snapshot must converge through a new auto-generated reference fact")

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
        ]
        if str(dict(registration_events[-1].get("payload") or {}).get("object_id") or "") != third_object_id:
            return _fail("changed unmanaged gap snapshot must converge through registration of the new object id")
        final_references = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "mathlib_pack"
        ]
        if str(dict(final_references[-1].get("payload") or {}).get("reference_ref") or "") != str(third_ref):
            return _fail("changed unmanaged gap snapshot must converge through a reference fact for the new ref")

    print(
        "[loop-system-repo-global-heavy-object-authority-gap-auto-reference][OK] "
        "repo-global authority-gap remediation deterministically fans out canonical heavy-object references"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
