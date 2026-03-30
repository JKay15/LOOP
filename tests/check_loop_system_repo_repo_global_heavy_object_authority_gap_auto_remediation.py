#!/usr/bin/env python3
"""Validate repo-global control-plane heavy-object authority-gap auto-remediation convergence."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(
        f"[loop-system-repo-global-heavy-object-authority-gap-auto-remediation][FAIL] {msg}",
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


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> Path:
    path = repo_root / rel
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
        query_heavy_object_authority_gap_inventory_view,
        query_heavy_object_authority_gap_repo_remediation_trace_view,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_heavy_object_authority_gap_auto_remediation_") as repo_root:
        anchor_root = repo_root / ".loop"
        control_plane_root = anchor_root / "repo_control_plane"
        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/auto-remediation-primary.pack",
            b"repo-global-heavy-object-auto-remediation-primary\n",
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/auto-remediation-primary.pack",
            b"repo-global-heavy-object-auto-remediation-primary\n",
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global auto-remediation requires a live repo control-plane service")

        def _first_auto_remediation_ready() -> bool:
            required = _committed_events_by_type(
                control_plane_root,
                "heavy_object_authority_gap_repo_remediation_required",
            )
            requested = _committed_events_by_type(
                anchor_root,
                "heavy_object_authority_gap_repo_remediation_requested",
            )
            settled = _committed_events_by_type(
                anchor_root,
                "heavy_object_authority_gap_repo_remediation_settled",
            )
            return len(required) >= 1 and len(requested) >= 1 and len(settled) >= 1

        if not _wait_until(_first_auto_remediation_ready, timeout_s=8.0):
            return _fail(
                "repo-global control-plane must auto-publish heavy_object_authority_gap_repo_remediation_required "
                "and converge one authoritative repo remediation command/result when unmanaged heavy-object candidates exist"
            )

        required_events = _committed_events_by_type(
            control_plane_root,
            "heavy_object_authority_gap_repo_remediation_required",
        )
        requested_events = _committed_events_by_type(
            anchor_root,
            "heavy_object_authority_gap_repo_remediation_requested",
        )
        settled_events = _committed_events_by_type(
            anchor_root,
            "heavy_object_authority_gap_repo_remediation_settled",
        )
        if len(required_events) != 1 or len(requested_events) != 1 or len(settled_events) != 1:
            return _fail("unchanged initial gap snapshot must converge through exactly one required/requested/settled remediation chain")

        required_payload = dict(required_events[0].get("payload") or {})
        requested_payload = dict(requested_events[0].get("payload") or {})
        settled_payload = dict(settled_events[0].get("payload") or {})
        first_envelope_id = str(requested_payload.get("envelope_id") or "")
        first_signature = str(requested_payload.get("remediation_signature") or "")
        if not first_envelope_id:
            return _fail("auto-remediation requested event must preserve the accepted envelope_id")
        if not first_signature:
            return _fail("auto-remediation requested event must preserve a deterministic remediation_signature")
        if str(required_payload.get("object_kind") or "") != "mathlib_pack":
            return _fail("auto-remediation requirement must preserve object_kind")
        if str(required_payload.get("repo_root") or "") != str(repo_root.resolve()):
            return _fail("auto-remediation requirement must preserve repo_root")
        if str(required_payload.get("trigger_kind") or "") != "repo_control_plane_auto_remediation":
            return _fail("auto-remediation requirement must preserve repo_control_plane_auto_remediation trigger kind")
        if str(required_payload.get("remediation_signature") or "") != first_signature:
            return _fail("auto-remediation requirement and requested event must agree on remediation_signature")
        if str(requested_payload.get("requirement_event_id") or "") != str(required_events[0].get("event_id") or ""):
            return _fail("auto-remediation requested event must link back to the canonical requirement fact")
        if int(settled_payload.get("candidate_count") or 0) != 2:
            return _fail("auto-remediation settled event must report the full initial candidate count")
        if int(settled_payload.get("requested_candidate_count") or 0) != 1:
            return _fail("auto-remediation settled event must report one requested candidate for the duplicate-pair setup")
        if not bool(settled_payload.get("fully_managed_after")):
            return _fail("auto-remediation settled event must report fully_managed_after for the duplicate-pair setup")
        if int(settled_payload.get("unrequested_unmanaged_candidate_count_after") or 0) != 0:
            return _fail("auto-remediation settled event must report zero remaining unmanaged candidates after remediation")

        trace = query_heavy_object_authority_gap_repo_remediation_trace_view(
            anchor_root,
            envelope_id=first_envelope_id,
        )
        if not bool(trace.get("required_event_present")):
            return _fail("auto-remediation trace must expose the linked canonical requirement fact")
        if str(dict(trace.get("required_event") or {}).get("event_id") or "") != str(required_events[0].get("event_id") or ""):
            return _fail("auto-remediation trace must point at the linked canonical requirement event")
        final_inventory = dict(trace.get("final_inventory") or {})
        if int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0) != 0:
            return _fail("auto-remediation trace must expose zero unmanaged candidates after duplicate-pair remediation")
        candidate_refs = {
            str(dict(item).get("object_ref") or "")
            for item in list(final_inventory.get("candidates") or [])
        }
        if candidate_refs != {str(primary_ref), str(duplicate_ref)}:
            return _fail("auto-remediation trace must preserve both duplicate candidate refs")
        if list(trace.get("gaps") or []):
            return _fail(f"complete auto-remediation trace must not report causal gaps, got {trace.get('gaps')!r}")

        post_inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(post_inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("repo-global auto-remediation must converge the duplicate-pair setup to zero unmanaged candidates")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(_committed_events_by_type(control_plane_root, "heavy_object_authority_gap_repo_remediation_required")) != 1:
            return _fail("unchanged gap snapshot must not emit a second canonical remediation requirement fact")
        if len(_committed_events_by_type(anchor_root, "heavy_object_authority_gap_repo_remediation_requested")) != 1:
            return _fail("unchanged gap snapshot must not emit a second accepted auto-remediation command")
        if len(_committed_events_by_type(anchor_root, "heavy_object_authority_gap_repo_remediation_settled")) != 1:
            return _fail("unchanged gap snapshot must not emit a second settled auto-remediation event")

        third_ref = _make_pack(
            repo_root,
            "workspace/publish/auto-remediation-third.pack",
            b"repo-global-heavy-object-auto-remediation-third\n",
        )
        if not third_ref.exists():
            return _fail("test fixture must materialize a new unmanaged heavy-object candidate before changed-gap resubmission")

        def _second_auto_remediation_ready() -> bool:
            return (
                len(_committed_events_by_type(control_plane_root, "heavy_object_authority_gap_repo_remediation_required")) >= 2
                and len(_committed_events_by_type(anchor_root, "heavy_object_authority_gap_repo_remediation_requested")) >= 2
                and len(_committed_events_by_type(anchor_root, "heavy_object_authority_gap_repo_remediation_settled")) >= 2
            )

        if not _wait_until(_second_auto_remediation_ready, timeout_s=8.0):
            return _fail("changed unmanaged gap snapshot must converge through a second repo-global auto-remediation")

        required_events = _committed_events_by_type(
            control_plane_root,
            "heavy_object_authority_gap_repo_remediation_required",
        )
        requested_events = _committed_events_by_type(
            anchor_root,
            "heavy_object_authority_gap_repo_remediation_requested",
        )
        settled_events = _committed_events_by_type(
            anchor_root,
            "heavy_object_authority_gap_repo_remediation_settled",
        )
        second_required_payload = dict(required_events[-1].get("payload") or {})
        second_requested_payload = dict(requested_events[-1].get("payload") or {})
        second_settled_payload = dict(settled_events[-1].get("payload") or {})
        second_envelope_id = str(second_requested_payload.get("envelope_id") or "")
        second_signature = str(second_requested_payload.get("remediation_signature") or "")
        if not second_envelope_id or second_envelope_id == first_envelope_id:
            return _fail("changed gap snapshot must produce a new accepted remediation envelope")
        if not second_signature or second_signature == first_signature:
            return _fail("changed gap snapshot must produce a new deterministic remediation_signature")
        if str(second_required_payload.get("remediation_signature") or "") != second_signature:
            return _fail("second requirement fact must preserve the changed-gap remediation_signature")
        if int(second_settled_payload.get("candidate_count") or 0) != 1:
            return _fail("second auto-remediation settled event must report the new single unmanaged candidate")
        if int(second_settled_payload.get("requested_candidate_count") or 0) != 1:
            return _fail("second auto-remediation settled event must report one requested candidate")
        if not bool(second_settled_payload.get("fully_managed_after")):
            return _fail("second auto-remediation settled event must report fully_managed_after")

        final_trace = query_heavy_object_authority_gap_repo_remediation_trace_view(
            anchor_root,
            envelope_id=second_envelope_id,
        )
        final_inventory = dict(final_trace.get("final_inventory") or {})
        if int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0) != 0:
            return _fail("changed-gap auto-remediation trace must preserve zero remaining unmanaged candidates after remediation")
        candidate_refs = {
            str(dict(item).get("object_ref") or "")
            for item in list(final_inventory.get("candidates") or [])
        }
        if str(third_ref) not in candidate_refs:
            return _fail("changed-gap auto-remediation trace must preserve the new managed candidate ref")

    print(
        "[loop-system-repo-global-heavy-object-authority-gap-auto-remediation][OK] "
        "repo-global control-plane auto-remediation emits one authoritative remediation per unchanged gap snapshot "
        "and resubmits only after the gap changes"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
