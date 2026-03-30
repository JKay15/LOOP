#!/usr/bin/env python3
"""Validate repo-global authority-gap remediation auto-registers the discovered primary heavy object."""

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
        f"[loop-system-repo-global-heavy-object-authority-gap-auto-registration][FAIL] {msg}",
        file=sys.stderr,
    )
    return 2


def _expected_canonical_store_ref(repo_root: Path, *, object_kind: str, object_id: str, is_dir: bool, suffix: str = "") -> Path:
    digest = str(object_id or "").removeprefix("sha256:").strip()
    leaf = "tree" if is_dir else f"object{suffix}"
    return (repo_root / ".loop" / "heavy_objects" / object_kind / digest / leaf).resolve()


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
        query_heavy_object_registration_trace_view,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_heavy_object_authority_gap_auto_registration_") as repo_root:
        anchor_root = repo_root / ".loop"
        payload = b"repo-global-heavy-object-auto-registration-primary\n"
        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/auto-registration-primary.pack",
            payload,
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/auto-registration-primary.pack",
            payload,
        )
        object_id = f"sha256:{hashlib.sha256(payload).hexdigest()}"

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global auto-registration requires a live repo control-plane service")

        def _first_auto_registration_ready() -> bool:
            remediation = _committed_events_by_type(anchor_root, "heavy_object_authority_gap_repo_remediation_requested")
            registrations = _committed_events_by_type(anchor_root, "heavy_object_registered")
            return len(remediation) >= 1 and len(registrations) >= 1

        if not _wait_until(_first_auto_registration_ready, timeout_s=8.0):
            return _fail(
                "repo-global authority-gap remediation must converge to one accepted heavy-object registration "
                "for the discovered primary object"
            )

        remediation_events = _committed_events_by_type(anchor_root, "heavy_object_authority_gap_repo_remediation_requested")
        candidate_remediation_events = _committed_events_by_type(anchor_root, "heavy_object_discovery_requested")
        registration_events = _committed_events_by_type(anchor_root, "heavy_object_registered")
        if len(remediation_events) != 1:
            return _fail("unchanged initial gap snapshot must converge through exactly one remediation command before auto-registration")
        if len(registration_events) != 1:
            return _fail("unchanged initial gap snapshot must converge through exactly one auto-generated heavy_object_registered fact")

        remediation_payload = dict(remediation_events[0].get("payload") or {})
        registration_payload = dict(registration_events[0].get("payload") or {})
        remediation_envelope_id = str(remediation_payload.get("envelope_id") or "")
        registration_envelope_id = str(registration_payload.get("envelope_id") or "")
        candidate_remediation = next(
            (
                dict(event)
                for event in candidate_remediation_events
                if str(dict(event.get("payload") or {}).get("discovery_kind") or "") == "authority_gap_remediation"
                and str(dict(event.get("payload") or {}).get("object_id") or "") == object_id
                and str(dict(event.get("payload") or {}).get("envelope_id") or "")
                == str(registration_payload.get("trigger_ref") or "")
            ),
            {},
        )
        candidate_remediation_payload = dict(candidate_remediation.get("payload") or {})
        candidate_remediation_envelope_id = str(candidate_remediation_payload.get("envelope_id") or "")
        expected_canonical_ref = _expected_canonical_store_ref(
            repo_root,
            object_kind="mathlib_pack",
            object_id=object_id,
            is_dir=False,
            suffix=".pack",
        )
        if not registration_envelope_id:
            return _fail("auto-generated registration event must preserve the accepted envelope_id")
        if not candidate_remediation_envelope_id:
            return _fail("auto-generated registration test must locate the matching authority-gap remediation discovery command")
        if str(registration_payload.get("object_id") or "") != object_id:
            return _fail("auto-generated registration event must preserve the discovered object_id")
        if str(registration_payload.get("object_ref") or "") != str(expected_canonical_ref):
            return _fail("auto-generated registration event must anchor the canonical content-addressed store ref instead of a candidate ref")
        if str(registration_payload.get("registration_kind") or "") != "authority_gap_auto_registration":
            return _fail("auto-generated registration event must preserve authority_gap_auto_registration kind")
        if str(registration_payload.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("auto-generated registration event must preserve heavy_object_authority_gap_remediation trigger kind")
        if str(registration_payload.get("trigger_ref") or "") != candidate_remediation_envelope_id:
            return _fail("auto-generated registration event must link back to the accepted per-candidate remediation envelope id")

        trace = query_heavy_object_registration_trace_view(anchor_root, envelope_id=registration_envelope_id)
        accepted_envelope = dict(trace.get("accepted_envelope") or {})
        registered_object = dict(trace.get("registered_object") or {})
        if str(accepted_envelope.get("registration_kind") or "") != "authority_gap_auto_registration":
            return _fail("registration trace must expose authority_gap_auto_registration on the accepted envelope")
        if str(accepted_envelope.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("registration trace must expose heavy_object_authority_gap_remediation trigger kind")
        if str(accepted_envelope.get("trigger_ref") or "") != candidate_remediation_envelope_id:
            return _fail("registration trace must expose the linked per-candidate remediation envelope id")
        if str(registered_object.get("registration_kind") or "") != "authority_gap_auto_registration":
            return _fail("registration trace must expose authority_gap_auto_registration on the canonical registration object")
        if str(registered_object.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("registration trace must expose trigger kind on the canonical registration object")
        if str(registered_object.get("trigger_ref") or "") != candidate_remediation_envelope_id:
            return _fail("registration trace must expose trigger ref on the canonical registration object")
        if not bool(trace.get("matching_observation_present")):
            return _fail("auto-generated registration trace must still expose the matching discovery observation")
        if list(trace.get("gaps") or []):
            return _fail(f"complete auto-generated registration trace must not report causal gaps, got {trace.get('gaps')!r}")

        inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(inventory.get("registered_candidate_count") or 0) != 2:
            return _fail("post-remediation inventory must report both duplicate candidates as registered after auto-registration")
        if int(inventory.get("discovery_covered_candidate_count") or 0) != 2:
            return _fail("post-remediation inventory must still report both duplicate refs as discovery-covered")
        if int(inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("post-remediation inventory must report zero unmanaged candidates after auto-registration")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(_committed_events_by_type(anchor_root, "heavy_object_registered")) != 1:
            return _fail("unchanged gap snapshot must not emit a second auto-generated registration")

        third_payload = b"repo-global-heavy-object-auto-registration-third\n"
        third_ref = _make_pack(
            repo_root,
            "workspace/publish/auto-registration-third.pack",
            third_payload,
        )
        third_object_id = f"sha256:{hashlib.sha256(third_payload).hexdigest()}"

        def _second_auto_registration_ready() -> bool:
            return len(_committed_events_by_type(anchor_root, "heavy_object_registered")) >= 2

        if not _wait_until(_second_auto_registration_ready, timeout_s=8.0):
            return _fail("changed unmanaged gap snapshot must converge through a second auto-generated registration")

        registration_events = _committed_events_by_type(anchor_root, "heavy_object_registered")
        second_registration_payload = dict(registration_events[-1].get("payload") or {})
        expected_third_canonical_ref = _expected_canonical_store_ref(
            repo_root,
            object_kind="mathlib_pack",
            object_id=third_object_id,
            is_dir=False,
            suffix=".pack",
        )
        if str(second_registration_payload.get("object_id") or "") != third_object_id:
            return _fail("second auto-generated registration must anchor the new discovered object id")
        if str(second_registration_payload.get("object_ref") or "") != str(expected_third_canonical_ref):
            return _fail("second auto-generated registration must anchor the new canonical content-addressed store ref")

        final_inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(final_inventory.get("registered_candidate_count") or 0) != 3:
            return _fail("final inventory must report all three candidates as registered after the changed-gap auto-registration")
        if int(final_inventory.get("managed_candidate_count") or 0) != 3:
            return _fail("final inventory must report all three candidates as managed after changed-gap auto-registration")
        if int(final_inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("final inventory must report zero unmanaged candidates after changed-gap auto-registration")

    print(
        "[loop-system-repo-global-heavy-object-authority-gap-auto-registration][OK] "
        "repo-global authority-gap remediation deterministically fans out auto-registration without duplicate replay"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
