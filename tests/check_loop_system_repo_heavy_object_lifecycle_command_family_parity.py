#!/usr/bin/env python3
"""Validate read-only command parity debugger over heavy-object lifecycle commands."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-lifecycle-command-family-parity][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-lifecycle-command-family-parity",
        root_goal="validate heavy-object lifecycle command-family parity",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object lifecycle command-family parity validation",
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
        from loop_product.event_journal import committed_event_count, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_command_parity_debugger_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_pin_release_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_reference_release_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_registration_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-registration-parity.pack",
            b"heavy-object-registration-parity\n",
        )
        accepted = submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="exercise heavy-object registration parity",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted registration command required for parity")
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("registration parity debugger must stay read-only")
        if str(debug.get("trace_surface") or "") != "heavy_object_registration_trace":
            return _fail("registration parity debugger must route through registration trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted registration command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted registration command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_registration":
            return _fail("trusted expected effect must expose heavy_object_registration command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_pin_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-pin-parity.pack",
            b"heavy-object-pin-parity\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="register heavy object before pin parity",
        )
        accepted = submit_heavy_object_pin_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="exercise heavy-object pin parity",
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_pin_trace":
            return _fail("pin parity debugger must route through pin trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted pin command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted pin command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_pin":
            return _fail("trusted expected effect must expose heavy_object_pin command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_pin_release_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-pin-release-parity.pack",
            b"heavy-object-pin-release-parity\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="register heavy object before pin-release parity",
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="attach pin before exercising pin-release parity",
        )
        accepted = submit_heavy_object_pin_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="exercise heavy-object pin-release parity",
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_pin_release_trace":
            return _fail("pin-release parity debugger must route through pin-release trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted pin-release command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted pin-release command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_pin_release":
            return _fail("trusted expected effect must expose heavy_object_pin_release command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_reference_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-reference-parity.pack",
            b"heavy-object-reference-parity\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="register heavy object before reference parity",
        )
        reference_holder_ref = repo_root / "workspace" / "reference-parity" / "deliverables" / "primary_artifact"
        reference_ref = (
            reference_holder_ref / ".lake" / "packages" / "mathlib" / ".git" / "objects" / "pack" / object_ref.name
        )
        reference_ref.parent.mkdir(parents=True, exist_ok=True)
        reference_ref.write_bytes(b"heavy-object-reference-parity\n")
        accepted = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_runtime_reference",
            reason="exercise heavy-object reference parity",
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_reference_trace":
            return _fail("reference parity debugger must route through reference trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted reference command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted reference command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_reference":
            return _fail("trusted expected effect must expose heavy_object_reference command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_reference_release_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-reference-release-parity.pack",
            b"heavy-object-reference-release-parity\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=object_ref.stat().st_size,
            reason="register heavy object before reference-release parity",
        )
        reference_holder_ref = repo_root / "workspace" / "reference-release-parity" / "deliverables" / "primary_artifact"
        reference_ref = (
            reference_holder_ref / ".lake" / "packages" / "mathlib" / ".git" / "objects" / "pack" / object_ref.name
        )
        reference_ref.parent.mkdir(parents=True, exist_ok=True)
        reference_ref.write_bytes(b"heavy-object-reference-release-parity\n")
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_runtime_reference",
            reason="attach reference before exercising reference-release parity",
        )
        accepted = submit_heavy_object_reference_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_runtime_reference",
            reason="exercise heavy-object reference-release parity",
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_reference_release_trace":
            return _fail("reference-release parity debugger must route through reference-release trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted reference-release command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted reference-release command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_reference_release":
            return _fail("trusted expected effect must expose heavy_object_reference_release command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_supersession_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old-supersession-parity.pack",
            b"heavy-object-old-supersession-parity\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new-supersession-parity.pack",
            b"heavy-object-new-supersession-parity\n",
        )
        for object_id, object_ref in ((old_id, old_ref), (new_id, new_ref)):
            submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason="register heavy objects before supersession parity",
            )
        accepted = submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="publish_bundle_replacement",
            reason="exercise heavy-object supersession parity",
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_supersession_trace":
            return _fail("supersession parity debugger must route through supersession trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted supersession command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted supersession command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_supersession":
            return _fail("trusted expected effect must expose heavy_object_supersession command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_gc_eligibility_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old-gc-parity.pack",
            b"heavy-object-old-gc-parity\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new-gc-parity.pack",
            b"heavy-object-new-gc-parity\n",
        )
        for object_id, object_ref in ((old_id, old_ref), (new_id, new_ref)):
            submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason="register heavy objects before gc-eligibility parity",
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
            reason="support gc-eligibility parity with supersession",
        )
        accepted = submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="exercise heavy-object gc-eligibility parity",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_gc_eligibility_trace":
            return _fail("gc-eligibility parity debugger must route through gc-eligibility trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted gc-eligibility command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted gc-eligibility command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_gc_eligibility":
            return _fail("trusted expected effect must expose heavy_object_gc_eligibility command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_reclamation_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old-reclamation-parity.pack",
            b"heavy-object-old-reclamation-parity\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new-reclamation-parity.pack",
            b"heavy-object-new-reclamation-parity\n",
        )
        for object_id, object_ref in ((old_id, old_ref), (new_id, new_ref)):
            submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason="register heavy objects before reclamation parity",
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
            reason="support reclamation parity with supersession",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="support reclamation parity with gc eligibility",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
        )
        accepted = submit_heavy_object_reclamation_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            reclamation_kind="authoritative_gc_reclaim",
            reason="exercise heavy-object reclamation parity",
        )
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(debug.get("trace_surface") or "") != "heavy_object_reclamation_trace":
            return _fail("reclamation parity debugger must route through reclamation trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted reclamation command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted reclamation command must not report mismatches")
        if str(dict(debug.get("trusted_expected_effect") or {}).get("command_kind") or "") != "heavy_object_reclamation":
            return _fail("trusted expected effect must expose heavy_object_reclamation command kind")

    with temporary_repo_root(prefix="loop_system_heavy_object_lifecycle_parity_gap_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_lifecycle_command_family_parity_runtime"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-gap-lifecycle-parity.pack",
            b"heavy-object-gap-lifecycle-parity\n",
        )
        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_registration_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "object_id": object_id,
                "object_kind": "mathlib_pack",
                "object_ref": str(object_ref.resolve()),
                "byte_size": object_ref.stat().st_size,
                "registration_kind": "heavy_object_registration",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object registration command missing canonical registration fact",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap lifecycle scenario requires an accepted registration envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap lifecycle scenario requires a compatibility mirror event")
        gap_debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if str(gap_debug.get("trace_surface") or "") != "heavy_object_registration_trace":
            return _fail("gapped lifecycle parity must still use the matching lifecycle trace")
        if bool(gap_debug.get("parity_ok")):
            return _fail("gapped lifecycle parity must not report parity")
        mismatches = set(str(item) for item in list(gap_debug.get("mismatches") or []))
        if "trace_gaps_present" not in mismatches:
            return _fail("gapped lifecycle parity must surface trace_gaps_present")

    print("[loop-system-heavy-object-lifecycle-command-family-parity][OK] heavy-object lifecycle commands participate in the unified read-only parity debugger")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
