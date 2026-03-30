#!/usr/bin/env python3
"""Validate read-only heavy-object reclamation trace visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-reclamation-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-reclamation-trace",
        root_goal="validate read-only heavy-object reclamation trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object reclamation trace validation",
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


def _reclamation_event_ids(state_root: Path) -> tuple[str, ...]:
    from loop_product.event_journal import iter_committed_events

    relevant_types = {
        "heavy_object_reclamation_requested",
        "heavy_object_reclaimed",
    }
    return tuple(
        str(event.get("event_id") or "")
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") in relevant_types
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_heavy_object_reclamation_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_reclamation_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old.pack",
            b"heavy-object-reclamation-trace-old-pack\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new.pack",
            b"heavy-object-reclamation-trace-new-pack\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register the old heavy object before reclamation trace exists",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register the replacement heavy object before reclamation trace exists",
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
            reason="replacement pack supersedes the old retained pack before reclamation trace exists",
            trigger_kind="heavy_object_reference_replaced",
            trigger_ref="manual-reference-event",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old pack becomes eligible before reclamation trace exists",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
            trigger_kind="heavy_object_auto_supersession",
            trigger_ref="manual-supersession-event",
        )
        accepted = submit_heavy_object_reclamation_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            reclamation_kind="authoritative_gc_reclaim",
            reason="trusted runtime reclaims the superseded heavy object",
            trigger_kind="heavy_object_auto_gc_eligibility",
            trigger_ref="manual-gc-eligibility-event",
        )

        event_ids_before = _reclamation_event_ids(state_root)
        trace = query_heavy_object_reclamation_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_ids_after = _reclamation_event_ids(state_root)
        if event_ids_before != event_ids_after:
            return _fail("heavy-object reclamation trace query must stay read-only and not append reclamation facts")
        if not bool(trace.get("read_only")):
            return _fail("heavy-object reclamation trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("heavy-object reclamation trace must expose accepted audit presence")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("heavy-object reclamation trace must expose compatibility mirror presence")
        if not bool(trace.get("canonical_request_event_present")):
            return _fail("heavy-object reclamation trace must expose canonical request fact presence")
        if not bool(trace.get("canonical_reclaimed_event_present")):
            return _fail("heavy-object reclamation trace must expose canonical reclaimed fact presence")
        if not bool(trace.get("reclamation_receipt_present")):
            return _fail("heavy-object reclamation trace must expose deterministic reclamation receipt presence")
        if not bool(trace.get("matching_registration_present")):
            return _fail("heavy-object reclamation trace must expose matching registration presence")
        if not bool(trace.get("supporting_gc_eligibility_present")):
            return _fail("heavy-object reclamation trace must expose supporting gc-eligibility presence")
        reclamation_effect = dict(trace.get("reclamation_effect") or {})
        if str(reclamation_effect.get("object_id") or "") != old_id:
            return _fail("heavy-object reclamation trace must expose the reclaimed object id")
        if str(reclamation_effect.get("reclamation_kind") or "") != "authoritative_gc_reclaim":
            return _fail("heavy-object reclamation trace must expose the reclamation kind")
        if not bool(reclamation_effect.get("reclaimed")):
            return _fail("heavy-object reclamation trace must expose successful reclamation")
        if bool(reclamation_effect.get("object_exists_after")):
            return _fail("heavy-object reclamation trace must expose that the reclaimed object no longer exists")
        if str(reclamation_effect.get("trigger_kind") or "") != "heavy_object_auto_gc_eligibility":
            return _fail("heavy-object reclamation trace must expose trigger_kind")
        if str(reclamation_effect.get("trigger_ref") or "") != "manual-gc-eligibility-event":
            return _fail("heavy-object reclamation trace must expose trigger_ref")
        registration = dict(trace.get("latest_matching_registration") or {})
        if str(registration.get("event_type") or "") != "heavy_object_registered":
            return _fail("heavy-object reclamation trace must surface the latest matching registration event summary")
        gc_eligibility = dict(trace.get("latest_supporting_gc_eligibility") or {})
        if str(gc_eligibility.get("event_type") or "") != "heavy_object_gc_eligible":
            return _fail("heavy-object reclamation trace must surface the latest supporting gc-eligibility event summary")
        if trace.get("gaps"):
            return _fail(f"complete heavy-object reclamation trace must not report gaps, got {trace['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_reclamation_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old.pack",
            b"heavy-object-reclamation-gap-old-pack\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new.pack",
            b"heavy-object-reclamation-gap-new-pack\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register old heavy object before reclamation trace gap scenario",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register replacement heavy object before reclamation trace gap scenario",
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
            reason="replacement pack supersedes the old retained pack before reclamation trace gap scenario",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old pack becomes eligible before reclamation trace gap scenario",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
        )

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_reclamation_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "object_id": old_id,
                "object_kind": "mathlib_pack",
                "object_ref": str(old_ref.resolve()),
                "reclamation_kind": "authoritative_gc_reclaim",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object reclamation command missing canonical request/reclaimed facts and receipt",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted heavy-object reclamation envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")

        event_ids_before = _reclamation_event_ids(state_root)
        gap_trace = query_heavy_object_reclamation_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_ids_after = _reclamation_event_ids(state_root)
        if event_ids_before != event_ids_after:
            return _fail("gap trace query must stay read-only even when canonical reclamation facts are missing")
        if bool(gap_trace.get("canonical_request_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object reclamation request fact")
        if bool(gap_trace.get("canonical_reclaimed_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object reclaimed fact")
        if bool(gap_trace.get("reclamation_receipt_present")):
            return _fail("gap trace must not invent a reclamation receipt")
        if not bool(gap_trace.get("matching_registration_present")):
            return _fail("gap trace should still expose the supporting registration anchor")
        if not bool(gap_trace.get("supporting_gc_eligibility_present")):
            return _fail("gap trace should still expose the supporting gc-eligibility anchor")
        gaps = set(str(item) for item in list(gap_trace.get("gaps") or []))
        if "missing_canonical_heavy_object_reclamation_requested_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object reclamation request fact")
        if "missing_canonical_heavy_object_reclaimed_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object reclaimed fact")
        if "missing_heavy_object_reclamation_receipt" not in gaps:
            return _fail("gap trace must surface missing heavy-object reclamation receipt")

    print("[loop-system-heavy-object-reclamation-trace][OK] heavy-object reclamation trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
