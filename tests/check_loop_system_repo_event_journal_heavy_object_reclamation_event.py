#!/usr/bin/env python3
"""Validate canonical accepted heavy-object reclamation events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-reclamation-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-reclamation-event",
        root_goal="validate canonical heavy-object reclamation command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object reclamation event validation",
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


def _relevant_reclamation_event_ids(state_root: Path) -> tuple[tuple[str, str], ...]:
    from loop_product.event_journal import iter_committed_events

    relevant_types = {
        "heavy_object_registered",
        "heavy_object_superseded",
        "heavy_object_gc_eligible",
        "heavy_object_reclamation_requested",
        "heavy_object_reclaimed",
    }
    return tuple(
        (str(event.get("event_type") or ""), str(event.get("event_id") or ""))
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") in relevant_types
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.submit import (
            submit_control_envelope,
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import heavy_object_reclamation_receipt_ref
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_reclamation_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old.pack",
            b"heavy-object-reclamation-old-pack\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new.pack",
            b"heavy-object-reclamation-new-pack\n",
        )

        for object_id, object_ref in ((old_id, old_ref), (new_id, new_ref)):
            accepted = submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason="register heavy objects before reclamation lifecycle exists",
            )
            if accepted.status is not EnvelopeStatus.ACCEPTED:
                return _fail("registration preconditions must be accepted")

        accepted = submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="publish_bundle_replacement",
            reason="replacement pack supersedes prior retained pack before authoritative reclamation",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("supersession precondition must be accepted")

        accepted = submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old pack becomes eligible for authoritative reclamation",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gc-eligibility precondition must be accepted")

        accepted = submit_heavy_object_reclamation_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            reclamation_kind="authoritative_gc_reclaim",
            reason="old superseded pack should be reclaimed by the trusted heavy-object surface",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object reclamation request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object reclamation request must preserve heavy_object classification")
        if old_ref.exists():
            return _fail("trusted heavy-object reclamation execution must retire the reclaimed object bytes")
        relevant_event_ids_before_replay = _relevant_reclamation_event_ids(state_root)

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object reclamation envelope must remain accepted")
        if _relevant_reclamation_event_ids(state_root) != relevant_event_ids_before_replay:
            return _fail("replayed accepted heavy-object reclamation envelope must not append duplicate canonical reclamation lifecycle facts")

        receipt_ref = heavy_object_reclamation_receipt_ref(state_root=state_root, envelope_id=str(accepted.envelope_id or ""))
        if not receipt_ref.exists():
            return _fail("heavy-object reclamation must persist a deterministic reclamation receipt")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("heavy_object_registered") != 2:
            return _fail("registration preconditions must stay singular under replay")
        if event_types.count("heavy_object_superseded") != 1:
            return _fail("supersession precondition must stay singular under replay")
        if event_types.count("heavy_object_gc_eligible") != 1:
            return _fail("gc-eligibility precondition must stay singular under replay")
        if event_types.count("heavy_object_reclamation_requested") != 1:
            return _fail("heavy-object reclamation command must emit exactly one canonical heavy_object_reclamation_requested fact")
        if event_types.count("heavy_object_reclaimed") != 1:
            return _fail("heavy-object reclamation execution must emit exactly one canonical heavy_object_reclaimed fact")

        request_event = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_reclamation_requested"),
            {},
        )
        request_payload = dict(request_event.get("payload") or {})
        if str(request_event.get("command_id") or "") != f"heavy_object_reclamation:{accepted.envelope_id}":
            return _fail("canonical heavy-object reclamation request event must use heavy_object_reclamation:<envelope_id> as command_id")
        if str(request_payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object reclamation request event must preserve the accepted envelope identity")
        if str(request_payload.get("object_id") or "") != old_id:
            return _fail("canonical heavy-object reclamation request event must preserve the object id")
        if str(request_payload.get("object_ref") or "") != str(old_ref.resolve()):
            return _fail("canonical heavy-object reclamation request event must preserve the normalized object ref")
        if str(request_payload.get("reclamation_kind") or "") != "authoritative_gc_reclaim":
            return _fail("canonical heavy-object reclamation request event must preserve the reclamation kind")

        reclaimed_event = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_reclaimed"),
            {},
        )
        reclaimed_payload = dict(reclaimed_event.get("payload") or {})
        if str(reclaimed_event.get("command_id") or "") != f"heavy_object_reclamation_result:{accepted.envelope_id}":
            return _fail("canonical heavy-object reclaimed event must use heavy_object_reclamation_result:<envelope_id> as command_id")
        if str(reclaimed_payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object reclaimed event must preserve the accepted envelope identity")
        if str(reclaimed_payload.get("object_id") or "") != old_id:
            return _fail("canonical heavy-object reclaimed event must preserve the object id")
        if str(reclaimed_payload.get("object_ref") or "") != str(old_ref.resolve()):
            return _fail("canonical heavy-object reclaimed event must preserve the normalized object ref")
        if not bool(reclaimed_payload.get("reclaimed")):
            return _fail("canonical heavy-object reclaimed event must record that the object was reclaimed")
        if bool(reclaimed_payload.get("object_exists_after")):
            return _fail("canonical heavy-object reclaimed event must record that the object no longer exists")
        if str(reclaimed_payload.get("receipt_ref") or "") != str(receipt_ref):
            return _fail("canonical heavy-object reclaimed event must point at the deterministic reclamation receipt")

    print("[loop-system-event-journal-heavy-object-reclamation-event][OK] accepted heavy-object reclamation commands emit canonical request/reclaimed facts without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
