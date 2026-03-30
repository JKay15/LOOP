#!/usr/bin/env python3
"""Validate canonical accepted heavy-object GC-eligibility events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-gc-eligibility-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-gc-eligibility-event",
        root_goal="validate canonical heavy-object gc-eligibility command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object gc-eligibility event validation",
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


def _relevant_gc_eligibility_event_ids(state_root: Path) -> tuple[tuple[str, str], ...]:
    from loop_product.event_journal import iter_committed_events

    relevant_types = {
        "heavy_object_registered",
        "heavy_object_superseded",
        "heavy_object_gc_eligible",
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
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_gc_eligibility_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-old.pack"
        old_bytes = b"heavy-object-gc-eligibility-old\n"
        old_ref.parent.mkdir(parents=True, exist_ok=True)
        old_ref.write_bytes(old_bytes)
        old_id = f"sha256:{hashlib.sha256(old_bytes).hexdigest()}"

        new_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-new.pack"
        new_bytes = b"heavy-object-gc-eligibility-new\n"
        new_ref.write_bytes(new_bytes)
        new_id = f"sha256:{hashlib.sha256(new_bytes).hexdigest()}"

        for object_id, object_ref, reason in (
            (old_id, old_ref, "register old heavy object before gc-eligibility lifecycle exists"),
            (new_id, new_ref, "register replacement heavy object before gc-eligibility lifecycle exists"),
        ):
            accepted = submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason=reason,
            )
            if accepted.status is not EnvelopeStatus.ACCEPTED:
                return _fail("heavy-object registration preconditions must be accepted")

        superseded = submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="replacement_pack",
            reason="replacement pack supersedes the old retained pack before gc eligibility",
        )
        if superseded.status is not EnvelopeStatus.ACCEPTED:
            return _fail("heavy-object supersession precondition must be accepted")

        accepted = submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old pack is superseded and may now enter authoritative gc-eligibility state",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object gc-eligibility request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object gc-eligibility request must preserve heavy_object classification")
        relevant_event_ids_before_replay = _relevant_gc_eligibility_event_ids(state_root)

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object gc-eligibility envelope must remain accepted")
        if _relevant_gc_eligibility_event_ids(state_root) != relevant_event_ids_before_replay:
            return _fail("replayed accepted heavy-object gc-eligibility envelope must not append duplicate canonical gc-eligibility lifecycle facts")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("heavy_object_registered") != 2:
            return _fail("heavy-object registration preconditions must stay singular under replay")
        if event_types.count("heavy_object_superseded") != 1:
            return _fail("heavy-object supersession precondition must stay singular under replay")
        if event_types.count("heavy_object_gc_eligible") != 1:
            return _fail("heavy-object gc-eligibility command must emit exactly one canonical heavy_object_gc_eligible fact")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_gc_eligible"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_gc_eligibility:{accepted.envelope_id}":
            return _fail("canonical heavy-object gc-eligibility event must use heavy_object_gc_eligibility:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object gc-eligibility event must preserve the accepted envelope identity")
        if str(payload.get("object_id") or "") != old_id:
            return _fail("canonical heavy-object gc-eligibility event must preserve the eligible object id")
        if str(payload.get("object_ref") or "") != str(old_ref.resolve()):
            return _fail("canonical heavy-object gc-eligibility event must preserve the normalized eligible object ref")
        if str(payload.get("eligibility_kind") or "") != "superseded_reclamation_candidate":
            return _fail("canonical heavy-object gc-eligibility event must preserve the eligibility kind")
        if str(payload.get("superseded_by_object_id") or "") != new_id:
            return _fail("canonical heavy-object gc-eligibility event must preserve the replacement object id")
        if str(payload.get("superseded_by_object_ref") or "") != str(new_ref.resolve()):
            return _fail("canonical heavy-object gc-eligibility event must preserve the normalized replacement object ref")

    print("[loop-system-event-journal-heavy-object-gc-eligibility-event][OK] accepted heavy-object gc-eligibility commands emit one canonical event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
