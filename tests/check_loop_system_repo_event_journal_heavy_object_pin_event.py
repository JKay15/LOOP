#!/usr/bin/env python3
"""Validate canonical accepted heavy-object pin events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-pin-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-pin-event",
        root_goal="validate canonical heavy-object pin command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object pin event validation",
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


def _relevant_pin_events(*, state_root: Path, envelope_ids: set[str], object_id: str, pin_envelope_id: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    relevant: list[dict] = []
    normalized_envelope_ids = {str(item or "").strip() for item in envelope_ids if str(item or "").strip()}
    normalized_object_id = str(object_id or "").strip()
    normalized_pin_envelope_id = str(pin_envelope_id or "").strip()
    for event in iter_committed_events(state_root):
        event_dict = dict(event)
        payload = dict(event_dict.get("payload") or {})
        event_type = str(event_dict.get("event_type") or "")
        if event_type == "control_envelope_accepted" and str(payload.get("envelope_id") or "") in normalized_envelope_ids:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_registered" and str(payload.get("object_id") or "") == normalized_object_id:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_pinned" and str(payload.get("envelope_id") or "") == normalized_pin_envelope_id:
            relevant.append(event_dict)
    return relevant


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.submit import (
            submit_control_envelope,
            submit_heavy_object_pin_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_pin_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        object_bytes = b"heavy-object-pin-foundation\n"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        registered = submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="register one canonical heavy object before pin lifecycle exists",
        )
        if registered.status is not EnvelopeStatus.ACCEPTED:
            return _fail("heavy-object registration precondition must be accepted")

        accepted = submit_heavy_object_pin_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="retain the registered heavy object while publish staging still references it",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object pin request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object pin request must preserve heavy_object classification")

        relevant_envelope_ids = {
            str(registered.envelope_id or ""),
            str(accepted.envelope_id or ""),
        }
        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_pin_events(
                state_root=state_root,
                envelope_ids=relevant_envelope_ids,
                object_id=object_id,
                pin_envelope_id=str(accepted.envelope_id or ""),
            )
        )
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object pin envelope must remain accepted")
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_pin_events(
                state_root=state_root,
                envelope_ids=relevant_envelope_ids,
                object_id=object_id,
                pin_envelope_id=str(accepted.envelope_id or ""),
            )
        )
        if relevant_before != relevant_after:
            return _fail("registration + pin replay must not append duplicate relevant committed facts")

        events = list(iter_committed_events(state_root))
        relevant_events = _relevant_pin_events(
            state_root=state_root,
            envelope_ids=relevant_envelope_ids,
            object_id=object_id,
            pin_envelope_id=str(accepted.envelope_id or ""),
        )
        event_types = [str(event.get("event_type") or "") for event in relevant_events]
        if event_types.count("control_envelope_accepted") != 2:
            return _fail("registration + pin must retain exactly two compatibility envelope mirrors")
        if event_types.count("heavy_object_registered") != 1:
            return _fail("heavy-object registration precondition must stay singular under replay")
        if event_types.count("heavy_object_pinned") != 1:
            return _fail("heavy-object pin command must emit exactly one canonical heavy_object_pinned fact")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_pinned"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_pin:{accepted.envelope_id}":
            return _fail("canonical heavy-object pin event must use heavy_object_pin:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object pin event must preserve the accepted envelope identity")
        if str(payload.get("object_id") or "") != object_id:
            return _fail("canonical heavy-object pin event must preserve the content-addressed object id")
        if str(payload.get("object_ref") or "") != str(object_ref.resolve()):
            return _fail("canonical heavy-object pin event must preserve the normalized object ref")
        if str(payload.get("pin_holder_id") or "") != "publish:artifact-bundle":
            return _fail("canonical heavy-object pin event must preserve the pin holder id")
        if str(payload.get("pin_kind") or "") != "publish_retention_pin":
            return _fail("canonical heavy-object pin event must preserve the pin kind")

    print("[loop-system-event-journal-heavy-object-pin-event][OK] accepted heavy-object pin commands emit one canonical event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
