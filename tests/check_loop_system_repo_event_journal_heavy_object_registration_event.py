#!/usr/bin/env python3
"""Validate canonical accepted heavy-object registration events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-registration-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-registration-event",
        root_goal="validate canonical heavy-object registration command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object registration event validation",
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


def _relevant_registration_events(*, state_root: Path, envelope_id: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    relevant: list[dict] = []
    for event in iter_committed_events(state_root):
        event_dict = dict(event)
        payload = dict(event_dict.get("payload") or {})
        if str(payload.get("envelope_id") or "") != envelope_id:
            continue
        if str(event_dict.get("event_type") or "") not in {
            "control_envelope_accepted",
            "heavy_object_registered",
        }:
            continue
        relevant.append(event_dict)
    return relevant


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_heavy_object_registration_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_registration_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        object_bytes = b"heavy-object-registration-foundation\n"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        accepted = submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="register one canonical heavy object before pin/gc lifecycle exists",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object registration request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object registration request must preserve heavy_object classification")

        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_registration_events(state_root=state_root, envelope_id=str(accepted.envelope_id or ""))
        )
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object registration envelope must remain accepted")
        relevant_events = _relevant_registration_events(state_root=state_root, envelope_id=str(accepted.envelope_id or ""))
        relevant_after = tuple(str(event.get("event_id") or "") for event in relevant_events)
        if relevant_after != relevant_before:
            return _fail("accepted heavy-object registration replay must not append duplicate relevant committed facts")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in relevant_events]
        if event_types.count("control_envelope_accepted") != 1:
            return _fail("heavy-object registration command must retain exactly one compatibility envelope mirror")
        if event_types.count("heavy_object_registered") != 1:
            return _fail("heavy-object registration command must emit exactly one canonical heavy_object_registered fact")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_registered"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_registration:{accepted.envelope_id}":
            return _fail("canonical heavy-object registration event must use heavy_object_registration:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object registration event must preserve the accepted envelope identity")
        if str(payload.get("object_id") or "") != object_id:
            return _fail("canonical heavy-object registration event must preserve the content-addressed object id")
        if str(payload.get("object_kind") or "") != "mathlib_pack":
            return _fail("canonical heavy-object registration event must preserve the object kind")
        if str(payload.get("object_ref") or "") != str(object_ref.resolve()):
            return _fail("canonical heavy-object registration event must preserve the normalized object ref")
        if int(payload.get("byte_size") or 0) != len(object_bytes):
            return _fail("canonical heavy-object registration event must preserve byte size")

    print("[loop-system-event-journal-heavy-object-registration-event][OK] accepted heavy-object registration commands emit one canonical event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
