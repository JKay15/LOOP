#!/usr/bin/env python3
"""Validate canonical accepted heavy-object supersession events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-supersession-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-supersession-event",
        root_goal="validate canonical heavy-object supersession command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object supersession event validation",
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


def _relevant_supersession_events(
    *,
    state_root: Path,
    envelope_ids: set[str],
    object_ids: set[str],
    supersession_envelope_id: str,
) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    relevant: list[dict] = []
    normalized_envelope_ids = {str(item or "").strip() for item in envelope_ids if str(item or "").strip()}
    normalized_object_ids = {str(item or "").strip() for item in object_ids if str(item or "").strip()}
    normalized_supersession_envelope_id = str(supersession_envelope_id or "").strip()
    for event in iter_committed_events(state_root):
        event_dict = dict(event)
        payload = dict(event_dict.get("payload") or {})
        event_type = str(event_dict.get("event_type") or "")
        if event_type == "control_envelope_accepted" and str(payload.get("envelope_id") or "") in normalized_envelope_ids:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_registered" and str(payload.get("object_id") or "") in normalized_object_ids:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_superseded" and str(payload.get("envelope_id") or "") == normalized_supersession_envelope_id:
            relevant.append(event_dict)
    return relevant


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.submit import (
            submit_control_envelope,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_supersession_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old.pack",
            b"heavy-object-old-pack\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new.pack",
            b"heavy-object-new-pack\n",
        )

        for object_id, object_ref in ((old_id, old_ref), (new_id, new_ref)):
            accepted = submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason="register heavy objects before supersession lifecycle exists",
            )
            if accepted.status is not EnvelopeStatus.ACCEPTED:
                return _fail("registration precondition must be accepted")

        accepted = submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="publish_bundle_replacement",
            reason="replacement pack supersedes prior retained pack",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object supersession request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object supersession request must preserve heavy_object classification")

        relevant_envelope_ids = {
            str(accepted.envelope_id or ""),
        }
        relevant_object_ids = {old_id, new_id}
        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_supersession_events(
                state_root=state_root,
                envelope_ids=relevant_envelope_ids,
                object_ids=relevant_object_ids,
                supersession_envelope_id=str(accepted.envelope_id or ""),
            )
        )
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object supersession envelope must remain accepted")
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_supersession_events(
                state_root=state_root,
                envelope_ids=relevant_envelope_ids,
                object_ids=relevant_object_ids,
                supersession_envelope_id=str(accepted.envelope_id or ""),
            )
        )
        if relevant_before != relevant_after:
            return _fail("two registrations plus supersession replay must not append duplicate relevant committed facts")

        events = list(iter_committed_events(state_root))
        relevant_events = _relevant_supersession_events(
            state_root=state_root,
            envelope_ids=relevant_envelope_ids,
            object_ids=relevant_object_ids,
            supersession_envelope_id=str(accepted.envelope_id or ""),
        )
        event_types = [str(event.get("event_type") or "") for event in relevant_events]
        if event_types.count("control_envelope_accepted") != 1:
            return _fail("heavy-object supersession command must retain exactly one compatibility envelope mirror")
        if event_types.count("heavy_object_registered") != 2:
            return _fail("registration preconditions must stay singular per object under replay")
        if event_types.count("heavy_object_superseded") != 1:
            return _fail("heavy-object supersession command must emit exactly one canonical heavy_object_superseded fact")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_superseded"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_supersession:{accepted.envelope_id}":
            return _fail("canonical heavy-object supersession event must use heavy_object_supersession:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object supersession event must preserve the accepted envelope identity")
        if str(payload.get("superseded_object_id") or "") != old_id:
            return _fail("canonical heavy-object supersession event must preserve the superseded object id")
        if str(payload.get("replacement_object_id") or "") != new_id:
            return _fail("canonical heavy-object supersession event must preserve the replacement object id")
        if str(payload.get("superseded_object_ref") or "") != str(old_ref.resolve()):
            return _fail("canonical heavy-object supersession event must preserve the normalized superseded object ref")
        if str(payload.get("replacement_object_ref") or "") != str(new_ref.resolve()):
            return _fail("canonical heavy-object supersession event must preserve the normalized replacement object ref")
        if str(payload.get("supersession_kind") or "") != "publish_bundle_replacement":
            return _fail("canonical heavy-object supersession event must preserve the supersession kind")

    print("[loop-system-event-journal-heavy-object-supersession-event][OK] accepted heavy-object supersession commands emit one canonical event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
