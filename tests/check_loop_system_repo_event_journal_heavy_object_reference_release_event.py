#!/usr/bin/env python3
"""Validate canonical accepted heavy-object reference-release events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-reference-release-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-reference-release-event",
        root_goal="validate canonical heavy-object reference-release command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object reference-release event validation",
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


def _relevant_reference_release_event_ids(state_root: Path) -> tuple[tuple[str, str], ...]:
    from loop_product.event_journal import iter_committed_events

    relevant_types = {
        "heavy_object_registered",
        "heavy_object_reference_attached",
        "heavy_object_reference_released",
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
            submit_heavy_object_reference_release_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_reference_release_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        object_bytes = b"heavy-object-reference-release-foundation\n"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        reference_holder_ref = repo_root / "workspace" / "reference-release-runtime" / "deliverables" / ".primary_artifact.publish.release"
        reference_ref = (
            reference_holder_ref
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / object_ref.name
        )
        reference_ref.parent.mkdir(parents=True, exist_ok=True)
        reference_ref.write_bytes(object_bytes)

        registered = submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="register one canonical heavy object before reference-release lifecycle exists",
        )
        if registered.status is not EnvelopeStatus.ACCEPTED:
            return _fail("heavy-object registration precondition must be accepted")

        attached = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_publish_reference",
            reason="attach one authoritative publish reference before release",
        )
        if attached.status is not EnvelopeStatus.ACCEPTED:
            return _fail("heavy-object reference precondition must be accepted")

        accepted = submit_heavy_object_reference_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_publish_reference",
            reason="release the publish reference after the holder stops pointing at the object",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object reference-release request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object reference-release request must preserve heavy_object classification")

        relevant_event_ids_before_replay = _relevant_reference_release_event_ids(state_root)
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object reference-release envelope must remain accepted")
        relevant_event_ids_after_replay = _relevant_reference_release_event_ids(state_root)
        if relevant_event_ids_after_replay != relevant_event_ids_before_replay:
            return _fail(
                "registration + reference + reference-release replay must not mutate the committed heavy-object "
                "registration/reference/reference-release fact set"
            )

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("heavy_object_registered") != 1:
            return _fail("heavy-object registration precondition must stay singular under replay")
        if event_types.count("heavy_object_reference_attached") != 1:
            return _fail("heavy-object reference precondition must stay singular under replay")
        if event_types.count("heavy_object_reference_released") != 1:
            return _fail(
                "heavy-object reference-release command must emit exactly one canonical heavy_object_reference_released fact"
            )

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_reference_released"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_reference_release:{accepted.envelope_id}":
            return _fail(
                "canonical heavy-object reference-release event must use heavy_object_reference_release:<envelope_id> as command_id"
            )
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object reference-release event must preserve the accepted envelope identity")
        if str(payload.get("object_id") or "") != object_id:
            return _fail("canonical heavy-object reference-release event must preserve the content-addressed object id")
        if str(payload.get("object_ref") or "") != str(object_ref.resolve()):
            return _fail("canonical heavy-object reference-release event must preserve the normalized object ref")
        if str(payload.get("reference_ref") or "") != str(reference_ref.resolve()):
            return _fail("canonical heavy-object reference-release event must preserve the normalized reference ref")
        if str(payload.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("canonical heavy-object reference-release event must preserve the holder kind")
        if str(payload.get("reference_holder_ref") or "") != str(reference_holder_ref.resolve()):
            return _fail("canonical heavy-object reference-release event must preserve the holder ref")
        if str(payload.get("reference_kind") or "") != "mathlib_pack_publish_reference":
            return _fail("canonical heavy-object reference-release event must preserve the reference kind")

    print(
        "[loop-system-event-journal-heavy-object-reference-release-event][OK] "
        "accepted heavy-object reference-release commands emit one canonical event without replay duplication"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
