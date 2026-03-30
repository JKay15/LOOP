#!/usr/bin/env python3
"""Validate canonical accepted heavy-object observation events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-observation-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-observation-event",
        root_goal="validate canonical heavy-object observation command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object observation event validation",
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


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_heavy_object_observation_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_observation_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-a.pack"
        object_bytes = b"heavy-object-observation-foundation\n"
        primary_ref.parent.mkdir(parents=True, exist_ok=True)
        primary_ref.write_bytes(object_bytes)
        duplicate_ref.parent.mkdir(parents=True, exist_ok=True)
        duplicate_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        accepted = submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            object_refs=[primary_ref, duplicate_ref],
            duplicate_count=2,
            total_bytes=len(object_bytes) * 2,
            observation_kind="duplicate_detection",
            reason="record one accepted heavy-object observation command",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object observation request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object observation request must preserve heavy_object classification")

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object observation envelope must remain accepted")
        if committed_event_count(state_root) != 2:
            return _fail("accepted heavy-object observation command must produce exactly two committed events under replay")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("control_envelope_accepted") != 1:
            return _fail("heavy-object observation command must retain exactly one compatibility envelope mirror")
        if event_types.count("heavy_object_observed") != 1:
            return _fail("heavy-object observation command must emit exactly one canonical heavy_object_observed fact")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_observed"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_observation:{accepted.envelope_id}":
            return _fail("canonical heavy-object observation event must use heavy_object_observation:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object observation event must preserve the accepted envelope identity")
        if str(payload.get("object_id") or "") != object_id:
            return _fail("canonical heavy-object observation event must preserve the content-addressed object id")
        if str(payload.get("object_kind") or "") != "mathlib_pack":
            return _fail("canonical heavy-object observation event must preserve the object kind")
        if str(payload.get("object_ref") or "") != str(primary_ref.resolve()):
            return _fail("canonical heavy-object observation event must preserve the normalized primary object ref")
        if [str(item) for item in list(payload.get("object_refs") or [])] != [
            str(primary_ref.resolve()),
            str(duplicate_ref.resolve()),
        ]:
            return _fail("canonical heavy-object observation event must preserve normalized object refs")
        if int(payload.get("duplicate_count") or 0) != 2:
            return _fail("canonical heavy-object observation event must preserve duplicate_count")
        if int(payload.get("total_bytes") or 0) != len(object_bytes) * 2:
            return _fail("canonical heavy-object observation event must preserve total_bytes")
        if str(payload.get("observation_kind") or "") != "duplicate_detection":
            return _fail("canonical heavy-object observation event must preserve observation_kind")

    print("[loop-system-event-journal-heavy-object-observation-event][OK] accepted heavy-object observation commands emit one canonical event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
