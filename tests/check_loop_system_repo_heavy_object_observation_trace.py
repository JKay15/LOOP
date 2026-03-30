#!/usr/bin/env python3
"""Validate read-only heavy-object observation trace over audit, event, and registration history."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-observation-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-observation-trace",
        root_goal="validate heavy-object observation trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object observation trace validation",
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


def _relevant_observation_trace_events(*, state_root: Path, envelope_id: str, object_id: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    relevant: list[dict] = []
    normalized_envelope_id = str(envelope_id or "").strip()
    normalized_object_id = str(object_id or "").strip()
    for event in iter_committed_events(state_root):
        event_dict = dict(event)
        payload = dict(event_dict.get("payload") or {})
        event_type = str(event_dict.get("event_type") or "")
        if event_type == "control_envelope_accepted" and str(payload.get("envelope_id") or "") == normalized_envelope_id:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_observed" and str(payload.get("envelope_id") or "") == normalized_envelope_id:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_registered" and str(payload.get("object_id") or "") == normalized_object_id:
            relevant.append(event_dict)
    return relevant


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_heavy_object_observation_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_observation_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_observation_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-a.pack"
        object_bytes = b"heavy-object-observation-trace\n"
        primary_ref.parent.mkdir(parents=True, exist_ok=True)
        primary_ref.write_bytes(object_bytes)
        duplicate_ref.parent.mkdir(parents=True, exist_ok=True)
        duplicate_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        registration = submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            byte_size=len(object_bytes),
            reason="register one heavy object before tracing the observation command",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if registration.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object registration command required for observation trace")

        accepted = submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            object_refs=[primary_ref, duplicate_ref],
            duplicate_count=2,
            total_bytes=len(object_bytes) * 2,
            observation_kind="duplicate_detection",
            reason="trace one accepted heavy-object observation command",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object observation command required for trace")

        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_observation_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        trace = query_heavy_object_observation_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_observation_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        if relevant_before != relevant_after:
            return _fail("heavy-object observation trace query must stay read-only and not mutate relevant committed facts")
        if not bool(trace.get("read_only")):
            return _fail("heavy-object observation trace must declare itself read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("trace must report the canonical heavy-object observation event")
        if not bool(trace.get("matching_registration_present")):
            return _fail("trace must expose the latest matching registration when one exists")
        observed_object = dict(trace.get("observed_object") or {})
        if str(observed_object.get("object_id") or "") != object_id:
            return _fail("trace must expose the observed object id")
        if str(observed_object.get("object_ref") or "") != str(primary_ref.resolve()):
            return _fail("trace must expose the normalized primary object ref")
        if int(observed_object.get("duplicate_count") or 0) != 2:
            return _fail("trace must expose duplicate_count for the observed object")
        latest_registration = dict(trace.get("latest_matching_registration") or {})
        if str(latest_registration.get("object_id") or "") != object_id:
            return _fail("trace must expose the matching registration summary")
        if list(trace.get("gaps") or []):
            return _fail("complete heavy-object observation trace must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_heavy_object_observation_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-gap.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-gap.pack"
        object_bytes = b"heavy-object-observation-gap\n"
        primary_ref.parent.mkdir(parents=True, exist_ok=True)
        primary_ref.write_bytes(object_bytes)
        duplicate_ref.parent.mkdir(parents=True, exist_ok=True)
        duplicate_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_observation_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "object_id": object_id,
                "object_kind": "mathlib_pack",
                "object_ref": str(primary_ref.resolve()),
                "object_refs": [str(primary_ref.resolve()), str(duplicate_ref.resolve())],
                "duplicate_count": 2,
                "total_bytes": len(object_bytes) * 2,
                "observation_kind": "duplicate_detection",
                "runtime_name": "publish-runtime",
                "repo_root": str(repo_root.resolve()),
                "observation_request_kind": "heavy_object_observation",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object observation command missing canonical event",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted heavy-object observation envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")

        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_observation_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        gap_trace = query_heavy_object_observation_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_observation_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        if relevant_before != relevant_after:
            return _fail("gap trace query must stay read-only even when canonical facts are missing")
        if bool(gap_trace.get("canonical_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object observation event")
        gaps = list(gap_trace.get("gaps") or [])
        if "missing_canonical_heavy_object_observed_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object observation event")

    print("[loop-system-heavy-object-observation-trace][OK] heavy-object observation trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
