#!/usr/bin/env python3
"""Validate read-only heavy-object registration trace over audit, event, and observation history."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-registration-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _expected_canonical_store_ref(repo_root: Path, *, object_kind: str, object_id: str, is_dir: bool, suffix: str = "") -> Path:
    digest = str(object_id or "").removeprefix("sha256:").strip()
    leaf = "tree" if is_dir else f"object{suffix}"
    return (repo_root / ".loop" / "heavy_objects" / object_kind / digest / leaf).resolve()


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-registration-trace",
        root_goal="validate heavy-object registration trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object registration trace validation",
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


def _relevant_registration_trace_events(*, state_root: Path, envelope_id: str, object_id: str) -> list[dict]:
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
        if event_type == "heavy_object_registered" and str(payload.get("envelope_id") or "") == normalized_envelope_id:
            relevant.append(event_dict)
            continue
        if event_type == "heavy_object_observed" and str(payload.get("object_id") or "") == normalized_object_id:
            relevant.append(event_dict)
    return relevant


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_heavy_object_observation_event,
            mirror_accepted_control_envelope_event,
        )
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_heavy_object_registration_trace_view
        from loop_product.kernel.submit import submit_heavy_object_registration_request
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_registration_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_bytes = b"heavy-object-trace\n"
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"
        object_ref = _expected_canonical_store_ref(
            repo_root,
            object_kind="mathlib_pack",
            object_id=object_id,
            is_dir=False,
            suffix=".pack",
        )
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        observed_candidate_ref = repo_root / "workspace" / "trace-candidate" / "pack-a.pack"
        observed_candidate_ref.parent.mkdir(parents=True, exist_ok=True)
        observed_candidate_ref.write_bytes(object_bytes)

        accepted = submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="trace one accepted heavy-object registration command",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object registration command required for trace")

        observation_payload = {
            "object_id": object_id,
            "object_kind": "mathlib_pack",
            "object_ref": str(observed_candidate_ref.resolve()),
            "object_refs": [str(observed_candidate_ref.resolve())],
            "duplicate_count": 1,
            "total_bytes": len(object_bytes),
        }
        commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-registration-trace",
            node_id="root-kernel",
            payload=observation_payload,
            producer="tests.heavy_object",
        )

        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_registration_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        trace = query_heavy_object_registration_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_registration_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        if relevant_before != relevant_after:
            return _fail("heavy-object registration trace query must stay read-only and not mutate relevant committed facts")
        if not bool(trace.get("read_only")):
            return _fail("heavy-object registration trace must declare itself read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("trace must report the canonical heavy-object registration event")
        if not bool(trace.get("matching_observation_present")):
            return _fail("trace must report the latest matching heavy-object observation when one exists")
        registered_object = dict(trace.get("registered_object") or {})
        if str(registered_object.get("object_id") or "") != object_id:
            return _fail("trace must expose the registered object id")
        if str(registered_object.get("object_ref") or "") != str(object_ref.resolve()):
            return _fail("trace must expose the canonical content-addressed object ref")
        latest_matching_observation = dict(trace.get("latest_matching_observation") or {})
        if dict(latest_matching_observation.get("payload") or {}) != observation_payload:
            return _fail("trace must expose the latest matching heavy-object observation payload even when the observed candidate ref differs from the canonical registration ref")
        if list(trace.get("gaps") or []):
            return _fail("complete heavy-object registration trace must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_heavy_object_registration_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-gap.pack"
        object_bytes = b"heavy-object-gap\n"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

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
                "byte_size": len(object_bytes),
                "registration_kind": "heavy_object_registration",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object registration command missing canonical event",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted heavy-object registration envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")

        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_registration_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        gap_trace = query_heavy_object_registration_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_registration_trace_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
                object_id=object_id,
            )
        )
        if relevant_before != relevant_after:
            return _fail("gap trace query must stay read-only even when canonical facts are missing")
        if bool(gap_trace.get("canonical_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object registration event")
        if bool(gap_trace.get("matching_observation_present")):
            return _fail("gap trace must not invent a matching heavy-object observation")
        gaps = list(gap_trace.get("gaps") or [])
        if "missing_canonical_heavy_object_registered_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object registration event")

    print("[loop-system-heavy-object-registration-trace][OK] heavy-object registration trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
