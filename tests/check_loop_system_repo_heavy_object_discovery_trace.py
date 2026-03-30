#!/usr/bin/env python3
"""Validate read-only heavy-object discovery trace visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-discovery-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-discovery-trace",
        root_goal="validate heavy-object discovery trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object discovery trace validation",
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
        from loop_product.event_journal import committed_event_count, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_heavy_object_discovery_trace_view
        from loop_product.kernel.submit import submit_heavy_object_discovery_request
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_discovery_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-discovery.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-discovery.pack"
        object_bytes = b"heavy-object-discovery-trace\n"
        primary_ref.parent.mkdir(parents=True, exist_ok=True)
        primary_ref.write_bytes(object_bytes)
        duplicate_ref.parent.mkdir(parents=True, exist_ok=True)
        duplicate_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        accepted = submit_heavy_object_discovery_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="trace one accepted heavy-object discovery command",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object discovery command required for trace")

        event_count_before = committed_event_count(state_root)
        trace = query_heavy_object_discovery_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("heavy-object discovery trace query must stay read-only and not mutate committed event history")
        if not bool(trace.get("read_only")):
            return _fail("heavy-object discovery trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("heavy-object discovery trace must expose accepted audit presence")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("heavy-object discovery trace must expose compatibility mirror presence")
        if not bool(trace.get("canonical_request_event_present")):
            return _fail("heavy-object discovery trace must expose canonical discovery-request fact presence")
        if not bool(trace.get("canonical_observation_event_present")):
            return _fail("heavy-object discovery trace must expose resulting canonical observation fact presence")
        if not bool(trace.get("discovery_receipt_present")):
            return _fail("heavy-object discovery trace must expose deterministic discovery receipt presence")
        discovery_effect = dict(trace.get("discovery_effect") or {})
        if str(discovery_effect.get("object_id") or "") != object_id:
            return _fail("heavy-object discovery trace must expose the discovered object id")
        if str(discovery_effect.get("discovery_kind") or "") != "matching_filename_sha256_scan":
            return _fail("heavy-object discovery trace must expose discovery_kind")
        if [str(item) for item in list(discovery_effect.get("object_refs") or [])] != [
            str(primary_ref.resolve()),
            str(duplicate_ref.resolve()),
        ]:
            return _fail("heavy-object discovery trace must expose discovered refs")
        if int(discovery_effect.get("duplicate_count") or 0) != 2:
            return _fail("heavy-object discovery trace must expose duplicate_count")
        if list(trace.get("gaps") or []):
            return _fail("complete heavy-object discovery trace must not report gaps")

    with temporary_repo_root(prefix="loop_system_heavy_object_discovery_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-discovery-gap.pack"
        object_bytes = b"heavy-object-discovery-gap\n"
        primary_ref.parent.mkdir(parents=True, exist_ok=True)
        primary_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_discovery_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "object_id": object_id,
                "object_kind": "mathlib_pack",
                "object_ref": str(primary_ref.resolve()),
                "discovery_root": str(repo_root.resolve()),
                "discovery_kind": "matching_filename_sha256_scan",
                "runtime_name": "publish-runtime",
                "repo_root": str(repo_root.resolve()),
                "discovery_request_kind": "heavy_object_discovery",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object discovery command missing canonical request fact, receipt, and resulting observation",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted heavy-object discovery envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")

        event_count_before = committed_event_count(state_root)
        gap_trace = query_heavy_object_discovery_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("gap trace query must stay read-only even when canonical facts are missing")
        if bool(gap_trace.get("canonical_request_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object discovery request fact")
        if bool(gap_trace.get("canonical_observation_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object observation fact")
        if bool(gap_trace.get("discovery_receipt_present")):
            return _fail("gap trace must not invent a discovery receipt")
        gaps = set(str(item) for item in list(gap_trace.get("gaps") or []))
        if "missing_canonical_heavy_object_discovery_requested_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object discovery request fact")
        if "missing_heavy_object_discovery_receipt" not in gaps:
            return _fail("gap trace must surface missing heavy-object discovery receipt")
        if "missing_canonical_heavy_object_observed_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object observation fact")

    print("[loop-system-heavy-object-discovery-trace][OK] heavy-object discovery trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
