#!/usr/bin/env python3
"""Validate read-only command parity debugger over heavy-object discovery and observation commands."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-command-family-parity][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-command-family-parity",
        root_goal="validate heavy-object command-family parity",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object command-family parity validation",
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
        from loop_product.kernel.query import query_command_parity_debugger_view
        from loop_product.kernel.submit import (
            submit_heavy_object_discovery_request,
            submit_heavy_object_observation_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_command_family_parity_discovery_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_command_family_parity_runtime"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-discovery-parity.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-discovery-parity.pack"
        object_bytes = b"heavy-object-discovery-parity\n"
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
            reason="exercise heavy-object discovery parity",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object discovery command required for parity")

        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("heavy-object discovery parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "heavy_object":
            return _fail("heavy-object discovery parity debugger must expose heavy_object classification")
        if str(debug.get("trace_surface") or "") != "heavy_object_discovery_trace":
            return _fail("heavy-object discovery parity debugger must route through discovery trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted heavy-object discovery command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted heavy-object discovery command must not report mismatches")
        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(trusted.get("command_kind") or "") != "heavy_object_discovery":
            return _fail("trusted expected effect must expose heavy_object_discovery command kind")
        if str(projected.get("object_id") or "") != object_id:
            return _fail("projected visible effect must expose discovered object id")
        if int(projected.get("duplicate_count") or 0) != 2:
            return _fail("projected visible effect must expose discovered duplicate count")

    with temporary_repo_root(prefix="loop_system_heavy_object_command_family_parity_discovery_gap_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_command_family_parity_runtime"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-discovery-gap-parity.pack"
        object_bytes = b"heavy-object-discovery-gap-parity\n"
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
            note="accepted heavy-object discovery command missing canonical request/receipt/observation",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap discovery scenario requires an accepted envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap discovery scenario requires a compatibility mirror event")

        before = committed_event_count(state_root)
        gap_debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("gapped heavy-object discovery parity debugger must stay read-only")
        if str(gap_debug.get("trace_surface") or "") != "heavy_object_discovery_trace":
            return _fail("gapped heavy-object discovery parity debugger must still use discovery trace")
        if bool(gap_debug.get("parity_ok")):
            return _fail("gapped heavy-object discovery command must not report parity")
        mismatches = set(str(item) for item in list(gap_debug.get("mismatches") or []))
        if "trace_gaps_present" not in mismatches:
            return _fail("gapped heavy-object discovery parity must surface trace_gaps_present")

    with temporary_repo_root(prefix="loop_system_heavy_object_command_family_parity_observation_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_command_family_parity_runtime"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-observation-parity.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-observation-parity.pack"
        object_bytes = b"heavy-object-observation-parity\n"
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
            reason="register heavy object before observation parity",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if registration.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object registration command required for observation parity")

        accepted = submit_heavy_object_observation_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            object_refs=[primary_ref, duplicate_ref],
            duplicate_count=2,
            total_bytes=len(object_bytes) * 2,
            observation_kind="duplicate_detection",
            reason="exercise heavy-object observation parity",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object observation command required for parity")

        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("heavy-object observation parity debugger must stay read-only")
        if str(debug.get("trace_surface") or "") != "heavy_object_observation_trace":
            return _fail("heavy-object observation parity debugger must route through observation trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted heavy-object observation command must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted heavy-object observation command must not report mismatches")
        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(trusted.get("command_kind") or "") != "heavy_object_observation":
            return _fail("trusted expected effect must expose heavy_object_observation command kind")
        if str(projected.get("object_id") or "") != object_id:
            return _fail("projected visible effect must expose observed object id")
        if int(projected.get("duplicate_count") or 0) != 2:
            return _fail("projected visible effect must expose observed duplicate count")

    with temporary_repo_root(prefix="loop_system_heavy_object_command_family_parity_observation_gap_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_command_family_parity_runtime"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-observation-gap-parity.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-observation-gap-parity.pack"
        object_bytes = b"heavy-object-observation-gap-parity\n"
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
            note="accepted heavy-object observation command missing canonical observation",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap observation scenario requires an accepted envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap observation scenario requires a compatibility mirror event")

        before = committed_event_count(state_root)
        gap_debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("gapped heavy-object observation parity debugger must stay read-only")
        if str(gap_debug.get("trace_surface") or "") != "heavy_object_observation_trace":
            return _fail("gapped heavy-object observation parity debugger must still use observation trace")
        if bool(gap_debug.get("parity_ok")):
            return _fail("gapped heavy-object observation command must not report parity")
        mismatches = set(str(item) for item in list(gap_debug.get("mismatches") or []))
        if "trace_gaps_present" not in mismatches:
            return _fail("gapped heavy-object observation parity must surface trace_gaps_present")

    print("[loop-system-heavy-object-command-family-parity][OK] heavy-object discovery and observation commands participate in the unified read-only parity debugger")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
