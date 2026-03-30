#!/usr/bin/env python3
"""Validate read-only repo-root heavy-object remediation batch trace visibility."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-repo-remediation-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-repo-remediation-trace",
        root_goal="validate repo-root heavy-object remediation batch trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-root heavy-object remediation trace validation",
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


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> Path:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import iter_committed_events, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_heavy_object_authority_gap_repo_remediation_trace_view
        from loop_product.kernel.submit import submit_heavy_object_authority_gap_repo_remediation_request
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_repo_remediation_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/repo-remediation-trace-primary.pack",
            b"heavy-object-authority-gap-repo-remediation-trace\n",
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/repo-remediation-trace-primary.pack",
            b"heavy-object-authority-gap-repo-remediation-trace\n",
        )

        accepted = submit_heavy_object_authority_gap_repo_remediation_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="trace one accepted repo-root heavy-object remediation batch command",
            runtime_name="publish-runtime",
        )
        events_before = list(iter_committed_events(state_root))
        matching_requested_before = sum(
            1
            for event in events_before
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        matching_settled_before = sum(
            1
            for event in events_before
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        trace = query_heavy_object_authority_gap_repo_remediation_trace_view(
            state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        events_after = list(iter_committed_events(state_root))
        matching_requested_after = sum(
            1
            for event in events_after
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        matching_settled_after = sum(
            1
            for event in events_after
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        if (
            matching_requested_after != matching_requested_before
            or matching_settled_after != matching_settled_before
        ):
            return _fail("repo remediation trace query must stay read-only and not mutate matching committed repo-remediation facts")
        if not bool(trace.get("read_only")):
            return _fail("repo remediation trace must declare itself read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("trace must report the canonical repo remediation event")
        if not bool(trace.get("canonical_result_event_present")):
            return _fail("trace must report the canonical repo remediation result fact")
        if not bool(trace.get("remediation_receipt_present")):
            return _fail("trace must report the runtime remediation receipt")
        remediation_effect = dict(trace.get("remediation_effect") or {})
        if int(remediation_effect.get("candidate_count") or 0) != 2:
            return _fail("trace must expose both unmanaged candidates from the batch scope")
        if int(remediation_effect.get("requested_candidate_count") or 0) != 1:
            return _fail("trace must expose one requested candidate in the duplicate-pair setup")
        if int(remediation_effect.get("skipped_candidate_count") or 0) != 1:
            return _fail("trace must expose one skipped candidate in the duplicate-pair setup")
        final_inventory = dict(trace.get("final_inventory") or {})
        if int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0) != 0:
            return _fail("trace must expose fully covered final remediation inventory for the duplicate-pair setup")
        if int(final_inventory.get("remediated_candidate_count") or 0) != 1:
            return _fail("trace must expose one directly remediated candidate in the final inventory")
        if list(trace.get("gaps") or []):
            return _fail(f"complete repo remediation trace must not report gaps, got {trace['gaps']!r}")

        candidate_summaries = list(final_inventory.get("candidates") or [])
        refs = {str(item.get("object_ref") or "") for item in candidate_summaries}
        if refs != {str(primary_ref.resolve()), str(duplicate_ref.resolve())}:
            return _fail("trace must expose both candidate refs in the final inventory")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_repo_remediation_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_authority_gap_repo_remediation_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "repo_root": str(repo_root.resolve()),
                "object_kind": "mathlib_pack",
                "runtime_name": "publish-runtime",
                "remediation_kind": "repo_root_authority_gap_remediation",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted repo remediation command missing canonical event and receipt",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted repo remediation envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")
        events_before = list(iter_committed_events(state_root))
        matching_requested_before = sum(
            1
            for event in events_before
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        matching_settled_before = sum(
            1
            for event in events_before
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        gap_trace = query_heavy_object_authority_gap_repo_remediation_trace_view(
            state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        events_after = list(iter_committed_events(state_root))
        matching_requested_after = sum(
            1
            for event in events_after
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        matching_settled_after = sum(
            1
            for event in events_after
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        if (
            matching_requested_after != matching_requested_before
            or matching_settled_after != matching_settled_before
        ):
            return _fail("gap trace query must stay read-only even when canonical repo-remediation facts are missing")
        if bool(gap_trace.get("canonical_event_present")):
            return _fail("gap trace must not invent a canonical repo remediation event")
        if bool(gap_trace.get("canonical_result_event_present")):
            return _fail("gap trace must not invent a canonical repo remediation result fact")
        if bool(gap_trace.get("remediation_receipt_present")):
            return _fail("gap trace must not invent a remediation receipt")
        gaps = list(gap_trace.get("gaps") or [])
        if "missing_canonical_heavy_object_authority_gap_repo_remediation_event" not in gaps:
            return _fail("gap trace must surface missing canonical repo remediation event")
        if "missing_canonical_heavy_object_authority_gap_repo_remediation_settled_event" not in gaps:
            return _fail("gap trace must surface missing canonical repo remediation result fact")
        if "missing_heavy_object_authority_gap_repo_remediation_receipt" not in gaps:
            return _fail("gap trace must surface missing repo remediation receipt")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_repo_remediation_trace_auto_") as repo_root:
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view
        from loop_product.runtime import ensure_repo_control_plane_services_running

        state_root = repo_root / ".loop"
        control_plane_root = state_root / "repo_control_plane"
        _make_pack(
            repo_root,
            "workspace/publish/repo-remediation-trace-auto-primary.pack",
            b"heavy-object-authority-gap-repo-remediation-trace-auto\n",
        )
        _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/repo-remediation-trace-auto-primary.pack",
            b"heavy-object-authority-gap-repo-remediation-trace-auto\n",
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        if int(dict(services.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
            return _fail("auto-remediation trace scenario requires a live repo control-plane service")

        deadline = time.time() + 8.0
        requested_events: list[dict] = []
        while time.time() < deadline:
            requested_events = [
                dict(event)
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_requested"
            ]
            settled_events = [
                dict(event)
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_settled"
            ]
            required_events = [
                dict(event)
                for event in iter_committed_events(control_plane_root)
                if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_required"
            ]
            if requested_events and settled_events and required_events:
                break
            time.sleep(0.05)
        if not requested_events:
            return _fail("auto-remediation trace scenario requires an accepted repo remediation command emitted by the repo-global control-plane")
        auto_requested = requested_events[-1]
        auto_trace = query_heavy_object_authority_gap_repo_remediation_trace_view(
            state_root,
            envelope_id=str(dict(auto_requested.get("payload") or {}).get("envelope_id") or ""),
        )
        if not bool(auto_trace.get("required_event_present")):
            return _fail("auto-generated repo remediation trace must expose the linked canonical requirement fact")
        required_event = dict(auto_trace.get("required_event") or {})
        if str(required_event.get("event_type") or "") != "heavy_object_authority_gap_repo_remediation_required":
            return _fail("auto-generated repo remediation trace must point at heavy_object_authority_gap_repo_remediation_required")
        final_inventory = dict(auto_trace.get("final_inventory") or {})
        if int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0) != 0:
            return _fail("auto-generated repo remediation trace must expose zero unmanaged candidates after remediation")
        post_inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(post_inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("auto-generated repo remediation trace scenario must converge the duplicate-pair setup to zero unmanaged candidates")
        if list(auto_trace.get("gaps") or []):
            return _fail(f"complete auto-generated repo remediation trace must not report gaps, got {auto_trace.get('gaps')!r}")

    print("[loop-system-heavy-object-authority-gap-repo-remediation-trace][OK] repo-root heavy-object remediation trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
