#!/usr/bin/env python3
"""Validate read-only heavy-object authority-gap audit trace visibility."""

from __future__ import annotations

import hashlib
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-audit-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-audit-trace",
        root_goal="validate heavy-object authority-gap audit trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap audit trace validation",
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


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> tuple[str, Path]:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    return f"sha256:{digest}", path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.query import query_heavy_object_authority_gap_audit_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_authority_gap_audit_request,
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.runtime import ensure_repo_control_plane_services_running
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_audit_trace_complete_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_audit_trace_runtime"
        _persist_anchor_state(state_root)

        managed_id, managed_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/audit-trace-managed.pack",
            b"heavy-object-authority-gap-audit-trace-managed\n",
        )
        _unused_id, unmanaged_ref = _make_pack(
            repo_root,
            "workspace/publish/audit-trace-unmanaged.pack",
            b"heavy-object-authority-gap-audit-trace-unmanaged\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            byte_size=managed_ref.stat().st_size,
            reason="register managed audit-trace candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover managed audit-trace candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        accepted = submit_heavy_object_authority_gap_audit_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="trace one accepted heavy-object authority-gap audit command",
            runtime_name="publish-runtime",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted heavy-object authority-gap audit command required for trace")

        event_count_before = committed_event_count(state_root)
        trace = query_heavy_object_authority_gap_audit_trace_view(
            state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("heavy-object authority-gap audit trace query must stay read-only and not mutate committed event history")
        if not bool(trace.get("read_only")):
            return _fail("heavy-object authority-gap audit trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("heavy-object authority-gap audit trace must expose accepted audit presence")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("heavy-object authority-gap audit trace must expose compatibility mirror presence")
        if not bool(trace.get("canonical_event_present")):
            return _fail("heavy-object authority-gap audit trace must expose canonical audit fact presence")
        if not bool(trace.get("canonical_result_event_present")):
            return _fail("heavy-object authority-gap audit trace must expose canonical audit result fact presence")
        if not bool(trace.get("audit_receipt_present")):
            return _fail("heavy-object authority-gap audit trace must expose deterministic audit receipt presence")
        audit_effect = dict(trace.get("audit_effect") or {})
        if int(audit_effect.get("candidate_count") or 0) != 2:
            return _fail("heavy-object authority-gap audit trace must expose all filesystem candidates in scope")
        if int(audit_effect.get("managed_candidate_count") or 0) != 1:
            return _fail("heavy-object authority-gap audit trace must expose one managed candidate")
        if int(audit_effect.get("unmanaged_candidate_count") or 0) != 1:
            return _fail("heavy-object authority-gap audit trace must expose one unmanaged candidate")
        final_inventory = dict(trace.get("final_inventory") or {})
        if int(final_inventory.get("filesystem_candidate_count") or 0) != 2:
            return _fail("heavy-object authority-gap audit trace must expose the final inventory")
        if int(final_inventory.get("unmanaged_candidate_count") or 0) != 1:
            return _fail("heavy-object authority-gap audit trace must preserve the unmanaged candidate in the final inventory")
        if list(trace.get("gaps") or []):
            return _fail(f"complete heavy-object authority-gap audit trace must not report causal gaps, got {trace['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_audit_trace_gap_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_audit_trace_runtime"
        _persist_anchor_state(state_root)

        _unused_id, unmanaged_ref = _make_pack(
            repo_root,
            "workspace/publish/audit-trace-gap-unmanaged.pack",
            b"heavy-object-authority-gap-audit-trace-gap\n",
        )

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_authority_gap_audit_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "repo_root": str(repo_root.resolve()),
                "object_kind": "mathlib_pack",
                "runtime_name": "publish-runtime",
                "audit_kind": "repo_root_authority_gap_audit",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object authority-gap audit command missing canonical event and receipt",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted heavy-object authority-gap audit envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")

        event_count_before = committed_event_count(state_root)
        gap_trace = query_heavy_object_authority_gap_audit_trace_view(
            state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("gap trace query must stay read-only even when canonical facts are missing")
        if bool(gap_trace.get("canonical_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object authority-gap audit fact")
        if bool(gap_trace.get("canonical_result_event_present")):
            return _fail("gap trace must not invent a canonical heavy-object authority-gap audit result fact")
        if bool(gap_trace.get("audit_receipt_present")):
            return _fail("gap trace must not invent a heavy-object authority-gap audit receipt")
        gaps = set(str(item) for item in list(gap_trace.get("gaps") or []))
        if "missing_canonical_heavy_object_authority_gap_audit_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object authority-gap audit fact")
        if "missing_canonical_heavy_object_authority_gap_audited_event" not in gaps:
            return _fail("gap trace must surface missing canonical heavy-object authority-gap audit result fact")
        if "missing_heavy_object_authority_gap_audit_receipt" not in gaps:
            return _fail("gap trace must surface missing heavy-object authority-gap audit receipt")
        final_inventory = dict(gap_trace.get("final_inventory") or {})
        if int(final_inventory.get("unmanaged_candidate_count") or 0) != 1:
            return _fail("gap trace must still expose the underlying unmanaged candidate")
        if str(unmanaged_ref.resolve()) not in {str(item.get("object_ref") or "") for item in list(final_inventory.get("unmanaged_candidates") or [])}:
            return _fail("gap trace must preserve the unmanaged candidate ref in the final inventory")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_audit_trace_auto_") as repo_root:
        state_root = repo_root / ".loop"

        _make_pack(
            repo_root,
            "workspace/publish/audit-trace-auto-unmanaged-a.pack",
            b"heavy-object-authority-gap-audit-trace-auto-a\n",
        )
        _make_pack(
            repo_root,
            "workspace/publish/audit-trace-auto-unmanaged-b.pack",
            b"heavy-object-authority-gap-audit-trace-auto-b\n",
        )

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        if int(dict(services.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
            return _fail("auto-audit trace scenario requires a live repo control-plane service")

        deadline = time.time() + 8.0
        requested = {}
        while time.time() < deadline:
            requested = next(
                (
                    dict(event)
                    for event in iter_committed_events(state_root)
                    if str(event.get("event_type") or "") == "heavy_object_authority_gap_audit_requested"
                ),
                {},
            )
            if requested:
                break
            time.sleep(0.05)
        if not requested:
            return _fail("auto-audit trace scenario requires a repo-global auto-submitted audit command")

        envelope_id = str(dict(requested.get("payload") or {}).get("envelope_id") or "")
        auto_trace = query_heavy_object_authority_gap_audit_trace_view(
            state_root,
            envelope_id=envelope_id,
        )
        if not bool(auto_trace.get("required_event_present")):
            return _fail("auto-audit trace must expose the linked canonical requirement event")
        required_event = dict(auto_trace.get("required_event") or {})
        if str(required_event.get("event_type") or "") != "heavy_object_authority_gap_audit_required":
            return _fail("auto-audit trace must expose heavy_object_authority_gap_audit_required as the linked requirement fact")
        accepted_summary = dict(auto_trace.get("accepted_envelope") or {})
        if str(accepted_summary.get("trigger_kind") or "") != "repo_control_plane_auto_audit":
            return _fail("auto-audit trace must preserve the auto-audit trigger_kind on the accepted envelope")
        if str(accepted_summary.get("requirement_event_id") or "") != str(required_event.get("event_id") or ""):
            return _fail("auto-audit trace must preserve requirement_event_id linkage on the accepted envelope")
        if "missing_canonical_heavy_object_authority_gap_audit_required_event" in set(
            str(item) for item in list(auto_trace.get("gaps") or [])
        ):
            return _fail("complete auto-audit trace must not report a missing canonical requirement fact")

    print("[loop-system-heavy-object-authority-gap-audit-trace][OK] heavy-object authority-gap audit trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
