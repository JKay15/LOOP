#!/usr/bin/env python3
"""Validate read-only repo-tree cleanup trace over command, event, and receipt history."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-tree-cleanup-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-repo-tree-cleanup-trace",
        root_goal="validate repo-tree cleanup trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-tree cleanup trace validation",
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
        from loop_product.kernel.query import query_repo_tree_cleanup_trace_view
        from loop_product.kernel.submit import submit_repo_tree_cleanup_request
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.runtime import (
            ensure_repo_control_plane_services_running,
            live_housekeeping_reap_controller_runtime,
            live_repo_control_plane_runtime,
            live_repo_reactor_runtime,
        )
        from test_support import temporary_repo_tree_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_tree_root(prefix="loop_system_repo_tree_cleanup_trace_complete_") as tree_root:
        anchor_repo_root = tree_root / "anchor_repo"
        state_root = anchor_repo_root / ".loop"
        _persist_anchor_state(state_root)

        child_repo_roots = [
            tree_root / "forest" / "child_repo_a",
            tree_root / "forest" / "child_repo_b",
        ]
        for child_repo_root in child_repo_roots:
            payload = ensure_repo_control_plane_services_running(repo_root=child_repo_root)
            if not dict(payload.get("repo_control_plane") or {}):
                return _fail("setup must bootstrap child repo control-plane services before cleanup trace")

        accepted = submit_repo_tree_cleanup_request(
            state_root,
            tree_root=tree_root / "forest",
            repo_roots=child_repo_roots,
            reason="trace one accepted repo-tree cleanup command",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("accepted repo-tree cleanup command required for trace")
        event_count_before = committed_event_count(state_root)
        trace = query_repo_tree_cleanup_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("repo-tree cleanup trace query must stay read-only and not mutate committed event history")
        if not bool(trace.get("read_only")):
            return _fail("repo-tree cleanup trace must declare itself read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("trace must report the canonical repo-tree cleanup event")
        if not bool(trace.get("cleanup_receipt_present")):
            return _fail("trace must report the batch cleanup receipt")
        cleanup_effect = dict(trace.get("cleanup_effect") or {})
        repo_receipts = list(cleanup_effect.get("repo_receipts") or [])
        if len(repo_receipts) != 2:
            return _fail("trace must expose one receipt per cleaned child repo root")
        if any(not bool(dict(item).get("quiesced")) for item in repo_receipts):
            return _fail("trace must expose quiesced cleanup receipts for each child repo root")
        if any(dict(item).get("remaining_live_services") for item in repo_receipts):
            return _fail("trace must not report remaining live child-repo services after successful batch cleanup")
        for child_repo_root in child_repo_roots:
            if live_repo_reactor_runtime(repo_root=child_repo_root) is not None:
                return _fail("batch repo cleanup must retire child repo reactor runtime")
            if live_repo_control_plane_runtime(repo_root=child_repo_root) is not None:
                return _fail("batch repo cleanup must retire child repo control-plane runtime")
            if live_housekeeping_reap_controller_runtime(repo_root=child_repo_root) is not None:
                return _fail("batch repo cleanup must retire child housekeeping runtime")
        if list(trace.get("gaps") or []):
            return _fail("complete repo-tree cleanup trace must not report causal gaps")

    with temporary_repo_tree_root(prefix="loop_system_repo_tree_cleanup_trace_gap_") as tree_root:
        anchor_repo_root = tree_root / "anchor_repo"
        state_root = anchor_repo_root / ".loop"
        _persist_anchor_state(state_root)

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="repo_tree_cleanup_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "tree_root": str((tree_root / "forest").resolve()),
                "repo_roots": [str((tree_root / "forest" / "child_repo_a").resolve())],
                "cleanup_kind": "batch_repo_tree_cleanup",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted batch cleanup command missing canonical event and receipt",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap scenario requires an accepted repo-tree cleanup envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap scenario requires a compatibility mirror event")
        event_count_before = committed_event_count(state_root)
        gap_trace = query_repo_tree_cleanup_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("gap trace query must stay read-only even when canonical facts are missing")
        if bool(gap_trace.get("canonical_event_present")):
            return _fail("gap trace must not invent a canonical repo-tree cleanup event")
        if bool(gap_trace.get("cleanup_receipt_present")):
            return _fail("gap trace must not invent a cleanup receipt")
        gaps = list(gap_trace.get("gaps") or [])
        if "missing_canonical_repo_tree_cleanup_event" not in gaps:
            return _fail("gap trace must surface missing canonical repo-tree cleanup event")
        if "missing_repo_tree_cleanup_receipt" not in gaps:
            return _fail("gap trace must surface missing repo-tree cleanup receipt")

    print("[loop-system-repo-tree-cleanup-trace][OK] repo-tree cleanup trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
