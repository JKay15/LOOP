#!/usr/bin/env python3
"""Validate read-only command parity debugger coverage for repo-tree cleanup commands."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-tree-cleanup-parity-debugger][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-repo-tree-cleanup-parity-debugger",
        root_goal="validate repo-tree cleanup parity debugger",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-tree cleanup parity debugger validation",
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
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_command_parity_debugger_view
        from loop_product.kernel.submit import submit_repo_tree_cleanup_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import ensure_repo_control_plane_services_running, repo_tree_cleanup_receipt_ref
        from test_support import temporary_repo_tree_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_tree_root(prefix="loop_system_repo_tree_cleanup_parity_complete_") as tree_root:
        anchor_repo_root = tree_root / "anchor-repo"
        state_root = anchor_repo_root / ".loop"
        _persist_anchor_state(state_root)
        child_repo_roots = [
            tree_root / "forest" / "batch-00",
            tree_root / "forest" / "batch-01",
        ]
        for child_repo_root in child_repo_roots:
            payload = ensure_repo_control_plane_services_running(repo_root=child_repo_root)
            if not dict(payload.get("repo_control_plane") or {}):
                return _fail("setup must bootstrap child repo control-plane services before parity cleanup")

        accepted = submit_repo_tree_cleanup_request(
            state_root,
            tree_root=tree_root / "forest",
            repo_roots=child_repo_roots,
            reason="retire sibling repo roots through one accepted batch cleanup command",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("repo-tree cleanup parity scenario requires an accepted cleanup command")

        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("repo-tree cleanup parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "repo_tree_cleanup":
            return _fail("repo-tree cleanup parity debugger must expose repo_tree_cleanup classification")
        if str(debug.get("trace_surface") or "") != "repo_tree_cleanup_trace":
            return _fail("repo-tree cleanup parity debugger must route through repo-tree cleanup trace")
        if not bool(debug.get("parity_ok")):
            return _fail("repo-tree cleanup parity debugger must report parity for accepted cleanup commands")
        if list(debug.get("mismatches") or []):
            return _fail("repo-tree cleanup parity debugger must not report mismatches for accepted cleanup commands")

        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        expected_repo_roots = sorted(str(path.resolve()) for path in child_repo_roots)
        if sorted(str(item) for item in list(trusted.get("repo_roots") or [])) != expected_repo_roots:
            return _fail("trusted expected effect must preserve the requested child repo-root set")
        if sorted(str(item) for item in list(projected.get("repo_roots") or [])) != expected_repo_roots:
            return _fail("projected visible effect must preserve the settled child repo-root set")
        if int(projected.get("repo_count") or 0) != len(expected_repo_roots):
            return _fail("projected visible effect must expose the settled repo-count")
        if int(projected.get("quiesced_repo_count") or 0) != len(expected_repo_roots):
            return _fail("projected visible effect must expose quiesced repo-count for accepted cleanup")
        if not bool(projected.get("quiesced")):
            return _fail("projected visible effect must report quiesced cleanup for accepted repo-tree cleanup")

    with temporary_repo_tree_root(prefix="loop_system_repo_tree_cleanup_parity_gap_") as tree_root:
        anchor_repo_root = tree_root / "anchor-gap-repo"
        child_repo_root = tree_root / "batch-gap-00"
        state_root = anchor_repo_root / ".loop"
        _persist_anchor_state(state_root)
        payload = ensure_repo_control_plane_services_running(repo_root=child_repo_root)
        if not dict(payload.get("repo_control_plane") or {}):
            return _fail("gap setup must bootstrap child repo control-plane services before cleanup")
        accepted = submit_repo_tree_cleanup_request(
            state_root,
            tree_root=tree_root,
            repo_roots=[child_repo_root],
            reason="exercise repo-tree cleanup parity gap handling",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("repo-tree cleanup gap scenario requires an accepted cleanup command")
        receipt_ref = repo_tree_cleanup_receipt_ref(state_root=state_root, envelope_id=str(accepted.envelope_id or ""))
        if not receipt_ref.exists():
            return _fail("repo-tree cleanup gap scenario requires an initial cleanup receipt")
        receipt_ref.unlink()

        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("repo-tree cleanup parity debugger must stay read-only when the cleanup receipt is missing")
        if str(debug.get("classification") or "") != "repo_tree_cleanup":
            return _fail("gapped repo-tree cleanup parity debugger must preserve repo_tree_cleanup classification")
        if str(debug.get("trace_surface") or "") != "repo_tree_cleanup_trace":
            return _fail("gapped repo-tree cleanup parity debugger must still route through repo-tree cleanup trace")
        if bool(debug.get("parity_ok")):
            return _fail("missing repo-tree cleanup receipt must fail parity")
        mismatches = [str(item) for item in list(debug.get("mismatches") or [])]
        if "trace_gaps_present" not in mismatches:
            return _fail("missing cleanup receipt must surface parity mismatch via trace_gaps_present")

    print("[loop-system-repo-tree-cleanup-parity-debugger][OK] repo-tree cleanup parity debugger stays read-only and reports both complete and gapped cleanup histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
