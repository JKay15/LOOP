#!/usr/bin/env python3
"""Validate canonical accepted repo-tree cleanup command events for Milestone 4."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-repo-tree-cleanup-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-repo-tree-cleanup-event",
        root_goal="validate canonical repo-tree cleanup command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-tree cleanup command validation",
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
        from loop_product.kernel.submit import submit_control_envelope, submit_repo_tree_cleanup_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import ensure_repo_control_plane_services_running
        from test_support import temporary_repo_tree_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_tree_root(prefix="loop_system_repo_tree_cleanup_event_") as tree_root:
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
                return _fail("setup must bootstrap child repo control-plane services before batch cleanup")

        accepted = submit_repo_tree_cleanup_request(
            state_root,
            tree_root=tree_root / "forest",
            repo_roots=child_repo_roots,
            reason="retire one materialized repo forest through one accepted batch cleanup command",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"batch repo cleanup request must be accepted, got {accepted.status.value!r}")

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted repo-tree cleanup envelope must remain accepted")
        if committed_event_count(state_root) != 2:
            return _fail("accepted repo-tree cleanup command must produce exactly two committed events under replay")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("control_envelope_accepted") != 1:
            return _fail("repo-tree cleanup command must retain exactly one compatibility envelope mirror")
        if event_types.count("repo_tree_cleanup_requested") != 1:
            return _fail("repo-tree cleanup command must emit exactly one canonical repo-tree cleanup event")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "repo_tree_cleanup_requested"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"repo_tree_cleanup:{accepted.envelope_id}":
            return _fail("canonical repo-tree cleanup event must use repo_tree_cleanup:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical repo-tree cleanup event must preserve the accepted envelope identity")
        if str(payload.get("tree_root") or "") != str((tree_root / "forest").resolve()):
            return _fail("canonical repo-tree cleanup event must preserve the requested tree root")
        if list(payload.get("repo_roots") or []) != [str(path.resolve()) for path in child_repo_roots]:
            return _fail("canonical repo-tree cleanup event must preserve the normalized child repo roots")
        if bool(payload.get("include_anchor_repo_root")):
            return _fail("canonical repo-tree cleanup event must keep anchor repo cleanup opt-in off by default")

    print("[loop-system-event-journal-repo-tree-cleanup-event][OK] accepted repo-tree cleanup commands emit one canonical event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
