#!/usr/bin/env python3
"""Validate explicit node-scoped projection rebuild when the kernel projection is current."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-node-projection-rebuild][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import iter_committed_events, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel import rebuild_node_projection
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view, query_projection_consistency_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate node projection rebuild",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_a = NodeSpec(
        node_id="node-projection-rebuild-child-a",
        node_kind="implementer",
        goal_slice="exercise explicit node projection rebuild",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/node-projection-rebuild-child-a.json",
        result_sink_ref="artifacts/node-projection-rebuild-child-a/result.json",
        lineage_ref="root-kernel->node-projection-rebuild-child-a",
        status=NodeStatus.ACTIVE,
    )
    child_b = NodeSpec(
        node_id="node-projection-rebuild-child-b",
        node_kind="implementer",
        goal_slice="exercise sibling stability during node projection rebuild",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/node-projection-rebuild-child-b.json",
        result_sink_ref="artifacts/node-projection-rebuild-child-b/result.json",
        lineage_ref="root-kernel->node-projection-rebuild-child-b",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_node_projection_rebuild_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-node-projection-rebuild",
            root_goal="validate node projection rebuild",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_a)
        kernel_state.register_node(child_b)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_a, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_b, authority=kernel_internal_authority())

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_a,
                    note="node-projection-rebuild dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before node rebuild tests")

        blocked = rebuild_node_projection(
            state_root,
            child_a.node_id,
            authority=kernel_internal_authority(),
        )
        if str(blocked.get("status") or "") != "blocked":
            return _fail("node-scoped rebuild must refuse to run while the kernel projection is stale")
        if str(blocked.get("blocking_reason") or "") != "kernel_projection_stale":
            return _fail("blocked node-scoped rebuild must explain that the kernel projection is stale")

        _ = query_authority_view(state_root)

        target_ref = state_root / "state" / f"{child_a.node_id}.json"
        sibling_ref = state_root / "state" / f"{child_b.node_id}.json"
        sibling_before = sibling_ref.read_text(encoding="utf-8")
        target_payload = json.loads(target_ref.read_text(encoding="utf-8"))
        target_payload["last_applied_seq"] = 0
        target_payload["projection_updated_at"] = "2026-03-24T00:00:00Z"
        os.chmod(target_ref, 0o644)
        target_ref.write_text(json.dumps(target_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        partial = query_projection_consistency_view(state_root)
        if str(partial.get("visibility_state") or "") != "partial":
            return _fail("manual target drift must produce partial projection visibility before node rebuild")

        rebuilt = rebuild_node_projection(
            state_root,
            child_a.node_id,
            authority=kernel_internal_authority(),
        )
        if str(rebuilt.get("status") or "") != "rebuilt":
            return _fail("node-scoped rebuild must report rebuilt when the target node projection lagged")
        if int(rebuilt.get("previous_node_last_applied_seq", -1)) != 0:
            return _fail("node-scoped rebuild must report the stale pre-rebuild node floor")
        if int(rebuilt.get("node_last_applied_seq") or 0) != 1:
            return _fail("node-scoped rebuild must advance the target node projection to the committed seq")
        if str(rebuilt.get("visibility_state") or "") != "atomically_visible":
            return _fail("node-scoped rebuild must restore atomically visible bundle state once the target catches up")
        if sibling_ref.read_text(encoding="utf-8") != sibling_before:
            return _fail("node-scoped rebuild must not rewrite an already-current sibling node projection")
        rebuild_events = [
            dict(event)
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "projection_rebuild_performed"
        ]
        if not rebuild_events:
            return _fail("node-scoped rebuild must leave a canonical projection_rebuild_performed fact")
        payload = dict(rebuild_events[-1].get("payload") or {})
        if str(payload.get("rebuild_scope") or "") != "node":
            return _fail("node-scoped rebuild fact must record rebuild_scope=node")
        if str(payload.get("target_node_id") or "") != child_a.node_id:
            return _fail("node-scoped rebuild fact must preserve the requested target node id")
        if str(payload.get("repair_state_before") or "") != "node_rebuild_required":
            return _fail("node-scoped rebuild fact must record node-scoped repair before rebuild")
        if str(payload.get("repair_scope_before") or "") != "node":
            return _fail("node-scoped rebuild fact must record repair_scope_before=node")
        if list(payload.get("repair_target_node_ids_before") or []) != [child_a.node_id]:
            return _fail("node-scoped rebuild fact must record the lagging target node before rebuild")
        if str(payload.get("repair_state_after") or "") != "none":
            return _fail("node-scoped rebuild fact must record no remaining repair need after rebuild")
        if str(payload.get("repair_scope_after") or "") != "none":
            return _fail("node-scoped rebuild fact must record repair_scope_after=none")
        if bool(payload.get("repair_cleared")) is not True:
            return _fail("node-scoped rebuild fact must record repair_cleared=True")

    print("[loop-system-event-journal-node-projection-rebuild][OK] node-scoped rebuild blocks on stale kernel state and repairs only the requested node projection")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
