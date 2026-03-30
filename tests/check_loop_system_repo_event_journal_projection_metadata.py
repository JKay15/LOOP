#!/usr/bin/env python3
"""Validate Milestone 1 projection metadata and query-time self-heal."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-metadata][FAIL] {msg}", file=sys.stderr)
    return 2


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _seq(payload: dict[str, object], key: str) -> int:
    value = payload.get(key)
    return -1 if value is None else int(value)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import latest_committed_seq
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection metadata",
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
    child_node = NodeSpec(
        node_id="projection-child-001",
        node_kind="implementer",
        goal_slice="exercise projection seq self-heal",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-child-001.json",
        result_sink_ref="artifacts/projection-child-001/result.json",
        lineage_ref="root-kernel->projection-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_metadata_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-metadata",
            root_goal="validate projection metadata and self-heal",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        initial_kernel = _read_json(state_root / "state" / "kernel_state.json")
        initial_child = _read_json(state_root / "state" / f"{child_node.node_id}.json")
        if _seq(initial_kernel, "last_applied_seq") != 0:
            return _fail("fresh kernel projection must start at last_applied_seq=0 before any journal event exists")
        if _seq(initial_child, "last_applied_seq") != 0:
            return _fail("fresh node projection must start at last_applied_seq=0 before any journal event exists")

        accepted = submit_control_envelope(
            state_root,
            build_child_dispatch_status(child_node, note="dispatch heartbeat for projection metadata").to_envelope(),
        )
        if str(accepted.status.value) != "accepted":
            return _fail("dispatch envelope must be accepted for projection metadata coverage")

        latest_seq = latest_committed_seq(state_root)
        if latest_seq <= 0:
            return _fail("accepted envelope mirroring must advance the event journal seq")

        refreshed_kernel = _read_json(state_root / "state" / "kernel_state.json")
        refreshed_child = _read_json(state_root / "state" / f"{child_node.node_id}.json")
        if str(refreshed_kernel.get("projection_name") or "") != "kernel_state":
            return _fail("kernel_state.json must record projection_name")
        if _seq(refreshed_kernel, "last_applied_seq") != latest_seq:
            return _fail("kernel_state.json must record the current journal seq when persisted")
        if str(refreshed_child.get("projection_name") or "") != "node_snapshot":
            return _fail("per-node snapshots must record projection_name")
        if _seq(refreshed_child, "last_applied_seq") != latest_seq:
            return _fail("per-node snapshots must record the current journal seq when persisted")

        stale_kernel = dict(refreshed_kernel)
        stale_kernel["last_applied_seq"] = 0
        stale_kernel["projection_updated_at"] = "2026-03-24T00:00:00Z"
        kernel_state_ref = state_root / "state" / "kernel_state.json"
        os.chmod(kernel_state_ref, 0o644)
        kernel_state_ref.write_text(
            json.dumps(stale_kernel, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        stale_child = dict(refreshed_child)
        stale_child["last_applied_seq"] = 0
        stale_child["projection_updated_at"] = "2026-03-24T00:00:00Z"
        child_snapshot_ref = state_root / "state" / f"{child_node.node_id}.json"
        os.chmod(child_snapshot_ref, 0o644)
        child_snapshot_ref.write_text(
            json.dumps(stale_child, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        authority_view = query_authority_view(state_root)
        if child_node.node_id not in list(authority_view.get("active_child_nodes") or []):
            return _fail("query_authority_view must still expose the active child while healing stale projection metadata")

        healed_kernel = _read_json(state_root / "state" / "kernel_state.json")
        healed_child = _read_json(state_root / "state" / f"{child_node.node_id}.json")
        if _seq(healed_kernel, "last_applied_seq") != latest_seq:
            return _fail("query_authority_view must self-heal stale kernel projection seq from the journal floor")
        if _seq(healed_child, "last_applied_seq") != latest_seq:
            return _fail("query_authority_view must self-heal stale node projection seq from the journal floor")

    print("[loop-system-event-journal-projection-metadata][OK] projection metadata records and self-heals journal seq visibility")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
