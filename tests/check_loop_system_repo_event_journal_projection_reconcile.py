#!/usr/bin/env python3
"""Validate query-time projection replay from committed journal events."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-reconcile][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import latest_committed_seq, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection replay",
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
        node_id="reconcile-child-001",
        node_kind="implementer",
        goal_slice="exercise journal replay into kernel projection",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/reconcile-child-001.json",
        result_sink_ref="artifacts/reconcile-child-001/result.json",
        lineage_ref="root-kernel->reconcile-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_reconcile_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-reconcile",
            root_goal="validate crash-boundary replay from the journal",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="dispatch heartbeat committed to the journal before projection visibility",
                ).to_envelope()
            ),
        )
        mirrored = mirror_accepted_control_envelope_event(state_root, accepted)
        if not mirrored:
            return _fail("accepted dispatch envelope must mirror into the event journal")
        journal_seq = latest_committed_seq(state_root)
        if journal_seq <= 0:
            return _fail("mirrored dispatch envelope must advance the committed journal seq")

        stale_kernel = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        stale_child = json.loads((state_root / "state" / f"{child_node.node_id}.json").read_text(encoding="utf-8"))
        if int(stale_kernel.get("accepted_envelope_count") or 0) != 0:
            return _fail("test harness expects the kernel projection to still be stale before query-time replay")
        if str(dict(stale_child.get("runtime_state") or {}).get("attachment_state") or "") != "UNOBSERVED":
            return _fail("test harness expects the child snapshot to remain pre-replay before query-time reconciliation")

        authority_view = query_authority_view(state_root)
        if child_node.node_id not in list(authority_view.get("active_child_nodes") or []):
            return _fail("query-time journal replay must preserve truthful ACTIVE child visibility")

        healed_kernel = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        healed_child = json.loads((state_root / "state" / f"{child_node.node_id}.json").read_text(encoding="utf-8"))
        healed_runtime = dict(healed_child.get("runtime_state") or {})
        if int(healed_kernel.get("accepted_envelope_count") or 0) != 1:
            return _fail("query-time journal replay must reconstruct accepted-envelope count after projection loss")
        if str(healed_runtime.get("attachment_state") or "") != "ATTACHED":
            return _fail("query-time journal replay must reconstruct dispatch-backed ATTACHED runtime state")
        if "dispatch heartbeat committed to the journal before projection visibility" not in str(healed_runtime.get("summary") or ""):
            return _fail("query-time journal replay must preserve the accepted dispatch summary")
        if int(healed_kernel.get("last_applied_seq") or -1) != journal_seq:
            return _fail("kernel projection must advance its last_applied_seq to the replayed journal seq")
        if int(healed_child.get("last_applied_seq") or -1) != journal_seq:
            return _fail("node projection must advance its last_applied_seq to the replayed journal seq")

    print("[loop-system-event-journal-projection-reconcile][OK] query-time replay repairs stale kernel projections from committed journal events")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
