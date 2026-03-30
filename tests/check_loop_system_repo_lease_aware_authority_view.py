#!/usr/bin/env python3
"""Validate lease-aware runtime projection inside the authority query surface."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-lease-aware-authority-view][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.event_journal import commit_guarded_runtime_liveness_observation_event
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.runtime import cleanup_test_repo_services

    with tempfile.TemporaryDirectory(prefix="loop_system_lease_aware_authority_view_") as td:
        temp_root = Path(td).resolve()
        state_root = temp_root / ".loop"
        try:
            ensure_runtime_tree(state_root)
            root_node = NodeSpec(
                node_id="root-kernel",
                node_kind="kernel",
                goal_slice="supervise lease-aware authority view validation",
                parent_node_id=None,
                generation=0,
                round_id="R0",
                execution_policy={"mode": "kernel"},
                reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
                budget_profile={"max_rounds": 1},
                allowed_actions=["dispatch", "submit", "audit"],
                delegation_ref="",
                result_sink_ref="artifacts/root.json",
                lineage_ref="root-kernel",
                status=NodeStatus.ACTIVE,
            )
            fresh_child = NodeSpec(
                node_id="child-fresh-001",
                node_kind="implementer",
                goal_slice="fresh lease child",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement"],
                delegation_ref="state/delegations/child-fresh-001.json",
                result_sink_ref="artifacts/child-fresh-001/result.json",
                lineage_ref="root-kernel->child-fresh-001",
                status=NodeStatus.ACTIVE,
            )
            stale_child = NodeSpec(
                node_id="child-stale-001",
                node_kind="implementer",
                goal_slice="expired lease child",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement"],
                delegation_ref="state/delegations/child-stale-001.json",
                result_sink_ref="artifacts/child-stale-001/result.json",
                lineage_ref="root-kernel->child-stale-001",
                status=NodeStatus.ACTIVE,
            )
            kernel_state = KernelState(
                task_id="lease-aware-authority-view",
                root_goal="validate lease-aware runtime projection in authority query",
                root_node_id=root_node.node_id,
            )
            kernel_state.register_node(root_node)
            kernel_state.register_node(fresh_child)
            kernel_state.register_node(stale_child)
            persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

            commit_guarded_runtime_liveness_observation_event(
                state_root,
                observation_id="fresh-attached",
                node_id=fresh_child.node_id,
                payload={
                    "attachment_state": "ATTACHED",
                    "observed_at": "2026-03-24T12:00:00.000000Z",
                    "lease_duration_s": 60,
                    "lease_epoch": 3,
                    "lease_owner_id": "sidecar:fresh",
                },
                producer="tests.lease_aware_authority",
            )
            commit_guarded_runtime_liveness_observation_event(
                state_root,
                observation_id="stale-attached",
                node_id=stale_child.node_id,
                payload={
                    "attachment_state": "ATTACHED",
                    "observed_at": "2026-03-24T12:00:00.000000Z",
                    "lease_duration_s": 30,
                    "lease_epoch": 4,
                    "lease_owner_id": "sidecar:stale",
                },
                producer="tests.lease_aware_authority",
            )

            authority = query_authority_view(state_root, now_utc="2026-03-24T12:01:00.000000Z")
            lease_summary = dict(authority.get("lease_fencing_summary") or {})
            if fresh_child.node_id not in list(authority.get("truthful_active_child_nodes") or []):
                return _fail("fresh attached lease child must appear in truthful_active_child_nodes")
            if stale_child.node_id in list(authority.get("truthful_active_child_nodes") or []):
                return _fail("expired attached lease child must not appear in truthful_active_child_nodes")
            if stale_child.node_id not in list(authority.get("nonlive_active_child_nodes") or []):
                return _fail("expired attached ACTIVE child must appear in nonlive_active_child_nodes")
            if int(lease_summary.get("authoritative_attached_node_count") or 0) != 1:
                return _fail("authority view must count one authoritative attached lease-fencing row")
            if int(lease_summary.get("expired_attached_rejected_node_count") or 0) != 1:
                return _fail("authority view must count one expired attached rejection row")
            if int(lease_summary.get("projection_nonlive_active_child_node_count") or 0) != 1:
                return _fail("authority view must count one projection_nonlive_active child")

            node_graph = {str(item.get("node_id") or ""): dict(item) for item in list(authority.get("node_graph") or [])}
            fresh_projection = dict(node_graph.get(fresh_child.node_id, {}).get("runtime_projection") or {})
            stale_projection = dict(node_graph.get(stale_child.node_id, {}).get("runtime_projection") or {})
            if str(fresh_projection.get("effective_attachment_state") or "") != "ATTACHED":
                return _fail("fresh attached lease must stay effectively ATTACHED in runtime_projection")
            if str(stale_projection.get("raw_attachment_state") or "") != "ATTACHED":
                return _fail("expired lease must preserve raw ATTACHED history in runtime_projection")
            if str(stale_projection.get("effective_attachment_state") or "") != "UNOBSERVED":
                return _fail("expired lease must degrade to effective UNOBSERVED in runtime_projection")
            if str(stale_projection.get("lease_freshness") or "") != "expired":
                return _fail("expired lease must report lease_freshness=expired")
            if str(stale_projection.get("source_kind") or "") != "runtime_liveness_event":
                return _fail("lease-aware projection must say it came from the runtime liveness journal when event evidence exists")
        finally:
            cleanup_test_repo_services(repo_root=temp_root, settle_timeout_s=4.0, poll_interval_s=0.05)

    print("[loop-system-lease-aware-authority-view] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
