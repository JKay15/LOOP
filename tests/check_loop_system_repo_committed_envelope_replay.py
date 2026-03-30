#!/usr/bin/env python3
"""Validate accepted-envelope replay against committed journal truth when projection lags."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-committed-envelope-replay][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, load_kernel_state, persist_kernel_state
        from loop_product.kernel.submit import submit_control_envelope, submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate committed accepted-envelope replay guard",
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
        node_id="projection-lag-replay-child",
        node_kind="implementer",
        goal_slice="exercise committed accepted-envelope replay under projection lag",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["resume_request"],
        delegation_ref="state/delegations/projection-lag-replay-child.json",
        result_sink_ref="artifacts/projection-lag-replay-child/result.json",
        lineage_ref="root-kernel->projection-lag-replay-child",
        status=NodeStatus.BLOCKED,
    )

    with temporary_repo_root(prefix="loop_system_committed_envelope_replay_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="committed-envelope-replay",
            root_goal="validate committed accepted-envelope replay under projection lag",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        kernel_state.blocked_reasons[child_node.node_id] = "projection lag replay fixture"
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        accepted = submit_topology_mutation(
            state_root,
            TopologyMutation.resume(
                child_node.node_id,
                reason="projection lag replay coverage",
                payload={"consistency_signal": "projection_lag_replay"},
            ),
            round_id=child_node.round_id,
            generation=child_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("initial topology mutation must be accepted")
        first_envelope_id = str(accepted.envelope_id or "")
        first_accepted_at = str(accepted.accepted_at or "")
        if committed_event_count(state_root) != 2:
            return _fail("initial accepted topology mutation must emit exactly two committed events")

        stale_state = load_kernel_state(state_root)
        stale_state.accepted_envelopes = []
        persist_kernel_state(state_root, stale_state, authority=kernel_internal_authority())

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("committed-journal replay must return the existing accepted envelope")
        if str(replay.envelope_id or "") != first_envelope_id:
            return _fail("committed-journal replay must preserve the accepted envelope identity")
        if str(replay.accepted_at or "") != first_accepted_at:
            return _fail("committed-journal replay must preserve the originally committed accepted_at timestamp")
        if committed_event_count(state_root) != 2:
            return _fail("replaying against committed accepted-envelope truth must not append duplicate committed events")

    print("[loop-system-committed-envelope-replay][OK] committed accepted-envelope truth prevents replay conflicts when projection lags")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
