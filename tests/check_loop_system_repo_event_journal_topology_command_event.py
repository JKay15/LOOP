#!/usr/bin/env python3
"""Validate canonical accepted-topology command events for Milestone 4."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-topology-command-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_source_node():
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="finish the primary birthday poster flow",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "split_request"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/child-implementer-001/result.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-topology-command-event",
        root_goal="validate canonical accepted-topology command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise topology command event validation",
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
    kernel_state.register_node(_base_source_node())
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.topology.split_review import build_split_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_topology_command_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-followup-001",
                    "goal_slice": "prepare the follow-up artifact pack after the primary poster is finished",
                    "depends_on_node_ids": [source_node.node_id],
                    "activation_condition": f"after:{source_node.node_id}:terminal",
                }
            ],
            split_mode="deferred",
            completed_work="the primary poster scope remains with the current implementer",
            remaining_work="the follow-up artifact pack should wait until the source node reaches terminal state",
            reason="persist a deferred follow-up branch without activating it yet",
        )
        accepted = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"deferred split must be accepted, got {accepted.status.value!r}")

        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted topology envelope must remain accepted")
        if committed_event_count(state_root) != 2:
            return _fail("accepted topology command must produce exactly two committed events under replay")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("control_envelope_accepted") != 1:
            return _fail("accepted topology command must retain exactly one compatibility envelope mirror")
        if event_types.count("topology_command_accepted") != 1:
            return _fail("accepted topology command must emit exactly one canonical topology command event")

        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "topology_command_accepted"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"topology_command:{accepted.envelope_id}":
            return _fail("canonical topology command event must use topology_command:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical topology command event must preserve the accepted envelope identity")
        if str(payload.get("command_kind") or "") != "split":
            return _fail("canonical topology command event must expose the topology command kind")
        if str(payload.get("source_node_id") or "") != source_node.node_id:
            return _fail("canonical topology command event must expose the source node identity")
        if list(payload.get("target_node_ids") or []) != ["child-followup-001"]:
            return _fail("canonical topology command event must preserve the normalized target node ids")

    print("[loop-system-event-journal-topology-command-event][OK] accepted topology commands emit one canonical event without duplicating replay side effects")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
