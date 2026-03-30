#!/usr/bin/env python3
"""Validate canonical accepted activate-command events for Milestone 4."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-activate-command-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_nodes(temp_root: Path):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    source_workspace = temp_root / "workspace" / "source-child"
    child_workspace = temp_root / "workspace" / "activated-child"
    source_workspace.mkdir(parents=True, exist_ok=True)
    child_workspace.mkdir(parents=True, exist_ok=True)
    source_node = NodeSpec(
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
        workspace_root=str(source_workspace.resolve()),
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/child-implementer-001/result.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.COMPLETED,
    )
    planned_child = NodeSpec(
        node_id="child-followup-001",
        node_kind="implementer",
        goal_slice="prepare the follow-up artifact pack after the primary poster is finished",
        parent_node_id=source_node.node_id,
        generation=2,
        round_id="R1.child-followup-001",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        workspace_root=str(child_workspace.resolve()),
        depends_on_node_ids=[source_node.node_id],
        activation_condition=f"after:{source_node.node_id}:terminal",
        delegation_ref="state/delegations/child-followup-001.json",
        result_sink_ref="artifacts/child-followup-001/result.json",
        lineage_ref="root-kernel->child-implementer-001->child-followup-001",
        status=NodeStatus.PLANNED,
    )
    return source_node, planned_child


def _persist_base_state(state_root: Path, *, temp_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.runtime_paths import node_machine_handoff_ref

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-activate-command-event",
        root_goal="validate canonical accepted activate command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise activate command event validation",
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
    source_node, planned_child = _base_nodes(temp_root)
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    kernel_state.register_node(planned_child)
    authority = kernel_internal_authority()
    persist_kernel_state(state_root, kernel_state, authority=authority)
    persist_node_snapshot(state_root, source_node.to_dict(), authority=authority)
    persist_node_snapshot(state_root, planned_child.to_dict(), authority=authority)

    handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id=source_node.node_id)
    handoff_ref.parent.mkdir(parents=True, exist_ok=True)
    handoff_ref.write_text(
        json.dumps(
            {
                "node_id": source_node.node_id,
                "parent_node_id": source_node.parent_node_id,
                "round_id": source_node.round_id,
                "lineage_ref": source_node.lineage_ref,
                "workspace_root": source_node.workspace_root,
                "state_root": str(state_root.resolve()),
                "endpoint_artifact_ref": str((temp_root / "EndpointArtifact.json").resolve()),
                "root_goal": "activate the deferred child from frozen handoff context",
                "goal_slice": source_node.goal_slice,
                "context_refs": [str((temp_root / "Context.md").resolve())],
                "result_sink_ref": source_node.result_sink_ref,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.topology.activate import build_activate_request
        import loop_product.kernel.topology as topology_module
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_activate_command_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root, temp_root=repo_root)
        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[str] = []
        supervision_calls: list[str] = []
        terminate_calls: list[str] = []
        original_bootstrap = topology_module.bootstrap_first_implementer_node
        original_launch = topology_module.launch_child_from_result_ref
        original_supervision = topology_module.ensure_child_supervision_running
        original_terminate = topology_module.terminate_runtime_owned_launch_result_ref
        try:
            def _fake_bootstrap_first_implementer_node(**kwargs):
                bootstrap_calls.append(dict(kwargs))
                return {"bootstrap_result_ref": str((repo_root / "bootstrap-result.json").resolve())}

            def _fake_launch_child_from_result_ref(*, result_ref: str):
                launch_calls.append(str(result_ref))
                return {"launch_result_ref": str((repo_root / "launches" / "attempt_001" / "ChildLaunchResult.json").resolve())}

            def _fake_ensure_child_supervision_running(*, launch_result_ref: str):
                supervision_calls.append(str(launch_result_ref))
                return {"launch_result_ref": str(launch_result_ref), "pid": 12345}

            def _fake_terminate_runtime_owned_launch_result_ref(*, result_ref: str):
                terminate_calls.append(str(result_ref))

            topology_module.bootstrap_first_implementer_node = _fake_bootstrap_first_implementer_node
            topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
            topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
            topology_module.terminate_runtime_owned_launch_result_ref = _fake_terminate_runtime_owned_launch_result_ref

            accepted = submit_topology_mutation(
                state_root,
                build_activate_request(
                    "child-followup-001",
                    reason="the source node is now terminal and the deferred child may activate",
                ),
                round_id="R1.child-followup-001",
                generation=2,
            )
        finally:
            topology_module.bootstrap_first_implementer_node = original_bootstrap
            topology_module.launch_child_from_result_ref = original_launch
            topology_module.ensure_child_supervision_running = original_supervision
            topology_module.terminate_runtime_owned_launch_result_ref = original_terminate

        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"activate request must be accepted, got {accepted.status.value!r}")
        if len(bootstrap_calls) != 1 or len(launch_calls) != 1 or len(supervision_calls) != 1:
            return _fail("accepted activate must perform one bootstrap, one launch, and one supervision attach")
        if terminate_calls:
            return _fail("successful accepted activate must not terminate the fresh launch result")

        replayed = submit_control_envelope(state_root, accepted)
        if replayed.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted activate envelope must stay transport-accepted")
        if committed_event_count(state_root) != 2:
            return _fail("accepted activate under replay must produce exactly two committed events")

        canonical = [
            dict(event)
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "topology_command_accepted"
        ]
        if len(canonical) != 1:
            return _fail("accepted activate must emit exactly one canonical topology-command event")
        payload = dict(canonical[0].get("payload") or {})
        if str(payload.get("command_kind") or "") != "activate":
            return _fail("canonical activate event must expose command_kind=activate")
        if str(payload.get("source_node_id") or "") != "child-followup-001":
            return _fail("canonical activate event must expose the activated child as source_node_id")
        if list(payload.get("target_node_ids") or []):
            return _fail("canonical activate event must keep target_node_ids empty for source-only activation")

    print("[loop-system-event-journal-activate-command-event][OK] accepted activate emits one canonical topology-command event without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
