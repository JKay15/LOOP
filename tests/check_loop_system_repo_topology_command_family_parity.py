#!/usr/bin/env python3
"""Validate read-only topology-family parity coverage over split/retry/activate/relaunch."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-topology-command-family-parity][FAIL] {msg}", file=sys.stderr)
    return 2


def _split_source_node():
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


def _persist_split_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-topology-family-parity-split",
        root_goal="validate split parity coverage",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise topology family parity validation",
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
    kernel_state.register_node(_split_source_node())
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _build_split_request():
    from loop_product.topology.split_review import build_split_request

    source_node = _split_source_node()
    return source_node, build_split_request(
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


def _make_recovery_source_node(*, node_id: str, status: str):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id=node_id,
        node_kind="implementer",
        goal_slice="recover the current bounded implementation lane",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "resume_request", "retry_request", "relaunch_request"],
        delegation_ref=f"state/delegations/{node_id}.json",
        result_sink_ref=f"artifacts/{node_id}/result.json",
        lineage_ref=f"root-kernel->{node_id}",
        status=NodeStatus(status),
    )


def _persist_recovery_base_state(state_root: Path, *, node_id: str, status: str, blocked_reason: str = "") -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-topology-family-parity-recovery",
        root_goal="validate recovery-family parity coverage",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise topology family parity validation",
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
    source_node = _make_recovery_source_node(node_id=node_id, status=status)
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    if blocked_reason:
        kernel_state.blocked_reasons[node_id] = blocked_reason
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _activate_base_nodes(temp_root: Path):
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


def _persist_activate_base_state(state_root: Path, *, temp_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.runtime_paths import node_machine_handoff_ref

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-topology-family-parity-activate",
        root_goal="validate activate parity coverage",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise topology family parity validation",
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
    source_node, planned_child = _activate_base_nodes(temp_root)
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
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_authority_view, query_command_parity_debugger_view
        from loop_product.kernel.submit import submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime.recover import build_relaunch_request, build_retry_request
        from loop_product.topology.activate import build_activate_request
        import loop_product.kernel.topology as topology_module
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_topology_command_family_parity_split_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_split_base_state(state_root)
        source_node, mutation = _build_split_request()
        accepted = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("split parity scenario requires an accepted split request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("split topology parity debugger must stay read-only")
        if str(debug.get("command_kind") or "") != "split":
            return _fail("split topology parity debugger must expose command_kind=split")
        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        if dict(trusted.get("expected_target_statuses") or {}) != {"child-followup-001": "PLANNED"}:
            return _fail("split topology parity debugger must expose expected deferred target statuses")
        if dict(projected.get("target_statuses") or {}).get("child-followup-001") != "PLANNED":
            return _fail("split topology parity debugger must expose the projected deferred child status")
        if str(projected.get("source_status") or "") != "ACTIVE":
            return _fail("split topology parity debugger must expose the active source status")
        if not bool(debug.get("parity_ok")) or list(debug.get("mismatches") or []):
            return _fail("accepted split topology parity debugger must not report mismatches")

    with temporary_repo_root(prefix="loop_system_topology_command_family_parity_retry_") as repo_root:
        state_root = repo_root / ".loop"
        source_node = _make_recovery_source_node(node_id="child-retry-001", status="FAILED")
        _persist_recovery_base_state(state_root, node_id=source_node.node_id, status="FAILED")
        accepted = submit_topology_mutation(
            state_root,
            build_retry_request(
                source_node,
                reason="the evaluator found a self-caused path issue and can retry safely",
                self_attribution="evaluator_path_error",
                self_repair="fix the staged command path before retrying the same node",
            ),
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("retry parity scenario requires an accepted retry request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("retry topology parity debugger must stay read-only")
        if str(debug.get("command_kind") or "") != "retry":
            return _fail("retry topology parity debugger must expose command_kind=retry")
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(projected.get("source_status") or "") != "ACTIVE":
            return _fail("retry topology parity debugger must expose the recovered source node as ACTIVE")
        if str(projected.get("source_blocked_reason") or ""):
            return _fail("retry topology parity debugger must expose cleared blocked_reason")
        if str(projected.get("source_runtime_attachment_state") or "") != "UNOBSERVED":
            return _fail("retry topology parity debugger must expose the reset runtime attachment state")
        if not bool(debug.get("parity_ok")) or list(debug.get("mismatches") or []):
            return _fail("accepted retry topology parity debugger must not report mismatches")

    with temporary_repo_root(prefix="loop_system_topology_command_family_parity_activate_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_activate_base_state(state_root, temp_root=repo_root)
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
            return _fail("activate parity scenario requires an accepted activate request")
        if len(bootstrap_calls) != 1 or len(launch_calls) != 1 or len(supervision_calls) != 1 or terminate_calls:
            return _fail("activate parity scenario requires the same trusted activation side effects as the canonical path")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("activate topology parity debugger must stay read-only")
        if str(debug.get("command_kind") or "") != "activate":
            return _fail("activate topology parity debugger must expose command_kind=activate")
        projected = dict(debug.get("projected_visible_effect") or {})
        if list(projected.get("target_node_ids") or []):
            return _fail("activate topology parity debugger must not invent target ids for source-only activation")
        if str(projected.get("source_status") or "") != "ACTIVE":
            return _fail("activate topology parity debugger must expose the activated child as ACTIVE")
        if not bool(debug.get("parity_ok")) or list(debug.get("mismatches") or []):
            return _fail("accepted activate topology parity debugger must not report mismatches")

    with temporary_repo_root(prefix="loop_system_topology_command_family_parity_relaunch_") as repo_root:
        state_root = repo_root / ".loop"
        source_node = _make_recovery_source_node(node_id="child-relaunch-001", status="BLOCKED")
        _persist_recovery_base_state(
            state_root,
            node_id=source_node.node_id,
            status="BLOCKED",
            blocked_reason="staged workspace is inconsistent and needs a fresh launch",
        )
        accepted = submit_topology_mutation(
            state_root,
            build_relaunch_request(
                source_node,
                replacement_node_id="child-relaunch-001-v2",
                reason="materialize a fresh replacement node with refreshed execution policy",
                self_attribution="staged_workspace_corruption",
                self_repair="launch a fresh child from durable delegation instead of blaming implementation",
                execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "single_retry"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 3},
            ),
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("relaunch parity scenario requires an accepted relaunch request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("relaunch topology parity debugger must stay read-only")
        if str(debug.get("command_kind") or "") != "relaunch":
            return _fail("relaunch topology parity debugger must expose command_kind=relaunch")
        projected = dict(debug.get("projected_visible_effect") or {})
        if list(projected.get("target_node_ids") or []) != ["child-relaunch-001-v2"]:
            return _fail("relaunch topology parity debugger must expose the replacement target id")
        if dict(projected.get("target_statuses") or {}).get("child-relaunch-001-v2") != "ACTIVE":
            return _fail("relaunch topology parity debugger must expose the replacement target as ACTIVE")
        if not bool(debug.get("parity_ok")) or list(debug.get("mismatches") or []):
            return _fail("accepted relaunch topology parity debugger must not report mismatches")

    print("[loop-system-topology-command-family-parity][OK] topology-family parity debugger stays read-only and covers split/retry/activate/relaunch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
