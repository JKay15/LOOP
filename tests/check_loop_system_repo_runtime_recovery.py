#!/usr/bin/env python3
"""Validate kernel-approved runtime recovery for the standalone LOOP product repo."""

from __future__ import annotations

from contextlib import contextmanager
import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-recovery][FAIL] {msg}", file=sys.stderr)
    return 2


@contextmanager
def _temporary_runtime_repo(prefix: str):
    from loop_product.runtime import cleanup_test_repo_services

    with tempfile.TemporaryDirectory(prefix=prefix) as td:
        temp_root = Path(td).resolve()
        try:
            yield temp_root
        finally:
            cleanup_receipt = cleanup_test_repo_services(
                repo_root=temp_root,
                settle_timeout_s=4.0,
                poll_interval_s=0.05,
            )
            if not bool(cleanup_receipt.get("quiesced")):
                raise RuntimeError(
                    "runtime recovery temp repo cleanup must quiesce repo services, "
                    f"got: {cleanup_receipt!r}"
                )
            if dict(cleanup_receipt.get("remaining_live_services") or {}):
                raise RuntimeError(
                    "runtime recovery temp repo cleanup must not leave live repo services behind, "
                    f"got: {cleanup_receipt!r}"
                )
            if dict(cleanup_receipt.get("remaining_retired_pids") or {}):
                raise RuntimeError(
                    "runtime recovery temp repo cleanup must not leave retired repo-service pids behind, "
                    f"got: {cleanup_receipt!r}"
                )
            if dict(cleanup_receipt.get("remaining_orphan_service_pids") or {}):
                raise RuntimeError(
                    "runtime recovery temp repo cleanup must not leave orphan repo-service pids behind, "
                    f"got: {cleanup_receipt!r}"
                )


def _make_source_node(
    *,
    node_id: str,
    status: str,
    workspace_root: str = "",
    workflow_scope: str = "generic",
    artifact_scope: str = "task",
    terminal_authority_scope: str = "local",
    required_output_paths: list[str] | None = None,
    startup_required_output_paths: list[str] | None = None,
    progress_checkpoints: list[dict[str, object]] | None = None,
):
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
        workflow_scope=workflow_scope,
        artifact_scope=artifact_scope,
        terminal_authority_scope=terminal_authority_scope,
        required_output_paths=list(required_output_paths or []),
        startup_required_output_paths=list(startup_required_output_paths or []),
        progress_checkpoints=list(progress_checkpoints or []),
        workspace_root=workspace_root,
        delegation_ref=f"state/delegations/{node_id}.json",
        result_sink_ref=f"artifacts/{node_id}/result.json",
        lineage_ref=f"root-kernel->{node_id}",
        status=NodeStatus(status),
    )


def _persist_base_state(
    state_root: Path,
    *,
    source_node_id: str,
    source_status: str,
    blocked_reason: str = "",
    source_workspace_root: str = "",
    workflow_scope: str = "generic",
    artifact_scope: str = "task",
    terminal_authority_scope: str = "local",
    required_output_paths: list[str] | None = None,
    startup_required_output_paths: list[str] | None = None,
    progress_checkpoints: list[dict[str, object]] | None = None,
) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave12-runtime-recovery",
        root_goal="prove runtime recovery proposals stay kernel-owned and durable",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise runtime recovery",
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
    source_node = _make_source_node(
        node_id=source_node_id,
        status=source_status,
        workspace_root=source_workspace_root,
        workflow_scope=workflow_scope,
        artifact_scope=artifact_scope,
        terminal_authority_scope=terminal_authority_scope,
        required_output_paths=required_output_paths,
        startup_required_output_paths=startup_required_output_paths,
        progress_checkpoints=progress_checkpoints,
    )
    kernel_state.register_node(source_node)
    if blocked_reason:
        kernel_state.blocked_reasons[source_node.node_id] = blocked_reason
    persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
    persist_node_snapshot(state_root, source_node, authority=kernel_internal_authority())
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _persist_source_runtime_state(
    state_root: Path,
    *,
    source_node_id: str,
    runtime_state: dict[str, object],
) -> None:
    state_ref = state_root / "state" / f"{source_node_id}.json"
    state_payload = json.loads(state_ref.read_text(encoding="utf-8"))
    state_payload["runtime_state"] = dict(runtime_state)
    state_ref.chmod(0o644)
    state_ref.write_text(json.dumps(state_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    kernel_ref = state_root / "state" / "kernel_state.json"
    kernel_payload = json.loads(kernel_ref.read_text(encoding="utf-8"))
    kernel_payload["nodes"][source_node_id]["runtime_state"] = dict(runtime_state)
    kernel_ref.chmod(0o644)
    kernel_ref.write_text(json.dumps(kernel_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _persist_resume_handoff(
    state_root: Path,
    *,
    temp_root: Path,
    source_node,
    workspace_root: Path,
    workflow_scope: str,
    artifact_scope: str,
    terminal_authority_scope: str,
    startup_required_output_paths: list[str] | None = None,
    progress_checkpoints: list[dict[str, object]] | None = None,
) -> None:
    from loop_product.runtime_paths import node_machine_handoff_ref

    handoff_path = node_machine_handoff_ref(state_root=state_root, node_id=source_node.node_id)
    handoff_path.parent.mkdir(parents=True, exist_ok=True)
    handoff_payload = {
        "node_id": source_node.node_id,
        "parent_node_id": source_node.parent_node_id,
        "round_id": source_node.round_id,
        "lineage_ref": source_node.lineage_ref,
        "workspace_root": str(workspace_root.resolve()),
        "state_root": str(state_root.resolve()),
        "endpoint_artifact_ref": str((temp_root / "EndpointArtifact.json").resolve()),
        "root_goal": "resume the same slice after a recoverable runtime loss",
        "child_goal_slice": source_node.goal_slice,
        "goal_slice": source_node.goal_slice,
        "workflow_scope": workflow_scope,
        "artifact_scope": artifact_scope,
        "terminal_authority_scope": terminal_authority_scope,
        "workspace_mirror_relpath": "deliverables/primary_artifact",
        "workspace_live_artifact_relpath": f"../../.loop/test_runtime_resume/live_artifacts/{source_node.node_id}/primary_artifact",
        "external_publish_target": "",
        "required_output_paths": [],
        "startup_required_output_paths": list(startup_required_output_paths or []),
        "progress_checkpoints": list(progress_checkpoints or []),
        "context_refs": [str((temp_root / "Context.md").resolve())],
        "result_sink_ref": source_node.result_sink_ref,
    }
    handoff_payload["required_outputs"] = list(handoff_payload["required_output_paths"])
    handoff_path.write_text(json.dumps(handoff_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _accepted_resume_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_resume_request

    with _temporary_runtime_repo("loop_system_runtime_resume_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-blocked-001", status="BLOCKED")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="BLOCKED",
            blocked_reason="waiting on an external prerequisite that is now repaired",
        )
        mutation = build_resume_request(
            source_node,
            reason="external prerequisite repaired and the same node can continue",
            consistency_signal="dependency_unblocked",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"resume request must be accepted after consistency passes, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("accepted resume must reactivate the blocked node")
        if source_node.node_id in authority["blocked_reasons"]:
            return _fail("accepted resume must clear the current blocked reason")

        audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
        if not any("resume" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
            return _fail("accepted resume must appear in recent structural decisions")

    return 0


def _accepted_resume_relaunch_preserves_exact_runtime_contract_case() -> int:
    import json

    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_resume_request
    from loop_product.runtime_paths import node_machine_handoff_ref
    import loop_product.runtime.recover as recover_module

    with _temporary_runtime_repo("loop_system_runtime_resume_relaunch_") as temp_root:
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace" / "resume-child"
        workspace_root.mkdir(parents=True, exist_ok=True)
        startup_required_output_paths = [
            "README.md",
            "TRACEABILITY.md",
            "analysis/STARTUP_BOUNDARY.json",
        ]
        progress_checkpoints = [
            {
                "checkpoint_id": "split_decision",
                "description": "record the first machine-auditable split decision after startup",
                "required_any_of": [
                    "split/SPLIT_REQUEST_SUBMISSION_RESULT.json",
                    "split/SPLIT_DECLINE_REASON.md",
                ],
                "window_s": 300.0,
            }
        ]
        source_node = _make_source_node(
            node_id="child-resume-relaunch-001",
            status="BLOCKED",
            workspace_root=str(workspace_root.resolve()),
            workflow_scope="whole_paper_formalization",
            artifact_scope="slice",
            terminal_authority_scope="local",
            startup_required_output_paths=startup_required_output_paths,
            progress_checkpoints=progress_checkpoints,
        )
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="BLOCKED",
            blocked_reason="runtime detached after startup bundle was already materialized",
            source_workspace_root=str(workspace_root.resolve()),
            workflow_scope="whole_paper_formalization",
            artifact_scope="slice",
            terminal_authority_scope="local",
            startup_required_output_paths=startup_required_output_paths,
            progress_checkpoints=progress_checkpoints,
        )
        handoff_path = node_machine_handoff_ref(state_root=state_root, node_id=source_node.node_id)
        handoff_path.parent.mkdir(parents=True, exist_ok=True)
        handoff_payload = {
            "node_id": source_node.node_id,
            "parent_node_id": source_node.parent_node_id,
            "round_id": source_node.round_id,
            "lineage_ref": source_node.lineage_ref,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "endpoint_artifact_ref": str((temp_root / "EndpointArtifact.json").resolve()),
            "root_goal": "resume the same slice after a recoverable runtime loss",
            "child_goal_slice": source_node.goal_slice,
            "goal_slice": source_node.goal_slice,
            "workflow_scope": "whole_paper_formalization",
            "artifact_scope": "slice",
            "terminal_authority_scope": "local",
            "workspace_mirror_relpath": "deliverables/primary_artifact",
            "workspace_live_artifact_relpath": "../../.loop/test_runtime_resume/live_artifacts/child-resume-relaunch-001/primary_artifact",
            "external_publish_target": "",
            "required_output_paths": [],
            "startup_required_output_paths": startup_required_output_paths,
            "progress_checkpoints": progress_checkpoints,
            "context_refs": [str((temp_root / "Context.md").resolve())],
            "result_sink_ref": source_node.result_sink_ref,
        }
        handoff_payload["required_outputs"] = list(handoff_payload["required_output_paths"])
        handoff_path.write_text(json.dumps(handoff_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[str] = []
        supervision_calls: list[str] = []
        terminator_calls: list[str] = []
        original_bootstrap = recover_module.bootstrap_first_implementer_node
        original_launch = recover_module.launch_child_from_result_ref
        original_supervision = getattr(recover_module, "ensure_child_supervision_running", None)
        original_terminator = recover_module.terminate_runtime_owned_launch_result_ref
        import loop_product.child_supervision_sidecar as sidecar_module
        original_sidecar_supervision = sidecar_module.ensure_child_supervision_running
        try:
            def _fake_bootstrap_first_implementer_node(**kwargs):
                bootstrap_calls.append(dict(kwargs))
                return {"bootstrap_result_ref": str((temp_root / "bootstrap-result.json").resolve())}

            def _fake_launch_child_from_result_ref(*, result_ref: str):
                launch_calls.append(str(result_ref))
                return {"launch_result_ref": str((temp_root / "launches" / "attempt_001" / "ChildLaunchResult.json").resolve())}

            def _fake_ensure_child_supervision_running(*, launch_result_ref: str):
                supervision_calls.append(str(launch_result_ref))
                return {"launch_result_ref": str(launch_result_ref), "pid": 12345}

            def _fake_terminate_runtime_owned_launch_result_ref(*, result_ref: str):
                terminator_calls.append(str(result_ref))

            recover_module.bootstrap_first_implementer_node = _fake_bootstrap_first_implementer_node
            recover_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
            sidecar_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
            recover_module.terminate_runtime_owned_launch_result_ref = _fake_terminate_runtime_owned_launch_result_ref

            mutation = build_resume_request(
                source_node,
                reason="runtime transport was lost but the same slice should continue from durable state",
                consistency_signal="runtime_relaunch_authorized",
            )
            envelope = submit_topology_mutation(
                state_root,
                mutation,
                round_id=source_node.round_id,
                generation=source_node.generation,
            )
        finally:
            recover_module.bootstrap_first_implementer_node = original_bootstrap
            recover_module.launch_child_from_result_ref = original_launch
            if original_supervision is not None:
                recover_module.ensure_child_supervision_running = original_supervision
            elif hasattr(recover_module, "ensure_child_supervision_running"):
                delattr(recover_module, "ensure_child_supervision_running")
            sidecar_module.ensure_child_supervision_running = original_sidecar_supervision
            recover_module.terminate_runtime_owned_launch_result_ref = original_terminator

        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"resume relaunch with exact frozen handoff must be accepted, got {envelope.status.value!r}")
        if len(bootstrap_calls) != 1:
            return _fail("accepted resume relaunch must rebuild exactly one continue_exact bootstrap bundle")
        if bootstrap_calls[0].get("mode") != "continue_exact":
            return _fail("accepted resume relaunch must reuse continue_exact bootstrap semantics")
        if str(bootstrap_calls[0].get("workspace_live_artifact_relpath") or "") != handoff_payload["workspace_live_artifact_relpath"]:
            return _fail("accepted resume relaunch must preserve workspace_live_artifact_relpath from the frozen handoff")
        if list(bootstrap_calls[0].get("startup_required_output_paths") or []) != startup_required_output_paths:
            return _fail("accepted resume relaunch must preserve startup_required_output_paths from the frozen handoff")
        if list(bootstrap_calls[0].get("progress_checkpoints") or []) != progress_checkpoints:
            return _fail("accepted resume relaunch must preserve progress_checkpoints from the frozen handoff")
        if launch_calls != [str((temp_root / "bootstrap-result.json").resolve())]:
            return _fail("accepted resume relaunch must launch exactly the refreshed bootstrap result")
        expected_launch_result_ref = str((temp_root / "launches" / "attempt_001" / "ChildLaunchResult.json").resolve())
        if supervision_calls != [expected_launch_result_ref]:
            return _fail("accepted resume relaunch must immediately attach committed child supervision to the relaunched child")
        if terminator_calls:
            return _fail("successful accepted resume relaunch must not terminate the fresh launch result")

    return 0


def _accepted_retry_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_retry_request

    with _temporary_runtime_repo("loop_system_runtime_retry_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-retry-001", status="FAILED")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="FAILED",
        )
        mutation = build_retry_request(
            source_node,
            reason="the evaluator found a self-caused path issue and can retry safely",
            self_attribution="evaluator_path_error",
            self_repair="fix the staged command path before retrying the same node",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"retry request must be accepted with bounded self-repair evidence, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("accepted retry must reactivate the same node")

        audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
        if not any(
            item.get("self_attribution") == "evaluator_path_error"
            and item.get("self_repair") == "fix the staged command path before retrying the same node"
            for item in audit_view["experience_assets"]
        ):
            return _fail("accepted retry must preserve self-attribution and self-repair evidence")

    return 0


def _accepted_orphaned_active_retry_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.liveness import build_runtime_loss_signal
    from loop_product.runtime.recover import build_retry_request

    with _temporary_runtime_repo("loop_system_runtime_orphaned_retry_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-active-lost-001", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_retry_request(
            source_node,
            reason="the backing child transport disappeared but the same node should continue in the same workspace",
            self_attribution="child_runtime_detached",
            self_repair="restart the same implementer node from the latest durable checkpoint",
            payload={
                "runtime_loss_signal": build_runtime_loss_signal(
                    observation_kind="child_runtime_detached",
                    summary="authoritative state still showed ACTIVE, but the backing child session disappeared",
                    evidence_refs=["threads.sqlite:latest-child-thread"],
                )
            },
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"retry on an orphaned ACTIVE node must be accepted when runtime-loss evidence exists, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("accepted orphaned-active retry must keep the same node ACTIVE")
        source_runtime = next(
            (item.get("runtime_state") for item in authority["node_graph"] if item["node_id"] == source_node.node_id),
            None,
        )
        if not isinstance(source_runtime, dict):
            return _fail("authority view must expose runtime_state for recoverable nodes")
        if str(source_runtime.get("attachment_state") or "") != "UNOBSERVED":
            return _fail("accepted orphaned-active retry must reset runtime attachment to UNOBSERVED until the relaunched child heartbeats")
        if "child_runtime_detached" not in str(source_runtime.get("summary") or ""):
            return _fail("accepted orphaned-active retry must preserve the runtime-loss reason in runtime_state summary")

    return 0


def _accepted_relaunch_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_relaunch_request
    import loop_product.runtime.recover as recover_module
    import loop_product.child_supervision_sidecar as sidecar_module

    with _temporary_runtime_repo("loop_system_runtime_relaunch_") as temp_root:
        state_root = temp_root / ".loop"
        with tempfile.TemporaryDirectory(prefix="runtime_relaunch_ws_", dir=ROOT / "workspace") as workspace_td:
            workspace_root = Path(workspace_td).resolve()
            source_node = _make_source_node(
                node_id="child-relaunch-001",
                status="BLOCKED",
                workspace_root=str(workspace_root),
            )
            _persist_base_state(
                state_root,
                source_node_id=source_node.node_id,
                source_status="BLOCKED",
                blocked_reason="staged workspace is inconsistent and needs a fresh launch",
                source_workspace_root=str(workspace_root),
            )
            _persist_resume_handoff(
                state_root,
                temp_root=temp_root,
                source_node=source_node,
                workspace_root=workspace_root,
                workflow_scope="generic",
                artifact_scope="task",
                terminal_authority_scope="local",
            )

            bootstrap_calls: list[dict[str, object]] = []
            launch_calls: list[str] = []
            supervision_calls: list[str] = []
            terminator_calls: list[str] = []
            original_bootstrap = recover_module.bootstrap_first_implementer_node
            original_launch = recover_module.launch_child_from_result_ref
            original_terminator = recover_module.terminate_runtime_owned_launch_result_ref
            original_sidecar_supervision = sidecar_module.ensure_child_supervision_running
            try:
                def _fake_bootstrap_first_implementer_node(**kwargs):
                    bootstrap_calls.append(dict(kwargs))
                    return {"bootstrap_result_ref": str((temp_root / "relaunch-bootstrap-result.json").resolve())}

                def _fake_launch_child_from_result_ref(*, result_ref: str):
                    launch_calls.append(str(result_ref))
                    return {"launch_result_ref": str((temp_root / "relaunches" / "attempt_001" / "ChildLaunchResult.json").resolve())}

                def _fake_ensure_child_supervision_running(*, launch_result_ref: str):
                    supervision_calls.append(str(launch_result_ref))
                    return {"launch_result_ref": str(launch_result_ref), "pid": 12345}

                def _fake_terminate_runtime_owned_launch_result_ref(*, result_ref: str):
                    terminator_calls.append(str(result_ref))

                recover_module.bootstrap_first_implementer_node = _fake_bootstrap_first_implementer_node
                recover_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
                recover_module.terminate_runtime_owned_launch_result_ref = _fake_terminate_runtime_owned_launch_result_ref
                sidecar_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running

                mutation = build_relaunch_request(
                    source_node,
                    replacement_node_id="child-relaunch-001-v2",
                    reason="materialize a fresh replacement node with refreshed execution policy",
                    self_attribution="staged_workspace_corruption",
                    self_repair="launch a fresh child from durable delegation instead of blaming implementation",
                    execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "single_retry"},
                    reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                    budget_profile={"max_rounds": 3},
                )
                envelope = submit_topology_mutation(
                    state_root,
                    mutation,
                    round_id=source_node.round_id,
                    generation=source_node.generation,
                )
            finally:
                recover_module.bootstrap_first_implementer_node = original_bootstrap
                recover_module.launch_child_from_result_ref = original_launch
                recover_module.terminate_runtime_owned_launch_result_ref = original_terminator
                sidecar_module.ensure_child_supervision_running = original_sidecar_supervision

            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"relaunch request must be accepted with a replacement node spec, got {envelope.status.value!r}")
            if len(bootstrap_calls) != 1:
                return _fail("accepted relaunch must rebuild exactly one replacement bootstrap bundle")
            if bootstrap_calls[0].get("mode") != "continue_exact":
                return _fail("accepted relaunch must reuse continue_exact bootstrap semantics for the replacement child")
            if str(bootstrap_calls[0].get("node_id") or "") != "child-relaunch-001-v2":
                return _fail("accepted relaunch must bootstrap the exact replacement child node")
            if launch_calls != [str((temp_root / "relaunch-bootstrap-result.json").resolve())]:
                return _fail("accepted relaunch must launch exactly the refreshed replacement bootstrap result")
            expected_launch_result_ref = str((temp_root / "relaunches" / "attempt_001" / "ChildLaunchResult.json").resolve())
            if supervision_calls != [expected_launch_result_ref]:
                return _fail("accepted relaunch must immediately attach committed child supervision to the replacement launch")
            if terminator_calls:
                return _fail("successful accepted relaunch must not terminate the fresh replacement launch")

            authority = query_authority_view(state_root)
            replacement = next(
                (item for item in authority["node_graph"] if item["node_id"] == "child-relaunch-001-v2"),
                None,
            )
            if replacement is None:
                return _fail("accepted relaunch must materialize a replacement child node")
            if replacement["parent_node_id"] != "root-kernel":
                return _fail("replacement child must preserve the original parent")
            if replacement["status"] != "ACTIVE":
                return _fail("replacement child must become ACTIVE immediately after accepted relaunch")
            if authority["node_lifecycle"].get(source_node.node_id) != "FAILED":
                return _fail("accepted relaunch must freeze the superseded source node as FAILED")

            replacement_state = state_root / "state" / "child-relaunch-001-v2.json"
            replacement_delegation = state_root / "state" / "delegations" / "child-relaunch-001-v2.json"
            if not replacement_state.exists() or not replacement_delegation.exists():
                return _fail("accepted relaunch must persist both replacement node state and delegation artifacts")

            audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
            if not any("relaunch" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
                return _fail("accepted relaunch must appear in recent structural decisions")

    return 0


def _accepted_orphaned_active_relaunch_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.liveness import build_runtime_loss_signal
    from loop_product.runtime.recover import build_relaunch_request

    with _temporary_runtime_repo("loop_system_runtime_orphaned_relaunch_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-active-lost-002", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_relaunch_request(
            source_node,
            replacement_node_id="child-active-lost-002-v2",
            reason="the same task must continue, but the dead runtime should be replaced with a fresh child",
            self_attribution="child_runtime_detached",
            self_repair="materialize a fresh child node from durable delegation after the backing runtime vanished",
            payload={
                "runtime_loss_signal": build_runtime_loss_signal(
                    observation_kind="child_runtime_detached",
                    summary="root supervision observed the child session vanish while node status remained ACTIVE",
                    evidence_refs=["child_session_ref:missing"],
                )
            },
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"relaunch on an orphaned ACTIVE node must be accepted when runtime-loss evidence exists, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "FAILED":
            return _fail("accepted orphaned-active relaunch must freeze the lost source node as FAILED")
        replacement = next(
            (item for item in authority["node_graph"] if item["node_id"] == "child-active-lost-002-v2"),
            None,
        )
        if replacement is None or replacement["status"] != "ACTIVE":
            return _fail("accepted orphaned-active relaunch must materialize an ACTIVE replacement node")
        source_runtime = next(
            (item.get("runtime_state") for item in authority["node_graph"] if item["node_id"] == source_node.node_id),
            None,
        )
        if not isinstance(source_runtime, dict) or str(source_runtime.get("attachment_state") or "") != "LOST":
            return _fail("accepted orphaned-active relaunch must preserve the source node's LOST runtime attachment state")

    return 0


def _rejected_resume_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_resume_request

    with _temporary_runtime_repo("loop_system_runtime_resume_reject_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-active-001", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_resume_request(
            source_node,
            reason="this should be rejected because the node is already active",
            consistency_signal="manual_override",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"resume on an already-active node must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("rejected recovery request must not mutate the source node lifecycle state")
        if any(node["node_id"] != "root-kernel" and node["node_id"] != source_node.node_id for node in authority["node_graph"]):
            return _fail("rejected recovery request must not materialize extra child nodes")

    return 0


def _rejected_orphaned_active_retry_without_loss_signal() -> int:
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_retry_request

    with _temporary_runtime_repo("loop_system_runtime_orphaned_retry_reject_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-active-002", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_retry_request(
            source_node,
            reason="this should be rejected because an ACTIVE node needs explicit runtime-loss evidence before retry",
            self_attribution="child_runtime_detached",
            self_repair="restart the same node",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"retry on an ACTIVE node without runtime-loss evidence must be rejected, got {envelope.status.value!r}")

    return 0


def _rejected_retry_for_state_blocked_node_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_retry_request

    with _temporary_runtime_repo("loop_system_runtime_retry_state_blocked_") as temp_root:
        state_root = temp_root / ".loop"
        source_node = _make_source_node(node_id="child-blocked-state-001", status="BLOCKED")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="BLOCKED",
            blocked_reason="dependency-gated whole-paper closeout blocker",
        )
        status_result_ref = (
            state_root
            / "artifacts"
            / "launches"
            / source_node.node_id
            / "attempt_001"
            / "ChildRuntimeStatusResult.json"
        )
        status_result_ref.parent.mkdir(parents=True, exist_ok=True)
        status_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(
                        (
                            status_result_ref.parent
                            / "ChildLaunchResult.json"
                        ).resolve()
                    ),
                    "status_result_ref": str(status_result_ref.resolve()),
                    "node_id": source_node.node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str((temp_root / "workspace" / source_node.node_id).resolve()),
                    "runtime_attachment_state": "LOST",
                    "runtime_observation_kind": "trusted_runtime_status_surface",
                    "runtime_observed_at": "2026-03-27T16:43:23Z",
                    "runtime_summary": "child supervision settled after the node itself became blocked",
                    "runtime_evidence_refs": [str(status_result_ref.resolve())],
                    "pid_alive": False,
                    "pid": 64961,
                    "lifecycle_status": "BLOCKED",
                    "recovery_eligible": False,
                    "recovery_reason": "node_status_blocked",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        _persist_source_runtime_state(
            state_root,
            source_node_id=source_node.node_id,
            runtime_state={
                "attachment_state": "LOST",
                "evidence_refs": [str(status_result_ref.resolve())],
                "observation_kind": "supervision:retry_budget_exhausted",
                "observed_at": "2026-03-27T16:43:23Z",
                "summary": "child supervision settled to retry_budget_exhausted after bounded recovery attempts were exhausted",
            },
        )
        mutation = build_retry_request(
            source_node,
            reason="provider pressure eased, so try the same node again",
            self_attribution="provider_capacity",
            self_repair="retry the same logical node in place after provider pressure drops",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail("retry must be rejected when the latest runtime truth already says the BLOCKED node is state-blocked")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "BLOCKED":
            return _fail("rejected retry for a state-blocked node must leave the node BLOCKED")

    return 0


def main() -> int:
    try:
        for check in (
            _accepted_resume_case,
            _accepted_resume_relaunch_preserves_exact_runtime_contract_case,
            _accepted_retry_case,
            _accepted_orphaned_active_retry_case,
            _accepted_relaunch_case,
            _accepted_orphaned_active_relaunch_case,
            _rejected_resume_case,
            _rejected_orphaned_active_retry_without_loss_signal,
            _rejected_retry_for_state_blocked_node_case,
        ):
            rc = check()
            if rc != 0:
                return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"runtime recovery runtime raised unexpectedly: {exc}")

    print("[loop-system-runtime-recovery] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
