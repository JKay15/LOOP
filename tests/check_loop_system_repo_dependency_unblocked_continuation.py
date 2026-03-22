#!/usr/bin/env python3
"""Validate dependency-unblocked continuation for blocked LOOP nodes."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-dependency-unblocked-continuation][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_minimal_endpoint_artifact(path: Path) -> None:
    payload = {
        "artifact_ref": str(path.resolve()),
        "confirmed_requirements": [],
        "denied_requirements": [],
        "latest_turn_ref": "turn-1",
        "mode": "bypassed",
        "original_user_prompt": "continue a blocked whole-paper integration node when dependency results arrive",
        "question_history": [],
        "requirement_artifact": {
            "artifact_ready_for_persistence": True,
            "final_effect": "continue the exact blocked integration node after dependency results are ready",
            "workflow_scope": "whole_paper_formalization",
            "hard_constraints": ["use committed kernel-reviewed continuation only"],
            "non_goals": ["do not invent replacement node ids"],
            "observable_success_criteria": ["same-node continuation relaunch occurs"],
            "open_questions": [],
            "relevant_context": ["dependencies may finish after the integration node first writes a blocked result"],
            "sufficient": True,
            "task_type": "runtime-hardening",
            "user_request_summary": "continue a blocked integration node after dependency results arrive",
        },
        "session_root": "endpoint_session",
        "status": "BYPASSED",
        "turn_count": 1,
        "version": "0.1",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_node_handoff(
    *,
    state_root: Path,
    workspace_root: Path,
    node_id: str,
    round_id: str,
    goal_slice: str,
    result_sink_ref: str,
) -> None:
    from loop_product.runtime_paths import node_machine_handoff_ref

    endpoint_artifact_ref = workspace_root / "EndpointArtifact.json"
    _write_minimal_endpoint_artifact(endpoint_artifact_ref)
    handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id=node_id)
    workspace_result_sink = workspace_root / result_sink_ref
    kernel_result_sink = state_root / result_sink_ref
    handoff_ref.parent.mkdir(parents=True, exist_ok=True)
    handoff_ref.write_text(
        json.dumps(
            {
                "node_id": node_id,
                "round_id": round_id,
                "state_root": str(state_root.resolve()),
                "endpoint_artifact_ref": str(endpoint_artifact_ref.resolve()),
                "root_goal": "continue the exact blocked integration node after dependency results are ready",
                "child_goal_slice": goal_slice,
                "workspace_root": str(workspace_root.resolve()),
                "workspace_mirror_relpath": "deliverables/primary_artifact",
                "external_publish_target": "",
                "context_refs": [str(endpoint_artifact_ref.resolve())],
                "result_sink_ref": result_sink_ref,
                "workspace_result_sink_ref": str(workspace_result_sink.resolve()),
                "kernel_result_sink_ref": str(kernel_result_sink.resolve()),
                "evaluator_submission_ref": str((state_root / "artifacts" / "bootstrap" / "EvaluatorNodeSubmission.json").resolve()),
                "evaluator_runner_ref": str((workspace_root / "RUN_EVALUATOR_NODE_UNTIL_TERMINAL.sh").resolve()),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeSpec, NodeStatus, RuntimeAttachmentState, normalize_runtime_state

    ensure_runtime_tree(state_root)
    authority = kernel_internal_authority()
    kernel_state = KernelState(
        task_id="dependency-unblocked-continuation",
        root_goal="continue a blocked integration node after dependency results become ready",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise dependency-unblocked continuation",
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
    source_workspace_root = ROOT / "workspace" / "test-dependency-unblocked-source-node"
    shutil.rmtree(source_workspace_root, ignore_errors=True)
    source_workspace_root.mkdir(parents=True, exist_ok=True)
    source_node = NodeSpec(
        node_id="source-node",
        node_kind="implementer",
        goal_slice="own the split children for a whole-paper continuation test",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "split_request", "resume_request", "retry_request", "relaunch_request"],
        workspace_root=str(source_workspace_root.resolve()),
        delegation_ref="state/delegations/source-node.json",
        result_sink_ref="artifacts/source-node/result.json",
        lineage_ref="root-kernel->source-node",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    source_record = kernel_state.nodes[source_node.node_id]
    source_record["runtime_state"] = normalize_runtime_state(
        {
            "attachment_state": RuntimeAttachmentState.ATTACHED.value,
            "observed_at": "",
            "summary": "source node already running",
            "observation_kind": "dispatch:started",
            "evidence_refs": [],
        }
    )
    persist_node_snapshot(state_root, root_node, authority=authority)
    persist_node_snapshot(state_root, source_record, authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    _write_node_handoff(
        state_root=state_root,
        workspace_root=source_workspace_root,
        node_id=source_node.node_id,
        round_id=source_node.round_id,
        goal_slice=source_node.goal_slice,
        result_sink_ref=source_node.result_sink_ref,
    )


def _materialize_child_nodes(state_root: Path) -> None:
    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus, RuntimeAttachmentState, normalize_runtime_state

    authority = kernel_internal_authority()
    kernel_state = load_kernel_state(state_root)
    dep_specs = [
        ("dep-linear", "close the linear chain", "artifacts/dep-linear/result.json"),
        ("dep-monotone", "close the monotone chain", "artifacts/dep-monotone/result.json"),
    ]
    for node_id, goal_slice, result_sink_ref in dep_specs:
        workspace_root = ROOT / "workspace" / f"test-dependency-unblocked-{node_id}"
        shutil.rmtree(workspace_root, ignore_errors=True)
        node = materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id="source-node",
            node_id=node_id,
            goal_slice=goal_slice,
            round_id=f"R1.{node_id}",
            execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 1},
            workspace_root=str(workspace_root.resolve()),
            result_sink_ref=result_sink_ref,
            authority=authority,
        )
        _write_node_handoff(
            state_root=state_root,
            workspace_root=Path(node.workspace_root),
            node_id=node.node_id,
            round_id=node.round_id,
            goal_slice=node.goal_slice,
            result_sink_ref=node.result_sink_ref,
        )

    integration_workspace_root = ROOT / "workspace" / "test-dependency-unblocked-final-integration"
    shutil.rmtree(integration_workspace_root, ignore_errors=True)
    integration_node = materialize_child(
        state_root=state_root,
        kernel_state=kernel_state,
        parent_node_id="source-node",
        node_id="final-integration",
        goal_slice="integrate dependency results into one final whole-paper outcome",
        round_id="R1.final-integration",
        execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "high", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        workspace_root=str(integration_workspace_root.resolve()),
        depends_on_node_ids=["dep-linear", "dep-monotone"],
        result_sink_ref="artifacts/final-integration/result.json",
        authority=authority,
    )
    integration_record = kernel_state.nodes[integration_node.node_id]
    integration_record["status"] = NodeStatus.BLOCKED.value
    integration_record["runtime_state"] = normalize_runtime_state(
        {
            "attachment_state": RuntimeAttachmentState.TERMINAL.value,
            "observed_at": "",
            "summary": "integration node stopped after recording a blocked dependency snapshot",
            "observation_kind": "authoritative_result",
            "evidence_refs": [],
        }
    )
    kernel_state.blocked_reasons[integration_node.node_id] = "waiting on dependency child results"
    persist_node_snapshot(state_root, integration_record, authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    _write_node_handoff(
        state_root=state_root,
        workspace_root=Path(integration_node.workspace_root),
        node_id=integration_node.node_id,
        round_id=integration_node.round_id,
        goal_slice=integration_node.goal_slice,
        result_sink_ref=integration_node.result_sink_ref,
    )


def _write_stale_blocked_result(state_root: Path) -> None:
    result_ref = state_root / "artifacts" / "final-integration" / "result.json"
    result_ref.parent.mkdir(parents=True, exist_ok=True)
    result_ref.write_text(
        json.dumps(
            {
                "schema": "loop_product.child_result",
                "node_id": "final-integration",
                "status": "BLOCKED",
                "outcome": "BLOCKED",
                "summary": "final integration is blocked because dependency child results were not ready yet",
                "dependency_status": {
                    "dep-linear": {
                        "state_status": "ACTIVE",
                        "result_sink_present": False,
                    },
                    "dep-monotone": {
                        "state_status": "ACTIVE",
                        "result_sink_present": False,
                    },
                },
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_dependency_results(state_root: Path) -> None:
    dep_payloads = {
        "dep-linear": {
            "status": "BLOCKED",
            "outcome": "PAPER_DEFECT_EXPOSED",
            "summary": "linear chain closed to a blocked-but-terminal paper defect exposure",
        },
        "dep-monotone": {
            "status": "COMPLETED",
            "outcome": "FULLY_FAITHFUL_COMPLETE",
            "summary": "monotone chain completed successfully",
        },
    }
    for node_id, payload in dep_payloads.items():
        result_ref = state_root / "artifacts" / node_id / "result.json"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": node_id,
                    **payload,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )


def _dependency_unblocked_continuation_case() -> int:
    import loop_product.runtime.recover as recover_module

    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state, query_kernel_state

    with tempfile.TemporaryDirectory(prefix="loop_system_dependency_unblocked_continuation_") as td:
        state_root = Path(td) / ".loop"
        cleanup_roots = [
            ROOT / "workspace" / "test-dependency-unblocked-source-node",
            ROOT / "workspace" / "test-dependency-unblocked-dep-linear",
            ROOT / "workspace" / "test-dependency-unblocked-dep-monotone",
            ROOT / "workspace" / "test-dependency-unblocked-final-integration",
        ]
        _persist_base_state(state_root)
        _materialize_child_nodes(state_root)
        _write_stale_blocked_result(state_root)
        _write_dependency_results(state_root)

        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[dict[str, object]] = []
        original_bootstrap = getattr(recover_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(recover_module, "launch_child_from_result_ref", None)

        def _fake_bootstrap(*, authority, **kwargs):
            del authority
            bootstrap_calls.append(dict(kwargs))
            bootstrap_ref = state_root / "artifacts" / "bootstrap" / "final-integration__resume_bootstrap.json"
            bootstrap_ref.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "bootstrap_result_ref": str(bootstrap_ref.resolve()),
                "node_id": str(kwargs.get("node_id") or ""),
                "workspace_root": str(kwargs.get("workspace_root") or ""),
                "state_root": str(kwargs.get("state_root") or ""),
                "mode": str(kwargs.get("mode") or ""),
                "launch_spec": {
                    "argv": ["/bin/echo", "resume-final-integration"],
                    "env": {},
                    "cwd": str(kwargs.get("workspace_root") or ""),
                    "stdin_path": str((Path(str(kwargs.get("workspace_root") or "")) / "CHILD_PROMPT.md").resolve()),
                },
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            launch_calls.append({"result_ref": str(Path(result_ref).resolve())})
            return {"launch_result_ref": str(Path(result_ref).resolve())}

        setattr(recover_module, "bootstrap_first_implementer_node", _fake_bootstrap)
        setattr(recover_module, "launch_child_from_result_ref", _fake_launch)
        try:
            query_kernel_state(state_root)
            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            integration_graph = node_graph.get("final-integration")
            if integration_graph is None:
                return _fail("integration node must remain present in authority view")
            if integration_graph["status"] != "ACTIVE":
                return _fail("dependency-unblocked continuation must return the blocked integration node to ACTIVE")
            if len(bootstrap_calls) != 1:
                return _fail(
                    f"dependency-unblocked continuation must exact-bootstrap the same logical node once (saw {len(bootstrap_calls)})"
                )
            if len(launch_calls) != 1:
                return _fail(
                    f"dependency-unblocked continuation must launch the exact resumed node once (saw {len(launch_calls)})"
                )
            bootstrap_request = dict(bootstrap_calls[0])
            if bootstrap_request.get("mode") != "continue_exact":
                return _fail("dependency-unblocked continuation must relaunch with continue_exact bootstrap semantics")
            if bootstrap_request.get("node_id") != "final-integration":
                return _fail("dependency-unblocked continuation must resume the same logical node id")

            accepted_envelopes = json.loads((state_root / "state" / "accepted_envelopes.json").read_text(encoding="utf-8"))
            accepted_resume = [
                item
                for item in accepted_envelopes
                if str(((item.get("payload") or {}).get("topology_mutation") or {}).get("kind") or "") == "resume"
                and str(((item.get("payload") or {}).get("topology_mutation") or {}).get("source_node_id") or "") == "final-integration"
            ]
            if len(accepted_resume) != 1:
                return _fail("dependency-unblocked continuation must persist one committed resume envelope for the integration node")

            kernel_state = load_kernel_state(state_root)
            integration_record = dict(kernel_state.nodes.get("final-integration") or {})
            runtime_state = dict(integration_record.get("runtime_state") or {})
            if integration_record.get("status") != "ACTIVE":
                return _fail("dependency-unblocked continuation must persist ACTIVE kernel state for the resumed node")
            if str(runtime_state.get("attachment_state") or "") != "UNOBSERVED":
                return _fail("dependency-unblocked continuation must reset runtime attachment state before relaunch")
            if "final-integration" in kernel_state.blocked_reasons:
                return _fail("dependency-unblocked continuation must clear the stale blocked reason")
        finally:
            for path in cleanup_roots:
                shutil.rmtree(path, ignore_errors=True)
            if original_bootstrap is not None:
                setattr(recover_module, "bootstrap_first_implementer_node", original_bootstrap)
            else:
                delattr(recover_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                setattr(recover_module, "launch_child_from_result_ref", original_launch)
            else:
                delattr(recover_module, "launch_child_from_result_ref")

    return 0


def main() -> int:
    return _dependency_unblocked_continuation_case()


if __name__ == "__main__":
    raise SystemExit(main())
