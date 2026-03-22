#!/usr/bin/env python3
"""Validate kernel-owned activation of deferred split children."""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-deferred-activation][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_source_node(*, workspace_root: Path | None = None):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="formalize the primary theorem statement from the current paper",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "split_request"],
        workspace_root=str(workspace_root.resolve()) if workspace_root is not None else "",
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
    workspace_root = state_root.parent / "workspace" / "child-implementer-001"
    workspace_root.mkdir(parents=True, exist_ok=True)
    kernel_state = KernelState(
        task_id="wave11-deferred-activation",
        root_goal="activate deferred children only after dependency completion",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise deferred activation",
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
    source_node = _base_source_node(workspace_root=workspace_root)
    source_record = source_node.to_dict()
    source_record["runtime_state"] = {
        "attachment_state": "ATTACHED",
        "observed_at": "",
        "summary": "test source node already running",
        "observation_kind": "dispatch:started",
        "evidence_refs": [],
    }
    kernel_state.nodes[source_node.node_id] = source_record
    kernel_state.delegation_map[source_node.node_id] = root_node.node_id
    kernel_state.active_requirements.append(source_node.goal_slice)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
    _write_source_handoff(state_root=state_root, workspace_root=workspace_root, source_node=source_node)


def _write_source_handoff(*, state_root: Path, workspace_root: Path, source_node) -> Path:
    from loop_product.runtime_paths import node_machine_handoff_ref

    endpoint_artifact_ref = workspace_root / "EndpointArtifact.json"
    endpoint_artifact_ref.write_text(
        json.dumps(
            {
                "schema": "loop.endpoint_artifact",
                "artifact_ref": str(endpoint_artifact_ref.resolve()),
                "status": "BYPASSED",
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id=source_node.node_id)
    workspace_result_sink = workspace_root / "artifacts" / source_node.node_id / "result.json"
    kernel_result_sink = state_root / "artifacts" / source_node.node_id / "result.json"
    handoff_ref.parent.mkdir(parents=True, exist_ok=True)
    handoff_ref.write_text(
        json.dumps(
            {
                "node_id": source_node.node_id,
                "round_id": source_node.round_id,
                "state_root": str(state_root.resolve()),
                "endpoint_artifact_ref": str(endpoint_artifact_ref.resolve()),
                "root_goal": "activate deferred children only after dependency completion",
                "child_goal_slice": source_node.goal_slice,
                "workspace_root": str(workspace_root.resolve()),
                "workspace_mirror_relpath": "deliverables/primary_artifact",
                "external_publish_target": "",
                "context_refs": [str(endpoint_artifact_ref.resolve())],
                "result_sink_ref": source_node.result_sink_ref,
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
    return handoff_ref


def _mark_source_completed(state_root: Path, node_id: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus

    authority = kernel_internal_authority()
    kernel_state = load_kernel_state(state_root)
    kernel_state.nodes[node_id]["status"] = NodeStatus.COMPLETED.value
    persist_node_snapshot(state_root, kernel_state.nodes[node_id], authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)


def _submit_deferred_split(state_root: Path) -> None:
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    source_node = _base_source_node(workspace_root=state_root.parent / "workspace" / "child-implementer-001")
    mutation = build_split_request(
        source_node=source_node,
        target_nodes=[
            {
                "node_id": "child-followup-001",
                "goal_slice": "formalize the deferred proof obligations after the primary theorem lands",
                "depends_on_node_ids": [source_node.node_id],
                "activation_condition": f"after:{source_node.node_id}:terminal",
            }
        ],
        split_mode="deferred",
        completed_work="primary theorem statement stays with current node",
        remaining_work="proof obligations defer to a child after the source reaches terminal state",
        reason="persist a deferred follow-up child for proof obligations",
    )
    envelope = submit_topology_mutation(
        state_root,
        mutation,
        round_id=source_node.round_id,
        generation=source_node.generation,
    )
    if envelope.status is not EnvelopeStatus.ACCEPTED:
        raise RuntimeError(f"deferred split setup must be accepted, got {envelope.status.value!r}")


def _accepted_activation_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.activate import build_activate_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_activation_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        _submit_deferred_split(state_root)
        _mark_source_completed(state_root, "child-implementer-001")
        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[dict[str, object]] = []
        supervision_calls: list[dict[str, object]] = []
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)
        original_supervision = getattr(topology_module, "ensure_child_supervision_running", None)

        def _fake_bootstrap(**kwargs):
            bootstrap_calls.append(dict(kwargs))
            bootstrap_ref = state_root / "artifacts" / "bootstrap" / f"{kwargs['node_id']}__bootstrap.json"
            bootstrap_ref.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "bootstrap_result_ref": str(bootstrap_ref.resolve()),
                "node_id": str(kwargs["node_id"]),
                "workspace_root": str(kwargs["workspace_root"]),
                "state_root": str(kwargs["state_root"]),
                "launch_spec": {
                    "argv": ["/bin/echo", "deferred-activate"],
                    "env": {},
                    "cwd": str(kwargs["workspace_root"]),
                    "stdin_path": str((Path(str(kwargs["workspace_root"])) / "CHILD_PROMPT.md").resolve()),
                },
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            bootstrap_ref = Path(result_ref).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            launch_calls.append({"result_ref": str(bootstrap_ref), "launch_result_ref": str(launch_result_ref.resolve())})
            return {"launch_result_ref": str(launch_result_ref.resolve())}

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running

        try:
            mutation = build_activate_request(
                "child-followup-001",
                reason="source node completed, so the deferred child may now activate",
            )
            envelope = submit_topology_mutation(
                state_root,
                mutation,
                round_id="R1",
                generation=2,
            )
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"accepted activation path must return ACCEPTED, got {envelope.status.value!r}")

            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            child_graph = node_graph.get("child-followup-001")
            if child_graph is None:
                return _fail("accepted activation must preserve the deferred child in authority view")
            if child_graph["status"] != NodeStatus.ACTIVE.value:
                return _fail("accepted activation must promote the deferred child to ACTIVE")
            if "child-followup-001" in authority["planned_child_nodes"]:
                return _fail("accepted activation must remove the child from planned_child_nodes")
            if "child-followup-001" not in authority["active_child_nodes"]:
                return _fail("accepted activation must expose the child in active_child_nodes")
            if len(bootstrap_calls) != 1:
                return _fail("accepted activation must bootstrap the activated deferred child before returning")
            if len(launch_calls) != 1:
                return _fail("accepted activation must immediately launch the activated deferred child")
            if len(supervision_calls) != 1:
                return _fail("accepted activation must automatically attach committed supervision to the launched deferred child")

            child_state_path = state_root / "state" / "child-followup-001.json"
            child_delegation_path = state_root / "state" / "delegations" / "child-followup-001.json"
            child_state = json.loads(child_state_path.read_text(encoding="utf-8"))
            child_delegation = json.loads(child_delegation_path.read_text(encoding="utf-8"))
            if child_state.get("status") != NodeStatus.ACTIVE.value:
                return _fail("accepted activation must persist ACTIVE node state")
            if child_state.get("depends_on_node_ids") != ["child-implementer-001"]:
                return _fail("accepted activation must preserve deferred dependency metadata")
            if child_state.get("activation_condition") != "after:child-implementer-001:terminal":
                return _fail("accepted activation must preserve deferred activation_condition metadata")
            if child_delegation.get("depends_on_node_ids") != ["child-implementer-001"]:
                return _fail("accepted activation must not rewrite frozen delegation dependency metadata")
        finally:
            if original_bootstrap is not None:
                topology_module.bootstrap_first_implementer_node = original_bootstrap
            else:
                delattr(topology_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                topology_module.launch_child_from_result_ref = original_launch
            else:
                delattr(topology_module, "launch_child_from_result_ref")
            if original_supervision is not None:
                topology_module.ensure_child_supervision_running = original_supervision
            else:
                delattr(topology_module, "ensure_child_supervision_running")

    return 0


def _authoritative_deferred_result_continuation_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_activation_continuation_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        _submit_deferred_split(state_root)
        authority = kernel_internal_authority()
        kernel_state = load_kernel_state(state_root)
        source_record = kernel_state.nodes["child-implementer-001"]
        source_record["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "",
            "summary": "source lane stopped after accepted deferred split",
            "observation_kind": "authoritative_result",
            "evidence_refs": [],
        }
        persist_node_snapshot(state_root, source_record, authority=authority)
        persist_kernel_state(state_root, kernel_state, authority=authority)

        result_ref = state_root / "artifacts" / "child-implementer-001" / "result.json"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": "child-implementer-001",
                    "status": "ACTIVE",
                    "outcome": "REPAIR_REQUIRED",
                    "summary": "source lane completed its own work and accepted a deferred split for follow-up proof obligations",
                    "split": {
                        "deferred_request": {
                            "proposed": True,
                            "accepted": True,
                            "planned_child_ids": ["child-followup-001"],
                        }
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[dict[str, object]] = []
        supervision_calls: list[dict[str, object]] = []
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)
        original_supervision = getattr(topology_module, "ensure_child_supervision_running", None)

        def _fake_bootstrap(**kwargs):
            bootstrap_calls.append(dict(kwargs))
            bootstrap_ref = state_root / "artifacts" / "bootstrap" / f"{kwargs['node_id']}__bootstrap.json"
            bootstrap_ref.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "bootstrap_result_ref": str(bootstrap_ref.resolve()),
                "node_id": str(kwargs["node_id"]),
                "workspace_root": str(kwargs["workspace_root"]),
                "state_root": str(kwargs["state_root"]),
                "launch_spec": {
                    "argv": ["/bin/echo", "deferred-continuation"],
                    "env": {},
                    "cwd": str(kwargs["workspace_root"]),
                    "stdin_path": str((Path(str(kwargs["workspace_root"])) / "CHILD_PROMPT.md").resolve()),
                },
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            bootstrap_ref = Path(result_ref).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            launch_calls.append({"result_ref": str(bootstrap_ref), "launch_result_ref": str(launch_result_ref.resolve())})
            return {"launch_result_ref": str(launch_result_ref.resolve())}

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            authority_view = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority_view["node_graph"]}
            source_graph = node_graph.get("child-implementer-001")
            child_graph = node_graph.get("child-followup-001")
            if source_graph is None or source_graph["status"] != NodeStatus.COMPLETED.value:
                return _fail("authoritative deferred result sync must normalize the completed source lane into COMPLETED")
            if child_graph is None or child_graph["status"] != NodeStatus.ACTIVE.value:
                return _fail("authoritative deferred result sync must auto-activate the ready deferred child")
            if "child-followup-001" not in authority_view["active_child_nodes"]:
                return _fail("auto-activated deferred child must appear in active_child_nodes")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("ready deferred continuation must bootstrap and launch the activated child exactly once")
            if len(supervision_calls) != 1:
                return _fail("ready deferred continuation must automatically supervise the activated child")

            accepted_envelopes = json.loads((state_root / "state" / "accepted_envelopes.json").read_text(encoding="utf-8"))
            activate_envelopes = [
                env
                for env in accepted_envelopes
                if ((env.get("payload") or {}).get("topology_mutation") or {}).get("kind") == "activate"
            ]
            if len(activate_envelopes) != 1:
                return _fail("authoritative deferred continuation must submit exactly one accepted activate proposal")
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if original_bootstrap is not None:
                topology_module.bootstrap_first_implementer_node = original_bootstrap
            else:
                delattr(topology_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                topology_module.launch_child_from_result_ref = original_launch
            else:
                delattr(topology_module, "launch_child_from_result_ref")
            if original_supervision is not None:
                topology_module.ensure_child_supervision_running = original_supervision
            else:
                delattr(topology_module, "ensure_child_supervision_running")

    return 0


def _split_continuation_release_activation_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus
    from loop_product.runtime_paths import node_live_artifact_root

    with tempfile.TemporaryDirectory(prefix="loop_system_split_continue_release_activation_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        authority = kernel_internal_authority()
        kernel_state = load_kernel_state(state_root)
        source_record = kernel_state.nodes["child-implementer-001"]
        source_workspace_root = Path(str(source_record.get("workspace_root") or "")).expanduser().resolve()
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-implementer-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# source split release\n", encoding="utf-8")
        (live_root / "partition").mkdir(parents=True, exist_ok=True)
        (live_root / "partition" / "PARTITION_SUMMARY.md").write_text("linear slice released\n", encoding="utf-8")
        publish_workspace_artifact_snapshot(
            node_id="child-implementer-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=source_workspace_root / "deliverables" / "primary_artifact",
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "child-implementer-001"
            / "WorkspaceArtifactPublicationReceipt.json",
        )
        source_record["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "",
            "summary": "source lane stopped after publishing split release artifacts",
            "observation_kind": "authoritative_result",
            "evidence_refs": [],
        }
        persist_node_snapshot(state_root, source_record, authority=authority)
        persist_kernel_state(state_root, kernel_state, authority=authority)
        result_ref = state_root / "artifacts" / "child-implementer-001" / "result.json"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": "child-implementer-001",
                    "status": "IN_PROGRESS",
                    "outcome": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
                    "summary": "source published partition outputs and released the linear slice after deferred split acceptance",
                    "split_state": {
                        "proposed": True,
                        "accepted": True,
                        "mode": "deferred",
                        "accepted_target_nodes": ["child-linear-001"],
                    },
                    "whole_paper_status": {
                        "status": "IN_PROGRESS",
                        "current_phase": "PARTITION_REVIEW",
                        "artifact_ready_for_evaluator": False,
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        child_workspace_root = ROOT / "workspace" / "test-split-continuation-release-child"
        shutil.rmtree(child_workspace_root, ignore_errors=True)
        child_node = materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id="child-implementer-001",
            node_id="child-linear-001",
            goal_slice="formalize the released linear slice after deferred split acceptance",
            round_id="R1.child-linear-001",
            execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "high", "role": "implementer"},
            budget_profile={"max_rounds": 1},
            workspace_root=str(child_workspace_root.resolve()),
            depends_on_node_ids=["child-implementer-001"],
            activation_condition="after:child-implementer-001:split_accepted_and_linear_slice_released",
            result_sink_ref="artifacts/child-linear-001/result.json",
            authority=authority,
        )
        child_record = kernel_state.nodes[child_node.node_id]
        child_record["status"] = NodeStatus.PLANNED.value
        persist_node_snapshot(state_root, child_record, authority=authority)
        persist_kernel_state(state_root, kernel_state, authority=authority)
        _write_source_handoff(
            state_root=state_root,
            workspace_root=Path(child_node.workspace_root),
            source_node=child_node,
        )

        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[dict[str, object]] = []
        supervision_calls: list[dict[str, object]] = []
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)
        original_supervision = getattr(topology_module, "ensure_child_supervision_running", None)

        def _fake_bootstrap(**kwargs):
            bootstrap_calls.append(dict(kwargs))
            bootstrap_ref = state_root / "artifacts" / "bootstrap" / f"{kwargs['node_id']}__bootstrap.json"
            bootstrap_ref.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "bootstrap_result_ref": str(bootstrap_ref.resolve()),
                "node_id": str(kwargs["node_id"]),
                "workspace_root": str(kwargs["workspace_root"]),
                "state_root": str(kwargs["state_root"]),
                "launch_spec": {
                    "argv": ["/bin/echo", "split-release-activate"],
                    "env": {},
                    "cwd": str(kwargs["workspace_root"]),
                    "stdin_path": str((Path(str(kwargs["workspace_root"])) / "CHILD_PROMPT.md").resolve()),
                },
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            bootstrap_ref = Path(result_ref).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            launch_calls.append({"result_ref": str(bootstrap_ref), "launch_result_ref": str(launch_result_ref.resolve())})
            return {"launch_result_ref": str(launch_result_ref.resolve())}

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            authority_view = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority_view["node_graph"]}
            if node_graph.get("child-implementer-001", {}).get("status") != NodeStatus.BLOCKED.value:
                return _fail("split continuation release case must normalize the source into BLOCKED before child activation")
            if node_graph.get("child-linear-001", {}).get("status") != NodeStatus.ACTIVE.value:
                return _fail("split continuation release case must auto-activate the released deferred child")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("released deferred child must bootstrap and launch exactly once after authoritative sync")
            if len(supervision_calls) != 1:
                return _fail("released deferred child must automatically attach committed supervision after launch")
            accepted_envelopes = json.loads((state_root / "state" / "accepted_envelopes.json").read_text(encoding="utf-8"))
            activate_envelopes = [
                env
                for env in accepted_envelopes
                if ((env.get("payload") or {}).get("topology_mutation") or {}).get("kind") == "activate"
            ]
            if len(activate_envelopes) != 1:
                return _fail("split continuation release case must submit exactly one accepted activate proposal")
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if original_bootstrap is not None:
                topology_module.bootstrap_first_implementer_node = original_bootstrap
            else:
                delattr(topology_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                topology_module.launch_child_from_result_ref = original_launch
            else:
                delattr(topology_module, "launch_child_from_result_ref")
            if original_supervision is not None:
                topology_module.ensure_child_supervision_running = original_supervision
            else:
                delattr(topology_module, "ensure_child_supervision_running")
            shutil.rmtree(child_workspace_root, ignore_errors=True)

    return 0


def _default_budget_parallel_split_case() -> int:
    from loop_product.kernel.state import load_kernel_state
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_parallel_budget_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        kernel_state = load_kernel_state(state_root)
        source_node = _base_source_node()
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {"node_id": "child-followup-001", "goal_slice": "formalize block one"},
                {"node_id": "child-followup-002", "goal_slice": "formalize block two"},
                {"node_id": "child-followup-003", "goal_slice": "formalize block three"},
                {"node_id": "child-followup-004", "goal_slice": "formalize block four"},
            ],
            split_mode="parallel",
            completed_work="source node identified four independent proof frontiers",
            remaining_work="run those frontiers in parallel child nodes",
            reason="exercise the default active-node budget on a source-plus-four-child split",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(
                "default complexity budget must allow a source-plus-four-child parallel split; "
                f"got {envelope.status.value!r} with budget={kernel_state.complexity_budget!r}"
            )

    return 0


def _dependency_ready_parallel_planned_activation_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state
    from loop_product.protocols.node import NodeStatus

    with tempfile.TemporaryDirectory(prefix="loop_system_parallel_planned_activation_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        authority = kernel_internal_authority()
        kernel_state = load_kernel_state(state_root)
        cleanup_roots = [
            ROOT / "workspace" / "test-parallel-planned-child-linear-001",
            ROOT / "workspace" / "test-parallel-planned-child-external-001",
            ROOT / "workspace" / "test-parallel-planned-child-final-001",
        ]

        dep_specs = [
            ("child-linear-001", "close the linear chain", "artifacts/child-linear-001/result.json"),
            ("child-external-001", "close the external dependency chain", "artifacts/child-external-001/result.json"),
        ]
        for node_id, goal_slice, result_sink_ref in dep_specs:
            workspace_root = ROOT / "workspace" / f"test-parallel-planned-{node_id}"
            node = materialize_child(
                state_root=state_root,
                kernel_state=kernel_state,
                parent_node_id="child-implementer-001",
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
            _write_source_handoff(state_root=state_root, workspace_root=Path(node.workspace_root), source_node=node)

        planned_workspace_root = ROOT / "workspace" / "test-parallel-planned-child-final-001"
        planned_node = materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id="child-implementer-001",
            node_id="child-final-001",
            goal_slice="integrate dependency-ready child results into one final whole-paper outcome",
            round_id="R1.child-final-001",
            execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "high", "role": "implementer"},
            budget_profile={"max_rounds": 2},
            workspace_root=str(planned_workspace_root.resolve()),
            depends_on_node_ids=["child-linear-001", "child-external-001"],
            result_sink_ref="artifacts/child-final-001/result.json",
            status=NodeStatus.PLANNED,
            authority=authority,
        )
        persist_kernel_state(state_root, kernel_state, authority=authority)
        _write_source_handoff(state_root=state_root, workspace_root=Path(planned_node.workspace_root), source_node=planned_node)

        dep_payloads = {
            "child-linear-001": {
                "status": "BLOCKED",
                "outcome": "PAPER_DEFECT_EXPOSED",
                "summary": "linear chain closed to a terminal paper defect exposure",
            },
            "child-external-001": {
                "status": "COMPLETED",
                "outcome": "FULLY_FAITHFUL_COMPLETE",
                "summary": "external dependency chain completed successfully",
            },
        }
        for node_id, payload in dep_payloads.items():
            result_ref = state_root / "artifacts" / node_id / "result.json"
            result_ref.parent.mkdir(parents=True, exist_ok=True)
            result_ref.write_text(
                json.dumps({"schema": "loop_product.child_result", "node_id": node_id, **payload}, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )

        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[dict[str, object]] = []
        supervision_calls: list[dict[str, object]] = []
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)
        original_supervision = getattr(topology_module, "ensure_child_supervision_running", None)

        def _fake_bootstrap(**kwargs):
            bootstrap_calls.append(dict(kwargs))
            bootstrap_ref = state_root / "artifacts" / "bootstrap" / f"{kwargs['node_id']}__bootstrap.json"
            bootstrap_ref.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "bootstrap_result_ref": str(bootstrap_ref.resolve()),
                "node_id": str(kwargs["node_id"]),
                "workspace_root": str(kwargs["workspace_root"]),
                "state_root": str(kwargs["state_root"]),
                "launch_spec": {
                    "argv": ["/bin/echo", "parallel-planned-activate"],
                    "env": {},
                    "cwd": str(kwargs["workspace_root"]),
                    "stdin_path": str((Path(str(kwargs["workspace_root"])) / "CHILD_PROMPT.md").resolve()),
                },
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            bootstrap_ref = Path(result_ref).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            launch_calls.append({"result_ref": str(bootstrap_ref), "launch_result_ref": str(launch_result_ref.resolve())})
            return {"launch_result_ref": str(launch_result_ref.resolve())}

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            authority_view = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority_view["node_graph"]}
            if node_graph.get("child-final-001", {}).get("status") != NodeStatus.ACTIVE.value:
                return _fail("planned child with terminal-ready dependencies must auto-activate once dependencies close")
            if "child-final-001" not in authority_view["active_child_nodes"]:
                return _fail("dependency-ready planned child must move into active_child_nodes after auto-activation")
            if "child-final-001" in authority_view["planned_child_nodes"]:
                return _fail("dependency-ready planned child must leave planned_child_nodes after auto-activation")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("dependency-ready planned child auto-activation must bootstrap and launch exactly once")
            if len(supervision_calls) != 1:
                return _fail("dependency-ready planned child auto-activation must attach committed supervision exactly once")

            accepted_envelopes = json.loads((state_root / "state" / "accepted_envelopes.json").read_text(encoding="utf-8"))
            activate_envelopes = [
                env
                for env in accepted_envelopes
                if ((env.get("payload") or {}).get("topology_mutation") or {}).get("kind") == "activate"
            ]
            if len(activate_envelopes) != 1:
                return _fail("dependency-ready planned child must submit exactly one accepted activate proposal")
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if original_bootstrap is not None:
                topology_module.bootstrap_first_implementer_node = original_bootstrap
            else:
                delattr(topology_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                topology_module.launch_child_from_result_ref = original_launch
            else:
                delattr(topology_module, "launch_child_from_result_ref")
            if original_supervision is not None:
                topology_module.ensure_child_supervision_running = original_supervision
            else:
                delattr(topology_module, "ensure_child_supervision_running")
            for cleanup_root in cleanup_roots:
                if cleanup_root.exists():
                    shutil.rmtree(cleanup_root, ignore_errors=True)

    return 0


def _rejected_activation_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.activate import build_activate_request

    with tempfile.TemporaryDirectory(prefix="loop_system_deferred_activation_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        _submit_deferred_split(state_root)

        mutation = build_activate_request(
            "child-followup-001",
            reason="attempt activation before dependency completion",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id="R1",
            generation=2,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"activation before dependency completion must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority["node_graph"]}
        child_graph = node_graph.get("child-followup-001")
        if child_graph is None or child_graph["status"] != "PLANNED":
            return _fail("rejected activation must leave the deferred child PLANNED")

    return 0


def main() -> int:
    try:
        rc = _accepted_activation_case()
        if rc:
            return rc
        rc = _authoritative_deferred_result_continuation_case()
        if rc:
            return rc
        rc = _split_continuation_release_activation_case()
        if rc:
            return rc
        rc = _default_budget_parallel_split_case()
        if rc:
            return rc
        rc = _dependency_ready_parallel_planned_activation_case()
        if rc:
            return rc
        rc = _rejected_activation_case()
        if rc:
            return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"deferred activation runtime raised unexpectedly: {exc}")

    print("[loop-system-deferred-activation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
