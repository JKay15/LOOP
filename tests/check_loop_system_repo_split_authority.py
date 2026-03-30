#!/usr/bin/env python3
"""Validate kernel-approved split authority for the standalone LOOP product repo."""

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
    print(f"[loop-system-split-authority][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_source_node():
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="deliver the current bounded endpoint bundle",
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


def _write_minimal_endpoint_artifact(path: Path) -> None:
    payload = {
        "artifact_ref": str(path.resolve()),
        "confirmed_requirements": [],
        "denied_requirements": [],
        "latest_turn_ref": "turn-1",
        "mode": "bypassed",
        "original_user_prompt": "run a real whole-paper faithful formalization benchmark",
        "question_history": [],
        "requirement_artifact": {
            "artifact_ready_for_persistence": True,
            "final_effect": "faithfully formalize the whole paper with no internal gaps",
            "workflow_scope": "whole_paper_formalization",
            "hard_constraints": ["do not distort the paper"],
            "non_goals": ["do not optimize for split count itself"],
            "observable_success_criteria": ["whole-paper faithful terminal result is explicit"],
            "open_questions": [],
            "relevant_context": ["parallel split may be used for independent proof fronts"],
            "sufficient": True,
            "task_type": "research",
            "user_request_summary": "formalize the whole paper faithfully",
        },
        "session_root": "endpoint_session",
        "status": "BYPASSED",
        "turn_count": 1,
        "version": "0.1",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_source_handoff(*, state_root: Path, workspace_root: Path, source_node) -> Path:
    from loop_product.runtime_paths import node_machine_handoff_ref

    endpoint_artifact_ref = workspace_root / "EndpointArtifact.json"
    _write_minimal_endpoint_artifact(endpoint_artifact_ref)
    handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id=source_node.node_id)
    handoff_md_ref = workspace_root / "FROZEN_HANDOFF.md"
    workspace_result_sink = workspace_root / "artifacts" / source_node.node_id / "result.json"
    kernel_result_sink = state_root / "artifacts" / source_node.node_id / "result.json"
    workspace_live_artifact_relpath = ".tmp_primary_artifact"
    workspace_live_artifact_ref = workspace_root / workspace_live_artifact_relpath
    artifact_publication_receipt_ref = (
        state_root / "artifacts" / "publication" / source_node.node_id / "WorkspaceArtifactPublicationReceipt.json"
    )
    artifact_publication_runner_ref = workspace_root / "PUBLISH_WORKSPACE_ARTIFACT.sh"
    payload = {
        "node_id": source_node.node_id,
        "round_id": source_node.round_id,
        "state_root": str(state_root.resolve()),
        "endpoint_artifact_ref": str(endpoint_artifact_ref.resolve()),
        "root_goal": "faithfully formalize the whole paper with no internal gaps",
        "child_goal_slice": source_node.goal_slice,
        "workspace_root": str(workspace_root.resolve()),
        "workspace_mirror_relpath": "deliverables/primary_artifact",
        "workspace_live_artifact_relpath": workspace_live_artifact_relpath,
        "workspace_live_artifact_ref": str(workspace_live_artifact_ref.resolve()),
        "external_publish_target": "",
        "context_refs": [str(endpoint_artifact_ref.resolve())],
        "result_sink_ref": source_node.result_sink_ref,
        "workspace_result_sink_ref": str(workspace_result_sink.resolve()),
        "kernel_result_sink_ref": str(kernel_result_sink.resolve()),
        "artifact_publication_receipt_ref": str(artifact_publication_receipt_ref.resolve()),
        "artifact_publication_runner_ref": str(artifact_publication_runner_ref.resolve()),
        "evaluator_submission_ref": str((state_root / "artifacts" / "bootstrap" / "EvaluatorNodeSubmission.json").resolve()),
        "evaluator_runner_ref": str((workspace_root / "RUN_EVALUATOR_NODE_UNTIL_TERMINAL.sh").resolve()),
    }
    handoff_ref.parent.mkdir(parents=True, exist_ok=True)
    handoff_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    handoff_md_ref.write_text(
        "\n".join(
            [
                "# Frozen Handoff",
                "",
                f"- node_id: `{source_node.node_id}`",
                f"- state_root: `{state_root.resolve()}`",
                f"- endpoint_artifact_ref: `{endpoint_artifact_ref.resolve()}`",
                "",
                "## Frozen Goal",
                "",
                source_node.goal_slice,
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return handoff_ref


def _persist_base_state(state_root: Path, *, max_active_nodes: int | None = None) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    source_workspace_root = state_root.parent / "workspace" / "child-implementer-001"
    source_workspace_root.mkdir(parents=True, exist_ok=True)
    kernel_state = KernelState(
        task_id="wave11-split-authority",
        root_goal="prove node proposes and kernel decides split mutations",
        root_node_id="root-kernel",
    )
    if max_active_nodes is not None:
        kernel_state.complexity_budget["max_active_nodes"] = int(max_active_nodes)
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise split authority",
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
    source_node = _base_source_node()
    source_record = source_node.to_dict()
    source_record["workspace_root"] = str(source_workspace_root.resolve())
    source_record["runtime_state"] = {
        "attachment_state": "ATTACHED",
        "observed_at": "",
        "summary": "test source node already running",
        "observation_kind": "dispatch:started",
        "evidence_refs": [],
    }
    kernel_state.nodes[source_node.node_id] = source_record
    kernel_state.delegation_map[source_node.node_id] = "root-kernel"
    kernel_state.active_requirements.append(source_node.goal_slice)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
    (state_root / "state" / f"{source_node.node_id}.json").write_text(
        json.dumps(source_record, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _write_source_handoff(state_root=state_root, workspace_root=source_workspace_root, source_node=source_node)


def _upgrade_source_to_whole_paper_contract(state_root: Path) -> tuple[list[str], list[dict[str, object]]]:
    from loop_product.control_intent import default_progress_checkpoints, default_startup_required_output_paths
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state

    expected_startup = default_startup_required_output_paths(
        workflow_scope="whole_paper_formalization",
        artifact_scope="task",
        terminal_authority_scope="whole_paper",
    )
    expected_progress = default_progress_checkpoints(
        workflow_scope="whole_paper_formalization",
        artifact_scope="task",
        terminal_authority_scope="whole_paper",
    )
    kernel_state = load_kernel_state(state_root)
    source_record = dict(kernel_state.nodes.get("child-implementer-001") or {})
    source_record["workflow_scope"] = "whole_paper_formalization"
    source_record["artifact_scope"] = "task"
    source_record["terminal_authority_scope"] = "whole_paper"
    source_record["startup_required_output_paths"] = list(expected_startup)
    source_record["progress_checkpoints"] = list(expected_progress)
    kernel_state.nodes["child-implementer-001"] = source_record
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
    (state_root / "state" / "child-implementer-001.json").write_text(
        json.dumps(source_record, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return list(expected_startup), [dict(item) for item in expected_progress]


def _accepted_split_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_split_authority_accept_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
        bootstrap_calls: list[dict[str, object]] = []
        launch_calls: list[dict[str, object]] = []
        supervision_calls: list[dict[str, object]] = []
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)
        original_supervision = getattr(topology_module, "ensure_child_supervision_running", None)
        original_supervision = getattr(topology_module, "ensure_child_supervision_running", None)

        def _fake_bootstrap(**kwargs):
            bootstrap_calls.append(dict(kwargs))
            bootstrap_ref = state_root / "artifacts" / "bootstrap" / f"{kwargs['node_id']}__bootstrap.json"
            bootstrap_ref.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "bootstrap_result_ref": str(bootstrap_ref.resolve()),
                "node_id": kwargs["node_id"],
                "state_root": str(state_root.resolve()),
                "workspace_root": str(kwargs["workspace_root"]),
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch_child_from_result_ref(**kwargs):
            launch_calls.append(dict(kwargs))
            bootstrap_ref = Path(str(kwargs.get("result_ref") or "")).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            return {
                "launch_decision": "test-only",
                "source_result_ref": str(bootstrap_ref),
                "launch_result_ref": str(launch_result_ref.resolve()),
            }

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-review-001",
                    "node_kind": "reviewer",
                    "goal_slice": "audit the risk-heavy branch in bounded scope",
                    "execution_policy": {"provider_selection": "codex_cli", "sandbox_mode": "read-only"},
                },
                {
                    "node_id": "child-repair-001",
                    "goal_slice": "repair the high-risk implementation branch with a higher thinking budget",
                    "execution_policy": {"provider_selection": "codex_cli", "sandbox_mode": "workspace-write"},
                    "reasoning_profile": {"thinking_budget": "high", "role": "implementer"},
                    "budget_profile": {"max_rounds": 3},
                },
            ],
            reason="split validation and repair into separate bounded child nodes",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        try:
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"accepted split path must return ACCEPTED, got {envelope.status.value!r}")

            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            for child_id in ("child-review-001", "child-repair-001"):
                if child_id not in node_graph:
                    return _fail(f"accepted split must materialize child node {child_id}")
                if node_graph[child_id]["parent_node_id"] != source_node.node_id:
                    return _fail(f"split child {child_id} must inherit source parentage")
                if node_graph[child_id]["generation"] != source_node.generation + 1:
                    return _fail(f"split child {child_id} must increment generation")
            if authority["node_lifecycle"].get(source_node.node_id) != NodeStatus.BLOCKED.value:
                return _fail("accepted split must block the source node while child nodes take over")
            if source_node.node_id not in authority["blocked_reasons"]:
                return _fail("accepted split must preserve a blocked reason for the source node")
            if "child-review-001" not in authority["active_child_nodes"] or "child-repair-001" not in authority["active_child_nodes"]:
                return _fail("accepted split must expose active split children in authority view")

            review_node_state = state_root / "state" / "child-review-001.json"
            repair_node_state = state_root / "state" / "child-repair-001.json"
            if not review_node_state.exists() or not repair_node_state.exists():
                return _fail("accepted split must persist split child node snapshots under .loop/state/")
            review_node = json.loads(review_node_state.read_text(encoding="utf-8"))
            repair_node = json.loads(repair_node_state.read_text(encoding="utf-8"))
            if review_node.get("execution_policy", {}).get("agent_provider") != "codex_cli":
                return _fail("accepted split child node state must canonicalize provider_selection to execution_policy.agent_provider")
            if repair_node.get("execution_policy", {}).get("agent_provider") != "codex_cli":
                return _fail("accepted split repair child node state must canonicalize provider_selection to execution_policy.agent_provider")
            if "provider_selection" in review_node.get("execution_policy", {}):
                return _fail("accepted split child node state must not persist legacy provider_selection keys")
            if "provider_selection" in repair_node.get("execution_policy", {}):
                return _fail("accepted split repair child node state must not persist legacy provider_selection keys")
            review_delegation = state_root / "state" / "delegations" / "child-review-001.json"
            repair_delegation = state_root / "state" / "delegations" / "child-repair-001.json"
            if not review_delegation.exists() or not repair_delegation.exists():
                return _fail("accepted split must freeze split child delegations")
            review_delegation_data = json.loads(review_delegation.read_text(encoding="utf-8"))
            repair_delegation_data = json.loads(repair_delegation.read_text(encoding="utf-8"))
            if review_delegation_data.get("execution_policy", {}).get("agent_provider") != "codex_cli":
                return _fail("accepted split child delegation must canonicalize provider_selection to execution_policy.agent_provider")
            if repair_delegation_data.get("execution_policy", {}).get("agent_provider") != "codex_cli":
                return _fail("accepted split repair child delegation must canonicalize provider_selection to execution_policy.agent_provider")
            if "provider_selection" in review_delegation_data.get("execution_policy", {}):
                return _fail("accepted split child delegation must not persist legacy provider_selection keys")
            if "provider_selection" in repair_delegation_data.get("execution_policy", {}):
                return _fail("accepted split repair child delegation must not persist legacy provider_selection keys")
            if len(bootstrap_calls) != 2:
                return _fail(
                    "accepted parallel split must bootstrap each accepted child into a canonical launch bundle before returning"
                )
            if len(launch_calls) != 2:
                return _fail("accepted parallel split must immediately materialize a launch attempt for each accepted child")
            if len(supervision_calls) != 2:
                return _fail("accepted parallel split must automatically attach committed supervision to each launched child")
            bootstrapped_ids = {str(item.get('node_id') or '') for item in bootstrap_calls}
            if bootstrapped_ids != {"child-review-001", "child-repair-001"}:
                return _fail("accepted split must bootstrap the exact accepted child ids")
            if any(str(item.get("mode") or "") != "continue_exact" for item in bootstrap_calls):
                return _fail("accepted split child bootstrap must reuse continue_exact frozen bootstrap semantics")
            if any(str(item.get("endpoint_artifact_ref") or "").strip() == "" for item in bootstrap_calls):
                return _fail("accepted split child bootstrap must reuse source frozen endpoint context")
            source_workspace_root = state_root.parent / "workspace" / "child-implementer-001"
            expected_inherited_refs = {
                str((source_workspace_root / "FROZEN_HANDOFF.md").resolve()),
            }
            for call in bootstrap_calls:
                context_refs = {str(item or "").strip() for item in list(call.get("context_refs") or []) if str(item or "").strip()}
                missing_refs = expected_inherited_refs - context_refs
                if missing_refs:
                    return _fail(
                        "accepted split child bootstrap must preserve the parent frozen handoff as inherited authoritative context, "
                        f"but missed {sorted(missing_refs)}"
                    )

            audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
            if not any("split" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
                return _fail("accepted split must appear in recent structural decisions")
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


def _rejected_split_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_split_authority_reject_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root, max_active_nodes=3)
        source_node = _base_source_node()
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {"node_id": "child-a", "goal_slice": "branch a"},
                {"node_id": "child-b", "goal_slice": "branch b"},
                {"node_id": "child-c", "goal_slice": "branch c"},
            ],
            reason="attempt an over-budget split that should be rejected",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"over-budget split must be rejected, got {envelope.status.value!r}")
        authority = query_authority_view(state_root)
        if any(node_id in authority["node_lifecycle"] for node_id in ("child-a", "child-b", "child-c")):
            return _fail("rejected split must not materialize child nodes")
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("rejected split must leave the source node active")

    return 0


def _parallel_split_with_dependency_bound_child_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_parallel_split_dependency_gate_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
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
                "node_id": kwargs["node_id"],
                "state_root": str(state_root.resolve()),
                "workspace_root": str(kwargs["workspace_root"]),
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch_child_from_result_ref(**kwargs):
            launch_calls.append(dict(kwargs))
            bootstrap_ref = Path(str(kwargs.get("result_ref") or "")).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            return {
                "launch_decision": "test-only",
                "source_result_ref": str(bootstrap_ref),
                "launch_result_ref": str(launch_result_ref.resolve()),
            }

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        try:
            try:
                build_split_request(
                    source_node=source_node,
                    target_nodes=[
                        {
                            "node_id": "child-extraction-001",
                            "goal_slice": "close the whole-paper extraction/dependency ledger",
                            "activation_condition": "May start immediately from the shared extraction ledger and dependency graph.",
                        },
                        {
                            "node_id": "child-linear-001",
                            "goal_slice": "formalize the linear chain independently",
                            "activation_condition": "May start immediately from the shared extraction ledger and dependency graph.",
                        },
                    ],
                    split_mode="parallel",
                    reason="prose-only activation conditions without machine gating must fail closed",
                )
            except ValueError:
                pass
            else:
                return _fail("build_split_request must reject prose-only activation conditions with no safe machine fallback")

            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            if node_graph.get("child-extraction-001") or node_graph.get("child-linear-001"):
                return _fail("rejected split must not materialize any child nodes")
            if bootstrap_calls or launch_calls:
                return _fail("rejected split must not bootstrap or launch child nodes")
        finally:
            if original_bootstrap is not None:
                topology_module.bootstrap_first_implementer_node = original_bootstrap
            else:
                delattr(topology_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                topology_module.launch_child_from_result_ref = original_launch
            else:
                delattr(topology_module, "launch_child_from_result_ref")

    return 0


def _parallel_split_dependency_gated_prose_rationale_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_parallel_split_prose_rationale_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
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
                "node_id": kwargs["node_id"],
                "state_root": str(state_root.resolve()),
                "workspace_root": str(kwargs["workspace_root"]),
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch_child_from_result_ref(**kwargs):
            launch_calls.append(dict(kwargs))
            bootstrap_ref = Path(str(kwargs.get("result_ref") or "")).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            return {
                "launch_decision": "test-only",
                "source_result_ref": str(bootstrap_ref),
                "launch_result_ref": str(launch_result_ref.resolve()),
            }

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-extraction-001",
                    "goal_slice": "close the whole-paper extraction/dependency ledger",
                },
                {
                    "node_id": "child-final-001",
                    "goal_slice": "integrate only after extraction finishes",
                    "depends_on_node_ids": ["child-extraction-001"],
                    "activation_condition": "Activate once the extraction ledger is durably published and reviewed.",
                },
            ],
            split_mode="parallel",
            reason="dependency-gated prose should be preserved as rationale while the machine gate comes from depends_on",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        try:
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(
                    "dependency-gated parallel split with prose activation rationale should be accepted via safe machine fallback, "
                    f"got {envelope.status.value!r}"
                )

            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            if node_graph.get("child-extraction-001", {}).get("status") != NodeStatus.ACTIVE.value:
                return _fail("dependency-free child should still launch immediately")
            if node_graph.get("child-final-001", {}).get("status") != NodeStatus.PLANNED.value:
                return _fail("dependency-gated child should remain PLANNED until activation")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("only the dependency-free child should bootstrap and launch immediately in this case")
            if len(supervision_calls) != 1:
                return _fail("only the immediately launched dependency-free child should get committed supervision in this case")

            final_state = json.loads((state_root / "state" / "child-final-001.json").read_text(encoding="utf-8"))
            if final_state.get("depends_on_node_ids") != ["child-extraction-001"]:
                return _fail("dependency-gated child must preserve depends_on_node_ids in node state")
            if final_state.get("activation_condition") != "":
                return _fail("unsupported prose activation_condition must not persist as a machine gate")
            if "durably published and reviewed" not in str(final_state.get("activation_rationale") or ""):
                return _fail("dependency-gated child must preserve prose gating explanation as activation_rationale")
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


def _split_required_outputs_persist_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_split_required_outputs_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_node = _base_source_node()
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
                "node_id": kwargs["node_id"],
                "state_root": str(state_root.resolve()),
                "workspace_root": str(kwargs["workspace_root"]),
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch_child_from_result_ref(**kwargs):
            launch_calls.append(dict(kwargs))
            bootstrap_ref = Path(str(kwargs.get("result_ref") or "")).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            return {
                "launch_decision": "test-only",
                "source_result_ref": str(bootstrap_ref),
                "launch_result_ref": str(launch_result_ref.resolve()),
            }

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        required_output_paths = [
            "formalization/appendix_claim_inventory.json",
            "formalization/appendix_claims.lean",
            "PARTITION/external_dependency_candidates.json",
            "WHOLE_PAPER_STATUS.json",
        ]
        startup_required_output_paths = [
            "README.md",
            "formalization/appendix_claim_inventory.json",
        ]
        progress_checkpoints = [
            {
                "checkpoint_id": "appendix_execution_decision",
                "description": "record the appendix execution decision before extended helper archaeology",
                "required_any_of": ["analysis/EXECUTION_DECISION.json"],
                "window_s": 300.0,
            }
        ]
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-appendix-001",
                    "goal_slice": "formalize appendix support claims and close named external dependencies",
                    "required_output_paths": required_output_paths,
                    "startup_required_output_paths": startup_required_output_paths,
                    "progress_checkpoints": progress_checkpoints,
                },
                {
                    "node_id": "child-followup-001",
                    "goal_slice": "continue after appendix block is complete",
                    "depends_on_node_ids": ["child-appendix-001"],
                },
            ],
            split_mode="parallel",
            reason="preserve required outputs from split proposal into durable child state",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        try:
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"split required-output persistence case must be accepted, got {envelope.status.value!r}")
            authority = query_authority_view(state_root)
            appendix_state_path = state_root / "state" / "child-appendix-001.json"
            appendix_delegation_path = state_root / "state" / "delegations" / "child-appendix-001.json"
            if not appendix_state_path.exists() or not appendix_delegation_path.exists():
                return _fail("accepted split must persist child node/delegation snapshots before checking required outputs")
            appendix_state = json.loads(appendix_state_path.read_text(encoding="utf-8"))
            appendix_delegation = json.loads(appendix_delegation_path.read_text(encoding="utf-8"))
            if appendix_state.get("required_output_paths") != required_output_paths:
                return _fail("accepted split child node state must preserve required_output_paths from the reviewed target payload")
            if appendix_state.get("startup_required_output_paths") != startup_required_output_paths:
                return _fail(
                    "accepted split child node state must preserve startup_required_output_paths from the reviewed target payload"
                )
            if appendix_state.get("progress_checkpoints") != progress_checkpoints:
                return _fail("accepted split child node state must preserve progress_checkpoints from the reviewed target payload")
            if appendix_delegation.get("required_output_paths") != required_output_paths:
                return _fail("accepted split child delegation must preserve required_output_paths from the reviewed target payload")
            if appendix_delegation.get("startup_required_output_paths") != startup_required_output_paths:
                return _fail(
                    "accepted split child delegation must preserve startup_required_output_paths from the reviewed target payload"
                )
            if appendix_delegation.get("progress_checkpoints") != progress_checkpoints:
                return _fail("accepted split child delegation must preserve progress_checkpoints from the reviewed target payload")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("only the dependency-free required-output child should bootstrap immediately in this case")
            if len(supervision_calls) != 1:
                return _fail("required-output split must automatically supervise each immediately launched child")
            if bootstrap_calls[0].get("required_output_paths") != required_output_paths:
                return _fail("split child bootstrap request must surface required_output_paths for frozen handoff generation")
            if bootstrap_calls[0].get("startup_required_output_paths") != startup_required_output_paths:
                return _fail("split child bootstrap request must surface startup_required_output_paths for frozen handoff generation")
            if bootstrap_calls[0].get("progress_checkpoints") != progress_checkpoints:
                return _fail("split child bootstrap request must surface progress_checkpoints for frozen handoff generation")
            bootstrap_workspace_root = Path(str(bootstrap_calls[0].get("workspace_root") or "")).expanduser().resolve()
            bootstrap_live_relpath = str(bootstrap_calls[0].get("workspace_live_artifact_relpath") or "")
            bootstrap_live_root = (bootstrap_workspace_root / bootstrap_live_relpath).resolve()
            from loop_product.runtime_paths import node_live_artifact_root, shared_cache_helper_ref

            expected_live_root = node_live_artifact_root(
                state_root=state_root,
                node_id="child-appendix-001",
                workspace_mirror_relpath="deliverables/primary_artifact",
            )
            if bootstrap_live_root != expected_live_root:
                return _fail("split child bootstrap request must move live artifact root into runtime-owned scratch outside the workspace")
            if expected_live_root.is_relative_to(bootstrap_workspace_root):
                return _fail("split child live artifact root must not remain under the child workspace root")
            expected_helper = shared_cache_helper_ref()
            context_refs = {str(item) for item in list(bootstrap_calls[0].get("context_refs") or [])}
            if str(expected_helper) not in context_refs:
                return _fail("split child bootstrap request must inject the committed shared-cache helper into frozen context refs")
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            if node_graph.get("child-followup-001", {}).get("status") != "PLANNED":
                return _fail("dependency-bound follower child must remain PLANNED in required-output persistence case")
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


def _whole_paper_split_child_contract_inheritance_case() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.control_intent import default_progress_checkpoints, default_startup_required_output_paths
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.topology.split_review import build_split_request

    with tempfile.TemporaryDirectory(prefix="loop_system_split_whole_paper_child_contract_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        source_startup, source_progress = _upgrade_source_to_whole_paper_contract(state_root)
        expected_slice_startup = default_startup_required_output_paths(
            workflow_scope="whole_paper_formalization",
            artifact_scope="slice",
            terminal_authority_scope="local",
        )
        expected_slice_progress = default_progress_checkpoints(
            workflow_scope="whole_paper_formalization",
            artifact_scope="slice",
            terminal_authority_scope="local",
        )
        source_node = _base_source_node()
        source_node.workflow_scope = "whole_paper_formalization"
        source_node.artifact_scope = "task"
        source_node.terminal_authority_scope = "whole_paper"
        source_node.startup_required_output_paths = list(source_startup)
        source_node.progress_checkpoints = [dict(item) for item in source_progress]
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
                "node_id": kwargs["node_id"],
                "state_root": str(state_root.resolve()),
                "workspace_root": str(kwargs["workspace_root"]),
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch_child_from_result_ref(**kwargs):
            launch_calls.append(dict(kwargs))
            bootstrap_ref = Path(str(kwargs.get("result_ref") or "")).resolve()
            launch_result_ref = bootstrap_ref.parent / "ChildLaunchResult.json"
            return {
                "launch_decision": "test-only",
                "source_result_ref": str(bootstrap_ref),
                "launch_result_ref": str(launch_result_ref.resolve()),
            }

        def _fake_ensure_child_supervision_running(**kwargs):
            supervision_calls.append(dict(kwargs))
            return {
                "launch_result_ref": str(Path(str(kwargs.get("launch_result_ref") or "")).resolve()),
                "status": "test-only",
            }

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        topology_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-whole-paper-slice-001",
                    "goal_slice": "formalize one whole-paper slice without allowing workflow downgrades",
                    "workflow_scope": "generic",
                    "artifact_scope": "slice",
                    "terminal_authority_scope": "local",
                    "startup_required_output_paths": [],
                    "progress_checkpoints": [],
                },
                {
                    "node_id": "child-followup-001",
                    "goal_slice": "follow after the first slice reaches a durable checkpoint",
                    "depends_on_node_ids": ["child-whole-paper-slice-001"],
                },
            ],
            split_mode="parallel",
            reason="whole-paper split child contract must inherit deterministic slice startup and progress defaults",
        )
        try:
            envelope = submit_topology_mutation(
                state_root,
                mutation,
                round_id=source_node.round_id,
                generation=source_node.generation,
            )
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail("whole-paper split child contract inheritance case must be accepted")
            child_state = json.loads((state_root / "state" / "child-whole-paper-slice-001.json").read_text(encoding="utf-8"))
            child_delegation = json.loads(
                (state_root / "state" / "delegations" / "child-whole-paper-slice-001.json").read_text(encoding="utf-8")
            )
            if child_state.get("workflow_scope") != "whole_paper_formalization":
                return _fail("whole-paper split child node state must inherit whole-paper workflow_scope instead of accepting generic downgrade")
            if child_delegation.get("workflow_scope") != "whole_paper_formalization":
                return _fail("whole-paper split child delegation must inherit whole-paper workflow_scope instead of accepting generic downgrade")
            if child_state.get("startup_required_output_paths") != expected_slice_startup:
                return _fail("whole-paper split child node state must restore deterministic slice startup_required_output_paths when payload leaves them empty")
            if child_delegation.get("startup_required_output_paths") != expected_slice_startup:
                return _fail("whole-paper split child delegation must restore deterministic slice startup_required_output_paths when payload leaves them empty")
            if child_state.get("progress_checkpoints") != expected_slice_progress:
                return _fail("whole-paper split child node state must restore deterministic slice progress_checkpoints when payload leaves them empty")
            if child_delegation.get("progress_checkpoints") != expected_slice_progress:
                return _fail("whole-paper split child delegation must restore deterministic slice progress_checkpoints when payload leaves them empty")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1 or len(supervision_calls) != 1:
                return _fail("whole-paper split child inheritance case must immediately bootstrap exactly one dependency-free child")
            if bootstrap_calls[0].get("workflow_scope") != "whole_paper_formalization":
                return _fail("whole-paper split child bootstrap request must keep the inherited whole-paper workflow_scope")
            if bootstrap_calls[0].get("startup_required_output_paths") != expected_slice_startup:
                return _fail("whole-paper split child bootstrap request must surface default slice startup outputs when payload provides an empty list")
            if bootstrap_calls[0].get("progress_checkpoints") != expected_slice_progress:
                return _fail("whole-paper split child bootstrap request must surface default slice checkpoints when payload provides an empty list")
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


def _authoritative_child_result_sync_case() -> int:
    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.dispatch import launch_runtime
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state
    from loop_product.protocols.node import NodeStatus
    from loop_product.runtime_paths import node_live_artifact_root

    with tempfile.TemporaryDirectory(prefix="loop_system_split_authority_result_sync_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        authority = kernel_internal_authority()
        kernel_state = load_kernel_state(state_root)
        child_workspace_root = ROOT / "workspace" / "test-child-blocked-result-sync"
        shutil.rmtree(child_workspace_root, ignore_errors=True)
        materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id="child-implementer-001",
            node_id="child-blocked-001",
            goal_slice="formalize the blocked dependent theorem segment",
            round_id="R1.child-blocked-001",
            execution_policy={"sandbox_mode": "workspace-write", "agent_provider": "codex_cli"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 1},
            node_kind="implementer",
            generation=2,
            allowed_actions=["implement", "report"],
            workspace_root=str(child_workspace_root.resolve()),
            codex_home="",
            depends_on_node_ids=[],
            activation_condition="",
            result_sink_ref="artifacts/child-blocked-001/result.json",
            lineage_ref="root-kernel->child-implementer-001->child-blocked-001",
            status=NodeStatus.ACTIVE,
            authority=authority,
        )
        result_ref = state_root / "artifacts" / "child-blocked-001" / "result.json"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": "child-blocked-001",
                    "status": "BLOCKED",
                    "outcome": "BLOCKED",
                    "summary": "authoritative child result recorded a blocked theorem lane",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-blocked-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# blocked child\n", encoding="utf-8")
        publish_workspace_artifact_snapshot(
            node_id="child-blocked-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=child_workspace_root / "deliverables" / "primary_artifact",
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "child-blocked-001"
            / "WorkspaceArtifactPublicationReceipt.json",
        )
        launch_result_ref = state_root / "artifacts" / "launches" / "child-blocked-001" / "attempt_001" / "ChildLaunchResult.json"
        launch_log_dir = launch_result_ref.parent / "logs"
        launch_log_dir.mkdir(parents=True, exist_ok=True)
        stdout_ref = launch_log_dir / "child-blocked-001.stdout.txt"
        stderr_ref = launch_log_dir / "child-blocked-001.stderr.txt"
        stdin_ref = child_workspace_root / "CHILD_PROMPT.md"
        stdout_ref.write_text("", encoding="utf-8")
        stderr_ref.write_text("", encoding="utf-8")
        stdin_ref.write_text("blocked child prompt\n", encoding="utf-8")
        launch_request_ref = launch_result_ref.parent / "ChildLaunchRequest.json"
        launch_request_ref.write_text(
            json.dumps({"node_id": "child-blocked-001"}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_decision": "started",
                    "source_result_ref": str(result_ref.resolve()),
                    "node_id": "child-blocked-001",
                    "workspace_root": str(child_workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "startup_health_timeout_ms": 250,
                    "startup_retry_limit": 0,
                    "attempt_count": 1,
                    "retryable_failure_kind": "",
                    "attempts": [
                        {
                            "attempt_index": 1,
                            "launch_decision": "started",
                            "launch_request_ref": str(launch_request_ref.resolve()),
                            "launch_log_dir": str(launch_log_dir.resolve()),
                            "stdout_ref": str(stdout_ref.resolve()),
                            "stderr_ref": str(stderr_ref.resolve()),
                            "stdin_ref": str(stdin_ref.resolve()),
                            "wrapped_argv": ["sleep", "1"],
                            "pid": 999999,
                            "exit_code": 0,
                            "retryable_failure_kind": "",
                        }
                    ],
                    "launch_request_ref": str(launch_request_ref.resolve()),
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "launch_log_dir": str(launch_log_dir.resolve()),
                    "stdout_ref": str(stdout_ref.resolve()),
                    "stderr_ref": str(stderr_ref.resolve()),
                    "stdin_ref": str(stdin_ref.resolve()),
                    "wrapped_argv": ["sleep", "1"],
                    "pid": 999999,
                    "exit_code": 0,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        status_payload = launch_runtime.child_runtime_status_from_launch_result_ref(
            result_ref=str(launch_result_ref.resolve()),
            stall_threshold_s=0,
        )
        if str(status_payload.get("lifecycle_status") or "") != NodeStatus.BLOCKED.value:
            return _fail("runtime status helper must absorb authoritative child blocked result before reporting lifecycle")

        authority_view = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority_view["node_graph"]}
        blocked_graph = node_graph.get("child-blocked-001")
        if blocked_graph is None or blocked_graph["status"] != NodeStatus.BLOCKED.value:
            return _fail("authoritative child result sync must expose BLOCKED child lifecycle in authority view")
        child_state = json.loads((state_root / "state" / "child-blocked-001.json").read_text(encoding="utf-8"))
        if child_state.get("status") != NodeStatus.BLOCKED.value:
            return _fail("authoritative child result sync must persist BLOCKED child lifecycle to node state")
        runtime_state = dict(child_state.get("runtime_state") or {})
        if runtime_state.get("attachment_state") != "TERMINAL":
            return _fail("authoritative child result sync must mark the stopped child runtime as TERMINAL")
        shutil.rmtree(child_workspace_root, ignore_errors=True)

        gated_workspace_root = ROOT / "workspace" / "test-child-publication-gate-sync"
        shutil.rmtree(gated_workspace_root, ignore_errors=True)
        materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id="child-implementer-001",
            node_id="child-publication-gated-001",
            goal_slice="formalize the gated theorem segment only after published mirror exists",
            round_id="R1.child-publication-gated-001",
            execution_policy={"sandbox_mode": "workspace-write", "agent_provider": "codex_cli"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["implement", "report"],
            workspace_root=str(gated_workspace_root.resolve()),
            codex_home="",
            depends_on_node_ids=[],
            activation_condition="",
            result_sink_ref="artifacts/child-publication-gated-001/result.json",
            lineage_ref="root-kernel->child-implementer-001->child-publication-gated-001",
            authority=authority,
        )
        gated_live_root = gated_workspace_root / ".tmp_primary_artifact"
        gated_publish_root = gated_workspace_root / "deliverables" / "primary_artifact"
        gated_receipt_ref = state_root / "artifacts" / "publication" / "child-publication-gated-001" / "WorkspaceArtifactPublicationReceipt.json"
        gated_live_root.mkdir(parents=True, exist_ok=True)
        (gated_live_root / "README.md").write_text("# child\n", encoding="utf-8")
        (gated_workspace_root / "FROZEN_HANDOFF.json").write_text(
            json.dumps(
                {
                    "node_id": "child-publication-gated-001",
                    "workspace_live_artifact_ref": str(gated_live_root.resolve()),
                    "workspace_mirror_ref": str(gated_publish_root.resolve()),
                    "artifact_publication_receipt_ref": str(gated_receipt_ref.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        gated_result_ref = state_root / "artifacts" / "child-publication-gated-001" / "result.json"
        gated_result_ref.parent.mkdir(parents=True, exist_ok=True)
        gated_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": "child-publication-gated-001",
                    "status": "BLOCKED",
                    "outcome": "BLOCKED",
                    "summary": "branch is ready to block only after the published mirror exists",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        gated_authority_view = query_authority_view(state_root)
        gated_graph = {item["node_id"]: item for item in gated_authority_view["node_graph"]}
        if gated_graph.get("child-publication-gated-001", {}).get("status") != NodeStatus.ACTIVE.value:
            return _fail("authoritative child result sync must not terminalize a workspace-local child before publication receipt exists")

        publish_workspace_artifact_snapshot(
            node_id="child-publication-gated-001",
            live_artifact_ref=gated_live_root,
            publish_artifact_ref=gated_publish_root,
            publication_receipt_ref=gated_receipt_ref,
        )
        gated_authority_view = query_authority_view(state_root)
        gated_graph = {item["node_id"]: item for item in gated_authority_view["node_graph"]}
        if gated_graph.get("child-publication-gated-001", {}).get("status") != NodeStatus.BLOCKED.value:
            return _fail("authoritative child result sync must terminalize the child once a matching publication receipt exists")
        shutil.rmtree(gated_workspace_root, ignore_errors=True)

        external_live_workspace_root = ROOT / "workspace" / "test-child-external-live-gate-sync"
        shutil.rmtree(external_live_workspace_root, ignore_errors=True)
        materialize_child(
            state_root=state_root,
            kernel_state=load_kernel_state(state_root),
            parent_node_id="child-implementer-001",
            node_id="child-external-live-gated-001",
            goal_slice="formalize a branch only after external live-root hygiene stays clean",
            round_id="R1.child-external-live-gated-001",
            execution_policy={"sandbox_mode": "workspace-write", "agent_provider": "codex_cli"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["implement", "report"],
            workspace_root=str(external_live_workspace_root.resolve()),
            codex_home="",
            depends_on_node_ids=[],
            activation_condition="",
            result_sink_ref="artifacts/child-external-live-gated-001/result.json",
            lineage_ref="root-kernel->child-implementer-001->child-external-live-gated-001",
            authority=authority,
        )
        external_live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-external-live-gated-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        external_publish_root = external_live_workspace_root / "deliverables" / "primary_artifact"
        external_receipt_ref = state_root / "artifacts" / "publication" / "child-external-live-gated-001" / "WorkspaceArtifactPublicationReceipt.json"
        external_live_root.mkdir(parents=True, exist_ok=True)
        (external_live_root / "README.md").write_text("# clean external live root\n", encoding="utf-8")
        (external_live_workspace_root / ".tmp_primary_artifact" / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
        (external_live_workspace_root / ".tmp_primary_artifact" / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text(
            "bad-local-cache\n",
            encoding="utf-8",
        )
        (external_live_workspace_root / "FROZEN_HANDOFF.json").write_text(
            json.dumps(
                {
                    "node_id": "child-external-live-gated-001",
                    "workspace_live_artifact_ref": str(external_live_root.resolve()),
                    "workspace_mirror_ref": str(external_publish_root.resolve()),
                    "artifact_publication_receipt_ref": str(external_receipt_ref.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        external_result_ref = state_root / "artifacts" / "child-external-live-gated-001" / "result.json"
        external_result_ref.parent.mkdir(parents=True, exist_ok=True)
        external_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": "child-external-live-gated-001",
                    "status": "BLOCKED",
                    "outcome": "BLOCKED",
                    "summary": "branch is blocked until external-live-root hygiene is restored",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        publish_workspace_artifact_snapshot(
            node_id="child-external-live-gated-001",
            live_artifact_ref=external_live_root,
            publish_artifact_ref=external_publish_root,
            publication_receipt_ref=external_receipt_ref,
        )
        external_authority_view = query_authority_view(state_root)
        external_graph = {item["node_id"]: item for item in external_authority_view["node_graph"]}
        if external_graph.get("child-external-live-gated-001", {}).get("status") != NodeStatus.ACTIVE.value:
            return _fail("authoritative child result sync must refuse terminalization while local workspace heavy trees remain under external-live-root mode")
        shutil.rmtree(external_live_workspace_root / ".tmp_primary_artifact", ignore_errors=True)
        external_authority_view = query_authority_view(state_root)
        external_graph = {item["node_id"]: item for item in external_authority_view["node_graph"]}
        if external_graph.get("child-external-live-gated-001", {}).get("status") != NodeStatus.BLOCKED.value:
            return _fail("authoritative child result sync must terminalize once the external-live-root workspace pollution is cleared")
        shutil.rmtree(external_live_workspace_root, ignore_errors=True)

    return 0


def _publication_rejects_materialized_live_runtime_heavy_trees_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.runtime_paths import node_live_artifact_root

    with tempfile.TemporaryDirectory(prefix="loop_system_live_root_publication_gate_") as td:
        state_root = Path(td) / ".loop"
        workspace_root = Path(td) / "workspace" / "live-root-publication-gate"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-live-root-publication-gate-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        publication_receipt_ref = (
            state_root
            / "artifacts"
            / "publication"
            / "child-live-root-publication-gate-001"
            / "WorkspaceArtifactPublicationReceipt.json"
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# live-root publication gate\n", encoding="utf-8")
        heavy_root = live_root / ".lake" / "packages" / "mathlib" / ".git"
        heavy_root.mkdir(parents=True, exist_ok=True)
        (heavy_root / "config").write_text("[core]\n\trepositoryformatversion = 0\n", encoding="utf-8")
        try:
            publish_workspace_artifact_snapshot(
                node_id="child-live-root-publication-gate-001",
                live_artifact_ref=live_root,
                publish_artifact_ref=publish_root,
                publication_receipt_ref=publication_receipt_ref,
            )
        except ValueError as exc:
            message = str(exc)
            if "materialized runtime-owned heavy trees" not in message:
                return _fail("publication preflight must explain that live-root runtime heavy trees are fail-closed")
            if ".lake" not in message:
                return _fail("publication preflight must report the offending live-root heavy tree path")
        else:
            return _fail("publication preflight must reject live roots containing materialized runtime heavy trees")
        if publish_root.exists():
            return _fail("publication preflight rejection must not materialize a publish root")
        if publication_receipt_ref.exists():
            return _fail("publication preflight rejection must not write a publication receipt")

    return 0


def _source_split_continuation_sync_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state
    from loop_product.protocols.node import NodeStatus
    from loop_product.runtime_paths import node_live_artifact_root

    with tempfile.TemporaryDirectory(prefix="loop_system_split_continue_sync_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        authority = kernel_internal_authority()
        kernel_state = load_kernel_state(state_root)
        source_record = dict(kernel_state.nodes.get("child-implementer-001") or {})
        source_workspace_root = Path(str(source_record.get("workspace_root") or "")).expanduser().resolve()
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-implementer-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# split continuation\n", encoding="utf-8")
        (live_root / "partition").mkdir(parents=True, exist_ok=True)
        (live_root / "partition" / "PARTITION_SUMMARY.md").write_text("slice release summary\n", encoding="utf-8")
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
                    "summary": "authoritative source result published partition output and accepted deferred split",
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
        authority_view = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority_view["node_graph"]}
        source_graph = node_graph.get("child-implementer-001")
        if source_graph is None:
            return _fail("split continuation sync case must preserve the source node in authority view")
        if source_graph.get("status") != NodeStatus.BLOCKED.value:
            return _fail("source split-continuation result must normalize the source node to BLOCKED instead of leaving it ACTIVE")
        source_state = json.loads((state_root / "state" / "child-implementer-001.json").read_text(encoding="utf-8"))
        if source_state.get("status") != NodeStatus.BLOCKED.value:
            return _fail("source split-continuation result must persist BLOCKED lifecycle to node state")
        runtime_state = dict(source_state.get("runtime_state") or {})
        if runtime_state.get("attachment_state") != "TERMINAL":
            return _fail("source split-continuation result must mark the source runtime attachment as TERMINAL")

    return 0


def _source_split_continuation_publication_surface_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.runtime_paths import node_live_artifact_root

    td = Path(tempfile.mkdtemp(prefix="loop_system_split_continue_publication_surface_"))
    try:
        state_root = td / ".loop"
        _persist_base_state(state_root)
        source_workspace_root = state_root.parent / "workspace" / "child-implementer-001"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-implementer-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# Whole-Paper Slice Startup Bundle\n", encoding="utf-8")
        (live_root / "TRACEABILITY.md").write_text("# Slice Startup Traceability\n", encoding="utf-8")
        (live_root / "SLICE_STATUS.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "goal_slice": "release remaining work into accepted child lanes",
                    "node_id": "child-implementer-001",
                    "round_id": "R1",
                    "slice_startup_batch_materialized": True,
                    "source_tex_ref": "/tmp/source.tex",
                    "status": "IN_PROGRESS",
                    "terminal_authority_scope": "local",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "analysis").mkdir(parents=True, exist_ok=True)
        (live_root / "analysis" / "SLICE_BOUNDARY.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "goal_slice": "release remaining work into accepted child lanes",
                    "node_id": "child-implementer-001",
                    "notes": [
                        "This deterministic startup bundle is slice-scoped.",
                        "The child still owes substantive formalization or dependency-closure progress after startup.",
                    ],
                    "source_tex_ref": "/tmp/source.tex",
                    "terminal_authority_scope": "local",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "accepted_child_node_ids": ["child-linear-001", "child-linear-002"],
                    "artifact_scope": "slice",
                    "goal_slice": "release remaining work into accepted child lanes",
                    "node_id": "child-implementer-001",
                    "released_remaining_work": [
                        {
                            "goal_slice": "probability lane",
                            "node_id": "child-linear-001",
                        },
                        {
                            "goal_slice": "faithfulness lane",
                            "node_id": "child-linear-002",
                        },
                    ],
                    "round_id": "R1",
                    "source_tex_ref": "/tmp/source.tex",
                    "status": "TERMINAL",
                    "summary": "Local deterministic core settled; remaining work released into accepted child lanes.",
                    "terminal_authority_scope": "local",
                    "terminal_classification": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
                    "whole_paper_terminal_authority": False,
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        publish_root = source_workspace_root / "deliverables" / "primary_artifact"
        publish_workspace_artifact_snapshot(
            node_id="child-implementer-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "child-implementer-001"
            / "WorkspaceArtifactPublicationReceipt.json",
        )
        published_readme = (publish_root / "README.md").read_text(encoding="utf-8").lower()
        if "split-continuation" not in published_readme or "accepted child lanes" not in published_readme:
            return _fail("split continuation publication must harmonize README.md to terminal-local split-continuation truth")
        published_traceability = (publish_root / "TRACEABILITY.md").read_text(encoding="utf-8").lower()
        if "split-continuation" not in published_traceability or "child-linear-001" not in published_traceability:
            return _fail("split continuation publication must harmonize TRACEABILITY.md with released child lane evidence")
        published_slice_status = json.loads((publish_root / "SLICE_STATUS.json").read_text(encoding="utf-8"))
        if str(published_slice_status.get("status") or "") != "TERMINAL":
            return _fail("split continuation publication must harmonize SLICE_STATUS.json to terminal state")
        if str(published_slice_status.get("slice_terminal_classification") or "") != "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
            return _fail("split continuation publication must persist slice_terminal_classification in SLICE_STATUS.json")
        published_boundary = json.loads((publish_root / "analysis" / "SLICE_BOUNDARY.json").read_text(encoding="utf-8"))
        notes = [str(item) for item in list(published_boundary.get("notes") or [])]
        if not any("split continuation" in item.lower() for item in notes):
            return _fail("split continuation publication must rewrite SLICE_BOUNDARY notes away from startup-bundle wording")
        published_whole_paper_status = json.loads((publish_root / "WHOLE_PAPER_STATUS.json").read_text(encoding="utf-8"))
        if str(published_whole_paper_status.get("terminal_classification") or "") != "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
            return _fail("split continuation publication must preserve the local whole-paper terminal classification evidence")
    finally:
        shutil.rmtree(td, ignore_errors=True)

    return 0


def _source_split_continuation_result_sink_repair_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.runtime.lifecycle import backfill_authoritative_terminal_results
    from loop_product.runtime_paths import node_live_artifact_root

    td = Path(tempfile.mkdtemp(prefix="loop_system_split_continue_result_sink_repair_"))
    try:
        state_root = td / ".loop"
        _persist_base_state(state_root)
        source_workspace_root = state_root.parent / "workspace" / "child-implementer-001"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-implementer-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# Whole-Paper Slice Startup Bundle\n", encoding="utf-8")
        (live_root / "TRACEABILITY.md").write_text("# Slice Startup Traceability\n", encoding="utf-8")
        (live_root / "SLICE_STATUS.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "goal_slice": "release remaining work into accepted child lanes",
                    "node_id": "child-implementer-001",
                    "round_id": "R1",
                    "slice_startup_batch_materialized": True,
                    "source_tex_ref": "/tmp/source.tex",
                    "status": "IN_PROGRESS",
                    "terminal_authority_scope": "local",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "analysis").mkdir(parents=True, exist_ok=True)
        (live_root / "analysis" / "SLICE_BOUNDARY.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "goal_slice": "release remaining work into accepted child lanes",
                    "node_id": "child-implementer-001",
                    "notes": [
                        "This deterministic startup bundle is slice-scoped.",
                        "The child still owes substantive formalization or dependency-closure progress after startup.",
                    ],
                    "source_tex_ref": "/tmp/source.tex",
                    "terminal_authority_scope": "local",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "accepted_child_node_ids": ["child-linear-001", "child-linear-002"],
                    "artifact_scope": "slice",
                    "goal_slice": "release remaining work into accepted child lanes",
                    "node_id": "child-implementer-001",
                    "released_remaining_work": [
                        {
                            "goal_slice": "probability lane",
                            "node_id": "child-linear-001",
                        },
                        {
                            "goal_slice": "faithfulness lane",
                            "node_id": "child-linear-002",
                        },
                    ],
                    "round_id": "R1",
                    "source_tex_ref": "/tmp/source.tex",
                    "status": "TERMINAL",
                    "summary": "Local deterministic core settled; remaining work released into accepted child lanes.",
                    "terminal_authority_scope": "local",
                    "terminal_classification": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
                    "whole_paper_terminal_authority": False,
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        publish_root = source_workspace_root / "deliverables" / "primary_artifact"
        publication_receipt_ref = (
            state_root / "artifacts" / "publication" / "child-implementer-001" / "WorkspaceArtifactPublicationReceipt.json"
        )
        publish_workspace_artifact_snapshot(
            node_id="child-implementer-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=publication_receipt_ref,
        )

        kernel_result_ref = state_root / "artifacts" / "child-implementer-001" / "result.json"
        workspace_result_ref = source_workspace_root / "artifacts" / "child-implementer-001" / "result.json"
        for result_ref in (kernel_result_ref, workspace_result_ref):
            result_ref.parent.mkdir(parents=True, exist_ok=True)
            result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": "child-implementer-001",
                        "parent_node_id": "root-kernel",
                        "lineage_ref": "root-kernel->child-implementer-001",
                        "round_id": "R1",
                        "workspace_root": str(source_workspace_root.resolve()),
                        "status": "IN_PROGRESS",
                        "outcome": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
                        "result_kind": "implementer_result",
                        "result_ref": str(kernel_result_ref.resolve()),
                        "workspace_result_sink_ref": str(workspace_result_ref.resolve()),
                        "kernel_result_sink_ref": str(kernel_result_ref.resolve()),
                        "workspace_mirror_ref": str(publish_root.resolve()),
                        "delivered_artifact_ref": str(publish_root.resolve()),
                        "publish_ready_artifact_refs": [str(publish_root.resolve())],
                        "publication_receipt_ref": str(publication_receipt_ref.resolve()),
                        "whole_paper_status_ref": "deliverables/primary_artifact/WHOLE_PAPER_STATUS.json",
                        "summary": "Local deterministic core settled; remaining work released into accepted child lanes.",
                        "split_state": {
                            "accepted": True,
                            "accepted_target_nodes": ["child-linear-001", "child-linear-002"],
                            "mode": "parallel",
                            "proposed": True,
                        },
                        "terminal_authority_scope": "local",
                        "workflow_scope": "whole_paper_formalization",
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

        repaired = backfill_authoritative_terminal_results(
            state_root=state_root,
            node_ids=["child-implementer-001"],
            continue_deferred=False,
        )
        if "child-implementer-001" not in repaired:
            return _fail("split continuation result sink repair must rewrite the stale authoritative source result")

        for result_ref in (kernel_result_ref, workspace_result_ref):
            payload = json.loads(result_ref.read_text(encoding="utf-8"))
            if str(payload.get("status") or "") != "COMPLETED":
                return _fail("split continuation result sink repair must terminalize stale IN_PROGRESS implementer results")
            if str(payload.get("outcome") or "") != "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
                return _fail("split continuation result sink repair must preserve the split-continuation outcome")

        source_state = json.loads((state_root / "state" / "child-implementer-001.json").read_text(encoding="utf-8"))
        kernel_state_payload = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        kernel_node = dict(dict(kernel_state_payload.get("nodes") or {}).get("child-implementer-001") or {})
        for surface in (source_state, kernel_node):
            if str(surface.get("status") or "") != "BLOCKED":
                return _fail("split continuation result sink repair must keep source node lifecycle BLOCKED after terminalizing the result sink")
            if str(surface.get("authoritative_result_status") or "") != "COMPLETED":
                return _fail("split continuation result sink repair must project authoritative_result_status=COMPLETED")
            if str(surface.get("authoritative_result_outcome") or "") != "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
                return _fail("split continuation result sink repair must project authoritative split-continuation outcome")
    finally:
        shutil.rmtree(td, ignore_errors=True)

    return 0


def _source_slice_local_completion_publication_surface_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.runtime_paths import node_live_artifact_root

    td = Path(tempfile.mkdtemp(prefix="loop_system_slice_local_completion_publication_surface_"))
    try:
        state_root = td / ".loop"
        _persist_base_state(state_root)
        source_workspace_root = state_root.parent / "workspace" / "child-implementer-001"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-implementer-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# Whole-Paper Slice Startup Bundle\n", encoding="utf-8")
        (live_root / "TRACEABILITY.md").write_text("# Slice Startup Traceability\n", encoding="utf-8")
        (live_root / "SLICE_STATUS.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "goal_slice": "close the probabilistic appendix slice faithfully",
                    "node_id": "child-implementer-001",
                    "round_id": "R1",
                    "slice_startup_batch_materialized": True,
                    "source_tex_ref": "/tmp/source.tex",
                    "status": "IN_PROGRESS",
                    "terminal_authority_scope": "local",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "analysis").mkdir(parents=True, exist_ok=True)
        (live_root / "analysis" / "SLICE_BOUNDARY.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "goal_slice": "close the probabilistic appendix slice faithfully",
                    "node_id": "child-implementer-001",
                    "notes": [
                        "This deterministic startup bundle is slice-scoped.",
                        "The child still owes substantive formalization or dependency-closure progress after startup.",
                    ],
                    "source_tex_ref": "/tmp/source.tex",
                    "terminal_authority_scope": "local",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "classification": "slice_local_formalization_complete",
                    "goal_slice": "close the probabilistic appendix slice faithfully",
                    "last_verified_utc": "2026-03-27T13:07:23Z",
                    "node_id": "child-implementer-001",
                    "round_id": "R1",
                    "source_tex_ref": "/tmp/source.tex",
                    "split_status": "requested_rejected_generation_budget_historical",
                    "status": "LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION",
                    "summary": "This slice-local artifact fully closes the probabilistic appendix lane while leaving whole-paper closeout parent-owned.",
                    "terminal_authority_scope": "local",
                    "verification": {
                        "axiom_checked_theorems": ["thm_probabilistic_closing"],
                        "lake_build": "success",
                    },
                    "whole_paper_terminal_classification": "NOT_OWNED_BY_SLICE",
                    "workflow_scope": "whole_paper_formalization",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        publish_root = source_workspace_root / "deliverables" / "primary_artifact"
        publish_workspace_artifact_snapshot(
            node_id="child-implementer-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "child-implementer-001"
            / "WorkspaceArtifactPublicationReceipt.json",
        )
        published_readme = (publish_root / "README.md").read_text(encoding="utf-8").lower()
        if "slice local completion" not in published_readme or "parent-owned" not in published_readme:
            return _fail("slice-local completion publication must harmonize README.md away from startup-bundle wording")
        published_traceability = (publish_root / "TRACEABILITY.md").read_text(encoding="utf-8").lower()
        if "slice local completion" not in published_traceability or "not_owned_by_slice" not in published_traceability:
            return _fail("slice-local completion publication must harmonize TRACEABILITY.md with local-completion truth")
        published_slice_status = json.loads((publish_root / "SLICE_STATUS.json").read_text(encoding="utf-8"))
        if str(published_slice_status.get("status") or "") != "LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION":
            return _fail("slice-local completion publication must harmonize SLICE_STATUS.json to the local completion status")
        if str(published_slice_status.get("slice_completion_classification") or "") != "slice_local_formalization_complete":
            return _fail("slice-local completion publication must persist slice completion classification in SLICE_STATUS.json")
        published_boundary = json.loads((publish_root / "analysis" / "SLICE_BOUNDARY.json").read_text(encoding="utf-8"))
        notes = [str(item) for item in list(published_boundary.get("notes") or [])]
        if not any("parent-owned" in item.lower() for item in notes):
            return _fail("slice-local completion publication must rewrite SLICE_BOUNDARY notes away from startup-bundle wording")
        published_whole_paper_status = json.loads((publish_root / "WHOLE_PAPER_STATUS.json").read_text(encoding="utf-8"))
        if str(published_whole_paper_status.get("classification") or "") != "slice_local_formalization_complete":
            return _fail("slice-local completion publication must preserve the whole-paper status classification evidence")
    finally:
        shutil.rmtree(td, ignore_errors=True)

    return 0


def _inflight_evaluator_authority_gate_case() -> int:
    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state
    from loop_product.protocols.node import NodeStatus
    from loop_product.runtime_paths import node_live_artifact_root

    with tempfile.TemporaryDirectory(prefix="loop_system_split_authority_inflight_eval_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        authority = kernel_internal_authority()
        kernel_state = load_kernel_state(state_root)
        child_workspace_root = ROOT / "workspace" / "test-child-inflight-evaluator-authority"
        shutil.rmtree(child_workspace_root, ignore_errors=True)
        materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id="child-implementer-001",
            node_id="child-inflight-evaluator-001",
            goal_slice="prove that in-flight evaluator state must gate authoritative blocked results",
            round_id="R1.child-inflight-evaluator-001",
            execution_policy={"sandbox_mode": "workspace-write", "agent_provider": "codex_cli"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 1},
            node_kind="implementer",
            generation=2,
            allowed_actions=["implement", "evaluate", "report"],
            workspace_root=str(child_workspace_root.resolve()),
            codex_home="",
            depends_on_node_ids=[],
            activation_condition="",
            result_sink_ref="artifacts/child-inflight-evaluator-001/result.json",
            lineage_ref="root-kernel->child-implementer-001->child-inflight-evaluator-001",
            status=NodeStatus.ACTIVE,
            authority=authority,
        )
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-inflight-evaluator-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# inflight evaluator gate\n", encoding="utf-8")
        publish_workspace_artifact_snapshot(
            node_id="child-inflight-evaluator-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=child_workspace_root / "deliverables" / "primary_artifact",
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "child-inflight-evaluator-001"
            / "WorkspaceArtifactPublicationReceipt.json",
        )
        result_ref = state_root / "artifacts" / "child-inflight-evaluator-001" / "result.json"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": "child-inflight-evaluator-001",
                    "status": "BLOCKED",
                    "outcome": "STUCK",
                    "summary": "child wrote a blocked result before the evaluator reached reviewer closure",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        evaluator_run_root = (
            state_root
            / "artifacts"
            / "evaluator_runs"
            / "child-inflight-evaluator-001"
            / "implementer_task_child-inflight-evaluator-001"
            / "R1__child-inflight-evaluator-001__evaluator"
        )
        evaluator_run_root.mkdir(parents=True, exist_ok=True)
        (evaluator_run_root / "EvaluatorRunState.json").write_text(
            json.dumps(
                {
                    "evaluation_id": "implementer_task_child-inflight-evaluator-001",
                    "run_id": "R1__child-inflight-evaluator-001__evaluator",
                    "status": "IN_PROGRESS",
                    "checker": {"status": "COMPLETED", "attempt_count": 1},
                    "lanes_by_unit_id": {
                        "EU-001": {"status": "COMPLETED"},
                        "EU-002": {"status": "RUNNING"},
                    },
                    "reviewer": {"status": "PENDING", "attempt_count": 0},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        authority_view = query_authority_view(state_root)
        node_graph = {item["node_id"]: item for item in authority_view["node_graph"]}
        if node_graph.get("child-inflight-evaluator-001", {}).get("status") != NodeStatus.ACTIVE.value:
            return _fail(
                "authoritative result sync must ignore blocked child results while the latest evaluator run is still in progress"
            )
        node_state = json.loads((state_root / "state" / "child-inflight-evaluator-001.json").read_text(encoding="utf-8"))
        if node_state.get("status") != NodeStatus.ACTIVE.value:
            return _fail(
                "per-node snapshot must stay ACTIVE while an in-flight evaluator conflicts with a child-authored blocked result"
            )
        shutil.rmtree(child_workspace_root, ignore_errors=True)

    return 0


def _split_child_workspace_is_run_scoped_case() -> int:
    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state
    from loop_product.runtime_paths import default_child_workspace_root

    with tempfile.TemporaryDirectory(prefix="loop_system_split_child_workspace_scope_") as td:
        state_root_a = Path(td) / ".loop" / "runtime_a"
        state_root_b = Path(td) / ".loop" / "runtime_b"
        _persist_base_state(state_root_a)
        _persist_base_state(state_root_b)
        kernel_a = load_kernel_state(state_root_a)
        kernel_b = load_kernel_state(state_root_b)
        parent = _base_source_node()
        kernel_a.register_node(parent)
        kernel_b.register_node(parent)
        authority = kernel_internal_authority()
        child_a = materialize_child(
            state_root=state_root_a,
            kernel_state=kernel_a,
            parent_node_id=parent.node_id,
            node_id="external_dependency_closure",
            goal_slice="close external dependencies",
            round_id="R1",
            execution_policy={"sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "xhigh"},
            budget_profile={"max_rounds": 3},
            authority=authority,
        )
        child_b = materialize_child(
            state_root=state_root_b,
            kernel_state=kernel_b,
            parent_node_id=parent.node_id,
            node_id="external_dependency_closure",
            goal_slice="close external dependencies",
            round_id="R1",
            execution_policy={"sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "xhigh"},
            budget_profile={"max_rounds": 3},
            authority=authority,
        )
        expected_a = default_child_workspace_root(state_root=state_root_a, node_id="external_dependency_closure")
        expected_b = default_child_workspace_root(state_root=state_root_b, node_id="external_dependency_closure")
        if Path(child_a.workspace_root).resolve() != expected_a.resolve():
            return _fail("split child without explicit workspace_root must default to a run-scoped workspace path")
        if Path(child_b.workspace_root).resolve() != expected_b.resolve():
            return _fail("run-scoped child workspace default must derive from the current state_root")
        if Path(child_a.workspace_root).resolve() == Path(child_b.workspace_root).resolve():
            return _fail("same split child node_id across different runtime roots must not share one workspace directory")

    return 0


def main() -> int:
    try:
        rc = _accepted_split_case()
        if rc:
            return rc
        rc = _parallel_split_with_dependency_bound_child_case()
        if rc:
            return rc
        rc = _parallel_split_dependency_gated_prose_rationale_case()
        if rc:
            return rc
        rc = _split_required_outputs_persist_case()
        if rc:
            return rc
        rc = _whole_paper_split_child_contract_inheritance_case()
        if rc:
            return rc
        rc = _authoritative_child_result_sync_case()
        if rc:
            return rc
        rc = _publication_rejects_materialized_live_runtime_heavy_trees_case()
        if rc:
            return rc
        rc = _source_split_continuation_sync_case()
        if rc:
            return rc
        rc = _source_split_continuation_publication_surface_case()
        if rc:
            return rc
        rc = _source_split_continuation_result_sink_repair_case()
        if rc:
            return rc
        rc = _source_slice_local_completion_publication_surface_case()
        if rc:
            return rc
        rc = _inflight_evaluator_authority_gate_case()
        if rc:
            return rc
        rc = _split_child_workspace_is_run_scoped_case()
        if rc:
            return rc
        rc = _rejected_split_case()
        if rc:
            return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"split authority runtime raised unexpectedly: {exc}")

    print("[loop-system-split-authority] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
