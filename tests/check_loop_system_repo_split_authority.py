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
    endpoint_artifact_ref = workspace_root / "EndpointArtifact.json"
    _write_minimal_endpoint_artifact(endpoint_artifact_ref)
    handoff_ref = workspace_root / "FROZEN_HANDOFF.json"
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
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)

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
            return {"launch_decision": "test-only", "source_result_ref": str(kwargs.get("result_ref") or "")}

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
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
            bootstrapped_ids = {str(item.get('node_id') or '') for item in bootstrap_calls}
            if bootstrapped_ids != {"child-review-001", "child-repair-001"}:
                return _fail("accepted split must bootstrap the exact accepted child ids")
            if any(str(item.get("mode") or "") != "continue_exact" for item in bootstrap_calls):
                return _fail("accepted split child bootstrap must reuse continue_exact frozen bootstrap semantics")
            if any(str(item.get("endpoint_artifact_ref") or "").strip() == "" for item in bootstrap_calls):
                return _fail("accepted split child bootstrap must reuse source frozen endpoint context")
            source_workspace_root = state_root.parent / "workspace" / "child-implementer-001"
            expected_inherited_refs = {
                str((source_workspace_root / "FROZEN_HANDOFF.json").resolve()),
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
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)

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
            return {"launch_decision": "test-only", "source_result_ref": str(kwargs.get("result_ref") or "")}

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        mutation = build_split_request(
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
                {
                    "node_id": "child-final-001",
                    "goal_slice": "integrate only after extraction and linear children finish",
                    "depends_on_node_ids": ["child-extraction-001", "child-linear-001"],
                },
            ],
            split_mode="parallel",
            reason="parallelize independent fronts but keep dependency-bound final integration gated",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        try:
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"dependency-gated parallel split must still be accepted, got {envelope.status.value!r}")

            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            if node_graph.get("child-extraction-001", {}).get("status") != NodeStatus.ACTIVE.value:
                return _fail("dependency-free parallel split child must become ACTIVE immediately")
            if node_graph.get("child-linear-001", {}).get("status") != NodeStatus.ACTIVE.value:
                return _fail("second dependency-free parallel split child must become ACTIVE immediately")
            if node_graph.get("child-final-001", {}).get("status") != NodeStatus.PLANNED.value:
                return _fail("dependency-bound child in a parallel split must remain PLANNED until dependencies are ready")
            if "child-final-001" not in authority["planned_child_nodes"]:
                return _fail("dependency-bound parallel child must appear in planned_child_nodes")
            if "child-final-001" in authority["active_child_nodes"]:
                return _fail("dependency-bound parallel child must not appear in active_child_nodes before activation")

            final_state = json.loads((state_root / "state" / "child-final-001.json").read_text(encoding="utf-8"))
            if final_state.get("depends_on_node_ids") != ["child-extraction-001", "child-linear-001"]:
                return _fail("dependency-bound parallel child must preserve depends_on_node_ids in node state")
            if len(bootstrap_calls) != 2 or len(launch_calls) != 2:
                return _fail("only dependency-free parallel split children should bootstrap and launch immediately")
            bootstrapped_ids = {str(item.get("node_id") or "") for item in bootstrap_calls}
            if bootstrapped_ids != {"child-extraction-001", "child-linear-001"}:
                return _fail("dependency-bound parallel child must not be bootstrapped before activation")
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
        original_bootstrap = getattr(topology_module, "bootstrap_first_implementer_node", None)
        original_launch = getattr(topology_module, "launch_child_from_result_ref", None)

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
            return {"launch_decision": "test-only", "source_result_ref": str(kwargs.get("result_ref") or "")}

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch_child_from_result_ref
        required_output_paths = [
            "formalization/appendix_claim_inventory.json",
            "formalization/appendix_claims.lean",
            "PARTITION/external_dependency_candidates.json",
            "WHOLE_PAPER_STATUS.json",
        ]
        mutation = build_split_request(
            source_node=source_node,
            target_nodes=[
                {
                    "node_id": "child-appendix-001",
                    "goal_slice": "formalize appendix support claims and close named external dependencies",
                    "required_output_paths": required_output_paths,
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
            if appendix_delegation.get("required_output_paths") != required_output_paths:
                return _fail("accepted split child delegation must preserve required_output_paths from the reviewed target payload")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("only the dependency-free required-output child should bootstrap immediately in this case")
            if bootstrap_calls[0].get("required_output_paths") != required_output_paths:
                return _fail("split child bootstrap request must surface required_output_paths for frozen handoff generation")
            bootstrap_workspace_root = Path(str(bootstrap_calls[0].get("workspace_root") or "")).expanduser().resolve()
            bootstrap_live_relpath = str(bootstrap_calls[0].get("workspace_live_artifact_relpath") or "")
            bootstrap_live_root = (bootstrap_workspace_root / bootstrap_live_relpath).resolve()
            from loop_product.runtime_paths import node_live_artifact_root

            expected_live_root = node_live_artifact_root(
                state_root=state_root,
                node_id="child-appendix-001",
                workspace_mirror_relpath="deliverables/primary_artifact",
            )
            if bootstrap_live_root != expected_live_root:
                return _fail("split child bootstrap request must move live artifact root into runtime-owned scratch outside the workspace")
            if expected_live_root.is_relative_to(bootstrap_workspace_root):
                return _fail("split child live artifact root must not remain under the child workspace root")
            expected_helper = (ROOT / "scripts" / "ensure_workspace_lake_packages.sh").resolve()
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


def main() -> int:
    try:
        rc = _accepted_split_case()
        if rc:
            return rc
        rc = _parallel_split_with_dependency_bound_child_case()
        if rc:
            return rc
        rc = _split_required_outputs_persist_case()
        if rc:
            return rc
        rc = _authoritative_child_result_sync_case()
        if rc:
            return rc
        rc = _source_split_continuation_sync_case()
        if rc:
            return rc
        rc = _inflight_evaluator_authority_gate_case()
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
