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
    workspace_result_sink = workspace_root / "artifacts" / source_node.node_id / "result.json"
    kernel_result_sink = state_root / "artifacts" / source_node.node_id / "result.json"
    payload = {
        "node_id": source_node.node_id,
        "round_id": source_node.round_id,
        "state_root": str(state_root.resolve()),
        "endpoint_artifact_ref": str(endpoint_artifact_ref.resolve()),
        "root_goal": "faithfully formalize the whole paper with no internal gaps",
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
    }
    handoff_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
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


def _authoritative_child_result_sync_case() -> int:
    from loop_product.dispatch.child_dispatch import materialize_child
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import load_kernel_state
    from loop_product.protocols.node import NodeStatus

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

    return 0


def main() -> int:
    try:
        rc = _accepted_split_case()
        if rc:
            return rc
        rc = _authoritative_child_result_sync_case()
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
