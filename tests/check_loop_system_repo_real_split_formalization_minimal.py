#!/usr/bin/env python3
"""Exercise a minimal real-paper deferred split/activate scenario."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PAPER_MAIN_TEX = REPO_ROOT / ".cache" / "leanatlas" / "tmp" / "arxiv_2602_11505v2" / "source" / "main.tex"
PAPER_TITLE = "Calibrating an Imperfect Auxiliary Predictor for Unobserved No-Purchase Choice"


def _fail(msg: str) -> int:
    print(f"[loop-system-real-split-formalization][FAIL] {msg}", file=sys.stderr)
    return 2


def _require_real_paper_lines() -> tuple[str, str]:
    if not PAPER_MAIN_TEX.exists():
        raise FileNotFoundError(f"missing staged paper source: {PAPER_MAIN_TEX}")
    tex = PAPER_MAIN_TEX.read_text(encoding="utf-8")
    if PAPER_TITLE not in tex:
        raise RuntimeError("staged paper source does not contain the expected title")
    theorem_line = next((line.strip() for line in tex.splitlines() if "\\label{thm:ident-consistency}" in line), "")
    proof_line = next(
        (line.strip() for line in tex.splitlines() if "\\subsection{Proof of Theorem \\ref{thm:ident-consistency}}" in line),
        "",
    )
    if not theorem_line or not proof_line:
        raise RuntimeError("staged paper source must expose both theorem and proof lines for the minimal split scenario")
    return theorem_line, proof_line


def _persist_base_state(state_root: Path, theorem_line: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    workspace_root = state_root.parent / "workspace" / "child-formalizer-001"
    workspace_root.mkdir(parents=True, exist_ok=True)
    kernel_state = KernelState(
        task_id="real-paper-deferred-split",
        root_goal=f"formally cover {PAPER_TITLE} without incomplete internal proof gaps",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise a real-paper deferred split formalization run",
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
    source_node = NodeSpec(
        node_id="child-formalizer-001",
        node_kind="implementer",
        goal_slice=(
            f"Using {PAPER_MAIN_TEX}, formalize the primary ident-consistency statement from "
            f"{PAPER_TITLE}. Freeze theorem source line: {theorem_line}"
        ),
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "high", "role": "implementer"},
        budget_profile={"max_rounds": 3},
        allowed_actions=["implement", "evaluate", "report", "split_request", "merge_request"],
        workspace_root=str(workspace_root.resolve()),
        delegation_ref="state/delegations/child-formalizer-001.json",
        result_sink_ref="artifacts/child-formalizer-001/result.json",
        lineage_ref="root-kernel->child-formalizer-001",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    source_record = source_node.to_dict()
    source_record["runtime_state"] = {
        "attachment_state": "ATTACHED",
        "observed_at": "",
        "summary": "real-paper source node already running",
        "observation_kind": "dispatch:started",
        "evidence_refs": [],
    }
    kernel_state.nodes[source_node.node_id] = source_record
    kernel_state.delegation_map[source_node.node_id] = root_node.node_id
    kernel_state.active_requirements.append(source_node.goal_slice)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
    _write_source_handoff(state_root=state_root, workspace_root=workspace_root, source_node=source_node)


def _write_source_handoff(*, state_root: Path, workspace_root: Path, source_node) -> Path:
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
    handoff_ref = workspace_root / "FROZEN_HANDOFF.json"
    workspace_result_sink = workspace_root / "artifacts" / source_node.node_id / "result.json"
    kernel_result_sink = state_root / "artifacts" / source_node.node_id / "result.json"
    handoff_ref.write_text(
        json.dumps(
            {
                "node_id": source_node.node_id,
                "round_id": source_node.round_id,
                "state_root": str(state_root.resolve()),
                "endpoint_artifact_ref": str(endpoint_artifact_ref.resolve()),
                "root_goal": f"formally cover {PAPER_TITLE} without incomplete internal proof gaps",
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


def _source_node(theorem_line: str, *, workspace_root: Path | None = None):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-formalizer-001",
        node_kind="implementer",
        goal_slice=(
            f"Using {PAPER_MAIN_TEX}, formalize the primary ident-consistency statement from "
            f"{PAPER_TITLE}. Freeze theorem source line: {theorem_line}"
        ),
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "high", "role": "implementer"},
        budget_profile={"max_rounds": 3},
        allowed_actions=["implement", "evaluate", "report", "split_request", "merge_request"],
        workspace_root=str(workspace_root.resolve()) if workspace_root is not None else "",
        delegation_ref="state/delegations/child-formalizer-001.json",
        result_sink_ref="artifacts/child-formalizer-001/result.json",
        lineage_ref="root-kernel->child-formalizer-001",
        status=NodeStatus.ACTIVE,
    )


def _mark_source_completed(state_root: Path, node_id: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus

    authority = kernel_internal_authority()
    kernel_state = load_kernel_state(state_root)
    kernel_state.nodes[node_id]["status"] = NodeStatus.COMPLETED.value
    persist_node_snapshot(state_root, kernel_state.nodes[node_id], authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)


def _mark_node_completed(state_root: Path, node_id: str) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeStatus

    authority = kernel_internal_authority()
    kernel_state = load_kernel_state(state_root)
    kernel_state.nodes[node_id]["status"] = NodeStatus.COMPLETED.value
    persist_node_snapshot(state_root, kernel_state.nodes[node_id], authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)


def main() -> int:
    import loop_product.kernel.topology as topology_module

    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeStatus
    from loop_product.topology.activate import build_activate_request
    from loop_product.topology.merge import build_merge_request
    from loop_product.topology.split_review import build_split_request

    try:
        theorem_line, proof_line = _require_real_paper_lines()
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc))

    with tempfile.TemporaryDirectory(prefix="loop_system_real_split_formalization_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root, theorem_line)
        source_node = _source_node(
            theorem_line,
            workspace_root=state_root.parent / "workspace" / "child-formalizer-001",
        )
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
                "node_id": str(kwargs["node_id"]),
                "workspace_root": str(kwargs["workspace_root"]),
                "state_root": str(kwargs["state_root"]),
                "launch_spec": {
                    "argv": ["/bin/echo", "real-paper-minimal"],
                    "env": {},
                    "cwd": str(kwargs["workspace_root"]),
                    "stdin_path": str((Path(str(kwargs["workspace_root"])) / "CHILD_PROMPT.md").resolve()),
                },
            }
            bootstrap_ref.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return payload

        def _fake_launch(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            launch_calls.append({"result_ref": str(Path(result_ref).resolve())})
            return {"launch_result_ref": str(Path(result_ref).resolve())}

        topology_module.bootstrap_first_implementer_node = _fake_bootstrap
        topology_module.launch_child_from_result_ref = _fake_launch
        try:
            mutation = build_split_request(
                source_node=source_node,
                target_nodes=[
                    {
                        "node_id": "child-proof-followup-001",
                        "goal_slice": (
                            f"After the primary theorem is formalized, continue with the deferred proof obligations from "
                            f"{PAPER_TITLE}. Freeze proof source line: {proof_line}"
                        ),
                        "depends_on_node_ids": [source_node.node_id],
                        "activation_condition": f"after:{source_node.node_id}:completed",
                    }
                ],
                split_mode="deferred",
                completed_work=(
                    "the current source node keeps the theorem statement and any immediately required supporting objects"
                ),
                remaining_work=(
                    "the detailed proof obligations remain deferred until the source node reaches COMPLETED"
                ),
                reason="split theorem statement from proof obligations using the staged real-paper source",
            )
            envelope = submit_topology_mutation(
                state_root,
                mutation,
                round_id=source_node.round_id,
                generation=source_node.generation,
            )
            if envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"real-paper deferred split must be accepted, got {envelope.status.value!r}")

            authority = query_authority_view(state_root)
            node_graph = {item["node_id"]: item for item in authority["node_graph"]}
            child_graph = node_graph.get("child-proof-followup-001")
            if child_graph is None or child_graph["status"] != NodeStatus.PLANNED.value:
                return _fail("real-paper deferred split must materialize a PLANNED proof-followup child")

            child_state_path = state_root / "state" / "child-proof-followup-001.json"
            child_state = json.loads(child_state_path.read_text(encoding="utf-8"))
            if PAPER_TITLE not in str(child_state.get("goal_slice") or ""):
                return _fail("real-paper child goal_slice must preserve the paper title")
            if proof_line not in str(child_state.get("goal_slice") or ""):
                return _fail("real-paper child goal_slice must freeze the proof source line")
            if child_state.get("activation_condition") != f"after:{source_node.node_id}:completed":
                return _fail("real-paper deferred child must preserve completed-based activation_condition")

            _mark_source_completed(state_root, source_node.node_id)
            activation = build_activate_request(
                "child-proof-followup-001",
                reason="source theorem lane completed; activate deferred proof-followup child",
            )
            activation_envelope = submit_topology_mutation(
                state_root,
                activation,
                round_id="R1",
                generation=2,
            )
            if activation_envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"real-paper deferred activation must be accepted, got {activation_envelope.status.value!r}")

            authority_after = query_authority_view(state_root)
            node_graph_after = {item["node_id"]: item for item in authority_after["node_graph"]}
            activated = node_graph_after.get("child-proof-followup-001")
            if activated is None or activated["status"] != NodeStatus.ACTIVE.value:
                return _fail("real-paper deferred child must become ACTIVE after activation")
            if "child-proof-followup-001" not in authority_after["active_child_nodes"]:
                return _fail("authority view must expose the activated real-paper deferred child as active")
            if "child-proof-followup-001" in authority_after["planned_child_nodes"]:
                return _fail("activated real-paper deferred child must leave planned_child_nodes")
            if len(bootstrap_calls) != 1 or len(launch_calls) != 1:
                return _fail("real-paper deferred activation must bootstrap and launch the activated child exactly once")

            _mark_node_completed(state_root, "child-proof-followup-001")

            merge = build_merge_request(
                source_node.node_id,
                ["child-proof-followup-001"],
                reason="the completed deferred proof child should converge back into the source theorem lane",
            )
            merge_envelope = submit_topology_mutation(
                state_root,
                merge,
                round_id="R1",
                generation=source_node.generation,
            )
            if merge_envelope.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"real-paper deferred merge must be accepted, got {merge_envelope.status.value!r}")

            authority_final = query_authority_view(state_root)
            node_graph_final = {item["node_id"]: item for item in authority_final["node_graph"]}
            source_final = node_graph_final.get(source_node.node_id)
            if source_final is None or source_final["status"] != NodeStatus.ACTIVE.value:
                return _fail("real-paper source node must return to ACTIVE after deferred merge")
            if "child-proof-followup-001" in authority_final["active_child_nodes"]:
                return _fail("completed proof child must not remain active after deferred merge")
        finally:
            if original_bootstrap is not None:
                topology_module.bootstrap_first_implementer_node = original_bootstrap
            else:
                delattr(topology_module, "bootstrap_first_implementer_node")
            if original_launch is not None:
                topology_module.launch_child_from_result_ref = original_launch
            else:
                delattr(topology_module, "launch_child_from_result_ref")

    print("[loop-system-real-split-formalization] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
