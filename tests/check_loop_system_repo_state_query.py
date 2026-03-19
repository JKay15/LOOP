#!/usr/bin/env python3
"""Validate IF-5 read-only authority query surface."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator, validate


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-state-query][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
    except Exception as exc:  # noqa: BLE001
        return _fail(f"loop system IF-5 imports failed: {exc}")

    try:
        authority_schema = _load_schema("LoopSystemAuthorityView.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"missing IF-5 schema: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise IF-5 authority query validation",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/authority_view.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="exercise authority query visibility",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/authority_view.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_state_query_") as td:
        state_root = Path(td) / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="wave5-state-query",
            root_goal="validate IF-5 authority query",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        submit_control_envelope(
            state_root,
            build_child_dispatch_status(child_node, note="child remains active while evaluator feedback is pending").to_envelope(),
        )
        submit_control_envelope(
            state_root,
            {
                "source": child_node.node_id,
                "envelope_type": "evaluator_result",
                "payload_type": "evaluator_result",
                "round_id": "R1.1",
                "generation": child_node.generation,
                "payload": EvaluatorResult(
                    verdict=EvaluatorVerdict.STUCK,
                    lane="reviewer",
                    summary="The evaluator is stuck on its own missing prerequisite and must self-repair.",
                    evidence_refs=["reviewer:stuck"],
                    retryable=False,
                    diagnostics={
                        "self_attribution": "evaluator_environment",
                        "self_repair": "repair the missing prerequisite before blaming implementation",
                    },
                ).to_dict(),
                "status": "report",
                "note": "stuck evaluator report",
            },
        )

        authority_view = query_authority_view(state_root)
        validate(authority_view, authority_schema)

        expected_keys = {
            "task_id",
            "root_goal",
            "root_node_id",
            "status",
            "node_graph",
            "node_lifecycle",
            "effective_requirements",
            "budget_view",
            "blocked_reasons",
            "delegation_map",
            "active_evaluator_lanes",
            "active_child_nodes",
            "planned_child_nodes",
        }
        if set(authority_view) != expected_keys:
            return _fail("authority view must stay simple and explicit rather than exposing raw kernel internals")
        if child_node.node_id not in authority_view["active_child_nodes"]:
            return _fail("authority view must directly expose active child nodes")
        if authority_view["planned_child_nodes"] != []:
            return _fail("authority view must expose planned_child_nodes separately from active child nodes")
        if authority_view["node_lifecycle"].get(child_node.node_id) != NodeStatus.ACTIVE.value:
            return _fail("authority view must expose per-node lifecycle state directly")
        if authority_view["delegation_map"].get(child_node.node_id) != root_node.node_id:
            return _fail("authority view must expose delegation map directly")
        if child_node.goal_slice not in authority_view["effective_requirements"]:
            return _fail("authority view must expose effective requirements directly")
        if child_node.node_id not in authority_view["blocked_reasons"]:
            return _fail("authority view must expose blocked reasons directly")
        if not any(
            lane["node_id"] == child_node.node_id and lane["verdict"] == "STUCK"
            for lane in authority_view["active_evaluator_lanes"]
        ):
            return _fail("authority view must expose active evaluator lanes directly")
        if not any(node["node_id"] == root_node.node_id for node in authority_view["node_graph"]):
            return _fail("authority view must expose the current authoritative node graph directly")

    print("[loop-system-state-query] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
