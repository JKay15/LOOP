#!/usr/bin/env python3
"""Validate IF-6 audit replay and experience query surfaces."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator, validate


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-audit-experience][FAIL] {msg}", file=sys.stderr)
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
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.audit import (
            query_audit_experience_view,
            query_recent_audit_events,
            record_audit_event,
        )
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_objects import NodeTerminalOutcome
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.runtime.lifecycle import build_node_terminal_result
    except Exception as exc:  # noqa: BLE001
        return _fail(f"loop system IF-6 imports failed: {exc}")

    try:
        audit_schema = _load_schema("LoopSystemAuditExperienceView.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"missing IF-6 schema: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise IF-6 audit visibility validation",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/audit_view.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="exercise audit and accepted experience visibility",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/audit_view.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_audit_experience_") as td:
        state_root = Path(td) / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="wave6-audit-experience",
            root_goal="validate IF-6 audit and experience queries",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        record_audit_event(
            state_root,
            "split_decision",
            {
                "node_id": child_node.node_id,
                "summary": "split child work into bounded validation and repair branches",
            },
        )
        record_audit_event(
            state_root,
            "rules_snapshot",
            {
                "node_id": root_node.node_id,
                "summary": "current hard rules frozen for this wave",
            },
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
                    summary="The evaluator hit its own environment issue and must self-repair.",
                    evidence_refs=["reviewer:stuck"],
                    retryable=False,
                    diagnostics={
                        "self_attribution": "evaluator_environment",
                        "self_repair": "repair the environment before blaming implementation",
                    },
                ).to_dict(),
                "status": "report",
                "note": "stuck evaluator report",
            },
        )
        submit_control_envelope(
            state_root,
            build_node_terminal_result(
                child_node,
                outcome=NodeTerminalOutcome.FAIL_CLOSED,
                summary="child terminated fail-closed after confirming its own runner misconfiguration",
                evidence_refs=["roundbook:child-implementer-001"],
                self_attribution="child_runtime",
                self_repair="refresh the runner config before the next relaunch",
            ).to_envelope(round_id="R1.2", generation=child_node.generation),
        )

        recent_events = query_recent_audit_events(state_root, node_id=child_node.node_id, limit=10)
        if not recent_events:
            return _fail("query_recent_audit_events must return recent child-scoped audit history")

        audit_view = query_audit_experience_view(state_root, node_id=child_node.node_id, limit=10)
        validate(audit_view, audit_schema)

        if not audit_view["recent_structural_decisions"]:
            return _fail("audit view must replay recent structural decisions")
        if not audit_view["recent_node_events"]:
            return _fail("audit view must expose recent node events")
        if not audit_view["effective_rule_sources"]:
            return _fail("audit view must expose current hard rule sources")
        if "LOOP_SYSTEM_AUDIT_EXPERIENCE_CONTRACT.md" not in " ".join(audit_view["effective_rule_sources"]):
            return _fail("audit view must expose the current audit/experience hard-rule source")
        if set(audit_view["storage_partitions"]) != {"experience_assets", "audit", "cache", "durable_state", "hard_rules"}:
            return _fail("audit view must distinguish experience assets, audit, cache, durable state, and hard rules")
        if not any(asset.get("self_attribution") == "evaluator_environment" for asset in audit_view["experience_assets"]):
            return _fail("accepted evaluator self-attribution must stay visible as experience assets")
        if not any(asset.get("self_attribution") == "child_runtime" for asset in audit_view["experience_assets"]):
            return _fail("accepted child-node self-attribution must stay visible as experience assets")

    print("[loop-system-audit-experience] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
