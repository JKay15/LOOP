#!/usr/bin/env python3
"""Validate read-only command parity debugger over trusted expected effects and visible projected truth."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-command-parity-debugger][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_terminal_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-command-parity-debugger-terminal",
        root_goal="validate terminal parity debugger",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise command parity debugger validation",
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
    child_node = NodeSpec(
        node_id="child-terminal-001",
        node_kind="implementer",
        goal_slice="exercise terminal-result parity coverage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-terminal-001.json",
        result_sink_ref="artifacts/child-terminal-001/result.json",
        lineage_ref="root-kernel->child-terminal-001",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(child_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _persist_evaluator_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-command-parity-debugger-evaluator",
        root_goal="validate evaluator parity debugger",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise command parity debugger validation",
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
    child_node = NodeSpec(
        node_id="child-evaluator-001",
        node_kind="implementer",
        goal_slice="exercise evaluator-result parity coverage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-evaluator-001.json",
        result_sink_ref="artifacts/child-evaluator-001/result.json",
        lineage_ref="root-kernel->child-evaluator-001",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(child_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _make_resume_source_node():
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="child-blocked-001",
        node_kind="implementer",
        goal_slice="recover the current bounded implementation lane",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "resume_request", "retry_request", "relaunch_request"],
        delegation_ref="state/delegations/child-blocked-001.json",
        result_sink_ref="artifacts/child-blocked-001/result.json",
        lineage_ref="root-kernel->child-blocked-001",
        status=NodeStatus.BLOCKED,
    )


def _persist_resume_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-command-parity-debugger-recovery",
        root_goal="validate recovery parity debugger",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise command parity debugger validation",
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
    source_node = _make_resume_source_node()
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    kernel_state.blocked_reasons[source_node.node_id] = "waiting on an external prerequisite that is now repaired"
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_authority_view, query_command_parity_debugger_view
        from loop_product.kernel.submit import submit_control_envelope, submit_topology_mutation
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.protocols.control_objects import NodeTerminalOutcome
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.runtime.lifecycle import build_node_terminal_result
        from loop_product.runtime.recover import build_resume_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_command_parity_debugger_terminal_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_terminal_base_state(state_root)
        child_node = NodeSpec(
            node_id="child-terminal-001",
            node_kind="implementer",
            goal_slice="exercise terminal-result parity coverage",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["implement", "evaluate", "report"],
            delegation_ref="state/delegations/child-terminal-001.json",
            result_sink_ref="artifacts/child-terminal-001/result.json",
            lineage_ref="root-kernel->child-terminal-001",
            status=NodeStatus.ACTIVE,
        )
        accepted = submit_control_envelope(
            state_root,
            build_node_terminal_result(
                child_node,
                outcome=NodeTerminalOutcome.FAIL_CLOSED,
                summary="bounded local loop terminated fail-closed after exhausting retries",
                evidence_refs=["roundbook:child-terminal-001"],
                self_attribution="bounded_retry_exhausted",
                self_repair="resume only after a new committed recovery command arrives",
            ).to_envelope(round_id="R1.terminal", generation=child_node.generation),
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("terminal parity debugger scenario requires an accepted terminal result")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("terminal parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "terminal":
            return _fail("terminal parity debugger must expose terminal classification")
        if str(debug.get("trace_surface") or "") != "terminal_result_trace":
            return _fail("terminal parity debugger must route through terminal-result trace")
        if not bool(debug.get("parity_ok")):
            return _fail("terminal parity debugger must report parity for accepted terminal results")
        if list(debug.get("mismatches") or []):
            return _fail("terminal parity debugger must not report mismatches for accepted terminal results")

    with temporary_repo_root(prefix="loop_system_command_parity_debugger_evaluator_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_evaluator_base_state(state_root)
        evaluator_result = EvaluatorResult(
            verdict=EvaluatorVerdict.FAIL,
            lane="reviewer",
            summary="The evaluator found a retryable implementation gap.",
            evidence_refs=["reviewer:milestone4-evaluator"],
            retryable=True,
            diagnostics={
                "self_attribution": "implementation_gap",
                "self_repair": "repair and rerun the evaluator",
            },
        )
        accepted = submit_control_envelope(
            state_root,
            ControlEnvelope(
                source="child-evaluator-001",
                envelope_type="evaluator_result",
                round_id="R1.eval",
                generation=1,
                payload=evaluator_result.to_dict(),
                status=EnvelopeStatus.REPORT,
                note=evaluator_result.summary,
            ),
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("evaluator parity debugger scenario requires an accepted evaluator result")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("evaluator parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "evaluator":
            return _fail("evaluator parity debugger must expose evaluator classification")
        if str(debug.get("trace_surface") or "") != "evaluator_result_trace":
            return _fail("evaluator parity debugger must route through evaluator-result trace")
        if not bool(debug.get("parity_ok")):
            return _fail("evaluator parity debugger must report parity for accepted evaluator results")
        if list(debug.get("mismatches") or []):
            return _fail("evaluator parity debugger must not report mismatches for accepted evaluator results")

    with temporary_repo_root(prefix="loop_system_command_parity_debugger_resume_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_resume_base_state(state_root)
        source_node = _make_resume_source_node()
        accepted = submit_topology_mutation(
            state_root,
            build_resume_request(
                source_node,
                reason="external prerequisite repaired and the same node can continue",
                consistency_signal="dependency_unblocked",
            ),
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("resume parity debugger scenario requires an accepted resume request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("resume parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "topology":
            return _fail("resume parity debugger must expose topology classification")
        if str(debug.get("trace_surface") or "") != "topology_command_trace":
            return _fail("resume parity debugger must route through topology command trace")
        if str(debug.get("command_kind") or "") != "resume":
            return _fail("resume parity debugger must expose the recovery command kind")
        if not bool(debug.get("parity_ok")):
            return _fail("resume parity debugger must report parity for accepted resume commands")
        if list(debug.get("mismatches") or []):
            return _fail("resume parity debugger must not report mismatches for accepted resume commands")

    print("[loop-system-command-parity-debugger][OK] command parity debugger stays read-only and compares trusted expected effects with visible projected truth")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
