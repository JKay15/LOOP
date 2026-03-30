#!/usr/bin/env python3
"""Validate read-only terminal-result trace over audit, events, and projection history."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-terminal-result-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-terminal-result-trace",
        root_goal="validate terminal-result trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise terminal-result trace validation",
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
        goal_slice="exercise terminal-result trace coverage",
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


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_authority_view, query_terminal_result_trace_view
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.protocols.control_objects import NodeTerminalOutcome
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.runtime.lifecycle import build_node_terminal_result
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_terminal_result_trace_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_base_state(state_root)
        child_node = NodeSpec(
            node_id="child-terminal-001",
            node_kind="implementer",
            goal_slice="exercise terminal-result trace coverage",
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
            return _fail("terminal trace scenario requires an accepted terminal result")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        trace = query_terminal_result_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("terminal-result trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("terminal-result trace must report the accepted audit envelope")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("terminal-result trace must report the compatibility envelope mirror")
        if not bool(trace.get("canonical_event_present")):
            return _fail("terminal-result trace must report the canonical terminal-result event")
        source_projection = dict(trace.get("source_node") or {})
        source_lineage = dict(trace.get("source_node_lineage") or {})
        canonical = dict(trace.get("canonical_event") or {})
        if str(source_projection.get("node_id") or "") != "child-terminal-001":
            return _fail("terminal-result trace must expose the source node id")
        if str(source_projection.get("status") or "") != "FAILED":
            return _fail("terminal-result trace must expose the visible terminal node status")
        if str(source_projection.get("runtime_attachment_state") or "") != "TERMINAL":
            return _fail("terminal-result trace must expose the terminal runtime attachment state")
        if str(source_projection.get("runtime_observation_kind") or "") != "terminal_result":
            return _fail("terminal-result trace must expose the terminal runtime observation kind")
        if str(canonical.get("self_attribution") or "") != "bounded_retry_exhausted":
            return _fail("terminal-result trace must expose self-attribution through the canonical event summary")
        if str(canonical.get("self_repair") or "") != "resume only after a new committed recovery command arrives":
            return _fail("terminal-result trace must expose self-repair through the canonical event summary")
        if not bool(source_lineage.get("read_only")):
            return _fail("terminal-result trace must expose the source node through nested node-lineage")
        if str(source_lineage.get("node_id") or "") != "child-terminal-001":
            return _fail("terminal-result trace nested source lineage must stay anchored to the visible source node")
        if list(trace.get("gaps") or []):
            return _fail("accepted terminal-result trace must not report causal gaps")

    print("[loop-system-terminal-result-trace][OK] terminal-result trace stays read-only and exposes the visible terminal projection effect")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
