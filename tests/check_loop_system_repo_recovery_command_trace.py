#!/usr/bin/env python3
"""Validate read-only recovery command trace over command, event, and projection history."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-recovery-command-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _make_source_node(*, node_id: str, status: str):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id=node_id,
        node_kind="implementer",
        goal_slice="recover the current bounded implementation lane",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "bounded"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report", "resume_request", "retry_request", "relaunch_request"],
        delegation_ref=f"state/delegations/{node_id}.json",
        result_sink_ref=f"artifacts/{node_id}/result.json",
        lineage_ref=f"root-kernel->{node_id}",
        status=NodeStatus(status),
    )


def _persist_base_state(state_root: Path, *, node_id: str, status: str, blocked_reason: str = "") -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone4-recovery-command-trace",
        root_goal="validate recovery command trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise recovery command trace validation",
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
    source_node = _make_source_node(node_id=node_id, status=status)
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    if blocked_reason:
        kernel_state.blocked_reasons[node_id] = blocked_reason
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_authority_view, query_topology_command_trace_view
        from loop_product.kernel.submit import submit_topology_mutation
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime.recover import build_resume_request, build_retry_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_recovery_command_trace_resume_") as repo_root:
        state_root = repo_root / ".loop"
        source_node = _make_source_node(node_id="child-blocked-001", status="BLOCKED")
        _persist_base_state(
            state_root,
            node_id=source_node.node_id,
            status="BLOCKED",
            blocked_reason="waiting on an external prerequisite that is now repaired",
        )
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
            return _fail("resume trace scenario requires an accepted resume request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        trace = query_topology_command_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("recovery command trace must stay read-only")
        source_projection = dict(dict(trace.get("projection_effect") or {}).get("source_node") or {})
        source_lineage = dict(trace.get("source_node_lineage") or {})
        if str(trace.get("command_kind") or "") != "resume":
            return _fail("resume trace must expose command_kind=resume")
        if str(source_projection.get("status") or "") != "ACTIVE":
            return _fail("resume trace must expose the resumed source node as ACTIVE")
        if str(source_projection.get("blocked_reason") or ""):
            return _fail("resume trace must show that blocked_reason was cleared")
        if str(source_projection.get("runtime_attachment_state") or "") != "UNOBSERVED":
            return _fail("resume trace must expose the reset runtime attachment state")
        if str(source_projection.get("runtime_observation_kind") or "") != "recovery_resume_accepted":
            return _fail("resume trace must expose the recovery-specific runtime observation kind")
        if not bool(source_lineage.get("read_only")):
            return _fail("resume trace must expose the resumed source node through nested node-lineage")
        if str(source_lineage.get("node_id") or "") != source_node.node_id:
            return _fail("resume trace nested source lineage must stay anchored to the resumed node")
        if list(trace.get("gaps") or []):
            return _fail("accepted resume trace must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_recovery_command_trace_retry_") as repo_root:
        state_root = repo_root / ".loop"
        source_node = _make_source_node(node_id="child-retry-001", status="FAILED")
        _persist_base_state(state_root, node_id=source_node.node_id, status="FAILED")
        accepted = submit_topology_mutation(
            state_root,
            build_retry_request(
                source_node,
                reason="the evaluator found a self-caused path issue and can retry safely",
                self_attribution="evaluator_path_error",
                self_repair="fix the staged command path before retrying the same node",
            ),
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("retry trace scenario requires an accepted retry request")
        _ = query_authority_view(state_root)
        before = committed_event_count(state_root)
        trace = query_topology_command_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("retry command trace must stay read-only")
        source_projection = dict(dict(trace.get("projection_effect") or {}).get("source_node") or {})
        source_lineage = dict(trace.get("source_node_lineage") or {})
        canonical = dict(trace.get("canonical_event") or {})
        if str(trace.get("command_kind") or "") != "retry":
            return _fail("retry trace must expose command_kind=retry")
        if str(source_projection.get("status") or "") != "ACTIVE":
            return _fail("retry trace must expose the retried source node as ACTIVE")
        if str(source_projection.get("runtime_attachment_state") or "") != "UNOBSERVED":
            return _fail("retry trace must expose the retry-reset runtime attachment state")
        if str(source_projection.get("runtime_observation_kind") or "") != "recovery_retry_accepted":
            return _fail("retry trace must expose the retry-specific runtime observation kind")
        if str(canonical.get("self_attribution") or "") != "evaluator_path_error":
            return _fail("retry trace must expose self-attribution through the canonical event summary")
        if str(canonical.get("self_repair") or "") != "fix the staged command path before retrying the same node":
            return _fail("retry trace must expose self-repair through the canonical event summary")
        if not bool(source_lineage.get("read_only")):
            return _fail("retry trace must expose the retried source node through nested node-lineage")
        if str(source_lineage.get("node_id") or "") != source_node.node_id:
            return _fail("retry trace nested source lineage must stay anchored to the retried node")
        if list(trace.get("gaps") or []):
            return _fail("accepted retry trace must not report causal gaps")

    print("[loop-system-recovery-command-trace][OK] recovery command trace stays read-only and exposes resume/retry projection effects")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
