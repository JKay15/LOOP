#!/usr/bin/env python3
"""Validate kernel-approved runtime recovery for the standalone LOOP product repo."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-recovery][FAIL] {msg}", file=sys.stderr)
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


def _persist_base_state(
    state_root: Path,
    *,
    source_node_id: str,
    source_status: str,
    blocked_reason: str = "",
) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave12-runtime-recovery",
        root_goal="prove runtime recovery proposals stay kernel-owned and durable",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise runtime recovery",
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
    source_node = _make_source_node(node_id=source_node_id, status=source_status)
    kernel_state.register_node(source_node)
    if blocked_reason:
        kernel_state.blocked_reasons[source_node.node_id] = blocked_reason
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _accepted_resume_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_resume_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_resume_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-blocked-001", status="BLOCKED")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="BLOCKED",
            blocked_reason="waiting on an external prerequisite that is now repaired",
        )
        mutation = build_resume_request(
            source_node,
            reason="external prerequisite repaired and the same node can continue",
            consistency_signal="dependency_unblocked",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"resume request must be accepted after consistency passes, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("accepted resume must reactivate the blocked node")
        if source_node.node_id in authority["blocked_reasons"]:
            return _fail("accepted resume must clear the current blocked reason")

        audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
        if not any("resume" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
            return _fail("accepted resume must appear in recent structural decisions")

    return 0


def _accepted_retry_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_retry_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_retry_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-retry-001", status="FAILED")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="FAILED",
        )
        mutation = build_retry_request(
            source_node,
            reason="the evaluator found a self-caused path issue and can retry safely",
            self_attribution="evaluator_path_error",
            self_repair="fix the staged command path before retrying the same node",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"retry request must be accepted with bounded self-repair evidence, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("accepted retry must reactivate the same node")

        audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
        if not any(
            item.get("self_attribution") == "evaluator_path_error"
            and item.get("self_repair") == "fix the staged command path before retrying the same node"
            for item in audit_view["experience_assets"]
        ):
            return _fail("accepted retry must preserve self-attribution and self-repair evidence")

    return 0


def _accepted_orphaned_active_retry_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.liveness import build_runtime_loss_signal
    from loop_product.runtime.recover import build_retry_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_orphaned_retry_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-active-lost-001", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_retry_request(
            source_node,
            reason="the backing child transport disappeared but the same node should continue in the same workspace",
            self_attribution="child_runtime_detached",
            self_repair="restart the same implementer node from the latest durable checkpoint",
            payload={
                "runtime_loss_signal": build_runtime_loss_signal(
                    observation_kind="child_runtime_detached",
                    summary="authoritative state still showed ACTIVE, but the backing child session disappeared",
                    evidence_refs=["threads.sqlite:latest-child-thread"],
                )
            },
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"retry on an orphaned ACTIVE node must be accepted when runtime-loss evidence exists, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("accepted orphaned-active retry must keep the same node ACTIVE")
        source_runtime = next(
            (item.get("runtime_state") for item in authority["node_graph"] if item["node_id"] == source_node.node_id),
            None,
        )
        if not isinstance(source_runtime, dict):
            return _fail("authority view must expose runtime_state for recoverable nodes")
        if str(source_runtime.get("attachment_state") or "") != "UNOBSERVED":
            return _fail("accepted orphaned-active retry must reset runtime attachment to UNOBSERVED until the relaunched child heartbeats")
        if "child_runtime_detached" not in str(source_runtime.get("summary") or ""):
            return _fail("accepted orphaned-active retry must preserve the runtime-loss reason in runtime_state summary")

    return 0


def _accepted_relaunch_case() -> int:
    from loop_product.kernel.audit import query_audit_experience_view
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_relaunch_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_relaunch_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-relaunch-001", status="BLOCKED")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="BLOCKED",
            blocked_reason="staged workspace is inconsistent and needs a fresh launch",
        )
        mutation = build_relaunch_request(
            source_node,
            replacement_node_id="child-relaunch-001-v2",
            reason="materialize a fresh replacement node with refreshed execution policy",
            self_attribution="staged_workspace_corruption",
            self_repair="launch a fresh child from durable delegation instead of blaming implementation",
            execution_policy={"sandbox_mode": "workspace-write", "retry_policy": "single_retry"},
            reasoning_profile={"thinking_budget": "high", "role": "implementer"},
            budget_profile={"max_rounds": 3},
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"relaunch request must be accepted with a replacement node spec, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        replacement = next(
            (item for item in authority["node_graph"] if item["node_id"] == "child-relaunch-001-v2"),
            None,
        )
        if replacement is None:
            return _fail("accepted relaunch must materialize a replacement child node")
        if replacement["parent_node_id"] != "root-kernel":
            return _fail("replacement child must preserve the original parent")
        if replacement["status"] != "ACTIVE":
            return _fail("replacement child must become ACTIVE immediately after accepted relaunch")
        if authority["node_lifecycle"].get(source_node.node_id) != "FAILED":
            return _fail("accepted relaunch must freeze the superseded source node as FAILED")

        replacement_state = state_root / "state" / "child-relaunch-001-v2.json"
        replacement_delegation = state_root / "state" / "delegations" / "child-relaunch-001-v2.json"
        if not replacement_state.exists() or not replacement_delegation.exists():
            return _fail("accepted relaunch must persist both replacement node state and delegation artifacts")

        audit_view = query_audit_experience_view(state_root, node_id=source_node.node_id, limit=10)
        if not any("relaunch" in str(item.get("event_type") or "").lower() for item in audit_view["recent_structural_decisions"]):
            return _fail("accepted relaunch must appear in recent structural decisions")

    return 0


def _accepted_orphaned_active_relaunch_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.liveness import build_runtime_loss_signal
    from loop_product.runtime.recover import build_relaunch_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_orphaned_relaunch_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-active-lost-002", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_relaunch_request(
            source_node,
            replacement_node_id="child-active-lost-002-v2",
            reason="the same task must continue, but the dead runtime should be replaced with a fresh child",
            self_attribution="child_runtime_detached",
            self_repair="materialize a fresh child node from durable delegation after the backing runtime vanished",
            payload={
                "runtime_loss_signal": build_runtime_loss_signal(
                    observation_kind="child_runtime_detached",
                    summary="root supervision observed the child session vanish while node status remained ACTIVE",
                    evidence_refs=["child_session_ref:missing"],
                )
            },
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"relaunch on an orphaned ACTIVE node must be accepted when runtime-loss evidence exists, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "FAILED":
            return _fail("accepted orphaned-active relaunch must freeze the lost source node as FAILED")
        replacement = next(
            (item for item in authority["node_graph"] if item["node_id"] == "child-active-lost-002-v2"),
            None,
        )
        if replacement is None or replacement["status"] != "ACTIVE":
            return _fail("accepted orphaned-active relaunch must materialize an ACTIVE replacement node")
        source_runtime = next(
            (item.get("runtime_state") for item in authority["node_graph"] if item["node_id"] == source_node.node_id),
            None,
        )
        if not isinstance(source_runtime, dict) or str(source_runtime.get("attachment_state") or "") != "LOST":
            return _fail("accepted orphaned-active relaunch must preserve the source node's LOST runtime attachment state")

    return 0


def _rejected_resume_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_resume_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_resume_reject_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-active-001", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_resume_request(
            source_node,
            reason="this should be rejected because the node is already active",
            consistency_signal="manual_override",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"resume on an already-active node must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get(source_node.node_id) != "ACTIVE":
            return _fail("rejected recovery request must not mutate the source node lifecycle state")
        if any(node["node_id"] != "root-kernel" and node["node_id"] != source_node.node_id for node in authority["node_graph"]):
            return _fail("rejected recovery request must not materialize extra child nodes")

    return 0


def _rejected_orphaned_active_retry_without_loss_signal() -> int:
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.runtime.recover import build_retry_request

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_orphaned_retry_reject_") as td:
        state_root = Path(td) / ".loop"
        source_node = _make_source_node(node_id="child-active-002", status="ACTIVE")
        _persist_base_state(
            state_root,
            source_node_id=source_node.node_id,
            source_status="ACTIVE",
        )
        mutation = build_retry_request(
            source_node,
            reason="this should be rejected because an ACTIVE node needs explicit runtime-loss evidence before retry",
            self_attribution="child_runtime_detached",
            self_repair="restart the same node",
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=source_node.round_id,
            generation=source_node.generation,
        )
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"retry on an ACTIVE node without runtime-loss evidence must be rejected, got {envelope.status.value!r}")

    return 0


def main() -> int:
    try:
        for check in (
            _accepted_resume_case,
            _accepted_retry_case,
            _accepted_orphaned_active_retry_case,
            _accepted_relaunch_case,
            _accepted_orphaned_active_relaunch_case,
            _rejected_resume_case,
            _rejected_orphaned_active_retry_without_loss_signal,
        ):
            rc = check()
            if rc != 0:
                return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"runtime recovery runtime raised unexpectedly: {exc}")

    print("[loop-system-runtime-recovery] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
