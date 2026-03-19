#!/usr/bin/env python3
"""Regression coverage for split-authority hardening bugs found in closeout review."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-split-hardening][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_base_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave13-split-hardening",
        root_goal="reject malformed or inconsistent split proposals before acceptance",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise split hardening",
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
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="deliver the bounded split surface",
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
    kernel_state.register_node(root_node)
    kernel_state.register_node(source_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _malformed_payload_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.topology import TopologyMutation

    with tempfile.TemporaryDirectory(prefix="loop_system_split_hardening_malformed_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        mutation = TopologyMutation.split(
            "child-implementer-001",
            ["child-a", "child-b"],
            reason="malformed child payload must be rejected before acceptance",
            payload={
                "target_nodes": [
                    {
                        "node_id": "child-a",
                        "goal_slice": "branch a",
                        "execution_policy": ["bad"],
                    },
                    {
                        "node_id": "child-b",
                        "goal_slice": "branch b",
                    },
                ]
            },
        )
        try:
            envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        except Exception as exc:  # noqa: BLE001
            return _fail(f"malformed split payload must fail closed before apply-time crash, got {exc!r}")
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"malformed split payload must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if authority["node_lifecycle"].get("child-implementer-001") != "ACTIVE":
            return _fail("rejected malformed split must leave the source node ACTIVE")
        if any(node["node_id"] in {"child-a", "child-b"} for node in authority["node_graph"]):
            return _fail("rejected malformed split must not materialize any child nodes")

    return 0


def _generation_drift_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.topology import TopologyMutation

    with tempfile.TemporaryDirectory(prefix="loop_system_split_hardening_generation_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        mutation = TopologyMutation.split(
            "child-implementer-001",
            ["child-a", "child-b"],
            reason="explicit child generation drift must be rejected",
            payload={
                "target_nodes": [
                    {
                        "node_id": "child-a",
                        "goal_slice": "branch a",
                        "generation": 999,
                    },
                    {
                        "node_id": "child-b",
                        "goal_slice": "branch b",
                    },
                ]
            },
        )
        envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"generation drift split must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        if any(node["node_id"] in {"child-a", "child-b"} for node in authority["node_graph"]):
            return _fail("rejected generation-drift split must not materialize child nodes")

    return 0


def _target_id_drift_case() -> int:
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.topology import TopologyMutation

    with tempfile.TemporaryDirectory(prefix="loop_system_split_hardening_target_ids_") as td:
        state_root = Path(td) / ".loop"
        _persist_base_state(state_root)
        mutation = TopologyMutation.split(
            "child-implementer-001",
            ["declared-a", "declared-b"],
            reason="payload target ids must match authoritative protocol target ids",
            payload={
                "target_nodes": [
                    {
                        "node_id": "actual-x",
                        "goal_slice": "branch x",
                    },
                    {
                        "node_id": "actual-y",
                        "goal_slice": "branch y",
                    },
                ]
            },
        )
        envelope = submit_topology_mutation(state_root, mutation, round_id="R1", generation=1)
        if envelope.status is not EnvelopeStatus.REJECTED:
            return _fail(f"target-id drift split must be rejected, got {envelope.status.value!r}")

        authority = query_authority_view(state_root)
        unexpected_ids = {"declared-a", "declared-b", "actual-x", "actual-y"}
        if any(node["node_id"] in unexpected_ids for node in authority["node_graph"]):
            return _fail("rejected target-id drift split must not materialize any declared or payload child ids")

    return 0


def main() -> int:
    try:
        for case in (
            _malformed_payload_case,
            _generation_drift_case,
            _target_id_drift_case,
        ):
            result = case()
            if result:
                return result
    except Exception as exc:  # noqa: BLE001
        return _fail(f"split hardening runtime raised unexpectedly: {exc}")

    print("[loop-system-split-hardening] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
