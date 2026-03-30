#!/usr/bin/env python3
"""Guard the explicit projector boundary for trusted query self-heal."""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projector-boundary][FAIL] {msg}", file=sys.stderr)
    return 2


def _query_kernel_state_object_block(source: str) -> str:
    match = re.search(
        r"def query_kernel_state_object\(.*?\n(?P<body>(?:    .*\n)+?)\n\ndef query_kernel_state",
        source,
        re.DOTALL,
    )
    if match is None:
        raise ValueError("query_kernel_state_object source block not found")
    return match.group("body")


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_commit_state_view, query_projection_replay_trace_view
        from loop_product.kernel.state import (
            KernelState,
            ensure_runtime_tree,
            persist_kernel_state,
            persist_node_snapshot,
            query_kernel_state_object,
        )
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    state_source = (ROOT / "loop_product" / "kernel" / "state.py").read_text(encoding="utf-8")
    query_kernel_state_object_body = _query_kernel_state_object_block(state_source)
    if "rebuild_kernel_projections(" not in query_kernel_state_object_body:
        return _fail("query_kernel_state_object must route self-heal through the explicit rebuild surface")
    if "synchronize_authoritative_node_results(" in query_kernel_state_object_body:
        return _fail("query_kernel_state_object must not inline projection sync once the explicit rebuild surface exists")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projector boundary",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="projector-boundary-child-001",
        node_kind="implementer",
        goal_slice="exercise query self-heal through rebuild surface",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projector-boundary-child-001.json",
        result_sink_ref="artifacts/projector-boundary-child-001/result.json",
        lineage_ref="root-kernel->projector-boundary-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projector_boundary_") as repo_root:
        state_root = repo_root / ".loop" / "event_journal_projector_boundary_runtime"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projector-boundary",
            root_goal="validate projector boundary",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projector-boundary dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before query self-heal")
        if str(query_commit_state_view(state_root).get("phase") or "") != "committed_not_projected":
            return _fail("pre-query commit-state must expose committed_not_projected")

        healed_state = query_kernel_state_object(state_root, continue_deferred=False)
        if len(list(healed_state.accepted_envelopes or [])) != 1:
            return _fail("query self-heal through the projector boundary must preserve the mirrored accepted envelope exactly once")
        if str(query_commit_state_view(state_root).get("phase") or "") != "projected":
            return _fail("query self-heal through the projector boundary must leave commit-state projected")
        replay_trace = query_projection_replay_trace_view(state_root, node_id=child_node.node_id)
        latest_rebuild = dict(replay_trace.get("latest_projection_rebuild_result") or {})
        payload = dict(latest_rebuild.get("payload") or {})
        if str(latest_rebuild.get("status") or "") != "completed":
            return _fail("query self-heal through the projector boundary must leave a completed rebuild-result fact")
        if str(payload.get("repair_state_before") or "") != "kernel_rebuild_replayable":
            return _fail("query self-heal rebuild fact must record replayable kernel repair before self-heal")
        if str(payload.get("repair_state_after") or "") != "none":
            return _fail("query self-heal rebuild fact must record no remaining repair need after self-heal")
        if bool(payload.get("repair_cleared")) is not True:
            return _fail("query self-heal rebuild fact must record repair_cleared=True")

    print("[loop-system-event-journal-projector-boundary][OK] trusted query self-heal routes through the explicit rebuild surface")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
