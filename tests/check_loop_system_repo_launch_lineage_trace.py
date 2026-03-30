#!/usr/bin/env python3
"""Validate read-only launch lineage trace over canonical launch, identity, supervision, and liveness facts."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-launch-lineage-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_kernel_state():
    from loop_product.kernel.state import KernelState
    from loop_product.protocols.node import NodeSpec, NodeStatus

    kernel_state = KernelState(
        task_id="milestone4-launch-lineage-trace",
        root_goal="validate launch lineage trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise launch-lineage trace validation",
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
        node_id="child-launch-001",
        node_kind="implementer",
        goal_slice="exercise launch-lineage trace coverage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-launch-001.json",
        result_sink_ref="artifacts/child-launch-001/result.json",
        lineage_ref="root-kernel->child-launch-001",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(child_node)
    return kernel_state


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path.resolve()


def _persist_projection_bundle(state_root: Path, kernel_state) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import ensure_runtime_tree, persist_kernel_state, persist_node_snapshot, persist_projection_manifest

    ensure_runtime_tree(state_root)
    authority = kernel_internal_authority()
    persist_kernel_state(state_root, kernel_state, authority=authority)
    for node in kernel_state.nodes.values():
        persist_node_snapshot(state_root, node, authority=authority)
    persist_projection_manifest(state_root, kernel_state, authority=authority)


def _commit_complete_launch_lineage(state_root: Path, repo_root: Path) -> str:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_launch_started_event,
        commit_process_identity_confirmed_event,
        commit_supervision_attached_event,
    )

    launch_result_ref = _write_json(
        state_root / "artifacts" / "launches" / "child-launch-001" / "attempt_001" / "ChildLaunchResult.json",
        {
            "launch_result_ref": str(
                (state_root / "artifacts" / "launches" / "child-launch-001" / "attempt_001" / "ChildLaunchResult.json").resolve()
            ),
            "node_id": "child-launch-001",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
    )
    source_result_ref = _write_json(
        state_root / "artifacts" / "bootstrap" / "LaunchLineageSourceResult.json",
        {"node_id": "child-launch-001"},
    )
    launch_request_ref = _write_json(
        state_root / "artifacts" / "launches" / "child-launch-001" / "attempt_001" / "LaunchRequest.json",
        {"request": "launch child-launch-001"},
    )
    status_result_ref = _write_json(
        state_root / "artifacts" / "status" / "child-launch-001" / "StatusResult.json",
        {"node_id": "child-launch-001", "attachment_state": "ATTACHED"},
    )
    runtime_ref = _write_json(
        state_root / "artifacts" / "sidecar" / "child-launch-001" / "ChildSupervisionRuntime.json",
        {"launch_result_ref": str(launch_result_ref)},
    )

    launch_event_id = str(launch_result_ref)
    commit_launch_started_event(
        state_root,
        observation_id="launch_started::launch_lineage_trace",
        node_id="child-launch-001",
        payload={
            "launch_event_id": launch_event_id,
            "source_result_ref": str(source_result_ref),
            "launch_request_ref": str(launch_request_ref),
            "pid": 41234,
            "wrapped_argv_fingerprint": "wrapped:launch-lineage",
            "process_fingerprint": "process:launch-lineage",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
        producer="loop.tests.launch_lineage",
    )
    commit_guarded_runtime_liveness_observation_event(
        state_root,
        observation_id="runtime_liveness::launch_lineage_trace",
        node_id="child-launch-001",
        payload={
            "attachment_state": "ATTACHED",
            "observed_at": "2026-03-25T12:00:00Z",
            "observation_kind": "status_query",
            "summary": "trusted runtime-status observed the launched child as attached",
            "evidence_refs": [str(status_result_ref)],
            "lease_owner_id": f"status-query:{launch_event_id}",
            "lease_epoch": 1,
            "lease_expires_at": "2099-01-01T00:00:00Z",
            "pid": 41234,
            "process_fingerprint": "process:launch-lineage",
            "launch_event_id": launch_event_id,
        },
        producer="loop.tests.launch_lineage",
    )
    commit_process_identity_confirmed_event(
        state_root,
        observation_id="process_identity_confirmed::launch_lineage_trace",
        node_id="child-launch-001",
        payload={
            "observed_at": "2026-03-25T12:00:00Z",
            "attachment_state": "ATTACHED",
            "observation_kind": "status_query",
            "summary": "trusted runtime-status confirmed child identity",
            "evidence_refs": [str(status_result_ref)],
            "pid_alive": True,
            "pid": 41234,
            "process_fingerprint": "process:launch-lineage",
            "launch_event_id": launch_event_id,
            "status_result_ref": str(status_result_ref),
        },
        producer="loop.tests.launch_lineage",
    )
    commit_supervision_attached_event(
        state_root,
        observation_id="supervision_attached::launch_lineage_trace",
        node_id="child-launch-001",
        payload={
            "observed_at": "2026-03-25T12:00:01Z",
            "lease_epoch": 1,
            "lease_owner_id": f"sidecar:{runtime_ref}",
            "pid": 51234,
            "process_fingerprint": "sidecar:launch-lineage",
            "launch_event_id": launch_event_id,
            "process_started_at_utc": "2026-03-25T12:00:01Z",
            "runtime_ref": str(runtime_ref),
            "evidence_refs": [str(runtime_ref)],
        },
        producer="loop.tests.launch_lineage",
    )
    return launch_event_id


def _commit_launch_origin_only(state_root: Path, repo_root: Path) -> str:
    from loop_product.event_journal import commit_launch_started_event

    launch_result_ref = _write_json(
        state_root / "artifacts" / "launches" / "child-launch-001" / "attempt_gap" / "ChildLaunchResult.json",
        {
            "launch_result_ref": str(
                (state_root / "artifacts" / "launches" / "child-launch-001" / "attempt_gap" / "ChildLaunchResult.json").resolve()
            ),
            "node_id": "child-launch-001",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
    )
    source_result_ref = _write_json(
        state_root / "artifacts" / "bootstrap" / "LaunchLineageGapSourceResult.json",
        {"node_id": "child-launch-001"},
    )
    launch_request_ref = _write_json(
        state_root / "artifacts" / "launches" / "child-launch-001" / "attempt_gap" / "LaunchRequest.json",
        {"request": "launch child-launch-001 gap"},
    )
    launch_event_id = str(launch_result_ref)
    commit_launch_started_event(
        state_root,
        observation_id="launch_started::launch_lineage_gap",
        node_id="child-launch-001",
        payload={
            "launch_event_id": launch_event_id,
            "source_result_ref": str(source_result_ref),
            "launch_request_ref": str(launch_request_ref),
            "pid": 42222,
            "wrapped_argv_fingerprint": "wrapped:launch-lineage-gap",
            "process_fingerprint": "process:launch-lineage-gap",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
        producer="loop.tests.launch_lineage",
    )
    return launch_event_id


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_launch_lineage_trace_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_launch_lineage_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        launch_event_id = _commit_complete_launch_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_launch_lineage_trace_view(state_root, launch_event_id=launch_event_id)
        after = committed_event_count(state_root)
        if before != after:
            return _fail("launch-lineage trace must stay read-only")
        if not bool(trace.get("read_only")):
            return _fail("launch-lineage trace must declare itself read-only")
        if not bool(trace.get("launch_started_present")):
            return _fail("launch-lineage trace must report the canonical launch_started event")
        if bool(trace.get("launch_reused_existing_present")):
            return _fail("launch-lineage trace must not invent launch_reused_existing for a started launch")
        if not bool(trace.get("process_identity_present")):
            return _fail("launch-lineage trace must report the canonical process_identity_confirmed event")
        if not bool(trace.get("supervision_attached_present")):
            return _fail("launch-lineage trace must report the canonical supervision_attached event")
        if str(trace.get("launch_origin_kind") or "") != "launch_started":
            return _fail("launch-lineage trace must expose launch_started as the origin kind")
        source_node = dict(trace.get("source_node") or {})
        raw_head = dict(trace.get("raw_liveness_head") or {})
        effective_head = dict(trace.get("effective_liveness_head") or {})
        if str(trace.get("projection_visibility") or "") != "atomically_visible":
            return _fail("launch-lineage trace must expose the current projection visibility without mutating it")
        if str(source_node.get("node_id") or "") != "child-launch-001":
            return _fail("launch-lineage trace must expose the projected source node id")
        if str(source_node.get("status") or "") != "ACTIVE":
            return _fail("launch-lineage trace must expose the projected source node status")
        if str(dict(source_node.get("runtime_projection") or {}).get("effective_attachment_state") or "") != "ATTACHED":
            return _fail("launch-lineage trace must expose the effective attached runtime projection for the source node")
        if str(raw_head.get("raw_attachment_state") or "") != "ATTACHED":
            return _fail("launch-lineage trace must expose the raw attached liveness head")
        if str(effective_head.get("effective_attachment_state") or "") != "ATTACHED":
            return _fail("launch-lineage trace must expose the effective attached liveness head")
        if str(dict(raw_head.get("payload") or {}).get("launch_event_id") or "") != launch_event_id:
            return _fail("launch-lineage trace must keep raw liveness tied to the queried launch identity")
        if str(dict(trace.get("process_identity_event") or {}).get("status_result_ref") or "") == "":
            return _fail("launch-lineage trace must expose process-identity status-result evidence")
        if str(dict(trace.get("supervision_attached_event") or {}).get("runtime_ref") or "") == "":
            return _fail("launch-lineage trace must expose supervision runtime evidence")
        if list(trace.get("gaps") or []):
            return _fail("complete launch-lineage trace must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_launch_lineage_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        launch_event_id = _commit_launch_origin_only(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_launch_lineage_trace_view(state_root, launch_event_id=launch_event_id)
        after = committed_event_count(state_root)
        if before != after:
            return _fail("launch-lineage trace gap scenario must stay read-only")
        gaps = list(trace.get("gaps") or [])
        if "missing_canonical_process_identity_confirmed_event" not in gaps:
            return _fail("gap launch-lineage trace must surface missing process-identity evidence")
        if "missing_canonical_supervision_attached_event" not in gaps:
            return _fail("gap launch-lineage trace must surface missing supervision evidence")
        if "missing_runtime_liveness_head" not in gaps:
            return _fail("gap launch-lineage trace must surface missing raw runtime liveness evidence")
        if "missing_effective_runtime_liveness_head" not in gaps:
            return _fail("gap launch-lineage trace must surface missing effective runtime liveness evidence")

    print("[loop-system-launch-lineage-trace][OK] launch-lineage trace stays read-only and exposes both complete and gapped committed launch histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
