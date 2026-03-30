#!/usr/bin/env python3
"""Validate read-only lease-lineage trace over canonical lease facts and visible liveness state."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-lease-lineage-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _base_kernel_state():
    from loop_product.kernel.state import KernelState
    from loop_product.protocols.node import NodeSpec, NodeStatus

    kernel_state = KernelState(
        task_id="milestone4-lease-lineage-trace",
        root_goal="validate lease lineage trace",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise lease-lineage trace validation",
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
        node_id="child-lease-001",
        node_kind="implementer",
        goal_slice="exercise lease-lineage trace coverage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-lease-001.json",
        result_sink_ref="artifacts/child-lease-001/result.json",
        lineage_ref="root-kernel->child-lease-001",
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


def _commit_live_lease_lineage(state_root: Path, repo_root: Path) -> dict[str, str]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_heartbeat_observed_event,
        commit_launch_started_event,
        commit_process_identity_confirmed_event,
        commit_supervision_attached_event,
    )

    launch_result_ref = _write_json(
        state_root / "artifacts" / "launches" / "child-lease-001" / "attempt_live" / "ChildLaunchResult.json",
        {
            "launch_result_ref": str(
                (state_root / "artifacts" / "launches" / "child-lease-001" / "attempt_live" / "ChildLaunchResult.json").resolve()
            ),
            "node_id": "child-lease-001",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
    )
    status_result_ref = _write_json(
        state_root / "artifacts" / "status" / "child-lease-001" / "StatusResult.live.json",
        {"node_id": "child-lease-001", "attachment_state": "ATTACHED"},
    )
    runtime_ref = _write_json(
        state_root / "artifacts" / "sidecar" / "child-lease-001" / "ChildSupervisionRuntime.live.json",
        {"launch_result_ref": str(launch_result_ref)},
    )
    launch_event_id = str(launch_result_ref)
    lease_owner_id = f"sidecar:{runtime_ref}"

    commit_launch_started_event(
        state_root,
        observation_id="launch_started::lease_lineage_live",
        node_id="child-lease-001",
        payload={
            "launch_event_id": launch_event_id,
            "source_result_ref": str(status_result_ref),
            "launch_request_ref": str(status_result_ref.with_name("LaunchRequest.live.json")),
            "pid": 43001,
            "wrapped_argv_fingerprint": "wrapped:lease-lineage-live",
            "process_fingerprint": "process:lease-lineage-live",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
        producer="loop.tests.lease_lineage",
    )
    commit_process_identity_confirmed_event(
        state_root,
        observation_id="process_identity_confirmed::lease_lineage_live",
        node_id="child-lease-001",
        payload={
            "observed_at": "2026-03-25T13:00:00Z",
            "attachment_state": "ATTACHED",
            "observation_kind": "status_query",
            "summary": "trusted runtime-status confirmed live child identity",
            "evidence_refs": [str(status_result_ref)],
            "pid_alive": True,
            "pid": 43001,
            "process_fingerprint": "process:lease-lineage-live",
            "launch_event_id": launch_event_id,
            "status_result_ref": str(status_result_ref),
        },
        producer="loop.tests.lease_lineage",
    )
    commit_supervision_attached_event(
        state_root,
        observation_id="supervision_attached::lease_lineage_live",
        node_id="child-lease-001",
        payload={
            "observed_at": "2026-03-25T13:00:01Z",
            "lease_epoch": 3,
            "lease_owner_id": lease_owner_id,
            "pid": 53001,
            "process_fingerprint": "sidecar:lease-lineage-live",
            "launch_event_id": launch_event_id,
            "process_started_at_utc": "2026-03-25T13:00:01Z",
            "runtime_ref": str(runtime_ref),
            "evidence_refs": [str(runtime_ref)],
        },
        producer="loop.tests.lease_lineage",
    )
    commit_heartbeat_observed_event(
        state_root,
        observation_id="heartbeat_observed::lease_lineage_live",
        node_id="child-lease-001",
        payload={
            "observed_at": "2026-03-25T13:00:10Z",
            "attachment_state": "ATTACHED",
            "observation_kind": "child_supervision_heartbeat",
            "summary": "trusted sidecar heartbeat kept the lease fresh",
            "lease_epoch": 3,
            "lease_owner_id": lease_owner_id,
            "pid": 53001,
            "process_fingerprint": "sidecar:lease-lineage-live",
            "launch_event_id": launch_event_id,
            "process_started_at_utc": "2026-03-25T13:00:01Z",
            "runtime_ref": str(runtime_ref),
            "status_result_ref": str(status_result_ref),
            "evidence_refs": [str(status_result_ref), str(runtime_ref)],
        },
        producer="loop.tests.lease_lineage",
    )
    commit_guarded_runtime_liveness_observation_event(
        state_root,
        observation_id="runtime_liveness::lease_lineage_live",
        node_id="child-lease-001",
        payload={
            "attachment_state": "ATTACHED",
            "observed_at": "2026-03-25T13:00:10Z",
            "observation_kind": "child_supervision_heartbeat",
            "summary": "fresh heartbeat keeps live lease attached",
            "evidence_refs": [str(status_result_ref), str(runtime_ref)],
            "lease_owner_id": lease_owner_id,
            "lease_epoch": 3,
            "lease_expires_at": "2099-01-01T00:00:00Z",
            "pid": 43001,
            "process_fingerprint": "process:lease-lineage-live",
            "launch_event_id": launch_event_id,
            "status_result_ref": str(status_result_ref),
            "runtime_ref": str(runtime_ref),
            "process_started_at_utc": "2026-03-25T13:00:00Z",
        },
        producer="loop.tests.lease_lineage",
    )
    return {
        "node_id": "child-lease-001",
        "launch_event_id": launch_event_id,
        "lease_owner_id": lease_owner_id,
    }


def _commit_missing_heartbeat_lineage(state_root: Path, repo_root: Path) -> dict[str, str]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_launch_started_event,
        commit_process_identity_confirmed_event,
        commit_supervision_attached_event,
    )

    launch_result_ref = _write_json(
        state_root / "artifacts" / "launches" / "child-lease-001" / "attempt_gap" / "ChildLaunchResult.json",
        {
            "launch_result_ref": str(
                (state_root / "artifacts" / "launches" / "child-lease-001" / "attempt_gap" / "ChildLaunchResult.json").resolve()
            ),
            "node_id": "child-lease-001",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
    )
    status_result_ref = _write_json(
        state_root / "artifacts" / "status" / "child-lease-001" / "StatusResult.gap.json",
        {"node_id": "child-lease-001", "attachment_state": "ATTACHED"},
    )
    runtime_ref = _write_json(
        state_root / "artifacts" / "sidecar" / "child-lease-001" / "ChildSupervisionRuntime.gap.json",
        {"launch_result_ref": str(launch_result_ref)},
    )
    launch_event_id = str(launch_result_ref)
    lease_owner_id = f"sidecar:{runtime_ref}"

    commit_launch_started_event(
        state_root,
        observation_id="launch_started::lease_lineage_gap",
        node_id="child-lease-001",
        payload={
            "launch_event_id": launch_event_id,
            "source_result_ref": str(status_result_ref),
            "launch_request_ref": str(status_result_ref.with_name("LaunchRequest.gap.json")),
            "pid": 43002,
            "wrapped_argv_fingerprint": "wrapped:lease-lineage-gap",
            "process_fingerprint": "process:lease-lineage-gap",
            "workspace_root": str((repo_root / "workspace").resolve()),
            "state_root": str(state_root.resolve()),
        },
        producer="loop.tests.lease_lineage",
    )
    commit_process_identity_confirmed_event(
        state_root,
        observation_id="process_identity_confirmed::lease_lineage_gap",
        node_id="child-lease-001",
        payload={
            "observed_at": "2026-03-25T13:10:00Z",
            "attachment_state": "ATTACHED",
            "observation_kind": "status_query",
            "summary": "trusted runtime-status confirmed child identity without heartbeat follow-up",
            "evidence_refs": [str(status_result_ref)],
            "pid_alive": True,
            "pid": 43002,
            "process_fingerprint": "process:lease-lineage-gap",
            "launch_event_id": launch_event_id,
            "status_result_ref": str(status_result_ref),
        },
        producer="loop.tests.lease_lineage",
    )
    commit_supervision_attached_event(
        state_root,
        observation_id="supervision_attached::lease_lineage_gap",
        node_id="child-lease-001",
        payload={
            "observed_at": "2026-03-25T13:10:01Z",
            "lease_epoch": 4,
            "lease_owner_id": lease_owner_id,
            "pid": 53002,
            "process_fingerprint": "sidecar:lease-lineage-gap",
            "launch_event_id": launch_event_id,
            "process_started_at_utc": "2026-03-25T13:10:01Z",
            "runtime_ref": str(runtime_ref),
            "evidence_refs": [str(runtime_ref)],
        },
        producer="loop.tests.lease_lineage",
    )
    commit_guarded_runtime_liveness_observation_event(
        state_root,
        observation_id="runtime_liveness::lease_lineage_gap",
        node_id="child-lease-001",
        payload={
            "attachment_state": "ATTACHED",
            "observed_at": "2026-03-25T13:10:05Z",
            "observation_kind": "status_query",
            "summary": "attached status exists but canonical heartbeat fact is missing",
            "evidence_refs": [str(status_result_ref), str(runtime_ref)],
            "lease_owner_id": lease_owner_id,
            "lease_epoch": 4,
            "lease_expires_at": "2099-01-01T00:00:00Z",
            "pid": 43002,
            "process_fingerprint": "process:lease-lineage-gap",
            "launch_event_id": launch_event_id,
            "status_result_ref": str(status_result_ref),
            "runtime_ref": str(runtime_ref),
            "process_started_at_utc": "2026-03-25T13:10:00Z",
        },
        producer="loop.tests.lease_lineage",
    )
    return {
        "node_id": "child-lease-001",
        "launch_event_id": launch_event_id,
        "lease_owner_id": lease_owner_id,
    }


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_lease_lineage_trace_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_lease_lineage_trace_live_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_live_lease_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_lease_lineage_trace_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("lease-lineage trace must stay read-only")
        if not bool(trace.get("read_only")):
            return _fail("lease-lineage trace must declare itself read-only")
        if str(trace.get("node_id") or "") != anchor["node_id"]:
            return _fail("lease-lineage trace must preserve node identity")
        if str(trace.get("launch_event_id") or "") != anchor["launch_event_id"]:
            return _fail("lease-lineage trace must preserve launch identity continuity")
        if str(trace.get("lease_owner_id") or "") != anchor["lease_owner_id"]:
            return _fail("lease-lineage trace must preserve lease owner continuity")
        if int(trace.get("lease_epoch") or 0) != 3:
            return _fail("lease-lineage trace must preserve lease epoch continuity")
        if not bool(trace.get("launch_origin_present")):
            return _fail("lease-lineage trace must report canonical launch origin facts")
        if not bool(trace.get("process_identity_present")):
            return _fail("lease-lineage trace must report canonical process identity facts")
        if not bool(trace.get("supervision_attached_present")):
            return _fail("lease-lineage trace must report canonical supervision attachment facts")
        if not bool(trace.get("heartbeat_observed_present")):
            return _fail("lease-lineage trace must report canonical heartbeat facts for a live lease")
        if bool(trace.get("lease_expired_present")):
            return _fail("live lease lineage must not claim lease_expired")
        if str(dict(trace.get("effective_liveness_head") or {}).get("effective_attachment_state") or "") != "ATTACHED":
            return _fail("live lease lineage must expose effective attached truth")
        if list(trace.get("gaps") or []):
            return _fail("complete live lease lineage must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_lease_lineage_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_missing_heartbeat_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_lease_lineage_trace_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("lease-lineage gap trace must stay read-only")
        gaps = [str(item) for item in list(trace.get("gaps") or [])]
        if "missing_canonical_heartbeat_observed_event" not in gaps:
            return _fail("attached lease without heartbeat fact must surface an explicit heartbeat causal gap")
        if bool(trace.get("heartbeat_observed_present")):
            return _fail("gap scenario must not pretend a heartbeat fact exists")

    print("[loop-system-lease-lineage-trace] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
