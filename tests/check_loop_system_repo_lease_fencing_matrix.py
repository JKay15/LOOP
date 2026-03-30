#!/usr/bin/env python3
"""Validate the lease/fencing matrix query surface."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-lease-fencing-matrix][FAIL] {msg}", file=sys.stderr)
    return 2


def _set_required_generation(db_ref: Path, *, required_generation: str) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute(
            "UPDATE writer_fence SET required_producer_generation = ?",
            (str(required_generation),),
        )
        conn.execute(
            "UPDATE journal_meta SET value_json = ? WHERE key = 'required_producer_generation'",
            (f'"{required_generation}"',),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_guarded_runtime_liveness_observation_event,
            commit_runtime_liveness_observation_event,
            ensure_event_journal,
        )
        from loop_product.kernel import (
            query_authority_view,
            query_commit_state_view,
            query_event_journal_status_view,
            query_lease_fencing_matrix_view,
            query_projection_explanation_view,
            query_runtime_liveness_view,
        )
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_lease_fencing_matrix_") as repo_root:
        state_root = repo_root / ".loop" / "lease_fencing_matrix_runtime"
        ensure_runtime_tree(state_root)
        root_node = NodeSpec(
            node_id="root-kernel",
            node_kind="kernel",
            goal_slice="validate lease fencing matrix",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/root.json",
            lineage_ref="root-kernel",
            status=NodeStatus.ACTIVE,
        )
        child_specs = [
            NodeSpec(
                node_id="child-fresh",
                node_kind="implementer",
                goal_slice="fresh attached child",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement"],
                delegation_ref="state/delegations/child-fresh.json",
                result_sink_ref="artifacts/child-fresh/result.json",
                lineage_ref="root-kernel->child-fresh",
                status=NodeStatus.ACTIVE,
            ),
            NodeSpec(
                node_id="child-expired",
                node_kind="implementer",
                goal_slice="expired attached child",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement"],
                delegation_ref="state/delegations/child-expired.json",
                result_sink_ref="artifacts/child-expired/result.json",
                lineage_ref="root-kernel->child-expired",
                status=NodeStatus.ACTIVE,
            ),
            NodeSpec(
                node_id="child-stale-epoch",
                node_kind="implementer",
                goal_slice="stale lower epoch child",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement"],
                delegation_ref="state/delegations/child-stale-epoch.json",
                result_sink_ref="artifacts/child-stale-epoch/result.json",
                lineage_ref="root-kernel->child-stale-epoch",
                status=NodeStatus.ACTIVE,
            ),
            NodeSpec(
                node_id="child-fenced-exit",
                node_kind="implementer",
                goal_slice="fenced owner exit child",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement"],
                delegation_ref="state/delegations/child-fenced-exit.json",
                result_sink_ref="artifacts/child-fenced-exit/result.json",
                lineage_ref="root-kernel->child-fenced-exit",
                status=NodeStatus.ACTIVE,
            ),
        ]
        kernel_state = KernelState(
            task_id="lease-fencing-matrix",
            root_goal="validate lease fencing matrix surface",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        for child in child_specs:
            kernel_state.register_node(child)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="fresh-attached",
            node_id="child-fresh",
            payload={
                "attachment_state": "ATTACHED",
                "observed_at": "2026-03-27T11:00:00.000000Z",
                "lease_expires_at": "2099-01-01T00:00:00.000000Z",
                "lease_epoch": 7,
                "lease_owner_id": "sidecar:fresh",
                "pid": 7001,
                "pid_alive": True,
            },
            producer="tests.lease_fencing_matrix",
        )
        commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="expired-attached",
            node_id="child-expired",
            payload={
                "attachment_state": "ATTACHED",
                "observed_at": "2026-03-27T11:00:00.000000Z",
                "lease_expires_at": "2000-01-01T00:00:00.000000Z",
                "lease_epoch": 6,
                "lease_owner_id": "sidecar:expired",
                "pid": 6001,
                "pid_alive": True,
            },
            producer="tests.lease_fencing_matrix",
        )
        commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="stale-epoch-dominant",
            node_id="child-stale-epoch",
            payload={
                "attachment_state": "ATTACHED",
                "observed_at": "2026-03-27T11:00:00.000000Z",
                "lease_expires_at": "2099-01-01T00:00:00.000000Z",
                "lease_epoch": 7,
                "lease_owner_id": "sidecar:stale:epoch7",
                "pid": 7007,
                "pid_alive": True,
            },
            producer="tests.lease_fencing_matrix",
        )
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="stale-epoch-raw",
            node_id="child-stale-epoch",
            payload={
                "attachment_state": "ATTACHED",
                "observed_at": "2026-03-27T11:00:10.000000Z",
                "lease_expires_at": "2099-01-01T00:00:00.000000Z",
                "lease_epoch": 6,
                "lease_owner_id": "sidecar:stale:epoch6",
                "pid": 6006,
                "pid_alive": True,
            },
            producer="tests.lease_fencing_matrix",
        )
        commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="fenced-old-attached",
            node_id="child-fenced-exit",
            payload={
                "attachment_state": "ATTACHED",
                "observed_at": "2026-03-27T11:00:00.000000Z",
                "lease_expires_at": "2099-01-01T00:00:00.000000Z",
                "lease_epoch": 1,
                "lease_owner_id": "sidecar:fenced:old",
                "pid": 5001,
                "pid_alive": True,
            },
            producer="tests.lease_fencing_matrix",
        )
        commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="fenced-new-attached",
            node_id="child-fenced-exit",
            payload={
                "attachment_state": "ATTACHED",
                "observed_at": "2026-03-27T11:00:05.000000Z",
                "lease_expires_at": "2099-01-01T00:00:00.000000Z",
                "lease_epoch": 2,
                "lease_owner_id": "sidecar:fenced:new",
                "pid": 5002,
                "pid_alive": True,
            },
            producer="tests.lease_fencing_matrix",
        )
        commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="fenced-old-exit",
            node_id="child-fenced-exit",
            payload={
                "attachment_state": "LOST",
                "observed_at": "2026-03-27T11:00:10.000000Z",
                "lease_epoch": 1,
                "lease_owner_id": "sidecar:fenced:old",
                "pid": 5001,
                "pid_alive": False,
                "loss_reason": "process_exit_observed",
            },
            producer="tests.lease_fencing_matrix",
        )

        matrix = query_lease_fencing_matrix_view(state_root)
        summary = dict(matrix.get("lease_fencing_summary") or {})
        rows = {str(item.get("node_id") or ""): dict(item) for item in list(matrix.get("matrix_rows") or [])}
        if str(matrix.get("compatibility_status") or "") != "compatible":
            return _fail("lease fencing matrix must report compatible for a healthy journal")
        if str(summary.get("matrix_state") or "") != "lease_fencing_engaged":
            return _fail("lease fencing matrix must report matrix_state=lease_fencing_engaged when stale/fenced rows exist")
        if int(summary.get("authoritative_attached_node_count") or 0) != 1:
            return _fail("lease fencing matrix must count one purely authoritative attached row")
        if int(summary.get("expired_attached_rejected_node_count") or 0) != 1:
            return _fail("lease fencing matrix must count one expired attached rejection row")
        if int(summary.get("stale_lower_epoch_rejected_node_count") or 0) != 1:
            return _fail("lease fencing matrix must count one stale lower-epoch rejection row")
        if int(summary.get("fenced_owner_exit_overridden_node_count") or 0) != 1:
            return _fail("lease fencing matrix must count one fenced-owner exit override row")
        if int(summary.get("projection_nonlive_active_child_node_count") or 0) != 1:
            return _fail("lease fencing matrix must count one ACTIVE child whose effective liveness is non-live")
        if str(dict(rows.get("child-fresh") or {}).get("matrix_cell") or "") != "authoritative_attached":
            return _fail("fresh child must resolve to matrix_cell=authoritative_attached")
        if str(dict(rows.get("child-expired") or {}).get("matrix_cell") or "") != "expired_attached_rejected":
            return _fail("expired child must resolve to matrix_cell=expired_attached_rejected")
        if not bool(dict(rows.get("child-expired") or {}).get("projection_nonlive_active")):
            return _fail("expired ACTIVE child must mark projection_nonlive_active=true")
        if str(dict(rows.get("child-stale-epoch") or {}).get("matrix_cell") or "") != "stale_lower_epoch_rejected":
            return _fail("stale lower-epoch child must resolve to matrix_cell=stale_lower_epoch_rejected")
        if str(dict(rows.get("child-fenced-exit") or {}).get("matrix_cell") or "") != "fenced_owner_exit_overridden":
            return _fail("fenced exit child must resolve to matrix_cell=fenced_owner_exit_overridden")

        runtime_liveness = query_runtime_liveness_view(state_root)
        authority = query_authority_view(state_root)
        status = query_event_journal_status_view(state_root)
        commit_state = query_commit_state_view(state_root)
        explanation = query_projection_explanation_view(state_root, node_id="child-expired")
        for label, payload in (
            ("runtime liveness", runtime_liveness),
            ("authority", authority),
            ("event journal status", status),
            ("commit state", commit_state),
            ("projection explanation", explanation),
        ):
            surface_summary = dict(payload.get("lease_fencing_summary") or {})
            if str(surface_summary.get("matrix_state") or "") != str(summary.get("matrix_state") or ""):
                return _fail(f"{label} must expose the same lease_fencing_summary matrix_state as the dedicated matrix surface")
            if int(surface_summary.get("stale_lower_epoch_rejected_node_count") or 0) != int(
                summary.get("stale_lower_epoch_rejected_node_count") or 0
            ):
                return _fail(f"{label} must expose the same stale-lower-epoch count as the dedicated matrix surface")
            if int(surface_summary.get("fenced_owner_exit_overridden_node_count") or 0) != int(
                summary.get("fenced_owner_exit_overridden_node_count") or 0
            ):
                return _fail(f"{label} must expose the same fenced-owner-exit count as the dedicated matrix surface")

    with temporary_repo_root(prefix="loop_system_lease_fencing_matrix_incompatible_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        _set_required_generation(db_ref, required_generation="")
        matrix = query_lease_fencing_matrix_view(state_root)
        summary = dict(matrix.get("lease_fencing_summary") or {})
        if str(matrix.get("compatibility_status") or "") != "incompatible":
            return _fail("lease fencing matrix must fail closed for incompatible journals")
        if str(summary.get("matrix_state") or "") != "incompatible_journal":
            return _fail("lease fencing matrix must expose matrix_state=incompatible_journal when compatibility is broken")
        if str(dict(matrix.get("runtime_liveness_view") or {}).get("compatibility_status") or "") != "incompatible":
            return _fail("lease fencing matrix must preserve incompatible runtime-liveness truth")
        if str(dict(matrix.get("event_journal_status") or {}).get("compatibility_status") or "") != "incompatible":
            return _fail("lease fencing matrix must preserve incompatible event-journal status truth")

    print("[loop-system-lease-fencing-matrix] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
