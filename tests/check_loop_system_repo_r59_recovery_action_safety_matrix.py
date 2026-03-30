#!/usr/bin/env python3
"""Validate truthful action-safety classification for the staged r59 recovery rehearsal root."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = ROOT.parent
CHECKPOINT_REF = (
    WORKSPACE_ROOT
    / ".cache"
    / "leanatlas"
    / "tmp"
    / "r59_golden_recovery_checkpoint_20260324T002141.json"
)


def _fail(msg: str) -> int:
    print(f"[loop-system-r59-recovery-action-safety-matrix][FAIL] {msg}", file=sys.stderr)
    return 2


def _safety_by_node(view: dict[str, object], node_id: str) -> dict[str, object]:
    for entry in list(view.get("node_action_safety") or []):
        payload = dict(entry or {})
        if str(payload.get("node_id") or "") == node_id:
            return payload
    return {}


def _fresh_attached_liveness_payload(*, summary: str, owner_id: str) -> dict[str, object]:
    observed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "attachment_state": "ATTACHED",
        "observation_kind": "child_supervision_heartbeat",
        "observed_at": observed_at,
        "summary": summary,
        "evidence_refs": [f"test:{owner_id}"],
        "pid_alive": True,
        "pid": 43210,
        "lease_duration_s": 3600.0,
        "lease_epoch": 9,
        "lease_owner_id": owner_id,
        "process_fingerprint": f"{owner_id}-fingerprint",
        "process_started_at_utc": observed_at,
        "runtime_ref": "test:runtime-ref",
        "status_result_ref": "test:status-result-ref",
    }


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import commit_guarded_runtime_liveness_observation_event
        from loop_product.kernel import (
            query_real_root_recovery_action_safety_matrix_view,
            query_real_root_recovery_continuation_matrix_view,
            query_real_root_recovery_rehearsal_view,
        )
        from loop_product.runtime import materialize_real_root_recovery_rehearsal
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    if not CHECKPOINT_REF.exists():
        return _fail(f"missing frozen r59 checkpoint {CHECKPOINT_REF}")

    with temporary_repo_root(prefix="loop_system_r59_recovery_action_safety_matrix_") as repo_root:
        materialized = materialize_real_root_recovery_rehearsal(
            checkpoint_ref=CHECKPOINT_REF,
            repo_root=repo_root,
            include_rebasing_summary=False,
        )
        state_root = Path(str(materialized.get("runtime_root") or "")).resolve()

        rehearsal = query_real_root_recovery_rehearsal_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        continuation_matrix = query_real_root_recovery_continuation_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_rehearsal_view=rehearsal,
        )
        matrix = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_continuation_matrix=continuation_matrix,
        )
        if str(matrix.get("compatibility_status") or "") != "compatible":
            return _fail("current staged r59 rehearsal root must preserve compatible action-safety truth")
        action_blockers = list(matrix.get("action_safety_blockers") or [])

        summary = dict(matrix.get("action_safety_summary") or {})
        if int(summary.get("safe_to_continue_now_count") or 0) != 1:
            return _fail(
                "current staged r59 rehearsal root must surface exactly one safe_to_continue_now node once global "
                f"blockers are gone, got {summary!r}"
            )
        if list(summary.get("safe_to_continue_now_node_ids") or []) != ["structural_identity_and_linear_calibration"]:
            return _fail(
                "current staged r59 rehearsal root must keep structural_identity_and_linear_calibration as the only "
                f"safe_to_continue_now node, got {summary!r}"
            )
        if str(dict(matrix.get("continuation_matrix") or {}).get("alignment_state") or "") != "generic_surfaces_aligned":
            return _fail("action safety must preserve generic_surfaces_aligned continuation truth")
        crash_summary = dict(matrix.get("crash_consistency_summary") or {})
        if bool(crash_summary.get("window_open")) and not any(
            str(item).startswith("projection_crash_window_open:")
            for item in action_blockers
        ):
            return _fail("action safety must name an open projection crash window as a blocker")
        lease_summary = dict(matrix.get("lease_fencing_summary") or {})
        if str(lease_summary.get("matrix_state") or "") == "lease_truth_drift_visible" and "lease_truth_drift_visible" not in action_blockers:
            return _fail("action safety must preserve visible lease-truth drift as a blocker")

        fresh_liveness = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="r59-action-safety-fresh-attached-utility",
            node_id="utility_learning_probabilistic_rate_chain",
            payload=_fresh_attached_liveness_payload(
                summary="fresh attached liveness should remove the node from action-safety recovery candidates",
                owner_id="test-sidecar:r59-action-safety-fresh-attached",
            ),
            producer="loop_system.tests.r59_action_safety_fresh_attached",
        )
        if str(fresh_liveness.get("outcome") or "") != "committed":
            return _fail(f"fresh attached liveness injection must commit, got {fresh_liveness!r}")

        refreshed_rehearsal = query_real_root_recovery_rehearsal_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_rebasing=rehearsal,
        )
        refreshed_continuation_matrix = query_real_root_recovery_continuation_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_rehearsal_view=refreshed_rehearsal,
        )
        refreshed_matrix = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            current_continuation_matrix=refreshed_continuation_matrix,
        )
        refreshed_summary = dict(refreshed_matrix.get("action_safety_summary") or {})
        if refreshed_summary != summary:
            return _fail(
                "fresh attached committed liveness must preserve the already-truthful action-safety summary once "
                f"utility_learning_probabilistic_rate_chain is no longer an action-safety candidate, got {refreshed_summary!r}"
            )
        refreshed_probabilistic = _safety_by_node(refreshed_matrix, "utility_learning_probabilistic_rate_chain")
        if refreshed_probabilistic:
            return _fail("fresh attached committed liveness must remove utility_learning_probabilistic_rate_chain from action-safety rows")

    print(
        "[loop-system-r59-recovery-action-safety-matrix][OK] staged r59 rehearsal root exposes truthful "
        "action-safety truth, blocker propagation, and fresh-attached candidate suppression"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
