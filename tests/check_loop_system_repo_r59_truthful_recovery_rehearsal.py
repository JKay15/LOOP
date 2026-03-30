#!/usr/bin/env python3
"""Validate truthful accepted recovery rehearsal over staged r59 roots."""

from __future__ import annotations

import json
import os
import sys
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
    print(f"[loop-system-r59-truthful-recovery-rehearsal][FAIL] {msg}", file=sys.stderr)
    return 2


def _prepare_safe_rehearsal_root(state_root: Path) -> None:
    for path in sorted((state_root / "audit").glob("envelope_*.json")):
        path.unlink()

    status_updates = {
        "arxiv_2602_11505v2_whole_paper_faithful_formalization_benchmark_r59": "FAILED",
        "nearly_monotone_mrc_formalization": "FAILED",
        "nearly_monotone_mrc_probability_formalization": "FAILED",
        "utility_learning_probabilistic_rate_chain": "FAILED",
        "whole_paper_integration_closeout": "FAILED",
    }
    kernel_ref = state_root / "state" / "kernel_state.json"
    kernel_payload = json.loads(kernel_ref.read_text(encoding="utf-8"))
    for node_id, status in status_updates.items():
        kernel_payload["nodes"][node_id]["status"] = status
        node_ref = state_root / "state" / f"{node_id}.json"
        node_payload = json.loads(node_ref.read_text(encoding="utf-8"))
        node_payload["status"] = status
        os.chmod(node_ref, 0o644)
        node_ref.write_text(json.dumps(node_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(kernel_ref, 0o644)
    kernel_ref.write_text(json.dumps(kernel_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel import query_real_root_recovery_action_safety_matrix_view
        from loop_product.runtime import (
            execute_real_root_recovery_rehearsal,
            materialize_real_root_recovery_rehearsal,
            repair_real_root_accepted_audit_mirror,
            repair_real_root_projection_adoption,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    if not CHECKPOINT_REF.exists():
        return _fail(f"missing frozen r59 checkpoint {CHECKPOINT_REF}")

    raw_target_node_id = "structural_identity_and_linear_calibration"
    prepared_target_node_id = "nearly_monotone_mrc_formalization"

    with temporary_repo_root(prefix="loop_system_r59_truthful_recovery_raw_") as repo_root:
        materialized = materialize_real_root_recovery_rehearsal(
            checkpoint_ref=CHECKPOINT_REF,
            repo_root=repo_root,
        )
        state_root = Path(str(materialized.get("runtime_root") or "")).resolve()
        preflight = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        if str(preflight.get("action_safety_readiness") or "") != "safe_to_continue_now":
            return _fail(
                "raw staged r59 preflight must now surface the single truthful safe_to_continue_now lane, "
                f"got {preflight.get('action_safety_readiness')!r}"
            )
        before_count = committed_event_count(state_root)
        receipt = execute_real_root_recovery_rehearsal(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            node_id=raw_target_node_id,
            action_id="retry_request",
            preflight_action_safety_matrix=preflight,
            include_trace=False,
        )
        after_count = committed_event_count(state_root)
        if str(receipt.get("status") or "") != "accepted":
            return _fail(f"raw staged r59 must accept the truthful safe lane, got {receipt.get('status')!r}")
        if not bool(receipt.get("recovery_executed")):
            return _fail("accepted truthful rehearsal must report recovery_executed")
        if dict(receipt.get("topology_trace") or {}):
            return _fail("execute_real_root_recovery_rehearsal must not eagerly materialize trace payloads when include_trace=False")
        if after_count <= before_count:
            return _fail("accepted truthful rehearsal must commit new command/event facts")
        if str(receipt.get("action_safety_state") or "") != "safe_to_continue_now":
            return _fail(
                "raw staged r59 must preserve safe_to_continue_now preflight truth on the accepted lane, "
                f"got {receipt.get('action_safety_state')!r}"
            )
        blockers = list(receipt.get("action_safety_blockers") or [])
        if blockers:
            return _fail(f"accepted truthful rehearsal may not report preflight blockers, got {blockers!r}")
        if list(receipt.get("candidate_action_ids") or []) != ["retry_request", "relaunch_request"]:
            return _fail("accepted truthful rehearsal must preserve retry/relaunch candidate actions")
        if str(receipt.get("preferred_action_id") or "") != "retry_request":
            return _fail("accepted truthful rehearsal must preserve retry_request as the preferred action")
        if str(receipt.get("command_kind") or "") != "retry":
            return _fail("accepted truthful rehearsal must preserve the accepted retry command kind")
        envelope_id = str(receipt.get("envelope_id") or "")
        if not envelope_id:
            return _fail("accepted truthful rehearsal must emit an accepted envelope id")

        current_action_safety = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        if str(current_action_safety.get("action_safety_readiness") or "") != "blocked_by_crash_window":
            return _fail(
                "after accepted raw-root retry, current action-safety truth should reopen a crash window, "
                f"got {current_action_safety.get('action_safety_readiness')!r}"
            )
        current_blockers = list(current_action_safety.get("action_safety_blockers") or [])
        if current_blockers != ["projection_crash_window_open:node_projection_lag"]:
            return _fail(
                "accepted raw-root truthful rehearsal must surface node_projection_lag as the post-action blocker, "
                f"got {current_blockers!r}"
            )

    with temporary_repo_root(prefix="loop_system_r59_truthful_recovery_accepted_") as repo_root:
        materialized = materialize_real_root_recovery_rehearsal(
            checkpoint_ref=CHECKPOINT_REF,
            repo_root=repo_root,
        )
        state_root = Path(str(materialized.get("runtime_root") or "")).resolve()
        accepted_receipt = repair_real_root_accepted_audit_mirror(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            include_post_recovery_truth=False,
        )
        if str(accepted_receipt.get("status") or "") not in {"noop", "repaired"}:
            return _fail("prepared staged r59 root must satisfy accepted-audit repair preconditions")
        projection_receipt = repair_real_root_projection_adoption(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            include_pre_repair_truth=False,
            include_post_repair_truth=False,
        )
        if str(projection_receipt.get("status") or "") not in {"noop", "repaired"}:
            return _fail("prepared staged r59 root must satisfy projection-adoption repair preconditions")
        _prepare_safe_rehearsal_root(state_root)
        action_safety = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        if str(action_safety.get("action_safety_readiness") or "") != "safe_to_continue_now":
            return _fail(
                "prepared staged r59 root must become safe_to_continue_now before execution, "
                f"got {action_safety.get('action_safety_readiness')!r}"
            )
        before_count = committed_event_count(state_root)
        receipt = execute_real_root_recovery_rehearsal(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            node_id=prepared_target_node_id,
            action_id="retry_request",
            preflight_action_safety_matrix=action_safety,
            include_trace=False,
        )
        after_count = committed_event_count(state_root)
        if str(receipt.get("status") or "") != "accepted":
            return _fail(f"prepared staged r59 root must accept truthful rehearsal, got {receipt.get('status')!r}")
        if not bool(receipt.get("recovery_executed")):
            return _fail("accepted truthful rehearsal must report recovery_executed")
        if dict(receipt.get("topology_trace") or {}):
            return _fail("execute_real_root_recovery_rehearsal must not eagerly materialize trace payloads when include_trace=False")
        if str(receipt.get("selected_action_id") or "") != "retry_request":
            return _fail("accepted truthful rehearsal must preserve retry_request as the selected action")
        if str(receipt.get("command_kind") or "") != "retry":
            return _fail("accepted truthful rehearsal must preserve the accepted retry command kind")
        envelope_id = str(receipt.get("envelope_id") or "")
        if not envelope_id:
            return _fail("accepted truthful rehearsal must emit an accepted envelope id")
        if after_count <= before_count:
            return _fail("accepted truthful rehearsal must commit new command/event facts")

        current_action_safety = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        if str(current_action_safety.get("action_safety_readiness") or "") != "blocked_by_crash_window":
            return _fail(
                "after accepted retry, current action-safety truth should reopen a crash window on lagging node projections, "
                f"got {current_action_safety.get('action_safety_readiness')!r}"
            )
        current_blockers = list(current_action_safety.get("action_safety_blockers") or [])
        if current_blockers != ["projection_crash_window_open:node_projection_lag"]:
            return _fail(
                "accepted truthful rehearsal must surface node_projection_lag as the post-action blocker, "
                f"got {current_blockers!r}"
            )

    print(
        "[loop-system-r59-truthful-recovery-rehearsal][OK] truthful rehearsal routes raw and prepared staged r59 roots "
        "through accepted recovery commands and preserves the post-action crash-window truth they reopen"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
