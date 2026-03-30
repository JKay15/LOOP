#!/usr/bin/env python3
"""Validate historical projection adoption repair over staged r59 roots."""

from __future__ import annotations

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
    print(f"[loop-system-r59-projection-adoption-repair][FAIL] {msg}", file=sys.stderr)
    return 2


def _count_committed_event_type(state_root: Path, event_type: str) -> int:
    from loop_product.event_journal import iter_committed_events

    wanted = str(event_type or "").strip()
    return sum(1 for event in iter_committed_events(state_root) if str(event.get("event_type") or "") == wanted)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel import (
            query_commit_state_view,
            query_projection_crash_consistency_matrix_view,
            query_real_root_recovery_action_safety_matrix_view,
        )
        from loop_product.runtime import (
            materialize_real_root_recovery_rehearsal,
            repair_real_root_accepted_audit_mirror,
            repair_real_root_projection_adoption,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    if not CHECKPOINT_REF.exists():
        return _fail(f"missing frozen r59 checkpoint {CHECKPOINT_REF}")

    with temporary_repo_root(prefix="loop_system_r59_projection_adoption_repair_") as repo_root:
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
        if str(accepted_receipt.get("status") or "") not in {"repaired", "noop"}:
            return _fail("accepted-audit mirror repair precondition must succeed before projection adoption repair")

        before_commit = query_commit_state_view(state_root, include_heavy_object_summaries=False)
        if str(before_commit.get("phase") or "") not in {"committed_not_projected", "projected"}:
            return _fail(
                "staged r59 root must stay on a truthful projected-or-committed_not_projected surface before projection adoption repair, "
                f"got {before_commit.get('phase')!r}"
            )
        if int(before_commit.get("accepted_only_count") or 0) != 0:
            return _fail("accepted-only gap must already be closed before projection adoption repair")

        before_matrix = query_projection_crash_consistency_matrix_view(state_root)
        crash_summary = dict(before_matrix.get("crash_consistency_summary") or {})
        if str(crash_summary.get("matrix_state") or "") not in {
            "committed_not_projected_blocked",
            "committed_not_projected_replayable",
            "caught_up",
        }:
            return _fail(
                "staged r59 root must stay on a truthful pre-repair crash surface before projection adoption repair, "
                f"got {crash_summary.get('matrix_state')!r}"
            )

        before_repair_events = _count_committed_event_type(state_root, "projection_adoption_repaired")
        receipt = repair_real_root_projection_adoption(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            include_pre_repair_truth=False,
        )
        if str(receipt.get("status") or "") not in {"repaired", "noop"}:
            return _fail(f"first projection adoption repair pass must report repaired or noop, got {receipt.get('status')!r}")
        if str(receipt.get("status") or "") == "repaired":
            if int(receipt.get("missing_accepted_envelope_count_before") or 0) <= 0:
                return _fail("projection adoption repair must report missing accepted envelopes before repair")
            if int(receipt.get("adopted_envelope_count") or 0) <= 0:
                return _fail("projection adoption repair must adopt at least one missing accepted envelope")
            if int(receipt.get("missing_accepted_envelope_count_after") or 0) != 0:
                return _fail("projection adoption repair must clear the missing accepted-envelope gap")
            if not bool(receipt.get("projection_gap_cleared")):
                return _fail("projection adoption repair must report projection_gap_cleared")
        else:
            if int(receipt.get("adopted_envelope_count") or 0) != 0:
                return _fail("noop projection adoption repair may not adopt new accepted envelopes")
            if int(receipt.get("missing_accepted_envelope_count_after") or 0) != 0:
                return _fail("noop projection adoption repair must preserve a cleared missing-envelope gap")
        repair_event_id = str(receipt.get("repair_result_event_id") or "")
        if not repair_event_id:
            return _fail("projection adoption repair must emit a canonical repair result event id")

        if str(receipt.get("commit_phase_after") or "") != "projected":
            return _fail(
                "projection adoption repair must close the crash window and leave phase=projected, "
                f"got {receipt.get('commit_phase_after')!r}"
            )
        if str(receipt.get("crash_matrix_state_after") or "") not in {"caught_up", "repaired_recently"}:
            return _fail(
                "projection adoption repair must close crash-consistency truth, "
                f"got {receipt.get('crash_matrix_state_after')!r}"
            )
        if str(receipt.get("replay_mode_after") or "") != "caught_up":
            return _fail("projection adoption repair must leave projection replay mode caught_up")

        after_commit = query_commit_state_view(state_root, include_heavy_object_summaries=False)
        if int(after_commit.get("accepted_only_count") or 0) != 0:
            return _fail("projection adoption repair may not reopen the accepted-only gap")
        if int(after_commit.get("committed_pending_projection_count") or 0) != 0:
            return _fail("projection adoption repair must show cleared current pending projection count")
        current_action_safety = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        if str(current_action_safety.get("action_safety_readiness") or "") == "blocked_by_crash_window":
            return _fail("projection adoption repair may not preserve blocked_by_crash_window")
        action_blockers = list(current_action_safety.get("action_safety_blockers") or [])
        if any(str(item).startswith("projection_crash_window_open:") for item in action_blockers):
            return _fail("projection adoption repair may not preserve projection_crash_window_open blockers")

        after_repair_events = _count_committed_event_type(state_root, "projection_adoption_repaired")
        if after_repair_events != before_repair_events + 1:
            return _fail("first projection adoption repair pass must add exactly one canonical repair result fact")

        second_receipt = repair_real_root_projection_adoption(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            include_pre_repair_truth=False,
            include_post_repair_truth=False,
        )
        if str(second_receipt.get("status") or "") != "noop":
            return _fail(f"second projection adoption repair pass must report noop, got {second_receipt.get('status')!r}")
        if int(second_receipt.get("adopted_envelope_count") or 0) != 0:
            return _fail("idempotent second projection adoption repair pass may not adopt new accepted envelopes")
        if int(second_receipt.get("missing_accepted_envelope_count_after") or 0) != 0:
            return _fail("idempotent second projection adoption repair pass must preserve a cleared missing-envelope gap")

    print(
        "[loop-system-r59-projection-adoption-repair][OK] projection adoption repair closes the staged r59 "
        "crash window truthfully and remains idempotent on a second pass"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
