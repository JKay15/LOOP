#!/usr/bin/env python3
"""Validate accepted-audit mirror repair over staged r59 historical roots."""

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
    print(f"[loop-system-r59-accepted-audit-mirror-repair][FAIL] {msg}", file=sys.stderr)
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
            query_real_root_accepted_audit_mirror_repair_trace_view,
            query_real_root_recovery_action_safety_matrix_view,
        )
        from loop_product.runtime import (
            materialize_real_root_recovery_rehearsal,
            repair_real_root_accepted_audit_mirror,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    if not CHECKPOINT_REF.exists():
        return _fail(f"missing frozen r59 checkpoint {CHECKPOINT_REF}")

    with temporary_repo_root(prefix="loop_system_r59_accepted_audit_mirror_repair_") as repo_root:
        materialized = materialize_real_root_recovery_rehearsal(
            checkpoint_ref=CHECKPOINT_REF,
            repo_root=repo_root,
        )
        state_root = Path(str(materialized.get("runtime_root") or "")).resolve()

        before_commit = query_commit_state_view(state_root, include_heavy_object_summaries=False)
        if str(before_commit.get("phase") or "") == "accepted_not_committed":
            return _fail("fresh staged r59 root may no longer regress to accepted_not_committed")
        before_accepted_only = int(before_commit.get("accepted_only_count") or 0)
        if before_accepted_only != 0:
            return _fail("fresh staged r59 root must already preserve a cleared accepted-only gap")
        before_mirror_events = _count_committed_event_type(state_root, "control_envelope_accepted")

        receipt = repair_real_root_accepted_audit_mirror(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            include_post_recovery_truth=False,
        )
        if str(receipt.get("status") or "") != "noop":
            return _fail(f"first repair pass must report noop on the already-repaired staged root, got {receipt.get('status')!r}")
        if int(receipt.get("accepted_only_count_before") or 0) != before_accepted_only:
            return _fail("repair receipt must preserve the before accepted-only count")
        if int(receipt.get("accepted_only_count_after") or 0) != 0:
            return _fail("repair receipt must clear the accepted-only gap")
        if int(receipt.get("duplicate_envelope_id_count") or 0) <= 0:
            return _fail("historical r59 repair must report duplicate accepted envelope ids")
        if int(receipt.get("mirrored_envelope_count") or 0) != 0:
            return _fail("noop mirror repair may not mirror new accepted envelopes")
        if bool(receipt.get("accepted_only_gap_cleared")):
            return _fail("noop mirror repair may not claim it freshly cleared an accepted-only gap")
        repair_event_id = str(receipt.get("repair_result_event_id") or "")
        if not repair_event_id:
            return _fail("repair receipt must include a canonical repair result event id")

        after_commit = query_commit_state_view(state_root, include_heavy_object_summaries=False)
        if int(after_commit.get("accepted_only_count") or 0) != 0:
            return _fail(
                "repair must clear the accepted-only gap in commit-state truth, "
                f"got {after_commit.get('accepted_only_count')!r}"
            )
        if str(after_commit.get("phase") or "") == "accepted_not_committed":
            return _fail("repair must move commit-state beyond accepted_not_committed")
        crash_summary = dict(after_commit.get("crash_consistency_summary") or {})
        if str(crash_summary.get("matrix_state") or "") == "accepted_not_committed":
            return _fail("repair must clear accepted_not_committed crash-consistency truth")

        after_mirror_events = _count_committed_event_type(state_root, "control_envelope_accepted")
        mirrored_delta = after_mirror_events - before_mirror_events
        if mirrored_delta != int(receipt.get("mirrored_envelope_count") or 0):
            return _fail(
                "repair must only add exactly the mirrored control_envelope_accepted facts, "
                f"expected delta={receipt.get('mirrored_envelope_count')!r} observed={mirrored_delta!r}"
            )

        action_safety = query_real_root_recovery_action_safety_matrix_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
        )
        if "projection_crash_window_open:accepted_not_committed" in list(
            action_safety.get("action_safety_blockers") or []
        ):
            return _fail("action-safety truth may not preserve the accepted_not_committed blocker after repair")

        trace = query_real_root_accepted_audit_mirror_repair_trace_view(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            event_id=repair_event_id,
            current_commit_state=after_commit,
            current_action_safety_matrix=action_safety,
        )
        repair_result = dict(trace.get("repair_result") or {})
        if str(repair_result.get("event_id") or "") != repair_event_id:
            return _fail("repair trace must expose the requested repair result event")
        if bool(repair_result.get("accepted_only_gap_cleared")):
            return _fail("noop repair trace may not claim it cleared a fresh accepted-only gap")
        current_commit = dict(trace.get("current_commit_state") or {})
        if int(current_commit.get("accepted_only_count") or 0) != 0:
            return _fail("repair trace must show cleared current accepted-only count")

        second_receipt = repair_real_root_accepted_audit_mirror(
            state_root,
            checkpoint_ref=CHECKPOINT_REF,
            include_post_recovery_truth=False,
        )
        if str(second_receipt.get("status") or "") != "noop":
            return _fail(f"second repair pass must report noop, got {second_receipt.get('status')!r}")
        if int(second_receipt.get("mirrored_envelope_count") or 0) != 0:
            return _fail("idempotent second repair pass may not mirror any new accepted envelopes")
        if int(second_receipt.get("accepted_only_count_after") or 0) != 0:
            return _fail("idempotent second repair pass must preserve the cleared accepted-only gap")
        final_mirror_events = _count_committed_event_type(state_root, "control_envelope_accepted")
        if final_mirror_events != after_mirror_events:
            return _fail("idempotent second repair pass may not add new control_envelope_accepted facts")

    print(
        "[loop-system-r59-accepted-audit-mirror-repair][OK] accepted-audit mirror repair stays truthful and "
        "idempotent once the staged r59 root already preserves the cleared accepted-only gap"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
