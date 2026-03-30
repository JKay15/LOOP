#!/usr/bin/env python3
"""Validate mixed-version guard truth across dedicated trace and generic replay/projection surfaces."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-mixed-version-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _set_required_generation(db_ref: Path, *, required_generation: str) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute("UPDATE writer_fence SET required_producer_generation = ?", (str(required_generation),))
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
        from loop_product.event_journal import EVENT_JOURNAL_PRODUCER_GENERATION, ensure_event_journal
        from loop_product.kernel.query import (
            query_commit_state_view,
            query_event_journal_mixed_version_trace_view,
            query_projection_consistency_view,
            query_projection_explanation_view,
            query_projection_replay_trace_view,
        )
        from loop_product.kernel.state import ensure_runtime_tree
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_mixed_version_trace_healthy_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        trace = query_event_journal_mixed_version_trace_view(state_root)
        summary = dict(trace.get("mixed_version_summary") or {})
        if str(trace.get("compatibility_status") or "") != "compatible":
            return _fail("healthy mixed-version trace must report compatible")
        if str(summary.get("replay_guard_state") or "") != "available":
            return _fail("healthy mixed-version trace must report replay_guard_state=available")
        if bool(summary.get("projection_truth_available")) is not True:
            return _fail("healthy mixed-version trace must report projection_truth_available=True")
        if bool(summary.get("rebuild_allowed")) is not True:
            return _fail("healthy mixed-version trace must report rebuild_allowed=True")
        if str(trace.get("required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("healthy mixed-version trace must surface the current required producer generation")

    with temporary_repo_root(prefix="loop_system_event_journal_mixed_version_trace_incompatible_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        _set_required_generation(db_ref, required_generation="unsupported-generation-v99")

        trace = query_event_journal_mixed_version_trace_view(state_root)
        summary = dict(trace.get("mixed_version_summary") or {})
        if str(trace.get("compatibility_status") or "") != "incompatible":
            return _fail("mixed-version trace must report incompatible when required producer generation is unsupported")
        if str(summary.get("replay_guard_state") or "") != "blocked_by_incompatibility":
            return _fail("mixed-version trace must report replay_guard_state=blocked_by_incompatibility")
        if bool(summary.get("projection_truth_available")) is not False:
            return _fail("mixed-version trace must report projection_truth_available=False when compatibility is broken")
        if bool(summary.get("rebuild_allowed")) is not False:
            return _fail("mixed-version trace must report rebuild_allowed=False when compatibility is broken")
        if not any(str(dict(issue).get("field") or "") == "required_producer_generation" for issue in list(trace.get("compatibility_issues") or [])):
            return _fail("mixed-version trace must preserve structured compatibility issues")

        consistency = query_projection_consistency_view(state_root)
        consistency_summary = dict(consistency.get("mixed_version_summary") or {})
        if str(consistency.get("compatibility_status") or "") != "incompatible":
            return _fail("projection consistency must fail closed with compatibility_status=incompatible")
        if str(consistency.get("visibility_state") or "") != "incompatible":
            return _fail("projection consistency must expose visibility_state=incompatible when replay truth is unavailable")
        if consistency.get("projection_replay_summary") is not None:
            return _fail("projection consistency must not pretend replay summary exists when compatibility is broken")
        if str(consistency_summary.get("replay_guard_state") or "") != "blocked_by_incompatibility":
            return _fail("projection consistency must surface the shared mixed-version replay guard summary")

        explanation = query_projection_explanation_view(state_root)
        explanation_summary = dict(explanation.get("mixed_version_summary") or {})
        if str(explanation.get("compatibility_status") or "") != "incompatible":
            return _fail("projection explanation must fail closed with compatibility_status=incompatible")
        if explanation.get("projection_replay_summary") is not None:
            return _fail("projection explanation must not pretend replay summary exists when compatibility is broken")
        if str(explanation_summary.get("replay_guard_state") or "") != "blocked_by_incompatibility":
            return _fail("projection explanation must surface the shared mixed-version replay guard summary")

        replay_trace = query_projection_replay_trace_view(state_root)
        replay_trace_summary = dict(replay_trace.get("mixed_version_summary") or {})
        if str(replay_trace.get("compatibility_status") or "") != "incompatible":
            return _fail("projection replay trace must fail closed with compatibility_status=incompatible")
        if replay_trace.get("projection_replay_summary") is not None:
            return _fail("projection replay trace must not pretend replay summary exists when compatibility is broken")
        if str(replay_trace_summary.get("replay_guard_state") or "") != "blocked_by_incompatibility":
            return _fail("projection replay trace must surface the shared mixed-version replay guard summary")

        commit_state = query_commit_state_view(state_root)
        commit_summary = dict(commit_state.get("mixed_version_summary") or {})
        if str(commit_state.get("compatibility_status") or "") != "incompatible":
            return _fail("commit-state must fail closed with compatibility_status=incompatible")
        if str(commit_state.get("phase") or "") != "incompatible_journal":
            return _fail("commit-state must expose phase=incompatible_journal when journal compatibility is broken")
        if commit_state.get("projection_replay_summary") is not None:
            return _fail("commit-state must not pretend replay summary exists when compatibility is broken")
        if str(commit_summary.get("replay_guard_state") or "") != "blocked_by_incompatibility":
            return _fail("commit-state must surface the shared mixed-version replay guard summary")

    print("[loop-system-event-journal-mixed-version-trace][OK] mixed-version guard truth blocks replay/projection surfaces consistently")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
