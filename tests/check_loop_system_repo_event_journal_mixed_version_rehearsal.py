#!/usr/bin/env python3
"""Validate mixed-version rehearsal hardening for disabled and misaligned writer-fence truth."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-mixed-version-rehearsal][FAIL] {msg}", file=sys.stderr)
    return 2


def _set_required_generation(
    db_ref: Path,
    *,
    fence_required_generation: str,
    meta_required_generation: str,
) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute(
            "UPDATE writer_fence SET required_producer_generation = ?",
            (str(fence_required_generation),),
        )
        conn.execute(
            "UPDATE journal_meta SET value_json = ? WHERE key = 'required_producer_generation'",
            (f'"{meta_required_generation}"',),
        )
        conn.commit()
    finally:
        conn.close()


def _assert_current_writer_fails_closed(*, state_root: Path, expected_fragment: str) -> None:
    from loop_product.event_journal import commit_control_plane_event

    try:
        commit_control_plane_event(
            state_root,
            event_id="evt_mixed_version_rehearsal_probe",
            command_id="mixed_version_rehearsal_probe",
            node_id="",
            event_type="mixed_version_rehearsal_probe",
            payload={"probe": True},
            producer="tests.mixed_version_rehearsal.probe",
            compatibility_floor="milestone6_mixed_version_rehearsal_hardening_v1",
        )
    except Exception as exc:  # noqa: BLE001
        if expected_fragment not in str(exc):
            raise AssertionError(
                f"trusted writer failed for the wrong reason: expected fragment {expected_fragment!r}, got {exc!r}"
            ) from exc
    else:
        raise AssertionError("trusted current writer must fail closed for damaged mixed-version rehearsal state")


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import EVENT_JOURNAL_PRODUCER_GENERATION, ensure_event_journal
        from loop_product.kernel.query import (
            query_commit_state_view,
            query_event_journal_mixed_version_trace_view,
            query_event_journal_status_view,
            query_projection_consistency_view,
        )
        from loop_product.kernel.state import ensure_runtime_tree
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_mixed_version_rehearsal_healthy_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        status = query_event_journal_status_view(state_root)
        summary = dict(status.get("mixed_version_summary") or {})
        if str(status.get("compatibility_status") or "") != "compatible":
            return _fail("healthy journal must report compatible mixed-version truth")
        if str(status.get("writer_fencing_status") or "") != "enforced":
            return _fail("healthy journal must report enforced writer fencing")
        if str(summary.get("writer_fence_alignment_status") or "") != "aligned":
            return _fail("healthy journal must report aligned writer-fence truth")
        if str(status.get("journal_meta_required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("healthy journal must preserve the raw journal-meta required producer generation")
        if str(status.get("required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("healthy journal must preserve the writer-fence required producer generation")

    with temporary_repo_root(prefix="loop_system_event_journal_mixed_version_rehearsal_disabled_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        _set_required_generation(
            db_ref,
            fence_required_generation="",
            meta_required_generation="",
        )
        trace = query_event_journal_mixed_version_trace_view(state_root)
        summary = dict(trace.get("mixed_version_summary") or {})
        if str(trace.get("compatibility_status") or "") != "incompatible":
            return _fail("disabled writer fence must report incompatible")
        if str(trace.get("writer_fencing_status") or "") != "disabled":
            return _fail("disabled writer fence must report writer_fencing_status=disabled")
        if str(summary.get("writer_fence_alignment_status") or "") != "disabled":
            return _fail("disabled writer fence must report writer_fence_alignment_status=disabled")
        if str(summary.get("replay_guard_state") or "") != "blocked_by_incompatibility":
            return _fail("disabled writer fence must block replay truth")
        issues = list(trace.get("compatibility_issues") or [])
        if not any(
            str(dict(issue).get("field") or "") == "required_producer_generation"
            and str(dict(issue).get("reason") or "") == "missing_required_producer_generation"
            for issue in issues
        ):
            return _fail("disabled writer fence must surface missing_required_producer_generation")
        consistency = query_projection_consistency_view(state_root)
        if str(consistency.get("compatibility_status") or "") != "incompatible":
            return _fail("disabled writer fence must fail close projection consistency")
        commit_state = query_commit_state_view(state_root)
        if str(commit_state.get("phase") or "") != "incompatible_journal":
            return _fail("disabled writer fence must fail close commit-state")
        try:
            _assert_current_writer_fails_closed(
                state_root=state_root,
                expected_fragment="required_producer_generation missing",
            )
        except AssertionError as exc:
            return _fail(str(exc))

    with temporary_repo_root(prefix="loop_system_event_journal_mixed_version_rehearsal_misaligned_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        _set_required_generation(
            db_ref,
            fence_required_generation=EVENT_JOURNAL_PRODUCER_GENERATION,
            meta_required_generation="event_writer_generation_legacy_v0",
        )
        status = query_event_journal_status_view(state_root)
        summary = dict(status.get("mixed_version_summary") or {})
        if str(status.get("compatibility_status") or "") != "incompatible":
            return _fail("meta-vs-fence divergence must report incompatible")
        if str(status.get("writer_fencing_status") or "") != "misaligned":
            return _fail("meta-vs-fence divergence must report writer_fencing_status=misaligned")
        if str(summary.get("writer_fence_alignment_status") or "") != "meta_mismatch":
            return _fail("meta-vs-fence divergence must report writer_fence_alignment_status=meta_mismatch")
        if str(status.get("required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("meta-vs-fence divergence must keep the writer-fence required generation as the source of truth")
        if str(status.get("journal_meta_required_producer_generation") or "") != "event_writer_generation_legacy_v0":
            return _fail("meta-vs-fence divergence must preserve the raw journal-meta required generation")
        issues = list(status.get("compatibility_issues") or [])
        if not any(
            str(dict(issue).get("field") or "") == "journal_meta.required_producer_generation"
            and str(dict(issue).get("reason") or "") == "required_producer_generation_meta_mismatch"
            for issue in issues
        ):
            return _fail("meta-vs-fence divergence must surface journal_meta.required_producer_generation mismatch")
        trace = query_event_journal_mixed_version_trace_view(state_root)
        if str(trace.get("compatibility_status") or "") != "incompatible":
            return _fail("mixed-version trace must fail close for meta-vs-fence divergence")
        try:
            _assert_current_writer_fails_closed(
                state_root=state_root,
                expected_fragment="journal_meta required_producer_generation mismatch",
            )
        except AssertionError as exc:
            return _fail(str(exc))

    print("[loop-system-event-journal-mixed-version-rehearsal][OK] disabled and misaligned mixed-version writer-fence states fail closed and remain inspectable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
