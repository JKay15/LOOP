#!/usr/bin/env python3
"""Validate journal-side writer fencing for mixed-version legacy inserts."""

from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-writer-fencing][FAIL] {msg}", file=sys.stderr)
    return 2


def _legacy_insert_without_generation(
    db_ref: Path,
    *,
    runtime_root: Path,
    event_id: str,
    command_id: str,
) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute(
            """
            INSERT INTO committed_events (
                event_id,
                command_id,
                runtime_root,
                node_id,
                event_type,
                recorded_at,
                producer,
                schema_version,
                compatibility_floor,
                payload_json,
                causal_refs_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                command_id,
                str(runtime_root.resolve()),
                "",
                "legacy_writer_probe",
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "legacy.writer.probe",
                "0.1.0",
                "",
                "{}",
                "[]",
            ),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
            EVENT_JOURNAL_PRODUCER_GENERATION,
            commit_control_plane_event,
            ensure_event_journal,
        )
        from loop_product.kernel.query import query_event_journal_status_view
        from loop_product.kernel.state import ensure_runtime_tree
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_writer_fencing_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        status = query_event_journal_status_view(state_root)
        if str(status.get("compatibility_status") or "") != "compatible":
            return _fail("fresh journal status must report compatible")
        if str(status.get("supported_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("journal status must surface the supported producer generation")
        if str(status.get("required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("journal status must surface the required producer generation")
        if str(status.get("writer_fencing_status") or "") != "enforced":
            return _fail("fresh journal status must report enforced writer fencing")
        if int(status.get("unstamped_committed_event_count") or 0) != 0:
            return _fail("fresh journal status must start with zero unstamped committed events")

    with temporary_repo_root(prefix="loop_system_event_journal_writer_fencing_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        try:
            _legacy_insert_without_generation(
                db_ref,
                runtime_root=state_root,
                event_id="evt_legacy_probe",
                command_id="legacy_probe_command",
            )
        except sqlite3.DatabaseError as exc:
            if "writer_fencing_required_producer_generation" not in str(exc):
                return _fail(f"legacy raw insert failed for the wrong reason: {exc}")
        else:
            return _fail("legacy raw insert without producer generation must be rejected by the journal")
        post_status = query_event_journal_status_view(state_root)
        if int(post_status.get("committed_event_count") or 0) != 0:
            return _fail("rejected legacy writer probe must not leave a committed event behind")

    with temporary_repo_root(prefix="loop_system_event_journal_writer_fencing_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        committed = commit_control_plane_event(
            state_root,
            event_id="evt_current_writer_probe",
            command_id="current_writer_probe_command",
            node_id="",
            event_type="current_writer_probe",
            payload={"probe": True},
            producer="tests.current_writer.probe",
            compatibility_floor="milestone6_writer_fencing_foundation_v1",
        )
        if str(committed.get("producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("current writer commit must record the current producer generation")
        status = query_event_journal_status_view(state_root)
        if int(status.get("committed_event_count") or 0) != 1:
            return _fail("current writer probe must commit exactly one event")
        if int(status.get("unstamped_committed_event_count") or 0) != 0:
            return _fail("current writer probe must not count as an unstamped event")
        observed_generations = list(status.get("observed_producer_generations") or [])
        if observed_generations != [EVENT_JOURNAL_PRODUCER_GENERATION]:
            return _fail("journal status must report the current writer generation in observed generations")
        journal_meta = dict(status.get("journal_meta") or {})
        if str(journal_meta.get("required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("journal meta must retain the required producer generation")
        if str(journal_meta.get("event_schema_version") or "") != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
            return _fail("journal meta must retain the canonical event schema version")

    print("[loop-system-event-journal-writer-fencing][OK] journal-side writer fencing rejects legacy inserts and preserves current writer commits")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
