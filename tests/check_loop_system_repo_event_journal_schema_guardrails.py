#!/usr/bin/env python3
"""Validate fail-closed schema-version guardrails for the event journal."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-schema-guardrails][FAIL] {msg}", file=sys.stderr)
    return 2


def _set_meta_value(db_ref: Path, *, key: str, value_json: str) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute("UPDATE journal_meta SET value_json = ? WHERE key = ?", (value_json, key))
        conn.commit()
    finally:
        conn.close()


def _set_committed_schema_version(db_ref: Path, *, seq: int, schema_version: str) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute(
            "UPDATE committed_events SET schema_version = ? WHERE seq = ?",
            (schema_version, int(seq)),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import commit_control_plane_event, ensure_event_journal, event_journal_db_ref, iter_committed_events
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_schema_guardrails_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)
        db_ref = ensure_event_journal(state_root)
        _set_meta_value(db_ref, key="event_schema_version", value_json='"unsupported-schema-v99"')
        try:
            ensure_event_journal(state_root)
        except RuntimeError:
            pass
        else:
            return _fail("ensure_event_journal must fail closed when journal meta event_schema_version is unsupported")

    with temporary_repo_root(prefix="loop_system_event_journal_schema_guardrails_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)
        ensure_event_journal(state_root)
        try:
            commit_control_plane_event(
                state_root,
                event_id="evt_schema_guard_write",
                command_id="cmd_schema_guard_write",
                node_id="demo-node",
                event_type="demo.accepted",
                payload={"demo": True},
                producer="tests.schema-guardrails",
                schema_version="unsupported-schema-v99",
            )
        except RuntimeError:
            pass
        else:
            return _fail("commit_control_plane_event must reject unsupported schema_version instead of silently committing")

    with temporary_repo_root(prefix="loop_system_event_journal_schema_guardrails_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)
        db_ref = event_journal_db_ref(state_root)
        committed = commit_control_plane_event(
            state_root,
            event_id="evt_schema_guard_read",
            command_id="cmd_schema_guard_read",
            node_id="demo-node",
            event_type="demo.accepted",
            payload={"demo": True},
            producer="tests.schema-guardrails",
        )
        _set_committed_schema_version(db_ref, seq=int(committed.get("seq") or 0), schema_version="unsupported-schema-v99")
        try:
            iter_committed_events(state_root)
        except RuntimeError:
            pass
        else:
            return _fail("iter_committed_events must fail closed when a committed row advertises an unsupported schema_version")

    print("[loop-system-event-journal-schema-guardrails][OK] schema-version mismatches fail closed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
