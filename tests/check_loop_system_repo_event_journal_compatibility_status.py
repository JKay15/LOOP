#!/usr/bin/env python3
"""Validate structured compatibility reporting for damaged event-journal meta."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-compatibility-status][FAIL] {msg}", file=sys.stderr)
    return 2


def _set_meta_value(db_ref: Path, *, key: str, value_json: str) -> None:
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute("UPDATE journal_meta SET value_json = ? WHERE key = ?", (value_json, key))
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
            EVENT_JOURNAL_STORE_FORMAT_VERSION,
            ensure_event_journal,
            event_journal_db_ref,
        )
        from loop_product.kernel.query import query_event_journal_status_view
        from loop_product.kernel.state import ensure_runtime_tree
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_compatibility_status_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        healthy = query_event_journal_status_view(state_root)
        if str(healthy.get("compatibility_status") or "") != "compatible":
            return _fail("fresh journal status must report compatible")
        if str(healthy.get("supported_event_schema_version") or "") != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
            return _fail("journal status must surface the supported event schema version")
        if str(healthy.get("supported_store_format_version") or "") != EVENT_JOURNAL_STORE_FORMAT_VERSION:
            return _fail("journal status must surface the supported store format version")
        if str(healthy.get("supported_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("journal status must surface the supported producer generation")
        if str(healthy.get("required_producer_generation") or "") != EVENT_JOURNAL_PRODUCER_GENERATION:
            return _fail("journal status must surface the required producer generation")
        if str(healthy.get("writer_fencing_status") or "") != "enforced":
            return _fail("fresh journal status must report enforced writer fencing")

    with temporary_repo_root(prefix="loop_system_event_journal_compatibility_status_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        _set_meta_value(db_ref, key="event_schema_version", value_json='"unsupported-schema-v99"')
        status = query_event_journal_status_view(state_root)
        if str(status.get("compatibility_status") or "") != "incompatible":
            return _fail("journal status must report incompatible when event schema meta is unsupported")
        issues = list(status.get("compatibility_issues") or [])
        if not any(str(dict(issue).get("field") or "") == "event_schema_version" for issue in issues):
            return _fail("journal status must explain the event_schema_version incompatibility")
        if status.get("committed_event_count") is not None:
            return _fail("journal status must not claim committed event counts while compatibility is broken")

    with temporary_repo_root(prefix="loop_system_event_journal_compatibility_status_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = event_journal_db_ref(state_root)
        ensure_event_journal(state_root)
        _set_meta_value(db_ref, key="store_format_version", value_json='"unsupported-format-v99"')
        status = query_event_journal_status_view(state_root)
        if str(status.get("compatibility_status") or "") != "incompatible":
            return _fail("journal status must report incompatible when store format meta is unsupported")
        issues = list(status.get("compatibility_issues") or [])
        if not any(str(dict(issue).get("field") or "") == "store_format_version" for issue in issues):
            return _fail("journal status must explain the store_format_version incompatibility")

    with temporary_repo_root(prefix="loop_system_event_journal_compatibility_status_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        conn = sqlite3.connect(str(db_ref))
        try:
            conn.execute("UPDATE writer_fence SET required_producer_generation = ?", ("unsupported-generation-v99",))
            conn.commit()
        finally:
            conn.close()
        status = query_event_journal_status_view(state_root)
        if str(status.get("compatibility_status") or "") != "incompatible":
            return _fail("journal status must report incompatible when required producer generation is unsupported")
        issues = list(status.get("compatibility_issues") or [])
        if not any(str(dict(issue).get("field") or "") == "required_producer_generation" for issue in issues):
            return _fail("journal status must explain the required_producer_generation incompatibility")

    print("[loop-system-event-journal-compatibility-status][OK] journal status surfaces structured incompatibility without reopening authority")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
