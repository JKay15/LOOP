#!/usr/bin/env python3
"""Validate fail-closed guardrails for journal meta identity and format mismatches."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-meta-guardrails][FAIL] {msg}", file=sys.stderr)
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
        from loop_product.event_journal import ensure_event_journal, event_journal_db_ref, load_event_journal_meta
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_meta_guardrails_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)
        db_ref = ensure_event_journal(state_root)
        _ = load_event_journal_meta(state_root)

        _set_meta_value(db_ref, key="runtime_root", value_json='"bogus-runtime-root"')
        try:
            ensure_event_journal(state_root)
        except RuntimeError:
            pass
        else:
            return _fail("ensure_event_journal must fail closed when journal meta runtime_root does not match the requested runtime root")

    with temporary_repo_root(prefix="loop_system_event_journal_meta_guardrails_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)
        db_ref = event_journal_db_ref(state_root)
        ensure_event_journal(state_root)
        _set_meta_value(db_ref, key="store_format_version", value_json='"unsupported-format-v99"')
        try:
            load_event_journal_meta(state_root)
        except RuntimeError:
            pass
        else:
            return _fail("load_event_journal_meta must fail closed when store_format_version is unsupported")

    print("[loop-system-event-journal-meta-guardrails][OK] journal meta mismatches fail closed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
