#!/usr/bin/env python3
"""Validate deterministic event-journal meta initialization and reload semantics."""

from __future__ import annotations

import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-meta][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
            ensure_event_journal,
            event_journal_db_ref,
            load_event_journal_meta,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_meta_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)
        db_ref = ensure_event_journal(state_root)
        if db_ref != event_journal_db_ref(state_root):
            return _fail("ensure_event_journal must return the canonical journal db ref")

        meta = load_event_journal_meta(state_root)
        if str(meta.get("runtime_root") or "") != str(state_root.resolve()):
            return _fail("journal meta must persist the authoritative runtime_root")
        if str(meta.get("event_schema_version") or "") != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
            return _fail("journal meta must persist the current event schema version")
        initialized_at = str(meta.get("initialized_at") or "")
        if not initialized_at:
            return _fail("journal meta must persist initialized_at")

        time.sleep(0.02)
        ensure_event_journal(state_root)
        meta_after_reopen = load_event_journal_meta(state_root)
        if meta_after_reopen != meta:
            return _fail("reopening an existing journal must not drift deterministic meta")

    print("[loop-system-event-journal-meta][OK] event journal meta initializes deterministically and reloads idempotently")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
