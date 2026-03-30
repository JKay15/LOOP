#!/usr/bin/env python3
"""Validate Milestone 1 event-journal foundation semantics."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-foundation][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
            commit_control_plane_event,
            committed_event_count,
            ensure_event_journal,
            event_journal_db_ref,
            iter_committed_events,
        )
        from loop_product.kernel.state import ensure_runtime_tree
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_journal_foundation_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        db_ref = ensure_event_journal(state_root)
        if db_ref != event_journal_db_ref(state_root):
            return _fail("ensure_event_journal must return the canonical db path")
        if not db_ref.exists():
            return _fail("event journal db must exist after ensure_event_journal")

        first = commit_control_plane_event(
            state_root,
            event_id="evt_foundation_demo",
            command_id="cmd_foundation_demo",
            node_id="demo-node",
            event_type="demo.accepted",
            payload={"status": "ACTIVE", "demo": True},
            causal_refs=["demo:seed"],
            producer="tests.foundation",
        )
        replay = commit_control_plane_event(
            state_root,
            event_id="evt_foundation_demo",
            command_id="cmd_foundation_demo",
            node_id="demo-node",
            event_type="demo.accepted",
            payload={"status": "ACTIVE", "demo": True},
            causal_refs=["demo:seed"],
            producer="tests.foundation",
        )

        if int(first.get("seq") or 0) != 1:
            return _fail("first committed event must receive seq=1 in an empty runtime root")
        if int(replay.get("seq") or 0) != int(first.get("seq") or 0):
            return _fail("replaying the same event must return the original committed seq")
        if str(first.get("schema_version") or "") != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
            return _fail("committed events must record the journal schema version")
        if committed_event_count(state_root) != 1:
            return _fail("replaying the same event must not duplicate committed rows")

        events = iter_committed_events(state_root)
        if len(events) != 1:
            return _fail("iter_committed_events must reflect one committed event after replay")
        event = dict(events[0] or {})
        if event.get("payload") != {"demo": True, "status": "ACTIVE"}:
            return _fail("event payload must round-trip canonically through the journal")
        if event.get("causal_refs") != ["demo:seed"]:
            return _fail("event causal refs must round-trip through the journal")

    print("[loop-system-event-journal-foundation][OK] event journal commits are atomic and idempotent")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
