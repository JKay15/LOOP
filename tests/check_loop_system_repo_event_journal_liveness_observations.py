#!/usr/bin/env python3
"""Validate journal-backed runtime liveness observation surfaces."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-liveness-observations][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_runtime_liveness_observation_event,
            committed_event_count,
            iter_committed_events,
        )
        from loop_product.kernel import query_runtime_liveness_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_liveness_obs_") as repo_root:
        state_root = repo_root / ".loop"

        attached_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "pid_alive": True,
            "pid": 4242,
            "launch_result_ref": "/tmp/runtime/launches/child-a/attempt_001/ChildLaunchResult.json",
        }
        first = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-child-a-001",
            node_id="child-a",
            payload=attached_payload,
            producer="tests.liveness",
        )
        replay = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-child-a-001",
            node_id="child-a",
            payload=attached_payload,
            producer="tests.liveness",
        )
        if int(first.get("seq") or 0) != int(replay.get("seq") or 0):
            return _fail("replaying the same liveness observation must stay idempotent")

        lost_payload = {
            "attachment_state": "LOST",
            "observation_kind": "runtime_status_helper",
            "pid_alive": False,
            "pid": 5252,
            "loss_reason": "process_exit_observed",
        }
        lost = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-child-b-001",
            node_id="child-b",
            payload=lost_payload,
            producer="tests.liveness",
        )

        terminal_payload = {
            "attachment_state": "TERMINAL",
            "observation_kind": "authoritative_result",
            "pid_alive": False,
            "result_ref": "/tmp/runtime/artifacts/child-a/result.json",
        }
        terminal = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-child-a-002",
            node_id="child-a",
            payload=terminal_payload,
            producer="tests.liveness",
        )

        if committed_event_count(state_root) != 3:
            return _fail("expected exactly three committed liveness events after replay-dedup")

        events = iter_committed_events(state_root)
        event_types = [str(item.get("event_type") or "") for item in events]
        if event_types != [
            "runtime_liveness_observed",
            "runtime_liveness_observed",
            "runtime_liveness_observed",
        ]:
            return _fail(f"unexpected liveness event types: {event_types!r}")

        view = query_runtime_liveness_view(state_root)
        if int(view.get("committed_seq") or 0) != int(terminal.get("seq") or 0):
            return _fail("runtime liveness query must expose the latest committed seq")
        if int(view.get("runtime_liveness_event_count") or 0) != 3:
            return _fail("runtime liveness query must count logical committed events")
        counts = dict(view.get("attachment_state_counts") or {})
        expected_counts = {"ATTACHED": 1, "LOST": 1, "TERMINAL": 1}
        if counts != expected_counts:
            return _fail(f"unexpected attachment_state_counts: {counts!r}")

        latest = dict(view.get("latest_runtime_liveness") or {})
        if str(latest.get("node_id") or "") != "child-a":
            return _fail("latest runtime liveness should point at the most recently observed node")
        if dict(latest.get("payload") or {}) != terminal_payload:
            return _fail("latest runtime liveness payload must equal the most recent payload")

        by_node = dict(view.get("latest_runtime_liveness_by_node") or {})
        child_a = dict(by_node.get("child-a") or {})
        child_b = dict(by_node.get("child-b") or {})
        if dict(child_a.get("payload") or {}) != terminal_payload:
            return _fail("child-a must reflect the later TERMINAL observation")
        if dict(child_b.get("payload") or {}) != lost_payload:
            return _fail("child-b must preserve the LOST observation")

        if int(child_a.get("seq") or 0) != int(terminal.get("seq") or 0):
            return _fail("child-a summary must carry the terminal event seq")
        if int(child_b.get("seq") or 0) != int(lost.get("seq") or 0):
            return _fail("child-b summary must carry the lost event seq")

    print("[loop-system-event-journal-liveness-observations] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
