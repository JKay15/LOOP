#!/usr/bin/env python3
"""Validate guarded writer-side stale-epoch rejection for runtime liveness events."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-liveness-writer-guard][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_guarded_runtime_liveness_observation_event,
            committed_event_count,
        )
        from loop_product.kernel import query_runtime_liveness_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_liveness_writer_guard_") as repo_root:
        state_root = repo_root / ".loop"

        epoch7_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T02:00:00.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:epoch-7",
            "pid_alive": True,
            "pid": 7007,
        }
        first = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-writer-guard-epoch-7",
            node_id="child-a",
            payload=epoch7_payload,
            producer="tests.liveness_writer_guard",
        )
        if str(first.get("outcome") or "") != "committed":
            return _fail("first higher-epoch attached observation must commit normally")
        if committed_event_count(state_root) != 1:
            return _fail("first committed observation must create exactly one journal row")

        stale_epoch6_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T02:00:05.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 6,
            "lease_owner_id": "sidecar:child-a:epoch-6",
            "pid_alive": True,
            "pid": 6006,
        }
        second = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-writer-guard-epoch-6",
            node_id="child-a",
            payload=stale_epoch6_payload,
            producer="tests.liveness_writer_guard",
        )
        if str(second.get("outcome") or "") != "rejected_stale_epoch":
            return _fail("lower-epoch attached observation must be rejected by the guarded writer")
        if int(second.get("dominant_lease_epoch") or 0) != 7:
            return _fail("stale-epoch rejection must report the dominant lease epoch")
        if committed_event_count(state_root) != 1:
            return _fail("stale-epoch rejection must not append a new journal row")

        lost_payload = {
            "attachment_state": "LOST",
            "observation_kind": "runtime_status_helper",
            "observed_at": "2026-03-24T02:00:10.000000Z",
            "pid_alive": False,
            "loss_reason": "process_exit_observed",
        }
        third = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-writer-guard-loss",
            node_id="child-a",
            payload=lost_payload,
            producer="tests.liveness_writer_guard",
        )
        if str(third.get("outcome") or "") != "committed":
            return _fail("non-live observations must still commit through the guarded writer")
        if committed_event_count(state_root) != 2:
            return _fail("non-live observation must append after stale-epoch rejection")

        view = query_runtime_liveness_view(
            state_root,
            now_utc="2026-03-24T02:00:11.000000Z",
        )
        raw_child = dict(dict(view.get("latest_runtime_liveness_by_node") or {}).get("child-a") or {})
        if str(raw_child.get("raw_attachment_state") or "") != "LOST":
            return _fail("latest raw node head must move to LOST after successful loss commit")

    print("[loop-system-event-journal-liveness-writer-guard] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
