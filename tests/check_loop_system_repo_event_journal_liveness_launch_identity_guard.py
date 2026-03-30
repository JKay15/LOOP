#!/usr/bin/env python3
"""Validate same-owner same-epoch same-pid same-fingerprint launch-identity rejection."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-liveness-launch-identity-guard][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_guarded_runtime_liveness_observation_event,
            committed_event_count,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_liveness_launch_identity_guard_") as repo_root:
        state_root = repo_root / ".loop"

        baseline_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T04:20:00.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:owner-a",
            "pid_alive": True,
            "pid": 7001,
            "process_fingerprint": "fp-a",
            "launch_event_id": "launch-001",
        }
        first = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-launch-identity-guard-baseline",
            node_id="child-a",
            payload=baseline_payload,
            producer="tests.liveness_launch_identity_guard",
        )
        if str(first.get("outcome") or "") != "committed":
            return _fail("baseline attached claim must commit")
        if committed_event_count(state_root) != 1:
            return _fail("baseline claim must create one committed row")

        conflicting_launch_payload = {
            **baseline_payload,
            "observed_at": "2026-03-24T04:20:05.000000Z",
            "launch_event_id": "launch-002",
        }
        second = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-launch-identity-guard-conflict",
            node_id="child-a",
            payload=conflicting_launch_payload,
            producer="tests.liveness_launch_identity_guard",
        )
        if str(second.get("outcome") or "") != "rejected_conflicting_launch_identity":
            return _fail("same-owner same-epoch same-pid same-fingerprint different-launch claim must be rejected")
        if str(second.get("dominant_launch_identity") or "") != "launch-001":
            return _fail("launch-identity rejection must report the dominant launch identity")
        if committed_event_count(state_root) != 1:
            return _fail("launch-identity rejection must not append a new journal row")

        same_launch_heartbeat = {
            **baseline_payload,
            "observed_at": "2026-03-24T04:20:06.000000Z",
        }
        third = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-launch-identity-guard-heartbeat",
            node_id="child-a",
            payload=same_launch_heartbeat,
            producer="tests.liveness_launch_identity_guard",
        )
        if str(third.get("outcome") or "") != "committed":
            return _fail("same launch identity heartbeat must remain allowed")
        if committed_event_count(state_root) != 2:
            return _fail("same launch identity heartbeat must append after launch-identity rejection")

    print("[loop-system-event-journal-liveness-launch-identity-guard] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
