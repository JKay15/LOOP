#!/usr/bin/env python3
"""Validate same-epoch conflicting owner rejection for guarded liveness writer."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-liveness-owner-guard][FAIL] {msg}", file=sys.stderr)
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

    with temporary_repo_root(prefix="loop_system_event_liveness_owner_guard_") as repo_root:
        state_root = repo_root / ".loop"

        owner_a_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T03:00:00.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:owner-a",
            "pid_alive": True,
            "pid": 7001,
        }
        first = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-owner-a-first",
            node_id="child-a",
            payload=owner_a_payload,
            producer="tests.liveness_owner_guard",
        )
        if str(first.get("outcome") or "") != "committed":
            return _fail("first same-epoch owner claim must commit")
        if committed_event_count(state_root) != 1:
            return _fail("first owner claim must create exactly one journal row")

        owner_b_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T03:00:05.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:owner-b",
            "pid_alive": True,
            "pid": 7002,
        }
        second = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-owner-b-conflict",
            node_id="child-a",
            payload=owner_b_payload,
            producer="tests.liveness_owner_guard",
        )
        if str(second.get("outcome") or "") != "rejected_conflicting_epoch_owner":
            return _fail("same-epoch conflicting owner claim must be rejected")
        if str(second.get("dominant_lease_owner_id") or "") != "sidecar:child-a:owner-a":
            return _fail("owner-collision rejection must report the dominant lease owner")
        if committed_event_count(state_root) != 1:
            return _fail("owner-collision rejection must not append a new journal row")

        owner_a_heartbeat_payload = {
            **owner_a_payload,
            "observed_at": "2026-03-24T03:00:06.000000Z",
        }
        third = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-owner-a-heartbeat",
            node_id="child-a",
            payload=owner_a_heartbeat_payload,
            producer="tests.liveness_owner_guard",
        )
        if str(third.get("outcome") or "") != "committed":
            return _fail("same-epoch same-owner heartbeat must remain allowed")
        if committed_event_count(state_root) != 2:
            return _fail("same-owner heartbeat must append after owner-collision rejection")

    print("[loop-system-event-journal-liveness-owner-guard] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
