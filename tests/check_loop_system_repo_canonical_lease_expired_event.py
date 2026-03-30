#!/usr/bin/env python3
"""Validate canonical lease-expired events from trusted lease-expiry materialization."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-canonical-lease-expired-event][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import commit_runtime_liveness_observation_event, iter_committed_events
        from loop_product.runtime import materialize_runtime_liveness_lease_expiry_events
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_canonical_lease_expired_event_") as td:
        state_root = Path(td) / ".loop"
        attached_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_supervision_heartbeat",
            "observed_at": "2026-03-24T00:00:00.000000Z",
            "lease_duration_s": 30,
            "lease_epoch": 11,
            "lease_owner_id": "sidecar:lease-expired:epoch-11",
            "pid_alive": True,
            "pid": 6161,
            "launch_event_id": "launch-lease-expired-001",
            "process_fingerprint": "fp-lease-expired-001",
            "process_started_at_utc": "2026-03-24T00:00:00.000000Z",
            "status_result_ref": "status-ref-001",
            "runtime_ref": "runtime-ref-001",
            "evidence_refs": ["evidence-a"],
        }
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-canonical-lease-expired-attached",
            node_id="lease-expired-node-001",
            payload=attached_payload,
            producer="tests.canonical_lease_expired_event",
        )

        result = materialize_runtime_liveness_lease_expiry_events(
            state_root=state_root,
            now_utc="2026-03-24T00:00:31.000000Z",
        )
        if int(result.get("materialized_event_count") or 0) != 1:
            return _fail("lease-expiry materialization must still append exactly one non-live liveness event")

        events = iter_committed_events(state_root)
        lease_expired_events = [event for event in events if str(event.get("event_type") or "") == "lease_expired"]
        if len(lease_expired_events) != 1:
            return _fail("expired attached lease must append exactly one canonical lease_expired event")
        payload = dict(lease_expired_events[0].get("payload") or {})
        if str(lease_expired_events[0].get("node_id") or "") != "lease-expired-node-001":
            return _fail("lease_expired event must target the expired node")
        if int(payload.get("lease_epoch") or 0) != 11:
            return _fail("lease_expired event must preserve lease epoch continuity")
        if str(payload.get("lease_owner_id") or "") != "sidecar:lease-expired:epoch-11":
            return _fail("lease_expired event must preserve lease owner continuity")
        if str(payload.get("launch_event_id") or "") != "launch-lease-expired-001":
            return _fail("lease_expired event must preserve launch identity continuity")
        if str(payload.get("process_fingerprint") or "") != "fp-lease-expired-001":
            return _fail("lease_expired event must preserve process fingerprint continuity")
        if str(payload.get("attachment_state") or "") != "LOST":
            return _fail("lease_expired event must materialize a non-live LOST attachment state")
        if str(payload.get("expired_from_observation_kind") or "") != "child_supervision_heartbeat":
            return _fail("lease_expired event must preserve the governing attached observation kind")
        if str(payload.get("expired_from_launch_event_id") or "") != "launch-lease-expired-001":
            return _fail("lease_expired event must preserve the governing attached launch identity")
        if not str(payload.get("expired_from_event_id") or ""):
            return _fail("lease_expired event must preserve the governing attached event id")

        repeated = materialize_runtime_liveness_lease_expiry_events(
            state_root=state_root,
            now_utc="2026-03-24T00:00:45.000000Z",
        )
        if int(repeated.get("materialized_event_count") or 0) != 0:
            return _fail("re-running lease-expiry materialization must stay idempotent")
        repeated_events = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "lease_expired"
        ]
        if len(repeated_events) != 1:
            return _fail("re-running lease-expiry materialization must not append duplicate lease_expired events")

    print("[loop-system-canonical-lease-expired-event] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
