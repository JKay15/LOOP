#!/usr/bin/env python3
"""Validate trusted lease-expiry materialization into durable non-live liveness facts."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-lease-expiry-materialization][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import commit_runtime_liveness_observation_event, iter_committed_events
        from loop_product.kernel import query_runtime_liveness_view
        from loop_product.runtime import materialize_runtime_liveness_lease_expiry_events
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_lease_expiry_materialization_") as td:
        state_root = Path(td) / ".loop"

        child_a_attached = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_supervision_heartbeat",
            "observed_at": "2026-03-24T00:00:00.000000Z",
            "lease_duration_s": 30,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:epoch-7",
            "pid_alive": True,
            "pid": 4242,
            "launch_event_id": "launch-child-a",
            "process_fingerprint": "fp-child-a",
        }
        child_b_attached = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_supervision_heartbeat",
            "observed_at": "2026-03-24T00:00:00.000000Z",
            "lease_duration_s": 30,
            "lease_epoch": 2,
            "lease_owner_id": "sidecar:child-b:epoch-2",
            "pid_alive": True,
            "pid": 5252,
            "launch_event_id": "launch-child-b",
            "process_fingerprint": "fp-child-b",
        }
        child_b_lost = {
            "attachment_state": "LOST",
            "observation_kind": "runtime_status_helper",
            "observed_at": "2026-03-24T00:00:10.000000Z",
            "pid_alive": False,
            "pid": 5252,
            "launch_event_id": "launch-child-b",
        }
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-lease-expiry-child-a-attached",
            node_id="child-a",
            payload=child_a_attached,
            producer="tests.lease_expiry_materialization",
        )
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-lease-expiry-child-b-attached",
            node_id="child-b",
            payload=child_b_attached,
            producer="tests.lease_expiry_materialization",
        )
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-lease-expiry-child-b-lost",
            node_id="child-b",
            payload=child_b_lost,
            producer="tests.lease_expiry_materialization",
        )

        materialized = materialize_runtime_liveness_lease_expiry_events(
            state_root=state_root,
            now_utc="2026-03-24T00:00:31.000000Z",
        )
        if int(materialized.get("materialized_event_count") or 0) != 1:
            return _fail("exactly one expired attached claim must materialize into a durable non-live event")
        if list(materialized.get("materialized_node_ids") or []) != ["child-a"]:
            return _fail("only the expired attached node should materialize a lease-expired event")

        events = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "runtime_liveness_observed"
        ]
        if len(events) != 4:
            return _fail("lease-expiry materialization must append exactly one new liveness event")
        latest = dict(events[-1])
        latest_payload = dict(latest.get("payload") or {})
        if str(latest.get("node_id") or "") != "child-a":
            return _fail("lease-expired materialization must target the expired node")
        if str(latest_payload.get("attachment_state") or "") != "LOST":
            return _fail("lease-expired materialization must write a non-live LOST event")
        if str(latest_payload.get("observation_kind") or "") != "lease_expired":
            return _fail("lease-expired materialization must use observation_kind=lease_expired")
        if int(latest_payload.get("lease_epoch") or 0) != 7:
            return _fail("lease-expired materialization must preserve lease epoch continuity")
        if str(latest_payload.get("lease_owner_id") or "") != "sidecar:child-a:epoch-7":
            return _fail("lease-expired materialization must preserve lease owner continuity")
        if str(latest_payload.get("launch_event_id") or "") != "launch-child-a":
            return _fail("lease-expired materialization must preserve launch identity continuity")
        if str(latest_payload.get("expired_from_event_id") or "") != str(events[0].get("event_id") or ""):
            return _fail("lease-expired materialization must point back to the governing attached event")

        query_after = query_runtime_liveness_view(
            state_root,
            now_utc="2026-03-24T00:00:31.000000Z",
        )
        child_a_latest = dict(dict(query_after.get("latest_runtime_liveness_by_node") or {}).get("child-a") or {})
        if str(child_a_latest.get("effective_attachment_state") or "") != "LOST":
            return _fail("query after materialization must see LOST as the effective head for the expired node")
        child_b_latest = dict(dict(query_after.get("latest_runtime_liveness_by_node") or {}).get("child-b") or {})
        if str(child_b_latest.get("effective_attachment_state") or "") != "LOST":
            return _fail("existing non-live nodes must remain governed by their own non-live facts")

        repeated = materialize_runtime_liveness_lease_expiry_events(
            state_root=state_root,
            now_utc="2026-03-24T00:00:45.000000Z",
        )
        if int(repeated.get("materialized_event_count") or 0) != 0:
            return _fail("re-running lease-expiry materialization must stay idempotent")
        repeated_events = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "runtime_liveness_observed"
        ]
        if len(repeated_events) != 4:
            return _fail("repeated lease-expiry materialization must not append duplicate events")

    print("[loop-system-lease-expiry-materialization] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
