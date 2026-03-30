#!/usr/bin/env python3
"""Validate epoch-aware stale-heartbeat rejection for runtime liveness queries."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-liveness-epoch-guard][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import commit_runtime_liveness_observation_event
        from loop_product.kernel import query_runtime_liveness_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_liveness_epoch_guard_") as repo_root:
        state_root = repo_root / ".loop"

        epoch7_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T01:00:00.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:epoch-7",
            "pid_alive": True,
            "pid": 7007,
        }
        epoch7 = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-epoch-7",
            node_id="child-a",
            payload=epoch7_payload,
            producer="tests.liveness_epoch_guard",
        )

        stale_epoch6_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T01:00:10.000000Z",
            "lease_duration_s": 60,
            "lease_epoch": 6,
            "lease_owner_id": "sidecar:child-a:epoch-6",
            "pid_alive": True,
            "pid": 6006,
        }
        epoch6 = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-epoch-6-stale",
            node_id="child-a",
            payload=stale_epoch6_payload,
            producer="tests.liveness_epoch_guard",
        )

        query_before_loss = query_runtime_liveness_view(
            state_root,
            now_utc="2026-03-24T01:00:20.000000Z",
        )
        summary_before_loss = dict(query_before_loss.get("lease_fencing_summary") or {})
        raw_by_node = dict(query_before_loss.get("latest_runtime_liveness_by_node") or {})
        effective_by_node = dict(query_before_loss.get("effective_runtime_liveness_by_node") or {})

        raw_child = dict(raw_by_node.get("child-a") or {})
        effective_child = dict(effective_by_node.get("child-a") or {})
        if int(raw_child.get("seq") or 0) != int(epoch6.get("seq") or 0):
            return _fail("raw latest head must reflect the later stale lower-epoch observation")
        if int(effective_child.get("seq") or 0) != int(epoch7.get("seq") or 0):
            return _fail("effective head must stay pinned to the dominant higher attached epoch")
        if str(raw_child.get("effective_attachment_state") or "") != "UNOBSERVED":
            return _fail("stale lower-epoch raw head must not claim effective ATTACHED truth")
        if str(raw_child.get("lease_freshness") or "") != "superseded_epoch":
            return _fail("stale lower-epoch raw head must report lease_freshness=superseded_epoch")
        if str(effective_child.get("effective_attachment_state") or "") != "ATTACHED":
            return _fail("effective dominant epoch head must remain ATTACHED while fresh")
        if str(effective_child.get("lease_owner_id") or "") != "sidecar:child-a:epoch-7":
            return _fail("effective dominant epoch head must preserve the newer lease owner")
        if int(summary_before_loss.get("stale_lower_epoch_rejected_node_count") or 0) != 1:
            return _fail("runtime liveness view must count one stale lower-epoch rejection before loss")

        lost_payload = {
            "attachment_state": "LOST",
            "observation_kind": "runtime_status_helper",
            "observed_at": "2026-03-24T01:00:30.000000Z",
            "pid_alive": False,
            "loss_reason": "process_exit_observed",
        }
        lost = commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-loss-after-stale-heartbeat",
            node_id="child-a",
            payload=lost_payload,
            producer="tests.liveness_epoch_guard",
        )

        query_after_loss = query_runtime_liveness_view(
            state_root,
            now_utc="2026-03-24T01:00:31.000000Z",
        )
        summary_after_loss = dict(query_after_loss.get("lease_fencing_summary") or {})
        raw_after_loss = dict(dict(query_after_loss.get("latest_runtime_liveness_by_node") or {}).get("child-a") or {})
        effective_after_loss = dict(
            dict(query_after_loss.get("effective_runtime_liveness_by_node") or {}).get("child-a") or {}
        )
        if int(raw_after_loss.get("seq") or 0) != int(lost.get("seq") or 0):
            return _fail("raw latest head must move to the non-live loss observation")
        if int(effective_after_loss.get("seq") or 0) != int(lost.get("seq") or 0):
            return _fail("non-live latest head must dominate effective truth")
        if str(effective_after_loss.get("effective_attachment_state") or "") != "LOST":
            return _fail("latest LOST observation must dominate effective truth")
        if int(summary_after_loss.get("latest_nonlive_dominates_node_count") or 0) != 1:
            return _fail("runtime liveness view must count one latest-nonlive-dominates row after loss")

    print("[loop-system-event-journal-liveness-epoch-guard] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
