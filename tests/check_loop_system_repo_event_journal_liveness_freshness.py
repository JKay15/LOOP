#!/usr/bin/env python3
"""Validate lease-aware freshness semantics for runtime liveness observations."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-liveness-freshness][FAIL] {msg}", file=sys.stderr)
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

    with temporary_repo_root(prefix="loop_system_event_liveness_freshness_") as repo_root:
        state_root = repo_root / ".loop"

        attached_payload = {
            "attachment_state": "ATTACHED",
            "observation_kind": "child_dispatch_status",
            "observed_at": "2026-03-24T00:00:00.000000Z",
            "lease_duration_s": 30,
            "lease_epoch": 7,
            "lease_owner_id": "sidecar:child-a:epoch-7",
            "pid_alive": True,
            "pid": 4242,
        }
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-freshness-child-a",
            node_id="child-a",
            payload=attached_payload,
            producer="tests.liveness_freshness",
        )

        lost_payload = {
            "attachment_state": "LOST",
            "observation_kind": "runtime_status_helper",
            "observed_at": "2026-03-24T00:00:05.000000Z",
            "pid_alive": False,
            "loss_reason": "process_exit_observed",
        }
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-freshness-child-b",
            node_id="child-b",
            payload=lost_payload,
            producer="tests.liveness_freshness",
        )

        terminal_payload = {
            "attachment_state": "TERMINAL",
            "observation_kind": "authoritative_result",
            "observed_at": "2026-03-24T00:00:06.000000Z",
            "pid_alive": False,
            "result_ref": "/tmp/runtime/artifacts/child-c/result.json",
        }
        commit_runtime_liveness_observation_event(
            state_root,
            observation_id="obs-liveness-freshness-child-c",
            node_id="child-c",
            payload=terminal_payload,
            producer="tests.liveness_freshness",
        )

        fresh_view = query_runtime_liveness_view(
            state_root,
            now_utc="2026-03-24T00:00:20.000000Z",
        )
        fresh_child = dict(dict(fresh_view.get("latest_runtime_liveness_by_node") or {}).get("child-a") or {})
        if str(fresh_child.get("raw_attachment_state") or "") != "ATTACHED":
            return _fail("fresh attached observation must expose the raw ATTACHED state")
        if str(fresh_child.get("effective_attachment_state") or "") != "ATTACHED":
            return _fail("fresh attached observation must remain effectively ATTACHED")
        if str(fresh_child.get("lease_freshness") or "") != "fresh":
            return _fail("fresh attached observation must report lease_freshness=fresh")
        if str(fresh_child.get("lease_owner_id") or "") != "sidecar:child-a:epoch-7":
            return _fail("fresh attached observation must preserve lease_owner_id")
        if int(fresh_child.get("lease_epoch") or 0) != 7:
            return _fail("fresh attached observation must preserve lease_epoch")
        if str(fresh_child.get("lease_expires_at") or "") != "2026-03-24T00:00:30.000000Z":
            return _fail("fresh attached observation must expose the computed lease expiry")
        if dict(fresh_child.get("payload") or {}) != attached_payload:
            return _fail("fresh attached query must not rewrite the committed payload")

        expired_view = query_runtime_liveness_view(
            state_root,
            now_utc="2026-03-24T00:00:31.000000Z",
        )
        expired_child = dict(dict(expired_view.get("latest_runtime_liveness_by_node") or {}).get("child-a") or {})
        if str(expired_child.get("raw_attachment_state") or "") != "ATTACHED":
            return _fail("expired attached observation must still preserve raw ATTACHED history")
        if str(expired_child.get("effective_attachment_state") or "") != "UNOBSERVED":
            return _fail("expired attached observation must degrade to effective UNOBSERVED")
        if str(expired_child.get("lease_freshness") or "") != "expired":
            return _fail("expired attached observation must report lease_freshness=expired")
        if dict(expired_child.get("payload") or {}) != attached_payload:
            return _fail("expired attached query must not rewrite the committed payload")

        lost_child = dict(dict(expired_view.get("latest_runtime_liveness_by_node") or {}).get("child-b") or {})
        if str(lost_child.get("effective_attachment_state") or "") != "LOST":
            return _fail("LOST observation must remain effectively LOST")
        if str(lost_child.get("lease_freshness") or "") != "not_applicable":
            return _fail("LOST observation must report lease_freshness=not_applicable")

        terminal_child = dict(dict(expired_view.get("latest_runtime_liveness_by_node") or {}).get("child-c") or {})
        if str(terminal_child.get("effective_attachment_state") or "") != "TERMINAL":
            return _fail("TERMINAL observation must remain effectively TERMINAL")
        if str(terminal_child.get("lease_freshness") or "") != "not_applicable":
            return _fail("TERMINAL observation must report lease_freshness=not_applicable")

    print("[loop-system-event-journal-liveness-freshness] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
