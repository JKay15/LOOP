#!/usr/bin/env python3
"""Validate canonical supervision-settled / process-exit events from trusted supervision persistence."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-supervision-exit-events][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime.lifecycle import persist_committed_supervision_result

    with tempfile.TemporaryDirectory(prefix="loop_system_supervision_exit_events_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        launch_result_ref = (
            state_root
            / "artifacts"
            / "launches"
            / "supervision-exit-events-001"
            / "attempt_001"
            / "ChildLaunchResult.json"
        )
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "node_id": "supervision-exit-events-001",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(30)", "exit-marker"],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        runtime_ref = launch_result_ref.with_name("ChildSupervisionRuntime.json")
        runtime_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_supervision_runtime",
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "pid": 53535,
                    "lease_epoch": 13,
                    "started_at_utc": "2026-03-24T04:00:00.000000Z",
                    "log_ref": str(launch_result_ref.with_name("ChildSupervision.log")),
                    "result_ref": str(launch_result_ref.with_name("ChildSupervisionResult.json")),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        status_result_ref = launch_result_ref.with_name("ChildRuntimeStatusResult.json")
        status_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "status_result_ref": str(status_result_ref.resolve()),
                    "node_id": "supervision-exit-events-001",
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "runtime_attachment_state": "LOST",
                    "runtime_observation_kind": "trusted_runtime_status_surface",
                    "runtime_observed_at": "2026-03-24T04:01:00.000000Z",
                    "runtime_summary": "child runtime no longer attached",
                    "runtime_evidence_refs": ["status-evidence"],
                    "pid_alive": False,
                    "pid": 53535,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        result_payload = {
            "launch_result_ref": str(launch_result_ref.resolve()),
            "latest_launch_result_ref": str(launch_result_ref.resolve()),
            "node_id": "supervision-exit-events-001",
            "state_root": str(state_root.resolve()),
            "workspace_root": str(workspace_root.resolve()),
            "settled": True,
            "settled_reason": "retry_budget_exhausted",
            "recoveries_used": 5,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "evaluator_verdict": "",
            "status_result_ref": str(status_result_ref.resolve()),
            "history": [],
        }

        result_ref, _sync_error = persist_committed_supervision_result(
            launch_result_ref=launch_result_ref,
            result_payload=result_payload,
        )

        events = iter_committed_events(state_root)
        settled_events = [e for e in events if str(e.get("event_type") or "") == "supervision_settled"]
        exit_events = [e for e in events if str(e.get("event_type") or "") == "process_exit_observed"]
        if len(settled_events) != 1:
            return _fail("persisted settled supervision result must append exactly one supervision_settled event")
        if len(exit_events) != 1:
            return _fail("persisted non-live supervision result must append exactly one process_exit_observed event")

        settled_payload = dict(settled_events[0].get("payload") or {})
        exit_payload = dict(exit_events[0].get("payload") or {})
        if str(settled_payload.get("settled_reason") or "") != "retry_budget_exhausted":
            return _fail("supervision_settled payload must preserve settled_reason")
        if str(settled_payload.get("supervision_result_ref") or "") != str(result_ref.resolve()):
            return _fail("supervision_settled payload must preserve supervision_result_ref")
        if str(settled_payload.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
            return _fail("supervision_settled payload must preserve launch identity")
        if int(settled_payload.get("lease_epoch") or 0) != 13:
            return _fail("supervision_settled payload must preserve lease_epoch continuity")

        if str(exit_payload.get("attachment_state") or "") != "LOST":
            return _fail("process_exit_observed payload must preserve non-live attachment state")
        if str(exit_payload.get("status_result_ref") or "") != str(status_result_ref.resolve()):
            return _fail("process_exit_observed payload must preserve status_result_ref")
        if str(exit_payload.get("supervision_result_ref") or "") != str(result_ref.resolve()):
            return _fail("process_exit_observed payload must preserve supervision_result_ref")
        if str(exit_payload.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
            return _fail("process_exit_observed payload must preserve launch identity")

        persist_committed_supervision_result(
            launch_result_ref=launch_result_ref,
            result_payload=result_payload,
        )
        repeat_events = iter_committed_events(state_root)
        if len([e for e in repeat_events if str(e.get("event_type") or "") == "supervision_settled"]) != 1:
            return _fail("re-persisting the same supervision result must keep supervision_settled idempotent")
        if len([e for e in repeat_events if str(e.get("event_type") or "") == "process_exit_observed"]) != 1:
            return _fail("re-persisting the same supervision result must keep process_exit_observed idempotent")

    print("[loop-system-supervision-exit-events] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
