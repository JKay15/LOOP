#!/usr/bin/env python3
"""Validate supervision-result persistence compatibility-mirrors non-live liveness facts."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-supervision-result-liveness-mirror][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime.lifecycle import persist_committed_supervision_result

    with tempfile.TemporaryDirectory(prefix="loop_system_supervision_result_liveness_mirror_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        launch_result_ref = (
            state_root
            / "artifacts"
            / "launches"
            / "supervision-result-liveness-mirror-001"
            / "attempt_001"
            / "ChildLaunchResult.json"
        )
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "node_id": "supervision-result-liveness-mirror-001",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(30)", "mirror-marker"],
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
                    "pid": 42424,
                    "lease_epoch": 11,
                    "started_at_utc": "2026-03-24T03:30:00.000000Z",
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
                    "node_id": "supervision-result-liveness-mirror-001",
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "runtime_attachment_state": "LOST",
                    "runtime_observation_kind": "trusted_runtime_status_surface",
                    "runtime_observed_at": "2026-03-24T03:31:00.000000Z",
                    "runtime_summary": "child runtime no longer attached",
                    "runtime_evidence_refs": ["status-evidence"],
                    "pid_alive": False,
                    "pid": 31337,
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
            "node_id": "supervision-result-liveness-mirror-001",
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
        if result_ref.resolve() != launch_result_ref.with_name("ChildSupervisionResult.json").resolve():
            return _fail("trusted supervision persistence must still write ChildSupervisionResult.json next to the launch result")

        events = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "runtime_liveness_observed"
        ]
        if len(events) != 1:
            return _fail("persisting a settled supervision result must append exactly one non-live liveness event")
        payload = dict(events[0].get("payload") or {})
        if str(payload.get("attachment_state") or "") != "LOST":
            return _fail("settled supervision mirror must preserve the non-live attachment state")
        if str(payload.get("lease_owner_id") or "") != f"sidecar:{runtime_ref.resolve()}":
            return _fail("settled supervision mirror must preserve sidecar lease owner continuity when runtime marker exists")
        if int(payload.get("lease_epoch") or 0) != 11:
            return _fail("settled supervision mirror must preserve the reserved lease epoch")
        if str(payload.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
            return _fail("settled supervision mirror must preserve launch identity continuity")
        if str(payload.get("status_result_ref") or "") != str(status_result_ref.resolve()):
            return _fail("settled supervision mirror must preserve status-result evidence")
        if str(payload.get("supervision_result_ref") or "") != str(result_ref.resolve()):
            return _fail("settled supervision mirror must point back to the committed supervision result")
        if str(payload.get("observation_kind") or "") != "child_supervision_settled:retry_budget_exhausted":
            return _fail("settled supervision mirror must expose a supervision-settled observation kind")

        persist_committed_supervision_result(
            launch_result_ref=launch_result_ref,
            result_payload=result_payload,
        )
        repeat_events = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "runtime_liveness_observed"
        ]
        if len(repeat_events) != 1:
            return _fail("re-persisting the same settled supervision result must stay idempotent")

    print("[loop-system-supervision-result-liveness-mirror] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
