#!/usr/bin/env python3
"""Validate canonical heartbeat_observed events from trusted sidecar heartbeat observation."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-canonical-heartbeat-observed-event][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    import loop_product.child_supervision_sidecar as sidecar_module
    from loop_product.event_journal import iter_committed_events

    with tempfile.TemporaryDirectory(prefix="loop_system_canonical_heartbeat_observed_event_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        launch_result_ref = state_root / "artifacts" / "launches" / "canonical-heartbeat-observed-001" / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "node_id": "canonical-heartbeat-observed-001",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        original_supervise = sidecar_module.supervise_child_until_settled
        original_persist = sidecar_module.persist_committed_supervision_result
        try:
            def _fake_supervise_child_until_settled(**kwargs):
                status_observer = kwargs.get("status_observer")
                if callable(status_observer):
                    status_observer(
                        {
                            "node_id": "canonical-heartbeat-observed-001",
                            "runtime_attachment_state": "ATTACHED",
                            "runtime_observation_kind": "trusted_runtime_status_surface",
                            "runtime_observed_at": "2026-03-24T02:00:10.000000Z",
                            "runtime_summary": "heartbeat one",
                            "runtime_evidence_refs": ["heartbeat-one"],
                            "status_result_ref": str(launch_result_ref.with_name("status-001.json")),
                            "pid_alive": True,
                        }
                    )
                    status_observer(
                        {
                            "node_id": "canonical-heartbeat-observed-001",
                            "runtime_attachment_state": "ATTACHED",
                            "runtime_observation_kind": "trusted_runtime_status_surface",
                            "runtime_observed_at": "2026-03-24T02:00:20.000000Z",
                            "runtime_summary": "heartbeat two",
                            "runtime_evidence_refs": ["heartbeat-two"],
                            "status_result_ref": str(launch_result_ref.with_name("status-002.json")),
                            "pid_alive": True,
                        }
                    )
                return {
                    "schema": "loop_product.child_supervision_result",
                    "settled": True,
                    "settled_reason": "test_complete",
                    "recoveries_used": 0,
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "status_result_ref": "",
                    "result_ref": "",
                    "final_runtime_status_ref": "",
                    "final_runtime_attachment_state": "COMPLETED",
                    "final_lifecycle_status": "COMPLETED",
                    "final_observation_kind": "test_complete",
                    "summary": "test complete",
                    "evidence_refs": [],
                    "settled_at_utc": "2026-03-24T00:00:00Z",
                }

            sidecar_module.supervise_child_until_settled = _fake_supervise_child_until_settled
            sidecar_module.persist_committed_supervision_result = lambda **_kwargs: (
                launch_result_ref.with_name("ChildSupervisionResult.json"),
                "",
            )

            exit_code = sidecar_module.main(
                [
                    "--launch-result-ref",
                    str(launch_result_ref.resolve()),
                    "--lease-epoch",
                    "12",
                    "--poll-interval-s",
                    "2.0",
                    "--stall-threshold-s",
                    "60.0",
                    "--max-recoveries",
                    "5",
                    "--no-substantive-progress-window-s",
                    "300.0",
                ]
            )
            if exit_code != 0:
                return _fail("sidecar main must succeed during canonical heartbeat_observed coverage")
        finally:
            sidecar_module.supervise_child_until_settled = original_supervise
            sidecar_module.persist_committed_supervision_result = original_persist

        heartbeat_events = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "heartbeat_observed"
        ]
        if len(heartbeat_events) != 2:
            return _fail("attached heartbeat observations must commit exactly two canonical heartbeat_observed events")
        payloads = [dict(event.get("payload") or {}) for event in heartbeat_events]
        latest = payloads[-1]
        runtime_ref = launch_result_ref.with_name("ChildSupervisionRuntime.json").resolve()
        if any(int(payload.get("lease_epoch") or 0) != 12 for payload in payloads):
            return _fail("heartbeat_observed must preserve lease epoch continuity")
        if any(str(payload.get("lease_owner_id") or "") != f"sidecar:{runtime_ref}" for payload in payloads):
            return _fail("heartbeat_observed must preserve lease owner continuity")
        if str(latest.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
            return _fail("heartbeat_observed must preserve launch identity continuity")
        if str(latest.get("status_result_ref") or "") != str(launch_result_ref.with_name("status-002.json")):
            return _fail("heartbeat_observed must preserve latest status_result_ref evidence")
        if str(latest.get("observed_at") or "") != "2026-03-24T02:00:20.000000Z":
            return _fail("heartbeat_observed must preserve observed_at continuity")
        if not str(latest.get("process_fingerprint") or "").strip():
            return _fail("heartbeat_observed must preserve process fingerprint continuity")

    print("[loop-system-canonical-heartbeat-observed-event] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
