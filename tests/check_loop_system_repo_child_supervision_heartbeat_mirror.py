#!/usr/bin/env python3
"""Validate committed child supervision sidecar heartbeat mirroring."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-child-supervision-heartbeat-mirror][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    import loop_product.child_supervision_sidecar as sidecar_module
    from loop_product.event_journal import iter_committed_events

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_heartbeat_mirror_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        launch_result_ref = state_root / "artifacts" / "launches" / "child-supervision-heartbeat-mirror-001" / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "node_id": "child-supervision-heartbeat-mirror-001",
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
                            "node_id": "child-supervision-heartbeat-mirror-001",
                            "runtime_attachment_state": "ATTACHED",
                            "runtime_observation_kind": "trusted_runtime_status_surface",
                            "runtime_observed_at": "2026-03-24T01:00:10.000000Z",
                            "runtime_summary": "heartbeat one",
                            "runtime_evidence_refs": ["heartbeat-one"],
                            "status_result_ref": str(launch_result_ref.with_name("status-001.json")),
                            "pid_alive": True,
                        }
                    )
                    status_observer(
                        {
                            "node_id": "child-supervision-heartbeat-mirror-001",
                            "runtime_attachment_state": "ATTACHED",
                            "runtime_observation_kind": "trusted_runtime_status_surface",
                            "runtime_observed_at": "2026-03-24T01:00:20.000000Z",
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
                    "final_runtime_attachment_state": "TERMINAL",
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
                    "9",
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
                return _fail("sidecar main must succeed during heartbeat-mirror coverage")
        finally:
            sidecar_module.supervise_child_until_settled = original_supervise
            sidecar_module.persist_committed_supervision_result = original_persist

        committed = [
            event
            for event in iter_committed_events(state_root)
            if str(event.get("event_type") or "") == "runtime_liveness_observed"
        ]
        if len(committed) != 3:
            return _fail("sidecar session must commit startup attached plus two heartbeat liveness events")

        payloads = [dict(event.get("payload") or {}) for event in committed]
        latest = payloads[-1]
        if any(int(payload.get("lease_epoch") or 0) != 9 for payload in payloads):
            return _fail("heartbeat mirror must preserve sidecar lease_epoch continuity")
        if any(str(payload.get("lease_owner_id") or "") != f"sidecar:{launch_result_ref.with_name('ChildSupervisionRuntime.json').resolve()}" for payload in payloads):
            return _fail("heartbeat mirror must preserve sidecar lease owner continuity")
        if str(latest.get("status_result_ref") or "") != str(launch_result_ref.with_name("status-002.json")):
            return _fail("latest heartbeat event must preserve the latest status_result_ref evidence")
        if str(latest.get("observed_at") or "") != "2026-03-24T01:00:20.000000Z":
            return _fail("latest heartbeat event must preserve the latest observed_at timestamp")

    print("[loop-system-child-supervision-heartbeat-mirror] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
