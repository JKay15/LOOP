#!/usr/bin/env python3
"""Validate committed child supervision sidecar liveness mirroring."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-child-supervision-liveness-mirror][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    import loop_product.child_supervision_sidecar as sidecar_module
    from loop_product.event_journal import iter_committed_events

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_liveness_mirror_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        launch_result_ref = state_root / "artifacts" / "launches" / "child-supervision-liveness-mirror-001" / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "node_id": "child-supervision-liveness-mirror-001",
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
            sidecar_module.supervise_child_until_settled = lambda **_kwargs: {
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
            sidecar_module.persist_committed_supervision_result = lambda **_kwargs: (
                launch_result_ref.with_name("ChildSupervisionResult.json"),
                "",
            )

            exit_code = sidecar_module.main(
                [
                    "--launch-result-ref",
                    str(launch_result_ref.resolve()),
                    "--lease-epoch",
                    "7",
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
                return _fail("sidecar main must succeed during liveness-mirror coverage")

            committed = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
            ]
            if len(committed) != 1:
                return _fail("sidecar startup must commit exactly one guarded liveness event")

            event = committed[0]
            payload = dict(event.get("payload") or {})
            runtime_ref = sidecar_module.supervision_runtime_ref(launch_result_ref=launch_result_ref)
            if str(payload.get("attachment_state") or "") != "ATTACHED":
                return _fail("sidecar liveness mirror must commit ATTACHED state")
            if int(payload.get("lease_epoch") or 0) != 7:
                return _fail("sidecar liveness mirror must preserve the reserved lease_epoch")
            if str(payload.get("lease_owner_id") or "") != f"sidecar:{runtime_ref.resolve()}":
                return _fail("sidecar liveness mirror must use a sidecar-specific lease owner id")
            if str(payload.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
                return _fail("sidecar liveness mirror must preserve launch identity continuity")
            if int(payload.get("pid") or 0) != int(sidecar_module.os.getpid()):
                return _fail("sidecar liveness mirror must commit the current sidecar pid")
            if not str(payload.get("process_fingerprint") or "").strip():
                return _fail("sidecar liveness mirror must include process fingerprint evidence")
            if not str(payload.get("process_started_at_utc") or "").strip():
                return _fail("sidecar liveness mirror must include process start evidence")

            repeat_exit_code = sidecar_module.main(
                [
                    "--launch-result-ref",
                    str(launch_result_ref.resolve()),
                    "--lease-epoch",
                    "7",
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
            if repeat_exit_code != 0:
                return _fail("sidecar main must remain non-fatal when guarded liveness append is rejected as stale/conflicting")
            committed_after_repeat = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
            ]
            if len(committed_after_repeat) != 1:
                return _fail("conflicting same-epoch sidecar liveness mirror must not append a second attached event")
        finally:
            sidecar_module.supervise_child_until_settled = original_supervise
            sidecar_module.persist_committed_supervision_result = original_persist

    print("[loop-system-child-supervision-liveness-mirror] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
