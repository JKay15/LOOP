#!/usr/bin/env python3
"""Validate the host-side child runtime-status request/response bridge."""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-host-child-runtime-status-supervisor][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module
    from loop_product import host_child_runtime_status as runtime_status_module
    from loop_product import host_child_launch_supervisor as supervisor_module

    state_root = ROOT / ".loop" / "test-host-child-runtime-status-supervisor"
    try:
        shutil.rmtree(state_root, ignore_errors=True)
        shutil.rmtree(ROOT / ".loop" / "host_child_runtime_status_requests", ignore_errors=True)
        launch_dir = state_root / "artifacts" / "launches" / "test-host-child-runtime-status-supervisor" / "attempt_001"
        launch_dir.mkdir(parents=True, exist_ok=True)
        launch_result_ref = launch_dir / "ChildLaunchResult.json"
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_decision": "started",
                    "node_id": "test-host-child-runtime-status-supervisor",
                    "workspace_root": str((ROOT / "workspace" / "test-host-child-runtime-status-supervisor").resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_request_ref": str((launch_dir / "ChildLaunchRequest.json").resolve()),
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "launch_log_dir": str((launch_dir / "logs").resolve()),
                    "stdout_ref": str((launch_dir / "logs" / "stdout.txt").resolve()),
                    "stderr_ref": str((launch_dir / "logs" / "stderr.txt").resolve()),
                    "stdin_ref": str((launch_dir / "stdin.txt").resolve()),
                    "wrapped_argv": ["codex", "exec", "-"],
                    "wrapper_cmd": "",
                    "pid": 12345,
                    "exit_code": None,
                    "startup_health_timeout_ms": 12000,
                    "startup_retry_limit": 1,
                    "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                    "attempt_count": 1,
                    "retryable_failure_kind": "",
                    "attempts": [],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        original_request = launch_runtime_module.request_host_child_runtime_status
        original_should = launch_runtime_module.should_request_host_child_runtime_status
        try:
            captured: dict[str, object] = {}

            def _fake_request(**kwargs: object) -> dict[str, object]:
                captured.update(kwargs)
                return {"lifecycle_status": "ACTIVE", "pid_alive": True}

            launch_runtime_module.request_host_child_runtime_status = _fake_request
            launch_runtime_module.should_request_host_child_runtime_status = lambda: True
            result = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref.resolve()),
                stall_threshold_s=17.5,
            )
            if not bool(result.get("pid_alive")):
                return _fail("host child runtime status request path must return the supervisor-provided status result")
            if str(captured.get("launch_result_ref") or "") != str(launch_result_ref.resolve()):
                return _fail("host child runtime status request path must preserve the exact launch result ref")
            if float(captured.get("stall_threshold_s") or -1.0) != 17.5:
                return _fail("host child runtime status request path must preserve stall_threshold_s")
        finally:
            launch_runtime_module.request_host_child_runtime_status = original_request
            launch_runtime_module.should_request_host_child_runtime_status = original_should

        request_root = runtime_status_module.host_child_runtime_status_request_root(repo_root=ROOT)
        request_dir = request_root / "manual_request"
        request_dir.mkdir(parents=True, exist_ok=True)
        response_ref = request_dir / "HostChildRuntimeStatusResponse.json"
        request_payload = {
            "schema": "loop_product.host_child_runtime_status_request",
            "request_id": "manual_request",
            "launch_result_ref": str(launch_result_ref.resolve()),
            "stall_threshold_s": 33.0,
            "response_ref": str(response_ref.resolve()),
        }
        (request_dir / "HostChildRuntimeStatusRequest.json").write_text(
            json.dumps(request_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        original_supervisor_status = supervisor_module.child_runtime_status_from_launch_result_ref
        try:
            captured_env: dict[str, str] = {}

            def _fake_supervisor_status(**kwargs: object) -> dict[str, object]:
                captured_env["runtime_status_mode"] = str(os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE") or "")
                captured_env["bridge_mode"] = str(os.environ.get("LOOP_CHILD_LAUNCH_BRIDGE_MODE") or "")
                return {
                    "lifecycle_status": "ACTIVE",
                    "pid_alive": True,
                    "echo_launch_result_ref": kwargs.get("result_ref"),
                    "echo_stall_threshold_s": kwargs.get("stall_threshold_s"),
                }

            supervisor_module.child_runtime_status_from_launch_result_ref = _fake_supervisor_status
            processed = supervisor_module.process_pending_runtime_status_requests(repo_root=ROOT)
            if processed != 1:
                return _fail("host child runtime status supervisor must process the pending request exactly once")
            if not response_ref.exists():
                return _fail("host child runtime status supervisor must persist a response artifact")
            response_payload = json.loads(response_ref.read_text(encoding="utf-8"))
            if str(response_payload.get("status") or "") != "completed":
                return _fail("host child runtime status supervisor must mark successful status requests as completed")
            child_status = dict(response_payload.get("child_runtime_status_result") or {})
            if str(child_status.get("echo_launch_result_ref") or "") != str(launch_result_ref.resolve()):
                return _fail("host child runtime status supervisor must pass the exact launch result ref into direct runtime-status observation")
            if float(child_status.get("echo_stall_threshold_s") or -1.0) != 33.0:
                return _fail("host child runtime status supervisor must pass stall_threshold_s into direct runtime-status observation")
            if captured_env.get("runtime_status_mode") != "direct":
                return _fail("host child runtime status supervisor must force direct runtime-status mode while consuming requests")
            if captured_env.get("bridge_mode") != "direct":
                return _fail("host child runtime status supervisor must force direct bridge mode while consuming requests")
        finally:
            supervisor_module.child_runtime_status_from_launch_result_ref = original_supervisor_status
    finally:
        shutil.rmtree(state_root, ignore_errors=True)
        shutil.rmtree(ROOT / ".loop" / "host_child_runtime_status_requests", ignore_errors=True)

    print("[loop-system-host-child-runtime-status-supervisor] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
