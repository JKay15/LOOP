#!/usr/bin/env python3
"""Validate the host-side child runtime-status request/response bridge."""

from __future__ import annotations

import atexit
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


def _cleanup_runtime_state(*, state_root: Path) -> None:
    from loop_product.runtime import cleanup_test_runtime_root

    try:
        if state_root.exists():
            cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
    except Exception:
        pass
    shutil.rmtree(state_root, ignore_errors=True)
    shutil.rmtree(ROOT / ".loop" / "host_child_runtime_status_requests", ignore_errors=True)


def _cleanup_repo_services_for_root() -> dict[str, object]:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        return dict(cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05) or {})
    except Exception:
        return {}


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module
    from loop_product import host_child_runtime_status as runtime_status_module
    from loop_product import host_child_launch_supervisor as supervisor_module

    atexit.register(_cleanup_repo_services_for_root)
    state_root = ROOT / ".loop" / "test-host-child-runtime-status-supervisor"
    try:
        initial_cleanup = _cleanup_repo_services_for_root()
        if not bool(initial_cleanup.get("quiesced")):
            return _fail("repo service cleanup must quiesce before host child runtime-status supervisor validation")
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
        original_ensure_supervisor_running = supervisor_module.ensure_host_child_launch_supervisor_running
        try:
            def _fake_ensure_supervisor_running(**kwargs: object) -> dict[str, object]:
                del kwargs
                request_dirs = sorted(request_root.glob("*"))
                if len(request_dirs) != 1:
                    raise AssertionError("client reap coverage expects exactly one pending host runtime-status request directory")
                direct_request_dir = request_dirs[0]
                direct_response_ref = direct_request_dir / "HostChildRuntimeStatusResponse.json"
                direct_response_ref.write_text(
                    json.dumps(
                        {
                            "schema": "loop_product.host_child_runtime_status_response",
                            "request_id": direct_request_dir.name,
                            "status": "completed",
                            "child_runtime_status_result": {
                                "lifecycle_status": "ACTIVE",
                                "pid_alive": True,
                            },
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return {"pid": 1, "repo_root": str(ROOT.resolve())}

            supervisor_module.ensure_host_child_launch_supervisor_running = _fake_ensure_supervisor_running
            direct_client_result = runtime_status_module.request_host_child_runtime_status(
                launch_result_ref=str(launch_result_ref.resolve()),
                stall_threshold_s=1.5,
                response_timeout_s=2,
                poll_interval_s=0.01,
            )
            if not bool(direct_client_result.get("pid_alive")):
                return _fail("host child runtime-status client must preserve completed supervisor status results")
            residual_dirs = sorted(request_root.glob("*"))
            if residual_dirs:
                return _fail("host child runtime-status client must reap its request directory after consuming the response")
        finally:
            supervisor_module.ensure_host_child_launch_supervisor_running = original_ensure_supervisor_running

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

        if not hasattr(supervisor_module, "runtime_lifecycle"):
            return _fail("host child runtime status supervisor must route through the trusted lifecycle status surface")

        original_supervisor_status = supervisor_module.runtime_lifecycle.child_runtime_status_from_launch_result_ref
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

            supervisor_module.runtime_lifecycle.child_runtime_status_from_launch_result_ref = _fake_supervisor_status
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
            reaped = supervisor_module.reap_completed_runtime_status_request_dirs(repo_root=ROOT, retention_s=0.0)
            if reaped != 1:
                return _fail("host child runtime status supervisor must sweep completed request directories after the retention window")
            if request_dir.exists():
                return _fail("completed host child runtime-status request directory must be removable by the supervisor sweep")

            vanished_request_dir = request_root / "vanished_request"
            vanished_request_dir.mkdir(parents=True, exist_ok=True)
            vanished_request_ref = vanished_request_dir / "HostChildRuntimeStatusRequest.json"
            vanished_request_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.host_child_runtime_status_request",
                        "request_id": "vanished_request",
                        "launch_result_ref": str(launch_result_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            original_iter_pending = supervisor_module._iter_pending_runtime_status_requests
            try:
                def _fake_iter_pending_runtime_status_requests(_request_root: Path):
                    vanished_request_ref.unlink(missing_ok=True)
                    return [vanished_request_ref]

                supervisor_module._iter_pending_runtime_status_requests = _fake_iter_pending_runtime_status_requests
                processed_missing = supervisor_module.process_pending_runtime_status_requests(repo_root=ROOT)
                if processed_missing != 0:
                    return _fail("missing runtime-status request files must be skipped without counting as processed work")
                if vanished_request_dir.exists():
                    return _fail("missing runtime-status request directories must be reaped instead of crashing the supervisor")
            finally:
                supervisor_module._iter_pending_runtime_status_requests = original_iter_pending
        finally:
            supervisor_module.runtime_lifecycle.child_runtime_status_from_launch_result_ref = original_supervisor_status
    finally:
        _cleanup_runtime_state(state_root=state_root)

    print("[loop-system-host-child-runtime-status-supervisor] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
