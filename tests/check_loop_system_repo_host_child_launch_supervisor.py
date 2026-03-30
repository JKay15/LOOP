#!/usr/bin/env python3
"""Validate the host-side child launch request/response bridge."""

from __future__ import annotations

import atexit
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-host-child-launch-supervisor][FAIL] {msg}", file=sys.stderr)
    return 2


def _safe_rmtree(path: Path) -> None:
    if not path.exists():
        return

    def _onerror(func, target, exc_info):  # type: ignore[no-untyped-def]
        del exc_info
        try:
            os.chmod(target, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        except OSError:
            return
        try:
            func(target)
        except OSError:
            return

    shutil.rmtree(path, ignore_errors=False, onerror=_onerror)


def _cleanup_runtime_tree(*, state_root: Path) -> None:
    from loop_product.runtime import cleanup_test_runtime_root

    try:
        if state_root.exists():
            cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
    except Exception:
        pass


def _cleanup_repo_services_for_root() -> dict[str, object]:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        return dict(cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05) or {})
    except Exception:
        return {}


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module
    from loop_product import host_child_launch as host_child_launch_module
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime import gc as gc_module

    atexit.register(_cleanup_repo_services_for_root)
    workspace_root = ROOT / "workspace" / "test-host-child-launch-supervisor"
    state_root = ROOT / ".loop" / "test-host-child-launch-supervisor"
    idle_repo_root = ROOT / ".loop" / "test-host-child-launch-supervisor-idle-root"
    try:
        initial_cleanup = _cleanup_repo_services_for_root()
        if not bool(initial_cleanup.get("quiesced")):
            return _fail("repo service cleanup must quiesce before host child launch supervisor validation")
        startup_lock_ref = supervisor_module._startup_gate_ref(repo_root=ROOT)
        startup_lock_ref.parent.mkdir(parents=True, exist_ok=True)
        startup_lock_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.host_child_launch_supervisor_startup_lock",
                    "repo_root": str(ROOT.resolve()),
                    "pid": os.getpid(),
                    "started_at_utc": "2026-03-28T00:00:00Z",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        try:
            reentrant_lock_ref, reentrant_existing = supervisor_module._acquire_startup_gate(
                repo_root=ROOT,
                startup_timeout_s=0.2,
            )
        finally:
            startup_lock_ref.unlink(missing_ok=True)
        if reentrant_existing is not None:
            return _fail("same-process startup-gate reuse must not pretend a live supervisor already exists when none is running")
        if reentrant_lock_ref != startup_lock_ref:
            return _fail("same-process startup-gate reuse must hand back the current startup lock instead of timing out")
        _safe_rmtree(workspace_root)
        _safe_rmtree(state_root)
        _safe_rmtree(idle_repo_root)
        _safe_rmtree(ROOT / ".loop" / "host_child_launch_requests")
        workspace_root.mkdir(parents=True, exist_ok=True)
        idle_repo_root.mkdir(parents=True, exist_ok=True)
        (state_root / "artifacts" / "bootstrap").mkdir(parents=True, exist_ok=True)
        prompt_path = workspace_root / "CHILD_PROMPT.md"
        prompt_path.write_text("PROMPT\n", encoding="utf-8")

        result_path = state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json"
        result_path.write_text(
            json.dumps(
                {
                    "node_id": "test-host-child-launch-supervisor",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": ["codex", "exec", "-C", str(workspace_root.resolve()), "-"],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(prompt_path.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        original_request = launch_runtime_module.request_host_child_launch
        original_should = launch_runtime_module.should_request_host_child_launch
        original_acquire_gate = launch_runtime_module._acquire_node_launch_gate
        try:
            captured: dict[str, object] = {}

            def _fake_request(**kwargs: object) -> dict[str, object]:
                captured.update(kwargs)
                return {"launch_decision": "started", "node_id": "test-host-child-launch-supervisor"}

            def _forbid_gate_acquire(**kwargs: object) -> tuple[Path | None, dict[str, object] | None]:
                del kwargs
                raise AssertionError(
                    "requester-side host child launch delegation must not acquire the per-node launch gate"
                )

            launch_runtime_module.request_host_child_launch = _fake_request
            launch_runtime_module.should_request_host_child_launch = lambda: True
            launch_runtime_module._acquire_node_launch_gate = _forbid_gate_acquire
            result = launch_runtime_module.launch_child_from_result_ref(result_ref=result_path, startup_probe_ms=77)
            if str(result.get("launch_decision") or "") != "started":
                return _fail("host child launch request path must return the supervisor-provided launch result")
            if str(captured.get("source_result_ref") or "") != str(result_path.resolve()):
                return _fail("host child launch request path must preserve the exact source result ref")
            if int(captured.get("startup_probe_ms") or -1) != 77:
                return _fail("host child launch request path must preserve startup_probe_ms")
        finally:
            launch_runtime_module.request_host_child_launch = original_request
            launch_runtime_module.should_request_host_child_launch = original_should
            launch_runtime_module._acquire_node_launch_gate = original_acquire_gate

        request_root = host_child_launch_module.host_child_launch_request_root(repo_root=ROOT)

        original_ensure_supervisor_running = supervisor_module.ensure_host_child_launch_supervisor_running
        try:
            def _fake_ensure_supervisor_running(**kwargs: object) -> dict[str, object]:
                del kwargs
                request_dirs = sorted(request_root.glob("*"))
                if len(request_dirs) != 1:
                    raise AssertionError("client reap coverage expects exactly one pending host launch request directory")
                direct_request_dir = request_dirs[0]
                direct_response_ref = direct_request_dir / "HostChildLaunchResponse.json"
                direct_response_ref.write_text(
                    json.dumps(
                        {
                            "schema": "loop_product.host_child_launch_response",
                            "request_id": direct_request_dir.name,
                            "status": "completed",
                            "child_launch_result": {
                                "launch_decision": "started",
                                "node_id": "test-host-child-launch-supervisor",
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
            direct_client_result = host_child_launch_module.request_host_child_launch(
                source_result_ref=str(result_path.resolve()),
                node_id="client-reap-check",
                state_root=state_root,
                startup_probe_ms=1,
                startup_health_timeout_ms=2,
                response_timeout_s=2,
                poll_interval_s=0.01,
            )
            if str(direct_client_result.get("launch_decision") or "") != "started":
                return _fail("host child launch client must preserve completed supervisor launch results")
            residual_dirs = sorted(request_root.glob("*"))
            if residual_dirs:
                return _fail("host child launch client must reap its request directory after consuming the response")
        finally:
            supervisor_module.ensure_host_child_launch_supervisor_running = original_ensure_supervisor_running

        request_dir = request_root / "manual_request"
        request_dir.mkdir(parents=True, exist_ok=True)
        response_ref = request_dir / "HostChildLaunchResponse.json"
        request_payload = {
            "schema": "loop_product.host_child_launch_request",
            "request_id": "manual_request",
            "node_id": "test-host-child-launch-supervisor",
            "source_result_ref": str(result_path.resolve()),
            "startup_probe_ms": 0,
            "startup_health_timeout_ms": 250,
            "response_ref": str(response_ref.resolve()),
        }
        (request_dir / "HostChildLaunchRequest.json").write_text(
            json.dumps(request_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        original_supervisor_launch = supervisor_module.launch_child_from_result_ref
        original_supervisor_ensure = supervisor_module.ensure_child_supervision_running
        try:
            captured_env: dict[str, str] = {}
            captured_supervision: dict[str, str] = {}

            def _fake_supervisor_launch(**kwargs: object) -> dict[str, object]:
                captured_env["launch_mode"] = str(os.environ.get("LOOP_CHILD_LAUNCH_MODE") or "")
                captured_env["bridge_mode"] = str(os.environ.get("LOOP_CHILD_LAUNCH_BRIDGE_MODE") or "")
                return {
                    "launch_decision": "started",
                    "node_id": "test-host-child-launch-supervisor",
                    "echo_source_result_ref": kwargs.get("result_ref"),
                    "launch_result_ref": str((request_dir / "ChildLaunchResult.json").resolve()),
                }

            def _fake_ensure_child_supervision_running(*, launch_result_ref: str) -> dict[str, object]:
                captured_supervision["launch_result_ref"] = launch_result_ref
                return {"status": "started", "launch_result_ref": launch_result_ref}

            supervisor_module.launch_child_from_result_ref = _fake_supervisor_launch
            supervisor_module.ensure_child_supervision_running = _fake_ensure_child_supervision_running
            processed = supervisor_module.process_pending_requests(repo_root=ROOT)
            if processed != 1:
                return _fail("host child launch supervisor must process the pending request exactly once")
            if not response_ref.exists():
                return _fail("host child launch supervisor must persist a response artifact")
            response_payload = json.loads(response_ref.read_text(encoding="utf-8"))
            if str(response_payload.get("status") or "") != "completed":
                return _fail("host child launch supervisor must mark successful launches as completed")
            child_result = dict(response_payload.get("child_launch_result") or {})
            if str(child_result.get("echo_source_result_ref") or "") != str(result_path.resolve()):
                return _fail("host child launch supervisor must pass the exact source result ref into direct launch")
            expected_launch_result_ref = str((request_dir / "ChildLaunchResult.json").resolve())
            if str(captured_supervision.get("launch_result_ref") or "") != expected_launch_result_ref:
                return _fail("host child launch supervisor must attach committed supervision using the exact launch_result_ref")
            if captured_env.get("launch_mode") != "direct":
                return _fail("host child launch supervisor must force direct child launch mode while consuming requests")
            if captured_env.get("bridge_mode") == "direct":
                return _fail("host child launch supervisor must not force direct bridge mode while consuming requests")
            reaped = supervisor_module.reap_completed_launch_request_dirs(repo_root=ROOT, retention_s=0.0)
            if reaped != 1:
                return _fail("host child launch supervisor must sweep completed request directories after the retention window")
            if request_dir.exists():
                return _fail("completed host child launch request directory must be removable by the supervisor sweep")
        finally:
            supervisor_module.launch_child_from_result_ref = original_supervisor_launch
            supervisor_module.ensure_child_supervision_running = original_supervisor_ensure

        runtime_payload = supervisor_module.ensure_host_child_launch_supervisor_running(
            repo_root=idle_repo_root,
            poll_interval_s=0.05,
            idle_exit_after_s=0.15,
        )
        runtime_ref = supervisor_module.supervisor_runtime_ref(repo_root=idle_repo_root)
        if not runtime_ref.exists():
            return _fail("auto-started host child launch supervisor must persist a runtime marker while it is live")
        if int(runtime_payload.get("pid") or 0) <= 0:
            return _fail("auto-started host child launch supervisor must report a live pid")
        deadline = time.time() + 3.0
        while runtime_ref.exists() and time.time() < deadline:
            time.sleep(0.05)
        if runtime_ref.exists():
            return _fail("idle host child launch supervisor must clean up its runtime marker after exiting")
        if supervisor_module.live_supervisor_runtime(repo_root=idle_repo_root) is not None:
            return _fail("idle host child launch supervisor must not remain reported as live after idle exit")

        stale_runtime_ref = supervisor_module.supervisor_runtime_ref(repo_root=idle_repo_root)
        stale_proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        try:
            stale_runtime_ref.parent.mkdir(parents=True, exist_ok=True)
            stale_runtime_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.host_child_launch_supervisor_runtime",
                        "repo_root": str(idle_repo_root.resolve()),
                        "pid": stale_proc.pid,
                        "code_fingerprint": supervisor_module._runtime_code_fingerprint(),
                        "poll_interval_s": 0.05,
                        "idle_exit_after_s": 0.15,
                        "started_at_utc": "2026-03-23T00:00:00+00:00",
                        "log_ref": str(stale_runtime_ref.with_suffix(".log")),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            restarted_payload = supervisor_module.ensure_host_child_launch_supervisor_running(
                repo_root=idle_repo_root,
                poll_interval_s=0.05,
                idle_exit_after_s=0.15,
            )
            if int(restarted_payload.get("pid") or 0) <= 0:
                return _fail("stale host supervisor runtime markers must be replaced by a fresh live supervisor pid")
            if int(restarted_payload.get("pid") or 0) == stale_proc.pid:
                return _fail("stale host supervisor runtime markers must not reuse the stale pid")
            if str(restarted_payload.get("code_fingerprint") or "").strip() != supervisor_module._runtime_code_fingerprint():
                return _fail("fresh host supervisor runtime marker must advertise the current code fingerprint")
            if stale_proc.poll() is not None:
                return _fail("stale host supervisor replacement must not terminate an unrelated live process that merely reused the recorded pid")
            deadline = time.time() + 3.0
            while stale_runtime_ref.exists() and time.time() < deadline:
                time.sleep(0.05)
            if stale_runtime_ref.exists():
                return _fail("fresh replacement supervisor must still clean up its runtime marker after idle exit")
        finally:
            if stale_proc.poll() is None:
                stale_proc.terminate()
                try:
                    stale_proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    stale_proc.kill()
                    stale_proc.wait(timeout=1.0)
    finally:
        housekeeping_runtime = gc_module.live_housekeeping_reap_controller_runtime(repo_root=idle_repo_root)
        if housekeeping_runtime is not None:
            housekeeping_pid = int(housekeeping_runtime.get("pid") or 0)
            if housekeeping_pid > 0:
                try:
                    os.kill(housekeeping_pid, 15)
                except OSError:
                    pass
        _cleanup_runtime_tree(state_root=state_root)
        _safe_rmtree(workspace_root)
        _safe_rmtree(state_root)
        _safe_rmtree(idle_repo_root)
        _safe_rmtree(ROOT / ".loop" / "host_child_launch_requests")

    print("[loop-system-host-child-launch-supervisor] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
