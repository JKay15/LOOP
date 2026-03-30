#!/usr/bin/env python3
"""Validate default service-mode behavior for request-triggered housekeeping controllers."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-housekeeping-controller-service-mode][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _persist_node(state_root: Path, node_id: str, *, status: str, workspace_root: Path) -> None:
    _write_json(
        state_root / "state" / f"{node_id}.json",
        {
            "node_id": node_id,
            "status": status,
            "workspace_root": str(workspace_root.resolve()),
            "runtime_state": {},
        },
    )


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _controller_process_alive(pid: int, *, repo_root: Path) -> bool:
    if not _pid_alive(pid):
        return False
    proc = subprocess.run(
        ["ps", "-ww", "-p", str(int(pid)), "-o", "command="],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return False
    command = str(proc.stdout or "").strip()
    return (
        "loop_product.runtime.gc" in command
        and "--run-housekeeping-controller" in command
        and str(repo_root.resolve()) in command
    )


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def _make_runtime(repo_root: Path, name: str) -> tuple[Path, Path]:
    state_root = repo_root / ".loop" / name
    workspace_root = repo_root / "workspace" / name
    (state_root / "state").mkdir(parents=True, exist_ok=True)
    workspace_root.mkdir(parents=True, exist_ok=True)
    _persist_node(state_root, name, status="BLOCKED", workspace_root=workspace_root)
    return state_root, workspace_root


def main() -> int:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_service_mode_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        runtime_a_state, runtime_a_workspace = _make_runtime(repo_root, "stopped-run-a")
        runtime_b_state, runtime_b_workspace = _make_runtime(repo_root, "stopped-run-b")

        proc_a = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(runtime_a_state.resolve()),
                str(runtime_a_workspace.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        proc_b = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(runtime_b_state.resolve()),
                str(runtime_b_workspace.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        controller_pid = 0
        try:
            request_a = gc_module.submit_stopped_run_reap_request(state_root=runtime_a_state, repo_root=repo_root)
            controller_pid = int(request_a.get("controller_pid") or 0)
            if controller_pid <= 0:
                return _fail("default request-triggered controller startup must expose a live pid")

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            controller_node_id = gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID
            if not _wait_until(
                lambda: bool(
                    dict(query_runtime_liveness_view(housekeeping_root).get("effective_runtime_liveness_by_node") or {}).get(
                        controller_node_id
                    )
                ),
                timeout_s=4.0,
            ):
                return _fail("default request-triggered controller startup must become visible in committed authority")

            effective = dict(
                dict(query_runtime_liveness_view(housekeeping_root).get("effective_runtime_liveness_by_node") or {}).get(
                    controller_node_id
                )
                or {}
            )
            payload = dict(effective.get("payload") or {})
            try:
                idle_exit_after_s = float(payload.get("idle_exit_after_s"))
            except (TypeError, ValueError):
                idle_exit_after_s = -1.0
            if idle_exit_after_s != 0.0:
                return _fail("default request-triggered controller must advertise service mode with idle_exit_after_s=0")

            if not _wait_until(lambda: not runtime_a_state.exists(), timeout_s=5.0):
                return _fail("service-mode controller must retire the first runtime root")
            if not _wait_until(lambda: proc_a.poll() is not None, timeout_s=3.0):
                return _fail("service-mode controller must terminate the first lingering process")

            time.sleep(1.5)
            if not _controller_process_alive(controller_pid, repo_root=repo_root):
                return _fail("default demand-started controller must remain alive after first cleanup instead of idling out")

            request_b = gc_module.submit_stopped_run_reap_request(state_root=runtime_b_state, repo_root=repo_root)
            controller_pid_b = int(request_b.get("controller_pid") or 0)
            if controller_pid_b != controller_pid:
                return _fail("later request must reuse the already-running service-mode controller")

            if not _wait_until(lambda: not runtime_b_state.exists(), timeout_s=5.0):
                return _fail("service-mode controller must retire the later runtime root without a second bootstrap")
            if not _wait_until(lambda: proc_b.poll() is not None, timeout_s=3.0):
                return _fail("service-mode controller must terminate the later lingering process")
        finally:
            _cleanup_repo_services(repo_root)
            for proc in (proc_a, proc_b):
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=1.0)

    print("[loop-system-housekeeping-controller-service-mode] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
