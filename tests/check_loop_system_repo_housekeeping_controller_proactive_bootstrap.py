#!/usr/bin/env python3
"""Validate proactive housekeeping controller bootstrap before first demand."""

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
    print(f"[loop-system-housekeeping-controller-proactive-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def main() -> int:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module
    from loop_product.runtime import cleanup_test_repo_services
    from loop_product import host_child_launch_supervisor as supervisor_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_proactive_bootstrap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        repo_root.mkdir(parents=True, exist_ok=True)
        runtime_state = repo_root / ".loop" / "stopped-run"
        runtime_workspace = repo_root / "workspace" / "stopped-run"
        controller_pid = 0
        owned_proc = None
        try:
            controller = gc_module.ensure_housekeeping_reap_controller_service_running(repo_root=repo_root)
            controller_pid = int(controller.get("pid") or 0)
            if controller_pid <= 0:
                return _fail("proactive bootstrap must expose a live controller pid before first demand")

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
                return _fail("proactive bootstrap must become visible in committed runtime authority before first demand")

            time.sleep(1.5)
            if not _controller_process_alive(controller_pid, repo_root=repo_root):
                return _fail("proactively booted housekeeping service must remain alive while idle before first demand")

            reused_non_watch = gc_module.ensure_housekeeping_reap_controller_running(
                repo_root=repo_root,
                watch_repo_control_plane=False,
            )
            if int(reused_non_watch.get("pid") or 0) != controller_pid:
                return _fail("non-watch callers must reuse an already-live watch-mode housekeeping service")
            if not bool(reused_non_watch.get("watch_repo_control_plane")):
                return _fail("watch-mode housekeeping service must not be downgraded by a later non-watch caller")

            supervisor_payload = supervisor_module.ensure_host_child_launch_supervisor_running(repo_root=repo_root)
            if int(supervisor_payload.get("pid") or 0) <= 0:
                return _fail("host child launch supervisor bootstrap must expose a live supervisor pid")
            controller_after_supervisor = gc_module.ensure_housekeeping_reap_controller_service_running(repo_root=repo_root)
            if int(controller_after_supervisor.get("pid") or 0) != controller_pid:
                return _fail("host child launch supervisor bootstrap must preserve the existing watch-mode housekeeping service")
            if not bool(controller_after_supervisor.get("watch_repo_control_plane")):
                return _fail("host child launch supervisor bootstrap must not downgrade housekeeping out of watch mode")

            (runtime_state / "state").mkdir(parents=True, exist_ok=True)
            runtime_workspace.mkdir(parents=True, exist_ok=True)
            _persist_node(runtime_state, "stopped-run", status="BLOCKED", workspace_root=runtime_workspace)
            owned_proc = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    "import time; time.sleep(30)",
                    str(runtime_state.resolve()),
                    str(runtime_workspace.resolve()),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
                close_fds=True,
            )

            request = gc_module.submit_stopped_run_reap_request(state_root=runtime_state, repo_root=repo_root)
            if int(request.get("controller_pid") or 0) != controller_pid:
                return _fail("later request must reuse the proactively booted housekeeping service")
            if not _wait_until(lambda: not runtime_state.exists(), timeout_s=5.0):
                return _fail("proactively booted housekeeping service must settle the later reap request")
            if owned_proc is not None and not _wait_until(lambda: owned_proc.poll() is not None, timeout_s=3.0):
                return _fail("proactively booted housekeeping service must terminate lingering owned processes")
        finally:
            if owned_proc is not None and owned_proc.poll() is None:
                owned_proc.kill()
                owned_proc.wait(timeout=1.0)
            try:
                cleanup_receipt = cleanup_test_repo_services(
                    repo_root=repo_root,
                    settle_timeout_s=4.0,
                    poll_interval_s=0.05,
                )
            except Exception:
                cleanup_receipt = {}
            if controller_pid > 0 and _controller_process_alive(controller_pid, repo_root=repo_root):
                try:
                    os.kill(controller_pid, 15)
                except OSError:
                    pass
            if cleanup_receipt:
                remaining_live_services = dict(cleanup_receipt.get("remaining_live_services") or {})
                remaining_orphans = dict(cleanup_receipt.get("remaining_orphan_service_pids") or {})
                if remaining_live_services or remaining_orphans:
                    return _fail(
                        "proactive housekeeping bootstrap cleanup must converge repo-scope services without residue"
                    )

    print("[loop-system-housekeeping-controller-proactive-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
