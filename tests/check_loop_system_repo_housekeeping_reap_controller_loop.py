#!/usr/bin/env python3
"""Validate long-lived housekeeping reap controller runtime behavior."""

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
    print(f"[loop-system-housekeeping-reap-controller-loop][FAIL] {msg}", file=sys.stderr)
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


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def main() -> int:
    from loop_product.kernel import query_operational_hygiene_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_reap_controller_loop_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "stopped-run"
        workspace_root = repo_root / "workspace" / "stopped-run"
        (state_root / "state").mkdir(parents=True, exist_ok=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(state_root, "stopped-run", status="BLOCKED", workspace_root=workspace_root)

        lingering_proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(state_root.resolve()),
                str(workspace_root.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        controller_pid = 0
        try:
            controller = gc_module.ensure_housekeeping_reap_controller_running(
                repo_root=repo_root,
                poll_interval_s=0.1,
                idle_exit_after_s=0.75,
            )
            controller_pid = int(controller.get("pid") or 0)
            if controller_pid <= 0:
                return _fail("controller launcher must return a live controller pid")
            live_runtime = gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root)
            if live_runtime is None:
                return _fail("controller launcher must materialize a housekeeping controller runtime marker")
            if int(live_runtime.get("pid") or 0) != controller_pid:
                return _fail("runtime marker pid must match the launched controller pid")
            if str(live_runtime.get("runtime_kind") or "") != "housekeeping_reap_controller":
                return _fail("runtime marker must declare housekeeping_reap_controller runtime kind")

            request = gc_module.submit_stopped_run_reap_request(
                state_root=state_root,
                repo_root=repo_root,
                auto_bootstrap_controller=False,
            )
            if not request.get("canonical_reap_requested_event_id"):
                return _fail("request submission must persist canonical reap intent")
            if not state_root.exists():
                return _fail("request submission alone must not delete the stopped runtime root")

            if not _wait_until(lambda: not state_root.exists(), timeout_s=5.0):
                return _fail("long-lived controller must eventually settle and retire the stopped runtime root")
            if not _wait_until(lambda: lingering_proc.poll() is not None, timeout_s=3.0):
                return _fail("long-lived controller must terminate lingering owned processes")

            hygiene = query_operational_hygiene_view(
                gc_module.housekeeping_runtime_root(repo_root=repo_root),
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("reap_requested_event_count") or 0) != 1:
                return _fail("controller-loop scenario must preserve one canonical reap_requested fact")
            if int(hygiene.get("process_reap_confirmed_event_count") or 0) != 1:
                return _fail("controller-loop scenario must emit one canonical process_reap_confirmed fact")
            if int(hygiene.get("runtime_root_quiesced_event_count") or 0) != 1:
                return _fail("controller-loop scenario must emit one canonical runtime_root_quiesced fact")

            if not _wait_until(
                lambda: gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root) is None,
                timeout_s=4.0,
            ):
                return _fail("controller must self-exit after idle timeout and clear its runtime marker")
            if controller_pid > 0 and not _wait_until(
                lambda: not _controller_process_alive(controller_pid, repo_root=repo_root),
                timeout_s=2.0,
            ):
                return _fail("controller process identity must be gone after idle self-exit")
        finally:
            if controller_pid > 0 and _controller_process_alive(controller_pid, repo_root=repo_root):
                os.kill(controller_pid, 15)
                _wait_until(
                    lambda: not _controller_process_alive(controller_pid, repo_root=repo_root),
                    timeout_s=2.0,
                )
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

    print("[loop-system-housekeeping-reap-controller-loop] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
