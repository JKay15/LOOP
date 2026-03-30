#!/usr/bin/env python3
"""Validate authoritative committed exit facts for the housekeeping controller."""

from __future__ import annotations

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
    print(f"[loop-system-housekeeping-controller-authoritative-exit][FAIL] {msg}", file=sys.stderr)
    return 2


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
    from loop_product.event_journal import iter_committed_events
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_authoritative_exit_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        repo_root.mkdir(parents=True, exist_ok=True)
        controller_pid = 0
        try:
            controller = gc_module.ensure_housekeeping_reap_controller_running(
                repo_root=repo_root,
                poll_interval_s=0.1,
                idle_exit_after_s=0.75,
            )
            controller_pid = int(controller.get("pid") or 0)
            if controller_pid <= 0:
                return _fail("controller launcher must return a live pid")

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
                return _fail("controller startup must become visible in committed runtime liveness authority")

            if not _wait_until(
                lambda: not _controller_process_alive(controller_pid, repo_root=repo_root),
                timeout_s=4.0,
            ):
                return _fail("controller must idle-exit without external termination")

            view = query_runtime_liveness_view(housekeeping_root)
            effective = dict(dict(view.get("effective_runtime_liveness_by_node") or {}).get(controller_node_id) or {})
            if str(effective.get("effective_attachment_state") or "").strip().upper() != "TERMINAL":
                return _fail("idle controller exit must become immediately non-live in effective committed authority")

            payload = dict(effective.get("payload") or {})
            if int(payload.get("pid") or 0) != controller_pid:
                return _fail("effective exit authority must preserve controller pid identity")
            if str(payload.get("exit_reason") or "").strip() != "idle_exit":
                return _fail("effective exit authority must record idle_exit reason")

            events = list(iter_committed_events(housekeeping_root))
            exit_events = [
                event
                for event in events
                if str(event.get("event_type") or "") == "process_exit_observed"
                and str(event.get("node_id") or "") == controller_node_id
                and int(dict(event.get("payload") or {}).get("pid") or 0) == controller_pid
            ]
            if not exit_events:
                return _fail("idle controller exit must commit canonical process_exit_observed")
            if str(dict(exit_events[-1].get("payload") or {}).get("exit_reason") or "").strip() != "idle_exit":
                return _fail("process_exit_observed must record idle_exit reason")

            nonlive_events = [
                event
                for event in events
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
                and str(event.get("node_id") or "") == controller_node_id
                and int(dict(event.get("payload") or {}).get("pid") or 0) == controller_pid
                and str(dict(event.get("payload") or {}).get("attachment_state") or "").strip().upper() == "TERMINAL"
            ]
            if not nonlive_events:
                return _fail("idle controller exit must commit immediate non-live runtime_liveness_observed")
        finally:
            if controller_pid > 0 and _controller_process_alive(controller_pid, repo_root=repo_root):
                try:
                    os.kill(controller_pid, 15)
                except OSError:
                    pass

    print("[loop-system-housekeeping-controller-authoritative-exit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
