#!/usr/bin/env python3
"""Validate repo-reactor residency-guard restoration of repo services."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-reactor-residency-guard-restore][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _process_alive(pid: int, needle: str, *, repo_root: Path) -> bool:
    proc = subprocess.run(
        ["ps", "-ww", "-p", str(int(pid)), "-o", "stat=,command="],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return False
    raw = str(proc.stdout or "").strip()
    if not raw:
        return False
    state, _, command = raw.partition(" ")
    if state.startswith("Z"):
        return False
    return all(fragment in command for fragment in str(needle or "").split()) and str(repo_root.resolve()) in command


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime import live_housekeeping_reap_controller_runtime
    from loop_product.runtime.control_plane import (
        ensure_repo_control_plane_services_running,
        live_repo_control_plane_runtime,
        live_repo_reactor_residency_guard_runtime,
        live_repo_reactor_runtime,
        repo_reactor_residency_guard_runtime_root,
        repo_reactor_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_reactor_residency_guard_restore_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        guard_pid = 0
        reactor_pid = 0
        control_plane_pid = 0
        supervisor_pid = 0
        housekeeping_pid = 0
        try:
            payload = ensure_repo_control_plane_services_running(repo_root=repo_root)
            guard_pid = int(dict(payload.get("repo_reactor_residency_guard") or {}).get("pid") or 0)
            reactor_pid = int(dict(payload.get("repo_reactor") or {}).get("pid") or 0)
            control_plane_pid = int(dict(payload.get("repo_control_plane") or {}).get("pid") or 0)
            supervisor_pid = int(dict(payload.get("host_child_launch_supervisor") or {}).get("pid") or 0)
            housekeeping_pid = int(dict(payload.get("housekeeping_reap_controller") or {}).get("pid") or 0)
            if min(guard_pid, reactor_pid, control_plane_pid, supervisor_pid, housekeeping_pid) <= 0:
                return _fail("setup must expose live residency guard, repo reactor, repo control-plane, host supervisor, and housekeeping pids")

            if not _process_alive(
                guard_pid,
                "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
                repo_root=repo_root,
            ):
                return _fail("setup must start a live repo-reactor residency-guard process")

            os.kill(reactor_pid, signal.SIGTERM)
            os.kill(control_plane_pid, signal.SIGTERM)
            os.kill(supervisor_pid, signal.SIGTERM)
            os.kill(housekeeping_pid, signal.SIGTERM)
            if not _wait_until(
                lambda: (
                    not _process_alive(reactor_pid, "loop_product.runtime.control_plane --run-repo-reactor", repo_root=repo_root)
                    and not _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root)
                    and not _process_alive(supervisor_pid, "loop_product.host_child_launch_supervisor", repo_root=repo_root)
                    and not _process_alive(housekeeping_pid, "loop_product.runtime.gc", repo_root=repo_root)
                ),
                timeout_s=4.0,
            ):
                return _fail("test setup must be able to terminate old subordinate repo-service pids")

            if not _process_alive(
                guard_pid,
                "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
                repo_root=repo_root,
            ):
                return _fail("residency guard must remain alive after repo reactor and subordinate services die")

            if not _wait_until(
                lambda: (
                    (guard := live_repo_reactor_residency_guard_runtime(repo_root=repo_root)) is not None
                    and int(guard.get("pid") or 0) == guard_pid
                    and (reactor := live_repo_reactor_runtime(repo_root=repo_root)) is not None
                    and int(reactor.get("pid") or 0) > 0
                    and int(reactor.get("pid") or 0) != reactor_pid
                    and (control_plane := live_repo_control_plane_runtime(repo_root=repo_root)) is not None
                    and int(control_plane.get("pid") or 0) > 0
                    and int(control_plane.get("pid") or 0) != control_plane_pid
                    and (supervisor := supervisor_module.live_supervisor_runtime(repo_root=repo_root)) is not None
                    and int(supervisor.get("pid") or 0) > 0
                    and int(supervisor.get("pid") or 0) != supervisor_pid
                    and (housekeeping := live_housekeeping_reap_controller_runtime(repo_root=repo_root)) is not None
                    and int(housekeeping.get("pid") or 0) > 0
                    and int(housekeeping.get("pid") or 0) != housekeeping_pid
                ),
                timeout_s=10.0,
            ):
                return _fail("surviving residency guard must restore repo reactor and all subordinate repo services")

            guard_required_events = [
                event
                for event in iter_committed_events(repo_reactor_residency_guard_runtime_root(repo_root=repo_root))
                if str(event.get("event_type") or "") == "repo_reactor_residency_required"
            ]
            if not guard_required_events:
                return _fail("residency guard bootstrap must publish canonical repo_reactor_residency_required")

            reactor_required_events = [
                event
                for event in iter_committed_events(repo_reactor_runtime_root(repo_root=repo_root))
                if str(event.get("event_type") or "") == "repo_reactor_required"
                and str(dict(event.get("payload") or {}).get("trigger_kind") or "") == "repo_reactor_residency_watchdog"
            ]
            if not reactor_required_events:
                return _fail("residency guard restore must publish canonical repo_reactor_required from residency watchdog")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-reactor-residency-guard-restore] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
