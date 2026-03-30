#!/usr/bin/env python3
"""Validate host-supervisor watchdog restoration of repo control-plane."""

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
    print(f"[loop-system-repo-control-plane-host-watchdog-restore][FAIL] {msg}", file=sys.stderr)
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


def _is_new_runtime_identity(
    payload: dict[str, object] | None,
    *,
    prior_pid: int,
    prior_lease_epoch: int,
    prior_started_at_utc: str,
) -> bool:
    runtime = dict(payload or {})
    current_pid = int(runtime.get("pid") or 0)
    current_lease_epoch = int(runtime.get("lease_epoch") or 0)
    current_started_at_utc = str(runtime.get("started_at_utc") or "")
    if current_pid <= 0:
        return False
    return (
        current_pid != int(prior_pid)
        or current_lease_epoch > int(prior_lease_epoch)
        or (
            bool(prior_started_at_utc.strip())
            and bool(current_started_at_utc.strip())
            and current_started_at_utc != prior_started_at_utc
        )
    )


def _runtime_identity_key(payload: dict[str, object] | None) -> tuple[int, int, str]:
    runtime = dict(payload or {})
    return (
        int(runtime.get("pid") or 0),
        int(runtime.get("lease_epoch") or 0),
        str(runtime.get("started_at_utc") or ""),
    )


def _wait_for_stable_new_runtime_identity(
    fetch_payload,
    *,
    prior_pid: int,
    prior_lease_epoch: int,
    prior_started_at_utc: str,
    timeout_s: float,
    interval_s: float = 0.05,
    stable_reads: int = 3,
) -> dict[str, object] | None:
    deadline = time.time() + max(0.0, float(timeout_s))
    stable_target = max(1, int(stable_reads))
    last_key: tuple[int, int, str] | None = None
    last_payload: dict[str, object] | None = None
    stable_count = 0
    while time.time() < deadline:
        payload = dict(fetch_payload() or {})
        if _is_new_runtime_identity(
            payload,
            prior_pid=prior_pid,
            prior_lease_epoch=prior_lease_epoch,
            prior_started_at_utc=prior_started_at_utc,
        ):
            key = _runtime_identity_key(payload)
            if key == last_key:
                stable_count += 1
            else:
                last_key = key
                last_payload = payload
                stable_count = 1
            if stable_count >= stable_target:
                return dict(last_payload or payload)
        else:
            last_key = None
            last_payload = None
            stable_count = 0
        time.sleep(interval_s)
    payload = dict(fetch_payload() or {})
    if _is_new_runtime_identity(
        payload,
        prior_pid=prior_pid,
        prior_lease_epoch=prior_lease_epoch,
        prior_started_at_utc=prior_started_at_utc,
    ):
        return payload
    return None


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime.control_plane import (
        ensure_repo_control_plane_running,
        live_repo_reactor_residency_guard_runtime,
        live_repo_reactor_runtime,
        live_repo_control_plane_runtime,
        repo_control_plane_runtime_root,
    )
    from loop_product.runtime.gc import live_housekeeping_reap_controller_runtime

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_host_watchdog_restore_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        control_plane_pid = 0
        supervisor_pid = 0
        try:
            control_plane = dict(ensure_repo_control_plane_running(repo_root=repo_root) or {})
            control_plane_pid = int(control_plane.get("pid") or 0)
            control_plane_lease_epoch = int(control_plane.get("lease_epoch") or 0)
            control_plane_started_at_utc = str(control_plane.get("started_at_utc") or "")
            if control_plane_pid <= 0:
                return _fail("setup must expose a live repo control-plane pid")
            if not _wait_until(
                lambda: (
                    (payload := supervisor_module.live_supervisor_runtime(repo_root=repo_root)) is not None
                    and int(payload.get("pid") or 0) > 0
                    and live_housekeeping_reap_controller_runtime(repo_root=repo_root) is not None
                ),
                timeout_s=6.0,
            ):
                return _fail("direct repo control-plane bootstrap must still converge host supervisor and housekeeping")
            supervisor_pid = int(dict(supervisor_module.live_supervisor_runtime(repo_root=repo_root) or {}).get("pid") or 0)
            if supervisor_pid <= 0:
                return _fail("setup must expose live repo control-plane and host supervisor pids")

            upper_guard_pid = int(
                dict(live_repo_reactor_residency_guard_runtime(repo_root=repo_root) or {}).get("pid") or 0
            )
            upper_reactor_pid = int(dict(live_repo_reactor_runtime(repo_root=repo_root) or {}).get("pid") or 0)
            for pid, needle in (
                (upper_guard_pid, "loop_product.runtime.control_plane --run-repo-reactor-residency-guard"),
                (upper_reactor_pid, "loop_product.runtime.control_plane --run-repo-reactor"),
            ):
                if pid > 0:
                    os.kill(pid, signal.SIGTERM)
                    if not _wait_until(
                        lambda pid=pid, needle=needle: not _process_alive(pid, needle, repo_root=repo_root),
                        timeout_s=3.0,
                    ):
                        return _fail("host-watchdog restore isolation must retire upper-chain services before the restore step")
            time.sleep(0.25)
            if live_repo_reactor_residency_guard_runtime(repo_root=repo_root) is not None:
                return _fail("host-watchdog restore isolation must remove the residency guard before killing control-plane")
            if live_repo_reactor_runtime(repo_root=repo_root) is not None:
                return _fail("host-watchdog restore isolation must remove the reactor before killing control-plane")

            os.kill(control_plane_pid, signal.SIGTERM)
            if not _wait_until(
                lambda: not _process_alive(
                    control_plane_pid,
                    "loop_product.runtime.control_plane",
                    repo_root=repo_root,
                ),
                timeout_s=3.0,
            ):
                return _fail("test setup must be able to terminate the original repo control-plane pid")

            restored = _wait_for_stable_new_runtime_identity(
                lambda: live_repo_control_plane_runtime(repo_root=repo_root),
                prior_pid=control_plane_pid,
                prior_lease_epoch=control_plane_lease_epoch,
                prior_started_at_utc=control_plane_started_at_utc,
                timeout_s=8.0,
            )
            if restored is None:
                return _fail("host-supervisor watchdog must restore repo control-plane after the old pid dies")
            if not _pid_alive(supervisor_pid):
                return _fail("host supervisor must stay alive long enough to restore repo control-plane")

            required_events = [
                event
                for event in iter_committed_events(repo_control_plane_runtime_root(repo_root=repo_root))
                if str(event.get("event_type") or "") == "repo_control_plane_required"
                and str(dict(event.get("payload") or {}).get("trigger_kind") or "") == "host_supervisor_watchdog"
            ]
            if not required_events:
                return _fail("host-supervisor watchdog restore must publish canonical repo_control_plane_required")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-host-watchdog-restore] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
