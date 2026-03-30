#!/usr/bin/env python3
"""Validate long-lived repo control-plane service reconciliation."""

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
    print(f"[loop-system-repo-control-plane-service-loop][FAIL] {msg}", file=sys.stderr)
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
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime import gc as gc_module
    from loop_product.runtime.control_plane import ensure_repo_control_plane_services_running
    from loop_product.runtime.control_plane import live_repo_control_plane_runtime

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_service_loop_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        control_plane_pid = 0
        supervisor_pid = 0
        housekeeping_pid = 0
        try:
            payload = ensure_repo_control_plane_services_running(repo_root=repo_root)
            control_plane = dict(payload.get("repo_control_plane") or {})
            supervisor = dict(payload.get("host_child_launch_supervisor") or {})
            housekeeping = dict(payload.get("housekeeping_reap_controller") or {})

            control_plane_pid = int(control_plane.get("pid") or 0)
            control_plane_lease_epoch = int(control_plane.get("lease_epoch") or 0)
            control_plane_started_at_utc = str(control_plane.get("started_at_utc") or "")
            supervisor_pid = int(supervisor.get("pid") or 0)
            housekeeping_pid = int(housekeeping.get("pid") or 0)
            if control_plane_pid <= 0:
                return _fail("repo control-plane bootstrap must expose a live control-plane pid")
            if supervisor_pid <= 0 or housekeeping_pid <= 0:
                return _fail("repo control-plane service must expose live subordinate service pids")
            if not _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                return _fail("repo control-plane bootstrap must start a live control-plane process")

            os.kill(supervisor_pid, signal.SIGTERM)
            if not _wait_until(
                lambda: (
                    (payload := supervisor_module.live_supervisor_runtime(repo_root=repo_root)) is not None
                    and int(payload.get("pid") or 0) > 0
                    and int(payload.get("pid") or 0) != supervisor_pid
                ),
                timeout_s=6.0,
            ):
                return _fail("repo control-plane service must restart the host supervisor after it dies")

            os.kill(housekeeping_pid, signal.SIGTERM)
            if not _wait_until(
                lambda: (
                    (payload := gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root)) is not None
                    and int(payload.get("pid") or 0) > 0
                    and int(payload.get("pid") or 0) != housekeeping_pid
                ),
                timeout_s=6.0,
            ):
                return _fail("repo control-plane service must restart housekeeping after it dies")

            reused = ensure_repo_control_plane_services_running(repo_root=repo_root)
            reused_control_plane = dict(reused.get("repo_control_plane") or {})
            live_control_plane = dict(live_repo_control_plane_runtime(repo_root=repo_root) or {})
            if not live_control_plane:
                return _fail("repo control-plane service must persist a live runtime marker")
            if int(reused_control_plane.get("pid") or 0) != int(live_control_plane.get("pid") or 0):
                return _fail("repeated repo control-plane bootstrap must return the authoritative live control-plane runtime")
            if _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                if int(live_control_plane.get("pid") or 0) != control_plane_pid:
                    return _fail("repeated repo control-plane bootstrap must not replace a still-live control-plane service")
            else:
                restored = _wait_for_stable_new_runtime_identity(
                    lambda: live_repo_control_plane_runtime(repo_root=repo_root),
                    prior_pid=control_plane_pid,
                    prior_lease_epoch=control_plane_lease_epoch,
                    prior_started_at_utc=control_plane_started_at_utc,
                    timeout_s=6.0,
                )
                if restored is None:
                    return _fail("replacement repo control-plane must expose a new runtime identity after prior service loss")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-service-loop] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
