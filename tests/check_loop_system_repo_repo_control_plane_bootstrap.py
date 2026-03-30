#!/usr/bin/env python3
"""Validate explicit repo control-plane bootstrap before any specific demand path."""

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
    print(f"[loop-system-repo-control-plane-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def _housekeeping_payload(*, housekeeping_root: Path) -> dict[str, object]:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    effective = dict(
        dict(query_runtime_liveness_view(housekeeping_root).get("effective_runtime_liveness_by_node") or {}).get(
            gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID
        )
        or {}
    )
    return dict(effective.get("payload") or {})


def main() -> int:
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime import live_housekeeping_reap_controller_runtime
    from loop_product.runtime.control_plane import ensure_repo_control_plane_services_running
    from loop_product.runtime.control_plane import live_repo_control_plane_runtime

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_bootstrap_") as td:
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
            supervisor_pid = int(supervisor.get("pid") or 0)
            housekeeping_pid = int(housekeeping.get("pid") or 0)
            if control_plane_pid <= 0:
                return _fail("repo control-plane bootstrap must expose a live repo control-plane pid")
            if supervisor_pid <= 0:
                return _fail("repo control-plane bootstrap must expose a live host supervisor pid")
            if housekeeping_pid <= 0:
                return _fail("repo control-plane bootstrap must expose a live housekeeping pid")
            if not _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                return _fail("repo control-plane bootstrap must start a live repo control-plane process")
            if not _process_alive(supervisor_pid, "loop_product.host_child_launch_supervisor", repo_root=repo_root):
                return _fail("repo control-plane bootstrap must start a live host supervisor process")
            if not _process_alive(housekeeping_pid, "loop_product.runtime.gc", repo_root=repo_root):
                return _fail("repo control-plane bootstrap must start a live housekeeping process")

            housekeeping_root = repo_root / ".loop" / "housekeeping"
            if not _wait_until(lambda: bool(_housekeeping_payload(housekeeping_root=housekeeping_root)), timeout_s=4.0):
                return _fail("repo control-plane bootstrap must materialize housekeeping authority")

            reused = ensure_repo_control_plane_services_running(repo_root=repo_root)
            if int(dict(reused.get("repo_control_plane") or {}).get("pid") or 0) != control_plane_pid:
                return _fail("repo control-plane bootstrap must reuse the live repo control-plane service")
            if int(dict(reused.get("host_child_launch_supervisor") or {}).get("pid") or 0) != supervisor_pid:
                return _fail("repo control-plane bootstrap must reuse the live host supervisor")
            if int(dict(reused.get("housekeeping_reap_controller") or {}).get("pid") or 0) != housekeeping_pid:
                return _fail("repo control-plane bootstrap must reuse the live housekeeping controller")

            if live_housekeeping_reap_controller_runtime(repo_root=repo_root) is None:
                return _fail("repo control-plane bootstrap must leave a live housekeeping runtime marker/authority payload")
            if live_repo_control_plane_runtime(repo_root=repo_root) is None:
                return _fail("repo control-plane bootstrap must leave a live repo control-plane runtime marker")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
