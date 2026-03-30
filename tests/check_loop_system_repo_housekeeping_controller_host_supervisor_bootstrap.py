#!/usr/bin/env python3
"""Validate that host-supervisor startup bootstraps housekeeping authority."""

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
    print(f"[loop-system-housekeeping-controller-host-supervisor-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def _process_alive_with_marker(pid: int, needle: str, *, repo_root: Path) -> bool:
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


def _assert_clean_host_supervisor_bootstrap_orders_housekeeping_after_runtime(*, repo_root: Path) -> None:
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime import cleanup_test_repo_services, gc as gc_module

    original_ensure_housekeeping = gc_module.ensure_housekeeping_reap_controller_service_running
    observed_runtime: list[dict[str, object]] = []

    def _guarded_ensure_housekeeping(**kwargs):
        runtime_payload = supervisor_module.live_supervisor_runtime(repo_root=repo_root)
        if runtime_payload is None:
            raise AssertionError(
                "host supervisor bootstrap must not request watch-mode housekeeping before its runtime marker is visible"
            )
        observed_runtime.append(dict(runtime_payload))
        return {
            "bootstrap_status": "test_stubbed",
            "pid": 0,
            "repo_root": str(repo_root),
            "service_kind": "housekeeping_reap_controller",
            "watch_repo_control_plane": True,
        }

    gc_module.ensure_housekeeping_reap_controller_service_running = _guarded_ensure_housekeeping
    try:
        supervisor = supervisor_module.ensure_host_child_launch_supervisor_running(
            repo_root=repo_root,
            poll_interval_s=0.05,
            idle_exit_after_s=0.15,
        )
        supervisor_pid = int(supervisor.get("pid") or 0)
        if supervisor_pid <= 0:
            raise AssertionError("host supervisor bootstrap must still expose a live supervisor pid under guarded housekeeping")
        if not observed_runtime:
            raise AssertionError("host supervisor bootstrap must request watch-mode housekeeping during clean bootstrap")
    finally:
        gc_module.ensure_housekeeping_reap_controller_service_running = original_ensure_housekeeping
        cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
        if not bool(cleanup_receipt.get("quiesced")):
            raise RuntimeError(f"repo-service cleanup did not quiesce after guarded bootstrap: {cleanup_receipt!r}")


def main() -> int:
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime import cleanup_test_repo_services, gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_host_supervisor_bootstrap_guarded_") as td:
        guarded_repo_root = Path(td).resolve() / "loop_product_repo"
        try:
            _assert_clean_host_supervisor_bootstrap_orders_housekeeping_after_runtime(repo_root=guarded_repo_root)
        except AssertionError as exc:
            return _fail(str(exc))

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_host_supervisor_bootstrap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        supervisor_pid = 0
        controller_pid = 0
        try:
            supervisor = supervisor_module.ensure_host_child_launch_supervisor_running(
                repo_root=repo_root,
                poll_interval_s=0.05,
                idle_exit_after_s=0.15,
            )
            supervisor_pid = int(supervisor.get("pid") or 0)
            if supervisor_pid <= 0:
                return _fail("host supervisor bootstrap must expose a live supervisor pid")

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            if not _wait_until(lambda: bool(_housekeeping_payload(housekeeping_root=housekeeping_root)), timeout_s=4.0):
                return _fail("host supervisor startup must bootstrap housekeeping authority before any runtime-lifecycle bootstrap")

            payload_a = _housekeeping_payload(housekeeping_root=housekeeping_root)
            controller_pid = int(payload_a.get("pid") or 0)
            if controller_pid <= 0:
                return _fail("host supervisor startup must expose a live housekeeping controller pid")
            if not _process_alive_with_marker(
                controller_pid,
                "loop_product.runtime.gc",
                repo_root=repo_root,
            ):
                return _fail("host supervisor startup must bring up a live housekeeping controller process")

            supervisor_again = supervisor_module.ensure_host_child_launch_supervisor_running(
                repo_root=repo_root,
                poll_interval_s=0.05,
                idle_exit_after_s=0.15,
            )
            supervisor_pid_again = int(supervisor_again.get("pid") or 0)
            if supervisor_pid_again != supervisor_pid:
                return _fail("repeated host supervisor bootstrap must reuse the same live supervisor")

            payload_b = _housekeeping_payload(housekeeping_root=housekeeping_root)
            controller_pid_b = int(payload_b.get("pid") or 0)
            if controller_pid_b != controller_pid:
                return _fail("repeated host supervisor bootstrap must reuse the same housekeeping controller")
        finally:
            cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(cleanup_receipt.get("quiesced")):
                raise RuntimeError(f"repo-service cleanup did not quiesce: {cleanup_receipt!r}")

    print("[loop-system-housekeeping-controller-host-supervisor-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
