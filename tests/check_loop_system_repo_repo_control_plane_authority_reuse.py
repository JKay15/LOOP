#!/usr/bin/env python3
"""Validate repo control-plane reuse from committed authority after marker loss."""

from __future__ import annotations

import json
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
    print(f"[loop-system-repo-control-plane-authority-reuse][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _control_plane_pids_for_repo(repo_root: Path) -> list[int]:
    proc = subprocess.run(
        ["ps", "-ax", "-o", "pid=,command="],
        text=True,
        capture_output=True,
        check=True,
    )
    result: list[int] = []
    marker = str(repo_root.resolve())
    for line in proc.stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        pid_text, _, command = raw.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if "loop_product.runtime.control_plane" not in command or "--run-repo-reactor" in command:
            continue
        if marker not in command:
            continue
        result.append(pid)
    return sorted(result)


def _repo_service_pids_for_repo(repo_root: Path) -> list[int]:
    proc = subprocess.run(
        ["ps", "-ax", "-o", "pid=,command="],
        text=True,
        capture_output=True,
        check=True,
    )
    result: list[int] = []
    marker = str(repo_root.resolve())
    for line in proc.stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        pid_text, _, command = raw.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if marker not in command:
            continue
        if (
            "loop_product.runtime.control_plane" in command
            or "loop_product.host_child_launch_supervisor" in command
            or ("loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command)
        ):
            result.append(pid)
    return sorted(set(result))


def _terminate_pid(pid: int) -> None:
    try:
        os.kill(int(pid), signal.SIGTERM)
    except OSError:
        return


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime.control_plane import (
        REPO_CONTROL_PLANE_NODE_ID,
        ensure_repo_control_plane_running,
        ensure_repo_control_plane_services_running,
        live_repo_control_plane_runtime,
        repo_control_plane_runtime_ref,
        repo_control_plane_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_authority_reuse_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        control_plane_pid = 0
        try:
            payload = ensure_repo_control_plane_services_running(repo_root=repo_root)
            control_plane = dict(payload.get("repo_control_plane") or {})
            control_plane_pid = int(control_plane.get("pid") or 0)
            if control_plane_pid <= 0:
                return _fail("repo control-plane bootstrap must expose a control-plane pid")

            control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
            if not _wait_until(
                lambda: bool(
                    dict(query_runtime_liveness_view(control_plane_root).get("effective_runtime_liveness_by_node") or {}).get(
                        REPO_CONTROL_PLANE_NODE_ID
                    )
                ),
                timeout_s=4.0,
            ):
                return _fail("initial repo control-plane startup must become visible in committed authority")

            marker_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
            if not marker_ref.exists():
                return _fail("repo control-plane startup must materialize a compatibility runtime marker before reuse is tested")
            marker_ref.unlink()
            if marker_ref.exists():
                return _fail("repo control-plane runtime marker removal must succeed before reuse is tested")

            control_plane_pids = _control_plane_pids_for_repo(repo_root)
            if control_plane_pid not in control_plane_pids:
                return _fail("original repo control-plane must still be alive after marker loss")
            if len(control_plane_pids) != 1:
                return _fail("marker-loss setup must start from exactly one live repo control-plane process")

            reused = ensure_repo_control_plane_running(repo_root=repo_root, poll_interval_s=0.1)
            reused_pid = int(reused.get("pid") or 0)
            if reused_pid != control_plane_pid:
                return _fail("marker loss must reuse the existing authority-backed repo control-plane instead of spawning a new pid")

            control_plane_pids = _control_plane_pids_for_repo(repo_root)
            if control_plane_pids != [control_plane_pid]:
                return _fail("marker loss must not leave multiple repo control-plane processes alive")

            if not _wait_until(lambda: marker_ref.exists(), timeout_s=2.0):
                return _fail("authority-backed repo control-plane reuse must self-heal the local runtime marker")
            marker_payload = json.loads(marker_ref.read_text(encoding="utf-8"))
            if int(marker_payload.get("pid") or 0) != control_plane_pid:
                return _fail("self-healed repo control-plane runtime marker must point at the reused control-plane pid")

            if live_repo_control_plane_runtime(repo_root=repo_root) is None:
                return _fail("authority-backed repo control-plane reuse must leave a live runtime marker payload")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-authority-reuse] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
