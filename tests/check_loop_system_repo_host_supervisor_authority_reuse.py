#!/usr/bin/env python3
"""Validate host supervisor reuse from committed authority after marker loss."""

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
    print(f"[loop-system-host-supervisor-authority-reuse][FAIL] {msg}", file=sys.stderr)
    return 2


def _host_supervisor_pids(repo_root: Path) -> list[int]:
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
        if "loop_product.host_child_launch_supervisor" not in command:
            continue
        result.append(pid)
    return sorted(set(result))


def _service_pids(repo_root: Path) -> list[int]:
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
        if "loop_product.host_child_launch_supervisor" in command or (
            "loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command
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
    from loop_product.host_child_launch_supervisor import (
        ensure_host_child_launch_supervisor_running,
        live_supervisor_runtime,
        supervisor_runtime_ref,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_host_supervisor_authority_reuse_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        supervisor_pid = 0
        try:
            payload = ensure_host_child_launch_supervisor_running(
                repo_root=repo_root,
                poll_interval_s=0.05,
                idle_exit_after_s=0.0,
            )
            supervisor_pid = int(payload.get("pid") or 0)
            if supervisor_pid <= 0:
                return _fail("host supervisor bootstrap must expose a supervisor pid")

            runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
            if not runtime_ref.exists():
                return _fail("host supervisor bootstrap must materialize a compatibility runtime marker before reuse is tested")
            runtime_ref.unlink()
            if runtime_ref.exists():
                return _fail("host supervisor runtime marker removal must succeed before reuse is tested")

            supervisor_pids = _host_supervisor_pids(repo_root)
            if supervisor_pids != [supervisor_pid]:
                return _fail("marker-loss setup must start from exactly one live host supervisor process")

            reused = ensure_host_child_launch_supervisor_running(
                repo_root=repo_root,
                poll_interval_s=0.05,
                idle_exit_after_s=0.0,
            )
            reused_pid = int(reused.get("pid") or 0)
            if reused_pid != supervisor_pid:
                return _fail("marker loss must reuse the existing authority-backed host supervisor instead of spawning a new pid")

            supervisor_pids = _host_supervisor_pids(repo_root)
            if supervisor_pids != [supervisor_pid]:
                return _fail("marker loss must not leave multiple host supervisor processes alive")

            if not runtime_ref.exists():
                return _fail("authority-backed host supervisor reuse must self-heal the local runtime marker")
            marker_payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
            if int(marker_payload.get("pid") or 0) != supervisor_pid:
                return _fail("self-healed host supervisor runtime marker must point at the reused supervisor pid")

            if live_supervisor_runtime(repo_root=repo_root) is None:
                return _fail("authority-backed host supervisor reuse must leave a live runtime marker payload")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-host-supervisor-authority-reuse] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
