#!/usr/bin/env python3
"""Validate authoritative committed exit facts for the repo-scope host supervisor."""

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
    print(f"[loop-system-host-supervisor-authoritative-exit][FAIL] {msg}", file=sys.stderr)
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
        if "loop_product.host_child_launch_supervisor" in command or (
            "loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command
        ):
            result.append(pid)
    return sorted(set(result))


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def _spawn_host_supervisor(repo_root: Path, *, poll_interval_s: float = 0.1, idle_exit_after_s: float = 0.75) -> subprocess.Popen[str]:
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "loop_product.host_child_launch_supervisor",
            "--repo-root",
            str(repo_root),
            "--poll-interval-s",
            str(poll_interval_s),
            "--idle-exit-after-s",
            str(idle_exit_after_s),
        ],
        cwd=str(ROOT),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        text=True,
    )


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.host_child_launch_supervisor import (
        HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID,
        host_supervisor_runtime_root,
    )
    from loop_product.kernel import query_runtime_liveness_view

    with tempfile.TemporaryDirectory(prefix="loop_system_host_supervisor_authoritative_exit_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        supervisor = None
        try:
            supervisor = _spawn_host_supervisor(repo_root, poll_interval_s=0.1, idle_exit_after_s=0.75)
            supervisor_pid = int(supervisor.pid or 0)
            if supervisor_pid <= 0:
                return _fail("spawned host supervisor must expose a live pid")

            supervisor_root = host_supervisor_runtime_root(repo_root=repo_root)
            if not _wait_until(
                lambda: bool(
                    dict(query_runtime_liveness_view(supervisor_root).get("effective_runtime_liveness_by_node") or {}).get(
                        HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID
                    )
                ),
                timeout_s=4.0,
            ):
                return _fail("host supervisor startup must become visible in committed runtime liveness authority")

            if not _wait_until(
                lambda: supervisor.poll() is not None
                or not _process_alive(supervisor_pid, "loop_product.host_child_launch_supervisor", repo_root=repo_root),
                timeout_s=4.0,
            ):
                return _fail("host supervisor must idle-exit without external termination when configured with finite idle_exit_after_s")

            view = query_runtime_liveness_view(supervisor_root)
            effective = dict(
                dict(view.get("effective_runtime_liveness_by_node") or {}).get(HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID) or {}
            )
            if str(effective.get("effective_attachment_state") or "").strip().upper() != "TERMINAL":
                return _fail("idle host supervisor exit must become immediately non-live in effective committed authority")

            payload = dict(effective.get("payload") or {})
            if int(payload.get("pid") or 0) != supervisor_pid:
                return _fail("effective host supervisor exit authority must preserve pid identity")
            if str(payload.get("exit_reason") or "").strip() != "idle_exit":
                return _fail("effective host supervisor exit authority must record idle_exit reason")

            events = list(iter_committed_events(supervisor_root))
            exit_events = [
                event
                for event in events
                if str(event.get("event_type") or "") == "process_exit_observed"
                and str(event.get("node_id") or "") == HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID
                and int(dict(event.get("payload") or {}).get("pid") or 0) == supervisor_pid
            ]
            if not exit_events:
                return _fail("idle host supervisor exit must commit canonical process_exit_observed")
            if str(dict(exit_events[-1].get("payload") or {}).get("exit_reason") or "").strip() != "idle_exit":
                return _fail("process_exit_observed must record host supervisor idle_exit reason")

            nonlive_events = [
                event
                for event in events
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
                and str(event.get("node_id") or "") == HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID
                and int(dict(event.get("payload") or {}).get("pid") or 0) == supervisor_pid
                and str(dict(event.get("payload") or {}).get("attachment_state") or "").strip().upper() == "TERMINAL"
            ]
            if not nonlive_events:
                return _fail("idle host supervisor exit must commit immediate non-live runtime_liveness_observed")
        finally:
            if supervisor is not None and supervisor.poll() is None:
                try:
                    os.kill(supervisor.pid, signal.SIGTERM)
                except OSError:
                    pass
            _cleanup_repo_services(repo_root)

    print("[loop-system-host-supervisor-authoritative-exit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
