#!/usr/bin/env python3
"""Validate authoritative committed exit facts for the repo control-plane."""

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
    print(f"[loop-system-repo-control-plane-authoritative-exit][FAIL] {msg}", file=sys.stderr)
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
        if (
            "loop_product.runtime.control_plane" in command
            or "loop_product.host_child_launch_supervisor" in command
            or ("loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command)
        ):
            result.append(pid)
    return sorted(set(result))


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def _spawn_control_plane(repo_root: Path, *, poll_interval_s: float = 0.1, idle_exit_after_s: float = 0.75) -> subprocess.Popen[str]:
    code = (
        "from loop_product.runtime.control_plane import run_repo_control_plane_loop; "
        "import sys; "
        "raise SystemExit(run_repo_control_plane_loop("
        "repo_root=sys.argv[1], poll_interval_s=float(sys.argv[2]), idle_exit_after_s=float(sys.argv[3])))"
    )
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            str(repo_root),
            str(poll_interval_s),
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
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime.control_plane import REPO_CONTROL_PLANE_NODE_ID, repo_control_plane_runtime_root

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_authoritative_exit_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        control_plane = None
        try:
            control_plane = _spawn_control_plane(repo_root, poll_interval_s=0.1, idle_exit_after_s=0.75)
            control_plane_pid = int(control_plane.pid or 0)
            if control_plane_pid <= 0:
                return _fail("spawned repo control-plane must expose a live pid")

            control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
            if not _wait_until(
                lambda: bool(
                    dict(query_runtime_liveness_view(control_plane_root).get("effective_runtime_liveness_by_node") or {}).get(
                        REPO_CONTROL_PLANE_NODE_ID
                    )
                ),
                timeout_s=4.0,
            ):
                return _fail("repo control-plane startup must become visible in committed runtime liveness authority")

            if not _wait_until(
                lambda: control_plane.poll() is not None or not _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root),
                timeout_s=4.0,
            ):
                return _fail("repo control-plane must idle-exit without external termination when configured with finite idle_exit_after_s")

            view = query_runtime_liveness_view(control_plane_root)
            effective = dict(dict(view.get("effective_runtime_liveness_by_node") or {}).get(REPO_CONTROL_PLANE_NODE_ID) or {})
            if str(effective.get("effective_attachment_state") or "").strip().upper() != "TERMINAL":
                return _fail("idle repo control-plane exit must become immediately non-live in effective committed authority")

            payload = dict(effective.get("payload") or {})
            if int(payload.get("pid") or 0) != control_plane_pid:
                return _fail("effective repo control-plane exit authority must preserve pid identity")
            if str(payload.get("exit_reason") or "").strip() != "idle_exit":
                return _fail("effective repo control-plane exit authority must record idle_exit reason")

            events = list(iter_committed_events(control_plane_root))
            exit_events = [
                event
                for event in events
                if str(event.get("event_type") or "") == "process_exit_observed"
                and str(event.get("node_id") or "") == REPO_CONTROL_PLANE_NODE_ID
                and int(dict(event.get("payload") or {}).get("pid") or 0) == control_plane_pid
            ]
            if not exit_events:
                return _fail("idle repo control-plane exit must commit canonical process_exit_observed")
            if str(dict(exit_events[-1].get("payload") or {}).get("exit_reason") or "").strip() != "idle_exit":
                return _fail("process_exit_observed must record repo control-plane idle_exit reason")

            nonlive_events = [
                event
                for event in events
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
                and str(event.get("node_id") or "") == REPO_CONTROL_PLANE_NODE_ID
                and int(dict(event.get("payload") or {}).get("pid") or 0) == control_plane_pid
                and str(dict(event.get("payload") or {}).get("attachment_state") or "").strip().upper() == "TERMINAL"
            ]
            if not nonlive_events:
                return _fail("idle repo control-plane exit must commit immediate non-live runtime_liveness_observed")
        finally:
            if control_plane is not None and control_plane.poll() is None:
                try:
                    os.kill(control_plane.pid, signal.SIGTERM)
                except OSError:
                    pass
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-authoritative-exit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
