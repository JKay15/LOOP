#!/usr/bin/env python3
"""Validate host supervisor self-fencing under lease supersession."""

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
    print(f"[loop-system-host-supervisor-lease-fencing][FAIL] {msg}", file=sys.stderr)
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


def _spawn_host_supervisor(repo_root: Path, *, poll_interval_s: float = 0.1, idle_exit_after_s: float = 5.0) -> subprocess.Popen[str]:
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
        live_supervisor_runtime,
        supervisor_runtime_ref,
    )
    from loop_product.kernel import query_lease_fencing_matrix_view, query_runtime_liveness_view

    with tempfile.TemporaryDirectory(prefix="loop_system_host_supervisor_lease_fencing_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        supervisor_a = None
        supervisor_b = None
        try:
            supervisor_a = _spawn_host_supervisor(repo_root, poll_interval_s=0.1, idle_exit_after_s=5.0)
            if not _wait_until(lambda: live_supervisor_runtime(repo_root=repo_root) is not None, timeout_s=4.0):
                return _fail("first host supervisor startup must materialize a live runtime marker")
            runtime_a = live_supervisor_runtime(repo_root=repo_root) or {}
            pid_a = int(runtime_a.get("pid") or 0)
            if pid_a <= 0:
                return _fail("first host supervisor runtime marker must expose a live pid")

            supervisor_b = _spawn_host_supervisor(repo_root, poll_interval_s=0.1, idle_exit_after_s=5.0)
            supervisor_root = host_supervisor_runtime_root(repo_root=repo_root)

            def _effective_head() -> dict[str, object]:
                view = query_runtime_liveness_view(supervisor_root)
                return dict(dict(view.get("effective_runtime_liveness_by_node") or {}).get(HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID) or {})

            if not _wait_until(
                lambda: int(dict(_effective_head().get("payload") or {}).get("lease_epoch") or 0) >= 2,
                timeout_s=4.0,
            ):
                return _fail("second host supervisor startup must supersede authority with a higher lease epoch")

            effective = _effective_head()
            payload = dict(effective.get("payload") or {})
            pid_b = int(payload.get("pid") or 0)
            if pid_b <= 0 or pid_b == pid_a:
                return _fail("superseding host supervisor must become authoritative with a distinct pid")

            if not _wait_until(
                lambda: supervisor_a.poll() is not None
                or not _process_alive(pid_a, "loop_product.host_child_launch_supervisor", repo_root=repo_root),
                timeout_s=4.0,
            ):
                return _fail("superseded host supervisor must self-fence and exit after losing lease authority")
            if not _process_alive(pid_b, "loop_product.host_child_launch_supervisor", repo_root=repo_root):
                return _fail("newer host supervisor must remain alive after fencing out the old supervisor")

            runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
            if not runtime_ref.exists():
                return _fail("surviving host supervisor must keep the compatibility runtime marker")
            runtime_payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
            if int(runtime_payload.get("pid") or 0) != pid_b:
                return _fail("runtime marker must remain owned by the surviving host supervisor after fencing")

            effective = _effective_head()
            effective_payload = dict(effective.get("payload") or {})
            if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
                return _fail("fenced host supervisor exit must not dethrone the newer supervisor's effective authority")
            if int(effective_payload.get("pid") or 0) != pid_b:
                return _fail("effective host supervisor authority must remain pinned to the newer supervisor after fencing")
            matrix = query_lease_fencing_matrix_view(supervisor_root)
            matrix_summary = dict(matrix.get("lease_fencing_summary") or {})
            rows = {
                str(item.get("node_id") or ""): dict(item)
                for item in list(matrix.get("matrix_rows") or [])
            }
            row = dict(rows.get(HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID) or {})
            matrix_cell = str(row.get("matrix_cell") or "")
            if matrix_cell not in {"fenced_owner_exit_overridden", "authoritative_attached"}:
                return _fail(
                    "lease fencing matrix must preserve host supervisor fencing truth as either a transient fenced-owner override "
                    "or the settled authoritative attached state"
                )
            if matrix_cell == "fenced_owner_exit_overridden":
                if int(matrix_summary.get("fenced_owner_exit_overridden_node_count") or 0) != 1:
                    return _fail("lease fencing matrix must count one fenced-owner exit override during host supervisor fencing")
            else:
                if int(matrix_summary.get("fenced_owner_exit_overridden_node_count") or 0) != 0:
                    return _fail("settled host supervisor truth must not keep a stale fenced-owner override count")
                if int(matrix_summary.get("authoritative_attached_node_count") or 0) < 1:
                    return _fail("settled host supervisor truth must remain represented as authoritative_attached")
                if int(row.get("effective_lease_epoch") or 0) < 2:
                    return _fail("settled host supervisor truth must remain pinned to the superseding lease epoch")

            exit_events = [
                event
                for event in iter_committed_events(supervisor_root)
                if str(event.get("event_type") or "") == "process_exit_observed"
                and str(event.get("node_id") or "") == HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID
                and int(dict(event.get("payload") or {}).get("pid") or 0) == pid_a
            ]
            if not exit_events:
                return _fail("fenced host supervisor must still leave committed process_exit_observed evidence")
        finally:
            for proc in (supervisor_a, supervisor_b):
                if proc is None:
                    continue
                if proc.poll() is None:
                    try:
                        os.kill(proc.pid, signal.SIGTERM)
                    except OSError:
                        pass
            _cleanup_repo_services(repo_root)

    print("[loop-system-host-supervisor-lease-fencing] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
