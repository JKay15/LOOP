#!/usr/bin/env python3
"""Validate repo-lifecycle bootstrap of the repo control-plane before explicit helper use."""

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
    print(f"[loop-system-repo-control-plane-repo-lifecycle-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def _repo_control_plane_payload(*, control_plane_root: Path) -> dict[str, object]:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime.control_plane import REPO_CONTROL_PLANE_NODE_ID

    effective = dict(
        dict(query_runtime_liveness_view(control_plane_root).get("effective_runtime_liveness_by_node") or {}).get(
            REPO_CONTROL_PLANE_NODE_ID
        )
        or {}
    )
    return dict(effective.get("payload") or {})


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


def main() -> int:
    from loop_product.runtime.control_plane import repo_control_plane_runtime_root
    from loop_product.runtime.lifecycle import initialize_evaluator_runtime

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_repo_lifecycle_bootstrap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        control_plane_pid = 0
        try:
            runtime_a = (repo_root / ".loop" / "runtime-a").resolve()
            runtime_b = (repo_root / ".loop" / "runtime-b").resolve()

            initialize_evaluator_runtime(
                state_root=runtime_a,
                task_id="repo-control-plane-lifecycle-a",
                root_goal="bootstrap repo lifecycle A",
                child_goal_slice="bootstrap repo lifecycle A child",
            )

            control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
            if not _wait_until(lambda: bool(_repo_control_plane_payload(control_plane_root=control_plane_root)), timeout_s=4.0):
                return _fail("repo lifecycle bootstrap must bring up repo control-plane authority before explicit helper use")

            payload_a = _repo_control_plane_payload(control_plane_root=control_plane_root)
            control_plane_pid = int(payload_a.get("pid") or 0)
            if control_plane_pid <= 0:
                return _fail("repo lifecycle bootstrap must expose a live repo control-plane pid")
            if not _process_alive_with_marker(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                return _fail("repo lifecycle bootstrap must start a live repo control-plane process")
            try:
                idle_exit_after_s = float(payload_a.get("idle_exit_after_s"))
            except (TypeError, ValueError):
                idle_exit_after_s = -1.0
            if idle_exit_after_s != 0.0:
                return _fail("repo lifecycle bootstrap must start repo control-plane in service mode")

            initialize_evaluator_runtime(
                state_root=runtime_b,
                task_id="repo-control-plane-lifecycle-b",
                root_goal="bootstrap repo lifecycle B",
                child_goal_slice="bootstrap repo lifecycle B child",
            )

            payload_b = _repo_control_plane_payload(control_plane_root=control_plane_root)
            control_plane_pid_b = int(payload_b.get("pid") or 0)
            if control_plane_pid_b != control_plane_pid:
                return _fail("repeated repo lifecycle bootstrap must reuse the same repo control-plane")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-repo-lifecycle-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
