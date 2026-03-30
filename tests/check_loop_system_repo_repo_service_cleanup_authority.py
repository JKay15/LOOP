#!/usr/bin/env python3
"""Validate repo-scope service cleanup through committed authority rather than direct kill loops."""

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
    print(f"[loop-system-repo-service-cleanup-authority][FAIL] {msg}", file=sys.stderr)
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


def _matching_repo_service_processes(*, repo_root: Path) -> list[tuple[int, str]]:
    proc = subprocess.run(
        ["ps", "-axww", "-o", "pid=,command="],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return []
    repo_marker = str(repo_root.resolve())
    needles = (
        "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
        "loop_product.runtime.control_plane --run-repo-reactor",
        "loop_product.runtime.control_plane",
        "loop_product.host_child_launch_supervisor",
        "loop_product.runtime.gc --run-housekeeping-controller",
    )
    matches: list[tuple[int, str]] = []
    for raw_line in str(proc.stdout or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        command = parts[1]
        if repo_marker not in command:
            continue
        if any(all(fragment in command for fragment in needle.split()) for needle in needles):
            matches.append((pid, command))
    return matches


def main() -> int:
    try:
        from loop_product import host_child_launch_supervisor as supervisor_module
        from loop_product.kernel.query import query_operational_hygiene_view
        from loop_product.runtime import (
            cleanup_test_repo_services,
            ensure_repo_control_plane_services_running,
            live_housekeeping_reap_controller_runtime,
        )
        from loop_product.runtime.control_plane import (
            live_repo_control_plane_runtime,
            live_repo_reactor_residency_guard_runtime,
            live_repo_reactor_runtime,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(f"repo cleanup authority imports failed: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_service_cleanup_authority_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"

        payload = ensure_repo_control_plane_services_running(repo_root=repo_root)
        guard = dict(payload.get("repo_reactor_residency_guard") or {})
        reactor = dict(payload.get("repo_reactor") or {})
        control_plane = dict(payload.get("repo_control_plane") or {})
        supervisor = dict(payload.get("host_child_launch_supervisor") or {})
        housekeeping = dict(payload.get("housekeeping_reap_controller") or {})

        guard_pid = int(guard.get("pid") or 0)
        reactor_pid = int(reactor.get("pid") or 0)
        control_plane_pid = int(control_plane.get("pid") or 0)
        supervisor_pid = int(supervisor.get("pid") or 0)
        housekeeping_pid = int(housekeeping.get("pid") or 0)
        if min(guard_pid, reactor_pid, control_plane_pid, supervisor_pid, housekeeping_pid) <= 0:
            return _fail(
                "setup must bootstrap repo reactor residency guard, repo reactor, repo control-plane, host supervisor, and housekeeping before cleanup authority is tested"
            )
        if not _process_alive(
            guard_pid,
            "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
            repo_root=repo_root,
        ):
            return _fail("setup must start a live repo reactor residency guard process")
        if not _process_alive(reactor_pid, "loop_product.runtime.control_plane --run-repo-reactor", repo_root=repo_root):
            return _fail("setup must start a live repo reactor process")
        if not _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
            return _fail("setup must start a live repo control-plane process")
        if not _process_alive(supervisor_pid, "loop_product.host_child_launch_supervisor", repo_root=repo_root):
            return _fail("setup must start a live host supervisor process")
        if not _process_alive(housekeeping_pid, "loop_product.runtime.gc", repo_root=repo_root):
            return _fail("setup must start a live housekeeping controller process")

        cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root)
        if not bool(cleanup_receipt.get("quiesced")):
            return _fail(f"repo-service cleanup authority must quiesce repo-scope services, got: {cleanup_receipt}")
        if dict(cleanup_receipt.get("remaining_retired_pids") or {}):
            return _fail(f"repo-service cleanup authority must not declare quiesced while retired pids still live: {cleanup_receipt}")

        if not _wait_until(lambda: live_repo_reactor_residency_guard_runtime(repo_root=repo_root) is None, timeout_s=4.0):
            return _fail("repo-service cleanup authority must retire repo reactor residency guard without direct test kill")
        if not _wait_until(lambda: live_repo_reactor_runtime(repo_root=repo_root) is None, timeout_s=4.0):
            return _fail("repo-service cleanup authority must retire repo reactor without direct test kill")
        if not _wait_until(lambda: live_repo_control_plane_runtime(repo_root=repo_root) is None, timeout_s=4.0):
            return _fail("repo-service cleanup authority must retire repo control-plane without direct test kill")
        if not _wait_until(lambda: supervisor_module.live_supervisor_runtime(repo_root=repo_root) is None, timeout_s=4.0):
            return _fail("repo-service cleanup authority must retire host supervisor without direct test kill")
        if not _wait_until(lambda: live_housekeeping_reap_controller_runtime(repo_root=repo_root) is None, timeout_s=4.0):
            return _fail("repo-service cleanup authority must retire housekeeping without direct test kill")

        if _process_alive(guard_pid, "loop_product.runtime.control_plane --run-repo-reactor-residency-guard", repo_root=repo_root):
            return _fail("repo reactor residency guard pid must not remain alive after committed cleanup retirement")
        if _process_alive(reactor_pid, "loop_product.runtime.control_plane --run-repo-reactor", repo_root=repo_root):
            return _fail("repo reactor pid must not remain alive after committed cleanup retirement")
        if _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
            return _fail("repo control-plane pid must not remain alive after committed cleanup retirement")
        if _process_alive(supervisor_pid, "loop_product.host_child_launch_supervisor", repo_root=repo_root):
            return _fail("host supervisor pid must not remain alive after committed cleanup retirement")
        if _process_alive(housekeeping_pid, "loop_product.runtime.gc", repo_root=repo_root):
            return _fail("housekeeping controller pid must not remain alive after committed cleanup retirement")
        matching_processes = _matching_repo_service_processes(repo_root=repo_root)
        if matching_processes:
            return _fail(f"repo-service cleanup authority must leave no repo-scope service residue, got: {matching_processes}")

        housekeeping_root = repo_root / ".loop" / "housekeeping"
        hygiene = query_operational_hygiene_view(
            housekeeping_root,
            include_heavy_object_summaries=False,
        )
        if int(hygiene.get("test_runtime_cleanup_committed_event_count") or 0) < 5:
            return _fail("repo-service cleanup authority must commit cleanup facts for each repo-scope service")
        if int(hygiene.get("runtime_root_quiesced_event_count") or 0) < 5:
            return _fail("repo-service cleanup authority must commit quiesced facts for each repo-scope service")

    print("[loop-system-repo-service-cleanup-authority] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
