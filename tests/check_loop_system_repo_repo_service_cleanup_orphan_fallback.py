#!/usr/bin/env python3
"""Validate deterministic orphan cleanup when repo-scope service markers are lost."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-service-cleanup-orphan-fallback][FAIL] {msg}", file=sys.stderr)
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
    from loop_product.runtime import cleanup_test_repo_services, ensure_repo_control_plane_services_running

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_service_cleanup_orphan_fallback_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"

        payload = ensure_repo_control_plane_services_running(repo_root=repo_root)
        expected_service_pids = {
            "repo_reactor_residency_guard": int(
                dict(payload.get("repo_reactor_residency_guard") or {}).get("pid") or 0
            ),
            "repo_reactor": int(dict(payload.get("repo_reactor") or {}).get("pid") or 0),
            "repo_control_plane": int(dict(payload.get("repo_control_plane") or {}).get("pid") or 0),
            "host_child_launch_supervisor": int(
                dict(payload.get("host_child_launch_supervisor") or {}).get("pid") or 0
            ),
            "housekeeping": int(dict(payload.get("housekeeping_reap_controller") or {}).get("pid") or 0),
        }
        pids = list(expected_service_pids.values())
        if any(pid <= 0 for pid in pids):
            return _fail(
                "setup must start repo reactor residency guard, repo reactor, repo control-plane, host supervisor, and housekeeping services"
            )
        if not _wait_until(
            lambda: all(_pid_alive(pid) for pid in pids),
            timeout_s=3.0,
        ):
            return _fail("setup must keep repo control-plane, host supervisor, and housekeeping alive before orphan fallback is exercised")

        for runtime_name in (
            "repo_reactor_residency_guard",
            "repo_reactor",
            "repo_control_plane",
            "host_child_launch_supervisor",
            "housekeeping",
        ):
            shutil.rmtree(repo_root / ".loop" / runtime_name, ignore_errors=True)
        for pid in pids:
            if not _pid_alive(pid):
                return _fail("marker-loss setup must keep the original repo-scope processes alive")

        cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
        if not bool(cleanup_receipt.get("quiesced")):
            return _fail(f"cleanup helper must still quiesce orphaned repo-scope processes, got: {cleanup_receipt}")
        if dict(cleanup_receipt.get("remaining_live_services") or {}):
            return _fail(f"cleanup helper must not leave authority-visible repo services behind, got: {cleanup_receipt}")
        if dict(cleanup_receipt.get("remaining_retired_pids") or {}):
            return _fail(f"cleanup helper must not leave retired repo-scope pids behind, got: {cleanup_receipt}")
        if dict(cleanup_receipt.get("remaining_orphan_service_pids") or {}):
            return _fail(f"cleanup helper must not declare quiesced while orphan repo-scope pids still live, got: {cleanup_receipt}")
        cleanup_event_ids = dict(cleanup_receipt.get("cleanup_event_ids") or {})
        expected_cleanup_kinds = {
            "repo_reactor_residency_guard",
            "repo_reactor",
            "repo_control_plane",
            "host_child_launch_supervisor",
            "housekeeping",
        }
        covered_cleanup_kinds = (
            set(cleanup_event_ids)
            | set(dict(cleanup_receipt.get("runtime_root_quiesced_event_ids") or {}))
            | set(dict(cleanup_receipt.get("fallback_terminated_orphan_pids") or {}))
        )
        missing_cleanup_kinds = expected_cleanup_kinds - covered_cleanup_kinds
        if missing_cleanup_kinds:
            still_live_missing = {
                kind: pid
                for kind, pid in expected_service_pids.items()
                if kind in missing_cleanup_kinds and pid > 0 and _pid_alive(pid)
            }
            if still_live_missing:
                return _fail(
                    "cleanup helper must either commit cleanup authority or prove terminal exit for every orphaned "
                    f"repo-scope service, got: {cleanup_receipt}, still_live_missing={still_live_missing}"
                )
        if not _wait_until(lambda: not _matching_repo_service_processes(repo_root=repo_root), timeout_s=3.0):
            return _fail(
                "cleanup helper must leave no repo-scope service residue after orphan fallback, "
                f"got: {_matching_repo_service_processes(repo_root=repo_root)}"
            )

    print("[loop-system-repo-service-cleanup-orphan-fallback] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
