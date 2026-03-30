#!/usr/bin/env python3
"""Validate committed authority mirroring for the repo control-plane service."""

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
    print(f"[loop-system-repo-control-plane-authority-mirror][FAIL] {msg}", file=sys.stderr)
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


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime.control_plane import (
        REPO_CONTROL_PLANE_NODE_ID,
        ensure_repo_control_plane_services_running,
        repo_control_plane_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_authority_mirror_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        control_plane_pid = 0
        try:
            payload = ensure_repo_control_plane_services_running(repo_root=repo_root)
            control_plane = dict(payload.get("repo_control_plane") or {})
            control_plane_pid = int(control_plane.get("pid") or 0)
            if control_plane_pid <= 0:
                return _fail("repo control-plane bootstrap must expose a live repo control-plane pid")
            if not _process_alive(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                return _fail("repo control-plane bootstrap must start a live repo control-plane process")

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

            liveness_view = query_runtime_liveness_view(control_plane_root)
            effective = dict(
                dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(REPO_CONTROL_PLANE_NODE_ID) or {}
            )
            if str(effective.get("effective_attachment_state") or "") != "ATTACHED":
                return _fail("repo control-plane authority mirror must report ATTACHED while the service is alive")
            if int(dict(effective.get("payload") or {}).get("pid") or 0) != control_plane_pid:
                return _fail("repo control-plane liveness payload must preserve control-plane pid identity")

            if not _wait_until(
                lambda: bool(
                    [
                        event
                        for event in iter_committed_events(control_plane_root)
                        if str(event.get("event_type") or "") == "process_identity_confirmed"
                        and str(event.get("node_id") or "") == REPO_CONTROL_PLANE_NODE_ID
                    ]
                ),
                timeout_s=3.0,
            ):
                return _fail("repo control-plane startup must commit process_identity_confirmed")

            identity_events = [
                event
                for event in iter_committed_events(control_plane_root)
                if str(event.get("event_type") or "") == "process_identity_confirmed"
                and str(event.get("node_id") or "") == REPO_CONTROL_PLANE_NODE_ID
            ]
            if not identity_events:
                return _fail("repo control-plane startup must commit process_identity_confirmed")
            identity_payload = dict(identity_events[-1].get("payload") or {})
            if int(identity_payload.get("pid") or 0) != control_plane_pid:
                return _fail("repo control-plane process_identity_confirmed must preserve pid identity")
            if not str(identity_payload.get("process_fingerprint") or ""):
                return _fail("repo control-plane process_identity_confirmed must preserve process fingerprint")

            baseline_count = int(liveness_view.get("runtime_liveness_event_count") or 0)
            if not _wait_until(
                lambda: int(query_runtime_liveness_view(control_plane_root).get("runtime_liveness_event_count") or 0)
                > baseline_count,
                timeout_s=3.0,
            ):
                return _fail("live repo control-plane service must refresh committed liveness while it remains running")
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-authority-mirror] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
