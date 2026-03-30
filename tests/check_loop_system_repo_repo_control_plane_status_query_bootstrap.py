#!/usr/bin/env python3
"""Validate status-query read-only semantics for repo control-plane residency."""

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
    print(f"[loop-system-repo-control-plane-status-query-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def _materialize_runtime_root(state_root: Path) -> None:
    from loop_product.runtime_identity import ensure_runtime_root_identity

    resolved = state_root.resolve()
    for rel in ("state", "cache", "audit", "artifacts", "quarantine"):
        (resolved / rel).mkdir(parents=True, exist_ok=True)
    ensure_runtime_root_identity(resolved)


def _exercise_status_query_bootstrap(*, query_kind: str) -> int:
    from loop_product.kernel import query_operational_hygiene_view, query_runtime_liveness_view
    from loop_product.runtime.control_plane import (
        live_repo_control_plane_runtime,
        repo_control_plane_runtime_root,
        repo_reactor_residency_guard_runtime_root,
    )
    from loop_product.event_journal import iter_committed_events

    query_fn = {
        "runtime_liveness": query_runtime_liveness_view,
        "operational_hygiene": query_operational_hygiene_view,
    }[query_kind]

    with tempfile.TemporaryDirectory(prefix=f"loop_system_repo_control_plane_status_query_bootstrap_{query_kind}_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / f"{query_kind}-runtime"
        _materialize_runtime_root(state_root)
        try:
            if live_repo_control_plane_runtime(repo_root=repo_root) is not None:
                return _fail(f"{query_kind}: setup must start without a pre-existing repo control-plane runtime")
            if _repo_service_pids_for_repo(repo_root):
                return _fail(f"{query_kind}: setup must start without any repo-scoped services already running")
            guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
            if any(iter_committed_events(guard_root)):
                return _fail(f"{query_kind}: setup must start without any pre-existing residency-guard requirement facts")

            query_payload = dict(query_fn(state_root) or {})
            if str(query_payload.get("runtime_root") or "") != str(state_root.resolve()):
                return _fail(f"{query_kind}: trusted status query must preserve normal payload semantics")

            control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
            if live_repo_control_plane_runtime(repo_root=repo_root) is not None:
                return _fail(f"{query_kind}: status query must remain read-only and must not start repo control-plane")
            if bool(_repo_control_plane_payload(control_plane_root=control_plane_root)):
                return _fail(f"{query_kind}: status query must not materialize repo control-plane authority as a side effect")
            if _repo_service_pids_for_repo(repo_root):
                return _fail(f"{query_kind}: status query must not leave any repo-scoped service processes behind")
            if any(iter_committed_events(guard_root)):
                return _fail(f"{query_kind}: status query must not append repo residency requirement facts")

            query_payload_b = dict(query_fn(state_root) or {})
            if str(query_payload_b.get("runtime_root") or "") != str(state_root.resolve()):
                return _fail(f"{query_kind}: repeated status query must preserve normal payload semantics")
            if live_repo_control_plane_runtime(repo_root=repo_root) is not None:
                return _fail(f"{query_kind}: repeated status query must still remain read-only")
            if _repo_service_pids_for_repo(repo_root):
                return _fail(f"{query_kind}: repeated status query must not leave any repo-scoped service processes behind")
        finally:
            _cleanup_repo_services(repo_root)
    return 0


def main() -> int:
    for query_kind in ("runtime_liveness", "operational_hygiene"):
        result = _exercise_status_query_bootstrap(query_kind=query_kind)
        if result != 0:
            return result
    print("[loop-system-repo-control-plane-status-query-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
