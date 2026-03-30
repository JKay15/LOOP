#!/usr/bin/env python3
"""Validate ambient repo-global service residency from top-level runtime-tree initialization."""

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
    print(f"[loop-system-repo-control-plane-runtime-tree-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def _service_mode(payload: dict[str, object]) -> float:
    try:
        return float(payload.get("idle_exit_after_s"))
    except (TypeError, ValueError):
        return -1.0


def _repo_reactor_residency_required_events(*, guard_root: Path, trigger_kind: str, trigger_ref: str) -> list[dict[str, object]]:
    from loop_product.event_journal import iter_committed_events

    return [
        event
        for event in iter_committed_events(guard_root)
        if str(event.get("event_type") or "") == "repo_reactor_residency_required"
        and str(dict(event.get("payload") or {}).get("trigger_kind") or "") == trigger_kind
        and str(dict(event.get("payload") or {}).get("trigger_ref") or "") == trigger_ref
    ]


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
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.kernel.state import ensure_runtime_tree
    from loop_product.runtime import live_housekeeping_reap_controller_runtime
    from loop_product.runtime.control_plane import (
        live_repo_control_plane_runtime,
        live_repo_reactor_residency_guard_runtime,
        live_repo_reactor_runtime,
        repo_reactor_residency_guard_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_runtime_tree_bootstrap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = (repo_root / ".loop").resolve()
        guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
        guard_pid = 0
        reactor_pid = 0
        control_plane_pid = 0
        supervisor_pid = 0
        housekeeping_pid = 0
        try:
            if live_repo_reactor_residency_guard_runtime(repo_root=repo_root) is not None:
                return _fail("setup must start without a pre-existing residency guard runtime")
            if live_repo_reactor_runtime(repo_root=repo_root) is not None:
                return _fail("setup must start without a pre-existing repo reactor runtime")
            if live_repo_control_plane_runtime(repo_root=repo_root) is not None:
                return _fail("setup must start without a pre-existing repo control-plane runtime")
            if supervisor_module.live_supervisor_runtime(repo_root=repo_root) is not None:
                return _fail("setup must start without a pre-existing host supervisor runtime")
            if live_housekeeping_reap_controller_runtime(repo_root=repo_root) is not None:
                return _fail("setup must start without a pre-existing housekeeping runtime")
            if _repo_service_pids_for_repo(repo_root):
                return _fail("setup must start without any repo-scoped services already running")

            ensure_runtime_tree(state_root)

            if not _wait_until(
                lambda: (
                    (guard := live_repo_reactor_residency_guard_runtime(repo_root=repo_root)) is not None
                    and int(guard.get("pid") or 0) > 0
                    and _process_alive_with_marker(
                        int(guard.get("pid") or 0),
                        "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
                        repo_root=repo_root,
                    )
                    and (reactor := live_repo_reactor_runtime(repo_root=repo_root)) is not None
                    and int(reactor.get("pid") or 0) > 0
                    and _process_alive_with_marker(
                        int(reactor.get("pid") or 0),
                        "loop_product.runtime.control_plane --run-repo-reactor",
                        repo_root=repo_root,
                    )
                    and (control_plane := live_repo_control_plane_runtime(repo_root=repo_root)) is not None
                    and int(control_plane.get("pid") or 0) > 0
                    and _process_alive_with_marker(
                        int(control_plane.get("pid") or 0),
                        "loop_product.runtime.control_plane",
                        repo_root=repo_root,
                    )
                    and (supervisor := supervisor_module.live_supervisor_runtime(repo_root=repo_root)) is not None
                    and int(supervisor.get("pid") or 0) > 0
                    and _process_alive_with_marker(
                        int(supervisor.get("pid") or 0),
                        "loop_product.host_child_launch_supervisor",
                        repo_root=repo_root,
                    )
                    and (housekeeping := live_housekeeping_reap_controller_runtime(repo_root=repo_root)) is not None
                    and int(housekeeping.get("pid") or 0) > 0
                    and _process_alive_with_marker(
                        int(housekeeping.get("pid") or 0),
                        "loop_product.runtime.gc --run-housekeeping-controller",
                        repo_root=repo_root,
                    )
                ),
                timeout_s=6.0,
            ):
                return _fail("top-level runtime-tree initialization must ambiently bootstrap the full repo-global service stack")

            guard_payload = dict(live_repo_reactor_residency_guard_runtime(repo_root=repo_root) or {})
            reactor_payload = dict(live_repo_reactor_runtime(repo_root=repo_root) or {})
            control_plane_payload = dict(live_repo_control_plane_runtime(repo_root=repo_root) or {})
            supervisor_payload = dict(supervisor_module.live_supervisor_runtime(repo_root=repo_root) or {})
            housekeeping_payload = dict(live_housekeeping_reap_controller_runtime(repo_root=repo_root) or {})

            guard_pid = int(guard_payload.get("pid") or 0)
            reactor_pid = int(reactor_payload.get("pid") or 0)
            control_plane_pid = int(control_plane_payload.get("pid") or 0)
            supervisor_pid = int(supervisor_payload.get("pid") or 0)
            housekeeping_pid = int(housekeeping_payload.get("pid") or 0)
            if min(guard_pid, reactor_pid, control_plane_pid, supervisor_pid, housekeeping_pid) <= 0:
                return _fail("runtime-tree ambient bootstrap must expose live pids for all repo-global services")

            if not _process_alive_with_marker(
                guard_pid,
                "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
                repo_root=repo_root,
            ):
                return _fail("runtime-tree ambient bootstrap must start a live residency guard process")
            if not _process_alive_with_marker(
                reactor_pid,
                "loop_product.runtime.control_plane --run-repo-reactor",
                repo_root=repo_root,
            ):
                return _fail("runtime-tree ambient bootstrap must start a live repo reactor process")
            if not _process_alive_with_marker(
                control_plane_pid,
                "loop_product.runtime.control_plane",
                repo_root=repo_root,
            ):
                return _fail("runtime-tree ambient bootstrap must start a live repo control-plane process")
            if not _process_alive_with_marker(
                supervisor_pid,
                "loop_product.host_child_launch_supervisor",
                repo_root=repo_root,
            ):
                return _fail("runtime-tree ambient bootstrap must start a live host supervisor process")
            if not _process_alive_with_marker(
                housekeeping_pid,
                "loop_product.runtime.gc --run-housekeeping-controller",
                repo_root=repo_root,
            ):
                return _fail("runtime-tree ambient bootstrap must start a live housekeeping process")

            if any(
                _service_mode(payload) != 0.0
                for payload in (
                    guard_payload,
                    reactor_payload,
                    control_plane_payload,
                    supervisor_payload,
                    housekeeping_payload,
                )
            ):
                return _fail("runtime-tree ambient bootstrap must keep the full repo-global service stack in service mode")

            requirement_events = _repo_reactor_residency_required_events(
                guard_root=guard_root,
                trigger_kind="runtime_tree_init",
                trigger_ref=str(state_root),
            )
            if len(requirement_events) != 1:
                return _fail(
                    "runtime-tree ambient bootstrap must publish exactly one canonical runtime_tree_init residency-guard requirement event"
                )

            time.sleep(1.0)
            if not all(
                (
                    _process_alive_with_marker(
                        guard_pid,
                        "loop_product.runtime.control_plane --run-repo-reactor-residency-guard",
                        repo_root=repo_root,
                    ),
                    _process_alive_with_marker(
                        reactor_pid,
                        "loop_product.runtime.control_plane --run-repo-reactor",
                        repo_root=repo_root,
                    ),
                    _process_alive_with_marker(
                        control_plane_pid,
                        "loop_product.runtime.control_plane",
                        repo_root=repo_root,
                    ),
                    _process_alive_with_marker(
                        supervisor_pid,
                        "loop_product.host_child_launch_supervisor",
                        repo_root=repo_root,
                    ),
                    _process_alive_with_marker(
                        housekeeping_pid,
                        "loop_product.runtime.gc --run-housekeeping-controller",
                        repo_root=repo_root,
                    ),
                )
            ):
                return _fail(
                    "repo-global services bootstrapped from top-level runtime-tree initialization must remain resident without child-runtime demand"
                )

            ensure_runtime_tree(state_root)
            if int(dict(live_repo_reactor_residency_guard_runtime(repo_root=repo_root) or {}).get("pid") or 0) != guard_pid:
                return _fail("repeated top-level runtime-tree initialization must reuse the same residency guard process")
            if int(dict(live_repo_reactor_runtime(repo_root=repo_root) or {}).get("pid") or 0) != reactor_pid:
                return _fail("repeated top-level runtime-tree initialization must reuse the same repo reactor process")
            if int(dict(live_repo_control_plane_runtime(repo_root=repo_root) or {}).get("pid") or 0) != control_plane_pid:
                return _fail("repeated top-level runtime-tree initialization must reuse the same repo control-plane process")
            if int(dict(supervisor_module.live_supervisor_runtime(repo_root=repo_root) or {}).get("pid") or 0) != supervisor_pid:
                return _fail("repeated top-level runtime-tree initialization must reuse the same host supervisor process")
            if int(dict(live_housekeeping_reap_controller_runtime(repo_root=repo_root) or {}).get("pid") or 0) != housekeeping_pid:
                return _fail("repeated top-level runtime-tree initialization must reuse the same housekeeping process")
            requirement_events_b = _repo_reactor_residency_required_events(
                guard_root=guard_root,
                trigger_kind="runtime_tree_init",
                trigger_ref=str(state_root),
            )
            if len(requirement_events_b) != 1:
                return _fail(
                    "repeated top-level runtime-tree initialization must dedupe the canonical runtime_tree_init residency-guard requirement event"
                )
        finally:
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-runtime-tree-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
