#!/usr/bin/env python3
"""Validate canonical repo control-plane requirement facts across trusted bootstrap triggers."""

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
    print(f"[loop-system-repo-control-plane-requirement-event][FAIL] {msg}", file=sys.stderr)
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

    cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    if not bool(cleanup_receipt.get("quiesced")):
        raise RuntimeError(
            "repo-control-plane requirement-event cleanup must quiesce repo services, "
            f"got: {cleanup_receipt}"
        )
    if dict(cleanup_receipt.get("remaining_live_services") or {}):
        raise RuntimeError(
            "repo-control-plane requirement-event cleanup must not leave repo services behind, "
            f"got: {cleanup_receipt}"
        )
    if dict(cleanup_receipt.get("remaining_retired_pids") or {}):
        raise RuntimeError(
            "repo-control-plane requirement-event cleanup must not leave retired repo service pids behind, "
            f"got: {cleanup_receipt}"
        )


def _required_events(*, control_plane_root: Path) -> list[dict[str, object]]:
    from loop_product.event_journal import iter_committed_events

    return [
        event
        for event in iter_committed_events(control_plane_root)
        if str(event.get("event_type") or "") == "repo_control_plane_required"
    ]


def _reactor_required_events(*, reactor_root: Path) -> list[dict[str, object]]:
    from loop_product.event_journal import iter_committed_events

    return [
        event
        for event in iter_committed_events(reactor_root)
        if str(event.get("event_type") or "") == "repo_reactor_required"
    ]


def _reactor_residency_required_events(*, guard_root: Path) -> list[dict[str, object]]:
    from loop_product.event_journal import iter_committed_events

    return [
        event
        for event in iter_committed_events(guard_root)
        if str(event.get("event_type") or "") == "repo_reactor_residency_required"
    ]


def main() -> int:
    from loop_product.kernel import query_operational_hygiene_view, query_runtime_liveness_view
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.runtime.control_plane import (
        ensure_repo_control_plane_services_running,
        live_repo_control_plane_runtime,
        repo_control_plane_runtime_root,
        repo_reactor_residency_guard_runtime_root,
        repo_reactor_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_requirement_event_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "requirement-runtime"
        control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
        guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
        reactor_root = repo_reactor_runtime_root(repo_root=repo_root)
        try:
            ensure_runtime_tree(state_root)
            root_node = NodeSpec(
                node_id="root-kernel",
                node_kind="kernel",
                goal_slice="requirement fact repo control-plane bootstrap",
                parent_node_id=None,
                generation=0,
                round_id="R0",
                execution_policy={"mode": "kernel"},
                reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
                budget_profile={"max_rounds": 1},
                allowed_actions=["dispatch", "submit", "audit"],
                delegation_ref="",
                result_sink_ref="artifacts/repo_control_plane_requirement.json",
                lineage_ref="root-kernel",
                status=NodeStatus.ACTIVE,
            )
            kernel_state = KernelState(
                task_id="repo-control-plane-requirement-event",
                root_goal="publish canonical repo control-plane requirement facts before convergence without query side effects",
                root_node_id=root_node.node_id,
            )
            kernel_state.register_node(root_node)
            persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

            initial_control_plane = dict(live_repo_control_plane_runtime(repo_root=repo_root) or {})
            initial_control_plane_pid = int(initial_control_plane.get("pid") or 0)
            if initial_control_plane_pid <= 0 or not _process_alive(
                initial_control_plane_pid,
                "loop_product.runtime.control_plane",
                repo_root=repo_root,
            ):
                return _fail("runtime-tree init must ambiently converge a live repo control-plane runtime before explicit helper use")

            initial_guard_events = _reactor_residency_required_events(guard_root=guard_root)
            if len(initial_guard_events) != 1:
                return _fail("runtime-tree init must commit exactly one deterministic repo_reactor_residency_required fact")
            if str(dict(initial_guard_events[-1].get("payload") or {}).get("trigger_kind") or "") != "runtime_tree_init":
                return _fail("ambient repo residency bootstrap must preserve trigger_kind=runtime_tree_init")

            initial_reactor_events = _reactor_required_events(reactor_root=reactor_root)
            if len(initial_reactor_events) != 1:
                return _fail("ambient repo bootstrap must commit exactly one deterministic repo_reactor_required fact")
            if str(dict(initial_reactor_events[-1].get("payload") or {}).get("trigger_kind") or "") != "repo_reactor_residency_watchdog":
                return _fail("ambient repo reactor requirement must preserve trigger_kind=repo_reactor_residency_watchdog")

            initial_control_plane_events = _required_events(control_plane_root=control_plane_root)
            if len(initial_control_plane_events) != 1:
                return _fail("ambient repo bootstrap must commit exactly one deterministic repo_control_plane_required fact")
            if str(dict(initial_control_plane_events[-1].get("payload") or {}).get("trigger_kind") or "") != "repo_reactor_watchdog":
                return _fail("ambient repo control-plane requirement must preserve trigger_kind=repo_reactor_watchdog")

            explicit = ensure_repo_control_plane_services_running(repo_root=repo_root)
            explicit_pid = int(dict(explicit.get("repo_control_plane") or {}).get("pid") or 0)
            if explicit_pid <= 0 or not _process_alive(explicit_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                return _fail("explicit bootstrap must still converge a live repo control-plane service")

            if not _wait_until(
                lambda: len(_reactor_residency_required_events(guard_root=guard_root)) >= 1,
                timeout_s=4.0,
            ):
                return _fail("explicit repo bootstrap must commit repo_reactor_residency_required")
            explicit_guard_events = [
                event
                for event in _reactor_residency_required_events(guard_root=guard_root)
                if str(dict(event.get("payload") or {}).get("trigger_kind") or "") == "explicit_repo_bootstrap"
            ]
            if len(explicit_guard_events) != 1:
                return _fail(
                    "explicit repo bootstrap must commit exactly one deterministic explicit_repo_bootstrap residency-guard requirement fact"
                )
            explicit_guard_event_id = str(explicit_guard_events[-1].get("event_id") or "")

            control_plane_watchdog_events = [
                event
                for event in _required_events(control_plane_root=control_plane_root)
                if str(dict(event.get("payload") or {}).get("trigger_kind") or "") == "repo_reactor_watchdog"
            ]
            if not control_plane_watchdog_events:
                return _fail("explicit repo bootstrap must still converge repo control-plane through repo_reactor_watchdog facts")

            ensure_repo_control_plane_services_running(repo_root=repo_root)
            explicit_guard_events_again = [
                event
                for event in _reactor_residency_required_events(guard_root=guard_root)
                if str(dict(event.get("payload") or {}).get("trigger_kind") or "") == "explicit_repo_bootstrap"
            ]
            if (
                len(explicit_guard_events_again) != 1
                or str(explicit_guard_events_again[-1].get("event_id") or "") != explicit_guard_event_id
            ):
                return _fail("repeating explicit repo bootstrap must not append duplicate explicit residency-guard requirement facts")

            baseline_guard_event_count = len(_reactor_residency_required_events(guard_root=guard_root))
            baseline_reactor_event_count = len(_reactor_required_events(reactor_root=reactor_root))
            baseline_control_plane_event_count = len(_required_events(control_plane_root=control_plane_root))

            authority_view = query_authority_view(state_root)
            if authority_view.get("root_node_id") != root_node.node_id:
                return _fail("authority query must preserve normal authority-view semantics")
            liveness_view = dict(query_runtime_liveness_view(state_root) or {})
            if str(liveness_view.get("runtime_root") or "") != str(state_root.resolve()):
                return _fail("runtime liveness query must preserve normal status-view semantics")
            hygiene_view = dict(
                query_operational_hygiene_view(
                    state_root,
                    include_heavy_object_summaries=False,
                )
                or {}
            )
            if str(hygiene_view.get("runtime_root") or "") != str(state_root.resolve()):
                return _fail("operational hygiene query must preserve normal status-view semantics")

            if len(_reactor_residency_required_events(guard_root=guard_root)) != baseline_guard_event_count:
                return _fail("ordinary authority/status queries must not append repo residency requirement facts")
            if len(_reactor_required_events(reactor_root=reactor_root)) != baseline_reactor_event_count:
                return _fail("ordinary authority/status queries must not append repo reactor requirement facts")
            if len(_required_events(control_plane_root=control_plane_root)) != baseline_control_plane_event_count:
                return _fail("ordinary authority/status queries must not append repo control-plane requirement facts")
        finally:
            _cleanup_repo_services(repo_root)
    print("[loop-system-repo-control-plane-requirement-event] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
