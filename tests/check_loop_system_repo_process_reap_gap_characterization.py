#!/usr/bin/env python3
"""Validate repo-root resident event-driven process reclamation."""

from __future__ import annotations

import json
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
    print(f"[loop-system-process-reap-gap][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def _controller_process_alive(pid: int, *, repo_root: Path) -> bool:
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
    return (
        "loop_product.runtime.gc" in command
        and "--run-housekeeping-controller" in command
        and str(repo_root.resolve()) in command
    )


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.kernel.query import query_event_journal_status_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_process_reap_gap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "gap-run"
        workspace_root = repo_root / "workspace" / "gap-run"
        (state_root / "state").mkdir(parents=True, exist_ok=True)
        workspace_root.mkdir(parents=True, exist_ok=True)

        _write_json(
            state_root / "state" / "gap-run.json",
            {
                "node_id": "gap-run",
                "status": "BLOCKED",
                "runtime_state": {},
                "workspace_root": str(workspace_root.resolve()),
            },
        )
        _write_json(
            state_root / "state" / "child-terminal-001.json",
            {
                "node_id": "child-terminal-001",
                "status": "COMPLETED",
                "runtime_state": {},
                "workspace_root": str(workspace_root.resolve()),
            },
        )

        lingering_proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(state_root),
                str(workspace_root),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        controller_pid = 0
        try:
            journal_before = query_event_journal_status_view(state_root)
            if int(journal_before.get("committed_event_count") or 0) != 0:
                return _fail("fresh characterization root must start with zero committed journal events")

            report = gc_module.stopped_run_gc_report(state_root=state_root, repo_root=repo_root)
            if not bool(report.get("reapable")):
                return _fail("characterization setup must produce a reapable stopped run")
            if not any(int(item.get("pid") or 0) == lingering_proc.pid for item in list(report.get("owned_processes") or [])):
                return _fail("characterization setup must detect the lingering owned process")

            request = gc_module.submit_stopped_run_reap_request(
                state_root=state_root,
                repo_root=repo_root,
                controller_poll_interval_s=0.1,
                controller_idle_exit_after_s=0.75,
            )
            if not request.get("canonical_reap_requested_event_id"):
                return _fail("current characterization expects request submission to persist canonical reap intent")
            if not state_root.exists():
                return _fail("current characterization expects request submission alone not to delete the runtime root")
            if str(request.get("controller_bootstrap_status") or "") != "ready":
                return _fail("current characterization expects request submission to demand-start the housekeeping controller")
            controller_pid = int(request.get("controller_pid") or 0)
            if controller_pid <= 0:
                return _fail("current characterization expects demand-started controller bootstrap to expose a live pid")

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            controller_node_id = gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID
            deadline = time.time() + 4.0
            effective_controller = {}
            while time.time() < deadline:
                liveness_view = query_runtime_liveness_view(housekeeping_root)
                effective_controller = dict(
                    dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(controller_node_id) or {}
                )
                if effective_controller:
                    break
                time.sleep(0.05)
            if not effective_controller:
                return _fail("current characterization expects the demand-started controller to be visible in committed liveness authority")
            if str(effective_controller.get("effective_attachment_state") or "") != "ATTACHED":
                return _fail("current characterization expects the demand-started controller to remain ATTACHED in committed liveness authority while alive")
            identity_events: list[dict[str, object]] = []
            deadline = time.time() + 3.0
            while time.time() < deadline:
                identity_events = [
                    event
                    for event in iter_committed_events(housekeeping_root)
                    if str(event.get("event_type") or "") == "process_identity_confirmed"
                    and str(event.get("node_id") or "") == controller_node_id
                ]
                if identity_events:
                    break
                time.sleep(0.05)
            if not identity_events:
                return _fail("current characterization expects housekeeping controller process identity to be committed before narrowing the remaining gap")

            deadline = time.time() + 5.0
            while time.time() < deadline and state_root.exists():
                time.sleep(0.05)
            if state_root.exists():
                return _fail("current characterization expects the demand-started housekeeping controller to retire the stopped runtime root")

            deadline = time.time() + 3.0
            while time.time() < deadline and lingering_proc.poll() is None:
                time.sleep(0.05)
            if lingering_proc.poll() is None:
                return _fail("characterization expects the demand-started controller path to terminate the lingering process")

            if not _controller_process_alive(controller_pid, repo_root=repo_root):
                return _fail("characterization expects demand-started housekeeping service to remain alive after one cleanup once service mode is enabled")
        finally:
            _cleanup_repo_services(repo_root)
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

    print("[loop-system-process-reap-gap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
