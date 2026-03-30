#!/usr/bin/env python3
"""Validate safe GC for stopped runtime runs."""

from __future__ import annotations

import json
import os
import shutil
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
    print(f"[loop-system-stopped-run-gc][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _persist_node(state_root: Path, node_id: str, *, status: str, workspace_root: Path | None = None) -> None:
    _write_json(
        state_root / "state" / f"{node_id}.json",
        {
            "node_id": node_id,
            "status": status,
            "workspace_root": str(workspace_root.resolve()) if workspace_root is not None else "",
            "runtime_state": {},
        },
    )


def _persist_launch_result(state_root: Path, node_id: str, *, pid: int) -> Path:
    launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
    _write_json(
        launch_ref,
        {
            "node_id": node_id,
            "pid": int(pid),
            "launch_result_ref": str(launch_ref.resolve()),
            "state_root": str(state_root.resolve()),
        },
    )
    return launch_ref


def _wait_dead_process(proc: subprocess.Popen[bytes], timeout_s: float = 5.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if proc.poll() is not None:
            return True
        time.sleep(0.05)
    return False


def main() -> int:
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_stopped_run_gc_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_scope_root = repo_root / ".loop"
        workspace_scope_root = repo_root / "workspace"
        state_scope_root.mkdir(parents=True, exist_ok=True)
        workspace_scope_root.mkdir(parents=True, exist_ok=True)

        stopped_state_root = state_scope_root / "stopped-run"
        stopped_workspace_root = workspace_scope_root / "stopped-run"
        stopped_workspace_root.mkdir(parents=True, exist_ok=True)
        (stopped_state_root / "state").mkdir(parents=True, exist_ok=True)
        _persist_node(stopped_state_root, "stopped-run", status="BLOCKED", workspace_root=stopped_workspace_root)
        _persist_node(stopped_state_root, "child-terminal-001", status="COMPLETED", workspace_root=stopped_workspace_root)
        _persist_launch_result(stopped_state_root, "child-terminal-001", pid=424242)

        lingering_proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time,sys; time.sleep(30)",
                str(stopped_state_root.resolve()),
                str(stopped_workspace_root.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        try:
            report = gc_module.stopped_run_gc_report(state_root=stopped_state_root, repo_root=repo_root)
            if not bool(report.get("reapable")):
                return _fail("terminal run with no live recorded pid must be classified as reapable")
            owned_pids = {int(item.get("pid") or 0) for item in list(report.get("owned_processes") or [])}
            if lingering_proc.pid not in owned_pids:
                return _fail("stopped-run GC must discover lingering owned descendant processes by path ownership")

            receipt = gc_module.reap_stopped_run(state_root=stopped_state_root, repo_root=repo_root)
            if str(receipt.get("status") or "") != "reaped":
                return _fail("reap_stopped_run must report a reaped receipt for a reapable stopped run")
            receipt_ref = Path(str(receipt.get("receipt_ref") or "")).expanduser()
            if not receipt_ref.exists():
                return _fail("reap_stopped_run must persist a GC receipt under .loop/quarantine/reaped_runs/")
            receipt_payload = json.loads(receipt_ref.read_text(encoding="utf-8"))
            if not receipt_payload.get("canonical_process_orphan_detected_event_ids"):
                return _fail("reap receipt must preserve canonical process_orphan_detected event refs")
            if not receipt_payload.get("canonical_runtime_root_quiesced_event_id"):
                return _fail("reap receipt must preserve canonical runtime_root_quiesced event ref")
            if stopped_state_root.exists():
                return _fail("reap_stopped_run must delete the stopped run state root")
            if stopped_workspace_root.exists():
                return _fail("reap_stopped_run must delete the stopped run workspace root")
            if not _wait_dead_process(lingering_proc, timeout_s=3.0):
                return _fail("reap_stopped_run must terminate lingering owned descendant processes before reclaiming the run")
        finally:
            if lingering_proc.poll() is None:
                try:
                    os.kill(lingering_proc.pid, signal.SIGTERM)
                except OSError:
                    pass
                try:
                    lingering_proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    lingering_proc.kill()
                    lingering_proc.wait(timeout=1.0)

        live_state_root = state_scope_root / "live-run"
        live_workspace_root = workspace_scope_root / "live-run"
        live_workspace_root.mkdir(parents=True, exist_ok=True)
        (live_state_root / "state").mkdir(parents=True, exist_ok=True)
        _persist_node(live_state_root, "live-run", status="ACTIVE", workspace_root=live_workspace_root)
        live_proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        try:
            _persist_launch_result(live_state_root, "live-run", pid=live_proc.pid)
            live_report = gc_module.stopped_run_gc_report(state_root=live_state_root, repo_root=repo_root)
            if bool(live_report.get("reapable")):
                return _fail("run with an ACTIVE node and live recorded pid must not be classified as reapable")
            blockers = list(live_report.get("blocking_reasons") or [])
            if not blockers:
                return _fail("non-reapable live run must explain why GC is blocked")
            list_payload = gc_module.list_stopped_run_gc_candidates(repo_root=repo_root)
            candidate_names = {str(item.get("runtime_name") or "") for item in list_payload}
            if "stopped-run" in candidate_names:
                return _fail("already reaped runs must no longer appear in stopped-run GC candidate listings")
            if "live-run" in candidate_names:
                return _fail("live runs must not appear in stopped-run GC candidate listings")
        finally:
            if live_proc.poll() is None:
                live_proc.terminate()
                try:
                    live_proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    live_proc.kill()
                    live_proc.wait(timeout=1.0)

    print("[loop-system-stopped-run-gc] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
