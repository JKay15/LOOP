#!/usr/bin/env python3
"""Validate request-triggered housekeeping controller autobootstrap."""

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
    print(f"[loop-system-housekeeping-reap-request-autobootstrap][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _persist_node(state_root: Path, node_id: str, *, status: str, workspace_root: Path) -> None:
    _write_json(
        state_root / "state" / f"{node_id}.json",
        {
            "node_id": node_id,
            "status": status,
            "workspace_root": str(workspace_root.resolve()),
            "runtime_state": {},
        },
    )


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_reap_request_autobootstrap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "stopped-run"
        workspace_root = repo_root / "workspace" / "stopped-run"
        (state_root / "state").mkdir(parents=True, exist_ok=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(state_root, "stopped-run", status="BLOCKED", workspace_root=workspace_root)

        lingering_proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(state_root.resolve()),
                str(workspace_root.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        controller_pid = 0
        try:
            if gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root) is not None:
                return _fail("fresh autobootstrap root must not start with a live housekeeping controller")

            request = gc_module.submit_stopped_run_reap_request(
                state_root=state_root,
                repo_root=repo_root,
                controller_poll_interval_s=0.1,
                controller_idle_exit_after_s=0.75,
            )
            if not request.get("canonical_reap_requested_event_id"):
                return _fail("request submission must still persist canonical reap intent before autobootstrap")
            if not state_root.exists():
                return _fail("request submission must not collapse back into synchronous inline cleanup")

            if not _wait_until(
                lambda: gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root) is not None,
                timeout_s=4.0,
            ):
                return _fail("request submission must autobootstrap the housekeeping controller")
            controller = gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root)
            controller_pid = int(dict(controller or {}).get("pid") or 0)
            if controller_pid <= 0:
                return _fail("autobootstrapped controller must publish a live runtime marker with pid")

            if not _wait_until(lambda: not state_root.exists(), timeout_s=5.0):
                return _fail("autobootstrapped controller must converge the committed reap request")
            if not _wait_until(lambda: lingering_proc.poll() is not None, timeout_s=3.0):
                return _fail("autobootstrapped controller must terminate lingering owned processes")

            if not _wait_until(
                lambda: gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root) is None,
                timeout_s=4.0,
            ):
                return _fail("autobootstrapped controller must self-exit after becoming idle")
        finally:
            _cleanup_repo_services(repo_root)
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

    print("[loop-system-housekeeping-reap-request-autobootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
