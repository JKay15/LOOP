#!/usr/bin/env python3
"""Validate housekeeping controller reuse from committed authority after marker loss."""

from __future__ import annotations

import json
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
    print(f"[loop-system-housekeeping-controller-authority-reuse][FAIL] {msg}", file=sys.stderr)
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


def _controller_pids_for_repo(repo_root: Path) -> list[int]:
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
        if "loop_product.runtime.gc" not in command:
            continue
        if "--run-housekeeping-controller" not in command:
            continue
        if marker not in command:
            continue
        result.append(pid)
    return sorted(result)


def _terminate_pid(pid: int) -> None:
    try:
        os.kill(int(pid), signal.SIGTERM)
    except OSError:
        return


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_authority_reuse_") as td:
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
        controller_pids: list[int] = []
        try:
            request = gc_module.submit_stopped_run_reap_request(
                state_root=state_root,
                repo_root=repo_root,
                controller_poll_interval_s=0.1,
                controller_idle_exit_after_s=5.0,
            )
            controller_pid = int(request.get("controller_pid") or 0)
            if controller_pid <= 0:
                return _fail("request-triggered startup must expose a controller pid")

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            controller_node_id = gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID
            if not _wait_until(
                lambda: bool(
                    dict(query_runtime_liveness_view(housekeeping_root).get("effective_runtime_liveness_by_node") or {}).get(
                        controller_node_id
                    )
                ),
                timeout_s=4.0,
            ):
                return _fail("initial controller startup must become visible in committed authority")

            marker_ref = gc_module.housekeeping_reap_controller_runtime_ref(repo_root=repo_root)
            if not marker_ref.exists():
                return _fail("controller startup must materialize a compatibility runtime marker before reuse is tested")
            marker_ref.unlink()
            if marker_ref.exists():
                return _fail("runtime marker removal must succeed before reuse is tested")

            controller_pids = _controller_pids_for_repo(repo_root)
            if controller_pid not in controller_pids:
                return _fail("original controller must still be alive after marker loss")
            if len(controller_pids) != 1:
                return _fail("marker-loss setup must start from exactly one live housekeeping controller")

            reused = gc_module.ensure_housekeeping_reap_controller_running(
                repo_root=repo_root,
                poll_interval_s=0.1,
                idle_exit_after_s=5.0,
                watch_repo_control_plane=True,
            )
            reused_pid = int(reused.get("pid") or 0)
            if reused_pid != controller_pid:
                return _fail("marker loss must reuse the existing authority-backed controller instead of spawning a new pid")

            controller_pids = _controller_pids_for_repo(repo_root)
            if controller_pids != [controller_pid]:
                return _fail("marker loss must not leave multiple housekeeping controller processes alive")

            if not _wait_until(lambda: marker_ref.exists(), timeout_s=2.0):
                return _fail("authority-backed reuse must self-heal the local runtime marker")
            marker_payload = json.loads(marker_ref.read_text(encoding="utf-8"))
            if int(marker_payload.get("pid") or 0) != controller_pid:
                return _fail("self-healed runtime marker must point at the reused controller pid")
        finally:
            _cleanup_repo_services(repo_root)
            for pid in _controller_pids_for_repo(repo_root):
                _terminate_pid(pid)
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

    print("[loop-system-housekeeping-controller-authority-reuse] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
