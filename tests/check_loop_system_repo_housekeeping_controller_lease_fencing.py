#!/usr/bin/env python3
"""Validate housekeeping controller self-fencing under lease supersession."""

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
    print(f"[loop-system-housekeeping-controller-lease-fencing][FAIL] {msg}", file=sys.stderr)
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


def _spawn_controller(repo_root: Path, *, poll_interval_s: float = 0.1, idle_exit_after_s: float = 5.0) -> subprocess.Popen[str]:
    package_repo_root = ROOT
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "loop_product.runtime.gc",
            "--repo-root",
            str(repo_root),
            "--run-housekeeping-controller",
            "--poll-interval-s",
            str(poll_interval_s),
            "--idle-exit-after-s",
            str(idle_exit_after_s),
        ],
        cwd=str(package_repo_root),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        text=True,
    )


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.kernel import query_lease_fencing_matrix_view, query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_lease_fencing_") as td:
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
        controller_a = None
        controller_b = None
        try:
            controller_a = _spawn_controller(repo_root)
            if not _wait_until(
                lambda: gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root) is not None,
                timeout_s=4.0,
            ):
                return _fail("first controller startup must materialize a live runtime marker")
            runtime_a = gc_module.live_housekeeping_reap_controller_runtime(repo_root=repo_root) or {}
            pid_a = int(runtime_a.get("pid") or 0)
            if pid_a <= 0:
                return _fail("first controller runtime marker must expose a live pid")

            controller_b = _spawn_controller(repo_root)
            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            controller_node_id = gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID

            def _effective_head() -> dict[str, object]:
                view = query_runtime_liveness_view(housekeeping_root)
                return dict(dict(view.get("effective_runtime_liveness_by_node") or {}).get(controller_node_id) or {})

            if not _wait_until(
                lambda: int(dict(_effective_head().get("payload") or {}).get("lease_epoch") or 0) >= 2,
                timeout_s=4.0,
            ):
                return _fail("second controller startup must supersede authority with a higher lease epoch")

            effective = _effective_head()
            payload = dict(effective.get("payload") or {})
            pid_b = int(payload.get("pid") or 0)
            if pid_b <= 0 or pid_b == pid_a:
                return _fail("superseding controller must become authoritative with a distinct pid")

            if not _wait_until(
                lambda: controller_a.poll() is not None or not _controller_process_alive(pid_a, repo_root=repo_root),
                timeout_s=3.0,
            ):
                return _fail("superseded controller must self-fence and exit after losing lease authority")
            if not _controller_process_alive(pid_b, repo_root=repo_root):
                return _fail("newer controller must remain alive after fencing out the old controller")

            runtime_ref = gc_module.housekeeping_reap_controller_runtime_ref(repo_root=repo_root)
            if not runtime_ref.exists():
                return _fail("surviving controller must keep the compatibility runtime marker")
            runtime_payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
            if int(runtime_payload.get("pid") or 0) != pid_b:
                return _fail("runtime marker must remain owned by the surviving controller after fencing")

            effective = _effective_head()
            effective_payload = dict(effective.get("payload") or {})
            if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
                return _fail("fenced controller exit must not dethrone the newer controller's effective authority")
            if int(effective_payload.get("pid") or 0) != pid_b:
                return _fail("effective authority must remain pinned to the newer controller after fencing")
            matrix = query_lease_fencing_matrix_view(housekeeping_root)
            matrix_summary = dict(matrix.get("lease_fencing_summary") or {})
            rows = {str(item.get("node_id") or ""): dict(item) for item in list(matrix.get("matrix_rows") or [])}
            row = dict(rows.get(controller_node_id) or {})
            matrix_cell = str(row.get("matrix_cell") or "")
            if matrix_cell not in {"fenced_owner_exit_overridden", "authoritative_attached"}:
                return _fail(
                    "lease fencing matrix must preserve housekeeping fencing truth as either a transient fenced-owner override "
                    "or the settled authoritative attached state"
                )
            if matrix_cell == "fenced_owner_exit_overridden":
                if int(matrix_summary.get("fenced_owner_exit_overridden_node_count") or 0) != 1:
                    return _fail("lease fencing matrix must count one fenced-owner exit override during housekeeping fencing")
            else:
                if int(matrix_summary.get("fenced_owner_exit_overridden_node_count") or 0) != 0:
                    return _fail("settled housekeeping truth must not keep a stale fenced-owner override count")
                if int(matrix_summary.get("authoritative_attached_node_count") or 0) < 1:
                    return _fail("settled housekeeping truth must remain represented as authoritative_attached")
                if int(row.get("effective_lease_epoch") or 0) < 2:
                    return _fail("settled housekeeping truth must remain pinned to the superseding lease epoch")

            exit_events = [
                event
                for event in iter_committed_events(housekeeping_root)
                if str(event.get("event_type") or "") == "process_exit_observed"
                and str(event.get("node_id") or "") == controller_node_id
                and int(dict(event.get("payload") or {}).get("pid") or 0) == pid_a
            ]
            if not exit_events:
                return _fail("fenced controller must still leave committed process_exit_observed evidence")
        finally:
            for proc in (controller_a, controller_b):
                if proc is None:
                    continue
                if proc.poll() is None:
                    try:
                        os.kill(proc.pid, signal.SIGTERM)
                    except OSError:
                        pass
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

    print("[loop-system-housekeeping-controller-lease-fencing] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
