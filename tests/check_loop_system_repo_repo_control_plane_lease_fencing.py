#!/usr/bin/env python3
"""Validate repo control-plane self-fencing under lease supersession."""

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
    print(f"[loop-system-repo-control-plane-lease-fencing][FAIL] {msg}", file=sys.stderr)
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


def _spawn_control_plane(repo_root: Path, *, poll_interval_s: float = 0.1, idle_exit_after_s: float = 5.0) -> subprocess.Popen[str]:
    code = (
        "from loop_product.runtime.control_plane import run_repo_control_plane_loop; "
        "import sys; "
        "raise SystemExit(run_repo_control_plane_loop("
        "repo_root=sys.argv[1], poll_interval_s=float(sys.argv[2]), idle_exit_after_s=float(sys.argv[3])))"
    )
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            str(repo_root),
            str(poll_interval_s),
            str(idle_exit_after_s),
        ],
        cwd=str(ROOT),
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
    from loop_product.runtime.control_plane import (
        REPO_CONTROL_PLANE_NODE_ID,
        live_repo_control_plane_runtime,
        repo_control_plane_runtime_ref,
        repo_control_plane_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_lease_fencing_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        controller_a = None
        controller_b = None
        try:
            controller_a = _spawn_control_plane(repo_root, poll_interval_s=0.1, idle_exit_after_s=5.0)
            if not _wait_until(lambda: live_repo_control_plane_runtime(repo_root=repo_root) is not None, timeout_s=4.0):
                return _fail("first repo control-plane startup must materialize a live runtime marker")
            runtime_a = live_repo_control_plane_runtime(repo_root=repo_root) or {}
            pid_a = int(runtime_a.get("pid") or 0)
            if pid_a <= 0:
                return _fail("first repo control-plane runtime marker must expose a live pid")

            controller_b = _spawn_control_plane(repo_root, poll_interval_s=0.1, idle_exit_after_s=5.0)
            control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)

            def _effective_head() -> dict[str, object]:
                view = query_runtime_liveness_view(control_plane_root)
                return dict(dict(view.get("effective_runtime_liveness_by_node") or {}).get(REPO_CONTROL_PLANE_NODE_ID) or {})

            if not _wait_until(
                lambda: int(dict(_effective_head().get("payload") or {}).get("lease_epoch") or 0) >= 2,
                timeout_s=4.0,
            ):
                return _fail("second repo control-plane startup must supersede authority with a higher lease epoch")

            effective = _effective_head()
            payload = dict(effective.get("payload") or {})
            pid_b = int(payload.get("pid") or 0)
            if pid_b <= 0 or pid_b == pid_a:
                return _fail("superseding repo control-plane must become authoritative with a distinct pid")

            if not _wait_until(
                lambda: controller_a.poll() is not None or not _process_alive(pid_a, "loop_product.runtime.control_plane", repo_root=repo_root),
                timeout_s=4.0,
            ):
                return _fail("superseded repo control-plane must self-fence and exit after losing lease authority")
            def _surviving_runtime_identity() -> tuple[dict[str, object], dict[str, object], int, int, int]:
                current_effective = _effective_head()
                current_payload = dict(current_effective.get("payload") or {})
                current_pid = int(current_payload.get("pid") or 0)
                runtime_payload = dict(live_repo_control_plane_runtime(repo_root=repo_root) or {})
                runtime_pid = int(runtime_payload.get("pid") or 0)
                marker_pid = 0
                runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
                if runtime_ref.exists():
                    try:
                        marker_pid = int(json.loads(runtime_ref.read_text(encoding="utf-8")).get("pid") or 0)
                    except Exception:
                        marker_pid = 0
                return current_effective, current_payload, current_pid, runtime_pid, marker_pid

            if not _wait_until(
                lambda: (
                    lambda effective_payload_tuple: (
                        str(effective_payload_tuple[0].get("effective_attachment_state") or "").strip().upper() == "ATTACHED"
                        and int(effective_payload_tuple[1].get("lease_epoch") or 0) >= 2
                        and effective_payload_tuple[2] > 0
                        and effective_payload_tuple[2] != pid_a
                        and _process_alive(
                            effective_payload_tuple[2],
                            "loop_product.runtime.control_plane",
                            repo_root=repo_root,
                        )
                        and effective_payload_tuple[3] == effective_payload_tuple[2]
                        and effective_payload_tuple[4] == effective_payload_tuple[2]
                    )
                )(_surviving_runtime_identity()),
                timeout_s=4.0,
            ):
                return _fail("a newer repo control-plane must remain authoritative after fencing out the old controller")

            effective, payload, surviving_pid, runtime_pid, marker_pid = _surviving_runtime_identity()

            runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
            if not runtime_ref.exists():
                return _fail("surviving repo control-plane must eventually restore the compatibility runtime marker")
            runtime_payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
            if (
                int(runtime_payload.get("pid") or 0) != surviving_pid
                or runtime_pid != surviving_pid
                or marker_pid != surviving_pid
            ):
                return _fail("runtime marker must remain owned by the surviving repo control-plane after fencing")

            if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
                return _fail("fenced repo control-plane exit must not dethrone the newer controller's effective authority")
            if int(payload.get("pid") or 0) != surviving_pid or surviving_pid == pid_a:
                return _fail("effective repo control-plane authority must remain pinned to a newer controller after fencing")
            matrix = query_lease_fencing_matrix_view(control_plane_root)
            matrix_summary = dict(matrix.get("lease_fencing_summary") or {})
            rows = {
                str(item.get("node_id") or ""): dict(item)
                for item in list(matrix.get("matrix_rows") or [])
            }
            row = dict(rows.get(REPO_CONTROL_PLANE_NODE_ID) or {})
            matrix_cell = str(row.get("matrix_cell") or "")
            if matrix_cell not in {"fenced_owner_exit_overridden", "authoritative_attached"}:
                return _fail(
                    "lease fencing matrix must preserve repo control-plane truth as either a transient fenced-owner override "
                    "or the settled authoritative attached state"
                )
            if matrix_cell == "fenced_owner_exit_overridden":
                if int(matrix_summary.get("fenced_owner_exit_overridden_node_count") or 0) != 1:
                    return _fail("lease fencing matrix must count one fenced-owner exit override during repo control-plane fencing")
            else:
                if int(matrix_summary.get("fenced_owner_exit_overridden_node_count") or 0) != 0:
                    return _fail("settled repo control-plane truth must not keep a stale fenced-owner override count")
                if int(matrix_summary.get("authoritative_attached_node_count") or 0) < 1:
                    return _fail("settled repo control-plane truth must remain represented as authoritative_attached")
                if int(row.get("effective_lease_epoch") or 0) < 2:
                    return _fail("settled repo control-plane truth must remain pinned to the superseding lease epoch")

            if not _wait_until(
                lambda: any(
                    str(event.get("event_type") or "") == "process_exit_observed"
                    and str(event.get("node_id") or "") == REPO_CONTROL_PLANE_NODE_ID
                    and int(dict(event.get("payload") or {}).get("pid") or 0) == pid_a
                    for event in iter_committed_events(control_plane_root)
                ),
                timeout_s=4.0,
            ):
                return _fail("fenced repo control-plane must still leave committed process_exit_observed evidence")
        finally:
            for proc in (controller_a, controller_b):
                if proc is None:
                    continue
                if proc.poll() is None:
                    try:
                        os.kill(proc.pid, signal.SIGTERM)
                    except OSError:
                        pass
            _cleanup_repo_services(repo_root)

    print("[loop-system-repo-control-plane-lease-fencing] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
