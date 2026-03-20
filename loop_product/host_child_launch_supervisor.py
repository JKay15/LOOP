"""Host-side supervisor for queued child launch/runtime-status requests."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from loop_product.dispatch.launch_runtime import child_runtime_status_from_launch_result_ref
from loop_product.dispatch.launch_runtime import launch_child_from_result_ref
from loop_product.host_child_launch import force_direct_child_launch_mode, host_child_launch_request_root
from loop_product.host_child_runtime_status import (
    force_direct_child_runtime_status_mode,
    host_child_runtime_status_request_root,
)

_DEFAULT_POLL_INTERVAL_S = 0.5
_DEFAULT_IDLE_EXIT_AFTER_S = 30.0
_STARTUP_TIMEOUT_S = 5.0


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _pid_alive(pid: int) -> bool:
    try:
        candidate = int(pid)
    except (TypeError, ValueError):
        return False
    if candidate <= 0:
        return False
    try:
        os.kill(candidate, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def supervisor_runtime_ref(*, repo_root: Path) -> Path:
    return repo_root.expanduser().resolve() / ".loop" / "host_child_launch_supervisor" / "HostChildLaunchSupervisorRuntime.json"


def _load_runtime_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def live_supervisor_runtime(*, repo_root: Path) -> dict[str, Any] | None:
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    if not runtime_ref.exists():
        return None
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        return None
    pid = int(payload.get("pid") or 0)
    if not _pid_alive(pid):
        runtime_ref.unlink(missing_ok=True)
        return None
    return payload


def _log_ref_for_repo(*, repo_root: Path) -> Path:
    return supervisor_runtime_ref(repo_root=repo_root).with_suffix(".log")


def ensure_host_child_launch_supervisor_running(
    *,
    repo_root: Path,
    poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_IDLE_EXIT_AFTER_S,
    startup_timeout_s: float = _STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    package_repo_root = Path(__file__).resolve().parents[1]
    existing = live_supervisor_runtime(repo_root=repo_root)
    if existing is not None:
        return existing

    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    log_ref = _log_ref_for_repo(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    with log_ref.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.host_child_launch_supervisor",
                "--repo-root",
                str(repo_root),
                "--poll-interval-s",
                str(max(0.05, float(poll_interval_s))),
                "--idle-exit-after-s",
                str(max(0.0, float(idle_exit_after_s))),
            ],
            cwd=str(package_repo_root),
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )

    deadline = time.monotonic() + max(1.0, float(startup_timeout_s))
    while time.monotonic() < deadline:
        payload = live_supervisor_runtime(repo_root=repo_root)
        if payload is not None:
            return payload
        exit_code = proc.poll()
        if exit_code is not None:
            detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
            raise RuntimeError(detail or f"host child launch supervisor exited early with code {exit_code}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for host child launch supervisor runtime marker at {runtime_ref}")


def _iter_pending_requests(request_root: Path) -> list[Path]:
    return sorted(request_root.glob("*/HostChildLaunchRequest.json"))


def _response_exists(request_path: Path) -> bool:
    return (request_path.parent / "HostChildLaunchResponse.json").exists()


def _iter_pending_runtime_status_requests(request_root: Path) -> list[Path]:
    return sorted(request_root.glob("*/HostChildRuntimeStatusRequest.json"))


def _runtime_status_response_exists(request_path: Path) -> bool:
    return (request_path.parent / "HostChildRuntimeStatusResponse.json").exists()


def process_pending_requests(*, repo_root: Path) -> int:
    request_root = host_child_launch_request_root(repo_root=repo_root)
    processed = 0
    for request_path in _iter_pending_requests(request_root):
        if _response_exists(request_path):
            continue
        request = json.loads(request_path.read_text(encoding="utf-8"))
        response_ref = Path(str(request.get("response_ref") or request_path.parent / "HostChildLaunchResponse.json")).expanduser().resolve()
        try:
            with force_direct_child_launch_mode():
                child_result = launch_child_from_result_ref(
                    result_ref=str(request["source_result_ref"]),
                    startup_probe_ms=int(request.get("startup_probe_ms", 1500)),
                    startup_health_timeout_ms=int(request.get("startup_health_timeout_ms", 12000)),
                )
            payload = {
                "schema": "loop_product.host_child_launch_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "completed",
                "child_launch_result": child_result,
            }
        except Exception as exc:
            payload = {
                "schema": "loop_product.host_child_launch_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "failed",
                "error": str(exc),
            }
        _write_json(response_ref, payload)
        processed += 1
    return processed


def process_pending_runtime_status_requests(*, repo_root: Path) -> int:
    request_root = host_child_runtime_status_request_root(repo_root=repo_root)
    processed = 0
    for request_path in _iter_pending_runtime_status_requests(request_root):
        if _runtime_status_response_exists(request_path):
            continue
        request = json.loads(request_path.read_text(encoding="utf-8"))
        response_ref = Path(str(request.get("response_ref") or request_path.parent / "HostChildRuntimeStatusResponse.json")).expanduser().resolve()
        try:
            with force_direct_child_runtime_status_mode():
                status_result = child_runtime_status_from_launch_result_ref(
                    result_ref=str(request["launch_result_ref"]),
                    stall_threshold_s=float(request.get("stall_threshold_s", 60.0)),
                )
            payload = {
                "schema": "loop_product.host_child_runtime_status_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "completed",
                "child_runtime_status_result": status_result,
            }
        except Exception as exc:
            payload = {
                "schema": "loop_product.host_child_runtime_status_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "failed",
                "error": str(exc),
            }
        _write_json(response_ref, payload)
        processed += 1
    return processed


@contextmanager
def _runtime_marker(
    *,
    repo_root: Path,
    poll_interval_s: float,
    idle_exit_after_s: float,
) -> Iterator[Path]:
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    _write_json(
        runtime_ref,
        {
            "schema": "loop_product.host_child_launch_supervisor_runtime",
            "repo_root": str(repo_root),
            "pid": os.getpid(),
            "poll_interval_s": float(poll_interval_s),
            "idle_exit_after_s": float(idle_exit_after_s),
            "started_at_utc": _now_iso(),
            "log_ref": str(_log_ref_for_repo(repo_root=repo_root)),
        },
    )
    try:
        yield runtime_ref
    finally:
        runtime_ref.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process queued host child launch requests.")
    parser.add_argument("--repo-root", required=True, help="Absolute path to the loop_product_repo root")
    parser.add_argument("--once", action="store_true", help="Process pending requests once and exit")
    parser.add_argument("--poll-interval-s", type=float, default=_DEFAULT_POLL_INTERVAL_S, help="Polling interval when not using --once")
    parser.add_argument(
        "--idle-exit-after-s",
        type=float,
        default=_DEFAULT_IDLE_EXIT_AFTER_S,
        help="Exit after this much idle time with no pending requests; use 0 to disable idle exit",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    poll_interval_s = max(0.05, float(args.poll_interval_s))
    idle_exit_after_s = max(0.0, float(args.idle_exit_after_s))

    with _runtime_marker(repo_root=repo_root, poll_interval_s=poll_interval_s, idle_exit_after_s=idle_exit_after_s):
        if args.once:
            process_pending_requests(repo_root=repo_root)
            process_pending_runtime_status_requests(repo_root=repo_root)
            return 0

        last_activity = time.monotonic()
        while True:
            processed = process_pending_requests(repo_root=repo_root)
            processed += process_pending_runtime_status_requests(repo_root=repo_root)
            if processed > 0:
                last_activity = time.monotonic()
            elif idle_exit_after_s > 0.0 and (time.monotonic() - last_activity) >= idle_exit_after_s:
                return 0
            time.sleep(poll_interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
