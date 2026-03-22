"""Detached committed supervision sidecar for launched child nodes."""

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

from loop_product.runtime.lifecycle import supervise_child_until_settled

_DEFAULT_POLL_INTERVAL_S = 2.0
_DEFAULT_STALL_THRESHOLD_S = 60.0
_DEFAULT_MAX_RECOVERIES = 5
_DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S = 300.0
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


def supervision_runtime_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionRuntime.json")


def supervision_result_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionResult.json")


def supervision_log_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervision.log")


def _load_runtime_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def live_child_supervision_runtime(*, launch_result_ref: str | Path) -> dict[str, Any] | None:
    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_result_ref)
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


@contextmanager
def _runtime_marker(
    *,
    launch_result_ref: Path,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
) -> Iterator[Path]:
    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_result_ref)
    _write_json(
        runtime_ref,
        {
            "schema": "loop_product.child_supervision_runtime",
            "launch_result_ref": str(launch_result_ref),
            "pid": os.getpid(),
            "poll_interval_s": float(poll_interval_s),
            "stall_threshold_s": float(stall_threshold_s),
            "max_recoveries": int(max_recoveries),
            "no_substantive_progress_window_s": float(no_substantive_progress_window_s),
            "started_at_utc": _now_iso(),
            "log_ref": str(supervision_log_ref(launch_result_ref=launch_result_ref)),
            "result_ref": str(supervision_result_ref(launch_result_ref=launch_result_ref)),
        },
    )
    try:
        yield runtime_ref
    finally:
        runtime_ref.unlink(missing_ok=True)


def ensure_child_supervision_running(
    *,
    launch_result_ref: str | Path,
    poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
    stall_threshold_s: float = _DEFAULT_STALL_THRESHOLD_S,
    max_recoveries: int = _DEFAULT_MAX_RECOVERIES,
    no_substantive_progress_window_s: float = _DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S,
    startup_timeout_s: float = _STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    existing = live_child_supervision_runtime(launch_result_ref=launch_path)
    if existing is not None:
        return existing

    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_path)
    log_ref = supervision_log_ref(launch_result_ref=launch_path)
    result_ref = supervision_result_ref(launch_result_ref=launch_path)
    package_repo_root = Path(__file__).resolve().parents[1]

    with log_ref.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.child_supervision_sidecar",
                "--launch-result-ref",
                str(launch_path),
                "--poll-interval-s",
                str(max(0.05, float(poll_interval_s))),
                "--stall-threshold-s",
                str(max(0.0, float(stall_threshold_s))),
                "--max-recoveries",
                str(max(0, int(max_recoveries))),
                "--no-substantive-progress-window-s",
                str(max(0.0, float(no_substantive_progress_window_s))),
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
        payload = live_child_supervision_runtime(launch_result_ref=launch_path)
        if payload is not None:
            return payload
        exit_code = proc.poll()
        if exit_code is not None:
            if exit_code == 0 and result_ref.exists():
                return {
                    "schema": "loop_product.child_supervision_runtime",
                    "launch_result_ref": str(launch_path),
                    "pid": 0,
                    "status": "settled_immediately",
                    "log_ref": str(log_ref),
                    "result_ref": str(result_ref),
                    "runtime_ref": str(runtime_ref),
                }
            detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
            raise RuntimeError(detail or f"child supervision sidecar exited early with code {exit_code}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for child supervision runtime marker at {runtime_ref}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detached committed child supervision sidecar")
    parser.add_argument("--launch-result-ref", required=True, help="Path to a ChildLaunchResult.json")
    parser.add_argument("--poll-interval-s", type=float, default=_DEFAULT_POLL_INTERVAL_S)
    parser.add_argument("--stall-threshold-s", type=float, default=_DEFAULT_STALL_THRESHOLD_S)
    parser.add_argument("--max-recoveries", type=int, default=_DEFAULT_MAX_RECOVERIES)
    parser.add_argument("--no-substantive-progress-window-s", type=float, default=_DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    launch_path = Path(args.launch_result_ref).expanduser().resolve()
    with _runtime_marker(
        launch_result_ref=launch_path,
        poll_interval_s=max(0.05, float(args.poll_interval_s)),
        stall_threshold_s=max(0.0, float(args.stall_threshold_s)),
        max_recoveries=max(0, int(args.max_recoveries)),
        no_substantive_progress_window_s=max(0.0, float(args.no_substantive_progress_window_s)),
    ):
        result = supervise_child_until_settled(
            launch_result_ref=launch_path,
            poll_interval_s=max(0.05, float(args.poll_interval_s)),
            stall_threshold_s=max(0.0, float(args.stall_threshold_s)),
            max_recoveries=max(0, int(args.max_recoveries)),
            no_substantive_progress_window_s=max(0.0, float(args.no_substantive_progress_window_s)),
        )
        _write_json(supervision_result_ref(launch_result_ref=launch_path), result)
        print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
