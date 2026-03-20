"""Host-side child runtime-status request/response bridge.

This lets a nested kernel session request committed child-runtime observation
from an external host-side supervisor instead of relying on sandbox-limited
pid inspection from inside the current Codex process.
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _now_token() -> str:
    return time.strftime("%Y%m%d_%H%M%S", time.localtime())


def _repo_root_from_result_ref(result_ref: Path) -> Path:
    resolved = result_ref.expanduser().resolve()
    marker = "/.loop/"
    payload = str(resolved)
    if marker not in payload:
        raise ValueError(f"launch result ref must live under .loop/: {resolved}")
    return Path(payload.split(marker, 1)[0]).resolve()


def host_child_runtime_status_request_root(*, repo_root: Path) -> Path:
    return repo_root.expanduser().resolve() / ".loop" / "host_child_runtime_status_requests"


def should_request_host_child_runtime_status() -> bool:
    raw = str(os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE") or "").strip().lower()
    if raw in {"direct", "off", "disabled"}:
        return False
    if raw in {"host", "host_supervisor", "request"}:
        return True
    return bool(str(os.environ.get("CODEX_THREAD_ID") or "").strip())


def request_host_child_runtime_status(
    *,
    launch_result_ref: str,
    stall_threshold_s: float,
    response_timeout_s: int = 180,
    poll_interval_s: float = 0.25,
) -> dict[str, Any]:
    launch_result_path = Path(launch_result_ref).expanduser().resolve()
    repo_root = _repo_root_from_result_ref(launch_result_path)
    request_root = host_child_runtime_status_request_root(repo_root=repo_root)
    request_id = f"{launch_result_path.parent.parent.name}_{_now_token()}"
    request_dir = request_root / request_id
    response_ref = request_dir / "HostChildRuntimeStatusResponse.json"
    request_payload = {
        "schema": "loop_product.host_child_runtime_status_request",
        "request_id": request_id,
        "launch_result_ref": str(launch_result_path),
        "stall_threshold_s": float(stall_threshold_s),
        "response_ref": str(response_ref.resolve()),
    }
    _write_json(request_dir / "HostChildRuntimeStatusRequest.json", request_payload)
    from loop_product import host_child_launch_supervisor as supervisor_module

    supervisor_module.ensure_host_child_launch_supervisor_running(repo_root=repo_root)
    deadline = time.monotonic() + max(1.0, float(response_timeout_s))
    while time.monotonic() < deadline:
        if response_ref.exists():
            response_payload = json.loads(response_ref.read_text(encoding="utf-8"))
            if str(response_payload.get("status") or "") == "completed":
                return dict(response_payload.get("child_runtime_status_result") or {})
            error_message = str(response_payload.get("error") or "host child runtime status failed").strip()
            raise RuntimeError(error_message)
        time.sleep(max(0.05, float(poll_interval_s)))
    raise TimeoutError(
        f"timed out waiting for host child runtime status response; expected {response_ref.resolve()}"
    )


@contextmanager
def force_direct_child_runtime_status_mode() -> Iterator[None]:
    previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
    previous_bridge_mode = os.environ.get("LOOP_CHILD_LAUNCH_BRIDGE_MODE")
    os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
    os.environ["LOOP_CHILD_LAUNCH_BRIDGE_MODE"] = "direct"
    try:
        yield
    finally:
        if previous_runtime_status_mode is None:
            os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
        else:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
        if previous_bridge_mode is None:
            os.environ.pop("LOOP_CHILD_LAUNCH_BRIDGE_MODE", None)
        else:
            os.environ["LOOP_CHILD_LAUNCH_BRIDGE_MODE"] = previous_bridge_mode
