"""Host-side child launch request/response bridge.

This lets a nested kernel session request a committed child launch without
directly spawning the child `codex exec` from inside the current Codex process.
An external host-side supervisor can consume these requests and execute the
existing child launch helper in a non-nested context.
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


def _repo_root_from_state_root(state_root: Path) -> Path:
    return state_root.expanduser().resolve().parents[1]


def host_child_launch_request_root(*, repo_root: Path) -> Path:
    return repo_root.expanduser().resolve() / ".loop" / "host_child_launch_requests"


def should_request_host_child_launch() -> bool:
    raw = str(os.environ.get("LOOP_CHILD_LAUNCH_MODE") or "").strip().lower()
    if raw in {"direct", "off", "disabled"}:
        return False
    if raw in {"host", "host_supervisor", "request"}:
        return True
    return bool(str(os.environ.get("CODEX_THREAD_ID") or "").strip())


def request_host_child_launch(
    *,
    source_result_ref: str,
    node_id: str,
    state_root: Path,
    startup_probe_ms: int,
    startup_health_timeout_ms: int,
    response_timeout_s: int = 180,
    poll_interval_s: float = 0.25,
) -> dict[str, Any]:
    repo_root = _repo_root_from_state_root(state_root)
    request_root = host_child_launch_request_root(repo_root=repo_root)
    request_id = f"{node_id}_{_now_token()}"
    request_dir = request_root / request_id
    response_ref = request_dir / "HostChildLaunchResponse.json"
    request_payload = {
        "schema": "loop_product.host_child_launch_request",
        "request_id": request_id,
        "node_id": str(node_id),
        "source_result_ref": str(Path(source_result_ref).expanduser().resolve()),
        "startup_probe_ms": int(startup_probe_ms),
        "startup_health_timeout_ms": int(startup_health_timeout_ms),
        "response_ref": str(response_ref.resolve()),
    }
    _write_json(request_dir / "HostChildLaunchRequest.json", request_payload)
    deadline = time.monotonic() + max(1.0, float(response_timeout_s))
    while time.monotonic() < deadline:
        if response_ref.exists():
            response_payload = json.loads(response_ref.read_text(encoding="utf-8"))
            if str(response_payload.get("status") or "") == "completed":
                return dict(response_payload.get("child_launch_result") or {})
            error_message = str(response_payload.get("error") or "host child launch failed").strip()
            raise RuntimeError(error_message)
        time.sleep(max(0.05, float(poll_interval_s)))
    raise TimeoutError(
        f"timed out waiting for host child launch response for {node_id}; expected {response_ref.resolve()}"
    )


@contextmanager
def force_direct_child_launch_mode() -> Iterator[None]:
    previous = os.environ.get("LOOP_CHILD_LAUNCH_MODE")
    os.environ["LOOP_CHILD_LAUNCH_MODE"] = "direct"
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("LOOP_CHILD_LAUNCH_MODE", None)
        else:
            os.environ["LOOP_CHILD_LAUNCH_MODE"] = previous
