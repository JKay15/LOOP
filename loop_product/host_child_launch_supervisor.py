"""Host-side supervisor for queued child launch requests."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from loop_product.dispatch.launch_runtime import launch_child_from_result_ref
from loop_product.host_child_launch import force_direct_child_launch_mode, host_child_launch_request_root


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _iter_pending_requests(request_root: Path) -> list[Path]:
    return sorted(request_root.glob("*/HostChildLaunchRequest.json"))


def _response_exists(request_path: Path) -> bool:
    return (request_path.parent / "HostChildLaunchResponse.json").exists()


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process queued host child launch requests.")
    parser.add_argument("--repo-root", required=True, help="Absolute path to the loop_product_repo root")
    parser.add_argument("--once", action="store_true", help="Process pending requests once and exit")
    parser.add_argument("--poll-interval-s", type=float, default=0.5, help="Polling interval when not using --once")
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    if args.once:
        process_pending_requests(repo_root=repo_root)
        return 0

    while True:
        process_pending_requests(repo_root=repo_root)
        time.sleep(max(0.05, float(args.poll_interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
