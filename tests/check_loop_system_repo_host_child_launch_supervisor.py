#!/usr/bin/env python3
"""Validate the host-side child launch request/response bridge."""

from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-host-child-launch-supervisor][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module
    from loop_product import host_child_launch as host_child_launch_module
    from loop_product import host_child_launch_supervisor as supervisor_module

    workspace_root = ROOT / "workspace" / "test-host-child-launch-supervisor"
    state_root = ROOT / ".loop" / "test-host-child-launch-supervisor"
    try:
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
        shutil.rmtree(ROOT / ".loop" / "host_child_launch_requests", ignore_errors=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        (state_root / "artifacts" / "bootstrap").mkdir(parents=True, exist_ok=True)
        prompt_path = workspace_root / "CHILD_PROMPT.md"
        prompt_path.write_text("PROMPT\n", encoding="utf-8")

        result_path = state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json"
        result_path.write_text(
            json.dumps(
                {
                    "node_id": "test-host-child-launch-supervisor",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": ["codex", "exec", "-C", str(workspace_root.resolve()), "-"],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(prompt_path.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        original_request = launch_runtime_module.request_host_child_launch
        original_should = launch_runtime_module.should_request_host_child_launch
        try:
            captured: dict[str, object] = {}

            def _fake_request(**kwargs: object) -> dict[str, object]:
                captured.update(kwargs)
                return {"launch_decision": "started", "node_id": "test-host-child-launch-supervisor"}

            launch_runtime_module.request_host_child_launch = _fake_request
            launch_runtime_module.should_request_host_child_launch = lambda: True
            result = launch_runtime_module.launch_child_from_result_ref(result_ref=result_path, startup_probe_ms=77)
            if str(result.get("launch_decision") or "") != "started":
                return _fail("host child launch request path must return the supervisor-provided launch result")
            if str(captured.get("source_result_ref") or "") != str(result_path.resolve()):
                return _fail("host child launch request path must preserve the exact source result ref")
            if int(captured.get("startup_probe_ms") or -1) != 77:
                return _fail("host child launch request path must preserve startup_probe_ms")
        finally:
            launch_runtime_module.request_host_child_launch = original_request
            launch_runtime_module.should_request_host_child_launch = original_should

        request_root = host_child_launch_module.host_child_launch_request_root(repo_root=ROOT)
        request_dir = request_root / "manual_request"
        request_dir.mkdir(parents=True, exist_ok=True)
        response_ref = request_dir / "HostChildLaunchResponse.json"
        request_payload = {
            "schema": "loop_product.host_child_launch_request",
            "request_id": "manual_request",
            "node_id": "test-host-child-launch-supervisor",
            "source_result_ref": str(result_path.resolve()),
            "startup_probe_ms": 0,
            "startup_health_timeout_ms": 250,
            "response_ref": str(response_ref.resolve()),
        }
        (request_dir / "HostChildLaunchRequest.json").write_text(
            json.dumps(request_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        original_supervisor_launch = supervisor_module.launch_child_from_result_ref
        try:
            supervisor_module.launch_child_from_result_ref = lambda **kwargs: {
                "launch_decision": "started",
                "node_id": "test-host-child-launch-supervisor",
                "echo_source_result_ref": kwargs.get("result_ref"),
            }
            processed = supervisor_module.process_pending_requests(repo_root=ROOT)
            if processed != 1:
                return _fail("host child launch supervisor must process the pending request exactly once")
            if not response_ref.exists():
                return _fail("host child launch supervisor must persist a response artifact")
            response_payload = json.loads(response_ref.read_text(encoding="utf-8"))
            if str(response_payload.get("status") or "") != "completed":
                return _fail("host child launch supervisor must mark successful launches as completed")
            child_result = dict(response_payload.get("child_launch_result") or {})
            if str(child_result.get("echo_source_result_ref") or "") != str(result_path.resolve()):
                return _fail("host child launch supervisor must pass the exact source result ref into direct launch")
        finally:
            supervisor_module.launch_child_from_result_ref = original_supervisor_launch
    finally:
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
        shutil.rmtree(ROOT / ".loop" / "host_child_launch_requests", ignore_errors=True)

    print("[loop-system-host-child-launch-supervisor] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
