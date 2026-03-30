#!/usr/bin/env python3
"""Validate same-node child launch serialization under concurrent callers."""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-launch-concurrency-guard][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module

    with tempfile.TemporaryDirectory(prefix="loop_system_launch_concurrency_guard_") as td:
        temp_root = Path(td)
        workspace_root = temp_root / "workspace" / "launch-concurrency-guard"
        state_root = temp_root / ".loop" / "launch-concurrency-guard"
        bootstrap_root = state_root / "artifacts" / "bootstrap"
        workspace_root.mkdir(parents=True, exist_ok=True)
        bootstrap_root.mkdir(parents=True, exist_ok=True)

        stdin_ref = workspace_root / "CHILD_PROMPT.md"
        stdin_ref.write_text("PROMPT\n", encoding="utf-8")

        source_result_ref = bootstrap_root / "ConcurrentLaunchSourceResult.json"
        source_result_ref.write_text(
            json.dumps(
                {
                    "node_id": "same-node-launch-concurrency-guard",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": [sys.executable, "-c", "import time; time.sleep(1)"],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(stdin_ref.resolve()),
                    },
                    "startup_retry_limit": 0,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        original_launch_once = launch_runtime_module._launch_once
        original_identity_matches = launch_runtime_module._launch_result_process_identity_matches
        original_should_request_host_child_launch = launch_runtime_module.should_request_host_child_launch

        launch_count = 0
        launch_count_lock = threading.Lock()
        second_entered = threading.Event()

        def _fake_launch_once(*, request, state_root, workspace_root, wrapper_prefix, wrapper_cmd):  # type: ignore[no-untyped-def]
            del wrapper_prefix, wrapper_cmd
            nonlocal launch_count
            with launch_count_lock:
                launch_count += 1
                ordinal = launch_count
            if ordinal == 1:
                second_entered.wait(timeout=0.4)
            else:
                second_entered.set()
            attempt_dir = state_root / "artifacts" / "launches" / str(request["node_id"]) / f"attempt_{ordinal:03d}"
            log_dir = attempt_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            request_ref = attempt_dir / "ChildLaunchRequest.json"
            request_ref.write_text(json.dumps(request, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stdout_ref = log_dir / "fake.stdout.txt"
            stderr_ref = log_dir / "fake.stderr.txt"
            stdout_ref.write_text("launch started\n", encoding="utf-8")
            stderr_ref.write_text("", encoding="utf-8")
            time.sleep(0.05)
            return {
                "launch_decision": "started",
                "source_result_ref": request["source_result_ref"],
                "node_id": request["node_id"],
                "workspace_root": str(workspace_root),
                "state_root": str(state_root),
                "startup_health_timeout_ms": int(request["startup_health_timeout_ms"]),
                "launch_request_ref": str(request_ref.resolve()),
                "launch_log_dir": str(log_dir.resolve()),
                "stdout_ref": str(stdout_ref.resolve()),
                "stderr_ref": str(stderr_ref.resolve()),
                "stdin_ref": str(Path(str(request["launch_spec"]["stdin_path"])).expanduser().resolve()),
                "wrapped_argv": [str(item) for item in list(request["launch_spec"]["argv"])],
                "wrapper_cmd": "",
                "pid": 424242,
                "exit_code": None,
                "retryable_failure_kind": "",
            }

        results: list[dict[str, object] | None] = [None, None]
        errors: list[BaseException | None] = [None, None]

        def _worker(index: int) -> None:
            try:
                results[index] = launch_runtime_module.launch_child_from_result_ref(
                    result_ref=source_result_ref,
                    startup_probe_ms=0,
                    startup_health_timeout_ms=50,
                )
            except BaseException as exc:  # pragma: no cover - surfaced in assertions
                errors[index] = exc

        try:
            launch_runtime_module._launch_once = _fake_launch_once
            launch_runtime_module._launch_result_process_identity_matches = (
                lambda payload: int(dict(payload or {}).get("pid") or 0) == 424242
            )
            launch_runtime_module.should_request_host_child_launch = lambda: False

            threads = [threading.Thread(target=_worker, args=(index,)) for index in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5.0)

            if any(thread.is_alive() for thread in threads):
                return _fail("concurrent launch callers must not deadlock")
            if any(error is not None for error in errors):
                return _fail(f"concurrent launch callers must both complete successfully: {errors!r}")
            if launch_count != 1:
                return _fail(f"same-node concurrent launch must execute exactly one direct launch, got {launch_count}")

            payloads = [dict(item or {}) for item in results]
            decisions = sorted(str(payload.get("launch_decision") or "") for payload in payloads)
            if decisions != ["started", "started_existing"]:
                return _fail(f"concurrent launch callers must resolve to started + started_existing, got {decisions!r}")
            pids = {int(payload.get("pid") or 0) for payload in payloads}
            if pids != {424242}:
                return _fail(f"concurrent launch callers must converge on one reused pid, got {sorted(pids)!r}")
            started_existing = [payload for payload in payloads if str(payload.get("launch_decision") or "") == "started_existing"]
            if len(started_existing) != 1:
                return _fail("exactly one concurrent caller must persist started_existing reuse truth")
            reuse_ref = str(started_existing[0].get("reuse_launch_result_ref") or "")
            if not reuse_ref.endswith("/attempt_001/ChildLaunchResult.json"):
                return _fail(f"started_existing launch must point at the winning attempt_001 result, got {reuse_ref!r}")
        finally:
            launch_runtime_module._launch_once = original_launch_once
            launch_runtime_module._launch_result_process_identity_matches = original_identity_matches
            launch_runtime_module.should_request_host_child_launch = original_should_request_host_child_launch

    print("[loop-system-launch-concurrency-guard][OK] concurrent same-node launch callers serialize to one direct launch")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
