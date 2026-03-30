#!/usr/bin/env python3
"""Validate that missing runtime context only retires supervision when cleanup authority exists."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-sidecar-cleanup-authority-guard][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    import loop_product.dispatch.supervision as supervision_module
    from loop_product.event_journal import commit_test_runtime_cleanup_committed_event

    with tempfile.TemporaryDirectory(prefix="loop_system_sidecar_cleanup_authority_guard_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "test-sidecar-cleanup-authority-guard"
        workspace_root = repo_root / "workspace" / "test-sidecar-cleanup-authority-guard"
        launch_result_ref = state_root / "artifacts" / "launches" / "test-sidecar-cleanup-authority-guard" / "attempt_001" / "ChildLaunchResult.json"
        _write_json(
            launch_result_ref,
            {
                "launch_result_ref": str(launch_result_ref.resolve()),
                "node_id": "test-sidecar-cleanup-authority-guard",
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
            },
        )
        launch_result_ref.unlink()

        fake_clock = {"now": 0.0}

        def _now() -> float:
            return float(fake_clock["now"])

        def _sleep(seconds: float) -> None:
            fake_clock["now"] += float(seconds)

        def _reader(**kwargs):
            del kwargs
            raise FileNotFoundError("runtime context missing")

        timeout_result = supervision_module.supervise_child_until_settled(
            launch_result_ref=launch_result_ref,
            poll_interval_s=0.05,
            stall_threshold_s=0.0,
            max_recoveries=0,
            max_wall_clock_s=0.15,
            runtime_status_reader=_reader,
            recovery_runner=lambda **kwargs: {},
            launcher=lambda **kwargs: {},
            now_fn=_now,
            sleep_fn=_sleep,
            apply_reasoning_budget_floor=False,
        )
        if bool(timeout_result.get("settled")):
            return _fail("missing runtime context without cleanup authority must not be treated as a settled retirement")
        if str(timeout_result.get("settled_reason") or "") != "timeout":
            return _fail("missing runtime context without cleanup authority must still behave like retry/timeout supervision")

        housekeeping_root = repo_root / ".loop" / "housekeeping"
        commit_test_runtime_cleanup_committed_event(
            housekeeping_root,
            observation_id="test-sidecar-cleanup-authority-guard:cleanup",
            node_id="test-sidecar-cleanup-authority-guard",
            payload={
                "runtime_name": state_root.name,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "cleanup_kind": "test_runtime_teardown",
            },
            producer="tests.sidecar_cleanup_authority_guard",
        )

        try:
            supervision_module.supervise_child_until_settled(
                launch_result_ref=launch_result_ref,
                poll_interval_s=0.05,
                stall_threshold_s=0.0,
                max_recoveries=0,
                max_wall_clock_s=0.15,
                runtime_status_reader=_reader,
                recovery_runner=lambda **kwargs: {},
                launcher=lambda **kwargs: {},
                now_fn=_now,
                sleep_fn=_sleep,
                apply_reasoning_budget_floor=False,
            )
        except supervision_module.RuntimeContextMissingError:
            pass
        else:
            return _fail("cleanup authority must convert missing runtime context into an explicit supervision retirement signal")

        live_reader_calls = {"count": 0}

        def _reader_live(**kwargs):
            del kwargs
            live_reader_calls["count"] += 1
            return {
                "launch_result_ref": str(launch_result_ref.resolve()),
                "node_id": "test-sidecar-cleanup-authority-guard",
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "status_result_ref": str((launch_result_ref.parent / "ChildRuntimeStatusResult.json").resolve()),
                "lifecycle_status": "ACTIVE",
                "runtime_attachment_state": "ATTACHED",
                "pid_alive": True,
                "recovery_eligible": False,
            }

        try:
            supervision_module.supervise_child_until_settled(
                launch_result_ref=launch_result_ref,
                poll_interval_s=0.05,
                stall_threshold_s=0.0,
                max_recoveries=0,
                max_wall_clock_s=0.15,
                runtime_status_reader=_reader_live,
                recovery_runner=lambda **kwargs: {},
                launcher=lambda **kwargs: {},
                now_fn=_now,
                sleep_fn=_sleep,
                apply_reasoning_budget_floor=False,
            )
        except supervision_module.RuntimeContextMissingError:
            pass
        else:
            return _fail("cleanup authority must retire supervision even before runtime status becomes unreadable")
        if live_reader_calls["count"] != 0:
            return _fail("cleanup authority retirement should preempt further runtime-status reads once retirement is committed")

        shutil.rmtree(repo_root, ignore_errors=True)

    print("[loop-system-sidecar-cleanup-authority-guard] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
