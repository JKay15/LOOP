#!/usr/bin/env python3
"""Validate repo-lifecycle housekeeping controller bootstrap before first demand."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-housekeeping-controller-repo-lifecycle-bootstrap][FAIL] {msg}", file=sys.stderr)
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


def _controller_effective_payload(*, housekeeping_root: Path) -> dict[str, object]:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    effective = dict(
        dict(query_runtime_liveness_view(housekeeping_root).get("effective_runtime_liveness_by_node") or {}).get(
            gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID
        )
        or {}
    )
    return dict(effective.get("payload") or {})


def main() -> int:
    from loop_product.kernel import query_operational_hygiene_view
    from loop_product.runtime import cleanup_test_repo_services, gc as gc_module
    from loop_product.runtime.lifecycle import initialize_evaluator_runtime

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_repo_lifecycle_bootstrap_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        controller_pid = 0
        try:
            runtime_a = (repo_root / ".loop" / "runtime-a").resolve()
            runtime_b = (repo_root / ".loop" / "runtime-b").resolve()

            initialize_evaluator_runtime(
                state_root=runtime_a,
                task_id="repo-lifecycle-a",
                root_goal="bootstrap repo lifecycle A",
                child_goal_slice="bootstrap repo lifecycle A child",
            )

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            if not _wait_until(
                lambda: bool(_controller_effective_payload(housekeeping_root=housekeeping_root)),
                timeout_s=4.0,
            ):
                return _fail("repo lifecycle bootstrap must bring up housekeeping authority before first demand")

            payload_a = _controller_effective_payload(housekeeping_root=housekeeping_root)
            controller_pid = int(payload_a.get("pid") or 0)
            if controller_pid <= 0:
                return _fail("repo lifecycle bootstrap must expose a live housekeeping controller pid")
            if not _controller_process_alive(controller_pid, repo_root=repo_root):
                return _fail("repo lifecycle bootstrap must start a live housekeeping controller process")
            try:
                idle_exit_after_s = float(payload_a.get("idle_exit_after_s"))
            except (TypeError, ValueError):
                idle_exit_after_s = -1.0
            if idle_exit_after_s != 0.0:
                return _fail("repo lifecycle bootstrap must start housekeeping in service mode")

            hygiene = query_operational_hygiene_view(
                housekeeping_root,
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("canonical_reap_requested_event_count") or 0) != 0:
                return _fail("repo lifecycle bootstrap must occur before any reap request is committed")

            initialize_evaluator_runtime(
                state_root=runtime_b,
                task_id="repo-lifecycle-b",
                root_goal="bootstrap repo lifecycle B",
                child_goal_slice="bootstrap repo lifecycle B child",
            )

            payload_b = _controller_effective_payload(housekeeping_root=housekeeping_root)
            controller_pid_b = int(payload_b.get("pid") or 0)
            if controller_pid_b != controller_pid:
                return _fail("repeated repo lifecycle bootstrap must reuse the same housekeeping controller")
        finally:
            cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(cleanup_receipt.get("quiesced")):
                raise RuntimeError(f"repo-service cleanup did not quiesce: {cleanup_receipt!r}")

    print("[loop-system-housekeeping-controller-repo-lifecycle-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
