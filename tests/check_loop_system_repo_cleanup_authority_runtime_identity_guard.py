#!/usr/bin/env python3
"""Validate cleanup authority is fenced by runtime-root identity across path reincarnation."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-cleanup-authority-runtime-identity-guard][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.kernel.state import ensure_runtime_tree
    from loop_product.runtime import cleanup_test_repo_services, cleanup_test_runtime_root

    with tempfile.TemporaryDirectory(prefix="loop_system_cleanup_authority_runtime_identity_guard_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "reincarnation-run"
        workspace_root = repo_root / "workspace" / "reincarnation-run"
        endpoint_root = repo_root / "endpoint"
        launch_script = ROOT / "scripts" / "launch_child_from_result.sh"

        launched_pid = 0
        try:
            ensure_runtime_tree(state_root)
            workspace_root.mkdir(parents=True, exist_ok=True)
            first_cleanup = cleanup_test_runtime_root(state_root=state_root, repo_root=repo_root)
            if not bool(first_cleanup.get("quiesced")):
                return _fail("first incarnation cleanup must quiesce before reincarnation")

            ensure_runtime_tree(state_root)
            workspace_root.mkdir(parents=True, exist_ok=True)
            prompt_ref = workspace_root / "CHILD_PROMPT.md"
            prompt_ref.write_text("hello\n", encoding="utf-8")
            source_result_ref = state_root / "artifacts" / "bootstrap" / "SourceResult.json"
            source_result_ref.parent.mkdir(parents=True, exist_ok=True)
            source_result_ref.write_text(
                json.dumps(
                    {
                        "node_id": "reincarnation-node",
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "launch_spec": {
                            "argv": [sys.executable, "-c", "import time; time.sleep(30)"],
                            "env": {},
                            "cwd": str(workspace_root.resolve()),
                            "stdin_path": str(prompt_ref.resolve()),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            first_launch_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(source_result_ref.resolve()),
                    "--startup-probe-ms",
                    "50",
                    "--startup-health-timeout-ms",
                    "250",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_LAUNCH_MODE": "direct"},
            )
            if first_launch_proc.returncode != 0:
                return _fail(f"new incarnation first launch must succeed after old cleanup authority exists: {first_launch_proc.stderr or first_launch_proc.stdout}")
            first_launch = json.loads(first_launch_proc.stdout)
            if str(first_launch.get("launch_decision") or "") != "started":
                return _fail("new incarnation first launch must start a fresh child")
            launched_pid = int(first_launch.get("pid") or 0)
            if launched_pid <= 0:
                return _fail("new incarnation first launch must expose a positive pid")

            second_launch_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(source_result_ref.resolve()),
                    "--startup-probe-ms",
                    "50",
                    "--startup-health-timeout-ms",
                    "250",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_LAUNCH_MODE": "direct"},
            )
            if second_launch_proc.returncode != 0:
                return _fail(f"new incarnation legitimate same-process reuse must survive old cleanup authority: {second_launch_proc.stderr or second_launch_proc.stdout}")
            second_launch = json.loads(second_launch_proc.stdout)
            if str(second_launch.get("launch_decision") or "") != "started_existing":
                return _fail("new incarnation second launch must still reuse the same live child")
            if int(second_launch.get("pid") or 0) != launched_pid:
                return _fail("new incarnation second launch must keep reusing the same child pid")
        finally:
            cleanup_errors: list[str] = []
            if state_root.exists() or workspace_root.exists():
                runtime_cleanup = cleanup_test_runtime_root(state_root=state_root, repo_root=repo_root)
                if not bool(runtime_cleanup.get("quiesced")):
                    cleanup_errors.append(f"runtime cleanup did not quiesce: {runtime_cleanup!r}")
            repo_cleanup = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(repo_cleanup.get("quiesced")):
                cleanup_errors.append(f"repo-service cleanup did not quiesce: {repo_cleanup!r}")
            if cleanup_errors:
                raise RuntimeError("; ".join(cleanup_errors))

    print("[loop-system-cleanup-authority-runtime-identity-guard] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
