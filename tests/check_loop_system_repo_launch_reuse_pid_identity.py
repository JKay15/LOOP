#!/usr/bin/env python3
"""Validate launch-time started_existing reuse against stale unrelated live PIDs."""

from __future__ import annotations

import json
import os
import shutil
import stat
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-launch-reuse-pid-identity][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict:
    return json.loads((ROOT / "docs" / "schemas" / name).read_text(encoding="utf-8"))


def _wait_dead(pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)


def _safe_rmtree(path: Path) -> None:
    def _onerror(func, target, exc_info):
        target_path = Path(target)
        try:
            target_path.chmod(target_path.stat().st_mode | stat.S_IWUSR)
        except OSError:
            pass
        try:
            func(target)
        except OSError:
            pass

    shutil.rmtree(path, onerror=_onerror)


def main() -> int:
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        cleanup_test_repo_services,
        cleanup_test_runtime_root,
    )

    launch_result_schema = _load_schema("LoopChildLaunchResult.schema.json")
    status_result_schema = _load_schema("LoopChildRuntimeStatusResult.schema.json")
    Draft202012Validator.check_schema(launch_result_schema)
    Draft202012Validator.check_schema(status_result_schema)

    with tempfile.TemporaryDirectory(prefix="loop_system_launch_reuse_pid_identity_") as td:
        temp_root = Path(td)
        endpoint = temp_root / "EndpointArtifact.json"
        endpoint.write_text(
            json.dumps(
                {
                    "version": "1",
                    "session_root": str((temp_root / "endpoint_session").resolve()),
                    "artifact_ref": str(endpoint.resolve()),
                    "latest_turn_ref": str((temp_root / "turns" / "0001" / "TurnResult.json").resolve()),
                    "mode": "VISION_COMPILER",
                    "status": "CLARIFIED",
                    "original_user_prompt": "Create one local birthday poster HTML with offline music playback.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "design",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Deliver one local birthday poster with offline music playback.",
                        "final_effect": "Deliver one local birthday poster with offline music playback.",
                        "observable_success_criteria": [
                            "A local HTML birthday poster exists.",
                            "The page plays local music without external links.",
                        ],
                        "hard_constraints": ["Output target is local."],
                        "non_goals": ["Do not use streaming embeds."],
                        "relevant_context": ["The task is already clarified."],
                        "open_questions": [],
                        "artifact_ready_for_persistence": True,
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        workspace_root = ROOT / "workspace" / "test-launch-reuse-pid-identity"
        state_root = ROOT / ".loop" / "test-launch-reuse-pid-identity"
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
        launched_pid = 0
        try:
            initial_cleanup = cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(initial_cleanup.get("quiesced")):
                return _fail("repo service cleanup must quiesce before launch reuse pid-identity validation")
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-launch-reuse-pid-identity",
                root_goal="bootstrap one implementer node for stale launch reuse validation",
                child_goal_slice="refuse started_existing reuse when a stale launch result points at an unrelated live pid",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            source_result_ref = state_root / "artifacts" / "bootstrap" / "StaleReuseSourceResult.json"
            source_result_ref.write_text(
                json.dumps(
                    {
                        "node_id": str(bootstrap["node_id"]),
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "launch_spec": {
                            "argv": [
                                sys.executable,
                                "-c",
                                (
                                    "from pathlib import Path; import sys, time; "
                                    f"Path(r'{(workspace_root / 'reuse_identity.stdin.txt').resolve()}').write_bytes(sys.stdin.buffer.read()); "
                                    "time.sleep(30)"
                                ),
                            ],
                            "env": {},
                            "cwd": str(workspace_root.resolve()),
                            "stdin_path": str((workspace_root / "CHILD_PROMPT.md").resolve()),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            launch_dir = state_root / "artifacts" / "launches" / str(bootstrap["node_id"]) / "attempt_001"
            log_dir = launch_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            stdout_ref = log_dir / "stale.stdout.txt"
            stderr_ref = log_dir / "stale.stderr.txt"
            stdin_ref = workspace_root / "CHILD_PROMPT.md"
            stdout_ref.write_text("stale old stdout\n", encoding="utf-8")
            stderr_ref.write_text("", encoding="utf-8")
            stale_request_ref = launch_dir / "ChildLaunchRequest.json"
            stale_request_ref.write_text(
                json.dumps(
                    {
                        "node_id": str(bootstrap["node_id"]),
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "source_result_ref": str(source_result_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            stale_launch_result = {
                "launch_decision": "started",
                "source_result_ref": str(source_result_ref.resolve()),
                "node_id": str(bootstrap["node_id"]),
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "startup_health_timeout_ms": 250,
                "startup_retry_limit": 0,
                "attempt_count": 1,
                "retryable_failure_kind": "",
                "attempts": [],
                "launch_request_ref": str(stale_request_ref.resolve()),
                "launch_result_ref": str((launch_dir / "ChildLaunchResult.json").resolve()),
                "launch_log_dir": str(log_dir.resolve()),
                "stdout_ref": str(stdout_ref.resolve()),
                "stderr_ref": str(stderr_ref.resolve()),
                "stdin_ref": str(stdin_ref.resolve()),
                "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(30)"],
                "wrapper_cmd": "",
                "pid": int(os.getpid()),
                "exit_code": None,
            }
            Draft202012Validator(launch_result_schema).validate(stale_launch_result)
            (launch_dir / "ChildLaunchResult.json").write_text(
                json.dumps(stale_launch_result, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )

            status_script = ROOT / "scripts" / "check_child_runtime_status.sh"
            stale_status_proc = subprocess.run(
                [
                    str(status_script),
                    "--result-ref",
                    str((launch_dir / "ChildLaunchResult.json").resolve()),
                    "--stall-threshold-s",
                    "0",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_RUNTIME_STATUS_MODE": "direct"},
            )
            if stale_status_proc.returncode != 0:
                return _fail(f"runtime status helper must still accept a stale launch result: {stale_status_proc.stderr or stale_status_proc.stdout}")
            stale_status = json.loads(stale_status_proc.stdout)
            Draft202012Validator(status_result_schema).validate(stale_status)
            if bool(stale_status.get("pid_alive")):
                return _fail("runtime status helper must not report pid_alive=true when the stored pid belongs to an unrelated live process")
            if str(stale_status.get("runtime_attachment_state") or "").upper() == "ATTACHED":
                return _fail("runtime status helper must not report ATTACHED when process identity does not match the launch result")

            launch_script = ROOT / "scripts" / "launch_child_from_result.sh"
            proc = subprocess.run(
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
            if proc.returncode != 0:
                return _fail(f"launch helper must succeed under stale reuse coverage: {proc.stderr or proc.stdout}")
            launch = json.loads(proc.stdout)
            Draft202012Validator(launch_result_schema).validate(launch)
            if str(launch.get("launch_decision") or "") != "started":
                return _fail("launch helper must refuse started_existing reuse when the only live pid belongs to an unrelated process")
            launched_pid = int(launch.get("pid") or 0)
            if launched_pid <= 0:
                return _fail("fresh replacement launch must preserve a positive pid")
            if launched_pid == os.getpid():
                return _fail("fresh replacement launch must not keep pointing at the unrelated stale pid")

            reused_proc = subprocess.run(
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
            if reused_proc.returncode != 0:
                return _fail(f"launch helper must still allow legitimate same-process reuse: {reused_proc.stderr or reused_proc.stdout}")
            reused_launch = json.loads(reused_proc.stdout)
            Draft202012Validator(launch_result_schema).validate(reused_launch)
            if str(reused_launch.get("launch_decision") or "") != "started_existing":
                return _fail("launch helper must still reuse the real live replacement child after rejecting the stale unrelated pid")
            if int(reused_launch.get("pid") or 0) != launched_pid:
                return _fail("legitimate same-process reuse must preserve the fresh replacement pid")
        finally:
            if state_root.exists():
                cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
            cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
            if launched_pid > 0:
                try:
                    os.kill(launched_pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                _wait_dead(launched_pid)
            if workspace_root.exists():
                _safe_rmtree(workspace_root)
            if state_root.exists():
                _safe_rmtree(state_root)

    print("[loop-system-launch-reuse-pid-identity][OK] stale unrelated live pid does not trigger started_existing reuse")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
