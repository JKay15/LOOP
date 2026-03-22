#!/usr/bin/env python3
"""Validate the committed child-runtime status helper."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import shutil
import sys
import tempfile
import time
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-child-runtime-status-helper][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict:
    return json.loads((ROOT / "docs" / "schemas" / name).read_text(encoding="utf-8"))


def _wait_dead(pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)


def main() -> int:
    from loop_product.runtime import bootstrap_first_implementer_node
    from loop_product.dispatch import launch_runtime

    launch_result_schema = _load_schema("LoopChildLaunchResult.schema.json")
    status_result_schema = _load_schema("LoopChildRuntimeStatusResult.schema.json")
    Draft202012Validator.check_schema(launch_result_schema)
    Draft202012Validator.check_schema(status_result_schema)

    with tempfile.TemporaryDirectory(prefix="loop_system_child_runtime_status_helper_") as td:
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
                            "The page plays local music without external links."
                        ],
                        "hard_constraints": ["Output target is local."],
                        "non_goals": ["Do not use streaming embeds."],
                        "relevant_context": ["The task is already clarified."],
                        "open_questions": [],
                        "artifact_ready_for_persistence": True
                    }
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        workspace_root = ROOT / "workspace" / "test-child-runtime-status-helper"
        state_root = ROOT / ".loop" / "test-child-runtime-status-helper"
        live_pid = 0
        zombie_parent_pid = 0
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-child-runtime-status-helper",
                root_goal="bootstrap one implementer node for runtime status helper validation",
                child_goal_slice="prepare the child runtime for later status inspection",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            live_source_result = state_root / "artifacts" / "bootstrap" / "LiveStatusLaunchResult.json"
            live_source_result.write_text(
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
                                    f"Path(r'{(workspace_root / 'status_helper.stdin.txt').resolve()}').write_bytes(sys.stdin.buffer.read()); "
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

            launch_script = ROOT / "scripts" / "launch_child_from_result.sh"
            launch_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(live_source_result.resolve()),
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
            if launch_proc.returncode != 0:
                return _fail(f"child launch wrapper must succeed for runtime status coverage: {launch_proc.stderr or launch_proc.stdout}")
            launch = json.loads(launch_proc.stdout)
            Draft202012Validator(launch_result_schema).validate(launch)
            if str(launch.get("launch_decision") or "") != "started":
                return _fail("runtime status setup launch must report started")
            live_pid = int(launch.get("pid") or 0)
            if live_pid <= 0:
                return _fail("runtime status setup launch must preserve a positive pid")

            status_script = ROOT / "scripts" / "check_child_runtime_status.sh"
            live_status_proc = subprocess.run(
                [
                    str(status_script),
                    "--result-ref",
                    str(Path(str(launch["launch_result_ref"])).resolve()),
                    "--stall-threshold-s",
                    "0",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_RUNTIME_STATUS_MODE": "direct"},
            )
            if live_status_proc.returncode != 0:
                return _fail(f"child runtime status helper must accept a live launch result: {live_status_proc.stderr or live_status_proc.stdout}")
            live_status = json.loads(live_status_proc.stdout)
            Draft202012Validator(status_result_schema).validate(live_status)
            if not bool(live_status.get("pid_alive")):
                return _fail("child runtime status helper must report pid_alive=true while the child is still live")
            if not bool(live_status.get("stalled_hint")):
                return _fail("stall-threshold=0 must mark the still-live child as a stalled hint without escalating to recovery")
            if bool(live_status.get("recovery_eligible")):
                return _fail("a still-live child must not be considered recovery-eligible")
            if str(live_status.get("recovery_reason") or "") != "live_pid_still_attached":
                return _fail("a still-live child must explain that recovery is blocked by a live pid")
            status_result_ref = Path(str(live_status.get("status_result_ref") or ""))
            if not status_result_ref.exists():
                return _fail("child runtime status helper must persist a status result artifact next to the launch result")

            original_kill = launch_runtime.os.kill

            def _permission_denied_kill(pid: int, sig: int) -> None:
                if int(pid) == live_pid and int(sig) == 0:
                    raise PermissionError("simulated sandbox signal denial for live pid probe")
                return original_kill(pid, sig)

            launch_runtime.os.kill = _permission_denied_kill
            try:
                live_status_permission_fallback = launch_runtime.child_runtime_status_from_launch_result_ref(
                    result_ref=str(Path(str(launch["launch_result_ref"])).resolve()),
                    stall_threshold_s=0,
                )
            finally:
                launch_runtime.os.kill = original_kill
            Draft202012Validator(status_result_schema).validate(live_status_permission_fallback)
            if not bool(live_status_permission_fallback.get("pid_alive")):
                return _fail("child runtime status helper must fall back to ps when signal probing is permission-denied")
            if str(live_status_permission_fallback.get("recovery_reason") or "") != "live_pid_still_attached":
                return _fail("permission-denied pid probing must still keep the child under live supervision")

            zombie_pid_path = temp_root / "zombie_pid.txt"
            zombie_driver = subprocess.Popen(
                [
                    sys.executable,
                    "-c",
                    (
                        "import os, sys, time\n"
                        "path = sys.argv[1]\n"
                        "pid = os.fork()\n"
                        "if pid == 0:\n"
                        "    os._exit(0)\n"
                        "with open(path, 'w', encoding='utf-8') as handle:\n"
                        "    handle.write(str(pid))\n"
                        "time.sleep(30)\n"
                    ),
                    str(zombie_pid_path),
                ],
                cwd=str(ROOT),
                text=True,
            )
            zombie_parent_pid = int(zombie_driver.pid)
            deadline = time.time() + 5.0
            while not zombie_pid_path.exists() and time.time() < deadline:
                time.sleep(0.05)
            if not zombie_pid_path.exists():
                return _fail("zombie runtime-status coverage must materialize a zombie pid")
            zombie_pid = int(zombie_pid_path.read_text(encoding="utf-8").strip())
            zombie_launch_result_ref = state_root / "artifacts" / "launches" / bootstrap["node_id"] / "attempt_zombie" / "ChildLaunchResult.json"
            zombie_stdout = zombie_launch_result_ref.parent / "logs" / "zombie.stdout.txt"
            zombie_stderr = zombie_launch_result_ref.parent / "logs" / "zombie.stderr.txt"
            zombie_stdout.parent.mkdir(parents=True, exist_ok=True)
            zombie_stdout.write_text("", encoding="utf-8")
            zombie_stderr.write_text("", encoding="utf-8")
            zombie_launch_result_ref.write_text(
                json.dumps(
                    {
                        "launch_decision": "started",
                        "source_result_ref": str(live_source_result.resolve()),
                        "node_id": str(bootstrap["node_id"]),
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "startup_health_timeout_ms": 250,
                        "startup_retry_limit": 0,
                        "attempt_count": 1,
                        "retryable_failure_kind": "",
                        "attempts": [],
                        "launch_request_ref": str((zombie_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                        "launch_result_ref": str(zombie_launch_result_ref.resolve()),
                        "launch_log_dir": str(zombie_stdout.parent.resolve()),
                        "stdout_ref": str(zombie_stdout.resolve()),
                        "stderr_ref": str(zombie_stderr.resolve()),
                        "stdin_ref": str((workspace_root / "CHILD_PROMPT.md").resolve()),
                        "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(30)"],
                        "wrapper_cmd": "",
                        "pid": zombie_pid,
                        "exit_code": None,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            zombie_status = launch_runtime.child_runtime_status_from_launch_result_ref(
                result_ref=str(zombie_launch_result_ref.resolve()),
                stall_threshold_s=0,
            )
            Draft202012Validator(status_result_schema).validate(zombie_status)
            if bool(zombie_status.get("pid_alive")):
                return _fail("child runtime status helper must treat zombie child pids as not alive")
            if str(zombie_status.get("recovery_reason") or "") == "live_pid_still_attached":
                return _fail("zombie child pids must not block orphaned-active recovery as if they were still live")

            original_request = launch_runtime.request_host_child_runtime_status
            original_should = launch_runtime.should_request_host_child_runtime_status
            try:
                def _fake_host_runtime_status(**_kwargs: object) -> dict[str, object]:
                    payload = dict(zombie_status)
                    payload["pid_alive"] = True
                    payload["recovery_eligible"] = False
                    payload["recovery_reason"] = "live_pid_still_attached"
                    return payload

                launch_runtime.request_host_child_runtime_status = _fake_host_runtime_status
                launch_runtime.should_request_host_child_runtime_status = lambda: True
                zombie_status_via_host = launch_runtime.child_runtime_status_from_launch_result_ref(
                    result_ref=str(zombie_launch_result_ref.resolve()),
                    stall_threshold_s=0,
                )
            finally:
                launch_runtime.request_host_child_runtime_status = original_request
                launch_runtime.should_request_host_child_runtime_status = original_should
            Draft202012Validator(status_result_schema).validate(zombie_status_via_host)
            if bool(zombie_status_via_host.get("pid_alive")):
                return _fail("host-backed runtime status must correct zombie child pids to pid_alive=false")
            if str(zombie_status_via_host.get("recovery_reason") or "") == "live_pid_still_attached":
                return _fail("host-backed runtime status must not keep zombie child pids under live supervision")

            os.kill(live_pid, signal.SIGTERM)
            _wait_dead(live_pid)

            dead_status_proc = subprocess.run(
                [
                    str(status_script),
                    "--result-ref",
                    str(Path(str(launch["launch_result_ref"])).resolve()),
                    "--stall-threshold-s",
                    "0",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_RUNTIME_STATUS_MODE": "direct"},
            )
            if dead_status_proc.returncode != 0:
                return _fail(f"child runtime status helper must accept a dead launch result: {dead_status_proc.stderr or dead_status_proc.stdout}")
            dead_status = json.loads(dead_status_proc.stdout)
            Draft202012Validator(status_result_schema).validate(dead_status)
            if bool(dead_status.get("pid_alive")):
                return _fail("child runtime status helper must report pid_alive=false after the child exits")
            if bool(dead_status.get("recovery_eligible")):
                return _fail("a freshly dead ACTIVE child must stay under supervision until runtime-loss is confirmed")
            if str(dead_status.get("recovery_reason") or "") != "active_without_live_pid_unconfirmed":
                return _fail("freshly dead ACTIVE child must explain that runtime-loss is not yet confirmed")

            launch_result_ref = Path(str(launch["launch_result_ref"])).resolve()
            stale_epoch = time.time() - 25.0
            for path in (
                launch_result_ref,
                Path(str(launch["stdout_ref"])).resolve(),
                Path(str(launch["stderr_ref"])).resolve(),
            ):
                os.utime(path, (stale_epoch, stale_epoch))

            confirmed_dead_status_proc = subprocess.run(
                [
                    str(status_script),
                    "--result-ref",
                    str(launch_result_ref),
                    "--stall-threshold-s",
                    "0",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_RUNTIME_STATUS_MODE": "direct"},
            )
            if confirmed_dead_status_proc.returncode != 0:
                return _fail(
                    f"child runtime status helper must still accept a stale dead launch result: {confirmed_dead_status_proc.stderr or confirmed_dead_status_proc.stdout}"
                )
            confirmed_dead_status = json.loads(confirmed_dead_status_proc.stdout)
            Draft202012Validator(status_result_schema).validate(confirmed_dead_status)
            if not bool(confirmed_dead_status.get("recovery_eligible")):
                return _fail("a stale dead ACTIVE child must become recovery-eligible after runtime-loss confirmation")
            if str(confirmed_dead_status.get("recovery_reason") or "") != "active_without_live_pid":
                return _fail("confirmed dead ACTIVE child must explain recovery eligibility through active_without_live_pid")
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if zombie_parent_pid > 0:
                try:
                    os.kill(zombie_parent_pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                _wait_dead(zombie_parent_pid)
            if live_pid > 0:
                try:
                    os.kill(live_pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                _wait_dead(live_pid)
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)

    print("[loop-system-child-runtime-status-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
