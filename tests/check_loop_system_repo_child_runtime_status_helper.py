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
        try:
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
            )
            if dead_status_proc.returncode != 0:
                return _fail(f"child runtime status helper must accept a dead launch result: {dead_status_proc.stderr or dead_status_proc.stdout}")
            dead_status = json.loads(dead_status_proc.stdout)
            Draft202012Validator(status_result_schema).validate(dead_status)
            if bool(dead_status.get("pid_alive")):
                return _fail("child runtime status helper must report pid_alive=false after the child exits")
            if not bool(dead_status.get("recovery_eligible")):
                return _fail("an ACTIVE node with no live pid must become recovery-eligible")
            if str(dead_status.get("recovery_reason") or "") != "active_without_live_pid":
                return _fail("dead ACTIVE child must explain recovery eligibility through active_without_live_pid")
        finally:
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
