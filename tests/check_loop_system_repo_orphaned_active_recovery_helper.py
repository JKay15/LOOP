#!/usr/bin/env python3
"""Validate the committed orphaned-ACTIVE recovery helper."""

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
    print(f"[loop-system-orphaned-active-recovery-helper][FAIL] {msg}", file=sys.stderr)
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


def main() -> int:
    from loop_product.runtime import bootstrap_first_implementer_node

    request_schema = _load_schema("LoopOrphanedActiveRecoveryRequest.schema.json")
    result_schema = _load_schema("LoopOrphanedActiveRecoveryResult.schema.json")
    launch_result_schema = _load_schema("LoopChildLaunchResult.schema.json")
    Draft202012Validator.check_schema(request_schema)
    Draft202012Validator.check_schema(result_schema)
    Draft202012Validator.check_schema(launch_result_schema)

    with tempfile.TemporaryDirectory(prefix="loop_system_orphaned_active_helper_") as td:
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

        workspace_root = ROOT / "workspace" / "test-orphaned-active-recovery-helper"
        state_root = ROOT / ".loop" / "test-orphaned-active-recovery-helper"
        try:
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-orphaned-active-recovery-helper",
                root_goal="bootstrap one implementer node for orphaned-active recovery helper validation",
                child_goal_slice="prepare the child runtime for later recovery",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            script = ROOT / "scripts" / "recover_orphaned_active.sh"
            bad_proc = subprocess.run(
                [
                    str(script),
                    "--state-root",
                    str(state_root),
                    "--node-id",
                    "test-orphaned-active-recovery-helper",
                    "--workspace-root",
                    str((workspace_root.parent / "wrong-workspace").resolve()),
                    "--reason",
                    "mismatch guard",
                    "--self-attribution",
                    "child_runtime_detached_network_disconnect",
                    "--self-repair",
                    "restart",
                    "--observation-kind",
                    "child_runtime_detached",
                    "--summary",
                    "mismatch guard",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
            )
            if bad_proc.returncode == 0:
                return _fail("recovery helper must reject a mismatched exact workspace_root guard")

            live_source_result = state_root / "artifacts" / "bootstrap" / "LiveExistingLaunchResult.json"
            live_source_result.write_text(
                json.dumps(
                    {
                        "node_id": "test-orphaned-active-recovery-helper",
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "launch_spec": {
                            "argv": [
                                sys.executable,
                                "-c",
                                (
                                    "from pathlib import Path; import sys, time; "
                                    f"Path(r'{(workspace_root / 'recovery_live.stdin.txt').resolve()}').write_bytes(sys.stdin.buffer.read()); "
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
            live_launch_proc = subprocess.run(
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
            if live_launch_proc.returncode != 0:
                return _fail(f"pre-existing live child launch must succeed for recovery reuse coverage: {live_launch_proc.stderr or live_launch_proc.stdout}")
            live_launch = json.loads(live_launch_proc.stdout)
            Draft202012Validator(launch_result_schema).validate(live_launch)
            if str(live_launch.get("launch_decision") or "") != "started":
                return _fail("pre-existing recovery live launch must report started")
            live_pid = int(live_launch.get("pid") or 0)
            if live_pid <= 0:
                return _fail("pre-existing recovery live launch must preserve a positive pid")

            premature_recovery_proc = subprocess.run(
                [
                    str(script),
                    "--state-root",
                    str(state_root),
                    "--node-id",
                    "test-orphaned-active-recovery-helper",
                    "--workspace-root",
                    str(workspace_root),
                    "--reason",
                    "the backing child transport disconnected before any evaluator-backed result",
                    "--self-attribution",
                    "child_runtime_detached_network_disconnect",
                    "--self-repair",
                    "restart the same implementer node from the latest durable checkpoint",
                    "--observation-kind",
                    "child_runtime_detached",
                    "--summary",
                    "authoritative state still showed ACTIVE but the backing child session disappeared",
                    "--evidence-ref",
                    str(Path(str(live_launch.get("launch_result_ref") or "")).resolve()),
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
            )
            if premature_recovery_proc.returncode == 0:
                return _fail("recovery helper must reject same-node retry while the latest child pid is still live")

            os.kill(live_pid, signal.SIGTERM)
            _wait_dead(live_pid)

            stale_epoch = time.time() - 25.0
            for path in (
                Path(str(live_launch.get("launch_result_ref") or "")).resolve(),
                Path(str(live_launch.get("stdout_ref") or "")).resolve(),
                Path(str(live_launch.get("stderr_ref") or "")).resolve(),
            ):
                os.utime(path, (stale_epoch, stale_epoch))

            live_status_proc = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "loop_product.runtime.lifecycle",
                    "child-runtime-status",
                    "--result-ref",
                    str(Path(str(live_launch.get("launch_result_ref") or "")).resolve()),
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_RUNTIME_STATUS_MODE": "direct"},
            )
            if live_status_proc.returncode != 0:
                return _fail(f"fresh runtime status query must succeed before direct structured recovery: {live_status_proc.stderr or live_status_proc.stdout}")
            live_status = json.loads(live_status_proc.stdout)
            live_status_ref = Path(str(live_status.get("status_result_ref") or "")).resolve()
            if not live_status_ref.exists():
                return _fail("fresh runtime status query must persist a status_result_ref before recovery")

            bundle_root = state_root / "artifacts" / "bootstrap" / "test-orphaned-active-recovery-helper"
            poisoned_manual = bundle_root / "EvaluatorProductManual.md"
            poisoned_effects = bundle_root / "EvaluatorFinalEffects.md"
            if not poisoned_manual.exists() or not poisoned_effects.exists():
                return _fail("recovery helper coverage requires the bootstrap evaluator bundle to exist before poisoning")
            poisoned_manual.write_text("# Poisoned Manual\n\nstale bundle should be rewritten during recovery\n", encoding="utf-8")
            poisoned_effects.write_text(
                "# Poisoned Final Effects\n\nwhole-paper faithful complete formalization\npaper defect exposed\n",
                encoding="utf-8",
            )

            proc = subprocess.run(
                [
                    str(script),
                    "--state-root",
                    str(state_root),
                    "--node-id",
                    "test-orphaned-active-recovery-helper",
                    "--workspace-root",
                    str(workspace_root),
                    "--confirmed-launch-result-ref",
                    str(Path(str(live_launch.get("launch_result_ref") or "")).resolve()),
                    "--confirmed-runtime-status-ref",
                    str(live_status_ref),
                    "--reason",
                    "the backing child transport disconnected before any evaluator-backed result",
                    "--self-attribution",
                    "child_runtime_detached_network_disconnect",
                    "--self-repair",
                    "restart the same implementer node from the latest durable checkpoint",
                    "--observation-kind",
                    "child_runtime_detached",
                    "--summary",
                    "authoritative state still showed ACTIVE but the backing child session disappeared",
                    "--evidence-ref",
                    str(Path(str(live_launch.get("launch_result_ref") or "")).resolve()),
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
            )
            if proc.returncode != 0:
                return _fail(f"recovery helper wrapper must accept direct structured flags after confirmed runtime loss: {proc.stderr or proc.stdout}")

            recovery = json.loads(proc.stdout)
            Draft202012Validator(result_schema).validate(recovery)

            if recovery.get("recovery_decision") != "accepted":
                return _fail(f"recovery helper must accept an orphaned-active retry, got {recovery.get('recovery_decision')!r}")
            if recovery.get("node_id") != bootstrap["node_id"]:
                return _fail("recovery helper must target the same node id")
            if recovery.get("workspace_root") != str(workspace_root.resolve()):
                return _fail("recovery helper must preserve the exact workspace_root")

            launch_spec = recovery.get("launch_spec") or {}
            argv = list(launch_spec.get("argv") or [])
            if "-C" not in argv or str(workspace_root.resolve()) not in argv:
                return _fail("recovery helper must relaunch through codex exec pinned to the same workspace root")
            if launch_spec.get("stdin_path") != str((workspace_root / "CHILD_PROMPT.md").resolve()):
                return _fail("recovery helper must relaunch the same child prompt")

            request_ref = Path(str(recovery.get("recovery_request_ref") or ""))
            result_ref = Path(str(recovery.get("recovery_result_ref") or ""))
            if not request_ref.exists() or not result_ref.exists():
                return _fail("recovery helper must persist both request and result artifacts")

            runtime_state = json.loads((state_root / "state" / "test-orphaned-active-recovery-helper.json").read_text(encoding="utf-8")).get("runtime_state") or {}
            if str(runtime_state.get("attachment_state") or "") != "UNOBSERVED":
                return _fail("accepted retry must reset the node runtime attachment to UNOBSERVED")

            if Path(str(recovery.get("confirmed_launch_result_ref") or "")).resolve() != Path(str(live_launch.get("launch_result_ref") or "")).resolve():
                return _fail("recovery helper must record which committed launch result was freshly rechecked")
            confirmed_status_ref = Path(str(recovery.get("confirmed_runtime_status_ref") or ""))
            if not confirmed_status_ref.exists():
                return _fail("accepted orphaned-active recovery must persist the fresh runtime status it relied on")
            if str(recovery.get("confirmed_runtime_recovery_reason") or "") != "active_without_live_pid":
                return _fail("accepted orphaned-active recovery must explain the confirmed runtime-loss reason")

            refreshed_manual_text = poisoned_manual.read_text(encoding="utf-8")
            refreshed_effects_text = poisoned_effects.read_text(encoding="utf-8")
            if "Poisoned Manual" in refreshed_manual_text:
                return _fail("same-node recovery must refresh the bootstrap evaluator manual instead of reusing a stale poisoned copy")
            if "Poisoned Final Effects" in refreshed_effects_text:
                return _fail("same-node recovery must refresh the bootstrap evaluator final effects instead of reusing a stale poisoned copy")

            reused_launch_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(result_ref.resolve()),
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
            if reused_launch_proc.returncode != 0:
                return _fail(f"launching from recovery result must still return structured reuse output: {reused_launch_proc.stderr or reused_launch_proc.stdout}")
            reused_launch = json.loads(reused_launch_proc.stdout)
            Draft202012Validator(launch_result_schema).validate(reused_launch)
            if str(reused_launch.get("launch_decision") or "") != "started":
                return _fail("launching from an accepted recovery result must start a replacement child once the original pid is gone")

            reused_pid = int(reused_launch.get("pid") or 0)
            if reused_pid <= 0:
                return _fail("launching from an accepted recovery result must preserve a positive replacement pid")

            reused_launch_again_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(result_ref.resolve()),
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
            if reused_launch_again_proc.returncode != 0:
                return _fail(
                    f"launching again from the same recovery result must return structured reuse output: {reused_launch_again_proc.stderr or reused_launch_again_proc.stdout}"
                )
            reused_launch_again = json.loads(reused_launch_again_proc.stdout)
            Draft202012Validator(launch_result_schema).validate(reused_launch_again)
            if str(reused_launch_again.get("launch_decision") or "") != "started_existing":
                return _fail("recovery launch must reuse the replacement child after it becomes the live same-node process")
            if int(reused_launch_again.get("pid") or 0) != reused_pid:
                return _fail("recovery reuse launch must point at the exact live replacement child pid")
        finally:
            for pid_name in ("live_pid", "reused_pid"):
                pid_value = locals().get(pid_name, 0)
                if not isinstance(pid_value, int) or pid_value <= 0:
                    continue
                try:
                    os.kill(pid_value, signal.SIGTERM)
                except ProcessLookupError:
                    continue
                deadline = time.time() + 5.0
                while time.time() < deadline:
                    try:
                        os.kill(pid_value, 0)
                    except ProcessLookupError:
                        break
                    time.sleep(0.05)
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)

    print("[loop-system-orphaned-active-recovery-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
