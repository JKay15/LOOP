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
    from loop_product.runtime import cleanup_test_runtime_root
    from loop_product.runtime_paths import node_machine_handoff_ref

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
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
        cleanup_errors: list[str] = []

        def _cleanup_runtime(*, state_root_value: Path | None, workspace_root_value: Path | None) -> None:
            if state_root_value is None or workspace_root_value is None:
                return
            try:
                receipt = cleanup_test_runtime_root(state_root=state_root_value, repo_root=ROOT)
            except Exception as exc:
                cleanup_errors.append(f"cleanup_test_runtime_root failed for {state_root_value}: {exc}")
                return
            if not bool(receipt.get("quiesced")):
                cleanup_errors.append(f"cleanup_test_runtime_root did not quiesce {state_root_value}: {receipt}")
            if list(receipt.get("remaining_owned_processes") or []):
                cleanup_errors.append(f"cleanup_test_runtime_root left owned processes for {state_root_value}: {receipt}")

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
                startup_required_output_paths=[
                    "README.md",
                    "TRACEABILITY.md",
                    "analysis/STARTUP_BOUNDARY.json",
                ],
                progress_checkpoints=[
                    {
                        "checkpoint_id": "recovery_decision",
                        "description": "persist the first machine-auditable execution decision after startup",
                        "required_any_of": [
                            "analysis/EXECUTION_DECISION.json",
                            "split/SPLIT_DECLINE_REASON.md",
                        ],
                        "window_s": 300.0,
                    }
                ],
            )
            initial_handoff_ref = Path(str(bootstrap.get("handoff_json_ref") or "")).resolve()
            initial_handoff_payload = json.loads(initial_handoff_ref.read_text(encoding="utf-8"))
            expected_live_relpath = str(initial_handoff_payload.get("workspace_live_artifact_relpath") or "")
            expected_startup_outputs = list(initial_handoff_payload.get("startup_required_output_paths") or [])
            expected_progress_checkpoints = list(initial_handoff_payload.get("progress_checkpoints") or [])

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

            try:
                os.kill(live_pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
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
                env={
                    **dict(os.environ),
                    "HTTP_PROXY": "http://127.0.0.1:7890",
                    "http_proxy": "http://127.0.0.1:7890",
                    "ALL_PROXY": "socks5://127.0.0.1:7890",
                    "all_proxy": "socks5://127.0.0.1:7890",
                    "NO_PROXY": "localhost,127.0.0.1",
                    "no_proxy": "localhost,127.0.0.1",
                },
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
            launch_env = dict(launch_spec.get("env") or {})
            if launch_env.get("HTTP_PROXY") != "http://127.0.0.1:7890" or launch_env.get("http_proxy") != "http://127.0.0.1:7890":
                return _fail("recovery helper must preserve proxy transport env in the relaunch launch_spec")
            if launch_env.get("ALL_PROXY") != "socks5://127.0.0.1:7890" or launch_env.get("all_proxy") != "socks5://127.0.0.1:7890":
                return _fail("recovery helper must preserve all_proxy transport env in the relaunch launch_spec")

            request_ref = Path(str(recovery.get("recovery_request_ref") or ""))
            result_ref = Path(str(recovery.get("recovery_result_ref") or ""))
            if not request_ref.exists() or not result_ref.exists():
                return _fail("recovery helper must persist both request and result artifacts")

            runtime_state = json.loads((state_root / "state" / "test-orphaned-active-recovery-helper.json").read_text(encoding="utf-8")).get("runtime_state") or {}
            if str(runtime_state.get("attachment_state") or "") != "UNOBSERVED":
                return _fail(
                    "accepted retry must reset the node runtime attachment to UNOBSERVED, "
                    f"got {str(runtime_state.get('attachment_state') or '')!r}"
                )

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
            refreshed_handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id="test-orphaned-active-recovery-helper")
            refreshed_handoff = json.loads(refreshed_handoff_ref.read_text(encoding="utf-8"))
            if str(refreshed_handoff.get("workspace_live_artifact_relpath") or "") != expected_live_relpath:
                return _fail("same-node orphaned-active recovery must preserve workspace_live_artifact_relpath in the refreshed frozen handoff")
            if list(refreshed_handoff.get("startup_required_output_paths") or []) != expected_startup_outputs:
                return _fail("same-node orphaned-active recovery must preserve startup_required_output_paths in the refreshed frozen handoff")
            if list(refreshed_handoff.get("progress_checkpoints") or []) != expected_progress_checkpoints:
                return _fail("same-node orphaned-active recovery must preserve progress_checkpoints in the refreshed frozen handoff")
            recovery_context_ref = Path(str(recovery.get("recovery_context_ref") or "")).resolve()
            if not recovery_context_ref.exists():
                return _fail("accepted orphaned-active recovery must persist RECOVERY_CONTEXT.md")
            refreshed_context_refs = [str(item) for item in list(refreshed_handoff.get("context_refs") or [])]
            if str(recovery_context_ref) not in refreshed_context_refs:
                return _fail("accepted orphaned-active recovery must inject RECOVERY_CONTEXT.md into the refreshed frozen handoff context refs")
            if str(Path(str(live_launch.get("launch_result_ref") or "")).resolve()) not in refreshed_context_refs:
                return _fail("accepted orphaned-active recovery must preserve recovery evidence refs in the refreshed frozen handoff context refs")

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

            workspace_root_retryable = ROOT / "workspace" / "test-orphaned-active-recovery-helper-retryable"
            state_root_retryable = ROOT / ".loop" / "test-orphaned-active-recovery-helper-retryable"
            shutil.rmtree(workspace_root_retryable, ignore_errors=True)
            shutil.rmtree(state_root_retryable, ignore_errors=True)
            retryable_bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-orphaned-active-recovery-helper-retryable",
                root_goal="bootstrap one implementer node for stale runtime status refresh validation",
                child_goal_slice="accept same-node retry after refreshing a stale terminal status for a retryable fail lane",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root_retryable),
                state_root=str(state_root_retryable),
                workspace_mirror_relpath="deliverables/out.txt",
                external_publish_target=str((temp_root / "Desktop" / "out-retryable.txt").resolve()),
                context_refs=[],
            )
            retryable_node_id = str(retryable_bootstrap["node_id"])
            retryable_result_ref = state_root_retryable / "artifacts" / retryable_node_id / "implementer_result.json"
            retryable_result_ref.parent.mkdir(parents=True, exist_ok=True)
            retryable_result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": retryable_node_id,
                        "status": "COMPLETED",
                        "outcome": "REPAIR_REQUIRED",
                        "summary": "simple evaluator reviewer verdict=FAIL",
                        "evaluator_result": {
                            "verdict": "FAIL",
                            "retryable": True,
                            "summary": "simple evaluator reviewer verdict=FAIL",
                            "diagnostics": {
                                "self_attribution": "implementation_gap",
                                "self_repair": "repair the unmet in-scope requirements and rerun evaluator",
                            },
                        },
                        "evaluation_report_ref": str((temp_root / "retryable-EvaluationReport.json").resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            (temp_root / "retryable-EvaluationReport.json").write_text("{}\n", encoding="utf-8")
            retryable_launch_result_ref = (
                state_root_retryable / "artifacts" / "launches" / retryable_node_id / "attempt_001" / "ChildLaunchResult.json"
            )
            retryable_launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
            retryable_launch_request_ref = retryable_launch_result_ref.with_name("ChildLaunchRequest.json")
            retryable_stdout_ref = retryable_launch_result_ref.with_name(f"{retryable_node_id}.stdout.txt")
            retryable_stderr_ref = retryable_launch_result_ref.with_name(f"{retryable_node_id}.stderr.txt")
            retryable_stdout_ref.write_text("stale retryable lane output\n", encoding="utf-8")
            retryable_stderr_ref.write_text("", encoding="utf-8")
            retryable_launch_request_ref.write_text("{}\n", encoding="utf-8")
            retryable_launch_result_ref.write_text(
                json.dumps(
                    {
                        "source_result_ref": str(retryable_result_ref.resolve()),
                        "launch_result_ref": str(retryable_launch_result_ref.resolve()),
                        "launch_request_ref": str(retryable_launch_request_ref.resolve()),
                        "launch_log_dir": str(retryable_launch_result_ref.parent.resolve()),
                        "launch_decision": "started",
                        "node_id": retryable_node_id,
                        "state_root": str(state_root_retryable.resolve()),
                        "workspace_root": str(workspace_root_retryable.resolve()),
                        "startup_health_timeout_ms": 12000,
                        "startup_retry_limit": 1,
                        "attempt_count": 1,
                        "retryable_failure_kind": "",
                        "attempts": [],
                        "stdin_ref": str((workspace_root_retryable / "CHILD_PROMPT.md").resolve()),
                        "wrapped_argv": ["codex", "exec", "-"],
                        "pid": 999999,
                        "exit_code": 1,
                        "stdout_ref": str(retryable_stdout_ref.resolve()),
                        "stderr_ref": str(retryable_stderr_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            stale_retryable_status_ref = retryable_launch_result_ref.with_name("stale-terminal-status.json")
            stale_retryable_status_ref.write_text(
                json.dumps(
                    {
                        "launch_result_ref": str(retryable_launch_result_ref.resolve()),
                        "status_result_ref": str(stale_retryable_status_ref.resolve()),
                        "node_id": retryable_node_id,
                        "state_root": str(state_root_retryable.resolve()),
                        "workspace_root": str(workspace_root_retryable.resolve()),
                        "pid": 999999,
                        "pid_alive": False,
                        "exit_code": 1,
                        "lifecycle_status": "ACTIVE",
                        "runtime_attachment_state": "TERMINAL",
                        "runtime_observation_kind": "terminal_result",
                        "runtime_summary": "stale terminal view from an older runtime-status helper",
                        "runtime_evidence_refs": [str(retryable_result_ref.resolve())],
                        "latest_log_ref": str(retryable_stdout_ref.resolve()),
                        "latest_log_mtime": "",
                        "latest_log_age_s": 999.0,
                        "stdout_ref": str(retryable_stdout_ref.resolve()),
                        "stderr_ref": str(retryable_stderr_ref.resolve()),
                        "stalled_hint": False,
                        "recovery_eligible": False,
                        "recovery_reason": "authoritative_terminal_result",
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            retryable_proc = subprocess.run(
                [
                    str(script),
                    "--state-root",
                    str(state_root_retryable),
                    "--node-id",
                    retryable_node_id,
                    "--workspace-root",
                    str(workspace_root_retryable),
                    "--confirmed-launch-result-ref",
                    str(retryable_launch_result_ref.resolve()),
                    "--confirmed-runtime-status-ref",
                    str(stale_retryable_status_ref.resolve()),
                    "--reason",
                    "retryable evaluator fail left the node active but the stale status ref still looked terminal",
                    "--self-attribution",
                    "implementation_gap",
                    "--self-repair",
                    "continue the same implementer task, repair the unmet in-scope requirements, and rerun evaluator",
                    "--observation-kind",
                    "retryable_terminal_result",
                    "--summary",
                    "stale runtime status must be refreshed before same-node recovery is judged",
                    "--evidence-ref",
                    str(retryable_result_ref.resolve()),
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
            )
            if retryable_proc.returncode != 0:
                return _fail(
                    "orphaned-active recovery helper must refresh stale terminal confirmed runtime status for retryable FAIL lanes "
                    f"instead of rejecting the same-node retry: {retryable_proc.stderr or retryable_proc.stdout}"
                )
            retryable_recovery = json.loads(retryable_proc.stdout)
            Draft202012Validator(result_schema).validate(retryable_recovery)
            if str(retryable_recovery.get("confirmed_runtime_recovery_reason") or "") == "authoritative_terminal_result":
                return _fail("recovery helper must not keep using a stale authoritative_terminal_result status after refresh")
            refreshed_retryable_status_ref = Path(str(retryable_recovery.get("confirmed_runtime_status_ref") or ""))
            if not refreshed_retryable_status_ref.exists():
                return _fail("recovery helper must persist the refreshed runtime status it actually used for retryable FAIL recovery")
            if refreshed_retryable_status_ref.resolve() == stale_retryable_status_ref.resolve():
                return _fail("recovery helper must replace stale terminal confirmed runtime status with a fresh trusted status")
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
            retryable_workspace = locals().get("workspace_root_retryable")
            retryable_state = locals().get("state_root_retryable")
            _cleanup_runtime(
                state_root_value=state_root if isinstance(locals().get("state_root"), Path) else None,
                workspace_root_value=workspace_root if isinstance(locals().get("workspace_root"), Path) else None,
            )
            _cleanup_runtime(
                state_root_value=retryable_state if isinstance(retryable_state, Path) else None,
                workspace_root_value=retryable_workspace if isinstance(retryable_workspace, Path) else None,
            )
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            if isinstance(retryable_workspace, Path):
                shutil.rmtree(retryable_workspace, ignore_errors=True)
            if isinstance(retryable_state, Path):
                shutil.rmtree(retryable_state, ignore_errors=True)

        if cleanup_errors:
            return _fail("; ".join(cleanup_errors))

    print("[loop-system-orphaned-active-recovery-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
