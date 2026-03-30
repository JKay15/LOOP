#!/usr/bin/env python3
"""Validate the committed child-runtime status helper."""

from __future__ import annotations

import atexit
import json
import os
import signal
import subprocess
import shutil
import stat
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


def _overwrite_json_projection(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        os.chmod(path, 0o644)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(path, 0o444)


def _cleanup_runtime_tree(*, state_root: Path, workspace_root: Path) -> None:
    from loop_product.runtime import cleanup_test_runtime_root

    def _safe_rmtree(path: Path) -> None:
        if not path.exists():
            return

        def _onerror(func, target, exc_info):  # type: ignore[no-untyped-def]
            del exc_info
            try:
                os.chmod(target, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
            except OSError:
                return
            try:
                func(target)
            except OSError:
                return

        shutil.rmtree(path, ignore_errors=False, onerror=_onerror)

    try:
        if state_root.exists():
            cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
    except Exception:
        pass
    _safe_rmtree(workspace_root)
    _safe_rmtree(state_root)


def _cleanup_repo_services_for_root() -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        child_runtime_status_from_launch_result_ref as trusted_child_runtime_status_from_launch_result_ref,
    )
    from loop_product.dispatch import launch_runtime
    from loop_product.runtime.lifecycle import persist_committed_supervision_result

    atexit.register(_cleanup_repo_services_for_root)
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
        live_terminal_pid = 0
        zombie_parent_pid = 0
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
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
            node_ref = state_root / "state" / f"{bootstrap['node_id']}.json"
            node_payload = json.loads(node_ref.read_text(encoding="utf-8"))
            stale_runtime_observed_at = "2026-03-24T00:00:00Z"
            node_payload["runtime_state"] = {
                "attachment_state": "LOST",
                "observed_at": stale_runtime_observed_at,
                "observation_kind": "recovery_retry_accepted",
                "summary": "stale retry truth",
                "evidence_refs": ["stale-runtime-state"],
            }
            _overwrite_json_projection(node_ref, node_payload)

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
            if str(live_status.get("runtime_observed_at") or "") == stale_runtime_observed_at:
                return _fail("a still-live child must refresh runtime_observed_at instead of reusing stale runtime_state timing")
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

            original_canonicalize = launch_runtime.canonicalize_workspace_artifact_heavy_trees
            try:
                def _raising_canonicalize(*_args: object, **_kwargs: object) -> list[str]:
                    raise OSError(66, "Directory not empty", "Tactic")

                launch_runtime.canonicalize_workspace_artifact_heavy_trees = _raising_canonicalize
                degraded_dead_status = launch_runtime.child_runtime_status_from_launch_result_ref(
                    result_ref=str(launch_result_ref),
                    stall_threshold_s=0,
                )
            finally:
                launch_runtime.canonicalize_workspace_artifact_heavy_trees = original_canonicalize
            Draft202012Validator(status_result_schema).validate(degraded_dead_status)
            if bool(degraded_dead_status.get("pid_alive")):
                return _fail("observation-error fallback must still report pid_alive=false for a dead child")
            if not bool(degraded_dead_status.get("recovery_eligible")):
                return _fail("observation-error fallback must preserve recovery eligibility for a confirmed dead ACTIVE child")
            if str(degraded_dead_status.get("runtime_observation_kind") or "") != "runtime_status_observation_error":
                return _fail("observation-error fallback must label degraded runtime status with runtime_status_observation_error")
            if "Directory not empty" not in str(degraded_dead_status.get("runtime_summary") or ""):
                return _fail("observation-error fallback must preserve the cleanup failure detail in runtime_summary")

            original_request = launch_runtime.request_host_child_runtime_status
            original_should = launch_runtime.should_request_host_child_runtime_status
            try:
                def _raising_host_runtime_status(**_kwargs: object) -> dict[str, object]:
                    raise RuntimeError("[Errno 66] Directory not empty: 'Tactic'")

                launch_runtime.request_host_child_runtime_status = _raising_host_runtime_status
                launch_runtime.should_request_host_child_runtime_status = lambda: True
                host_fallback_status = launch_runtime.child_runtime_status_from_launch_result_ref(
                    result_ref=str(launch_result_ref),
                    stall_threshold_s=0,
                )
            finally:
                launch_runtime.request_host_child_runtime_status = original_request
                launch_runtime.should_request_host_child_runtime_status = original_should
            Draft202012Validator(status_result_schema).validate(host_fallback_status)
            if bool(host_fallback_status.get("pid_alive")):
                return _fail("host observation fallback must correct a dead child to pid_alive=false")
            if not bool(host_fallback_status.get("recovery_eligible")):
                return _fail("host observation fallback must preserve recovery eligibility for a confirmed dead ACTIVE child")

            from loop_product import child_supervision_sidecar as sidecar_module

            supervision_runtime_ref = launch_result_ref.with_name("ChildSupervisionRuntime.json")
            supervision_result_ref = launch_result_ref.with_name("ChildSupervisionResult.json")
            supervision_runtime_ref.unlink(missing_ok=True)
            supervision_result_ref.unlink(missing_ok=True)

            original_ensure_supervision_running = sidecar_module.ensure_child_supervision_running
            try:
                reattach_calls: list[str] = []

                def _fake_ensure_supervision_running(*, launch_result_ref: str | Path, **_kwargs: object) -> dict[str, object]:
                    launch_path = Path(launch_result_ref).resolve()
                    reattach_calls.append(str(launch_path))
                    launch_path.with_name("ChildSupervisionRuntime.json").write_text(
                        json.dumps(
                            {
                                "schema": "loop_product.child_supervision_runtime",
                                "launch_result_ref": str(launch_path),
                                "pid": 1,
                            },
                            indent=2,
                            sort_keys=True,
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    return {"launch_result_ref": str(launch_path), "pid": 1}

                sidecar_module.ensure_child_supervision_running = _fake_ensure_supervision_running
                reattach_status = launch_runtime.child_runtime_status_from_launch_result_ref(
                    result_ref=str(launch_result_ref),
                    stall_threshold_s=0,
                )
            finally:
                sidecar_module.ensure_child_supervision_running = original_ensure_supervision_running
                supervision_runtime_ref.unlink(missing_ok=True)
            Draft202012Validator(status_result_schema).validate(reattach_status)
            if reattach_calls != [str(launch_result_ref.resolve())]:
                return _fail("trusted child runtime status helper must reattach committed supervision when an ACTIVE dead-pid launch has no live sidecar or settled supervision result")

            superseded_launch_result_ref = (
                state_root
                / "artifacts"
                / "launches"
                / str(bootstrap["node_id"])
                / "attempt_002"
                / "ChildLaunchResult.json"
            )
            superseded_launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
            superseded_launch_result_ref.write_text(
                json.dumps(
                    {
                        **launch,
                        "launch_result_ref": str(superseded_launch_result_ref.resolve()),
                        "launch_request_ref": str((superseded_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                        "stdout_ref": str((superseded_launch_result_ref.parent / "logs" / "stdout.txt").resolve()),
                        "stderr_ref": str((superseded_launch_result_ref.parent / "logs" / "stderr.txt").resolve()),
                        "launch_log_dir": str((superseded_launch_result_ref.parent / "logs").resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            try:
                superseded_reattach_calls: list[str] = []

                def _fake_ensure_supervision_running_superseded(*, launch_result_ref: str | Path, **_kwargs: object) -> dict[str, object]:
                    superseded_reattach_calls.append(str(Path(launch_result_ref).resolve()))
                    return {"launch_result_ref": str(Path(launch_result_ref).resolve()), "pid": 1}

                sidecar_module.ensure_child_supervision_running = _fake_ensure_supervision_running_superseded
                superseded_status = launch_runtime.child_runtime_status_from_launch_result_ref(
                    result_ref=str(launch_result_ref),
                    stall_threshold_s=0,
                )
            finally:
                sidecar_module.ensure_child_supervision_running = original_ensure_supervision_running
            Draft202012Validator(status_result_schema).validate(superseded_status)
            if superseded_reattach_calls:
                return _fail("trusted child runtime status helper must not reattach committed supervision for a superseded older launch attempt of the same node")

            from loop_product.runtime import lifecycle as runtime_lifecycle

            node_ref = state_root / "state" / f"{bootstrap['node_id']}.json"
            node_payload = json.loads(node_ref.read_text(encoding="utf-8"))
            terminal_report_ref = (
                state_root
                / "artifacts"
                / "evaluator_runs"
                / str(bootstrap["node_id"])
                / "EvaluationReport.json"
            ).resolve()
            terminal_report_ref.parent.mkdir(parents=True, exist_ok=True)
            terminal_report_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "summary": "review completed",
                        "verdict": "FAIL",
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            terminal_result_ref = (
                state_root / "artifacts" / str(bootstrap["node_id"]) / "result.json"
            ).resolve()
            terminal_result_ref.parent.mkdir(parents=True, exist_ok=True)
            terminal_result_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "outcome": "REPAIR_REQUIRED",
                        "summary": "authoritative terminal result already present",
                        "completed_at_utc": "2026-03-23T07:00:00Z",
                        "evaluation_report_ref": str(terminal_report_ref),
                        "evaluator_result": {"verdict": "FAIL"},
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            node_payload["status"] = "ACTIVE"
            node_payload["result_sink_ref"] = str(terminal_result_ref.relative_to(state_root))
            _overwrite_json_projection(node_ref, node_payload)

            original_sync_authoritative_state = runtime_lifecycle.synchronize_authoritative_state_from_launch_result_ref
            supervision_result_ref.write_text(
                json.dumps(
                    {
                        "launch_result_ref": str(launch_result_ref.resolve()),
                        "latest_launch_result_ref": str(launch_result_ref.resolve()),
                        "node_id": str(bootstrap["node_id"]),
                        "state_root": str(state_root.resolve()),
                        "workspace_root": str(workspace_root.resolve()),
                        "settled": True,
                        "settled_reason": "no_substantive_progress",
                        "recoveries_used": 0,
                        "implementer_result_ref": "",
                        "implementer_outcome": "",
                        "evaluator_verdict": "",
                        "status_result_ref": str(Path(str(confirmed_dead_status.get("status_result_ref") or "")).resolve()),
                        "history": [],
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            supervision_result_ref.unlink(missing_ok=True)

            try:
                skip_reattach_calls: list[str] = []

                def _fake_ensure_supervision_running_when_terminal(*, launch_result_ref: str | Path, **_kwargs: object) -> dict[str, object]:
                    skip_reattach_calls.append(str(Path(launch_result_ref).resolve()))
                    return {"launch_result_ref": str(Path(launch_result_ref).resolve()), "pid": 1}

                runtime_lifecycle.synchronize_authoritative_state_from_launch_result_ref = lambda **_kwargs: None
                sidecar_module.ensure_child_supervision_running = _fake_ensure_supervision_running_when_terminal
                terminal_status = trusted_child_runtime_status_from_launch_result_ref(
                    result_ref=str(launch_result_ref.resolve()),
                    stall_threshold_s=0,
                )
            finally:
                runtime_lifecycle.synchronize_authoritative_state_from_launch_result_ref = original_sync_authoritative_state
                sidecar_module.ensure_child_supervision_running = original_ensure_supervision_running

            Draft202012Validator(status_result_schema).validate(terminal_status)
            if skip_reattach_calls:
                return _fail("trusted child runtime status helper must not reattach committed supervision when an authoritative terminal result already exists for the same ACTIVE node")
            if str(terminal_status.get("runtime_observation_kind") or "") != "terminal_result":
                return _fail("authoritative terminal result presence must surface through terminal_result runtime observation")
            if str(terminal_status.get("recovery_reason") or "") != "authoritative_terminal_result":
                return _fail("authoritative terminal result presence must not be treated as orphaned-active recovery")

            retryable_report_ref = (
                state_root
                / "artifacts"
                / "evaluator_runs"
                / str(bootstrap["node_id"])
                / "RetryableEvaluationReport.json"
            ).resolve()
            retryable_report_ref.parent.mkdir(parents=True, exist_ok=True)
            retryable_report_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "summary": "retryable fail completed with repair guidance",
                        "verdict": "FAIL",
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            retryable_result_ref = (
                state_root / "artifacts" / str(bootstrap["node_id"]) / "retryable-result.json"
            ).resolve()
            retryable_result_ref.parent.mkdir(parents=True, exist_ok=True)
            retryable_result_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "outcome": "REPAIR_REQUIRED",
                        "summary": "retryable evaluator fail still needs local repair",
                        "completed_at_utc": "2026-03-23T07:10:00Z",
                        "evaluation_report_ref": str(retryable_report_ref),
                        "evaluator_result": {"verdict": "FAIL", "retryable": True},
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            node_payload["status"] = "ACTIVE"
            node_payload["result_sink_ref"] = str(retryable_result_ref.relative_to(state_root))
            node_payload["runtime_state"] = {
                "attachment_state": "TERMINAL",
                "observed_at": "2026-03-23T07:10:01Z",
                "observation_kind": "authoritative_result",
                "summary": "stale terminal retryable fail state",
                "evidence_refs": [str(retryable_result_ref.resolve())],
            }
            _overwrite_json_projection(node_ref, node_payload)

            retryable_fail_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref.resolve()),
                stall_threshold_s=0,
            )
            Draft202012Validator(status_result_schema).validate(retryable_fail_status)
            if str(retryable_fail_status.get("runtime_attachment_state") or "") == "TERMINAL":
                return _fail("retryable evaluator FAIL must not surface as terminal runtime closure in child runtime status")
            if str(retryable_fail_status.get("runtime_observation_kind") or "") == "terminal_result":
                return _fail("retryable evaluator FAIL must not be reported as terminal_result by the runtime status helper")
            if not bool(retryable_fail_status.get("recovery_eligible")):
                return _fail("retryable evaluator FAIL on a dead stale launch must remain recovery-eligible for local repair")
            if str(retryable_fail_status.get("recovery_reason") or "") != "active_without_live_pid":
                return _fail("retryable evaluator FAIL should fall back to ordinary dead-active recovery instead of terminal closure")

            dangling_terminal_report_ref = (
                state_root
                / "artifacts"
                / "evaluator_runs"
                / str(bootstrap["node_id"])
                / "DanglingEvaluationReport.json"
            ).resolve()
            dangling_terminal_result_ref = (
                state_root / "artifacts" / str(bootstrap["node_id"]) / "dangling-terminal-result.json"
            ).resolve()
            dangling_terminal_result_ref.parent.mkdir(parents=True, exist_ok=True)
            dangling_terminal_result_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "outcome": "FAIL_CLOSED",
                        "summary": "durable terminal node state already accepted",
                        "completed_at_utc": "2026-03-23T07:15:00Z",
                        "evaluation_report_ref": str(dangling_terminal_report_ref),
                        "evaluator_result": {"verdict": "ERROR"},
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            live_terminal_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                cwd=str(ROOT),
                text=True,
            )
            live_terminal_pid = int(live_terminal_proc.pid)
            terminal_live_launch_result_ref = (
                state_root
                / "artifacts"
                / "launches"
                / str(bootstrap["node_id"])
                / "attempt_003"
                / "ChildLaunchResult.json"
            ).resolve()
            terminal_live_stdout = terminal_live_launch_result_ref.parent / "logs" / "terminal-live.stdout.txt"
            terminal_live_stderr = terminal_live_launch_result_ref.parent / "logs" / "terminal-live.stderr.txt"
            terminal_live_stdout.parent.mkdir(parents=True, exist_ok=True)
            terminal_live_stdout.write_text("", encoding="utf-8")
            terminal_live_stderr.write_text("", encoding="utf-8")
            terminal_live_launch_result_ref.write_text(
                json.dumps(
                    {
                        **launch,
                        "launch_result_ref": str(terminal_live_launch_result_ref),
                        "launch_request_ref": str((terminal_live_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                        "pid": live_terminal_pid,
                        "stdout_ref": str(terminal_live_stdout.resolve()),
                        "stderr_ref": str(terminal_live_stderr.resolve()),
                        "launch_log_dir": str((terminal_live_launch_result_ref.parent / "logs").resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            node_payload["status"] = "FAILED"
            node_payload["result_sink_ref"] = str(dangling_terminal_result_ref.relative_to(state_root))
            node_payload["runtime_state"] = {
                "attachment_state": "TERMINAL",
                "observed_at": "2026-03-23T07:15:01Z",
                "observation_kind": "terminal_result",
                "summary": "durable terminal node state accepted",
                "evidence_refs": ["control_envelope:env_durable_terminal"],
            }
            _overwrite_json_projection(node_ref, node_payload)

            try:
                skip_reattach_durable_calls: list[str] = []

                def _fake_ensure_supervision_running_when_durable_terminal(*, launch_result_ref: str | Path, **_kwargs: object) -> dict[str, object]:
                    skip_reattach_durable_calls.append(str(Path(launch_result_ref).resolve()))
                    return {"launch_result_ref": str(Path(launch_result_ref).resolve()), "pid": 1}

                runtime_lifecycle.synchronize_authoritative_state_from_launch_result_ref = lambda **_kwargs: None
                sidecar_module.ensure_child_supervision_running = _fake_ensure_supervision_running_when_durable_terminal
                durable_terminal_status = trusted_child_runtime_status_from_launch_result_ref(
                    result_ref=str(terminal_live_launch_result_ref),
                    stall_threshold_s=0,
                )
            finally:
                runtime_lifecycle.synchronize_authoritative_state_from_launch_result_ref = original_sync_authoritative_state
                sidecar_module.ensure_child_supervision_running = original_ensure_supervision_running

            Draft202012Validator(status_result_schema).validate(durable_terminal_status)
            if skip_reattach_durable_calls:
                return _fail("trusted child runtime status helper must not reattach supervision when durable terminal node state is already present")
            if bool(durable_terminal_status.get("pid_alive")):
                return _fail("durable terminal node state must reap a still-live launch alias even when evaluation_report_ref is dangling")
            if str(durable_terminal_status.get("runtime_attachment_state") or "") != "TERMINAL":
                return _fail("durable terminal node state must surface as terminal attachment")
            if str(durable_terminal_status.get("runtime_observation_kind") or "") != "terminal_node_state":
                return _fail("durable terminal node state must surface through terminal_node_state observation when the evaluator report is dangling")
            if str(durable_terminal_status.get("recovery_reason") or "") != "authoritative_terminal_node_state":
                return _fail("durable terminal node state must not be treated as recovery-eligible just because evaluator evidence is dangling")
            _wait_dead(live_terminal_pid)
            if Path(f"/proc/{live_terminal_pid}").exists():
                return _fail("durable terminal node state must actually terminate the live launch alias pid")

            persist_committed_supervision_result(
                launch_result_ref=launch_result_ref.resolve(),
                result_payload={
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "latest_launch_result_ref": str(launch_result_ref.resolve()),
                    "node_id": str(bootstrap["node_id"]),
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "settled": True,
                    "settled_reason": "no_substantive_progress",
                    "recoveries_used": 0,
                    "implementer_result_ref": "",
                    "implementer_outcome": "",
                    "evaluator_verdict": "",
                    "status_result_ref": str(Path(str(confirmed_dead_status.get("status_result_ref") or "")).resolve()),
                    "history": [],
                },
            )

            synchronized_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref.resolve()),
                stall_threshold_s=0,
            )
            Draft202012Validator(status_result_schema).validate(synchronized_status)
            if str(synchronized_status.get("lifecycle_status") or "") != "BLOCKED":
                return _fail("trusted child runtime status helper must reconcile a settled no_substantive_progress stop point into BLOCKED lifecycle state")
            state_payload = json.loads((state_root / "state" / f"{bootstrap['node_id']}.json").read_text(encoding="utf-8"))
            if str(state_payload.get("status") or "") != "BLOCKED":
                return _fail("settled supervision stop points must no longer leave the durable node falsely ACTIVE")
            runtime_state = dict(state_payload.get("runtime_state") or {})
            if str(runtime_state.get("attachment_state") or "") != "LOST":
                return _fail("settled no_substantive_progress stop points must persist runtime attachment LOST")
            if "no_substantive_progress" not in str(runtime_state.get("summary") or ""):
                return _fail("settled supervision stop points must preserve the stop-point reason in runtime_state summary")
            kernel_state_payload = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
            blocked_reasons = dict(kernel_state_payload.get("blocked_reasons") or {})
            if "no_substantive_progress" not in str(blocked_reasons.get(str(bootstrap["node_id"])) or ""):
                return _fail("settled supervision stop points must persist a blocked reason for the same node")
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
            if live_terminal_pid > 0:
                try:
                    os.kill(live_terminal_pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                _wait_dead(live_terminal_pid)
            _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)

    print("[loop-system-child-runtime-status-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
