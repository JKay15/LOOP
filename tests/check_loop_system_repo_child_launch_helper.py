#!/usr/bin/env python3
"""Validate the committed child-launch helper."""

from __future__ import annotations

import json
import os
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-child-launch-helper][FAIL] {msg}", file=sys.stderr)
    return 2


def _run_helper(
    *,
    script: Path,
    result_ref: Path,
    wrapper_path: Path,
    startup_probe_ms: int = 50,
    startup_health_timeout_ms: int = 250,
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    env["LOOP_CHILD_LAUNCH_WRAPPER"] = str(wrapper_path.resolve())
    env["LOOP_CHILD_LAUNCH_MODE"] = "direct"
    return subprocess.run(
        [
            str(script),
            "--result-ref",
            str(result_ref.resolve()),
            "--startup-probe-ms",
            str(startup_probe_ms),
            "--startup-health-timeout-ms",
            str(startup_health_timeout_ms),
        ],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        env=env,
    )


def main() -> int:
    script = ROOT / "scripts" / "launch_child_from_result.sh"
    request_schema_path = ROOT / "docs" / "schemas" / "LoopChildLaunchRequest.schema.json"
    result_schema_path = ROOT / "docs" / "schemas" / "LoopChildLaunchResult.schema.json"
    if not script.exists():
        return _fail("missing committed child launch helper wrapper")
    if not request_schema_path.exists() or not result_schema_path.exists():
        return _fail("child launch helper must ship request/result schemas")

    request_schema = json.loads(request_schema_path.read_text(encoding="utf-8"))
    result_schema = json.loads(result_schema_path.read_text(encoding="utf-8"))

    workspace_root = ROOT / "workspace" / "test-child-launch-helper"
    state_root = ROOT / ".loop" / "test-child-launch-helper"
    try:
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        (state_root / "artifacts" / "bootstrap").mkdir(parents=True, exist_ok=True)

        prompt_path = workspace_root / "CHILD_PROMPT.md"
        prompt_text = "PROMPT_PAYLOAD_FROM_CHILD_LAUNCH_HELPER\n"
        prompt_path.write_text(prompt_text, encoding="utf-8")

        wrapper_marker = workspace_root / "wrapper_used.txt"
        wrapper_path = workspace_root / "wrapper.sh"
        wrapper_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env sh",
                    "set -eu",
                    f"printf '%s\\n' WRAPPED >> '{wrapper_marker.as_posix()}'",
                    'exec "$@"',
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        wrapper_path.chmod(0o755)

        deterministic_result = {
            "node_id": "test-child-launch-helper",
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "launch_spec": {
                "argv": ["/bin/sh", "-lc", "cat > child.stdin.txt; printf DONE"],
                "env": {},
                "cwd": str(workspace_root.resolve()),
                "stdin_path": str(prompt_path.resolve()),
            },
        }
        fake_result_path = state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json"
        fake_result_path.write_text(json.dumps(deterministic_result, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        proc = _run_helper(script=script, result_ref=fake_result_path, wrapper_path=wrapper_path)
        if proc.returncode != 0:
            detail = proc.stderr.strip() or proc.stdout.strip()
            return _fail(f"committed child launch helper wrapper must succeed: {detail}")

        result = json.loads(proc.stdout)
        Draft202012Validator(result_schema).validate(result)

        if str(result.get("launch_decision") or "") != "exited":
            return _fail("short deterministic child launch should exit during startup probe")
        if int(result.get("startup_health_timeout_ms") or -1) != 250:
            return _fail("child launch result must preserve the startup health timeout used for classification")
        if str(result.get("source_result_ref") or "") != str(fake_result_path.resolve()):
            return _fail("launch result must preserve the exact source result ref")
        if int(result.get("exit_code")) != 0:
            return _fail("launch helper must preserve successful child exit_code")

        request_ref = Path(str(result.get("launch_request_ref") or ""))
        result_ref = Path(str(result.get("launch_result_ref") or ""))
        stdout_ref = Path(str(result.get("stdout_ref") or ""))
        stderr_ref = Path(str(result.get("stderr_ref") or ""))
        if not request_ref.exists() or not result_ref.exists():
            return _fail("launch helper must persist both request and result artifacts")
        if not stdout_ref.exists() or not stderr_ref.exists():
            return _fail("launch helper must persist launch stdout/stderr refs")

        request_payload = json.loads(request_ref.read_text(encoding="utf-8"))
        Draft202012Validator(request_schema).validate(request_payload)
        if str(request_payload.get("node_id") or "") != "test-child-launch-helper":
            return _fail("launch request must preserve the exact node_id")

        wrapped_argv = list(result.get("wrapped_argv") or [])
        if not wrapped_argv or wrapped_argv[0] != str(wrapper_path.resolve()):
            return _fail("launch helper must prefix child launch through the local wrapper hook when configured")
        if int(result.get("attempt_count") or 0) != 1:
            return _fail("deterministic child launch should complete in a single helper attempt")

        if wrapper_marker.read_text(encoding="utf-8").strip() != "WRAPPED":
            return _fail("wrapper hook must actually execute before the child command")
        if (workspace_root / "child.stdin.txt").read_text(encoding="utf-8") != prompt_text:
            return _fail("launch helper must pipe stdin_path into the child command")
        if "DONE" not in stdout_ref.read_text(encoding="utf-8", errors="replace"):
            return _fail("launch helper must preserve child stdout in the structured log ref")

        retry_counter = workspace_root / "retry.counter"
        retry_result_path = state_root / "artifacts" / "bootstrap" / "RetryImplementerBootstrapResult.json"
        retry_result_path.write_text(
            json.dumps(
                {
                    "node_id": "test-child-launch-helper",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": [
                            "/bin/sh",
                            "-lc",
                            (
                                f"count=$(cat '{retry_counter.as_posix()}' 2>/dev/null || echo 0); "
                                "count=$((count + 1)); "
                                f"printf '%s' \"$count\" > '{retry_counter.as_posix()}'; "
                                'if [ "$count" -eq 1 ]; then '
                                "printf '%s\\n' \"We're currently experiencing high demand\" >&2; "
                                "exit 1; "
                                "fi; "
                                "cat > retry.stdin.txt; "
                                "printf RETRY_OK"
                            ),
                        ],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(prompt_path.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        retry_proc = _run_helper(script=script, result_ref=retry_result_path, wrapper_path=wrapper_path)
        if retry_proc.returncode != 0:
            detail = retry_proc.stderr.strip() or retry_proc.stdout.strip()
            return _fail(f"retryable early-exit launch should eventually succeed: {detail}")
        retry_result = json.loads(retry_proc.stdout)
        Draft202012Validator(result_schema).validate(retry_result)
        if str(retry_result.get("launch_decision") or "") != "exited":
            return _fail("retry-once helper launch should return exited after the successful second attempt")
        if int(retry_result.get("attempt_count") or 0) != 2:
            return _fail("retryable early-exit launch must use a second helper attempt")
        if str(retry_result.get("retryable_failure_kind") or "") != "":
            return _fail("successful retry should clear the final retryable failure classification")
        attempts = list(retry_result.get("attempts") or [])
        if len(attempts) != 2:
            return _fail("child launch result must retain structured evidence for both startup attempts")
        if str(attempts[0].get("retryable_failure_kind") or "") != "provider_capacity":
            return _fail("high-demand early exit should classify as provider_capacity")
        if int(attempts[0].get("exit_code") or -1) != 1:
            return _fail("first retryable launch attempt must preserve its non-zero exit code")
        if "RETRY_OK" not in Path(str(retry_result.get("stdout_ref") or "")).read_text(encoding="utf-8", errors="replace"):
            return _fail("retryable success path must preserve final child stdout")
        if (workspace_root / "retry.stdin.txt").read_text(encoding="utf-8") != prompt_text:
            return _fail("successful retry attempt must still receive the frozen prompt through stdin")

        exhausted_result_path = state_root / "artifacts" / "bootstrap" / "RetryExhaustedBootstrapResult.json"
        exhausted_result_path.write_text(
            json.dumps(
                {
                    "node_id": "test-child-launch-helper",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": [
                            "/bin/sh",
                            "-lc",
                            "printf '%s\\n' 'stream disconnected before completion: error sending request for url' >&2; exit 1",
                        ],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(prompt_path.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        exhausted_proc = _run_helper(script=script, result_ref=exhausted_result_path, wrapper_path=wrapper_path)
        if exhausted_proc.returncode != 0:
            detail = exhausted_proc.stderr.strip() or exhausted_proc.stdout.strip()
            return _fail(f"retry-exhausted child launch should still return structured output: {detail}")
        exhausted_result = json.loads(exhausted_proc.stdout)
        Draft202012Validator(result_schema).validate(exhausted_result)
        if str(exhausted_result.get("launch_decision") or "") != "retry_exhausted":
            return _fail("persistent retryable early-exit launch should report retry_exhausted")
        if int(exhausted_result.get("attempt_count") or 0) != 2:
            return _fail("retry-exhausted launch should consume the bounded second attempt")
        if str(exhausted_result.get("retryable_failure_kind") or "") != "provider_transport":
            return _fail("stream-disconnect early exit should classify as provider_transport")
        exhausted_attempts = list(exhausted_result.get("attempts") or [])
        if len(exhausted_attempts) != 2:
            return _fail("retry-exhausted launch must retain both failed startup attempts")

        unhealthy_result_path = state_root / "artifacts" / "bootstrap" / "StartupUnhealthyBootstrapResult.json"
        unhealthy_result_path.write_text(
            json.dumps(
                {
                    "node_id": "test-child-launch-helper",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": [
                            "/bin/sh",
                            "-lc",
                            (
                                "printf '%s\\n' 'failed to connect to websocket: Operation not permitted (os error 1)' >&2; "
                                "sleep 1"
                            ),
                        ],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(prompt_path.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        unhealthy_proc = _run_helper(
            script=script,
            result_ref=unhealthy_result_path,
            wrapper_path=wrapper_path,
            startup_probe_ms=50,
            startup_health_timeout_ms=150,
        )
        if unhealthy_proc.returncode != 0:
            detail = unhealthy_proc.stderr.strip() or unhealthy_proc.stdout.strip()
            return _fail(f"startup-unhealthy child launch should still return structured output: {detail}")
        unhealthy_result = json.loads(unhealthy_proc.stdout)
        Draft202012Validator(result_schema).validate(unhealthy_result)
        if str(unhealthy_result.get("launch_decision") or "") != "retry_exhausted":
            return _fail("persistent startup-unhealthy launch must classify as retry_exhausted instead of started")
        if int(unhealthy_result.get("attempt_count") or 0) != 2:
            return _fail("startup-unhealthy launch should consume the bounded second attempt")
        if str(unhealthy_result.get("retryable_failure_kind") or "") != "provider_transport":
            return _fail("startup-unhealthy websocket failure must classify as provider_transport")

        live_node_id = "test-child-launch-helper-live"
        live_result_path = state_root / "artifacts" / "bootstrap" / "LiveBootstrapResult.json"
        live_result_path.write_text(
            json.dumps(
                {
                    "node_id": live_node_id,
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "launch_spec": {
                        "argv": [
                            sys.executable,
                            "-c",
                            (
                                "from pathlib import Path; import sys, time; "
                                f"Path(r'{(workspace_root / 'live.stdin.txt').resolve()}').write_bytes(sys.stdin.buffer.read()); "
                                "time.sleep(30)"
                            ),
                        ],
                        "env": {},
                        "cwd": str(workspace_root.resolve()),
                        "stdin_path": str(prompt_path.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        live_proc = _run_helper(script=script, result_ref=live_result_path, wrapper_path=wrapper_path, startup_probe_ms=50, startup_health_timeout_ms=250)
        if live_proc.returncode != 0:
            detail = live_proc.stderr.strip() or live_proc.stdout.strip()
            return _fail(f"long-running child launch must succeed and stay started: {detail}")
        live_result = json.loads(live_proc.stdout)
        Draft202012Validator(result_schema).validate(live_result)
        if str(live_result.get("launch_decision") or "") != "started":
            return _fail("long-running child launch must report a started child")
        live_pid = int(live_result.get("pid") or 0)
        if live_pid <= 0:
            return _fail("started child launch must preserve a positive pid")
        if (workspace_root / "live.stdin.txt").read_text(encoding="utf-8") != prompt_text:
            return _fail("started long-running child must still receive the frozen prompt through stdin")

        reused_proc = _run_helper(script=script, result_ref=live_result_path, wrapper_path=wrapper_path, startup_probe_ms=50, startup_health_timeout_ms=250)
        if reused_proc.returncode != 0:
            detail = reused_proc.stderr.strip() or reused_proc.stdout.strip()
            return _fail(f"relaunching an already-live child must return structured reuse output: {detail}")
        reused_result = json.loads(reused_proc.stdout)
        Draft202012Validator(result_schema).validate(reused_result)
        if str(reused_result.get("launch_decision") or "") != "started_existing":
            return _fail("second launch against the same live child must reuse the existing process instead of starting another one")
        if int(reused_result.get("pid") or 0) != live_pid:
            return _fail("reused live child result must point at the same pid")
        if int(reused_result.get("attempt_count", -1)) != 0:
            return _fail("reused live child launch must not record a fresh launch attempt")
        reuse_ref = Path(str(reused_result.get("reuse_launch_result_ref") or ""))
        if reuse_ref.resolve() != Path(str(live_result.get("launch_result_ref") or "")).resolve():
            return _fail("reused live child launch must preserve the exact prior launch result ref")

        attempt_dirs = sorted((state_root / "artifacts" / "launches" / live_node_id).glob("attempt_*"))
        if len(attempt_dirs) != 2:
            return _fail("reused live child launch must only persist one original attempt and one reuse result envelope")

        from loop_product import ai_launch as ai_launch_module
        from loop_product.dispatch import launch_runtime as launch_runtime_module

        original_tmux_available = ai_launch_module._tmux_available
        original_resolve_tmux_target_session = ai_launch_module._resolve_tmux_target_session
        original_start_tmux_launch = ai_launch_module._start_tmux_launch
        try:
            bridge_capture: dict[str, object] = {}

            def _fake_tmux_launch(
                *,
                cmd: object,
                cwd: Path,
                log_dir: Path,
                label: str,
                env: object | None,
                stdin_path: object | None,
            ) -> object:
                del cwd, env, stdin_path
                log_dir.mkdir(parents=True, exist_ok=True)
                stdout_path = log_dir / "bridge.stdout.txt"
                stderr_path = log_dir / "bridge.stderr.txt"
                stdout_path.write_text("", encoding="utf-8")
                stderr_path.write_text("", encoding="utf-8")
                bridge_capture["cmd"] = list(cmd)
                bridge_capture["label"] = label
                return ai_launch_module.AiLaunchHandle(
                    mode="tmux",
                    cmd=[str(item) for item in list(cmd)],
                    cwd=str(workspace_root.resolve()),
                    log_dir=log_dir,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    started_at_s=0.0,
                    pid=99999,
                    tmux_session_name="main",
                    tmux_window_index="99",
                    tmux_pane_id="%99",
                    tmux_pane_pid=99999,
                    tmux_exit_code_path=log_dir / "bridge.exit_code.txt",
                    tmux_script_path=log_dir / "bridge.tmux-launch.sh",
                )

            ai_launch_module._tmux_available = lambda: True
            ai_launch_module._resolve_tmux_target_session = lambda: "main"
            ai_launch_module._start_tmux_launch = _fake_tmux_launch
            os.environ["CODEX_THREAD_ID"] = "test-thread"
            bridged = ai_launch_module.start_ai_launch(
                cmd=["codex", "exec", "-C", str(workspace_root.resolve()), "-"],
                cwd=workspace_root,
                log_dir=workspace_root / ".artifacts" / "bridge",
                label="bridge-check",
                env={},
                stdin_path=prompt_path,
                start_new_session=False,
            )
            if bridged.mode != "tmux":
                return _fail("committed AI launch must select the tmux bridge when nested Codex launch is active")
            if list(bridge_capture.get("cmd") or [])[:2] != ["codex", "exec"]:
                return _fail("tmux bridge selection must preserve the original committed Codex argv")
        finally:
            ai_launch_module._tmux_available = original_tmux_available
            ai_launch_module._resolve_tmux_target_session = original_resolve_tmux_target_session
            ai_launch_module._start_tmux_launch = original_start_tmux_launch
            os.environ.pop("CODEX_THREAD_ID", None)

        captured: dict[str, object] = {}
        original_observe_startup_health = launch_runtime_module._observe_startup_health
        original_start_ai_launch = launch_runtime_module.start_ai_launch
        original_detach_ai_launch_handle = launch_runtime_module.detach_ai_launch_handle
        try:
            def _fake_start_ai_launch(
                *,
                cmd: object,
                cwd: object,
                log_dir: Path,
                label: str,
                env: object | None = None,
                stdin_path: object | None = None,
                start_new_session: bool = True,
            ) -> object:
                del cmd, cwd, label, env, stdin_path
                log_dir.mkdir(parents=True, exist_ok=True)
                stdout_path = log_dir / "fake.stdout.txt"
                stderr_path = log_dir / "fake.stderr.txt"
                stdout_path.write_text("", encoding="utf-8")
                stderr_path.write_text("", encoding="utf-8")
                captured["start_new_session"] = start_new_session
                return ai_launch_module.AiLaunchHandle(
                    mode="direct",
                    cmd=["codex", "exec", "-"],
                    cwd=str(workspace_root.resolve()),
                    log_dir=log_dir,
                    stdout_path=stdout_path,
                    stderr_path=stderr_path,
                    started_at_s=0.0,
                    pid=424242,
                )

            def _fake_observe_startup_health(handle: object, *, startup_health_timeout_ms: int) -> tuple[str, int | None]:
                del handle, startup_health_timeout_ms
                return "", None

            def _fake_detach_ai_launch_handle(handle: object) -> None:
                del handle

            launch_runtime_module.start_ai_launch = _fake_start_ai_launch
            launch_runtime_module._observe_startup_health = _fake_observe_startup_health
            launch_runtime_module.detach_ai_launch_handle = _fake_detach_ai_launch_handle

            started_result = launch_runtime_module.launch_child_from_result_ref(result_ref=fake_result_path, startup_probe_ms=0)
            if str(started_result.get("launch_decision") or "") != "started":
                return _fail("child launch helper unit path must preserve a healthy started child")
            if captured.get("start_new_session") is not False:
                return _fail("committed child launch helper must launch Codex children without start_new_session detaching")
        finally:
            launch_runtime_module.start_ai_launch = original_start_ai_launch
            launch_runtime_module._observe_startup_health = original_observe_startup_health
            launch_runtime_module.detach_ai_launch_handle = original_detach_ai_launch_handle
        try:
            os.kill(live_pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.time() + 5.0
        while time.time() < deadline:
            try:
                os.kill(live_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
    finally:
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)

    print("[loop-system-child-launch-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
