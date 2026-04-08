#!/usr/bin/env python3
"""Validate router runtime child launch wiring."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-runtime][FAIL] {msg}", file=sys.stderr)
    return 2


class _FakeAiLaunchHandle:
    def __init__(self, pid: int) -> None:
        self.pid = int(pid)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.launch import build_router_actor_launch_plan
        import loop.runtime as runtime_module
        from loop.core import ActorLaunchSpec
        from loop.events import ActorKind
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router runtime imports failed: {exc}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        router_db_path = tmp / "router.sqlite3"
        workspace_root = tmp / "workspace" / "node-1"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects_file = workspace_root / "FINAL_EFFECTS.md"
        final_effects_file.write_text("Do the work.\n", encoding="utf-8")
        prompt_path = tmp / "implementer.prompt.md"
        prompt_path.write_text("Implement carefully.\n", encoding="utf-8")

        launch_plan = build_router_actor_launch_plan(
            actor_kind=ActorKind.IMPLEMENTER,
            workspace_root=workspace_root,
            prompt_path=prompt_path,
            actor_env={"LOOP_NODE_ID": "1"},
            codex_home=tmp / ".codex_runtime",
        )
        expected_cmd = [
            "codex",
            "exec",
            "-C",
            str(workspace_root.resolve()),
            "--skip-git-repo-check",
            "-s",
            "danger-full-access",
            "-c",
            'model_reasoning_effort="xhigh"',
            "-",
        ]
        if launch_plan.argv != expected_cmd:
            return _fail("router launch builder must emit the committed codex exec argv policy")
        if launch_plan.bridge_mode_override != "direct":
            return _fail("router launch builder must force direct bridge mode")
        if launch_plan.start_new_session is not False:
            return _fail("router launch builder must keep router child launches in the current process session")
        if launch_plan.cwd.resolve() != workspace_root.resolve():
            return _fail("router launch builder must use the actor workspace as cwd")
        if launch_plan.stdin_path.resolve() != prompt_path.resolve():
            return _fail("router launch builder must preserve the prompt path as stdin_path")
        if str(launch_plan.env.get("CODEX_HOME") or "") != str((tmp / ".codex_runtime").resolve()):
            return _fail("router launch builder must inject the provided child CODEX_HOME")

        tester_launch_plan = build_router_actor_launch_plan(
            actor_kind=ActorKind.EVALUATOR_TESTER,
            workspace_root=workspace_root,
            prompt_path=prompt_path,
            actor_env={"LOOP_NODE_ID": "1"},
            codex_home=tmp / ".codex_runtime",
        )
        expected_tester_cmd = [
            "codex",
            "exec",
            "-C",
            str(workspace_root.resolve()),
            "review",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            "-c",
            'model_reasoning_effort="xhigh"',
            "-",
        ]
        if tester_launch_plan.argv != expected_tester_cmd:
            return _fail("router tester launch builder must switch tester to codex exec review argv")
        if tester_launch_plan.stdin_path.resolve() != prompt_path.resolve():
            return _fail("router tester review launch must still preserve the prompt path as stdin_path")

        captured: dict[str, object] = {}
        original_start_ai_launch = runtime_module.start_ai_launch
        original_wait_for_rollout = runtime_module._wait_for_rollout
        try:
            def _fake_start_ai_launch(
                *,
                cmd,
                cwd,
                log_dir,
                label,
                env=None,
                stdin_path=None,
                start_new_session=False,
                bridge_mode_override=None,
            ):
                captured["cmd"] = [str(item) for item in cmd]
                captured["cwd"] = str(cwd)
                captured["log_dir"] = str(log_dir)
                captured["label"] = str(label)
                captured["env"] = dict(env or {})
                captured["stdin_path"] = str(stdin_path or "")
                captured["start_new_session"] = bool(start_new_session)
                captured["bridge_mode_override"] = str(bridge_mode_override or "")
                return _FakeAiLaunchHandle(pid=424242)

            runtime_module.start_ai_launch = _fake_start_ai_launch
            runtime_module._wait_for_rollout = lambda runtime_codex_home, timeout_seconds=15.0: (
                "runtime-session-001",
                (Path(runtime_codex_home) / "sessions" / "fake-rollout.jsonl").resolve(),
            )

            launcher = runtime_module.build_actor_launcher(router_db_path=router_db_path)
            spec = ActorLaunchSpec(
                node_id="1",
                parent_node_id="0",
                actor_kind=ActorKind.IMPLEMENTER,
                attempt_count=1,
                workspace_root=workspace_root.resolve(),
                final_effects_file=final_effects_file.resolve(),
                prompt_text="Implement carefully.\n",
                env={"LOOP_NODE_ID": "1"},
            )
            launch = launcher(spec)
        finally:
            runtime_module.start_ai_launch = original_start_ai_launch
            runtime_module._wait_for_rollout = original_wait_for_rollout

        if captured.get("bridge_mode_override") != "direct":
            return _fail("router runtime child launch must explicitly disable tmux bridge by forcing direct mode")
        if captured.get("start_new_session") is not False:
            return _fail("router runtime child launch must continue to launch in-process without start_new_session")
        if captured.get("cmd") != expected_cmd:
            return _fail("router runtime child launch must preserve the committed codex exec argv shape and explicit startup policy")
        env = dict(captured.get("env") or {})
        if "CODEX_HOME" not in env:
            return _fail("router runtime child launch must still materialize a child CODEX_HOME")
        if launch.session_id != "runtime-session-001":
            return _fail("router runtime child launch must return the rollout-derived session id")
        if int(getattr(launch.process, "pid", 0) or 0) != 424242:
            return _fail("router runtime child launch must preserve the ai launch pid")

        captured.clear()
        original_start_ai_launch = runtime_module.start_ai_launch
        original_wait_for_rollout = runtime_module._wait_for_rollout
        try:
            def _fake_start_ai_launch_tester(
                *,
                cmd,
                cwd,
                log_dir,
                label,
                env=None,
                stdin_path=None,
                start_new_session=False,
                bridge_mode_override=None,
            ):
                captured["cmd"] = [str(item) for item in cmd]
                captured["cwd"] = str(cwd)
                captured["log_dir"] = str(log_dir)
                captured["label"] = str(label)
                captured["env"] = dict(env or {})
                captured["stdin_path"] = str(stdin_path or "")
                captured["start_new_session"] = bool(start_new_session)
                captured["bridge_mode_override"] = str(bridge_mode_override or "")
                return _FakeAiLaunchHandle(pid=525252)

            runtime_module.start_ai_launch = _fake_start_ai_launch_tester
            runtime_module._wait_for_rollout = lambda runtime_codex_home, timeout_seconds=15.0: (
                "runtime-session-002",
                (Path(runtime_codex_home) / "sessions" / "fake-review-rollout.jsonl").resolve(),
            )

            launcher = runtime_module.build_actor_launcher(router_db_path=router_db_path)
            tester_spec = ActorLaunchSpec(
                node_id="1",
                parent_node_id="0",
                actor_kind=ActorKind.EVALUATOR_TESTER,
                attempt_count=1,
                workspace_root=workspace_root.resolve(),
                final_effects_file=final_effects_file.resolve(),
                prompt_text="Run one global code review.\n",
                env={"LOOP_NODE_ID": "1"},
            )
            tester_launch = launcher(tester_spec)
        finally:
            runtime_module.start_ai_launch = original_start_ai_launch
            runtime_module._wait_for_rollout = original_wait_for_rollout

        if captured.get("cmd") != expected_tester_cmd:
            return _fail("router tester runtime launch must preserve the committed codex exec review argv shape")
        if captured.get("stdin_path") != str(
            (tmp / "router" / "runtime" / "node-1" / "evaluator_tester" / "attempt-1" / "evaluator_tester.prompt.md").resolve()
        ):
            return _fail("router tester runtime launch must still pass the tester prompt file via stdin_path")
        if tester_launch.session_id != "runtime-session-002":
            return _fail("router tester runtime launch must return the rollout-derived session id")
        if int(getattr(tester_launch.process, "pid", 0) or 0) != 525252:
            return _fail("router tester runtime launch must preserve the ai launch pid")
        implementer_codex_home = str((tmp / "router" / "runtime" / "node-1" / "implementer" / "attempt-1" / ".codex_runtime").resolve())
        tester_codex_home = str((captured.get("env") or {}).get("CODEX_HOME") or "")
        if tester_codex_home == implementer_codex_home:
            return _fail("router runtime must not reuse implementer CODEX_HOME for tester attempt_count=1")

    print("[loop-system-router-runtime][OK] router runtime child launch forces direct bridge mode")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
