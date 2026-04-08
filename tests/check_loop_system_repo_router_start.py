#!/usr/bin/env python3
"""Validate router start/materialization minimal closure."""

from __future__ import annotations

import sys
import subprocess
import tempfile
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-start][FAIL] {msg}", file=sys.stderr)
    return 2


class _FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = int(pid)
        self._done = threading.Event()
        self._returncode = 0
        self.terminate_calls = 0

    def wait(self) -> int:
        self._done.wait()
        return int(self._returncode)

    def poll(self) -> int | None:
        return None

    def terminate(self) -> None:
        self.terminate_calls += 1


class _FakeProcSupervisor:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.registered: list[dict[str, object]] = []

    def start(self) -> None:
        self.started += 1

    def stop(self) -> None:
        self.stopped += 1

    def register_actor(
        self,
        actor,
        *,
        process,
        process_birth_time,
        session_id,
        rollout_path=None,
        now=None,
    ):
        self.registered.append(
            {
                "actor": actor,
                "process": process,
                "process_birth_time": process_birth_time,
                "session_id": session_id,
                "rollout_path": rollout_path,
                "now": now,
            }
        )
        return rollout_path


class _RaisingProcSupervisor(_FakeProcSupervisor):
    def register_actor(
        self,
        actor,
        *,
        process,
        process_birth_time,
        session_id,
        rollout_path=None,
        now=None,
    ):
        super().register_actor(
            actor,
            process=process,
            process_birth_time=process_birth_time,
            session_id=session_id,
            rollout_path=rollout_path,
            now=now,
        )
        raise RuntimeError("register failed")


class _StartFailingRouterCore:
    """Mixin-like helper used to force start() failure after launch commit."""

    def start(self) -> None:
        raise RuntimeError("steady state failed")


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.completion import actor_completion_record_path
        from loop.core import ActorLaunchResult, ActorLaunchSpec, RouterCore
        from loop.node_table import ComponentStatus, component_key
        from loop.events import ActorKind
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router start imports failed: {exc}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        workspace_root = tmp / "workspace" / "demo_project"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects_file = workspace_root / "FINAL_EFFECTS.md"
        kernel_rollout_file = tmp / "kernel-rollout.jsonl"
        final_effects_file.write_text("Finish the product correctly.\n", encoding="utf-8")
        subprocess.run(["git", "-C", str(workspace_root), "init", "-q"], check=True)
        subprocess.run(["git", "-C", str(workspace_root), "config", "user.email", "router-test@example.com"], check=True)
        subprocess.run(["git", "-C", str(workspace_root), "config", "user.name", "Router Test"], check=True)
        subprocess.run(["git", "-C", str(workspace_root), "add", "FINAL_EFFECTS.md"], check=True)
        subprocess.run(["git", "-C", str(workspace_root), "commit", "-q", "-m", "init"], check=True)
        base_commit = subprocess.run(
            ["git", "-C", str(workspace_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        kernel_rollout_file.write_text(
            '{"type":"session_meta","payload":{"id":"kernel-session-001","timestamp":"2026-04-03T00:30:00Z"}}\n',
            encoding="utf-8",
        )

        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        fake_process = _FakeProcess(pid=54321)
        launch_specs: list[ActorLaunchSpec] = []

        def _fake_actor_launcher(request: ActorLaunchSpec) -> ActorLaunchResult:
            launch_specs.append(request)
            return ActorLaunchResult(
                process=fake_process,
                process_birth_time=1712275200.125,
                session_id="root-session-001",
                rollout_path=(tmp / "runtime" / "root-rollout.jsonl").resolve(),
            )

        core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "router" / "wakeup.sock",
            proc_supervisor=fake_proc,
            actor_launcher=_fake_actor_launcher,
        )
        try:
            payload = core.start_router(
                kernel_session_id="kernel-session-001",
                kernel_rollout_path=str(kernel_rollout_file.resolve()),
                kernel_started_at="2026-04-03T00:30:00Z",
                final_effects_file=final_effects_file,
            )

            if payload.get("accepted") is not True or payload.get("status") != "STARTED":
                return _fail("start_router must return an accepted STARTED payload on success")
            if payload.get("root_node_id") != "1":
                return _fail("start_router must materialize root node 1")
            if payload.get("root_actor_kind") != "implementer":
                return _fail("start_router must report implementer as the root actor kind")
            if int(payload.get("root_attempt_count") or 0) != 1:
                return _fail("start_router must report root attempt_count=1")
            if int(payload.get("root_pid") or 0) != 54321:
                return _fail("start_router must surface the launched root actor pid")
            if str(payload.get("root_session_id") or "") != "root-session-001":
                return _fail("start_router must surface the launched root actor session_id")
            if Path(str(payload.get("root_rollout_path") or "")).resolve() != (
                tmp / "runtime" / "root-rollout.jsonl"
            ).resolve():
                return _fail("start_router must surface the launched root actor rollout path")

            if fake_proc.started != 1:
                return _fail("start_router must start proc before steady-state runtime begins")
            if len(fake_proc.registered) != 1:
                return _fail("start_router must register exactly one root actor with proc")

            registered = fake_proc.registered[0]
            registered_actor = registered["actor"]
            if registered_actor.node_id != "1" or registered_actor.actor_kind is not ActorKind.IMPLEMENTER:
                return _fail("proc registration must use root node 1 implementer actor identity")
            if int(registered_actor.attempt_count) != 1:
                return _fail("proc registration must use root attempt_count=1")
            if str(registered.get("session_id") or "") != "root-session-001":
                return _fail("proc registration must preserve the launched root session_id")
            if Path(str(registered.get("rollout_path") or "")).resolve() != (
                tmp / "runtime" / "root-rollout.jsonl"
            ).resolve():
                return _fail("proc registration must use the launched explicit rollout path")

            if len(launch_specs) != 1:
                return _fail("start_router must launch exactly one root actor")
            launch_spec = launch_specs[0]
            if launch_spec.parent_node_id != "0":
                return _fail("root actor launch spec must use parent_node_id=0")
            if launch_spec.actor_kind is not ActorKind.IMPLEMENTER:
                return _fail("root actor launch spec must target the implementer actor")
            if launch_spec.workspace_root.resolve() != workspace_root.resolve():
                return _fail("root actor launch must use final_effects_file parent as workspace_root")
            if launch_spec.final_effects_file.resolve() != final_effects_file.resolve():
                return _fail("root actor launch must preserve final_effects_file path")
            if "Finish the product correctly." not in launch_spec.prompt_text:
                return _fail("root actor prompt must inline the full final_effects_file content")
            if str(launch_spec.env.get("LOOP_ROUTER_DB_PATH") or "") != str(db_path.resolve()):
                return _fail("root actor env must inject LOOP_ROUTER_DB_PATH")
            if str(launch_spec.env.get("LOOP_NODE_ID") or "") != "1":
                return _fail("root actor env must inject LOOP_NODE_ID=1")
            if str(launch_spec.env.get("LOOP_PARENT_NODE_ID") or "") != "0":
                return _fail("root actor env must inject LOOP_PARENT_NODE_ID=0")
            if str(launch_spec.env.get("LOOP_ACTOR_KIND") or "") != "implementer":
                return _fail("root actor env must inject LOOP_ACTOR_KIND=implementer")
            if str(launch_spec.env.get("LOOP_ATTEMPT_COUNT") or "") != "1":
                return _fail("root actor env must inject LOOP_ATTEMPT_COUNT=1")
            if str(launch_spec.env.get("LOOP_WORKSPACE_ROOT") or "") != str(workspace_root.resolve()):
                return _fail("root actor env must inject LOOP_WORKSPACE_ROOT")
            if str(launch_spec.env.get("LOOP_FINAL_EFFECTS_FILE") or "") != str(final_effects_file.resolve()):
                return _fail("root actor env must inject LOOP_FINAL_EFFECTS_FILE")
            if str(launch_spec.env.get("LOOP_COMPLETION_FILE") or "") != str(
                actor_completion_record_path(workspace_root.resolve(), ActorKind.IMPLEMENTER)
            ):
                return _fail("root actor env must inject LOOP_COMPLETION_FILE")
            if "request-split --split-bundle-ref" not in launch_spec.prompt_text or "complete" not in launch_spec.prompt_text:
                return _fail("root actor prompt must explain split/control completion interface usage")

            if store.read_kernel_session_id() != "kernel-session-001":
                return _fail("start_router must persist kernel_session_id")
            if store.read_kernel_rollout_path() != str(kernel_rollout_file.resolve()):
                return _fail("start_router must persist kernel_rollout_path")
            if store.read_kernel_started_at() != "2026-04-03T00:30:00Z":
                return _fail("start_router must persist kernel_started_at")

            root_record = store.load_node("1")
            if root_record is None:
                return _fail("start_router must materialize node 1 into node_table")
            if root_record.parent_node_id != "0":
                return _fail("root node must use parent_node_id=0")
            if root_record.workspace_root != str(workspace_root.resolve()):
                return _fail("root node must persist workspace_root for future relaunch/recovery")
            if root_record.final_effects_file != str(final_effects_file.resolve()):
                return _fail("root node must persist final_effects_file for future relaunch/recovery")
            if root_record.durable_commit != base_commit:
                return _fail("root node must persist the current workspace HEAD as durable_commit")
            if len(root_record.current_components) != 1:
                return _fail("root node must start with one active implementer actor")
            if root_record.current_components[0].actor_kind is not ActorKind.IMPLEMENTER:
                return _fail("root node current_components must start as implementer")
            root_component = root_record.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
            if root_component is None:
                return _fail("root node must materialize an implementer component state")
            if root_component.status is not ComponentStatus.RUNNING:
                return _fail("root implementer component must start in RUNNING state")
            if int(root_component.attempt_count) != 1:
                return _fail("root implementer component must start with attempt_count=1")
            if int(root_component.pid or 0) != 54321:
                return _fail("root implementer component must preserve the launched pid")
            if root_component.process_birth_time != 1712275200.125:
                return _fail("root implementer component must preserve process_birth_time")
            if root_component.session_ids != ["root-session-001"]:
                return _fail("root implementer component must preserve ordered session_ids")
        finally:
            core.stop()
        if fake_proc.stopped != 1:
            return _fail("core.stop must also stop the owned proc supervisor")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        workspace_root = tmp / "workspace" / "demo_project"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects_file = workspace_root / "FINAL_EFFECTS.md"
        kernel_rollout_file = tmp / "kernel-rollout.jsonl"
        final_effects_file.write_text("Finish the product correctly.\n", encoding="utf-8")
        kernel_rollout_file.write_text(
            '{"type":"session_meta","payload":{"id":"kernel-session-001","timestamp":"2026-04-03T00:30:00Z"}}\n',
            encoding="utf-8",
        )

        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        raising_proc = _RaisingProcSupervisor()
        failing_process = _FakeProcess(pid=65432)

        def _failing_commit_launcher(_request: ActorLaunchSpec) -> ActorLaunchResult:
            return ActorLaunchResult(
                process=failing_process,
                process_birth_time=1712275300.25,
                session_id="root-session-002",
                rollout_path=(tmp / "runtime" / "root-rollout-002.jsonl").resolve(),
            )

        core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "router" / "wakeup.sock",
            proc_supervisor=raising_proc,
            actor_launcher=_failing_commit_launcher,
        )
        try:
            core.start_router(
                kernel_session_id="kernel-session-001",
                kernel_rollout_path=str(kernel_rollout_file.resolve()),
                kernel_started_at="2026-04-03T00:30:00Z",
                final_effects_file=final_effects_file,
            )
        except RuntimeError as exc:
            if str(exc) != "register failed":
                return _fail("start_router failure path must preserve the original register_actor error")
        else:
            return _fail("start_router must surface register_actor failures")
        if failing_process.terminate_calls != 1:
            return _fail("start_router must terminate a launched actor if later commit/register work fails")
        if store.load_node("1") is not None:
            return _fail("start_router must roll back a persisted node when later actor registration fails")
        if raising_proc.stopped != 1:
            return _fail("start_router failure path must stop the proc supervisor")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        workspace_root = tmp / "workspace" / "demo_project"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects_file = workspace_root / "FINAL_EFFECTS.md"
        kernel_rollout_file = tmp / "kernel-rollout.jsonl"
        final_effects_file.write_text("Finish the product correctly.\n", encoding="utf-8")
        kernel_rollout_file.write_text(
            '{"type":"session_meta","payload":{"id":"kernel-session-002","timestamp":"2026-04-03T00:35:00Z"}}\n',
            encoding="utf-8",
        )

        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        store.write_kernel_bootstrap_info(
            kernel_session_id="previous-kernel-session",
            kernel_rollout_path="/tmp/previous-rollout.jsonl",
            kernel_started_at="2026-04-03T00:00:00Z",
        )
        failing_start_proc = _FakeProcSupervisor()
        failing_start_process = _FakeProcess(pid=76543)

        def _failing_start_launcher(_request: ActorLaunchSpec) -> ActorLaunchResult:
            return ActorLaunchResult(
                process=failing_start_process,
                process_birth_time=1712275400.5,
                session_id="root-session-003",
                rollout_path=(tmp / "runtime" / "root-rollout-003.jsonl").resolve(),
            )

        class _StartFailingCore(_StartFailingRouterCore, RouterCore):
            pass

        core = _StartFailingCore(
            store=store,
            wakeup_socket_path=tmp / "router" / "wakeup.sock",
            proc_supervisor=failing_start_proc,
            actor_launcher=_failing_start_launcher,
        )
        try:
            core.start_router(
                kernel_session_id="kernel-session-002",
                kernel_rollout_path=str(kernel_rollout_file.resolve()),
                kernel_started_at="2026-04-03T00:35:00Z",
                final_effects_file=final_effects_file,
            )
        except RuntimeError as exc:
            if str(exc) != "steady state failed":
                return _fail("start_router must preserve self.start() failures")
        else:
            return _fail("start_router must surface self.start() failures")
        if failing_start_process.terminate_calls != 1:
            return _fail("start_router must terminate a launched actor if steady-state startup later fails")
        if store.load_node("1") is not None:
            return _fail("start_router must delete the root node if steady-state startup later fails")
        if store.read_kernel_session_id() != "previous-kernel-session":
            return _fail("start_router must restore previous kernel_session_id on steady-state startup failure")
        if store.read_kernel_rollout_path() != "/tmp/previous-rollout.jsonl":
            return _fail("start_router must restore previous kernel_rollout_path on steady-state startup failure")
        if store.read_kernel_started_at() != "2026-04-03T00:00:00Z":
            return _fail("start_router must restore previous kernel_started_at on steady-state startup failure")
        if failing_start_proc.stopped != 1:
            return _fail("start_router steady-state failure path must stop the proc supervisor")

    print("[loop-system-router-start][PASS] start_router persists kernel bootstrap metadata, launches/registers the root implementer through the generic actor path, and rolls back failed launches")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
