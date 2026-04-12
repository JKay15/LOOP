#!/usr/bin/env python3
"""Validate router evaluator lifecycle driven by complete records + ProcessExitedObserved."""

from __future__ import annotations

import json
import hashlib
import sys
import tempfile
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-evaluator][FAIL] {msg}", file=sys.stderr)
    return 2


class _LiveProcess:
    def __init__(self, pid: int = 0) -> None:
        self.pid = int(pid)
        self.terminate_calls = 0

    def poll(self):
        return None

    def terminate(self) -> None:
        self.terminate_calls += 1


class _FakeProcSupervisor:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.registered: list[dict[str, object]] = []
        self.live_actors: set[object] = set()
        self.terminated: list[object] = []

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
        self.live_actors.add(actor)
        return rollout_path

    def has_actor(self, actor) -> bool:
        return actor in self.live_actors

    def terminate_actor(self, actor) -> bool:
        self.terminated.append(actor)
        self.live_actors.discard(actor)
        return True


def _write_file(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path.resolve()


def _write_completion(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path.resolve()


def _write_checker_tasks(workspace_root: Path, task_ids: list[str]) -> Path:
    return _write_file(
        workspace_root / ".loop" / "router_runtime" / "nodes" / "node-1" / "checker" / "current" / "tasks.json",
        json.dumps(
            {
                "version": 2,
                "snapshot_id": "",
                "workspace_root": str(workspace_root.resolve()),
                "authoritative_final_effects_path": str((workspace_root / "FINAL_EFFECTS.md").resolve()),
                "final_effects_sha256": hashlib.sha256((workspace_root / "FINAL_EFFECTS.md").read_bytes()).hexdigest(),
                "obligations": [
                    {
                        "id": "req-1",
                        "section": "Main",
                        "requirement_text": (workspace_root / "FINAL_EFFECTS.md").read_text(encoding="utf-8").strip(),
                    }
                ],
                "tasks": [
                    {
                        "task_id": task_id,
                        "covered_requirement_ids": ["req-1"],
                        "goal": f"Validate {task_id}.",
                        "blocking_condition": f"Block if {task_id} fails.",
                        "task_instructions": f"Inspect the workspace and validate {task_id}.",
                        "required_evidence": [],
                    }
                    for task_id in task_ids
                ],
            },
            indent=2,
            sort_keys=True,
        ),
    )


def _wait_until(predicate, *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


def _upsert_running_node(
    *,
    store,
    node_id: str,
    parent_node_id: str,
    attempt_count: int,
    durable_commit: str,
    workspace_root: Path,
    final_effects_file: Path,
    actor_kind,
    pid: int,
    process_birth_time: float,
    session_ids: list[str],
    workspace_fingerprint_before: str,
    task_id: str | None = None,
):
    from loop.events import ActorRef
    from loop.node_table import (
        ComponentRuntimeState,
        ComponentStatus,
        NodeRuntimeRecord,
        component_key,
    )

    store.upsert_node(
        NodeRuntimeRecord(
            node_id=node_id,
            parent_node_id=parent_node_id,
            child_node_ids=[],
            workspace_root=str(workspace_root.resolve()),
            final_effects_file=str(final_effects_file.resolve()),
            split_request=0,
            split_approved=0,
            evaluator_phase="",
            checker_tasks_ref="",
            task_result_refs={},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            pending_prelude_lines=[],
            current_components=[
                ActorRef(
                    node_id=node_id,
                    actor_kind=actor_kind,
                    attempt_count=int(attempt_count),
                    task_id=str(task_id or "").strip() or None,
                )
            ],
            durable_commit=durable_commit,
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint="",
            components={
                component_key(
                    actor_kind=actor_kind,
                    task_id=str(task_id or "").strip() or None,
                ): ComponentRuntimeState(
                    status=ComponentStatus.RUNNING,
                    attempt_count=int(attempt_count),
                    task_id=str(task_id or "").strip(),
                    pid=int(pid),
                    process_birth_time=float(process_birth_time),
                    session_ids=list(session_ids),
                    workspace_fingerprint_before=str(workspace_fingerprint_before),
                    saw_output_in_attempt=False,
                    consecutive_no_progress=0,
                    consecutive_failed_exits=0,
                )
            },
        )
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.completion import actor_completion_record_path
        from loop.evaluator_runtime import (
            ai_user_current_result_path,
            checker_current_tasks_path,
            reviewer_current_feedback_path,
            reviewer_current_report_path,
            tester_current_result_path,
        )
        from loop.core import (
            ActorLaunchResult,
            RouterCore,
            notify_router_wakeup,
            router_wakeup_socket_path,
        )
        from loop.events import ActorKind, ActorRef, OutputWindow, ProcessExitedObserved
        from loop.node_table import component_key, ComponentRuntimeState, ComponentStatus
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router evaluator imports failed: {exc}")

    def _current_snapshot_id(store, node_id: str) -> str:
        record = store.load_node(node_id)
        if record is None:
            raise AssertionError(f"node {node_id} missing")
        checker_tasks_ref = Path(str(record.checker_tasks_ref or "")).resolve()
        payload = json.loads(checker_tasks_ref.read_text(encoding="utf-8"))
        snapshot_id = str(payload.get("snapshot_id") or "").strip()
        if not snapshot_id:
            raise AssertionError(f"node {node_id} missing snapshot_id")
        return snapshot_id

    now = datetime.now(timezone.utc)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()

        workspace_root = (tmp / "workspace" / "node-1").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Implement the bounded evaluator flow.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc123",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=11111,
            process_birth_time=1711965600.0,
            session_ids=["session-1"],
            workspace_fingerprint_before="fp-before-implementer",
        )
        fake_proc.live_actors.add(
            ActorRef(node_id="1", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )

        actor_launch_specs: list[object] = []

        def _actor_launcher(spec) -> ActorLaunchResult:
            actor_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=30000 + len(actor_launch_specs)),
                process_birth_time=1713000000.0 + len(actor_launch_specs),
                session_id=f"session-{len(actor_launch_specs)}",
                rollout_path=(tmp / "runtime" / f"{spec.node_id}-{spec.actor_kind.value}-{spec.attempt_count}.jsonl").resolve(),
            )

        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_actor_launcher,
        )
        core.start()
        try:
            _write_file(workspace_root / "implementation" / "deliverable.md", "impl output\n")
            _write_completion(
                actor_completion_record_path(workspace_root, ActorKind.IMPLEMENTER),
                {
                    "version": 1,
                    "node_id": "1",
                    "actor_kind": ActorKind.IMPLEMENTER.value,
                    "attempt_count": 1,
                    "pid": 11111,
                    "process_birth_time": 1711965600.0,
                    "completed_at": now.isoformat(),
                },
            )
            store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(node_id="1", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1),
                    pid=11111,
                    process_birth_time=1711965600.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: len(actor_launch_specs) >= 1
                and actor_launch_specs[0].actor_kind is ActorKind.EVALUATOR_CHECKER,
                timeout_seconds=2.0,
            ):
                return _fail("implementer completion + exit must launch evaluator_checker")
            checker_running_record = store.load_node("1")
            if checker_running_record is None or len(checker_running_record.current_components) != 1:
                return _fail("checker launch must materialize one current_components entry")
            if checker_running_record.current_components[0].actor_kind is not ActorKind.EVALUATOR_CHECKER:
                return _fail("current_components must track the launched checker actor")

            checker_spec = actor_launch_specs[0]
            checker_record = store.load_node("1")
            if checker_record is None:
                return _fail("node record must still exist after implementer completion")
            checker_component = checker_record.components[component_key(actor_kind=ActorKind.EVALUATOR_CHECKER)]
            checker_tasks = _write_file(
                checker_current_tasks_path(workspace_root, "1"),
                json.dumps(
                    {
                        "version": 2,
                        "snapshot_id": "",
                        "workspace_root": str(workspace_root.resolve()),
                        "authoritative_final_effects_path": str(final_effects),
                        "final_effects_sha256": hashlib.sha256(final_effects.read_bytes()).hexdigest(),
                        "obligations": [
                            {
                                "id": "req-1",
                                "section": "Main",
                                "requirement_text": final_effects.read_text(encoding="utf-8").strip(),
                            }
                        ],
                        "tasks": [
                            {
                                "task_id": "task-1",
                                "covered_requirement_ids": ["req-1"],
                                "goal": "Check A.",
                                "blocking_condition": "Block if A fails.",
                                "task_instructions": "Inspect A.",
                                "required_evidence": [],
                            },
                            {
                                "task_id": "task-2",
                                "covered_requirement_ids": ["req-1"],
                                "goal": "Check B.",
                                "blocking_condition": "Block if B fails.",
                                "task_instructions": "Inspect B.",
                                "required_evidence": [],
                            },
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            _write_completion(
                actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_CHECKER, node_id="1"),
                {
                    "version": 1,
                    "node_id": "1",
                    "actor_kind": ActorKind.EVALUATOR_CHECKER.value,
                    "attempt_count": checker_spec.attempt_count,
                    "pid": checker_component.pid,
                    "process_birth_time": checker_component.process_birth_time,
                    "completed_at": now.isoformat(),
                    "tasks_ref": str(checker_tasks),
                },
            )
            store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="1",
                        actor_kind=ActorKind.EVALUATOR_CHECKER,
                        attempt_count=checker_spec.attempt_count,
                    ),
                    pid=int(checker_component.pid or 0),
                    process_birth_time=checker_component.process_birth_time,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: len(actor_launch_specs) >= 4
                and actor_launch_specs[1].actor_kind is ActorKind.EVALUATOR_TESTER
                and str(actor_launch_specs[1].env.get("LOOP_TASK_ID") or "") == "",
                timeout_seconds=2.0,
            ):
                return _fail("checker completion must launch tester and parallel AI-user task lanes")
            if actor_launch_specs[2].actor_kind is not ActorKind.EVALUATOR_AI_USER or str(
                actor_launch_specs[2].env.get("LOOP_TASK_ID") or ""
            ) != "task-1":
                return _fail("checker completion must launch ai_user(task-1) in parallel with tester")
            if actor_launch_specs[3].actor_kind is not ActorKind.EVALUATOR_AI_USER or str(
                actor_launch_specs[3].env.get("LOOP_TASK_ID") or ""
            ) != "task-2":
                return _fail("checker completion must launch ai_user(task-2) in parallel with tester")
            tasks_running_record = store.load_node("1")
            if tasks_running_record is None:
                return _fail("node record must remain loadable during evaluator tasks")
            current_component_ids = {
                (
                    actor.actor_kind.value,
                    str(actor.task_id or "").strip(),
                )
                for actor in tasks_running_record.current_components
            }
            if current_component_ids != {
                (ActorKind.EVALUATOR_TESTER.value, ""),
                (ActorKind.EVALUATOR_AI_USER.value, "task-1"),
                (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
            }:
                return _fail("tasks phase must retain tester plus all running AI-user lanes")

            current_snapshot_id = _current_snapshot_id(store, "1")

            def _complete_current_tester_review(*, result_name: str) -> int:
                record = store.load_node("1")
                if record is None:
                    raise AssertionError("node record missing")
                component = record.components.get(component_key(actor_kind=ActorKind.EVALUATOR_TESTER))
                if component is None:
                    raise AssertionError("missing component for evaluator_tester")
                result_ref = _write_file(
                    tester_current_result_path(workspace_root, "1"),
                    "\n".join(
                        [
                            "# Tester Result",
                            f"Current dispatched snapshot id: {current_snapshot_id}",
                            "Overall verdict: PASS",
                            "Evidence: current evaluator batch completed.",
                        ]
                    )
                    + "\n",
                )
                _write_completion(
                    actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_TESTER, node_id="1"),
                    {
                        "version": 1,
                        "node_id": "1",
                        "actor_kind": ActorKind.EVALUATOR_TESTER.value,
                        "attempt_count": int(component.attempt_count),
                        "pid": component.pid,
                        "process_birth_time": component.process_birth_time,
                        "completed_at": now.isoformat(),
                        "result_ref": str(result_ref),
                    },
                )
                exit_seq = store.append_event(
                    ProcessExitedObserved(
                        actor=ActorRef(
                            node_id="1",
                            actor_kind=ActorKind.EVALUATOR_TESTER,
                            attempt_count=int(component.attempt_count),
                        ),
                        pid=int(component.pid or 0),
                        process_birth_time=component.process_birth_time,
                        exit_code=0,
                        signal_name=None,
                        occurred_at=now,
                    ),
                    recorded_at=now,
                )
                notify_router_wakeup(wakeup_path)
                return int(exit_seq)

            def _complete_current_ai_user_task(*, task_id: str, result_name: str) -> int:
                record = store.load_node("1")
                if record is None:
                    raise AssertionError("node record missing")
                component = record.components.get(
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id=task_id)
                )
                if component is None:
                    raise AssertionError("missing component for evaluator_ai_user")
                result_ref = _write_file(
                    ai_user_current_result_path(workspace_root, "1", task_id=task_id),
                    "\n".join(
                        [
                            f"Task id: {task_id}",
                            f"Current dispatched snapshot id: {current_snapshot_id}",
                            "Task verdict: PASS",
                            f"Evidence: completed {result_name}.",
                            "Task scope closure: sufficient.",
                        ]
                    )
                    + "\n",
                )
                _write_completion(
                    actor_completion_record_path(
                        workspace_root,
                        ActorKind.EVALUATOR_AI_USER,
                        node_id="1",
                        task_id=task_id,
                    ),
                    {
                        "version": 1,
                        "node_id": "1",
                        "actor_kind": ActorKind.EVALUATOR_AI_USER.value,
                        "attempt_count": int(component.attempt_count),
                        "pid": component.pid,
                        "process_birth_time": component.process_birth_time,
                        "completed_at": now.isoformat(),
                        "task_id": task_id,
                        "result_ref": str(result_ref),
                    },
                )
                exit_seq = store.append_event(
                    ProcessExitedObserved(
                        actor=ActorRef(
                            node_id="1",
                            actor_kind=ActorKind.EVALUATOR_AI_USER,
                            attempt_count=int(component.attempt_count),
                            task_id=task_id,
                        ),
                        pid=int(component.pid or 0),
                        process_birth_time=component.process_birth_time,
                        exit_code=0,
                        signal_name=None,
                        occurred_at=now,
                    ),
                    recorded_at=now,
                )
                notify_router_wakeup(wakeup_path)
                return int(exit_seq)

            tester_exit_seq = _complete_current_tester_review(result_name="global-tester-review")
            if not _wait_until(
                lambda: store.read_last_applied_seq() == tester_exit_seq,
                timeout_seconds=2.0,
            ):
                return _fail("tester completion must advance the inbox before AI-user lane assertions")
            task_1_exit_seq = _complete_current_ai_user_task(task_id="task-1", result_name="task-1-ai-user")
            if not _wait_until(
                lambda: store.read_last_applied_seq() == task_1_exit_seq,
                timeout_seconds=2.0,
            ):
                return _fail("ai_user(task-1) completion must advance the inbox")
            record_after_task_1 = store.load_node("1")
            if record_after_task_1 is None:
                return _fail("node record missing after ai_user(task-1) completion")
            remaining_components = {
                (
                    actor.actor_kind.value,
                    str(actor.task_id or "").strip(),
                )
                for actor in record_after_task_1.current_components
            }
            if remaining_components != {
                (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
            }:
                return _fail("after tester + ai_user(task-1) complete, only ai_user(task-2) should remain running")
            if len(actor_launch_specs) != 4:
                return _fail("completing one AI-user lane must not relaunch reviewer while another AI-user lane is still running")

            task_2_exit_seq = _complete_current_ai_user_task(task_id="task-2", result_name="task-2-ai-user")
            if not _wait_until(
                lambda: store.read_last_applied_seq() == task_2_exit_seq and len(actor_launch_specs) >= 5
                and actor_launch_specs[4].actor_kind is ActorKind.EVALUATOR_REVIEWER,
                timeout_seconds=2.0,
            ):
                return _fail("reviewer must launch only after tester and all AI-user lanes complete")

            reviewer_record = store.load_node("1")
            if reviewer_record is None:
                return _fail("node record missing before reviewer completion")
            reviewer_component = reviewer_record.components[component_key(actor_kind=ActorKind.EVALUATOR_REVIEWER)]
            reviewer_report = _write_file(
                reviewer_current_report_path(workspace_root, "1"),
                "\n".join(
                    [
                        "# Reviewer Report",
                        f"Current dispatched snapshot id: {current_snapshot_id}",
                        "Reroute required.",
                    ]
                )
                + "\n",
            )
            reviewer_feedback = _write_file(
                reviewer_current_feedback_path(workspace_root, "1"),
                json.dumps(
                    {
                        "version": 3,
                        "snapshot_id": current_snapshot_id,
                        "reroute_mask": 2,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
            )
            _write_completion(
                actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_REVIEWER, node_id="1"),
                {
                    "version": 1,
                    "node_id": "1",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": int(reviewer_component.attempt_count),
                    "pid": reviewer_component.pid,
                    "process_birth_time": reviewer_component.process_birth_time,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "REROUTE_REQUIRED",
                    "report_ref": str(reviewer_report),
                    "feedback_ref": str(reviewer_feedback),
                    "feedback_submission": {
                        "version": 3,
                        "snapshot_id": current_snapshot_id,
                        "reroute_mask": 2,
                    },
                },
            )
            store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="1",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=int(reviewer_component.attempt_count),
                    ),
                    pid=int(reviewer_component.pid or 0),
                    process_birth_time=reviewer_component.process_birth_time,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: len(actor_launch_specs) >= 6
                and actor_launch_specs[5].actor_kind is ActorKind.IMPLEMENTER
                and int(actor_launch_specs[5].attempt_count) == 2,
                timeout_seconds=2.0,
            ):
                return _fail("reviewer completion must relaunch implementer when verdict requires action")
            node = store.load_node("1")
            if node is None or len(node.current_components) != 1:
                return _fail("node should return to one implementer actor after reviewer requests action")
            if node.current_components[0].actor_kind is not ActorKind.IMPLEMENTER:
                return _fail("node should return to implementer after reviewer requests action")
            registered_kinds = [entry["actor"].actor_kind for entry in fake_proc.registered]
            expected_kinds = [
                ActorKind.EVALUATOR_CHECKER,
                ActorKind.EVALUATOR_TESTER,
                ActorKind.EVALUATOR_AI_USER,
                ActorKind.EVALUATOR_AI_USER,
                ActorKind.EVALUATOR_REVIEWER,
                ActorKind.IMPLEMENTER,
            ]
            if registered_kinds != expected_kinds:
                return _fail(f"all evaluator actors must use unified proc registration: {registered_kinds!r}")
            ai_user_registered_task_ids = [
                str(entry["actor"].task_id or "")
                for entry in fake_proc.registered
                if entry["actor"].actor_kind is ActorKind.EVALUATOR_AI_USER
            ]
            if ai_user_registered_task_ids != ["task-1", "task-2"]:
                return _fail(
                    "AI-user proc registration must preserve task_id identity for each launched evaluator task"
                )
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-1b").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Tasks must wait for implementer and checker.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="1b",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc1b",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=11121,
            process_birth_time=1711966600.0,
            session_ids=["session-1b-implementer"],
            workspace_fingerprint_before="fp-before-1b",
        )
        base_record = store.load_node("1b")
        if base_record is None:
            return _fail("parallel barrier node must be loadable")
        store.upsert_node(
            replace(
                base_record,
                evaluator_phase="checker",
                current_components=[
                    ActorRef(node_id="1b", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1),
                    ActorRef(node_id="1b", actor_kind=ActorKind.EVALUATOR_CHECKER, attempt_count=1),
                ],
                components={
                    **base_record.components,
                    component_key(actor_kind=ActorKind.EVALUATOR_CHECKER): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        pid=11122,
                        process_birth_time=1711966601.0,
                        session_ids=["session-1b-checker"],
                        workspace_fingerprint_before="fp-before-checker-1b",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        barrier_specs: list[object] = []
        barrier_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "parallel-barrier.sock",
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=lambda spec: (
                barrier_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=30100 + len(barrier_specs)),
                    process_birth_time=1713010000.0 + len(barrier_specs),
                    session_id=f"parallel-barrier-{len(barrier_specs)}",
                    rollout_path=(tmp / "runtime" / f"parallel-barrier-{len(barrier_specs)}.jsonl").resolve(),
                )
            ),
        )
        after_checker = barrier_core._advance_after_valid_actor_completion(
            record=store.load_node("1b"),
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            task_id=None,
            completion_payload={"tasks_ref": str(checker_tasks_ref)},
        )
        if barrier_specs:
            return _fail("checker completion must not start tasks before implementer completes")
        if after_checker.current_components != [ActorRef(node_id="1b", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)]:
            return _fail("checker completion must leave the running implementer in place while waiting on the barrier")
        if str(after_checker.evaluator_phase or "").strip() != "checker":
            return _fail("checker completion must remain in checker phase until implementer completes")
        barrier_core._advance_after_valid_actor_completion(
            record=store.load_node("1b"),
            actor_kind=ActorKind.IMPLEMENTER,
            task_id=None,
            completion_payload={},
        )
        launched_kinds = [spec.actor_kind for spec in barrier_specs]
        if launched_kinds != [
            ActorKind.EVALUATOR_TESTER,
            ActorKind.EVALUATOR_AI_USER,
            ActorKind.EVALUATOR_AI_USER,
        ]:
            return _fail("tasks must launch only after both checker and implementer are complete")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-2").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Parallel evaluator batching.\n",
        )
        tasks_payload = {
            "version": 1,
            "tasks": [
                {
                    "task_id": f"task-{idx:02d}",
                    "task_ref": str(
                        _write_file(
                            workspace_root / "checker" / f"task-{idx:02d}.md",
                            f"Task {idx}\n",
                        )
                    ),
                }
                for idx in range(1, 13)
            ],
        }
        checker_tasks_ref = _write_file(
            workspace_root / "checker" / "tasks.json",
            json.dumps(tasks_payload, indent=2, sort_keys=True),
        )
        _upsert_running_node(
            store=store,
            node_id="2",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="def456",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            pid=22222,
            process_birth_time=1712965600.0,
            session_ids=["session-2"],
            workspace_fingerprint_before="fp-before-checker",
        )
        record = store.load_node("2")
        if record is None:
            return _fail("parallel evaluator node must be loadable")
        store.upsert_node(
            record.__class__(
                **{
                    **record.__dict__,
                    "current_components": [],
                    "components": {},
                    "evaluator_phase": "tasks",
                    "checker_tasks_ref": str(checker_tasks_ref),
                }
            )
        )
        cap_launch_specs: list[object] = []
        cap_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "cap.sock",
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=lambda spec: (
                cap_launch_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=50000 + len(cap_launch_specs)),
                    process_birth_time=1715000000.0 + len(cap_launch_specs),
                    session_id=f"cap-{len(cap_launch_specs)}",
                    rollout_path=(tmp / "runtime" / f"cap-{len(cap_launch_specs)}.jsonl").resolve(),
                )
            ),
        )
        pending_record = store.load_node("2")
        if pending_record is None:
            return _fail("parallel evaluator node must remain loadable before dispatch")
        launched_record = cap_core._continue_tasks_phase(pending_record)
        launched_task_ids = [
            str(spec.env.get("LOOP_TASK_ID") or "")
            for spec in cap_launch_specs
            if spec.actor_kind is ActorKind.EVALUATOR_AI_USER
        ]
        if len(cap_launch_specs) != 11:
            return _fail("tasks phase must launch one tester plus at most ten AI-user lanes in the first batch")
        if [spec.actor_kind for spec in cap_launch_specs[:1]] != [ActorKind.EVALUATOR_TESTER]:
            return _fail("tasks phase must launch tester first")
        if launched_task_ids != [f"task-{idx:02d}" for idx in range(1, 11)]:
            return _fail("first evaluator batch must launch exactly the first ten pending AI-user task ids")
        launched_component_ids = {
            (
                actor.actor_kind.value,
                str(actor.task_id or "").strip(),
            )
            for actor in launched_record.current_components
        }
        if len(launched_component_ids) != 11:
            return _fail("first evaluator batch must retain tester plus ten running AI-user lanes")
        if (ActorKind.EVALUATOR_AI_USER.value, "task-11") in launched_component_ids:
            return _fail("eleventh AI-user lane must not launch until a slot frees up")
        advanced_record = cap_core._mark_component_completed(
            launched_record,
            ActorKind.EVALUATOR_AI_USER,
            task_id="task-01",
        )
        advanced_record = replace(
            advanced_record,
            task_result_refs={
                **advanced_record.task_result_refs,
                "task-01": {ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-01-result.md"},
            },
        )
        store.upsert_node(advanced_record)
        second_batch_record = cap_core._continue_tasks_phase(advanced_record)
        if len(cap_launch_specs) != 12:
            return _fail("freeing one AI-user slot must launch the next pending task")
        if cap_launch_specs[11].actor_kind is not ActorKind.EVALUATOR_AI_USER or str(
            cap_launch_specs[11].env.get("LOOP_TASK_ID") or ""
        ) != "task-11":
            return _fail("the next pending AI-user task must be launched when a batch slot frees up")
        second_batch_ids = {
            str(actor.task_id or "").strip()
            for actor in second_batch_record.current_components
            if actor.actor_kind is ActorKind.EVALUATOR_AI_USER
        }
        if len(second_batch_ids) != 10 or "task-11" not in second_batch_ids or "task-01" in second_batch_ids:
            return _fail("AI-user batching must keep the running lane count capped at ten while advancing the next pending task")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-5").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Reviewer must wait for all batched evaluator work.\n",
        )
        checker_tasks_ref = _write_file(
            workspace_root / "checker" / "tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [
                        {
                            "task_id": f"task-{idx:02d}",
                            "task_ref": str(
                                _write_file(
                                    workspace_root / "checker" / f"task-{idx:02d}.md",
                                    f"Task {idx}\n",
                                )
                            ),
                        }
                        for idx in range(1, 13)
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _upsert_running_node(
            store=store,
            node_id="5",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="mno345",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            pid=55501,
            process_birth_time=1715965601.0,
            session_ids=["checker-session-5"],
            workspace_fingerprint_before="fp-before-checker-5",
        )
        base_record = store.load_node("5")
        if base_record is None:
            return _fail("reviewer gating node must be loadable")
        store.upsert_node(
            replace(
                base_record,
                current_components=[],
                components={},
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
            )
        )
        reviewer_gate_specs: list[object] = []
        reviewer_gate_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "reviewer-gate.sock",
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=lambda spec: (
                reviewer_gate_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=56000 + len(reviewer_gate_specs)),
                    process_birth_time=1715600000.0 + len(reviewer_gate_specs),
                    session_id=f"reviewer-gate-{len(reviewer_gate_specs)}",
                    rollout_path=(tmp / "runtime" / f"reviewer-gate-{len(reviewer_gate_specs)}.jsonl").resolve(),
                )
            ),
        )
        current_record = store.load_node("5")
        if current_record is None:
            return _fail("reviewer gating node must remain loadable before dispatch")
        current_record = reviewer_gate_core._continue_tasks_phase(current_record)
        if len(reviewer_gate_specs) != 11:
            return _fail("reviewer gating setup must launch tester plus the first ten AI-user tasks")
        current_record = reviewer_gate_core._advance_after_valid_actor_completion(
            record=store.load_node("5"),
            actor_kind=ActorKind.EVALUATOR_TESTER,
            task_id=None,
            completion_payload={"result_ref": "/tmp/reviewer-gate-tester.md"},
        )
        if any(spec.actor_kind is ActorKind.EVALUATOR_REVIEWER for spec in reviewer_gate_specs):
            return _fail("reviewer must not launch immediately after tester when AI-user tasks remain running")
        for idx in range(1, 11):
            current_record = reviewer_gate_core._advance_after_valid_actor_completion(
                record=store.load_node("5"),
                actor_kind=ActorKind.EVALUATOR_AI_USER,
                task_id=f"task-{idx:02d}",
                completion_payload={
                    "task_id": f"task-{idx:02d}",
                    "result_ref": f"/tmp/reviewer-gate-task-{idx:02d}.md",
                },
            )
        if any(spec.actor_kind is ActorKind.EVALUATOR_REVIEWER for spec in reviewer_gate_specs):
            return _fail("reviewer must not launch after the first ten AI-user tasks while the second batch is still pending/running")
        remaining_ai_user_ids = {
            str(actor.task_id or "").strip()
            for actor in current_record.current_components
            if actor.actor_kind is ActorKind.EVALUATOR_AI_USER
        }
        if remaining_ai_user_ids != {"task-11", "task-12"}:
            return _fail("after the first batch completes, only the remaining second-batch AI-user tasks should be running")
        current_record = reviewer_gate_core._advance_after_valid_actor_completion(
            record=store.load_node("5"),
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            task_id="task-11",
            completion_payload={
                "task_id": "task-11",
                "result_ref": "/tmp/reviewer-gate-task-11.md",
            },
        )
        if any(spec.actor_kind is ActorKind.EVALUATOR_REVIEWER for spec in reviewer_gate_specs):
            return _fail("reviewer must not launch while one second-batch AI-user task is still running")
        current_record = reviewer_gate_core._advance_after_valid_actor_completion(
            record=store.load_node("5"),
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            task_id="task-12",
            completion_payload={
                "task_id": "task-12",
                "result_ref": "/tmp/reviewer-gate-task-12.md",
            },
        )
        if not reviewer_gate_specs or reviewer_gate_specs[-1].actor_kind is not ActorKind.EVALUATOR_REVIEWER:
            return _fail("reviewer must launch after tester and all batched AI-user tasks complete")
        if len(current_record.current_components) != 1 or current_record.current_components[0].actor_kind is not ActorKind.EVALUATOR_REVIEWER:
            return _fail("final batched evaluator completion must materialize one running reviewer actor")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-3").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "AI-user failure isolation.\n",
        )
        checker_tasks_ref = _write_file(
            workspace_root / "checker" / "tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [
                        {
                            "task_id": "task-1",
                            "task_ref": str(_write_file(workspace_root / "checker" / "task-1.md", "Task 1\n")),
                        },
                        {
                            "task_id": "task-2",
                            "task_ref": str(_write_file(workspace_root / "checker" / "task-2.md", "Task 2\n")),
                        },
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _upsert_running_node(
            store=store,
            node_id="3",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="ghi789",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=33301,
            process_birth_time=1713965601.0,
            session_ids=["tester-session-3"],
            workspace_fingerprint_before="fp-before-tester-3",
        )
        tester_record = store.load_node("3")
        if tester_record is None:
            return _fail("AI-user isolation node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="3", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="3", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                    ActorRef(node_id="3", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): tester_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-1",
                        pid=33311,
                        process_birth_time=1713965611.0,
                        session_ids=["ai-session-3-1"],
                        workspace_fingerprint_before="fp-before-ai-3-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-2",
                        pid=33312,
                        process_birth_time=1713965612.0,
                        session_ids=["ai-session-3-2"],
                        workspace_fingerprint_before="fp-before-ai-3-2",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        ai_relaunch_specs: list[object] = []

        def _ai_relaunch_launcher(spec) -> ActorLaunchResult:
            ai_relaunch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=53000 + len(ai_relaunch_specs)),
                process_birth_time=1715300000.0 + len(ai_relaunch_specs),
                session_id=f"ai-relaunch-{len(ai_relaunch_specs)}",
                rollout_path=(tmp / "runtime" / f"ai-relaunch-{len(ai_relaunch_specs)}.jsonl").resolve(),
            )

        ai_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_ai_relaunch_launcher,
        )
        fake_proc.live_actors.update(
            {
                ActorRef(node_id="3", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                ActorRef(node_id="3", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                ActorRef(node_id="3", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
            }
        )
        ai_core.start()
        try:
            failed_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="3",
                        actor_kind=ActorKind.EVALUATOR_AI_USER,
                        attempt_count=1,
                        task_id="task-1",
                    ),
                    pid=33311,
                    process_birth_time=1713965611.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == failed_exit_seq and len(ai_relaunch_specs) == 1,
                timeout_seconds=2.0,
            ):
                return _fail("failed ai_user(task-1) exit must relaunch exactly one lane")
            if ai_relaunch_specs[0].actor_kind is not ActorKind.EVALUATOR_AI_USER or str(
                ai_relaunch_specs[0].env.get("LOOP_TASK_ID") or ""
            ) != "task-1":
                return _fail("failed ai_user(task-1) exit must relaunch the same task_id only")
            recovered_record = store.load_node("3")
            if recovered_record is None:
                return _fail("AI-user isolation node must remain loadable after relaunch")
            recovered_components = {
                (actor.actor_kind.value, str(actor.task_id or "").strip())
                for actor in recovered_record.current_components
            }
            if recovered_components != {
                (ActorKind.EVALUATOR_TESTER.value, ""),
                (ActorKind.EVALUATOR_AI_USER.value, "task-1"),
                (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
            }:
                return _fail("AI-user relaunch must preserve tester and sibling AI-user lanes")
            if recovered_record.components[component_key(actor_kind=ActorKind.EVALUATOR_TESTER)].attempt_count != 1:
                return _fail("AI-user relaunch must not restart tester")
            if recovered_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1")
            ].attempt_count != 2:
                return _fail("failed ai_user(task-1) lane must bump only its own attempt_count")
            if recovered_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2")
            ].attempt_count != 1:
                return _fail("failed ai_user(task-1) lane must not restart sibling AI-user lanes")
        finally:
            ai_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-4").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Tester failure isolation.\n",
        )
        checker_tasks_ref = _write_file(
            workspace_root / "checker" / "tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [
                        {
                            "task_id": "task-1",
                            "task_ref": str(_write_file(workspace_root / "checker" / "task-1.md", "Task 1\n")),
                        },
                        {
                            "task_id": "task-2",
                            "task_ref": str(_write_file(workspace_root / "checker" / "task-2.md", "Task 2\n")),
                        },
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _upsert_running_node(
            store=store,
            node_id="4",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="jkl012",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=44401,
            process_birth_time=1714965601.0,
            session_ids=["tester-session-4"],
            workspace_fingerprint_before="fp-before-tester-4",
        )
        tester_record = store.load_node("4")
        if tester_record is None:
            return _fail("tester isolation node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="4", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="4", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                    ActorRef(node_id="4", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): tester_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-1",
                        pid=44411,
                        process_birth_time=1714965611.0,
                        session_ids=["ai-session-4-1"],
                        workspace_fingerprint_before="fp-before-ai-4-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-2",
                        pid=44412,
                        process_birth_time=1714965612.0,
                        session_ids=["ai-session-4-2"],
                        workspace_fingerprint_before="fp-before-ai-4-2",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        tester_relaunch_specs: list[object] = []

        def _tester_relaunch_launcher(spec) -> ActorLaunchResult:
            tester_relaunch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=54000 + len(tester_relaunch_specs)),
                process_birth_time=1715400000.0 + len(tester_relaunch_specs),
                session_id=f"tester-relaunch-{len(tester_relaunch_specs)}",
                rollout_path=(tmp / "runtime" / f"tester-relaunch-{len(tester_relaunch_specs)}.jsonl").resolve(),
            )

        tester_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_tester_relaunch_launcher,
        )
        fake_proc.live_actors.update(
            {
                ActorRef(node_id="4", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                ActorRef(node_id="4", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                ActorRef(node_id="4", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
            }
        )
        tester_core.start()
        try:
            failed_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="4",
                        actor_kind=ActorKind.EVALUATOR_TESTER,
                        attempt_count=1,
                    ),
                    pid=44401,
                    process_birth_time=1714965601.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == failed_exit_seq and len(tester_relaunch_specs) == 1,
                timeout_seconds=2.0,
            ):
                return _fail("failed tester exit must relaunch exactly one tester lane")
            if tester_relaunch_specs[0].actor_kind is not ActorKind.EVALUATOR_TESTER or str(
                tester_relaunch_specs[0].env.get("LOOP_TASK_ID") or ""
            ) != "":
                return _fail("failed tester exit must relaunch tester only")
            recovered_record = store.load_node("4")
            if recovered_record is None:
                return _fail("tester isolation node must remain loadable after relaunch")
            recovered_components = {
                (actor.actor_kind.value, str(actor.task_id or "").strip())
                for actor in recovered_record.current_components
            }
            if recovered_components != {
                (ActorKind.EVALUATOR_TESTER.value, ""),
                (ActorKind.EVALUATOR_AI_USER.value, "task-1"),
                (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
            }:
                return _fail("tester relaunch must preserve all running AI-user lanes")
            if recovered_record.components[component_key(actor_kind=ActorKind.EVALUATOR_TESTER)].attempt_count != 2:
                return _fail("failed tester lane must bump only tester attempt_count")
            if recovered_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1")
            ].attempt_count != 1 or recovered_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2")
            ].attempt_count != 1:
                return _fail("failed tester lane must not restart any AI-user lanes")
        finally:
            tester_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-6").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Terminal failure before reviewer.\n",
        )
        checker_tasks_ref = _write_file(
            workspace_root / "checker" / "tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [
                        {
                            "task_id": "task-1",
                            "task_ref": str(_write_file(workspace_root / "checker" / "task-1.md", "Task 1\n")),
                        }
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _upsert_running_node(
            store=store,
            node_id="6",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="pqr678",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=66601,
            process_birth_time=1716965601.0,
            session_ids=["tester-session-6"],
            workspace_fingerprint_before="fp-before-tester-6",
        )
        tester_record = store.load_node("6")
        if tester_record is None:
            return _fail("terminal-failure node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="6", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="6", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=3, task_id="task-1"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): tester_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=3,
                        task_id="task-1",
                        pid=66611,
                        process_birth_time=1716965611.0,
                        session_ids=["ai-session-6-1"],
                        workspace_fingerprint_before="fp-before-ai-6-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=2,
                    ),
                },
            )
        )
        terminal_specs: list[object] = []

        def _terminal_launcher(spec) -> ActorLaunchResult:
            terminal_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=57000 + len(terminal_specs)),
                process_birth_time=1715700000.0 + len(terminal_specs),
                session_id=f"terminal-{len(terminal_specs)}",
                rollout_path=(tmp / "runtime" / f"terminal-{len(terminal_specs)}.jsonl").resolve(),
            )

        terminal_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_terminal_launcher,
        )
        fake_proc.live_actors.update(
            {
                ActorRef(node_id="6", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                ActorRef(node_id="6", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=3, task_id="task-1"),
            }
        )
        terminal_core.start()
        try:
            failed_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="6",
                        actor_kind=ActorKind.EVALUATOR_AI_USER,
                        attempt_count=3,
                        task_id="task-1",
                    ),
                    pid=66611,
                    process_birth_time=1716965611.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == failed_exit_seq,
                timeout_seconds=2.0,
            ):
                return _fail("terminal ai_user failure must still advance the inbox")
            if terminal_specs:
                return _fail("terminal ai_user failure before reviewer must not launch any new evaluator actor")
            terminal_record = store.load_node("6")
            if terminal_record is None:
                return _fail("terminal-failure node must remain loadable")
            failed_component = terminal_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1")
            ]
            if failed_component.status is not ComponentStatus.TERMINAL_FAILED:
                return _fail("third failed ai_user exit must mark that lane terminal_failed")
            if any(actor.actor_kind is ActorKind.EVALUATOR_REVIEWER for actor in terminal_record.current_components):
                return _fail("terminal ai_user failure before reviewer must not sneak into reviewer")
            if str(store.read_router_status() or "").strip() != "terminal_failed":
                return _fail("terminal ai_user failure on the only unfinished frontier must mark the router terminal_failed")
        finally:
            terminal_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-7").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Terminal tester failure before reviewer.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1"])
        _upsert_running_node(
            store=store,
            node_id="7",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="stu901",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=77701,
            process_birth_time=1717965601.0,
            session_ids=["tester-session-7"],
            workspace_fingerprint_before="fp-before-tester-7",
        )
        tester_record = store.load_node("7")
        if tester_record is None:
            return _fail("terminal tester failure node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="7", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=3),
                    ActorRef(node_id="7", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=3,
                        task_id="",
                        pid=77701,
                        process_birth_time=1717965601.0,
                        session_ids=["tester-session-7"],
                        workspace_fingerprint_before="fp-before-tester-7",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=2,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-1",
                        pid=77711,
                        process_birth_time=1717965611.0,
                        session_ids=["ai-session-7-1"],
                        workspace_fingerprint_before="fp-before-ai-7-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        tester_terminal_specs: list[object] = []

        def _tester_terminal_launcher(spec) -> ActorLaunchResult:
            tester_terminal_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=58000 + len(tester_terminal_specs)),
                process_birth_time=1715800000.0 + len(tester_terminal_specs),
                session_id=f"tester-terminal-{len(tester_terminal_specs)}",
                rollout_path=(tmp / "runtime" / f"tester-terminal-{len(tester_terminal_specs)}.jsonl").resolve(),
            )

        tester_terminal_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_tester_terminal_launcher,
        )
        fake_proc.live_actors.update(
            {
                ActorRef(node_id="7", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=3),
                ActorRef(node_id="7", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
            }
        )
        tester_terminal_core.start()
        try:
            failed_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="7",
                        actor_kind=ActorKind.EVALUATOR_TESTER,
                        attempt_count=3,
                    ),
                    pid=77701,
                    process_birth_time=1717965601.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == failed_exit_seq,
                timeout_seconds=2.0,
            ):
                return _fail("terminal tester failure must still advance the inbox")
            if tester_terminal_specs:
                return _fail("terminal tester failure before reviewer must not launch any new evaluator actor")
            terminal_record = store.load_node("7")
            if terminal_record is None:
                return _fail("terminal tester failure node must remain loadable")
            failed_component = terminal_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
            ]
            if failed_component.status is not ComponentStatus.TERMINAL_FAILED:
                return _fail("third failed tester exit must mark tester terminal_failed")
            if any(actor.actor_kind is ActorKind.EVALUATOR_REVIEWER for actor in terminal_record.current_components):
                return _fail("terminal tester failure before reviewer must not sneak into reviewer")
            if str(store.read_router_status() or "").strip() != "terminal_failed":
                return _fail("terminal tester failure on the only unfinished frontier must mark the router terminal_failed")
        finally:
            tester_terminal_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-8").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "AI-user no-progress isolation.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="8",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="vwx234",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=88801,
            process_birth_time=1718965601.0,
            session_ids=["tester-session-8"],
            workspace_fingerprint_before="fp-before-tester-8",
        )
        tester_record = store.load_node("8")
        if tester_record is None:
            return _fail("AI-user no-progress node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="8", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="8", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                    ActorRef(node_id="8", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): tester_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-1",
                        pid=88811,
                        process_birth_time=1718965611.0,
                        session_ids=["ai-session-8-1"],
                        workspace_fingerprint_before="fp-before-ai-8-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-2",
                        pid=88812,
                        process_birth_time=1718965612.0,
                        session_ids=["ai-session-8-2"],
                        workspace_fingerprint_before="fp-before-ai-8-2",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        ai_no_progress_specs: list[object] = []

        def _ai_no_progress_launcher(spec) -> ActorLaunchResult:
            ai_no_progress_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=59000 + len(ai_no_progress_specs)),
                process_birth_time=1715900000.0 + len(ai_no_progress_specs),
                session_id=f"ai-no-progress-{len(ai_no_progress_specs)}",
                rollout_path=(tmp / "runtime" / f"ai-no-progress-{len(ai_no_progress_specs)}.jsonl").resolve(),
            )

        ai_no_progress_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_ai_no_progress_launcher,
        )
        task_1_actor = ActorRef(
            node_id="8",
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            attempt_count=1,
            task_id="task-1",
        )
        fake_proc.live_actors.update(
            {
                ActorRef(node_id="8", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                task_1_actor,
                ActorRef(node_id="8", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
            }
        )
        ai_no_progress_core.start()
        try:
            last_seq = 0
            for _ in range(5):
                last_seq = store.append_event(
                    OutputWindow(
                        actor=task_1_actor,
                        had_output=False,
                        observed_at=now,
                    ),
                    recorded_at=now,
                )
                notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: task_1_actor in fake_proc.terminated,
                timeout_seconds=2.0,
            ):
                return _fail("five AI-user no-progress windows must terminate only that AI-user lane")
            if any(
                actor.actor_kind is ActorKind.EVALUATOR_TESTER or str(actor.task_id or "").strip() == "task-2"
                for actor in fake_proc.terminated
            ):
                return _fail("AI-user no-progress termination must not target tester or sibling AI-user lanes")
            if not _wait_until(lambda: store.read_last_applied_seq() == last_seq, timeout_seconds=2.0):
                return _fail("AI-user no-progress windows must still advance the inbox")
            failed_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=task_1_actor,
                    pid=88811,
                    process_birth_time=1718965611.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == failed_exit_seq and len(ai_no_progress_specs) == 1,
                timeout_seconds=2.0,
            ):
                return _fail("AI-user no-progress exit must relaunch only that task lane")
            if ai_no_progress_specs[0].actor_kind is not ActorKind.EVALUATOR_AI_USER or str(
                ai_no_progress_specs[0].env.get("LOOP_TASK_ID") or ""
            ) != "task-1":
                return _fail("AI-user no-progress recovery must relaunch the same task_id")
            recovered_record = store.load_node("8")
            if recovered_record is None:
                return _fail("AI-user no-progress node must remain loadable after relaunch")
            recovered_components = {
                (actor.actor_kind.value, str(actor.task_id or "").strip())
                for actor in recovered_record.current_components
            }
            if recovered_components != {
                (ActorKind.EVALUATOR_TESTER.value, ""),
                (ActorKind.EVALUATOR_AI_USER.value, "task-1"),
                (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
            }:
                return _fail("AI-user no-progress recovery must preserve tester and sibling AI-user lanes")
            if recovered_record.components[
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1")
            ].attempt_count != 2:
                return _fail("AI-user no-progress recovery must only bump the failed lane attempt_count")
        finally:
            ai_no_progress_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-9").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Tester no-progress isolation.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="9",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="yz0123",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=99901,
            process_birth_time=1719965601.0,
            session_ids=["tester-session-9"],
            workspace_fingerprint_before="fp-before-tester-9",
        )
        tester_record = store.load_node("9")
        if tester_record is None:
            return _fail("tester no-progress node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="9", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="9", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                    ActorRef(node_id="9", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): tester_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-1",
                        pid=99911,
                        process_birth_time=1719965611.0,
                        session_ids=["ai-session-9-1"],
                        workspace_fingerprint_before="fp-before-ai-9-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-2",
                        pid=99912,
                        process_birth_time=1719965612.0,
                        session_ids=["ai-session-9-2"],
                        workspace_fingerprint_before="fp-before-ai-9-2",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        tester_no_progress_specs: list[object] = []

        def _tester_no_progress_launcher(spec) -> ActorLaunchResult:
            tester_no_progress_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=60000 + len(tester_no_progress_specs)),
                process_birth_time=1716000000.0 + len(tester_no_progress_specs),
                session_id=f"tester-no-progress-{len(tester_no_progress_specs)}",
                rollout_path=(tmp / "runtime" / f"tester-no-progress-{len(tester_no_progress_specs)}.jsonl").resolve(),
            )

        tester_no_progress_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_tester_no_progress_launcher,
        )
        tester_actor = ActorRef(node_id="9", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1)
        fake_proc.live_actors.update(
            {
                tester_actor,
                ActorRef(node_id="9", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                ActorRef(node_id="9", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
            }
        )
        tester_no_progress_core.start()
        try:
            last_seq = 0
            for _ in range(5):
                last_seq = store.append_event(
                    OutputWindow(
                        actor=tester_actor,
                        had_output=False,
                        observed_at=now,
                    ),
                    recorded_at=now,
                )
                notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: tester_actor in fake_proc.terminated,
                timeout_seconds=2.0,
            ):
                return _fail("five tester no-progress windows must terminate the tester lane")
            if any(
                actor.actor_kind is ActorKind.EVALUATOR_AI_USER
                for actor in fake_proc.terminated
            ):
                return _fail("tester no-progress termination must not target any AI-user lanes")
            if not _wait_until(lambda: store.read_last_applied_seq() == last_seq, timeout_seconds=2.0):
                return _fail("tester no-progress windows must still advance the inbox")
            failed_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=tester_actor,
                    pid=99901,
                    process_birth_time=1719965601.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == failed_exit_seq and len(tester_no_progress_specs) == 1,
                timeout_seconds=2.0,
            ):
                return _fail("tester no-progress exit must relaunch only tester")
            if tester_no_progress_specs[0].actor_kind is not ActorKind.EVALUATOR_TESTER or str(
                tester_no_progress_specs[0].env.get("LOOP_TASK_ID") or ""
            ) != "":
                return _fail("tester no-progress recovery must relaunch tester only")
            recovered_record = store.load_node("9")
            if recovered_record is None:
                return _fail("tester no-progress node must remain loadable after relaunch")
            recovered_components = {
                (actor.actor_kind.value, str(actor.task_id or "").strip())
                for actor in recovered_record.current_components
            }
            if recovered_components != {
                (ActorKind.EVALUATOR_TESTER.value, ""),
                (ActorKind.EVALUATOR_AI_USER.value, "task-1"),
                (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
            }:
                return _fail("tester no-progress recovery must preserve all AI-user lanes")
            if recovered_record.components[component_key(actor_kind=ActorKind.EVALUATOR_TESTER)].attempt_count != 2:
                return _fail("tester no-progress recovery must only bump tester attempt_count")
        finally:
            tester_no_progress_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-10").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Startup sweep missing-lane recovery.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="10",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc1000",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=100001,
            process_birth_time=1720965601.0,
            session_ids=["tester-session-10"],
            workspace_fingerprint_before="fp-before-tester-10",
        )
        base_record = store.load_node("10")
        if base_record is None:
            return _fail("startup sweep node must be loadable")
        store.upsert_node(
            replace(
                base_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="10", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="10", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-1"),
                    ActorRef(node_id="10", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): base_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-1",
                        pid=100011,
                        process_birth_time=1720965611.0,
                        session_ids=["ai-session-10-1"],
                        workspace_fingerprint_before="fp-before-ai-10-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-2",
                        pid=100012,
                        process_birth_time=1720965612.0,
                        session_ids=["ai-session-10-2"],
                        workspace_fingerprint_before="fp-before-ai-10-2",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        startup_specs: list[object] = []
        startup_proc = _FakeProcSupervisor()
        startup_proc.live_actors.update(
            {
                ActorRef(node_id="10", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                ActorRef(node_id="10", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
            }
        )
        startup_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "startup-sweep.sock",
            proc_supervisor=startup_proc,
            actor_launcher=lambda spec: (
                startup_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=61000 + len(startup_specs)),
                    process_birth_time=1716100000.0 + len(startup_specs),
                    session_id=f"startup-{len(startup_specs)}",
                    rollout_path=(tmp / "runtime" / f"startup-{len(startup_specs)}.jsonl").resolve(),
                )
            ),
        )
        startup_core._startup_running_actor_sweep()
        if len(startup_specs) != 1:
            return _fail("startup sweep must relaunch only the missing lane")
        if startup_specs[0].actor_kind is not ActorKind.EVALUATOR_AI_USER or str(
            startup_specs[0].env.get("LOOP_TASK_ID") or ""
        ) != "task-1":
            return _fail("startup sweep must relaunch exactly the missing ai_user(task-1) lane")
        recovered_record = store.load_node("10")
        if recovered_record is None:
            return _fail("startup sweep node must remain loadable after relaunch")
        recovered_components = {
            (actor.actor_kind.value, str(actor.task_id or "").strip())
            for actor in recovered_record.current_components
        }
        if recovered_components != {
            (ActorKind.EVALUATOR_TESTER.value, ""),
            (ActorKind.EVALUATOR_AI_USER.value, "task-1"),
            (ActorKind.EVALUATOR_AI_USER.value, "task-2"),
        }:
            return _fail("startup sweep relaunch must preserve unaffected lanes while restoring the missing lane")
        if recovered_record.components[
            component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1")
        ].attempt_count != 2:
            return _fail("startup sweep recovery must bump only the recovered lane attempt_count")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-11").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Reconcile must refill evaluator AI-user slots.\n",
        )
        checker_tasks_ref = _write_checker_tasks(
            workspace_root,
            [f"task-{idx:02d}" for idx in range(1, 14)],
        )
        _upsert_running_node(
            store=store,
            node_id="11",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc1100",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=110001,
            process_birth_time=1721965601.0,
            session_ids=["tester-session-11"],
            workspace_fingerprint_before="fp-before-tester-11",
        )
        base_record = store.load_node("11")
        if base_record is None:
            return _fail("reconcile refill node must be loadable")
        running_ai_task_ids = [f"task-{idx:02d}" for idx in range(4, 11)]
        store.upsert_node(
            replace(
                base_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                task_result_refs={
                    "task-01": {ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-01-ai.md"},
                    "task-02": {ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-02-ai.md"},
                    "task-03": {ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-03-ai.md"},
                },
                current_components=[
                    ActorRef(node_id="11", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    *[
                        ActorRef(
                            node_id="11",
                            actor_kind=ActorKind.EVALUATOR_AI_USER,
                            attempt_count=1,
                            task_id=task_id,
                        )
                        for task_id in running_ai_task_ids
                    ],
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): base_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    **{
                        component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id=task_id): ComponentRuntimeState(
                            status=ComponentStatus.RUNNING,
                            attempt_count=1,
                            task_id=task_id,
                            pid=110100 + idx,
                            process_birth_time=1721965700.0 + idx,
                            session_ids=[f"ai-session-11-{task_id}"],
                            workspace_fingerprint_before=f"fp-before-{task_id}",
                            saw_output_in_attempt=False,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=0,
                        )
                        for idx, task_id in enumerate(running_ai_task_ids, start=1)
                    },
                },
            )
        )
        refill_specs: list[object] = []
        refill_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "reconcile-refill.sock",
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=lambda spec: (
                refill_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=62000 + len(refill_specs)),
                    process_birth_time=1716200000.0 + len(refill_specs),
                    session_id=f"refill-{len(refill_specs)}",
                    rollout_path=(tmp / "runtime" / f"refill-{len(refill_specs)}.jsonl").resolve(),
                )
            ),
        )
        refill_core._reconcile_pending_for_record(store.load_node("11"))
        refill_task_ids = [
            str(spec.env.get("LOOP_TASK_ID") or "")
            for spec in refill_specs
            if spec.actor_kind is ActorKind.EVALUATOR_AI_USER
        ]
        if refill_task_ids != ["task-11", "task-12", "task-13"]:
            return _fail("reconcile must refill freed AI-user slots with the next pending tasks up to the cap")
        refilled_record = store.load_node("11")
        if refilled_record is None:
            return _fail("reconcile refill node must remain loadable after refill")
        refilled_ai_ids = {
            str(actor.task_id or "").strip()
            for actor in refilled_record.current_components
            if actor.actor_kind is ActorKind.EVALUATOR_AI_USER
        }
        if refilled_ai_ids != set(running_ai_task_ids + ["task-11", "task-12", "task-13"]):
            return _fail("reconcile refill must preserve running AI-user lanes and add only the next pending tasks")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-12").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Reconcile should relaunch tester only.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="12",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc1200",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=120001,
            process_birth_time=1722965601.0,
            session_ids=["tester-session-12"],
            workspace_fingerprint_before="fp-before-tester-12",
        )
        base_record = store.load_node("12")
        if base_record is None:
            return _fail("tester-only reconcile node must be loadable")
        store.upsert_node(
            replace(
                base_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                task_result_refs={
                    "task-1": {ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-1-ai.md"},
                    "task-2": {ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-2-ai.md"},
                },
                current_components=[],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): replace(
                        base_record.components[component_key(actor_kind=ActorKind.EVALUATOR_TESTER)],
                        status=ComponentStatus.INACTIVE,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=1,
                        task_id="task-1",
                        pid=120011,
                        process_birth_time=1722965611.0,
                        session_ids=["ai-session-12-1"],
                        workspace_fingerprint_before="fp-before-ai-12-1",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=1,
                        task_id="task-2",
                        pid=120012,
                        process_birth_time=1722965612.0,
                        session_ids=["ai-session-12-2"],
                        workspace_fingerprint_before="fp-before-ai-12-2",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        tester_only_specs: list[object] = []
        tester_only_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "reconcile-tester-only.sock",
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=lambda spec: (
                tester_only_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=63000 + len(tester_only_specs)),
                    process_birth_time=1716300000.0 + len(tester_only_specs),
                    session_id=f"tester-only-{len(tester_only_specs)}",
                    rollout_path=(tmp / "runtime" / f"tester-only-{len(tester_only_specs)}.jsonl").resolve(),
                )
            ),
        )
        tester_only_core._reconcile_pending_for_record(store.load_node("12"))
        if len(tester_only_specs) != 1 or tester_only_specs[0].actor_kind is not ActorKind.EVALUATOR_TESTER:
            return _fail("when AI-user work is done but tester is missing, reconcile must relaunch tester only")
        if str(tester_only_specs[0].env.get("LOOP_TASK_ID") or "") != "":
            return _fail("reconcile tester relaunch must not carry LOOP_TASK_ID")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = (tmp / "workspace" / "node-13").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Reconcile reviewer launches once.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="13",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc1300",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=130001,
            process_birth_time=1723965601.0,
            session_ids=["tester-session-13"],
            workspace_fingerprint_before="fp-before-tester-13",
        )
        base_record = store.load_node("13")
        if base_record is None:
            return _fail("reviewer-once reconcile node must be loadable")
        store.upsert_node(
            replace(
                base_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                task_result_refs={
                    "task-1": {
                        ActorKind.EVALUATOR_TESTER.value: "/tmp/task-1-tester.md",
                        ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-1-ai.md",
                    },
                    "task-2": {
                        ActorKind.EVALUATOR_TESTER.value: "/tmp/task-2-tester.md",
                        ActorKind.EVALUATOR_AI_USER.value: "/tmp/task-2-ai.md",
                    },
                },
                current_components=[],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): replace(
                        base_record.components[component_key(actor_kind=ActorKind.EVALUATOR_TESTER)],
                        status=ComponentStatus.COMPLETED,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=1,
                        task_id="task-1",
                        pid=130011,
                        process_birth_time=1723965611.0,
                        session_ids=["ai-session-13-1"],
                        workspace_fingerprint_before="fp-before-ai-13-1",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=1,
                        task_id="task-2",
                        pid=130012,
                        process_birth_time=1723965612.0,
                        session_ids=["ai-session-13-2"],
                        workspace_fingerprint_before="fp-before-ai-13-2",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        reviewer_once_specs: list[object] = []
        reviewer_once_core = RouterCore(
            store=store,
            wakeup_socket_path=tmp / "reconcile-reviewer-once.sock",
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=lambda spec: (
                reviewer_once_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=64000 + len(reviewer_once_specs)),
                    process_birth_time=1716400000.0 + len(reviewer_once_specs),
                    session_id=f"reviewer-once-{len(reviewer_once_specs)}",
                    rollout_path=(tmp / "runtime" / f"reviewer-once-{len(reviewer_once_specs)}.jsonl").resolve(),
                )
            ),
        )
        reviewer_once_core._reconcile_pending_for_record(store.load_node("13"))
        if len(reviewer_once_specs) != 1 or reviewer_once_specs[0].actor_kind is not ActorKind.EVALUATOR_REVIEWER:
            return _fail("reconcile must launch reviewer once after tester and all AI-user work are complete")
        reviewer_once_core._reconcile_pending_for_record(store.load_node("13"))
        if len(reviewer_once_specs) != 1:
            return _fail("reconcile must not launch duplicate reviewer actors once reviewer is already running")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        workspace_root = (tmp / "workspace" / "node-14").resolve()
        final_effects = _write_file(
            workspace_root / "FINAL_EFFECTS.md",
            "Stale exit must be ignored.\n",
        )
        checker_tasks_ref = _write_checker_tasks(workspace_root, ["task-1", "task-2"])
        _upsert_running_node(
            store=store,
            node_id="14",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc1400",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=140001,
            process_birth_time=1724965601.0,
            session_ids=["tester-session-14"],
            workspace_fingerprint_before="fp-before-tester-14",
        )
        tester_record = store.load_node("14")
        if tester_record is None:
            return _fail("stale-exit node must be loadable")
        store.upsert_node(
            replace(
                tester_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref),
                current_components=[
                    ActorRef(node_id="14", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                    ActorRef(node_id="14", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=2, task_id="task-1"),
                    ActorRef(node_id="14", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
                ],
                components={
                    component_key(actor_kind=ActorKind.EVALUATOR_TESTER): tester_record.components[
                        component_key(actor_kind=ActorKind.EVALUATOR_TESTER)
                    ],
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-1"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=2,
                        task_id="task-1",
                        pid=140012,
                        process_birth_time=1724965612.0,
                        session_ids=["ai-session-14-1"],
                        workspace_fingerprint_before="fp-before-ai-14-1",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                    component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-2"): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        task_id="task-2",
                        pid=140013,
                        process_birth_time=1724965613.0,
                        session_ids=["ai-session-14-2"],
                        workspace_fingerprint_before="fp-before-ai-14-2",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    ),
                },
            )
        )
        _write_completion(
            actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_AI_USER, task_id="task-1"),
            {
                "version": 1,
                "node_id": "14",
                "actor_kind": ActorKind.EVALUATOR_AI_USER.value,
                "attempt_count": 1,
                "pid": 140011,
                "process_birth_time": 1724965611.0,
                "completed_at": now.isoformat(),
                "task_id": "task-1",
                "result_ref": str(_write_file(workspace_root / "results" / "stale-task-1.md", "stale\n")),
            },
        )
        stale_specs: list[object] = []
        stale_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=lambda spec: (
                stale_specs.append(spec)
                or ActorLaunchResult(
                    process=_LiveProcess(pid=65000 + len(stale_specs)),
                    process_birth_time=1716500000.0 + len(stale_specs),
                    session_id=f"stale-{len(stale_specs)}",
                    rollout_path=(tmp / "runtime" / f"stale-{len(stale_specs)}.jsonl").resolve(),
                )
            ),
        )
        fake_proc.live_actors.update(
            {
                ActorRef(node_id="14", actor_kind=ActorKind.EVALUATOR_TESTER, attempt_count=1),
                ActorRef(node_id="14", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=2, task_id="task-1"),
                ActorRef(node_id="14", actor_kind=ActorKind.EVALUATOR_AI_USER, attempt_count=1, task_id="task-2"),
            }
        )
        stale_core.start()
        try:
            stale_exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="14",
                        actor_kind=ActorKind.EVALUATOR_AI_USER,
                        attempt_count=1,
                        task_id="task-1",
                    ),
                    pid=140011,
                    process_birth_time=1724965611.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == stale_exit_seq,
                timeout_seconds=2.0,
            ):
                return _fail("stale exit must still advance the inbox")
            if stale_specs:
                return _fail("stale old-attempt exit must not relaunch any evaluator lane")
            stale_record = store.load_node("14")
            if stale_record is None:
                return _fail("stale-exit node must remain loadable")
            stale_components = {
                (actor.actor_kind.value, str(actor.task_id or "").strip(), int(actor.attempt_count))
                for actor in stale_record.current_components
            }
            if stale_components != {
                (ActorKind.EVALUATOR_TESTER.value, "", 1),
                (ActorKind.EVALUATOR_AI_USER.value, "task-1", 2),
                (ActorKind.EVALUATOR_AI_USER.value, "task-2", 1),
            }:
                return _fail("stale exit must leave the currently running evaluator lanes unchanged")
        finally:
            stale_core.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
