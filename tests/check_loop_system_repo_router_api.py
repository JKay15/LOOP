#!/usr/bin/env python3
"""Validate router_api start/status/resume/pause behavior."""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-api][FAIL] {msg}", file=sys.stderr)
    return 2


def _run_main(main_fn, argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout_buffer = io.StringIO()
    with contextlib.redirect_stdout(stdout_buffer):
        exit_code = int(main_fn(argv) or 0)
    raw = stdout_buffer.getvalue().strip()
    if not raw:
        raise AssertionError("router_api main did not print a JSON payload")
    return exit_code, json.loads(raw)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import loop.agent_api as agent_api_module
        from loop.router_api import build_parser, main as router_api_main
        from loop.events import ActorKind, ActorRef, RequestSplit
        from loop.node_table import (
            ComponentRuntimeState,
            ComponentStatus,
            NodeRuntimeRecord,
            component_key,
        )
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router_api imports failed: {exc}")

    commands = set()
    parser = build_parser()
    for action in parser._actions:
        choices = getattr(action, "choices", None)
        if choices is not None:
            commands = set(choices)
            break
    expected_commands = {"pause", "resume", "start", "status"}
    if commands != expected_commands:
        return _fail(f"router_api parser must expose exactly {sorted(expected_commands)!r}, got {sorted(commands)!r}")
    start_args = parser.parse_args(
        [
            "start",
            "--router-db",
            "/tmp/router.sqlite3",
            "--kernel-session-id",
            "kernel-session-001",
            "--kernel-rollout-path",
            "/tmp/rollout.jsonl",
            "--kernel-started-at",
            "2026-04-03T00:30:00Z",
            "--final-effects-file",
            "/tmp/FINAL_EFFECTS.md",
            "--prompt-overlay-ref",
            "/tmp/prompt-overlay",
        ]
    )
    if str(getattr(start_args, "prompt_overlay_ref", "") or "") != "/tmp/prompt-overlay":
        return _fail("router_api start parser must accept --prompt-overlay-ref")
    resume_args = parser.parse_args(
        [
            "resume",
            "--router-db",
            "/tmp/router.sqlite3",
            "--prompt-overlay-ref",
            "/tmp/prompt-overlay",
        ]
    )
    if str(getattr(resume_args, "prompt_overlay_ref", "") or "") != "/tmp/prompt-overlay":
        return _fail("router_api resume parser must accept --prompt-overlay-ref")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        final_effects_file = tmp / "FINAL_EFFECTS.md"
        kernel_rollout_file = tmp / "kernel-rollout.jsonl"
        prompt_overlay = tmp / "prompt-overlay"
        prompt_overlay.mkdir(parents=True, exist_ok=True)
        (prompt_overlay / "all.md").write_text("overlay all\n", encoding="utf-8")
        final_effects_file.write_text("Implement correctly.\n", encoding="utf-8")
        kernel_rollout_file.write_text(
            '{"type":"session_meta","payload":{"id":"kernel-session-001","timestamp":"2026-04-03T00:30:00Z"}}\n',
            encoding="utf-8",
        )

        missing_exit_code, missing_payload = _run_main(
            router_api_main,
            [
                "start",
                "--router-db",
                str(db_path),
                "--kernel-rollout-path",
                str(kernel_rollout_file.resolve()),
                "--kernel-started-at",
                "2026-04-03T00:30:00Z",
                "--final-effects-file",
                str(final_effects_file.resolve()),
            ],
        )
        if missing_exit_code == 0:
            return _fail("router_api start must reject missing kernel_session_id")
        if str(missing_payload.get("reason_code") or "") != "MISSING_KERNEL_BOOTSTRAP_INPUT":
            return _fail("router_api start missing-input rejection must use a stable reason code")

        original_spawn = agent_api_module._spawn_router_runtime
        captured_start_kwargs: dict[str, object] = {}
        try:
            def _fake_spawn_router_runtime(*, startup_result_path: Path, **kwargs):
                captured_start_kwargs.update(kwargs)
                del kwargs
                startup_result_path.parent.mkdir(parents=True, exist_ok=True)
                startup_result_path.write_text(
                    json.dumps(
                        {
                            "accepted": True,
                            "status": "STARTED",
                            "message": "router started; root implementer launched",
                            "kernel_session_id": "kernel-session-001",
                            "root_node_id": "1",
                            "root_actor_kind": "implementer",
                            "root_attempt_count": 1,
                            "root_pid": 12345,
                            "root_process_birth_time": 1712275200.125,
                            "root_session_id": "root-session-001",
                            "root_rollout_path": str((tmp / "root-rollout.jsonl").resolve()),
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                class _DummyProcess:
                    pid = 99991

                    def poll(self):
                        return None

                return _DummyProcess()

            agent_api_module._spawn_router_runtime = _fake_spawn_router_runtime
            start_exit_code, start_payload = _run_main(
                router_api_main,
                [
                    "start",
                    "--router-db",
                    str(db_path),
                    "--kernel-session-id",
                    "kernel-session-001",
                    "--kernel-rollout-path",
                    str(kernel_rollout_file.resolve()),
                    "--kernel-started-at",
                    "2026-04-03T00:30:00Z",
                    "--final-effects-file",
                    str(final_effects_file.resolve()),
                    "--prompt-overlay-ref",
                    str(prompt_overlay.resolve()),
                ],
            )
        finally:
            agent_api_module._spawn_router_runtime = original_spawn

        if start_exit_code != 0 or start_payload.get("status") != "STARTED":
            return _fail("router_api start must surface a successful STARTED payload from the runtime helper")
        if str(captured_start_kwargs.get("prompt_overlay_ref") or "") != str(prompt_overlay.resolve()):
            return _fail("router_api start must forward --prompt-overlay-ref to the runtime launcher")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = tmp / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects = workspace_root / "FINAL_EFFECTS.md"
        final_effects.write_text("Do the work.\n", encoding="utf-8")
        kernel_rollout_file = tmp / "kernel-rollout.jsonl"
        kernel_rollout_file.write_text("{}", encoding="utf-8")
        store.write_kernel_bootstrap_info(
            kernel_session_id="kernel-session-xyz",
            kernel_rollout_path=str(kernel_rollout_file.resolve()),
            kernel_started_at="2026-04-06T08:44:00Z",
        )
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="1",
                parent_node_id="0",
                child_node_ids=[],
                workspace_root=str(workspace_root.resolve()),
                final_effects_file=str(final_effects.resolve()),
                split_request=1,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=["resume line"],
                current_components=[
                    ActorRef(
                        node_id="1",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    )
                ],
                durable_commit="abc123",
                result_commit="",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        pid=4242,
                        process_birth_time=1712390000.0,
                        session_ids=["sess-1"],
                        workspace_fingerprint_before="fp-before",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        seq = store.append_event(
            RequestSplit(
                actor=ActorRef(
                    node_id="1",
                    actor_kind=ActorKind.IMPLEMENTER,
                    attempt_count=1,
                ),
                split_bundle_ref=str((tmp / "split-bundle").resolve()),
                durable_commit="abc123",
                diff_fingerprint="fp-1",
                requested_at=datetime.now(timezone.utc),
            )
        )
        if int(seq) <= 0:
            return _fail("router_api status setup must append a RequestSplit event")

        pause_exit_code, pause_payload = _run_main(
            router_api_main,
            [
                "pause",
                "--router-db",
                str(db_path),
                "--reason",
                "manual pause for prompt surgery",
            ],
        )
        if pause_exit_code != 0 or pause_payload.get("status") != "PAUSE_PREPARED":
            return _fail("router_api pause must surface a successful PAUSE_PREPARED payload")
        if str(store.read_router_status() or "").strip() != "paused":
            return _fail("router_api pause must persist router_status=paused")
        if "manual pause" not in str(store.read_router_paused_reason_json() or ""):
            return _fail("router_api pause must persist the pause reason")

        status_exit_code, status_payload = _run_main(
            router_api_main,
            [
                "status",
                "--router-db",
                str(db_path),
            ],
        )
        if status_exit_code != 0 or status_payload.get("status") != "OK":
            return _fail("router_api status must return a stable OK payload")
        router_meta = dict(status_payload.get("router_meta") or {})
        if router_meta.get("router_status") != "paused":
            return _fail("router_api status must expose paused router_status")
        if "manual pause" not in str(router_meta.get("router_paused_reason_json") or ""):
            return _fail("router_api status must expose router_paused_reason_json")
        pending_reviews = list(status_payload.get("pending_split_reviews") or [])
        if len(pending_reviews) != 1 or int(pending_reviews[0].get("request_seq") or 0) != int(seq):
            return _fail("router_api status must expose durable pending split reviews")

        original_spawn = agent_api_module._spawn_router_runtime
        try:
            def _fake_resume_spawn(*, startup_result_path: Path, **kwargs):
                startup_result_path.parent.mkdir(parents=True, exist_ok=True)
                startup_result_path.write_text(
                    json.dumps(
                        {
                            "accepted": True,
                            "status": "RESUMED",
                            "message": "router resumed from the latest durable state",
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                class _DummyProcess:
                    pid = 99992

                    def poll(self):
                        return None

                return _DummyProcess()

            agent_api_module._spawn_router_runtime = _fake_resume_spawn
            paused_resume_exit_code, paused_resume_payload = _run_main(
                router_api_main,
                [
                    "resume",
                    "--router-db",
                    str(db_path),
                ],
            )
        finally:
            agent_api_module._spawn_router_runtime = original_spawn

        if paused_resume_exit_code != 0 or paused_resume_payload.get("status") != "RESUMED":
            return _fail("router_api resume must surface a successful RESUMED payload for paused recovery")
        if str(store.read_router_status() or "").strip():
            return _fail("router_api paused resume must clear router_status before runtime restart")
        if str(store.read_router_paused_reason_json() or "").strip():
            return _fail("router_api paused resume must clear router_paused_reason_json")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = tmp / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects = workspace_root / "FINAL_EFFECTS.md"
        final_effects.write_text("Original paused obligations.\n", encoding="utf-8")
        prompt_overlay = tmp / "resume-overlay"
        prompt_overlay.mkdir(parents=True, exist_ok=True)
        (prompt_overlay / "all.md").write_text("resume overlay all\n", encoding="utf-8")
        append_ref = tmp / "append-final-effects.md"
        append_ref.write_text("Paused append obligations.\n", encoding="utf-8")
        child_ref = ActorRef(
            node_id="7",
            actor_kind=ActorKind.IMPLEMENTER,
            attempt_count=2,
        )
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="1",
                parent_node_id="0",
                child_node_ids=["7"],
                workspace_root=str(workspace_root.resolve()),
                final_effects_file=str(final_effects.resolve()),
                split_request=1,
                split_approved=1,
                approved_split_request_seq=9,
                evaluator_phase="tasks",
                checker_tasks_ref="/tmp/paused-checker.json",
                task_result_refs={"task-1": {"evaluator_ai_user": "/tmp/result.json"}},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                current_components=[child_ref],
                durable_commit="paused-durable",
                result_commit="",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="paused-fingerprint",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.INACTIVE,
                        attempt_count=1,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["paused-root"],
                        workspace_fingerprint_before="paused-root-fp",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        store.write_router_paused_state(
            router_status="paused",
            router_paused_reason_json='{"reason":"manual pause"}',
            router_paused_at="2026-04-09T08:44:00Z",
        )

        original_spawn = agent_api_module._spawn_router_runtime
        try:
            def _fake_paused_append_spawn(*, startup_result_path: Path, **kwargs):
                startup_result_path.parent.mkdir(parents=True, exist_ok=True)
                startup_result_path.write_text(
                    json.dumps(
                        {
                            "accepted": True,
                            "status": "RESUMED",
                            "message": "router resumed from the latest durable state",
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                class _DummyProcess:
                    pid = 999921

                    def poll(self):
                        return None

                return _DummyProcess()

            agent_api_module._spawn_router_runtime = _fake_paused_append_spawn
            paused_append_exit_code, paused_append_payload = _run_main(
                router_api_main,
                [
                    "resume",
                    "--router-db",
                    str(db_path),
                    "--prompt-overlay-ref",
                    str(prompt_overlay.resolve()),
                    "--append-final-effects-ref",
                    str(append_ref),
                ],
            )
        finally:
            agent_api_module._spawn_router_runtime = original_spawn

        if paused_append_exit_code != 0 or paused_append_payload.get("status") != "RESUMED":
            return _fail("router_api resume must surface a successful RESUMED payload for paused append recovery")
        if str(store.read_router_status() or "").strip():
            return _fail("router_api paused append resume must clear router_status before runtime restart")
        if child_ref != (store.load_node("1").current_components or [None])[0]:
            return _fail("router_api paused append resume must preserve current frontier ownership")
        merged_paused_final_effects = final_effects.read_text(encoding="utf-8")
        if "Original paused obligations." not in merged_paused_final_effects or "Paused append obligations." not in merged_paused_final_effects:
            return _fail("router_api paused append resume must append to the root FINAL_EFFECTS file")
        if not (db_path.parent / "router" / "prompt_overlays" / "001" / "all.md").is_file():
            return _fail("router_api resume must snapshot --prompt-overlay-ref into router/prompt_overlays")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = tmp / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects = workspace_root / "FINAL_EFFECTS.md"
        final_effects.write_text("Do the work.\n", encoding="utf-8")
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="71",
                parent_node_id="70",
                child_node_ids=[],
                workspace_root=str(workspace_root.resolve()),
                final_effects_file=str(final_effects.resolve()),
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                current_components=[
                    ActorRef(
                        node_id="71",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=2,
                    )
                ],
                durable_commit="base-commit",
                result_commit="",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.TERMINAL_FAILED,
                        attempt_count=2,
                        pid=4242,
                        process_birth_time=1712390000.0,
                        session_ids=["resume-sess-1"],
                        workspace_fingerprint_before="resume-fp-before",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=5,
                        consecutive_failed_exits=3,
                    )
                },
            )
        )
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"majority_frontier_terminal_failed"}',
            router_terminal_at="2026-04-09T10:00:00Z",
        )

        original_spawn = agent_api_module._spawn_router_runtime
        try:
            def _fake_resume_spawn(*, startup_result_path: Path, **kwargs):
                startup_result_path.parent.mkdir(parents=True, exist_ok=True)
                startup_result_path.write_text(
                    json.dumps(
                        {
                            "accepted": True,
                            "status": "RESUMED",
                            "message": "router resumed from the latest durable state",
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                class _DummyProcess:
                    pid = 99993

                    def poll(self):
                        return None

                return _DummyProcess()

            agent_api_module._spawn_router_runtime = _fake_resume_spawn
            resume_exit_code, resume_payload = _run_main(
                router_api_main,
                [
                    "resume",
                    "--router-db",
                    str(db_path),
                ],
            )
        finally:
            agent_api_module._spawn_router_runtime = original_spawn

        if resume_exit_code != 0 or resume_payload.get("status") != "RESUMED":
            return _fail("router_api resume must surface a successful RESUMED payload for terminal_failed recovery")
        if str(store.read_router_status() or "").strip():
            return _fail("router_api terminal_failed resume must clear router_status before runtime restart")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        root_workspace = tmp / "root-workspace"
        root_workspace.mkdir(parents=True, exist_ok=True)
        root_final_effects = root_workspace / "FINAL_EFFECTS.md"
        root_final_effects.write_text("Original terminal obligations.\n", encoding="utf-8")
        child_workspace = tmp / "child-workspace"
        child_workspace.mkdir(parents=True, exist_ok=True)
        child_final_effects = child_workspace / "FINAL_EFFECTS.md"
        child_final_effects.write_text("Child obligations.\n", encoding="utf-8")
        append_ref = tmp / "append-final-effects.md"
        append_ref.write_text("Terminal append obligations.\n", encoding="utf-8")
        store.upsert_nodes(
            [
                NodeRuntimeRecord(
                    node_id="1",
                    parent_node_id="0",
                    child_node_ids=["71"],
                    workspace_root=str(root_workspace.resolve()),
                    final_effects_file=str(root_final_effects.resolve()),
                    split_request=1,
                    split_approved=1,
                    approved_split_request_seq=5,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[],
                    durable_commit="root-durable",
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.TERMINAL_FAILED,
                            attempt_count=1,
                            pid=0,
                            process_birth_time=None,
                            session_ids=["root-terminal"],
                            workspace_fingerprint_before="root-fp",
                            saw_output_in_attempt=True,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=1,
                        )
                    },
                ),
                NodeRuntimeRecord(
                    node_id="71",
                    parent_node_id="1",
                    child_node_ids=[],
                    workspace_root=str(child_workspace.resolve()),
                    final_effects_file=str(child_final_effects.resolve()),
                    split_request=0,
                    split_approved=0,
                    approved_split_request_seq=0,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[
                        ActorRef(
                            node_id="71",
                            actor_kind=ActorKind.IMPLEMENTER,
                            attempt_count=2,
                        )
                    ],
                    durable_commit="child-durable",
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.TERMINAL_FAILED,
                            attempt_count=2,
                            pid=4242,
                            process_birth_time=1712390000.0,
                            session_ids=["sess-1"],
                            workspace_fingerprint_before="fp-before",
                            saw_output_in_attempt=True,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=2,
                        )
                    },
                ),
            ]
        )
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"majority_frontier_terminal_failed"}',
            router_terminal_at="2026-04-09T09:00:00Z",
        )

        original_spawn = agent_api_module._spawn_router_runtime
        try:
            def _fake_terminal_append_spawn(*, startup_result_path: Path, **kwargs):
                startup_result_path.parent.mkdir(parents=True, exist_ok=True)
                startup_result_path.write_text(
                    json.dumps(
                        {
                            "accepted": True,
                            "status": "RESUMED",
                            "message": "router resumed from the latest durable state",
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                class _DummyProcess:
                    pid = 999922

                    def poll(self):
                        return None

                return _DummyProcess()

            agent_api_module._spawn_router_runtime = _fake_terminal_append_spawn
            terminal_append_exit_code, terminal_append_payload = _run_main(
                router_api_main,
                [
                    "resume",
                    "--router-db",
                    str(db_path),
                    "--append-final-effects-ref",
                    str(append_ref),
                ],
            )
        finally:
            agent_api_module._spawn_router_runtime = original_spawn

        if terminal_append_exit_code != 0 or terminal_append_payload.get("status") != "RESUMED":
            return _fail("router_api resume must surface a successful RESUMED payload for terminal_failed append recovery")
        if str(store.read_router_status() or "").strip():
            return _fail("router_api terminal_failed append resume must clear router_status before runtime restart")
        recovered_child = store.load_node("71")
        recovered_child_component = None if recovered_child is None else recovered_child.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
        if recovered_child_component is None or recovered_child_component.status is not ComponentStatus.INACTIVE:
            return _fail("router_api terminal_failed append resume must still reactivate the unfinished child frontier")
        merged_terminal_final_effects = root_final_effects.read_text(encoding="utf-8")
        if "Original terminal obligations." not in merged_terminal_final_effects or "Terminal append obligations." not in merged_terminal_final_effects:
            return _fail("router_api terminal_failed append resume must append to the root FINAL_EFFECTS file")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        workspace_root = tmp / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        final_effects = workspace_root / "FINAL_EFFECTS.md"
        final_effects.write_text("Original obligations.\n", encoding="utf-8")
        takeover_root = tmp / "takeover-workspace"
        takeover_root.mkdir(parents=True, exist_ok=True)
        takeover_final_effects = takeover_root / "FINAL_EFFECTS.md"
        takeover_final_effects.write_text("Previously completed obligations.\n", encoding="utf-8")
        append_ref = tmp / "append-final-effects.md"
        append_ref.write_text("New obligations.\n", encoding="utf-8")
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="1",
                parent_node_id="0",
                child_node_ids=["7", "8"],
                workspace_root=str(workspace_root.resolve()),
                final_effects_file=str(final_effects.resolve()),
                split_request=1,
                split_approved=1,
                approved_split_request_seq=41,
                evaluator_phase="reviewer",
                checker_tasks_ref="/tmp/old-checker.json",
                task_result_refs={"task-1": {"evaluator_tester": "/tmp/old-result.md"}},
                reviewer_verdict_kind="OK",
                reviewer_report_ref="/tmp/old-reviewer.md",
                pending_prelude_lines=[],
                current_components=[],
                durable_commit="base-commit",
                result_commit="older-root-result",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="old-root-fingerprint",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=4,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["old-implementer"],
                        workspace_fingerprint_before="completed-fp",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="0",
                parent_node_id="0",
                child_node_ids=[],
                workspace_root=str(takeover_root.resolve()),
                final_effects_file=str(takeover_final_effects.resolve()),
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="OK",
                reviewer_report_ref="/tmp/final-reviewer.md",
                pending_prelude_lines=[],
                current_components=[],
                durable_commit="kernel-base-commit",
                result_commit="result-commit-final",
                escalated_to_kernel=True,
                last_rejected_split_diff_fingerprint="kernel-fingerprint",
                components={
                    component_key(actor_kind=ActorKind.KERNEL): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=3,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["old-kernel"],
                        workspace_fingerprint_before="kernel-fp",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        store.write_router_completed_state(
            router_status="completed",
            router_completed_result_commit="result-commit-final",
            router_completed_report_ref="/tmp/final-reviewer.md",
            router_completed_at="2026-04-09T11:00:00Z",
        )

        original_spawn = agent_api_module._spawn_router_runtime
        try:
            def _fake_completed_resume_spawn(*, startup_result_path: Path, **kwargs):
                startup_result_path.parent.mkdir(parents=True, exist_ok=True)
                startup_result_path.write_text(
                    json.dumps(
                        {
                            "accepted": True,
                            "status": "RESUMED",
                            "message": "router resumed from the latest durable state",
                        },
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )

                class _DummyProcess:
                    pid = 99994

                    def poll(self):
                        return None

                return _DummyProcess()

            agent_api_module._spawn_router_runtime = _fake_completed_resume_spawn
            completed_resume_exit_code, completed_resume_payload = _run_main(
                router_api_main,
                [
                    "resume",
                    "--router-db",
                    str(db_path),
                    "--append-final-effects-ref",
                    str(append_ref),
                ],
            )
        finally:
            agent_api_module._spawn_router_runtime = original_spawn

        if completed_resume_exit_code != 0 or completed_resume_payload.get("status") != "RESUMED":
            return _fail("router_api resume must surface a successful RESUMED payload for completed append recovery")
        resumed_root = store.load_node("1")
        if resumed_root is None:
            return _fail("router_api completed resume must preserve the root node")
        if str(store.read_router_status() or "").strip():
            return _fail("router_api completed resume must clear router_status before runtime restart")
        if str(resumed_root.workspace_root or "") != str(takeover_root.resolve()):
            return _fail("router_api completed resume must relaunch root from the final completed workspace")
        if str(Path(resumed_root.final_effects_file).resolve()) != str(takeover_final_effects.resolve()):
            return _fail("router_api completed resume must relaunch root against the final completed FINAL_EFFECTS file")

    print("[loop-system-router-api][PASS] router_api hard-splits router commands and supports start/status/pause/resume plus append-final-effects across paused, terminal_failed, and completed states")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
