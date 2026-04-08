#!/usr/bin/env python3
"""Validate router unified complete command and evaluator-facing parser surface."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-agent-api-evaluator][FAIL] {msg}", file=sys.stderr)
    return 2


@contextlib.contextmanager
def _env(**updates: str):
    previous = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _run_main(main_fn, argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout_buffer = io.StringIO()
    with contextlib.redirect_stdout(stdout_buffer):
        exit_code = int(main_fn(argv) or 0)
    raw = stdout_buffer.getvalue().strip()
    if not raw:
        raise AssertionError("agent_api main did not print a JSON payload")
    return exit_code, json.loads(raw)


def _write_file(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path.resolve()


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
                )
            ],
            durable_commit=durable_commit,
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint="",
            components={
                component_key(actor_kind=actor_kind): ComponentRuntimeState(
                    status=ComponentStatus.RUNNING,
                    attempt_count=int(attempt_count),
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


def _subcommand_names(parser: argparse.ArgumentParser) -> set[str]:
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return set(action.choices)
    return set()


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.agent_api import main as agent_api_main
        from loop.agent_api import build_parser
        from loop.completion import actor_completion_record_path
        from loop.events import ActorKind
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router evaluator agent_api imports failed: {exc}")

    parser = build_parser()
    commands = _subcommand_names(parser)
    expected_present = {"request-split", "complete", "start"}
    if not expected_present.issubset(commands):
        return _fail(f"agent_api parser must expose {sorted(expected_present)!r}, got {sorted(commands)!r}")
    unexpected = {
        "request-evaluation",
        "submit-checker-tasks",
        "submit-task-result",
        "submit-reviewer-verdict",
    }
    if commands & unexpected:
        return _fail("agent_api parser must not expose the removed evaluator submission commands")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)

        workspace_root = (tmp / "workspace" / "node-1").resolve()
        final_effects = _write_file(workspace_root / "FINAL_EFFECTS.md", "Do the node work.\n")

        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=2,
            durable_commit="abc123",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=11111,
            process_birth_time=1711965600.0,
            session_ids=["impl-session"],
            workspace_fingerprint_before="fp-before-impl",
        )
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="1",
            LOOP_ACTOR_KIND="implementer",
            LOOP_ATTEMPT_COUNT="2",
            LOOP_WORKSPACE_ROOT=str(workspace_root),
        ):
            exit_code, payload = _run_main(agent_api_main, ["complete"])
        if exit_code != 0 or str(payload.get("status") or "") != "RECORDED":
            return _fail("implementer complete must record successfully")
        impl_completion_path = actor_completion_record_path(workspace_root, ActorKind.IMPLEMENTER)
        impl_payload = json.loads(impl_completion_path.read_text(encoding="utf-8"))
        if impl_payload.get("pid") != 11111 or impl_payload.get("process_birth_time") != 1711965600.0:
            return _fail("implementer complete must auto-record pid and process_birth_time from router-owned state")

        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc123",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            pid=22222,
            process_birth_time=1711965601.0,
            session_ids=["checker-session"],
            workspace_fingerprint_before="fp-before-checker",
        )
        checker_tasks = _write_file(
            workspace_root / "checker" / "tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [
                        {"task_id": "task-1", "task_ref": str(_write_file(workspace_root / "checker" / "task-1.md", "Check A\n"))},
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="1",
            LOOP_ACTOR_KIND="evaluator_checker",
            LOOP_ATTEMPT_COUNT="1",
            LOOP_WORKSPACE_ROOT=str(workspace_root),
        ):
            missing_exit_code, missing_payload = _run_main(agent_api_main, ["complete"])
            if missing_exit_code == 0 or str(missing_payload.get("reason_code") or "") != "INVALID_CHECKER_TASKS_REF":
                return _fail("checker complete must reject missing tasks_ref")
            exit_code, payload = _run_main(agent_api_main, ["complete", "--tasks-ref", str(checker_tasks)])
        if exit_code != 0 or str(payload.get("status") or "") != "RECORDED":
            return _fail("checker complete must record successfully")
        checker_completion_path = actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_CHECKER)
        checker_payload = json.loads(checker_completion_path.read_text(encoding="utf-8"))
        if checker_payload.get("tasks_ref") != str(checker_tasks):
            return _fail("checker complete must preserve tasks_ref in the completion record")

        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=4,
            durable_commit="abc123",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            pid=33333,
            process_birth_time=1711965602.0,
            session_ids=["tester-session"],
            workspace_fingerprint_before="fp-before-tester",
        )
        tester_result = _write_file(workspace_root / "results" / "task-1-tester.md", "tester result\n")
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="1",
            LOOP_ACTOR_KIND="evaluator_tester",
            LOOP_ATTEMPT_COUNT="4",
            LOOP_WORKSPACE_ROOT=str(workspace_root),
        ):
            exit_code, payload = _run_main(
                agent_api_main,
                ["complete", "--result-ref", str(tester_result)],
            )
        if exit_code != 0 or str(payload.get("status") or "") != "RECORDED":
            return _fail("tester complete must record successfully")
        tester_completion_path = actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_TESTER)
        tester_payload = json.loads(tester_completion_path.read_text(encoding="utf-8"))
        if tester_payload.get("result_ref") != str(tester_result):
            return _fail("tester complete must preserve result_ref")

        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc123",
            workspace_root=workspace_root,
            final_effects_file=final_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=44444,
            process_birth_time=1711965603.0,
            session_ids=["reviewer-session"],
            workspace_fingerprint_before="fp-before-reviewer",
        )
        reviewer_report = _write_file(workspace_root / "reviewer" / "report.md", "needs fixes\n")
        reviewer_feedback_detail = _write_file(
            workspace_root / "reviewer" / "implementer_feedback.md",
            "implementer must address the reviewer finding\n",
        )
        reviewer_feedback_evidence = _write_file(
            workspace_root / "reviewer" / "evidence.txt",
            "fidelity mismatch evidence\n",
        )
        reviewer_feedback = _write_file(
            workspace_root / "reviewer" / "feedback.json",
            json.dumps(
                {
                    "version": 1,
                    "implementer": {
                        "feedback_ref": str(reviewer_feedback_detail),
                        "findings": [
                            {
                                "blocking": True,
                                "summary": "implementer finding",
                                "evidence_ref": str(reviewer_feedback_evidence),
                            }
                        ],
                    },
                    "checker": {"feedback_ref": "", "findings": []},
                    "tester": {"feedback_ref": "", "findings": []},
                    "ai_user": [],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="1",
            LOOP_ACTOR_KIND="evaluator_reviewer",
            LOOP_ATTEMPT_COUNT="1",
            LOOP_WORKSPACE_ROOT=str(workspace_root),
        ):
            invalid_exit_code, invalid_payload = _run_main(
                agent_api_main,
                [
                    "complete",
                    "--verdict-kind",
                    "MAYBE",
                    "--report-ref",
                    str(reviewer_report),
                ],
            )
            if invalid_exit_code == 0 or str(invalid_payload.get("reason_code") or "") != "INVALID_REVIEWER_VERDICT":
                return _fail("reviewer complete must reject invalid verdict kinds")
            exit_code, payload = _run_main(
                agent_api_main,
                [
                    "complete",
                    "--verdict-kind",
                    "IMPLEMENTER_ACTION_REQUIRED",
                    "--report-ref",
                    str(reviewer_report),
                    "--feedback-ref",
                    str(reviewer_feedback),
                ],
            )
        if exit_code != 0 or str(payload.get("status") or "") != "RECORDED":
            return _fail("reviewer complete must record successfully")
        reviewer_completion_path = actor_completion_record_path(workspace_root, ActorKind.EVALUATOR_REVIEWER)
        reviewer_payload = json.loads(reviewer_completion_path.read_text(encoding="utf-8"))
        if reviewer_payload.get("verdict_kind") != "IMPLEMENTER_ACTION_REQUIRED":
            return _fail("reviewer complete must preserve verdict_kind")
        if reviewer_payload.get("report_ref") != str(reviewer_report):
            return _fail("reviewer complete must preserve report_ref")
        if reviewer_payload.get("feedback_ref") != str(reviewer_feedback):
            return _fail("reviewer complete must preserve feedback_ref")

        shell_workspace_root = (tmp / "workspace" / "node-2").resolve()
        shell_final_effects = _write_file(shell_workspace_root / "FINAL_EFFECTS.md", "Do the shell node work.\n")
        _upsert_running_node(
            store=store,
            node_id="2",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc123",
            workspace_root=shell_workspace_root,
            final_effects_file=shell_final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=55555,
            process_birth_time=1711965604.0,
            session_ids=["shell-session"],
            workspace_fingerprint_before="fp-before-shell",
        )
        shell_env = dict(os.environ)
        shell_env.update(
            {
                "LOOP_ROUTER_DB_PATH": str(db_path),
                "LOOP_NODE_ID": "2",
                "LOOP_ACTOR_KIND": "implementer",
                "LOOP_ATTEMPT_COUNT": "1",
                "LOOP_WORKSPACE_ROOT": str(shell_workspace_root),
            }
        )
        shell_proc = subprocess.run(
            [str(ROOT / "scripts" / "routerctl.sh"), "complete"],
            cwd=str(shell_workspace_root),
            env=shell_env,
            capture_output=True,
            text=True,
        )
        if shell_proc.returncode != 0:
            return _fail(
                "routerctl.sh complete must succeed from the actor workspace cwd; "
                f"stdout={shell_proc.stdout!r} stderr={shell_proc.stderr!r}"
            )
        shell_payload = json.loads(shell_proc.stdout)
        if str(shell_payload.get("status") or "") != "RECORDED":
            return _fail("routerctl.sh complete from workspace cwd must record successfully")
        shell_completion_path = actor_completion_record_path(shell_workspace_root, ActorKind.IMPLEMENTER)
        if not shell_completion_path.exists():
            return _fail("routerctl.sh complete from workspace cwd must write the completion record")

        kernel_workspace_root = (tmp / "workspace" / "node-0").resolve()
        kernel_final_effects = _write_file(kernel_workspace_root / "FINAL_EFFECTS.md", "Do the kernel review work.\n")
        _upsert_running_node(
            store=store,
            node_id="0",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="abc123",
            workspace_root=kernel_workspace_root,
            final_effects_file=kernel_final_effects,
            actor_kind=ActorKind.KERNEL,
            pid=66666,
            process_birth_time=1711965605.0,
            session_ids=["kernel-session"],
            workspace_fingerprint_before="fp-before-kernel",
        )
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="0",
            LOOP_ACTOR_KIND="kernel",
            LOOP_ATTEMPT_COUNT="1",
            LOOP_WORKSPACE_ROOT=str(kernel_workspace_root),
            LOOP_REQUEST_SEQ="7",
        ):
            invalid_kernel_exit_code, invalid_kernel_payload = _run_main(
                agent_api_main,
                ["complete", "--verdict-kind", "MAYBE"],
            )
            if (
                invalid_kernel_exit_code == 0
                or str(invalid_kernel_payload.get("reason_code") or "") != "INVALID_KERNEL_VERDICT"
            ):
                return _fail("kernel complete must reject invalid verdict kinds")
            exit_code, payload = _run_main(
                agent_api_main,
                ["complete", "--verdict-kind", "APPROVE"],
            )
        if exit_code != 0 or str(payload.get("status") or "") != "RECORDED":
            return _fail("kernel complete must record successfully")
        kernel_completion_path = actor_completion_record_path(kernel_workspace_root, ActorKind.KERNEL)
        kernel_payload = json.loads(kernel_completion_path.read_text(encoding="utf-8"))
        if kernel_payload.get("verdict_kind") != "APPROVE":
            return _fail("kernel complete must preserve verdict_kind")
        if kernel_payload.get("request_seq") != 7:
            return _fail("kernel complete must preserve LOOP_REQUEST_SEQ in the completion record")

        kernel_takeover_workspace_root = (tmp / "workspace" / "node-0-final").resolve()
        kernel_takeover_effects = _write_file(
            kernel_takeover_workspace_root / "FINAL_EFFECTS.md",
            "Do the final kernel closeout work.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="0",
            parent_node_id="0",
            attempt_count=2,
            durable_commit="def456",
            workspace_root=kernel_takeover_workspace_root,
            final_effects_file=kernel_takeover_effects,
            actor_kind=ActorKind.KERNEL,
            pid=77777,
            process_birth_time=1711965606.0,
            session_ids=["kernel-final-session"],
            workspace_fingerprint_before="fp-before-kernel-final",
        )
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="0",
            LOOP_ACTOR_KIND="kernel",
            LOOP_ATTEMPT_COUNT="2",
            LOOP_WORKSPACE_ROOT=str(kernel_takeover_workspace_root),
        ):
            exit_code, payload = _run_main(agent_api_main, ["complete"])
        if exit_code != 0 or str(payload.get("status") or "") != "RECORDED":
            return _fail("ordinary kernel complete must record successfully without split-review verdict fields")
        kernel_takeover_completion_path = actor_completion_record_path(
            kernel_takeover_workspace_root,
            ActorKind.KERNEL,
        )
        kernel_takeover_payload = json.loads(
            kernel_takeover_completion_path.read_text(encoding="utf-8")
        )
        if kernel_takeover_payload.get("verdict_kind") not in {None, ""}:
            return _fail("ordinary kernel complete must not inject kernel review verdict fields")
        if kernel_takeover_payload.get("request_seq") not in {None, ""}:
            return _fail("ordinary kernel complete must not inject LOOP_REQUEST_SEQ when absent")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
