#!/usr/bin/env python3
"""Validate router agent API request-split behavior."""

from __future__ import annotations

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-agent-api][FAIL] {msg}", file=sys.stderr)
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


@contextlib.contextmanager
def _chdir(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def _git(*args: str, cwd: Path) -> None:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise AssertionError(proc.stderr.strip() or f"git {' '.join(args)} failed")


def _write_split_bundle(root: Path, *, bundle_name: str, child_names: list[str]) -> Path:
    bundle_dir = (root / bundle_name).resolve()
    children_dir = bundle_dir / "children"
    children_dir.mkdir(parents=True, exist_ok=True)
    coverage_units: list[dict[str, str]] = []
    children_payload: list[dict[str, object]] = []
    for index, child_name in enumerate(child_names, start=1):
        final_effects_path = children_dir / child_name / "FINAL_EFFECTS.md"
        final_effects_path.parent.mkdir(parents=True, exist_ok=True)
        final_effects_path.write_text(f"# {child_name}\n\nDo the child work.\n", encoding="utf-8")
        unit_id = f"u{index:02d}"
        coverage_units.append(
            {
                "id": unit_id,
                "requirement_text": f"Remaining work for {child_name}.",
            }
        )
        children_payload.append(
            {
                "name": child_name,
                "final_effects_file": str(final_effects_path.relative_to(bundle_dir)),
                "covered_unit_ids": [unit_id],
            }
        )
    (bundle_dir / "proposal.json").write_text(
        json.dumps(
            {
                "version": 2,
                "coverage_units": coverage_units,
                "children": children_payload,
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return bundle_dir


def _run_main(main_fn, argv: list[str]) -> tuple[int, dict[str, object]]:
    stdout_buffer = io.StringIO()
    with contextlib.redirect_stdout(stdout_buffer):
        exit_code = int(main_fn(argv) or 0)
    raw = stdout_buffer.getvalue().strip()
    if not raw:
        raise AssertionError("agent_api main did not print a JSON payload")
    return exit_code, json.loads(raw)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.agent_api import main as agent_api_main
        from loop.core import RouterCore, router_wakeup_socket_path
        from loop.events import (
            ActorKind,
            ActorRef,
            RejectSplit,
            KERNEL_ATTEMPT_COUNT,
            KERNEL_NODE_ID,
        )
        from loop.node_table import (
            ComponentRuntimeState,
            ComponentStatus,
            NodeRuntimeRecord,
            component_key,
        )
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router agent_api imports failed: {exc}")

    now = datetime.now(timezone.utc)

    with tempfile.TemporaryDirectory() as tmpdir:
        workspace_root = Path(tmpdir) / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        _git("init", "-q", cwd=workspace_root)
        _git("config", "user.email", "router-test@example.com", cwd=workspace_root)
        _git("config", "user.name", "Router Test", cwd=workspace_root)
        (workspace_root / "artifact.txt").write_text("line-1\n", encoding="utf-8")
        (workspace_root / "FINAL_EFFECTS.md").write_text("Implement correctly.\n", encoding="utf-8")
        _git("add", "artifact.txt", cwd=workspace_root)
        _git("commit", "-q", "-m", "init", cwd=workspace_root)
        base_commit = subprocess.run(
            ["git", "-C", str(workspace_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        (workspace_root / "artifact.txt").write_text("line-1\nline-2\n", encoding="utf-8")
        first_bundle = _write_split_bundle(
            workspace_root,
            bundle_name="split-bundle-001",
            child_names=["child_1", "child_2"],
        )
        second_bundle = _write_split_bundle(
            workspace_root,
            bundle_name="split-bundle-002",
            child_names=["child_1"],
        )
        third_bundle = _write_split_bundle(
            workspace_root,
            bundle_name="split-bundle-003",
            child_names=["child_1"],
        )
        invalid_bundle = (workspace_root / "split-bundle-invalid").resolve()
        invalid_bundle.mkdir(parents=True, exist_ok=True)

        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        core = RouterCore(store=store, wakeup_socket_path=router_wakeup_socket_path(db_path))
        try:
            store.upsert_node(
                NodeRuntimeRecord(
                    node_id="1",
                    parent_node_id="0",
                    child_node_ids=[],
                    workspace_root=str(workspace_root.resolve()),
                    final_effects_file=str((workspace_root / "FINAL_EFFECTS.md").resolve()),
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
                            node_id="1",
                            actor_kind=ActorKind.IMPLEMENTER,
                            attempt_count=3,
                        )
                    ],
                    durable_commit=base_commit,
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.RUNNING,
                            attempt_count=3,
                            pid=43210,
                            process_birth_time=1711965600.0,
                            session_ids=["session-1"],
                            workspace_fingerprint_before="fingerprint-before-001",
                            saw_output_in_attempt=False,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=0,
                        )
                    },
                )
            )

            with _env(
                LOOP_ROUTER_DB_PATH=str(db_path),
                LOOP_NODE_ID="1",
                LOOP_ACTOR_KIND="implementer",
                LOOP_ATTEMPT_COUNT="3",
            ), _chdir(workspace_root):
                first_exit_code, first_payload = _run_main(
                    agent_api_main,
                    [
                        "request-split",
                        "--split-bundle-ref",
                        str(first_bundle),
                    ],
                )
                if first_exit_code != 0:
                    return _fail("first request-split call must enqueue successfully")
                if str(first_payload.get("status") or "") != "ENQUEUED":
                    return _fail("successful request-split must report ENQUEUED status")
                if str(first_payload.get("durable_commit") or "") != base_commit:
                    return _fail("successful request-split must surface the durable commit it was keyed against")
                first_diff_fingerprint = str(first_payload.get("diff_fingerprint") or "").strip()
                if not first_diff_fingerprint:
                    return _fail("successful request-split must surface a non-empty diff_fingerprint")
                frozen_bundle_ref = Path(str(first_payload.get("split_bundle_ref") or "")).resolve()
                if frozen_bundle_ref == first_bundle.resolve():
                    return _fail("successful request-split must freeze the split bundle into a router-owned snapshot path")
                if not (frozen_bundle_ref / "proposal.json").exists():
                    return _fail("frozen split bundle snapshot must contain proposal.json")

                events_after_first = store.list_events_after(0)
                if len(events_after_first) != 1 or events_after_first[0].event_type != "RequestSplit":
                    return _fail("successful request-split must append exactly one RequestSplit inbox item")
                first_request_seq = int(first_payload.get("request_seq") or 0)
                if first_request_seq <= 0:
                    return _fail("successful request-split must surface a positive request_seq")
                first_record_after_submit = store.load_node("1")
                if first_record_after_submit is None or first_record_after_submit.split_request != 1:
                    return _fail("request-split must reserve split_request=1 atomically before core drains the event")
                first_event_payload = json.loads(events_after_first[0].payload_json)
                if Path(str(first_event_payload.get("split_bundle_ref") or "")).resolve() != frozen_bundle_ref:
                    return _fail("stored RequestSplit payload must preserve the frozen split_bundle_ref snapshot path")
                if str(first_event_payload.get("durable_commit") or "") != base_commit:
                    return _fail("stored RequestSplit payload must preserve durable_commit")
                if str(first_event_payload.get("diff_fingerprint") or "") != first_diff_fingerprint:
                    return _fail("stored RequestSplit payload must preserve diff_fingerprint")
                if first_request_seq != int(events_after_first[0].seq):
                    return _fail("request_seq must equal the original RequestSplit inbox seq")
                original_first_proposal = (first_bundle / "proposal.json").read_text(encoding="utf-8")
                (first_bundle / "proposal.json").write_text(
                    json.dumps(
                        {
                            "version": 2,
                            "coverage_units": [
                                {
                                    "id": "u01",
                                    "requirement_text": "mutated requirement",
                                }
                            ],
                            "children": [
                                {
                                    "name": "mutated_child",
                                    "final_effects_file": "children/child_1/FINAL_EFFECTS.md",
                                    "covered_unit_ids": ["u01"],
                                }
                            ],
                        },
                        indent=2,
                        sort_keys=True,
                    ),
                    encoding="utf-8",
                )
                snapshot_proposal = json.loads((frozen_bundle_ref / "proposal.json").read_text(encoding="utf-8"))
                child_names = [str(child.get("name") or "") for child in snapshot_proposal.get("children") or []]
                if child_names != ["child_1", "child_2"]:
                    return _fail("frozen split bundle snapshot must not change after the original bundle is mutated")
                (first_bundle / "proposal.json").write_text(original_first_proposal, encoding="utf-8")

                pending_exit_code, pending_payload = _run_main(
                    agent_api_main,
                    [
                        "request-split",
                        "--split-bundle-ref",
                        str(second_bundle),
                    ],
                )
                if pending_exit_code == 0:
                    return _fail("request-split must reject repeated submission while a split review is already pending")
                if str(pending_payload.get("reason_code") or "") != "SPLIT_REVIEW_ALREADY_PENDING":
                    return _fail("pending split review rejection must return a stable reason code")
                if "already has a split request" not in str(pending_payload.get("message") or "").lower():
                    return _fail("pending split review rejection must return a human-readable child-facing message")
                if len(store.list_events_after(0)) != 1:
                    return _fail("pending split review rejection must not append a duplicate RequestSplit inbox item")

                core.start()
                if not _wait_until(
                    lambda: store.read_last_applied_seq() == first_request_seq,
                    timeout_seconds=2.0,
                ):
                    return _fail("startup core must drain the previously reserved RequestSplit event")

                store.append_event(
                    RejectSplit(
                        actor=ActorRef(
                            node_id=KERNEL_NODE_ID,
                            actor_kind=ActorKind.KERNEL,
                            attempt_count=KERNEL_ATTEMPT_COUNT,
                        ),
                        target_node_id="1",
                        request_seq=first_request_seq,
                        rejected_at=now,
                        reason_ref="/tmp/reject.md",
                    ),
                    recorded_at=now,
                )
                store.upsert_node(
                    NodeRuntimeRecord(
                        node_id="1",
                        parent_node_id="0",
                        child_node_ids=[],
                        workspace_root=str(workspace_root.resolve()),
                        final_effects_file=str((workspace_root / "FINAL_EFFECTS.md").resolve()),
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
                                node_id="1",
                                actor_kind=ActorKind.IMPLEMENTER,
                                attempt_count=3,
                            )
                        ],
                        durable_commit=base_commit,
                        escalated_to_kernel=False,
                        last_rejected_split_diff_fingerprint=first_diff_fingerprint,
                        components={
                            component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                                status=ComponentStatus.RUNNING,
                                attempt_count=3,
                                pid=43210,
                                process_birth_time=1711965600.0,
                                session_ids=["session-1"],
                                workspace_fingerprint_before="fingerprint-before-001",
                                saw_output_in_attempt=False,
                                consecutive_no_progress=0,
                                consecutive_failed_exits=0,
                            )
                        },
                    )
                )

                second_exit_code, second_payload = _run_main(
                    agent_api_main,
                    [
                        "request-split",
                        "--split-bundle-ref",
                        str(second_bundle),
                    ],
                )
                if second_exit_code == 0:
                    return _fail("request-split must reject repeated submission after rejection with no new durable progress")
                if str(second_payload.get("reason_code") or "") != "NO_NEW_EFFECTIVE_DIFF_SINCE_LAST_SPLIT_REJECTION":
                    return _fail("duplicate-after-rejection guard must return a stable reason code")
                if "no new effective git diff" not in str(second_payload.get("message") or "").lower():
                    return _fail("duplicate-after-rejection guard must return a human-readable child-facing message")
                if len(store.list_events_after(0)) != 2:
                    return _fail("guarded duplicate request-split must not append a new inbox item")

                (workspace_root / "artifact.txt").write_text("line-1\nline-2\nline-3\n", encoding="utf-8")
                store.upsert_node(
                    NodeRuntimeRecord(
                        node_id="1",
                        parent_node_id="0",
                        child_node_ids=[],
                        workspace_root=str(workspace_root.resolve()),
                        final_effects_file=str((workspace_root / "FINAL_EFFECTS.md").resolve()),
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
                                node_id="1",
                                actor_kind=ActorKind.IMPLEMENTER,
                                attempt_count=3,
                            )
                        ],
                        durable_commit=base_commit,
                        escalated_to_kernel=False,
                        last_rejected_split_diff_fingerprint=first_diff_fingerprint,
                        components={
                            component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                                status=ComponentStatus.RUNNING,
                                attempt_count=3,
                                pid=43210,
                                process_birth_time=1711965600.0,
                                session_ids=["session-1"],
                                workspace_fingerprint_before="fingerprint-before-001",
                                saw_output_in_attempt=True,
                                consecutive_no_progress=0,
                                consecutive_failed_exits=0,
                            )
                        },
                    )
                )

                third_exit_code, third_payload = _run_main(
                    agent_api_main,
                    [
                        "request-split",
                        "--split-bundle-ref",
                        str(third_bundle),
                    ],
                )
                if third_exit_code != 0:
                    return _fail("request-split must allow a new submission after effective git diff changes")
                if str(third_payload.get("durable_commit") or "") != base_commit:
                    return _fail("successful post-progress request-split must preserve the same durable commit baseline")
                if str(third_payload.get("diff_fingerprint") or "") == first_diff_fingerprint:
                    return _fail("post-progress request-split must produce a new diff_fingerprint after workspace changes")
                final_events = store.list_events_after(0)
                if len(final_events) != 3 or final_events[-1].event_type != "RequestSplit":
                    return _fail("post-progress request-split must append a fresh RequestSplit inbox item")
                if not _wait_until(
                    lambda: store.read_last_applied_seq() == int(third_payload["request_seq"]),
                    timeout_seconds=2.0,
                ):
                    return _fail("post-progress request-split must keep waking the running core")

                invalid_exit_code, invalid_payload = _run_main(
                    agent_api_main,
                    [
                        "request-split",
                        "--split-bundle-ref",
                        str(invalid_bundle),
                    ],
                )
                if invalid_exit_code == 0:
                    return _fail("request-split must reject an invalid split bundle directory")
                if str(invalid_payload.get("reason_code") or "") != "INVALID_SPLIT_BUNDLE":
                    return _fail("invalid split bundle must return a stable reason code")
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        workspace_root = Path(tmpdir) / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        _git("init", "-q", cwd=workspace_root)
        _git("config", "user.email", "router-test@example.com", cwd=workspace_root)
        _git("config", "user.name", "Router Test", cwd=workspace_root)
        (workspace_root / "artifact.txt").write_text("line-1\n", encoding="utf-8")
        (workspace_root / "FINAL_EFFECTS.md").write_text("Implement correctly.\n", encoding="utf-8")
        _git("add", "artifact.txt", cwd=workspace_root)
        _git("commit", "-q", "-m", "init", cwd=workspace_root)
        base_commit = subprocess.run(
            ["git", "-C", str(workspace_root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        (workspace_root / "artifact.txt").write_text("line-1\nline-2\n", encoding="utf-8")
        shell_bundle = _write_split_bundle(
            workspace_root,
            bundle_name="split-bundle-shell",
            child_names=["child_1"],
        )
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="2",
                parent_node_id="0",
                child_node_ids=[],
                workspace_root=str(workspace_root.resolve()),
                final_effects_file=str((workspace_root / "FINAL_EFFECTS.md").resolve()),
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
                        node_id="2",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    )
                ],
                durable_commit=base_commit,
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        pid=54321,
                        process_birth_time=1711965600.0,
                        session_ids=["session-shell"],
                        workspace_fingerprint_before="fingerprint-before-shell",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        shell_env = dict(os.environ)
        shell_env.update(
            {
                "LOOP_ROUTER_DB_PATH": str(db_path),
                "LOOP_NODE_ID": "2",
                "LOOP_ACTOR_KIND": "implementer",
                "LOOP_ATTEMPT_COUNT": "1",
            }
        )
        shell_proc = subprocess.run(
            [
                str(ROOT / "scripts" / "routerctl.sh"),
                "request-split",
                "--split-bundle-ref",
                str(shell_bundle),
            ],
            cwd=str(workspace_root),
            env=shell_env,
            capture_output=True,
            text=True,
        )
        if shell_proc.returncode != 0:
            return _fail(
                "routerctl.sh request-split must preserve the actor workspace cwd; "
                f"stdout={shell_proc.stdout!r} stderr={shell_proc.stderr!r}"
            )
        shell_payload = json.loads(shell_proc.stdout)
        if str(shell_payload.get("status") or "") != "ENQUEUED":
            return _fail("routerctl.sh request-split from workspace cwd must enqueue successfully")

    with tempfile.TemporaryDirectory() as tmpdir:
        workspace_root = Path(tmpdir) / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        _git("init", "-q", cwd=workspace_root)
        _git("config", "user.email", "router-test@example.com", cwd=workspace_root)
        _git("config", "user.name", "Router Test", cwd=workspace_root)
        (workspace_root / "artifact.txt").write_text("line-1\n", encoding="utf-8")
        (workspace_root / "FINAL_EFFECTS.md").write_text("Implement correctly.\n", encoding="utf-8")
        _git("add", "artifact.txt", cwd=workspace_root)
        _git("commit", "-q", "-m", "init", cwd=workspace_root)
        missing_baseline_bundle = _write_split_bundle(
            workspace_root,
            bundle_name="split-bundle-missing-baseline",
            child_names=["child_1"],
        )
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="1",
                parent_node_id="0",
                child_node_ids=[],
                workspace_root=str(workspace_root.resolve()),
                final_effects_file=str((workspace_root / "FINAL_EFFECTS.md").resolve()),
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
                        node_id="1",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    )
                ],
                durable_commit="",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.RUNNING,
                        attempt_count=1,
                        pid=12345,
                        process_birth_time=1711965600.0,
                        session_ids=["session-1"],
                        workspace_fingerprint_before="fingerprint-before-001",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        with _env(
            LOOP_ROUTER_DB_PATH=str(db_path),
            LOOP_NODE_ID="1",
            LOOP_ACTOR_KIND="implementer",
            LOOP_ATTEMPT_COUNT="1",
        ), _chdir(workspace_root):
            missing_exit_code, missing_payload = _run_main(
                agent_api_main,
                [
                    "request-split",
                    "--split-bundle-ref",
                    str(missing_baseline_bundle),
                ],
            )
            if missing_exit_code == 0:
                return _fail("request-split must reject when the node has no durable git baseline commit")
            if str(missing_payload.get("reason_code") or "") != "MISSING_DURABLE_COMMIT_BASELINE":
                return _fail("missing durable git baseline must return a stable reason code")
            if "no durable git baseline" not in str(missing_payload.get("message") or "").lower():
                return _fail("missing durable git baseline must return a human-readable child-facing message")
            if store.list_events_after(0):
                return _fail("missing durable git baseline must not append a RequestSplit inbox item")

    print("[loop-system-router-agent-api][PASS] request-split enqueues once and enforces the durable split request contract")
    return 0


def _wait_until(predicate, *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


if __name__ == "__main__":
    raise SystemExit(main())
