"""Agent-facing completion and split API entrypoint for child/kernel actors."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from loop.core import (
    RouterCore,
    RouterResumeError,
    RouterStartError,
    notify_router_wakeup,
    resolve_router_start_inputs,
    router_wakeup_socket_path,
)
from loop.completion import actor_completion_record_path
from loop.evaluator_runtime import (
    active_feedback_path as node_active_feedback_path,
    ai_user_artifacts_dir,
    ai_user_current_result_path,
    checker_current_tasks_path,
    feedback_history_path as node_feedback_history_path,
    reviewer_current_feedback_path,
    reviewer_current_report_path,
    tester_current_result_path,
)
from loop.events import ActorKind, ActorRef, RequestSplit
from loop.node_table import component_key_for_actor
from loop.split_contract import parse_split_bundle
from loop.store import RouterStore
from loop.runtime_noise import (
    is_runtime_noise_path,
    runtime_noise_git_exclude_patterns,
)


_ENV_ROUTER_DB_PATH = "LOOP_ROUTER_DB_PATH"
_ENV_NODE_ID = "LOOP_NODE_ID"
_ENV_ACTOR_KIND = "LOOP_ACTOR_KIND"
_ENV_ATTEMPT_COUNT = "LOOP_ATTEMPT_COUNT"
_ENV_WORKSPACE_ROOT = "LOOP_WORKSPACE_ROOT"
_ENV_TASK_ID = "LOOP_TASK_ID"
_ENV_REQUEST_SEQ = "LOOP_REQUEST_SEQ"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _build_child_actor_from_env(*, allow_kernel: bool = False) -> ActorRef:
    node_id = str(os.environ.get(_ENV_NODE_ID) or "").strip()
    actor_kind_raw = str(os.environ.get(_ENV_ACTOR_KIND) or "").strip()
    attempt_count_raw = str(os.environ.get(_ENV_ATTEMPT_COUNT) or "").strip()
    if not node_id:
        raise ValueError(f"missing required environment variable {_ENV_NODE_ID}")
    if not actor_kind_raw:
        raise ValueError(f"missing required environment variable {_ENV_ACTOR_KIND}")
    if not attempt_count_raw:
        raise ValueError(f"missing required environment variable {_ENV_ATTEMPT_COUNT}")
    actor_kind = ActorKind(actor_kind_raw)
    if actor_kind is ActorKind.KERNEL and not allow_kernel:
        raise ValueError("request-split must originate from a child actor, not kernel")
    try:
        attempt_count = int(attempt_count_raw)
    except ValueError as exc:
        raise ValueError(f"invalid {_ENV_ATTEMPT_COUNT}: {attempt_count_raw!r}") from exc
    return ActorRef(
        node_id=node_id,
        actor_kind=actor_kind,
        attempt_count=attempt_count,
        task_id=(
            str(os.environ.get(_ENV_TASK_ID) or "").strip()
            if actor_kind is ActorKind.EVALUATOR_AI_USER
            else None
        ),
    )


def _resolve_router_db_path(explicit: str | None) -> Path:
    raw = str(explicit or os.environ.get(_ENV_ROUTER_DB_PATH) or "").strip()
    if not raw:
        raise ValueError(
            f"missing router db path; pass --router-db or set {_ENV_ROUTER_DB_PATH}"
        )
    return Path(raw).expanduser().resolve()


def _resolve_running_component(store: RouterStore, actor: ActorRef):
    record = store.load_node(actor.node_id)
    if record is None:
        raise ValueError(f"unknown node {actor.node_id}")
    component = record.components.get(component_key_for_actor(actor))
    if component is None:
        raise ValueError(
            f"node component {actor.actor_kind.value!r} is not present in node_table"
        )
    if int(component.attempt_count) != int(actor.attempt_count):
        raise ValueError(
            "actor attempt_count does not match the current router-owned component state"
        )
    expected_task_id = str(component.task_id or "").strip()
    actor_task_id = str(actor.task_id or "").strip()
    if expected_task_id != actor_task_id:
        raise ValueError("actor task_id does not match the current router-owned component state")
    if actor not in record.current_components:
        raise ValueError(
            f"router does not currently consider {actor.actor_kind.value!r} an active component"
        )
    if str(getattr(component, "status", "") or "") != "ComponentStatus.RUNNING" and str(
        getattr(component, "status", "") or ""
    ) != "running":
        raise ValueError(f"{actor.actor_kind.value!r} is not currently running")
    return record, component


def _resolve_existing_file_ref(raw: str | Path | None, *, label: str) -> Path:
    normalized = str(raw or "").strip()
    if not normalized:
        raise ValueError(f"missing required {label}")
    candidate = Path(normalized).expanduser().resolve()
    if not candidate.exists() or not candidate.is_file():
        raise ValueError(f"{label} must exist and be a regular file")
    return candidate


def _reject_complete(
    *,
    reason_code: str,
    message: str,
    fix_hint: str | None = None,
) -> int:
    payload: dict[str, object] = {
        "accepted": False,
        "exit_code": 2,
        "message": f"complete rejected locally: {message}",
        "reason_code": reason_code,
        "status": "REJECTED",
    }
    if fix_hint:
        payload["fix_hint"] = fix_hint
    return _print_payload(payload)


def _load_json_object(path: Path, *, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} must be valid UTF-8 JSON") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must decode to a JSON object")
    return payload


def _load_dispatched_manifest(record) -> dict[str, object]:
    checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
    if not checker_tasks_ref:
        raise ValueError("current checker task manifest is missing")
    manifest_ref = Path(checker_tasks_ref).expanduser().resolve()
    return _load_json_object(manifest_ref, label="checker_tasks_ref")


def _validate_snapshot_markers(
    *,
    text: str,
    expected_snapshot_id: str,
    label: str,
) -> list[str]:
    if not expected_snapshot_id:
        return []
    reason_lines: list[str] = []
    expected_marker = f"Current dispatched snapshot id: {expected_snapshot_id}"
    if expected_marker not in text:
        reason_lines.append(f"{label} must include `{expected_marker}`")
    snapshot_pattern = re.compile(r"snapshot-[0-9a-f]+")
    for index, line in enumerate(text.splitlines(), start=1):
        line_snapshots = {match.group(0) for match in snapshot_pattern.finditer(line)}
        for snapshot_id in sorted(line_snapshots):
            if snapshot_id == expected_snapshot_id:
                continue
            lowered = line.lower()
            if any(token in lowered for token in ("historical", "stale", "superseded")):
                continue
            reason_lines.append(
                f"{label} line {index} references stale snapshot {snapshot_id} without labeling it historical/stale/superseded"
            )
    return reason_lines


def _canonical_checker_tasks_ref(record) -> Path:
    return checker_current_tasks_path(record.workspace_root, record.node_id)


def _canonical_result_ref(record, actor: ActorRef) -> Path:
    if actor.actor_kind is ActorKind.EVALUATOR_TESTER:
        return tester_current_result_path(record.workspace_root, record.node_id)
    if actor.actor_kind is ActorKind.EVALUATOR_AI_USER:
        return ai_user_current_result_path(
            record.workspace_root,
            record.node_id,
            task_id=str(actor.task_id or "").strip(),
        )
    raise ValueError(f"unsupported result actor kind {actor.actor_kind.value}")


def _print_payload(payload: dict[str, object]) -> int:
    print(json.dumps(payload, indent=2, sort_keys=True))
    return int(payload.get("exit_code", 0) or 0)


def _component_status_payload(component) -> dict[str, object]:
    return {
        "status": getattr(component.status, "value", str(component.status)),
        "attempt_count": int(component.attempt_count),
        "task_id": str(component.task_id or ""),
        "pid": int(component.pid or 0),
        "process_birth_time": component.process_birth_time,
        "session_ids": [str(item) for item in list(component.session_ids)],
        "workspace_fingerprint_before": str(component.workspace_fingerprint_before),
        "saw_output_in_attempt": bool(component.saw_output_in_attempt),
        "consecutive_no_progress": int(component.consecutive_no_progress),
        "consecutive_failed_exits": int(component.consecutive_failed_exits),
    }


def _node_status_payload(record) -> dict[str, object]:
    active_feedback = node_active_feedback_path(record.workspace_root, record.node_id)
    feedback_history = node_feedback_history_path(record.workspace_root, record.node_id)
    components_payload = {
        component_key: _component_status_payload(component)
        for component_key, component in sorted(record.components.items(), key=lambda item: item[0])
    }
    return {
        "node_id": str(record.node_id),
        "parent_node_id": str(record.parent_node_id),
        "child_node_ids": [str(item) for item in list(record.child_node_ids)],
        "workspace_root": str(record.workspace_root),
        "final_effects_file": str(record.final_effects_file),
        "current_components": [
            {
                "actor_kind": actor.actor_kind.value,
                "attempt_count": int(actor.attempt_count),
                "task_id": str(actor.task_id or ""),
            }
            for actor in list(record.current_components)
        ],
        "split_request": int(record.split_request),
        "split_approved": int(record.split_approved),
        "approved_split_request_seq": int(record.approved_split_request_seq),
        "evaluator_phase": str(record.evaluator_phase),
        "checker_tasks_ref": str(record.checker_tasks_ref),
        "reviewer_verdict_kind": str(record.reviewer_verdict_kind),
        "reviewer_report_ref": str(record.reviewer_report_ref),
        "active_feedback_ref": str(active_feedback) if active_feedback.is_file() else "",
        "feedback_history_ref": str(feedback_history) if feedback_history.is_file() else "",
        "pending_prelude_lines": [str(item) for item in list(record.pending_prelude_lines)],
        "durable_commit": str(record.durable_commit),
        "result_commit": str(record.result_commit),
        "escalated_to_kernel": bool(record.escalated_to_kernel),
        "last_rejected_split_diff_fingerprint": str(record.last_rejected_split_diff_fingerprint),
        "task_result_refs": {
            str(task_id): {str(actor_kind): str(ref) for actor_kind, ref in dict(result_map).items()}
            for task_id, result_map in record.task_result_refs.items()
        },
        "components": components_payload,
    }


def _handle_status(args: argparse.Namespace) -> int:
    router_db_path = _resolve_router_db_path(args.router_db)
    store = RouterStore(router_db_path)
    last_applied_seq = int(store.read_last_applied_seq())
    next_pending_events = store.list_events_after(last_applied_seq, limit=20)
    return _print_payload(
        {
            "accepted": True,
            "exit_code": 0,
            "status": "OK",
            "router_db": str(router_db_path),
            "router_meta": {
                "router_status": str(store.read_router_status() or ""),
                "last_applied_seq": last_applied_seq,
                "kernel_session_id": str(store.read_kernel_session_id() or ""),
                "kernel_rollout_path": str(store.read_kernel_rollout_path() or ""),
                "kernel_started_at": str(store.read_kernel_started_at() or ""),
                "router_paused_reason_json": str(store.read_router_paused_reason_json() or ""),
                "router_paused_at": str(store.read_router_paused_at() or ""),
                "router_terminal_reason_json": str(store.read_router_terminal_reason_json() or ""),
                "router_terminal_at": str(store.read_router_terminal_at() or ""),
                "router_completed_result_commit": str(store.read_router_completed_result_commit() or ""),
                "router_completed_report_ref": str(store.read_router_completed_report_ref() or ""),
                "router_completed_at": str(store.read_router_completed_at() or ""),
            },
            "pending_split_reviews": [
                {
                    "node_id": str(pending.node_id),
                    "request_seq": int(pending.request_seq),
                }
                for pending in store.list_pending_split_reviews()
            ],
            "next_pending_events": [
                {
                    "seq": int(item.seq),
                    "event_type": str(item.event_type),
                    "node_id": str(item.node_id),
                    "actor_kind": item.actor_kind.value,
                    "attempt_count": int(item.attempt_count),
                    "recorded_at": item.recorded_at.isoformat(),
                }
                for item in next_pending_events
            ],
            "nodes": [_node_status_payload(record) for record in store.list_nodes()],
        }
    )


def _startup_result_path(router_db_path: Path) -> Path:
    return (router_db_path.parent / "router" / "startup_result.json").resolve()


def _spawn_router_runtime(
    *,
    router_db_path: Path,
    kernel_session_id: str | None = None,
    kernel_rollout_path: str | None = None,
    kernel_started_at: str | None = None,
    final_effects_file: str | None = None,
    startup_result_path: Path,
    resume_only: bool = False,
):
    runtime_cmd = [
        sys.executable,
        "-m",
        "loop.runtime",
        "--router-db",
        str(router_db_path),
        "--startup-result-file",
        str(startup_result_path),
    ]
    if resume_only:
        runtime_cmd.append("--resume-only")
    else:
        runtime_cmd.extend(
            [
                "--kernel-session-id",
                str(kernel_session_id),
                "--kernel-rollout-path",
                str(kernel_rollout_path),
                "--kernel-started-at",
                str(kernel_started_at),
                "--final-effects-file",
                str(final_effects_file),
            ]
        )
    log_dir = (router_db_path.parent / "router").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / "runtime.stdout.txt"
    stderr_path = log_dir / "runtime.stderr.txt"
    stdout_handle = stdout_path.open("ab")
    stderr_handle = stderr_path.open("ab")
    try:
        proc = subprocess.Popen(
            runtime_cmd,
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=stdout_handle,
            stderr=stderr_handle,
            start_new_session=True,
        )
    finally:
        stdout_handle.close()
        stderr_handle.close()
    return proc


def _wait_for_startup_result(
    *,
    startup_result_path: Path,
    runtime_process,
    timeout_seconds: float = 20.0,
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if startup_result_path.exists():
            return json.loads(startup_result_path.read_text(encoding="utf-8"))
        if runtime_process is not None and callable(getattr(runtime_process, "poll", None)):
            exit_code = runtime_process.poll()
            if exit_code is not None:
                break
        time.sleep(0.05)
    return {
        "accepted": False,
        "status": "REJECTED",
        "reason_code": "ROOT_ACTOR_LAUNCH_FAILED",
        "message": (
            "router runtime rejected: did not produce a startup result in time. "
            "Check router/runtime.stderr.txt and retry."
        ),
    }


def _run_git(*args: str, cwd: Path) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        stderr = str(proc.stderr or "").strip()
        raise ValueError(f"git {' '.join(args)} failed in {cwd}: {stderr or 'unknown error'}")
    return str(proc.stdout or "")


def _resolve_workspace_git_root() -> Path:
    try:
        root = _run_git("rev-parse", "--show-toplevel", cwd=Path.cwd()).strip()
    except ValueError as exc:
        raise ValueError("request-split must run inside the child workspace git repository") from exc
    if not root:
        raise ValueError("request-split could not resolve the child workspace git root")
    return Path(root).expanduser().resolve()


def _tracked_diff_text(*, workspace_root: Path, durable_commit: str) -> str:
    args = [
        "diff",
        "--no-ext-diff",
        "--no-color",
        "--find-renames=0",
        str(durable_commit),
        "--",
        ".",
        *[
            f":(exclude){pattern}"
            for pattern in runtime_noise_git_exclude_patterns()
        ],
    ]
    return _run_git(*args, cwd=workspace_root)


def _untracked_snapshot_text(*, workspace_root: Path) -> str:
    raw = _run_git("ls-files", "--others", "--exclude-standard", "--", ".", cwd=workspace_root)
    entries: list[str] = []
    for line in raw.splitlines():
        relpath = str(line or "").strip()
        if not relpath or is_runtime_noise_path(relpath):
            continue
        file_path = (workspace_root / relpath).resolve()
        if file_path.is_dir():
            continue
        digest = hashlib.sha256(file_path.read_bytes()).hexdigest()
        entries.append(f"{relpath}\0{digest}")
    return "\n".join(sorted(entries))


def _compute_split_diff_fingerprint(*, workspace_root: Path, durable_commit: str) -> str:
    tracked_diff = _tracked_diff_text(workspace_root=workspace_root, durable_commit=durable_commit)
    untracked_snapshot = _untracked_snapshot_text(workspace_root=workspace_root)
    digest = hashlib.sha256()
    digest.update(str(durable_commit).encode("utf-8"))
    digest.update(b"\0")
    digest.update(tracked_diff.encode("utf-8"))
    digest.update(b"\0")
    digest.update(untracked_snapshot.encode("utf-8"))
    return digest.hexdigest()


def _resolve_split_bundle(bundle_ref: str | Path) -> Path:
    bundle_dir, _proposal = parse_split_bundle(bundle_ref, error_type=ValueError)
    return bundle_dir


def _snapshot_split_bundle(
    *,
    router_db_path: Path,
    actor: ActorRef,
    split_bundle_dir: Path,
) -> Path:
    snapshots_root = (
        router_db_path.parent
        / "router"
        / "split_bundles"
        / f"node-{actor.node_id}"
    ).resolve()
    snapshots_root.mkdir(parents=True, exist_ok=True)
    snapshot_dir = (snapshots_root / f"request-{uuid.uuid4().hex}").resolve()
    shutil.copytree(split_bundle_dir, snapshot_dir)
    return snapshot_dir


def _write_completion_record(path: Path, payload: dict[str, object]) -> None:
    resolved = Path(path).expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    tmp = resolved.with_suffix(resolved.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(resolved)


def _handle_request_split(args: argparse.Namespace) -> int:
    actor = _build_child_actor_from_env()
    router_db_path = _resolve_router_db_path(args.router_db)
    store = RouterStore(router_db_path)
    workspace_root = _resolve_workspace_git_root()
    try:
        split_bundle_dir = _resolve_split_bundle(args.split_bundle_ref)
    except ValueError as exc:
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": f"request-split rejected locally: {exc}",
                "reason_code": "INVALID_SPLIT_BUNDLE",
                "status": "REJECTED",
            }
        )
    try:
        frozen_bundle_dir = _snapshot_split_bundle(
            router_db_path=router_db_path,
            actor=actor,
            split_bundle_dir=split_bundle_dir,
        )
    except OSError as exc:
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": f"request-split rejected locally: could not snapshot split bundle: {exc}",
                "reason_code": "SPLIT_BUNDLE_SNAPSHOT_FAILED",
                "status": "REJECTED",
            }
        )
    record = store.load_node(actor.node_id)
    if record is None:
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": f"request-split rejected locally: unknown node {actor.node_id}",
                "reason_code": "UNKNOWN_NODE",
                "status": "REJECTED",
            }
        )
    component = record.components.get(actor.actor_kind)
    if component is None:
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": (
                    "request-split rejected locally: node component "
                    f"{actor.actor_kind.value!r} is not present in node_table"
                ),
                "reason_code": "UNKNOWN_COMPONENT",
                "status": "REJECTED",
            }
        )
    if int(component.attempt_count) != int(actor.attempt_count):
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": (
                    "request-split rejected locally: actor attempt_count does not match "
                    "the current router-owned component state"
                ),
                "reason_code": "ATTEMPT_COUNT_MISMATCH",
                "status": "REJECTED",
            }
        )
    if int(record.split_request) == 1:
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": (
                    "request-split rejected locally: this node already has a split request "
                    "waiting for kernel review or queued review processing"
                ),
                "reason_code": "SPLIT_REVIEW_ALREADY_PENDING",
                "status": "REJECTED",
            }
        )

    durable_commit = str(record.durable_commit or "").strip()
    if not durable_commit:
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": (
                    "request-split rejected locally: this node has no durable git baseline commit yet"
                ),
                "reason_code": "MISSING_DURABLE_COMMIT_BASELINE",
                "status": "REJECTED",
            }
        )
    try:
        diff_fingerprint = _compute_split_diff_fingerprint(
            workspace_root=workspace_root,
            durable_commit=durable_commit,
        )
    except ValueError as exc:
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": f"request-split rejected locally: {exc}",
                "reason_code": "INVALID_DURABLE_COMMIT_BASELINE",
                "status": "REJECTED",
            }
        )
    if (
        str(record.last_rejected_split_diff_fingerprint or "").strip()
        and str(record.last_rejected_split_diff_fingerprint or "").strip() == diff_fingerprint
    ):
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        return _print_payload(
            {
                "accepted": False,
                "durable_commit": durable_commit,
                "diff_fingerprint": diff_fingerprint,
                "exit_code": 2,
                "message": (
                    "request-split rejected locally: no new effective git diff since the last "
                    "kernel-rejected split request"
                ),
                "reason_code": "NO_NEW_EFFECTIVE_DIFF_SINCE_LAST_SPLIT_REJECTION",
                "status": "REJECTED",
            }
        )

    request = RequestSplit(
        actor=actor,
        split_bundle_ref=str(frozen_bundle_dir),
        durable_commit=durable_commit,
        diff_fingerprint=diff_fingerprint,
        requested_at=_utc_now(),
    )
    seq = store.reserve_split_request_and_append_event(request)
    if seq is None:
        shutil.rmtree(frozen_bundle_dir, ignore_errors=True)
        refreshed_record = store.load_node(actor.node_id)
        if refreshed_record is not None and int(refreshed_record.split_request) == 1:
            return _print_payload(
                {
                    "accepted": False,
                    "exit_code": 2,
                    "message": (
                        "request-split rejected locally: this node already has a split request "
                        "waiting for kernel review or queued review processing"
                    ),
                    "reason_code": "SPLIT_REVIEW_ALREADY_PENDING",
                    "status": "REJECTED",
                }
            )
        return _print_payload(
            {
                "accepted": False,
                "exit_code": 2,
                "message": (
                    "request-split rejected locally: router-owned component state changed before "
                    "the split request could be reserved"
                ),
                "reason_code": "INVALID_COMPONENT_STATE",
                "status": "REJECTED",
            }
        )
    notify_router_wakeup(router_wakeup_socket_path(router_db_path))
    return _print_payload(
        {
            "accepted": True,
            "attempt_count": actor.attempt_count,
            "durable_commit": durable_commit,
            "diff_fingerprint": diff_fingerprint,
            "exit_code": 0,
            "node_id": actor.node_id,
            "split_bundle_ref": request.split_bundle_ref,
            "request_seq": int(seq),
            "status": "ENQUEUED",
            "workspace_root": str(workspace_root),
        }
    )


def _handle_complete(args: argparse.Namespace) -> int:
    actor = _build_child_actor_from_env(allow_kernel=True)
    router_db_path = _resolve_router_db_path(args.router_db)
    store = RouterStore(router_db_path)
    try:
        record, component = _resolve_running_component(store, actor)
    except ValueError as exc:
        return _reject_complete(reason_code="INVALID_COMPONENT_STATE", message=str(exc))
    completion_path = actor_completion_record_path(
        record.workspace_root,
        actor.actor_kind,
        node_id=actor.node_id,
        task_id=actor.task_id,
    )
    payload: dict[str, object] = {
        "version": 1,
        "node_id": actor.node_id,
        "actor_kind": actor.actor_kind.value,
        "attempt_count": actor.attempt_count,
        "pid": int(component.pid or 0),
        "process_birth_time": component.process_birth_time,
        "completed_at": _utc_now().isoformat(),
    }
    if actor.actor_kind is ActorKind.EVALUATOR_CHECKER:
        try:
            tasks_ref = _resolve_existing_file_ref(args.tasks_ref, label="tasks_ref")
        except ValueError as exc:
            return _reject_complete(
                reason_code="INVALID_CHECKER_TASKS_REF",
                message=str(exc),
            )
        expected_tasks_ref = _canonical_checker_tasks_ref(record)
        if tasks_ref != expected_tasks_ref:
            return _reject_complete(
                reason_code="NONCANONICAL_CHECKER_TASKS_REF",
                message=f"checker tasks_ref must be {expected_tasks_ref}",
                fix_hint="Write the checker manifest to the canonical current path injected by router, then resubmit.",
            )
        try:
            manifest = _load_json_object(tasks_ref, label="tasks_ref")
        except ValueError as exc:
            return _reject_complete(
                reason_code="INVALID_CHECKER_TASKS_REF",
                message=str(exc),
            )
        try:
            version_value = int(manifest.get("version") or 0)
        except (TypeError, ValueError):
            version_value = -1
        if version_value != 2:
            return _reject_complete(
                reason_code="INVALID_CHECKER_MANIFEST_VERSION",
                message="checker manifest must use version=2",
                fix_hint="Rewrite checker/tasks.json as the new single-source manifest with inline task entries.",
            )
        payload["tasks_ref"] = str(tasks_ref)
    elif actor.actor_kind is ActorKind.EVALUATOR_TESTER:
        try:
            result_ref = _resolve_existing_file_ref(args.result_ref, label="result_ref")
        except ValueError as exc:
            return _reject_complete(reason_code="INVALID_TASK_RESULT_REF", message=str(exc))
        expected_result_ref = _canonical_result_ref(record, actor)
        if result_ref != expected_result_ref:
            return _reject_complete(
                reason_code="NONCANONICAL_TESTER_RESULT_REF",
                message=f"tester result_ref must be {expected_result_ref}",
                fix_hint="Write the tester report to the canonical current result path injected by router, then resubmit.",
            )
        try:
            manifest = _load_dispatched_manifest(record)
        except ValueError as exc:
            return _reject_complete(reason_code="MISSING_CHECKER_MANIFEST", message=str(exc))
        text = result_ref.read_text(encoding="utf-8", errors="replace")
        if "Overall verdict:" not in text:
            return _reject_complete(
                reason_code="INVALID_TESTER_RESULT_FORMAT",
                message="tester result must include `Overall verdict:`",
                fix_hint="Rewrite the tester result using the prompt-required sections and include the overall verdict line.",
            )
        snapshot_errors = _validate_snapshot_markers(
            text=text,
            expected_snapshot_id=str(manifest.get("snapshot_id") or "").strip(),
            label="tester result",
        )
        if snapshot_errors:
            return _reject_complete(
                reason_code="STALE_SNAPSHOT_ID",
                message=snapshot_errors[0],
                fix_hint="Rewrite the current-pass tester result to use the injected current snapshot id, and label any older snapshot ids as historical/stale/superseded.",
            )
        payload["result_ref"] = str(result_ref)
    elif actor.actor_kind is ActorKind.EVALUATOR_AI_USER:
        task_id = str(os.environ.get(_ENV_TASK_ID) or "").strip()
        if not task_id:
            return _reject_complete(
                reason_code="INVALID_TASK_ID",
                message="missing LOOP_TASK_ID for evaluator task actor",
            )
        try:
            result_ref = _resolve_existing_file_ref(args.result_ref, label="result_ref")
        except ValueError as exc:
            return _reject_complete(reason_code="INVALID_TASK_RESULT_REF", message=str(exc))
        expected_result_ref = _canonical_result_ref(record, actor)
        if result_ref != expected_result_ref:
            return _reject_complete(
                reason_code="NONCANONICAL_TASK_RESULT_REF",
                message=f"AI-user result_ref must be {expected_result_ref}",
                fix_hint="Write the lane result to the canonical current result path injected by router, and keep screenshots or extra evidence under the injected artifacts directory.",
            )
        try:
            manifest = _load_dispatched_manifest(record)
        except ValueError as exc:
            return _reject_complete(reason_code="MISSING_CHECKER_MANIFEST", message=str(exc))
        text = result_ref.read_text(encoding="utf-8", errors="replace")
        if f"Task id: {task_id}" not in text:
            return _reject_complete(
                reason_code="INVALID_TASK_RESULT_FORMAT",
                message=f"AI-user result must include `Task id: {task_id}`",
                fix_hint="Rewrite the lane result using the prompt-required header lines, including the exact task id line.",
            )
        if "Task verdict:" not in text:
            return _reject_complete(
                reason_code="INVALID_TASK_RESULT_FORMAT",
                message="AI-user result must include `Task verdict:`",
                fix_hint="Rewrite the lane result using the prompt-required sections and include the task verdict line.",
            )
        snapshot_errors = _validate_snapshot_markers(
            text=text,
            expected_snapshot_id=str(manifest.get("snapshot_id") or "").strip(),
            label="AI-user result",
        )
        if snapshot_errors:
            return _reject_complete(
                reason_code="STALE_SNAPSHOT_ID",
                message=snapshot_errors[0],
                fix_hint="Rewrite the current-pass lane result to use the injected current snapshot id, and label any older snapshot ids as historical/stale/superseded.",
            )
        payload["task_id"] = task_id
        payload["result_ref"] = str(result_ref)
    elif actor.actor_kind is ActorKind.EVALUATOR_REVIEWER:
        verdict_kind = str(args.verdict_kind or "").strip().upper()
        if verdict_kind not in {"OK", "IMPLEMENTER_ACTION_REQUIRED", "EVALUATOR_FAULT"}:
            return _reject_complete(
                reason_code="INVALID_REVIEWER_VERDICT",
                message="verdict_kind must be one of OK, IMPLEMENTER_ACTION_REQUIRED, or EVALUATOR_FAULT",
            )
        try:
            report_ref = _resolve_existing_file_ref(args.report_ref, label="report_ref")
        except ValueError as exc:
            return _reject_complete(reason_code="INVALID_REVIEWER_REPORT_REF", message=str(exc))
        expected_report_ref = reviewer_current_report_path(record.workspace_root, record.node_id)
        if report_ref != expected_report_ref:
            return _reject_complete(
                reason_code="NONCANONICAL_REVIEWER_REPORT_REF",
                message=f"reviewer report_ref must be {expected_report_ref}",
                fix_hint="Write the reviewer report to the canonical current report path injected by router, then resubmit.",
            )
        try:
            manifest = _load_dispatched_manifest(record)
        except ValueError as exc:
            return _reject_complete(reason_code="MISSING_CHECKER_MANIFEST", message=str(exc))
        report_text = report_ref.read_text(encoding="utf-8", errors="replace")
        snapshot_errors = _validate_snapshot_markers(
            text=report_text,
            expected_snapshot_id=str(manifest.get("snapshot_id") or "").strip(),
            label="reviewer report",
        )
        if snapshot_errors:
            return _reject_complete(
                reason_code="STALE_SNAPSHOT_ID",
                message=snapshot_errors[0],
                fix_hint="Rewrite the current-pass reviewer report to use the injected current snapshot id, and label any older snapshot ids as historical/stale/superseded.",
            )
        payload["verdict_kind"] = verdict_kind
        payload["report_ref"] = str(report_ref)
        if args.feedback_ref not in {None, ""}:
            try:
                feedback_ref = _resolve_existing_file_ref(args.feedback_ref, label="feedback_ref")
            except ValueError as exc:
                return _reject_complete(reason_code="INVALID_REVIEWER_FEEDBACK_REF", message=str(exc))
            expected_feedback_ref = reviewer_current_feedback_path(record.workspace_root, record.node_id)
            if feedback_ref != expected_feedback_ref:
                return _reject_complete(
                    reason_code="NONCANONICAL_REVIEWER_FEEDBACK_REF",
                    message=f"reviewer feedback_ref must be {expected_feedback_ref}",
                    fix_hint="Write feedback.json to the canonical current feedback path injected by router, then resubmit.",
                )
            try:
                feedback_payload = _load_json_object(feedback_ref, label="feedback_ref")
            except ValueError as exc:
                return _reject_complete(
                    reason_code="INVALID_REVIEWER_FEEDBACK_REF",
                    message=str(exc),
                )
            try:
                feedback_version = int(feedback_payload.get("version") or 0)
            except (TypeError, ValueError):
                feedback_version = -1
            if feedback_version != 2:
                return _reject_complete(
                    reason_code="INVALID_REVIEWER_FEEDBACK_SCHEMA",
                    message="feedback.json must use version=2",
                    fix_hint="Rewrite feedback.json using the prompt-provided version=2 schema.",
                )
            if str(feedback_payload.get("snapshot_id") or "").strip() != str(manifest.get("snapshot_id") or "").strip():
                return _reject_complete(
                    reason_code="STALE_SNAPSHOT_ID",
                    message="feedback.json snapshot_id must match the current dispatched snapshot id",
                    fix_hint="Rewrite feedback.json with the exact current snapshot id injected by router.",
                )
            payload["feedback_ref"] = str(feedback_ref)
    elif actor.actor_kind is ActorKind.KERNEL:
        request_seq = str(os.environ.get(_ENV_REQUEST_SEQ) or "").strip()
        verdict_kind = str(args.verdict_kind or "").strip().upper()
        if request_seq or verdict_kind or args.reason_ref or args.report_ref:
            if verdict_kind not in {"APPROVE", "REJECT"}:
                return _print_payload(
                    {
                        "accepted": False,
                        "exit_code": 2,
                        "message": (
                            "complete rejected locally: verdict_kind must be one of "
                            "APPROVE or REJECT"
                        ),
                        "reason_code": "INVALID_KERNEL_VERDICT",
                        "status": "REJECTED",
                    }
                )
            if not request_seq:
                return _print_payload(
                    {
                        "accepted": False,
                        "exit_code": 2,
                        "message": "complete rejected locally: missing LOOP_REQUEST_SEQ for kernel actor",
                        "reason_code": "INVALID_REQUEST_SEQ",
                        "status": "REJECTED",
                    }
                )
            try:
                payload["request_seq"] = int(request_seq)
            except ValueError:
                return _print_payload(
                    {
                        "accepted": False,
                        "exit_code": 2,
                        "message": f"complete rejected locally: invalid LOOP_REQUEST_SEQ {request_seq!r}",
                        "reason_code": "INVALID_REQUEST_SEQ",
                        "status": "REJECTED",
                    }
                )
            payload["verdict_kind"] = verdict_kind
            if verdict_kind == "REJECT":
                try:
                    reason_ref = _resolve_existing_file_ref(
                        args.reason_ref or args.report_ref,
                        label="reason_ref",
                    )
                except ValueError as exc:
                    return _print_payload(
                        {
                            "accepted": False,
                            "exit_code": 2,
                            "message": f"complete rejected locally: {exc}",
                            "reason_code": "INVALID_KERNEL_REASON_REF",
                            "status": "REJECTED",
                        }
                    )
                payload["reason_ref"] = str(reason_ref)
    _write_completion_record(completion_path, payload)
    return _print_payload(
        {
            "accepted": True,
            "attempt_count": actor.attempt_count,
            "completion_ref": str(completion_path),
            "exit_code": 0,
            "node_id": actor.node_id,
            "status": "RECORDED",
        }
    )


def _handle_start(args: argparse.Namespace) -> int:
    router_db_path = _resolve_router_db_path(args.router_db)
    startup_result_path = _startup_result_path(router_db_path)
    startup_result_path.unlink(missing_ok=True)
    try:
        inputs = resolve_router_start_inputs(
            kernel_session_id=args.kernel_session_id,
            kernel_rollout_path=args.kernel_rollout_path,
            kernel_started_at=args.kernel_started_at,
            final_effects_file=args.final_effects_file,
        )
    except RouterStartError as exc:
        payload = exc.to_payload()
        payload["exit_code"] = 2
        return _print_payload(payload)

    runtime_process = _spawn_router_runtime(
        router_db_path=router_db_path,
        kernel_session_id=inputs.kernel_session_id,
        kernel_rollout_path=str(inputs.kernel_rollout_path),
        kernel_started_at=inputs.kernel_started_at,
        final_effects_file=str(inputs.final_effects_file),
        startup_result_path=startup_result_path,
    )
    payload = _wait_for_startup_result(
        startup_result_path=startup_result_path,
        runtime_process=runtime_process,
    )
    payload["exit_code"] = 0 if payload.get("accepted") else 2
    return _print_payload(payload)


def _handle_resume(args: argparse.Namespace) -> int:
    router_db_path = _resolve_router_db_path(args.router_db)
    startup_result_path = _startup_result_path(router_db_path)
    startup_result_path.unlink(missing_ok=True)
    append_final_effects_ref: Path | None = None
    if args.append_final_effects_ref:
        try:
            append_final_effects_ref = _resolve_existing_file_ref(
                args.append_final_effects_ref,
                label="append_final_effects_ref",
            )
        except ValueError as exc:
            return _print_payload(
                {
                    "accepted": False,
                    "exit_code": 2,
                    "message": f"resume rejected locally: {exc}",
                    "reason_code": "INVALID_APPEND_FINAL_EFFECTS",
                    "status": "REJECTED",
                }
            )
    store = RouterStore(router_db_path)
    core = RouterCore(
        store=store,
        wakeup_socket_path=router_wakeup_socket_path(store.db_path),
    )
    try:
        core.prepare_resume(append_final_effects_ref=append_final_effects_ref)
    except RouterResumeError as exc:
        payload = exc.to_payload()
        payload["exit_code"] = 2
        return _print_payload(payload)

    runtime_process = _spawn_router_runtime(
        router_db_path=router_db_path,
        startup_result_path=startup_result_path,
        resume_only=True,
    )
    payload = _wait_for_startup_result(
        startup_result_path=startup_result_path,
        runtime_process=runtime_process,
    )
    payload["exit_code"] = 0 if payload.get("accepted") else 2
    return _print_payload(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Agent-facing completion and split API entrypoint for child/kernel actors."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    request_split = subparsers.add_parser(
        "request-split",
        help="Submit a child split request into the router inbox.",
    )
    request_split.add_argument("--router-db")
    request_split.add_argument("--split-bundle-ref", required=True)
    request_split.set_defaults(handler=_handle_request_split)

    complete = subparsers.add_parser(
        "complete",
        help="Record router-owned completion metadata for the current actor attempt.",
    )
    complete.add_argument("--router-db")
    complete.add_argument("--tasks-ref")
    complete.add_argument("--result-ref")
    complete.add_argument("--verdict-kind")
    complete.add_argument("--report-ref")
    complete.add_argument("--feedback-ref")
    complete.add_argument("--reason-ref")
    complete.set_defaults(handler=_handle_complete)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        raise SystemExit("router agent API command handler is not implemented")
    return int(handler(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
