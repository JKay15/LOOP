"""Runtime-owned repo-scoped supervision for evaluator submissions."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from loop_product.evaluator_authority import evaluator_run_is_inflight, load_latest_evaluator_run_state
from loop_product.protocols.evaluator import EvaluatorNodeSubmission, EvaluatorResult
from .evaluator_client import (
    _load_submission_ref,
    _same_run_recovery_delay_s,
    run_evaluator_node_until_terminal,
)
from loop_product.runtime_paths import require_runtime_root
from loop_product.state_io import write_json_read_only

_DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS = 40
_STARTUP_TIMEOUT_S = 5.0
_REACTOR_CONTROL_LOOP_S = 0.05
_REACTOR_IDLE_BACKOFF_S = 0.5
_REACTOR_RUNTIME_HEARTBEAT_S = 1.0
_PROVIDER_CAPACITY_TIMESTAMP_RE = re.compile(r"(20\d{2}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_json_read_only(path, payload)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _pid_alive(pid: int) -> bool:
    try:
        candidate = int(pid)
    except (TypeError, ValueError):
        return False
    if candidate <= 0:
        return False
    try:
        os.kill(candidate, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _pid_command_line(pid: int) -> str:
    try:
        proc = subprocess.run(
            ["ps", "-ww", "-p", str(int(pid)), "-o", "command="],
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception:
        return ""
    if proc.returncode != 0:
        return ""
    return str(proc.stdout or "").strip()


def _command_flag_value(command_line: str, flag: str) -> str:
    normalized_flag = str(flag or "").strip()
    if not normalized_flag:
        return ""
    tokens = [token for token in str(command_line or "").split() if token]
    for index, token in enumerate(tokens):
        if token == normalized_flag:
            if index + 1 < len(tokens):
                return str(tokens[index + 1] or "").strip()
            return ""
        if token.startswith(f"{normalized_flag}="):
            return str(token.split("=", 1)[1] or "").strip()
    return ""


def repo_evaluator_supervision_root(*, state_root: str | Path) -> Path:
    return (require_runtime_root(Path(state_root).expanduser().resolve()) / "artifacts" / "runtime" / "repo_evaluator_supervision").resolve()


def repo_evaluator_supervision_runtime_ref(*, repo_evaluator_supervision_root: str | Path) -> Path:
    return Path(repo_evaluator_supervision_root).expanduser().resolve() / "RepoEvaluatorSupervisionRuntime.json"


def repo_evaluator_supervision_registry_ref(*, repo_evaluator_supervision_root: str | Path) -> Path:
    return Path(repo_evaluator_supervision_root).expanduser().resolve() / "TrackedEvaluators.json"


def repo_evaluator_supervision_log_ref(*, repo_evaluator_supervision_root: str | Path) -> Path:
    return Path(repo_evaluator_supervision_root).expanduser().resolve() / "RepoEvaluatorSupervision.log"


def repo_evaluator_provider_retry_queue_ref(*, repo_evaluator_supervision_root: str | Path) -> Path:
    return Path(repo_evaluator_supervision_root).expanduser().resolve() / "ProviderRetryQueue.json"


def repo_evaluator_supervision_spawn_lock_ref(*, repo_evaluator_supervision_root: str | Path) -> Path:
    return Path(repo_evaluator_supervision_root).expanduser().resolve() / "RepoEvaluatorSupervision.lock"


def _repo_evaluator_supervision_runtime_payload(
    *,
    repo_evaluator_supervision_root: Path,
    tracked_submission_refs: list[str],
    worker_states: dict[str, str],
    started_at_utc: str,
) -> dict[str, Any]:
    return {
        "schema": "loop_product.repo_scoped_evaluator_supervision_runtime",
        "runtime_kind": "repo_scoped_evaluator_supervision_reactor",
        "repo_evaluator_supervision_root": str(repo_evaluator_supervision_root),
        "pid": int(os.getpid()),
        "tracked_submission_refs": [str(item) for item in tracked_submission_refs if str(item).strip()],
        "worker_states": {str(k): str(v) for k, v in dict(worker_states or {}).items()},
        "started_at_utc": str(started_at_utc or _now_iso()),
        "log_ref": str(repo_evaluator_supervision_log_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root)),
        "registry_ref": str(repo_evaluator_supervision_registry_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root)),
    }


def _repo_evaluator_supervision_matches_process(payload: dict[str, Any]) -> bool:
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    root = str(payload.get("repo_evaluator_supervision_root") or "").strip()
    if not root:
        return False
    return (
        "loop_product.loop.evaluator_supervision" in command_line
        and "--run-repo-evaluator-supervision-reactor" in command_line
        and _command_flag_value(command_line, "--repo-evaluator-supervision-root") == root
    )


def live_repo_evaluator_supervision_runtime(*, repo_evaluator_supervision_root: str | Path) -> dict[str, Any] | None:
    runtime_ref = repo_evaluator_supervision_runtime_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root)
    if not runtime_ref.exists():
        return None
    try:
        payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        return None
    if not _repo_evaluator_supervision_matches_process(payload):
        runtime_ref.unlink(missing_ok=True)
        return None
    return payload


def _load_repo_evaluator_supervision_registry(repo_evaluator_supervision_root: Path) -> dict[str, Any]:
    registry_ref = repo_evaluator_supervision_registry_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root)
    if not registry_ref.exists():
        return {
            "schema": "loop_product.repo_scoped_evaluator_supervision_registry",
            "repo_evaluator_supervision_root": str(repo_evaluator_supervision_root),
            "tracked_submissions": {},
        }
    try:
        payload = json.loads(registry_ref.read_text(encoding="utf-8"))
    except Exception:
        return {
            "schema": "loop_product.repo_scoped_evaluator_supervision_registry",
            "repo_evaluator_supervision_root": str(repo_evaluator_supervision_root),
            "tracked_submissions": {},
        }
    return {
        "schema": "loop_product.repo_scoped_evaluator_supervision_registry",
        "repo_evaluator_supervision_root": str(repo_evaluator_supervision_root),
        "tracked_submissions": dict(payload.get("tracked_submissions") or {}),
    }


def load_provider_retry_queue(*, repo_evaluator_supervision_root: str | Path) -> dict[str, Any]:
    root = Path(repo_evaluator_supervision_root).expanduser().resolve()
    queue_ref = repo_evaluator_provider_retry_queue_ref(repo_evaluator_supervision_root=root)
    if not queue_ref.exists():
        return {
            "schema": "loop_product.repo_scoped_evaluator_provider_retry_queue",
            "repo_evaluator_supervision_root": str(root),
            "queued_submissions": {},
        }
    try:
        payload = json.loads(queue_ref.read_text(encoding="utf-8"))
    except Exception:
        return {
            "schema": "loop_product.repo_scoped_evaluator_provider_retry_queue",
            "repo_evaluator_supervision_root": str(root),
            "queued_submissions": {},
        }
    return {
        "schema": "loop_product.repo_scoped_evaluator_provider_retry_queue",
        "repo_evaluator_supervision_root": str(root),
        "queued_submissions": dict(payload.get("queued_submissions") or {}),
    }


def _write_provider_retry_queue(
    *,
    repo_evaluator_supervision_root: Path,
    queued_submissions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema": "loop_product.repo_scoped_evaluator_provider_retry_queue",
        "repo_evaluator_supervision_root": str(repo_evaluator_supervision_root),
        "updated_at_utc": _now_iso(),
        "queued_submissions": queued_submissions,
    }
    _write_json(
        repo_evaluator_provider_retry_queue_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root),
        payload,
    )
    return payload


def _write_repo_evaluator_supervision_registry(
    *,
    repo_evaluator_supervision_root: Path,
    tracked_submissions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema": "loop_product.repo_scoped_evaluator_supervision_registry",
        "repo_evaluator_supervision_root": str(repo_evaluator_supervision_root),
        "updated_at_utc": _now_iso(),
        "tracked_submissions": tracked_submissions,
    }
    _write_json(
        repo_evaluator_supervision_registry_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root),
        payload,
    )
    return payload


def _submission_is_terminal(*, state_root: Path, submission_ref: Path) -> bool:
    if not submission_ref.exists():
        return True
    try:
        submission = _load_submission_ref(submission_ref)
    except Exception:
        return False
    _run_state_ref, run_state = load_latest_evaluator_run_state(
        state_root=state_root,
        node_id=str(submission.target_node_id),
    )
    if not run_state:
        return False
    return not evaluator_run_is_inflight(run_state)


def reconcile_repo_evaluator_supervision_registry(*, repo_evaluator_supervision_root: str | Path) -> dict[str, Any]:
    root = Path(repo_evaluator_supervision_root).expanduser().resolve()
    registry = _load_repo_evaluator_supervision_registry(root)
    tracked = dict(registry.get("tracked_submissions") or {})
    retained: dict[str, dict[str, Any]] = {}
    for submission_ref, entry in tracked.items():
        submission_path = Path(str(submission_ref)).expanduser().resolve()
        state_root = require_runtime_root(Path(str(dict(entry).get("state_root") or "")).expanduser().resolve())
        if _submission_is_terminal(state_root=state_root, submission_ref=submission_path):
            continue
        retained[str(submission_path)] = dict(entry or {})
    return _write_repo_evaluator_supervision_registry(
        repo_evaluator_supervision_root=root,
        tracked_submissions=retained,
    )


def register_submission_for_repo_evaluator_supervision(
    *,
    state_root: str | Path,
    submission_ref: str | Path,
    max_same_run_recovery_attempts: int = _DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS,
) -> dict[str, Any]:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    submission_path = Path(submission_ref).expanduser().resolve()
    root = repo_evaluator_supervision_root(state_root=runtime_root)
    root.mkdir(parents=True, exist_ok=True)
    registry = reconcile_repo_evaluator_supervision_registry(repo_evaluator_supervision_root=root)
    tracked = dict(registry.get("tracked_submissions") or {})
    tracked[str(submission_path)] = {
        "submission_ref": str(submission_path),
        "state_root": str(runtime_root),
        "max_same_run_recovery_attempts": int(max(0, int(max_same_run_recovery_attempts))),
        "registered_at_utc": _now_iso(),
    }
    return _write_repo_evaluator_supervision_registry(
        repo_evaluator_supervision_root=root,
        tracked_submissions=tracked,
    )


def _issue_delay_s(*, result: EvaluatorResult, outer_failure_count: int) -> float:
    return max(1.0, _same_run_recovery_delay_s(result=result, failure_count=max(1, int(outer_failure_count))))


def _provider_capacity_recovery_cooldown_s(*, prior_attempt_count: int) -> float:
    attempts = max(0, int(prior_attempt_count))
    if attempts <= 0:
        return 0.0
    return 30.0


def _provider_issue_timestamp_utc(message: str) -> datetime | None:
    match = _PROVIDER_CAPACITY_TIMESTAMP_RE.search(str(message or ""))
    if match is None:
        return None
    try:
        return datetime.fromisoformat(str(match.group(1)).replace("Z", "+00:00"))
    except ValueError:
        return None


def _retryable_submission_cooldown_s(
    *,
    state_root: Path,
    submission_ref: Path,
    now_utc: datetime | None = None,
) -> float:
    try:
        submission = _load_submission_ref(submission_ref)
    except Exception:
        return 0.0
    target_node_id = str(getattr(submission, "target_node_id", "") or "")
    if not target_node_id:
        return 0.0
    _run_state_ref, run_state = load_latest_evaluator_run_state(
        state_root=state_root,
        node_id=target_node_id,
    )
    if not run_state:
        return 0.0
    checker_state = dict(run_state.get("checker") or {})
    checker_error = dict(checker_state.get("error") or {})
    run_status = str(run_state.get("status") or "").strip().upper()
    checker_status = str(checker_state.get("status") or "").strip().upper()
    inflight_or_retryable = evaluator_run_is_inflight(run_state) or (
        run_status == "RECOVERY_REQUIRED" and checker_status == "BLOCKED_RETRYABLE"
    )
    if not inflight_or_retryable:
        return 0.0
    if str(checker_error.get("issue_kind") or "").strip().lower() != "provider_capacity":
        return 0.0
    cooldown_s = _provider_capacity_recovery_cooldown_s(
        prior_attempt_count=int(checker_state.get("attempt_count") or 0),
    )
    if cooldown_s <= 0.0:
        return 0.0
    issue_timestamp = _provider_issue_timestamp_utc(str(checker_error.get("message") or ""))
    if issue_timestamp is None:
        return cooldown_s
    effective_now = now_utc or _now_utc()
    elapsed_s = max(0.0, (effective_now - issue_timestamp).total_seconds())
    return max(0.0, cooldown_s - elapsed_s)


def ensure_submission_provider_retry_queued(
    *,
    state_root: str | Path,
    submission_ref: str | Path,
    repo_evaluator_supervision_root: str | Path,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    submission_path = Path(submission_ref).expanduser().resolve()
    root = Path(repo_evaluator_supervision_root).expanduser().resolve()
    queue = load_provider_retry_queue(repo_evaluator_supervision_root=root)
    queued = dict(queue.get("queued_submissions") or {})
    previous_entry = dict(queued.get(str(submission_path)) or {})
    cooldown_s = _retryable_submission_cooldown_s(
        state_root=runtime_root,
        submission_ref=submission_path,
        now_utc=now_utc,
    )
    if cooldown_s <= 0.0:
        if str(submission_path) in queued:
            queued.pop(str(submission_path), None)
            return _write_provider_retry_queue(
                repo_evaluator_supervision_root=root,
                queued_submissions=queued,
            )
        return queue

    submission: EvaluatorNodeSubmission = _load_submission_ref(submission_path)
    _run_state_ref, run_state = load_latest_evaluator_run_state(
        state_root=runtime_root,
        node_id=str(submission.target_node_id),
    )
    checker_state = dict((run_state or {}).get("checker") or {})
    checker_error = dict(checker_state.get("error") or {})
    effective_now = now_utc or _now_utc()
    ready_after = effective_now + timedelta(seconds=float(cooldown_s))
    queued[str(submission_path)] = {
        "submission_ref": str(submission_path),
        "state_root": str(runtime_root),
        "target_node_id": str(submission.target_node_id),
        "issue_kind": "provider_capacity",
        "attempt_count": int(checker_state.get("attempt_count") or 0),
        "queued_at_utc": str(previous_entry.get("queued_at_utc") or effective_now.isoformat(timespec="seconds")),
        "ready_after_utc": ready_after.isoformat(timespec="seconds"),
        "issue_message": str(checker_error.get("message") or ""),
    }
    return _write_provider_retry_queue(
        repo_evaluator_supervision_root=root,
        queued_submissions=queued,
    )


def pop_due_provider_retry_submission_refs(
    *,
    repo_evaluator_supervision_root: str | Path,
    now_utc: datetime | None = None,
) -> list[str]:
    root = Path(repo_evaluator_supervision_root).expanduser().resolve()
    queue = load_provider_retry_queue(repo_evaluator_supervision_root=root)
    queued = dict(queue.get("queued_submissions") or {})
    effective_now = now_utc or _now_utc()
    due: list[str] = []
    retained: dict[str, dict[str, Any]] = {}
    for submission_ref, entry in queued.items():
        ready_after_raw = str(dict(entry).get("ready_after_utc") or "").strip()
        if ready_after_raw:
            try:
                ready_after = datetime.fromisoformat(ready_after_raw)
            except ValueError:
                ready_after = effective_now
        else:
            ready_after = effective_now
        if ready_after <= effective_now:
            due.append(str(Path(submission_ref).expanduser().resolve()))
            continue
        retained[str(Path(submission_ref).expanduser().resolve())] = dict(entry or {})
    _write_provider_retry_queue(
        repo_evaluator_supervision_root=root,
        queued_submissions=retained,
    )
    return sorted(due)


def _next_provider_retry_wakeup_s(*, repo_evaluator_supervision_root: Path) -> float | None:
    queue = load_provider_retry_queue(repo_evaluator_supervision_root=repo_evaluator_supervision_root)
    queued = dict(queue.get("queued_submissions") or {})
    if not queued:
        return None
    effective_now = _now_utc()
    candidates: list[float] = []
    for entry in queued.values():
        ready_after_raw = str(dict(entry).get("ready_after_utc") or "").strip()
        if not ready_after_raw:
            candidates.append(0.0)
            continue
        try:
            ready_after = datetime.fromisoformat(ready_after_raw)
        except ValueError:
            candidates.append(0.0)
            continue
        candidates.append(max(0.0, (ready_after - effective_now).total_seconds()))
    if not candidates:
        return None
    return min(candidates)


def run_submission_under_runtime_supervision(
    *,
    state_root: str | Path,
    submission_ref: str | Path,
    max_same_run_recovery_attempts: int = _DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS,
) -> tuple[EvaluatorResult, dict[str, str]]:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    submission_path = Path(submission_ref).expanduser().resolve()
    outer_failures = 0
    while True:
        cooldown_s = _retryable_submission_cooldown_s(
            state_root=runtime_root,
            submission_ref=submission_path,
            now_utc=_now_utc(),
        )
        if cooldown_s > 0.0:
            time.sleep(cooldown_s)
        submission = _load_submission_ref(submission_path)
        evaluator_result, runtime_refs = run_evaluator_node_until_terminal(
            state_root=runtime_root,
            submission=submission,
            max_same_run_recovery_attempts=max_same_run_recovery_attempts,
        )
        recovery = dict(evaluator_result.recovery or {})
        if not evaluator_result.retryable or not bool(recovery.get("required")):
            return evaluator_result, runtime_refs
        outer_failures += 1
        time.sleep(_issue_delay_s(result=evaluator_result, outer_failure_count=outer_failures))


@contextmanager
def _repo_evaluator_supervision_runtime_marker(*, repo_evaluator_supervision_root: Path) -> Any:
    runtime_ref = repo_evaluator_supervision_runtime_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    started_at_utc = _now_iso()
    _write_json(
        runtime_ref,
        _repo_evaluator_supervision_runtime_payload(
            repo_evaluator_supervision_root=repo_evaluator_supervision_root,
            tracked_submission_refs=[],
            worker_states={},
            started_at_utc=started_at_utc,
        ),
    )
    try:
        yield runtime_ref, started_at_utc
    finally:
        runtime_ref.unlink(missing_ok=True)


def _update_repo_evaluator_supervision_runtime(
    *,
    repo_evaluator_supervision_root: Path,
    tracked_submission_refs: list[str],
    worker_states: dict[str, str],
    started_at_utc: str,
) -> None:
    _write_json(
        repo_evaluator_supervision_runtime_ref(repo_evaluator_supervision_root=repo_evaluator_supervision_root),
        _repo_evaluator_supervision_runtime_payload(
            repo_evaluator_supervision_root=repo_evaluator_supervision_root,
            tracked_submission_refs=tracked_submission_refs,
            worker_states=worker_states,
            started_at_utc=started_at_utc,
        ),
    )


def _reactor_sleep_s(
    *,
    tracked_submission_refs: list[str],
    worker_states: dict[str, str],
    next_provider_retry_wakeup_s: float | None,
) -> float:
    if not tracked_submission_refs:
        return _REACTOR_IDLE_BACKOFF_S
    if any(str(state).strip().lower() in {"starting", "retrying"} for state in worker_states.values()):
        return _REACTOR_CONTROL_LOOP_S
    if next_provider_retry_wakeup_s is not None:
        return max(_REACTOR_CONTROL_LOOP_S, float(next_provider_retry_wakeup_s))
    return _REACTOR_IDLE_BACKOFF_S


def _submission_worker(
    *,
    state_root: Path,
    submission_ref: Path,
    repo_evaluator_supervision_root: Path,
    max_same_run_recovery_attempts: int,
    worker_states: dict[str, str],
) -> None:
    key = str(submission_ref.resolve())
    worker_states[key] = "running"
    outer_failures = 0
    try:
        submission = _load_submission_ref(submission_ref)
        while True:
            evaluator_result, _runtime_refs = run_evaluator_node_until_terminal(
                state_root=state_root,
                submission=submission,
                max_same_run_recovery_attempts=0,
            )
            recovery = dict(evaluator_result.recovery or {})
            if not evaluator_result.retryable or not bool(recovery.get("required")):
                worker_states[key] = "completed"
                return
            issue_kind = str((dict(evaluator_result.diagnostics or {})).get("issue_kind") or "").strip().lower()
            if issue_kind == "provider_capacity":
                ensure_submission_provider_retry_queued(
                    state_root=state_root,
                    submission_ref=submission_ref,
                    repo_evaluator_supervision_root=repo_evaluator_supervision_root,
                )
                worker_states[key] = "queued"
                return
            outer_failures += 1
            worker_states[key] = "retrying"
            time.sleep(_issue_delay_s(result=evaluator_result, outer_failure_count=outer_failures))
    except Exception:
        worker_states[key] = "failed"


def _run_repo_scoped_evaluator_supervision_reactor(
    *,
    repo_evaluator_supervision_root: str | Path,
) -> int:
    root = Path(repo_evaluator_supervision_root).expanduser().resolve()
    workers: dict[str, threading.Thread] = {}
    worker_states: dict[str, str] = {}
    last_runtime_update = 0.0
    with _repo_evaluator_supervision_runtime_marker(repo_evaluator_supervision_root=root) as (_runtime_ref, started_at_utc):
        while True:
            registry = reconcile_repo_evaluator_supervision_registry(repo_evaluator_supervision_root=root)
            tracked = dict(registry.get("tracked_submissions") or {})
            tracked_refs = sorted(str(item) for item in tracked.keys())
            for submission_ref, entry in tracked.items():
                key = str(Path(submission_ref).expanduser().resolve())
                if key in workers:
                    continue
                ensure_submission_provider_retry_queued(
                    state_root=Path(str(dict(entry).get("state_root") or "")).expanduser().resolve(),
                    submission_ref=key,
                    repo_evaluator_supervision_root=root,
                )
            due_submission_refs = set(
                pop_due_provider_retry_submission_refs(
                    repo_evaluator_supervision_root=root,
                )
            )
            queued_submission_refs = set(
                str(Path(item).expanduser().resolve())
                for item in dict(load_provider_retry_queue(repo_evaluator_supervision_root=root).get("queued_submissions") or {}).keys()
            )

            for submission_ref, worker in list(workers.items()):
                if worker.is_alive():
                    continue
                workers.pop(submission_ref, None)

            for submission_ref, entry in tracked.items():
                key = str(Path(submission_ref).expanduser().resolve())
                if key in workers:
                    continue
                if key in queued_submission_refs and key not in due_submission_refs:
                    worker_states[key] = "queued"
                    continue
                worker_states[key] = "starting"
                worker = threading.Thread(
                    target=_submission_worker,
                    kwargs={
                        "state_root": require_runtime_root(Path(str(dict(entry).get("state_root") or "")).expanduser().resolve()),
                        "submission_ref": Path(key),
                        "repo_evaluator_supervision_root": root,
                        "max_same_run_recovery_attempts": int(dict(entry).get("max_same_run_recovery_attempts") or _DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS),
                        "worker_states": worker_states,
                    },
                    daemon=True,
                    name=f"repo-evaluator-supervision::{Path(key).stem}",
                )
                workers[key] = worker
                worker.start()

            now = time.monotonic()
            if now - last_runtime_update >= _REACTOR_RUNTIME_HEARTBEAT_S:
                _update_repo_evaluator_supervision_runtime(
                    repo_evaluator_supervision_root=root,
                    tracked_submission_refs=tracked_refs,
                    worker_states=worker_states,
                    started_at_utc=started_at_utc,
                )
                last_runtime_update = now

            if not tracked_refs and not workers:
                _update_repo_evaluator_supervision_runtime(
                    repo_evaluator_supervision_root=root,
                    tracked_submission_refs=[],
                    worker_states={},
                    started_at_utc=started_at_utc,
                )
                return 0
            time.sleep(
                _reactor_sleep_s(
                    tracked_submission_refs=tracked_refs,
                    worker_states=worker_states,
                    next_provider_retry_wakeup_s=_next_provider_retry_wakeup_s(
                        repo_evaluator_supervision_root=root,
                    ),
                )
            )


def ensure_evaluator_supervision_running(
    *,
    state_root: str | Path,
    submission_ref: str | Path,
    max_same_run_recovery_attempts: int = _DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS,
    startup_timeout_s: float = _STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    submission_path = Path(submission_ref).expanduser().resolve()
    register_submission_for_repo_evaluator_supervision(
        state_root=runtime_root,
        submission_ref=submission_path,
        max_same_run_recovery_attempts=max_same_run_recovery_attempts,
    )
    root = repo_evaluator_supervision_root(state_root=runtime_root)
    root.mkdir(parents=True, exist_ok=True)
    runtime_ref = repo_evaluator_supervision_runtime_ref(repo_evaluator_supervision_root=root)
    existing = live_repo_evaluator_supervision_runtime(repo_evaluator_supervision_root=root)
    if existing is None:
        spawn_lock = repo_evaluator_supervision_spawn_lock_ref(repo_evaluator_supervision_root=root)
        spawn_lock.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(spawn_lock, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
            existing = live_repo_evaluator_supervision_runtime(repo_evaluator_supervision_root=root)
            if existing is None:
                log_ref = repo_evaluator_supervision_log_ref(repo_evaluator_supervision_root=root)
                with open(log_ref, "ab", buffering=0) as log_file:
                    proc = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "loop_product.loop.evaluator_supervision",
                            "--run-repo-evaluator-supervision-reactor",
                            "--repo-evaluator-supervision-root",
                            str(root),
                        ],
                        cwd=str(runtime_root.parent if runtime_root.name == ".loop" else runtime_root),
                        stdout=log_file,
                        stderr=log_file,
                        start_new_session=True,
                    )
                if proc.poll() is not None:
                    raise RuntimeError("repo-scoped evaluator supervision reactor exited immediately")
        finally:
            try:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    deadline = time.monotonic() + max(0.0, float(startup_timeout_s))
    while time.monotonic() <= deadline:
        payload = live_repo_evaluator_supervision_runtime(repo_evaluator_supervision_root=root)
        if payload is not None:
            return {
                "schema": "loop_product.evaluator_supervision_runtime",
                "runtime_kind": "repo_scoped_evaluator_supervision",
                "repo_evaluator_supervision_root": str(root),
                "submission_ref": str(submission_path),
                "runtime_ref": str(runtime_ref),
                "log_ref": str(repo_evaluator_supervision_log_ref(repo_evaluator_supervision_root=root)),
                "pid": int(payload.get("pid") or 0),
            }
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for evaluator supervision runtime marker at {runtime_ref}")


def wait_for_submission_terminal(
    *,
    state_root: str | Path,
    submission_ref: str | Path,
    poll_interval_s: float = 0.2,
) -> dict[str, Any]:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    submission = _load_submission_ref(Path(submission_ref).expanduser().resolve())
    while True:
        run_state_ref, run_state = load_latest_evaluator_run_state(
            state_root=runtime_root,
            node_id=str(submission.target_node_id),
        )
        if run_state_ref is not None and run_state and not evaluator_run_is_inflight(run_state):
            return {
                "run_state_ref": str(run_state_ref.resolve()),
                "run_state": run_state,
            }
        time.sleep(max(0.05, float(poll_interval_s)))


def supervise_submission_until_terminal(
    *,
    state_root: str | Path,
    submission_ref: str | Path,
    max_same_run_recovery_attempts: int = _DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS,
) -> dict[str, Any]:
    ensure_evaluator_supervision_running(
        state_root=state_root,
        submission_ref=submission_ref,
        max_same_run_recovery_attempts=max_same_run_recovery_attempts,
    )
    return wait_for_submission_terminal(state_root=state_root, submission_ref=submission_ref)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repo-scoped runtime-owned evaluator supervision")
    parser.add_argument("--run-repo-evaluator-supervision-reactor", action="store_true")
    parser.add_argument("--repo-evaluator-supervision-root", default="")
    parser.add_argument("--state-root", default="")
    parser.add_argument("--submission-ref", default="")
    parser.add_argument("--max-same-run-recovery-attempts", type=int, default=_DEFAULT_MAX_SAME_RUN_RECOVERY_ATTEMPTS)
    parser.add_argument("--supervise-until-terminal", action="store_true")
    args = parser.parse_args(argv)

    if args.run_repo_evaluator_supervision_reactor:
        if not str(args.repo_evaluator_supervision_root).strip():
            parser.error("--repo-evaluator-supervision-root is required with --run-repo-evaluator-supervision-reactor")
        return _run_repo_scoped_evaluator_supervision_reactor(
            repo_evaluator_supervision_root=args.repo_evaluator_supervision_root
        )

    if args.supervise_until_terminal:
        if not str(args.state_root).strip() or not str(args.submission_ref).strip():
            parser.error("--state-root and --submission-ref are required with --supervise-until-terminal")
        payload = supervise_submission_until_terminal(
            state_root=args.state_root,
            submission_ref=args.submission_ref,
            max_same_run_recovery_attempts=int(args.max_same_run_recovery_attempts),
        )
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    parser.error("must pass --run-repo-evaluator-supervision-reactor or --supervise-until-terminal")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
