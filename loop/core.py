"""Minimal router core wakeup + split dispatch loop."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import signal
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from loop.completion import (
    actor_completion_record_path,
    is_router_runtime_metadata_path,
    workspace_output_fingerprint,
)
from loop.evaluator_runtime import (
    active_feedback_path as node_active_feedback_path,
    ai_user_accepted_result_path,
    ai_user_artifacts_dir,
    ai_user_current_result_path,
    checker_accepted_tasks_path,
    checker_current_tasks_path,
    feedback_history_path as node_feedback_history_path,
    reviewer_accepted_feedback_path,
    reviewer_accepted_report_path,
    reviewer_current_feedback_path,
    reviewer_current_report_path,
    reviewer_feedback_submission_snapshot_path as reviewer_submission_snapshot_path,
    tester_accepted_result_path,
    tester_current_result_path,
)
from loop.events import (
    ActorKind,
    ApproveSplit,
    KERNEL_NODE_ID,
    RejectSplit,
    TakeoverResolved,
)
from loop.feedback import active_feedback_path, feedback_history_path
from loop.node_table import (
    ComponentStatus,
    NodeRuntimeRecord,
    component_key,
    component_key_for_actor,
    component_keys_for_kind,
)
from loop.proc import ProcSupervisor
from loop.split_contract import SplitProposal, parse_split_bundle
from loop.store import PendingSplitReview, RouterStore, StoredRouterInboxItem
from loop.runtime_noise import is_non_substantive_workspace_path

ACTOR_MAX_NO_PROGRESS_WINDOWS = 5
ACTOR_MAX_FAILED_EXITS = 3
EVALUATOR_AI_USER_MAX_PARALLEL = 10
ROUTER_THREAD_JOIN_TIMEOUT_SECONDS = 2.0
ROOT_NODE_ID = "1"
ROOT_NODE_PARENT_ID = "0"
ROOT_ACTOR_ATTEMPT_COUNT = 1


LOGGER = logging.getLogger(__name__)


def _resolve_workspace_durable_commit(workspace_root: Path) -> str:
    resolved = Path(workspace_root).expanduser().resolve()
    proc = subprocess.run(
        ["git", "-C", str(resolved), "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return ""
    return str(proc.stdout or "").strip()


def _create_workspace_result_commit(
    workspace_root: Path,
    *,
    paths: list[str],
) -> str:
    resolved = Path(workspace_root).expanduser().resolve()
    if not paths:
        return _resolve_workspace_durable_commit(resolved)
    _run_git(resolved, "add", "-A", "--", *paths)
    _run_git(
        resolved,
        "commit",
        "-m",
        "LOOP result commit",
        "--",
        *paths,
    )
    return _resolve_workspace_durable_commit(resolved)


def _run_git(
    workspace_root: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    resolved = Path(workspace_root).expanduser().resolve()
    proc = subprocess.run(
        ["git", "-C", str(resolved), *args],
        capture_output=True,
        text=True,
        env={
            **dict(os.environ),
            "GIT_AUTHOR_NAME": "LOOP Router",
            "GIT_AUTHOR_EMAIL": "loop-router@example.invalid",
            "GIT_COMMITTER_NAME": "LOOP Router",
            "GIT_COMMITTER_EMAIL": "loop-router@example.invalid",
        },
    )
    if check and proc.returncode != 0:
        stderr = str(proc.stderr or "").strip() or str(proc.stdout or "").strip() or "unknown error"
        raise RuntimeError(f"git {' '.join(args)} failed in {resolved}: {stderr}")
    return proc


def _resolve_workspace_git_root(workspace_root: Path) -> Path:
    proc = _run_git(workspace_root, "rev-parse", "--show-toplevel")
    root = str(proc.stdout or "").strip()
    if not root:
        raise RuntimeError(f"could not resolve git root for {Path(workspace_root).expanduser().resolve()}")
    return Path(root).expanduser().resolve()


def _substantive_git_dirty_paths(workspace_root: Path) -> list[str]:
    resolved = Path(workspace_root).expanduser().resolve()
    paths: set[str] = set()
    for args in (
        ("diff", "--name-only", "--cached", "HEAD", "--"),
        ("diff", "--name-only", "HEAD", "--"),
        ("ls-files", "--others", "--exclude-standard", "--"),
    ):
        proc = _run_git(resolved, *args)
        for raw_line in str(proc.stdout or "").splitlines():
            relpath = str(raw_line or "").strip()
            if not relpath:
                continue
            if is_non_substantive_workspace_path(relpath) or is_router_runtime_metadata_path(relpath):
                continue
            paths.add(relpath)
    return sorted(paths)


def _relative_path_within_workspace(*, workspace_root: Path, file_path: Path) -> Path:
    resolved_workspace = Path(workspace_root).expanduser().resolve()
    resolved_file = Path(file_path).expanduser().resolve()
    return resolved_file.relative_to(resolved_workspace)


def _materialize_workspace_file_into_worktree(
    *,
    source_file: Path,
    target_file: Path,
    missing_error: str,
) -> None:
    source_resolved = Path(source_file).expanduser().resolve()
    if not source_resolved.exists() or not source_resolved.is_file():
        raise InvalidSplitBundleError(missing_error)
    target_resolved = Path(target_file).expanduser().resolve()
    target_resolved.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_resolved, target_resolved)


class RouterBusinessError(Exception):
    """Typed business-level router failure that should not crash worker threads."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code)
        self.message = str(message)

    def __str__(self) -> str:
        return self.message


class RouterStartError(RouterBusinessError):
    """Stable router start rejection."""

    def to_payload(self) -> dict[str, object]:
        return {
            "accepted": False,
            "status": "REJECTED",
            "reason_code": self.reason_code,
            "message": self.message,
        }


class RouterResumeError(RouterBusinessError):
    """Stable router resume rejection."""

    def to_payload(self) -> dict[str, object]:
        return {
            "accepted": False,
            "status": "REJECTED",
            "reason_code": self.reason_code,
            "message": self.message,
        }


class RouterPauseError(RouterBusinessError):
    """Stable router pause rejection."""

    def to_payload(self) -> dict[str, object]:
        return {
            "accepted": False,
            "status": "REJECTED",
            "reason_code": self.reason_code,
            "message": self.message,
        }


class InvalidEventPayloadError(RouterBusinessError):
    """Durable inbox payload is malformed for the expected event schema."""

    def __init__(self, message: str) -> None:
        super().__init__("INVALID_EVENT_PAYLOAD", message)


class InvalidCheckerTasksError(RouterBusinessError):
    """Checker task manifest is invalid."""

    def __init__(self, message: str) -> None:
        super().__init__("INVALID_CHECKER_TASKS", message)


class InvalidSplitBundleError(RouterBusinessError):
    """Split bundle directory or proposal manifest is invalid."""

    def __init__(self, message: str) -> None:
        super().__init__("INVALID_SPLIT_BUNDLE", message)


class InvalidReviewerVerdictError(RouterBusinessError):
    """Reviewer verdict payload is structurally invalid."""

    def __init__(self, message: str) -> None:
        super().__init__("INVALID_REVIEWER_VERDICT", message)


class ActorLaunchRejectedError(RouterBusinessError):
    """Actor launch produced a stable rejection instead of a running child."""

    def __init__(self, message: str) -> None:
        super().__init__("ACTOR_LAUNCH_REJECTED", message)


@dataclass(frozen=True)
class RouterStartInputs:
    """Validated router start inputs."""

    kernel_session_id: str
    kernel_rollout_path: Path
    kernel_started_at: str
    final_effects_file: Path


@dataclass(frozen=True)
class ActorLaunchSpec:
    """Generic launch context for one LOOP actor attempt."""

    node_id: str
    parent_node_id: str
    actor_kind: ActorKind
    attempt_count: int
    workspace_root: Path
    final_effects_file: Path
    prompt_text: str
    env: dict[str, str]


@dataclass(frozen=True)
class ActorLaunchResult:
    """Minimal launched actor metadata needed by router."""

    process: Any
    process_birth_time: float | None
    session_id: str
    rollout_path: Path | None


@dataclass(frozen=True)
class ActorCompletionCheck:
    """Validation result for one actor attempt completion."""

    accepted: bool
    payload: dict[str, object] | None
    reason_lines: list[str]
    output_fingerprint_after: str | None = None


def _missing_start_message(missing_fields: list[str]) -> str:
    hints: list[str] = []
    if "kernel_session_id" in missing_fields:
        hints.append(
            "Read kernel_session_id from the CODEX_THREAD_ID environment variable."
        )
    if "kernel_rollout_path" in missing_fields:
        hints.append(
            "Find kernel_rollout_path by searching ~/.codex/sessions/**/rollout-*{kernel_session_id}.jsonl."
        )
    if "kernel_started_at" in missing_fields:
        hints.append(
            "Read kernel_started_at from the first session_meta.payload.timestamp line in the rollout jsonl."
        )
    if "final_effects_file" in missing_fields:
        hints.append(
            "Produce the final effects file first, then call start again with final_effects_file."
        )
    joined_missing = ", ".join(missing_fields)
    return (
        f"router start rejected: missing {joined_missing}. As the kernel Codex agent, "
        + " ".join(hints)
        + " Then call start again with kernel_session_id, kernel_rollout_path, "
        + "kernel_started_at, and final_effects_file."
    )


def resolve_router_start_inputs(
    *,
    kernel_session_id: str | None,
    kernel_rollout_path: str | Path | None,
    kernel_started_at: str | None,
    final_effects_file: str | Path | None,
) -> RouterStartInputs:
    """Validate the user-approved router start inputs."""

    normalized_kernel_session_id = str(kernel_session_id or "").strip()
    normalized_kernel_started_at = str(kernel_started_at or "").strip()
    normalized_kernel_rollout_path = str(kernel_rollout_path or "").strip()
    normalized_final_effects_file = str(final_effects_file or "").strip()

    missing_fields: list[str] = []
    if not normalized_kernel_session_id:
        missing_fields.append("kernel_session_id")
    if not normalized_kernel_rollout_path:
        missing_fields.append("kernel_rollout_path")
    if not normalized_kernel_started_at:
        missing_fields.append("kernel_started_at")
    if not normalized_final_effects_file:
        missing_fields.append("final_effects_file")
    if missing_fields:
        raise RouterStartError(
            reason_code="MISSING_KERNEL_BOOTSTRAP_INPUT",
            message=_missing_start_message(missing_fields),
        )

    resolved_kernel_rollout_path = Path(normalized_kernel_rollout_path).expanduser().resolve()
    if not resolved_kernel_rollout_path.exists() or not resolved_kernel_rollout_path.is_file():
        raise RouterStartError(
            reason_code="INVALID_KERNEL_BOOTSTRAP_INPUT",
            message=(
                "router start rejected: kernel_rollout_path does not exist or is not a file. "
                "As the kernel Codex agent, locate your rollout jsonl under "
                "~/.codex/sessions/**/rollout-*{kernel_session_id}.jsonl and call start again."
            ),
        )

    resolved_final_effects_file = Path(normalized_final_effects_file).expanduser().resolve()
    if not resolved_final_effects_file.exists() or not resolved_final_effects_file.is_file():
        raise RouterStartError(
            reason_code="INVALID_FINAL_EFFECTS_FILE",
            message=(
                "router start rejected: final_effects_file does not exist or is not a regular file. "
                "Produce the final effects file first, then call start again with final_effects_file."
            ),
        )

    return RouterStartInputs(
        kernel_session_id=normalized_kernel_session_id,
        kernel_rollout_path=resolved_kernel_rollout_path,
        kernel_started_at=normalized_kernel_started_at,
        final_effects_file=resolved_final_effects_file,
    )


def router_wakeup_socket_path(router_db_path: str | Path) -> Path:
    """Return the canonical wakeup socket path for one router db."""

    db_path = Path(router_db_path).expanduser().resolve()
    digest = hashlib.sha256(str(db_path).encode("utf-8")).hexdigest()[:16]
    return (Path.home() / ".codex" / "router_ipc" / f"{digest}.sock").resolve()


def notify_router_wakeup(wakeup_socket_path: str | Path) -> bool:
    """Best-effort wakeup notification for a running router core."""

    sock_path = Path(wakeup_socket_path).expanduser().resolve()
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as client:
            client.sendto(b"1", str(sock_path))
        return True
    except OSError:
        return False


class RouterCore:
    """Background router core that drains the durable inbox to empty on wake."""

    def __init__(
        self,
        *,
        store: RouterStore,
        wakeup_socket_path: str | Path,
        placeholder_observer: Callable[[StoredRouterInboxItem], None] | None = None,
        proc_supervisor: ProcSupervisor | None = None,
        actor_launcher: Callable[[ActorLaunchSpec], ActorLaunchResult] | None = None,
        repo_root: str | Path | None = None,
        codex_home: str | Path | None = None,
    ) -> None:
        self._store = store
        self._wakeup_socket_path = Path(wakeup_socket_path).expanduser().resolve()
        self._placeholder_observer = placeholder_observer
        self._proc_supervisor = proc_supervisor
        self._actor_launcher = actor_launcher
        self._repo_root = Path(repo_root or Path(__file__).resolve().parents[1]).expanduser().resolve()
        self._codex_home = Path(codex_home or (Path.home() / ".codex")).expanduser().resolve()

        self._stop_requested = False
        self._thread: threading.Thread | None = None
        self._socket: socket.socket | None = None
        self._lock = threading.Lock()
        self._split_approved_output_counts: dict[tuple[str, str, int], int] = {}

    @property
    def wakeup_socket_path(self) -> Path:
        return self._wakeup_socket_path

    def start(self) -> None:
        """Start the background drain loop once."""

        request_terminal_shutdown = False
        with self._lock:
            if self._thread is not None:
                return
            self._stop_requested = False
            self._bind_socket()
            self._split_approved_output_counts.clear()
            self._ensure_proc_supervisor().start()
            request_terminal_shutdown = str(self._store.read_router_status() or "").strip() in {
                "paused",
                "terminal_failed",
                "completed",
            }
            if not request_terminal_shutdown:
                self._startup_running_actor_sweep()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="router-core",
            )
            self._thread.start()
        if request_terminal_shutdown:
            self._request_router_terminal_shutdown()

    def stop(self) -> None:
        """Request stop, drain any remaining inbox work, and clean up the socket."""

        with self._lock:
            self._stop_requested = True
            core_thread = self._thread
        notify_router_wakeup(self._wakeup_socket_path)
        if core_thread is not None:
            core_thread.join(timeout=ROUTER_THREAD_JOIN_TIMEOUT_SECONDS)
        core_thread_alive = bool(core_thread is not None and core_thread.is_alive())
        if core_thread_alive:
            LOGGER.warning("router core thread did not exit within %.1fs", ROUTER_THREAD_JOIN_TIMEOUT_SECONDS)
        if self._proc_supervisor is not None:
            self._proc_supervisor.stop()
        if not core_thread_alive:
            self._cleanup_socket()
        with self._lock:
            if not core_thread_alive:
                self._thread = None
        if not core_thread_alive:
            self._split_approved_output_counts.clear()

    def start_router(
        self,
        *,
        kernel_session_id: str | None,
        kernel_rollout_path: str | Path | None,
        kernel_started_at: str | None,
        final_effects_file: str | Path | None,
    ) -> dict[str, object]:
        """Materialize the first LOOP node, launch its actor, and enter steady state."""

        inputs = resolve_router_start_inputs(
            kernel_session_id=kernel_session_id,
            kernel_rollout_path=kernel_rollout_path,
            kernel_started_at=kernel_started_at,
            final_effects_file=final_effects_file,
        )
        if self._store.load_node(ROOT_NODE_ID) is not None:
            raise RouterStartError(
                reason_code="ROOT_NODE_MATERIALIZATION_FAILED",
                message="router start rejected: root node 1 already exists in node_table.",
            )
        spec = self._build_actor_launch_spec(
            node_id=ROOT_NODE_ID,
            parent_node_id=ROOT_NODE_PARENT_ID,
            actor_kind=ActorKind.IMPLEMENTER,
            attempt_count=ROOT_ACTOR_ATTEMPT_COUNT,
            final_effects_file=inputs.final_effects_file,
        )
        proc_supervisor = self._ensure_proc_supervisor()
        previous_kernel_bootstrap = (
            self._store.read_kernel_session_id(),
            self._store.read_kernel_rollout_path(),
            self._store.read_kernel_started_at(),
        )
        launch: ActorLaunchResult | None = None
        launch_committed = False
        try:
            self._store.write_kernel_bootstrap_info(
                kernel_session_id=inputs.kernel_session_id,
                kernel_rollout_path=str(inputs.kernel_rollout_path),
                kernel_started_at=inputs.kernel_started_at,
            )
            _record, launch = self._launch_and_commit_actor(spec=spec, base_record=None)
            launch_committed = True
            self.start()
        except Exception as exc:
            if launch_committed and launch is not None:
                self._terminate_process_best_effort(launch.process)
                self._store.delete_node(spec.node_id)
            try:
                self._store.write_kernel_bootstrap_info(
                    kernel_session_id=previous_kernel_bootstrap[0],
                    kernel_rollout_path=previous_kernel_bootstrap[1],
                    kernel_started_at=previous_kernel_bootstrap[2],
                )
            except Exception:  # noqa: BLE001
                LOGGER.warning(
                    "failed to restore router kernel bootstrap info during start_router rollback",
                    exc_info=True,
                )
            proc_supervisor.stop()
            if isinstance(exc, ActorLaunchRejectedError):
                raise RouterStartError(
                    reason_code="ROOT_ACTOR_LAUNCH_FAILED",
                    message=f"router start rejected: {exc.message}",
                ) from exc
            raise
        return {
            "accepted": True,
            "status": "STARTED",
            "message": "router started; root implementer launched",
            "kernel_session_id": inputs.kernel_session_id,
            "root_node_id": spec.node_id,
            "root_actor_kind": spec.actor_kind.value,
            "root_attempt_count": spec.attempt_count,
            "root_pid": int(getattr(launch.process, "pid", 0) or 0),
            "root_process_birth_time": launch.process_birth_time,
            "root_session_id": launch.session_id,
            "root_rollout_path": (
                str(Path(launch.rollout_path).expanduser().resolve())
                if launch.rollout_path is not None
                else ""
            ),
        }

    def prepare_resume(
        self,
        *,
        append_final_effects_ref: str | Path | None = None,
    ) -> dict[str, object]:
        router_status = str(self._store.read_router_status() or "").strip()
        append_ref = (
            None
            if append_final_effects_ref is None
            else Path(append_final_effects_ref).expanduser().resolve()
        )
        if append_ref is None:
            if router_status == "paused":
                self._store.clear_router_pause_state()
                return {
                    "accepted": True,
                    "status": "RESUME_PREPARED",
                    "resume_mode": "paused",
                }
            if router_status != "terminal_failed":
                raise RouterResumeError(
                    reason_code="INVALID_RESUME_STATE",
                    message=(
                        "router resume rejected: recovery without appended final effects requires "
                        "router_status=paused or router_status=terminal_failed."
                    ),
                )
            recovered_components = 0
            updated_records: list[NodeRuntimeRecord] = []
            for record in self._store.list_nodes():
                updated_record, recovered_count = self._prepare_terminal_failed_record_resume(record)
                if recovered_count <= 0:
                    continue
                recovered_components += int(recovered_count)
                updated_records.append(updated_record)
            if recovered_components <= 0:
                raise RouterResumeError(
                    reason_code="NOTHING_TO_RESUME",
                    message=(
                        "router resume rejected: terminal_failed run has no recoverable unfinished frontier components."
                    ),
                )
            self._store.clear_router_outcome_state()
            if updated_records:
                self._store.upsert_nodes(updated_records)
            return {
                "accepted": True,
                "status": "RESUME_PREPARED",
                "resume_mode": "terminal_failed",
                "recovered_components": int(recovered_components),
            }

        if not append_ref.exists() or not append_ref.is_file():
            raise RouterResumeError(
                reason_code="INVALID_APPEND_FINAL_EFFECTS",
                message="router resume rejected: append-final-effects-ref must exist and be a regular file.",
            )
        appended_text = append_ref.read_text(encoding="utf-8").strip()
        if not appended_text:
            raise RouterResumeError(
                reason_code="INVALID_APPEND_FINAL_EFFECTS",
                message="router resume rejected: append-final-effects-ref must be non-empty.",
            )
        if router_status == "paused":
            _root_record, final_effects_path = self._resolve_root_append_final_effects_target()
            self._append_final_effects(final_effects_path, append_ref)
            self._store.clear_router_pause_state()
            return {
                "accepted": True,
                "status": "RESUME_PREPARED",
                "resume_mode": "paused_append",
                "final_effects_file": str(final_effects_path),
            }
        if router_status == "terminal_failed":
            recovered_components = 0
            updated_records: list[NodeRuntimeRecord] = []
            for record in self._store.list_nodes():
                updated_record, recovered_count = self._prepare_terminal_failed_record_resume(record)
                if recovered_count <= 0:
                    continue
                recovered_components += int(recovered_count)
                updated_records.append(updated_record)
            if recovered_components <= 0:
                raise RouterResumeError(
                    reason_code="NOTHING_TO_RESUME",
                    message=(
                        "router resume rejected: terminal_failed run has no recoverable unfinished frontier components."
                    ),
                )
            _root_record, final_effects_path = self._resolve_root_append_final_effects_target()
            self._append_final_effects(final_effects_path, append_ref)
            self._store.clear_router_outcome_state()
            if updated_records:
                self._store.upsert_nodes(updated_records)
            return {
                "accepted": True,
                "status": "RESUME_PREPARED",
                "resume_mode": "terminal_failed_append",
                "recovered_components": int(recovered_components),
                "final_effects_file": str(final_effects_path),
            }
        if router_status != "completed":
            raise RouterResumeError(
                reason_code="INVALID_RESUME_STATE",
                message=(
                    "router resume rejected: append-final-effects recovery requires "
                    "router_status=paused, router_status=terminal_failed, or router_status=completed."
                ),
            )
        completed_source = self._resolve_completed_resume_source_record()
        root_record = self._store.load_node(ROOT_NODE_ID)
        if root_record is None:
            raise RouterResumeError(
                reason_code="MISSING_ROOT_NODE",
                message="router resume rejected: completed run has no durable root node.",
            )
        prior_result_commit = str(completed_source.result_commit or "").strip()
        if not prior_result_commit:
            raise RouterResumeError(
                reason_code="MISSING_RESULT_COMMIT",
                message="router resume rejected: completed run has no durable result_commit to extend from.",
            )
        final_effects_path = Path(completed_source.final_effects_file).expanduser().resolve()
        self._append_final_effects(final_effects_path, append_ref)
        resumed_root = self._prepare_completed_root_resume(
            root_record=root_record,
            completed_source=completed_source,
            final_effects_path=final_effects_path,
        )
        updated_records = [resumed_root]
        kernel_record = self._store.load_node(KERNEL_NODE_ID)
        if kernel_record is not None:
            updated_records.append(self._prepare_completed_kernel_resume(kernel_record))
        self._store.clear_router_outcome_state()
        self._store.upsert_nodes(updated_records)
        return {
            "accepted": True,
            "status": "RESUME_PREPARED",
            "resume_mode": "completed_append",
            "final_effects_file": str(final_effects_path),
            "base_result_commit": prior_result_commit,
        }

    def prepare_pause(
        self,
        *,
        reason: str | None = None,
    ) -> dict[str, object]:
        router_status = str(self._store.read_router_status() or "").strip()
        if router_status == "paused":
            raise RouterPauseError(
                reason_code="INVALID_PAUSE_STATE",
                message="router pause rejected: router_status=paused already.",
            )
        if router_status in {"terminal_failed", "completed"}:
            raise RouterPauseError(
                reason_code="INVALID_PAUSE_STATE",
                message=(
                    "router pause rejected: only active runs can be paused; "
                    "router_status must not be terminal_failed or completed."
                ),
            )
        if not self._store.list_nodes():
            raise RouterPauseError(
                reason_code="NOTHING_TO_PAUSE",
                message="router pause rejected: no durable run state exists yet.",
            )
        reason_payload = {
            "reason": str(reason or "").strip(),
        }
        self._store.write_router_paused_state(
            router_status="paused",
            router_paused_reason_json=json.dumps(reason_payload, sort_keys=True),
            router_paused_at=datetime.now(timezone.utc).isoformat(),
        )
        return {
            "accepted": True,
            "status": "PAUSE_PREPARED",
            "pause_mode": "paused",
        }

    def _resolve_completed_resume_source_record(self) -> NodeRuntimeRecord:
        completed_result_commit = str(self._store.read_router_completed_result_commit() or "").strip()
        completed_report_ref = self._normalize_ref_path(
            str(self._store.read_router_completed_report_ref() or "").strip()
        )
        if not completed_result_commit:
            raise RouterResumeError(
                reason_code="MISSING_RESULT_COMMIT",
                message="router resume rejected: completed run has no durable result_commit to extend from.",
            )
        commit_matches = [
            record
            for record in self._store.list_nodes()
            if str(record.result_commit or "").strip() == completed_result_commit
        ]
        if not commit_matches:
            raise RouterResumeError(
                reason_code="MISSING_COMPLETED_SOURCE",
                message=(
                    "router resume rejected: completed run has no durable node carrying the completed result_commit."
                ),
            )
        if completed_report_ref:
            matched = [
                record
                for record in commit_matches
                if self._normalize_ref_path(str(record.reviewer_report_ref or "").strip())
                == completed_report_ref
            ]
            if len(matched) == 1:
                return matched[0]
            if len(matched) > 1:
                raise RouterResumeError(
                    reason_code="AMBIGUOUS_COMPLETED_SOURCE",
                    message=(
                        "router resume rejected: multiple durable nodes match the completed result/report pair."
                    ),
                )
        if len(commit_matches) == 1:
            return commit_matches[0]
        raise RouterResumeError(
            reason_code="AMBIGUOUS_COMPLETED_SOURCE",
            message=(
                "router resume rejected: multiple durable nodes share the completed result_commit; "
                "cannot determine the final completed source node."
            ),
        )

    def _normalize_ref_path(self, raw_ref: str) -> str:
        raw = str(raw_ref or "").strip()
        if not raw:
            return ""
        return str(Path(raw).expanduser().resolve())

    def _ensure_proc_supervisor(self) -> ProcSupervisor:
        if self._proc_supervisor is None:
            self._proc_supervisor = ProcSupervisor(
                event_sink=self._store.append_event,
                event_notifier=lambda: notify_router_wakeup(self._wakeup_socket_path),
            )
        return self._proc_supervisor

    def _append_final_effects(self, final_effects_path: Path, append_ref: Path) -> None:
        original_text = final_effects_path.read_text(encoding="utf-8").rstrip()
        appended_text = append_ref.read_text(encoding="utf-8").strip()
        merged = (
            f"{original_text}\n\n"
            "## Appended Final Effects\n\n"
            f"{appended_text}\n"
        )
        final_effects_path.write_text(merged, encoding="utf-8")

    def _resolve_root_append_final_effects_target(self) -> tuple[NodeRuntimeRecord, Path]:
        root_record = self._store.load_node(ROOT_NODE_ID)
        if root_record is None:
            raise RouterResumeError(
                reason_code="MISSING_ROOT_NODE",
                message="router resume rejected: run has no durable root node.",
            )
        final_effects_raw = str(root_record.final_effects_file or "").strip()
        if not final_effects_raw:
            raise RouterResumeError(
                reason_code="INVALID_FINAL_EFFECTS_TARGET",
                message="router resume rejected: root FINAL_EFFECTS target is missing.",
            )
        final_effects_path = Path(final_effects_raw).expanduser().resolve()
        if not final_effects_path.exists() or not final_effects_path.is_file():
            raise RouterResumeError(
                reason_code="INVALID_FINAL_EFFECTS_TARGET",
                message=(
                    "router resume rejected: root FINAL_EFFECTS target must exist and be a regular file."
                ),
            )
        return root_record, final_effects_path

    def _prepare_terminal_failed_record_resume(
        self,
        record: NodeRuntimeRecord,
    ) -> tuple[NodeRuntimeRecord, int]:
        if not self._record_is_unfinished_frontier(record):
            return record, 0
        updated_components = dict(record.components)
        recovered_keys: list[str] = []
        for key, component in record.components.items():
            if component.status is not ComponentStatus.TERMINAL_FAILED:
                continue
            updated_components[key] = replace(
                component,
                status=ComponentStatus.INACTIVE,
                consecutive_failed_exits=0,
                consecutive_no_progress=0,
            )
            recovered_keys.append(key)
        if not recovered_keys:
            return record, 0
        next_current_components = [
            actor_ref
            for actor_ref in record.current_components
            if component_key_for_actor(actor_ref) not in set(recovered_keys)
        ]
        return (
            replace(
                record,
                components=updated_components,
                current_components=next_current_components,
                pending_prelude_lines=[
                    "Router resume after manual terminal-failure recovery.",
                ],
            ),
            len(recovered_keys),
        )

    def _prepare_completed_root_resume(
        self,
        *,
        root_record: NodeRuntimeRecord,
        completed_source: NodeRuntimeRecord,
        final_effects_path: Path,
    ) -> NodeRuntimeRecord:
        updated_components = dict(root_record.components)
        implementer_key = component_key(actor_kind=ActorKind.IMPLEMENTER)
        implementer_component = updated_components.get(implementer_key)
        if implementer_component is not None:
            updated_components[implementer_key] = replace(
                implementer_component,
                status=ComponentStatus.INACTIVE,
                consecutive_failed_exits=0,
                consecutive_no_progress=0,
            )
        return replace(
            root_record,
            child_node_ids=[],
            workspace_root=str(Path(completed_source.workspace_root).expanduser().resolve()),
            final_effects_file=str(final_effects_path),
            split_request=0,
            split_approved=0,
            approved_split_request_seq=0,
            durable_commit=str(completed_source.result_commit or "").strip(),
            result_commit="",
            evaluator_phase="",
            checker_tasks_ref="",
            task_result_refs={},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint="",
            pending_prelude_lines=[
                "Router resume after completed run extension.",
                "This node resumes from a previously completed root result.",
                "The current FINAL_EFFECTS.md contains both previously completed obligations and newly appended obligations.",
                "Preserve the previously completed obligations while implementing the newly appended obligations.",
                "If new obligations conflict with previously completed obligations, report the conflict concretely and implement the maximally compatible result honestly.",
            ],
            current_components=[],
            components=updated_components,
        )

    def _prepare_completed_kernel_resume(self, kernel_record: NodeRuntimeRecord) -> NodeRuntimeRecord:
        updated_components = {
            key: replace(
                component,
                status=ComponentStatus.INACTIVE,
                consecutive_failed_exits=0,
                consecutive_no_progress=0,
            )
            for key, component in kernel_record.components.items()
        }
        return replace(
            kernel_record,
            child_node_ids=[],
            split_request=0,
            split_approved=0,
            approved_split_request_seq=0,
            evaluator_phase="",
            checker_tasks_ref="",
            task_result_refs={},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            pending_prelude_lines=[],
            current_components=[],
            result_commit="",
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint="",
            components=updated_components,
        )

    def _terminate_process_best_effort(self, process: Any) -> None:
        terminate = getattr(process, "terminate", None)
        if callable(terminate):
            try:
                terminate()
                return
            except Exception:  # noqa: BLE001
                pass
        pid = int(getattr(process, "pid", 0) or 0)
        if pid <= 0:
            return
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass

    def _actor_ref(
        self,
        *,
        node_id: str,
        actor_kind: ActorKind,
        attempt_count: int,
        task_id: str | None = None,
    ):
        from loop.events import ActorRef

        return ActorRef(
            node_id=str(node_id),
            actor_kind=actor_kind,
            attempt_count=int(attempt_count),
            task_id=str(task_id or "").strip() or None,
        )

    def _build_completion_contract_lines(
        self,
        *,
        actor_kind: ActorKind,
        routerctl: Path,
    ) -> list[str]:
        lines = [
            "Before exiting, you must leave valid completion signals for router-driven state progression.",
        ]
        if actor_kind is ActorKind.IMPLEMENTER:
            lines.extend(
                [
                    f"- If you need split, use: {routerctl} request-split --split-bundle-ref <bundle_dir>",
                    f"- Otherwise, use: {routerctl} complete",
                ]
            )
            return lines
        if actor_kind is ActorKind.EVALUATOR_CHECKER:
            lines.append(
                f"- Write the canonical checker manifest path injected by router, then use: {routerctl} complete --tasks-ref <path>"
            )
            return lines
        if actor_kind is ActorKind.EVALUATOR_TESTER:
            lines.append(
                f"- Write the canonical tester result path injected by router, then use: {routerctl} complete --result-ref <path>"
            )
            return lines
        if actor_kind is ActorKind.EVALUATOR_AI_USER:
            lines.append(
                f"- Write the canonical AI-user lane result path injected by router, then use: {routerctl} complete --result-ref <path>"
            )
            return lines
        if actor_kind is ActorKind.EVALUATOR_REVIEWER:
            lines.append(
                f"- Write the canonical reviewer feedback.json first, then the canonical reviewer report path, then use: {routerctl} complete --verdict-kind <OK|IMPLEMENTER_ACTION_REQUIRED|EVALUATOR_FAULT> --report-ref <path> --feedback-ref <path>"
            )
            return lines
        if actor_kind is ActorKind.KERNEL:
            lines.append(
                f"- If LOOP_REQUEST_SEQ is set, approve with: {routerctl} complete --verdict-kind APPROVE"
            )
            lines.append(
                f"- If LOOP_REQUEST_SEQ is set, reject with a written reason: {routerctl} complete --verdict-kind REJECT --reason-ref <path>"
            )
            lines.append(f"- Otherwise, use: {routerctl} complete")
            return lines
        return lines

    def _build_implementer_prompt_lines(
        self,
        *,
        routerctl: Path,
    ) -> list[str]:
        split_guide = (Path(__file__).resolve().parents[1] / "IMPLEMENTER_SPLIT_GUIDE.md").resolve()
        return [
            "Implementer role:",
            "- Your job is to satisfy FINAL_EFFECTS.md in the current workspace.",
            "- The implementation you leave behind must itself be the finished deliverable, not merely a path toward one.",
            "- Do not treat helper materials, cached references, fetched examples, temporary scaffolds, or copied comparison artifacts as the finished result unless FINAL_EFFECTS.md explicitly allows that.",
            "",
            "Authority order:",
            "1. FINAL_EFFECTS.md defines the actual target.",
            "2. Reviewer feedback and router feedback history provide correction context.",
            "3. Helper/reference material is supporting evidence only; it does not replace FINAL_EFFECTS.md.",
            "",
            "Implementation integrity:",
            "- Before declaring completion, check the cheapest concrete way the work could still be falsely 'done'.",
            "- If that cheap failure mode is still live, do not complete yet.",
            "- The completion claim you submit must match the current workspace state directly.",
            "- Do not rely on hidden manual steps, temporary runtime conditions, or uncommitted follow-up cleanup to make the result seem complete.",
            "",
            "Ground-truth rule:",
            "- If FINAL_EFFECTS.md names a real comparison target, required behavior, or required deliverable form, validate against that target directly.",
            "- Do not substitute helper/reference material for the real target unless FINAL_EFFECTS.md explicitly says it is authoritative.",
            "",
            "Split interface:",
            "- `split` means proposing to kernel that the remaining unfinished scope of the parent FINAL_EFFECTS.md should be decomposed into multiple child nodes, with optional explicit `parent_after_merge` residual work.",
            f"- To request split, write a split bundle directory and call: `{routerctl} request-split --split-bundle-ref <bundle_dir>`",
            "- A split bundle must contain:",
            "  - `proposal.json` with `version = 2`",
            "  - one child FINAL_EFFECTS file for each proposed child",
            "  - optional `parent_after_merge/FINAL_EFFECTS.md` if explicit residual parent work must remain after child merge",
            "- Do not request split for confusion, partial progress, or vague planning.",
            "",
            "Split gate:",
            "- Consider split only after you have already made real implementation progress in the current workspace.",
            "- Split is allowed when the remaining unfinished scope can be partitioned with zero semantic omission into multiple concrete child scopes that can each make meaningful parallel progress before final merge.",
            "- Do not require near-total independence: shared later integration boundaries or a shared central theorem target do not by themselves make split inappropriate.",
            "- Prefer split whenever parallel child progress is reasonably likely to reduce time-to-truth, expose decisive evidence faster, or outperform purely serial continuation.",
            "- If child merge would still leave explicit parent residual work, do not call `complete` until that residual is finished in the merged workspace.",
            f"- Before calling `{routerctl} request-split --split-bundle-ref <bundle_dir>`, you must read the shared split guide at `{split_guide}` because it contains the full split contract and complete `proposal.json v2` schema.",
            "",
            "Post-split behavior:",
            f"- After `{routerctl} request-split --split-bundle-ref <bundle_dir>` succeeds, stop the current attempt. Router and kernel now own the split decision.",
            "- If the split is rejected and you are relaunched on this same node, continue implementing this node instead of trying to re-argue the same split immediately.",
            "- Do not submit another split request after rejection unless there is new effective implementation progress and a materially improved split case.",
            "- If the split is approved, child nodes run first.",
            "- After child work returns, your responsibility is the merged parent workspace, not just the pre-split parent workspace.",
            "- If you are resumed after child completion, finish the merge closeout for the parent node:",
            "  - resolve merge conflicts if needed,",
            "  - incorporate merged child outputs,",
            "  - finish any explicit `parent_after_merge` residual work,",
            f"  - and only then call `{routerctl} complete`.",
            "- Parent completion after a split still means the merged parent workspace honestly satisfies the original parent FINAL_EFFECTS.md.",
            "",
            "Authority-grounded rebuttal rule:",
            "- Active feedback, checker-derived tasks, and reviewer findings are routing inputs, not higher authority than FINAL_EFFECTS.md.",
            "- If a checker-derived task, active finding, or reviewer instruction appears to narrow FINAL_EFFECTS.md beyond what its text actually supports, do not silently comply.",
            "",
            "Required rebuttal behavior:",
            "- Cite the controlling FINAL_EFFECTS.md text.",
            "- State what stronger interpretation is being imposed.",
            "- Explain why that interpretation is a semantic narrowing rather than a necessary operational clarification.",
            "- State the more faithful interpretation you are following.",
            "",
            "Execution rule:",
            "- After an authority-grounded rebuttal, continue with the most faithful implementation allowed by FINAL_EFFECTS.md and current evidence.",
            "- If there is a real conflict between old obligations and new instructions, report the conflict explicitly and implement the maximally compatible result honestly.",
            "- Do not treat checker/task self-consistency as sufficient when it conflicts with the authoritative requirement text.",
        ]

    def _build_kernel_split_review_prompt_lines(self) -> list[str]:
        return [
            "Kernel split-review role:",
            "- You are reviewing one pending implementer split request.",
            "- Approve only when the proposal is both coverage-complete and operationally worthwhile.",
            "- Reject when the proposal is incomplete, ambiguous, structurally unsound, or not worth the extra orchestration cost.",
            "",
            "Authority order:",
            "1. The parent node's FINAL_EFFECTS.md defines the parent's true target.",
            "2. The frozen split bundle's proposal.json is the sole truth of the split proposal.",
            "3. The frozen child FINAL_EFFECTS files and optional parent_after_merge FINAL_EFFECTS file define the proposed delegated/residual scopes.",
            "4. The current parent workspace and current parent git diff provide evidence for whether this split is worth approving now.",
            "",
            "Zero-omission rule:",
            "- Treat the parent FINAL_EFFECTS.md as the authoritative scope.",
            "- coverage_units must account for every still-unfinished requirement, constraint, acceptance condition, and integration dependency from the parent FINAL_EFFECTS.md that matters to honest completion.",
            "- It is not enough to cover the obvious headline tasks; layout constraints, interaction requirements, deliverable-form requirements, and cross-part integration constraints must also be preserved if they remain unfinished.",
            "- Every still-unfinished piece of parent scope must appear in at least one coverage unit.",
            "- If a parent requirement is only partially delegated, the undelegated remainder must be made explicit in parent_after_merge.",
            "- The union of all child FINAL_EFFECTS scopes, plus parent_after_merge scope if present, must preserve the full remaining unfinished meaning of the parent FINAL_EFFECTS.md.",
            "- If any remaining requirement could disappear, become weaker, or become ownerless under the proposal, reject the split.",
            "",
            "Parallelism rules:",
            "- Children must be independently implementable enough that parallel work is likely to produce meaningful progress before final merge; perfect independence is not required.",
            "- Children must not have overlapping ownership of the same coverage unit.",
            "- Children must not be defined so that their normal success path requires repeated simultaneous ownership of the same integration boundary.",
            "- Shared later integration boundaries or a shared central theorem target do not by themselves make a split invalid.",
            "- If two children are expected to make normal, repeated changes across the same tightly shared implementation surface, approval should turn on whether substantial parallel progress still exists before that merge point.",
            "- Reject splits only when overlap or cross-child blocking dominates the normal success path strongly enough that likely integration cost outweighs the benefit of parallel progress.",
            "",
            "System-pressure thresholds:",
            "- If active system node count is below 20, use the normal approval bar.",
            "- If active system node count is 20-49, require clearly independent child scopes.",
            "- If active system node count is 50-99, approve only if the split is unusually clean and likely to reduce a real bottleneck.",
            "- If active system node count is 100 or higher, reject by default.",
            "- As open split-approved node count increases, tighten the approval bar further.",
            "",
            "Parent implementation rule:",
            "- Reject splits that appear to push essentially all real implementation downward while the parent contributes little beyond decomposition.",
            "- Inspect the current parent workspace and current parent git diff against the durable commit baseline.",
            "- If the parent has not made real implementation progress in the current workspace, the approval bar should be much higher.",
            "",
            "Parent-after-merge rule:",
            "- parent_after_merge is allowed only for explicit residual work that truly remains after child outputs are merged.",
            "- parent_after_merge must not be used as a vague catch-all bucket.",
            "- If parent_after_merge exists, its FINAL_EFFECTS file must make that residual scope concrete enough for later implementation and evaluation.",
            "",
            "Required review steps:",
            "1. Read the parent FINAL_EFFECTS.md.",
            "2. Read the frozen proposal.json.",
            "3. Read every child FINAL_EFFECTS file in the frozen bundle.",
            "4. If parent_after_merge exists, read its FINAL_EFFECTS file.",
            "5. Inspect the current parent workspace and the current parent git diff against the durable baseline.",
            "6. Decide whether the proposal is coverage-complete, non-overlapping, actually parallelizable, worth approving now, and still justified under current system pressure.",
            "",
            "Decision rule:",
            "- APPROVE only if both are true:",
            "  1. the proposal is zero-omission and structurally sound;",
            "  2. the split is worth the added orchestration overhead at the current subtree scale.",
            "- Otherwise REJECT.",
        ]

    def _build_kernel_takeover_prompt_lines(self) -> list[str]:
        return [
            "Kernel final-closeout role:",
            "- You are taking over final closeout for the entire LOOP subtree from the current merged workspace state.",
            "- This is not a split-review task.",
            "- Treat the current workspace and FINAL_EFFECTS.md as the authoritative closeout target.",
            "- Make any final integrated implementation changes needed before the closing evaluator wave.",
            "",
            "Closeout rules:",
            "- Do not look for split proposal artifacts unless router explicitly injects LOOP_REQUEST_SEQ.",
            "- Do not widen scope beyond the current FINAL_EFFECTS.md and explicit router prelude instructions.",
            "- Preserve already-working behavior unless a final integrated fix is actually needed.",
            "- Use routerctl complete only when the current workspace itself is your honest final-closeout claim for the full target.",
        ]

    def _build_checker_prompt_lines(
        self,
        *,
        routerctl: Path,
    ) -> list[str]:
        return [
            "You are the LOOP evaluator_checker.",
            "Router actor identity is provided by the environment.",
            "",
            "Checker role:",
            "- Translate the authoritative FINAL_EFFECTS.md into a checker task graph with zero semantic omission.",
            "- You are planning later evaluator work; you are not performing the substantive audit now.",
            "- Prefer the first complete manifest over prolonged refinement.",
            "- This is a single-pass artifact-writing task. Do not use planning tools, todo tools, or progress-commentary updates.",
            "",
            "Single-source-of-truth rule:",
            "- checker/tasks.json is the sole durable evaluator task truth for this wave.",
            "- Do not create parallel authoritative files for obligations, coverage, routing, disposition, manifests, checklists, summaries, certifications, or per-task markdown.",
            "- Do not use task_ref indirection. The task graph itself must contain the full routed task specification.",
            "- Historical checker artifacts are evidence only; they are not authoritative for this wave.",
            "- If router prelude says the previous evaluator wave ended in EVALUATOR_FAULT, treat the injected reviewer report and feedback as explicit correction targets for this checker pass.",
            "- In that case, do not reproduce the prior checker manifest blindly; correct the evaluator-side faults first, then emit the new manifest.",
            "",
            "Checker scope boundary:",
            "- Your job is to operationalize FINAL_EFFECTS.md into a truthful, testable task manifest.",
            "- You may clarify how a requirement should be checked, evidenced, or partitioned into evaluator tasks.",
            "- You may add concrete verification structure, evidence expectations, and execution-level checks when they help evaluators test the authoritative requirement honestly.",
            "- But do not silently narrow or strengthen the meaning of FINAL_EFFECTS.md.",
            "- In particular, do not convert an open-text allowance into a stricter hard cap, exclusive path, or smaller permission set unless that stronger restriction is already clearly supported by the authoritative text.",
            "- Do not reduce a requirement's allowable solution space just because a narrower formulation seems easier to evaluate.",
            "- If a requirement remains genuinely ambiguous after good-faith operational clarification, preserve that ambiguity in the task framing rather than resolving it by silently choosing the stricter interpretation.",
            "",
            "Implementer rebuttal handling:",
            "- The implementer may explicitly argue that a checker-derived task interpretation over-tightens FINAL_EFFECTS.md.",
            "- If such an authority-grounded rebuttal exists in the current workspace or completion materials, do not ignore it.",
            "- Make the task manifest faithful to FINAL_EFFECTS.md, not merely self-consistent with a prior over-tightened checker interpretation.",
            "",
            "Non-negotiable rules:",
            "- Every concrete requirement in FINAL_EFFECTS.md must be covered by at least one checker task.",
            "- Use the smallest task graph that still preserves zero semantic omission.",
            "- Obligation extraction rule: each bullet or numbered item becomes one obligation; each prose paragraph outside lists becomes one obligation.",
            "- Do not subdivide a bullet or paragraph further unless it clearly contains multiple independent required outcomes.",
            "- Grouping is allowed, but semantic omission is not allowed.",
            "- If multiple requirements are grouped into one task, covered_requirement_ids must still name them explicitly.",
            "- If any requirement is only partially covered, the uncovered remainder must be made explicit and assigned to a task.",
            "- If honest closure still depends on unresolved implementation work or unresolved evidence, assign that dependency to a concrete task.",
            "- If evidence is incomplete, encode the uncertainty inside a task goal or blocking_condition instead of broadening the scan.",
            "",
            "Artifact gate policy:",
            "- Distinguish between strong-gate required evidence and weak-gate mirrored paperwork.",
            "- A required evidence item is allowed only if at least one is true:",
            "  1. FINAL_EFFECTS.md directly requires that deliverable;",
            "  2. router or a later actor actually consumes it as durable input;",
            "  3. without it, later actors would lack the sole minimal non-redundant evidence needed for honest pass/fail or honest closeout.",
            "- If none of the above is true, it is weak-gate by default and must not become blocking paperwork.",
            "- Do not require an md/json pair by default.",
            "- Prefer one durable artifact over multiple mirrored artifacts.",
            "- If evidence can be consumed directly from the task graph or later lane result, do not manufacture sidecar truth.",
            "",
            f"Completion interface: write the canonical checker manifest, then use {routerctl} complete --tasks-ref <path>",
            "",
            "Write exactly one durable checker artifact:",
            "1. checker/tasks.json",
            "- Top-level keys exactly:",
            "  - version",
            "  - snapshot_id",
            "  - workspace_root",
            "  - authoritative_final_effects_path",
            "  - obligations",
            "  - tasks",
            "- version must be exactly 2.",
            "- snapshot_id may be empty in the current file; router assigns the accepted dispatched snapshot id.",
            "- obligations must be a list of objects with keys exactly:",
            "  - id",
            "  - section",
            "  - requirement_text",
            "- tasks must be a list of objects with keys exactly:",
            "  - task_id",
            "  - covered_requirement_ids",
            "  - goal",
            "  - blocking_condition",
            "  - task_instructions",
            "  - required_evidence",
            "- task_instructions must contain the full routed task specification needed by the assigned lane.",
            "- required_evidence must stay minimal and strong-gate only.",
            "",
            "Task quality rule:",
            "- Do not collapse the job into 2-3 vague umbrella tasks.",
            "- Task count may vary, but every task must remain concrete and executable.",
            "- Prefer 4-12 tasks unless more are required to avoid semantic omission.",
            "",
            "Finish gate: do not complete until all of the following are true:",
            "- obligations cover FINAL_EFFECTS with zero semantic omission.",
            "- Every obligation_id is covered by at least one task.",
            "- Every task entry is concrete and self-sufficient without task_ref indirection.",
            "- If unresolved implementation or unresolved evidence still blocks honest closure, at least one task explicitly owns that work.",
            "- If all finish-gate conditions hold, stop immediately.",
        ]

    def _build_tester_prompt_lines(self) -> list[str]:
        return [
            "You are the global reviewer for the current evaluator pass.",
            "Your job is to find substantive problems that would make honest approval wrong.",
            "Do not re-implement the task.",
            "Do not downgrade into trivial compliance checking.",
            "Do not assume every run requires the same review dimensions; review only what materially matters for the current FINAL_EFFECTS.",
            "",
            "Authority order:",
            "1. FINAL_EFFECTS.md defines the true goal.",
            "2. checker/tasks.json is the sole evaluator task truth for the current wave.",
            "3. Each lane result file is the sole durable truth for that lane.",
            "4. Local helper/reference files are supporting evidence only; they do not override FINAL_EFFECTS.",
            "",
            "Single-source-of-truth rule:",
            "- Do not treat mirrored checker sidecar docs as parallel truth sources.",
            "- If duplicate sidecar files disagree with checker/tasks.json or lane results, the durable source wins.",
            "- Write one global tester report and no additional authoritative evaluator sidecar docs.",
            "",
            "Required workflow:",
            "1. Partition the current review surface into small review blocks before judging the run.",
            "- Each block must have one primary review question.",
            "- Code block limits:",
            "  - at most 3 files,",
            "  - at most 200 changed lines,",
            "  - at most 3 closely related functions/components.",
            "- If any limit is exceeded, split the block further.",
            "- If you cannot summarize a block as one sentence of the form \"this block verifies whether X is true\", the block is still too large.",
            "",
            "2. Review each block with this fixed sequence:",
            "- State the honest pass condition for the block.",
            "- Inspect the primary evidence in the block.",
            "- Inspect only the minimum adjacent context needed.",
            "- Ask: what is the cheapest concrete way this block could still be failing?",
            "- Run the minimum validation needed to check that failure mode.",
            "- Record only material findings.",
            "",
            "3. Choose review dimensions conditionally, not mechanically.",
            "- Review only the dimensions that materially matter for the current block and current FINAL_EFFECTS.",
            "- Examples include:",
            "  - implementation bugs,",
            "  - behavior bugs,",
            "  - fidelity failures,",
            "  - checker decomposition defects,",
            "  - stale or inconsistent durable truth,",
            "  - closeout-readiness failures.",
            "- Do not force irrelevant review categories onto the run.",
            "",
            "4. Synthesize globally after block review.",
            "- Check whether findings from different blocks combine into a larger failure.",
            "- Check whether the run is honestly ready for approval.",
            "",
            "5. Tag ownership for every finding.",
            "- owner must be one of:",
            "  - implementer",
            "  - checker",
            "  - tester",
            "  - ai_user:<task_id>",
            "  - shared",
            "- If the issue is global but originates from one actor, still name the concrete owner.",
            "",
            "Materiality rule:",
            "- Report only issues that would change an honest pass/fail decision, honest closeout, or correct owner-routing.",
            "- Do not report stylistic preferences, alternative designs, or minor drift that would not materially affect the current task or run outcome.",
            "- Before reporting a finding, confirm all four:",
            "  1. this is a real problem, not just a different acceptable choice;",
            "  2. it matters to the current task or current run;",
            "  3. you have concrete evidence for it;",
            "  4. it is actionable by a specific owner.",
            "",
            "Artifact gate policy:",
            "- Distinguish between strong-gate evidence and weak-gate mirrored paperwork.",
            "- A strong-gate item is blocking only if at least one is true:",
            "  1. FINAL_EFFECTS.md directly requires it as a primary deliverable;",
            "  2. router or a later actor actually consumes it as durable input;",
            "  3. it is the sole minimal non-redundant evidence needed for honest pass/fail or honest closeout.",
            "- If none of the above is true, it is weak-gate by default.",
            "- Missing weak-gate artifacts must not block by themselves.",
            "- Missing weak-gate mirrored paperwork must not block by itself.",
            "- If a checker contract overweights low-value paperwork, report that as a checker-side contract problem.",
            "",
            "Snapshot-writing rule:",
            "- When you write the current tester result, use the exact current dispatched snapshot id injected by router.",
            "- If you mention any older snapshot id, label it explicitly as historical, stale, or superseded.",
            "",
            "Output requirements:",
            "- Write one global tester report.",
            "- Include:",
            "  - overall verdict,",
            "  - findings,",
            "  - owner for each finding,",
            "  - concrete evidence refs,",
            "  - a short global correctness/closeout judgment.",
            "- You are the final judge of your own findings; if you use narrower subreviews or local notes, do not forward them blindly.",
        ]

    def _build_ai_user_prompt_lines(self) -> list[str]:
        return [
            "You are the adversarial validator for the current task.",
            "Your job is to determine whether this task can honestly pass.",
            "You are not the global reviewer.",
            "You are not responsible for judging the whole run.",
            "You are not a checklist robot: the routed task entry is your starting scope, not an excuse to ignore obvious implied checks.",
            "",
            "Authority order:",
            "1. FINAL_EFFECTS.md defines the true goal.",
            "2. The routed task entry from checker/tasks.json defines your direct assignment.",
            "3. Workspace code, runtime behavior, and task-local artifacts provide evidence.",
            "4. Local helper/reference files are supporting evidence only; they do not override FINAL_EFFECTS.",
            "",
            "Single-source-of-truth rule:",
            "- The routed task entry is the only task truth for this lane.",
            "- Your only durable evaluator output is one lane result file.",
            "- Do not create auxiliary manifests, checklists, certification files, mirrored summaries, or sidecar routing notes unless FINAL_EFFECTS directly requires them or router/later actors truly consume them.",
            "",
            "Split-lifecycle rule:",
            "- Use the current node split truth injected by router as the primary split-lifecycle evidence for this node.",
            "- Do not treat historical nodes, historical `parent_node_id` ancestry rows, or the mere presence of other nodes in router.db as proof that the current node split.",
            "- A split or no-split claim for this node must be grounded in current-node split lifecycle evidence, not historical topology alone.",
            "",
            "Artifact gate policy:",
            "- Evaluate the lane using the task's required evidence, not duplicate paperwork.",
            "- A required evidence item is blocking only if at least one is true:",
            "  1. FINAL_EFFECTS.md directly requires it as a primary deliverable;",
            "  2. router or a later actor actually consumes it as durable input;",
            "  3. it is the sole minimal non-redundant evidence needed for honest pass/fail or honest closeout.",
            "- If none of the above is true, it is weak-gate by default.",
            "- Missing weak-gate artifacts must not block by themselves.",
            "- Missing mirrored sidecar paperwork is non-blocking by default unless it is the sole remaining non-redundant evidence needed for an honest verdict.",
            "- If the routed task labels low-value repackaged paperwork as blocking, report that as a checker-side contract problem.",
            "",
            "Required workflow:",
            "1. Read the routed task entry and restate the honest pass condition for this lane.",
            "- State in 1-3 sentences what must be true for this task to pass honestly.",
            "",
            "2. Extract the direct checks explicitly required by the task.",
            "- Write a short direct-check list.",
            "- This list should contain only checks clearly demanded by the routed task entry.",
            "",
            "3. Run the direct checks.",
            "- If the task is about controls or behavior, prefer real interaction.",
            "- If the task is about fidelity, prefer direct visual/source comparison appropriate to the task.",
            "- If the task is about manifests or closeout, inspect the relevant artifacts directly.",
            "",
            "4. Add only the minimum implied checks needed for an honest verdict.",
            "- Add an implied check only if, without it, a pass decision would obviously be fake or under-validated.",
            "- Add at most 3 implied checks.",
            "- For each implied check, briefly justify why it is necessary for honest closure of this task.",
            "- Do not expand into unrelated auditing.",
            "",
            "5. Try to falsify the task before passing it.",
            "- Ask:",
            "  - what is the cheapest concrete way this task could still be failing?",
            "  - what evidence would prove it is not actually complete?",
            "- Check that failure mode directly first.",
            "- If you cannot falsify it, explain what evidence proves pass.",
            "",
            "6. Evaluate closure.",
            "- State whether the lane produced enough concrete, non-redundant evidence for an honest verdict.",
            "- Do not fail the task solely because mirrored sidecar paperwork is absent, stale, or cosmetically imperfect.",
            "",
            "7. If the task itself is under-scoped or misowned, report that explicitly.",
            "- If the implementation is bad, say so.",
            "- If checker assigned the task too narrowly or left a coverage gap, say so.",
            "- Distinguish:",
            "  - task failed because implementation failed",
            "  - task cannot honestly close because checker ownership/scope is wrong",
            "",
            "8. Tag ownership for every finding.",
            "- owner must be one of:",
            "  - implementer",
            "  - checker",
            "  - tester",
            "  - ai_user:<task_id>",
            "  - shared",
            "",
            "Materiality rule:",
            "- Report only issues that would make this task fail honestly, or that reveal a real owner-routing/scope defect blocking honest task closure.",
            "- Do not report stylistic preferences, alternative designs, or minor drift that would not materially affect this task's verdict.",
            "- Before reporting a finding, confirm all four:",
            "  1. this is a real problem, not just a different acceptable choice;",
            "  2. it matters to this task's honest pass/fail;",
            "  3. you have concrete evidence for it;",
            "  4. it is actionable by a specific owner.",
            "",
            "Snapshot-writing rule:",
            "- When you write the current lane result, use the exact current dispatched snapshot id injected by router.",
            "- Do not copy snapshot ids from history into current-pass claims.",
            "- If you mention any older snapshot id, label it explicitly as historical, stale, or superseded.",
            "",
            "Output requirements:",
            "- Write one task result.",
            "- Include:",
            "  - Task id: <current task id>,",
            "  - Current dispatched snapshot id: <current snapshot id>,",
            "  - task verdict,",
            "  - direct checks performed,",
            "  - any implied checks you added and why,",
            "  - concrete evidence,",
            "  - any material findings,",
            "  - owner for each finding,",
            "  - whether the task itself is sufficiently scoped for honest closure.",
        ]

    def _build_reviewer_prompt_lines(self) -> list[str]:
        return [
            "You are the final adjudicator for the current evaluator pass.",
            "Your first priority is to produce the router-readable feedback.json submission.",
            "Your second priority is to write reviewer_report.md for humans.",
            "If feedback.json and reviewer_report.md conflict, feedback.json must be treated as authoritative for routing.",
            "Do not re-run the full tester workload or redo every AI-user lane from scratch; adjudicate from the current evidence.",
            "",
            "Authority order:",
            "1. FINAL_EFFECTS.md defines the true goal.",
            "2. checker/tasks.json is the sole evaluator task truth for the current wave.",
            "3. Each tester result or AI-user lane result is the sole durable truth for that evaluator output.",
            "4. Active feedback and feedback history provide prior context only; they do not override current evidence.",
            "5. If current evidence contradicts an older finding, mark the older finding resolved or superseded in reviewer_report.md and reflect only the still-active findings in feedback.json.",
            "",
            "Single-source-of-truth rule:",
            "- feedback.json is the sole routing truth for this reviewer pass.",
            "- reviewer_report.md is explanatory only and must not introduce active findings absent from feedback.json.",
            "- Do not keep mirrored checker sidecar docs alive as separate semantic sources.",
            "- If a duplicate evaluator file disagrees with checker/tasks.json or the accepted lane results, the durable source wins.",
            "- A stale mirrored file is blocking only if it proves the actual task graph, gate semantics, or closeout semantics are wrong.",
            "",
            "Reviewer adjudication rule:",
            "- Treat FINAL_EFFECTS.md as the authoritative requirement source.",
            "- Treat checker/tasks.json as a derived operationalization, not as a stronger authority than FINAL_EFFECTS.md.",
            "- If an implementer explicitly argues that a checker-derived task or active finding over-tightens FINAL_EFFECTS.md, you must adjudicate that rebuttal explicitly.",
            "",
            "Adjudication steps:",
            "1. Read the controlling FINAL_EFFECTS.md text.",
            "2. Determine whether the checker/task interpretation is an operational clarification or a semantic narrowing.",
            "3. Compare that interpretation against the implementer's authority-grounded rebuttal and the current workspace evidence.",
            "4. State which interpretation governs and why.",
            "",
            "Reviewer prohibition:",
            "- Do not reject or fault an implementation merely because it violates an over-tightened checker/task interpretation that is not actually supported by FINAL_EFFECTS.md.",
            "- Do not treat checker self-consistency as sufficient when it conflicts with the authoritative requirement text.",
            "",
            "Split-lifecycle rule:",
            "- Use the current node split truth injected by router as the primary split-lifecycle evidence for this node.",
            "- Do not treat historical nodes, historical `parent_node_id` ancestry rows, or the mere presence of other nodes in router.db as proof that the current node split.",
            "- Resolve split-related findings against current-node split lifecycle evidence before carrying them forward.",
            "",
            "Artifact gate policy:",
            "- Distinguish between strong-gate and weak-gate artifacts.",
            "- A finding may be `blocking: true` only if at least one is true:",
            "  1. a primary deliverable required by FINAL_EFFECTS.md is still missing or materially wrong;",
            "  2. a durable handoff artifact consumed by router or a later actor is missing or materially wrong;",
            "  3. the sole minimal non-redundant evidence needed for honest pass/fail or honest closeout is missing or materially wrong.",
            "- If none of the above is true, the finding must be treated as weak-gate by default.",
            "- Missing weak-gate artifacts must not block by themselves.",
            "- If checker or a task contract over-classifies weak-gate artifacts as blockers, route that as a checker-side contract problem instead of repeatedly blocking ai_user lanes for low-value paperwork.",
            "- Record weak-gate findings in feedback.json only when they are useful advisory context for a later actor; otherwise leave them out of feedback.json and mention them only in reviewer_report.md.",
            "",
            "Snapshot-writing rule:",
            "- When you write any current-pass reviewer finding, report, or routing decision, use the exact current dispatched snapshot id injected by router.",
            "- Do not copy snapshot ids from history into current-pass claims.",
            "- If you mention any older snapshot id, label it explicitly as historical, stale, or superseded.",
            "",
            "Required workflow:",
            "1. Read the current checker task manifest, tester report(s), AI-user lane result(s), and the minimal workspace/closeout state needed for an honest decision.",
            "2. Read any active feedback or feedback-history paths injected below the ROUTER FEEDBACK SLOT marker.",
            "3. Decide which findings are still active now.",
            "4. Write feedback.json first as the complete current active-finding snapshot for router.",
            "5. Write reviewer_report.md second to explain the adjudication and summarize resolved / still-open / superseded findings.",
            "6. Submit both files through routerctl complete.",
            "",
            "feedback.json is the primary routing artifact.",
            "- feedback.json is a complete snapshot of all findings that are still active after this review pass.",
            "- feedback.json is not an incremental patch.",
            "- Do not omit empty buckets.",
            "- Do not duplicate one finding across multiple owner buckets unless the problem is genuinely shared and must be routed as shared evidence inside reviewer_report.md; feedback.json itself must still choose one concrete owner bucket.",
            "",
            "feedback.json schema requirements:",
            "- Top-level keys must be exactly:",
            "  - version",
            "  - snapshot_id",
            "  - implementer",
            "  - checker",
            "  - tester",
            "  - ai_user",
            "- version must be exactly 2.",
            "- snapshot_id must equal the current dispatched snapshot id.",
            "- implementer/checker/tester must be objects with keys exactly:",
            "  - findings",
            "- ai_user must be a list of objects with keys exactly:",
            "  - task_id",
            "  - findings",
            "- Every finding object must contain exactly:",
            "  - blocking",
            "  - summary",
            "  - evidence_ref",
            "- ai_user.task_id must match a task_id from the current checker manifest.",
            "",
            "feedback.json template:",
            "```json",
            "{",
            '  "version": 2,',
            '  "snapshot_id": "snapshot-abc123",',
            '  "implementer": {',
            '    "findings": [',
            "      {",
            '        "blocking": true,',
            '        "summary": "short actionable statement",',
            '        "evidence_ref": "/abs/path/to/evidence"',
            "      }",
            "    ]",
            "  },",
            '  "checker": {',
            '    "findings": [',
            "      {",
            '        "blocking": false,',
            '        "summary": "non-blocking advisory note about an overweight weak-gate artifact contract",',
            '        "evidence_ref": "/abs/path/to/evidence"',
            "      }",
            "    ]",
            "  },",
            '  "tester": {',
            '    "findings": []',
            "  },",
            '  "ai_user": [',
            "    {",
            '      "task_id": "task-01",',
            '      "findings": [',
            "        {",
            '          "blocking": true,',
            '          "summary": "short actionable statement",',
            '          "evidence_ref": "/abs/path/to/evidence"',
            "        }",
            "      ]",
            "    }",
            "  ]",
            "}",
            "```",
            "",
            "Output requirements:",
            "- feedback.json must be written before reviewer_report.md.",
            "- reviewer_report.md should explain why each active finding belongs to its owner bucket and summarize what is resolved, still open, or superseded.",
            "- Keep reviewer_report.md consistent with feedback.json.",
            "- reviewer_report.md should state explicitly which findings are blocking and which are non-blocking.",
            "",
            "Submission interface:",
            f"- Use: {self._repo_root / 'scripts' / 'routerctl.sh'} complete --verdict-kind <OK|IMPLEMENTER_ACTION_REQUIRED|EVALUATOR_FAULT> --report-ref <path/to/reviewer_report.md> --feedback-ref <path/to/feedback.json>",
            "- report-ref and feedback-ref must be the canonical current reviewer paths injected by router.",
            "- verdict_kind remains required for compatibility with the current router interface.",
            "",
            "Hard validation rule:",
            "- If routerctl complete rejects feedback_ref or report_ref, the review is not complete.",
            "- Treat any routerctl rejection as a hard validation failure.",
            "- Read the rejection reason carefully, fix the submission file(s), and retry.",
            "- Do not ignore field-level validation errors.",
            "- Do not claim success until routerctl complete accepts the submission.",
        ]

    def _build_feedback_slot_lines(self) -> list[str]:
        return [
            "=== ROUTER FEEDBACK SLOT ===",
            "- Reserved for future actor-targeted feedback or resume instructions from router/reviewer.",
            "- If explicit feedback, resume guidance, or actor-targeted findings appear below this marker, treat them as authoritative for this actor.",
            "- If no such lines appear below this marker, continue with the normal router dispatch context.",
        ]

    def _build_actor_prompt_text(
        self,
        *,
        node_id: str,
        actor_kind: ActorKind,
        workspace_root: Path,
        final_effects_file: Path,
        prelude_lines: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> str:
        routerctl = (self._repo_root / "scripts" / "routerctl.sh").resolve()
        if actor_kind is ActorKind.EVALUATOR_CHECKER:
            lines = [
                *self._build_checker_prompt_lines(routerctl=routerctl),
                f"Workspace root: {workspace_root}",
                f"Router control helper: {routerctl}",
            ]
            lines.extend(["", *self._build_feedback_slot_lines()])
        else:
            lines = [
                f"You are the LOOP {actor_kind.value} for node {node_id}.",
                f"Workspace root: {workspace_root}",
                f"Router control helper: {routerctl}",
            ]
            lines.extend(
                self._build_completion_contract_lines(actor_kind=actor_kind, routerctl=routerctl)
            )
            if actor_kind is ActorKind.IMPLEMENTER:
                lines.extend(["", *self._build_implementer_prompt_lines(routerctl=routerctl)])
            elif actor_kind is ActorKind.KERNEL:
                if str((extra_env or {}).get("LOOP_REQUEST_SEQ") or "").strip():
                    lines.extend(["", *self._build_kernel_split_review_prompt_lines()])
                else:
                    lines.extend(["", *self._build_kernel_takeover_prompt_lines()])
            elif actor_kind is ActorKind.EVALUATOR_TESTER:
                lines.extend(["", *self._build_tester_prompt_lines()])
            elif actor_kind is ActorKind.EVALUATOR_AI_USER:
                lines.extend(["", *self._build_ai_user_prompt_lines()])
            elif actor_kind is ActorKind.EVALUATOR_REVIEWER:
                lines.extend(["", *self._build_reviewer_prompt_lines()])
            lines.extend(["", *self._build_feedback_slot_lines()])
        if prelude_lines:
            lines.extend(["", *prelude_lines])
        lines.extend(
            [
                "",
                f"Final effects file: {final_effects_file}",
                "The file at this path is the authoritative durable task source.",
                "The inline copy below is only a startup snapshot for convenience.",
                "--- BEGIN FINAL EFFECTS ---",
                final_effects_file.read_text(encoding="utf-8", errors="replace"),
                "--- END FINAL EFFECTS ---",
            ]
        )
        return "\n".join(lines)

    def _build_actor_launch_spec(
        self,
        *,
        node_id: str,
        parent_node_id: str,
        actor_kind: ActorKind,
        attempt_count: int,
        final_effects_file: Path,
        prelude_lines: list[str] | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> ActorLaunchSpec:
        resolved_final_effects = Path(final_effects_file).expanduser().resolve()
        workspace_root = resolved_final_effects.parent.resolve()
        actor_task_id = (
            str((extra_env or {}).get("LOOP_TASK_ID") or "").strip() or None
        )
        completion_file = actor_completion_record_path(
            workspace_root,
            actor_kind,
            node_id=str(node_id),
            task_id=actor_task_id if actor_kind is ActorKind.EVALUATOR_AI_USER else None,
        )
        return ActorLaunchSpec(
            node_id=str(node_id),
            parent_node_id=str(parent_node_id),
            actor_kind=actor_kind,
            attempt_count=int(attempt_count),
            workspace_root=workspace_root,
            final_effects_file=resolved_final_effects,
            prompt_text=self._build_actor_prompt_text(
                node_id=str(node_id),
                actor_kind=actor_kind,
                workspace_root=workspace_root,
                final_effects_file=resolved_final_effects,
                prelude_lines=prelude_lines,
                extra_env=extra_env,
            ),
            env={
                "LOOP_ROUTER_DB_PATH": str(self._store.db_path.resolve()),
                "LOOP_NODE_ID": str(node_id),
                "LOOP_PARENT_NODE_ID": str(parent_node_id),
                "LOOP_ACTOR_KIND": actor_kind.value,
                "LOOP_ATTEMPT_COUNT": str(int(attempt_count)),
                "LOOP_WORKSPACE_ROOT": str(workspace_root),
                "LOOP_FINAL_EFFECTS_FILE": str(resolved_final_effects),
                "LOOP_COMPLETION_FILE": str(completion_file),
                **(dict(extra_env or {})),
            },
        )

    def _launch_actor(self, spec: ActorLaunchSpec) -> ActorLaunchResult:
        if self._actor_launcher is None:
            raise ActorLaunchRejectedError(
                "actor launcher is not configured."
            )
        launch = self._actor_launcher(spec)
        session_id = str(launch.session_id or "").strip()
        if not session_id:
            raise ActorLaunchRejectedError(
                "launched actor did not produce a session_id."
            )
        return launch

    def _build_kernel_pending_record(
        self,
        *,
        source_record: NodeRuntimeRecord,
    ) -> NodeRuntimeRecord:
        kernel_record = self._store.load_node(KERNEL_NODE_ID)
        if kernel_record is None:
            return NodeRuntimeRecord(
                node_id=KERNEL_NODE_ID,
                parent_node_id=KERNEL_NODE_ID,
                child_node_ids=[],
                workspace_root=str(source_record.workspace_root),
                final_effects_file=str(source_record.final_effects_file),
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                current_components=[],
                durable_commit="",
                result_commit="",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="",
                components={},
            )
        return replace(
            kernel_record,
            workspace_root=str(source_record.workspace_root),
            final_effects_file=str(source_record.final_effects_file),
            result_commit="",
            pending_prelude_lines=[],
        )

    def _subtree_records(self, root_node_id: str) -> list[NodeRuntimeRecord]:
        records_by_id = {
            str(record.node_id): record
            for record in self._store.list_nodes()
        }
        root_record = records_by_id.get(str(root_node_id))
        if root_record is None:
            return []
        ordered: list[NodeRuntimeRecord] = []
        stack = [str(root_node_id)]
        seen: set[str] = set()
        while stack:
            node_id = stack.pop()
            if node_id in seen:
                continue
            seen.add(node_id)
            record = records_by_id.get(node_id)
            if record is None:
                continue
            ordered.append(record)
            stack.extend(reversed([str(child_id) for child_id in record.child_node_ids]))
        return ordered

    def _record_has_pending_split_review(self, record: NodeRuntimeRecord) -> bool:
        return int(record.split_request) != 0 and int(record.split_approved) == 0

    def _record_is_active_system_node(self, record: NodeRuntimeRecord) -> bool:
        if record.current_components:
            return True
        return self._record_has_pending_split_review(record)

    def _active_system_node_count(self) -> int:
        return sum(
            1
            for record in self._store.list_nodes()
            if self._record_is_active_system_node(record)
        )

    def _open_split_approved_node_count(self) -> int:
        return sum(
            1
            for record in self._store.list_nodes()
            if int(record.split_approved) == 1 and not str(record.result_commit or "").strip()
        )

    def _build_kernel_review_prelude(
        self,
        *,
        request_seq: int,
        source_record: NodeRuntimeRecord,
        split_bundle_ref: str,
        durable_commit: str,
        diff_fingerprint: str,
    ) -> list[str]:
        return [
            "Router kernel review dispatch.",
            f"Split request seq: {request_seq}",
            f"Parent workspace root: {source_record.workspace_root}",
            f"Parent final effects file: {source_record.final_effects_file}",
            f"Frozen split bundle snapshot: {split_bundle_ref}",
            f"Parent durable commit baseline: {durable_commit}",
            f"Source diff fingerprint: {diff_fingerprint}",
            f"Active system node count: {int(self._active_system_node_count())}",
            f"Open split-approved node count: {int(self._open_split_approved_node_count())}",
            "Review the frozen split bundle snapshot, not the live workspace bundle.",
            "Write REJECT only when the proposal should not be accepted in its current frozen form.",
        ]

    def _build_running_node_record(
        self,
        *,
        spec: ActorLaunchSpec,
        launch: ActorLaunchResult,
        base_record: NodeRuntimeRecord | None = None,
        workspace_fingerprint_before: str,
    ) -> NodeRuntimeRecord:
        from loop.node_table import ComponentRuntimeState

        running_actor = self._actor_ref(
            node_id=spec.node_id,
            actor_kind=spec.actor_kind,
            attempt_count=spec.attempt_count,
            task_id=str(spec.env.get("LOOP_TASK_ID") or "").strip() or None,
        )
        component_state_key = component_key_for_actor(running_actor)
        components = (
            {key: state for key, state in base_record.components.items()}
            if base_record is not None
            else {}
        )
        existing_current_components = (
            [
                actor
                for actor in base_record.current_components
                if component_key_for_actor(actor) != component_state_key
            ]
            if base_record is not None
            else []
        )
        previous_component = components.get(component_state_key)
        session_ids = list(previous_component.session_ids) if previous_component is not None else []
        session_ids.append(str(launch.session_id))
        consecutive_failed_exits = (
            int(previous_component.consecutive_failed_exits)
            if previous_component is not None
            else 0
        )
        split_request = 0 if base_record is None else int(base_record.split_request)
        split_approved = 0 if base_record is None else int(base_record.split_approved)
        approved_split_request_seq = (
            0 if base_record is None else int(base_record.approved_split_request_seq)
        )
        if base_record is None:
            durable_commit = _resolve_workspace_durable_commit(spec.workspace_root)
        else:
            durable_commit = str(base_record.durable_commit or "").strip()
            if not durable_commit:
                durable_commit = _resolve_workspace_durable_commit(spec.workspace_root)
        escalated_to_kernel = False if base_record is None else bool(base_record.escalated_to_kernel)
        last_rejected = "" if base_record is None else str(base_record.last_rejected_split_diff_fingerprint)
        result_commit = "" if base_record is None else str(base_record.result_commit or "").strip()
        child_node_ids = [] if base_record is None else list(base_record.child_node_ids)
        task_result_refs = {} if base_record is None else dict(base_record.task_result_refs)
        pending_prelude_lines: list[str] = []
        return NodeRuntimeRecord(
            node_id=spec.node_id if base_record is None else base_record.node_id,
            parent_node_id=spec.parent_node_id if base_record is None else base_record.parent_node_id,
            child_node_ids=child_node_ids,
            workspace_root=(
                str(spec.workspace_root.resolve())
                if base_record is None
                else str(base_record.workspace_root)
            ),
            final_effects_file=(
                str(spec.final_effects_file.resolve())
                if base_record is None
                else str(base_record.final_effects_file)
            ),
            split_request=split_request,
            split_approved=split_approved,
            approved_split_request_seq=approved_split_request_seq,
            evaluator_phase="" if base_record is None else str(base_record.evaluator_phase),
            checker_tasks_ref="" if base_record is None else str(base_record.checker_tasks_ref),
            task_result_refs=task_result_refs,
            reviewer_verdict_kind=(
                "" if base_record is None else str(base_record.reviewer_verdict_kind)
            ),
            reviewer_report_ref="" if base_record is None else str(base_record.reviewer_report_ref),
            pending_prelude_lines=pending_prelude_lines,
            current_components=[*existing_current_components, running_actor],
            durable_commit=durable_commit,
            result_commit=result_commit,
            escalated_to_kernel=escalated_to_kernel,
            last_rejected_split_diff_fingerprint=last_rejected,
            components={
                **components,
                component_state_key: ComponentRuntimeState(
                    status=ComponentStatus.RUNNING,
                    attempt_count=spec.attempt_count,
                    task_id=str(spec.env.get("LOOP_TASK_ID") or "").strip(),
                    pid=int(getattr(launch.process, "pid", 0) or 0),
                    process_birth_time=launch.process_birth_time,
                    session_ids=session_ids,
                    workspace_fingerprint_before=workspace_fingerprint_before,
                    saw_output_in_attempt=False,
                    consecutive_no_progress=0,
                    consecutive_failed_exits=consecutive_failed_exits,
                ),
            },
        )

    def _launch_and_commit_actor(
        self,
        *,
        spec: ActorLaunchSpec,
        base_record: NodeRuntimeRecord | None,
    ) -> tuple[NodeRuntimeRecord, ActorLaunchResult]:
        launch: ActorLaunchResult | None = None
        node_persisted = False
        completion_file = actor_completion_record_path(
            spec.workspace_root,
            spec.actor_kind,
            node_id=spec.node_id,
            task_id=str(spec.env.get("LOOP_TASK_ID") or "").strip() or None,
        )
        completion_file.unlink(missing_ok=True)
        workspace_fingerprint_before = workspace_output_fingerprint(spec.workspace_root)
        try:
            launch = self._launch_actor(spec)
            updated_record = self._build_running_node_record(
                spec=spec,
                launch=launch,
                base_record=base_record,
                workspace_fingerprint_before=workspace_fingerprint_before,
            )
            self._store.upsert_node(updated_record)
            node_persisted = True
            self._register_actor_with_proc(spec=spec, launch=launch)
            return updated_record, launch
        except Exception:
            if node_persisted:
                if base_record is None:
                    self._store.delete_node(spec.node_id)
                else:
                    self._store.upsert_node(base_record)
            if launch is not None:
                self._terminate_process_best_effort(launch.process)
            raise

    def _register_actor_with_proc(
        self,
        *,
        spec: ActorLaunchSpec,
        launch: ActorLaunchResult,
    ) -> None:
        proc_supervisor = self._ensure_proc_supervisor()
        proc_supervisor.register_actor(
            self._actor_ref(
                node_id=spec.node_id,
                actor_kind=spec.actor_kind,
                attempt_count=spec.attempt_count,
                task_id=str(spec.env.get("LOOP_TASK_ID") or "").strip() or None,
            ),
            process=launch.process,
            process_birth_time=launch.process_birth_time,
            session_id=launch.session_id,
            rollout_path=launch.rollout_path,
        )

    def _run_loop(self) -> None:
        try:
            self._drain_inbox_to_empty()
            while True:
                if self._stop_requested and self._inbox_is_empty():
                    return
                server = self._socket
                if server is None:
                    return
                try:
                    server.recv(1)
                except OSError:
                    return
                self._drain_inbox_to_empty()
        finally:
            self._cleanup_socket()

    def _drain_inbox_to_empty(self) -> None:
        while True:
            last_applied_seq = self._store.read_last_applied_seq()
            items = self._store.list_events_after(last_applied_seq, limit=1)
            if not items:
                self._reconcile_pending_launches_once()
                return
            item = items[0]
            self._handle_inbox_item_with_boundary(item)
            self._store.write_last_applied_seq(item.seq)

    def _handle_inbox_item_with_boundary(self, item: StoredRouterInboxItem) -> None:
        try:
            self._handle_inbox_item(item)
        except RouterBusinessError as exc:
            LOGGER.warning(
                "router inbox business error for seq=%s event_type=%s reason_code=%s: %s",
                item.seq,
                item.event_type,
                exc.reason_code,
                exc.message,
            )
        except Exception:
            LOGGER.exception(
                "router inbox handler failed for seq=%s event_type=%s; skipping poisoned event",
                item.seq,
                item.event_type,
            )

    def _load_event_payload_dict(self, item: StoredRouterInboxItem) -> dict[str, object]:
        try:
            payload = json.loads(item.payload_json)
        except json.JSONDecodeError as exc:
            raise InvalidEventPayloadError(
                f"{item.event_type} payload_json must be valid JSON"
            ) from exc
        if not isinstance(payload, dict):
            raise InvalidEventPayloadError(
                f"{item.event_type} payload_json must decode to an object"
            )
        return payload

    def _payload_required_str(
        self,
        payload: dict[str, object],
        *,
        event_type: str,
        field_name: str,
    ) -> str:
        value = str(payload.get(field_name) or "").strip()
        if not value:
            raise InvalidEventPayloadError(
                f"{event_type} payload must define non-empty {field_name}"
            )
        return value

    def _payload_optional_str(
        self,
        payload: dict[str, object],
        *,
        field_name: str,
    ) -> str:
        return str(payload.get(field_name) or "").strip()

    def _payload_required_int(
        self,
        payload: dict[str, object],
        *,
        event_type: str,
        field_name: str,
    ) -> int:
        raw = payload.get(field_name)
        if raw is None or raw == "":
            raise InvalidEventPayloadError(
                f"{event_type} payload must define {field_name}"
            )
        try:
            return int(raw)
        except (TypeError, ValueError) as exc:
            raise InvalidEventPayloadError(
                f"{event_type} payload field {field_name} must be an integer"
            ) from exc

    def _inbox_is_empty(self) -> bool:
        last_applied_seq = self._store.read_last_applied_seq()
        return not self._store.list_events_after(last_applied_seq, limit=1)

    def _bind_socket(self) -> None:
        self._wakeup_socket_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            if self._wakeup_socket_path.exists() or self._wakeup_socket_path.is_symlink():
                self._wakeup_socket_path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass
        server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        server.bind(str(self._wakeup_socket_path))
        self._socket = server

    def _cleanup_socket(self) -> None:
        server = self._socket
        self._socket = None
        if server is not None:
            try:
                server.close()
            except OSError:
                pass
        try:
            if self._wakeup_socket_path.exists() or self._wakeup_socket_path.is_symlink():
                self._wakeup_socket_path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass

    def _handle_inbox_item(self, item: StoredRouterInboxItem) -> None:
        handler = {
            "OutputWindow": self._handle_output_window,
            "ProcessExitedObserved": self._handle_process_exited_observed,
            "RequestSplit": self._handle_request_split,
            "ApproveSplit": self._handle_approve_split,
            "RejectSplit": self._handle_reject_split,
            "TakeoverResolved": self._handle_takeover_resolved,
        }.get(item.event_type)
        if handler is None:
            raise RuntimeError(f"unsupported router inbox item type: {item.event_type}")
        handler(item)

    def _observe_placeholder(self, item: StoredRouterInboxItem) -> None:
        if self._placeholder_observer is not None:
            self._placeholder_observer(item)

    def _handle_output_window(self, item: StoredRouterInboxItem) -> None:
        payload = self._load_event_payload_dict(item)
        event_task_id = self._payload_optional_str(payload, field_name="task_id") or None
        observed_actor = self._actor_ref(
            node_id=item.node_id,
            actor_kind=item.actor_kind,
            attempt_count=item.attempt_count,
            task_id=event_task_id,
        )
        raw_had_output = payload.get("had_output")
        if isinstance(raw_had_output, bool):
            had_output = raw_had_output
        elif str(raw_had_output).strip().lower() in {"0", "1", "true", "false"}:
            had_output = str(raw_had_output).strip().lower() in {"1", "true"}
        else:
            raise InvalidEventPayloadError(
                f"{item.event_type} payload field had_output must be a boolean"
            )
        record = self._store.load_node(item.node_id)
        if record is not None:
            component = self._component_state_for_actor(record, observed_actor)
            if (
                self._has_current_actor(record, observed_actor)
                and component is not None
                and component.status is ComponentStatus.RUNNING
                and int(component.attempt_count) == int(item.attempt_count)
                and str(component.task_id or "").strip() == str(event_task_id or "").strip()
            ):
                updated_component = component
                should_persist = False
                if had_output:
                    next_saw_output = True
                    next_no_progress = 0
                else:
                    next_saw_output = bool(component.saw_output_in_attempt)
                    next_no_progress = int(component.consecutive_no_progress) + 1
                if (
                    bool(updated_component.saw_output_in_attempt) != bool(next_saw_output)
                    or int(updated_component.consecutive_no_progress) != int(next_no_progress)
                ):
                    updated_component = replace(
                        updated_component,
                        saw_output_in_attempt=next_saw_output,
                        consecutive_no_progress=int(next_no_progress),
                    )
                    should_persist = True
                if should_persist:
                    updated_record = replace(
                        record,
                        components={
                            **record.components,
                            component_key_for_actor(observed_actor): updated_component,
                        },
                    )
                    self._store.upsert_node(updated_record)
                    record = updated_record
                    component = updated_component
                if int(record.split_approved) == 1 and had_output:
                    actor_key = (item.node_id, item.actor_kind.value, int(item.attempt_count))
                    new_count = int(self._split_approved_output_counts.get(actor_key, 0)) + 1
                    self._split_approved_output_counts[actor_key] = new_count
                    if new_count >= 2:
                        proc_supervisor = self._proc_supervisor
                        terminate_actor = (
                            None if proc_supervisor is None else getattr(proc_supervisor, "terminate_actor", None)
                        )
                        if callable(terminate_actor):
                            terminate_actor(
                                self._actor_ref(
                                    node_id=item.node_id,
                                    actor_kind=item.actor_kind,
                                    attempt_count=item.attempt_count,
                                    task_id=event_task_id,
                                )
                            )
                        self._split_approved_output_counts.pop(actor_key, None)
                elif (
                    not had_output
                    and int(component.consecutive_no_progress) >= ACTOR_MAX_NO_PROGRESS_WINDOWS
                ):
                    proc_supervisor = self._proc_supervisor
                    terminate_actor = (
                        None if proc_supervisor is None else getattr(proc_supervisor, "terminate_actor", None)
                    )
                    if callable(terminate_actor):
                        terminate_actor(
                            self._actor_ref(
                                node_id=item.node_id,
                                actor_kind=item.actor_kind,
                                attempt_count=item.attempt_count,
                                task_id=event_task_id,
                            )
                        )
        self._observe_placeholder(item)

    def _handle_process_exited_observed(self, item: StoredRouterInboxItem) -> None:
        self._split_approved_output_counts.pop(
            (item.node_id, item.actor_kind.value, int(item.attempt_count)),
            None,
        )
        record = self._store.load_node(item.node_id)
        if record is None:
            self._observe_placeholder(item)
            return
        payload = self._load_process_exited_payload(item)
        observed_actor = self._actor_ref(
            node_id=item.node_id,
            actor_kind=item.actor_kind,
            attempt_count=item.attempt_count,
            task_id=str(payload.get("task_id") or "").strip() or None,
        )
        component = self._component_state_for_actor(record, observed_actor)
        if not self._process_exit_matches_running_actor(
            record=record,
            item=item,
            observed_actor=observed_actor,
            component=component,
            payload=payload,
        ):
            self._observe_placeholder(item)
            return
        if int(record.split_request) == 1 or int(record.split_approved) == 1:
            self._store.upsert_node(
                self._mark_component_inactive(
                    record,
                    item.actor_kind,
                    task_id=observed_actor.task_id,
                )
            )
            self._observe_placeholder(item)
            return
        completion_check = self._validate_actor_completion(
            record=record,
            item=item,
            component=component,
            payload=payload,
        )
        if not completion_check.accepted:
            self._recover_actor_after_exit(
                record=record,
                actor_kind=item.actor_kind,
                component=component,
                reason_lines=completion_check.reason_lines,
            )
            self._observe_placeholder(item)
            return
        self._advance_after_valid_actor_completion(
            record=record,
            actor_kind=item.actor_kind,
            task_id=observed_actor.task_id,
            completion_payload=dict(completion_check.payload or {}),
        )
        self._observe_placeholder(item)

    def _handle_request_split(self, item: StoredRouterInboxItem) -> None:
        record = self._store.load_node(item.node_id)
        if record is None:
            self._observe_placeholder(item)
            return
        observed_actor = self._actor_ref(
            node_id=item.node_id,
            actor_kind=item.actor_kind,
            attempt_count=item.attempt_count,
        )
        component = self._component_state_for_actor(record, observed_actor)
        if (
            item.actor_kind is not ActorKind.IMPLEMENTER
            or not self._has_current_actor(record, observed_actor)
            or component is None
            or component.status is not ComponentStatus.RUNNING
            or int(component.attempt_count) != int(item.attempt_count)
            or int(record.split_approved) != 0
        ):
            self._observe_placeholder(item)
            return
        if int(record.split_request) != 1:
            self._observe_placeholder(item)
            return
        self._observe_placeholder(item)

    def _handle_approve_split(self, item: StoredRouterInboxItem) -> None:
        payload = self._load_event_payload_dict(item)
        target_node_id = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="target_node_id",
        )
        request_seq = self._payload_required_int(
            payload,
            event_type=item.event_type,
            field_name="request_seq",
        )
        record = self._store.load_node(target_node_id)
        if record is None:
            self._observe_placeholder(item)
            return
        if int(record.split_request) != 1:
            self._observe_placeholder(item)
            return
        updated_parent = replace(
            record,
            split_request=0,
            split_approved=1,
            approved_split_request_seq=request_seq,
        )
        updated_parent, new_child_records = self._materialize_split_children(
            parent_record=updated_parent,
            request_seq=request_seq,
        )
        self._split_approved_output_counts.pop(
            (
                target_node_id,
                ActorKind.IMPLEMENTER.value,
                int(self._component_state_for_kind(record, ActorKind.IMPLEMENTER).attempt_count)
                if self._component_state_for_kind(record, ActorKind.IMPLEMENTER) is not None
                else 0,
            ),
            None,
        )
        for child_record in new_child_records:
            self._launch_implementer_for_record(child_record)
        self._observe_placeholder(item)

    def _handle_reject_split(self, item: StoredRouterInboxItem) -> None:
        payload = self._load_event_payload_dict(item)
        target_node_id = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="target_node_id",
        )
        request_seq = self._payload_required_int(
            payload,
            event_type=item.event_type,
            field_name="request_seq",
        )
        record = self._store.load_node(target_node_id)
        if record is None:
            self._observe_placeholder(item)
            return
        if int(record.split_request) != 1:
            self._observe_placeholder(item)
            return
        diff_fingerprint = self._resolve_request_diff_fingerprint(request_seq)
        reason_ref = self._payload_optional_str(payload, field_name="reason_ref") or None
        implementer_state = self._component_state_for_kind(record, ActorKind.IMPLEMENTER)
        has_actor = (
            None if self._proc_supervisor is None else getattr(self._proc_supervisor, "has_actor", None)
        )
        implementer_actor = None
        if implementer_state is not None:
            implementer_actor = self._actor_ref(
                node_id=record.node_id,
                actor_kind=ActorKind.IMPLEMENTER,
                attempt_count=implementer_state.attempt_count,
            )
        needs_recovery = implementer_state is not None and (
            not callable(has_actor) or not bool(has_actor(implementer_actor))
        )
        updated_record = replace(
            record,
            split_request=0,
            approved_split_request_seq=0,
            last_rejected_split_diff_fingerprint=diff_fingerprint,
        )
        if needs_recovery:
            updated_record = replace(
                self._mark_component_inactive(updated_record, ActorKind.IMPLEMENTER),
                pending_prelude_lines=self._build_reject_recovery_prelude(reason_ref),
            )
        self._store.upsert_node(updated_record)
        if needs_recovery:
            self._launch_implementer_for_record(updated_record)
        self._observe_placeholder(item)

    def _load_process_exited_payload(self, item: StoredRouterInboxItem) -> dict[str, object]:
        payload = self._load_event_payload_dict(item)
        normalized: dict[str, object] = {
            "pid": self._payload_required_int(
                payload,
                event_type=item.event_type,
                field_name="pid",
            ),
            "signal_name": self._payload_optional_str(payload, field_name="signal_name") or None,
            "task_id": self._payload_optional_str(payload, field_name="task_id") or None,
        }
        raw_birth = payload.get("process_birth_time")
        if raw_birth in {None, ""}:
            normalized["process_birth_time"] = None
        else:
            try:
                normalized["process_birth_time"] = float(raw_birth)
            except (TypeError, ValueError) as exc:
                raise InvalidEventPayloadError(
                    f"{item.event_type} payload field process_birth_time must be a float or null"
                ) from exc
        raw_exit = payload.get("exit_code")
        if raw_exit in {None, ""}:
            normalized["exit_code"] = None
        else:
            try:
                normalized["exit_code"] = int(raw_exit)
            except (TypeError, ValueError) as exc:
                raise InvalidEventPayloadError(
                    f"{item.event_type} payload field exit_code must be an integer or null"
                ) from exc
        return normalized

    def _process_exit_matches_running_actor(
        self,
        *,
        record: NodeRuntimeRecord,
        item: StoredRouterInboxItem,
        observed_actor,
        component,
        payload: dict[str, object],
    ) -> bool:
        if (
            not self._has_current_actor(record, observed_actor)
            or component is None
            or component.status is not ComponentStatus.RUNNING
            or int(component.attempt_count) != int(item.attempt_count)
        ):
            return False
        observed_task_id = str(payload.get("task_id") or "").strip()
        expected_task_id = str(component.task_id or "").strip()
        if expected_task_id != observed_task_id:
            return False
        expected_pid = int(component.pid or 0)
        observed_pid = int(payload.get("pid") or 0)
        if expected_pid > 0 and observed_pid > 0 and expected_pid != observed_pid:
            return False
        expected_birth = component.process_birth_time
        observed_birth = payload.get("process_birth_time")
        if (
            expected_birth is not None
            and observed_birth is not None
            and float(expected_birth) != float(observed_birth)
        ):
            return False
        return True

    def _resolve_existing_payload_file(self, value: object) -> Path | None:
        normalized = str(value or "").strip()
        if not normalized:
            return None
        candidate = Path(normalized).expanduser().resolve()
        if not candidate.exists() or not candidate.is_file():
            return None
        return candidate

    def _snapshot_runtime_output(
        self,
        *,
        source_ref: Path,
        target_ref: Path,
        label: str,
    ) -> Path:
        try:
            target_ref.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_ref, target_ref)
        except Exception as exc:
            target_ref.unlink(missing_ok=True)
            raise RouterBusinessError(f"{label} snapshot failed: {exc}") from exc
        return target_ref

    def _current_dispatched_snapshot_id(self, record: NodeRuntimeRecord) -> str:
        checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
        if not checker_tasks_ref:
            return ""
        try:
            manifest = self._load_checker_task_manifest(Path(checker_tasks_ref))
        except RouterBusinessError:
            return ""
        return str(manifest.get("snapshot_id") or "").strip()

    def _validate_snapshot_markers(
        self,
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
            reason_lines.append(
                f"{label} must include the exact line `{expected_marker}`."
            )
        snapshot_pattern = re.compile(r"snapshot-[0-9a-f]+")
        for index, line in enumerate(str(text or "").splitlines(), start=1):
            line_snapshots = {match.group(0) for match in snapshot_pattern.finditer(line)}
            if not line_snapshots:
                continue
            normalized_line = line.lower()
            for snapshot_id in sorted(line_snapshots):
                if snapshot_id == expected_snapshot_id:
                    continue
                if any(token in normalized_line for token in ("historical", "stale", "superseded")):
                    continue
                reason_lines.append(
                    f"{label} line {index} references stale snapshot {snapshot_id} without marking it historical/stale/superseded."
                )
        return reason_lines

    def _validate_tester_result_file(
        self,
        *,
        result_ref: Path,
        record: NodeRuntimeRecord,
    ) -> list[str]:
        text = result_ref.read_text(encoding="utf-8", errors="replace")
        reason_lines: list[str] = []
        if "Overall verdict:" not in text:
            reason_lines.append("Evaluator tester result must include `Overall verdict:`.")
        reason_lines.extend(
            self._validate_snapshot_markers(
                text=text,
                expected_snapshot_id=self._current_dispatched_snapshot_id(record),
                label="Evaluator tester result",
            )
        )
        return reason_lines

    def _validate_ai_user_result_file(
        self,
        *,
        result_ref: Path,
        record: NodeRuntimeRecord,
        task_id: str,
    ) -> list[str]:
        text = result_ref.read_text(encoding="utf-8", errors="replace")
        reason_lines: list[str] = []
        if f"Task id: {task_id}" not in text:
            reason_lines.append(
                f"Evaluator AI-user result must include the exact line `Task id: {task_id}`."
            )
        if "Task verdict:" not in text:
            reason_lines.append("Evaluator AI-user result must include `Task verdict:`.")
        reason_lines.extend(
            self._validate_snapshot_markers(
                text=text,
                expected_snapshot_id=self._current_dispatched_snapshot_id(record),
                label="Evaluator AI-user result",
            )
        )
        return reason_lines

    def _validate_reviewer_report_file(
        self,
        *,
        report_ref: Path,
        record: NodeRuntimeRecord,
    ) -> list[str]:
        text = report_ref.read_text(encoding="utf-8", errors="replace")
        reason_lines: list[str] = []
        reason_lines.extend(
            self._validate_snapshot_markers(
                text=text,
                expected_snapshot_id=self._current_dispatched_snapshot_id(record),
                label="Reviewer report",
            )
        )
        return reason_lines

    def _normalize_reviewer_feedback_findings(
        self,
        *,
        bucket_label: str,
        findings_payload: object,
    ) -> tuple[list[dict[str, object]], list[str]]:
        if not isinstance(findings_payload, list):
            return [], [f"{bucket_label}.findings must be a list."]
        normalized_findings: list[dict[str, object]] = []
        reason_lines: list[str] = []
        for index, raw_finding in enumerate(findings_payload, start=1):
            finding_label = f"{bucket_label}.findings[{index}]"
            if not isinstance(raw_finding, dict):
                reason_lines.append(f"{finding_label} must be an object.")
                continue
            allowed_keys = {"blocking", "summary", "evidence_ref"}
            extra_keys = sorted(set(raw_finding) - allowed_keys)
            if extra_keys:
                reason_lines.append(
                    f"{finding_label} contains unsupported keys: {', '.join(extra_keys)}."
                )
            if "blocking" not in raw_finding:
                reason_lines.append(f"{finding_label}.blocking is required.")
            blocking = raw_finding.get("blocking")
            if not isinstance(blocking, bool):
                reason_lines.append(f"{finding_label}.blocking must be true or false.")
            summary = str(raw_finding.get("summary") or "").strip()
            if not summary:
                reason_lines.append(f"{finding_label}.summary must be a non-empty string.")
            evidence_ref = self._resolve_existing_payload_file(raw_finding.get("evidence_ref"))
            if evidence_ref is None:
                reason_lines.append(
                    f"{finding_label}.evidence_ref must be an existing regular file."
                )
            if (
                isinstance(blocking, bool)
                and summary
                and evidence_ref is not None
            ):
                normalized_findings.append(
                    {
                        "blocking": bool(blocking),
                        "summary": summary,
                        "evidence_ref": str(evidence_ref),
                    }
                )
        return normalized_findings, reason_lines

    def _normalize_reviewer_feedback_bucket(
        self,
        *,
        bucket_label: str,
        bucket_payload: object,
        task_id_required: bool,
        dispatched_task_ids: set[str],
    ) -> tuple[dict[str, object] | None, list[str]]:
        if not isinstance(bucket_payload, dict):
            return None, [f"{bucket_label} must be an object."]
        allowed_keys = {"findings"}
        if task_id_required:
            allowed_keys.add("task_id")
        extra_keys = sorted(set(bucket_payload) - allowed_keys)
        reason_lines: list[str] = []
        if extra_keys:
            reason_lines.append(
                f"{bucket_label} contains unsupported keys: {', '.join(extra_keys)}."
            )
        task_id = ""
        if task_id_required:
            task_id = str(bucket_payload.get("task_id") or "").strip()
            if not task_id:
                reason_lines.append(f"{bucket_label}.task_id must be a non-empty string.")
            elif task_id not in dispatched_task_ids:
                reason_lines.append(
                    f"{bucket_label}.task_id does not match any task in the current checker manifest."
                )
        normalized_findings, finding_reason_lines = self._normalize_reviewer_feedback_findings(
            bucket_label=bucket_label,
            findings_payload=bucket_payload.get("findings"),
        )
        reason_lines.extend(finding_reason_lines)
        if reason_lines:
            return None, reason_lines
        normalized_bucket: dict[str, object] = {
            "findings": normalized_findings,
        }
        if task_id_required:
            normalized_bucket["task_id"] = task_id
        return normalized_bucket, []

    def _resolve_reviewer_feedback_submission(
        self,
        *,
        record: NodeRuntimeRecord,
        feedback_ref: Path,
    ) -> tuple[dict[str, object] | None, list[str]]:
        try:
            raw_payload = json.loads(feedback_ref.read_text(encoding="utf-8"))
        except UnicodeDecodeError:
            return None, ["Reviewer feedback_ref must be valid UTF-8 JSON."]
        except json.JSONDecodeError as exc:
            return None, [f"Reviewer feedback_ref must contain valid JSON: {exc.msg}."]
        if not isinstance(raw_payload, dict):
            return None, ["Reviewer feedback_ref JSON must be an object."]
        allowed_keys = {"version", "snapshot_id", "implementer", "checker", "tester", "ai_user"}
        extra_keys = sorted(set(raw_payload) - allowed_keys)
        reason_lines: list[str] = []
        if extra_keys:
            reason_lines.append(
                "Reviewer feedback_ref contains unsupported top-level keys: "
                + ", ".join(extra_keys)
                + "."
            )
        version = raw_payload.get("version", 1)
        try:
            version_value = int(version)
        except (TypeError, ValueError):
            version_value = -1
        if version_value != 2:
            reason_lines.append("Reviewer feedback_ref version must be exactly 2.")
        expected_snapshot_id = self._current_dispatched_snapshot_id(record)
        observed_snapshot_id = str(raw_payload.get("snapshot_id") or "").strip()
        if not expected_snapshot_id:
            reason_lines.append(
                "Reviewer feedback_ref requires a current dispatched snapshot id from checker before submission."
            )
        elif observed_snapshot_id != expected_snapshot_id:
            reason_lines.append(
                "Reviewer feedback_ref snapshot_id must match the current dispatched snapshot id "
                f"{expected_snapshot_id}."
            )
        try:
            checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
            dispatched_task_ids = (
                {
                    str(task["task_id"])
                    for task in self._resolve_checker_tasks(Path(checker_tasks_ref))
                }
                if checker_tasks_ref
                else set()
            )
        except RouterBusinessError as exc:
            return None, [
                "Reviewer feedback_ref requires a valid checker task manifest before ai_user findings can be validated: "
                + exc.message
            ]
        normalized: dict[str, object] = {"version": 2, "snapshot_id": expected_snapshot_id}
        for owner in ("implementer", "checker", "tester"):
            if owner not in raw_payload:
                reason_lines.append(f"Reviewer feedback_ref is missing top-level bucket {owner!r}.")
                continue
            normalized_bucket, bucket_reason_lines = self._normalize_reviewer_feedback_bucket(
                bucket_label=owner,
                bucket_payload=raw_payload.get(owner),
                task_id_required=False,
                dispatched_task_ids=dispatched_task_ids,
            )
            reason_lines.extend(bucket_reason_lines)
            if normalized_bucket is not None:
                normalized[owner] = normalized_bucket
        if "ai_user" not in raw_payload:
            reason_lines.append("Reviewer feedback_ref is missing top-level bucket 'ai_user'.")
        elif not isinstance(raw_payload.get("ai_user"), list):
            reason_lines.append("Reviewer feedback_ref.ai_user must be a list.")
        else:
            normalized_ai_user: list[dict[str, object]] = []
            seen_task_ids: set[str] = set()
            for index, bucket_payload in enumerate(list(raw_payload.get("ai_user") or []), start=1):
                bucket_label = f"ai_user[{index}]"
                normalized_bucket, bucket_reason_lines = self._normalize_reviewer_feedback_bucket(
                    bucket_label=bucket_label,
                    bucket_payload=bucket_payload,
                    task_id_required=True,
                    dispatched_task_ids=dispatched_task_ids,
                )
                reason_lines.extend(bucket_reason_lines)
                if normalized_bucket is None:
                    continue
                task_id = str(normalized_bucket.get("task_id") or "").strip()
                if task_id in seen_task_ids:
                    reason_lines.append(
                        f"{bucket_label}.task_id duplicates another ai_user feedback bucket."
                    )
                    continue
                seen_task_ids.add(task_id)
                normalized_ai_user.append(normalized_bucket)
            normalized["ai_user"] = sorted(
                normalized_ai_user,
                key=lambda item: str(item.get("task_id") or ""),
            )
        if reason_lines:
            return None, reason_lines
        return normalized, []

    def _clear_workspace_feedback_ledger(self, workspace_root: Path, *, node_id: str) -> None:
        active_path = node_active_feedback_path(workspace_root, node_id)
        active_path.unlink(missing_ok=True)

    def _load_active_feedback_ledger(
        self,
        workspace_root: str | Path,
        *,
        node_id: str,
    ) -> dict[str, object] | None:
        active_path = node_active_feedback_path(workspace_root, node_id)
        if not active_path.is_file():
            return None
        try:
            payload = json.loads(active_path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def _feedback_bucket_has_blocking_findings(self, bucket_payload: object) -> bool:
        if not isinstance(bucket_payload, dict):
            return False
        findings = bucket_payload.get("findings")
        if not isinstance(findings, list):
            return False
        return any(
            isinstance(finding, dict) and bool(finding.get("blocking"))
            for finding in findings
        )

    def _blocking_ai_user_feedback_task_ids(
        self,
        ledger_payload: dict[str, object],
    ) -> list[str]:
        buckets = ledger_payload.get("ai_user")
        if not isinstance(buckets, list):
            return []
        task_ids: list[str] = []
        for bucket in buckets:
            if not isinstance(bucket, dict):
                continue
            if not self._feedback_bucket_has_blocking_findings(bucket):
                continue
            task_id = str(bucket.get("task_id") or "").strip()
            if task_id:
                task_ids.append(task_id)
        return sorted(set(task_ids))

    def _select_feedback_restart_target(
        self,
        record: NodeRuntimeRecord,
    ) -> tuple[str | None, bool, list[str]]:
        ledger_payload = self._load_active_feedback_ledger(
            record.workspace_root,
            node_id=record.node_id,
        )
        if ledger_payload is None:
            return None, False, []
        if self._feedback_bucket_has_blocking_findings(ledger_payload.get("implementer")):
            return "implementer", False, []
        if self._feedback_bucket_has_blocking_findings(ledger_payload.get("checker")):
            return "checker", False, []
        rerun_tester = self._feedback_bucket_has_blocking_findings(ledger_payload.get("tester"))
        ai_user_task_ids = self._blocking_ai_user_feedback_task_ids(ledger_payload)
        if rerun_tester or ai_user_task_ids:
            return "tasks", rerun_tester, ai_user_task_ids
        return None, False, []

    def _build_actor_feedback_prelude(
        self,
        record: NodeRuntimeRecord,
        *,
        actor_kind: ActorKind,
        task_id: str | None = None,
    ) -> list[str]:
        ledger_payload = self._load_active_feedback_ledger(
            record.workspace_root,
            node_id=record.node_id,
        )
        if ledger_payload is None:
            return []
        findings: list[dict[str, object]] = []
        if actor_kind is ActorKind.IMPLEMENTER:
            bucket = ledger_payload.get("implementer")
            if isinstance(bucket, dict):
                findings = [
                    dict(item)
                    for item in list(bucket.get("findings") or [])
                    if isinstance(item, dict)
                ]
        elif actor_kind is ActorKind.EVALUATOR_CHECKER:
            bucket = ledger_payload.get("checker")
            if isinstance(bucket, dict):
                findings = [
                    dict(item)
                    for item in list(bucket.get("findings") or [])
                    if isinstance(item, dict)
                ]
        elif actor_kind is ActorKind.EVALUATOR_TESTER:
            bucket = ledger_payload.get("tester")
            if isinstance(bucket, dict):
                findings = [
                    dict(item)
                    for item in list(bucket.get("findings") or [])
                    if isinstance(item, dict)
                ]
        elif actor_kind is ActorKind.EVALUATOR_AI_USER:
            normalized_task_id = str(task_id or "").strip()
            for bucket in list(ledger_payload.get("ai_user") or []):
                if not isinstance(bucket, dict):
                    continue
                if str(bucket.get("task_id") or "").strip() != normalized_task_id:
                    continue
                findings = [
                    dict(item)
                    for item in list(bucket.get("findings") or [])
                    if isinstance(item, dict)
                ]
                break
        if not findings:
            return []
        lines = [
            "Router active feedback for this actor:",
            f"- Active feedback ledger: {node_active_feedback_path(record.workspace_root, record.node_id)}",
            f"- Feedback history reference: {node_feedback_history_path(record.workspace_root, record.node_id)}",
            "- History may mention older snapshots or superseded task ids; current-pass outputs must use the current dispatched snapshot id injected by router.",
            "- Active feedback is routing context, not authoritative proof. If it conflicts with current concrete evidence or current-node split truth injected by router, address that conflict explicitly instead of inheriting the finding blindly.",
        ]
        if findings:
            lines.append("- Active findings:")
            for finding in findings:
                blocking = bool(finding.get("blocking"))
                summary = str(finding.get("summary") or "").strip()
                evidence_ref = str(finding.get("evidence_ref") or "").strip()
                if not summary:
                    continue
                prefix = "BLOCKING" if blocking else "NON_BLOCKING"
                if evidence_ref:
                    lines.append(f"- [{prefix}] {summary} (evidence: {evidence_ref})")
                else:
                    lines.append(f"- [{prefix}] {summary}")
        return lines

    def _materialize_reviewer_feedback_ledger(
        self,
        *,
        record: NodeRuntimeRecord,
        actor_attempt_count: int,
        verdict_kind: str,
        report_ref: str,
        feedback_submission_ref: Path,
        feedback_submission: dict[str, object],
    ) -> Path:
        workspace_root = Path(record.workspace_root).expanduser().resolve()
        snapshot_path = reviewer_submission_snapshot_path(
            workspace_root,
            node_id=record.node_id,
            attempt_count=actor_attempt_count,
        )
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(
            json.dumps(feedback_submission, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        active_payload = {
            "version": 2,
            "node_id": str(record.node_id),
            "reviewer_attempt_count": int(actor_attempt_count),
            "snapshot_id": str(feedback_submission.get("snapshot_id") or ""),
            "verdict_kind": str(verdict_kind),
            "report_ref": str(report_ref),
            "feedback_submission_ref": str(feedback_submission_ref),
            "submission_snapshot_ref": str(snapshot_path),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "implementer": dict(feedback_submission.get("implementer") or {}),
            "checker": dict(feedback_submission.get("checker") or {}),
            "tester": dict(feedback_submission.get("tester") or {}),
            "ai_user": list(feedback_submission.get("ai_user") or []),
        }
        active_path = node_active_feedback_path(workspace_root, record.node_id)
        active_path.parent.mkdir(parents=True, exist_ok=True)
        active_path.write_text(
            json.dumps(active_payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        history_path = node_feedback_history_path(workspace_root, record.node_id)
        history_path.parent.mkdir(parents=True, exist_ok=True)
        with history_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(active_payload, sort_keys=True, separators=(",", ":")) + "\n")
        return active_path

    def _validate_actor_completion(
        self,
        *,
        record: NodeRuntimeRecord,
        item: StoredRouterInboxItem,
        component,
        payload: dict[str, object],
    ) -> ActorCompletionCheck:
        reason_lines: list[str] = []
        exit_code = payload.get("exit_code")
        signal_name = str(payload.get("signal_name") or "").strip()
        if signal_name:
            reason_lines.append(f"The previous attempt exited via signal {signal_name}.")
        elif exit_code is None:
            reason_lines.append("The previous attempt did not report a normal exit code.")
        elif int(exit_code) != 0:
            reason_lines.append(f"The previous attempt exited with code {int(exit_code)}.")
        if int(component.consecutive_no_progress) >= ACTOR_MAX_NO_PROGRESS_WINDOWS:
            reason_lines.append(
                f"Router terminated the previous attempt after {ACTOR_MAX_NO_PROGRESS_WINDOWS} consecutive output windows without progress."
            )

        completion_path = actor_completion_record_path(
            record.workspace_root,
            item.actor_kind,
            node_id=record.node_id,
            task_id=str(component.task_id or "").strip() or None,
        )
        completion_payload: dict[str, object] | None = None
        if not completion_path.exists() or not completion_path.is_file():
            reason_lines.append("No completion record was written via router control.")
        else:
            try:
                loaded_payload = json.loads(completion_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                reason_lines.append("The completion record is not valid JSON.")
            else:
                if not isinstance(loaded_payload, dict):
                    reason_lines.append("The completion record must decode to a JSON object.")
                else:
                    completion_payload = dict(loaded_payload)
        if completion_payload is not None:
            if str(completion_payload.get("node_id") or "").strip() != str(record.node_id):
                reason_lines.append("The completion record node_id does not match this node.")
            if str(completion_payload.get("actor_kind") or "").strip() != item.actor_kind.value:
                reason_lines.append("The completion record actor_kind does not match the active actor.")
            try:
                completion_attempt = int(completion_payload.get("attempt_count"))
            except (TypeError, ValueError):
                completion_attempt = -1
            if completion_attempt != int(item.attempt_count):
                reason_lines.append("The completion record attempt_count does not match the active attempt.")
            try:
                completion_pid = int(completion_payload.get("pid"))
            except (TypeError, ValueError):
                completion_pid = -1
            if completion_pid != int(component.pid or 0):
                reason_lines.append("The completion record pid does not match the active process.")
            recorded_birth = completion_payload.get("process_birth_time")
            if component.process_birth_time is not None:
                try:
                    completion_birth = float(recorded_birth)
                except (TypeError, ValueError):
                    completion_birth = None
                if completion_birth != float(component.process_birth_time):
                    reason_lines.append(
                        "The completion record process_birth_time does not match the active process."
                    )
            if not str(completion_payload.get("completed_at") or "").strip():
                reason_lines.append("The completion record must define completed_at.")
            if item.actor_kind is ActorKind.EVALUATOR_CHECKER:
                tasks_ref = self._resolve_existing_payload_file(
                    completion_payload.get("tasks_ref"),
                )
                if tasks_ref is None:
                    reason_lines.append("Checker completion must define an existing tasks_ref file.")
                else:
                    expected_tasks_ref = checker_current_tasks_path(
                        record.workspace_root,
                        record.node_id,
                    )
                    if tasks_ref != expected_tasks_ref:
                        reason_lines.append(
                            "Checker completion must use the canonical current manifest path "
                            f"{expected_tasks_ref}; rewrite that file and resubmit."
                        )
                    try:
                        manifest = self._load_checker_task_manifest(tasks_ref)
                    except RouterBusinessError as exc:
                        reason_lines.append(exc.message)
                    else:
                        if int(manifest.get("version") or 0) != 2:
                            reason_lines.append(
                                "Checker completion must submit manifest version=2 with inline task entries."
                            )
                        if not reason_lines:
                            try:
                                accepted_tasks_ref = self._snapshot_checker_tasks(
                                    record=record,
                                    tasks_ref=tasks_ref,
                                    attempt_count=int(component.attempt_count),
                                )
                            except RouterBusinessError as exc:
                                reason_lines.append(exc.message)
                            else:
                                completion_payload["tasks_ref"] = str(accepted_tasks_ref)
            elif item.actor_kind is ActorKind.EVALUATOR_TESTER:
                result_ref = self._resolve_existing_payload_file(
                    completion_payload.get("result_ref"),
                )
                if result_ref is None:
                    reason_lines.append("Evaluator tester completion must define an existing result_ref file.")
                else:
                    expected_result_ref = tester_current_result_path(
                        record.workspace_root,
                        record.node_id,
                    )
                    if result_ref != expected_result_ref:
                        reason_lines.append(
                            "Evaluator tester completion must use the canonical current result path "
                            f"{expected_result_ref}; rewrite that file and resubmit."
                        )
                    reason_lines.extend(
                        self._validate_tester_result_file(
                            result_ref=result_ref,
                            record=record,
                        )
                    )
                    if not reason_lines:
                        try:
                            accepted_result_ref = self._snapshot_runtime_output(
                                source_ref=result_ref,
                                target_ref=tester_accepted_result_path(
                                    record.workspace_root,
                                    record.node_id,
                                    attempt_count=int(component.attempt_count),
                                ),
                                label="Evaluator tester result",
                            )
                        except RouterBusinessError as exc:
                            reason_lines.append(exc.message)
                        else:
                            completion_payload["result_ref"] = str(accepted_result_ref)
            elif item.actor_kind is ActorKind.EVALUATOR_AI_USER:
                task_id = str(completion_payload.get("task_id") or "").strip()
                if not task_id:
                    reason_lines.append("Evaluator AI-user completion must define task_id.")
                else:
                    try:
                        checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
                        if not checker_tasks_ref:
                            raise InvalidCheckerTasksError(
                                "checker task manifest is missing while validating evaluator AI-user completion"
                            )
                        dispatched_task_ids = {
                            str(task["task_id"])
                            for task in self._resolve_checker_tasks(Path(checker_tasks_ref))
                        }
                    except RouterBusinessError as exc:
                        reason_lines.append(
                            f"Evaluator AI-user completion requires a valid checker task manifest: {exc.message}"
                        )
                    else:
                        if task_id not in dispatched_task_ids:
                            reason_lines.append(
                                "Evaluator AI-user completion task_id does not match any router-dispatched evaluator task."
                            )
                result_ref = self._resolve_existing_payload_file(
                    completion_payload.get("result_ref"),
                )
                if result_ref is None:
                    reason_lines.append("Evaluator AI-user completion must define an existing result_ref file.")
                else:
                    expected_result_ref = ai_user_current_result_path(
                        record.workspace_root,
                        record.node_id,
                        task_id=task_id,
                    )
                    if result_ref != expected_result_ref:
                        reason_lines.append(
                            "Evaluator AI-user completion must use the canonical current result path "
                            f"{expected_result_ref}; rewrite that file and resubmit."
                        )
                    reason_lines.extend(
                        self._validate_ai_user_result_file(
                            result_ref=result_ref,
                            record=record,
                            task_id=task_id,
                        )
                    )
                    if not reason_lines:
                        try:
                            accepted_result_ref = self._snapshot_runtime_output(
                                source_ref=result_ref,
                                target_ref=ai_user_accepted_result_path(
                                    record.workspace_root,
                                    record.node_id,
                                    task_id=task_id,
                                    attempt_count=int(component.attempt_count),
                                ),
                                label="Evaluator AI-user result",
                            )
                        except RouterBusinessError as exc:
                            reason_lines.append(exc.message)
                        else:
                            completion_payload["result_ref"] = str(accepted_result_ref)
            elif item.actor_kind is ActorKind.EVALUATOR_REVIEWER:
                verdict_kind = str(completion_payload.get("verdict_kind") or "").strip().upper()
                if verdict_kind not in {"OK", "IMPLEMENTER_ACTION_REQUIRED", "EVALUATOR_FAULT"}:
                    reason_lines.append(
                        "Reviewer completion verdict_kind must be one of OK, IMPLEMENTER_ACTION_REQUIRED, or EVALUATOR_FAULT."
                    )
                else:
                    completion_payload["verdict_kind"] = verdict_kind
                report_ref = self._resolve_existing_payload_file(
                    completion_payload.get("report_ref"),
                )
                if report_ref is None:
                    reason_lines.append("Reviewer completion must define an existing report_ref file.")
                else:
                    expected_report_ref = reviewer_current_report_path(
                        record.workspace_root,
                        record.node_id,
                    )
                    if report_ref != expected_report_ref:
                        reason_lines.append(
                            "Reviewer completion must use the canonical current report path "
                            f"{expected_report_ref}; rewrite that file and resubmit."
                        )
                    reason_lines.extend(
                        self._validate_reviewer_report_file(
                            report_ref=report_ref,
                            record=record,
                        )
                    )
                feedback_ref = self._resolve_existing_payload_file(
                    completion_payload.get("feedback_ref"),
                )
                if feedback_ref is not None:
                    expected_feedback_ref = reviewer_current_feedback_path(
                        record.workspace_root,
                        record.node_id,
                    )
                    if feedback_ref != expected_feedback_ref:
                        reason_lines.append(
                            "Reviewer completion must use the canonical current feedback path "
                            f"{expected_feedback_ref}; rewrite that file and resubmit."
                        )
                    normalized_feedback_submission, feedback_reason_lines = (
                        self._resolve_reviewer_feedback_submission(
                            record=record,
                            feedback_ref=feedback_ref,
                        )
                    )
                    reason_lines.extend(feedback_reason_lines)
                    if normalized_feedback_submission is not None and report_ref is not None:
                        try:
                            accepted_feedback_ref = self._snapshot_runtime_output(
                                source_ref=feedback_ref,
                                target_ref=reviewer_accepted_feedback_path(
                                    record.workspace_root,
                                    record.node_id,
                                    attempt_count=int(component.attempt_count),
                                ),
                                label="Reviewer feedback",
                            )
                            accepted_report_ref = self._snapshot_runtime_output(
                                source_ref=report_ref,
                                target_ref=reviewer_accepted_report_path(
                                    record.workspace_root,
                                    record.node_id,
                                    attempt_count=int(component.attempt_count),
                                ),
                                label="Reviewer report",
                            )
                        except RouterBusinessError as exc:
                            reason_lines.append(exc.message)
                        else:
                            completion_payload["feedback_ref"] = str(accepted_feedback_ref)
                            completion_payload["report_ref"] = str(accepted_report_ref)
                        completion_payload["feedback_submission"] = normalized_feedback_submission
                if verdict_kind == "OK":
                    result_commit, git_reason_lines = self._resolve_workspace_result_commit(
                        Path(record.workspace_root)
                    )
                    if not result_commit:
                        reason_lines.extend(git_reason_lines)
                    else:
                        completion_payload["result_commit"] = result_commit
            elif item.actor_kind is ActorKind.KERNEL:
                if bool(record.escalated_to_kernel):
                    if str(completion_payload.get("verdict_kind") or "").strip():
                        reason_lines.append(
                            "Ordinary kernel completion must not define verdict_kind."
                        )
                    if completion_payload.get("request_seq") not in {None, ""}:
                        reason_lines.append(
                            "Ordinary kernel completion must not define request_seq."
                        )
                    if completion_payload.get("reason_ref") not in {None, ""}:
                        reason_lines.append(
                            "Ordinary kernel completion must not define reason_ref."
                        )
                else:
                    verdict_kind = str(completion_payload.get("verdict_kind") or "").strip().upper()
                    if verdict_kind not in {"APPROVE", "REJECT"}:
                        reason_lines.append(
                            "Kernel completion verdict_kind must be one of APPROVE or REJECT."
                        )
                    else:
                        completion_payload["verdict_kind"] = verdict_kind
                    try:
                        request_seq = int(completion_payload.get("request_seq"))
                    except (TypeError, ValueError):
                        request_seq = 0
                    if request_seq <= 0:
                        reason_lines.append("Kernel completion must define a positive request_seq.")
                    else:
                        request_item = self._store.load_event(request_seq)
                        if request_item is None or request_item.event_type != "RequestSplit":
                            reason_lines.append(
                                "Kernel completion request_seq must refer to an existing RequestSplit."
                            )
                        else:
                            completion_payload["request_seq"] = int(request_seq)
                    reason_ref = completion_payload.get("reason_ref")
                    if verdict_kind == "REJECT":
                        resolved_reason = self._resolve_existing_payload_file(reason_ref)
                        if resolved_reason is None:
                            reason_lines.append(
                                "Kernel REJECT completion must define an existing reason_ref file."
                            )
                        else:
                            completion_payload["reason_ref"] = str(resolved_reason)
                    elif reason_ref not in {None, ""}:
                        resolved_reason = self._resolve_existing_payload_file(reason_ref)
                        if resolved_reason is None:
                            reason_lines.append(
                                "Kernel completion reason_ref must be an existing file when provided."
                            )
                        else:
                            completion_payload["reason_ref"] = str(resolved_reason)

        output_fingerprint_after = workspace_output_fingerprint(record.workspace_root)
        evaluator_runtime_only_actor = item.actor_kind in {
            ActorKind.EVALUATOR_CHECKER,
            ActorKind.EVALUATOR_TESTER,
            ActorKind.EVALUATOR_AI_USER,
            ActorKind.EVALUATOR_REVIEWER,
        }
        if (
            not evaluator_runtime_only_actor
            and item.actor_kind is not ActorKind.KERNEL
            and str(output_fingerprint_after) == str(component.workspace_fingerprint_before)
        ):
            if bool(component.saw_output_in_attempt):
                reason_lines.append("No substantive workspace output changed relative to launch.")
            else:
                reason_lines.append(
                    "No substantive workspace output changed relative to launch, and no output window was observed."
                )
        if (
            not reason_lines
            and completion_payload is not None
            and item.actor_kind is ActorKind.EVALUATOR_CHECKER
        ):
            tasks_ref = Path(str(completion_payload.get("tasks_ref") or "")).expanduser().resolve()
            try:
                completion_payload["tasks_ref"] = str(
                    self._snapshot_checker_tasks(
                        record=record,
                        tasks_ref=tasks_ref,
                        attempt_count=int(component.attempt_count),
                    )
                )
            except RouterBusinessError as exc:
                reason_lines.append(exc.message)
        return ActorCompletionCheck(
            accepted=not reason_lines,
            payload=completion_payload,
            reason_lines=reason_lines,
            output_fingerprint_after=output_fingerprint_after,
        )

    def _build_after_exit_recovery_prelude(
        self,
        *,
        actor_kind: ActorKind,
        reason_lines: list[str],
    ) -> list[str]:
        routerctl = (self._repo_root / "scripts" / "routerctl.sh").resolve()
        lines = [
            f"Router recovery: your previous {actor_kind.value} attempt exited before reaching a valid completion checkpoint.",
            "Continue from the current workspace state and finish the same task.",
            "Router rejected the previous attempt for these reasons:",
        ]
        lines.extend([f"- {line}" for line in reason_lines])
        lines.extend(self._build_completion_contract_lines(actor_kind=actor_kind, routerctl=routerctl))
        return lines

    def _build_evaluator_fault_checker_prelude(
        self,
        *,
        report_ref: str,
        feedback_ref: str | None,
        prior_checker_tasks_ref: str | None,
    ) -> list[str]:
        lines = [
            "Reviewer reported evaluator-side fault.",
            f"Reviewer report reference: {report_ref}",
        ]
        feedback_ref_text = str(feedback_ref or "").strip()
        if feedback_ref_text:
            lines.append(f"Reviewer feedback reference: {feedback_ref_text}")
        prior_checker_tasks_ref_text = str(prior_checker_tasks_ref or "").strip()
        if prior_checker_tasks_ref_text:
            lines.append(f"Previous checker manifest reference: {prior_checker_tasks_ref_text}")
        lines.extend(
            [
                "Previous evaluator wave ended in EVALUATOR_FAULT. Do not reproduce the prior checker manifest blindly.",
                "Read the reviewer report and feedback first, identify the evaluator-side faults they call out, and correct those faults before emitting a new checker manifest.",
            ]
        )
        return lines

    def _recover_actor_after_exit(
        self,
        *,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
        component,
        reason_lines: list[str],
    ) -> NodeRuntimeRecord:
        failed_exits = int(component.consecutive_failed_exits) + 1
        if actor_kind is ActorKind.KERNEL:
            pending_record = replace(
                self._mark_component_inactive(
                    record,
                    actor_kind,
                    task_id=str(component.task_id or "").strip() or None,
                ),
                components={
                    **record.components,
                    component_key(
                        actor_kind=actor_kind,
                        task_id=str(component.task_id or "").strip() or None,
                    ): replace(
                        component,
                        status=ComponentStatus.INACTIVE,
                        consecutive_failed_exits=failed_exits,
                    ),
                },
                pending_prelude_lines=self._build_after_exit_recovery_prelude(
                    actor_kind=actor_kind,
                    reason_lines=reason_lines,
                ),
            )
            if failed_exits >= ACTOR_MAX_FAILED_EXITS:
                pending = next(iter(self._store.list_pending_split_reviews()), None)
                if pending is not None:
                    self._clear_split_request_after_kernel_failure(
                        pending.node_id,
                        pending.request_seq,
                    )
                pending_record = replace(
                    pending_record,
                    components={
                        **pending_record.components,
                        component_key(
                            actor_kind=actor_kind,
                            task_id=str(component.task_id or "").strip() or None,
                        ): replace(
                            pending_record.components[
                                component_key(
                                    actor_kind=actor_kind,
                                    task_id=str(component.task_id or "").strip() or None,
                                )
                            ],
                            consecutive_failed_exits=0,
                        ),
                    },
                    pending_prelude_lines=[],
                )
            self._store.upsert_node(pending_record)
            return self._relaunch_pending_actor_for_kind(
                pending_record=pending_record,
                actor_kind=actor_kind,
            )
        if failed_exits >= ACTOR_MAX_FAILED_EXITS:
            terminal_record = self._mark_component_terminal_failed(
                record,
                actor_kind,
                task_id=str(component.task_id or "").strip() or None,
                failed_exits=failed_exits,
            )
            self._store.upsert_node(terminal_record)
            self._maybe_request_router_terminal_shutdown()
            return terminal_record
        inactive_component = replace(
            component,
            status=ComponentStatus.INACTIVE,
            consecutive_failed_exits=failed_exits,
        )
        pending_record = replace(
            self._mark_component_inactive(
                record,
                actor_kind,
                task_id=str(component.task_id or "").strip() or None,
            ),
            components={
                **record.components,
                component_key(
                    actor_kind=actor_kind,
                    task_id=str(component.task_id or "").strip() or None,
                ): inactive_component,
            },
            pending_prelude_lines=self._build_after_exit_recovery_prelude(
                actor_kind=actor_kind,
                reason_lines=reason_lines,
            ),
        )
        self._store.upsert_node(pending_record)
        return self._relaunch_pending_actor_for_kind(
            pending_record=pending_record,
            actor_kind=actor_kind,
        )

    def _advance_after_valid_actor_completion(
        self,
        *,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
        task_id: str | None,
        completion_payload: dict[str, object],
    ) -> NodeRuntimeRecord:
        completed_record = self._mark_component_completed(
            record,
            actor_kind,
            task_id=task_id,
        )
        if actor_kind is ActorKind.IMPLEMENTER or (
            actor_kind is ActorKind.KERNEL and bool(record.escalated_to_kernel)
        ):
            checker_pending = replace(
                completed_record,
                evaluator_phase="checker",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
            )
            self._store.upsert_node(checker_pending)
            return self._launch_checker_for_node(checker_pending)
        if actor_kind is ActorKind.EVALUATOR_CHECKER:
            tasks_record = replace(
                completed_record,
                current_components=[],
                evaluator_phase="tasks",
                checker_tasks_ref=str(completion_payload.get("tasks_ref") or ""),
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
            )
            self._store.upsert_node(tasks_record)
            return self._continue_tasks_phase(tasks_record)
        if actor_kind is ActorKind.EVALUATOR_TESTER:
            result_ref = str(completion_payload.get("result_ref") or "").strip()
            try:
                tasks = self._resolve_checker_tasks(Path(completed_record.checker_tasks_ref))
            except RouterBusinessError as exc:
                checker_pending = replace(
                    completed_record,
                    pending_prelude_lines=[
                        "Router evaluator task-manifest recovery.",
                        f"The previous checker task manifest became invalid: {exc.message}",
                        "Re-run checker and write a fresh valid task manifest before evaluator tasks continue.",
                    ],
                )
                return self._launch_checker_for_node(checker_pending)
            task_result_refs = {
                str(task): dict(result_map)
                for task, result_map in completed_record.task_result_refs.items()
            }
            for task in tasks:
                task_result_refs.setdefault(str(task["task_id"]), {})[
                    ActorKind.EVALUATOR_TESTER.value
                ] = result_ref
            tester_completed = replace(
                completed_record,
                task_result_refs=task_result_refs,
                pending_prelude_lines=[],
            )
            self._store.upsert_node(tester_completed)
            return self._continue_tasks_phase(tester_completed)
        if actor_kind is ActorKind.EVALUATOR_AI_USER:
            task_id = str(completion_payload.get("task_id") or "").strip()
            result_ref = str(completion_payload.get("result_ref") or "").strip()
            task_result_refs = {
                str(task): dict(result_map)
                for task, result_map in completed_record.task_result_refs.items()
            }
            task_result_refs.setdefault(task_id, {})[actor_kind.value] = result_ref
            task_pending = replace(
                completed_record,
                task_result_refs=task_result_refs,
                pending_prelude_lines=[],
            )
            self._store.upsert_node(task_pending)
            return self._continue_tasks_phase(task_pending)
        if actor_kind is ActorKind.EVALUATOR_REVIEWER:
            verdict_kind = str(completion_payload.get("verdict_kind") or "").strip().upper()
            report_ref = str(completion_payload.get("report_ref") or "").strip()
            result_commit = str(completion_payload.get("result_commit") or "").strip()
            self._clear_workspace_feedback_ledger(
                Path(record.workspace_root),
                node_id=record.node_id,
            )
            if completion_payload.get("feedback_submission") is not None:
                self._materialize_reviewer_feedback_ledger(
                    record=completed_record,
                    actor_attempt_count=self._component_state_for_kind(
                        completed_record,
                        ActorKind.EVALUATOR_REVIEWER,
                    ).attempt_count,
                    verdict_kind=verdict_kind,
                    report_ref=report_ref,
                    feedback_submission_ref=Path(
                        str(completion_payload.get("feedback_ref") or "")
                    ).expanduser().resolve(),
                    feedback_submission=dict(
                        completion_payload.get("feedback_submission") or {}
                    ),
                )
            reviewer_completed = replace(
                completed_record,
                reviewer_verdict_kind=verdict_kind,
                reviewer_report_ref=report_ref,
                pending_prelude_lines=[],
            )
            if verdict_kind == "OK":
                final_record = replace(
                    reviewer_completed,
                    current_components=[],
                    evaluator_phase="",
                    result_commit=result_commit,
                )
                self._archive_absorbed_direct_children(final_record)
                self._store.upsert_node(final_record)
                if str(record.node_id) == KERNEL_NODE_ID and bool(record.escalated_to_kernel):
                    self._mark_router_completed(
                        result_commit=result_commit,
                        report_ref=report_ref,
                    )
                    return final_record
                if str(record.node_id) == ROOT_NODE_ID:
                    return self._activate_final_kernel_takeover(source_record=final_record)
                return final_record
            restart_target, rerun_tester, rerun_ai_user_task_ids = self._select_feedback_restart_target(
                reviewer_completed
            )
            if restart_target == "implementer":
                implementer_pending = replace(
                    reviewer_completed,
                    current_components=[],
                    evaluator_phase="",
                    pending_prelude_lines=[
                        "Router resume after evaluator reviewer feedback.",
                        f"Reviewer report reference: {report_ref}",
                        "Read the reviewer feedback and either fix the implementation or argue concretely with evidence.",
                    ],
                )
                self._store.upsert_node(implementer_pending)
                return self._launch_implementer_for_record(implementer_pending)
            if restart_target == "checker":
                feedback_ref = str(completion_payload.get("feedback_ref") or "").strip()
                checker_pending = replace(
                    reviewer_completed,
                    current_components=[],
                    evaluator_phase="checker",
                    checker_tasks_ref="",
                    task_result_refs={},
                    pending_prelude_lines=(
                        self._build_evaluator_fault_checker_prelude(
                            report_ref=report_ref,
                            feedback_ref=feedback_ref,
                            prior_checker_tasks_ref=str(reviewer_completed.checker_tasks_ref or ""),
                        )
                        if verdict_kind == "EVALUATOR_FAULT"
                        else [
                            "Reviewer reported evaluator-side fault.",
                            f"Reviewer report reference: {report_ref}",
                            "Re-run evaluator from checker and keep evaluator-side blame on the evaluator path.",
                        ]
                    ),
                )
                self._store.upsert_node(checker_pending)
                return self._launch_checker_for_node(checker_pending)
            if restart_target == "tasks":
                task_result_refs = {
                    str(task_id): dict(result_map)
                    for task_id, result_map in reviewer_completed.task_result_refs.items()
                }
                if rerun_tester:
                    for result_map in task_result_refs.values():
                        result_map.pop(ActorKind.EVALUATOR_TESTER.value, None)
                for rerun_task_id in rerun_ai_user_task_ids:
                    task_result_refs.setdefault(str(rerun_task_id), {}).pop(
                        ActorKind.EVALUATOR_AI_USER.value,
                        None,
                    )
                tasks_pending = replace(
                    reviewer_completed,
                    current_components=[],
                    evaluator_phase="tasks",
                    task_result_refs=task_result_refs,
                    pending_prelude_lines=[
                        "Router resume after evaluator reviewer feedback.",
                        f"Reviewer report reference: {report_ref}",
                        "Re-run only the evaluator lanes that still have active reviewer feedback.",
                    ],
                )
                self._store.upsert_node(tasks_pending)
                return self._continue_tasks_phase(tasks_pending)
            if verdict_kind == "IMPLEMENTER_ACTION_REQUIRED":
                implementer_pending = replace(
                    reviewer_completed,
                    current_components=[],
                    evaluator_phase="",
                    pending_prelude_lines=[
                        "Router resume after evaluator reviewer feedback.",
                        f"Reviewer report reference: {report_ref}",
                        "Read the reviewer feedback and either fix the implementation or argue concretely with evidence.",
                    ],
                )
                self._store.upsert_node(implementer_pending)
                return self._launch_implementer_for_record(implementer_pending)
            if verdict_kind == "EVALUATOR_FAULT":
                feedback_ref = str(completion_payload.get("feedback_ref") or "").strip()
                checker_pending = replace(
                    reviewer_completed,
                    current_components=[],
                    evaluator_phase="checker",
                    checker_tasks_ref="",
                    task_result_refs={},
                    pending_prelude_lines=self._build_evaluator_fault_checker_prelude(
                        report_ref=report_ref,
                        feedback_ref=feedback_ref,
                        prior_checker_tasks_ref=str(reviewer_completed.checker_tasks_ref or ""),
                    ),
                )
                self._store.upsert_node(checker_pending)
                return self._launch_checker_for_node(checker_pending)
            raise InvalidReviewerVerdictError(
                "reviewer verdict must be one of OK, IMPLEMENTER_ACTION_REQUIRED, or EVALUATOR_FAULT"
            )
        if actor_kind is ActorKind.KERNEL:
            verdict_kind = str(completion_payload.get("verdict_kind") or "").strip().upper()
            request_seq = int(completion_payload.get("request_seq") or 0)
            request_item = self._store.load_event(request_seq)
            if request_item is None or request_item.event_type != "RequestSplit":
                raise InvalidReviewerVerdictError(
                    "kernel completion request_seq must refer to a durable RequestSplit"
                )
            kernel_actor = self._actor_ref(
                node_id=record.node_id,
                actor_kind=ActorKind.KERNEL,
                attempt_count=self._component_state_for_kind(
                    completed_record,
                    ActorKind.KERNEL,
                ).attempt_count,
            )
            if verdict_kind == "APPROVE":
                self._store.upsert_node(completed_record)
                self._store.append_event(
                    ApproveSplit(
                        actor=kernel_actor,
                        target_node_id=request_item.node_id,
                        request_seq=request_seq,
                        approved_at=datetime.now(timezone.utc),
                    )
                )
                return completed_record
            if verdict_kind == "REJECT":
                self._store.upsert_node(completed_record)
                self._store.append_event(
                    RejectSplit(
                        actor=kernel_actor,
                        target_node_id=request_item.node_id,
                        request_seq=request_seq,
                        rejected_at=datetime.now(timezone.utc),
                        reason_ref=str(completion_payload.get("reason_ref") or "").strip() or None,
                    )
                )
                return completed_record
            raise InvalidReviewerVerdictError(
                "kernel verdict must be one of APPROVE or REJECT"
            )
        self._store.upsert_node(completed_record)
        return completed_record

    def _next_attempt_count(self, record: NodeRuntimeRecord, actor_kind: ActorKind) -> int:
        component_keys = component_keys_for_kind(record.components, actor_kind)
        if not component_keys:
            return 1
        return max(int(record.components[key].attempt_count) for key in component_keys) + 1

    def _component_state_for_actor(
        self,
        record: NodeRuntimeRecord,
        actor_ref,
    ):
        return record.components.get(component_key_for_actor(actor_ref))

    def _component_state_for_kind(
        self,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
    ):
        component_keys = component_keys_for_kind(record.components, actor_kind)
        if not component_keys:
            return None
        latest_key = max(
            component_keys,
            key=lambda item: int(record.components[item].attempt_count),
        )
        return record.components[latest_key]

    def _replace_current_components(
        self,
        record: NodeRuntimeRecord,
        current_components: list,
    ) -> NodeRuntimeRecord:
        return replace(record, current_components=list(current_components))

    def _has_current_actor(self, record: NodeRuntimeRecord, actor_ref) -> bool:
        return actor_ref in record.current_components

    def _mark_component_inactive(
        self,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
        *,
        task_id: str | None = None,
    ) -> NodeRuntimeRecord:
        component_state_key = component_key(actor_kind=actor_kind, task_id=task_id)
        component = record.components.get(component_state_key)
        if component is None:
            return record
        updated_components = {
            **record.components,
            component_state_key: replace(component, status=ComponentStatus.INACTIVE),
        }
        updated_record = replace(
            record,
            components=updated_components,
        )
        task_id = str(component.task_id or "").strip()
        next_current_components = [
            actor
            for actor in record.current_components
            if not (
                actor.actor_kind is actor_kind
                and str(actor.task_id or "").strip() == task_id
            )
        ]
        return self._replace_current_components(updated_record, next_current_components)

    def _mark_component_completed(
        self,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
        *,
        task_id: str | None = None,
    ) -> NodeRuntimeRecord:
        component_state_key = component_key(actor_kind=actor_kind, task_id=task_id)
        component = record.components.get(component_state_key)
        if component is None:
            return record
        updated_components = {
            **record.components,
            component_state_key: replace(
                component,
                status=ComponentStatus.COMPLETED,
                consecutive_no_progress=0,
                consecutive_failed_exits=0,
            ),
        }
        updated_record = replace(
            record,
            components=updated_components,
        )
        task_id = str(component.task_id or "").strip()
        next_current_components = [
            actor
            for actor in record.current_components
            if not (
                actor.actor_kind is actor_kind
                and str(actor.task_id or "").strip() == task_id
            )
        ]
        return self._replace_current_components(updated_record, next_current_components)

    def _mark_component_terminal_failed(
        self,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
        *,
        task_id: str | None = None,
        failed_exits: int,
    ) -> NodeRuntimeRecord:
        component_state_key = component_key(actor_kind=actor_kind, task_id=task_id)
        component = record.components.get(component_state_key)
        if component is None:
            return record
        updated_components = {
            **record.components,
            component_state_key: replace(
                component,
                status=ComponentStatus.TERMINAL_FAILED,
                consecutive_failed_exits=int(failed_exits),
            ),
        }
        updated_record = replace(
            record,
            components=updated_components,
        )
        task_id = str(component.task_id or "").strip() or None
        return self._replace_current_components(
            updated_record,
            [
                self._actor_ref(
                    node_id=record.node_id,
                    actor_kind=actor_kind,
                    attempt_count=int(component.attempt_count),
                    task_id=str(component.task_id or "").strip() or None,
                )
            ],
        )

    def _load_checker_task_manifest(self, tasks_ref: Path) -> dict[str, object]:
        resolved = Path(tasks_ref).expanduser().resolve()
        try:
            payload = json.loads(resolved.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise InvalidCheckerTasksError(
                f"checker tasks file does not exist: {resolved}"
            ) from exc
        except json.JSONDecodeError as exc:
            raise InvalidCheckerTasksError(
                f"checker tasks file must be valid JSON: {resolved}"
            ) from exc
        if not isinstance(payload, dict):
            raise InvalidCheckerTasksError("checker tasks payload must decode to an object")
        try:
            version = int(payload.get("version") or 0)
        except (TypeError, ValueError):
            version = -1
        if version not in {1, 2}:
            raise InvalidCheckerTasksError("checker tasks payload must use version=1 or version=2")
        if version == 2:
            allowed_top_keys = {
                "version",
                "snapshot_id",
                "workspace_root",
                "authoritative_final_effects_path",
                "obligations",
                "tasks",
            }
            extra_keys = sorted(set(payload) - allowed_top_keys)
            if extra_keys:
                raise InvalidCheckerTasksError(
                    "checker version=2 manifest contains unsupported top-level keys: "
                    + ", ".join(extra_keys)
                )
            if not isinstance(payload.get("obligations"), list):
                raise InvalidCheckerTasksError(
                    "checker version=2 manifest must define obligations as a list"
                )
        tasks = payload.get("tasks")
        if not isinstance(tasks, list) or not tasks:
            raise InvalidCheckerTasksError(
                "checker tasks payload must contain a non-empty tasks list"
            )
        normalized_tasks: list[dict[str, object]] = []
        seen_task_ids: set[str] = set()
        for task in tasks:
            if not isinstance(task, dict):
                raise InvalidCheckerTasksError("checker tasks entries must be objects")
            task_id = str(task.get("task_id") or "").strip()
            if not task_id:
                raise InvalidCheckerTasksError("checker tasks entries must define task_id")
            if task_id in seen_task_ids:
                raise InvalidCheckerTasksError(
                    f"checker tasks payload must not reuse task_id {task_id!r}"
                )
            seen_task_ids.add(task_id)
            if version == 1:
                task_ref = str(task.get("task_ref") or "").strip()
                if not task_ref:
                    raise InvalidCheckerTasksError(
                        "checker version=1 tasks entries must define task_ref"
                    )
                task_ref_path = Path(task_ref).expanduser().resolve()
                if not task_ref_path.exists() or not task_ref_path.is_file():
                    raise InvalidCheckerTasksError(
                        f"checker task_ref for task {task_id!r} must exist and be a regular file"
                    )
                normalized_tasks.append(
                    {
                        "task_id": task_id,
                        "task_ref": str(task_ref_path),
                        "covered_requirement_ids": list(task.get("covered_requirement_ids") or []),
                        "goal": str(task.get("goal") or "").strip(),
                        "blocking_condition": str(task.get("blocking_condition") or "").strip(),
                        "task_instructions": task_ref_path.read_text(
                            encoding="utf-8",
                            errors="replace",
                        ),
                        "required_evidence": list(task.get("required_evidence") or task.get("required_artifacts") or []),
                    }
                )
                continue
            allowed_task_keys = {
                "task_id",
                "covered_requirement_ids",
                "goal",
                "blocking_condition",
                "task_instructions",
                "required_evidence",
            }
            extra_task_keys = sorted(set(task) - allowed_task_keys)
            if extra_task_keys:
                raise InvalidCheckerTasksError(
                    f"checker version=2 task {task_id!r} contains unsupported keys: {', '.join(extra_task_keys)}"
                )
            covered_requirement_ids = task.get("covered_requirement_ids")
            if not isinstance(covered_requirement_ids, list):
                raise InvalidCheckerTasksError(
                    f"checker version=2 task {task_id!r} must define covered_requirement_ids as a list"
                )
            required_evidence = task.get("required_evidence")
            if not isinstance(required_evidence, list):
                raise InvalidCheckerTasksError(
                    f"checker version=2 task {task_id!r} must define required_evidence as a list"
                )
            goal = str(task.get("goal") or "").strip()
            blocking_condition = str(task.get("blocking_condition") or "").strip()
            task_instructions = str(task.get("task_instructions") or "").strip()
            if not goal or not blocking_condition or not task_instructions:
                raise InvalidCheckerTasksError(
                    f"checker version=2 task {task_id!r} must define goal, blocking_condition, and task_instructions"
                )
            normalized_tasks.append(
                {
                    "task_id": task_id,
                    "covered_requirement_ids": [str(item) for item in covered_requirement_ids],
                    "goal": goal,
                    "blocking_condition": blocking_condition,
                    "task_instructions": task_instructions,
                    "required_evidence": [str(item) for item in required_evidence],
                }
            )
        snapshot_id = str(payload.get("snapshot_id") or "").strip()
        return {
            "version": version,
            "snapshot_id": snapshot_id,
            "workspace_root": str(payload.get("workspace_root") or "").strip(),
            "authoritative_final_effects_path": str(
                payload.get("authoritative_final_effects_path") or ""
            ).strip(),
            "obligations": list(payload.get("obligations") or []),
            "tasks_ref": str(resolved),
            "tasks": normalized_tasks,
        }

    def _resolve_checker_tasks(self, tasks_ref: Path) -> list[dict[str, object]]:
        return list(self._load_checker_task_manifest(tasks_ref).get("tasks") or [])

    def _snapshot_checker_tasks(
        self,
        *,
        record: NodeRuntimeRecord,
        tasks_ref: Path,
        attempt_count: int,
    ) -> Path:
        manifest = self._load_checker_task_manifest(tasks_ref)
        snapshot_id = f"snapshot-{uuid.uuid4().hex}"
        accepted_path = checker_accepted_tasks_path(
            record.workspace_root,
            record.node_id,
            attempt_count=attempt_count,
        )
        accepted_path.parent.mkdir(parents=True, exist_ok=True)
        accepted_payload = {
            "version": 2,
            "snapshot_id": snapshot_id,
            "workspace_root": str(record.workspace_root),
            "authoritative_final_effects_path": str(record.final_effects_file),
            "obligations": list(manifest.get("obligations") or []),
            "tasks": [
                {
                    "task_id": str(task["task_id"]),
                    "covered_requirement_ids": list(task.get("covered_requirement_ids") or []),
                    "goal": str(task.get("goal") or "").strip(),
                    "blocking_condition": str(task.get("blocking_condition") or "").strip(),
                    "task_instructions": str(task.get("task_instructions") or "").strip(),
                    "required_evidence": list(task.get("required_evidence") or []),
                }
                for task in list(manifest.get("tasks") or [])
            ],
        }
        try:
            accepted_path.write_text(
                json.dumps(accepted_payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            return accepted_path
        except Exception as exc:
            accepted_path.unlink(missing_ok=True)
            raise InvalidCheckerTasksError(
                f"checker task manifest snapshot failed: {exc}"
            ) from exc

    def _pending_ai_user_tasks(
        self,
        record: NodeRuntimeRecord,
        *,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
        if not checker_tasks_ref:
            raise InvalidCheckerTasksError("checker task manifest is missing while evaluator_phase=tasks")
        tasks = self._resolve_checker_tasks(Path(checker_tasks_ref))
        task_result_refs = {
            str(task_id): {str(kind): str(ref) for kind, ref in dict(result_map).items()}
            for task_id, result_map in record.task_result_refs.items()
        }
        running_ai_user_task_ids = {
            str(actor.task_id or "").strip()
            for actor in record.current_components
            if actor.actor_kind is ActorKind.EVALUATOR_AI_USER and str(actor.task_id or "").strip()
        }
        pending: list[dict[str, object]] = []
        for task in tasks:
            task_id = str(task["task_id"])
            task_results = task_result_refs.get(task_id, {})
            if ActorKind.EVALUATOR_AI_USER.value in task_results:
                continue
            if task_id in running_ai_user_task_ids:
                continue
            pending.append(dict(task))
            if limit is not None and len(pending) >= int(limit):
                break
        return pending

    def _checker_manifest_context(self, record: NodeRuntimeRecord) -> tuple[str, str]:
        checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
        if not checker_tasks_ref:
            return "", ""
        try:
            manifest = self._load_checker_task_manifest(Path(checker_tasks_ref))
        except RouterBusinessError:
            return checker_tasks_ref, ""
        return checker_tasks_ref, str(manifest.get("snapshot_id") or "").strip()

    def _build_current_node_split_truth_prelude(
        self,
        record: NodeRuntimeRecord,
    ) -> list[str]:
        return [
            f"Current node split_request: {int(record.split_request)}",
            f"Current node split_approved: {int(record.split_approved)}",
            f"Current node child_node_ids: {json.dumps([str(item) for item in record.child_node_ids])}",
        ]

    def _build_serial_task_prelude(
        self,
        *,
        record: NodeRuntimeRecord,
        task_entry: dict[str, object],
    ) -> list[str]:
        task_id = str(task_entry.get("task_id") or "").strip()
        checker_tasks_ref, snapshot_id = self._checker_manifest_context(record)
        result_path = ai_user_current_result_path(
            record.workspace_root,
            record.node_id,
            task_id=task_id,
        )
        artifacts_dir = ai_user_artifacts_dir(
            record.workspace_root,
            record.node_id,
            task_id=task_id,
        )
        return [
            "Router evaluator lane dispatch.",
            f"Evaluator task id: {task_id}",
            f"Current dispatched snapshot id: {snapshot_id or '<pending-snapshot-id>'}",
            f"Authoritative checker manifest path: {checker_tasks_ref}",
            f"Your only durable result path: {result_path}",
            f"Your task-local artifacts directory: {artifacts_dir}",
            *self._build_current_node_split_truth_prelude(record),
            "The JSON object below is your only routed task truth for this lane.",
            "--- BEGIN TASK ENTRY JSON ---",
            json.dumps(task_entry, indent=2, sort_keys=True),
            "--- END TASK ENTRY JSON ---",
            "Complete this evaluator_ai_user task and record completion via router control before exiting.",
        ]

    def _build_tester_review_prelude(self, record: NodeRuntimeRecord) -> list[str]:
        checker_tasks_ref, snapshot_id = self._checker_manifest_context(record)
        lines = [
            "Router evaluator tester dispatch.",
            "Run one global code review over the current workspace changes and write a single review report.",
            f"Current dispatched snapshot id: {snapshot_id or '<pending-snapshot-id>'}",
            f"Authoritative checker manifest path: {checker_tasks_ref}",
            f"Your only durable result path: {tester_current_result_path(record.workspace_root, record.node_id)}",
            "This tester run is global, not per-task.",
        ]
        return lines

    def _build_checker_dispatch_prelude(self, record: NodeRuntimeRecord) -> list[str]:
        return [
            "Router evaluator checker dispatch.",
            f"Your only durable checker manifest path: {checker_current_tasks_path(record.workspace_root, record.node_id)}",
            "Router will assign the dispatched snapshot id when that manifest is accepted.",
            "Do not write checker sidecar truth under workspace root; keep evaluator durability inside .loop/router_runtime.",
        ]

    def _build_reviewer_prelude(self, record: NodeRuntimeRecord) -> list[str]:
        checker_tasks_ref, snapshot_id = self._checker_manifest_context(record)
        lines = [
            "Router evaluator reviewer dispatch.",
            f"Current dispatched snapshot id: {snapshot_id or '<pending-snapshot-id>'}",
            f"Authoritative checker manifest path: {checker_tasks_ref}",
            f"Your only durable feedback path: {reviewer_current_feedback_path(record.workspace_root, record.node_id)}",
            f"Your only durable report path: {reviewer_current_report_path(record.workspace_root, record.node_id)}",
            *self._build_current_node_split_truth_prelude(record),
            "Lane results collected so far:",
        ]
        if not record.task_result_refs:
            lines.append("- none")
            return lines
        tester_refs = sorted(
            {
                str(result_map.get(ActorKind.EVALUATOR_TESTER.value) or "").strip()
                for result_map in record.task_result_refs.values()
                if str(result_map.get(ActorKind.EVALUATOR_TESTER.value) or "").strip()
            }
        )
        if len(tester_refs) == 1:
            lines.append(f"- global evaluator_tester review: {tester_refs[0]}")
        elif tester_refs:
            for idx, tester_ref in enumerate(tester_refs, start=1):
                lines.append(f"- evaluator_tester review #{idx}: {tester_ref}")
        for task_id in sorted(record.task_result_refs):
            result_map = record.task_result_refs[task_id]
            for actor_kind, result_ref in sorted(result_map.items()):
                if actor_kind == ActorKind.EVALUATOR_TESTER.value:
                    continue
                lines.append(f"- {task_id} / {actor_kind}: {result_ref}")
        return lines

    def _combine_prelude_lines(
        self,
        record: NodeRuntimeRecord,
        *extra_groups: list[str] | None,
    ) -> list[str] | None:
        lines: list[str] = []
        lines.extend([str(line) for line in record.pending_prelude_lines if str(line).strip()])
        for group in extra_groups:
            if group:
                lines.extend([str(line) for line in group if str(line).strip()])
        return lines or None

    def _launch_kernel_for_pending_request(
        self,
        *,
        pending_record: NodeRuntimeRecord,
        pending: PendingSplitReview,
    ) -> NodeRuntimeRecord:
        request_item = self._store.load_event(int(pending.request_seq))
        if request_item is None or request_item.event_type != "RequestSplit":
            self._clear_split_request_after_kernel_failure(pending.node_id, pending.request_seq)
            return pending_record
        source_record = self._store.load_node(pending.node_id)
        if source_record is None or int(source_record.split_request) != 1:
            return pending_record
        payload = self._load_event_payload_dict(request_item)
        split_bundle_ref = self._payload_required_str(
            payload,
            event_type=request_item.event_type,
            field_name="split_bundle_ref",
        )
        durable_commit = str(payload.get("durable_commit") or "").strip()
        diff_fingerprint = str(payload.get("diff_fingerprint") or "").strip()
        kernel_pending = replace(
            pending_record,
            workspace_root=str(source_record.workspace_root),
            final_effects_file=str(source_record.final_effects_file),
            current_components=[],
            pending_prelude_lines=[],
        )
        self._store.upsert_node(kernel_pending)
        spec = self._build_actor_launch_spec(
            node_id=kernel_pending.node_id,
            parent_node_id=kernel_pending.parent_node_id,
            actor_kind=ActorKind.KERNEL,
            attempt_count=self._next_attempt_count(kernel_pending, ActorKind.KERNEL),
            final_effects_file=Path(kernel_pending.final_effects_file),
            prelude_lines=self._combine_prelude_lines(
                kernel_pending,
                self._build_kernel_review_prelude(
                    request_seq=int(pending.request_seq),
                    source_record=source_record,
                    split_bundle_ref=split_bundle_ref,
                    durable_commit=durable_commit,
                    diff_fingerprint=diff_fingerprint,
                ),
            ),
            extra_env={
                "LOOP_REQUEST_SEQ": str(int(pending.request_seq)),
                "LOOP_SOURCE_NODE_ID": str(pending.node_id),
                "LOOP_SPLIT_BUNDLE_REF": str(split_bundle_ref),
            },
        )
        return self._try_launch_pending_actor(
            pending_record=kernel_pending,
            spec=spec,
            failure_context=f"kernel launch for request {int(pending.request_seq)}",
        )

    def _launch_implementer_for_record(
        self,
        record: NodeRuntimeRecord,
        *,
        extra_prelude_lines: list[str] | None = None,
    ) -> NodeRuntimeRecord:
        actor_kind = (
            ActorKind.KERNEL
            if str(record.node_id) == KERNEL_NODE_ID
            else ActorKind.IMPLEMENTER
        )
        spec = self._build_actor_launch_spec(
            node_id=record.node_id,
            parent_node_id=record.parent_node_id,
            actor_kind=actor_kind,
            attempt_count=self._next_attempt_count(record, actor_kind),
            final_effects_file=Path(record.final_effects_file),
            prelude_lines=self._combine_prelude_lines(
                record,
                self._build_actor_feedback_prelude(record, actor_kind=actor_kind),
                extra_prelude_lines,
            ),
        )
        return self._try_launch_pending_actor(
            pending_record=record,
            spec=spec,
            failure_context=f"{actor_kind.value} launch for node {record.node_id}",
        )

    def _launch_checker_for_node(
        self,
        record: NodeRuntimeRecord,
        *,
        prelude_lines: list[str] | None = None,
    ) -> NodeRuntimeRecord:
        pending_record = record
        for current_actor in list(pending_record.current_components):
            pending_record = self._mark_component_completed(
                pending_record,
                current_actor.actor_kind,
                task_id=current_actor.task_id,
            )
        pending_record = replace(
            pending_record,
            evaluator_phase="checker",
            checker_tasks_ref="",
            task_result_refs={},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            current_components=[],
        )
        self._store.upsert_node(pending_record)
        spec = self._build_actor_launch_spec(
            node_id=pending_record.node_id,
            parent_node_id=pending_record.parent_node_id,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            attempt_count=self._next_attempt_count(pending_record, ActorKind.EVALUATOR_CHECKER),
            final_effects_file=Path(pending_record.final_effects_file),
            prelude_lines=self._combine_prelude_lines(
                pending_record,
                self._build_actor_feedback_prelude(
                    pending_record,
                    actor_kind=ActorKind.EVALUATOR_CHECKER,
                ),
                self._build_checker_dispatch_prelude(pending_record),
                prelude_lines,
            ),
        )
        return self._try_launch_pending_actor(
            pending_record=pending_record,
            spec=spec,
            failure_context=f"checker launch for node {pending_record.node_id}",
        )

    def _has_running_actor_kind(
        self,
        record: NodeRuntimeRecord,
        actor_kind: ActorKind,
    ) -> bool:
        return any(actor.actor_kind is actor_kind for actor in record.current_components)

    def _launch_tester_for_node(self, record: NodeRuntimeRecord) -> NodeRuntimeRecord:
        pending_record = replace(record, evaluator_phase="tasks")
        self._store.upsert_node(pending_record)
        spec = self._build_actor_launch_spec(
            node_id=pending_record.node_id,
            parent_node_id=pending_record.parent_node_id,
            actor_kind=ActorKind.EVALUATOR_TESTER,
            attempt_count=self._next_attempt_count(pending_record, ActorKind.EVALUATOR_TESTER),
            final_effects_file=Path(pending_record.final_effects_file),
            prelude_lines=self._combine_prelude_lines(
                pending_record,
                self._build_actor_feedback_prelude(
                    pending_record,
                    actor_kind=ActorKind.EVALUATOR_TESTER,
                ),
                self._build_tester_review_prelude(pending_record),
            ),
        )
        return self._try_launch_pending_actor(
            pending_record=pending_record,
            spec=spec,
            failure_context=f"tester review launch for node {pending_record.node_id}",
        )

    def _launch_ai_user_task(
        self,
        record: NodeRuntimeRecord,
        *,
        task_entry: dict[str, object],
    ) -> NodeRuntimeRecord:
        task_id = str(task_entry.get("task_id") or "").strip()
        pending_record = replace(record, evaluator_phase="tasks")
        self._store.upsert_node(pending_record)
        spec = self._build_actor_launch_spec(
            node_id=pending_record.node_id,
            parent_node_id=pending_record.parent_node_id,
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            attempt_count=self._next_attempt_count(pending_record, ActorKind.EVALUATOR_AI_USER),
            final_effects_file=Path(pending_record.final_effects_file),
            prelude_lines=self._combine_prelude_lines(
                pending_record,
                self._build_actor_feedback_prelude(
                    pending_record,
                    actor_kind=ActorKind.EVALUATOR_AI_USER,
                    task_id=str(task_id),
                ),
                self._build_serial_task_prelude(
                    record=pending_record,
                    task_entry=dict(task_entry),
                ),
            ),
            extra_env={"LOOP_TASK_ID": str(task_id)},
        )
        return self._try_launch_pending_actor(
            pending_record=pending_record,
            spec=spec,
            failure_context=f"evaluator task launch for node {pending_record.node_id} task {task_id}",
        )

    def _launch_reviewer_for_node(self, record: NodeRuntimeRecord) -> NodeRuntimeRecord:
        pending_record = replace(record, evaluator_phase="reviewer", current_components=[])
        self._store.upsert_node(pending_record)
        spec = self._build_actor_launch_spec(
            node_id=pending_record.node_id,
            parent_node_id=pending_record.parent_node_id,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            attempt_count=self._next_attempt_count(pending_record, ActorKind.EVALUATOR_REVIEWER),
            final_effects_file=Path(pending_record.final_effects_file),
            prelude_lines=self._combine_prelude_lines(
                pending_record,
                self._build_reviewer_prelude(pending_record),
            ),
        )
        return self._try_launch_pending_actor(
            pending_record=pending_record,
            spec=spec,
            failure_context=f"reviewer launch for node {pending_record.node_id}",
        )

    def _relaunch_implementer_after_review(
        self,
        record: NodeRuntimeRecord,
        *,
        report_ref: str,
    ) -> NodeRuntimeRecord:
        pending_record = replace(
            record,
            current_components=[],
            pending_prelude_lines=[
                "Router resume after evaluator reviewer feedback.",
                f"Reviewer report reference: {report_ref}",
                "Read the reviewer feedback and either fix the implementation or argue concretely with evidence.",
            ],
        )
        self._store.upsert_node(pending_record)
        return self._launch_implementer_for_record(pending_record)

    def _prepare_kernel_takeover_workspace(
        self,
        *,
        source_record: NodeRuntimeRecord,
        takeover_id: str,
    ) -> tuple[Path, Path]:
        repo_root = _resolve_workspace_git_root(Path(source_record.workspace_root))
        base_commit = str(source_record.result_commit or "").strip()
        if not base_commit:
            raise InvalidSplitBundleError("kernel takeover source has no result_commit")
        takeover_root = (
            self._store.db_path.parent
            / "router"
            / "worktrees"
            / f"node-{KERNEL_NODE_ID}"
            / f"takeover-{takeover_id}"
        ).resolve()
        takeover_root.parent.mkdir(parents=True, exist_ok=True)
        relative_final_effects = _relative_path_within_workspace(
            workspace_root=Path(source_record.workspace_root),
            file_path=Path(source_record.final_effects_file),
        )
        try:
            _run_git(repo_root, "worktree", "add", "--detach", str(takeover_root), base_commit)
            takeover_final_effects = (takeover_root / relative_final_effects).resolve()
            _materialize_workspace_file_into_worktree(
                source_file=Path(source_record.final_effects_file),
                target_file=takeover_final_effects,
                missing_error=(
                    "kernel takeover source final_effects_file was not materialized into the git worktree: "
                    f"{takeover_final_effects}"
                ),
            )
        except Exception:
            self._cleanup_git_worktree(repo_root=repo_root, worktree_root=takeover_root)
            raise
        return takeover_root, takeover_final_effects

    def _write_kernel_takeover_report(
        self,
        *,
        source_record: NodeRuntimeRecord,
        takeover_id: str,
        workspace_root: Path,
    ) -> Path:
        takeover_root = (
            self._store.db_path.parent
            / "router"
            / "takeovers"
            / f"node-{KERNEL_NODE_ID}"
            / f"takeover-{takeover_id}"
        ).resolve()
        takeover_root.mkdir(parents=True, exist_ok=True)
        report_path = (takeover_root / "handoff.md").resolve()
        report_path.write_text(
            "\n".join(
                [
                    "Router final kernel takeover handoff.",
                    f"Source node: {source_record.node_id}",
                    f"Source result commit: {source_record.result_commit}",
                    f"Source workspace root: {source_record.workspace_root}",
                    f"Kernel takeover workspace root: {workspace_root}",
                    "Close out this entire LOOP subtree from the current merged workspace state.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return report_path

    def _activate_final_kernel_takeover(
        self,
        *,
        source_record: NodeRuntimeRecord,
    ) -> NodeRuntimeRecord:
        existing = self._store.load_node(KERNEL_NODE_ID)
        if existing is not None and (
            bool(existing.escalated_to_kernel)
            or bool(str(existing.result_commit or "").strip())
            or bool(existing.current_components)
            or str(existing.evaluator_phase or "").strip()
        ):
            return existing
        takeover_id = uuid.uuid4().hex
        workspace_root, final_effects_file = self._prepare_kernel_takeover_workspace(
            source_record=source_record,
            takeover_id=takeover_id,
        )
        resolution_ref = self._write_kernel_takeover_report(
            source_record=source_record,
            takeover_id=takeover_id,
            workspace_root=workspace_root,
        )
        durable_commit = str(source_record.result_commit or "").strip()
        if existing is None:
            pending_record = NodeRuntimeRecord(
                node_id=KERNEL_NODE_ID,
                parent_node_id=KERNEL_NODE_ID,
                child_node_ids=[],
                workspace_root=str(workspace_root),
                final_effects_file=str(final_effects_file),
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                current_components=[],
                durable_commit=durable_commit,
                result_commit="",
                escalated_to_kernel=True,
                last_rejected_split_diff_fingerprint="",
                components={},
            )
        else:
            pending_record = replace(
                existing,
                workspace_root=str(workspace_root),
                final_effects_file=str(final_effects_file),
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                current_components=[],
                durable_commit=durable_commit,
                result_commit="",
                escalated_to_kernel=True,
            )
        pending_record = replace(
            pending_record,
            pending_prelude_lines=[
                "Router final kernel takeover dispatch.",
                f"Source node id: {source_record.node_id}",
                f"Source result commit: {source_record.result_commit}",
                f"Takeover resolution reference: {resolution_ref}",
                "Take over final closeout for the entire LOOP subtree from this workspace state.",
            ],
        )
        self._store.upsert_node(pending_record)
        return self._launch_implementer_for_record(pending_record)

    def _handle_takeover_resolved(self, item: StoredRouterInboxItem) -> None:
        payload = self._load_event_payload_dict(item)
        target_node_id = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="target_node_id",
        )
        takeover_id = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="takeover_id",
        )
        resolution_ref = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="resolution_ref",
        )
        workspace_root = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="workspace_root",
        )
        final_effects_file = self._payload_required_str(
            payload,
            event_type=item.event_type,
            field_name="final_effects_file",
        )
        record = self._store.load_node(target_node_id)
        if record is None or int(record.split_approved) != 1:
            self._observe_placeholder(item)
            return
        parent_after_merge_ref = self._resolve_parent_after_merge_final_effects_ref(
            parent_record=record,
        )
        takeover_lines = [
            "Router takeover dispatch after child merge conflict.",
            f"Takeover id: {takeover_id}",
            f"Takeover resolution reference: {resolution_ref}",
        ]
        if parent_after_merge_ref:
            takeover_lines.extend(
                [
                    "First resolve the existing git merge conflicts in this workspace, then continue the approved parent-after-merge residual scope.",
                    f"Approved parent-after-merge final effects reference: {parent_after_merge_ref}",
                    "Do not expand scope beyond the original parent FINAL_EFFECTS.md and the approved residual scope.",
                ]
            )
        else:
            takeover_lines.extend(
                [
                    "Your only job is to resolve the existing git merge conflicts in this workspace.",
                    "Do not expand scope, redesign the task, or make unrelated changes.",
                ]
            )
        takeover_pending = replace(
            record,
            split_request=0,
            split_approved=0,
            approved_split_request_seq=0,
            workspace_root=str(Path(workspace_root).expanduser().resolve()),
            final_effects_file=str(Path(final_effects_file).expanduser().resolve()),
            checker_tasks_ref="",
            task_result_refs={},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            current_components=[],
            result_commit="",
            pending_prelude_lines=takeover_lines,
        )
        self._store.upsert_node(takeover_pending)
        self._launch_implementer_for_record(takeover_pending)
        self._observe_placeholder(item)

    def _resolve_workspace_result_commit(self, workspace_root: Path) -> tuple[str, list[str]]:
        resolved = Path(workspace_root).expanduser().resolve()
        try:
            head_commit = _resolve_workspace_durable_commit(resolved)
        except Exception as exc:  # noqa: BLE001
            return "", [f"Workspace git HEAD could not be resolved: {exc}"]
        if not head_commit:
            return "", ["Workspace git HEAD could not be resolved."]
        try:
            dirty_paths = _substantive_git_dirty_paths(resolved)
        except Exception as exc:  # noqa: BLE001
            return "", [f"Workspace git cleanliness could not be checked: {exc}"]
        if not dirty_paths:
            return head_commit, []
        try:
            result_commit = _create_workspace_result_commit(resolved, paths=dirty_paths)
        except Exception as exc:  # noqa: BLE001
            preview = ", ".join(dirty_paths[:5])
            suffix = " ..." if len(dirty_paths) > 5 else ""
            return "", [
                "Router could not create a result commit for substantive workspace changes "
                f"({preview}{suffix}): {exc}"
            ]
        if not result_commit:
            return "", ["Router created no result commit for substantive workspace changes."]
        try:
            remaining_dirty = _substantive_git_dirty_paths(resolved)
        except Exception as exc:  # noqa: BLE001
            return "", [f"Workspace git cleanliness could not be checked after router result commit: {exc}"]
        if remaining_dirty:
            preview = ", ".join(remaining_dirty[:5])
            suffix = " ..." if len(remaining_dirty) > 5 else ""
            return "", [
                "Workspace still has substantive uncommitted git changes after router result commit: "
                f"{preview}{suffix}"
            ]
        return result_commit, []

    def _child_result_records_for_parent(
        self,
        parent_record: NodeRuntimeRecord,
    ) -> list[NodeRuntimeRecord] | None:
        if not parent_record.child_node_ids:
            return None
        children: list[NodeRuntimeRecord] = []
        for child_id in parent_record.child_node_ids:
            child_record = self._store.load_node(child_id)
            if child_record is None:
                return None
            if (
                child_record.current_components
                or str(child_record.evaluator_phase or "").strip()
                or str(child_record.reviewer_verdict_kind or "").strip().upper() != "OK"
                or not str(child_record.result_commit or "").strip()
            ):
                return None
            children.append(child_record)
        return children

    def _cleanup_git_worktree(self, *, repo_root: Path, worktree_root: Path) -> None:
        try:
            _run_git(repo_root, "worktree", "remove", "--force", str(worktree_root), check=False)
        except Exception:  # noqa: BLE001
            pass
        shutil.rmtree(worktree_root, ignore_errors=True)

    def _archive_file_ref(
        self,
        *,
        source_ref: str,
        archive_root: Path,
        relative_path: Path,
    ) -> str:
        normalized = str(source_ref or "").strip()
        if not normalized:
            return ""
        source_path = Path(normalized).expanduser().resolve()
        target_path = (archive_root / relative_path).resolve()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.exists() and source_path.is_file():
            shutil.copy2(source_path, target_path)
        else:
            target_path.write_text(
                f"Archived router evidence source was missing: {source_path}\n",
                encoding="utf-8",
            )
        return str(target_path)

    def _archive_task_result_refs(
        self,
        *,
        record: NodeRuntimeRecord,
        archive_root: Path,
    ) -> dict[str, dict[str, str]]:
        archived: dict[str, dict[str, str]] = {}
        for task_id, result_map in sorted(record.task_result_refs.items()):
            archived_task: dict[str, str] = {}
            for actor_kind_raw, result_ref in sorted(dict(result_map).items()):
                actor_kind_label = str(actor_kind_raw or "").strip() or "result"
                result_path = Path(str(result_ref or "").strip())
                suffix = result_path.suffix or ".md"
                archived_task[actor_kind_label] = self._archive_file_ref(
                    source_ref=str(result_ref),
                    archive_root=archive_root,
                    relative_path=Path("task_results") / str(task_id) / f"{actor_kind_label}{suffix}",
                )
            archived[str(task_id)] = archived_task
        return archived

    def _delete_consumed_node_runtime_artifacts(self, record: NodeRuntimeRecord) -> None:
        workspace_root_raw = str(record.workspace_root or "").strip()
        if workspace_root_raw:
            workspace_root = Path(workspace_root_raw).expanduser().resolve()
            if workspace_root.exists():
                try:
                    repo_root = _resolve_workspace_git_root(workspace_root)
                except Exception:  # noqa: BLE001
                    shutil.rmtree(workspace_root, ignore_errors=True)
                else:
                    self._cleanup_git_worktree(repo_root=repo_root, worktree_root=workspace_root)
        runtime_root = (
            self._store.db_path.parent / "router" / "runtime" / f"node-{record.node_id}"
        ).resolve()
        shutil.rmtree(runtime_root, ignore_errors=True)

    def _archive_consumed_node(self, record: NodeRuntimeRecord) -> NodeRuntimeRecord:
        archive_root = (
            self._store.db_path.parent
            / "router"
            / "archive"
            / f"node-{record.node_id}"
            / f"result-{(str(record.result_commit or '').strip() or 'unknown')[:12]}"
        ).resolve()
        archive_root.mkdir(parents=True, exist_ok=True)
        reviewer_report_ref = self._archive_file_ref(
            source_ref=str(record.reviewer_report_ref or ""),
            archive_root=archive_root,
            relative_path=Path("reports") / "reviewer.md",
        )
        archived_task_result_refs = self._archive_task_result_refs(
            record=record,
            archive_root=archive_root,
        )
        archived_record = replace(
            record,
            workspace_root=str(archive_root),
            final_effects_file="",
            checker_tasks_ref="",
            task_result_refs=archived_task_result_refs,
            reviewer_report_ref=reviewer_report_ref,
            pending_prelude_lines=[],
            current_components=[],
        )
        self._store.upsert_node(archived_record)
        self._delete_consumed_node_runtime_artifacts(record)
        return archived_record

    def _archive_absorbed_direct_children(self, parent_record: NodeRuntimeRecord) -> None:
        if not parent_record.child_node_ids or not str(parent_record.result_commit or "").strip():
            return
        for child_id in parent_record.child_node_ids:
            child_record = self._store.load_node(child_id)
            if child_record is None:
                continue
            try:
                self._archive_consumed_node(child_record)
            except Exception:  # noqa: BLE001
                LOGGER.exception(
                    "failed to archive absorbed child node %s after parent %s converged",
                    child_id,
                    parent_record.node_id,
                )

    def _prepare_split_child_workspace(
        self,
        *,
        parent_record: NodeRuntimeRecord,
        child_id: str,
        bundle_dir: Path,
        rel_final_effects: str,
    ) -> tuple[Path, Path]:
        parent_repo_root = _resolve_workspace_git_root(Path(parent_record.workspace_root))
        base_commit = str(parent_record.durable_commit or "").strip()
        if not base_commit:
            raise InvalidSplitBundleError("approved split parent has no durable git baseline commit")
        child_workspace_root = (
            self._store.db_path.parent / "router" / "worktrees" / f"node-{child_id}"
        ).resolve()
        if child_workspace_root.exists():
            raise InvalidSplitBundleError(
                f"child workspace root already exists for node {child_id}: {child_workspace_root}"
            )
        child_workspace_root.parent.mkdir(parents=True, exist_ok=True)
        relative_final_effects = Path(rel_final_effects)
        try:
            _run_git(parent_repo_root, "worktree", "add", "--detach", str(child_workspace_root), base_commit)
            source_subtree = (bundle_dir / relative_final_effects.parent).resolve()
            target_subtree = (child_workspace_root / relative_final_effects.parent).resolve()
            if relative_final_effects.parent == Path("."):
                target_subtree.mkdir(parents=True, exist_ok=True)
                shutil.copy2((bundle_dir / relative_final_effects).resolve(), (child_workspace_root / relative_final_effects).resolve())
            else:
                shutil.copytree(source_subtree, target_subtree, dirs_exist_ok=True)
            child_final_effects = (child_workspace_root / relative_final_effects).resolve()
            if not child_final_effects.exists() or not child_final_effects.is_file():
                raise InvalidSplitBundleError(
                    f"child final_effects_file was not materialized into the git worktree: {child_final_effects}"
                )
            return child_workspace_root, child_final_effects
        except Exception:
            self._cleanup_git_worktree(repo_root=parent_repo_root, worktree_root=child_workspace_root)
            raise

    def _prepare_parent_integration_workspace(
        self,
        *,
        parent_record: NodeRuntimeRecord,
        takeover_id: str,
    ) -> tuple[Path, Path]:
        parent_repo_root = _resolve_workspace_git_root(Path(parent_record.workspace_root))
        base_commit = str(parent_record.durable_commit or "").strip()
        if not base_commit:
            raise InvalidSplitBundleError("split-approved parent has no durable git baseline commit")
        integration_root = (
            self._store.db_path.parent
            / "router"
            / "worktrees"
            / f"node-{parent_record.node_id}"
            / f"takeover-{takeover_id}"
        ).resolve()
        integration_root.parent.mkdir(parents=True, exist_ok=True)
        relative_final_effects = _relative_path_within_workspace(
            workspace_root=Path(parent_record.workspace_root),
            file_path=Path(parent_record.final_effects_file),
        )
        try:
            _run_git(parent_repo_root, "worktree", "add", "--detach", str(integration_root), base_commit)
            integration_final_effects = (integration_root / relative_final_effects).resolve()
            _materialize_workspace_file_into_worktree(
                source_file=Path(parent_record.final_effects_file),
                target_file=integration_final_effects,
                missing_error=(
                    "parent integration final_effects_file was not materialized into the git worktree: "
                    f"{integration_final_effects}"
                ),
            )
        except Exception:
            self._cleanup_git_worktree(repo_root=parent_repo_root, worktree_root=integration_root)
            raise
        return integration_root, integration_final_effects

    def _resolve_parent_after_merge_final_effects_ref(
        self,
        *,
        parent_record: NodeRuntimeRecord,
    ) -> str:
        request_seq = int(parent_record.approved_split_request_seq or 0)
        if request_seq <= 0:
            return ""
        try:
            bundle_dir, proposal = self._resolve_request_split_bundle(request_seq)
        except InvalidSplitBundleError:
            return ""
        parent_after_merge = proposal.parent_after_merge
        if parent_after_merge is None:
            return ""
        return str((bundle_dir / parent_after_merge.final_effects_file).resolve())

    def _write_takeover_report(
        self,
        *,
        parent_record: NodeRuntimeRecord,
        takeover_id: str,
        baseline_commit: str,
        child_records: list[NodeRuntimeRecord],
        conflict_files: list[str],
    ) -> Path:
        takeover_root = (
            self._store.db_path.parent
            / "router"
            / "takeovers"
            / f"node-{parent_record.node_id}"
            / f"takeover-{takeover_id}"
        ).resolve()
        takeover_root.mkdir(parents=True, exist_ok=True)
        report_path = (takeover_root / "conflicts.md").resolve()
        lines = [
            "Router takeover conflict report.",
            f"Parent node: {parent_record.node_id}",
            f"Baseline commit: {baseline_commit}",
            "Child result commits:",
        ]
        for child_record in child_records:
            lines.append(f"- {child_record.node_id}: {child_record.result_commit}")
        lines.append("Conflicted paths:")
        if conflict_files:
            lines.extend([f"- {path}" for path in conflict_files])
        else:
            lines.append("- <unknown>")
        report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return report_path

    def _attempt_parent_split_merge(self, parent_record: NodeRuntimeRecord) -> None:
        child_records = self._child_result_records_for_parent(parent_record)
        if child_records is None:
            return
        parent_after_merge_ref = self._resolve_parent_after_merge_final_effects_ref(
            parent_record=parent_record,
        )
        takeover_id = uuid.uuid4().hex
        integration_root, integration_final_effects = self._prepare_parent_integration_workspace(
            parent_record=parent_record,
            takeover_id=takeover_id,
        )
        baseline_commit = str(parent_record.durable_commit or "").strip()
        try:
            for child_record in child_records:
                merge_proc = _run_git(
                    integration_root,
                    "merge",
                    "--no-ff",
                    "--no-edit",
                    str(child_record.result_commit),
                    check=False,
                )
                if merge_proc.returncode == 0:
                    continue
                conflict_files = [
                    str(line or "").strip()
                    for line in _run_git(
                        integration_root,
                        "diff",
                        "--name-only",
                        "--diff-filter=U",
                        check=False,
                    ).stdout.splitlines()
                    if str(line or "").strip()
                ]
                if conflict_files:
                    resolution_ref = self._write_takeover_report(
                        parent_record=parent_record,
                        takeover_id=takeover_id,
                        baseline_commit=baseline_commit,
                        child_records=child_records,
                        conflict_files=conflict_files,
                    )
                    self._store.append_event(
                        TakeoverResolved(
                            actor=self._actor_ref(
                                node_id=KERNEL_NODE_ID,
                                actor_kind=ActorKind.KERNEL,
                                attempt_count=1,
                            ),
                            target_node_id=parent_record.node_id,
                            takeover_id=takeover_id,
                            resolution_ref=str(resolution_ref),
                            workspace_root=str(integration_root),
                            final_effects_file=str(integration_final_effects),
                            resolved_at=datetime.now(timezone.utc),
                        )
                    )
                    notify_router_wakeup(self._wakeup_socket_path)
                    return
                raise RuntimeError(
                    f"git merge failed without explicit conflict files for parent node {parent_record.node_id}: "
                    f"{str(merge_proc.stderr or '').strip() or str(merge_proc.stdout or '').strip() or 'unknown error'}"
                )
            result_commit = _resolve_workspace_durable_commit(integration_root)
            if parent_after_merge_ref:
                pending_lines = [
                    "Router resume after clean child merge.",
                    f"Merged child result commits into workspace: {result_commit}",
                    f"Approved parent-after-merge final effects reference: {parent_after_merge_ref}",
                    "Continue implementing the approved parent-after-merge residual scope from this merged workspace before completing.",
                    "Do not expand scope beyond the original parent FINAL_EFFECTS.md and the approved residual scope.",
                ]
                residual_record = replace(
                    parent_record,
                    split_request=0,
                    split_approved=0,
                    approved_split_request_seq=0,
                    workspace_root=str(integration_root),
                    final_effects_file=str(integration_final_effects),
                    current_components=[],
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=pending_lines,
                    durable_commit=result_commit,
                    result_commit="",
                )
                self._store.upsert_node(residual_record)
                self._launch_implementer_for_record(residual_record)
                return
            final_record = replace(
                parent_record,
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                workspace_root=str(integration_root),
                final_effects_file=str(integration_final_effects),
                current_components=[],
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="OK",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                result_commit=result_commit,
            )
            self._archive_absorbed_direct_children(final_record)
            self._store.upsert_node(final_record)
            if str(parent_record.node_id) == ROOT_NODE_ID:
                self._activate_final_kernel_takeover(source_record=final_record)
        except Exception:
            self._cleanup_git_worktree(
                repo_root=_resolve_workspace_git_root(Path(parent_record.workspace_root)),
                worktree_root=integration_root,
            )
            raise

    def _resolve_request_diff_fingerprint(self, request_seq: int) -> str:
        request_item = self._store.load_event(request_seq)
        if request_item is None or request_item.event_type != "RequestSplit":
            return ""
        payload = json.loads(request_item.payload_json)
        return str(payload.get("diff_fingerprint") or "")

    def _resolve_request_split_bundle(self, request_seq: int) -> tuple[Path, SplitProposal]:
        request_item = self._store.load_event(request_seq)
        if request_item is None or request_item.event_type != "RequestSplit":
            raise InvalidSplitBundleError(f"missing RequestSplit for request_seq={request_seq}")
        payload = self._load_event_payload_dict(request_item)
        return parse_split_bundle(
            self._payload_required_str(
                payload,
                event_type=request_item.event_type,
                field_name="split_bundle_ref",
            ),
            error_type=InvalidSplitBundleError,
        )

    def _clear_split_request_after_kernel_failure(self, node_id: str, request_seq: int) -> None:
        record = self._store.load_node(node_id)
        if record is None or int(record.split_request) != 1:
            return
        self._store.upsert_node(replace(record, split_request=0, approved_split_request_seq=0))

    def _record_is_unfinished_frontier(self, record: NodeRuntimeRecord) -> bool:
        if str(record.node_id) == KERNEL_NODE_ID and not bool(record.escalated_to_kernel):
            return False
        if str(record.result_commit or "").strip():
            return False
        if int(record.split_request) != 0 or int(record.split_approved) != 0:
            return False
        if record.current_components:
            return True
        evaluator_phase = str(record.evaluator_phase or "").strip()
        reviewer_verdict_kind = str(record.reviewer_verdict_kind or "").strip().upper()
        primary_actor_kind = (
            ActorKind.KERNEL
            if str(record.node_id) == KERNEL_NODE_ID
            else ActorKind.IMPLEMENTER
        )
        implementer_state = self._component_state_for_kind(record, primary_actor_kind)
        if evaluator_phase in {"checker", "tasks", "reviewer"}:
            return True
        if reviewer_verdict_kind == "IMPLEMENTER_ACTION_REQUIRED":
            return True
        if implementer_state is None:
            return True
        return (
            implementer_state.status is ComponentStatus.INACTIVE
            and reviewer_verdict_kind == ""
            and evaluator_phase == ""
        )

    def _maybe_request_router_terminal_shutdown(self) -> None:
        router_status = str(self._store.read_router_status() or "").strip()
        if router_status == "paused":
            self._terminate_all_running_actors_best_effort()
            self._request_router_terminal_shutdown()
            return
        if router_status in {"terminal_failed", "completed"}:
            self._request_router_terminal_shutdown()
            return
        frontier_total = 0
        frontier_terminal_failed = 0
        for record in self._store.list_nodes():
            if not self._record_is_unfinished_frontier(record):
                continue
            frontier_total += 1
            if any(
                component.status is ComponentStatus.TERMINAL_FAILED
                for component in record.components.values()
            ):
                frontier_terminal_failed += 1
        if frontier_total <= 0 or (frontier_terminal_failed * 2) <= frontier_total:
            return
        summary = {
            "reason": "majority_frontier_terminal_failed",
            "frontier_total": int(frontier_total),
            "frontier_terminal_failed": int(frontier_terminal_failed),
        }
        self._store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json=json.dumps(summary, sort_keys=True, separators=(",", ":")),
            router_terminal_at=datetime.now(timezone.utc).isoformat(),
        )
        self._request_router_terminal_shutdown()

    def _request_router_terminal_shutdown(self) -> None:
        with self._lock:
            self._stop_requested = True
        proc_supervisor = self._proc_supervisor
        if proc_supervisor is not None:
            proc_supervisor.stop()
        notify_router_wakeup(self._wakeup_socket_path)

    def _mark_router_completed(
        self,
        *,
        result_commit: str,
        report_ref: str,
    ) -> None:
        if str(self._store.read_router_status() or "").strip() == "completed":
            self._request_router_terminal_shutdown()
            return
        self._store.write_router_completed_state(
            router_status="completed",
            router_completed_result_commit=str(result_commit),
            router_completed_report_ref=str(report_ref),
            router_completed_at=datetime.now(timezone.utc).isoformat(),
        )
        self._request_router_terminal_shutdown()

    def _terminate_all_running_actors_best_effort(self) -> None:
        proc_supervisor = self._proc_supervisor
        terminate_actor = (
            None if proc_supervisor is None else getattr(proc_supervisor, "terminate_actor", None)
        )
        for record in self._store.list_nodes():
            for actor_ref in list(record.current_components):
                terminated = False
                if callable(terminate_actor):
                    terminated = bool(terminate_actor(actor_ref))
                if terminated:
                    continue
                component = self._component_state_for_actor(record, actor_ref)
                pid = 0 if component is None else int(component.pid or 0)
                if pid <= 0:
                    continue
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass

    def _build_reject_recovery_prelude(self, reason_ref: str | None) -> list[str]:
        lines = [
            "Router resume after split rejection.",
            "Continue this node instead of splitting right now.",
            "Do not request split again until there is new effective git diff progress.",
        ]
        if reason_ref:
            lines.append(f"Reject reason reference: {reason_ref}")
        return lines

    def _materialize_split_children(
        self,
        *,
        parent_record: NodeRuntimeRecord,
        request_seq: int,
    ) -> tuple[NodeRuntimeRecord, list[NodeRuntimeRecord]]:
        bundle_dir, proposal = self._resolve_request_split_bundle(request_seq)
        children = list(proposal.children)
        child_ids = (
            list(parent_record.child_node_ids)
            if parent_record.child_node_ids
            else self._store.allocate_next_node_ids(len(children))
        )
        if len(child_ids) != len(children):
            raise InvalidSplitBundleError(
                "approved split child count no longer matches the approved split bundle"
            )
        updated_parent = replace(parent_record, child_node_ids=list(child_ids))
        child_records: list[NodeRuntimeRecord] = []
        new_child_records: list[NodeRuntimeRecord] = []
        for child_id, child in zip(child_ids, children):
            child_record = self._store.load_node(child_id)
            if child_record is None:
                child_workspace_root, final_effects_file = self._prepare_split_child_workspace(
                    parent_record=parent_record,
                    child_id=str(child_id),
                    bundle_dir=bundle_dir,
                    rel_final_effects=str(child.final_effects_file),
                )
                child_record = NodeRuntimeRecord(
                    node_id=str(child_id),
                    parent_node_id=parent_record.node_id,
                    child_node_ids=[],
                    workspace_root=str(child_workspace_root),
                    final_effects_file=str(final_effects_file),
                    split_request=0,
                    split_approved=0,
                    approved_split_request_seq=0,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[],
                    durable_commit=(
                        str(parent_record.durable_commit or "").strip()
                        or _resolve_workspace_durable_commit(child_workspace_root)
                    ),
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={},
                )
                new_child_records.append(child_record)
            child_records.append(child_record)
        self._store.upsert_nodes([updated_parent, *child_records])
        return updated_parent, new_child_records

    def _try_launch_pending_actor(
        self,
        *,
        pending_record: NodeRuntimeRecord,
        spec: ActorLaunchSpec,
        failure_context: str,
    ) -> NodeRuntimeRecord:
        try:
            updated_record, _launch = self._launch_and_commit_actor(
                spec=spec,
                base_record=pending_record,
            )
            return updated_record
        except RouterBusinessError as exc:
            LOGGER.warning(
                "%s failed reason_code=%s: %s; leaving durable pending state",
                failure_context,
                exc.reason_code,
                exc.message,
            )
            return pending_record
        except Exception:
            LOGGER.exception("%s failed; leaving durable pending state", failure_context)
            return pending_record

    def _relaunch_pending_actor_for_kind(
        self,
        *,
        pending_record: NodeRuntimeRecord,
        actor_kind: ActorKind,
    ) -> NodeRuntimeRecord:
        if actor_kind is ActorKind.KERNEL:
            if bool(pending_record.escalated_to_kernel):
                return self._launch_implementer_for_record(pending_record)
            pending = next(iter(self._store.list_pending_split_reviews()), None)
            if pending is None:
                return pending_record
            return self._launch_kernel_for_pending_request(
                pending_record=pending_record,
                pending=pending,
            )
        if actor_kind is ActorKind.IMPLEMENTER:
            return self._launch_implementer_for_record(pending_record)
        if actor_kind is ActorKind.EVALUATOR_CHECKER:
            return self._launch_checker_for_node(pending_record)
        if actor_kind in {ActorKind.EVALUATOR_TESTER, ActorKind.EVALUATOR_AI_USER}:
            return self._continue_tasks_phase(pending_record)
        if actor_kind is ActorKind.EVALUATOR_REVIEWER:
            return self._launch_reviewer_for_node(pending_record)
        return pending_record

    def _startup_running_actor_sweep(self) -> None:
        proc_supervisor = self._proc_supervisor
        has_actor = None if proc_supervisor is None else getattr(proc_supervisor, "has_actor", None)
        if not callable(has_actor):
            return
        for record in self._store.list_nodes():
            for actor_ref in list(record.current_components):
                actor_kind = actor_ref.actor_kind
                component = self._component_state_for_actor(record, actor_ref)
                if component is None or component.status is not ComponentStatus.RUNNING:
                    continue
                if bool(has_actor(actor_ref)):
                    continue
                terminate_actor = getattr(proc_supervisor, "terminate_actor", None)
                terminated = False
                if callable(terminate_actor):
                    terminated = bool(terminate_actor(actor_ref))
                if not terminated:
                    pid = int(component.pid or 0)
                    if pid > 0:
                        try:
                            os.kill(pid, signal.SIGTERM)
                        except OSError:
                            pass
                if int(record.split_request) != 0 or int(record.split_approved) != 0:
                    self._store.upsert_node(
                        self._mark_component_inactive(
                            record,
                            actor_kind,
                            task_id=actor_ref.task_id,
                        )
                    )
                    continue
                self._recover_actor_after_exit(
                    record=record,
                    actor_kind=actor_kind,
                    component=component,
                    reason_lines=[
                        "Router startup found the previous attempt still marked running but no longer under proc supervision.",
                    ],
                )

    def _reconcile_pending_launches_once(self) -> None:
        if str(self._store.read_router_status() or "").strip() in {"paused", "terminal_failed", "completed"}:
            if str(self._store.read_router_status() or "").strip() == "paused":
                self._terminate_all_running_actors_best_effort()
                self._request_router_terminal_shutdown()
            return
        pending_reviews = self._store.list_pending_split_reviews()
        if pending_reviews and self._store.load_node(KERNEL_NODE_ID) is None:
            source_record = self._store.load_node(pending_reviews[0].node_id)
            if source_record is not None:
                self._store.upsert_node(
                    self._build_kernel_pending_record(source_record=source_record)
                )
        for record in self._store.list_nodes():
            self._reconcile_pending_for_record(record)

    def _reconcile_pending_for_record(self, record: NodeRuntimeRecord) -> None:
        if str(record.node_id) == KERNEL_NODE_ID and not bool(record.escalated_to_kernel):
            if record.current_components:
                return
            pending = next(iter(self._store.list_pending_split_reviews()), None)
            if pending is None:
                return
            self._launch_kernel_for_pending_request(
                pending_record=record,
                pending=pending,
            )
            return
        if str(record.node_id) == ROOT_NODE_ID and str(record.result_commit or "").strip():
            self._activate_final_kernel_takeover(source_record=record)
            return
        if str(record.result_commit or "").strip():
            return
        if int(record.split_approved) == 1:
            materialization_incomplete = not record.child_node_ids or any(
                self._store.load_node(child_id) is None for child_id in record.child_node_ids
            )
            request_seq = int(record.approved_split_request_seq)
            if materialization_incomplete and request_seq > 0:
                try:
                    _updated_parent, new_child_records = self._materialize_split_children(
                        parent_record=record,
                        request_seq=request_seq,
                    )
                except Exception:
                    LOGGER.exception(
                        "split child materialization reconcile failed for node %s",
                        record.node_id,
                    )
                else:
                    for child_record in new_child_records:
                        self._launch_implementer_for_record(child_record)
                return
            try:
                self._attempt_parent_split_merge(record)
            except Exception:
                LOGGER.exception(
                    "split parent merge reconcile failed for node %s",
                    record.node_id,
                )
            return
        evaluator_phase = str(record.evaluator_phase or "").strip()
        if evaluator_phase == "tasks":
            self._continue_tasks_phase(record)
            return
        if record.current_components:
            return
        if int(record.split_request) != 0 or int(record.split_approved) != 0:
            return
        reviewer_verdict_kind = str(record.reviewer_verdict_kind or "").strip().upper()
        primary_actor_kind = (
            ActorKind.KERNEL
            if str(record.node_id) == KERNEL_NODE_ID
            else ActorKind.IMPLEMENTER
        )
        implementer_state = self._component_state_for_kind(record, primary_actor_kind)
        if evaluator_phase == "checker":
            self._launch_checker_for_node(record)
            return
        if evaluator_phase == "reviewer":
            self._launch_reviewer_for_node(record)
            return
        if reviewer_verdict_kind == "IMPLEMENTER_ACTION_REQUIRED":
            self._relaunch_implementer_after_review(
                record,
                report_ref=str(record.reviewer_report_ref or ""),
            )
            return
        if implementer_state is None:
            self._launch_implementer_for_record(record)
            return
        if (
            implementer_state.status is ComponentStatus.INACTIVE
            and reviewer_verdict_kind == ""
            and evaluator_phase == ""
        ):
            self._launch_implementer_for_record(record)

    def _continue_tasks_phase(self, record: NodeRuntimeRecord) -> NodeRuntimeRecord:
        try:
            checker_tasks_ref = str(record.checker_tasks_ref or "").strip()
            if not checker_tasks_ref:
                raise InvalidCheckerTasksError(
                    "checker task manifest is missing while evaluator_phase=tasks"
                )
            tasks = self._resolve_checker_tasks(Path(checker_tasks_ref))
            task_result_refs = {
                str(task_id): {str(kind): str(ref) for kind, ref in dict(result_map).items()}
                for task_id, result_map in record.task_result_refs.items()
            }
            tester_pending = any(
                not str(
                    task_result_refs.get(str(task["task_id"]), {}).get(
                        ActorKind.EVALUATOR_TESTER.value,
                        "",
                    )
                ).strip()
                for task in tasks
            )
            pending_record = record
            if tester_pending and not self._has_running_actor_kind(
                pending_record,
                ActorKind.EVALUATOR_TESTER,
            ):
                pending_record = self._launch_tester_for_node(pending_record)
            running_ai_user_count = sum(
                1
                for actor in pending_record.current_components
                if actor.actor_kind is ActorKind.EVALUATOR_AI_USER
            )
            available_ai_user_slots = max(
                0,
                EVALUATOR_AI_USER_MAX_PARALLEL - int(running_ai_user_count),
            )
            if available_ai_user_slots > 0:
                for task in self._pending_ai_user_tasks(
                    pending_record,
                    limit=available_ai_user_slots,
                ):
                    pending_record = self._launch_ai_user_task(
                        pending_record,
                        task_entry=dict(task),
                    )
            if self._has_running_actor_kind(pending_record, ActorKind.EVALUATOR_TESTER):
                return pending_record
            if any(
                actor.actor_kind is ActorKind.EVALUATOR_AI_USER
                for actor in pending_record.current_components
            ):
                return pending_record
            if tester_pending:
                return pending_record
            if self._pending_ai_user_tasks(pending_record, limit=1):
                return pending_record
            reviewer_pending = replace(pending_record, evaluator_phase="reviewer")
            return self._launch_reviewer_for_node(reviewer_pending)
        except RouterBusinessError as exc:
            checker_pending = replace(
                record,
                pending_prelude_lines=[
                    "Router evaluator task-manifest recovery.",
                    f"The checker task manifest is invalid: {exc.message}",
                    "Re-run checker and write a fresh valid task manifest before evaluator tasks continue.",
                ],
            )
            return self._launch_checker_for_node(checker_pending)
