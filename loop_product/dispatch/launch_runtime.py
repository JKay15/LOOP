"""Committed child launch helper surfaces."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import os
import re
import signal
import shlex
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from loop_product.artifact_hygiene import canonicalize_workspace_artifact_heavy_trees
from loop_product.codex_home import prepare_runtime_owned_codex_home
from loop_product.dispatch.launch_policy import merge_runtime_transport_env
from loop_product.dispatch.publication import (
    inspect_live_artifact_runtime_state,
    inspect_workspace_publication_state,
    inspect_workspace_local_runtime_state,
    repair_workspace_publication_surface,
    reset_live_artifact_runtime_heavy_roots,
    reset_workspace_publish_root,
    reset_workspace_local_runtime_heavy_roots,
)
from loop_product.evaluator_authority import (
    authoritative_result_retryable_nonterminal,
    evaluator_backed_terminal_closure,
)
from loop_product.event_journal import commit_launch_reused_existing_event
from loop_product.event_journal import commit_launch_started_event
from loop_product.ai_launch import (
    ai_launch_exit_code,
    detach_ai_launch_handle,
    finish_ai_launch,
    start_ai_launch,
    terminate_ai_launch,
)
from loop_product.host_child_launch import request_host_child_launch, should_request_host_child_launch
from loop_product.host_child_runtime_status import (
    request_host_child_runtime_status,
    should_request_host_child_runtime_status,
)
from loop_product.protocols.node import normalize_runtime_state
from loop_product.protocols.schema import validate_repo_object
from loop_product.run_cmd import _ENV_UNSET_SENTINEL
from loop_product.runtime_identity import runtime_root_identity_token
from loop_product.state_io import write_json_read_only
from loop_product.runtime_paths import node_machine_handoff_ref, require_runtime_root

DEFAULT_STARTUP_RETRY_LIMIT = 1
DEFAULT_STARTUP_HEALTH_TIMEOUT_MS = 12000
_STARTUP_HEALTH_POLL_MS = 250
_STARTUP_PROGRESS_RE = re.compile(r"(?m)^(thinking|codex|exec|assistant)$")
RUNTIME_LOSS_CONFIRMATION_S = 20.0
_TERMINAL_NODE_STATUSES = {"BLOCKED", "COMPLETED", "FAILED"}
def _nonempty(value: Any) -> str:
    return str(value or "").strip()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_json_read_only(path, payload)


def _utc_timestamp() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _prepare_child_codex_home(*, workspace_root: Path, launch_dir: Path) -> Path:
    child_home = (workspace_root / ".codex_child_runtime" / launch_dir.name).resolve()
    return prepare_runtime_owned_codex_home(runtime_home=child_home)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_json(path: Path) -> dict[str, Any]:
    try:
        return _load_json(path)
    except Exception:
        return {}


def _wrapped_argv_process_fingerprint(wrapped_argv: list[str]) -> str:
    normalized = [str(item) for item in list(wrapped_argv or []) if str(item)]
    if not normalized:
        return ""
    digest = hashlib.sha256()
    digest.update(json.dumps(normalized, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def _launch_started_observation_id(
    *,
    launch_result_ref: Path,
    launch_payload: dict[str, Any],
    wrapped_argv_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "source_result_ref": str(launch_payload.get("source_result_ref") or ""),
        "launch_request_ref": str(launch_payload.get("launch_request_ref") or ""),
        "node_id": str(launch_payload.get("node_id") or ""),
        "pid": int(launch_payload.get("pid") or 0),
        "wrapped_argv_fingerprint": wrapped_argv_fingerprint,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"launch_started::{digest.hexdigest()[:16]}"


def _mirror_canonical_launch_started_event_from_launch_payload(
    launch_payload: dict[str, Any],
) -> dict[str, Any] | None:
    if str(launch_payload.get("launch_decision") or "").strip() != "started":
        return None
    try:
        state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
    except Exception:
        return None
    node_id = str(launch_payload.get("node_id") or "").strip()
    launch_result_ref = Path(str(launch_payload.get("launch_result_ref") or "")).expanduser().resolve()
    if not node_id or not str(launch_result_ref):
        return None
    wrapped_argv = [str(item) for item in list(launch_payload.get("wrapped_argv") or [])]
    wrapped_argv_fingerprint = _wrapped_argv_process_fingerprint(wrapped_argv)
    observation_id = _launch_started_observation_id(
        launch_result_ref=launch_result_ref,
        launch_payload=launch_payload,
        wrapped_argv_fingerprint=wrapped_argv_fingerprint,
    )
    return commit_launch_started_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        payload={
            "launch_event_id": str(launch_result_ref),
            "launch_result_ref": str(launch_result_ref),
            "source_result_ref": str(launch_payload.get("source_result_ref") or ""),
            "launch_request_ref": str(launch_payload.get("launch_request_ref") or ""),
            "launch_decision": "started",
            "workspace_root": str(launch_payload.get("workspace_root") or ""),
            "state_root": str(state_root),
            "launch_log_dir": str(launch_payload.get("launch_log_dir") or ""),
            "stdout_ref": str(launch_payload.get("stdout_ref") or ""),
            "stderr_ref": str(launch_payload.get("stderr_ref") or ""),
            "stdin_ref": str(launch_payload.get("stdin_ref") or ""),
            "wrapper_cmd": str(launch_payload.get("wrapper_cmd") or ""),
            "wrapped_argv": wrapped_argv,
            "wrapped_argv_fingerprint": wrapped_argv_fingerprint,
            "process_fingerprint": wrapped_argv_fingerprint,
            "pid": int(launch_payload.get("pid") or 0),
            "exit_code": launch_payload.get("exit_code"),
        },
        producer="dispatch.launch_runtime",
    )


def _launch_reused_existing_observation_id(
    *,
    launch_result_ref: Path,
    launch_payload: dict[str, Any],
    wrapped_argv_fingerprint: str,
    reused_launch_result_ref: Path,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "reuse_launch_result_ref": str(reused_launch_result_ref),
        "source_result_ref": str(launch_payload.get("source_result_ref") or ""),
        "launch_request_ref": str(launch_payload.get("launch_request_ref") or ""),
        "node_id": str(launch_payload.get("node_id") or ""),
        "pid": int(launch_payload.get("pid") or 0),
        "wrapped_argv_fingerprint": wrapped_argv_fingerprint,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"launch_reused_existing::{digest.hexdigest()[:16]}"


def _mirror_canonical_launch_reused_existing_event_from_launch_payload(
    launch_payload: dict[str, Any],
) -> dict[str, Any] | None:
    if str(launch_payload.get("launch_decision") or "").strip() != "started_existing":
        return None
    try:
        state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
    except Exception:
        return None
    node_id = str(launch_payload.get("node_id") or "").strip()
    launch_result_ref = Path(str(launch_payload.get("launch_result_ref") or "")).expanduser().resolve()
    reused_launch_result_ref = Path(str(launch_payload.get("reuse_launch_result_ref") or "")).expanduser().resolve()
    if not node_id or not str(launch_result_ref) or not str(reused_launch_result_ref):
        return None
    wrapped_argv = [str(item) for item in list(launch_payload.get("wrapped_argv") or [])]
    wrapped_argv_fingerprint = _wrapped_argv_process_fingerprint(wrapped_argv)
    observation_id = _launch_reused_existing_observation_id(
        launch_result_ref=launch_result_ref,
        launch_payload=launch_payload,
        wrapped_argv_fingerprint=wrapped_argv_fingerprint,
        reused_launch_result_ref=reused_launch_result_ref,
    )
    return commit_launch_reused_existing_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        payload={
            "launch_event_id": str(launch_result_ref),
            "launch_result_ref": str(launch_result_ref),
            "reused_launch_event_id": str(reused_launch_result_ref),
            "reuse_launch_result_ref": str(reused_launch_result_ref),
            "source_result_ref": str(launch_payload.get("source_result_ref") or ""),
            "launch_request_ref": str(launch_payload.get("launch_request_ref") or ""),
            "launch_decision": "started_existing",
            "workspace_root": str(launch_payload.get("workspace_root") or ""),
            "state_root": str(state_root),
            "launch_log_dir": str(launch_payload.get("launch_log_dir") or ""),
            "stdout_ref": str(launch_payload.get("stdout_ref") or ""),
            "stderr_ref": str(launch_payload.get("stderr_ref") or ""),
            "stdin_ref": str(launch_payload.get("stdin_ref") or ""),
            "wrapper_cmd": str(launch_payload.get("wrapper_cmd") or ""),
            "wrapped_argv": wrapped_argv,
            "wrapped_argv_fingerprint": wrapped_argv_fingerprint,
            "process_fingerprint": wrapped_argv_fingerprint,
            "pid": int(launch_payload.get("pid") or 0),
            "exit_code": launch_payload.get("exit_code"),
        },
        producer="dispatch.launch_runtime",
    )


def _authoritative_result_ref(*, state_root: Path, node_id: str, node_payload: dict[str, Any]) -> Path:
    sink = str(node_payload.get("result_sink_ref") or "").strip()
    if not sink:
        sink = f"artifacts/{node_id}/implementer_result.json"
    return (state_root / sink).resolve()


def _existing_machine_handoff_ref(*, state_root: Path, node_id: str) -> Path | None:
    candidate = node_machine_handoff_ref(state_root=state_root, node_id=node_id)
    if candidate.exists():
        return candidate
    return None


def _normalize_launch_request(
    *,
    source_result_ref: str,
    payload: dict[str, Any],
    startup_probe_ms: int,
    startup_health_timeout_ms: int,
) -> dict[str, Any]:
    launch_spec = dict(payload.get("launch_spec") or {})
    normalized = {
        "source_result_ref": _nonempty(source_result_ref),
        "node_id": _nonempty(payload.get("node_id")),
        "workspace_root": _nonempty(payload.get("workspace_root")),
        "state_root": _nonempty(payload.get("state_root")),
        "startup_probe_ms": max(0, int(startup_probe_ms)),
        "startup_health_timeout_ms": max(0, int(payload.get("startup_health_timeout_ms", startup_health_timeout_ms))),
        "startup_retry_limit": max(0, int(payload.get("startup_retry_limit", DEFAULT_STARTUP_RETRY_LIMIT))),
        "launch_spec": {
            "argv": [str(item) for item in (launch_spec.get("argv") or [])],
            "env": {str(k): str(v) for k, v in dict(launch_spec.get("env") or {}).items()},
            "cwd": _nonempty(launch_spec.get("cwd")),
            "stdin_path": _nonempty(launch_spec.get("stdin_path")),
        },
    }
    validate_repo_object("LoopChildLaunchRequest.schema.json", normalized)
    return normalized


def _normalize_runtime_status_result(
    *,
    launch_result_ref: str,
    payload: dict[str, Any],
    stall_threshold_s: float,
) -> dict[str, Any]:
    normalized = {
        "launch_result_ref": str(launch_result_ref),
        "status_result_ref": str(payload.get("status_result_ref") or ""),
        "node_id": _nonempty(payload.get("node_id")),
        "workspace_root": _nonempty(payload.get("workspace_root")),
        "state_root": _nonempty(payload.get("state_root")),
        "node_ref": str(payload.get("node_ref") or ""),
        "launch_decision": _nonempty(payload.get("launch_decision")),
        "pid": int(payload.get("pid") or 0),
        "pid_alive": bool(payload.get("pid_alive")),
        "exit_code": payload.get("exit_code"),
        "lifecycle_status": _nonempty(payload.get("lifecycle_status")),
        "runtime_attachment_state": _nonempty(payload.get("runtime_attachment_state")),
        "runtime_observed_at": str(payload.get("runtime_observed_at") or ""),
        "runtime_observation_kind": str(payload.get("runtime_observation_kind") or ""),
        "runtime_summary": str(payload.get("runtime_summary") or ""),
        "runtime_evidence_refs": [str(item) for item in list(payload.get("runtime_evidence_refs") or [])],
        "stdout_ref": str(payload.get("stdout_ref") or ""),
        "stderr_ref": str(payload.get("stderr_ref") or ""),
        "latest_log_ref": str(payload.get("latest_log_ref") or ""),
        "latest_log_mtime": str(payload.get("latest_log_mtime") or ""),
        "latest_log_age_s": float(payload.get("latest_log_age_s") or 0.0),
        "stall_threshold_s": float(stall_threshold_s),
        "stalled_hint": bool(payload.get("stalled_hint")),
        "recovery_eligible": bool(payload.get("recovery_eligible")),
        "recovery_reason": str(payload.get("recovery_reason") or ""),
    }
    validate_repo_object("LoopChildRuntimeStatusResult.schema.json", normalized)
    return normalized


def _ensure_committed_supervision_attached_if_needed(*, result_ref: str | Path) -> None:
    result_path = Path(result_ref).expanduser().resolve()
    launch_payload = _load_optional_json(result_path)
    if not launch_payload:
        return
    launch_decision = str(launch_payload.get("launch_decision") or "").strip()
    if launch_decision not in {"started", "started_existing"}:
        return
    if result_path.with_name("ChildSupervisionResult.json").exists():
        return
    state_root_raw = str(launch_payload.get("state_root") or "").strip()
    node_id = str(launch_payload.get("node_id") or "").strip()
    if not state_root_raw or not node_id:
        return
    try:
        state_root = require_runtime_root(Path(state_root_raw).expanduser().resolve())
    except Exception:
        return
    node_ref = state_root / "state" / f"{node_id}.json"
    node_payload = _load_optional_json(node_ref)
    node_status = str(node_payload.get("status") or "").strip()
    if node_status not in {"", "ACTIVE"}:
        return
    attempt_dir_name = result_path.parent.name
    attempt_suffix = attempt_dir_name[len("attempt_") :] if attempt_dir_name.startswith("attempt_") else ""
    if not attempt_suffix.isdigit():
        return
    launch_root = state_root / "artifacts" / "launches" / node_id
    latest_launch_ref: Path | None = None
    if launch_root.exists():
        candidates = [
            candidate
            for candidate in launch_root.glob("attempt_*/ChildLaunchResult.json")
            if candidate.parent.name.startswith("attempt_")
            and candidate.parent.name[len("attempt_") :].isdigit()
        ]
        if candidates:
            latest_launch_ref = max(
                candidates,
                key=lambda candidate: int(candidate.parent.name[len("attempt_") :]),
            )
    if latest_launch_ref is not None and latest_launch_ref.resolve() != result_path:
        return
    _, terminal_result_payload = _load_terminal_implementer_result(
        state_root=state_root,
        node_id=node_id,
        node_payload=node_payload,
    )
    if terminal_result_payload:
        return
    from loop_product.child_supervision_sidecar import (
        ensure_child_supervision_running,
        live_child_supervision_runtime,
    )

    if live_child_supervision_runtime(launch_result_ref=result_path) is not None:
        return
    ensure_child_supervision_running(launch_result_ref=result_path)


def _should_ensure_committed_supervision_after_status(
    payload: Mapping[str, Any],
) -> bool:
    attachment_state = str(payload.get("runtime_attachment_state") or "").strip().upper()
    recovery_reason = str(payload.get("recovery_reason") or "").strip()
    if attachment_state == "ATTACHED" and bool(payload.get("pid_alive")):
        return True
    return recovery_reason in {
        "active_without_live_pid",
        "active_without_live_pid_unconfirmed",
        "runtime_status_observation_error_without_live_pid",
        "runtime_status_observation_error_without_live_pid_unconfirmed",
    }


def _load_terminal_implementer_result(*, state_root: Path, node_id: str, node_payload: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    result_ref = _authoritative_result_ref(state_root=state_root, node_id=node_id, node_payload=node_payload)
    if not result_ref.exists():
        return result_ref, {}
    try:
        payload = _load_json(result_ref)
    except Exception:
        return result_ref, {}
    if not _has_evaluator_backed_terminal_closure(payload):
        return result_ref, {}
    return result_ref, payload


def _supervision_stop_point_summary(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized == "no_substantive_progress":
        return "child supervision settled to no_substantive_progress after terminating a live runtime that stopped making substantive artifact progress"
    if normalized == "retry_budget_exhausted":
        return "child supervision settled to retry_budget_exhausted after bounded recovery attempts were exhausted"
    return f"child supervision settled to {normalized or 'unknown'}"


def _runtime_state_confirms_dead_active_loss(runtime_state: Mapping[str, Any]) -> bool:
    attachment_state = str(runtime_state.get("attachment_state") or "").strip().upper()
    if attachment_state != "LOST":
        return False
    # Recovery acceptance projections can linger after a child is reattached; they must
    # not bypass the normal runtime-loss confirmation window for a freshly dead ACTIVE child.
    observation_kind = str(runtime_state.get("observation_kind") or "").strip().lower()
    if observation_kind.startswith("recovery_"):
        return False
    return True


def _live_workspace_artifact_hygiene_sync(
    *,
    workspace_root: Path | None,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    if workspace_root is None:
        return {
            "removed_refs": [],
            "sync_error": "",
            "publication_state": {"applicable": False, "violation": False},
            "local_runtime_state": {"applicable": False, "violation": False},
            "live_runtime_state": {"applicable": False, "violation": False},
        }
    try:
        candidate = workspace_root.expanduser().resolve()
    except Exception:
        return {
            "removed_refs": [],
            "sync_error": "",
            "publication_state": {"applicable": False, "violation": False},
            "local_runtime_state": {"applicable": False, "violation": False},
            "live_runtime_state": {"applicable": False, "violation": False},
        }
    if not candidate.exists() or not candidate.is_dir():
        return {
            "removed_refs": [],
            "sync_error": "",
            "publication_state": {"applicable": False, "violation": False},
            "local_runtime_state": {"applicable": False, "violation": False},
            "live_runtime_state": {"applicable": False, "violation": False},
        }
    publication_state = inspect_workspace_publication_state(
        workspace_root=candidate,
        machine_handoff_ref=machine_handoff_ref,
    )
    local_runtime_state = inspect_workspace_local_runtime_state(
        workspace_root=candidate,
        machine_handoff_ref=machine_handoff_ref,
    )
    live_runtime_state = inspect_live_artifact_runtime_state(
        workspace_root=candidate,
        machine_handoff_ref=machine_handoff_ref,
    )
    excluded_roots: list[Path] = []
    publish_ref = str(publication_state.get("publish_artifact_ref") or "").strip()
    if publish_ref:
        excluded_roots.append(Path(publish_ref).expanduser().resolve())
    live_ref = str(publication_state.get("live_artifact_ref") or "").strip()
    if live_ref:
        excluded_roots.append(Path(live_ref).expanduser().resolve())
    sync_error = ""
    try:
        removed_refs = canonicalize_workspace_artifact_heavy_trees(candidate, exclude_roots=excluded_roots)
    except Exception as exc:
        removed_refs = []
        sync_error = str(exc)
    return {
        "removed_refs": removed_refs,
        "sync_error": sync_error,
        "publication_state": publication_state,
        "local_runtime_state": local_runtime_state,
        "live_runtime_state": live_runtime_state,
    }


def _terminal_evaluation_report_ref(payload: Mapping[str, Any]) -> Path | None:
    runtime_refs = dict(payload.get("runtime_refs") or {})
    evaluator = dict(payload.get("evaluator") or {})
    for candidate in (
        payload.get("evaluation_report_ref"),
        runtime_refs.get("evaluation_report_ref"),
        evaluator.get("evaluation_report_ref"),
    ):
        ref = str(candidate or "").strip()
        if ref:
            return Path(ref).expanduser().resolve()
    return None


def _has_evaluator_backed_terminal_closure(payload: Mapping[str, Any]) -> bool:
    return evaluator_backed_terminal_closure(payload)


def _resolved_terminal_lifecycle_status(
    *,
    node_payload: Mapping[str, Any],
    result_payload: Mapping[str, Any],
) -> str:
    node_status = str(node_payload.get("status") or "").strip().upper()
    if node_status in _TERMINAL_NODE_STATUSES:
        return node_status
    evaluator_result = dict(result_payload.get("evaluator_result") or {})
    verdict = str(evaluator_result.get("verdict") or "").strip().upper()
    if verdict == "PASS":
        return "COMPLETED"
    if verdict == "STUCK":
        return "BLOCKED"
    if verdict:
        return "FAILED"
    result_status = str(result_payload.get("status") or "").strip().upper()
    if result_status in _TERMINAL_NODE_STATUSES:
        return result_status
    return "COMPLETED"


def _durable_terminal_node_state(
    *,
    node_payload: Mapping[str, Any],
    result_ref: Path,
    result_payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    if authoritative_result_retryable_nonterminal(result_payload):
        return None
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    attachment_state = str(runtime_state.get("attachment_state") or "").strip().upper()
    node_status = str(node_payload.get("status") or "").strip().upper()
    if attachment_state != "TERMINAL":
        return None
    if node_status not in _TERMINAL_NODE_STATUSES:
        return None
    evidence_refs: list[str] = []
    if result_ref.exists():
        evidence_refs.append(str(result_ref.resolve()))
    for item in list(runtime_state.get("evidence_refs") or []):
        item_text = str(item or "").strip()
        if item_text and item_text not in evidence_refs:
            evidence_refs.append(item_text)
    return {
        "lifecycle_status": _resolved_terminal_lifecycle_status(
            node_payload=node_payload,
            result_payload=result_payload,
        ),
        "runtime_attachment_state": "TERMINAL",
        "runtime_observed_at": str(
            runtime_state.get("observed_at")
            or result_payload.get("completed_at_utc")
            or _utc_timestamp()
        ),
        "runtime_observation_kind": "terminal_node_state",
        "runtime_summary": str(
            runtime_state.get("summary")
            or result_payload.get("summary")
            or "durable terminal node state present"
        ),
        "runtime_evidence_refs": evidence_refs,
        "recovery_reason": "authoritative_terminal_node_state",
    }


def _next_launch_attempt_dir(state_root: Path, node_id: str) -> Path:
    base = state_root / "artifacts" / "launches" / node_id
    suffix = 1
    while True:
        candidate = base / f"attempt_{suffix:03d}"
        if not candidate.exists():
            return candidate
        suffix += 1


def _node_launch_gate_ref(*, state_root: Path, node_id: str) -> Path:
    return (state_root / "artifacts" / "launches" / str(node_id) / "ChildLaunchGate.lock").resolve()


def _load_node_launch_gate_payload(lock_ref: Path) -> dict[str, Any]:
    try:
        return _load_json(lock_ref)
    except Exception:
        return {}


def _launch_gate_timeout_s(request: Mapping[str, Any]) -> float:
    attempt_count = max(1, int(request.get("startup_retry_limit") or 0) + 1)
    startup_probe_s = max(0.0, float(request.get("startup_probe_ms") or 0) / 1000.0)
    startup_health_s = max(0.0, float(request.get("startup_health_timeout_ms") or 0) / 1000.0)
    retry_backoff_s = max(0.0, float(max(0, attempt_count - 1)))
    return max(5.0, attempt_count * max(0.25, startup_probe_s + startup_health_s) + retry_backoff_s + 2.0)


def _acquire_node_launch_gate(
    *,
    request: Mapping[str, Any],
    state_root: Path,
    workspace_root: Path,
) -> tuple[Path | None, dict[str, Any] | None]:
    lock_ref = _node_launch_gate_ref(state_root=state_root, node_id=str(request["node_id"]))
    deadline = time.monotonic() + _launch_gate_timeout_s(request)
    while time.monotonic() < deadline:
        existing = _existing_live_launch_result(
            state_root=state_root,
            node_id=str(request["node_id"]),
            workspace_root=workspace_root,
        )
        if existing is not None:
            return None, existing
        lock_ref.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(lock_ref, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            payload = _load_node_launch_gate_payload(lock_ref)
            owner_pid = int(payload.get("pid") or 0)
            if owner_pid > 0 and not _pid_alive(owner_pid):
                lock_ref.unlink(missing_ok=True)
                continue
            time.sleep(0.05)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "schema": "loop_product.child_launch_gate",
                    "node_id": str(request["node_id"]),
                    "state_root": str(state_root),
                    "workspace_root": str(workspace_root),
                    "source_result_ref": str(request.get("source_result_ref") or ""),
                    "pid": os.getpid(),
                    "started_at_utc": _utc_timestamp(),
                },
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
        existing = _existing_live_launch_result(
            state_root=state_root,
            node_id=str(request["node_id"]),
            workspace_root=workspace_root,
        )
        if existing is not None:
            lock_ref.unlink(missing_ok=True)
            return None, existing
        return lock_ref, None
    raise TimeoutError(f"timed out waiting to acquire child launch gate at {lock_ref}")


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
        return _pid_visible_via_ps(candidate)
    except OSError as exc:
        if getattr(exc, "errno", None) == 1:
            return _pid_visible_via_ps(candidate)
        return False
    if _pid_is_zombie(candidate):
        return False
    return True


def _pid_visible_via_ps(pid: int) -> bool:
    for _ in range(3):
        try:
            proc = subprocess.run(
                ["ps", "-p", str(int(pid)), "-o", "pid="],
                text=True,
                capture_output=True,
                check=False,
            )
        except Exception:
            return False
        if proc.returncode == 0 and str(pid) in {line.strip() for line in str(proc.stdout or "").splitlines() if line.strip()}:
            return not _pid_is_zombie(pid)
        time.sleep(0.1)
    return False


def _pid_is_zombie(pid: int) -> bool:
    try:
        proc = subprocess.run(
            ["ps", "-p", str(int(pid)), "-o", "stat="],
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception:
        return False
    if proc.returncode != 0:
        return False
    stat = str(proc.stdout or "").strip().upper()
    return stat.startswith("Z") or " Z" in stat


def _launch_identity_markers(launch_result: Mapping[str, Any]) -> list[str]:
    markers: list[str] = []
    stdout_ref = Path(str(launch_result.get("stdout_ref") or "")).expanduser()
    if str(stdout_ref):
        stdout_name = stdout_ref.name
        if stdout_name.endswith(".stdout.txt"):
            markers.append(
                str(stdout_ref.with_name(stdout_name[: -len(".stdout.txt")] + ".tmux-launch.sh").resolve())
            )
    for token in (
        str(launch_result.get("workspace_root") or "").strip(),
        str(launch_result.get("stdin_ref") or "").strip(),
    ):
        if len(token) >= 8:
            markers.append(token)
    for raw_item in list(launch_result.get("wrapped_argv") or [])[1:]:
        token = str(raw_item).strip()
        if len(token) < 8:
            continue
        if token.startswith("-") and " " not in token and os.sep not in token and "=" not in token and "." not in token:
            continue
        markers.append(token)
    deduped: list[str] = []
    seen: set[str] = set()
    for marker in markers:
        if not marker or marker in seen:
            continue
        seen.add(marker)
        deduped.append(marker)
    return deduped


def _launch_result_process_identity_matches(launch_result: Mapping[str, Any]) -> bool:
    pid = int(launch_result.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    markers = _launch_identity_markers(launch_result)
    if not markers:
        return False
    return any(marker in command_line for marker in markers)


def _terminate_pid(pid: int, *, grace_s: float = 0.75) -> None:
    if pid <= 0:
        return
    if not _pid_alive(pid):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError:
        return
    deadline = time.monotonic() + max(0.1, grace_s)
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError:
        return


def _child_pids(pid: int) -> list[int]:
    if pid <= 0:
        return []
    try:
        proc = subprocess.run(
            ["ps", "-o", "pid=", "--ppid", str(int(pid))],
            text=True,
            capture_output=True,
            check=False,
        )
    except Exception:
        return []
    if proc.returncode != 0:
        return []
    children: list[int] = []
    for line in str(proc.stdout or "").splitlines():
        value = str(line).strip()
        if not value:
            continue
        try:
            child_pid = int(value)
        except ValueError:
            continue
        if child_pid > 0:
            children.append(child_pid)
    return children


def _pid_descendants(pid: int) -> list[int]:
    if pid <= 0:
        return []
    descendants: list[int] = []
    seen: set[int] = set()
    pending = [int(pid)]
    while pending:
        current = pending.pop()
        for child_pid in _child_pids(current):
            if child_pid in seen or child_pid == pid:
                continue
            seen.add(child_pid)
            descendants.append(child_pid)
            pending.append(child_pid)
    return descendants


def _terminate_pid_tree(pid: int, *, grace_s: float = 0.75) -> None:
    for child_pid in _pid_descendants(pid):
        _terminate_pid(child_pid, grace_s=grace_s)
    _terminate_pid(pid, grace_s=grace_s)


def _reap_runtime_owned_launch(launch_result: dict[str, Any]) -> None:
    pid = int(launch_result.get("pid") or 0)
    if pid > 0 and _launch_result_process_identity_matches(launch_result):
        _terminate_pid_tree(pid)


def terminate_runtime_owned_launch_result_ref(*, result_ref: str | Path) -> None:
    """Terminate the runtime-owned child process referenced by a launch result."""

    payload = _load_optional_json(Path(result_ref).expanduser().resolve())
    if payload:
        _reap_runtime_owned_launch(payload)


def _existing_live_launch_result(*, state_root: Path, node_id: str, workspace_root: Path) -> dict[str, Any] | None:
    launch_root = state_root / "artifacts" / "launches" / node_id
    if not launch_root.exists():
        return None
    for result_path in sorted(launch_root.glob("attempt_*/ChildLaunchResult.json"), reverse=True):
        try:
            payload = _load_json(result_path)
        except Exception:
            continue
        if str(payload.get("node_id") or "") != str(node_id):
            continue
        if Path(str(payload.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
            continue
        if Path(str(payload.get("state_root") or "")).expanduser().resolve() != state_root:
            continue
        if str(payload.get("launch_decision") or "") not in {"started", "started_existing"}:
            continue
        if payload.get("exit_code") not in (None, ""):
            continue
        if not _launch_result_process_identity_matches(payload):
            continue
        return payload
    return None


def _latest_numeric_attempt_launch_result(*, state_root: Path, node_id: str, workspace_root: Path) -> dict[str, Any] | None:
    launch_root = state_root / "artifacts" / "launches" / node_id
    if not launch_root.exists():
        return None
    candidates: list[tuple[int, Path]] = []
    for result_path in launch_root.glob("attempt_*/ChildLaunchResult.json"):
        attempt_dir = result_path.parent.name
        if not attempt_dir.startswith("attempt_"):
            continue
        suffix = attempt_dir[len("attempt_") :]
        if not suffix.isdigit():
            continue
        candidates.append((int(suffix), result_path))
    for _, result_path in sorted(candidates, reverse=True):
        try:
            payload = _load_json(result_path)
        except Exception:
            continue
        if str(payload.get("node_id") or "") != str(node_id):
            continue
        if Path(str(payload.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
            continue
        if Path(str(payload.get("state_root") or "")).expanduser().resolve() != state_root:
            continue
        return payload
    return None


def _persist_reused_launch_result(
    *,
    request: dict[str, Any],
    state_root: Path,
    workspace_root: Path,
    existing: dict[str, Any],
) -> dict[str, Any]:
    launch_dir = _next_launch_attempt_dir(state_root, request["node_id"])
    request_ref = launch_dir / "ChildLaunchRequest.json"
    result_ref = launch_dir / "ChildLaunchResult.json"
    _write_json(request_ref, request)
    existing_launch_result_ref = Path(str(existing.get("reuse_launch_result_ref") or existing.get("launch_result_ref") or "")).expanduser()
    root_identity_token = str(existing.get("runtime_root_identity_token") or runtime_root_identity_token(state_root, ensure=True))
    payload = {
        "launch_decision": "started_existing",
        "source_result_ref": request["source_result_ref"],
        "node_id": request["node_id"],
        "workspace_root": str(workspace_root),
        "state_root": str(state_root),
        "runtime_root_identity_token": root_identity_token,
        "startup_health_timeout_ms": int(request["startup_health_timeout_ms"]),
        "startup_retry_limit": int(request["startup_retry_limit"]),
        "attempt_count": 0,
        "retryable_failure_kind": "",
        "attempts": [],
        "launch_request_ref": str(request_ref.resolve()),
        "launch_result_ref": str(result_ref.resolve()),
        "launch_log_dir": str(existing.get("launch_log_dir") or ""),
        "stdout_ref": str(existing.get("stdout_ref") or ""),
        "stderr_ref": str(existing.get("stderr_ref") or ""),
        "stdin_ref": str(existing.get("stdin_ref") or ""),
        "wrapped_argv": [str(item) for item in list(existing.get("wrapped_argv") or [])],
        "wrapper_cmd": str(existing.get("wrapper_cmd") or ""),
        "pid": int(existing.get("pid") or 0),
        "exit_code": None,
        "reuse_launch_result_ref": str(existing_launch_result_ref.resolve()) if str(existing_launch_result_ref) else "",
    }
    validate_repo_object("LoopChildLaunchResult.schema.json", payload)
    _write_json(result_ref, payload)
    try:
        _mirror_canonical_launch_reused_existing_event_from_launch_payload(payload)
    except Exception:
        pass
    return payload


def _wrapper_prefix() -> tuple[list[str], str]:
    raw = str(os.environ.get("LOOP_CHILD_LAUNCH_WRAPPER") or "").strip()
    if not raw:
        return [], ""
    prefix = shlex.split(raw)
    if not prefix:
        return [], ""
    return prefix, raw


def _child_launch_env_overrides(*, child_codex_home: Path) -> dict[str, str]:
    sanitized_parent_codex_env = {
        key: _ENV_UNSET_SENTINEL
        for key in os.environ
        if key.startswith("CODEX_") and key != "CODEX_HOME"
    }
    return {
        **sanitized_parent_codex_env,
        "CODEX_HOME": str(child_codex_home.resolve()),
        "OTEL_SDK_DISABLED": "true",
        "OTEL_TRACES_EXPORTER": "none",
        "OTEL_METRICS_EXPORTER": "none",
        "OTEL_LOGS_EXPORTER": "none",
    }


def _classify_retryable_failure(*, stdout_path: Path, stderr_path: Path) -> str:
    text = _combined_launch_text(stdout_path=stdout_path, stderr_path=stderr_path).lower()
    if "currently experiencing high demand" in text or "high demand" in text:
        return "provider_capacity"
    if (
        "failed to connect to websocket" in text
        or "stream disconnected before completion" in text
        or "error sending request for url" in text
        or "connection reset by peer" in text
        or "operation not permitted (os error 1)" in text
    ):
        return "provider_transport"
    if (
        "attempted to create a null object" in text
        or "reqwest-internal-sync-runtime" in text
        or "event loop thread panicked" in text
        or "could not create otel exporter" in text
    ):
        return "provider_runtime"
    if "usage limit" in text:
        return "provider_quota"
    if "unauthorized" in text or "401" in text:
        return "provider_auth"
    return ""


def _combined_launch_text(*, stdout_path: Path, stderr_path: Path) -> str:
    return (
        stderr_path.read_text(encoding="utf-8", errors="replace")
        + "\n"
        + stdout_path.read_text(encoding="utf-8", errors="replace")
    )


def _startup_progress_marker(*, stdout_path: Path, stderr_path: Path) -> bool:
    text = _combined_launch_text(stdout_path=stdout_path, stderr_path=stderr_path)
    if _STARTUP_PROGRESS_RE.search(text):
        return True
    return "i’m " in text.lower() or "i'm " in text.lower()


def _retryable_failure(kind: str) -> bool:
    return kind in {"provider_capacity", "provider_transport", "provider_runtime"}


def _latest_log_snapshot(*, result_path: Path, stdout_path: Path, stderr_path: Path) -> tuple[Path, float]:
    candidates = [path for path in (stderr_path, stdout_path, result_path) if path.exists()]
    latest = max(candidates, key=lambda item: item.stat().st_mtime)
    return latest, float(latest.stat().st_mtime)


def _format_mtime(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds).astimezone().isoformat(timespec="seconds")


def _observation_error_runtime_status_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float,
    observation_error: Exception,
    host_error: Exception | None = None,
) -> dict[str, Any]:
    result_path = Path(result_ref).expanduser().resolve()
    launch_result = _load_json(result_path)
    validate_repo_object("LoopChildLaunchResult.schema.json", launch_result)

    state_root = require_runtime_root(Path(str(launch_result["state_root"])).expanduser().resolve())
    node_id = str(launch_result["node_id"])
    workspace_root = Path(str(launch_result.get("workspace_root") or "")).expanduser()
    node_ref = state_root / "state" / f"{node_id}.json"
    node_payload = _load_json(node_ref) if node_ref.exists() else {}
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))

    pid = int(launch_result.get("pid") or 0)
    pid_alive = _launch_result_process_identity_matches(launch_result)
    stdout_path = Path(str(launch_result.get("stdout_ref") or "")).expanduser()
    stderr_path = Path(str(launch_result.get("stderr_ref") or "")).expanduser()
    latest_log_ref, latest_log_epoch = _latest_log_snapshot(
        result_path=result_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    latest_log_age_s = max(0.0, time.time() - latest_log_epoch)
    lifecycle_status = str(node_payload.get("status") or "")
    lifecycle_active_like = lifecycle_status in {"", "ACTIVE"}
    runtime_loss_confirmed = bool(
        not pid_alive
        and lifecycle_active_like
        and (
            launch_result.get("exit_code") not in (None, "")
            or latest_log_age_s >= RUNTIME_LOSS_CONFIRMATION_S
            or _runtime_state_confirms_dead_active_loss(runtime_state)
        )
    )

    if pid_alive:
        runtime_attachment_state = "ATTACHED"
        recovery_eligible = False
        recovery_reason = "runtime_status_observation_error_live_pid"
    elif lifecycle_active_like:
        runtime_attachment_state = "LOST"
        recovery_eligible = runtime_loss_confirmed
        recovery_reason = (
            "runtime_status_observation_error_without_live_pid"
            if runtime_loss_confirmed
            else "runtime_status_observation_error_without_live_pid_unconfirmed"
        )
    else:
        runtime_attachment_state = str(runtime_state.get("attachment_state") or "UNOBSERVED")
        recovery_eligible = False
        recovery_reason = f"runtime_status_observation_error_node_status_{lifecycle_status.lower() or 'unknown'}"

    summary_parts: list[str] = []
    if host_error is not None:
        summary_parts.append(f"host runtime-status observation failed: {host_error}")
    summary_parts.append(f"runtime status observation failed: {observation_error}")
    status_result_ref = result_path.parent / "ChildRuntimeStatusResult.json"
    payload = _normalize_runtime_status_result(
        launch_result_ref=str(result_path),
        payload={
            "status_result_ref": str(status_result_ref.resolve()),
            "node_id": node_id,
            "workspace_root": str(launch_result.get("workspace_root") or ""),
            "state_root": str(state_root),
            "node_ref": str(node_ref.resolve()) if node_ref.exists() else "",
            "launch_decision": str(launch_result.get("launch_decision") or ""),
            "pid": pid,
            "pid_alive": pid_alive,
            "exit_code": launch_result.get("exit_code"),
            "lifecycle_status": lifecycle_status,
            "runtime_attachment_state": runtime_attachment_state,
            "runtime_observed_at": _utc_timestamp(),
            "runtime_observation_kind": "runtime_status_observation_error",
            "runtime_summary": "; ".join(summary_parts),
            "runtime_evidence_refs": [
                str(result_path.resolve()),
                *([str(node_ref.resolve())] if node_ref.exists() else []),
            ],
            "stdout_ref": str(stdout_path.resolve()),
            "stderr_ref": str(stderr_path.resolve()),
            "latest_log_ref": str(latest_log_ref.resolve()),
            "latest_log_mtime": _format_mtime(latest_log_epoch),
            "latest_log_age_s": latest_log_age_s,
            "stalled_hint": bool(pid_alive and latest_log_age_s >= max(0.0, float(stall_threshold_s))),
            "recovery_eligible": recovery_eligible,
            "recovery_reason": recovery_reason,
        },
        stall_threshold_s=stall_threshold_s,
    )
    _write_json(status_result_ref, payload)
    return payload


def _observe_startup_health(handle: Any, *, startup_health_timeout_ms: int) -> tuple[str, int | None]:
    failure_kind = _classify_retryable_failure(stdout_path=handle.stdout_path, stderr_path=handle.stderr_path)
    observed_exit_code = ai_launch_exit_code(handle)
    if observed_exit_code is not None:
        finished = finish_ai_launch(handle)
        return failure_kind, int(finished.span.get("exit_code"))
    if _startup_progress_marker(stdout_path=handle.stdout_path, stderr_path=handle.stderr_path) or not failure_kind:
        return "", None

    deadline = time.monotonic() + max(0.0, float(startup_health_timeout_ms) / 1000.0)
    while time.monotonic() < deadline:
        time.sleep(min(_STARTUP_HEALTH_POLL_MS / 1000.0, max(0.0, deadline - time.monotonic())))
        failure_kind = _classify_retryable_failure(stdout_path=handle.stdout_path, stderr_path=handle.stderr_path)
        observed_exit_code = ai_launch_exit_code(handle)
        if observed_exit_code is not None:
            finished = finish_ai_launch(handle)
            return failure_kind, int(finished.span.get("exit_code"))
        if _startup_progress_marker(stdout_path=handle.stdout_path, stderr_path=handle.stderr_path) or not failure_kind:
            return "", None

    terminate_ai_launch(handle, grace_s=0.25)
    finished = finish_ai_launch(handle)
    failure_kind = _classify_retryable_failure(stdout_path=handle.stdout_path, stderr_path=handle.stderr_path) or "startup_unhealthy"
    return failure_kind, int(finished.span.get("exit_code"))


def _launch_once(
    *,
    request: dict[str, Any],
    state_root: Path,
    workspace_root: Path,
    wrapper_prefix: list[str],
    wrapper_cmd: str,
) -> dict[str, Any]:
    launch_spec = dict(request["launch_spec"])
    launch_dir = _next_launch_attempt_dir(state_root, request["node_id"])
    log_dir = launch_dir / "logs"
    request_ref = launch_dir / "ChildLaunchRequest.json"
    _write_json(request_ref, request)

    argv = wrapper_prefix + list(launch_spec["argv"])
    child_codex_home = _prepare_child_codex_home(workspace_root=workspace_root, launch_dir=launch_dir)
    launch_env = {
        **merge_runtime_transport_env(state_root=state_root, env=dict(launch_spec.get("env") or {})),
        **_child_launch_env_overrides(child_codex_home=child_codex_home),
    }
    handle = start_ai_launch(
        cmd=argv,
        cwd=Path(launch_spec["cwd"]).expanduser().resolve(),
        log_dir=log_dir,
        label=request["node_id"],
        env=launch_env,
        stdin_path=Path(launch_spec["stdin_path"]).expanduser().resolve(),
        start_new_session=False,
    )
    probe_s = max(0.0, float(request["startup_probe_ms"]) / 1000.0)
    if probe_s > 0.0:
        time.sleep(probe_s)

    failure_kind, observed_exit_code = _observe_startup_health(
        handle,
        startup_health_timeout_ms=int(request["startup_health_timeout_ms"]),
    )
    if observed_exit_code is None:
        launch_decision = "started"
        exit_code: int | None = None
        retryable_failure_kind = ""
        detach_ai_launch_handle(handle)
    else:
        launch_decision = "exited"
        exit_code = int(observed_exit_code)
        retryable_failure_kind = failure_kind

    return {
        "launch_decision": launch_decision,
        "source_result_ref": request["source_result_ref"],
        "node_id": request["node_id"],
        "workspace_root": str(workspace_root),
        "state_root": str(state_root),
        "startup_health_timeout_ms": int(request["startup_health_timeout_ms"]),
        "launch_request_ref": str(request_ref.resolve()),
        "launch_log_dir": str(log_dir.resolve()),
        "stdout_ref": str(handle.stdout_path.resolve()),
        "stderr_ref": str(handle.stderr_path.resolve()),
        "stdin_ref": str(Path(launch_spec["stdin_path"]).expanduser().resolve()),
        "wrapped_argv": list(argv),
        "wrapper_cmd": wrapper_cmd,
        "pid": int(handle.pid) if handle.pid is not None else None,
        "exit_code": exit_code,
        "retryable_failure_kind": retryable_failure_kind,
    }


def _direct_launch_child_from_request(
    *,
    request: dict[str, Any],
    state_root: Path,
    workspace_root: Path,
) -> dict[str, Any]:
    wrapper_prefix, wrapper_cmd = _wrapper_prefix()
    attempts: list[dict[str, Any]] = []
    final_attempt: dict[str, Any] | None = None
    launch_decision = "exited"
    for attempt_index in range(request["startup_retry_limit"] + 1):
        attempt = _launch_once(
            request=request,
            state_root=state_root,
            workspace_root=workspace_root,
            wrapper_prefix=wrapper_prefix,
            wrapper_cmd=wrapper_cmd,
        )
        attempt["attempt_index"] = attempt_index + 1
        attempts.append(attempt)
        final_attempt = attempt
        if attempt["launch_decision"] == "started":
            launch_decision = "started"
            break
        if _retryable_failure(str(attempt["retryable_failure_kind"])) and attempt_index < request["startup_retry_limit"]:
            time.sleep(min(1.0, 0.25 * float(attempt_index + 1)))
            continue
        if _retryable_failure(str(attempt["retryable_failure_kind"])) and attempt_index >= request["startup_retry_limit"]:
            launch_decision = "retry_exhausted"
        else:
            launch_decision = "exited"
        break

    assert final_attempt is not None
    result_ref = Path(final_attempt["launch_request_ref"]).parent / "ChildLaunchResult.json"
    root_identity_token = runtime_root_identity_token(state_root, ensure=True)
    result_payload = {
        "launch_decision": launch_decision,
        "source_result_ref": request["source_result_ref"],
        "node_id": request["node_id"],
        "workspace_root": str(workspace_root),
        "state_root": str(state_root),
        "runtime_root_identity_token": root_identity_token,
        "startup_health_timeout_ms": int(request["startup_health_timeout_ms"]),
        "startup_retry_limit": int(request["startup_retry_limit"]),
        "attempt_count": len(attempts),
        "retryable_failure_kind": "" if launch_decision in {"started", "exited"} and final_attempt["exit_code"] in (0, None) else str(final_attempt["retryable_failure_kind"]),
        "attempts": attempts,
        "launch_request_ref": str(final_attempt["launch_request_ref"]),
        "launch_result_ref": str(result_ref.resolve()),
        "launch_log_dir": str(final_attempt["launch_log_dir"]),
        "stdout_ref": str(final_attempt["stdout_ref"]),
        "stderr_ref": str(final_attempt["stderr_ref"]),
        "stdin_ref": str(final_attempt["stdin_ref"]),
        "wrapped_argv": list(final_attempt["wrapped_argv"]),
        "wrapper_cmd": str(final_attempt["wrapper_cmd"]),
        "pid": int(final_attempt["pid"]),
        "exit_code": final_attempt["exit_code"],
    }
    validate_repo_object("LoopChildLaunchResult.schema.json", result_payload)
    _write_json(result_ref, result_payload)
    try:
        _mirror_canonical_launch_started_event_from_launch_payload(result_payload)
    except Exception:
        pass
    return result_payload


def _launch_or_reuse_child_from_request(
    *,
    request: dict[str, Any],
    state_root: Path,
    workspace_root: Path,
) -> dict[str, Any]:
    lock_ref: Path | None = None
    try:
        existing_live: dict[str, Any] | None
        lock_ref, existing_live = _acquire_node_launch_gate(
            request=request,
            state_root=state_root,
            workspace_root=workspace_root,
        )
        if existing_live is not None:
            latest_numeric = _latest_numeric_attempt_launch_result(
                state_root=state_root,
                node_id=str(request["node_id"]),
                workspace_root=workspace_root,
            )
            if latest_numeric is not None and str(latest_numeric.get("launch_decision") or "") == "started_existing":
                latest_pid = int(latest_numeric.get("pid") or 0)
                existing_pid = int(existing_live.get("pid") or 0)
                if (
                    latest_pid > 0
                    and latest_pid == existing_pid
                    and _launch_result_process_identity_matches(latest_numeric)
                ):
                    return latest_numeric
            return _persist_reused_launch_result(
                request=request,
                state_root=state_root,
                workspace_root=workspace_root,
                existing=existing_live,
            )
        return _direct_launch_child_from_request(
            request=request,
            state_root=state_root,
            workspace_root=workspace_root,
        )
    finally:
        if lock_ref is not None:
            lock_ref.unlink(missing_ok=True)


def launch_child_from_result_ref(
    *,
    result_ref: str | Path,
    startup_probe_ms: int = 1500,
    startup_health_timeout_ms: int = DEFAULT_STARTUP_HEALTH_TIMEOUT_MS,
) -> dict[str, Any]:
    """Launch a materialized child from a bootstrap/recovery result that carries launch_spec."""

    result_path = Path(result_ref).expanduser().resolve()
    payload = json.loads(result_path.read_text(encoding="utf-8"))
    request = _normalize_launch_request(
        source_result_ref=str(result_path),
        payload=payload,
        startup_probe_ms=startup_probe_ms,
        startup_health_timeout_ms=startup_health_timeout_ms,
    )
    state_root = require_runtime_root(Path(request["state_root"]).expanduser().resolve())
    workspace_root = Path(request["workspace_root"]).expanduser().resolve()
    request["launch_spec"]["env"] = merge_runtime_transport_env(
        state_root=state_root,
        env=dict((request.get("launch_spec") or {}).get("env") or {}),
    )
    if should_request_host_child_launch():
        payload = request_host_child_launch(
            source_result_ref=request["source_result_ref"],
            node_id=str(request["node_id"]),
            state_root=state_root,
            startup_probe_ms=int(request["startup_probe_ms"]),
            startup_health_timeout_ms=int(request["startup_health_timeout_ms"]),
        )
        pid = int(payload.get("pid") or 0)
        launch_decision = str(payload.get("launch_decision") or "")
        if (
            launch_decision in {"started", "started_existing"}
            and pid > 0
            and not _launch_result_process_identity_matches(payload)
        ):
            return _launch_or_reuse_child_from_request(
                request=request,
                state_root=state_root,
                workspace_root=workspace_root,
            )
        return payload
    return _launch_or_reuse_child_from_request(
        request=request,
        state_root=state_root,
        workspace_root=workspace_root,
    )


def _direct_child_runtime_status_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
) -> dict[str, Any]:
    result_path = Path(result_ref).expanduser().resolve()
    launch_result = _load_json(result_path)
    validate_repo_object("LoopChildLaunchResult.schema.json", launch_result)

    state_root = require_runtime_root(Path(str(launch_result["state_root"])).expanduser().resolve())
    node_id = str(launch_result["node_id"])
    workspace_root = Path(str(launch_result.get("workspace_root") or "")).expanduser()
    node_ref = state_root / "state" / f"{node_id}.json"
    node_payload = _load_json(node_ref) if node_ref.exists() else {}
    hygiene_state = _live_workspace_artifact_hygiene_sync(
        workspace_root=workspace_root,
        machine_handoff_ref=_existing_machine_handoff_ref(state_root=state_root, node_id=node_id),
    )
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    authoritative_result_ref = _authoritative_result_ref(
        state_root=state_root,
        node_id=node_id,
        node_payload=node_payload,
    )
    authoritative_result_payload = (
        _load_optional_json(authoritative_result_ref)
        if authoritative_result_ref.exists()
        else {}
    )
    terminal_result_ref, terminal_result_payload = _load_terminal_implementer_result(
        state_root=state_root,
        node_id=node_id,
        node_payload=node_payload,
    )
    terminal_node_state = _durable_terminal_node_state(
        node_payload=node_payload,
        result_ref=authoritative_result_ref,
        result_payload=authoritative_result_payload,
    )

    pid = int(launch_result.get("pid") or 0)
    pid_alive = _launch_result_process_identity_matches(launch_result)
    stdout_path = Path(str(launch_result.get("stdout_ref") or "")).expanduser()
    stderr_path = Path(str(launch_result.get("stderr_ref") or "")).expanduser()
    latest_log_ref, latest_log_epoch = _latest_log_snapshot(
        result_path=result_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    now = time.time()
    latest_log_age_s = max(0.0, now - latest_log_epoch)
    launch_decision = str(launch_result.get("launch_decision") or "")
    lifecycle_status = str(node_payload.get("status") or "")
    runtime_attachment_state = str(runtime_state.get("attachment_state") or "")
    runtime_observed_at = str(runtime_state.get("observed_at") or "")
    runtime_observation_kind = str(runtime_state.get("observation_kind") or "")
    runtime_summary = str(runtime_state.get("summary") or "")
    runtime_evidence_refs = list(runtime_state.get("evidence_refs") or [])
    supervision_result_payload = _load_optional_json(result_path.with_name("ChildSupervisionResult.json"))
    settled_supervision_reason = str(supervision_result_payload.get("settled_reason") or "").strip().lower()
    preserve_blocked_supervision_stop_point = settled_supervision_reason in {
        "no_substantive_progress",
        "retry_budget_exhausted",
    }
    retryable_authoritative_result = authoritative_result_retryable_nonterminal(authoritative_result_payload)
    if retryable_authoritative_result:
        terminal_result_payload = {}
        terminal_node_state = None
        if lifecycle_status in {"", "COMPLETED", "FAILED"} or (
            lifecycle_status == "BLOCKED" and not preserve_blocked_supervision_stop_point
        ):
            lifecycle_status = "ACTIVE"
        if preserve_blocked_supervision_stop_point and lifecycle_status == "BLOCKED":
            runtime_attachment_state = "LOST"
            runtime_observation_kind = f"supervision:{settled_supervision_reason}"
            runtime_summary = _supervision_stop_point_summary(settled_supervision_reason)
            supervision_result_ref = str(result_path.with_name("ChildSupervisionResult.json").resolve())
            runtime_evidence_refs = [
                supervision_result_ref,
                *[str(item) for item in runtime_evidence_refs if str(item)],
            ]
        else:
            if runtime_attachment_state == "TERMINAL":
                runtime_attachment_state = "UNOBSERVED"
            runtime_observation_kind = "authoritative_retryable_result"
            runtime_summary = str(
                authoritative_result_payload.get("summary")
                or runtime_summary
                or "authoritative retryable evaluator result still requires local repair"
            )
            retryable_result_ref = str(authoritative_result_ref.resolve())
            runtime_evidence_refs = [retryable_result_ref, *[str(item) for item in runtime_evidence_refs if str(item)]]
    publication_state = dict(hygiene_state.get("publication_state") or {})
    local_runtime_state = dict(hygiene_state.get("local_runtime_state") or {})
    live_runtime_state = dict(hygiene_state.get("live_runtime_state") or {})
    hygiene_sync_error = str(hygiene_state.get("sync_error") or "").strip()
    publish_root_violation = bool(publication_state.get("violation"))
    if publish_root_violation:
        publication_state = repair_workspace_publication_surface(
            workspace_root=workspace_root,
            machine_handoff_ref=_existing_machine_handoff_ref(state_root=state_root, node_id=node_id),
        )
        publish_root_violation = bool(publication_state.get("violation"))
    local_workspace_violation = bool(local_runtime_state.get("violation"))
    live_root_violation = bool(live_runtime_state.get("violation"))
    if publish_root_violation:
        reset_workspace_publish_root(
            workspace_root=workspace_root,
            machine_handoff_ref=_existing_machine_handoff_ref(state_root=state_root, node_id=node_id),
        )
    if local_workspace_violation:
        reset_workspace_local_runtime_heavy_roots(
            workspace_root=workspace_root,
            machine_handoff_ref=_existing_machine_handoff_ref(state_root=state_root, node_id=node_id),
        )
    if live_root_violation:
        reset_live_artifact_runtime_heavy_roots(
            workspace_root=workspace_root,
            machine_handoff_ref=_existing_machine_handoff_ref(state_root=state_root, node_id=node_id),
        )
    if (publish_root_violation or local_workspace_violation or live_root_violation) and pid_alive:
        _reap_runtime_owned_launch(launch_result)
        pid_alive = _launch_result_process_identity_matches(launch_result)

    if terminal_result_payload:
        _reap_runtime_owned_launch(launch_result)
        pid_alive = _launch_result_process_identity_matches(launch_result)
        lifecycle_status = _resolved_terminal_lifecycle_status(
            node_payload=node_payload,
            result_payload=terminal_result_payload,
        )
        runtime_attachment_state = "TERMINAL"
        runtime_observed_at = str(terminal_result_payload.get("completed_at_utc") or runtime_observed_at)
        runtime_observation_kind = "terminal_result"
        runtime_summary = str(terminal_result_payload.get("summary") or runtime_summary or "authoritative terminal result present")
        runtime_evidence_refs = [
            str(terminal_result_ref.resolve()),
            *[str(item) for item in runtime_evidence_refs],
        ]
        stalled_hint = False
        recovery_eligible = False
        recovery_reason = "authoritative_terminal_result"
    elif terminal_node_state is not None:
        _reap_runtime_owned_launch(launch_result)
        pid_alive = _launch_result_process_identity_matches(launch_result)
        lifecycle_status = str(terminal_node_state["lifecycle_status"])
        runtime_attachment_state = str(terminal_node_state["runtime_attachment_state"])
        runtime_observed_at = str(terminal_node_state["runtime_observed_at"])
        runtime_observation_kind = str(terminal_node_state["runtime_observation_kind"])
        runtime_summary = str(terminal_node_state["runtime_summary"])
        runtime_evidence_refs = [str(item) for item in list(terminal_node_state["runtime_evidence_refs"])]
        stalled_hint = False
        recovery_eligible = False
        recovery_reason = str(terminal_node_state["recovery_reason"])
    elif publish_root_violation:
        lifecycle_status = lifecycle_status or "ACTIVE"
        runtime_attachment_state = "LOST"
        runtime_observed_at = _utc_timestamp()
        runtime_observation_kind = "publish_root_violation"
        runtime_summary = str(publication_state.get("violation_summary") or "publish root violated runtime publication invariants")
        violation_evidence = str(publication_state.get("publish_artifact_ref") or "").strip()
        runtime_evidence_refs = [item for item in [violation_evidence, *runtime_evidence_refs] if item]
        stalled_hint = False
        recovery_eligible = True
        recovery_reason = str(publication_state.get("violation_reason") or "publish_root_violation")
    elif local_workspace_violation:
        lifecycle_status = lifecycle_status or "ACTIVE"
        runtime_attachment_state = "LOST"
        runtime_observed_at = _utc_timestamp()
        runtime_observation_kind = "workspace_runtime_hygiene_violation"
        runtime_summary = str(
            local_runtime_state.get("violation_summary")
            or "workspace-local runtime heavy trees appeared despite an external live artifact root"
        )
        runtime_evidence_refs = [
            *[str(item) for item in list(local_runtime_state.get("workspace_runtime_heavy_root_refs") or [])],
            *runtime_evidence_refs,
        ]
        stalled_hint = False
        recovery_eligible = True
        recovery_reason = str(
            local_runtime_state.get("violation_reason") or "workspace_contains_local_runtime_heavy_trees"
        )
    elif live_root_violation:
        lifecycle_status = lifecycle_status or "ACTIVE"
        runtime_attachment_state = "LOST"
        runtime_observed_at = _utc_timestamp()
        runtime_observation_kind = "live_artifact_runtime_hygiene_violation"
        runtime_summary = str(
            live_runtime_state.get("violation_summary")
            or "live artifact root contains materialized runtime heavy trees"
        )
        runtime_evidence_refs = [
            *[str(item) for item in list(live_runtime_state.get("live_runtime_heavy_tree_refs") or [])],
            *runtime_evidence_refs,
        ]
        stalled_hint = False
        recovery_eligible = True
        recovery_reason = str(
            live_runtime_state.get("violation_reason") or "live_artifact_root_contains_runtime_heavy_trees"
        )
    else:
        lifecycle_active_like = lifecycle_status in {"", "ACTIVE"}
        runtime_loss_confirmed = bool(
            not pid_alive
            and lifecycle_active_like
            and (
                launch_result.get("exit_code") not in (None, "")
                or latest_log_age_s >= RUNTIME_LOSS_CONFIRMATION_S
                or _runtime_state_confirms_dead_active_loss(runtime_state)
            )
        )

        stalled_hint = bool(
            pid_alive
            and launch_decision in {"started", "started_existing"}
            and lifecycle_status == "ACTIVE"
            and latest_log_age_s >= max(0.0, float(stall_threshold_s))
        )
        recovery_eligible = runtime_loss_confirmed
        if pid_alive:
            runtime_attachment_state = "ATTACHED"
            runtime_observed_at = _utc_timestamp()
            recovery_reason = "live_pid_still_attached"
        elif lifecycle_status not in {"", "ACTIVE"}:
            recovery_reason = f"node_status_{lifecycle_status.lower() or 'unknown'}"
        elif not runtime_loss_confirmed:
            runtime_attachment_state = "UNOBSERVED"
            recovery_reason = "active_without_live_pid_unconfirmed"
        else:
            runtime_attachment_state = "LOST"
            recovery_reason = "active_without_live_pid"
        if hygiene_sync_error:
            runtime_observed_at = _utc_timestamp()
            runtime_observation_kind = "runtime_status_observation_error"
            runtime_summary = f"runtime status observation degraded after hygiene cleanup failure: {hygiene_sync_error}"
            runtime_evidence_refs = [str(result_path.resolve()), *runtime_evidence_refs]
            if pid_alive:
                runtime_attachment_state = "ATTACHED"
                recovery_eligible = False
                recovery_reason = "runtime_status_observation_error_live_pid"
            elif lifecycle_active_like:
                runtime_attachment_state = "LOST"
                recovery_eligible = runtime_loss_confirmed
                recovery_reason = (
                    "runtime_status_observation_error_without_live_pid"
                    if runtime_loss_confirmed
                    else "runtime_status_observation_error_without_live_pid_unconfirmed"
                )
    if (
        retryable_authoritative_result
        and not (preserve_blocked_supervision_stop_point and lifecycle_status == "BLOCKED")
        and runtime_observation_kind == "terminal_result"
    ):
        runtime_observation_kind = "authoritative_retryable_result"
        runtime_summary = str(
            authoritative_result_payload.get("summary")
            or runtime_summary
            or "authoritative retryable evaluator result still requires local repair"
        )
        retryable_result_ref = str(authoritative_result_ref.resolve())
        runtime_evidence_refs = [retryable_result_ref, *[str(item) for item in runtime_evidence_refs if str(item)]]

    status_result_ref = result_path.parent / "ChildRuntimeStatusResult.json"
    payload = _normalize_runtime_status_result(
        launch_result_ref=str(result_path),
        payload={
            "status_result_ref": str(status_result_ref.resolve()),
            "node_id": node_id,
            "workspace_root": str(launch_result.get("workspace_root") or ""),
            "state_root": str(state_root),
            "node_ref": str(node_ref.resolve()) if node_ref.exists() else "",
            "launch_decision": launch_decision,
            "pid": pid,
            "pid_alive": pid_alive,
            "exit_code": launch_result.get("exit_code"),
            "lifecycle_status": lifecycle_status,
            "runtime_attachment_state": runtime_attachment_state,
            "runtime_observed_at": runtime_observed_at,
            "runtime_observation_kind": runtime_observation_kind,
            "runtime_summary": runtime_summary,
            "runtime_evidence_refs": runtime_evidence_refs,
            "stdout_ref": str(stdout_path.resolve()),
            "stderr_ref": str(stderr_path.resolve()),
            "latest_log_ref": str(latest_log_ref.resolve()),
            "latest_log_mtime": _format_mtime(latest_log_epoch),
            "latest_log_age_s": latest_log_age_s,
            "stalled_hint": stalled_hint,
            "recovery_eligible": recovery_eligible,
            "recovery_reason": recovery_reason,
        },
        stall_threshold_s=stall_threshold_s,
    )
    _write_json(status_result_ref, payload)
    return payload


def child_runtime_status_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
) -> dict[str, Any]:
    """Summarize current child-runtime status from a committed ChildLaunchResult."""

    result_path = Path(result_ref).expanduser().resolve()
    try:
        from loop_product.runtime.lifecycle import synchronize_authoritative_state_from_launch_result_ref

        synchronize_authoritative_state_from_launch_result_ref(result_ref=result_path)
    except Exception:
        pass

    if should_request_host_child_runtime_status():
        try:
            payload = request_host_child_runtime_status(
                launch_result_ref=str(result_path),
                stall_threshold_s=float(stall_threshold_s),
            )
        except Exception as host_exc:
            try:
                payload = _direct_child_runtime_status_from_launch_result_ref(
                    result_ref=result_path,
                    stall_threshold_s=stall_threshold_s,
                )
            except Exception as direct_exc:
                return _observation_error_runtime_status_from_launch_result_ref(
                    result_ref=result_path,
                    stall_threshold_s=stall_threshold_s,
                    observation_error=direct_exc,
                    host_error=host_exc,
                )
        hygiene_state = _live_workspace_artifact_hygiene_sync(
            workspace_root=Path(str(payload.get("workspace_root") or "")).expanduser(),
            machine_handoff_ref=_existing_machine_handoff_ref(
                state_root=require_runtime_root(Path(str(payload.get("state_root") or "")).expanduser().resolve()),
                node_id=str(payload.get("node_id") or ""),
            ) if str(payload.get("state_root") or "").strip() and str(payload.get("node_id") or "").strip() else None,
        )
        publication_state = dict(hygiene_state.get("publication_state") or {})
        local_runtime_state = dict(hygiene_state.get("local_runtime_state") or {})
        live_runtime_state = dict(hygiene_state.get("live_runtime_state") or {})
        if bool(publication_state.get("violation")) or bool(local_runtime_state.get("violation")) or bool(live_runtime_state.get("violation")):
            launch_payload = _load_optional_json(Path(result_ref).expanduser().resolve())
            if launch_payload:
                _reap_runtime_owned_launch(launch_payload)
            payload = _direct_child_runtime_status_from_launch_result_ref(
                result_ref=result_path,
                stall_threshold_s=stall_threshold_s,
            )
        else:
            launch_payload = _load_optional_json(Path(result_ref).expanduser().resolve())
            pid = int(payload.get("pid") or 0)
            if bool(payload.get("pid_alive")) and pid > 0 and not _launch_result_process_identity_matches(launch_payload):
                payload = _direct_child_runtime_status_from_launch_result_ref(
                    result_ref=result_path,
                    stall_threshold_s=stall_threshold_s,
                )
            else:
                try:
                    launch_payload = _load_optional_json(result_path)
                    state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
                    node_id = str(launch_payload["node_id"])
                    node_ref = state_root / "state" / f"{node_id}.json"
                    node_payload = _load_json(node_ref) if node_ref.exists() else {}
                    result_ref = _authoritative_result_ref(
                        state_root=state_root,
                        node_id=node_id,
                        node_payload=node_payload,
                    )
                    result_payload = _load_optional_json(result_ref) if result_ref.exists() else {}
                    if _durable_terminal_node_state(
                        node_payload=node_payload,
                        result_ref=result_ref,
                        result_payload=result_payload,
                    ) is not None:
                        payload = _direct_child_runtime_status_from_launch_result_ref(
                            result_ref=result_path,
                            stall_threshold_s=stall_threshold_s,
                        )
                except Exception:
                    pass
    else:
        try:
            payload = _direct_child_runtime_status_from_launch_result_ref(
                result_ref=result_path,
                stall_threshold_s=stall_threshold_s,
            )
        except Exception as exc:
            return _observation_error_runtime_status_from_launch_result_ref(
                result_ref=result_path,
                stall_threshold_s=stall_threshold_s,
                observation_error=exc,
            )

    try:
        if _should_ensure_committed_supervision_after_status(payload):
            _ensure_committed_supervision_attached_if_needed(result_ref=result_path)
    except Exception:
        pass
    return payload
