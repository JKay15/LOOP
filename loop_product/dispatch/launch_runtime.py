"""Committed child launch helper surfaces."""

from __future__ import annotations

from datetime import datetime
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
from loop_product.dispatch.publication import (
    inspect_workspace_publication_state,
    inspect_workspace_local_runtime_state,
    reset_workspace_publish_root,
    reset_workspace_local_runtime_heavy_roots,
)
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
from loop_product.runtime_paths import require_runtime_root

DEFAULT_STARTUP_RETRY_LIMIT = 1
DEFAULT_STARTUP_HEALTH_TIMEOUT_MS = 12000
_STARTUP_HEALTH_POLL_MS = 250
_STARTUP_PROGRESS_RE = re.compile(r"(?m)^(thinking|codex|exec|assistant)$")
RUNTIME_LOSS_CONFIRMATION_S = 20.0


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _utc_timestamp() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_json(path: Path) -> dict[str, Any]:
    try:
        return _load_json(path)
    except Exception:
        return {}


def _authoritative_result_ref(*, state_root: Path, node_id: str, node_payload: dict[str, Any]) -> Path:
    sink = str(node_payload.get("result_sink_ref") or "").strip()
    if not sink:
        sink = f"artifacts/{node_id}/implementer_result.json"
    return (state_root / sink).resolve()


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


def _live_workspace_artifact_hygiene_sync(*, workspace_root: Path | None) -> dict[str, Any]:
    if workspace_root is None:
        return {
            "removed_refs": [],
            "publication_state": {"applicable": False, "violation": False},
            "local_runtime_state": {"applicable": False, "violation": False},
        }
    try:
        candidate = workspace_root.expanduser().resolve()
    except Exception:
        return {
            "removed_refs": [],
            "publication_state": {"applicable": False, "violation": False},
            "local_runtime_state": {"applicable": False, "violation": False},
        }
    if not candidate.exists() or not candidate.is_dir():
        return {
            "removed_refs": [],
            "publication_state": {"applicable": False, "violation": False},
            "local_runtime_state": {"applicable": False, "violation": False},
        }
    publication_state = inspect_workspace_publication_state(workspace_root=candidate)
    local_runtime_state = inspect_workspace_local_runtime_state(workspace_root=candidate)
    excluded_roots: list[Path] = []
    publish_ref = str(publication_state.get("publish_artifact_ref") or "").strip()
    if publish_ref:
        excluded_roots.append(Path(publish_ref).expanduser().resolve())
    removed_refs = canonicalize_workspace_artifact_heavy_trees(candidate, exclude_roots=excluded_roots)
    return {
        "removed_refs": removed_refs,
        "publication_state": publication_state,
        "local_runtime_state": local_runtime_state,
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
    if str(payload.get("status") or "").strip().upper() != "COMPLETED":
        return False
    evaluator_result = dict(payload.get("evaluator_result") or {})
    verdict = str(evaluator_result.get("verdict") or "").strip().upper()
    if not verdict:
        return False
    report_ref = _terminal_evaluation_report_ref(payload)
    if report_ref is None or not report_ref.exists():
        return False
    return True


def _next_launch_attempt_dir(state_root: Path, node_id: str) -> Path:
    base = state_root / "artifacts" / "launches" / node_id
    suffix = 1
    while True:
        candidate = base / f"attempt_{suffix:03d}"
        if not candidate.exists():
            return candidate
        suffix += 1


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


def _reap_runtime_owned_launch(launch_result: dict[str, Any]) -> None:
    pid = int(launch_result.get("pid") or 0)
    if pid > 0:
        _terminate_pid(pid)


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
        pid = int(payload.get("pid") or 0)
        if pid <= 0 or not _pid_alive(pid):
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
    payload = {
        "launch_decision": "started_existing",
        "source_result_ref": request["source_result_ref"],
        "node_id": request["node_id"],
        "workspace_root": str(workspace_root),
        "state_root": str(state_root),
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
    return payload


def _wrapper_prefix() -> tuple[list[str], str]:
    raw = str(os.environ.get("LOOP_CHILD_LAUNCH_WRAPPER") or "").strip()
    if not raw:
        return [], ""
    prefix = shlex.split(raw)
    if not prefix:
        return [], ""
    return prefix, raw


def _child_launch_env_overrides() -> dict[str, str]:
    sanitized_parent_codex_env = {
        key: ""
        for key in os.environ
        if key.startswith("CODEX_") and key != "CODEX_HOME"
    }
    return {
        **sanitized_parent_codex_env,
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
    launch_env = {
        **dict(launch_spec.get("env") or {}),
        **_child_launch_env_overrides(),
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
    result_payload = {
        "launch_decision": launch_decision,
        "source_result_ref": request["source_result_ref"],
        "node_id": request["node_id"],
        "workspace_root": str(workspace_root),
        "state_root": str(state_root),
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
    return result_payload


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
    existing_live = _existing_live_launch_result(
        state_root=state_root,
        node_id=str(request["node_id"]),
        workspace_root=workspace_root,
    )
    if existing_live is not None:
        return _persist_reused_launch_result(
            request=request,
            state_root=state_root,
            workspace_root=workspace_root,
            existing=existing_live,
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
        if launch_decision in {"started", "started_existing"} and pid > 0 and not _pid_alive(pid):
            return _direct_launch_child_from_request(
                request=request,
                state_root=state_root,
                workspace_root=workspace_root,
            )
        return payload
    return _direct_launch_child_from_request(
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
    hygiene_state = _live_workspace_artifact_hygiene_sync(workspace_root=workspace_root)
    node_ref = state_root / "state" / f"{node_id}.json"
    node_payload = _load_json(node_ref) if node_ref.exists() else {}
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    terminal_result_ref, terminal_result_payload = _load_terminal_implementer_result(
        state_root=state_root,
        node_id=node_id,
        node_payload=node_payload,
    )

    pid = int(launch_result.get("pid") or 0)
    pid_alive = pid > 0 and _pid_alive(pid)
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
    publication_state = dict(hygiene_state.get("publication_state") or {})
    local_runtime_state = dict(hygiene_state.get("local_runtime_state") or {})
    publish_root_violation = bool(publication_state.get("violation"))
    local_workspace_violation = bool(local_runtime_state.get("violation"))
    if publish_root_violation:
        reset_workspace_publish_root(workspace_root=workspace_root)
    if local_workspace_violation:
        reset_workspace_local_runtime_heavy_roots(workspace_root=workspace_root)
    if (publish_root_violation or local_workspace_violation) and pid_alive:
        _reap_runtime_owned_launch(launch_result)
        pid_alive = pid > 0 and _pid_alive(pid)

    if terminal_result_payload:
        _reap_runtime_owned_launch(launch_result)
        pid_alive = pid > 0 and _pid_alive(pid)
        lifecycle_status = "COMPLETED"
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
    else:
        lifecycle_active_like = lifecycle_status in {"", "ACTIVE"}
        runtime_loss_confirmed = bool(
            not pid_alive
            and lifecycle_active_like
            and (
                launch_result.get("exit_code") not in (None, "")
                or latest_log_age_s >= RUNTIME_LOSS_CONFIRMATION_S
                or str(runtime_state.get("attachment_state") or "").upper() == "LOST"
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
            recovery_reason = "live_pid_still_attached"
        elif lifecycle_status not in {"", "ACTIVE"}:
            recovery_reason = f"node_status_{lifecycle_status.lower() or 'unknown'}"
        elif not runtime_loss_confirmed:
            recovery_reason = "active_without_live_pid_unconfirmed"
        else:
            recovery_reason = "active_without_live_pid"

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
        payload = request_host_child_runtime_status(
            launch_result_ref=str(result_path),
            stall_threshold_s=float(stall_threshold_s),
        )
        hygiene_state = _live_workspace_artifact_hygiene_sync(
            workspace_root=Path(str(payload.get("workspace_root") or "")).expanduser()
        )
        publication_state = dict(hygiene_state.get("publication_state") or {})
        if bool(publication_state.get("violation")):
            launch_payload = _load_optional_json(Path(result_ref).expanduser().resolve())
            if launch_payload:
                _reap_runtime_owned_launch(launch_payload)
            return _direct_child_runtime_status_from_launch_result_ref(
                result_ref=result_path,
                stall_threshold_s=stall_threshold_s,
            )
        pid = int(payload.get("pid") or 0)
        if bool(payload.get("pid_alive")) and pid > 0 and not _pid_alive(pid):
            return _direct_child_runtime_status_from_launch_result_ref(
                result_ref=result_path,
                stall_threshold_s=stall_threshold_s,
            )
        return payload

    return _direct_child_runtime_status_from_launch_result_ref(
        result_ref=result_path,
        stall_threshold_s=stall_threshold_s,
    )
