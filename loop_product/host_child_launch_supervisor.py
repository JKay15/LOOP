"""Host-side supervisor for queued child launch/runtime-status requests."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from loop_product.child_supervision_sidecar import ensure_child_supervision_running
from loop_product.dispatch.launch_runtime import terminate_runtime_owned_launch_result_ref
from loop_product.host_child_launch import force_direct_child_launch_mode, host_child_launch_request_root
from loop_product.host_child_runtime_status import (
    force_direct_child_runtime_status_mode,
    host_child_runtime_status_request_root,
)
from loop_product.runtime.cleanup_authority import runtime_cleanup_retirement_authority
from loop_product.runtime import lifecycle as runtime_lifecycle
from loop_product.runtime.launch_surface import launch_child_from_result_ref
from loop_product.state_io import write_json_read_only

_DEFAULT_POLL_INTERVAL_S = 0.5
_DEFAULT_IDLE_EXIT_AFTER_S = 30.0
_STARTUP_TIMEOUT_S = 5.0
_COMPLETED_REQUEST_RETENTION_S = 30.0
_AUTHORITY_REUSE_SETTLE_S = 0.1
HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID = "host_child_launch_supervisor"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_json_read_only(path, payload)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _runtime_code_fingerprint() -> str:
    package_root = Path(__file__).resolve().parent
    refs = [
        package_root / "host_child_launch_supervisor.py",
        package_root / "host_child_runtime_status.py",
        package_root / "host_child_launch.py",
        package_root / "child_supervision_sidecar.py",
        package_root / "artifact_hygiene.py",
        package_root / "runtime" / "lifecycle.py",
        package_root / "runtime" / "launch_surface.py",
        package_root / "dispatch" / "launch_runtime.py",
        package_root / "dispatch" / "publication.py",
    ]
    digest = hashlib.sha256()
    for ref in refs:
        resolved = ref.resolve()
        digest.update(str(resolved).encode("utf-8"))
        digest.update(b"\0")
        digest.update(resolved.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


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


def _command_tokens(command_line: str) -> list[str]:
    return [token for token in str(command_line or "").split() if token]


def _command_flag_value(command_line: str, flag: str) -> str:
    normalized_flag = str(flag or "").strip()
    if not normalized_flag:
        return ""
    tokens = _command_tokens(command_line)
    for index, token in enumerate(tokens):
        if token == normalized_flag:
            if index + 1 < len(tokens):
                return str(tokens[index + 1] or "").strip()
            return ""
        if token.startswith(f"{normalized_flag}="):
            return str(token.split("=", 1)[1] or "").strip()
    return ""


def _command_repo_root_matches(command_line: str, repo_root: str | Path) -> bool:
    raw_repo_root = _command_flag_value(command_line, "--repo-root")
    if not raw_repo_root:
        return False
    try:
        return Path(raw_repo_root).expanduser().resolve() == Path(repo_root).expanduser().resolve()
    except Exception:
        return False


def _runtime_marker_matches_process(payload: dict[str, Any]) -> bool:
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    repo_root = str(payload.get("repo_root") or "").strip()
    return "loop_product.host_child_launch_supervisor" in command_line and _command_repo_root_matches(command_line, repo_root)


def _host_supervisor_process_matches(*, pid: int, repo_root: Path) -> bool:
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    return "loop_product.host_child_launch_supervisor" in command_line and _command_repo_root_matches(command_line, repo_root)


def _host_supervisor_process_fingerprint(*, pid: int) -> str:
    command_line = _pid_command_line(pid)
    if not command_line:
        return ""
    digest = hashlib.sha256()
    digest.update(command_line.encode("utf-8"))
    return digest.hexdigest()


def _matching_host_supervisor_processes(*, repo_root: Path) -> list[dict[str, Any]]:
    try:
        proc = subprocess.run(
            ["ps", "-ax", "-o", "pid=,command="],
            text=True,
            capture_output=True,
            check=True,
        )
    except Exception:
        return []
    matches: list[dict[str, Any]] = []
    for line in str(proc.stdout or "").splitlines():
        raw = line.strip()
        if not raw:
            continue
        pid_text, _, command = raw.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid <= 0 or "loop_product.host_child_launch_supervisor" not in command:
            continue
        if not _command_repo_root_matches(command, repo_root):
            continue
        matches.append({"pid": pid, "command": command})
    return matches


def host_supervisor_runtime_root(*, repo_root: Path) -> Path:
    return supervisor_runtime_ref(repo_root=repo_root).parent


def _host_supervisor_launch_identity(*, repo_root: Path) -> str:
    return str(supervisor_runtime_ref(repo_root=repo_root))


def _host_supervisor_authority_payload(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    supervisor_root = host_supervisor_runtime_root(repo_root=repo_root)
    effective = dict(
        dict(
            query_runtime_liveness_view(supervisor_root, initialize_if_missing=False).get(
                "effective_runtime_liveness_by_node"
            )
            or {}
        ).get(
            HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID
        )
        or {}
    )
    if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
        return None
    payload = dict(effective.get("payload") or {})
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return None
    if not _host_supervisor_process_matches(pid=pid, repo_root=repo_root):
        return None
    current_process_fingerprint = _host_supervisor_process_fingerprint(pid=pid)
    payload_process_fingerprint = str(payload.get("process_fingerprint") or "")
    if payload_process_fingerprint and current_process_fingerprint and payload_process_fingerprint != current_process_fingerprint:
        return None
    current_code_fingerprint = _runtime_code_fingerprint()
    payload_code_fingerprint = str(payload.get("code_fingerprint") or "")
    if payload_code_fingerprint and payload_code_fingerprint != current_code_fingerprint:
        return None
    launch_identity = str(payload.get("launch_event_id") or payload.get("launch_result_ref") or "").strip()
    expected_launch_identity = _host_supervisor_launch_identity(repo_root=repo_root)
    if launch_identity and launch_identity != expected_launch_identity:
        return None
    return {
        **payload,
        "pid": pid,
        "process_fingerprint": current_process_fingerprint or payload_process_fingerprint,
        "code_fingerprint": payload_code_fingerprint or current_code_fingerprint,
    }


def _materialize_host_supervisor_runtime_marker_from_authority(
    *,
    repo_root: Path,
    authority_payload: dict[str, Any],
) -> dict[str, Any]:
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    log_ref = _log_ref_for_repo(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = {
        "schema": "loop_product.host_child_launch_supervisor_runtime",
        "repo_root": str(repo_root),
        "pid": int(authority_payload.get("pid") or 0),
        "code_fingerprint": str(authority_payload.get("code_fingerprint") or _runtime_code_fingerprint()),
        "poll_interval_s": float(authority_payload.get("poll_interval_s") or _DEFAULT_POLL_INTERVAL_S),
        "idle_exit_after_s": float(authority_payload.get("idle_exit_after_s") or _DEFAULT_IDLE_EXIT_AFTER_S),
        "watch_repo_control_plane": bool(authority_payload.get("watch_repo_control_plane")),
        "lease_epoch": int(authority_payload.get("lease_epoch") or 0),
        "started_at_utc": str(
            authority_payload.get("process_started_at_utc")
            or authority_payload.get("started_at_utc")
            or authority_payload.get("observed_at")
            or _now_iso()
        ),
        "log_ref": str(log_ref),
    }
    write_json_read_only(runtime_ref, marker_payload)
    return marker_payload


def _stable_host_supervisor_authority_payload(
    *,
    repo_root: Path,
    settle_s: float = 0.0,
) -> dict[str, Any] | None:
    authority_payload = _host_supervisor_authority_payload(repo_root=repo_root)
    if authority_payload is None:
        return None
    if float(settle_s) <= 0.0:
        return authority_payload
    time.sleep(float(settle_s))
    return _host_supervisor_authority_payload(repo_root=repo_root)


def _process_scan_host_supervisor_runtime_marker(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    liveness_view = query_runtime_liveness_view(
        host_supervisor_runtime_root(repo_root=repo_root),
        initialize_if_missing=False,
    )
    effective = dict(
        dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID) or {}
    )
    effective_attachment_state = str(
        effective.get("effective_attachment_state")
        or effective.get("raw_attachment_state")
        or ""
    ).strip().upper()
    if effective_attachment_state in {"TERMINAL", "LOST"}:
        return None
    matches = _matching_host_supervisor_processes(repo_root=repo_root)
    if len(matches) != 1:
        return None
    match = dict(matches[0] or {})
    pid = int(match.get("pid") or 0)
    if pid <= 0:
        return None
    command_line = str(match.get("command") or "")
    effective_payload = dict(effective.get("payload") or {})
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    log_ref = _log_ref_for_repo(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = {
        "schema": "loop_product.host_child_launch_supervisor_runtime",
        "repo_root": str(repo_root),
        "pid": pid,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(_command_flag_value(command_line, "--poll-interval-s") or _DEFAULT_POLL_INTERVAL_S),
        "idle_exit_after_s": float(_command_flag_value(command_line, "--idle-exit-after-s") or _DEFAULT_IDLE_EXIT_AFTER_S),
        "watch_repo_control_plane": "--watch-repo-control-plane" in _command_tokens(command_line),
        "lease_epoch": int(effective_payload.get("lease_epoch") or effective.get("lease_epoch") or 0),
        "started_at_utc": str(
            effective_payload.get("process_started_at_utc")
            or effective_payload.get("started_at_utc")
            or effective_payload.get("observed_at")
            or effective.get("observed_at")
            or _now_iso()
        ),
        "log_ref": str(log_ref),
    }
    write_json_read_only(runtime_ref, marker_payload)
    return marker_payload


def _materialize_host_supervisor_runtime_marker_from_spawned_process(
    *,
    repo_root: Path,
    pid: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
    watch_repo_control_plane: bool,
) -> dict[str, Any] | None:
    candidate_pid = int(pid or 0)
    if candidate_pid <= 0 or not _host_supervisor_process_matches(pid=candidate_pid, repo_root=repo_root):
        return None
    matches = _matching_host_supervisor_processes(repo_root=repo_root)
    if len(matches) != 1 or int(dict(matches[0] or {}).get("pid") or 0) != candidate_pid:
        return None
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    log_ref = _log_ref_for_repo(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = {
        "schema": "loop_product.host_child_launch_supervisor_runtime",
        "repo_root": str(repo_root),
        "pid": candidate_pid,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "watch_repo_control_plane": bool(watch_repo_control_plane),
        "lease_epoch": 0,
        "started_at_utc": _now_iso(),
        "log_ref": str(log_ref),
    }
    write_json_read_only(runtime_ref, marker_payload)
    return marker_payload


def _host_supervisor_next_lease_epoch(*, repo_root: Path) -> int:
    from loop_product.event_journal import iter_committed_events

    latest_epoch = 0
    supervisor_root = host_supervisor_runtime_root(repo_root=repo_root)
    for event in iter_committed_events(supervisor_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        if str(event.get("node_id") or "") != HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("attachment_state") or "").strip().upper() != "ATTACHED":
            continue
        try:
            latest_epoch = max(latest_epoch, int(payload.get("lease_epoch") or 0))
        except (TypeError, ValueError):
            continue
    return latest_epoch + 1 if latest_epoch > 0 else 1


def _host_supervisor_liveness_observation_id(
    *,
    repo_root: Path,
    pid: int,
    lease_epoch: int,
    heartbeat_ordinal: int,
) -> str:
    payload = {
        "repo_root": str(repo_root),
        "pid": int(pid),
        "lease_epoch": int(lease_epoch),
        "heartbeat_ordinal": int(heartbeat_ordinal),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"host_supervisor_liveness::{digest}"


def _host_supervisor_identity_observation_id(
    *,
    repo_root: Path,
    pid: int,
    process_fingerprint: str,
    launch_identity: str,
    lease_epoch: int,
    process_started_at_utc: str,
) -> str:
    payload = {
        "repo_root": str(repo_root),
        "pid": int(pid),
        "process_fingerprint": str(process_fingerprint),
        "launch_identity": str(launch_identity),
        "lease_epoch": int(lease_epoch),
        "process_started_at_utc": str(process_started_at_utc),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"host_supervisor_identity::{digest}"


def _host_supervisor_exit_observation_id(
    *,
    repo_root: Path,
    pid: int,
    lease_epoch: int,
    exit_reason: str,
    process_started_at_utc: str,
) -> str:
    payload = {
        "repo_root": str(repo_root),
        "pid": int(pid),
        "lease_epoch": int(lease_epoch),
        "exit_reason": str(exit_reason),
        "process_started_at_utc": str(process_started_at_utc),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"host_supervisor_exit::{digest}"


def _host_supervisor_process_exit_observation_id(
    *,
    repo_root: Path,
    pid: int,
    lease_epoch: int,
    exit_reason: str,
    process_started_at_utc: str,
) -> str:
    payload = {
        "repo_root": str(repo_root),
        "pid": int(pid),
        "lease_epoch": int(lease_epoch),
        "exit_reason": str(exit_reason),
        "process_started_at_utc": str(process_started_at_utc),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"host_supervisor_process_exit::{digest}"


def _emit_host_supervisor_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    heartbeat_ordinal: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
    watch_repo_control_plane: bool,
    process_started_at_utc: str,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_process_identity_confirmed_event,
    )

    pid = os.getpid()
    observed_at = _now_iso()
    process_fingerprint = _host_supervisor_process_fingerprint(pid=pid)
    launch_identity = _host_supervisor_launch_identity(repo_root=repo_root)
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    supervisor_root = host_supervisor_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "host_child_launch_supervisor",
        "attachment_state": "ATTACHED",
        "observed_at": observed_at,
        "lease_owner_id": f"host-supervisor:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "lease_duration_s": max(5.0, float(poll_interval_s) * 3.0),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "watch_repo_control_plane": bool(watch_repo_control_plane),
        "launch_result_ref": launch_identity,
        "launch_event_id": launch_identity,
        "process_started_at_utc": str(process_started_at_utc),
        "runtime_ref": str(runtime_ref),
    }
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        supervisor_root,
        observation_id=_host_supervisor_liveness_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
        ),
        node_id=HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID,
        payload=payload,
        producer="host_child_launch_supervisor.main",
    )
    if heartbeat_ordinal == 0:
        commit_process_identity_confirmed_event(
            supervisor_root,
            observation_id=_host_supervisor_identity_observation_id(
                repo_root=repo_root,
                pid=pid,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
                lease_epoch=lease_epoch,
                process_started_at_utc=process_started_at_utc,
            ),
            node_id=HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID,
            payload={
                "controller_kind": "host_child_launch_supervisor",
                "observed_at": observed_at,
                "pid": int(pid),
                "process_fingerprint": process_fingerprint,
                "code_fingerprint": _runtime_code_fingerprint(),
                "poll_interval_s": float(poll_interval_s),
                "idle_exit_after_s": float(idle_exit_after_s),
                "watch_repo_control_plane": bool(watch_repo_control_plane),
                "launch_result_ref": launch_identity,
                "launch_event_id": launch_identity,
                "process_started_at_utc": str(process_started_at_utc),
                "lease_epoch": int(lease_epoch),
                "lease_owner_id": f"host-supervisor:{runtime_ref}",
                "runtime_ref": str(runtime_ref),
            },
            producer="host_child_launch_supervisor.main",
        )
    return dict(liveness_outcome or {})


def _emit_host_supervisor_exit_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
    watch_repo_control_plane: bool,
    process_started_at_utc: str,
    exit_reason: str,
    cleanup_authority: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_process_exit_observed_event,
    )

    pid = os.getpid()
    observed_at = _now_iso()
    process_fingerprint = _host_supervisor_process_fingerprint(pid=pid)
    launch_identity = _host_supervisor_launch_identity(repo_root=repo_root)
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    supervisor_root = host_supervisor_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "host_child_launch_supervisor",
        "attachment_state": "TERMINAL",
        "observation_kind": f"host_supervisor_exit:{str(exit_reason)}",
        "exit_reason": str(exit_reason),
        "observed_at": observed_at,
        "lease_owner_id": f"host-supervisor:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "watch_repo_control_plane": bool(watch_repo_control_plane),
        "launch_result_ref": launch_identity,
        "launch_event_id": launch_identity,
        "process_started_at_utc": str(process_started_at_utc),
        "runtime_ref": str(runtime_ref),
    }
    authority = dict(cleanup_authority or {})
    if authority:
        payload["cleanup_authority_event_id"] = str(authority.get("event_id") or "")
        payload["cleanup_authority_event_type"] = str(authority.get("event_type") or "")
        payload["cleanup_authority_seq"] = int(authority.get("seq") or 0)
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        supervisor_root,
        observation_id=_host_supervisor_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID,
        payload=payload,
        producer="host_child_launch_supervisor.main",
    )
    process_exit_event = commit_process_exit_observed_event(
        supervisor_root,
        observation_id=_host_supervisor_process_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=HOST_CHILD_LAUNCH_SUPERVISOR_NODE_ID,
        payload=payload,
        producer="host_child_launch_supervisor.main",
    )
    return {
        "liveness_outcome": dict(liveness_outcome or {}),
        "process_exit_event": dict(process_exit_event or {}),
    }


def _host_supervisor_should_self_fence(liveness_outcome: dict[str, Any] | None) -> bool:
    outcome = str(dict(liveness_outcome or {}).get("outcome") or "").strip()
    return outcome.startswith("rejected_")


def _host_supervisor_cleanup_authority(*, repo_root: Path) -> dict[str, Any] | None:
    return runtime_cleanup_retirement_authority(
        state_root=host_supervisor_runtime_root(repo_root=repo_root),
        repo_root=repo_root,
    )


def supervisor_runtime_ref(*, repo_root: Path) -> Path:
    return repo_root.expanduser().resolve() / ".loop" / "host_child_launch_supervisor" / "HostChildLaunchSupervisorRuntime.json"


def _load_runtime_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def live_supervisor_runtime(*, repo_root: Path) -> dict[str, Any] | None:
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    if not runtime_ref.exists():
        authority_payload = _stable_host_supervisor_authority_payload(
            repo_root=repo_root,
            settle_s=_AUTHORITY_REUSE_SETTLE_S,
        )
        if authority_payload is not None:
            return _materialize_host_supervisor_runtime_marker_from_authority(
                repo_root=repo_root,
                authority_payload=authority_payload,
            )
        return _process_scan_host_supervisor_runtime_marker(repo_root=repo_root)
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        authority_payload = _stable_host_supervisor_authority_payload(
            repo_root=repo_root,
            settle_s=_AUTHORITY_REUSE_SETTLE_S,
        )
        runtime_ref.unlink(missing_ok=True)
        if authority_payload is not None:
            return _materialize_host_supervisor_runtime_marker_from_authority(
                repo_root=repo_root,
                authority_payload=authority_payload,
            )
        return _process_scan_host_supervisor_runtime_marker(repo_root=repo_root)
    if not _runtime_marker_matches_process(payload):
        time.sleep(_AUTHORITY_REUSE_SETTLE_S)
        try:
            refreshed_payload = _load_runtime_payload(runtime_ref)
        except Exception:
            refreshed_payload = {}
        if refreshed_payload and _runtime_marker_matches_process(refreshed_payload):
            return refreshed_payload
        authority_payload = _host_supervisor_authority_payload(repo_root=repo_root)
        if authority_payload is not None:
            return _materialize_host_supervisor_runtime_marker_from_authority(
                repo_root=repo_root,
                authority_payload=authority_payload,
            )
        runtime_ref.unlink(missing_ok=True)
        return _process_scan_host_supervisor_runtime_marker(repo_root=repo_root)
    return payload


def _terminate_supervisor_pid(pid: int, *, wait_s: float = 1.0) -> None:
    try:
        candidate = int(pid)
    except (TypeError, ValueError):
        return
    if candidate <= 0 or not _pid_alive(candidate):
        return
    try:
        os.kill(candidate, signal.SIGTERM)
    except OSError:
        return
    deadline = time.monotonic() + max(0.0, float(wait_s))
    while time.monotonic() < deadline:
        if not _pid_alive(candidate):
            return
        time.sleep(0.05)
    try:
        os.kill(candidate, signal.SIGKILL)
    except OSError:
        return


def _log_ref_for_repo(*, repo_root: Path) -> Path:
    return supervisor_runtime_ref(repo_root=repo_root).with_suffix(".log")


def _startup_gate_ref(*, repo_root: Path) -> Path:
    return host_supervisor_runtime_root(repo_root=repo_root) / "HostChildLaunchSupervisorStartup.lock"


def _load_startup_gate_payload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _acquire_startup_gate(
    *,
    repo_root: Path,
    startup_timeout_s: float,
) -> tuple[Path | None, dict[str, Any] | None]:
    lock_ref = _startup_gate_ref(repo_root=repo_root)
    deadline = time.monotonic() + max(1.0, float(startup_timeout_s))
    while time.monotonic() < deadline:
        existing = live_supervisor_runtime(repo_root=repo_root)
        if existing is not None:
            return None, existing
        lock_ref.parent.mkdir(parents=True, exist_ok=True)
        try:
            fd = os.open(lock_ref, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            payload = _load_startup_gate_payload(lock_ref)
            owner_pid = int(payload.get("pid") or 0)
            if owner_pid == os.getpid():
                existing = live_supervisor_runtime(repo_root=repo_root)
                if existing is not None:
                    return None, existing
                return lock_ref, None
            if owner_pid > 0 and not _pid_alive(owner_pid):
                lock_ref.unlink(missing_ok=True)
                continue
            time.sleep(0.05)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "schema": "loop_product.host_child_launch_supervisor_startup_lock",
                    "repo_root": str(repo_root),
                    "pid": os.getpid(),
                    "started_at_utc": _now_iso(),
                },
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
        existing = live_supervisor_runtime(repo_root=repo_root)
        if existing is not None:
            lock_ref.unlink(missing_ok=True)
            return None, existing
        return lock_ref, None
    raise TimeoutError(f"timed out waiting to acquire host child launch supervisor startup gate at {lock_ref}")


def ensure_host_child_launch_supervisor_running(
    *,
    repo_root: Path,
    poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_IDLE_EXIT_AFTER_S,
    watch_repo_control_plane: bool = False,
    startup_timeout_s: float = _STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    cleanup_authority = _host_supervisor_cleanup_authority(repo_root=repo_root)
    if cleanup_authority is not None:
        return {
            "bootstrap_status": "blocked_cleanup_committed",
            "service_kind": "host_child_launch_supervisor",
            "repo_root": str(repo_root),
            "cleanup_authority_event_id": str(cleanup_authority.get("event_id") or ""),
            "cleanup_authority_event_type": str(cleanup_authority.get("event_type") or ""),
            "cleanup_authority_seq": int(cleanup_authority.get("seq") or 0),
        }
    from loop_product.runtime.gc import ensure_housekeeping_reap_controller_service_running

    def _ensure_watch_mode_housekeeping() -> dict[str, Any]:
        return ensure_housekeeping_reap_controller_service_running(
            repo_root=repo_root,
            watch_repo_control_plane=True,
        )

    startup_lock_ref, gated_existing = _acquire_startup_gate(
        repo_root=repo_root,
        startup_timeout_s=startup_timeout_s,
    )
    if gated_existing is not None:
        _ensure_watch_mode_housekeeping()
        return gated_existing
    try:
        package_repo_root = Path(__file__).resolve().parents[1]
        current_code_fingerprint = _runtime_code_fingerprint()
        existing = live_supervisor_runtime(repo_root=repo_root)
        if existing is not None:
            existing_code_fingerprint = str(existing.get("code_fingerprint") or "").strip()
            existing_watch_repo_control_plane = bool(existing.get("watch_repo_control_plane"))
            if existing_code_fingerprint == current_code_fingerprint and existing_watch_repo_control_plane == bool(
                watch_repo_control_plane
            ):
                _ensure_watch_mode_housekeeping()
                return existing
            _terminate_supervisor_pid(int(existing.get("pid") or 0))
            supervisor_runtime_ref(repo_root=repo_root).unlink(missing_ok=True)

        runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
        log_ref = _log_ref_for_repo(repo_root=repo_root)
        runtime_ref.parent.mkdir(parents=True, exist_ok=True)
        with log_ref.open("a", encoding="utf-8") as log_handle:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "loop_product.host_child_launch_supervisor",
                    "--repo-root",
                    str(repo_root),
                    "--poll-interval-s",
                    str(max(0.05, float(poll_interval_s))),
                    "--idle-exit-after-s",
                    str(max(0.0, float(idle_exit_after_s))),
                    *(["--watch-repo-control-plane"] if watch_repo_control_plane else []),
                ],
                cwd=str(package_repo_root),
                stdin=subprocess.DEVNULL,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
            )

        deadline = time.monotonic() + max(1.0, float(startup_timeout_s))
        while time.monotonic() < deadline:
            payload = live_supervisor_runtime(repo_root=repo_root)
            if payload is not None:
                _ensure_watch_mode_housekeeping()
                return payload
            spawned_payload = _materialize_host_supervisor_runtime_marker_from_spawned_process(
                repo_root=repo_root,
                pid=proc.pid,
                poll_interval_s=max(0.05, float(poll_interval_s)),
                idle_exit_after_s=max(0.0, float(idle_exit_after_s)),
                watch_repo_control_plane=watch_repo_control_plane,
            )
            if spawned_payload is not None:
                _ensure_watch_mode_housekeeping()
                return spawned_payload
            exit_code = proc.poll()
            if exit_code is not None:
                payload = live_supervisor_runtime(repo_root=repo_root)
                if payload is not None:
                    return payload
                if int(exit_code) == 0:
                    time.sleep(0.05)
                    continue
                detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
                raise RuntimeError(detail or f"host child launch supervisor exited early with code {exit_code}")
            time.sleep(0.05)
        raise TimeoutError(f"timed out waiting for host child launch supervisor runtime marker at {runtime_ref}")
    finally:
        if startup_lock_ref is not None:
            startup_lock_ref.unlink(missing_ok=True)


def _iter_pending_requests(request_root: Path) -> list[Path]:
    return sorted(request_root.glob("*/HostChildLaunchRequest.json"))


def _response_exists(request_path: Path) -> bool:
    return (request_path.parent / "HostChildLaunchResponse.json").exists()


def _iter_pending_runtime_status_requests(request_root: Path) -> list[Path]:
    return sorted(request_root.glob("*/HostChildRuntimeStatusRequest.json"))


def _runtime_status_response_exists(request_path: Path) -> bool:
    return (request_path.parent / "HostChildRuntimeStatusResponse.json").exists()


def _reap_request_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def _request_dir_age_s(path: Path, *, now_s: float) -> float:
    try:
        stat = path.stat()
    except OSError:
        return 0.0
    return max(0.0, now_s - float(stat.st_mtime))


def _reap_completed_request_dirs(
    *,
    request_root: Path,
    response_name: str,
    retention_s: float,
    now_s: float | None = None,
) -> int:
    now_value = time.time() if now_s is None else float(now_s)
    required_age_s = max(0.0, float(retention_s))
    reaped = 0
    for request_dir in sorted(request_root.glob("*")):
        if not request_dir.is_dir():
            continue
        response_ref = request_dir / response_name
        if not response_ref.exists():
            continue
        if _request_dir_age_s(request_dir, now_s=now_value) < required_age_s:
            continue
        _reap_request_dir(request_dir)
        if not request_dir.exists():
            reaped += 1
    return reaped


def reap_completed_launch_request_dirs(*, repo_root: Path, retention_s: float = _COMPLETED_REQUEST_RETENTION_S) -> int:
    return _reap_completed_request_dirs(
        request_root=host_child_launch_request_root(repo_root=repo_root),
        response_name="HostChildLaunchResponse.json",
        retention_s=retention_s,
    )


def reap_completed_runtime_status_request_dirs(
    *,
    repo_root: Path,
    retention_s: float = _COMPLETED_REQUEST_RETENTION_S,
) -> int:
    return _reap_completed_request_dirs(
        request_root=host_child_runtime_status_request_root(repo_root=repo_root),
        response_name="HostChildRuntimeStatusResponse.json",
        retention_s=retention_s,
    )


def process_pending_requests(*, repo_root: Path) -> int:
    request_root = host_child_launch_request_root(repo_root=repo_root)
    processed = 0
    for request_path in _iter_pending_requests(request_root):
        if _response_exists(request_path):
            continue
        try:
            request = json.loads(request_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            _reap_request_dir(request_path.parent)
            continue
        response_ref = Path(str(request.get("response_ref") or request_path.parent / "HostChildLaunchResponse.json")).expanduser().resolve()
        launch_result_ref = ""
        try:
            with force_direct_child_launch_mode():
                child_result = launch_child_from_result_ref(
                    result_ref=str(request["source_result_ref"]),
                    startup_probe_ms=int(request.get("startup_probe_ms", 1500)),
                    startup_health_timeout_ms=int(request.get("startup_health_timeout_ms", 12000)),
                )
            launch_result_ref = str(child_result.get("launch_result_ref") or "").strip()
            if not launch_result_ref:
                raise ValueError("host child launch bridge did not return launch_result_ref")
            ensure_child_supervision_running(launch_result_ref=launch_result_ref)
            payload = {
                "schema": "loop_product.host_child_launch_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "completed",
                "child_launch_result": child_result,
            }
        except Exception as exc:
            if launch_result_ref:
                try:
                    terminate_runtime_owned_launch_result_ref(result_ref=launch_result_ref)
                except Exception:
                    pass
            payload = {
                "schema": "loop_product.host_child_launch_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "failed",
                "error": str(exc),
            }
        _write_json(response_ref, payload)
        processed += 1
    return processed


def process_pending_runtime_status_requests(*, repo_root: Path) -> int:
    request_root = host_child_runtime_status_request_root(repo_root=repo_root)
    processed = 0
    for request_path in _iter_pending_runtime_status_requests(request_root):
        if _runtime_status_response_exists(request_path):
            continue
        try:
            request = json.loads(request_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            _reap_request_dir(request_path.parent)
            continue
        response_ref = Path(str(request.get("response_ref") or request_path.parent / "HostChildRuntimeStatusResponse.json")).expanduser().resolve()
        try:
            with force_direct_child_runtime_status_mode():
                status_result = runtime_lifecycle.child_runtime_status_from_launch_result_ref(
                    result_ref=str(request["launch_result_ref"]),
                    stall_threshold_s=float(request.get("stall_threshold_s", 60.0)),
                )
            payload = {
                "schema": "loop_product.host_child_runtime_status_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "completed",
                "child_runtime_status_result": status_result,
            }
        except Exception as exc:
            payload = {
                "schema": "loop_product.host_child_runtime_status_response",
                "request_id": str(request.get("request_id") or ""),
                "status": "failed",
                "error": str(exc),
            }
        _write_json(response_ref, payload)
        processed += 1
    return processed


@contextmanager
def _runtime_marker(
    *,
    repo_root: Path,
    poll_interval_s: float,
    idle_exit_after_s: float,
    watch_repo_control_plane: bool,
    lease_epoch: int,
    started_at_utc: str,
) -> Iterator[Path]:
    runtime_ref = supervisor_runtime_ref(repo_root=repo_root)
    _write_json(
        runtime_ref,
        {
            "schema": "loop_product.host_child_launch_supervisor_runtime",
            "repo_root": str(repo_root),
            "pid": os.getpid(),
            "code_fingerprint": _runtime_code_fingerprint(),
            "poll_interval_s": float(poll_interval_s),
            "idle_exit_after_s": float(idle_exit_after_s),
            "watch_repo_control_plane": bool(watch_repo_control_plane),
            "lease_epoch": int(lease_epoch),
            "started_at_utc": str(started_at_utc),
            "log_ref": str(_log_ref_for_repo(repo_root=repo_root)),
        },
    )
    try:
        yield runtime_ref
    finally:
        try:
            current = _load_runtime_payload(runtime_ref)
        except Exception:
            current = {}
        if int(dict(current or {}).get("pid") or 0) == os.getpid():
            runtime_ref.unlink(missing_ok=True)


def _ensure_repo_control_plane_watchdog(*, repo_root: Path, poll_interval_s: float) -> dict[str, Any] | None:
    from loop_product.runtime.control_plane import ensure_repo_control_plane_running, live_repo_control_plane_runtime

    payload = live_repo_control_plane_runtime(repo_root=repo_root)
    if payload is not None:
        return payload
    return ensure_repo_control_plane_running(
        repo_root=repo_root,
        poll_interval_s=max(0.05, float(poll_interval_s)),
        trigger_kind="host_supervisor_watchdog",
        trigger_ref=str(supervisor_runtime_ref(repo_root=repo_root)),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Process queued host child launch requests.")
    parser.add_argument("--repo-root", required=True, help="Absolute path to the loop_product_repo root")
    parser.add_argument("--once", action="store_true", help="Process pending requests once and exit")
    parser.add_argument("--poll-interval-s", type=float, default=_DEFAULT_POLL_INTERVAL_S, help="Polling interval when not using --once")
    parser.add_argument(
        "--idle-exit-after-s",
        type=float,
        default=_DEFAULT_IDLE_EXIT_AFTER_S,
        help="Exit after this much idle time with no pending requests; use 0 to disable idle exit",
    )
    parser.add_argument(
        "--watch-repo-control-plane",
        action="store_true",
        help="When enabled, restart the repo control-plane if it disappears while this supervisor remains live",
    )
    args = parser.parse_args(argv)

    repo_root = Path(args.repo_root).expanduser().resolve()
    poll_interval_s = max(0.05, float(args.poll_interval_s))
    idle_exit_after_s = max(0.0, float(args.idle_exit_after_s))
    watch_repo_control_plane = bool(args.watch_repo_control_plane)
    lease_epoch = _host_supervisor_next_lease_epoch(repo_root=repo_root)
    started_at_utc = _now_iso()

    with _runtime_marker(
        repo_root=repo_root,
        poll_interval_s=poll_interval_s,
        idle_exit_after_s=idle_exit_after_s,
        watch_repo_control_plane=watch_repo_control_plane,
        lease_epoch=lease_epoch,
        started_at_utc=started_at_utc,
    ):
        startup_outcome = _emit_host_supervisor_authority(
            repo_root=repo_root,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=0,
            poll_interval_s=poll_interval_s,
            idle_exit_after_s=idle_exit_after_s,
            watch_repo_control_plane=watch_repo_control_plane,
            process_started_at_utc=started_at_utc,
        )
        if _host_supervisor_should_self_fence(startup_outcome):
            _emit_host_supervisor_exit_authority(
                repo_root=repo_root,
                lease_epoch=lease_epoch,
                poll_interval_s=poll_interval_s,
                idle_exit_after_s=idle_exit_after_s,
                watch_repo_control_plane=watch_repo_control_plane,
                process_started_at_utc=started_at_utc,
                exit_reason="lease_fenced",
            )
            return 0
        if args.once:
            process_pending_requests(repo_root=repo_root)
            process_pending_runtime_status_requests(repo_root=repo_root)
            return 0

        last_activity = time.monotonic()
        heartbeat_ordinal = 0
        while True:
            cleanup_authority = _host_supervisor_cleanup_authority(repo_root=repo_root)
            if cleanup_authority is not None:
                _emit_host_supervisor_exit_authority(
                    repo_root=repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_s,
                    idle_exit_after_s=idle_exit_after_s,
                    watch_repo_control_plane=watch_repo_control_plane,
                    process_started_at_utc=started_at_utc,
                    exit_reason="cleanup_committed",
                    cleanup_authority=cleanup_authority,
                )
                return 0
            if watch_repo_control_plane:
                _ensure_repo_control_plane_watchdog(repo_root=repo_root, poll_interval_s=poll_interval_s)
            processed = process_pending_requests(repo_root=repo_root)
            processed += process_pending_runtime_status_requests(repo_root=repo_root)
            reaped = reap_completed_launch_request_dirs(repo_root=repo_root)
            reaped += reap_completed_runtime_status_request_dirs(repo_root=repo_root)
            if processed > 0:
                last_activity = time.monotonic()
            elif reaped > 0:
                last_activity = time.monotonic()
            elif idle_exit_after_s > 0.0 and (time.monotonic() - last_activity) >= idle_exit_after_s:
                _emit_host_supervisor_exit_authority(
                    repo_root=repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_s,
                    idle_exit_after_s=idle_exit_after_s,
                    watch_repo_control_plane=watch_repo_control_plane,
                    process_started_at_utc=started_at_utc,
                    exit_reason="idle_exit",
                )
                return 0
            heartbeat_ordinal += 1
            heartbeat_outcome = _emit_host_supervisor_authority(
                repo_root=repo_root,
                lease_epoch=lease_epoch,
                heartbeat_ordinal=heartbeat_ordinal,
                poll_interval_s=poll_interval_s,
                idle_exit_after_s=idle_exit_after_s,
                watch_repo_control_plane=watch_repo_control_plane,
                process_started_at_utc=started_at_utc,
            )
            if _host_supervisor_should_self_fence(heartbeat_outcome):
                _emit_host_supervisor_exit_authority(
                    repo_root=repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_s,
                    idle_exit_after_s=idle_exit_after_s,
                    watch_repo_control_plane=watch_repo_control_plane,
                    process_started_at_utc=started_at_utc,
                    exit_reason="lease_fenced",
                )
                return 0
            time.sleep(poll_interval_s)


if __name__ == "__main__":
    raise SystemExit(main())
