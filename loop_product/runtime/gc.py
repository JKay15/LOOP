"""Safe GC for stopped runtime runs."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from loop_product.state_io import write_json_read_only
from loop_product.runtime.cleanup_authority import runtime_cleanup_retirement_authority
from loop_product.runtime_identity import runtime_root_identity_token
from loop_product.runtime_paths import require_runtime_root


_BLOCKING_NODE_STATUSES = {"ACTIVE", "PLANNED"}
_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S = 0.5
_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_IDLE_EXIT_AFTER_S = 30.0
_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_SERVICE_IDLE_EXIT_AFTER_S = 0.0
_HOUSEKEEPING_REAP_CONTROLLER_STARTUP_TIMEOUT_S = 10.0
HOUSEKEEPING_REAP_CONTROLLER_NODE_ID = "housekeeping_reap_controller"


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


def _terminate_pid(pid: int, *, grace_s: float = 0.75) -> None:
    if not _pid_alive(pid):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    deadline = time.monotonic() + max(0.0, float(grace_s))
    while time.monotonic() < deadline:
        if not _pid_alive(pid):
            return
        time.sleep(0.05)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        return


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_json_read_only(path: Path, payload: dict[str, Any]) -> None:
    write_json_read_only(path, payload)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _repo_root(repo_root: str | Path) -> Path:
    return Path(repo_root).expanduser().resolve()


def _state_root(state_root: str | Path) -> Path:
    return Path(state_root).expanduser().resolve()


def housekeeping_runtime_root(*, repo_root: str | Path) -> Path:
    return (_repo_root(repo_root) / ".loop" / "housekeeping").resolve()


def housekeeping_reap_controller_runtime_ref(*, repo_root: str | Path) -> Path:
    return housekeeping_runtime_root(repo_root=repo_root) / "HousekeepingReapControllerRuntime.json"


def housekeeping_reap_controller_log_ref(*, repo_root: str | Path) -> Path:
    return housekeeping_runtime_root(repo_root=repo_root) / "HousekeepingReapController.log"


def _top_level_repo_root_from_runtime_root(*, state_root: str | Path) -> Path | None:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    if resolved_state_root.name == ".loop":
        return resolved_state_root.parent.resolve()
    if resolved_state_root.name == housekeeping_runtime_root(repo_root=resolved_state_root.parent.parent).name:
        return None
    parent = resolved_state_root.parent
    if parent.name != ".loop":
        return None
    return parent.parent.resolve()


def _housekeeping_reap_controller_code_fingerprint() -> str:
    refs = [
        Path(__file__).resolve(),
        Path(__file__).resolve().parents[1] / "event_journal" / "store.py",
    ]
    digest = hashlib.sha256()
    for ref in refs:
        resolved = ref.resolve()
        digest.update(str(resolved).encode("utf-8"))
        digest.update(b"\0")
        digest.update(resolved.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _workspace_root_for_state_root(*, repo_root: Path, state_root: Path) -> Path:
    top_level_state_root = (repo_root / ".loop").resolve()
    if state_root.resolve() == top_level_state_root:
        return (repo_root / "workspace").resolve()
    return (repo_root / "workspace" / state_root.name).resolve()


def _iter_node_state_paths(state_root: Path) -> list[Path]:
    state_dir = state_root / "state"
    if not state_dir.exists():
        return []
    result: list[Path] = []
    for path in sorted(state_dir.glob("*.json")):
        if path.name in {"accepted_envelopes.json", "kernel_state.json", "root-kernel.json"}:
            continue
        result.append(path)
    return result


def _load_node_states(state_root: Path) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for path in _iter_node_state_paths(state_root):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        payloads.append(payload)
    return payloads


def _iter_launch_result_paths(state_root: Path) -> list[Path]:
    launches_root = state_root / "artifacts" / "launches"
    if not launches_root.exists():
        return []
    return sorted(launches_root.glob("**/ChildLaunchResult.json"))


def _live_recorded_launches(state_root: Path) -> list[dict[str, Any]]:
    live: list[dict[str, Any]] = []
    for path in _iter_launch_result_paths(state_root):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        pid = int(payload.get("pid") or 0)
        if _pid_alive(pid):
            live.append(
                {
                    "node_id": str(payload.get("node_id") or ""),
                    "pid": pid,
                    "launch_result_ref": str(path.resolve()),
                }
            )
    return live


def _parent_pid(pid: int) -> int:
    try:
        proc = subprocess.run(
            ["ps", "-o", "ppid=", "-p", str(int(pid))],
            check=True,
            text=True,
            capture_output=True,
        )
    except Exception:
        return 0
    raw = proc.stdout.strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0


def _excluded_pids() -> set[int]:
    excluded: set[int] = set()
    current = os.getpid()
    while current > 1 and current not in excluded:
        excluded.add(current)
        current = _parent_pid(current)
    return excluded


def _load_runtime_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _housekeeping_runtime_marker_matches_process(payload: dict[str, Any]) -> bool:
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    repo_root = str(payload.get("repo_root") or "").strip()
    return _housekeeping_controller_process_matches(pid=pid, repo_root=Path(repo_root))


def _housekeeping_controller_process_matches(*, pid: int, repo_root: Path) -> bool:
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    return (
        "loop_product.runtime.gc" in command_line
        and "--run-housekeeping-controller" in command_line
        and _command_repo_root_matches(command_line, repo_root)
    )


def _housekeeping_controller_runtime_settings_from_command(*, pid: int) -> dict[str, float]:
    command_line = _pid_command_line(pid)
    poll_interval_s = float(_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S)
    idle_exit_after_s = float(_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_IDLE_EXIT_AFTER_S)
    watch_repo_control_plane = False
    if not command_line:
        return {
            "poll_interval_s": poll_interval_s,
            "idle_exit_after_s": idle_exit_after_s,
            "watch_repo_control_plane": watch_repo_control_plane,
        }
    try:
        argv = shlex.split(command_line)
    except Exception:
        argv = []
    for index, token in enumerate(argv):
        if token == "--poll-interval-s" and index + 1 < len(argv):
            try:
                poll_interval_s = float(argv[index + 1])
            except (TypeError, ValueError):
                pass
        if token == "--idle-exit-after-s" and index + 1 < len(argv):
            try:
                idle_exit_after_s = float(argv[index + 1])
            except (TypeError, ValueError):
                pass
        if token == "--watch-repo-control-plane":
            watch_repo_control_plane = True
    return {
        "poll_interval_s": poll_interval_s,
        "idle_exit_after_s": idle_exit_after_s,
        "watch_repo_control_plane": watch_repo_control_plane,
    }


def _housekeeping_controller_authority_payload(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    housekeeping_root = housekeeping_runtime_root(repo_root=repo_root)
    effective = dict(
        dict(
            query_runtime_liveness_view(housekeeping_root, initialize_if_missing=False).get(
                "effective_runtime_liveness_by_node"
            )
            or {}
        ).get(
            HOUSEKEEPING_REAP_CONTROLLER_NODE_ID
        )
        or {}
    )
    if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
        return None
    payload = dict(effective.get("payload") or {})
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return None
    if not _housekeeping_controller_process_matches(pid=pid, repo_root=repo_root):
        return None
    current_process_fingerprint = _housekeeping_controller_process_fingerprint(pid=pid)
    payload_process_fingerprint = str(payload.get("process_fingerprint") or "")
    if payload_process_fingerprint and current_process_fingerprint and payload_process_fingerprint != current_process_fingerprint:
        return None
    current_code_fingerprint = _housekeeping_reap_controller_code_fingerprint()
    payload_code_fingerprint = str(payload.get("code_fingerprint") or "")
    if payload_code_fingerprint and payload_code_fingerprint != current_code_fingerprint:
        return None
    launch_identity = str(payload.get("launch_event_id") or payload.get("launch_result_ref") or "").strip()
    expected_launch_identity = _housekeeping_controller_launch_identity(repo_root=repo_root)
    if launch_identity and launch_identity != expected_launch_identity:
        return None
    return {
        **payload,
        "pid": pid,
        "process_fingerprint": current_process_fingerprint or payload_process_fingerprint,
        "code_fingerprint": payload_code_fingerprint or current_code_fingerprint,
    }


def _materialize_housekeeping_runtime_marker_from_authority(
    *,
    repo_root: Path,
    authority_payload: dict[str, Any],
) -> dict[str, Any]:
    runtime_ref = housekeeping_reap_controller_runtime_ref(repo_root=repo_root)
    log_ref = housekeeping_reap_controller_log_ref(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    pid = int(authority_payload.get("pid") or 0)
    settings = _housekeeping_controller_runtime_settings_from_command(pid=pid)
    marker_payload = {
        "schema": "loop_product.housekeeping_reap_controller_runtime",
        "runtime_kind": "housekeeping_reap_controller",
        "repo_root": str(repo_root),
        "housekeeping_state_root": str(housekeeping_runtime_root(repo_root=repo_root)),
        "pid": pid,
        "code_fingerprint": str(authority_payload.get("code_fingerprint") or _housekeeping_reap_controller_code_fingerprint()),
        "poll_interval_s": float(authority_payload.get("poll_interval_s") or settings["poll_interval_s"]),
        "idle_exit_after_s": float(authority_payload.get("idle_exit_after_s") or settings["idle_exit_after_s"]),
        "watch_repo_control_plane": bool(
            authority_payload.get("watch_repo_control_plane", settings["watch_repo_control_plane"])
        ),
        "lease_epoch": int(authority_payload.get("lease_epoch") or 0),
        "started_at_utc": str(
            authority_payload.get("process_started_at_utc")
            or authority_payload.get("started_at_utc")
            or authority_payload.get("observed_at")
            or _now_iso()
        ),
        "log_ref": str(log_ref),
    }
    _write_json_read_only(runtime_ref, marker_payload)
    return marker_payload


def live_housekeeping_reap_controller_runtime(*, repo_root: str | Path) -> dict[str, Any] | None:
    resolved_repo_root = _repo_root(repo_root)
    runtime_ref = housekeeping_reap_controller_runtime_ref(repo_root=resolved_repo_root)
    if not runtime_ref.exists():
        authority_payload = _housekeeping_controller_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_housekeeping_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        authority_payload = _housekeeping_controller_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_housekeeping_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    if not _housekeeping_runtime_marker_matches_process(payload):
        runtime_ref.unlink(missing_ok=True)
        authority_payload = _housekeeping_controller_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_housekeeping_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    return payload


@contextmanager
def _housekeeping_reap_controller_runtime_marker(
    *,
    repo_root: Path,
    poll_interval_s: float,
    idle_exit_after_s: float,
    watch_repo_control_plane: bool,
    lease_epoch: int,
    started_at_utc: str,
) -> Any:
    runtime_ref = housekeeping_reap_controller_runtime_ref(repo_root=repo_root)
    log_ref = housekeeping_reap_controller_log_ref(repo_root=repo_root)
    _write_json_read_only(
        runtime_ref,
        {
            "schema": "loop_product.housekeeping_reap_controller_runtime",
            "runtime_kind": "housekeeping_reap_controller",
            "repo_root": str(repo_root),
            "housekeeping_state_root": str(housekeeping_runtime_root(repo_root=repo_root)),
            "pid": os.getpid(),
            "code_fingerprint": _housekeeping_reap_controller_code_fingerprint(),
            "poll_interval_s": float(poll_interval_s),
            "idle_exit_after_s": float(idle_exit_after_s),
            "watch_repo_control_plane": bool(watch_repo_control_plane),
            "lease_epoch": int(lease_epoch),
            "started_at_utc": str(started_at_utc),
            "log_ref": str(log_ref),
        },
    )
    try:
        yield runtime_ref
    finally:
        try:
            current = _load_runtime_payload(runtime_ref)
        except Exception:
            current = {}
        if (
            int(dict(current or {}).get("pid") or 0) == os.getpid()
            and int(dict(current or {}).get("lease_epoch") or 0) == int(lease_epoch)
        ):
            runtime_ref.unlink(missing_ok=True)


def _housekeeping_result_signature(result: dict[str, Any]) -> str:
    return json.dumps(
        {
            "runtime_name": str(result.get("runtime_name") or ""),
            "outcome": str(result.get("outcome") or ""),
            "request_event_id": str(result.get("request_event_id") or ""),
            "reap_deferred_event_id": str(result.get("reap_deferred_event_id") or ""),
            "process_reap_confirmed_event_id": str(result.get("process_reap_confirmed_event_id") or ""),
            "runtime_root_quiesced_event_id": str(result.get("runtime_root_quiesced_event_id") or ""),
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _housekeeping_controller_next_lease_epoch(*, repo_root: Path) -> int:
    from loop_product.event_journal import iter_committed_events

    latest_epoch = 0
    housekeeping_root = housekeeping_runtime_root(repo_root=repo_root)
    for event in iter_committed_events(housekeeping_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        if str(event.get("node_id") or "") != HOUSEKEEPING_REAP_CONTROLLER_NODE_ID:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("attachment_state") or "").strip().upper() != "ATTACHED":
            continue
        try:
            latest_epoch = max(latest_epoch, int(payload.get("lease_epoch") or 0))
        except (TypeError, ValueError):
            continue
    return latest_epoch + 1 if latest_epoch > 0 else 1


def _housekeeping_controller_process_fingerprint(*, pid: int) -> str:
    command_line = _pid_command_line(pid)
    if not command_line:
        return ""
    digest = hashlib.sha256()
    digest.update(command_line.encode("utf-8"))
    return digest.hexdigest()


def _housekeeping_controller_launch_identity(*, repo_root: Path) -> str:
    return str(housekeeping_reap_controller_runtime_ref(repo_root=repo_root))


def _housekeeping_controller_liveness_observation_id(
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
    return f"housekeeping_controller_liveness::{digest}"


def _housekeeping_controller_identity_observation_id(
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
    return f"housekeeping_controller_identity::{digest}"


def _housekeeping_controller_exit_observation_id(
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
    return f"housekeeping_controller_exit::{digest}"


def _housekeeping_controller_process_exit_observation_id(
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
    return f"housekeeping_controller_process_exit::{digest}"


def _emit_housekeeping_controller_authority(
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
    process_fingerprint = _housekeeping_controller_process_fingerprint(pid=pid)
    launch_identity = _housekeeping_controller_launch_identity(repo_root=repo_root)
    runtime_ref = housekeeping_reap_controller_runtime_ref(repo_root=repo_root)
    payload = {
        "controller_kind": "housekeeping_reap_controller",
        "attachment_state": "ATTACHED",
        "observed_at": observed_at,
        "lease_owner_id": f"housekeeping-controller:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "lease_duration_s": max(5.0, float(poll_interval_s) * 3.0),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _housekeeping_reap_controller_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "watch_repo_control_plane": bool(watch_repo_control_plane),
        "launch_result_ref": launch_identity,
        "launch_event_id": launch_identity,
        "process_started_at_utc": str(process_started_at_utc),
        "runtime_ref": str(runtime_ref),
    }
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        housekeeping_runtime_root(repo_root=repo_root),
        observation_id=_housekeeping_controller_liveness_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
        ),
        node_id=HOUSEKEEPING_REAP_CONTROLLER_NODE_ID,
        payload=payload,
        producer="runtime.gc.run_housekeeping_reap_controller_loop",
    )
    if heartbeat_ordinal == 0:
        commit_process_identity_confirmed_event(
            housekeeping_runtime_root(repo_root=repo_root),
            observation_id=_housekeeping_controller_identity_observation_id(
                repo_root=repo_root,
                pid=pid,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
                lease_epoch=lease_epoch,
                process_started_at_utc=process_started_at_utc,
            ),
            node_id=HOUSEKEEPING_REAP_CONTROLLER_NODE_ID,
            payload={
                "controller_kind": "housekeeping_reap_controller",
                "observed_at": observed_at,
                "pid": int(pid),
                "process_fingerprint": process_fingerprint,
                "code_fingerprint": _housekeeping_reap_controller_code_fingerprint(),
                "poll_interval_s": float(poll_interval_s),
                "idle_exit_after_s": float(idle_exit_after_s),
                "watch_repo_control_plane": bool(watch_repo_control_plane),
                "launch_result_ref": launch_identity,
                "launch_event_id": launch_identity,
                "process_started_at_utc": str(process_started_at_utc),
                "lease_epoch": int(lease_epoch),
                "lease_owner_id": f"housekeeping-controller:{runtime_ref}",
                "runtime_ref": str(runtime_ref),
            },
            producer="runtime.gc.run_housekeeping_reap_controller_loop",
        )
    return dict(liveness_outcome or {})


def _emit_housekeeping_controller_exit_authority(
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
    process_fingerprint = _housekeeping_controller_process_fingerprint(pid=pid)
    launch_identity = _housekeeping_controller_launch_identity(repo_root=repo_root)
    runtime_ref = housekeeping_reap_controller_runtime_ref(repo_root=repo_root)
    payload = {
        "controller_kind": "housekeeping_reap_controller",
        "attachment_state": "TERMINAL",
        "observation_kind": f"housekeeping_controller_exit:{str(exit_reason)}",
        "exit_reason": str(exit_reason),
        "observed_at": observed_at,
        "lease_owner_id": f"housekeeping-controller:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _housekeeping_reap_controller_code_fingerprint(),
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
        housekeeping_runtime_root(repo_root=repo_root),
        observation_id=_housekeeping_controller_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=HOUSEKEEPING_REAP_CONTROLLER_NODE_ID,
        payload=payload,
        producer="runtime.gc.run_housekeeping_reap_controller_loop",
    )
    process_exit_event = commit_process_exit_observed_event(
        housekeeping_runtime_root(repo_root=repo_root),
        observation_id=_housekeeping_controller_process_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=HOUSEKEEPING_REAP_CONTROLLER_NODE_ID,
        payload=payload,
        producer="runtime.gc.run_housekeeping_reap_controller_loop",
    )
    return {
        "liveness_outcome": dict(liveness_outcome or {}),
        "process_exit_event": dict(process_exit_event or {}),
    }


def _housekeeping_controller_should_self_fence(liveness_outcome: dict[str, Any] | None) -> bool:
    outcome = str(dict(liveness_outcome or {}).get("outcome") or "").strip()
    return outcome.startswith("rejected_")


def _ensure_repo_control_plane_watchdog(*, repo_root: Path, poll_interval_s: float) -> dict[str, Any] | None:
    from loop_product.runtime.control_plane import ensure_repo_control_plane_running
    from loop_product.runtime.control_plane import live_repo_control_plane_runtime
    from loop_product import host_child_launch_supervisor as supervisor_module

    payload = live_repo_control_plane_runtime(repo_root=repo_root)
    if payload is not None:
        return payload
    if supervisor_module.live_supervisor_runtime(repo_root=repo_root) is not None:
        return None
    return ensure_repo_control_plane_running(
        repo_root=repo_root,
        poll_interval_s=max(0.05, float(poll_interval_s)),
        trigger_kind="housekeeping_watchdog",
        trigger_ref=str(housekeeping_reap_controller_runtime_ref(repo_root=repo_root)),
    )


def _housekeeping_controller_cleanup_authority(*, repo_root: Path) -> dict[str, Any] | None:
    return runtime_cleanup_retirement_authority(
        state_root=housekeeping_runtime_root(repo_root=repo_root),
        repo_root=repo_root,
    )


def _owned_processes(*, state_root: Path, workspace_root: Path) -> list[dict[str, Any]]:
    markers = [str(state_root), str(workspace_root)]
    excluded = _excluded_pids()
    try:
        proc = subprocess.run(
            ["ps", "-ax", "-o", "pid=,command="],
            check=True,
            text=True,
            capture_output=True,
        )
    except Exception:
        return []
    owned: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        pid_text, _, command = raw.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid in excluded:
            continue
        if any(marker and marker in command for marker in markers):
            owned.append({"pid": pid, "command": command})
    return owned


def stopped_run_gc_report(*, state_root: str | Path, repo_root: str | Path) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    resolved_state_root = _state_root(state_root)
    resolved_workspace_root = _workspace_root_for_state_root(repo_root=resolved_repo_root, state_root=resolved_state_root)
    node_payloads = _load_node_states(resolved_state_root)
    blocking_nodes = [
        {
            "node_id": str(payload.get("node_id") or ""),
            "status": str(payload.get("status") or ""),
        }
        for payload in node_payloads
        if str(payload.get("status") or "").upper() in _BLOCKING_NODE_STATUSES
    ]
    live_launches = _live_recorded_launches(resolved_state_root)
    owned = _owned_processes(state_root=resolved_state_root, workspace_root=resolved_workspace_root)
    blocking_reasons: list[str] = []
    if not node_payloads:
        blocking_reasons.append("missing_node_state")
    if blocking_nodes:
        blocking_reasons.append("active_or_planned_nodes_present")
    if live_launches:
        blocking_reasons.append("live_recorded_launch_pids_present")
    return {
        "runtime_name": resolved_state_root.name,
        "repo_root": str(resolved_repo_root),
        "state_root": str(resolved_state_root),
        "workspace_root": str(resolved_workspace_root),
        "runtime_root_identity_token": runtime_root_identity_token(resolved_state_root),
        "reapable": not blocking_reasons,
        "blocking_reasons": blocking_reasons,
        "blocking_nodes": blocking_nodes,
        "live_launches": live_launches,
        "owned_processes": owned,
    }


def _receipt_ref(*, repo_root: Path, runtime_name: str) -> Path:
    return (repo_root / ".loop" / "quarantine" / "reaped_runs" / f"{runtime_name}.json").resolve()


def _reap_request_id(runtime_name: str) -> str:
    return f"{runtime_name}:reap-requested"


def _reap_confirmed_id(runtime_name: str) -> str:
    return f"{runtime_name}:reap-confirmed"


def _reap_deferred_id(runtime_name: str, *, blocking_reasons: list[str]) -> str:
    digest = hashlib.sha256(
        json.dumps(sorted(str(item) for item in blocking_reasons), separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()[:12]
    return f"{runtime_name}:reap-deferred:{digest}"


def _cleanup_nonce(
    runtime_name: str,
    *,
    state_root: Path,
    workspace_root: Path,
    live_launches: list[dict[str, Any]],
    owned_processes: list[dict[str, Any]],
) -> str:
    payload = {
        "runtime_name": runtime_name,
        "state_root": str(state_root),
        "workspace_root": str(workspace_root),
        "live_launch_pids": sorted(int(item.get("pid") or 0) for item in live_launches),
        "owned_processes": [
            {
                "pid": int(item.get("pid") or 0),
                "command": str(item.get("command") or ""),
            }
            for item in owned_processes
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:12]


def _test_cleanup_id(runtime_name: str, *, cleanup_nonce: str) -> str:
    return f"{runtime_name}:test-runtime-cleanup-committed:{cleanup_nonce}"


def _runtime_quiesced_id(runtime_name: str, *, cleanup_nonce: str) -> str:
    return f"{runtime_name}:runtime-root-quiesced:{cleanup_nonce}"


def _orphan_detected_id(runtime_name: str, pid: int, *, ordinal: int, cleanup_nonce: str) -> str:
    normalized_pid = int(pid) if int(pid) > 0 else 0
    return f"{runtime_name}:process-orphan-detected:{cleanup_nonce}:{normalized_pid or ordinal}"


def _runtime_name_from_payload(payload: dict[str, Any]) -> str:
    return str(payload.get("runtime_name") or "").strip()


def _request_payload_from_report(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "runtime_name": str(report.get("runtime_name") or ""),
        "state_root": str(report.get("state_root") or ""),
        "workspace_root": str(report.get("workspace_root") or ""),
        "runtime_root_identity_token": str(report.get("runtime_root_identity_token") or ""),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
        "blocking_nodes": [dict(item) for item in list(report.get("blocking_nodes") or [])],
        "live_launches": [dict(item) for item in list(report.get("live_launches") or [])],
        "owned_processes": [dict(item) for item in list(report.get("owned_processes") or [])],
    }


def _iter_latest_housekeeping_reap_heads(
    housekeeping_root: Path, *, runtime_name: str = ""
) -> dict[str, dict[str, dict[str, Any]]]:
    from loop_product.event_journal import iter_committed_events

    heads: dict[str, dict[str, dict[str, Any]]] = {}
    for event in iter_committed_events(housekeeping_root):
        event_type = str(event.get("event_type") or "")
        if event_type not in {
            "reap_requested",
            "reap_deferred",
            "process_reap_confirmed",
            "runtime_reap_requested",
            "runtime_reap_deferred",
            "runtime_reap_confirmed",
            "runtime_root_quiesced",
        }:
            continue
        payload = dict(event.get("payload") or {})
        candidate_runtime = _runtime_name_from_payload(payload) or str(event.get("node_id") or "").strip()
        if not candidate_runtime:
            continue
        if runtime_name and candidate_runtime != runtime_name:
            continue
        heads.setdefault(candidate_runtime, {})[event_type] = dict(event)
    return heads


def _build_receipt(
    *,
    runtime_name: str,
    repo_root: Path,
    housekeeping_root: Path,
    state_root_path: Path,
    workspace_root_path: Path,
    owned_processes: list[dict[str, Any]],
    canonical_request_event: dict[str, Any],
    canonical_confirmed_event: dict[str, Any],
    orphan_event_ids: list[str],
    quiesced_event: dict[str, Any],
    request_event: dict[str, Any],
    confirmed_event: dict[str, Any],
    execution_owner: str,
    reconciler_outcome: str,
    controller_runner: str = "",
) -> dict[str, Any]:
    receipt_ref = _receipt_ref(repo_root=repo_root, runtime_name=runtime_name)
    receipt = {
        "schema": "loop_product.stopped_run_gc_receipt",
        "runtime_name": runtime_name,
        "state_root": str(state_root_path),
        "workspace_root": str(workspace_root_path),
        "owned_processes_terminated": [dict(item) for item in owned_processes],
        "reaped_at_unix_s": time.time(),
        "status": "reaped",
        "housekeeping_state_root": str(housekeeping_root),
        "canonical_reap_requested_event_id": str(canonical_request_event.get("event_id") or ""),
        "canonical_process_reap_confirmed_event_id": str(canonical_confirmed_event.get("event_id") or ""),
        "canonical_process_orphan_detected_event_ids": orphan_event_ids,
        "canonical_runtime_root_quiesced_event_id": str(quiesced_event.get("event_id") or ""),
        "reap_requested_event_id": str(request_event.get("event_id") or ""),
        "reap_confirmed_event_id": str(confirmed_event.get("event_id") or ""),
        "execution_owner": execution_owner,
        "reconciler_outcome": reconciler_outcome,
    }
    if controller_runner:
        receipt["controller_runner"] = controller_runner
    _write_json(receipt_ref, receipt)
    receipt["receipt_ref"] = str(receipt_ref)
    return receipt


def reconcile_housekeeping_reap_requests(
    *,
    repo_root: str | Path,
    runtime_name: str = "",
    grace_s: float = 0.75,
    controller_runner: str = "",
) -> list[dict[str, Any]]:
    from loop_product.event_journal import (
        commit_process_orphan_detected_event,
        commit_process_reap_confirmed_event,
        commit_reap_deferred_event,
        commit_runtime_reap_confirmed_event,
        commit_runtime_reap_deferred_event,
        commit_runtime_root_quiesced_event,
    )

    resolved_repo_root = _repo_root(repo_root)
    housekeeping_root = housekeeping_runtime_root(repo_root=resolved_repo_root)
    heads = _iter_latest_housekeeping_reap_heads(housekeeping_root, runtime_name=runtime_name)
    reconciled: list[dict[str, Any]] = []
    for candidate_runtime_name in sorted(heads):
        event_heads = dict(heads.get(candidate_runtime_name) or {})
        request_event = dict(event_heads.get("reap_requested") or {})
        if not request_event:
            continue
        request_seq = int(request_event.get("seq") or 0)
        confirmed_event = dict(event_heads.get("process_reap_confirmed") or {})
        quiesced_event = dict(event_heads.get("runtime_root_quiesced") or {})
        if int(confirmed_event.get("seq") or 0) > request_seq and int(quiesced_event.get("seq") or 0) > request_seq:
            reconciled.append(
                {
                    "runtime_name": candidate_runtime_name,
                    "execution_owner": "reap_reconciler",
                    "outcome": "already_confirmed",
                    "request_event_id": str(request_event.get("event_id") or ""),
                    "process_reap_confirmed_event_id": str(confirmed_event.get("event_id") or ""),
                    "runtime_root_quiesced_event_id": str(quiesced_event.get("event_id") or ""),
                    **({"controller_runner": controller_runner} if controller_runner else {}),
                }
            )
            continue

        request_payload = dict(request_event.get("payload") or {})
        runtime_identity_token_value = str(request_payload.get("runtime_root_identity_token") or "")
        state_root_path = _state_root(str(request_payload.get("state_root") or ""))
        workspace_root_path = _workspace_root_for_state_root(
            repo_root=resolved_repo_root,
            state_root=state_root_path,
        )
        if str(request_payload.get("workspace_root") or "").strip():
            workspace_root_path = Path(str(request_payload.get("workspace_root") or "")).expanduser().resolve()

        if not state_root_path.exists() and not workspace_root_path.exists():
            reconciled.append(
                {
                    "runtime_name": candidate_runtime_name,
                    "execution_owner": "reap_reconciler",
                    "outcome": "already_quiesced",
                    "request_event_id": str(request_event.get("event_id") or ""),
                    "runtime_root_quiesced_event_id": str(quiesced_event.get("event_id") or ""),
                    **({"controller_runner": controller_runner} if controller_runner else {}),
                }
            )
            continue

        report = stopped_run_gc_report(state_root=state_root_path, repo_root=resolved_repo_root)
        if not bool(report.get("reapable")):
            deferred_payload = {
                "runtime_name": candidate_runtime_name,
                "state_root": str(report.get("state_root") or state_root_path),
                "workspace_root": str(report.get("workspace_root") or workspace_root_path),
                "runtime_root_identity_token": runtime_identity_token_value,
                "blocking_reasons": list(report.get("blocking_reasons") or []),
                "blocking_nodes": [dict(item) for item in list(report.get("blocking_nodes") or [])],
                "live_launches": [dict(item) for item in list(report.get("live_launches") or [])],
                "canonical_reap_requested_event_id": str(request_event.get("event_id") or ""),
            }
            canonical_deferred_event = commit_reap_deferred_event(
                housekeeping_root,
                request_id=_reap_deferred_id(
                    candidate_runtime_name,
                    blocking_reasons=list(deferred_payload["blocking_reasons"]),
                ),
                node_id=candidate_runtime_name,
                payload=deferred_payload,
                producer="runtime.gc.reconcile_housekeeping_reap_requests",
            )
            compatibility_deferred_event = commit_runtime_reap_deferred_event(
                housekeeping_root,
                request_id=_reap_deferred_id(
                    candidate_runtime_name,
                    blocking_reasons=list(deferred_payload["blocking_reasons"]),
                ),
                node_id=candidate_runtime_name,
                payload=deferred_payload,
                producer="runtime.gc.reconcile_housekeeping_reap_requests",
            )
            reconciled.append(
                {
                    "runtime_name": candidate_runtime_name,
                    "execution_owner": "reap_reconciler",
                    "outcome": "deferred",
                    "request_event_id": str(request_event.get("event_id") or ""),
                    "reap_deferred_event_id": str(canonical_deferred_event.get("event_id") or ""),
                    "runtime_reap_deferred_event_id": str(compatibility_deferred_event.get("event_id") or ""),
                    "blocking_reasons": list(deferred_payload["blocking_reasons"]),
                    **({"controller_runner": controller_runner} if controller_runner else {}),
                }
            )
            continue

        owned_processes = [dict(item) for item in list(report.get("owned_processes") or [])]
        cleanup_nonce = _cleanup_nonce(
            candidate_runtime_name,
            state_root=state_root_path,
            workspace_root=workspace_root_path,
            live_launches=[dict(item) for item in list(report.get("live_launches") or [])],
            owned_processes=owned_processes,
        )
        orphan_event_ids: list[str] = []
        for ordinal, item in enumerate(owned_processes, start=1):
            orphan_event = commit_process_orphan_detected_event(
                housekeeping_root,
                observation_id=_orphan_detected_id(
                    candidate_runtime_name,
                    int(item.get("pid") or 0),
                    ordinal=ordinal,
                    cleanup_nonce=cleanup_nonce,
                ),
                node_id=candidate_runtime_name,
                payload={
                    "runtime_name": candidate_runtime_name,
                    "state_root": str(state_root_path),
                    "workspace_root": str(workspace_root_path),
                    "runtime_root_identity_token": runtime_identity_token_value,
                    "pid": int(item.get("pid") or 0),
                    "command": str(item.get("command") or ""),
                    "canonical_reap_requested_event_id": str(request_event.get("event_id") or ""),
                },
                producer="runtime.gc.reconcile_housekeeping_reap_requests",
            )
            orphan_event_ids.append(str(orphan_event.get("event_id") or ""))

        for item in owned_processes:
            _terminate_pid(int(item.get("pid") or 0), grace_s=grace_s)

        receipt_ref = _receipt_ref(repo_root=resolved_repo_root, runtime_name=candidate_runtime_name)
        canonical_confirmed_event = commit_process_reap_confirmed_event(
            housekeeping_root,
            request_id=_reap_confirmed_id(candidate_runtime_name),
            node_id=candidate_runtime_name,
            payload={
                "runtime_name": candidate_runtime_name,
                "state_root": str(state_root_path),
                "workspace_root": str(workspace_root_path),
                "runtime_root_identity_token": runtime_identity_token_value,
                "owned_processes_terminated": owned_processes,
                "canonical_reap_requested_event_id": str(request_event.get("event_id") or ""),
                "receipt_ref": str(receipt_ref),
            },
            producer="runtime.gc.reconcile_housekeeping_reap_requests",
        )
        compatibility_confirmed_event = commit_runtime_reap_confirmed_event(
            housekeeping_root,
            request_id=_reap_confirmed_id(candidate_runtime_name),
            node_id=candidate_runtime_name,
            payload={
                "runtime_name": candidate_runtime_name,
                "state_root": str(state_root_path),
                "workspace_root": str(workspace_root_path),
                "runtime_root_identity_token": runtime_identity_token_value,
                "owned_processes_terminated": owned_processes,
                "canonical_reap_requested_event_id": str(request_event.get("event_id") or ""),
                "receipt_ref": str(receipt_ref),
            },
            producer="runtime.gc.reconcile_housekeeping_reap_requests",
        )

        shutil.rmtree(state_root_path, ignore_errors=True)
        shutil.rmtree(workspace_root_path, ignore_errors=True)

        quiesced_event = commit_runtime_root_quiesced_event(
            housekeeping_root,
            observation_id=_runtime_quiesced_id(candidate_runtime_name, cleanup_nonce=cleanup_nonce),
            node_id=candidate_runtime_name,
            payload={
                "runtime_name": candidate_runtime_name,
                "state_root": str(state_root_path),
                "workspace_root": str(workspace_root_path),
                "runtime_root_identity_token": runtime_identity_token_value,
                "canonical_reap_requested_event_id": str(request_event.get("event_id") or ""),
                "canonical_process_reap_confirmed_event_id": str(canonical_confirmed_event.get("event_id") or ""),
                "canonical_process_orphan_detected_event_ids": orphan_event_ids,
                "receipt_ref": str(receipt_ref),
            },
            producer="runtime.gc.reconcile_housekeeping_reap_requests",
        )
        receipt = _build_receipt(
            runtime_name=candidate_runtime_name,
            repo_root=resolved_repo_root,
            housekeeping_root=housekeeping_root,
            state_root_path=state_root_path,
            workspace_root_path=workspace_root_path,
            owned_processes=owned_processes,
            canonical_request_event=request_event,
            canonical_confirmed_event=canonical_confirmed_event,
            orphan_event_ids=orphan_event_ids,
            quiesced_event=quiesced_event,
            request_event=event_heads.get("runtime_reap_requested") or {},
            confirmed_event=compatibility_confirmed_event,
            execution_owner="reap_reconciler",
            reconciler_outcome="confirmed",
            controller_runner=controller_runner,
        )
        reconciled.append(
            {
                **receipt,
                "execution_owner": "reap_reconciler",
                "outcome": "confirmed",
                "request_event_id": str(request_event.get("event_id") or ""),
                "process_reap_confirmed_event_id": str(canonical_confirmed_event.get("event_id") or ""),
                "runtime_root_quiesced_event_id": str(quiesced_event.get("event_id") or ""),
                **({"controller_runner": controller_runner} if controller_runner else {}),
            }
        )
    return reconciled


def submit_stopped_run_reap_request(
    *,
    state_root: str | Path,
    repo_root: str | Path,
    auto_bootstrap_controller: bool = True,
    controller_poll_interval_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S,
    controller_idle_exit_after_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_SERVICE_IDLE_EXIT_AFTER_S,
    controller_startup_timeout_s: float = _HOUSEKEEPING_REAP_CONTROLLER_STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_reap_requested_event,
        commit_runtime_reap_requested_event,
    )

    report = stopped_run_gc_report(state_root=state_root, repo_root=repo_root)
    resolved_repo_root = _repo_root(repo_root)
    housekeeping_root = housekeeping_runtime_root(repo_root=resolved_repo_root)
    runtime_name = str(report.get("runtime_name") or "run")
    request_payload = _request_payload_from_report(report)
    canonical_request_event = commit_reap_requested_event(
        housekeeping_root,
        request_id=_reap_request_id(runtime_name),
        node_id=runtime_name,
        payload=request_payload,
        producer="runtime.gc.submit_stopped_run_reap_request",
    )
    compatibility_request_event = commit_runtime_reap_requested_event(
        housekeeping_root,
        request_id=_reap_request_id(runtime_name),
        node_id=runtime_name,
        payload=request_payload,
        producer="runtime.gc.submit_stopped_run_reap_request",
    )
    result = {
        "runtime_name": runtime_name,
        "housekeeping_state_root": str(housekeeping_root),
        "reapable": bool(report.get("reapable")),
        "blocking_reasons": list(report.get("blocking_reasons") or []),
        "canonical_reap_requested_event_id": str(canonical_request_event.get("event_id") or ""),
        "reap_requested_event_id": str(compatibility_request_event.get("event_id") or ""),
    }
    if not auto_bootstrap_controller:
        result["controller_bootstrap_status"] = "disabled"
        return result

    try:
        controller_payload = ensure_housekeeping_reap_controller_running(
            repo_root=resolved_repo_root,
            poll_interval_s=float(controller_poll_interval_s),
            idle_exit_after_s=float(controller_idle_exit_after_s),
            watch_repo_control_plane=True,
            startup_timeout_s=float(controller_startup_timeout_s),
        )
    except Exception as exc:
        result["controller_bootstrap_status"] = "failed"
        result["controller_bootstrap_error"] = str(exc)
        return result

    result["controller_bootstrap_status"] = "ready"
    result["controller_pid"] = int(controller_payload.get("pid") or 0)
    result["controller_runtime_ref"] = str(
        housekeeping_reap_controller_runtime_ref(repo_root=resolved_repo_root)
    )
    return result


def run_housekeeping_reap_controller_once(
    *, repo_root: str | Path, runtime_name: str = "", grace_s: float = 0.75
) -> list[dict[str, Any]]:
    return reconcile_housekeeping_reap_requests(
        repo_root=repo_root,
        runtime_name=runtime_name,
        grace_s=grace_s,
        controller_runner="housekeeping_reap_controller",
    )


def _run_housekeeping_reap_controller_cycle(
    *, repo_root: str | Path, grace_s: float = 0.75
) -> list[dict[str, Any]]:
    return reconcile_housekeeping_reap_requests(
        repo_root=repo_root,
        grace_s=grace_s,
        controller_runner="housekeeping_reap_controller_loop",
    )


def run_housekeeping_reap_controller_loop(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_IDLE_EXIT_AFTER_S,
    watch_repo_control_plane: bool = False,
    grace_s: float = 0.75,
) -> int:
    resolved_repo_root = _repo_root(repo_root)
    poll_interval_value = max(0.05, float(poll_interval_s))
    idle_exit_value = max(0.0, float(idle_exit_after_s))
    seen_signatures: set[str] = set()
    lease_epoch = _housekeeping_controller_next_lease_epoch(repo_root=resolved_repo_root)
    started_at_utc = _now_iso()
    heartbeat_ordinal = 0

    with _housekeeping_reap_controller_runtime_marker(
        repo_root=resolved_repo_root,
        poll_interval_s=poll_interval_value,
        idle_exit_after_s=idle_exit_value,
        watch_repo_control_plane=watch_repo_control_plane,
        lease_epoch=lease_epoch,
        started_at_utc=started_at_utc,
    ):
        startup_outcome = _emit_housekeeping_controller_authority(
            repo_root=resolved_repo_root,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
            poll_interval_s=poll_interval_value,
            idle_exit_after_s=idle_exit_value,
            watch_repo_control_plane=watch_repo_control_plane,
            process_started_at_utc=started_at_utc,
        )
        if _housekeeping_controller_should_self_fence(startup_outcome):
            _emit_housekeeping_controller_exit_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                watch_repo_control_plane=watch_repo_control_plane,
                process_started_at_utc=started_at_utc,
                exit_reason="lease_fenced",
            )
            return 0
        last_activity = time.monotonic()
        while True:
            cleanup_authority = _housekeeping_controller_cleanup_authority(repo_root=resolved_repo_root)
            if cleanup_authority is not None:
                _emit_housekeeping_controller_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    watch_repo_control_plane=watch_repo_control_plane,
                    process_started_at_utc=started_at_utc,
                    exit_reason="cleanup_committed",
                    cleanup_authority=cleanup_authority,
                )
                return 0
            if watch_repo_control_plane:
                _ensure_repo_control_plane_watchdog(
                    repo_root=resolved_repo_root,
                    poll_interval_s=poll_interval_value,
                )
            results = _run_housekeeping_reap_controller_cycle(
                repo_root=resolved_repo_root,
                grace_s=grace_s,
            )
            novel = False
            for item in results:
                signature = _housekeeping_result_signature(dict(item))
                if signature not in seen_signatures:
                    seen_signatures.add(signature)
                    novel = True
            if novel:
                last_activity = time.monotonic()
            elif idle_exit_value > 0.0 and (time.monotonic() - last_activity) >= idle_exit_value:
                _emit_housekeeping_controller_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    watch_repo_control_plane=watch_repo_control_plane,
                    process_started_at_utc=started_at_utc,
                    exit_reason="idle_exit",
                )
                return 0
            heartbeat_ordinal += 1
            heartbeat_outcome = _emit_housekeeping_controller_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                heartbeat_ordinal=heartbeat_ordinal,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                watch_repo_control_plane=watch_repo_control_plane,
                process_started_at_utc=started_at_utc,
            )
            if _housekeeping_controller_should_self_fence(heartbeat_outcome):
                _emit_housekeeping_controller_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    watch_repo_control_plane=watch_repo_control_plane,
                    process_started_at_utc=started_at_utc,
                    exit_reason="lease_fenced",
                )
                return 0
            time.sleep(poll_interval_value)


def ensure_housekeeping_reap_controller_running(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_IDLE_EXIT_AFTER_S,
    watch_repo_control_plane: bool = False,
    startup_timeout_s: float = _HOUSEKEEPING_REAP_CONTROLLER_STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    cleanup_authority = _housekeeping_controller_cleanup_authority(repo_root=resolved_repo_root)
    if cleanup_authority is not None:
        return {
            "bootstrap_status": "blocked_cleanup_committed",
            "service_kind": "housekeeping_reap_controller",
            "repo_root": str(resolved_repo_root),
            "cleanup_authority_event_id": str(cleanup_authority.get("event_id") or ""),
            "cleanup_authority_event_type": str(cleanup_authority.get("event_type") or ""),
            "cleanup_authority_seq": int(cleanup_authority.get("seq") or 0),
        }
    current_code_fingerprint = _housekeeping_reap_controller_code_fingerprint()
    existing = live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root)
    if existing is not None:
        existing_watch_repo_control_plane = bool(existing.get("watch_repo_control_plane"))
        requested_watch_repo_control_plane = bool(watch_repo_control_plane)
        if (
            str(existing.get("code_fingerprint") or "").strip() == current_code_fingerprint
            and (existing_watch_repo_control_plane or not requested_watch_repo_control_plane)
        ):
            return existing
        _terminate_pid(int(existing.get("pid") or 0))
        housekeeping_reap_controller_runtime_ref(repo_root=resolved_repo_root).unlink(missing_ok=True)

    runtime_ref = housekeeping_reap_controller_runtime_ref(repo_root=resolved_repo_root)
    log_ref = housekeeping_reap_controller_log_ref(repo_root=resolved_repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    package_repo_root = Path(__file__).resolve().parents[2]
    with log_ref.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.runtime.gc",
                "--repo-root",
                str(resolved_repo_root),
                "--run-housekeeping-controller",
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
        payload = live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root)
        if payload is not None:
            return payload
        exit_code = proc.poll()
        if exit_code is not None:
            payload = live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root)
            if payload is not None:
                return payload
            if int(exit_code) == 0:
                time.sleep(0.05)
                continue
            detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
            raise RuntimeError(detail or f"housekeeping reap controller exited early with code {exit_code}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for housekeeping reap controller runtime marker at {runtime_ref}")


def ensure_housekeeping_reap_controller_service_running(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S,
    watch_repo_control_plane: bool = True,
    startup_timeout_s: float = _HOUSEKEEPING_REAP_CONTROLLER_STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    return ensure_housekeeping_reap_controller_running(
        repo_root=repo_root,
        poll_interval_s=float(poll_interval_s),
        idle_exit_after_s=_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_SERVICE_IDLE_EXIT_AFTER_S,
        watch_repo_control_plane=watch_repo_control_plane,
        startup_timeout_s=float(startup_timeout_s),
    )


def ensure_housekeeping_reap_controller_service_for_runtime_root(
    *,
    state_root: str | Path,
    poll_interval_s: float = _DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S,
    startup_timeout_s: float = _HOUSEKEEPING_REAP_CONTROLLER_STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    repo_root = _top_level_repo_root_from_runtime_root(state_root=resolved_state_root)
    if repo_root is None:
        return {
            "bootstrap_status": "skipped",
            "skip_reason": "non_top_level_runtime_root",
            "state_root": str(resolved_state_root),
        }
    payload = ensure_housekeeping_reap_controller_service_running(
        repo_root=repo_root,
        poll_interval_s=float(poll_interval_s),
        startup_timeout_s=float(startup_timeout_s),
    )
    return {
        **dict(payload or {}),
        "bootstrap_status": "ready",
        "repo_root": str(repo_root),
        "state_root": str(resolved_state_root),
    }


def reap_stopped_run(*, state_root: str | Path, repo_root: str | Path, grace_s: float = 0.75) -> dict[str, Any]:
    report = stopped_run_gc_report(state_root=state_root, repo_root=repo_root)
    resolved_repo_root = _repo_root(repo_root)
    runtime_name = str(report.get("runtime_name") or "run")
    request = submit_stopped_run_reap_request(
        state_root=state_root,
        repo_root=resolved_repo_root,
        auto_bootstrap_controller=False,
    )
    reconciled = run_housekeeping_reap_controller_once(
        repo_root=resolved_repo_root,
        runtime_name=runtime_name,
        grace_s=grace_s,
    )
    if not reconciled:
        raise RuntimeError(f"reap reconciler produced no outcome for {runtime_name!r}")
    outcome = dict(reconciled[-1])
    if str(outcome.get("outcome") or "") == "deferred":
        raise ValueError(
            f"run {report.get('runtime_name')!r} is not reapable: {', '.join(list(outcome.get('blocking_reasons') or []))}"
        )
    if str(outcome.get("outcome") or "") not in {"confirmed", "already_confirmed", "already_quiesced"}:
        raise RuntimeError(f"unexpected reap reconciler outcome for {runtime_name!r}: {outcome!r}")
    outcome.setdefault("housekeeping_state_root", str(request.get("housekeeping_state_root") or ""))
    outcome.setdefault("canonical_reap_requested_event_id", str(request.get("canonical_reap_requested_event_id") or ""))
    outcome.setdefault("reap_requested_event_id", str(request.get("reap_requested_event_id") or ""))
    return outcome


def cleanup_test_runtime_root(
    *,
    state_root: str | Path,
    repo_root: str | Path,
    grace_s: float = 0.75,
    settle_timeout_s: float = 6.0,
    poll_interval_s: float = 0.05,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_process_orphan_detected_event,
        commit_test_runtime_cleanup_committed_event,
        commit_runtime_root_quiesced_event,
    )

    resolved_repo_root = _repo_root(repo_root)
    resolved_state_root = _state_root(state_root)
    resolved_workspace_root = _workspace_root_for_state_root(repo_root=resolved_repo_root, state_root=resolved_state_root)
    defer_state_root_delete_until_quiesced = True
    housekeeping_root = housekeeping_runtime_root(repo_root=resolved_repo_root)
    runtime_name = resolved_state_root.name
    root_identity_token = runtime_root_identity_token(resolved_state_root, ensure=True)
    live_launches = _live_recorded_launches(resolved_state_root)
    owned_processes = _owned_processes(state_root=resolved_state_root, workspace_root=resolved_workspace_root)
    excluded = _excluded_pids()
    cleanup_nonce = _cleanup_nonce(
        runtime_name,
        state_root=resolved_state_root,
        workspace_root=resolved_workspace_root,
        live_launches=live_launches,
        owned_processes=owned_processes,
    )

    cleanup_event = commit_test_runtime_cleanup_committed_event(
        housekeeping_root,
        observation_id=_test_cleanup_id(runtime_name, cleanup_nonce=cleanup_nonce),
        node_id=runtime_name,
        payload={
            "runtime_name": runtime_name,
            "state_root": str(resolved_state_root),
            "workspace_root": str(resolved_workspace_root),
            "runtime_root_identity_token": root_identity_token,
            "cleanup_kind": "test_runtime_teardown",
            "live_launches": [dict(item) for item in live_launches],
            "owned_processes": [dict(item) for item in owned_processes],
        },
        producer="runtime.gc.cleanup_test_runtime_root",
    )
    orphan_events: list[str] = []
    for ordinal, item in enumerate(owned_processes, start=1):
        orphan_event = commit_process_orphan_detected_event(
            housekeeping_root,
            observation_id=_orphan_detected_id(
                runtime_name,
                int(item.get("pid") or 0),
                ordinal=ordinal,
                cleanup_nonce=cleanup_nonce,
            ),
            node_id=runtime_name,
            payload={
                "runtime_name": runtime_name,
                "state_root": str(resolved_state_root),
                "workspace_root": str(resolved_workspace_root),
                "runtime_root_identity_token": root_identity_token,
                "pid": int(item.get("pid") or 0),
                "command": str(item.get("command") or ""),
                "cleanup_event_id": str(cleanup_event.get("event_id") or ""),
            },
            producer="runtime.gc.cleanup_test_runtime_root",
        )
        orphan_events.append(str(orphan_event.get("event_id") or ""))

    terminated_pids: set[int] = set()
    for launch in live_launches:
        pid = int(launch.get("pid") or 0)
        if pid > 0 and pid not in terminated_pids and pid not in excluded:
            _terminate_pid(pid, grace_s=grace_s)
            terminated_pids.add(pid)
    for item in owned_processes:
        pid = int(item.get("pid") or 0)
        command = str(item.get("command") or "")
        if pid <= 0 or pid in terminated_pids:
            continue
        _terminate_pid(pid, grace_s=grace_s)
        terminated_pids.add(pid)

    shutil.rmtree(resolved_workspace_root, ignore_errors=True)
    if not defer_state_root_delete_until_quiesced:
        shutil.rmtree(resolved_state_root, ignore_errors=True)

    deadline = time.monotonic() + max(0.0, float(settle_timeout_s))
    remaining_owned_processes = _owned_processes(state_root=resolved_state_root, workspace_root=resolved_workspace_root)
    while remaining_owned_processes and time.monotonic() < deadline:
        time.sleep(max(0.01, float(poll_interval_s)))
        remaining_owned_processes = _owned_processes(state_root=resolved_state_root, workspace_root=resolved_workspace_root)

    quiesced = not remaining_owned_processes
    quiesced_event_id = ""
    if quiesced:
        if defer_state_root_delete_until_quiesced:
            shutil.rmtree(resolved_state_root, ignore_errors=True)
        quiesced_event = commit_runtime_root_quiesced_event(
            housekeeping_root,
            observation_id=_runtime_quiesced_id(runtime_name, cleanup_nonce=cleanup_nonce),
            node_id=runtime_name,
            payload={
                "runtime_name": runtime_name,
                "state_root": str(resolved_state_root),
                "workspace_root": str(resolved_workspace_root),
                "runtime_root_identity_token": root_identity_token,
                "cleanup_event_id": str(cleanup_event.get("event_id") or ""),
                "orphan_event_ids": orphan_events,
                "terminated_pids": sorted(terminated_pids),
            },
            producer="runtime.gc.cleanup_test_runtime_root",
        )
        quiesced_event_id = str(quiesced_event.get("event_id") or "")

    return {
        "runtime_name": runtime_name,
        "repo_root": str(resolved_repo_root),
        "state_root": str(resolved_state_root),
        "workspace_root": str(resolved_workspace_root),
        "cleanup_event_id": str(cleanup_event.get("event_id") or ""),
        "process_orphan_detected_event_ids": orphan_events,
        "runtime_root_quiesced_event_id": quiesced_event_id,
        "terminated_pids": sorted(terminated_pids),
        "remaining_owned_processes": [dict(item) for item in remaining_owned_processes],
        "quiesced": quiesced,
    }


def list_stopped_run_gc_candidates(*, repo_root: str | Path) -> list[dict[str, Any]]:
    resolved_repo_root = _repo_root(repo_root)
    state_scope_root = resolved_repo_root / ".loop"
    candidates: list[dict[str, Any]] = []
    if not state_scope_root.exists():
        return candidates
    for path in sorted(state_scope_root.iterdir()):
        if not path.is_dir():
            continue
        if path.name in {"quarantine", "host_child_launch_requests", "host_child_runtime_status_requests", "host_child_launch_supervisor"}:
            continue
        if not (path / "state").exists():
            continue
        report = stopped_run_gc_report(state_root=path, repo_root=resolved_repo_root)
        if bool(report.get("reapable")):
            candidates.append(report)
    return candidates


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="GC stopped LOOP runtime runs")
    parser.add_argument("--repo-root", required=True, help="Path to loop_product_repo")
    parser.add_argument(
        "--run-housekeeping-controller",
        action="store_true",
        help="Run the long-lived housekeeping reap controller loop",
    )
    parser.add_argument("--once", action="store_true", help="When used with --run-housekeeping-controller, process once and exit")
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_POLL_INTERVAL_S,
        help="Controller polling interval when using --run-housekeeping-controller",
    )
    parser.add_argument(
        "--idle-exit-after-s",
        type=float,
        default=_DEFAULT_HOUSEKEEPING_REAP_CONTROLLER_IDLE_EXIT_AFTER_S,
        help="Controller idle timeout when using --run-housekeeping-controller; use 0 to disable idle exit",
    )
    parser.add_argument(
        "--watch-repo-control-plane",
        action="store_true",
        help="When enabled, restore repo control-plane if it disappears while housekeeping remains live",
    )
    parser.add_argument(
        "--grace-s",
        type=float,
        default=0.75,
        help="Termination grace period for owned processes during reap reconciliation",
    )
    parser.add_argument("--list", action="store_true", help="List reapable stopped runs")
    parser.add_argument("--state-root", default="", help="Reap exactly one stopped run rooted here")
    parser.add_argument("--reap-stopped-all", action="store_true", help="Reap every currently reapable stopped run")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    repo_root = _repo_root(args.repo_root)
    if args.run_housekeeping_controller:
        if args.once:
            run_housekeeping_reap_controller_once(repo_root=repo_root, grace_s=float(args.grace_s))
            return 0
        return run_housekeeping_reap_controller_loop(
            repo_root=repo_root,
            poll_interval_s=float(args.poll_interval_s),
            idle_exit_after_s=float(args.idle_exit_after_s),
            watch_repo_control_plane=bool(args.watch_repo_control_plane),
            grace_s=float(args.grace_s),
        )
    if args.state_root:
        print(
            json.dumps(
                reap_stopped_run(state_root=args.state_root, repo_root=repo_root, grace_s=float(args.grace_s)),
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if args.reap_stopped_all:
        receipts = [
            reap_stopped_run(state_root=str(item["state_root"]), repo_root=repo_root, grace_s=float(args.grace_s))
            for item in list_stopped_run_gc_candidates(repo_root=repo_root)
        ]
        print(json.dumps(receipts, indent=2, sort_keys=True))
        return 0
    print(json.dumps(list_stopped_run_gc_candidates(repo_root=repo_root), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
