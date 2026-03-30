"""Detached committed supervision sidecar for launched child nodes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from loop_product.runtime.lifecycle import (
    RuntimeContextMissingError,
    persist_committed_supervision_result,
    supervise_child_until_settled,
)
from loop_product.event_journal import commit_guarded_runtime_liveness_observation_event
from loop_product.event_journal import commit_heartbeat_observed_event
from loop_product.event_journal import commit_supervision_attached_event
from loop_product.event_journal import commit_supervision_detached_event
from loop_product.event_journal import iter_committed_events
from loop_product.runtime_identity import runtime_root_identity_token
from loop_product.runtime_paths import require_runtime_root
from loop_product.state_io import write_json_read_only

_DEFAULT_POLL_INTERVAL_S = 2.0
_DEFAULT_STALL_THRESHOLD_S = 60.0
_DEFAULT_MAX_RECOVERIES = 5
_DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S = 300.0
_STARTUP_TIMEOUT_S = 5.0
_REPO_SUPERVISION_CONTROL_LOOP_S = 0.05
_REPO_SUPERVISION_RUNTIME_HEARTBEAT_S = 1.0
_REPO_SUPERVISION_IDLE_BACKOFF_S = 0.5
_REPO_SUPERVISION_ORPHAN_WORKER_GRACE_S = 1.0


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    write_json_read_only(path, payload)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def _runtime_marker_matches_process(payload: dict[str, Any]) -> bool:
    runtime_kind = str(payload.get("runtime_kind") or "").strip()
    if runtime_kind == "repo_scoped_child_supervision":
        return _repo_supervision_reactor_matches_process(payload)
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    launch_result_ref = str(payload.get("launch_result_ref") or "").strip()
    if not launch_result_ref:
        return False
    return "loop_product.child_supervision_sidecar" in command_line and launch_result_ref in command_line


def supervision_runtime_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionRuntime.json")


def supervision_lease_state_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionLeaseState.json")


def supervision_result_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionResult.json")


def supervision_log_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervision.log")


def supervision_spawn_lock_ref(*, launch_result_ref: str | Path) -> Path:
    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionRuntime.lock")


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


def _load_launch_roots(launch_result_ref: str | Path) -> tuple[Path, Path]:
    launch_payload = _load_launch_payload(Path(launch_result_ref).expanduser().resolve())
    workspace_root = Path(str(launch_payload.get("workspace_root") or "")).expanduser().resolve()
    state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
    return workspace_root, state_root


def repo_supervision_reactor_root(*, launch_result_ref: str | Path) -> Path:
    _, state_root = _load_launch_roots(launch_result_ref)
    return (state_root / "artifacts" / "runtime" / "repo_supervision_reactor").resolve()


def repo_supervision_reactor_runtime_ref(*, repo_supervision_root: str | Path) -> Path:
    return Path(repo_supervision_root).expanduser().resolve() / "RepoSupervisionReactorRuntime.json"


def repo_supervision_reactor_registry_ref(*, repo_supervision_root: str | Path) -> Path:
    return Path(repo_supervision_root).expanduser().resolve() / "TrackedLaunches.json"


def repo_supervision_reactor_log_ref(*, repo_supervision_root: str | Path) -> Path:
    return Path(repo_supervision_root).expanduser().resolve() / "RepoSupervisionReactor.log"


def repo_supervision_reactor_spawn_lock_ref(*, repo_supervision_root: str | Path) -> Path:
    return Path(repo_supervision_root).expanduser().resolve() / "RepoSupervisionReactor.lock"


def _repo_supervision_runtime_payload(
    *,
    repo_supervision_root: Path,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
    tracked_launch_refs: list[str],
    started_at_utc: str,
) -> dict[str, Any]:
    return {
        "schema": "loop_product.repo_scoped_child_supervision_reactor_runtime",
        "runtime_kind": "repo_scoped_child_supervision_reactor",
        "repo_supervision_root": str(repo_supervision_root),
        "pid": int(os.getpid()),
        "poll_interval_s": float(poll_interval_s),
        "stall_threshold_s": float(stall_threshold_s),
        "max_recoveries": int(max_recoveries),
        "no_substantive_progress_window_s": float(no_substantive_progress_window_s),
        "tracked_launch_refs": [str(item) for item in tracked_launch_refs if str(item)],
        "started_at_utc": str(started_at_utc or _now_iso()),
        "log_ref": str(repo_supervision_reactor_log_ref(repo_supervision_root=repo_supervision_root)),
        "registry_ref": str(repo_supervision_reactor_registry_ref(repo_supervision_root=repo_supervision_root)),
    }


def _repo_supervision_reactor_matches_process(payload: dict[str, Any]) -> bool:
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    repo_supervision_root = str(payload.get("repo_supervision_root") or "").strip()
    if not repo_supervision_root:
        return False
    return (
        "loop_product.child_supervision_sidecar" in command_line
        and "--run-repo-supervision-reactor" in command_line
        and _command_flag_value(command_line, "--repo-supervision-root") == repo_supervision_root
    )


def live_repo_supervision_reactor_runtime(*, repo_supervision_root: str | Path) -> dict[str, Any] | None:
    runtime_ref = repo_supervision_reactor_runtime_ref(repo_supervision_root=repo_supervision_root)
    if not runtime_ref.exists():
        return None
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        return None
    if not _repo_supervision_reactor_matches_process(payload):
        runtime_ref.unlink(missing_ok=True)
        return None
    return payload


def _load_repo_supervision_registry(repo_supervision_root: Path) -> dict[str, Any]:
    registry_ref = repo_supervision_reactor_registry_ref(repo_supervision_root=repo_supervision_root)
    if not registry_ref.exists():
        return {
            "schema": "loop_product.repo_scoped_child_supervision_registry",
            "repo_supervision_root": str(repo_supervision_root),
            "tracked_launches": {},
        }
    try:
        payload = _load_runtime_payload(registry_ref)
    except Exception:
        return {
            "schema": "loop_product.repo_scoped_child_supervision_registry",
            "repo_supervision_root": str(repo_supervision_root),
            "tracked_launches": {},
        }
    tracked_launches = dict(payload.get("tracked_launches") or {})
    return {
        "schema": "loop_product.repo_scoped_child_supervision_registry",
        "repo_supervision_root": str(repo_supervision_root),
        "tracked_launches": tracked_launches,
    }


def _write_repo_supervision_registry(
    *,
    repo_supervision_root: Path,
    tracked_launches: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "schema": "loop_product.repo_scoped_child_supervision_registry",
        "repo_supervision_root": str(repo_supervision_root),
        "updated_at_utc": _now_iso(),
        "tracked_launches": tracked_launches,
    }
    _write_json(repo_supervision_reactor_registry_ref(repo_supervision_root=repo_supervision_root), payload)
    return payload


def reconcile_repo_supervision_registry(*, repo_supervision_root: str | Path) -> dict[str, Any]:
    root = Path(repo_supervision_root).expanduser().resolve()
    registry = _load_repo_supervision_registry(root)
    tracked_launches = dict(registry.get("tracked_launches") or {})
    retained: dict[str, dict[str, Any]] = {}
    for launch_result_ref, entry in tracked_launches.items():
        launch_ref = Path(str(launch_result_ref)).expanduser().resolve()
        if not launch_ref.exists():
            continue
        if supervision_result_ref(launch_result_ref=launch_ref).exists():
            continue
        retained[str(launch_ref)] = dict(entry or {})
    return _write_repo_supervision_registry(repo_supervision_root=root, tracked_launches=retained)


def register_launch_for_repo_supervision(
    *,
    launch_result_ref: str | Path,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
) -> dict[str, Any]:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    repo_root = repo_supervision_reactor_root(launch_result_ref=launch_path)
    repo_root.mkdir(parents=True, exist_ok=True)
    registry = reconcile_repo_supervision_registry(repo_supervision_root=repo_root)
    tracked_launches = dict(registry.get("tracked_launches") or {})
    tracked_launches[str(launch_path)] = {
        "launch_result_ref": str(launch_path),
        "poll_interval_s": float(poll_interval_s),
        "stall_threshold_s": float(stall_threshold_s),
        "max_recoveries": int(max_recoveries),
        "no_substantive_progress_window_s": float(no_substantive_progress_window_s),
        "registered_at_utc": _now_iso(),
    }
    return _write_repo_supervision_registry(repo_supervision_root=repo_root, tracked_launches=tracked_launches)


def _should_refresh_repo_supervision_runtime(
    *,
    tracked_launch_refs: list[str],
    previous_tracked_launch_refs: list[str] | None,
    had_worker_state_change: bool,
    now_monotonic: float,
    last_runtime_update_monotonic: float,
    heartbeat_interval_s: float,
) -> bool:
    if previous_tracked_launch_refs is None:
        return True
    if list(previous_tracked_launch_refs) != list(tracked_launch_refs):
        return True
    if had_worker_state_change:
        return True
    return (now_monotonic - float(last_runtime_update_monotonic)) >= max(0.0, float(heartbeat_interval_s))


def _repo_supervision_reactor_sleep_s(
    *,
    base_poll_interval_s: float,
    tracked_launch_refs: list[str],
    previous_tracked_launch_refs: list[str] | None,
    worker_count: int,
    had_worker_state_change: bool,
) -> float:
    del base_poll_interval_s
    base_interval = _REPO_SUPERVISION_CONTROL_LOOP_S
    if previous_tracked_launch_refs is None:
        return base_interval
    if had_worker_state_change or list(previous_tracked_launch_refs) != list(tracked_launch_refs):
        return base_interval
    if worker_count > 0:
        return max(base_interval, _REPO_SUPERVISION_IDLE_BACKOFF_S)
    if tracked_launch_refs:
        return max(base_interval, 0.2)
    return base_interval


def _repo_supervision_reactor_should_exit(
    *,
    tracked_launch_refs: list[str],
    worker_count: int,
    empty_registry_since_monotonic: float | None,
    now_monotonic: float,
    orphan_worker_grace_s: float,
) -> bool:
    if tracked_launch_refs:
        return False
    if int(worker_count) <= 0:
        return True
    if empty_registry_since_monotonic is None:
        return False
    return (float(now_monotonic) - float(empty_registry_since_monotonic)) >= max(0.0, float(orphan_worker_grace_s))

def _load_runtime_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_launch_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _argv_process_fingerprint(argv: list[str]) -> str:
    normalized = [str(item) for item in list(argv or []) if str(item)]
    if not normalized:
        return ""
    digest = hashlib.sha256()
    digest.update(json.dumps(normalized, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def _payload_lease_epoch(payload: dict[str, Any]) -> int:
    try:
        epoch = int(dict(payload or {}).get("lease_epoch") or 0)
    except (TypeError, ValueError):
        return 0
    return epoch if epoch > 0 else 0


def _runtime_marker_epoch_floor(runtime_ref: Path) -> int:
    if not runtime_ref.exists():
        return 0
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        return 0
    epoch = _payload_lease_epoch(payload)
    if epoch > 0:
        return epoch
    if str(payload.get("schema") or "").strip() == "loop_product.child_supervision_runtime":
        return 1
    return 0


def _lease_state_epoch_floor(lease_state_ref: Path) -> int:
    if not lease_state_ref.exists():
        return 0
    try:
        payload = _load_runtime_payload(lease_state_ref)
    except Exception:
        return 0
    try:
        epoch = int(dict(payload or {}).get("last_reserved_lease_epoch") or 0)
    except (TypeError, ValueError):
        return 0
    return epoch if epoch > 0 else 0


def _preserve_stale_runtime_epoch_floor(*, runtime_ref: Path, payload: dict[str, Any]) -> None:
    epoch_floor = _payload_lease_epoch(payload)
    if epoch_floor <= 0 and str(payload.get("schema") or "").strip() == "loop_product.child_supervision_runtime":
        epoch_floor = 1
    if epoch_floor <= 0:
        return
    lease_state_ref = supervision_lease_state_ref(launch_result_ref=runtime_ref.with_name("ChildLaunchResult.json"))
    if _lease_state_epoch_floor(lease_state_ref) >= epoch_floor:
        return
    _write_json(
        lease_state_ref,
        {
            "schema": "loop_product.child_supervision_lease_state",
            "launch_result_ref": str(runtime_ref.with_name("ChildLaunchResult.json").resolve()),
            "last_reserved_lease_epoch": int(epoch_floor),
            "reserved_at_utc": _now_iso(),
            "runtime_ref": str(runtime_ref.resolve()),
        },
    )


def _node_launch_epoch_floor(*, launch_result_ref: Path) -> int:
    try:
        launch_payload = _load_launch_payload(launch_result_ref)
        state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
    except Exception:
        return 0
    node_id = str(launch_payload.get("node_id") or "").strip()
    if not node_id:
        return 0
    launch_root = state_root / "artifacts" / "launches" / node_id
    if not launch_root.exists():
        return 0
    epoch_floor = 0
    for runtime_ref in sorted(launch_root.glob("attempt_*/ChildSupervisionRuntime.json")):
        epoch_floor = max(epoch_floor, _runtime_marker_epoch_floor(runtime_ref))
    for lease_state_ref in sorted(launch_root.glob("attempt_*/ChildSupervisionLeaseState.json")):
        epoch_floor = max(epoch_floor, _lease_state_epoch_floor(lease_state_ref))
    return epoch_floor


def _node_committed_epoch_floor(*, launch_result_ref: Path) -> int:
    try:
        launch_payload = _load_launch_payload(launch_result_ref)
        state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
    except Exception:
        return 0
    node_id = str(launch_payload.get("node_id") or "").strip()
    if not node_id:
        return 0
    epoch_floor = 0
    for event in iter_committed_events(state_root):
        if str(event.get("node_id") or "").strip() != node_id:
            continue
        if str(event.get("event_type") or "").strip() != "runtime_liveness_observed":
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("attachment_state") or "").strip().upper() != "ATTACHED":
            continue
        epoch_floor = max(epoch_floor, _payload_lease_epoch(payload))
    return epoch_floor


def reserve_supervision_lease_epoch(
    *,
    launch_result_ref: str | Path,
    epoch_floor_hint: int = 0,
) -> int:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_path)
    lease_state_ref = supervision_lease_state_ref(launch_result_ref=launch_path)
    epoch_floor = max(
        int(epoch_floor_hint or 0),
        _runtime_marker_epoch_floor(runtime_ref),
        _lease_state_epoch_floor(lease_state_ref),
        _node_launch_epoch_floor(launch_result_ref=launch_path),
        _node_committed_epoch_floor(launch_result_ref=launch_path),
    )
    next_epoch = epoch_floor + 1 if epoch_floor > 0 else 1
    _write_json(
        lease_state_ref,
        {
            "schema": "loop_product.child_supervision_lease_state",
            "launch_result_ref": str(launch_path),
            "last_reserved_lease_epoch": int(next_epoch),
            "reserved_at_utc": _now_iso(),
            "runtime_ref": str(runtime_ref),
        },
    )
    return int(next_epoch)


def live_child_supervision_runtime(*, launch_result_ref: str | Path) -> dict[str, Any] | None:
    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_result_ref)
    if not runtime_ref.exists():
        return None
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        return None
    if not _runtime_marker_matches_process(payload):
        try:
            _preserve_stale_runtime_epoch_floor(runtime_ref=runtime_ref, payload=payload)
        except Exception:
            pass
        runtime_ref.unlink(missing_ok=True)
        return None
    return payload


def _same_node_live_supervision_runtime(*, launch_result_ref: str | Path) -> dict[str, Any] | None:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    try:
        launch_payload = _load_launch_payload(launch_path)
    except Exception:
        return None

    node_id = str(launch_payload.get("node_id") or "").strip()
    workspace_root_raw = str(launch_payload.get("workspace_root") or "").strip()
    state_root_raw = str(launch_payload.get("state_root") or "").strip()
    if not node_id or not workspace_root_raw or not state_root_raw:
        return None

    try:
        workspace_root = Path(workspace_root_raw).expanduser().resolve()
        state_root = Path(state_root_raw).expanduser().resolve()
    except Exception:
        return None

    launch_root = state_root / "artifacts" / "launches" / node_id
    if not launch_root.exists():
        return None

    exact_runtime_ref = supervision_runtime_ref(launch_result_ref=launch_path)
    for runtime_ref in sorted(launch_root.glob("attempt_*/ChildSupervisionRuntime.json"), reverse=True):
        if runtime_ref.resolve() == exact_runtime_ref:
            continue
        try:
            runtime_payload = _load_runtime_payload(runtime_ref)
        except Exception:
            runtime_ref.unlink(missing_ok=True)
            continue
        if not _runtime_marker_matches_process(runtime_payload):
            runtime_ref.unlink(missing_ok=True)
            continue
        launch_result_raw = str(runtime_payload.get("launch_result_ref") or "").strip()
        if not launch_result_raw:
            continue
        try:
            sibling_launch_ref = Path(launch_result_raw).expanduser().resolve()
            sibling_launch_payload = _load_launch_payload(sibling_launch_ref)
        except Exception:
            continue
        if str(sibling_launch_payload.get("node_id") or "").strip() != node_id:
            continue
        try:
            sibling_workspace_root = Path(str(sibling_launch_payload.get("workspace_root") or "")).expanduser().resolve()
            sibling_state_root = Path(str(sibling_launch_payload.get("state_root") or "")).expanduser().resolve()
        except Exception:
            continue
        if sibling_workspace_root != workspace_root or sibling_state_root != state_root:
            continue
        return runtime_payload
    return None


def _load_spawn_lock_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _sidecar_liveness_observation_id(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    node_id: str,
    pid: int,
    lease_epoch: int,
    started_at_utc: str,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "launch_result_ref": str(launch_result_ref),
                "runtime_ref": str(runtime_ref),
                "node_id": str(node_id or ""),
                "pid": int(pid),
                "lease_epoch": int(lease_epoch),
                "started_at_utc": str(started_at_utc or ""),
                "process_fingerprint": str(process_fingerprint or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return f"sidecar_liveness::{digest.hexdigest()[:16]}"


def _supervision_attached_observation_id(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    node_id: str,
    pid: int,
    lease_epoch: int,
    started_at_utc: str,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "launch_result_ref": str(launch_result_ref),
                "runtime_ref": str(runtime_ref),
                "node_id": str(node_id or ""),
                "pid": int(pid),
                "lease_epoch": int(lease_epoch),
                "started_at_utc": str(started_at_utc or ""),
                "process_fingerprint": str(process_fingerprint or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return f"supervision_attached::{digest.hexdigest()[:16]}"


def _supervision_detached_observation_id(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    node_id: str,
    lease_epoch: int,
) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "launch_result_ref": str(launch_result_ref),
                "runtime_ref": str(runtime_ref),
                "node_id": str(node_id or ""),
                "lease_epoch": int(lease_epoch),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return f"supervision_detached::{digest.hexdigest()[:16]}"


def _mirror_sidecar_runtime_liveness_attached_event(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    started_at_utc: str,
    lease_epoch: int,
    raw_argv: list[str],
) -> dict[str, Any] | None:
    try:
        launch_payload = _load_launch_payload(launch_result_ref)
        state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
    except Exception:
        return None
    node_id = str(launch_payload.get("node_id") or "").strip()
    if not node_id:
        return None
    process_fingerprint = _argv_process_fingerprint(raw_argv)
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        state_root,
        observation_id=_sidecar_liveness_observation_id(
            launch_result_ref=launch_result_ref,
            runtime_ref=runtime_ref,
            node_id=node_id,
            pid=os.getpid(),
            lease_epoch=lease_epoch,
            started_at_utc=started_at_utc,
            process_fingerprint=process_fingerprint,
        ),
        node_id=node_id,
        payload={
            "attachment_state": "ATTACHED",
            "observation_kind": "child_supervision_runtime_marker",
            "observed_at": str(started_at_utc or _now_iso()),
            "summary": "committed child supervision sidecar attached",
            "evidence_refs": [str(runtime_ref), str(launch_result_ref)],
            "pid_alive": True,
            "pid": int(os.getpid()),
            "lease_duration_s": 60.0,
            "lease_epoch": int(max(1, int(lease_epoch))),
            "lease_owner_id": f"sidecar:{runtime_ref}",
            "process_fingerprint": process_fingerprint,
            "launch_event_id": str(launch_result_ref),
            "process_started_at_utc": str(started_at_utc or ""),
            "runtime_ref": str(runtime_ref),
        },
        producer="loop_product.child_supervision_sidecar.runtime_marker",
    )
    canonical_event: dict[str, Any] | None = None
    if str(liveness_outcome.get("outcome") or "") == "committed":
        try:
            from loop_product.runtime.lifecycle import synchronize_authoritative_state_from_launch_result_ref

            synchronize_authoritative_state_from_launch_result_ref(result_ref=launch_result_ref)
        except Exception:
            pass
        canonical_event = commit_supervision_attached_event(
            state_root,
            observation_id=_supervision_attached_observation_id(
                launch_result_ref=launch_result_ref,
                runtime_ref=runtime_ref,
                node_id=node_id,
                pid=os.getpid(),
                lease_epoch=lease_epoch,
                started_at_utc=started_at_utc,
                process_fingerprint=process_fingerprint,
            ),
            node_id=node_id,
            payload={
                "observed_at": str(started_at_utc or _now_iso()),
                "lease_epoch": int(max(1, int(lease_epoch))),
                "lease_owner_id": f"sidecar:{runtime_ref}",
                "pid": int(os.getpid()),
                "process_fingerprint": process_fingerprint,
                "launch_event_id": str(launch_result_ref),
                "process_started_at_utc": str(started_at_utc or ""),
                "runtime_ref": str(runtime_ref),
                "evidence_refs": [str(runtime_ref), str(launch_result_ref)],
            },
            producer="loop_product.child_supervision_sidecar.runtime_marker",
        )
    return {
        **dict(liveness_outcome or {}),
        "supervision_attached_event": canonical_event,
    }


def _mirror_sidecar_runtime_supervision_detached_event(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    started_at_utc: str,
    lease_epoch: int,
    raw_argv: list[str],
) -> dict[str, Any] | None:
    try:
        launch_payload = _load_launch_payload(launch_result_ref)
        state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
    except Exception:
        return None
    node_id = str(launch_payload.get("node_id") or "").strip()
    if not node_id:
        return None
    process_fingerprint = _argv_process_fingerprint(raw_argv)
    observed_at = _now_iso()
    result_ref = supervision_result_ref(launch_result_ref=launch_result_ref)
    payload = {
        "observed_at": observed_at,
        "detachment_kind": "runtime_marker_exit",
        "lease_epoch": int(max(1, int(lease_epoch))),
        "lease_owner_id": f"sidecar:{runtime_ref}",
        "pid": int(os.getpid()),
        "process_fingerprint": process_fingerprint,
        "launch_event_id": str(launch_result_ref),
        "process_started_at_utc": str(started_at_utc or ""),
        "runtime_ref": str(runtime_ref),
        "supervision_result_ref": str(result_ref),
        "summary": "committed child supervision sidecar detached",
        "evidence_refs": [str(runtime_ref), str(launch_result_ref), str(result_ref)],
    }
    return commit_supervision_detached_event(
        state_root,
        observation_id=_supervision_detached_observation_id(
            launch_result_ref=launch_result_ref,
            runtime_ref=runtime_ref,
            node_id=node_id,
            lease_epoch=lease_epoch,
        ),
        node_id=node_id,
        payload=payload,
        producer="loop_product.child_supervision_sidecar.runtime_marker",
    )


def _sidecar_status_liveness_observation_id(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    status_payload: dict[str, Any],
    lease_epoch: int,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "launch_result_ref": str(launch_result_ref),
                "runtime_ref": str(runtime_ref),
                "status_result_ref": str(status_payload.get("status_result_ref") or ""),
                "runtime_attachment_state": str(status_payload.get("runtime_attachment_state") or ""),
                "runtime_observed_at": str(status_payload.get("runtime_observed_at") or ""),
                "lease_epoch": int(lease_epoch),
                "process_fingerprint": str(process_fingerprint or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return f"sidecar_heartbeat::{digest.hexdigest()[:16]}"


def _heartbeat_observed_observation_id(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    status_payload: dict[str, Any],
    lease_epoch: int,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    digest.update(
        json.dumps(
            {
                "launch_result_ref": str(launch_result_ref),
                "runtime_ref": str(runtime_ref),
                "status_result_ref": str(status_payload.get("status_result_ref") or ""),
                "runtime_attachment_state": str(status_payload.get("runtime_attachment_state") or ""),
                "runtime_observed_at": str(status_payload.get("runtime_observed_at") or ""),
                "lease_epoch": int(lease_epoch),
                "process_fingerprint": str(process_fingerprint or ""),
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    return f"heartbeat_observed::{digest.hexdigest()[:16]}"


def _build_sidecar_status_observer(
    *,
    launch_result_ref: Path,
    runtime_ref: Path,
    started_at_utc: str,
    lease_epoch: int,
    raw_argv: list[str],
) -> Callable[[dict[str, Any]], None]:
    process_fingerprint = _argv_process_fingerprint(raw_argv)
    launch_identity = str(launch_result_ref)
    lease_owner_id = f"sidecar:{runtime_ref}"
    try:
        launch_payload = _load_launch_payload(launch_result_ref)
        state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
        node_id = str(launch_payload.get("node_id") or "").strip()
    except Exception:
        state_root = None
        node_id = ""

    def _observer(status_payload: dict[str, Any]) -> None:
        if state_root is None or not node_id:
            return
        payload = dict(status_payload or {})
        attachment_state = str(payload.get("runtime_attachment_state") or "").strip().upper()
        observed_at = str(payload.get("runtime_observed_at") or "").strip() or _now_iso()
        status_result_ref = str(payload.get("status_result_ref") or "").strip()
        evidence_refs = [str(item) for item in list(payload.get("runtime_evidence_refs") or []) if str(item)]
        for candidate in (status_result_ref, str(runtime_ref), str(launch_result_ref)):
            if candidate and candidate not in evidence_refs:
                evidence_refs.append(candidate)
        mirror_outcome = commit_guarded_runtime_liveness_observation_event(
            state_root,
            observation_id=_sidecar_status_liveness_observation_id(
                launch_result_ref=launch_result_ref,
                runtime_ref=runtime_ref,
                status_payload=payload,
                lease_epoch=lease_epoch,
                process_fingerprint=process_fingerprint,
            ),
            node_id=node_id,
            payload={
                "attachment_state": attachment_state or "UNOBSERVED",
                "observation_kind": (
                    "child_supervision_heartbeat"
                    if attachment_state == "ATTACHED"
                    else str(payload.get("runtime_observation_kind") or "").strip() or "child_supervision_status"
                ),
                "observed_at": observed_at,
                "summary": str(payload.get("runtime_summary") or "committed child supervision sidecar heartbeat"),
                "evidence_refs": evidence_refs,
                "pid_alive": bool(payload.get("pid_alive", attachment_state == "ATTACHED")),
                "pid": int(os.getpid()),
                "lease_duration_s": 60.0,
                "lease_epoch": int(max(1, int(lease_epoch))),
                "lease_owner_id": lease_owner_id,
                "process_fingerprint": process_fingerprint,
                "launch_event_id": launch_identity,
                "process_started_at_utc": str(started_at_utc or ""),
                "runtime_ref": str(runtime_ref),
                "status_result_ref": status_result_ref,
            },
            producer="loop_product.child_supervision_sidecar.heartbeat",
        )
        if str(mirror_outcome.get("outcome") or "") == "committed":
            try:
                from loop_product.runtime.lifecycle import synchronize_authoritative_state_from_launch_result_ref

                synchronize_authoritative_state_from_launch_result_ref(result_ref=launch_result_ref)
            except Exception:
                pass
        if str(mirror_outcome.get("outcome") or "") == "committed" and attachment_state == "ATTACHED":
            commit_heartbeat_observed_event(
                state_root,
                observation_id=_heartbeat_observed_observation_id(
                    launch_result_ref=launch_result_ref,
                    runtime_ref=runtime_ref,
                    status_payload=payload,
                    lease_epoch=lease_epoch,
                    process_fingerprint=process_fingerprint,
                ),
                node_id=node_id,
                payload={
                    "observed_at": observed_at,
                    "lease_epoch": int(max(1, int(lease_epoch))),
                    "lease_owner_id": lease_owner_id,
                    "pid": int(os.getpid()),
                    "process_fingerprint": process_fingerprint,
                    "launch_event_id": launch_identity,
                    "process_started_at_utc": str(started_at_utc or ""),
                    "runtime_ref": str(runtime_ref),
                    "status_result_ref": status_result_ref,
                    "summary": str(payload.get("runtime_summary") or "committed child supervision sidecar heartbeat"),
                    "evidence_refs": evidence_refs,
                },
                producer="loop_product.child_supervision_sidecar.heartbeat",
            )
        if str(mirror_outcome.get("outcome") or "") != "committed":
            print(
                "[child-supervision-sidecar] warning: guarded heartbeat mirror rejected "
                f"with outcome={mirror_outcome.get('outcome')}"
            )

    return _observer


def _acquire_spawn_gate(
    *,
    launch_result_ref: Path,
    startup_timeout_s: float,
) -> tuple[Path | None, dict[str, Any] | None]:
    lock_ref = supervision_spawn_lock_ref(launch_result_ref=launch_result_ref)
    deadline = time.monotonic() + max(1.0, float(startup_timeout_s))
    while time.monotonic() < deadline:
        existing = live_child_supervision_runtime(launch_result_ref=launch_result_ref)
        if existing is not None:
            return None, existing
        try:
            fd = os.open(lock_ref, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            payload = _load_spawn_lock_payload(lock_ref)
            owner_pid = int(payload.get("pid") or 0)
            if owner_pid > 0 and not _pid_alive(owner_pid):
                lock_ref.unlink(missing_ok=True)
                continue
            time.sleep(0.05)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "schema": "loop_product.child_supervision_spawn_lock",
                    "launch_result_ref": str(launch_result_ref),
                    "pid": os.getpid(),
                    "started_at_utc": _now_iso(),
                },
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
        return lock_ref, None
    raise TimeoutError(f"timed out waiting to acquire child supervision spawn gate at {lock_ref}")


def _acquire_repo_supervision_spawn_gate(
    *,
    repo_supervision_root: Path,
    startup_timeout_s: float,
) -> tuple[Path | None, dict[str, Any] | None]:
    lock_ref = repo_supervision_reactor_spawn_lock_ref(repo_supervision_root=repo_supervision_root)
    deadline = time.monotonic() + max(1.0, float(startup_timeout_s))
    while time.monotonic() < deadline:
        existing = live_repo_supervision_reactor_runtime(repo_supervision_root=repo_supervision_root)
        if existing is not None:
            return None, existing
        try:
            fd = os.open(lock_ref, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            payload = _load_spawn_lock_payload(lock_ref)
            owner_pid = int(payload.get("pid") or 0)
            if owner_pid > 0 and not _pid_alive(owner_pid):
                lock_ref.unlink(missing_ok=True)
                continue
            time.sleep(0.05)
            continue
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "schema": "loop_product.repo_scoped_child_supervision_spawn_lock",
                    "repo_supervision_root": str(repo_supervision_root),
                    "pid": os.getpid(),
                    "started_at_utc": _now_iso(),
                },
                handle,
                indent=2,
                sort_keys=True,
            )
            handle.write("\n")
        return lock_ref, None
    raise TimeoutError(f"timed out waiting to acquire repo supervision spawn gate at {lock_ref}")


@contextmanager
def _runtime_marker(
    *,
    launch_result_ref: Path,
    lease_epoch: int,
    raw_argv: list[str],
    runtime_kind: str = "child_supervision_runtime",
    repo_supervision_root: Path | None = None,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
) -> Iterator[Path]:
    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_result_ref)
    launch_payload = _load_launch_payload(launch_result_ref)
    state_root = require_runtime_root(Path(str(launch_payload.get("state_root") or "")).expanduser().resolve())
    root_identity_token = str(
        launch_payload.get("runtime_root_identity_token") or runtime_root_identity_token(state_root, ensure=True)
    )
    started_at_utc = _now_iso()
    _write_json(
        runtime_ref,
        {
            "schema": "loop_product.child_supervision_runtime",
            "launch_result_ref": str(launch_result_ref),
            "runtime_root_identity_token": root_identity_token,
            "runtime_kind": str(runtime_kind or "child_supervision_runtime"),
            **(
                {"repo_supervision_root": str(repo_supervision_root)}
                if repo_supervision_root is not None
                else {}
            ),
            "pid": os.getpid(),
            "poll_interval_s": float(poll_interval_s),
            "stall_threshold_s": float(stall_threshold_s),
            "max_recoveries": int(max_recoveries),
            "no_substantive_progress_window_s": float(no_substantive_progress_window_s),
            "lease_epoch": int(max(1, int(lease_epoch))),
            "started_at_utc": started_at_utc,
            "log_ref": str(supervision_log_ref(launch_result_ref=launch_result_ref)),
            "result_ref": str(supervision_result_ref(launch_result_ref=launch_result_ref)),
        },
    )
    mirror_outcome = _mirror_sidecar_runtime_liveness_attached_event(
        launch_result_ref=launch_result_ref,
        runtime_ref=runtime_ref,
        started_at_utc=started_at_utc,
        lease_epoch=int(max(1, int(lease_epoch))),
        raw_argv=raw_argv,
    )
    attached_committed = str(dict(mirror_outcome or {}).get("outcome") or "") == "committed"
    if mirror_outcome is not None and str(mirror_outcome.get("outcome") or "") != "committed":
        print(
            "[child-supervision-sidecar] warning: guarded liveness mirror rejected "
            f"with outcome={mirror_outcome.get('outcome')}"
        )
    try:
        yield runtime_ref
    finally:
        if attached_committed:
            try:
                _mirror_sidecar_runtime_supervision_detached_event(
                    launch_result_ref=launch_result_ref,
                    runtime_ref=runtime_ref,
                    started_at_utc=started_at_utc,
                    lease_epoch=int(max(1, int(lease_epoch))),
                    raw_argv=raw_argv,
                )
            except Exception as exc:
                print(f"[child-supervision-sidecar] warning: supervision_detached mirror failed: {exc}")
        runtime_ref.unlink(missing_ok=True)


@contextmanager
def _repo_supervision_reactor_runtime_marker(
    *,
    repo_supervision_root: Path,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
) -> Iterator[Path]:
    runtime_ref = repo_supervision_reactor_runtime_ref(repo_supervision_root=repo_supervision_root)
    started_at_utc = _now_iso()
    _write_json(
        runtime_ref,
        _repo_supervision_runtime_payload(
            repo_supervision_root=repo_supervision_root,
            poll_interval_s=poll_interval_s,
            stall_threshold_s=stall_threshold_s,
            max_recoveries=max_recoveries,
            no_substantive_progress_window_s=no_substantive_progress_window_s,
            tracked_launch_refs=[],
            started_at_utc=started_at_utc,
        ),
    )
    try:
        yield runtime_ref
    finally:
        runtime_ref.unlink(missing_ok=True)


def _update_repo_supervision_reactor_runtime(
    *,
    repo_supervision_root: Path,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
    tracked_launch_refs: list[str],
) -> dict[str, Any]:
    runtime_ref = repo_supervision_reactor_runtime_ref(repo_supervision_root=repo_supervision_root)
    current_started_at_utc = ""
    if runtime_ref.exists():
        try:
            current_started_at_utc = str(_load_runtime_payload(runtime_ref).get("started_at_utc") or "")
        except Exception:
            current_started_at_utc = ""
    payload = _repo_supervision_runtime_payload(
        repo_supervision_root=repo_supervision_root,
        poll_interval_s=poll_interval_s,
        stall_threshold_s=stall_threshold_s,
        max_recoveries=max_recoveries,
        no_substantive_progress_window_s=no_substantive_progress_window_s,
        tracked_launch_refs=tracked_launch_refs,
        started_at_utc=current_started_at_utc or _now_iso(),
    )
    _write_json(runtime_ref, payload)
    return payload


def _run_single_child_supervision(
    *,
    launch_path: Path,
    lease_epoch: int,
    raw_argv: list[str],
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
    repo_supervision_root: Path | None = None,
) -> dict[str, Any] | None:
    runtime_kind = "repo_scoped_child_supervision" if repo_supervision_root is not None else "child_supervision_runtime"
    with _runtime_marker(
        launch_result_ref=launch_path,
        lease_epoch=max(1, int(lease_epoch)),
        raw_argv=raw_argv,
        runtime_kind=runtime_kind,
        repo_supervision_root=repo_supervision_root,
        poll_interval_s=max(0.05, float(poll_interval_s)),
        stall_threshold_s=max(0.0, float(stall_threshold_s)),
        max_recoveries=max(0, int(max_recoveries)),
        no_substantive_progress_window_s=max(0.0, float(no_substantive_progress_window_s)),
    ):
        runtime_ref = supervision_runtime_ref(launch_result_ref=launch_path)
        status_observer = _build_sidecar_status_observer(
            launch_result_ref=launch_path,
            runtime_ref=runtime_ref,
            started_at_utc=_load_runtime_payload(runtime_ref).get("started_at_utc", ""),
            lease_epoch=max(1, int(lease_epoch)),
            raw_argv=raw_argv,
        )
        try:
            result = supervise_child_until_settled(
                launch_result_ref=launch_path,
                poll_interval_s=max(0.05, float(poll_interval_s)),
                stall_threshold_s=max(0.0, float(stall_threshold_s)),
                max_recoveries=max(0, int(max_recoveries)),
                no_substantive_progress_window_s=max(0.0, float(no_substantive_progress_window_s)),
                status_observer=status_observer,
            )
        except RuntimeContextMissingError as exc:
            print(f"[child-supervision-sidecar] info: {exc}; retiring detached supervision worker")
            return None
        _, sync_error = persist_committed_supervision_result(
            launch_result_ref=launch_path,
            result_payload=result,
        )
        if sync_error:
            print(f"[child-supervision-sidecar] warning: state sync after settled supervision failed: {sync_error}")
        print(json.dumps(result, indent=2, sort_keys=True))
        return result


def _run_repo_scoped_supervision_reactor(
    *,
    repo_supervision_root: Path,
    poll_interval_s: float,
    stall_threshold_s: float,
    max_recoveries: int,
    no_substantive_progress_window_s: float,
    raw_argv: list[str],
) -> int:
    workers: dict[str, threading.Thread] = {}
    previous_tracked_launch_refs: list[str] | None = None
    last_runtime_update_monotonic = 0.0
    empty_registry_since_monotonic: float | None = None
    with _repo_supervision_reactor_runtime_marker(
        repo_supervision_root=repo_supervision_root,
        poll_interval_s=poll_interval_s,
        stall_threshold_s=stall_threshold_s,
        max_recoveries=max_recoveries,
        no_substantive_progress_window_s=no_substantive_progress_window_s,
    ):
        while True:
            registry = reconcile_repo_supervision_registry(repo_supervision_root=repo_supervision_root)
            tracked_launches = dict(registry.get("tracked_launches") or {})
            tracked_launch_refs = sorted(str(item) for item in tracked_launches.keys())
            if tracked_launch_refs:
                empty_registry_since_monotonic = None
            had_worker_state_change = False
            for launch_result_ref, entry in tracked_launches.items():
                if launch_result_ref in workers and workers[launch_result_ref].is_alive():
                    continue
                launch_path = Path(str(launch_result_ref)).expanduser().resolve()
                if supervision_result_ref(launch_result_ref=launch_path).exists():
                    continue
                worker = threading.Thread(
                    target=_run_single_child_supervision,
                    kwargs={
                        "launch_path": launch_path,
                        "lease_epoch": max(
                            1,
                            reserve_supervision_lease_epoch(launch_result_ref=launch_path),
                        ),
                        "raw_argv": raw_argv,
                        "poll_interval_s": float(entry.get("poll_interval_s") or poll_interval_s),
                        "stall_threshold_s": float(entry.get("stall_threshold_s") or stall_threshold_s),
                        "max_recoveries": int(entry.get("max_recoveries") or max_recoveries),
                        "no_substantive_progress_window_s": float(
                            entry.get("no_substantive_progress_window_s") or no_substantive_progress_window_s
                        ),
                        "repo_supervision_root": repo_supervision_root,
                    },
                    daemon=True,
                    name=f"repo-supervision::{launch_path.name}",
                )
                workers[launch_result_ref] = worker
                worker.start()
                had_worker_state_change = True
            completed = [launch_ref for launch_ref, worker in workers.items() if not worker.is_alive()]
            for launch_ref in completed:
                workers.pop(launch_ref, None)
                had_worker_state_change = True
            now_monotonic = time.monotonic()
            if not tracked_launch_refs and empty_registry_since_monotonic is None:
                empty_registry_since_monotonic = now_monotonic
            if _should_refresh_repo_supervision_runtime(
                tracked_launch_refs=tracked_launch_refs,
                previous_tracked_launch_refs=previous_tracked_launch_refs,
                had_worker_state_change=had_worker_state_change,
                now_monotonic=now_monotonic,
                last_runtime_update_monotonic=last_runtime_update_monotonic,
                heartbeat_interval_s=_REPO_SUPERVISION_RUNTIME_HEARTBEAT_S,
            ):
                _update_repo_supervision_reactor_runtime(
                    repo_supervision_root=repo_supervision_root,
                    poll_interval_s=poll_interval_s,
                    stall_threshold_s=stall_threshold_s,
                    max_recoveries=max_recoveries,
                    no_substantive_progress_window_s=no_substantive_progress_window_s,
                    tracked_launch_refs=tracked_launch_refs,
                )
                last_runtime_update_monotonic = now_monotonic
            if _repo_supervision_reactor_should_exit(
                tracked_launch_refs=tracked_launch_refs,
                worker_count=len(workers),
                empty_registry_since_monotonic=empty_registry_since_monotonic,
                now_monotonic=now_monotonic,
                orphan_worker_grace_s=_REPO_SUPERVISION_ORPHAN_WORKER_GRACE_S,
            ):
                break
            sleep_s = _repo_supervision_reactor_sleep_s(
                base_poll_interval_s=poll_interval_s,
                tracked_launch_refs=tracked_launch_refs,
                previous_tracked_launch_refs=previous_tracked_launch_refs,
                worker_count=len(workers),
                had_worker_state_change=had_worker_state_change,
            )
            previous_tracked_launch_refs = list(tracked_launch_refs)
            time.sleep(sleep_s)
    return 0


def ensure_child_supervision_running(
    *,
    launch_result_ref: str | Path,
    poll_interval_s: float = _DEFAULT_POLL_INTERVAL_S,
    stall_threshold_s: float = _DEFAULT_STALL_THRESHOLD_S,
    max_recoveries: int = _DEFAULT_MAX_RECOVERIES,
    no_substantive_progress_window_s: float = _DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S,
    startup_timeout_s: float = _STARTUP_TIMEOUT_S,
) -> dict[str, Any]:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    existing = live_child_supervision_runtime(launch_result_ref=launch_path)
    if existing is not None:
        return existing

    lock_ref, existing = _acquire_spawn_gate(
        launch_result_ref=launch_path,
        startup_timeout_s=startup_timeout_s,
    )
    if existing is not None:
        return existing

    runtime_ref = supervision_runtime_ref(launch_result_ref=launch_path)
    result_ref = supervision_result_ref(launch_result_ref=launch_path)
    repo_supervision_root = repo_supervision_reactor_root(launch_result_ref=launch_path)
    package_repo_root = Path(__file__).resolve().parents[1]

    try:
        existing = live_child_supervision_runtime(launch_result_ref=launch_path)
        if existing is not None:
            return existing

        register_launch_for_repo_supervision(
            launch_result_ref=launch_path,
            poll_interval_s=poll_interval_s,
            stall_threshold_s=stall_threshold_s,
            max_recoveries=max_recoveries,
            no_substantive_progress_window_s=no_substantive_progress_window_s,
        )
        repo_lock_ref, repo_runtime = _acquire_repo_supervision_spawn_gate(
            repo_supervision_root=repo_supervision_root,
            startup_timeout_s=startup_timeout_s,
        )
        proc = None
        try:
            if repo_runtime is None:
                log_ref = repo_supervision_reactor_log_ref(repo_supervision_root=repo_supervision_root)
                with log_ref.open("a", encoding="utf-8") as log_handle:
                    proc = subprocess.Popen(
                        [
                            sys.executable,
                            "-m",
                            "loop_product.child_supervision_sidecar",
                            "--run-repo-supervision-reactor",
                            "--repo-supervision-root",
                            str(repo_supervision_root),
                            "--poll-interval-s",
                            str(max(0.05, float(poll_interval_s))),
                            "--stall-threshold-s",
                            str(max(0.0, float(stall_threshold_s))),
                            "--max-recoveries",
                            str(max(0, int(max_recoveries))),
                            "--no-substantive-progress-window-s",
                            str(max(0.0, float(no_substantive_progress_window_s))),
                        ],
                        cwd=str(package_repo_root),
                        stdin=subprocess.DEVNULL,
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                        close_fds=True,
                    )
        finally:
            if repo_lock_ref is not None:
                repo_lock_ref.unlink(missing_ok=True)

        deadline = time.monotonic() + max(1.0, float(startup_timeout_s))
        while time.monotonic() < deadline:
            payload = live_child_supervision_runtime(launch_result_ref=launch_path)
            if payload is not None:
                return payload
            reactor_payload = live_repo_supervision_reactor_runtime(repo_supervision_root=repo_supervision_root)
            if proc is not None:
                exit_code = proc.poll()
            elif reactor_payload is not None:
                exit_code = None
            else:
                exit_code = 0
            if exit_code is not None:
                if exit_code == 0 and result_ref.exists():
                    return {
                        "schema": "loop_product.child_supervision_runtime",
                        "launch_result_ref": str(launch_path),
                        "pid": 0,
                        "status": "settled_immediately",
                        "log_ref": str(repo_supervision_reactor_log_ref(repo_supervision_root=repo_supervision_root)),
                        "result_ref": str(result_ref),
                        "runtime_ref": str(runtime_ref),
                    }
                log_ref = repo_supervision_reactor_log_ref(repo_supervision_root=repo_supervision_root)
                detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
                raise RuntimeError(detail or f"repo-scoped child supervision reactor exited early with code {exit_code}")
            time.sleep(0.05)
        raise TimeoutError(f"timed out waiting for child supervision runtime marker at {runtime_ref}")
    finally:
        if lock_ref is not None:
            lock_ref.unlink(missing_ok=True)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Detached committed child supervision sidecar")
    parser.add_argument("--launch-result-ref", help="Path to a ChildLaunchResult.json")
    parser.add_argument("--run-repo-supervision-reactor", action="store_true")
    parser.add_argument("--repo-supervision-root", help="Path to a repo-scoped child supervision reactor root")
    parser.add_argument("--lease-epoch", type=int, default=1)
    parser.add_argument("--poll-interval-s", type=float, default=_DEFAULT_POLL_INTERVAL_S)
    parser.add_argument("--stall-threshold-s", type=float, default=_DEFAULT_STALL_THRESHOLD_S)
    parser.add_argument("--max-recoveries", type=int, default=_DEFAULT_MAX_RECOVERIES)
    parser.add_argument("--no-substantive-progress-window-s", type=float, default=_DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    raw_argv = list(argv) if argv is not None else list(sys.argv[1:])
    if args.run_repo_supervision_reactor:
        repo_supervision_root_raw = str(args.repo_supervision_root or "").strip()
        if not repo_supervision_root_raw:
            parser.error("--repo-supervision-root is required with --run-repo-supervision-reactor")
        return _run_repo_scoped_supervision_reactor(
            repo_supervision_root=Path(repo_supervision_root_raw).expanduser().resolve(),
            poll_interval_s=max(0.05, float(args.poll_interval_s)),
            stall_threshold_s=max(0.0, float(args.stall_threshold_s)),
            max_recoveries=max(0, int(args.max_recoveries)),
            no_substantive_progress_window_s=max(0.0, float(args.no_substantive_progress_window_s)),
            raw_argv=raw_argv,
        )
    if not args.launch_result_ref:
        parser.error("--launch-result-ref is required unless --run-repo-supervision-reactor is set")
    launch_path = Path(args.launch_result_ref).expanduser().resolve()
    _run_single_child_supervision(
        launch_path=launch_path,
        lease_epoch=max(1, int(args.lease_epoch)),
        raw_argv=raw_argv,
        poll_interval_s=max(0.05, float(args.poll_interval_s)),
        stall_threshold_s=max(0.0, float(args.stall_threshold_s)),
        max_recoveries=max(0, int(args.max_recoveries)),
        no_substantive_progress_window_s=max(0.0, float(args.no_substantive_progress_window_s)),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
