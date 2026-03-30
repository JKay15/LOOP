"""Trusted repo control-plane bootstrap and service-loop surfaces."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from loop_product import host_child_launch_supervisor as supervisor_module
from loop_product import evaluator_authority
from loop_product.event_journal import commit_repo_control_plane_required_event
from loop_product.event_journal import commit_repo_reactor_required_event
from loop_product.event_journal import commit_repo_reactor_residency_required_event
from loop_product.loop import evaluator_supervision as evaluator_supervision_module
from loop_product.runtime.cleanup_authority import commit_runtime_root_quiesced_from_cleanup
from loop_product.runtime.cleanup_authority import commit_test_runtime_cleanup_authority
from loop_product.runtime.cleanup_authority import repo_root_from_runtime_root
from loop_product.runtime.cleanup_authority import runtime_cleanup_retirement_authority
from loop_product.runtime.gc import ensure_housekeeping_reap_controller_service_running
from loop_product.runtime.gc import _housekeeping_controller_process_matches
from loop_product.runtime.gc import housekeeping_runtime_root
from loop_product.runtime.gc import live_housekeeping_reap_controller_runtime
from loop_product.runtime_paths import require_runtime_root
from loop_product.state_io import write_json_read_only

_DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S = 0.5
_DEFAULT_REPO_CONTROL_PLANE_SERVICE_IDLE_EXIT_AFTER_S = 0.0
_REPO_CONTROL_PLANE_STARTUP_TIMEOUT_S = 5.0
_REPO_CONTROL_PLANE_AUTHORITY_REUSE_SETTLE_S = 0.1
_DEFAULT_REPO_REACTOR_POLL_INTERVAL_S = 0.5
_DEFAULT_REPO_REACTOR_SERVICE_IDLE_EXIT_AFTER_S = 0.0
_REPO_REACTOR_STARTUP_TIMEOUT_S = 5.0
_DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_POLL_INTERVAL_S = 0.5
_DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_SERVICE_IDLE_EXIT_AFTER_S = 0.0
_REPO_REACTOR_RESIDENCY_GUARD_STARTUP_TIMEOUT_S = 5.0
REPO_CONTROL_PLANE_NODE_ID = "repo_control_plane"
REPO_REACTOR_NODE_ID = "repo_reactor"
REPO_REACTOR_RESIDENCY_GUARD_NODE_ID = "repo_reactor_residency_guard"
_REPO_GLOBAL_HEAVY_OBJECT_AUTO_AUDIT_OBJECT_KINDS = ("mathlib_pack", "publish_tree", "runtime_heavy_tree")
_REPO_GLOBAL_HEAVY_OBJECT_AUTO_AUDIT_ROUND_ID = "R-repo-global-heavy-object-audit"
_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID = "R-repo-global-heavy-object-lifecycle"


def _repo_root(repo_root: str | Path) -> Path:
    return Path(repo_root).expanduser().resolve()


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


def _command_has_flag(command_line: str, flag: str) -> bool:
    return str(flag or "").strip() in _command_tokens(command_line)


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


def _command_repo_root(command_line: str) -> Path | None:
    raw_repo_root = _command_flag_value(command_line, "--repo-root")
    if not raw_repo_root:
        tokens = _command_tokens(command_line)
        if "run_repo_control_plane_loop" in str(command_line or "") and len(tokens) >= 3:
            try:
                float(tokens[-2])
                float(tokens[-1])
            except (TypeError, ValueError):
                pass
            else:
                raw_repo_root = str(tokens[-3] or "").strip()
    if not raw_repo_root:
        return None
    try:
        return _repo_root(raw_repo_root)
    except Exception:
        return None


def _command_repo_root_matches(command_line: str, repo_root: str | Path) -> bool:
    command_repo_root = _command_repo_root(command_line)
    if command_repo_root is None:
        return False
    return command_repo_root == _repo_root(repo_root)


def _command_is_repo_reactor_residency_guard(command_line: str) -> bool:
    return "loop_product.runtime.control_plane" in str(command_line or "") and _command_has_flag(
        command_line,
        "--run-repo-reactor-residency-guard",
    )


def _command_is_repo_reactor(command_line: str) -> bool:
    return "loop_product.runtime.control_plane" in str(command_line or "") and _command_has_flag(
        command_line,
        "--run-repo-reactor",
    )


def _command_is_repo_control_plane(command_line: str) -> bool:
    command = str(command_line or "")
    return (
        "loop_product.runtime.control_plane" in command
        and not _command_is_repo_reactor(command)
        and not _command_is_repo_reactor_residency_guard(command)
    )


def _command_is_repo_evaluator_supervision(command_line: str) -> bool:
    return "loop_product.loop.evaluator_supervision" in str(command_line or "") and _command_has_flag(
        command_line,
        "--run-repo-evaluator-supervision-reactor",
    )


def _command_repo_evaluator_supervision_matches(command_line: str, repo_root: str | Path) -> bool:
    expected_root = evaluator_supervision_module.repo_evaluator_supervision_root(
        state_root=repo_anchor_runtime_root(repo_root=repo_root)
    )
    observed_root = _command_flag_value(command_line, "--repo-evaluator-supervision-root")
    if not observed_root:
        return False
    try:
        return Path(observed_root).expanduser().resolve() == expected_root
    except Exception:
        return False


def _terminate_pid(pid: int, *, wait_s: float = 1.0) -> None:
    try:
        candidate = int(pid)
    except (TypeError, ValueError):
        return
    if candidate <= 0 or not _pid_alive(candidate):
        return
    try:
        os.kill(candidate, 15)
    except OSError:
        return
    deadline = time.monotonic() + max(0.0, float(wait_s))
    while time.monotonic() < deadline:
        if not _pid_alive(candidate):
            return
        time.sleep(0.05)
    try:
        os.kill(candidate, 9)
    except OSError:
        return


def _repo_service_processes_by_kind(*, repo_root: Path) -> dict[str, list[int]]:
    resolved_repo_root = _repo_root(repo_root)
    proc = subprocess.run(
        ["ps", "-axww", "-o", "pid=,command="],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return {}
    repo_marker = str(resolved_repo_root)
    matches: dict[str, set[int]] = {
        "repo_reactor_residency_guard": set(),
        "repo_reactor": set(),
        "repo_control_plane": set(),
        "repo_evaluator_supervision": set(),
        "host_child_launch_supervisor": set(),
        "housekeeping": set(),
    }
    for raw_line in str(proc.stdout or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        command = parts[1]
        if not _command_repo_root_matches(command, repo_marker):
            if _command_is_repo_evaluator_supervision(command) and _command_repo_evaluator_supervision_matches(
                command,
                repo_marker,
            ):
                matches["repo_evaluator_supervision"].add(pid)
            continue
        if _command_is_repo_reactor_residency_guard(command):
            matches["repo_reactor_residency_guard"].add(pid)
        elif _command_is_repo_reactor(command):
            matches["repo_reactor"].add(pid)
        elif _command_is_repo_control_plane(command):
            matches["repo_control_plane"].add(pid)
        elif "loop_product.host_child_launch_supervisor" in command:
            matches["host_child_launch_supervisor"].add(pid)
        elif "loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command:
            matches["housekeeping"].add(pid)
    return {
        kind: sorted(pids)
        for kind, pids in matches.items()
        if pids
    }


def _path_within(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _is_temp_test_repo_root(repo_root: Path) -> bool:
    resolved_repo_root = _repo_root(repo_root)
    temp_root = Path(tempfile.gettempdir()).expanduser().resolve()
    if not _path_within(temp_root, resolved_repo_root):
        return False
    return any(str(part or "").startswith("loop_system_") for part in resolved_repo_root.parts)


def _iter_temp_test_repo_roots(*, exclude_repo_roots: set[Path] | None = None) -> list[Path]:
    temp_root = Path(tempfile.gettempdir()).expanduser().resolve()
    excluded = {_repo_root(path) for path in set(exclude_repo_roots or set())}
    repo_roots: set[Path] = set()
    for entry in sorted(temp_root.glob("loop_system_*")):
        for candidate in (entry, entry / "loop_product_repo"):
            resolved_candidate = candidate.expanduser().resolve()
            if resolved_candidate in excluded:
                continue
            if not candidate.exists():
                continue
            if not _is_temp_test_repo_root(resolved_candidate):
                continue
            if _repo_service_processes_by_kind(repo_root=resolved_candidate):
                repo_roots.add(resolved_candidate)
    return sorted(repo_roots, key=str)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def repo_control_plane_runtime_root(*, repo_root: str | Path) -> Path:
    return (_repo_root(repo_root) / ".loop" / "repo_control_plane").resolve()


def repo_reactor_runtime_root(*, repo_root: str | Path) -> Path:
    return (_repo_root(repo_root) / ".loop" / "repo_reactor").resolve()


def repo_reactor_residency_guard_runtime_root(*, repo_root: str | Path) -> Path:
    return (_repo_root(repo_root) / ".loop" / "repo_reactor_residency_guard").resolve()


def repo_anchor_runtime_root(*, repo_root: str | Path) -> Path:
    return (_repo_root(repo_root) / ".loop").resolve()


def repo_control_plane_runtime_ref(*, repo_root: str | Path) -> Path:
    return repo_control_plane_runtime_root(repo_root=repo_root) / "RepoControlPlaneRuntime.json"


def repo_reactor_runtime_ref(*, repo_root: str | Path) -> Path:
    return repo_reactor_runtime_root(repo_root=repo_root) / "RepoReactorRuntime.json"


def repo_reactor_residency_guard_runtime_ref(*, repo_root: str | Path) -> Path:
    return repo_reactor_residency_guard_runtime_root(repo_root=repo_root) / "RepoReactorResidencyGuardRuntime.json"


def repo_control_plane_log_ref(*, repo_root: str | Path) -> Path:
    return repo_control_plane_runtime_root(repo_root=repo_root) / "RepoControlPlane.log"


def repo_reactor_log_ref(*, repo_root: str | Path) -> Path:
    return repo_reactor_runtime_root(repo_root=repo_root) / "RepoReactor.log"


def repo_reactor_residency_guard_log_ref(*, repo_root: str | Path) -> Path:
    return repo_reactor_residency_guard_runtime_root(repo_root=repo_root) / "RepoReactorResidencyGuard.log"


def _top_level_repo_root_from_runtime_root(*, state_root: str | Path) -> Path | None:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    if resolved_state_root.name == ".loop":
        return resolved_state_root.parent.resolve()
    if resolved_state_root.name in {
        repo_control_plane_runtime_root(repo_root=resolved_state_root.parent.parent).name,
        repo_reactor_runtime_root(repo_root=resolved_state_root.parent.parent).name,
        repo_reactor_residency_guard_runtime_root(repo_root=resolved_state_root.parent.parent).name,
        "housekeeping",
        "host_child_launch_supervisor",
    }:
        return None
    parent = resolved_state_root.parent
    if parent.name != ".loop":
        return None
    return parent.parent.resolve()


def _runtime_code_fingerprint() -> str:
    refs = [
        Path(__file__).resolve(),
        Path(__file__).resolve().parents[1] / "host_child_launch_supervisor.py",
        Path(__file__).resolve().parents[1] / "runtime" / "gc.py",
    ]
    digest = hashlib.sha256()
    for ref in refs:
        resolved = ref.resolve()
        digest.update(str(resolved).encode("utf-8"))
        digest.update(b"\0")
        digest.update(resolved.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _load_runtime_payload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _runtime_marker_matches_process(payload: dict[str, Any]) -> bool:
    pid = int(payload.get("pid") or 0)
    if pid <= 0 or not _pid_alive(pid):
        return False
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    repo_root = str(payload.get("repo_root") or "").strip()
    runtime_kind = str(payload.get("runtime_kind") or "").strip()
    if runtime_kind == "repo_reactor_residency_guard":
        return _command_is_repo_reactor_residency_guard(command_line) and _command_repo_root_matches(command_line, repo_root)
    if runtime_kind == "repo_reactor":
        return _command_is_repo_reactor(command_line) and _command_repo_root_matches(command_line, repo_root)
    return _command_is_repo_control_plane(command_line) and _command_repo_root_matches(command_line, repo_root)


def _repo_control_plane_process_matches(*, pid: int, repo_root: Path) -> bool:
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    return _command_is_repo_control_plane(command_line) and _command_repo_root_matches(command_line, repo_root)


def _repo_evaluator_supervision_process_matches(*, pid: int, repo_root: Path) -> bool:
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    return _command_is_repo_evaluator_supervision(command_line) and _command_repo_evaluator_supervision_matches(
        command_line,
        repo_root,
    )


def _repo_control_plane_process_fingerprint(*, pid: int) -> str:
    command_line = _pid_command_line(pid)
    if not command_line:
        return ""
    digest = hashlib.sha256()
    digest.update(command_line.encode("utf-8"))
    return digest.hexdigest()


def _repo_control_plane_launch_identity(*, repo_root: Path) -> str:
    return str(repo_control_plane_runtime_ref(repo_root=repo_root))


def _repo_control_plane_requirement_observation_id(
    *,
    repo_root: Path,
    trigger_kind: str,
    trigger_ref: str,
) -> str:
    raw = "\0".join(
        (
            "repo_control_plane_required",
            str(repo_root.resolve()),
            str(trigger_kind or "").strip(),
            str(trigger_ref or "").strip(),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _commit_repo_control_plane_requirement(
    *,
    repo_root: Path,
    trigger_kind: str,
    trigger_ref: str = "",
    producer: str,
) -> dict[str, Any]:
    trigger_kind_value = str(trigger_kind or "").strip()
    if not trigger_kind_value:
        raise ValueError("trigger_kind is required")
    trigger_ref_value = str(trigger_ref or "").strip()
    return commit_repo_control_plane_required_event(
        repo_control_plane_runtime_root(repo_root=repo_root),
        observation_id=_repo_control_plane_requirement_observation_id(
            repo_root=repo_root,
            trigger_kind=trigger_kind_value,
            trigger_ref=trigger_ref_value,
        ),
        node_id=REPO_CONTROL_PLANE_NODE_ID,
        payload={
            "repo_root": str(repo_root.resolve()),
            "required_service_kind": "repo_control_plane",
            "trigger_kind": trigger_kind_value,
            "trigger_ref": trigger_ref_value,
        },
        producer=producer,
    )


def _heavy_object_authority_gap_audit_signature(
    *,
    repo_root: Path,
    object_kind: str,
    unmanaged_candidates: list[dict[str, Any]],
) -> str:
    payload = {
        "repo_root": str(repo_root.resolve()),
        "object_kind": str(object_kind or "").strip(),
        "unmanaged_candidates": [
            {
                "object_ref": str(dict(item or {}).get("object_ref") or ""),
                "file_name": str(dict(item or {}).get("file_name") or ""),
                "byte_size": int(dict(item or {}).get("byte_size") or 0),
            }
            for item in list(unmanaged_candidates or [])
        ],
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _heavy_object_authority_gap_audit_requirement_observation_id(
    *,
    repo_root: Path,
    object_kind: str,
    audit_signature: str,
) -> str:
    raw = "\0".join(
        (
            "heavy_object_authority_gap_audit_required",
            str(repo_root.resolve()),
            str(object_kind or "").strip(),
            str(audit_signature or "").strip(),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _commit_heavy_object_authority_gap_audit_requirement(
    *,
    repo_root: Path,
    object_kind: str,
    audit_signature: str,
    producer: str,
) -> dict[str, Any]:
    from loop_product.event_journal import commit_heavy_object_authority_gap_audit_required_event

    normalized_object_kind = str(object_kind or "").strip()
    normalized_signature = str(audit_signature or "").strip()
    if not normalized_object_kind:
        raise ValueError("object_kind is required")
    if not normalized_signature:
        raise ValueError("audit_signature is required")
    return commit_heavy_object_authority_gap_audit_required_event(
        repo_control_plane_runtime_root(repo_root=repo_root),
        observation_id=_heavy_object_authority_gap_audit_requirement_observation_id(
            repo_root=repo_root,
            object_kind=normalized_object_kind,
            audit_signature=normalized_signature,
        ),
        node_id=REPO_CONTROL_PLANE_NODE_ID,
        payload={
            "repo_root": str(repo_root.resolve()),
            "required_service_kind": "heavy_object_authority_gap_audit",
            "object_kind": normalized_object_kind,
            "audit_signature": normalized_signature,
            "trigger_kind": "repo_control_plane_auto_audit",
            "trigger_ref": normalized_signature,
            "anchor_runtime_root": str(repo_anchor_runtime_root(repo_root=repo_root)),
        },
        producer=producer,
    )


def _heavy_object_authority_gap_repo_remediation_requirement_observation_id(
    *,
    repo_root: Path,
    object_kind: str,
    remediation_signature: str,
) -> str:
    raw = "\0".join(
        (
            "heavy_object_authority_gap_repo_remediation_required",
            str(repo_root.resolve()),
            str(object_kind or "").strip(),
            str(remediation_signature or "").strip(),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _commit_heavy_object_authority_gap_repo_remediation_requirement(
    *,
    repo_root: Path,
    object_kind: str,
    remediation_signature: str,
    producer: str,
    audit_result_event_id: str,
) -> dict[str, Any]:
    from loop_product.event_journal import commit_heavy_object_authority_gap_repo_remediation_required_event

    normalized_object_kind = str(object_kind or "").strip()
    normalized_signature = str(remediation_signature or "").strip()
    normalized_audit_result_event_id = str(audit_result_event_id or "").strip()
    if not normalized_object_kind:
        raise ValueError("object_kind is required")
    if not normalized_signature:
        raise ValueError("remediation_signature is required")
    return commit_heavy_object_authority_gap_repo_remediation_required_event(
        repo_control_plane_runtime_root(repo_root=repo_root),
        observation_id=_heavy_object_authority_gap_repo_remediation_requirement_observation_id(
            repo_root=repo_root,
            object_kind=normalized_object_kind,
            remediation_signature=normalized_signature,
        ),
        node_id=REPO_CONTROL_PLANE_NODE_ID,
        payload={
            "repo_root": str(repo_root.resolve()),
            "required_service_kind": "heavy_object_authority_gap_repo_remediation",
            "object_kind": normalized_object_kind,
            "remediation_signature": normalized_signature,
            "trigger_kind": "repo_control_plane_auto_remediation",
            "trigger_ref": normalized_signature,
            "audit_result_event_id": normalized_audit_result_event_id,
            "anchor_runtime_root": str(repo_anchor_runtime_root(repo_root=repo_root)),
        },
        producer=producer,
    )


def _repo_reactor_process_matches(*, pid: int, repo_root: Path) -> bool:
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    return _command_is_repo_reactor(command_line) and _command_repo_root_matches(command_line, repo_root)


def _repo_reactor_process_fingerprint(*, pid: int) -> str:
    command_line = _pid_command_line(pid)
    if not command_line:
        return ""
    digest = hashlib.sha256()
    digest.update(command_line.encode("utf-8"))
    return digest.hexdigest()


def _repo_reactor_launch_identity(*, repo_root: Path) -> str:
    return str(repo_reactor_runtime_ref(repo_root=repo_root))


def _repo_reactor_requirement_observation_id(
    *,
    repo_root: Path,
    trigger_kind: str,
    trigger_ref: str,
) -> str:
    raw = "\0".join(
        (
            "repo_reactor_required",
            str(repo_root.resolve()),
            str(trigger_kind or "").strip(),
            str(trigger_ref or "").strip(),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _commit_repo_reactor_requirement(
    *,
    repo_root: Path,
    trigger_kind: str,
    trigger_ref: str = "",
    producer: str,
) -> dict[str, Any]:
    trigger_kind_value = str(trigger_kind or "").strip()
    if not trigger_kind_value:
        raise ValueError("trigger_kind is required")
    trigger_ref_value = str(trigger_ref or "").strip()
    return commit_repo_reactor_required_event(
        repo_reactor_runtime_root(repo_root=repo_root),
        observation_id=_repo_reactor_requirement_observation_id(
            repo_root=repo_root,
            trigger_kind=trigger_kind_value,
            trigger_ref=trigger_ref_value,
        ),
        node_id=REPO_REACTOR_NODE_ID,
        payload={
            "repo_root": str(repo_root.resolve()),
            "required_service_kind": "repo_reactor",
            "trigger_kind": trigger_kind_value,
            "trigger_ref": trigger_ref_value,
        },
        producer=producer,
    )


def _repo_reactor_authority_payload(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    reactor_root = repo_reactor_runtime_root(repo_root=repo_root)
    effective = dict(
        dict(
            query_runtime_liveness_view(reactor_root, initialize_if_missing=False).get(
                "effective_runtime_liveness_by_node"
            )
            or {}
        ).get(
            REPO_REACTOR_NODE_ID
        )
        or {}
    )
    if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
        return None
    payload = dict(effective.get("payload") or {})
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return None
    if not _repo_reactor_process_matches(pid=pid, repo_root=repo_root):
        return None
    current_process_fingerprint = _repo_reactor_process_fingerprint(pid=pid)
    payload_process_fingerprint = str(payload.get("process_fingerprint") or "")
    if payload_process_fingerprint and current_process_fingerprint and payload_process_fingerprint != current_process_fingerprint:
        return None
    current_code_fingerprint = _runtime_code_fingerprint()
    payload_code_fingerprint = str(payload.get("code_fingerprint") or "")
    if payload_code_fingerprint and payload_code_fingerprint != current_code_fingerprint:
        return None
    launch_identity = str(payload.get("launch_event_id") or payload.get("launch_result_ref") or "").strip()
    expected_launch_identity = _repo_reactor_launch_identity(repo_root=repo_root)
    if launch_identity and launch_identity != expected_launch_identity:
        return None
    return {
        **payload,
        "pid": pid,
        "process_fingerprint": current_process_fingerprint or payload_process_fingerprint,
        "code_fingerprint": payload_code_fingerprint or current_code_fingerprint,
    }


def _materialize_repo_reactor_runtime_marker_from_authority(
    *,
    repo_root: Path,
    authority_payload: dict[str, Any],
) -> dict[str, Any]:
    runtime_ref = repo_reactor_runtime_ref(repo_root=repo_root)
    log_ref = repo_reactor_log_ref(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    pid = int(authority_payload.get("pid") or 0)
    marker_payload = {
        "schema": "loop_product.repo_reactor_runtime",
        "runtime_kind": "repo_reactor",
        "repo_root": str(repo_root),
        "pid": pid,
        "code_fingerprint": str(authority_payload.get("code_fingerprint") or _runtime_code_fingerprint()),
        "poll_interval_s": float(authority_payload.get("poll_interval_s") or _DEFAULT_REPO_REACTOR_POLL_INTERVAL_S),
        "idle_exit_after_s": float(
            authority_payload.get("idle_exit_after_s") or _DEFAULT_REPO_REACTOR_SERVICE_IDLE_EXIT_AFTER_S
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
    write_json_read_only(runtime_ref, marker_payload)
    return marker_payload


def _repo_reactor_next_lease_epoch(*, repo_root: Path) -> int:
    from loop_product.event_journal import iter_committed_events

    latest_epoch = 0
    reactor_root = repo_reactor_runtime_root(repo_root=repo_root)
    for event in iter_committed_events(reactor_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        if str(event.get("node_id") or "") != REPO_REACTOR_NODE_ID:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("attachment_state") or "").strip().upper() != "ATTACHED":
            continue
        try:
            latest_epoch = max(latest_epoch, int(payload.get("lease_epoch") or 0))
        except (TypeError, ValueError):
            continue
    return latest_epoch + 1 if latest_epoch > 0 else 1


def _repo_reactor_liveness_observation_id(
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
    return f"repo_reactor_liveness::{digest}"


def _repo_reactor_identity_observation_id(
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
    return f"repo_reactor_identity::{digest}"


def _repo_reactor_exit_observation_id(
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
    return f"repo_reactor_exit::{digest}"


def _repo_reactor_process_exit_observation_id(
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
    return f"repo_reactor_process_exit::{digest}"


def _emit_repo_reactor_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    heartbeat_ordinal: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
    process_started_at_utc: str,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_process_identity_confirmed_event,
    )

    pid = os.getpid()
    observed_at = _now_iso()
    process_fingerprint = _repo_reactor_process_fingerprint(pid=pid)
    launch_identity = _repo_reactor_launch_identity(repo_root=repo_root)
    runtime_ref = repo_reactor_runtime_ref(repo_root=repo_root)
    reactor_root = repo_reactor_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "repo_reactor",
        "attachment_state": "ATTACHED",
        "observed_at": observed_at,
        "lease_owner_id": f"repo-reactor:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "lease_duration_s": max(5.0, float(poll_interval_s) * 3.0),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "launch_result_ref": launch_identity,
        "launch_event_id": launch_identity,
        "process_started_at_utc": str(process_started_at_utc),
        "runtime_ref": str(runtime_ref),
    }
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        reactor_root,
        observation_id=_repo_reactor_liveness_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
        ),
        node_id=REPO_REACTOR_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_reactor_loop",
    )
    if heartbeat_ordinal == 0:
        commit_process_identity_confirmed_event(
            reactor_root,
            observation_id=_repo_reactor_identity_observation_id(
                repo_root=repo_root,
                pid=pid,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
                lease_epoch=lease_epoch,
                process_started_at_utc=process_started_at_utc,
            ),
            node_id=REPO_REACTOR_NODE_ID,
            payload={
                "controller_kind": "repo_reactor",
                "observed_at": observed_at,
                "pid": int(pid),
                "process_fingerprint": process_fingerprint,
                "code_fingerprint": _runtime_code_fingerprint(),
                "poll_interval_s": float(poll_interval_s),
                "idle_exit_after_s": float(idle_exit_after_s),
                "launch_result_ref": launch_identity,
                "launch_event_id": launch_identity,
                "process_started_at_utc": str(process_started_at_utc),
                "lease_epoch": int(lease_epoch),
                "lease_owner_id": f"repo-reactor:{runtime_ref}",
                "runtime_ref": str(runtime_ref),
            },
            producer="runtime.control_plane.run_repo_reactor_loop",
        )
    return dict(liveness_outcome or {})


def _emit_repo_reactor_exit_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
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
    process_fingerprint = _repo_reactor_process_fingerprint(pid=pid)
    launch_identity = _repo_reactor_launch_identity(repo_root=repo_root)
    runtime_ref = repo_reactor_runtime_ref(repo_root=repo_root)
    reactor_root = repo_reactor_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "repo_reactor",
        "attachment_state": "TERMINAL",
        "observation_kind": f"repo_reactor_exit:{str(exit_reason)}",
        "exit_reason": str(exit_reason),
        "observed_at": observed_at,
        "lease_owner_id": f"repo-reactor:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
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
        reactor_root,
        observation_id=_repo_reactor_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=REPO_REACTOR_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_reactor_loop",
    )
    process_exit_event = commit_process_exit_observed_event(
        reactor_root,
        observation_id=_repo_reactor_process_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=REPO_REACTOR_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_reactor_loop",
    )
    return {
        "liveness_outcome": dict(liveness_outcome or {}),
        "process_exit_event": dict(process_exit_event or {}),
    }


def _repo_reactor_should_self_fence(liveness_outcome: dict[str, Any] | None) -> bool:
    outcome = str(dict(liveness_outcome or {}).get("outcome") or "").strip()
    return outcome.startswith("rejected_")


def _repo_reactor_cleanup_authority(
    *,
    repo_root: Path,
) -> dict[str, Any] | None:
    return runtime_cleanup_retirement_authority(
        state_root=repo_reactor_runtime_root(repo_root=repo_root),
        repo_root=repo_root,
    )


def _repo_reactor_residency_guard_process_matches(*, pid: int, repo_root: Path) -> bool:
    command_line = _pid_command_line(pid)
    if not command_line:
        return False
    return _command_is_repo_reactor_residency_guard(command_line) and _command_repo_root_matches(command_line, repo_root)


def _repo_reactor_residency_guard_process_fingerprint(*, pid: int) -> str:
    command_line = _pid_command_line(pid)
    if not command_line:
        return ""
    digest = hashlib.sha256()
    digest.update(command_line.encode("utf-8"))
    return digest.hexdigest()


def _repo_reactor_residency_guard_launch_identity(*, repo_root: Path) -> str:
    return str(repo_reactor_residency_guard_runtime_ref(repo_root=repo_root))


def _repo_reactor_residency_requirement_observation_id(
    *,
    repo_root: Path,
    trigger_kind: str,
    trigger_ref: str,
) -> str:
    raw = "\0".join(
        (
            "repo_reactor_residency_required",
            str(repo_root.resolve()),
            str(trigger_kind or "").strip(),
            str(trigger_ref or "").strip(),
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _commit_repo_reactor_residency_requirement(
    *,
    repo_root: Path,
    trigger_kind: str,
    trigger_ref: str = "",
    producer: str,
) -> dict[str, Any]:
    trigger_kind_value = str(trigger_kind or "").strip()
    if not trigger_kind_value:
        raise ValueError("trigger_kind is required")
    trigger_ref_value = str(trigger_ref or "").strip()
    return commit_repo_reactor_residency_required_event(
        repo_reactor_residency_guard_runtime_root(repo_root=repo_root),
        observation_id=_repo_reactor_residency_requirement_observation_id(
            repo_root=repo_root,
            trigger_kind=trigger_kind_value,
            trigger_ref=trigger_ref_value,
        ),
        node_id=REPO_REACTOR_RESIDENCY_GUARD_NODE_ID,
        payload={
            "repo_root": str(repo_root.resolve()),
            "required_service_kind": "repo_reactor_residency_guard",
            "trigger_kind": trigger_kind_value,
            "trigger_ref": trigger_ref_value,
        },
        producer=producer,
    )


def _repo_reactor_residency_guard_authority_payload(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
    effective = dict(
        dict(
            query_runtime_liveness_view(guard_root, initialize_if_missing=False).get(
                "effective_runtime_liveness_by_node"
            )
            or {}
        ).get(
            REPO_REACTOR_RESIDENCY_GUARD_NODE_ID
        )
        or {}
    )
    if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
        return None
    payload = dict(effective.get("payload") or {})
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return None
    if not _repo_reactor_residency_guard_process_matches(pid=pid, repo_root=repo_root):
        return None
    current_process_fingerprint = _repo_reactor_residency_guard_process_fingerprint(pid=pid)
    payload_process_fingerprint = str(payload.get("process_fingerprint") or "")
    if payload_process_fingerprint and current_process_fingerprint and payload_process_fingerprint != current_process_fingerprint:
        return None
    current_code_fingerprint = _runtime_code_fingerprint()
    payload_code_fingerprint = str(payload.get("code_fingerprint") or "")
    if payload_code_fingerprint and payload_code_fingerprint != current_code_fingerprint:
        return None
    launch_identity = str(payload.get("launch_event_id") or payload.get("launch_result_ref") or "").strip()
    expected_launch_identity = _repo_reactor_residency_guard_launch_identity(repo_root=repo_root)
    if launch_identity and launch_identity != expected_launch_identity:
        return None
    return {
        **payload,
        "pid": pid,
        "process_fingerprint": current_process_fingerprint or payload_process_fingerprint,
        "code_fingerprint": payload_code_fingerprint or current_code_fingerprint,
    }


def _materialize_repo_reactor_residency_guard_runtime_marker_from_authority(
    *,
    repo_root: Path,
    authority_payload: dict[str, Any],
) -> dict[str, Any]:
    runtime_ref = repo_reactor_residency_guard_runtime_ref(repo_root=repo_root)
    log_ref = repo_reactor_residency_guard_log_ref(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    pid = int(authority_payload.get("pid") or 0)
    marker_payload = {
        "schema": "loop_product.repo_reactor_residency_guard_runtime",
        "runtime_kind": "repo_reactor_residency_guard",
        "repo_root": str(repo_root),
        "pid": pid,
        "code_fingerprint": str(authority_payload.get("code_fingerprint") or _runtime_code_fingerprint()),
        "poll_interval_s": float(
            authority_payload.get("poll_interval_s") or _DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_POLL_INTERVAL_S
        ),
        "idle_exit_after_s": float(
            authority_payload.get("idle_exit_after_s") or _DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_SERVICE_IDLE_EXIT_AFTER_S
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
    write_json_read_only(runtime_ref, marker_payload)
    return marker_payload


def _repo_reactor_residency_guard_next_lease_epoch(*, repo_root: Path) -> int:
    from loop_product.event_journal import iter_committed_events

    latest_epoch = 0
    guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
    for event in iter_committed_events(guard_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        if str(event.get("node_id") or "") != REPO_REACTOR_RESIDENCY_GUARD_NODE_ID:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("attachment_state") or "").strip().upper() != "ATTACHED":
            continue
        try:
            latest_epoch = max(latest_epoch, int(payload.get("lease_epoch") or 0))
        except (TypeError, ValueError):
            continue
    return latest_epoch + 1 if latest_epoch > 0 else 1


def _repo_reactor_residency_guard_liveness_observation_id(
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
    return f"repo_reactor_residency_guard_liveness::{digest}"


def _repo_reactor_residency_guard_identity_observation_id(
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
    return f"repo_reactor_residency_guard_identity::{digest}"


def _repo_reactor_residency_guard_exit_observation_id(
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
    return f"repo_reactor_residency_guard_exit::{digest}"


def _repo_reactor_residency_guard_process_exit_observation_id(
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
    return f"repo_reactor_residency_guard_process_exit::{digest}"


def _emit_repo_reactor_residency_guard_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    heartbeat_ordinal: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
    process_started_at_utc: str,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_process_identity_confirmed_event,
    )

    pid = os.getpid()
    observed_at = _now_iso()
    process_fingerprint = _repo_reactor_residency_guard_process_fingerprint(pid=pid)
    launch_identity = _repo_reactor_residency_guard_launch_identity(repo_root=repo_root)
    runtime_ref = repo_reactor_residency_guard_runtime_ref(repo_root=repo_root)
    guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "repo_reactor_residency_guard",
        "attachment_state": "ATTACHED",
        "observed_at": observed_at,
        "lease_owner_id": f"repo-reactor-residency-guard:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "lease_duration_s": max(5.0, float(poll_interval_s) * 3.0),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "launch_result_ref": launch_identity,
        "launch_event_id": launch_identity,
        "process_started_at_utc": str(process_started_at_utc),
        "runtime_ref": str(runtime_ref),
    }
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        guard_root,
        observation_id=_repo_reactor_residency_guard_liveness_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
        ),
        node_id=REPO_REACTOR_RESIDENCY_GUARD_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_reactor_residency_guard_loop",
    )
    if heartbeat_ordinal == 0:
        commit_process_identity_confirmed_event(
            guard_root,
            observation_id=_repo_reactor_residency_guard_identity_observation_id(
                repo_root=repo_root,
                pid=pid,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
                lease_epoch=lease_epoch,
                process_started_at_utc=process_started_at_utc,
            ),
            node_id=REPO_REACTOR_RESIDENCY_GUARD_NODE_ID,
            payload={
                "controller_kind": "repo_reactor_residency_guard",
                "observed_at": observed_at,
                "pid": int(pid),
                "process_fingerprint": process_fingerprint,
                "code_fingerprint": _runtime_code_fingerprint(),
                "poll_interval_s": float(poll_interval_s),
                "idle_exit_after_s": float(idle_exit_after_s),
                "launch_result_ref": launch_identity,
                "launch_event_id": launch_identity,
                "process_started_at_utc": str(process_started_at_utc),
                "lease_epoch": int(lease_epoch),
                "lease_owner_id": f"repo-reactor-residency-guard:{runtime_ref}",
                "runtime_ref": str(runtime_ref),
            },
            producer="runtime.control_plane.run_repo_reactor_residency_guard_loop",
        )
    return dict(liveness_outcome or {})


def _emit_repo_reactor_residency_guard_exit_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
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
    process_fingerprint = _repo_reactor_residency_guard_process_fingerprint(pid=pid)
    launch_identity = _repo_reactor_residency_guard_launch_identity(repo_root=repo_root)
    runtime_ref = repo_reactor_residency_guard_runtime_ref(repo_root=repo_root)
    guard_root = repo_reactor_residency_guard_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "repo_reactor_residency_guard",
        "attachment_state": "TERMINAL",
        "observation_kind": f"repo_reactor_residency_guard_exit:{str(exit_reason)}",
        "exit_reason": str(exit_reason),
        "observed_at": observed_at,
        "lease_owner_id": f"repo-reactor-residency-guard:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
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
        guard_root,
        observation_id=_repo_reactor_residency_guard_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=REPO_REACTOR_RESIDENCY_GUARD_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_reactor_residency_guard_loop",
    )
    process_exit_event = commit_process_exit_observed_event(
        guard_root,
        observation_id=_repo_reactor_residency_guard_process_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=REPO_REACTOR_RESIDENCY_GUARD_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_reactor_residency_guard_loop",
    )
    return {
        "liveness_outcome": dict(liveness_outcome or {}),
        "process_exit_event": dict(process_exit_event or {}),
    }


def _repo_reactor_residency_guard_should_self_fence(liveness_outcome: dict[str, Any] | None) -> bool:
    outcome = str(dict(liveness_outcome or {}).get("outcome") or "").strip()
    return outcome.startswith("rejected_")


def _repo_reactor_residency_guard_cleanup_authority(
    *,
    repo_root: Path,
) -> dict[str, Any] | None:
    return runtime_cleanup_retirement_authority(
        state_root=repo_reactor_residency_guard_runtime_root(repo_root=repo_root),
        repo_root=repo_root,
    )


def live_repo_reactor_residency_guard_runtime(*, repo_root: str | Path) -> dict[str, Any] | None:
    resolved_repo_root = _repo_root(repo_root)
    runtime_ref = repo_reactor_residency_guard_runtime_ref(repo_root=resolved_repo_root)
    if not runtime_ref.exists():
        authority_payload = _repo_reactor_residency_guard_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_repo_reactor_residency_guard_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        authority_payload = _repo_reactor_residency_guard_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_repo_reactor_residency_guard_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    if not _runtime_marker_matches_process(payload):
        runtime_ref.unlink(missing_ok=True)
        authority_payload = _repo_reactor_residency_guard_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_repo_reactor_residency_guard_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    return payload


@contextmanager
def _repo_reactor_residency_guard_runtime_marker(
    *,
    repo_root: Path,
    poll_interval_s: float,
    idle_exit_after_s: float,
    lease_epoch: int,
    started_at_utc: str,
) -> Any:
    runtime_ref = repo_reactor_residency_guard_runtime_ref(repo_root=repo_root)
    log_ref = repo_reactor_residency_guard_log_ref(repo_root=repo_root)
    write_json_read_only(
        runtime_ref,
        {
            "schema": "loop_product.repo_reactor_residency_guard_runtime",
            "runtime_kind": "repo_reactor_residency_guard",
            "repo_root": str(repo_root),
            "pid": os.getpid(),
            "code_fingerprint": _runtime_code_fingerprint(),
            "poll_interval_s": float(poll_interval_s),
            "idle_exit_after_s": float(idle_exit_after_s),
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
        if int(dict(current or {}).get("pid") or 0) == os.getpid():
            runtime_ref.unlink(missing_ok=True)


def _reconcile_repo_reactor_from_residency_guard(
    *,
    repo_root: Path,
    poll_interval_s: float,
) -> dict[str, Any]:
    reactor_payload = live_repo_reactor_runtime(repo_root=repo_root)
    if reactor_payload is not None:
        return dict(reactor_payload or {})
    return ensure_repo_reactor_running(
        repo_root=repo_root,
        poll_interval_s=max(0.05, float(poll_interval_s)),
        trigger_kind="repo_reactor_residency_watchdog",
        trigger_ref=str(repo_reactor_residency_guard_runtime_ref(repo_root=repo_root)),
    )


def _repo_reactor_residency_guard_service_signature(payload: dict[str, Any] | None) -> str:
    payload = dict(payload or {})
    signature_payload = {
        "repo_reactor_pid": int(payload.get("pid") or 0),
    }
    return json.dumps(signature_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def run_repo_reactor_residency_guard_loop(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_SERVICE_IDLE_EXIT_AFTER_S,
) -> int:
    resolved_repo_root = _repo_root(repo_root)
    poll_interval_value = max(0.05, float(poll_interval_s))
    idle_exit_value = max(0.0, float(idle_exit_after_s))
    lease_epoch = _repo_reactor_residency_guard_next_lease_epoch(repo_root=resolved_repo_root)
    started_at_utc = _now_iso()
    heartbeat_ordinal = 0
    last_activity = time.monotonic()

    with _repo_reactor_residency_guard_runtime_marker(
        repo_root=resolved_repo_root,
        poll_interval_s=poll_interval_value,
        idle_exit_after_s=idle_exit_value,
        lease_epoch=lease_epoch,
        started_at_utc=started_at_utc,
    ):
        startup_outcome = _emit_repo_reactor_residency_guard_authority(
            repo_root=resolved_repo_root,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
            poll_interval_s=poll_interval_value,
            idle_exit_after_s=idle_exit_value,
            process_started_at_utc=started_at_utc,
        )
        if _repo_reactor_residency_guard_should_self_fence(startup_outcome):
            _emit_repo_reactor_residency_guard_exit_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                process_started_at_utc=started_at_utc,
                exit_reason="lease_fenced",
            )
            return 0
        last_activity = time.monotonic()
        last_signature = ""
        while True:
            cleanup_authority = _repo_reactor_residency_guard_cleanup_authority(repo_root=resolved_repo_root)
            if cleanup_authority is not None:
                _emit_repo_reactor_residency_guard_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="cleanup_committed",
                    cleanup_authority=cleanup_authority,
                )
                return 0
            reactor_payload = _reconcile_repo_reactor_from_residency_guard(
                repo_root=resolved_repo_root,
                poll_interval_s=poll_interval_value,
            )
            signature = _repo_reactor_residency_guard_service_signature(reactor_payload)
            if signature != last_signature:
                last_signature = signature
                last_activity = time.monotonic()
            elif idle_exit_value > 0.0 and (time.monotonic() - last_activity) >= idle_exit_value:
                _emit_repo_reactor_residency_guard_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="idle_exit",
                )
                return 0
            heartbeat_ordinal += 1
            heartbeat_outcome = _emit_repo_reactor_residency_guard_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                heartbeat_ordinal=heartbeat_ordinal,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                process_started_at_utc=started_at_utc,
            )
            if _repo_reactor_residency_guard_should_self_fence(heartbeat_outcome):
                _emit_repo_reactor_residency_guard_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="lease_fenced",
                )
                return 0
            time.sleep(poll_interval_value)


def ensure_repo_reactor_residency_guard_running(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_REACTOR_RESIDENCY_GUARD_POLL_INTERVAL_S,
    startup_timeout_s: float = _REPO_REACTOR_RESIDENCY_GUARD_STARTUP_TIMEOUT_S,
    trigger_kind: str = "explicit_repo_bootstrap",
    trigger_ref: str = "",
) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    cleanup_authority = _repo_reactor_residency_guard_cleanup_authority(repo_root=resolved_repo_root)
    if cleanup_authority is not None:
        return {
            "bootstrap_status": "blocked_cleanup_committed",
            "service_kind": "repo_reactor_residency_guard",
            "repo_root": str(resolved_repo_root),
            "cleanup_authority_event_id": str(cleanup_authority.get("event_id") or ""),
            "cleanup_authority_event_type": str(cleanup_authority.get("event_type") or ""),
            "cleanup_authority_seq": int(cleanup_authority.get("seq") or 0),
        }
    _commit_repo_reactor_residency_requirement(
        repo_root=resolved_repo_root,
        trigger_kind=trigger_kind,
        trigger_ref=trigger_ref,
        producer="runtime.control_plane.ensure_repo_reactor_residency_guard_running",
    )
    current_code_fingerprint = _runtime_code_fingerprint()
    existing = live_repo_reactor_residency_guard_runtime(repo_root=resolved_repo_root)
    if existing is not None:
        if str(existing.get("code_fingerprint") or "").strip() == current_code_fingerprint:
            return existing
        _terminate_pid(int(existing.get("pid") or 0))
        repo_reactor_residency_guard_runtime_ref(repo_root=resolved_repo_root).unlink(missing_ok=True)

    runtime_ref = repo_reactor_residency_guard_runtime_ref(repo_root=resolved_repo_root)
    log_ref = repo_reactor_residency_guard_log_ref(repo_root=resolved_repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    package_repo_root = Path(__file__).resolve().parents[2]
    with log_ref.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.runtime.control_plane",
                "--run-repo-reactor-residency-guard",
                "--repo-root",
                str(resolved_repo_root),
                "--poll-interval-s",
                str(max(0.05, float(poll_interval_s))),
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
        payload = live_repo_reactor_residency_guard_runtime(repo_root=resolved_repo_root)
        if payload is not None:
            return payload
        exit_code = proc.poll()
        if exit_code is not None:
            payload = live_repo_reactor_residency_guard_runtime(repo_root=resolved_repo_root)
            if payload is not None:
                return payload
            if int(exit_code) == 0:
                time.sleep(0.05)
                continue
            detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
            raise RuntimeError(detail or f"repo reactor residency guard exited early with code {exit_code}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for repo reactor residency guard runtime marker at {runtime_ref}")


def _repo_control_plane_authority_payload(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
    effective = dict(
        dict(
            query_runtime_liveness_view(control_plane_root, initialize_if_missing=False).get(
                "effective_runtime_liveness_by_node"
            )
            or {}
        ).get(
            REPO_CONTROL_PLANE_NODE_ID
        )
        or {}
    )
    if str(effective.get("effective_attachment_state") or "").strip().upper() != "ATTACHED":
        return None
    payload = dict(effective.get("payload") or {})
    pid = int(payload.get("pid") or 0)
    if pid <= 0:
        return None
    if not _repo_control_plane_process_matches(pid=pid, repo_root=repo_root):
        return None
    current_process_fingerprint = _repo_control_plane_process_fingerprint(pid=pid)
    payload_process_fingerprint = str(payload.get("process_fingerprint") or "")
    if payload_process_fingerprint and current_process_fingerprint and payload_process_fingerprint != current_process_fingerprint:
        return None
    current_code_fingerprint = _runtime_code_fingerprint()
    payload_code_fingerprint = str(payload.get("code_fingerprint") or "")
    if payload_code_fingerprint and payload_code_fingerprint != current_code_fingerprint:
        return None
    launch_identity = str(payload.get("launch_event_id") or payload.get("launch_result_ref") or "").strip()
    expected_launch_identity = _repo_control_plane_launch_identity(repo_root=repo_root)
    if launch_identity and launch_identity != expected_launch_identity:
        return None
    return {
        **payload,
        "pid": pid,
        "process_fingerprint": current_process_fingerprint or payload_process_fingerprint,
        "code_fingerprint": payload_code_fingerprint or current_code_fingerprint,
    }


def _stable_repo_control_plane_authority_payload(
    *,
    repo_root: Path,
    settle_s: float = 0.0,
) -> dict[str, Any] | None:
    authority_payload = _repo_control_plane_authority_payload(repo_root=repo_root)
    if authority_payload is None:
        return None
    if float(settle_s) <= 0.0:
        return authority_payload
    time.sleep(float(settle_s))
    return _repo_control_plane_authority_payload(repo_root=repo_root)


def _materialize_repo_control_plane_runtime_marker_from_authority(
    *,
    repo_root: Path,
    authority_payload: dict[str, Any],
) -> dict[str, Any]:
    runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
    log_ref = repo_control_plane_log_ref(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    pid = int(authority_payload.get("pid") or 0)
    marker_payload = {
        "schema": "loop_product.repo_control_plane_runtime",
        "runtime_kind": "repo_control_plane",
        "repo_root": str(repo_root),
        "pid": pid,
        "code_fingerprint": str(authority_payload.get("code_fingerprint") or _runtime_code_fingerprint()),
        "poll_interval_s": float(authority_payload.get("poll_interval_s") or _DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S),
        "idle_exit_after_s": float(authority_payload.get("idle_exit_after_s") or _DEFAULT_REPO_CONTROL_PLANE_SERVICE_IDLE_EXIT_AFTER_S),
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


def _matching_repo_control_plane_processes(*, repo_root: Path) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for pid in _repo_service_processes_by_kind(repo_root=repo_root).get("repo_control_plane", []):
        command_line = _pid_command_line(pid)
        if not command_line:
            continue
        matches.append({"pid": int(pid), "command": command_line})
    return matches


def _process_scan_repo_control_plane_runtime_marker(*, repo_root: Path) -> dict[str, Any] | None:
    from loop_product.kernel import query_runtime_liveness_view

    liveness_view = query_runtime_liveness_view(
        repo_control_plane_runtime_root(repo_root=repo_root),
        initialize_if_missing=False,
    )
    effective = dict(
        dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(REPO_CONTROL_PLANE_NODE_ID) or {}
    )
    effective_attachment_state = str(
        effective.get("effective_attachment_state")
        or effective.get("raw_attachment_state")
        or ""
    ).strip().upper()
    if effective_attachment_state in {"TERMINAL", "LOST"}:
        return None
    matches = _matching_repo_control_plane_processes(repo_root=repo_root)
    if len(matches) != 1:
        return None
    match = dict(matches[0] or {})
    pid = int(match.get("pid") or 0)
    if pid <= 0:
        return None
    command_line = str(match.get("command") or "")
    effective_payload = dict(effective.get("payload") or {})
    runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
    log_ref = repo_control_plane_log_ref(repo_root=repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    marker_payload = {
        "schema": "loop_product.repo_control_plane_runtime",
        "runtime_kind": "repo_control_plane",
        "repo_root": str(repo_root),
        "pid": pid,
        "code_fingerprint": str(effective_payload.get("code_fingerprint") or _runtime_code_fingerprint()),
        "poll_interval_s": float(
            effective_payload.get("poll_interval_s")
            or _command_flag_value(command_line, "--poll-interval-s")
            or _DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S
        ),
        "idle_exit_after_s": float(
            effective_payload.get("idle_exit_after_s") or _DEFAULT_REPO_CONTROL_PLANE_SERVICE_IDLE_EXIT_AFTER_S
        ),
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


def _repo_control_plane_next_lease_epoch(*, repo_root: Path) -> int:
    from loop_product.event_journal import iter_committed_events

    latest_epoch = 0
    control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
    for event in iter_committed_events(control_plane_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        if str(event.get("node_id") or "") != REPO_CONTROL_PLANE_NODE_ID:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("attachment_state") or "").strip().upper() != "ATTACHED":
            continue
        try:
            latest_epoch = max(latest_epoch, int(payload.get("lease_epoch") or 0))
        except (TypeError, ValueError):
            continue
    return latest_epoch + 1 if latest_epoch > 0 else 1


def _repo_control_plane_liveness_observation_id(
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
    return f"repo_control_plane_liveness::{digest}"


def _repo_control_plane_identity_observation_id(
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
    return f"repo_control_plane_identity::{digest}"


def _repo_control_plane_exit_observation_id(
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
    return f"repo_control_plane_exit::{digest}"


def _repo_control_plane_process_exit_observation_id(
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
    return f"repo_control_plane_process_exit::{digest}"


def _emit_repo_control_plane_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    heartbeat_ordinal: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
    process_started_at_utc: str,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_guarded_runtime_liveness_observation_event,
        commit_process_identity_confirmed_event,
    )

    pid = os.getpid()
    observed_at = _now_iso()
    process_fingerprint = _repo_control_plane_process_fingerprint(pid=pid)
    launch_identity = _repo_control_plane_launch_identity(repo_root=repo_root)
    runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
    control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "repo_control_plane",
        "attachment_state": "ATTACHED",
        "observed_at": observed_at,
        "lease_owner_id": f"repo-control-plane:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "lease_duration_s": max(5.0, float(poll_interval_s) * 3.0),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
        "launch_result_ref": launch_identity,
        "launch_event_id": launch_identity,
        "process_started_at_utc": str(process_started_at_utc),
        "runtime_ref": str(runtime_ref),
    }
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        control_plane_root,
        observation_id=_repo_control_plane_liveness_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
        ),
        node_id=REPO_CONTROL_PLANE_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_control_plane_loop",
    )
    if heartbeat_ordinal == 0:
        commit_process_identity_confirmed_event(
            control_plane_root,
            observation_id=_repo_control_plane_identity_observation_id(
                repo_root=repo_root,
                pid=pid,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
                lease_epoch=lease_epoch,
                process_started_at_utc=process_started_at_utc,
            ),
            node_id=REPO_CONTROL_PLANE_NODE_ID,
            payload={
                "controller_kind": "repo_control_plane",
                "observed_at": observed_at,
                "pid": int(pid),
                "process_fingerprint": process_fingerprint,
                "code_fingerprint": _runtime_code_fingerprint(),
                "poll_interval_s": float(poll_interval_s),
                "idle_exit_after_s": float(idle_exit_after_s),
                "launch_result_ref": launch_identity,
                "launch_event_id": launch_identity,
                "process_started_at_utc": str(process_started_at_utc),
                "lease_epoch": int(lease_epoch),
                "lease_owner_id": f"repo-control-plane:{runtime_ref}",
                "runtime_ref": str(runtime_ref),
            },
            producer="runtime.control_plane.run_repo_control_plane_loop",
        )
    return dict(liveness_outcome or {})


def _emit_repo_control_plane_exit_authority(
    *,
    repo_root: Path,
    lease_epoch: int,
    poll_interval_s: float,
    idle_exit_after_s: float,
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
    process_fingerprint = _repo_control_plane_process_fingerprint(pid=pid)
    launch_identity = _repo_control_plane_launch_identity(repo_root=repo_root)
    runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
    control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
    payload = {
        "controller_kind": "repo_control_plane",
        "attachment_state": "TERMINAL",
        "observation_kind": f"repo_control_plane_exit:{str(exit_reason)}",
        "exit_reason": str(exit_reason),
        "observed_at": observed_at,
        "lease_owner_id": f"repo-control-plane:{runtime_ref}",
        "lease_epoch": int(lease_epoch),
        "pid": int(pid),
        "process_fingerprint": process_fingerprint,
        "code_fingerprint": _runtime_code_fingerprint(),
        "poll_interval_s": float(poll_interval_s),
        "idle_exit_after_s": float(idle_exit_after_s),
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
        control_plane_root,
        observation_id=_repo_control_plane_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=REPO_CONTROL_PLANE_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_control_plane_loop",
    )
    process_exit_event = commit_process_exit_observed_event(
        control_plane_root,
        observation_id=_repo_control_plane_process_exit_observation_id(
            repo_root=repo_root,
            pid=pid,
            lease_epoch=lease_epoch,
            exit_reason=exit_reason,
            process_started_at_utc=process_started_at_utc,
        ),
        node_id=REPO_CONTROL_PLANE_NODE_ID,
        payload=payload,
        producer="runtime.control_plane.run_repo_control_plane_loop",
    )
    return {
        "liveness_outcome": dict(liveness_outcome or {}),
        "process_exit_event": dict(process_exit_event or {}),
    }


def _repo_control_plane_should_self_fence(liveness_outcome: dict[str, Any] | None) -> bool:
    outcome = str(dict(liveness_outcome or {}).get("outcome") or "").strip()
    return outcome.startswith("rejected_")


def _repo_control_plane_cleanup_authority(
    *,
    repo_root: Path,
) -> dict[str, Any] | None:
    return runtime_cleanup_retirement_authority(
        state_root=repo_control_plane_runtime_root(repo_root=repo_root),
        repo_root=repo_root,
    )


def live_repo_reactor_runtime(*, repo_root: str | Path) -> dict[str, Any] | None:
    resolved_repo_root = _repo_root(repo_root)
    runtime_ref = repo_reactor_runtime_ref(repo_root=resolved_repo_root)
    if not runtime_ref.exists():
        authority_payload = _repo_reactor_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_repo_reactor_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        runtime_ref.unlink(missing_ok=True)
        authority_payload = _repo_reactor_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_repo_reactor_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    if not _runtime_marker_matches_process(payload):
        runtime_ref.unlink(missing_ok=True)
        authority_payload = _repo_reactor_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is None:
            return None
        return _materialize_repo_reactor_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    return payload


@contextmanager
def _repo_reactor_runtime_marker(
    *,
    repo_root: Path,
    poll_interval_s: float,
    idle_exit_after_s: float,
    lease_epoch: int,
    started_at_utc: str,
) -> Any:
    runtime_ref = repo_reactor_runtime_ref(repo_root=repo_root)
    log_ref = repo_reactor_log_ref(repo_root=repo_root)
    write_json_read_only(
        runtime_ref,
        {
            "schema": "loop_product.repo_reactor_runtime",
            "runtime_kind": "repo_reactor",
            "repo_root": str(repo_root),
            "pid": os.getpid(),
            "code_fingerprint": _runtime_code_fingerprint(),
            "poll_interval_s": float(poll_interval_s),
            "idle_exit_after_s": float(idle_exit_after_s),
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
        if int(dict(current or {}).get("pid") or 0) == os.getpid():
            runtime_ref.unlink(missing_ok=True)


def _reconcile_repo_control_plane_from_reactor(
    *,
    repo_root: Path,
    poll_interval_s: float,
) -> dict[str, Any]:
    control_plane_payload = live_repo_control_plane_runtime(repo_root=repo_root)
    if control_plane_payload is not None:
        return dict(control_plane_payload or {})
    return ensure_repo_control_plane_running(
        repo_root=repo_root,
        poll_interval_s=max(0.05, float(poll_interval_s)),
        trigger_kind="repo_reactor_watchdog",
        trigger_ref=str(repo_reactor_runtime_ref(repo_root=repo_root)),
    )


def _repo_reactor_service_signature(payload: dict[str, Any] | None) -> str:
    payload = dict(payload or {})
    signature_payload = {
        "repo_control_plane_pid": int(payload.get("pid") or 0),
    }
    return json.dumps(signature_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def run_repo_reactor_loop(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_REACTOR_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_REPO_REACTOR_SERVICE_IDLE_EXIT_AFTER_S,
) -> int:
    resolved_repo_root = _repo_root(repo_root)
    poll_interval_value = max(0.05, float(poll_interval_s))
    idle_exit_value = max(0.0, float(idle_exit_after_s))
    lease_epoch = _repo_reactor_next_lease_epoch(repo_root=resolved_repo_root)
    started_at_utc = _now_iso()
    heartbeat_ordinal = 0
    last_activity = time.monotonic()

    with _repo_reactor_runtime_marker(
        repo_root=resolved_repo_root,
        poll_interval_s=poll_interval_value,
        idle_exit_after_s=idle_exit_value,
        lease_epoch=lease_epoch,
        started_at_utc=started_at_utc,
    ):
        startup_outcome = _emit_repo_reactor_authority(
            repo_root=resolved_repo_root,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
            poll_interval_s=poll_interval_value,
            idle_exit_after_s=idle_exit_value,
            process_started_at_utc=started_at_utc,
        )
        if _repo_reactor_should_self_fence(startup_outcome):
            _emit_repo_reactor_exit_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                process_started_at_utc=started_at_utc,
                exit_reason="lease_fenced",
            )
            return 0
        last_activity = time.monotonic()
        last_signature = ""
        while True:
            cleanup_authority = _repo_reactor_cleanup_authority(repo_root=resolved_repo_root)
            if cleanup_authority is not None:
                _emit_repo_reactor_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="cleanup_committed",
                    cleanup_authority=cleanup_authority,
                )
                return 0
            control_plane_payload = _reconcile_repo_control_plane_from_reactor(
                repo_root=resolved_repo_root,
                poll_interval_s=poll_interval_value,
            )
            signature = _repo_reactor_service_signature(control_plane_payload)
            if signature != last_signature:
                last_signature = signature
                last_activity = time.monotonic()
            elif idle_exit_value > 0.0 and (time.monotonic() - last_activity) >= idle_exit_value:
                _emit_repo_reactor_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="idle_exit",
                )
                return 0
            heartbeat_ordinal += 1
            heartbeat_outcome = _emit_repo_reactor_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                heartbeat_ordinal=heartbeat_ordinal,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                process_started_at_utc=started_at_utc,
            )
            if _repo_reactor_should_self_fence(heartbeat_outcome):
                _emit_repo_reactor_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="lease_fenced",
                )
                return 0
            time.sleep(poll_interval_value)


def ensure_repo_reactor_running(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_REACTOR_POLL_INTERVAL_S,
    startup_timeout_s: float = _REPO_REACTOR_STARTUP_TIMEOUT_S,
    trigger_kind: str = "explicit_repo_bootstrap",
    trigger_ref: str = "",
) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    cleanup_authority = _repo_reactor_cleanup_authority(repo_root=resolved_repo_root)
    if cleanup_authority is not None:
        return {
            "bootstrap_status": "blocked_cleanup_committed",
            "service_kind": "repo_reactor",
            "repo_root": str(resolved_repo_root),
            "cleanup_authority_event_id": str(cleanup_authority.get("event_id") or ""),
            "cleanup_authority_event_type": str(cleanup_authority.get("event_type") or ""),
            "cleanup_authority_seq": int(cleanup_authority.get("seq") or 0),
        }
    _commit_repo_reactor_requirement(
        repo_root=resolved_repo_root,
        trigger_kind=trigger_kind,
        trigger_ref=trigger_ref,
        producer="runtime.control_plane.ensure_repo_reactor_running",
    )
    current_code_fingerprint = _runtime_code_fingerprint()
    existing = live_repo_reactor_runtime(repo_root=resolved_repo_root)
    if existing is not None:
        if str(existing.get("code_fingerprint") or "").strip() == current_code_fingerprint:
            return existing
        _terminate_pid(int(existing.get("pid") or 0))
        repo_reactor_runtime_ref(repo_root=resolved_repo_root).unlink(missing_ok=True)

    runtime_ref = repo_reactor_runtime_ref(repo_root=resolved_repo_root)
    log_ref = repo_reactor_log_ref(repo_root=resolved_repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    package_repo_root = Path(__file__).resolve().parents[2]
    with log_ref.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.runtime.control_plane",
                "--run-repo-reactor",
                "--repo-root",
                str(resolved_repo_root),
                "--poll-interval-s",
                str(max(0.05, float(poll_interval_s))),
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
        payload = live_repo_reactor_runtime(repo_root=resolved_repo_root)
        if payload is not None:
            return payload
        exit_code = proc.poll()
        if exit_code is not None:
            payload = live_repo_reactor_runtime(repo_root=resolved_repo_root)
            if payload is not None:
                return payload
            if int(exit_code) == 0:
                time.sleep(0.05)
                continue
            detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
            raise RuntimeError(detail or f"repo reactor exited early with code {exit_code}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for repo reactor runtime marker at {runtime_ref}")


def live_repo_control_plane_runtime(*, repo_root: str | Path) -> dict[str, Any] | None:
    resolved_repo_root = _repo_root(repo_root)
    runtime_ref = repo_control_plane_runtime_ref(repo_root=resolved_repo_root)
    if not runtime_ref.exists():
        authority_payload = _stable_repo_control_plane_authority_payload(
            repo_root=resolved_repo_root,
            settle_s=_REPO_CONTROL_PLANE_AUTHORITY_REUSE_SETTLE_S,
        )
        if authority_payload is None:
            return _process_scan_repo_control_plane_runtime_marker(repo_root=resolved_repo_root)
        return _materialize_repo_control_plane_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    try:
        payload = _load_runtime_payload(runtime_ref)
    except Exception:
        authority_payload = _stable_repo_control_plane_authority_payload(
            repo_root=resolved_repo_root,
            settle_s=_REPO_CONTROL_PLANE_AUTHORITY_REUSE_SETTLE_S,
        )
        runtime_ref.unlink(missing_ok=True)
        if authority_payload is None:
            return _process_scan_repo_control_plane_runtime_marker(repo_root=resolved_repo_root)
        return _materialize_repo_control_plane_runtime_marker_from_authority(
            repo_root=resolved_repo_root,
            authority_payload=authority_payload,
        )
    if not _runtime_marker_matches_process(payload):
        time.sleep(_REPO_CONTROL_PLANE_AUTHORITY_REUSE_SETTLE_S)
        try:
            refreshed_payload = _load_runtime_payload(runtime_ref)
        except Exception:
            refreshed_payload = {}
        if refreshed_payload and _runtime_marker_matches_process(refreshed_payload):
            return refreshed_payload
        authority_payload = _repo_control_plane_authority_payload(repo_root=resolved_repo_root)
        if authority_payload is not None:
            return _materialize_repo_control_plane_runtime_marker_from_authority(
                repo_root=resolved_repo_root,
                authority_payload=authority_payload,
            )
        runtime_ref.unlink(missing_ok=True)
        return _process_scan_repo_control_plane_runtime_marker(repo_root=resolved_repo_root)
    return payload


@contextmanager
def _repo_control_plane_runtime_marker(
    *,
    repo_root: Path,
    poll_interval_s: float,
    idle_exit_after_s: float,
    lease_epoch: int,
    started_at_utc: str,
) -> Any:
    runtime_ref = repo_control_plane_runtime_ref(repo_root=repo_root)
    log_ref = repo_control_plane_log_ref(repo_root=repo_root)
    write_json_read_only(
        runtime_ref,
        {
            "schema": "loop_product.repo_control_plane_runtime",
            "runtime_kind": "repo_control_plane",
            "repo_root": str(repo_root),
            "pid": os.getpid(),
            "code_fingerprint": _runtime_code_fingerprint(),
            "poll_interval_s": float(poll_interval_s),
            "idle_exit_after_s": float(idle_exit_after_s),
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
        if int(dict(current or {}).get("pid") or 0) == os.getpid():
            runtime_ref.unlink(missing_ok=True)


def _iter_retryable_evaluator_submission_refs(*, repo_root: Path) -> list[Path]:
    state_root = repo_anchor_runtime_root(repo_root=repo_root)
    evaluator_nodes_root = state_root / "artifacts" / "evaluator_nodes"
    if not evaluator_nodes_root.exists():
        return []
    retained: list[Path] = []
    for submission_ref in sorted(evaluator_nodes_root.glob("**/EvaluatorNodeSubmission.json")):
        try:
            payload = json.loads(submission_ref.read_text(encoding="utf-8"))
        except Exception:
            continue
        target_node_id = str(payload.get("target_node_id") or "").strip()
        if not target_node_id:
            continue
        _run_state_ref, run_state = evaluator_authority.load_latest_evaluator_run_state(
            state_root=state_root,
            node_id=target_node_id,
        )
        if not run_state or not evaluator_authority.evaluator_run_is_inflight(run_state):
            continue
        retained.append(submission_ref.resolve())
    return retained


def _reconcile_repo_retryable_evaluator_supervision_from_control_plane(
    *,
    repo_root: Path,
) -> dict[str, Any]:
    state_root = repo_anchor_runtime_root(repo_root=repo_root)
    tracked_submission_refs = _iter_retryable_evaluator_submission_refs(repo_root=repo_root)
    runtime_payload = dict(
        evaluator_supervision_module.live_repo_evaluator_supervision_runtime(
            repo_evaluator_supervision_root=evaluator_supervision_module.repo_evaluator_supervision_root(
                state_root=state_root,
            )
        )
        or {}
    )
    bootstrap_errors: list[str] = []
    for submission_ref in tracked_submission_refs:
        try:
            runtime_payload = dict(
                evaluator_supervision_module.ensure_evaluator_supervision_running(
                    state_root=state_root,
                    submission_ref=submission_ref,
                )
                or runtime_payload
            )
        except TimeoutError as exc:
            bootstrap_errors.append(str(exc))
    return {
        "repo_evaluator_supervision": runtime_payload,
        "tracked_submission_count": len(tracked_submission_refs),
        "tracked_submission_refs": [str(item) for item in tracked_submission_refs],
        **({"bootstrap_errors": list(bootstrap_errors)} if bootstrap_errors else {}),
    }


def _reconcile_repo_scope_services(
    *,
    repo_root: Path,
    poll_interval_s: float,
) -> dict[str, Any]:
    evaluator_supervision_payload = _reconcile_repo_retryable_evaluator_supervision_from_control_plane(
        repo_root=repo_root,
    )
    supervisor_payload = supervisor_module.live_supervisor_runtime(repo_root=repo_root)
    if supervisor_payload is None:
        try:
            supervisor_payload = supervisor_module.ensure_host_child_launch_supervisor_running(
                repo_root=repo_root,
                poll_interval_s=max(0.05, float(poll_interval_s)),
                idle_exit_after_s=_DEFAULT_REPO_CONTROL_PLANE_SERVICE_IDLE_EXIT_AFTER_S,
                watch_repo_control_plane=True,
            )
        except TimeoutError as exc:
            supervisor_payload = {
                "bootstrap_status": "retryable_startup_gate_pending",
                "service_kind": "host_child_launch_supervisor",
                "repo_root": str(repo_root),
                "retryable_error": str(exc),
            }
    housekeeping_payload = live_housekeeping_reap_controller_runtime(repo_root=repo_root)
    if housekeeping_payload is None or not bool(housekeeping_payload.get("watch_repo_control_plane")):
        housekeeping_payload = ensure_housekeeping_reap_controller_service_running(
            repo_root=repo_root,
            poll_interval_s=max(0.05, float(poll_interval_s)),
            watch_repo_control_plane=True,
        )
    return {
        **dict(evaluator_supervision_payload or {}),
        "host_child_launch_supervisor": dict(supervisor_payload or {}),
        "housekeeping_reap_controller": dict(housekeeping_payload or {}),
    }


def _reconcile_repo_upper_services_from_control_plane(
    *,
    repo_root: Path,
    poll_interval_s: float,
) -> dict[str, Any]:
    trigger_ref = str(repo_control_plane_runtime_ref(repo_root=repo_root))
    residency_guard_payload = live_repo_reactor_residency_guard_runtime(repo_root=repo_root)
    if residency_guard_payload is None:
        residency_guard_payload = ensure_repo_reactor_residency_guard_running(
            repo_root=repo_root,
            poll_interval_s=max(0.05, float(poll_interval_s)),
            trigger_kind="repo_control_plane_watchdog",
            trigger_ref=trigger_ref,
        )
    reactor_payload = live_repo_reactor_runtime(repo_root=repo_root)
    return {
        "repo_reactor_residency_guard": dict(residency_guard_payload or {}),
        "repo_reactor": dict(reactor_payload or {}),
    }


def _accepted_control_envelope_payload(*, state_root: Path, envelope_id: str) -> dict[str, Any]:
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        return {}
    audit_dir = state_root / "audit"
    if not audit_dir.exists():
        return {}
    for path in sorted(audit_dir.glob("envelope_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("status") or "").strip().lower() != "accepted":
            continue
        if str(payload.get("envelope_id") or "").strip() != normalized_envelope_id:
            continue
        return dict(payload or {})
    return {}


def _event_for_envelope_id(
    *,
    state_root: Path,
    event_type: str,
    envelope_id: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_event_type = str(event_type or "").strip()
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_event_type or not normalized_envelope_id:
        return {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "").strip() != normalized_event_type:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("envelope_id") or "").strip() != normalized_envelope_id:
            continue
        return dict(event or {})
    return {}


def _event_by_id(
    state_root: Path,
    *,
    event_id: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_event_id = str(event_id or "").strip()
    if not normalized_event_id:
        return {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_id") or "").strip() == normalized_event_id:
            return dict(event or {})
    return {}


def _ensure_repo_global_heavy_object_authority_gap_audit(
    *,
    repo_root: Path,
    object_kind: str,
    audit_signature: str,
    requirement_event: dict[str, Any],
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_heavy_object_authority_gap_audit_requested_event,
        mirror_accepted_control_envelope_event,
    )
    from loop_product.gateway import accept_control_envelope, normalize_control_envelope
    from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    requirement_event_id = str(dict(requirement_event or {}).get("event_id") or "")
    raw_envelope = ControlEnvelope(
        source=REPO_CONTROL_PLANE_NODE_ID,
        envelope_type="heavy_object_authority_gap_audit_request",
        payload_type="local_control_decision",
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_AUDIT_ROUND_ID,
        generation=0,
        payload={
            "repo_root": str(repo_root.resolve()),
            "object_kind": str(object_kind or "").strip(),
            "runtime_name": "",
            "audit_kind": "repo_root_authority_gap_audit",
            "audit_signature": str(audit_signature or "").strip(),
            "trigger_kind": "repo_control_plane_auto_audit",
            "trigger_ref": str(audit_signature or "").strip(),
            "requirement_event_id": requirement_event_id,
        },
        status=EnvelopeStatus.PROPOSAL,
        note=(
            "repo-global heavy-object authority-gap auto audit "
            f"[{str(object_kind or '').strip()}:{str(audit_signature or '').strip()[:12]}]"
        ),
    )
    normalized = normalize_control_envelope(raw_envelope)
    envelope_id = str(normalized.envelope_id or "")
    accepted_payload = _accepted_control_envelope_payload(
        state_root=anchor_state_root,
        envelope_id=envelope_id,
    )
    if accepted_payload:
        accepted = ControlEnvelope.from_dict(accepted_payload)
    else:
        accepted = accept_control_envelope(anchor_state_root, normalized)
    if accepted.status is not EnvelopeStatus.ACCEPTED:
        return {
            "envelope_id": envelope_id,
            "accepted": False,
            "status": str(accepted.status.value),
            "rejected_note": str(accepted.note or ""),
            "requirement_event_id": requirement_event_id,
            "audit_signature": str(audit_signature or "").strip(),
        }
    mirror_event = mirror_accepted_control_envelope_event(anchor_state_root, accepted)
    requested_event = commit_heavy_object_authority_gap_audit_requested_event(anchor_state_root, accepted)
    receipt = execute_accepted_heavy_object_authority_gap_audit(
        anchor_state_root,
        envelope=accepted.to_dict(),
    )
    result_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_authority_gap_audited",
        envelope_id=str(accepted.envelope_id or ""),
    )
    return {
        "envelope_id": str(accepted.envelope_id or ""),
        "accepted": True,
        "compatibility_event_id": str(dict(mirror_event or {}).get("event_id") or ""),
        "requested_event_id": str(dict(requested_event or {}).get("event_id") or ""),
        "result_event_id": str(dict(result_event or {}).get("event_id") or ""),
        "requirement_event_id": requirement_event_id,
        "audit_signature": str(audit_signature or "").strip(),
        "receipt_ref": str(dict(receipt or {}).get("receipt_ref") or ""),
    }


def _ensure_repo_global_heavy_object_authority_gap_repo_remediation(
    *,
    repo_root: Path,
    object_kind: str,
    remediation_signature: str,
    requirement_event: dict[str, Any],
    audit_result_event_id: str,
) -> dict[str, Any]:
    from loop_product.event_journal import (
        commit_heavy_object_authority_gap_repo_remediation_requested_event,
        mirror_accepted_control_envelope_event,
    )
    from loop_product.gateway import accept_control_envelope, normalize_control_envelope
    from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    requirement_event_id = str(dict(requirement_event or {}).get("event_id") or "")
    normalized_audit_result_event_id = str(audit_result_event_id or "").strip()
    raw_envelope = ControlEnvelope(
        source=REPO_CONTROL_PLANE_NODE_ID,
        envelope_type="heavy_object_authority_gap_repo_remediation_request",
        payload_type="local_control_decision",
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_AUDIT_ROUND_ID,
        generation=0,
        payload={
            "repo_root": str(repo_root.resolve()),
            "object_kind": str(object_kind or "").strip(),
            "runtime_name": "",
            "remediation_kind": "repo_root_authority_gap_remediation",
            "remediation_signature": str(remediation_signature or "").strip(),
            "trigger_kind": "repo_control_plane_auto_remediation",
            "trigger_ref": str(remediation_signature or "").strip(),
            "requirement_event_id": requirement_event_id,
            "audit_result_event_id": normalized_audit_result_event_id,
        },
        status=EnvelopeStatus.PROPOSAL,
        note=(
            "repo-global heavy-object authority-gap auto remediation "
            f"[{str(object_kind or '').strip()}:{str(remediation_signature or '').strip()[:12]}]"
        ),
    )
    normalized = normalize_control_envelope(raw_envelope)
    envelope_id = str(normalized.envelope_id or "")
    accepted_payload = _accepted_control_envelope_payload(
        state_root=anchor_state_root,
        envelope_id=envelope_id,
    )
    if accepted_payload:
        accepted = ControlEnvelope.from_dict(accepted_payload)
    else:
        accepted = accept_control_envelope(anchor_state_root, normalized)
    if accepted.status is not EnvelopeStatus.ACCEPTED:
        return {
            "envelope_id": envelope_id,
            "accepted": False,
            "status": str(accepted.status.value),
            "rejected_note": str(accepted.note or ""),
            "requirement_event_id": requirement_event_id,
            "audit_result_event_id": normalized_audit_result_event_id,
            "remediation_signature": str(remediation_signature or "").strip(),
        }
    mirror_event = mirror_accepted_control_envelope_event(anchor_state_root, accepted)
    requested_event = commit_heavy_object_authority_gap_repo_remediation_requested_event(anchor_state_root, accepted)
    receipt = execute_accepted_heavy_object_authority_gap_repo_remediation(
        anchor_state_root,
        envelope=accepted.to_dict(),
    )
    result_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_authority_gap_repo_remediation_settled",
        envelope_id=str(accepted.envelope_id or ""),
    )
    return {
        "envelope_id": str(accepted.envelope_id or ""),
        "accepted": True,
        "compatibility_event_id": str(dict(mirror_event or {}).get("event_id") or ""),
        "requested_event_id": str(dict(requested_event or {}).get("event_id") or ""),
        "result_event_id": str(dict(result_event or {}).get("event_id") or ""),
        "requirement_event_id": requirement_event_id,
        "audit_result_event_id": normalized_audit_result_event_id,
        "remediation_signature": str(remediation_signature or "").strip(),
        "receipt_ref": str(dict(receipt or {}).get("receipt_ref") or ""),
    }


def _reconcile_repo_global_heavy_object_authority_gap_audits_from_control_plane(
    *,
    repo_root: Path,
) -> dict[str, Any]:
    from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    results: dict[str, Any] = {}
    for object_kind in _REPO_GLOBAL_HEAVY_OBJECT_AUTO_AUDIT_OBJECT_KINDS:
        inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_state_root,
            object_kind=object_kind,
            repo_root=repo_root,
        )
        unmanaged_candidates = [dict(item or {}) for item in list(inventory.get("unmanaged_candidates") or [])]
        audit_signature = (
            _heavy_object_authority_gap_audit_signature(
                repo_root=repo_root,
                object_kind=object_kind,
                unmanaged_candidates=unmanaged_candidates,
            )
            if unmanaged_candidates
            else ""
        )
        requirement_event: dict[str, Any] = {}
        audit_submission: dict[str, Any] = {}
        if audit_signature:
            requirement_event = _commit_heavy_object_authority_gap_audit_requirement(
                repo_root=repo_root,
                object_kind=object_kind,
                audit_signature=audit_signature,
                producer="runtime.control_plane.run_repo_control_plane_loop",
            )
            audit_submission = _ensure_repo_global_heavy_object_authority_gap_audit(
                repo_root=repo_root,
                object_kind=object_kind,
                audit_signature=audit_signature,
                requirement_event=requirement_event,
            )
        results[object_kind] = {
            "object_kind": object_kind,
            "filesystem_candidate_count": int(inventory.get("filesystem_candidate_count") or 0),
            "unmanaged_candidate_count": int(inventory.get("unmanaged_candidate_count") or 0),
            "audit_signature": audit_signature,
            "requirement_event_id": str(dict(requirement_event or {}).get("event_id") or ""),
            "auto_audit_envelope_id": str(dict(audit_submission or {}).get("envelope_id") or ""),
            "auto_audit_result_event_id": str(dict(audit_submission or {}).get("result_event_id") or ""),
        }
    return results


def _reconcile_repo_global_heavy_object_authority_gap_repo_remediations_from_control_plane(
    *,
    repo_root: Path,
    heavy_object_auto_audits: dict[str, Any],
) -> dict[str, Any]:
    results: dict[str, Any] = {}
    for object_kind, audit_summary in sorted(dict(heavy_object_auto_audits or {}).items()):
        normalized_object_kind = str(object_kind or "").strip()
        unmanaged_candidate_count = int(dict(audit_summary or {}).get("unmanaged_candidate_count") or 0)
        remediation_signature = str(dict(audit_summary or {}).get("audit_signature") or "").strip()
        audit_result_event_id = str(dict(audit_summary or {}).get("auto_audit_result_event_id") or "").strip()
        requirement_event: dict[str, Any] = {}
        remediation_submission: dict[str, Any] = {}
        if normalized_object_kind and unmanaged_candidate_count > 0 and remediation_signature:
            requirement_event = _commit_heavy_object_authority_gap_repo_remediation_requirement(
                repo_root=repo_root,
                object_kind=normalized_object_kind,
                remediation_signature=remediation_signature,
                producer="runtime.control_plane.run_repo_control_plane_loop",
                audit_result_event_id=audit_result_event_id,
            )
            remediation_submission = _ensure_repo_global_heavy_object_authority_gap_repo_remediation(
                repo_root=repo_root,
                object_kind=normalized_object_kind,
                remediation_signature=remediation_signature,
                requirement_event=requirement_event,
                audit_result_event_id=audit_result_event_id,
            )
        results[normalized_object_kind] = {
            "object_kind": normalized_object_kind,
            "unmanaged_candidate_count": unmanaged_candidate_count,
            "remediation_signature": remediation_signature,
            "requirement_event_id": str(dict(requirement_event or {}).get("event_id") or ""),
            "audit_result_event_id": audit_result_event_id,
            "auto_remediation_envelope_id": str(dict(remediation_submission or {}).get("envelope_id") or ""),
            "auto_remediation_result_event_id": str(dict(remediation_submission or {}).get("result_event_id") or ""),
        }
    return results


def _ensure_repo_global_heavy_object_supersession(
    *,
    repo_root: Path,
    superseded_object_id: str,
    superseded_object_kind: str,
    superseded_object_ref: str,
    replacement_object_id: str,
    replacement_object_kind: str,
    replacement_object_ref: str,
    runtime_name: str,
    trigger_ref: str,
) -> dict[str, Any]:
    from loop_product.kernel.submit import submit_heavy_object_supersession_request
    from loop_product.protocols.control_envelope import EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    accepted = submit_heavy_object_supersession_request(
        anchor_state_root,
        superseded_object_id=superseded_object_id,
        superseded_object_kind=superseded_object_kind,
        superseded_object_ref=superseded_object_ref,
        replacement_object_id=replacement_object_id,
        replacement_object_kind=replacement_object_kind,
        replacement_object_ref=replacement_object_ref,
        supersession_kind="reference_replaced_auto_supersession",
        reason=(
            "repo-global heavy-object auto supersession "
            f"[{superseded_object_kind}:{superseded_object_id[:12]}->{replacement_object_id[:12]}]"
        ),
        source=REPO_CONTROL_PLANE_NODE_ID,
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
        generation=0,
        runtime_name=runtime_name,
        repo_root=repo_root,
        trigger_kind="heavy_object_reference_replaced",
        trigger_ref=trigger_ref,
    )
    canonical_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_superseded",
        envelope_id=str(accepted.envelope_id or ""),
    )
    return {
        "accepted": accepted.status is EnvelopeStatus.ACCEPTED,
        "envelope_id": str(accepted.envelope_id or ""),
        "event_id": str(dict(canonical_event or {}).get("event_id") or ""),
    }


def _ensure_repo_global_heavy_object_reference_release(
    *,
    repo_root: Path,
    object_id: str,
    object_kind: str,
    object_ref: str,
    reference_ref: str,
    reference_holder_kind: str,
    reference_holder_ref: str,
    reference_kind: str,
    runtime_name: str,
    trigger_ref: str,
) -> dict[str, Any]:
    from loop_product.kernel.submit import submit_heavy_object_reference_release_request
    from loop_product.protocols.control_envelope import EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    accepted = submit_heavy_object_reference_release_request(
        anchor_state_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=object_ref,
        reference_ref=reference_ref,
        reference_holder_kind=reference_holder_kind,
        reference_holder_ref=reference_holder_ref,
        reference_kind=reference_kind,
        reason=(
            "repo-global heavy-object auto reference release "
            f"[{object_kind}:{object_id[:12]}]"
        ),
        source=REPO_CONTROL_PLANE_NODE_ID,
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
        generation=0,
        runtime_name=runtime_name,
        repo_root=repo_root,
        trigger_kind="heavy_object_reference_replaced",
        trigger_ref=trigger_ref,
    )
    canonical_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_reference_released",
        envelope_id=str(accepted.envelope_id or ""),
    )
    return {
        "accepted": accepted.status is EnvelopeStatus.ACCEPTED,
        "envelope_id": str(accepted.envelope_id or ""),
        "event_id": str(dict(canonical_event or {}).get("event_id") or ""),
    }


def _ensure_repo_global_heavy_object_gc_eligibility(
    *,
    repo_root: Path,
    object_id: str,
    object_kind: str,
    object_ref: str,
    superseded_by_object_id: str,
    superseded_by_object_ref: str,
    runtime_name: str,
    trigger_ref: str,
) -> dict[str, Any]:
    from loop_product.kernel.submit import submit_heavy_object_gc_eligibility_request
    from loop_product.protocols.control_envelope import EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    accepted = submit_heavy_object_gc_eligibility_request(
        anchor_state_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=object_ref,
        eligibility_kind="superseded_unreferenced_auto_gc_candidate",
        reason=(
            "repo-global heavy-object auto gc eligibility "
            f"[{object_kind}:{object_id[:12]}]"
        ),
        source=REPO_CONTROL_PLANE_NODE_ID,
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
        generation=0,
        runtime_name=runtime_name,
        repo_root=repo_root,
        superseded_by_object_id=superseded_by_object_id,
        superseded_by_object_ref=superseded_by_object_ref,
        trigger_kind="heavy_object_auto_supersession",
        trigger_ref=trigger_ref,
    )
    canonical_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_gc_eligible",
        envelope_id=str(accepted.envelope_id or ""),
    )
    return {
        "accepted": accepted.status is EnvelopeStatus.ACCEPTED,
        "envelope_id": str(accepted.envelope_id or ""),
        "event_id": str(dict(canonical_event or {}).get("event_id") or ""),
    }


def _ensure_repo_global_heavy_object_reclamation(
    *,
    repo_root: Path,
    object_id: str,
    object_kind: str,
    object_ref: str,
    runtime_name: str,
    trigger_ref: str,
) -> dict[str, Any]:
    from loop_product.kernel.submit import submit_heavy_object_reclamation_request
    from loop_product.protocols.control_envelope import EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    accepted = submit_heavy_object_reclamation_request(
        anchor_state_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=object_ref,
        reclamation_kind="authoritative_auto_reclaim",
        reason=(
            "repo-global heavy-object auto reclamation "
            f"[{object_kind}:{object_id[:12]}]"
        ),
        source=REPO_CONTROL_PLANE_NODE_ID,
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
        generation=0,
        runtime_name=runtime_name,
        repo_root=repo_root,
        trigger_kind="heavy_object_auto_gc_eligibility",
        trigger_ref=trigger_ref,
    )
    canonical_request_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_reclamation_requested",
        envelope_id=str(accepted.envelope_id or ""),
    )
    canonical_result_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_reclaimed",
        envelope_id=str(accepted.envelope_id or ""),
    )
    return {
        "accepted": accepted.status is EnvelopeStatus.ACCEPTED,
        "envelope_id": str(accepted.envelope_id or ""),
        "request_event_id": str(dict(canonical_request_event or {}).get("event_id") or ""),
        "result_event_id": str(dict(canonical_result_event or {}).get("event_id") or ""),
    }


def _iter_repo_global_managed_duplicate_handoff_candidates(
    *,
    state_root: Path,
) -> list[dict[str, Any]]:
    from loop_product.artifact_hygiene import canonical_heavy_object_store_ref
    from loop_product.event_journal import iter_committed_events

    repo_root = repo_root_from_runtime_root(state_root=state_root)
    if repo_root is None:
        return []

    latest_by_holder: dict[tuple[str, str, str], dict[str, Any]] = {}
    for event in iter_committed_events(state_root):
        event_type = str(event.get("event_type") or "").strip()
        if event_type not in {"heavy_object_reference_attached", "heavy_object_reference_released"}:
            continue
        payload = dict(event.get("payload") or {})
        holder_key = _heavy_object_reference_holder_key(payload)
        if not all(holder_key):
            continue
        latest_by_holder[holder_key] = dict(event or {})

    candidates: list[dict[str, Any]] = []
    for latest_event in latest_by_holder.values():
        if str(latest_event.get("event_type") or "").strip() != "heavy_object_reference_attached":
            continue
        payload = dict(latest_event.get("payload") or {})
        object_kind = str(payload.get("object_kind") or "").strip()
        if object_kind not in {"publish_tree", "runtime_heavy_tree"}:
            continue
        object_id = str(payload.get("object_id") or "").strip()
        object_ref = str(payload.get("object_ref") or "").strip()
        reference_ref = str(payload.get("reference_ref") or "").strip()
        if not object_id or not object_ref or not reference_ref:
            continue
        if object_ref == reference_ref:
            # Reclaiming an object whose object_ref is also the live holder path would delete the holder itself.
            continue
        try:
            canonical_ref = canonical_heavy_object_store_ref(
                repo_root=repo_root,
                object_kind=object_kind,
                object_id=object_id,
                source_ref=Path(object_ref),
            )
        except Exception:
            continue
        if str(canonical_ref) == object_ref:
            continue
        candidates.append(
            {
                "reference_event": dict(latest_event or {}),
                "reference_payload": payload,
                "object_id": object_id,
                "object_kind": object_kind,
                "current_object_ref": object_ref,
                "canonical_object_ref": str(canonical_ref),
                "reference_ref": reference_ref,
                "reference_holder_kind": str(payload.get("reference_holder_kind") or ""),
                "reference_holder_ref": str(payload.get("reference_holder_ref") or ""),
                "reference_kind": str(payload.get("reference_kind") or ""),
                "runtime_name": str(payload.get("runtime_name") or ""),
            }
        )
    return candidates


def _ensure_repo_global_managed_duplicate_handoff(
    *,
    repo_root: Path,
    object_id: str,
    object_kind: str,
    current_object_ref: str,
    canonical_object_ref: str,
    reference_ref: str,
    reference_holder_kind: str,
    reference_holder_ref: str,
    runtime_name: str,
    trigger_ref: str,
) -> dict[str, Any]:
    from loop_product.artifact_hygiene import heavy_object_identity, materialize_canonical_heavy_object_store_ref
    from loop_product.event_journal import iter_committed_events
    from loop_product.kernel.submit import (
        submit_heavy_object_observation_request,
        submit_heavy_object_reference_request,
        submit_heavy_object_registration_request,
    )
    from loop_product.protocols.control_envelope import EnvelopeStatus

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    normalized_object_identity = (
        str(object_id or "").strip(),
        str(current_object_ref or "").strip(),
    )

    def _stale_holder_result() -> dict[str, Any]:
        latest_holder_fact = _latest_heavy_object_reference_fact_for_holder(
            state_root=anchor_state_root,
            reference_ref=reference_ref,
            reference_holder_kind=reference_holder_kind,
            reference_holder_ref=reference_holder_ref,
        )
        return {
            "accepted": False,
            "skipped_reason": "stale_holder_reference",
            "reference_ref": reference_ref,
            "reference_holder_kind": reference_holder_kind,
            "reference_holder_ref": reference_holder_ref,
            "current_object_ref": str(current_object_ref or ""),
            "canonical_object_ref": str(canonical_object_ref or ""),
            "latest_holder_event_id": str(latest_holder_fact.get("event_id") or ""),
            "latest_holder_event_type": str(latest_holder_fact.get("event_type") or ""),
        }

    def _holder_still_points_to_current_object() -> bool:
        latest_holder_fact = _latest_heavy_object_reference_fact_for_holder(
            state_root=anchor_state_root,
            reference_ref=reference_ref,
            reference_holder_kind=reference_holder_kind,
            reference_holder_ref=reference_holder_ref,
        )
        latest_holder_payload = dict(latest_holder_fact.get("payload") or {})
        latest_holder_identity = _heavy_object_identity_tuple(latest_holder_payload)
        if not (
            str(latest_holder_fact.get("event_type") or "").strip() == "heavy_object_reference_attached"
            and latest_holder_identity[:2] == normalized_object_identity
        ):
            return False
        try:
            live_holder_identity = heavy_object_identity(
                Path(reference_ref).expanduser().resolve(),
                object_kind=object_kind,
            )
        except Exception:
            return False
        return str(live_holder_identity.get("object_id") or "").strip() == normalized_object_identity[0]

    if not _holder_still_points_to_current_object():
        return _stale_holder_result()
    current_object_path = Path(current_object_ref).expanduser().resolve()
    canonical_object_path = Path(canonical_object_ref).expanduser().resolve()
    canonical_registration_event = _latest_heavy_object_registration_anchor(
        state_root=anchor_state_root,
        object_id="",
        object_ref=str(canonical_object_path),
    )
    registration_event_id = str(canonical_registration_event.get("event_id") or "")
    registration_envelope_id = str(dict(canonical_registration_event.get("payload") or {}).get("envelope_id") or "")
    if not canonical_registration_event:
        materialized_canonical_ref = materialize_canonical_heavy_object_store_ref(
            repo_root=repo_root,
            object_kind=object_kind,
            object_id=object_id,
            source_ref=current_object_path,
        )
        registration_identity = heavy_object_identity(materialized_canonical_ref, object_kind=object_kind)
        accepted_registration = submit_heavy_object_registration_request(
            anchor_state_root,
            object_id=object_id,
            object_kind=object_kind,
            object_ref=materialized_canonical_ref,
            byte_size=int(registration_identity.get("byte_size") or 0),
            reason=(
                "repo-global managed duplicate canonical registration "
                f"[{object_kind}:{object_id[:12]}]"
            ),
            source=REPO_CONTROL_PLANE_NODE_ID,
            round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
            generation=0,
            runtime_name=runtime_name,
            repo_root=repo_root,
            registration_kind="shared_reference_auto_registration",
            trigger_kind="heavy_object_shared_reference_convergence",
            trigger_ref=trigger_ref,
        )
        canonical_registration_event = _event_for_envelope_id(
            state_root=anchor_state_root,
            event_type="heavy_object_registered",
            envelope_id=str(accepted_registration.envelope_id or ""),
        )
        registration_event_id = str(canonical_registration_event.get("event_id") or "")
        registration_envelope_id = str(accepted_registration.envelope_id or "")
        canonical_object_path = Path(str(dict(canonical_registration_event.get("payload") or {}).get("object_ref") or materialized_canonical_ref)).expanduser().resolve()

    observed_refs = sorted({str(current_object_path), str(canonical_object_path)})
    observation_event: dict[str, Any] = {}
    for event in iter_committed_events(anchor_state_root):
        if str(event.get("event_type") or "").strip() != "heavy_object_observed":
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("object_id") or "").strip() != object_id:
            continue
        if str(payload.get("object_ref") or "").strip() != str(canonical_object_path):
            continue
        if str(payload.get("observation_kind") or "").strip() != "managed_duplicate_shared_reference_convergence":
            continue
        payload_observed_refs = sorted(
            {
                str(payload.get("object_ref") or "").strip(),
                *[
                    str(item)
                    for item in list(payload.get("object_refs") or [])
                    if str(item or "").strip()
                ],
            }
        )
        if payload_observed_refs != observed_refs:
            continue
        observation_event = dict(event or {})
    observation_envelope_id = str(dict(observation_event.get("payload") or {}).get("envelope_id") or "")
    observation_event_id = str(observation_event.get("event_id") or "")
    if not observation_event:
        canonical_identity = heavy_object_identity(canonical_object_path, object_kind=object_kind)
        current_identity = heavy_object_identity(current_object_path, object_kind=object_kind)
        if not _holder_still_points_to_current_object():
            return _stale_holder_result()
        accepted_observation = submit_heavy_object_observation_request(
            anchor_state_root,
            object_id=object_id,
            object_kind=object_kind,
            object_ref=canonical_object_path,
            object_refs=[current_object_path],
            duplicate_count=len(observed_refs),
            total_bytes=int(canonical_identity.get("byte_size") or 0) + int(current_identity.get("byte_size") or 0),
            observation_kind="managed_duplicate_shared_reference_convergence",
            reason=(
                "repo-global managed duplicate convergence observation "
                f"[{object_kind}:{object_id[:12]}]"
            ),
            source=REPO_CONTROL_PLANE_NODE_ID,
            round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
            generation=0,
            runtime_name=runtime_name,
            repo_root=repo_root,
            trigger_kind="heavy_object_shared_reference_convergence",
            trigger_ref=trigger_ref,
        )
        observation_event = _event_for_envelope_id(
            state_root=anchor_state_root,
            event_type="heavy_object_observed",
            envelope_id=str(accepted_observation.envelope_id or ""),
        )
        observation_envelope_id = str(accepted_observation.envelope_id or "")
        observation_event_id = str(dict(observation_event or {}).get("event_id") or "")

    if not _holder_still_points_to_current_object():
        return _stale_holder_result()
    accepted_reference = submit_heavy_object_reference_request(
        anchor_state_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=canonical_object_path,
        reference_ref=reference_ref,
        reference_holder_kind=reference_holder_kind,
        reference_holder_ref=reference_holder_ref,
        reference_kind="shared_reference_auto_handoff",
        reason=(
            "repo-global managed duplicate shared-reference handoff "
            f"[{object_kind}:{object_id[:12]}]"
        ),
        source=REPO_CONTROL_PLANE_NODE_ID,
        round_id=_REPO_GLOBAL_HEAVY_OBJECT_AUTO_LIFECYCLE_ROUND_ID,
        generation=0,
        runtime_name=runtime_name,
        repo_root=repo_root,
        trigger_kind="heavy_object_shared_reference_convergence",
        trigger_ref=trigger_ref,
    )
    canonical_reference_event = _event_for_envelope_id(
        state_root=anchor_state_root,
        event_type="heavy_object_reference_attached",
        envelope_id=str(accepted_reference.envelope_id or ""),
    )
    return {
        "accepted": accepted_reference.status is EnvelopeStatus.ACCEPTED,
        "registration_envelope_id": registration_envelope_id,
        "registration_event_id": registration_event_id,
        "observation_envelope_id": observation_envelope_id,
        "observation_event_id": observation_event_id,
        "reference_envelope_id": str(accepted_reference.envelope_id or ""),
        "reference_event_id": str(dict(canonical_reference_event or {}).get("event_id") or ""),
        "current_object_ref": str(current_object_path),
        "canonical_object_ref": str(canonical_object_path),
        "reference_ref": reference_ref,
    }


def _reconcile_repo_global_heavy_object_auto_lifecycle_from_control_plane(
    *,
    repo_root: Path,
) -> dict[str, Any]:
    from loop_product.kernel.query import query_heavy_object_retention_view

    anchor_state_root = repo_anchor_runtime_root(repo_root=repo_root)
    handoff_candidates = _iter_repo_global_managed_duplicate_handoff_candidates(state_root=anchor_state_root)
    handoff_results: list[dict[str, Any]] = []
    for candidate in handoff_candidates:
        reference_event = dict(candidate.get("reference_event") or {})
        handoff_results.append(
            _ensure_repo_global_managed_duplicate_handoff(
                repo_root=repo_root,
                object_id=str(candidate.get("object_id") or ""),
                object_kind=str(candidate.get("object_kind") or ""),
                current_object_ref=str(candidate.get("current_object_ref") or ""),
                canonical_object_ref=str(candidate.get("canonical_object_ref") or ""),
                reference_ref=str(candidate.get("reference_ref") or ""),
                reference_holder_kind=str(candidate.get("reference_holder_kind") or ""),
                reference_holder_ref=str(candidate.get("reference_holder_ref") or ""),
                runtime_name=str(candidate.get("runtime_name") or ""),
                trigger_ref=str(reference_event.get("event_id") or ""),
            )
        )
    replacement_candidates = _iter_heavy_object_reference_replacements(state_root=anchor_state_root)
    release_results: list[dict[str, Any]] = []
    for candidate in replacement_candidates:
        reference_event = dict(candidate.get("reference_event") or {})
        reference_payload = dict(candidate.get("reference_payload") or {})
        superseded_reference_event = dict(candidate.get("superseded_reference_event") or {})
        superseded_reference_payload = dict(candidate.get("superseded_reference_payload") or {})
        latest_release = _latest_heavy_object_reference_release_anchor(
            state_root=anchor_state_root,
            object_id=str(candidate.get("superseded_object_id") or ""),
            object_ref=str(candidate.get("superseded_object_ref") or ""),
            reference_ref=str(candidate.get("reference_ref") or ""),
            reference_holder_kind=str(candidate.get("reference_holder_kind") or ""),
            reference_holder_ref=str(candidate.get("reference_holder_ref") or ""),
        )
        if int(latest_release.get("seq") or 0) > int(superseded_reference_event.get("seq") or 0):
            latest_release_payload = dict(latest_release.get("payload") or {})
            release_results.append(
                {
                    "accepted": True,
                    "envelope_id": str(latest_release_payload.get("envelope_id") or ""),
                    "event_id": str(latest_release.get("event_id") or ""),
                }
            )
            continue
        release_results.append(
            _ensure_repo_global_heavy_object_reference_release(
                repo_root=repo_root,
                object_id=str(candidate.get("superseded_object_id") or ""),
                object_kind=str(candidate.get("superseded_object_kind") or ""),
                object_ref=str(candidate.get("superseded_object_ref") or ""),
                reference_ref=str(candidate.get("reference_ref") or ""),
                reference_holder_kind=str(candidate.get("reference_holder_kind") or ""),
                reference_holder_ref=str(candidate.get("reference_holder_ref") or ""),
                reference_kind=str(superseded_reference_payload.get("reference_kind") or reference_payload.get("reference_kind") or ""),
                runtime_name=str(reference_payload.get("runtime_name") or ""),
                trigger_ref=str(reference_event.get("event_id") or ""),
            )
        )

    supersession_results: list[dict[str, Any]] = []
    for candidate in replacement_candidates:
        reference_event = dict(candidate.get("reference_event") or {})
        reference_payload = dict(candidate.get("reference_payload") or {})
        superseded_object_id = str(candidate.get("superseded_object_id") or "").strip()
        superseded_object_ref = str(candidate.get("superseded_object_ref") or "").strip()
        latest_supersession = _latest_heavy_object_supersession_anchor(
            state_root=anchor_state_root,
            superseded_object_id=superseded_object_id,
            superseded_object_ref=superseded_object_ref,
        )
        latest_supersession_payload = dict(latest_supersession.get("payload") or {})
        if (
            latest_supersession_payload
            and str(latest_supersession_payload.get("replacement_object_id") or "").strip()
            == str(candidate.get("replacement_object_id") or "").strip()
            and str(latest_supersession_payload.get("replacement_object_ref") or "").strip()
            == str(candidate.get("replacement_object_ref") or "").strip()
            and str(latest_supersession_payload.get("trigger_ref") or "").strip()
            == str(reference_event.get("event_id") or "").strip()
        ):
            supersession_results.append(
                {
                    "envelope_id": str(latest_supersession_payload.get("envelope_id") or ""),
                    "event_id": str(latest_supersession.get("event_id") or ""),
                    "accepted": True,
                }
            )
            continue
        supersession_results.append(
            _ensure_repo_global_heavy_object_supersession(
                repo_root=repo_root,
                superseded_object_id=superseded_object_id,
                superseded_object_kind=str(candidate.get("superseded_object_kind") or ""),
                superseded_object_ref=superseded_object_ref,
                replacement_object_id=str(candidate.get("replacement_object_id") or ""),
                replacement_object_kind=str(candidate.get("replacement_object_kind") or ""),
                replacement_object_ref=str(candidate.get("replacement_object_ref") or ""),
                runtime_name=str(reference_payload.get("runtime_name") or ""),
                trigger_ref=str(reference_event.get("event_id") or ""),
            )
        )

    gc_results: list[dict[str, Any]] = []
    supersession_latest_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for event in supersession_results:
        event_id = str(dict(event or {}).get("event_id") or "").strip()
        if not event_id:
            continue
        canonical_event = _event_by_id(anchor_state_root, event_id=event_id)
        payload = dict(canonical_event.get("payload") or {})
        key = (
            str(payload.get("superseded_object_id") or "").strip(),
            str(payload.get("superseded_object_ref") or "").strip(),
        )
        if not all(key):
            continue
        supersession_latest_by_identity[key] = canonical_event
    for canonical_supersession in supersession_latest_by_identity.values():
        payload = dict(canonical_supersession.get("payload") or {})
        object_id = str(payload.get("superseded_object_id") or "").strip()
        object_ref = str(payload.get("superseded_object_ref") or "").strip()
        retention_view = query_heavy_object_retention_view(
            anchor_state_root,
            object_id=object_id,
            object_ref=object_ref,
        )
        if not bool(retention_view.get("registered_present")):
            continue
        if bool(retention_view.get("reclaimed_present")) or bool(retention_view.get("gc_eligibility_present")):
            continue
        if int(retention_view.get("active_reference_count") or 0) > 0:
            continue
        if int(retention_view.get("active_pin_count") or 0) > 0:
            continue
        gc_results.append(
            _ensure_repo_global_heavy_object_gc_eligibility(
                repo_root=repo_root,
                object_id=object_id,
                object_kind=str(payload.get("superseded_object_kind") or ""),
                object_ref=object_ref,
                superseded_by_object_id=str(payload.get("replacement_object_id") or ""),
                superseded_by_object_ref=str(payload.get("replacement_object_ref") or ""),
                runtime_name=str(payload.get("runtime_name") or ""),
                trigger_ref=str(canonical_supersession.get("event_id") or ""),
            )
        )

    reclamation_results: list[dict[str, Any]] = []
    gc_latest_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for event in gc_results:
        event_id = str(dict(event or {}).get("event_id") or "").strip()
        if not event_id:
            continue
        canonical_event = _event_by_id(anchor_state_root, event_id=event_id)
        payload = dict(canonical_event.get("payload") or {})
        key = (
            str(payload.get("object_id") or "").strip(),
            str(payload.get("object_ref") or "").strip(),
        )
        if not all(key):
            continue
        gc_latest_by_identity[key] = canonical_event
    for canonical_gc in gc_latest_by_identity.values():
        payload = dict(canonical_gc.get("payload") or {})
        object_id = str(payload.get("object_id") or "").strip()
        object_ref = str(payload.get("object_ref") or "").strip()
        retention_view = query_heavy_object_retention_view(
            anchor_state_root,
            object_id=object_id,
            object_ref=object_ref,
        )
        if not bool(retention_view.get("registered_present")):
            continue
        if not bool(retention_view.get("gc_eligibility_present")) or bool(retention_view.get("reclaimed_present")):
            continue
        if int(retention_view.get("active_reference_count") or 0) > 0:
            continue
        if int(retention_view.get("active_pin_count") or 0) > 0:
            continue
        latest_registration_for_ref = _latest_heavy_object_registration_for_ref(
            state_root=anchor_state_root,
            object_ref=object_ref,
        )
        latest_registration_for_ref_payload = dict(latest_registration_for_ref.get("payload") or {})
        if (
            latest_registration_for_ref_payload
            and str(latest_registration_for_ref_payload.get("object_id") or "").strip() not in {"", object_id}
        ):
            continue
        if not Path(object_ref).expanduser().resolve().exists():
            continue
        latest_reclaimed = _latest_heavy_object_reclaimed_anchor(
            state_root=anchor_state_root,
            object_id=object_id,
            object_ref=object_ref,
        )
        if latest_reclaimed:
            latest_reclaimed_payload = dict(latest_reclaimed.get("payload") or {})
            if bool(latest_reclaimed_payload.get("reclaimed")) and str(latest_reclaimed_payload.get("trigger_ref") or "").strip() == str(canonical_gc.get("event_id") or "").strip():
                reclamation_results.append(
                    {
                        "accepted": True,
                        "envelope_id": str(latest_reclaimed_payload.get("envelope_id") or ""),
                        "request_event_id": str(latest_reclaimed_payload.get("envelope_id") or ""),
                        "result_event_id": str(latest_reclaimed.get("event_id") or ""),
                    }
                )
                continue
        reclamation_results.append(
            _ensure_repo_global_heavy_object_reclamation(
                repo_root=repo_root,
                object_id=object_id,
                object_kind=str(payload.get("object_kind") or ""),
                object_ref=object_ref,
                runtime_name=str(payload.get("runtime_name") or ""),
                trigger_ref=str(canonical_gc.get("event_id") or ""),
            )
        )

    return {
        "managed_duplicate_handoff_candidate_count": len(handoff_candidates),
        "auto_reference_handoff_count": len(
            [item for item in handoff_results if str(dict(item).get("reference_event_id") or "")]
        ),
        "replacement_candidate_count": len(replacement_candidates),
        "auto_reference_release_count": len([item for item in release_results if str(dict(item).get("event_id") or "")]),
        "auto_supersession_count": len([item for item in supersession_results if str(dict(item).get("event_id") or "")]),
        "auto_gc_eligibility_count": len([item for item in gc_results if str(dict(item).get("event_id") or "")]),
        "auto_reclamation_count": len([item for item in reclamation_results if str(dict(item).get("result_event_id") or "")]),
        "auto_reference_handoff_event_ids": sorted(
            str(dict(item).get("reference_event_id") or "")
            for item in handoff_results
            if str(dict(item).get("reference_event_id") or "")
        ),
        "auto_reference_release_event_ids": sorted(
            str(dict(item).get("event_id") or "")
            for item in release_results
            if str(dict(item).get("event_id") or "")
        ),
        "auto_supersession_event_ids": sorted(
            str(dict(item).get("event_id") or "")
            for item in supersession_results
            if str(dict(item).get("event_id") or "")
        ),
        "auto_gc_eligibility_event_ids": sorted(
            str(dict(item).get("event_id") or "")
            for item in gc_results
            if str(dict(item).get("event_id") or "")
        ),
        "auto_reclamation_event_ids": sorted(
            str(dict(item).get("result_event_id") or "")
            for item in reclamation_results
            if str(dict(item).get("result_event_id") or "")
        ),
    }


def _reconcile_repo_global_services_from_control_plane(
    *,
    repo_root: Path,
    poll_interval_s: float,
) -> dict[str, Any]:
    return {
        **_reconcile_repo_upper_services_from_control_plane(
            repo_root=repo_root,
            poll_interval_s=poll_interval_s,
        ),
        **_reconcile_repo_scope_services(
            repo_root=repo_root,
            poll_interval_s=poll_interval_s,
        ),
    }


def _repo_scope_service_signature(
    payload: dict[str, Any] | None,
    *,
    heavy_object_auto_audits: dict[str, Any] | None = None,
    heavy_object_auto_remediations: dict[str, Any] | None = None,
    heavy_object_auto_lifecycle: dict[str, Any] | None = None,
) -> str:
    payload = dict(payload or {})
    signature_payload = {
        "repo_reactor_residency_guard_pid": int(
            dict(payload.get("repo_reactor_residency_guard") or {}).get("pid") or 0
        ),
        "repo_reactor_pid": int(dict(payload.get("repo_reactor") or {}).get("pid") or 0),
        "host_child_launch_supervisor_pid": int(
            dict(payload.get("host_child_launch_supervisor") or {}).get("pid") or 0
        ),
        "repo_evaluator_supervision_pid": int(
            dict(payload.get("repo_evaluator_supervision") or {}).get("pid") or 0
        ),
        "repo_evaluator_supervision_tracked_submission_count": int(payload.get("tracked_submission_count") or 0),
        "housekeeping_reap_controller_pid": int(
            dict(payload.get("housekeeping_reap_controller") or {}).get("pid") or 0
        ),
        "heavy_object_auto_audits": {
            str(kind): {
                "unmanaged_candidate_count": int(dict(summary or {}).get("unmanaged_candidate_count") or 0),
                "audit_signature": str(dict(summary or {}).get("audit_signature") or ""),
                "requirement_event_id": str(dict(summary or {}).get("requirement_event_id") or ""),
                "auto_audit_envelope_id": str(dict(summary or {}).get("auto_audit_envelope_id") or ""),
                "auto_audit_result_event_id": str(dict(summary or {}).get("auto_audit_result_event_id") or ""),
            }
            for kind, summary in sorted(dict(heavy_object_auto_audits or {}).items())
        },
        "heavy_object_auto_remediations": {
            str(kind): {
                "unmanaged_candidate_count": int(dict(summary or {}).get("unmanaged_candidate_count") or 0),
                "remediation_signature": str(dict(summary or {}).get("remediation_signature") or ""),
                "requirement_event_id": str(dict(summary or {}).get("requirement_event_id") or ""),
                "audit_result_event_id": str(dict(summary or {}).get("audit_result_event_id") or ""),
                "auto_remediation_envelope_id": str(dict(summary or {}).get("auto_remediation_envelope_id") or ""),
                "auto_remediation_result_event_id": str(dict(summary or {}).get("auto_remediation_result_event_id") or ""),
            }
            for kind, summary in sorted(dict(heavy_object_auto_remediations or {}).items())
        },
        "heavy_object_auto_lifecycle": {
            "managed_duplicate_handoff_candidate_count": int(
                dict(heavy_object_auto_lifecycle or {}).get("managed_duplicate_handoff_candidate_count") or 0
            ),
            "auto_reference_handoff_count": int(
                dict(heavy_object_auto_lifecycle or {}).get("auto_reference_handoff_count") or 0
            ),
            "replacement_candidate_count": int(dict(heavy_object_auto_lifecycle or {}).get("replacement_candidate_count") or 0),
            "auto_reference_release_count": int(dict(heavy_object_auto_lifecycle or {}).get("auto_reference_release_count") or 0),
            "auto_supersession_count": int(dict(heavy_object_auto_lifecycle or {}).get("auto_supersession_count") or 0),
            "auto_gc_eligibility_count": int(dict(heavy_object_auto_lifecycle or {}).get("auto_gc_eligibility_count") or 0),
            "auto_reclamation_count": int(dict(heavy_object_auto_lifecycle or {}).get("auto_reclamation_count") or 0),
            "auto_reference_handoff_event_ids": [str(item) for item in list(dict(heavy_object_auto_lifecycle or {}).get("auto_reference_handoff_event_ids") or [])],
            "auto_reference_release_event_ids": [str(item) for item in list(dict(heavy_object_auto_lifecycle or {}).get("auto_reference_release_event_ids") or [])],
            "auto_supersession_event_ids": [str(item) for item in list(dict(heavy_object_auto_lifecycle or {}).get("auto_supersession_event_ids") or [])],
            "auto_gc_eligibility_event_ids": [str(item) for item in list(dict(heavy_object_auto_lifecycle or {}).get("auto_gc_eligibility_event_ids") or [])],
            "auto_reclamation_event_ids": [str(item) for item in list(dict(heavy_object_auto_lifecycle or {}).get("auto_reclamation_event_ids") or [])],
        },
    }
    return json.dumps(signature_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def run_repo_control_plane_loop(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S,
    idle_exit_after_s: float = _DEFAULT_REPO_CONTROL_PLANE_SERVICE_IDLE_EXIT_AFTER_S,
) -> int:
    resolved_repo_root = _repo_root(repo_root)
    poll_interval_value = max(0.05, float(poll_interval_s))
    idle_exit_value = max(0.0, float(idle_exit_after_s))
    service_mode = idle_exit_value <= 0.0
    lease_epoch = _repo_control_plane_next_lease_epoch(repo_root=resolved_repo_root)
    started_at_utc = _now_iso()
    heartbeat_ordinal = 0
    last_activity = time.monotonic()

    with _repo_control_plane_runtime_marker(
        repo_root=resolved_repo_root,
        poll_interval_s=poll_interval_value,
        idle_exit_after_s=idle_exit_value,
        lease_epoch=lease_epoch,
        started_at_utc=started_at_utc,
    ):
        startup_outcome = _emit_repo_control_plane_authority(
            repo_root=resolved_repo_root,
            lease_epoch=lease_epoch,
            heartbeat_ordinal=heartbeat_ordinal,
            poll_interval_s=poll_interval_value,
            idle_exit_after_s=idle_exit_value,
            process_started_at_utc=started_at_utc,
        )
        if _repo_control_plane_should_self_fence(startup_outcome):
            _emit_repo_control_plane_exit_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                process_started_at_utc=started_at_utc,
                exit_reason="lease_fenced",
            )
            return 0
        last_activity = time.monotonic()
        last_signature = ""
        while True:
            cleanup_authority = _repo_control_plane_cleanup_authority(repo_root=resolved_repo_root)
            if cleanup_authority is not None:
                _emit_repo_control_plane_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="cleanup_committed",
                    cleanup_authority=cleanup_authority,
                )
                return 0
            if service_mode:
                services_payload = _reconcile_repo_global_services_from_control_plane(
                    repo_root=resolved_repo_root,
                    poll_interval_s=poll_interval_value,
                )
                heavy_object_auto_audits = _reconcile_repo_global_heavy_object_authority_gap_audits_from_control_plane(
                    repo_root=resolved_repo_root,
                )
                heavy_object_auto_remediations = _reconcile_repo_global_heavy_object_authority_gap_repo_remediations_from_control_plane(
                    repo_root=resolved_repo_root,
                    heavy_object_auto_audits=heavy_object_auto_audits,
                )
                heavy_object_auto_lifecycle = _reconcile_repo_global_heavy_object_auto_lifecycle_from_control_plane(
                    repo_root=resolved_repo_root,
                )
            else:
                # Finite-idle direct runs are used by authoritative-exit and lease tests.
                # They must not implicitly materialize the background repo service stack,
                # or later watchdog restarts will overwrite the very exit facts being audited.
                services_payload = {}
                heavy_object_auto_audits = {}
                heavy_object_auto_remediations = {}
                heavy_object_auto_lifecycle = {}
            signature = _repo_scope_service_signature(
                services_payload,
                heavy_object_auto_audits=heavy_object_auto_audits,
                heavy_object_auto_remediations=heavy_object_auto_remediations,
                heavy_object_auto_lifecycle=heavy_object_auto_lifecycle,
            )
            if signature != last_signature:
                last_signature = signature
                last_activity = time.monotonic()
            elif idle_exit_value > 0.0 and (time.monotonic() - last_activity) >= idle_exit_value:
                _emit_repo_control_plane_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="idle_exit",
                )
                return 0
            heartbeat_ordinal += 1
            heartbeat_outcome = _emit_repo_control_plane_authority(
                repo_root=resolved_repo_root,
                lease_epoch=lease_epoch,
                heartbeat_ordinal=heartbeat_ordinal,
                poll_interval_s=poll_interval_value,
                idle_exit_after_s=idle_exit_value,
                process_started_at_utc=started_at_utc,
            )
            if _repo_control_plane_should_self_fence(heartbeat_outcome):
                _emit_repo_control_plane_exit_authority(
                    repo_root=resolved_repo_root,
                    lease_epoch=lease_epoch,
                    poll_interval_s=poll_interval_value,
                    idle_exit_after_s=idle_exit_value,
                    process_started_at_utc=started_at_utc,
                    exit_reason="lease_fenced",
                )
                return 0
            time.sleep(poll_interval_value)


def ensure_repo_control_plane_running(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S,
    startup_timeout_s: float = _REPO_CONTROL_PLANE_STARTUP_TIMEOUT_S,
    trigger_kind: str = "explicit_repo_bootstrap",
    trigger_ref: str = "",
) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    cleanup_authority = _repo_control_plane_cleanup_authority(repo_root=resolved_repo_root)
    if cleanup_authority is not None:
        return {
            "bootstrap_status": "blocked_cleanup_committed",
            "service_kind": "repo_control_plane",
            "repo_root": str(resolved_repo_root),
            "cleanup_authority_event_id": str(cleanup_authority.get("event_id") or ""),
            "cleanup_authority_event_type": str(cleanup_authority.get("event_type") or ""),
            "cleanup_authority_seq": int(cleanup_authority.get("seq") or 0),
        }
    _commit_repo_control_plane_requirement(
        repo_root=resolved_repo_root,
        trigger_kind=trigger_kind,
        trigger_ref=trigger_ref,
        producer="runtime.control_plane.ensure_repo_control_plane_running",
    )
    current_code_fingerprint = _runtime_code_fingerprint()
    existing = live_repo_control_plane_runtime(repo_root=resolved_repo_root)
    if existing is not None:
        if str(existing.get("code_fingerprint") or "").strip() == current_code_fingerprint:
            return existing
        _terminate_pid(int(existing.get("pid") or 0))
        repo_control_plane_runtime_ref(repo_root=resolved_repo_root).unlink(missing_ok=True)

    runtime_ref = repo_control_plane_runtime_ref(repo_root=resolved_repo_root)
    log_ref = repo_control_plane_log_ref(repo_root=resolved_repo_root)
    runtime_ref.parent.mkdir(parents=True, exist_ok=True)
    package_repo_root = Path(__file__).resolve().parents[2]
    with log_ref.open("a", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.runtime.control_plane",
                "--repo-root",
                str(resolved_repo_root),
                "--poll-interval-s",
                str(max(0.05, float(poll_interval_s))),
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
        payload = live_repo_control_plane_runtime(repo_root=resolved_repo_root)
        if payload is not None:
            return payload
        exit_code = proc.poll()
        if exit_code is not None:
            payload = live_repo_control_plane_runtime(repo_root=resolved_repo_root)
            if payload is not None:
                return payload
            if int(exit_code) == 0:
                time.sleep(0.05)
                continue
            detail = log_ref.read_text(encoding="utf-8", errors="replace").strip() if log_ref.exists() else ""
            raise RuntimeError(detail or f"repo control-plane exited early with code {exit_code}")
        time.sleep(0.05)
    raise TimeoutError(f"timed out waiting for repo control-plane runtime marker at {runtime_ref}")


def ensure_repo_control_plane_services_running(
    *,
    repo_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S,
    trigger_kind: str = "explicit_repo_bootstrap",
    trigger_ref: str = "",
) -> dict[str, Any]:
    resolved_repo_root = Path(repo_root).expanduser().resolve()
    poll_interval_value = max(0.05, float(poll_interval_s))

    residency_guard_payload = ensure_repo_reactor_residency_guard_running(
        repo_root=resolved_repo_root,
        poll_interval_s=poll_interval_value,
        trigger_kind=trigger_kind,
        trigger_ref=trigger_ref,
    )
    deadline = time.monotonic() + max(1.0, _REPO_CONTROL_PLANE_STARTUP_TIMEOUT_S)
    reactor_payload: dict[str, Any] | None = None
    control_plane_payload: dict[str, Any] | None = None
    subordinate_payloads: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        reactor_payload = live_repo_reactor_runtime(repo_root=resolved_repo_root)
        control_plane_payload = live_repo_control_plane_runtime(repo_root=resolved_repo_root)
        supervisor_payload = supervisor_module.live_supervisor_runtime(repo_root=resolved_repo_root)
        housekeeping_payload = live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root)
        if (
            reactor_payload is not None
            and control_plane_payload is not None
            and supervisor_payload is not None
            and housekeeping_payload is not None
        ):
            subordinate_payloads = {
                "host_child_launch_supervisor": dict(supervisor_payload or {}),
                "housekeeping_reap_controller": dict(housekeeping_payload or {}),
            }
            break
        time.sleep(0.05)
    if reactor_payload is None:
        reactor_payload = live_repo_reactor_runtime(repo_root=resolved_repo_root)
    if control_plane_payload is None:
        control_plane_payload = live_repo_control_plane_runtime(repo_root=resolved_repo_root)
    if subordinate_payloads is None:
        subordinate_payloads = _reconcile_repo_scope_services(
            repo_root=resolved_repo_root,
            poll_interval_s=poll_interval_value,
        )
    if reactor_payload is None:
        reactor_payload = _reconcile_repo_reactor_from_residency_guard(
            repo_root=resolved_repo_root,
            poll_interval_s=poll_interval_value,
        )
    if control_plane_payload is None:
        reactor_runtime_ref = repo_reactor_runtime_ref(repo_root=resolved_repo_root)
        control_plane_payload = ensure_repo_control_plane_running(
            repo_root=resolved_repo_root,
            poll_interval_s=poll_interval_value,
            trigger_kind="repo_reactor_watchdog",
            trigger_ref=str(reactor_runtime_ref),
        )
    return {
        "repo_root": str(resolved_repo_root),
        "repo_reactor_residency_guard": dict(residency_guard_payload or {}),
        "repo_reactor": dict(reactor_payload or {}),
        "repo_control_plane": dict(control_plane_payload or {}),
        "repo_evaluator_supervision": dict(subordinate_payloads.get("repo_evaluator_supervision") or {}),
        "tracked_submission_count": int(subordinate_payloads.get("tracked_submission_count") or 0),
        "host_child_launch_supervisor": dict(subordinate_payloads.get("host_child_launch_supervisor") or {}),
        "housekeeping_reap_controller": dict(subordinate_payloads.get("housekeeping_reap_controller") or {}),
    }


def ensure_repo_control_plane_service_for_runtime_root(
    *,
    state_root: str | Path,
    poll_interval_s: float = _DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S,
    trigger_kind: str = "runtime_root_bootstrap",
) -> dict[str, Any]:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    repo_root = _top_level_repo_root_from_runtime_root(state_root=resolved_state_root)
    if repo_root is None:
        return {
            "bootstrap_status": "skipped",
            "skip_reason": "non_top_level_runtime_root",
            "state_root": str(resolved_state_root),
        }
    payload = ensure_repo_control_plane_services_running(
        repo_root=repo_root,
        poll_interval_s=float(poll_interval_s),
        trigger_kind=trigger_kind,
        trigger_ref=str(resolved_state_root),
    )
    return {
        **dict(payload or {}),
        "bootstrap_status": "ready",
        "repo_root": str(repo_root),
        "state_root": str(resolved_state_root),
    }


def cleanup_repo_services(
    *,
    repo_root: str | Path,
    settle_timeout_s: float = 6.0,
    poll_interval_s: float = 0.05,
    cleanup_kind: str = "repo_service_cleanup",
    producer: str = "runtime.control_plane.cleanup_repo_services",
) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    cleanup_kind_value = str(cleanup_kind or "").strip() or "repo_service_cleanup"
    producer_value = str(producer or "").strip() or "runtime.control_plane.cleanup_repo_services"
    service_roots = {
        "repo_reactor_residency_guard": repo_reactor_residency_guard_runtime_root(repo_root=resolved_repo_root),
        "repo_reactor": repo_reactor_runtime_root(repo_root=resolved_repo_root),
        "repo_control_plane": repo_control_plane_runtime_root(repo_root=resolved_repo_root),
        "repo_evaluator_supervision": evaluator_supervision_module.repo_evaluator_supervision_root(
            state_root=repo_anchor_runtime_root(repo_root=resolved_repo_root)
        ),
        "host_child_launch_supervisor": supervisor_module.host_supervisor_runtime_root(repo_root=resolved_repo_root),
        "housekeeping": housekeeping_runtime_root(repo_root=resolved_repo_root),
    }
    live_payloads = {
        "repo_reactor_residency_guard": dict(
            live_repo_reactor_residency_guard_runtime(repo_root=resolved_repo_root) or {}
        ),
        "repo_reactor": dict(live_repo_reactor_runtime(repo_root=resolved_repo_root) or {}),
        "repo_control_plane": dict(live_repo_control_plane_runtime(repo_root=resolved_repo_root) or {}),
        "repo_evaluator_supervision": dict(
            evaluator_supervision_module.live_repo_evaluator_supervision_runtime(
                repo_evaluator_supervision_root=evaluator_supervision_module.repo_evaluator_supervision_root(
                    state_root=repo_anchor_runtime_root(repo_root=resolved_repo_root)
                )
            )
            or {}
        ),
        "host_child_launch_supervisor": dict(supervisor_module.live_supervisor_runtime(repo_root=resolved_repo_root) or {}),
        "housekeeping": dict(live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root) or {}),
    }
    initial_orphan_service_pids = _repo_service_processes_by_kind(repo_root=resolved_repo_root)
    cleanup_events: dict[str, str] = {}
    retired_pids: dict[str, int] = {}
    fallback_terminated_orphan_pids: dict[str, list[int]] = {}
    fallback_terminated_live_pids: dict[str, list[int]] = {}

    def _record_cleanup_event(runtime_name: str) -> None:
        if runtime_name in cleanup_events:
            return
        state_root_path = service_roots[runtime_name]
        live_payload = dict(live_payloads.get(runtime_name) or {})
        cleanup_event = commit_test_runtime_cleanup_authority(
            state_root=state_root_path,
            repo_root=resolved_repo_root,
            cleanup_kind=cleanup_kind_value,
            producer=producer_value,
            extra_payload={
                "service_kind": runtime_name,
            },
        )
        cleanup_events[runtime_name] = str(cleanup_event.get("event_id") or "")
        retired_pids[runtime_name] = int(live_payload.get("pid") or 0)

    def _ensure_cleanup_authority_for_detected_services(
        *,
        live_services: dict[str, dict[str, Any]] | None = None,
        orphan_service_pids: dict[str, list[int]] | None = None,
    ) -> None:
        live_services = dict(live_services or {})
        orphan_service_pids = dict(orphan_service_pids or {})
        for runtime_name, state_root_path in service_roots.items():
            live_payload = dict(live_payloads.get(runtime_name) or {})
            orphan_pids = [
                int(pid)
                for pid in list(orphan_service_pids.get(runtime_name) or [])
                if int(pid or 0) > 0 and _pid_alive(int(pid))
            ]
            if (
                live_payload
                or state_root_path.exists()
                or dict(live_services.get(runtime_name) or {})
                or orphan_pids
            ):
                _record_cleanup_event(runtime_name)

    _ensure_cleanup_authority_for_detected_services(
        orphan_service_pids=initial_orphan_service_pids,
    )

    def _remaining_retired_pids() -> dict[str, int]:
        remaining: dict[str, int] = {}
        guard_pid = int(retired_pids.get("repo_reactor_residency_guard") or 0)
        if guard_pid > 0 and _repo_reactor_residency_guard_process_matches(pid=guard_pid, repo_root=resolved_repo_root):
            remaining["repo_reactor_residency_guard"] = guard_pid
        reactor_pid = int(retired_pids.get("repo_reactor") or 0)
        if reactor_pid > 0 and _repo_reactor_process_matches(pid=reactor_pid, repo_root=resolved_repo_root):
            remaining["repo_reactor"] = reactor_pid
        control_plane_pid = int(retired_pids.get("repo_control_plane") or 0)
        if control_plane_pid > 0 and _repo_control_plane_process_matches(pid=control_plane_pid, repo_root=resolved_repo_root):
            remaining["repo_control_plane"] = control_plane_pid
        evaluator_supervision_pid = int(retired_pids.get("repo_evaluator_supervision") or 0)
        if evaluator_supervision_pid > 0 and _repo_evaluator_supervision_process_matches(
            pid=evaluator_supervision_pid,
            repo_root=resolved_repo_root,
        ):
            remaining["repo_evaluator_supervision"] = evaluator_supervision_pid
        supervisor_pid = int(retired_pids.get("host_child_launch_supervisor") or 0)
        if supervisor_pid > 0 and supervisor_module._host_supervisor_process_matches(
            pid=supervisor_pid,
            repo_root=resolved_repo_root,
        ):
            remaining["host_child_launch_supervisor"] = supervisor_pid
        housekeeping_pid = int(retired_pids.get("housekeeping") or 0)
        if housekeeping_pid > 0 and _housekeeping_controller_process_matches(
            pid=housekeeping_pid,
            repo_root=resolved_repo_root,
        ):
            remaining["housekeeping"] = housekeeping_pid
        return remaining

    def _remaining_orphan_service_pids() -> dict[str, list[int]]:
        matches = _repo_service_processes_by_kind(repo_root=resolved_repo_root)
        remaining: dict[str, list[int]] = {}
        for runtime_name, pids in matches.items():
            filtered = sorted(
                {
                    int(pid)
                    for pid in list(pids or [])
                    if int(pid or 0) > 0 and _pid_alive(int(pid))
                }
            )
            if filtered:
                remaining[runtime_name] = filtered
        return remaining

    def _remaining_live_services() -> dict[str, dict[str, Any]]:
        remaining: dict[str, dict[str, Any]] = {}
        guard_payload = dict(live_repo_reactor_residency_guard_runtime(repo_root=resolved_repo_root) or {})
        if guard_payload:
            remaining["repo_reactor_residency_guard"] = guard_payload
        reactor_payload = dict(live_repo_reactor_runtime(repo_root=resolved_repo_root) or {})
        if reactor_payload:
            remaining["repo_reactor"] = reactor_payload
        control_plane_payload = dict(live_repo_control_plane_runtime(repo_root=resolved_repo_root) or {})
        if control_plane_payload:
            remaining["repo_control_plane"] = control_plane_payload
        evaluator_supervision_payload = dict(
            evaluator_supervision_module.live_repo_evaluator_supervision_runtime(
                repo_evaluator_supervision_root=evaluator_supervision_module.repo_evaluator_supervision_root(
                    state_root=repo_anchor_runtime_root(repo_root=resolved_repo_root)
                )
            )
            or {}
        )
        if evaluator_supervision_payload:
            remaining["repo_evaluator_supervision"] = evaluator_supervision_payload
        supervisor_payload = dict(supervisor_module.live_supervisor_runtime(repo_root=resolved_repo_root) or {})
        if supervisor_payload:
            remaining["host_child_launch_supervisor"] = supervisor_payload
        housekeeping_payload = dict(live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root) or {})
        if housekeeping_payload:
            remaining["housekeeping"] = housekeeping_payload
        return remaining

    deadline = time.monotonic() + max(0.0, float(settle_timeout_s))
    remaining = _remaining_live_services()
    remaining_retired_pids = _remaining_retired_pids()
    remaining_orphan_service_pids = _remaining_orphan_service_pids()
    _ensure_cleanup_authority_for_detected_services(
        live_services=remaining,
        orphan_service_pids=remaining_orphan_service_pids,
    )
    while (remaining or remaining_retired_pids or remaining_orphan_service_pids) and time.monotonic() < deadline:
        time.sleep(max(0.01, float(poll_interval_s)))
        remaining = _remaining_live_services()
        remaining_retired_pids = _remaining_retired_pids()
        remaining_orphan_service_pids = _remaining_orphan_service_pids()
        _ensure_cleanup_authority_for_detected_services(
            live_services=remaining,
            orphan_service_pids=remaining_orphan_service_pids,
        )

    if remaining or remaining_retired_pids:
        termination_order = (
            "repo_control_plane",
            "repo_evaluator_supervision",
            "host_child_launch_supervisor",
            "housekeeping",
            "repo_reactor",
            "repo_reactor_residency_guard",
        )
        for runtime_name in termination_order:
            live_payload = dict(remaining.get(runtime_name) or {})
            live_pid = int(live_payload.get("pid") or 0)
            if live_pid > 0 and _pid_alive(live_pid):
                _terminate_pid(live_pid)
                fallback_terminated_live_pids.setdefault(runtime_name, []).append(live_pid)
            retired_pid = int(remaining_retired_pids.get(runtime_name) or 0)
            if retired_pid > 0 and _pid_alive(retired_pid):
                _terminate_pid(retired_pid)
                fallback_terminated_live_pids.setdefault(runtime_name, []).append(retired_pid)
        fallback_deadline = time.monotonic() + max(0.5, float(settle_timeout_s))
        remaining = _remaining_live_services()
        remaining_retired_pids = _remaining_retired_pids()
        remaining_orphan_service_pids = _remaining_orphan_service_pids()
        _ensure_cleanup_authority_for_detected_services(
            live_services=remaining,
            orphan_service_pids=remaining_orphan_service_pids,
        )
        while (remaining or remaining_retired_pids or remaining_orphan_service_pids) and time.monotonic() < fallback_deadline:
            time.sleep(max(0.01, float(poll_interval_s)))
            remaining = _remaining_live_services()
            remaining_retired_pids = _remaining_retired_pids()
            remaining_orphan_service_pids = _remaining_orphan_service_pids()
            _ensure_cleanup_authority_for_detected_services(
                live_services=remaining,
                orphan_service_pids=remaining_orphan_service_pids,
            )

    if remaining_orphan_service_pids:
        termination_order = (
            "repo_control_plane",
            "repo_evaluator_supervision",
            "host_child_launch_supervisor",
            "housekeeping",
            "repo_reactor",
            "repo_reactor_residency_guard",
        )
        for runtime_name in termination_order:
            for pid in list(remaining_orphan_service_pids.get(runtime_name) or []):
                _terminate_pid(pid)
                fallback_terminated_orphan_pids.setdefault(runtime_name, []).append(int(pid))
        fallback_deadline = time.monotonic() + max(0.5, float(settle_timeout_s))
        remaining = _remaining_live_services()
        remaining_retired_pids = _remaining_retired_pids()
        remaining_orphan_service_pids = _remaining_orphan_service_pids()
        _ensure_cleanup_authority_for_detected_services(
            live_services=remaining,
            orphan_service_pids=remaining_orphan_service_pids,
        )
        while (remaining or remaining_retired_pids or remaining_orphan_service_pids) and time.monotonic() < fallback_deadline:
            time.sleep(max(0.01, float(poll_interval_s)))
            remaining = _remaining_live_services()
            remaining_retired_pids = _remaining_retired_pids()
            remaining_orphan_service_pids = _remaining_orphan_service_pids()
            _ensure_cleanup_authority_for_detected_services(
                live_services=remaining,
                orphan_service_pids=remaining_orphan_service_pids,
            )

    quiesced_events: dict[str, str] = {}
    if not remaining and not remaining_retired_pids and not remaining_orphan_service_pids:
        from loop_product.runtime_identity import runtime_root_identity_ref

        for runtime_name, cleanup_event_id in cleanup_events.items():
            quiesced_event = commit_runtime_root_quiesced_from_cleanup(
                state_root=service_roots[runtime_name],
                repo_root=resolved_repo_root,
                cleanup_event_id=cleanup_event_id,
                producer=producer_value,
                extra_payload={
                    "service_kind": runtime_name,
                },
            )
            quiesced_events[runtime_name] = str(quiesced_event.get("event_id") or "")
            runtime_root_identity_ref(service_roots[runtime_name]).unlink(missing_ok=True)

    return {
        "repo_root": str(resolved_repo_root),
        "cleanup_event_ids": cleanup_events,
        "runtime_root_quiesced_event_ids": quiesced_events,
        "retired_pids": retired_pids,
        "fallback_terminated_live_pids": fallback_terminated_live_pids,
        "fallback_terminated_orphan_pids": fallback_terminated_orphan_pids,
        "remaining_live_services": remaining,
        "remaining_retired_pids": remaining_retired_pids,
        "remaining_orphan_service_pids": remaining_orphan_service_pids,
        "quiesced": not remaining and not remaining_retired_pids and not remaining_orphan_service_pids,
    }


def cleanup_test_repo_services(
    *,
    repo_root: str | Path,
    settle_timeout_s: float = 6.0,
    poll_interval_s: float = 0.05,
    temp_test_repo_roots: list[str | Path] | None = None,
) -> dict[str, Any]:
    resolved_repo_root = _repo_root(repo_root)
    receipt = cleanup_repo_services(
        repo_root=repo_root,
        settle_timeout_s=settle_timeout_s,
        poll_interval_s=poll_interval_s,
        cleanup_kind="test_repo_service_cleanup",
        producer="runtime.control_plane.cleanup_test_repo_services",
    )
    selected_temp_test_repo_roots: list[Path] = []
    temp_test_repo_receipts: list[dict[str, Any]] = []
    temp_test_repo_quiesced = True
    if not _is_temp_test_repo_root(resolved_repo_root):
        if temp_test_repo_roots is None:
            selected_temp_test_repo_roots = _iter_temp_test_repo_roots(exclude_repo_roots={resolved_repo_root})
        else:
            selected_temp_test_repo_roots = sorted(
                {
                    _repo_root(path)
                    for path in list(temp_test_repo_roots or [])
                    if _is_temp_test_repo_root(_repo_root(path)) and _repo_root(path) != resolved_repo_root
                },
                key=str,
            )
        if selected_temp_test_repo_roots:
            temp_repo_receipt_by_root: dict[str, dict[str, Any]] = {}
            pending_temp_test_repo_roots = list(selected_temp_test_repo_roots)
            temp_tree_deadline = time.monotonic() + max(0.0, float(settle_timeout_s))
            while pending_temp_test_repo_roots:
                tree_receipt = cleanup_repo_tree_services(
                    tree_root=Path(tempfile.gettempdir()).expanduser().resolve(),
                    repo_roots=pending_temp_test_repo_roots,
                    anchor_repo_root=resolved_repo_root,
                    include_anchor_repo_root=False,
                    settle_timeout_s=min(float(settle_timeout_s), 0.5),
                    poll_interval_s=min(float(poll_interval_s), 0.05),
                    cleanup_kind="test_repo_service_cleanup",
                    producer="runtime.control_plane.cleanup_test_repo_services",
                )
                pending_temp_test_repo_roots = []
                for repo_receipt in list(tree_receipt.get("repo_receipts") or []):
                    normalized_receipt = dict(repo_receipt or {})
                    repo_root_text = str(normalized_receipt.get("repo_root") or "")
                    if not repo_root_text:
                        continue
                    temp_repo_receipt_by_root[repo_root_text] = normalized_receipt
                    if not bool(normalized_receipt.get("quiesced")):
                        pending_temp_test_repo_roots.append(Path(repo_root_text))
                if not pending_temp_test_repo_roots or time.monotonic() >= temp_tree_deadline:
                    break
                time.sleep(max(0.01, min(float(poll_interval_s), 0.05)))
            temp_test_repo_receipts = [
                dict(temp_repo_receipt_by_root.get(str(path)) or {"repo_root": str(path), "quiesced": False})
                for path in selected_temp_test_repo_roots
            ]
            temp_test_repo_quiesced = all(
                bool(dict(temp_repo_receipt_by_root.get(str(path)) or {}).get("quiesced"))
                for path in selected_temp_test_repo_roots
            )
    receipt["temp_test_repo_roots"] = [str(path) for path in selected_temp_test_repo_roots]
    receipt["temp_test_repo_receipts"] = temp_test_repo_receipts
    receipt["temp_test_repo_quiesced"] = temp_test_repo_quiesced
    receipt["quiesced"] = bool(receipt.get("quiesced")) and temp_test_repo_quiesced
    return receipt


def cleanup_repo_tree_services(
    *,
    tree_root: str | Path,
    repo_roots: list[str | Path] | None = None,
    anchor_repo_root: str | Path | None = None,
    include_anchor_repo_root: bool = False,
    settle_timeout_s: float = 6.0,
    poll_interval_s: float = 0.05,
    cleanup_kind: str = "batch_repo_tree_cleanup",
    producer: str = "runtime.control_plane.cleanup_repo_tree_services",
) -> dict[str, Any]:
    resolved_tree_root = Path(tree_root).expanduser().resolve()
    resolved_anchor_repo_root = (
        _repo_root(anchor_repo_root)
        if anchor_repo_root not in (None, "")
        else None
    )
    explicit_repo_roots = [
        _repo_root(path)
        for path in list(repo_roots or [])
    ]
    if explicit_repo_roots:
        target_repo_roots = sorted(
            {path for path in explicit_repo_roots},
            key=lambda path: (len(path.parts), str(path)),
        )
    else:
        target_repo_roots = sorted(
            {
                loop_dir.parent.resolve()
                for loop_dir in resolved_tree_root.rglob(".loop")
                if loop_dir.is_dir()
            },
            key=lambda path: (len(path.parts), str(path)),
        )
    if resolved_anchor_repo_root is not None and not include_anchor_repo_root:
        target_repo_roots = [path for path in target_repo_roots if path != resolved_anchor_repo_root]
    repo_receipts: list[dict[str, Any]] = []
    for target_repo_root in target_repo_roots:
        receipt = cleanup_repo_services(
            repo_root=target_repo_root,
            settle_timeout_s=settle_timeout_s,
            poll_interval_s=poll_interval_s,
            cleanup_kind=cleanup_kind,
            producer=producer,
        )
        repo_receipts.append(
            {
                "repo_root": str(target_repo_root),
                "quiesced": bool(receipt.get("quiesced")),
                "cleanup_event_ids": dict(receipt.get("cleanup_event_ids") or {}),
                "runtime_root_quiesced_event_ids": dict(receipt.get("runtime_root_quiesced_event_ids") or {}),
                "retired_pids": dict(receipt.get("retired_pids") or {}),
                "fallback_terminated_orphan_pids": dict(receipt.get("fallback_terminated_orphan_pids") or {}),
                "remaining_live_services": dict(receipt.get("remaining_live_services") or {}),
                "remaining_retired_pids": dict(receipt.get("remaining_retired_pids") or {}),
                "remaining_orphan_service_pids": dict(receipt.get("remaining_orphan_service_pids") or {}),
            }
        )
    quiesced_repo_count = sum(1 for receipt in repo_receipts if bool(receipt.get("quiesced")))
    return {
        "tree_root": str(resolved_tree_root),
        "anchor_repo_root": str(resolved_anchor_repo_root or ""),
        "include_anchor_repo_root": bool(include_anchor_repo_root),
        "repo_roots": [str(path) for path in target_repo_roots],
        "repo_receipts": repo_receipts,
        "repo_count": len(target_repo_roots),
        "quiesced_repo_count": quiesced_repo_count,
        "failed_repo_count": len(target_repo_roots) - quiesced_repo_count,
        "quiesced": quiesced_repo_count == len(target_repo_roots),
    }


def repo_tree_cleanup_receipt_ref(*, state_root: str | Path, envelope_id: str) -> Path:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        raise ValueError("envelope_id is required")
    return (resolved_state_root / "artifacts" / "repo_tree_cleanup" / f"{normalized_envelope_id}.json").resolve()


def heavy_object_authority_gap_repo_remediation_receipt_ref(*, state_root: str | Path, envelope_id: str) -> Path:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        raise ValueError("envelope_id is required")
    return (
        resolved_state_root
        / "artifacts"
        / "heavy_object_authority_gap_repo_remediation"
        / f"{normalized_envelope_id}.json"
    ).resolve()


def heavy_object_authority_gap_audit_receipt_ref(*, state_root: str | Path, envelope_id: str) -> Path:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        raise ValueError("envelope_id is required")
    return (
        resolved_state_root
        / "artifacts"
        / "heavy_object_authority_gap_audit"
        / f"{normalized_envelope_id}.json"
    ).resolve()


def heavy_object_reclamation_receipt_ref(*, state_root: str | Path, envelope_id: str) -> Path:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        raise ValueError("envelope_id is required")
    return (resolved_state_root / "artifacts" / "heavy_object_reclamation" / f"{normalized_envelope_id}.json").resolve()


def heavy_object_discovery_receipt_ref(*, state_root: str | Path, envelope_id: str) -> Path:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        raise ValueError("envelope_id is required")
    return (resolved_state_root / "artifacts" / "heavy_object_discovery" / f"{normalized_envelope_id}.json").resolve()


def _ensure_anchor_kernel_state(*, state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.policy import kernel_execution_policy, kernel_reasoning_profile
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    kernel_state_ref = resolved_state_root / "state" / "kernel_state.json"
    if kernel_state_ref.exists():
        return
    authority = kernel_internal_authority()
    ensure_runtime_tree(resolved_state_root)
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise ambient repo-global control-plane accepted commands",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy=kernel_execution_policy(),
        reasoning_profile=kernel_reasoning_profile(),
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/repo_global_control_plane_summary.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    kernel_state = KernelState(
        task_id="repo-global-control-plane",
        root_goal="supervise ambient repo-global control-plane accepted commands",
        root_node_id=root_node.node_id,
    )
    kernel_state.register_node(root_node)
    persist_kernel_state(resolved_state_root, kernel_state, authority=authority)

def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_matches_heavy_object_identity(
    payload: dict[str, Any],
    *,
    object_id: str,
    object_ref: str,
    reference_ref_fallback: str = "",
) -> bool:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    payload_object_id = str(payload.get("object_id") or "").strip()
    payload_object_ref = str(payload.get("object_ref") or "").strip()
    payload_reference_ref = str(payload.get(reference_ref_fallback) or "").strip() if reference_ref_fallback else ""

    if normalized_object_id and normalized_object_ref:
        return payload_object_id == normalized_object_id and (
            payload_object_ref == normalized_object_ref
            or (bool(reference_ref_fallback) and payload_reference_ref == normalized_object_ref)
        )
    if normalized_object_id:
        return payload_object_id == normalized_object_id
    if normalized_object_ref:
        return payload_object_ref == normalized_object_ref or (
            bool(reference_ref_fallback) and payload_reference_ref == normalized_object_ref
        )
    return False


def _heavy_object_identity_tuple(payload: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(payload.get("object_id") or "").strip(),
        str(payload.get("object_ref") or "").strip(),
        str(payload.get("object_kind") or "").strip(),
    )


def _heavy_object_reference_holder_key(payload: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(payload.get("reference_ref") or "").strip(),
        str(payload.get("reference_holder_kind") or "").strip(),
        str(payload.get("reference_holder_ref") or "").strip(),
    )


def _latest_heavy_object_reclaimed_anchor(
    *,
    state_root: Path,
    object_id: str,
    object_ref: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    latest: dict[str, Any] = {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "heavy_object_reclaimed":
            continue
        payload = dict(event.get("payload") or {})
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            continue
        latest = dict(event or {})
    return latest


def _latest_heavy_object_supersession_anchor(
    *,
    state_root: Path,
    superseded_object_id: str,
    superseded_object_ref: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_object_id = str(superseded_object_id or "").strip()
    normalized_object_ref = str(superseded_object_ref or "").strip()
    latest: dict[str, Any] = {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "heavy_object_superseded":
            continue
        payload = {
            "object_id": str(dict(event.get("payload") or {}).get("superseded_object_id") or "").strip(),
            "object_ref": str(dict(event.get("payload") or {}).get("superseded_object_ref") or "").strip(),
        }
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            continue
        latest = dict(event or {})
    return latest


def _latest_heavy_object_registration_for_ref(
    *,
    state_root: Path,
    object_ref: str,
) -> dict[str, Any]:
    return _latest_heavy_object_registration_anchor(
        state_root=state_root,
        object_id="",
        object_ref=object_ref,
    )


def _iter_heavy_object_reference_replacements(
    *,
    state_root: Path,
) -> list[dict[str, Any]]:
    from loop_product.event_journal import iter_committed_events

    latest_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    replacements: list[dict[str, Any]] = []
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "").strip() != "heavy_object_reference_attached":
            continue
        payload = dict(event.get("payload") or {})
        current_key = _heavy_object_reference_holder_key(payload)
        if not any(current_key):
            continue
        current_identity = _heavy_object_identity_tuple(payload)
        if not current_identity[0] or not current_identity[1]:
            continue
        previous_event = latest_by_key.get(current_key)
        if previous_event:
            previous_payload = dict(previous_event.get("payload") or {})
            previous_identity = _heavy_object_identity_tuple(previous_payload)
            if (
                previous_identity[0]
                and previous_identity[1]
                and previous_identity[:2] != current_identity[:2]
            ):
                replacements.append(
                    {
                        "reference_event": dict(event or {}),
                        "reference_payload": payload,
                        "superseded_reference_event": dict(previous_event or {}),
                        "superseded_reference_payload": previous_payload,
                        "superseded_object_id": previous_identity[0],
                        "superseded_object_ref": previous_identity[1],
                        "superseded_object_kind": previous_identity[2],
                        "replacement_object_id": current_identity[0],
                        "replacement_object_ref": current_identity[1],
                        "replacement_object_kind": current_identity[2],
                        "reference_ref": current_key[0],
                        "reference_holder_kind": current_key[1],
                        "reference_holder_ref": current_key[2],
                    }
                )
        latest_by_key[current_key] = dict(event or {})
    return replacements


def _latest_heavy_object_reference_fact_for_holder(
    *,
    state_root: Path,
    reference_ref: str,
    reference_holder_kind: str,
    reference_holder_ref: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_holder_key = (
        str(reference_ref or "").strip(),
        str(reference_holder_kind or "").strip(),
        str(reference_holder_ref or "").strip(),
    )
    latest: dict[str, Any] = {}
    for event in iter_committed_events(state_root):
        event_type = str(event.get("event_type") or "").strip()
        if event_type not in {"heavy_object_reference_attached", "heavy_object_reference_released"}:
            continue
        payload = dict(event.get("payload") or {})
        if _heavy_object_reference_holder_key(payload) != normalized_holder_key:
            continue
        latest = dict(event or {})
    return latest


def _latest_heavy_object_reference_release_anchor(
    *,
    state_root: Path,
    object_id: str,
    object_ref: str,
    reference_ref: str,
    reference_holder_kind: str,
    reference_holder_ref: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    normalized_reference_ref = str(reference_ref or "").strip()
    normalized_reference_holder_kind = str(reference_holder_kind or "").strip()
    normalized_reference_holder_ref = str(reference_holder_ref or "").strip()
    latest: dict[str, Any] = {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "heavy_object_reference_released":
            continue
        payload = dict(event.get("payload") or {})
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
            reference_ref_fallback="reference_ref",
        ):
            continue
        if _heavy_object_reference_holder_key(payload) != (
            normalized_reference_ref,
            normalized_reference_holder_kind,
            normalized_reference_holder_ref,
        ):
            continue
        latest = dict(event or {})
    return latest


def _latest_heavy_object_registration_anchor(
    *,
    state_root: Path,
    object_id: str,
    object_ref: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    latest: dict[str, Any] = {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "heavy_object_registered":
            continue
        payload = dict(event.get("payload") or {})
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            continue
        latest = dict(event or {})
    return latest


def _latest_heavy_object_gc_eligibility_anchor(
    *,
    state_root: Path,
    object_id: str,
    object_ref: str,
) -> dict[str, Any]:
    from loop_product.event_journal import iter_committed_events

    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    latest: dict[str, Any] = {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "heavy_object_gc_eligible":
            continue
        payload = dict(event.get("payload") or {})
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            continue
        latest = dict(event or {})
    return latest


def _reclaim_heavy_object_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
        return
    if path.is_dir():
        shutil.rmtree(path)


def execute_accepted_heavy_object_authority_gap_repo_remediation(
    state_root: str | Path,
    *,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    envelope_payload = dict(envelope or {})
    envelope_id = str(envelope_payload.get("envelope_id") or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object repo remediation envelope must have an envelope_id")
    receipt_ref = heavy_object_authority_gap_repo_remediation_receipt_ref(
        state_root=resolved_state_root,
        envelope_id=envelope_id,
    )
    from loop_product.event_journal import commit_heavy_object_authority_gap_repo_remediation_settled_event
    from loop_product.protocols.control_envelope import ControlEnvelope

    if receipt_ref.exists():
        receipt = json.loads(receipt_ref.read_text(encoding="utf-8"))
        commit_heavy_object_authority_gap_repo_remediation_settled_event(
            resolved_state_root,
            envelope=ControlEnvelope.from_dict(envelope_payload),
            receipt=receipt,
        )
        return receipt

    anchor_repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if anchor_repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")
    _ensure_anchor_kernel_state(state_root=resolved_state_root)

    payload = dict(envelope_payload.get("payload") or {})
    requested_repo_root = (
        Path(str(payload.get("repo_root") or "")).expanduser().resolve()
        if str(payload.get("repo_root") or "").strip()
        else anchor_repo_root
    )
    object_kind = str(payload.get("object_kind") or "").strip()
    runtime_name = str(payload.get("runtime_name") or "")
    remediation_kind = str(payload.get("remediation_kind") or "repo_root_authority_gap_remediation").strip()
    remediation_signature = str(payload.get("remediation_signature") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    requirement_event_id = str(payload.get("requirement_event_id") or "").strip()
    audit_result_event_id = str(payload.get("audit_result_event_id") or "").strip()

    failure_reasons: list[str] = []
    if requested_repo_root != anchor_repo_root:
        failure_reasons.append("repo_root_mismatch_with_anchor_runtime")
    if not object_kind:
        failure_reasons.append("missing_object_kind")
    if not requested_repo_root.exists():
        failure_reasons.append("repo_root_missing")

    requested_candidates: list[dict[str, Any]] = []
    skipped_candidates: list[dict[str, Any]] = []
    candidate_count = 0
    final_inventory: dict[str, Any] = {}
    if not failure_reasons:
        from loop_product.kernel.query import (
            query_heavy_object_authority_gap_inventory_view,
            query_heavy_object_authority_gap_remediation_inventory_view,
        )
        from loop_product.kernel.submit import submit_heavy_object_authority_gap_remediation_request

        initial_inventory = query_heavy_object_authority_gap_inventory_view(
            resolved_state_root,
            runtime_name=runtime_name,
            object_kind=object_kind,
            repo_root=requested_repo_root,
        )
        initial_candidates = [dict(item) for item in list(initial_inventory.get("unmanaged_candidates") or [])]
        candidate_count = len(initial_candidates)
        for candidate in initial_candidates:
            object_ref = str(candidate.get("object_ref") or "").strip()
            current_inventory = query_heavy_object_authority_gap_remediation_inventory_view(
                resolved_state_root,
                runtime_name=runtime_name,
                object_kind=object_kind,
                repo_root=requested_repo_root,
            )
            current_candidate = next(
                (
                    dict(item)
                    for item in list(current_inventory.get("candidates") or [])
                    if str(item.get("object_ref") or "").strip() == object_ref
                ),
                {},
            )
            coverage_state = str(current_candidate.get("coverage_state") or "")
            remediation_state = str(current_candidate.get("remediation_state") or "")
            if coverage_state == "UNMANAGED" and remediation_state == "UNREQUESTED":
                accepted = submit_heavy_object_authority_gap_remediation_request(
                    resolved_state_root,
                    object_ref=object_ref,
                    object_kind=object_kind,
                    reason=f"{str(envelope_payload.get('note') or '') or 'repo-root heavy-object authority-gap remediation'} [{Path(object_ref).name}]",
                    source=str(envelope_payload.get("source") or "root-kernel"),
                    round_id=str(envelope_payload.get("round_id") or "R0"),
                    generation=int(envelope_payload.get("generation") or 0),
                    runtime_name=runtime_name,
                    repo_root=requested_repo_root,
                )
                requested_candidates.append(
                    {
                        "object_ref": object_ref,
                        "file_name": str(current_candidate.get("file_name") or Path(object_ref).name),
                        "byte_size": int(current_candidate.get("byte_size") or 0),
                        "remediation_envelope_id": str(accepted.envelope_id or ""),
                    }
                )
                continue
            if coverage_state == "MANAGED":
                skip_reason = "already_managed_after_prior_fanout_request"
            elif remediation_state == "REQUESTED":
                skip_reason = "already_requested_before_fanout_step"
            else:
                skip_reason = "candidate_no_longer_eligible_for_repo_fanout"
            skipped_candidates.append(
                {
                    "object_ref": object_ref,
                    "file_name": str(current_candidate.get("file_name") or candidate.get("file_name") or Path(object_ref).name),
                    "byte_size": int(current_candidate.get("byte_size") or candidate.get("byte_size") or 0),
                    "skip_reason": skip_reason,
                    "coverage_state": coverage_state,
                    "remediation_state": remediation_state,
                }
            )

        final_inventory = query_heavy_object_authority_gap_remediation_inventory_view(
            resolved_state_root,
            runtime_name=runtime_name,
            object_kind=object_kind,
            repo_root=requested_repo_root,
        )

    fully_managed_after = bool(
        not failure_reasons
        and int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0) == 0
        and int(final_inventory.get("pending_remediation_candidate_count") or 0) == 0
    )
    receipt = {
        "schema": "loop_product.heavy_object_authority_gap_repo_remediation_receipt",
        "schema_version": "0.1.0",
        "envelope_id": envelope_id,
        "accepted_at": str(envelope_payload.get("accepted_at") or ""),
        "command_note": str(envelope_payload.get("note") or ""),
        "repo_root": str(requested_repo_root),
        "object_kind": object_kind,
        "runtime_name": runtime_name,
        "remediation_kind": remediation_kind,
        "remediation_signature": remediation_signature,
        "trigger_kind": trigger_kind,
        "trigger_ref": trigger_ref,
        "requirement_event_id": requirement_event_id,
        "audit_result_event_id": audit_result_event_id,
        "candidate_count": int(candidate_count),
        "requested_candidate_count": len(requested_candidates),
        "skipped_candidate_count": len(skipped_candidates),
        "requested_candidates": requested_candidates,
        "skipped_candidates": skipped_candidates,
        "fully_managed_after": fully_managed_after,
        "unrequested_unmanaged_candidate_count_after": int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0),
        "pending_remediation_candidate_count_after": int(final_inventory.get("pending_remediation_candidate_count") or 0),
        "remediated_candidate_count_after": int(final_inventory.get("remediated_candidate_count") or 0),
        "failure_reasons": failure_reasons,
        "receipt_ref": str(receipt_ref),
        "remediated_at": _now_iso(),
    }
    receipt_ref.parent.mkdir(parents=True, exist_ok=True)
    write_json_read_only(receipt_ref, receipt)
    commit_heavy_object_authority_gap_repo_remediation_settled_event(
        resolved_state_root,
        envelope=ControlEnvelope.from_dict(envelope_payload),
        receipt=receipt,
    )
    return receipt


def execute_accepted_heavy_object_authority_gap_audit(
    state_root: str | Path,
    *,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    envelope_payload = dict(envelope or {})
    envelope_id = str(envelope_payload.get("envelope_id") or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object authority-gap audit envelope must have an envelope_id")
    receipt_ref = heavy_object_authority_gap_audit_receipt_ref(
        state_root=resolved_state_root,
        envelope_id=envelope_id,
    )
    from loop_product.event_journal import commit_heavy_object_authority_gap_audited_event
    from loop_product.protocols.control_envelope import ControlEnvelope

    if receipt_ref.exists():
        receipt = json.loads(receipt_ref.read_text(encoding="utf-8"))
        commit_heavy_object_authority_gap_audited_event(
            resolved_state_root,
            envelope=ControlEnvelope.from_dict(envelope_payload),
            receipt=receipt,
        )
        return receipt

    anchor_repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if anchor_repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")

    payload = dict(envelope_payload.get("payload") or {})
    requested_repo_root = (
        Path(str(payload.get("repo_root") or "")).expanduser().resolve()
        if str(payload.get("repo_root") or "").strip()
        else anchor_repo_root
    )
    object_kind = str(payload.get("object_kind") or "").strip()
    runtime_name = str(payload.get("runtime_name") or "")
    audit_kind = str(payload.get("audit_kind") or "repo_root_authority_gap_audit").strip()
    audit_signature = str(payload.get("audit_signature") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    requirement_event_id = str(payload.get("requirement_event_id") or "").strip()

    failure_reasons: list[str] = []
    if requested_repo_root != anchor_repo_root:
        failure_reasons.append("repo_root_mismatch_with_anchor_runtime")
    if not object_kind:
        failure_reasons.append("missing_object_kind")
    if not requested_repo_root.exists():
        failure_reasons.append("repo_root_missing")

    inventory: dict[str, Any] = {}
    if not failure_reasons:
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view

        inventory = query_heavy_object_authority_gap_inventory_view(
            resolved_state_root,
            runtime_name=runtime_name,
            object_kind=object_kind,
            repo_root=requested_repo_root,
        )

    receipt = {
        "schema": "loop_product.heavy_object_authority_gap_audit_receipt",
        "schema_version": "0.1.0",
        "envelope_id": envelope_id,
        "accepted_at": str(envelope_payload.get("accepted_at") or ""),
        "command_note": str(envelope_payload.get("note") or ""),
        "repo_root": str(requested_repo_root),
        "object_kind": object_kind,
        "runtime_name": runtime_name,
        "audit_kind": audit_kind,
        "audit_signature": audit_signature,
        "trigger_kind": trigger_kind,
        "trigger_ref": trigger_ref,
        "requirement_event_id": requirement_event_id,
        "candidate_count": int(inventory.get("filesystem_candidate_count") or 0),
        "registered_candidate_count": int(inventory.get("registered_candidate_count") or 0),
        "discovery_covered_candidate_count": int(inventory.get("discovery_covered_candidate_count") or 0),
        "managed_candidate_count": int(inventory.get("managed_candidate_count") or 0),
        "unmanaged_candidate_count": int(inventory.get("unmanaged_candidate_count") or 0),
        "strongest_duplicate_count": int(inventory.get("strongest_duplicate_count") or 0),
        "managed_candidates": list(inventory.get("managed_candidates") or []),
        "unmanaged_candidates": list(inventory.get("unmanaged_candidates") or []),
        "failure_reasons": failure_reasons,
        "receipt_ref": str(receipt_ref),
        "audited_at": _now_iso(),
    }
    receipt_ref.parent.mkdir(parents=True, exist_ok=True)
    write_json_read_only(receipt_ref, receipt)
    commit_heavy_object_authority_gap_audited_event(
        resolved_state_root,
        envelope=ControlEnvelope.from_dict(envelope_payload),
        receipt=receipt,
    )
    return receipt


def execute_accepted_heavy_object_reclamation(
    state_root: str | Path,
    *,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    envelope_payload = dict(envelope or {})
    envelope_id = str(envelope_payload.get("envelope_id") or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object reclamation envelope must have an envelope_id")
    receipt_ref = heavy_object_reclamation_receipt_ref(state_root=resolved_state_root, envelope_id=envelope_id)
    if receipt_ref.exists():
        return json.loads(receipt_ref.read_text(encoding="utf-8"))

    repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")

    payload = dict(envelope_payload.get("payload") or {})
    object_id = str(payload.get("object_id") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    object_ref = Path(str(payload.get("object_ref") or "")).expanduser().resolve()
    reclamation_kind = str(payload.get("reclamation_kind") or "").strip()
    owner_node_id = str(payload.get("owner_node_id") or envelope_payload.get("source") or "").strip()
    runtime_name = str(payload.get("runtime_name") or "")
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    registration_event = _latest_heavy_object_registration_anchor(
        state_root=resolved_state_root,
        object_id=object_id,
        object_ref=str(object_ref),
    )
    gc_eligibility_event = _latest_heavy_object_gc_eligibility_anchor(
        state_root=resolved_state_root,
        object_id=object_id,
        object_ref=str(object_ref),
    )

    failure_reasons: list[str] = []
    object_existed_before = object_ref.exists()
    byte_size_before = int(object_ref.stat().st_size) if object_ref.exists() and object_ref.is_file() else 0
    latest_registration_for_ref = _latest_heavy_object_registration_for_ref(
        state_root=resolved_state_root,
        object_ref=str(object_ref),
    )
    if not _path_within(repo_root, object_ref):
        failure_reasons.append("object_ref_outside_repo_root")
    if not registration_event:
        failure_reasons.append("missing_matching_heavy_object_registration")
    if not gc_eligibility_event:
        failure_reasons.append("missing_supporting_heavy_object_gc_eligibility")
    if latest_registration_for_ref and str(dict(latest_registration_for_ref.get("payload") or {}).get("object_id") or "").strip() not in {"", object_id}:
        failure_reasons.append("object_ref_reused_by_newer_registration")
    if not object_existed_before:
        failure_reasons.append("object_missing_before_reclaim")

    reclaimed = False
    if not failure_reasons:
        _reclaim_heavy_object_path(object_ref)
        reclaimed = not object_ref.exists()
        if not reclaimed:
            failure_reasons.append("object_still_exists_after_reclaim")

    receipt = {
        "schema": "loop_product.heavy_object_reclamation_receipt",
        "schema_version": "0.1.0",
        "envelope_id": envelope_id,
        "accepted_at": str(envelope_payload.get("accepted_at") or ""),
        "command_note": str(envelope_payload.get("note") or ""),
        "repo_root": str(repo_root),
        "object_id": object_id,
        "object_kind": object_kind,
        "object_ref": str(object_ref),
        "reclamation_kind": reclamation_kind,
        "owner_node_id": owner_node_id,
        "runtime_name": runtime_name,
        "trigger_kind": trigger_kind,
        "trigger_ref": trigger_ref,
        "reclaimed": reclaimed,
        "object_existed_before": object_existed_before,
        "object_exists_after": object_ref.exists(),
        "byte_size_before": byte_size_before,
        "registration_event_id": str(registration_event.get("event_id") or ""),
        "registration_seq": int(registration_event.get("seq") or 0),
        "latest_registration_for_ref_event_id": str(latest_registration_for_ref.get("event_id") or ""),
        "gc_eligibility_event_id": str(gc_eligibility_event.get("event_id") or ""),
        "gc_eligibility_seq": int(gc_eligibility_event.get("seq") or 0),
        "failure_reasons": failure_reasons,
        "receipt_ref": str(receipt_ref),
        "reclaimed_at": _now_iso() if reclaimed else "",
    }
    receipt_ref.parent.mkdir(parents=True, exist_ok=True)
    write_json_read_only(receipt_ref, receipt)

    if reclaimed:
        from loop_product.event_journal import commit_heavy_object_reclaimed_event
        from loop_product.protocols.control_envelope import ControlEnvelope

        commit_heavy_object_reclaimed_event(
            resolved_state_root,
            envelope=ControlEnvelope.from_dict(envelope_payload),
            receipt=receipt,
        )
    return receipt


def execute_accepted_heavy_object_discovery(
    state_root: str | Path,
    *,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    from loop_product.artifact_hygiene import (
        heavy_object_identity,
        materialize_canonical_heavy_object_store_ref,
    )

    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    envelope_payload = dict(envelope or {})
    envelope_id = str(envelope_payload.get("envelope_id") or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object discovery envelope must have an envelope_id")
    receipt_ref = heavy_object_discovery_receipt_ref(state_root=resolved_state_root, envelope_id=envelope_id)

    repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")

    payload = dict(envelope_payload.get("payload") or {})
    object_id = str(payload.get("object_id") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    object_ref = Path(str(payload.get("object_ref") or "")).expanduser().resolve()
    discovery_root = Path(str(payload.get("discovery_root") or "")).expanduser().resolve()
    discovery_kind = str(payload.get("discovery_kind") or "").strip()
    owner_node_id = str(payload.get("owner_node_id") or envelope_payload.get("source") or "").strip()
    runtime_name = str(payload.get("runtime_name") or "")

    failure_reasons: list[str] = []
    if not object_id:
        failure_reasons.append("missing_object_id")
    if not str(object_ref):
        failure_reasons.append("missing_object_ref")
    if not str(discovery_root):
        failure_reasons.append("missing_discovery_root")
    if not discovery_kind:
        failure_reasons.append("missing_discovery_kind")
    if not _path_within(repo_root, object_ref):
        failure_reasons.append("object_ref_outside_repo_root")
    if not _path_within(repo_root, discovery_root):
        failure_reasons.append("discovery_root_outside_repo_root")
    if not discovery_root.exists():
        failure_reasons.append("discovery_root_missing")

    discovered_refs: list[str] = []
    total_bytes = 0
    object_name = object_ref.name
    target_is_dir = str(object_kind or "").strip() == "publish_tree" or object_ref.is_dir()
    if receipt_ref.exists():
        receipt = json.loads(receipt_ref.read_text(encoding="utf-8"))
    else:
        discovery_candidates: list[Path] = []
        if not failure_reasons:
            if target_is_dir and object_kind == "publish_tree":
                from loop_product.artifact_hygiene import iter_publish_tree_candidate_roots

                discovery_candidates = iter_publish_tree_candidate_roots(repo_root)
            elif target_is_dir and object_kind == "runtime_heavy_tree":
                from loop_product.artifact_hygiene import iter_runtime_heavy_tree_candidate_roots

                discovery_candidates = iter_runtime_heavy_tree_candidate_roots(repo_root)
            elif object_name:
                discovery_candidates = sorted(discovery_root.rglob(object_name))
        if not failure_reasons:
            for candidate in discovery_candidates:
                try:
                    resolved_candidate = candidate.expanduser().resolve()
                except OSError:
                    continue
                if target_is_dir:
                    if not resolved_candidate.is_dir():
                        continue
                elif not resolved_candidate.is_file():
                    continue
                if not _path_within(repo_root, resolved_candidate):
                    continue
                try:
                    candidate_identity = heavy_object_identity(
                        resolved_candidate,
                        object_kind=object_kind,
                    )
                except Exception:
                    continue
                if object_id and str(candidate_identity.get("object_id") or "").strip() != object_id:
                    continue
                discovered_refs.append(str(resolved_candidate))
                total_bytes += int(candidate_identity.get("byte_size") or 0)

        receipt = {
            "schema": "loop_product.heavy_object_discovery_receipt",
            "schema_version": "0.1.0",
            "envelope_id": envelope_id,
            "accepted_at": str(envelope_payload.get("accepted_at") or ""),
            "command_note": str(envelope_payload.get("note") or ""),
            "repo_root": str(repo_root),
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": str(object_ref),
            "discovery_root": str(discovery_root),
            "discovery_kind": discovery_kind,
            "owner_node_id": owner_node_id,
            "runtime_name": runtime_name,
            "discovered_refs": discovered_refs,
            "duplicate_count": len(discovered_refs),
            "total_bytes": total_bytes,
            "failure_reasons": failure_reasons,
            "receipt_ref": str(receipt_ref),
            "discovered_at": _now_iso(),
        }
        receipt_ref.parent.mkdir(parents=True, exist_ok=True)
        write_json_read_only(receipt_ref, receipt)

    from loop_product.event_journal import (
        commit_heavy_object_authority_gap_remediation_settled_event,
        commit_heavy_object_discovery_observed_event,
    )
    from loop_product.artifact_hygiene import (
        classify_mathlib_pack_reference_holder,
        classify_publish_tree_reference_holder,
        classify_runtime_heavy_tree_reference_holder,
    )
    from loop_product.kernel.submit import (
        submit_heavy_object_reference_request,
        submit_heavy_object_registration_request,
    )
    from loop_product.protocols.control_envelope import ControlEnvelope

    normalized_envelope = ControlEnvelope.from_dict(envelope_payload)
    commit_heavy_object_discovery_observed_event(
        resolved_state_root,
        envelope=normalized_envelope,
        receipt=receipt,
    )
    commit_heavy_object_authority_gap_remediation_settled_event(
        resolved_state_root,
        envelope=normalized_envelope,
        receipt=receipt,
    )
    if discovery_kind == "authority_gap_remediation" and not failure_reasons and discovered_refs:
        existing_registration = _latest_heavy_object_registration_anchor(
            state_root=resolved_state_root,
            object_id=object_id,
            object_ref=str(object_ref),
        )
        authoritative_object_ref = str(dict(existing_registration.get("payload") or {}).get("object_ref") or "").strip()
        if not existing_registration:
            registration_source_ref = object_ref if object_ref.exists() else Path(discovered_refs[0])
            canonical_object_ref = materialize_canonical_heavy_object_store_ref(
                repo_root=repo_root,
                object_kind=object_kind,
                object_id=object_id,
                source_ref=registration_source_ref,
            )
            try:
                registration_identity = heavy_object_identity(
                    canonical_object_ref,
                    object_kind=object_kind,
                )
                byte_size = int(registration_identity.get("byte_size") or 0)
            except Exception:
                byte_size = 0
            submit_heavy_object_registration_request(
                resolved_state_root,
                object_id=object_id,
                object_kind=object_kind,
                object_ref=canonical_object_ref,
                byte_size=byte_size,
                reason=f"{str(envelope_payload.get('note') or '') or 'heavy object authority-gap remediation'} [auto-registration]",
                source=str(envelope_payload.get("source") or owner_node_id or "root-kernel"),
                round_id=str(envelope_payload.get("round_id") or "R0"),
                generation=int(envelope_payload.get("generation") or 0),
                owner_node_id=owner_node_id,
                runtime_name=runtime_name,
                repo_root=repo_root,
                registration_kind="authority_gap_auto_registration",
                trigger_kind="heavy_object_authority_gap_remediation",
                trigger_ref=envelope_id,
            )
            authoritative_object_ref = str(canonical_object_ref)
        if not authoritative_object_ref:
            refreshed_registration = _latest_heavy_object_registration_anchor(
                state_root=resolved_state_root,
                object_id=object_id,
                object_ref="",
            )
            authoritative_object_ref = str(
                dict(refreshed_registration.get("payload") or {}).get("object_ref") or authoritative_object_ref or ""
            ).strip()
        if not authoritative_object_ref:
            authoritative_object_ref = str(object_ref)
        if object_kind == "mathlib_pack":
            for discovered_ref in [str(item) for item in list(receipt.get("discovered_refs") or []) if str(item).strip()]:
                holder = classify_mathlib_pack_reference_holder(Path(discovered_ref), repo_root=repo_root)
                if not holder:
                    continue
                submit_heavy_object_reference_request(
                    resolved_state_root,
                    object_id=object_id,
                    object_kind=object_kind,
                    object_ref=authoritative_object_ref,
                    reference_ref=str(holder.get("reference_ref") or discovered_ref),
                    reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
                    reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
                    reference_kind="authority_gap_auto_reference",
                    reason=f"{str(envelope_payload.get('note') or '') or 'heavy object authority-gap remediation'} [auto-reference]",
                    source=str(envelope_payload.get("source") or owner_node_id or "root-kernel"),
                    round_id=str(envelope_payload.get("round_id") or "R0"),
                    generation=int(envelope_payload.get("generation") or 0),
                    owner_node_id=owner_node_id,
                    runtime_name=runtime_name,
                    repo_root=repo_root,
                    trigger_kind="heavy_object_authority_gap_remediation",
                    trigger_ref=envelope_id,
                )
        elif object_kind == "publish_tree":
            for discovered_ref in [str(item) for item in list(receipt.get("discovered_refs") or []) if str(item).strip()]:
                holder = classify_publish_tree_reference_holder(Path(discovered_ref), repo_root=repo_root)
                if not holder:
                    continue
                submit_heavy_object_reference_request(
                    resolved_state_root,
                    object_id=object_id,
                    object_kind=object_kind,
                    object_ref=authoritative_object_ref,
                    reference_ref=str(holder.get("reference_ref") or discovered_ref),
                    reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
                    reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
                    reference_kind="authority_gap_auto_reference",
                    reason=f"{str(envelope_payload.get('note') or '') or 'heavy object authority-gap remediation'} [auto-reference]",
                    source=str(envelope_payload.get("source") or owner_node_id or "root-kernel"),
                    round_id=str(envelope_payload.get("round_id") or "R0"),
                    generation=int(envelope_payload.get("generation") or 0),
                    owner_node_id=owner_node_id,
                    runtime_name=runtime_name,
                    repo_root=repo_root,
                    trigger_kind="heavy_object_authority_gap_remediation",
                    trigger_ref=envelope_id,
                )
        elif object_kind == "runtime_heavy_tree":
            for discovered_ref in [str(item) for item in list(receipt.get("discovered_refs") or []) if str(item).strip()]:
                holder = classify_runtime_heavy_tree_reference_holder(Path(discovered_ref), repo_root=repo_root)
                if not holder:
                    continue
                submit_heavy_object_reference_request(
                    resolved_state_root,
                    object_id=object_id,
                    object_kind=object_kind,
                    object_ref=authoritative_object_ref,
                    reference_ref=str(holder.get("reference_ref") or discovered_ref),
                    reference_holder_kind=str(holder.get("reference_holder_kind") or ""),
                    reference_holder_ref=str(holder.get("reference_holder_ref") or ""),
                    reference_kind="authority_gap_auto_reference",
                    reason=f"{str(envelope_payload.get('note') or '') or 'heavy object authority-gap remediation'} [auto-reference]",
                    source=str(envelope_payload.get("source") or owner_node_id or "root-kernel"),
                    round_id=str(envelope_payload.get("round_id") or "R0"),
                    generation=int(envelope_payload.get("generation") or 0),
                    owner_node_id=owner_node_id,
                    runtime_name=runtime_name,
                    repo_root=repo_root,
                    trigger_kind="heavy_object_authority_gap_remediation",
                    trigger_ref=envelope_id,
                )
    return receipt


def execute_accepted_repo_tree_cleanup(
    state_root: str | Path,
    *,
    envelope: dict[str, Any],
    settle_timeout_s: float = 6.0,
    poll_interval_s: float = 0.05,
) -> dict[str, Any]:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    envelope_payload = dict(envelope or {})
    envelope_id = str(envelope_payload.get("envelope_id") or "").strip()
    if not envelope_id:
        raise ValueError("accepted repo-tree cleanup envelope must have an envelope_id")
    receipt_ref = repo_tree_cleanup_receipt_ref(state_root=resolved_state_root, envelope_id=envelope_id)
    if receipt_ref.exists():
        return json.loads(receipt_ref.read_text(encoding="utf-8"))
    payload = dict(envelope_payload.get("payload") or {})
    anchor_repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if anchor_repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")
    receipt_payload = cleanup_repo_tree_services(
        tree_root=Path(str(payload.get("tree_root") or "")).expanduser().resolve(),
        repo_roots=[Path(str(item)).expanduser().resolve() for item in list(payload.get("repo_roots") or [])],
        anchor_repo_root=anchor_repo_root,
        include_anchor_repo_root=bool(payload.get("include_anchor_repo_root")),
        settle_timeout_s=settle_timeout_s,
        poll_interval_s=poll_interval_s,
        cleanup_kind=str(payload.get("cleanup_kind") or "batch_repo_tree_cleanup"),
        producer="runtime.control_plane.execute_accepted_repo_tree_cleanup",
    )
    receipt = {
        "schema": "loop_product.repo_tree_cleanup_receipt",
        "schema_version": "0.1.0",
        "envelope_id": envelope_id,
        "anchor_repo_root": str(anchor_repo_root),
        "accepted_at": str(envelope_payload.get("accepted_at") or ""),
        "command_note": str(envelope_payload.get("note") or ""),
        "tree_root": str(receipt_payload.get("tree_root") or ""),
        "include_anchor_repo_root": bool(receipt_payload.get("include_anchor_repo_root")),
        "repo_roots": [str(item) for item in list(receipt_payload.get("repo_roots") or [])],
        "repo_receipts": list(receipt_payload.get("repo_receipts") or []),
        "repo_count": int(receipt_payload.get("repo_count") or 0),
        "quiesced_repo_count": int(receipt_payload.get("quiesced_repo_count") or 0),
        "failed_repo_count": int(receipt_payload.get("failed_repo_count") or 0),
        "quiesced": bool(receipt_payload.get("quiesced")),
    }
    receipt_ref.parent.mkdir(parents=True, exist_ok=True)
    write_json_read_only(receipt_ref, receipt)
    return receipt


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trusted repo control-plane service loop")
    parser.add_argument("--repo-root", required=True, help="Path to loop_product_repo")
    parser.add_argument(
        "--run-repo-reactor-residency-guard",
        action="store_true",
        help="Run the top-level repo-reactor residency-guard loop",
    )
    parser.add_argument(
        "--run-repo-reactor",
        action="store_true",
        help="Run the top-level repo reactor loop instead of the repo control-plane loop",
    )
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=_DEFAULT_REPO_CONTROL_PLANE_POLL_INTERVAL_S,
        help="Controller polling interval when running repo reactor/control-plane loops",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if bool(args.run_repo_reactor_residency_guard):
        return run_repo_reactor_residency_guard_loop(
            repo_root=args.repo_root,
            poll_interval_s=float(args.poll_interval_s),
        )
    if bool(args.run_repo_reactor):
        return run_repo_reactor_loop(
            repo_root=args.repo_root,
            poll_interval_s=float(args.poll_interval_s),
        )
    return run_repo_control_plane_loop(
        repo_root=args.repo_root,
        poll_interval_s=float(args.poll_interval_s),
    )


if __name__ == "__main__":
    raise SystemExit(main())
