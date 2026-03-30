"""Shared committed cleanup-authority helpers for runtime/service retirement."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from loop_product.event_journal import (
    commit_runtime_root_quiesced_event,
    commit_test_runtime_cleanup_committed_event,
    iter_committed_events,
)
from loop_product.runtime_identity import runtime_root_identity_token
from loop_product.runtime_paths import require_runtime_root


_CLEANUP_RETIRE_EVENT_TYPES = {
    "test_runtime_cleanup_committed",
    "runtime_root_quiesced",
    "reap_requested",
    "process_reap_confirmed",
}


def _resolved_runtime_root(state_root: str | Path) -> Path:
    return require_runtime_root(Path(state_root).expanduser().resolve())


def repo_root_from_runtime_root(*, state_root: str | Path) -> Path | None:
    resolved_state_root = _resolved_runtime_root(state_root)
    parts = resolved_state_root.parts
    for idx, part in enumerate(parts):
        if part != ".loop":
            continue
        return (Path(*parts[:idx]) if idx > 0 else Path(resolved_state_root.anchor)).expanduser().resolve()
    return None


def runtime_cleanup_descriptor(
    *, state_root: str | Path, repo_root: str | Path | None = None, ensure_identity: bool = False
) -> dict[str, Any] | None:
    resolved_state_root = _resolved_runtime_root(state_root)
    resolved_repo_root = (
        Path(repo_root).expanduser().resolve()
        if repo_root not in (None, "")
        else repo_root_from_runtime_root(state_root=resolved_state_root)
    )
    if resolved_repo_root is None:
        return None
    runtime_name = resolved_state_root.name
    workspace_root = (
        (resolved_repo_root / "workspace").resolve()
        if runtime_name == ".loop"
        else (resolved_repo_root / "workspace" / runtime_name).resolve()
    )
    housekeeping_root = (resolved_repo_root / ".loop" / "housekeeping").resolve()
    identity_token = runtime_root_identity_token(resolved_state_root, ensure=ensure_identity)
    return {
        "runtime_name": runtime_name,
        "repo_root": str(resolved_repo_root),
        "state_root": str(resolved_state_root),
        "workspace_root": str(workspace_root),
        "housekeeping_root": str(housekeeping_root),
        "runtime_root_identity_token": identity_token,
    }


def runtime_cleanup_retirement_authority(
    *,
    state_root: str | Path,
    repo_root: str | Path | None = None,
    expected_runtime_identity_token: str = "",
) -> dict[str, Any] | None:
    descriptor = runtime_cleanup_descriptor(
        state_root=state_root,
        repo_root=repo_root,
        ensure_identity=False,
    )
    if descriptor is None:
        return None
    housekeeping_root = Path(str(descriptor["housekeeping_root"])).resolve()
    if not housekeeping_root.exists():
        return None
    state_root_value = str(descriptor["state_root"])
    workspace_root_value = str(descriptor["workspace_root"])
    runtime_name = str(descriptor["runtime_name"])
    identity_token = str(expected_runtime_identity_token or descriptor.get("runtime_root_identity_token") or "")
    matching: dict[str, Any] | None = None
    for event in iter_committed_events(housekeeping_root):
        event_type = str(event.get("event_type") or "").strip()
        if event_type not in _CLEANUP_RETIRE_EVENT_TYPES:
            continue
        payload = dict(event.get("payload") or {})
        if (
            str(payload.get("runtime_name") or "").strip() != runtime_name
            and str(payload.get("state_root") or "").strip() != state_root_value
            and str(payload.get("workspace_root") or "").strip() != workspace_root_value
        ):
            continue
        payload_identity = str(payload.get("runtime_root_identity_token") or "").strip()
        if payload_identity:
            if not identity_token:
                continue
            if payload_identity != identity_token:
                continue
        matching = {
            "event_type": event_type,
            "event_id": str(event.get("event_id") or ""),
            "seq": int(event.get("seq") or 0),
            "payload": payload,
            "runtime_name": runtime_name,
            "runtime_root_identity_token": identity_token or payload_identity,
            "housekeeping_root": str(housekeeping_root),
            "state_root": state_root_value,
            "workspace_root": workspace_root_value,
        }
    return matching


def _cleanup_observation_id(*, descriptor: dict[str, Any], cleanup_kind: str) -> str:
    payload = {
        "runtime_name": str(descriptor["runtime_name"]),
        "state_root": str(descriptor["state_root"]),
        "workspace_root": str(descriptor["workspace_root"]),
        "runtime_root_identity_token": str(descriptor.get("runtime_root_identity_token") or ""),
        "cleanup_kind": str(cleanup_kind),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"runtime_cleanup::{digest}"


def _runtime_quiesced_observation_id(*, descriptor: dict[str, Any], cleanup_event_id: str) -> str:
    payload = {
        "runtime_name": str(descriptor["runtime_name"]),
        "state_root": str(descriptor["state_root"]),
        "workspace_root": str(descriptor["workspace_root"]),
        "runtime_root_identity_token": str(descriptor.get("runtime_root_identity_token") or ""),
        "cleanup_event_id": str(cleanup_event_id),
    }
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"runtime_quiesced::{digest}"


def commit_test_runtime_cleanup_authority(
    *,
    state_root: str | Path,
    repo_root: str | Path | None = None,
    cleanup_kind: str = "test_runtime_teardown",
    producer: str = "runtime.cleanup_authority.commit_test_runtime_cleanup_authority",
    extra_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    descriptor = runtime_cleanup_descriptor(state_root=state_root, repo_root=repo_root, ensure_identity=True)
    if descriptor is None:
        raise ValueError(f"unable to derive repo root for runtime cleanup authority: {state_root}")
    housekeeping_root = Path(str(descriptor["housekeeping_root"])).resolve()
    payload = {
        "runtime_name": str(descriptor["runtime_name"]),
        "state_root": str(descriptor["state_root"]),
        "workspace_root": str(descriptor["workspace_root"]),
        "runtime_root_identity_token": str(descriptor["runtime_root_identity_token"]),
        "cleanup_kind": str(cleanup_kind),
    }
    if extra_payload:
        payload.update(dict(extra_payload))
    return commit_test_runtime_cleanup_committed_event(
        housekeeping_root,
        observation_id=_cleanup_observation_id(descriptor=descriptor, cleanup_kind=cleanup_kind),
        node_id=str(descriptor["runtime_name"]),
        payload=payload,
        producer=producer,
    )


def commit_runtime_root_quiesced_from_cleanup(
    *,
    state_root: str | Path,
    repo_root: str | Path | None = None,
    cleanup_event_id: str,
    producer: str = "runtime.cleanup_authority.commit_runtime_root_quiesced_from_cleanup",
    extra_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    descriptor = runtime_cleanup_descriptor(state_root=state_root, repo_root=repo_root, ensure_identity=True)
    if descriptor is None:
        raise ValueError(f"unable to derive repo root for runtime quiesced fact: {state_root}")
    housekeeping_root = Path(str(descriptor["housekeeping_root"])).resolve()
    runtime_identity_token = str(descriptor["runtime_root_identity_token"])
    for event in iter_committed_events(housekeeping_root):
        if str(event.get("event_type") or "").strip() != "runtime_root_quiesced":
            continue
        existing_payload = dict(event.get("payload") or {})
        if str(existing_payload.get("cleanup_event_id") or "").strip() != str(cleanup_event_id):
            continue
        if str(existing_payload.get("runtime_root_identity_token") or "").strip() != runtime_identity_token:
            continue
        return dict(event)
    payload = {
        "runtime_name": str(descriptor["runtime_name"]),
        "state_root": str(descriptor["state_root"]),
        "workspace_root": str(descriptor["workspace_root"]),
        "runtime_root_identity_token": runtime_identity_token,
        "cleanup_event_id": str(cleanup_event_id),
    }
    if extra_payload:
        payload.update(dict(extra_payload))
    return commit_runtime_root_quiesced_event(
        housekeeping_root,
        observation_id=_runtime_quiesced_observation_id(descriptor=descriptor, cleanup_event_id=cleanup_event_id),
        node_id=str(descriptor["runtime_name"]),
        payload=payload,
        producer=producer,
    )
