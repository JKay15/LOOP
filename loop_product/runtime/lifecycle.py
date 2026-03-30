"""Lifecycle helpers and public runtime bootstrap surfaces."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from loop_product.dispatch.bootstrap import bootstrap_first_implementer_node as dispatch_bootstrap_first_implementer_node
from loop_product.dispatch.bootstrap import bootstrap_first_implementer_from_endpoint as dispatch_bootstrap_first_implementer_from_endpoint
from loop_product.dispatch.child_progress_snapshot import (
    child_progress_snapshot_from_launch_result_ref as dispatch_child_progress_snapshot_from_launch_result_ref,
)
from loop_product.dispatch.launch_runtime import child_runtime_status_from_launch_result_ref as dispatch_child_runtime_status_from_launch_result_ref
from loop_product.dispatch.launch_runtime import launch_child_from_result_ref as dispatch_launch_child_from_result_ref
from loop_product.dispatch.publication import publish_external_from_implementer_result as dispatch_publish_external_from_implementer_result
from loop_product.dispatch.recovery_bootstrap import recover_orphaned_active_node as dispatch_recover_orphaned_active_node
from loop_product.dispatch.supervision import RuntimeContextMissingError
from loop_product.dispatch.supervision import supervise_child_until_settled as dispatch_supervise_child_until_settled
from loop_product.dispatch.child_dispatch import load_child_runtime_context, materialize_child
from loop_product.evaluator_authority import authoritative_result_retryable_nonterminal
from loop_product.kernel.authority import kernel_internal_authority
from loop_product.kernel.policy import (
    implementer_budget_profile,
    implementer_execution_policy,
    implementer_reasoning_profile,
    kernel_execution_policy,
    kernel_reasoning_profile,
)
from loop_product.kernel.state import (
    KernelState,
    ensure_runtime_tree,
    load_kernel_state,
    persist_node_snapshot,
    persist_kernel_state,
    synchronize_supervision_stop_point_from_launch_result_ref,
    synchronize_authoritative_node_results,
)
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.control_objects import NodeTerminalOutcome, NodeTerminalResult
from loop_product.protocols.node import NodeStatus
from loop_product.protocols.schema import validate_repo_object
from loop_product.event_journal import commit_guarded_runtime_liveness_observation_event
from loop_product.event_journal import commit_lease_expired_event
from loop_product.event_journal import commit_process_identity_confirmed_event
from loop_product.event_journal import commit_process_exit_observed_event
from loop_product.event_journal import commit_runtime_loss_confirmed_event
from loop_product.event_journal import commit_supervision_settled_event
from loop_product.event_journal import iter_committed_events
from loop_product.state_io import is_user_writable, write_json_read_only
from loop_product.runtime.launch_surface import launch_child_from_result_ref as runtime_launch_child_from_result_ref
from loop_product.runtime_paths import require_runtime_root
from loop_product.runtime.liveness import select_effective_runtime_liveness_head, summarize_runtime_liveness_observation


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _runtime_status_process_fingerprint(launch_payload: dict[str, Any]) -> str:
    wrapped_argv = [str(item) for item in list(dict(launch_payload or {}).get("wrapped_argv") or [])]
    if not wrapped_argv:
        return ""
    digest = hashlib.sha256()
    digest.update(json.dumps(wrapped_argv, ensure_ascii=True, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def _runtime_status_mirror_observation_id(
    *,
    launch_result_ref: Path,
    status_payload: dict[str, Any],
    process_fingerprint: str,
    launch_identity: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "status_result_ref": str(status_payload.get("status_result_ref") or ""),
        "node_id": str(status_payload.get("node_id") or ""),
        "runtime_attachment_state": str(status_payload.get("runtime_attachment_state") or ""),
        "runtime_observed_at": str(status_payload.get("runtime_observed_at") or ""),
        "pid": int(status_payload.get("pid") or 0),
        "pid_alive": bool(status_payload.get("pid_alive")),
        "recovery_reason": str(status_payload.get("recovery_reason") or ""),
        "process_fingerprint": process_fingerprint,
        "launch_identity": launch_identity,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"status_liveness::{digest.hexdigest()[:16]}"


def _runtime_loss_confirmed_observation_id(
    *,
    launch_result_ref: Path,
    status_payload: dict[str, Any],
    process_fingerprint: str,
    launch_identity: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "status_result_ref": str(status_payload.get("status_result_ref") or ""),
        "node_id": str(status_payload.get("node_id") or ""),
        "recovery_reason": str(status_payload.get("recovery_reason") or ""),
        "pid": int(status_payload.get("pid") or 0),
        "process_fingerprint": process_fingerprint,
        "launch_identity": launch_identity,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"runtime_loss_confirmed::{digest.hexdigest()[:16]}"


def _process_identity_confirmed_observation_id(
    *,
    launch_result_ref: Path,
    status_payload: dict[str, Any],
    process_fingerprint: str,
    launch_identity: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "status_result_ref": str(status_payload.get("status_result_ref") or ""),
        "node_id": str(status_payload.get("node_id") or ""),
        "pid": int(status_payload.get("pid") or 0),
        "process_fingerprint": process_fingerprint,
        "launch_identity": launch_identity,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"process_identity_confirmed::{digest.hexdigest()[:16]}"


def _supervision_result_runtime_ref(launch_result_ref: Path) -> Path:
    return launch_result_ref.with_name("ChildSupervisionRuntime.json")


def _load_optional_json_payload(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _supervision_result_attachment_state(
    *,
    result_payload: dict[str, Any],
    status_payload: dict[str, Any],
) -> str:
    attachment_state = str(status_payload.get("runtime_attachment_state") or "").strip().upper()
    if attachment_state and attachment_state != "ATTACHED":
        return attachment_state
    settled_reason = str(result_payload.get("settled_reason") or "").strip().lower()
    if settled_reason in {"pass", "terminal_nonretryable", "authoritative_result"}:
        return "TERMINAL"
    return "LOST"


def _supervision_result_mirror_observation_id(
    *,
    launch_result_ref: Path,
    supervision_result_ref: Path,
    result_payload: dict[str, Any],
    attachment_state: str,
    lease_owner_id: str,
    lease_epoch: int,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "supervision_result_ref": str(supervision_result_ref),
        "status_result_ref": str(result_payload.get("status_result_ref") or ""),
        "settled_reason": str(result_payload.get("settled_reason") or ""),
        "attachment_state": str(attachment_state or ""),
        "lease_owner_id": str(lease_owner_id or ""),
        "lease_epoch": int(lease_epoch),
        "process_fingerprint": process_fingerprint,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"supervision_result_liveness::{digest.hexdigest()[:16]}"


def _mirror_runtime_liveness_observation_from_supervision_result(
    *,
    launch_result_ref: str | Path,
    supervision_result_ref: str | Path,
    result_payload: dict[str, Any],
) -> dict[str, Any] | None:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    supervision_path = Path(supervision_result_ref).expanduser().resolve()
    try:
        launch_payload = json.loads(launch_path.read_text(encoding="utf-8"))
        state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
    except Exception:
        return None
    node_id = str(result_payload.get("node_id") or launch_payload.get("node_id") or "").strip()
    if not node_id:
        return None

    status_result_ref = str(result_payload.get("status_result_ref") or "").strip()
    status_payload = _load_optional_json_payload(Path(status_result_ref).expanduser().resolve()) if status_result_ref else {}
    runtime_ref = _supervision_result_runtime_ref(launch_path)
    runtime_payload = _load_optional_json_payload(runtime_ref)
    process_fingerprint = _runtime_status_process_fingerprint(launch_payload)
    try:
        lease_epoch = int(dict(runtime_payload or {}).get("lease_epoch") or 0)
    except (TypeError, ValueError):
        lease_epoch = 0
    lease_epoch = lease_epoch if lease_epoch > 0 else 1
    lease_owner_id = (
        f"sidecar:{runtime_ref.resolve()}"
        if runtime_payload
        else f"supervision-result:{launch_path}"
    )
    attachment_state = _supervision_result_attachment_state(
        result_payload=result_payload,
        status_payload=status_payload,
    )
    observed_at = (
        str(result_payload.get("settled_at_utc") or "").strip()
        or str(status_payload.get("runtime_observed_at") or "").strip()
        or _now_utc_iso()
    )
    evidence_refs: list[str] = []
    for candidate in (
        str(status_result_ref),
        str(supervision_path),
        str(result_payload.get("implementer_result_ref") or ""),
        str(runtime_ref.resolve()) if runtime_payload else "",
        str(launch_path),
    ):
        if candidate and candidate not in evidence_refs:
            evidence_refs.append(candidate)
    return commit_guarded_runtime_liveness_observation_event(
        state_root,
        observation_id=_supervision_result_mirror_observation_id(
            launch_result_ref=launch_path,
            supervision_result_ref=supervision_path,
            result_payload=result_payload,
            attachment_state=attachment_state,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
            process_fingerprint=process_fingerprint,
        ),
        node_id=node_id,
        payload={
            "attachment_state": attachment_state,
            "observation_kind": f"child_supervision_settled:{str(result_payload.get('settled_reason') or '').strip().lower()}",
            "observed_at": observed_at,
            "summary": f"child supervision settled to {str(result_payload.get('settled_reason') or 'unknown').strip().lower()}",
            "evidence_refs": evidence_refs,
            "pid_alive": False,
            "pid": int(status_payload.get("pid") or runtime_payload.get("pid") or 0),
            "lease_duration_s": 0.0,
            "lease_epoch": int(lease_epoch),
            "lease_owner_id": lease_owner_id,
            "process_fingerprint": process_fingerprint,
            "launch_event_id": str(launch_path),
            "process_started_at_utc": str(runtime_payload.get("started_at_utc") or ""),
            "runtime_ref": str(runtime_ref.resolve()) if runtime_payload else "",
            "status_result_ref": str(status_result_ref),
            "supervision_result_ref": str(supervision_path),
        },
        producer="loop_product.runtime.lifecycle.persist_committed_supervision_result",
    )


def _supervision_settled_observation_id(
    *,
    launch_result_ref: Path,
    supervision_result_ref: Path,
    result_payload: dict[str, Any],
    lease_owner_id: str,
    lease_epoch: int,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "supervision_result_ref": str(supervision_result_ref),
        "settled_reason": str(result_payload.get("settled_reason") or ""),
        "status_result_ref": str(result_payload.get("status_result_ref") or ""),
        "lease_owner_id": str(lease_owner_id or ""),
        "lease_epoch": int(lease_epoch),
        "process_fingerprint": process_fingerprint,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"supervision_settled::{digest.hexdigest()[:16]}"


def _process_exit_observed_observation_id(
    *,
    launch_result_ref: Path,
    supervision_result_ref: Path,
    attachment_state: str,
    status_result_ref: str,
    lease_owner_id: str,
    lease_epoch: int,
    process_fingerprint: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "launch_result_ref": str(launch_result_ref),
        "supervision_result_ref": str(supervision_result_ref),
        "status_result_ref": str(status_result_ref or ""),
        "attachment_state": str(attachment_state or ""),
        "lease_owner_id": str(lease_owner_id or ""),
        "lease_epoch": int(lease_epoch),
        "process_fingerprint": process_fingerprint,
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"process_exit_observed::{digest.hexdigest()[:16]}"


def _commit_supervision_exit_events_from_result(
    *,
    launch_result_ref: str | Path,
    supervision_result_ref: str | Path,
    result_payload: dict[str, Any],
) -> dict[str, Any] | None:
    launch_path = Path(launch_result_ref).expanduser().resolve()
    supervision_path = Path(supervision_result_ref).expanduser().resolve()
    try:
        launch_payload = json.loads(launch_path.read_text(encoding="utf-8"))
        state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
    except Exception:
        return None
    node_id = str(result_payload.get("node_id") or launch_payload.get("node_id") or "").strip()
    if not node_id:
        return None

    status_result_ref = str(result_payload.get("status_result_ref") or "").strip()
    status_payload = _load_optional_json_payload(Path(status_result_ref).expanduser().resolve()) if status_result_ref else {}
    runtime_ref = _supervision_result_runtime_ref(launch_path)
    runtime_payload = _load_optional_json_payload(runtime_ref)
    process_fingerprint = _runtime_status_process_fingerprint(launch_payload)
    try:
        lease_epoch = int(dict(runtime_payload or {}).get("lease_epoch") or 0)
    except (TypeError, ValueError):
        lease_epoch = 0
    lease_epoch = lease_epoch if lease_epoch > 0 else 1
    lease_owner_id = (
        f"sidecar:{runtime_ref.resolve()}"
        if runtime_payload
        else f"supervision-result:{launch_path}"
    )
    attachment_state = _supervision_result_attachment_state(
        result_payload=result_payload,
        status_payload=status_payload,
    )
    observed_at = (
        str(status_payload.get("runtime_observed_at") or "").strip()
        or _now_utc_iso()
    )
    evidence_refs: list[str] = []
    for candidate in (
        str(status_result_ref),
        str(supervision_path),
        str(result_payload.get("implementer_result_ref") or ""),
        str(runtime_ref.resolve()) if runtime_payload else "",
        str(launch_path),
    ):
        if candidate and candidate not in evidence_refs:
            evidence_refs.append(candidate)

    settled_event = commit_supervision_settled_event(
        state_root,
        observation_id=_supervision_settled_observation_id(
            launch_result_ref=launch_path,
            supervision_result_ref=supervision_path,
            result_payload=result_payload,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
            process_fingerprint=process_fingerprint,
        ),
        node_id=node_id,
        payload={
            "observed_at": observed_at,
            "settled_reason": str(result_payload.get("settled_reason") or "").strip().lower(),
            "recoveries_used": int(result_payload.get("recoveries_used") or 0),
            "attachment_state": attachment_state,
            "lease_epoch": int(lease_epoch),
            "lease_owner_id": lease_owner_id,
            "process_fingerprint": process_fingerprint,
            "launch_event_id": str(launch_path),
            "process_started_at_utc": str(runtime_payload.get("started_at_utc") or ""),
            "runtime_ref": str(runtime_ref.resolve()) if runtime_payload else "",
            "status_result_ref": str(status_result_ref),
            "supervision_result_ref": str(supervision_path),
            "evidence_refs": evidence_refs,
            "pid": int(status_payload.get("pid") or runtime_payload.get("pid") or 0),
        },
        producer="loop_product.runtime.lifecycle.persist_committed_supervision_result",
    )

    exit_event: dict[str, Any] | None = None
    if attachment_state in {"LOST", "TERMINAL"}:
        exit_event = commit_process_exit_observed_event(
            state_root,
            observation_id=_process_exit_observed_observation_id(
                launch_result_ref=launch_path,
                supervision_result_ref=supervision_path,
                attachment_state=attachment_state,
                status_result_ref=status_result_ref,
                lease_owner_id=lease_owner_id,
                lease_epoch=lease_epoch,
                process_fingerprint=process_fingerprint,
            ),
            node_id=node_id,
            payload={
                "observed_at": observed_at,
                "attachment_state": attachment_state,
                "observation_kind": f"child_supervision_settled:{str(result_payload.get('settled_reason') or '').strip().lower()}",
                "lease_epoch": int(lease_epoch),
                "lease_owner_id": lease_owner_id,
                "process_fingerprint": process_fingerprint,
                "launch_event_id": str(launch_path),
                "process_started_at_utc": str(runtime_payload.get("started_at_utc") or ""),
                "runtime_ref": str(runtime_ref.resolve()) if runtime_payload else "",
                "status_result_ref": str(status_result_ref),
                "supervision_result_ref": str(supervision_path),
                "evidence_refs": evidence_refs,
                "pid": int(status_payload.get("pid") or runtime_payload.get("pid") or 0),
            },
            producer="loop_product.runtime.lifecycle.persist_committed_supervision_result",
        )

    return {
        "supervision_settled_event": settled_event,
        "process_exit_observed_event": exit_event,
    }


def _lease_expiry_materialization_observation_id(
    *,
    node_id: str,
    expired_from_event_id: str,
    lease_expires_at: str,
    lease_epoch: int,
    lease_owner_id: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "node_id": str(node_id or ""),
        "expired_from_event_id": str(expired_from_event_id or ""),
        "lease_expires_at": str(lease_expires_at or ""),
        "lease_epoch": int(lease_epoch),
        "lease_owner_id": str(lease_owner_id or ""),
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"lease_expiry::{digest.hexdigest()[:16]}"


def _lease_expired_observation_id(
    *,
    node_id: str,
    expired_from_event_id: str,
    lease_expires_at: str,
    lease_epoch: int,
    lease_owner_id: str,
) -> str:
    digest = hashlib.sha256()
    stable_payload = {
        "node_id": str(node_id or ""),
        "expired_from_event_id": str(expired_from_event_id or ""),
        "lease_expires_at": str(lease_expires_at or ""),
        "lease_epoch": int(lease_epoch),
        "lease_owner_id": str(lease_owner_id or ""),
    }
    digest.update(json.dumps(stable_payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"lease_expired::{digest.hexdigest()[:16]}"


def materialize_runtime_liveness_lease_expiry_events(
    *,
    state_root: str | Path,
    now_utc: str = "",
) -> dict[str, Any]:
    """Materialize durable non-live facts for expired attached leases."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    existing_event_ids: set[str] = set()
    observations_by_node: dict[str, list[dict[str, Any]]] = {}
    effective_now_utc = str(now_utc or "").strip() or _now_utc_iso()

    for event in iter_committed_events(runtime_root):
        existing_event_ids.add(str(event.get("event_id") or ""))
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        payload = dict(event.get("payload") or {})
        node_id = str(event.get("node_id") or "")
        summary = {
            "seq": int(event.get("seq") or 0),
            "event_id": str(event.get("event_id") or ""),
            "recorded_at": str(event.get("recorded_at") or ""),
            "node_id": node_id,
            "payload": payload,
        }
        summary.update(
            summarize_runtime_liveness_observation(
                payload,
                recorded_at=str(summary.get("recorded_at") or ""),
                now_utc=effective_now_utc,
            )
        )
        observations_by_node.setdefault(node_id, []).append(summary)

    materialized_node_ids: list[str] = []
    materialized_event_ids: list[str] = []
    canonical_materialized_event_ids: list[str] = []
    candidate_node_ids: list[str] = []
    for node_id, observations in sorted(observations_by_node.items()):
        raw_head, effective_head = select_effective_runtime_liveness_head(observations)
        if not raw_head or not effective_head:
            continue
        if str(raw_head.get("raw_attachment_state") or "") != "ATTACHED":
            continue
        if str(raw_head.get("lease_freshness") or "") != "expired":
            continue
        if str(effective_head.get("effective_attachment_state") or "") != "UNOBSERVED":
            continue
        if int(raw_head.get("seq") or 0) != int(effective_head.get("seq") or 0):
            continue
        candidate_node_ids.append(str(node_id))
        raw_payload = dict(raw_head.get("payload") or {})
        lease_epoch = int(raw_head.get("lease_epoch") or 0)
        lease_owner_id = str(raw_head.get("lease_owner_id") or "")
        lease_expires_at = str(raw_head.get("lease_expires_at") or "").strip() or effective_now_utc
        evidence_refs = [str(item) for item in list(raw_payload.get("evidence_refs") or []) if str(item)]
        for candidate in (
            str(raw_payload.get("status_result_ref") or ""),
            str(raw_payload.get("runtime_ref") or ""),
            str(raw_payload.get("launch_event_id") or ""),
        ):
            if candidate and candidate not in evidence_refs:
                evidence_refs.append(candidate)
        result = commit_guarded_runtime_liveness_observation_event(
            runtime_root,
            observation_id=_lease_expiry_materialization_observation_id(
                node_id=node_id,
                expired_from_event_id=str(raw_head.get("event_id") or ""),
                lease_expires_at=lease_expires_at,
                lease_epoch=lease_epoch,
                lease_owner_id=lease_owner_id,
            ),
            node_id=node_id,
            payload={
                "attachment_state": "LOST",
                "observation_kind": "lease_expired",
                "observed_at": lease_expires_at,
                "summary": "attached lease expired without fresh heartbeat",
                "evidence_refs": evidence_refs,
                "pid_alive": False,
                "pid": int(raw_payload.get("pid") or 0),
                "lease_duration_s": 0.0,
                "lease_epoch": lease_epoch,
                "lease_owner_id": lease_owner_id,
                "process_fingerprint": str(raw_payload.get("process_fingerprint") or ""),
                "launch_event_id": str(raw_payload.get("launch_event_id") or ""),
                "process_started_at_utc": str(raw_payload.get("process_started_at_utc") or ""),
                "status_result_ref": str(raw_payload.get("status_result_ref") or ""),
                "runtime_ref": str(raw_payload.get("runtime_ref") or ""),
                "expired_from_event_id": str(raw_head.get("event_id") or ""),
                "expired_from_seq": int(raw_head.get("seq") or 0),
                "expired_from_observed_at": str(raw_head.get("observed_at") or ""),
            },
            producer="loop_product.runtime.lifecycle.materialize_runtime_liveness_lease_expiry_events",
        )
        canonical_result = commit_lease_expired_event(
            runtime_root,
            observation_id=_lease_expired_observation_id(
                node_id=node_id,
                expired_from_event_id=str(raw_head.get("event_id") or ""),
                lease_expires_at=lease_expires_at,
                lease_epoch=lease_epoch,
                lease_owner_id=lease_owner_id,
            ),
            node_id=node_id,
            payload={
                "observed_at": lease_expires_at,
                "attachment_state": "LOST",
                "observation_kind": "lease_expired",
                "lease_epoch": lease_epoch,
                "lease_owner_id": lease_owner_id,
                "process_fingerprint": str(raw_payload.get("process_fingerprint") or ""),
                "launch_event_id": str(raw_payload.get("launch_event_id") or ""),
                "process_started_at_utc": str(raw_payload.get("process_started_at_utc") or ""),
                "status_result_ref": str(raw_payload.get("status_result_ref") or ""),
                "runtime_ref": str(raw_payload.get("runtime_ref") or ""),
                "pid": int(raw_payload.get("pid") or 0),
                "evidence_refs": evidence_refs,
                "expired_from_event_id": str(raw_head.get("event_id") or ""),
                "expired_from_seq": int(raw_head.get("seq") or 0),
                "expired_from_observed_at": str(raw_head.get("observed_at") or ""),
                "expired_from_observation_kind": str(raw_payload.get("observation_kind") or ""),
                "expired_from_launch_event_id": str(raw_payload.get("launch_event_id") or ""),
            },
            producer="loop_product.runtime.lifecycle.materialize_runtime_liveness_lease_expiry_events",
        )
        event_id = str(result.get("event_id") or "")
        if event_id and event_id not in existing_event_ids:
            existing_event_ids.add(event_id)
            materialized_node_ids.append(str(node_id))
            materialized_event_ids.append(event_id)
        canonical_event_id = str(canonical_result.get("event_id") or "")
        if canonical_event_id and canonical_event_id not in existing_event_ids:
            existing_event_ids.add(canonical_event_id)
            canonical_materialized_event_ids.append(canonical_event_id)

    return {
        "runtime_root": str(runtime_root),
        "now_utc": effective_now_utc,
        "candidate_node_ids": candidate_node_ids,
        "materialized_node_ids": materialized_node_ids,
        "materialized_event_ids": materialized_event_ids,
        "materialized_event_count": len(materialized_event_ids),
        "canonical_materialized_event_ids": canonical_materialized_event_ids,
        "canonical_materialized_event_count": len(canonical_materialized_event_ids),
    }


def _mirror_runtime_liveness_observation_from_status_result(
    *,
    launch_result_ref: str | Path,
    status_payload: dict[str, Any],
) -> dict[str, Any] | None:
    result_path = Path(launch_result_ref).expanduser().resolve()
    try:
        launch_payload = json.loads(result_path.read_text(encoding="utf-8"))
        state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
    except Exception:
        return None
    node_id = str(status_payload.get("node_id") or launch_payload.get("node_id") or "").strip()
    if not node_id:
        return None
    process_fingerprint = _runtime_status_process_fingerprint(launch_payload)
    launch_identity = str(result_path)
    observed_at = str(status_payload.get("runtime_observed_at") or "").strip() or _now_utc_iso()
    event_payload = {
        "attachment_state": str(status_payload.get("runtime_attachment_state") or "").strip().upper(),
        "observation_kind": str(status_payload.get("runtime_observation_kind") or "").strip() or "trusted_runtime_status_surface",
        "observed_at": observed_at,
        "summary": str(status_payload.get("runtime_summary") or ""),
        "evidence_refs": [str(item) for item in list(status_payload.get("runtime_evidence_refs") or [])],
        "pid_alive": bool(status_payload.get("pid_alive")),
        "pid": int(status_payload.get("pid") or 0),
        "lease_duration_s": 60.0,
        "lease_epoch": 1,
        "lease_owner_id": f"status-query:{result_path}",
        "process_fingerprint": process_fingerprint,
        "launch_event_id": launch_identity,
        "status_result_ref": str(status_payload.get("status_result_ref") or ""),
    }
    liveness_outcome = commit_guarded_runtime_liveness_observation_event(
        state_root,
        observation_id=_runtime_status_mirror_observation_id(
            launch_result_ref=result_path,
            status_payload=status_payload,
            process_fingerprint=process_fingerprint,
            launch_identity=launch_identity,
        ),
        node_id=node_id,
        payload=event_payload,
        producer="loop_product.runtime.lifecycle.child_runtime_status_from_launch_result_ref",
    )
    identity_event: dict[str, Any] | None = None
    if str(event_payload.get("attachment_state") or "") == "ATTACHED" and bool(status_payload.get("pid_alive")):
        identity_event = commit_process_identity_confirmed_event(
            state_root,
            observation_id=_process_identity_confirmed_observation_id(
                launch_result_ref=result_path,
                status_payload=status_payload,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
            ),
            node_id=node_id,
            payload={
                "observed_at": observed_at,
                "attachment_state": str(event_payload.get("attachment_state") or ""),
                "observation_kind": str(event_payload.get("observation_kind") or ""),
                "summary": str(event_payload.get("summary") or ""),
                "evidence_refs": list(event_payload.get("evidence_refs") or []),
                "pid_alive": bool(event_payload.get("pid_alive")),
                "pid": int(event_payload.get("pid") or 0),
                "process_fingerprint": process_fingerprint,
                "launch_event_id": launch_identity,
                "status_result_ref": str(status_payload.get("status_result_ref") or ""),
            },
            producer="loop_product.runtime.lifecycle.child_runtime_status_from_launch_result_ref",
        )
    canonical_event: dict[str, Any] | None = None
    if (
        str(event_payload.get("attachment_state") or "") == "LOST"
        and bool(status_payload.get("recovery_eligible"))
        and str(status_payload.get("recovery_reason") or "") == "active_without_live_pid"
    ):
        canonical_event = commit_runtime_loss_confirmed_event(
            state_root,
            observation_id=_runtime_loss_confirmed_observation_id(
                launch_result_ref=result_path,
                status_payload=status_payload,
                process_fingerprint=process_fingerprint,
                launch_identity=launch_identity,
            ),
            node_id=node_id,
            payload={
                "observed_at": observed_at,
                "attachment_state": str(event_payload.get("attachment_state") or ""),
                "recovery_reason": str(status_payload.get("recovery_reason") or ""),
                "observation_kind": str(event_payload.get("observation_kind") or ""),
                "summary": str(event_payload.get("summary") or ""),
                "evidence_refs": list(event_payload.get("evidence_refs") or []),
                "pid": int(event_payload.get("pid") or 0),
                "process_fingerprint": process_fingerprint,
                "launch_event_id": launch_identity,
                "status_result_ref": str(status_payload.get("status_result_ref") or ""),
            },
            producer="loop_product.runtime.lifecycle.child_runtime_status_from_launch_result_ref",
        )
    return {
        **dict(liveness_outcome or {}),
        "process_identity_confirmed_event": identity_event,
        "runtime_loss_confirmed_event": canonical_event,
    }


def synchronize_authoritative_state_from_launch_result_ref(*, result_ref: str | Path) -> Path | None:
    """Best-effort runtime-owned sync for a committed child launch result."""

    result_path = Path(result_ref).expanduser().resolve()
    try:
        launch_payload = json.loads(result_path.read_text(encoding="utf-8"))
        state_root = require_runtime_root(Path(str(launch_payload["state_root"])).expanduser().resolve())
        node_id = str(launch_payload.get("node_id") or "").strip()
        preserved_retryable_snapshot = _load_retryable_sink_preservation_candidate(
            state_root=state_root,
            node_id=node_id,
        )
        synchronize_authoritative_node_results(
            state_root,
            continue_deferred=True,
            authority=kernel_internal_authority(),
        )
        synchronize_supervision_stop_point_from_launch_result_ref(
            state_root,
            launch_result_ref=result_path,
            authority=kernel_internal_authority(),
        )
        if preserved_retryable_snapshot is not None:
            _restore_retryable_sink_if_clobbered(
                state_root=state_root,
                node_id=node_id,
                launch_result_ref=result_path,
                preserved_snapshot=preserved_retryable_snapshot,
            )
        return state_root
    except Exception:
        return None


def _load_retryable_sink_preservation_candidate(
    *,
    state_root: Path,
    node_id: str,
) -> dict[str, Any] | None:
    if not node_id:
        return None
    node_ref = state_root / "state" / f"{node_id}.json"
    try:
        node_payload = json.loads(node_ref.read_text(encoding="utf-8"))
    except Exception:
        return None
    sink = str(node_payload.get("result_sink_ref") or "").strip()
    if not sink:
        return None
    result_ref = (state_root / sink).resolve()
    try:
        result_payload = json.loads(result_ref.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not authoritative_result_retryable_nonterminal(result_payload):
        return None
    return {
        "node_ref": str(node_ref.resolve()),
        "result_sink_ref": sink,
        "node_payload": dict(node_payload),
    }


def _restore_retryable_sink_if_clobbered(
    *,
    state_root: Path,
    node_id: str,
    launch_result_ref: Path,
    preserved_snapshot: dict[str, Any],
) -> bool:
    if not node_id:
        return False
    if launch_result_ref.with_name("ChildSupervisionResult.json").exists():
        return False
    node_ref = state_root / "state" / f"{node_id}.json"
    try:
        current_node_payload = json.loads(node_ref.read_text(encoding="utf-8"))
    except Exception:
        current_node_payload = {}
    current_sink = str(current_node_payload.get("result_sink_ref") or "").strip()
    if current_sink:
        current_result_ref = (state_root / current_sink).resolve()
        try:
            current_result_payload = json.loads(current_result_ref.read_text(encoding="utf-8"))
        except Exception:
            current_result_payload = {}
        if authoritative_result_retryable_nonterminal(current_result_payload):
            return False

    restored_node_payload = dict(current_node_payload)
    preserved_node_payload = dict(preserved_snapshot.get("node_payload") or {})
    restored_node_payload["result_sink_ref"] = str(
        preserved_node_payload.get("result_sink_ref")
        or preserved_snapshot.get("result_sink_ref")
        or ""
    )
    if str(restored_node_payload.get("status") or "").strip() not in {"", "ACTIVE"}:
        restored_node_payload["status"] = str(preserved_node_payload.get("status") or "ACTIVE")
    preserved_runtime_state = dict(preserved_node_payload.get("runtime_state") or {})
    if preserved_runtime_state:
        restored_node_payload["runtime_state"] = preserved_runtime_state

    authority = kernel_internal_authority()
    persist_node_snapshot(state_root, restored_node_payload, authority=authority)
    kernel_state = load_kernel_state(state_root)
    if node_id in kernel_state.nodes:
        kernel_state.nodes[node_id] = dict(restored_node_payload)
        if str(restored_node_payload.get("status") or "").strip() in {"", "ACTIVE"}:
            kernel_state.blocked_reasons.pop(node_id, None)
        persist_kernel_state(state_root, kernel_state, authority=authority)
    return True


def synchronize_authoritative_state(
    *,
    state_root: str | Path,
    continue_deferred: bool = True,
) -> KernelState:
    """Public trusted surface for authoritative result -> kernel/node synchronization."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    return synchronize_authoritative_node_results(
        runtime_root,
        continue_deferred=continue_deferred,
        authority=kernel_internal_authority(),
    )


def backfill_authoritative_terminal_results(
    *,
    state_root: str | Path,
    node_ids: Iterable[str] | None = None,
    continue_deferred: bool = False,
) -> list[str]:
    """Repair stale authoritative implementer_result sinks, then resynchronize derived state."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    selected = {str(item).strip() for item in (node_ids or []) if str(item).strip()}
    from loop_product.loop.evaluator_client import canonicalize_authoritative_implementer_result_payload

    kernel_state = load_kernel_state(runtime_root)
    repaired: list[str] = []
    for node_id, node_payload in dict(kernel_state.nodes).items():
        if selected and node_id not in selected:
            continue
        payload = dict(node_payload or {})
        result_sink_ref = str(payload.get("result_sink_ref") or "").strip()
        if not result_sink_ref:
            continue
        kernel_result_ref = (runtime_root / result_sink_ref).resolve()
        if not kernel_result_ref.exists():
            continue
        try:
            existing = json.loads(kernel_result_ref.read_text(encoding="utf-8"))
        except Exception:
            continue
        normalized = canonicalize_authoritative_implementer_result_payload(existing=existing)
        kernel_needs_rewrite = normalized != existing or is_user_writable(kernel_result_ref)
        if kernel_needs_rewrite:
            write_json_read_only(kernel_result_ref, normalized)
            workspace_result_ref = str(normalized.get("workspace_result_sink_ref") or "").strip()
            if workspace_result_ref:
                workspace_path = Path(workspace_result_ref).expanduser().resolve()
                try:
                    workspace_existing = json.loads(workspace_path.read_text(encoding="utf-8"))
                except Exception:
                    workspace_existing = {}
                workspace_normalized = canonicalize_authoritative_implementer_result_payload(existing=workspace_existing or normalized)
                if workspace_normalized != workspace_existing or (workspace_path.exists() and is_user_writable(workspace_path)):
                    write_json_read_only(workspace_path, workspace_normalized)
            repaired.append(node_id)
    if repaired:
        synchronize_authoritative_node_results(
            runtime_root,
            continue_deferred=continue_deferred,
            authority=kernel_internal_authority(),
        )
    return repaired


def node_status_for_outcome(outcome: NodeTerminalOutcome) -> str:
    if outcome is NodeTerminalOutcome.PASS:
        return "COMPLETED"
    if outcome is NodeTerminalOutcome.STUCK:
        return "BLOCKED"
    return "FAILED"


def lifecycle_status(node: NodeSpec) -> str:
    return node.status.value


def build_node_terminal_result(
    node: NodeSpec,
    *,
    outcome: NodeTerminalOutcome,
    summary: str,
    evidence_refs: list[str] | tuple[str, ...] = (),
    self_attribution: str = "",
    self_repair: str = "",
) -> NodeTerminalResult:
    """Create a structured node terminal result for gateway submission."""

    return NodeTerminalResult(
        node_id=node.node_id,
        round_id=node.round_id,
        generation=node.generation,
        outcome=outcome,
        node_status=node_status_for_outcome(outcome),
        summary=summary,
        evidence_refs=[str(item) for item in evidence_refs],
        self_attribution=self_attribution,
        self_repair=self_repair,
    )


def bootstrap_first_implementer_node(**payload: Any) -> dict[str, Any]:
    """Public trusted surface for first implementer bootstrap."""

    authority = kernel_internal_authority()
    return dispatch_bootstrap_first_implementer_node(authority=authority, **payload)


def bootstrap_first_implementer_from_endpoint(**payload: Any) -> dict[str, Any]:
    """Public trusted surface for endpoint-driven first implementer bootstrap."""

    authority = kernel_internal_authority()
    return dispatch_bootstrap_first_implementer_from_endpoint(authority=authority, **payload)


def recover_orphaned_active_node(**payload: Any) -> dict[str, Any]:
    """Public trusted surface for same-node orphaned-ACTIVE recovery."""

    authority = kernel_internal_authority()
    return dispatch_recover_orphaned_active_node(authority=authority, **payload)


def launch_child_from_result_ref(
    *,
    result_ref: str | Path,
    startup_probe_ms: int = 1500,
    startup_health_timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Public trusted surface for committed child launch plus supervision attach."""

    return runtime_launch_child_from_result_ref(
        result_ref=result_ref,
        startup_probe_ms=startup_probe_ms,
        startup_health_timeout_ms=startup_health_timeout_ms,
    )


def child_runtime_status_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
) -> dict[str, Any]:
    """Public trusted surface for committed child-runtime status queries."""

    result_path = Path(result_ref).expanduser().resolve()
    synchronize_authoritative_state_from_launch_result_ref(result_ref=result_path)

    payload = dispatch_child_runtime_status_from_launch_result_ref(
        result_ref=result_path,
        stall_threshold_s=stall_threshold_s,
    )
    try:
        _mirror_runtime_liveness_observation_from_status_result(
            launch_result_ref=result_path,
            status_payload=payload,
        )
        synchronize_authoritative_state_from_launch_result_ref(result_ref=result_path)
    except Exception:
        pass
    return payload


def child_progress_snapshot_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
) -> dict[str, Any]:
    """Public trusted surface for committed child progress snapshots."""

    return dispatch_child_progress_snapshot_from_launch_result_ref(
        result_ref=result_ref,
        stall_threshold_s=stall_threshold_s,
    )


def committed_supervision_result_ref(*, launch_result_ref: str | Path) -> Path:
    """Return the committed supervision result sink next to a launch result."""

    return Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionResult.json")


def persist_committed_supervision_result(
    *,
    launch_result_ref: str | Path,
    result_payload: dict[str, Any],
) -> tuple[Path, str]:
    """Persist one settled supervision result and best-effort synchronize authoritative state."""

    launch_path = Path(launch_result_ref).expanduser().resolve()
    result_ref = committed_supervision_result_ref(launch_result_ref=launch_path)
    validate_repo_object("LoopChildSupervisionResult.schema.json", result_payload)
    write_json_read_only(result_ref, result_payload)
    try:
        _mirror_runtime_liveness_observation_from_supervision_result(
            launch_result_ref=launch_path,
            supervision_result_ref=result_ref,
            result_payload=dict(result_payload or {}),
        )
    except Exception:
        pass
    try:
        _commit_supervision_exit_events_from_result(
            launch_result_ref=launch_path,
            supervision_result_ref=result_ref,
            result_payload=dict(result_payload or {}),
        )
    except Exception:
        pass
    sync_error = ""
    try:
        synchronize_authoritative_state_from_launch_result_ref(result_ref=launch_path)
    except Exception as exc:  # noqa: BLE001
        sync_error = str(exc)
    return result_ref, sync_error


def publish_external_from_implementer_result(*, result_ref: str | Path) -> dict[str, Any]:
    """Public trusted surface for deterministic root-kernel external publication."""

    return dispatch_publish_external_from_implementer_result(result_ref=result_ref)


def supervise_child_until_settled(
    *,
    launch_result_ref: str | Path,
    poll_interval_s: float = 2.0,
    stall_threshold_s: float = 60.0,
    max_recoveries: int = 5,
    max_wall_clock_s: float = 0.0,
    status_observer: Callable[[dict[str, Any]], None] | None = None,
    no_substantive_progress_window_s: float = 300.0,
) -> dict[str, Any]:
    """Public trusted surface for committed root-side child supervision."""

    return dispatch_supervise_child_until_settled(
        launch_result_ref=launch_result_ref,
        poll_interval_s=poll_interval_s,
        stall_threshold_s=stall_threshold_s,
        max_recoveries=max_recoveries,
        max_wall_clock_s=max_wall_clock_s,
        no_substantive_progress_window_s=no_substantive_progress_window_s,
        status_observer=status_observer,
        runtime_status_reader=child_runtime_status_from_launch_result_ref,
        recovery_runner=recover_orphaned_active_node,
        launcher=dispatch_launch_child_from_result_ref,
    )


def initialize_evaluator_runtime(
    *,
    state_root: Path,
    task_id: str,
    root_goal: str,
    child_goal_slice: str,
    child_node_id: str = "child-implementer-001",
    round_id: str = "R1",
) -> dict[str, Any]:
    """Prepare a trusted `.loop` runtime tree for an ordinary implementer evaluator run."""

    authority = kernel_internal_authority()
    state_root = require_runtime_root(state_root)
    ensure_runtime_tree(state_root)
    from loop_product.runtime.gc import ensure_housekeeping_reap_controller_service_for_runtime_root
    from loop_product.runtime.control_plane import ensure_repo_control_plane_service_for_runtime_root

    ensure_housekeeping_reap_controller_service_for_runtime_root(state_root=state_root)
    ensure_repo_control_plane_service_for_runtime_root(state_root=state_root)

    kernel_state_path = state_root / "state" / "kernel_state.json"
    if kernel_state_path.exists():
        kernel_state = load_kernel_state(state_root)
    else:
        root_node = NodeSpec(
            node_id="root-kernel",
            node_kind="kernel",
            goal_slice=root_goal,
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy=kernel_execution_policy(),
            reasoning_profile=kernel_reasoning_profile(),
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/runtime_bootstrap_summary.json",
            lineage_ref="root-kernel",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id=task_id,
            root_goal=root_goal,
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        persist_kernel_state(state_root, kernel_state, authority=authority)

    root_node_id = str(kernel_state.root_node_id)
    if root_node_id not in kernel_state.nodes:
        raise ValueError(f"kernel state missing root node snapshot for {root_node_id!r}")

    if child_node_id not in kernel_state.nodes:
        materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id=root_node_id,
            node_id=child_node_id,
            goal_slice=child_goal_slice,
            round_id=round_id,
            execution_policy=implementer_execution_policy(),
            reasoning_profile=implementer_reasoning_profile(),
            budget_profile=implementer_budget_profile(),
            authority=authority,
        )

    runtime_context = load_child_runtime_context(state_root, child_node_id)
    return {
        "state_root": str(state_root.resolve()),
        "kernel_state_ref": str((state_root / "state" / "kernel_state.json").resolve()),
        "child_node_id": child_node_id,
        "child_node_ref": str((state_root / "state" / f"{child_node_id}.json").resolve()),
        "delegation_ref": str((state_root / "state" / "delegations" / f"{child_node_id}.json").resolve()),
        "workspace_root": str(runtime_context.get("workspace_root") or ""),
        "codex_home": str(runtime_context.get("codex_home") or ""),
        "runtime_context": runtime_context,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="loop_product runtime lifecycle helpers")
    subparsers = parser.add_subparsers(dest="command")

    bootstrap = subparsers.add_parser(
        "bootstrap-first-implementer",
        help="Materialize the first implementer node and its frozen launch bundle",
    )
    bootstrap.add_argument("--request", required=True, help="JSON file following LoopFirstImplementerBootstrapRequest")
    bootstrap_from_endpoint = subparsers.add_parser(
        "bootstrap-first-implementer-from-endpoint",
        help="Materialize the first implementer node directly from a clarified endpoint artifact",
    )
    bootstrap_from_endpoint.add_argument("--endpoint-artifact-ref", required=True, help="Path to EndpointArtifact.json")
    bootstrap_from_endpoint.add_argument("--external-publish-target", default="", help="Optional publish target outside the workspace")
    bootstrap_from_endpoint.add_argument("--workspace-mirror-relpath", default="", help="Optional workspace mirror relpath override")
    bootstrap_from_endpoint.add_argument("--task-slug", default="", help="Optional task slug override")
    bootstrap_from_endpoint.add_argument("--workspace-root", default="", help="Optional exact workspace root for fresh bootstrap")
    bootstrap_from_endpoint.add_argument("--state-root", default="", help="Optional exact state root for fresh bootstrap")
    bootstrap_from_endpoint.add_argument("--node-id", default="", help="Optional exact node id override")
    bootstrap_from_endpoint.add_argument("--round-id", default="R1", help="Optional round id override")
    bootstrap_from_endpoint.add_argument("--context-ref", action="append", default=[], help="Optional extra context ref (repeatable)")
    bootstrap_from_endpoint.add_argument("--result-sink-ref", default="", help="Optional result sink ref override")
    recovery = subparsers.add_parser(
        "recover-orphaned-active",
        help="Accept same-node retry for an orphaned ACTIVE child and rebuild its canonical launch spec",
    )
    recovery.add_argument("--request", default="", help="Optional JSON file following LoopOrphanedActiveRecoveryRequest")
    recovery.add_argument("--state-root", default="", help="Runtime state root for direct structured recovery")
    recovery.add_argument("--node-id", default="", help="Exact ACTIVE node id for direct structured recovery")
    recovery.add_argument("--workspace-root", default="", help="Optional exact workspace root guard for direct structured recovery")
    recovery.add_argument("--confirmed-launch-result-ref", default="", help="Optional exact launch result ref already confirmed by the caller")
    recovery.add_argument("--confirmed-runtime-status-ref", default="", help="Optional exact runtime status ref already confirmed by the caller")
    recovery.add_argument("--reason", default="", help="Recovery reason for direct structured recovery")
    recovery.add_argument("--self-attribution", default="", help="Self-attribution for direct structured recovery")
    recovery.add_argument("--self-repair", default="", help="Self-repair for direct structured recovery")
    recovery.add_argument("--observation-kind", default="", help="Observation kind for runtime-loss evidence")
    recovery.add_argument("--summary", default="", help="Human-readable runtime-loss summary")
    recovery.add_argument("--evidence-ref", action="append", default=[], help="Repeatable evidence ref for runtime-loss evidence")
    launch = subparsers.add_parser(
        "launch-child",
        help="Launch a materialized child from a bootstrap/recovery result carrying launch_spec",
    )
    launch.add_argument("--result-ref", required=True, help="Path to a bootstrap/recovery result JSON containing launch_spec")
    launch.add_argument("--startup-probe-ms", type=int, default=1500, help="Milliseconds to wait before classifying launch as started/exited")
    launch.add_argument(
        "--startup-health-timeout-ms",
        type=int,
        default=12000,
        help="Additional milliseconds to wait for startup failure evidence to settle before treating the child as healthy",
    )
    runtime_status = subparsers.add_parser(
        "child-runtime-status",
        help="Summarize current runtime status for a committed ChildLaunchResult",
    )
    runtime_status.add_argument("--result-ref", required=True, help="Path to a ChildLaunchResult.json")
    runtime_status.add_argument(
        "--stall-threshold-s",
        type=float,
        default=60.0,
        help="Seconds of log silence before a still-live child is marked as stalled_hint",
    )
    progress_snapshot = subparsers.add_parser(
        "child-progress-snapshot",
        help="Summarize current child progress, evaluator state, and observed document refs",
    )
    progress_snapshot.add_argument("--launch-result-ref", required=True, help="Path to a ChildLaunchResult.json")
    progress_snapshot.add_argument(
        "--stall-threshold-s",
        type=float,
        default=60.0,
        help="Seconds of log silence before a still-live child is considered stalled in the underlying runtime status",
    )
    supervise = subparsers.add_parser(
        "supervise-child",
        help="Keep supervising a launched child until PASS, a confirmed non-retryable terminal result, or timeout",
    )
    supervise.add_argument("--launch-result-ref", required=True, help="Path to a ChildLaunchResult.json")
    supervise.add_argument("--poll-interval-s", type=float, default=2.0, help="Polling interval between supervision checks")
    supervise.add_argument("--stall-threshold-s", type=float, default=60.0, help="Seconds of log silence before a live child is considered stalled")
    supervise.add_argument("--max-recoveries", type=int, default=5, help="Maximum committed recover/relaunch cycles before settling exhausted")
    supervise.add_argument("--max-wall-clock-s", type=float, default=0.0, help="Optional supervision timeout in seconds; 0 disables timeout")
    supervise.add_argument(
        "--no-substantive-progress-window-s",
        type=float,
        default=300.0,
        help="Optional placeholder-only progress window in seconds before supervision returns a truthful no_substantive_progress stop point; 0 disables it",
    )
    publish = subparsers.add_parser(
        "publish-external-result",
        help="Publish the exact implementer workspace artifact to its frozen external target",
    )
    publish.add_argument("--result-ref", required=True, help="Path to the authoritative implementer_result.json")
    return parser


def _cmd_bootstrap_first_implementer(args: argparse.Namespace) -> int:
    request_path = Path(args.request).expanduser().resolve()
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    result = bootstrap_first_implementer_node(**payload)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_bootstrap_first_implementer_from_endpoint(args: argparse.Namespace) -> int:
    result = bootstrap_first_implementer_from_endpoint(
        endpoint_artifact_ref=args.endpoint_artifact_ref,
        external_publish_target=args.external_publish_target,
        workspace_mirror_relpath=args.workspace_mirror_relpath,
        task_slug=args.task_slug,
        workspace_root=args.workspace_root,
        state_root=args.state_root,
        node_id=args.node_id,
        round_id=args.round_id,
        context_refs=list(args.context_ref or []),
        result_sink_ref=args.result_sink_ref,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_recover_orphaned_active(args: argparse.Namespace) -> int:
    if str(args.request or "").strip():
        request_path = Path(args.request).expanduser().resolve()
        payload = json.loads(request_path.read_text(encoding="utf-8"))
    else:
        payload = {
            "state_root": args.state_root,
            "node_id": args.node_id,
            "workspace_root": args.workspace_root,
            "confirmed_launch_result_ref": args.confirmed_launch_result_ref,
            "confirmed_runtime_status_ref": args.confirmed_runtime_status_ref,
            "reason": args.reason,
            "self_attribution": args.self_attribution,
            "self_repair": args.self_repair,
            "observation_kind": args.observation_kind,
            "summary": args.summary,
            "evidence_refs": list(args.evidence_ref or []),
        }
    result = recover_orphaned_active_node(**payload)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_publish_external_result(args: argparse.Namespace) -> int:
    result = publish_external_from_implementer_result(result_ref=args.result_ref)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_supervise_child(args: argparse.Namespace) -> int:
    launch_result_ref = Path(args.launch_result_ref).expanduser().resolve()
    result = supervise_child_until_settled(
        launch_result_ref=launch_result_ref,
        poll_interval_s=args.poll_interval_s,
        stall_threshold_s=args.stall_threshold_s,
        max_recoveries=args.max_recoveries,
        max_wall_clock_s=args.max_wall_clock_s,
        no_substantive_progress_window_s=args.no_substantive_progress_window_s,
    )
    _, sync_error = persist_committed_supervision_result(
        launch_result_ref=launch_result_ref,
        result_payload=result,
    )
    if sync_error:
        print(f"[supervise-child] warning: state sync after settled supervision failed: {sync_error}", file=sys.stderr)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_launch_child(args: argparse.Namespace) -> int:
    result = launch_child_from_result_ref(
        result_ref=args.result_ref,
        startup_probe_ms=args.startup_probe_ms,
        startup_health_timeout_ms=args.startup_health_timeout_ms,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_child_runtime_status(args: argparse.Namespace) -> int:
    result = child_runtime_status_from_launch_result_ref(
        result_ref=args.result_ref,
        stall_threshold_s=args.stall_threshold_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_child_progress_snapshot(args: argparse.Namespace) -> int:
    result = child_progress_snapshot_from_launch_result_ref(
        result_ref=args.launch_result_ref,
        stall_threshold_s=args.stall_threshold_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "bootstrap-first-implementer":
        return _cmd_bootstrap_first_implementer(args)
    if args.command == "bootstrap-first-implementer-from-endpoint":
        return _cmd_bootstrap_first_implementer_from_endpoint(args)
    if args.command == "recover-orphaned-active":
        return _cmd_recover_orphaned_active(args)
    if args.command == "launch-child":
        return _cmd_launch_child(args)
    if args.command == "child-runtime-status":
        return _cmd_child_runtime_status(args)
    if args.command == "child-progress-snapshot":
        return _cmd_child_progress_snapshot(args)
    if args.command == "supervise-child":
        return _cmd_supervise_child(args)
    if args.command == "publish-external-result":
        return _cmd_publish_external_result(args)
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
