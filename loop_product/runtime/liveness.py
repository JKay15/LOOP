"""Liveness helpers for active node visibility."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from loop_product.protocols.node import NodeSpec, NodeStatus, RuntimeAttachmentState, normalize_runtime_state

DEFAULT_ATTACHED_LEASE_DURATION_S = 60.0


def _parse_utc_timestamp(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _format_utc_timestamp(value: datetime | None) -> str:
    if value is None:
        return ""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _coerce_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0.0:
        return None
    return numeric


def runtime_attachment_state(node: NodeSpec | Mapping[str, Any]) -> RuntimeAttachmentState:
    payload = node.to_dict() if isinstance(node, NodeSpec) else dict(node)
    runtime_state = normalize_runtime_state(dict(payload.get("runtime_state") or {}))
    return RuntimeAttachmentState(str(runtime_state["attachment_state"]))


def build_runtime_loss_signal(
    *,
    observation_kind: str,
    summary: str,
    evidence_refs: list[str] | tuple[str, ...] = (),
    observed_at: str = "",
) -> dict[str, Any]:
    return normalize_runtime_state(
        {
            "attachment_state": RuntimeAttachmentState.LOST.value,
            "observed_at": observed_at,
            "summary": summary,
            "observation_kind": observation_kind,
            "evidence_refs": [str(item) for item in evidence_refs],
        }
    )


def is_live(node: NodeSpec) -> bool:
    return (
        node.status in {NodeStatus.PLANNED, NodeStatus.ACTIVE, NodeStatus.BLOCKED}
        and runtime_attachment_state(node) is not RuntimeAttachmentState.LOST
    )


def summarize_runtime_liveness_observation(
    payload: Mapping[str, Any],
    *,
    recorded_at: str,
    now_utc: str = "",
) -> dict[str, Any]:
    """Derive raw/effective liveness semantics from one committed observation."""

    normalized_payload = dict(payload or {})
    raw_attachment_state = str(normalized_payload.get("attachment_state") or "").strip().upper()
    if not raw_attachment_state:
        raw_attachment_state = RuntimeAttachmentState.UNOBSERVED.value
    observed_at = str(normalized_payload.get("observed_at") or recorded_at or "")
    observed_dt = _parse_utc_timestamp(observed_at) or _parse_utc_timestamp(recorded_at)
    now_dt = _parse_utc_timestamp(now_utc) or datetime.now(timezone.utc)
    lease_epoch = 0
    try:
        lease_epoch = int(normalized_payload.get("lease_epoch") or 0)
    except (TypeError, ValueError):
        lease_epoch = 0
    lease_owner_id = str(normalized_payload.get("lease_owner_id") or "")

    summary = {
        "raw_attachment_state": raw_attachment_state,
        "effective_attachment_state": raw_attachment_state,
        "lease_freshness": "not_applicable",
        "lease_source": "",
        "lease_duration_s": None,
        "lease_expires_at": "",
        "lease_epoch": lease_epoch,
        "lease_owner_id": lease_owner_id,
        "observed_at": observed_at,
    }

    if raw_attachment_state != RuntimeAttachmentState.ATTACHED.value:
        return summary

    explicit_expires_at = _parse_utc_timestamp(str(normalized_payload.get("lease_expires_at") or ""))
    explicit_duration_s = _coerce_float(normalized_payload.get("lease_duration_s"))
    if explicit_expires_at is not None:
        lease_expires_at = explicit_expires_at
        lease_duration_s = explicit_duration_s
        lease_source = "payload_explicit_expiry"
    elif explicit_duration_s is not None and observed_dt is not None:
        lease_expires_at = observed_dt + timedelta(seconds=explicit_duration_s)
        lease_duration_s = explicit_duration_s
        lease_source = "payload_duration"
    elif observed_dt is not None:
        lease_duration_s = DEFAULT_ATTACHED_LEASE_DURATION_S
        lease_expires_at = observed_dt + timedelta(seconds=lease_duration_s)
        lease_source = "legacy_default"
    else:
        summary["effective_attachment_state"] = RuntimeAttachmentState.UNOBSERVED.value
        summary["lease_freshness"] = "unknown"
        return summary

    summary["lease_source"] = lease_source
    summary["lease_duration_s"] = lease_duration_s
    summary["lease_expires_at"] = _format_utc_timestamp(lease_expires_at)
    if now_dt <= lease_expires_at:
        summary["lease_freshness"] = "fresh"
        summary["effective_attachment_state"] = RuntimeAttachmentState.ATTACHED.value
    else:
        summary["lease_freshness"] = "expired"
        summary["effective_attachment_state"] = RuntimeAttachmentState.UNOBSERVED.value
    return summary


def select_effective_runtime_liveness_head(
    observations: list[dict[str, Any]] | tuple[dict[str, Any], ...],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the raw latest node head plus the effective epoch-aware node head."""

    ordered = [dict(item or {}) for item in observations if dict(item or {})]
    if not ordered:
        return {}, {}

    raw_latest = dict(ordered[-1])
    raw_state = str(raw_latest.get("raw_attachment_state") or "").strip().upper()
    try:
        raw_latest_epoch = int(raw_latest.get("lease_epoch") or 0)
    except (TypeError, ValueError):
        raw_latest_epoch = 0

    dominant_attached_epoch: int | None = None
    dominant_attached_head: dict[str, Any] | None = None
    for summary in ordered:
        state = str(summary.get("raw_attachment_state") or "").strip().upper()
        if state != RuntimeAttachmentState.ATTACHED.value:
            continue
        try:
            epoch = int(summary.get("lease_epoch") or 0)
        except (TypeError, ValueError):
            epoch = 0
        if dominant_attached_epoch is None or epoch >= dominant_attached_epoch:
            dominant_attached_epoch = epoch
            dominant_attached_head = dict(summary)

    if (
        raw_state in {RuntimeAttachmentState.LOST.value, RuntimeAttachmentState.TERMINAL.value}
        and dominant_attached_epoch is not None
        and raw_latest_epoch > 0
        and raw_latest_epoch < dominant_attached_epoch
    ):
        stale_raw = dict(raw_latest)
        stale_raw["lease_freshness"] = "superseded_epoch"
        stale_raw["effective_attachment_state"] = RuntimeAttachmentState.UNOBSERVED.value
        stale_raw["superseded_by_lease_epoch"] = dominant_attached_epoch
        return stale_raw, dict(dominant_attached_head or stale_raw)

    if raw_state in {RuntimeAttachmentState.LOST.value, RuntimeAttachmentState.TERMINAL.value}:
        return raw_latest, raw_latest

    if (
        raw_state == RuntimeAttachmentState.ATTACHED.value
        and dominant_attached_epoch is not None
        and int(raw_latest.get("lease_epoch") or 0) < dominant_attached_epoch
    ):
        stale_raw = dict(raw_latest)
        stale_raw["lease_freshness"] = "superseded_epoch"
        stale_raw["effective_attachment_state"] = RuntimeAttachmentState.UNOBSERVED.value
        stale_raw["superseded_by_lease_epoch"] = dominant_attached_epoch
        return stale_raw, dict(dominant_attached_head or stale_raw)

    return raw_latest, raw_latest
