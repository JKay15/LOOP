"""Audit helpers for kernel-visible runtime events."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loop_product.kernel.state import load_kernel_state
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_paths import require_runtime_root

_HARD_RULE_SOURCES = [
    "docs/contracts/LOOP_SYSTEM_KERNEL_CONTRACT.md",
    "docs/contracts/LOOP_SYSTEM_NODE_CONTRACT.md",
    "docs/contracts/LOOP_SYSTEM_EVALUATOR_NODE_CONTRACT.md",
    "docs/contracts/LOOP_SYSTEM_CONTROL_ENVELOPE_CONTRACT.md",
    "docs/contracts/LOOP_SYSTEM_RUNTIME_RECOVERY_CONTRACT.md",
    "docs/contracts/LOOP_SYSTEM_STATE_QUERY_CONTRACT.md",
    "docs/contracts/LOOP_SYSTEM_AUDIT_EXPERIENCE_CONTRACT.md",
]


def record_audit_event(state_root: Path, event_type: str, payload: dict[str, Any]) -> Path:
    """Append a structured audit event under the runtime tree."""

    state_root = require_runtime_root(state_root)
    audit_dir = state_root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = audit_dir / f"{stamp}_{event_type}.json"
    record = {
        "event_type": event_type,
        "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "payload": dict(payload),
    }
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def query_recent_audit_events(state_root: Path, *, node_id: str | None = None, limit: int = 10) -> list[dict[str, Any]]:
    """Return recent audit records in reverse chronological order."""

    state_root = require_runtime_root(state_root)
    normalized_limit = max(int(limit), 0)
    if normalized_limit == 0:
        return []
    records: list[dict[str, Any]] = []
    audit_dir = state_root / "audit"
    if not audit_dir.exists():
        return records
    for path in sorted(audit_dir.glob("*.json"), key=_audit_sort_key, reverse=True):
        normalized = _normalize_audit_record(path)
        if node_id and normalized.get("node_id") != node_id:
            continue
        records.append(normalized)
        if len(records) >= normalized_limit:
            break
    return records


def query_audit_experience_view(state_root: Path, *, node_id: str | None = None, limit: int = 10) -> dict[str, Any]:
    """Return the IF-6 read-only audit and experience view."""

    state_root = require_runtime_root(state_root)
    kernel_state = load_kernel_state(state_root)
    recent_all = query_recent_audit_events(state_root, limit=max(int(limit), 1) * 5)
    recent_structural_decisions = [
        item
        for item in recent_all
        if item.get("classification") in {"topology", "local_control", "evaluator", "terminal"}
        or any(
            token in str(item.get("event_type") or "").lower()
            for token in ("split", "merge", "resume", "retry", "relaunch", "decision", "terminal", "evaluator")
        )
    ][: max(int(limit), 0)]
    recent_node_events = query_recent_audit_events(state_root, node_id=node_id, limit=limit)
    experience_assets = [
        dict(item)
        for item in kernel_state.experience_assets
        if not node_id or str(item.get("node_id") or "") == node_id
    ]
    view = {
        "recent_structural_decisions": recent_structural_decisions,
        "recent_node_events": recent_node_events,
        "effective_rule_sources": list(_HARD_RULE_SOURCES),
        "experience_assets": experience_assets,
        "storage_partitions": {
            "experience_assets": ".loop/state/kernel_state.json#experience_assets",
            "audit": ".loop/audit/",
            "cache": ".loop/cache/",
            "durable_state": ".loop/state/",
            "hard_rules": list(_HARD_RULE_SOURCES),
        },
    }
    validate_repo_object("LoopSystemAuditExperienceView.schema.json", view)
    return view


def _normalize_audit_record(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    path_str = str(path)
    if {"source", "envelope_type", "classification"} <= set(data):
        payload = dict(data.get("payload") or {})
        return {
            "event_type": str(data.get("envelope_type") or path.stem),
            "record_ref": path_str,
            "node_id": str(payload.get("node_id") or data.get("source") or ""),
            "summary": str(payload.get("summary") or data.get("note") or ""),
            "round_id": str(data.get("round_id") or ""),
            "classification": str(data.get("classification") or ""),
        }
    payload = dict(data.get("payload") or {})
    event_type = str(data.get("event_type") or _event_type_from_path(path))
    return {
        "event_type": event_type,
        "record_ref": path_str,
        "node_id": str(payload.get("node_id") or data.get("node_id") or ""),
        "summary": str(payload.get("summary") or data.get("summary") or ""),
        "round_id": str(payload.get("round_id") or data.get("round_id") or ""),
        "classification": "audit_event",
    }


def _event_type_from_path(path: Path) -> str:
    stem = path.stem
    if "_" not in stem:
        return stem
    return stem.split("_", 1)[1]


def _audit_sort_key(path: Path) -> str:
    stem = path.stem
    if stem.startswith("envelope_"):
        parts = stem.split("_", 2)
        if len(parts) >= 3:
            return parts[1]
    if "_" in stem:
        return stem.split("_", 1)[0]
    return stem
