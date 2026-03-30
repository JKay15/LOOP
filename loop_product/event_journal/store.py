"""SQLite-backed committed event journal for the LOOP control plane."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
from typing import Any, Iterable

from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
from loop_product.protocols.topology import TopologyMutation, topology_target_node_ids
from loop_product.runtime_paths import require_runtime_root


EVENT_JOURNAL_EVENT_SCHEMA_VERSION = "0.1.0"
EVENT_JOURNAL_STORE_FORMAT_VERSION = "sqlite_wal_v1"
EVENT_JOURNAL_PRODUCER_GENERATION = "event_writer_generation_v1"
PROJECTION_AFFECTING_EVENT_TYPES = frozenset({"control_envelope_accepted"})
_EVENT_JOURNAL_INIT_RETRY_S = (0.05, 0.1, 0.2, 0.4, 0.8)


def event_journal_db_ref(state_root: Path) -> Path:
    state_root = require_runtime_root(state_root)
    return (state_root / "event_journal" / "control_plane.sqlite3").resolve()


def ensure_event_journal(state_root: Path) -> Path:
    runtime_root = require_runtime_root(state_root).resolve()
    db_ref = event_journal_db_ref(runtime_root)
    db_ref.parent.mkdir(parents=True, exist_ok=True)
    last_exc: sqlite3.OperationalError | None = None
    retry_count = len(_EVENT_JOURNAL_INIT_RETRY_S)
    for attempt in range(retry_count + 1):
        db_preexisting = db_ref.exists()
        try:
            with _connect(db_ref) as conn:
                if not db_preexisting:
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS committed_events (
                        seq INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL UNIQUE,
                        command_id TEXT NOT NULL,
                        runtime_root TEXT NOT NULL,
                        node_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        recorded_at TEXT NOT NULL,
                        producer TEXT NOT NULL,
                        schema_version TEXT NOT NULL,
                        compatibility_floor TEXT,
                        payload_json TEXT NOT NULL,
                        causal_refs_json TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS journal_meta (
                        key TEXT PRIMARY KEY,
                        value_json TEXT NOT NULL
                    )
                    """
                )
                _ensure_committed_events_producer_generation_column(conn)
                _ensure_writer_fence_table(
                    conn,
                    required_producer_generation=EVENT_JOURNAL_PRODUCER_GENERATION,
                )
                _ensure_writer_fence_trigger(conn)
                initialized_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                _insert_meta_if_missing(conn, key="runtime_root", value=str(runtime_root))
                _insert_meta_if_missing(conn, key="event_schema_version", value=EVENT_JOURNAL_EVENT_SCHEMA_VERSION)
                _insert_meta_if_missing(conn, key="store_format_version", value=EVENT_JOURNAL_STORE_FORMAT_VERSION)
                _insert_meta_if_missing(
                    conn,
                    key="required_producer_generation",
                    value=_load_required_producer_generation(conn),
                )
                _insert_meta_if_missing(conn, key="initialized_at", value=initialized_at)
                _validate_meta_payload(
                    _load_meta_payload(conn),
                    expected_runtime_root=str(runtime_root),
                    required_producer_generation=_load_required_producer_generation(conn),
                )
            return db_ref
        except sqlite3.OperationalError as exc:
            if not _is_sqlite_lock_error(exc):
                raise
            last_exc = exc
            if attempt >= retry_count:
                break
            time.sleep(_EVENT_JOURNAL_INIT_RETRY_S[attempt])
    if last_exc is not None:
        raise last_exc
    return db_ref


def load_event_journal_meta(state_root: Path) -> dict[str, Any]:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        payload = dict(_load_meta_payload(conn))
        journal_meta_required_generation = str(payload.get("required_producer_generation") or "").strip()
        required_generation = _load_required_producer_generation(conn)
        payload["journal_meta_required_producer_generation"] = journal_meta_required_generation
        payload["required_producer_generation"] = required_generation
        payload["writer_fence_required_producer_generation"] = required_generation
    payload["db_ref"] = str(db_ref)
    return payload


def describe_event_journal_compatibility(state_root: Path, *, initialize_if_missing: bool = True) -> dict[str, Any]:
    runtime_root = require_runtime_root(state_root).resolve()
    db_ref = event_journal_db_ref(runtime_root)
    if not db_ref.exists():
        if initialize_if_missing:
            ensure_event_journal(runtime_root)
        else:
            return {
                "runtime_root": str(runtime_root),
                "db_ref": str(db_ref),
                "db_exists": False,
                "compatibility_status": "missing",
                "supported_event_schema_version": EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
                "supported_store_format_version": EVENT_JOURNAL_STORE_FORMAT_VERSION,
                "supported_producer_generation": EVENT_JOURNAL_PRODUCER_GENERATION,
                "required_producer_generation": EVENT_JOURNAL_PRODUCER_GENERATION,
                "journal_meta_required_producer_generation": "",
                "writer_fencing_status": "missing",
                "writer_fence_alignment_status": "missing",
                "journal_meta": {},
                "compatibility_issues": [
                    {
                        "field": "db_ref",
                        "reason": "missing_event_journal_db",
                        "expected": str(db_ref),
                        "observed": "",
                    }
                ],
            }

    journal_meta: dict[str, Any] = {}
    required_producer_generation = ""
    journal_meta_required_producer_generation = ""
    issues: list[dict[str, Any]] = []
    try:
        with _connect(db_ref) as conn:
            journal_meta = _load_meta_payload(conn)
            required_producer_generation = _load_required_producer_generation(conn)
    except sqlite3.DatabaseError as exc:
        issues.append(
            {
                "field": "db_ref",
                "reason": "sqlite_open_failed",
                "expected": str(db_ref),
                "observed": str(exc),
            }
        )
    else:
        expected_runtime_root = str(runtime_root)
        observed_runtime_root = str(journal_meta.get("runtime_root") or "").strip()
        if observed_runtime_root and observed_runtime_root != expected_runtime_root:
            issues.append(
                {
                    "field": "runtime_root",
                    "reason": "runtime_root_mismatch",
                    "expected": expected_runtime_root,
                    "observed": observed_runtime_root,
                }
            )
        observed_event_schema = str(journal_meta.get("event_schema_version") or "").strip()
        if observed_event_schema and observed_event_schema != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
            issues.append(
                {
                    "field": "event_schema_version",
                    "reason": "unsupported_event_schema_version",
                    "expected": EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
                    "observed": observed_event_schema,
                }
            )
        observed_store_format = str(journal_meta.get("store_format_version") or "").strip()
        if observed_store_format and observed_store_format != EVENT_JOURNAL_STORE_FORMAT_VERSION:
            issues.append(
                {
                    "field": "store_format_version",
                    "reason": "unsupported_store_format_version",
                    "expected": EVENT_JOURNAL_STORE_FORMAT_VERSION,
                    "observed": observed_store_format,
                }
            )
        journal_meta_required_producer_generation = str(
            journal_meta.get("required_producer_generation") or ""
        ).strip()
        if not required_producer_generation:
            issues.append(
                {
                    "field": "required_producer_generation",
                    "reason": "missing_required_producer_generation",
                    "expected": EVENT_JOURNAL_PRODUCER_GENERATION,
                    "observed": required_producer_generation,
                }
            )
        elif required_producer_generation != EVENT_JOURNAL_PRODUCER_GENERATION:
            issues.append(
                {
                    "field": "required_producer_generation",
                    "reason": "unsupported_required_producer_generation",
                    "expected": EVENT_JOURNAL_PRODUCER_GENERATION,
                    "observed": required_producer_generation,
                }
            )
        if journal_meta_required_producer_generation != required_producer_generation:
            issues.append(
                {
                    "field": "journal_meta.required_producer_generation",
                    "reason": "required_producer_generation_meta_mismatch",
                    "expected": required_producer_generation,
                    "observed": journal_meta_required_producer_generation,
                }
            )

    if issues and not journal_meta:
        writer_fencing_status = "incompatible"
        writer_fence_alignment_status = "unknown"
    elif not required_producer_generation:
        writer_fencing_status = "disabled"
        writer_fence_alignment_status = "disabled"
    elif journal_meta_required_producer_generation != required_producer_generation:
        writer_fencing_status = "misaligned"
        writer_fence_alignment_status = "meta_mismatch"
    elif not issues and required_producer_generation == EVENT_JOURNAL_PRODUCER_GENERATION:
        writer_fencing_status = "enforced"
        writer_fence_alignment_status = "aligned"
    else:
        writer_fencing_status = "incompatible"
        writer_fence_alignment_status = "aligned"

    return {
        "runtime_root": str(runtime_root),
        "db_ref": str(db_ref),
        "db_exists": db_ref.exists(),
        "compatibility_status": "compatible" if not issues else "incompatible",
        "supported_event_schema_version": EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
        "supported_store_format_version": EVENT_JOURNAL_STORE_FORMAT_VERSION,
        "supported_producer_generation": EVENT_JOURNAL_PRODUCER_GENERATION,
        "required_producer_generation": required_producer_generation,
        "journal_meta_required_producer_generation": journal_meta_required_producer_generation,
        "writer_fencing_status": writer_fencing_status,
        "writer_fence_alignment_status": writer_fence_alignment_status,
        "journal_meta": (
            {
                **journal_meta,
                "journal_meta_required_producer_generation": journal_meta_required_producer_generation,
                "required_producer_generation": required_producer_generation,
                "writer_fence_required_producer_generation": required_producer_generation,
                "db_ref": str(db_ref),
            }
            if journal_meta
            else {
                "journal_meta_required_producer_generation": journal_meta_required_producer_generation,
                "required_producer_generation": required_producer_generation,
                "writer_fence_required_producer_generation": required_producer_generation,
                "db_ref": str(db_ref),
            }
        ),
        "compatibility_issues": issues,
    }


def commit_control_plane_event(
    state_root: Path,
    *,
    event_id: str,
    command_id: str,
    node_id: str,
    event_type: str,
    payload: dict[str, Any],
    causal_refs: Iterable[str] = (),
    producer: str,
    producer_generation: str = EVENT_JOURNAL_PRODUCER_GENERATION,
    schema_version: str = EVENT_JOURNAL_EVENT_SCHEMA_VERSION,
    compatibility_floor: str = "",
) -> dict[str, Any]:
    """Atomically commit one control-plane event or replay the existing committed event."""

    event_id = str(event_id or "").strip()
    command_id = str(command_id or "").strip()
    event_type = str(event_type or "").strip()
    producer = str(producer or "").strip()
    producer_generation = str(producer_generation or "").strip()
    node_id = str(node_id or "").strip()
    schema_version = str(schema_version or "").strip()
    compatibility_floor = str(compatibility_floor or "").strip()
    if not event_id:
        raise ValueError("event_id is required")
    if not command_id:
        raise ValueError("command_id is required")
    if not event_type:
        raise ValueError("event_type is required")
    if not producer:
        raise ValueError("producer is required")
    if not producer_generation:
        raise ValueError("producer_generation is required")
    if not schema_version:
        raise ValueError("schema_version is required")
    _require_supported_event_schema_version(schema_version, context="commit_control_plane_event")
    _require_supported_producer_generation(producer_generation, context="commit_control_plane_event")

    runtime_root = str(require_runtime_root(state_root).resolve())
    payload_json = _canonical_json(payload)
    causal_refs_json = _canonical_json([str(item or "").strip() for item in causal_refs if str(item or "").strip()])
    recorded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        conn.execute("BEGIN IMMEDIATE")
        existing = conn.execute(
            """
            SELECT seq, event_id, command_id, runtime_root, node_id, event_type, recorded_at, producer,
                   producer_generation, schema_version, compatibility_floor, payload_json, causal_refs_json
            FROM committed_events
            WHERE event_id = ?
            """,
            (event_id,),
        ).fetchone()
        if existing is not None:
            row = _row_to_event(existing)
            _assert_replay_compatible(
                existing=row,
                command_id=command_id,
                runtime_root=runtime_root,
                node_id=node_id,
                event_type=event_type,
                producer=producer,
                producer_generation=producer_generation,
                schema_version=schema_version,
                compatibility_floor=compatibility_floor,
                payload_json=payload_json,
                causal_refs_json=causal_refs_json,
            )
            conn.commit()
            return row

        cursor = conn.execute(
            """
            INSERT INTO committed_events (
                event_id, command_id, runtime_root, node_id, event_type, recorded_at, producer,
                producer_generation, schema_version, compatibility_floor, payload_json, causal_refs_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                command_id,
                runtime_root,
                node_id,
                event_type,
                recorded_at,
                producer,
                producer_generation,
                schema_version,
                compatibility_floor,
                payload_json,
                causal_refs_json,
            ),
        )
        seq = int(cursor.lastrowid)
        conn.commit()
    return {
        "seq": seq,
        "event_id": event_id,
        "command_id": command_id,
        "runtime_root": runtime_root,
        "node_id": node_id,
        "event_type": event_type,
        "recorded_at": recorded_at,
        "producer": producer,
        "producer_generation": producer_generation,
        "schema_version": schema_version,
        "compatibility_floor": compatibility_floor,
        "payload": json.loads(payload_json),
        "causal_refs": json.loads(causal_refs_json),
    }


def mirror_accepted_control_envelope_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Mirror one accepted control envelope into the event journal foundation."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope if str(envelope.envelope_id or "").strip() else envelope.with_identity()
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted control envelope must have an envelope_id")
    event_id = f"evt_{hashlib.sha256(f'accepted_control_envelope:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"accepted_control_envelope:{envelope_id}",
        node_id=str(normalized.source or "").strip(),
        event_type="control_envelope_accepted",
        payload=normalized.to_dict(),
        causal_refs=[f"control_envelope:{envelope_id}"],
        producer="kernel.submit.compatibility_mirror",
        compatibility_floor="milestone1_compatibility_mirror_v1",
    )


def commit_accepted_audit_mirror_repaired_event(
    state_root: Path,
    *,
    receipt: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one canonical accepted-audit mirror repair result fact."""

    receipt_payload = dict(receipt or {})
    repair_kind = str(receipt_payload.get("repair_kind") or "").strip().lower()
    if repair_kind != "accepted_audit_mirror_repair":
        raise ValueError("accepted-audit mirror repair receipt must carry repair_kind=accepted_audit_mirror_repair")
    runtime_root = str(receipt_payload.get("runtime_root") or require_runtime_root(state_root))
    status = str(receipt_payload.get("status") or "").strip().lower()
    accepted_only_before = int(receipt_payload.get("accepted_only_count_before") or 0)
    accepted_only_after = int(receipt_payload.get("accepted_only_count_after") or 0)
    mirrored_envelope_count = int(receipt_payload.get("mirrored_envelope_count") or 0)
    duplicate_envelope_id_count = int(receipt_payload.get("duplicate_envelope_id_count") or 0)
    rebuild_status = str(receipt_payload.get("rebuild_status") or "").strip().lower()
    repair_key = {
        "repair_kind": repair_kind,
        "runtime_root": runtime_root,
        "status": status,
        "accepted_only_count_before": accepted_only_before,
        "accepted_only_count_after": accepted_only_after,
        "mirrored_envelope_count": mirrored_envelope_count,
        "duplicate_envelope_id_count": duplicate_envelope_id_count,
        "rebuild_status": rebuild_status,
        "journal_committed_seq_after": int(receipt_payload.get("journal_committed_seq_after") or 0),
        "repair_result_event_seq_before": int(receipt_payload.get("repair_result_event_seq_before") or 0),
    }
    event_id = f"evt_{hashlib.sha256(_canonical_json(repair_key).encode('utf-8')).hexdigest()[:16]}"
    rebuild_result_event_id = str(receipt_payload.get("rebuild_result_event_id") or "").strip()
    causal_refs = []
    if rebuild_result_event_id:
        causal_refs.append(f"committed_event:{rebuild_result_event_id}")
    for envelope_id in list(receipt_payload.get("mirrored_envelope_ids_sample") or []):
        normalized_envelope_id = str(envelope_id or "").strip()
        if normalized_envelope_id:
            causal_refs.append(f"control_envelope:{normalized_envelope_id}")
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=(
            f"accepted_audit_mirror_repair:{status}:{accepted_only_before}:{accepted_only_after}:{mirrored_envelope_count}"
        ),
        node_id="",
        event_type="accepted_audit_mirror_repaired",
        payload={
            "repair_kind": repair_kind,
            "status": status,
            "runtime_root": runtime_root,
            "checkpoint_ref": str(receipt_payload.get("checkpoint_ref") or ""),
            "accepted_only_count_before": accepted_only_before,
            "accepted_only_count_after": accepted_only_after,
            "accepted_only_gap_cleared": bool(receipt_payload.get("accepted_only_gap_cleared")),
            "accepted_audit_unique_envelope_count": int(
                receipt_payload.get("accepted_audit_unique_envelope_count") or 0
            ),
            "already_mirrored_count": int(receipt_payload.get("already_mirrored_count") or 0),
            "mirrored_envelope_count": mirrored_envelope_count,
            "mirrored_envelope_ids_sample": [
                str(item) for item in list(receipt_payload.get("mirrored_envelope_ids_sample") or [])
            ],
            "duplicate_envelope_id_count": duplicate_envelope_id_count,
            "duplicate_envelope_ids_sample": [
                str(item) for item in list(receipt_payload.get("duplicate_envelope_ids_sample") or [])
            ],
            "commit_phase_before": str(receipt_payload.get("commit_phase_before") or ""),
            "commit_phase_after": str(receipt_payload.get("commit_phase_after") or ""),
            "crash_matrix_state_before": str(receipt_payload.get("crash_matrix_state_before") or ""),
            "crash_matrix_state_after": str(receipt_payload.get("crash_matrix_state_after") or ""),
            "journal_committed_seq_before": int(receipt_payload.get("journal_committed_seq_before") or 0),
            "journal_committed_seq_after": int(receipt_payload.get("journal_committed_seq_after") or 0),
            "rebuild_status": rebuild_status,
            "rebuild_projection_state": str(receipt_payload.get("rebuild_projection_state") or ""),
            "rebuild_visibility_state": str(receipt_payload.get("rebuild_visibility_state") or ""),
            "rebuild_blocking_reason": str(receipt_payload.get("rebuild_blocking_reason") or ""),
            "rebuild_result_event_id": rebuild_result_event_id,
            "continuation_readiness_after": str(receipt_payload.get("continuation_readiness_after") or ""),
            "action_safety_readiness_after": str(receipt_payload.get("action_safety_readiness_after") or ""),
            "action_safety_blockers_after": [
                str(item) for item in list(receipt_payload.get("action_safety_blockers_after") or [])
            ],
        },
        causal_refs=causal_refs,
        producer=producer,
        compatibility_floor="milestone6_real_root_accepted_audit_mirror_repair_v1",
    )


def commit_repo_service_cleanup_retirement_repaired_event(
    state_root: Path,
    *,
    receipt: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one canonical repo-service cleanup-retirement repair result fact."""

    receipt_payload = dict(receipt or {})
    repair_kind = str(receipt_payload.get("repair_kind") or "").strip().lower()
    if repair_kind != "repo_service_cleanup_retirement_repair":
        raise ValueError(
            "repo-service cleanup-retirement repair receipt must carry repair_kind=repo_service_cleanup_retirement_repair"
        )
    repo_root = str(receipt_payload.get("repo_root") or "").strip()
    status = str(receipt_payload.get("status") or "").strip().lower()
    blocked_before = int(receipt_payload.get("blocked_service_count_before") or 0)
    reseeded_count = int(receipt_payload.get("reseeded_service_count") or 0)
    blocked_after = int(receipt_payload.get("blocked_service_count_after") or 0)
    repair_key = {
        "repair_kind": repair_kind,
        "repo_root": repo_root,
        "status": status,
        "blocked_service_count_before": blocked_before,
        "reseeded_service_count": reseeded_count,
        "blocked_service_count_after": blocked_after,
        "repair_result_event_seq_before": int(receipt_payload.get("repair_result_event_seq_before") or 0),
    }
    event_id = f"evt_{hashlib.sha256(_canonical_json(repair_key).encode('utf-8')).hexdigest()[:16]}"
    causal_refs = []
    for event_id_value in list(receipt_payload.get("cleanup_authority_event_ids") or []):
        normalized_event_id = str(event_id_value or "").strip()
        if normalized_event_id:
            causal_refs.append(f"committed_event:{normalized_event_id}")
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=(
            f"repo_service_cleanup_retirement_repair:{status}:{blocked_before}:{reseeded_count}:{blocked_after}"
        ),
        node_id="",
        event_type="repo_service_cleanup_retirement_repaired",
        payload={
            "repair_kind": repair_kind,
            "status": status,
            "repo_root": repo_root,
            "blocked_service_count_before": blocked_before,
            "blocked_service_count_after": blocked_after,
            "blocked_service_kinds_before": [
                str(item) for item in list(receipt_payload.get("blocked_service_kinds_before") or [])
            ],
            "reseeded_service_count": reseeded_count,
            "reseeded_service_kinds": [
                str(item) for item in list(receipt_payload.get("reseeded_service_kinds") or [])
            ],
            "skipped_live_service_kinds": [
                str(item) for item in list(receipt_payload.get("skipped_live_service_kinds") or [])
            ],
            "still_blocked_service_kinds_after": [
                str(item) for item in list(receipt_payload.get("still_blocked_service_kinds_after") or [])
            ],
            "cleanup_authority_event_ids": [
                str(item) for item in list(receipt_payload.get("cleanup_authority_event_ids") or [])
            ],
            "runtime_identity_reseeded": bool(receipt_payload.get("runtime_identity_reseeded")),
            "repair_gap_cleared": bool(receipt_payload.get("repair_gap_cleared")),
        },
        causal_refs=causal_refs,
        producer=producer,
        compatibility_floor="milestone6_repo_service_cleanup_retirement_repair_v1",
    )


def commit_projection_adoption_repaired_event(
    state_root: Path,
    *,
    receipt: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one canonical historical projection-adoption repair result fact."""

    receipt_payload = dict(receipt or {})
    repair_kind = str(receipt_payload.get("repair_kind") or "").strip().lower()
    if repair_kind != "projection_adoption_repair":
        raise ValueError("projection adoption repair receipt must carry repair_kind=projection_adoption_repair")
    runtime_root = str(receipt_payload.get("runtime_root") or require_runtime_root(state_root))
    status = str(receipt_payload.get("status") or "").strip().lower()
    missing_before = int(receipt_payload.get("missing_accepted_envelope_count_before") or 0)
    missing_after = int(receipt_payload.get("missing_accepted_envelope_count_after") or 0)
    adopted_count = int(receipt_payload.get("adopted_envelope_count") or 0)
    repair_key = {
        "repair_kind": repair_kind,
        "runtime_root": runtime_root,
        "status": status,
        "missing_accepted_envelope_count_before": missing_before,
        "missing_accepted_envelope_count_after": missing_after,
        "adopted_envelope_count": adopted_count,
        "projection_committed_seq_after": int(receipt_payload.get("projection_committed_seq_after") or 0),
        "repair_result_event_seq_before": int(receipt_payload.get("repair_result_event_seq_before") or 0),
    }
    event_id = f"evt_{hashlib.sha256(_canonical_json(repair_key).encode('utf-8')).hexdigest()[:16]}"
    causal_refs = []
    accepted_audit_repair_event_id = str(receipt_payload.get("accepted_audit_mirror_repair_event_id") or "").strip()
    if accepted_audit_repair_event_id:
        causal_refs.append(f"committed_event:{accepted_audit_repair_event_id}")
    for envelope_id in list(receipt_payload.get("adopted_envelope_ids_sample") or []):
        normalized_envelope_id = str(envelope_id or "").strip()
        if normalized_envelope_id:
            causal_refs.append(f"control_envelope:{normalized_envelope_id}")
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=(
            f"projection_adoption_repair:{status}:{missing_before}:{missing_after}:{adopted_count}"
        ),
        node_id="",
        event_type="projection_adoption_repaired",
        payload={
            "repair_kind": repair_kind,
            "status": status,
            "runtime_root": runtime_root,
            "checkpoint_ref": str(receipt_payload.get("checkpoint_ref") or ""),
            "missing_accepted_envelope_count_before": missing_before,
            "missing_accepted_envelope_count_after": missing_after,
            "adopted_envelope_count": adopted_count,
            "adopted_envelope_ids_sample": [
                str(item) for item in list(receipt_payload.get("adopted_envelope_ids_sample") or [])
            ],
            "projection_gap_cleared": bool(receipt_payload.get("projection_gap_cleared")),
            "commit_phase_before": str(receipt_payload.get("commit_phase_before") or ""),
            "commit_phase_after": str(receipt_payload.get("commit_phase_after") or ""),
            "crash_matrix_state_before": str(receipt_payload.get("crash_matrix_state_before") or ""),
            "crash_matrix_state_after": str(receipt_payload.get("crash_matrix_state_after") or ""),
            "journal_committed_seq_before": int(receipt_payload.get("journal_committed_seq_before") or 0),
            "journal_committed_seq_after": int(receipt_payload.get("journal_committed_seq_after") or 0),
            "projection_committed_seq_before": int(receipt_payload.get("projection_committed_seq_before") or 0),
            "projection_committed_seq_after": int(receipt_payload.get("projection_committed_seq_after") or 0),
            "projection_visibility_state_after": str(receipt_payload.get("projection_visibility_state_after") or ""),
            "replay_mode_after": str(receipt_payload.get("replay_mode_after") or ""),
            "continuation_readiness_after": str(receipt_payload.get("continuation_readiness_after") or ""),
            "action_safety_readiness_after": str(receipt_payload.get("action_safety_readiness_after") or ""),
            "action_safety_blockers_after": [
                str(item) for item in list(receipt_payload.get("action_safety_blockers_after") or [])
            ],
            "accepted_audit_mirror_repair_event_id": accepted_audit_repair_event_id,
        },
        causal_refs=causal_refs,
        producer=producer,
        compatibility_floor="milestone6_real_root_projection_adoption_repair_v1",
    )


def commit_topology_command_accepted_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted-topology command fact for Milestone 4."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "topology":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted topology command must have an envelope_id")
    payload = dict(normalized.payload or {})
    mutation_payload = payload.get("topology_mutation") or payload
    mutation = TopologyMutation.from_dict(dict(mutation_payload or {}))
    review = dict(payload.get("review") or {})
    target_node_ids = topology_target_node_ids(mutation, review=review)
    event_id = f"evt_{hashlib.sha256(f'topology_command:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"topology_command:{envelope_id}",
        node_id=str(mutation.source_node_id or normalized.source or "").strip(),
        event_type="topology_command_accepted",
        payload={
            "envelope_id": envelope_id,
            "classification": "topology",
            "command_kind": str(mutation.kind or ""),
            "source_node_id": str(mutation.source_node_id or ""),
            "target_node_ids": target_node_ids,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "review_decision": str(review.get("decision") or ""),
            "review_summary": str(review.get("summary") or ""),
            "topology_mutation": mutation.to_dict(),
            "review": review,
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"topology_mutation:{str(mutation.kind or '')}:{str(mutation.source_node_id or '')}",
        ],
        producer="kernel.submit.topology_command",
        compatibility_floor="milestone4_topology_command_event_v1",
    )


def commit_terminal_result_recorded_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted-terminal-result fact for Milestone 4."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "terminal":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted terminal result must have an envelope_id")
    payload = dict(normalized.payload or {})
    node_id = str(payload.get("node_id") or normalized.source or "").strip()
    event_id = f"evt_{hashlib.sha256(f'terminal_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"terminal_result:{envelope_id}",
        node_id=node_id,
        event_type="terminal_result_recorded",
        payload={
            "envelope_id": envelope_id,
            "classification": "terminal",
            "node_id": node_id,
            "outcome": str(payload.get("outcome") or ""),
            "node_status": str(payload.get("node_status") or ""),
            "round_id": str(normalized.round_id or payload.get("round_id") or ""),
            "summary": str(payload.get("summary") or normalized.note or ""),
            "evidence_refs": [str(item) for item in list(payload.get("evidence_refs") or [])],
            "self_attribution": str(payload.get("self_attribution") or ""),
            "self_repair": str(payload.get("self_repair") or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"terminal_result:{node_id}",
        ],
        producer="kernel.submit.terminal_result",
        compatibility_floor="milestone4_terminal_result_event_v1",
    )


def commit_evaluator_result_recorded_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted-evaluator-result fact for Milestone 4."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "evaluator":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted evaluator result must have an envelope_id")
    payload = dict(normalized.payload or {})
    diagnostics = dict(payload.get("diagnostics") or {})
    node_id = str(normalized.source or "").strip()
    lane = str(payload.get("lane") or "unknown")
    verdict = str(payload.get("verdict") or "UNKNOWN")
    event_id = f"evt_{hashlib.sha256(f'evaluator_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"evaluator_result:{envelope_id}",
        node_id=node_id,
        event_type="evaluator_result_recorded",
        payload={
            "envelope_id": envelope_id,
            "classification": "evaluator",
            "node_id": node_id,
            "lane": lane,
            "verdict": verdict,
            "retryable": bool(payload.get("retryable")),
            "round_id": str(normalized.round_id or payload.get("round_id") or ""),
            "summary": str(payload.get("summary") or normalized.note or ""),
            "evidence_refs": [str(item) for item in list(payload.get("evidence_refs") or [])],
            "self_attribution": str(diagnostics.get("self_attribution") or ""),
            "self_repair": str(diagnostics.get("self_repair") or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"evaluator_result:{node_id}:{lane}",
        ],
        producer="kernel.submit.evaluator_result",
        compatibility_floor="milestone4_evaluator_result_event_v1",
    )


def commit_repo_tree_cleanup_requested_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted repo-tree cleanup fact for Milestone 4."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "repo_tree_cleanup":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted repo-tree cleanup command must have an envelope_id")
    payload = dict(normalized.payload or {})
    tree_root = str(payload.get("tree_root") or "").strip()
    repo_roots = [str(item) for item in list(payload.get("repo_roots") or [])]
    include_anchor_repo_root = bool(payload.get("include_anchor_repo_root"))
    cleanup_kind = str(payload.get("cleanup_kind") or "batch_repo_tree_cleanup")
    event_id = f"evt_{hashlib.sha256(f'repo_tree_cleanup:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"repo_tree_cleanup:{envelope_id}",
        node_id=str(normalized.source or "").strip(),
        event_type="repo_tree_cleanup_requested",
        payload={
            "envelope_id": envelope_id,
            "classification": "repo_tree_cleanup",
            "tree_root": tree_root,
            "repo_roots": repo_roots,
            "repo_count": len(repo_roots),
            "include_anchor_repo_root": include_anchor_repo_root,
            "cleanup_kind": cleanup_kind,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"repo_tree:{tree_root}",
        ],
        producer="kernel.submit.repo_tree_cleanup",
        compatibility_floor="milestone4_repo_tree_cleanup_event_v1",
    )


def commit_heavy_object_authority_gap_repo_remediation_requested_event(
    state_root: Path,
    envelope: ControlEnvelope,
) -> dict[str, Any] | None:
    """Commit one canonical accepted repo-root heavy-object remediation fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_authority_gap_repo_remediation_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object repo remediation command must have an envelope_id")
    payload = dict(normalized.payload or {})
    repo_root = str(payload.get("repo_root") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    runtime_name = str(payload.get("runtime_name") or "").strip()
    remediation_kind = str(payload.get("remediation_kind") or "repo_root_authority_gap_remediation").strip()
    remediation_signature = str(payload.get("remediation_signature") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    requirement_event_id = str(payload.get("requirement_event_id") or "").strip()
    audit_result_event_id = str(payload.get("audit_result_event_id") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_authority_gap_repo_remediation:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_authority_gap_repo_remediation:{envelope_id}",
        node_id=str(normalized.source or "").strip(),
        event_type="heavy_object_authority_gap_repo_remediation_requested",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "repo_root": repo_root,
            "object_kind": object_kind,
            "runtime_name": runtime_name,
            "remediation_kind": remediation_kind,
            "remediation_signature": remediation_signature,
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "requirement_event_id": requirement_event_id,
            "audit_result_event_id": audit_result_event_id,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"repo_root:{repo_root}",
        ],
        producer="kernel.submit.heavy_object_authority_gap_repo_remediation",
        compatibility_floor="milestone5_heavy_object_authority_gap_repo_remediation_request_event_v1",
    )


def commit_heavy_object_authority_gap_repo_remediation_settled_event(
    state_root: Path,
    *,
    envelope: ControlEnvelope,
    receipt: dict[str, Any],
) -> dict[str, Any] | None:
    """Commit one canonical authoritative repo-root heavy-object remediation batch result fact."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_authority_gap_repo_remediation_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object repo remediation command must have an envelope_id")
    receipt_payload = dict(receipt or {})
    payload = dict(normalized.payload or {})
    repo_root = str(receipt_payload.get("repo_root") or payload.get("repo_root") or "").strip()
    object_kind = str(receipt_payload.get("object_kind") or payload.get("object_kind") or "").strip()
    runtime_name = str(receipt_payload.get("runtime_name") or payload.get("runtime_name") or "").strip()
    remediation_kind = str(
        receipt_payload.get("remediation_kind")
        or payload.get("remediation_kind")
        or "repo_root_authority_gap_remediation"
    ).strip()
    remediation_signature = str(
        receipt_payload.get("remediation_signature")
        or payload.get("remediation_signature")
        or ""
    ).strip()
    trigger_kind = str(receipt_payload.get("trigger_kind") or payload.get("trigger_kind") or "").strip()
    trigger_ref = str(receipt_payload.get("trigger_ref") or payload.get("trigger_ref") or "").strip()
    requirement_event_id = str(
        receipt_payload.get("requirement_event_id")
        or payload.get("requirement_event_id")
        or ""
    ).strip()
    audit_result_event_id = str(
        receipt_payload.get("audit_result_event_id")
        or payload.get("audit_result_event_id")
        or ""
    ).strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_authority_gap_repo_remediation_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_authority_gap_repo_remediation_result:{envelope_id}",
        node_id=str(normalized.source or "").strip(),
        event_type="heavy_object_authority_gap_repo_remediation_settled",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "repo_root": repo_root,
            "object_kind": object_kind,
            "runtime_name": runtime_name,
            "remediation_kind": remediation_kind,
            "remediation_signature": remediation_signature,
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "requirement_event_id": requirement_event_id,
            "audit_result_event_id": audit_result_event_id,
            "candidate_count": int(receipt_payload.get("candidate_count") or 0),
            "requested_candidate_count": int(receipt_payload.get("requested_candidate_count") or 0),
            "skipped_candidate_count": int(receipt_payload.get("skipped_candidate_count") or 0),
            "requested_candidates": [dict(item) for item in list(receipt_payload.get("requested_candidates") or [])],
            "skipped_candidates": [dict(item) for item in list(receipt_payload.get("skipped_candidates") or [])],
            "fully_managed_after": bool(receipt_payload.get("fully_managed_after")),
            "unrequested_unmanaged_candidate_count_after": int(
                receipt_payload.get("unrequested_unmanaged_candidate_count_after") or 0
            ),
            "pending_remediation_candidate_count_after": int(
                receipt_payload.get("pending_remediation_candidate_count_after") or 0
            ),
            "remediated_candidate_count_after": int(receipt_payload.get("remediated_candidate_count_after") or 0),
            "failure_reasons": [str(item) for item in list(receipt_payload.get("failure_reasons") or [])],
            "receipt_ref": str(receipt_payload.get("receipt_ref") or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"repo_root:{repo_root}",
            f"heavy_object_authority_gap_repo_remediation_receipt:{envelope_id}",
        ],
        producer="runtime.control_plane.heavy_object_authority_gap_repo_remediation",
        compatibility_floor="milestone5_heavy_object_authority_gap_repo_remediation_result_event_v1",
    )


def commit_heavy_object_authority_gap_remediation_settled_event(
    state_root: Path,
    *,
    envelope: ControlEnvelope,
    receipt: dict[str, Any],
) -> dict[str, Any] | None:
    """Commit one canonical authoritative repo-local heavy-object remediation result fact."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_discovery_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object remediation command must have an envelope_id")
    receipt_payload = dict(receipt or {})
    payload = dict(normalized.payload or {})
    discovery_kind = str(
        receipt_payload.get("discovery_kind")
        or payload.get("discovery_kind")
        or ""
    ).strip()
    if discovery_kind != "authority_gap_remediation":
        return None
    object_id = str(receipt_payload.get("object_id") or payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object remediation result must preserve object_id")
    object_kind = str(receipt_payload.get("object_kind") or payload.get("object_kind") or "").strip()
    object_ref = str(receipt_payload.get("object_ref") or payload.get("object_ref") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_authority_gap_remediation_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_authority_gap_remediation_result:{envelope_id}",
        node_id=str(receipt_payload.get("owner_node_id") or payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_authority_gap_remediation_settled",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "object_refs": [str(item) for item in list(receipt_payload.get("discovered_refs") or [])],
            "duplicate_count": int(receipt_payload.get("duplicate_count") or 0),
            "total_bytes": int(receipt_payload.get("total_bytes") or 0),
            "discovery_kind": discovery_kind,
            "owner_node_id": str(receipt_payload.get("owner_node_id") or payload.get("owner_node_id") or normalized.source or "").strip(),
            "runtime_name": str(receipt_payload.get("runtime_name") or payload.get("runtime_name") or ""),
            "repo_root": str(receipt_payload.get("repo_root") or payload.get("repo_root") or ""),
            "discovery_root": str(receipt_payload.get("discovery_root") or payload.get("discovery_root") or ""),
            "failure_reasons": [str(item) for item in list(receipt_payload.get("failure_reasons") or [])],
            "receipt_ref": str(receipt_payload.get("receipt_ref") or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
            f"heavy_object_discovery_receipt:{envelope_id}",
        ],
        producer="runtime.control_plane.heavy_object_authority_gap_remediation",
        compatibility_floor="milestone5_heavy_object_authority_gap_remediation_result_event_v1",
    )


def commit_heavy_object_authority_gap_audit_required_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical heavy-object authority-gap-audit-required fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="heavy_object_authority_gap_audit_required",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone5_heavy_object_authority_gap_audit_required_v1",
    )


def commit_heavy_object_authority_gap_repo_remediation_required_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical heavy-object repo-remediation-required fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="heavy_object_authority_gap_repo_remediation_required",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone5_heavy_object_authority_gap_repo_remediation_required_v1",
    )


def commit_heavy_object_authority_gap_audit_requested_event(
    state_root: Path,
    envelope: ControlEnvelope,
) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object authority-gap audit fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_authority_gap_audit_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object authority-gap audit command must have an envelope_id")
    payload = dict(normalized.payload or {})
    repo_root = str(payload.get("repo_root") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    runtime_name = str(payload.get("runtime_name") or "").strip()
    audit_kind = str(payload.get("audit_kind") or "repo_root_authority_gap_audit").strip()
    audit_signature = str(payload.get("audit_signature") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    requirement_event_id = str(payload.get("requirement_event_id") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_authority_gap_audit:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_authority_gap_audit:{envelope_id}",
        node_id=str(normalized.source or "").strip(),
        event_type="heavy_object_authority_gap_audit_requested",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "repo_root": repo_root,
            "object_kind": object_kind,
            "runtime_name": runtime_name,
            "audit_kind": audit_kind,
            "audit_signature": audit_signature,
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "requirement_event_id": requirement_event_id,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"repo_root:{repo_root}",
        ],
        producer="kernel.submit.heavy_object_authority_gap_audit",
        compatibility_floor="milestone5_heavy_object_authority_gap_audit_request_event_v1",
    )


def commit_heavy_object_authority_gap_audited_event(
    state_root: Path,
    *,
    envelope: ControlEnvelope,
    receipt: dict[str, Any],
) -> dict[str, Any] | None:
    """Commit one canonical authoritative heavy-object authority-gap audit result fact."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_authority_gap_audit_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object authority-gap audit command must have an envelope_id")
    receipt_payload = dict(receipt or {})
    payload = dict(normalized.payload or {})
    repo_root = str(receipt_payload.get("repo_root") or payload.get("repo_root") or "").strip()
    object_kind = str(receipt_payload.get("object_kind") or payload.get("object_kind") or "").strip()
    runtime_name = str(receipt_payload.get("runtime_name") or payload.get("runtime_name") or "").strip()
    audit_kind = str(receipt_payload.get("audit_kind") or payload.get("audit_kind") or "repo_root_authority_gap_audit").strip()
    audit_signature = str(receipt_payload.get("audit_signature") or payload.get("audit_signature") or "").strip()
    trigger_kind = str(receipt_payload.get("trigger_kind") or payload.get("trigger_kind") or "").strip()
    trigger_ref = str(receipt_payload.get("trigger_ref") or payload.get("trigger_ref") or "").strip()
    requirement_event_id = str(
        receipt_payload.get("requirement_event_id") or payload.get("requirement_event_id") or ""
    ).strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_authority_gap_audit_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_authority_gap_audit_result:{envelope_id}",
        node_id=str(normalized.source or "").strip(),
        event_type="heavy_object_authority_gap_audited",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "repo_root": repo_root,
            "object_kind": object_kind,
            "runtime_name": runtime_name,
            "audit_kind": audit_kind,
            "audit_signature": audit_signature,
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "requirement_event_id": requirement_event_id,
            "candidate_count": int(receipt_payload.get("candidate_count") or 0),
            "registered_candidate_count": int(receipt_payload.get("registered_candidate_count") or 0),
            "discovery_covered_candidate_count": int(receipt_payload.get("discovery_covered_candidate_count") or 0),
            "managed_candidate_count": int(receipt_payload.get("managed_candidate_count") or 0),
            "unmanaged_candidate_count": int(receipt_payload.get("unmanaged_candidate_count") or 0),
            "strongest_duplicate_count": int(receipt_payload.get("strongest_duplicate_count") or 0),
            "managed_candidates": [dict(item) for item in list(receipt_payload.get("managed_candidates") or [])],
            "unmanaged_candidates": [dict(item) for item in list(receipt_payload.get("unmanaged_candidates") or [])],
            "receipt_ref": str(receipt_payload.get("receipt_ref") or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"repo_root:{repo_root}",
            f"heavy_object_authority_gap_audit_receipt:{envelope_id}",
        ],
        producer="runtime.control_plane.heavy_object_authority_gap_audit",
        compatibility_floor="milestone5_heavy_object_authority_gap_audit_result_event_v1",
    )


def commit_heavy_object_registered_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object registration fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object registration command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object registration command must preserve object_id")
    object_ref = str(payload.get("object_ref") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    byte_size = int(payload.get("byte_size") or 0)
    owner_node_id = str(payload.get("owner_node_id") or normalized.source or "").strip()
    registration_kind = str(payload.get("registration_kind") or "heavy_object_registration")
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_registration:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_registration:{envelope_id}",
        node_id=owner_node_id,
        event_type="heavy_object_registered",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "byte_size": byte_size,
            "owner_node_id": owner_node_id,
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "registration_kind": registration_kind,
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="kernel.submit.heavy_object_registration",
        compatibility_floor="milestone5_heavy_object_registration_event_v1",
    )


def commit_heavy_object_discovery_requested_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object discovery request fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_discovery_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object discovery command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object discovery command must preserve object_id")
    discovery_kind = str(payload.get("discovery_kind") or "").strip()
    if not discovery_kind:
        raise ValueError("accepted heavy-object discovery command must preserve discovery_kind")
    object_ref = str(payload.get("object_ref") or "").strip()
    discovery_root = str(payload.get("discovery_root") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    owner_node_id = str(payload.get("owner_node_id") or normalized.source or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_discovery:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_discovery:{envelope_id}",
        node_id=owner_node_id,
        event_type="heavy_object_discovery_requested",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "discovery_root": discovery_root,
            "discovery_kind": discovery_kind,
            "owner_node_id": owner_node_id,
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
            f"heavy_object_discovery_root:{discovery_root}",
        ],
        producer="kernel.submit.heavy_object_discovery",
        compatibility_floor="milestone5_heavy_object_discovery_request_event_v1",
    )


def commit_heavy_object_observed_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object observation fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_observation_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object observation command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object observation command must preserve object_id")
    object_ref = str(payload.get("object_ref") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    object_refs = [str(item) for item in list(payload.get("object_refs") or []) if str(item or "").strip()]
    duplicate_count = int(payload.get("duplicate_count") or 0)
    total_bytes = int(payload.get("total_bytes") or 0)
    observation_kind = str(payload.get("observation_kind") or "heavy_object_observation").strip()
    owner_node_id = str(payload.get("owner_node_id") or normalized.source or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_observation:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_observation:{envelope_id}",
        node_id=owner_node_id,
        event_type="heavy_object_observed",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "object_refs": object_refs,
            "duplicate_count": duplicate_count,
            "total_bytes": total_bytes,
            "observation_kind": observation_kind,
            "owner_node_id": owner_node_id,
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="kernel.submit.heavy_object_observation",
        compatibility_floor="milestone5_heavy_object_observation_event_v1",
    )


def commit_heavy_object_reference_attached_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object reference attachment fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_reference_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object reference command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object reference command must preserve object_id")
    object_ref = str(payload.get("object_ref") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    reference_ref = str(payload.get("reference_ref") or "").strip()
    reference_holder_kind = str(payload.get("reference_holder_kind") or "").strip()
    reference_holder_ref = str(payload.get("reference_holder_ref") or "").strip()
    reference_kind = str(payload.get("reference_kind") or "").strip()
    owner_node_id = str(payload.get("owner_node_id") or normalized.source or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_reference:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_reference:{envelope_id}",
        node_id=owner_node_id,
        event_type="heavy_object_reference_attached",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "reference_ref": reference_ref,
            "reference_holder_kind": reference_holder_kind,
            "reference_holder_ref": reference_holder_ref,
            "reference_kind": reference_kind,
            "owner_node_id": owner_node_id,
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
            f"heavy_object_reference_holder:{reference_holder_ref}",
            f"heavy_object_reference_ref:{reference_ref}",
        ],
        producer="kernel.submit.heavy_object_reference",
        compatibility_floor="milestone5_heavy_object_reference_event_v1",
    )


def commit_heavy_object_reference_released_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object reference release fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_reference_release_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object reference release command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object reference release command must preserve object_id")
    object_ref = str(payload.get("object_ref") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    reference_ref = str(payload.get("reference_ref") or "").strip()
    reference_holder_kind = str(payload.get("reference_holder_kind") or "").strip()
    reference_holder_ref = str(payload.get("reference_holder_ref") or "").strip()
    reference_kind = str(payload.get("reference_kind") or "").strip()
    owner_node_id = str(payload.get("owner_node_id") or normalized.source or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_reference_release:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_reference_release:{envelope_id}",
        node_id=owner_node_id,
        event_type="heavy_object_reference_released",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "reference_ref": reference_ref,
            "reference_holder_kind": reference_holder_kind,
            "reference_holder_ref": reference_holder_ref,
            "reference_kind": reference_kind,
            "owner_node_id": owner_node_id,
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
            f"heavy_object_reference_holder:{reference_holder_ref}",
            f"heavy_object_reference_ref:{reference_ref}",
        ],
        producer="kernel.submit.heavy_object_reference_release",
        compatibility_floor="milestone5_heavy_object_reference_release_event_v1",
    )


def commit_heavy_object_discovery_observed_event(
    state_root: Path,
    *,
    envelope: ControlEnvelope,
    receipt: dict[str, Any],
) -> dict[str, Any] | None:
    """Commit one canonical authoritative heavy-object observation fact produced by trusted discovery."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_discovery_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object discovery command must have an envelope_id")
    receipt_payload = dict(receipt or {})
    payload = dict(normalized.payload or {})
    object_id = str(receipt_payload.get("object_id") or payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("authoritative heavy-object discovery observation event must preserve object_id")
    object_ref = str(receipt_payload.get("object_ref") or payload.get("object_ref") or "").strip()
    object_kind = str(receipt_payload.get("object_kind") or payload.get("object_kind") or "").strip()
    object_refs = [str(item) for item in list(receipt_payload.get("discovered_refs") or []) if str(item or "").strip()]
    duplicate_count = int(receipt_payload.get("duplicate_count") or 0)
    total_bytes = int(receipt_payload.get("total_bytes") or 0)
    observation_kind = str(receipt_payload.get("discovery_kind") or payload.get("discovery_kind") or "").strip()
    owner_node_id = str(receipt_payload.get("owner_node_id") or payload.get("owner_node_id") or normalized.source or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_discovery_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_discovery_result:{envelope_id}",
        node_id=owner_node_id,
        event_type="heavy_object_observed",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "object_refs": object_refs,
            "duplicate_count": duplicate_count,
            "total_bytes": total_bytes,
            "observation_kind": observation_kind,
            "owner_node_id": owner_node_id,
            "runtime_name": str(receipt_payload.get("runtime_name") or payload.get("runtime_name") or ""),
            "repo_root": str(receipt_payload.get("repo_root") or payload.get("repo_root") or ""),
            "discovery_root": str(receipt_payload.get("discovery_root") or payload.get("discovery_root") or ""),
            "discovery_receipt_ref": str(receipt_payload.get("receipt_ref") or ""),
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="runtime.control_plane.heavy_object_discovery",
        compatibility_floor="milestone5_heavy_object_discovery_result_event_v1",
    )


def commit_heavy_object_pinned_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object pin fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_pin_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object pin command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object pin command must preserve object_id")
    object_ref = str(payload.get("object_ref") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    pin_holder_id = str(payload.get("pin_holder_id") or "").strip()
    if not pin_holder_id:
        raise ValueError("accepted heavy-object pin command must preserve pin_holder_id")
    pin_kind = str(payload.get("pin_kind") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_pin:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_pin:{envelope_id}",
        node_id=str(payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_pinned",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "pin_holder_id": pin_holder_id,
            "pin_kind": pin_kind,
            "owner_node_id": str(payload.get("owner_node_id") or normalized.source or ""),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="kernel.submit.heavy_object_pin",
        compatibility_floor="milestone5_heavy_object_pin_event_v1",
    )


def commit_heavy_object_pin_released_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object pin-release fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_pin_release_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object pin-release command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object pin-release command must preserve object_id")
    object_ref = str(payload.get("object_ref") or "").strip()
    object_kind = str(payload.get("object_kind") or "").strip()
    pin_holder_id = str(payload.get("pin_holder_id") or "").strip()
    if not pin_holder_id:
        raise ValueError("accepted heavy-object pin-release command must preserve pin_holder_id")
    pin_kind = str(payload.get("pin_kind") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_pin_release:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_pin_release:{envelope_id}",
        node_id=str(payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_pin_released",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": object_kind,
            "object_ref": object_ref,
            "pin_holder_id": pin_holder_id,
            "pin_kind": pin_kind,
            "owner_node_id": str(payload.get("owner_node_id") or normalized.source or ""),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="kernel.submit.heavy_object_pin_release",
        compatibility_floor="milestone5_heavy_object_pin_release_event_v1",
    )


def commit_heavy_object_superseded_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object supersession fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_supersession_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object supersession command must have an envelope_id")
    payload = dict(normalized.payload or {})
    superseded_object_id = str(payload.get("superseded_object_id") or "").strip()
    replacement_object_id = str(payload.get("replacement_object_id") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    if not superseded_object_id:
        raise ValueError("accepted heavy-object supersession command must preserve superseded_object_id")
    if not replacement_object_id:
        raise ValueError("accepted heavy-object supersession command must preserve replacement_object_id")
    event_id = f"evt_{hashlib.sha256(f'heavy_object_supersession:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_supersession:{envelope_id}",
        node_id=str(payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_superseded",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "superseded_object_id": superseded_object_id,
            "superseded_object_kind": str(payload.get("superseded_object_kind") or "").strip(),
            "superseded_object_ref": str(payload.get("superseded_object_ref") or "").strip(),
            "replacement_object_id": replacement_object_id,
            "replacement_object_kind": str(payload.get("replacement_object_kind") or "").strip(),
            "replacement_object_ref": str(payload.get("replacement_object_ref") or "").strip(),
            "supersession_kind": str(payload.get("supersession_kind") or "heavy_object_supersession").strip(),
            "owner_node_id": str(payload.get("owner_node_id") or normalized.source or "").strip(),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{superseded_object_id}",
            f"heavy_object:{replacement_object_id}",
        ],
        producer="kernel.submit.heavy_object_supersession",
        compatibility_floor="milestone5_heavy_object_supersession_event_v1",
    )


def commit_heavy_object_gc_eligible_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object GC-eligibility fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_gc_eligibility_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object gc-eligibility command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object gc-eligibility command must preserve object_id")
    eligibility_kind = str(payload.get("eligibility_kind") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    if not eligibility_kind:
        raise ValueError("accepted heavy-object gc-eligibility command must preserve eligibility_kind")
    superseded_by_object_id = str(payload.get("superseded_by_object_id") or "").strip()
    event_id = f"evt_{hashlib.sha256(f'heavy_object_gc_eligibility:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    causal_refs = [
        f"control_envelope:{envelope_id}",
        f"heavy_object:{object_id}",
    ]
    if superseded_by_object_id:
        causal_refs.append(f"heavy_object:{superseded_by_object_id}")
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_gc_eligibility:{envelope_id}",
        node_id=str(payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_gc_eligible",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": str(payload.get("object_kind") or "").strip(),
            "object_ref": str(payload.get("object_ref") or "").strip(),
            "eligibility_kind": eligibility_kind,
            "superseded_by_object_id": superseded_by_object_id,
            "superseded_by_object_ref": str(payload.get("superseded_by_object_ref") or "").strip(),
            "owner_node_id": str(payload.get("owner_node_id") or normalized.source or "").strip(),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=causal_refs,
        producer="kernel.submit.heavy_object_gc_eligibility",
        compatibility_floor="milestone5_heavy_object_gc_eligibility_event_v1",
    )


def commit_heavy_object_reclamation_requested_event(state_root: Path, envelope: ControlEnvelope) -> dict[str, Any] | None:
    """Commit one canonical accepted heavy-object reclamation request fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_reclamation_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object reclamation command must have an envelope_id")
    payload = dict(normalized.payload or {})
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        raise ValueError("accepted heavy-object reclamation command must preserve object_id")
    reclamation_kind = str(payload.get("reclamation_kind") or "").strip()
    trigger_kind = str(payload.get("trigger_kind") or "").strip()
    trigger_ref = str(payload.get("trigger_ref") or "").strip()
    if not reclamation_kind:
        raise ValueError("accepted heavy-object reclamation command must preserve reclamation_kind")
    event_id = f"evt_{hashlib.sha256(f'heavy_object_reclamation:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_reclamation:{envelope_id}",
        node_id=str(payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_reclamation_requested",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": str(payload.get("object_kind") or "").strip(),
            "object_ref": str(payload.get("object_ref") or "").strip(),
            "reclamation_kind": reclamation_kind,
            "owner_node_id": str(payload.get("owner_node_id") or normalized.source or "").strip(),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "round_id": str(normalized.round_id or ""),
            "generation": int(normalized.generation or 0),
            "payload_type": str(normalized.payload_type or ""),
            "note": str(normalized.note or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="kernel.submit.heavy_object_reclamation",
        compatibility_floor="milestone5_heavy_object_reclamation_request_event_v1",
    )


def commit_heavy_object_reclaimed_event(
    state_root: Path,
    *,
    envelope: ControlEnvelope,
    receipt: dict[str, Any],
) -> dict[str, Any] | None:
    """Commit one canonical authoritative heavy-object reclamation settlement fact for Milestone 5."""

    if envelope.status is not EnvelopeStatus.ACCEPTED:
        return None
    normalized = envelope.with_identity()
    if str(normalized.classification or "").strip().lower() != "heavy_object":
        return None
    if str(normalized.envelope_type or "").strip().lower() != "heavy_object_reclamation_request":
        return None
    envelope_id = str(normalized.envelope_id or "").strip()
    if not envelope_id:
        raise ValueError("accepted heavy-object reclamation command must have an envelope_id")
    receipt_payload = dict(receipt or {})
    if not bool(receipt_payload.get("reclaimed")):
        return None
    payload = dict(normalized.payload or {})
    object_id = str(receipt_payload.get("object_id") or payload.get("object_id") or "").strip()
    trigger_kind = str(receipt_payload.get("trigger_kind") or payload.get("trigger_kind") or "").strip()
    trigger_ref = str(receipt_payload.get("trigger_ref") or payload.get("trigger_ref") or "").strip()
    if not object_id:
        raise ValueError("authoritative heavy-object reclamation event must preserve object_id")
    event_id = f"evt_{hashlib.sha256(f'heavy_object_reclamation_result:{envelope_id}'.encode('utf-8')).hexdigest()[:16]}"
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"heavy_object_reclamation_result:{envelope_id}",
        node_id=str(receipt_payload.get("owner_node_id") or payload.get("owner_node_id") or normalized.source or "").strip(),
        event_type="heavy_object_reclaimed",
        payload={
            "envelope_id": envelope_id,
            "classification": "heavy_object",
            "object_id": object_id,
            "object_kind": str(receipt_payload.get("object_kind") or payload.get("object_kind") or "").strip(),
            "object_ref": str(receipt_payload.get("object_ref") or payload.get("object_ref") or "").strip(),
            "reclamation_kind": str(receipt_payload.get("reclamation_kind") or payload.get("reclamation_kind") or "").strip(),
            "owner_node_id": str(receipt_payload.get("owner_node_id") or payload.get("owner_node_id") or normalized.source or "").strip(),
            "runtime_name": str(receipt_payload.get("runtime_name") or payload.get("runtime_name") or ""),
            "repo_root": str(receipt_payload.get("repo_root") or payload.get("repo_root") or ""),
            "trigger_kind": trigger_kind,
            "trigger_ref": trigger_ref,
            "reclaimed": bool(receipt_payload.get("reclaimed")),
            "object_existed_before": bool(receipt_payload.get("object_existed_before")),
            "object_exists_after": bool(receipt_payload.get("object_exists_after")),
            "byte_size_before": int(receipt_payload.get("byte_size_before") or 0),
            "receipt_ref": str(receipt_payload.get("receipt_ref") or ""),
            "registration_event_id": str(receipt_payload.get("registration_event_id") or ""),
            "gc_eligibility_event_id": str(receipt_payload.get("gc_eligibility_event_id") or ""),
            "reclaimed_at": str(receipt_payload.get("reclaimed_at") or ""),
            "accepted_envelope": normalized.to_dict(),
        },
        causal_refs=[
            f"control_envelope:{envelope_id}",
            f"heavy_object:{object_id}",
        ],
        producer="runtime.control_plane.execute_accepted_heavy_object_reclamation",
        compatibility_floor="milestone5_heavy_object_reclaimed_event_v1",
    )


def _observation_event_id(prefix: str, observation_id: str) -> str:
    raw = f"{prefix}:{observation_id}".encode("utf-8")
    return f"evt_{hashlib.sha256(raw).hexdigest()[:16]}"


def _commit_observation_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    event_type: str,
    payload: dict[str, Any],
    producer: str,
    compatibility_floor: str,
) -> dict[str, Any]:
    observation_id = str(observation_id or "").strip()
    if not observation_id:
        raise ValueError("observation_id is required")
    return commit_control_plane_event(
        state_root,
        event_id=_observation_event_id(event_type, observation_id),
        command_id=f"{event_type}:{observation_id}",
        node_id=str(node_id or "").strip(),
        event_type=event_type,
        payload=payload,
        causal_refs=[f"observation:{observation_id}"],
        producer=producer,
        compatibility_floor=compatibility_floor,
    )


def commit_operational_hygiene_observation_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent operational hygiene observation fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="operational_hygiene_observed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone2_operational_hygiene_observation_v1",
    )


def commit_heavy_object_observation_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent heavy-object observation fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="heavy_object_observed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone2_heavy_object_observation_v1",
    )


def commit_runtime_liveness_observation_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent runtime-liveness observation fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="runtime_liveness_observed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_runtime_liveness_observation_v1",
    )


def _payload_attachment_state(payload: dict[str, Any]) -> str:
    return str(dict(payload or {}).get("attachment_state") or "").strip().upper()


def _payload_lease_epoch(payload: dict[str, Any]) -> int:
    try:
        return int(dict(payload or {}).get("lease_epoch") or 0)
    except (TypeError, ValueError):
        return 0


def _payload_lease_owner_id(payload: dict[str, Any]) -> str:
    return str(dict(payload or {}).get("lease_owner_id") or "").strip()


def _payload_pid(payload: dict[str, Any]) -> int:
    try:
        return int(dict(payload or {}).get("pid") or 0)
    except (TypeError, ValueError):
        return 0


def _payload_process_fingerprint(payload: dict[str, Any]) -> str:
    normalized = dict(payload or {})
    for key in ("process_fingerprint", "wrapped_argv_fingerprint", "command_fingerprint"):
        value = str(normalized.get(key) or "").strip()
        if value:
            return value
    return ""


def _payload_launch_identity(payload: dict[str, Any]) -> str:
    normalized = dict(payload or {})
    for key in ("launch_event_id", "launch_nonce", "launch_result_ref", "launch_id"):
        value = str(normalized.get(key) or "").strip()
        if value:
            return value
    return ""


def _payload_process_start_evidence(payload: dict[str, Any]) -> str:
    normalized = dict(payload or {})
    for key in ("process_started_at_utc", "process_start_evidence", "started_at_utc"):
        value = str(normalized.get(key) or "").strip()
        if value:
            return value
    return ""


def _latest_committed_attached_epoch_for_node(state_root: Path, *, node_id: str) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        if str(event.get("node_id") or "") != str(node_id or ""):
            continue
        payload = dict(event.get("payload") or {})
        if _payload_attachment_state(payload) != "ATTACHED":
            continue
        epoch = _payload_lease_epoch(payload)
        if latest is None or epoch >= int(latest.get("lease_epoch") or 0):
            latest = {
                "seq": int(event.get("seq") or 0),
                "event_id": str(event.get("event_id") or ""),
                "lease_epoch": epoch,
                "payload": payload,
            }
    return latest


def commit_guarded_runtime_liveness_observation_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one liveness observation unless it is a stale lower-epoch ATTACHED claim."""

    normalized_payload = dict(payload or {})
    if _payload_attachment_state(normalized_payload) == "ATTACHED":
        candidate_epoch = _payload_lease_epoch(normalized_payload)
        dominant = _latest_committed_attached_epoch_for_node(state_root, node_id=node_id)
        if dominant is not None and candidate_epoch < int(dominant.get("lease_epoch") or 0):
            return {
                "outcome": "rejected_stale_epoch",
                "runtime_root": str(require_runtime_root(state_root).resolve()),
                "node_id": str(node_id or ""),
                "observation_id": str(observation_id or ""),
                "candidate_lease_epoch": candidate_epoch,
                "dominant_lease_epoch": int(dominant.get("lease_epoch") or 0),
                "dominant_seq": int(dominant.get("seq") or 0),
                "dominant_event_id": str(dominant.get("event_id") or ""),
            }
        if dominant is not None and candidate_epoch == int(dominant.get("lease_epoch") or 0):
            candidate_owner_id = _payload_lease_owner_id(normalized_payload)
            dominant_owner_id = _payload_lease_owner_id(dict(dominant.get("payload") or {}))
            if candidate_owner_id and dominant_owner_id and candidate_owner_id != dominant_owner_id:
                return {
                    "outcome": "rejected_conflicting_epoch_owner",
                    "runtime_root": str(require_runtime_root(state_root).resolve()),
                    "node_id": str(node_id or ""),
                    "observation_id": str(observation_id or ""),
                    "candidate_lease_epoch": candidate_epoch,
                    "candidate_lease_owner_id": candidate_owner_id,
                    "dominant_lease_epoch": int(dominant.get("lease_epoch") or 0),
                    "dominant_lease_owner_id": dominant_owner_id,
                    "dominant_seq": int(dominant.get("seq") or 0),
                    "dominant_event_id": str(dominant.get("event_id") or ""),
                }
            candidate_pid = _payload_pid(normalized_payload)
            dominant_pid = _payload_pid(dict(dominant.get("payload") or {}))
            if (
                candidate_owner_id
                and dominant_owner_id
                and candidate_owner_id == dominant_owner_id
                and candidate_pid > 0
                and dominant_pid > 0
                and candidate_pid != dominant_pid
            ):
                return {
                    "outcome": "rejected_conflicting_process_identity",
                    "runtime_root": str(require_runtime_root(state_root).resolve()),
                    "node_id": str(node_id or ""),
                    "observation_id": str(observation_id or ""),
                    "candidate_lease_epoch": candidate_epoch,
                    "candidate_lease_owner_id": candidate_owner_id,
                    "candidate_pid": candidate_pid,
                    "dominant_lease_epoch": int(dominant.get("lease_epoch") or 0),
                    "dominant_lease_owner_id": dominant_owner_id,
                    "dominant_pid": dominant_pid,
                    "dominant_seq": int(dominant.get("seq") or 0),
                    "dominant_event_id": str(dominant.get("event_id") or ""),
                }
            candidate_process_fingerprint = _payload_process_fingerprint(normalized_payload)
            dominant_process_fingerprint = _payload_process_fingerprint(dict(dominant.get("payload") or {}))
            if (
                candidate_owner_id
                and dominant_owner_id
                and candidate_owner_id == dominant_owner_id
                and candidate_pid > 0
                and dominant_pid > 0
                and candidate_pid == dominant_pid
                and candidate_process_fingerprint
                and dominant_process_fingerprint
                and candidate_process_fingerprint != dominant_process_fingerprint
            ):
                return {
                    "outcome": "rejected_conflicting_process_fingerprint",
                    "runtime_root": str(require_runtime_root(state_root).resolve()),
                    "node_id": str(node_id or ""),
                    "observation_id": str(observation_id or ""),
                    "candidate_lease_epoch": candidate_epoch,
                    "candidate_lease_owner_id": candidate_owner_id,
                    "candidate_pid": candidate_pid,
                    "candidate_process_fingerprint": candidate_process_fingerprint,
                    "dominant_lease_epoch": int(dominant.get("lease_epoch") or 0),
                    "dominant_lease_owner_id": dominant_owner_id,
                    "dominant_pid": dominant_pid,
                    "dominant_process_fingerprint": dominant_process_fingerprint,
                    "dominant_seq": int(dominant.get("seq") or 0),
                    "dominant_event_id": str(dominant.get("event_id") or ""),
                }
            candidate_launch_identity = _payload_launch_identity(normalized_payload)
            dominant_launch_identity = _payload_launch_identity(dict(dominant.get("payload") or {}))
            if (
                candidate_owner_id
                and dominant_owner_id
                and candidate_owner_id == dominant_owner_id
                and candidate_pid > 0
                and dominant_pid > 0
                and candidate_pid == dominant_pid
                and candidate_process_fingerprint
                and dominant_process_fingerprint
                and candidate_process_fingerprint == dominant_process_fingerprint
                and candidate_launch_identity
                and dominant_launch_identity
                and candidate_launch_identity != dominant_launch_identity
            ):
                return {
                    "outcome": "rejected_conflicting_launch_identity",
                    "runtime_root": str(require_runtime_root(state_root).resolve()),
                    "node_id": str(node_id or ""),
                    "observation_id": str(observation_id or ""),
                    "candidate_lease_epoch": candidate_epoch,
                    "candidate_lease_owner_id": candidate_owner_id,
                    "candidate_pid": candidate_pid,
                    "candidate_process_fingerprint": candidate_process_fingerprint,
                    "candidate_launch_identity": candidate_launch_identity,
                    "dominant_lease_epoch": int(dominant.get("lease_epoch") or 0),
                    "dominant_lease_owner_id": dominant_owner_id,
                    "dominant_pid": dominant_pid,
                    "dominant_process_fingerprint": dominant_process_fingerprint,
                    "dominant_launch_identity": dominant_launch_identity,
                    "dominant_seq": int(dominant.get("seq") or 0),
                    "dominant_event_id": str(dominant.get("event_id") or ""),
                }
            candidate_process_start_evidence = _payload_process_start_evidence(normalized_payload)
            dominant_process_start_evidence = _payload_process_start_evidence(dict(dominant.get("payload") or {}))
            if (
                candidate_owner_id
                and dominant_owner_id
                and candidate_owner_id == dominant_owner_id
                and candidate_pid > 0
                and dominant_pid > 0
                and candidate_pid == dominant_pid
                and candidate_process_fingerprint
                and dominant_process_fingerprint
                and candidate_process_fingerprint == dominant_process_fingerprint
                and candidate_launch_identity
                and dominant_launch_identity
                and candidate_launch_identity == dominant_launch_identity
                and candidate_process_start_evidence
                and dominant_process_start_evidence
                and candidate_process_start_evidence != dominant_process_start_evidence
            ):
                return {
                    "outcome": "rejected_conflicting_process_start_evidence",
                    "runtime_root": str(require_runtime_root(state_root).resolve()),
                    "node_id": str(node_id or ""),
                    "observation_id": str(observation_id or ""),
                    "candidate_lease_epoch": candidate_epoch,
                    "candidate_lease_owner_id": candidate_owner_id,
                    "candidate_pid": candidate_pid,
                    "candidate_process_fingerprint": candidate_process_fingerprint,
                    "candidate_launch_identity": candidate_launch_identity,
                    "candidate_process_start_evidence": candidate_process_start_evidence,
                    "dominant_lease_epoch": int(dominant.get("lease_epoch") or 0),
                    "dominant_lease_owner_id": dominant_owner_id,
                    "dominant_pid": dominant_pid,
                    "dominant_process_fingerprint": dominant_process_fingerprint,
                    "dominant_launch_identity": dominant_launch_identity,
                    "dominant_process_start_evidence": dominant_process_start_evidence,
                    "dominant_seq": int(dominant.get("seq") or 0),
                    "dominant_event_id": str(dominant.get("event_id") or ""),
                }
    committed = commit_runtime_liveness_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        payload=normalized_payload,
        producer=producer,
    )
    return {"outcome": "committed", **dict(committed or {})}


def commit_runtime_reap_requested_event(
    state_root: Path,
    *,
    request_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent runtime reap-request fact."""

    return _commit_observation_event(
        state_root,
        observation_id=request_id,
        node_id=node_id,
        event_type="runtime_reap_requested",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone2_runtime_reap_command_v1",
    )


def commit_reap_requested_event(
    state_root: Path,
    *,
    request_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical reap-requested fact."""

    return _commit_observation_event(
        state_root,
        observation_id=request_id,
        node_id=node_id,
        event_type="reap_requested",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_reap_vocabulary_v1",
    )


def commit_runtime_reap_deferred_event(
    state_root: Path,
    *,
    request_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent runtime reap-deferred fact."""

    return _commit_observation_event(
        state_root,
        observation_id=request_id,
        node_id=node_id,
        event_type="runtime_reap_deferred",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone2_runtime_reap_command_v1",
    )


def commit_reap_deferred_event(
    state_root: Path,
    *,
    request_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical reap-deferred fact."""

    return _commit_observation_event(
        state_root,
        observation_id=request_id,
        node_id=node_id,
        event_type="reap_deferred",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_reap_vocabulary_v1",
    )


def commit_runtime_reap_confirmed_event(
    state_root: Path,
    *,
    request_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent runtime reap-confirmed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=request_id,
        node_id=node_id,
        event_type="runtime_reap_confirmed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone2_runtime_reap_command_v1",
    )


def commit_process_reap_confirmed_event(
    state_root: Path,
    *,
    request_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical process-reap-confirmed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=request_id,
        node_id=node_id,
        event_type="process_reap_confirmed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_reap_vocabulary_v1",
    )


def commit_process_orphan_detected_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical orphan-process-detected fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="process_orphan_detected",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_process_orphan_detected_v1",
    )


def commit_test_runtime_cleanup_committed_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical test-runtime-cleanup-committed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="test_runtime_cleanup_committed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_test_runtime_cleanup_committed_v1",
    )


def commit_repo_control_plane_required_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical repo-control-plane-required fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="repo_control_plane_required",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_repo_control_plane_required_v1",
    )


def commit_repo_reactor_required_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical repo-reactor-required fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="repo_reactor_required",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_repo_reactor_required_v1",
    )


def commit_repo_reactor_residency_required_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical repo-reactor-residency-required fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="repo_reactor_residency_required",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_repo_reactor_residency_required_v1",
    )


def commit_runtime_root_quiesced_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical runtime-root-quiesced fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="runtime_root_quiesced",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_runtime_root_quiesced_v1",
    )


def commit_launch_started_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical launch-started fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="launch_started",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_launch_started_v1",
    )


def commit_launch_reused_existing_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical launch-reused-existing fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="launch_reused_existing",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_launch_reused_existing_v1",
    )


def commit_process_identity_confirmed_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical process-identity-confirmed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="process_identity_confirmed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_process_identity_confirmed_v1",
    )


def commit_supervision_attached_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical supervision-attached fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="supervision_attached",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_supervision_attached_v1",
    )


def commit_supervision_detached_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical supervision-detached fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="supervision_detached",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_supervision_detached_v1",
    )


def commit_heartbeat_observed_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical heartbeat-observed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="heartbeat_observed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_heartbeat_observed_v1",
    )


def commit_runtime_loss_confirmed_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical runtime-loss-confirmed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="runtime_loss_confirmed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_runtime_loss_confirmed_v1",
    )


def commit_supervision_settled_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical supervision-settled fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="supervision_settled",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_supervision_exit_v1",
    )


def commit_process_exit_observed_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical process-exit-observed fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="process_exit_observed",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_supervision_exit_v1",
    )


def commit_lease_expired_event(
    state_root: Path,
    *,
    observation_id: str,
    node_id: str,
    payload: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one idempotent canonical lease-expired fact."""

    return _commit_observation_event(
        state_root,
        observation_id=observation_id,
        node_id=node_id,
        event_type="lease_expired",
        payload=payload,
        producer=producer,
        compatibility_floor="milestone3_lease_expiry_v1",
    )


def commit_projection_rebuild_performed_event(
    state_root: Path,
    *,
    receipt: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    """Commit one canonical explicit projection-rebuild result fact."""

    receipt_payload = dict(receipt or {})
    rebuild_scope = str(receipt_payload.get("rebuild_scope") or "").strip().lower()
    if rebuild_scope not in {"kernel", "node"}:
        raise ValueError("projection rebuild receipt must carry rebuild_scope=kernel|node")
    target_node_id = str(receipt_payload.get("target_node_id") or "").strip()
    projection_state = str(receipt_payload.get("projection_state") or "").strip()
    status = str(receipt_payload.get("status") or "").strip().lower()
    blocking_reason = str(receipt_payload.get("blocking_reason") or "").strip()
    projection_committed_seq = int(receipt_payload.get("committed_seq") or 0)
    journal_committed_seq_before = int(receipt_payload.get("journal_committed_seq") or latest_committed_seq(state_root))
    event_key = {
        "rebuild_scope": rebuild_scope,
        "target_node_id": target_node_id,
        "status": status,
        "projection_state": projection_state,
        "projection_committed_seq": projection_committed_seq,
        "journal_committed_seq_before": journal_committed_seq_before,
        "blocking_reason": blocking_reason,
        "kernel_last_applied_seq": int(receipt_payload.get("kernel_last_applied_seq") or 0),
        "node_last_applied_seq": int(receipt_payload.get("node_last_applied_seq") or 0),
        "projection_manifest_last_applied_seq": int(receipt_payload.get("projection_manifest_last_applied_seq") or 0),
        "replayed_event_count": int(receipt_payload.get("replayed_event_count") or 0),
        "repair_state_before": str(receipt_payload.get("repair_state_before") or ""),
        "repair_scope_before": str(receipt_payload.get("repair_scope_before") or ""),
        "repair_state_after": str(receipt_payload.get("repair_state_after") or ""),
        "repair_scope_after": str(receipt_payload.get("repair_scope_after") or ""),
        "repair_cleared": bool(receipt_payload.get("repair_cleared")),
    }
    event_id = f"evt_{hashlib.sha256(_canonical_json(event_key).encode('utf-8')).hexdigest()[:16]}"
    blocking_event = dict(dict(receipt_payload.get("pending_replay") or {}).get("blocking_event") or {})
    causal_refs = []
    if target_node_id:
        causal_refs.append(f"node:{target_node_id}")
    if int(blocking_event.get("seq") or 0) > 0:
        causal_refs.append(f"committed_event_seq:{int(blocking_event.get('seq') or 0)}")
    return commit_control_plane_event(
        state_root,
        event_id=event_id,
        command_id=f"projection_rebuild:{rebuild_scope}:{target_node_id or 'runtime'}:{projection_committed_seq}:{status}",
        node_id=target_node_id,
        event_type="projection_rebuild_performed",
        payload={
            "rebuild_scope": rebuild_scope,
            "target_node_id": target_node_id,
            "status": status,
            "projection_state": projection_state,
            "blocking_reason": blocking_reason,
            "projection_committed_seq": projection_committed_seq,
            "journal_committed_seq_before": journal_committed_seq_before,
            "previous_kernel_last_applied_seq": int(receipt_payload.get("previous_kernel_last_applied_seq") or 0),
            "kernel_last_applied_seq": int(receipt_payload.get("kernel_last_applied_seq") or 0),
            "previous_node_last_applied_seq": int(receipt_payload.get("previous_node_last_applied_seq") or 0),
            "node_last_applied_seq": int(receipt_payload.get("node_last_applied_seq") or 0),
            "previous_projection_manifest_last_applied_seq": int(
                receipt_payload.get("previous_projection_manifest_last_applied_seq") or 0
            ),
            "projection_manifest_last_applied_seq": int(
                receipt_payload.get("projection_manifest_last_applied_seq") or 0
            ),
            "replayed_event_count": int(receipt_payload.get("replayed_event_count") or 0),
            "updated_node_ids": [str(item) for item in list(receipt_payload.get("updated_node_ids") or [])],
            "lagging_node_ids": [str(item) for item in list(receipt_payload.get("lagging_node_ids") or [])],
            "visibility_state": str(receipt_payload.get("visibility_state") or ""),
            "repair_state_before": str(receipt_payload.get("repair_state_before") or ""),
            "repair_scope_before": str(receipt_payload.get("repair_scope_before") or ""),
            "repair_target_node_ids_before": [
                str(item) for item in list(receipt_payload.get("repair_target_node_ids_before") or [])
            ],
            "repairable_before": bool(receipt_payload.get("repairable_before")),
            "repair_state_after": str(receipt_payload.get("repair_state_after") or ""),
            "repair_scope_after": str(receipt_payload.get("repair_scope_after") or ""),
            "repair_target_node_ids_after": [
                str(item) for item in list(receipt_payload.get("repair_target_node_ids_after") or [])
            ],
            "repairable_after": bool(receipt_payload.get("repairable_after")),
            "repair_cleared": bool(receipt_payload.get("repair_cleared")),
            "pending_replay": dict(receipt_payload.get("pending_replay") or {}),
        },
        causal_refs=causal_refs,
        producer=producer,
        compatibility_floor="milestone6_projection_rebuild_result_event_v1",
    )


def iter_committed_events(state_root: Path) -> list[dict[str, Any]]:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        rows = conn.execute(
            """
            SELECT seq, event_id, command_id, runtime_root, node_id, event_type, recorded_at, producer,
                   producer_generation, schema_version, compatibility_floor, payload_json, causal_refs_json
            FROM committed_events
            ORDER BY seq ASC
            """
        ).fetchall()
    return [_row_to_event(row) for row in rows]


def iter_committed_events_after_seq(state_root: Path, *, after_seq: int) -> list[dict[str, Any]]:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        rows = conn.execute(
            """
            SELECT seq, event_id, command_id, runtime_root, node_id, event_type, recorded_at, producer,
                   producer_generation, schema_version, compatibility_floor, payload_json, causal_refs_json
            FROM committed_events
            WHERE seq > ?
            ORDER BY seq ASC
            """,
            (int(after_seq or 0),),
        ).fetchall()
    return [_row_to_event(row) for row in rows]


def committed_event_count(state_root: Path) -> int:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        row = conn.execute("SELECT COUNT(*) FROM committed_events").fetchone()
    return int((row or [0])[0])


def committed_event_stats_by_type(
    state_root: Path,
    *,
    event_types: Iterable[str],
) -> dict[str, dict[str, Any]]:
    normalized_event_types = tuple(sorted({str(item or "").strip() for item in event_types if str(item or "").strip()}))
    if not normalized_event_types:
        return {}
    db_ref = ensure_event_journal(state_root)
    placeholders = ",".join("?" for _ in normalized_event_types)
    with _connect(db_ref) as conn:
        count_rows = conn.execute(
            f"""
            SELECT event_type, COUNT(*) AS event_count, COALESCE(MAX(seq), 0) AS latest_seq
            FROM committed_events
            WHERE event_type IN ({placeholders})
            GROUP BY event_type
            """,
            normalized_event_types,
        ).fetchall()
        latest_seq_by_type = {
            str(row["event_type"] or ""): int(row["latest_seq"] or 0)
            for row in count_rows
            if str(row["event_type"] or "") and int(row["latest_seq"] or 0) > 0
        }
        latest_rows_by_seq: dict[int, dict[str, Any]] = {}
        if latest_seq_by_type:
            latest_rows = conn.execute(
                """
                SELECT seq, event_id, command_id, runtime_root, node_id, event_type, recorded_at, producer,
                       producer_generation, schema_version, compatibility_floor, payload_json, causal_refs_json
                FROM committed_events
                WHERE seq IN (%s)
                """
                % ",".join("?" for _ in latest_seq_by_type.values()),
                tuple(sorted(set(latest_seq_by_type.values()))),
            ).fetchall()
            latest_rows_by_seq = {
                int(row["seq"] or 0): _row_to_event(row)
                for row in latest_rows
                if int(row["seq"] or 0) > 0
            }
    result: dict[str, dict[str, Any]] = {
        event_type: {"count": 0, "latest_seq": 0, "latest_event": None}
        for event_type in normalized_event_types
    }
    for row in count_rows:
        event_type = str(row["event_type"] or "")
        if not event_type:
            continue
        latest_seq = int(row["latest_seq"] or 0)
        result[event_type] = {
            "count": int(row["event_count"] or 0),
            "latest_seq": latest_seq,
            "latest_event": dict(latest_rows_by_seq.get(latest_seq) or {}) or None,
        }
    return result


def projection_committed_event_count(state_root: Path) -> int:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM committed_events WHERE event_type IN (%s)"
            % ",".join("?" for _ in PROJECTION_AFFECTING_EVENT_TYPES),
            tuple(sorted(PROJECTION_AFFECTING_EVENT_TYPES)),
        ).fetchone()
    return int((row or [0])[0])


def latest_committed_seq(state_root: Path) -> int:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        row = conn.execute("SELECT COALESCE(MAX(seq), 0) FROM committed_events").fetchone()
    return int((row or [0])[0])


def latest_projection_committed_seq(state_root: Path) -> int:
    db_ref = ensure_event_journal(state_root)
    with _connect(db_ref) as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM committed_events WHERE event_type IN (%s)"
            % ",".join("?" for _ in PROJECTION_AFFECTING_EVENT_TYPES),
            tuple(sorted(PROJECTION_AFFECTING_EVENT_TYPES)),
        ).fetchone()
    return int((row or [0])[0])


def event_affects_projection(event: dict[str, Any] | str) -> bool:
    if isinstance(event, str):
        event_type = str(event or "").strip()
    else:
        event_type = str(dict(event or {}).get("event_type") or "").strip()
    return event_type in PROJECTION_AFFECTING_EVENT_TYPES


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _is_sqlite_lock_error(exc: sqlite3.Error) -> bool:
    message = str(exc).strip().lower()
    return (
        "database is locked" in message
        or "database schema is locked" in message
        or "database table is locked" in message
    )


def _insert_meta_if_missing(conn: sqlite3.Connection, *, key: str, value: Any) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO journal_meta (key, value_json)
        VALUES (?, ?)
        """,
        (str(key), _canonical_json(value)),
    )


def _committed_events_column_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("PRAGMA table_info(committed_events)").fetchall()
    return {str(row[1]) for row in rows}


def _ensure_committed_events_producer_generation_column(conn: sqlite3.Connection) -> None:
    if "producer_generation" in _committed_events_column_names(conn):
        return
    try:
        conn.execute(
            """
            ALTER TABLE committed_events
            ADD COLUMN producer_generation TEXT NOT NULL DEFAULT ''
            """
        )
    except sqlite3.OperationalError as exc:
        if "duplicate column name" not in str(exc).lower():
            raise


def _ensure_writer_fence_table(
    conn: sqlite3.Connection,
    *,
    required_producer_generation: str,
) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writer_fence (
            required_producer_generation TEXT NOT NULL
        )
        """
    )
    row = conn.execute(
        "SELECT required_producer_generation FROM writer_fence LIMIT 1"
    ).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO writer_fence (required_producer_generation) VALUES (?)",
            (str(required_producer_generation or "").strip(),),
        )


def _ensure_writer_fence_trigger(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TRIGGER IF NOT EXISTS committed_events_writer_fence_insert
        BEFORE INSERT ON committed_events
        FOR EACH ROW
        BEGIN
            SELECT CASE
                WHEN COALESCE(NEW.producer_generation, '') != COALESCE(
                    (SELECT required_producer_generation FROM writer_fence LIMIT 1),
                    ''
                )
                THEN RAISE(ABORT, 'writer_fencing_required_producer_generation')
            END;
        END
        """
    )


def _load_required_producer_generation(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT required_producer_generation FROM writer_fence LIMIT 1"
    ).fetchone()
    if row is None:
        return ""
    return str(row[0] or "").strip()


def _load_meta_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT key, value_json
        FROM journal_meta
        ORDER BY key ASC
        """
    ).fetchall()
    return {str(row["key"]): json.loads(str(row["value_json"])) for row in rows}


def _validate_meta_payload(
    payload: dict[str, Any],
    *,
    expected_runtime_root: str,
    required_producer_generation: str | None = None,
) -> None:
    recorded_runtime_root = str(payload.get("runtime_root") or "").strip()
    if recorded_runtime_root and recorded_runtime_root != str(expected_runtime_root):
        raise RuntimeError(
            "event journal meta runtime_root mismatch: "
            f"expected {expected_runtime_root!r}, found {recorded_runtime_root!r}"
        )
    recorded_event_schema_version = str(payload.get("event_schema_version") or "").strip()
    if recorded_event_schema_version and recorded_event_schema_version != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
        raise RuntimeError(
            "event journal event_schema_version mismatch: "
            f"expected {EVENT_JOURNAL_EVENT_SCHEMA_VERSION!r}, found {recorded_event_schema_version!r}"
        )
    recorded_store_format = str(payload.get("store_format_version") or "").strip()
    if recorded_store_format and recorded_store_format != EVENT_JOURNAL_STORE_FORMAT_VERSION:
        raise RuntimeError(
            "event journal store_format_version mismatch: "
            f"expected {EVENT_JOURNAL_STORE_FORMAT_VERSION!r}, found {recorded_store_format!r}"
        )
    recorded_required_producer_generation = str(payload.get("required_producer_generation") or "").strip()
    if required_producer_generation is None:
        if (
            recorded_required_producer_generation
            and recorded_required_producer_generation != EVENT_JOURNAL_PRODUCER_GENERATION
        ):
            raise RuntimeError(
                "event journal required_producer_generation mismatch: "
                f"expected {EVENT_JOURNAL_PRODUCER_GENERATION!r}, found {recorded_required_producer_generation!r}"
            )
        return

    required_producer_generation = str(required_producer_generation or "").strip()
    if not required_producer_generation:
        raise RuntimeError("event journal required_producer_generation missing")
    if required_producer_generation != EVENT_JOURNAL_PRODUCER_GENERATION:
        raise RuntimeError(
            "event journal required_producer_generation mismatch: "
            f"expected {EVENT_JOURNAL_PRODUCER_GENERATION!r}, found {required_producer_generation!r}"
        )
    if recorded_required_producer_generation != required_producer_generation:
        raise RuntimeError(
            "event journal journal_meta required_producer_generation mismatch: "
            f"writer_fence={required_producer_generation!r}, "
            f"journal_meta={recorded_required_producer_generation!r}"
        )


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    schema_version = str(row["schema_version"])
    _require_supported_event_schema_version(schema_version, context="iter_committed_events")
    return {
        "seq": int(row["seq"]),
        "event_id": str(row["event_id"]),
        "command_id": str(row["command_id"]),
        "runtime_root": str(row["runtime_root"]),
        "node_id": str(row["node_id"]),
        "event_type": str(row["event_type"]),
        "recorded_at": str(row["recorded_at"]),
        "producer": str(row["producer"]),
        "producer_generation": str(row["producer_generation"] or ""),
        "schema_version": schema_version,
        "compatibility_floor": str(row["compatibility_floor"] or ""),
        "payload": json.loads(str(row["payload_json"])),
        "causal_refs": json.loads(str(row["causal_refs_json"])),
    }


def _require_supported_event_schema_version(schema_version: str, *, context: str) -> None:
    if str(schema_version or "").strip() != EVENT_JOURNAL_EVENT_SCHEMA_VERSION:
        raise RuntimeError(
            f"{context} encountered unsupported event schema_version: "
            f"{schema_version!r}; supported={EVENT_JOURNAL_EVENT_SCHEMA_VERSION!r}"
        )


def _require_supported_producer_generation(producer_generation: str, *, context: str) -> None:
    if str(producer_generation or "").strip() != EVENT_JOURNAL_PRODUCER_GENERATION:
        raise RuntimeError(
            f"{context} encountered unsupported producer_generation: "
            f"{producer_generation!r}; supported={EVENT_JOURNAL_PRODUCER_GENERATION!r}"
        )


def _assert_replay_compatible(
    *,
    existing: dict[str, Any],
    command_id: str,
    runtime_root: str,
    node_id: str,
    event_type: str,
    producer: str,
    producer_generation: str,
    schema_version: str,
    compatibility_floor: str,
    payload_json: str,
    causal_refs_json: str,
) -> None:
    if existing["command_id"] != command_id:
        raise RuntimeError("replayed event_id conflicts with a different command_id")
    if existing["runtime_root"] != runtime_root:
        raise RuntimeError("replayed event_id conflicts with a different runtime_root")
    if existing["node_id"] != node_id:
        raise RuntimeError("replayed event_id conflicts with a different node_id")
    if existing["event_type"] != event_type:
        raise RuntimeError("replayed event_id conflicts with a different event_type")
    if existing["producer"] != producer:
        raise RuntimeError("replayed event_id conflicts with a different producer")
    existing_producer_generation = str(existing.get("producer_generation") or "").strip()
    if existing_producer_generation and existing_producer_generation != producer_generation:
        raise RuntimeError("replayed event_id conflicts with a different producer_generation")
    if existing["schema_version"] != schema_version:
        raise RuntimeError("replayed event_id conflicts with a different schema_version")
    if str(existing.get("compatibility_floor") or "") != compatibility_floor:
        raise RuntimeError("replayed event_id conflicts with a different compatibility_floor")
    if _canonical_json(existing["payload"]) != payload_json:
        raise RuntimeError("replayed event_id conflicts with a different payload")
    if _canonical_json(existing["causal_refs"]) != causal_refs_json:
        raise RuntimeError("replayed event_id conflicts with different causal_refs")
