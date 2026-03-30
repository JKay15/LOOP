"""Read-only authority queries over durable kernel state."""

from __future__ import annotations

import json
from pathlib import Path
from collections import deque
from typing import Any

from loop_product.control_intent import (
    ARTIFACT_SCOPE_SPEC,
    TERMINAL_AUTHORITY_SCOPE_SPEC,
    WORKFLOW_SCOPE_SPEC,
    normalize_machine_choice,
)
from loop_product.artifact_hygiene import (
    artifact_byte_size,
    heavy_object_identity,
    iter_publish_tree_candidate_roots,
    iter_runtime_heavy_tree_candidate_roots,
    path_within_heavy_object_store,
)
from loop_product.event_journal import (
    committed_event_count,
    committed_event_stats_by_type,
    describe_event_journal_compatibility,
    event_affects_projection,
    iter_committed_events,
    iter_committed_events_after_seq,
    latest_committed_seq,
    latest_projection_committed_seq,
)
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.kernel.state import ACTIVE_NODE_STATUSES, load_kernel_state, query_kernel_state
from loop_product.protocols.node import NodeSpec, RuntimeAttachmentState, normalize_runtime_state
from loop_product.protocols.topology import TopologyMutation, topology_target_node_ids
from loop_product.topology.activate import build_activate_request, review_activate_request
from loop_product.runtime.liveness import (
    build_runtime_loss_signal,
    select_effective_runtime_liveness_head,
    summarize_runtime_liveness_observation,
)
from loop_product.runtime import (
    load_real_root_recovery_checkpoint,
    summarize_real_root_recovery_rebasing,
    heavy_object_authority_gap_audit_receipt_ref,
    heavy_object_authority_gap_repo_remediation_receipt_ref,
    heavy_object_reclamation_receipt_ref,
    repo_tree_cleanup_receipt_ref,
)
from loop_product.runtime.recover import (
    build_relaunch_request,
    build_resume_request,
    build_retry_request,
    review_recovery_request,
)
from loop_product.runtime.cleanup_authority import repo_root_from_runtime_root
from loop_product.runtime_paths import require_runtime_root
from loop_product.protocols.schema import validate_repo_object


def _authority_runtime_projection(
    node: dict[str, Any],
    *,
    raw_latest_by_node: dict[str, dict[str, Any]],
    effective_by_node: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    node_id = str(node.get("node_id") or "")
    status = str(node.get("status") or "")
    raw_head = dict(raw_latest_by_node.get(node_id) or {})
    effective_head = dict(effective_by_node.get(node_id) or {})
    if raw_head or effective_head:
        source = dict(effective_head or raw_head)
        raw_attachment_state = str(
            raw_head.get("raw_attachment_state")
            or source.get("raw_attachment_state")
            or source.get("effective_attachment_state")
            or RuntimeAttachmentState.UNOBSERVED.value
        )
        effective_attachment_state = str(
            effective_head.get("effective_attachment_state")
            or source.get("effective_attachment_state")
            or raw_attachment_state
        )
        lease_freshness = str(
            source.get("lease_freshness")
            or raw_head.get("lease_freshness")
            or "not_applicable"
        )
        observed_at = str(raw_head.get("observed_at") or source.get("observed_at") or "")
        try:
            lease_epoch = int(source.get("lease_epoch") or raw_head.get("lease_epoch") or 0)
        except (TypeError, ValueError):
            lease_epoch = 0
        try:
            source_seq = int(source.get("seq") or raw_head.get("seq") or 0)
        except (TypeError, ValueError):
            source_seq = 0
        projection = {
            "source_kind": "runtime_liveness_event",
            "source_event_id": str(source.get("event_id") or raw_head.get("event_id") or ""),
            "source_seq": source_seq,
            "raw_attachment_state": raw_attachment_state,
            "effective_attachment_state": effective_attachment_state,
            "lease_freshness": lease_freshness,
            "lease_epoch": lease_epoch,
            "lease_owner_id": str(source.get("lease_owner_id") or raw_head.get("lease_owner_id") or ""),
            "observed_at": observed_at,
        }
    else:
        runtime_state = normalize_runtime_state(dict(node.get("runtime_state") or {}))
        raw_attachment_state = str(runtime_state.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value)
        projection = {
            "source_kind": "node_runtime_state",
            "source_event_id": "",
            "source_seq": 0,
            "raw_attachment_state": raw_attachment_state,
            "effective_attachment_state": raw_attachment_state,
            "lease_freshness": "not_available",
            "lease_epoch": 0,
            "lease_owner_id": "",
            "observed_at": str(runtime_state.get("observed_at") or ""),
        }
    projection["truthful_active"] = bool(
        status in ACTIVE_NODE_STATUSES
        and str(projection.get("effective_attachment_state") or "") == RuntimeAttachmentState.ATTACHED.value
    )
    return projection


def query_authority_view(
    state_root: Path,
    *,
    now_utc: str = "",
    materialize_projection: bool = True,
) -> dict[str, Any]:
    """Return the current authority view.

    External callers keep the historical projection-materializing behavior by default.
    Nested trace/parity surfaces can opt into a strict pure-read snapshot via
    ``materialize_projection=False`` so they never heal the projection as a side effect.
    """

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    snapshot = (
        query_kernel_state(runtime_root)
        if materialize_projection
        else load_kernel_state(runtime_root).to_dict()
    )
    liveness_view = query_runtime_liveness_view(runtime_root, now_utc=now_utc)
    raw_latest_by_node = {
        str(node_id): dict(payload or {})
        for node_id, payload in dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).items()
    }
    effective_by_node = {
        str(node_id): dict(payload or {})
        for node_id, payload in dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).items()
    }
    nodes = dict(snapshot.get("nodes") or {})
    node_graph: list[dict[str, Any]] = []
    node_lifecycle: dict[str, str] = {}
    active_child_nodes: list[str] = []
    truthful_active_child_nodes: list[str] = []
    nonlive_active_child_nodes: list[str] = []
    planned_child_nodes: list[str] = []

    for node_id, node in sorted(nodes.items(), key=lambda item: (int(item[1].get("generation", 0)), str(item[0]))):
        runtime_projection = _authority_runtime_projection(
            dict(node or {}),
            raw_latest_by_node=raw_latest_by_node,
            effective_by_node=effective_by_node,
        )
        graph_entry = {
            "node_id": str(node_id),
            "parent_node_id": node.get("parent_node_id"),
            "node_kind": str(node.get("node_kind") or ""),
            "goal_slice": str(node.get("goal_slice") or ""),
            "workflow_scope": normalize_machine_choice(node.get("workflow_scope"), WORKFLOW_SCOPE_SPEC),
            "artifact_scope": normalize_machine_choice(node.get("artifact_scope"), ARTIFACT_SCOPE_SPEC),
            "terminal_authority_scope": normalize_machine_choice(
                node.get("terminal_authority_scope"),
                TERMINAL_AUTHORITY_SCOPE_SPEC,
            ),
            "generation": int(node.get("generation") or 0),
            "round_id": str(node.get("round_id") or ""),
            "status": str(node.get("status") or ""),
            "runtime_state": dict(node.get("runtime_state") or {}),
            "runtime_projection": runtime_projection,
        }
        node_graph.append(graph_entry)
        node_lifecycle[graph_entry["node_id"]] = graph_entry["status"]
        if (
            graph_entry["node_id"] != str(snapshot.get("root_node_id") or "")
            and graph_entry["status"] in ACTIVE_NODE_STATUSES
        ):
            active_child_nodes.append(graph_entry["node_id"])
            if bool(runtime_projection.get("truthful_active")):
                truthful_active_child_nodes.append(graph_entry["node_id"])
            else:
                nonlive_active_child_nodes.append(graph_entry["node_id"])
        if (
            graph_entry["node_id"] != str(snapshot.get("root_node_id") or "")
            and graph_entry["status"] == "PLANNED"
        ):
            planned_child_nodes.append(graph_entry["node_id"])

    lease_fencing_summary = _lease_fencing_summary(
        runtime_root,
        raw_latest_by_node=raw_latest_by_node,
        effective_by_node=effective_by_node,
        node_graph=node_graph,
        root_node_id=str(snapshot.get("root_node_id") or ""),
    )

    authority_view = {
        "task_id": str(snapshot.get("task_id") or ""),
        "root_goal": str(snapshot.get("root_goal") or ""),
        "root_node_id": str(snapshot.get("root_node_id") or ""),
        "status": str(snapshot.get("status") or ""),
        "node_graph": node_graph,
        "node_lifecycle": node_lifecycle,
        "effective_requirements": [str(item) for item in (snapshot.get("active_requirements") or [])],
        "budget_view": dict(snapshot.get("complexity_budget") or {}),
        "blocked_reasons": dict(snapshot.get("blocked_reasons") or {}),
        "delegation_map": dict(snapshot.get("delegation_map") or {}),
        "active_evaluator_lanes": list(snapshot.get("active_evaluator_lanes") or []),
        "active_child_nodes": active_child_nodes,
        "truthful_active_child_nodes": truthful_active_child_nodes,
        "nonlive_active_child_nodes": nonlive_active_child_nodes,
        "planned_child_nodes": planned_child_nodes,
        "lease_fencing_summary": lease_fencing_summary,
    }
    validate_repo_object("LoopSystemAuthorityView.schema.json", authority_view)
    return authority_view


def _projection_payload(path: Path) -> tuple[bool, dict[str, Any]]:
    path = Path(path).expanduser().resolve()
    payload: dict[str, Any] = {}
    exists = path.exists()
    if exists:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            payload = {}
    return exists, payload


def _projection_metadata(path: Path) -> dict[str, Any]:
    path = Path(path).expanduser().resolve()
    exists, payload = _projection_payload(path)
    try:
        last_applied_seq = int(payload.get("last_applied_seq") or 0)
    except Exception:
        last_applied_seq = 0
    return {
        "path": str(path),
        "exists": exists,
        "projection_name": str(payload.get("projection_name") or ""),
        "last_applied_seq": last_applied_seq,
        "projection_updated_at": str(payload.get("projection_updated_at") or ""),
        "source_event_journal_ref": str(payload.get("source_event_journal_ref") or ""),
        "projection_bundle_ref": str(payload.get("projection_bundle_ref") or ""),
    }


def _projection_manifest_metadata(path: Path) -> dict[str, Any]:
    metadata = _projection_metadata(path)
    _exists, payload = _projection_payload(path)
    metadata["kernel_projection_name"] = str(payload.get("kernel_projection_name") or "")
    metadata["node_projection_name"] = str(payload.get("node_projection_name") or "")
    metadata["covered_node_ids"] = [str(item) for item in list(payload.get("covered_node_ids") or [])]
    try:
        metadata["covered_node_count"] = int(payload.get("covered_node_count") or 0)
    except Exception:
        metadata["covered_node_count"] = 0
    return metadata


def _pending_replay_summary(state_root: Path, *, committed_seq: int, after_seq: int) -> dict[str, Any]:
    try:
        durable_kernel_state = load_kernel_state(state_root)
    except Exception:
        durable_kernel_state = None
    seen_envelope_ids = {
        str(record.get("envelope_id") or "").strip()
        for record in list(getattr(durable_kernel_state, "accepted_envelopes", []) or [])
        if str(record.get("envelope_id") or "").strip()
    }
    pending = iter_committed_events_after_seq(state_root, after_seq=after_seq)
    pending_event_types: list[str] = []
    skipped_non_projection_event_types: list[str] = []
    replayable_event_count = 0
    skipped_non_projection_event_count = 0
    blocking_event: dict[str, Any] | None = None
    for event in pending:
        if not event_affects_projection(event):
            skipped_non_projection_event_count += 1
            event_type = str(event.get("event_type") or "")
            if event_type and event_type not in skipped_non_projection_event_types:
                skipped_non_projection_event_types.append(event_type)
            continue
        event_type = str(event.get("event_type") or "")
        pending_event_types.append(event_type)
        if event_type != "control_envelope_accepted":
            blocking_event = {
                "seq": int(event.get("seq") or 0),
                "event_type": event_type,
                "reason": "unsupported_event_type",
            }
            break
        envelope = ControlEnvelope.from_dict(dict(event.get("payload") or {}))
        envelope_id = str(envelope.envelope_id or envelope.with_identity().envelope_id or "").strip()
        if envelope_id and envelope_id in seen_envelope_ids:
            replayable_event_count += 1
            continue
        if str(envelope.classification or "").strip().lower() == "topology":
            blocking_event = {
                "seq": int(event.get("seq") or 0),
                "event_type": event_type,
                "reason": "topology_replay_deferred",
                "envelope_id": envelope_id,
            }
            break
        replayable_event_count += 1
    if committed_seq <= after_seq:
        mode = "caught_up"
    elif blocking_event is None:
        mode = "replayable_now"
    elif replayable_event_count > 0:
        mode = "partially_replayable"
    else:
        mode = "blocked"
    return {
        "mode": mode,
        "pending_event_count": len(pending_event_types),
        "replayable_event_count": replayable_event_count,
        "pending_event_types": pending_event_types,
        "skipped_non_projection_event_count": skipped_non_projection_event_count,
        "skipped_non_projection_event_types": skipped_non_projection_event_types,
        "blocking_event": blocking_event,
    }


def _projection_rebuild_event_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    summary = _event_summary(event)
    summary["payload"] = payload
    summary["rebuild_scope"] = str(payload.get("rebuild_scope") or "")
    summary["target_node_id"] = str(payload.get("target_node_id") or "")
    summary["status"] = str(payload.get("status") or "")
    summary["projection_state"] = str(payload.get("projection_state") or "")
    summary["blocking_reason"] = str(payload.get("blocking_reason") or "")
    summary["projection_committed_seq"] = int(payload.get("projection_committed_seq") or 0)
    summary["journal_committed_seq_before"] = int(payload.get("journal_committed_seq_before") or 0)
    summary["visibility_state"] = str(payload.get("visibility_state") or "")
    summary["repair_state_before"] = str(payload.get("repair_state_before") or "")
    summary["repair_scope_before"] = str(payload.get("repair_scope_before") or "")
    summary["repair_target_node_ids_before"] = [
        str(item) for item in list(payload.get("repair_target_node_ids_before") or [])
    ]
    summary["repairable_before"] = bool(payload.get("repairable_before"))
    summary["repair_state_after"] = str(payload.get("repair_state_after") or "")
    summary["repair_scope_after"] = str(payload.get("repair_scope_after") or "")
    summary["repair_target_node_ids_after"] = [
        str(item) for item in list(payload.get("repair_target_node_ids_after") or [])
    ]
    summary["repairable_after"] = bool(payload.get("repairable_after"))
    summary["repair_cleared"] = bool(payload.get("repair_cleared"))
    return summary


def _accepted_audit_mirror_repair_event_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    summary = _event_summary(event)
    summary["payload"] = payload
    summary["repair_kind"] = str(payload.get("repair_kind") or "")
    summary["status"] = str(payload.get("status") or "")
    summary["checkpoint_ref"] = str(payload.get("checkpoint_ref") or "")
    summary["accepted_only_count_before"] = int(payload.get("accepted_only_count_before") or 0)
    summary["accepted_only_count_after"] = int(payload.get("accepted_only_count_after") or 0)
    summary["accepted_only_gap_cleared"] = bool(payload.get("accepted_only_gap_cleared"))
    summary["accepted_audit_unique_envelope_count"] = int(payload.get("accepted_audit_unique_envelope_count") or 0)
    summary["already_mirrored_count"] = int(payload.get("already_mirrored_count") or 0)
    summary["mirrored_envelope_count"] = int(payload.get("mirrored_envelope_count") or 0)
    summary["mirrored_envelope_ids_sample"] = [
        str(item) for item in list(payload.get("mirrored_envelope_ids_sample") or [])
    ]
    summary["duplicate_envelope_id_count"] = int(payload.get("duplicate_envelope_id_count") or 0)
    summary["duplicate_envelope_ids_sample"] = [
        str(item) for item in list(payload.get("duplicate_envelope_ids_sample") or [])
    ]
    summary["commit_phase_before"] = str(payload.get("commit_phase_before") or "")
    summary["commit_phase_after"] = str(payload.get("commit_phase_after") or "")
    summary["crash_matrix_state_before"] = str(payload.get("crash_matrix_state_before") or "")
    summary["crash_matrix_state_after"] = str(payload.get("crash_matrix_state_after") or "")
    summary["journal_committed_seq_before"] = int(payload.get("journal_committed_seq_before") or 0)
    summary["journal_committed_seq_after"] = int(payload.get("journal_committed_seq_after") or 0)
    summary["rebuild_status"] = str(payload.get("rebuild_status") or "")
    summary["rebuild_projection_state"] = str(payload.get("rebuild_projection_state") or "")
    summary["rebuild_visibility_state"] = str(payload.get("rebuild_visibility_state") or "")
    summary["rebuild_blocking_reason"] = str(payload.get("rebuild_blocking_reason") or "")
    summary["rebuild_result_event_id"] = str(payload.get("rebuild_result_event_id") or "")
    summary["continuation_readiness_after"] = str(payload.get("continuation_readiness_after") or "")
    summary["action_safety_readiness_after"] = str(payload.get("action_safety_readiness_after") or "")
    summary["action_safety_blockers_after"] = [
        str(item) for item in list(payload.get("action_safety_blockers_after") or [])
    ]
    return summary


def _projection_adoption_repair_event_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    summary = _event_summary(event)
    summary["payload"] = payload
    summary["repair_kind"] = str(payload.get("repair_kind") or "")
    summary["status"] = str(payload.get("status") or "")
    summary["checkpoint_ref"] = str(payload.get("checkpoint_ref") or "")
    summary["missing_accepted_envelope_count_before"] = int(payload.get("missing_accepted_envelope_count_before") or 0)
    summary["missing_accepted_envelope_count_after"] = int(payload.get("missing_accepted_envelope_count_after") or 0)
    summary["adopted_envelope_count"] = int(payload.get("adopted_envelope_count") or 0)
    summary["adopted_envelope_ids_sample"] = [
        str(item) for item in list(payload.get("adopted_envelope_ids_sample") or [])
    ]
    summary["projection_gap_cleared"] = bool(payload.get("projection_gap_cleared"))
    summary["commit_phase_before"] = str(payload.get("commit_phase_before") or "")
    summary["commit_phase_after"] = str(payload.get("commit_phase_after") or "")
    summary["crash_matrix_state_before"] = str(payload.get("crash_matrix_state_before") or "")
    summary["crash_matrix_state_after"] = str(payload.get("crash_matrix_state_after") or "")
    summary["journal_committed_seq_before"] = int(payload.get("journal_committed_seq_before") or 0)
    summary["journal_committed_seq_after"] = int(payload.get("journal_committed_seq_after") or 0)
    summary["projection_committed_seq_before"] = int(payload.get("projection_committed_seq_before") or 0)
    summary["projection_committed_seq_after"] = int(payload.get("projection_committed_seq_after") or 0)
    summary["projection_visibility_state_after"] = str(payload.get("projection_visibility_state_after") or "")
    summary["replay_mode_after"] = str(payload.get("replay_mode_after") or "")
    summary["continuation_readiness_after"] = str(payload.get("continuation_readiness_after") or "")
    summary["action_safety_readiness_after"] = str(payload.get("action_safety_readiness_after") or "")
    summary["action_safety_blockers_after"] = [
        str(item) for item in list(payload.get("action_safety_blockers_after") or [])
    ]
    summary["accepted_audit_mirror_repair_event_id"] = str(payload.get("accepted_audit_mirror_repair_event_id") or "")
    return summary


def _repo_service_cleanup_retirement_repair_event_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    summary = _event_summary(event)
    summary["payload"] = payload
    summary["repair_kind"] = str(payload.get("repair_kind") or "")
    summary["status"] = str(payload.get("status") or "")
    summary["repo_root"] = str(payload.get("repo_root") or "")
    summary["blocked_service_count_before"] = int(payload.get("blocked_service_count_before") or 0)
    summary["blocked_service_count_after"] = int(payload.get("blocked_service_count_after") or 0)
    summary["blocked_service_kinds_before"] = [
        str(item) for item in list(payload.get("blocked_service_kinds_before") or [])
    ]
    summary["reseeded_service_count"] = int(payload.get("reseeded_service_count") or 0)
    summary["reseeded_service_kinds"] = [str(item) for item in list(payload.get("reseeded_service_kinds") or [])]
    summary["skipped_live_service_kinds"] = [
        str(item) for item in list(payload.get("skipped_live_service_kinds") or [])
    ]
    summary["still_blocked_service_kinds_after"] = [
        str(item) for item in list(payload.get("still_blocked_service_kinds_after") or [])
    ]
    summary["cleanup_authority_event_ids"] = [
        str(item) for item in list(payload.get("cleanup_authority_event_ids") or [])
    ]
    summary["runtime_identity_reseeded"] = bool(payload.get("runtime_identity_reseeded"))
    summary["repair_gap_cleared"] = bool(payload.get("repair_gap_cleared"))
    return summary


def _latest_projection_rebuild_result(
    state_root: Path,
    *,
    target_node_id: str = "",
    rebuild_scope: str = "",
) -> dict[str, Any] | None:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    wanted_node_id = str(target_node_id or "").strip()
    wanted_scope = str(rebuild_scope or "").strip().lower()
    latest: dict[str, Any] | None = None
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "projection_rebuild_performed":
            continue
        payload = dict(event.get("payload") or {})
        event_scope = str(payload.get("rebuild_scope") or "").strip().lower()
        event_node_id = str(payload.get("target_node_id") or "").strip()
        if wanted_scope and event_scope != wanted_scope:
            continue
        if wanted_node_id and event_node_id != wanted_node_id:
            continue
        latest = _projection_rebuild_event_summary(event)
    return latest


def _latest_accepted_audit_mirror_repair_result(state_root: Path) -> dict[str, Any] | None:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    latest: dict[str, Any] | None = None
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "accepted_audit_mirror_repaired":
            continue
        latest = _accepted_audit_mirror_repair_event_summary(event)
    return latest


def _latest_projection_adoption_repair_result(state_root: Path) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "projection_adoption_repaired":
            continue
        latest = _projection_adoption_repair_event_summary(event)
    return latest


def _latest_repo_service_cleanup_retirement_repair_result(state_root: Path) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "repo_service_cleanup_retirement_repaired":
            continue
        latest = _repo_service_cleanup_retirement_repair_event_summary(event)
    return latest


def _projection_replay_summary(
    state_root: Path,
    *,
    committed_seq: int,
    kernel_projection: dict[str, Any],
    node_projections: dict[str, Any],
    projection_manifest: dict[str, Any],
    visibility_state: str,
    pending_replay: dict[str, Any],
    latest_rebuild_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    lagging_node_ids = [
        str(node_id)
        for node_id, metadata in sorted(node_projections.items())
        if int(dict(metadata or {}).get("last_applied_seq") or 0) < committed_seq
    ]
    blocking_event = dict(pending_replay.get("blocking_event") or {})
    replay_mode = str(pending_replay.get("mode") or "")
    if replay_mode in {"blocked", "partially_replayable"}:
        repair_state = "kernel_rebuild_blocked"
        repair_scope = "kernel"
        repairable_now = False
        repair_target_node_ids: list[str] = []
    elif replay_mode == "replayable_now":
        repair_state = "kernel_rebuild_replayable"
        repair_scope = "kernel"
        repairable_now = True
        repair_target_node_ids = []
    elif lagging_node_ids or str(visibility_state or "") == "partial":
        repair_state = "node_rebuild_required"
        repair_scope = "node"
        repairable_now = True
        repair_target_node_ids = lagging_node_ids
    else:
        repair_state = "none"
        repair_scope = "none"
        repairable_now = False
        repair_target_node_ids = []
    repair_needed = repair_state != "none"
    return {
        "mode": replay_mode,
        "visibility_state": str(visibility_state or ""),
        "journal_committed_seq": int(latest_committed_seq(runtime_root)),
        "projection_committed_seq": int(committed_seq or 0),
        "kernel_last_applied_seq": int(kernel_projection.get("last_applied_seq") or 0),
        "projection_manifest_last_applied_seq": int(projection_manifest.get("last_applied_seq") or 0),
        "pending_projection_event_count": int(pending_replay.get("pending_event_count") or 0),
        "replayable_projection_event_count": int(pending_replay.get("replayable_event_count") or 0),
        "blocking_reason": str(blocking_event.get("reason") or ""),
        "blocking_event_seq": int(blocking_event.get("seq") or 0),
        "lagging_node_count": len(lagging_node_ids),
        "lagging_node_ids": lagging_node_ids,
        "skipped_non_projection_event_count": int(pending_replay.get("skipped_non_projection_event_count") or 0),
        "repair_state": repair_state,
        "repair_scope": repair_scope,
        "repairable_now": repairable_now,
        "repair_target_node_ids": repair_target_node_ids,
        "repair_needed": repair_needed,
        "latest_rebuild_status": str(dict(latest_rebuild_result or {}).get("status") or ""),
        "latest_rebuild_projection_state": str(dict(latest_rebuild_result or {}).get("projection_state") or ""),
        "latest_rebuild_scope": str(dict(latest_rebuild_result or {}).get("rebuild_scope") or ""),
        "latest_rebuild_target_node_id": str(dict(latest_rebuild_result or {}).get("target_node_id") or ""),
        "latest_rebuild_repair_state_before": str(dict(latest_rebuild_result or {}).get("repair_state_before") or ""),
        "latest_rebuild_repair_scope_before": str(dict(latest_rebuild_result or {}).get("repair_scope_before") or ""),
        "latest_rebuild_repair_state_after": str(dict(latest_rebuild_result or {}).get("repair_state_after") or ""),
        "latest_rebuild_repair_scope_after": str(dict(latest_rebuild_result or {}).get("repair_scope_after") or ""),
        "latest_rebuild_repair_cleared": bool(dict(latest_rebuild_result or {}).get("repair_cleared")),
    }


def _crash_boundary_phase(
    *,
    compatibility_status: str,
    accepted_only_ids: list[str],
    committed_pending_projection_count: int,
    lagging_node_ids: list[str],
) -> str:
    if str(compatibility_status or "") != "compatible":
        return "incompatible_journal"
    if accepted_only_ids:
        return "accepted_not_committed"
    if committed_pending_projection_count > 0:
        return "committed_not_projected"
    if lagging_node_ids:
        return "partially_projected"
    return "projected"


def _crash_consistency_summary(
    *,
    compatibility_status: str,
    accepted_only_ids: list[str],
    committed_pending_projection_count: int,
    lagging_node_ids: list[str],
    visibility_state: str,
    projection_replay_summary: dict[str, Any] | None,
    latest_rebuild_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    accepted_ids = [str(item) for item in list(accepted_only_ids or [])]
    lagging_ids = [str(item) for item in list(lagging_node_ids or [])]
    replay_summary = dict(projection_replay_summary or {})
    latest_rebuild = dict(latest_rebuild_result or {})
    phase = _crash_boundary_phase(
        compatibility_status=str(compatibility_status or ""),
        accepted_only_ids=accepted_ids,
        committed_pending_projection_count=int(committed_pending_projection_count or 0),
        lagging_node_ids=lagging_ids,
    )
    replay_mode = str(replay_summary.get("mode") or "")
    repair_state = str(replay_summary.get("repair_state") or "")
    repair_scope = str(replay_summary.get("repair_scope") or "")
    repairable_now = bool(replay_summary.get("repairable_now"))
    blocking_reason = str(replay_summary.get("blocking_reason") or "")

    if str(compatibility_status or "") != "compatible":
        matrix_state = "incompatible_journal"
        matrix_scope = "incompatible"
        replay_mode = "blocked_by_incompatibility"
        repair_state = "incompatible"
        repair_scope = "incompatible"
        repairable_now = False
        window_open = False
    elif accepted_ids:
        matrix_state = "accepted_not_committed"
        matrix_scope = "journal"
        window_open = True
    elif repair_state == "kernel_rebuild_blocked":
        matrix_state = "committed_not_projected_blocked"
        matrix_scope = "kernel"
        window_open = True
    elif repair_state == "kernel_rebuild_replayable":
        matrix_state = "committed_not_projected_replayable"
        matrix_scope = "kernel"
        window_open = True
    elif repair_state == "node_rebuild_required":
        matrix_state = "node_projection_lag"
        matrix_scope = "node"
        window_open = True
    elif bool(latest_rebuild.get("repair_cleared")):
        matrix_state = "repaired_recently"
        matrix_scope = "none"
        window_open = False
    else:
        matrix_state = "caught_up"
        matrix_scope = "none"
        window_open = False

    return {
        "matrix_state": matrix_state,
        "matrix_scope": matrix_scope,
        "window_open": window_open,
        "compatibility_status": str(compatibility_status or ""),
        "phase": phase,
        "visibility_state": str(visibility_state or ""),
        "replay_mode": replay_mode,
        "repair_state": repair_state,
        "repair_scope": repair_scope,
        "repairable_now": repairable_now,
        "blocking_reason": blocking_reason,
        "accepted_only_count": len(accepted_ids),
        "accepted_only_envelope_ids": accepted_ids,
        "committed_pending_projection_count": int(committed_pending_projection_count or 0),
        "lagging_node_ids": lagging_ids,
        "latest_repair_status": str(latest_rebuild.get("status") or ""),
        "latest_repair_scope": str(latest_rebuild.get("rebuild_scope") or ""),
        "latest_repair_target_node_id": str(latest_rebuild.get("target_node_id") or ""),
        "latest_repair_cleared": bool(latest_rebuild.get("repair_cleared")),
        "latest_repair_state_before": str(latest_rebuild.get("repair_state_before") or ""),
        "latest_repair_state_after": str(latest_rebuild.get("repair_state_after") or ""),
    }


def _mixed_version_summary(compatibility: dict[str, Any]) -> dict[str, Any]:
    issues = [dict(item) for item in list(compatibility.get("compatibility_issues") or [])]
    compatible = str(compatibility.get("compatibility_status") or "") == "compatible"
    return {
        "compatibility_status": str(compatibility.get("compatibility_status") or ""),
        "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
        "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
        "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            compatibility.get("journal_meta_required_producer_generation") or ""
        ),
        "replay_guard_state": "available" if compatible else "blocked_by_incompatibility",
        "projection_truth_available": compatible,
        "rebuild_allowed": compatible,
        "blocking_issue_fields": [str(item.get("field") or "") for item in issues],
        "blocking_issue_reasons": [str(item.get("reason") or "") for item in issues],
    }


def _lease_fencing_matrix_rows(
    *,
    raw_latest_by_node: dict[str, dict[str, Any]],
    effective_by_node: dict[str, dict[str, Any]],
    node_graph: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
    root_node_id: str = "",
) -> list[dict[str, Any]]:
    node_entries = {
        str(dict(item or {}).get("node_id") or ""): dict(item or {})
        for item in list(node_graph or [])
        if str(dict(item or {}).get("node_id") or "").strip()
    }
    normalized_root_node_id = str(root_node_id or "").strip()
    node_ids = sorted(
        {
            *(str(node_id) for node_id in dict(raw_latest_by_node or {}).keys()),
            *(str(node_id) for node_id in dict(effective_by_node or {}).keys()),
            *(str(node_id) for node_id in node_entries.keys()),
        }
    )
    rows: list[dict[str, Any]] = []
    for node_id in node_ids:
        source_node = dict(node_entries.get(node_id) or {})
        raw_head = dict(dict(raw_latest_by_node or {}).get(node_id) or {})
        effective_head = dict(dict(effective_by_node or {}).get(node_id) or {})
        raw_payload = dict(raw_head.get("payload") or {})
        effective_payload = dict(effective_head.get("payload") or {})
        source_status = str(source_node.get("status") or "").strip()
        raw_attachment_state = str(
            raw_head.get("raw_attachment_state")
            or raw_head.get("effective_attachment_state")
            or RuntimeAttachmentState.UNOBSERVED.value
        ).strip().upper()
        effective_attachment_state = str(
            effective_head.get("effective_attachment_state")
            or raw_head.get("effective_attachment_state")
            or raw_attachment_state
            or RuntimeAttachmentState.UNOBSERVED.value
        ).strip().upper()
        raw_lease_freshness = str(raw_head.get("lease_freshness") or "").strip()
        effective_lease_freshness = str(
            effective_head.get("lease_freshness")
            or raw_head.get("lease_freshness")
            or ""
        ).strip()
        try:
            raw_lease_epoch = int(raw_head.get("lease_epoch") or 0)
        except (TypeError, ValueError):
            raw_lease_epoch = 0
        try:
            effective_lease_epoch = int(
                effective_head.get("lease_epoch")
                or raw_head.get("lease_epoch")
                or 0
            )
        except (TypeError, ValueError):
            effective_lease_epoch = 0
        try:
            raw_seq = int(raw_head.get("seq") or 0)
        except (TypeError, ValueError):
            raw_seq = 0
        try:
            effective_seq = int(effective_head.get("seq") or raw_head.get("seq") or 0)
        except (TypeError, ValueError):
            effective_seq = 0
        try:
            raw_pid = int(raw_payload.get("pid") or 0)
        except (TypeError, ValueError):
            raw_pid = 0
        try:
            effective_pid = int(
                effective_payload.get("pid")
                or raw_payload.get("pid")
                or 0
            )
        except (TypeError, ValueError):
            effective_pid = 0
        truthful_active = bool(
            source_node
            and node_id != normalized_root_node_id
            and source_status in ACTIVE_NODE_STATUSES
            and effective_attachment_state == RuntimeAttachmentState.ATTACHED.value
        )
        projection_nonlive_active = bool(
            source_node
            and node_id != normalized_root_node_id
            and source_status in ACTIVE_NODE_STATUSES
            and effective_attachment_state != RuntimeAttachmentState.ATTACHED.value
        )
        if (
            raw_head
            and raw_lease_freshness == "superseded_epoch"
            and raw_attachment_state in {RuntimeAttachmentState.LOST.value, RuntimeAttachmentState.TERMINAL.value}
            and effective_attachment_state == RuntimeAttachmentState.ATTACHED.value
        ):
            matrix_cell = "fenced_owner_exit_overridden"
        elif raw_head and raw_lease_freshness == "superseded_epoch":
            matrix_cell = "stale_lower_epoch_rejected"
        elif (
            raw_head
            and raw_attachment_state == RuntimeAttachmentState.ATTACHED.value
            and raw_lease_freshness == "expired"
        ):
            matrix_cell = "expired_attached_rejected"
        elif effective_attachment_state == RuntimeAttachmentState.ATTACHED.value:
            matrix_cell = "authoritative_attached"
        elif raw_attachment_state in {RuntimeAttachmentState.LOST.value, RuntimeAttachmentState.TERMINAL.value} and (
            effective_attachment_state in {RuntimeAttachmentState.LOST.value, RuntimeAttachmentState.TERMINAL.value}
        ):
            matrix_cell = "latest_nonlive_dominates"
        else:
            matrix_cell = "unobserved"
        rows.append(
            {
                "node_id": node_id,
                "matrix_cell": matrix_cell,
                "source_status": source_status,
                "raw_attachment_state": raw_attachment_state,
                "effective_attachment_state": effective_attachment_state,
                "raw_lease_freshness": raw_lease_freshness,
                "effective_lease_freshness": effective_lease_freshness,
                "raw_lease_epoch": raw_lease_epoch,
                "effective_lease_epoch": effective_lease_epoch,
                "raw_seq": raw_seq,
                "effective_seq": effective_seq,
                "raw_pid": raw_pid,
                "effective_pid": effective_pid,
                "raw_lease_owner_id": str(raw_head.get("lease_owner_id") or raw_payload.get("lease_owner_id") or ""),
                "effective_lease_owner_id": str(
                    effective_head.get("lease_owner_id")
                    or raw_head.get("lease_owner_id")
                    or effective_payload.get("lease_owner_id")
                    or raw_payload.get("lease_owner_id")
                    or ""
                ),
                "truthful_active": truthful_active,
                "projection_nonlive_active": projection_nonlive_active,
                "superseded_by_lease_epoch": int(raw_head.get("superseded_by_lease_epoch") or 0),
                "raw_source_event_id": str(raw_head.get("event_id") or ""),
                "effective_source_event_id": str(effective_head.get("event_id") or raw_head.get("event_id") or ""),
            }
        )
    return rows


def _lease_fencing_summary_from_rows(
    rows: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    compatibility_status: str,
) -> dict[str, Any]:
    normalized_rows = [dict(item or {}) for item in list(rows or [])]
    if str(compatibility_status or "") != "compatible":
        return {
            "matrix_state": "incompatible_journal",
            "compatibility_status": str(compatibility_status or ""),
            "matrix_node_count": 0,
            "authoritative_attached_node_count": 0,
            "expired_attached_rejected_node_count": 0,
            "stale_lower_epoch_rejected_node_count": 0,
            "fenced_owner_exit_overridden_node_count": 0,
            "latest_nonlive_dominates_node_count": 0,
            "unobserved_node_count": 0,
            "truthful_active_child_node_count": 0,
            "projection_nonlive_active_child_node_count": 0,
            "matrix_cells": {},
            "authoritative_attached_node_ids": [],
            "expired_attached_rejected_node_ids": [],
            "stale_lower_epoch_rejected_node_ids": [],
            "fenced_owner_exit_overridden_node_ids": [],
            "latest_nonlive_dominates_node_ids": [],
            "projection_nonlive_active_child_node_ids": [],
        }

    def _ids_for(*, cell: str = "", projection_nonlive_active: bool | None = None) -> list[str]:
        node_ids: list[str] = []
        for row in normalized_rows:
            if cell and str(row.get("matrix_cell") or "") != cell:
                continue
            if projection_nonlive_active is not None and bool(row.get("projection_nonlive_active")) != projection_nonlive_active:
                continue
            node_id = str(row.get("node_id") or "").strip()
            if node_id:
                node_ids.append(node_id)
        return sorted(node_ids)

    matrix_cells: dict[str, int] = {}
    for row in normalized_rows:
        cell = str(row.get("matrix_cell") or "").strip()
        if not cell:
            continue
        matrix_cells[cell] = int(matrix_cells.get(cell) or 0) + 1

    authoritative_ids = _ids_for(cell="authoritative_attached")
    expired_ids = _ids_for(cell="expired_attached_rejected")
    stale_ids = _ids_for(cell="stale_lower_epoch_rejected")
    fenced_ids = _ids_for(cell="fenced_owner_exit_overridden")
    latest_nonlive_ids = _ids_for(cell="latest_nonlive_dominates")
    projection_nonlive_active_ids = _ids_for(projection_nonlive_active=True)

    if fenced_ids or stale_ids:
        matrix_state = "lease_fencing_engaged"
    elif expired_ids or projection_nonlive_active_ids:
        matrix_state = "lease_truth_drift_visible"
    elif latest_nonlive_ids:
        matrix_state = "nonlive_truth_dominant"
    else:
        matrix_state = "clean"

    return {
        "matrix_state": matrix_state,
        "compatibility_status": "compatible",
        "matrix_node_count": len(normalized_rows),
        "authoritative_attached_node_count": len(authoritative_ids),
        "expired_attached_rejected_node_count": len(expired_ids),
        "stale_lower_epoch_rejected_node_count": len(stale_ids),
        "fenced_owner_exit_overridden_node_count": len(fenced_ids),
        "latest_nonlive_dominates_node_count": len(latest_nonlive_ids),
        "unobserved_node_count": int(matrix_cells.get("unobserved") or 0),
        "truthful_active_child_node_count": sum(1 for row in normalized_rows if bool(row.get("truthful_active"))),
        "projection_nonlive_active_child_node_count": len(projection_nonlive_active_ids),
        "matrix_cells": matrix_cells,
        "authoritative_attached_node_ids": authoritative_ids[:5],
        "expired_attached_rejected_node_ids": expired_ids[:5],
        "stale_lower_epoch_rejected_node_ids": stale_ids[:5],
        "fenced_owner_exit_overridden_node_ids": fenced_ids[:5],
        "latest_nonlive_dominates_node_ids": latest_nonlive_ids[:5],
        "projection_nonlive_active_child_node_ids": projection_nonlive_active_ids[:5],
    }


def _lease_fencing_summary(
    runtime_root: Path,
    *,
    raw_latest_by_node: dict[str, dict[str, Any]] | None = None,
    effective_by_node: dict[str, dict[str, Any]] | None = None,
    node_graph: list[dict[str, Any]] | tuple[dict[str, Any], ...] | None = None,
    root_node_id: str = "",
) -> dict[str, Any]:
    runtime_root = require_runtime_root(Path(runtime_root).expanduser().resolve())
    if raw_latest_by_node is None or effective_by_node is None:
        liveness_view = query_runtime_liveness_view(runtime_root, initialize_if_missing=True)
        compatibility_status = str(liveness_view.get("compatibility_status") or "")
        if compatibility_status != "compatible":
            return _lease_fencing_summary_from_rows([], compatibility_status=compatibility_status)
        raw_latest_by_node = {
            str(node_id): dict(payload or {})
            for node_id, payload in dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).items()
        }
        effective_by_node = {
            str(node_id): dict(payload or {})
            for node_id, payload in dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).items()
        }
    if node_graph is None or not root_node_id:
        try:
            kernel_state = load_kernel_state(runtime_root)
        except Exception:
            kernel_state = None
        snapshot_root_node_id = str(getattr(kernel_state, "root_node_id", "") or "").strip()
        snapshot_nodes = dict(getattr(kernel_state, "nodes", {}) or {}) if kernel_state is not None else {}
        normalized_root_node_id = str(root_node_id or snapshot_root_node_id or "").strip()
        if node_graph is None:
            node_graph = [
                {
                    "node_id": str(node_id),
                    "status": str(dict(node or {}).get("status") or ""),
                }
                for node_id, node in snapshot_nodes.items()
            ]
    else:
        normalized_root_node_id = str(root_node_id or "").strip()
    rows = _lease_fencing_matrix_rows(
        raw_latest_by_node=raw_latest_by_node or {},
        effective_by_node=effective_by_node or {},
        node_graph=node_graph or [],
        root_node_id=normalized_root_node_id,
    )
    return _lease_fencing_summary_from_rows(rows, compatibility_status="compatible")


def _accepted_audit_envelope_ids(runtime_root: Path) -> set[str]:
    accepted_ids: set[str] = set()
    audit_dir = runtime_root / "audit"
    if not audit_dir.exists():
        return accepted_ids
    for path in sorted(audit_dir.glob("envelope_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("status") or "").strip().lower() != "accepted":
            continue
        envelope_id = str(payload.get("envelope_id") or "").strip()
        if envelope_id:
            accepted_ids.add(envelope_id)
    return accepted_ids


def _mirrored_envelope_ids(runtime_root: Path) -> set[str]:
    mirrored: set[str] = set()
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "control_envelope_accepted":
            continue
        payload = dict(event.get("payload") or {})
        envelope_id = str(payload.get("envelope_id") or "").strip()
        if envelope_id:
            mirrored.add(envelope_id)
    return mirrored


def _event_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = dict(event.get("payload") or {})
    summary = {
        "seq": int(event.get("seq") or 0),
        "event_type": str(event.get("event_type") or ""),
        "event_id": str(event.get("event_id") or ""),
        "command_id": str(event.get("command_id") or ""),
        "schema_version": str(event.get("schema_version") or ""),
        "recorded_at": str(event.get("recorded_at") or ""),
    }
    if summary["event_type"] == "control_envelope_accepted":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["source"] = str(payload.get("source") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
        summary["payload_type"] = str(payload.get("payload_type") or "")
    if summary["event_type"] == "topology_command_accepted":
        review = dict(payload.get("review") or {})
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["command_kind"] = str(payload.get("command_kind") or "")
        summary["source_node_id"] = str(payload.get("source_node_id") or "")
        summary["target_node_ids"] = [str(item) for item in list(payload.get("target_node_ids") or [])]
        summary["round_id"] = str(payload.get("round_id") or "")
        summary["review_decision"] = str(payload.get("review_decision") or review.get("decision") or "")
        summary["review_summary"] = str(payload.get("review_summary") or review.get("summary") or "")
        summary["self_attribution"] = str(review.get("self_attribution") or "")
        summary["self_repair"] = str(review.get("self_repair") or "")
    if summary["event_type"] == "terminal_result_recorded":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["node_id"] = str(payload.get("node_id") or "")
        summary["outcome"] = str(payload.get("outcome") or "")
        summary["node_status"] = str(payload.get("node_status") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
        summary["summary"] = str(payload.get("summary") or "")
        summary["self_attribution"] = str(payload.get("self_attribution") or "")
        summary["self_repair"] = str(payload.get("self_repair") or "")
    if summary["event_type"] == "evaluator_result_recorded":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["node_id"] = str(payload.get("node_id") or "")
        summary["lane"] = str(payload.get("lane") or "")
        summary["verdict"] = str(payload.get("verdict") or "")
        summary["retryable"] = bool(payload.get("retryable"))
        summary["round_id"] = str(payload.get("round_id") or "")
        summary["summary"] = str(payload.get("summary") or "")
        summary["self_attribution"] = str(payload.get("self_attribution") or "")
        summary["self_repair"] = str(payload.get("self_repair") or "")
    if summary["event_type"] == "repo_tree_cleanup_requested":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["tree_root"] = str(payload.get("tree_root") or "")
        summary["repo_roots"] = [str(item) for item in list(payload.get("repo_roots") or [])]
        summary["repo_count"] = int(payload.get("repo_count") or 0)
        summary["include_anchor_repo_root"] = bool(payload.get("include_anchor_repo_root"))
        summary["cleanup_kind"] = str(payload.get("cleanup_kind") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_discovery_requested":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["discovery_root"] = str(payload.get("discovery_root") or "")
        summary["discovery_kind"] = str(payload.get("discovery_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_registered":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["byte_size"] = int(payload.get("byte_size") or 0)
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["registration_kind"] = str(payload.get("registration_kind") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_observed":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["object_refs"] = [str(item) for item in list(payload.get("object_refs") or [])]
        summary["duplicate_count"] = int(payload.get("duplicate_count") or 0)
        summary["total_bytes"] = int(payload.get("total_bytes") or 0)
        summary["observation_kind"] = str(payload.get("observation_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_authority_gap_remediation_settled":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["object_refs"] = [str(item) for item in list(payload.get("object_refs") or [])]
        summary["duplicate_count"] = int(payload.get("duplicate_count") or 0)
        summary["total_bytes"] = int(payload.get("total_bytes") or 0)
        summary["discovery_kind"] = str(payload.get("discovery_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["receipt_ref"] = str(payload.get("receipt_ref") or "")
    if summary["event_type"] == "heavy_object_pinned":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["pin_holder_id"] = str(payload.get("pin_holder_id") or "")
        summary["pin_kind"] = str(payload.get("pin_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_pin_released":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["pin_holder_id"] = str(payload.get("pin_holder_id") or "")
        summary["pin_kind"] = str(payload.get("pin_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_reference_attached":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["reference_ref"] = str(payload.get("reference_ref") or "")
        summary["reference_holder_kind"] = str(payload.get("reference_holder_kind") or "")
        summary["reference_holder_ref"] = str(payload.get("reference_holder_ref") or "")
        summary["reference_kind"] = str(payload.get("reference_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["trigger_kind"] = str(payload.get("trigger_kind") or "")
        summary["trigger_ref"] = str(payload.get("trigger_ref") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_reference_released":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["reference_ref"] = str(payload.get("reference_ref") or "")
        summary["reference_holder_kind"] = str(payload.get("reference_holder_kind") or "")
        summary["reference_holder_ref"] = str(payload.get("reference_holder_ref") or "")
        summary["reference_kind"] = str(payload.get("reference_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["trigger_kind"] = str(payload.get("trigger_kind") or "")
        summary["trigger_ref"] = str(payload.get("trigger_ref") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_superseded":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["superseded_object_id"] = str(payload.get("superseded_object_id") or "")
        summary["superseded_object_kind"] = str(payload.get("superseded_object_kind") or "")
        summary["superseded_object_ref"] = str(payload.get("superseded_object_ref") or "")
        summary["replacement_object_id"] = str(payload.get("replacement_object_id") or "")
        summary["replacement_object_kind"] = str(payload.get("replacement_object_kind") or "")
        summary["replacement_object_ref"] = str(payload.get("replacement_object_ref") or "")
        summary["supersession_kind"] = str(payload.get("supersession_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["trigger_kind"] = str(payload.get("trigger_kind") or "")
        summary["trigger_ref"] = str(payload.get("trigger_ref") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_gc_eligible":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["eligibility_kind"] = str(payload.get("eligibility_kind") or "")
        summary["superseded_by_object_id"] = str(payload.get("superseded_by_object_id") or "")
        summary["superseded_by_object_ref"] = str(payload.get("superseded_by_object_ref") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["trigger_kind"] = str(payload.get("trigger_kind") or "")
        summary["trigger_ref"] = str(payload.get("trigger_ref") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_reclamation_requested":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["reclamation_kind"] = str(payload.get("reclamation_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["trigger_kind"] = str(payload.get("trigger_kind") or "")
        summary["trigger_ref"] = str(payload.get("trigger_ref") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_authority_gap_repo_remediation_requested":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["remediation_kind"] = str(payload.get("remediation_kind") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_authority_gap_repo_remediation_settled":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["remediation_kind"] = str(payload.get("remediation_kind") or "")
        summary["candidate_count"] = int(payload.get("candidate_count") or 0)
        summary["requested_candidate_count"] = int(payload.get("requested_candidate_count") or 0)
        summary["skipped_candidate_count"] = int(payload.get("skipped_candidate_count") or 0)
        summary["fully_managed_after"] = bool(payload.get("fully_managed_after"))
        summary["receipt_ref"] = str(payload.get("receipt_ref") or "")
    if summary["event_type"] == "heavy_object_authority_gap_audit_requested":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["audit_kind"] = str(payload.get("audit_kind") or "")
        summary["round_id"] = str(payload.get("round_id") or "")
    if summary["event_type"] == "heavy_object_authority_gap_audited":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["audit_kind"] = str(payload.get("audit_kind") or "")
        summary["candidate_count"] = int(payload.get("candidate_count") or 0)
        summary["managed_candidate_count"] = int(payload.get("managed_candidate_count") or 0)
        summary["unmanaged_candidate_count"] = int(payload.get("unmanaged_candidate_count") or 0)
        summary["receipt_ref"] = str(payload.get("receipt_ref") or "")
    if summary["event_type"] == "heavy_object_reclaimed":
        summary["envelope_id"] = str(payload.get("envelope_id") or "")
        summary["classification"] = str(payload.get("classification") or "")
        summary["object_id"] = str(payload.get("object_id") or "")
        summary["object_kind"] = str(payload.get("object_kind") or "")
        summary["object_ref"] = str(payload.get("object_ref") or "")
        summary["reclamation_kind"] = str(payload.get("reclamation_kind") or "")
        summary["owner_node_id"] = str(payload.get("owner_node_id") or "")
        summary["runtime_name"] = str(payload.get("runtime_name") or "")
        summary["repo_root"] = str(payload.get("repo_root") or "")
        summary["trigger_kind"] = str(payload.get("trigger_kind") or "")
        summary["trigger_ref"] = str(payload.get("trigger_ref") or "")
        summary["reclaimed"] = bool(payload.get("reclaimed"))
        summary["object_existed_before"] = bool(payload.get("object_existed_before"))
        summary["object_exists_after"] = bool(payload.get("object_exists_after"))
        summary["byte_size_before"] = int(payload.get("byte_size_before") or 0)
        summary["receipt_ref"] = str(payload.get("receipt_ref") or "")
        summary["registration_event_id"] = str(payload.get("registration_event_id") or "")
        summary["gc_eligibility_event_id"] = str(payload.get("gc_eligibility_event_id") or "")
        summary["reclaimed_at"] = str(payload.get("reclaimed_at") or "")
    if summary["event_type"] == "launch_started":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["source_result_ref"] = str(payload.get("source_result_ref") or "")
        summary["launch_request_ref"] = str(payload.get("launch_request_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["wrapped_argv_fingerprint"] = str(payload.get("wrapped_argv_fingerprint") or "")
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
    if summary["event_type"] == "launch_reused_existing":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["reused_launch_event_id"] = str(payload.get("reused_launch_event_id") or "")
        summary["reuse_launch_result_ref"] = str(payload.get("reuse_launch_result_ref") or "")
        summary["source_result_ref"] = str(payload.get("source_result_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["wrapped_argv_fingerprint"] = str(payload.get("wrapped_argv_fingerprint") or "")
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
    if summary["event_type"] == "process_identity_confirmed":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["status_result_ref"] = str(payload.get("status_result_ref") or "")
        summary["attachment_state"] = str(payload.get("attachment_state") or "")
        summary["observation_kind"] = str(payload.get("observation_kind") or "")
        summary["summary"] = str(payload.get("summary") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
    if summary["event_type"] == "supervision_attached":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["lease_owner_id"] = str(payload.get("lease_owner_id") or "")
        summary["lease_epoch"] = int(payload.get("lease_epoch") or 0)
        summary["runtime_ref"] = str(payload.get("runtime_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
        summary["process_started_at_utc"] = str(payload.get("process_started_at_utc") or "")
    if summary["event_type"] == "heartbeat_observed":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["lease_owner_id"] = str(payload.get("lease_owner_id") or "")
        summary["lease_epoch"] = int(payload.get("lease_epoch") or 0)
        summary["attachment_state"] = str(payload.get("attachment_state") or "")
        summary["observation_kind"] = str(payload.get("observation_kind") or "")
        summary["summary"] = str(payload.get("summary") or "")
        summary["status_result_ref"] = str(payload.get("status_result_ref") or "")
        summary["runtime_ref"] = str(payload.get("runtime_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
        summary["process_started_at_utc"] = str(payload.get("process_started_at_utc") or "")
    if summary["event_type"] == "lease_expired":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["lease_owner_id"] = str(payload.get("lease_owner_id") or "")
        summary["lease_epoch"] = int(payload.get("lease_epoch") or 0)
        summary["attachment_state"] = str(payload.get("attachment_state") or "")
        summary["observation_kind"] = str(payload.get("observation_kind") or "")
        summary["status_result_ref"] = str(payload.get("status_result_ref") or "")
        summary["runtime_ref"] = str(payload.get("runtime_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
        summary["expired_from_event_id"] = str(payload.get("expired_from_event_id") or "")
        summary["expired_from_seq"] = int(payload.get("expired_from_seq") or 0)
    if summary["event_type"] == "process_exit_observed":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["lease_owner_id"] = str(payload.get("lease_owner_id") or "")
        summary["lease_epoch"] = int(payload.get("lease_epoch") or 0)
        summary["attachment_state"] = str(payload.get("attachment_state") or "")
        summary["observation_kind"] = str(payload.get("observation_kind") or "")
        summary["status_result_ref"] = str(payload.get("status_result_ref") or "")
        summary["runtime_ref"] = str(payload.get("runtime_ref") or "")
        summary["supervision_result_ref"] = str(payload.get("supervision_result_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
    if summary["event_type"] == "runtime_loss_confirmed":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["attachment_state"] = str(payload.get("attachment_state") or "")
        summary["observation_kind"] = str(payload.get("observation_kind") or "")
        summary["status_result_ref"] = str(payload.get("status_result_ref") or "")
        summary["runtime_ref"] = str(payload.get("runtime_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
        summary["recovery_reason"] = str(payload.get("recovery_reason") or "")
    if summary["event_type"] == "supervision_settled":
        summary["node_id"] = str(event.get("node_id") or "")
        summary["launch_event_id"] = str(payload.get("launch_event_id") or "")
        summary["lease_owner_id"] = str(payload.get("lease_owner_id") or "")
        summary["lease_epoch"] = int(payload.get("lease_epoch") or 0)
        summary["attachment_state"] = str(payload.get("attachment_state") or "")
        summary["settled_reason"] = str(payload.get("settled_reason") or "")
        summary["recoveries_used"] = int(payload.get("recoveries_used") or 0)
        summary["status_result_ref"] = str(payload.get("status_result_ref") or "")
        summary["runtime_ref"] = str(payload.get("runtime_ref") or "")
        summary["supervision_result_ref"] = str(payload.get("supervision_result_ref") or "")
        summary["pid"] = int(payload.get("pid") or 0)
        summary["process_fingerprint"] = str(payload.get("process_fingerprint") or "")
    return summary


def _accepted_audit_envelope(runtime_root: Path, envelope_id: str) -> dict[str, Any]:
    envelope_id = str(envelope_id or "").strip()
    if not envelope_id:
        return {}
    audit_dir = runtime_root / "audit"
    if not audit_dir.exists():
        return {}
    for path in sorted(audit_dir.glob("envelope_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("envelope_id") or "").strip() != envelope_id:
            continue
        return dict(payload or {})
    return {}


def _event_for_envelope(runtime_root: Path, *, event_type: str, envelope_id: str) -> dict[str, Any]:
    envelope_id = str(envelope_id or "").strip()
    if not envelope_id:
        return {}
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != event_type:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("envelope_id") or "").strip() != envelope_id:
            continue
        return dict(event or {})
    return {}


def _event_by_id(runtime_root: Path, *, event_id: str) -> dict[str, Any]:
    normalized_event_id = str(event_id or "").strip()
    if not normalized_event_id:
        return {}
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_id") or "").strip() == normalized_event_id:
            return dict(event or {})
    return {}


def _event_for_object(runtime_root: Path, *, event_type: str, object_id: str, object_ref: str) -> dict[str, Any]:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    latest: dict[str, Any] = {}
    if not normalized_object_id and not normalized_object_ref:
        return {}
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != event_type:
            continue
        payload = dict(event.get("payload") or {})
        if _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            latest = dict(event or {})
    return latest


def _event_for_launch(runtime_root: Path, *, event_type: str, launch_event_id: str) -> dict[str, Any]:
    launch_event_id = str(launch_event_id or "").strip()
    if not launch_event_id:
        return {}
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != event_type:
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("launch_event_id") or "").strip() != launch_event_id:
            continue
        return dict(event or {})
    return {}


def _latest_matching_heavy_object_supersession(
    runtime_root: Path,
    *,
    superseded_object_id: str,
    superseded_object_ref: str,
    replacement_object_id: str,
    replacement_object_ref: str,
) -> dict[str, Any]:
    normalized_superseded_id = str(superseded_object_id or "").strip()
    normalized_superseded_ref = str(superseded_object_ref or "").strip()
    normalized_replacement_id = str(replacement_object_id or "").strip()
    normalized_replacement_ref = str(replacement_object_ref or "").strip()
    if not normalized_superseded_id or not normalized_superseded_ref:
        return {}
    for event in reversed(list(iter_committed_events(runtime_root))):
        if str(event.get("event_type") or "") != "heavy_object_superseded":
            continue
        payload = dict(event.get("payload") or {})
        if str(payload.get("superseded_object_id") or "").strip() != normalized_superseded_id:
            continue
        if str(payload.get("superseded_object_ref") or "").strip() != normalized_superseded_ref:
            continue
        if normalized_replacement_id and str(payload.get("replacement_object_id") or "").strip() != normalized_replacement_id:
            continue
        if normalized_replacement_ref and str(payload.get("replacement_object_ref") or "").strip() != normalized_replacement_ref:
            continue
        return dict(event)
    return {}


def _latest_lineage_event_for_node(
    runtime_root: Path,
    *,
    event_type: str,
    node_id: str,
    launch_event_id: str = "",
    lease_owner_id: str = "",
    lease_epoch: int = 0,
) -> dict[str, Any]:
    normalized_node_id = str(node_id or "").strip()
    normalized_launch_event_id = str(launch_event_id or "").strip()
    normalized_lease_owner_id = str(lease_owner_id or "").strip()
    normalized_lease_epoch = int(lease_epoch or 0)
    latest: dict[str, Any] = {}
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != event_type:
            continue
        if normalized_node_id and str(event.get("node_id") or "").strip() != normalized_node_id:
            continue
        payload = dict(event.get("payload") or {})
        payload_launch_event_id = str(payload.get("launch_event_id") or "").strip()
        if normalized_launch_event_id and payload_launch_event_id != normalized_launch_event_id:
            continue
        payload_lease_owner_id = str(payload.get("lease_owner_id") or "").strip()
        if normalized_lease_owner_id and payload_lease_owner_id and payload_lease_owner_id != normalized_lease_owner_id:
            continue
        try:
            payload_lease_epoch = int(payload.get("lease_epoch") or 0)
        except (TypeError, ValueError):
            payload_lease_epoch = 0
        if normalized_lease_epoch > 0 and payload_lease_epoch > 0 and payload_lease_epoch != normalized_lease_epoch:
            continue
        latest = dict(event or {})
    return latest


def _repo_tree_cleanup_receipt(runtime_root: Path, *, envelope_id: str) -> dict[str, Any]:
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        return {}
    receipt_ref = repo_tree_cleanup_receipt_ref(state_root=runtime_root, envelope_id=normalized_envelope_id)
    if not receipt_ref.exists():
        return {}
    try:
        return json.loads(receipt_ref.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _heavy_object_reclamation_receipt(runtime_root: Path, *, envelope_id: str) -> dict[str, Any]:
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        return {}
    receipt_ref = heavy_object_reclamation_receipt_ref(state_root=runtime_root, envelope_id=normalized_envelope_id)
    if not receipt_ref.exists():
        return {}
    try:
        return json.loads(receipt_ref.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _heavy_object_authority_gap_repo_remediation_receipt(runtime_root: Path, *, envelope_id: str) -> dict[str, Any]:
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        return {}
    receipt_ref = heavy_object_authority_gap_repo_remediation_receipt_ref(
        state_root=runtime_root,
        envelope_id=normalized_envelope_id,
    )
    if not receipt_ref.exists():
        return {}
    try:
        return json.loads(receipt_ref.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _heavy_object_authority_gap_audit_receipt(runtime_root: Path, *, envelope_id: str) -> dict[str, Any]:
    normalized_envelope_id = str(envelope_id or "").strip()
    if not normalized_envelope_id:
        return {}
    receipt_ref = heavy_object_authority_gap_audit_receipt_ref(
        state_root=runtime_root,
        envelope_id=normalized_envelope_id,
    )
    if not receipt_ref.exists():
        return {}
    try:
        return json.loads(receipt_ref.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _latest_matching_heavy_object_observation(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str,
    object_ref: str,
) -> dict[str, Any]:
    matches = _matching_heavy_object_observation_events(
        runtime_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        object_id=object_id,
        object_ref=object_ref,
    )
    return dict(matches[-1] or {}) if matches else {}


def _resolve_effective_heavy_object_query_identity(
    runtime_root: Path,
    *,
    object_id: str = "",
    object_ref: str = "",
) -> tuple[str, str]:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    if normalized_object_id:
        return normalized_object_id, normalized_object_ref
    if not normalized_object_ref:
        return normalized_object_id, normalized_object_ref

    exact_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id="",
        object_ref=normalized_object_ref,
    )
    exact_registration_payload = dict(exact_registration.get("payload") or {})
    if str(exact_registration_payload.get("object_id") or "").strip():
        return str(exact_registration_payload.get("object_id") or "").strip(), normalized_object_ref

    exact_observation = _latest_matching_heavy_object_observation(
        runtime_root,
        object_id="",
        object_ref=normalized_object_ref,
    )
    exact_observation_payload = dict(exact_observation.get("payload") or {})
    if str(exact_observation_payload.get("object_id") or "").strip():
        return str(exact_observation_payload.get("object_id") or "").strip(), normalized_object_ref

    exact_discovery_request = _matching_heavy_object_discovery_request_events(
        runtime_root,
        object_id="",
        object_ref=normalized_object_ref,
    )
    if exact_discovery_request:
        exact_request_payload = dict(exact_discovery_request[-1].get("payload") or {})
        if str(exact_request_payload.get("object_id") or "").strip():
            return str(exact_request_payload.get("object_id") or "").strip(), normalized_object_ref

    return normalized_object_id, normalized_object_ref


def _matching_heavy_object_observation_events(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str = "",
    object_ref: str = "",
) -> list[dict[str, Any]]:
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    matches: list[dict[str, Any]] = []
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_observed":
            continue
        payload = dict(event.get("payload") or {})
        payload_runtime_name = str(payload.get("runtime_name") or "").strip()
        payload_object_kind = str(payload.get("object_kind") or "").strip()
        payload_object_id = str(payload.get("object_id") or "").strip()
        payload_object_ref = str(payload.get("object_ref") or "").strip()
        payload_object_refs = [str(item) for item in list(payload.get("object_refs") or [])]
        if normalized_runtime_name and payload_runtime_name != normalized_runtime_name:
            continue
        if normalized_object_kind and payload_object_kind != normalized_object_kind:
            continue
        if normalized_object_id:
            if payload_object_id != normalized_object_id:
                continue
        elif normalized_object_ref and (
            payload_object_ref != normalized_object_ref and normalized_object_ref not in payload_object_refs
        ):
            continue
        matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_registration_events(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str = "",
    object_ref: str = "",
    repo_root: str = "",
) -> list[dict[str, Any]]:
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    normalized_repo_root = str(repo_root or "").strip()
    matches: list[dict[str, Any]] = []
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_registered":
            continue
        payload = dict(event.get("payload") or {})
        payload_runtime_name = str(payload.get("runtime_name") or "").strip()
        payload_object_kind = str(payload.get("object_kind") or "").strip()
        payload_object_id = str(payload.get("object_id") or "").strip()
        payload_object_ref = str(payload.get("object_ref") or "").strip()
        payload_repo_root = str(payload.get("repo_root") or "").strip()
        if normalized_runtime_name and payload_runtime_name != normalized_runtime_name:
            continue
        if normalized_object_kind and payload_object_kind != normalized_object_kind:
            continue
        if normalized_object_id:
            if payload_object_id != normalized_object_id:
                continue
        elif normalized_object_ref and payload_object_ref != normalized_object_ref:
            continue
        if normalized_repo_root and payload_repo_root != normalized_repo_root:
            continue
        matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_discovery_observation_events(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str = "",
    object_ref: str = "",
    repo_root: str = "",
    discovery_kind: str = "",
) -> list[dict[str, Any]]:
    normalized_repo_root = str(repo_root or "").strip()
    normalized_discovery_kind = str(discovery_kind or "").strip()
    matches: list[dict[str, Any]] = []
    for event in _matching_heavy_object_observation_events(
        runtime_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        object_id=object_id,
        object_ref=object_ref,
    ):
        payload = dict(event.get("payload") or {})
        if not str(payload.get("discovery_root") or "").strip() and not str(payload.get("discovery_receipt_ref") or "").strip():
            continue
        if normalized_repo_root and str(payload.get("repo_root") or "").strip() != normalized_repo_root:
            continue
        if normalized_discovery_kind and str(payload.get("observation_kind") or "").strip() != normalized_discovery_kind:
            continue
        matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_discovery_request_events(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str = "",
    object_ref: str = "",
    repo_root: str = "",
    discovery_kind: str = "",
) -> list[dict[str, Any]]:
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    normalized_repo_root = str(repo_root or "").strip()
    normalized_discovery_kind = str(discovery_kind or "").strip()
    matches: list[dict[str, Any]] = []
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_discovery_requested":
            continue
        payload = dict(event.get("payload") or {})
        if normalized_runtime_name and str(payload.get("runtime_name") or "").strip() != normalized_runtime_name:
            continue
        if normalized_object_kind and str(payload.get("object_kind") or "").strip() != normalized_object_kind:
            continue
        if normalized_repo_root and str(payload.get("repo_root") or "").strip() != normalized_repo_root:
            continue
        if normalized_discovery_kind and str(payload.get("discovery_kind") or "").strip() != normalized_discovery_kind:
            continue
        payload_object_id = str(payload.get("object_id") or "").strip()
        payload_object_ref = str(payload.get("object_ref") or "").strip()
        if normalized_object_id:
            if payload_object_id != normalized_object_id:
                continue
        elif normalized_object_ref and payload_object_ref != normalized_object_ref:
            continue
        matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_authority_gap_remediation_result_events(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str = "",
    object_ref: str = "",
    repo_root: str = "",
) -> list[dict[str, Any]]:
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    normalized_repo_root = str(repo_root or "").strip()
    matches: list[dict[str, Any]] = []
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_authority_gap_remediation_settled":
            continue
        payload = dict(event.get("payload") or {})
        if normalized_runtime_name and str(payload.get("runtime_name") or "").strip() != normalized_runtime_name:
            continue
        if normalized_object_kind and str(payload.get("object_kind") or "").strip() != normalized_object_kind:
            continue
        if normalized_repo_root and str(payload.get("repo_root") or "").strip() != normalized_repo_root:
            continue
        payload_object_id = str(payload.get("object_id") or "").strip()
        payload_object_ref = str(payload.get("object_ref") or "").strip()
        if normalized_object_id:
            if payload_object_id != normalized_object_id:
                continue
        elif normalized_object_ref and payload_object_ref != normalized_object_ref:
            continue
        matches.append(dict(event or {}))
    return matches


def _heavy_object_candidate_paths(
    repo_root: Path,
    *,
    object_kind: str,
) -> list[Path]:
    normalized_object_kind = str(object_kind or "").strip()
    candidates: list[Path] = []
    seen: set[str] = set()
    if normalized_object_kind in {"", "mathlib_pack"}:
        try:
            pack_paths = sorted(repo_root.rglob("*.pack"))
        except FileNotFoundError:
            pack_paths = []
        except OSError:
            pack_paths = []
        for path in pack_paths:
            try:
                resolved = path.expanduser().resolve()
            except OSError:
                continue
            if not resolved.is_file():
                continue
            if path_within_heavy_object_store(repo_root=repo_root, candidate=resolved):
                continue
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(resolved)
    elif normalized_object_kind == "publish_tree":
        for path in iter_publish_tree_candidate_roots(repo_root):
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
    elif normalized_object_kind == "runtime_heavy_tree":
        for path in iter_runtime_heavy_tree_candidate_roots(repo_root):
            key = str(path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
    return candidates


def _repo_anchor_runtime_root_for_query(runtime_root: Path) -> Path | None:
    repo_root = repo_root_from_runtime_root(state_root=runtime_root)
    if repo_root is None:
        return None
    return (repo_root / ".loop").resolve()


def _payload_matches_heavy_object_scope(
    payload: dict[str, Any],
    *,
    runtime_name: str,
    object_kind: str,
    repo_root: str,
) -> bool:
    if runtime_name and str(payload.get("runtime_name") or "").strip() != runtime_name:
        return False
    if object_kind and str(payload.get("object_kind") or "").strip() != object_kind:
        return False
    if repo_root and str(payload.get("repo_root") or "").strip() != repo_root:
        return False
    return True


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


def _runtime_root_has_heavy_object_authority_facts(
    runtime_root: Path,
    *,
    runtime_name: str,
    object_kind: str,
    repo_root: str,
) -> bool:
    for event in iter_committed_events(runtime_root):
        event_type = str(event.get("event_type") or "").strip()
        payload = dict(event.get("payload") or {})
        if event_type == "heavy_object_observed":
            if not str(payload.get("discovery_root") or "").strip() and not str(payload.get("discovery_receipt_ref") or "").strip():
                continue
        elif event_type not in {
            "heavy_object_registered",
            "heavy_object_discovery_requested",
            "heavy_object_reference_attached",
            "heavy_object_reference_released",
            "heavy_object_authority_gap_audit_requested",
            "heavy_object_authority_gap_audited",
            "heavy_object_authority_gap_remediation_settled",
            "heavy_object_authority_gap_repo_remediation_requested",
            "heavy_object_authority_gap_repo_remediation_settled",
        }:
            continue
        if _payload_matches_heavy_object_scope(
            payload,
            runtime_name=runtime_name,
            object_kind=object_kind,
            repo_root=repo_root,
        ):
            return True
    return False


def _select_heavy_object_authority_runtime_root(
    runtime_root: Path,
    *,
    runtime_name: str,
    object_kind: str,
    repo_root: str,
) -> Path:
    anchor_root = _repo_anchor_runtime_root_for_query(runtime_root)
    if anchor_root is None or anchor_root == runtime_root:
        return runtime_root
    if _runtime_root_has_heavy_object_authority_facts(
        runtime_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        repo_root=repo_root,
    ):
        return runtime_root
    if _runtime_root_has_heavy_object_authority_facts(
        anchor_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        repo_root=repo_root,
    ):
        return anchor_root
    return runtime_root


def _matching_heavy_object_pin_events(
    runtime_root: Path,
    *,
    object_id: str,
    object_ref: str,
) -> list[dict[str, Any]]:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    matches: list[dict[str, Any]] = []
    if not normalized_object_id and not normalized_object_ref:
        return matches
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_pinned":
            continue
        payload = dict(event.get("payload") or {})
        if _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_pin_release_events(
    runtime_root: Path,
    *,
    object_id: str,
    object_ref: str,
) -> list[dict[str, Any]]:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    matches: list[dict[str, Any]] = []
    if not normalized_object_id and not normalized_object_ref:
        return matches
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_pin_released":
            continue
        payload = dict(event.get("payload") or {})
        if _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
        ):
            matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_reference_events(
    runtime_root: Path,
    *,
    object_id: str = "",
    object_ref: str = "",
    reference_ref: str = "",
    reference_holder_kind: str = "",
    reference_holder_ref: str = "",
    reference_kind: str = "",
) -> list[dict[str, Any]]:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    normalized_reference_ref = str(reference_ref or "").strip()
    normalized_reference_holder_kind = str(reference_holder_kind or "").strip()
    normalized_reference_holder_ref = str(reference_holder_ref or "").strip()
    normalized_reference_kind = str(reference_kind or "").strip()
    matches: list[dict[str, Any]] = []
    if not any(
        (
            normalized_object_id,
            normalized_object_ref,
            normalized_reference_ref,
            normalized_reference_holder_kind,
            normalized_reference_holder_ref,
            normalized_reference_kind,
        )
    ):
        return matches
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_reference_attached":
            continue
        payload = dict(event.get("payload") or {})
        payload_reference_ref = str(payload.get("reference_ref") or "").strip()
        payload_reference_holder_kind = str(payload.get("reference_holder_kind") or "").strip()
        payload_reference_holder_ref = str(payload.get("reference_holder_ref") or "").strip()
        payload_reference_kind = str(payload.get("reference_kind") or "").strip()
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
            reference_ref_fallback="reference_ref",
        ):
            if normalized_object_id or normalized_object_ref:
                continue
        if normalized_reference_ref and payload_reference_ref != normalized_reference_ref:
            continue
        if normalized_reference_holder_kind and payload_reference_holder_kind != normalized_reference_holder_kind:
            continue
        if normalized_reference_holder_ref and payload_reference_holder_ref != normalized_reference_holder_ref:
            continue
        if normalized_reference_kind and payload_reference_kind != normalized_reference_kind:
            continue
        matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_reference_release_events(
    runtime_root: Path,
    *,
    object_id: str = "",
    object_ref: str = "",
    reference_ref: str = "",
    reference_holder_kind: str = "",
    reference_holder_ref: str = "",
    reference_kind: str = "",
) -> list[dict[str, Any]]:
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(object_ref or "").strip()
    normalized_reference_ref = str(reference_ref or "").strip()
    normalized_reference_holder_kind = str(reference_holder_kind or "").strip()
    normalized_reference_holder_ref = str(reference_holder_ref or "").strip()
    normalized_reference_kind = str(reference_kind or "").strip()
    matches: list[dict[str, Any]] = []
    if not any(
        (
            normalized_object_id,
            normalized_object_ref,
            normalized_reference_ref,
            normalized_reference_holder_kind,
            normalized_reference_holder_ref,
            normalized_reference_kind,
        )
    ):
        return matches
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_reference_released":
            continue
        payload = dict(event.get("payload") or {})
        payload_reference_ref = str(payload.get("reference_ref") or "").strip()
        payload_reference_holder_kind = str(payload.get("reference_holder_kind") or "").strip()
        payload_reference_holder_ref = str(payload.get("reference_holder_ref") or "").strip()
        payload_reference_kind = str(payload.get("reference_kind") or "").strip()
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
            reference_ref_fallback="reference_ref",
        ):
            if normalized_object_id or normalized_object_ref:
                continue
        if normalized_reference_ref and payload_reference_ref != normalized_reference_ref:
            continue
        if normalized_reference_holder_kind and payload_reference_holder_kind != normalized_reference_holder_kind:
            continue
        if normalized_reference_holder_ref and payload_reference_holder_ref != normalized_reference_holder_ref:
            continue
        if normalized_reference_kind and payload_reference_kind != normalized_reference_kind:
            continue
        matches.append(dict(event or {}))
    return matches


def _matching_heavy_object_replacement_events(
    runtime_root: Path,
    *,
    replacement_object_id: str,
    replacement_object_ref: str,
) -> list[dict[str, Any]]:
    normalized_replacement_id = str(replacement_object_id or "").strip()
    normalized_replacement_ref = str(replacement_object_ref or "").strip()
    matches: list[dict[str, Any]] = []
    if not normalized_replacement_id and not normalized_replacement_ref:
        return matches
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_superseded":
            continue
        payload = dict(event.get("payload") or {})
        payload_replacement_id = str(payload.get("replacement_object_id") or "").strip()
        payload_replacement_ref = str(payload.get("replacement_object_ref") or "").strip()
        if normalized_replacement_id and payload_replacement_id == normalized_replacement_id:
            matches.append(dict(event or {}))
            continue
        if normalized_replacement_ref and payload_replacement_ref == normalized_replacement_ref:
            matches.append(dict(event or {}))
    return matches


def _node_projection_summary(
    runtime_root: Path,
    *,
    node_id: str,
    blocked_reason: str = "",
) -> dict[str, Any]:
    node_id = str(node_id or "").strip()
    path = runtime_root / "state" / f"{node_id}.json"
    metadata = _projection_metadata(path)
    _exists, payload = _projection_payload(path)
    runtime_state = normalize_runtime_state(dict(payload.get("runtime_state") or {}))
    return {
        "node_id": node_id,
        "exists": bool(metadata.get("exists")),
        "status": str(payload.get("status") or ""),
        "blocked_reason": str(blocked_reason or ""),
        "runtime_attachment_state": str(runtime_state.get("attachment_state") or ""),
        "runtime_observed_at": str(runtime_state.get("observed_at") or ""),
        "runtime_summary": str(runtime_state.get("summary") or ""),
        "runtime_observation_kind": str(runtime_state.get("observation_kind") or ""),
        "last_applied_seq": int(metadata.get("last_applied_seq") or 0),
        "projection_bundle_ref": str(metadata.get("projection_bundle_ref") or ""),
        "source_event_journal_ref": str(metadata.get("source_event_journal_ref") or ""),
    }


def query_topology_command_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted topology command via audit, committed events, visible projection state, and node lineage."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="topology_command_accepted",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    mutation_payload = accepted_payload.get("topology_mutation") or accepted_payload
    review = dict(accepted_payload.get("review") or {})
    mutation = TopologyMutation.from_dict(dict(mutation_payload or {})) if mutation_payload else TopologyMutation(
        kind="",
        source_node_id="",
    )
    command_kind = str(mutation.kind or "")
    source_node_id = str(mutation.source_node_id or accepted_audit.get("source") or "")
    target_node_ids = topology_target_node_ids(mutation, review=review)

    consistency = query_projection_consistency_view(runtime_root)
    kernel_state = load_kernel_state(runtime_root).to_dict()
    kernel_nodes = dict(kernel_state.get("nodes") or {})
    blocked_reasons = {str(k): str(v or "") for k, v in dict(kernel_state.get("blocked_reasons") or {}).items()}
    source_node = (
        _node_projection_summary(
            runtime_root,
            node_id=source_node_id,
            blocked_reason=blocked_reasons.get(source_node_id, ""),
        )
        if source_node_id
        else {}
    )
    target_nodes = [
        _node_projection_summary(
            runtime_root,
            node_id=node_id,
            blocked_reason=blocked_reasons.get(node_id, ""),
        )
        for node_id in target_node_ids
    ]
    source_node_lineage = (
        query_node_lineage_trace_view(runtime_root, node_id=source_node_id)
        if source_node_id
        else {}
    )
    target_node_lineages = [
        query_node_lineage_trace_view(runtime_root, node_id=node_id)
        for node_id in target_node_ids
        if node_id
    ]

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_topology_command_event")
    if source_node_id and source_node_id not in kernel_nodes:
        gaps.append("projection_not_materialized")
    elif any(target_node_id and target_node_id not in kernel_nodes for target_node_id in target_node_ids):
        gaps.append("projection_not_materialized")

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "command_kind": command_kind,
        "source_node_id": source_node_id,
        "target_node_ids": target_node_ids,
    }
    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "command_kind": command_kind,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "projection_visibility": str(consistency.get("visibility_state") or ""),
        "projection_effect": {
            "source_node": source_node,
            "target_nodes": target_nodes,
        },
        "source_node_lineage": source_node_lineage,
        "target_node_lineages": target_node_lineages,
        "gaps": gaps,
    }


def query_repo_tree_cleanup_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted repo-tree cleanup command via audit, committed events, and receipt history."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="repo_tree_cleanup_requested",
        envelope_id=normalized_envelope_id,
    )
    receipt = _repo_tree_cleanup_receipt(runtime_root, envelope_id=normalized_envelope_id)

    accepted_payload = dict(accepted_audit.get("payload") or {})
    requested_repo_roots = [str(item) for item in list(accepted_payload.get("repo_roots") or [])]
    cleanup_effect = {
        "tree_root": str(receipt.get("tree_root") or accepted_payload.get("tree_root") or ""),
        "repo_roots": [str(item) for item in list(receipt.get("repo_roots") or requested_repo_roots)],
        "repo_count": int(receipt.get("repo_count") or len(requested_repo_roots)),
        "quiesced_repo_count": int(receipt.get("quiesced_repo_count") or 0),
        "failed_repo_count": int(receipt.get("failed_repo_count") or 0),
        "quiesced": bool(receipt.get("quiesced")),
        "repo_receipts": list(receipt.get("repo_receipts") or []),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_repo_tree_cleanup_event")
    if not receipt:
        gaps.append("missing_repo_tree_cleanup_receipt")

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "tree_root": str(accepted_payload.get("tree_root") or ""),
        "repo_roots": requested_repo_roots,
        "include_anchor_repo_root": bool(accepted_payload.get("include_anchor_repo_root")),
    }
    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "cleanup_receipt_present": bool(receipt),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "cleanup_effect": cleanup_effect,
        "gaps": gaps,
    }


def query_heavy_object_registration_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object registration command via audit, committed events, and matching observations."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_registered",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    registered_payload = dict(canonical_event.get("payload") or {})
    object_id = str(registered_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(registered_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    matching_observation = _latest_matching_heavy_object_observation(
        runtime_root,
        object_id=object_id,
        object_ref=object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "byte_size": int(accepted_payload.get("byte_size") or 0),
        "registration_kind": str(accepted_payload.get("registration_kind") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    registered_object = {
        "object_id": object_id,
        "object_kind": str(registered_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "byte_size": int(registered_payload.get("byte_size") or accepted_payload.get("byte_size") or 0),
        "owner_node_id": str(registered_payload.get("owner_node_id") or accepted_payload.get("owner_node_id") or ""),
        "runtime_name": str(registered_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(registered_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
        "registration_kind": str(registered_payload.get("registration_kind") or accepted_payload.get("registration_kind") or ""),
        "trigger_kind": str(registered_payload.get("trigger_kind") or accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(registered_payload.get("trigger_ref") or accepted_payload.get("trigger_ref") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_registered_event")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_observation_present": bool(matching_observation),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "registered_object": registered_object,
        "latest_matching_observation": (
            {**_event_summary(matching_observation), "payload": dict(matching_observation.get("payload") or {})}
            if matching_observation
            else {}
        ),
        "gaps": gaps,
    }


def query_heavy_object_discovery_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object discovery command via audit, committed facts, and deterministic receipt."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_request_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_discovery_requested",
        envelope_id=normalized_envelope_id,
    )
    canonical_observation_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_observed",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    request_payload = dict(canonical_request_event.get("payload") or {})
    observation_payload = dict(canonical_observation_event.get("payload") or {})
    object_id = str(
        observation_payload.get("object_id")
        or request_payload.get("object_id")
        or accepted_payload.get("object_id")
        or ""
    )
    object_ref = str(
        observation_payload.get("object_ref")
        or request_payload.get("object_ref")
        or accepted_payload.get("object_ref")
        or ""
    )
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )

    receipt_ref = (
        runtime_root / "artifacts" / "heavy_object_discovery" / f"{normalized_envelope_id}.json"
    ).resolve()
    receipt_payload = {}
    if receipt_ref.exists():
        try:
            receipt_payload = json.loads(receipt_ref.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            receipt_payload = {}

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "discovery_root": str(accepted_payload.get("discovery_root") or ""),
        "discovery_kind": str(accepted_payload.get("discovery_kind") or ""),
    }
    discovery_effect = {
        "object_id": object_id,
        "object_kind": str(
            observation_payload.get("object_kind")
            or request_payload.get("object_kind")
            or accepted_payload.get("object_kind")
            or ""
        ),
        "object_ref": object_ref,
        "object_refs": [str(item) for item in list(observation_payload.get("object_refs") or receipt_payload.get("discovered_refs") or [])],
        "duplicate_count": int(observation_payload.get("duplicate_count") or receipt_payload.get("duplicate_count") or 0),
        "total_bytes": int(observation_payload.get("total_bytes") or receipt_payload.get("total_bytes") or 0),
        "discovery_root": str(
            receipt_payload.get("discovery_root")
            or request_payload.get("discovery_root")
            or accepted_payload.get("discovery_root")
            or ""
        ),
        "discovery_kind": str(
            receipt_payload.get("discovery_kind")
            or observation_payload.get("observation_kind")
            or request_payload.get("discovery_kind")
            or accepted_payload.get("discovery_kind")
            or ""
        ),
        "runtime_name": str(
            observation_payload.get("runtime_name")
            or receipt_payload.get("runtime_name")
            or request_payload.get("runtime_name")
            or accepted_payload.get("runtime_name")
            or ""
        ),
        "repo_root": str(
            observation_payload.get("repo_root")
            or receipt_payload.get("repo_root")
            or request_payload.get("repo_root")
            or accepted_payload.get("repo_root")
            or ""
        ),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_request_event:
        gaps.append("missing_canonical_heavy_object_discovery_requested_event")
    if not receipt_payload:
        gaps.append("missing_heavy_object_discovery_receipt")
    if not canonical_observation_event:
        gaps.append("missing_canonical_heavy_object_observed_event")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_request_event_present": bool(canonical_request_event),
        "canonical_observation_event_present": bool(canonical_observation_event),
        "discovery_receipt_present": bool(receipt_payload),
        "matching_registration_present": bool(matching_registration),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_request_event": _event_summary(canonical_request_event) if canonical_request_event else {},
        "canonical_observation_event": _event_summary(canonical_observation_event) if canonical_observation_event else {},
        "discovery_effect": discovery_effect,
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "discovery_receipt": receipt_payload,
        "gaps": gaps,
    }


def query_heavy_object_observation_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object observation command via audit, committed events, and matching registration."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_observed",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    observed_payload = dict(canonical_event.get("payload") or {})
    object_id = str(observed_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(observed_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "object_refs": [str(item) for item in list(accepted_payload.get("object_refs") or [])],
        "duplicate_count": int(accepted_payload.get("duplicate_count") or 0),
        "total_bytes": int(accepted_payload.get("total_bytes") or 0),
        "observation_kind": str(accepted_payload.get("observation_kind") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    observed_object = {
        "object_id": object_id,
        "object_kind": str(observed_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "object_refs": [str(item) for item in list(observed_payload.get("object_refs") or accepted_payload.get("object_refs") or [])],
        "duplicate_count": int(observed_payload.get("duplicate_count") or accepted_payload.get("duplicate_count") or 0),
        "total_bytes": int(observed_payload.get("total_bytes") or accepted_payload.get("total_bytes") or 0),
        "observation_kind": str(observed_payload.get("observation_kind") or accepted_payload.get("observation_kind") or ""),
        "owner_node_id": str(observed_payload.get("owner_node_id") or accepted_payload.get("owner_node_id") or ""),
        "runtime_name": str(observed_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(observed_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
        "trigger_kind": str(observed_payload.get("trigger_kind") or accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(observed_payload.get("trigger_ref") or accepted_payload.get("trigger_ref") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_observed_event")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_registration_present": bool(matching_registration),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "observed_object": observed_object,
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_reference_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object reference command via audit, committed events, and matching registration."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_reference_attached",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    referenced_payload = dict(canonical_event.get("payload") or {})
    object_id = str(referenced_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(referenced_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "reference_ref": str(accepted_payload.get("reference_ref") or ""),
        "reference_holder_kind": str(accepted_payload.get("reference_holder_kind") or ""),
        "reference_holder_ref": str(accepted_payload.get("reference_holder_ref") or ""),
        "reference_kind": str(accepted_payload.get("reference_kind") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    referenced_object = {
        "object_id": object_id,
        "object_kind": str(referenced_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "reference_ref": str(referenced_payload.get("reference_ref") or accepted_payload.get("reference_ref") or ""),
        "reference_holder_kind": str(
            referenced_payload.get("reference_holder_kind") or accepted_payload.get("reference_holder_kind") or ""
        ),
        "reference_holder_ref": str(
            referenced_payload.get("reference_holder_ref") or accepted_payload.get("reference_holder_ref") or ""
        ),
        "reference_kind": str(referenced_payload.get("reference_kind") or accepted_payload.get("reference_kind") or ""),
        "runtime_name": str(referenced_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(referenced_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
        "trigger_kind": str(referenced_payload.get("trigger_kind") or accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(referenced_payload.get("trigger_ref") or accepted_payload.get("trigger_ref") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_reference_attached_event")
    if not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_registration_present": bool(matching_registration),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "referenced_object": referenced_object,
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_reference_release_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object reference-release command via audit, committed events, matching reference, and registration."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_reference_released",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    released_payload = dict(canonical_event.get("payload") or {})
    object_id = str(released_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(released_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    reference_ref = str(released_payload.get("reference_ref") or accepted_payload.get("reference_ref") or "")
    reference_holder_kind = str(
        released_payload.get("reference_holder_kind") or accepted_payload.get("reference_holder_kind") or ""
    )
    reference_holder_ref = str(
        released_payload.get("reference_holder_ref") or accepted_payload.get("reference_holder_ref") or ""
    )
    reference_kind = str(released_payload.get("reference_kind") or accepted_payload.get("reference_kind") or "")
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )
    matching_reference_events = _matching_heavy_object_reference_events(
        runtime_root,
        object_id=object_id,
        object_ref=object_ref,
        reference_ref=reference_ref,
        reference_holder_kind=reference_holder_kind,
        reference_holder_ref=reference_holder_ref,
        reference_kind=reference_kind,
    )
    matching_reference = dict(matching_reference_events[-1] or {}) if matching_reference_events else {}

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "reference_ref": str(accepted_payload.get("reference_ref") or ""),
        "reference_holder_kind": str(accepted_payload.get("reference_holder_kind") or ""),
        "reference_holder_ref": str(accepted_payload.get("reference_holder_ref") or ""),
        "reference_kind": str(accepted_payload.get("reference_kind") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    released_reference = {
        "object_id": object_id,
        "object_kind": str(released_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "reference_ref": reference_ref,
        "reference_holder_kind": reference_holder_kind,
        "reference_holder_ref": reference_holder_ref,
        "reference_kind": reference_kind,
        "runtime_name": str(released_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(released_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
        "trigger_kind": str(released_payload.get("trigger_kind") or accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(released_payload.get("trigger_ref") or accepted_payload.get("trigger_ref") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_reference_released_event")
    if not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")
    if not matching_reference:
        gaps.append("missing_matching_heavy_object_reference")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_registration_present": bool(matching_registration),
        "matching_reference_present": bool(matching_reference),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "released_reference": released_reference,
        "matching_reference": _event_summary(matching_reference) if matching_reference else {},
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_pin_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object pin command via audit, committed events, and matching registration."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_pinned",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    pinned_payload = dict(canonical_event.get("payload") or {})
    object_id = str(pinned_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(pinned_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "pin_holder_id": str(accepted_payload.get("pin_holder_id") or ""),
        "pin_kind": str(accepted_payload.get("pin_kind") or ""),
    }
    pinned_object = {
        "object_id": object_id,
        "object_kind": str(pinned_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "pin_holder_id": str(pinned_payload.get("pin_holder_id") or accepted_payload.get("pin_holder_id") or ""),
        "pin_kind": str(pinned_payload.get("pin_kind") or accepted_payload.get("pin_kind") or ""),
        "owner_node_id": str(pinned_payload.get("owner_node_id") or accepted_payload.get("owner_node_id") or ""),
        "runtime_name": str(pinned_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(pinned_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_pinned_event")
    if not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_registration_present": bool(matching_registration),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "pinned_object": pinned_object,
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_pin_release_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object pin-release command via audit, committed events, matching pin, and registration."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_pin_released",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    released_payload = dict(canonical_event.get("payload") or {})
    object_id = str(released_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(released_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    pin_holder_id = str(released_payload.get("pin_holder_id") or accepted_payload.get("pin_holder_id") or "")
    pin_kind = str(released_payload.get("pin_kind") or accepted_payload.get("pin_kind") or "")
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )
    matching_pin = next(
        (
            dict(event)
            for event in _matching_heavy_object_pin_events(
                runtime_root,
                object_id=object_id,
                object_ref=object_ref,
            )
            if (
                str(dict(event.get("payload") or {}).get("pin_holder_id") or "") == pin_holder_id
                and str(dict(event.get("payload") or {}).get("pin_kind") or "") == pin_kind
            )
        ),
        {},
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "pin_holder_id": str(accepted_payload.get("pin_holder_id") or ""),
        "pin_kind": str(accepted_payload.get("pin_kind") or ""),
    }
    released_pin = {
        "object_id": object_id,
        "object_kind": str(released_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "pin_holder_id": pin_holder_id,
        "pin_kind": pin_kind,
        "owner_node_id": str(released_payload.get("owner_node_id") or accepted_payload.get("owner_node_id") or ""),
        "runtime_name": str(released_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(released_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_pin_released_event")
    if not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")
    if not matching_pin:
        gaps.append("missing_matching_heavy_object_pin")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_registration_present": bool(matching_registration),
        "matching_pin_present": bool(matching_pin),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "released_pin": released_pin,
        "matching_pin": _event_summary(matching_pin) if matching_pin else {},
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_supersession_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object supersession command via audit, committed events, and matching registrations."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_superseded",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    supersession_payload = dict(canonical_event.get("payload") or {})
    superseded_object_id = str(supersession_payload.get("superseded_object_id") or accepted_payload.get("superseded_object_id") or "")
    superseded_object_ref = str(supersession_payload.get("superseded_object_ref") or accepted_payload.get("superseded_object_ref") or "")
    replacement_object_id = str(supersession_payload.get("replacement_object_id") or accepted_payload.get("replacement_object_id") or "")
    replacement_object_ref = str(supersession_payload.get("replacement_object_ref") or accepted_payload.get("replacement_object_ref") or "")
    superseded_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=superseded_object_id,
        object_ref=superseded_object_ref,
    )
    replacement_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=replacement_object_id,
        object_ref=replacement_object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "superseded_object_id": str(accepted_payload.get("superseded_object_id") or ""),
        "superseded_object_kind": str(accepted_payload.get("superseded_object_kind") or ""),
        "superseded_object_ref": str(accepted_payload.get("superseded_object_ref") or ""),
        "replacement_object_id": str(accepted_payload.get("replacement_object_id") or ""),
        "replacement_object_kind": str(accepted_payload.get("replacement_object_kind") or ""),
        "replacement_object_ref": str(accepted_payload.get("replacement_object_ref") or ""),
        "supersession_kind": str(accepted_payload.get("supersession_kind") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    supersession = {
        "superseded_object_id": superseded_object_id,
        "superseded_object_kind": str(supersession_payload.get("superseded_object_kind") or accepted_payload.get("superseded_object_kind") or ""),
        "superseded_object_ref": superseded_object_ref,
        "replacement_object_id": replacement_object_id,
        "replacement_object_kind": str(supersession_payload.get("replacement_object_kind") or accepted_payload.get("replacement_object_kind") or ""),
        "replacement_object_ref": replacement_object_ref,
        "supersession_kind": str(supersession_payload.get("supersession_kind") or accepted_payload.get("supersession_kind") or ""),
        "owner_node_id": str(supersession_payload.get("owner_node_id") or accepted_payload.get("owner_node_id") or ""),
        "runtime_name": str(supersession_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(supersession_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
        "trigger_kind": str(supersession_payload.get("trigger_kind") or accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(supersession_payload.get("trigger_ref") or accepted_payload.get("trigger_ref") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_superseded_event")
    if not superseded_registration:
        gaps.append("missing_superseded_heavy_object_registration")
    if not replacement_registration:
        gaps.append("missing_replacement_heavy_object_registration")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "superseded_registration_present": bool(superseded_registration),
        "replacement_registration_present": bool(replacement_registration),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "supersession": supersession,
        "superseded_registration": _event_summary(superseded_registration) if superseded_registration else {},
        "replacement_registration": _event_summary(replacement_registration) if replacement_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_gc_eligibility_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object GC-eligibility command via audit, committed events, and supporting lifecycle facts."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_gc_eligible",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    gc_payload = dict(canonical_event.get("payload") or {})
    object_id = str(gc_payload.get("object_id") or accepted_payload.get("object_id") or "")
    object_ref = str(gc_payload.get("object_ref") or accepted_payload.get("object_ref") or "")
    superseded_by_object_id = str(gc_payload.get("superseded_by_object_id") or accepted_payload.get("superseded_by_object_id") or "")
    superseded_by_object_ref = str(gc_payload.get("superseded_by_object_ref") or accepted_payload.get("superseded_by_object_ref") or "")
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )
    supporting_supersession = _latest_matching_heavy_object_supersession(
        runtime_root,
        superseded_object_id=object_id,
        superseded_object_ref=object_ref,
        replacement_object_id=superseded_by_object_id,
        replacement_object_ref=superseded_by_object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "eligibility_kind": str(accepted_payload.get("eligibility_kind") or ""),
        "superseded_by_object_id": str(accepted_payload.get("superseded_by_object_id") or ""),
        "superseded_by_object_ref": str(accepted_payload.get("superseded_by_object_ref") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    eligible_object = {
        "object_id": object_id,
        "object_kind": str(gc_payload.get("object_kind") or accepted_payload.get("object_kind") or ""),
        "object_ref": object_ref,
        "eligibility_kind": str(gc_payload.get("eligibility_kind") or accepted_payload.get("eligibility_kind") or ""),
        "superseded_by_object_id": superseded_by_object_id,
        "superseded_by_object_ref": superseded_by_object_ref,
        "owner_node_id": str(gc_payload.get("owner_node_id") or accepted_payload.get("owner_node_id") or ""),
        "runtime_name": str(gc_payload.get("runtime_name") or accepted_payload.get("runtime_name") or ""),
        "repo_root": str(gc_payload.get("repo_root") or accepted_payload.get("repo_root") or ""),
        "trigger_kind": str(gc_payload.get("trigger_kind") or accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(gc_payload.get("trigger_ref") or accepted_payload.get("trigger_ref") or ""),
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_gc_eligible_event")
    if not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")
    if superseded_by_object_id and not supporting_supersession:
        gaps.append("missing_supporting_heavy_object_supersession")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "matching_registration_present": bool(matching_registration),
        "supporting_supersession_present": bool(supporting_supersession),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "eligible_object": eligible_object,
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "latest_supporting_supersession": _event_summary(supporting_supersession) if supporting_supersession else {},
        "gaps": gaps,
    }


def query_heavy_object_reclamation_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted heavy-object reclamation command via audit, committed events, anchors, and receipt."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_request_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_reclamation_requested",
        envelope_id=normalized_envelope_id,
    )
    canonical_reclaimed_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_reclaimed",
        envelope_id=normalized_envelope_id,
    )
    receipt = _heavy_object_reclamation_receipt(runtime_root, envelope_id=normalized_envelope_id)

    accepted_payload = dict(accepted_audit.get("payload") or {})
    request_payload = dict(canonical_request_event.get("payload") or {})
    reclaimed_payload = dict(canonical_reclaimed_event.get("payload") or {})
    receipt_payload = dict(receipt or {})

    object_id = str(
        receipt_payload.get("object_id")
        or reclaimed_payload.get("object_id")
        or request_payload.get("object_id")
        or accepted_payload.get("object_id")
        or ""
    )
    object_ref = str(
        receipt_payload.get("object_ref")
        or reclaimed_payload.get("object_ref")
        or request_payload.get("object_ref")
        or accepted_payload.get("object_ref")
        or ""
    )
    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=object_id,
        object_ref=object_ref,
    )
    supporting_gc_eligibility = _event_for_object(
        runtime_root,
        event_type="heavy_object_gc_eligible",
        object_id=object_id,
        object_ref=object_ref,
    )

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "object_id": str(accepted_payload.get("object_id") or ""),
        "object_kind": str(accepted_payload.get("object_kind") or ""),
        "object_ref": str(accepted_payload.get("object_ref") or ""),
        "reclamation_kind": str(accepted_payload.get("reclamation_kind") or ""),
        "trigger_kind": str(accepted_payload.get("trigger_kind") or ""),
        "trigger_ref": str(accepted_payload.get("trigger_ref") or ""),
    }
    reclamation_effect = {
        "object_id": object_id,
        "object_kind": str(
            receipt_payload.get("object_kind")
            or reclaimed_payload.get("object_kind")
            or request_payload.get("object_kind")
            or accepted_payload.get("object_kind")
            or ""
        ),
        "object_ref": object_ref,
        "reclamation_kind": str(
            receipt_payload.get("reclamation_kind")
            or reclaimed_payload.get("reclamation_kind")
            or request_payload.get("reclamation_kind")
            or accepted_payload.get("reclamation_kind")
            or ""
        ),
        "owner_node_id": str(
            receipt_payload.get("owner_node_id")
            or reclaimed_payload.get("owner_node_id")
            or request_payload.get("owner_node_id")
            or accepted_payload.get("owner_node_id")
            or ""
        ),
        "runtime_name": str(
            receipt_payload.get("runtime_name")
            or reclaimed_payload.get("runtime_name")
            or request_payload.get("runtime_name")
            or accepted_payload.get("runtime_name")
            or ""
        ),
        "repo_root": str(
            receipt_payload.get("repo_root")
            or reclaimed_payload.get("repo_root")
            or request_payload.get("repo_root")
            or accepted_payload.get("repo_root")
            or ""
        ),
        "trigger_kind": str(
            receipt_payload.get("trigger_kind")
            or reclaimed_payload.get("trigger_kind")
            or request_payload.get("trigger_kind")
            or accepted_payload.get("trigger_kind")
            or ""
        ),
        "trigger_ref": str(
            receipt_payload.get("trigger_ref")
            or reclaimed_payload.get("trigger_ref")
            or request_payload.get("trigger_ref")
            or accepted_payload.get("trigger_ref")
            or ""
        ),
        "reclaimed": bool(receipt_payload.get("reclaimed") or reclaimed_payload.get("reclaimed")),
        "object_existed_before": bool(
            receipt_payload.get("object_existed_before") or reclaimed_payload.get("object_existed_before")
        ),
        "object_exists_after": bool(
            receipt_payload.get("object_exists_after") or reclaimed_payload.get("object_exists_after")
        ),
        "byte_size_before": int(receipt_payload.get("byte_size_before") or reclaimed_payload.get("byte_size_before") or 0),
        "receipt_ref": str(receipt_payload.get("receipt_ref") or reclaimed_payload.get("receipt_ref") or ""),
        "failure_reasons": [str(item) for item in list(receipt_payload.get("failure_reasons") or [])],
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_request_event:
        gaps.append("missing_canonical_heavy_object_reclamation_requested_event")
    if not canonical_reclaimed_event:
        gaps.append("missing_canonical_heavy_object_reclaimed_event")
    if not receipt:
        gaps.append("missing_heavy_object_reclamation_receipt")
    if not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")
    if not supporting_gc_eligibility:
        gaps.append("missing_supporting_heavy_object_gc_eligibility")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_request_event_present": bool(canonical_request_event),
        "canonical_reclaimed_event_present": bool(canonical_reclaimed_event),
        "reclamation_receipt_present": bool(receipt),
        "matching_registration_present": bool(matching_registration),
        "supporting_gc_eligibility_present": bool(supporting_gc_eligibility),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_request_event": _event_summary(canonical_request_event) if canonical_request_event else {},
        "canonical_reclaimed_event": _event_summary(canonical_reclaimed_event) if canonical_reclaimed_event else {},
        "reclamation_effect": reclamation_effect,
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "latest_supporting_gc_eligibility": _event_summary(supporting_gc_eligibility) if supporting_gc_eligibility else {},
        "reclamation_receipt": receipt_payload,
        "gaps": gaps,
    }


def query_heavy_object_reference_inventory_view(
    state_root: Path,
    *,
    object_id: str = "",
    object_ref: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed authoritative references for one heavy-object identity."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(Path(object_ref).expanduser().resolve()) if str(object_ref or "").strip() else ""
    normalized_object_id, normalized_object_ref = _resolve_effective_heavy_object_query_identity(
        runtime_root,
        object_id=normalized_object_id,
        object_ref=normalized_object_ref,
    )

    latest_reference_attach_by_holder: dict[tuple[str, str, str], dict[str, Any]] = {}
    latest_reference_attach_by_holder_object: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    latest_reference_release_by_holder_object: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for event in iter_committed_events(runtime_root):
        event_type = str(event.get("event_type") or "").strip()
        if event_type not in {"heavy_object_reference_attached", "heavy_object_reference_released"}:
            continue
        payload = dict(event.get("payload") or {})
        holder_key = (
            str(payload.get("reference_ref") or "").strip(),
            str(payload.get("reference_holder_kind") or "").strip(),
            str(payload.get("reference_holder_ref") or "").strip(),
        )
        object_key = (
            str(payload.get("object_id") or "").strip(),
            str(payload.get("object_ref") or "").strip(),
        )
        if not any(holder_key) or not all(object_key):
            continue
        holder_object_key = (*holder_key, *object_key)
        if event_type == "heavy_object_reference_attached":
            latest_reference_attach_by_holder[holder_key] = dict(event or {})
            latest_reference_attach_by_holder_object[holder_object_key] = dict(event or {})
            continue
        latest_reference_release_by_holder_object[holder_object_key] = dict(event or {})

    active_references: list[dict[str, Any]] = []
    released_references: list[dict[str, Any]] = []
    holder_object_keys = sorted(
        {
            *latest_reference_attach_by_holder_object.keys(),
            *latest_reference_release_by_holder_object.keys(),
        },
        key=lambda item: (item[1], item[2], item[0], item[3], item[4]),
    )
    for holder_object_key in holder_object_keys:
        attach_event = dict(latest_reference_attach_by_holder_object.get(holder_object_key) or {})
        release_event = dict(latest_reference_release_by_holder_object.get(holder_object_key) or {})
        representative_event = attach_event or release_event
        payload = dict(representative_event.get("payload") or {})
        if not _payload_matches_heavy_object_identity(
            payload,
            object_id=normalized_object_id,
            object_ref=normalized_object_ref,
            reference_ref_fallback="reference_ref",
        ):
            continue
        attach_summary = {
            **_event_summary(attach_event),
            "object_id": str(payload.get("object_id") or ""),
            "object_kind": str(payload.get("object_kind") or ""),
            "object_ref": str(payload.get("object_ref") or ""),
            "reference_ref": str(payload.get("reference_ref") or ""),
            "reference_holder_kind": str(payload.get("reference_holder_kind") or ""),
            "reference_holder_ref": str(payload.get("reference_holder_ref") or ""),
            "reference_kind": str(payload.get("reference_kind") or ""),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": str(payload.get("trigger_kind") or ""),
            "trigger_ref": str(payload.get("trigger_ref") or ""),
        }
        release_summary = {
            **_event_summary(release_event),
            "object_id": str(payload.get("object_id") or ""),
            "object_kind": str(payload.get("object_kind") or ""),
            "object_ref": str(payload.get("object_ref") or ""),
            "reference_ref": str(payload.get("reference_ref") or ""),
            "reference_holder_kind": str(payload.get("reference_holder_kind") or ""),
            "reference_holder_ref": str(payload.get("reference_holder_ref") or ""),
            "reference_kind": str(payload.get("reference_kind") or ""),
            "runtime_name": str(payload.get("runtime_name") or ""),
            "repo_root": str(payload.get("repo_root") or ""),
            "trigger_kind": str(dict(release_event.get("payload") or {}).get("trigger_kind") or ""),
            "trigger_ref": str(dict(release_event.get("payload") or {}).get("trigger_ref") or ""),
        }
        holder_key = holder_object_key[:3]
        latest_holder_attach = dict(latest_reference_attach_by_holder.get(holder_key) or {})
        attach_seq = int(attach_event.get("seq") or 0)
        release_seq = int(release_event.get("seq") or 0)
        latest_holder_attach_seq = int(latest_holder_attach.get("seq") or 0)
        if attach_event and latest_holder_attach_seq == attach_seq and release_seq <= attach_seq:
            active_references.append(attach_summary)
            continue
        if release_event and release_seq > attach_seq:
            released_references.append(release_summary)

    effective_object_id = normalized_object_id
    effective_object_ref = normalized_object_ref
    effective_object_kind = ""
    reference_anchor = dict(active_references[0] if active_references else (released_references[0] if released_references else {}))
    if reference_anchor:
        effective_object_id = str(reference_anchor.get("object_id") or effective_object_id)
        effective_object_ref = str(reference_anchor.get("object_ref") or effective_object_ref)
        effective_object_kind = str(reference_anchor.get("object_kind") or "")

    matching_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    registration_payload = dict(matching_registration.get("payload") or {})
    if not effective_object_kind:
        effective_object_kind = str(registration_payload.get("object_kind") or "")

    gaps: list[str] = []
    if not active_references:
        gaps.append("missing_matching_heavy_object_reference")
    if (active_references or released_references) and not matching_registration:
        gaps.append("missing_matching_heavy_object_registration")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "object_id": effective_object_id,
        "object_kind": effective_object_kind,
        "object_ref": effective_object_ref,
        "active_reference_count": len(active_references),
        "released_reference_count": len(released_references),
        "active_reference_holder_kinds": sorted(
            {
                str(item.get("reference_holder_kind") or "")
                for item in active_references
                if str(item.get("reference_holder_kind") or "")
            }
        ),
        "active_reference_refs": sorted(
            {
                str(item.get("reference_ref") or "")
                for item in active_references
                if str(item.get("reference_ref") or "")
            }
        ),
        "released_reference_refs": sorted(
            {
                str(item.get("reference_ref") or "")
                for item in released_references
                if str(item.get("reference_ref") or "")
            }
        ),
        "active_references": active_references,
        "released_references": released_references,
        "matching_registration_present": bool(matching_registration),
        "latest_matching_registration": _event_summary(matching_registration) if matching_registration else {},
        "gaps": gaps,
    }


def query_heavy_object_retention_view(
    state_root: Path,
    *,
    object_id: str = "",
    object_ref: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed retention state for one heavy-object identity."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(Path(object_ref).expanduser().resolve()) if str(object_ref or "").strip() else ""
    normalized_object_id, normalized_object_ref = _resolve_effective_heavy_object_query_identity(
        runtime_root,
        object_id=normalized_object_id,
        object_ref=normalized_object_ref,
    )

    latest_registration = _event_for_object(
        runtime_root,
        event_type="heavy_object_registered",
        object_id=normalized_object_id,
        object_ref=normalized_object_ref,
    )
    registration_payload = dict(latest_registration.get("payload") or {})
    effective_object_id = str(registration_payload.get("object_id") or normalized_object_id or "").strip()
    effective_object_ref = str(registration_payload.get("object_ref") or normalized_object_ref or "").strip()

    latest_superseded_by = _latest_matching_heavy_object_supersession(
        runtime_root,
        superseded_object_id=effective_object_id,
        superseded_object_ref=effective_object_ref,
        replacement_object_id="",
        replacement_object_ref="",
    )
    latest_gc_eligibility = _event_for_object(
        runtime_root,
        event_type="heavy_object_gc_eligible",
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    latest_reclaimed = _event_for_object(
        runtime_root,
        event_type="heavy_object_reclaimed",
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    latest_matching_observation = _latest_matching_heavy_object_observation(
        runtime_root,
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    reference_inventory = query_heavy_object_reference_inventory_view(
        runtime_root,
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    pin_events = _matching_heavy_object_pin_events(
        runtime_root,
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    pin_release_events = _matching_heavy_object_pin_release_events(
        runtime_root,
        object_id=effective_object_id,
        object_ref=effective_object_ref,
    )
    replacement_events = _matching_heavy_object_replacement_events(
        runtime_root,
        replacement_object_id=effective_object_id,
        replacement_object_ref=effective_object_ref,
    )

    gc_payload = dict(latest_gc_eligibility.get("payload") or {})
    reclaimed_payload = dict(latest_reclaimed.get("payload") or {})

    latest_pin_fact_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for event in [*pin_events, *pin_release_events]:
        payload = dict(event.get("payload") or {})
        pin_key = (
            str(payload.get("pin_holder_id") or "").strip(),
            str(payload.get("pin_kind") or "").strip(),
        )
        if not any(pin_key):
            continue
        latest_pin_fact_by_identity[pin_key] = dict(event or {})
    latest_pin_fact_summaries = [_event_summary(event) for event in latest_pin_fact_by_identity.values()]
    active_pin_summaries = [
        summary
        for summary in latest_pin_fact_summaries
        if str(summary.get("event_type") or "") == "heavy_object_pinned"
    ]
    released_pin_summaries = [
        summary
        for summary in latest_pin_fact_summaries
        if str(summary.get("event_type") or "") == "heavy_object_pin_released"
    ]
    active_pin_summaries.sort(key=lambda item: (str(item.get("pin_holder_id") or ""), str(item.get("pin_kind") or "")))
    released_pin_summaries.sort(key=lambda item: (str(item.get("pin_holder_id") or ""), str(item.get("pin_kind") or "")))
    replacement_summaries = [_event_summary(event) for event in replacement_events]
    active_reference_count = int(reference_inventory.get("active_reference_count") or 0)
    active_reference_summaries = [dict(item or {}) for item in list(reference_inventory.get("active_references") or [])]

    registered_present = bool(latest_registration)
    superseded_present = bool(latest_superseded_by)
    gc_eligibility_present = bool(latest_gc_eligibility)
    reclaimed_present = bool(latest_reclaimed)
    active_pin_count = 0 if reclaimed_present else len(active_pin_summaries)

    if reclaimed_present:
        retention_state = "RECLAIMED"
    elif active_pin_count > 0:
        retention_state = "PINNED"
    elif active_reference_count > 0:
        retention_state = "REFERENCED"
    elif gc_eligibility_present:
        retention_state = "GC_ELIGIBLE"
    elif superseded_present:
        retention_state = "SUPERSEDED"
    elif registered_present:
        retention_state = "REGISTERED"
    else:
        retention_state = "UNREGISTERED"

    reclaimable = bool(
        registered_present
        and gc_eligibility_present
        and not reclaimed_present
        and active_pin_count == 0
        and active_reference_count == 0
    )

    gaps: list[str] = []
    if not effective_object_id and not effective_object_ref:
        gaps.append("missing_heavy_object_identity")
    if not registered_present:
        gaps.append("missing_canonical_heavy_object_registered_event")
    if gc_eligibility_present and str(gc_payload.get("superseded_by_object_id") or "").strip() and not superseded_present:
        gaps.append("missing_supporting_heavy_object_supersession")
    if reclaimed_present and not gc_eligibility_present:
        gaps.append("missing_supporting_heavy_object_gc_eligibility")

    registration_payload_summary = {
        "object_id": effective_object_id,
        "object_kind": str(
            registration_payload.get("object_kind")
            or gc_payload.get("object_kind")
            or reclaimed_payload.get("object_kind")
            or ""
        ).strip(),
        "object_ref": effective_object_ref,
        "byte_size": int(registration_payload.get("byte_size") or 0),
        "owner_node_id": str(
            registration_payload.get("owner_node_id")
            or gc_payload.get("owner_node_id")
            or reclaimed_payload.get("owner_node_id")
            or ""
        ).strip(),
        "runtime_name": str(
            registration_payload.get("runtime_name")
            or gc_payload.get("runtime_name")
            or reclaimed_payload.get("runtime_name")
            or ""
        ).strip(),
        "repo_root": str(
            registration_payload.get("repo_root")
            or gc_payload.get("repo_root")
            or reclaimed_payload.get("repo_root")
            or ""
        ).strip(),
    }

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "object_id": effective_object_id,
        "object_kind": registration_payload_summary["object_kind"],
        "object_ref": effective_object_ref,
        "registered_present": registered_present,
        "active_pin_count": active_pin_count,
        "active_reference_count": active_reference_count,
        "superseded_present": superseded_present,
        "gc_eligibility_present": gc_eligibility_present,
        "reclaimed_present": reclaimed_present,
        "reclaimable": reclaimable,
        "retention_state": retention_state,
        "replacement_for_object_ids": [
            str(summary.get("superseded_object_id") or "")
            for summary in replacement_summaries
            if str(summary.get("superseded_object_id") or "")
        ],
        "registration": registration_payload_summary,
        "latest_registration": _event_summary(latest_registration) if latest_registration else {},
        "active_pins": active_pin_summaries,
        "released_pin_count": len(released_pin_summaries),
        "released_pins": released_pin_summaries,
        "active_references": active_reference_summaries,
        "latest_superseded_by": _event_summary(latest_superseded_by) if latest_superseded_by else {},
        "replacement_for_events": replacement_summaries,
        "latest_gc_eligibility": _event_summary(latest_gc_eligibility) if latest_gc_eligibility else {},
        "latest_reclaimed": _event_summary(latest_reclaimed) if latest_reclaimed else {},
        "latest_matching_observation": (
            {**_event_summary(latest_matching_observation), "payload": dict(latest_matching_observation.get("payload") or {})}
            if latest_matching_observation
            else {}
        ),
        "gaps": gaps,
    }


def query_heavy_object_observation_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    object_id: str = "",
    object_ref: str | Path = "",
) -> dict[str, Any]:
    """Explain the latest committed heavy-object observation matching one filter set."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(Path(object_ref).expanduser().resolve()) if str(object_ref or "").strip() else ""

    matching_observations = _matching_heavy_object_observation_events(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        object_id=normalized_object_id,
        object_ref=normalized_object_ref,
    )
    latest_matching_observation = dict(matching_observations[-1] or {}) if matching_observations else {}
    latest_payload = dict(latest_matching_observation.get("payload") or {})

    observed_object_refs = sorted(
        {
            str(item)
            for event in matching_observations
            for item in (
                [str(dict(event.get("payload") or {}).get("object_ref") or "")]
                + [str(ref) for ref in list(dict(event.get("payload") or {}).get("object_refs") or [])]
            )
            if str(item or "").strip()
        }
    )
    duplicate_counts: list[int] = []
    for event in matching_observations:
        payload = dict(event.get("payload") or {})
        try:
            duplicate_counts.append(int(payload.get("duplicate_count") or 0))
        except (TypeError, ValueError):
            duplicate_counts.append(0)

    heavy_object_observation_event_count = 0
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") == "heavy_object_observed":
            heavy_object_observation_event_count += 1

    gaps: list[str] = []
    if not matching_observations:
        gaps.append("missing_matching_heavy_object_observation")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "object_id": normalized_object_id,
        "object_ref": normalized_object_ref,
        "heavy_object_observation_event_count": heavy_object_observation_event_count,
        "matching_observation_count": len(matching_observations),
        "observed_object_refs": observed_object_refs,
        "observed_object_ref_count": len(observed_object_refs),
        "max_duplicate_count": max(duplicate_counts, default=0),
        "latest_duplicate_count": int(latest_payload.get("duplicate_count") or 0),
        "latest_total_bytes": int(latest_payload.get("total_bytes") or 0),
        "latest_matching_observation": (
            {
                **_event_summary(latest_matching_observation),
                "payload": latest_payload,
            }
            if latest_matching_observation
            else {}
        ),
        "gaps": gaps,
    }


def _retired_observed_heavy_object_ref_summary(
    runtime_root: Path,
    *,
    object_id: str,
    canonical_object_ref: str,
    observed_ref: str,
) -> dict[str, Any]:
    normalized_object_id = str(object_id or "").strip()
    normalized_canonical_ref = str(canonical_object_ref or "").strip()
    normalized_observed_ref = str(observed_ref or "").strip()
    if not normalized_object_id or not normalized_observed_ref:
        return {"retired": False, "retirement_state": "", "retention_view": {}}
    if normalized_observed_ref == normalized_canonical_ref:
        return {"retired": False, "retirement_state": "", "retention_view": {}}

    observed_retention = query_heavy_object_retention_view(
        runtime_root,
        object_id=normalized_object_id,
        object_ref=normalized_observed_ref,
    )

    latest_superseded_by = dict(observed_retention.get("latest_superseded_by") or {})
    latest_gc_eligibility = dict(observed_retention.get("latest_gc_eligibility") or {})
    replacement_object_id = str(
        latest_superseded_by.get("replacement_object_id")
        or latest_gc_eligibility.get("superseded_by_object_id")
        or ""
    ).strip()
    replacement_object_ref = str(
        latest_superseded_by.get("replacement_object_ref")
        or latest_gc_eligibility.get("superseded_by_object_ref")
        or ""
    ).strip()
    if replacement_object_id and replacement_object_id != normalized_object_id:
        return {"retired": False, "retirement_state": "", "retention_view": dict(observed_retention or {})}
    if replacement_object_ref and replacement_object_ref != normalized_canonical_ref:
        return {"retired": False, "retirement_state": "", "retention_view": dict(observed_retention or {})}
    if not (replacement_object_id or replacement_object_ref):
        return {"retired": False, "retirement_state": "", "retention_view": dict(observed_retention or {})}

    gc_eligibility_present = bool(observed_retention.get("gc_eligibility_present"))
    reclaimed_present = bool(observed_retention.get("reclaimed_present"))
    if not gc_eligibility_present and not reclaimed_present:
        return {"retired": False, "retirement_state": "", "retention_view": dict(observed_retention or {})}

    if reclaimed_present:
        retirement_state = "RECLAIMED"
    elif bool(observed_retention.get("reclaimable")):
        retirement_state = "RECLAIMABLE"
    else:
        retirement_state = "BLOCKED"
    return {
        "retired": True,
        "retirement_state": retirement_state,
        "retention_view": dict(observed_retention or {}),
    }


def query_heavy_object_sharing_view(
    state_root: Path,
    *,
    object_id: str = "",
    object_ref: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed shareability for one heavy-object identity."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_object_id = str(object_id or "").strip()
    normalized_object_ref = str(Path(object_ref).expanduser().resolve()) if str(object_ref or "").strip() else ""

    retention_view = query_heavy_object_retention_view(
        runtime_root,
        object_id=normalized_object_id,
        object_ref=normalized_object_ref,
    )
    observation_view = query_heavy_object_observation_view(
        runtime_root,
        object_id=normalized_object_id or str(retention_view.get("object_id") or ""),
        object_ref=normalized_object_ref or str(retention_view.get("object_ref") or ""),
    )

    effective_object_id = str(retention_view.get("object_id") or normalized_object_id or "").strip()
    effective_object_kind = str(retention_view.get("object_kind") or "").strip()
    effective_object_ref = str(retention_view.get("object_ref") or normalized_object_ref or "").strip()

    registered_present = bool(retention_view.get("registered_present"))
    superseded_present = bool(retention_view.get("superseded_present"))
    reclaimed_present = bool(retention_view.get("reclaimed_present"))
    observation_present = bool(observation_view.get("matching_observation_count"))
    active_reference_count = int(retention_view.get("active_reference_count") or 0)
    active_reference_refs = sorted(
        {
            str(item.get("reference_ref") or "")
            for item in list(retention_view.get("active_references") or [])
            if str(dict(item or {}).get("reference_ref") or "").strip()
        }
    )
    observed_object_refs = sorted(
        {
            str(item)
            for item in list(observation_view.get("observed_object_refs") or [])
            if str(item or "").strip()
        }
    )
    observed_object_ref_count = len(observed_object_refs)
    max_duplicate_count = int(observation_view.get("max_duplicate_count") or 0)
    latest_duplicate_count = int(observation_view.get("latest_duplicate_count") or 0)
    active_reference_ref_set = set(active_reference_refs)
    covered_observed_ref_set = {
        ref
        for ref in observed_object_refs
        if ref in active_reference_ref_set
    }
    if effective_object_ref and active_reference_count > 0 and effective_object_ref in observed_object_refs:
        covered_observed_ref_set.add(effective_object_ref)
    retired_observed_ref_by_state = {
        "BLOCKED": set(),
        "RECLAIMABLE": set(),
        "RECLAIMED": set(),
    }
    for ref in observed_object_refs:
        if ref in covered_observed_ref_set:
            continue
        retired_summary = _retired_observed_heavy_object_ref_summary(
            runtime_root,
            object_id=effective_object_id,
            canonical_object_ref=effective_object_ref,
            observed_ref=ref,
        )
        if not bool(retired_summary.get("retired")):
            continue
        retirement_state = str(retired_summary.get("retirement_state") or "").strip().upper()
        if retirement_state not in retired_observed_ref_by_state:
            retirement_state = "BLOCKED"
        retired_observed_ref_by_state[retirement_state].add(ref)
    covered_observed_refs = sorted(covered_observed_ref_set)
    blocked_retired_observed_refs = sorted(retired_observed_ref_by_state["BLOCKED"])
    reclaimable_retired_observed_refs = sorted(retired_observed_ref_by_state["RECLAIMABLE"])
    reclaimed_retired_observed_refs = sorted(retired_observed_ref_by_state["RECLAIMED"])
    retired_observed_ref_set = set().union(*retired_observed_ref_by_state.values())
    retired_observed_refs = sorted(retired_observed_ref_set)
    missing_observed_refs = sorted(
        {
            ref
            for ref in observed_object_refs
            if ref not in covered_observed_ref_set and ref not in retired_observed_ref_set
        }
    )
    covered_observed_ref_count = len(covered_observed_refs)
    retired_observed_ref_count = len(retired_observed_refs)
    blocked_retired_observed_ref_count = len(blocked_retired_observed_refs)
    reclaimable_retired_observed_ref_count = len(reclaimable_retired_observed_refs)
    reclaimed_retired_observed_ref_count = len(reclaimed_retired_observed_refs)
    resolved_observed_ref_count = covered_observed_ref_count + retired_observed_ref_count
    missing_observed_ref_count = len(missing_observed_refs)

    converged = bool(
        registered_present
        and not superseded_present
        and not reclaimed_present
        and observation_present
        and observed_object_ref_count > 1
        and max_duplicate_count > 1
        and retired_observed_ref_count == 0
        and covered_observed_ref_count == observed_object_ref_count
    )
    settled = bool(
        registered_present
        and not superseded_present
        and not reclaimed_present
        and observation_present
        and observed_object_ref_count > 1
        and max_duplicate_count > 1
        and retired_observed_ref_count > 0
        and resolved_observed_ref_count == observed_object_ref_count
        and reclaimed_retired_observed_ref_count == retired_observed_ref_count
    )
    settling = bool(
        registered_present
        and not superseded_present
        and not reclaimed_present
        and observation_present
        and observed_object_ref_count > 1
        and max_duplicate_count > 1
        and retired_observed_ref_count > 0
        and resolved_observed_ref_count == observed_object_ref_count
        and reclaimed_retired_observed_ref_count < retired_observed_ref_count
    )

    shareable = bool(
        registered_present
        and not superseded_present
        and not reclaimed_present
        and not converged
        and not settling
        and not settled
        and (
            active_reference_count > 1
            or (
                observation_present
                and observed_object_ref_count > 1
                and max_duplicate_count > 1
            )
        )
    )

    if reclaimed_present:
        sharing_state = "RECLAIMED"
    elif superseded_present:
        sharing_state = "SUPERSEDED"
    elif settling:
        sharing_state = "SETTLING"
    elif settled:
        sharing_state = "SETTLED"
    elif converged:
        sharing_state = "CONVERGED"
    elif shareable:
        sharing_state = "SHAREABLE"
    elif observation_present:
        sharing_state = "SINGLE_REF"
    elif registered_present:
        sharing_state = "UNOBSERVED"
    else:
        sharing_state = "UNREGISTERED"

    gaps = [
        str(item)
        for item in dict.fromkeys(
            [
                *[str(item) for item in list(retention_view.get("gaps") or [])],
                *[str(item) for item in list(observation_view.get("gaps") or [])],
            ]
        )
        if str(item or "").strip()
    ]

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "object_id": effective_object_id,
        "object_kind": effective_object_kind,
        "object_ref": effective_object_ref,
        "registered_present": registered_present,
        "observation_present": observation_present,
        "shareable": shareable,
        "converged": converged,
        "settling": settling,
        "settled": settled,
        "sharing_state": sharing_state,
        "retention_state": str(retention_view.get("retention_state") or ""),
        "active_pin_count": int(retention_view.get("active_pin_count") or 0),
        "active_reference_count": active_reference_count,
        "active_reference_refs": active_reference_refs,
        "observed_object_refs": observed_object_refs,
        "observed_object_ref_count": observed_object_ref_count,
        "covered_observed_refs": covered_observed_refs,
        "covered_observed_ref_count": covered_observed_ref_count,
        "retired_observed_refs": retired_observed_refs,
        "retired_observed_ref_count": retired_observed_ref_count,
        "blocked_retired_observed_refs": blocked_retired_observed_refs,
        "blocked_retired_observed_ref_count": blocked_retired_observed_ref_count,
        "reclaimable_retired_observed_refs": reclaimable_retired_observed_refs,
        "reclaimable_retired_observed_ref_count": reclaimable_retired_observed_ref_count,
        "reclaimed_retired_observed_refs": reclaimed_retired_observed_refs,
        "reclaimed_retired_observed_ref_count": reclaimed_retired_observed_ref_count,
        "resolved_observed_ref_count": resolved_observed_ref_count,
        "missing_observed_refs": missing_observed_refs,
        "missing_observed_ref_count": missing_observed_ref_count,
        "max_duplicate_count": max_duplicate_count,
        "latest_duplicate_count": latest_duplicate_count,
        "latest_total_bytes": int(observation_view.get("latest_total_bytes") or 0),
        "latest_registration": dict(retention_view.get("latest_registration") or {}),
        "latest_matching_observation": dict(observation_view.get("latest_matching_observation") or {}),
        "latest_superseded_by": dict(retention_view.get("latest_superseded_by") or {}),
        "latest_reclaimed": dict(retention_view.get("latest_reclaimed") or {}),
        "replacement_for_object_ids": [str(item) for item in list(retention_view.get("replacement_for_object_ids") or [])],
        "gaps": gaps,
    }


def query_heavy_object_sharing_inventory_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed shareable-heavy-object candidate set."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_repo_root = str(Path(repo_root).expanduser().resolve()) if str(repo_root or "").strip() else ""

    registrations: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_registered":
            continue
        payload = dict(event.get("payload") or {})
        if normalized_runtime_name and str(payload.get("runtime_name") or "").strip() != normalized_runtime_name:
            continue
        if normalized_object_kind and str(payload.get("object_kind") or "").strip() != normalized_object_kind:
            continue
        if normalized_repo_root and str(payload.get("repo_root") or "").strip() != normalized_repo_root:
            continue
        object_id = str(payload.get("object_id") or "").strip()
        object_ref = str(payload.get("object_ref") or "").strip()
        key = (object_id, object_ref)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        registrations.append(dict(event))

    candidate_summaries: list[dict[str, Any]] = []
    converged_summaries: list[dict[str, Any]] = []
    settling_summaries: list[dict[str, Any]] = []
    settled_summaries: list[dict[str, Any]] = []
    display_registration_count = 0
    superseded_count = 0
    unobserved_count = 0
    converged_count = 0
    settling_count = 0
    settled_count = 0
    max_duplicate_count = 0
    for registration in registrations:
        payload = dict(registration.get("payload") or {})
        payload_object_id = str(payload.get("object_id") or "").strip()
        payload_object_ref = str(payload.get("object_ref") or "").strip()
        registration_kind = str(payload.get("registration_kind") or "").strip()
        sharing_view = query_heavy_object_sharing_view(
            runtime_root,
            object_id=payload_object_id,
            object_ref=payload_object_ref,
        )
        aggregate_sharing_view: dict[str, Any] = {}
        if payload_object_id:
            aggregate_sharing_view = query_heavy_object_sharing_view(
                runtime_root,
                object_id=payload_object_id,
            )
            primary_state = str(sharing_view.get("sharing_state") or "").strip().upper()
            aggregate_state = str(aggregate_sharing_view.get("sharing_state") or "").strip().upper()
            aggregate_object_ref = str(aggregate_sharing_view.get("object_ref") or "").strip()
            if (
                registration_kind == "shared_reference_auto_registration"
                and aggregate_object_ref
                and aggregate_object_ref != payload_object_ref
                and aggregate_state != "UNREGISTERED"
            ):
                continue
            if primary_state in {"SINGLE_REF", "SHAREABLE", "UNOBSERVED", "UNREGISTERED"} and aggregate_state in {
                "SHAREABLE",
                "CONVERGED",
                "SETTLING",
                "SETTLED",
            }:
                sharing_view = aggregate_sharing_view
        display_registration_count += 1
        max_duplicate_count = max(max_duplicate_count, int(sharing_view.get("max_duplicate_count") or 0))
        if str(sharing_view.get("sharing_state") or "") in {"SUPERSEDED", "RECLAIMED"}:
            superseded_count += 1
        if str(sharing_view.get("sharing_state") or "") == "UNOBSERVED":
            unobserved_count += 1
        if str(sharing_view.get("sharing_state") or "") == "CONVERGED":
            converged_count += 1
            converged_summaries.append(
                {
                    "object_id": str(sharing_view.get("object_id") or ""),
                    "object_kind": str(sharing_view.get("object_kind") or ""),
                    "object_ref": str(sharing_view.get("object_ref") or ""),
                    "sharing_state": str(sharing_view.get("sharing_state") or ""),
                    "retention_state": str(sharing_view.get("retention_state") or ""),
                    "max_duplicate_count": int(sharing_view.get("max_duplicate_count") or 0),
                    "covered_observed_ref_count": int(sharing_view.get("covered_observed_ref_count") or 0),
                    "missing_observed_ref_count": int(sharing_view.get("missing_observed_ref_count") or 0),
                    "observed_object_refs": [str(item) for item in list(sharing_view.get("observed_object_refs") or [])],
                    "active_reference_refs": [str(item) for item in list(sharing_view.get("active_reference_refs") or [])],
                    "latest_registration": dict(sharing_view.get("latest_registration") or {}),
                    "latest_matching_observation": dict(sharing_view.get("latest_matching_observation") or {}),
                }
            )
            continue
        if str(sharing_view.get("sharing_state") or "") == "SETTLING":
            settling_count += 1
            settling_summaries.append(
                {
                    "object_id": str(sharing_view.get("object_id") or ""),
                    "object_kind": str(sharing_view.get("object_kind") or ""),
                    "object_ref": str(sharing_view.get("object_ref") or ""),
                    "sharing_state": str(sharing_view.get("sharing_state") or ""),
                    "retention_state": str(sharing_view.get("retention_state") or ""),
                    "max_duplicate_count": int(sharing_view.get("max_duplicate_count") or 0),
                    "covered_observed_ref_count": int(sharing_view.get("covered_observed_ref_count") or 0),
                    "blocked_retired_observed_ref_count": int(
                        sharing_view.get("blocked_retired_observed_ref_count") or 0
                    ),
                    "reclaimable_retired_observed_ref_count": int(
                        sharing_view.get("reclaimable_retired_observed_ref_count") or 0
                    ),
                    "reclaimed_retired_observed_ref_count": int(
                        sharing_view.get("reclaimed_retired_observed_ref_count") or 0
                    ),
                    "missing_observed_ref_count": int(sharing_view.get("missing_observed_ref_count") or 0),
                    "observed_object_refs": [str(item) for item in list(sharing_view.get("observed_object_refs") or [])],
                    "blocked_retired_observed_refs": [
                        str(item) for item in list(sharing_view.get("blocked_retired_observed_refs") or [])
                    ],
                    "reclaimable_retired_observed_refs": [
                        str(item) for item in list(sharing_view.get("reclaimable_retired_observed_refs") or [])
                    ],
                    "active_reference_refs": [str(item) for item in list(sharing_view.get("active_reference_refs") or [])],
                    "latest_registration": dict(sharing_view.get("latest_registration") or {}),
                    "latest_matching_observation": dict(sharing_view.get("latest_matching_observation") or {}),
                    "latest_superseded_by": dict(sharing_view.get("latest_superseded_by") or {}),
                }
            )
            continue
        if str(sharing_view.get("sharing_state") or "") == "SETTLED":
            settled_count += 1
            settled_summaries.append(
                {
                    "object_id": str(sharing_view.get("object_id") or ""),
                    "object_kind": str(sharing_view.get("object_kind") or ""),
                    "object_ref": str(sharing_view.get("object_ref") or ""),
                    "sharing_state": str(sharing_view.get("sharing_state") or ""),
                    "retention_state": str(sharing_view.get("retention_state") or ""),
                    "max_duplicate_count": int(sharing_view.get("max_duplicate_count") or 0),
                    "covered_observed_ref_count": int(sharing_view.get("covered_observed_ref_count") or 0),
                    "retired_observed_ref_count": int(sharing_view.get("retired_observed_ref_count") or 0),
                    "missing_observed_ref_count": int(sharing_view.get("missing_observed_ref_count") or 0),
                    "observed_object_refs": [str(item) for item in list(sharing_view.get("observed_object_refs") or [])],
                    "retired_observed_refs": [str(item) for item in list(sharing_view.get("retired_observed_refs") or [])],
                    "active_reference_refs": [str(item) for item in list(sharing_view.get("active_reference_refs") or [])],
                    "latest_registration": dict(sharing_view.get("latest_registration") or {}),
                    "latest_matching_observation": dict(sharing_view.get("latest_matching_observation") or {}),
                    "latest_reclaimed": dict(sharing_view.get("latest_reclaimed") or {}),
                }
            )
            continue
        if not bool(sharing_view.get("shareable")):
            continue
        candidate_summaries.append(
            {
                "object_id": str(sharing_view.get("object_id") or ""),
                "object_kind": str(sharing_view.get("object_kind") or ""),
                "object_ref": str(sharing_view.get("object_ref") or ""),
                "sharing_state": str(sharing_view.get("sharing_state") or ""),
                "retention_state": str(sharing_view.get("retention_state") or ""),
                "max_duplicate_count": int(sharing_view.get("max_duplicate_count") or 0),
                "observed_object_refs": [str(item) for item in list(sharing_view.get("observed_object_refs") or [])],
                "active_pin_count": int(sharing_view.get("active_pin_count") or 0),
                "latest_registration": dict(sharing_view.get("latest_registration") or {}),
                "latest_matching_observation": dict(sharing_view.get("latest_matching_observation") or {}),
            }
        )

    candidate_summaries.sort(
        key=lambda item: (
            -int(item.get("max_duplicate_count") or 0),
            str(item.get("object_kind") or ""),
            str(item.get("object_id") or ""),
            str(item.get("object_ref") or ""),
        )
    )
    converged_summaries.sort(
        key=lambda item: (
            -int(item.get("max_duplicate_count") or 0),
            str(item.get("object_kind") or ""),
            str(item.get("object_id") or ""),
            str(item.get("object_ref") or ""),
        )
    )
    settling_summaries.sort(
        key=lambda item: (
            -int(item.get("max_duplicate_count") or 0),
            str(item.get("object_kind") or ""),
            str(item.get("object_id") or ""),
            str(item.get("object_ref") or ""),
        )
    )
    settled_summaries.sort(
        key=lambda item: (
            -int(item.get("max_duplicate_count") or 0),
            str(item.get("object_kind") or ""),
            str(item.get("object_id") or ""),
            str(item.get("object_ref") or ""),
        )
    )

    gaps: list[str] = []
    if not registrations:
        gaps.append("missing_matching_heavy_object_registration")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "repo_root": normalized_repo_root,
        "registered_object_count": display_registration_count,
        "shareable_object_count": len(candidate_summaries),
        "converged_object_count": converged_count,
        "settling_object_count": settling_count,
        "settled_object_count": settled_count,
        "superseded_object_count": superseded_count,
        "unobserved_object_count": unobserved_count,
        "max_duplicate_count": max_duplicate_count,
        "shareable_candidates": candidate_summaries,
        "converged_objects": converged_summaries,
        "settling_objects": settling_summaries,
        "settled_objects": settled_summaries,
        "gaps": gaps,
    }


def query_heavy_object_discovery_inventory_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    discovery_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed heavy-object discovery candidate set."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_discovery_kind = str(discovery_kind or "").strip()
    normalized_repo_root = str(Path(repo_root).expanduser().resolve()) if str(repo_root or "").strip() else ""

    matching_events = _matching_heavy_object_discovery_observation_events(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
        discovery_kind=normalized_discovery_kind,
    )

    latest_by_object: dict[tuple[str, str], dict[str, Any]] = {}
    for event in matching_events:
        payload = dict(event.get("payload") or {})
        key = (
            str(payload.get("object_id") or "").strip(),
            str(payload.get("object_ref") or "").strip(),
        )
        if not key[0] and not key[1]:
            continue
        latest_by_object[key] = dict(event or {})

    discovered_objects: list[dict[str, Any]] = []
    strongest_duplicate_count = 0
    shareable_object_count = 0
    for event in latest_by_object.values():
        payload = dict(event.get("payload") or {})
        sharing_view = query_heavy_object_sharing_view(
            runtime_root,
            object_id=str(payload.get("object_id") or ""),
            object_ref=str(payload.get("object_ref") or ""),
        )
        duplicate_count = int(payload.get("duplicate_count") or 0)
        strongest_duplicate_count = max(strongest_duplicate_count, duplicate_count)
        shareable = bool(sharing_view.get("shareable"))
        if shareable:
            shareable_object_count += 1
        discovered_objects.append(
            {
                "object_id": str(payload.get("object_id") or ""),
                "object_kind": str(payload.get("object_kind") or ""),
                "object_ref": str(payload.get("object_ref") or ""),
                "discovery_kind": str(payload.get("observation_kind") or ""),
                "discovery_root": str(payload.get("discovery_root") or ""),
                "duplicate_count": duplicate_count,
                "total_bytes": int(payload.get("total_bytes") or 0),
                "discovered_object_refs": [str(item) for item in list(payload.get("object_refs") or [])],
                "shareable": shareable,
                "sharing_state": str(sharing_view.get("sharing_state") or ""),
                "retention_state": str(sharing_view.get("retention_state") or ""),
                "latest_registration": dict(sharing_view.get("latest_registration") or {}),
                "latest_discovery_observation": {
                    **_event_summary(event),
                    "payload": payload,
                },
            }
        )

    discovered_objects.sort(
        key=lambda item: (
            -int(item.get("duplicate_count") or 0),
            str(item.get("object_kind") or ""),
            str(item.get("object_id") or ""),
            str(item.get("object_ref") or ""),
        )
    )

    gaps: list[str] = []
    if not matching_events:
        gaps.append("missing_matching_heavy_object_discovery_observation")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "discovery_kind": normalized_discovery_kind,
        "repo_root": normalized_repo_root,
        "matching_discovery_observation_count": len(matching_events),
        "discovered_object_count": len(discovered_objects),
        "shareable_object_count": shareable_object_count,
        "strongest_duplicate_count": strongest_duplicate_count,
        "discovered_objects": discovered_objects,
        "gaps": gaps,
    }


def query_heavy_object_dedup_inventory_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    discovery_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed heavy-object dedup candidate set."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_discovery_kind = str(discovery_kind or "").strip()
    normalized_repo_root = str(Path(repo_root).expanduser().resolve()) if str(repo_root or "").strip() else ""

    matching_events = _matching_heavy_object_discovery_observation_events(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
        discovery_kind=normalized_discovery_kind,
    )

    latest_by_object: dict[tuple[str, str], dict[str, Any]] = {}
    for event in matching_events:
        payload = dict(event.get("payload") or {})
        key = (
            str(payload.get("object_id") or "").strip(),
            str(payload.get("object_ref") or "").strip(),
        )
        if not key[0] and not key[1]:
            continue
        latest_by_object[key] = dict(event or {})

    dedup_candidates: list[dict[str, Any]] = []
    converged_discovered_objects: list[dict[str, Any]] = []
    settling_discovered_objects: list[dict[str, Any]] = []
    settled_discovered_objects: list[dict[str, Any]] = []
    blocked_discovered_objects: list[dict[str, Any]] = []
    strongest_duplicate_count = 0
    for event in latest_by_object.values():
        payload = dict(event.get("payload") or {})
        payload_object_id = str(payload.get("object_id") or "")
        payload_object_ref = str(payload.get("object_ref") or "")
        sharing_view = query_heavy_object_sharing_view(
            runtime_root,
            object_id=payload_object_id,
            object_ref=payload_object_ref,
        )
        if payload_object_id:
            aggregate_sharing_view = query_heavy_object_sharing_view(
                runtime_root,
                object_id=payload_object_id,
            )
            primary_state = str(sharing_view.get("sharing_state") or "").strip().upper()
            aggregate_state = str(aggregate_sharing_view.get("sharing_state") or "").strip().upper()
            if primary_state in {"SINGLE_REF", "SHAREABLE", "UNOBSERVED", "UNREGISTERED"} and aggregate_state in {
                "CONVERGED",
                "SETTLING",
                "SETTLED",
            }:
                sharing_view = aggregate_sharing_view
        duplicate_count = int(payload.get("duplicate_count") or 0)
        strongest_duplicate_count = max(strongest_duplicate_count, duplicate_count)
        summary = {
            "object_id": payload_object_id,
            "object_kind": str(payload.get("object_kind") or ""),
            "object_ref": str(sharing_view.get("object_ref") or payload_object_ref or ""),
            "discovery_kind": str(payload.get("observation_kind") or ""),
            "discovery_root": str(payload.get("discovery_root") or ""),
            "duplicate_count": duplicate_count,
            "total_bytes": int(payload.get("total_bytes") or 0),
            "discovered_object_refs": [str(item) for item in list(payload.get("object_refs") or [])],
            "shareable": bool(sharing_view.get("shareable")),
            "sharing_state": str(sharing_view.get("sharing_state") or ""),
            "retention_state": str(sharing_view.get("retention_state") or ""),
            "active_pin_count": int(sharing_view.get("active_pin_count") or 0),
            "latest_registration": dict(sharing_view.get("latest_registration") or {}),
            "latest_discovery_observation": {
                **_event_summary(event),
                "payload": payload,
            },
        }
        if str(sharing_view.get("sharing_state") or "") == "CONVERGED":
            converged_discovered_objects.append(summary)
        elif str(sharing_view.get("sharing_state") or "") == "SETTLING":
            settling_discovered_objects.append(
                {
                    **summary,
                    "covered_observed_ref_count": int(sharing_view.get("covered_observed_ref_count") or 0),
                    "blocked_retired_observed_ref_count": int(
                        sharing_view.get("blocked_retired_observed_ref_count") or 0
                    ),
                    "reclaimable_retired_observed_ref_count": int(
                        sharing_view.get("reclaimable_retired_observed_ref_count") or 0
                    ),
                    "reclaimed_retired_observed_ref_count": int(
                        sharing_view.get("reclaimed_retired_observed_ref_count") or 0
                    ),
                    "missing_observed_ref_count": int(sharing_view.get("missing_observed_ref_count") or 0),
                    "blocked_retired_observed_refs": [
                        str(item) for item in list(sharing_view.get("blocked_retired_observed_refs") or [])
                    ],
                    "reclaimable_retired_observed_refs": [
                        str(item) for item in list(sharing_view.get("reclaimable_retired_observed_refs") or [])
                    ],
                }
            )
        elif str(sharing_view.get("sharing_state") or "") == "SETTLED":
            settled_discovered_objects.append(
                {
                    **summary,
                    "covered_observed_ref_count": int(sharing_view.get("covered_observed_ref_count") or 0),
                    "retired_observed_ref_count": int(sharing_view.get("retired_observed_ref_count") or 0),
                    "missing_observed_ref_count": int(sharing_view.get("missing_observed_ref_count") or 0),
                    "retired_observed_refs": [str(item) for item in list(sharing_view.get("retired_observed_refs") or [])],
                }
            )
        elif bool(sharing_view.get("shareable")):
            dedup_candidates.append(summary)
        else:
            blocked_discovered_objects.append(summary)

    sort_key = lambda item: (  # noqa: E731
        -int(item.get("duplicate_count") or 0),
        str(item.get("object_kind") or ""),
        str(item.get("object_id") or ""),
        str(item.get("object_ref") or ""),
    )
    dedup_candidates.sort(key=sort_key)
    converged_discovered_objects.sort(key=sort_key)
    settling_discovered_objects.sort(key=sort_key)
    settled_discovered_objects.sort(key=sort_key)
    blocked_discovered_objects.sort(key=sort_key)

    gaps: list[str] = []
    if not matching_events:
        gaps.append("missing_matching_heavy_object_discovery_observation")
    elif (
        not dedup_candidates
        and not converged_discovered_objects
        and not settling_discovered_objects
        and not settled_discovered_objects
    ):
        gaps.append("missing_current_heavy_object_dedup_candidate")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "discovery_kind": normalized_discovery_kind,
        "repo_root": normalized_repo_root,
        "matching_discovery_observation_count": len(matching_events),
        "discovered_object_count": len(latest_by_object),
        "dedup_candidate_count": len(dedup_candidates),
        "converged_discovered_object_count": len(converged_discovered_objects),
        "settling_discovered_object_count": len(settling_discovered_objects),
        "settled_discovered_object_count": len(settled_discovered_objects),
        "blocked_discovered_object_count": len(blocked_discovered_objects),
        "strongest_duplicate_count": strongest_duplicate_count,
        "dedup_candidates": dedup_candidates,
        "converged_discovered_objects": converged_discovered_objects,
        "settling_discovered_objects": settling_discovered_objects,
        "settled_discovered_objects": settled_discovered_objects,
        "blocked_discovered_objects": blocked_discovered_objects,
        "gaps": gaps,
    }


def query_heavy_object_gc_candidate_inventory_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain the current committed heavy-object GC candidate set."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    normalized_repo_root = str(Path(repo_root).expanduser().resolve()) if str(repo_root or "").strip() else ""

    registrations: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str]] = set()
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_registered":
            continue
        payload = dict(event.get("payload") or {})
        if normalized_runtime_name and str(payload.get("runtime_name") or "").strip() != normalized_runtime_name:
            continue
        if normalized_object_kind and str(payload.get("object_kind") or "").strip() != normalized_object_kind:
            continue
        if normalized_repo_root and str(payload.get("repo_root") or "").strip() != normalized_repo_root:
            continue
        object_id = str(payload.get("object_id") or "").strip()
        object_ref = str(payload.get("object_ref") or "").strip()
        key = (object_id, object_ref)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        registrations.append(dict(event))

    gc_candidates: list[dict[str, Any]] = []
    blocked_gc_eligible_objects: list[dict[str, Any]] = []
    reclaimed_objects: list[dict[str, Any]] = []
    settlement_reclaimable_objects: list[dict[str, Any]] = []
    settlement_blocked_objects: list[dict[str, Any]] = []
    gc_eligible_object_count = 0
    for registration in registrations:
        payload = dict(registration.get("payload") or {})
        retention_view = query_heavy_object_retention_view(
            runtime_root,
            object_id=str(payload.get("object_id") or ""),
            object_ref=str(payload.get("object_ref") or ""),
        )
        if not bool(retention_view.get("gc_eligibility_present")):
            continue
        gc_eligible_object_count += 1
        summary = {
            "object_id": str(retention_view.get("object_id") or ""),
            "object_kind": str(retention_view.get("object_kind") or ""),
            "object_ref": str(retention_view.get("object_ref") or ""),
            "retention_state": str(retention_view.get("retention_state") or ""),
            "reclaimable": bool(retention_view.get("reclaimable")),
            "active_pin_count": int(retention_view.get("active_pin_count") or 0),
            "active_reference_count": int(retention_view.get("active_reference_count") or 0),
            "latest_registration": dict(retention_view.get("latest_registration") or {}),
            "latest_superseded_by": dict(retention_view.get("latest_superseded_by") or {}),
            "latest_gc_eligibility": dict(retention_view.get("latest_gc_eligibility") or {}),
            "gc_eligibility_kind": str(dict(retention_view.get("latest_gc_eligibility") or {}).get("eligibility_kind") or ""),
            "latest_reclaimed": dict(retention_view.get("latest_reclaimed") or {}),
        }
        if str(retention_view.get("retention_state") or "") == "RECLAIMED":
            reclaimed_objects.append(summary)
        elif bool(retention_view.get("reclaimable")):
            gc_candidates.append(summary)
            if str(summary.get("gc_eligibility_kind") or "").startswith("shared_reference_"):
                settlement_reclaimable_objects.append(summary)
        else:
            blocked_gc_eligible_objects.append(summary)
            if str(summary.get("gc_eligibility_kind") or "").startswith("shared_reference_"):
                settlement_blocked_objects.append(summary)

    sort_key = lambda item: (  # noqa: E731
        str(item.get("object_kind") or ""),
        str(item.get("object_id") or ""),
        str(item.get("object_ref") or ""),
    )
    gc_candidates.sort(key=sort_key)
    blocked_gc_eligible_objects.sort(key=sort_key)
    reclaimed_objects.sort(key=sort_key)
    settlement_reclaimable_objects.sort(key=sort_key)
    settlement_blocked_objects.sort(key=sort_key)

    gaps: list[str] = []
    if not registrations:
        gaps.append("missing_matching_heavy_object_registration")
    elif not gc_candidates:
        gaps.append("missing_current_reclaimable_heavy_object")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "repo_root": normalized_repo_root,
        "registered_object_count": len(registrations),
        "gc_eligible_object_count": gc_eligible_object_count,
        "reclaimable_object_count": len(gc_candidates),
        "blocked_gc_eligible_object_count": len(blocked_gc_eligible_objects),
        "reclaimed_object_count": len(reclaimed_objects),
        "settlement_reclaimable_object_count": len(settlement_reclaimable_objects),
        "settlement_blocked_object_count": len(settlement_blocked_objects),
        "gc_candidates": gc_candidates,
        "blocked_gc_eligible_objects": blocked_gc_eligible_objects,
        "reclaimed_objects": reclaimed_objects,
        "settlement_reclaimable_objects": settlement_reclaimable_objects,
        "settlement_blocked_objects": settlement_blocked_objects,
        "gaps": gaps,
    }


def query_heavy_object_authority_gap_inventory_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain repo-local heavy-object candidates that still sit outside committed lifecycle coverage."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    resolved_repo_root = (
        Path(repo_root).expanduser().resolve()
        if str(repo_root or "").strip()
        else repo_root_from_runtime_root(state_root=runtime_root)
    )
    normalized_repo_root = str(resolved_repo_root) if resolved_repo_root is not None else ""
    authority_runtime_root = _select_heavy_object_authority_runtime_root(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )

    candidate_paths = _heavy_object_candidate_paths(
        resolved_repo_root or runtime_root.parent.resolve(),
        object_kind=normalized_object_kind,
    )

    managed_candidates: list[dict[str, Any]] = []
    unmanaged_candidates: list[dict[str, Any]] = []
    registered_candidate_count = 0
    discovery_covered_candidate_count = 0
    strongest_duplicate_count = 0
    for candidate in candidate_paths:
        normalized_candidate_ref = str(candidate.resolve())
        try:
            candidate_identity = heavy_object_identity(candidate, object_kind=normalized_object_kind or "mathlib_pack")
        except Exception:
            candidate_identity = {}
        candidate_object_id = str(candidate_identity.get("object_id") or "").strip()
        matching_registrations = _matching_heavy_object_registration_events(
            authority_runtime_root,
            runtime_name=normalized_runtime_name,
            object_kind=normalized_object_kind,
            object_id=candidate_object_id,
            object_ref=normalized_candidate_ref,
            repo_root=normalized_repo_root,
        )
        latest_registration = dict(matching_registrations[-1] or {}) if matching_registrations else {}
        latest_discovery = _latest_matching_heavy_object_observation(
            authority_runtime_root,
            runtime_name=normalized_runtime_name,
            object_kind=normalized_object_kind,
            object_id=candidate_object_id,
            object_ref=normalized_candidate_ref,
        )
        discovery_payload = dict(latest_discovery.get("payload") or {})
        registered_present = bool(latest_registration)
        discovery_present = bool(latest_discovery) and (
            not normalized_repo_root or str(discovery_payload.get("repo_root") or "").strip() == normalized_repo_root
        )
        if registered_present:
            registered_candidate_count += 1
        if discovery_present:
            discovery_covered_candidate_count += 1
            strongest_duplicate_count = max(strongest_duplicate_count, int(discovery_payload.get("duplicate_count") or 0))
        registration_payload = dict(latest_registration.get("payload") or {})
        summary = {
            "object_id": str(
                registration_payload.get("object_id")
                or discovery_payload.get("object_id")
                or candidate_object_id
                or ""
            ),
            "object_kind": str(
                registration_payload.get("object_kind")
                or discovery_payload.get("object_kind")
                or normalized_object_kind
            ),
            "object_ref": normalized_candidate_ref,
            "file_name": candidate.name,
            "byte_size": artifact_byte_size(candidate, ignore_runtime_heavy=normalized_object_kind == "publish_tree"),
            "registered_present": registered_present,
            "discovery_present": discovery_present,
            "coverage_state": "MANAGED" if (registered_present or discovery_present) else "UNMANAGED",
            "latest_registration": (
                {**_event_summary(latest_registration), "payload": registration_payload}
                if latest_registration
                else {}
            ),
            "latest_discovery_observation": (
                {**_event_summary(latest_discovery), "payload": discovery_payload}
                if latest_discovery
                else {}
            ),
        }
        if registered_present or discovery_present:
            managed_candidates.append(summary)
        else:
            unmanaged_candidates.append(summary)

    sort_key = lambda item: (  # noqa: E731
        str(item.get("file_name") or ""),
        str(item.get("object_ref") or ""),
    )
    managed_candidates.sort(key=sort_key)
    unmanaged_candidates.sort(key=sort_key)

    gaps: list[str] = []
    if not candidate_paths:
        gaps.append("missing_matching_filesystem_heavy_object_candidate")
    if unmanaged_candidates:
        gaps.append("unmanaged_heavy_object_candidates_present")

    return {
        "runtime_root": str(runtime_root),
        "authority_runtime_root": str(authority_runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "repo_root": normalized_repo_root,
        "filesystem_candidate_count": len(candidate_paths),
        "registered_candidate_count": registered_candidate_count,
        "discovery_covered_candidate_count": discovery_covered_candidate_count,
        "managed_candidate_count": len(managed_candidates),
        "unmanaged_candidate_count": len(unmanaged_candidates),
        "strongest_duplicate_count": strongest_duplicate_count,
        "managed_candidates": managed_candidates,
        "unmanaged_candidates": unmanaged_candidates,
        "gaps": gaps,
    }


def _heavy_object_authority_gap_summary(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
    sample_limit: int = 3,
) -> dict[str, Any]:
    inventory = query_heavy_object_authority_gap_inventory_view(
        runtime_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        repo_root=repo_root,
    )
    unmanaged_candidates = [dict(item or {}) for item in list(inventory.get("unmanaged_candidates") or [])]
    limit = max(int(sample_limit), 0)
    sample = unmanaged_candidates[:limit] if limit else []
    return {
        "runtime_root": str(inventory.get("runtime_root") or str(runtime_root)),
        "authority_runtime_root": str(inventory.get("authority_runtime_root") or str(runtime_root)),
        "repo_root": str(inventory.get("repo_root") or ""),
        "filesystem_candidate_count": int(inventory.get("filesystem_candidate_count") or 0),
        "managed_candidate_count": int(inventory.get("managed_candidate_count") or 0),
        "unmanaged_candidate_count": int(inventory.get("unmanaged_candidate_count") or 0),
        "registered_candidate_count": int(inventory.get("registered_candidate_count") or 0),
        "discovery_covered_candidate_count": int(inventory.get("discovery_covered_candidate_count") or 0),
        "strongest_duplicate_count": int(inventory.get("strongest_duplicate_count") or 0),
        "sample_unmanaged_candidate_refs": [str(item.get("object_ref") or "") for item in sample],
        "sample_unmanaged_candidate_file_names": [str(item.get("file_name") or "") for item in sample],
        "gaps": [str(item) for item in list(inventory.get("gaps") or [])],
    }


def _heavy_object_pressure_summary(
    runtime_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
    sample_limit: int = 3,
) -> dict[str, Any]:
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    resolved_repo_root = (
        Path(repo_root).expanduser().resolve()
        if str(repo_root or "").strip()
        else repo_root_from_runtime_root(state_root=runtime_root)
    )
    normalized_repo_root = str(resolved_repo_root) if resolved_repo_root is not None else ""
    authority_runtime_root = _select_heavy_object_authority_runtime_root(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )

    sharing_inventory = query_heavy_object_sharing_inventory_view(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )
    gc_inventory = query_heavy_object_gc_candidate_inventory_view(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )

    registrations: list[dict[str, Any]] = []
    seen_registration_keys: set[tuple[str, str]] = set()
    for event in _matching_heavy_object_registration_events(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    ):
        payload = dict(event.get("payload") or {})
        key = (
            str(payload.get("object_id") or "").strip(),
            str(payload.get("object_ref") or "").strip(),
        )
        if not any(key):
            continue
        if key in seen_registration_keys:
            continue
        seen_registration_keys.add(key)
        registrations.append(dict(event or {}))

    active_reference_object_count = 0
    active_reference_count = 0
    pinned_object_count = 0
    active_pin_count = 0
    for registration in registrations:
        payload = dict(registration.get("payload") or {})
        retention_view = query_heavy_object_retention_view(
            authority_runtime_root,
            object_id=str(payload.get("object_id") or ""),
            object_ref=str(payload.get("object_ref") or ""),
        )
        object_active_reference_count = int(retention_view.get("active_reference_count") or 0)
        object_active_pin_count = int(retention_view.get("active_pin_count") or 0)
        active_reference_count += object_active_reference_count
        active_pin_count += object_active_pin_count
        if object_active_reference_count > 0:
            active_reference_object_count += 1
        if object_active_pin_count > 0:
            pinned_object_count += 1

    latest_duplicate_observations_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    strongest_duplicate_count = 0
    for event in iter_committed_events(authority_runtime_root):
        if str(event.get("event_type") or "") != "heavy_object_observed":
            continue
        payload = dict(event.get("payload") or {})
        if normalized_runtime_name and str(payload.get("runtime_name") or "").strip() != normalized_runtime_name:
            continue
        if normalized_object_kind and str(payload.get("object_kind") or "").strip() != normalized_object_kind:
            continue
        payload_repo_root = str(payload.get("repo_root") or "").strip()
        if normalized_repo_root and payload_repo_root and payload_repo_root != normalized_repo_root:
            continue
        object_refs = sorted(
            {
                str(item)
                for item in (
                    [str(payload.get("object_ref") or "")]
                    + [str(item) for item in list(payload.get("object_refs") or [])]
                )
                if str(item or "").strip()
            }
        )
        try:
            duplicate_count = int(payload.get("duplicate_count") or 0)
        except (TypeError, ValueError):
            duplicate_count = 0
        duplicate_count = max(duplicate_count, len(object_refs))
        strongest_duplicate_count = max(strongest_duplicate_count, duplicate_count)
        if duplicate_count <= 1:
            continue
        key = (
            str(payload.get("object_kind") or "").strip(),
            str(payload.get("object_id") or "").strip(),
            str(payload.get("object_ref") or "").strip() or (object_refs[0] if object_refs else ""),
        )
        if not any(key) and not object_refs:
            continue
        latest_duplicate_observations_by_key[key] = {
            "object_kind": str(payload.get("object_kind") or "").strip(),
            "object_id": str(payload.get("object_id") or "").strip(),
            "object_ref": str(payload.get("object_ref") or "").strip() or (object_refs[0] if object_refs else ""),
            "duplicate_count": duplicate_count,
            "object_refs": object_refs,
            "total_bytes": int(payload.get("total_bytes") or 0),
        }

    duplicate_observation_summaries = sorted(
        latest_duplicate_observations_by_key.values(),
        key=lambda item: (
            -int(item.get("duplicate_count") or 0),
            str(item.get("object_kind") or ""),
            str(item.get("object_id") or ""),
            str(item.get("object_ref") or ""),
        ),
    )

    limit = max(int(sample_limit), 0)
    shareable_candidates = [dict(item or {}) for item in list(sharing_inventory.get("shareable_candidates") or [])]
    converged_objects = [dict(item or {}) for item in list(sharing_inventory.get("converged_objects") or [])]
    settling_objects = [dict(item or {}) for item in list(sharing_inventory.get("settling_objects") or [])]
    settled_objects = [dict(item or {}) for item in list(sharing_inventory.get("settled_objects") or [])]
    reclaimable_objects = [dict(item or {}) for item in list(gc_inventory.get("gc_candidates") or [])]
    blocked_gc_objects = [dict(item or {}) for item in list(gc_inventory.get("blocked_gc_eligible_objects") or [])]
    settlement_reclaimable_objects = [dict(item or {}) for item in list(gc_inventory.get("settlement_reclaimable_objects") or [])]
    settlement_blocked_objects = [dict(item or {}) for item in list(gc_inventory.get("settlement_blocked_objects") or [])]

    lifecycle_managed_duplicate_object_count = (
        int(sharing_inventory.get("shareable_object_count") or 0)
        + int(sharing_inventory.get("converged_object_count") or 0)
        + int(sharing_inventory.get("settling_object_count") or 0)
        + int(sharing_inventory.get("settled_object_count") or 0)
    )

    gaps: list[str] = []
    if int(sharing_inventory.get("registered_object_count") or 0) == 0:
        gaps.append("missing_matching_heavy_object_registration")
    if (
        duplicate_observation_summaries
        and int(sharing_inventory.get("registered_object_count") or 0) > 0
        and lifecycle_managed_duplicate_object_count == 0
    ):
        gaps.append("missing_managed_duplicate_heavy_object_lifecycle_state")

    return {
        "runtime_root": str(runtime_root),
        "authority_runtime_root": str(authority_runtime_root),
        "repo_root": normalized_repo_root,
        "observed_duplicate_object_count": len(duplicate_observation_summaries),
        "registered_object_count": int(sharing_inventory.get("registered_object_count") or 0),
        "active_reference_object_count": active_reference_object_count,
        "active_reference_count": active_reference_count,
        "pinned_object_count": pinned_object_count,
        "active_pin_count": active_pin_count,
        "shareable_object_count": int(sharing_inventory.get("shareable_object_count") or 0),
        "converged_object_count": int(sharing_inventory.get("converged_object_count") or 0),
        "settling_object_count": int(sharing_inventory.get("settling_object_count") or 0),
        "settled_object_count": int(sharing_inventory.get("settled_object_count") or 0),
        "lifecycle_managed_duplicate_object_count": lifecycle_managed_duplicate_object_count,
        "gc_eligible_object_count": int(gc_inventory.get("gc_eligible_object_count") or 0),
        "reclaimable_object_count": int(gc_inventory.get("reclaimable_object_count") or 0),
        "blocked_gc_eligible_object_count": int(gc_inventory.get("blocked_gc_eligible_object_count") or 0),
        "reclaimed_object_count": int(gc_inventory.get("reclaimed_object_count") or 0),
        "settlement_reclaimable_object_count": int(gc_inventory.get("settlement_reclaimable_object_count") or 0),
        "settlement_blocked_object_count": int(gc_inventory.get("settlement_blocked_object_count") or 0),
        "duplicate_pressure_present": bool(duplicate_observation_summaries),
        "cleanup_pressure_present": bool(
            int(sharing_inventory.get("shareable_object_count") or 0) > 0
            or int(sharing_inventory.get("settling_object_count") or 0) > 0
            or int(gc_inventory.get("reclaimable_object_count") or 0) > 0
            or int(gc_inventory.get("blocked_gc_eligible_object_count") or 0) > 0
        ),
        "strongest_duplicate_count": max(
            strongest_duplicate_count,
            int(sharing_inventory.get("max_duplicate_count") or 0),
        ),
        "sample_duplicate_object_refs": [
            str(item.get("object_ref") or "") for item in duplicate_observation_summaries[:limit]
        ],
        "sample_shareable_object_refs": [
            str(item.get("object_ref") or "") for item in shareable_candidates[:limit]
        ],
        "sample_converged_object_refs": [
            str(item.get("object_ref") or "") for item in converged_objects[:limit]
        ],
        "sample_settling_object_refs": [
            str(item.get("object_ref") or "") for item in settling_objects[:limit]
        ],
        "sample_settled_object_refs": [
            str(item.get("object_ref") or "") for item in settled_objects[:limit]
        ],
        "sample_reclaimable_object_refs": [
            str(item.get("object_ref") or "") for item in reclaimable_objects[:limit]
        ],
        "sample_blocked_gc_object_refs": [
            str(item.get("object_ref") or "") for item in blocked_gc_objects[:limit]
        ],
        "sample_settlement_reclaimable_object_refs": [
            str(item.get("object_ref") or "") for item in settlement_reclaimable_objects[:limit]
        ],
        "sample_settlement_blocked_object_refs": [
            str(item.get("object_ref") or "") for item in settlement_blocked_objects[:limit]
        ],
        "gaps": gaps,
    }


def query_heavy_object_authority_gap_trace_view(
    state_root: Path,
    *,
    object_ref: str | Path,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain one repo-local heavy-object candidate through filesystem evidence plus committed lifecycle coverage."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    resolved_repo_root = (
        Path(repo_root).expanduser().resolve()
        if str(repo_root or "").strip()
        else repo_root_from_runtime_root(state_root=runtime_root)
    )
    normalized_repo_root = str(resolved_repo_root) if resolved_repo_root is not None else ""
    normalized_object_ref = str(Path(object_ref).expanduser().resolve()) if str(object_ref or "").strip() else ""
    authority_runtime_root = _select_heavy_object_authority_runtime_root(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )

    filesystem_candidate_present = False
    candidate_path = Path(normalized_object_ref) if normalized_object_ref else Path()
    candidate_identity: dict[str, Any] = {}
    if normalized_object_ref:
        candidate_paths = _heavy_object_candidate_paths(
            resolved_repo_root or runtime_root.parent.resolve(),
            object_kind=normalized_object_kind,
        )
        filesystem_candidate_present = normalized_object_ref in {str(path) for path in candidate_paths}
        if filesystem_candidate_present and candidate_path.exists():
            try:
                candidate_identity = heavy_object_identity(
                    candidate_path,
                    object_kind=normalized_object_kind or "mathlib_pack",
                )
            except Exception:
                candidate_identity = {}
    candidate_object_id = str(candidate_identity.get("object_id") or "").strip()

    matching_registrations = _matching_heavy_object_registration_events(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        object_id=candidate_object_id,
        object_ref=normalized_object_ref,
        repo_root=normalized_repo_root,
    )
    latest_registration = dict(matching_registrations[-1] or {}) if matching_registrations else {}
    latest_discovery = _latest_matching_heavy_object_observation(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        object_id=candidate_object_id,
        object_ref=normalized_object_ref,
    )
    discovery_payload = dict(latest_discovery.get("payload") or {})
    registered_present = bool(latest_registration)
    discovery_present = bool(latest_discovery) and (
        not normalized_repo_root or str(discovery_payload.get("repo_root") or "").strip() == normalized_repo_root
    )
    coverage_state = "MANAGED" if (registered_present or discovery_present) else "UNMANAGED"

    gaps: list[str] = []
    if not filesystem_candidate_present:
        gaps.append("missing_matching_filesystem_heavy_object_candidate")
    elif coverage_state == "UNMANAGED":
        gaps.append("unmanaged_heavy_object_candidate_present")

    registration_payload = dict(latest_registration.get("payload") or {})
    return {
        "runtime_root": str(runtime_root),
        "authority_runtime_root": str(authority_runtime_root),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "repo_root": normalized_repo_root,
        "object_ref": normalized_object_ref,
        "filesystem_candidate_present": filesystem_candidate_present,
        "file_name": candidate_path.name if filesystem_candidate_present else "",
        "byte_size": (
            artifact_byte_size(candidate_path, ignore_runtime_heavy=normalized_object_kind == "publish_tree")
            if filesystem_candidate_present and candidate_path.exists()
            else 0
        ),
        "registered_present": registered_present,
        "discovery_present": discovery_present,
        "coverage_state": coverage_state,
        "object_id": str(
            registration_payload.get("object_id")
            or discovery_payload.get("object_id")
            or candidate_object_id
            or ""
        ),
        "latest_registration": (
            {**_event_summary(latest_registration), "payload": registration_payload}
            if latest_registration
            else {}
        ),
        "latest_discovery_observation": (
            {**_event_summary(latest_discovery), "payload": discovery_payload}
            if latest_discovery
            else {}
        ),
        "gaps": gaps,
    }


def query_heavy_object_authority_gap_audit_trace_view(
    state_root: Path,
    *,
    envelope_id: str,
) -> dict[str, Any]:
    """Explain one accepted heavy-object authority-gap audit via audit envelope, canonical fact, receipt, and final inventory."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_authority_gap_audit_requested",
        envelope_id=normalized_envelope_id,
    )
    canonical_result_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_authority_gap_audited",
        envelope_id=normalized_envelope_id,
    )
    receipt = _heavy_object_authority_gap_audit_receipt(
        runtime_root,
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    canonical_payload = dict(canonical_event.get("payload") or {})
    result_payload = dict(canonical_result_event.get("payload") or {})
    repo_root = str(canonical_payload.get("repo_root") or accepted_payload.get("repo_root") or "").strip()
    object_kind = str(canonical_payload.get("object_kind") or accepted_payload.get("object_kind") or "").strip()
    runtime_name = str(canonical_payload.get("runtime_name") or accepted_payload.get("runtime_name") or "").strip()
    audit_kind = str(canonical_payload.get("audit_kind") or accepted_payload.get("audit_kind") or "").strip()
    audit_signature = str(
        canonical_payload.get("audit_signature")
        or result_payload.get("audit_signature")
        or accepted_payload.get("audit_signature")
        or ""
    ).strip()
    trigger_kind = str(
        canonical_payload.get("trigger_kind")
        or result_payload.get("trigger_kind")
        or accepted_payload.get("trigger_kind")
        or ""
    ).strip()
    trigger_ref = str(
        canonical_payload.get("trigger_ref")
        or result_payload.get("trigger_ref")
        or accepted_payload.get("trigger_ref")
        or ""
    ).strip()
    requirement_event_id = str(
        canonical_payload.get("requirement_event_id")
        or result_payload.get("requirement_event_id")
        or accepted_payload.get("requirement_event_id")
        or ""
    ).strip()
    required_event = {}
    if requirement_event_id and repo_root:
        required_runtime_root = (Path(repo_root).expanduser().resolve() / ".loop" / "repo_control_plane").resolve()
        required_event = _event_by_id(required_runtime_root, event_id=requirement_event_id)

    final_inventory = query_heavy_object_authority_gap_inventory_view(
        runtime_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        repo_root=repo_root,
    )
    audit_effect = {
        "candidate_count": int(result_payload.get("candidate_count") or receipt.get("candidate_count") or 0),
        "registered_candidate_count": int(
            result_payload.get("registered_candidate_count") or receipt.get("registered_candidate_count") or 0
        ),
        "discovery_covered_candidate_count": int(
            result_payload.get("discovery_covered_candidate_count") or receipt.get("discovery_covered_candidate_count") or 0
        ),
        "managed_candidate_count": int(result_payload.get("managed_candidate_count") or receipt.get("managed_candidate_count") or 0),
        "unmanaged_candidate_count": int(
            result_payload.get("unmanaged_candidate_count") or receipt.get("unmanaged_candidate_count") or 0
        ),
        "strongest_duplicate_count": int(
            result_payload.get("strongest_duplicate_count") or receipt.get("strongest_duplicate_count") or 0
        ),
    }
    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "repo_root": repo_root,
        "object_kind": object_kind,
        "runtime_name": runtime_name,
        "audit_kind": audit_kind,
        "audit_signature": audit_signature,
        "trigger_kind": trigger_kind,
        "trigger_ref": trigger_ref,
        "requirement_event_id": requirement_event_id,
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_authority_gap_audit_event")
    if not canonical_result_event:
        gaps.append("missing_canonical_heavy_object_authority_gap_audited_event")
    if not receipt:
        gaps.append("missing_heavy_object_authority_gap_audit_receipt")
    if requirement_event_id and not required_event:
        gaps.append("missing_canonical_heavy_object_authority_gap_audit_required_event")
    gaps = list(dict.fromkeys(gaps))

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "canonical_result_event_present": bool(canonical_result_event),
        "required_event_present": bool(required_event),
        "audit_receipt_present": bool(receipt),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "canonical_result_event": _event_summary(canonical_result_event) if canonical_result_event else {},
        "required_event": _event_summary(required_event) if required_event else {},
        "audit_effect": audit_effect,
        "audit_receipt": receipt,
        "final_inventory": final_inventory,
        "gaps": gaps,
    }


def query_heavy_object_authority_gap_remediation_trace_view(
    state_root: Path,
    *,
    object_ref: str | Path,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain one repo-local heavy-object candidate plus its latest remediation attempt on the canonical discovery chain."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    resolved_repo_root = (
        Path(repo_root).expanduser().resolve()
        if str(repo_root or "").strip()
        else repo_root_from_runtime_root(state_root=runtime_root)
    )
    normalized_repo_root = str(resolved_repo_root) if resolved_repo_root is not None else ""
    normalized_object_ref = str(Path(object_ref).expanduser().resolve()) if str(object_ref or "").strip() else ""
    authority_runtime_root = _select_heavy_object_authority_runtime_root(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )

    authority_gap_trace = query_heavy_object_authority_gap_trace_view(
        runtime_root,
        object_ref=normalized_object_ref,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )
    matching_requests = _matching_heavy_object_discovery_request_events(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        object_ref=normalized_object_ref,
        repo_root=normalized_repo_root,
        discovery_kind="authority_gap_remediation",
    )
    latest_request = dict(matching_requests[-1] or {}) if matching_requests else {}
    latest_request_payload = dict(latest_request.get("payload") or {})
    latest_observations = _matching_heavy_object_discovery_observation_events(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        object_ref=normalized_object_ref,
        repo_root=normalized_repo_root,
        discovery_kind="authority_gap_remediation",
    )
    latest_observation = dict(latest_observations[-1] or {}) if latest_observations else {}
    latest_observation_payload = dict(latest_observation.get("payload") or {})
    latest_results = _matching_heavy_object_authority_gap_remediation_result_events(
        authority_runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        object_ref=normalized_object_ref,
        repo_root=normalized_repo_root,
    )
    latest_result = dict(latest_results[-1] or {}) if latest_results else {}
    remediation_envelope_id = str(latest_request_payload.get("envelope_id") or latest_observation_payload.get("envelope_id") or "")
    remediation_trace = (
        query_heavy_object_discovery_trace_view(runtime_root, envelope_id=remediation_envelope_id)
        if remediation_envelope_id
        else {}
    )
    gaps = [str(item) for item in list(authority_gap_trace.get("gaps") or [])]
    if not latest_request:
        gaps.append("missing_matching_heavy_object_authority_gap_remediation_request")
    elif not latest_result:
        gaps.append("missing_canonical_heavy_object_authority_gap_remediation_settled_event")
    elif remediation_trace and list(remediation_trace.get("gaps") or []):
        gaps.append("remediation_trace_gaps_present")
    gaps = list(dict.fromkeys(gaps))

    return {
        "runtime_root": str(runtime_root),
        "authority_runtime_root": str(
            authority_gap_trace.get("authority_runtime_root") or str(authority_runtime_root)
        ),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "repo_root": normalized_repo_root,
        "object_ref": normalized_object_ref,
        "current_coverage_state": str(authority_gap_trace.get("coverage_state") or ""),
        "authority_gap_trace": authority_gap_trace,
        "remediation_request_present": bool(latest_request),
        "remediation_trace_present": bool(remediation_trace),
        "canonical_result_event_present": bool(latest_result),
        "matching_remediation_request_count": len(matching_requests),
        "matching_remediation_observation_count": len(latest_observations),
        "matching_remediation_result_count": len(latest_results),
        "remediation_envelope_id": remediation_envelope_id,
        "latest_matching_remediation_request": _event_summary(latest_request) if latest_request else {},
        "latest_matching_remediation_observation": (
            {**_event_summary(latest_observation), "payload": latest_observation_payload}
            if latest_observation
            else {}
        ),
        "canonical_result_event": _event_summary(latest_result) if latest_result else {},
        "remediation_trace": remediation_trace,
        "gaps": gaps,
    }


def query_heavy_object_authority_gap_repo_remediation_trace_view(
    state_root: Path,
    *,
    envelope_id: str,
) -> dict[str, Any]:
    """Explain one accepted repo-root heavy-object remediation batch via audit, committed facts, receipt, and final inventory."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_authority_gap_repo_remediation_requested",
        envelope_id=normalized_envelope_id,
    )
    canonical_result_event = _event_for_envelope(
        runtime_root,
        event_type="heavy_object_authority_gap_repo_remediation_settled",
        envelope_id=normalized_envelope_id,
    )
    receipt = _heavy_object_authority_gap_repo_remediation_receipt(
        runtime_root,
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    canonical_payload = dict(canonical_event.get("payload") or {})
    canonical_result_payload = dict(canonical_result_event.get("payload") or {})
    repo_root = str(canonical_payload.get("repo_root") or accepted_payload.get("repo_root") or "").strip()
    object_kind = str(canonical_payload.get("object_kind") or accepted_payload.get("object_kind") or "").strip()
    runtime_name = str(canonical_payload.get("runtime_name") or accepted_payload.get("runtime_name") or "").strip()
    remediation_kind = str(
        canonical_payload.get("remediation_kind")
        or accepted_payload.get("remediation_kind")
        or ""
    ).strip()
    remediation_signature = str(
        canonical_payload.get("remediation_signature")
        or canonical_result_payload.get("remediation_signature")
        or accepted_payload.get("remediation_signature")
        or ""
    ).strip()
    trigger_kind = str(
        canonical_payload.get("trigger_kind")
        or canonical_result_payload.get("trigger_kind")
        or accepted_payload.get("trigger_kind")
        or ""
    ).strip()
    trigger_ref = str(
        canonical_payload.get("trigger_ref")
        or canonical_result_payload.get("trigger_ref")
        or accepted_payload.get("trigger_ref")
        or ""
    ).strip()
    requirement_event_id = str(
        canonical_payload.get("requirement_event_id")
        or canonical_result_payload.get("requirement_event_id")
        or accepted_payload.get("requirement_event_id")
        or ""
    ).strip()
    required_event = {}
    if requirement_event_id and repo_root:
        required_runtime_root = (Path(repo_root).expanduser().resolve() / ".loop" / "repo_control_plane").resolve()
        required_event = _event_by_id(required_runtime_root, event_id=requirement_event_id)

    final_inventory = query_heavy_object_authority_gap_remediation_inventory_view(
        runtime_root,
        runtime_name=runtime_name,
        object_kind=object_kind,
        repo_root=repo_root,
    )
    remediation_source = canonical_result_payload if canonical_result_payload else receipt
    remediation_effect = {
        "candidate_count": int(remediation_source.get("candidate_count") or 0),
        "requested_candidate_count": int(remediation_source.get("requested_candidate_count") or 0),
        "skipped_candidate_count": int(remediation_source.get("skipped_candidate_count") or 0),
        "fully_managed_after": bool(remediation_source.get("fully_managed_after")),
    }
    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "repo_root": repo_root,
        "object_kind": object_kind,
        "runtime_name": runtime_name,
        "remediation_kind": remediation_kind,
        "remediation_signature": remediation_signature,
        "trigger_kind": trigger_kind,
        "trigger_ref": trigger_ref,
        "requirement_event_id": requirement_event_id,
    }

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_heavy_object_authority_gap_repo_remediation_event")
    if not canonical_result_event:
        gaps.append("missing_canonical_heavy_object_authority_gap_repo_remediation_settled_event")
    if not receipt:
        gaps.append("missing_heavy_object_authority_gap_repo_remediation_receipt")
    if requirement_event_id and not required_event:
        gaps.append("missing_canonical_heavy_object_authority_gap_repo_remediation_required_event")
    if list(final_inventory.get("gaps") or []):
        gaps.append("final_inventory_gaps_present")
    gaps = list(dict.fromkeys(gaps))

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "canonical_result_event_present": bool(canonical_result_event),
        "required_event_present": bool(required_event),
        "remediation_receipt_present": bool(receipt),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "canonical_result_event": _event_summary(canonical_result_event) if canonical_result_event else {},
        "required_event": _event_summary(required_event) if required_event else {},
        "remediation_effect": remediation_effect,
        "remediation_receipt": receipt,
        "final_inventory": final_inventory,
        "gaps": gaps,
    }


def query_heavy_object_authority_gap_remediation_inventory_view(
    state_root: Path,
    *,
    runtime_name: str = "",
    object_kind: str = "",
    repo_root: str | Path = "",
) -> dict[str, Any]:
    """Explain repo-local heavy-object candidates plus their latest remediation-request coverage."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_runtime_name = str(runtime_name or "").strip()
    normalized_object_kind = str(object_kind or "").strip()
    resolved_repo_root = (
        Path(repo_root).expanduser().resolve()
        if str(repo_root or "").strip()
        else repo_root_from_runtime_root(state_root=runtime_root)
    )
    normalized_repo_root = str(resolved_repo_root) if resolved_repo_root is not None else ""
    authority_runtime_root = _select_heavy_object_authority_runtime_root(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )

    base_inventory = query_heavy_object_authority_gap_inventory_view(
        runtime_root,
        runtime_name=normalized_runtime_name,
        object_kind=normalized_object_kind,
        repo_root=normalized_repo_root,
    )
    candidate_summaries = [
        dict(item)
        for item in list(base_inventory.get("managed_candidates") or []) + list(base_inventory.get("unmanaged_candidates") or [])
    ]
    candidate_summaries.sort(
        key=lambda item: (
            str(item.get("file_name") or ""),
            str(item.get("object_ref") or ""),
        )
    )

    candidates: list[dict[str, Any]] = []
    remediation_requested_candidate_count = 0
    remediation_result_candidate_count = 0
    remediated_candidate_count = 0
    pending_remediation_candidate_count = 0
    unrequested_unmanaged_candidate_count = 0
    strongest_duplicate_count = int(base_inventory.get("strongest_duplicate_count") or 0)
    for candidate in candidate_summaries:
        object_ref = str(candidate.get("object_ref") or "").strip()
        coverage_state = str(candidate.get("coverage_state") or "").strip()
        matching_requests = _matching_heavy_object_discovery_request_events(
            authority_runtime_root,
            runtime_name=normalized_runtime_name,
            object_kind=normalized_object_kind,
            object_ref=object_ref,
            repo_root=normalized_repo_root,
            discovery_kind="authority_gap_remediation",
        )
        latest_request = dict(matching_requests[-1] or {}) if matching_requests else {}
        latest_request_payload = dict(latest_request.get("payload") or {})
        matching_observations = _matching_heavy_object_discovery_observation_events(
            authority_runtime_root,
            runtime_name=normalized_runtime_name,
            object_kind=normalized_object_kind,
            object_ref=object_ref,
            repo_root=normalized_repo_root,
            discovery_kind="authority_gap_remediation",
        )
        latest_observation = dict(matching_observations[-1] or {}) if matching_observations else {}
        latest_observation_payload = dict(latest_observation.get("payload") or {})
        matching_results = _matching_heavy_object_authority_gap_remediation_result_events(
            authority_runtime_root,
            runtime_name=normalized_runtime_name,
            object_kind=normalized_object_kind,
            object_ref=object_ref,
            repo_root=normalized_repo_root,
        )
        latest_result = dict(matching_results[-1] or {}) if matching_results else {}
        remediation_request_present = bool(latest_request)
        remediation_trace_present = bool(latest_request and latest_observation)
        remediation_result_event_present = bool(latest_result)
        remediation_envelope_id = str(
            latest_request_payload.get("envelope_id") or latest_observation_payload.get("envelope_id") or ""
        )
        if remediation_request_present:
            remediation_requested_candidate_count += 1
        if remediation_result_event_present:
            remediation_result_candidate_count += 1
        if coverage_state == "MANAGED" and remediation_result_event_present:
            remediation_state = "REMEDIATED"
            remediated_candidate_count += 1
        elif remediation_request_present:
            remediation_state = "REQUESTED"
            pending_remediation_candidate_count += 1
        elif coverage_state == "UNMANAGED":
            remediation_state = "UNREQUESTED"
            unrequested_unmanaged_candidate_count += 1
        else:
            remediation_state = "NOT_REQUIRED"
        strongest_duplicate_count = max(
            strongest_duplicate_count,
            int(latest_observation_payload.get("duplicate_count") or 0),
        )
        candidates.append(
            {
                **candidate,
                "remediation_request_present": remediation_request_present,
                "remediation_trace_present": remediation_trace_present,
                "remediation_result_event_present": remediation_result_event_present,
                "matching_remediation_request_count": len(matching_requests),
                "matching_remediation_observation_count": len(matching_observations),
                "matching_remediation_result_count": len(matching_results),
                "remediation_envelope_id": remediation_envelope_id,
                "remediation_state": remediation_state,
                "latest_matching_remediation_request": _event_summary(latest_request) if latest_request else {},
                "latest_matching_remediation_observation": (
                    {**_event_summary(latest_observation), "payload": latest_observation_payload}
                    if latest_observation
                    else {}
                ),
                "latest_matching_remediation_result": _event_summary(latest_result) if latest_result else {},
            }
        )

    gaps: list[str] = []
    for gap in list(base_inventory.get("gaps") or []):
        gap_text = str(gap)
        if gap_text == "unmanaged_heavy_object_candidates_present":
            continue
        gaps.append(gap_text)
    if pending_remediation_candidate_count:
        gaps.append("pending_heavy_object_authority_gap_remediation_present")
    if remediation_requested_candidate_count > remediation_result_candidate_count:
        gaps.append("missing_canonical_heavy_object_authority_gap_remediation_settled_event")
    if unrequested_unmanaged_candidate_count:
        gaps.append("unmanaged_heavy_object_candidates_without_remediation_request_present")
    gaps = list(dict.fromkeys(gaps))

    return {
        "runtime_root": str(runtime_root),
        "authority_runtime_root": str(base_inventory.get("authority_runtime_root") or str(authority_runtime_root)),
        "read_only": True,
        "runtime_name": normalized_runtime_name,
        "object_kind": normalized_object_kind,
        "repo_root": normalized_repo_root,
        "filesystem_candidate_count": int(base_inventory.get("filesystem_candidate_count") or 0),
        "managed_candidate_count": int(base_inventory.get("managed_candidate_count") or 0),
        "unmanaged_candidate_count": int(base_inventory.get("unmanaged_candidate_count") or 0),
        "remediation_requested_candidate_count": remediation_requested_candidate_count,
        "remediation_result_candidate_count": remediation_result_candidate_count,
        "remediated_candidate_count": remediated_candidate_count,
        "pending_remediation_candidate_count": pending_remediation_candidate_count,
        "unrequested_unmanaged_candidate_count": unrequested_unmanaged_candidate_count,
        "strongest_duplicate_count": strongest_duplicate_count,
        "candidates": candidates,
        "gaps": gaps,
    }


def query_terminal_result_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted terminal result via audit, committed events, visible projection state, and node lineage."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="terminal_result_recorded",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    node_id = str(accepted_payload.get("node_id") or accepted_audit.get("source") or "")
    consistency = query_projection_consistency_view(runtime_root)
    kernel_state = load_kernel_state(runtime_root).to_dict()
    kernel_nodes = dict(kernel_state.get("nodes") or {})
    blocked_reasons = {str(k): str(v or "") for k, v in dict(kernel_state.get("blocked_reasons") or {}).items()}
    source_node = (
        _node_projection_summary(
            runtime_root,
            node_id=node_id,
            blocked_reason=blocked_reasons.get(node_id, ""),
        )
        if node_id
        else {}
    )
    source_node_lineage = (
        query_node_lineage_trace_view(runtime_root, node_id=node_id)
        if node_id
        else {}
    )

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_terminal_result_event")
    if node_id and node_id not in kernel_nodes:
        gaps.append("projection_not_materialized")

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "node_id": node_id,
        "outcome": str(accepted_payload.get("outcome") or ""),
        "node_status": str(accepted_payload.get("node_status") or ""),
    }
    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "projection_visibility": str(consistency.get("visibility_state") or ""),
        "source_node": source_node,
        "source_node_lineage": source_node_lineage,
        "gaps": gaps,
    }


def query_evaluator_result_trace_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Explain one accepted evaluator result via audit, committed events, visible authority state, and node lineage."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    compatibility_event = _event_for_envelope(
        runtime_root,
        event_type="control_envelope_accepted",
        envelope_id=normalized_envelope_id,
    )
    canonical_event = _event_for_envelope(
        runtime_root,
        event_type="evaluator_result_recorded",
        envelope_id=normalized_envelope_id,
    )

    accepted_payload = dict(accepted_audit.get("payload") or {})
    node_id = str(accepted_audit.get("source") or "").strip()
    consistency = query_projection_consistency_view(runtime_root)
    authority_view = query_authority_view(runtime_root, materialize_projection=False)
    source_node = next(
        (dict(item or {}) for item in list(authority_view.get("node_graph") or []) if str(item.get("node_id") or "") == node_id),
        {},
    )
    source_node_lineage = (
        query_node_lineage_trace_view(runtime_root, node_id=node_id)
        if node_id
        else {}
    )
    active_evaluator_lanes = [
        dict(item or {})
        for item in list(authority_view.get("active_evaluator_lanes") or [])
        if str(item.get("node_id") or "") == node_id
    ]

    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if not compatibility_event:
        gaps.append("missing_compatibility_envelope_mirror")
    if not canonical_event:
        gaps.append("missing_canonical_evaluator_result_event")
    if node_id and not source_node:
        gaps.append("projection_not_materialized")
    elif node_id and not active_evaluator_lanes:
        gaps.append("projection_not_materialized")

    accepted_summary = {
        "envelope_id": str(accepted_audit.get("envelope_id") or ""),
        "classification": str(accepted_audit.get("classification") or ""),
        "source": str(accepted_audit.get("source") or ""),
        "round_id": str(accepted_audit.get("round_id") or ""),
        "payload_type": str(accepted_audit.get("payload_type") or ""),
        "status": str(accepted_audit.get("status") or ""),
        "node_id": node_id,
        "lane": str(accepted_payload.get("lane") or ""),
        "verdict": str(accepted_payload.get("verdict") or ""),
        "retryable": bool(accepted_payload.get("retryable")),
    }
    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "accepted_audit_present": bool(accepted_audit),
        "compatibility_event_present": bool(compatibility_event),
        "canonical_event_present": bool(canonical_event),
        "accepted_envelope": accepted_summary,
        "compatibility_event": _event_summary(compatibility_event) if compatibility_event else {},
        "canonical_event": _event_summary(canonical_event) if canonical_event else {},
        "projection_visibility": str(consistency.get("visibility_state") or ""),
        "source_node": source_node,
        "source_node_lineage": source_node_lineage,
        "active_evaluator_lanes": active_evaluator_lanes,
        "gaps": gaps,
    }


def query_launch_lineage_trace_view(state_root: Path, *, launch_event_id: str) -> dict[str, Any]:
    """Explain one launch identity through canonical launch, identity, supervision, and liveness facts."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_launch_event_id = str(launch_event_id or "").strip()
    launch_started_event = _event_for_launch(
        runtime_root,
        event_type="launch_started",
        launch_event_id=normalized_launch_event_id,
    )
    launch_reused_existing_event = _event_for_launch(
        runtime_root,
        event_type="launch_reused_existing",
        launch_event_id=normalized_launch_event_id,
    )
    process_identity_event = _event_for_launch(
        runtime_root,
        event_type="process_identity_confirmed",
        launch_event_id=normalized_launch_event_id,
    )
    supervision_attached_event = _event_for_launch(
        runtime_root,
        event_type="supervision_attached",
        launch_event_id=normalized_launch_event_id,
    )

    launch_origin_event = launch_started_event or launch_reused_existing_event
    if launch_started_event:
        launch_origin_kind = "launch_started"
    elif launch_reused_existing_event:
        launch_origin_kind = "launch_reused_existing"
    else:
        launch_origin_kind = ""

    source_node_id = next(
        (
            str(candidate.get("node_id") or "").strip()
            for candidate in (
                launch_origin_event,
                process_identity_event,
                supervision_attached_event,
            )
            if str(candidate.get("node_id") or "").strip()
        ),
        "",
    )
    consistency = query_projection_consistency_view(runtime_root)
    authority_view = query_authority_view(runtime_root, materialize_projection=False)
    liveness_view = query_runtime_liveness_view(runtime_root)
    source_node = next(
        (
            dict(item or {})
            for item in list(authority_view.get("node_graph") or [])
            if str(item.get("node_id") or "") == source_node_id
        ),
        {},
    )
    raw_liveness_head = dict(
        dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).get(source_node_id) or {}
    )
    effective_liveness_head = dict(
        dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(source_node_id) or {}
    )

    gaps: list[str] = []
    if not launch_origin_event:
        gaps.append("missing_canonical_launch_origin_event")
    if not process_identity_event:
        gaps.append("missing_canonical_process_identity_confirmed_event")
    if not supervision_attached_event:
        gaps.append("missing_canonical_supervision_attached_event")
    if source_node_id and not source_node:
        gaps.append("projection_not_materialized")
    if source_node_id and not raw_liveness_head:
        gaps.append("missing_runtime_liveness_head")
    if source_node_id and not effective_liveness_head:
        gaps.append("missing_effective_runtime_liveness_head")
    if str(consistency.get("visibility_state") or "") != "atomically_visible":
        gaps.append("projection_not_fully_visible")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "launch_event_id": normalized_launch_event_id,
        "launch_origin_kind": launch_origin_kind,
        "launch_started_present": bool(launch_started_event),
        "launch_reused_existing_present": bool(launch_reused_existing_event),
        "process_identity_present": bool(process_identity_event),
        "supervision_attached_present": bool(supervision_attached_event),
        "launch_started_event": _event_summary(launch_started_event) if launch_started_event else {},
        "launch_reused_existing_event": (
            _event_summary(launch_reused_existing_event) if launch_reused_existing_event else {}
        ),
        "process_identity_event": _event_summary(process_identity_event) if process_identity_event else {},
        "supervision_attached_event": _event_summary(supervision_attached_event) if supervision_attached_event else {},
        "projection_visibility": str(consistency.get("visibility_state") or ""),
        "source_node": source_node,
        "raw_liveness_head": raw_liveness_head,
        "effective_liveness_head": effective_liveness_head,
        "gaps": gaps,
    }


def query_lease_lineage_trace_view(state_root: Path, *, node_id: str) -> dict[str, Any]:
    """Explain one node's current visible lease lineage through committed lease facts and liveness state."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_node_id = str(node_id or "").strip()
    consistency = query_projection_consistency_view(runtime_root)
    authority_view = query_authority_view(runtime_root, materialize_projection=False)
    liveness_view = query_runtime_liveness_view(runtime_root)
    source_node = next(
        (
            dict(item or {})
            for item in list(authority_view.get("node_graph") or [])
            if str(item.get("node_id") or "") == normalized_node_id
        ),
        {},
    )
    raw_liveness_head = dict(
        dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).get(normalized_node_id) or {}
    )
    effective_liveness_head = dict(
        dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(normalized_node_id) or {}
    )
    lease_anchor = dict(effective_liveness_head or raw_liveness_head)
    lease_anchor_payload = dict(
        lease_anchor.get("payload")
        or raw_liveness_head.get("payload")
        or effective_liveness_head.get("payload")
        or {}
    )
    normalized_launch_event_id = str(lease_anchor_payload.get("launch_event_id") or "").strip()
    normalized_lease_owner_id = str(
        lease_anchor.get("lease_owner_id")
        or raw_liveness_head.get("lease_owner_id")
        or effective_liveness_head.get("lease_owner_id")
        or lease_anchor_payload.get("lease_owner_id")
        or ""
    ).strip()
    try:
        normalized_lease_epoch = int(
            lease_anchor.get("lease_epoch")
            or raw_liveness_head.get("lease_epoch")
            or effective_liveness_head.get("lease_epoch")
            or lease_anchor_payload.get("lease_epoch")
            or 0
        )
    except (TypeError, ValueError):
        normalized_lease_epoch = 0
    launch_started_event = _event_for_launch(
        runtime_root,
        event_type="launch_started",
        launch_event_id=normalized_launch_event_id,
    )
    launch_reused_existing_event = _event_for_launch(
        runtime_root,
        event_type="launch_reused_existing",
        launch_event_id=normalized_launch_event_id,
    )
    process_identity_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="process_identity_confirmed",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
    )
    supervision_attached_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="supervision_attached",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
        lease_owner_id=normalized_lease_owner_id,
        lease_epoch=normalized_lease_epoch,
    )
    heartbeat_observed_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="heartbeat_observed",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
        lease_owner_id=normalized_lease_owner_id,
        lease_epoch=normalized_lease_epoch,
    )
    lease_expired_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="lease_expired",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
        lease_owner_id=normalized_lease_owner_id,
        lease_epoch=normalized_lease_epoch,
    )
    process_exit_observed_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="process_exit_observed",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
        lease_owner_id=normalized_lease_owner_id,
        lease_epoch=normalized_lease_epoch,
    )
    runtime_loss_confirmed_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="runtime_loss_confirmed",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
        lease_owner_id=normalized_lease_owner_id,
        lease_epoch=normalized_lease_epoch,
    )
    supervision_settled_event = _latest_lineage_event_for_node(
        runtime_root,
        event_type="supervision_settled",
        node_id=normalized_node_id,
        launch_event_id=normalized_launch_event_id,
        lease_owner_id=normalized_lease_owner_id,
        lease_epoch=normalized_lease_epoch,
    )

    if launch_started_event:
        launch_origin_kind = "launch_started"
    elif launch_reused_existing_event:
        launch_origin_kind = "launch_reused_existing"
    else:
        launch_origin_kind = ""
    current_attachment_state = str(
        effective_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("raw_attachment_state")
        or ""
    )
    current_observation_kind = str(
        lease_anchor_payload.get("observation_kind")
        or lease_anchor.get("observation_kind")
        or raw_liveness_head.get("observation_kind")
        or ""
    )

    gaps: list[str] = []
    if not raw_liveness_head:
        gaps.append("missing_runtime_liveness_head")
    if not effective_liveness_head:
        gaps.append("missing_effective_runtime_liveness_head")
    if normalized_launch_event_id and not (launch_started_event or launch_reused_existing_event):
        gaps.append("missing_canonical_launch_origin_event")
    if normalized_launch_event_id and not process_identity_event:
        gaps.append("missing_canonical_process_identity_confirmed_event")
    if normalized_launch_event_id and not supervision_attached_event:
        gaps.append("missing_canonical_supervision_attached_event")
    if current_attachment_state == "ATTACHED" and not heartbeat_observed_event:
        gaps.append("missing_canonical_heartbeat_observed_event")
    if current_observation_kind == "lease_expired" and not lease_expired_event:
        gaps.append("missing_canonical_lease_expired_event")
    if current_observation_kind == "runtime_loss_confirmed" and not runtime_loss_confirmed_event:
        gaps.append("missing_canonical_runtime_loss_confirmed_event")
    if current_observation_kind.startswith("child_supervision_settled") and not supervision_settled_event:
        gaps.append("missing_canonical_supervision_settled_event")
    if (
        current_attachment_state in {"LOST", "TERMINAL"}
        and current_observation_kind.startswith("child_supervision_settled")
        and not process_exit_observed_event
    ):
        gaps.append("missing_canonical_process_exit_observed_event")
    if normalized_node_id and not source_node:
        gaps.append("projection_not_materialized")
    if str(consistency.get("visibility_state") or "") != "atomically_visible":
        gaps.append("projection_not_fully_visible")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "node_id": normalized_node_id,
        "launch_event_id": normalized_launch_event_id,
        "lease_owner_id": normalized_lease_owner_id,
        "lease_epoch": normalized_lease_epoch,
        "launch_origin_kind": launch_origin_kind,
        "launch_origin_present": bool(launch_started_event or launch_reused_existing_event),
        "process_identity_present": bool(process_identity_event),
        "supervision_attached_present": bool(supervision_attached_event),
        "heartbeat_observed_present": bool(heartbeat_observed_event),
        "lease_expired_present": bool(lease_expired_event),
        "process_exit_observed_present": bool(process_exit_observed_event),
        "runtime_loss_confirmed_present": bool(runtime_loss_confirmed_event),
        "supervision_settled_present": bool(supervision_settled_event),
        "launch_started_event": _event_summary(launch_started_event) if launch_started_event else {},
        "launch_reused_existing_event": (
            _event_summary(launch_reused_existing_event) if launch_reused_existing_event else {}
        ),
        "process_identity_event": _event_summary(process_identity_event) if process_identity_event else {},
        "supervision_attached_event": _event_summary(supervision_attached_event) if supervision_attached_event else {},
        "heartbeat_observed_event": _event_summary(heartbeat_observed_event) if heartbeat_observed_event else {},
        "lease_expired_event": _event_summary(lease_expired_event) if lease_expired_event else {},
        "process_exit_observed_event": (
            _event_summary(process_exit_observed_event) if process_exit_observed_event else {}
        ),
        "runtime_loss_confirmed_event": (
            _event_summary(runtime_loss_confirmed_event) if runtime_loss_confirmed_event else {}
        ),
        "supervision_settled_event": _event_summary(supervision_settled_event) if supervision_settled_event else {},
        "projection_visibility": str(consistency.get("visibility_state") or ""),
        "source_node": source_node,
        "raw_liveness_head": raw_liveness_head,
        "effective_liveness_head": effective_liveness_head,
        "gaps": gaps,
    }


def query_runtime_identity_trace_view(state_root: Path, *, node_id: str) -> dict[str, Any]:
    """Explain one node's current visible runtime identity through canonical identity and liveness facts."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_node_id = str(node_id or "").strip()
    lease_lineage = query_lease_lineage_trace_view(runtime_root, node_id=normalized_node_id)
    launch_event_id = str(lease_lineage.get("launch_event_id") or "").strip()
    launch_lineage = (
        query_launch_lineage_trace_view(runtime_root, launch_event_id=launch_event_id)
        if launch_event_id
        else {}
    )

    process_identity_event = dict(lease_lineage.get("process_identity_event") or {})
    supervision_attached_event = dict(lease_lineage.get("supervision_attached_event") or {})
    heartbeat_observed_event = dict(lease_lineage.get("heartbeat_observed_event") or {})
    lease_expired_event = dict(lease_lineage.get("lease_expired_event") or {})
    process_exit_observed_event = dict(lease_lineage.get("process_exit_observed_event") or {})
    runtime_loss_confirmed_event = dict(lease_lineage.get("runtime_loss_confirmed_event") or {})
    supervision_settled_event = dict(lease_lineage.get("supervision_settled_event") or {})
    raw_liveness_head = dict(lease_lineage.get("raw_liveness_head") or {})
    effective_liveness_head = dict(lease_lineage.get("effective_liveness_head") or {})
    source_node = dict(lease_lineage.get("source_node") or {})

    dominant_liveness_head = dict(effective_liveness_head or raw_liveness_head)
    dominant_payload = dict(
        dominant_liveness_head.get("payload")
        or effective_liveness_head.get("payload")
        or raw_liveness_head.get("payload")
        or {}
    )
    current_attachment_state = str(
        effective_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("raw_attachment_state")
        or dominant_liveness_head.get("effective_attachment_state")
        or dominant_liveness_head.get("raw_attachment_state")
        or ""
    )
    current_observation_kind = str(
        dominant_payload.get("observation_kind")
        or dominant_liveness_head.get("observation_kind")
        or ""
    )
    try:
        visible_pid = int(
            dominant_liveness_head.get("pid")
            or raw_liveness_head.get("pid")
            or effective_liveness_head.get("pid")
            or dominant_payload.get("pid")
            or 0
        )
    except (TypeError, ValueError):
        visible_pid = 0
    try:
        visible_lease_epoch = int(
            dominant_liveness_head.get("lease_epoch")
            or raw_liveness_head.get("lease_epoch")
            or effective_liveness_head.get("lease_epoch")
            or dominant_payload.get("lease_epoch")
            or 0
        )
    except (TypeError, ValueError):
        visible_lease_epoch = 0

    closure_kind = ""
    closure_event: dict[str, Any] = {}
    if current_observation_kind == "lease_expired":
        closure_kind = "lease_expired"
        closure_event = dict(lease_expired_event or {})
    elif current_observation_kind == "runtime_loss_confirmed":
        closure_kind = "runtime_loss_confirmed"
        closure_event = dict(runtime_loss_confirmed_event or {})
    elif current_observation_kind.startswith("child_supervision_settled"):
        closure_kind = "supervision_settled"
        closure_event = dict(supervision_settled_event or process_exit_observed_event or {})
    elif current_attachment_state in {"LOST", "TERMINAL"} and process_exit_observed_event:
        closure_kind = "process_exit_observed"
        closure_event = dict(process_exit_observed_event or {})

    supervision_identity_event = dict(closure_event or {})
    if not supervision_identity_event and heartbeat_observed_event:
        supervision_identity_event = dict(heartbeat_observed_event or {})
    if not supervision_identity_event and supervision_attached_event:
        supervision_identity_event = dict(supervision_attached_event or {})

    process_identity_anchor = {
        "source_event_type": str(process_identity_event.get("event_type") or ""),
        "launch_event_id": str(process_identity_event.get("launch_event_id") or launch_event_id or ""),
        "pid": int(process_identity_event.get("pid") or 0),
        "process_fingerprint": str(process_identity_event.get("process_fingerprint") or ""),
        "attachment_state": str(process_identity_event.get("attachment_state") or ""),
        "observation_kind": str(process_identity_event.get("observation_kind") or ""),
        "status_result_ref": str(process_identity_event.get("status_result_ref") or ""),
    }
    supervision_identity_anchor = {
        "source_event_type": str(supervision_identity_event.get("event_type") or ""),
        "launch_event_id": str(supervision_identity_event.get("launch_event_id") or launch_event_id or ""),
        "lease_owner_id": str(
            supervision_identity_event.get("lease_owner_id")
            or lease_lineage.get("lease_owner_id")
            or ""
        ).strip(),
        "lease_epoch": int(
            supervision_identity_event.get("lease_epoch")
            or lease_lineage.get("lease_epoch")
            or 0
        ),
        "pid": int(supervision_identity_event.get("pid") or 0),
        "process_fingerprint": str(supervision_identity_event.get("process_fingerprint") or ""),
        "process_started_at_utc": str(supervision_identity_event.get("process_started_at_utc") or ""),
        "runtime_ref": str(
            supervision_identity_event.get("runtime_ref")
            or closure_event.get("runtime_ref")
            or supervision_attached_event.get("runtime_ref")
            or ""
        ),
    }
    current_runtime_identity = {
        "source_kind": "effective_liveness_head" if effective_liveness_head else "raw_liveness_head" if raw_liveness_head else "",
        "launch_event_id": launch_event_id,
        "lease_owner_id": str(
            dominant_liveness_head.get("lease_owner_id")
            or raw_liveness_head.get("lease_owner_id")
            or effective_liveness_head.get("lease_owner_id")
            or dominant_payload.get("lease_owner_id")
            or ""
        ).strip(),
        "lease_epoch": visible_lease_epoch,
        "pid": visible_pid,
        "process_fingerprint": str(
            dominant_liveness_head.get("process_fingerprint")
            or raw_liveness_head.get("process_fingerprint")
            or effective_liveness_head.get("process_fingerprint")
            or dominant_payload.get("process_fingerprint")
            or ""
        ),
        "process_started_at_utc": str(
            dominant_liveness_head.get("process_started_at_utc")
            or raw_liveness_head.get("process_started_at_utc")
            or effective_liveness_head.get("process_started_at_utc")
            or dominant_payload.get("process_started_at_utc")
            or ""
        ),
        "raw_attachment_state": str(
            raw_liveness_head.get("raw_attachment_state")
            or dominant_liveness_head.get("raw_attachment_state")
            or current_attachment_state
        ),
        "effective_attachment_state": current_attachment_state,
        "observation_kind": current_observation_kind,
        "observed_at": str(
            dominant_liveness_head.get("observed_at")
            or raw_liveness_head.get("observed_at")
            or effective_liveness_head.get("observed_at")
            or dominant_payload.get("observed_at")
            or ""
        ),
    }

    gaps: list[str] = []
    for gap in list(lease_lineage.get("gaps") or []):
        normalized_gap = str(gap)
        if normalized_gap and normalized_gap not in gaps:
            gaps.append(normalized_gap)
    for gap in list(launch_lineage.get("gaps") or []):
        normalized_gap = str(gap)
        if normalized_gap and normalized_gap not in gaps:
            gaps.append(normalized_gap)

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "node_id": normalized_node_id,
        "launch_event_id": launch_event_id,
        "lease_owner_id": str(lease_lineage.get("lease_owner_id") or "").strip(),
        "lease_epoch": int(lease_lineage.get("lease_epoch") or 0),
        "launch_origin_kind": str(
            lease_lineage.get("launch_origin_kind")
            or launch_lineage.get("launch_origin_kind")
            or ""
        ),
        "current_attachment_state": current_attachment_state,
        "current_observation_kind": current_observation_kind,
        "launch_origin_present": bool(lease_lineage.get("launch_origin_present") or launch_lineage.get("launch_origin_kind")),
        "process_identity_present": bool(lease_lineage.get("process_identity_present")),
        "supervision_attached_present": bool(lease_lineage.get("supervision_attached_present")),
        "heartbeat_observed_present": bool(lease_lineage.get("heartbeat_observed_present")),
        "lease_expired_present": bool(lease_lineage.get("lease_expired_present")),
        "process_exit_observed_present": bool(lease_lineage.get("process_exit_observed_present")),
        "runtime_loss_confirmed_present": bool(lease_lineage.get("runtime_loss_confirmed_present")),
        "supervision_settled_present": bool(lease_lineage.get("supervision_settled_present")),
        "launch_origin_event": dict(
            launch_lineage.get("launch_started_event")
            or launch_lineage.get("launch_reused_existing_event")
            or {}
        ),
        "process_identity_event": process_identity_event,
        "supervision_attached_event": supervision_attached_event,
        "heartbeat_observed_event": heartbeat_observed_event,
        "lease_expired_event": lease_expired_event,
        "process_exit_observed_event": process_exit_observed_event,
        "runtime_loss_confirmed_event": runtime_loss_confirmed_event,
        "supervision_settled_event": supervision_settled_event,
        "process_identity_anchor": process_identity_anchor,
        "supervision_identity_anchor": supervision_identity_anchor,
        "current_runtime_identity": current_runtime_identity,
        "closure_kind": closure_kind,
        "closure_event": _event_summary(closure_event) if closure_event else {},
        "source_node": source_node,
        "raw_liveness_head": raw_liveness_head,
        "effective_liveness_head": effective_liveness_head,
        "launch_lineage_gaps": [str(item) for item in list(launch_lineage.get("gaps") or [])],
        "lease_lineage_gaps": [str(item) for item in list(lease_lineage.get("gaps") or [])],
        "gaps": gaps,
    }


def query_node_lineage_trace_view(state_root: Path, *, node_id: str) -> dict[str, Any]:
    """Explain one node through its visible ancestor chain plus nested launch/lease/runtime lineages."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_node_id = str(node_id or "").strip()
    consistency = query_projection_consistency_view(runtime_root)
    authority_view = query_authority_view(runtime_root, materialize_projection=False)
    node_graph = [
        dict(item or {})
        for item in list(authority_view.get("node_graph") or [])
    ]
    nodes_by_id = {
        str(item.get("node_id") or ""): dict(item or {})
        for item in node_graph
        if str(item.get("node_id") or "")
    }
    current_node = dict(nodes_by_id.get(normalized_node_id) or {})

    lineage_chain_reversed: list[dict[str, Any]] = []
    gaps: list[str] = []
    seen_node_ids: set[str] = set()
    cursor_node_id = normalized_node_id
    while cursor_node_id:
        if cursor_node_id in seen_node_ids:
            gaps.append("cycle_in_parent_lineage")
            break
        node_payload = dict(nodes_by_id.get(cursor_node_id) or {})
        if not node_payload:
            gaps.append("missing_ancestor_node")
            break
        lineage_chain_reversed.append(node_payload)
        seen_node_ids.add(cursor_node_id)
        cursor_node_id = str(node_payload.get("parent_node_id") or "").strip()
    lineage_chain = list(reversed(lineage_chain_reversed))

    lease_lineage = (
        query_lease_lineage_trace_view(runtime_root, node_id=normalized_node_id)
        if current_node
        else {}
    )
    launch_event_id = str(dict(lease_lineage or {}).get("launch_event_id") or "").strip()
    launch_lineage = (
        query_launch_lineage_trace_view(runtime_root, launch_event_id=launch_event_id)
        if launch_event_id
        else {}
    )
    runtime_identity_trace = (
        query_runtime_identity_trace_view(runtime_root, node_id=normalized_node_id)
        if current_node
        else {}
    )

    if not current_node:
        gaps.append("missing_node_projection")
    if current_node and list(dict(lease_lineage or {}).get("gaps") or []):
        gaps.append("lease_lineage_gaps_present")
    if launch_event_id and list(dict(launch_lineage or {}).get("gaps") or []):
        gaps.append("launch_lineage_gaps_present")
    if current_node and list(dict(runtime_identity_trace or {}).get("gaps") or []):
        gaps.append("runtime_identity_trace_gaps_present")
    if str(consistency.get("visibility_state") or "") != "atomically_visible":
        gaps.append("projection_not_fully_visible")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "node_id": normalized_node_id,
        "root_node_id": str(authority_view.get("root_node_id") or ""),
        "current_node": current_node,
        "lineage_chain": lineage_chain,
        "ancestor_node_ids": [str(item.get("node_id") or "") for item in lineage_chain[:-1]],
        "launch_lineage": launch_lineage,
        "lease_lineage": lease_lineage,
        "runtime_identity_trace": runtime_identity_trace,
        "projection_visibility": str(consistency.get("visibility_state") or ""),
        "gaps": gaps,
    }


def query_runtime_identity_parity_debugger_view(state_root: Path, *, node_id: str) -> dict[str, Any]:
    """Compare the trusted current runtime identity with the visible runtime-identity trace surface."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_node_id = str(node_id or "").strip()
    authority_view = query_authority_view(runtime_root, materialize_projection=False)
    liveness_view = query_runtime_liveness_view(runtime_root)
    node_graph = [dict(item or {}) for item in list(authority_view.get("node_graph") or [])]
    current_node = next(
        (
            dict(item or {})
            for item in node_graph
            if str(item.get("node_id") or "") == normalized_node_id
        ),
        {},
    )
    raw_liveness_head = dict(
        dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).get(normalized_node_id) or {}
    )
    effective_liveness_head = dict(
        dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(normalized_node_id) or {}
    )
    dominant_liveness_head = dict(effective_liveness_head or raw_liveness_head)
    dominant_payload = dict(
        dominant_liveness_head.get("payload")
        or effective_liveness_head.get("payload")
        or raw_liveness_head.get("payload")
        or {}
    )
    expected_launch_event_id = str(
        dominant_payload.get("launch_event_id")
        or dominant_liveness_head.get("launch_event_id")
        or ""
    ).strip()
    expected_lease_owner_id = str(
        dominant_liveness_head.get("lease_owner_id")
        or raw_liveness_head.get("lease_owner_id")
        or dominant_payload.get("lease_owner_id")
        or ""
    ).strip()
    try:
        expected_lease_epoch = int(
            dominant_liveness_head.get("lease_epoch")
            or raw_liveness_head.get("lease_epoch")
            or dominant_payload.get("lease_epoch")
            or 0
        )
    except (TypeError, ValueError):
        expected_lease_epoch = 0
    try:
        expected_runtime_pid = int(
            dominant_liveness_head.get("pid")
            or raw_liveness_head.get("pid")
            or dominant_payload.get("pid")
            or 0
        )
    except (TypeError, ValueError):
        expected_runtime_pid = 0
    expected_runtime_process_fingerprint = str(
        dominant_liveness_head.get("process_fingerprint")
        or raw_liveness_head.get("process_fingerprint")
        or dominant_payload.get("process_fingerprint")
        or ""
    )
    expected_attachment_state = str(
        effective_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("raw_attachment_state")
        or ""
    )

    runtime_identity_trace = query_runtime_identity_trace_view(runtime_root, node_id=normalized_node_id)
    current_runtime_identity = dict(runtime_identity_trace.get("current_runtime_identity") or {})
    process_identity_anchor = dict(runtime_identity_trace.get("process_identity_anchor") or {})
    supervision_identity_anchor = dict(runtime_identity_trace.get("supervision_identity_anchor") or {})
    projected_visible_effect = {
        "node_id": str(runtime_identity_trace.get("node_id") or ""),
        "node_status": str(current_node.get("status") or ""),
        "launch_event_id": str(runtime_identity_trace.get("launch_event_id") or "").strip(),
        "lease_owner_id": str(runtime_identity_trace.get("lease_owner_id") or "").strip(),
        "lease_epoch": int(runtime_identity_trace.get("lease_epoch") or 0),
        "runtime_pid": int(current_runtime_identity.get("pid") or 0),
        "runtime_process_fingerprint": str(current_runtime_identity.get("process_fingerprint") or ""),
        "runtime_attachment_state": str(current_runtime_identity.get("effective_attachment_state") or ""),
        "process_identity_source_event_type": str(process_identity_anchor.get("source_event_type") or ""),
        "supervision_identity_source_event_type": str(supervision_identity_anchor.get("source_event_type") or ""),
        "closure_kind": str(runtime_identity_trace.get("closure_kind") or ""),
    }
    trusted_expected_effect = {
        "node_id": normalized_node_id,
        "node_status": str(current_node.get("status") or ""),
        "launch_event_id": expected_launch_event_id,
        "lease_owner_id": expected_lease_owner_id,
        "lease_epoch": expected_lease_epoch,
        "runtime_pid": expected_runtime_pid,
        "runtime_process_fingerprint": expected_runtime_process_fingerprint,
        "runtime_attachment_state": expected_attachment_state,
    }

    mismatches: list[str] = []
    if current_node and projected_visible_effect["node_id"] != normalized_node_id:
        mismatches.append("node_id_mismatch")
    if (
        str(trusted_expected_effect.get("node_status") or "")
        and projected_visible_effect["node_status"] != str(trusted_expected_effect.get("node_status") or "")
    ):
        mismatches.append("node_status_mismatch")
    if expected_launch_event_id and projected_visible_effect["launch_event_id"] != expected_launch_event_id:
        mismatches.append("launch_event_id_mismatch")
    if expected_lease_owner_id and projected_visible_effect["lease_owner_id"] != expected_lease_owner_id:
        mismatches.append("lease_owner_id_mismatch")
    if expected_lease_epoch and projected_visible_effect["lease_epoch"] != expected_lease_epoch:
        mismatches.append("lease_epoch_mismatch")
    if expected_runtime_pid and projected_visible_effect["runtime_pid"] != expected_runtime_pid:
        mismatches.append("runtime_pid_mismatch")
    if (
        expected_runtime_process_fingerprint
        and projected_visible_effect["runtime_process_fingerprint"] != expected_runtime_process_fingerprint
    ):
        mismatches.append("runtime_process_fingerprint_mismatch")
    if (
        expected_attachment_state
        and projected_visible_effect["runtime_attachment_state"] != expected_attachment_state
    ):
        mismatches.append("runtime_attachment_state_mismatch")

    trace_gaps = [str(item) for item in list(runtime_identity_trace.get("gaps") or [])]
    if trace_gaps:
        mismatches.append("trace_gaps_present")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "node_id": normalized_node_id,
        "trace_surface": "runtime_identity_trace",
        "trusted_expected_effect": trusted_expected_effect,
        "projected_visible_effect": projected_visible_effect,
        "trace_gaps": trace_gaps,
        "mismatches": mismatches,
        "runtime_identity_trace": runtime_identity_trace,
    }


def query_lineage_parity_debugger_view(state_root: Path, *, node_id: str) -> dict[str, Any]:
    """Compare the trusted current node/runtime view with the visible lineage trace surfaces."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_node_id = str(node_id or "").strip()
    authority_view = query_authority_view(runtime_root, materialize_projection=False)
    liveness_view = query_runtime_liveness_view(runtime_root)
    node_graph = [dict(item or {}) for item in list(authority_view.get("node_graph") or [])]
    nodes_by_id = {
        str(item.get("node_id") or ""): dict(item or {})
        for item in node_graph
        if str(item.get("node_id") or "")
    }
    current_node = dict(nodes_by_id.get(normalized_node_id) or {})
    raw_liveness_head = dict(
        dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).get(normalized_node_id) or {}
    )
    effective_liveness_head = dict(
        dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(normalized_node_id) or {}
    )
    lineage_trace = query_node_lineage_trace_view(runtime_root, node_id=normalized_node_id)
    launch_lineage = dict(lineage_trace.get("launch_lineage") or {})
    lease_lineage = dict(lineage_trace.get("lease_lineage") or {})
    runtime_identity_trace = dict(lineage_trace.get("runtime_identity_trace") or {})
    current_runtime_identity = dict(runtime_identity_trace.get("current_runtime_identity") or {})
    lineage_chain = [dict(item or {}) for item in list(lineage_trace.get("lineage_chain") or [])]

    expected_ancestor_node_ids: list[str] = []
    if current_node:
        seen_node_ids: set[str] = set()
        cursor_node_id = str(current_node.get("parent_node_id") or "").strip()
        lineage_chain_reversed: list[str] = []
        while cursor_node_id:
            if cursor_node_id in seen_node_ids:
                expected_ancestor_node_ids = []
                break
            node_payload = dict(nodes_by_id.get(cursor_node_id) or {})
            if not node_payload:
                expected_ancestor_node_ids = []
                break
            lineage_chain_reversed.append(cursor_node_id)
            seen_node_ids.add(cursor_node_id)
            cursor_node_id = str(node_payload.get("parent_node_id") or "").strip()
        else:
            expected_ancestor_node_ids = list(reversed(lineage_chain_reversed))

    dominant_liveness_head = dict(effective_liveness_head or raw_liveness_head)
    dominant_payload = dict(
        dominant_liveness_head.get("payload")
        or effective_liveness_head.get("payload")
        or raw_liveness_head.get("payload")
        or {}
    )
    expected_launch_event_id = str(
        dominant_payload.get("launch_event_id")
        or dominant_liveness_head.get("launch_event_id")
        or ""
    ).strip()
    expected_lease_owner_id = str(
        dominant_liveness_head.get("lease_owner_id")
        or raw_liveness_head.get("lease_owner_id")
        or dominant_payload.get("lease_owner_id")
        or ""
    ).strip()
    try:
        expected_lease_epoch = int(
            dominant_liveness_head.get("lease_epoch")
            or raw_liveness_head.get("lease_epoch")
            or dominant_payload.get("lease_epoch")
            or 0
        )
    except (TypeError, ValueError):
        expected_lease_epoch = 0
    expected_attachment_state = str(
        effective_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("effective_attachment_state")
        or raw_liveness_head.get("raw_attachment_state")
        or ""
    )
    try:
        expected_runtime_pid = int(
            dominant_liveness_head.get("pid")
            or raw_liveness_head.get("pid")
            or effective_liveness_head.get("pid")
            or dominant_payload.get("pid")
            or 0
        )
    except (TypeError, ValueError):
        expected_runtime_pid = 0
    expected_runtime_process_fingerprint = str(
        dominant_liveness_head.get("process_fingerprint")
        or raw_liveness_head.get("process_fingerprint")
        or effective_liveness_head.get("process_fingerprint")
        or dominant_payload.get("process_fingerprint")
        or ""
    )

    projected_effective_liveness_head = dict(lease_lineage.get("effective_liveness_head") or {})
    projected_raw_liveness_head = dict(lease_lineage.get("raw_liveness_head") or {})
    projected_visible_effect = {
        "node_id": str(dict(lineage_trace.get("current_node") or {}).get("node_id") or ""),
        "node_status": str(dict(lineage_trace.get("current_node") or {}).get("status") or ""),
        "ancestor_node_ids": [str(item.get("node_id") or "") for item in lineage_chain[:-1]],
        "launch_event_id": str(
            lease_lineage.get("launch_event_id")
            or launch_lineage.get("launch_event_id")
            or ""
        ).strip(),
        "lease_owner_id": str(lease_lineage.get("lease_owner_id") or "").strip(),
        "lease_epoch": int(lease_lineage.get("lease_epoch") or 0),
        "runtime_attachment_state": str(
            current_runtime_identity.get("effective_attachment_state")
            or projected_effective_liveness_head.get("effective_attachment_state")
            or projected_raw_liveness_head.get("effective_attachment_state")
            or projected_raw_liveness_head.get("raw_attachment_state")
            or ""
        ),
        "runtime_pid": int(current_runtime_identity.get("pid") or 0),
        "runtime_process_fingerprint": str(current_runtime_identity.get("process_fingerprint") or ""),
        "launch_trace_source_node_id": str(dict(launch_lineage.get("source_node") or {}).get("node_id") or ""),
        "lease_trace_source_node_id": str(dict(lease_lineage.get("source_node") or {}).get("node_id") or ""),
        "runtime_trace_node_id": str(runtime_identity_trace.get("node_id") or ""),
    }
    trusted_expected_effect = {
        "node_id": normalized_node_id,
        "node_status": str(current_node.get("status") or ""),
        "ancestor_node_ids": expected_ancestor_node_ids,
        "launch_event_id": expected_launch_event_id,
        "lease_owner_id": expected_lease_owner_id,
        "lease_epoch": expected_lease_epoch,
        "runtime_attachment_state": expected_attachment_state,
        "runtime_pid": expected_runtime_pid,
        "runtime_process_fingerprint": expected_runtime_process_fingerprint,
    }

    mismatches: list[str] = []
    if current_node and projected_visible_effect["node_id"] != normalized_node_id:
        mismatches.append("node_id_mismatch")
    if (
        str(trusted_expected_effect.get("node_status") or "")
        and projected_visible_effect["node_status"] != str(trusted_expected_effect.get("node_status") or "")
    ):
        mismatches.append("node_status_mismatch")
    if current_node and projected_visible_effect["ancestor_node_ids"] != expected_ancestor_node_ids:
        mismatches.append("ancestor_chain_mismatch")
    if (
        expected_launch_event_id
        and projected_visible_effect["launch_event_id"] != expected_launch_event_id
    ):
        mismatches.append("launch_event_id_mismatch")
    if (
        expected_lease_owner_id
        and projected_visible_effect["lease_owner_id"] != expected_lease_owner_id
    ):
        mismatches.append("lease_owner_id_mismatch")
    if expected_lease_epoch and projected_visible_effect["lease_epoch"] != expected_lease_epoch:
        mismatches.append("lease_epoch_mismatch")
    if (
        expected_attachment_state
        and projected_visible_effect["runtime_attachment_state"] != expected_attachment_state
    ):
        mismatches.append("runtime_attachment_state_mismatch")
    if expected_runtime_pid and projected_visible_effect["runtime_pid"] != expected_runtime_pid:
        mismatches.append("runtime_pid_mismatch")
    if (
        expected_runtime_process_fingerprint
        and projected_visible_effect["runtime_process_fingerprint"] != expected_runtime_process_fingerprint
    ):
        mismatches.append("runtime_process_fingerprint_mismatch")
    if (
        current_node
        and projected_visible_effect["launch_trace_source_node_id"]
        and projected_visible_effect["launch_trace_source_node_id"] != normalized_node_id
    ):
        mismatches.append("launch_trace_source_node_mismatch")
    if (
        current_node
        and projected_visible_effect["lease_trace_source_node_id"]
        and projected_visible_effect["lease_trace_source_node_id"] != normalized_node_id
    ):
        mismatches.append("lease_trace_source_node_mismatch")
    if (
        current_node
        and projected_visible_effect["runtime_trace_node_id"]
        and projected_visible_effect["runtime_trace_node_id"] != normalized_node_id
    ):
        mismatches.append("runtime_trace_source_node_mismatch")

    trace_gaps = [str(item) for item in list(lineage_trace.get("gaps") or [])]
    if trace_gaps:
        mismatches.append("trace_gaps_present")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "node_id": normalized_node_id,
        "trace_surface": "node_lineage_trace",
        "trusted_expected_effect": trusted_expected_effect,
        "projected_visible_effect": projected_visible_effect,
        "trace_gaps": trace_gaps,
        "mismatches": mismatches,
        "lineage_trace": lineage_trace,
    }


def query_command_parity_debugger_view(state_root: Path, *, envelope_id: str) -> dict[str, Any]:
    """Compare trusted-path expected command effects with the currently visible projected truth."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    classification = str(accepted_audit.get("classification") or "").strip().lower()
    trace_surface = ""
    trace_view: dict[str, Any] = {}
    trusted_expected_effect: dict[str, Any] = {}
    projected_visible_effect: dict[str, Any] = {}
    mismatches: list[str] = []

    if classification == "topology":
        trace_surface = "topology_command_trace"
        trace_view = query_topology_command_trace_view(runtime_root, envelope_id=normalized_envelope_id)
        accepted_summary = dict(trace_view.get("accepted_envelope") or {})
        projection_effect = dict(trace_view.get("projection_effect") or {})
        source_node = dict(projection_effect.get("source_node") or {})
        target_nodes = [dict(item or {}) for item in list(projection_effect.get("target_nodes") or [])]
        command_kind = str(trace_view.get("command_kind") or accepted_summary.get("command_kind") or "")
        trusted_expected_effect = {
            "command_kind": command_kind,
            "source_node_id": str(accepted_summary.get("source_node_id") or ""),
            "target_node_ids": [str(item) for item in list(accepted_summary.get("target_node_ids") or [])],
        }
        if command_kind == "split":
            trusted_expected_effect["expected_target_statuses"] = {
                str(node_id): "PLANNED"
                for node_id in list(trusted_expected_effect.get("target_node_ids") or [])
                if str(node_id)
            }
        if command_kind in {"resume", "retry", "activate"}:
            trusted_expected_effect["expected_source_status"] = "ACTIVE"
        if command_kind in {"resume", "retry"}:
            trusted_expected_effect["expected_blocked_reason"] = ""
            trusted_expected_effect["expected_runtime_attachment_state"] = "UNOBSERVED"
        if command_kind == "relaunch":
            trusted_expected_effect["expected_target_statuses"] = {
                str(node_id): "ACTIVE"
                for node_id in list(trusted_expected_effect.get("target_node_ids") or [])
                if str(node_id)
            }

        projected_visible_effect = {
            "source_node_id": str(source_node.get("node_id") or ""),
            "source_status": str(source_node.get("status") or ""),
            "source_blocked_reason": str(source_node.get("blocked_reason") or ""),
            "source_runtime_attachment_state": str(source_node.get("runtime_attachment_state") or ""),
            "target_node_ids": [str(item.get("node_id") or "") for item in target_nodes],
            "target_statuses": {
                str(item.get("node_id") or ""): str(item.get("status") or "")
                for item in target_nodes
                if str(item.get("node_id") or "")
            },
            "self_attribution": str(dict(trace_view.get("canonical_event") or {}).get("self_attribution") or ""),
            "self_repair": str(dict(trace_view.get("canonical_event") or {}).get("self_repair") or ""),
        }
        if trusted_expected_effect["source_node_id"] and projected_visible_effect["source_node_id"] != trusted_expected_effect["source_node_id"]:
            mismatches.append("source_node_id_mismatch")
        expected_target_ids = trusted_expected_effect.get("target_node_ids") or []
        if expected_target_ids and projected_visible_effect.get("target_node_ids") != expected_target_ids:
            mismatches.append("target_node_ids_mismatch")
        expected_target_statuses = {
            str(node_id): str(status)
            for node_id, status in dict(trusted_expected_effect.get("expected_target_statuses") or {}).items()
            if str(node_id)
        }
        if expected_target_statuses:
            visible_target_statuses = {
                str(node_id): str(status)
                for node_id, status in dict(projected_visible_effect.get("target_statuses") or {}).items()
                if str(node_id)
            }
            if any(visible_target_statuses.get(node_id) != status for node_id, status in expected_target_statuses.items()):
                mismatches.append("target_statuses_mismatch")
        expected_source_status = str(trusted_expected_effect.get("expected_source_status") or "")
        if expected_source_status and str(projected_visible_effect.get("source_status") or "") != expected_source_status:
            mismatches.append("source_status_mismatch")
        if (
            "expected_blocked_reason" in trusted_expected_effect
            and str(projected_visible_effect.get("source_blocked_reason") or "") != str(trusted_expected_effect.get("expected_blocked_reason") or "")
        ):
            mismatches.append("blocked_reason_mismatch")
        expected_runtime_attachment_state = str(
            trusted_expected_effect.get("expected_runtime_attachment_state") or ""
        )
        if (
            expected_runtime_attachment_state
            and str(projected_visible_effect.get("source_runtime_attachment_state") or "") != expected_runtime_attachment_state
        ):
            mismatches.append("runtime_attachment_state_mismatch")

    elif classification == "terminal":
        trace_surface = "terminal_result_trace"
        trace_view = query_terminal_result_trace_view(runtime_root, envelope_id=normalized_envelope_id)
        accepted_summary = dict(trace_view.get("accepted_envelope") or {})
        source_node = dict(trace_view.get("source_node") or {})
        trusted_expected_effect = {
            "node_id": str(accepted_summary.get("node_id") or ""),
            "node_status": str(accepted_summary.get("node_status") or ""),
            "outcome": str(accepted_summary.get("outcome") or ""),
            "expected_runtime_attachment_state": "TERMINAL",
        }
        projected_visible_effect = {
            "node_id": str(source_node.get("node_id") or ""),
            "node_status": str(source_node.get("status") or ""),
            "runtime_attachment_state": str(source_node.get("runtime_attachment_state") or ""),
            "runtime_observation_kind": str(source_node.get("runtime_observation_kind") or ""),
        }
        if projected_visible_effect["node_id"] != trusted_expected_effect["node_id"]:
            mismatches.append("node_id_mismatch")
        if projected_visible_effect["node_status"] != trusted_expected_effect["node_status"]:
            mismatches.append("node_status_mismatch")
        if (
            str(projected_visible_effect.get("runtime_attachment_state") or "")
            != str(trusted_expected_effect.get("expected_runtime_attachment_state") or "")
        ):
            mismatches.append("runtime_attachment_state_mismatch")

    elif classification == "evaluator":
        trace_surface = "evaluator_result_trace"
        trace_view = query_evaluator_result_trace_view(runtime_root, envelope_id=normalized_envelope_id)
        accepted_summary = dict(trace_view.get("accepted_envelope") or {})
        source_node = dict(trace_view.get("source_node") or {})
        active_lanes = [dict(item or {}) for item in list(trace_view.get("active_evaluator_lanes") or [])]
        matched_lane = next(
            (
                lane
                for lane in active_lanes
                if str(lane.get("lane") or "") == str(accepted_summary.get("lane") or "")
            ),
            dict(active_lanes[0] or {}) if active_lanes else {},
        )
        trusted_expected_effect = {
            "node_id": str(accepted_summary.get("node_id") or ""),
            "lane": str(accepted_summary.get("lane") or ""),
            "verdict": str(accepted_summary.get("verdict") or ""),
            "retryable": bool(accepted_summary.get("retryable")),
        }
        projected_visible_effect = {
            "node_id": str(source_node.get("node_id") or ""),
            "lane": str(matched_lane.get("lane") or ""),
            "verdict": str(matched_lane.get("verdict") or ""),
            "active_lane_count": len(active_lanes),
        }
        if projected_visible_effect["node_id"] != trusted_expected_effect["node_id"]:
            mismatches.append("node_id_mismatch")
        if projected_visible_effect["lane"] != trusted_expected_effect["lane"]:
            mismatches.append("lane_mismatch")
        if projected_visible_effect["verdict"] != trusted_expected_effect["verdict"]:
            mismatches.append("verdict_mismatch")

    elif classification == "repo_tree_cleanup":
        trace_surface = "repo_tree_cleanup_trace"
        trace_view = query_repo_tree_cleanup_trace_view(runtime_root, envelope_id=normalized_envelope_id)
        accepted_summary = dict(trace_view.get("accepted_envelope") or {})
        cleanup_effect = dict(trace_view.get("cleanup_effect") or {})
        requested_repo_roots = sorted(str(item) for item in list(accepted_summary.get("repo_roots") or []))
        settled_repo_roots = sorted(str(item) for item in list(cleanup_effect.get("repo_roots") or []))
        trusted_expected_effect = {
            "tree_root": str(accepted_summary.get("tree_root") or ""),
            "repo_roots": requested_repo_roots,
            "requested_repo_count": len(requested_repo_roots),
            "include_anchor_repo_root": bool(accepted_summary.get("include_anchor_repo_root")),
        }
        projected_visible_effect = {
            "tree_root": str(cleanup_effect.get("tree_root") or ""),
            "repo_roots": settled_repo_roots,
            "repo_count": int(cleanup_effect.get("repo_count") or 0),
            "quiesced_repo_count": int(cleanup_effect.get("quiesced_repo_count") or 0),
            "failed_repo_count": int(cleanup_effect.get("failed_repo_count") or 0),
            "quiesced": bool(cleanup_effect.get("quiesced")),
        }
        if projected_visible_effect["tree_root"] != trusted_expected_effect["tree_root"]:
            mismatches.append("tree_root_mismatch")
        if projected_visible_effect["repo_roots"] != requested_repo_roots:
            mismatches.append("repo_roots_mismatch")
        if projected_visible_effect["repo_count"] != trusted_expected_effect["requested_repo_count"]:
            mismatches.append("repo_count_mismatch")
        if projected_visible_effect["quiesced_repo_count"] != projected_visible_effect["repo_count"]:
            mismatches.append("quiesced_repo_count_mismatch")
        if projected_visible_effect["failed_repo_count"] != 0:
            mismatches.append("failed_repo_count_mismatch")
        if not projected_visible_effect["quiesced"]:
            mismatches.append("cleanup_not_quiesced")

    elif classification == "heavy_object":
        envelope_type = str(accepted_audit.get("envelope_type") or "").strip().lower()
        if envelope_type == "heavy_object_registration_request":
            trace_surface = "heavy_object_registration_trace"
            trace_view = query_heavy_object_registration_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            registered_object = dict(trace_view.get("registered_object") or {})
            effective_object_id = str(registered_object.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(registered_object.get("object_ref") or accepted_summary.get("object_ref") or "")
            retention_view = query_heavy_object_retention_view(
                runtime_root,
                object_id=effective_object_id,
                object_ref=effective_object_ref,
            )
            retention_registration = dict(retention_view.get("registration") or {})
            trusted_expected_effect = {
                "command_kind": "heavy_object_registration",
                "object_id": effective_object_id,
                "object_kind": str(registered_object.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "byte_size": int(registered_object.get("byte_size") or accepted_summary.get("byte_size") or 0),
                "runtime_name": str(registered_object.get("runtime_name") or ""),
                "repo_root": str(registered_object.get("repo_root") or ""),
            }
            projected_visible_effect = {
                "registered_present": bool(retention_view.get("registered_present")),
                "retention_state": str(retention_view.get("retention_state") or ""),
                "object_id": str(retention_view.get("object_id") or ""),
                "object_kind": str(retention_registration.get("object_kind") or ""),
                "object_ref": str(retention_view.get("object_ref") or ""),
                "byte_size": int(retention_registration.get("byte_size") or 0),
                "runtime_name": str(retention_registration.get("runtime_name") or ""),
                "repo_root": str(retention_registration.get("repo_root") or ""),
            }
            if not projected_visible_effect["registered_present"]:
                mismatches.append("registered_present_mismatch")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["byte_size"] != trusted_expected_effect["byte_size"]:
                mismatches.append("byte_size_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
        elif envelope_type == "heavy_object_reference_request":
            trace_surface = "heavy_object_reference_trace"
            trace_view = query_heavy_object_reference_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            referenced_object = dict(trace_view.get("referenced_object") or {})
            effective_object_id = str(referenced_object.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(referenced_object.get("object_ref") or accepted_summary.get("object_ref") or "")
            reference_inventory = query_heavy_object_reference_inventory_view(
                runtime_root,
                object_id=effective_object_id,
                object_ref=effective_object_ref,
            )
            matching_reference = next(
                (
                    dict(item)
                    for item in list(reference_inventory.get("active_references") or [])
                    if (
                        str(item.get("reference_ref") or "")
                        == str(referenced_object.get("reference_ref") or accepted_summary.get("reference_ref") or "")
                        and str(item.get("reference_holder_kind") or "")
                        == str(
                            referenced_object.get("reference_holder_kind")
                            or accepted_summary.get("reference_holder_kind")
                            or ""
                        )
                        and str(item.get("reference_holder_ref") or "")
                        == str(
                            referenced_object.get("reference_holder_ref")
                            or accepted_summary.get("reference_holder_ref")
                            or ""
                        )
                    )
                ),
                {},
            )
            trusted_expected_effect = {
                "command_kind": "heavy_object_reference",
                "object_id": effective_object_id,
                "object_kind": str(referenced_object.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "reference_ref": str(referenced_object.get("reference_ref") or accepted_summary.get("reference_ref") or ""),
                "reference_holder_kind": str(
                    referenced_object.get("reference_holder_kind")
                    or accepted_summary.get("reference_holder_kind")
                    or ""
                ),
                "reference_holder_ref": str(
                    referenced_object.get("reference_holder_ref")
                    or accepted_summary.get("reference_holder_ref")
                    or ""
                ),
                "reference_kind": str(referenced_object.get("reference_kind") or accepted_summary.get("reference_kind") or ""),
                "runtime_name": str(referenced_object.get("runtime_name") or ""),
                "repo_root": str(referenced_object.get("repo_root") or ""),
            }
            projected_visible_effect = {
                "matching_reference_present": bool(matching_reference),
                "active_reference_count": int(reference_inventory.get("active_reference_count") or 0),
                "object_id": str(reference_inventory.get("object_id") or ""),
                "object_ref": str(reference_inventory.get("object_ref") or ""),
                "reference_ref": str(matching_reference.get("reference_ref") or ""),
                "reference_holder_kind": str(matching_reference.get("reference_holder_kind") or ""),
                "reference_holder_ref": str(matching_reference.get("reference_holder_ref") or ""),
                "reference_kind": str(matching_reference.get("reference_kind") or ""),
                "runtime_name": str(matching_reference.get("runtime_name") or ""),
                "repo_root": str(matching_reference.get("repo_root") or ""),
            }
            if not projected_visible_effect["matching_reference_present"]:
                mismatches.append("matching_reference_missing")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["reference_ref"] != trusted_expected_effect["reference_ref"]:
                mismatches.append("reference_ref_mismatch")
            if projected_visible_effect["reference_holder_kind"] != trusted_expected_effect["reference_holder_kind"]:
                mismatches.append("reference_holder_kind_mismatch")
            if projected_visible_effect["reference_holder_ref"] != trusted_expected_effect["reference_holder_ref"]:
                mismatches.append("reference_holder_ref_mismatch")
            if projected_visible_effect["reference_kind"] != trusted_expected_effect["reference_kind"]:
                mismatches.append("reference_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
        elif envelope_type == "heavy_object_reference_release_request":
            trace_surface = "heavy_object_reference_release_trace"
            trace_view = query_heavy_object_reference_release_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            released_reference = dict(trace_view.get("released_reference") or {})
            effective_object_id = str(released_reference.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(released_reference.get("object_ref") or accepted_summary.get("object_ref") or "")
            reference_inventory = query_heavy_object_reference_inventory_view(
                runtime_root,
                object_id=effective_object_id,
                object_ref=effective_object_ref,
            )
            matching_released_reference = next(
                (
                    dict(item)
                    for item in list(reference_inventory.get("released_references") or [])
                    if (
                        str(item.get("reference_ref") or "")
                        == str(released_reference.get("reference_ref") or accepted_summary.get("reference_ref") or "")
                        and str(item.get("reference_holder_kind") or "")
                        == str(
                            released_reference.get("reference_holder_kind")
                            or accepted_summary.get("reference_holder_kind")
                            or ""
                        )
                        and str(item.get("reference_holder_ref") or "")
                        == str(
                            released_reference.get("reference_holder_ref")
                            or accepted_summary.get("reference_holder_ref")
                            or ""
                        )
                    )
                ),
                {},
            )
            matching_active_reference = next(
                (
                    dict(item)
                    for item in list(reference_inventory.get("active_references") or [])
                    if (
                        str(item.get("reference_ref") or "")
                        == str(released_reference.get("reference_ref") or accepted_summary.get("reference_ref") or "")
                        and str(item.get("reference_holder_kind") or "")
                        == str(
                            released_reference.get("reference_holder_kind")
                            or accepted_summary.get("reference_holder_kind")
                            or ""
                        )
                        and str(item.get("reference_holder_ref") or "")
                        == str(
                            released_reference.get("reference_holder_ref")
                            or accepted_summary.get("reference_holder_ref")
                            or ""
                        )
                    )
                ),
                {},
            )
            trusted_expected_effect = {
                "command_kind": "heavy_object_reference_release",
                "object_id": effective_object_id,
                "object_kind": str(released_reference.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "reference_ref": str(released_reference.get("reference_ref") or accepted_summary.get("reference_ref") or ""),
                "reference_holder_kind": str(
                    released_reference.get("reference_holder_kind")
                    or accepted_summary.get("reference_holder_kind")
                    or ""
                ),
                "reference_holder_ref": str(
                    released_reference.get("reference_holder_ref")
                    or accepted_summary.get("reference_holder_ref")
                    or ""
                ),
                "reference_kind": str(released_reference.get("reference_kind") or accepted_summary.get("reference_kind") or ""),
                "runtime_name": str(released_reference.get("runtime_name") or ""),
                "repo_root": str(released_reference.get("repo_root") or ""),
            }
            projected_visible_effect = {
                "matching_released_reference_present": bool(matching_released_reference),
                "matching_active_reference_present": bool(matching_active_reference),
                "active_reference_count": int(reference_inventory.get("active_reference_count") or 0),
                "released_reference_count": int(reference_inventory.get("released_reference_count") or 0),
                "object_id": str(reference_inventory.get("object_id") or ""),
                "object_kind": str(reference_inventory.get("object_kind") or ""),
                "object_ref": str(reference_inventory.get("object_ref") or ""),
                "reference_ref": str(matching_released_reference.get("reference_ref") or ""),
                "reference_holder_kind": str(matching_released_reference.get("reference_holder_kind") or ""),
                "reference_holder_ref": str(matching_released_reference.get("reference_holder_ref") or ""),
                "reference_kind": str(matching_released_reference.get("reference_kind") or ""),
                "runtime_name": str(matching_released_reference.get("runtime_name") or ""),
                "repo_root": str(matching_released_reference.get("repo_root") or ""),
            }
            if not projected_visible_effect["matching_released_reference_present"]:
                mismatches.append("matching_released_reference_missing")
            if projected_visible_effect["matching_active_reference_present"]:
                mismatches.append("released_reference_still_active")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["reference_ref"] != trusted_expected_effect["reference_ref"]:
                mismatches.append("reference_ref_mismatch")
            if projected_visible_effect["reference_holder_kind"] != trusted_expected_effect["reference_holder_kind"]:
                mismatches.append("reference_holder_kind_mismatch")
            if projected_visible_effect["reference_holder_ref"] != trusted_expected_effect["reference_holder_ref"]:
                mismatches.append("reference_holder_ref_mismatch")
            if projected_visible_effect["reference_kind"] != trusted_expected_effect["reference_kind"]:
                mismatches.append("reference_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
        elif envelope_type == "heavy_object_pin_request":
            trace_surface = "heavy_object_pin_trace"
            trace_view = query_heavy_object_pin_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            pinned_object = dict(trace_view.get("pinned_object") or {})
            effective_object_id = str(pinned_object.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(pinned_object.get("object_ref") or accepted_summary.get("object_ref") or "")
            retention_view = query_heavy_object_retention_view(
                runtime_root,
                object_id=effective_object_id,
                object_ref=effective_object_ref,
            )
            matching_pin = next(
                (
                    dict(item)
                    for item in list(retention_view.get("active_pins") or [])
                    if (
                        str(item.get("pin_holder_id") or "") == str(pinned_object.get("pin_holder_id") or accepted_summary.get("pin_holder_id") or "")
                        and str(item.get("pin_kind") or "") == str(pinned_object.get("pin_kind") or accepted_summary.get("pin_kind") or "")
                    )
                ),
                {},
            )
            trusted_expected_effect = {
                "command_kind": "heavy_object_pin",
                "object_id": effective_object_id,
                "object_kind": str(pinned_object.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "pin_holder_id": str(pinned_object.get("pin_holder_id") or accepted_summary.get("pin_holder_id") or ""),
                "pin_kind": str(pinned_object.get("pin_kind") or accepted_summary.get("pin_kind") or ""),
                "runtime_name": str(pinned_object.get("runtime_name") or ""),
                "repo_root": str(pinned_object.get("repo_root") or ""),
            }
            projected_visible_effect = {
                "matching_pin_present": bool(matching_pin),
                "active_pin_count": int(retention_view.get("active_pin_count") or 0),
                "retention_state": str(retention_view.get("retention_state") or ""),
                "object_id": str(retention_view.get("object_id") or ""),
                "object_kind": str(retention_view.get("object_kind") or ""),
                "object_ref": str(retention_view.get("object_ref") or ""),
                "pin_holder_id": str(matching_pin.get("pin_holder_id") or ""),
                "pin_kind": str(matching_pin.get("pin_kind") or ""),
                "runtime_name": str(matching_pin.get("runtime_name") or ""),
                "repo_root": str(matching_pin.get("repo_root") or ""),
            }
            if not projected_visible_effect["matching_pin_present"]:
                mismatches.append("matching_pin_missing")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["pin_holder_id"] != trusted_expected_effect["pin_holder_id"]:
                mismatches.append("pin_holder_id_mismatch")
            if projected_visible_effect["pin_kind"] != trusted_expected_effect["pin_kind"]:
                mismatches.append("pin_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
        elif envelope_type == "heavy_object_pin_release_request":
            trace_surface = "heavy_object_pin_release_trace"
            trace_view = query_heavy_object_pin_release_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            released_pin = dict(trace_view.get("released_pin") or {})
            effective_object_id = str(released_pin.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(released_pin.get("object_ref") or accepted_summary.get("object_ref") or "")
            retention_view = query_heavy_object_retention_view(
                runtime_root,
                object_id=effective_object_id,
                object_ref=effective_object_ref,
            )
            matching_released_pin = next(
                (
                    dict(item)
                    for item in list(retention_view.get("released_pins") or [])
                    if (
                        str(item.get("pin_holder_id") or "") == str(released_pin.get("pin_holder_id") or accepted_summary.get("pin_holder_id") or "")
                        and str(item.get("pin_kind") or "") == str(released_pin.get("pin_kind") or accepted_summary.get("pin_kind") or "")
                    )
                ),
                {},
            )
            matching_active_pin = next(
                (
                    dict(item)
                    for item in list(retention_view.get("active_pins") or [])
                    if (
                        str(item.get("pin_holder_id") or "") == str(released_pin.get("pin_holder_id") or accepted_summary.get("pin_holder_id") or "")
                        and str(item.get("pin_kind") or "") == str(released_pin.get("pin_kind") or accepted_summary.get("pin_kind") or "")
                    )
                ),
                {},
            )
            trusted_expected_effect = {
                "command_kind": "heavy_object_pin_release",
                "object_id": effective_object_id,
                "object_kind": str(released_pin.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "pin_holder_id": str(released_pin.get("pin_holder_id") or accepted_summary.get("pin_holder_id") or ""),
                "pin_kind": str(released_pin.get("pin_kind") or accepted_summary.get("pin_kind") or ""),
                "runtime_name": str(released_pin.get("runtime_name") or ""),
                "repo_root": str(released_pin.get("repo_root") or ""),
            }
            projected_visible_effect = {
                "matching_released_pin_present": bool(matching_released_pin),
                "matching_active_pin_present": bool(matching_active_pin),
                "active_pin_count": int(retention_view.get("active_pin_count") or 0),
                "released_pin_count": int(retention_view.get("released_pin_count") or 0),
                "retention_state": str(retention_view.get("retention_state") or ""),
                "object_id": str(retention_view.get("object_id") or ""),
                "object_kind": str(retention_view.get("object_kind") or ""),
                "object_ref": str(retention_view.get("object_ref") or ""),
                "pin_holder_id": str(matching_released_pin.get("pin_holder_id") or ""),
                "pin_kind": str(matching_released_pin.get("pin_kind") or ""),
                "runtime_name": str(matching_released_pin.get("runtime_name") or ""),
                "repo_root": str(matching_released_pin.get("repo_root") or ""),
            }
            if not projected_visible_effect["matching_released_pin_present"]:
                mismatches.append("matching_released_pin_missing")
            if projected_visible_effect["matching_active_pin_present"]:
                mismatches.append("released_pin_still_active")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["pin_holder_id"] != trusted_expected_effect["pin_holder_id"]:
                mismatches.append("pin_holder_id_mismatch")
            if projected_visible_effect["pin_kind"] != trusted_expected_effect["pin_kind"]:
                mismatches.append("pin_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
        elif envelope_type == "heavy_object_supersession_request":
            trace_surface = "heavy_object_supersession_trace"
            trace_view = query_heavy_object_supersession_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            supersession = dict(trace_view.get("supersession") or {})
            retention_view = query_heavy_object_retention_view(
                runtime_root,
                object_id=str(supersession.get("superseded_object_id") or accepted_summary.get("superseded_object_id") or ""),
                object_ref=str(supersession.get("superseded_object_ref") or accepted_summary.get("superseded_object_ref") or ""),
            )
            latest_superseded_by = dict(retention_view.get("latest_superseded_by") or {})
            trusted_expected_effect = {
                "command_kind": "heavy_object_supersession",
                "superseded_object_id": str(supersession.get("superseded_object_id") or accepted_summary.get("superseded_object_id") or ""),
                "superseded_object_kind": str(supersession.get("superseded_object_kind") or accepted_summary.get("superseded_object_kind") or ""),
                "superseded_object_ref": str(supersession.get("superseded_object_ref") or accepted_summary.get("superseded_object_ref") or ""),
                "replacement_object_id": str(supersession.get("replacement_object_id") or accepted_summary.get("replacement_object_id") or ""),
                "replacement_object_kind": str(supersession.get("replacement_object_kind") or accepted_summary.get("replacement_object_kind") or ""),
                "replacement_object_ref": str(supersession.get("replacement_object_ref") or accepted_summary.get("replacement_object_ref") or ""),
                "supersession_kind": str(supersession.get("supersession_kind") or accepted_summary.get("supersession_kind") or ""),
                "runtime_name": str(supersession.get("runtime_name") or ""),
                "repo_root": str(supersession.get("repo_root") or ""),
                "trigger_kind": str(supersession.get("trigger_kind") or accepted_summary.get("trigger_kind") or ""),
                "trigger_ref": str(supersession.get("trigger_ref") or accepted_summary.get("trigger_ref") or ""),
            }
            projected_visible_effect = {
                "superseded_present": bool(retention_view.get("superseded_present")),
                "retention_state": str(retention_view.get("retention_state") or ""),
                "superseded_object_id": str(retention_view.get("object_id") or ""),
                "superseded_object_kind": str(retention_view.get("object_kind") or ""),
                "superseded_object_ref": str(retention_view.get("object_ref") or ""),
                "replacement_object_id": str(latest_superseded_by.get("replacement_object_id") or ""),
                "replacement_object_kind": str(latest_superseded_by.get("replacement_object_kind") or ""),
                "replacement_object_ref": str(latest_superseded_by.get("replacement_object_ref") or ""),
                "supersession_kind": str(latest_superseded_by.get("supersession_kind") or ""),
                "runtime_name": str(latest_superseded_by.get("runtime_name") or ""),
                "repo_root": str(latest_superseded_by.get("repo_root") or ""),
                "trigger_kind": str(latest_superseded_by.get("trigger_kind") or ""),
                "trigger_ref": str(latest_superseded_by.get("trigger_ref") or ""),
            }
            if not projected_visible_effect["superseded_present"]:
                mismatches.append("superseded_present_mismatch")
            if projected_visible_effect["superseded_object_id"] != trusted_expected_effect["superseded_object_id"]:
                mismatches.append("superseded_object_id_mismatch")
            if projected_visible_effect["superseded_object_kind"] != trusted_expected_effect["superseded_object_kind"]:
                mismatches.append("superseded_object_kind_mismatch")
            if projected_visible_effect["superseded_object_ref"] != trusted_expected_effect["superseded_object_ref"]:
                mismatches.append("superseded_object_ref_mismatch")
            if projected_visible_effect["replacement_object_id"] != trusted_expected_effect["replacement_object_id"]:
                mismatches.append("replacement_object_id_mismatch")
            if projected_visible_effect["replacement_object_kind"] != trusted_expected_effect["replacement_object_kind"]:
                mismatches.append("replacement_object_kind_mismatch")
            if projected_visible_effect["replacement_object_ref"] != trusted_expected_effect["replacement_object_ref"]:
                mismatches.append("replacement_object_ref_mismatch")
            if projected_visible_effect["supersession_kind"] != trusted_expected_effect["supersession_kind"]:
                mismatches.append("supersession_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["trigger_kind"] != trusted_expected_effect["trigger_kind"]:
                mismatches.append("trigger_kind_mismatch")
            if projected_visible_effect["trigger_ref"] != trusted_expected_effect["trigger_ref"]:
                mismatches.append("trigger_ref_mismatch")
        elif envelope_type == "heavy_object_gc_eligibility_request":
            trace_surface = "heavy_object_gc_eligibility_trace"
            trace_view = query_heavy_object_gc_eligibility_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            eligible_object = dict(trace_view.get("eligible_object") or {})
            retention_view = query_heavy_object_retention_view(
                runtime_root,
                object_id=str(eligible_object.get("object_id") or accepted_summary.get("object_id") or ""),
                object_ref=str(eligible_object.get("object_ref") or accepted_summary.get("object_ref") or ""),
            )
            latest_gc_eligibility = dict(retention_view.get("latest_gc_eligibility") or {})
            trusted_expected_effect = {
                "command_kind": "heavy_object_gc_eligibility",
                "object_id": str(eligible_object.get("object_id") or accepted_summary.get("object_id") or ""),
                "object_kind": str(eligible_object.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": str(eligible_object.get("object_ref") or accepted_summary.get("object_ref") or ""),
                "eligibility_kind": str(eligible_object.get("eligibility_kind") or accepted_summary.get("eligibility_kind") or ""),
                "superseded_by_object_id": str(eligible_object.get("superseded_by_object_id") or accepted_summary.get("superseded_by_object_id") or ""),
                "superseded_by_object_ref": str(eligible_object.get("superseded_by_object_ref") or accepted_summary.get("superseded_by_object_ref") or ""),
                "runtime_name": str(eligible_object.get("runtime_name") or ""),
                "repo_root": str(eligible_object.get("repo_root") or ""),
                "trigger_kind": str(eligible_object.get("trigger_kind") or accepted_summary.get("trigger_kind") or ""),
                "trigger_ref": str(eligible_object.get("trigger_ref") or accepted_summary.get("trigger_ref") or ""),
            }
            projected_visible_effect = {
                "gc_eligibility_present": bool(retention_view.get("gc_eligibility_present")),
                "retention_state": str(retention_view.get("retention_state") or ""),
                "object_id": str(retention_view.get("object_id") or ""),
                "object_kind": str(retention_view.get("object_kind") or ""),
                "object_ref": str(retention_view.get("object_ref") or ""),
                "eligibility_kind": str(latest_gc_eligibility.get("eligibility_kind") or ""),
                "superseded_by_object_id": str(latest_gc_eligibility.get("superseded_by_object_id") or ""),
                "superseded_by_object_ref": str(latest_gc_eligibility.get("superseded_by_object_ref") or ""),
                "runtime_name": str(latest_gc_eligibility.get("runtime_name") or ""),
                "repo_root": str(latest_gc_eligibility.get("repo_root") or ""),
                "trigger_kind": str(latest_gc_eligibility.get("trigger_kind") or ""),
                "trigger_ref": str(latest_gc_eligibility.get("trigger_ref") or ""),
            }
            if not projected_visible_effect["gc_eligibility_present"]:
                mismatches.append("gc_eligibility_present_mismatch")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["eligibility_kind"] != trusted_expected_effect["eligibility_kind"]:
                mismatches.append("eligibility_kind_mismatch")
            if projected_visible_effect["superseded_by_object_id"] != trusted_expected_effect["superseded_by_object_id"]:
                mismatches.append("superseded_by_object_id_mismatch")
            if projected_visible_effect["superseded_by_object_ref"] != trusted_expected_effect["superseded_by_object_ref"]:
                mismatches.append("superseded_by_object_ref_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["trigger_kind"] != trusted_expected_effect["trigger_kind"]:
                mismatches.append("trigger_kind_mismatch")
            if projected_visible_effect["trigger_ref"] != trusted_expected_effect["trigger_ref"]:
                mismatches.append("trigger_ref_mismatch")
        elif envelope_type == "heavy_object_reclamation_request":
            trace_surface = "heavy_object_reclamation_trace"
            trace_view = query_heavy_object_reclamation_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            reclamation_effect = dict(trace_view.get("reclamation_effect") or {})
            retention_view = query_heavy_object_retention_view(
                runtime_root,
                object_id=str(reclamation_effect.get("object_id") or accepted_summary.get("object_id") or ""),
                object_ref=str(reclamation_effect.get("object_ref") or accepted_summary.get("object_ref") or ""),
            )
            latest_reclaimed = dict(retention_view.get("latest_reclaimed") or {})
            trusted_expected_effect = {
                "command_kind": "heavy_object_reclamation",
                "object_id": str(reclamation_effect.get("object_id") or accepted_summary.get("object_id") or ""),
                "object_kind": str(reclamation_effect.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": str(reclamation_effect.get("object_ref") or accepted_summary.get("object_ref") or ""),
                "reclamation_kind": str(reclamation_effect.get("reclamation_kind") or accepted_summary.get("reclamation_kind") or ""),
                "runtime_name": str(reclamation_effect.get("runtime_name") or ""),
                "repo_root": str(reclamation_effect.get("repo_root") or ""),
                "trigger_kind": str(reclamation_effect.get("trigger_kind") or accepted_summary.get("trigger_kind") or ""),
                "trigger_ref": str(reclamation_effect.get("trigger_ref") or accepted_summary.get("trigger_ref") or ""),
                "reclaimed": bool(reclamation_effect.get("reclaimed")),
                "object_exists_after": bool(reclamation_effect.get("object_exists_after")),
            }
            projected_visible_effect = {
                "reclaimed_present": bool(retention_view.get("reclaimed_present")),
                "retention_state": str(retention_view.get("retention_state") or ""),
                "object_id": str(retention_view.get("object_id") or ""),
                "object_kind": str(retention_view.get("object_kind") or ""),
                "object_ref": str(retention_view.get("object_ref") or ""),
                "reclamation_kind": str(latest_reclaimed.get("reclamation_kind") or ""),
                "runtime_name": str(latest_reclaimed.get("runtime_name") or ""),
                "repo_root": str(latest_reclaimed.get("repo_root") or ""),
                "trigger_kind": str(latest_reclaimed.get("trigger_kind") or ""),
                "trigger_ref": str(latest_reclaimed.get("trigger_ref") or ""),
                "reclaimed": bool(latest_reclaimed.get("reclaimed")),
                "object_exists_after": bool(latest_reclaimed.get("object_exists_after")),
            }
            if not projected_visible_effect["reclaimed_present"]:
                mismatches.append("reclaimed_present_mismatch")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["reclamation_kind"] != trusted_expected_effect["reclamation_kind"]:
                mismatches.append("reclamation_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["trigger_kind"] != trusted_expected_effect["trigger_kind"]:
                mismatches.append("trigger_kind_mismatch")
            if projected_visible_effect["trigger_ref"] != trusted_expected_effect["trigger_ref"]:
                mismatches.append("trigger_ref_mismatch")
            if projected_visible_effect["reclaimed"] != trusted_expected_effect["reclaimed"]:
                mismatches.append("reclaimed_flag_mismatch")
            if projected_visible_effect["object_exists_after"] != trusted_expected_effect["object_exists_after"]:
                mismatches.append("object_exists_after_mismatch")
        elif envelope_type == "heavy_object_discovery_request":
            trace_surface = "heavy_object_discovery_trace"
            trace_view = query_heavy_object_discovery_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            discovery_effect = dict(trace_view.get("discovery_effect") or {})
            effective_object_id = str(discovery_effect.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(discovery_effect.get("object_ref") or accepted_summary.get("object_ref") or "")
            discovery_inventory = query_heavy_object_discovery_inventory_view(
                runtime_root,
                runtime_name=str(discovery_effect.get("runtime_name") or ""),
                object_kind=str(discovery_effect.get("object_kind") or accepted_summary.get("object_kind") or ""),
                discovery_kind=str(discovery_effect.get("discovery_kind") or accepted_summary.get("discovery_kind") or ""),
                repo_root=str(discovery_effect.get("repo_root") or ""),
            )
            matching_discovered_object = next(
                (
                    dict(item)
                    for item in list(discovery_inventory.get("discovered_objects") or [])
                    if (
                        (not effective_object_id or str(item.get("object_id") or "") == effective_object_id)
                        and (not effective_object_ref or str(item.get("object_ref") or "") == effective_object_ref)
                    )
                ),
                {},
            )
            trusted_expected_effect = {
                "command_kind": "heavy_object_discovery",
                "object_id": effective_object_id,
                "object_kind": str(discovery_effect.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "discovery_root": str(discovery_effect.get("discovery_root") or accepted_summary.get("discovery_root") or ""),
                "discovery_kind": str(discovery_effect.get("discovery_kind") or accepted_summary.get("discovery_kind") or ""),
                "runtime_name": str(discovery_effect.get("runtime_name") or ""),
                "repo_root": str(discovery_effect.get("repo_root") or ""),
                "duplicate_count": int(discovery_effect.get("duplicate_count") or 0),
            }
            projected_visible_effect = {
                "object_id": str(matching_discovered_object.get("object_id") or ""),
                "object_kind": str(matching_discovered_object.get("object_kind") or ""),
                "object_ref": str(matching_discovered_object.get("object_ref") or ""),
                "discovery_kind": str(matching_discovered_object.get("discovery_kind") or ""),
                "runtime_name": str(discovery_inventory.get("runtime_name") or ""),
                "repo_root": str(discovery_inventory.get("repo_root") or ""),
                "duplicate_count": int(matching_discovered_object.get("duplicate_count") or 0),
            }
            if not matching_discovered_object:
                mismatches.append("matching_discovered_object_missing")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["discovery_kind"] != trusted_expected_effect["discovery_kind"]:
                mismatches.append("discovery_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["duplicate_count"] != trusted_expected_effect["duplicate_count"]:
                mismatches.append("duplicate_count_mismatch")
        elif envelope_type == "heavy_object_observation_request":
            trace_surface = "heavy_object_observation_trace"
            trace_view = query_heavy_object_observation_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            observed_object = dict(trace_view.get("observed_object") or {})
            effective_object_id = str(observed_object.get("object_id") or accepted_summary.get("object_id") or "")
            effective_object_ref = str(observed_object.get("object_ref") or accepted_summary.get("object_ref") or "")
            observation_view = query_heavy_object_observation_view(
                runtime_root,
                runtime_name=str(observed_object.get("runtime_name") or ""),
                object_kind=str(observed_object.get("object_kind") or accepted_summary.get("object_kind") or ""),
                object_id=effective_object_id,
                object_ref=effective_object_ref,
            )
            latest_observation = dict(observation_view.get("latest_matching_observation") or {})
            latest_observation_payload = dict(latest_observation.get("payload") or {})
            trusted_expected_effect = {
                "command_kind": "heavy_object_observation",
                "object_id": effective_object_id,
                "object_kind": str(observed_object.get("object_kind") or accepted_summary.get("object_kind") or ""),
                "object_ref": effective_object_ref,
                "observation_kind": str(observed_object.get("observation_kind") or accepted_summary.get("observation_kind") or ""),
                "runtime_name": str(observed_object.get("runtime_name") or ""),
                "repo_root": str(observed_object.get("repo_root") or ""),
                "duplicate_count": int(observed_object.get("duplicate_count") or accepted_summary.get("duplicate_count") or 0),
                "total_bytes": int(observed_object.get("total_bytes") or accepted_summary.get("total_bytes") or 0),
            }
            projected_visible_effect = {
                "object_id": str(latest_observation_payload.get("object_id") or ""),
                "object_kind": str(latest_observation_payload.get("object_kind") or ""),
                "object_ref": str(latest_observation_payload.get("object_ref") or ""),
                "observation_kind": str(latest_observation_payload.get("observation_kind") or ""),
                "runtime_name": str(observation_view.get("runtime_name") or ""),
                "repo_root": str(latest_observation_payload.get("repo_root") or ""),
                "duplicate_count": int(observation_view.get("latest_duplicate_count") or 0),
                "total_bytes": int(observation_view.get("latest_total_bytes") or 0),
            }
            if not latest_observation_payload:
                mismatches.append("matching_observation_missing")
            if projected_visible_effect["object_id"] != trusted_expected_effect["object_id"]:
                mismatches.append("object_id_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["object_ref"] != trusted_expected_effect["object_ref"]:
                mismatches.append("object_ref_mismatch")
            if projected_visible_effect["observation_kind"] != trusted_expected_effect["observation_kind"]:
                mismatches.append("observation_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["duplicate_count"] != trusted_expected_effect["duplicate_count"]:
                mismatches.append("duplicate_count_mismatch")
            if projected_visible_effect["total_bytes"] != trusted_expected_effect["total_bytes"]:
                mismatches.append("total_bytes_mismatch")
        elif envelope_type == "heavy_object_authority_gap_audit_request":
            trace_surface = "heavy_object_authority_gap_audit_trace"
            trace_view = query_heavy_object_authority_gap_audit_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            audit_effect = dict(trace_view.get("audit_effect") or {})
            final_inventory = dict(trace_view.get("final_inventory") or {})
            trusted_expected_effect = {
                "command_kind": "heavy_object_authority_gap_audit",
                "repo_root": str(accepted_summary.get("repo_root") or ""),
                "object_kind": str(accepted_summary.get("object_kind") or ""),
                "runtime_name": str(accepted_summary.get("runtime_name") or ""),
                "audit_kind": str(accepted_summary.get("audit_kind") or ""),
                "candidate_count": int(audit_effect.get("candidate_count") or 0),
                "managed_candidate_count": int(audit_effect.get("managed_candidate_count") or 0),
                "unmanaged_candidate_count": int(audit_effect.get("unmanaged_candidate_count") or 0),
                "strongest_duplicate_count": int(audit_effect.get("strongest_duplicate_count") or 0),
            }
            projected_visible_effect = {
                "repo_root": str(final_inventory.get("repo_root") or ""),
                "object_kind": str(final_inventory.get("object_kind") or ""),
                "runtime_name": str(final_inventory.get("runtime_name") or ""),
                "filesystem_candidate_count": int(final_inventory.get("filesystem_candidate_count") or 0),
                "managed_candidate_count": int(final_inventory.get("managed_candidate_count") or 0),
                "unmanaged_candidate_count": int(final_inventory.get("unmanaged_candidate_count") or 0),
                "strongest_duplicate_count": int(final_inventory.get("strongest_duplicate_count") or 0),
            }
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["filesystem_candidate_count"] != trusted_expected_effect["candidate_count"]:
                mismatches.append("candidate_count_mismatch")
            if projected_visible_effect["managed_candidate_count"] != trusted_expected_effect["managed_candidate_count"]:
                mismatches.append("managed_candidate_count_mismatch")
            if projected_visible_effect["unmanaged_candidate_count"] != trusted_expected_effect["unmanaged_candidate_count"]:
                mismatches.append("unmanaged_candidate_count_mismatch")
            if projected_visible_effect["strongest_duplicate_count"] != trusted_expected_effect["strongest_duplicate_count"]:
                mismatches.append("strongest_duplicate_count_mismatch")
        elif envelope_type == "heavy_object_authority_gap_repo_remediation_request":
            trace_surface = "heavy_object_authority_gap_repo_remediation_trace"
            trace_view = query_heavy_object_authority_gap_repo_remediation_trace_view(
                runtime_root,
                envelope_id=normalized_envelope_id,
            )
            accepted_summary = dict(trace_view.get("accepted_envelope") or {})
            remediation_effect = dict(trace_view.get("remediation_effect") or {})
            final_inventory = dict(trace_view.get("final_inventory") or {})
            projected_requested_resolution_candidate_count = int(
                final_inventory.get("remediated_candidate_count") or 0
            ) + int(final_inventory.get("pending_remediation_candidate_count") or 0)
            projected_skipped_candidate_count = max(
                int(final_inventory.get("filesystem_candidate_count") or 0)
                - projected_requested_resolution_candidate_count
                - int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0),
                0,
            )
            projected_currently_fully_managed = (
                int(final_inventory.get("pending_remediation_candidate_count") or 0) == 0
                and int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0) == 0
            )
            trusted_expected_effect = {
                "command_kind": "heavy_object_authority_gap_repo_remediation",
                "repo_root": str(accepted_summary.get("repo_root") or ""),
                "object_kind": str(accepted_summary.get("object_kind") or ""),
                "runtime_name": str(accepted_summary.get("runtime_name") or ""),
                "remediation_kind": str(accepted_summary.get("remediation_kind") or ""),
                "candidate_count": int(remediation_effect.get("candidate_count") or 0),
                "requested_resolution_candidate_count": int(remediation_effect.get("requested_candidate_count") or 0),
                "skipped_candidate_count": int(remediation_effect.get("skipped_candidate_count") or 0),
                "fully_managed_after": bool(remediation_effect.get("fully_managed_after")),
            }
            projected_visible_effect = {
                "repo_root": str(final_inventory.get("repo_root") or ""),
                "object_kind": str(final_inventory.get("object_kind") or ""),
                "runtime_name": str(final_inventory.get("runtime_name") or ""),
                "filesystem_candidate_count": int(final_inventory.get("filesystem_candidate_count") or 0),
                "requested_resolution_candidate_count": projected_requested_resolution_candidate_count,
                "skipped_candidate_count": projected_skipped_candidate_count,
                "pending_remediation_candidate_count": int(final_inventory.get("pending_remediation_candidate_count") or 0),
                "unrequested_unmanaged_candidate_count": int(final_inventory.get("unrequested_unmanaged_candidate_count") or 0),
                "currently_fully_managed": projected_currently_fully_managed,
            }
            if projected_visible_effect["repo_root"] != trusted_expected_effect["repo_root"]:
                mismatches.append("repo_root_mismatch")
            if projected_visible_effect["object_kind"] != trusted_expected_effect["object_kind"]:
                mismatches.append("object_kind_mismatch")
            if projected_visible_effect["runtime_name"] != trusted_expected_effect["runtime_name"]:
                mismatches.append("runtime_name_mismatch")
            if projected_visible_effect["filesystem_candidate_count"] != trusted_expected_effect["candidate_count"]:
                mismatches.append("candidate_count_mismatch")
            if (
                projected_visible_effect["requested_resolution_candidate_count"]
                != trusted_expected_effect["requested_resolution_candidate_count"]
            ):
                mismatches.append("requested_resolution_candidate_count_mismatch")
            if projected_visible_effect["skipped_candidate_count"] != trusted_expected_effect["skipped_candidate_count"]:
                mismatches.append("skipped_candidate_count_mismatch")
            if projected_visible_effect["currently_fully_managed"] != trusted_expected_effect["fully_managed_after"]:
                mismatches.append("fully_managed_after_mismatch")
        else:
            trace_surface = "unsupported_heavy_object_command"
            mismatches.append("unsupported_heavy_object_command")

    else:
        trace_surface = "unsupported"
        mismatches.append("unsupported_classification")

    trace_gaps = [str(item) for item in list(trace_view.get("gaps") or [])]
    if trace_gaps:
        mismatches.append("trace_gaps_present")

    return {
        "runtime_root": str(runtime_root),
        "read_only": True,
        "envelope_id": normalized_envelope_id,
        "classification": classification,
        "trace_surface": trace_surface,
        "command_kind": str(trusted_expected_effect.get("command_kind") or ""),
        "trusted_expected_effect": trusted_expected_effect,
        "projected_visible_effect": projected_visible_effect,
        "trace_gaps": trace_gaps,
        "parity_ok": not mismatches,
        "mismatches": mismatches,
        "trace_view": trace_view,
    }


def _heavy_object_summary_payloads(
    runtime_root: Path,
    *,
    include_heavy_object_summaries: bool,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not include_heavy_object_summaries:
        return None, None
    return _heavy_object_authority_gap_summary(runtime_root), _heavy_object_pressure_summary(runtime_root)


def query_projection_consistency_view(
    state_root: Path,
    *,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Explain whether durable projections are caught up with the committed journal."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    compatibility = describe_event_journal_compatibility(runtime_root, initialize_if_missing=True)
    mixed_version_summary = _mixed_version_summary(compatibility)
    if str(compatibility.get("compatibility_status") or "") != "compatible":
        crash_consistency_summary = _crash_consistency_summary(
            compatibility_status=str(compatibility.get("compatibility_status") or ""),
            accepted_only_ids=[],
            committed_pending_projection_count=0,
            lagging_node_ids=[],
            visibility_state="incompatible",
            projection_replay_summary=None,
            latest_rebuild_result=None,
        )
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=str(compatibility.get("compatibility_status") or ""),
        )
        return {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(compatibility.get("compatibility_status") or ""),
            "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
            "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                compatibility.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(compatibility.get("journal_meta") or {}),
            "compatibility_issues": list(compatibility.get("compatibility_issues") or []),
            "mixed_version_summary": mixed_version_summary,
            "committed_seq": None,
            "journal_committed_seq": None,
            "kernel_projection": None,
            "node_projections": {},
            "projection_manifest": None,
            "bundle_alignment": {},
            "visibility_state": "incompatible",
            "pending_replay": None,
            "projection_replay_summary": None,
            "crash_consistency_summary": crash_consistency_summary,
        }
    projection_committed_seq = latest_projection_committed_seq(runtime_root)
    kernel_path = runtime_root / "state" / "kernel_state.json"
    kernel_projection = _projection_metadata(kernel_path)
    _kernel_exists, kernel_payload = _projection_payload(kernel_path)
    node_payload = dict(kernel_payload.get("nodes") or {})
    node_projections: dict[str, Any] = {}
    for node_id in sorted(node_payload):
        metadata = _projection_metadata(runtime_root / "state" / f"{node_id}.json")
        metadata["stale"] = int(metadata.get("last_applied_seq") or 0) < projection_committed_seq
        node_projections[str(node_id)] = metadata
    kernel_projection["stale"] = int(kernel_projection.get("last_applied_seq") or 0) < projection_committed_seq
    projection_manifest = _projection_manifest_metadata(runtime_root / "state" / "ProjectionManifest.json")
    projection_manifest["stale"] = int(projection_manifest.get("last_applied_seq") or 0) < projection_committed_seq
    pending_replay = _pending_replay_summary(
        runtime_root,
        committed_seq=projection_committed_seq,
        after_seq=int(kernel_projection.get("last_applied_seq") or 0),
    )
    accepted_only_ids = sorted(_accepted_audit_envelope_ids(runtime_root) - _mirrored_envelope_ids(runtime_root))
    expected_node_ids = sorted(str(node_id) for node_id in node_payload)
    manifest_node_ids = [str(item) for item in list(projection_manifest.get("covered_node_ids") or [])]
    expected_manifest_ref = str((runtime_root / "state" / "ProjectionManifest.json").resolve())
    expected_journal_ref = str((runtime_root / "event_journal" / "control_plane.sqlite3").resolve())
    manifest_matches_nodes = (
        manifest_node_ids == expected_node_ids
        and int(projection_manifest.get("covered_node_count") or 0) == len(expected_node_ids)
    )
    kernel_bundle_aligned = (
        str(kernel_projection.get("projection_bundle_ref") or "") == expected_manifest_ref
        and str(kernel_projection.get("source_event_journal_ref") or "") == expected_journal_ref
    )
    node_bundle_aligned = all(
        str(dict(metadata or {}).get("projection_bundle_ref") or "") == expected_manifest_ref
        and str(dict(metadata or {}).get("source_event_journal_ref") or "") == expected_journal_ref
        for metadata in node_projections.values()
    )
    atomically_visible = (
        bool(projection_manifest.get("exists"))
        and not bool(projection_manifest.get("stale"))
        and str(projection_manifest.get("projection_bundle_ref") or "") == expected_manifest_ref
        and str(projection_manifest.get("source_event_journal_ref") or "") == expected_journal_ref
        and int(kernel_projection.get("last_applied_seq") or 0) == projection_committed_seq
        and manifest_matches_nodes
        and kernel_bundle_aligned
        and node_bundle_aligned
        and all(
            int(dict(metadata or {}).get("last_applied_seq") or 0) == projection_committed_seq
            for metadata in node_projections.values()
        )
    )
    if not bool(projection_manifest.get("exists")):
        visibility_state = "untracked"
    elif atomically_visible:
        visibility_state = "atomically_visible"
    else:
        visibility_state = "partial"
    latest_rebuild_result = _latest_projection_rebuild_result(runtime_root, rebuild_scope="kernel")
    projection_replay_summary = _projection_replay_summary(
        runtime_root,
        committed_seq=projection_committed_seq,
        kernel_projection=kernel_projection,
        node_projections=node_projections,
        projection_manifest=projection_manifest,
        visibility_state=visibility_state,
        pending_replay=pending_replay,
        latest_rebuild_result=latest_rebuild_result,
    )
    committed_pending_projection_count = max(
        projection_committed_seq - int(kernel_projection.get("last_applied_seq") or 0),
        0,
    )
    crash_consistency_summary = _crash_consistency_summary(
        compatibility_status="compatible",
        accepted_only_ids=accepted_only_ids,
        committed_pending_projection_count=committed_pending_projection_count,
        lagging_node_ids=list(projection_replay_summary.get("lagging_node_ids") or []),
        visibility_state=visibility_state,
        projection_replay_summary=projection_replay_summary,
        latest_rebuild_result=latest_rebuild_result,
    )
    return {
        "runtime_root": str(runtime_root),
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
        "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            compatibility.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(compatibility.get("journal_meta") or {}),
        "compatibility_issues": [],
        "mixed_version_summary": mixed_version_summary,
        "committed_seq": projection_committed_seq,
        "journal_committed_seq": latest_committed_seq(runtime_root),
        "kernel_projection": kernel_projection,
        "node_projections": node_projections,
        "projection_manifest": projection_manifest,
        "bundle_alignment": {
            "expected_projection_manifest_ref": expected_manifest_ref,
            "expected_source_event_journal_ref": expected_journal_ref,
            "kernel_bundle_aligned": kernel_bundle_aligned,
            "node_bundle_aligned": node_bundle_aligned,
        },
        "visibility_state": visibility_state,
        "pending_replay": pending_replay,
        "projection_replay_summary": projection_replay_summary,
        "crash_consistency_summary": crash_consistency_summary,
    }


def _projection_explanation_summary_from_preflight_surfaces(
    runtime_root: Path,
    *,
    projection_consistency: dict[str, Any],
    commit_state: dict[str, Any],
    node_id: str | None = None,
) -> dict[str, Any]:
    """Build a summary-only projection explanation from precomputed preflight surfaces."""

    consistency = dict(projection_consistency or {})
    commit = dict(commit_state or {})
    compatibility_status = str(consistency.get("compatibility_status") or commit.get("compatibility_status") or "")
    requested_node_id = str(node_id or "").strip()
    if compatibility_status != "compatible":
        lease_fencing_summary = dict(commit.get("lease_fencing_summary") or {})
        if not lease_fencing_summary:
            lease_fencing_summary = _lease_fencing_summary_from_rows([], compatibility_status=compatibility_status)
        return {
            "runtime_root": str(runtime_root),
            "requested_node_id": requested_node_id or None,
            "compatibility_status": compatibility_status,
            "supported_event_schema_version": str(
                consistency.get("supported_event_schema_version") or commit.get("supported_event_schema_version") or ""
            ),
            "supported_store_format_version": str(
                consistency.get("supported_store_format_version") or commit.get("supported_store_format_version") or ""
            ),
            "supported_producer_generation": str(
                consistency.get("supported_producer_generation") or commit.get("supported_producer_generation") or ""
            ),
            "required_producer_generation": str(
                consistency.get("required_producer_generation") or commit.get("required_producer_generation") or ""
            ),
            "journal_meta_required_producer_generation": str(
                consistency.get("journal_meta_required_producer_generation")
                or commit.get("journal_meta_required_producer_generation")
                or ""
            ),
            "writer_fencing_status": str(
                consistency.get("writer_fencing_status") or commit.get("writer_fencing_status") or ""
            ),
            "writer_fence_alignment_status": str(
                consistency.get("writer_fence_alignment_status")
                or commit.get("writer_fence_alignment_status")
                or ""
            ),
            "journal_meta": dict(consistency.get("journal_meta") or commit.get("journal_meta") or {}),
            "compatibility_issues": list(consistency.get("compatibility_issues") or commit.get("compatibility_issues") or []),
            "mixed_version_summary": dict(consistency.get("mixed_version_summary") or commit.get("mixed_version_summary") or {}),
            "visibility_state": "incompatible",
            "committed_seq": None,
            "journal_committed_seq": None,
            "effective_projection_source": "",
            "effective_last_applied_seq": None,
            "effective_source_event_journal_ref": "",
            "effective_projection_bundle_ref": "",
            "projection_manifest": None,
            "kernel_projection": None,
            "node_projection": None,
            "applied_event_tail": [],
            "pending_event_head": [],
            "pending_replay": None,
            "projection_replay_summary": None,
            "crash_consistency_summary": dict(consistency.get("crash_consistency_summary") or {}),
            "lease_fencing_summary": lease_fencing_summary,
            "heavy_object_authority_gap_summary": None,
            "heavy_object_pressure_summary": None,
        }

    kernel_projection = dict(consistency.get("kernel_projection") or {})
    node_projections = dict(consistency.get("node_projections") or {})
    manifest = dict(consistency.get("projection_manifest") or {})
    visibility_state = str(consistency.get("visibility_state") or "")
    selected_node_projection = dict(node_projections.get(requested_node_id) or {}) if requested_node_id else {}
    if visibility_state == "atomically_visible" and bool(manifest.get("exists")):
        effective_source = "projection_manifest"
        effective_last_applied_seq = int(manifest.get("last_applied_seq") or 0)
    elif requested_node_id and selected_node_projection:
        effective_source = "node_projection"
        effective_last_applied_seq = int(selected_node_projection.get("last_applied_seq") or 0)
    else:
        effective_source = "kernel_projection"
        effective_last_applied_seq = int(kernel_projection.get("last_applied_seq") or 0)

    effective_source_payload = (
        manifest
        if effective_source == "projection_manifest"
        else (selected_node_projection if effective_source == "node_projection" else kernel_projection)
    )
    return {
        "runtime_root": str(runtime_root),
        "requested_node_id": requested_node_id or None,
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(consistency.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(consistency.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(consistency.get("supported_producer_generation") or ""),
        "required_producer_generation": str(consistency.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            consistency.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(consistency.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(consistency.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(consistency.get("journal_meta") or {}),
        "compatibility_issues": list(consistency.get("compatibility_issues") or []),
        "mixed_version_summary": dict(consistency.get("mixed_version_summary") or {}),
        "visibility_state": visibility_state,
        "committed_seq": int(consistency.get("committed_seq") or 0),
        "journal_committed_seq": int(consistency.get("journal_committed_seq") or 0),
        "effective_projection_source": effective_source,
        "effective_last_applied_seq": effective_last_applied_seq,
        "effective_source_event_journal_ref": str(effective_source_payload.get("source_event_journal_ref") or ""),
        "effective_projection_bundle_ref": str(effective_source_payload.get("projection_bundle_ref") or ""),
        "projection_manifest": manifest,
        "kernel_projection": kernel_projection,
        "node_projection": selected_node_projection or None,
        "applied_event_tail": [],
        "pending_event_head": [],
        "pending_replay": dict(consistency.get("pending_replay") or {}),
        "projection_replay_summary": dict(consistency.get("projection_replay_summary") or {}),
        "crash_consistency_summary": dict(consistency.get("crash_consistency_summary") or {}),
        "lease_fencing_summary": dict(commit.get("lease_fencing_summary") or {}),
        "heavy_object_authority_gap_summary": None,
        "heavy_object_pressure_summary": None,
    }


def query_projection_explanation_view(
    state_root: Path,
    *,
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Explain which committed facts currently underpin the visible projection bundle."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    consistency = query_projection_consistency_view(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    if str(consistency.get("compatibility_status") or "") != "compatible":
        crash_consistency_summary = _crash_consistency_summary(
            compatibility_status=str(consistency.get("compatibility_status") or ""),
            accepted_only_ids=[],
            committed_pending_projection_count=0,
            lagging_node_ids=[],
            visibility_state="incompatible",
            projection_replay_summary=None,
            latest_rebuild_result=None,
        )
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=str(consistency.get("compatibility_status") or ""),
        )
        return {
            "runtime_root": str(runtime_root),
            "requested_node_id": str(node_id or "").strip() or None,
            "compatibility_status": str(consistency.get("compatibility_status") or ""),
            "supported_event_schema_version": str(consistency.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(consistency.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(consistency.get("supported_producer_generation") or ""),
            "required_producer_generation": str(consistency.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                consistency.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(consistency.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(consistency.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(consistency.get("journal_meta") or {}),
            "compatibility_issues": list(consistency.get("compatibility_issues") or []),
            "mixed_version_summary": dict(consistency.get("mixed_version_summary") or {}),
            "visibility_state": "incompatible",
            "committed_seq": None,
            "journal_committed_seq": None,
            "effective_projection_source": "",
            "effective_last_applied_seq": None,
            "effective_source_event_journal_ref": "",
            "effective_projection_bundle_ref": "",
            "projection_manifest": None,
            "kernel_projection": None,
            "node_projection": None,
            "applied_event_tail": [],
            "pending_event_head": [],
            "pending_replay": None,
            "projection_replay_summary": None,
            "crash_consistency_summary": crash_consistency_summary,
            "lease_fencing_summary": lease_fencing_summary,
            "heavy_object_authority_gap_summary": None,
            "heavy_object_pressure_summary": None,
        }
    kernel_projection = dict(consistency.get("kernel_projection") or {})
    node_projections = dict(consistency.get("node_projections") or {})
    manifest = dict(consistency.get("projection_manifest") or {})
    committed_seq = int(consistency.get("committed_seq") or 0)
    visibility_state = str(consistency.get("visibility_state") or "")
    requested_node_id = str(node_id or "").strip()
    selected_node_projection = (
        dict(node_projections.get(requested_node_id) or {}) if requested_node_id else {}
    )
    if visibility_state == "atomically_visible" and bool(manifest.get("exists")):
        effective_source = "projection_manifest"
        effective_last_applied_seq = int(manifest.get("last_applied_seq") or 0)
    elif requested_node_id and selected_node_projection:
        effective_source = "node_projection"
        effective_last_applied_seq = int(selected_node_projection.get("last_applied_seq") or 0)
    else:
        effective_source = "kernel_projection"
        effective_last_applied_seq = int(kernel_projection.get("last_applied_seq") or 0)

    applied_tail: deque[dict[str, Any]] = deque(maxlen=max(int(tail_limit), 0))
    pending_head: list[dict[str, Any]] = []
    for event in iter_committed_events(runtime_root):
        seq = int(event.get("seq") or 0)
        if seq <= effective_last_applied_seq:
            if tail_limit > 0:
                applied_tail.append(_event_summary(event))
            continue
        if len(pending_head) < max(int(tail_limit), 0):
            pending_head.append(_event_summary(event))
    heavy_object_authority_gap_summary, heavy_object_pressure_summary = _heavy_object_summary_payloads(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    lease_fencing_summary = _lease_fencing_summary(runtime_root)
    latest_rebuild_result = (
        _latest_projection_rebuild_result(runtime_root, target_node_id=requested_node_id, rebuild_scope="node")
        if requested_node_id
        else None
    ) or _latest_projection_rebuild_result(runtime_root, rebuild_scope="kernel")
    projection_replay_summary = _projection_replay_summary(
        runtime_root,
        committed_seq=int(consistency.get("committed_seq") or 0),
        kernel_projection=kernel_projection,
        node_projections=node_projections,
        projection_manifest=manifest,
        visibility_state=visibility_state,
        pending_replay=dict(consistency.get("pending_replay") or {}),
        latest_rebuild_result=latest_rebuild_result,
    )
    accepted_only_ids = sorted(_accepted_audit_envelope_ids(runtime_root) - _mirrored_envelope_ids(runtime_root))
    crash_consistency_summary = _crash_consistency_summary(
        compatibility_status="compatible",
        accepted_only_ids=accepted_only_ids,
        committed_pending_projection_count=max(
            int(consistency.get("committed_seq") or 0) - int(kernel_projection.get("last_applied_seq") or 0),
            0,
        ),
        lagging_node_ids=list(projection_replay_summary.get("lagging_node_ids") or []),
        visibility_state=visibility_state,
        projection_replay_summary=projection_replay_summary,
        latest_rebuild_result=latest_rebuild_result,
    )

    return {
        "runtime_root": str(runtime_root),
        "requested_node_id": requested_node_id or None,
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(consistency.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(consistency.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(consistency.get("supported_producer_generation") or ""),
        "required_producer_generation": str(consistency.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            consistency.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(consistency.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(consistency.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(consistency.get("journal_meta") or {}),
        "compatibility_issues": [],
        "mixed_version_summary": dict(consistency.get("mixed_version_summary") or {}),
        "visibility_state": visibility_state,
        "committed_seq": committed_seq,
        "journal_committed_seq": int(consistency.get("journal_committed_seq") or latest_committed_seq(runtime_root)),
        "effective_projection_source": effective_source,
        "effective_last_applied_seq": effective_last_applied_seq,
        "effective_source_event_journal_ref": str(
            (
                manifest.get("source_event_journal_ref")
                if effective_source == "projection_manifest"
                else (
                    selected_node_projection.get("source_event_journal_ref")
                    if effective_source == "node_projection"
                    else kernel_projection.get("source_event_journal_ref")
                )
            )
            or ""
        ),
        "effective_projection_bundle_ref": str(
            (
                manifest.get("projection_bundle_ref")
                if effective_source == "projection_manifest"
                else (
                    selected_node_projection.get("projection_bundle_ref")
                    if effective_source == "node_projection"
                    else kernel_projection.get("projection_bundle_ref")
                )
            )
            or ""
        ),
        "projection_manifest": manifest,
        "kernel_projection": kernel_projection,
        "node_projection": selected_node_projection or None,
        "applied_event_tail": list(applied_tail),
        "pending_event_head": pending_head,
        "pending_replay": dict(consistency.get("pending_replay") or {}),
        "projection_replay_summary": projection_replay_summary,
        "crash_consistency_summary": crash_consistency_summary,
        "lease_fencing_summary": lease_fencing_summary,
        "heavy_object_authority_gap_summary": heavy_object_authority_gap_summary,
        "heavy_object_pressure_summary": heavy_object_pressure_summary,
    }


def _query_projection_replay_trace_view(
    runtime_root: Path,
    *,
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
    projection_consistency: dict[str, Any] | None = None,
    projection_explanation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    explanation = dict(
        projection_explanation
        or query_projection_explanation_view(
            runtime_root,
            node_id=node_id,
            tail_limit=tail_limit,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    if str(explanation.get("compatibility_status") or "") != "compatible":
        return {
            "runtime_root": str(runtime_root),
            "requested_node_id": str(node_id or "").strip() or None,
            "compatibility_status": str(explanation.get("compatibility_status") or ""),
            "supported_event_schema_version": str(explanation.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(explanation.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(explanation.get("supported_producer_generation") or ""),
            "required_producer_generation": str(explanation.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                explanation.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(explanation.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(explanation.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(explanation.get("journal_meta") or {}),
            "compatibility_issues": list(explanation.get("compatibility_issues") or []),
            "mixed_version_summary": dict(explanation.get("mixed_version_summary") or {}),
            "journal_committed_seq": None,
            "projection_committed_seq": None,
            "visibility_state": "incompatible",
            "effective_projection_source": "",
            "effective_last_applied_seq": None,
            "projection_replay_summary": None,
            "crash_consistency_summary": dict(explanation.get("crash_consistency_summary") or {}),
            "pending_replay": None,
            "latest_projection_rebuild_result": None,
            "latest_kernel_projection_rebuild_result": None,
            "latest_node_projection_rebuild_result": None,
            "applied_projection_event_tail": [],
            "pending_projection_event_head": [],
            "projection_consistency": query_projection_consistency_view(
                runtime_root,
                include_heavy_object_summaries=include_heavy_object_summaries,
            ),
            "projection_explanation": explanation,
        }
    consistency = dict(
        projection_consistency
        or query_projection_consistency_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    requested_node_id = str(node_id or "").strip()
    latest_kernel_rebuild = _latest_projection_rebuild_result(runtime_root, rebuild_scope="kernel")
    latest_node_rebuild = (
        _latest_projection_rebuild_result(runtime_root, target_node_id=requested_node_id, rebuild_scope="node")
        if requested_node_id
        else None
    )
    latest_rebuild_result = latest_node_rebuild or latest_kernel_rebuild

    effective_last_applied_seq = int(explanation.get("effective_last_applied_seq") or 0)
    applied_projection_event_tail: deque[dict[str, Any]] = deque(maxlen=max(int(tail_limit), 0))
    pending_projection_event_head: list[dict[str, Any]] = []
    for event in iter_committed_events(runtime_root):
        if not event_affects_projection(event):
            continue
        seq = int(event.get("seq") or 0)
        if seq <= effective_last_applied_seq:
            if tail_limit > 0:
                applied_projection_event_tail.append(_event_summary(event))
            continue
        if len(pending_projection_event_head) < max(int(tail_limit), 0):
            pending_projection_event_head.append(_event_summary(event))

    return {
        "runtime_root": str(runtime_root),
        "requested_node_id": requested_node_id or None,
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(explanation.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(explanation.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(explanation.get("supported_producer_generation") or ""),
        "required_producer_generation": str(explanation.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            explanation.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(explanation.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(explanation.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(explanation.get("journal_meta") or {}),
        "compatibility_issues": [],
        "mixed_version_summary": dict(explanation.get("mixed_version_summary") or {}),
        "journal_committed_seq": int(consistency.get("journal_committed_seq") or latest_committed_seq(runtime_root)),
        "projection_committed_seq": int(consistency.get("committed_seq") or 0),
        "visibility_state": str(consistency.get("visibility_state") or ""),
        "effective_projection_source": str(explanation.get("effective_projection_source") or ""),
        "effective_last_applied_seq": effective_last_applied_seq,
        "projection_replay_summary": dict(explanation.get("projection_replay_summary") or {}),
        "crash_consistency_summary": dict(explanation.get("crash_consistency_summary") or {}),
        "pending_replay": dict(explanation.get("pending_replay") or {}),
        "latest_projection_rebuild_result": latest_rebuild_result,
        "latest_kernel_projection_rebuild_result": latest_kernel_rebuild,
        "latest_node_projection_rebuild_result": latest_node_rebuild,
        "applied_projection_event_tail": list(applied_projection_event_tail),
        "pending_projection_event_head": pending_projection_event_head,
        "projection_consistency": consistency,
        "projection_explanation": explanation,
    }


def query_projection_replay_trace_view(
    state_root: Path,
    *,
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Explain projection replay/crash truth through committed causal history."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    return _query_projection_replay_trace_view(
        runtime_root,
        node_id=node_id,
        tail_limit=tail_limit,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )


def _query_projection_crash_consistency_matrix_view(
    runtime_root: Path,
    *,
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
    event_journal_status: dict[str, Any] | None = None,
    commit_state: dict[str, Any] | None = None,
    projection_consistency: dict[str, Any] | None = None,
    projection_explanation: dict[str, Any] | None = None,
    projection_replay_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status = dict(
        event_journal_status
        or query_event_journal_status_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    commit = dict(
        commit_state
        or query_commit_state_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    consistency = dict(
        projection_consistency
        or query_projection_consistency_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    explanation = dict(
        projection_explanation
        or query_projection_explanation_view(
            runtime_root,
            node_id=node_id,
            tail_limit=tail_limit,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    replay_trace = dict(
        projection_replay_trace
        or _query_projection_replay_trace_view(
            runtime_root,
            node_id=node_id,
            tail_limit=tail_limit,
            include_heavy_object_summaries=include_heavy_object_summaries,
            projection_consistency=consistency,
            projection_explanation=explanation,
        )
    )
    return {
        "runtime_root": str(runtime_root),
        "requested_node_id": str(node_id or "").strip() or None,
        "compatibility_status": str(replay_trace.get("compatibility_status") or ""),
        "supported_event_schema_version": str(replay_trace.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(replay_trace.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(replay_trace.get("supported_producer_generation") or ""),
        "required_producer_generation": str(replay_trace.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            replay_trace.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(replay_trace.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(replay_trace.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(replay_trace.get("journal_meta") or {}),
        "compatibility_issues": list(replay_trace.get("compatibility_issues") or []),
        "mixed_version_summary": dict(replay_trace.get("mixed_version_summary") or {}),
        "crash_consistency_summary": dict(replay_trace.get("crash_consistency_summary") or {}),
        "event_journal_status": status,
        "commit_state": commit,
        "projection_consistency": consistency,
        "projection_explanation": explanation,
        "projection_replay_trace": replay_trace,
    }


def query_projection_crash_consistency_matrix_view(
    state_root: Path,
    *,
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Return one read-only matrix over current projection crash-consistency truth."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    return _query_projection_crash_consistency_matrix_view(
        runtime_root,
        node_id=node_id,
        tail_limit=tail_limit,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )


def _query_lease_fencing_matrix_view(
    runtime_root: Path,
    *,
    now_utc: str = "",
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
    runtime_liveness_view: dict[str, Any] | None = None,
    event_journal_status: dict[str, Any] | None = None,
    commit_state: dict[str, Any] | None = None,
    projection_explanation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    liveness_view = dict(runtime_liveness_view or query_runtime_liveness_view(runtime_root, now_utc=now_utc))
    compatibility_status = str(liveness_view.get("compatibility_status") or "")
    try:
        if compatibility_status == "compatible":
            kernel_state = load_kernel_state(runtime_root)
            authority_view = {
                "runtime_root": str(runtime_root),
                "root_node_id": str(getattr(kernel_state, "root_node_id", "") or ""),
                "node_graph": [
                    {
                        "node_id": str(graph_node_id),
                        "status": str(dict(node or {}).get("status") or ""),
                    }
                    for graph_node_id, node in dict(getattr(kernel_state, "nodes", {}) or {}).items()
                ],
            }
        else:
            authority_view = {}
    except Exception:
        authority_view = {}
    status = dict(
        event_journal_status
        or query_event_journal_status_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    commit = dict(
        commit_state
        or query_commit_state_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    explanation = dict(
        projection_explanation
        or query_projection_explanation_view(
            runtime_root,
            node_id=node_id,
            tail_limit=tail_limit,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    requested_node_id = str(node_id or "").strip()
    if compatibility_status != "compatible":
        matrix_rows: list[dict[str, Any]] = []
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=compatibility_status,
        )
    else:
        matrix_rows = _lease_fencing_matrix_rows(
            raw_latest_by_node={
                str(node_key): dict(payload or {})
                for node_key, payload in dict(liveness_view.get("latest_runtime_liveness_by_node") or {}).items()
            },
            effective_by_node={
                str(node_key): dict(payload or {})
                for node_key, payload in dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).items()
            },
            node_graph=list(authority_view.get("node_graph") or []),
            root_node_id=str(authority_view.get("root_node_id") or ""),
        )
        if requested_node_id:
            matrix_rows = [row for row in matrix_rows if str(row.get("node_id") or "") == requested_node_id]
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            matrix_rows,
            compatibility_status="compatible",
        )
    return {
        "runtime_root": str(runtime_root),
        "requested_node_id": requested_node_id or None,
        "compatibility_status": compatibility_status or str(status.get("compatibility_status") or ""),
        "compatibility_issues": list(liveness_view.get("compatibility_issues") or []),
        "lease_fencing_summary": lease_fencing_summary,
        "matrix_rows": matrix_rows,
        "authority_view": authority_view,
        "runtime_liveness_view": liveness_view,
        "event_journal_status": status,
        "commit_state": commit,
        "projection_explanation": explanation,
    }


def query_lease_fencing_matrix_view(
    state_root: Path,
    *,
    now_utc: str = "",
    node_id: str | None = None,
    tail_limit: int = 5,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Return one read-only matrix over current lease/fencing truth."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    return _query_lease_fencing_matrix_view(
        runtime_root,
        now_utc=now_utc,
        node_id=node_id,
        tail_limit=tail_limit,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )


def query_event_journal_status_view(
    state_root: Path,
    *,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Return a read-only compatibility/debug view over journal meta and projection lag."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    compatibility = describe_event_journal_compatibility(runtime_root, initialize_if_missing=True)
    mixed_version_summary = _mixed_version_summary(compatibility)
    if str(compatibility.get("compatibility_status") or "") != "compatible":
        crash_consistency_summary = _crash_consistency_summary(
            compatibility_status=str(compatibility.get("compatibility_status") or ""),
            accepted_only_ids=[],
            committed_pending_projection_count=0,
            lagging_node_ids=[],
            visibility_state="incompatible",
            projection_replay_summary=None,
            latest_rebuild_result=None,
        )
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=str(compatibility.get("compatibility_status") or ""),
        )
        return {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(compatibility.get("compatibility_status") or ""),
            "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
            "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                compatibility.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(compatibility.get("journal_meta") or {}),
            "compatibility_issues": list(compatibility.get("compatibility_issues") or []),
            "mixed_version_summary": mixed_version_summary,
            "crash_consistency_summary": crash_consistency_summary,
            "lease_fencing_summary": lease_fencing_summary,
            "committed_event_count": None,
            "committed_seq": None,
            "observed_producer_generations": [],
            "unstamped_committed_event_count": None,
            "projection_consistency": None,
            "projection_replay_summary": None,
            "heavy_object_authority_gap_summary": None,
            "heavy_object_pressure_summary": None,
        }
    observed_producer_generations: list[str] = []
    unstamped_committed_event_count = 0
    for event in iter_committed_events(runtime_root):
        producer_generation = str(event.get("producer_generation") or "").strip()
        if not producer_generation:
            unstamped_committed_event_count += 1
            continue
        if producer_generation not in observed_producer_generations:
            observed_producer_generations.append(producer_generation)
    projection_consistency = query_projection_consistency_view(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    projection_replay_summary = dict(projection_consistency.get("projection_replay_summary") or {})
    crash_consistency_summary = dict(projection_consistency.get("crash_consistency_summary") or {})
    heavy_object_authority_gap_summary, heavy_object_pressure_summary = _heavy_object_summary_payloads(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    lease_fencing_summary = _lease_fencing_summary(runtime_root)
    return {
        "runtime_root": str(runtime_root),
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
        "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            compatibility.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(compatibility.get("journal_meta") or {}),
        "compatibility_issues": list(compatibility.get("compatibility_issues") or []),
        "mixed_version_summary": mixed_version_summary,
        "crash_consistency_summary": crash_consistency_summary,
        "lease_fencing_summary": lease_fencing_summary,
        "committed_event_count": committed_event_count(runtime_root),
        "committed_seq": latest_committed_seq(runtime_root),
        "observed_producer_generations": observed_producer_generations,
        "unstamped_committed_event_count": unstamped_committed_event_count,
        "projection_consistency": projection_consistency,
        "projection_replay_summary": projection_replay_summary,
        "heavy_object_authority_gap_summary": heavy_object_authority_gap_summary,
        "heavy_object_pressure_summary": heavy_object_pressure_summary,
    }


def query_operational_hygiene_view(
    state_root: Path,
    *,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Return the latest committed hygiene/heavy-object observations for one runtime root."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    compatibility = describe_event_journal_compatibility(runtime_root, initialize_if_missing=True)
    if str(compatibility.get("compatibility_status") or "") != "compatible":
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=str(compatibility.get("compatibility_status") or ""),
        )
        return {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(compatibility.get("compatibility_status") or ""),
            "compatibility_issues": list(compatibility.get("compatibility_issues") or []),
            "committed_seq": None,
            "operational_hygiene_event_count": None,
            "heavy_object_event_count": None,
            "process_orphan_detected_event_count": None,
            "reap_requested_event_count": None,
            "reap_deferred_event_count": None,
            "process_reap_confirmed_event_count": None,
            "test_runtime_cleanup_committed_event_count": None,
            "runtime_reap_requested_event_count": None,
            "runtime_reap_deferred_event_count": None,
            "runtime_reap_confirmed_event_count": None,
            "runtime_root_quiesced_event_count": None,
            "latest_operational_hygiene": None,
            "latest_heavy_object": None,
            "latest_process_orphan_detected": None,
            "latest_reap_requested": None,
            "latest_reap_deferred": None,
            "latest_process_reap_confirmed": None,
            "latest_test_runtime_cleanup_committed": None,
            "latest_runtime_reap_requested": None,
            "latest_runtime_reap_deferred": None,
            "latest_runtime_reap_confirmed": None,
            "latest_runtime_root_quiesced": None,
            "heavy_object_authority_gap_summary": None,
            "heavy_object_pressure_summary": None,
        }

    event_types = (
        "operational_hygiene_observed",
        "heavy_object_observed",
        "process_orphan_detected",
        "runtime_reap_requested",
        "runtime_reap_deferred",
        "runtime_reap_confirmed",
        "reap_requested",
        "reap_deferred",
        "process_reap_confirmed",
        "test_runtime_cleanup_committed",
        "runtime_root_quiesced",
    )
    event_stats = committed_event_stats_by_type(runtime_root, event_types=event_types)

    def _count(event_type: str) -> int:
        return int(dict(event_stats.get(event_type) or {}).get("count") or 0)

    def _latest_summary(event_type: str) -> dict[str, Any] | None:
        event = dict(dict(event_stats.get(event_type) or {}).get("latest_event") or {})
        if not event:
            return None
        summary = _event_summary(event)
        summary["payload"] = dict(event.get("payload") or {})
        return summary

    latest_hygiene = _latest_summary("operational_hygiene_observed")
    latest_heavy = _latest_summary("heavy_object_observed")
    latest_process_orphan_detected = _latest_summary("process_orphan_detected")
    latest_reap_requested = _latest_summary("runtime_reap_requested")
    latest_reap_deferred = _latest_summary("runtime_reap_deferred")
    latest_reap_confirmed = _latest_summary("runtime_reap_confirmed")
    latest_canonical_reap_requested = _latest_summary("reap_requested")
    latest_canonical_reap_deferred = _latest_summary("reap_deferred")
    latest_process_reap_confirmed = _latest_summary("process_reap_confirmed")
    latest_test_runtime_cleanup_committed = _latest_summary("test_runtime_cleanup_committed")
    latest_runtime_root_quiesced = _latest_summary("runtime_root_quiesced")
    hygiene_count = _count("operational_hygiene_observed")
    heavy_count = _count("heavy_object_observed")
    process_orphan_detected_count = _count("process_orphan_detected")
    reap_requested_count = _count("runtime_reap_requested")
    reap_deferred_count = _count("runtime_reap_deferred")
    reap_confirmed_count = _count("runtime_reap_confirmed")
    canonical_reap_requested_count = _count("reap_requested")
    canonical_reap_deferred_count = _count("reap_deferred")
    process_reap_confirmed_count = _count("process_reap_confirmed")
    test_runtime_cleanup_committed_count = _count("test_runtime_cleanup_committed")
    runtime_root_quiesced_count = _count("runtime_root_quiesced")
    heavy_object_authority_gap_summary, heavy_object_pressure_summary = _heavy_object_summary_payloads(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    return {
        "runtime_root": str(runtime_root),
        "compatibility_status": "compatible",
        "compatibility_issues": [],
        "committed_seq": latest_committed_seq(runtime_root),
        "operational_hygiene_event_count": hygiene_count,
        "heavy_object_event_count": heavy_count,
        "process_orphan_detected_event_count": process_orphan_detected_count,
        "reap_requested_event_count": canonical_reap_requested_count,
        "reap_deferred_event_count": canonical_reap_deferred_count,
        "process_reap_confirmed_event_count": process_reap_confirmed_count,
        "test_runtime_cleanup_committed_event_count": test_runtime_cleanup_committed_count,
        "runtime_reap_requested_event_count": reap_requested_count,
        "runtime_reap_deferred_event_count": reap_deferred_count,
        "runtime_reap_confirmed_event_count": reap_confirmed_count,
        "runtime_root_quiesced_event_count": runtime_root_quiesced_count,
        "latest_operational_hygiene": latest_hygiene,
        "latest_heavy_object": latest_heavy,
        "latest_process_orphan_detected": latest_process_orphan_detected,
        "latest_reap_requested": latest_canonical_reap_requested,
        "latest_reap_deferred": latest_canonical_reap_deferred,
        "latest_process_reap_confirmed": latest_process_reap_confirmed,
        "latest_test_runtime_cleanup_committed": latest_test_runtime_cleanup_committed,
        "latest_runtime_reap_requested": latest_reap_requested,
        "latest_runtime_reap_deferred": latest_reap_deferred,
        "latest_runtime_reap_confirmed": latest_reap_confirmed,
        "latest_runtime_root_quiesced": latest_runtime_root_quiesced,
        "heavy_object_authority_gap_summary": heavy_object_authority_gap_summary,
        "heavy_object_pressure_summary": heavy_object_pressure_summary,
    }


def query_runtime_liveness_view(
    state_root: Path,
    *,
    now_utc: str = "",
    initialize_if_missing: bool = True,
) -> dict[str, Any]:
    """Return the latest committed runtime-liveness observations for one runtime root."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    compatibility = describe_event_journal_compatibility(
        runtime_root,
        initialize_if_missing=bool(initialize_if_missing),
    )
    if str(compatibility.get("compatibility_status") or "") != "compatible":
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=str(compatibility.get("compatibility_status") or ""),
        )
        return {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(compatibility.get("compatibility_status") or ""),
            "compatibility_issues": list(compatibility.get("compatibility_issues") or []),
            "committed_seq": None,
            "runtime_liveness_event_count": None,
            "attachment_state_counts": {},
            "effective_attachment_state_counts": {},
            "latest_runtime_liveness": None,
            "latest_runtime_liveness_by_node": {},
            "effective_runtime_liveness_by_node": {},
            "lease_fencing_summary": lease_fencing_summary,
        }

    latest_liveness: dict[str, Any] | None = None
    raw_latest_by_node: dict[str, dict[str, Any]] = {}
    effective_by_node: dict[str, dict[str, Any]] = {}
    observations_by_node: dict[str, list[dict[str, Any]]] = {}
    event_count = 0
    attachment_state_counts: dict[str, int] = {}
    effective_attachment_state_counts: dict[str, int] = {}
    for event in iter_committed_events(runtime_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        event_count += 1
        payload = dict(event.get("payload") or {})
        summary = _event_summary(event)
        summary["node_id"] = str(event.get("node_id") or "")
        summary["payload"] = payload
        summary.update(
            summarize_runtime_liveness_observation(
                payload,
                recorded_at=str(summary.get("recorded_at") or ""),
                now_utc=now_utc,
            )
        )
        attachment_state = str(payload.get("attachment_state") or "").strip().upper()
        if attachment_state:
            attachment_state_counts[attachment_state] = int(attachment_state_counts.get(attachment_state) or 0) + 1
        latest_liveness = summary
        node_id = str(event.get("node_id") or "")
        observations_by_node.setdefault(node_id, []).append(summary)

    for node_id, observations in observations_by_node.items():
        raw_head, effective_head = select_effective_runtime_liveness_head(observations)
        raw_latest_by_node[str(node_id)] = raw_head
        effective_by_node[str(node_id)] = effective_head

    for summary in effective_by_node.values():
        effective_state = str(summary.get("effective_attachment_state") or "").strip().upper()
        if not effective_state:
            continue
        effective_attachment_state_counts[effective_state] = (
            int(effective_attachment_state_counts.get(effective_state) or 0) + 1
        )
    lease_fencing_summary = _lease_fencing_summary(
        runtime_root,
        raw_latest_by_node=raw_latest_by_node,
        effective_by_node=effective_by_node,
    )

    return {
        "runtime_root": str(runtime_root),
        "compatibility_status": "compatible",
        "compatibility_issues": [],
        "committed_seq": latest_committed_seq(runtime_root),
        "runtime_liveness_event_count": event_count,
        "attachment_state_counts": attachment_state_counts,
        "effective_attachment_state_counts": effective_attachment_state_counts,
        "latest_runtime_liveness": latest_liveness,
        "latest_runtime_liveness_by_node": raw_latest_by_node,
        "effective_runtime_liveness_by_node": effective_by_node,
        "lease_fencing_summary": lease_fencing_summary,
    }


def query_commit_state_view(
    state_root: Path,
    *,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Explain the current crash-boundary phase across audit, journal, and projections."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    consistency = query_projection_consistency_view(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    if str(consistency.get("compatibility_status") or "") != "compatible":
        crash_consistency_summary = _crash_consistency_summary(
            compatibility_status=str(consistency.get("compatibility_status") or ""),
            accepted_only_ids=[],
            committed_pending_projection_count=0,
            lagging_node_ids=[],
            visibility_state="incompatible",
            projection_replay_summary=None,
            latest_rebuild_result=None,
        )
        lease_fencing_summary = _lease_fencing_summary_from_rows(
            [],
            compatibility_status=str(consistency.get("compatibility_status") or ""),
        )
        return {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(consistency.get("compatibility_status") or ""),
            "supported_event_schema_version": str(consistency.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(consistency.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(consistency.get("supported_producer_generation") or ""),
            "required_producer_generation": str(consistency.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                consistency.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(consistency.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(consistency.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(consistency.get("journal_meta") or {}),
            "compatibility_issues": list(consistency.get("compatibility_issues") or []),
            "mixed_version_summary": dict(consistency.get("mixed_version_summary") or {}),
            "crash_consistency_summary": crash_consistency_summary,
            "lease_fencing_summary": lease_fencing_summary,
            "phase": "incompatible_journal",
            "committed_seq": None,
            "journal_committed_seq": None,
            "kernel_last_applied_seq": None,
            "accepted_only_count": None,
            "accepted_only_envelope_ids": [],
            "committed_pending_projection_count": None,
            "lagging_node_ids": [],
            "pending_replay": None,
            "projection_replay_summary": None,
            "heavy_object_authority_gap_summary": None,
            "heavy_object_pressure_summary": None,
        }
    committed_seq = int(consistency.get("committed_seq") or 0)
    kernel_projection = dict(consistency.get("kernel_projection") or {})
    node_projections = dict(consistency.get("node_projections") or {})
    accepted_only_ids = sorted(_accepted_audit_envelope_ids(runtime_root) - _mirrored_envelope_ids(runtime_root))
    kernel_last_applied_seq = int(kernel_projection.get("last_applied_seq") or 0)
    lagging_node_ids = [
        str(node_id)
        for node_id, metadata in sorted(node_projections.items())
        if int(dict(metadata or {}).get("last_applied_seq") or 0) < committed_seq
    ]
    committed_pending_projection_count = max(committed_seq - kernel_last_applied_seq, 0)
    heavy_object_authority_gap_summary, heavy_object_pressure_summary = _heavy_object_summary_payloads(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )
    lease_fencing_summary = _lease_fencing_summary(runtime_root)
    phase = _crash_boundary_phase(
        compatibility_status="compatible",
        accepted_only_ids=accepted_only_ids,
        committed_pending_projection_count=committed_pending_projection_count,
        lagging_node_ids=lagging_node_ids,
    )
    crash_consistency_summary = _crash_consistency_summary(
        compatibility_status="compatible",
        accepted_only_ids=accepted_only_ids,
        committed_pending_projection_count=committed_pending_projection_count,
        lagging_node_ids=lagging_node_ids,
        visibility_state=str(consistency.get("visibility_state") or ""),
        projection_replay_summary=dict(consistency.get("projection_replay_summary") or {}),
        latest_rebuild_result=_latest_projection_rebuild_result(runtime_root, rebuild_scope="kernel"),
    )
    return {
        "runtime_root": str(runtime_root),
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(consistency.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(consistency.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(consistency.get("supported_producer_generation") or ""),
        "required_producer_generation": str(consistency.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            consistency.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(consistency.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(consistency.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(consistency.get("journal_meta") or {}),
        "compatibility_issues": [],
        "mixed_version_summary": dict(consistency.get("mixed_version_summary") or {}),
        "crash_consistency_summary": crash_consistency_summary,
        "lease_fencing_summary": lease_fencing_summary,
        "phase": phase,
        "committed_seq": committed_seq,
        "journal_committed_seq": int(consistency.get("journal_committed_seq") or latest_committed_seq(runtime_root)),
        "kernel_last_applied_seq": kernel_last_applied_seq,
        "accepted_only_count": len(accepted_only_ids),
        "accepted_only_envelope_ids": accepted_only_ids,
        "committed_pending_projection_count": committed_pending_projection_count,
        "lagging_node_ids": lagging_node_ids,
        "pending_replay": dict(consistency.get("pending_replay") or {}),
        "projection_replay_summary": dict(consistency.get("projection_replay_summary") or {}),
        "heavy_object_authority_gap_summary": heavy_object_authority_gap_summary,
        "heavy_object_pressure_summary": heavy_object_pressure_summary,
    }


def _query_event_journal_mixed_version_trace_view(
    runtime_root: Path,
    *,
    include_heavy_object_summaries: bool = True,
    event_journal_status: dict[str, Any] | None = None,
    projection_consistency: dict[str, Any] | None = None,
    projection_explanation: dict[str, Any] | None = None,
    projection_replay_trace: dict[str, Any] | None = None,
    commit_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    compatibility = describe_event_journal_compatibility(runtime_root, initialize_if_missing=True)
    mixed_version_summary = _mixed_version_summary(compatibility)
    status = dict(
        event_journal_status
        or query_event_journal_status_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    if str(compatibility.get("compatibility_status") or "") != "compatible":
        return {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(compatibility.get("compatibility_status") or ""),
            "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
            "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                compatibility.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(compatibility.get("journal_meta") or {}),
            "compatibility_issues": list(compatibility.get("compatibility_issues") or []),
            "mixed_version_summary": mixed_version_summary,
            "event_journal_status": status,
            "projection_consistency": None,
            "projection_explanation": None,
            "projection_replay_trace": None,
            "commit_state": None,
        }

    consistency = dict(
        projection_consistency
        or query_projection_consistency_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    explanation = dict(
        projection_explanation
        or query_projection_explanation_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    replay_trace = dict(
        projection_replay_trace
        or _query_projection_replay_trace_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
            projection_consistency=consistency,
            projection_explanation=explanation,
        )
    )
    commit = dict(
        commit_state
        or query_commit_state_view(
            runtime_root,
            include_heavy_object_summaries=include_heavy_object_summaries,
        )
    )
    return {
        "runtime_root": str(runtime_root),
        "compatibility_status": "compatible",
        "supported_event_schema_version": str(compatibility.get("supported_event_schema_version") or ""),
        "supported_store_format_version": str(compatibility.get("supported_store_format_version") or ""),
        "supported_producer_generation": str(compatibility.get("supported_producer_generation") or ""),
        "required_producer_generation": str(compatibility.get("required_producer_generation") or ""),
        "journal_meta_required_producer_generation": str(
            compatibility.get("journal_meta_required_producer_generation") or ""
        ),
        "writer_fencing_status": str(compatibility.get("writer_fencing_status") or ""),
        "writer_fence_alignment_status": str(compatibility.get("writer_fence_alignment_status") or ""),
        "journal_meta": dict(compatibility.get("journal_meta") or {}),
        "compatibility_issues": [],
        "mixed_version_summary": mixed_version_summary,
        "event_journal_status": status,
        "projection_consistency": consistency,
        "projection_explanation": explanation,
        "projection_replay_trace": replay_trace,
        "commit_state": commit,
    }


def query_event_journal_mixed_version_trace_view(
    state_root: Path,
    *,
    include_heavy_object_summaries: bool = True,
) -> dict[str, Any]:
    """Explain whether mixed-version compatibility allows replay/projection truth to be trusted."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    return _query_event_journal_mixed_version_trace_view(
        runtime_root,
        include_heavy_object_summaries=include_heavy_object_summaries,
    )


def _continuation_review_brief(review: dict[str, Any]) -> dict[str, Any]:
    checks = [dict(item or {}) for item in list(review.get("checks") or [])]
    return {
        "decision": str(review.get("decision") or ""),
        "summary": str(review.get("summary") or ""),
        "passed_check_ids": [
            str(dict(item).get("check_id") or "")
            for item in checks
            if bool(dict(item).get("passed"))
        ],
        "failed_check_ids": [
            str(dict(item).get("check_id") or "")
            for item in checks
            if not bool(dict(item).get("passed"))
        ],
    }


def _effective_continuation_runtime_truth(
    node_id: str,
    node_payload: dict[str, Any],
    *,
    effective_liveness_by_node: dict[str, dict[str, Any]],
    allow_expired_attached_heartbeat_snapshot: bool = False,
) -> dict[str, Any]:
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    normalized_node_id = str(node_id or "").strip()
    effective_liveness = dict(effective_liveness_by_node.get(normalized_node_id) or {})
    effective_payload = dict(effective_liveness.get("payload") or {})
    effective_attachment_state = str(
        effective_liveness.get("effective_attachment_state")
        or effective_liveness.get("raw_attachment_state")
        or effective_payload.get("attachment_state")
        or ""
    ).strip() or str(runtime_state.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value)
    effective_observed_at = str(
        effective_liveness.get("observed_at")
        or effective_payload.get("observed_at")
        or ""
    ).strip() or str(runtime_state.get("observed_at") or "")
    effective_observation_kind = str(
        effective_payload.get("observation_kind")
        or effective_liveness.get("observation_kind")
        or ""
    ).strip() or str(runtime_state.get("observation_kind") or "")
    lease_freshness = str(effective_liveness.get("lease_freshness") or "").strip().lower()
    effective_live_process_trusted = bool(runtime_state.get("live_process_trusted"))
    if (
        effective_attachment_state == RuntimeAttachmentState.ATTACHED.value
        and lease_freshness == "fresh"
        and bool(effective_payload.get("pid_alive"))
    ):
        effective_live_process_trusted = True
    if (
        allow_expired_attached_heartbeat_snapshot
        and effective_attachment_state != RuntimeAttachmentState.ATTACHED.value
        and str(
            effective_payload.get("attachment_state")
            or effective_liveness.get("raw_attachment_state")
            or ""
        ).strip()
        == RuntimeAttachmentState.ATTACHED.value
        and bool(effective_payload.get("pid_alive"))
        and str(
            effective_payload.get("observation_kind")
            or effective_liveness.get("observation_kind")
            or ""
        ).strip()
        == "child_supervision_heartbeat"
    ):
        effective_attachment_state = RuntimeAttachmentState.ATTACHED.value
        effective_live_process_trusted = True
    return {
        "attachment_state": effective_attachment_state,
        "observed_at": effective_observed_at,
        "observation_kind": effective_observation_kind,
        "live_process_trusted": effective_live_process_trusted,
        "effective_liveness": effective_liveness,
        "effective_payload": effective_payload,
    }


def _continuation_runtime_loss_signal(
    node_payload: dict[str, Any],
    *,
    runtime_truth: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_state = dict(runtime_truth or normalize_runtime_state(dict(node_payload.get("runtime_state") or {})))
    if str(node_payload.get("status") or "") != "ACTIVE":
        return {}
    attachment_state = str(runtime_state.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value)
    if (
        attachment_state == RuntimeAttachmentState.ATTACHED.value
        and bool(runtime_state.get("live_process_trusted"))
    ):
        return {}
    if attachment_state == RuntimeAttachmentState.LOST.value:
        return {}
    observation_kind = str(runtime_state.get("observation_kind") or "").strip()
    summary = str(runtime_state.get("summary") or "").strip()
    evidence_refs = [str(item) for item in list(runtime_state.get("evidence_refs") or []) if str(item).strip()]
    observed_at = str(runtime_state.get("observed_at") or "")
    if not (observation_kind or summary or evidence_refs or observed_at):
        return {}
    return build_runtime_loss_signal(
        observation_kind=observation_kind or "recovery_rehearsal_inferred_runtime_loss",
        summary=summary or f"read-only recovery rehearsal inferred runtime-loss evidence for {node_payload.get('node_id')}",
        evidence_refs=evidence_refs,
        observed_at=observed_at,
    )


def _build_relaunch_probe(source_node: NodeSpec, *, runtime_loss_signal: dict[str, Any] | None = None) -> TopologyMutation:
    return build_relaunch_request(
        source_node,
        replacement_node_id=f"{source_node.node_id}__recovery_rehearsal_probe",
        reason="read-only real-root recovery continuation rehearsal",
        self_attribution="continuation rehearsal classified the node as unfinished under durable truth",
        self_repair="materialize a fresh replacement child only after authoritative recovery review confirms relaunch is allowed",
        goal_slice=source_node.goal_slice,
        execution_policy=dict(source_node.execution_policy or {}),
        reasoning_profile=dict(source_node.reasoning_profile or {}),
        budget_profile=dict(source_node.budget_profile or {}),
        allowed_actions=list(source_node.allowed_actions or []),
        result_sink_ref=source_node.result_sink_ref,
        lineage_ref=source_node.lineage_ref,
        payload={"runtime_loss_signal": dict(runtime_loss_signal or {})} if runtime_loss_signal else None,
    )


def _continuation_candidates(
    runtime_root: Path,
    *,
    kernel_state: Any,
    fake_live_regressions: list[str] | tuple[str, ...],
    allow_expired_attached_heartbeat_snapshot: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    from loop_product.kernel.state import _dependency_unblocked_resume_candidates

    current_nodes = {
        str(node_id): dict(node or {})
        for node_id, node in dict(getattr(kernel_state, "nodes", {}) or {}).items()
    }
    effective_liveness_by_node = {
        str(node_id): dict(payload or {})
        for node_id, payload in dict(
            query_runtime_liveness_view(runtime_root).get("effective_runtime_liveness_by_node") or {}
        ).items()
    }
    blocked_resume_candidates = set(
        _dependency_unblocked_resume_candidates(
            state_root=runtime_root,
            kernel_state=kernel_state,
        )
    )
    candidates: list[dict[str, Any]] = []
    for node_id, node_payload in sorted(current_nodes.items()):
        if str(node_payload.get("node_kind") or "") == "kernel":
            continue
        status = str(node_payload.get("status") or "")
        if status == "COMPLETED":
            continue
        source_node = NodeSpec.from_dict(node_payload)
        runtime_truth = _effective_continuation_runtime_truth(
            node_id,
            node_payload,
            effective_liveness_by_node=effective_liveness_by_node,
            allow_expired_attached_heartbeat_snapshot=allow_expired_attached_heartbeat_snapshot,
        )
        dependency_statuses = {
            dependency_node_id: str(dict(current_nodes.get(dependency_node_id) or {}).get("status") or "")
            for dependency_node_id in [str(item) for item in list(node_payload.get("depends_on_node_ids") or []) if str(item).strip()]
        }
        review_details: dict[str, Any] = {}
        accepted_action_ids: list[str] = []
        rejected_action_ids: list[str] = []
        continuation_state = "blocked_operator_judgment"

        current_attachment_state = str(
            runtime_truth.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value
        )
        current_live_process_trusted = bool(runtime_truth.get("live_process_trusted"))
        if status == "ACTIVE" and current_attachment_state == RuntimeAttachmentState.ATTACHED.value and current_live_process_trusted:
            continue

        runtime_loss_signal = _continuation_runtime_loss_signal(
            node_payload,
            runtime_truth=runtime_truth,
        )
        if status == "PLANNED":
            activate_review = review_activate_request(
                kernel_state,
                build_activate_request(
                    node_id,
                    reason="read-only r59 continuation rehearsal",
                ),
                state_root=runtime_root,
            )
            review_details["activate_request"] = _continuation_review_brief(activate_review)
            if str(activate_review.get("decision") or "").upper() == "ACCEPT":
                accepted_action_ids.append("activate_request")
                continuation_state = "activation_ready"
            else:
                continuation_state = "dependency_gated" if dependency_statuses else "blocked_operator_judgment"
        elif status == "BLOCKED":
            if node_id in blocked_resume_candidates and "resume_request" in {str(item) for item in list(source_node.allowed_actions or [])}:
                resume_mutation = build_resume_request(
                    source_node,
                    reason="read-only r59 continuation rehearsal",
                    consistency_signal="dependency_results_now_ready_after_blocked_snapshot",
                    payload={
                        "dependency_unblocked": True,
                        "self_attribution": "continuation rehearsal detected that authoritative dependency truth is now terminal-ready",
                        "self_repair": "resume the same logical node only after durable recovery review confirms the stale blocked snapshot can clear",
                    },
                )
                resume_review = review_recovery_request(kernel_state, resume_mutation)
                review_details["resume_request"] = _continuation_review_brief(resume_review)
                if str(resume_review.get("decision") or "").upper() == "ACCEPT":
                    accepted_action_ids.append("resume_request")
            continuation_state = "recovery_candidate" if accepted_action_ids else "blocked_operator_judgment"
        else:
            if "retry_request" in {str(item) for item in list(source_node.allowed_actions or [])}:
                retry_mutation = build_retry_request(
                    source_node,
                    reason="read-only r59 continuation rehearsal",
                    self_attribution="continuation rehearsal classified the node as unfinished under durable truth",
                    self_repair="retry the same logical node only after authoritative recovery review confirms the node can continue in place",
                    payload={"runtime_loss_signal": runtime_loss_signal} if runtime_loss_signal else None,
                )
                retry_review = review_recovery_request(kernel_state, retry_mutation)
                review_details["retry_request"] = _continuation_review_brief(retry_review)
                if str(retry_review.get("decision") or "").upper() == "ACCEPT":
                    accepted_action_ids.append("retry_request")
                else:
                    rejected_action_ids.append("retry_request")
            if "relaunch_request" in {str(item) for item in list(source_node.allowed_actions or [])}:
                relaunch_review = review_recovery_request(
                    kernel_state,
                    _build_relaunch_probe(source_node, runtime_loss_signal=runtime_loss_signal),
                )
                review_details["relaunch_request"] = _continuation_review_brief(relaunch_review)
                if str(relaunch_review.get("decision") or "").upper() == "ACCEPT":
                    accepted_action_ids.append("relaunch_request")
                else:
                    rejected_action_ids.append("relaunch_request")
            continuation_state = "recovery_candidate" if accepted_action_ids else "blocked_operator_judgment"

        preferred_action_id = ""
        for action_id in ("resume_request", "retry_request", "relaunch_request", "activate_request"):
            if action_id in accepted_action_ids:
                preferred_action_id = action_id
                break
        candidates.append(
            {
                "node_id": node_id,
                "node_kind": str(node_payload.get("node_kind") or ""),
                "current_status": status,
                "current_attachment_state": current_attachment_state,
                "current_live_process_trusted": current_live_process_trusted,
                "runtime_observation_kind": str(runtime_truth.get("observation_kind") or ""),
                "runtime_observed_at": str(runtime_truth.get("observed_at") or ""),
                "blocked_reason": str(dict(getattr(kernel_state, "blocked_reasons", {}) or {}).get(node_id) or ""),
                "dependency_statuses": dependency_statuses,
                "continuation_state": continuation_state,
                "accepted_action_ids": accepted_action_ids,
                "rejected_action_ids": rejected_action_ids,
                "preferred_action_id": preferred_action_id,
                "runtime_loss_signal_present": bool(runtime_loss_signal),
                "review_details": review_details,
                "fake_live_regression": bool(node_id in set(str(item) for item in list(fake_live_regressions or []))),
            }
        )

    direct_ids = [
        str(entry.get("node_id") or "")
        for entry in candidates
        if str(entry.get("continuation_state") or "") == "recovery_candidate"
    ]
    dependency_gated_ids = [
        str(entry.get("node_id") or "")
        for entry in candidates
        if str(entry.get("continuation_state") or "") == "dependency_gated"
    ]
    blocked_ids = [
        str(entry.get("node_id") or "")
        for entry in candidates
        if str(entry.get("continuation_state") or "") == "blocked_operator_judgment"
    ]
    activation_ready_ids = [
        str(entry.get("node_id") or "")
        for entry in candidates
        if str(entry.get("continuation_state") or "") == "activation_ready"
    ]
    summary = {
        "unfinished_node_count": len(candidates),
        "direct_recovery_candidate_count": len(direct_ids),
        "direct_recovery_candidate_node_ids": direct_ids,
        "dependency_gated_count": len(dependency_gated_ids),
        "dependency_gated_node_ids": dependency_gated_ids,
        "blocked_operator_judgment_count": len(blocked_ids),
        "blocked_operator_judgment_node_ids": blocked_ids,
        "activation_ready_count": len(activation_ready_ids),
        "activation_ready_node_ids": activation_ready_ids,
        "fake_live_regression_count": len(list(fake_live_regressions or [])),
        "fake_live_regression_visible": bool(list(fake_live_regressions or [])),
    }
    return candidates, summary


def _nested_matrix_state(payload: dict[str, Any], summary_key: str) -> str:
    return str(dict(dict(payload or {}).get(summary_key) or {}).get("matrix_state") or "")


def _r59_continuation_alignment(
    *,
    rehearsal: dict[str, Any],
    event_journal_status: dict[str, Any],
    commit_state: dict[str, Any],
    projection_consistency: dict[str, Any],
    projection_explanation: dict[str, Any],
    mixed_version_trace: dict[str, Any],
    crash_consistency_matrix: dict[str, Any],
    lease_fencing_matrix: dict[str, Any],
) -> tuple[str, list[str]]:
    alignment_issues: list[str] = []
    compatibility_states = {
        "rehearsal": str(rehearsal.get("compatibility_status") or ""),
        "event_journal_status": str(event_journal_status.get("compatibility_status") or ""),
        "commit_state": str(commit_state.get("compatibility_status") or ""),
        "projection_consistency": str(projection_consistency.get("compatibility_status") or ""),
        "projection_explanation": str(projection_explanation.get("compatibility_status") or ""),
        "mixed_version_trace": str(mixed_version_trace.get("compatibility_status") or ""),
        "crash_consistency_matrix": str(crash_consistency_matrix.get("compatibility_status") or ""),
        "lease_fencing_matrix": str(lease_fencing_matrix.get("compatibility_status") or ""),
    }
    unique_compatibility = {value for value in compatibility_states.values() if value}
    if len(unique_compatibility) > 1:
        alignment_issues.append(f"compatibility_status_divergence:{compatibility_states}")

    crash_states = {
        "event_journal_status": _nested_matrix_state(event_journal_status, "crash_consistency_summary"),
        "commit_state": _nested_matrix_state(commit_state, "crash_consistency_summary"),
        "projection_consistency": _nested_matrix_state(projection_consistency, "crash_consistency_summary"),
        "projection_explanation": _nested_matrix_state(projection_explanation, "crash_consistency_summary"),
        "crash_consistency_matrix": _nested_matrix_state(crash_consistency_matrix, "crash_consistency_summary"),
    }
    if len({value for value in crash_states.values() if value}) > 1:
        alignment_issues.append(f"crash_consistency_divergence:{crash_states}")

    lease_states = {
        "event_journal_status": _nested_matrix_state(event_journal_status, "lease_fencing_summary"),
        "commit_state": _nested_matrix_state(commit_state, "lease_fencing_summary"),
        "projection_explanation": _nested_matrix_state(projection_explanation, "lease_fencing_summary"),
        "lease_fencing_matrix": _nested_matrix_state(lease_fencing_matrix, "lease_fencing_summary"),
    }
    if len({value for value in lease_states.values() if value}) > 1:
        alignment_issues.append(f"lease_fencing_divergence:{lease_states}")

    return (
        "generic_surfaces_aligned" if not alignment_issues else "generic_surfaces_divergent",
        alignment_issues,
    )


def _r59_action_safety_state_from_blockers(blockers: list[str]) -> str:
    if any(str(item).startswith("mixed_version_incompatible:") for item in blockers):
        return "blocked_by_incompatibility"
    if any(str(item).startswith("projection_crash_window_open:") for item in blockers):
        return "blocked_by_crash_window"
    if "fake_live_regression_visible" in blockers:
        return "blocked_by_fake_live_regression"
    if "lease_truth_drift_visible" in blockers:
        return "blocked_by_stale_lease"
    return "safe_to_continue_now"


def _r59_dependency_blockers(candidate: dict[str, Any]) -> list[str]:
    dependency_statuses = {
        str(node_id): str(status or "")
        for node_id, status in dict(candidate.get("dependency_statuses") or {}).items()
        if str(node_id).strip()
    }
    unfinished = [
        f"{node_id}={status}"
        for node_id, status in dependency_statuses.items()
        if status not in {"COMPLETED", "FAILED"}
    ]
    if unfinished:
        return [f"dependencies_unfinished:{','.join(unfinished)}"]
    if dependency_statuses:
        return ["dependencies_not_activation_ready"]
    return ["dependencies_not_activation_ready"]


def _r59_operator_judgment_blockers(candidate: dict[str, Any]) -> list[str]:
    blocked_reason = str(candidate.get("blocked_reason") or "").strip()
    if blocked_reason:
        normalized_reason = blocked_reason.lower()
        if "source paper defect" in normalized_reason:
            return [f"source_paper_defect:{blocked_reason}"]
        return [f"operator_judgment_required:{blocked_reason}"]
    rejected = [str(item) for item in list(candidate.get("rejected_action_ids") or []) if str(item).strip()]
    if rejected:
        return [f"kernel_review_rejected:{','.join(rejected)}"]
    return ["operator_judgment_required:no_safe_recovery_path"]


def _r59_action_safety_rows(
    continuation_matrix: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], str, list[str]]:
    continuation_summary = dict(continuation_matrix.get("continuation_summary") or {})
    readiness = str(continuation_matrix.get("continuation_readiness") or "")
    global_blockers = [str(item) for item in list(continuation_matrix.get("readiness_blockers") or []) if str(item).strip()]
    candidates = [dict(item or {}) for item in list(dict(continuation_matrix.get("rehearsal_view") or {}).get("continuation_candidates") or [])]
    rows: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    node_ids_by_state: dict[str, list[str]] = {}

    for candidate in candidates:
        node_id = str(candidate.get("node_id") or "")
        continuation_state = str(candidate.get("continuation_state") or "")
        candidate_action_ids = [str(item) for item in list(candidate.get("accepted_action_ids") or []) if str(item).strip()]
        action_blockers: list[str]
        if continuation_state == "dependency_gated":
            action_state = "blocked_by_dependencies"
            action_blockers = _r59_dependency_blockers(candidate)
            safe_action_ids_now: list[str] = []
        elif continuation_state == "blocked_operator_judgment":
            action_state = "operator_judgment_required"
            action_blockers = _r59_operator_judgment_blockers(candidate)
            safe_action_ids_now = []
        else:
            action_blockers = list(global_blockers)
            action_state = _r59_action_safety_state_from_blockers(action_blockers)
            safe_action_ids_now = candidate_action_ids if action_state == "safe_to_continue_now" else []
        rows.append(
            {
                "node_id": node_id,
                "current_status": str(candidate.get("current_status") or ""),
                "continuation_state": continuation_state,
                "candidate_action_ids": candidate_action_ids,
                "preferred_action_id": str(candidate.get("preferred_action_id") or ""),
                "safe_action_ids_now": safe_action_ids_now,
                "action_safety_state": action_state,
                "action_safety_blockers": action_blockers,
                "dependency_statuses": dict(candidate.get("dependency_statuses") or {}),
                "blocked_reason": str(candidate.get("blocked_reason") or ""),
                "runtime_loss_signal_present": bool(candidate.get("runtime_loss_signal_present")),
                "fake_live_regression": bool(candidate.get("fake_live_regression")),
                "review_details": dict(candidate.get("review_details") or {}),
            }
        )
        counts[action_state] = counts.get(action_state, 0) + 1
        node_ids_by_state.setdefault(action_state, []).append(node_id)

    action_safety_readiness = _r59_action_safety_state_from_blockers(global_blockers)
    if action_safety_readiness == "safe_to_continue_now":
        if counts.get("safe_to_continue_now", 0) > 0:
            action_safety_readiness = "safe_to_continue_now"
        elif int(continuation_summary.get("dependency_gated_count") or 0) > 0:
            action_safety_readiness = "blocked_by_dependencies"
        elif int(continuation_summary.get("blocked_operator_judgment_count") or 0) > 0:
            action_safety_readiness = "operator_judgment_required"
        else:
            action_safety_readiness = "no_actionable_candidates"

    summary = {
        "unfinished_node_count": len(rows),
        "candidate_count": int(continuation_summary.get("direct_recovery_candidate_count") or 0)
        + int(continuation_summary.get("activation_ready_count") or 0),
        "safe_to_continue_now_count": counts.get("safe_to_continue_now", 0),
        "safe_to_continue_now_node_ids": list(node_ids_by_state.get("safe_to_continue_now", [])),
        "blocked_by_crash_window_count": counts.get("blocked_by_crash_window", 0),
        "blocked_by_crash_window_node_ids": list(node_ids_by_state.get("blocked_by_crash_window", [])),
        "blocked_by_stale_lease_count": counts.get("blocked_by_stale_lease", 0),
        "blocked_by_stale_lease_node_ids": list(node_ids_by_state.get("blocked_by_stale_lease", [])),
        "blocked_by_dependencies_count": counts.get("blocked_by_dependencies", 0),
        "blocked_by_dependencies_node_ids": list(node_ids_by_state.get("blocked_by_dependencies", [])),
        "operator_judgment_required_count": counts.get("operator_judgment_required", 0),
        "operator_judgment_required_node_ids": list(node_ids_by_state.get("operator_judgment_required", [])),
        "blocked_by_incompatibility_count": counts.get("blocked_by_incompatibility", 0),
        "blocked_by_incompatibility_node_ids": list(node_ids_by_state.get("blocked_by_incompatibility", [])),
        "blocked_by_fake_live_regression_count": counts.get("blocked_by_fake_live_regression", 0),
        "blocked_by_fake_live_regression_node_ids": list(node_ids_by_state.get("blocked_by_fake_live_regression", [])),
        "global_blocker_count": len(global_blockers),
        "global_blockers": list(global_blockers),
    }
    return rows, summary, action_safety_readiness, global_blockers


def query_real_root_recovery_continuation_matrix_view(
    state_root: Path,
    *,
    checkpoint_ref: str | Path | None = None,
    current_rehearsal_view: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bundle r59 rehearsal continuation truth with generic hardening surfaces."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    rehearsal = dict(current_rehearsal_view or {})
    if not rehearsal:
        rehearsal = query_real_root_recovery_rehearsal_view(runtime_root, checkpoint_ref=checkpoint_ref)
    rehearsal_state = str(rehearsal.get("rehearsal_state") or "")
    slim_generic_surfaces = rehearsal_state in {
        "source_root",
        "isolated_materialized",
        "isolated_rebase_pending",
        "noncanonical_materialization",
    }
    event_journal_status = query_event_journal_status_view(
        runtime_root,
        include_heavy_object_summaries=not slim_generic_surfaces,
    )
    commit_state = query_commit_state_view(
        runtime_root,
        include_heavy_object_summaries=not slim_generic_surfaces,
    )
    projection_consistency = query_projection_consistency_view(
        runtime_root,
        include_heavy_object_summaries=not slim_generic_surfaces,
    )
    if slim_generic_surfaces:
        projection_explanation = _projection_explanation_summary_from_preflight_surfaces(
            runtime_root,
            projection_consistency=projection_consistency,
            commit_state=commit_state,
        )
        mixed_version_trace = {
            "runtime_root": str(runtime_root),
            "compatibility_status": str(event_journal_status.get("compatibility_status") or ""),
            "supported_event_schema_version": str(event_journal_status.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(event_journal_status.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(event_journal_status.get("supported_producer_generation") or ""),
            "required_producer_generation": str(event_journal_status.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                event_journal_status.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(event_journal_status.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(event_journal_status.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(event_journal_status.get("journal_meta") or {}),
            "compatibility_issues": list(event_journal_status.get("compatibility_issues") or []),
            "mixed_version_summary": dict(event_journal_status.get("mixed_version_summary") or {}),
            "event_journal_status": event_journal_status,
            "projection_consistency": projection_consistency,
            "projection_explanation": projection_explanation,
            "projection_replay_trace": None,
            "commit_state": commit_state,
        }
        crash_consistency_matrix = {
            "runtime_root": str(runtime_root),
            "requested_node_id": None,
            "compatibility_status": str(projection_explanation.get("compatibility_status") or ""),
            "supported_event_schema_version": str(projection_explanation.get("supported_event_schema_version") or ""),
            "supported_store_format_version": str(projection_explanation.get("supported_store_format_version") or ""),
            "supported_producer_generation": str(projection_explanation.get("supported_producer_generation") or ""),
            "required_producer_generation": str(projection_explanation.get("required_producer_generation") or ""),
            "journal_meta_required_producer_generation": str(
                projection_explanation.get("journal_meta_required_producer_generation") or ""
            ),
            "writer_fencing_status": str(projection_explanation.get("writer_fencing_status") or ""),
            "writer_fence_alignment_status": str(projection_explanation.get("writer_fence_alignment_status") or ""),
            "journal_meta": dict(projection_explanation.get("journal_meta") or {}),
            "compatibility_issues": list(projection_explanation.get("compatibility_issues") or []),
            "mixed_version_summary": dict(projection_explanation.get("mixed_version_summary") or {}),
            "crash_consistency_summary": dict(projection_explanation.get("crash_consistency_summary") or {}),
            "event_journal_status": event_journal_status,
            "commit_state": commit_state,
            "projection_consistency": projection_consistency,
            "projection_explanation": projection_explanation,
            "projection_replay_trace": None,
        }
        lease_fencing_matrix = {
            "runtime_root": str(runtime_root),
            "requested_node_id": None,
            "compatibility_status": str(commit_state.get("compatibility_status") or ""),
            "compatibility_issues": list(commit_state.get("compatibility_issues") or []),
            "lease_fencing_summary": dict(commit_state.get("lease_fencing_summary") or {}),
            "matrix_rows": [],
            "authority_view": {},
            "runtime_liveness_view": None,
            "event_journal_status": event_journal_status,
            "commit_state": commit_state,
            "projection_explanation": projection_explanation,
        }
    else:
        projection_explanation = query_projection_explanation_view(
            runtime_root,
            include_heavy_object_summaries=True,
        )
        projection_replay_trace = _query_projection_replay_trace_view(
            runtime_root,
            include_heavy_object_summaries=True,
            projection_consistency=projection_consistency,
            projection_explanation=projection_explanation,
        )
        mixed_version_trace = _query_event_journal_mixed_version_trace_view(
            runtime_root,
            include_heavy_object_summaries=True,
            event_journal_status=event_journal_status,
            projection_consistency=projection_consistency,
            projection_explanation=projection_explanation,
            projection_replay_trace=projection_replay_trace,
            commit_state=commit_state,
        )
        crash_consistency_matrix = _query_projection_crash_consistency_matrix_view(
            runtime_root,
            include_heavy_object_summaries=True,
            event_journal_status=event_journal_status,
            commit_state=commit_state,
            projection_consistency=projection_consistency,
            projection_explanation=projection_explanation,
            projection_replay_trace=projection_replay_trace,
        )
        lease_fencing_matrix = _query_lease_fencing_matrix_view(
            runtime_root,
            include_heavy_object_summaries=True,
            event_journal_status=event_journal_status,
            commit_state=commit_state,
            projection_explanation=projection_explanation,
        )
    continuation_summary = dict(rehearsal.get("continuation_summary") or {})
    crash_consistency_summary = dict(crash_consistency_matrix.get("crash_consistency_summary") or {})
    lease_fencing_summary = dict(lease_fencing_matrix.get("lease_fencing_summary") or {})
    mixed_version_summary = dict(mixed_version_trace.get("mixed_version_summary") or {})
    alignment_state, alignment_issues = _r59_continuation_alignment(
        rehearsal=rehearsal,
        event_journal_status=event_journal_status,
        commit_state=commit_state,
        projection_consistency=projection_consistency,
        projection_explanation=projection_explanation,
        mixed_version_trace=mixed_version_trace,
        crash_consistency_matrix=crash_consistency_matrix,
        lease_fencing_matrix=lease_fencing_matrix,
    )

    readiness_blockers: list[str] = []
    if str(mixed_version_trace.get("compatibility_status") or "") != "compatible":
        readiness_blockers.append(
            f"mixed_version_incompatible:{mixed_version_trace.get('compatibility_status') or 'unknown'}"
        )
    if bool(crash_consistency_summary.get("window_open")):
        readiness_blockers.append(
            f"projection_crash_window_open:{str(crash_consistency_summary.get('matrix_state') or '')}"
        )
    if bool(continuation_summary.get("fake_live_regression_visible")):
        readiness_blockers.append("fake_live_regression_visible")
    if str(lease_fencing_summary.get("matrix_state") or "") == "lease_truth_drift_visible":
        readiness_blockers.append("lease_truth_drift_visible")

    if any(item.startswith("mixed_version_incompatible:") for item in readiness_blockers):
        continuation_readiness = "blocked_by_incompatibility"
    elif any(item.startswith("projection_crash_window_open:") for item in readiness_blockers):
        continuation_readiness = "blocked_by_crash_window"
    elif "fake_live_regression_visible" in readiness_blockers:
        continuation_readiness = "blocked_by_fake_live_regression"
    elif int(continuation_summary.get("direct_recovery_candidate_count") or 0) > 0:
        continuation_readiness = "ready_for_operator_recovery"
    elif int(continuation_summary.get("dependency_gated_count") or 0) > 0:
        continuation_readiness = "dependency_gated_only"
    else:
        continuation_readiness = "no_continuation_candidates"

    return {
        "runtime_root": str(runtime_root),
        "checkpoint_ref": str(rehearsal.get("checkpoint_ref") or ""),
        "compatibility_status": str(mixed_version_trace.get("compatibility_status") or ""),
        "generic_surface_mode": "real_root_preflight_slim" if slim_generic_surfaces else "default",
        "alignment_state": alignment_state,
        "alignment_issues": alignment_issues,
        "continuation_readiness": continuation_readiness,
        "readiness_blockers": readiness_blockers,
        "continuation_summary": continuation_summary,
        "mixed_version_summary": mixed_version_summary,
        "crash_consistency_summary": crash_consistency_summary,
        "lease_fencing_summary": lease_fencing_summary,
        "rehearsal_view": rehearsal,
        "event_journal_status": event_journal_status,
        "commit_state": commit_state,
        "projection_consistency": projection_consistency,
        "projection_explanation": projection_explanation,
        "mixed_version_trace": mixed_version_trace,
        "crash_consistency_matrix": crash_consistency_matrix,
        "lease_fencing_matrix": lease_fencing_matrix,
    }


def query_real_root_recovery_action_safety_matrix_view(
    state_root: Path,
    *,
    checkpoint_ref: str | Path | None = None,
    current_continuation_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Bundle staged r59 continuation truth with per-node action-safety classification."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    continuation_matrix = dict(current_continuation_matrix or {})
    if not continuation_matrix:
        continuation_matrix = query_real_root_recovery_continuation_matrix_view(
            runtime_root,
            checkpoint_ref=checkpoint_ref,
        )
    return _query_real_root_recovery_action_safety_matrix_view_from_continuation_matrix(continuation_matrix)


def _query_real_root_recovery_action_safety_matrix_view_from_continuation_matrix(
    continuation_matrix: dict[str, Any],
) -> dict[str, Any]:
    node_action_safety, action_safety_summary, action_safety_readiness, action_safety_blockers = _r59_action_safety_rows(
        continuation_matrix
    )
    return {
        "runtime_root": str(continuation_matrix.get("runtime_root") or ""),
        "checkpoint_ref": str(continuation_matrix.get("checkpoint_ref") or ""),
        "compatibility_status": str(continuation_matrix.get("compatibility_status") or ""),
        "alignment_state": str(continuation_matrix.get("alignment_state") or ""),
        "alignment_issues": list(continuation_matrix.get("alignment_issues") or []),
        "action_safety_readiness": action_safety_readiness,
        "action_safety_blockers": list(action_safety_blockers),
        "action_safety_summary": action_safety_summary,
        "node_action_safety": node_action_safety,
        "continuation_matrix": continuation_matrix,
        "continuation_summary": dict(continuation_matrix.get("continuation_summary") or {}),
        "mixed_version_summary": dict(continuation_matrix.get("mixed_version_summary") or {}),
        "crash_consistency_summary": dict(continuation_matrix.get("crash_consistency_summary") or {}),
        "lease_fencing_summary": dict(continuation_matrix.get("lease_fencing_summary") or {}),
    }


def query_real_root_accepted_audit_mirror_repair_trace_view(
    state_root: Path,
    *,
    checkpoint_ref: str | Path | None = None,
    event_id: str = "",
    current_commit_state: dict[str, Any] | None = None,
    current_action_safety_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Explain the latest accepted-audit mirror repair and its current recovery truth."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_event_id = str(event_id or "").strip()
    if normalized_event_id:
        repair_event = _event_by_id(runtime_root, event_id=normalized_event_id)
        if str(repair_event.get("event_type") or "") != "accepted_audit_mirror_repaired":
            repair_summary: dict[str, Any] = {}
        else:
            repair_summary = _accepted_audit_mirror_repair_event_summary(repair_event)
    else:
        repair_summary = dict(_latest_accepted_audit_mirror_repair_result(runtime_root) or {})
    commit_state = dict(current_commit_state or query_commit_state_view(runtime_root, include_heavy_object_summaries=False))
    action_safety_matrix = dict(current_action_safety_matrix or {})
    continuation_matrix = dict(action_safety_matrix.get("continuation_matrix") or {})
    if not action_safety_matrix:
        continuation_matrix = query_real_root_recovery_continuation_matrix_view(runtime_root, checkpoint_ref=checkpoint_ref)
        action_safety_matrix = _query_real_root_recovery_action_safety_matrix_view_from_continuation_matrix(
            continuation_matrix
        )
    gaps: list[str] = []
    if not repair_summary:
        gaps.append("missing_accepted_audit_mirror_repair_result")
    if int(commit_state.get("accepted_only_count") or 0) > 0:
        gaps.append("accepted_only_gap_still_open")
    gaps = list(dict.fromkeys(gaps))
    return {
        "runtime_root": str(runtime_root),
        "checkpoint_ref": str(repair_summary.get("checkpoint_ref") or checkpoint_ref or ""),
        "event_id": str(repair_summary.get("event_id") or normalized_event_id),
        "repair_result": repair_summary,
        "current_commit_state": commit_state,
        "current_continuation_matrix": continuation_matrix,
        "current_action_safety_matrix": action_safety_matrix,
        "gaps": gaps,
    }


def query_real_root_projection_adoption_repair_trace_view(
    state_root: Path,
    *,
    checkpoint_ref: str | Path | None = None,
    event_id: str = "",
    current_commit_state: dict[str, Any] | None = None,
    current_action_safety_matrix: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Explain the latest historical projection-adoption repair and its current recovery truth."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_event_id = str(event_id or "").strip()
    if normalized_event_id:
        repair_event = _event_by_id(runtime_root, event_id=normalized_event_id)
        if str(repair_event.get("event_type") or "") != "projection_adoption_repaired":
            repair_summary: dict[str, Any] = {}
        else:
            repair_summary = _projection_adoption_repair_event_summary(repair_event)
    else:
        repair_summary = dict(_latest_projection_adoption_repair_result(runtime_root) or {})
    commit_state = dict(current_commit_state or query_commit_state_view(runtime_root, include_heavy_object_summaries=False))
    action_safety_matrix = dict(current_action_safety_matrix or {})
    continuation_matrix = dict(action_safety_matrix.get("continuation_matrix") or {})
    if not action_safety_matrix:
        continuation_matrix = query_real_root_recovery_continuation_matrix_view(runtime_root, checkpoint_ref=checkpoint_ref)
        action_safety_matrix = _query_real_root_recovery_action_safety_matrix_view_from_continuation_matrix(
            continuation_matrix
        )
    gaps: list[str] = []
    if not repair_summary:
        gaps.append("missing_projection_adoption_repair_result")
    if int(commit_state.get("committed_pending_projection_count") or 0) > 0:
        gaps.append("projection_gap_still_open")
    gaps = list(dict.fromkeys(gaps))
    return {
        "runtime_root": str(runtime_root),
        "checkpoint_ref": str(repair_summary.get("checkpoint_ref") or checkpoint_ref or ""),
        "event_id": str(repair_summary.get("event_id") or normalized_event_id),
        "repair_result": repair_summary,
        "current_commit_state": commit_state,
        "current_continuation_matrix": continuation_matrix,
        "current_action_safety_matrix": action_safety_matrix,
        "gaps": gaps,
    }


def query_real_root_repo_service_cleanup_retirement_repair_trace_view(
    state_root: Path,
    *,
    event_id: str = "",
) -> dict[str, Any]:
    """Explain the latest repo-service cleanup-retirement repair and current service blocking truth."""

    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime.cleanup_authority import repo_root_from_runtime_root, runtime_cleanup_retirement_authority
    from loop_product.runtime.control_plane import (
        live_repo_control_plane_runtime,
        live_repo_reactor_residency_guard_runtime,
        live_repo_reactor_runtime,
        repo_control_plane_runtime_root,
        repo_reactor_residency_guard_runtime_root,
        repo_reactor_runtime_root,
    )
    from loop_product.runtime.gc import housekeeping_runtime_root, live_housekeeping_reap_controller_runtime

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    repo_root = repo_root_from_runtime_root(state_root=runtime_root)
    normalized_event_id = str(event_id or "").strip()
    if normalized_event_id:
        repair_event = _event_by_id(runtime_root, event_id=normalized_event_id)
        if str(repair_event.get("event_type") or "") != "repo_service_cleanup_retirement_repaired":
            repair_summary: dict[str, Any] = {}
        else:
            repair_summary = _repo_service_cleanup_retirement_repair_event_summary(repair_event)
    else:
        repair_summary = dict(_latest_repo_service_cleanup_retirement_repair_result(runtime_root) or {})
    current_services: dict[str, dict[str, Any]] = {}
    gaps: list[str] = []
    if repo_root is None:
        gaps.append("missing_repo_root_for_runtime")
    else:
        service_specs = {
            "repo_reactor_residency_guard": (
                repo_reactor_residency_guard_runtime_root(repo_root=repo_root),
                lambda: live_repo_reactor_residency_guard_runtime(repo_root=repo_root),
            ),
            "repo_reactor": (
                repo_reactor_runtime_root(repo_root=repo_root),
                lambda: live_repo_reactor_runtime(repo_root=repo_root),
            ),
            "repo_control_plane": (
                repo_control_plane_runtime_root(repo_root=repo_root),
                lambda: live_repo_control_plane_runtime(repo_root=repo_root),
            ),
            "host_child_launch_supervisor": (
                supervisor_module.host_supervisor_runtime_root(repo_root=repo_root),
                lambda: supervisor_module.live_supervisor_runtime(repo_root=repo_root),
            ),
            "housekeeping_reap_controller": (
                housekeeping_runtime_root(repo_root=repo_root),
                lambda: live_housekeeping_reap_controller_runtime(repo_root=repo_root),
            ),
        }
        for service_kind, (service_root, live_fetch) in service_specs.items():
            authority = runtime_cleanup_retirement_authority(state_root=service_root, repo_root=repo_root)
            live_payload = dict(live_fetch() or {})
            current_services[service_kind] = {
                "state_root": str(service_root),
                "cleanup_blocked": authority is not None,
                "cleanup_authority_event_id": str(dict(authority or {}).get("event_id") or ""),
                "cleanup_authority_event_type": str(dict(authority or {}).get("event_type") or ""),
                "pid": int(live_payload.get("pid") or 0),
                "status": str(live_payload.get("status") or ""),
            }
        if any(bool(dict(item or {}).get("cleanup_blocked")) for item in current_services.values()):
            gaps.append("repo_service_cleanup_block_still_visible")
    if not repair_summary:
        gaps.append("missing_repo_service_cleanup_retirement_repair_result")
    return {
        "runtime_root": str(runtime_root),
        "repo_root": str(repo_root or ""),
        "event_id": str(repair_summary.get("event_id") or normalized_event_id),
        "status": str(repair_summary.get("status") or ""),
        "reseeded_service_count": int(repair_summary.get("reseeded_service_count") or 0),
        "repair_result": repair_summary,
        "current_services": current_services,
        "gaps": list(dict.fromkeys(gaps)),
    }


def query_real_root_recovery_rehearsal_trace_view(
    state_root: Path,
    *,
    envelope_id: str,
    checkpoint_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Explain one accepted truthful real-root recovery rehearsal action."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_envelope_id = str(envelope_id or "").strip()
    accepted_audit = _accepted_audit_envelope(runtime_root, normalized_envelope_id)
    accepted_payload = dict(accepted_audit.get("payload") or {})
    mutation_payload = dict(accepted_payload.get("topology_mutation") or accepted_payload)
    rehearsal_payload = dict(mutation_payload.get("payload") or {})
    action_safety_matrix = _query_real_root_recovery_action_safety_matrix_view_from_continuation_matrix(
        query_real_root_recovery_continuation_matrix_view(
            runtime_root,
            checkpoint_ref=checkpoint_ref,
        )
    )
    topology_trace = (
        query_topology_command_trace_view(runtime_root, envelope_id=normalized_envelope_id)
        if normalized_envelope_id
        else {}
    )
    gaps: list[str] = []
    if not accepted_audit:
        gaps.append("missing_accepted_audit_envelope")
    if str(rehearsal_payload.get("rehearsal_kind") or "") != "real_root_truthful_recovery_rehearsal":
        gaps.append("missing_truthful_recovery_rehearsal_marker")
    if list(topology_trace.get("gaps") or []):
        gaps.append("topology_trace_gaps_present")
    gaps = list(dict.fromkeys(gaps))

    return {
        "runtime_root": str(runtime_root),
        "checkpoint_ref": str(
            rehearsal_payload.get("checkpoint_ref")
            or checkpoint_ref
            or ""
        ),
        "envelope_id": normalized_envelope_id,
        "rehearsal_kind": str(rehearsal_payload.get("rehearsal_kind") or ""),
        "requested_action_id": str(rehearsal_payload.get("requested_action_id") or ""),
        "selected_action_id": str(rehearsal_payload.get("selected_action_id") or ""),
        "preflight_action_safety_state": str(rehearsal_payload.get("preflight_action_safety_state") or ""),
        "preflight_action_safety_readiness": str(rehearsal_payload.get("preflight_action_safety_readiness") or ""),
        "preflight_action_safety_blockers": [
            str(item)
            for item in list(rehearsal_payload.get("preflight_action_safety_blockers") or [])
            if str(item).strip()
        ],
        "preflight_safe_action_ids_now": [
            str(item)
            for item in list(rehearsal_payload.get("preflight_safe_action_ids_now") or [])
            if str(item).strip()
        ],
        "preflight_candidate_action_ids": [
            str(item)
            for item in list(rehearsal_payload.get("preflight_candidate_action_ids") or [])
            if str(item).strip()
        ],
        "preflight_continuation_state": str(rehearsal_payload.get("preflight_continuation_state") or ""),
        "preflight_alignment_state": str(rehearsal_payload.get("preflight_alignment_state") or ""),
        "preflight_compatibility_status": str(rehearsal_payload.get("preflight_compatibility_status") or ""),
        "current_action_safety_readiness": str(action_safety_matrix.get("action_safety_readiness") or ""),
        "current_action_safety_blockers": [
            str(item)
            for item in list(action_safety_matrix.get("action_safety_blockers") or [])
            if str(item).strip()
        ],
        "current_action_safety_summary": dict(action_safety_matrix.get("action_safety_summary") or {}),
        "current_action_safety_matrix": action_safety_matrix,
        "topology_trace": topology_trace,
        "gaps": gaps,
    }


def query_real_root_recovery_rehearsal_view(
    state_root: Path,
    *,
    checkpoint_ref: str | Path | None = None,
    current_rebasing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Compare a staged real-root rehearsal tree against the frozen recovery checkpoint."""

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    checkpoint = load_real_root_recovery_checkpoint(checkpoint_ref=checkpoint_ref)
    source_runtime_root = require_runtime_root(Path(str(checkpoint.get("runtime_root") or "")).expanduser().resolve())
    source_repo_root = Path(str(checkpoint.get("repo_root") or "")).expanduser().resolve()
    current_repo_root = repo_root_from_runtime_root(state_root=runtime_root)
    consistency = query_projection_consistency_view(
        runtime_root,
        include_heavy_object_summaries=False,
    )
    if runtime_root == source_runtime_root:
        rebasing = {
            "runtime_root": str(runtime_root),
            "source_runtime_root": str(source_runtime_root),
            "source_repo_root": str(source_repo_root),
            "remaining_source_runtime_root_refs": 0,
            "remaining_source_repo_root_refs": 0,
            "remaining_source_runtime_root_ref_samples": [],
            "remaining_source_repo_root_ref_samples": [],
            "path_rebase_pending": False,
            "scanned_text_file_count": 0,
            "rebasing_scan_state": "not_applicable_source_root",
        }
    elif current_rebasing:
        rebasing = dict(current_rebasing or {})
        rebasing.setdefault("runtime_root", str(runtime_root))
        rebasing.setdefault("source_runtime_root", str(source_runtime_root))
        rebasing.setdefault("source_repo_root", str(source_repo_root))
        rebasing.setdefault("rebasing_scan_state", "precomputed")
    else:
        rebasing = summarize_real_root_recovery_rebasing(runtime_root=runtime_root, checkpoint_ref=checkpoint_ref)
        rebasing["rebasing_scan_state"] = "performed"
    kernel_state = load_kernel_state(runtime_root)
    current_nodes = {
        str(node_id): dict(node or {})
        for node_id, node in dict(getattr(kernel_state, "nodes", {}) or {}).items()
    }

    node_alignment: dict[str, Any] = {}
    gaps: list[str] = []
    fake_live_regressions: list[str] = []
    mismatched_nodes: list[str] = []
    for node_id, expected in sorted(dict(checkpoint.get("node_snapshots") or {}).items()):
        expected_snapshot = dict(expected or {})
        current = dict(current_nodes.get(str(node_id)) or {})
        runtime_state = normalize_runtime_state(dict(current.get("runtime_state") or {}))
        current_status = str(current.get("status") or "")
        current_attachment_state = str(
            runtime_state.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value
        )
        current_live_process_trusted = bool(runtime_state.get("live_process_trusted"))
        expected_status = str(expected_snapshot.get("status") or "")
        expected_attachment_state = str(
            expected_snapshot.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value
        )
        expected_live_process_present = "live_process_trusted" in expected_snapshot
        expected_live_process_trusted = (
            bool(expected_snapshot.get("live_process_trusted")) if expected_live_process_present else None
        )
        checks = {
            "status": current_status == expected_status,
            "attachment_state": current_attachment_state == expected_attachment_state,
            "live_process_trusted": (
                True
                if not expected_live_process_present
                else current_live_process_trusted == bool(expected_live_process_trusted)
            ),
        }
        matches = bool(current) and all(bool(value) for value in checks.values())
        node_alignment[str(node_id)] = {
            "node_id": str(node_id),
            "expected_status": expected_status,
            "current_status": current_status,
            "expected_attachment_state": expected_attachment_state,
            "current_attachment_state": current_attachment_state,
            "expected_live_process_trusted": expected_live_process_trusted,
            "current_live_process_trusted": current_live_process_trusted,
            "present": bool(current),
            "matches": matches,
            "checks": checks,
        }
        if not current:
            gaps.append(f"checkpoint_node_missing:{node_id}")
            continue
        if not matches:
            mismatched_nodes.append(str(node_id))
            gaps.append(f"checkpoint_node_mismatch:{node_id}")
        if expected_live_process_present and expected_live_process_trusted is False and current_live_process_trusted:
            fake_live_regressions.append(str(node_id))

    isolated_materialization = bool(
        current_repo_root is not None
        and current_repo_root.resolve() != source_repo_root
        and runtime_root != source_runtime_root
    )
    path_rebase_pending = bool(rebasing.get("path_rebase_pending"))
    compatibility_status = str(consistency.get("compatibility_status") or "")
    if isolated_materialization and not path_rebase_pending:
        rehearsal_state = "isolated_materialized"
    elif isolated_materialization:
        rehearsal_state = "isolated_rebase_pending"
    elif runtime_root == source_runtime_root:
        rehearsal_state = "source_root"
    else:
        rehearsal_state = "noncanonical_materialization"
    if compatibility_status != "compatible":
        gaps.append(f"incompatible_journal:{compatibility_status}")
    if fake_live_regressions:
        gaps.extend(f"fake_live_regression:{node_id}" for node_id in fake_live_regressions)

    continuation_candidates, continuation_summary = _continuation_candidates(
        runtime_root,
        kernel_state=kernel_state,
        fake_live_regressions=fake_live_regressions,
        allow_expired_attached_heartbeat_snapshot=isolated_materialization and not path_rebase_pending,
    )
    rehearsal_ready = bool(
        isolated_materialization
        and compatibility_status == "compatible"
        and not path_rebase_pending
        and not fake_live_regressions
        and not mismatched_nodes
    )
    return {
        "runtime_root": str(runtime_root),
        "repo_root": str(current_repo_root) if current_repo_root is not None else "",
        "checkpoint_ref": str(checkpoint.get("checkpoint_ref") or ""),
        "checkpoint_runtime_root": str(source_runtime_root),
        "checkpoint_repo_root": str(source_repo_root),
        "checkpoint_captured_at_local": str(checkpoint.get("captured_at_local") or ""),
        "checkpoint_goal": str(checkpoint.get("checkpoint_goal") or ""),
        "forbidden_regressions": [str(item) for item in list(checkpoint.get("forbidden_regressions") or [])],
        "notes": [str(item) for item in list(checkpoint.get("notes") or [])],
        "checkpoint_accepted_envelope_count": int(checkpoint.get("accepted_envelope_count") or 0),
        "checkpoint_node_count": len(dict(checkpoint.get("node_snapshots") or {})),
        "current_node_count": len(current_nodes),
        "compatibility_status": compatibility_status,
        "projection_visibility_state": str(consistency.get("visibility_state") or ""),
        "projection_committed_seq": int(consistency.get("committed_seq") or 0),
        "journal_committed_seq": int(consistency.get("journal_committed_seq") or 0),
        "rehearsal_state": rehearsal_state,
        "isolated_materialization": isolated_materialization,
        "path_rebase_pending": path_rebase_pending,
        "rehearsal_ready": rehearsal_ready,
        "current_task_id": str(getattr(kernel_state, "task_id", "") or ""),
        "current_root_goal": str(getattr(kernel_state, "root_goal", "") or ""),
        "current_root_node_id": str(getattr(kernel_state, "root_node_id", "") or ""),
        "checkpoint_node_alignment": node_alignment,
        "continuation_summary": continuation_summary,
        "continuation_candidates": continuation_candidates,
        "mismatched_nodes": mismatched_nodes,
        "fake_live_regressions": fake_live_regressions,
        "remaining_source_runtime_root_refs": int(rebasing.get("remaining_source_runtime_root_refs") or 0),
        "remaining_source_repo_root_refs": int(rebasing.get("remaining_source_repo_root_refs") or 0),
        "remaining_source_runtime_root_ref_samples": list(
            rebasing.get("remaining_source_runtime_root_ref_samples") or []
        ),
        "remaining_source_repo_root_ref_samples": list(rebasing.get("remaining_source_repo_root_ref_samples") or []),
        "scanned_text_file_count": int(rebasing.get("scanned_text_file_count") or 0),
        "rebasing_scan_state": str(rebasing.get("rebasing_scan_state") or ""),
        "projection_consistency": consistency,
        "gaps": gaps,
    }
