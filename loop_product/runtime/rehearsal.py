"""Helpers for isolated real-root recovery rehearsals."""

from __future__ import annotations

import json
import sqlite3
import shutil
from pathlib import Path
from typing import Any

from loop_product.runtime.cleanup_authority import repo_root_from_runtime_root
from loop_product.runtime_paths import require_runtime_root


_WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_R59_GOLDEN_RECOVERY_CHECKPOINT_REF = (
    _WORKSPACE_ROOT
    / ".cache"
    / "leanatlas"
    / "tmp"
    / "r59_golden_recovery_checkpoint_20260324T002141.json"
).resolve()
_RECOVERY_REHEARSAL_DURABLE_ENTRIES = (
    "state",
    "event_journal",
    "audit",
)


def default_real_root_recovery_checkpoint_ref() -> Path:
    """Return the canonical frozen checkpoint used for real-root recovery rehearsal."""

    return _DEFAULT_R59_GOLDEN_RECOVERY_CHECKPOINT_REF


def load_real_root_recovery_checkpoint(*, checkpoint_ref: str | Path | None = None) -> dict[str, Any]:
    """Load and normalize the frozen real-root recovery checkpoint."""

    resolved_ref = Path(
        checkpoint_ref if checkpoint_ref not in (None, "") else default_real_root_recovery_checkpoint_ref()
    ).expanduser().resolve()
    payload = json.loads(resolved_ref.read_text(encoding="utf-8"))
    runtime_root = require_runtime_root(Path(str(payload.get("runtime_root") or "")).expanduser().resolve())
    source_repo_root = repo_root_from_runtime_root(state_root=runtime_root)
    if source_repo_root is None:
        raise ValueError(f"unable to derive repo root from checkpoint runtime root {runtime_root}")
    acceptance_intent = dict(payload.get("acceptance_intent") or {})
    node_snapshots = {
        str(node_id): dict(snapshot or {})
        for node_id, snapshot in dict(payload.get("node_snapshots") or {}).items()
    }
    return {
        "checkpoint_ref": str(resolved_ref),
        "runtime_root": str(runtime_root),
        "repo_root": str(source_repo_root),
        "captured_at_local": str(payload.get("captured_at_local") or ""),
        "accepted_envelope_count": int(payload.get("accepted_envelope_count") or 0),
        "notes": [str(item) for item in list(payload.get("notes") or [])],
        "acceptance_intent": acceptance_intent,
        "checkpoint_goal": str(acceptance_intent.get("goal") or ""),
        "forbidden_regressions": [str(item) for item in list(acceptance_intent.get("forbidden_regressions") or [])],
        "node_snapshots": node_snapshots,
    }


def _iter_rehearsal_text_files(tree_root: Path):
    for path in sorted(tree_root.rglob("*")):
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except Exception:
            continue
        yield path, text


def summarize_real_root_recovery_rebasing(
    *,
    runtime_root: str | Path,
    checkpoint_ref: str | Path | None = None,
    sample_limit: int = 8,
) -> dict[str, Any]:
    """Summarize whether a rehearsal tree still leaks source absolute paths."""

    resolved_runtime_root = require_runtime_root(Path(runtime_root).expanduser().resolve())
    checkpoint = load_real_root_recovery_checkpoint(checkpoint_ref=checkpoint_ref)
    source_runtime_root = str(checkpoint.get("runtime_root") or "")
    source_repo_root = str(checkpoint.get("repo_root") or "")
    runtime_ref_count = 0
    repo_ref_count = 0
    runtime_ref_samples: list[str] = []
    repo_ref_samples: list[str] = []
    scanned_text_file_count = 0
    for path, text in _iter_rehearsal_text_files(resolved_runtime_root):
        scanned_text_file_count += 1
        if source_runtime_root and source_runtime_root in text:
            runtime_ref_count += 1
            if len(runtime_ref_samples) < sample_limit:
                runtime_ref_samples.append(str(path))
        if source_repo_root and source_repo_root in text:
            repo_ref_count += 1
            if len(repo_ref_samples) < sample_limit:
                repo_ref_samples.append(str(path))
    return {
        "runtime_root": str(resolved_runtime_root),
        "source_runtime_root": source_runtime_root,
        "source_repo_root": source_repo_root,
        "remaining_source_runtime_root_refs": runtime_ref_count,
        "remaining_source_repo_root_refs": repo_ref_count,
        "remaining_source_runtime_root_ref_samples": runtime_ref_samples,
        "remaining_source_repo_root_ref_samples": repo_ref_samples,
        "path_rebase_pending": bool(runtime_ref_count or repo_ref_count),
        "scanned_text_file_count": scanned_text_file_count,
    }


def _rewrite_rehearsal_paths(
    *,
    runtime_root: Path,
    source_runtime_root: str,
    target_runtime_root: str,
    source_repo_root: str,
    target_repo_root: str,
) -> dict[str, int]:
    runtime_root_replacement_count = 0
    repo_root_replacement_count = 0
    rewritten_text_file_count = 0
    for path, text in _iter_rehearsal_text_files(runtime_root):
        updated = text
        runtime_occurrences = updated.count(source_runtime_root) if source_runtime_root else 0
        if runtime_occurrences:
            updated = updated.replace(source_runtime_root, target_runtime_root)
            runtime_root_replacement_count += runtime_occurrences
        repo_occurrences = updated.count(source_repo_root) if source_repo_root else 0
        if repo_occurrences:
            updated = updated.replace(source_repo_root, target_repo_root)
            repo_root_replacement_count += repo_occurrences
        if updated == text:
            continue
        path.chmod(path.stat().st_mode | 0o200)
        path.write_text(updated, encoding="utf-8")
        rewritten_text_file_count += 1
    return {
        "runtime_root_replacement_count": runtime_root_replacement_count,
        "repo_root_replacement_count": repo_root_replacement_count,
        "rewritten_text_file_count": rewritten_text_file_count,
    }


def _rewrite_event_journal_db(
    *,
    runtime_root: Path,
    source_runtime_root: str,
    target_runtime_root: str,
    source_repo_root: str,
    target_repo_root: str,
) -> None:
    db_ref = runtime_root / "event_journal" / "control_plane.sqlite3"
    if not db_ref.exists():
        return
    conn = sqlite3.connect(str(db_ref))
    try:
        conn.execute(
            """
            UPDATE journal_meta
               SET value_json = ?
             WHERE key = 'runtime_root'
            """,
            (json.dumps(target_runtime_root),),
        )
        conn.execute(
            """
            UPDATE committed_events
               SET runtime_root = REPLACE(runtime_root, ?, ?),
                   payload_json = REPLACE(REPLACE(payload_json, ?, ?), ?, ?),
                   causal_refs_json = REPLACE(REPLACE(causal_refs_json, ?, ?), ?, ?)
            """,
            (
                source_runtime_root,
                target_runtime_root,
                source_runtime_root,
                target_runtime_root,
                source_repo_root,
                target_repo_root,
                source_runtime_root,
                target_runtime_root,
                source_repo_root,
                target_repo_root,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _runtime_state_from_checkpoint_snapshot(
    *,
    snapshot: dict[str, Any],
    current_runtime_state: dict[str, Any],
) -> dict[str, Any]:
    from loop_product.protocols.node import RuntimeAttachmentState, normalize_runtime_state

    normalized = normalize_runtime_state(dict(current_runtime_state or {}))
    expected_attachment_state = str(
        snapshot.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value
    ).strip().upper()
    normalized["attachment_state"] = RuntimeAttachmentState(expected_attachment_state).value
    if "live_process_trusted" in snapshot:
        normalized["live_process_trusted"] = bool(snapshot.get("live_process_trusted"))
    if normalized["attachment_state"] == RuntimeAttachmentState.UNOBSERVED.value:
        normalized["observed_at"] = ""
        normalized["summary"] = ""
        normalized["observation_kind"] = ""
        normalized["evidence_refs"] = []
        normalized["live_process_trusted"] = bool(snapshot.get("live_process_trusted"))
        for key in (
            "launch_event_id",
            "pid",
            "pid_alive",
            "lease_epoch",
            "lease_owner_id",
            "lease_freshness",
            "lease_source",
            "lease_duration_s",
            "lease_expires_at",
            "status_result_ref",
            "runtime_ref",
            "process_fingerprint",
            "process_started_at_utc",
        ):
            normalized.pop(key, None)
    return normalized


def _reapply_checkpoint_node_snapshots(
    *,
    runtime_root: Path,
    checkpoint: dict[str, Any],
) -> None:
    from loop_product.event_journal import latest_projection_committed_seq
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import (
        load_kernel_state,
        persist_kernel_state,
        persist_node_snapshot,
        persist_projection_manifest,
    )
    from loop_product.protocols.node import NodeStatus

    kernel_state = load_kernel_state(runtime_root)
    dirty = False
    changed_node_ids: list[str] = []
    checkpoint_snapshots = {
        str(node_id): dict(snapshot or {})
        for node_id, snapshot in dict(checkpoint.get("node_snapshots") or {}).items()
    }
    for node_id, snapshot in checkpoint_snapshots.items():
        current_node = dict(kernel_state.nodes.get(node_id) or {})
        if not current_node:
            continue
        expected_status = str(snapshot.get("status") or current_node.get("status") or "").strip()
        updated_node = dict(current_node)
        if expected_status and str(updated_node.get("status") or "") != expected_status:
            updated_node["status"] = expected_status
        updated_runtime_state = _runtime_state_from_checkpoint_snapshot(
            snapshot=snapshot,
            current_runtime_state=dict(updated_node.get("runtime_state") or {}),
        )
        if updated_runtime_state != dict(updated_node.get("runtime_state") or {}):
            updated_node["runtime_state"] = updated_runtime_state
        if updated_node == current_node:
            continue
        kernel_state.nodes[node_id] = updated_node
        dirty = True
        changed_node_ids.append(node_id)
        if expected_status and expected_status != NodeStatus.BLOCKED.value:
            if kernel_state.blocked_reasons.pop(node_id, None) is not None:
                dirty = True

    if not dirty:
        return

    projection_seq = int(latest_projection_committed_seq(runtime_root))
    authority = kernel_internal_authority()
    persist_kernel_state(runtime_root, kernel_state, authority=authority, projection_seq=projection_seq)
    for node_id in changed_node_ids:
        persist_node_snapshot(
            runtime_root,
            kernel_state.nodes[node_id],
            authority=authority,
            projection_seq=projection_seq,
        )
    persist_projection_manifest(
        runtime_root,
        kernel_state,
        authority=authority,
        projection_seq=projection_seq,
    )


def materialize_real_root_recovery_rehearsal(
    *,
    repo_root: str | Path,
    checkpoint_ref: str | Path | None = None,
    runtime_name: str = "",
    include_rebasing_summary: bool = True,
) -> dict[str, Any]:
    """Copy the frozen real-root checkpoint into an isolated repo root and rebase absolute paths."""

    checkpoint = load_real_root_recovery_checkpoint(checkpoint_ref=checkpoint_ref)
    source_runtime_root = require_runtime_root(Path(str(checkpoint.get("runtime_root") or "")).expanduser().resolve())
    source_repo_root = Path(str(checkpoint.get("repo_root") or "")).expanduser().resolve()
    resolved_repo_root = Path(repo_root).expanduser().resolve()
    target_runtime_name = str(runtime_name or source_runtime_root.name).strip()
    if not target_runtime_name:
        raise ValueError("runtime_name must not be empty")
    target_runtime_root = (resolved_repo_root / ".loop" / target_runtime_name).resolve()
    if target_runtime_root.exists():
        raise FileExistsError(f"target rehearsal runtime root already exists: {target_runtime_root}")
    target_runtime_root.mkdir(parents=True, exist_ok=False)
    copied_entries: list[str] = []
    for entry_name in _RECOVERY_REHEARSAL_DURABLE_ENTRIES:
        source_entry = source_runtime_root / entry_name
        if not source_entry.exists():
            continue
        target_entry = target_runtime_root / entry_name
        if source_entry.is_dir():
            shutil.copytree(source_entry, target_entry)
        else:
            target_entry.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_entry, target_entry)
        copied_entries.append(entry_name)
    _rewrite_event_journal_db(
        runtime_root=target_runtime_root,
        source_runtime_root=str(source_runtime_root),
        target_runtime_root=str(target_runtime_root),
        source_repo_root=str(source_repo_root),
        target_repo_root=str(resolved_repo_root),
    )
    rewrite_counts = _rewrite_rehearsal_paths(
        runtime_root=target_runtime_root,
        source_runtime_root=str(source_runtime_root),
        target_runtime_root=str(target_runtime_root),
        source_repo_root=str(source_repo_root),
        target_repo_root=str(resolved_repo_root),
    )
    _reapply_checkpoint_node_snapshots(
        runtime_root=target_runtime_root,
        checkpoint=checkpoint,
    )
    receipt = {
        "checkpoint_ref": str(checkpoint.get("checkpoint_ref") or ""),
        "source_runtime_root": str(source_runtime_root),
        "source_repo_root": str(source_repo_root),
        "runtime_root": str(target_runtime_root),
        "repo_root": str(resolved_repo_root),
        "copied_entries": copied_entries,
        **rewrite_counts,
    }
    if include_rebasing_summary:
        rebasing = summarize_real_root_recovery_rebasing(
            runtime_root=target_runtime_root,
            checkpoint_ref=checkpoint_ref,
        )
        receipt.update(rebasing)
    return receipt


def _normalized_rehearsal_checkpoint_ref(checkpoint_ref: str | Path | None) -> str:
    checkpoint = load_real_root_recovery_checkpoint(checkpoint_ref=checkpoint_ref)
    return str(checkpoint.get("checkpoint_ref") or "")


def _accepted_audit_repair_candidates(runtime_root: Path) -> dict[str, Any]:
    accepted_by_envelope_id: dict[str, dict[str, Any]] = {}
    duplicate_envelope_ids: list[str] = []
    audit_dir = runtime_root / "audit"
    if not audit_dir.exists():
        return {
            "accepted_by_envelope_id": accepted_by_envelope_id,
            "duplicate_envelope_id_count": 0,
            "duplicate_envelope_ids_sample": [],
        }
    counts: dict[str, int] = {}
    for path in sorted(audit_dir.glob("envelope_*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("status") or "").strip().lower() != "accepted":
            continue
        envelope_id = str(payload.get("envelope_id") or "").strip()
        if not envelope_id:
            continue
        counts[envelope_id] = counts.get(envelope_id, 0) + 1
        accepted_by_envelope_id[envelope_id] = dict(payload or {})
    duplicate_envelope_ids = [envelope_id for envelope_id, count in sorted(counts.items()) if count > 1]
    return {
        "accepted_by_envelope_id": accepted_by_envelope_id,
        "duplicate_envelope_id_count": len(duplicate_envelope_ids),
        "duplicate_envelope_ids_sample": duplicate_envelope_ids[:8],
    }


def _mirrored_control_envelope_ids(runtime_root: Path) -> set[str]:
    from loop_product.kernel import query as query_module

    return set(query_module._mirrored_envelope_ids(runtime_root))


def _journal_committed_seq_via_query(state_root: Path) -> int:
    from loop_product.kernel import query_commit_state_view

    commit_state = query_commit_state_view(state_root, include_heavy_object_summaries=False)
    return int(commit_state.get("journal_committed_seq") or 0)


def _candidate_row_by_node(action_safety_matrix: dict[str, Any], *, node_id: str) -> dict[str, Any]:
    normalized_node_id = str(node_id or "").strip()
    for row in list(action_safety_matrix.get("node_action_safety") or []):
        payload = dict(row or {})
        if str(payload.get("node_id") or "") == normalized_node_id:
            return payload
    return {}


def _continuation_candidate_by_node(action_safety_matrix: dict[str, Any], *, node_id: str) -> dict[str, Any]:
    normalized_node_id = str(node_id or "").strip()
    rehearsal_view = dict(dict(action_safety_matrix.get("continuation_matrix") or {}).get("rehearsal_view") or {})
    for candidate in list(rehearsal_view.get("continuation_candidates") or []):
        payload = dict(candidate or {})
        if str(payload.get("node_id") or "") == normalized_node_id:
            return payload
    return {}


def _rehearsal_runtime_loss_signal(node_payload: dict[str, Any]) -> dict[str, Any]:
    from loop_product.protocols.node import RuntimeAttachmentState, normalize_runtime_state
    from loop_product.runtime import build_runtime_loss_signal

    if str(node_payload.get("status") or "") != "ACTIVE":
        return {}
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    if str(runtime_state.get("attachment_state") or "") == RuntimeAttachmentState.LOST.value:
        return {}
    observation_kind = str(runtime_state.get("observation_kind") or "").strip()
    summary = str(runtime_state.get("summary") or "").strip()
    evidence_refs = [str(item) for item in list(runtime_state.get("evidence_refs") or []) if str(item).strip()]
    observed_at = str(runtime_state.get("observed_at") or "").strip()
    if not (observation_kind or summary or evidence_refs or observed_at):
        return {}
    return build_runtime_loss_signal(
        observation_kind=observation_kind or "real_root_recovery_rehearsal_runtime_loss",
        summary=summary or f"truthful recovery rehearsal inferred runtime-loss evidence for {node_payload.get('node_id')}",
        evidence_refs=evidence_refs,
        observed_at=observed_at,
    )


def _next_rehearsal_relaunch_node_id(*, runtime_root: Path, source_node_id: str) -> str:
    from loop_product.kernel.state import load_kernel_state

    kernel_state = load_kernel_state(runtime_root)
    existing = {str(node_id) for node_id in dict(getattr(kernel_state, "nodes", {}) or {}).keys()}
    base = f"{source_node_id}__r59_truthful_recovery"
    if base not in existing:
        return base
    suffix = 2
    while f"{base}_{suffix}" in existing:
        suffix += 1
    return f"{base}_{suffix}"


def execute_real_root_recovery_rehearsal(
    state_root: str | Path,
    *,
    checkpoint_ref: str | Path | None = None,
    node_id: str,
    action_id: str = "",
    preflight_action_safety_matrix: dict[str, Any] | None = None,
    include_trace: bool = True,
) -> dict[str, Any]:
    """Run one truthful recovery rehearsal action on an isolated staged real-root."""

    from loop_product.kernel import query_real_root_recovery_action_safety_matrix_view
    from loop_product.kernel.submit import submit_topology_mutation
    from loop_product.kernel.state import load_kernel_state
    from loop_product.protocols.control_envelope import EnvelopeStatus
    from loop_product.protocols.node import NodeSpec
    from loop_product.runtime.recover import build_relaunch_request, build_resume_request, build_retry_request
    from loop_product.topology.activate import build_activate_request

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_node_id = str(node_id or "").strip()
    requested_action_id = str(action_id or "").strip()
    normalized_checkpoint_ref = _normalized_rehearsal_checkpoint_ref(checkpoint_ref)
    if not normalized_node_id:
        raise ValueError("node_id must be non-empty")

    action_safety_matrix = dict(preflight_action_safety_matrix or {})
    if not action_safety_matrix:
        action_safety_matrix = query_real_root_recovery_action_safety_matrix_view(
            runtime_root,
            checkpoint_ref=normalized_checkpoint_ref,
        )
    action_row = _candidate_row_by_node(action_safety_matrix, node_id=normalized_node_id)
    continuation_candidate = _continuation_candidate_by_node(action_safety_matrix, node_id=normalized_node_id)
    safe_action_ids_now = [str(item) for item in list(action_row.get("safe_action_ids_now") or []) if str(item).strip()]
    candidate_action_ids = [str(item) for item in list(action_row.get("candidate_action_ids") or []) if str(item).strip()]
    preferred_action_id = str(action_row.get("preferred_action_id") or "").strip()
    selected_action_id = requested_action_id or preferred_action_id

    receipt = {
        "runtime_root": str(runtime_root),
        "checkpoint_ref": normalized_checkpoint_ref,
        "node_id": normalized_node_id,
        "requested_action_id": requested_action_id,
        "selected_action_id": "",
        "preferred_action_id": preferred_action_id,
        "candidate_action_ids": candidate_action_ids,
        "safe_action_ids_now": safe_action_ids_now,
        "compatibility_status": str(action_safety_matrix.get("compatibility_status") or ""),
        "alignment_state": str(action_safety_matrix.get("alignment_state") or ""),
        "action_safety_readiness": str(action_safety_matrix.get("action_safety_readiness") or ""),
        "action_safety_state": str(action_row.get("action_safety_state") or ""),
        "action_safety_blockers": [str(item) for item in list(action_row.get("action_safety_blockers") or []) if str(item).strip()],
        "continuation_state": str(continuation_candidate.get("continuation_state") or ""),
        "recovery_executed": False,
        "status": "blocked",
        "envelope_id": "",
        "command_kind": "",
        "topology_trace": {},
    }

    if not action_row or not continuation_candidate:
        receipt["status"] = "unknown_node"
        return receipt
    if not selected_action_id:
        receipt["status"] = "blocked"
        return receipt
    if selected_action_id not in safe_action_ids_now:
        receipt["status"] = "blocked"
        return receipt

    kernel_state = load_kernel_state(runtime_root)
    source_payload = dict(getattr(kernel_state, "nodes", {}).get(normalized_node_id) or {})
    if not source_payload:
        receipt["status"] = "unknown_node"
        return receipt
    source_node = NodeSpec.from_dict(source_payload)
    rehearsal_payload = {
        "rehearsal_kind": "real_root_truthful_recovery_rehearsal",
        "checkpoint_ref": normalized_checkpoint_ref,
        "requested_action_id": requested_action_id or selected_action_id,
        "selected_action_id": selected_action_id,
        "preflight_action_safety_state": str(action_row.get("action_safety_state") or ""),
        "preflight_action_safety_blockers": list(receipt["action_safety_blockers"]),
        "preflight_safe_action_ids_now": list(safe_action_ids_now),
        "preflight_candidate_action_ids": list(candidate_action_ids),
        "preflight_continuation_state": str(continuation_candidate.get("continuation_state") or ""),
        "preflight_alignment_state": str(action_safety_matrix.get("alignment_state") or ""),
        "preflight_action_safety_readiness": str(action_safety_matrix.get("action_safety_readiness") or ""),
        "preflight_compatibility_status": str(action_safety_matrix.get("compatibility_status") or ""),
    }
    runtime_loss_signal = _rehearsal_runtime_loss_signal(source_payload)
    mutation = None
    if selected_action_id == "activate_request":
        mutation = build_activate_request(
            normalized_node_id,
            reason="truthful real-root recovery rehearsal on an isolated staged root",
            payload=dict(rehearsal_payload),
        )
    elif selected_action_id == "resume_request":
        mutation = build_resume_request(
            source_node,
            reason="truthful real-root recovery rehearsal on an isolated staged root",
            consistency_signal="real_root_recovery_rehearsal_safe_to_continue_now",
            payload={
                **rehearsal_payload,
                "self_attribution": "truthful recovery rehearsal preflight cleared every generic blocker on the staged root",
                "self_repair": "resume the same logical node only on the isolated rehearsal root after safety truth reports safe_to_continue_now",
            },
        )
    elif selected_action_id == "retry_request":
        retry_payload = dict(rehearsal_payload)
        if runtime_loss_signal:
            retry_payload["runtime_loss_signal"] = dict(runtime_loss_signal)
        mutation = build_retry_request(
            source_node,
            reason="truthful real-root recovery rehearsal on an isolated staged root",
            self_attribution="truthful recovery rehearsal preflight cleared every generic blocker on the staged root",
            self_repair="retry the same logical node only on the isolated rehearsal root after safety truth reports safe_to_continue_now",
            payload=retry_payload,
        )
    elif selected_action_id == "relaunch_request":
        relaunch_payload = dict(rehearsal_payload)
        if runtime_loss_signal:
            relaunch_payload["runtime_loss_signal"] = dict(runtime_loss_signal)
        mutation = build_relaunch_request(
            source_node,
            replacement_node_id=_next_rehearsal_relaunch_node_id(
                runtime_root=runtime_root,
                source_node_id=normalized_node_id,
            ),
            reason="truthful real-root recovery rehearsal on an isolated staged root",
            self_attribution="truthful recovery rehearsal preflight cleared every generic blocker on the staged root",
            self_repair="materialize a fresh replacement child only on the isolated rehearsal root after safety truth reports safe_to_continue_now",
            goal_slice=source_node.goal_slice,
            execution_policy=dict(source_node.execution_policy or {}),
            reasoning_profile=dict(source_node.reasoning_profile or {}),
            budget_profile=dict(source_node.budget_profile or {}),
            allowed_actions=list(source_node.allowed_actions or []),
            result_sink_ref=source_node.result_sink_ref,
            lineage_ref=source_node.lineage_ref,
            payload=relaunch_payload,
        )
    else:
        return receipt

    accepted = submit_topology_mutation(
        runtime_root,
        mutation,
        round_id=source_node.round_id,
        generation=source_node.generation,
    )
    receipt["selected_action_id"] = selected_action_id
    receipt["status"] = str(accepted.status.value or "")
    receipt["envelope_id"] = str(accepted.envelope_id or "")
    receipt["command_kind"] = str(mutation.kind or "")
    receipt["recovery_executed"] = accepted.status is EnvelopeStatus.ACCEPTED
    if accepted.status is EnvelopeStatus.ACCEPTED and include_trace:
        from loop_product.kernel import query_real_root_recovery_rehearsal_trace_view

        receipt["topology_trace"] = query_real_root_recovery_rehearsal_trace_view(
            runtime_root,
            checkpoint_ref=normalized_checkpoint_ref,
            envelope_id=str(accepted.envelope_id or ""),
        )
    return receipt


def repair_real_root_accepted_audit_mirror(
    state_root: str | Path,
    *,
    checkpoint_ref: str | Path | None = None,
    include_post_recovery_truth: bool = True,
) -> dict[str, Any]:
    """Mirror accepted audit envelopes into committed truth for a historical recovery root."""

    from loop_product.event_journal import commit_accepted_audit_mirror_repaired_event
    from loop_product.event_journal import mirror_accepted_control_envelope_event
    from loop_product.kernel import (
        query_commit_state_view,
        query_real_root_recovery_action_safety_matrix_view,
        query_real_root_recovery_continuation_matrix_view,
    )
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import rebuild_kernel_projections
    from loop_product.protocols.control_envelope import ControlEnvelope

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_checkpoint_ref = (
        _normalized_rehearsal_checkpoint_ref(checkpoint_ref) if checkpoint_ref not in (None, "") else ""
    )
    before_commit = query_commit_state_view(runtime_root, include_heavy_object_summaries=False)
    candidates = _accepted_audit_repair_candidates(runtime_root)
    accepted_by_envelope_id = dict(candidates.get("accepted_by_envelope_id") or {})
    duplicate_envelope_id_count = int(candidates.get("duplicate_envelope_id_count") or 0)
    duplicate_envelope_ids_sample = [
        str(item) for item in list(candidates.get("duplicate_envelope_ids_sample") or []) if str(item).strip()
    ]
    mirrored_before_ids = _mirrored_control_envelope_ids(runtime_root)
    mirrored_envelope_ids: list[str] = []
    for envelope_id, payload in sorted(accepted_by_envelope_id.items()):
        if envelope_id in mirrored_before_ids:
            continue
        event = mirror_accepted_control_envelope_event(runtime_root, ControlEnvelope.from_dict(dict(payload or {})))
        if event is not None:
            mirrored_envelope_ids.append(envelope_id)

    rebuild_summary: dict[str, Any] = {}
    if mirrored_envelope_ids:
        rebuild_summary = rebuild_kernel_projections(runtime_root, authority=kernel_internal_authority())

    after_commit = query_commit_state_view(runtime_root, include_heavy_object_summaries=False)
    continuation_matrix: dict[str, Any] = {}
    action_safety_matrix: dict[str, Any] = {}
    if normalized_checkpoint_ref and include_post_recovery_truth:
        continuation_matrix = query_real_root_recovery_continuation_matrix_view(
            runtime_root,
            checkpoint_ref=normalized_checkpoint_ref,
        )
        action_safety_matrix = query_real_root_recovery_action_safety_matrix_view(
            runtime_root,
            checkpoint_ref=normalized_checkpoint_ref,
        )
    accepted_only_after = int(after_commit.get("accepted_only_count") or 0)
    if mirrored_envelope_ids and accepted_only_after == 0:
        status = "repaired"
    elif mirrored_envelope_ids:
        status = "partial"
    elif accepted_only_after == 0:
        status = "noop"
    else:
        status = "blocked"

    receipt = {
        "repair_kind": "accepted_audit_mirror_repair",
        "runtime_root": str(runtime_root),
        "checkpoint_ref": normalized_checkpoint_ref,
        "status": status,
        "accepted_audit_unique_envelope_count": len(accepted_by_envelope_id),
        "duplicate_envelope_id_count": duplicate_envelope_id_count,
        "duplicate_envelope_ids_sample": duplicate_envelope_ids_sample,
        "already_mirrored_count": len(set(accepted_by_envelope_id) & mirrored_before_ids),
        "mirrored_envelope_count": len(mirrored_envelope_ids),
        "mirrored_envelope_ids_sample": mirrored_envelope_ids[:8],
        "accepted_only_count_before": int(before_commit.get("accepted_only_count") or 0),
        "accepted_only_count_after": accepted_only_after,
        "accepted_only_gap_cleared": int(before_commit.get("accepted_only_count") or 0) > 0 and accepted_only_after == 0,
        "commit_phase_before": str(before_commit.get("phase") or ""),
        "commit_phase_after": str(after_commit.get("phase") or ""),
        "crash_matrix_state_before": str(
            dict(before_commit.get("crash_consistency_summary") or {}).get("matrix_state") or ""
        ),
        "crash_matrix_state_after": str(
            dict(after_commit.get("crash_consistency_summary") or {}).get("matrix_state") or ""
        ),
        "journal_committed_seq_before": int(before_commit.get("journal_committed_seq") or 0),
        "journal_committed_seq_after": int(after_commit.get("journal_committed_seq") or 0),
        "repair_result_event_seq_before": int(after_commit.get("journal_committed_seq") or 0),
        "rebuild_status": str(rebuild_summary.get("status") or ""),
        "rebuild_projection_state": str(rebuild_summary.get("projection_state") or ""),
        "rebuild_visibility_state": str(rebuild_summary.get("visibility_state") or ""),
        "rebuild_blocking_reason": str(rebuild_summary.get("blocking_reason") or ""),
        "rebuild_result_event_id": str(rebuild_summary.get("rebuild_result_event_id") or ""),
        "continuation_readiness_after": str(continuation_matrix.get("continuation_readiness") or ""),
        "action_safety_readiness_after": str(action_safety_matrix.get("action_safety_readiness") or ""),
        "action_safety_blockers_after": [
            str(item) for item in list(action_safety_matrix.get("action_safety_blockers") or []) if str(item).strip()
        ],
    }
    repair_event = commit_accepted_audit_mirror_repaired_event(
        runtime_root,
        receipt=receipt,
        producer="runtime.rehearsal.accepted_audit_mirror_repair",
    )
    receipt["repair_result_event_id"] = str(repair_event.get("event_id") or "")
    receipt["repair_result_event_seq"] = int(repair_event.get("seq") or 0)
    return receipt


def repair_real_root_projection_adoption(
    state_root: str | Path,
    *,
    checkpoint_ref: str | Path | None = None,
    include_pre_repair_truth: bool = True,
    include_post_repair_truth: bool = True,
) -> dict[str, Any]:
    """Adopt missing accepted-envelope history into a preserved real-root projection bundle."""

    from loop_product.event_journal import commit_projection_adoption_repaired_event
    from loop_product.kernel import (
        query_commit_state_view,
        query_projection_crash_consistency_matrix_view,
    )
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import adopt_historical_projection_bundle

    runtime_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_checkpoint_ref = (
        _normalized_rehearsal_checkpoint_ref(checkpoint_ref) if checkpoint_ref not in (None, "") else ""
    )
    before_commit: dict[str, Any] = {}
    before_projection: dict[str, Any] = {}
    if include_pre_repair_truth:
        before_commit = query_commit_state_view(runtime_root, include_heavy_object_summaries=False)
        before_projection = query_projection_crash_consistency_matrix_view(
            runtime_root,
            include_heavy_object_summaries=False,
        )
    before_crash_summary = dict(before_projection.get("crash_consistency_summary") or {})

    adoption_summary = adopt_historical_projection_bundle(
        runtime_root,
        authority=kernel_internal_authority(),
    )
    after_commit: dict[str, Any] = {}
    after_projection: dict[str, Any] = {}
    if include_post_repair_truth:
        after_commit = query_commit_state_view(runtime_root, include_heavy_object_summaries=False)
        after_projection = query_projection_crash_consistency_matrix_view(
            runtime_root,
            include_heavy_object_summaries=False,
        )
    after_crash_summary = dict(after_projection.get("crash_consistency_summary") or {})

    missing_before = int(adoption_summary.get("missing_accepted_envelope_count_before") or 0)
    missing_after = int(adoption_summary.get("missing_accepted_envelope_count_after") or 0)
    adopted_count = int(adoption_summary.get("adopted_envelope_count") or 0)
    if include_post_repair_truth:
        if adopted_count > 0 and str(after_commit.get("phase") or "") == "projected":
            status = "repaired"
        elif adopted_count > 0:
            status = "partial"
        elif missing_before == 0 and str(after_commit.get("phase") or "") == "projected":
            status = "noop"
        else:
            status = "blocked"
    else:
        if adopted_count > 0 and missing_after == 0:
            status = "repaired"
        elif adopted_count > 0:
            status = "partial"
        elif missing_before == 0 and missing_after == 0:
            status = "noop"
        else:
            status = "blocked"

    receipt = {
        "repair_kind": "projection_adoption_repair",
        "runtime_root": str(runtime_root),
        "checkpoint_ref": normalized_checkpoint_ref,
        "status": status,
        "missing_accepted_envelope_count_before": missing_before,
        "missing_accepted_envelope_count_after": missing_after,
        "adopted_envelope_count": adopted_count,
        "adopted_envelope_ids_sample": [
            str(item) for item in list(adoption_summary.get("adopted_envelope_ids_sample") or []) if str(item).strip()
        ],
        "projection_gap_cleared": missing_before > 0 and missing_after == 0 and str(after_commit.get("phase") or "") == "projected",
        "commit_phase_before": str(before_commit.get("phase") or ""),
        "commit_phase_after": str(after_commit.get("phase") or ""),
        "crash_matrix_state_before": str(before_crash_summary.get("matrix_state") or ""),
        "crash_matrix_state_after": str(after_crash_summary.get("matrix_state") or ""),
        "journal_committed_seq_before": int(before_commit.get("journal_committed_seq") or 0),
        "journal_committed_seq_after": int(after_commit.get("journal_committed_seq") or 0),
        "projection_committed_seq_before": int(before_commit.get("kernel_last_applied_seq") or 0),
        "projection_committed_seq_after": int(after_commit.get("kernel_last_applied_seq") or 0),
        "projection_visibility_state_after": str(after_crash_summary.get("visibility_state") or ""),
        "replay_mode_after": str(after_crash_summary.get("replay_mode") or ""),
        "continuation_readiness_after": "",
        "action_safety_readiness_after": "",
        "action_safety_blockers_after": [],
        "accepted_audit_mirror_repair_event_id": "",
        "repair_result_event_seq_before": int(after_commit.get("journal_committed_seq") or 0),
    }
    repair_event = commit_projection_adoption_repaired_event(
        runtime_root,
        receipt=receipt,
        producer="runtime.rehearsal.projection_adoption_repair",
    )
    receipt["repair_result_event_id"] = str(repair_event.get("event_id") or "")
    receipt["repair_result_event_seq"] = int(repair_event.get("seq") or 0)
    return receipt


def repair_real_root_repo_service_cleanup_retirement(
    *,
    repo_root: str | Path,
) -> dict[str, Any]:
    """Repair mistaken repo-service cleanup retirement by reseeding blocked service identities."""

    from loop_product.event_journal import commit_repo_service_cleanup_retirement_repaired_event
    from loop_product import host_child_launch_supervisor as supervisor_module
    from loop_product.runtime.cleanup_authority import runtime_cleanup_retirement_authority
    from loop_product.runtime.control_plane import (
        live_repo_control_plane_runtime,
        live_repo_reactor_residency_guard_runtime,
        live_repo_reactor_runtime,
        repo_control_plane_runtime_root,
        repo_reactor_residency_guard_runtime_root,
        repo_reactor_runtime_root,
    )
    from loop_product.runtime.gc import housekeeping_runtime_root, live_housekeeping_reap_controller_runtime
    from loop_product.runtime_identity import reseed_runtime_root_identity

    resolved_repo_root = Path(repo_root).expanduser().resolve()
    housekeeping_root = housekeeping_runtime_root(repo_root=resolved_repo_root)
    service_specs = {
        "repo_reactor_residency_guard": (
            repo_reactor_residency_guard_runtime_root(repo_root=resolved_repo_root),
            lambda: live_repo_reactor_residency_guard_runtime(repo_root=resolved_repo_root),
        ),
        "repo_reactor": (
            repo_reactor_runtime_root(repo_root=resolved_repo_root),
            lambda: live_repo_reactor_runtime(repo_root=resolved_repo_root),
        ),
        "repo_control_plane": (
            repo_control_plane_runtime_root(repo_root=resolved_repo_root),
            lambda: live_repo_control_plane_runtime(repo_root=resolved_repo_root),
        ),
        "host_child_launch_supervisor": (
            supervisor_module.host_supervisor_runtime_root(repo_root=resolved_repo_root),
            lambda: supervisor_module.live_supervisor_runtime(repo_root=resolved_repo_root),
        ),
        "housekeeping_reap_controller": (
            housekeeping_root,
            lambda: live_housekeeping_reap_controller_runtime(repo_root=resolved_repo_root),
        ),
    }

    blocked_before: dict[str, dict[str, Any]] = {}
    reseeded_service_kinds: list[str] = []
    skipped_live_service_kinds: list[str] = []
    cleanup_authority_event_ids: list[str] = []
    for service_kind, (state_root, live_fetch) in service_specs.items():
        authority = runtime_cleanup_retirement_authority(state_root=state_root, repo_root=resolved_repo_root)
        if authority is None:
            continue
        blocked_before[service_kind] = dict(authority)
        cleanup_authority_event_id = str(authority.get("event_id") or "").strip()
        if cleanup_authority_event_id:
            cleanup_authority_event_ids.append(cleanup_authority_event_id)
        live_payload = dict(live_fetch() or {})
        if int(live_payload.get("pid") or 0) > 0:
            skipped_live_service_kinds.append(service_kind)
            continue
        reseed_runtime_root_identity(state_root)
        reseeded_service_kinds.append(service_kind)

    blocked_after: dict[str, dict[str, Any]] = {}
    for service_kind, (state_root, _) in service_specs.items():
        authority = runtime_cleanup_retirement_authority(state_root=state_root, repo_root=resolved_repo_root)
        if authority is not None:
            blocked_after[service_kind] = dict(authority)

    if reseeded_service_kinds and not blocked_after:
        status = "repaired"
    elif reseeded_service_kinds:
        status = "partial"
    elif not blocked_before:
        status = "noop"
    else:
        status = "blocked"

    receipt = {
        "repair_kind": "repo_service_cleanup_retirement_repair",
        "repo_root": str(resolved_repo_root),
        "status": status,
        "blocked_service_count_before": len(blocked_before),
        "blocked_service_kinds_before": sorted(blocked_before),
        "reseeded_service_count": len(reseeded_service_kinds),
        "reseeded_service_kinds": sorted(reseeded_service_kinds),
        "skipped_live_service_kinds": sorted(skipped_live_service_kinds),
        "blocked_service_count_after": len(blocked_after),
        "still_blocked_service_kinds_after": sorted(blocked_after),
        "cleanup_authority_event_ids": sorted(dict.fromkeys(cleanup_authority_event_ids)),
        "runtime_identity_reseeded": bool(reseeded_service_kinds),
        "repair_gap_cleared": bool(blocked_before) and not blocked_after,
        "repair_result_event_seq_before": _journal_committed_seq_via_query(housekeeping_root),
    }
    repair_event = commit_repo_service_cleanup_retirement_repaired_event(
        housekeeping_root,
        receipt=receipt,
        producer="runtime.rehearsal.repo_service_cleanup_retirement_repair",
    )
    receipt["repair_result_event_id"] = str(repair_event.get("event_id") or "")
    receipt["repair_result_event_seq"] = int(repair_event.get("seq") or 0)
    return receipt
