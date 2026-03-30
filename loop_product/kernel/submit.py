"""Kernel submission helpers for control envelopes."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from loop_product.event_journal import (
    commit_evaluator_result_recorded_event,
    commit_heavy_object_authority_gap_audit_requested_event,
    commit_heavy_object_authority_gap_repo_remediation_requested_event,
    commit_heavy_object_discovery_requested_event,
    commit_heavy_object_gc_eligible_event,
    commit_heavy_object_observed_event,
    commit_heavy_object_pinned_event,
    commit_heavy_object_pin_released_event,
    commit_heavy_object_reference_attached_event,
    commit_heavy_object_reference_released_event,
    commit_heavy_object_reclamation_requested_event,
    commit_heavy_object_registered_event,
    commit_heavy_object_superseded_event,
    commit_repo_tree_cleanup_requested_event,
    commit_terminal_result_recorded_event,
    commit_topology_command_accepted_event,
    mirror_accepted_control_envelope_event,
)
from loop_product.gateway import accept_control_envelope, normalize_control_envelope
from loop_product.artifact_hygiene import heavy_object_identity
from loop_product.kernel.authority import kernel_internal_authority
from loop_product.kernel.topology import apply_accepted_topology_mutation, review_topology_mutation
from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot
from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
from loop_product.protocols.topology import TopologyMutation
from loop_product.runtime.cleanup_authority import repo_root_from_runtime_root
from loop_product.runtime_paths import require_runtime_root


def _enforce_topology_kernel_review(state_root: Path, kernel_state, envelope: ControlEnvelope) -> ControlEnvelope:
    payload = dict(envelope.payload or {})
    mutation_payload = payload.get("topology_mutation") or payload
    try:
        mutation = TopologyMutation.from_dict(dict(mutation_payload or {}))
    except Exception as exc:  # noqa: BLE001
        return envelope.rejected(f"invalid topology mutation payload: {exc}")
    review = review_topology_mutation(kernel_state, mutation, state_root=state_root)
    summary = str(review.get("summary") or mutation.reason or envelope.note or "kernel reviewed topology mutation")
    reviewed = replace(
        envelope,
        payload={
            "topology_mutation": mutation.to_dict(),
            "review": review,
        },
        note=summary,
    )
    if str(review.get("decision") or "").upper() != "ACCEPT":
        return reviewed.rejected(summary)
    return reviewed


def _accepted_replay(
    kernel_state,
    envelope: ControlEnvelope,
) -> ControlEnvelope | None:
    envelope_id = str(envelope.envelope_id or envelope.with_identity().envelope_id or "").strip()
    if not envelope_id:
        return None
    for record in kernel_state.accepted_envelopes:
        if str(record.get("envelope_id") or "").strip() == envelope_id:
            return ControlEnvelope.from_dict(dict(record))
    return None


def _accepted_replay_from_committed_journal(
    state_root: Path,
    envelope: ControlEnvelope,
) -> ControlEnvelope | None:
    envelope_id = str(envelope.envelope_id or envelope.with_identity().envelope_id or "").strip()
    if not envelope_id:
        return None
    from loop_product.event_journal import iter_committed_events

    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "").strip() != "control_envelope_accepted":
            continue
        payload = dict(event.get("payload") or {})
        payload_envelope_id = str(payload.get("envelope_id") or "").strip()
        if not payload_envelope_id:
            try:
                payload_envelope_id = str(
                    ControlEnvelope.from_dict(payload).envelope_id
                    or ControlEnvelope.from_dict(payload).with_identity().envelope_id
                    or ""
                ).strip()
            except Exception:
                payload_envelope_id = ""
        if payload_envelope_id != envelope_id:
            continue
        try:
            return ControlEnvelope.from_dict(payload)
        except Exception:
            continue
    return None


def _path_within(root: Path, candidate: Path) -> bool:
    try:
        candidate.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def _execute_accepted_repo_tree_cleanup(state_root: Path, *, envelope: dict) -> None:
    from loop_product.runtime import execute_accepted_repo_tree_cleanup

    execute_accepted_repo_tree_cleanup(state_root, envelope=envelope)


def _execute_accepted_heavy_object_authority_gap_audit(state_root: Path, *, envelope: dict) -> None:
    from loop_product.runtime import execute_accepted_heavy_object_authority_gap_audit

    execute_accepted_heavy_object_authority_gap_audit(state_root, envelope=envelope)


def _execute_accepted_heavy_object_authority_gap_repo_remediation(state_root: Path, *, envelope: dict) -> None:
    from loop_product.runtime import execute_accepted_heavy_object_authority_gap_repo_remediation

    execute_accepted_heavy_object_authority_gap_repo_remediation(state_root, envelope=envelope)


def _execute_accepted_heavy_object_discovery(state_root: Path, *, envelope: dict) -> None:
    from loop_product.runtime import execute_accepted_heavy_object_discovery

    execute_accepted_heavy_object_discovery(state_root, envelope=envelope)


def _execute_accepted_heavy_object_reclamation(state_root: Path, *, envelope: dict) -> None:
    from loop_product.runtime import execute_accepted_heavy_object_reclamation

    execute_accepted_heavy_object_reclamation(state_root, envelope=envelope)
def submit_control_envelope(state_root: Path, envelope: ControlEnvelope) -> ControlEnvelope:
    """Normalize and accept a control envelope."""

    authority = kernel_internal_authority()
    normalized = normalize_control_envelope(envelope)
    kernel_state = load_kernel_state(state_root)
    if normalized.status is EnvelopeStatus.ACCEPTED:
        replay = _accepted_replay(kernel_state, normalized)
        if replay is None:
            replay = _accepted_replay_from_committed_journal(state_root, normalized)
        if replay is not None:
            return replay
    if normalized.status.value != "rejected" and normalized.classification == "topology":
        normalized = _enforce_topology_kernel_review(state_root, kernel_state, normalized)
    accepted = accept_control_envelope(state_root, normalized)
    if accepted.status.value != "accepted":
        return accepted
    replay = _accepted_replay(kernel_state, accepted)
    if replay is not None:
        return replay
    mirror_accepted_control_envelope_event(state_root, accepted)
    if accepted.classification == "topology":
        commit_topology_command_accepted_event(state_root, accepted)
    elif accepted.classification == "evaluator":
        commit_evaluator_result_recorded_event(state_root, accepted)
    elif accepted.classification == "terminal":
        commit_terminal_result_recorded_event(state_root, accepted)
    elif accepted.classification == "repo_tree_cleanup":
        commit_repo_tree_cleanup_requested_event(state_root, accepted)
    elif accepted.classification == "heavy_object":
        envelope_type = str(accepted.envelope_type or "").strip().lower()
        if envelope_type == "heavy_object_authority_gap_audit_request":
            commit_heavy_object_authority_gap_audit_requested_event(state_root, accepted)
        elif envelope_type == "heavy_object_authority_gap_repo_remediation_request":
            commit_heavy_object_authority_gap_repo_remediation_requested_event(state_root, accepted)
        elif envelope_type == "heavy_object_discovery_request":
            commit_heavy_object_discovery_requested_event(state_root, accepted)
        elif envelope_type == "heavy_object_observation_request":
            commit_heavy_object_observed_event(state_root, accepted)
        elif envelope_type == "heavy_object_reference_request":
            commit_heavy_object_reference_attached_event(state_root, accepted)
        elif envelope_type == "heavy_object_reference_release_request":
            commit_heavy_object_reference_released_event(state_root, accepted)
        elif envelope_type == "heavy_object_pin_request":
            commit_heavy_object_pinned_event(state_root, accepted)
        elif envelope_type == "heavy_object_pin_release_request":
            commit_heavy_object_pin_released_event(state_root, accepted)
        elif envelope_type == "heavy_object_supersession_request":
            commit_heavy_object_superseded_event(state_root, accepted)
        elif envelope_type == "heavy_object_gc_eligibility_request":
            commit_heavy_object_gc_eligible_event(state_root, accepted)
        elif envelope_type == "heavy_object_reclamation_request":
            commit_heavy_object_reclamation_requested_event(state_root, accepted)
        else:
            commit_heavy_object_registered_event(state_root, accepted)
    kernel_state.apply_accepted_envelope(accepted)
    if accepted.classification == "topology":
        apply_accepted_topology_mutation(state_root, kernel_state, accepted, authority=authority)
    elif accepted.classification in {"dispatch", "terminal"}:
        payload = dict(accepted.payload or {})
        node_id = str(payload.get("node_id") or accepted.source or "").strip()
        if node_id and node_id in kernel_state.nodes:
            persist_node_snapshot(state_root, kernel_state.nodes[node_id], authority=authority)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    if accepted.classification == "repo_tree_cleanup":
        _execute_accepted_repo_tree_cleanup(state_root, envelope=accepted.to_dict())
    elif accepted.classification == "heavy_object":
        envelope_type = str(accepted.envelope_type or "").strip().lower()
        if envelope_type == "heavy_object_authority_gap_audit_request":
            _execute_accepted_heavy_object_authority_gap_audit(state_root, envelope=accepted.to_dict())
        elif envelope_type == "heavy_object_authority_gap_repo_remediation_request":
            _execute_accepted_heavy_object_authority_gap_repo_remediation(state_root, envelope=accepted.to_dict())
        elif envelope_type == "heavy_object_discovery_request":
            _execute_accepted_heavy_object_discovery(state_root, envelope=accepted.to_dict())
        elif envelope_type == "heavy_object_reclamation_request":
            _execute_accepted_heavy_object_reclamation(state_root, envelope=accepted.to_dict())
    return accepted


def submit_topology_mutation(
    state_root: Path,
    mutation: TopologyMutation,
    *,
    round_id: str,
    generation: int,
    source: str | None = None,
) -> ControlEnvelope:
    """Review a topology mutation under kernel authority, then accept or reject it."""

    kernel_state = load_kernel_state(state_root)
    review = review_topology_mutation(kernel_state, mutation, state_root=state_root)
    envelope = mutation.to_envelope(
        round_id=round_id,
        generation=generation,
        source=source or mutation.source_node_id,
        status=EnvelopeStatus.PROPOSAL,
        note=str(review.get("summary") or mutation.reason or ""),
    )
    envelope.payload = {
        "topology_mutation": mutation.to_dict(),
        "review": review,
    }
    if str(review.get("decision") or "").upper() != "ACCEPT":
        envelope = envelope.rejected(str(review.get("summary") or "kernel rejected topology mutation"))
    return submit_control_envelope(state_root, envelope)


def submit_repo_tree_cleanup_request(
    state_root: Path,
    *,
    tree_root: str | Path,
    repo_roots: list[str | Path] | None = None,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    include_anchor_repo_root: bool = False,
) -> ControlEnvelope:
    normalized_tree_root = str(Path(tree_root).expanduser().resolve())
    normalized_repo_roots = [str(Path(path).expanduser().resolve()) for path in list(repo_roots or [])]
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="repo_tree_cleanup_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "tree_root": normalized_tree_root,
            "repo_roots": normalized_repo_roots,
            "include_anchor_repo_root": bool(include_anchor_repo_root),
            "cleanup_kind": "batch_repo_tree_cleanup",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "batch repo tree cleanup"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_registration_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    byte_size: int,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    registration_kind: str = "heavy_object_registration",
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_registration_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "byte_size": int(byte_size or 0),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "registration_kind": str(registration_kind or "heavy_object_registration").strip(),
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object registration"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_discovery_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    discovery_root: str | Path,
    discovery_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_discovery_root = str(Path(discovery_root).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_discovery_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "discovery_root": normalized_discovery_root,
            "discovery_kind": str(discovery_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "discovery_request_kind": "heavy_object_discovery",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object discovery"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_authority_gap_remediation_request(
    state_root: Path,
    *,
    object_ref: str | Path,
    object_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
) -> ControlEnvelope:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    normalized_object_ref = Path(object_ref).expanduser().resolve()
    normalized_repo_root = (
        Path(repo_root).expanduser().resolve()
        if repo_root is not None
        else repo_root_from_runtime_root(state_root=resolved_state_root)
    )
    if normalized_repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")
    if not normalized_object_ref.exists() or not (normalized_object_ref.is_file() or normalized_object_ref.is_dir()):
        raise ValueError(
            f"heavy-object authority-gap remediation target must be an existing file or directory: {normalized_object_ref}"
        )
    if not _path_within(normalized_repo_root, normalized_object_ref):
        raise ValueError(
            f"heavy-object authority-gap remediation target must stay within repo root {normalized_repo_root}: {normalized_object_ref}"
        )
    object_id = str(
        heavy_object_identity(
            normalized_object_ref,
            object_kind=object_kind,
        ).get("object_id")
        or ""
    ).strip()
    if not object_id:
        raise ValueError(f"unable to derive heavy-object identity for remediation target: {normalized_object_ref}")
    return submit_heavy_object_discovery_request(
        resolved_state_root,
        object_id=object_id,
        object_kind=object_kind,
        object_ref=normalized_object_ref,
        discovery_root=normalized_repo_root,
        discovery_kind="authority_gap_remediation",
        reason=reason,
        source=source,
        round_id=round_id,
        generation=generation,
        owner_node_id=owner_node_id,
        runtime_name=runtime_name,
        repo_root=normalized_repo_root,
    )


def submit_heavy_object_authority_gap_repo_remediation_request(
    state_root: Path,
    *,
    repo_root: str | Path | None,
    object_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    runtime_name: str = "",
    remediation_kind: str = "repo_root_authority_gap_remediation",
) -> ControlEnvelope:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    anchor_repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if anchor_repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")
    normalized_repo_root = (
        Path(repo_root).expanduser().resolve()
        if repo_root is not None
        else anchor_repo_root
    )
    if normalized_repo_root != anchor_repo_root:
        raise ValueError(
            f"repo remediation root must match anchor repo root {anchor_repo_root}: {normalized_repo_root}"
        )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_authority_gap_repo_remediation_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "repo_root": str(normalized_repo_root),
            "object_kind": str(object_kind or "").strip(),
            "runtime_name": str(runtime_name or ""),
            "remediation_kind": str(remediation_kind or "repo_root_authority_gap_remediation").strip(),
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "repo-root heavy-object authority-gap remediation"),
    )
    return submit_control_envelope(resolved_state_root, envelope)


def submit_heavy_object_authority_gap_audit_request(
    state_root: Path,
    *,
    repo_root: str | Path | None,
    object_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    runtime_name: str = "",
    audit_kind: str = "repo_root_authority_gap_audit",
) -> ControlEnvelope:
    resolved_state_root = require_runtime_root(Path(state_root).expanduser().resolve())
    anchor_repo_root = repo_root_from_runtime_root(state_root=resolved_state_root)
    if anchor_repo_root is None:
        raise ValueError(f"unable to derive anchor repo root from {resolved_state_root}")
    normalized_repo_root = (
        Path(repo_root).expanduser().resolve()
        if repo_root is not None
        else anchor_repo_root
    )
    if normalized_repo_root != anchor_repo_root:
        raise ValueError(
            f"repo audit root must match anchor repo root {anchor_repo_root}: {normalized_repo_root}"
        )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_authority_gap_audit_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "repo_root": str(normalized_repo_root),
            "object_kind": str(object_kind or "").strip(),
            "runtime_name": str(runtime_name or ""),
            "audit_kind": str(audit_kind or "repo_root_authority_gap_audit").strip(),
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "repo-root heavy-object authority-gap audit"),
    )
    return submit_control_envelope(resolved_state_root, envelope)


def submit_heavy_object_observation_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    object_refs: list[str | Path] | None,
    duplicate_count: int,
    total_bytes: int,
    observation_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_object_refs: list[str] = []
    seen_refs: set[str] = set()
    for candidate in [normalized_object_ref, *[str(item) for item in list(object_refs or [])]]:
        normalized = str(Path(candidate).expanduser().resolve()) if str(candidate or "").strip() else ""
        if not normalized or normalized in seen_refs:
            continue
        normalized_object_refs.append(normalized)
        seen_refs.add(normalized)
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_observation_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "object_refs": normalized_object_refs,
            "duplicate_count": int(duplicate_count or 0),
            "total_bytes": int(total_bytes or 0),
            "observation_kind": str(observation_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "observation_request_kind": "heavy_object_observation",
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object observation"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_pin_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    pin_holder_id: str,
    pin_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_pin_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "pin_holder_id": str(pin_holder_id or "").strip(),
            "pin_kind": str(pin_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "pin_request_kind": "heavy_object_pin",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object pin"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_pin_release_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    pin_holder_id: str,
    pin_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_pin_release_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "pin_holder_id": str(pin_holder_id or "").strip(),
            "pin_kind": str(pin_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "pin_release_request_kind": "heavy_object_pin_release",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object pin release"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_reference_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    reference_ref: str | Path,
    reference_holder_kind: str,
    reference_holder_ref: str | Path,
    reference_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_reference_ref = str(Path(reference_ref).expanduser().resolve())
    normalized_reference_holder_ref = str(Path(reference_holder_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_reference_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "reference_ref": normalized_reference_ref,
            "reference_holder_kind": str(reference_holder_kind or "").strip(),
            "reference_holder_ref": normalized_reference_holder_ref,
            "reference_kind": str(reference_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
            "reference_request_kind": "heavy_object_reference",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object reference"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_reference_release_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    reference_ref: str | Path,
    reference_holder_kind: str,
    reference_holder_ref: str | Path,
    reference_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_reference_ref = str(Path(reference_ref).expanduser().resolve())
    normalized_reference_holder_ref = str(Path(reference_holder_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_reference_release_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "reference_ref": normalized_reference_ref,
            "reference_holder_kind": str(reference_holder_kind or "").strip(),
            "reference_holder_ref": normalized_reference_holder_ref,
            "reference_kind": str(reference_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
            "reference_release_request_kind": "heavy_object_reference_release",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object reference release"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_supersession_request(
    state_root: Path,
    *,
    superseded_object_id: str,
    superseded_object_kind: str,
    superseded_object_ref: str | Path,
    replacement_object_id: str,
    replacement_object_kind: str,
    replacement_object_ref: str | Path,
    supersession_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_superseded_ref = str(Path(superseded_object_ref).expanduser().resolve())
    normalized_replacement_ref = str(Path(replacement_object_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_supersession_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "superseded_object_id": str(superseded_object_id or "").strip(),
            "superseded_object_kind": str(superseded_object_kind or "").strip(),
            "superseded_object_ref": normalized_superseded_ref,
            "replacement_object_id": str(replacement_object_id or "").strip(),
            "replacement_object_kind": str(replacement_object_kind or "").strip(),
            "replacement_object_ref": normalized_replacement_ref,
            "supersession_kind": str(supersession_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
            "supersession_request_kind": "heavy_object_supersession",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object supersession"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_gc_eligibility_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    eligibility_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    superseded_by_object_id: str = "",
    superseded_by_object_ref: str | Path | None = None,
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    normalized_replacement_ref = (
        str(Path(superseded_by_object_ref).expanduser().resolve()) if superseded_by_object_ref is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_gc_eligibility_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "eligibility_kind": str(eligibility_kind or "").strip(),
            "superseded_by_object_id": str(superseded_by_object_id or "").strip(),
            "superseded_by_object_ref": normalized_replacement_ref,
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
            "gc_eligibility_request_kind": "heavy_object_gc_eligibility",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object gc eligibility"),
    )
    return submit_control_envelope(state_root, envelope)


def submit_heavy_object_reclamation_request(
    state_root: Path,
    *,
    object_id: str,
    object_kind: str,
    object_ref: str | Path,
    reclamation_kind: str,
    reason: str,
    source: str = "root-kernel",
    round_id: str = "R0",
    generation: int = 0,
    owner_node_id: str | None = None,
    runtime_name: str = "",
    repo_root: str | Path | None = None,
    trigger_kind: str = "",
    trigger_ref: str = "",
) -> ControlEnvelope:
    normalized_object_ref = str(Path(object_ref).expanduser().resolve())
    normalized_repo_root = (
        str(Path(repo_root).expanduser().resolve()) if repo_root is not None else ""
    )
    envelope = ControlEnvelope(
        source=str(source or "root-kernel"),
        envelope_type="heavy_object_reclamation_request",
        payload_type="local_control_decision",
        round_id=str(round_id or "R0"),
        generation=int(generation or 0),
        payload={
            "object_id": str(object_id or "").strip(),
            "object_kind": str(object_kind or "").strip(),
            "object_ref": normalized_object_ref,
            "reclamation_kind": str(reclamation_kind or "").strip(),
            "owner_node_id": str(owner_node_id or source or "root-kernel"),
            "runtime_name": str(runtime_name or ""),
            "repo_root": normalized_repo_root,
            "trigger_kind": str(trigger_kind or "").strip(),
            "trigger_ref": str(trigger_ref or "").strip(),
            "reclamation_request_kind": "heavy_object_reclamation",
        },
        status=EnvelopeStatus.PROPOSAL,
        note=str(reason or "heavy object reclamation"),
    )
    return submit_control_envelope(state_root, envelope)
