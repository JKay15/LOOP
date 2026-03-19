"""Kernel-owned topology review and application helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loop_product.dispatch.child_dispatch import materialize_child
from loop_product.kernel.authority import KernelMutationAuthority, require_kernel_authority
from loop_product.kernel.state import KernelState, persist_node_snapshot
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.protocols.node import NodeStatus, RuntimeAttachmentState, normalize_runtime_state
from loop_product.protocols.topology import TopologyMutation
from loop_product.runtime.recover import apply_accepted_recovery_mutation, review_recovery_request
from loop_product.runtime_paths import require_runtime_root
from loop_product.topology.activate import review_activate_request
from loop_product.topology.merge import review_merge_request
from loop_product.topology.prune import review_reap_request
from loop_product.topology.split_review import review_split_request


def review_topology_mutation(kernel_state: KernelState, mutation: TopologyMutation) -> dict[str, Any]:
    """Review a topology mutation under current kernel authority."""

    if mutation.kind == "split":
        return review_split_request(kernel_state, mutation)
    if mutation.kind == "activate":
        return review_activate_request(kernel_state, mutation)
    if mutation.kind == "merge":
        return review_merge_request(kernel_state, mutation)
    if mutation.kind == "reap":
        return review_reap_request(kernel_state, mutation)
    if mutation.kind in {"resume", "retry", "relaunch"}:
        return review_recovery_request(kernel_state, mutation)
    return {
        "decision": "REJECT",
        "summary": f"kernel does not yet accept topology mutation kind `{mutation.kind}`",
        "checks": [],
        "normalized_target_nodes": [],
    }


def apply_accepted_topology_mutation(
    state_root: Path,
    kernel_state: KernelState,
    envelope: ControlEnvelope,
    *,
    authority: KernelMutationAuthority | None = None,
) -> None:
    """Apply accepted topology side effects to durable state."""

    require_kernel_authority(authority, surface="apply_accepted_topology_mutation")
    state_root = require_runtime_root(state_root)
    payload = dict(envelope.payload or {})
    mutation_obj = dict(payload.get("topology_mutation") or payload)
    mutation = TopologyMutation.from_dict(mutation_obj)
    review = dict(payload.get("review") or {})
    if mutation.kind == "reap":
        source_node_id = mutation.source_node_id
        archived = kernel_state.unregister_node(source_node_id)
        if archived is None:
            return
        archive_dir = state_root / "quarantine" / "reaped"
        archive_dir.mkdir(parents=True, exist_ok=True)
        (archive_dir / f"{source_node_id}.json").write_text(
            json.dumps(archived, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        node_state_path = state_root / "state" / f"{source_node_id}.json"
        if node_state_path.exists():
            node_state_path.unlink()
        delegation_path = state_root / "state" / "delegations" / f"{source_node_id}.json"
        if delegation_path.exists():
            delegation_archive_dir = archive_dir / "delegations"
            delegation_archive_dir.mkdir(parents=True, exist_ok=True)
            delegation_payload = delegation_path.read_text(encoding="utf-8")
            (delegation_archive_dir / f"{source_node_id}.json").write_text(delegation_payload, encoding="utf-8")
            delegation_path.unlink()
        return
    if mutation.kind == "activate":
        source_node_id = mutation.source_node_id
        if source_node_id not in kernel_state.nodes:
            return
        source_record = kernel_state.nodes[source_node_id]
        source_record["status"] = "ACTIVE"
        persist_node_snapshot(state_root, source_record, authority=authority)
        return
    if mutation.kind == "merge":
        source_node_id = mutation.source_node_id
        if source_node_id not in kernel_state.nodes:
            return
        source_record = kernel_state.nodes[source_node_id]
        source_record["status"] = "ACTIVE"
        source_record["runtime_state"] = normalize_runtime_state(
            {
                "attachment_state": RuntimeAttachmentState.UNOBSERVED.value,
                "observed_at": str(envelope.accepted_at or ""),
                "summary": str(review.get("summary") or mutation.reason or envelope.note or ""),
                "observation_kind": "merge_reactivation",
                "evidence_refs": [f"control_envelope:{str(envelope.envelope_id or '')}"],
            }
        )
        kernel_state.blocked_reasons.pop(source_node_id, None)
        persist_node_snapshot(state_root, source_record, authority=authority)
        return
    if mutation.kind in {"resume", "retry", "relaunch"}:
        apply_accepted_recovery_mutation(state_root, kernel_state, envelope, authority=authority)
        return
    if mutation.kind != "split":
        return

    source_node_id = mutation.source_node_id
    if source_node_id not in kernel_state.nodes:
        return

    source_record = kernel_state.nodes[source_node_id]
    normalized_split_mode = str(review.get("normalized_split_mode") or mutation.payload.get("split_mode") or "parallel")
    if normalized_split_mode == "parallel":
        source_record["status"] = "BLOCKED"
        kernel_state.blocked_reasons[source_node_id] = str(review.get("summary") or mutation.reason or envelope.note or "")
    else:
        source_record["status"] = "ACTIVE"
        kernel_state.blocked_reasons.pop(source_node_id, None)
    persist_node_snapshot(state_root, source_record, authority=authority)

    target_nodes = [dict(item) for item in (review.get("normalized_target_nodes") or [])]
    for target in target_nodes:
        node_id = str(target.get("node_id") or "").strip()
        goal_slice = str(target.get("goal_slice") or "").strip()
        if not (node_id and goal_slice):
            continue
        materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id=source_node_id,
            node_id=node_id,
            goal_slice=goal_slice,
            round_id=str(target.get("round_id") or f"{source_record.get('round_id')}.{node_id}"),
            execution_policy=dict(target.get("execution_policy") or {}),
            reasoning_profile=dict(target.get("reasoning_profile") or {}),
            budget_profile=dict(target.get("budget_profile") or {}),
            node_kind=str(target.get("node_kind") or "implementer"),
            generation=int(target.get("generation") or int(source_record.get("generation") or 0) + 1),
            allowed_actions=list(target.get("allowed_actions") or []),
            workspace_root=str(target.get("workspace_root") or ""),
            codex_home=str(target.get("codex_home") or ""),
            depends_on_node_ids=list(target.get("depends_on_node_ids") or []),
            activation_condition=str(target.get("activation_condition") or ""),
            result_sink_ref=str(target.get("result_sink_ref") or ""),
            lineage_ref=str(
                target.get("lineage_ref") or f"{source_record.get('lineage_ref') or source_node_id}->{node_id}"
            ),
            status=NodeStatus.PLANNED if normalized_split_mode == "deferred" else NodeStatus.ACTIVE,
            authority=authority,
        )
