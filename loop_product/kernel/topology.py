"""Kernel-owned topology review and application helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from loop_product.dispatch.bootstrap import bootstrap_first_implementer_node
from loop_product.dispatch.child_dispatch import materialize_child
from loop_product.dispatch.launch_runtime import launch_child_from_result_ref
from loop_product.kernel.authority import KernelMutationAuthority, require_kernel_authority
from loop_product.kernel.state import KernelState, persist_node_snapshot
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.protocols.node import NodeStatus, RuntimeAttachmentState, normalize_runtime_state
from loop_product.protocols.topology import TopologyMutation
from loop_product.runtime.recover import apply_accepted_recovery_mutation, review_recovery_request
from loop_product.runtime_paths import node_live_artifact_root, product_repo_root, require_runtime_root
from loop_product.topology.activate import review_activate_request
from loop_product.topology.merge import review_merge_request
from loop_product.topology.prune import review_reap_request
from loop_product.topology.split_review import review_split_request


def _load_source_handoff(source_record: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    workspace_root = Path(str(source_record.get("workspace_root") or "")).expanduser().resolve()
    if not str(workspace_root).strip():
        raise ValueError("source node missing workspace_root for split child bootstrap")
    handoff_ref = workspace_root / "FROZEN_HANDOFF.json"
    if not handoff_ref.exists():
        raise ValueError(f"source frozen handoff missing for split child bootstrap: {handoff_ref}")
    payload = json.loads(handoff_ref.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"source frozen handoff must be a JSON object: {handoff_ref}")
    return handoff_ref, payload


def _child_bootstrap_request_from_source_handoff(
    *,
    state_root: Path,
    source_record: dict[str, Any],
    child_record: dict[str, Any],
) -> dict[str, Any]:
    handoff_ref, handoff = _load_source_handoff(source_record)
    node_id = str(child_record.get("node_id") or "").strip()
    goal_slice = str(child_record.get("goal_slice") or "").strip()
    workspace_root = str(child_record.get("workspace_root") or "").strip()
    result_sink_ref = str(child_record.get("result_sink_ref") or "").strip()
    round_id = str(child_record.get("round_id") or "").strip()
    if not (node_id and goal_slice and workspace_root and result_sink_ref and round_id):
        raise ValueError(
            f"child bootstrap requires node_id/goal_slice/workspace_root/result_sink_ref/round_id for {node_id or '<missing>'}"
        )
    endpoint_artifact_ref = str(handoff.get("endpoint_artifact_ref") or "").strip()
    root_goal = str(handoff.get("root_goal") or "").strip()
    if not endpoint_artifact_ref or not root_goal:
        raise ValueError("source frozen handoff missing endpoint_artifact_ref or root_goal for child bootstrap")
    inherited_context_refs: list[str] = [str(item or "") for item in list(handoff.get("context_refs") or [])]
    inherited_context_refs.append(str(handoff_ref.resolve()))
    handoff_md_ref = handoff_ref.with_suffix(".md")
    if handoff_md_ref.exists():
        inherited_context_refs.append(str(handoff_md_ref.resolve()))
    shared_cache_helper = (product_repo_root().resolve() / "scripts" / "ensure_workspace_lake_packages.sh").resolve()
    inherited_context_refs.append(str(shared_cache_helper))
    deduped_context_refs: list[str] = []
    seen_context_refs: set[str] = set()
    for raw_ref in inherited_context_refs:
        normalized_ref = str(raw_ref or "").strip()
        if not normalized_ref or normalized_ref in seen_context_refs:
            continue
        seen_context_refs.add(normalized_ref)
        deduped_context_refs.append(normalized_ref)
    workspace_root_path = Path(workspace_root).expanduser().resolve()
    workspace_mirror_relpath = str(handoff.get("workspace_mirror_relpath") or "deliverables/primary_artifact")
    external_live_root = node_live_artifact_root(
        state_root=state_root,
        node_id=node_id,
        workspace_mirror_relpath=workspace_mirror_relpath,
    )
    workspace_live_artifact_relpath = os.path.relpath(external_live_root, start=workspace_root_path)
    return {
        "mode": "continue_exact",
        "state_root": str(state_root.resolve()),
        "workspace_root": workspace_root,
        "node_id": node_id,
        "round_id": round_id,
        "root_goal": root_goal,
        "child_goal_slice": goal_slice,
        "endpoint_artifact_ref": endpoint_artifact_ref,
        "workspace_mirror_relpath": workspace_mirror_relpath,
        "workspace_live_artifact_relpath": workspace_live_artifact_relpath,
        "external_publish_target": str(handoff.get("external_publish_target") or ""),
        "required_output_paths": [str(item).strip() for item in list(child_record.get("required_output_paths") or []) if str(item).strip()],
        "context_refs": deduped_context_refs,
        "result_sink_ref": result_sink_ref,
    }


def _mark_split_child_launch_failure(
    *,
    state_root: Path,
    kernel_state: KernelState,
    node_id: str,
    summary: str,
    authority: KernelMutationAuthority,
) -> None:
    if node_id not in kernel_state.nodes:
        return
    node_record = kernel_state.nodes[node_id]
    node_record["status"] = "BLOCKED"
    node_record["runtime_state"] = normalize_runtime_state(
        {
            "attachment_state": RuntimeAttachmentState.LOST.value,
            "observed_at": "",
            "summary": summary,
            "observation_kind": "parallel_split_launch_error",
            "evidence_refs": [],
        }
    )
    kernel_state.blocked_reasons[node_id] = summary
    persist_node_snapshot(state_root, node_record, authority=authority)


def _activation_condition_blocks_immediate_parallel_launch(activation_condition: str) -> bool:
    text = str(activation_condition or "").strip()
    if not text:
        return False
    normalized = text.lower()
    immediate_prefixes = (
        "may start immediately",
        "start immediately",
        "may launch immediately",
        "launch immediately",
        "can start immediately",
        "can launch immediately",
    )
    if normalized.startswith(immediate_prefixes):
        return False
    if normalized.startswith("after:"):
        return True
    blocking_markers = (
        "only after",
        "until ",
        "wait until",
        "wait for",
        "blocked until",
    )
    return any(marker in normalized for marker in blocking_markers)


def _bootstrap_and_launch_parallel_children(
    *,
    state_root: Path,
    kernel_state: KernelState,
    source_record: dict[str, Any],
    target_nodes: list[dict[str, Any]],
    authority: KernelMutationAuthority,
) -> None:
    for target in target_nodes:
        node_id = str(target.get("node_id") or "").strip()
        if not node_id:
            continue
        child_record = dict(kernel_state.nodes.get(node_id) or {})
        try:
            bootstrap_payload = bootstrap_first_implementer_node(
                authority=authority,
                **_child_bootstrap_request_from_source_handoff(
                    state_root=state_root,
                    source_record=source_record,
                    child_record=child_record,
                ),
            )
            launch_child_from_result_ref(result_ref=str(bootstrap_payload.get("bootstrap_result_ref") or ""))
        except Exception as exc:  # noqa: BLE001
            _mark_split_child_launch_failure(
                state_root=state_root,
                kernel_state=kernel_state,
                node_id=node_id,
                summary=f"parallel split child bootstrap/launch failed: {exc}",
                authority=authority,
            )


def _bootstrap_and_launch_activated_child(
    *,
    state_root: Path,
    kernel_state: KernelState,
    child_record: dict[str, Any],
    authority: KernelMutationAuthority,
) -> None:
    parent_node_id = str(child_record.get("parent_node_id") or "").strip()
    if not parent_node_id:
        raise ValueError(f"activated child {child_record.get('node_id')!r} missing parent_node_id")
    source_record = dict(kernel_state.nodes.get(parent_node_id) or {})
    if not source_record:
        raise ValueError(f"activated child {child_record.get('node_id')!r} missing parent source record {parent_node_id!r}")
    bootstrap_payload = bootstrap_first_implementer_node(
        authority=authority,
        **_child_bootstrap_request_from_source_handoff(
            state_root=state_root,
            source_record=source_record,
            child_record=child_record,
        ),
    )
    launch_child_from_result_ref(result_ref=str(bootstrap_payload.get("bootstrap_result_ref") or ""))


def review_topology_mutation(
    kernel_state: KernelState,
    mutation: TopologyMutation,
    *,
    state_root: Path | None = None,
) -> dict[str, Any]:
    """Review a topology mutation under current kernel authority."""

    if mutation.kind == "split":
        return review_split_request(kernel_state, mutation)
    if mutation.kind == "activate":
        return review_activate_request(kernel_state, mutation, state_root=state_root)
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
        try:
            _bootstrap_and_launch_activated_child(
                state_root=state_root,
                kernel_state=kernel_state,
                child_record=source_record,
                authority=authority,
            )
        except Exception as exc:  # noqa: BLE001
            _mark_split_child_launch_failure(
                state_root=state_root,
                kernel_state=kernel_state,
                node_id=source_node_id,
                summary=f"deferred activation child bootstrap/launch failed: {exc}",
                authority=authority,
            )
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
    launch_targets: list[dict[str, Any]] = []
    for target in target_nodes:
        node_id = str(target.get("node_id") or "").strip()
        goal_slice = str(target.get("goal_slice") or "").strip()
        if not (node_id and goal_slice):
            continue
        launch_immediately = (
            normalized_split_mode == "parallel"
            and not list(target.get("depends_on_node_ids") or [])
            and not _activation_condition_blocks_immediate_parallel_launch(str(target.get("activation_condition") or ""))
        )
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
            required_output_paths=list(target.get("required_output_paths") or []),
            workspace_root=str(target.get("workspace_root") or ""),
            codex_home=str(target.get("codex_home") or ""),
            depends_on_node_ids=list(target.get("depends_on_node_ids") or []),
            activation_condition=str(target.get("activation_condition") or ""),
            result_sink_ref=str(target.get("result_sink_ref") or ""),
            lineage_ref=str(
                target.get("lineage_ref") or f"{source_record.get('lineage_ref') or source_node_id}->{node_id}"
            ),
            status=NodeStatus.ACTIVE if launch_immediately else NodeStatus.PLANNED,
            authority=authority,
        )
        if launch_immediately:
            launch_targets.append(target)
    if normalized_split_mode == "parallel" and launch_targets:
        _bootstrap_and_launch_parallel_children(
            state_root=state_root,
            kernel_state=kernel_state,
            source_record=source_record,
            target_nodes=launch_targets,
            authority=authority,
        )
