"""Recovery proposal builders and kernel-reviewed recovery helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from loop_product.kernel.authority import KernelMutationAuthority
from loop_product.kernel.state import ACTIVE_NODE_STATUSES, KernelState, persist_node_snapshot
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.protocols.node import NodeSpec, RuntimeAttachmentState, normalize_runtime_state
from loop_product.protocols.topology import TopologyMutation
from loop_product.topology.budget import normalized_complexity_budget


def build_resume_request(
    source_node: NodeSpec,
    *,
    reason: str,
    consistency_signal: str,
    payload: Mapping[str, Any] | None = None,
) -> TopologyMutation:
    """Build a structured resume proposal for a blocked node."""

    request_payload = dict(payload or {})
    request_payload.update(
        {
            "consistency_signal": str(consistency_signal).strip(),
            "self_attribution": str(request_payload.get("self_attribution") or "").strip(),
            "self_repair": str(request_payload.get("self_repair") or "").strip(),
        }
    )
    return TopologyMutation.resume(source_node.node_id, reason=reason, payload=request_payload)


def build_retry_request(
    source_node: NodeSpec,
    *,
    reason: str,
    self_attribution: str,
    self_repair: str,
    payload: Mapping[str, Any] | None = None,
) -> TopologyMutation:
    """Build a structured retry proposal that keeps work on the same node."""

    request_payload = dict(payload or {})
    request_payload.update(
        {
            "self_attribution": str(self_attribution).strip(),
            "self_repair": str(self_repair).strip(),
        }
    )
    return TopologyMutation.retry(source_node.node_id, reason=reason, payload=request_payload)


def build_relaunch_request(
    source_node: NodeSpec,
    *,
    replacement_node_id: str,
    reason: str,
    self_attribution: str,
    self_repair: str,
    goal_slice: str | None = None,
    execution_policy: Mapping[str, Any] | None = None,
    reasoning_profile: Mapping[str, Any] | None = None,
    budget_profile: Mapping[str, Any] | None = None,
    allowed_actions: Sequence[str] | None = None,
    result_sink_ref: str = "",
    lineage_ref: str = "",
    payload: Mapping[str, Any] | None = None,
) -> TopologyMutation:
    """Build a relaunch proposal that materializes a fresh replacement child if accepted."""

    normalized_replacement = _normalized_relaunch_target(
        source_node=source_node,
        replacement_node_id=replacement_node_id,
        goal_slice=goal_slice,
        execution_policy=execution_policy,
        reasoning_profile=reasoning_profile,
        budget_profile=budget_profile,
        allowed_actions=allowed_actions,
        result_sink_ref=result_sink_ref,
        lineage_ref=lineage_ref,
    )
    request_payload = dict(payload or {})
    request_payload.update(
        {
            "self_attribution": str(self_attribution).strip(),
            "self_repair": str(self_repair).strip(),
            "replacement_node": normalized_replacement,
        }
    )
    return TopologyMutation.relaunch(source_node.node_id, reason=reason, payload=request_payload)


def review_recovery_request(kernel_state: KernelState, mutation: TopologyMutation) -> dict[str, Any]:
    """Review a runtime recovery request under current kernel authority."""

    source = dict(kernel_state.nodes.get(mutation.source_node_id) or {})
    source_status = str(source.get("status") or "")
    allowed_actions = {str(item) for item in source.get("allowed_actions") or []}
    source_runtime_state = normalize_runtime_state(dict(source.get("runtime_state") or {}))
    source_attachment_state = str(source_runtime_state.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value)
    payload = dict(mutation.payload or {})
    normalized_runtime_loss_signal = _normalized_runtime_loss_signal(payload.get("runtime_loss_signal"))
    self_attribution = str(payload.get("self_attribution") or "").strip()
    self_repair = str(payload.get("self_repair") or "").strip()

    if mutation.kind == "resume":
        checks = [
            {
                "check_id": "R1_source_node_authorized",
                "passed": bool(source)
                and source_status == "BLOCKED"
                and "resume_request" in allowed_actions,
                "detail": "resume requires an existing BLOCKED node that allows resume_request",
            },
            {
                "check_id": "R2_consistency_signal",
                "passed": bool(str(payload.get("consistency_signal") or "").strip()),
                "detail": "resume requires a non-empty consistency signal showing the blocker is resolved",
            },
        ]
        accepted = all(bool(item["passed"]) for item in checks)
        return {
            "decision": "ACCEPT" if accepted else "REJECT",
            "summary": (
                f"accepted resume request for {mutation.source_node_id}"
                if accepted
                else f"rejected resume request for {mutation.source_node_id}: kernel review failed"
            ),
            "checks": checks,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
        }

    if mutation.kind == "retry":
        active_runtime_loss_ok = source_status == "ACTIVE" and (
            source_attachment_state == RuntimeAttachmentState.LOST.value or bool(normalized_runtime_loss_signal)
        )
        checks = [
            {
                "check_id": "R1_source_node_authorized",
                "passed": bool(source)
                and (source_status in {"BLOCKED", "FAILED"} or active_runtime_loss_ok)
                and "retry_request" in allowed_actions,
                "detail": "retry requires an existing BLOCKED/FAILED node or orphaned ACTIVE node that allows retry_request",
            },
            {
                "check_id": "R2_self_repair_evidence",
                "passed": bool(self_attribution and self_repair),
                "detail": "retry requires explicit self-attribution and self-repair evidence",
            },
            {
                "check_id": "R3_runtime_loss_signal",
                "passed": source_status != "ACTIVE" or active_runtime_loss_ok,
                "detail": "retry on an ACTIVE node requires explicit runtime-loss evidence or a persisted LOST runtime attachment",
            },
        ]
        accepted = all(bool(item["passed"]) for item in checks)
        return {
            "decision": "ACCEPT" if accepted else "REJECT",
            "summary": (
                f"accepted retry request for {mutation.source_node_id}"
                if accepted
                else f"rejected retry request for {mutation.source_node_id}: kernel review failed"
            ),
            "checks": checks,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
            "normalized_runtime_loss_signal": normalized_runtime_loss_signal,
        }

    if mutation.kind == "relaunch":
        replacement = dict(payload.get("replacement_node") or {})
        replacement_node_id = str(replacement.get("node_id") or "").strip()
        parent_node_id = str(source.get("parent_node_id") or "").strip()
        active_runtime_loss_ok = source_status == "ACTIVE" and (
            source_attachment_state == RuntimeAttachmentState.LOST.value or bool(normalized_runtime_loss_signal)
        )
        active_now = sum(
            1 for node in kernel_state.nodes.values() if str(node.get("status") or "") in ACTIVE_NODE_STATUSES
        )
        source_active_weight = 1 if source_status in ACTIVE_NODE_STATUSES else 0
        max_active_nodes = int(normalized_complexity_budget(dict(kernel_state.complexity_budget)).get("max_active_nodes") or 0)
        projected_active = active_now - source_active_weight + 1
        checks = [
            {
                "check_id": "R1_source_node_authorized",
                "passed": bool(source)
                and (source_status in {"BLOCKED", "FAILED"} or active_runtime_loss_ok)
                and "relaunch_request" in allowed_actions
                and bool(parent_node_id)
                and parent_node_id in kernel_state.nodes,
                "detail": "relaunch requires an existing BLOCKED/FAILED node or orphaned ACTIVE node with a live parent and relaunch_request authority",
            },
            {
                "check_id": "R2_self_repair_evidence",
                "passed": bool(self_attribution and self_repair),
                "detail": "relaunch requires explicit self-attribution and self-repair evidence",
            },
            {
                "check_id": "R3_runtime_loss_signal",
                "passed": source_status != "ACTIVE" or active_runtime_loss_ok,
                "detail": "relaunch on an ACTIVE node requires explicit runtime-loss evidence or a persisted LOST runtime attachment",
            },
            {
                "check_id": "R4_replacement_node_spec",
                "passed": bool(replacement_node_id)
                and replacement_node_id != mutation.source_node_id
                and replacement_node_id not in kernel_state.nodes
                and bool(str(replacement.get("goal_slice") or "").strip()),
                "detail": "relaunch requires a distinct non-empty replacement node spec that is not already materialized",
            },
            {
                "check_id": "R5_active_node_budget",
                "passed": projected_active <= max_active_nodes,
                "detail": "accepted relaunch must stay within max_active_nodes after superseding the source node",
            },
        ]
        accepted = all(bool(item["passed"]) for item in checks)
        return {
            "decision": "ACCEPT" if accepted else "REJECT",
            "summary": (
                f"accepted relaunch request for {mutation.source_node_id} as {replacement_node_id}"
                if accepted
                else f"rejected relaunch request for {mutation.source_node_id}: kernel review failed"
            ),
            "checks": checks,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
            "normalized_relaunch_target": replacement,
            "normalized_runtime_loss_signal": normalized_runtime_loss_signal,
        }

    return {
        "decision": "REJECT",
        "summary": f"kernel does not recognize recovery kind `{mutation.kind}`",
        "checks": [],
        "self_attribution": self_attribution,
        "self_repair": self_repair,
    }


def apply_accepted_recovery_mutation(
    state_root: Path,
    kernel_state: KernelState,
    envelope: ControlEnvelope,
    *,
    authority: KernelMutationAuthority | None = None,
) -> None:
    """Apply accepted runtime recovery effects to durable state."""

    payload = dict(envelope.payload or {})
    mutation = TopologyMutation.from_dict(dict(payload.get("topology_mutation") or payload))
    review = dict(payload.get("review") or {})
    source_node_id = mutation.source_node_id
    if source_node_id not in kernel_state.nodes:
        return

    if mutation.kind in {"resume", "retry"}:
        source_record = kernel_state.nodes[source_node_id]
        source_record["status"] = "ACTIVE"
        if mutation.kind == "retry":
            source_record["runtime_state"] = _retry_runtime_state(
                review=review,
                mutation=mutation,
                accepted_at=str(envelope.accepted_at or ""),
                envelope_id=str(envelope.envelope_id or ""),
            )
        kernel_state.blocked_reasons.pop(source_node_id, None)
        persist_node_snapshot(state_root, source_record, authority=authority)
        return

    if mutation.kind != "relaunch":
        return

    source_record = kernel_state.nodes[source_node_id]
    parent_node_id = str(source_record.get("parent_node_id") or "").strip()
    if not parent_node_id or parent_node_id not in kernel_state.nodes:
        return

    source_record["status"] = "FAILED"
    normalized_runtime_loss_signal = dict(review.get("normalized_runtime_loss_signal") or mutation.payload.get("runtime_loss_signal") or {})
    if normalized_runtime_loss_signal:
        persisted_loss = normalize_runtime_state(normalized_runtime_loss_signal)
        persisted_loss["evidence_refs"] = list(persisted_loss.get("evidence_refs") or []) + [
            f"control_envelope:{str(envelope.envelope_id or '')}"
        ]
        source_record["runtime_state"] = normalize_runtime_state(persisted_loss)
    kernel_state.blocked_reasons.pop(source_node_id, None)
    persist_node_snapshot(state_root, source_record, authority=authority)

    target = dict(review.get("normalized_relaunch_target") or mutation.payload.get("replacement_node") or {})
    replacement_node_id = str(target.get("node_id") or "").strip()
    goal_slice = str(target.get("goal_slice") or "").strip()
    if not (replacement_node_id and goal_slice):
        return

    from loop_product.dispatch.child_dispatch import materialize_child

    materialize_child(
        state_root=state_root,
        kernel_state=kernel_state,
        parent_node_id=parent_node_id,
        node_id=replacement_node_id,
        goal_slice=goal_slice,
        round_id=str(target.get("round_id") or f"{source_record.get('round_id')}.relaunch"),
        execution_policy=dict(target.get("execution_policy") or {}),
        reasoning_profile=dict(target.get("reasoning_profile") or {}),
        budget_profile=dict(target.get("budget_profile") or {}),
        node_kind=str(target.get("node_kind") or source_record.get("node_kind") or "implementer"),
        generation=int(target.get("generation") or int(source_record.get("generation") or 0)),
        allowed_actions=list(target.get("allowed_actions") or []),
        workspace_root=str(target.get("workspace_root") or ""),
        codex_home=str(target.get("codex_home") or ""),
        result_sink_ref=str(target.get("result_sink_ref") or ""),
        lineage_ref=str(target.get("lineage_ref") or ""),
        authority=authority,
    )


def _normalized_relaunch_target(
    *,
    source_node: NodeSpec,
    replacement_node_id: str,
    goal_slice: str | None,
    execution_policy: Mapping[str, Any] | None,
    reasoning_profile: Mapping[str, Any] | None,
    budget_profile: Mapping[str, Any] | None,
    allowed_actions: Sequence[str] | None,
    result_sink_ref: str,
    lineage_ref: str,
) -> dict[str, Any]:
    node_id = str(replacement_node_id).strip()
    return {
        "node_id": node_id,
        "goal_slice": str(goal_slice or source_node.goal_slice or "").strip(),
        "node_kind": source_node.node_kind,
        "round_id": f"{source_node.round_id}.relaunch",
        "execution_policy": {
            **dict(source_node.execution_policy),
            **dict(execution_policy or {}),
        },
        "reasoning_profile": {
            **dict(source_node.reasoning_profile),
            **dict(reasoning_profile or {}),
        },
        "budget_profile": {
            **dict(source_node.budget_profile),
            **dict(budget_profile or {}),
        },
        "allowed_actions": list(allowed_actions or source_node.allowed_actions or []),
        "workspace_root": str(source_node.workspace_root or ""),
        "codex_home": str(source_node.codex_home or source_node.workspace_root or ""),
        "result_sink_ref": str(result_sink_ref or f"artifacts/{node_id}/result.json"),
        "lineage_ref": str(lineage_ref or f"{source_node.parent_node_id or 'root'}->{node_id}"),
        "generation": source_node.generation,
    }


def _normalized_runtime_loss_signal(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        return {}
    normalized = normalize_runtime_state(dict(payload))
    return normalized if normalized["attachment_state"] == RuntimeAttachmentState.LOST.value else {}


def _retry_runtime_state(
    *,
    review: Mapping[str, Any],
    mutation: TopologyMutation,
    accepted_at: str,
    envelope_id: str,
) -> dict[str, Any]:
    runtime_loss_signal = dict(review.get("normalized_runtime_loss_signal") or mutation.payload.get("runtime_loss_signal") or {})
    if runtime_loss_signal:
        previous = normalize_runtime_state(runtime_loss_signal)
        summary = str(previous.get("summary") or "").strip()
        observation_kind = str(previous.get("observation_kind") or "").strip()
        return normalize_runtime_state(
            {
                "attachment_state": RuntimeAttachmentState.UNOBSERVED.value,
                "observed_at": accepted_at,
                "summary": (
                    f"retry accepted after runtime-loss signal ({observation_kind}): {summary}"
                    if summary or observation_kind
                    else "retry accepted after runtime loss"
                ),
                "observation_kind": "recovery_retry_accepted",
                "evidence_refs": list(previous.get("evidence_refs") or []) + [f"control_envelope:{envelope_id}"],
            }
        )
    return normalize_runtime_state(
        {
            "attachment_state": RuntimeAttachmentState.UNOBSERVED.value,
            "observed_at": accepted_at,
            "summary": "retry accepted",
            "observation_kind": "recovery_retry_accepted",
            "evidence_refs": [f"control_envelope:{envelope_id}"],
        }
    )
