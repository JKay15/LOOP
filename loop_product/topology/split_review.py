"""Split-review helpers."""

from __future__ import annotations

from collections.abc import Sequence as SequenceABC
from typing import Any, Mapping, Sequence

from loop_product.kernel.state import ACTIVE_NODE_STATUSES, KernelState
from loop_product.protocols.node import NodeSpec, normalize_execution_policy, normalize_reasoning_profile
from loop_product.protocols.topology import TopologyMutation

_SPLIT_CHECK_ORDER = (
    "S1_source_node_authorized",
    "S2_target_node_ids",
    "S3_target_goal_slices",
    "S4_generation_budget",
    "S5_active_node_budget",
)

_SUPPORTED_SPLIT_MODES = {"parallel", "deferred"}


def _normalize_split_mode(value: Any) -> str:
    mode = str(value or "parallel").strip().lower()
    return mode if mode in _SUPPORTED_SPLIT_MODES else ""


def _normalized_split_targets(
    *,
    source_node: NodeSpec,
    target_nodes: Sequence[Mapping[str, Any]],
    split_mode: str,
) -> list[dict[str, Any]]:
    normalized, errors = _normalize_target_payloads(
        source_node_id=source_node.node_id,
        source_generation=source_node.generation,
        source_round_id=source_node.round_id,
        source_node_kind=source_node.node_kind,
        source_execution_policy=source_node.execution_policy,
        source_reasoning_profile=source_node.reasoning_profile,
        source_budget_profile=source_node.budget_profile,
        source_allowed_actions=source_node.allowed_actions,
        target_nodes=target_nodes,
        split_mode=split_mode,
    )
    if errors:
        raise ValueError("; ".join(errors))
    return normalized


def _normalize_target_payloads(
    *,
    source_node_id: str,
    source_generation: int,
    source_round_id: str,
    source_node_kind: str,
    source_execution_policy: Mapping[str, Any],
    source_reasoning_profile: Mapping[str, Any],
    source_budget_profile: Mapping[str, Any],
    source_allowed_actions: Sequence[str],
    target_nodes: Sequence[Any],
    split_mode: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    normalized: list[dict[str, Any]] = []
    errors: list[str] = []
    expected_generation = int(source_generation) + 1
    for index, raw_target in enumerate(target_nodes, start=1):
        if not isinstance(raw_target, Mapping):
            errors.append(f"target_nodes[{index - 1}] must be an object mapping")
            continue
        target = dict(raw_target or {})
        node_id = str(target.get("node_id") or "").strip()
        goal_slice = str(target.get("goal_slice") or "").strip()
        execution_policy_raw = target.get("execution_policy") or {}
        reasoning_profile_raw = target.get("reasoning_profile") or {}
        budget_profile_raw = target.get("budget_profile") or {}
        allowed_actions_raw = target.get("allowed_actions")
        depends_on_raw = target.get("depends_on_node_ids")
        explicit_generation = target.get("generation")
        activation_condition = str(target.get("activation_condition") or "").strip()

        if not isinstance(execution_policy_raw, Mapping):
            errors.append(f"target {node_id or index} execution_policy must be a mapping")
            continue
        if not isinstance(reasoning_profile_raw, Mapping):
            errors.append(f"target {node_id or index} reasoning_profile must be a mapping")
            continue
        if not isinstance(budget_profile_raw, Mapping):
            errors.append(f"target {node_id or index} budget_profile must be a mapping")
            continue
        if allowed_actions_raw is None:
            resolved_allowed_actions = list(source_allowed_actions or [])
        elif isinstance(allowed_actions_raw, SequenceABC) and not isinstance(allowed_actions_raw, (str, bytes)):
            resolved_allowed_actions = [str(item) for item in allowed_actions_raw]
        else:
            errors.append(f"target {node_id or index} allowed_actions must be a list of strings")
            continue
        if depends_on_raw is None or depends_on_raw == "":
            resolved_depends_on = [source_node_id] if split_mode == "deferred" else []
        elif isinstance(depends_on_raw, SequenceABC) and not isinstance(depends_on_raw, (str, bytes)):
            resolved_depends_on = [str(item).strip() for item in depends_on_raw if str(item).strip()]
        else:
            errors.append(f"target {node_id or index} depends_on_node_ids must be a list of strings")
            continue
        if split_mode == "deferred" and not activation_condition:
            activation_condition = f"after:{source_node_id}:terminal"
        if explicit_generation not in {None, ""}:
            try:
                candidate_generation = int(explicit_generation)
            except (TypeError, ValueError):
                errors.append(f"target {node_id or index} generation must be an integer")
                continue
            if candidate_generation != expected_generation:
                errors.append(
                    f"target {node_id or index} generation must equal source_generation + 1 ({expected_generation})"
                )
                continue
        normalized.append(
            {
                "node_id": node_id,
                "goal_slice": goal_slice,
                "node_kind": str(target.get("node_kind") or source_node_kind or "implementer"),
                "round_id": str(target.get("round_id") or f"{source_round_id}.S{index}"),
                "execution_policy": normalize_execution_policy(
                    {
                        **dict(source_execution_policy),
                        **dict(execution_policy_raw),
                    },
                    node_kind=str(target.get("node_kind") or source_node_kind or "implementer"),
                ),
                "reasoning_profile": normalize_reasoning_profile(
                    {
                        **dict(source_reasoning_profile),
                        **dict(reasoning_profile_raw),
                    },
                    node_kind=str(target.get("node_kind") or source_node_kind or "implementer"),
                ),
                "budget_profile": {
                    **dict(source_budget_profile),
                    **dict(budget_profile_raw),
                },
                "allowed_actions": resolved_allowed_actions,
                "workspace_root": str(target.get("workspace_root") or ""),
                "codex_home": str(target.get("codex_home") or ""),
                "depends_on_node_ids": resolved_depends_on,
                "activation_condition": activation_condition if split_mode == "deferred" else "",
                "result_sink_ref": str(target.get("result_sink_ref") or f"artifacts/{node_id}/result.json"),
                "generation": expected_generation,
            }
        )
    return normalized, errors


def _request_skeleton_checks(
    *,
    source_node: NodeSpec,
    normalized_targets: Sequence[Mapping[str, Any]],
    split_mode: str,
) -> list[dict[str, Any]]:
    target_ids = [str(item.get("node_id") or "").strip() for item in normalized_targets]
    minimum_target_count = 2 if split_mode == "parallel" else 1
    checks = {
        "S1_source_node_authorized": {
            "passed": "split_request" in source_node.allowed_actions,
            "detail": "source node must explicitly allow split_request in its allowed_actions",
        },
        "S2_target_node_ids": {
            "passed": split_mode in _SUPPORTED_SPLIT_MODES
            and len(target_ids) >= minimum_target_count
            and all(target_ids)
            and len(set(target_ids)) == len(target_ids)
            and source_node.node_id not in set(target_ids),
            "detail": (
                "parallel split must propose at least two distinct non-source target node ids; "
                "deferred split may propose one or more distinct non-source target node ids"
            ),
        },
        "S3_target_goal_slices": {
            "passed": all(
                str(item.get("goal_slice") or "").strip()
                and (
                    split_mode != "deferred"
                    or (
                        list(item.get("depends_on_node_ids") or [])
                        and str(item.get("activation_condition") or "").strip()
                    )
                )
                for item in normalized_targets
            ),
            "detail": (
                "every split target must preserve a non-empty goal_slice; "
                "deferred targets must also persist dependency and activation metadata"
            ),
        },
        "S4_generation_budget": {
            "passed": True,
            "detail": "request skeleton does not exceed generation budget by construction alone",
        },
        "S5_active_node_budget": {
            "passed": True,
            "detail": "request skeleton does not exceed active-node budget until kernel reviews current state",
        },
    }
    return [{"check_id": check_id, **checks[check_id]} for check_id in _SPLIT_CHECK_ORDER]


def build_split_request(
    source_node_id: str | None = None,
    target_node_ids: list[str] | None = None,
    *,
    source_node: NodeSpec | None = None,
    target_nodes: Sequence[Mapping[str, Any]] | None = None,
    split_mode: str = "parallel",
    completed_work: str = "",
    remaining_work: str = "",
    reason: str = "",
) -> TopologyMutation:
    """Build a structured split request without applying topology changes directly."""

    normalized_split_mode = _normalize_split_mode(split_mode) or "parallel"
    if source_node is None:
        if not source_node_id:
            raise ValueError("build_split_request needs source_node or source_node_id")
        raw_target_ids = [str(item).strip() for item in (target_node_ids or [])]
        return TopologyMutation.split(
            str(source_node_id),
            raw_target_ids,
            reason=reason,
            payload={
                "split_mode": normalized_split_mode,
                "completed_work": str(completed_work or "").strip(),
                "remaining_work": str(remaining_work or "").strip(),
                "target_nodes": [{"node_id": item, "goal_slice": ""} for item in raw_target_ids],
                "split_checks": [],
            },
        )

    normalized_targets = _normalized_split_targets(
        source_node=source_node,
        target_nodes=list(target_nodes or []),
        split_mode=normalized_split_mode,
    )
    return TopologyMutation.split(
        source_node.node_id,
        [str(item["node_id"]) for item in normalized_targets],
        reason=reason,
        payload={
            "split_mode": normalized_split_mode,
            "completed_work": str(completed_work or "").strip(),
            "remaining_work": str(remaining_work or "").strip(),
            "target_nodes": normalized_targets,
            "split_checks": _request_skeleton_checks(
                source_node=source_node,
                normalized_targets=normalized_targets,
                split_mode=normalized_split_mode,
            ),
        },
    )


def review_split_request(kernel_state: KernelState, mutation: TopologyMutation) -> dict[str, Any]:
    """Review a split request under current kernel authority and complexity limits."""

    source = dict(kernel_state.nodes.get(mutation.source_node_id) or {})
    source_status = str(source.get("status") or "")
    source_allowed_actions = list(source.get("allowed_actions") or [])
    normalized_split_mode = _normalize_split_mode(mutation.payload.get("split_mode"))
    source_generation = int(source.get("generation") or 0)
    raw_target_nodes = list(mutation.payload.get("target_nodes") or [])
    normalized_target_nodes, payload_errors = _normalize_target_payloads(
        source_node_id=mutation.source_node_id,
        source_generation=source_generation,
        source_round_id=str(source.get("round_id") or "R-unknown"),
        source_node_kind=str(source.get("node_kind") or "implementer"),
        source_execution_policy=dict(source.get("execution_policy") or {}),
        source_reasoning_profile=dict(source.get("reasoning_profile") or {}),
        source_budget_profile=dict(source.get("budget_profile") or {}),
        source_allowed_actions=list(source.get("allowed_actions") or []),
        target_nodes=raw_target_nodes,
        split_mode=normalized_split_mode or "parallel",
    )
    declared_target_ids = [str(item).strip() for item in mutation.target_node_ids]
    target_ids = [str(item.get("node_id") or "").strip() for item in normalized_target_nodes]
    existing_ids = set(kernel_state.nodes.keys())
    active_now = sum(
        1 for node in kernel_state.nodes.values() if str(node.get("status") or "") in ACTIVE_NODE_STATUSES
    )
    max_active_nodes = int(dict(kernel_state.complexity_budget).get("max_active_nodes", 4) or 4)
    max_child_generations = int(dict(kernel_state.complexity_budget).get("max_child_generations", 3) or 3)
    minimum_target_count = 2 if normalized_split_mode == "parallel" else 1
    projected_active = active_now + len(normalized_target_nodes) if normalized_split_mode == "parallel" else active_now

    checks = [
        {
            "check_id": "S1_source_node_authorized",
            "passed": bool(source) and source_status == "ACTIVE" and "split_request" in source_allowed_actions,
            "detail": "source node must exist, be ACTIVE, and explicitly allow split_request",
        },
        {
            "check_id": "S2_target_node_ids",
            "passed": normalized_split_mode in _SUPPORTED_SPLIT_MODES
            and len(declared_target_ids) >= minimum_target_count
            and declared_target_ids == target_ids
            and len(raw_target_nodes) == len(target_ids)
            and all(target_ids)
            and len(set(target_ids)) == len(target_ids)
            and mutation.source_node_id not in set(target_ids)
            and not any(target_id in existing_ids for target_id in target_ids),
            "detail": (
                "split mode must be supported; target node ids must satisfy the mode-specific minimum count, "
                "be distinct, non-empty, match the payload exactly, be non-source, and not already materialized"
            ),
        },
        {
            "check_id": "S3_target_goal_slices",
            "passed": bool(normalized_target_nodes)
            and not payload_errors
            and all(
                str(item.get("goal_slice") or "").strip()
                and (
                    normalized_split_mode != "deferred"
                    or (
                        list(item.get("depends_on_node_ids") or [])
                        and str(item.get("activation_condition") or "").strip()
                    )
                )
                for item in normalized_target_nodes
            ),
            "detail": (
                "every proposed split child must carry a valid mapping-shaped payload with a non-empty goal_slice; "
                "deferred children must also keep dependency and activation metadata"
            ),
        },
        {
            "check_id": "S4_generation_budget",
            "passed": source_generation + 1 <= max_child_generations and not payload_errors,
            "detail": "accepted split children must stay within max_child_generations and may not override the kernel-owned child generation step",
        },
        {
            "check_id": "S5_active_node_budget",
            "passed": projected_active <= max_active_nodes,
            "detail": (
                "parallel split must keep source-plus-active-children within max_active_nodes; "
                "deferred split may persist planned children without consuming active-node budget yet"
            ),
        },
    ]
    accepted = all(bool(item["passed"]) for item in checks)
    summary = (
        f"accepted {normalized_split_mode or 'unknown'} split request from {mutation.source_node_id} into {len(normalized_target_nodes)} child nodes"
        if accepted
        else (
            f"rejected split request from {mutation.source_node_id}: kernel review failed"
            + (f" ({'; '.join(payload_errors)})" if payload_errors else "")
        )
    )
    return {
        "decision": "ACCEPT" if accepted else "REJECT",
        "summary": summary,
        "checks": checks,
        "normalized_target_nodes": normalized_target_nodes if accepted else [],
        "normalized_split_mode": normalized_split_mode,
        "payload_errors": payload_errors,
    }
