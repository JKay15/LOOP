"""Materialize child nodes from frozen kernel delegation."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loop_product.kernel.authority import KernelMutationAuthority, require_kernel_authority
from loop_product.kernel.state import KernelState, persist_kernel_state
from loop_product.protocols.node import NodeSpec, NodeStatus
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_paths import (
    require_runtime_root,
    resolve_implementer_project_root,
)


def _default_allowed_actions_for_node_kind(node_kind: str) -> list[str]:
    normalized = str(node_kind or "implementer").strip().lower() or "implementer"
    if normalized == "implementer":
        return [
            "implement",
            "evaluate",
            "report",
            "split_request",
            "resume_request",
            "retry_request",
            "relaunch_request",
        ]
    return ["implement", "evaluate", "report", "resume_request", "retry_request", "relaunch_request"]


def materialize_child(
    *,
    state_root: Path,
    kernel_state: KernelState,
    parent_node_id: str,
    node_id: str,
    goal_slice: str,
    round_id: str,
    execution_policy: dict[str, Any],
    reasoning_profile: dict[str, Any],
    budget_profile: dict[str, Any],
    node_kind: str = "implementer",
    generation: int | None = None,
    allowed_actions: list[str] | None = None,
    required_output_paths: list[str] | None = None,
    workspace_root: str | Path | None = None,
    codex_home: str | Path | None = None,
    depends_on_node_ids: list[str] | None = None,
    activation_condition: str = "",
    activation_rationale: str = "",
    result_sink_ref: str = "",
    lineage_ref: str = "",
    status: NodeStatus = NodeStatus.ACTIVE,
    authority: KernelMutationAuthority | None = None,
) -> NodeSpec:
    """Create a child node under the state tree."""

    require_kernel_authority(authority, surface="materialize_child")
    state_root = require_runtime_root(state_root)
    delegation_dir = state_root / "state" / "delegations"
    delegation_dir.mkdir(parents=True, exist_ok=True)
    delegation_rel = Path("state") / "delegations" / f"{node_id}.json"
    parent_snapshot = dict(kernel_state.nodes.get(parent_node_id) or {})
    resolved_generation = int(generation) if generation is not None else int(parent_snapshot.get("generation") or 0) + 1
    resolved_allowed_actions = list(allowed_actions or _default_allowed_actions_for_node_kind(node_kind))
    resolved_required_output_paths = [str(item).strip() for item in (required_output_paths or []) if str(item).strip()]
    resolved_depends_on = [str(item).strip() for item in (depends_on_node_ids or []) if str(item).strip()]
    resolved_result_sink_ref = result_sink_ref or f"artifacts/{node_id}/result.json"
    resolved_lineage_ref = lineage_ref or f"{parent_snapshot.get('lineage_ref') or parent_node_id}->{node_id}"
    resolved_workspace_root = resolve_implementer_project_root(node_id=node_id, workspace_root=workspace_root)
    resolved_workspace_root.mkdir(parents=True, exist_ok=True)
    node = NodeSpec(
        node_id=node_id,
        node_kind=node_kind,
        goal_slice=goal_slice,
        parent_node_id=parent_node_id,
        generation=resolved_generation,
        round_id=round_id,
        execution_policy=dict(execution_policy),
        reasoning_profile=dict(reasoning_profile),
        budget_profile=dict(budget_profile),
        allowed_actions=resolved_allowed_actions,
        required_output_paths=resolved_required_output_paths,
        workspace_root=str(resolved_workspace_root),
        codex_home="",
        depends_on_node_ids=resolved_depends_on,
        activation_condition=str(activation_condition or ""),
        activation_rationale=str(activation_rationale or ""),
        runtime_state={},
        delegation_ref=delegation_rel.as_posix(),
        result_sink_ref=resolved_result_sink_ref,
        lineage_ref=resolved_lineage_ref,
        status=status,
    )
    node_record = node.to_dict()
    delegation_payload = {
        "node_id": node.node_id,
        "parent_node_id": parent_node_id,
        "node_kind": node.node_kind,
        "goal_slice": goal_slice,
        "round_id": round_id,
        "execution_policy": dict(node_record["execution_policy"]),
        "reasoning_profile": dict(node_record["reasoning_profile"]),
        "budget_profile": dict(node_record["budget_profile"]),
        "allowed_actions": list(node.allowed_actions),
        "required_output_paths": list(node.required_output_paths),
        "workspace_root": str(node.workspace_root),
        "codex_home": str(node.codex_home),
        "depends_on_node_ids": list(node.depends_on_node_ids),
        "activation_condition": str(node.activation_condition),
        "activation_rationale": str(node.activation_rationale),
        "result_sink_ref": node.result_sink_ref,
        "lineage_ref": node.lineage_ref,
    }
    (delegation_dir / f"{node.node_id}.json").write_text(
        json.dumps(delegation_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    kernel_state.register_node(node)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    path = state_root / "state" / f"{node.node_id}.json"
    validate_repo_object("LoopSystemNodeSpec.schema.json", node_record)
    path.write_text(json.dumps(node_record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return node


def load_child_runtime_context(state_root: Path, node_id: str) -> dict[str, Any]:
    """Load the frozen delegation plus node-local context for one materialized child."""

    state_root = require_runtime_root(state_root)
    node_path = state_root / "state" / f"{node_id}.json"
    delegation_path = state_root / "state" / "delegations" / f"{node_id}.json"
    node_payload = json.loads(node_path.read_text(encoding="utf-8"))
    delegation_payload = json.loads(delegation_path.read_text(encoding="utf-8"))
    return {
        "node": node_payload,
        "node_ref": str(node_path.resolve()),
        "delegation": delegation_payload,
        "delegation_ref": str(delegation_path.resolve()),
        "workspace_root": str(node_payload.get("workspace_root") or ""),
        "codex_home": str(node_payload.get("codex_home") or ""),
    }
