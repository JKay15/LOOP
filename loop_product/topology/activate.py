"""Deferred-child activation review helpers."""

from __future__ import annotations

from typing import Any

from pathlib import Path

from loop_product.kernel.state import ACTIVE_NODE_STATUSES, KernelState, authoritative_node_dependency_ready
from loop_product.protocols.topology import TopologyMutation
from loop_product.runtime_paths import require_runtime_root
from loop_product.topology.budget import normalized_complexity_budget

_TERMINAL_STATUSES = {"COMPLETED", "FAILED"}


def build_activate_request(
    source_node_id: str,
    *,
    reason: str = "",
    payload: dict[str, Any] | None = None,
) -> TopologyMutation:
    """Build a structured deferred-child activation request."""

    return TopologyMutation.activate(source_node_id, reason=reason, payload=dict(payload or {}))


def _dependency_ready_for_activation(
    *,
    kernel_state: KernelState,
    dependency_node_id: str,
    state_root: Path | None = None,
) -> bool:
    dependency = dict(kernel_state.nodes.get(str(dependency_node_id)) or {})
    if not dependency:
        return False
    if state_root is not None:
        if authoritative_node_dependency_ready(
            state_root=require_runtime_root(state_root),
            node_payload=dependency,
        ):
            return True
    return str(dependency.get("status") or "") in _TERMINAL_STATUSES


def _condition_satisfied(kernel_state: KernelState, condition: str, *, state_root: Path | None = None) -> tuple[bool, str]:
    normalized = str(condition or "").strip()
    if not normalized:
        return True, ""
    if not normalized.startswith("after:"):
        return False, f"unsupported activation condition {normalized!r}"
    parts = normalized.split(":")
    if len(parts) != 3:
        return False, f"unsupported activation condition {normalized!r}"
    _, dependency_node_id, requirement = parts
    dependency = dict(kernel_state.nodes.get(str(dependency_node_id)) or {})
    if not dependency:
        return False, f"activation dependency {dependency_node_id!r} is missing"
    dependency_status = str(dependency.get("status") or "")
    requirement = str(requirement or "").strip().lower()
    if requirement == "terminal":
        return _dependency_ready_for_activation(
            kernel_state=kernel_state,
            dependency_node_id=dependency_node_id,
            state_root=state_root,
        ), ""
    if requirement == "completed":
        return dependency_status == "COMPLETED", ""
    if requirement == "failed":
        return dependency_status == "FAILED", ""
    return False, f"unsupported activation requirement {requirement!r}"


def review_activate_request(
    kernel_state: KernelState,
    mutation: TopologyMutation,
    *,
    state_root: Path | None = None,
) -> dict[str, Any]:
    """Review a deferred-child activation request under current kernel authority."""

    source = dict(kernel_state.nodes.get(mutation.source_node_id) or {})
    source_status = str(source.get("status") or "")
    depends_on = [str(item).strip() for item in (source.get("depends_on_node_ids") or []) if str(item).strip()]
    activation_condition = str(source.get("activation_condition") or "").strip()
    dependency_statuses = {
        node_id: str(dict(kernel_state.nodes.get(node_id) or {}).get("status") or "")
        for node_id in depends_on
    }
    dependencies_terminal = bool(depends_on) and all(
        _dependency_ready_for_activation(
            kernel_state=kernel_state,
            dependency_node_id=node_id,
            state_root=state_root,
        )
        for node_id in depends_on
    )
    condition_ok, condition_error = _condition_satisfied(kernel_state, activation_condition, state_root=state_root)
    active_now = sum(
        1 for node in kernel_state.nodes.values() if str(node.get("status") or "") in ACTIVE_NODE_STATUSES
    )
    max_active_nodes = int(normalized_complexity_budget(dict(kernel_state.complexity_budget)).get("max_active_nodes") or 0)

    checks = [
        {
            "check_id": "A1_planned_child_exists",
            "passed": bool(source) and source_status == "PLANNED",
            "detail": "activation requires an existing deferred child currently in PLANNED status",
        },
        {
            "check_id": "A2_dependencies_satisfied",
            "passed": dependencies_terminal,
            "detail": "activation requires every declared depends_on_node_ids entry to already be terminal",
        },
        {
            "check_id": "A3_activation_condition",
            "passed": condition_ok,
            "detail": "activation_condition must be supported and satisfied before activation",
        },
        {
            "check_id": "A4_active_node_budget",
            "passed": active_now + 1 <= max_active_nodes,
            "detail": "activating a deferred child must stay within max_active_nodes",
        },
    ]
    accepted = all(bool(item["passed"]) for item in checks)
    summary = (
        f"accepted deferred activation for {mutation.source_node_id}"
        if accepted
        else f"rejected deferred activation for {mutation.source_node_id}: kernel review failed"
    )
    if condition_error:
        summary = f"{summary} ({condition_error})"
    return {
        "decision": "ACCEPT" if accepted else "REJECT",
        "summary": summary,
        "checks": checks,
        "normalized_source_node_id": mutation.source_node_id if accepted else "",
        "dependency_statuses": dependency_statuses,
        "condition_error": condition_error,
    }
