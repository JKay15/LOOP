"""Kernel-owned routing for new user requirements."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from loop_product.kernel.authority import kernel_internal_authority
from loop_product.kernel.audit import record_audit_event
from loop_product.kernel.state import persist_kernel_state, query_kernel_state_object


def route_user_requirements(state_root: Path, requirements: str | Iterable[str]) -> list[str]:
    """Persist newly routed user requirements into authoritative kernel state."""

    if isinstance(requirements, str):
        raw_items = [requirements]
    else:
        raw_items = [str(item) for item in requirements]

    normalized = [item.strip() for item in raw_items if str(item).strip()]
    if not normalized:
        return []

    authority = kernel_internal_authority()
    kernel_state = query_kernel_state_object(state_root, continue_deferred=False)
    routed: list[str] = []
    for requirement in normalized:
        if requirement not in kernel_state.active_requirements:
            kernel_state.active_requirements.append(requirement)
            routed.append(requirement)

    if not routed:
        return []

    persist_kernel_state(state_root, kernel_state, authority=authority)
    record_audit_event(
        state_root,
        "user_requirements_routed",
        {
            "node_id": kernel_state.root_node_id,
            "summary": f"accept a newly routed user requirement: {', '.join(routed)}",
            "requirements": routed,
        },
    )
    return routed
