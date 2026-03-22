"""Shared structured control-intent helpers.

Critical runtime routing must not infer control authority from free-form prose.
This module provides a small shared template for machine-choice controls and
machine-only activation gates. Free-form text may still be preserved as
rationale, but it must never become an executable control decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class MachineChoiceSpec:
    control_id: str
    field_name: str
    allowed_values: tuple[str, ...]
    default_value: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MachineChoiceResolution:
    control_id: str
    value: str
    source: str
    raw_value: str


WORKFLOW_SCOPE_SPEC = MachineChoiceSpec(
    control_id="workflow_scope",
    field_name="workflow_scope",
    allowed_values=("generic", "whole_paper_formalization"),
    default_value="generic",
)

ARTIFACT_SCOPE_SPEC = MachineChoiceSpec(
    control_id="artifact_scope",
    field_name="artifact_scope",
    allowed_values=("task", "slice"),
    default_value="task",
)

TERMINAL_AUTHORITY_SCOPE_SPEC = MachineChoiceSpec(
    control_id="terminal_authority_scope",
    field_name="terminal_authority_scope",
    allowed_values=("local", "whole_paper"),
    default_value="local",
)


def resolve_machine_choice(payload: Mapping[str, Any] | None, spec: MachineChoiceSpec) -> MachineChoiceResolution:
    mapping = dict(payload or {})
    candidates = [spec.field_name, *list(spec.aliases)]
    for key in candidates:
        raw = str(mapping.get(key) or "").strip().lower()
        if not raw:
            continue
        if raw in spec.allowed_values:
            return MachineChoiceResolution(
                control_id=spec.control_id,
                value=raw,
                source="explicit",
                raw_value=raw,
            )
        return MachineChoiceResolution(
            control_id=spec.control_id,
            value=spec.default_value,
            source="defaulted_invalid",
            raw_value=raw,
        )
    return MachineChoiceResolution(
        control_id=spec.control_id,
        value=spec.default_value,
        source="defaulted_missing",
        raw_value="",
    )


def normalize_machine_choice(value: Any, spec: MachineChoiceSpec) -> str:
    raw = str(value or "").strip().lower()
    if raw in spec.allowed_values:
        return raw
    return spec.default_value


def normalize_activation_condition(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if not normalized.startswith("after:"):
        return ""
    parts = normalized.split(":")
    if len(parts) != 3:
        return ""
    _, dependency_node_id, requirement = parts
    dependency_node_id = str(dependency_node_id or "").strip()
    requirement = str(requirement or "").strip().lower()
    if not dependency_node_id:
        return ""
    if requirement in {"terminal", "completed", "failed"}:
        return f"after:{dependency_node_id}:{requirement}"
    if requirement.startswith("split_accepted_and_") and requirement.endswith("_released"):
        return f"after:{dependency_node_id}:{requirement}"
    return ""


def is_machine_activation_condition(value: Any) -> bool:
    return bool(normalize_activation_condition(value))
