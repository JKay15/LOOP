"""Shared structured control-intent helpers.

Critical runtime routing must not infer control authority from free-form prose.
This module provides a small shared template for machine-choice controls and
machine-only activation gates. Free-form text may still be preserved as
rationale, but it must never become an executable control decision.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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

SPLIT_REQUEST_PROPOSAL_RELPATH = "split/SPLIT_REQUEST_PROPOSAL.json"
SPLIT_REQUEST_RESULT_RELPATH = "split/SPLIT_REQUEST_SUBMISSION_RESULT.json"
ACTIVATE_REQUEST_PROPOSAL_RELPATH = "activate/ACTIVATE_REQUEST_PROPOSAL.json"
ACTIVATE_REQUEST_RESULT_RELPATH = "activate/ACTIVATE_REQUEST_SUBMISSION_RESULT.json"


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


def inherit_machine_choice(
    value: Any,
    spec: MachineChoiceSpec,
    *,
    inherited_value: Any = None,
    sticky_inherited_values: tuple[str, ...] = (),
) -> str:
    inherited = (
        normalize_machine_choice(inherited_value, spec)
        if inherited_value not in {None, ""}
        else spec.default_value
    )
    requested = inherited if value in {None, ""} else normalize_machine_choice(value, spec)
    if inherited in sticky_inherited_values:
        return inherited
    return requested


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


def default_startup_required_output_paths(*, workflow_scope: str, artifact_scope: str, terminal_authority_scope: str) -> list[str]:
    normalized_workflow = normalize_machine_choice(workflow_scope, WORKFLOW_SCOPE_SPEC)
    normalized_artifact_scope = normalize_machine_choice(artifact_scope, ARTIFACT_SCOPE_SPEC)
    normalized_terminal_scope = normalize_machine_choice(
        terminal_authority_scope,
        TERMINAL_AUTHORITY_SCOPE_SPEC,
    )
    if (
        normalized_workflow == "whole_paper_formalization"
        and normalized_artifact_scope == "task"
        and normalized_terminal_scope == "whole_paper"
    ):
        return [
            "README.md",
            "TRACEABILITY.md",
            "WHOLE_PAPER_STATUS.json",
            "extraction/source_structure.json",
            "extraction/theorem_inventory.json",
            "analysis/internal_dependency_graph.json",
            "analysis/block_partition_draft.json",
            "external_dependencies/EXTERNAL_DEPENDENCY_LEDGER.json",
            SPLIT_REQUEST_PROPOSAL_RELPATH,
            "split/SPLIT_REVIEW.json",
        ]
    if (
        normalized_workflow == "whole_paper_formalization"
        and normalized_artifact_scope == "slice"
        and normalized_terminal_scope == "local"
    ):
        return [
            "README.md",
            "TRACEABILITY.md",
            "SLICE_STATUS.json",
            "analysis/SLICE_BOUNDARY.json",
        ]
    return []


def default_progress_checkpoints(*, workflow_scope: str, artifact_scope: str, terminal_authority_scope: str) -> list[dict[str, Any]]:
    normalized_workflow = normalize_machine_choice(workflow_scope, WORKFLOW_SCOPE_SPEC)
    normalized_artifact_scope = normalize_machine_choice(artifact_scope, ARTIFACT_SCOPE_SPEC)
    normalized_terminal_scope = normalize_machine_choice(
        terminal_authority_scope,
        TERMINAL_AUTHORITY_SCOPE_SPEC,
    )
    if (
        normalized_workflow == "whole_paper_formalization"
        and normalized_artifact_scope == "task"
        and normalized_terminal_scope == "whole_paper"
    ):
        return [
            {
                "checkpoint_id": "whole_paper_source_split_decision",
                "description": (
                    "After the deterministic source control bundle is materialized, explicitly submit or decline the staged split proposal."
                ),
                "required_any_of": [
                    SPLIT_REQUEST_RESULT_RELPATH,
                    "split/SPLIT_DECLINE_REASON.md",
                ],
                "window_s": 300.0,
            }
        ]
    if (
        normalized_workflow == "whole_paper_formalization"
        and normalized_artifact_scope == "slice"
        and normalized_terminal_scope == "local"
    ):
        return [
            {
                "checkpoint_id": "whole_paper_slice_execution_decision",
                "description": (
                    "After the deterministic slice startup bundle is materialized, record a slice-local execution decision or submit a further split proposal before extended theorem search."
                ),
                "required_any_of": [
                    "analysis/SLICE_EXECUTION_DECISION.json",
                    SPLIT_REQUEST_RESULT_RELPATH,
                    "split/SPLIT_DECLINE_REASON.md",
                ],
                "window_s": 300.0,
            }
        ]
    return []


def default_control_surface_root(live_artifact_root: str | Path, *, workspace_mirror_relpath: str = "") -> Path:
    root = Path(live_artifact_root).expanduser().resolve()
    mirror_relpath = str(workspace_mirror_relpath or "").strip()
    if mirror_relpath and Path(mirror_relpath).suffix:
        return root.parent
    return root


def default_split_request_refs(live_artifact_root: str | Path, *, workspace_mirror_relpath: str = "") -> tuple[Path, Path]:
    root = default_control_surface_root(
        live_artifact_root,
        workspace_mirror_relpath=workspace_mirror_relpath,
    )
    return root / SPLIT_REQUEST_PROPOSAL_RELPATH, root / SPLIT_REQUEST_RESULT_RELPATH


def default_activate_request_refs(live_artifact_root: str | Path, *, workspace_mirror_relpath: str = "") -> tuple[Path, Path]:
    root = default_control_surface_root(
        live_artifact_root,
        workspace_mirror_relpath=workspace_mirror_relpath,
    )
    return root / ACTIVATE_REQUEST_PROPOSAL_RELPATH, root / ACTIVATE_REQUEST_RESULT_RELPATH
