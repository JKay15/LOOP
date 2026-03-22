"""Node protocol definitions for the wave-1 LOOP repo bootstrap."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from loop_product.control_intent import (
    ARTIFACT_SCOPE_SPEC,
    TERMINAL_AUTHORITY_SCOPE_SPEC,
    WORKFLOW_SCOPE_SPEC,
    normalize_machine_choice,
)


DEFAULT_AGENT_PROVIDER = "codex_cli"
DEFAULT_SANDBOX_MODE = "danger-full-access"
DEFAULT_THINKING_BUDGET = "medium"


class NodeStatus(str, Enum):
    """Lifecycle states for a LOOP node."""

    PLANNED = "PLANNED"
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class RuntimeAttachmentState(str, Enum):
    """Durable attachment state for the backing runtime of a node."""

    UNOBSERVED = "UNOBSERVED"
    ATTACHED = "ATTACHED"
    LOST = "LOST"
    TERMINAL = "TERMINAL"


@dataclass(slots=True)
class NodeSpec:
    """Canonical node spec for child-dispatch and kernel accounting."""

    node_id: str
    node_kind: str
    goal_slice: str
    parent_node_id: str | None
    generation: int
    round_id: str
    execution_policy: dict[str, Any]
    reasoning_profile: dict[str, Any]
    budget_profile: dict[str, Any]
    allowed_actions: list[str] = field(default_factory=list)
    workflow_scope: str = WORKFLOW_SCOPE_SPEC.default_value
    artifact_scope: str = ARTIFACT_SCOPE_SPEC.default_value
    terminal_authority_scope: str = TERMINAL_AUTHORITY_SCOPE_SPEC.default_value
    required_output_paths: list[str] = field(default_factory=list)
    workspace_root: str = ""
    codex_home: str = ""
    depends_on_node_ids: list[str] = field(default_factory=list)
    activation_condition: str = ""
    activation_rationale: str = ""
    runtime_state: dict[str, Any] = field(default_factory=dict)
    delegation_ref: str = ""
    result_sink_ref: str = ""
    lineage_ref: str = ""
    status: NodeStatus = NodeStatus.PLANNED

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["execution_policy"] = normalize_execution_policy(self.execution_policy, node_kind=self.node_kind)
        data["reasoning_profile"] = normalize_reasoning_profile(self.reasoning_profile, node_kind=self.node_kind)
        data["workflow_scope"] = normalize_machine_choice(self.workflow_scope, WORKFLOW_SCOPE_SPEC)
        data["artifact_scope"] = normalize_machine_choice(self.artifact_scope, ARTIFACT_SCOPE_SPEC)
        data["terminal_authority_scope"] = normalize_machine_choice(
            self.terminal_authority_scope,
            TERMINAL_AUTHORITY_SCOPE_SPEC,
        )
        data["runtime_state"] = normalize_runtime_state(self.runtime_state)
        data["status"] = self.status.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "NodeSpec":
        node_kind = str(data["node_kind"])
        return cls(
            node_id=str(data["node_id"]),
            node_kind=node_kind,
            goal_slice=str(data["goal_slice"]),
            parent_node_id=data.get("parent_node_id"),
            generation=int(data["generation"]),
            round_id=str(data["round_id"]),
            execution_policy=normalize_execution_policy(dict(data.get("execution_policy") or {}), node_kind=node_kind),
            reasoning_profile=normalize_reasoning_profile(dict(data.get("reasoning_profile") or {}), node_kind=node_kind),
            budget_profile=dict(data.get("budget_profile") or {}),
            allowed_actions=list(data.get("allowed_actions") or []),
            workflow_scope=normalize_machine_choice(data.get("workflow_scope"), WORKFLOW_SCOPE_SPEC),
            artifact_scope=normalize_machine_choice(data.get("artifact_scope"), ARTIFACT_SCOPE_SPEC),
            terminal_authority_scope=normalize_machine_choice(
                data.get("terminal_authority_scope"),
                TERMINAL_AUTHORITY_SCOPE_SPEC,
            ),
            required_output_paths=[str(item) for item in (data.get("required_output_paths") or []) if str(item).strip()],
            workspace_root=str(data.get("workspace_root") or ""),
            codex_home=str(data.get("codex_home") or ""),
            depends_on_node_ids=[str(item) for item in (data.get("depends_on_node_ids") or [])],
            activation_condition=str(data.get("activation_condition") or ""),
            activation_rationale=str(data.get("activation_rationale") or ""),
            runtime_state=normalize_runtime_state(dict(data.get("runtime_state") or {})),
            delegation_ref=str(data.get("delegation_ref") or ""),
            result_sink_ref=str(data.get("result_sink_ref") or ""),
            lineage_ref=str(data.get("lineage_ref") or ""),
            status=NodeStatus(str(data.get("status") or NodeStatus.PLANNED.value)),
        )


def normalize_execution_policy(policy: dict[str, Any], *, node_kind: str) -> dict[str, Any]:
    normalized = dict(policy or {})
    legacy_provider = str(normalized.pop("provider_selection", "") or "").strip()
    if legacy_provider and not str(normalized.get("agent_provider") or "").strip():
        normalized["agent_provider"] = legacy_provider
    normalized.setdefault("agent_provider", DEFAULT_AGENT_PROVIDER)
    normalized.setdefault("sandbox_mode", DEFAULT_SANDBOX_MODE)
    if node_kind == "kernel":
        normalized.setdefault("mode", "kernel")
    return normalized


def normalize_reasoning_profile(profile: dict[str, Any], *, node_kind: str) -> dict[str, Any]:
    normalized = dict(profile or {})
    normalized.setdefault("thinking_budget", DEFAULT_THINKING_BUDGET)
    normalized.setdefault("role", node_kind or "worker")
    return normalized


def normalize_runtime_state(state: dict[str, Any] | None = None) -> dict[str, Any]:
    normalized = dict(state or {})
    attachment_state = str(normalized.get("attachment_state") or RuntimeAttachmentState.UNOBSERVED.value).strip().upper()
    normalized["attachment_state"] = RuntimeAttachmentState(attachment_state).value
    normalized["observed_at"] = str(normalized.get("observed_at") or "")
    normalized["summary"] = str(normalized.get("summary") or "")
    normalized["observation_kind"] = str(normalized.get("observation_kind") or "")
    normalized["evidence_refs"] = [str(item) for item in (normalized.get("evidence_refs") or [])]
    return normalized
