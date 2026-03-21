"""Evaluator protocol surfaces for the wave-1 LOOP repo bootstrap."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


class EvaluatorVerdict(str, Enum):
    """Fail-closed evaluator verdicts."""

    PASS = "PASS"
    FAIL = "FAIL"
    STRUCTURED_EXCEPTION = "STRUCTURED_EXCEPTION"
    BUG = "BUG"
    STUCK = "STUCK"
    ERROR = "ERROR"


@dataclass(slots=True)
class EvaluatorResult:
    """Structured evaluator result for a single round."""

    verdict: EvaluatorVerdict
    lane: str
    summary: str
    evidence_refs: list[str] = field(default_factory=list)
    retryable: bool = False
    diagnostics: dict[str, Any] = field(default_factory=dict)
    recovery: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["verdict"] = self.verdict.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EvaluatorResult":
        return cls(
            verdict=EvaluatorVerdict(str(data["verdict"])),
            lane=str(data["lane"]),
            summary=str(data["summary"]),
            evidence_refs=list(data.get("evidence_refs") or []),
            retryable=bool(data.get("retryable", False)),
            diagnostics=dict(data.get("diagnostics") or {}),
            recovery=dict(data.get("recovery") or {}),
        )


@dataclass(slots=True)
class EvaluatorNodeSubmission:
    """Structured node -> evaluator submission for the real evaluator-node boundary."""

    evaluation_id: str
    evaluator_node_id: str
    target_node_id: str
    parent_node_id: str | None
    generation: int
    round_id: str
    lineage_ref: str
    goal_slice: str
    workspace_root: str
    output_root: str
    implementation_package_ref: str
    product_manual_ref: str
    required_output_paths: list[str] = field(default_factory=list)
    final_effects_text_ref: str = ""
    final_effect_requirements: list[dict[str, Any]] = field(default_factory=list)
    role_requirements: dict[str, Any] = field(default_factory=dict)
    agent_execution: dict[str, Any] = field(default_factory=dict)
    ai_user_scheduler: dict[str, Any] = field(default_factory=dict)
    context_refs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        for key in ("required_output_paths", "final_effects_text_ref", "final_effect_requirements", "ai_user_scheduler", "context_refs"):
            value = data.get(key)
            if value in ("", [], {}):
                data.pop(key, None)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "EvaluatorNodeSubmission":
        return cls(
            evaluation_id=str(data["evaluation_id"]),
            evaluator_node_id=str(data["evaluator_node_id"]),
            target_node_id=str(data["target_node_id"]),
            parent_node_id=data.get("parent_node_id"),
            generation=int(data["generation"]),
            round_id=str(data["round_id"]),
            lineage_ref=str(data["lineage_ref"]),
            goal_slice=str(data["goal_slice"]),
            workspace_root=str(data["workspace_root"]),
            output_root=str(data["output_root"]),
            implementation_package_ref=str(data["implementation_package_ref"]),
            product_manual_ref=str(data["product_manual_ref"]),
            required_output_paths=[str(item) for item in (data.get("required_output_paths") or []) if str(item).strip()],
            final_effects_text_ref=str(data.get("final_effects_text_ref") or ""),
            final_effect_requirements=[dict(item) for item in (data.get("final_effect_requirements") or [])],
            role_requirements=dict(data.get("role_requirements") or {}),
            agent_execution=dict(data.get("agent_execution") or {}),
            ai_user_scheduler=dict(data.get("ai_user_scheduler") or {}),
            context_refs=[str(item) for item in (data.get("context_refs") or []) if str(item).strip()],
        )
