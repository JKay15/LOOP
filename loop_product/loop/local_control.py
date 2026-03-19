"""Local control decisions for a node round."""

from __future__ import annotations

from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
from loop_product.protocols.control_objects import LocalControlAction, LocalControlDecision
from loop_product.protocols.node import NodeSpec


def decide_next_action(result: EvaluatorResult) -> str:
    """Choose the next action from an evaluator verdict."""

    if result.verdict == EvaluatorVerdict.PASS:
        return "COMPLETE"
    if result.retryable:
        return "RETRY"
    if result.verdict == EvaluatorVerdict.BUG:
        return "ESCALATE_BUG"
    if result.verdict == EvaluatorVerdict.STUCK:
        return "ESCALATE_STUCK"
    return "FAIL_CLOSED"


def build_local_control_decision(node: NodeSpec, result: EvaluatorResult) -> LocalControlDecision:
    """Materialize a structured local control decision from evaluator feedback."""

    return LocalControlDecision(
        node_id=node.node_id,
        round_id=node.round_id,
        generation=node.generation,
        action=LocalControlAction(decide_next_action(result)),
        source_lane=result.lane,
        summary=result.summary,
        evidence_refs=list(result.evidence_refs),
        retryable=result.retryable,
        self_attribution=str(result.diagnostics.get("self_attribution") or ""),
        self_repair=str(result.diagnostics.get("self_repair") or ""),
    )
