#!/usr/bin/env python3
"""Validate the first-wave LOOP system placeholder contracts and protocol anchors."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-contracts][FAIL] {msg}", file=sys.stderr)
    return 2


def _require_text(path: Path, needles: list[str]) -> str:
    if not path.exists():
        raise FileNotFoundError(path)
    text = path.read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            raise ValueError(f"{path.relative_to(ROOT)} missing required text: {needle!r}")
    return text


def main() -> int:
    try:
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_KERNEL_CONTRACT.md",
            [
                "IF-1",
                "kernel",
                "split",
                "activate",
                "merge",
                "resume",
                "reap",
                "LoopSystemKernelState.schema.json",
                "kernel_state.json",
                "state_root",
                "outside `.loop/` must be rejected",
                "query_recent_audit_events",
                "route_user_requirements",
                "initialize_evaluator_runtime",
                "bootstrap_first_implementer_node",
                "bootstrap_first_implementer_from_endpoint",
                "child_runtime_status_from_launch_result_ref",
                "recover_orphaned_active_node",
                "terminal evaluator-backed result",
                "RECOVERY_REQUIRED",
                "not terminal evaluator-backed results",
                "liveness, status, and runtime-recovery",
                "external publish target outside the implementer project folder",
                "must not be treated as root acceptance",
                "preserve the evaluator verdict honestly",
                "orphaned ACTIVE",
                "runtime attachment",
                "still-live PID",
                "same-node orphaned-active retry",
                "EvaluatorRunState.json",
                "same evaluator run",
                "frozen checker graph",
                "deferred split",
                "PLANNED",
                "deferred activation",
                "completed deferred source",
                "runtime attachment state",
            ],
        )
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_NODE_CONTRACT.md",
            [
                "IF-2",
                "node_id",
                "generation",
                "execution_policy",
                "reasoning_profile",
                "allowed_actions",
                "workspace_root",
                "agent_provider",
                "sandbox_mode",
                "thinking_budget",
                "node-local context",
                "child-dispatch",
                "workspace/",
                "LoopSystemNodeSpec.schema.json",
                "state/delegations/",
                "runtime_state",
                "depends_on_node_ids",
                "activation_condition",
                "accepted deferred activation",
                "completed deferred source node proposes a merge",
                "accepted deferred merge",
                "FrozenHandoff.json",
                "CHILD_PROMPT.md",
                "LoopOrphanedActiveRecoveryRequest.schema.json",
                "LoopOrphanedActiveRecoveryResult.schema.json",
                "child_runtime_status_from_launch_result_ref",
                "bootstrap_first_implementer_from_endpoint",
            ],
        )
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_EVALUATOR_NODE_CONTRACT.md",
            [
                "IF-3",
                "checker",
                "ordinary-test",
                "AI-as-User",
                "reviewer",
                "run_root/.loop/<role>/runs/",
                "dedicated role-specific `cwd`",
                "structured-exception",
                "BUG",
                "STUCK",
                "ERROR",
                "LoopSystemEvaluatorResult.schema.json",
                "LoopSystemEvaluatorSubmission.schema.json",
                "diagnostics",
                "evaluator-side self-caused",
                "must not override the default Codex home",
                "product repo root to `PYTHONPATH`",
                "must scrub parent `CODEX_*` session/transport env vars",
                "provider_transport",
                "provider_capacity",
                "provider_runtime",
                "provider_quota",
                "bounded lane retry",
                "EvaluatorRunState.json",
                "RECOVERY_REQUIRED",
                "machine-readable recovery metadata",
                "freeze the checker-produced lane plan",
                "skip already-completed lanes",
                "reviewer only after every lane is terminal",
            ],
        )
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_CONTROL_ENVELOPE_CONTRACT.md",
            [
                "IF-4",
                "split request",
                "activate request",
                "merge request",
                "resume",
                "retry",
                "relaunch",
                "local control",
                "heartbeat",
                "terminal result",
                "LoopSystemControlEnvelope.schema.json",
                "LoopSystemLocalControlDecision.schema.json",
                "LoopSystemDispatchStatus.schema.json",
                "LoopSystemNodeTerminalResult.schema.json",
                "payload_type",
                "rejected",
            ],
        )
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_STATE_QUERY_CONTRACT.md",
            [
                "IF-5",
                "delegation",
                "evaluator lane",
                "active",
                "planned child",
                "LoopSystemKernelState.schema.json",
                "LoopSystemAuthorityView.schema.json",
                "query_authority_view",
                "active child",
                "leave `planned_child_nodes`",
                "accepts deferred merge",
            ],
        )
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_RUNTIME_RECOVERY_CONTRACT.md",
            [
                "runtime-recovery",
                "resume",
                "retry",
                "relaunch",
                "kernel",
                "self-attribution",
                "self-repair",
                "ACTIVE",
                "runtime-loss",
                "same evaluator run",
                "EvaluatorRunState.json",
                "frozen checker graph",
                "RECOVERY_REQUIRED",
                "recover_orphaned_active_node",
            ],
        )
        _require_text(
            ROOT / "docs/contracts/LOOP_SYSTEM_AUDIT_EXPERIENCE_CONTRACT.md",
            [
                "IF-6",
                "self-attribution",
                "self-repair",
                "experience_assets",
                ".loop/audit/",
                "LoopSystemAuditExperienceView.schema.json",
                "query_recent_audit_events",
                "query_audit_experience_view",
                "hard rules",
            ],
        )

        _require_text(
            ROOT / "loop_product/protocols/node.py",
            [
                "class NodeSpec",
                "node_id",
                "parent_node_id",
                "generation",
                "round_id",
                "execution_policy",
                "reasoning_profile",
            ],
        )
        _require_text(
            ROOT / "loop_product/protocols/control_envelope.py",
            ["class ControlEnvelope", "payload_type", "status", "proposal", "accepted"],
        )
        _require_text(
            ROOT / "loop_product/protocols/evaluator.py",
            [
                "class EvaluatorVerdict",
                "class EvaluatorNodeSubmission",
                "STRUCTURED_EXCEPTION",
                "ERROR",
                "BUG",
                "STUCK",
                "FAIL",
                "PASS",
            ],
        )
        _require_text(
            ROOT / "loop_product/protocols/topology.py",
            ["class TopologyMutation", "split", "activate", "merge", "resume", "retry", "relaunch", "to_envelope"],
        )
        _require_text(
            ROOT / "loop_product/protocols/control_objects.py",
            ["class ChildDispatchStatus", "class LocalControlDecision", "class NodeTerminalResult", "to_envelope"],
        )
        _require_text(
            ROOT / "loop_product/kernel/query.py",
            ["query_authority_view", "active_child_nodes", "planned_child_nodes", "node_graph", "node_lifecycle"],
        )
        _require_text(
            ROOT / "loop_product/kernel/audit.py",
            ["query_recent_audit_events", "query_audit_experience_view", "experience_assets", "hard_rules"],
        )
    except (FileNotFoundError, ValueError) as exc:
        return _fail(str(exc))

    print("[loop-system-contracts] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
