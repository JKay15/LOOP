"""Evaluator-node adapters for LOOP child nodes."""

from __future__ import annotations

import argparse
import json
import hashlib
import re
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from loop_product import build_endpoint_clarification_part1_input, run_evaluator
from loop_product.evaluator.agent_execution_policy import validate_evaluator_agent_execution, validate_repo_shipped_role_agent_cmd
from loop_product.kernel.submit import submit_control_envelope
from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
from loop_product.protocols.evaluator import EvaluatorNodeSubmission, EvaluatorResult, EvaluatorVerdict
from loop_product.protocols.node import NodeSpec
from loop_product.runtime_paths import (
    product_contract_path,
    product_package_root,
    require_runtime_root,
)

_PACKAGE_ROOT = product_package_root()
_ENDPOINT_MANUAL_PATH = product_contract_path("LOOP_ENDPOINT_CLARIFICATION_PART1_PRODUCT_MANUAL.md")
_ENDPOINT_FINAL_EFFECTS_PATH = product_contract_path("LOOP_ENDPOINT_CLARIFICATION_PART1_FINAL_EFFECTS.md")
_DEFAULT_AGENT_EXECUTION: dict[str, dict[str, str]] = {
    "default": {
        "agent_provider": "codex_cli",
        "sandbox_mode": "danger-full-access",
        "reasoning_effort": "high",
    },
    "reviewer": {
        "agent_provider": "codex_cli",
        "sandbox_mode": "danger-full-access",
        "reasoning_effort": "high",
    },
}
_GENERIC_IMPLEMENTER_ROLE_REQUIREMENTS: dict[str, str] = {
    "checker": (
        "Normalize the frozen task final-effects text into explicit blocking requirements and a requirement graph. "
        "Only repair wording when the fix is obvious. Do not act as test_ai, ai_user, or reviewer."
    ),
    "test_designer": (
        "Design only documented-surface ordinary tests against the task-scoped product manual and workspace mirror "
        "artifact. Do not rewrite final effects, checker grouping, or reviewer verdicts."
    ),
    "ai_user": (
        "Behave like a real user validating the documented workspace mirror artifact against the frozen final effects. "
        "Follow the checker-declared evaluation units. Do not redesign requirements, checker grouping, or reviewer verdicts."
    ),
    "reviewer": (
        "Judge only from reviewer-visible checker, test_ai, and ai_user artifacts whether the frozen final effects were "
        "met. Do not invent new requirements or retest the product outside the recorded evidence."
    ),
}
_SMOKE_MANUAL_TEMPLATE = """# LOOP System Smoke Manual

This manual exists only to drive the bounded wave-1 smoke evaluator.

## Goal

- Exercise the real evaluator-node runtime around the child implementer.
- Keep the run deterministic by using the repo-local smoke role agent.
- Preserve reviewer-visible artifacts for checker, test_designer, ai_user, reviewer, supervisor, and evaluation.

## Documented smoke command

`python -m loop_product.kernel.entry --smoke --state-root <state-root>`
"""
_SMOKE_FINAL_EFFECTS_TEMPLATE = """# LOOP System Smoke Final Effects

- The current smoke round must execute through the real evaluator-node runtime and preserve request, supervisor, and evaluation report artifacts.

- The evaluator runtime must preserve checker, ordinary-test, AI-as-User, and reviewer role artifacts for reviewer-visible inspection.

- For smoke round {round_index}, the reviewer-visible verdict must be `{expected_verdict}` so the child loop can prove bounded retry handling end-to-end.
"""
_REVIEWER_VERDICT_RE = re.compile(
    r"VERDICT\s*:\s*(PASS|FAIL|STRUCTURED_EXCEPTION|BUG|STUCK|ERROR|INCONCLUSIVE|UNKNOWN)\b",
    re.IGNORECASE,
)


def _parse_simple_reviewer_verdict(text: str) -> str:
    sample = str(text or "")
    match = _REVIEWER_VERDICT_RE.search(sample)
    if match:
        return match.group(1).upper()
    for token in ("STRUCTURED_EXCEPTION", "ERROR", "BUG", "STUCK", "FAIL", "PASS", "INCONCLUSIVE"):
        if re.search(rf"\b{token}\b", sample, flags=re.IGNORECASE):
            return token
    return "UNKNOWN"


def _override_agent_execution_with_cmd(agent_execution: dict[str, Any], *, agent_cmd: str) -> dict[str, Any]:
    validate_repo_shipped_role_agent_cmd(
        agent_cmd,
        context="explicit evaluator role_agent_cmd override",
    )
    normalized: dict[str, Any] = {}
    for role_id, role_cfg_raw in agent_execution.items():
        role_cfg = dict(role_cfg_raw or {})
        role_cfg["agent_cmd"] = agent_cmd
        role_cfg.pop("agent_provider", None)
        role_cfg.pop("agent_profile", None)
        normalized[role_id] = role_cfg
    if "default" not in normalized:
        normalized["default"] = {"agent_cmd": agent_cmd}
    return normalized


def build_evaluator_submission_for_frozen_task(
    *,
    target_node: NodeSpec,
    workspace_root: Path,
    output_root: Path,
    implementation_package_ref: str | Path,
    product_manual_ref: str | Path,
    final_effects_text_ref: str | Path = "",
    final_effect_requirements: Sequence[Mapping[str, Any]] | None = None,
    role_requirements: Mapping[str, Any] | None = None,
    context_refs: Sequence[str | Path] | None = None,
    role_agent_cmd: str | None = None,
    evaluation_id: str | None = None,
) -> EvaluatorNodeSubmission:
    """Build a generic evaluator-node submission for one frozen implementer task."""

    normalized_manual_ref = str(Path(product_manual_ref).resolve())
    normalized_final_effects_ref = str(Path(final_effects_text_ref).resolve()) if str(final_effects_text_ref or "").strip() else ""
    normalized_requirements = [dict(item) for item in (final_effect_requirements or [])]
    if not normalized_final_effects_ref and not normalized_requirements:
        raise ValueError(
            "build_evaluator_submission_for_frozen_task requires final_effects_text_ref or final_effect_requirements"
        )
    agent_execution: dict[str, Any] = {role_id: dict(cfg) for role_id, cfg in _DEFAULT_AGENT_EXECUTION.items()}
    if role_agent_cmd:
        agent_execution = _override_agent_execution_with_cmd(agent_execution, agent_cmd=role_agent_cmd)
    normalized_context_refs = [
        str(Path(item).resolve()) if not Path(str(item)).is_absolute() else str(Path(item).resolve())
        for item in (context_refs or [])
        if str(item).strip()
    ]
    submission_role_requirements = dict(role_requirements or _GENERIC_IMPLEMENTER_ROLE_REQUIREMENTS)
    normalized_output_root = Path(output_root).resolve()
    normalized_workspace_root = Path(workspace_root).resolve()
    normalized_impl_ref = str(Path(implementation_package_ref).resolve())
    return EvaluatorNodeSubmission(
        evaluation_id=evaluation_id or f"implementer_task_{target_node.node_id}",
        evaluator_node_id=f"{target_node.node_id}__evaluator",
        target_node_id=target_node.node_id,
        parent_node_id=target_node.parent_node_id,
        generation=target_node.generation,
        round_id=target_node.round_id,
        lineage_ref=target_node.lineage_ref,
        goal_slice=target_node.goal_slice,
        workspace_root=str(normalized_workspace_root),
        output_root=str(normalized_output_root),
        implementation_package_ref=normalized_impl_ref,
        product_manual_ref=normalized_manual_ref,
        final_effects_text_ref=normalized_final_effects_ref,
        final_effect_requirements=normalized_requirements,
        role_requirements=submission_role_requirements,
        agent_execution=agent_execution,
        ai_user_scheduler={"max_parallel_runs": 1},
        context_refs=normalized_context_refs,
    )


def build_evaluator_submission_for_endpoint_clarification(
    *,
    target_node: NodeSpec,
    workspace_root: Path,
    output_root: Path,
    role_agent_cmd: str | None = None,
) -> EvaluatorNodeSubmission:
    """Build a reusable evaluator-node submission for the accepted front-half product."""

    base_request = build_endpoint_clarification_part1_input(
        workspace_root=workspace_root,
        output_root=output_root,
        product_manual_ref=_ENDPOINT_MANUAL_PATH,
        final_effects_text_ref=_ENDPOINT_FINAL_EFFECTS_PATH,
    )
    agent_execution = dict(base_request.get("agent_execution") or {})
    if role_agent_cmd:
        agent_execution = _override_agent_execution_with_cmd(agent_execution, agent_cmd=role_agent_cmd)
    return EvaluatorNodeSubmission(
        evaluation_id=f"{str(base_request.get('evaluation_id') or 'evaluator')}_{target_node.node_id}",
        evaluator_node_id=f"{target_node.node_id}__evaluator",
        target_node_id=target_node.node_id,
        parent_node_id=target_node.parent_node_id,
        generation=target_node.generation,
        round_id=target_node.round_id,
        lineage_ref=target_node.lineage_ref,
        goal_slice=target_node.goal_slice,
        workspace_root=str(Path(workspace_root).resolve()),
        output_root=str(Path(output_root).resolve()),
        implementation_package_ref=str((_PACKAGE_ROOT / "endpoint" / "clarification.py").resolve()),
        product_manual_ref=str(_ENDPOINT_MANUAL_PATH.resolve()),
        final_effects_text_ref=str(_ENDPOINT_FINAL_EFFECTS_PATH.resolve()),
        role_requirements=dict(base_request.get("role_requirements") or {}),
        agent_execution=agent_execution,
        ai_user_scheduler=dict(base_request.get("ai_user_scheduler") or {}),
        context_refs=[
            str(product_contract_path("LOOP_ENDPOINT_CLARIFICATION_CONTRACT.md")),
            str(product_contract_path("LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_CONTRACT.md")),
        ],
    )


def _materialize_smoke_support_docs(
    *,
    state_root: Path,
    round_index: int,
    expected_verdict: str,
) -> tuple[Path, Path]:
    state_root = require_runtime_root(state_root)
    support_root = state_root / "artifacts" / "smoke_support" / f"round_{round_index}"
    support_root.mkdir(parents=True, exist_ok=True)
    manual_path = support_root / "SMOKE_PRODUCT_MANUAL.md"
    final_effects_path = support_root / "SMOKE_FINAL_EFFECTS.md"
    manual_path.write_text(_SMOKE_MANUAL_TEMPLATE, encoding="utf-8")
    final_effects_path.write_text(
        _SMOKE_FINAL_EFFECTS_TEMPLATE.format(round_index=round_index, expected_verdict=expected_verdict),
        encoding="utf-8",
    )
    return manual_path, final_effects_path


def build_evaluator_submission_for_smoke_round(
    *,
    target_node: NodeSpec,
    state_root: Path,
    workspace_root: Path,
    output_root: Path,
    round_index: int,
    role_agent_cmd: str,
) -> EvaluatorNodeSubmission:
    """Build a real evaluator-node submission for the bounded smoke loop."""

    expected_verdict = "FAIL" if round_index == 1 else "PASS"
    smoke_run_token = hashlib.sha256(str(state_root.resolve()).encode("utf-8")).hexdigest()[:12]
    manual_path, final_effects_path = _materialize_smoke_support_docs(
        state_root=state_root,
        round_index=round_index,
        expected_verdict=expected_verdict,
    )
    agent_execution: dict[str, Any] = {
        "default": {
            "agent_provider": "codex_cli",
            "sandbox_mode": "danger-full-access",
            "reasoning_effort": "high",
        },
        "reviewer": {
            "agent_provider": "codex_cli",
            "sandbox_mode": "danger-full-access",
            "reasoning_effort": "high",
        },
    }
    agent_execution = _override_agent_execution_with_cmd(agent_execution, agent_cmd=role_agent_cmd)
    context_refs = [
        str((state_root / "state" / f"{target_node.node_id}.json").resolve()),
        str((state_root / target_node.delegation_ref).resolve()),
    ]
    return EvaluatorNodeSubmission(
        evaluation_id=f"loop_system_smoke_round_{round_index}_{smoke_run_token}",
        evaluator_node_id=f"{target_node.node_id}__evaluator",
        target_node_id=target_node.node_id,
        parent_node_id=target_node.parent_node_id,
        generation=target_node.generation,
        round_id=f"{target_node.round_id}.{round_index}",
        lineage_ref=target_node.lineage_ref,
        goal_slice=f"{target_node.goal_slice} (smoke round {round_index})",
        workspace_root=str(Path(workspace_root).resolve()),
        output_root=str(Path(output_root).resolve()),
        implementation_package_ref=str((_PACKAGE_ROOT / "kernel" / "entry.py").resolve()),
        product_manual_ref=str(manual_path.resolve()),
        final_effects_text_ref=str(final_effects_path.resolve()),
        role_requirements={
            "checker": (
                "Normalize the smoke final-effects text into a single blocking requirement and keep the claim bounded "
                "to evaluator-node artifact visibility plus the expected reviewer verdict."
            ),
            "test_designer": (
                "Record only one documented-surface smoke probe and preserve its ordinary-test artifacts for reviewer use."
            ),
            "ai_user": (
                "Behave like a caller running the documented smoke path and record a faithful operation log for the single "
                "evaluation unit."
            ),
            "reviewer": (
                "Return the reviewer-visible verdict supported by the role artifacts for this smoke round without blaming "
                "missing evidence on the implementation when the evaluator runtime is at fault."
            ),
        },
        agent_execution=agent_execution,
        ai_user_scheduler={"max_parallel_runs": 1},
        context_refs=context_refs,
    )


def materialize_evaluator_request(
    *,
    state_root: Path,
    submission: EvaluatorNodeSubmission,
) -> tuple[Path, dict[str, Any]]:
    """Write an evaluator request for a specific evaluator-node submission."""

    state_root = require_runtime_root(state_root)
    validate_evaluator_agent_execution(
        submission.agent_execution,
        context=f"evaluator submission `{submission.evaluator_node_id}`",
    )
    request_root = state_root / "artifacts" / "evaluator_nodes" / submission.evaluator_node_id / submission.round_id
    request_root.mkdir(parents=True, exist_ok=True)
    request_obj: dict[str, Any] = {
        "schema": "loop_product.loop_evaluator_prototype_input",
        "schema_version": "1.0.0",
        "evaluation_id": submission.evaluation_id,
        "run_id": f"{submission.round_id}__{submission.evaluator_node_id}",
        "workspace_root": submission.workspace_root,
        "output_root": submission.output_root,
        "product_manual_ref": submission.product_manual_ref,
        "role_requirements": dict(submission.role_requirements),
        "agent_execution": dict(submission.agent_execution),
    }
    if submission.final_effects_text_ref:
        request_obj["final_effects_text_ref"] = submission.final_effects_text_ref
    elif submission.final_effect_requirements:
        request_obj["final_effect_requirements"] = list(submission.final_effect_requirements)
    if submission.ai_user_scheduler:
        request_obj["ai_user_scheduler"] = dict(submission.ai_user_scheduler)
    request_path = request_root / "EvaluatorNodeRequest.json"
    request_path.write_text(json.dumps(request_obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (request_root / "EvaluatorNodeSubmission.json").write_text(
        json.dumps(submission.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return request_path, request_obj


def run_evaluator_node(
    *,
    state_root: Path,
    submission: EvaluatorNodeSubmission,
    poll_interval_s: float = 0.2,
    no_progress_timeout_s: float = 30.0,
    supervisor_child_argv_override: Sequence[str] | None = None,
) -> tuple[EvaluatorResult, dict[str, str]]:
    """Run the simple evaluator as a node-local evaluator boundary."""

    request_ref, request_obj = materialize_evaluator_request(state_root=state_root, submission=submission)
    del poll_interval_s, no_progress_timeout_s, supervisor_child_argv_override
    report = run_evaluator(request=request_ref)
    report_run_root = Path(str(report.get("run_root") or submission.output_root))
    runtime_refs: dict[str, str] = {
        "request_ref": str(request_ref),
        "evaluation_report_ref": str(report_run_root / "EvaluationReport.json"),
    }
    reviewer_response_ref = str(((report.get("reviewer") or {}).get("response_ref")) or "").strip()
    if reviewer_response_ref and Path(reviewer_response_ref).exists():
        runtime_refs["reviewer_response_ref"] = reviewer_response_ref
    recovery = dict(report.get("recovery") or {})
    recovery_required = bool(recovery.get("required"))
    run_state_ref = str(recovery.get("run_state_ref") or "").strip()
    if run_state_ref:
        runtime_refs["run_state_ref"] = run_state_ref
    verdict = EvaluatorVerdict.STUCK
    summary = "simple evaluator returned a non-terminal reviewer verdict"
    retryable = False
    diagnostics: dict[str, Any] = {"self_attribution": "", "self_repair": ""}
    evidence_refs = [str(request_ref), runtime_refs["evaluation_report_ref"]]
    if "reviewer_response_ref" in runtime_refs:
        evidence_refs.append(runtime_refs["reviewer_response_ref"])
    if "run_state_ref" in runtime_refs:
        evidence_refs.append(runtime_refs["run_state_ref"])

    report_status = str(report.get("status") or "").strip().upper()
    if report_status == "COMPLETED":
        reviewer_text = str(((report.get("reviewer") or {}).get("raw_output_text")) or "")
        reviewer_verdict = _parse_simple_reviewer_verdict(reviewer_text)
        summary = f"simple evaluator reviewer verdict={reviewer_verdict}"
        if reviewer_verdict == "PASS":
            verdict = EvaluatorVerdict.PASS
        elif reviewer_verdict == "FAIL":
            verdict = EvaluatorVerdict.FAIL
            retryable = True
        elif reviewer_verdict == "STRUCTURED_EXCEPTION":
            verdict = EvaluatorVerdict.STRUCTURED_EXCEPTION
            diagnostics = {
                "self_attribution": "evaluator_structured_exception_terminal",
                "self_repair": "inspect evaluator-side lane artifacts before blaming the implementation",
            }
        elif reviewer_verdict == "BUG":
            verdict = EvaluatorVerdict.BUG
            diagnostics = {
                "self_attribution": "evaluator_reviewer_bug",
                "self_repair": "inspect reviewer-visible lane artifacts before blaming the implementation",
            }
        elif reviewer_verdict == "ERROR" or reviewer_verdict == "UNKNOWN":
            verdict = EvaluatorVerdict.ERROR
            diagnostics = {
                "self_attribution": "evaluator_unknown_terminal",
                "self_repair": "repair the reviewer terminal output before treating the evaluator result as authoritative",
            }
        else:
            verdict = EvaluatorVerdict.STUCK
            diagnostics = {
                "self_attribution": "evaluator_uncertain",
                "self_repair": "narrow the acceptance surface or gather more visible evidence before retrying",
            }
    elif report_status == "HUMAN_GATE":
        verdict = EvaluatorVerdict.STUCK
        summary = "simple evaluator reached a human gate"
        diagnostics = {
            "self_attribution": "evaluator_human_gate",
            "self_repair": "clarify the evaluator request or final effects before retrying the evaluator node",
        }
    elif report_status == "RECOVERY_REQUIRED":
        error_obj = dict(report.get("error") or {})
        stage = str(error_obj.get("stage") or "runtime")
        message = str(error_obj.get("message") or "simple evaluator requires same-run recovery")
        retryable = True
        verdict = EvaluatorVerdict.STUCK
        summary = f"simple evaluator recovery required at {stage}: {message}"
        diagnostics = {
            "self_attribution": str(error_obj.get("self_attribution") or f"evaluator_{stage}"),
            "self_repair": str(
                error_obj.get("self_repair")
                or "resume the same evaluator run from EvaluatorRunState.json instead of starting a fresh checker pass"
            ),
            "issue_kind": str(error_obj.get("issue_kind") or ""),
        }
    else:
        error_obj = dict(report.get("error") or {})
        stage = str(error_obj.get("stage") or "runtime")
        message = str(error_obj.get("message") or "simple evaluator error")
        error_kind = str(error_obj.get("kind") or "").strip().upper()
        issue_kind = str(error_obj.get("issue_kind") or "").strip()
        summary = f"simple evaluator error at {stage}: {message}"
        if error_kind == "STRUCTURED_EXCEPTION":
            verdict = EvaluatorVerdict.STRUCTURED_EXCEPTION
            diagnostics = {
                "self_attribution": str(error_obj.get("self_attribution") or f"evaluator_{stage}"),
                "self_repair": str(
                    error_obj.get("self_repair")
                    or "repair the evaluator-side execution path before blaming the implementation"
                ),
            }
            if issue_kind:
                summary = f"simple evaluator structured exception ({issue_kind}) at {stage}: {message}"
        else:
            verdict = EvaluatorVerdict.BUG
            diagnostics = {
                "self_attribution": f"evaluator_{stage}",
                "self_repair": "inspect the simple evaluator report and lane artifacts before blaming implementation",
            }

    evaluator_result = EvaluatorResult(
        verdict=verdict,
        lane="reviewer",
        summary=summary,
        evidence_refs=evidence_refs,
        retryable=retryable,
        diagnostics=diagnostics,
        recovery=recovery if recovery_required else {},
    )
    accepted_transport = submit_control_envelope(
        state_root,
        ControlEnvelope(
            source=submission.evaluator_node_id,
            envelope_type="evaluator_result",
            round_id=submission.round_id,
            generation=submission.generation,
            payload={**evaluator_result.to_dict(), **runtime_refs},
            status=EnvelopeStatus.REPORT,
            note=evaluator_result.summary,
        ),
    )
    if accepted_transport.status is not EnvelopeStatus.ACCEPTED:
        return (
            EvaluatorResult(
                verdict=EvaluatorVerdict.BUG,
                lane="reviewer",
                summary="evaluator result transport failed closed before authoritative acceptance",
                evidence_refs=evidence_refs,
                retryable=False,
                diagnostics={
                    "self_attribution": "evaluator_transport",
                    "self_repair": "inspect gateway/kernel transport before blaming the implementation",
                },
            ),
            runtime_refs,
    )
    return evaluator_result, runtime_refs


def _same_run_recovery_delay_s(*, result: EvaluatorResult, failure_count: int) -> float:
    issue_kind = str((dict(result.diagnostics or {})).get("issue_kind") or "").strip().lower()
    if issue_kind == "provider_capacity":
        return min(3.0, 0.75 * max(1, failure_count))
    if issue_kind in {"provider_transport", "provider_runtime"}:
        return min(2.0, 0.5 * max(1, failure_count))
    return min(1.0, 0.25 * max(1, failure_count))


def run_evaluator_node_until_terminal(
    *,
    state_root: Path,
    submission: EvaluatorNodeSubmission,
    max_same_run_recovery_attempts: int = 3,
    poll_interval_s: float = 0.2,
    no_progress_timeout_s: float = 30.0,
    supervisor_child_argv_override: Sequence[str] | None = None,
) -> tuple[EvaluatorResult, dict[str, str]]:
    """Run the evaluator-node boundary until it reaches a terminal or bounded retry limit."""

    attempts_used = 0
    while True:
        attempts_used += 1
        evaluator_result, runtime_refs = run_evaluator_node(
            state_root=state_root,
            submission=submission,
            poll_interval_s=poll_interval_s,
            no_progress_timeout_s=no_progress_timeout_s,
            supervisor_child_argv_override=supervisor_child_argv_override,
        )
        recovery = dict(evaluator_result.recovery or {})
        if not evaluator_result.retryable or not bool(recovery.get("required")):
            return evaluator_result, runtime_refs
        if attempts_used > max(0, int(max_same_run_recovery_attempts)):
            return evaluator_result, runtime_refs
        time.sleep(_same_run_recovery_delay_s(result=evaluator_result, failure_count=attempts_used))


def _load_submission_ref(submission_ref: str | Path) -> EvaluatorNodeSubmission:
    submission_path = Path(submission_ref).expanduser().resolve()
    payload = json.loads(submission_path.read_text(encoding="utf-8"))
    return EvaluatorNodeSubmission.from_dict(payload)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="LOOP evaluator-node helpers")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    run_until_parser = subparsers.add_parser(
        "run-until-terminal",
        help="Run an evaluator node and continue same-run recovery until terminal or bounded retry exhaustion.",
    )
    run_until_parser.add_argument("--state-root", required=True)
    run_until_parser.add_argument("--submission-ref", required=True)
    run_until_parser.add_argument("--max-same-run-recovery-attempts", type=int, default=3)

    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.cmd == "run-until-terminal":
        evaluator_result, runtime_refs = run_evaluator_node_until_terminal(
            state_root=Path(args.state_root),
            submission=_load_submission_ref(args.submission_ref),
            max_same_run_recovery_attempts=int(args.max_same_run_recovery_attempts),
        )
        print(
            json.dumps(
                {
                    "evaluator_result": evaluator_result.to_dict(),
                    "runtime_refs": runtime_refs,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    raise AssertionError(f"unsupported command: {args.cmd}")


if __name__ == "__main__":
    raise SystemExit(main())
