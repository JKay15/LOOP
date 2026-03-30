#!/usr/bin/env python3
"""Validate retryable recovery for evaluator lane runtime interruptions."""

from __future__ import annotations

import json
import shlex
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-interruption-recovery][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def _fixture_role_agent_cmd(
    *,
    scenario: str,
    failing_role: str,
    failure_message: str,
    failure_exit_code: int | None = None,
    failure_signal: str | None = None,
) -> str:
    parts = [
        shlex.quote(sys.executable),
        "-m",
        "loop_product.loop.fixture_role_agent",
        "--scenario",
        shlex.quote(scenario),
        "--failing-role",
        shlex.quote(failing_role),
        "--failure-message",
        shlex.quote(failure_message),
    ]
    if failure_exit_code is not None:
        parts.extend(["--failure-exit-code", shlex.quote(str(int(failure_exit_code)))])
    if failure_signal:
        parts.extend(["--failure-signal", shlex.quote(str(failure_signal))])
    return " ".join(parts)


def _target_node(*, workspace_root: Path):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="evaluator-interruption-node",
        node_kind="implementer",
        goal_slice="validate evaluator interruption recovery",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "danger-full-access"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_runtime_s": 1800},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/evaluator-interruption-node.json",
        result_sink_ref="artifacts/evaluator_interruption_result.json",
        workspace_root=str(workspace_root.resolve()),
        codex_home=str(workspace_root.resolve()),
        lineage_ref="lineage.json",
        status=NodeStatus.ACTIVE,
    )


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _run_case(
    *,
    temp_root: Path,
    case_name: str,
    cmd_kwargs: dict[str, object],
) -> tuple[object, dict[str, str], dict[str, object]]:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
    from loop_product.loop.evaluator_client import build_evaluator_submission_for_frozen_task, run_evaluator_node
    from loop_product.protocols.node import NodeSpec, NodeStatus

    case_root = temp_root / case_name
    state_root = case_root / ".loop"
    workspace_root = case_root / "workspace"
    output_root = state_root / "artifacts" / "evaluator_runs"
    implementation_root = case_root / "implementation"
    ensure_runtime_tree(state_root)
    workspace_root.mkdir(parents=True, exist_ok=True)
    implementation_root.mkdir(parents=True, exist_ok=True)
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="root",
        goal_slice="root evaluator recovery harness",
        parent_node_id="",
        generation=0,
        round_id="R0",
        execution_policy={},
        reasoning_profile={},
        budget_profile={},
        allowed_actions=["delegate", "evaluate", "report"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    target_node = _target_node(workspace_root=workspace_root)
    kernel_state = KernelState(
        task_id=f"evaluator-interruption-{case_name}",
        root_goal="validate evaluator interruption recovery",
        root_node_id=root_node.node_id,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(target_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
    persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
    persist_node_snapshot(state_root, target_node, authority=kernel_internal_authority())
    _write_text(implementation_root / "README.md", "# Implementation\n")
    manual_ref = _write_text(
        case_root / "PRODUCT_MANUAL.md",
        "# Manual\n\nUse only documented evaluator surfaces and emit structured JSON.\n",
    )
    effects_ref = _write_text(
        case_root / "FINAL_EFFECTS.md",
        "# Final Effects\n\n- Preserve retryable evaluator-side recovery for interrupted lanes.\n",
    )
    submission = build_evaluator_submission_for_frozen_task(
        target_node=target_node,
        workspace_root=workspace_root,
        output_root=output_root,
        implementation_package_ref=implementation_root,
        product_manual_ref=manual_ref,
        final_effects_text_ref=effects_ref,
        role_agent_cmd=_fixture_role_agent_cmd(
            scenario="lane_failure",
            failing_role="test_designer",
            **cmd_kwargs,
        ),
        evaluation_id=f"evaluator_interruption_{case_name}",
    )
    result, refs = run_evaluator_node(state_root=state_root, submission=submission)
    report = json.loads(Path(str(refs["evaluation_report_ref"])).read_text(encoding="utf-8"))
    return result, refs, report


def main() -> int:
    evaluator_result_schema = _load_schema("LoopSystemEvaluatorResult.schema.json")
    with tempfile.TemporaryDirectory(prefix="loop_system_eval_interruptions_") as td:
        temp_root = Path(td)
        issue_cases = {
            "sigterm_negative": {
                "failure_message": "synthetic evaluator lane interrupted before result emission",
                "failure_signal": "SIGTERM",
            },
            "shell_exit_143": {
                "failure_message": "synthetic shell wrapper terminated the evaluator lane before result emission",
                "failure_exit_code": 143,
            },
            "shell_exit_137": {
                "failure_message": "synthetic shell wrapper hard-killed the evaluator lane before result emission",
                "failure_exit_code": 137,
            },
        }
        for case_name, cmd_kwargs in issue_cases.items():
            result, refs, report = _run_case(temp_root=temp_root, case_name=case_name, cmd_kwargs=cmd_kwargs)
            Draft202012Validator(evaluator_result_schema).validate(result.to_dict())
            if result.verdict.value != "STUCK":
                return _fail(f"{case_name} must surface STUCK while recovery is pending, got {result.verdict.value}")
            if result.retryable is not True:
                return _fail(f"{case_name} must remain retryable for same-run recovery")
            if str(result.diagnostics.get("self_attribution") or "") != "evaluator_exec_provider_runtime":
                return _fail(f"{case_name} must attribute interruption to evaluator_exec_provider_runtime")
            if "retry the same evaluator lane" not in str(result.diagnostics.get("self_repair") or ""):
                return _fail(f"{case_name} must keep a bounded retry hint")
            if str(report.get("status") or "") != "RECOVERY_REQUIRED":
                return _fail(f"{case_name} must persist RECOVERY_REQUIRED instead of terminal ERROR")
            report_error = dict(report.get("error") or {})
            if str(report_error.get("issue_kind") or "") != "provider_runtime":
                return _fail(f"{case_name} must classify interruption as provider_runtime")
            recovery = dict(report.get("recovery") or {})
            if recovery.get("required") is not True or str(recovery.get("mode") or "") != "SAME_RUN_RESUME":
                return _fail(f"{case_name} must expose SAME_RUN_RESUME recovery metadata")
            if "evaluation_report_ref" not in refs:
                return _fail(f"{case_name} must return evaluation_report_ref")
    print("[loop-system-evaluator-interruption-recovery][OK] interruption recovery semantics preserved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
