#!/usr/bin/env python3
"""Regression check for smoke retry exhaustion fail-closed behavior."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-smoke-failure-closure][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        from loop_product.dispatch.child_dispatch import materialize_child
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
        from loop_product.loop import runner
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    class _Submission:
        def __init__(self, round_id: str) -> None:
            self.evaluator_node_id = "child-implementer-001__evaluator"
            self.round_id = round_id

    def _fake_build_submission(
        *,
        target_node: NodeSpec,
        state_root: Path,
        workspace_root: Path,
        output_root: Path,
        round_index: int,
        role_agent_cmd: str,
    ) -> _Submission:
        return _Submission(f"{target_node.round_id}.{round_index}")

    def _fake_run_evaluator_node(*, state_root: Path, submission: _Submission) -> tuple[EvaluatorResult, dict[str, str]]:
        return (
            EvaluatorResult(
                verdict=EvaluatorVerdict.FAIL,
                lane="reviewer",
                summary="forced retryable failure",
                evidence_refs=["/tmp/request.json", "/tmp/SupervisorReport.json", "/tmp/EvaluationReport.json"],
                retryable=True,
                diagnostics={},
            ),
            {
                "request_ref": "/tmp/request.json",
                "supervisor_report_ref": "/tmp/SupervisorReport.json",
                "evaluation_report_ref": "/tmp/EvaluationReport.json",
            },
        )

    original_build = runner.build_evaluator_submission_for_smoke_round
    original_run = runner.run_evaluator_node
    try:
        runner.build_evaluator_submission_for_smoke_round = _fake_build_submission
        runner.run_evaluator_node = _fake_run_evaluator_node
        with tempfile.TemporaryDirectory(prefix="loop_system_smoke_fail_closed_") as td:
            state_root = Path(td) / ".loop"
            ensure_runtime_tree(state_root)
            root_node = NodeSpec(
                node_id="root-kernel",
                node_kind="kernel",
                goal_slice="supervise fail-closed smoke retry exhaustion",
                parent_node_id=None,
                generation=0,
                round_id="R0",
                execution_policy={"mode": "kernel"},
                reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
                budget_profile={"max_rounds": 1},
                allowed_actions=["dispatch", "submit", "audit"],
                status=NodeStatus.ACTIVE,
            )
            kernel_state = KernelState(task_id="wave10-smoke-fail-closed", root_goal="prove fail-closed status", root_node_id="root-kernel")
            kernel_state.register_node(root_node)
            authority = kernel_internal_authority()
            persist_kernel_state(state_root, kernel_state, authority=authority)
            node = materialize_child(
                state_root=state_root,
                kernel_state=kernel_state,
                parent_node_id=root_node.node_id,
                node_id="child-implementer-001",
                goal_slice="exercise fail-closed smoke retry exhaustion",
                round_id="R1",
                execution_policy={},
                reasoning_profile={},
                budget_profile={},
                authority=authority,
            )

            result = runner.run_child_smoke_loop(state_root, node)

        if result.get("evaluator_status") != "FAIL_CLOSED":
            return _fail(f"retry exhaustion must fail close, got {result.get('evaluator_status')!r}")
        if node.status is not NodeStatus.FAILED:
            return _fail(f"retry exhaustion must mark child FAILED, got {node.status.value!r}")
        roundbook = list(result.get("roundbook") or [])
        if len(roundbook) != 2:
            return _fail(f"retry exhaustion must preserve both failed rounds, got {len(roundbook)}")
        if any(str(entry.get("action")) != "RETRY" for entry in roundbook):
            return _fail(f"retry exhaustion rounds must remain retry decisions, got {roundbook!r}")
    finally:
        runner.build_evaluator_submission_for_smoke_round = original_build
        runner.run_evaluator_node = original_run

    print("[loop-system-smoke-failure-closure] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
