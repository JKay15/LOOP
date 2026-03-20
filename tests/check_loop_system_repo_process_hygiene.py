#!/usr/bin/env python3
"""Guard supervisor/runtime hygiene after terminal child closeout."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-process-hygiene][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    with tempfile.TemporaryDirectory(prefix="loop_product_process_hygiene_") as td:
        temp_root = Path(td)
        idle_repo_root = temp_root / "idle_repo"
        idle_repo_root.mkdir(parents=True, exist_ok=True)
        supervisor_runtime_ref = idle_repo_root / ".loop" / "host_child_launch_supervisor" / "HostChildLaunchSupervisorRuntime.json"
        supervisor_proc = subprocess.Popen(
            [
                "uv",
                "run",
                "--locked",
                "python",
                "-m",
                "loop_product.host_child_launch_supervisor",
                "--repo-root",
                str(idle_repo_root),
                "--poll-interval-s",
                "0.05",
                "--idle-exit-after-s",
                "0.15",
            ],
            cwd=str(ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            supervisor_stdout, supervisor_stderr = supervisor_proc.communicate(timeout=3.0)
        except subprocess.TimeoutExpired:
            supervisor_proc.kill()
            supervisor_stdout, supervisor_stderr = supervisor_proc.communicate(timeout=3.0)
            return _fail("host child launch supervisor must exit when idle instead of lingering forever")
        if supervisor_proc.returncode != 0:
            detail = supervisor_stderr.strip() or supervisor_stdout.strip()
            return _fail(f"idle supervisor must exit cleanly: {detail}")
        if supervisor_runtime_ref.exists():
            return _fail("idle supervisor must clean its runtime marker on exit")

        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)
        node = NodeSpec(
            node_id="child-process-hygiene-001",
            node_kind="implementer",
            goal_slice="finish the exact same child without residual recovery churn",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["implement", "evaluate", "report", "split_request"],
            delegation_ref="state/delegations/child-process-hygiene-001.json",
            result_sink_ref="artifacts/child-process-hygiene-001/implementer_result.json",
            lineage_ref="root-kernel->child-process-hygiene-001",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id="process-hygiene",
            root_goal="settle terminal runtime status without stale active recovery",
            root_node_id="root-kernel",
        )
        kernel_state.register_node(
            NodeSpec(
                node_id="root-kernel",
                node_kind="kernel",
                goal_slice="supervise child hygiene",
                parent_node_id=None,
                generation=0,
                round_id="R0",
                execution_policy={"mode": "kernel"},
                reasoning_profile={"thinking_budget": "medium", "role": "kernel"},
                budget_profile={"max_rounds": 1},
                allowed_actions=["dispatch", "submit", "audit"],
                delegation_ref="",
                result_sink_ref="artifacts/kernel.json",
                lineage_ref="root-kernel",
                status=NodeStatus.ACTIVE,
            )
        )
        kernel_state.register_node(node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        launch_result_ref = state_root / "artifacts" / "launches" / node.node_id / "attempt_001" / "ChildLaunchResult.json"
        stdout_ref = launch_result_ref.parent / "logs" / "child.stdout.txt"
        stderr_ref = launch_result_ref.parent / "logs" / "child.stderr.txt"
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        stdout_ref.parent.mkdir(parents=True, exist_ok=True)
        stdout_ref.write_text("", encoding="utf-8")
        stderr_ref.write_text("", encoding="utf-8")
        (workspace_root / "CHILD_PROMPT.md").write_text("PROMPT\n", encoding="utf-8")
        launch_result_payload = {
            "launch_decision": "started",
            "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
            "node_id": node.node_id,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "startup_health_timeout_ms": 250,
            "startup_retry_limit": 0,
            "attempt_count": 1,
            "retryable_failure_kind": "",
            "attempts": [],
            "launch_request_ref": str((launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
            "launch_result_ref": str(launch_result_ref.resolve()),
            "launch_log_dir": str((launch_result_ref.parent / "logs").resolve()),
            "stdout_ref": str(stdout_ref.resolve()),
            "stderr_ref": str(stderr_ref.resolve()),
            "stdin_ref": str((workspace_root / "CHILD_PROMPT.md").resolve()),
            "wrapped_argv": ["/bin/sh", "-lc", "printf 'terminal lane\\n'"],
            "wrapper_cmd": "",
            "pid": 999999,
            "exit_code": None,
        }
        launch_result_ref.write_text(json.dumps(launch_result_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        stale_epoch = time.time() - 25.0
        for path in (launch_result_ref, stdout_ref, stderr_ref):
            os.utime(path, (stale_epoch, stale_epoch))

        implementer_result_ref = state_root / "artifacts" / node.node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        incomplete_result_payload = {
            "schema": "loop_product.implementer_result",
            "schema_version": "0.1.0",
            "node_id": node.node_id,
            "status": "COMPLETED",
            "outcome": "COMPLETED",
            "delivered_artifact_exactly_evaluated": True,
        }
        implementer_result_ref.write_text(
            json.dumps(incomplete_result_payload, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )

        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                result_ref=launch_result_ref,
                stall_threshold_s=0.0,
            )
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
        if not bool(status_payload.get("recovery_eligible")):
            return _fail("completed implementer_result without evaluator-backed evidence must remain recovery-eligible")
        if str(status_payload.get("recovery_reason") or "") == "authoritative_terminal_result":
            return _fail("incomplete/manual implementer_result must not be accepted as authoritative terminal closure")
        if str(status_payload.get("lifecycle_status") or "") == "COMPLETED":
            return _fail("incomplete/manual implementer_result must not force lifecycle_status=COMPLETED")

        evaluation_report_ref = temp_root / "EvaluationReport.json"
        evaluation_report_ref.write_text(
            json.dumps({"status": "COMPLETED", "verdict": "PASS"}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        implementer_result_ref.write_text(
            json.dumps(
                {
                    **incomplete_result_payload,
                    "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                    "evaluator_result": {"verdict": "PASS", "retryable": False},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
        try:
            status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                result_ref=launch_result_ref,
                stall_threshold_s=0.0,
            )
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
        if bool(status_payload.get("recovery_eligible")):
            return _fail("runtime status must not keep a node recovery-eligible once authoritative terminal result exists")
        if str(status_payload.get("recovery_reason") or "") != "authoritative_terminal_result":
            return _fail("runtime status must explain terminal closeout with authoritative_terminal_result")
        if str(status_payload.get("lifecycle_status") or "") != "COMPLETED":
            return _fail("runtime status must surface terminal lifecycle_status once authoritative terminal result exists")

    print("[loop-system-process-hygiene] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
