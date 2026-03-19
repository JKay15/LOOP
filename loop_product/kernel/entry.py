"""Kernel entrypoint and smoke path."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from loop_product.dispatch import LaunchPolicy, build_child_heartbeat
from loop_product.dispatch.child_dispatch import materialize_child
from loop_product.kernel.authority import kernel_internal_authority
from loop_product.kernel.audit import record_audit_event
from loop_product.kernel.policy import (
    implementer_budget_profile,
    implementer_execution_policy,
    implementer_reasoning_profile,
    kernel_execution_policy,
    kernel_reasoning_profile,
)
from loop_product.kernel.state import (
    KernelState,
    ensure_runtime_tree,
    load_kernel_state,
    persist_kernel_state,
)
from loop_product.kernel.submit import submit_control_envelope
from loop_product.protocols.node import NodeSpec, NodeStatus
from loop_product.runtime_paths import require_runtime_root


def run_kernel_smoke(state_root: Path) -> dict[str, object]:
    """Run the wave-1 kernel -> child -> evaluator smoke path."""

    authority = kernel_internal_authority()
    state_root = require_runtime_root(state_root)
    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="wave1-smoke",
        root_goal="bootstrap the complete LOOP system repo smoke path",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise the wave-1 smoke path",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy=kernel_execution_policy(),
        reasoning_profile=kernel_reasoning_profile(),
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/smoke_summary.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    persist_kernel_state(state_root, kernel_state, authority=authority)
    record_audit_event(state_root, "kernel_started", root_node.to_dict())

    launch_policy = LaunchPolicy(
        agent_provider=implementer_execution_policy()["agent_provider"],
        thinking_budget=implementer_reasoning_profile()["thinking_budget"],
        sandbox_mode=implementer_execution_policy()["sandbox_mode"],
        retry_policy=implementer_execution_policy()["retry_policy"],
    )
    record_audit_event(
        state_root,
        "launch_policy",
        {
            "agent_provider": launch_policy.agent_provider,
            "thinking_budget": launch_policy.thinking_budget,
            "sandbox_mode": launch_policy.sandbox_mode,
            "retry_policy": launch_policy.retry_policy,
        },
    )
    child_node = materialize_child(
        state_root=state_root,
        kernel_state=kernel_state,
        parent_node_id=root_node.node_id,
        node_id="child-implementer-001",
        goal_slice="implement a bounded smoke-path fix and accept evaluator feedback",
        round_id="R1",
        execution_policy=implementer_execution_policy(),
        reasoning_profile=implementer_reasoning_profile(),
        budget_profile=implementer_budget_profile(),
        authority=authority,
    )
    persist_kernel_state(state_root, kernel_state, authority=authority)
    submit_control_envelope(
        state_root,
        build_child_heartbeat(child_node, note="child-dispatch materialized the implementer child"),
    )
    record_audit_event(state_root, "child_materialized", child_node.to_dict())
    child_run_root = state_root / "artifacts" / "child_runs" / child_node.node_id
    child_result_path = child_run_root / "result.json"
    child_stdout_path = child_run_root / "stdout.txt"
    child_stderr_path = child_run_root / "stderr.txt"
    child_run_root.mkdir(parents=True, exist_ok=True)
    child_argv = [
        sys.executable,
        "-m",
        "loop_product.loop.runner",
        "--state-root",
        str(state_root),
        "--node-id",
        child_node.node_id,
        "--result-path",
        str(child_result_path),
    ]
    child_proc = subprocess.run(
        child_argv,
        cwd=str(Path(child_node.workspace_root).resolve()),
        capture_output=True,
        text=True,
        env=dict(os.environ),
    )
    child_stdout_path.write_text(child_proc.stdout, encoding="utf-8")
    child_stderr_path.write_text(child_proc.stderr, encoding="utf-8")
    if child_proc.returncode != 0:
        raise RuntimeError(
            "child-local smoke runner failed: "
            f"exit_code={child_proc.returncode} stderr={child_proc.stderr.strip()!r}"
        )
    loop_result = json.loads(child_result_path.read_text(encoding="utf-8"))
    kernel_state = load_kernel_state(state_root)
    kernel_state.update_node_status(
        child_node.node_id,
        NodeStatus(str(loop_result.get("node_status") or NodeStatus.FAILED.value)),
    )
    persist_kernel_state(state_root, kernel_state, authority=authority)
    record_audit_event(
        state_root,
        "child_completed",
        {
            "node_id": child_node.node_id,
            "status": str(loop_result.get("node_status") or NodeStatus.FAILED.value),
            "roundbook": loop_result["roundbook"],
            "child_runner_result_ref": str(child_result_path),
        },
    )

    summary = {
        "kernel_status": kernel_state.status,
        "root_node_id": root_node.node_id,
        "child_node_id": child_node.node_id,
        "child_status": str(loop_result.get("node_status") or NodeStatus.FAILED.value),
        "evaluator_status": loop_result["evaluator_status"],
        "roundbook": loop_result["roundbook"],
        "smoke_status": "OK",
    }
    summary_path = state_root / "artifacts" / "smoke_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Wave-1 kernel entrypoint for the complete LOOP product repo.")
    parser.add_argument("--smoke", action="store_true", help="Run the kernel -> child -> evaluator smoke path.")
    parser.add_argument("--state-root", default=".loop", help="Runtime state root to use.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not args.smoke:
        parser.print_help()
        return 0
    run_kernel_smoke(Path(args.state_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
