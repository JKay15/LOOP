"""Minimal node runner for the wave-1 smoke loop."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path
from typing import Any

from loop_product.dispatch import load_child_runtime_context
from loop_product.loop.evaluator_client import build_evaluator_submission_for_smoke_round, run_evaluator_node
from loop_product.loop.local_control import decide_next_action
from loop_product.loop.roundbook import append_round_entry
from loop_product.protocols.node import NodeSpec, NodeStatus
from loop_product.runtime_paths import default_workspace_root


def run_child_smoke_loop(state_root: Path, node: NodeSpec) -> dict[str, Any]:
    """Run a deterministic two-round local loop."""

    roundbook: list[dict[str, Any]] = []
    final_status = "FAIL_CLOSED"
    for round_index in (1, 2):
        smoke_mode = "fail" if round_index == 1 else "pass"
        runtime_context = load_child_runtime_context(state_root, node.node_id)
        runtime_node = NodeSpec.from_dict(dict(runtime_context["node"]))
        workspace_root = Path(str(runtime_context.get("workspace_root") or default_workspace_root())).resolve()
        submission = build_evaluator_submission_for_smoke_round(
            target_node=runtime_node,
            state_root=state_root,
            workspace_root=workspace_root,
            output_root=state_root / "artifacts" / "evaluator_node_runs",
            round_index=round_index,
            role_agent_cmd=(
                f"{shlex.quote(sys.executable)} -m loop_product.loop.smoke_role_agent --mode {smoke_mode}"
            ),
        )
        result, runtime_refs = run_evaluator_node(
            state_root=state_root,
            submission=submission,
        )
        action = decide_next_action(result)
        entry = {
            "round_index": round_index,
            "node_id": runtime_node.node_id,
            "verdict": result.verdict.value,
            "lane": result.lane,
            "action": action,
            "summary": result.summary,
            "request_ref": runtime_refs["request_ref"],
            "evidence_refs": list(result.evidence_refs),
        }
        if "evaluation_report_ref" in runtime_refs:
            entry["evaluation_report_ref"] = runtime_refs["evaluation_report_ref"]
        if "reviewer_response_ref" in runtime_refs:
            entry["reviewer_response_ref"] = runtime_refs["reviewer_response_ref"]
        roundbook = append_round_entry(state_root, node.node_id, entry)
        if result.verdict.value == "PASS":
            node.status = NodeStatus.COMPLETED
            final_status = "PASS_AFTER_RETRY" if round_index > 1 else "PASS"
            break
        if action != "RETRY":
            node.status = NodeStatus.FAILED
            break
    if node.status == NodeStatus.ACTIVE and final_status == "FAIL_CLOSED":
        node.status = NodeStatus.FAILED
    return {
        "roundbook": roundbook,
        "evaluator_status": final_status,
        "node_status": node.status.value,
        "node_id": node.node_id,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a materialized LOOP child smoke loop.")
    parser.add_argument("--state-root", required=True, help="Repo-local .loop runtime root.")
    parser.add_argument("--node-id", required=True, help="Materialized child node id to execute.")
    parser.add_argument("--result-path", required=True, help="Path where the child result JSON will be written.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    state_root = Path(args.state_root).resolve()
    runtime_context = load_child_runtime_context(state_root, str(args.node_id))
    node = NodeSpec.from_dict(dict(runtime_context["node"]))
    result = run_child_smoke_loop(state_root, node)
    result_path = Path(args.result_path).resolve()
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
