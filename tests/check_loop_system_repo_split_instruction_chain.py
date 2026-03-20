#!/usr/bin/env python3
"""Require the product split instruction chain to stay aligned."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-split-instruction-chain][FAIL] {msg}", file=sys.stderr)
    return 2


def _require_contains(text: str, needle: str, *, context: str) -> None:
    if needle not in text:
        raise AssertionError(f"{context} must contain {needle!r}")


def main() -> int:
    from loop_product.dispatch import bootstrap as bootstrap_module
    from loop_product.protocols.node import NodeSpec, NodeStatus

    product_agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    workspace_agents = (ROOT / "workspace" / "AGENTS.md").read_text(encoding="utf-8")
    child_dispatch_skill = (ROOT / ".agents" / "skills" / "child-dispatch" / "SKILL.md").read_text(encoding="utf-8")
    loop_runner_skill = (ROOT / ".agents" / "skills" / "loop-runner" / "SKILL.md").read_text(encoding="utf-8")
    kernel_contract = (ROOT / "docs" / "contracts" / "LOOP_SYSTEM_KERNEL_CONTRACT.md").read_text(encoding="utf-8")
    node_contract = (ROOT / "docs" / "contracts" / "LOOP_SYSTEM_NODE_CONTRACT.md").read_text(encoding="utf-8")

    try:
        _require_contains(
            product_agents,
            "If the implementer reports that the frozen task should split",
            context="root product rules",
        )
        _require_contains(
            product_agents,
            "report whether split was proposed, whether kernel accepted it",
            context="root product rules",
        )
        _require_contains(
            workspace_agents,
            "surface an explicit split request upward to the root kernel",
            context="workspace rules",
        )
        _require_contains(
            workspace_agents,
            "Do not directly materialize child nodes or mutate topology fact from inside the workspace",
            context="workspace rules",
        )
        _require_contains(
            child_dispatch_skill,
            "Only the current implementer may propose that the task should split",
            context="child-dispatch skill",
        )
        _require_contains(
            child_dispatch_skill,
            "closeout must say whether split was proposed, accepted, rejected, or never requested",
            context="child-dispatch skill",
        )
        _require_contains(
            loop_runner_skill,
            "If bounded progress reveals a meaningful parallelizable gap, surface a split request upward",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "Do not treat a possible split as local permission to spawn helpers or mutate topology yourself",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "Treat exact frozen refs as authoritative",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "fresh workspace with no deliverable yet",
            context="loop-runner skill",
        )
        _require_contains(
            kernel_contract,
            "the kernel may materialize additional child nodes only after an implementer-authored split proposal passes kernel review",
            context="kernel contract",
        )
        _require_contains(
            kernel_contract,
            "product closeout for an auto-split-capable run must state whether split was proposed, accepted, and useful",
            context="kernel contract",
        )
        _require_contains(
            node_contract,
            "treat exact frozen refs as authoritative during startup",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "first concrete workspace action before broad reconnaissance",
            context="node contract",
        )
    except AssertionError as exc:
        return _fail(str(exc))

    with tempfile.TemporaryDirectory(prefix="loop_product_split_prompt_") as td:
        workspace_root = Path(td)
        handoff_md = workspace_root / "FROZEN_HANDOFF.md"
        handoff_md.write_text("# Frozen Handoff\n", encoding="utf-8")
        node = NodeSpec(
            node_id="child-implementer-001",
            node_kind="implementer",
            goal_slice="formalize a real-paper result chain honestly",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "workspace-write"},
            reasoning_profile={"thinking_budget": "high", "role": "implementer"},
            budget_profile={"max_rounds": 3},
            allowed_actions=["implement", "evaluate", "report", "split_request", "merge_request"],
            delegation_ref="state/delegations/child-implementer-001.json",
            result_sink_ref="artifacts/child-implementer-001/implementer_result.json",
            lineage_ref="root-kernel->child-implementer-001",
            status=NodeStatus.ACTIVE,
        )
        prompt_text = bootstrap_module._render_child_prompt(
            workspace_root=workspace_root,
            handoff_md_path=handoff_md,
            node=node,
            workspace_result_sink_abs=workspace_root / "workspace_result.json",
            kernel_result_sink_abs=workspace_root / "kernel_result.json",
            evaluator_submission_abs=workspace_root / "EvaluatorNodeSubmission.json",
            evaluator_runner_abs=workspace_root / "RUN_EVALUATOR_NODE_UNTIL_TERMINAL.sh",
        )
        if "surface a split request upward to the root kernel" not in prompt_text:
            return _fail("bootstrap child prompt must tell the implementer how to escalate a split request upward")
        if "Do not directly materialize child nodes yourself" not in prompt_text:
            return _fail("bootstrap child prompt must forbid self-materializing child nodes from implementer context")
        if "report whether split was proposed or accepted" not in prompt_text:
            return _fail("bootstrap child prompt must require split/outcome reporting in the implementer closeout")
        if "exact frozen refs in the handoff/prompt as authoritative" not in prompt_text:
            return _fail("bootstrap child prompt must treat exact frozen refs as authoritative during startup")
        if "one concrete workspace-local action" not in prompt_text:
            return _fail("bootstrap child prompt must require one concrete startup action before broad reconnaissance")

    print("[loop-system-split-instruction-chain] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
