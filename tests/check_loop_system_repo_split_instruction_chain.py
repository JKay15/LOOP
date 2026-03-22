#!/usr/bin/env python3
"""Require the product split instruction chain to stay aligned."""

from __future__ import annotations

import json
import subprocess
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
    from loop_product.kernel.state import KernelState, persist_kernel_state
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.kernel.state import ensure_runtime_tree
    from loop_product.runtime_paths import node_machine_handoff_ref

    product_agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    workspace_agents = (ROOT / "workspace" / "AGENTS.md").read_text(encoding="utf-8")
    child_dispatch_skill = (ROOT / ".agents" / "skills" / "child-dispatch" / "SKILL.md").read_text(encoding="utf-8")
    loop_runner_skill = (ROOT / ".agents" / "skills" / "loop-runner" / "SKILL.md").read_text(encoding="utf-8")
    kernel_contract = (ROOT / "docs" / "contracts" / "LOOP_SYSTEM_KERNEL_CONTRACT.md").read_text(encoding="utf-8")
    node_contract = (ROOT / "docs" / "contracts" / "LOOP_SYSTEM_NODE_CONTRACT.md").read_text(encoding="utf-8")
    evaluator_node_contract = (ROOT / "docs" / "contracts" / "LOOP_SYSTEM_EVALUATOR_NODE_CONTRACT.md").read_text(
        encoding="utf-8"
    )

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
            workspace_agents,
            "submit_split_request_from_handoff.sh",
            context="workspace rules",
        )
        _require_contains(
            workspace_agents,
            "submit_activate_request_from_handoff.sh",
            context="workspace rules",
        )
        _require_contains(
            workspace_agents,
            "WHOLE_PAPER_STATUS.json",
            context="workspace rules",
        )
        _require_contains(
            workspace_agents,
            "required outputs",
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
            "submit_split_request_from_handoff.sh",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "submit_activate_request_from_handoff.sh",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "WHOLE_PAPER_STATUS.json",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "required outputs",
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
            loop_runner_skill,
            "at least one non-empty file under the workspace mirror or required artifact/result path",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "Creating only an empty directory does not satisfy that startup requirement",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "do not keep retrying the same search family in the opening phase",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "startup note or checkpoint file alone does not count as substantive startup progress",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "actual artifact skeleton or first substantive deliverable batch",
            context="loop-runner skill",
        )
        _require_contains(
            loop_runner_skill,
            "Do not broad-search repo roots, `.loop/**` history, or unrelated evaluator workspaces",
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
            kernel_contract,
            "text-only split recommendations in workspace prose are not submitted topology proposals",
            context="kernel contract",
        )
        _require_contains(
            kernel_contract,
            "accepted deferred split children require a later activate proposal",
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
        _require_contains(
            node_contract,
            "at least one non-empty file under the workspace mirror or required artifact/result path",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "startup search rate-limit or tool-exhaustion should downgrade",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "startup note or checkpoint file alone does not count as substantive startup progress",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "Do not broad-search repo roots, `.loop/**` history, or unrelated evaluator workspaces",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "repo-shipped split helper",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "repo-shipped activate helper",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "WHOLE_PAPER_STATUS.json",
            context="node contract",
        )
        _require_contains(
            node_contract,
            "required outputs",
            context="node contract",
        )
        _require_contains(
            evaluator_node_contract,
            "required outputs",
            context="evaluator node contract",
        )
        _require_contains(
            evaluator_node_contract,
            "WHOLE_PAPER_STATUS.json",
            context="evaluator node contract",
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
            workspace_live_artifact_abs=workspace_root / ".tmp_primary_artifact",
            artifact_publication_receipt_abs=workspace_root / "artifacts" / "publication" / "receipt.json",
            artifact_publication_runner_abs=workspace_root / "PUBLISH_WORKSPACE_ARTIFACT.sh",
            split_runner_abs=workspace_root / "SUBMIT_SPLIT_REQUEST.sh",
            activate_runner_abs=workspace_root / "SUBMIT_ACTIVATE_REQUEST.sh",
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
        if "SUBMIT_SPLIT_REQUEST.sh" not in prompt_text:
            return _fail("bootstrap child prompt must surface an exact split helper path instead of only prose guidance")
        if "SUBMIT_ACTIVATE_REQUEST.sh" not in prompt_text:
            return _fail("bootstrap child prompt must surface an exact activate helper path for deferred children")
        if "after:<node_id>:<requirement>" not in prompt_text:
            return _fail("bootstrap child prompt must pin activation_condition to machine-evaluable after:<node_id>:<requirement> syntax")
        if "activation_rationale" not in prompt_text:
            return _fail("bootstrap child prompt must tell the implementer where explanatory activation prose belongs")
        if "WHOLE_PAPER_STATUS.json" not in prompt_text:
            return _fail("bootstrap child prompt must require structured whole-paper terminal evidence before evaluator")
        if "exact frozen refs in the handoff/prompt as authoritative" not in prompt_text:
            return _fail("bootstrap child prompt must treat exact frozen refs as authoritative during startup")
        if "one concrete workspace-local action" not in prompt_text:
            return _fail("bootstrap child prompt must require one concrete startup action before broad reconnaissance")
        if "at least one non-empty file under the workspace mirror or required artifact/result path" not in prompt_text:
            return _fail("bootstrap child prompt must require a non-empty file materialization during startup")
        if "Creating only an empty directory does not satisfy that startup requirement" not in prompt_text:
            return _fail("bootstrap child prompt must forbid satisfying startup with only an empty directory")
        if "do not keep retrying the same search family in the opening phase" not in prompt_text:
            return _fail("bootstrap child prompt must require startup search downgrade after rate-limit or tool exhaustion")
        if "startup note or checkpoint file alone does not count as substantive startup progress" not in prompt_text:
            return _fail("bootstrap child prompt must forbid treating a startup note as substantive startup progress")
        if "actual artifact skeleton or first substantive deliverable batch" not in prompt_text:
            return _fail("bootstrap child prompt must require substantive artifact startup before broad theorem search")
        if "Do not broad-search repo roots, `.loop/**` history, or unrelated evaluator workspaces" not in prompt_text:
            return _fail("bootstrap child prompt must forbid broad history mining when exact frozen refs exist")

    with tempfile.TemporaryDirectory(prefix="loop_product_split_helper_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop" / "split-helper"
        workspace_root = temp_root / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)
        root_node = NodeSpec(
            node_id="root-kernel",
            node_kind="kernel",
            goal_slice="test split helper",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/root/result.json",
            lineage_ref="root-kernel",
            status=NodeStatus.ACTIVE,
        )
        child_node = NodeSpec(
            node_id="child-001",
            node_kind="implementer",
            goal_slice="test direct split helper submission",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["implement", "evaluate", "report", "split_request"],
            workspace_root=str(workspace_root.resolve()),
            delegation_ref="state/delegations/child-001.json",
            result_sink_ref="artifacts/child-001/implementer_result.json",
            lineage_ref="root-kernel->child-001",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id="split-helper-test",
            root_goal="exercise split helper",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        handoff = node_machine_handoff_ref(state_root=state_root, node_id=child_node.node_id)
        handoff.parent.mkdir(parents=True, exist_ok=True)
        handoff.write_text(
            json.dumps(
                {
                    "node_id": child_node.node_id,
                    "round_id": child_node.round_id,
                    "kernel_result_sink_ref": str((state_root / child_node.result_sink_ref).resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        proposal = workspace_root / "SplitRequest.json"
        proposal.write_text(
            json.dumps(
                {
                    "split_mode": "parallel",
                    "reason": "parallelizable proof frontier split",
                    "completed_work": "block A inventory and starter formalization",
                    "remaining_work": "blocks B/C and utility-learning appendix remain independent",
                    "target_nodes": [
                        {"node_id": "child-001-block-bc", "goal_slice": "formalize Block B and Block C"},
                        {"node_id": "child-001-block-e", "goal_slice": "formalize Block E"},
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        result_ref = workspace_root / "SplitRequestResult.json"
        helper = ROOT / "scripts" / "submit_split_request_from_handoff.sh"
        proc = subprocess.run(
            [str(helper), "--handoff-ref", str(handoff), "--proposal-ref", str(proposal), "--result-ref", str(result_ref)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return _fail(
                "repo-shipped split helper must submit a kernel-reviewed split proposal from frozen handoff context; "
                f"stderr={proc.stderr.strip()!r}"
            )
        if not result_ref.exists():
            return _fail("split helper must materialize a deterministic result file")
        result_payload = json.loads(result_ref.read_text(encoding="utf-8"))
        if str(result_payload.get("status") or "") != "ACCEPTED":
            return _fail("split helper must surface ACCEPTED when kernel review accepts the proposal")

        kernel_state_payload = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        kernel_state_payload["nodes"]["child-001"]["status"] = "ACTIVE"
        (state_root / "state" / "kernel_state.json").write_text(
            json.dumps(kernel_state_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        child_state_path = state_root / "state" / "child-001.json"
        child_state_payload = json.loads(child_state_path.read_text(encoding="utf-8"))
        child_state_payload["status"] = "ACTIVE"
        child_state_path.write_text(json.dumps(child_state_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        rationale_proposal = workspace_root / "RationaleSplitRequest.json"
        rationale_proposal.write_text(
            json.dumps(
                {
                    "split_mode": "parallel",
                    "reason": "dependency-gated split with prose explanation should normalize cleanly",
                    "completed_work": "source established the boundary-producing child",
                    "remaining_work": "an immediate helper child and one dependency-gated follow-up child remain",
                    "target_nodes": [
                        {
                            "node_id": "child-001-rationale-anchor",
                            "goal_slice": "materialize an immediate helper branch so the split remains a real parallel split",
                        },
                        {
                            "node_id": "child-001-followup-rationale",
                            "goal_slice": "follow up after the boundary is published",
                            "depends_on_node_ids": ["child-001-block-bc"],
                            "activation_condition": "Activate once the upstream boundary is ready.",
                        }
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        rationale_result_ref = workspace_root / "RationaleSplitRequestResult.json"
        rationale_proc = subprocess.run(
            [str(helper), "--handoff-ref", str(handoff), "--proposal-ref", str(rationale_proposal), "--result-ref", str(rationale_result_ref)],
            capture_output=True,
            text=True,
        )
        if rationale_proc.returncode != 0:
            return _fail(
                "split helper must accept prose activation rationale when depends_on_node_ids already provide the machine gate; "
                f"stderr={rationale_proc.stderr.strip()!r}"
            )
        rationale_payload = json.loads(rationale_result_ref.read_text(encoding="utf-8"))
        if str(rationale_payload.get("status") or "") != "ACCEPTED":
            return _fail("split helper must surface ACCEPTED when prose activation rationale can be normalized safely")
        normalized_state_path = state_root / "state" / "child-001-followup-rationale.json"
        if not normalized_state_path.exists():
            return _fail("accepted dependency-gated prose split must materialize the normalized child state")
        normalized_state = json.loads(normalized_state_path.read_text(encoding="utf-8"))
        if str(normalized_state.get("activation_condition") or "") != "":
            return _fail("safe prose activation rationale must not persist as a machine activation_condition")
        if "upstream boundary is ready" not in str(normalized_state.get("activation_rationale") or ""):
            return _fail("safe prose activation rationale must persist in activation_rationale")

        bad_proposal = workspace_root / "BadSplitRequest.json"
        bad_proposal.write_text(
            json.dumps(
                {
                    "split_mode": "parallel",
                    "reason": "prose-only activation condition with no machine fallback should fail closed",
                    "completed_work": "source prepared a boundary sketch",
                    "remaining_work": "downstream block wants a gate but does not supply dependencies or machine syntax",
                    "target_nodes": [
                        {
                            "node_id": "child-001-bad-followup",
                            "goal_slice": "follow up after some unspecified condition",
                            "activation_condition": "Activate once the upstream boundary is ready.",
                        }
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        bad_result_ref = workspace_root / "BadSplitRequestResult.json"
        bad_proc = subprocess.run(
            [str(helper), "--handoff-ref", str(handoff), "--proposal-ref", str(bad_proposal), "--result-ref", str(bad_result_ref)],
            capture_output=True,
            text=True,
        )
        if bad_proc.returncode == 0:
            return _fail("split helper must reject prose-only activation_condition values when there is no safe machine fallback")
        if not bad_result_ref.exists():
            return _fail("split helper rejection must still materialize a deterministic result file")
        bad_result_payload = json.loads(bad_result_ref.read_text(encoding="utf-8"))
        if str(bad_result_payload.get("status") or "") != "REJECTED":
            return _fail("split helper must surface REJECTED when prose activation_condition lacks a machine fallback")

    with tempfile.TemporaryDirectory(prefix="loop_product_activate_helper_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop" / "activate-helper"
        workspace_root = temp_root / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)
        root_node = NodeSpec(
            node_id="root-kernel",
            node_kind="kernel",
            goal_slice="test activate helper",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/root/result.json",
            lineage_ref="root-kernel",
            status=NodeStatus.ACTIVE,
        )
        child_node = NodeSpec(
            node_id="child-001",
            node_kind="implementer",
            goal_slice="test deferred activate helper submission",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["implement", "evaluate", "report", "split_request"],
            workspace_root=str(workspace_root.resolve()),
            delegation_ref="state/delegations/child-001.json",
            result_sink_ref="artifacts/child-001/implementer_result.json",
            lineage_ref="root-kernel->child-001",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id="activate-helper-test",
            root_goal="exercise activate helper",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        deferred_proposal = workspace_root / "SplitRequest.json"
        deferred_proposal.write_text(
            json.dumps(
                {
                    "split_mode": "deferred",
                    "reason": "defer formalization lane until source checkpoint is done",
                    "completed_work": "source node owns extraction and partition",
                    "remaining_work": "follow-up child formalizes deferred branch after source terminal",
                    "target_nodes": [
                        {
                            "node_id": "child-001-followup",
                            "goal_slice": "formalize the deferred branch",
                            "depends_on_node_ids": ["child-001"],
                            "activation_condition": "after:child-001:terminal",
                        }
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        handoff = node_machine_handoff_ref(state_root=state_root, node_id=child_node.node_id)
        handoff.parent.mkdir(parents=True, exist_ok=True)
        handoff.write_text(
            json.dumps(
                {
                    "node_id": child_node.node_id,
                    "round_id": child_node.round_id,
                    "kernel_result_sink_ref": str((state_root / child_node.result_sink_ref).resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        split_result_ref = workspace_root / "SplitRequestResult.json"
        split_helper = ROOT / "scripts" / "submit_split_request_from_handoff.sh"
        split_proc = subprocess.run(
            [str(split_helper), "--handoff-ref", str(handoff), "--proposal-ref", str(deferred_proposal), "--result-ref", str(split_result_ref)],
            capture_output=True,
            text=True,
        )
        if split_proc.returncode != 0:
            return _fail(
                "activate helper setup requires the deferred split helper path to succeed first; "
                f"stderr={split_proc.stderr.strip()!r}"
            )
        source_state_path = state_root / "state" / "child-001.json"
        source_state = json.loads(source_state_path.read_text(encoding="utf-8"))
        source_state["status"] = "COMPLETED"
        source_state_path.write_text(json.dumps(source_state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        kernel_state_obj = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        kernel_state_obj["nodes"]["child-001"]["status"] = "COMPLETED"
        (state_root / "state" / "kernel_state.json").write_text(
            json.dumps(kernel_state_obj, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        activate_proposal = workspace_root / "ActivateRequest.json"
        activate_proposal.write_text(
            json.dumps(
                {
                    "target_node_id": "child-001-followup",
                    "reason": "source checkpoint completed; activate deferred child",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        activate_result_ref = workspace_root / "ActivateRequestResult.json"
        activate_helper = ROOT / "scripts" / "submit_activate_request_from_handoff.sh"
        activate_proc = subprocess.run(
            [
                str(activate_helper),
                "--handoff-ref",
                str(handoff),
                "--proposal-ref",
                str(activate_proposal),
                "--result-ref",
                str(activate_result_ref),
            ],
            capture_output=True,
            text=True,
        )
        if activate_proc.returncode != 0:
            return _fail(
                "repo-shipped activate helper must submit a kernel-reviewed activate proposal from frozen handoff context; "
                f"stderr={activate_proc.stderr.strip()!r}"
            )
        if not activate_result_ref.exists():
            return _fail("activate helper must materialize a deterministic result file")
        activate_result_payload = json.loads(activate_result_ref.read_text(encoding="utf-8"))
        if str(activate_result_payload.get("status") or "") != "ACCEPTED":
            return _fail("activate helper must surface ACCEPTED when kernel review accepts the proposal")

    print("[loop-system-split-instruction-chain] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
