#!/usr/bin/env python3
"""Validate the public first-implementer bootstrap surface."""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-first-child-bootstrap][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import loop_product as loop_product_package
        import loop_product.runtime as runtime_surface

        from loop_product.dispatch.child_dispatch import materialize_child
        from loop_product.dispatch.child_dispatch import load_child_runtime_context
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.policy import (
            implementer_budget_profile,
            implementer_execution_policy,
            implementer_reasoning_profile,
        )
        from loop_product.kernel.state import load_kernel_state
        from loop_product.protocols.node import NodeSpec
        from loop_product.runtime_paths import node_live_artifact_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"bootstrap imports failed: {exc}")

    if not hasattr(runtime_surface, "bootstrap_first_implementer_node"):
        return _fail("loop_product.runtime must expose bootstrap_first_implementer_node")
    if not hasattr(runtime_surface, "bootstrap_first_implementer_from_endpoint"):
        return _fail("loop_product.runtime must expose bootstrap_first_implementer_from_endpoint")
    if not hasattr(loop_product_package, "bootstrap_first_implementer_node"):
        return _fail("loop_product package root must expose bootstrap_first_implementer_node")
    if not hasattr(loop_product_package, "bootstrap_first_implementer_from_endpoint"):
        return _fail("loop_product package root must expose bootstrap_first_implementer_from_endpoint")

    try:
        request_schema = _load_schema("LoopFirstImplementerBootstrapRequest.schema.json")
        result_schema = _load_schema("LoopFirstImplementerBootstrapResult.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"bootstrap schemas missing or invalid: {exc}")

    bootstrap_first_implementer_node = runtime_surface.bootstrap_first_implementer_node
    bootstrap_first_implementer_from_endpoint = runtime_surface.bootstrap_first_implementer_from_endpoint

    with tempfile.TemporaryDirectory(prefix="loop_first_child_bootstrap_") as td:
        temp_root = Path(td)
        required_doc = temp_root / "required-doc.md"
        required_doc.write_text("# required doc\n", encoding="utf-8")
        required_wrapper = temp_root / "ensure_workspace_lake_packages.sh"
        required_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        required_wrapper.chmod(0o755)
        whole_paper_template_ref = (ROOT.parent / "docs" / "design" / "leanatlas_real_autosplit_formalization_experiment_template_v0_1.md").resolve()
        whole_paper_acceptance_ref = (ROOT.parent / "docs" / "design" / "leanatlas_complete_formalization_acceptance_v0_1.md").resolve()
        whole_paper_doc_pack_ref = (ROOT.parent / "docs" / "design" / "leanatlas_single_node_formalization_doc_pack_v0_1.md").resolve()
        operator_workflow_ref = (ROOT.parent / "docs" / "agents" / "OPERATOR_WORKFLOW.md").resolve()
        operator_skill_ref = (ROOT.parent / ".agents" / "skills" / "leanatlas-operator-proof-loop" / "SKILL.md").resolve()
        shared_cache_helper_ref = (ROOT.parent / "scripts" / "ensure_workspace_lake_packages.sh").resolve()
        ancestor_relative_ref = ROOT.parent / ".cache" / "loop_bootstrap_relative_context_ref_test.md"
        ancestor_relative_ref.parent.mkdir(parents=True, exist_ok=True)
        ancestor_relative_ref.write_text("relative context ref\n", encoding="utf-8")
        stale_loop_artifact = ROOT / ".loop" / "context_ref_pollution_fixture" / "artifacts" / "stale.json"
        stale_workspace_artifact = ROOT / "workspace" / "context_ref_pollution_fixture" / "deliverables" / "primary_artifact" / "README.md"
        stale_loop_artifact.parent.mkdir(parents=True, exist_ok=True)
        stale_workspace_artifact.parent.mkdir(parents=True, exist_ok=True)
        stale_loop_artifact.write_text("{}\n", encoding="utf-8")
        stale_workspace_artifact.write_text("# stale runtime artifact\n", encoding="utf-8")
        project_name = "test-first-child-bootstrap"
        workspace_root = ROOT / "workspace" / project_name
        state_root = ROOT / ".loop" / project_name
        if workspace_root.exists():
            shutil.rmtree(workspace_root)
        if state_root.exists():
            shutil.rmtree(state_root)
        try:
            endpoint_artifact = temp_root / "EndpointArtifact.json"
            endpoint_artifact.write_text(
                json.dumps(
                    {
                        "version": "1",
                        "session_root": str((temp_root / "endpoint_session").resolve()),
                        "artifact_ref": str(endpoint_artifact.resolve()),
                        "latest_turn_ref": str((temp_root / "turns" / "0001" / "TurnResult.json").resolve()),
                        "mode": "VISION_COMPILER",
                        "status": "CLARIFIED",
                        "original_user_prompt": (
                            "Create a Gon birthday poster HTML with offline music playback. "
                            f"Read `{required_doc.resolve()}`, use `{required_wrapper.resolve()}` before the first build, "
                            f"and also keep `.cache/{ancestor_relative_ref.name}` as required local context."
                        ),
                        "confirmed_requirements": [],
                        "denied_requirements": [],
                        "question_history": [],
                        "turn_count": 1,
                        "requirement_artifact": {
                            "task_type": "design",
                            "sufficient": True,
                            "user_request_summary": "Deliver a local birthday poster for Gon with offline music playback.",
                            "final_effect": "Deliver a local birthday poster for Gon with offline music playback.",
                            "observable_success_criteria": [
                                "A local HTML birthday poster exists.",
                                "The page plays local music without external links.",
                            ],
                            "hard_constraints": ["Output target is the desktop."],
                            "non_goals": ["Do not use streaming embeds."],
                            "relevant_context": [
                                "The task is already clarified.",
                                str(required_doc.resolve()),
                            ],
                            "open_questions": [],
                            "artifact_ready_for_persistence": True,
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            request = {
                "mode": "fresh",
                "task_slug": "gon birthday poster",
                "root_goal": "deliver the gon birthday poster task through one materialized implementer node",
                "child_goal_slice": "implement the frozen Gon birthday poster and run evaluator before reporting",
                "endpoint_artifact_ref": str(endpoint_artifact.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "workspace_mirror_relpath": "deliverables/gon_birthday_poster.html",
                "external_publish_target": "/Users/example/Desktop/gon_birthday_poster.html",
                "context_refs": [str(endpoint_artifact.resolve())],
            }
            Draft202012Validator(request_schema).validate(request)
            result = bootstrap_first_implementer_node(**request)
            if not isinstance(result, dict):
                return _fail("bootstrap_first_implementer_node must return a dict result")
            Draft202012Validator(result_schema).validate(result)

            if str(result.get("mode") or "") != "fresh":
                return _fail("fresh bootstrap must report mode=fresh")
            if str(result.get("reuse_decision") or "") != "fresh":
                return _fail("fresh bootstrap must report reuse_decision=fresh")
            if Path(str(result.get("workspace_root") or "")).resolve() != workspace_root.resolve():
                return _fail("fresh bootstrap must preserve the requested workspace_root")
            if Path(str(result.get("state_root") or "")).resolve() != state_root.resolve():
                return _fail("fresh bootstrap must preserve the requested state_root")

            handoff_json = Path(str(result.get("handoff_json_ref") or ""))
            handoff_md = Path(str(result.get("handoff_md_ref") or ""))
            child_prompt = Path(str(result.get("child_prompt_ref") or ""))
            node_ref = Path(str(result.get("node_ref") or ""))
            delegation_ref = Path(str(result.get("delegation_ref") or ""))
            bootstrap_result_ref = Path(str(result.get("bootstrap_result_ref") or ""))
            for path in (handoff_json, handoff_md, child_prompt, node_ref, delegation_ref, bootstrap_result_ref):
                if not path.exists():
                    return _fail(f"fresh bootstrap must materialize {path}")

            handoff_payload = json.loads(handoff_json.read_text(encoding="utf-8"))
            if str(handoff_payload.get("endpoint_artifact_ref") or "") != str(endpoint_artifact.resolve()):
                return _fail("frozen handoff must persist endpoint_artifact_ref")
            if str(handoff_payload.get("state_root") or "") != str(state_root.resolve()):
                return _fail("frozen handoff must persist the exact state_root for topology helpers")
            if str(handoff_payload.get("workspace_mirror_relpath") or "") != "deliverables/gon_birthday_poster.html":
                return _fail("frozen handoff must persist the workspace mirror relpath")
            expected_live_artifact_relpath = ".tmp_primary_artifact/gon_birthday_poster.html"
            expected_live_artifact_ref = str((workspace_root / expected_live_artifact_relpath).resolve())
            if str(handoff_payload.get("workspace_live_artifact_relpath") or "") != expected_live_artifact_relpath:
                return _fail("frozen handoff must persist the exact live artifact relpath separate from the publish root")
            if str(handoff_payload.get("workspace_live_artifact_ref") or "") != expected_live_artifact_ref:
                return _fail("frozen handoff must persist the exact live artifact ref separate from the publish root")
            if str(handoff_payload.get("external_publish_target") or "") != "/Users/example/Desktop/gon_birthday_poster.html":
                return _fail("frozen handoff must persist the external publish target")
            expected_workspace_sink_relpath = f"artifacts/{str(result.get('node_id') or '')}/implementer_result.json"
            expected_workspace_sink_ref = str((workspace_root / expected_workspace_sink_relpath).resolve())
            expected_kernel_sink_ref = str((state_root / expected_workspace_sink_relpath).resolve())
            evaluator_bundle_root = (state_root / "artifacts" / "bootstrap" / str(result.get("node_id") or "")).resolve()
            expected_evaluator_submission_ref = str((evaluator_bundle_root / "EvaluatorNodeSubmission.json").resolve())
            expected_evaluator_runner_ref = str((workspace_root / "RUN_EVALUATOR_NODE_UNTIL_TERMINAL.sh").resolve())
            expected_publication_receipt_ref = str(
                (state_root / "artifacts" / "publication" / str(result.get("node_id") or "") / "WorkspaceArtifactPublicationReceipt.json").resolve()
            )
            expected_publication_runner_ref = str((workspace_root / "PUBLISH_WORKSPACE_ARTIFACT.sh").resolve())
            expected_evaluator_manual_ref = str((evaluator_bundle_root / "EvaluatorProductManual.md").resolve())
            expected_evaluator_final_effects_ref = str((evaluator_bundle_root / "EvaluatorFinalEffects.md").resolve())
            if str(handoff_payload.get("workspace_result_sink_relpath") or "") != expected_workspace_sink_relpath:
                return _fail("frozen handoff must persist the workspace-local implementer result mirror relpath")
            if str(handoff_payload.get("workspace_result_sink_ref") or "") != expected_workspace_sink_ref:
                return _fail("frozen handoff must persist the workspace-local implementer result mirror ref")
            if str(handoff_payload.get("kernel_result_sink_ref") or "") != expected_kernel_sink_ref:
                return _fail("frozen handoff must preserve the kernel-visible authoritative sink ref separately from the workspace mirror")
            if str(handoff_payload.get("evaluator_submission_ref") or "") != expected_evaluator_submission_ref:
                return _fail("frozen handoff must persist the exact evaluator submission ref")
            if str(handoff_payload.get("evaluator_runner_ref") or "") != expected_evaluator_runner_ref:
                return _fail("frozen handoff must persist the exact evaluator runner ref")
            if str(handoff_payload.get("artifact_publication_receipt_ref") or "") != expected_publication_receipt_ref:
                return _fail("frozen handoff must persist the exact artifact publication receipt ref")
            if str(handoff_payload.get("artifact_publication_runner_ref") or "") != expected_publication_runner_ref:
                return _fail("frozen handoff must persist the exact workspace-local publication runner ref")
            if str(handoff_payload.get("external_publication_owner") or "") != "root-kernel":
                return _fail("frozen handoff must persist root-kernel as the normal external publication owner")
            if str(handoff_payload.get("external_input_policy") or "") != "read_only_external_inputs_allowed_when_required_by_frozen_endpoint":
                return _fail("frozen handoff must preserve the generic external input discovery policy")
            if str(result.get("workspace_result_sink_ref") or "") != expected_workspace_sink_ref:
                return _fail("bootstrap result must surface the workspace-local implementer result mirror ref")
            if str(result.get("kernel_result_sink_ref") or "") != expected_kernel_sink_ref:
                return _fail("bootstrap result must surface the kernel-visible authoritative implementer result sink ref")
            if str(result.get("evaluator_submission_ref") or "") != expected_evaluator_submission_ref:
                return _fail("bootstrap result must surface the exact evaluator submission ref")
            if str(result.get("evaluator_runner_ref") or "") != expected_evaluator_runner_ref:
                return _fail("bootstrap result must surface the exact evaluator runner ref")
            if str(result.get("workspace_live_artifact_ref") or "") != expected_live_artifact_ref:
                return _fail("bootstrap result must surface the exact live artifact ref separate from the publish root")
            if str(result.get("artifact_publication_receipt_ref") or "") != expected_publication_receipt_ref:
                return _fail("bootstrap result must surface the exact artifact publication receipt ref")
            if str(result.get("artifact_publication_runner_ref") or "") != expected_publication_runner_ref:
                return _fail("bootstrap result must surface the exact workspace-local publication runner ref")
            handoff_md_text = handoff_md.read_text(encoding="utf-8")
            if f"- state_root: `{state_root.resolve()}`" not in handoff_md_text:
                return _fail("frozen handoff markdown must include the exact state_root for helper replay")
            if f"- workspace_live_artifact_ref: `{expected_live_artifact_ref}`" not in handoff_md_text:
                return _fail("frozen handoff markdown must include the exact live artifact ref")

            prompt_text = child_prompt.read_text(encoding="utf-8")
            for needle in (
                "Read and follow:",
                str((workspace_root.parent / "AGENTS.md").resolve()),
                str(handoff_md.resolve()),
                "You are not the root kernel.",
                "Keep durable implementation writes inside the current workspace root, except for the exact authoritative kernel-visible result sink frozen in the handoff.",
                "inspect the necessary repo-local or user-local paths directly and materialize the required copies into the workspace when useful",
                "--root/--term/--ext",
                "broad prose query",
                "canonical local roots first",
                "do not browse, download, or substitute remote assets",
                "non-trivial content transforms",
                "temp path and atomic replace",
                "Do not publish directly to the external publish target",
                expected_evaluator_submission_ref,
                expected_evaluator_runner_ref,
                str((ROOT / ".agents" / "skills" / "loop-runner" / "SKILL.md").resolve()),
                str((ROOT / ".agents" / "skills" / "evaluator-exec" / "SKILL.md").resolve()),
                str((ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()),
                str((ROOT / "scripts" / "find_local_input_candidates.sh").resolve()),
                str((ROOT / "scripts" / "submit_split_request_from_handoff.sh").resolve()),
                str((ROOT / "scripts" / "submit_activate_request_from_handoff.sh").resolve()),
                expected_live_artifact_ref,
                expected_publication_receipt_ref,
                expected_publication_runner_ref,
                expected_kernel_sink_ref,
                expected_workspace_sink_ref,
                "fresh workspace-local Lean package",
                "Build in the live artifact root first and treat the workspace mirror as publish-only",
                "Publish through the exact publication runner before evaluator or terminal report",
                "A child-authored WHOLE_PAPER_STATUS.json or branch README is not a publication receipt",
                "shared-cache helper named in the frozen handoff context refs",
                "before the first `lake build`",
                "live `git clone`, `lake update`, or `lake exe cache get`",
                "artifact-local `.lake/packages`",
                "exact frozen refs in the handoff/prompt as authoritative",
                "do not guess alternate repo-root or lookalike paths",
                "fresh workspace with no deliverable or artifact yet",
                "one concrete workspace-local action",
                "at least one non-empty file under the workspace mirror or required artifact/result path",
                "Creating only an empty directory does not satisfy that startup requirement",
                "opening phase on broad repo scans",
                "startup search or MCP helper reports rate-limit, tool exhaustion, or validation failure",
                "do not keep retrying the same search family in the opening phase",
                "downgrade to direct artifact writing, local proof drafting, or the first build path",
                "If the frozen endpoint already determines the artifact shape, a startup note or checkpoint file alone does not count as substantive startup progress",
                "Materialize the actual artifact skeleton or first substantive deliverable batch before broad theorem search or helper archaeology",
                "If the frozen goal already names staged benchmark phases or required sections, do not stop after creating a placeholder skeleton for those sections",
                "After the skeleton exists, materially advance the first incomplete staged phase with source-backed content before broadening into later phases or open-ended reconnaissance",
                "planned outputs, `pending` tables, TODO notes, or placeholder headings alone do not count as substantive staged progress",
                "Do not broad-search repo roots, `.loop/**` history, or unrelated evaluator workspaces for helper or template discovery when the exact frozen refs already name the required helper or baseline artifacts",
                "If you decide split is warranted, materialize a structured split proposal and call the exact split helper named in this prompt instead of only writing the recommendation into deliverable prose",
                "If kernel accepts a deferred split, do not assume the planned children will start automatically",
                "materialize a structured activate proposal and call the exact activate helper named in this prompt",
                "Do not start evaluator for a staged whole-paper benchmark until the artifact carries structured terminal-classification evidence in `WHOLE_PAPER_STATUS.json`",
                "If your split slice carries declared required outputs",
                "do not start evaluator or mark the slice terminal until those required outputs exist",
                "Before evaluator or final report, the final `deliverables/primary_artifact` must not ship runtime-owned heavy trees",
                "such as `.lake`, `.git`, `.venv`, `.uv-cache`, `build`, or `_lake_build`",
            ):
                if needle not in prompt_text:
                    return _fail(f"child prompt must mention {needle!r}")
            for needle in ("authoritative kernel-visible sink", "workspace-local mirror result sink"):
                if needle not in prompt_text:
                    return _fail(f"child prompt must explain {needle!r}")
            if not Path(expected_evaluator_runner_ref).exists():
                return _fail("bootstrap must materialize the exact evaluator runner script")
            if not Path(expected_publication_runner_ref).exists():
                return _fail("bootstrap must materialize the exact publication runner script")
            if not Path(expected_evaluator_submission_ref).exists():
                return _fail("bootstrap must materialize the exact evaluator submission artifact")
            if not Path(expected_evaluator_manual_ref).exists():
                return _fail("bootstrap must materialize a task-scoped evaluator product manual")
            if not Path(expected_evaluator_final_effects_ref).exists():
                return _fail("bootstrap must materialize task-scoped evaluator final effects")
            if Path(expected_evaluator_submission_ref).parent != evaluator_bundle_root:
                return _fail("bootstrap must materialize the evaluator bundle under a node-scoped bootstrap directory")

            submission_payload = json.loads(Path(expected_evaluator_submission_ref).read_text(encoding="utf-8"))
            if str(submission_payload.get("target_node_id") or "") != str(result.get("node_id") or ""):
                return _fail("node-scoped evaluator submission must target the materialized implementer node")
            if str(submission_payload.get("product_manual_ref") or "") != expected_evaluator_manual_ref:
                return _fail("ordinary implementer bootstrap must point evaluator submission at the task-scoped manual")
            if str(submission_payload.get("final_effects_text_ref") or "") != expected_evaluator_final_effects_ref:
                return _fail("ordinary implementer bootstrap must point evaluator submission at the task-scoped final effects")
            if "endpoint_clarification_part1" in str(submission_payload.get("evaluation_id") or "").lower():
                return _fail("ordinary implementer bootstrap must not reuse endpoint-clarification evaluation ids")
            if "LOOP_ENDPOINT_CLARIFICATION_PART1_PRODUCT_MANUAL.md" in json.dumps(submission_payload, ensure_ascii=False):
                return _fail("ordinary implementer bootstrap must not hardcode endpoint-clarification product manual refs")
            if "LOOP_ENDPOINT_CLARIFICATION_PART1_FINAL_EFFECTS.md" in json.dumps(submission_payload, ensure_ascii=False):
                return _fail("ordinary implementer bootstrap must not hardcode endpoint-clarification final effects refs")
            if str(submission_payload.get("implementation_package_ref") or "") != str((workspace_root / "deliverables" / "gon_birthday_poster.html").resolve()):
                return _fail("ordinary implementer bootstrap must point implementation_package_ref at the workspace mirror artifact")
            if str(submission_payload.get("artifact_publication_receipt_ref") or "") != expected_publication_receipt_ref:
                return _fail("ordinary implementer bootstrap must point evaluator submission at the exact publication receipt ref")
            role_requirements = dict(submission_payload.get("role_requirements") or {})
            for role_id in ("checker", "test_designer", "ai_user", "reviewer"):
                if not str(role_requirements.get(role_id) or "").strip():
                    return _fail(f"ordinary implementer bootstrap must provide a non-empty role requirement for {role_id}")
            context_refs = [str(item) for item in list(submission_payload.get("context_refs") or [])]
            if str(endpoint_artifact.resolve()) not in context_refs:
                return _fail("ordinary implementer bootstrap must preserve the endpoint artifact as evaluator context")
            if str(handoff_json.resolve()) not in context_refs:
                return _fail("ordinary implementer bootstrap must preserve the frozen handoff as evaluator context")

            node = NodeSpec.from_dict(json.loads(node_ref.read_text(encoding="utf-8")))
            if node.node_id != str(result.get("node_id") or ""):
                return _fail("bootstrap result node_id must match the materialized node snapshot")
            if "split_request" not in set(node.allowed_actions or []):
                return _fail("ordinary implementer bootstrap must materialize split_request on the child node")
            if str((node.reasoning_profile or {}).get("thinking_budget") or "") != "xhigh":
                return _fail("ordinary implementer bootstrap must default the primary implementer child to xhigh reasoning")
            runtime_context = load_child_runtime_context(state_root, node.node_id)
            if Path(str(runtime_context.get("workspace_root") or "")).resolve() != workspace_root.resolve():
                return _fail("child runtime context must point at the materialized workspace root")
            delegation_payload = dict(runtime_context.get("delegation") or {})
            if "split_request" not in set(delegation_payload.get("allowed_actions") or []):
                return _fail("ordinary implementer bootstrap must persist split_request in the frozen delegation artifact")

            split_workspace_root = ROOT / "workspace" / "test-first-child-bootstrap-slice"
            if split_workspace_root.exists():
                shutil.rmtree(split_workspace_root)
            split_workspace_root.mkdir(parents=True, exist_ok=True)
            split_node_id = f"{str(result.get('node_id') or '')}__slice"
            split_goal_slice = (
                "faithfully formalize only the bounded utility appendix slice "
                "without mutating the whole-paper coverage ledger owned by the source node"
            )
            kernel_state = load_kernel_state(state_root)
            split_node = materialize_child(
                state_root=state_root,
                kernel_state=kernel_state,
                parent_node_id=str(result.get("node_id") or ""),
                node_id=split_node_id,
                goal_slice=split_goal_slice,
                round_id="R1.S1",
                execution_policy=implementer_execution_policy(),
                reasoning_profile=implementer_reasoning_profile(),
                budget_profile=implementer_budget_profile(),
                workspace_root=split_workspace_root,
                required_output_paths=["README.md", "TRACEABILITY.md", "WHOLE_PAPER_STATUS.json"],
                result_sink_ref=f"artifacts/{split_node_id}/result.json",
                authority=kernel_internal_authority(),
            )
            split_live_root = node_live_artifact_root(
                state_root=state_root,
                node_id=split_node.node_id,
                workspace_mirror_relpath="deliverables/primary_artifact",
            )
            split_live_relpath = os.path.relpath(split_live_root, start=split_workspace_root.resolve())
            split_request = {
                "mode": "continue_exact",
                "task_slug": "gon birthday poster",
                "root_goal": "deliver the gon birthday poster task through one materialized implementer node",
                "child_goal_slice": split_goal_slice,
                "endpoint_artifact_ref": str(endpoint_artifact.resolve()),
                "workspace_root": str(split_workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "node_id": split_node.node_id,
                "round_id": split_node.round_id,
                "workspace_mirror_relpath": "deliverables/primary_artifact",
                "workspace_live_artifact_relpath": split_live_relpath,
                "external_publish_target": "",
                "context_refs": [str(endpoint_artifact.resolve()), str(handoff_json.resolve())],
                "required_output_paths": ["README.md", "TRACEABILITY.md", "WHOLE_PAPER_STATUS.json"],
                "result_sink_ref": split_node.result_sink_ref,
            }
            Draft202012Validator(request_schema).validate(split_request)
            split_bootstrap = bootstrap_first_implementer_node(**split_request)
            Draft202012Validator(result_schema).validate(split_bootstrap)
            split_handoff = json.loads(Path(str(split_bootstrap.get("handoff_json_ref") or "")).read_text(encoding="utf-8"))
            split_child_prompt = Path(str(split_bootstrap.get("child_prompt_ref") or ""))
            if not split_child_prompt.exists():
                return _fail("split child bootstrap must materialize a child prompt")
            split_bundle_root = (state_root / "artifacts" / "bootstrap" / split_node.node_id).resolve()
            split_submission_ref = str((split_bundle_root / "EvaluatorNodeSubmission.json").resolve())
            split_final_effects_ref = str((split_bundle_root / "EvaluatorFinalEffects.md").resolve())
            if str(split_handoff.get("evaluator_submission_ref") or "") != split_submission_ref:
                return _fail("split child bootstrap must persist a node-scoped evaluator submission ref")
            if str(split_handoff.get("workspace_live_artifact_ref") or "") != str(split_live_root.resolve()):
                return _fail("split child bootstrap must preserve the exact external live artifact root")
            split_submission_payload = json.loads(Path(split_submission_ref).read_text(encoding="utf-8"))
            if str(split_submission_payload.get("goal_slice") or "") != split_goal_slice:
                return _fail("split child evaluator submission must preserve the narrowed slice goal")
            split_manual_ref = str(split_submission_payload.get("product_manual_ref") or "")
            if not split_manual_ref:
                return _fail("split child evaluator submission must persist a product_manual_ref")
            split_manual_text = Path(split_manual_ref).read_text(encoding="utf-8")
            if "# Slice-Scoped Evaluator Product Manual" not in split_manual_text:
                return _fail("split child mentioning whole-paper dependency text must still receive the slice-scoped evaluator manual")
            split_final_effects_text = Path(split_final_effects_ref).read_text(encoding="utf-8").lower()
            if "whole-paper faithful complete formalization" in split_final_effects_text:
                return _fail("split child evaluator bundle must not reuse whole-paper final effects text")
            if "paper defect exposed" in split_final_effects_text and "external dependency blocked" in split_final_effects_text:
                return _fail("split child evaluator bundle must stay slice-scoped instead of advertising whole-paper terminal classifications")
            if "must not claim whole-paper terminal classifications" not in split_final_effects_text:
                return _fail("split child evaluator final effects must explicitly forbid whole-paper terminal classification claims")
            if "this node is slice-scoped and does not own whole-paper terminal classification authority" not in split_child_prompt.read_text(encoding="utf-8").lower():
                return _fail("split child prompt must explicitly deny whole-paper terminal classification authority for ordinary slices")

            launch_spec = dict(result.get("launch_spec") or {})
            argv = list(launch_spec.get("argv") or [])
            if argv[:4] != ["codex", "exec", "-C", str(workspace_root.resolve())]:
                return _fail("bootstrap launch spec must pin codex exec to the workspace root via -C")
            if str(launch_spec.get("cwd") or "") != str(workspace_root.resolve()):
                return _fail("bootstrap launch spec must pin cwd to the workspace root")
            if str(launch_spec.get("stdin_path") or "") != str(child_prompt.resolve()):
                return _fail("bootstrap launch spec must point stdin_path at CHILD_PROMPT.md")
            if 'model_reasoning_effort="xhigh"' not in argv:
                return _fail("bootstrap launch spec must request xhigh Codex reasoning for the default implementer child")

            continue_request = dict(request)
            continue_request.update(
                {
                    "mode": "continue_exact",
                    "node_id": str(result.get("node_id") or ""),
                }
            )
            Draft202012Validator(request_schema).validate(continue_request)
            continued = bootstrap_first_implementer_node(**continue_request)
            Draft202012Validator(result_schema).validate(continued)
            if str(continued.get("reuse_decision") or "") != "continue_exact":
                return _fail("continue_exact bootstrap must report reuse_decision=continue_exact")
            if str(continued.get("node_ref") or "") != str(node_ref.resolve()):
                return _fail("continue_exact bootstrap must reuse the exact node_ref")
            if str(continued.get("handoff_json_ref") or "") != str(handoff_json.resolve()):
                return _fail("continue_exact bootstrap must reuse the exact frozen handoff")

            bad_continue = dict(continue_request)
            bad_continue["workspace_root"] = str((ROOT / "workspace" / "wrong").resolve())
            try:
                bootstrap_first_implementer_node(**bad_continue)
            except ValueError:
                pass
            else:
                return _fail("continue_exact bootstrap must reject mismatched exact refs")

            derived_project_name = "test-first-child-bootstrap-from-endpoint"
            derived_workspace_root = ROOT / "workspace" / derived_project_name
            derived_state_root = ROOT / ".loop" / derived_project_name
            if derived_workspace_root.exists():
                shutil.rmtree(derived_workspace_root)
            if derived_state_root.exists():
                shutil.rmtree(derived_state_root)

            derived = bootstrap_first_implementer_from_endpoint(
                endpoint_artifact_ref=str(endpoint_artifact.resolve()),
                external_publish_target="/Users/example/Desktop/gon_birthday_poster.html",
                workspace_root=str(derived_workspace_root.resolve()),
                state_root=str(derived_state_root.resolve()),
            )
            Draft202012Validator(result_schema).validate(derived)
            if Path(str(derived.get("workspace_root") or "")).resolve() != derived_workspace_root.resolve():
                return _fail("endpoint-driven bootstrap must preserve the requested workspace_root")
            if Path(str(derived.get("state_root") or "")).resolve() != derived_state_root.resolve():
                return _fail("endpoint-driven bootstrap must preserve the requested state_root")

            derived_handoff_json = Path(str(derived.get("handoff_json_ref") or ""))
            if not derived_handoff_json.exists():
                return _fail("endpoint-driven bootstrap must materialize the frozen handoff")
            derived_handoff_payload = json.loads(derived_handoff_json.read_text(encoding="utf-8"))
            if str(derived_handoff_payload.get("endpoint_artifact_ref") or "") != str(endpoint_artifact.resolve()):
                return _fail("endpoint-driven bootstrap must preserve endpoint_artifact_ref")
            if str(derived_handoff_payload.get("state_root") or "") != str(derived_state_root.resolve()):
                return _fail("endpoint-driven bootstrap must preserve state_root in the frozen handoff")
            if str(derived_handoff_payload.get("workspace_mirror_relpath") or "") != "deliverables/gon_birthday_poster.html":
                return _fail(
                    "endpoint-driven bootstrap must derive workspace_mirror_relpath from the external publish target basename"
                )
            if str(derived_handoff_payload.get("workspace_live_artifact_relpath") or "") != ".tmp_primary_artifact/gon_birthday_poster.html":
                return _fail("endpoint-driven bootstrap must derive a distinct live artifact relpath from the publish target")
            if "offline music playback" not in str(derived_handoff_payload.get("child_goal_slice") or ""):
                return _fail("endpoint-driven bootstrap must derive child_goal_slice from the endpoint requirement artifact")
            derived_workspace_sink_relpath = f"artifacts/{str(derived.get('node_id') or '')}/implementer_result.json"
            if str(derived_handoff_payload.get("workspace_result_sink_relpath") or "") != derived_workspace_sink_relpath:
                return _fail("endpoint-driven bootstrap must preserve the workspace-local implementer result mirror relpath")

            derived_bootstrap_result = Path(str(derived.get("bootstrap_result_ref") or ""))
            derived_request_path = derived_bootstrap_result.parent / "FirstImplementerBootstrapRequest.json"
            derived_request = json.loads(derived_request_path.read_text(encoding="utf-8"))
            derived_context_refs = [str(item) for item in list(derived_request.get("context_refs") or [])]
            expected_context_refs = {
                str(endpoint_artifact.resolve()),
                str(required_doc.resolve()),
                str(required_wrapper.resolve()),
                str(ancestor_relative_ref.resolve()),
            }
            if not expected_context_refs.issubset(set(derived_context_refs)):
                return _fail("endpoint-driven bootstrap must preserve explicit existing local context refs from the endpoint artifact")
            if derived_context_refs[0] != str(endpoint_artifact.resolve()):
                return _fail("endpoint-driven bootstrap must keep the endpoint artifact as the first context ref")
            derived_handoff_context_refs = {str(item) for item in list(derived_handoff_payload.get("context_refs") or [])}
            if not expected_context_refs.issubset(derived_handoff_context_refs):
                return _fail("endpoint-driven bootstrap must project explicit local context refs into the frozen handoff")

            whole_paper_project_name = "test-first-child-bootstrap-whole-paper"
            whole_paper_workspace_root = ROOT / "workspace" / whole_paper_project_name
            whole_paper_state_root = ROOT / ".loop" / whole_paper_project_name
            shutil.rmtree(whole_paper_workspace_root, ignore_errors=True)
            shutil.rmtree(whole_paper_state_root, ignore_errors=True)

            whole_paper_endpoint_artifact = temp_root / "WholePaperEndpointArtifact.json"
            whole_paper_endpoint_artifact.write_text(
                json.dumps(
                    {
                        "version": "1",
                        "session_root": str((temp_root / "whole_paper_endpoint_session").resolve()),
                        "artifact_ref": str(whole_paper_endpoint_artifact.resolve()),
                        "latest_turn_ref": str((temp_root / "turns" / "0002" / "TurnResult.json").resolve()),
                        "mode": "VISION_COMPILER",
                        "status": "CLARIFIED",
                        "original_user_prompt": (
                            "Run a whole-paper faithful complete formalization benchmark for "
                            "/Users/xiongjiangkai/xjk_papers/leanatlas/.cache/leanatlas/tmp/arxiv_2602_11505v2/source/main.tex."
                        ),
                        "confirmed_requirements": [],
                        "denied_requirements": [],
                        "question_history": [],
                        "turn_count": 1,
                        "requirement_artifact": {
                            "task_type": "research",
                            "sufficient": True,
                            "user_request_summary": "Whole-paper faithful complete formalization benchmark for arxiv_2602_11505v2.",
                            "final_effect": (
                                "Produce a whole-paper faithful complete formalization benchmark using the staged "
                                "Extraction Agent, Partition Agent, Block Formalization Agents, External Dependency Agents, "
                                "and Final Integration / Final Evaluation structure."
                            ),
                            "observable_success_criteria": [
                                "Whole-paper evaluator gives an authoritative terminal result.",
                                "All proof-relevant mathematical content is explicitly extracted and closed or honestly classified.",
                            ],
                            "hard_constraints": [
                                "Faithfulness outranks compilation.",
                                "Do not leave internal gaps.",
                            ],
                            "non_goals": [
                                "Do not reuse a stale single-node bounded-gap artifact as the target outcome.",
                            ],
                            "relevant_context": [
                                str(whole_paper_template_ref),
                                str(whole_paper_acceptance_ref),
                                str(whole_paper_doc_pack_ref),
                                str(shared_cache_helper_ref),
                                str(operator_workflow_ref),
                                str(operator_skill_ref),
                                str(stale_loop_artifact.resolve()),
                                str(stale_workspace_artifact.resolve()),
                            ],
                            "open_questions": [],
                            "artifact_ready_for_persistence": True,
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            whole_paper_bootstrap = bootstrap_first_implementer_from_endpoint(
                endpoint_artifact_ref=str(whole_paper_endpoint_artifact.resolve()),
                workspace_root=str(whole_paper_workspace_root.resolve()),
                state_root=str(whole_paper_state_root.resolve()),
            )
            Draft202012Validator(result_schema).validate(whole_paper_bootstrap)
            whole_paper_bootstrap_result = Path(str(whole_paper_bootstrap.get("bootstrap_result_ref") or ""))
            whole_paper_request_path = whole_paper_bootstrap_result.parent / "FirstImplementerBootstrapRequest.json"
            whole_paper_request = json.loads(whole_paper_request_path.read_text(encoding="utf-8"))
            whole_paper_request_context_refs = {str(item) for item in list(whole_paper_request.get("context_refs") or [])}
            whole_paper_handoff_json = Path(str(whole_paper_bootstrap.get("handoff_json_ref") or ""))
            whole_paper_handoff_payload = json.loads(whole_paper_handoff_json.read_text(encoding="utf-8"))
            whole_paper_handoff_context_refs = {str(item) for item in list(whole_paper_handoff_payload.get("context_refs") or [])}

            expected_whole_paper_refs = {
                str(whole_paper_endpoint_artifact.resolve()),
                str(whole_paper_template_ref),
                str(whole_paper_acceptance_ref),
                str(whole_paper_doc_pack_ref),
                str(shared_cache_helper_ref),
            }
            polluted_whole_paper_refs = {
                str(operator_workflow_ref),
                str(operator_skill_ref),
                str(stale_loop_artifact.resolve()),
                str(stale_workspace_artifact.resolve()),
            }
            if not expected_whole_paper_refs.issubset(whole_paper_request_context_refs):
                return _fail("whole-paper endpoint bootstrap must retain the whole-paper benchmark refs required by the frozen goal")
            if whole_paper_request_context_refs.intersection(polluted_whole_paper_refs):
                return _fail("whole-paper endpoint bootstrap must filter OPERATOR-mode and stale runtime artifact refs from context_refs")
            if whole_paper_handoff_context_refs.intersection(polluted_whole_paper_refs):
                return _fail("whole-paper frozen handoff must not project OPERATOR-mode or stale runtime artifact refs")
        finally:
            ancestor_relative_ref.unlink(missing_ok=True)
            stale_loop_artifact.unlink(missing_ok=True)
            stale_workspace_artifact.unlink(missing_ok=True)
            shutil.rmtree(ROOT / ".loop" / "context_ref_pollution_fixture", ignore_errors=True)
            shutil.rmtree(ROOT / "workspace" / "context_ref_pollution_fixture", ignore_errors=True)
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            shutil.rmtree(ROOT / "workspace" / "test-first-child-bootstrap-slice", ignore_errors=True)
            shutil.rmtree(ROOT / "workspace" / "test-first-child-bootstrap-from-endpoint", ignore_errors=True)
            shutil.rmtree(ROOT / ".loop" / "test-first-child-bootstrap-from-endpoint", ignore_errors=True)
            shutil.rmtree(ROOT / "workspace" / "test-first-child-bootstrap-whole-paper", ignore_errors=True)
            shutil.rmtree(ROOT / ".loop" / "test-first-child-bootstrap-whole-paper", ignore_errors=True)

    print("[loop-system-first-child-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
