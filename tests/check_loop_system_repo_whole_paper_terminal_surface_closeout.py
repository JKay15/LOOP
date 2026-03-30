#!/usr/bin/env python3
"""Targeted regressions for whole-paper terminal surface closeout."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-whole-paper-terminal-surface-closeout][FAIL] {msg}", file=sys.stderr)
    return 2


def _whole_paper_preflight_accepts_canonical_terminal_classification_case() -> int:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.loop.evaluator_client import (
        _enforce_whole_paper_preflight,
        build_evaluator_submission_for_frozen_task,
    )
    from loop_product.protocols.node import NodeSpec, NodeStatus

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_whole_paper_preflight_canonical_"))
    try:
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        artifact_root = workspace_root / "deliverables" / "primary_artifact"
        artifact_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)

        root_node = NodeSpec(
            node_id="whole-paper-root",
            node_kind="kernel",
            goal_slice="exercise whole-paper canonical classification preflight",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/whole_paper_root/result.json",
            lineage_ref="whole-paper-root",
            status=NodeStatus.ACTIVE,
        )
        final_node = NodeSpec(
            node_id="whole-paper-closeout",
            node_kind="implementer",
            goal_slice="close the whole paper once the canonical terminal artifact exists",
            parent_node_id=root_node.node_id,
            generation=1,
            round_id="R1.final",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report"],
            workflow_scope="whole_paper_formalization",
            artifact_scope="task",
            terminal_authority_scope="whole_paper",
            delegation_ref="state/delegations/whole-paper-closeout.json",
            result_sink_ref="artifacts/whole_paper_closeout/result.json",
            lineage_ref="whole-paper-root->whole-paper-closeout",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id="whole-paper-terminal-surface-closeout",
            root_goal="accept canonical whole-paper terminal classifications at evaluator preflight",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(final_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        (artifact_root / "README.md").write_text("# README\n", encoding="utf-8")
        (artifact_root / "TRACEABILITY.md").write_text("# Traceability\n", encoding="utf-8")
        (artifact_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "status": "TERMINAL",
                    "whole_paper_terminal_classification": "PAPER_DEFECT_EXPOSED",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        manual_ref = workspace_root / "PRODUCT_MANUAL.md"
        manual_ref.write_text("# Manual\n\nWhole-paper final integration.\n", encoding="utf-8")
        final_effects_ref = workspace_root / "FINAL_EFFECTS.md"
        final_effects_ref.write_text(
            "\n".join(
                [
                    "# Final Effects",
                    "",
                    "- whole-paper faithful complete formalization",
                    "- paper defect exposed",
                    "- external dependency blocked",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        submission = build_evaluator_submission_for_frozen_task(
            target_node=final_node,
            workspace_root=workspace_root,
            output_root=state_root / "artifacts" / "evaluator_node_runs",
            implementation_package_ref=artifact_root,
            product_manual_ref=manual_ref,
            final_effects_text_ref=final_effects_ref,
        )
        try:
            _enforce_whole_paper_preflight(submission)
        except ValueError as exc:
            return _fail(
                "whole-paper evaluator preflight must accept canonical whole_paper_terminal_classification "
                f"without raising, got: {exc}"
            )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _whole_paper_publication_harmonizes_terminal_surface_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.runtime_paths import node_live_artifact_root

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_whole_paper_publication_harmonization_"))
    try:
        state_root = temp_root / ".loop"
        workspace_root = state_root.parent / "workspace" / "whole-paper-terminal-001"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="whole-paper-terminal-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# Whole-Paper Startup Bundle\n", encoding="utf-8")
        (live_root / "TRACEABILITY.md").write_text("# Whole-Paper Startup Traceability\n", encoding="utf-8")
        (live_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "node_id": "whole-paper-terminal-001",
                    "round_id": "R1",
                    "source_tex_ref": "/tmp/source.tex",
                    "status": "TERMINAL",
                    "whole_paper_terminal_authority": True,
                    "whole_paper_terminal_classification": "PAPER_DEFECT_EXPOSED",
                    "terminal_drivers": [
                        {
                            "label": "thm:counterexample",
                            "kind": "paper_defect",
                            "summary": "The printed theorem is false as written.",
                        }
                    ],
                    "notes": [
                        "Faithfulness outranks completion.",
                        "Whole-paper closeout remains truthful and defect-exposed.",
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        publish_root = workspace_root / "deliverables" / "primary_artifact"
        publish_workspace_artifact_snapshot(
            node_id="whole-paper-terminal-001",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "whole-paper-terminal-001"
            / "WorkspaceArtifactPublicationReceipt.json",
        )

        published_readme = (publish_root / "README.md").read_text(encoding="utf-8").lower()
        if "whole-paper integration closeout" not in published_readme or "paper_defect_exposed" not in published_readme:
            return _fail("whole-paper terminal publication must harmonize README.md away from startup-bundle wording")
        if "thm:counterexample" not in published_readme:
            return _fail("whole-paper terminal publication must carry terminal-driver evidence into README.md")

        published_traceability = (publish_root / "TRACEABILITY.md").read_text(encoding="utf-8").lower()
        if "whole-paper terminal classification" not in published_traceability or "thm:counterexample" not in published_traceability:
            return _fail("whole-paper terminal publication must harmonize TRACEABILITY.md with terminal classification and driver evidence")

        published_status = json.loads((publish_root / "WHOLE_PAPER_STATUS.json").read_text(encoding="utf-8"))
        if str(published_status.get("whole_paper_terminal_classification") or "") != "PAPER_DEFECT_EXPOSED":
            return _fail("whole-paper terminal publication must preserve canonical whole_paper_terminal_classification")
        if published_status.get("whole_paper_terminal_authority") is not True:
            return _fail("whole-paper terminal publication must preserve whole_paper_terminal_authority")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _whole_paper_publication_harmonizes_integration_consistency_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.runtime_paths import node_live_artifact_root

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_whole_paper_publication_integration_consistency_"))
    try:
        state_root = temp_root / ".loop"
        workspace_root = state_root.parent / "workspace" / "whole-paper-terminal-002"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="whole-paper-terminal-002",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        live_root.mkdir(parents=True, exist_ok=True)

        proofs_child_artifact_root = workspace_root / "children" / "proofs_child" / "deliverables" / "primary_artifact"
        setup_child_artifact_root = workspace_root / "children" / "setup_child" / "deliverables" / "primary_artifact"
        dynamics_child_artifact_root = workspace_root / "children" / "dynamics_child" / "deliverables" / "primary_artifact"
        golden_child_artifact_root = workspace_root / "children" / "golden_child" / "deliverables" / "primary_artifact"
        setup_result_ref = state_root / "artifacts" / "Problem__Setup_formalization" / "result.json"
        setup_eval_ref = state_root / "artifacts" / "Problem__Setup_formalization" / "EvaluationReport.json"
        dynamics_result_ref = state_root / "artifacts" / "Dynamics__between__Annotator__and__Company_formalization" / "result.json"
        dynamics_eval_ref = state_root / "artifacts" / "Dynamics__between__Annotator__and__Company_formalization" / "EvaluationReport.json"
        golden_result_ref = (
            state_root / "artifacts" / "Golden__Questions__Effectiveness__and__Selection__Criteria_formalization" / "result.json"
        )
        golden_eval_ref = (
            state_root
            / "artifacts"
            / "Golden__Questions__Effectiveness__and__Selection__Criteria_formalization"
            / "EvaluationReport.json"
        )
        proofs_result_ref = state_root / "artifacts" / "Proofs__and__Discussions_formalization" / "result.json"
        proofs_eval_ref = state_root / "artifacts" / "Proofs__and__Discussions_formalization" / "EvaluationReport.json"

        (proofs_child_artifact_root / "external_dependencies").mkdir(parents=True, exist_ok=True)
        (proofs_child_artifact_root / "analysis").mkdir(parents=True, exist_ok=True)
        (setup_child_artifact_root / "analysis").mkdir(parents=True, exist_ok=True)
        (dynamics_child_artifact_root / "analysis").mkdir(parents=True, exist_ok=True)
        (golden_child_artifact_root / "analysis").mkdir(parents=True, exist_ok=True)
        (setup_child_artifact_root / "ProblemSetupFormalization").mkdir(parents=True, exist_ok=True)
        (dynamics_child_artifact_root / "DynamicsBetweenAnnotatorAndCompanyFormalization").mkdir(parents=True, exist_ok=True)
        (golden_child_artifact_root / "GoldenQuestionsSelection").mkdir(parents=True, exist_ok=True)
        (proofs_child_artifact_root / "ProofsAndDiscussions" / "Appendix").mkdir(parents=True, exist_ok=True)

        (setup_child_artifact_root / "ProblemSetupFormalization" / "AnnotationModel.lean").write_text(
            "def AnnotationModelSurface : Prop := True\n",
            encoding="utf-8",
        )
        (dynamics_child_artifact_root / "DynamicsBetweenAnnotatorAndCompanyFormalization" / "Theory.lean").write_text(
            "def DynamicsTheorySurface : Prop := True\n",
            encoding="utf-8",
        )
        (golden_child_artifact_root / "GoldenQuestionsSelection" / "Basic.lean").write_text(
            "def GoldenQuestionsSurface : Prop := True\n",
            encoding="utf-8",
        )
        (proofs_child_artifact_root / "ProofsAndDiscussions" / "Appendix" / "BinaryUpperBound.lean").write_text(
            "def BinaryUpperBoundSurface : Prop := True\n",
            encoding="utf-8",
        )

        for result_ref, evaluation_report_ref, child_artifact_ref in (
            (setup_result_ref, setup_eval_ref, setup_child_artifact_root),
            (dynamics_result_ref, dynamics_eval_ref, dynamics_child_artifact_root),
            (golden_result_ref, golden_eval_ref, golden_child_artifact_root),
            (proofs_result_ref, proofs_eval_ref, proofs_child_artifact_root),
        ):
            result_ref.parent.mkdir(parents=True, exist_ok=True)
            result_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "outcome": "COMPLETED",
                        "delivered_artifact_exactly_evaluated": True,
                        "delivered_artifact_ref": str(child_artifact_ref.resolve()),
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "evaluator": {
                            "verdict": "PASS",
                            "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            evaluation_report_ref.parent.mkdir(parents=True, exist_ok=True)
            evaluation_report_ref.write_text(
                json.dumps(
                    {
                        "status": "COMPLETED",
                        "verdict": "PASS",
                        "workspace_mirror_ref": str(child_artifact_ref.resolve()),
                        "reviewer_response_ref": str((child_artifact_ref / "TRACEABILITY.md").resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

        (proofs_child_artifact_root / "external_dependencies" / "EXTERNAL_DEPENDENCY_LEDGER.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "dependencies": [
                        {
                            "citation_key": "liu2025humans",
                            "status": "RESOLVED_SOURCE_EXTRACTED_AND_MAPPED",
                            "proof_impact": "proof_relevant",
                            "used_by": ["Theorem `thm:binary` lower bound proof"],
                            "why_resolved": ["An authoritative child closed this citation family."],
                            "evidence_refs": [
                                "external_dependencies/liu2025humans_closure_report.md",
                                "external_dependencies/liu2025humans_dependency_surface.json",
                                "external_dependencies/liu2025humans_provenance.md",
                            ],
                            "child_result_ref": "/tmp/liu/result.json",
                            "child_evaluation_report_ref": "/tmp/liu/EvaluationReport.json",
                        },
                        {
                            "citation_key": "nagaev1965some",
                            "status": "UNRESOLVED_MISSING_LOCAL_SOURCE",
                            "proof_impact": "proof_relevant",
                            "used_by": ["Theorem `thm:binary` upper-bound proof"],
                            "why_blocking": ["No local source was found."],
                            "evidence_refs": [
                                "external_dependencies/CITATION_CONTEXTS.md",
                                "external_dependencies/nonuniform_berry_esseen_provenance.md",
                            ],
                            "required_next_step_if_closure_is_desired": "Provide a local copy of the cited paper.",
                        },
                        {
                            "citation_key": "michel1981constant",
                            "status": "UNRESOLVED_MISSING_LOCAL_SOURCE",
                            "proof_impact": "proof_relevant",
                            "used_by": ["Theorem `thm:binary` upper-bound proof"],
                            "why_blocking": ["No local source was found."],
                            "evidence_refs": [
                                "external_dependencies/CITATION_CONTEXTS.md",
                                "external_dependencies/nonuniform_berry_esseen_provenance.md",
                            ],
                            "required_next_step_if_closure_is_desired": "Provide a local copy of the cited paper.",
                        },
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (proofs_child_artifact_root / "analysis" / "THM_BINARY_OBLIGATIONS.md").write_text(
            "\n".join(
                [
                    "# Theorem `thm:binary` Obligations",
                    "",
                    "1. Partial-moment transfer:",
                    "   prove the truncated first moment transfer under the non-uniform Berry-Esseen bound.",
                    "2. Tail-probability transfer:",
                    "   prove the failure-probability transfer at the first-order-condition threshold.",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        (setup_child_artifact_root / "analysis" / "problem_setup_source_map.json").write_text(
            json.dumps(
                {
                    "items": [
                        {
                            "id": "setup.dataset_and_behavior_parameter",
                            "line_start": 124,
                            "line_end": 139,
                            "artifact_targets": ["ProblemSetupFormalization/AnnotationModel.lean"],
                            "lean_symbols": [
                                "AnnotatedDatum",
                                "BehavioralAnnotationModel",
                            ],
                        },
                        {
                            "id": "setup.golden_questions_and_annotation_system",
                            "line_start": 192,
                            "line_end": 212,
                            "artifact_targets": ["ProblemSetupFormalization/GoldenQuestions.lean"],
                            "lean_symbols": [
                                "MonitoringDatasetSetup",
                                "AnnotationSystem",
                            ],
                        },
                    ]
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (dynamics_child_artifact_root / "analysis" / "SECTION_SOURCE_MAP.json").write_text(
            json.dumps(
                {
                    "targets": [
                        {
                            "paper_anchor": {
                                "description": "Binary contract algorithm and reject-region test",
                                "label": "alg_binary_contr",
                                "start_line": 297,
                                "end_line": 319,
                            },
                            "lean_counterpart": "DynamicsBetweenAnnotatorAndCompanyFormalization.Theory.BinaryContract",
                            "status": "formalized_as_structure",
                        },
                        {
                            "paper_anchor": {
                                "description": "Regular principal-agent assumptions",
                                "label": "assum:regular_PA_model",
                                "start_line": 391,
                                "end_line": 403,
                            },
                            "lean_counterpart": "DynamicsBetweenAnnotatorAndCompanyFormalization.Theory.RegularPAAssumptions",
                            "status": "formalized_as_assumption_pack",
                        },
                    ]
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (dynamics_child_artifact_root / "analysis" / "FORMALIZATION_LEDGER.json").write_text(
            json.dumps(
                {
                    "formalization_bindings": [
                        {
                            "binding_id": "binding_alg_binary_contract",
                            "claim_id": "alg_binary_contr",
                            "formalization_status": "FORMALIZED",
                            "lean_target": {
                                "declaration_name": "binaryContractSurface",
                                "file_path": "DynamicsBetweenAnnotatorAndCompanyFormalization/Theory.lean",
                            },
                        }
                    ]
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (golden_child_artifact_root / "analysis" / "FORMALIZATION_NOTES.md").write_text(
            "\n".join(
                [
                    "# Formalization Notes",
                    "",
                    "## Certainty-scaling note",
                    "",
                    "- In the main text, the preference-data certainty score is written as `|p̂ - 1/2|`.",
                    "- In the appendix, the certainty estimator is normalized to `2 * |p̂ - 1/2|`.",
                    "- The artifact records both forms explicitly to preserve notation fidelity.",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        (live_root / "analysis").mkdir(parents=True, exist_ok=True)
        (live_root / "extraction").mkdir(parents=True, exist_ok=True)
        (live_root / "external_dependencies").mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# Whole-Paper Integration Closeout\n", encoding="utf-8")
        (live_root / "TRACEABILITY.md").write_text(
            "# Whole-Paper Traceability\n\n- `liu2025humans`: resolved\n", encoding="utf-8"
        )
        (live_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "node_id": "whole-paper-terminal-002",
                    "round_id": "R1",
                    "source_tex_ref": "/Users/test/source/main.tex",
                    "status": "TERMINAL",
                    "artifact_scope": "task",
                    "whole_paper_terminal_authority": True,
                    "whole_paper_terminal_classification": "EXTERNAL_DEPENDENCY_BLOCKED",
                    "classification": "external_dependency_blocked",
                    "child_dependency_gate": {
                        "all_dependency_nodes_terminal": True,
                        "child_evaluation_report_refs": [str(setup_eval_ref.resolve())],
                        "child_result_refs": [str(setup_result_ref.resolve())],
                        "integrated_child_count": 1,
                    },
                    "coverage": {
                        "all_extracted_blocks_integrated": True,
                        "all_theorem_like_items_accounted_for": True,
                        "block_count": 4,
                        "internal_cross_references_closed": True,
                        "internal_dependency_edge_count": 5,
                        "open_internal_cross_references": [],
                        "section_count": 30,
                        "theorem_like_count": 21,
                        "theorem_like_item_count_materialized": 21,
                    },
                    "docs_used": [
                        "/Users/test/docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
                        "/Users/test/.agents/skills/loop-runner/SKILL.md",
                    ],
                    "proof_relevant_external_dependencies": {
                        "blocked": ["nagaev1965some", "michel1981constant"],
                        "resolved": ["liu2025humans"],
                        "count": 3,
                        "ledger_ref": "external_dependencies/EXTERNAL_DEPENDENCY_LEDGER.json",
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "extraction" / "theorem_inventory.json").write_text(
            json.dumps({"theorem_like_count": 21, "items": [{} for _ in range(21)]}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (live_root / "extraction" / "source_structure.json").write_text(
            json.dumps({"section_count": 24}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (live_root / "analysis" / "internal_dependency_graph.json").write_text(
            json.dumps({"node_count": 21, "edge_count": 5, "nodes": [], "edges": []}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        (live_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "task",
                    "coverage_summary": {
                        "all_extracted_blocks_integrated": True,
                        "all_theorem_like_items_accounted_for": True,
                        "block_count": 4,
                        "block_partition_ref": "analysis/block_partition_draft.json",
                        "formalizer_owned_internal_gaps": [],
                        "internal_cross_references_closed_by_integrated_blocks": True,
                        "internal_dependency_edge_count": 19,
                        "internal_dependency_graph_ref": "analysis/internal_dependency_graph.json",
                        "inventory_ref": "extraction/theorem_inventory.json",
                        "open_internal_cross_references": [],
                        "section_count": 30,
                        "theorem_like_count": 19,
                        "theorem_like_item_count_materialized": 19,
                    },
                    "integrated_children": [
                        {
                            "node_id": "Problem__Setup_formalization",
                            "delivered_artifact_ref": str(setup_child_artifact_root.resolve()),
                            "evaluation_report_ref": str(setup_eval_ref.resolve()),
                            "result_ref": str(setup_result_ref.resolve()),
                            "traceability_ref": "/Users/test/workspace/setup/TRACEABILITY.md",
                        },
                        {
                            "node_id": "Dynamics__between__Annotator__and__Company_formalization",
                            "delivered_artifact_ref": str(dynamics_child_artifact_root.resolve()),
                            "evaluation_report_ref": str(dynamics_eval_ref.resolve()),
                            "result_ref": str(dynamics_result_ref.resolve()),
                            "traceability_ref": "/Users/test/workspace/dynamics/TRACEABILITY.md",
                        },
                        {
                            "node_id": "Golden__Questions__Effectiveness__and__Selection__Criteria_formalization",
                            "delivered_artifact_ref": str(golden_child_artifact_root.resolve()),
                            "evaluation_report_ref": str(golden_eval_ref.resolve()),
                            "result_ref": str(golden_result_ref.resolve()),
                            "traceability_ref": "/Users/test/workspace/golden/TRACEABILITY.md",
                        },
                        {
                            "node_id": "Proofs__and__Discussions_formalization",
                            "delivered_artifact_ref": str(proofs_child_artifact_root.resolve()),
                            "evaluation_report_ref": str(proofs_eval_ref.resolve()),
                            "result_ref": str(proofs_result_ref.resolve()),
                            "traceability_ref": "/Users/test/workspace/child/TRACEABILITY.md",
                        }
                    ],
                    "evidence_refs": [
                        "analysis/PAPER_DEFECT_EXPOSURE.json",
                        str(proofs_result_ref.resolve()),
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "analysis" / "PAPER_DEFECT_EXPOSURE.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "task",
                    "node_id": "whole-paper-terminal-002",
                    "summary": "paper defect summary",
                    "source_tex_ref": "/Users/test/source/main.tex",
                    "evidence_refs": [
                        "/Users/test/workspace/child/analysis/THM_BINARY_PROOF_DEFECTS.json",
                        "/Users/test/.loop/child/result.json",
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (live_root / "external_dependencies" / "EXTERNAL_DEPENDENCY_LEDGER.json").write_text(
            json.dumps(
                {
                    "artifact_scope": "slice",
                    "whole_paper_terminal_claim": "not_owned_by_this_slice",
                    "dependencies": None,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        for relpath in (
            "CITATION_CONTEXTS.md",
            "liu2025humans_closure_report.md",
            "liu2025humans_dependency_surface.json",
            "liu2025humans_provenance.md",
            "nonuniform_berry_esseen_provenance.md",
        ):
            target = live_root / "external_dependencies" / relpath
            target.write_text(f"{relpath}\n", encoding="utf-8")

        publish_root = workspace_root / "deliverables" / "primary_artifact"
        publish_workspace_artifact_snapshot(
            node_id="whole-paper-terminal-002",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=state_root
            / "artifacts"
            / "publication"
            / "whole-paper-terminal-002"
            / "WorkspaceArtifactPublicationReceipt.json",
        )

        published_summary = json.loads(
            (publish_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json").read_text(encoding="utf-8")
        )
        coverage = dict(published_summary.get("coverage_summary") or {})
        if int(coverage.get("section_count") or 0) != 24:
            return _fail("whole-paper publication must normalize integration coverage_summary section_count from source_structure")
        if int(coverage.get("theorem_like_count") or 0) != 21:
            return _fail("whole-paper publication must normalize integration coverage_summary theorem_like_count")
        if int(coverage.get("theorem_like_item_count_materialized") or 0) != 21:
            return _fail("whole-paper publication must normalize theorem_like_item_count_materialized")
        if int(coverage.get("internal_dependency_edge_count") or 0) != 5:
            return _fail("whole-paper publication must normalize internal_dependency_edge_count from the graph")
        if str(coverage.get("proof_relevant_inventory_ref") or "") != "extraction/proof_relevant_inventory.json":
            return _fail("whole-paper publication must publish a proof_relevant_inventory_ref in the integration summary")
        published_children = [dict(item) for item in list(published_summary.get("integrated_children") or []) if isinstance(item, dict)]
        if len(published_children) != 4:
            return _fail("whole-paper publication must preserve integrated_children entries in the published summary")
        for child in published_children:
            node_id = str(child.get("node_id") or "")
            for key in ("delivered_artifact_ref", "evaluation_report_ref", "result_ref"):
                rel = str(child.get(key) or "")
                if not rel:
                    return _fail(f"whole-paper publication must preserve {key} for integrated child {node_id}")
                if rel.startswith("/"):
                    return _fail(f"whole-paper publication must rewrite {key} for integrated child {node_id} into a local relative ref")
                if not (publish_root / rel).exists():
                    return _fail(f"whole-paper publication must materialize local evidence for {key} of integrated child {node_id}")

        published_inventory = json.loads(
            (publish_root / "extraction" / "proof_relevant_inventory.json").read_text(encoding="utf-8")
        )
        summary_payload = dict(published_inventory.get("summary") or {})
        if int(summary_payload.get("definition_count") or 0) < 1:
            return _fail("whole-paper publication must materialize definition/model surfaces in proof_relevant_inventory")
        if int(summary_payload.get("notation_item_count") or 0) < 1:
            return _fail("whole-paper publication must materialize notation coverage in proof_relevant_inventory")
        if int(summary_payload.get("implicit_proof_obligation_count") or 0) < 1:
            return _fail("whole-paper publication must materialize implicit proof obligations in proof_relevant_inventory")
        content_kinds = {
            str(item.get("content_kind") or "")
            for item in list(published_inventory.get("items") or [])
            if isinstance(item, dict)
        }
        for required_kind in ("definition", "notation", "implicit_proof_obligation"):
            if required_kind not in content_kinds:
                return _fail(
                    f"whole-paper publication proof_relevant_inventory must include reviewer-visible {required_kind} items"
                )

        published_ledger = json.loads(
            (publish_root / "external_dependencies" / "EXTERNAL_DEPENDENCY_LEDGER.json").read_text(encoding="utf-8")
        )
        if str(published_ledger.get("artifact_scope") or "") != "task":
            return _fail("whole-paper publication must normalize external dependency ledger to artifact_scope=task")
        if str(published_ledger.get("whole_paper_terminal_claim") or "") != "external_dependency_blocked":
            return _fail("whole-paper publication must write the whole-paper terminal claim into the parent ledger")
        dependencies = {
            str(item.get("citation_key") or ""): item
            for item in list(published_ledger.get("dependencies") or [])
            if isinstance(item, dict)
        }
        if str(dict(dependencies.get("liu2025humans") or {}).get("status") or "") != "RESOLVED_SOURCE_EXTRACTED_AND_MAPPED":
            return _fail("whole-paper publication must preserve resolved liu2025humans status in the parent ledger")
        if str(dict(dependencies.get("nagaev1965some") or {}).get("status") or "") != "UNRESOLVED_MISSING_LOCAL_SOURCE":
            return _fail("whole-paper publication must preserve unresolved external blockers in the parent ledger")
        summary = dict(published_ledger.get("summary") or {})
        if list(summary.get("resolved_citation_keys") or []) != ["liu2025humans"]:
            return _fail("whole-paper publication must summarize resolved external dependencies coherently")
        if list(summary.get("blocking_citation_keys") or []) != ["nagaev1965some", "michel1981constant"]:
            return _fail("whole-paper publication must summarize blocking external dependencies coherently")
        published_status = json.loads((publish_root / "WHOLE_PAPER_STATUS.json").read_text(encoding="utf-8"))
        if str(published_status.get("source_tex_ref") or "") != "main.tex":
            return _fail("whole-paper publication must collapse source_tex_ref to a shipped-surface label instead of an external absolute path")
        if int(dict(published_status.get("coverage") or {}).get("section_count") or 0) != 24:
            return _fail("whole-paper publication must normalize whole-paper status coverage section_count from source_structure")
        if str(dict(published_status.get("coverage") or {}).get("proof_relevant_inventory_ref") or "") != "extraction/proof_relevant_inventory.json":
            return _fail("whole-paper publication must publish proof_relevant_inventory_ref in whole-paper status coverage")
        child_dependency_gate = dict(published_status.get("child_dependency_gate") or {})
        child_result_refs = [str(item) for item in list(child_dependency_gate.get("child_result_refs") or []) if str(item)]
        child_evaluation_report_refs = [
            str(item) for item in list(child_dependency_gate.get("child_evaluation_report_refs") or []) if str(item)
        ]
        if len(child_result_refs) != 4 or any(ref.startswith("/") for ref in child_result_refs):
            return _fail("whole-paper publication must preserve local child_result_refs in the shipped status surface")
        if len(child_evaluation_report_refs) != 4 or any(ref.startswith("/") for ref in child_evaluation_report_refs):
            return _fail("whole-paper publication must preserve local child_evaluation_report_refs in the shipped status surface")
        if any(not (publish_root / ref).exists() for ref in child_result_refs):
            return _fail("whole-paper publication must materialize every child_result_ref on-surface")
        if any(not (publish_root / ref).exists() for ref in child_evaluation_report_refs):
            return _fail("whole-paper publication must materialize every child_evaluation_report_ref on-surface")
        if any(str(item).startswith("/") for item in list(published_status.get("docs_used") or [])):
            return _fail("whole-paper publication must rewrite docs_used into non-absolute shipped-surface labels")
        published_defect_payload = json.loads(
            (publish_root / "analysis" / "PAPER_DEFECT_EXPOSURE.json").read_text(encoding="utf-8")
        )
        if list(published_defect_payload.get("evidence_refs") or []):
            return _fail("whole-paper publication must strip off-surface paper defect evidence refs from the shipped artifact")

        for relpath in (
            "WHOLE_PAPER_STATUS.json",
            "analysis/WHOLE_PAPER_INTEGRATION_SUMMARY.json",
            "analysis/PAPER_DEFECT_EXPOSURE.json",
            "external_dependencies/EXTERNAL_DEPENDENCY_LEDGER.json",
            "extraction/proof_relevant_inventory.json",
            "integrated_children/problem_setup_formalization/result.json",
            "integrated_children/problem_setup_formalization/EvaluationReport.json",
        ):
            published_text = (publish_root / relpath).read_text(encoding="utf-8")
            if str(temp_root) in published_text or any(token in published_text for token in ("/Users/test/", "/workspace/", "/.loop/")):
                return _fail(f"whole-paper publication must not ship off-surface absolute runtime refs in {relpath}")
        if not (
            publish_root
            / "integrated_children"
            / "problem_setup_formalization"
            / "primary_artifact"
            / "ProblemSetupFormalization"
            / "AnnotationModel.lean"
        ).exists():
            return _fail("whole-paper publication must materialize child Lean payloads inside the integrated child evidence bundle")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _whole_paper_authoritative_result_projects_summary_surface_case() -> int:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, synchronize_authoritative_node_results
    from loop_product.protocols.node import NodeSpec, NodeStatus

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_whole_paper_authoritative_projection_"))
    try:
        state_root = temp_root / ".loop"
        ensure_runtime_tree(state_root)

        root_node = NodeSpec(
            node_id="whole-paper-root",
            node_kind="kernel",
            goal_slice="root harness for whole-paper authoritative projection",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/whole_paper_root/result.json",
            lineage_ref="whole-paper-root",
            status=NodeStatus.ACTIVE,
        )
        target_node = NodeSpec(
            node_id="whole-paper-closeout",
            node_kind="implementer",
            goal_slice="project whole-paper authoritative result summary/ref surface into node snapshots",
            parent_node_id=root_node.node_id,
            generation=1,
            round_id="R1.final",
            execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report"],
            workflow_scope="whole_paper_formalization",
            artifact_scope="task",
            terminal_authority_scope="whole_paper",
            delegation_ref="state/delegations/whole-paper-closeout.json",
            result_sink_ref="artifacts/whole_paper_closeout/result.json",
            lineage_ref="whole-paper-root->whole-paper-closeout",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id="whole-paper-terminal-authoritative-projection",
            root_goal="project whole-paper authoritative result surfaces into node snapshots",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(target_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        result_ref = state_root / "artifacts" / "whole_paper_closeout" / "result.json"
        evaluation_report_ref = temp_root / "EvaluationReport.json"
        reviewer_response_ref = temp_root / "reviewer_response.txt"
        workspace_result_sink_ref = temp_root / "workspace_result.json"
        workspace_mirror_ref = temp_root / "deliverables" / "primary_artifact"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        workspace_mirror_ref.mkdir(parents=True, exist_ok=True)
        evaluation_report_ref.write_text("{}\n", encoding="utf-8")
        reviewer_response_ref.write_text("PASS\n\nFinal whole-paper classification: PAPER_DEFECT_EXPOSED.\n", encoding="utf-8")
        workspace_result_sink_ref.write_text("{}\n", encoding="utf-8")
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": target_node.node_id,
                    "status": "COMPLETED",
                    "outcome": "COMPLETED",
                    "summary": "simple evaluator reviewer verdict=PASS",
                    "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                    "reviewer_response_ref": str(reviewer_response_ref.resolve()),
                    "workspace_result_sink_ref": str(workspace_result_sink_ref.resolve()),
                    "workspace_mirror_ref": str(workspace_mirror_ref.resolve()),
                    "evaluator": {
                        "verdict": "PASS",
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "reviewer_response_ref": str(reviewer_response_ref.resolve()),
                    },
                    "evaluator_result": {
                        "verdict": "PASS",
                        "retryable": False,
                        "summary": "simple evaluator reviewer verdict=PASS",
                        "diagnostics": {},
                        "evidence_refs": [],
                        "lane": "reviewer",
                        "recovery": {},
                    },
                    "runtime_refs": {
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "reviewer_response_ref": str(reviewer_response_ref.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        synchronize_authoritative_node_results(
            state_root,
            continue_deferred=False,
            authority=kernel_internal_authority(),
        )

        node_snapshot = json.loads((state_root / "state" / f"{target_node.node_id}.json").read_text(encoding="utf-8"))
        kernel_state_payload = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        kernel_node = dict(dict(kernel_state_payload.get("nodes") or {}).get(target_node.node_id) or {})

        for surface in (node_snapshot, kernel_node):
            if str(surface.get("status") or "") != "COMPLETED":
                return _fail("whole-paper authoritative result projection must terminalize both node snapshot and kernel node graph")
            if str(surface.get("authoritative_result_ref") or "") != str(result_ref.resolve()):
                return _fail("whole-paper authoritative result projection must expose authoritative_result_ref")
            if str(surface.get("authoritative_result_summary") or "") != "simple evaluator reviewer verdict=PASS":
                return _fail("whole-paper authoritative result projection must expose authoritative_result_summary")
            if str(surface.get("authoritative_result_status") or "") != "COMPLETED":
                return _fail("whole-paper authoritative result projection must expose authoritative_result_status")
            if str(surface.get("authoritative_result_outcome") or "") != "COMPLETED":
                return _fail("whole-paper authoritative result projection must expose authoritative_result_outcome")
            if str(surface.get("authoritative_evaluator_verdict") or "") != "PASS":
                return _fail("whole-paper authoritative result projection must expose authoritative_evaluator_verdict")
            if str(surface.get("authoritative_evaluation_report_ref") or "") != str(evaluation_report_ref.resolve()):
                return _fail("whole-paper authoritative result projection must expose authoritative_evaluation_report_ref")
            if str(surface.get("authoritative_reviewer_response_ref") or "") != str(reviewer_response_ref.resolve()):
                return _fail("whole-paper authoritative result projection must expose authoritative_reviewer_response_ref")
            if str(surface.get("authoritative_workspace_result_sink_ref") or "") != str(workspace_result_sink_ref.resolve()):
                return _fail("whole-paper authoritative result projection must expose authoritative_workspace_result_sink_ref")
            if str(surface.get("authoritative_workspace_mirror_ref") or "") != str(workspace_mirror_ref.resolve()):
                return _fail("whole-paper authoritative result projection must expose authoritative_workspace_mirror_ref")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _whole_paper_root_adopts_terminal_closeout_child_case() -> int:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, synchronize_authoritative_node_results
    from loop_product.protocols.node import NodeSpec, NodeStatus

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_whole_paper_root_closeout_adoption_"))
    try:
        state_root = temp_root / ".loop"
        ensure_runtime_tree(state_root)

        root_workspace_root = temp_root / "workspace" / "whole-paper-root"
        child_workspace_root = temp_root / "workspace" / "whole-paper-closeout"
        child_artifact_root = child_workspace_root / "deliverables" / "primary_artifact"
        child_artifact_root.mkdir(parents=True, exist_ok=True)

        root_node = NodeSpec(
            node_id="whole-paper-root",
            node_kind="implementer",
            goal_slice="root harness for whole-paper closeout child adoption",
            parent_node_id="root-kernel",
            generation=1,
            round_id="R1",
            execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["implement", "evaluate", "report", "split_request", "resume_request", "retry_request", "relaunch_request"],
            workflow_scope="whole_paper_formalization",
            artifact_scope="task",
            terminal_authority_scope="whole_paper",
            delegation_ref="state/delegations/whole-paper-root.json",
            result_sink_ref="artifacts/whole_paper_root/implementer_result.json",
            lineage_ref="root-kernel->whole-paper-root",
            workspace_root=str(root_workspace_root.resolve()),
            status=NodeStatus.BLOCKED,
        )
        target_node = NodeSpec(
            node_id="whole-paper-closeout",
            node_kind="implementer",
            goal_slice="dedicated whole-paper closeout child",
            parent_node_id=root_node.node_id,
            generation=2,
            round_id="R1.final",
            execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report"],
            workflow_scope="whole_paper_formalization",
            artifact_scope="task",
            terminal_authority_scope="whole_paper",
            delegation_ref="state/delegations/whole-paper-closeout.json",
            result_sink_ref="artifacts/whole_paper_closeout/result.json",
            lineage_ref="root-kernel->whole-paper-root->whole-paper-closeout",
            workspace_root=str(child_workspace_root.resolve()),
            status=NodeStatus.COMPLETED,
        )
        kernel_state = KernelState(
            task_id="whole-paper-root-closeout-adoption",
            root_goal="root node should adopt dedicated whole-paper closeout child truth",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(target_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        root_result_ref = state_root / "artifacts" / "whole_paper_root" / "implementer_result.json"
        child_result_ref = state_root / "artifacts" / "whole_paper_closeout" / "result.json"
        evaluation_report_ref = temp_root / "EvaluationReport.json"
        reviewer_response_ref = temp_root / "reviewer_response.txt"
        workspace_result_sink_ref = temp_root / "workspace_result.json"
        root_result_ref.parent.mkdir(parents=True, exist_ok=True)
        child_result_ref.parent.mkdir(parents=True, exist_ok=True)
        evaluation_report_ref.write_text("{}\n", encoding="utf-8")
        reviewer_response_ref.write_text("PASS\n\nFinal whole-paper classification: PAPER_DEFECT_EXPOSED.\n", encoding="utf-8")
        workspace_result_sink_ref.write_text("{}\n", encoding="utf-8")

        root_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": root_node.node_id,
                    "status": "BLOCKED",
                    "outcome": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
                    "summary": "root is still pinned to the initial split-continuation sink",
                    "workspace_mirror_ref": str((root_workspace_root / "deliverables" / "primary_artifact").resolve()),
                    "workspace_result_sink_ref": str((root_workspace_root / "artifacts" / "whole_paper_root" / "implementer_result.json").resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        child_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": target_node.node_id,
                    "status": "COMPLETED",
                    "outcome": "COMPLETED",
                    "summary": "simple evaluator reviewer verdict=PASS",
                    "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                    "reviewer_response_ref": str(reviewer_response_ref.resolve()),
                    "workspace_result_sink_ref": str(workspace_result_sink_ref.resolve()),
                    "workspace_mirror_ref": str(child_artifact_root.resolve()),
                    "evaluator": {
                        "verdict": "PASS",
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "reviewer_response_ref": str(reviewer_response_ref.resolve()),
                    },
                    "evaluator_result": {
                        "verdict": "PASS",
                        "retryable": False,
                        "summary": "simple evaluator reviewer verdict=PASS",
                        "diagnostics": {},
                        "evidence_refs": [],
                        "lane": "reviewer",
                        "recovery": {},
                    },
                    "runtime_refs": {
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "reviewer_response_ref": str(reviewer_response_ref.resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (child_artifact_root / "README.md").write_text("# Whole-Paper Integration Closeout\n", encoding="utf-8")
        (child_artifact_root / "TRACEABILITY.md").write_text("# Whole-Paper Terminal Traceability\n", encoding="utf-8")
        (child_artifact_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "node_id": target_node.node_id,
                    "round_id": target_node.round_id,
                    "status": "TERMINAL",
                    "whole_paper_terminal_authority": True,
                    "whole_paper_terminal_classification": "PAPER_DEFECT_EXPOSED",
                    "terminal_drivers": [
                        {
                            "label": "thm:counterexample",
                            "kind": "paper_defect",
                            "summary": "The printed theorem is false as written.",
                        }
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        synchronize_authoritative_node_results(
            state_root,
            continue_deferred=False,
            authority=kernel_internal_authority(),
        )

        node_snapshot = json.loads((state_root / "state" / f"{root_node.node_id}.json").read_text(encoding="utf-8"))
        kernel_state_payload = json.loads((state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        kernel_node = dict(dict(kernel_state_payload.get("nodes") or {}).get(root_node.node_id) or {})

        for surface in (node_snapshot, kernel_node):
            if str(surface.get("status") or "") != "COMPLETED":
                return _fail("whole-paper root must adopt dedicated closeout child truth and terminalize to COMPLETED")
            if str(surface.get("authoritative_result_ref") or "") != str(child_result_ref.resolve()):
                return _fail("whole-paper root must expose the closeout child authoritative_result_ref")
            if str(surface.get("authoritative_result_summary") or "") != "simple evaluator reviewer verdict=PASS":
                return _fail("whole-paper root must expose the closeout child authoritative_result_summary")
            if str(surface.get("authoritative_workspace_mirror_ref") or "") != str(child_artifact_root.resolve()):
                return _fail("whole-paper root must expose the closeout child authoritative_workspace_mirror_ref")
            if str(surface.get("authoritative_evaluator_verdict") or "") != "PASS":
                return _fail("whole-paper root must expose the closeout child evaluator verdict")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def main() -> int:
    try:
        rc = _whole_paper_preflight_accepts_canonical_terminal_classification_case()
        if rc:
            return rc
        rc = _whole_paper_publication_harmonizes_terminal_surface_case()
        if rc:
            return rc
        rc = _whole_paper_publication_harmonizes_integration_consistency_case()
        if rc:
            return rc
        rc = _whole_paper_authoritative_result_projects_summary_surface_case()
        if rc:
            return rc
        rc = _whole_paper_root_adopts_terminal_closeout_child_case()
        if rc:
            return rc
    except Exception as exc:  # noqa: BLE001
        return _fail(f"whole-paper terminal surface closeout raised unexpectedly: {exc}")

    print("[loop-system-whole-paper-terminal-surface-closeout] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
