#!/usr/bin/env python3
"""Regressions for publication receipt / live / publish atomic recovery."""

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
    print(f"[loop-system-publication-receipt-atomic-recovery][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_workspace_handoff(
    workspace_root: Path,
    *,
    live_root: Path,
    publish_root: Path,
    receipt_ref: Path,
) -> Path:
    handoff_ref = workspace_root / "FROZEN_HANDOFF.json"
    handoff_ref.write_text(
        json.dumps(
            {
                "workspace_live_artifact_ref": str(live_root.resolve()),
                "workspace_mirror_ref": str(publish_root.resolve()),
                "artifact_publication_receipt_ref": str(receipt_ref.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return handoff_ref


def _write_whole_paper_live_artifact(live_root: Path) -> None:
    live_root.mkdir(parents=True, exist_ok=True)
    (live_root / "README.md").write_text("# Whole-Paper Startup Bundle\n", encoding="utf-8")
    (live_root / "TRACEABILITY.md").write_text("# Whole-Paper Startup Traceability\n", encoding="utf-8")
    (live_root / "WHOLE_PAPER_STATUS.json").write_text(
        json.dumps(
            {
                "node_id": "whole-paper-terminal-001",
                "status": "TERMINAL",
                "whole_paper_terminal_authority": True,
                "whole_paper_terminal_classification": "EXTERNAL_DEPENDENCY_BLOCKED",
                "integrated_child_count": 5,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _make_node(*, node_id: str, parent_node_id: str | None = "root") -> "NodeSpec":
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id=node_id,
        node_kind="implementer",
        goal_slice="exercise publication receipt atomic recovery",
        parent_node_id=parent_node_id,
        generation=1 if parent_node_id else 0,
        round_id="R1",
        execution_policy={"sandbox_mode": "danger-full-access"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["evaluate", "report"],
        workflow_scope="whole_paper_formalization",
        artifact_scope="task",
        terminal_authority_scope="whole_paper",
        delegation_ref=f"state/delegations/{node_id}.json",
        result_sink_ref=f"artifacts/{node_id}/implementer_result.json",
        lineage_ref=node_id if parent_node_id is None else f"{parent_node_id}->{node_id}",
        workspace_root="",
        status=NodeStatus.ACTIVE,
    )


def _publication_ready_rebuilds_missing_publish_root_case() -> int:
    from loop_product.dispatch.publication import (
        publish_workspace_artifact_snapshot,
        workspace_publication_ready_for_terminal_state,
    )

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_publication_ready_rebuild_"))
    try:
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace" / "whole-paper"
        live_root = workspace_root / ".tmp_primary_artifact"
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        receipt_ref = (
            state_root
            / "artifacts"
            / "publication"
            / "whole-paper"
            / "WorkspaceArtifactPublicationReceipt.json"
        )
        _write_whole_paper_live_artifact(live_root)
        handoff_ref = _write_workspace_handoff(
            workspace_root,
            live_root=live_root,
            publish_root=publish_root,
            receipt_ref=receipt_ref,
        )
        publish_workspace_artifact_snapshot(
            node_id="whole-paper",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=receipt_ref,
        )
        shutil.rmtree(publish_root)
        report = workspace_publication_ready_for_terminal_state(
            workspace_root=workspace_root,
            machine_handoff_ref=handoff_ref,
            required_output_paths=["WHOLE_PAPER_STATUS.json"],
        )
        if not bool(report.get("ready")):
            return _fail("publication ready gate must rebuild a missing publish root when receipt/live root still match")
        if not publish_root.exists():
            return _fail("publication ready gate must materialize publish root after auto-repair")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _evaluator_preflight_repairs_missing_publish_root_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.loop.evaluator_client import (
        _enforce_publication_receipt_preflight,
        build_evaluator_submission_for_frozen_task,
    )

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_evaluator_preflight_publish_repair_"))
    try:
        workspace_root = temp_root / "workspace"
        live_root = workspace_root / ".tmp_primary_artifact"
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        receipt_ref = temp_root / ".loop" / "artifacts" / "publication" / "node" / "WorkspaceArtifactPublicationReceipt.json"
        _write_whole_paper_live_artifact(live_root)
        publish_workspace_artifact_snapshot(
            node_id="publication-repair-node",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=receipt_ref,
        )
        shutil.rmtree(publish_root)
        manual_ref = workspace_root / "PRODUCT_MANUAL.md"
        final_effects_ref = workspace_root / "FINAL_EFFECTS.md"
        manual_ref.write_text("# Manual\n", encoding="utf-8")
        final_effects_ref.write_text("# Final Effects\n\n- whole-paper faithful complete formalization\n", encoding="utf-8")
        submission = build_evaluator_submission_for_frozen_task(
            target_node=_make_node(node_id="publication-repair-node"),
            workspace_root=workspace_root,
            output_root=temp_root / ".loop" / "artifacts" / "evaluator_runs",
            implementation_package_ref=publish_root,
            artifact_publication_receipt_ref=receipt_ref,
            product_manual_ref=manual_ref,
            final_effects_text_ref=final_effects_ref,
            required_output_paths=["WHOLE_PAPER_STATUS.json"],
        )
        try:
            _enforce_publication_receipt_preflight(submission)
        except Exception as exc:  # pragma: no cover - regression guard
            return _fail(f"evaluator publication preflight must repair missing publish root instead of raising: {exc}")
        if not publish_root.exists():
            return _fail("evaluator publication preflight must restore the missing publish root")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _deliverable_surface_source_roots_repair_missing_publish_root_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.evaluator import prototype as evaluator_prototype
    from loop_product.evaluator import simple as evaluator_simple

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_publication_surface_source_roots_repair_"))
    try:
        workspace_root = temp_root / "workspace"
        live_root = workspace_root / ".tmp_primary_artifact"
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        receipt_ref = temp_root / ".loop" / "artifacts" / "publication" / "node" / "WorkspaceArtifactPublicationReceipt.json"
        _write_whole_paper_live_artifact(live_root)
        publish_workspace_artifact_snapshot(
            node_id="publication-surface-roots-repair-node",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=receipt_ref,
        )
        shutil.rmtree(publish_root)
        request = {
            "implementation_package_ref": str(publish_root.resolve()),
            "artifact_publication_receipt_ref": str(receipt_ref.resolve()),
        }
        for resolver in (
            evaluator_simple._deliverable_surface_source_roots,
            evaluator_prototype._deliverable_surface_source_roots,
        ):
            roots = resolver(request)
            if publish_root.resolve() not in roots:
                return _fail("deliverable surface source-root resolution must repair and include the restored publish root")
        if not publish_root.exists():
            return _fail("deliverable surface source-root repair must restore the publish root")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _unrepairable_publication_surface_is_retryable_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.query import load_kernel_state
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.loop.evaluator_client import (
        build_evaluator_submission_for_frozen_task,
        run_evaluator_node,
    )
    from loop_product.protocols.evaluator import EvaluatorVerdict
    from loop_product.protocols.node import NodeSpec, NodeStatus

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_publication_retryable_recovery_"))
    try:
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        live_root = workspace_root / ".tmp_primary_artifact"
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        receipt_ref = state_root / "artifacts" / "publication" / "node" / "WorkspaceArtifactPublicationReceipt.json"
        ensure_runtime_tree(state_root)
        root_node = NodeSpec(
            node_id="publication-retry-root",
            node_kind="kernel",
            goal_slice="exercise retryable publication surface recovery",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/publication_retry_root/result.json",
            lineage_ref="publication-retry-root",
            status=NodeStatus.ACTIVE,
        )
        child_node = _make_node(node_id="publication-retry-child", parent_node_id=root_node.node_id)
        child_node.workspace_root = str(workspace_root.resolve())
        kernel_state = KernelState(
            task_id="publication-retryable-recovery",
            root_goal="repair or recover publication receipt inconsistencies without terminalizing implementer node",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        _write_whole_paper_live_artifact(live_root)
        publish_workspace_artifact_snapshot(
            node_id=child_node.node_id,
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=receipt_ref,
        )
        shutil.rmtree(publish_root)
        shutil.rmtree(live_root)
        manual_ref = workspace_root / "PRODUCT_MANUAL.md"
        final_effects_ref = workspace_root / "FINAL_EFFECTS.md"
        manual_ref.write_text("# Manual\n", encoding="utf-8")
        final_effects_ref.write_text("# Final Effects\n\n- whole-paper faithful complete formalization\n", encoding="utf-8")
        submission = build_evaluator_submission_for_frozen_task(
            target_node=child_node,
            workspace_root=workspace_root,
            output_root=state_root / "artifacts" / "evaluator_runs",
            implementation_package_ref=publish_root,
            artifact_publication_receipt_ref=receipt_ref,
            product_manual_ref=manual_ref,
            final_effects_text_ref=final_effects_ref,
            required_output_paths=["WHOLE_PAPER_STATUS.json"],
        )
        result, runtime_refs = run_evaluator_node(
            state_root=state_root,
            submission=submission,
        )
        if result.verdict is not EvaluatorVerdict.STUCK:
            return _fail("unrepairable publication-surface inconsistency must surface as STUCK")
        if result.retryable is not True:
            return _fail("unrepairable publication-surface inconsistency must remain retryable")
        if str((result.diagnostics or {}).get("issue_kind") or "") != "publication_surface":
            return _fail("retryable publication-surface inconsistency must expose issue_kind=publication_surface")
        impl_ref = Path(
            str(runtime_refs.get("workspace_implementer_result_ref") or runtime_refs.get("implementer_result_ref") or "")
        )
        if not impl_ref.exists():
            return _fail("retryable publication-surface inconsistency must still materialize implementer_result")
        impl_payload = json.loads(impl_ref.read_text(encoding="utf-8"))
        if str(impl_payload.get("outcome") or "") != "REPAIR_REQUIRED":
            return _fail("retryable publication-surface implementer_result must preserve REPAIR_REQUIRED outcome")
        kernel_state_after = load_kernel_state(state_root)
        child_after = dict(kernel_state_after.nodes.get(child_node.node_id) or {})
        if str(child_after.get("status") or "") != "ACTIVE":
            return _fail("retryable publication-surface inconsistency must leave implementer node ACTIVE")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def _reset_publish_root_revokes_receipt_case() -> int:
    from loop_product.dispatch.publication import publish_workspace_artifact_snapshot, reset_workspace_publish_root

    temp_root = Path(tempfile.mkdtemp(prefix="loop_system_publish_reset_receipt_revoke_"))
    try:
        workspace_root = temp_root / "workspace"
        live_root = workspace_root / ".tmp_primary_artifact"
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        receipt_ref = temp_root / ".loop" / "artifacts" / "publication" / "node" / "WorkspaceArtifactPublicationReceipt.json"
        _write_whole_paper_live_artifact(live_root)
        handoff_ref = _write_workspace_handoff(
            workspace_root,
            live_root=live_root,
            publish_root=publish_root,
            receipt_ref=receipt_ref,
        )
        publish_workspace_artifact_snapshot(
            node_id="publish-reset-node",
            live_artifact_ref=live_root,
            publish_artifact_ref=publish_root,
            publication_receipt_ref=receipt_ref,
        )
        reset_report = reset_workspace_publish_root(
            workspace_root=workspace_root,
            machine_handoff_ref=handoff_ref,
        )
        if publish_root.exists():
            return _fail("destructive publish-root reset must remove the published mirror")
        if receipt_ref.exists():
            return _fail("destructive publish-root reset must also revoke the stale publication receipt")
        if not bool(reset_report.get("removed_publish_root")):
            return _fail("publish-root reset must report removed_publish_root=true when publish root existed")
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0


def main() -> int:
    cases = [
        _publication_ready_rebuilds_missing_publish_root_case,
        _evaluator_preflight_repairs_missing_publish_root_case,
        _deliverable_surface_source_roots_repair_missing_publish_root_case,
        _unrepairable_publication_surface_is_retryable_case,
        _reset_publish_root_revokes_receipt_case,
    ]
    for case in cases:
        rc = case()
        if rc:
            return rc
    print("[loop-system-publication-receipt-atomic-recovery][OK] atomic publication recovery regressions hold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
