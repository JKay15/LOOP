#!/usr/bin/env python3
"""Validate canonical reference inventory for evaluator-staging publish-tree holders."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-publish-tree-reference-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-evaluator-publish-tree-reference-inventory-view",
        root_goal="validate evaluator publish-tree reference inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise evaluator publish-tree reference inventory validation",
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
    kernel_state.register_node(root_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _write_publish_tree(root: Path, *, label: str) -> Path:
    (root / "formalization").mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text(f"# {label}\n", encoding="utf-8")
    (root / "formalization" / "STATUS.md").write_text(f"{label}: ready\n", encoding="utf-8")
    return root.resolve()


def _tree_identity(root: Path) -> tuple[str, int]:
    from loop_product.artifact_hygiene import artifact_byte_size, artifact_fingerprint

    fingerprint = artifact_fingerprint(root, ignore_runtime_heavy=True)
    return (
        f"sha256:{str(fingerprint.get('fingerprint') or '').strip()}",
        int(artifact_byte_size(root, ignore_runtime_heavy=True)),
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.artifact_hygiene import classify_publish_tree_reference_holder
        from loop_product.kernel.query import (
            query_heavy_object_reference_inventory_view,
            query_heavy_object_reference_trace_view,
        )
        from loop_product.kernel.submit import (
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_evaluator_publish_tree_reference_inventory_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        evaluator_workspace_root = (
            repo_root
            / ".loop"
            / "evaluator-publish-tree-reference"
            / "artifacts"
            / "evaluator_runs"
            / "lane_a"
            / ".loop"
            / "ai_user"
            / "workspace"
        )
        evaluator_artifact_ref = _write_publish_tree(
            evaluator_workspace_root / "deliverables" / "primary_artifact",
            label="evaluator-publish-tree-reference",
        )
        evaluator_staging_ref = _write_publish_tree(
            evaluator_workspace_root
            / "deliverables"
            / ".primary_artifact.publish.reference"
            / "primary_artifact",
            label="evaluator-publish-tree-reference",
        )
        object_id, byte_size = _tree_identity(evaluator_artifact_ref)

        artifact_holder = classify_publish_tree_reference_holder(evaluator_artifact_ref, repo_root=repo_root)
        if str(artifact_holder.get("reference_holder_kind") or "") != "evaluator_workspace_artifact_root":
            return _fail("evaluator publish-tree artifact holder must classify as evaluator_workspace_artifact_root")
        if str(artifact_holder.get("reference_holder_ref") or "") != str(evaluator_artifact_ref):
            return _fail("evaluator publish-tree artifact holder must preserve the evaluator primary_artifact root")

        staging_holder = classify_publish_tree_reference_holder(evaluator_staging_ref, repo_root=repo_root)
        if str(staging_holder.get("reference_holder_kind") or "") != "evaluator_workspace_publication_staging_root":
            return _fail("evaluator publish-tree staging holder must classify as evaluator_workspace_publication_staging_root")
        if str(staging_holder.get("reference_holder_ref") or "") != str(evaluator_staging_ref.parent):
            return _fail("evaluator publish-tree staging holder must preserve the evaluator staging root")

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="publish_tree",
            object_ref=evaluator_artifact_ref,
            byte_size=byte_size,
            reason="register evaluator publish tree before attaching evaluator-staging references",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
            registration_kind="evaluator_publish_tree_registration",
        )

        artifact_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="publish_tree",
            object_ref=evaluator_artifact_ref,
            reference_ref=str(artifact_holder.get("reference_ref") or ""),
            reference_holder_kind=str(artifact_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(artifact_holder.get("reference_holder_ref") or ""),
            reference_kind="evaluator_publish_tree_reference",
            reason="attach canonical evaluator publish-tree artifact reference",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
            trigger_kind="evaluator_publish_tree_reference_test",
            trigger_ref="artifact-root",
        )
        staging_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="publish_tree",
            object_ref=evaluator_artifact_ref,
            reference_ref=str(staging_holder.get("reference_ref") or ""),
            reference_holder_kind=str(staging_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(staging_holder.get("reference_holder_ref") or ""),
            reference_kind="evaluator_publish_tree_reference",
            reason="attach canonical evaluator publish-tree staging reference",
            runtime_name="evaluator-runtime",
            repo_root=repo_root,
            trigger_kind="evaluator_publish_tree_reference_test",
            trigger_ref="staging-root",
        )

        inventory = query_heavy_object_reference_inventory_view(
            state_root,
            object_id=object_id,
            object_ref=evaluator_artifact_ref,
        )
        if not bool(inventory.get("read_only")):
            return _fail("evaluator publish-tree reference inventory must stay read-only")
        if int(inventory.get("active_reference_count") or 0) != 2:
            return _fail("evaluator publish-tree reference inventory must report two active references")
        if {
            str(item) for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {
            "evaluator_workspace_artifact_root",
            "evaluator_workspace_publication_staging_root",
        }:
            return _fail("evaluator publish-tree reference inventory must surface both evaluator holder kinds")
        if {
            str(item) for item in list(inventory.get("active_reference_refs") or [])
        } != {str(evaluator_artifact_ref), str(evaluator_staging_ref)}:
            return _fail("evaluator publish-tree reference inventory must preserve both evaluator refs")
        if inventory.get("gaps"):
            return _fail(f"complete evaluator publish-tree reference inventory must not report gaps, got {inventory['gaps']!r}")

        artifact_trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(artifact_envelope.envelope_id or ""))
        artifact_referenced_object = dict(artifact_trace.get("referenced_object") or {})
        if str(artifact_referenced_object.get("reference_holder_kind") or "") != "evaluator_workspace_artifact_root":
            return _fail("evaluator publish-tree reference trace must preserve the evaluator artifact holder kind")

        staging_trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(staging_envelope.envelope_id or ""))
        staging_referenced_object = dict(staging_trace.get("referenced_object") or {})
        if str(staging_referenced_object.get("reference_holder_kind") or "") != "evaluator_workspace_publication_staging_root":
            return _fail("evaluator publish-tree reference trace must preserve the evaluator staging holder kind")

    print(
        "[loop-system-evaluator-publish-tree-reference-inventory-view][OK] "
        "evaluator publish-tree reference inventory and trace preserve evaluator artifact plus evaluator staging holder truth"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
