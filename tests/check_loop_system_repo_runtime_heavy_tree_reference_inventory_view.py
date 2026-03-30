#!/usr/bin/env python3
"""Validate canonical reference inventory for runtime heavy-tree holder roots."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-heavy-tree-reference-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-runtime-heavy-tree-reference-inventory-view",
        root_goal="validate runtime-heavy-tree reference inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise runtime-heavy-tree reference inventory validation",
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


def _write_runtime_heavy_tree(artifact_root: Path, *, label: str) -> Path:
    tree_root = artifact_root / ".lake"
    (tree_root / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
    (tree_root / "packages" / "mathlib" / "STATUS.txt").write_text(f"{label}: ready\n", encoding="utf-8")
    return tree_root.resolve()


def _tree_identity(root: Path) -> tuple[str, int]:
    from loop_product.artifact_hygiene import heavy_object_identity

    identity = heavy_object_identity(root, object_kind="runtime_heavy_tree")
    return (
        str(identity.get("object_id") or ""),
        int(identity.get("byte_size") or 0),
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.artifact_hygiene import classify_runtime_heavy_tree_reference_holder
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

    with temporary_repo_root(prefix="loop_system_runtime_heavy_tree_reference_inventory_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        workspace_live_ref = _write_runtime_heavy_tree(
            repo_root / "workspace" / "runtime-heavy-reference" / ".tmp_primary_artifact",
            label="runtime-heavy-reference",
        )
        anchor_live_ref = _write_runtime_heavy_tree(
            repo_root
            / ".loop"
            / "runtime-heavy-reference"
            / "artifacts"
            / "live_artifacts"
            / "child-runtime-heavy-reference-001"
            / "primary_artifact",
            label="runtime-heavy-reference",
        )
        staging_ref = _write_runtime_heavy_tree(
            repo_root
            / "workspace"
            / "runtime-heavy-reference"
            / "deliverables"
            / ".primary_artifact.publish.reference"
            / "primary_artifact",
            label="runtime-heavy-reference",
        )
        object_id, byte_size = _tree_identity(workspace_live_ref)

        workspace_holder = classify_runtime_heavy_tree_reference_holder(workspace_live_ref, repo_root=repo_root)
        if str(workspace_holder.get("reference_holder_kind") or "") != "runtime_workspace_live_artifact_root":
            return _fail("workspace live heavy-tree holder must classify as runtime_workspace_live_artifact_root")
        if str(workspace_holder.get("reference_holder_ref") or "") != str(workspace_live_ref.parent):
            return _fail("workspace live heavy-tree holder must preserve the .tmp_primary_artifact root")

        anchor_holder = classify_runtime_heavy_tree_reference_holder(anchor_live_ref, repo_root=repo_root)
        if str(anchor_holder.get("reference_holder_kind") or "") != "runtime_live_artifact_root":
            return _fail("repo-anchored live heavy-tree holder must classify as runtime_live_artifact_root")
        if str(anchor_holder.get("reference_holder_ref") or "") != str(anchor_live_ref.parent):
            return _fail("repo-anchored live heavy-tree holder must preserve the live_artifacts primary_artifact root")

        staging_holder = classify_runtime_heavy_tree_reference_holder(staging_ref, repo_root=repo_root)
        if str(staging_holder.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("staging heavy-tree holder must classify as workspace_publication_staging_root")

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="runtime_heavy_tree",
            object_ref=workspace_live_ref,
            byte_size=byte_size,
            reason="register runtime heavy tree before attaching runtime-heavy references",
            runtime_name="heavy-runtime",
            repo_root=repo_root,
            registration_kind="runtime_heavy_tree_registration",
        )

        workspace_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="runtime_heavy_tree",
            object_ref=workspace_live_ref,
            reference_ref=str(workspace_holder.get("reference_ref") or ""),
            reference_holder_kind=str(workspace_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(workspace_holder.get("reference_holder_ref") or ""),
            reference_kind="runtime_heavy_tree_reference",
            reason="attach canonical runtime-heavy workspace live reference",
            runtime_name="heavy-runtime",
            repo_root=repo_root,
            trigger_kind="runtime_heavy_tree_reference_test",
            trigger_ref="workspace-live",
        )
        anchor_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="runtime_heavy_tree",
            object_ref=workspace_live_ref,
            reference_ref=str(anchor_holder.get("reference_ref") or ""),
            reference_holder_kind=str(anchor_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(anchor_holder.get("reference_holder_ref") or ""),
            reference_kind="runtime_heavy_tree_reference",
            reason="attach canonical repo-anchored runtime live heavy-tree reference",
            runtime_name="heavy-runtime",
            repo_root=repo_root,
            trigger_kind="runtime_heavy_tree_reference_test",
            trigger_ref="anchor-live",
        )
        staging_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="runtime_heavy_tree",
            object_ref=workspace_live_ref,
            reference_ref=str(staging_holder.get("reference_ref") or ""),
            reference_holder_kind=str(staging_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(staging_holder.get("reference_holder_ref") or ""),
            reference_kind="runtime_heavy_tree_reference",
            reason="attach canonical publication staging heavy-tree reference",
            runtime_name="heavy-runtime",
            repo_root=repo_root,
            trigger_kind="runtime_heavy_tree_reference_test",
            trigger_ref="staging",
        )

        inventory = query_heavy_object_reference_inventory_view(
            state_root,
            object_id=object_id,
            object_ref=workspace_live_ref,
        )
        if not bool(inventory.get("read_only")):
            return _fail("runtime-heavy-tree reference inventory must stay read-only")
        if int(inventory.get("active_reference_count") or 0) != 3:
            return _fail("runtime-heavy-tree reference inventory must report three active references")
        if {
            str(item) for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {
            "runtime_workspace_live_artifact_root",
            "runtime_live_artifact_root",
            "workspace_publication_staging_root",
        }:
            return _fail("runtime-heavy-tree reference inventory must surface all holder kinds")
        if {
            str(item) for item in list(inventory.get("active_reference_refs") or [])
        } != {str(workspace_live_ref), str(anchor_live_ref), str(staging_ref)}:
            return _fail("runtime-heavy-tree reference inventory must preserve all heavy-tree refs")
        if inventory.get("gaps"):
            return _fail(f"complete runtime-heavy-tree reference inventory must not report gaps, got {inventory['gaps']!r}")

        active_references = [dict(item or {}) for item in list(inventory.get("active_references") or [])]
        if {
            str(item.get("reference_holder_ref") or "") for item in active_references
        } != {str(workspace_live_ref.parent), str(anchor_live_ref.parent), str(staging_ref.parent.parent)}:
            return _fail("runtime-heavy-tree reference inventory must preserve holder roots")

        trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(workspace_envelope.envelope_id or ""))
        referenced_object = dict(trace.get("referenced_object") or {})
        if str(referenced_object.get("reference_holder_kind") or "") != "runtime_workspace_live_artifact_root":
            return _fail("runtime-heavy-tree reference trace must preserve the workspace live holder kind")
        if str(referenced_object.get("reference_holder_ref") or "") != str(workspace_live_ref.parent):
            return _fail("runtime-heavy-tree reference trace must preserve the workspace live holder ref")

        anchor_trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(anchor_envelope.envelope_id or ""))
        anchor_referenced_object = dict(anchor_trace.get("referenced_object") or {})
        if str(anchor_referenced_object.get("reference_holder_kind") or "") != "runtime_live_artifact_root":
            return _fail("runtime-heavy-tree reference trace must preserve the repo-anchored live holder kind")

        staging_trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(staging_envelope.envelope_id or ""))
        staging_referenced_object = dict(staging_trace.get("referenced_object") or {})
        if str(staging_referenced_object.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("runtime-heavy-tree reference trace must preserve the staging holder kind")

    print(
        "[loop-system-runtime-heavy-tree-reference-inventory-view][OK] "
        "runtime-heavy-tree reference inventory and trace preserve workspace-live, anchor-live, and staging holder truth"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
