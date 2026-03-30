#!/usr/bin/env python3
"""Validate canonical reference inventory for runtime-owned publish-tree holder roots."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-owned-publish-tree-reference-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-runtime-owned-publish-tree-reference-inventory-view",
        root_goal="validate runtime-owned publish-tree reference inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise runtime-owned publish-tree reference inventory validation",
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

    with temporary_repo_root(prefix="loop_system_runtime_owned_publish_tree_reference_inventory_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        workspace_live_ref = _write_publish_tree(
            repo_root / "workspace" / "runtime-owned-reference" / ".tmp_primary_artifact",
            label="runtime-owned-reference",
        )
        anchor_live_ref = _write_publish_tree(
            repo_root
            / ".loop"
            / "runtime-owned-reference"
            / "artifacts"
            / "live_artifacts"
            / "child-runtime-reference-001"
            / "primary_artifact",
            label="runtime-owned-reference",
        )
        object_id, byte_size = _tree_identity(workspace_live_ref)

        workspace_holder = classify_publish_tree_reference_holder(workspace_live_ref, repo_root=repo_root)
        if str(workspace_holder.get("reference_holder_kind") or "") != "runtime_workspace_live_artifact_root":
            return _fail("workspace live publish-tree holder must classify as runtime_workspace_live_artifact_root")
        if str(workspace_holder.get("reference_holder_ref") or "") != str(workspace_live_ref):
            return _fail("workspace live publish-tree holder must preserve the .tmp_primary_artifact root")

        anchor_holder = classify_publish_tree_reference_holder(anchor_live_ref, repo_root=repo_root)
        if str(anchor_holder.get("reference_holder_kind") or "") != "runtime_live_artifact_root":
            return _fail("repo-anchored live publish-tree holder must classify as runtime_live_artifact_root")
        if str(anchor_holder.get("reference_holder_ref") or "") != str(anchor_live_ref):
            return _fail("repo-anchored live publish-tree holder must preserve the live_artifacts root")

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="publish_tree",
            object_ref=workspace_live_ref,
            byte_size=byte_size,
            reason="register runtime-owned publish tree before attaching runtime-live references",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            registration_kind="runtime_owned_publish_tree_registration",
        )

        workspace_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="publish_tree",
            object_ref=workspace_live_ref,
            reference_ref=str(workspace_holder.get("reference_ref") or ""),
            reference_holder_kind=str(workspace_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(workspace_holder.get("reference_holder_ref") or ""),
            reference_kind="runtime_owned_publish_tree_reference",
            reason="attach canonical runtime-owned workspace live publish-tree reference",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            trigger_kind="runtime_owned_publish_tree_reference_test",
            trigger_ref="workspace-live",
        )
        anchor_envelope = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="publish_tree",
            object_ref=workspace_live_ref,
            reference_ref=str(anchor_holder.get("reference_ref") or ""),
            reference_holder_kind=str(anchor_holder.get("reference_holder_kind") or ""),
            reference_holder_ref=str(anchor_holder.get("reference_holder_ref") or ""),
            reference_kind="runtime_owned_publish_tree_reference",
            reason="attach canonical repo-anchored runtime live publish-tree reference",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            trigger_kind="runtime_owned_publish_tree_reference_test",
            trigger_ref="anchor-live",
        )

        inventory = query_heavy_object_reference_inventory_view(
            state_root,
            object_id=object_id,
            object_ref=workspace_live_ref,
        )
        if not bool(inventory.get("read_only")):
            return _fail("runtime-owned publish-tree reference inventory must stay read-only")
        if int(inventory.get("active_reference_count") or 0) != 2:
            return _fail("runtime-owned publish-tree reference inventory must report two active references")
        if {
            str(item) for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {"runtime_workspace_live_artifact_root", "runtime_live_artifact_root"}:
            return _fail("runtime-owned publish-tree reference inventory must surface both runtime-live holder kinds")
        if {
            str(item) for item in list(inventory.get("active_reference_refs") or [])
        } != {str(workspace_live_ref), str(anchor_live_ref)}:
            return _fail("runtime-owned publish-tree reference inventory must preserve both runtime-owned refs")
        if inventory.get("gaps"):
            return _fail(f"complete runtime-owned publish-tree reference inventory must not report gaps, got {inventory['gaps']!r}")

        active_references = [dict(item or {}) for item in list(inventory.get("active_references") or [])]
        if {
            str(item.get("reference_holder_ref") or "") for item in active_references
        } != {str(workspace_live_ref), str(anchor_live_ref)}:
            return _fail("runtime-owned publish-tree reference inventory must preserve both holder roots")

        trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(workspace_envelope.envelope_id or ""))
        referenced_object = dict(trace.get("referenced_object") or {})
        if str(referenced_object.get("reference_holder_kind") or "") != "runtime_workspace_live_artifact_root":
            return _fail("runtime-owned publish-tree reference trace must preserve the workspace live holder kind")
        if str(referenced_object.get("reference_holder_ref") or "") != str(workspace_live_ref):
            return _fail("runtime-owned publish-tree reference trace must preserve the workspace live holder ref")

        anchor_trace = query_heavy_object_reference_trace_view(state_root, envelope_id=str(anchor_envelope.envelope_id or ""))
        anchor_referenced_object = dict(anchor_trace.get("referenced_object") or {})
        if str(anchor_referenced_object.get("reference_holder_kind") or "") != "runtime_live_artifact_root":
            return _fail("runtime-owned publish-tree reference trace must preserve the repo-anchored live holder kind")

    print(
        "[loop-system-runtime-owned-publish-tree-reference-inventory-view][OK] "
        "runtime-owned publish-tree reference inventory and trace preserve workspace-live plus anchor-live holder truth"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
