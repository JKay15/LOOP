#!/usr/bin/env python3
"""Validate read-only authority-gap inventory for runtime heavy-tree candidates."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-heavy-tree-authority-gap-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-runtime-heavy-tree-authority-gap-inventory-view",
        root_goal="validate runtime-heavy-tree authority-gap inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise runtime-heavy-tree authority-gap inventory validation",
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


def _write_runtime_heavy_tree(artifact_root: Path, *, tree_name: str, label: str) -> Path:
    tree_root = artifact_root / tree_name
    (tree_root / "content").mkdir(parents=True, exist_ok=True)
    (tree_root / "README.txt").write_text(f"{label}\n", encoding="utf-8")
    (tree_root / "content" / "STATE.txt").write_text(f"{label}: ready\n", encoding="utf-8")
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
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view
        from loop_product.kernel.submit import submit_heavy_object_registration_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_runtime_heavy_tree_authority_gap_inventory_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        managed_ref = _write_runtime_heavy_tree(
            repo_root / "workspace" / "managed-runtime-heavy" / "deliverables" / "primary_artifact",
            tree_name=".lake",
            label="managed-runtime-heavy",
        )
        workspace_live_ref = _write_runtime_heavy_tree(
            repo_root / "workspace" / "runtime-heavy" / ".tmp_primary_artifact",
            tree_name="build",
            label="workspace-live-runtime-heavy",
        )
        anchor_live_ref = _write_runtime_heavy_tree(
            repo_root
            / ".loop"
            / "runtime-heavy"
            / "artifacts"
            / "live_artifacts"
            / "child-runtime-heavy-001"
            / "primary_artifact",
            tree_name="_lake_build",
            label="anchor-live-runtime-heavy",
        )

        object_id, byte_size = _tree_identity(managed_ref)
        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="runtime_heavy_tree",
            object_ref=managed_ref,
            byte_size=byte_size,
            reason="register one managed runtime heavy tree before checking authority gaps",
            runtime_name="heavy-runtime",
            repo_root=repo_root,
            registration_kind="runtime_heavy_tree_registration",
        )

        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            runtime_name="heavy-runtime",
            object_kind="runtime_heavy_tree",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("runtime-heavy-tree authority-gap inventory must stay read-only")
        if int(inventory.get("filesystem_candidate_count") or 0) != 3:
            return _fail("runtime-heavy-tree inventory must count .lake, build, and _lake_build candidates")
        if int(inventory.get("registered_candidate_count") or 0) != 1:
            return _fail("runtime-heavy-tree inventory must preserve exactly one registered candidate")
        if int(inventory.get("managed_candidate_count") or 0) != 1:
            return _fail("runtime-heavy-tree inventory must keep only the registered candidate managed before remediation")
        if int(inventory.get("unmanaged_candidate_count") or 0) != 2:
            return _fail("runtime-heavy-tree inventory must expose both live heavy trees as unmanaged candidates")

        managed_candidates = [dict(item or {}) for item in list(inventory.get("managed_candidates") or [])]
        unmanaged_candidates = [dict(item or {}) for item in list(inventory.get("unmanaged_candidates") or [])]
        if len(managed_candidates) != 1 or len(unmanaged_candidates) != 2:
            return _fail("runtime-heavy-tree inventory must split one managed and two unmanaged candidates")

        managed_summary = managed_candidates[0]
        if str(managed_summary.get("object_ref") or "") != str(managed_ref):
            return _fail("managed runtime-heavy-tree summary must preserve the normalized ref")
        if str(managed_summary.get("file_name") or "") != ".lake":
            return _fail("managed runtime-heavy-tree summary must preserve file_name=.lake")
        if int(managed_summary.get("byte_size") or 0) != byte_size:
            return _fail("managed runtime-heavy-tree summary must expose truthful directory byte_size")

        unmanaged_refs = {str(item.get("object_ref") or "") for item in unmanaged_candidates}
        if unmanaged_refs != {str(workspace_live_ref), str(anchor_live_ref)}:
            return _fail("runtime-heavy-tree inventory must expose both unmanaged live heavy-tree refs")
        if {
            str(item.get("file_name") or "")
            for item in unmanaged_candidates
        } != {"build", "_lake_build"}:
            return _fail("runtime-heavy-tree inventory must preserve file names for both unmanaged heavy trees")
        for summary in unmanaged_candidates:
            if str(summary.get("coverage_state") or "") != "UNMANAGED":
                return _fail("runtime-heavy-tree candidates must report coverage_state=UNMANAGED before remediation")

        gaps = [str(item) for item in list(inventory.get("gaps") or [])]
        if "unmanaged_heavy_object_candidates_present" not in gaps:
            return _fail("runtime-heavy-tree inventory must surface unmanaged_heavy_object_candidates_present")

    print(
        "[loop-system-runtime-heavy-tree-authority-gap-inventory-view][OK] "
        "runtime-heavy-tree inventory stays read-only and exposes managed plus unmanaged heavy-tree roots"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
