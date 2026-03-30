#!/usr/bin/env python3
"""Validate read-only publish-tree authority-gap inventory visibility for Milestone 5."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-publish-tree-authority-gap-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-publish-tree-authority-gap-inventory-view",
        root_goal="validate read-only publish-tree authority-gap inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise publish-tree authority-gap inventory validation",
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


def _tree_byte_size(root: Path) -> int:
    total = 0
    for path in sorted(root.rglob("*")):
        if path.is_file():
            total += int(path.stat().st_size)
    return total


def _tree_object_id(root: Path) -> str:
    from loop_product.artifact_hygiene import artifact_fingerprint

    fingerprint = artifact_fingerprint(root, ignore_runtime_heavy=True)
    return f"sha256:{str(fingerprint.get('fingerprint') or '').strip()}"


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view
        from loop_product.kernel.submit import submit_heavy_object_registration_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_publish_tree_authority_gap_inventory_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        managed_ref = _write_publish_tree(
            repo_root / "workspace" / "publish-tree-managed" / "deliverables" / "primary_artifact",
            label="managed",
        )
        gap_a_ref = _write_publish_tree(
            repo_root / "workspace" / "publish-tree-gap-a" / "deliverables" / "primary_artifact",
            label="gap-a",
        )
        gap_b_ref = _write_publish_tree(
            repo_root
            / "workspace"
            / "publish-tree-gap-a"
            / "deliverables"
            / ".primary_artifact.publish.gapdup"
            / "primary_artifact",
            label="gap-a",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=_tree_object_id(managed_ref),
            object_kind="publish_tree",
            object_ref=managed_ref,
            byte_size=_tree_byte_size(managed_ref),
            reason="register one managed publish tree",
            runtime_name="publish-runtime",
            repo_root=repo_root,
            registration_kind="publish_tree_registration",
        )

        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="publish_tree",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("publish-tree authority-gap inventory must stay read-only")
        if int(inventory.get("filesystem_candidate_count") or 0) != 3:
            return _fail("publish-tree authority-gap inventory must count matching directory candidates")
        if int(inventory.get("registered_candidate_count") or 0) != 1:
            return _fail("publish-tree authority-gap inventory must preserve registered directory coverage")
        if int(inventory.get("managed_candidate_count") or 0) != 1:
            return _fail("publish-tree authority-gap inventory must preserve managed directory coverage")
        if int(inventory.get("unmanaged_candidate_count") or 0) != 2:
            return _fail("publish-tree authority-gap inventory must expose unmanaged publish-tree directories")

        managed_candidates = list(inventory.get("managed_candidates") or [])
        unmanaged_candidates = list(inventory.get("unmanaged_candidates") or [])
        if len(managed_candidates) != 1 or len(unmanaged_candidates) != 2:
            return _fail("publish-tree authority-gap inventory must split managed and unmanaged candidates")

        managed_summary = dict(managed_candidates[0] or {})
        if str(managed_summary.get("object_ref") or "") != str(managed_ref):
            return _fail("managed publish-tree candidate must preserve the normalized directory ref")
        if int(managed_summary.get("byte_size") or 0) != _tree_byte_size(managed_ref):
            return _fail("managed publish-tree candidate must expose truthful directory byte_size")
        if not bool(managed_summary.get("registered_present")):
            return _fail("managed publish-tree candidate must report committed registration coverage")

        unmanaged_refs = {str(item.get("object_ref") or "") for item in unmanaged_candidates}
        if unmanaged_refs != {str(gap_a_ref), str(gap_b_ref)}:
            return _fail("publish-tree authority-gap inventory must expose both unmanaged directory refs")
        for summary in unmanaged_candidates:
            blob = dict(summary or {})
            if str(blob.get("coverage_state") or "") != "UNMANAGED":
                return _fail("unmanaged publish-tree candidates must report coverage_state=UNMANAGED")

        gaps = [str(item) for item in list(inventory.get("gaps") or [])]
        if "unmanaged_heavy_object_candidates_present" not in gaps:
            return _fail("publish-tree authority-gap inventory must surface unmanaged_heavy_object_candidates_present")

    print(
        "[loop-system-publish-tree-authority-gap-inventory-view][OK] "
        "publish-tree authority-gap inventory stays read-only and exposes managed vs unmanaged directory candidates"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
