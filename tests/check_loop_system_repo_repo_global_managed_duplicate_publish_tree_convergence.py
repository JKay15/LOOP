#!/usr/bin/env python3
"""Validate repo-global managed duplicate publish-tree convergence onto canonical store refs."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-global-managed-duplicate-publish-tree-convergence][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.artifact_hygiene import classify_publish_tree_reference_holder
    from managed_duplicate_heavy_object_convergence_support import (
        assert_managed_duplicate_convergence,
        persist_anchor_state,
        write_publish_tree,
    )
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_managed_duplicate_publish_tree_convergence_") as repo_root:
        anchor_root = repo_root / ".loop"
        persist_anchor_state(
            anchor_root,
            task_id="milestone5-managed-duplicate-publish-tree-convergence",
            root_goal="validate repo-global managed duplicate publish-tree convergence",
        )

        holder_ref = write_publish_tree(
            repo_root / "workspace" / "managed-duplicate-publish-tree" / "deliverables" / "primary_artifact",
            label="managed-duplicate-publish-tree-shared",
        )
        holder = classify_publish_tree_reference_holder(holder_ref, repo_root=repo_root)
        if str(holder.get("reference_holder_kind") or "") != "workspace_artifact_root":
            return _fail("managed duplicate publish-tree fixture must classify the holder as workspace_artifact_root")

        duplicate_store_ref = write_publish_tree(
            repo_root / "workspace" / "managed-duplicate-publish-tree" / "stores" / "duplicate-store-tree",
            label="managed-duplicate-publish-tree-shared",
        )

        try:
            assert_managed_duplicate_convergence(
                repo_root=repo_root,
                anchor_root=anchor_root,
                object_kind="publish_tree",
                holder_ref=holder_ref,
                holder_kind=str(holder.get("reference_holder_kind") or ""),
                holder_ref_root=str(holder.get("reference_holder_ref") or ""),
                duplicate_store_ref=duplicate_store_ref,
                runtime_name="managed-duplicate-publish-runtime",
                label="managed duplicate publish-tree convergence",
            )
        except AssertionError as exc:
            return _fail(str(exc))

    print(
        "[loop-system-repo-global-managed-duplicate-publish-tree-convergence][OK] "
        "repo-global control-plane hands managed duplicate publish-tree holders back to the canonical store ref "
        "and reclaims the duplicate store ref"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
