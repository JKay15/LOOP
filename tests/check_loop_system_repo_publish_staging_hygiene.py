#!/usr/bin/env python3
"""Guard hidden publication staging roots from ordinary workspace-runtime hygiene."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-publish-staging-hygiene][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    from loop_product.artifact_hygiene import iter_workspace_artifact_roots
    from loop_product.dispatch.publication import (
        inspect_workspace_local_runtime_state,
        reset_workspace_local_runtime_heavy_roots,
    )
    from loop_product.kernel.state import ensure_runtime_tree
    from loop_product.runtime_paths import node_live_artifact_root
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_product_publish_staging_hygiene_") as temp_root:
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace" / "child-publish-staging-001"
        publish_root = workspace_root / "deliverables" / "primary_artifact"
        staging_root = workspace_root / "deliverables" / ".primary_artifact.publish.tmpstage"
        live_root = node_live_artifact_root(
            state_root=state_root,
            node_id="child-publish-staging-001",
            workspace_mirror_relpath="deliverables/primary_artifact",
        )
        receipt_ref = (
            state_root / "artifacts" / "publication" / "child-publish-staging-001" / "WorkspaceArtifactPublicationReceipt.json"
        )

        ensure_runtime_tree(state_root)
        workspace_root.mkdir(parents=True, exist_ok=True)
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# external live root\n", encoding="utf-8")
        (workspace_root / "FROZEN_HANDOFF.json").write_text(
            json.dumps(
                {
                    "node_id": "child-publish-staging-001",
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
        (staging_root / "primary_artifact" / ".lake" / "build" / "lib").mkdir(parents=True, exist_ok=True)
        (
            staging_root / "primary_artifact" / ".lake" / "build" / "lib" / "artifact.txt"
        ).write_text("staged-build\n", encoding="utf-8")

        artifact_roots = [str(item) for item in iter_workspace_artifact_roots(workspace_root)]
        if any(".primary_artifact.publish." in item for item in artifact_roots):
            return _fail("hidden publication staging roots must not be treated as ordinary workspace artifact roots")

        report = inspect_workspace_local_runtime_state(workspace_root=workspace_root)
        if bool(report.get("violation")):
            return _fail("hidden publication staging roots must not trigger workspace-local runtime-heavy-tree violations")
        if any(".primary_artifact.publish." in str(item) for item in report.get("workspace_runtime_heavy_root_refs") or []):
            return _fail("workspace local runtime report must not cite hidden publication staging roots")

        reset_payload = reset_workspace_local_runtime_heavy_roots(workspace_root=workspace_root)
        removed_refs = [str(item) for item in reset_payload.get("removed_workspace_publication_staging_root_refs") or []]
        if str(staging_root.resolve()) not in removed_refs:
            return _fail("workspace-runtime reset must reap stale hidden publication staging roots")
        if staging_root.exists():
            return _fail("hidden publication staging root must be removed by explicit reset")

    print("[loop-system-publish-staging-hygiene] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
