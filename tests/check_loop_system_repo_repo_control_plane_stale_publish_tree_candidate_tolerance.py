#!/usr/bin/env python3
"""Guard repo bootstrap against stale publish-tree candidates with broken runtime-heavy leaves."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(
        f"[loop-system-repo-control-plane-stale-publish-tree-candidate-tolerance][FAIL] {msg}",
        file=sys.stderr,
    )
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.artifact_hygiene import artifact_byte_size
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view
        from loop_product.runtime import ensure_repo_control_plane_services_running
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_repo_control_plane_stale_publish_tree_candidate_") as repo_root:
        state_root = repo_root / ".loop"
        state_root.mkdir(parents=True, exist_ok=True)

        publish_tree = (
            repo_root
            / ".loop"
            / "arxiv_2602_11505v2_whole_paper_faithful_formalization_benchmark_r54"
            / "artifacts"
            / "live_artifacts"
            / "Nearly__Monotone__Biased__Predictor_plugin_stability"
            / "primary_artifact"
        )
        publish_tree.mkdir(parents=True, exist_ok=True)
        readme = publish_tree / "README.txt"
        readme_payload = b"stale publish tree should not kill repo bootstrap\n"
        readme.write_bytes(readme_payload)
        broken_runtime_leaf = publish_tree / ".lake"
        broken_runtime_leaf.symlink_to(
            repo_root
            / ".loop"
            / "arxiv_2602_11505v2_whole_paper_faithful_formalization_benchmark_r54"
            / "workspace"
            / "missing_lake_packages"
        )

        expected_size = len(readme_payload)
        measured_size = artifact_byte_size(publish_tree, ignore_runtime_heavy=True)
        if measured_size != expected_size:
            return _fail(
                "publish-tree byte-size must ignore broken runtime-heavy leaves; "
                f"expected {expected_size}, got {measured_size}"
            )

        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            object_kind="publish_tree",
            repo_root=repo_root,
        )
        unmanaged_candidates = list(inventory.get("unmanaged_candidates") or [])
        matching = [
            dict(item or {})
            for item in unmanaged_candidates
            if str(dict(item or {}).get("object_ref") or "") == str(publish_tree.resolve())
        ]
        if len(matching) != 1:
            return _fail("authority-gap inventory must preserve the stale publish-tree candidate instead of crashing")
        if int(matching[0].get("byte_size") or 0) != expected_size:
            return _fail("authority-gap inventory must report byte-size without counting the broken runtime-heavy leaf")

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        if int(dict(services.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
            return _fail("repo control-plane must still bootstrap when stale publish-tree candidates are present")
        if int(dict(services.get("repo_reactor") or {}).get("pid") or 0) <= 0:
            return _fail("repo reactor must still bootstrap when stale publish-tree candidates are present")
        if int(dict(services.get("repo_reactor_residency_guard") or {}).get("pid") or 0) <= 0:
            return _fail("repo reactor residency guard must still bootstrap when stale publish-tree candidates are present")

    print("[loop-system-repo-control-plane-stale-publish-tree-candidate-tolerance][OK] stale publish-tree candidates do not crash repo bootstrap")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
