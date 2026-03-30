#!/usr/bin/env python3
"""Validate repo-global auto-takeover for runtime heavy-tree roots."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(
        f"[loop-system-repo-global-runtime-heavy-tree-auto-takeover][FAIL] {msg}",
        file=sys.stderr,
    )
    return 2


def _expected_canonical_store_ref(repo_root: Path, *, object_kind: str, object_id: str, is_dir: bool, suffix: str = "") -> Path:
    digest = str(object_id or "").removeprefix("sha256:").strip()
    leaf = "tree" if is_dir else f"object{suffix}"
    return (repo_root / ".loop" / "heavy_objects" / object_kind / digest / leaf).resolve()


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _write_runtime_heavy_tree(artifact_root: Path, *, label: str) -> Path:
    tree_root = artifact_root / ".lake"
    (tree_root / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
    (tree_root / "packages" / "mathlib" / "STATUS.txt").write_text(f"{label}: ready\n", encoding="utf-8")
    return tree_root.resolve()


def _tree_object_id(root: Path) -> str:
    from loop_product.artifact_hygiene import heavy_object_identity

    return str(heavy_object_identity(root, object_kind="runtime_heavy_tree").get("object_id") or "")


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    from loop_product.kernel.query import (
        query_heavy_object_authority_gap_inventory_view,
        query_heavy_object_reference_inventory_view,
        query_heavy_object_reference_trace_view,
        query_heavy_object_sharing_view,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_runtime_heavy_tree_auto_takeover_") as repo_root:
        anchor_root = repo_root / ".loop"
        workspace_live_ref = _write_runtime_heavy_tree(
            repo_root / "workspace" / "runtime-heavy-auto" / ".tmp_primary_artifact",
            label="runtime-heavy-auto",
        )
        anchor_live_ref = _write_runtime_heavy_tree(
            repo_root
            / ".loop"
            / "runtime-heavy-auto"
            / "artifacts"
            / "live_artifacts"
            / "child-runtime-heavy-auto-001"
            / "primary_artifact",
            label="runtime-heavy-auto",
        )
        staging_ref = _write_runtime_heavy_tree(
            repo_root
            / "workspace"
            / "runtime-heavy-auto"
            / "deliverables"
            / ".primary_artifact.publish.autotake"
            / "primary_artifact",
            label="runtime-heavy-auto",
        )
        object_id = _tree_object_id(workspace_live_ref)

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global runtime-heavy-tree auto-takeover requires a live repo control-plane service")

        def _first_takeover_ready() -> bool:
            registrations = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
            ]
            references = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
            ]
            inventory = query_heavy_object_reference_inventory_view(
                anchor_root,
                object_id=object_id,
            )
            return (
                len(registrations) >= 1
                and len(references) >= 3
                and int(inventory.get("active_reference_count") or 0) >= 3
            )

        if not _wait_until(_first_takeover_ready, timeout_s=8.0):
            return _fail(
                "repo-global runtime-heavy-tree remediation must converge to canonical registration "
                "and canonical references for workspace-live, anchor-live, and staging holder roots"
            )

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
        ]
        reference_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
        ]
        if len(registration_events) != 1:
            return _fail("initial unchanged runtime-heavy-tree snapshot must converge through exactly one auto-generated registration")
        if str(dict(registration_events[0].get("payload") or {}).get("object_ref") or "") != str(
            _expected_canonical_store_ref(
                repo_root,
                object_kind="runtime_heavy_tree",
                object_id=object_id,
                is_dir=True,
            )
        ):
            return _fail("initial unchanged runtime-heavy-tree snapshot must register the canonical content-addressed tree ref")
        if len(reference_events) != 3:
            return _fail("initial unchanged runtime-heavy-tree snapshot must converge through exactly three canonical references")

        reference_payloads = [dict(event.get("payload") or {}) for event in reference_events]
        if {
            str(payload.get("reference_ref") or "")
            for payload in reference_payloads
        } != {str(workspace_live_ref), str(anchor_live_ref), str(staging_ref)}:
            return _fail("runtime-heavy-tree auto-reference events must preserve all discovered heavy-tree refs")
        if {
            str(payload.get("reference_holder_kind") or "")
            for payload in reference_payloads
        } != {
            "runtime_workspace_live_artifact_root",
            "runtime_live_artifact_root",
            "workspace_publication_staging_root",
        }:
            return _fail("runtime-heavy-tree auto-reference events must preserve all holder kinds")

        inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=object_id,
        )
        if int(inventory.get("active_reference_count") or 0) != 3:
            return _fail("runtime-heavy-tree reference inventory must report three active refs after auto-takeover")
        if {
            str(item)
            for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {
            "runtime_workspace_live_artifact_root",
            "runtime_live_artifact_root",
            "workspace_publication_staging_root",
        }:
            return _fail("runtime-heavy-tree reference inventory must surface all auto-classified holder kinds")

        sharing_view = query_heavy_object_sharing_view(
            anchor_root,
            object_id=object_id,
        )
        if bool(sharing_view.get("shareable")):
            return _fail("fully auto-referenced runtime-heavy-tree refs must no longer report shareable=true")
        if not bool(sharing_view.get("converged")):
            return _fail("fully auto-referenced runtime-heavy-tree refs must report converged=true")
        if str(sharing_view.get("sharing_state") or "") != "CONVERGED":
            return _fail("fully auto-referenced runtime-heavy-tree refs must report sharing_state=CONVERGED")
        if int(sharing_view.get("covered_observed_ref_count") or 0) != 3:
            return _fail("runtime-heavy-tree sharing view must report all observed refs as covered")
        if int(sharing_view.get("missing_observed_ref_count") or 0) != 0:
            return _fail("runtime-heavy-tree sharing view must report zero missing observed refs after convergence")

        gap_inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_root,
            object_kind="runtime_heavy_tree",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("post-takeover runtime-heavy-tree gap inventory must report zero unmanaged candidates")

        trace = query_heavy_object_reference_trace_view(
            anchor_root,
            envelope_id=str(reference_payloads[0].get("envelope_id") or ""),
        )
        referenced_object = dict(trace.get("referenced_object") or {})
        if str(referenced_object.get("reference_kind") or "") != "authority_gap_auto_reference":
            return _fail("runtime-heavy-tree auto-takeover trace must preserve authority_gap_auto_reference kind")
        if str(referenced_object.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("runtime-heavy-tree auto-takeover trace must preserve the remediation trigger kind")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(
            [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
            ]
        ) != 3:
            return _fail("unchanged runtime-heavy-tree snapshot must not emit duplicate auto-reference facts")

        third_ref = _write_runtime_heavy_tree(
            repo_root / "workspace" / "runtime-heavy-auto-next" / ".tmp_primary_artifact",
            label="runtime-heavy-auto-next",
        )
        (third_ref / "packages" / "mathlib" / "NEXT.txt").write_text("next snapshot\n", encoding="utf-8")
        third_object_id = _tree_object_id(third_ref)

        def _second_takeover_ready() -> bool:
            registrations = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
            ]
            references = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
            ]
            return len(registrations) >= 2 and len(references) >= 4

        if not _wait_until(_second_takeover_ready, timeout_s=8.0):
            return _fail("changed runtime-heavy-tree snapshot must converge through a second auto-generated registration and reference")

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
        ]
        if str(dict(registration_events[-1].get("payload") or {}).get("object_id") or "") != third_object_id:
            return _fail("changed runtime-heavy-tree snapshot must converge through registration of the new object id")
        if str(dict(registration_events[-1].get("payload") or {}).get("object_ref") or "") != str(
            _expected_canonical_store_ref(
                repo_root,
                object_kind="runtime_heavy_tree",
                object_id=third_object_id,
                is_dir=True,
            )
        ):
            return _fail("changed runtime-heavy-tree snapshot must converge through registration of the new canonical tree ref")

    print(
        "[loop-system-repo-global-runtime-heavy-tree-auto-takeover][OK] "
        "repo-global runtime-heavy-tree remediation deterministically auto-registers and auto-references live roots"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
