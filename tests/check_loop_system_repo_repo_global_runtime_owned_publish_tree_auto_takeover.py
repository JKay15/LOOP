#!/usr/bin/env python3
"""Validate repo-global auto-takeover for runtime-owned publish-tree roots."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(
        f"[loop-system-repo-global-runtime-owned-publish-tree-auto-takeover][FAIL] {msg}",
        file=sys.stderr,
    )
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _write_publish_tree(root: Path, *, label: str) -> Path:
    (root / "formalization").mkdir(parents=True, exist_ok=True)
    (root / "README.md").write_text(f"# {label}\n", encoding="utf-8")
    (root / "formalization" / "STATUS.md").write_text(f"{label}: ready\n", encoding="utf-8")
    return root.resolve()


def _tree_object_id(root: Path) -> str:
    from loop_product.artifact_hygiene import artifact_fingerprint

    fingerprint = artifact_fingerprint(root, ignore_runtime_heavy=True)
    return f"sha256:{str(fingerprint.get('fingerprint') or '').strip()}"


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
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_runtime_owned_publish_tree_auto_takeover_") as repo_root:
        anchor_root = repo_root / ".loop"
        workspace_live_ref = _write_publish_tree(
            repo_root / "workspace" / "runtime-owned-auto" / ".tmp_primary_artifact",
            label="runtime-owned-auto",
        )
        anchor_live_ref = _write_publish_tree(
            repo_root
            / ".loop"
            / "runtime-owned-auto"
            / "artifacts"
            / "live_artifacts"
            / "child-runtime-auto-001"
            / "primary_artifact",
            label="runtime-owned-auto",
        )
        staging_ref = _write_publish_tree(
            repo_root
            / "workspace"
            / "runtime-owned-auto"
            / "deliverables"
            / ".primary_artifact.publish.autotake"
            / "primary_artifact",
            label="runtime-owned-auto",
        )
        object_id = _tree_object_id(workspace_live_ref)

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global runtime-owned publish-tree auto-takeover requires a live repo control-plane service")

        def _first_takeover_ready() -> bool:
            registrations = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
            ]
            references = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
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
                "repo-global runtime-owned publish-tree remediation must converge to canonical registration "
                "and canonical references for workspace-live, anchor-live, and staging holder roots"
            )

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
        ]
        reference_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
        ]
        if len(registration_events) != 1:
            return _fail("initial unchanged runtime-owned publish-tree snapshot must converge through exactly one auto-generated registration")
        if len(reference_events) != 3:
            return _fail("initial unchanged runtime-owned publish-tree snapshot must converge through exactly three canonical references")

        reference_payloads = [dict(event.get("payload") or {}) for event in reference_events]
        if {
            str(payload.get("reference_ref") or "")
            for payload in reference_payloads
        } != {str(workspace_live_ref), str(anchor_live_ref), str(staging_ref)}:
            return _fail("runtime-owned publish-tree auto-reference events must preserve all discovered directory refs")
        if {
            str(payload.get("reference_holder_kind") or "")
            for payload in reference_payloads
        } != {
            "runtime_workspace_live_artifact_root",
            "runtime_live_artifact_root",
            "workspace_publication_staging_root",
        }:
            return _fail("runtime-owned publish-tree auto-reference events must preserve all holder kinds")

        inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=object_id,
        )
        if int(inventory.get("active_reference_count") or 0) != 3:
            return _fail("runtime-owned publish-tree reference inventory must report three active refs after auto-takeover")
        if {
            str(item)
            for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {
            "runtime_workspace_live_artifact_root",
            "runtime_live_artifact_root",
            "workspace_publication_staging_root",
        }:
            return _fail("runtime-owned publish-tree reference inventory must surface all auto-classified holder kinds")

        gap_inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_root,
            object_kind="publish_tree",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("post-takeover runtime-owned publish-tree gap inventory must report zero unmanaged candidates")

        trace = query_heavy_object_reference_trace_view(
            anchor_root,
            envelope_id=str(reference_payloads[0].get("envelope_id") or ""),
        )
        referenced_object = dict(trace.get("referenced_object") or {})
        if str(referenced_object.get("reference_kind") or "") != "authority_gap_auto_reference":
            return _fail("runtime-owned publish-tree auto-takeover trace must preserve authority_gap_auto_reference kind")
        if str(referenced_object.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("runtime-owned publish-tree auto-takeover trace must preserve the remediation trigger kind")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(
            [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
            ]
        ) != 3:
            return _fail("unchanged runtime-owned publish-tree snapshot must not emit duplicate auto-reference facts")

        third_ref = _write_publish_tree(
            repo_root / "workspace" / "runtime-owned-auto-next" / ".tmp_primary_artifact",
            label="runtime-owned-auto-next",
        )
        third_object_id = _tree_object_id(third_ref)

        def _second_takeover_ready() -> bool:
            registrations = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
            ]
            references = [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
            ]
            return len(registrations) >= 2 and len(references) >= 4

        if not _wait_until(_second_takeover_ready, timeout_s=8.0):
            return _fail("changed runtime-owned publish-tree gap snapshot must converge through a second auto-generated registration and reference")

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
        ]
        if str(dict(registration_events[-1].get("payload") or {}).get("object_id") or "") != third_object_id:
            return _fail("changed runtime-owned publish-tree gap snapshot must converge through registration of the new object id")

    print(
        "[loop-system-repo-global-runtime-owned-publish-tree-auto-takeover][OK] "
        "repo-global runtime-owned publish-tree remediation deterministically auto-registers and auto-references live roots"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
