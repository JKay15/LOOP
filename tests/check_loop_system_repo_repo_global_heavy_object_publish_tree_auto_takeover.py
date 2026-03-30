#!/usr/bin/env python3
"""Validate repo-global authority-gap auto-takeover for publish-tree directories."""

from __future__ import annotations

import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(
        f"[loop-system-repo-global-heavy-object-publish-tree-auto-takeover][FAIL] {msg}",
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
        query_heavy_object_sharing_view,
    )
    from loop_product.runtime import ensure_repo_control_plane_services_running
    from test_support import temporary_repo_root

    with temporary_repo_root(prefix="loop_system_repo_global_heavy_object_publish_tree_auto_takeover_") as repo_root:
        anchor_root = repo_root / ".loop"
        runtime_ref = _write_publish_tree(
            repo_root
            / ".loop"
            / "publish-tree-auto-run"
            / "artifacts"
            / "evaluator_runs"
            / "lane_a"
            / ".loop"
            / "ai_user"
            / "workspace"
            / "deliverables"
            / "primary_artifact",
            label="shared-publish-tree",
        )
        staging_ref = _write_publish_tree(
            repo_root
            / "workspace"
            / "publish-tree-auto-run"
            / "deliverables"
            / ".primary_artifact.publish.autotake"
            / "primary_artifact",
            label="shared-publish-tree",
        )
        object_id = _tree_object_id(runtime_ref)

        services = ensure_repo_control_plane_services_running(repo_root=repo_root)
        control_plane = dict(services.get("repo_control_plane") or {})
        if int(control_plane.get("pid") or 0) <= 0:
            return _fail("repo-global publish-tree auto-takeover requires a live repo control-plane service")

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
                and len(references) >= 2
                and int(inventory.get("active_reference_count") or 0) >= 2
            )

        if not _wait_until(_first_takeover_ready, timeout_s=8.0):
            return _fail(
                "repo-global publish-tree authority-gap remediation must converge to canonical registration "
                "and canonical references for the discovered directory holder roots"
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
            return _fail("initial unchanged publish-tree gap snapshot must converge through exactly one auto-generated registration")
        if str(dict(registration_events[0].get("payload") or {}).get("object_ref") or "") != str(
            _expected_canonical_store_ref(
                repo_root,
                object_kind="publish_tree",
                object_id=object_id,
                is_dir=True,
            )
        ):
            return _fail("initial unchanged publish-tree gap snapshot must register the canonical content-addressed tree ref")
        if len(reference_events) != 2:
            return _fail("initial unchanged publish-tree gap snapshot must converge through exactly two canonical references")

        reference_payloads = [dict(event.get("payload") or {}) for event in reference_events]
        if {
            str(payload.get("reference_ref") or "")
            for payload in reference_payloads
        } != {str(runtime_ref), str(staging_ref)}:
            return _fail("publish-tree auto-reference events must preserve both discovered directory refs")
        if {
            str(payload.get("reference_holder_kind") or "")
            for payload in reference_payloads
        } != {"evaluator_workspace_artifact_root", "workspace_publication_staging_root"}:
            return _fail("publish-tree auto-reference events must preserve both holder kinds")

        inventory = query_heavy_object_reference_inventory_view(
            anchor_root,
            object_id=object_id,
        )
        if int(inventory.get("active_reference_count") or 0) != 2:
            return _fail("publish-tree reference inventory must report two active refs after auto-takeover")

        sharing_view = query_heavy_object_sharing_view(
            anchor_root,
            object_id=object_id,
        )
        if bool(sharing_view.get("shareable")):
            return _fail("fully auto-referenced publish-tree refs must no longer report shareable=true")
        if not bool(sharing_view.get("converged")):
            return _fail("fully auto-referenced publish-tree refs must report converged=true")
        if str(sharing_view.get("sharing_state") or "") != "CONVERGED":
            return _fail("fully auto-referenced publish-tree refs must report sharing_state=CONVERGED")
        if int(sharing_view.get("covered_observed_ref_count") or 0) != 2:
            return _fail("publish-tree sharing view must report both observed refs as covered")
        if int(sharing_view.get("missing_observed_ref_count") or 0) != 0:
            return _fail("publish-tree sharing view must report zero missing observed refs after convergence")

        gap_inventory = query_heavy_object_authority_gap_inventory_view(
            anchor_root,
            object_kind="publish_tree",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("unmanaged_candidate_count") or 0) != 0:
            return _fail("post-takeover publish-tree gap inventory must report zero unmanaged candidates")

        reference_trace = query_heavy_object_reference_trace_view(
            anchor_root,
            envelope_id=str(reference_payloads[0].get("envelope_id") or ""),
        )
        referenced_object = dict(reference_trace.get("referenced_object") or {})
        if str(referenced_object.get("reference_kind") or "") != "authority_gap_auto_reference":
            return _fail("publish-tree auto-takeover trace must preserve authority_gap_auto_reference kind")
        if str(referenced_object.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("publish-tree auto-takeover trace must preserve the remediation trigger kind")

        ensure_repo_control_plane_services_running(repo_root=repo_root)
        time.sleep(0.5)
        if len(
            [
                dict(event)
                for event in _committed_events_by_type(anchor_root, "heavy_object_reference_attached")
                if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
            ]
        ) != 2:
            return _fail("unchanged publish-tree gap snapshot must not emit duplicate auto-reference facts")

        third_ref = _write_publish_tree(
            repo_root / "workspace" / "publish-tree-auto-next" / "deliverables" / "primary_artifact",
            label="third-publish-tree",
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
            return len(registrations) >= 2 and len(references) >= 3

        if not _wait_until(_second_takeover_ready, timeout_s=8.0):
            return _fail("changed unmanaged publish-tree gap snapshot must converge through a second auto-generated registration and reference")

        registration_events = [
            dict(event)
            for event in _committed_events_by_type(anchor_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "publish_tree"
        ]
        if str(dict(registration_events[-1].get("payload") or {}).get("object_id") or "") != third_object_id:
            return _fail("changed unmanaged publish-tree gap snapshot must converge through registration of the new object id")
        if str(dict(registration_events[-1].get("payload") or {}).get("object_ref") or "") != str(
            _expected_canonical_store_ref(
                repo_root,
                object_kind="publish_tree",
                object_id=third_object_id,
                is_dir=True,
            )
        ):
            return _fail("changed unmanaged publish-tree gap snapshot must converge through registration of the new canonical tree ref")

    print(
        "[loop-system-repo-global-heavy-object-publish-tree-auto-takeover][OK] "
        "repo-global publish-tree authority-gap remediation deterministically auto-registers and auto-references directory roots"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
