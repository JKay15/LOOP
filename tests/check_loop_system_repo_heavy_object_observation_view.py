#!/usr/bin/env python3
"""Validate read-only heavy-object observation visibility for Milestone 5."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-observation-view][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_heavy_object_observation_event,
            committed_event_count,
        )
        from loop_product.kernel.query import query_heavy_object_observation_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_observation_view_complete_") as repo_root:
        state_root = repo_root / ".loop"
        matching_ref_a = str(
            (
                repo_root
                / "artifacts"
                / "evaluator_runs"
                / "lane-a"
                / ".lake"
                / "packages"
                / "mathlib"
                / ".git"
                / "objects"
                / "pack"
                / "pack-a.pack"
            ).resolve()
        )
        matching_ref_b = str(
            (
                repo_root
                / "workspace"
                / "publish"
                / ".primary_artifact.publish.heavydup"
                / "primary_artifact"
                / ".lake"
                / "packages"
                / "mathlib"
                / ".git"
                / "objects"
                / "pack"
                / "pack-a.pack"
            ).resolve()
        )
        matching_ref_c = str(
            (
                repo_root
                / "workspace"
                / "publish"
                / ".primary_artifact.publish.heavydup"
                / "secondary_artifact"
                / ".lake"
                / "packages"
                / "mathlib"
                / ".git"
                / "objects"
                / "pack"
                / "pack-a.pack"
            ).resolve()
        )

        commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-other-001",
            node_id="root-kernel",
            payload={
                "runtime_name": "other-runtime",
                "object_kind": "publish_tree",
                "duplicate_count": 1,
                "object_refs": [str((repo_root / "other" / "tree").resolve())],
                "total_bytes": 123,
            },
            producer="tests.heavy_object",
        )
        commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-match-001",
            node_id="root-kernel",
            payload={
                "runtime_name": "event-hygiene-001",
                "object_kind": "mathlib_pack",
                "duplicate_count": 2,
                "object_refs": [matching_ref_a, matching_ref_b],
                "total_bytes": 912680550,
            },
            producer="tests.heavy_object",
        )
        commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-match-002",
            node_id="root-kernel",
            payload={
                "runtime_name": "event-hygiene-001",
                "object_kind": "mathlib_pack",
                "duplicate_count": 3,
                "object_refs": [matching_ref_a, matching_ref_b, matching_ref_c],
                "total_bytes": 1012680550,
            },
            producer="tests.heavy_object",
        )

        event_count_before = committed_event_count(state_root)
        view = query_heavy_object_observation_view(
            state_root,
            runtime_name="event-hygiene-001",
            object_kind="mathlib_pack",
            object_ref=matching_ref_b,
        )
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("heavy-object observation view must stay read-only and not mutate committed event history")
        if not bool(view.get("read_only")):
            return _fail("heavy-object observation view must stay read-only")
        if int(view.get("heavy_object_observation_event_count") or 0) != 3:
            return _fail("heavy-object observation view must expose the total committed heavy-object observation count")
        if int(view.get("matching_observation_count") or 0) != 2:
            return _fail("heavy-object observation view must count matching committed observations")
        latest = dict(view.get("latest_matching_observation") or {})
        payload = dict(latest.get("payload") or {})
        if payload.get("duplicate_count") != 3:
            return _fail("heavy-object observation view must expose the latest matching duplicate_count")
        observed_refs = [str(item) for item in list(view.get("observed_object_refs") or [])]
        if observed_refs != [matching_ref_a, matching_ref_b, matching_ref_c]:
            return _fail("heavy-object observation view must expose sorted unique observed object refs from matching observations")
        if int(view.get("observed_object_ref_count") or 0) != 3:
            return _fail("heavy-object observation view must expose the number of unique matching object refs")
        if int(view.get("max_duplicate_count") or 0) != 3:
            return _fail("heavy-object observation view must expose the strongest duplicate_count across matching observations")
        if int(view.get("latest_total_bytes") or 0) != 1012680550:
            return _fail("heavy-object observation view must expose latest matching total_bytes")
        if list(view.get("gaps") or []):
            return _fail(f"matching heavy-object observation view must not report gaps, got {view.get('gaps')!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_observation_view_gap_") as repo_root:
        state_root = repo_root / ".loop"
        commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-gap-other-001",
            node_id="root-kernel",
            payload={
                "runtime_name": "different-runtime",
                "object_kind": "mathlib_pack",
                "duplicate_count": 2,
                "object_refs": [str((repo_root / "artifacts" / "other.pack").resolve())],
                "total_bytes": 2048,
            },
            producer="tests.heavy_object",
        )
        gap_view = query_heavy_object_observation_view(
            state_root,
            runtime_name="event-hygiene-001",
            object_kind="mathlib_pack",
            object_ref=str((repo_root / "artifacts" / "missing.pack").resolve()),
        )
        if int(gap_view.get("matching_observation_count") or 0) != 0:
            return _fail("gap heavy-object observation view must not invent matching observations")
        if bool(gap_view.get("latest_matching_observation")):
            return _fail("gap heavy-object observation view must not invent a latest matching observation")
        gaps = [str(item) for item in list(gap_view.get("gaps") or [])]
        if "missing_matching_heavy_object_observation" not in gaps:
            return _fail("gap heavy-object observation view must surface missing_matching_heavy_object_observation")

    print("[loop-system-heavy-object-observation-view][OK] heavy-object observation view stays read-only and exposes committed duplicate-heavy-tree observations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
