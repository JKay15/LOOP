#!/usr/bin/env python3
"""Validate journal-backed operational hygiene observation surfaces."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-hygiene-observations][FAIL] {msg}", file=sys.stderr)
    return 2


def _materialize_duplicate_pack_candidates(*, repo_root: Path, state_root: Path, pack_name: str) -> list[Path]:
    pack_paths = [
        state_root
        / "artifacts"
        / "hygiene_observation"
        / ".loop"
        / "ai_user"
        / "workspace"
        / "deliverables"
        / "hygiene_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
        repo_root
        / "workspace"
        / "event-journal-hygiene-observations"
        / "deliverables"
        / ".hygiene_artifact.publish.hygdup"
        / "hygiene_artifact"
        / ".lake"
        / "packages"
        / "mathlib"
        / ".git"
        / "objects"
        / "pack"
        / pack_name,
    ]
    for idx, path in enumerate(pack_paths, start=1):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((f"hygiene-pack-{idx}\n").encode("utf-8"))
    return pack_paths


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import (
            commit_heavy_object_observation_event,
            commit_operational_hygiene_observation_event,
            committed_event_count,
            iter_committed_events,
        )
        from loop_product.kernel import query_operational_hygiene_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_event_hygiene_obs_") as repo_root:
        state_root = repo_root / ".loop"
        pack_name = "pack-event-journal-hygiene-observations.pack"
        pack_paths = _materialize_duplicate_pack_candidates(
            repo_root=repo_root,
            state_root=state_root,
            pack_name=pack_name,
        )

        hygiene_payload = {
            "runtime_name": "event-hygiene-001",
            "owned_process_count": 3,
            "owned_pids": [111, 222, 333],
            "blocking_reasons": ["stale_sidecars_present"],
        }
        event = commit_operational_hygiene_observation_event(
            state_root,
            observation_id="obs-hygiene-001",
            node_id="root-kernel",
            payload=hygiene_payload,
            producer="tests.hygiene",
        )
        replay = commit_operational_hygiene_observation_event(
            state_root,
            observation_id="obs-hygiene-001",
            node_id="root-kernel",
            payload=hygiene_payload,
            producer="tests.hygiene",
        )
        if int(event.get("seq") or 0) != int(replay.get("seq") or 0):
            return _fail("replaying the same hygiene observation must stay idempotent")

        heavy_payload = {
            "runtime_name": "event-hygiene-001",
            "object_kind": "mathlib_pack",
            "duplicate_count": len(pack_paths),
            "object_refs": [str(path) for path in pack_paths],
            "total_bytes": 912680550,
        }
        heavy = commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-001",
            node_id="root-kernel",
            payload=heavy_payload,
            producer="tests.hygiene",
        )
        heavy_replay = commit_heavy_object_observation_event(
            state_root,
            observation_id="obs-heavy-001",
            node_id="root-kernel",
            payload=heavy_payload,
            producer="tests.hygiene",
        )
        if int(heavy.get("seq") or 0) != int(heavy_replay.get("seq") or 0):
            return _fail("replaying the same heavy-object observation must stay idempotent")

        if committed_event_count(state_root) != 2:
            return _fail("expected exactly two committed observation events after replay-dedup")

        events = iter_committed_events(state_root)
        event_types = [str(item.get("event_type") or "") for item in events]
        if event_types != ["operational_hygiene_observed", "heavy_object_observed"]:
            return _fail(f"unexpected observation event order/types: {event_types!r}")

        status = query_operational_hygiene_view(state_root)
        if int(status.get("committed_seq") or 0) != 2:
            return _fail("operational hygiene query must expose the latest committed seq")
        latest_hygiene = dict(status.get("latest_operational_hygiene") or {})
        latest_heavy = dict(status.get("latest_heavy_object") or {})
        if dict(latest_hygiene.get("payload") or {}) != hygiene_payload:
            return _fail("operational hygiene query must expose the latest hygiene payload")
        if dict(latest_heavy.get("payload") or {}) != heavy_payload:
            return _fail("operational hygiene query must expose the latest heavy-object payload")
        if int(status.get("operational_hygiene_event_count") or 0) != 1:
            return _fail("operational hygiene query must count hygiene observations by logical committed event")
        if int(status.get("heavy_object_event_count") or 0) != 1:
            return _fail("operational hygiene query must count heavy-object observations by logical committed event")
        gap_summary = dict(status.get("heavy_object_authority_gap_summary") or {})
        pressure_summary = dict(status.get("heavy_object_pressure_summary") or {})
        if int(gap_summary.get("filesystem_candidate_count") or 0) != len(pack_paths):
            return _fail("operational hygiene query must expose repo-local heavy-object candidates through the generic hygiene surface")
        if int(gap_summary.get("unmanaged_candidate_count") or 0) != len(pack_paths):
            return _fail("operational hygiene query must surface unmanaged heavy-object candidates while no lifecycle coverage exists")
        if pack_name not in str(gap_summary):
            return _fail("operational hygiene query must expose a compact heavy-object authority-gap sample")
        if int(pressure_summary.get("observed_duplicate_object_count") or 0) != 1:
            return _fail("operational hygiene query must surface duplicate-heavy-object pressure even before lifecycle registration exists")
        if int(pressure_summary.get("strongest_duplicate_count") or 0) != len(pack_paths):
            return _fail("operational hygiene query must expose strongest duplicate evidence through the generic pressure summary")
        if int(pressure_summary.get("registered_object_count") or 0) != 0:
            return _fail("operational hygiene query must not invent registered heavy-object pressure before lifecycle facts exist")
        if bool(pressure_summary.get("cleanup_pressure_present")):
            return _fail("operational hygiene query must not claim cleanup pressure when only raw duplicate observations exist")
        if "missing_matching_heavy_object_registration" not in [str(item) for item in list(pressure_summary.get("gaps") or [])]:
            return _fail("operational hygiene query must surface missing heavy-object registration as a generic pressure gap")

    print("[loop-system-event-journal-hygiene-observations] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
