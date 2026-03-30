#!/usr/bin/env python3
"""Guard that Milestone 2 closeout evidence stays frozen and complete."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_DOC = ROOT / "docs" / "contracts" / "LOOP_SYSTEM_EVENT_JOURNAL_MILESTONE2_CLOSEOUT.md"


REQUIRED_TEST_REFS = {
    "tests/check_loop_system_repo_event_journal_projection_metadata.py",
    "tests/check_loop_system_repo_event_journal_commit_state.py",
    "tests/check_loop_system_repo_event_journal_projection_reconcile.py",
    "tests/check_loop_system_repo_event_journal_projection_status.py",
    "tests/check_loop_system_repo_event_journal_status.py",
    "tests/check_loop_system_repo_event_journal_projection_manifest.py",
    "tests/check_loop_system_repo_event_journal_projection_explanation.py",
    "tests/check_loop_system_repo_event_journal_projection_rebuild.py",
    "tests/check_loop_system_repo_event_journal_projection_rebuild_blocked.py",
    "tests/check_loop_system_repo_event_journal_projector_boundary.py",
    "tests/check_loop_system_repo_event_journal_node_projection_rebuild.py",
    "tests/check_loop_system_repo_event_journal_projection_provenance.py",
    "tests/check_loop_system_repo_event_journal_hygiene_observations.py",
    "tests/check_loop_system_repo_process_reap_gap_characterization.py",
    "tests/check_loop_system_repo_heavy_object_gap_characterization.py",
    "tests/check_loop_system_repo_state_query.py",
    "tests/check_loop_system_repo_evaluator_authority_status_sync.py",
    "tests/check_loop_system_repo_launch_reuse_pid_identity.py",
    "tests/check_loop_system_repo_retryable_recovery_status_refresh.py",
    "tests/check_loop_system_repo_child_supervision_spawn_dedupe.py",
    "tests/check_loop_system_repo_host_child_launch_supervisor.py",
    "tests/check_loop_system_repo_child_runtime_status_helper.py",
    "tests/check_loop_system_repo_milestone0_inventory_guardrails.py",
}

REQUIRED_DOC_SNIPPETS = (
    "projection engine and authoritative observation/debug surfaces",
    "projection metadata on kernel/node projections",
    "commit-state and crash-boundary visibility",
    "projection consistency/debug status view",
    "projection manifest and atomic bundle visibility",
    "projection explanation/debug surface",
    "explicit rebuild boundary",
    "projection provenance",
    "authoritative hygiene/heavy-object observation foundation",
    "Milestone 2 does not complete the lease/heartbeat/fencing model.",
    "Milestone 2 does not complete the canonical heavy-object lifecycle/share/retention model.",
    "python tests/run.py --profile core",
)


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-milestone2-closeout][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not CLOSEOUT_DOC.exists():
        return _fail(f"missing Milestone 2 closeout doc: {CLOSEOUT_DOC}")

    text = CLOSEOUT_DOC.read_text(encoding="utf-8")
    for snippet in REQUIRED_DOC_SNIPPETS:
        if snippet not in text:
            return _fail(f"Milestone 2 closeout doc missing required snippet: {snippet!r}")

    found_refs = set(re.findall(r"`(tests/check_[^`]+\.py)`", text))
    missing_refs = sorted(REQUIRED_TEST_REFS - found_refs)
    if missing_refs:
        return _fail("Milestone 2 closeout doc missing required evidence refs: " + ", ".join(missing_refs))

    for rel in sorted(found_refs):
        path = ROOT / rel
        if not path.exists():
            return _fail(f"Milestone 2 closeout evidence ref missing on disk: {path}")

    print("[loop-system-event-journal-milestone2-closeout][OK] Milestone 2 closeout evidence is frozen and complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
