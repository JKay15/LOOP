#!/usr/bin/env python3
"""Guard that Milestone 1 closeout evidence stays frozen and complete."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_DOC = ROOT / "docs" / "contracts" / "LOOP_SYSTEM_EVENT_JOURNAL_MILESTONE1_CLOSEOUT.md"


REQUIRED_TEST_REFS = {
    "tests/check_loop_system_repo_event_journal_foundation.py",
    "tests/check_loop_system_repo_event_journal_meta.py",
    "tests/check_loop_system_repo_event_journal_meta_guardrails.py",
    "tests/check_loop_system_repo_event_journal_submit_mirror.py",
    "tests/check_loop_system_repo_event_journal_schema_guardrails.py",
    "tests/check_loop_system_repo_event_journal_compatibility_status.py",
    "tests/check_loop_system_repo_event_journal_commit_state.py",
    "tests/check_loop_system_repo_event_journal_projection_reconcile.py",
    "tests/check_loop_system_repo_event_journal_projection_rebuild.py",
    "tests/check_loop_system_repo_event_journal_projection_rebuild_blocked.py",
    "tests/check_loop_system_repo_kernel_envelope_idempotence.py",
    "tests/check_loop_system_repo_evaluator_authority_status_sync.py",
    "tests/check_loop_system_repo_launch_reuse_pid_identity.py",
    "tests/check_loop_system_repo_retryable_recovery_status_refresh.py",
    "tests/check_loop_system_repo_child_supervision_spawn_dedupe.py",
    "tests/check_loop_system_repo_host_child_launch_supervisor.py",
    "tests/check_loop_system_repo_child_runtime_status_helper.py",
    "tests/check_loop_system_repo_milestone0_inventory_guardrails.py",
    "tests/check_loop_system_repo_event_journal_surface_guardrails.py",
}

REQUIRED_DOC_SNIPPETS = (
    "canonical event schema",
    "embedded journal store",
    "atomic commit API returning global `seq`",
    "command id / event id / causal ref model",
    "compatibility shim",
    "schema-versioned event envelope format",
    "commit-state model",
    "codex exec",
    "python tests/run.py --profile core",
)


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-milestone1-closeout][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not CLOSEOUT_DOC.exists():
        return _fail(f"missing Milestone 1 closeout doc: {CLOSEOUT_DOC}")

    text = CLOSEOUT_DOC.read_text(encoding="utf-8")
    for snippet in REQUIRED_DOC_SNIPPETS:
        if snippet not in text:
            return _fail(f"Milestone 1 closeout doc missing required snippet: {snippet!r}")

    found_refs = set(re.findall(r"`(tests/check_[^`]+\.py)`", text))
    missing_refs = sorted(REQUIRED_TEST_REFS - found_refs)
    if missing_refs:
        return _fail("Milestone 1 closeout doc missing required evidence refs: " + ", ".join(missing_refs))

    for rel in sorted(found_refs):
        path = ROOT / rel
        if not path.exists():
            return _fail(f"Milestone 1 closeout evidence ref missing on disk: {path}")

    print("[loop-system-event-journal-milestone1-closeout][OK] Milestone 1 closeout evidence is frozen and complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
