#!/usr/bin/env python3
"""Guard that Milestone 4 targeted-closeout evidence stays frozen and complete."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_DOC = ROOT / "docs" / "contracts" / "LOOP_SYSTEM_EVENT_JOURNAL_MILESTONE4_TARGETED_CLOSEOUT.md"


REQUIRED_TEST_REFS = {
    "tests/check_loop_system_repo_event_journal_topology_command_event.py",
    "tests/check_loop_system_repo_topology_command_trace.py",
    "tests/check_loop_system_repo_event_journal_activate_command_event.py",
    "tests/check_loop_system_repo_activate_command_trace.py",
    "tests/check_loop_system_repo_event_journal_recovery_command_event.py",
    "tests/check_loop_system_repo_recovery_command_trace.py",
    "tests/check_loop_system_repo_event_journal_relaunch_command_event.py",
    "tests/check_loop_system_repo_relaunch_command_trace.py",
    "tests/check_loop_system_repo_event_journal_evaluator_result_event.py",
    "tests/check_loop_system_repo_evaluator_result_trace.py",
    "tests/check_loop_system_repo_event_journal_terminal_result_event.py",
    "tests/check_loop_system_repo_terminal_result_trace.py",
    "tests/check_loop_system_repo_launch_lineage_trace.py",
    "tests/check_loop_system_repo_lease_lineage_trace.py",
    "tests/check_loop_system_repo_node_lineage_trace.py",
    "tests/check_loop_system_repo_runtime_identity_trace.py",
    "tests/check_loop_system_repo_command_parity_debugger.py",
    "tests/check_loop_system_repo_topology_command_family_parity.py",
    "tests/check_loop_system_repo_runtime_identity_parity_debugger.py",
    "tests/check_loop_system_repo_lineage_parity_debugger.py",
    "tests/check_loop_system_repo_event_journal_repo_tree_cleanup_event.py",
    "tests/check_loop_system_repo_repo_tree_cleanup_trace.py",
    "tests/check_loop_system_repo_repo_tree_cleanup_parity_debugger.py",
}

REQUIRED_DOC_SNIPPETS = (
    "command migration and observability",
    "command-event, read-only trace, lineage, and parity-debugger surfaces",
    "split/activate/retry/resume/evaluator/terminal",
    "historical bug matrix is green",
    "exact non-core profiles (`nightly-only`, `soak-only`) are green",
    "LOOP process residue converges back to zero after verification",
    "python tests/run.py --profile core",
    "Milestone 4 targeted closeout does not replace the formal `core` gate.",
    "Milestone 4 targeted closeout does not complete Milestone 5 heavy-object lifecycle/share/retention work.",
)


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-milestone4-targeted-closeout][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not CLOSEOUT_DOC.exists():
        return _fail(f"missing Milestone 4 targeted closeout doc: {CLOSEOUT_DOC}")

    text = CLOSEOUT_DOC.read_text(encoding="utf-8")
    for snippet in REQUIRED_DOC_SNIPPETS:
        if snippet not in text:
            return _fail(f"Milestone 4 targeted closeout doc missing required snippet: {snippet!r}")

    found_refs = set(re.findall(r"`(tests/check_[^`]+\.py)`", text))
    missing_refs = sorted(REQUIRED_TEST_REFS - found_refs)
    if missing_refs:
        return _fail("Milestone 4 targeted closeout doc missing required evidence refs: " + ", ".join(missing_refs))

    for rel in sorted(found_refs):
        path = ROOT / rel
        if not path.exists():
            return _fail(f"Milestone 4 targeted closeout evidence ref missing on disk: {path}")

    print("[loop-system-event-journal-milestone4-targeted-closeout][OK] Milestone 4 targeted closeout evidence is frozen and complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
