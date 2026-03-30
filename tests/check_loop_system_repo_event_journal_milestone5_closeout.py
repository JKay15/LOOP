#!/usr/bin/env python3
"""Guard that Milestone 5 formal closeout evidence stays frozen and complete."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLOSEOUT_DOC = ROOT / "docs" / "contracts" / "LOOP_SYSTEM_EVENT_JOURNAL_MILESTONE5_CLOSEOUT.md"


REQUIRED_TEST_REFS = {
    "tests/check_loop_system_repo_heavy_object_gap_characterization.py",
    "tests/check_loop_system_repo_repo_global_heavy_object_auto_lifecycle.py",
    "tests/check_loop_system_repo_heavy_object_lifecycle_command_family_parity.py",
    "tests/check_loop_system_repo_heavy_object_sharing_inventory_view.py",
}

REQUIRED_DOC_SNIPPETS = (
    "Milestone 5 formal closeout",
    "builds on the already-frozen Milestone 4 formal closeout",
    "full strict non-core closeout gate",
    "python tests/run.py --profile core",
    "Milestone 5 formal closeout does not complete Milestone 6 hardening/productization work.",
)


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-milestone5-closeout][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not CLOSEOUT_DOC.exists():
        return _fail(f"missing Milestone 5 closeout doc: {CLOSEOUT_DOC}")

    text = CLOSEOUT_DOC.read_text(encoding="utf-8")
    for snippet in REQUIRED_DOC_SNIPPETS:
        if snippet not in text:
            return _fail(f"Milestone 5 closeout doc missing required snippet: {snippet!r}")

    found_test_refs = set(re.findall(r"`(tests/check_[^`]+\.py)`", text))
    missing_refs = sorted(REQUIRED_TEST_REFS - found_test_refs)
    if missing_refs:
        return _fail("Milestone 5 closeout doc missing required evidence refs: " + ", ".join(missing_refs))

    for rel in sorted(found_test_refs):
        path = ROOT / rel
        if not path.exists():
            return _fail(f"Milestone 5 closeout evidence ref missing on disk: {path}")

    artifact_refs = set(re.findall(r"`(/Users/[^`]+)`", text))
    if not artifact_refs:
        return _fail("Milestone 5 closeout doc must record verification artifact paths")
    missing_artifacts = sorted(path for path in artifact_refs if not Path(path).exists())
    if missing_artifacts:
        return _fail("Milestone 5 closeout doc references missing verification artifacts: " + ", ".join(missing_artifacts))

    print("[loop-system-event-journal-milestone5-closeout][OK] Milestone 5 formal closeout evidence is frozen and complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
