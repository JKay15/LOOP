#!/usr/bin/env python3
"""Guard the split-workspace invariant design note."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "design" / "loop_system_split_workspace_invariants_v0_1.md"


def _fail(msg: str) -> int:
    print(f"[loop-system-split-workspace-design][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not DOC.exists():
        return _fail(f"missing design doc: {DOC.relative_to(ROOT)}")

    text = DOC.read_text(encoding="utf-8")
    required_snippets = (
        "## Writable Isolation",
        "## Explicit Shared Reads",
        "## Merge Inputs",
        "ACTIVE implementer nodes must never share the same writable directory",
        "shared resources must be declared explicitly as read-only inputs",
        "merge must consume declared artifacts or result refs",
        "must not scan sibling workspaces or historical deliverables implicitly",
        "publish targets are separate from implementer workspaces",
        "baseline selection is distinct from writable workspace ownership",
    )
    for snippet in required_snippets:
        if snippet not in text:
            return _fail(f"design doc missing required snippet: {snippet!r}")

    print("[loop-system-split-workspace-design] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
