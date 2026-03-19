#!/usr/bin/env python3
"""Require a compact design doc capturing single-child lessons and split-upgrade direction."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOC = ROOT / "docs" / "design" / "loop_system_single_child_lessons_and_split_dag_v0_1.md"


def _fail(msg: str) -> int:
    print(f"[loop-system-single-child-lessons-split-design][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if not DOC.exists():
        return _fail(f"missing design doc: {DOC.relative_to(ROOT)}")
    text = DOC.read_text(encoding="utf-8")
    required = [
        "# Single-Child Lessons and Split/DAG Upgrade",
        "## Single-Child Lessons",
        "fresh by default",
        "committed bootstrap helper",
        "workspace mirror artifact",
        "terminal evaluator-backed result",
        "EvaluatorRunState.json",
        "## Split/DAG Upgrade",
        "implementer proposes, kernel decides",
        "parallel",
        "deferred",
        "depends_on",
        "activation_condition",
    ]
    for needle in required:
        if needle not in text:
            return _fail(f"design doc missing required text: {needle!r}")
    if len(text.splitlines()) > 120:
        return _fail("design doc should stay compact (<= 120 lines)")
    print("[loop-system-single-child-lessons-split-design] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
