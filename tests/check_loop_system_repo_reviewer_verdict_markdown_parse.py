#!/usr/bin/env python3
"""Regression: simple evaluator reviewer verdict parser must accept markdown prose verdicts."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-reviewer-verdict-markdown-parse][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    sys.path.insert(0, str(ROOT))
    try:
        from loop_product.loop.evaluator_client import _parse_simple_reviewer_verdict
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    cases = {
        "VERDICT: PASS": "PASS",
        "FAIL": "FAIL",
        "Overall verdict: `PASS`.": "PASS",
        "Overall verdict: `FAIL`.": "FAIL",
        "Overall verdict: FAIL.": "FAIL",
    }
    for text, expected in cases.items():
        observed = _parse_simple_reviewer_verdict(text)
        if observed != expected:
            return _fail(f"expected {expected} for {text!r}, got {observed}")

    if _parse_simple_reviewer_verdict("No terminal verdict here.") != "UNKNOWN":
        return _fail("non-verdict prose must remain UNKNOWN")

    print("[loop-system-reviewer-verdict-markdown-parse] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
