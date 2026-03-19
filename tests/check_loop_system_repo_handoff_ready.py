#!/usr/bin/env python3
"""Aggregate acceptance for handoff sections 6 and 8."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-handoff-ready][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    required_checks = [
        ROOT / "tests" / "check_loop_system_repo_skeleton.py",
        ROOT / "tests" / "check_loop_system_repo_contracts.py",
        ROOT / "tests" / "check_loop_system_repo_agents_rules.py",
        ROOT / "tests" / "check_loop_system_repo_skill_skeletons.py",
        ROOT / "tests" / "check_loop_system_repo_smoke.py",
        ROOT / "tests" / "check_loop_system_repo_smoke_failure_closure.py",
        ROOT / "tests" / "check_loop_system_repo_smoke_parallel_isolation.py",
    ]
    for check in required_checks:
        proc = subprocess.run(
            [sys.executable, str(check)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return _fail(f"{check.name} failed: stdout={proc.stdout!r} stderr={proc.stderr!r}")

    print("[loop-system-handoff-ready] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
