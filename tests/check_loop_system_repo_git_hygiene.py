#!/usr/bin/env python3
"""Git hygiene regression checks for the standalone loop_product_repo."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-git-hygiene][FAIL] {msg}", file=sys.stderr)
    return 2


def _check_ignore(path: str, *, expected_ignored: bool) -> str | None:
    proc = subprocess.run(
        ["git", "check-ignore", "-v", path],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return proc.stdout.strip() or ""
    if proc.returncode == 1:
        return None
    raise RuntimeError(proc.stderr.strip() or f"git check-ignore failed for {path}")


def main() -> int:
    ignored_paths = [
        ".loop",
        ".cache",
        "artifacts",
        ".pytest_cache/dummy",
        ".mypy_cache/dummy",
        ".ruff_cache/dummy",
        "workspace/example-project/out/simple_stdout_handoff/current/.loop/checker/prompt.md",
    ]
    for rel in ignored_paths:
        rule = _check_ignore(rel, expected_ignored=True)
        if rule is None:
            return _fail(f"expected {rel} to stay ignored by .gitignore")

    tracked_source_path = "tests/_temp_repo_root.py"
    if _check_ignore(tracked_source_path, expected_ignored=False) is not None:
        return _fail(f"{tracked_source_path} is source and must not be ignored")

    py_dirs = sorted(path.relative_to(ROOT).as_posix() for path in (ROOT / "loop_product").rglob("*.py") if path.is_dir())
    if py_dirs:
        return _fail(f"source tree must not contain directories at *.py paths: {py_dirs}")

    entry_path = ROOT / "loop_product" / "kernel" / "entry.py"
    if not entry_path.exists():
        return _fail("loop_product/kernel/entry.py must exist")
    if not entry_path.is_file():
        return _fail("loop_product/kernel/entry.py must be a source file, not a directory")

    print("[loop-system-git-hygiene] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
