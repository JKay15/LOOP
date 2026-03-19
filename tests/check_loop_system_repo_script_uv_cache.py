#!/usr/bin/env python3
"""Validate repo-shipped wrapper scripts default UV cache under repo-shared cache."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-script-uv-cache][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    scripts = (
        ROOT / "scripts" / "bootstrap_first_implementer_from_endpoint.sh",
        ROOT / "scripts" / "launch_child_from_result.sh",
        ROOT / "scripts" / "recover_orphaned_active.sh",
        ROOT / "scripts" / "run_evaluator_node_until_terminal.sh",
        ROOT / "scripts" / "persist_clarification_question.sh",
        ROOT / "scripts" / "persist_clarification_confirmation.sh",
        ROOT / "scripts" / "find_local_input_candidates.sh",
    )
    for path in scripts:
        text = path.read_text(encoding="utf-8")
        if 'shared_uv_cache="${repo_root}/.cache/uv"' not in text:
            return _fail(f"{path.name} must default UV cache to the repo-shared .cache/uv directory")
        if 'export UV_CACHE_DIR="${UV_CACHE_DIR:-${shared_uv_cache}}"' not in text:
            return _fail(f"{path.name} must preserve explicit UV_CACHE_DIR overrides")
        if 'mkdir -p "${UV_CACHE_DIR}"' not in text:
            return _fail(f"{path.name} must materialize the chosen UV cache dir")
    print("[loop-system-script-uv-cache] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
