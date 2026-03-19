#!/usr/bin/env python3
"""Validate the committed fresh clarification session-root helper."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "new_clarification_session_root.sh"


def _fail(msg: str) -> int:
    print(f"[loop-system-clarification-session-root-helper][FAIL] {msg}", file=sys.stderr)
    return 2


def _run_helper(*, slug: str, positional: bool = False) -> str:
    argv = [str(SCRIPT), slug] if positional else [str(SCRIPT), "--task-slug", slug]
    proc = subprocess.run(
        argv,
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or f"exit {proc.returncode}")
    return str(proc.stdout).strip()


def main() -> int:
    if not SCRIPT.exists():
        return _fail("missing scripts/new_clarification_session_root.sh")

    static_root = ROOT / ".cache" / "endpoint_clarification" / "apple_anime_birthday_poster"
    static_root.mkdir(parents=True, exist_ok=True)
    created: list[Path] = []
    try:
        first_rel = _run_helper(slug="apple_anime_birthday_poster")
        second_rel = _run_helper(slug="apple_anime_birthday_poster", positional=True)
        if not first_rel or not second_rel:
            return _fail("helper must print a session-root path")
        if first_rel == second_rel:
            return _fail("helper must allocate a new fresh session root on each call, even across flag and positional invocation styles")
        if first_rel == ".cache/endpoint_clarification/apple_anime_birthday_poster":
            return _fail("helper must not reuse the unsafe static session-root path for fresh tasks")
        if second_rel == ".cache/endpoint_clarification/apple_anime_birthday_poster":
            return _fail("helper must not reuse the unsafe static session-root path for fresh tasks")

        for rel in (first_rel, second_rel):
            if not rel.startswith(".cache/endpoint_clarification/"):
                return _fail("helper must allocate under .cache/endpoint_clarification/")
            path = ROOT / rel
            created.append(path)
            if not path.exists() or not path.is_dir():
                return _fail("helper must materialize the allocated session-root directory")
            if (path / "EndpointArtifact.json").exists():
                return _fail("fresh session-root allocation must not pre-create EndpointArtifact.json")

        print("[loop-system-clarification-session-root-helper] OK")
        return 0
    except Exception as exc:  # noqa: BLE001
        return _fail(str(exc))
    finally:
        for path in created:
            shutil.rmtree(path, ignore_errors=True)
        shutil.rmtree(static_root, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
