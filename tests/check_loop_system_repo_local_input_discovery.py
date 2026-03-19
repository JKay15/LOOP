#!/usr/bin/env python3
"""Validate the committed local filename/path discovery helper."""

from __future__ import annotations

import json
import sys
import tempfile
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-local-input-discovery][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    try:
        from loop_product.local_input_discovery import (
            _normalize_search_roots,
            _parse_args,
            discover_local_input_candidates,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import discovery helper: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_local_input_discovery_") as td:
        temp_root = Path(td)
        desktop_root = temp_root / "Desktop"
        music_root = temp_root / "Music"
        downloads_root = temp_root / "Downloads"
        desktop_root.mkdir(parents=True, exist_ok=True)
        music_root.mkdir(parents=True, exist_ok=True)
        downloads_root.mkdir(parents=True, exist_ok=True)

        target = desktop_root / "音乐" / "departure! - 小野正利.flac"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"fake flac bytes")

        noise = downloads_root / "notes.txt"
        noise.write_text("departure keyword inside file body only", encoding="utf-8")
        pruned_noise = desktop_root / "node_modules" / "departure.mp3"
        pruned_noise.parent.mkdir(parents=True, exist_ok=True)
        pruned_noise.write_bytes(b"fake mp3 bytes")

        result = discover_local_input_candidates(
            query_terms=["departure", "小野正利"],
            roots=[desktop_root, music_root, downloads_root],
            extensions=["flac", ".mp3"],
            limit=10,
        )
        candidates = list(result.get("candidates") or [])
        if not candidates:
            return _fail("discovery helper must return at least one candidate for matching filename/path terms")
        if str(candidates[0].get("path") or "") != str(target.resolve()):
            return _fail("discovery helper must prefer the filename/path match rather than unrelated files")
        if any(str(item.get("path") or "") == str(noise.resolve()) for item in candidates):
            return _fail("discovery helper must not return files that only match in file contents")
        if any(str(item.get("path") or "") == str(pruned_noise.resolve()) for item in candidates):
            return _fail("discovery helper must prune heavy implementation directories from local input search")

        normalized_roots = _normalize_search_roots([temp_root], home_root=temp_root)
        expected_roots = [desktop_root.resolve(), music_root.resolve(), downloads_root.resolve()]
        if normalized_roots != expected_roots:
            return _fail("discovery helper must collapse a too-broad home root into the shallow default search roots")

        bare_args = _parse_args(["departure", "小野正利"])
        if list(bare_args.term or []) != ["departure", "小野正利"]:
            return _fail("discovery helper CLI must accept bare query terms without requiring --term")

        explicit_search_args = _parse_args(["search", "--term", "departure", "--root", str(desktop_root)])
        if list(explicit_search_args.term or []) != ["departure"]:
            return _fail("discovery helper CLI must continue accepting the explicit search subcommand form")
        if list(explicit_search_args.root or []) != [str(desktop_root)]:
            return _fail("discovery helper CLI must preserve explicit --root flags in search-subcommand form")

        cli_proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "loop_product.local_input_discovery",
                "--root",
                str(desktop_root),
                "--term",
                "departure",
                "--ext",
                "flac",
                "--limit",
                "3",
            ],
            cwd=str(ROOT),
            check=False,
            capture_output=True,
            text=True,
        )
        if cli_proc.returncode != 0:
            return _fail(f"discovery helper module CLI must succeed: {cli_proc.stderr.strip() or cli_proc.stdout.strip()}")
        cli_payload = json.loads(cli_proc.stdout)
        if list(cli_payload.get("query_terms") or []) != ["departure"]:
            return _fail("discovery helper module CLI must preserve explicit --term arguments")
        if list(cli_payload.get("extensions") or []) != [".flac"]:
            return _fail("discovery helper module CLI must preserve explicit --ext arguments")
        if list(cli_payload.get("roots") or []) != [str(desktop_root.resolve())]:
            return _fail("discovery helper module CLI must preserve explicit --root arguments")

    print("[loop-system-local-input-discovery] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
