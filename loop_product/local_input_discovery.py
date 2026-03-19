"""Committed local file-discovery helpers for implementer-side input lookup."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Sequence


_PRUNED_DIR_NAMES = (
    ".git",
    ".cache",
    ".venv",
    ".uv-cache",
    "node_modules",
    "__pycache__",
)


def _normalize_terms(terms: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in terms:
        value = str(raw or "").strip().lower()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _normalize_extensions(extensions: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for raw in extensions:
        value = str(raw or "").strip().lower()
        if not value:
            continue
        if not value.startswith("."):
            value = "." + value
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _default_roots(*, home_root: Path | None = None) -> list[Path]:
    home = (home_root or Path.home()).expanduser().resolve()
    return [
        home / "Desktop",
        home / "Music",
        home / "Downloads",
    ]


def _normalize_search_roots(
    roots: Sequence[str | Path] | None,
    *,
    home_root: Path | None = None,
) -> list[Path]:
    resolved_home = (home_root or Path.home()).expanduser().resolve()
    requested = list(roots or [])
    raw_roots = [Path(item).expanduser().resolve() for item in requested] if requested else _default_roots()
    if not requested:
        return _default_roots(home_root=resolved_home)
    normalized: list[Path] = []
    seen: set[Path] = set()
    for root in raw_roots:
        candidates = _default_roots(home_root=resolved_home) if root == resolved_home else [root]
        for candidate in candidates:
            resolved_candidate = candidate.expanduser().resolve()
            if resolved_candidate in seen:
                continue
            seen.add(resolved_candidate)
            normalized.append(resolved_candidate)
    return normalized


def _iter_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return ()
    find_exe = shutil.which("find")
    if find_exe:
        cmd = [find_exe, str(root), "(", "-type", "d", "("]
        for index, name in enumerate(_PRUNED_DIR_NAMES):
            if index:
                cmd.append("-o")
            cmd.extend(["-name", name])
        cmd.extend([")", "-prune", ")", "-o", "(", "-type", "f", "-print0", ")"])
        try:
            proc = subprocess.run(cmd, check=False, capture_output=True)
            if proc.returncode == 0:
                return (
                    Path(raw.decode("utf-8", errors="surrogateescape"))
                    for raw in proc.stdout.split(b"\0")
                    if raw
                )
        except OSError:
            pass
    try:
        return (path for path in root.rglob("*") if path.is_file())
    except OSError:
        return ()


def _score_candidate(path: Path, terms: Sequence[str]) -> tuple[int, list[str]]:
    haystack = str(path).lower()
    name = path.name.lower()
    matched_terms = [term for term in terms if term in haystack]
    if not terms:
        return (1, [])
    if not matched_terms:
        return (0, [])
    name_matches = sum(1 for term in matched_terms if term in name)
    score = len(matched_terms) * 100 + name_matches * 25
    if len(matched_terms) == len(terms):
        score += 20
    score -= len(path.parts)
    return (score, matched_terms)


def discover_local_input_candidates(
    *,
    query_terms: Sequence[str],
    roots: Sequence[str | Path] | None = None,
    extensions: Sequence[str] | None = None,
    limit: int = 20,
) -> dict[str, Any]:
    normalized_terms = _normalize_terms(query_terms)
    normalized_exts = _normalize_extensions(extensions or [])
    search_roots = _normalize_search_roots(roots)
    results: list[dict[str, Any]] = []
    for root in search_roots:
        for path in _iter_files(root):
            if normalized_exts and path.suffix.lower() not in normalized_exts:
                continue
            score, matched_terms = _score_candidate(path, normalized_terms)
            if normalized_terms and not matched_terms:
                continue
            results.append(
                {
                    "path": str(path.resolve()),
                    "name": path.name,
                    "extension": path.suffix.lower(),
                    "matched_terms": matched_terms,
                    "score": score,
                    "size_bytes": path.stat().st_size,
                }
            )
    results.sort(key=lambda item: (-int(item["score"]), str(item["path"])))
    return {
        "query_terms": normalized_terms,
        "roots": [str(path) for path in search_roots],
        "extensions": normalized_exts,
        "candidates": results[: max(0, int(limit))],
    }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    raw_args = [str(item) for item in (list(sys.argv[1:]) if argv is None else list(argv))]
    if raw_args and raw_args[0] == "search":
        raw_args = raw_args[1:]
    parser = argparse.ArgumentParser(description="Find local file candidates by filename/path rather than content grep.")
    parser.add_argument("terms", nargs="*", help="Bare filename/path query terms.")
    parser.add_argument("--term", action="append", default=[], help="Filename/path query term. Repeatable.")
    parser.add_argument("--root", action="append", default=[], help="Root directory to search. Repeatable.")
    parser.add_argument("--ext", action="append", default=[], help="Allowed file extension (with or without dot).")
    parser.add_argument("--limit", type=int, default=20, help="Maximum number of candidates to emit.")
    args = parser.parse_args(raw_args)
    args.term = list(args.term or []) + [str(item) for item in list(args.terms or []) if str(item).strip()]
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    payload = discover_local_input_candidates(
        query_terms=list(args.term or []),
        roots=list(args.root or []),
        extensions=list(args.ext or []),
        limit=int(args.limit),
    )
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
