#!/usr/bin/env python3
"""Guard trusted event-journal read/replay surfaces against drift."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOOP_ROOT = ROOT / "loop_product"
SCRIPTS_ROOT = ROOT / "scripts"

ALLOWED_META_READERS = {
    "loop_product/event_journal/store.py",
    "loop_product/kernel/query.py",
}

ALLOWED_REPLAY_SURFACES = {
    "loop_product/event_journal/store.py",
    "loop_product/kernel/query.py",
    "loop_product/kernel/state.py",
}

ALLOWED_SEQ_READERS = {
    "loop_product/event_journal/store.py",
    "loop_product/kernel/query.py",
    "loop_product/kernel/state.py",
}


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-surface-guardrails][FAIL] {msg}", file=sys.stderr)
    return 2


def _iter_non_test_python() -> list[Path]:
    files = sorted(LOOP_ROOT.rglob("*.py"))
    files.extend(sorted(SCRIPTS_ROOT.rglob("*.py")))
    return [path for path in files if "/tests/" not in str(path)]


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT.resolve()))


def main() -> int:
    meta_reader_offenders: list[str] = []
    replay_surface_offenders: list[str] = []
    seq_reader_offenders: list[str] = []

    for path in _iter_non_test_python():
        rel = _relative(path)
        text = path.read_text(encoding="utf-8")
        if "load_event_journal_meta(" in text and rel not in ALLOWED_META_READERS:
            meta_reader_offenders.append(rel)
        if "iter_committed_events_after_seq(" in text and rel not in ALLOWED_REPLAY_SURFACES:
            replay_surface_offenders.append(rel)
        if (
            ("latest_committed_seq(" in text or "committed_event_count(" in text)
            and rel not in ALLOWED_SEQ_READERS
        ):
            seq_reader_offenders.append(rel)

    if meta_reader_offenders:
        return _fail(
            "disallowed direct load_event_journal_meta readers remain: "
            + ", ".join(sorted(meta_reader_offenders))
        )
    if replay_surface_offenders:
        return _fail(
            "disallowed direct journal replay readers remain: "
            + ", ".join(sorted(replay_surface_offenders))
        )
    if seq_reader_offenders:
        return _fail(
            "disallowed direct journal seq/count readers remain: "
            + ", ".join(sorted(seq_reader_offenders))
        )

    print("[loop-system-event-journal-surface-guardrails][OK] trusted journal read/replay surfaces stay bounded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
