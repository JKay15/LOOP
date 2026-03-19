"""Roundbook helpers for node-local execution history."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loop_product.runtime_paths import require_runtime_root


def append_round_entry(state_root: Path, node_id: str, entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Append a round entry for the given node."""

    state_root = require_runtime_root(state_root)
    path = state_root / "artifacts" / f"roundbook_{node_id}.json"
    rounds: list[dict[str, Any]] = []
    if path.exists():
        rounds = json.loads(path.read_text(encoding="utf-8"))
    rounds.append(entry)
    path.write_text(json.dumps(rounds, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return rounds
