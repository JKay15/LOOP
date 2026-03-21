"""Complexity-budget helpers."""

from __future__ import annotations

from typing import Any


_DEFAULT_COMPLEXITY_BUDGET = {
    "max_active_nodes": 8,
    "max_child_generations": 3,
}


def default_complexity_budget() -> dict[str, int]:
    return dict(_DEFAULT_COMPLEXITY_BUDGET)


def normalized_complexity_budget(raw: dict[str, Any] | None = None) -> dict[str, int]:
    budget = default_complexity_budget()
    for key, value in dict(raw or {}).items():
        if value in (None, ""):
            continue
        budget[str(key)] = int(value)
    return budget
