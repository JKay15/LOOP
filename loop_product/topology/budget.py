"""Complexity-budget helpers."""

from __future__ import annotations


def default_complexity_budget() -> dict[str, int]:
    return {"max_active_nodes": 4, "max_child_generations": 3}
