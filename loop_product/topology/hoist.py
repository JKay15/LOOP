"""Hoist placeholder helpers."""

from __future__ import annotations


def hoistable(node_kind: str) -> bool:
    return node_kind in {"reviewer", "evaluator", "implementer"}
