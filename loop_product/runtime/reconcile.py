"""Minimal reconciliation helpers."""

from __future__ import annotations

from typing import Any


def summarize_active_nodes(nodes: dict[str, dict[str, Any]]) -> list[str]:
    return [node_id for node_id, data in nodes.items() if data.get("status") in {"PLANNED", "ACTIVE", "BLOCKED"}]
