#!/usr/bin/env python3
"""Durable run-state ledger for the thin evaluator."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from . import prototype as _legacy
from ..runtime_paths import product_schema_path

_RUN_STATE_SCHEMA_PATH = product_schema_path("LoopEvaluatorRunState.schema.json")
_RUN_STATE_FILENAME = "EvaluatorRunState.json"


def run_state_path(*, run_root: Path) -> Path:
    return run_root / _RUN_STATE_FILENAME


def request_fingerprint(*, request: Mapping[str, Any]) -> str:
    normalized = {
        "schema": str(request.get("schema") or ""),
        "schema_version": str(request.get("schema_version") or ""),
        "evaluation_id": str(request.get("evaluation_id") or ""),
        "run_id": str(request.get("run_id") or ""),
        "workspace_root": str(request.get("workspace_root") or ""),
        "product_manual_ref": str(request.get("product_manual_ref") or ""),
        "final_effects_text_ref": str(request.get("final_effects_text_ref") or ""),
        "final_effect_requirements": list(request.get("final_effect_requirements") or []),
        "role_requirements": dict(request.get("role_requirements") or {}),
        "agent_execution": dict(request.get("agent_execution") or {}),
    }
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def build_initial_run_state(
    *,
    context: Mapping[str, Any],
    request_fingerprint_value: str,
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "schema": "loop_product.loop_evaluator_run_state",
        "schema_version": "0.1.0",
        "evaluation_id": str(context["evaluation_id"]),
        "run_id": str(context["run_id"]),
        "run_root": str(context["run_root"]),
        "workspace_root": str(context["workspace_root"]),
        "product_manual_ref": str(context["product_manual_ref"]),
        "request_fingerprint": request_fingerprint_value,
        "status": "IN_PROGRESS",
        "checker": {
            "status": "PENDING",
            "attempt_count": 0,
            "notes": [],
            "repair_actions": [],
            "human_gate_reasons": [],
        },
        "final_effect_requirements": [],
        "runtime_closure_requirements": [],
        "lane_plan": [],
        "lanes_by_unit_id": {},
        "reviewer": {
            "status": "PENDING",
            "attempt_count": 0,
        },
        "notes": [],
    }
    final_effects_ref = context.get("final_effects_text_ref")
    if final_effects_ref is not None:
        state["final_effects_text_ref"] = str(final_effects_ref)
    return state


def load_run_state(*, run_root: Path) -> dict[str, Any] | None:
    path = run_state_path(run_root=run_root)
    if not path.exists():
        return None
    return _legacy._load_json(path)


def write_run_state(*, run_root: Path, state: Mapping[str, Any]) -> dict[str, Any]:
    run_root.mkdir(parents=True, exist_ok=True)
    state_obj = dict(state)
    _legacy._validate_with_schema(obj=state_obj, schema_path=_RUN_STATE_SCHEMA_PATH, label="evaluator run state")
    _legacy._write_json(run_state_path(run_root=run_root), state_obj)
    return state_obj


def assert_resume_compatible(
    *,
    state: Mapping[str, Any],
    context: Mapping[str, Any],
    request_fingerprint_value: str,
) -> None:
    if str(state.get("request_fingerprint") or "") != request_fingerprint_value:
        raise ValueError(
            "existing EvaluatorRunState.json belongs to a different evaluator input fingerprint; choose a new run_id instead of resuming this run"
        )
    for key in ("evaluation_id", "run_id"):
        if str(state.get(key) or "") != str(context[key]):
            raise ValueError(f"existing EvaluatorRunState.json does not match {key}")


def ordered_lanes(*, state: Mapping[str, Any]) -> list[dict[str, Any]]:
    lanes_by_unit_id = {str(key): dict(value) for key, value in dict(state.get("lanes_by_unit_id") or {}).items()}
    ordered: list[dict[str, Any]] = []
    for item in list(state.get("lane_plan") or []):
        unit_id = str(dict(item).get("unit_id") or "")
        if unit_id and unit_id in lanes_by_unit_id:
            ordered.append(dict(lanes_by_unit_id[unit_id]))
    for unit_id, lane in lanes_by_unit_id.items():
        if unit_id not in {str(dict(item).get("unit_id") or "") for item in list(state.get("lane_plan") or [])}:
            ordered.append(dict(lane))
    return ordered
