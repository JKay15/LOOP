"""Shared helpers for evaluator in-flight authority gating."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from loop_product.runtime_paths import require_runtime_root

_INFLIGHT_EVALUATOR_STATUSES = {
    "IN_PROGRESS",
    "RECOVERY_REQUIRED",
}
_TERMINALISH_RESULT_STATUSES = {
    "BLOCKED",
    "COMPLETED",
    "FAILED",
    "STUCK",
    "FAIL",
    "PASS",
}
_TERMINALISH_OUTCOMES = {
    "BLOCKED",
    "COMPLETED",
    "FAIL",
    "FAILED",
    "PASS",
    "STUCK",
    "BUG",
    "ERROR",
}
_NONTERMINAL_SPLIT_OUTCOMES = {
    "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
    "SPLIT_ACCEPTED_AWAITING_CHILDREN",
}


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def authoritative_result_retryable_nonterminal(payload: Mapping[str, Any]) -> bool:
    evaluator_result = dict(payload.get("evaluator_result") or {})
    verdict = str(evaluator_result.get("verdict") or "").strip().upper()
    retryable = bool(evaluator_result.get("retryable"))
    return bool(verdict) and retryable and verdict != "PASS"


def authoritative_result_has_evaluator_evidence(payload: Mapping[str, Any]) -> bool:
    evaluator_result = dict(payload.get("evaluator_result") or {})
    verdict = str(evaluator_result.get("verdict") or "").strip().upper()
    if not verdict:
        return False
    for candidate in (
        payload.get("evaluation_report_ref"),
        dict(payload.get("runtime_refs") or {}).get("evaluation_report_ref"),
        dict(payload.get("evaluator") or {}).get("evaluation_report_ref"),
    ):
        ref = str(candidate or "").strip()
        if ref and Path(ref).expanduser().resolve().exists():
            return True
    return False


def latest_evaluator_run_state_ref(*, state_root: Path, node_id: str) -> Path | None:
    runtime_root = require_runtime_root(state_root)
    base = runtime_root / "artifacts" / "evaluator_runs" / node_id
    if not base.exists():
        return None
    candidates = list(base.glob("**/EvaluatorRunState.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def load_latest_evaluator_run_state(*, state_root: Path, node_id: str) -> tuple[Path | None, dict[str, Any]]:
    run_state_ref = latest_evaluator_run_state_ref(state_root=state_root, node_id=node_id)
    if run_state_ref is None:
        return None, {}
    try:
        return run_state_ref, _load_json(run_state_ref)
    except Exception:
        return run_state_ref, {}


def evaluator_run_is_inflight(payload: Mapping[str, Any]) -> bool:
    status = str(payload.get("status") or "").strip().upper()
    return status in _INFLIGHT_EVALUATOR_STATUSES


def evaluator_backed_terminal_closure(payload: Mapping[str, Any]) -> bool:
    if str(payload.get("status") or "").strip().upper() != "COMPLETED":
        return False
    if not authoritative_result_has_evaluator_evidence(payload):
        return False
    if authoritative_result_retryable_nonterminal(payload):
        return False
    return True


def authoritative_result_claims_terminal_state(payload: Mapping[str, Any]) -> bool:
    if authoritative_result_retryable_nonterminal(payload):
        return False
    status = str(payload.get("status") or "").strip().upper()
    outcome = str(payload.get("outcome") or "").strip().upper()
    if status in _TERMINALISH_RESULT_STATUSES:
        return True
    if outcome in _TERMINALISH_OUTCOMES and outcome not in _NONTERMINAL_SPLIT_OUTCOMES:
        return True
    return False


def authoritative_result_conflicts_with_inflight_evaluator(
    *,
    state_root: Path,
    node_id: str,
    payload: Mapping[str, Any],
) -> bool:
    if not payload:
        return False
    if evaluator_backed_terminal_closure(payload):
        return False
    if not authoritative_result_claims_terminal_state(payload):
        return False
    _run_state_ref, run_state = load_latest_evaluator_run_state(state_root=state_root, node_id=node_id)
    if not run_state:
        return False
    return evaluator_run_is_inflight(run_state)
