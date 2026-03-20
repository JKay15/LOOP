"""Committed root-side supervision loop for launched child nodes."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from loop_product.kernel.state import load_kernel_state
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.schema import validate_repo_object


def _absolute(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return _load_json(path)
    except Exception:
        return {}


def _authoritative_result_ref(*, state_root: Path, node_id: str) -> Path:
    try:
        node_payload = dict(load_kernel_state(state_root).nodes.get(node_id) or {})
    except Exception:
        node_payload = {}
    if node_payload:
        node = NodeSpec.from_dict(node_payload)
        sink = str(node.result_sink_ref or f"artifacts/{node.node_id}/implementer_result.json").strip()
    else:
        sink = f"artifacts/{node_id}/implementer_result.json"
    return (state_root / sink).resolve()


def _classify_terminal_result(payload: Mapping[str, Any]) -> str:
    evaluator_result = dict(payload.get("evaluator_result") or {})
    verdict = str(evaluator_result.get("verdict") or "").strip().upper()
    retryable = bool(evaluator_result.get("retryable"))
    if verdict == "PASS":
        return "PASS"
    if retryable and verdict:
        return "RETRYABLE_TERMINAL"
    if verdict:
        return "TERMINAL_NONRETRYABLE"
    return "MISSING"


def _build_recovery_payload(
    *,
    status: Mapping[str, Any],
    result_ref: Path | None,
    result_payload: Mapping[str, Any] | None,
    reason: str = "retryable_terminal_result_after_child_stop",
    observation_kind: str = "retryable_terminal_result",
    default_self_attribution: str = "retryable_terminal_result",
    default_self_repair: str = "continue the same implementer task, repair the unmet in-scope requirements, and rerun evaluator",
    default_summary: str = "child stopped after a retryable terminal evaluator result",
) -> dict[str, Any]:
    evaluator_result = dict((result_payload or {}).get("evaluator_result") or {})
    diagnostics = dict(evaluator_result.get("diagnostics") or {})
    evidence_refs = []
    for candidate in (
        str(status.get("status_result_ref") or ""),
        str(result_ref or ""),
        str((result_payload or {}).get("evaluation_report_ref") or ""),
        str((result_payload or {}).get("reviewer_response_ref") or ""),
    ):
        if candidate and candidate not in evidence_refs:
            evidence_refs.append(candidate)
    return {
        "state_root": str(status.get("state_root") or ""),
        "node_id": str(status.get("node_id") or ""),
        "workspace_root": str(status.get("workspace_root") or ""),
        "confirmed_launch_result_ref": str(status.get("launch_result_ref") or ""),
        "confirmed_runtime_status_ref": str(status.get("status_result_ref") or ""),
        "reason": reason,
        "self_attribution": str(
            diagnostics.get("self_attribution")
            or default_self_attribution
        ),
        "self_repair": str(
            diagnostics.get("self_repair")
            or default_self_repair
        ),
        "observation_kind": observation_kind,
        "summary": str(
            evaluator_result.get("summary")
            or (result_payload or {}).get("summary")
            or default_summary
        ),
        "evidence_refs": evidence_refs,
    }


def _retryable_issue_kind(
    *,
    status: Mapping[str, Any],
    result_payload: Mapping[str, Any] | None,
) -> str:
    evaluator_result = dict((result_payload or {}).get("evaluator_result") or {})
    diagnostics = dict(evaluator_result.get("diagnostics") or {})
    issue_kind = str(diagnostics.get("issue_kind") or "").strip().lower()
    if issue_kind:
        return issue_kind
    launch_result_ref = _absolute(str(status.get("launch_result_ref") or ""))
    launch_payload = _load_optional_json(launch_result_ref)
    return str(launch_payload.get("retryable_failure_kind") or "").strip().lower()


def _recovery_delay_s(*, issue_kind: str, recovery_count: int) -> float:
    normalized = str(issue_kind or "").strip().lower()
    if normalized == "provider_capacity":
        return min(3.0, 0.75 * max(1, recovery_count))
    if normalized in {"provider_transport", "provider_runtime"}:
        return min(2.0, 0.5 * max(1, recovery_count))
    return 0.0


def supervise_child_until_settled(
    *,
    launch_result_ref: str | Path,
    poll_interval_s: float = 2.0,
    stall_threshold_s: float = 60.0,
    max_recoveries: int = 5,
    max_wall_clock_s: float = 0.0,
    runtime_status_reader: Callable[..., dict[str, Any]],
    recovery_runner: Callable[..., dict[str, Any]],
    launcher: Callable[..., dict[str, Any]],
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    current_launch_result_ref = _absolute(launch_result_ref)
    recoveries_used = 0
    started_at = time.time()
    history: list[dict[str, Any]] = []

    while True:
        status = runtime_status_reader(
            result_ref=str(current_launch_result_ref),
            stall_threshold_s=stall_threshold_s,
        )
        state_root = _absolute(str(status.get("state_root") or ""))
        node_id = str(status.get("node_id") or "").strip()
        result_ref = _authoritative_result_ref(state_root=state_root, node_id=node_id)
        result_payload = _load_json(result_ref) if result_ref.exists() else None
        classification = _classify_terminal_result(result_payload or {})
        history.append(
            {
                "launch_result_ref": str(current_launch_result_ref),
                "status_result_ref": str(status.get("status_result_ref") or ""),
                "pid_alive": bool(status.get("pid_alive")),
                "recovery_eligible": bool(status.get("recovery_eligible")),
                "result_ref": str(result_ref) if result_ref.exists() else "",
                "result_classification": classification,
            }
        )

        if classification == "PASS":
            result = {
                "launch_result_ref": str(_absolute(launch_result_ref)),
                "latest_launch_result_ref": str(current_launch_result_ref),
                "node_id": node_id,
                "state_root": str(state_root),
                "workspace_root": str(status.get("workspace_root") or ""),
                "settled": True,
                "settled_reason": "pass",
                "recoveries_used": recoveries_used,
                "implementer_result_ref": str(result_ref),
                "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
                "evaluator_verdict": "PASS",
                "status_result_ref": str(status.get("status_result_ref") or ""),
                "history": history,
            }
            validate_repo_object("LoopChildSupervisionResult.schema.json", result)
            return result

        if classification == "TERMINAL_NONRETRYABLE":
            result = {
                "launch_result_ref": str(_absolute(launch_result_ref)),
                "latest_launch_result_ref": str(current_launch_result_ref),
                "node_id": node_id,
                "state_root": str(state_root),
                "workspace_root": str(status.get("workspace_root") or ""),
                "settled": True,
                "settled_reason": "terminal_nonretryable",
                "recoveries_used": recoveries_used,
                "implementer_result_ref": str(result_ref),
                "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
                "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
                "status_result_ref": str(status.get("status_result_ref") or ""),
                "history": history,
            }
            validate_repo_object("LoopChildSupervisionResult.schema.json", result)
            return result

        retryable_terminal_stopped = classification == "RETRYABLE_TERMINAL" and not bool(status.get("pid_alive"))
        incomplete_terminal_stopped = (
            classification == "MISSING"
            and result_ref.exists()
            and not bool(status.get("pid_alive"))
        )
        runtime_loss_without_result = classification == "MISSING" and (
            bool(status.get("recovery_eligible")) or incomplete_terminal_stopped
        )
        if retryable_terminal_stopped or runtime_loss_without_result:
            if recoveries_used >= max(0, int(max_recoveries)):
                result = {
                    "launch_result_ref": str(_absolute(launch_result_ref)),
                    "latest_launch_result_ref": str(current_launch_result_ref),
                    "node_id": node_id,
                    "state_root": str(state_root),
                    "workspace_root": str(status.get("workspace_root") or ""),
                    "settled": True,
                    "settled_reason": "retry_budget_exhausted",
                    "recoveries_used": recoveries_used,
                    "implementer_result_ref": str(result_ref) if result_ref.exists() else "",
                    "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
                    "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
                    "status_result_ref": str(status.get("status_result_ref") or ""),
                    "history": history,
                }
                validate_repo_object("LoopChildSupervisionResult.schema.json", result)
                return result
            delay_s = _recovery_delay_s(
                issue_kind=_retryable_issue_kind(status=status, result_payload=result_payload),
                recovery_count=recoveries_used + 1,
            )
            if delay_s > 0.0:
                sleep_fn(delay_s)
            if incomplete_terminal_stopped:
                recovery_payload = _build_recovery_payload(
                    status=status,
                    result_ref=result_ref if result_ref.exists() else None,
                    result_payload=result_payload,
                    reason="incomplete_terminal_result_after_child_stop",
                    observation_kind="incomplete_terminal_result",
                    default_self_attribution="terminal_result_missing_evaluator_evidence",
                    default_self_repair=(
                        "materialize evaluator-backed terminal closure through the committed evaluator path instead of hand-writing an implementer result"
                    ),
                    default_summary=(
                        "child stopped after writing a completed implementer_result without evaluator-backed closure evidence"
                    ),
                )
            else:
                recovery_payload = _build_recovery_payload(
                    status=status,
                    result_ref=result_ref if result_ref.exists() else None,
                    result_payload=result_payload,
                )
            recovery_result = recovery_runner(**recovery_payload)
            relaunched = launcher(result_ref=str(recovery_result["recovery_result_ref"]))
            current_launch_result_ref = _absolute(str(relaunched["launch_result_ref"]))
            recoveries_used += 1
            continue

        if max_wall_clock_s > 0 and (time.time() - started_at) >= max_wall_clock_s:
            result = {
                "launch_result_ref": str(_absolute(launch_result_ref)),
                "latest_launch_result_ref": str(current_launch_result_ref),
                "node_id": node_id,
                "state_root": str(state_root),
                "workspace_root": str(status.get("workspace_root") or ""),
                "settled": False,
                "settled_reason": "timeout",
                "recoveries_used": recoveries_used,
                "implementer_result_ref": str(result_ref) if result_ref.exists() else "",
                "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
                "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
                "status_result_ref": str(status.get("status_result_ref") or ""),
                "history": history,
            }
            validate_repo_object("LoopChildSupervisionResult.schema.json", result)
            return result

        sleep_fn(max(0.05, float(poll_interval_s)))
