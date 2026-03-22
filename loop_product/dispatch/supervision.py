"""Committed root-side supervision loop for launched child nodes."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from loop_product.dispatch.child_progress_snapshot import child_progress_snapshot_from_launch_result_ref
from loop_product.dispatch.launch_runtime import terminate_runtime_owned_launch_result_ref
from loop_product.kernel.state import load_kernel_state
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.schema import validate_repo_object

_PLACEHOLDER_DOC_BASENAMES = {
    "readme.md",
    "traceability.md",
    "status.md",
    "implementer_status.md",
    "paper_manifest.md",
}
_PLACEHOLDER_LOG_MARKERS = (
    "workspace-local artifact skeleton",
    "no split has been proposed yet",
    "evaluator has not been run yet",
    "planned outputs",
    "`pending`",
    "pending table",
    "todo",
    "placeholder",
)
_DEFAULT_INITIAL_REQUIRED_ARTIFACT_WINDOW_S = 60.0
_INITIAL_REQUIRED_ARTIFACT_WINDOW_BY_BUDGET_S = {
    "xhigh": 180.0,
    "high": 120.0,
    "medium": 60.0,
    "low": 45.0,
}


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


def _is_split_continuation_result(payload: Mapping[str, Any]) -> bool:
    outcome = str(payload.get("outcome") or "").strip().upper()
    return outcome == "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION"


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


def _progress_snapshot_fingerprint(snapshot: Mapping[str, Any]) -> str:
    payload = {
        "terminal_result_present": bool(snapshot.get("terminal_result_present")),
        "implementer_result_ref": str(snapshot.get("implementer_result_ref") or ""),
        "implementer_outcome": str(snapshot.get("implementer_outcome") or ""),
        "implementer_verdict": str(snapshot.get("implementer_verdict") or ""),
        "evaluator_status": str(snapshot.get("evaluator_status") or ""),
        "evaluator_running_unit_ids": list(snapshot.get("evaluator_running_unit_ids") or []),
        "evaluator_completed_unit_ids": list(snapshot.get("evaluator_completed_unit_ids") or []),
        "evaluator_blocked_retryable_unit_ids": list(snapshot.get("evaluator_blocked_retryable_unit_ids") or []),
        "evaluator_blocked_terminal_unit_ids": list(snapshot.get("evaluator_blocked_terminal_unit_ids") or []),
        "evaluator_pending_unit_ids": list(snapshot.get("evaluator_pending_unit_ids") or []),
        "observed_doc_refs": list(snapshot.get("observed_doc_refs") or []),
        "observed_evaluator_doc_refs": list(snapshot.get("observed_evaluator_doc_refs") or []),
        "recent_log_lines": list(snapshot.get("recent_log_lines") or []),
    }
    return json.dumps(payload, sort_keys=True)


def _load_frozen_handoff(workspace_root: Path) -> dict[str, Any]:
    return _load_optional_json((workspace_root / "FROZEN_HANDOFF.json").resolve())


def _path_has_nonempty_file(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_file():
        try:
            return path.stat().st_size > 0
        except OSError:
            return False
    try:
        for candidate in path.rglob("*"):
            if candidate.is_file():
                return True
    except OSError:
        return False
    return False


def _required_workspace_artifact_materialized(snapshot: Mapping[str, Any]) -> bool:
    workspace_root_raw = str(snapshot.get("workspace_root") or "").strip()
    if not workspace_root_raw:
        return False
    workspace_root = _absolute(workspace_root_raw)
    handoff = _load_frozen_handoff(workspace_root)
    candidates: list[Path] = []
    workspace_live_artifact_ref = str(handoff.get("workspace_live_artifact_ref") or "").strip()
    if workspace_live_artifact_ref:
        candidates.append(_absolute(workspace_live_artifact_ref))
    workspace_live_artifact_relpath = str(handoff.get("workspace_live_artifact_relpath") or "").strip()
    if workspace_live_artifact_relpath:
        candidates.append((workspace_root / workspace_live_artifact_relpath).resolve())
    workspace_mirror_ref = str(handoff.get("workspace_mirror_ref") or "").strip()
    if workspace_mirror_ref:
        candidates.append(_absolute(workspace_mirror_ref))
    workspace_mirror_relpath = str(handoff.get("workspace_mirror_relpath") or "").strip()
    if workspace_mirror_relpath:
        candidates.append((workspace_root / workspace_mirror_relpath).resolve())
    workspace_result_sink_ref = str(handoff.get("workspace_result_sink_ref") or "").strip()
    if workspace_result_sink_ref:
        candidates.append(_absolute(workspace_result_sink_ref))
    workspace_result_sink_relpath = str(handoff.get("workspace_result_sink_relpath") or "").strip()
    if workspace_result_sink_relpath:
        candidates.append((workspace_root / workspace_result_sink_relpath).resolve())
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if _path_has_nonempty_file(candidate):
            return True
    return False


def _startup_reasoning_budget(*, state_root: Path, node_id: str, status: Mapping[str, Any]) -> str:
    try:
        node_payload = dict(load_kernel_state(state_root).nodes.get(node_id) or {})
    except Exception:
        node_payload = {}
    if node_payload:
        reasoning_profile = dict(node_payload.get("reasoning_profile") or {})
        budget = str(reasoning_profile.get("thinking_budget") or "").strip().lower()
        if budget:
            return budget
    launch_result_ref = _absolute(str(status.get("launch_result_ref") or ""))
    launch_payload = _load_optional_json(launch_result_ref)
    wrapped_argv = list(launch_payload.get("wrapped_argv") or [])
    for token in wrapped_argv:
        token_text = str(token or "")
        marker = 'model_reasoning_effort="'
        if marker in token_text:
            suffix = token_text.split(marker, 1)[1]
            budget = suffix.split('"', 1)[0].strip().lower()
            if budget:
                return budget
    return ""


def _initial_required_artifact_window_s(*, state_root: Path, node_id: str, status: Mapping[str, Any]) -> float:
    budget = _startup_reasoning_budget(state_root=state_root, node_id=node_id, status=status)
    return float(
        _INITIAL_REQUIRED_ARTIFACT_WINDOW_BY_BUDGET_S.get(
            budget,
            _DEFAULT_INITIAL_REQUIRED_ARTIFACT_WINDOW_S,
        )
    )


def _placeholder_only_progress_snapshot(snapshot: Mapping[str, Any]) -> bool:
    if bool(snapshot.get("terminal_result_present")):
        return False
    if str(snapshot.get("implementer_result_ref") or "").strip():
        return False
    if str(snapshot.get("implementer_outcome") or "").strip():
        return False
    if str(snapshot.get("implementer_verdict") or "").strip():
        return False
    if str(snapshot.get("evaluator_status") or "").strip():
        return False
    for key in (
        "evaluator_running_unit_ids",
        "evaluator_completed_unit_ids",
        "evaluator_blocked_retryable_unit_ids",
        "evaluator_blocked_terminal_unit_ids",
        "evaluator_pending_unit_ids",
    ):
        if list(snapshot.get(key) or []):
            return False
    observed_doc_refs = [str(ref or "").strip() for ref in list(snapshot.get("observed_doc_refs") or []) if str(ref or "").strip()]
    if observed_doc_refs:
        basenames = {Path(ref).name.lower() for ref in observed_doc_refs}
        if any(name not in _PLACEHOLDER_DOC_BASENAMES for name in basenames):
            return False
    recent_lines = [str(line or "").strip().lower() for line in list(snapshot.get("recent_log_lines") or []) if str(line or "").strip()]
    if not recent_lines:
        return False
    return all(any(marker in line for marker in _PLACEHOLDER_LOG_MARKERS) for line in recent_lines)


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
    launch_terminator: Callable[..., None] | None = None,
    progress_snapshot_reader: Callable[..., dict[str, Any]] | None = None,
    no_substantive_progress_window_s: float = 300.0,
    now_fn: Callable[[], float] = time.time,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    current_launch_result_ref = _absolute(launch_result_ref)
    recoveries_used = 0
    started_at = now_fn()
    history: list[dict[str, Any]] = []
    placeholder_progress_fingerprint = ""
    placeholder_progress_started_at = 0.0
    empty_required_artifact_started_at = 0.0
    if progress_snapshot_reader is None:
        def _default_progress_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, Any]:
            return child_progress_snapshot_from_launch_result_ref(
                result_ref=result_ref,
                stall_threshold_s=stall_threshold_s,
                runtime_status_reader=runtime_status_reader,
            )

        progress_snapshot_reader = _default_progress_snapshot_reader
    if launch_terminator is None:
        def _default_launch_terminator(*, result_ref: str | Path) -> None:
            terminate_runtime_owned_launch_result_ref(result_ref=result_ref)

        launch_terminator = _default_launch_terminator

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

        if (
            float(no_substantive_progress_window_s) > 0.0
            and bool(status.get("pid_alive"))
            and not bool(status.get("recovery_eligible"))
            and classification == "MISSING"
        ):
            snapshot = progress_snapshot_reader(
                result_ref=str(current_launch_result_ref),
                stall_threshold_s=stall_threshold_s,
            )
            observed_at = now_fn()
            if not _required_workspace_artifact_materialized(snapshot):
                if empty_required_artifact_started_at <= 0.0:
                    empty_required_artifact_started_at = observed_at
                elif (observed_at - empty_required_artifact_started_at) >= min(
                    float(no_substantive_progress_window_s),
                    _initial_required_artifact_window_s(
                        state_root=state_root,
                        node_id=node_id,
                        status=status,
                    ),
                ):
                    launch_terminator(result_ref=str(current_launch_result_ref))
                    status = runtime_status_reader(
                        result_ref=str(current_launch_result_ref),
                        stall_threshold_s=stall_threshold_s,
                    )
                    if bool(status.get("pid_alive")):
                        sleep_fn(max(0.05, float(poll_interval_s)))
                        continue
                    result = {
                        "launch_result_ref": str(_absolute(launch_result_ref)),
                        "latest_launch_result_ref": str(current_launch_result_ref),
                        "node_id": node_id,
                        "state_root": str(state_root),
                        "workspace_root": str(status.get("workspace_root") or ""),
                        "settled": True,
                        "settled_reason": "no_substantive_progress",
                        "recoveries_used": recoveries_used,
                        "implementer_result_ref": "",
                        "implementer_outcome": "",
                        "evaluator_verdict": "",
                        "status_result_ref": str(status.get("status_result_ref") or ""),
                        "history": history,
                    }
                    validate_repo_object("LoopChildSupervisionResult.schema.json", result)
                    return result
            else:
                empty_required_artifact_started_at = 0.0
            if _placeholder_only_progress_snapshot(snapshot):
                snapshot_fingerprint = _progress_snapshot_fingerprint(snapshot)
                if snapshot_fingerprint != placeholder_progress_fingerprint:
                    placeholder_progress_fingerprint = snapshot_fingerprint
                    placeholder_progress_started_at = observed_at
                elif (observed_at - placeholder_progress_started_at) >= float(no_substantive_progress_window_s):
                    launch_terminator(result_ref=str(current_launch_result_ref))
                    status = runtime_status_reader(
                        result_ref=str(current_launch_result_ref),
                        stall_threshold_s=stall_threshold_s,
                    )
                    if bool(status.get("pid_alive")):
                        sleep_fn(max(0.05, float(poll_interval_s)))
                        continue
                    result = {
                        "launch_result_ref": str(_absolute(launch_result_ref)),
                        "latest_launch_result_ref": str(current_launch_result_ref),
                        "node_id": node_id,
                        "state_root": str(state_root),
                        "workspace_root": str(status.get("workspace_root") or ""),
                        "settled": True,
                        "settled_reason": "no_substantive_progress",
                        "recoveries_used": recoveries_used,
                        "implementer_result_ref": "",
                        "implementer_outcome": "",
                        "evaluator_verdict": "",
                        "status_result_ref": str(status.get("status_result_ref") or ""),
                        "history": history,
                    }
                    validate_repo_object("LoopChildSupervisionResult.schema.json", result)
                    return result
            else:
                placeholder_progress_fingerprint = ""
                placeholder_progress_started_at = 0.0
        else:
            placeholder_progress_fingerprint = ""
            placeholder_progress_started_at = 0.0
            empty_required_artifact_started_at = 0.0

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
        authoritative_terminal_stopped = (
            classification == "MISSING"
            and result_ref.exists()
            and not bool(status.get("pid_alive"))
            and str(status.get("lifecycle_status") or "") in {"BLOCKED", "COMPLETED", "FAILED"}
            and str(status.get("runtime_attachment_state") or "") == "TERMINAL"
        )
        split_continuation_stopped = (
            classification == "MISSING"
            and result_ref.exists()
            and not bool(status.get("pid_alive"))
            and _is_split_continuation_result(result_payload or {})
        )
        incomplete_terminal_stopped = (
            classification == "MISSING"
            and result_ref.exists()
            and not bool(status.get("pid_alive"))
            and not _is_split_continuation_result(result_payload or {})
        )
        if authoritative_terminal_stopped or split_continuation_stopped:
            result = {
                "launch_result_ref": str(_absolute(launch_result_ref)),
                "latest_launch_result_ref": str(current_launch_result_ref),
                "node_id": node_id,
                "state_root": str(state_root),
                "workspace_root": str(status.get("workspace_root") or ""),
                "settled": True,
                "settled_reason": "authoritative_result",
                "recoveries_used": recoveries_used,
                "implementer_result_ref": str(result_ref),
                "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
                "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
                "status_result_ref": str(status.get("status_result_ref") or ""),
                "history": history,
            }
            validate_repo_object("LoopChildSupervisionResult.schema.json", result)
            return result
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
            placeholder_progress_fingerprint = ""
            placeholder_progress_started_at = 0.0
            empty_required_artifact_started_at = 0.0
            continue

        if max_wall_clock_s > 0 and (now_fn() - started_at) >= max_wall_clock_s:
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
