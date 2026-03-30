"""Committed root-side supervision loop for launched child nodes."""

from __future__ import annotations

import json
import hashlib
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from loop_product.artifact_hygiene import artifact_fingerprint
from loop_product.dispatch.child_progress_snapshot import child_progress_snapshot_from_launch_result_ref
from loop_product.dispatch.launch_runtime import terminate_runtime_owned_launch_result_ref
from loop_product.evaluator_authority import authoritative_result_conflicts_with_inflight_evaluator
from loop_product.kernel.state import query_kernel_state_object
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime.cleanup_authority import runtime_cleanup_retirement_authority
from loop_product.runtime_identity import read_runtime_root_identity
from loop_product.runtime_paths import node_machine_handoff_ref

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
_DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S = 300.0
_NO_SUBSTANTIVE_PROGRESS_WINDOW_BY_BUDGET_S = {
    "xhigh": 1800.0,
    "high": 450.0,
    "medium": 300.0,
    "low": 180.0,
}
_NONACTIVE_LIFECYCLE_STATUSES = {"BLOCKED", "COMPLETED", "FAILED"}


class RuntimeContextMissingError(RuntimeError):
    """Raised when cleanup authority retires a supervision loop."""

    def __init__(self, launch_result_ref: str | Path, *, cleanup_authority: Mapping[str, Any] | None = None):
        launch_ref = _absolute(launch_result_ref)
        authority = dict(cleanup_authority or {})
        authority_type = str(authority.get("event_type") or "").strip()
        authority_event_id = str(authority.get("event_id") or "").strip()
        if authority_type or authority_event_id:
            detail = authority_type or "cleanup_authority"
            if authority_event_id:
                detail = f"{detail} ({authority_event_id})"
            message = f"runtime context retired for {launch_ref} via {detail}"
        else:
            message = f"runtime context missing for {launch_ref}"
        super().__init__(message)
        self.launch_result_ref = launch_ref
        self.cleanup_authority = authority


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
    node_payload = _load_optional_json((state_root / "state" / f"{node_id}.json").resolve())
    if not node_payload:
        try:
            node_payload = dict(query_kernel_state_object(state_root, continue_deferred=False).nodes.get(node_id) or {})
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


def _load_frozen_handoff(*, state_root: Path, node_id: str, workspace_root: Path) -> dict[str, Any]:
    handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id=node_id)
    if handoff_ref.exists():
        return _load_optional_json(handoff_ref)
    return _load_optional_json((workspace_root / "FROZEN_HANDOFF.json").resolve())


def _artifact_root_candidates(*, workspace_root: Path, handoff: Mapping[str, Any]) -> list[Path]:
    candidates: list[Path] = []
    for artifact_ref in (
        str(handoff.get("workspace_live_artifact_ref") or "").strip(),
        str(handoff.get("workspace_mirror_ref") or "").strip(),
    ):
        if artifact_ref:
            candidates.append(_absolute(artifact_ref))
    for artifact_relpath in (
        str(handoff.get("workspace_live_artifact_relpath") or "").strip(),
        str(handoff.get("workspace_mirror_relpath") or "").strip(),
    ):
        if artifact_relpath:
            candidates.append((workspace_root / artifact_relpath).resolve())
    deduped: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


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
    state_root_raw = str(snapshot.get("state_root") or "").strip()
    node_id = str(snapshot.get("node_id") or "").strip()
    if not state_root_raw or not node_id:
        return False
    handoff = _load_frozen_handoff(
        state_root=_absolute(state_root_raw),
        node_id=node_id,
        workspace_root=workspace_root,
    )
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
    startup_required_output_paths = [
        str(item).strip() for item in list(handoff.get("startup_required_output_paths") or []) if str(item).strip()
    ]
    artifact_root_candidates = _artifact_root_candidates(workspace_root=workspace_root, handoff=handoff)
    if startup_required_output_paths:
        for relpath in startup_required_output_paths:
            if not any(_path_has_nonempty_file((root / relpath).resolve()) for root in artifact_root_candidates):
                return False
        return True
    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if _path_has_nonempty_file(candidate):
            return True
    return False


def _progress_checkpoint_context(snapshot: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[Path]]:
    workspace_root_raw = str(snapshot.get("workspace_root") or "").strip()
    state_root_raw = str(snapshot.get("state_root") or "").strip()
    node_id = str(snapshot.get("node_id") or "").strip()
    if not workspace_root_raw or not state_root_raw or not node_id:
        return [], []
    workspace_root = _absolute(workspace_root_raw)
    handoff = _load_frozen_handoff(
        state_root=_absolute(state_root_raw),
        node_id=node_id,
        workspace_root=workspace_root,
    )
    checkpoints = [dict(item) for item in list(handoff.get("progress_checkpoints") or []) if isinstance(item, dict)]
    roots = _artifact_root_candidates(workspace_root=workspace_root, handoff=handoff)
    return checkpoints, roots


def _progress_checkpoint_satisfied(checkpoint: Mapping[str, Any], artifact_roots: list[Path]) -> bool:
    required_any_of = [str(item).strip() for item in list(checkpoint.get("required_any_of") or []) if str(item).strip()]
    if not required_any_of:
        return True
    for relpath in required_any_of:
        if any(_path_has_nonempty_file((root / relpath).resolve()) for root in artifact_roots):
            return True
    return False


def _current_incomplete_progress_checkpoint(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    checkpoints, artifact_roots = _progress_checkpoint_context(snapshot)
    if not checkpoints:
        return None
    for checkpoint in checkpoints:
        if not _progress_checkpoint_satisfied(checkpoint, artifact_roots):
            return checkpoint
    return None


def _artifact_tree_progress_fingerprint(snapshot: Mapping[str, Any]) -> str:
    workspace_root_raw = str(snapshot.get("workspace_root") or "").strip()
    if not workspace_root_raw:
        return ""
    workspace_root = _absolute(workspace_root_raw)
    state_root_raw = str(snapshot.get("state_root") or "").strip()
    node_id = str(snapshot.get("node_id") or "").strip()
    if not state_root_raw or not node_id:
        return ""
    handoff = _load_frozen_handoff(
        state_root=_absolute(state_root_raw),
        node_id=node_id,
        workspace_root=workspace_root,
    )
    digest = hashlib.sha256()
    observed = False
    for root in _artifact_root_candidates(workspace_root=workspace_root, handoff=handoff):
        if not root.exists():
            continue
        observed = True
        fingerprint = artifact_fingerprint(root, ignore_runtime_heavy=True)
        digest.update(str(root).encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(fingerprint.get("artifact_kind") or "").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(fingerprint.get("fingerprint") or "").encode("utf-8"))
        digest.update(b"\0")
    if not observed:
        return ""
    return digest.hexdigest()


def _launch_context_from_launch_result_ref(launch_result_ref: Path) -> dict[str, str]:
    payload = _load_optional_json(launch_result_ref)
    node_id = str(payload.get("node_id") or "").strip()
    state_root = str(payload.get("state_root") or "").strip()
    workspace_root = str(payload.get("workspace_root") or "").strip()
    if node_id and state_root and workspace_root:
        return {
            "node_id": node_id,
            "state_root": state_root,
            "workspace_root": workspace_root,
        }

    launch_ref = _absolute(launch_result_ref)
    derived_repo_root, runtime_name = _repo_root_and_runtime_name_from_launch_result_ref(launch_ref)
    if not node_id:
        parents = launch_ref.parents
        if len(parents) >= 3 and parents[2].name == "launches":
            node_id = str(parents[1].name or "").strip()
    if derived_repo_root is not None:
        if not state_root and runtime_name:
            if runtime_name == ".loop":
                state_root = str((derived_repo_root / ".loop").resolve())
            else:
                state_root = str((derived_repo_root / ".loop" / runtime_name).resolve())
        if not workspace_root and runtime_name:
            if runtime_name == ".loop":
                workspace_root = str((derived_repo_root / "workspace").resolve())
            else:
                workspace_root = str((derived_repo_root / "workspace" / runtime_name).resolve())
    return {
        "node_id": node_id,
        "state_root": state_root,
        "workspace_root": workspace_root,
    }


def _runtime_context_missing(launch_result_ref: Path) -> bool:
    launch_ref = _absolute(launch_result_ref)
    if not launch_ref.exists():
        return True
    if not launch_ref.parent.exists():
        return True
    launch_context = _launch_context_from_launch_result_ref(launch_ref)
    state_root_raw = str(launch_context.get("state_root") or "").strip()
    if state_root_raw:
        try:
            if not _absolute(state_root_raw).exists():
                return True
        except Exception:
            return True
    workspace_root_raw = str(launch_context.get("workspace_root") or "").strip()
    if workspace_root_raw:
        try:
            if not _absolute(workspace_root_raw).exists():
                return True
        except Exception:
            return True
    return False


def _repo_root_and_runtime_name_from_launch_result_ref(launch_result_ref: Path) -> tuple[Path | None, str]:
    launch_ref = _absolute(launch_result_ref)
    parts = launch_ref.parts
    for idx, part in enumerate(parts):
        if part != ".loop":
            continue
        if idx + 1 >= len(parts):
            return None, ""
        repo_root = Path(*parts[:idx]) if idx > 0 else Path(launch_ref.anchor)
        next_part = str(parts[idx + 1] or "").strip()
        if next_part in {"artifacts", "state"}:
            return repo_root.expanduser().resolve(), ".loop"
        return repo_root.expanduser().resolve(), next_part
    return None, ""


def _cleanup_retirement_authority(launch_result_ref: Path) -> dict[str, Any] | None:
    repo_root, runtime_name = _repo_root_and_runtime_name_from_launch_result_ref(launch_result_ref)
    if repo_root is None or not runtime_name:
        return None
    if runtime_name == ".loop":
        state_root = str((repo_root / ".loop").resolve())
    else:
        state_root = str((repo_root / ".loop" / runtime_name).resolve())
    expected_runtime_identity_token = ""
    runtime_ref = Path(launch_result_ref).expanduser().resolve().with_name("ChildSupervisionRuntime.json")
    if runtime_ref.exists():
        expected_runtime_identity_token = str(_load_optional_json(runtime_ref).get("runtime_root_identity_token") or "")
    if not expected_runtime_identity_token:
        launch_payload = _load_optional_json(launch_result_ref)
        expected_runtime_identity_token = str(launch_payload.get("runtime_root_identity_token") or "")
    if not expected_runtime_identity_token:
        expected_runtime_identity_token = str(
            read_runtime_root_identity(Path(state_root)).get("runtime_identity_token") or ""
        )
    return runtime_cleanup_retirement_authority(
        state_root=state_root,
        repo_root=repo_root,
        expected_runtime_identity_token=expected_runtime_identity_token,
    )


def _numeric_attempt_suffix(launch_result_ref: Path) -> int | None:
    attempt_dir = launch_result_ref.parent.name
    if not attempt_dir.startswith("attempt_"):
        return None
    suffix = attempt_dir[len("attempt_") :]
    if not suffix.isdigit():
        return None
    return int(suffix)


def _newer_numeric_attempt_ref(
    *,
    current_launch_result_ref: Path,
    state_root: Path,
    node_id: str,
    workspace_root: Path,
) -> Path | None:
    current_attempt = _numeric_attempt_suffix(current_launch_result_ref)
    if current_attempt is None:
        return None
    launch_root = state_root / "artifacts" / "launches" / node_id
    if not launch_root.exists():
        return None
    candidates: list[tuple[int, Path]] = []
    for result_path in launch_root.glob("attempt_*/ChildLaunchResult.json"):
        attempt_num = _numeric_attempt_suffix(result_path)
        if attempt_num is None or attempt_num <= current_attempt:
            continue
        payload = _load_optional_json(result_path)
        if not payload:
            continue
        if str(payload.get("launch_decision") or "").strip() not in {"started", "started_existing", "exited", "retry_exhausted"}:
            continue
        runtime_ref = result_path.with_name("ChildSupervisionRuntime.json")
        settled_ref = result_path.with_name("ChildSupervisionResult.json")
        if not runtime_ref.exists() and not settled_ref.exists():
            continue
        if Path(str(payload.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
            continue
        if Path(str(payload.get("state_root") or "")).expanduser().resolve() != state_root:
            continue
        if str(payload.get("node_id") or "").strip() != node_id:
            continue
        candidates.append((attempt_num, result_path.resolve()))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _build_timeout_result(
    *,
    launch_result_ref: str | Path,
    current_launch_result_ref: Path,
    status: Mapping[str, Any] | None,
    result_ref: Path | None,
    result_payload: Mapping[str, Any] | None,
    recoveries_used: int,
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    launch_context = _launch_context_from_launch_result_ref(current_launch_result_ref)
    node_id = str((status or {}).get("node_id") or launch_context.get("node_id") or "")
    state_root = str((status or {}).get("state_root") or launch_context.get("state_root") or "")
    workspace_root = str((status or {}).get("workspace_root") or launch_context.get("workspace_root") or "")
    payload = {
        "launch_result_ref": str(_absolute(launch_result_ref)),
        "latest_launch_result_ref": str(current_launch_result_ref),
        "node_id": node_id,
        "state_root": state_root,
        "workspace_root": workspace_root,
        "settled": False,
        "settled_reason": "timeout",
        "recoveries_used": recoveries_used,
        "implementer_result_ref": str(result_ref) if result_ref is not None and result_ref.exists() else "",
        "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
        "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
        "status_result_ref": str((status or {}).get("status_result_ref") or ""),
        "history": history,
    }
    validate_repo_object("LoopChildSupervisionResult.schema.json", payload)
    return payload


def _build_settled_result(
    *,
    launch_result_ref: str | Path,
    current_launch_result_ref: Path,
    settled_reason: str,
    status: Mapping[str, Any],
    result_ref: Path | None,
    result_payload: Mapping[str, Any] | None,
    recoveries_used: int,
    history: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "launch_result_ref": str(_absolute(launch_result_ref)),
        "latest_launch_result_ref": str(current_launch_result_ref),
        "node_id": str(status.get("node_id") or ""),
        "state_root": str(status.get("state_root") or ""),
        "workspace_root": str(status.get("workspace_root") or ""),
        "settled": True,
        "settled_reason": settled_reason,
        "recoveries_used": recoveries_used,
        "implementer_result_ref": str(result_ref) if result_ref is not None and result_ref.exists() else "",
        "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
        "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
        "status_result_ref": str(status.get("status_result_ref") or ""),
        "history": history,
    }
    validate_repo_object("LoopChildSupervisionResult.schema.json", payload)
    return payload


def _authoritative_terminal_fast_path(
    *,
    launch_result_ref: Path,
) -> dict[str, Any] | None:
    launch_context = _launch_context_from_launch_result_ref(launch_result_ref)
    node_id = str(launch_context.get("node_id") or "").strip()
    state_root_raw = str(launch_context.get("state_root") or "").strip()
    workspace_root = str(launch_context.get("workspace_root") or "").strip()
    if not node_id or not state_root_raw:
        return None
    state_root = _absolute(state_root_raw)
    result_ref = _authoritative_result_ref(state_root=state_root, node_id=node_id)
    if not result_ref.exists():
        return None
    result_payload = _load_optional_json(result_ref)
    if not result_payload:
        return None
    if authoritative_result_conflicts_with_inflight_evaluator(
        state_root=state_root,
        node_id=node_id,
        payload=result_payload,
    ):
        return None
    classification = _classify_terminal_result(result_payload)
    if classification == "PASS":
        settled_reason = "pass"
    elif classification == "TERMINAL_NONRETRYABLE":
        settled_reason = "terminal_nonretryable"
    else:
        return None
    return {
        "classification": classification,
        "settled_reason": settled_reason,
        "status": {
            "launch_result_ref": str(launch_result_ref),
            "node_id": node_id,
            "state_root": str(state_root),
            "workspace_root": workspace_root,
            "status_result_ref": "",
            "pid_alive": False,
            "recovery_eligible": False,
            "runtime_attachment_state": "TERMINAL",
            "lifecycle_status": "COMPLETED",
        },
        "result_ref": result_ref,
        "result_payload": result_payload,
    }


def _status_left_active_nonrecoverable(status: Mapping[str, Any]) -> bool:
    lifecycle_status = str(status.get("lifecycle_status") or "").strip().upper()
    return (
        lifecycle_status in _NONACTIVE_LIFECYCLE_STATUSES
        and not bool(status.get("pid_alive"))
        and not bool(status.get("recovery_eligible"))
    )


def _startup_reasoning_budget(*, state_root: Path, node_id: str, status: Mapping[str, Any]) -> str:
    try:
        node_payload = dict(query_kernel_state_object(state_root, continue_deferred=False).nodes.get(node_id) or {})
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


def _effective_no_substantive_progress_window_s(
    *,
    state_root: Path,
    node_id: str,
    status: Mapping[str, Any],
    configured_window_s: float,
    apply_reasoning_budget_floor: bool,
) -> float:
    configured_window = float(configured_window_s)
    if configured_window <= 0.0:
        return 0.0
    if not apply_reasoning_budget_floor:
        return configured_window
    budget = _startup_reasoning_budget(state_root=state_root, node_id=node_id, status=status)
    budget_floor = float(
        _NO_SUBSTANTIVE_PROGRESS_WINDOW_BY_BUDGET_S.get(
            budget,
            _DEFAULT_NO_SUBSTANTIVE_PROGRESS_WINDOW_S,
        )
    )
    return max(configured_window, budget_floor)


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


def _snapshot_has_machine_progress_exemption(snapshot: Mapping[str, Any]) -> bool:
    if bool(snapshot.get("terminal_result_present")):
        return True
    if str(snapshot.get("implementer_result_ref") or "").strip():
        return True
    if str(snapshot.get("implementer_outcome") or "").strip():
        return True
    if str(snapshot.get("implementer_verdict") or "").strip():
        return True
    if str(snapshot.get("evaluator_status") or "").strip():
        return True
    for key in (
        "evaluator_running_unit_ids",
        "evaluator_completed_unit_ids",
        "evaluator_blocked_retryable_unit_ids",
        "evaluator_blocked_terminal_unit_ids",
        "evaluator_pending_unit_ids",
    ):
        if list(snapshot.get(key) or []):
            return True
    return False


def _checkpoint_activity_fingerprint(snapshot: Mapping[str, Any]) -> str:
    if not _placeholder_only_progress_snapshot(snapshot):
        return _progress_snapshot_fingerprint(snapshot)
    return ""


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
    status_observer: Callable[[dict[str, Any]], None] | None = None,
    no_substantive_progress_window_s: float = 300.0,
    apply_reasoning_budget_floor: bool = True,
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
    artifact_progress_fingerprint = ""
    artifact_progress_last_activity_at = 0.0
    artifact_activity_fingerprint = ""
    checkpoint_id = ""
    checkpoint_last_activity_at = 0.0
    checkpoint_activity_fingerprint = ""
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

    def _read_status(result_ref: Path) -> dict[str, Any]:
        cleanup_authority = _cleanup_retirement_authority(result_ref)
        if cleanup_authority is not None:
            raise RuntimeContextMissingError(result_ref, cleanup_authority=cleanup_authority)
        try:
            observed_status = runtime_status_reader(
                result_ref=str(result_ref),
                stall_threshold_s=stall_threshold_s,
            )
        except Exception as exc:
            if _runtime_context_missing(result_ref):
                cleanup_authority = _cleanup_retirement_authority(result_ref)
                if cleanup_authority is not None:
                    raise RuntimeContextMissingError(result_ref, cleanup_authority=cleanup_authority) from exc
            raise
        if status_observer is not None:
            try:
                status_observer(dict(observed_status or {}))
            except Exception as exc:
                print(
                    f"[child-supervision] warning: status observer failed for {result_ref}: {exc}",
                    flush=True,
                )
        return observed_status

    while True:
        fast_path = _authoritative_terminal_fast_path(
            launch_result_ref=current_launch_result_ref,
        )
        if fast_path is not None:
            history.append(
                {
                    "launch_result_ref": str(current_launch_result_ref),
                    "status_result_ref": "",
                    "pid_alive": False,
                    "recovery_eligible": False,
                    "result_ref": str(fast_path["result_ref"]),
                    "result_classification": str(fast_path["classification"]),
                }
            )
            return _build_settled_result(
                launch_result_ref=launch_result_ref,
                current_launch_result_ref=current_launch_result_ref,
                settled_reason=str(fast_path["settled_reason"]),
                status=dict(fast_path["status"]),
                result_ref=Path(str(fast_path["result_ref"])),
                result_payload=dict(fast_path["result_payload"]),
                recoveries_used=recoveries_used,
                history=history,
            )
        try:
            status = _read_status(current_launch_result_ref)
        except RuntimeContextMissingError:
            raise
        except Exception as exc:
            print(
                f"[child-supervision] warning: runtime status observation failed for {current_launch_result_ref}: {exc}",
                flush=True,
            )
            if max_wall_clock_s > 0 and (now_fn() - started_at) >= max_wall_clock_s:
                return _build_timeout_result(
                    launch_result_ref=launch_result_ref,
                    current_launch_result_ref=current_launch_result_ref,
                    status=None,
                    result_ref=None,
                    result_payload=None,
                    recoveries_used=recoveries_used,
                    history=history,
                )
            sleep_fn(max(0.05, float(poll_interval_s)))
            continue
        state_root = _absolute(str(status.get("state_root") or ""))
        node_id = str(status.get("node_id") or "").strip()
        result_ref = _authoritative_result_ref(state_root=state_root, node_id=node_id)
        result_payload = _load_json(result_ref) if result_ref.exists() else None
        if result_payload and authoritative_result_conflicts_with_inflight_evaluator(
            state_root=state_root,
            node_id=node_id,
            payload=result_payload,
        ):
            result_payload = None
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
        newer_attempt_ref = _newer_numeric_attempt_ref(
            current_launch_result_ref=current_launch_result_ref,
            state_root=state_root,
            node_id=node_id,
            workspace_root=_absolute(str(status.get("workspace_root") or "")),
        )
        if newer_attempt_ref is not None:
            result = {
                "launch_result_ref": str(_absolute(launch_result_ref)),
                "latest_launch_result_ref": str(newer_attempt_ref),
                "node_id": node_id,
                "state_root": str(state_root),
                "workspace_root": str(status.get("workspace_root") or ""),
                "settled": True,
                "settled_reason": "superseded_attempt",
                "recoveries_used": recoveries_used,
                "implementer_result_ref": str(result_ref) if result_ref.exists() else "",
                "implementer_outcome": str((result_payload or {}).get("outcome") or ""),
                "evaluator_verdict": str(((result_payload or {}).get("evaluator_result") or {}).get("verdict") or ""),
                "status_result_ref": str(status.get("status_result_ref") or ""),
                "history": history,
            }
            validate_repo_object("LoopChildSupervisionResult.schema.json", result)
            return result

        if (
            float(no_substantive_progress_window_s) > 0.0
            and bool(status.get("pid_alive"))
            and not bool(status.get("recovery_eligible"))
            and classification == "MISSING"
        ):
            no_progress_window_s = _effective_no_substantive_progress_window_s(
                state_root=state_root,
                node_id=node_id,
                status=status,
                configured_window_s=no_substantive_progress_window_s,
                apply_reasoning_budget_floor=apply_reasoning_budget_floor,
            )
            try:
                snapshot = progress_snapshot_reader(
                    result_ref=str(current_launch_result_ref),
                    stall_threshold_s=stall_threshold_s,
                )
            except Exception as exc:
                print(
                    f"[child-supervision] warning: progress snapshot failed for {current_launch_result_ref}: {exc}",
                    flush=True,
                )
                if max_wall_clock_s > 0 and (now_fn() - started_at) >= max_wall_clock_s:
                    return _build_timeout_result(
                        launch_result_ref=launch_result_ref,
                        current_launch_result_ref=current_launch_result_ref,
                        status=status,
                        result_ref=result_ref,
                        result_payload=result_payload,
                        recoveries_used=recoveries_used,
                        history=history,
                    )
                sleep_fn(max(0.05, float(poll_interval_s)))
                continue
            observed_at = now_fn()
            if not _required_workspace_artifact_materialized(snapshot):
                if empty_required_artifact_started_at <= 0.0:
                    empty_required_artifact_started_at = observed_at
                elif (observed_at - empty_required_artifact_started_at) >= min(
                    no_progress_window_s,
                    _initial_required_artifact_window_s(
                        state_root=state_root,
                        node_id=node_id,
                        status=status,
                    ),
                ):
                    launch_terminator(result_ref=str(current_launch_result_ref))
                    status = _read_status(current_launch_result_ref)
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
                machine_progress_exempt = _snapshot_has_machine_progress_exemption(snapshot)
                current_checkpoint = _current_incomplete_progress_checkpoint(snapshot) if not machine_progress_exempt else None
                if current_checkpoint is not None:
                    current_checkpoint_id = str(current_checkpoint.get("checkpoint_id") or "").strip()
                    current_activity_fingerprint = _checkpoint_activity_fingerprint(snapshot)
                    if current_checkpoint_id != checkpoint_id:
                        checkpoint_id = current_checkpoint_id
                        checkpoint_last_activity_at = observed_at
                        checkpoint_activity_fingerprint = current_activity_fingerprint
                    else:
                        if current_activity_fingerprint and current_activity_fingerprint != checkpoint_activity_fingerprint:
                            checkpoint_activity_fingerprint = current_activity_fingerprint
                            checkpoint_last_activity_at = observed_at
                        elif checkpoint_last_activity_at <= 0.0:
                            checkpoint_last_activity_at = observed_at
                    if (
                        checkpoint_last_activity_at > 0.0
                        and no_progress_window_s > 0.0
                        and (observed_at - checkpoint_last_activity_at) >= no_progress_window_s
                    ):
                        launch_terminator(result_ref=str(current_launch_result_ref))
                        status = _read_status(current_launch_result_ref)
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
                    checkpoint_id = ""
                    checkpoint_last_activity_at = 0.0
                    checkpoint_activity_fingerprint = ""
                if current_checkpoint is None and not machine_progress_exempt:
                    artifact_tree_fingerprint = _artifact_tree_progress_fingerprint(snapshot)
                    current_artifact_activity_fingerprint = _progress_snapshot_fingerprint(snapshot)
                    if artifact_tree_fingerprint:
                        if artifact_tree_fingerprint != artifact_progress_fingerprint:
                            artifact_progress_fingerprint = artifact_tree_fingerprint
                            artifact_progress_last_activity_at = observed_at
                            artifact_activity_fingerprint = current_artifact_activity_fingerprint
                        elif (
                            current_artifact_activity_fingerprint
                            and current_artifact_activity_fingerprint != artifact_activity_fingerprint
                        ):
                            artifact_activity_fingerprint = current_artifact_activity_fingerprint
                            artifact_progress_last_activity_at = observed_at
                        elif artifact_progress_last_activity_at <= 0.0:
                            artifact_progress_last_activity_at = observed_at
                        elif (
                            no_progress_window_s > 0.0
                            and (observed_at - artifact_progress_last_activity_at) >= no_progress_window_s
                        ):
                            launch_terminator(result_ref=str(current_launch_result_ref))
                            status = _read_status(current_launch_result_ref)
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
                        artifact_progress_fingerprint = ""
                        artifact_progress_last_activity_at = 0.0
                        artifact_activity_fingerprint = ""
                else:
                    artifact_progress_fingerprint = ""
                    artifact_progress_last_activity_at = 0.0
                    artifact_activity_fingerprint = ""
                if current_checkpoint is not None:
                    artifact_progress_fingerprint = ""
                    artifact_progress_last_activity_at = 0.0
                    artifact_activity_fingerprint = ""
            if _placeholder_only_progress_snapshot(snapshot):
                snapshot_fingerprint = _progress_snapshot_fingerprint(snapshot)
                if snapshot_fingerprint != placeholder_progress_fingerprint:
                    placeholder_progress_fingerprint = snapshot_fingerprint
                    placeholder_progress_started_at = observed_at
                elif (observed_at - placeholder_progress_started_at) >= no_progress_window_s:
                    launch_terminator(result_ref=str(current_launch_result_ref))
                    status = _read_status(current_launch_result_ref)
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
            artifact_progress_fingerprint = ""
            artifact_progress_last_activity_at = 0.0
            artifact_activity_fingerprint = ""
            checkpoint_id = ""
            checkpoint_last_activity_at = 0.0
            checkpoint_activity_fingerprint = ""

        if classification == "PASS":
            return _build_settled_result(
                launch_result_ref=launch_result_ref,
                current_launch_result_ref=current_launch_result_ref,
                settled_reason="pass",
                status=status,
                result_ref=result_ref,
                result_payload=result_payload,
                recoveries_used=recoveries_used,
                history=history,
            )

        if classification == "TERMINAL_NONRETRYABLE":
            return _build_settled_result(
                launch_result_ref=launch_result_ref,
                current_launch_result_ref=current_launch_result_ref,
                settled_reason="terminal_nonretryable",
                status=status,
                result_ref=result_ref,
                result_payload=result_payload,
                recoveries_used=recoveries_used,
                history=history,
            )

        retryable_terminal_stopped = classification == "RETRYABLE_TERMINAL" and not bool(status.get("pid_alive"))
        nonrecoverable_nonactive_stopped = _status_left_active_nonrecoverable(status)
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
        if (
            (nonrecoverable_nonactive_stopped and result_ref.exists() and classification != "MISSING")
            or authoritative_terminal_stopped
            or split_continuation_stopped
        ):
            return _build_settled_result(
                launch_result_ref=launch_result_ref,
                current_launch_result_ref=current_launch_result_ref,
                settled_reason="authoritative_result",
                status=status,
                result_ref=result_ref,
                result_payload=result_payload,
                recoveries_used=recoveries_used,
                history=history,
            )
        if nonrecoverable_nonactive_stopped and not result_ref.exists():
            return _build_settled_result(
                launch_result_ref=launch_result_ref,
                current_launch_result_ref=current_launch_result_ref,
                settled_reason="retry_budget_exhausted",
                status=status,
                result_ref=result_ref,
                result_payload=result_payload,
                recoveries_used=recoveries_used,
                history=history,
            )
        runtime_loss_without_result = classification == "MISSING" and (
            bool(status.get("recovery_eligible")) or incomplete_terminal_stopped
        )
        if retryable_terminal_stopped or runtime_loss_without_result:
            if recoveries_used >= max(0, int(max_recoveries)):
                return _build_settled_result(
                    launch_result_ref=launch_result_ref,
                    current_launch_result_ref=current_launch_result_ref,
                    settled_reason="retry_budget_exhausted",
                    status=status,
                    result_ref=result_ref,
                    result_payload=result_payload,
                    recoveries_used=recoveries_used,
                    history=history,
                )
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
            try:
                recovery_result = recovery_runner(**recovery_payload)
            except Exception as exc:
                print(
                    f"[child-supervision] warning: recovery step failed for {current_launch_result_ref}: {exc}",
                    flush=True,
                )
                if max_wall_clock_s > 0 and (now_fn() - started_at) >= max_wall_clock_s:
                    return _build_timeout_result(
                        launch_result_ref=launch_result_ref,
                        current_launch_result_ref=current_launch_result_ref,
                        status=status,
                        result_ref=result_ref,
                        result_payload=result_payload,
                        recoveries_used=recoveries_used,
                        history=history,
                    )
                sleep_fn(max(0.05, float(poll_interval_s)))
                continue
            try:
                relaunched = launcher(result_ref=str(recovery_result["recovery_result_ref"]))
            except Exception as exc:
                print(
                    f"[child-supervision] warning: relaunch step failed for {current_launch_result_ref}: {exc}",
                    flush=True,
                )
                if max_wall_clock_s > 0 and (now_fn() - started_at) >= max_wall_clock_s:
                    return _build_timeout_result(
                        launch_result_ref=launch_result_ref,
                        current_launch_result_ref=current_launch_result_ref,
                        status=status,
                        result_ref=result_ref,
                        result_payload=result_payload,
                        recoveries_used=recoveries_used,
                        history=history,
                    )
                sleep_fn(max(0.05, float(poll_interval_s)))
                continue
            current_launch_result_ref = _absolute(str(relaunched["launch_result_ref"]))
            recoveries_used += 1
            placeholder_progress_fingerprint = ""
            placeholder_progress_started_at = 0.0
            empty_required_artifact_started_at = 0.0
            artifact_progress_fingerprint = ""
            artifact_progress_started_at = 0.0
            checkpoint_id = ""
            checkpoint_last_activity_at = 0.0
            checkpoint_activity_fingerprint = ""
            continue

        if max_wall_clock_s > 0 and (now_fn() - started_at) >= max_wall_clock_s:
            return _build_timeout_result(
                launch_result_ref=launch_result_ref,
                current_launch_result_ref=current_launch_result_ref,
                status=status,
                result_ref=result_ref,
                result_payload=result_payload,
                recoveries_used=recoveries_used,
                history=history,
            )

        sleep_fn(max(0.05, float(poll_interval_s)))
