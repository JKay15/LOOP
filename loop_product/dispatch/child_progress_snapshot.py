"""Committed child progress snapshots for live root-side supervision."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Iterable

from loop_product.dispatch.launch_runtime import child_runtime_status_from_launch_result_ref
from loop_product.evaluator_authority import (
    authoritative_result_conflicts_with_inflight_evaluator,
    latest_evaluator_run_state_ref,
)
from loop_product.protocols.schema import validate_repo_object

_DOC_SUFFIXES = {".md", ".markdown", ".rst", ".txt", ".tex", ".pdf"}
_ABS_PATH_RE = re.compile(
    r"""
    (?:
        ['"](?P<quoted>/[^'"\n]+)['"]
      |
        (?P<bare>/[^\s"'()<>\[\]]+)
    )
    """,
    re.VERBOSE,
)


def _absolute(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _authoritative_result_ref(*, state_root: Path, node_id: str) -> Path:
    return (state_root / "artifacts" / node_id / "implementer_result.json").resolve()


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _recent_log_lines(*, log_path: Path, limit: int = 8) -> list[str]:
    text = _read_text(log_path)
    if not text:
        return []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[-max(1, int(limit)) :]


def _raw_path_fragments(raw: str) -> list[str]:
    text = raw.strip()
    if not text:
        return []
    if ":" not in text:
        return [text]
    fragments = [part.strip() for part in text.split(":") if part.strip().startswith("/")]
    return fragments or [text]


def _iter_observed_paths(*, texts: Iterable[str]) -> list[Path]:
    observed: list[Path] = []
    seen: set[Path] = set()
    for text in texts:
        if not text:
            continue
        for match in _ABS_PATH_RE.finditer(text):
            raw = match.group("quoted") or match.group("bare") or ""
            if not raw:
                continue
            for fragment in _raw_path_fragments(raw):
                candidate = Path(fragment).expanduser()
                try:
                    resolved = candidate.resolve()
                except Exception:
                    continue
                try:
                    exists = resolved.exists()
                except OSError:
                    continue
                if not exists:
                    continue
                if resolved in seen:
                    continue
                seen.add(resolved)
                observed.append(resolved)
    return observed


def _repo_root_from_state_root(state_root: Path) -> Path:
    resolved = state_root.resolve()
    if resolved.name == ".loop":
        return resolved.parent.resolve()
    for parent in resolved.parents:
        if parent.name == ".loop":
            return parent.parent.resolve()
    return resolved.parent.resolve()


def _split_doc_refs(*, state_root: Path, observed_paths: Iterable[Path]) -> tuple[list[str], list[str], list[str]]:
    state_root = state_root.resolve()
    repo_root = _repo_root_from_state_root(state_root)
    all_doc_refs: list[str] = []
    repo_doc_refs: list[str] = []
    external_doc_refs: list[str] = []
    for path in observed_paths:
        suffix = path.suffix.lower()
        if suffix not in _DOC_SUFFIXES:
            continue
        ref = str(path.resolve())
        all_doc_refs.append(ref)
        try:
            path.resolve().relative_to(repo_root)
        except ValueError:
            external_doc_refs.append(ref)
        else:
            repo_doc_refs.append(ref)
    return all_doc_refs, repo_doc_refs, external_doc_refs


def _evaluator_log_refs(*, run_state_ref: Path) -> list[Path]:
    run_root = run_state_ref.parent
    evaluator_loop_root = run_root / ".loop"
    if not evaluator_loop_root.exists():
        return []
    refs: list[Path] = []
    seen: set[Path] = set()
    for path in evaluator_loop_root.rglob("*"):
        if not path.is_file():
            continue
        name = path.name
        if name in {"prompt.md", "response.txt", "stdout.txt", "stderr.txt"} or name.endswith(".stdout.txt") or name.endswith(".stderr.txt"):
            resolved = path.resolve()
            if resolved in seen:
                continue
            seen.add(resolved)
            refs.append(resolved)
    return sorted(refs)


def _lane_status_ids(lanes_by_unit_id: dict[str, Any], status: str) -> list[str]:
    return sorted(
        unit_id
        for unit_id, lane in lanes_by_unit_id.items()
        if str(dict(lane or {}).get("status") or "") == status
    )


def _evaluator_summary(*, state_root: Path, node_id: str) -> dict[str, Any]:
    run_state_ref = latest_evaluator_run_state_ref(state_root=state_root, node_id=node_id)
    if run_state_ref is None:
        return {
            "evaluator_run_state_ref": "",
            "evaluator_status": "",
            "evaluator_checker_status": "",
            "evaluator_reviewer_status": "",
            "evaluator_completed_unit_ids": [],
            "evaluator_running_unit_ids": [],
            "evaluator_blocked_retryable_unit_ids": [],
            "evaluator_blocked_terminal_unit_ids": [],
            "evaluator_pending_unit_ids": [],
            "observed_evaluator_doc_source_refs": [],
            "observed_evaluator_doc_refs": [],
        }
    payload = _load_json(run_state_ref)
    lanes_by_unit_id = dict(payload.get("lanes_by_unit_id") or {})
    evaluator_log_refs = _evaluator_log_refs(run_state_ref=run_state_ref)
    evaluator_observed_paths = _iter_observed_paths(texts=[_read_text(path) for path in evaluator_log_refs])
    evaluator_doc_refs, _repo_doc_refs, _external_doc_refs = _split_doc_refs(
        state_root=state_root,
        observed_paths=evaluator_observed_paths,
    )
    return {
        "evaluator_run_state_ref": str(run_state_ref.resolve()),
        "evaluator_status": str(payload.get("status") or ""),
        "evaluator_checker_status": str(dict(payload.get("checker") or {}).get("status") or ""),
        "evaluator_reviewer_status": str(dict(payload.get("reviewer") or {}).get("status") or ""),
        "evaluator_completed_unit_ids": _lane_status_ids(lanes_by_unit_id, "COMPLETED"),
        "evaluator_running_unit_ids": _lane_status_ids(lanes_by_unit_id, "RUNNING"),
        "evaluator_blocked_retryable_unit_ids": _lane_status_ids(lanes_by_unit_id, "BLOCKED_RETRYABLE"),
        "evaluator_blocked_terminal_unit_ids": _lane_status_ids(lanes_by_unit_id, "BLOCKED_TERMINAL"),
        "evaluator_pending_unit_ids": _lane_status_ids(lanes_by_unit_id, "PENDING"),
        "observed_evaluator_doc_source_refs": [str(path.resolve()) for path in evaluator_log_refs],
        "observed_evaluator_doc_refs": evaluator_doc_refs,
    }


def _phase_hint(
    *,
    pid_alive: bool,
    recovery_eligible: bool,
    terminal_result_present: bool,
    evaluator_status: str,
    implementer_verdict: str,
    implementer_retryable: bool,
) -> str:
    status = evaluator_status.strip().upper()
    verdict = implementer_verdict.strip().upper()
    if recovery_eligible and not pid_alive:
        return "RUNTIME_RECOVERY"
    if terminal_result_present and verdict and verdict != "PASS" and implementer_retryable and pid_alive:
        return "REPAIRING"
    if status:
        return "EVALUATING"
    if terminal_result_present:
        return "REPORTED"
    return "IMPLEMENTING"


def child_progress_snapshot_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
    runtime_status_reader: Callable[..., dict[str, Any]] = child_runtime_status_from_launch_result_ref,
) -> dict[str, Any]:
    launch_result_ref = _absolute(result_ref)
    launch_payload = _load_json(launch_result_ref)
    status = runtime_status_reader(
        result_ref=str(launch_result_ref),
        stall_threshold_s=stall_threshold_s,
    )
    state_root = _absolute(str(status.get("state_root") or launch_payload.get("state_root") or ""))
    node_id = str(status.get("node_id") or launch_payload.get("node_id") or "").strip()
    workspace_root = str(_absolute(str(status.get("workspace_root") or launch_payload.get("workspace_root") or "")))
    latest_log_ref = _absolute(str(status.get("latest_log_ref") or status.get("stderr_ref") or launch_payload.get("stderr_ref") or ""))
    stdout_ref = _absolute(str(status.get("stdout_ref") or launch_payload.get("stdout_ref") or ""))
    stderr_ref = _absolute(str(status.get("stderr_ref") or launch_payload.get("stderr_ref") or ""))
    recent_lines = _recent_log_lines(log_path=latest_log_ref)
    observed_paths = _iter_observed_paths(texts=[_read_text(stdout_ref), _read_text(stderr_ref)])
    observed_doc_refs, observed_repo_doc_refs, observed_external_doc_refs = _split_doc_refs(
        state_root=state_root,
        observed_paths=observed_paths,
    )
    implementer_result_ref = _authoritative_result_ref(state_root=state_root, node_id=node_id)
    terminal_result_present = implementer_result_ref.exists()
    result_payload = _load_json(implementer_result_ref) if terminal_result_present else {}
    if terminal_result_present and authoritative_result_conflicts_with_inflight_evaluator(
        state_root=state_root,
        node_id=node_id,
        payload=result_payload,
    ):
        terminal_result_present = False
        result_payload = {}
    evaluator_summary = _evaluator_summary(state_root=state_root, node_id=node_id)
    implementer_evaluator = dict(result_payload.get("evaluator_result") or {})
    snapshot_ref = launch_result_ref.with_name("ChildProgressSnapshot.json")
    payload = {
        "version": "1",
        "snapshot_ref": str(snapshot_ref.resolve()),
        "launch_result_ref": str(launch_result_ref.resolve()),
        "runtime_status_ref": str(status.get("status_result_ref") or ""),
        "node_id": node_id,
        "workspace_root": workspace_root,
        "state_root": str(state_root),
        "pid_alive": bool(status.get("pid_alive")),
        "lifecycle_status": str(status.get("lifecycle_status") or ""),
        "runtime_attachment_state": str(status.get("runtime_attachment_state") or ""),
        "recovery_eligible": bool(status.get("recovery_eligible")),
        "recovery_reason": str(status.get("recovery_reason") or ""),
        "latest_log_ref": str(latest_log_ref.resolve()),
        "latest_log_age_s": float(status.get("latest_log_age_s") or 0.0),
        "recent_log_lines": recent_lines,
        "terminal_result_present": terminal_result_present,
        "implementer_result_ref": str(implementer_result_ref.resolve()) if terminal_result_present else "",
        "implementer_outcome": str(result_payload.get("outcome") or ""),
        "implementer_verdict": str(implementer_evaluator.get("verdict") or ""),
        "implementer_retryable": bool(implementer_evaluator.get("retryable")),
        "observed_doc_source_refs": [
            ref
            for ref in [str(stdout_ref.resolve()), str(stderr_ref.resolve())]
            if ref and Path(ref).exists()
        ],
        "observed_child_doc_refs": observed_doc_refs,
        "observed_doc_refs": observed_doc_refs,
        "observed_repo_doc_refs": observed_repo_doc_refs,
        "observed_external_doc_refs": observed_external_doc_refs,
        **evaluator_summary,
    }
    evaluator_doc_refs = list(payload.get("observed_evaluator_doc_refs") or [])
    combined_doc_refs = list(dict.fromkeys([*payload["observed_doc_refs"], *evaluator_doc_refs]))
    combined_repo_doc_refs = list(
        dict.fromkeys(
            [
                *payload["observed_repo_doc_refs"],
                *[
                    ref
                    for ref in evaluator_doc_refs
                    if ref.startswith(f"{_repo_root_from_state_root(state_root)}{Path('/').as_posix()[:-1]}")
                ],
            ]
        )
    )
    combined_external_doc_refs = list(dict.fromkeys([*payload["observed_external_doc_refs"], *[ref for ref in evaluator_doc_refs if ref not in combined_repo_doc_refs]]))
    payload["observed_doc_refs"] = combined_doc_refs
    payload["observed_repo_doc_refs"] = combined_repo_doc_refs
    payload["observed_external_doc_refs"] = combined_external_doc_refs
    payload["phase_hint"] = _phase_hint(
        pid_alive=bool(payload["pid_alive"]),
        recovery_eligible=bool(payload["recovery_eligible"]),
        terminal_result_present=bool(payload["terminal_result_present"]),
        evaluator_status=str(payload["evaluator_status"]),
        implementer_verdict=str(payload["implementer_verdict"]),
        implementer_retryable=bool(payload["implementer_retryable"]),
    )
    validate_repo_object("LoopChildProgressSnapshot.schema.json", payload)
    _write_json(snapshot_ref, payload)
    return payload
