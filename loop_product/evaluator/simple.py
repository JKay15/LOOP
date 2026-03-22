#!/usr/bin/env python3
"""Thin stdout-handoff evaluator implementation."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from . import prototype as _legacy
from . import role_launch as _role_launch
from . import run_state as _run_state
from ..agent_provider import apply_env_map, resolve_agent_invocation
from .agent_execution_policy import validate_repo_shipped_role_agent_cmd
from ..runtime_paths import product_repo_root, product_schema_path

_ROOT = product_repo_root()
_INPUT_SCHEMA_PATH = product_schema_path("LoopEvaluatorPrototypeInput.schema.json")
_SIMPLE_EVAL_GUARD_ENV = "LOOP_PRODUCT_EVAL_SIMPLE_DEPTH"
_STRUCTURED_ISSUE_RULES: tuple[tuple[str, tuple[str, ...], str, str], ...] = (
    (
        "uv",
        ("uv run --project", "virtual environment"),
        "evaluator_exec_uv",
        "retry with staged-workspace `python` or another already-confirmed repo-local command instead of `uv run`.",
    ),
    (
        "path",
        ("can't open file", "no such file or directory"),
        "evaluator_exec_path",
        "correct the path before retrying the evaluator lane.",
    ),
    (
        "repo_structure",
        ("pyproject.toml not found", "repo root", "workspace layout assumption"),
        "evaluator_exec_repo_structure",
        "confirm the repo structure before retrying from a different cwd or package root assumption.",
    ),
    (
        "command_sequence",
        ("parse error near", "generated command sequence"),
        "evaluator_exec_command_sequence",
        "repair the command sequence before retrying the evaluator lane.",
    ),
)


def _safe_slug(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(text or "").strip()).strip("._-") or "item"


def _load_request(request: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    if isinstance(request, Mapping):
        return dict(request)
    return _legacy._load_json(Path(request))


def _resolve_path(base: Path, raw: str) -> Path:
    path = Path(raw)
    return path.resolve() if path.is_absolute() else (base / path).resolve()


def _read_text(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _compact_detail(text: str, *, limit: int = 240) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def _classify_evaluator_side_issue(message: str) -> dict[str, str] | None:
    provider_issue = _legacy._classify_provider_issue_text(message)
    if provider_issue is not None:
        return provider_issue
    lowered = str(message or "").lower()
    for issue_kind, needles, self_attribution, self_repair in _STRUCTURED_ISSUE_RULES:
        if all(needle in lowered for needle in needles):
            return {
                "kind": "STRUCTURED_EXCEPTION",
                "issue_kind": issue_kind,
                "self_attribution": self_attribution,
                "self_repair": self_repair,
            }
    if "uv run --project" in lowered:
        issue_kind, _, self_attribution, self_repair = _STRUCTURED_ISSUE_RULES[0]
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": issue_kind,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
        }
    if "can't open file" in lowered or "no such file or directory" in lowered:
        issue_kind, _, self_attribution, self_repair = _STRUCTURED_ISSUE_RULES[1]
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": issue_kind,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
        }
    if "pyproject.toml not found" in lowered or "workspace layout assumption" in lowered:
        issue_kind, _, self_attribution, self_repair = _STRUCTURED_ISSUE_RULES[2]
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": issue_kind,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
        }
    if "parse error near" in lowered or "generated command sequence" in lowered:
        issue_kind, _, self_attribution, self_repair = _STRUCTURED_ISSUE_RULES[3]
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": issue_kind,
            "self_attribution": self_attribution,
            "self_repair": self_repair,
        }
    return None


def _simple_eval_guard_depth() -> int:
    raw = str(os.environ.get(_SIMPLE_EVAL_GUARD_ENV) or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 1


def _activate_self_eval_guard(request: Mapping[str, Any]) -> tuple[object | str | None, object | str | None]:
    current_depth = _simple_eval_guard_depth()
    if current_depth >= 1:
        raise ValueError(
            "simple evaluator recursion detected: delegated roles must not relaunch the thin evaluator from inside an active evaluator run"
        )
    previous_simple = os.environ.get(_SIMPLE_EVAL_GUARD_ENV)
    os.environ[_SIMPLE_EVAL_GUARD_ENV] = str(current_depth + 1)
    try:
        previous_packaged = _legacy._activate_packaged_self_eval_guard(request=request)
    except Exception:
        if previous_simple is None:
            os.environ.pop(_SIMPLE_EVAL_GUARD_ENV, None)
        else:
            os.environ[_SIMPLE_EVAL_GUARD_ENV] = previous_simple
        raise
    return previous_simple, previous_packaged


def _restore_self_eval_guard(previous: tuple[object | str | None, object | str | None] | None) -> None:
    if previous is None:
        return
    previous_simple, previous_packaged = previous
    _legacy._restore_packaged_self_eval_guard(previous_packaged)
    if previous_simple is None:
        os.environ.pop(_SIMPLE_EVAL_GUARD_ENV, None)
    else:
        os.environ[_SIMPLE_EVAL_GUARD_ENV] = str(previous_simple)


def _resolve_exec_log_path(*, role_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    return path.resolve() if path.is_absolute() else (role_dir.parent / path).resolve()


def _materialize_simple_role_workspace(
    *,
    request: Mapping[str, Any],
    run_root: Path,
    role_dir: Path,
    role_id: str,
    workspace_name: str,
    copy_source_workspace: bool,
) -> Path:
    source_workspace_root = Path(str(request["workspace_root"])).resolve()
    output_root = Path(str(request["output_root"])).resolve()
    workspace_root = (role_dir / "workspaces" / _safe_slug(workspace_name) / "workspace").resolve()
    if copy_source_workspace:
        excluded_paths = _legacy._default_ai_user_excluded_paths(
            source_workspace_root=source_workspace_root,
            output_root=output_root,
        )
        _legacy._copy_tree_excluding(
            source_root=source_workspace_root,
            dest_root=workspace_root,
            excluded_paths=excluded_paths,
        )
    else:
        if workspace_root.exists():
            import shutil

            shutil.rmtree(workspace_root)
        workspace_root.mkdir(parents=True, exist_ok=True)
    _legacy._materialize_current_run_snapshot(
        run_root=run_root,
        snapshot_root=workspace_root / ".loop_evaluator_internal" / "current_run_snapshot",
    )
    return workspace_root


def _build_provisional_context(request_obj: Mapping[str, Any]) -> dict[str, Any]:
    evaluation_id = str(request_obj.get("evaluation_id") or "simple_evaluator")
    run_id = str(request_obj.get("run_id") or _legacy._utc_stamp())
    workspace_root = _resolve_path(_ROOT, str(request_obj.get("workspace_root") or _ROOT))
    output_root = _resolve_path(
        _ROOT,
        str(request_obj.get("output_root") or (_ROOT / ".cache" / "loop_product" / "evaluator_simple")),
    )
    product_manual_ref = _resolve_path(
        workspace_root,
        str(request_obj.get("product_manual_ref") or (workspace_root / "__missing_product_manual_ref__")),
    )
    final_effects_text_ref_raw = str(request_obj.get("final_effects_text_ref") or "").strip()
    return {
        "evaluation_id": evaluation_id,
        "run_id": run_id,
        "run_root": output_root / evaluation_id / run_id,
        "workspace_root": workspace_root,
        "product_manual_ref": product_manual_ref,
        "final_effects_text_ref": (
            _resolve_path(workspace_root, final_effects_text_ref_raw) if final_effects_text_ref_raw else None
        ),
    }


def _build_lane_prompt(
    *,
    role_id: str,
    request: Mapping[str, Any],
    manual_text: str,
    target_unit: Mapping[str, Any],
) -> str:
    role_req = str(dict(request.get("role_requirements") or {}).get(role_id) or "").strip()
    role_name = "Test AI" if role_id == "test_designer" else "AI-as-User"
    requirements = [dict(item) for item in (target_unit.get("requirements") or [])]
    mission_lines = [
        f"# LOOP Evaluator Simple Prototype: {role_name}",
        "",
        "## Mission",
        "- Do not relaunch this evaluator, and do not run any local probe that would relaunch it from inside this delegated run, even if the product manual mentions such a probe; recursion protection will reject it.",
        "- The endpoint for this evaluation unit is already explicit; do not route this turn through endpoint clarification or any clarification-host helper.",
        "- Do not ask for confirmation, permission, or a follow-up choice such as `Proceed now?`; execute the bounded evaluation work now and return decisive evidence for this unit.",
    ]
    if role_id == "ai_user":
        mission_lines.extend(
            [
                "- Use the product from its documented surface first.",
                "- Prefer one bounded documented-surface user workflow or probe that directly tests this evaluation unit.",
                "- Prefer bounded non-interactive probes over headed or long-lived interactive sessions.",
                "- Any auxiliary browser, server, watcher, or helper process you start must be short-lived and cleaned up before you return.",
                "- Only fall back to direct source inspection or already-materialized artifacts when a documented executable path would itself relaunch this evaluator or would otherwise be evaluator-side misuse.",
                "- If a failure comes from your own path, command, or environment mistake, self-attribute and repair it before blaming the product.",
                "- Once decisive evidence exists for the current evaluation unit, stop and return immediately instead of hunting for nicer evidence.",
            ]
        )
    else:
        mission_lines.extend(
            [
                "- Prefer one bounded documented-surface probe plus reviewer-visible artifacts that directly test this evaluation unit.",
                "- Prefer bounded non-interactive probes over headed or long-lived interactive sessions.",
                "- Any auxiliary browser, server, watcher, or helper process you start must be short-lived and cleaned up before you return.",
                "- If a documented executable path would relaunch this evaluator or would otherwise be evaluator-side misuse, inspect source files or already-materialized artifacts under `current_run_root` instead.",
                "- Once decisive evidence exists for the current evaluation unit, stop and return immediately instead of hunting for nicer evidence.",
            ]
        )
    if role_req:
        mission_lines.append(f"- Task-specific focus, only if consistent with the mandatory rules above: {role_req}")
    mission_lines.extend(
        [
            "- Work on exactly one evaluation unit and keep the grouped requirements together.",
            "- Return only your final answer text.",
            "- The evaluator will forward your raw final output directly to the reviewer.",
            "",
            "## Target Evaluation Unit",
            "```json",
            json.dumps(dict(target_unit), indent=2, sort_keys=True),
            "```",
            "",
            "## Requirements In This Unit",
            "```json",
            json.dumps(requirements, indent=2, sort_keys=True),
            "```",
            "",
            "## Product Manual",
            manual_text,
        ]
    )
    return "\n".join(mission_lines)


def _build_reviewer_prompt(
    *,
    request: Mapping[str, Any],
    manual_text: str,
    checker_result: Mapping[str, Any] | None,
    lanes: Sequence[Mapping[str, Any]],
) -> str:
    role_req = str(dict(request.get("role_requirements") or {}).get("reviewer") or "").strip()
    compact_lanes = []
    for lane in lanes:
        compact_lanes.append(
            {
                "target_evaluation_unit": dict(lane.get("target_evaluation_unit") or {}),
                "test_ai": {
                    "raw_output_text": str(((lane.get("test_ai") or {}).get("raw_output_text")) or ""),
                },
                "ai_user": {
                    "raw_output_text": str(((lane.get("ai_user") or {}).get("raw_output_text")) or ""),
                },
            }
        )
    return "\n".join(
        [
            "# LOOP Evaluator Simple Prototype: Reviewer",
            "",
            "## Mission",
            f"- {role_req}",
            "- Read the checker output and the delegated raw lane outputs.",
            "- Treat self-caused recursive or relaunch-style probes as evaluator-side misuse rather than product failure.",
            "- A self-caused recursive probe failure must not outweigh consistent non-recursive evidence from the same unit.",
            "- Write only your final plain-text review summary.",
            "- Do not emit JSON unless you genuinely want plain-text JSON.",
            "",
            "## Checker Result",
            "```json",
            json.dumps(dict(checker_result or {}), indent=2, sort_keys=True),
            "```",
            "",
            "## Delegated Lanes",
            "```json",
            json.dumps(compact_lanes, indent=2, sort_keys=True),
            "```",
            "",
            "## Product Manual",
            manual_text,
        ]
    )


def _retryable_provider_issue_delay(issue: Mapping[str, Any] | None, *, failure_count: int) -> float | None:
    if not issue:
        return None
    return _legacy._provider_retry_delay_s(str(issue.get("issue_kind") or ""), failure_count=failure_count)


def _invoke_text_role(
    *,
    request: Mapping[str, Any],
    role_id: str,
    role_instance_id: str,
    run_root: Path,
    role_dir: Path,
    workspace_root: Path,
    prompt_text: str,
    context_obj: Mapping[str, Any],
) -> dict[str, Any]:
    role_dir.mkdir(parents=True, exist_ok=True)
    artifact_root = _legacy._role_runtime_artifact_root(role_dir, role_instance_id)
    prompt_path = artifact_root / "prompt.md"
    context_path = artifact_root / "context.json"
    response_path = artifact_root / "response.txt"
    result_path = artifact_root / "result.json"
    invocation_path = artifact_root / "invocation.json"
    exec_span_path = artifact_root / "exec_span.json"
    operation_log_path = artifact_root / "operation_log.md"
    for stale_path in (response_path, result_path, operation_log_path):
        if stale_path.exists():
            stale_path.unlink()
    _legacy._write_text(prompt_path, prompt_text)
    _legacy._write_json(context_path, dict(context_obj))

    cfg = _legacy._normalize_agent_config(request, role_id)
    agent_cmd = str(cfg.get("agent_cmd") or "").strip()
    if agent_cmd:
        validate_repo_shipped_role_agent_cmd(agent_cmd, context=f"evaluator request role `{role_id}`")
    resolved = resolve_agent_invocation(
        repo_root=_legacy._ROOT,
        mode="run",
        agent_cmd=agent_cmd or None,
        agent_provider=str(cfg.get("agent_provider") or "").strip() or None,
        agent_profile=str(cfg.get("agent_profile") or "").strip() or None,
    )
    if resolved is None:
        raise ValueError(f"unable to resolve agent invocation for {role_id}")
    effective_cmd = _legacy._effective_agent_cmd(resolved=resolved, config=cfg)
    runtime_env_overrides = _legacy._agent_runtime_env_overrides(
        run_root=run_root,
        role_id=role_instance_id,
        resolved=resolved,
        effective_cmd=effective_cmd,
        role_root=role_dir,
    )
    env = apply_env_map(
        resolved=resolved,
        env={
            "LOOP_PRODUCT_EVAL_PROMPT": str(prompt_path),
            "LOOP_PRODUCT_EVAL_ROLE": role_id,
            "LOOP_PRODUCT_EVAL_ROLE_INSTANCE": role_instance_id,
            "LOOP_PRODUCT_EVAL_WORKSPACE_ROOT": str(workspace_root),
            "LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT": str(request["workspace_root"]),
            "LOOP_PRODUCT_EVAL_CONTEXT_PATH": str(context_path),
            "LOOP_PRODUCT_EVAL_RESULT_PATH": str(result_path),
            "LOOP_PRODUCT_EVAL_RESPONSE_PATH": str(response_path),
            **(
                {"LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH": str(operation_log_path)}
                if role_id == "ai_user"
                else {}
            ),
        },
    )
    if runtime_env_overrides:
        env.update(runtime_env_overrides)
    launch_spec = _role_launch.build_committed_role_launch_spec(
        resolved=resolved,
        config=cfg,
        prompt_path=prompt_path,
        workspace_root=workspace_root,
        response_path=response_path,
        base_env=env,
    )
    retry_history: list[dict[str, Any]] = []
    safe_role_label = role_instance_id.replace(":", "__")
    final_cmd_result = None
    final_stdout_ref = None
    final_stderr_ref = None
    final_raw_output_text = ""
    final_raw_output_source = "stdout"
    final_exit_code = -1
    final_attempt_count = 0
    terminal_success_streams: list[str] | None = None
    terminal_success_patterns: list[str] | None = None
    if (
        str(launch_spec.get("launch_mode") or "") == "provider_argv"
        and str(launch_spec.get("provider_id") or "") == "codex_cli"
    ):
        terminal_success_streams = ["stderr"]
        terminal_success_patterns = [r"\btokens used\b"]
    for attempt_index in range(1, 5):
        final_attempt_count = attempt_index
        for stale_path in (response_path, result_path, operation_log_path):
            if stale_path.exists():
                stale_path.unlink()
        attempt_suffix = f"attempt_{attempt_index:02d}"
        attempt_invocation_path = artifact_root / f"invocation.{attempt_suffix}.json"
        attempt_exec_span_path = artifact_root / f"exec_span.{attempt_suffix}.json"
        attempt_invocation = {
            "role_id": role_id,
            "role_instance_id": role_instance_id,
            "attempt_index": attempt_index,
            "prompt_ref": str(prompt_path),
            "context_ref": str(context_path),
            "response_ref": str(response_path),
            "result_ref": str(result_path),
            "requested_config": cfg,
            "resolved_invocation": resolved.to_metadata(),
            "effective_agent_cmd": effective_cmd,
            "launch_spec": dict(launch_spec),
            **({"runtime_env_overrides": dict(runtime_env_overrides)} if runtime_env_overrides else {}),
        }
        _legacy._write_json(attempt_invocation_path, attempt_invocation)
        cmd_result = _role_launch.run_committed_role_launch(
            launch_spec=launch_spec,
            log_dir=artifact_root,
            label=f"{safe_role_label}__{attempt_suffix}",
            timeout_s=3600,
            idle_timeout_s=600,
            terminal_success_paths=[str(response_path)],
            terminal_success_stable_s=1.0,
            terminal_success_streams=terminal_success_streams,
            terminal_success_patterns=terminal_success_patterns,
            reconnect_grace_s=30,
            reconnect_max_events=8,
        )
        _legacy._write_json(attempt_exec_span_path, cmd_result.span)
        stdout_ref = _resolve_exec_log_path(role_dir=artifact_root, raw_path=str(cmd_result.span.get("stdout_path") or ""))
        stderr_ref = _resolve_exec_log_path(role_dir=artifact_root, raw_path=str(cmd_result.span.get("stderr_path") or ""))
        response_text = _read_text(response_path)
        stdout_text = _read_text(stdout_ref)
        stderr_text = _read_text(stderr_ref)
        raw_output_text = response_text if response_text else stdout_text
        raw_output_source = "response_path" if response_text else "stdout"
        exit_code = int(cmd_result.span.get("exit_code", -1))
        final_cmd_result = cmd_result
        final_stdout_ref = stdout_ref
        final_stderr_ref = stderr_ref
        final_raw_output_text = raw_output_text
        final_raw_output_source = raw_output_source
        final_exit_code = exit_code
        if exit_code == 0:
            break
        failure_text = "\n".join(part for part in (response_text, stderr_text, stdout_text) if part)
        issue = _classify_evaluator_side_issue(failure_text)
        retry_delay_s = _retryable_provider_issue_delay(issue, failure_count=len(retry_history) + 1)
        retry_history.append(
            {
                "attempt_index": attempt_index,
                "exit_code": exit_code,
                "issue": dict(issue or {}),
                "invocation_ref": str(attempt_invocation_path),
                "exec_span_ref": str(attempt_exec_span_path),
                "stdout_ref": str(stdout_ref),
                "stderr_ref": str(stderr_ref),
                **({"retry_delay_s": retry_delay_s} if retry_delay_s is not None else {}),
            }
        )
        if retry_delay_s is not None:
            time.sleep(retry_delay_s)
            continue
        break

    if final_cmd_result is None or final_stdout_ref is None or final_stderr_ref is None:
        raise RuntimeError(f"{role_instance_id} failed before launching the delegated role command")
    _legacy._write_json(
        invocation_path,
        {
            "role_id": role_id,
            "role_instance_id": role_instance_id,
            "prompt_ref": str(prompt_path),
            "context_ref": str(context_path),
            "response_ref": str(response_path),
            "result_ref": str(result_path),
            "requested_config": cfg,
            "resolved_invocation": resolved.to_metadata(),
            "effective_agent_cmd": effective_cmd,
            "launch_spec": dict(launch_spec),
            "attempt_count": final_attempt_count,
            "retry_history": retry_history,
            **({"runtime_env_overrides": dict(runtime_env_overrides)} if runtime_env_overrides else {}),
        },
    )
    _legacy._write_json(
        exec_span_path,
        {
            **dict(final_cmd_result.span),
            "attempt_count": final_attempt_count,
            "retry_history": retry_history,
        },
    )
    if final_exit_code != 0:
        final_issue = dict((retry_history[-1] if retry_history else {}).get("issue") or {})
        detail = _compact_detail(final_raw_output_text or _read_text(final_stderr_ref))
        suffix = f": {detail}" if detail else ""
        issue_kind = str(final_issue.get("issue_kind") or "").strip()
        if issue_kind.startswith("provider_"):
            raise RuntimeError(f"{role_instance_id} hit an upstream {issue_kind} blocker before producing result.json{suffix}")
        raise RuntimeError(f"{role_instance_id} failed with exit code {final_exit_code}{suffix}")
    return {
        "role_id": role_id,
        "role_instance_id": role_instance_id,
        "prompt_ref": str(prompt_path),
        "context_ref": str(context_path),
        "response_ref": str(response_path),
        "stdout_ref": str(final_stdout_ref),
        "stderr_ref": str(final_stderr_ref),
        "exec_span_ref": str(exec_span_path),
        "invocation_ref": str(invocation_path),
        "exit_code": final_exit_code,
        "raw_output_text": final_raw_output_text,
        "raw_output_source": final_raw_output_source,
        "attempt_count": final_attempt_count,
        "retry_history": retry_history,
        **({"operation_log_ref": str(operation_log_path)} if operation_log_path.exists() else {}),
    }


def _runtime_closure_requirements(
    requirements: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in requirements
        if _legacy.requirement_kind(dict(item)) == "runtime_closure"
    ]


def _product_effect_requirements(
    requirements: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in requirements
        if _legacy.requirement_kind(dict(item)) != "runtime_closure"
    ]


def _build_lane_units(
    *,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    checker_requirement_graph: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    if checker_requirement_graph is None:
        product_requirements = _product_effect_requirements(final_effect_requirements)
        requirement_ids = [str(item.get("requirement_id") or "") for item in product_requirements]
        if not requirement_ids:
            return []
        return [
            {
                "unit_id": "EU-001",
                "requirement_ids": requirement_ids,
                "requirements": [dict(item) for item in product_requirements],
            }
        ]
    requirement_by_id = {str(item["requirement_id"]): dict(item) for item in checker_requirement_graph["requirements"]}
    lane_units: list[dict[str, Any]] = []
    for item in checker_requirement_graph["evaluation_units"]:
        requirement_ids = [
            str(ref)
            for ref in item.get("requirement_ids") or []
            if _legacy.requirement_kind(dict(requirement_by_id.get(str(ref)) or {})) != "runtime_closure"
        ]
        if not requirement_ids:
            continue
        lane_units.append(
            {
                "unit_id": str(item["unit_id"]),
                "requirement_ids": requirement_ids,
                "requirements": [dict(requirement_by_id[requirement_id]) for requirement_id in requirement_ids],
            }
        )
    return lane_units


def _runtime_closure_notes(
    requirements: Sequence[Mapping[str, Any]],
    *,
    terminal: bool,
) -> list[str]:
    closure_requirements = _runtime_closure_requirements(requirements)
    if not closure_requirements:
        return []
    requirement_ids = ", ".join(str(item.get("requirement_id") or "") for item in closure_requirements)
    if terminal:
        return [
            f"Runtime-closure requirements satisfied by evaluator terminal completion: {requirement_ids}.",
        ]
    return [
        f"Runtime-closure requirements remain pending until evaluator terminal completion: {requirement_ids}.",
    ]


def _project_checker_result_from_state(state: Mapping[str, Any]) -> dict[str, Any] | None:
    checker_state = dict(state.get("checker") or {})
    if str(checker_state.get("status") or "") != "COMPLETED":
        return None
    return {
        "result_ref": str(checker_state.get("result_ref") or ""),
        "response_ref": str(checker_state.get("response_ref") or ""),
        "normalized_requirements": [dict(item) for item in list(checker_state.get("normalized_requirements") or [])],
        "requirement_graph": dict(checker_state.get("requirement_graph") or {}),
        "repair_actions": [dict(item) for item in list(checker_state.get("repair_actions") or [])],
        "notes": [str(item) for item in list(checker_state.get("notes") or [])],
    }


def _project_lanes_from_state(state: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(item) for item in _run_state.ordered_lanes(state=state)]


def _project_reviewer_from_state(state: Mapping[str, Any]) -> dict[str, Any] | None:
    reviewer_state = dict(state.get("reviewer") or {})
    if str(reviewer_state.get("status") or "") != "COMPLETED":
        return None
    return reviewer_state


def _issue_info_from_exception(exc: Exception) -> dict[str, str]:
    issue = _classify_evaluator_side_issue(str(exc)) or {}
    return {
        "kind": str(issue.get("kind") or "RUNTIME_ERROR"),
        "issue_kind": str(issue.get("issue_kind") or ""),
        "self_attribution": str(issue.get("self_attribution") or ""),
        "self_repair": str(issue.get("self_repair") or ""),
        "message": str(exc),
    }


def _is_same_run_recoverable_issue(issue_info: Mapping[str, Any]) -> bool:
    issue_kind = str(issue_info.get("issue_kind") or "").strip()
    if issue_kind.startswith("provider_"):
        return True
    return False


def _lane_blocked_status(issue_info: Mapping[str, Any]) -> str:
    return "BLOCKED_RETRYABLE" if _is_same_run_recoverable_issue(issue_info) else "BLOCKED_TERMINAL"


def _pick_blocked_issue(
    *,
    state: Mapping[str, Any],
    statuses: set[str],
) -> dict[str, Any] | None:
    checker_state = dict(state.get("checker") or {})
    checker_status = str(checker_state.get("status") or "")
    if checker_status in statuses:
        error = dict(checker_state.get("error") or {})
        if error:
            return {"stage": "checker", "issue": error}
    for lane in _project_lanes_from_state(state):
        lane_status = str(lane.get("status") or "")
        if lane_status in statuses:
            error = dict(lane.get("error") or {})
            if error:
                return {"stage": "lanes", "issue": error, "unit_id": str(dict(lane.get("target_evaluation_unit") or {}).get("unit_id") or "")}
    reviewer_state = dict(state.get("reviewer") or {})
    reviewer_status = str(reviewer_state.get("status") or "")
    if reviewer_status in statuses:
        error = dict(reviewer_state.get("error") or {})
        if error:
            return {"stage": "reviewer", "issue": error}
    return None


def _project_issue_info(error: Mapping[str, Any], *, default_message: str) -> dict[str, str]:
    return {
        "kind": str(error.get("kind") or "RUNTIME_ERROR"),
        "issue_kind": str(error.get("issue_kind") or ""),
        "self_attribution": str(error.get("self_attribution") or ""),
        "self_repair": str(error.get("self_repair") or ""),
        "message": str(error.get("message") or default_message),
    }


def _build_recovery_descriptor(
    *,
    run_root: Path,
    state: Mapping[str, Any],
    resume_from_stage: str,
) -> dict[str, Any]:
    lanes = _project_lanes_from_state(state)
    return {
        "required": True,
        "mode": "SAME_RUN_RESUME",
        "run_state_ref": str(_run_state.run_state_path(run_root=run_root)),
        "resume_from_stage": resume_from_stage,
        "preserve_frozen_checker_graph": str(dict(state.get("checker") or {}).get("status") or "") == "COMPLETED",
        "completed_lane_unit_ids": [
            str(dict(lane.get("target_evaluation_unit") or {}).get("unit_id") or "")
            for lane in lanes
            if str(dict(lane).get("status") or "") == "COMPLETED"
        ],
        "remaining_lane_unit_ids": [
            str(dict(lane.get("target_evaluation_unit") or {}).get("unit_id") or "")
            for lane in lanes
            if str(dict(lane).get("status") or "") != "COMPLETED"
        ],
    }


def _prune_stale_stdout_response_refs(*, state: dict[str, Any]) -> None:
    for lane in _project_lanes_from_state(state):
        for role_key in ("test_ai", "ai_user"):
            role_state = dict(lane.get(role_key) or {})
            if str(role_state.get("raw_output_source") or "") != "stdout":
                continue
            response_ref = str(role_state.get("response_ref") or "").strip()
            if not response_ref:
                continue
            response_path = Path(response_ref)
            if response_path.exists():
                response_path.unlink()
    reviewer_state = dict(state.get("reviewer") or {})
    if str(reviewer_state.get("raw_output_source") or "") == "stdout":
        response_ref = str(reviewer_state.get("response_ref") or "").strip()
        if response_ref:
            response_path = Path(response_ref)
            if response_path.exists():
                response_path.unlink()


def _run_lane(
    *,
    index: int,
    target_unit: Mapping[str, Any],
    request: Mapping[str, Any],
    run_root: Path,
    workspace_root: Path,
    manual_path: Path,
    manual_text: str,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    state: dict[str, Any],
) -> dict[str, Any]:
    target_unit_obj = dict(target_unit)
    unit_id = str(target_unit_obj.get("unit_id") or "").strip()
    lane_root = run_root / ".loop" / "lanes" / f"{index + 1:03d}_{_safe_slug(unit_id)}"
    lane_root.mkdir(parents=True, exist_ok=True)
    lanes_by_unit_id = dict(state.get("lanes_by_unit_id") or {})
    lane_state = dict(lanes_by_unit_id.get(unit_id) or {})
    if lane_state:
        existing_unit = dict(lane_state.get("target_evaluation_unit") or {})
        if existing_unit and str(existing_unit.get("unit_id") or "") != unit_id:
            raise ValueError(f"existing run state lane `{unit_id}` does not match the frozen checker lane plan")
    else:
        lane_state = {
            "status": "PENDING",
            "lane_root": str(lane_root),
            "target_evaluation_unit": target_unit_obj,
        }
    lane_state["lane_root"] = str(lane_root)
    lane_state["target_evaluation_unit"] = target_unit_obj
    state["lanes_by_unit_id"] = dict(lanes_by_unit_id)
    state["lanes_by_unit_id"][unit_id] = lane_state
    _run_state.write_run_state(run_root=run_root, state=state)
    base_context = {
        "evaluation_id": request["evaluation_id"],
        "current_run_root": str(run_root),
        "workspace_root": str(workspace_root),
        "product_manual_ref": str(manual_path),
        "final_effect_requirements": [dict(item) for item in final_effect_requirements],
        "target_evaluation_unit": target_unit_obj,
    }
    if str(lane_state.get("status") or "") == "COMPLETED":
        return lane_state

    lane_state["status"] = "RUNNING"
    lane_state.pop("error", None)
    state["lanes_by_unit_id"][unit_id] = lane_state
    _run_state.write_run_state(run_root=run_root, state=state)

    test_ai_role_dir = run_root / ".loop" / "test_ai"
    if str(dict(lane_state.get("test_ai") or {}).get("status") or "") != "COMPLETED":
        test_ai_workspace = _materialize_simple_role_workspace(
            request=request,
            run_root=run_root,
            role_dir=test_ai_role_dir,
            role_id="test_ai",
            workspace_name=f"test_ai__{unit_id}",
            copy_source_workspace=True,
        )
        test_ai_manual_text = _legacy._rewrite_workspace_absolute_paths(
            text=manual_text,
            source_workspace_root=workspace_root,
            target_workspace_root=test_ai_workspace,
        )
        try:
            test_ai = _invoke_text_role(
                request=request,
                role_id="test_designer",
                role_instance_id=f"test_ai__{unit_id}",
                run_root=run_root,
                role_dir=test_ai_role_dir,
                workspace_root=test_ai_workspace,
                prompt_text=_build_lane_prompt(
                    role_id="test_designer",
                    request=request,
                    manual_text=test_ai_manual_text,
                    target_unit=target_unit_obj,
                ),
                context_obj={
                    **base_context,
                    "role": "test_designer",
                    "workspace_root": str(test_ai_workspace),
                    "product_manual_ref": str(
                        _legacy._materialize_staged_role_context_ref(
                            staged_workspace_root=test_ai_workspace,
                            source_workspace_root=workspace_root,
                            source_path=manual_path,
                            label="product_manual",
                        )
                    ),
                },
            )
        except Exception as exc:  # noqa: BLE001
            issue_info = _issue_info_from_exception(exc)
            lane_state["test_ai"] = {
                "status": _lane_blocked_status(issue_info),
                "attempt_count": int(dict(lane_state.get("test_ai") or {}).get("attempt_count") or 0) + 1,
                "error": issue_info,
            }
            lane_state["status"] = str(lane_state["test_ai"]["status"])
            lane_state["error"] = issue_info
            state["lanes_by_unit_id"][unit_id] = lane_state
            _run_state.write_run_state(run_root=run_root, state=state)
            raise
        lane_state["test_ai"] = {
            "status": "COMPLETED",
            **dict(test_ai),
        }
        lane_state.pop("error", None)
        state["lanes_by_unit_id"][unit_id] = lane_state
        _run_state.write_run_state(run_root=run_root, state=state)
    ai_user_role_dir = run_root / ".loop" / "ai_user"
    if str(dict(lane_state.get("ai_user") or {}).get("status") or "") != "COMPLETED":
        ai_user_workspace = _materialize_simple_role_workspace(
            request=request,
            run_root=run_root,
            role_dir=ai_user_role_dir,
            role_id="ai_user",
            workspace_name=f"ai_user__{unit_id}",
            copy_source_workspace=True,
        )
        ai_user_manual_text = _legacy._rewrite_workspace_absolute_paths(
            text=manual_text,
            source_workspace_root=workspace_root,
            target_workspace_root=ai_user_workspace,
        )
        try:
            ai_user = _invoke_text_role(
                request=request,
                role_id="ai_user",
                role_instance_id=f"ai_user__{unit_id}",
                run_root=run_root,
                role_dir=ai_user_role_dir,
                workspace_root=ai_user_workspace,
                prompt_text=_build_lane_prompt(
                    role_id="ai_user",
                    request=request,
                    manual_text=ai_user_manual_text,
                    target_unit=target_unit_obj,
                ),
                context_obj={
                    **base_context,
                    "role": "ai_user",
                    "workspace_root": str(ai_user_workspace),
                    "product_manual_ref": str(
                        _legacy._materialize_staged_role_context_ref(
                            staged_workspace_root=ai_user_workspace,
                            source_workspace_root=workspace_root,
                            source_path=manual_path,
                            label="product_manual",
                        )
                    ),
                },
            )
        except Exception as exc:  # noqa: BLE001
            issue_info = _issue_info_from_exception(exc)
            lane_state["ai_user"] = {
                "status": _lane_blocked_status(issue_info),
                "attempt_count": int(dict(lane_state.get("ai_user") or {}).get("attempt_count") or 0) + 1,
                "error": issue_info,
            }
            lane_state["status"] = str(lane_state["ai_user"]["status"])
            lane_state["error"] = issue_info
            state["lanes_by_unit_id"][unit_id] = lane_state
            _run_state.write_run_state(run_root=run_root, state=state)
            raise
        lane_state["ai_user"] = {
            "status": "COMPLETED",
            **dict(ai_user),
        }
    lane_state["status"] = "COMPLETED"
    lane_state.pop("error", None)
    state["lanes_by_unit_id"][unit_id] = lane_state
    _run_state.write_run_state(run_root=run_root, state=state)
    return lane_state


def _write_report(run_root: Path, report: Mapping[str, Any]) -> dict[str, Any]:
    run_root.mkdir(parents=True, exist_ok=True)
    _legacy._write_json(run_root / "EvaluationReport.json", dict(report))
    return dict(report)


def _error_report(
    *,
    context: Mapping[str, Any],
    stage: str,
    message: str,
    status: str = "ERROR",
    kind: str = "RUNTIME_ERROR",
    issue_kind: str = "",
    self_attribution: str = "",
    self_repair: str = "",
    recovery: Mapping[str, Any] | None = None,
    checker_result: Mapping[str, Any] | None = None,
    final_effect_requirements: Sequence[Mapping[str, Any]] | None = None,
    runtime_closure_requirements: Sequence[Mapping[str, Any]] | None = None,
    lanes: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    runtime_closure_items = (
        [dict(item) for item in runtime_closure_requirements]
        if runtime_closure_requirements is not None
        else _runtime_closure_requirements(final_effect_requirements or [])
    )
    return _write_report(
        Path(str(context["run_root"])),
        {
            "schema": "loop_product.loop_evaluator_simple_prototype_report",
            "schema_version": "0.2.0",
            "evaluation_id": str(context["evaluation_id"]),
            "run_id": str(context["run_id"]),
            "run_root": str(context["run_root"]),
            "workspace_root": str(context["workspace_root"]),
            "product_manual_ref": str(context["product_manual_ref"]),
            **(
                {"final_effects_text_ref": str(context["final_effects_text_ref"])}
                if context.get("final_effects_text_ref") is not None
                else {}
            ),
            "status": status,
            "error": {
                "stage": stage,
                "message": message,
                "kind": kind,
                "issue_kind": issue_kind,
                "self_attribution": self_attribution,
                "self_repair": self_repair,
            },
            **({"recovery": dict(recovery)} if recovery is not None else {}),
            **({"checker_result": dict(checker_result)} if checker_result is not None else {}),
            **(
                {"final_effect_requirements": [dict(item) for item in final_effect_requirements]}
                if final_effect_requirements is not None
                else {}
            ),
            **(
                {"runtime_closure_requirements": runtime_closure_items}
                if runtime_closure_items
                else {}
            ),
            **({"lanes": [dict(item) for item in lanes]} if lanes else {}),
            "notes": [message, *_runtime_closure_notes(runtime_closure_items, terminal=False)],
        },
    )


def run_evaluator_simple_prototype(*, request: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    request_obj = _load_request(request)
    context = _build_provisional_context(request_obj)
    run_root = Path(str(context["run_root"]))
    run_root.mkdir(parents=True, exist_ok=True)
    _legacy._write_json(run_root / "input_snapshot.json", request_obj)

    stage = "preflight"
    checker_result: dict[str, Any] | None = None
    final_effect_requirements: list[dict[str, Any]] = []
    runtime_closure_requirements: list[dict[str, Any]] = []
    lane_units: list[dict[str, Any]] = []
    lanes: list[dict[str, Any]] = []
    reviewer: dict[str, Any] | None = None
    guard_previous: tuple[object | str | None, object | str | None] | None = None
    state: dict[str, Any] | None = None
    try:
        guard_previous = _activate_self_eval_guard(request_obj)
        normalized_request = _legacy._normalize_request(request_obj)
        workspace_root = Path(str(normalized_request["workspace_root"]))
        manual_path, manual_text = _legacy._read_text_ref(
            workspace_root=workspace_root,
            text_ref=str(normalized_request["product_manual_ref"]),
            label="product_manual_ref",
        )
        run_root = Path(str(normalized_request["output_root"])) / str(normalized_request["evaluation_id"]) / str(
            normalized_request["run_id"]
        )
        run_root.mkdir(parents=True, exist_ok=True)
        _legacy._write_json(run_root / "input_snapshot.json", normalized_request)
        context = {
            "evaluation_id": normalized_request["evaluation_id"],
            "run_id": normalized_request["run_id"],
            "run_root": run_root,
            "workspace_root": workspace_root,
            "product_manual_ref": manual_path,
            "final_effects_text_ref": (
                Path(str(normalized_request["final_effects_text_ref"]))
                if str(normalized_request.get("final_effects_text_ref") or "").strip()
                else None
            ),
        }
        request_fingerprint_value = _run_state.request_fingerprint(request=normalized_request)
        existing_state = _run_state.load_run_state(run_root=run_root)
        if existing_state is None:
            _legacy._clear_run_root_artifacts(run_root=run_root)
            run_root.mkdir(parents=True, exist_ok=True)
            _legacy._write_json(run_root / "input_snapshot.json", normalized_request)
            state = _run_state.build_initial_run_state(
                context=context,
                request_fingerprint_value=request_fingerprint_value,
            )
            _run_state.write_run_state(run_root=run_root, state=state)
        else:
            state = dict(existing_state)
            try:
                _run_state.assert_resume_compatible(
                    state=state,
                    context=context,
                    request_fingerprint_value=request_fingerprint_value,
                )
            except ValueError:
                _legacy._clear_run_root_artifacts(run_root=run_root)
                run_root.mkdir(parents=True, exist_ok=True)
                _legacy._write_json(run_root / "input_snapshot.json", normalized_request)
                state = _run_state.build_initial_run_state(
                    context=context,
                    request_fingerprint_value=request_fingerprint_value,
                )
                _run_state.write_run_state(run_root=run_root, state=state)
            else:
                _legacy._write_json(run_root / "input_snapshot.json", normalized_request)
                _prune_stale_stdout_response_refs(state=state)

        checker_state = dict(state.get("checker") or {})
        if context["final_effects_text_ref"] is not None:
            if str(checker_state.get("status") or "") != "COMPLETED":
                stage = "checker"
                goals_path, goals_text = _legacy._read_text_ref(
                    workspace_root=workspace_root,
                    text_ref=str(normalized_request["final_effects_text_ref"]),
                    label="final_effects_text_ref",
                )
                checker_role_dir = _legacy._default_role_runtime_root(run_root=run_root, role_id="checker")
                checker_workspace_root = _materialize_simple_role_workspace(
                    request=normalized_request,
                    run_root=run_root,
                    role_dir=checker_role_dir,
                    role_id="checker",
                    workspace_name="checker",
                    copy_source_workspace=False,
                )
                state["checker"] = {
                    **checker_state,
                    "status": "RUNNING",
                    "attempt_count": int(checker_state.get("attempt_count") or 0) + 1,
                }
                state["status"] = "IN_PROGRESS"
                _run_state.write_run_state(run_root=run_root, state=state)
                try:
                    checker_result_raw, checker_run = _legacy._invoke_role(
                        role_id="checker",
                        request=normalized_request,
                        run_root=run_root,
                        role_dir=checker_role_dir,
                        workspace_root=checker_workspace_root,
                        prompt_text=_legacy.build_checker_prompt(
                            request=normalized_request,
                            final_effects_text=_legacy._rewrite_workspace_absolute_paths(
                                text=goals_text,
                                source_workspace_root=workspace_root,
                                target_workspace_root=checker_workspace_root,
                            ),
                            result_path=(
                                _legacy._role_runtime_artifact_root(
                                    checker_role_dir,
                                    "checker",
                                )
                                / "result.json"
                            ),
                            response_path=(
                                _legacy._role_runtime_artifact_root(
                                    checker_role_dir,
                                    "checker",
                                )
                                / "response.md"
                            ),
                        ),
                        context_obj={
                            "evaluation_id": normalized_request["evaluation_id"],
                            "role": "checker",
                            "workspace_root": str(checker_workspace_root),
                            "final_effects_text_ref": str(
                                _legacy._materialize_staged_role_context_ref(
                                    staged_workspace_root=checker_workspace_root,
                                    source_workspace_root=workspace_root,
                                    source_path=goals_path,
                                    label="final_effects_text",
                                )
                            ),
                        },
                    )
                    checker_validated = _legacy._validate_checker_result(checker_result_raw)
                except _legacy.CheckerHumanGateError as exc:
                    state["checker"] = {
                        **dict(state.get("checker") or {}),
                        "status": "HUMAN_GATE",
                        "human_gate_reasons": list(exc.human_gate_reasons),
                        "notes": list(exc.human_gate_reasons),
                    }
                    state["status"] = "HUMAN_GATE"
                    _run_state.write_run_state(run_root=run_root, state=state)
                    return _write_report(
                        run_root,
                        {
                            "schema": "loop_product.loop_evaluator_simple_prototype_report",
                            "schema_version": "0.2.0",
                            "evaluation_id": str(context["evaluation_id"]),
                            "run_id": str(context["run_id"]),
                            "run_root": str(context["run_root"]),
                            "workspace_root": str(context["workspace_root"]),
                            "product_manual_ref": str(context["product_manual_ref"]),
                            **(
                                {"final_effects_text_ref": str(context["final_effects_text_ref"])}
                                if context.get("final_effects_text_ref") is not None
                                else {}
                            ),
                            "status": "HUMAN_GATE",
                            "human_gate_reasons": list(exc.human_gate_reasons),
                            "notes": list(exc.human_gate_reasons),
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    issue_info = _issue_info_from_exception(exc)
                    checker_status = _lane_blocked_status(issue_info)
                    state["checker"] = {
                        **dict(state.get("checker") or {}),
                        "status": checker_status,
                        "error": issue_info,
                        "human_gate_reasons": [],
                    }
                    state["status"] = "RECOVERY_REQUIRED" if checker_status == "BLOCKED_RETRYABLE" else "ERROR"
                    _run_state.write_run_state(run_root=run_root, state=state)
                    raise
                _legacy._materialize_validated_checker_artifacts(
                    role_dir=checker_role_dir,
                    validated_checker_result=checker_validated,
                )
                final_effect_requirements = [dict(item) for item in checker_validated["normalized_requirements"]]
                runtime_closure_requirements = _runtime_closure_requirements(final_effect_requirements)
                lane_units = _build_lane_units(
                    final_effect_requirements=final_effect_requirements,
                    checker_requirement_graph=dict(checker_validated["requirement_graph"]),
                )
                state["checker"] = {
                    "status": "COMPLETED",
                    "attempt_count": int(dict(state.get("checker") or {}).get("attempt_count") or 0),
                    "result_ref": str(checker_run["result_ref"]),
                    "response_ref": str(checker_run["response_ref"]),
                    "normalized_requirements": final_effect_requirements,
                    "requirement_graph": dict(checker_validated["requirement_graph"]),
                    "repair_actions": [dict(item) for item in checker_validated["repair_actions"]],
                    "notes": [str(item) for item in checker_validated["notes"]],
                    "human_gate_reasons": [],
                }
                state["final_effect_requirements"] = [dict(item) for item in final_effect_requirements]
                state["runtime_closure_requirements"] = [dict(item) for item in runtime_closure_requirements]
                if not list(state.get("lane_plan") or []):
                    state["lane_plan"] = [dict(item) for item in lane_units]
                state["status"] = "IN_PROGRESS"
                _run_state.write_run_state(run_root=run_root, state=state)
            checker_result = _project_checker_result_from_state(state)
            final_effect_requirements = [dict(item) for item in list(state.get("final_effect_requirements") or [])]
            runtime_closure_requirements = [dict(item) for item in list(state.get("runtime_closure_requirements") or [])]
            if not runtime_closure_requirements and final_effect_requirements:
                runtime_closure_requirements = _runtime_closure_requirements(final_effect_requirements)
                state["runtime_closure_requirements"] = [dict(item) for item in runtime_closure_requirements]
            lane_units = [dict(item) for item in list(state.get("lane_plan") or [])]
        else:
            final_effect_requirements = [dict(item) for item in list(state.get("final_effect_requirements") or [])]
            if not final_effect_requirements:
                final_effect_requirements = _legacy._validate_final_effect_requirements(
                    normalized_request.get("final_effect_requirements") or [],
                    label="evaluator final_effect_requirements",
                )
                state["final_effect_requirements"] = [dict(item) for item in final_effect_requirements]
            runtime_closure_requirements = [dict(item) for item in list(state.get("runtime_closure_requirements") or [])]
            if not runtime_closure_requirements and final_effect_requirements:
                runtime_closure_requirements = _runtime_closure_requirements(final_effect_requirements)
                state["runtime_closure_requirements"] = [dict(item) for item in runtime_closure_requirements]
            if not list(state.get("lane_plan") or []):
                state["lane_plan"] = _build_lane_units(
                    final_effect_requirements=final_effect_requirements,
                    checker_requirement_graph=None,
                )
            if str(dict(state.get("checker") or {}).get("status") or "") == "PENDING":
                state["checker"] = {
                    **dict(state.get("checker") or {}),
                    "status": "SKIPPED",
                }
            _run_state.write_run_state(run_root=run_root, state=state)
            lane_units = [dict(item) for item in list(state.get("lane_plan") or [])]

        if not final_effect_requirements:
            raise ValueError("evaluator requires at least one normalized requirement")

        stage = "lanes"
        for index, target_unit in enumerate(lane_units):
            unit_id = str(dict(target_unit).get("unit_id") or "")
            existing_lane = dict((dict(state.get("lanes_by_unit_id") or {})).get(unit_id) or {})
            if str(existing_lane.get("status") or "") == "COMPLETED":
                continue
            if str(existing_lane.get("status") or "") == "BLOCKED_TERMINAL":
                continue
            _run_lane(
                index=index,
                target_unit=target_unit,
                request=normalized_request,
                run_root=run_root,
                workspace_root=workspace_root,
                manual_path=manual_path,
                manual_text=manual_text,
                final_effect_requirements=final_effect_requirements,
                state=state,
            )

        lanes = _project_lanes_from_state(state)
        incomplete_lanes = [
            dict(lane)
            for lane in lanes
            if str(dict(lane).get("status") or "") != "COMPLETED"
        ]
        if incomplete_lanes:
            blocked_retryable = _pick_blocked_issue(state=state, statuses={"BLOCKED_RETRYABLE"})
            blocked_terminal = _pick_blocked_issue(state=state, statuses={"BLOCKED_TERMINAL"})
            blocked_issue = _project_issue_info(
                dict(((blocked_retryable or blocked_terminal or {}).get("issue") or {})),
                default_message="evaluator lanes remain unfinished in the current run state",
            )
            if blocked_retryable is not None and blocked_terminal is None:
                state["status"] = "RECOVERY_REQUIRED"
                _run_state.write_run_state(run_root=run_root, state=state)
                return _error_report(
                    context=context,
                    stage=str(blocked_retryable.get("stage") or stage),
                    status="RECOVERY_REQUIRED",
                    message=str(blocked_issue["message"]),
                    kind=str(blocked_issue["kind"]),
                    issue_kind=str(blocked_issue["issue_kind"]),
                    self_attribution=str(blocked_issue["self_attribution"]),
                    self_repair=str(blocked_issue["self_repair"]),
                    recovery=_build_recovery_descriptor(
                        run_root=run_root,
                        state=state,
                        resume_from_stage=str(blocked_retryable.get("stage") or stage),
                    ),
                    checker_result=_project_checker_result_from_state(state),
                    final_effect_requirements=[dict(item) for item in list(state.get("final_effect_requirements") or [])],
                    runtime_closure_requirements=[dict(item) for item in list(state.get("runtime_closure_requirements") or [])],
                    lanes=_project_lanes_from_state(state),
                )
            state["status"] = "ERROR"
            _run_state.write_run_state(run_root=run_root, state=state)
            return _error_report(
                context=context,
                stage=stage,
                message=str(blocked_issue["message"]),
                kind=str(blocked_issue["kind"]),
                issue_kind=str(blocked_issue["issue_kind"]),
                self_attribution=str(blocked_issue["self_attribution"]),
                self_repair=str(blocked_issue["self_repair"]),
                checker_result=_project_checker_result_from_state(state),
                final_effect_requirements=[dict(item) for item in list(state.get("final_effect_requirements") or [])],
                runtime_closure_requirements=[dict(item) for item in list(state.get("runtime_closure_requirements") or [])],
                lanes=_project_lanes_from_state(state),
            )

        stage = "reviewer"
        reviewer_state = dict(state.get("reviewer") or {})
        if not lane_units:
            state["reviewer"] = {
                **reviewer_state,
                "status": "SKIPPED",
                "attempt_count": int(reviewer_state.get("attempt_count") or 0),
            }
            _run_state.write_run_state(run_root=run_root, state=state)
        elif str(reviewer_state.get("status") or "") != "COMPLETED":
            reviewer_role_dir = _legacy._default_role_runtime_root(run_root=run_root, role_id="reviewer")
            reviewer_workspace_root = _materialize_simple_role_workspace(
                request=normalized_request,
                run_root=run_root,
                role_dir=reviewer_role_dir,
                role_id="reviewer",
                workspace_name="reviewer",
                copy_source_workspace=False,
            )
            state["reviewer"] = {
                **reviewer_state,
                "status": "RUNNING",
                "attempt_count": int(reviewer_state.get("attempt_count") or 0) + 1,
            }
            _run_state.write_run_state(run_root=run_root, state=state)
            try:
                reviewer_manual_text = _legacy._rewrite_workspace_absolute_paths(
                    text=manual_text,
                    source_workspace_root=workspace_root,
                    target_workspace_root=reviewer_workspace_root,
                )
                reviewer_run = _invoke_text_role(
                    request=normalized_request,
                    role_id="reviewer",
                    role_instance_id="reviewer",
                    run_root=run_root,
                    role_dir=reviewer_role_dir,
                    workspace_root=reviewer_workspace_root,
                    prompt_text=_build_reviewer_prompt(
                        request=normalized_request,
                        manual_text=reviewer_manual_text,
                        checker_result=_project_checker_result_from_state(state),
                        lanes=_project_lanes_from_state(state),
                    ),
                    context_obj={
                        "evaluation_id": normalized_request["evaluation_id"],
                        "role": "reviewer",
                        "workspace_root": str(reviewer_workspace_root),
                        "product_manual_ref": str(
                            _legacy._materialize_staged_role_context_ref(
                                staged_workspace_root=reviewer_workspace_root,
                                source_workspace_root=workspace_root,
                                source_path=manual_path,
                                label="product_manual",
                            )
                        ),
                        "checker_result": dict(_project_checker_result_from_state(state) or {}),
                        "final_effect_requirements": [dict(item) for item in list(state.get("final_effect_requirements") or [])],
                        "lanes": [dict(item) for item in _project_lanes_from_state(state)],
                    },
                )
            except Exception as exc:  # noqa: BLE001
                issue_info = _issue_info_from_exception(exc)
                reviewer_status = _lane_blocked_status(issue_info)
                state["reviewer"] = {
                    **dict(state.get("reviewer") or {}),
                    "status": reviewer_status,
                    "error": issue_info,
                }
                state["status"] = "RECOVERY_REQUIRED" if reviewer_status == "BLOCKED_RETRYABLE" else "ERROR"
                _run_state.write_run_state(run_root=run_root, state=state)
                raise
            state["reviewer"] = {
                "status": "COMPLETED",
                **dict(reviewer_run),
            }
            _run_state.write_run_state(run_root=run_root, state=state)
        reviewer = _project_reviewer_from_state(state)
    except _legacy.CheckerHumanGateError as exc:
        if state is not None:
            state["status"] = "HUMAN_GATE"
            _run_state.write_run_state(run_root=run_root, state=state)
        return _write_report(
            run_root,
            {
                "schema": "loop_product.loop_evaluator_simple_prototype_report",
                "schema_version": "0.2.0",
                "evaluation_id": str(context["evaluation_id"]),
                "run_id": str(context["run_id"]),
                "run_root": str(context["run_root"]),
                "workspace_root": str(context["workspace_root"]),
                "product_manual_ref": str(context["product_manual_ref"]),
                **(
                    {"final_effects_text_ref": str(context["final_effects_text_ref"])}
                    if context.get("final_effects_text_ref") is not None
                    else {}
                ),
                "status": "HUMAN_GATE",
                "human_gate_reasons": list(exc.human_gate_reasons),
                **(
                    {"checker_result": dict(checker_result)}
                    if checker_result is not None
                    else {}
                ),
                "notes": list(exc.human_gate_reasons),
            },
        )
    except Exception as exc:  # noqa: BLE001
        issue = _classify_evaluator_side_issue(str(exc))
        if state is not None:
            blocked_retryable = _pick_blocked_issue(state=state, statuses={"BLOCKED_RETRYABLE"})
            if blocked_retryable is not None:
                state["status"] = "RECOVERY_REQUIRED"
                _run_state.write_run_state(run_root=run_root, state=state)
                checker_result = _project_checker_result_from_state(state)
                final_effect_requirements = [dict(item) for item in list(state.get("final_effect_requirements") or [])]
                lanes = _project_lanes_from_state(state)
                issue_info = _project_issue_info(
                    dict(blocked_retryable.get("issue") or {}),
                    default_message=str(exc),
                )
                return _error_report(
                    context=context,
                    stage=str(blocked_retryable.get("stage") or stage),
                    status="RECOVERY_REQUIRED",
                    message=str(issue_info["message"]),
                    kind=str(issue_info["kind"]),
                    issue_kind=str(issue_info["issue_kind"]),
                    self_attribution=str(issue_info["self_attribution"]),
                    self_repair=str(issue_info["self_repair"]),
                    recovery=_build_recovery_descriptor(
                        run_root=run_root,
                        state=state,
                        resume_from_stage=str(blocked_retryable.get("stage") or stage),
                    ),
                    checker_result=checker_result,
                    final_effect_requirements=final_effect_requirements,
                    runtime_closure_requirements=[dict(item) for item in list(state.get("runtime_closure_requirements") or [])],
                    lanes=lanes,
                )
            state["status"] = "ERROR"
            _run_state.write_run_state(run_root=run_root, state=state)
            checker_result = _project_checker_result_from_state(state)
            final_effect_requirements = [dict(item) for item in list(state.get("final_effect_requirements") or [])]
            runtime_closure_requirements = [dict(item) for item in list(state.get("runtime_closure_requirements") or [])]
            lanes = _project_lanes_from_state(state)
        return _error_report(
            context=context,
            stage=stage,
            message=str(exc),
            kind=str((issue or {}).get("kind") or "RUNTIME_ERROR"),
            issue_kind=str((issue or {}).get("issue_kind") or ""),
            self_attribution=str((issue or {}).get("self_attribution") or ""),
            self_repair=str((issue or {}).get("self_repair") or ""),
            checker_result=checker_result,
            final_effect_requirements=final_effect_requirements,
            runtime_closure_requirements=runtime_closure_requirements,
            lanes=lanes,
        )
    finally:
        _restore_self_eval_guard(guard_previous)

    if state is not None:
        state["status"] = "COMPLETED"
        _run_state.write_run_state(run_root=run_root, state=state)
        checker_result = _project_checker_result_from_state(state)
        final_effect_requirements = [dict(item) for item in list(state.get("final_effect_requirements") or [])]
        runtime_closure_requirements = [dict(item) for item in list(state.get("runtime_closure_requirements") or [])]
        lanes = _project_lanes_from_state(state)
        reviewer = _project_reviewer_from_state(state)

    return _write_report(
        run_root,
        {
            "schema": "loop_product.loop_evaluator_simple_prototype_report",
            "schema_version": "0.2.0",
            "evaluation_id": str(context["evaluation_id"]),
            "run_id": str(context["run_id"]),
            "run_root": str(context["run_root"]),
            "workspace_root": str(context["workspace_root"]),
            "product_manual_ref": str(context["product_manual_ref"]),
            **(
                {"final_effects_text_ref": str(context["final_effects_text_ref"])}
                if context.get("final_effects_text_ref") is not None
                else {}
            ),
            "status": "COMPLETED",
            **({"checker_result": checker_result} if checker_result is not None else {}),
            "final_effect_requirements": [dict(item) for item in final_effect_requirements],
            **(
                {"runtime_closure_requirements": [dict(item) for item in runtime_closure_requirements]}
                if runtime_closure_requirements
                else {}
            ),
            "lanes": lanes,
            "reviewer": reviewer,
            "notes": _runtime_closure_notes(runtime_closure_requirements, terminal=True),
        },
    )


def run_evaluator(*, request: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    """Formal thin evaluator alias over the committed simple runtime body."""

    return run_evaluator_simple_prototype(request=request)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to an evaluator request JSON file.")
    args = parser.parse_args(argv)
    report = run_evaluator(request=Path(args.input))
    print(Path(str(report["run_root"])) / "EvaluationReport.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
