#!/usr/bin/env python3
"""AI-visible policy adapter for endpoint clarification.

`clarification.py` owns the deterministic shell: session files, artifact updates,
and CLI behavior. This module delegates high-variance language judgment to a
configurable policy command or agent provider so the shell does not grow its own
English rule engine.
"""

from __future__ import annotations

import json
import os
import re
import shlex
from pathlib import Path
from typing import Any, Mapping

from ..agent_provider import apply_env_map, resolve_agent_invocation
from ..run_cmd import run_cmd
from ..runtime_paths import product_repo_root

_ROOT = product_repo_root()
_DEFAULT_POLICY_PROVIDER = "codex_cli"
_POLICY_DIRNAME = ".endpoint_policy"
_POLICY_CMD_ENV = "LOOP_ENDPOINT_POLICY_CMD"
_POLICY_AGENT_CMD_ENV = "LOOP_ENDPOINT_POLICY_AGENT_CMD"
_POLICY_AGENT_PROVIDER_ENV = "LOOP_ENDPOINT_POLICY_AGENT_PROVIDER"
_POLICY_AGENT_PROFILE_ENV = "LOOP_ENDPOINT_POLICY_AGENT_PROFILE"
_POLICY_TIMEOUT_ENV = "LOOP_ENDPOINT_POLICY_TIMEOUT_S"
_POLICY_IDLE_TIMEOUT_ENV = "LOOP_ENDPOINT_POLICY_IDLE_TIMEOUT_S"
_PROMPT_ENV = "LOOP_PRODUCT_EVAL_PROMPT"
_TASK_TYPES = {"code", "writing", "research", "analysis", "planning", "design", "data", "other"}
_DEFAULT_POLICY_TIMEOUT_S = 25
_DEFAULT_POLICY_IDLE_TIMEOUT_S = 25
_GENERAL_START_QUESTION = "When this is successful, what exact thing should exist or happen at the end?"
_GENERAL_CONTINUE_QUESTION = "What would still be wrong, missing, or unclear if we stopped here?"
_BUGFIX_OBJECT_QUESTION = "What exact tool, command, file, page, or service is the thing you want fixed?"
_BUGFIX_SYMPTOM_QUESTION = "What exact command, input, or action is failing now, and what error or wrong behavior do you currently see?"
_BUGFIX_EFFECT_QUESTION = "After the fix, what should happen under that same command or input, and what should stop happening?"
_ENV_ASSIGNMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")
_UNSUPPORTED_SHELL_TOKENS = {"|", "||", "&&", ";", "<", ">", ">>", "2>", "2>>", "<<", "<<<"}


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _policy_dir(session_root: Path) -> Path:
    path = session_root / _POLICY_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _env_timeout(name: str, default: int) -> int:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _normalize_text(text: object) -> str:
    return " ".join(str(text or "").split()).strip()


def _split_clauses(text: object) -> list[str]:
    out: list[str] = []
    current: list[str] = []
    for ch in str(text or ""):
        if ch in ".!?;\n":
            clause = _normalize_text("".join(current).strip(" -,/\t\r\n"))
            if clause:
                out.append(clause)
            current = []
            continue
        current.append(" " if ch.isspace() else ch)
    tail = _normalize_text("".join(current).strip(" -,/\t\r\n"))
    if tail:
        out.append(tail)
    return out


def _dedupe_texts(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _normalize_text(item)
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _classify_clauses(text: object) -> tuple[list[str], list[str]]:
    confirmed: list[str] = []
    denied: list[str] = []
    for clause in _split_clauses(text):
        lowered = clause.casefold()
        if any(
            marker in lowered
            for marker in (
                "do not",
                "don't",
                "dont",
                "must not",
                "should not",
                "would be wrong",
                "wrong if",
                "unacceptable",
                "too heavy",
                "feels wrong",
                "not want",
                "no ",
                "without ",
            )
        ):
            denied.append(clause)
        else:
            confirmed.append(clause)
    return _dedupe_texts(confirmed), _dedupe_texts(denied)


def _is_bugfix_request(text: object) -> bool:
    lowered = _normalize_text(text).casefold()
    return any(
        marker in lowered
        for marker in (
            "fix",
            "bug",
            "broken",
            "not working",
            "repair",
            "debug",
            "panic",
            "crash",
            "failing",
            "fails",
        )
    )


def _target_object_known(text: object) -> bool:
    lowered = _normalize_text(text).casefold()
    return any(
        marker in lowered
        for marker in (
            "command",
            "tool",
            "file",
            "page",
            "service",
            "app",
            "script",
            "cli",
            "dashboard",
            "site",
            "html",
        )
    )


def _current_symptom_known(text: object) -> bool:
    lowered = _normalize_text(text).casefold()
    return any(
        marker in lowered
        for marker in (
            "error",
            "panic",
            "exception",
            "traceback",
            "crash",
            "fails",
            "failing",
            "wrong behavior",
            "wrong output",
            "empty output",
            "timed out",
            "stuck",
            "when i run",
        )
    )


def _desired_effect_known(text: object) -> bool:
    lowered = _normalize_text(text).casefold()
    return any(
        marker in lowered
        for marker in (
            "success means",
            "when this works",
            "when this is successful",
            "after the fix",
            "should ",
            "should be able",
            "should return",
            "should show",
            "must let",
            "must be able",
            "i want",
            "it should",
        )
    )


def _fallback_task_type(text: str) -> str:
    lowered = _normalize_text(text).casefold()
    if _is_bugfix_request(lowered):
        return "code"
    if any(marker in lowered for marker in ("poster", "landing page", "homepage", "visual", "style", "design")):
        return "design"
    if any(marker in lowered for marker in ("write", "rewrite", "copy", "essay", "blog", "article")):
        return "writing"
    if any(marker in lowered for marker in ("research", "look up", "find sources")):
        return "research"
    if any(marker in lowered for marker in ("plan", "roadmap")):
        return "planning"
    return "other"


def normalize_task_type_hint(task_type: object, *, fallback_text: object = "") -> str:
    normalized = _normalize_text(task_type)
    if normalized in _TASK_TYPES:
        return normalized
    return _fallback_task_type(f"{normalized} {_normalize_text(fallback_text)}".strip())


def _fallback_trace_path(session_root: Path, label: str) -> Path:
    return _policy_dir(session_root) / f"{label}.fallback.json"


def _build_fallback_decision(*, payload: Mapping[str, Any], reason: str) -> dict[str, Any]:
    action = str(payload.get("action") or "").strip().upper()
    user_text = _normalize_text(payload.get("user_text") or "")
    original_prompt = _normalize_text(payload.get("original_user_prompt") or user_text)
    current_requirement_artifact = dict(payload.get("current_requirement_artifact") or {})
    question_history = list(payload.get("question_history") or [])
    confirmed, denied = _classify_clauses(user_text)
    current_final_effect = _normalize_text(current_requirement_artifact.get("final_effect") or "")
    current_success = _dedupe_texts(list(current_requirement_artifact.get("observable_success_criteria") or []))
    current_denied = _dedupe_texts(list(current_requirement_artifact.get("non_goals") or []))
    current_context = _dedupe_texts(list(current_requirement_artifact.get("relevant_context") or []))
    bugfix_mode = _is_bugfix_request(original_prompt) or _is_bugfix_request(user_text)
    target_known = _target_object_known(original_prompt) or _target_object_known(user_text) or bool(current_final_effect)
    symptom_known = _current_symptom_known(user_text) or bool(current_context)
    desired_known = _desired_effect_known(user_text) or bool(current_success)
    exact_bypass = any(marker in user_text.casefold() for marker in ("exactly this", "nothing else", "exact output"))
    task_type = _fallback_task_type(original_prompt or user_text)

    if action == "START" and exact_bypass and confirmed:
        return {
            "task_type": task_type,
            "sufficient": True,
            "user_request_summary": original_prompt or user_text,
            "final_effect": confirmed[0],
            "observable_success_criteria": confirmed,
            "hard_constraints": [],
            "non_goals": denied,
            "relevant_context": [],
            "open_questions": [],
            "question_to_user": "",
            "confirmation_message": "Please confirm whether this restated final effect is the correct intended outcome before implementation starts.",
            "artifact_ready_for_persistence": True,
            "_fallback_reason": reason,
        }

    if bugfix_mode:
        if not target_known:
            open_question = "target_object"
            question_to_user = _BUGFIX_OBJECT_QUESTION
        elif not symptom_known:
            open_question = "current_symptom"
            question_to_user = _BUGFIX_SYMPTOM_QUESTION
        elif not desired_known:
            open_question = "post_fix_observable_behavior"
            question_to_user = _BUGFIX_EFFECT_QUESTION
        else:
            open_question = ""
            question_to_user = ""
    else:
        open_question = "concrete observable success criteria"
        question_to_user = _GENERAL_START_QUESTION if action == "START" else _GENERAL_CONTINUE_QUESTION

    clarified = False
    if bugfix_mode:
        clarified = target_known and symptom_known and desired_known
    else:
        total_confirmed = _dedupe_texts(current_success + confirmed)
        clarified = (
            len(_normalize_text(user_text).split()) >= 8
            and len(total_confirmed) >= 1
            and (len(confirmed) + len(denied) >= 2 or len(question_history) >= 1 or len(total_confirmed) >= 2)
        )

    if clarified:
        final_effect_candidates = _dedupe_texts(([current_final_effect] if current_final_effect else []) + confirmed)
        observable = _dedupe_texts(current_success + confirmed)
        if not final_effect_candidates:
            final_effect_candidates = [_normalize_text(user_text)]
        if not observable:
            observable = [_normalize_text(user_text)]
        return {
            "task_type": task_type,
            "sufficient": True,
            "user_request_summary": original_prompt or user_text,
            "final_effect": final_effect_candidates[0],
            "observable_success_criteria": observable,
            "hard_constraints": [],
            "non_goals": _dedupe_texts(current_denied + denied),
            "relevant_context": _dedupe_texts(current_context + ([user_text] if bugfix_mode and symptom_known else [])),
            "open_questions": [],
            "question_to_user": "",
            "confirmation_message": "Please confirm whether this restated final effect is the correct intended outcome before implementation starts.",
            "artifact_ready_for_persistence": True,
            "_fallback_reason": reason,
        }

    return {
        "task_type": task_type,
        "sufficient": False,
        "user_request_summary": original_prompt or user_text,
        "final_effect": "",
        "observable_success_criteria": [],
        "hard_constraints": [],
        "non_goals": _dedupe_texts(current_denied + denied),
        "relevant_context": _dedupe_texts(current_context + ([user_text] if bugfix_mode and symptom_known else [])),
        "open_questions": [open_question],
        "question_to_user": question_to_user,
        "confirmation_message": "",
        "artifact_ready_for_persistence": False,
        "_fallback_reason": reason,
    }


def _write_fallback_trace(*, session_root: Path, label: str, payload: Mapping[str, Any], reason: str, decision: Mapping[str, Any]) -> None:
    trace = {
        "fallback_used": True,
        "label": label,
        "reason": reason,
        "payload": dict(payload),
        "decision": dict(decision),
    }
    _write_text(_fallback_trace_path(session_root, label), _canonical_json(trace))


def _normalize_text_list(value: object) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not isinstance(value, list):
        return out
    for item in value:
        text = str(item or "").strip()
        key = text.casefold()
        if not text or key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _extract_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
    obj = json.loads(text)
    if not isinstance(obj, dict):
        raise ValueError("endpoint policy must return a JSON object")
    return obj


def _build_policy_prompt(*, payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "You are RequirementClarityGate.",
            "",
            "Your only job is to decide whether the user's current request contains enough information",
            "for the next worker agent to start safely and productively.",
            "",
            "Do NOT solve the task itself.",
            "Do NOT ask multiple questions.",
            "Do NOT ask for nice-to-have details.",
            "Do NOT guess missing high-impact requirements.",
            "",
            'Redefine "sufficient" as follows:',
            "",
            '"Sufficient" does NOT mean merely that the next worker agent can start investigating.',
            '"Sufficient" means ALL of the following are true:',
            "",
            "1. The intended final effect is explicit enough to be shown back to the user for confirmation.",
            "2. The intended final effect is concrete enough to be converted into a persistent requirement artifact for downstream use.",
            "3. The intended final effect is testable enough that a later evaluator could judge whether the task succeeded or failed.",
            "4. No missing detail remains whose wrong assumption would materially change the expected final behavior, acceptance criteria, or evaluation outcome.",
            "",
            "If any of the above is not true, then the request is still insufficient.",
            "",
            "Additional mandatory rule: before declaring the request sufficient, you must have an explicit statement of the intended final effect, not merely the bug location, symptom, or implementation target.",
            "",
            "When sufficient=true, you must present back to the user:",
            "- the final effect to be achieved,",
            "- the concrete observable success criteria,",
            "- the key constraints or non-goals,",
            "- the main context that materially affects execution.",
            "",
            "Do not just say that the information is sufficient.",
            "Do not summarize only the bug or the tool location.",
            "You must restate the intended end state in a way the user can directly confirm or correct.",
            "",
            "Step 1. Infer the task type from the request.",
            "Possible task types include: code, writing, research, analysis, planning, design, data, other.",
            "",
            "Step 2. Extract the currently known information:",
            "- deliverable",
            "- target_or_scope",
            "- success_criteria_or_final_effect",
            "- hard_constraints",
            "- environment_or_context",
            "- references_or_examples",
            "",
            "Step 3. Decide which fields are REQUIRED for this task type.",
            "Rules:",
            "- Always require a clear deliverable and target/scope.",
            "- Require success criteria when the request is implementation, generation, or evaluation oriented.",
            "- Require environment/context only if it materially changes the output.",
            "- Treat style/tone/audience as required for writing/design tasks, but optional for most technical tasks unless explicitly important.",
            "- If the missing detail is retrievable from tools/context, prefer lookup instead of asking.",
            "- Ask a clarifying question only when the missing detail is not otherwise retrievable.",
            "",
            "Special rule for bug-fix requests:",
            "",
            "For bug-fix, repair, or debug tasks, do not treat the request as sufficient merely because you recognize the repo, file, tool, or failing location.",
            "",
            "A bug-fix request is only sufficient when the intended post-fix behavior is clear enough to evaluate, and the current failing behavior is concrete enough to diagnose safely.",
            "",
            "The usual minimum bug-fix information set is:",
            "- the target object to be fixed, if that object is still ambiguous,",
            "- the current observable symptom under a concrete command, input, or action,",
            "- the intended post-fix observable behavior under that same command, input, or action.",
            "",
            "Do NOT force a rigid three-step script.",
            "If one of those items is already explicit, skip it.",
            "Ask only for the highest-impact missing item next.",
            "Do not declare sufficient while any still-missing bug-fix item could materially change diagnosis, acceptance, or evaluation.",
            "",
            "Question selection priority rule:",
            "",
            "When choosing a clarification question, prioritize missing information about the intended final effect or acceptance criteria over missing implementation details, file paths, or internal locations.",
            "",
            "For bug-fix tasks, prefer the earliest missing prerequisite among:",
            "- target object, if the thing to fix is still ambiguous,",
            "- current concrete symptom, if the object is clear but the failure is still vague,",
            "- intended post-fix observable behavior, once the object and symptom are already clear.",
            "",
            'Prefer asking "What should be true after this is fixed?" over asking "Where is the code?" or "Which file is it in?" unless implementation location is the single highest-impact blocker.',
            "",
            "Your question should reduce uncertainty about the desired end state, not merely about where work might begin.",
            "",
            "Step 4. If insufficient:",
            "- Ask exactly ONE question.",
            "- Choose the single missing detail with the highest information gain.",
            "- The question must be specific, short, domain-relevant, and about one topic only.",
            "- Prefer constrained options when the ambiguity space is small.",
            '- Never ask generic questions like "Anything else?" or "Can you provide more details?"',
            "",
            "Step 5. If sufficient:",
            "- State that the information is sufficient for the next step.",
            "- Summarize your understanding of the final effect in 1-3 sentences.",
            "- List only the key constraints that materially affect execution.",
            "- End with a concise confirmation request.",
            "",
            "Additional mandatory artifact rule:",
            "",
            "Your job is not complete unless you produce a structured requirement artifact that captures the confirmed final effect for downstream evaluation.",
            "",
            "This artifact must be treated as the source of truth for later stages.",
            "",
            "The artifact must contain at least:",
            "- task_type",
            "- user_request_summary",
            "- final_effect",
            "- observable_success_criteria",
            "- hard_constraints",
            "- non_goals",
            "- relevant_context",
            "- open_questions",
            "- sufficient",
            "- artifact_ready_for_persistence",
            "",
            "When sufficient=true:",
            "- final_effect must be non-empty and concrete.",
            "- observable_success_criteria must be non-empty.",
            "- open_questions must be empty.",
            "- the artifact must be ready to hand off to a downstream evaluator.",
            "",
            "When sufficient=false:",
            "- open_questions must contain exactly one highest-priority missing point.",
            "- final_effect may be provisional, but must clearly indicate what is still unresolved.",
            "",
            "Persistence rule:",
            "",
            "You must explicitly indicate whether the requirement artifact is ready to be persisted.",
            "",
            "- If the runtime supports file writing or artifact saving, output the artifact in a persistence-ready form.",
            "- If the runtime does not actually write files, you must still output the full artifact content that should be saved downstream.",
            "- Never imply that requirements have been saved unless you have either:",
            "  1. actually written them through a supported mechanism, or",
            "  2. emitted the exact structured artifact content for downstream persistence.",
            "",
            "Do not leave the persistence state implicit.",
            "",
            "Output JSON only in this exact schema:",
            "",
            "```json",
            "{",
            '  "task_type": "code | writing | research | analysis | planning | design | data | other",',
            '  "sufficient": true,',
            '  "user_request_summary": "",',
            '  "final_effect": "",',
            '  "observable_success_criteria": [],',
            '  "hard_constraints": [],',
            '  "non_goals": [],',
            '  "relevant_context": [],',
            '  "open_questions": [],',
            '  "question_to_user": "",',
            '  "confirmation_message": ""',
            '  "artifact_ready_for_persistence": true',
            "}",
            "```",
            "",
            "Additional rules:",
            '- If sufficient = false, "open_questions" must contain exactly one item.',
            '- If sufficient = false, "question_to_user" must be non-empty and "confirmation_message" must be empty.',
            '- If sufficient = false, "artifact_ready_for_persistence" must be false.',
            '- If sufficient = true, "final_effect" must be concrete and user-facing.',
            '- If sufficient = true, "observable_success_criteria" must be non-empty.',
            '- If sufficient = true, "open_questions" must be empty.',
            '- If sufficient = true, "question_to_user" must be empty.',
            '- If sufficient = true, "confirmation_message" must explicitly ask the user to confirm the restated final effect.',
            '- If sufficient = true, "artifact_ready_for_persistence" must be true.',
            '- Keep "final_effect" faithful to the user\'s words. Do not invent extra scope.',
            "- If multiple missing fields exist, ask about the one whose wrong assumption would create the most rework.",
            "",
            "Context JSON:",
            "```json",
            _canonical_json(dict(payload)).rstrip(),
            "```",
        ]
    )


def _run_override_command(*, session_root: Path, payload: Mapping[str, Any], label: str) -> dict[str, Any]:
    command = str(os.environ.get(_POLICY_CMD_ENV) or "").strip()
    if not command:
        raise ValueError(f"missing {_POLICY_CMD_ENV}")
    policy_dir = _policy_dir(session_root)
    input_path = policy_dir / f"{label}.input.json"
    _write_text(input_path, _canonical_json(dict(payload)))
    result = run_cmd(
        cmd=["bash", "-lc", command],
        cwd=session_root,
        log_dir=policy_dir,
        label=label,
        timeout_s=300,
        idle_timeout_s=300,
        capture_text=True,
        env={"LOOP_ENDPOINT_POLICY_INPUT": str(input_path)},
    )
    exit_code = int(result.span.get("exit_code", -1))
    raw_output = str(result.stdout_text or "").strip()
    if exit_code != 0:
        detail = str(result.stderr_text or raw_output or "").strip()
        raise RuntimeError(f"endpoint policy command failed with exit code {exit_code}: {detail}")
    return _extract_json_object(raw_output)


def _parse_direct_agent_cmd(agent_cmd: str) -> tuple[dict[str, str], list[str]]:
    raw = str(agent_cmd or "").strip()
    if not raw:
        raise ValueError("endpoint policy agent_cmd cannot be empty")
    tokens = shlex.split(raw)
    env_updates: dict[str, str] = {}
    while tokens and _ENV_ASSIGNMENT_RE.fullmatch(tokens[0] or ""):
        key, value = str(tokens.pop(0)).split("=", 1)
        env_updates[key] = os.path.expandvars(value)
    if not tokens:
        raise ValueError("endpoint policy agent_cmd must include an executable argv")
    bad_tokens = [token for token in tokens if token in _UNSUPPORTED_SHELL_TOKENS]
    if bad_tokens:
        raise ValueError(
            "endpoint policy agent_cmd must be a direct argv command without shell operators; "
            f"got unsupported tokens: {', '.join(sorted(set(bad_tokens)))}"
        )
    return env_updates, [str(token) for token in tokens]


def _provider_argv_for_prompt(*, provider_id: str, prompt_transport: str, prompt_arg: str, prompt_path: Path) -> tuple[list[str], str | None]:
    if provider_id == "codex_cli":
        argv = [
            "codex",
            "exec",
            "-C",
            str(_ROOT),
            "--skip-git-repo-check",
            "-s",
            "danger-full-access",
        ]
        if prompt_transport == "stdin":
            argv.append("-")
            return argv, str(prompt_path)
        if prompt_transport == "env_path":
            argv.append(str(prompt_path))
            return argv, None
        if prompt_transport == "arg":
            argv.extend([prompt_arg or "--prompt-file", str(prompt_path)])
            return argv, None
    if provider_id == "claude_code":
        argv = ["claude", "exec"]
        if prompt_transport == "stdin":
            argv.append("-")
            return argv, str(prompt_path)
        if prompt_transport == "env_path":
            argv.append(str(prompt_path))
            return argv, None
        if prompt_transport == "arg":
            argv.extend([prompt_arg or "--prompt-file", str(prompt_path)])
            return argv, None
    raise ValueError(f"unsupported endpoint policy provider launch: {provider_id} / {prompt_transport}")


def _run_default_agent(*, session_root: Path, payload: Mapping[str, Any], label: str) -> dict[str, Any]:
    policy_dir = _policy_dir(session_root)
    prompt_path = policy_dir / f"{label}.prompt.md"
    _write_text(prompt_path, _build_policy_prompt(payload=payload))
    resolved = resolve_agent_invocation(
        repo_root=_ROOT,
        mode="run",
        agent_cmd=str(os.environ.get(_POLICY_AGENT_CMD_ENV) or "").strip() or None,
        agent_provider=str(os.environ.get(_POLICY_AGENT_PROVIDER_ENV) or "").strip() or _DEFAULT_POLICY_PROVIDER,
        agent_profile=str(os.environ.get(_POLICY_AGENT_PROFILE_ENV) or "").strip() or None,
    )
    if resolved is None:
        raise ValueError("endpoint policy agent invocation could not be resolved")
    env = apply_env_map(resolved=resolved, env={_PROMPT_ENV: str(prompt_path)})
    stdin_path = ""
    try:
        parsed_env, argv = _parse_direct_agent_cmd(str(resolved.agent_cmd or ""))
        env.update(parsed_env)
    except Exception:
        argv, direct_stdin_path = _provider_argv_for_prompt(
            provider_id=str(resolved.provider_id or ""),
            prompt_transport=str(resolved.prompt_transport or "stdin"),
            prompt_arg=str(resolved.prompt_arg or ""),
            prompt_path=prompt_path,
        )
        stdin_path = str(direct_stdin_path or "")
    result = run_cmd(
        cmd=argv,
        cwd=_ROOT,
        log_dir=policy_dir,
        label=label,
        timeout_s=_env_timeout(_POLICY_TIMEOUT_ENV, _DEFAULT_POLICY_TIMEOUT_S),
        idle_timeout_s=_env_timeout(_POLICY_IDLE_TIMEOUT_ENV, _DEFAULT_POLICY_IDLE_TIMEOUT_S),
        capture_text=True,
        env=env,
        stdin_path=stdin_path,
    )
    exit_code = int(result.span.get("exit_code", -1))
    raw_output = str(result.stdout_text or "").strip()
    if exit_code != 0:
        detail = str(result.stderr_text or raw_output or "").strip()
        raise RuntimeError(f"endpoint policy agent failed with exit code {exit_code}: {detail}")
    return _extract_json_object(raw_output)


def _invoke_policy(*, session_root: Path, payload: Mapping[str, Any], label: str) -> dict[str, Any]:
    if str(os.environ.get(_POLICY_CMD_ENV) or "").strip():
        return _run_override_command(session_root=session_root, payload=payload, label=label)
    try:
        return _run_default_agent(session_root=session_root, payload=payload, label=label)
    except Exception as exc:  # noqa: BLE001
        reason = _normalize_text(str(exc) or exc.__class__.__name__)
        decision = _build_fallback_decision(payload=payload, reason=reason)
        _write_fallback_trace(
            session_root=session_root,
            label=label,
            payload=payload,
            reason=reason,
            decision=decision,
        )
        return decision


def _normalize_requirement_artifact(obj: Mapping[str, Any]) -> tuple[dict[str, Any], str, str]:
    task_type = str(obj.get("task_type") or "").strip()
    if task_type not in _TASK_TYPES:
        raise ValueError("endpoint policy decision must return a supported task_type")
    if not isinstance(obj.get("sufficient"), bool):
        raise ValueError("endpoint policy decision must return boolean sufficient")
    sufficient = bool(obj.get("sufficient"))
    user_request_summary = str(obj.get("user_request_summary") or "").strip()
    final_effect = str(obj.get("final_effect") or "").strip()
    observable_success_criteria = _normalize_text_list(obj.get("observable_success_criteria"))
    hard_constraints = _normalize_text_list(obj.get("hard_constraints"))
    non_goals = _normalize_text_list(obj.get("non_goals"))
    relevant_context = _normalize_text_list(obj.get("relevant_context"))
    open_questions = _normalize_text_list(obj.get("open_questions"))
    question_to_user = str(obj.get("question_to_user") or "").strip()
    confirmation_message = str(obj.get("confirmation_message") or "").strip()
    artifact_ready = obj.get("artifact_ready_for_persistence")
    if not isinstance(artifact_ready, bool):
        raise ValueError("endpoint policy decision must return boolean artifact_ready_for_persistence")
    if not user_request_summary:
        raise ValueError("endpoint policy decision must return non-empty user_request_summary")
    if sufficient:
        if not final_effect:
            raise ValueError("endpoint policy sufficient decision must return non-empty final_effect")
        if not observable_success_criteria:
            raise ValueError("endpoint policy sufficient decision must return observable_success_criteria")
        if open_questions:
            raise ValueError("endpoint policy sufficient decision must not return open_questions")
        if question_to_user:
            raise ValueError("endpoint policy sufficient decision must not return question_to_user")
        if not confirmation_message:
            raise ValueError("endpoint policy sufficient decision must return confirmation_message")
        if not artifact_ready:
            raise ValueError("endpoint policy sufficient decision must set artifact_ready_for_persistence = true")
    else:
        if len(open_questions) != 1:
            raise ValueError("endpoint policy insufficient decision must return exactly one open_questions item")
        if not question_to_user:
            raise ValueError("endpoint policy insufficient decision must return question_to_user")
        if confirmation_message:
            raise ValueError("endpoint policy insufficient decision must not return confirmation_message")
        if artifact_ready:
            raise ValueError("endpoint policy insufficient decision must set artifact_ready_for_persistence = false")
    artifact = {
        "task_type": task_type,
        "sufficient": sufficient,
        "user_request_summary": user_request_summary,
        "final_effect": final_effect,
        "observable_success_criteria": observable_success_criteria,
        "hard_constraints": hard_constraints,
        "non_goals": non_goals,
        "relevant_context": relevant_context,
        "open_questions": open_questions,
        "artifact_ready_for_persistence": artifact_ready,
    }
    return artifact, question_to_user, confirmation_message


def _normalize_start_decision(obj: Mapping[str, Any]) -> dict[str, Any]:
    requirement_artifact, question_to_user, confirmation_message = _normalize_requirement_artifact(obj)
    if requirement_artifact["sufficient"]:
        return {
            "mode_decision": "BYPASS",
            "session_status": "BYPASSED",
            "assistant_question": "",
            "assistant_message": confirmation_message,
            "requirement_artifact": requirement_artifact,
        }
    return {
        "mode_decision": "VISION_COMPILER",
        "session_status": "ACTIVE",
        "assistant_question": question_to_user,
        "assistant_message": "",
        "requirement_artifact": requirement_artifact,
    }


def _normalize_continue_decision(obj: Mapping[str, Any]) -> dict[str, Any]:
    requirement_artifact, question_to_user, confirmation_message = _normalize_requirement_artifact(obj)
    if requirement_artifact["sufficient"]:
        session_status = "CLARIFIED"
        assistant_question = ""
        assistant_message = confirmation_message
    else:
        session_status = "ACTIVE"
        assistant_question = question_to_user
        assistant_message = ""
    return {
        "mode_decision": "VISION_COMPILER",
        "session_status": session_status,
        "assistant_question": assistant_question,
        "assistant_message": assistant_message,
        "requirement_artifact": requirement_artifact,
    }


def evaluate_start_policy(*, session_root: str | Path, user_text: str) -> dict[str, Any]:
    root = Path(session_root).resolve()
    payload = {
        "action": "START",
        "user_text": str(user_text),
    }
    return _normalize_start_decision(
        _invoke_policy(session_root=root, payload=payload, label="start_policy")
    )


def normalize_start_policy_decision(obj: Mapping[str, Any]) -> dict[str, Any]:
    return _normalize_start_decision(obj)


def evaluate_continue_policy(
    *,
    session_root: str | Path,
    original_user_prompt: str,
    user_text: str,
    question_history: list[str],
    confirmed_requirements: list[dict[str, str]],
    denied_requirements: list[dict[str, str]],
    current_requirement_artifact: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(session_root).resolve()
    payload = {
        "action": "CONTINUE",
        "original_user_prompt": str(original_user_prompt),
        "user_text": str(user_text),
        "question_history": list(question_history),
        "confirmed_requirements": [str(item.get("text") or "") for item in confirmed_requirements],
        "denied_requirements": [str(item.get("text") or "") for item in denied_requirements],
        "current_requirement_artifact": dict(current_requirement_artifact or {}),
    }
    return _normalize_continue_decision(
        _invoke_policy(session_root=root, payload=payload, label="continue_policy")
    )


def normalize_continue_policy_decision(obj: Mapping[str, Any]) -> dict[str, Any]:
    return _normalize_continue_decision(obj)
