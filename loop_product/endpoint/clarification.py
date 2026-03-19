#!/usr/bin/env python3
"""Deterministic front-half endpoint-clarification shell and CLI."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .policy import (
    evaluate_continue_policy,
    evaluate_start_policy,
    normalize_continue_policy_decision,
    normalize_start_policy_decision,
    normalize_task_type_hint,
)


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_canonical_json(obj), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _session_root(raw: str | Path) -> Path:
    return Path(raw).resolve()


def _artifact_path(session_root: Path) -> Path:
    return session_root / "EndpointArtifact.json"


def _turn_dir(session_root: Path, turn_index: int) -> Path:
    return session_root / "turns" / f"{turn_index:04d}"


def _turn_result_path(session_root: Path, turn_index: int) -> Path:
    return _turn_dir(session_root, turn_index) / "TurnResult.json"


def _signal_entry(*, turn_id: str, kind: str, text: str) -> dict[str, str]:
    return {
        "clause_id": "",
        "kind": kind,
        "source_turn_id": turn_id,
        "text": text,
    }


def _signal_entries(*, turn_id: str, kind: str, texts: list[str]) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for index, text in enumerate(texts, start=1):
        item = _signal_entry(turn_id=turn_id, kind=kind, text=text)
        item["clause_id"] = f"{turn_id}-C{index:02d}-{kind}"
        out.append(item)
    return out


def _dedupe_clauses(items: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for item in items:
        key = (str(item.get("kind") or ""), str(item.get("text") or "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _artifact(
    *,
    session_root: Path,
    mode: str,
    status: str,
    original_user_prompt: str,
    confirmed_requirements: list[dict[str, str]],
    denied_requirements: list[dict[str, str]],
    question_history: list[str],
    turn_count: int,
    latest_turn_ref: Path,
    requirement_artifact: dict[str, Any],
) -> dict[str, Any]:
    return {
        "version": "1",
        "session_root": str(session_root),
        "artifact_ref": str(_artifact_path(session_root)),
        "latest_turn_ref": str(latest_turn_ref),
        "mode": mode,
        "status": status,
        "original_user_prompt": original_user_prompt,
        "confirmed_requirements": confirmed_requirements,
        "denied_requirements": denied_requirements,
        "question_history": question_history,
        "turn_count": turn_count,
        "requirement_artifact": requirement_artifact,
    }


def _turn_result(
    *,
    action: str,
    turn_id: str,
    mode_decision: str,
    session_status: str,
    assistant_message: str,
    assistant_question: str,
    artifact_ref: Path,
    turn_result_ref: Path,
    requirement_artifact_ready_for_persistence: bool,
) -> dict[str, Any]:
    return {
        "version": "1",
        "action": action,
        "turn_id": turn_id,
        "mode_decision": mode_decision,
        "session_status": session_status,
        "assistant_message": assistant_message,
        "assistant_question": assistant_question,
        "question_count": 1 if assistant_question else 0,
        "artifact_ref": str(artifact_ref),
        "turn_result_ref": str(turn_result_ref),
        "artifact_location_disclosed": True,
        "requirement_artifact_ready_for_persistence": requirement_artifact_ready_for_persistence,
    }


def _message_with_artifact(*, message: str, artifact_path: Path) -> str:
    text = str(message or "").strip()
    artifact_line = f"Endpoint artifact: {artifact_path}"
    if not text:
        return artifact_line
    if artifact_line in text:
        return text
    return f"{text}\n{artifact_line}"


def _requirement_artifact_from_decision(decision: dict[str, Any]) -> dict[str, Any]:
    artifact = dict(decision.get("requirement_artifact") or {})
    return {
        "task_type": str(artifact.get("task_type") or "").strip(),
        "sufficient": bool(artifact.get("sufficient")),
        "user_request_summary": str(artifact.get("user_request_summary") or "").strip(),
        "final_effect": str(artifact.get("final_effect") or "").strip(),
        "observable_success_criteria": [str(item or "").strip() for item in list(artifact.get("observable_success_criteria") or []) if str(item or "").strip()],
        "hard_constraints": [str(item or "").strip() for item in list(artifact.get("hard_constraints") or []) if str(item or "").strip()],
        "non_goals": [str(item or "").strip() for item in list(artifact.get("non_goals") or []) if str(item or "").strip()],
        "relevant_context": [str(item or "").strip() for item in list(artifact.get("relevant_context") or []) if str(item or "").strip()],
        "open_questions": [str(item or "").strip() for item in list(artifact.get("open_questions") or []) if str(item or "").strip()],
        "artifact_ready_for_persistence": bool(artifact.get("artifact_ready_for_persistence")),
    }


def _requirement_artifact_clause_texts(requirement_artifact: dict[str, Any]) -> tuple[list[str], list[str]]:
    confirmed = []
    for text in (
        requirement_artifact.get("user_request_summary"),
        requirement_artifact.get("final_effect"),
    ):
        text = str(text or "").strip()
        if text:
            confirmed.append(text)
    confirmed.extend(list(requirement_artifact.get("observable_success_criteria") or []))
    confirmed.extend(list(requirement_artifact.get("hard_constraints") or []))
    confirmed.extend(list(requirement_artifact.get("relevant_context") or []))
    denied = [str(item or "").strip() for item in list(requirement_artifact.get("non_goals") or []) if str(item or "").strip()]
    return confirmed, denied


def _format_list_section(*, title: str, items: list[str]) -> list[str]:
    normalized = [str(item or "").strip() for item in items if str(item or "").strip()]
    if not normalized:
        return []
    return ["", f"{title}:", *[f"- {item}" for item in normalized]]


def _build_sufficient_message(
    *,
    requirement_artifact: dict[str, Any],
    confirmation_message: str,
    artifact_path: Path,
) -> str:
    lines = ["The request is now sufficiently defined for the next step."]
    summary = str(requirement_artifact.get("user_request_summary") or "").strip()
    final_effect = str(requirement_artifact.get("final_effect") or "").strip()
    if summary:
        lines.extend(["", "User request summary:", summary])
    if final_effect:
        lines.extend(["", "Final effect:", final_effect])
    lines.extend(
        _format_list_section(
            title="Observable success criteria",
            items=list(requirement_artifact.get("observable_success_criteria") or []),
        )
    )
    lines.extend(
        _format_list_section(
            title="Hard constraints",
            items=list(requirement_artifact.get("hard_constraints") or []),
        )
    )
    lines.extend(
        _format_list_section(
            title="Non-goals",
            items=list(requirement_artifact.get("non_goals") or []),
        )
    )
    lines.extend(
        _format_list_section(
            title="Relevant context",
            items=list(requirement_artifact.get("relevant_context") or []),
        )
    )
    lines.extend(
        [
            "",
            "Requirement artifact ready for persistence: "
            + ("yes" if requirement_artifact.get("artifact_ready_for_persistence") else "no"),
            f"Endpoint artifact: {artifact_path}",
        ]
    )
    confirmation = str(confirmation_message or "").strip()
    if confirmation:
        lines.extend(["", confirmation])
    return "\n".join(lines)


def _persist_start_decision(*, root: Path, user_text: str, decision: dict[str, Any]) -> dict[str, Any]:
    artifact_path = _artifact_path(root)
    if artifact_path.exists():
        raise ValueError(f"endpoint clarification session already exists: {artifact_path}")
    turn_index = 1
    turn_id = f"T{turn_index:04d}"
    requirement_artifact = _requirement_artifact_from_decision(decision)
    confirmed_texts, denied_texts = _requirement_artifact_clause_texts(requirement_artifact)
    confirmed = _signal_entries(turn_id=turn_id, kind="MANDATORY", texts=confirmed_texts)
    denied = _signal_entries(turn_id=turn_id, kind="FORBIDDEN", texts=denied_texts)
    confirmed = _dedupe_clauses(confirmed)
    denied = _dedupe_clauses(denied)
    mode = str(decision.get("mode_decision") or "")
    assistant_message_override = str(decision.get("assistant_message") or "").strip()
    if mode == "BYPASS":
        mode = "BYPASS"
        status = "BYPASSED"
        assistant_question = ""
        assistant_message = _build_sufficient_message(
            requirement_artifact=requirement_artifact,
            confirmation_message=assistant_message_override,
            artifact_path=artifact_path,
        )
        question_history: list[str] = []
    else:
        mode = "VISION_COMPILER"
        status = "ACTIVE"
        assistant_question = str(decision.get("assistant_question") or "")
        assistant_message = _message_with_artifact(
            message=assistant_message_override
            or (
                "Entering VISION_COMPILER to clarify the desired final result.\n"
                f"{assistant_question}"
            ),
            artifact_path=artifact_path,
        )
        question_history = [assistant_question]

    turn_result_path = _turn_result_path(root, turn_index)
    artifact = _artifact(
        session_root=root,
        mode=mode,
        status=status,
        original_user_prompt=str(user_text),
        confirmed_requirements=confirmed,
        denied_requirements=denied,
        question_history=question_history,
        turn_count=turn_index,
        latest_turn_ref=turn_result_path,
        requirement_artifact=requirement_artifact,
    )
    turn_result = _turn_result(
        action="START",
        turn_id=turn_id,
        mode_decision=mode,
        session_status=status,
        assistant_message=assistant_message,
        assistant_question=assistant_question,
        artifact_ref=artifact_path,
        turn_result_ref=turn_result_path,
        requirement_artifact_ready_for_persistence=bool(
            requirement_artifact.get("artifact_ready_for_persistence")
        ),
    )
    _write_json(artifact_path, artifact)
    _write_json(turn_result_path, turn_result)
    return turn_result


def _persist_continue_decision(*, root: Path, user_text: str, decision: dict[str, Any]) -> dict[str, Any]:
    artifact_path = _artifact_path(root)
    if not artifact_path.exists():
        raise ValueError(f"endpoint clarification artifact does not exist: {artifact_path}")
    artifact = dict(_read_json(artifact_path))
    if artifact.get("mode") != "VISION_COMPILER":
        raise ValueError("continue_endpoint_clarification can only continue a VISION_COMPILER session")
    if artifact.get("status") != "ACTIVE":
        raise ValueError("continue_endpoint_clarification can only continue an ACTIVE session")

    turn_index = int(artifact.get("turn_count") or 0) + 1
    turn_id = f"T{turn_index:04d}"
    question_history = list(artifact.get("question_history") or [])
    requirement_artifact = _requirement_artifact_from_decision(decision)
    new_confirmed_texts, new_denied_texts = _requirement_artifact_clause_texts(requirement_artifact)
    new_confirmed = _signal_entries(turn_id=turn_id, kind="MANDATORY", texts=new_confirmed_texts)
    new_denied = _signal_entries(turn_id=turn_id, kind="FORBIDDEN", texts=new_denied_texts)
    confirmed = _dedupe_clauses(list(artifact.get("confirmed_requirements") or []) + new_confirmed)
    denied = _dedupe_clauses(list(artifact.get("denied_requirements") or []) + new_denied)
    turn_result_path = _turn_result_path(root, turn_index)

    status = str(decision.get("session_status") or "")
    assistant_message_override = str(decision.get("assistant_message") or "").strip()
    if status == "CLARIFIED":
        status = "CLARIFIED"
        assistant_question = ""
        summary_bits: list[str] = []
        if confirmed:
            summary_bits.append("confirmed endpoint requirements recorded")
        if denied:
            summary_bits.append("denied endpoint requirements recorded")
        summary = ", ".join(summary_bits) if summary_bits else "endpoint signals recorded"
        assistant_message = _build_sufficient_message(
            requirement_artifact=requirement_artifact,
            confirmation_message=assistant_message_override
            or f"Please confirm whether this is the correct intended outcome before implementation starts. ({summary})",
            artifact_path=artifact_path,
        )
    else:
        status = "ACTIVE"
        assistant_question = str(decision.get("assistant_question") or "")
        question_history.append(assistant_question)
        assistant_message = _message_with_artifact(
            message=assistant_message_override
            or f"Continuing VISION_COMPILER.\n{assistant_question}",
            artifact_path=artifact_path,
        )

    updated_artifact = _artifact(
        session_root=root,
        mode="VISION_COMPILER",
        status=status,
        original_user_prompt=str(artifact.get("original_user_prompt") or ""),
        confirmed_requirements=confirmed,
        denied_requirements=denied,
        question_history=question_history,
        turn_count=turn_index,
        latest_turn_ref=turn_result_path,
        requirement_artifact=requirement_artifact,
    )
    turn_result = _turn_result(
        action="CONTINUE",
        turn_id=turn_id,
        mode_decision="VISION_COMPILER",
        session_status=status,
        assistant_message=assistant_message,
        assistant_question=assistant_question,
        artifact_ref=artifact_path,
        turn_result_ref=turn_result_path,
        requirement_artifact_ready_for_persistence=bool(
            requirement_artifact.get("artifact_ready_for_persistence")
        ),
    )
    _write_json(artifact_path, updated_artifact)
    _write_json(turn_result_path, turn_result)
    return turn_result


def start_endpoint_clarification(*, session_root: str | Path, user_text: str) -> dict[str, Any]:
    root = _session_root(session_root)
    decision = evaluate_start_policy(session_root=root, user_text=user_text)
    return _persist_start_decision(root=root, user_text=user_text, decision=decision)


def continue_endpoint_clarification(*, session_root: str | Path, user_text: str) -> dict[str, Any]:
    root = _session_root(session_root)
    artifact = load_endpoint_artifact(session_root=root)
    decision = evaluate_continue_policy(
        session_root=root,
        original_user_prompt=str(artifact.get("original_user_prompt") or ""),
        user_text=user_text,
        question_history=list(artifact.get("question_history") or []),
        confirmed_requirements=list(artifact.get("confirmed_requirements") or []),
        denied_requirements=list(artifact.get("denied_requirements") or []),
        current_requirement_artifact=dict(artifact.get("requirement_artifact") or {}),
    )
    return _persist_continue_decision(root=root, user_text=user_text, decision=decision)


def apply_endpoint_clarification_decision(
    *,
    session_root: str | Path,
    user_text: str,
    decision: dict[str, Any],
) -> dict[str, Any]:
    root = _session_root(session_root)
    artifact_path = _artifact_path(root)
    if artifact_path.exists():
        normalized = normalize_continue_policy_decision(dict(decision))
        return _persist_continue_decision(root=root, user_text=user_text, decision=normalized)
    normalized = normalize_start_policy_decision(dict(decision))
    return _persist_start_decision(root=root, user_text=user_text, decision=normalized)


def handle_endpoint_clarification_turn(*, session_root: str | Path, user_text: str) -> dict[str, Any]:
    root = _session_root(session_root)
    artifact_path = _artifact_path(root)
    if not artifact_path.exists():
        return start_endpoint_clarification(session_root=root, user_text=user_text)
    artifact = dict(_read_json(artifact_path))
    if artifact.get("mode") == "VISION_COMPILER" and artifact.get("status") == "ACTIVE":
        return continue_endpoint_clarification(session_root=root, user_text=user_text)
    raise ValueError(
        "handle_endpoint_clarification_turn can only auto-continue an ACTIVE VISION_COMPILER session; "
        "terminal or bypassed sessions must hand off to downstream handling or a fresh session root"
    )


def load_endpoint_artifact(*, session_root: str | Path) -> dict[str, Any]:
    artifact_path = _artifact_path(_session_root(session_root))
    if not artifact_path.exists():
        raise ValueError(f"endpoint clarification artifact does not exist: {artifact_path}")
    return dict(_read_json(artifact_path))


def _print_turn_result(result: dict[str, Any]) -> None:
    print(f"Mode: {result['mode_decision']}")
    print(f"Status: {result['session_status']}")
    print(
        "Requirement artifact ready for persistence: "
        + ("yes" if result["requirement_artifact_ready_for_persistence"] else "no")
    )
    print(f"Endpoint artifact: {result['artifact_ref']}")
    print(f"Turn result: {result['turn_result_ref']}")
    print("Assistant:")
    print(str(result["assistant_message"]))


def _cmd_start(args: argparse.Namespace) -> int:
    result = start_endpoint_clarification(
        session_root=args.session_root,
        user_text=args.user_text,
    )
    _print_turn_result(result)
    return 0


def _cmd_continue(args: argparse.Namespace) -> int:
    result = continue_endpoint_clarification(
        session_root=args.session_root,
        user_text=args.user_text,
    )
    _print_turn_result(result)
    return 0


def _cmd_show_artifact(args: argparse.Namespace) -> int:
    artifact = load_endpoint_artifact(session_root=args.session_root)
    print(_canonical_json(artifact), end="")
    return 0


def _cmd_chat_turn(args: argparse.Namespace) -> int:
    result = handle_endpoint_clarification_turn(
        session_root=args.session_root,
        user_text=args.user_text,
    )
    _print_turn_result(result)
    return 0


def _load_supplied_decision(*, decision_file: str | None, decision_json: str | None) -> dict[str, Any]:
    if bool(decision_file) == bool(decision_json):
        raise ValueError("provide exactly one of --decision-file or --decision-json")
    if decision_file:
        obj = json.loads(Path(decision_file).read_text(encoding="utf-8"))
    else:
        obj = json.loads(str(decision_json))
    if not isinstance(obj, dict):
        raise ValueError("clarification decision must be a JSON object")
    return dict(obj)


def _normalize_text_list(values: list[str] | None) -> list[str]:
    return [str(item).strip() for item in list(values or []) if str(item).strip()]


def _build_question_decision(args: argparse.Namespace) -> dict[str, Any]:
    question = str(args.question or "").strip()
    if not question:
        raise ValueError("persist-question requires non-empty --question")
    summary = str(args.user_request_summary or "").strip()
    if not summary:
        raise ValueError("persist-question requires non-empty --user-request-summary")
    task_type = normalize_task_type_hint(
        getattr(args, "task_type", None),
        fallback_text=f"{summary} {getattr(args, 'user_text', '')}",
    )
    return {
        "task_type": task_type,
        "sufficient": False,
        "user_request_summary": summary,
        "final_effect": "",
        "observable_success_criteria": [],
        "hard_constraints": _normalize_text_list(getattr(args, "hard_constraint", None)),
        "non_goals": _normalize_text_list(getattr(args, "non_goal", None)),
        "relevant_context": _normalize_text_list(getattr(args, "relevant_context", None)),
        "open_questions": [question],
        "question_to_user": question,
        "confirmation_message": "",
        "artifact_ready_for_persistence": False,
    }


def _build_confirmation_decision(args: argparse.Namespace) -> dict[str, Any]:
    summary = str(args.user_request_summary or "").strip()
    if not summary:
        raise ValueError("persist-confirmation requires non-empty --user-request-summary")
    final_effect = str(args.final_effect or "").strip()
    if not final_effect:
        raise ValueError("persist-confirmation requires non-empty --final-effect")
    task_type = normalize_task_type_hint(
        getattr(args, "task_type", None),
        fallback_text=f"{summary} {final_effect} {getattr(args, 'user_text', '')}",
    )
    success_criteria = _normalize_text_list(getattr(args, "observable_success_criterion", None))
    if not success_criteria:
        raise ValueError("persist-confirmation requires at least one --observable-success-criterion")
    confirmation_message = str(args.confirmation_message or "").strip() or (
        "If the above understanding is correct, please confirm whether to proceed now."
    )
    return {
        "task_type": task_type,
        "sufficient": True,
        "user_request_summary": summary,
        "final_effect": final_effect,
        "observable_success_criteria": success_criteria,
        "hard_constraints": _normalize_text_list(getattr(args, "hard_constraint", None)),
        "non_goals": _normalize_text_list(getattr(args, "non_goal", None)),
        "relevant_context": _normalize_text_list(getattr(args, "relevant_context", None)),
        "open_questions": [],
        "question_to_user": "",
        "confirmation_message": confirmation_message,
        "artifact_ready_for_persistence": True,
    }


def _cmd_apply_decision(args: argparse.Namespace) -> int:
    result = apply_endpoint_clarification_decision(
        session_root=args.session_root,
        user_text=args.user_text,
        decision=_load_supplied_decision(
            decision_file=getattr(args, "decision_file", None),
            decision_json=getattr(args, "decision_json", None),
        ),
    )
    _print_turn_result(result)
    return 0


def _cmd_persist_question(args: argparse.Namespace) -> int:
    result = apply_endpoint_clarification_decision(
        session_root=args.session_root,
        user_text=args.user_text,
        decision=_build_question_decision(args),
    )
    _print_turn_result(result)
    return 0


def _cmd_persist_confirmation(args: argparse.Namespace) -> int:
    result = apply_endpoint_clarification_decision(
        session_root=args.session_root,
        user_text=args.user_text,
        decision=_build_confirmation_decision(args),
    )
    _print_turn_result(result)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    chat_turn = subparsers.add_parser(
        "chat-turn",
        help="Host-ready unified entrypoint: auto-start or auto-continue endpoint clarification for one user turn",
    )
    chat_turn.add_argument("--session-root", required=True)
    chat_turn.add_argument("--user-text", required=True)
    chat_turn.set_defaults(func=_cmd_chat_turn)

    apply_decision = subparsers.add_parser(
        "apply-decision",
        help="Persist a host-supplied clarification decision into the current session without invoking the default policy agent",
    )
    apply_decision.add_argument("--session-root", required=True)
    apply_decision.add_argument("--user-text", required=True)
    apply_decision.add_argument("--decision-file")
    apply_decision.add_argument("--decision-json")
    apply_decision.set_defaults(func=_cmd_apply_decision)

    persist_question = subparsers.add_parser(
        "persist-question",
        help="Persist one host-owned clarification question without hand-authoring an apply-decision JSON blob",
    )
    persist_question.add_argument("--session-root", required=True)
    persist_question.add_argument("--user-text", required=True)
    persist_question.add_argument("--task-type")
    persist_question.add_argument("--user-request-summary", required=True)
    persist_question.add_argument("--question", required=True)
    persist_question.add_argument("--hard-constraint", action="append", default=[])
    persist_question.add_argument("--non-goal", action="append", default=[])
    persist_question.add_argument("--relevant-context", action="append", default=[])
    persist_question.set_defaults(func=_cmd_persist_question)

    persist_confirmation = subparsers.add_parser(
        "persist-confirmation",
        help="Persist a host-owned clarified confirmation turn without hand-authoring an apply-decision JSON blob",
    )
    persist_confirmation.add_argument("--session-root", required=True)
    persist_confirmation.add_argument("--user-text", required=True)
    persist_confirmation.add_argument("--task-type")
    persist_confirmation.add_argument("--user-request-summary", required=True)
    persist_confirmation.add_argument("--final-effect", required=True)
    persist_confirmation.add_argument("--observable-success-criterion", action="append", default=[])
    persist_confirmation.add_argument("--hard-constraint", action="append", default=[])
    persist_confirmation.add_argument("--non-goal", action="append", default=[])
    persist_confirmation.add_argument("--relevant-context", action="append", default=[])
    persist_confirmation.add_argument("--confirmation-message")
    persist_confirmation.set_defaults(func=_cmd_persist_confirmation)

    start = subparsers.add_parser("start", help="Start a new endpoint-clarification session")
    start.add_argument("--session-root", required=True)
    start.add_argument("--user-text", required=True)
    start.set_defaults(func=_cmd_start)

    cont = subparsers.add_parser("continue", help="Continue an existing endpoint-clarification session")
    cont.add_argument("--session-root", required=True)
    cont.add_argument("--user-text", required=True)
    cont.set_defaults(func=_cmd_continue)

    show = subparsers.add_parser("show-artifact", help="Print the current endpoint artifact as JSON")
    show.add_argument("--session-root", required=True)
    show.set_defaults(func=_cmd_show_artifact)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
