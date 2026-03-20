#!/usr/bin/env python3
"""Legacy richer evaluator compatibility surface for the standalone LOOP product repo."""

from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import os
import re
import shlex
import shutil
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

if __package__ in (None, ""):
    from loop_product.agent_provider import apply_env_map, resolve_agent_invocation
    from loop_product.evaluator import role_launch as _role_launch
    from loop_product.run_cmd import finish_running_cmd, start_cmd, terminate_running_cmd
    from loop_product.runtime_paths import product_contract_path, product_repo_root, product_schema_path
else:
    from ..agent_provider import apply_env_map, resolve_agent_invocation
    from . import role_launch as _role_launch
    from ..run_cmd import finish_running_cmd, start_cmd, terminate_running_cmd
    from ..runtime_paths import product_contract_path, product_repo_root, product_schema_path

_STANDALONE_PRODUCT_REPO_ROOT = product_repo_root()
_ROOT = _STANDALONE_PRODUCT_REPO_ROOT
_INPUT_SCHEMA_PATH = product_schema_path("LoopEvaluatorPrototypeInput.schema.json")
_REPORT_SCHEMA_PATH = product_schema_path("LoopEvaluatorPrototypeReport.schema.json")
_SUPERVISOR_SCHEMA_PATH = product_schema_path("LoopEvaluatorPrototypeSupervisorReport.schema.json")
_FINAL_EFFECTS_DOC_PATH = product_contract_path("LOOP_EVALUATOR_PROTOTYPE_FINAL_EFFECTS.md")
_PRODUCT_MANUAL_DOC_PATH = product_contract_path("LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md")
_STANDALONE_FINAL_EFFECTS_DOC_PATH = _FINAL_EFFECTS_DOC_PATH
_STANDALONE_PRODUCT_MANUAL_DOC_PATH = _PRODUCT_MANUAL_DOC_PATH
_DEFAULT_SELF_EVAL_OUTER_FINAL_EFFECTS_TEXT = """# Bounded Live Self-Eval Acceptance Surface

- The packaged evaluator self-eval uses session-local manuals and fixture requests that stay runnable from staged workspaces, and the dedicated positive-path fixture reaches a truthful terminal PASS.

- The dedicated malformed-reviewer and missing-evidence fixtures are rejected fail-closed.

- The dedicated supervision fixtures report confirmed BUG or STUCK instead of pretending the child evaluator completed.

- Shared-repo architecture claims are supported by reviewer-visible repo-local evidence, including validation that repo-shipped self-eval inputs stay on the product side of the LeanAtlas adapter boundary, and the live self-eval stays serialized within the documented nested-capacity budget.
"""
_DEFAULT_SELF_EVAL_POSITIVE_FIXTURE_FINAL_EFFECTS_TEXT = """# Bounded Positive Fixture Acceptance Surface

- The current positive-path evaluator run launches the checker, ordinary tests, and the active AI-as-User evaluation unit from code using the product manual plus final-effects text as inputs, and that launch stays visible from the staged control surface plus the bounded current-run snapshot. The same positive-path run preserves stage-visible evidence that later review can use to decide terminal PASS truthfully, without requiring the in-flight run to inspect a terminal EvaluationReport.json before that report exists.

- In the current positive-path evaluator run, when the raw final-effects text is repairable, the checker auto-repairs it into normalized requirements and a requirement graph instead of stopping early.
"""
_ROLE_IDS = ("checker", "test_designer", "ai_user", "reviewer")
_REASONING_EFFORTS = {"minimal", "low", "medium", "high", "xhigh"}
_TERMINAL_EVALUATION_STATUSES = {"PASS", "FAIL", "INCONCLUSIVE", "ERROR"}
_SUPERVISOR_STATUSES = {"TERMINAL", "BUG", "STUCK"}
_FILE_REF_LINE_SUFFIX = re.compile(r"^(?P<path>.+?):(?P<line>\d+)(?::(?P<column>\d+))?$")
_OPERATION_LOG_STEP_REF = re.compile(r"^(?:op|operation)\s*[-_#: ]?\s*(?P<index>\d+)$", re.IGNORECASE)
_MANUAL_LINE_REF = re.compile(r"^manual[_ ]line[_ ](?P<line>\d+)$", re.IGNORECASE)
_MANUAL_LINES_REF = re.compile(r"^manual[_ ]lines[_ ](?P<start>\d+)[_ -]+(?P<end>\d+)$", re.IGNORECASE)
_RUNTIME_BANNER_RE = re.compile(r"^(provider|model|reasoning[ _-]?effort)\s*:\s*(?P<value>.+?)\s*$", re.IGNORECASE)
_PACKAGED_SELF_EVAL_GUARD_ENV = "LOOP_PRODUCT_EVAL_PACKAGED_SELF_EVAL_DEPTH"
_PACKAGED_SELF_EVAL_GUARD_UNCHANGED = object()
_RUNTIME_TEMP_ROOT = Path(tempfile.gettempdir()).resolve() / "loop_product" / "tmp"
_AI_USER_DEFAULT_EXCLUDED_RELPATHS = (
    Path(".loop"),
    Path(".loop_runtime"),
    Path(".git"),
    Path(".lake"),
    Path(".uv-cache"),
    Path(".venv"),
    Path("artifacts"),
    Path("workspace"),
    Path(".cache") / "loop_product",
    Path(".cache") / "loop_evaluator_self_eval" / "fixture_output",
    Path(".cache") / "loop_evaluator_self_eval" / "generated_tests",
)
_NESTED_HEAVYWEIGHT_DIR_NAMES = frozenset({".lake", ".git", ".venv", ".uv-cache"})
_OPERATION_LOG_TEXT_STOPWORDS = {
    "a",
    "an",
    "after",
    "and",
    "as",
    "at",
    "by",
    "for",
    "from",
    "in",
    "inside",
    "into",
    "of",
    "on",
    "or",
    "the",
    "to",
    "under",
    "visible",
    "with",
}
_OPERATION_LOG_LABEL_DROP_TOKENS = {"positive", "negative", "path", "run", "check", "result", "report"}
_MANUAL_LABEL_DROP_TOKENS = {"expectation", "manual", "product", "specific"}


class CheckerHumanGateError(ValueError):
    """Raised when checker normalization cannot continue without human clarification."""

    def __init__(self, reasons: Sequence[str]):
        self.human_gate_reasons = [str(item) for item in reasons if str(item).strip()]
        detail = "; ".join(self.human_gate_reasons) if self.human_gate_reasons else "checker could not repair the goal text safely"
        super().__init__(f"checker reached human gate: {detail}")


class AIUserBatchError(RuntimeError):
    """Raised when one or more AI-as-User evaluation-unit runs fail after partial progress."""

    def __init__(
        self,
        *,
        errors: Sequence[tuple[str, Exception]],
        partial_runs: Sequence[Mapping[str, Any]],
        scheduler: Mapping[str, Any],
    ):
        self.errors = [(str(requirement_id), exc) for requirement_id, exc in errors]
        self.partial_runs = [dict(item) for item in partial_runs]
        self.scheduler = dict(scheduler)
        first_requirement_id, first_exc = self.errors[0]
        self.first_requirement_id = first_requirement_id
        self.first_exception = first_exc
        detail = "; ".join(f"{rid}: {exc}" for rid, exc in self.errors)
        super().__init__(f"AI-as-User batch failed after partial progress: {detail}")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _safe_role_runtime_id(role_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "__", role_id).strip("._-") or "role"


def _default_role_runtime_root(*, run_root: Path, role_id: str) -> Path:
    return (run_root / ".loop" / _safe_role_runtime_id(role_id)).resolve()


def _default_role_workspace_root(*, role_root: Path) -> Path:
    return (Path(role_root).resolve() / "workspace").resolve()


def _default_role_unit_workspace_root(*, role_root: Path, unit_id: str) -> Path:
    return (Path(role_root).resolve() / "workspaces" / _safe_role_runtime_id(unit_id) / "workspace").resolve()


def _role_runtime_artifact_root(role_root: Path, role_instance_id: str) -> Path:
    return Path(role_root).resolve() / "runs" / _safe_role_runtime_id(role_instance_id)


def _write_session_local_self_eval_text(path: Path, text: str) -> Path:
    normalized = str(text).rstrip() + "\n"
    _write_text(path, normalized)
    return path


def _is_canonical_evaluator_final_effects_path(path: Path) -> bool:
    resolved = Path(path).resolve()
    return resolved in {
        _FINAL_EFFECTS_DOC_PATH.resolve(),
        _STANDALONE_FINAL_EFFECTS_DOC_PATH.resolve(),
    }


def _sanitize_self_eval_manual_text(*, text: str, workspace_root: Path) -> str:
    sanitized = str(text)
    absolute_python = str((workspace_root.resolve() / ".venv" / "bin" / "python3").resolve())
    if absolute_python:
        sanitized = sanitized.replace(absolute_python, "python")
    workspace_prefix = str(workspace_root.resolve()) + "/"
    sanitized = sanitized.replace(workspace_prefix, "")
    sanitized = re.sub(
        r"\nAbsolute materialized path for [^\n]+:\n\n`[^`]+`\n\n",
        "\n",
        sanitized,
    )
    sanitized = sanitized.replace(
        "This session-local direct-interpreter command uses the already provisioned Python interpreter for the current run, "
        "so the self-eval measures evaluator behavior instead of `uv` environment bootstrap noise.\n\n",
        "",
    )
    return sanitized


def _preferred_workspace_python(workspace_root: Path) -> Path | None:
    workspace_root = Path(workspace_root)
    resolved_workspace_root = workspace_root.resolve()
    current_python = Path(sys.executable)
    if current_python.is_absolute() and current_python.exists() and _is_within(current_python, resolved_workspace_root):
        return current_python
    for candidate in (
        workspace_root / ".venv" / "bin" / "python3",
        workspace_root / ".venv" / "bin" / "python",
    ):
        if candidate.exists():
            return candidate
    return None


def _preferred_workspace_python_command(workspace_root: Path) -> str:
    workspace_root = Path(workspace_root)
    preferred = _preferred_workspace_python(workspace_root)
    if preferred is None:
        return "python"
    root_expr = '"${LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT:-.}"'
    try:
        rel = preferred.relative_to(workspace_root)
    except ValueError:
        try:
            rel = preferred.relative_to(workspace_root.resolve())
        except ValueError:
            return shlex.quote(str(preferred))
    rel_text = rel.as_posix()
    if rel_text in {"", "."}:
        return root_expr
    return f"{root_expr}/{shlex.quote(rel_text)}"


def _normalize_generated_test_python_argv(*, argv: Sequence[str], workspace_root: Path) -> list[str]:
    normalized = [str(arg) for arg in argv]
    if not normalized or normalized[0] not in {"python", "python3"}:
        return normalized
    preferred = _preferred_workspace_python(workspace_root)
    if preferred is None:
        return normalized
    normalized[0] = str(preferred)
    return normalized


def _ordinary_test_command_tokens(argv: Sequence[str]) -> list[str]:
    tokens = [str(arg).strip() for arg in argv if str(arg).strip()]
    if len(tokens) < 3:
        return tokens
    shell_name = Path(tokens[0]).name.lower()
    if shell_name not in {"bash", "sh", "zsh"} or tokens[1] not in {"-c", "-lc"}:
        return tokens
    try:
        inner_tokens = [str(arg).strip() for arg in shlex.split(tokens[2]) if str(arg).strip()]
    except ValueError:
        return tokens
    return inner_tokens or tokens


def _evidence_ref_path_key(ref: str) -> str:
    path_part, _suffix = _split_evidence_ref(ref)
    if not path_part:
        return ""
    return str(Path(path_part).resolve(strict=False))


def _rebase_request_ref_into_workspace(
    *,
    raw_ref: str | Path,
    original_workspace_root: Path,
    target_workspace_root: Path,
    support_dir: Path,
) -> str:
    resolved = _resolve_path(original_workspace_root, raw_ref)
    if _is_within(resolved, original_workspace_root):
        return str(resolved.relative_to(original_workspace_root))
    suffix = "".join(resolved.suffixes)
    stem = resolved.name[: -len(suffix)] if suffix else resolved.name
    digest = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:12]
    copied = support_dir / f"{stem}__{digest}{suffix}"
    _copy_file_if_needed(source=resolved, dest=copied)
    return str(copied.relative_to(target_workspace_root))


def _self_eval_ref_needs_session_materialization(
    *,
    resolved_ref: Path,
    original_workspace_root: Path,
) -> bool:
    if not _is_within(resolved_ref, original_workspace_root):
        return True
    rel = resolved_ref.relative_to(original_workspace_root)
    # Generated self-eval fixture requests must keep repo-shipped support refs
    # under `.cache/loop_evaluator_self_eval/inputs/**` so standalone proof
    # commands remain valid even when the source request points at repo-local docs.
    return tuple(rel.parts[:3]) != (".cache", "loop_evaluator_self_eval", "inputs")


def _materialize_self_eval_request_ref(
    *,
    raw_ref: str | Path,
    original_workspace_root: Path,
    target_workspace_root: Path,
    support_dir: Path,
) -> str:
    resolved = _resolve_path(original_workspace_root, raw_ref)
    if _self_eval_ref_needs_session_materialization(
        resolved_ref=resolved,
        original_workspace_root=original_workspace_root,
    ):
        suffix = "".join(resolved.suffixes)
        stem = resolved.name[: -len(suffix)] if suffix else resolved.name
        digest = hashlib.sha256(str(resolved).encode("utf-8")).hexdigest()[:12]
        copied = support_dir / f"{stem}__{digest}{suffix}"
        _copy_file_if_needed(source=resolved, dest=copied)
        return str(copied.relative_to(target_workspace_root))
    return str(resolved.relative_to(original_workspace_root))


def _rewrite_self_eval_agent_cmd(
    *,
    raw_cmd: str,
    source_workspace_anchor_env: str = "LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT",
) -> str:
    command = str(raw_cmd or "").strip()
    if not command:
        return command
    anchor_prefix = f"${{{source_workspace_anchor_env}}}/.cache/loop_evaluator_self_eval/"
    if anchor_prefix in command:
        return command
    rel_prefix = ".cache/loop_evaluator_self_eval/"
    command = re.sub(
        rf"(^|[\s=])'{re.escape(rel_prefix)}([^']*)'",
        lambda match: f'{match.group(1)}"{anchor_prefix}{match.group(2)}"',
        command,
    )
    command = re.sub(
        rf'(^|[\s=])"{re.escape(rel_prefix)}([^"]*)"',
        lambda match: f'{match.group(1)}"{anchor_prefix}{match.group(2)}"',
        command,
    )
    return re.sub(
        rf"(^|[\s=]){re.escape(rel_prefix)}([^\s'\"`]+)",
        lambda match: f"{match.group(1)}{anchor_prefix}{match.group(2)}",
        command,
    )


def _extract_self_eval_agent_cmd_relpaths(raw_cmd: str) -> list[str]:
    command = str(raw_cmd or "").strip()
    if not command:
        return []
    try:
        tokens = shlex.split(command)
    except ValueError:
        return []
    relpaths: list[str] = []
    seen: set[str] = set()
    for token in tokens:
        candidate = token
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", token):
            _key, candidate = token.split("=", 1)
        candidate = str(candidate).strip()
        if not candidate.startswith(".cache/loop_evaluator_self_eval/"):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        relpaths.append(candidate)
    return relpaths


def _materialize_self_eval_agent_cmd_support_files(
    *,
    raw_cmd: str,
    original_workspace_root: Path,
    target_workspace_root: Path,
    fixture_request_path: Path,
) -> None:
    relpaths = _extract_self_eval_agent_cmd_relpaths(raw_cmd)
    if not relpaths:
        return
    candidate_roots: list[Path] = []
    for root in (
        original_workspace_root.resolve(),
        fixture_request_path.resolve().parent,
        *fixture_request_path.resolve().parents,
        target_workspace_root.resolve().parent,
        *target_workspace_root.resolve().parents,
    ):
        resolved = Path(root).resolve()
        if resolved not in candidate_roots:
            candidate_roots.append(resolved)
    for relpath in relpaths:
        dest = (target_workspace_root / relpath).resolve()
        if dest.exists():
            continue
        source: Path | None = None
        for base in candidate_roots:
            candidate = (base / relpath).resolve()
            if candidate.exists() and candidate.is_file():
                source = candidate
                break
        if source is None:
            continue
        _copy_file_if_needed(source=source, dest=dest)


def _fresh_session_run_id() -> str:
    return f"{_utc_stamp()}__{time.time_ns()}"


def _materialize_self_eval_support_agent(path: Path) -> None:
    script = """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\\n", encoding="utf-8")


def main() -> int:
    role = str(os.environ.get("LOOP_PRODUCT_EVAL_ROLE") or "").strip()
    if role != "reviewer":
        raise SystemExit(f"self-eval support agent only supports reviewer role, got: {role or '<missing>'}")
    context = json.loads(Path(os.environ["LOOP_PRODUCT_EVAL_CONTEXT_PATH"]).read_text(encoding="utf-8"))
    result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
    response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])

    effect_reviews = []
    for run in context.get("ai_user_results") or []:
        operation_log_ref = str(run.get("operation_log_ref") or "")
        effect_results = list(run.get("effect_results") or [])
        if not effect_results and run.get("effect_result"):
            effect_results = [dict(run["effect_result"])]
        for effect in effect_results:
            effect_reviews.append(
                {
                    "requirement_id": str(effect["requirement_id"]),
                    "verdict": "PASS",
                    "evidence_refs": [
                        operation_log_ref,
                        *[str(ref) for ref in (effect.get("evidence_refs") or [])],
                    ],
                }
            )

    _write_json(
        result_path,
        {
            "status": "OK",
            "summary": {
                "kind": "negative_path_fixture",
                "text": "Intentionally malformed reviewer output for fail-closed validation.",
            },
            "effect_reviews": effect_reviews,
        },
    )
    response_path.write_text(
        "# Reviewer\\n\\nIntentionally omitted `reproduction_steps` so the evaluator must fail closed.\\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
    _write_text(path, script)
    path.chmod(0o755)


def _materialize_self_eval_missing_evidence_agent(path: Path) -> None:
    script = """#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\\n", encoding="utf-8")


def main() -> int:
    role = str(os.environ.get("LOOP_PRODUCT_EVAL_ROLE") or "").strip()
    if role != "reviewer":
        raise SystemExit(f"self-eval missing-evidence agent only supports reviewer role, got: {role or '<missing>'}")
    context = json.loads(Path(os.environ["LOOP_PRODUCT_EVAL_CONTEXT_PATH"]).read_text(encoding="utf-8"))
    result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
    response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])

    effect_reviews = []
    for run in context.get("ai_user_results") or []:
        effect_results = list(run.get("effect_results") or [])
        if not effect_results and run.get("effect_result"):
            effect_results = [dict(run["effect_result"])]
        for effect in effect_results:
            effect_reviews.append(
                {
                    "requirement_id": str(effect["requirement_id"]),
                    "verdict": "PASS",
                    "reproduction_steps": [
                        "1. Inspect the cited AI-as-User run.",
                        "2. Replay the visible CLI operations.",
                    ],
                }
            )

    _write_json(
        result_path,
        {
            "status": "OK",
            "summary": {
                "kind": "negative_path_fixture",
                "text": "Intentionally malformed reviewer output with missing evidence refs for fail-closed validation.",
            },
            "effect_reviews": effect_reviews,
        },
    )
    response_path.write_text(
        "# Reviewer\\n\\nIntentionally omitted `evidence_refs` so the evaluator must fail closed.\\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
    _write_text(path, script)
    path.chmod(0o755)


def _materialize_self_eval_supervisor_bug_agent(path: Path) -> None:
    script = """#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import time


def main() -> int:
    target = os.getppid()
    if target is None or target <= 1:
        raise SystemExit("unable to resolve evaluator parent pid for bug fixture")
    os.kill(target, signal.SIGKILL)
    time.sleep(0.2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
    _write_text(path, script)
    path.chmod(0o755)


def _materialize_self_eval_supervisor_stuck_agent(path: Path) -> None:
    script = """#!/usr/bin/env python3
from __future__ import annotations

import time


def main() -> int:
    time.sleep(600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""
    _write_text(path, script)
    path.chmod(0o755)


def _materialize_self_eval_adapter_snapshot(*, source_root: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for relpath in (
        Path("tools") / "loop" / "evaluator_prototype.py",
        Path("tools") / "loop" / "endpoint_clarification.py",
    ):
        source_path = source_root / relpath
        if not source_path.exists():
            raise FileNotFoundError(source_path)
        destination = target_dir / relpath.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, destination)


def _role_output_envelope_schema(role_id: str) -> dict[str, Any]:
    requirement_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["requirement_id", "description", "blocking", "requirement_kind"],
        "properties": {
            "requirement_id": {"minLength": 1, "type": "string"},
            "description": {"minLength": 1, "type": "string"},
            "blocking": {"type": "boolean"},
            "requirement_kind": {
                "type": "string",
                "enum": ["product_effect", "runtime_closure"],
            },
        },
    }
    evaluation_unit_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["unit_id", "requirement_ids"],
        "properties": {
            "unit_id": {"minLength": 1, "type": "string"},
            "requirement_ids": {"type": "array", "items": {"type": "string"}},
        },
    }
    dependency_edge_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["from_unit_id", "to_unit_id"],
        "properties": {
            "from_unit_id": {"minLength": 1, "type": "string"},
            "to_unit_id": {"minLength": 1, "type": "string"},
        },
    }
    repair_action_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["raw_item", "action", "emitted_requirement_ids"],
        "properties": {
            "raw_item": {"minLength": 1, "type": "string"},
            "action": {"minLength": 1, "type": "string"},
            "emitted_requirement_ids": {"type": "array", "items": {"type": "string"}},
        },
    }
    ordinary_test_result_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["test_id", "test_kind", "argv", "passed", "exit_code", "stdout_ref", "stderr_ref", "exec_span_ref"],
        "properties": {
            "test_id": {"minLength": 1, "type": "string"},
            "test_kind": {"minLength": 1, "type": "string"},
            "argv": {"type": "array", "items": {"type": "string"}},
            "passed": {"type": "boolean"},
            "exit_code": {"type": "integer"},
            "stdout_ref": {"minLength": 1, "type": "string"},
            "stderr_ref": {"minLength": 1, "type": "string"},
            "exec_span_ref": {"minLength": 1, "type": "string"},
        },
    }
    effect_result_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["requirement_id", "outcome", "summary", "evidence_refs"],
        "properties": {
            "requirement_id": {"minLength": 1, "type": "string"},
            "outcome": {"minLength": 1, "type": "string"},
            "summary": {"minLength": 1, "type": "string"},
            "evidence_refs": {"type": "array", "items": {"type": "string"}},
        },
    }
    effect_review_item = {
        "type": "object",
        "additionalProperties": False,
        "required": ["requirement_id", "verdict", "evidence_refs", "reproduction_steps"],
        "properties": {
            "requirement_id": {"minLength": 1, "type": "string"},
            "verdict": {"minLength": 1, "type": "string"},
            "evidence_refs": {"type": "array", "items": {"type": "string"}},
            "reproduction_steps": {"type": "array", "items": {"type": "string"}},
        },
    }
    base_summary = {
        "summary_markdown": {
            "minLength": 1,
            "type": "string",
        }
    }
    if role_id == "checker":
        return {
            "type": "object",
            "required": ["result", "summary_markdown"],
            "additionalProperties": False,
            "properties": {
                "result": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "status",
                        "normalized_requirements",
                        "requirement_graph",
                        "repair_actions",
                        "human_gate_reasons",
                        "notes",
                    ],
                    "properties": {
                        "status": {"type": "string", "enum": ["OK", "HUMAN_GATE"]},
                        "normalized_requirements": {"type": "array", "items": requirement_item},
                        "requirement_graph": {
                            "anyOf": [
                                {
                                    "type": "object",
                                    "additionalProperties": False,
                                    "required": ["requirements", "evaluation_units", "dependency_edges"],
                                    "properties": {
                                        "requirements": {"type": "array", "items": requirement_item},
                                        "evaluation_units": {"type": "array", "items": evaluation_unit_item},
                                        "dependency_edges": {"type": "array", "items": dependency_edge_item},
                                    },
                                },
                                {"type": "null"},
                            ],
                        },
                        "repair_actions": {"type": "array", "items": repair_action_item},
                        "human_gate_reasons": {"type": "array", "items": {"type": "string"}},
                        "notes": {"type": "array", "items": {"type": "string"}},
                    },
                },
                **base_summary,
            },
        }
    if role_id == "test_designer":
        return {
            "type": "object",
            "required": ["result", "summary_markdown"],
            "additionalProperties": False,
            "properties": {
                "result": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["status", "ordinary_test_results", "notes"],
                    "properties": {
                        "status": {"type": "string", "const": "OK"},
                        "ordinary_test_results": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["all_passed", "executed_tests"],
                            "properties": {
                                "all_passed": {"type": "boolean"},
                                "executed_tests": {"type": "array", "items": ordinary_test_result_item},
                            },
                        },
                        "notes": {"type": "array", "items": {"type": "string"}},
                    },
                },
                **base_summary,
            },
        }
    if role_id == "ai_user":
        return {
            "type": "object",
            "required": ["result", "summary_markdown", "operation_log_markdown"],
            "additionalProperties": False,
            "properties": {
                "result": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["status", "unit_id", "effect_results", "notes"],
                    "properties": {
                        "status": {"type": "string", "const": "OK"},
                        "unit_id": {"minLength": 1, "type": "string"},
                        "effect_results": {"type": "array", "items": effect_result_item},
                        "notes": {"type": "array", "items": {"type": "string"}},
                    },
                },
                **base_summary,
                "operation_log_markdown": {
                    "minLength": 1,
                    "type": "string",
                },
            },
        }
    if role_id == "reviewer":
        return {
            "type": "object",
            "required": ["result", "summary_markdown"],
            "additionalProperties": False,
            "properties": {
                "result": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["status", "effect_reviews"],
                    "properties": {
                        "status": {"type": "string", "const": "OK"},
                        "effect_reviews": {"type": "array", "items": effect_review_item},
                    },
                },
                **base_summary,
            },
        }
    raise ValueError(f"unsupported structured-output role: {role_id}")


def _materialize_structured_role_outputs(
    *,
    role_id: str,
    envelope_obj: Mapping[str, Any],
    result_path: Path,
    response_path: Path,
    operation_log_path: Path | None = None,
    raw_response_path: Path | None = None,
) -> dict[str, Any]:
    result_obj = dict(envelope_obj.get("result") or {})
    summary_markdown = str(envelope_obj.get("summary_markdown") or "").strip()
    if not summary_markdown:
        raise ValueError(f"{role_id} structured output must include summary_markdown")
    if role_id == "ai_user":
        if operation_log_path is None:
            raise ValueError("AI-as-User structured output requires operation_log_path")
        operation_log_markdown = str(envelope_obj.get("operation_log_markdown") or "").strip()
        if not operation_log_markdown:
            raise ValueError("AI-as-User structured output must include operation_log_markdown")
        _write_text(operation_log_path, operation_log_markdown.rstrip() + "\n")
        result_obj.setdefault("operation_log_ref", str(operation_log_path))
    _write_json(result_path, result_obj)
    _write_text(response_path, summary_markdown.rstrip() + "\n")
    if raw_response_path is not None and raw_response_path.exists():
        raw_copy_path = response_path.parent / "structured_response.json"
        _copy_file_if_needed(source=raw_response_path, dest=raw_copy_path)
    return result_obj


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _copy_tree_excluding(*, source_root: Path, dest_root: Path, excluded_paths: Sequence[Path]) -> None:
    source_root = source_root.resolve()
    dest_root = dest_root.resolve()
    if dest_root.exists():
        shutil.rmtree(dest_root)
    normalized_excluded = [
        path.resolve()
        for path in excluded_paths
        if _is_within(path.resolve(), source_root)
    ]

    def _ignore(dirpath: str, names: list[str]) -> set[str]:
        current = Path(dirpath).resolve()
        ignored: set[str] = set()
        for name in names:
            if name in {"AGENTS.md", "AGENTS.override.md"}:
                ignored.add(name)
                continue
            candidate = (current / name).resolve()
            if any(candidate == blocked or _is_within(candidate, blocked) for blocked in normalized_excluded):
                ignored.add(name)
        return ignored

    shutil.copytree(source_root, dest_root, ignore=_ignore)


def _copy_file_if_needed(*, source: Path, dest: Path) -> None:
    if source.resolve() == dest.resolve():
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)


def _copy_path_if_present(*, source: Path, dest: Path) -> None:
    if not source.exists():
        return
    if source.is_dir():
        if dest.exists():
            if dest.is_dir():
                shutil.rmtree(dest)
            else:
                dest.unlink()
        shutil.copytree(source, dest)
        return
    if dest.exists() and dest.is_dir():
        shutil.rmtree(dest)
    _copy_file_if_needed(source=source, dest=dest)


def _clear_run_root_artifacts(*, run_root: Path, preserve_paths: Sequence[Path] = ()) -> None:
    if not run_root.exists():
        return
    preserved = {Path(path).resolve() for path in preserve_paths if Path(path).exists()}
    managed_relpaths = (
        Path("EvaluationReport.json"),
        Path("SupervisorReport.json"),
        Path("SupervisorProgress.log"),
        Path("order.log"),
        Path("input_snapshot.json"),
        Path("lanes"),
        Path(".loop"),
        Path("roles"),
        Path("ordinary_tests"),
        Path(".loop_evaluator_internal"),
        Path("supervisor_child.stdout.txt"),
        Path("supervisor_child.stderr.txt"),
    )
    for relpath in managed_relpaths:
        candidate = run_root / relpath
        if not candidate.exists():
            continue
        resolved = candidate.resolve()
        if resolved in preserved:
            continue
        if candidate.is_dir():
            shutil.rmtree(candidate)
        else:
            candidate.unlink()


def _materialize_current_run_snapshot(*, run_root: Path, snapshot_root: Path) -> Path:
    if snapshot_root.exists():
        shutil.rmtree(snapshot_root)
    for relpath in (
        Path("order.log"),
        Path("input_snapshot.json"),
        Path("SupervisorProgress.log"),
        Path("SupervisorRequest.json"),
    ):
        _copy_path_if_present(source=run_root / relpath, dest=snapshot_root / relpath)
    for relpath in (
        Path(".loop") / "checker" / "runs",
        Path(".loop") / "test_ai" / "runs",
        Path(".loop") / "ai_user" / "runs",
        Path("roles") / "checker",
        Path("roles") / "test_designer",
        Path("roles") / "ai_user",
        Path("ordinary_tests"),
    ):
        _copy_path_if_present(source=run_root / relpath, dest=snapshot_root / relpath)
    return snapshot_root


def _default_ai_user_excluded_paths(*, source_workspace_root: Path, output_root: Path) -> list[Path]:
    excluded: list[Path] = []
    if _is_within(output_root, source_workspace_root):
        excluded.append(output_root)
    for relpath in _AI_USER_DEFAULT_EXCLUDED_RELPATHS:
        candidate = (source_workspace_root / relpath).resolve()
        if _is_within(candidate, source_workspace_root):
            excluded.append(candidate)
    self_eval_inputs_root = (source_workspace_root / ".cache" / "loop_evaluator_self_eval").resolve()
    cache_root = (source_workspace_root / ".cache").resolve()
    if cache_root.exists():
        for child in cache_root.iterdir():
            child_resolved = child.resolve()
            if child_resolved == self_eval_inputs_root:
                continue
            if _is_within(child_resolved, source_workspace_root):
                excluded.append(child_resolved)
    for child in source_workspace_root.iterdir():
        child_resolved = child.resolve()
        if not child_resolved.is_dir() or not _is_within(child_resolved, source_workspace_root):
            continue
        for relpath in (Path(".cache"), Path("artifacts")):
            candidate = (child_resolved / relpath).resolve()
            if candidate.exists() and _is_within(candidate, source_workspace_root):
                excluded.append(candidate)
    excluded.extend(
        _nested_heavyweight_excluded_paths(
            source_workspace_root=source_workspace_root,
            excluded_paths=excluded,
        )
    )
    return excluded


def _nested_heavyweight_excluded_paths(
    *,
    source_workspace_root: Path,
    excluded_paths: Sequence[Path],
) -> list[Path]:
    source_workspace_root = source_workspace_root.resolve()
    blocked_paths = [
        path.resolve()
        for path in excluded_paths
        if _is_within(path.resolve(), source_workspace_root)
    ]
    seen_blocked = {path for path in blocked_paths}
    discovered: list[Path] = []
    for dirpath, dirnames, _filenames in os.walk(source_workspace_root, topdown=True):
        current = Path(dirpath).resolve()
        if any(current == blocked or _is_within(current, blocked) for blocked in blocked_paths):
            dirnames[:] = []
            continue
        dirnames.sort()
        retained: list[str] = []
        for name in dirnames:
            candidate = (current / name).resolve()
            if any(candidate == blocked or _is_within(candidate, blocked) for blocked in blocked_paths):
                continue
            if name in _NESTED_HEAVYWEIGHT_DIR_NAMES:
                if candidate not in seen_blocked:
                    blocked_paths.append(candidate)
                    seen_blocked.add(candidate)
                    discovered.append(candidate)
                continue
            retained.append(name)
        dirnames[:] = retained
    return discovered


def _materialize_ai_user_workspace(
    *,
    request: Mapping[str, Any],
    run_root: Path,
    unit_id: str,
    manual_path: Path,
) -> dict[str, Path]:
    source_workspace_root = Path(str(request["workspace_root"])).resolve()
    output_root = Path(str(request["output_root"])).resolve()
    role_root = _default_role_runtime_root(run_root=run_root, role_id="ai_user")
    staging_root = _default_role_unit_workspace_root(role_root=role_root, unit_id=unit_id).parent.resolve()
    workspace_copy_root = staging_root / "workspace"
    excluded_paths = _default_ai_user_excluded_paths(
        source_workspace_root=source_workspace_root,
        output_root=output_root,
    )
    _copy_tree_excluding(
        source_root=source_workspace_root,
        dest_root=workspace_copy_root,
        excluded_paths=excluded_paths,
    )
    shared_snapshot_root = run_root / ".loop_evaluator_internal" / "current_run_snapshot"
    if not shared_snapshot_root.exists():
        _materialize_current_run_snapshot(
            run_root=run_root,
            snapshot_root=shared_snapshot_root,
        )
    current_run_snapshot_root = _materialize_current_run_snapshot(
        run_root=run_root,
        snapshot_root=workspace_copy_root / ".loop_evaluator_internal" / "current_run_snapshot",
    )

    if _is_within(manual_path.resolve(), source_workspace_root):
        staged_manual_path = workspace_copy_root / manual_path.resolve().relative_to(source_workspace_root)
    else:
        staged_manual_path = workspace_copy_root / ".loop_evaluator_internal" / "inputs" / manual_path.name
        _copy_file_if_needed(source=manual_path, dest=staged_manual_path)

    internal_root = workspace_copy_root / ".loop_evaluator_internal" / unit_id
    control_dir = internal_root / "control"
    dropbox_dir = internal_root / "dropbox"
    scratch_dir = internal_root / "scratch"
    return {
        "workspace_root": workspace_copy_root,
        "manual_path": staged_manual_path,
        "control_dir": control_dir,
        "dropbox_dir": dropbox_dir,
        "scratch_dir": scratch_dir,
        "current_run_snapshot_root": current_run_snapshot_root,
        "prompt_path": control_dir / "prompt.md",
        "context_path": control_dir / "context.json",
        "result_path": dropbox_dir / "result.json",
        "response_path": dropbox_dir / "response.md",
        "operation_log_path": dropbox_dir / "operation_log.md",
    }


def _materialize_codex_role_workspace(
    *,
    request: Mapping[str, Any],
    run_root: Path,
    role_id: str,
    copy_source_workspace: bool,
) -> dict[str, Path]:
    source_workspace_root = Path(str(request["workspace_root"])).resolve()
    output_root = Path(str(request["output_root"])).resolve()
    safe_role_id = re.sub(r"[^A-Za-z0-9_.-]+", "__", role_id).strip("._-") or "role"
    role_root = _default_role_runtime_root(run_root=run_root, role_id=role_id)
    workspace_root = _default_role_workspace_root(role_root=role_root)
    if copy_source_workspace:
        excluded_paths = _default_ai_user_excluded_paths(
            source_workspace_root=source_workspace_root,
            output_root=output_root,
        )
        _copy_tree_excluding(
            source_root=source_workspace_root,
            dest_root=workspace_root,
            excluded_paths=excluded_paths,
        )
    else:
        if workspace_root.exists():
            shutil.rmtree(workspace_root)
        workspace_root.mkdir(parents=True, exist_ok=True)
    current_run_snapshot_root = _materialize_current_run_snapshot(
        run_root=run_root,
        snapshot_root=workspace_root / ".loop_evaluator_internal" / "current_run_snapshot",
    )
    ordinary_test_artifact_root = workspace_root / ".loop_evaluator_internal" / safe_role_id / "ordinary_tests"
    return {
        "workspace_root": workspace_root,
        "current_run_snapshot_root": current_run_snapshot_root,
        "ordinary_test_artifact_root": ordinary_test_artifact_root,
    }


def _materialize_staged_role_context_ref(
    *,
    staged_workspace_root: Path,
    source_workspace_root: Path,
    source_path: Path,
    label: str,
) -> Path:
    resolved_source = source_path.resolve()
    staged_root = staged_workspace_root.resolve()
    source_root = source_workspace_root.resolve()
    if _is_within(resolved_source, source_root):
        candidate = staged_root / resolved_source.relative_to(source_root)
        if candidate.exists():
            return candidate
    staged_input_path = staged_root / ".loop_evaluator_internal" / "inputs" / f"{label}__{resolved_source.name}"
    _copy_file_if_needed(source=resolved_source, dest=staged_input_path)
    return staged_input_path


def _validate_with_schema(*, obj: Any, schema_path: Path, label: str) -> None:
    try:
        import jsonschema
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            f"{label} needs jsonschema for schema validation; install jsonschema before running evaluator operations that validate schemas"
        ) from exc
    schema = _load_json(schema_path)
    validator = jsonschema.Draft202012Validator(schema)
    errors = sorted(validator.iter_errors(obj), key=lambda e: list(e.absolute_path))
    if errors:
        loc = "/" + "/".join(str(item) for item in errors[0].absolute_path)
        raise ValueError(f"{label} failed schema validation at {loc}: {errors[0].message}")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y%m%dT%H%M%SZ")


def _supervisor_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _append_supervisor_progress(progress_log_path: Path, message: str) -> None:
    progress_log_path.parent.mkdir(parents=True, exist_ok=True)
    with progress_log_path.open("a", encoding="utf-8") as handle:
        handle.write(f"{_supervisor_timestamp()} {message.rstrip()}\n")


def _supervisor_progress_snapshot(
    *,
    run_root: Path,
    min_mtime_ns: int,
    ignored_paths: Sequence[Path],
) -> tuple[int, int, int]:
    if not run_root.exists():
        return (0, 0, 0)
    ignored = [path.resolve() for path in ignored_paths]
    latest_mtime_ns = 0
    total_size = 0
    file_count = 0
    for dirpath, _dirnames, filenames in os.walk(run_root, onerror=lambda _exc: None):
        current_dir = Path(dirpath)
        for filename in filenames:
            candidate = current_dir / filename
            try:
                resolved = candidate.resolve()
                if any(resolved == blocked for blocked in ignored):
                    continue
                stat = resolved.stat()
            except FileNotFoundError:
                # Current-run snapshots refresh in-place while the supervisor scans
                # for semantic progress. Files that disappear mid-scan should be
                # treated as transient, not as a supervision failure.
                continue
            if stat.st_mtime_ns < min_mtime_ns:
                continue
            file_count += 1
            total_size += int(stat.st_size)
            latest_mtime_ns = max(latest_mtime_ns, int(stat.st_mtime_ns))
    return (latest_mtime_ns, total_size, file_count)


def _resolve_path(root: Path, raw: str | Path) -> Path:
    path = Path(raw)
    if not path.is_absolute():
        path = root / path
    return path.resolve()


def _request_anchor_root(request_path: Path | None) -> Path:
    if request_path is None:
        return _ROOT.resolve()
    resolved_request = Path(request_path).resolve()
    for parent in (resolved_request.parent, *resolved_request.parents):
        if parent.name == "loop_evaluator_self_eval" and parent.parent.name == ".cache":
            return parent.parent.parent.resolve()
    return resolved_request.parent.resolve()


def _path_is_packaged_self_eval_fixture_request(raw: str | Path) -> bool:
    path = Path(str(raw).strip())
    if not path.name.startswith("fixture_request") or path.suffix != ".json":
        return False
    parts = path.parts
    return len(parts) >= 3 and tuple(parts[-3:-1]) == (".cache", "loop_evaluator_self_eval")


def _request_is_packaged_self_eval_fixture(request: Mapping[str, Any]) -> bool:
    raw_output_root = str(request.get("output_root") or "").strip()
    if not raw_output_root:
        return False
    parts = Path(raw_output_root).parts
    return len(parts) >= 3 and tuple(parts[-3:]) == (".cache", "loop_evaluator_self_eval", "fixture_output")


def _packaged_self_eval_guard_depth_from_env() -> int:
    raw_value = str(os.environ.get(_PACKAGED_SELF_EVAL_GUARD_ENV) or "").strip()
    if not raw_value:
        return 0
    try:
        return max(0, int(raw_value))
    except ValueError:
        return 1


def _activate_packaged_self_eval_guard(*, request: Mapping[str, Any]) -> object | str | None:
    if not _request_is_packaged_self_eval_fixture(request):
        return _PACKAGED_SELF_EVAL_GUARD_UNCHANGED
    current_depth = _packaged_self_eval_guard_depth_from_env()
    if current_depth >= 1:
        raise ValueError(
            "packaged self-eval fixture recursion detected: nested packaged self-eval evaluator launches are forbidden inside an existing packaged self-eval run"
        )
    previous = os.environ.get(_PACKAGED_SELF_EVAL_GUARD_ENV)
    os.environ[_PACKAGED_SELF_EVAL_GUARD_ENV] = str(current_depth + 1)
    return previous


def _restore_packaged_self_eval_guard(previous: object | str | None) -> None:
    if previous is _PACKAGED_SELF_EVAL_GUARD_UNCHANGED:
        return
    if previous is None:
        os.environ.pop(_PACKAGED_SELF_EVAL_GUARD_ENV, None)
        return
    os.environ[_PACKAGED_SELF_EVAL_GUARD_ENV] = previous


def _generated_test_recursively_invokes_packaged_self_eval(
    *,
    argv: Sequence[str],
    cwd: str,
    workspace_root: Path,
) -> bool:
    tokens = _ordinary_test_command_tokens(argv)
    if not tokens:
        return False

    joined = " ".join(tokens)
    invokes_evaluator_entrypoint = (
        "loop_product.evaluator.prototype" in joined or "evaluator_prototype.py" in joined
    )
    if not invokes_evaluator_entrypoint:
        return False

    if "--input" in tokens:
        input_index = tokens.index("--input")
        if input_index + 1 < len(tokens):
            input_ref = _resolve_path(_resolve_path(workspace_root, cwd), tokens[input_index + 1])
            if _path_is_packaged_self_eval_fixture_request(input_ref):
                return True

    return ".cache/loop_evaluator_self_eval/fixture_request" in joined


def _ordinary_test_semantic_activity_paths(
    *,
    workspace_root: Path,
    argv: Sequence[str],
    cwd: str,
) -> list[Path]:
    tokens = _ordinary_test_command_tokens(argv)
    if not tokens:
        return []
    joined = " ".join(tokens)
    invokes_evaluator_entrypoint = (
        "loop_product.evaluator.prototype" in joined or "evaluator_prototype.py" in joined
    )
    if not invokes_evaluator_entrypoint or "--input" not in tokens:
        return []
    input_index = tokens.index("--input")
    if input_index + 1 >= len(tokens):
        return []
    current_cwd = _resolve_path(workspace_root, cwd)
    input_ref = _resolve_path(current_cwd, tokens[input_index + 1])
    if not input_ref.exists():
        return []
    try:
        request_obj = _load_json(input_ref)
    except Exception:
        return []
    request_workspace_root_raw = str(request_obj.get("workspace_root") or "").strip()
    output_root_raw = str(request_obj.get("output_root") or "").strip()
    evaluation_id = str(request_obj.get("evaluation_id") or "").strip()
    run_id = str(request_obj.get("run_id") or "").strip()
    if not output_root_raw or not evaluation_id:
        return []
    nested_workspace_root = (
        _resolve_path(_ROOT, request_workspace_root_raw) if request_workspace_root_raw else current_cwd
    )
    nested_output_root = _resolve_path(nested_workspace_root, output_root_raw)
    activity_root = nested_output_root / evaluation_id / run_id if run_id else nested_output_root / evaluation_id
    return [activity_root]


def _read_text_ref(*, workspace_root: Path, text_ref: str, label: str) -> tuple[Path, str]:
    text_path = _resolve_path(workspace_root, text_ref)
    if not text_path.exists() or not text_path.is_file():
        raise ValueError(f"{label} must point to an existing file: {text_ref}")
    return text_path, text_path.read_text(encoding="utf-8")


def _normalize_ai_user_scheduler(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    if not raw:
        return {}
    scheduler = dict(raw)
    normalized: dict[str, Any] = {}
    max_parallel_raw = scheduler.get("max_parallel_runs")
    if max_parallel_raw is not None:
        try:
            max_parallel = int(max_parallel_raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("ai_user_scheduler.max_parallel_runs must be an integer") from exc
        if max_parallel < 1:
            raise ValueError("ai_user_scheduler.max_parallel_runs must be >= 1")
        normalized["max_parallel_runs"] = max_parallel
    return normalized


def _normalize_request(request: Mapping[str, Any], *, request_path: Path | None = None) -> dict[str, Any]:
    normalized = json.loads(json.dumps(request))
    _validate_with_schema(obj=normalized, schema_path=_INPUT_SCHEMA_PATH, label="prototype input")
    request_anchor_root = _request_anchor_root(request_path)
    workspace_root = _resolve_path(request_anchor_root, str(normalized["workspace_root"]))
    if not workspace_root.exists() or not workspace_root.is_dir():
        raise ValueError(f"workspace_root must be an existing directory: {workspace_root}")
    output_root = _resolve_path(request_anchor_root, str(normalized["output_root"]))
    run_id = str(normalized.get("run_id") or "").strip() or _utc_stamp()
    normalized["run_id"] = run_id
    normalized["workspace_root"] = str(workspace_root)
    normalized["output_root"] = str(output_root)
    normalized["product_manual_ref"] = str(_resolve_path(workspace_root, str(normalized["product_manual_ref"])))

    final_effects_text_ref = str(normalized.get("final_effects_text_ref") or "").strip()
    if final_effects_text_ref:
        normalized["final_effects_text_ref"] = str(_resolve_path(workspace_root, final_effects_text_ref))
    elif not normalized.get("final_effect_requirements"):
        raise ValueError("prototype input must provide final_effects_text_ref or final_effect_requirements")

    role_requirements = dict(normalized.get("role_requirements") or {})
    if final_effects_text_ref and not str(role_requirements.get("checker") or "").strip():
        raise ValueError("role_requirements.checker is required when final_effects_text_ref is provided")

    agent_execution = dict(normalized.get("agent_execution") or {})
    for role in ("default", *_ROLE_IDS):
        cfg = dict(agent_execution.get(role) or {})
        if "sandbox_mode" not in cfg:
            cfg["sandbox_mode"] = "danger-full-access"
        elif str(cfg.get("sandbox_mode") or "").strip() in {"read-only", "workspace-write"}:
            cfg["sandbox_mode"] = "danger-full-access"
        if role != "default" or cfg:
            agent_execution[role] = cfg
    normalized["agent_execution"] = agent_execution
    normalized["ai_user_scheduler"] = _normalize_ai_user_scheduler(
        normalized.get("ai_user_scheduler") if isinstance(normalized.get("ai_user_scheduler"), Mapping) else None
    )
    runtime_assertions = normalized.get("runtime_assertions") or []
    if not isinstance(runtime_assertions, list):
        raise ValueError("runtime_assertions must be a list when provided")
    normalized_runtime_assertions: list[dict[str, Any]] = []
    seen_assertion_ids: set[str] = set()
    for idx, item in enumerate(runtime_assertions):
        if not isinstance(item, Mapping):
            raise ValueError(f"runtime_assertions[{idx}] must be an object")
        assertion_id = str(item.get("assertion_id") or "").strip()
        if not assertion_id:
            raise ValueError(f"runtime_assertions[{idx}] must include assertion_id")
        if assertion_id in seen_assertion_ids:
            raise ValueError(f"duplicate runtime_assertions assertion_id: {assertion_id}")
        seen_assertion_ids.add(assertion_id)
        kind = str(item.get("kind") or "").strip()
        if kind not in {"evaluation_report", "supervisor_report"}:
            raise ValueError(f"unsupported runtime_assertions kind: {kind or '<missing>'}")
        report_ref_raw = str(item.get("report_ref") or "").strip()
        if not report_ref_raw:
            raise ValueError(f"runtime_assertions[{assertion_id}] must include report_ref")
        expected_status = str(item.get("expected_status") or "").strip().upper()
        allowed_statuses = _TERMINAL_EVALUATION_STATUSES if kind == "evaluation_report" else _SUPERVISOR_STATUSES
        if expected_status not in allowed_statuses:
            raise ValueError(
                f"runtime_assertions[{assertion_id}] expected_status must be one of {allowed_statuses}"
            )
        normalized_runtime_assertions.append(
            {
                "assertion_id": assertion_id,
                "kind": kind,
                "blocking": bool(item.get("blocking", True)),
                "report_ref": str(_resolve_path(workspace_root, report_ref_raw)),
                "expected_status": expected_status,
                **(
                    {"expected_evaluation_id": str(item.get("expected_evaluation_id") or "").strip()}
                    if str(item.get("expected_evaluation_id") or "").strip()
                    else {}
                ),
                **(
                    {"expected_run_id": str(item.get("expected_run_id") or "").strip()}
                    if str(item.get("expected_run_id") or "").strip()
                    else {}
                ),
                **(
                    {"expected_error_stage": str(item.get("expected_error_stage") or "").strip()}
                    if kind == "evaluation_report" and str(item.get("expected_error_stage") or "").strip()
                    else {}
                ),
                **(
                    {
                        "expected_error_message_substring": str(
                            item.get("expected_error_message_substring") or ""
                        ).strip()
                    }
                    if str(item.get("expected_error_message_substring") or "").strip()
                    else {}
                ),
            }
        )
    if normalized_runtime_assertions:
        normalized["runtime_assertions"] = normalized_runtime_assertions
    return normalized


def _evaluate_runtime_assertions(
    runtime_assertions: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for item in runtime_assertions:
        assertion_id = str(item.get("assertion_id") or "").strip()
        report_ref = Path(str(item.get("report_ref") or "")).resolve()
        expected_status = str(item.get("expected_status") or "").strip().upper()
        expected_evaluation_id = str(item.get("expected_evaluation_id") or "").strip()
        expected_run_id = str(item.get("expected_run_id") or "").strip()
        expected_error_stage = str(item.get("expected_error_stage") or "").strip()
        expected_error_message_substring = str(item.get("expected_error_message_substring") or "").strip()
        kind = str(item.get("kind") or "").strip()
        notes: list[str] = []
        status = "FAIL"
        if not report_ref.exists() or not report_ref.is_file():
            notes.append(f"expected nested evaluation report missing: {report_ref}")
        else:
            try:
                report_obj = _load_json(report_ref)
                schema_path = _REPORT_SCHEMA_PATH if kind == "evaluation_report" else _SUPERVISOR_SCHEMA_PATH
                label = "nested evaluation report" if kind == "evaluation_report" else "nested supervisor report"
                _validate_with_schema(obj=report_obj, schema_path=schema_path, label=label)
            except Exception as exc:  # noqa: BLE001
                notes.append(f"failed to read nested {kind} {report_ref}: {exc}")
            else:
                observed_status = str(report_obj.get("status") or "").strip().upper()
                if observed_status != expected_status:
                    notes.append(
                        f"expected nested {kind} status {expected_status}, observed {observed_status or '<missing>'}"
                    )
                observed_evaluation_id = str(report_obj.get("evaluation_id") or "").strip()
                if expected_evaluation_id and observed_evaluation_id != expected_evaluation_id:
                    notes.append(
                        "expected nested evaluation_id "
                        f"{expected_evaluation_id}, observed {observed_evaluation_id or '<missing>'}"
                    )
                observed_run_id = str(report_obj.get("run_id") or "").strip()
                if expected_run_id and observed_run_id != expected_run_id:
                    notes.append(
                        f"expected nested run_id {expected_run_id}, observed {observed_run_id or '<missing>'}"
                    )
                if kind == "evaluation_report":
                    error_obj = dict(report_obj.get("error") or {})
                    observed_error_stage = str(error_obj.get("stage") or "").strip()
                    if expected_error_stage and observed_error_stage != expected_error_stage:
                        notes.append(
                            f"expected nested error.stage {expected_error_stage}, observed {observed_error_stage or '<missing>'}"
                        )
                    observed_message = str(error_obj.get("message") or "")
                    if expected_error_message_substring and expected_error_message_substring not in observed_message:
                        notes.append(
                            "expected nested error.message to contain "
                            f"{expected_error_message_substring!r}, observed {observed_message!r}"
                        )
                else:
                    observed_message = str(report_obj.get("message") or "")
                    if expected_error_message_substring and expected_error_message_substring not in observed_message:
                        notes.append(
                            "expected nested supervisor message to contain "
                            f"{expected_error_message_substring!r}, observed {observed_message!r}"
                        )
                if not notes:
                    status = "PASS"
                    notes.append(f"observed expected nested {kind}: {report_ref}")
        results.append(
            {
                "assertion_id": assertion_id,
                "kind": kind or "evaluation_report",
                "blocking": bool(item.get("blocking", True)),
                "status": status,
                "report_ref": str(report_ref),
                "notes": notes,
            }
        )
    return results


def _fold_runtime_assertions_into_status(
    *,
    base_status: str,
    runtime_assertions: Sequence[Mapping[str, Any]],
) -> str:
    if any(
        bool(item.get("blocking", True)) and str(item.get("status") or "").strip().upper() != "PASS"
        for item in runtime_assertions
    ):
        return "FAIL"
    return base_status


def _render_effects(requirements: Sequence[Mapping[str, Any]]) -> str:
    lines = []
    for item in requirements:
        rid = str(item.get("requirement_id") or "").strip()
        desc = str(item.get("description") or "").strip()
        blocking = bool(item.get("blocking", True))
        lines.append(f"- `{rid}` ({'blocking' if blocking else 'non-blocking'}): {desc}")
    return "\n".join(lines)


def _render_requirement_graph(requirement_graph: Mapping[str, Any]) -> str:
    return json.dumps(dict(requirement_graph), indent=2, sort_keys=True)


def _truncate_prompt_text(text: str, *, limit: int = 280) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 1)].rstrip() + "..."


def _prompt_ref_label(
    ref: str,
    *,
    run_root: Path,
    workspace_root: Path,
    context: Mapping[str, Any] | None = None,
) -> str:
    raw = str(ref).strip()
    if not raw:
        return raw
    path_part, suffix = _split_evidence_ref(raw)
    resolved = _resolve_evidence_ref_path(path_part, base_dirs=(run_root, workspace_root), context=context)
    if resolved is None:
        return _truncate_prompt_text(raw, limit=200)
    resolved = resolved.resolve()
    for label, base in (("run", run_root.resolve()), ("workspace", workspace_root.resolve())):
        try:
            relative = resolved.relative_to(base)
        except ValueError:
            continue
        return f"{label}:{relative.as_posix()}{suffix}"
    for root in _context_roots(context):
        try:
            relative = resolved.relative_to(Path(root["path"]).resolve())
        except ValueError:
            continue
        return f"{str(root['root_id'])}:{relative.as_posix()}{suffix}"
    return str(resolved) + suffix


def _compact_ordinary_test_results_for_reviewer_prompt(
    ordinary_test_results: Mapping[str, Any],
    *,
    run_root: Path,
    workspace_root: Path,
) -> dict[str, Any]:
    context = _build_evidence_runtime_context(run_root=run_root, workspace_root=workspace_root)
    executed_tests: list[dict[str, Any]] = []
    for item in ordinary_test_results.get("executed_tests") or []:
        if not isinstance(item, Mapping):
            continue
        compact_item = {
            "test_id": str(item.get("test_id") or "").strip(),
            "test_kind": str(item.get("test_kind") or "").strip(),
            "passed": bool(item.get("passed")),
            "exit_code": int(item.get("exit_code", -1)),
            "stdout_ref": _prompt_ref_label(
                str(item.get("stdout_ref") or ""),
                run_root=run_root,
                workspace_root=workspace_root,
                context=context,
            ),
            "stderr_ref": _prompt_ref_label(
                str(item.get("stderr_ref") or ""),
                run_root=run_root,
                workspace_root=workspace_root,
                context=context,
            ),
            "exec_span_ref": _prompt_ref_label(
                str(item.get("exec_span_ref") or ""),
                run_root=run_root,
                workspace_root=workspace_root,
                context=context,
            ),
        }
        executed_tests.append(compact_item)
    compact: dict[str, Any] = {
        "all_passed": bool(ordinary_test_results.get("all_passed")),
        "executed_tests": executed_tests,
    }
    generated_tests_ref = str(ordinary_test_results.get("generated_tests_ref") or "").strip()
    if generated_tests_ref:
        compact["generated_tests_ref"] = _prompt_ref_label(
            generated_tests_ref,
            run_root=run_root,
            workspace_root=workspace_root,
            context=context,
        )
    return compact


def _compact_ai_user_results_for_reviewer_prompt(
    ai_user_results: Sequence[Mapping[str, Any]],
    *,
    run_root: Path,
    workspace_root: Path,
) -> list[dict[str, Any]]:
    compact_runs: list[dict[str, Any]] = []
    for run in ai_user_results:
        if not isinstance(run, Mapping):
            continue
        run_context = _build_evidence_runtime_context(
            result_ref=str(run.get("result_ref") or "") or None,
            run_root=run_root,
            workspace_root=workspace_root,
            source_workspace_root=workspace_root,
            operation_log_ref=str(run.get("operation_log_ref") or "") or None,
        )
        compact_effects: list[dict[str, Any]] = []
        for effect in run.get("effect_results") or []:
            if not isinstance(effect, Mapping):
                continue
            evidence_refs = [
                _prompt_ref_label(
                    str(ref),
                    run_root=run_root,
                    workspace_root=workspace_root,
                    context=run_context,
                )
                for ref in (effect.get("evidence_refs") or [])
                if str(ref).strip()
            ]
            extra_refs = max(0, len(evidence_refs) - 4)
            visible_refs = evidence_refs[:4]
            if extra_refs:
                visible_refs.append(f"... ({extra_refs} more evidence refs omitted from prompt)")
            compact_effects.append(
                {
                    "requirement_id": str(effect.get("requirement_id") or "").strip(),
                    "outcome": str(effect.get("outcome") or "").strip(),
                    "summary": _truncate_prompt_text(str(effect.get("summary") or "").strip()),
                    "evidence_refs": visible_refs,
                }
            )
        compact_run: dict[str, Any] = {
            "unit_id": str(run.get("unit_id") or run.get("evaluation_unit_id") or "").strip(),
            "operation_log_ref": _prompt_ref_label(
                str(run.get("operation_log_ref") or ""),
                run_root=run_root,
                workspace_root=workspace_root,
                context=run_context,
            ),
            "effect_results": compact_effects,
        }
        requirement_ids = [
            str(ref).strip()
            for ref in (
                run.get("covered_requirement_ids")
                or run.get("requirement_ids")
                or []
            )
            if str(ref).strip()
        ]
        if requirement_ids:
            compact_run["covered_requirement_ids"] = requirement_ids
        result_ref = str(run.get("result_ref") or "").strip()
        if result_ref:
            compact_run["result_ref"] = _prompt_ref_label(
                result_ref,
                run_root=run_root,
                workspace_root=workspace_root,
                context=run_context,
            )
        response_ref = str(run.get("response_ref") or "").strip()
        if response_ref:
            compact_run["response_ref"] = _prompt_ref_label(
                response_ref,
                run_root=run_root,
                workspace_root=workspace_root,
                context=run_context,
            )
        notes = [str(note).strip() for note in (run.get("notes") or []) if str(note).strip()]
        if notes:
            compact_run["notes"] = [_truncate_prompt_text(note) for note in notes[:4]]
        compact_runs.append(compact_run)
    return compact_runs


def build_checker_prompt(
    *,
    request: Mapping[str, Any],
    final_effects_text: str,
    result_path: Path,
    response_path: Path,
) -> str:
    role_req = str(dict(request.get("role_requirements") or {}).get("checker") or "").strip()
    return "\n".join(
        [
            "# LOOP Evaluator Prototype: Checker",
            "",
            "## Mission",
            f"- {role_req}",
            "- Validate that the final-effects text lists one final effect per listed item.",
            "- If an item is locally and obviously malformed or merged, repair locally obvious malformed or merged items into one-effect-per-item requirements.",
            "- Normalize the accepted items into stable requirement records without inventing new goals.",
            "- Build the requirement graph around co-testable evidence surfaces: group co-testable requirements into as few evaluation_units as the evidence surface supports.",
            "- Do not emit one singleton evaluation unit per requirement when the same black-box probe can validate several requirements.",
            "- Only stop when automatic repair would require semantic guessing about the user's intent.",
            "- Return exactly one final JSON object with `result` and `summary_markdown`; the evaluator runtime will materialize the artifact files for you.",
            "- Use only the final-effects text in this prompt; do not inspect prior runs, prior checker outputs, or unrelated workspace files.",
            "- Do not use apply_patch, shell heredocs, or direct file writes for evaluator artifacts.",
            "",
            "## Output Contract",
            f"- The evaluator runtime owns `{result_path}` and `{response_path}`; do not write those paths yourself.",
            "- After you emit the single final JSON object, stop immediately instead of continuing to explore.",
            "- Because the structured-output schema is strict, you must always include every checker result key on every run.",
            "- The JSON result file must contain:",
            "  - `status = OK | HUMAN_GATE`",
            "  - if `status = OK`: `normalized_requirements = [{requirement_id, description, blocking, requirement_kind}]`; if `status = HUMAN_GATE`, set `normalized_requirements = []`",
            "  - if `status = OK`: `requirement_graph = {requirements, evaluation_units, dependency_edges}`; if `status = HUMAN_GATE`, set `requirement_graph = null`",
            "  - if `status = OK`: `requirement_graph.requirements` must be the full `normalized_requirements` objects, not bare requirement ids",
            "  - if `status = OK`: `requirement_graph.evaluation_units = [{unit_id, requirement_ids}]` and the field name must be exactly `unit_id` (not `evaluation_unit_id`)",
            "  - if `status = OK`: `requirement_graph.dependency_edges = [{from_unit_id, to_unit_id}]` and both ids must reference `evaluation_units[*].unit_id`",
            "  - if `status = OK`: `repair_actions = [{raw_item, action, emitted_requirement_ids}]`; if a repair/ledger item introduces no new normalized requirement ids, `emitted_requirement_ids` may be `[]`; if `status = HUMAN_GATE`, set `repair_actions = []`",
            "  - if `status = OK`: set `human_gate_reasons = []`; if `status = HUMAN_GATE`: `human_gate_reasons = [..]`",
            "  - `notes = [..]`",
            "  - always include `requirement_kind`; use `product_effect` for ordinary reviewer-visible product checks and `runtime_closure` only when the requirement is satisfied by evaluator/runtime terminal closure itself (for example authoritative terminal-result/result-sink obligations)",
            "  - minimal valid `requirement_graph` example:",
            "    ```json",
            '    {"requirements":[{"requirement_id":"REQ-001","description":"...","blocking":true,"requirement_kind":"product_effect"}],"evaluation_units":[{"unit_id":"EU-001","requirement_ids":["REQ-001"]}],"dependency_edges":[]}',
            "    ```",
            "",
            "## Final-Effects Text",
            final_effects_text,
        ]
    )


def build_test_designer_prompt(
    *,
    request: Mapping[str, Any],
    manual_text: str,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    result_path: Path,
    response_path: Path,
    ordinary_test_artifact_root: Path,
) -> str:
    role_req = str(dict(request.get("role_requirements") or {}).get("test_designer") or "").strip()
    self_eval_recursion_guard_lines: list[str] = []
    if _request_is_packaged_self_eval_fixture(request):
        self_eval_recursion_guard_lines = [
            "- This run is itself a packaged self-eval fixture.",
            "- Ordinary tests must not recursively launch another packaged self-eval fixture through the evaluator entrypoint, whether raw or supervised.",
            "- Recursive packaged self-eval evaluator re-entry is evaluator recursion, not valid coverage, and the runtime will reject it fail-closed.",
            "- When the current run artifacts already expose the acceptance surface, do not inspect product source files; stay inside the stage-visible evidence.",
        ]
    return "\n".join(
        [
            "# LOOP Evaluator Prototype: Ordinary Test Designer",
            "",
            "## Mission",
            f"- {role_req}",
            "- You own the ordinary-testing loop end-to-end: decide what to test, run the tests yourself, and save reviewer-visible evidence.",
            "- Generate ordinary tests such as unit tests and pressure/stress tests from the documented product surface only.",
            "- Use the normalized final-effect requirements as the target acceptance surface.",
            "- Treat documented example commands as optional aids, not a mandatory checklist; run only the probes that are actually needed for the evidence you are collecting.",
            "- Do not invent undocumented features.",
            f"- Use `{ordinary_test_artifact_root}` for reviewer-visible ordinary-test logs, helper files, and execution records.",
            "- Prefer direct documented commands and short single-purpose assertions over generating large helper scripts; only create helper files when the evidence genuinely requires them.",
            "- Prefer bounded non-interactive probes over headed or long-lived interactive sessions.",
            "- Any auxiliary browser, server, watcher, or helper process you start must be short-lived and cleaned up before you return.",
            "- Prefer documented product commands, repo-local metadata, and current-run visible artifacts over repo-internal validation scripts or fixture helpers.",
            "- Do not run repo-internal validation scripts under `tests/` or packaged self-eval fixture helpers under `.cache/loop_evaluator_self_eval/**` as ordinary tests unless the product manual explicitly documents them as part of the product surface.",
            "- Prefer staged-safe documented entrypoints and stable workspace-relative commands over brittle interpreter guesses.",
            "- If `uv run --project .` starts creating a virtual environment, building the project, or reaching for network access in a staged workspace, treat that as evaluator-side bootstrap noise and immediately switch to the already-working staged-workspace `python` surface.",
            "- If a path, interpreter, import, or harness problem is evaluator-side, record it as evaluator-side instead of blaming the product.",
            "- The runtime will not pre-classify that failure for you; inspect your own evidence and repair or re-attribute it yourself.",
            "- If one of your own commands is brittle, shell-specific, irrelevant, or self-caused, repair it and continue instead of blaming the product.",
            "- If a helper file, quoted command, or generated snippet you create fails to parse, import, or execute, treat that as your own probe mistake, repair or discard it, and continue.",
            "- Put explanatory notes in your final JSON or summary, not into a shell command.",
            "- If you accidentally trigger one of those repo-internal checks and it fails, treat that as your own probe mistake or evaluator-side evidence unless the documented product surface explicitly requires that check.",
            "- Do not inspect implementation source files to decide what to test unless a documented command or public import stayed ambiguous after one decisive probe.",
            "- When you need reviewer-visible serialization of public Python objects, prefer stable helpers such as `dataclasses.asdict`, an object's own `to_dict()`, or explicit field projection; do not assume `model_dump()` or `vars()` exist.",
            "- Do not keep opening near-duplicate probes once one decisive current-run probe already covers the same requirement family; only retry when you are repairing a specific self-caused mistake or the earlier probe stayed objectively ambiguous.",
            "- When a requirement depends on reliable enter-vs-bypass routing, prefer at least one clearly objective bypass-style prompt and one clearly fuzzy prompt; do not turn a subjective guess that some other prompt should bypass into product-failure evidence without supporting evidence.",
            "- When a requirement claims preserved behavior, isolation, or stability, include at least one focused repeated or isolated probe from the documented surface so the reviewer can judge non-regression from current-run evidence.",
            "- Before you finalize, verify that every `stdout_ref`, `stderr_ref`, and `exec_span_ref` you plan to return actually exists and still points inside the ordinary-test artifact root.",
            "- Do not inspect prior `prototype_artifacts`, prior `generated_tests`, local skills, or unrelated repository docs.",
            "- Prefer a small number of focused exploratory probes before finalizing the ordinary tests.",
            "- Once one successful smoke-style probe and one successful API or import probe already cover the documented surface, stop exploring and finalize.",
            "- Once decisive evidence exists for the current evaluation unit, stop and return immediately instead of hunting for nicer evidence.",
            "- Do not write a test that depends on future evaluator stages having already finished.",
            "- Once you have enough information, stop exploring, finish the ordinary tests, and return the evidence immediately.",
            *self_eval_recursion_guard_lines,
            "- Return exactly one final JSON object with `result` and `summary_markdown`; the evaluator runtime will materialize the artifact files for you.",
            "- Do not use apply_patch, shell heredocs, or direct file writes for evaluator artifacts.",
            "",
            "## Normalized Final-Effect Requirements",
            _render_effects(final_effect_requirements),
            "",
            "## Output Contract",
            f"- The evaluator runtime owns `{result_path}` and `{response_path}`; do not write those paths yourself.",
            "- The reviewer-visible ordinary-test evidence under the ordinary-test artifact root is yours to create and reference.",
            "- After you emit the single final JSON object, stop immediately instead of continuing to inspect the workspace.",
            "- The JSON result file must contain:",
            "  - `status = OK`",
            "  - `ordinary_test_results = {all_passed, executed_tests = [{test_id, test_kind, argv, passed, exit_code, stdout_ref, stderr_ref, exec_span_ref}]}`",
            "  - `notes = [..]`",
            "",
            "## Product Manual",
            manual_text,
        ]
    )


def build_ai_user_prompt(
    *,
    request: Mapping[str, Any],
    manual_text: str,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    requirement_graph: Mapping[str, Any] | None = None,
    target_unit: Mapping[str, Any] | None = None,
    target_requirement: Mapping[str, Any] | None = None,
    result_path: Path,
    response_path: Path,
    operation_log_path: Path,
    effect_scratch_dir: Path,
    current_run_snapshot_root: Path,
) -> str:
    role_req = str(dict(request.get("role_requirements") or {}).get("ai_user") or "").strip()
    current_unit_control_dir = effect_scratch_dir.parent / "control"
    self_eval_recursion_guard_lines: list[str] = []
    if _request_is_packaged_self_eval_fixture(request):
        self_eval_recursion_guard_lines = [
            "- This run is itself a packaged self-eval fixture.",
            "- Do not recursively launch another packaged self-eval fixture through the evaluator entrypoint, whether raw or supervised.",
            "- Recursive packaged self-eval evaluator re-entry is evaluator recursion, not valid coverage, and the runtime will reject it fail-closed before launch.",
        ]
    normalized_graph = _validate_requirement_graph(
        requirement_graph or _build_singleton_requirement_graph(final_effect_requirements),
        default_requirements=final_effect_requirements,
        label="AI-as-User requirement_graph",
    )
    unit = dict(target_unit or {})
    if not unit:
        target_requirement = dict(target_requirement or {})
        rid = str(target_requirement.get("requirement_id") or "").strip()
        if not rid:
            raise ValueError("build_ai_user_prompt requires target_unit or target_requirement")
        unit = {
            "unit_id": rid,
            "requirement_ids": [rid],
        }
    unit_id = str(unit.get("unit_id") or "").strip()
    unit_requirement_ids = [str(item) for item in unit.get("requirement_ids") or [] if str(item).strip()]
    requirement_by_id = {
        str(item.get("requirement_id") or "").strip(): dict(item) for item in final_effect_requirements
    }
    covered = [requirement_by_id[rid] for rid in unit_requirement_ids if rid in requirement_by_id]
    return "\n".join(
        [
            "# LOOP Evaluator Prototype: AI-as-User",
            "",
            "## Mission",
            f"- {role_req}",
            "- Use the product as a real user would, following only the documented product surface.",
            f"- Test exactly one evaluation unit per run: `{unit_id}`.",
            "- Only use commands or probes that are relevant to the covered requirements for this unit; ignore unrelated fixture examples or helper scripts elsewhere in the manual.",
            "- Prefer current-run visible artifacts plus documented user-facing commands over repo-internal validation scripts or fixture helpers.",
            "- Do not run repo-internal validation scripts under `tests/` or packaged self-eval fixture helpers under `.cache/loop_evaluator_self_eval/**` as product probes unless the product manual for this unit explicitly documents them as part of the product surface.",
            f"- Use `{effect_scratch_dir}` for user-created artifacts in this run so evaluation-unit runs stay isolated.",
            f"- Current-run visible artifacts from checker, test-designer, prior AI-as-User units, and ordinary tests are mirrored under `{current_run_snapshot_root}`.",
            f"- Your staged control files under `{current_unit_control_dir}` are visible current-run evidence that this AI-as-User unit was launched from code.",
            "- Prefer that snapshot when validating evidence from the current evaluator run; do not search for future terminal reports that have not been produced yet.",
            "- This staged workspace copy may omit heavyweight trees such as `.venv`, `.git`, `.lake`, and unrelated caches.",
            "- Prefer direct documented commands and short single-purpose assertions over generating large helper scripts; only create helper files when the evidence genuinely requires them.",
            "- Prefer bounded non-interactive probes over headed or long-lived interactive sessions.",
            "- Any auxiliary browser, server, watcher, or helper process you start must be short-lived and cleaned up before you return.",
            "- If a probe fails, first decide whether the problem is product-side, evaluator-side, or your own probe mistake.",
            "- If the staged workspace is missing an interpreter, module, path, or shell behavior your probe assumed, treat that as evaluator-side or ambiguous unless the documented surface for this unit explicitly requires it.",
            "- If you accidentally trigger one of those repo-internal checks and it fails, treat that as your own probe mistake or evaluator-side evidence unless the documented product surface explicitly requires that check.",
            "- If your command is brittle, irrelevant, or self-caused, repair it and continue instead of blaming the product.",
            "- If a helper file, quoted command, or generated snippet you create fails to parse, import, or execute, treat that as your own probe mistake, repair or discard it, and continue.",
            "- Do not keep opening near-duplicate probes once one decisive current-run probe already covers the same requirement family; only retry when you are repairing a specific self-caused mistake or the earlier probe stayed objectively ambiguous.",
            "- Do not turn a subjective guess that a prompt was already specific enough into FAIL evidence unless the prompt objectively matches documented bypass cues or another visible current-run probe already shows the same prompt class bypassing.",
            "- When packaging or reuse is under test, prefer staged-safe local evidence such as repo-local metadata, declared entry points, runnable module help, and offline/local build steps before any network-dependent packaging probe.",
            "- If a packaging probe needs network access or a missing build backend/toolchain in the staged workspace, treat that as evaluator-side blockage, salvage the local packaging evidence you do have, and only leave the effect `UNCERTAIN` if product-side packaging still is not evidenced.",
            "- For non-regression requirements, prefer current-run ordinary evidence plus small repeated or isolated documented probes over abstract guesses about prior history.",
            "- Do not inspect local skills, prior `prototype_artifacts`, prior `generated_tests`, or unrelated repository docs; stay inside the product manual plus documented product commands.",
            "- Record every visible operation in the operation log.",
            "- Once decisive evidence exists for the current evaluation unit, stop and return immediately instead of hunting for nicer evidence.",
            "- Do not claim success unless the recorded operations support it.",
            "- If the evidence is weak or ambiguous, mark the outcome as `UNCERTAIN` instead of claiming success.",
            *self_eval_recursion_guard_lines,
            "- Return exactly one final JSON object with `result`, `summary_markdown`, and `operation_log_markdown`; the evaluator runtime will materialize the artifact files for you.",
            "- Do not use apply_patch, shell heredocs, or direct file writes for evaluator artifacts.",
            "",
            "## Evaluation unit under test",
            f"- `unit_id = {unit_id}`",
            "- Covered requirements:",
            _render_effects(covered),
            "",
            "## All normalized final-effect requirements",
            _render_effects(final_effect_requirements),
            "",
            "## Requirement graph",
            "```json",
            _render_requirement_graph(normalized_graph),
            "```",
            "",
            "## Output Contract",
            f"- The evaluator runtime owns `{result_path}`, `{response_path}`, and `{operation_log_path}`; do not write those paths yourself.",
            "- After you emit the single final JSON object, stop immediately instead of continuing to inspect more evidence.",
            "- The JSON result file must contain:",
            "  - `status = OK`",
            f"  - `unit_id = {unit_id}`",
            "  - `effect_results = [{requirement_id, outcome, summary, evidence_refs}]`",
            "  - `notes = [..]`",
            "",
            "## Product Manual",
            manual_text,
        ]
    )


def build_reviewer_prompt(
    *,
    request: Mapping[str, Any],
    manual_text: str,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    requirement_graph: Mapping[str, Any] | None = None,
    ordinary_test_results: Mapping[str, Any],
    ai_user_results: Sequence[Mapping[str, Any]],
    result_path: Path,
    response_path: Path,
    run_root: Path,
    workspace_root: Path,
) -> str:
    role_req = str(dict(request.get("role_requirements") or {}).get("reviewer") or "").strip()
    normalized_graph = _validate_requirement_graph(
        requirement_graph or _build_singleton_requirement_graph(final_effect_requirements),
        default_requirements=final_effect_requirements,
        label="reviewer requirement_graph",
    )
    compact_ordinary = _compact_ordinary_test_results_for_reviewer_prompt(
        ordinary_test_results,
        run_root=run_root,
        workspace_root=workspace_root,
    )
    compact_ai_user = _compact_ai_user_results_for_reviewer_prompt(
        ai_user_results,
        run_root=run_root,
        workspace_root=workspace_root,
    )
    return "\n".join(
        [
            "# LOOP Evaluator Prototype: Reviewer",
            "",
            "## Mission",
            f"- {role_req}",
            "- Review both the ordinary test evidence and the aggregated effect-scoped AI-as-User evidence.",
            "- ordinary tests themselves may be criticized by the reviewer.",
            "- Before blaming the product, first decide whether the negative signal is evaluator-side, product-side, or still ambiguous.",
            "- Do not treat ordinary-test or AI-as-User interpreter/import/path/harness failures as direct product FAIL evidence.",
            "- If a delegated AI's self-generated helper file, quoted command, or probe snippet fails to parse, import, or execute, treat that as evaluator-side/self-caused unless the documented product surface explicitly required that helper.",
            "- If an ordinary test or AI-as-User probe depends on repo-internal validation scripts or fixture helpers that the product manual does not document, keep any resulting failure evaluator-side/self-caused unless the documented product surface explicitly required that probe.",
            "- If the negative signal came from a staged-workspace limitation or a delegated AI's broken probe, keep that blame on the evaluator side or return `INCONCLUSIVE`/`UNCERTAIN` instead of `FAIL`.",
            "- If the record already contains a clear BYPASS probe and a clear VISION_COMPILER probe, do not turn a delegated AI's subjective `this other prompt should have bypassed` guess into FAIL evidence without objective support from the documented surface or a matching current-run probe.",
            "- If repo-local packaging metadata, declared entry points, and runnable module surfaces are present, and the only failed wheel/build probes were blocked by staged-environment network or missing build backend/toolchain limits, keep that negative signal evaluator-side; use PASS or UNCERTAIN based on the remaining visible evidence, but not FAIL.",
            "- For non-regression requirements, weigh current-run ordinary evidence of preserved behavior, artifact behavior, isolation, and repeated stability before leaving the effect uncertain.",
            "- No fixed helper script or runtime classifier will decide that blame for you; inspect the evidence and justify the verdict yourself.",
            "- Only affirm outcomes that are supported by visible evidence.",
            "- Every PASS or FAIL verdict for a final effect MUST include reproduction_steps.",
            "- You are running inside the evaluator artifact workspace; do not attempt to modify product files.",
            "- Return exactly one final JSON object with `result` and `summary_markdown`; the evaluator runtime will materialize the artifact files for you.",
            "- Do not use apply_patch, shell heredocs, or direct file writes for evaluator artifacts.",
            "",
            "## Normalized Final-Effect Requirements",
            _render_effects(final_effect_requirements),
            "",
            "## Requirement graph",
            "```json",
            _render_requirement_graph(normalized_graph),
            "```",
            "",
            "## Ordinary-Test Evidence Summary",
            "- Prompt-sized summary only; inspect the listed refs if you need raw logs.",
            "```json",
            json.dumps(compact_ordinary, indent=2, sort_keys=True),
            "```",
            "",
            "## AI-as-User Evidence Summary",
            "- Prompt-sized summary only; inspect the listed refs if you need raw logs or raw result payloads.",
            "```json",
            json.dumps(compact_ai_user, indent=2, sort_keys=True),
            "```",
            "",
            "## Output Contract",
            f"- The evaluator runtime owns `{result_path}` and `{response_path}`; do not write those paths yourself.",
            "- After you emit the single final JSON object, stop immediately instead of continuing to inspect more workspace state.",
            "- The top-level report status is computed by the evaluator runtime from reviewer per-requirement verdicts and blocking flags.",
            "- The JSON result file must contain:",
            "  - `status = OK`",
            "  - `effect_reviews = [{requirement_id, verdict, evidence_refs, reproduction_steps}]`",
            "- Put any extra narrative commentary in `summary_markdown` instead of inventing extra JSON fields.",
            "",
            "## Product Manual",
            manual_text,
        ]
    )


def _normalize_agent_config(request: Mapping[str, Any], role_id: str) -> dict[str, Any]:
    agent_execution = dict(request.get("agent_execution") or {})
    merged = dict(agent_execution.get("default") or {})
    merged.update(dict(agent_execution.get(role_id) or {}))
    if not any(str(merged.get(key) or "").strip() for key in ("agent_cmd", "agent_provider", "agent_profile")):
        merged["agent_provider"] = "codex_cli"
    sandbox_mode = str(merged.get("sandbox_mode") or "").strip() or "danger-full-access"
    if sandbox_mode not in {"read-only", "workspace-write", "danger-full-access"}:
        raise ValueError(f"unsupported sandbox_mode for {role_id}: {sandbox_mode}")
    if sandbox_mode in {"read-only", "workspace-write"}:
        sandbox_mode = "danger-full-access"
    merged["sandbox_mode"] = sandbox_mode
    reasoning_effort = str(merged.get("reasoning_effort") or "").strip().lower()
    if reasoning_effort and reasoning_effort not in _REASONING_EFFORTS:
        raise ValueError(f"unsupported reasoning_effort for {role_id}: {reasoning_effort}")
    return merged


def _effective_agent_cmd(*, resolved: Any, config: Mapping[str, Any]) -> str:
    raw_cmd = str(getattr(resolved, "agent_cmd", "") or "").strip()
    if str(getattr(resolved, "source", "") or "") == "cli.agent_cmd":
        return raw_cmd
    if str(getattr(resolved, "provider_id", "") or "") != "codex_cli":
        return raw_cmd
    parts = [
        "codex exec",
        '-C "$LOOP_PRODUCT_EVAL_WORKSPACE_ROOT"',
        "--skip-git-repo-check",
        f'-s {shlex.quote(str(config.get("sandbox_mode") or "danger-full-access"))}',
        '${LOOP_PRODUCT_EVAL_OUTPUT_SCHEMA:+--output-schema "$LOOP_PRODUCT_EVAL_OUTPUT_SCHEMA"}',
        '-o "${LOOP_PRODUCT_EVAL_RAW_RESPONSE_PATH:-$LOOP_PRODUCT_EVAL_RESPONSE_PATH}"',
    ]
    reasoning_effort = str(config.get("reasoning_effort") or "").strip().lower()
    if reasoning_effort in _REASONING_EFFORTS:
        config_value = f'model_reasoning_effort="{reasoning_effort}"'
        parts.append(f"-c {shlex.quote(config_value)}")
    prompt_transport = str(getattr(resolved, "prompt_transport", "") or "stdin").strip()
    prompt_arg = str(getattr(resolved, "prompt_arg", "") or "").strip()
    if prompt_transport == "stdin":
        parts.append('- < "$LOOP_PRODUCT_EVAL_PROMPT"')
    elif prompt_transport == "env_path":
        parts.append('"$LOOP_PRODUCT_EVAL_PROMPT"')
    elif prompt_transport == "arg":
        parts.append(f'{shlex.quote(prompt_arg or "--prompt-file")} "$LOOP_PRODUCT_EVAL_PROMPT"')
    else:
        return raw_cmd
    return " ".join(parts)


def _agent_runtime_env_overrides(
    *,
    run_root: Path,
    role_id: str,
    resolved: Any,
    effective_cmd: str,
    role_root: Path | None = None,
) -> dict[str, str]:
    del resolved, effective_cmd
    del role_root
    sanitized_parent_codex_env = {
        key: ""
        for key in os.environ
        if key.startswith("CODEX_") and key != "CODEX_HOME"
    }
    existing_pythonpath = [part for part in str(os.environ.get("PYTHONPATH") or "").split(os.pathsep) if part]
    repo_pythonpath = [str(_ROOT)]
    for part in existing_pythonpath:
        if part not in repo_pythonpath:
            repo_pythonpath.append(part)
    return {
        **sanitized_parent_codex_env,
        "PYTHONPATH": os.pathsep.join(repo_pythonpath),
        "OTEL_SDK_DISABLED": "true",
        "OTEL_TRACES_EXPORTER": "none",
        "OTEL_METRICS_EXPORTER": "none",
        "OTEL_LOGS_EXPORTER": "none",
    }

def _extract_runtime_metadata_from_path(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    observed: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = _RUNTIME_BANNER_RE.match(raw_line)
        if match is None:
            continue
        raw_key = str(match.group(1)).strip().lower().replace("-", " ").replace("_", " ")
        key = "reasoning_effort" if raw_key == "reasoning effort" else raw_key
        value = str(match.group("value") or "").strip()
        if value:
            observed[key] = value
    return observed


def _extract_observed_runtime(*, exec_span: Mapping[str, Any]) -> dict[str, Any]:
    stdout_path_raw = str(exec_span.get("stdout_path") or "").strip()
    stderr_path_raw = str(exec_span.get("stderr_path") or "").strip()
    stdout_path = Path(stdout_path_raw) if stdout_path_raw else None
    stderr_path = Path(stderr_path_raw) if stderr_path_raw else None
    observed: dict[str, Any] = {}
    observed["cwd"] = str(Path(str(exec_span.get("cwd") or "")).resolve())
    for key, value in _extract_runtime_metadata_from_path(stdout_path).items():
        observed[key] = value
    for key, value in _extract_runtime_metadata_from_path(stderr_path).items():
        observed.setdefault(key, value)
    return observed


def _read_exec_log_text(exec_span: Mapping[str, Any], key: str, *, base_dir: Path | None = None) -> str:
    raw_path = str(exec_span.get(key) or "").strip()
    if not raw_path:
        return ""
    path = Path(raw_path)
    if not path.is_absolute() and base_dir is not None:
        path = (base_dir / path).resolve()
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def _combined_exec_log_text(exec_span: Mapping[str, Any], *, base_dir: Path | None = None) -> str:
    return "\n".join(
        part
        for part in (
            _read_exec_log_text(exec_span, "stderr_path", base_dir=base_dir),
            _read_exec_log_text(exec_span, "stdout_path", base_dir=base_dir),
        )
        if part
    )


def _classify_provider_issue_text(message: str) -> dict[str, str] | None:
    lowered = str(message or "").lower()
    if "issue_kind=provider_quota" in lowered or "upstream provider_quota blocker" in lowered:
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_quota",
            "self_attribution": "evaluator_exec_provider_quota",
            "self_repair": "retry after the provider quota window resets or switch to an authorized account with available quota.",
        }
    if "issue_kind=provider_capacity" in lowered or "upstream provider_capacity blocker" in lowered:
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_capacity",
            "self_attribution": "evaluator_exec_provider_capacity",
            "self_repair": "retry the same evaluator lane after a short bounded backoff because the provider reported temporary high demand.",
        }
    if "issue_kind=provider_transport" in lowered or "upstream provider_transport blocker" in lowered:
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_transport",
            "self_attribution": "evaluator_exec_provider_transport",
            "self_repair": "retry the same evaluator lane with bounded backoff because provider transport disconnected before completion.",
        }
    if "issue_kind=provider_runtime" in lowered or "upstream provider_runtime blocker" in lowered:
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_runtime",
            "self_attribution": "evaluator_exec_provider_runtime",
            "self_repair": "retry the same evaluator lane with bounded backoff because the provider CLI failed during startup before producing structured output.",
        }
    if "usage limit" in lowered:
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_quota",
            "self_attribution": "evaluator_exec_provider_quota",
            "self_repair": "retry after the provider quota window resets or switch to an authorized account with available quota.",
        }
    if "high demand" in lowered or "currently experiencing high demand" in lowered:
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_capacity",
            "self_attribution": "evaluator_exec_provider_capacity",
            "self_repair": "retry the same evaluator lane after a short bounded backoff because the provider reported temporary high demand.",
        }
    if any(
        needle in lowered
        for needle in (
            "stream disconnected",
            "connection reset by peer",
            "failed to connect to websocket",
            "falling back from websockets to https transport",
            "falling back to http",
            "startup websocket prewarm setup failed",
        )
    ):
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_transport",
            "self_attribution": "evaluator_exec_provider_transport",
            "self_repair": "retry the same evaluator lane with bounded backoff because provider transport disconnected before completion.",
        }
    if any(
        needle in lowered
        for needle in (
            "attempted to create a null object",
            "reqwest-internal-sync-runtime",
            "event loop thread panicked",
            "could not create otel exporter",
        )
    ):
        return {
            "kind": "STRUCTURED_EXCEPTION",
            "issue_kind": "provider_runtime",
            "self_attribution": "evaluator_exec_provider_runtime",
            "self_repair": "retry the same evaluator lane with bounded backoff because the provider CLI failed during startup before producing structured output.",
        }
    return None


def _provider_retry_delay_s(issue_kind: str, *, failure_count: int) -> float | None:
    plans: dict[str, tuple[float, ...]] = {
        "provider_transport": (1.0, 2.0),
        "provider_runtime": (1.0, 2.0),
        "provider_capacity": (2.0,),
    }
    delays = plans.get(str(issue_kind or "").strip())
    if not delays:
        return None
    index = max(0, int(failure_count) - 1)
    if index >= len(delays):
        return None
    return float(delays[index])


def _compact_exec_failure_detail(exec_span: Mapping[str, Any], *, base_dir: Path | None = None, limit: int = 240) -> str:
    compact = " ".join(_combined_exec_log_text(exec_span, base_dir=base_dir).split())
    if len(compact) <= limit:
        return compact
    return compact[: max(0, limit - 3)].rstrip() + "..."


def _detect_usage_limit_message(exec_span: Mapping[str, Any], *, base_dir: Path | None = None) -> str | None:
    combined = _combined_exec_log_text(exec_span, base_dir=base_dir)
    lowered = combined.lower()
    if "usage limit" not in lowered:
        return None
    for line in combined.splitlines():
        if "usage limit" in line.lower():
            return line.strip()
    return "upstream agent reported a usage-limit failure before producing structured output"


def _structured_output_pre_result_error(
    *,
    runtime_role_instance_id: str,
    output_schema_path: Path | None,
    raw_response_path: Path,
    effective_result_path: Path,
    exec_span: Mapping[str, Any],
    base_dir: Path | None = None,
) -> RuntimeError | None:
    if output_schema_path is None or not raw_response_path.exists() or effective_result_path.exists():
        return None
    if raw_response_path.stat().st_size != 0:
        return None
    provider_issue = _classify_provider_issue_text(_combined_exec_log_text(exec_span, base_dir=base_dir))
    if provider_issue is not None:
        detail = _compact_exec_failure_detail(exec_span, base_dir=base_dir)
        suffix = f": {detail}" if detail else ""
        return RuntimeError(
            f"{runtime_role_instance_id} hit an upstream {provider_issue['issue_kind']} blocker before producing result.json{suffix}"
        )
    return RuntimeError(f"{runtime_role_instance_id} produced an empty structured-output envelope before result.json")


def _invoke_role(
    *,
    role_id: str,
    request: Mapping[str, Any],
    run_root: Path,
    prompt_text: str,
    context_obj: Mapping[str, Any],
    config_role_id: str | None = None,
    role_instance_id: str | None = None,
    env_role_instance_id: str | None = None,
    order_log_id: str | None = None,
    role_dir: Path | None = None,
    workspace_root: Path | None = None,
    extra_env: Mapping[str, str] | None = None,
    agent_prompt_path: Path | None = None,
    agent_context_path: Path | None = None,
    agent_result_path: Path | None = None,
    agent_response_path: Path | None = None,
    agent_operation_log_path: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    cfg_role = config_role_id or role_id
    runtime_role_instance_id = str(role_instance_id or role_id)
    runtime_env_role_instance_id = str(env_role_instance_id or runtime_role_instance_id)
    resolved_workspace_root = workspace_root or Path(str(request["workspace_root"]))
    source_workspace_root = Path(str(request["workspace_root"]))
    role_dir = role_dir or _default_role_runtime_root(run_root=run_root, role_id=role_id)
    artifact_root = _role_runtime_artifact_root(role_dir, runtime_role_instance_id)
    prompt_path = artifact_root / "prompt.md"
    context_path = artifact_root / "context.json"
    result_path = artifact_root / "result.json"
    response_path = artifact_root / "response.md"
    invocation_path = artifact_root / "invocation.json"
    exec_span_path = artifact_root / "exec_span.json"
    operation_log_path = artifact_root / "operation_log.md"
    effective_prompt_path = agent_prompt_path or prompt_path
    effective_context_path = agent_context_path or context_path
    effective_result_path = agent_result_path or result_path
    effective_response_path = agent_response_path or response_path
    effective_operation_log_path = agent_operation_log_path or operation_log_path
    raw_response_path = artifact_root / "response.raw.json"
    _write_text(prompt_path, prompt_text)
    _write_json(context_path, dict(context_obj))
    if effective_prompt_path != prompt_path:
        _write_text(effective_prompt_path, prompt_text)
    if effective_context_path != context_path:
        _write_json(effective_context_path, dict(context_obj))
    cfg = _normalize_agent_config(request, cfg_role)
    resolved = resolve_agent_invocation(
        repo_root=_ROOT,
        mode="run",
        agent_cmd=str(cfg.get("agent_cmd") or "").strip() or None,
        agent_provider=str(cfg.get("agent_provider") or "").strip() or None,
        agent_profile=str(cfg.get("agent_profile") or "").strip() or None,
    )
    if resolved is None:
        raise ValueError(f"unable to resolve agent invocation for {role_id}")
    structured_output_schema: dict[str, Any] | None = None
    output_schema_path: Path | None = None
    if str(getattr(resolved, "provider_id", "") or "") == "codex_cli" and str(getattr(resolved, "source", "") or "") != "cli.agent_cmd":
        structured_output_schema = _role_output_envelope_schema(cfg_role)
        output_schema_path = role_dir / "output_schema.json"
        _write_json(output_schema_path, structured_output_schema)
    effective_cmd = _effective_agent_cmd(resolved=resolved, config=cfg)
    runtime_env_overrides = _agent_runtime_env_overrides(
        run_root=run_root,
        role_id=runtime_role_instance_id,
        resolved=resolved,
        effective_cmd=effective_cmd,
        role_root=role_dir,
    )
    invocation_obj = {
        "requested_config": cfg,
        "resolved_invocation": resolved.to_metadata(),
        "effective_agent_cmd": effective_cmd,
        **({"runtime_env_overrides": dict(runtime_env_overrides)} if runtime_env_overrides else {}),
        "prompt_ref": str(prompt_path),
        "context_ref": str(context_path),
        "result_ref": str(result_path),
        "response_ref": str(response_path),
        **({"agent_prompt_ref": str(effective_prompt_path)} if effective_prompt_path != prompt_path else {}),
        **({"agent_context_ref": str(effective_context_path)} if effective_context_path != context_path else {}),
        **({"agent_result_ref": str(effective_result_path)} if effective_result_path != result_path else {}),
        **({"agent_response_ref": str(effective_response_path)} if effective_response_path != response_path else {}),
        **(
            {"agent_operation_log_ref": str(effective_operation_log_path)}
            if effective_operation_log_path != operation_log_path
            else {}
        ),
        **({"output_schema_ref": str(output_schema_path)} if output_schema_path is not None else {}),
        "role_instance_id": runtime_role_instance_id,
        "config_role_id": cfg_role,
    }
    _write_json(invocation_path, invocation_obj)
    order_log_path = run_root / "order.log"
    order_log_path.parent.mkdir(parents=True, exist_ok=True)
    order_log_label = str(order_log_id or runtime_role_instance_id)
    with order_log_path.open("a", encoding="utf-8") as order_handle:
        order_handle.write(order_log_label + "\n")
    env = apply_env_map(
        resolved=resolved,
        env={
            "LOOP_PRODUCT_EVAL_PROMPT": str(effective_prompt_path),
            "LOOP_PRODUCT_EVAL_ROLE": cfg_role,
            "LOOP_PRODUCT_EVAL_ROLE_INSTANCE": runtime_env_role_instance_id,
            "LOOP_PRODUCT_EVAL_WORKSPACE_ROOT": str(resolved_workspace_root),
            "LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT": str(source_workspace_root),
            "LOOP_PRODUCT_EVAL_CONTEXT_PATH": str(effective_context_path),
            "LOOP_PRODUCT_EVAL_RESULT_PATH": str(effective_result_path),
            "LOOP_PRODUCT_EVAL_RESPONSE_PATH": str(effective_response_path),
            "LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH": str(effective_operation_log_path),
        },
    )
    if runtime_env_overrides:
        env.update(runtime_env_overrides)
    if output_schema_path is not None:
        env["LOOP_PRODUCT_EVAL_OUTPUT_SCHEMA"] = str(output_schema_path)
        env["LOOP_PRODUCT_EVAL_RAW_RESPONSE_PATH"] = str(raw_response_path)
    if extra_env:
        env.update({str(k): str(v) for k, v in extra_env.items()})
    launch_spec = _role_launch.build_committed_role_launch_spec(
        resolved=resolved,
        config=cfg,
        prompt_path=effective_prompt_path,
        workspace_root=resolved_workspace_root,
        response_path=effective_response_path,
        base_env=env,
        output_schema_path=output_schema_path,
        raw_response_path=raw_response_path if output_schema_path is not None else None,
    )
    retry_history: list[dict[str, Any]] = []
    safe_role_label = runtime_role_instance_id.replace(":", "__")
    final_cmd_result = None
    final_exit_code = -1
    final_attempt_count = 0
    terminal_error: Exception | None = None
    for attempt_index in range(1, 5):
        final_attempt_count = attempt_index
        for stale_path in (
            raw_response_path,
            effective_result_path,
            effective_response_path,
            effective_operation_log_path,
            result_path,
            response_path,
            operation_log_path,
        ):
            if stale_path.exists():
                stale_path.unlink()
        attempt_suffix = f"attempt_{attempt_index:02d}"
        attempt_invocation_path = artifact_root / f"invocation.{attempt_suffix}.json"
        attempt_exec_span_path = artifact_root / f"exec_span.{attempt_suffix}.json"
        attempt_invocation = {
            **invocation_obj,
            "attempt_index": attempt_index,
            "launch_spec": dict(launch_spec),
        }
        _write_json(attempt_invocation_path, attempt_invocation)
        cmd_result = _role_launch.run_committed_role_launch(
            launch_spec=launch_spec,
            log_dir=artifact_root,
            label=f"{safe_role_label}__{attempt_suffix}",
            timeout_s=3600,
            idle_timeout_s=600,
            reconnect_grace_s=30,
            reconnect_max_events=8,
        )
        _write_json(attempt_exec_span_path, cmd_result.span)
        final_cmd_result = cmd_result
        final_exit_code = int(cmd_result.span.get("exit_code", -1))
        usage_limit_message = _detect_usage_limit_message(cmd_result.span, base_dir=artifact_root.parent)
        if usage_limit_message is not None and not effective_result_path.exists():
            terminal_error = RuntimeError(
                f"{runtime_role_instance_id} hit an upstream usage-limit blocker before producing result.json: {usage_limit_message}"
            )
        else:
            structured_output_error = _structured_output_pre_result_error(
                runtime_role_instance_id=runtime_role_instance_id,
                output_schema_path=output_schema_path,
                raw_response_path=raw_response_path,
                effective_result_path=effective_result_path,
                exec_span=cmd_result.span,
                base_dir=artifact_root.parent,
            )
            if structured_output_error is not None:
                terminal_error = structured_output_error
            else:
                if (
                    output_schema_path is not None
                    and raw_response_path.exists()
                    and not effective_result_path.exists()
                ):
                    envelope_obj = _load_json(raw_response_path)
                    _materialize_structured_role_outputs(
                        role_id=cfg_role,
                        envelope_obj=envelope_obj,
                        result_path=effective_result_path,
                        response_path=effective_response_path,
                        operation_log_path=effective_operation_log_path if cfg_role == "ai_user" else None,
                        raw_response_path=raw_response_path,
                    )
                if effective_result_path.exists() and effective_result_path != result_path:
                    _copy_file_if_needed(source=effective_result_path, dest=result_path)
                if effective_response_path.exists() and effective_response_path != response_path:
                    _copy_file_if_needed(source=effective_response_path, dest=response_path)
                if effective_operation_log_path.exists() and effective_operation_log_path != operation_log_path:
                    _copy_file_if_needed(source=effective_operation_log_path, dest=operation_log_path)
                if final_exit_code != 0 and not effective_result_path.exists():
                    provider_issue = _classify_provider_issue_text(
                        _combined_exec_log_text(cmd_result.span, base_dir=artifact_root.parent)
                    )
                    if provider_issue is not None:
                        terminal_error = RuntimeError(
                            f"{runtime_role_instance_id} hit an upstream {provider_issue['issue_kind']} blocker before producing result.json: "
                            f"{_compact_exec_failure_detail(cmd_result.span, base_dir=artifact_root.parent)}"
                        )
                    else:
                        detail = _compact_exec_failure_detail(cmd_result.span, base_dir=artifact_root.parent)
                        suffix = f": {detail}" if detail else ""
                        terminal_error = RuntimeError(
                            f"{runtime_role_instance_id} agent command failed before producing result.json{suffix}"
                        )
                elif not effective_result_path.exists():
                    terminal_error = RuntimeError(f"{runtime_role_instance_id} did not produce result.json")
                elif not effective_response_path.exists():
                    terminal_error = RuntimeError(f"{runtime_role_instance_id} did not produce response.md")
                else:
                    terminal_error = None
        if terminal_error is None:
            break
        failure_text = _combined_exec_log_text(cmd_result.span, base_dir=artifact_root.parent)
        provider_issue = _classify_provider_issue_text(failure_text)
        retry_delay_s = None
        if provider_issue is not None:
            retry_delay_s = _provider_retry_delay_s(
                str(provider_issue.get("issue_kind") or ""),
                failure_count=len(retry_history) + 1,
            )
        retry_history.append(
            {
                "attempt_index": attempt_index,
                "exit_code": final_exit_code,
                "issue": dict(provider_issue or {}),
                "invocation_ref": str(attempt_invocation_path),
                "exec_span_ref": str(attempt_exec_span_path),
                "stdout_ref": str(_resolve_role_exec_log_ref(artifact_root=artifact_root, exec_span=cmd_result.span, key="stdout_path")),
                "stderr_ref": str(_resolve_role_exec_log_ref(artifact_root=artifact_root, exec_span=cmd_result.span, key="stderr_path")),
                **({"retry_delay_s": retry_delay_s} if retry_delay_s is not None else {}),
            }
        )
        if retry_delay_s is not None:
            time.sleep(retry_delay_s)
            continue
        raise terminal_error
    if final_cmd_result is None:
        raise RuntimeError(f"{runtime_role_instance_id} failed before launching the delegated role command")
    _write_json(
        invocation_path,
        {
            **invocation_obj,
            "launch_spec": dict(launch_spec),
            "attempt_count": final_attempt_count,
            "retry_history": retry_history,
        },
    )
    _write_json(
        exec_span_path,
        {
            **dict(final_cmd_result.span),
            "attempt_count": final_attempt_count,
            "retry_history": retry_history,
        },
    )
    if terminal_error is not None:
        raise terminal_error
    if not prompt_path.exists():
        _write_text(prompt_path, prompt_text)
    if not context_path.exists():
        _write_json(context_path, dict(context_obj))
    if not invocation_path.exists():
        _write_json(invocation_path, invocation_obj)
    result_obj = _load_json(result_path)
    role_run = {
        "prompt_ref": str(prompt_path),
        "context_ref": str(context_path),
        "result_ref": str(result_path),
        "response_ref": str(response_path),
        "invocation_ref": str(invocation_path),
        "exec_span_ref": str(exec_span_path),
        "stdout_ref": _resolve_role_exec_log_ref(artifact_root=artifact_root, exec_span=final_cmd_result.span, key="stdout_path"),
        "stderr_ref": _resolve_role_exec_log_ref(artifact_root=artifact_root, exec_span=final_cmd_result.span, key="stderr_path"),
        "exit_code": final_exit_code,
        "agent_provider_id": str(getattr(resolved, "provider_id", "") or "unknown"),
        "requested_agent_policy": dict(cfg),
        "resolved_invocation": resolved.to_metadata(),
        "effective_agent_cmd": effective_cmd,
        "role_instance_id": runtime_role_instance_id,
        "launch_spec": dict(launch_spec),
        "attempt_count": final_attempt_count,
        "retry_history": retry_history,
        "observed_runtime": _extract_observed_runtime(exec_span=final_cmd_result.span),
        **({"operation_log_ref": str(operation_log_path)} if operation_log_path.exists() else {}),
    }
    _materialize_canonical_role_runtime_artifacts(role_dir=role_dir, role_run=role_run)
    report_role_run = dict(role_run)
    for internal_only_key in ("stdout_ref", "stderr_ref", "operation_log_ref"):
        report_role_run.pop(internal_only_key, None)
    return result_obj, report_role_run


def _materialize_canonical_role_runtime_artifacts(*, role_dir: Path, role_run: Mapping[str, Any]) -> None:
    canonical_targets: list[tuple[Path, Path]] = []
    canonical_runtime_id = _safe_role_runtime_id(str(role_run.get("role_instance_id") or role_dir.name))
    fixed_names = {
        "prompt_ref": "prompt.md",
        "context_ref": "context.json",
        "result_ref": "result.json",
        "response_ref": "response.md",
        "invocation_ref": "invocation.json",
        "exec_span_ref": "exec_span.json",
        "operation_log_ref": "operation_log.md",
    }
    for ref_key, dest_name in fixed_names.items():
        raw_ref = str(role_run.get(ref_key) or "").strip()
        if not raw_ref:
            continue
        source = Path(raw_ref)
        if not source.exists():
            continue
        dest = role_dir / dest_name
        if source.resolve() == dest.resolve():
            continue
        canonical_targets.append((source, dest))
    for ref_key in ("stdout_ref", "stderr_ref"):
        raw_ref = str(role_run.get(ref_key) or "").strip()
        if not raw_ref:
            continue
        source = Path(raw_ref)
        if not source.exists():
            continue
        suffix = "stdout.txt" if ref_key == "stdout_ref" else "stderr.txt"
        for dest in (role_dir / f"{canonical_runtime_id}.{suffix}", role_dir / source.name):
            if source.resolve() == dest.resolve():
                continue
            canonical_targets.append((source, dest))
    for source, dest in canonical_targets:
        _copy_file_if_needed(source=source, dest=dest)


def _resolve_role_exec_log_ref(*, artifact_root: Path, exec_span: Mapping[str, Any], key: str) -> str:
    raw_path = str(exec_span.get(key) or "").strip()
    if not raw_path:
        return ""
    path = Path(raw_path)
    if path.is_absolute():
        return str(path)
    return str((artifact_root.parent / path).resolve())


def _normalize_evidence_root_id(raw: str) -> str:
    token = re.sub(r"[^a-z0-9]+", "_", str(raw or "").strip().lower()).strip("_")
    return token or "root"


def _build_evidence_runtime_context(
    *,
    result_ref: Path | str | None = None,
    run_root: Path | str | None = None,
    workspace_root: Path | str | None = None,
    source_workspace_root: Path | str | None = None,
    product_manual_ref: Path | str | None = None,
    operation_log_ref: Path | str | None = None,
    base_dirs: Sequence[Path] = (),
) -> dict[str, Any]:
    roots: list[dict[str, Any]] = []
    seen_paths: set[str] = set()

    def _register(
        root_id: str,
        raw_path: Path | str | None,
        *,
        aliases: Sequence[str] = (),
        group: str | None = None,
        priority: int = 100,
    ) -> None:
        if raw_path in (None, ""):
            return
        resolved = Path(raw_path).resolve()
        key = str(resolved)
        if key in seen_paths:
            return
        seen_paths.add(key)
        alias_ids = {
            _normalize_evidence_root_id(root_id),
            *(_normalize_evidence_root_id(alias) for alias in aliases if str(alias).strip()),
        }
        roots.append(
            {
                "root_id": _normalize_evidence_root_id(root_id),
                "path": resolved,
                "aliases": tuple(sorted(alias_ids)),
                "group": str(group or "").strip() or None,
                "priority": int(priority),
            }
        )

    if result_ref not in (None, ""):
        _register("result", Path(result_ref).resolve().parent, aliases=("result_dir", "role"), priority=5)
    _register("run", run_root, aliases=("artifact_workspace",), group="run_space", priority=10)
    _register("workspace", workspace_root, aliases=("worktree",), group="workspace_space", priority=20)
    if source_workspace_root not in (None, ""):
        source_resolved = Path(source_workspace_root).resolve()
        workspace_resolved = Path(workspace_root).resolve() if workspace_root not in (None, "") else None
        if workspace_resolved is None or source_resolved != workspace_resolved:
            _register(
                "source_workspace",
                source_resolved,
                aliases=("source", "original_workspace"),
                group="workspace_space",
                priority=30,
            )
    if product_manual_ref not in (None, ""):
        _register("manual", Path(product_manual_ref).resolve().parent, aliases=("product_manual",), priority=40)
    if operation_log_ref not in (None, ""):
        _register("operation_log", Path(operation_log_ref).resolve().parent, aliases=("oplog",), priority=2)
    for idx, base_dir in enumerate(base_dirs):
        _register(f"base_{idx}", base_dir, priority=200 + idx)
    roots.sort(key=lambda item: (int(item["priority"]), str(item["path"])))
    return {"roots": roots}


def _context_roots(context: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(context, Mapping):
        return []
    raw_roots = context.get("roots")
    if not isinstance(raw_roots, list):
        return []
    roots: list[dict[str, Any]] = []
    for item in raw_roots:
        if not isinstance(item, Mapping):
            continue
        try:
            path = Path(str(item.get("path") or "")).resolve()
        except Exception:
            continue
        aliases = {
            _normalize_evidence_root_id(str(item.get("root_id") or "")),
            *(
                _normalize_evidence_root_id(str(alias))
                for alias in (item.get("aliases") or [])
                if str(alias).strip()
            ),
        }
        roots.append(
            {
                "root_id": _normalize_evidence_root_id(str(item.get("root_id") or "")),
                "path": path,
                "aliases": tuple(sorted(alias for alias in aliases if alias)),
                "group": str(item.get("group") or "").strip() or None,
                "priority": int(item.get("priority", 100)),
            }
        )
    roots.sort(key=lambda item: (int(item["priority"]), str(item["path"])))
    return roots


def _labeled_context_root_candidates(raw_path: str, *, context: Mapping[str, Any] | None) -> list[Path]:
    if ":" not in raw_path:
        return []
    label, _sep, labeled_suffix = raw_path.partition(":")
    suffix = labeled_suffix.strip()
    if not suffix:
        return []
    suffix_path = Path(suffix)
    if suffix_path.is_absolute():
        return [suffix_path]
    normalized_label = _normalize_evidence_root_id(label)
    candidates: list[Path] = []
    suffix_variants = [suffix_path, *_collapse_adjacent_duplicate_path_segments(suffix_path)]
    for root in _context_roots(context):
        if normalized_label not in set(root.get("aliases") or ()):
            continue
        root_path = Path(root["path"])
        for variant in suffix_variants:
            candidates.append(root_path / variant)
    return candidates


def _rebase_path_via_context_roots(
    path: Path,
    *,
    context: Mapping[str, Any] | None,
) -> Path | None:
    if not path.is_absolute():
        return None
    roots = _context_roots(context)
    if not roots:
        return None
    grouped_targets: dict[str, list[dict[str, Any]]] = {}
    for root in roots:
        group = str(root.get("group") or "").strip()
        if not group:
            continue
        grouped_targets.setdefault(group, []).append(root)
    for targets in grouped_targets.values():
        targets.sort(key=lambda item: (int(item["priority"]), str(item["path"])))
    for source_root in roots:
        group = str(source_root.get("group") or "").strip()
        if not group:
            continue
        try:
            relative = path.relative_to(Path(source_root["path"]))
        except ValueError:
            continue
        for target_root in grouped_targets.get(group, []):
            candidate = Path(target_root["path"]) / relative
            if candidate.exists():
                return candidate.resolve()
    return None


def _candidate_evidence_resolution_roots(
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> list[Path]:
    roots: list[Path] = []
    seen: set[str] = set()
    for root in _context_roots(context):
        root_path = Path(root["path"]).resolve()
        key = str(root_path)
        if key in seen:
            continue
        seen.add(key)
        roots.append(root_path)
    for base_dir in base_dirs:
        root_path = Path(base_dir).resolve()
        key = str(root_path)
        if key in seen:
            continue
        seen.add(key)
        roots.append(root_path)
    return roots


def _unique_existing_resolution(candidates: Sequence[Path]) -> Path | None:
    matches: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate.exists():
            continue
        resolved = candidate.resolve()
        key = str(resolved)
        if key in seen:
            continue
        seen.add(key)
        matches.append(resolved)
    if len(matches) == 1:
        return matches[0]
    return None


def _ensure_file_refs_exist(
    refs: Sequence[str],
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> list[str]:
    normalized_refs: list[str] = []
    for ref in refs:
        normalized_refs.append(_normalize_evidence_ref(str(ref), base_dirs=base_dirs, context=context))
    return normalized_refs


def _collapse_adjacent_duplicate_path_segments(path: Path) -> list[Path]:
    parts = path.parts
    if len(parts) < 4:
        return []
    collapsed: list[Path] = []
    seen: set[str] = set()
    for start in range(len(parts) - 1):
        remaining = len(parts) - start
        for width in range(1, remaining // 2 + 1):
            left = parts[start : start + width]
            right = parts[start + width : start + (2 * width)]
            if left != right:
                continue
            candidate_parts = parts[: start + width] + parts[start + (2 * width) :]
            if not candidate_parts:
                continue
            candidate = Path(*candidate_parts)
            candidate_key = str(candidate)
            if candidate_key in seen or candidate_key == str(path):
                continue
            seen.add(candidate_key)
            collapsed.append(candidate)
    return collapsed


def _relocate_absolute_evidence_ref_path(
    ref_path: Path,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> Path | None:
    if not ref_path.is_absolute():
        return None
    parts = ref_path.parts
    if not parts:
        return None
    seen_anchors: set[Path] = set()
    candidates: list[Path] = []
    for resolved_base in _candidate_evidence_resolution_roots(base_dirs=base_dirs, context=context):
        for anchor in (resolved_base, *resolved_base.parents):
            if anchor in seen_anchors:
                continue
            seen_anchors.add(anchor)
            anchor_name = anchor.name.strip()
            if not anchor_name:
                continue
            for anchor_index, part in enumerate(parts):
                if part != anchor_name:
                    continue
                candidates.append(anchor.joinpath(*parts[anchor_index + 1 :]))
    return _unique_existing_resolution(candidates)


def _relocate_absolute_evidence_ref_by_suffix(
    ref_path: Path,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> Path | None:
    if not ref_path.is_absolute():
        return None
    parts = ref_path.parts
    if len(parts) < 2:
        return None
    candidates: list[Path] = []
    for root in _candidate_evidence_resolution_roots(base_dirs=base_dirs, context=context):
        for index in range(1, len(parts)):
            candidates.append(root.joinpath(*parts[index:]))
    return _unique_existing_resolution(candidates)


def _repair_single_path_segment_typo(path: Path) -> Path | None:
    candidate = path if path.is_absolute() else None
    if candidate is None:
        return None
    parts = candidate.parts
    if not parts:
        return None
    current = Path(parts[0])
    start_index = 1
    for index in range(start_index, len(parts)):
        part = parts[index]
        next_path = current / part
        if next_path.exists():
            current = next_path
            continue
        if not current.exists() or not current.is_dir() or len(part) < 6:
            return None
        sibling_names = [child.name for child in current.iterdir()]
        matches = difflib.get_close_matches(part, sibling_names, n=2, cutoff=0.92)
        if len(matches) != 1:
            return None
        repaired = (current / matches[0]).joinpath(*parts[index + 1 :])
        if repaired.exists():
            return repaired.resolve()
        return None
    return candidate.resolve() if candidate.exists() else None


def _evidence_search_parts(path: Path) -> tuple[str, ...]:
    parts = path.parts
    if path.is_absolute() and parts and parts[0] == path.anchor:
        parts = parts[1:]
    return tuple(part for part in parts if part and part != ".")


def _path_parts_form_ordered_subsequence(
    expected_parts: Sequence[str],
    candidate_parts: Sequence[str],
) -> bool:
    if not expected_parts or not candidate_parts:
        return False
    expected_index = 0
    for candidate_part in candidate_parts:
        if candidate_part != expected_parts[expected_index]:
            continue
        expected_index += 1
        if expected_index == len(expected_parts):
            return True
    return False


def _relocate_evidence_ref_with_omitted_segment(
    ref_path: Path,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> Path | None:
    search_parts = _evidence_search_parts(ref_path)
    if len(search_parts) < 3:
        return None
    filename = search_parts[-1]
    candidates: list[Path] = []
    for root in _candidate_evidence_resolution_roots(base_dirs=base_dirs, context=context):
        if not root.exists() or not root.is_dir():
            continue
        search_root = root
        first_part = search_parts[0]
        anchored_root = root / first_part
        if first_part not in {"..", "~"} and anchored_root.exists() and anchored_root.is_dir():
            search_root = anchored_root
        try:
            matches = search_root.rglob(filename)
        except OSError:
            continue
        for match in matches:
            try:
                if not match.is_file():
                    continue
                resolved = match.resolve()
            except OSError:
                continue
            try:
                relative_parts = resolved.relative_to(root.resolve()).parts
            except ValueError:
                relative_parts = resolved.parts
            if not _path_parts_form_ordered_subsequence(
                search_parts,
                _evidence_search_parts(Path(*relative_parts)),
            ):
                continue
            candidates.append(resolved)
    return _unique_existing_resolution(candidates)


def _relocate_fuzzy_evidence_ref_path(
    ref_path: Path,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> Path | None:
    candidates: list[Path] = []
    if ref_path.is_absolute():
        rebased = _rebase_path_via_context_roots(ref_path, context=context)
        if rebased is not None:
            candidates.append(rebased)
        candidates.append(ref_path)
    else:
        for base_dir in _candidate_evidence_resolution_roots(base_dirs=base_dirs, context=context):
            candidates.append((base_dir / ref_path).resolve(strict=False))
    seen: set[str] = set()
    for candidate in candidates:
        candidate_key = str(candidate)
        if candidate_key in seen:
            continue
        seen.add(candidate_key)
        repaired = _repair_single_path_segment_typo(candidate)
        if repaired is not None:
            return repaired
    return None


def _resolve_evidence_ref_path(
    ref: str,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> Path | None:
    raw = str(ref).strip()
    if not raw:
        return None
    raw_path, _suffix = _split_evidence_ref(raw)
    candidate_raw_paths = [raw_path]
    if ":" in raw_path:
        _prefix, _sep, labeled_suffix = raw_path.partition(":")
        labeled_suffix = labeled_suffix.strip()
        if labeled_suffix and (
            labeled_suffix.startswith((".", "/", "~"))
            or "/" in labeled_suffix
            or "\\" in labeled_suffix
        ):
            candidate_raw_paths.append(labeled_suffix)
    if "@" in raw_path:
        at_suffix = raw_path.rsplit("@", 1)[1].strip()
        if at_suffix and (
            at_suffix.startswith((".", "/", "~"))
            or "/" in at_suffix
            or "\\" in at_suffix
        ):
            candidate_raw_paths.append(at_suffix)
    for match in re.finditer(
        r"(?P<path>(?:~|/|\.{1,2}/|\.[A-Za-z0-9_.-]+/)[^`'\"\s)>,;]+)",
        raw_path,
    ):
        embedded_path = str(match.group("path") or "").strip()
        if embedded_path:
            candidate_raw_paths.append(embedded_path)
    seen_raw_paths: set[str] = set()
    for candidate_raw_path in candidate_raw_paths:
        normalized_raw_path = str(candidate_raw_path).strip()
        if not normalized_raw_path or normalized_raw_path in seen_raw_paths:
            continue
        seen_raw_paths.add(normalized_raw_path)
        candidates: list[Path] = []
        labeled_candidates = _labeled_context_root_candidates(normalized_raw_path, context=context)
        candidates.extend(labeled_candidates)
        direct = Path(normalized_raw_path)
        direct_variants = [direct, *_collapse_adjacent_duplicate_path_segments(direct)]
        for variant in direct_variants:
            if variant.is_absolute():
                rebased = _rebase_path_via_context_roots(variant, context=context)
                if rebased is not None:
                    return rebased.resolve()
                candidates.append(variant)
            else:
                for base_dir in _candidate_evidence_resolution_roots(base_dirs=base_dirs, context=context):
                    candidates.append(base_dir / variant)
                candidates.append(variant)
        seen_candidates: set[str] = set()
        for candidate in candidates:
            candidate_key = str(candidate)
            if candidate_key in seen_candidates:
                continue
            seen_candidates.add(candidate_key)
            if candidate.exists():
                return candidate.resolve()
        relocated = _relocate_absolute_evidence_ref_path(direct, base_dirs=base_dirs, context=context)
        if relocated is None:
            relocated = _relocate_absolute_evidence_ref_by_suffix(direct, base_dirs=base_dirs, context=context)
        if relocated is not None:
            return relocated
        fuzzy_relocated = _relocate_fuzzy_evidence_ref_path(direct, base_dirs=base_dirs, context=context)
        if fuzzy_relocated is not None:
            return fuzzy_relocated
        omitted_segment_relocated = _relocate_evidence_ref_with_omitted_segment(
            direct,
            base_dirs=base_dirs,
            context=context,
        )
        if omitted_segment_relocated is not None:
            return omitted_segment_relocated
    return None


def _split_evidence_ref(ref: str) -> tuple[str, str]:
    raw = str(ref).strip()
    if not raw:
        return "", ""
    if "->" in raw:
        raw = raw.rsplit("->", 1)[1].strip()
    fragment_suffix = ""
    path_candidate = raw
    if "#" in raw:
        path_candidate, fragment = raw.split("#", 1)
        fragment_suffix = "#" + fragment
    line_suffix = ""
    match = _FILE_REF_LINE_SUFFIX.match(path_candidate)
    if match:
        path_candidate = str(match.group("path")).strip()
        line_suffix = ":" + str(match.group("line"))
        if match.group("column") is not None:
            line_suffix += ":" + str(match.group("column"))
    return path_candidate.strip(), line_suffix + fragment_suffix


def _normalize_evidence_ref(
    ref: str,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> str:
    path_part, suffix = _split_evidence_ref(ref)
    resolved = _resolve_evidence_ref_path(path_part, base_dirs=base_dirs, context=context)
    if resolved is None and ":" in path_part:
        candidate_path_part, candidate_field_suffix = path_part.rsplit(":", 1)
        candidate_field_suffix = candidate_field_suffix.strip()
        if candidate_field_suffix:
            candidate_resolved = _resolve_evidence_ref_path(
                candidate_path_part,
                base_dirs=base_dirs,
                context=context,
            )
            if candidate_resolved is not None and _json_field_ref_exists(
                candidate_resolved,
                candidate_field_suffix,
            ):
                resolved = candidate_resolved
                suffix = ":" + candidate_field_suffix + suffix
    if resolved is None:
        raise ValueError(f"missing evidence ref: {ref}")
    return str(resolved) + suffix


def _json_field_ref_exists(path: Path, field_ref: str) -> bool:
    if path.suffix.lower() != ".json":
        return False
    try:
        current: Any = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    parts = [part.strip() for part in str(field_ref or "").split(".") if part.strip()]
    if not parts:
        return False
    for part in parts:
        if isinstance(current, Mapping):
            if part not in current:
                return False
            current = current[part]
            continue
        if isinstance(current, Sequence) and not isinstance(current, (str, bytes, bytearray)):
            if not part.isdigit():
                return False
            index = int(part)
            if index < 0 or index >= len(current):
                return False
            current = current[index]
            continue
        return False
    return True


def _looks_like_direct_evidence_ref(ref: str) -> bool:
    raw = str(ref).strip()
    if not raw:
        return False
    path_part, _suffix = _split_evidence_ref(raw)
    if path_part.startswith((".", "/", "~")):
        return True
    if ":" in path_part:
        _prefix, _sep, labeled_suffix = path_part.partition(":")
        if labeled_suffix.strip().startswith((".", "/", "~")):
            return True
    if "@" in path_part:
        at_suffix = path_part.rsplit("@", 1)[1].strip()
        if at_suffix.startswith((".", "/", "~")):
            return True
    return False


def _normalize_operation_log_step_ref(ref: str, *, operation_log_ref: str) -> str | None:
    raw = str(ref).strip()
    if not raw:
        return None
    range_match = re.match(
        r"^(?:operation[_ ]log\s*:\s*)?steps?\s*[-_ ]?(?P<start>\d+)(?:[-_ ]+(?P<end>\d+))?$",
        raw,
        re.IGNORECASE,
    )
    if range_match is not None:
        return f"{operation_log_ref}#op{int(str(range_match.group('start')))}"
    operation_log_match = re.match(r"^operation[_ ]log\s*[:#-]?\s*(?P<index>\d+)$", raw, re.IGNORECASE)
    if operation_log_match is not None:
        return f"{operation_log_ref}#op{int(str(operation_log_match.group('index')))}"
    candidate = raw
    match = _OPERATION_LOG_STEP_REF.fullmatch(candidate)
    if match is None and ":" in raw:
        prefix, _sep, _suffix = raw.partition(":")
        candidate = prefix.strip()
        match = _OPERATION_LOG_STEP_REF.fullmatch(candidate)
    if match is None:
        match = re.match(
            r"^(?:op|operation)\s*[-_#: ]?\s*(?P<index>\d+)(?:[_-].+)?$",
            raw,
            re.IGNORECASE,
        )
    if match is None:
        match = re.search(
            r"\b(?:op|operation)\s*[-_#: ]?\s*(?P<index>\d+)\b",
            raw,
            re.IGNORECASE,
        )
    if match is None:
        match = re.search(
            r"\bstep\s*[-_#: ]?\s*(?P<index>\d+)\b",
            raw,
            re.IGNORECASE,
        )
    if match is None:
        return None
    return f"{operation_log_ref}#op{int(str(match.group('index')))}"


def _normalize_operation_log_command_ref(ref: str, *, operation_log_ref: str) -> str | None:
    raw = str(ref).strip()
    if not raw:
        return None
    match = re.search(r"(?:^|\b)(?:cmd|command)\s*:\s*(?P<command>.+?)\s*$", raw, re.IGNORECASE)
    if match is None:
        return None
    command_text = str(match.group("command") or "").strip().strip("`")
    if not command_text:
        return None
    candidate_texts = [command_text]
    command_candidates = {command_text}
    for separator in ("->", "=>"):
        if separator not in command_text:
            continue
        command_prefix, observed_suffix = command_text.split(separator, 1)
        command_prefix = command_prefix.strip().strip("`")
        observed_suffix = observed_suffix.strip().strip("`")
        if command_prefix:
            candidate_texts.append(command_prefix)
            command_candidates.add(command_prefix)
        if observed_suffix:
            candidate_texts.append(observed_suffix)
    normalized_candidates: list[tuple[str, tuple[str, ...], bool]] = []
    seen_candidates: set[str] = set()
    for candidate_text in candidate_texts:
        normalized_candidate = _normalize_text_for_operation_log_match(candidate_text)
        if not normalized_candidate or normalized_candidate in seen_candidates:
            continue
        seen_candidates.add(normalized_candidate)
        normalized_candidates.append(
            (
                normalized_candidate,
                tuple(dict.fromkeys(_operation_log_match_tokens(candidate_text))),
                candidate_text in command_candidates,
            )
        )
    if not normalized_candidates:
        return None
    matches: list[tuple[int, int]] = []
    for step_index, block_text in _iter_operation_log_step_blocks(operation_log_ref):
        normalized_block = _normalize_text_for_operation_log_match(block_text)
        block_tokens = set(_operation_log_match_tokens(block_text))
        best_score = 0
        for normalized_candidate, candidate_tokens, is_command_candidate in normalized_candidates:
            if f"`{command_text}`" in block_text:
                best_score = max(best_score, 4)
                continue
            if normalized_candidate in normalized_block:
                best_score = max(best_score, 3 if is_command_candidate else 2)
                continue
            if len(candidate_tokens) >= 3 and all(token in block_tokens for token in candidate_tokens):
                best_score = max(best_score, 2 if is_command_candidate else 1)
        if best_score > 0:
            matches.append((best_score, step_index))
    if not matches:
        return None
    best_score = max(score for score, _step_index in matches)
    best_steps = [step_index for score, step_index in matches if score == best_score]
    if len(best_steps) == 1:
        return f"{operation_log_ref}#op{best_steps[0]}"
    return None


def _normalize_text_for_operation_log_match(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(text).lower())
    return " ".join(part for part in normalized.split() if part)


def _operation_log_match_tokens(text: str) -> list[str]:
    normalized = _normalize_text_for_operation_log_match(text)
    tokens: list[str] = []
    for token in normalized.split():
        if token in _OPERATION_LOG_TEXT_STOPWORDS:
            continue
        if len(token) <= 1 and not token.isdigit():
            continue
        tokens.append(token)
    return tokens


def _iter_operation_log_step_blocks(operation_log_ref: str) -> list[tuple[int, str]]:
    operation_log_path_part, _suffix = _split_evidence_ref(operation_log_ref)
    operation_log_path = Path(operation_log_path_part)
    if not operation_log_path.exists():
        return []
    operation_log_text = operation_log_path.read_text(encoding="utf-8")
    blocks: list[tuple[int, str]] = []
    for match in re.finditer(r"(?ms)^(?P<index>\d+)\.\s.*?(?=^\d+\.\s|\Z)", operation_log_text):
        try:
            step_index = int(str(match.group("index")))
        except Exception:
            continue
        blocks.append((step_index, str(match.group(0))))
    return blocks


def _normalize_operation_log_text_ref(ref: str, *, operation_log_ref: str) -> str | None:
    raw = str(ref).strip()
    if not raw:
        return None
    candidate_texts = [raw]
    if ":" in raw:
        colon_suffix = raw.partition(":")[2].strip()
        if colon_suffix:
            candidate_texts.append(colon_suffix)
    if raw.lower().startswith("absence of "):
        candidate_texts.append(raw[len("absence of ") :].strip())
    filename_summary_match = re.match(
        r"^(?P<label>\S+\.(?:json|log|md|txt))\s+(?P<body>.+)$",
        raw,
        re.IGNORECASE,
    )
    if filename_summary_match is not None:
        body = str(filename_summary_match.group("body")).strip()
        if body:
            candidate_texts.append(body)
    normalized_candidates: list[tuple[str, tuple[str, ...]]] = []
    seen_candidates: set[str] = set()
    for candidate_text in candidate_texts:
        normalized_candidate = _normalize_text_for_operation_log_match(candidate_text)
        if len(normalized_candidate) < 8 or normalized_candidate in seen_candidates:
            continue
        seen_candidates.add(normalized_candidate)
        token_tuple = tuple(dict.fromkeys(_operation_log_match_tokens(candidate_text)))
        normalized_candidates.append((normalized_candidate, token_tuple))
    if not normalized_candidates:
        return None
    for step_index, block_text in _iter_operation_log_step_blocks(operation_log_ref):
        normalized_block = _normalize_text_for_operation_log_match(block_text)
        block_tokens = set(_operation_log_match_tokens(block_text))
        for normalized_candidate, candidate_tokens in normalized_candidates:
            if normalized_candidate in normalized_block:
                return f"{operation_log_ref}#op{step_index}"
            if len(candidate_tokens) >= 3 and all(token in block_tokens for token in candidate_tokens):
                return f"{operation_log_ref}#op{step_index}"
    return None


def _normalize_operation_log_search_ref(ref: str, *, operation_log_ref: str) -> str | None:
    raw = str(ref).strip()
    match = re.match(r"^(?:search|find|glob)\s*:\s*(?P<target>.+)$", raw, re.IGNORECASE)
    if match is None:
        return None
    search_target = str(match.group("target") or "").strip()
    if not search_target:
        return None
    candidate_texts = [search_target]
    search_path_part, _suffix = _split_evidence_ref(search_target)
    search_path = str(search_path_part).strip()
    if search_path:
        filename = Path(search_path).name.strip()
        if filename and filename != search_path:
            candidate_texts.append(filename)
        if "/**/" in search_path:
            nested_target = search_path.rsplit("/**/", 1)[1].strip()
            if nested_target and nested_target not in candidate_texts:
                candidate_texts.append(nested_target)
    normalized_candidates: list[tuple[str, tuple[str, ...]]] = []
    seen_candidates: set[str] = set()
    for candidate_text in candidate_texts:
        normalized_candidate = _normalize_text_for_operation_log_match(candidate_text)
        if len(normalized_candidate) < 4 or normalized_candidate in seen_candidates:
            continue
        seen_candidates.add(normalized_candidate)
        token_tuple = tuple(dict.fromkeys(_operation_log_match_tokens(candidate_text)))
        normalized_candidates.append((normalized_candidate, token_tuple))
    if not normalized_candidates:
        return None
    for step_index, block_text in _iter_operation_log_step_blocks(operation_log_ref):
        normalized_block = _normalize_text_for_operation_log_match(block_text)
        if not re.search(r"\b(search|searched|find|found|grep|rg|glob)\b", normalized_block):
            continue
        block_tokens = set(_operation_log_match_tokens(block_text))
        for normalized_candidate, candidate_tokens in normalized_candidates:
            if normalized_candidate in normalized_block:
                return f"{operation_log_ref}#op{step_index}"
            if candidate_tokens and all(token in block_tokens for token in candidate_tokens):
                return f"{operation_log_ref}#op{step_index}"
    return None


def _normalize_product_manual_text_ref(ref: str, *, product_manual_ref: str) -> str | None:
    raw = str(ref).strip()
    if not raw:
        return None
    manual_path_part, _suffix = _split_evidence_ref(product_manual_ref)
    manual_path = Path(manual_path_part)
    if not manual_path.exists():
        return None
    manual_lines = manual_path.read_text(encoding="utf-8").splitlines()
    line_match = _MANUAL_LINE_REF.fullmatch(raw)
    if line_match is not None:
        line_no = int(str(line_match.group("line")))
        if 1 <= line_no <= len(manual_lines):
            return f"{manual_path.resolve()}:{line_no}"
        return None
    line_range_match = _MANUAL_LINES_REF.fullmatch(raw)
    if line_range_match is not None:
        start_line = int(str(line_range_match.group("start")))
        end_line = int(str(line_range_match.group("end")))
        if start_line > end_line:
            start_line, end_line = end_line, start_line
        if 1 <= start_line <= len(manual_lines) and 1 <= end_line <= len(manual_lines):
            return f"{manual_path.resolve()}:{start_line}#lines-{start_line}-{end_line}"
        return None
    lowered = raw.lower()
    manual_text = "\n".join(manual_lines)
    if ":" in raw:
        prefix, _sep, suffix = raw.partition(":")
        if prefix.strip().lower() in {"product_manual", "product manual", "manual"}:
            suffix_tokens = [
                token
                for token in _operation_log_match_tokens(suffix.replace("_", " "))
                if token not in _MANUAL_LABEL_DROP_TOKENS
            ]
            manual_tokens = set(_operation_log_match_tokens(manual_text))
            if not suffix_tokens or all(token in manual_tokens for token in suffix_tokens):
                return str(manual_path.resolve())
    if "product manual" not in lowered and "manual (prompt)" not in lowered:
        return None
    section_names = [str(item).strip() for item in re.findall(r"`([^`]+)`", raw) if str(item).strip()]
    if section_names and any(section_name not in manual_text for section_name in section_names):
        return None
    return str(manual_path.resolve())


def _normalize_ref_against_operation_logs(
    ref: str,
    *,
    operation_log_refs: Sequence[str],
    include_text_refs: bool = True,
) -> str | None:
    normalized_operation_logs = [str(item).strip() for item in operation_log_refs if str(item).strip()]
    if not normalized_operation_logs:
        return None
    matches: list[str] = []
    seen: set[str] = set()
    for operation_log_ref in normalized_operation_logs:
        candidates = [
            _normalize_operation_log_step_ref(ref, operation_log_ref=operation_log_ref),
            _normalize_operation_log_command_ref(ref, operation_log_ref=operation_log_ref),
            _normalize_operation_log_labeled_ref(ref, operation_log_ref=operation_log_ref),
            _normalize_operation_log_search_ref(ref, operation_log_ref=operation_log_ref),
        ]
        if include_text_refs:
            candidates.append(_normalize_operation_log_text_ref(ref, operation_log_ref=operation_log_ref))
        for candidate in candidates:
            if candidate is None:
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            matches.append(candidate)
    if len(matches) == 1:
        return matches[0]
    return None


def _normalize_runtime_evidence_ref(
    ref: str,
    *,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
    operation_log_refs: Sequence[str] = (),
    product_manual_ref: Path | str | None = None,
) -> str:
    operation_log_anchor = _normalize_ref_against_operation_logs(
        ref,
        operation_log_refs=operation_log_refs,
        include_text_refs=False,
    )
    if operation_log_anchor is not None:
        return operation_log_anchor
    file_ref_error: ValueError | None = None
    if _looks_like_direct_evidence_ref(ref):
        try:
            return _normalize_evidence_ref(ref, base_dirs=base_dirs, context=context)
        except ValueError as exc:
            file_ref_error = exc
    manual_ref = str(product_manual_ref or "").strip()
    manual_anchor = (
        _normalize_product_manual_text_ref(ref, product_manual_ref=manual_ref)
        if manual_ref
        else None
    )
    if manual_anchor is not None:
        return manual_anchor
    operation_log_anchor = _normalize_ref_against_operation_logs(
        ref,
        operation_log_refs=operation_log_refs,
        include_text_refs=True,
    )
    if operation_log_anchor is not None:
        return operation_log_anchor
    if not _looks_like_direct_evidence_ref(ref):
        try:
            return _normalize_evidence_ref(ref, base_dirs=base_dirs, context=context)
        except ValueError as exc:
            file_ref_error = exc
    if file_ref_error is not None:
        raise file_ref_error
    raise ValueError(f"missing evidence ref: {ref}")


def _collect_requirement_operation_log_refs(
    *,
    ai_user_results: Sequence[Mapping[str, Any]],
    requirement_id: str,
    base_dirs: Sequence[Path] = (),
    context: Mapping[str, Any] | None = None,
) -> list[str]:
    def _candidate_refs(*, scoped_only: bool) -> list[str]:
        collected: list[str] = []
        seen: set[str] = set()
        for item in ai_user_results:
            if not isinstance(item, Mapping):
                continue
            raw_operation_log_ref = str(item.get("operation_log_ref") or "").strip()
            if not raw_operation_log_ref:
                continue
            requirement_ids: set[str] = set()
            target_unit = item.get("target_evaluation_unit")
            if isinstance(target_unit, Mapping):
                requirement_ids.update(
                    str(ref).strip()
                    for ref in target_unit.get("requirement_ids") or []
                    if str(ref).strip()
                )
            for effect_result in item.get("effect_results") or []:
                if not isinstance(effect_result, Mapping):
                    continue
                effect_requirement_id = str(effect_result.get("requirement_id") or "").strip()
                if effect_requirement_id:
                    requirement_ids.add(effect_requirement_id)
            if scoped_only and requirement_ids and requirement_id not in requirement_ids:
                continue
            try:
                normalized_operation_log_ref = _ensure_file_refs_exist(
                    [raw_operation_log_ref],
                    base_dirs=base_dirs,
                    context=context,
                )[0]
            except ValueError:
                continue
            if normalized_operation_log_ref in seen:
                continue
            seen.add(normalized_operation_log_ref)
            collected.append(normalized_operation_log_ref)
        return collected

    scoped_refs = _candidate_refs(scoped_only=True)
    if scoped_refs:
        return scoped_refs
    return _candidate_refs(scoped_only=False)


def _normalize_operation_log_labeled_ref(ref: str, *, operation_log_ref: str) -> str | None:
    raw = str(ref).strip()
    if not raw or ":" not in raw:
        return None
    prefix, _sep, suffix = raw.partition(":")
    if prefix.strip().lower() not in {"cli", "stdout", "stderr", "operation_log", "oplog"}:
        return None
    labeled_text = suffix.replace("_", " ").replace("-", " ").strip()
    direct_match = _normalize_operation_log_text_ref(labeled_text, operation_log_ref=operation_log_ref)
    if direct_match is not None:
        return direct_match
    candidate_tokens = [
        token
        for token in _operation_log_match_tokens(labeled_text)
        if token not in _OPERATION_LOG_LABEL_DROP_TOKENS
    ]
    if not candidate_tokens:
        return None
    matches: list[int] = []
    for step_index, block_text in _iter_operation_log_step_blocks(operation_log_ref):
        block_tokens = set(_operation_log_match_tokens(block_text))
        if all(token in block_tokens for token in candidate_tokens):
            matches.append(step_index)
    if not matches:
        return None
    return f"{operation_log_ref}#op{matches[0]}"


_REQUIREMENT_KIND_PRODUCT_EFFECT = "product_effect"
_REQUIREMENT_KIND_RUNTIME_CLOSURE = "runtime_closure"


def _normalize_requirement_kind(*, requirement: Mapping[str, Any], description: str) -> str:
    raw_kind = str(requirement.get("requirement_kind") or "").strip().lower()
    if raw_kind in {_REQUIREMENT_KIND_PRODUCT_EFFECT, _REQUIREMENT_KIND_RUNTIME_CLOSURE}:
        return raw_kind
    lowered = description.lower()
    if "terminal evaluator-backed result" in lowered:
        return _REQUIREMENT_KIND_RUNTIME_CLOSURE
    if "implementer_result.json" in lowered:
        return _REQUIREMENT_KIND_RUNTIME_CLOSURE
    return _REQUIREMENT_KIND_PRODUCT_EFFECT


def requirement_kind(requirement: Mapping[str, Any]) -> str:
    description = str(requirement.get("description") or "").strip()
    return _normalize_requirement_kind(requirement=requirement, description=description)


def _validate_final_effect_requirements(
    requirements: Sequence[Mapping[str, Any]],
    *,
    label: str,
) -> list[dict[str, Any]]:
    if not isinstance(requirements, Sequence) or not requirements:
        raise ValueError(f"{label} must be a non-empty sequence")
    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in requirements:
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} items must be objects")
        requirement_id = str(item.get("requirement_id") or "").strip()
        description = str(item.get("description") or "").strip()
        if not requirement_id or not description:
            raise ValueError(f"{label} items must include requirement_id and description")
        if requirement_id in seen_ids:
            raise ValueError(f"{label} requirement_id must be unique: {requirement_id}")
        seen_ids.add(requirement_id)
        normalized.append(
            {
                "requirement_id": requirement_id,
                "description": description,
                "blocking": bool(item.get("blocking", True)),
                "requirement_kind": _normalize_requirement_kind(requirement=item, description=description),
            }
        )
    return normalized


def _build_singleton_requirement_graph(
    requirements: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_requirements = _validate_final_effect_requirements(
        requirements,
        label="singleton requirement_graph requirements",
    )
    return {
        "requirements": normalized_requirements,
        "evaluation_units": [
            {
                "unit_id": str(item["requirement_id"]),
                "requirement_ids": [str(item["requirement_id"])],
            }
            for item in normalized_requirements
        ],
        "dependency_edges": [],
    }


def _validate_requirement_graph(
    graph_obj: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    *,
    default_requirements: Sequence[Mapping[str, Any]] | None = None,
    label: str,
) -> dict[str, Any]:
    if isinstance(graph_obj, Sequence) and not isinstance(graph_obj, Mapping):
        return _build_singleton_requirement_graph(graph_obj)
    if not isinstance(graph_obj, Mapping):
        raise ValueError(f"{label} must be an object")
    requirements = _validate_final_effect_requirements(
        graph_obj.get("requirements") or default_requirements or [],
        label=f"{label} requirements",
    )
    requirement_ids = [str(item["requirement_id"]) for item in requirements]
    requirement_id_set = set(requirement_ids)
    raw_units = graph_obj.get("evaluation_units") or []
    if not isinstance(raw_units, list) or not raw_units:
        raw_units = _build_singleton_requirement_graph(requirements)["evaluation_units"]
    unit_ids: list[str] = []
    declared_unit_ids: set[str] = set()
    seen_unit_ids: set[str] = set()
    seen_requirement_ids: set[str] = set()
    evaluation_units: list[dict[str, Any]] = []
    for item in raw_units:
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} evaluation_units items must be objects")
        unit_id = str(item.get("unit_id") or "").strip()
        requirement_refs = [str(ref).strip() for ref in item.get("requirement_ids") or [] if str(ref).strip()]
        if not unit_id or not requirement_refs:
            raise ValueError(f"{label} evaluation_units items must include unit_id and non-empty requirement_ids")
        if unit_id in seen_unit_ids:
            raise ValueError(f"{label} evaluation_units unit_id must be unique: {unit_id}")
        declared_unit_ids.add(unit_id)
        if len(requirement_refs) != len(set(requirement_refs)):
            raise ValueError(f"{label} evaluation_units requirement_ids must be unique inside unit: {unit_id}")
        missing = [ref for ref in requirement_refs if ref not in requirement_id_set]
        if missing:
            raise ValueError(f"{label} evaluation_units references unknown requirement_ids: {', '.join(missing)}")
        if seen_requirement_ids:
            requirement_refs = [ref for ref in requirement_refs if ref not in seen_requirement_ids]
            if not requirement_refs:
                continue
        seen_unit_ids.add(unit_id)
        seen_requirement_ids.update(requirement_refs)
        unit_ids.append(unit_id)
        evaluation_units.append(
            {
                "unit_id": unit_id,
                "requirement_ids": requirement_refs,
            }
        )
    if seen_requirement_ids != requirement_id_set:
        missing = [rid for rid in requirement_ids if rid not in seen_requirement_ids]
        raise ValueError(f"{label} evaluation_units must cover every requirement exactly once; missing: {', '.join(missing)}")
    raw_edges = graph_obj.get("dependency_edges") or []
    if not isinstance(raw_edges, list):
        raise ValueError(f"{label} dependency_edges must be a list")
    seen_edges: set[tuple[str, str]] = set()
    dependency_edges: list[dict[str, str]] = []
    predecessors: dict[str, set[str]] = {unit_id: set() for unit_id in unit_ids}
    for item in raw_edges:
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} dependency_edges items must be objects")
        from_unit_id = str(item.get("from_unit_id") or "").strip()
        to_unit_id = str(item.get("to_unit_id") or "").strip()
        if not from_unit_id or not to_unit_id:
            raise ValueError(f"{label} dependency_edges items must include from_unit_id and to_unit_id")
        if from_unit_id not in declared_unit_ids or to_unit_id not in declared_unit_ids:
            raise ValueError(f"{label} dependency_edges must reference known evaluation units")
        if from_unit_id not in seen_unit_ids or to_unit_id not in seen_unit_ids:
            continue
        if from_unit_id == to_unit_id:
            raise ValueError(f"{label} dependency_edges cannot self-reference unit_id: {from_unit_id}")
        edge = (from_unit_id, to_unit_id)
        if edge in seen_edges:
            continue
        seen_edges.add(edge)
        predecessors[to_unit_id].add(from_unit_id)
        dependency_edges.append({"from_unit_id": from_unit_id, "to_unit_id": to_unit_id})
    remaining = {unit_id: set(preds) for unit_id, preds in predecessors.items()}
    ready = [unit_id for unit_id in unit_ids if not remaining[unit_id]]
    visited: list[str] = []
    while ready:
        current = ready.pop(0)
        visited.append(current)
        for edge in dependency_edges:
            if edge["from_unit_id"] != current:
                continue
            target = edge["to_unit_id"]
            remaining[target].discard(current)
            if not remaining[target] and target not in visited and target not in ready:
                ready.append(target)
    if len(visited) != len(unit_ids):
        raise ValueError(f"{label} dependency_edges must be acyclic")
    return {
        "requirements": requirements,
        "evaluation_units": evaluation_units,
        "dependency_edges": dependency_edges,
    }


def _derive_runtime_status(
    *,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    effect_reviews: Sequence[Mapping[str, Any]],
) -> str:
    verdict_by_requirement = {
        str(item.get("requirement_id") or "").strip(): str(item.get("verdict") or "").strip().upper()
        for item in effect_reviews
    }
    blocking_ids = [
        str(item.get("requirement_id") or "").strip()
        for item in final_effect_requirements
        if bool(item.get("blocking", True))
    ]
    relevant_ids = blocking_ids or [str(item.get("requirement_id") or "").strip() for item in final_effect_requirements]
    relevant_verdicts = [verdict_by_requirement.get(rid, "INCONCLUSIVE") for rid in relevant_ids]
    if any(verdict == "FAIL" for verdict in relevant_verdicts):
        return "FAIL"
    if any(verdict != "PASS" for verdict in relevant_verdicts):
        return "INCONCLUSIVE"
    return "PASS"


def _compact_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _next_checker_sibling_id(
    *,
    existing_ids: set[str],
    source_id: str,
    suffix: str,
) -> str:
    candidate = f"{source_id}{suffix}"
    if candidate not in existing_ids:
        return candidate
    counter = 2
    while f"{candidate}_{counter}" in existing_ids:
        counter += 1
    return f"{candidate}_{counter}"


def _to_gerund_phrase(text: str) -> str:
    normalized = _compact_whitespace(text)
    if not normalized:
        return normalized
    first, separator, remainder = normalized.partition(" ")
    lowered = first.lower()
    if lowered.endswith("ing"):
        gerund = first
    elif lowered in {"be": "being", "do": "doing", "go": "going", "have": "having"}:
        gerund = {"be": "being", "do": "doing", "go": "going", "have": "having"}[lowered]
    elif lowered.endswith("ie"):
        gerund = f"{lowered[:-2]}ying"
    elif lowered.endswith("e") and not lowered.endswith(("ee", "oe", "ye")):
        gerund = f"{lowered[:-1]}ing"
    elif (
        len(lowered) >= 3
        and lowered[-1] not in "aeiouwxy"
        and lowered[-2] in "aeiou"
        and lowered[-3] not in "aeiou"
    ):
        gerund = f"{lowered}{lowered[-1]}ing"
    else:
        gerund = f"{lowered}ing"
    if first[:1].isupper():
        gerund = gerund[:1].upper() + gerund[1:]
    return f"{gerund}{separator}{remainder}" if separator else gerund


def _split_requirement_embedded_guard(description: str) -> tuple[str, str] | None:
    normalized = _compact_whitespace(description)
    match = re.match(
        r"^(?P<head>.+?)\s*,?\s*without requiring\s+(?P<tail>.+?)(?:\.)?$",
        normalized,
        flags=re.IGNORECASE,
    )
    if match is None:
        return None
    guard_tail = _compact_whitespace(match.group("tail")).rstrip(".")
    if " before " not in guard_tail.lower() or "exist" not in guard_tail.lower():
        return None
    positive_description = f"{match.group('head').strip().rstrip(' ,;:.')}."
    subject_match = re.match(
        r"^(?P<subject>(?:the|that|this)\s+.+?)\s+to\s+(?P<predicate>.+)$",
        guard_tail,
        flags=re.IGNORECASE,
    )
    if subject_match is None:
        guard_description = f"The current run does not require {_to_gerund_phrase(guard_tail)}."
    else:
        subject = _compact_whitespace(subject_match.group("subject"))
        predicate = _to_gerund_phrase(subject_match.group("predicate"))
        guard_description = f"{subject[:1].upper() + subject[1:]} does not require {predicate}."
    return positive_description, guard_description


def _repair_checker_requirement_graph_guard_split(
    requirement_graph: Mapping[str, Any],
    *,
    source_requirement_id: str,
    synthesized_requirement: Mapping[str, Any],
) -> dict[str, Any]:
    repaired_graph = dict(requirement_graph)
    raw_requirements = repaired_graph.get("requirements") or []
    if isinstance(raw_requirements, list):
        graph_requirements = [dict(item) for item in raw_requirements]
    else:
        graph_requirements = []
    synthesized_requirement_id = str(synthesized_requirement.get("requirement_id") or "").strip()
    existing_graph_requirement_ids = {
        str(item.get("requirement_id") or "").strip()
        for item in graph_requirements
        if isinstance(item, Mapping)
    }
    if synthesized_requirement_id and synthesized_requirement_id not in existing_graph_requirement_ids:
        insert_at = next(
            (
                index + 1
                for index, item in enumerate(graph_requirements)
                if str(item.get("requirement_id") or "").strip() == source_requirement_id
            ),
            len(graph_requirements),
        )
        graph_requirements.insert(insert_at, dict(synthesized_requirement))
    repaired_graph["requirements"] = graph_requirements
    raw_units = repaired_graph.get("evaluation_units") or []
    if not isinstance(raw_units, list) or not raw_units:
        return repaired_graph
    evaluation_units = [dict(item) for item in raw_units]
    if any(
        synthesized_requirement_id in [str(ref).strip() for ref in unit.get("requirement_ids") or []]
        for unit in evaluation_units
    ):
        return repaired_graph
    source_unit_index = None
    source_unit_id = ""
    for index, unit in enumerate(evaluation_units):
        requirement_ids = [str(ref).strip() for ref in unit.get("requirement_ids") or [] if str(ref).strip()]
        if source_requirement_id in requirement_ids:
            source_unit_index = index
            source_unit_id = str(unit.get("unit_id") or "").strip()
            break
    if source_unit_index is None:
        return repaired_graph
    existing_unit_ids = {str(item.get("unit_id") or "").strip() for item in evaluation_units}
    synthesized_unit_id = _next_checker_sibling_id(
        existing_ids=existing_unit_ids,
        source_id=source_unit_id or source_requirement_id,
        suffix="__guard",
    )
    evaluation_units.insert(
        source_unit_index + 1,
        {
            "unit_id": synthesized_unit_id,
            "requirement_ids": [synthesized_requirement_id],
        },
    )
    raw_edges = repaired_graph.get("dependency_edges") or []
    if not isinstance(raw_edges, list):
        repaired_graph["evaluation_units"] = evaluation_units
        return repaired_graph
    dependency_edges = [dict(item) for item in raw_edges]
    seen_edges = {
        (str(item.get("from_unit_id") or "").strip(), str(item.get("to_unit_id") or "").strip())
        for item in dependency_edges
        if isinstance(item, Mapping)
    }
    cloned_edges: list[dict[str, str]] = []
    for edge in dependency_edges:
        from_unit_id = str(edge.get("from_unit_id") or "").strip()
        to_unit_id = str(edge.get("to_unit_id") or "").strip()
        if to_unit_id == source_unit_id:
            candidate = (from_unit_id, synthesized_unit_id)
            if candidate not in seen_edges:
                seen_edges.add(candidate)
                cloned_edges.append({"from_unit_id": from_unit_id, "to_unit_id": synthesized_unit_id})
        if from_unit_id == source_unit_id:
            candidate = (synthesized_unit_id, to_unit_id)
            if candidate not in seen_edges:
                seen_edges.add(candidate)
                cloned_edges.append({"from_unit_id": synthesized_unit_id, "to_unit_id": to_unit_id})
    repaired_graph["evaluation_units"] = evaluation_units
    repaired_graph["dependency_edges"] = dependency_edges + cloned_edges
    return repaired_graph


def _repair_checker_guarded_requirement_splits(
    *,
    normalized_requirements: Sequence[Mapping[str, Any]],
    repair_actions: Sequence[Mapping[str, Any]],
    requirement_graph: Mapping[str, Any],
    notes: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[str]]:
    repaired_requirements = [dict(item) for item in normalized_requirements]
    repaired_actions = [dict(item) for item in repair_actions]
    repaired_graph = dict(requirement_graph)
    repaired_notes = [str(item) for item in notes if str(item).strip()]
    changed = False
    index = 0
    while index < len(repaired_requirements):
        requirement = repaired_requirements[index]
        source_requirement_id = str(requirement.get("requirement_id") or "").strip()
        split = _split_requirement_embedded_guard(str(requirement.get("description") or ""))
        if split is None:
            index += 1
            continue
        positive_description, guard_description = split
        if str(requirement.get("description") or "").strip() != positive_description:
            requirement["description"] = positive_description
            changed = True
        existing_guard = next(
            (
                item
                for item in repaired_requirements
                if _compact_whitespace(str(item.get("description") or "")).lower() == guard_description.lower()
            ),
            None,
        )
        if existing_guard is None:
            existing_ids = {str(item.get("requirement_id") or "").strip() for item in repaired_requirements}
            guard_requirement = {
                "requirement_id": _next_checker_sibling_id(
                    existing_ids=existing_ids,
                    source_id=source_requirement_id,
                    suffix="__guard",
                ),
                "description": guard_description,
                "blocking": bool(requirement.get("blocking", True)),
            }
            repaired_requirements.insert(index + 1, guard_requirement)
            changed = True
        else:
            guard_requirement = {
                "requirement_id": str(existing_guard.get("requirement_id") or "").strip(),
                "description": str(existing_guard.get("description") or "").strip(),
                "blocking": bool(existing_guard.get("blocking", True)),
            }
        guard_requirement_id = str(guard_requirement["requirement_id"])
        repaired_graph = _repair_checker_requirement_graph_guard_split(
            repaired_graph,
            source_requirement_id=source_requirement_id,
            synthesized_requirement=guard_requirement,
        )
        for action in repaired_actions:
            emitted_ids = [str(ref).strip() for ref in action.get("emitted_requirement_ids") or [] if str(ref).strip()]
            raw_item = str(action.get("raw_item") or "")
            action_label = str(action.get("action") or "")
            if source_requirement_id not in emitted_ids:
                continue
            if "without requiring" not in raw_item.lower() and "split" not in action_label.lower():
                continue
            if guard_requirement_id in emitted_ids:
                continue
            source_position = emitted_ids.index(source_requirement_id)
            emitted_ids.insert(source_position + 1, guard_requirement_id)
            action["emitted_requirement_ids"] = emitted_ids
            if "guard" not in action_label.lower() and "does not require" not in action_label.lower():
                action["action"] = f"{action_label.rstrip('.')} and recovered a dedicated guard requirement".strip()
            changed = True
        index += 1
    if changed:
        repair_note = (
            "Validator split embedded no-premature dependency guards into dedicated requirements when checker output "
            "collapsed them into a positive evidence-preservation item."
        )
        if repair_note not in repaired_notes:
            repaired_notes.append(repair_note)
    return repaired_requirements, repaired_actions, repaired_graph, repaired_notes


def _validated_checker_result_payload(validated_checker_result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": "OK",
        "normalized_requirements": [dict(item) for item in validated_checker_result.get("normalized_requirements") or []],
        "requirement_graph": dict(validated_checker_result.get("requirement_graph") or {}),
        "repair_actions": [dict(item) for item in validated_checker_result.get("repair_actions") or []],
        "human_gate_reasons": [],
        "notes": [str(item) for item in validated_checker_result.get("notes") or [] if str(item).strip()],
    }


def _render_validated_checker_summary(validated_checker_result: Mapping[str, Any]) -> str:
    requirement_count = len(validated_checker_result.get("normalized_requirements") or [])
    repair_count = len(validated_checker_result.get("repair_actions") or [])
    summary = f"Accepted {requirement_count} normalized requirements."
    if repair_count:
        noun = "repair action" if repair_count == 1 else "repair actions"
        summary = f"{summary} Recorded {repair_count} {noun}."
    return summary


def _materialize_validated_checker_artifacts(
    *,
    role_dir: Path,
    validated_checker_result: Mapping[str, Any],
) -> None:
    artifact_root = _role_runtime_artifact_root(role_dir, role_dir.name)
    canonical_payload = _validated_checker_result_payload(validated_checker_result)
    summary_markdown = _render_validated_checker_summary(validated_checker_result)
    result_targets = [role_dir / "result.json"]
    response_targets = [role_dir / "response.md"]
    if (artifact_root / "result.json").resolve() != result_targets[0].resolve():
        result_targets.append(artifact_root / "result.json")
    if (artifact_root / "response.md").resolve() != response_targets[0].resolve():
        response_targets.append(artifact_root / "response.md")
    for target in result_targets:
        _write_json(target, canonical_payload)
    for target in response_targets:
        _write_text(target, summary_markdown.rstrip() + "\n")

    structured_targets = [role_dir / "structured_response.json"]
    artifact_structured = artifact_root / "structured_response.json"
    if artifact_structured.resolve() != structured_targets[0].resolve():
        structured_targets.append(artifact_structured)
    for structured_response_path in structured_targets:
        if not structured_response_path.exists():
            continue
        envelope_obj = _load_json(structured_response_path)
        if not isinstance(envelope_obj, Mapping):
            envelope_obj = {}
        updated_envelope = dict(envelope_obj)
        updated_envelope["result"] = canonical_payload
        updated_envelope["summary_markdown"] = summary_markdown
        _write_json(structured_response_path, updated_envelope)


def _validate_checker_repair_actions(
    repair_actions: Sequence[Mapping[str, Any]],
    *,
    normalized_requirements: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    normalized_ids = {str(item.get("requirement_id") or "").strip() for item in normalized_requirements}
    validated: list[dict[str, Any]] = []
    for item in repair_actions:
        if not isinstance(item, Mapping):
            raise ValueError("checker repair_actions items must be objects")
        raw_item = str(item.get("raw_item") or "").strip()
        action = str(item.get("action") or "").strip()
        emitted_ids = [str(ref).strip() for ref in item.get("emitted_requirement_ids") or [] if str(ref).strip()]
        if not raw_item or not action:
            raise ValueError("checker repair_actions items must include raw_item and action")
        if any(ref not in normalized_ids for ref in emitted_ids):
            raise ValueError("checker repair_actions emitted_requirement_ids must reference normalized requirement_ids")
        validated.append(
            {
                "raw_item": raw_item,
                "action": action,
                "emitted_requirement_ids": emitted_ids,
            }
        )
    return validated


def _repair_checker_requirement_graph_dedup_units(
    requirement_graph: Mapping[str, Any],
    *,
    repair_actions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    raw_units = requirement_graph.get("evaluation_units") or []
    if not isinstance(raw_units, list) or not raw_units:
        return dict(requirement_graph)
    deduplicated_ids = {
        str(requirement_id).strip()
        for action in repair_actions
        if str(action.get("action") or "").strip().startswith("deduplicated_with_existing_requirement")
        for requirement_id in (action.get("emitted_requirement_ids") or [])
        if str(requirement_id).strip()
    }
    if not deduplicated_ids:
        return dict(requirement_graph)
    repaired_units: list[dict[str, Any]] = []
    seen_requirement_ids: set[str] = set()
    changed = False
    dropped_unit_ids: set[str] = set()
    for item in raw_units:
        if not isinstance(item, Mapping):
            repaired_units.append(dict(item))
            continue
        unit_id = str(item.get("unit_id") or "").strip()
        requirement_ids = [str(ref).strip() for ref in item.get("requirement_ids") or [] if str(ref).strip()]
        filtered_ids = [
            requirement_id
            for requirement_id in requirement_ids
            if requirement_id not in seen_requirement_ids or requirement_id not in deduplicated_ids
        ]
        if filtered_ids != requirement_ids:
            changed = True
        if not filtered_ids:
            dropped_unit_ids.add(unit_id)
            continue
        seen_requirement_ids.update(filtered_ids)
        repaired_units.append(
            {
                "unit_id": unit_id,
                "requirement_ids": filtered_ids,
            }
        )
    if not changed:
        return dict(requirement_graph)
    surviving_unit_ids = {str(item.get("unit_id") or "").strip() for item in repaired_units}
    repaired_edges = []
    for edge in requirement_graph.get("dependency_edges") or []:
        if not isinstance(edge, Mapping):
            repaired_edges.append(dict(edge))
            continue
        from_unit_id = str(edge.get("from_unit_id") or "").strip()
        to_unit_id = str(edge.get("to_unit_id") or "").strip()
        if from_unit_id in surviving_unit_ids and to_unit_id in surviving_unit_ids:
            repaired_edges.append(
                {
                    "from_unit_id": from_unit_id,
                    "to_unit_id": to_unit_id,
                }
            )
    return {
        "requirements": list(requirement_graph.get("requirements") or []),
        "evaluation_units": repaired_units,
        "dependency_edges": repaired_edges,
    }


def _checker_surface_tokens(text: str) -> list[str]:
    stopwords = {
        "a",
        "an",
        "and",
        "are",
        "as",
        "be",
        "been",
        "being",
        "but",
        "by",
        "can",
        "do",
        "does",
        "for",
        "from",
        "has",
        "have",
        "in",
        "into",
        "is",
        "it",
        "its",
        "of",
        "on",
        "or",
        "should",
        "that",
        "the",
        "their",
        "them",
        "these",
        "this",
        "those",
        "to",
        "was",
        "were",
        "with",
        "without",
    }
    return [token for token in re.findall(r"[a-z0-9]+", str(text).lower()) if token and token not in stopwords]


def _checker_requirements_share_probe_surface(left: str, right: str) -> bool:
    left_tokens = _checker_surface_tokens(left)
    right_tokens = _checker_surface_tokens(right)
    if len(left_tokens) >= 5 and len(right_tokens) >= 5 and tuple(left_tokens[:5]) == tuple(right_tokens[:5]):
        return True
    if len(left_tokens) >= 3 and len(right_tokens) >= 3 and tuple(left_tokens[-3:]) == tuple(right_tokens[-3:]):
        return True
    return False


def _repair_checker_requirement_graph_compact_singleton_units(
    requirement_graph: Mapping[str, Any],
    *,
    normalized_requirements: Sequence[Mapping[str, Any]],
    repair_actions: Sequence[Mapping[str, Any]],
    notes: Sequence[str],
) -> tuple[dict[str, Any], list[str]]:
    raw_units = requirement_graph.get("evaluation_units") or []
    if not isinstance(raw_units, list) or len(raw_units) < 6:
        return dict(requirement_graph), [str(item) for item in notes if str(item).strip()]

    ordered_units: list[dict[str, str]] = []
    requirement_to_unit: dict[str, str] = {}
    for item in raw_units:
        if not isinstance(item, Mapping):
            return dict(requirement_graph), [str(note) for note in notes if str(note).strip()]
        unit_id = str(item.get("unit_id") or "").strip()
        requirement_ids = [str(ref).strip() for ref in item.get("requirement_ids") or [] if str(ref).strip()]
        if not unit_id or len(requirement_ids) != 1:
            return dict(requirement_graph), [str(note) for note in notes if str(note).strip()]
        requirement_id = requirement_ids[0]
        ordered_units.append({"unit_id": unit_id, "requirement_id": requirement_id})
        requirement_to_unit[requirement_id] = unit_id

    requirement_by_id = {
        str(item.get("requirement_id") or "").strip(): dict(item)
        for item in normalized_requirements
        if str(item.get("requirement_id") or "").strip()
    }
    if not ordered_units or any(item["requirement_id"] not in requirement_by_id for item in ordered_units):
        return dict(requirement_graph), [str(note) for note in notes if str(note).strip()]

    edge_pairs = {
        (str(item.get("from_unit_id") or "").strip(), str(item.get("to_unit_id") or "").strip())
        for item in (requirement_graph.get("dependency_edges") or [])
        if isinstance(item, Mapping)
    }

    parent = {item["unit_id"]: item["unit_id"] for item in ordered_units}

    def _find(unit_id: str) -> str:
        while parent[unit_id] != unit_id:
            parent[unit_id] = parent[parent[unit_id]]
            unit_id = parent[unit_id]
        return unit_id

    def _union(left_unit_id: str, right_unit_id: str) -> bool:
        left_root = _find(left_unit_id)
        right_root = _find(right_unit_id)
        if left_root == right_root:
            return False
        left_index = next(index for index, item in enumerate(ordered_units) if item["unit_id"] == left_root)
        right_index = next(index for index, item in enumerate(ordered_units) if item["unit_id"] == right_root)
        if left_index <= right_index:
            parent[right_root] = left_root
        else:
            parent[left_root] = right_root
        return True

    changed = False
    for action in repair_actions:
        requirement_ids = [
            str(ref).strip()
            for ref in (action.get("emitted_requirement_ids") or [])
            if str(ref).strip() in requirement_to_unit
        ]
        if len(requirement_ids) < 2:
            continue
        unit_ids = [requirement_to_unit[requirement_id] for requirement_id in requirement_ids]
        if any((left, right) in edge_pairs or (right, left) in edge_pairs for left in unit_ids for right in unit_ids if left != right):
            continue
        for left_unit_id, right_unit_id in zip(unit_ids, unit_ids[1:]):
            changed = _union(left_unit_id, right_unit_id) or changed

    for current, following in zip(ordered_units, ordered_units[1:]):
        left_unit_id = current["unit_id"]
        right_unit_id = following["unit_id"]
        if _find(left_unit_id) == _find(right_unit_id):
            continue
        if (left_unit_id, right_unit_id) in edge_pairs or (right_unit_id, left_unit_id) in edge_pairs:
            continue
        left_description = str(requirement_by_id[current["requirement_id"]].get("description") or "")
        right_description = str(requirement_by_id[following["requirement_id"]].get("description") or "")
        if _checker_requirements_share_probe_surface(left_description, right_description):
            changed = _union(left_unit_id, right_unit_id) or changed

    if not changed:
        return dict(requirement_graph), [str(note) for note in notes if str(note).strip()]

    grouped_units: list[dict[str, Any]] = []
    representative_to_unit_id: dict[str, str] = {}
    old_to_new_unit_id: dict[str, str] = {}
    for item in ordered_units:
        representative = _find(item["unit_id"])
        new_unit_id = representative_to_unit_id.setdefault(representative, representative)
        old_to_new_unit_id[item["unit_id"]] = new_unit_id
        if grouped_units and grouped_units[-1]["unit_id"] == new_unit_id:
            grouped_units[-1]["requirement_ids"].append(item["requirement_id"])
            continue
        grouped_units.append(
            {
                "unit_id": new_unit_id,
                "requirement_ids": [item["requirement_id"]],
            }
        )

    repaired_edges: list[dict[str, str]] = []
    seen_edges: set[tuple[str, str]] = set()
    for item in requirement_graph.get("dependency_edges") or []:
        if not isinstance(item, Mapping):
            continue
        from_unit_id = old_to_new_unit_id.get(str(item.get("from_unit_id") or "").strip(), "")
        to_unit_id = old_to_new_unit_id.get(str(item.get("to_unit_id") or "").strip(), "")
        if not from_unit_id or not to_unit_id or from_unit_id == to_unit_id:
            continue
        edge = (from_unit_id, to_unit_id)
        if edge in seen_edges:
            continue
        seen_edges.add(edge)
        repaired_edges.append({"from_unit_id": from_unit_id, "to_unit_id": to_unit_id})

    repaired_notes = [str(note) for note in notes if str(note).strip()]
    compaction_note = (
        "Validator compacted singleton evaluation units into co-testable groups when repair-actions or shared "
        "probe-surface wording made separate AI-as-User runs unnecessary."
    )
    if compaction_note not in repaired_notes:
        repaired_notes.append(compaction_note)
    return {
        "requirements": list(requirement_graph.get("requirements") or []),
        "evaluation_units": grouped_units,
        "dependency_edges": repaired_edges,
    }, repaired_notes


def _validate_checker_result(result: Mapping[str, Any]) -> dict[str, Any]:
    status = str(result.get("status") or "").strip().upper()
    if status == "HUMAN_GATE":
        raw_human_gate_reasons = result.get("human_gate_reasons")
        raw_notes = result.get("notes")
        if not isinstance(raw_human_gate_reasons, list) or not [
            str(item) for item in raw_human_gate_reasons if str(item).strip()
        ]:
            raise ValueError("checker HUMAN_GATE must include non-empty human_gate_reasons")
        if not isinstance(raw_notes, list):
            raise ValueError("checker HUMAN_GATE must include notes")
        human_gate_reasons = [str(item) for item in raw_human_gate_reasons if str(item).strip()]
        raise CheckerHumanGateError(human_gate_reasons)
    if status == "FORMAT_ERROR":
        format_errors = [str(item) for item in result.get("format_errors") or [] if str(item).strip()]
        detail = "; ".join(format_errors) if format_errors else "checker returned deprecated FORMAT_ERROR"
        raise ValueError(f"checker returned deprecated FORMAT_ERROR instead of OK or HUMAN_GATE: {detail}")
    if status != "OK":
        raise ValueError(f"checker returned unsupported status: {status or '<empty>'}")
    normalized_requirements = _validate_final_effect_requirements(
        result.get("normalized_requirements") or [],
        label="checker normalized_requirements",
    )
    raw_notes = result.get("notes")
    if not isinstance(raw_notes, list):
        raise ValueError("checker result must include notes")
    normalized_requirements, raw_repair_actions, raw_requirement_graph, notes = _repair_checker_guarded_requirement_splits(
        normalized_requirements=normalized_requirements,
        repair_actions=result.get("repair_actions") or [],
        requirement_graph=result.get("requirement_graph") or {"requirements": normalized_requirements},
        notes=[str(item) for item in raw_notes if str(item).strip()],
    )
    repair_actions = _validate_checker_repair_actions(
        raw_repair_actions,
        normalized_requirements=normalized_requirements,
    )
    repaired_requirement_graph, notes = _repair_checker_requirement_graph_compact_singleton_units(
        _repair_checker_requirement_graph_dedup_units(
            raw_requirement_graph,
            repair_actions=repair_actions,
        ),
        normalized_requirements=normalized_requirements,
        repair_actions=repair_actions,
        notes=notes,
    )
    requirement_graph = _validate_requirement_graph(
        repaired_requirement_graph,
        default_requirements=normalized_requirements,
        label="checker requirement_graph",
    )
    return {
        "normalized_requirements": normalized_requirements,
        "requirement_graph": requirement_graph,
        "repair_actions": repair_actions,
        "notes": notes,
    }


def _validate_test_designer_result(
    result: Mapping[str, Any],
    *,
    request: Mapping[str, Any] | None = None,
    workspace_root: Path | None = None,
    run_root: Path | None = None,
    result_ref: Path | None = None,
) -> dict[str, Any]:
    if str(result.get("status") or "") != "OK":
        raise ValueError("ordinary-test agent result must have status=OK")
    ordinary_test_results = result.get("ordinary_test_results")
    if not isinstance(ordinary_test_results, Mapping):
        raise ValueError("ordinary-test agent must return ordinary_test_results")
    executed_tests = ordinary_test_results.get("executed_tests")
    if not isinstance(executed_tests, list) or not executed_tests:
        raise ValueError("ordinary-test agent must return non-empty ordinary_test_results.executed_tests")
    request_obj = dict(request or {})
    current_workspace_root = Path(workspace_root) if workspace_root is not None else None
    current_run_root = Path(run_root) if run_root is not None else None
    base_dirs = tuple(path for path in (result_ref.parent if result_ref is not None else None, current_run_root, current_workspace_root) if path is not None)
    resolution_context = _build_evidence_runtime_context(
        result_ref=result_ref,
        run_root=current_run_root,
        workspace_root=current_workspace_root,
        source_workspace_root=current_workspace_root,
        base_dirs=base_dirs,
    )
    normalized: list[dict[str, Any]] = []
    seen_test_ids: set[str] = set()
    for item in executed_tests:
        if not isinstance(item, Mapping):
            raise ValueError("ordinary_test_results.executed_tests items must be objects")
        test_id = str(item.get("test_id") or "").strip()
        test_kind = str(item.get("test_kind") or "").strip()
        argv = [str(arg) for arg in item.get("argv") or [] if str(arg)]
        if current_workspace_root is not None:
            argv = _normalize_generated_test_python_argv(argv=argv, workspace_root=current_workspace_root)
        if not test_id or not test_kind or not argv:
            raise ValueError("ordinary_test_results.executed_tests items must include test_id, test_kind, and argv")
        if test_id in seen_test_ids:
            raise ValueError(f"ordinary_test_results.executed_tests test_id must be unique: {test_id}")
        passed_raw = item.get("passed")
        if not isinstance(passed_raw, bool):
            raise ValueError("ordinary_test_results.executed_tests passed must be a boolean")
        try:
            exit_code = int(item.get("exit_code"))
        except (TypeError, ValueError) as exc:
            raise ValueError("ordinary_test_results.executed_tests exit_code must be an integer") from exc
        normalized_refs = _ensure_file_refs_exist(
            [
                str(item.get("stdout_ref") or "").strip(),
                str(item.get("stderr_ref") or "").strip(),
                str(item.get("exec_span_ref") or "").strip(),
            ],
            base_dirs=base_dirs,
            context=resolution_context,
        )
        stdout_ref, stderr_ref, exec_span_ref = normalized_refs
        if not stdout_ref or not stderr_ref or not exec_span_ref:
            raise ValueError("ordinary_test_results.executed_tests refs must be non-empty existing file refs")
        if current_run_root is not None:
            for label, ref in (
                ("stdout_ref", stdout_ref),
                ("stderr_ref", stderr_ref),
                ("exec_span_ref", exec_span_ref),
            ):
                resolved = _resolve_evidence_ref_path(ref, base_dirs=base_dirs, context=resolution_context)
                if resolved is None or not _is_within(resolved, current_run_root):
                    raise ValueError(f"ordinary_test_results.executed_tests {label} must stay under run_root")
        if (
            request_obj
            and current_workspace_root is not None
            and _request_is_packaged_self_eval_fixture(request_obj)
            and _generated_test_recursively_invokes_packaged_self_eval(
                argv=argv,
                cwd=".",
                workspace_root=current_workspace_root,
            )
        ):
            raise ValueError(
                "packaged self-eval fixture runs must not recursively launch another packaged self-eval fixture through the evaluator entrypoint (raw or supervised)"
            )
        seen_test_ids.add(test_id)
        normalized.append(
            {
                "test_id": test_id,
                "test_kind": test_kind,
                "argv": argv,
                "passed": passed_raw,
                "exit_code": exit_code,
                "stdout_ref": stdout_ref,
                "stderr_ref": stderr_ref,
                "exec_span_ref": exec_span_ref,
            }
        )
    return {
        **(
            {"generated_tests_ref": str(result_ref)}
            if result_ref is not None
            else {}
        ),
        "all_passed": all(bool(item["passed"]) for item in normalized),
        "executed_tests": normalized,
    }


def _remap_staged_ordinary_test_refs(
    *,
    result: Mapping[str, Any],
    staged_artifact_root: Path,
    canonical_artifact_root: Path,
) -> dict[str, Any]:
    ordinary_test_results = result.get("ordinary_test_results")
    if not isinstance(ordinary_test_results, Mapping):
        return dict(result)
    remapped_result = json.loads(json.dumps(result))
    staged_root = staged_artifact_root.resolve()
    canonical_root = canonical_artifact_root.resolve()
    staged_workspace_root: Path | None = None
    internal_dir = staged_root.parent.parent
    if internal_dir.name == ".loop_evaluator_internal":
        staged_workspace_root = internal_dir.parent.resolve()
    if staged_artifact_root.exists():
        if canonical_artifact_root.exists():
            shutil.rmtree(canonical_artifact_root)
        shutil.copytree(staged_artifact_root, canonical_artifact_root)
    for item in (remapped_result.get("ordinary_test_results", {}).get("executed_tests") or []):
        if not isinstance(item, Mapping):
            continue
        for key in ("stdout_ref", "stderr_ref", "exec_span_ref"):
            raw_ref = str(item.get(key) or "").strip()
            if not raw_ref:
                continue
            candidate_paths: list[Path] = []
            ref_path = Path(raw_ref)
            if ref_path.is_absolute():
                candidate_paths.append(ref_path.resolve(strict=False))
                candidate_paths.extend(
                    collapsed.resolve(strict=False)
                    for collapsed in _collapse_adjacent_duplicate_path_segments(ref_path)
                )
            else:
                if staged_workspace_root is not None:
                    candidate_paths.append((staged_workspace_root / ref_path).resolve())
                candidate_paths.append((staged_root / ref_path).resolve())
            for candidate in candidate_paths:
                try:
                    relative = candidate.relative_to(staged_root)
                except ValueError:
                    continue
                item[key] = str((canonical_root / relative).resolve())
                break
    return remapped_result
def _validate_ai_user_effect_result(
    *,
    result: Mapping[str, Any],
    target_unit: Mapping[str, Any],
    result_ref: Path,
    workspace_root: Path,
    run_root: Path,
    source_workspace_root: Path | str | None = None,
    product_manual_ref: Path | str | None = None,
) -> dict[str, Any]:
    if str(result.get("status") or "") != "OK":
        raise ValueError("AI-as-User result must have status=OK")
    operation_log_ref = str(result.get("operation_log_ref") or "").strip()
    if not operation_log_ref:
        raise ValueError("AI-as-User result must include operation_log_ref")
    base_dirs = (result_ref.parent, run_root, workspace_root)
    resolution_context = _build_evidence_runtime_context(
        result_ref=result_ref,
        run_root=run_root,
        workspace_root=workspace_root,
        source_workspace_root=source_workspace_root,
        product_manual_ref=product_manual_ref,
        base_dirs=base_dirs,
    )
    operation_log_ref = _ensure_file_refs_exist(
        [operation_log_ref],
        base_dirs=base_dirs,
        context=resolution_context,
    )[0]
    resolution_context = _build_evidence_runtime_context(
        result_ref=result_ref,
        run_root=run_root,
        workspace_root=workspace_root,
        source_workspace_root=source_workspace_root,
        product_manual_ref=product_manual_ref,
        operation_log_ref=_split_evidence_ref(operation_log_ref)[0],
        base_dirs=base_dirs,
    )
    expected_unit_id = str(target_unit.get("unit_id") or "").strip()
    unit_id = str(result.get("unit_id") or "").strip() or expected_unit_id
    if unit_id != expected_unit_id:
        raise ValueError("AI-as-User unit_id must match the target evaluation unit")
    effect_results_raw = result.get("effect_results")
    if effect_results_raw is None and isinstance(result.get("effect_result"), Mapping):
        effect_results_raw = [result.get("effect_result")]
    if not isinstance(effect_results_raw, list) or not effect_results_raw:
        raise ValueError("AI-as-User result must include non-empty effect_results")
    expected_requirement_ids = [str(ref).strip() for ref in target_unit.get("requirement_ids") or [] if str(ref).strip()]
    seen_ids: set[str] = set()
    normalized_effect_results: list[dict[str, Any]] = []
    for effect_result in effect_results_raw:
        if not isinstance(effect_result, Mapping):
            raise ValueError("AI-as-User effect_results items must be objects")
        requirement_id = str(effect_result.get("requirement_id") or "").strip()
        if requirement_id not in expected_requirement_ids:
            raise ValueError("AI-as-User effect_results must stay inside the target evaluation unit")
        if requirement_id in seen_ids:
            raise ValueError(f"AI-as-User effect_results requirement_id must be unique inside a unit: {requirement_id}")
        outcome = str(effect_result.get("outcome") or "").strip()
        summary = str(effect_result.get("summary") or "").strip()
        evidence_refs = [str(ref) for ref in effect_result.get("evidence_refs") or [] if str(ref).strip()]
        if not outcome:
            raise ValueError("AI-as-User effect_results must include a non-empty outcome string")
        if not summary or not evidence_refs:
            raise ValueError("AI-as-User effect_results must include summary and evidence_refs")
        normalized_refs = [
            _normalize_runtime_evidence_ref(
                ref,
                base_dirs=base_dirs,
                context=resolution_context,
                operation_log_refs=(operation_log_ref,),
                product_manual_ref=product_manual_ref,
            )
            for ref in evidence_refs
        ]
        seen_ids.add(requirement_id)
        normalized_effect_results.append(
            {
                "requirement_id": requirement_id,
                "outcome": outcome,
                "summary": summary,
                "evidence_refs": normalized_refs,
            }
        )
    if set(expected_requirement_ids) != seen_ids:
        raise ValueError("AI-as-User effect_results must cover every requirement in the target evaluation unit exactly once")
    normalized_effect_results.sort(key=lambda item: expected_requirement_ids.index(str(item["requirement_id"])))
    return {
        "unit_id": unit_id,
        "operation_log_ref": operation_log_ref,
        "effect_results": normalized_effect_results,
    }


def _validate_reviewer_result(
    *,
    result: Mapping[str, Any],
    final_effect_requirements: Sequence[Mapping[str, Any]],
    result_ref: Path,
    workspace_root: Path,
    run_root: Path,
    product_manual_ref: Path | str | None = None,
    ai_user_results: Sequence[Mapping[str, Any]] = (),
    ordinary_test_results: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if str(result.get("status") or "") != "OK":
        raise ValueError("reviewer result must have status=OK")
    base_dirs = (result_ref.parent, run_root, workspace_root)
    resolution_context = _build_evidence_runtime_context(
        result_ref=result_ref,
        run_root=run_root,
        workspace_root=workspace_root,
        product_manual_ref=product_manual_ref,
        base_dirs=base_dirs,
    )
    effect_reviews = result.get("effect_reviews")
    if not isinstance(effect_reviews, list) or not effect_reviews:
        raise ValueError("reviewer result must include non-empty effect_reviews")
    expected_ids = [str(item.get("requirement_id") or "").strip() for item in final_effect_requirements]
    seen_ids: set[str] = set()
    normalized_reviews: list[dict[str, Any]] = []
    effect_required_keys = {"requirement_id", "verdict", "evidence_refs", "reproduction_steps"}
    for item in effect_reviews:
        if not isinstance(item, Mapping):
            raise ValueError("effect_reviews items must be objects")
        requirement_id = str(item.get("requirement_id") or "").strip()
        verdict = str(item.get("verdict") or "").strip().upper()
        evidence_refs = [str(ref) for ref in item.get("evidence_refs") or [] if str(ref).strip()]
        reproduction_steps = [str(step) for step in item.get("reproduction_steps") or [] if str(step).strip()]
        if requirement_id not in expected_ids:
            raise ValueError(f"reviewer reported unknown requirement_id: {requirement_id}")
        if requirement_id in seen_ids:
            raise ValueError(f"reviewer effect_reviews requirement_id must be unique: {requirement_id}")
        if verdict not in {"PASS", "FAIL", "INCONCLUSIVE", "UNCERTAIN"}:
            raise ValueError(f"reviewer effect verdict unsupported: {verdict}")
        if not evidence_refs:
            raise ValueError("reviewer effect reviews must include evidence_refs")
        if verdict in {"PASS", "FAIL"} and not reproduction_steps:
            raise ValueError("reviewer PASS/FAIL effect verdicts must include reproduction_steps")
        requirement_operation_log_refs = _collect_requirement_operation_log_refs(
            ai_user_results=ai_user_results,
            requirement_id=requirement_id,
            base_dirs=base_dirs,
            context=resolution_context,
        )
        evidence_refs = [
            _normalize_runtime_evidence_ref(
                ref,
                base_dirs=base_dirs,
                context=resolution_context,
                operation_log_refs=requirement_operation_log_refs,
                product_manual_ref=product_manual_ref,
            )
            for ref in evidence_refs
        ]
        seen_ids.add(requirement_id)
        normalized_item: dict[str, Any] = {
            "requirement_id": requirement_id,
            "verdict": verdict,
            "evidence_refs": evidence_refs,
            **({"reproduction_steps": reproduction_steps} if reproduction_steps else {}),
        }
        for key, value in item.items():
            key_str = str(key)
            if key_str in effect_required_keys:
                continue
            normalized_item[key_str] = value
        normalized_reviews.append(normalized_item)
    if seen_ids != set(expected_ids):
        raise ValueError("reviewer must return one effect review per final-effect requirement")
    normalized_reviews.sort(key=lambda item: expected_ids.index(str(item["requirement_id"])))
    normalized_result: dict[str, Any] = {"effect_reviews": normalized_reviews}
    reviewer_required_keys = {"status", "effect_reviews"}
    for key, value in result.items():
        key_str = str(key)
        if key_str in reviewer_required_keys:
            continue
        normalized_result[key_str] = value
    return normalized_result


def _run_ai_user_effect(
    *,
    request: Mapping[str, Any],
    run_root: Path,
    manual_path: Path,
    manual_text: str,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    requirement_graph: Mapping[str, Any],
    target_unit: Mapping[str, Any],
) -> dict[str, Any]:
    unit_id = str(target_unit["unit_id"])
    role_instance_id = f"ai_user__{unit_id}"
    env_role_instance_id = f"ai_user:{unit_id}"
    role_dir = _default_role_runtime_root(run_root=run_root, role_id="ai_user")
    role_artifact_root = _role_runtime_artifact_root(role_dir, role_instance_id)
    staging = _materialize_ai_user_workspace(
        request=request,
        run_root=run_root,
        unit_id=unit_id,
        manual_path=manual_path,
    )
    staged_workspace_root = Path(staging["workspace_root"])
    staged_manual_path = Path(staging["manual_path"])
    effect_scratch_dir = Path(staging["scratch_dir"])
    current_run_snapshot_root = Path(staging["current_run_snapshot_root"])
    effect_scratch_dir.mkdir(parents=True, exist_ok=True)
    prompt_text = build_ai_user_prompt(
        request=request,
        manual_text=manual_text,
        final_effect_requirements=final_effect_requirements,
        requirement_graph=requirement_graph,
        target_unit=target_unit,
        result_path=role_artifact_root / "result.json",
        response_path=role_artifact_root / "response.md",
        operation_log_path=role_artifact_root / "operation_log.md",
        effect_scratch_dir=effect_scratch_dir,
        current_run_snapshot_root=current_run_snapshot_root,
    )
    context_obj = {
        "evaluation_id": request["evaluation_id"],
        "role": "ai_user",
        "role_instance_id": role_instance_id,
        "workspace_root": str(staged_workspace_root),
        "source_workspace_root": str(request["workspace_root"]),
        "product_manual_ref": str(staged_manual_path),
        "final_effect_requirements": list(final_effect_requirements),
        "requirement_graph": dict(requirement_graph),
        "target_evaluation_unit": dict(target_unit),
        "effect_scratch_dir": str(effect_scratch_dir),
        "ai_user_dropbox_root": str(staging["dropbox_dir"]),
        "current_run_snapshot_root": str(current_run_snapshot_root),
    }
    result_raw, role_run = _invoke_role(
        role_id=role_instance_id,
        config_role_id="ai_user",
        request=request,
        run_root=run_root,
        workspace_root=staged_workspace_root,
        role_instance_id=role_instance_id,
        env_role_instance_id=env_role_instance_id,
        order_log_id=env_role_instance_id,
        role_dir=role_dir,
        prompt_text=prompt_text,
        context_obj=context_obj,
        extra_env={
            "LOOP_PRODUCT_EVAL_TARGET_EVALUATION_UNIT_ID": unit_id,
            "LOOP_PRODUCT_EVAL_EFFECT_SCRATCH_DIR": str(effect_scratch_dir),
        },
        agent_prompt_path=Path(staging["prompt_path"]),
        agent_context_path=Path(staging["context_path"]),
    )
    validated = _validate_ai_user_effect_result(
        result=result_raw,
        target_unit=target_unit,
        result_ref=Path(str(role_run["result_ref"])),
        workspace_root=staged_workspace_root,
        run_root=run_root,
        source_workspace_root=Path(str(request["workspace_root"])),
        product_manual_ref=staged_manual_path,
    )
    stage_visible_ai_user_dir = run_root / "roles" / "ai_user" / unit_id
    _copy_file_if_needed(source=Path(str(role_run["prompt_ref"])), dest=stage_visible_ai_user_dir / "prompt.md")
    _copy_file_if_needed(source=Path(str(role_run["context_ref"])), dest=stage_visible_ai_user_dir / "context.json")
    _copy_file_if_needed(source=Path(str(role_run["result_ref"])), dest=stage_visible_ai_user_dir / "result.json")
    _copy_file_if_needed(source=Path(str(role_run["response_ref"])), dest=stage_visible_ai_user_dir / "response.md")
    _copy_file_if_needed(source=Path(str(role_run["invocation_ref"])), dest=stage_visible_ai_user_dir / "invocation.json")
    _copy_file_if_needed(source=Path(str(role_run["exec_span_ref"])), dest=stage_visible_ai_user_dir / "exec_span.json")
    _copy_file_if_needed(
        source=Path(str(validated["operation_log_ref"])),
        dest=stage_visible_ai_user_dir / "operation_log.md",
    )
    exec_span = _load_json(Path(str(role_run["exec_span_ref"])))
    canonical_runtime_id = _safe_role_runtime_id(str(role_run.get("role_instance_id") or role_instance_id))
    for key in ("stdout_path", "stderr_path"):
        raw_log_path = _resolve_role_exec_log_ref(
            artifact_root=Path(str(role_run["exec_span_ref"])).parent,
            exec_span=exec_span,
            key=key,
        )
        if not raw_log_path:
            continue
        source_path = Path(raw_log_path)
        if not source_path.exists():
            continue
        suffix = "stdout.txt" if key == "stdout_path" else "stderr.txt"
        for dest in (
            stage_visible_ai_user_dir / source_path.name,
            stage_visible_ai_user_dir / f"{canonical_runtime_id}.{suffix}",
        ):
            _copy_file_if_needed(source=source_path, dest=dest)
    return {
        "unit_id": unit_id,
        "target_evaluation_unit": dict(target_unit),
        "effect_scratch_dir": str(effect_scratch_dir),
        "result_ref": role_run["result_ref"],
        "response_ref": role_run["response_ref"],
        "operation_log_ref": validated["operation_log_ref"],
        "effect_results": validated["effect_results"],
        "role_run": role_run,
    }


def _plan_ai_user_batches(requirement_graph: Mapping[str, Any]) -> list[list[dict[str, Any]]]:
    units = [dict(item) for item in requirement_graph.get("evaluation_units") or []]
    if not units:
        return []
    unit_order = [str(item["unit_id"]) for item in units]
    predecessors: dict[str, set[str]] = {unit_id: set() for unit_id in unit_order}
    for edge in requirement_graph.get("dependency_edges") or []:
        predecessors[str(edge["to_unit_id"])].add(str(edge["from_unit_id"]))
    scheduled: set[str] = set()
    batches: list[list[dict[str, Any]]] = []
    while len(scheduled) < len(unit_order):
        ready_ids = [
            unit_id
            for unit_id in unit_order
            if unit_id not in scheduled and predecessors[unit_id].issubset(scheduled)
        ]
        if not ready_ids:
            raise ValueError("AI-as-User requirement graph scheduling became cyclic after validation")
        batches.append([dict(next(item for item in units if str(item["unit_id"]) == unit_id)) for unit_id in ready_ids])
        scheduled.update(ready_ids)
    return batches


def _run_ai_user_effects(
    *,
    request: Mapping[str, Any],
    run_root: Path,
    manual_path: Path,
    manual_text: str,
    final_effect_requirements: Sequence[Mapping[str, Any]],
    requirement_graph: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    units = [dict(item) for item in requirement_graph.get("evaluation_units") or []]
    if not units:
        return [], {"mode": "parallel_evaluation_units", "planned_runs": 0, "max_parallel_runs": 0, "batches": []}
    batches = _plan_ai_user_batches(requirement_graph)
    scheduler_cfg = _normalize_ai_user_scheduler(
        request.get("ai_user_scheduler") if isinstance(request.get("ai_user_scheduler"), Mapping) else None
    )
    configured_max_parallel = int(scheduler_cfg.get("max_parallel_runs") or 0)
    max_parallel_runs = min(max(len(batch) for batch in batches), configured_max_parallel or 4)
    results_by_id: dict[str, dict[str, Any]] = {}
    errors: list[tuple[str, Exception]] = []
    for batch in batches:
        # Refresh the shared snapshot once per serial batch so later batches can
        # see prior AI-as-User results without parallel units racing on the
        # same run-root snapshot tree.
        _materialize_current_run_snapshot(
            run_root=run_root,
            snapshot_root=run_root / ".loop_evaluator_internal" / "current_run_snapshot",
        )
        batch_parallelism = min(len(batch), max_parallel_runs)
        pending_items = list(batch)
        while pending_items and not errors:
            window_items = pending_items[:batch_parallelism]
            pending_items = pending_items[batch_parallelism:]
            with ThreadPoolExecutor(max_workers=batch_parallelism) as executor:
                future_map = {
                    executor.submit(
                        _run_ai_user_effect,
                        request=request,
                        run_root=run_root,
                        manual_path=manual_path,
                        manual_text=manual_text,
                        final_effect_requirements=final_effect_requirements,
                        requirement_graph=requirement_graph,
                        target_unit=item,
                    ): str(item["unit_id"])
                    for item in window_items
                }
                for future in as_completed(future_map):
                    unit_id = future_map[future]
                    try:
                        results_by_id[unit_id] = future.result()
                    except Exception as exc:  # noqa: BLE001
                        errors.append((unit_id, exc))
        if errors:
            break
    scheduler = {
        "mode": "parallel_evaluation_units",
        "planned_runs": len(units),
        "max_parallel_runs": max_parallel_runs,
        **({"configured_max_parallel_runs": configured_max_parallel} if configured_max_parallel else {}),
        "batches": [[str(item["unit_id"]) for item in batch] for batch in batches],
    }
    ordered = [
        results_by_id[str(item["unit_id"])]
        for item in units
        if str(item["unit_id"]) in results_by_id
    ]
    if errors:
        raise AIUserBatchError(errors=errors, partial_runs=ordered, scheduler=scheduler)
    return ordered, scheduler


def _provisional_error_context(request_obj: Mapping[str, Any], *, request_path: Path | None = None) -> dict[str, Any]:
    evaluation_id = str(request_obj.get("evaluation_id") or "").strip() or "prototype_input_error"
    run_id = str(request_obj.get("run_id") or "").strip() or _utc_stamp()
    request_anchor_root = _request_anchor_root(request_path)
    output_root_raw = str(request_obj.get("output_root") or "").strip()
    output_root = (
        _resolve_path(request_anchor_root, output_root_raw)
        if output_root_raw
        else (_ROOT / ".cache" / "loop_product" / "tmp" / "loop_evaluator_prototype_errors").resolve()
    )
    workspace_root_raw = str(request_obj.get("workspace_root") or "").strip()
    workspace_root = (
        _resolve_path(request_anchor_root, workspace_root_raw)
        if workspace_root_raw
        else _ROOT.resolve()
    )
    manual_ref_raw = str(request_obj.get("product_manual_ref") or "").strip()
    manual_path = (
        _resolve_path(workspace_root, manual_ref_raw)
        if manual_ref_raw
        else (workspace_root / "__missing_product_manual_ref__").resolve()
    )
    final_effects_text_ref_raw = str(request_obj.get("final_effects_text_ref") or "").strip()
    return {
        "evaluation_id": evaluation_id,
        "run_id": run_id,
        "run_root": output_root / evaluation_id / run_id,
        "workspace_root": workspace_root,
        "product_manual_ref": manual_path,
        "final_effects_text_ref": (
            str(_resolve_path(workspace_root, final_effects_text_ref_raw))
            if final_effects_text_ref_raw
            else ""
        ),
    }


def _build_error_report(
    *,
    normalized_request: Mapping[str, Any],
    run_root: Path,
    workspace_root: Path,
    manual_path: Path,
    stage: str,
    kind: str,
    message: str,
    role_runs: Mapping[str, Any],
    notes: Sequence[str],
    evidence_refs: Sequence[str] | None = None,
    human_gate_reasons: Sequence[str] | None = None,
    checker_result_report: Mapping[str, Any] | None = None,
    final_effect_requirements: Sequence[Mapping[str, Any]] | None = None,
    requirement_graph: Mapping[str, Any] | None = None,
    ordinary_test_results: Mapping[str, Any] | None = None,
    ai_user_scheduler: Mapping[str, Any] | None = None,
    ai_user_runs: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema": "loop_product.loop_evaluator_prototype_report",
        "schema_version": "1.0.0",
        "evaluation_id": normalized_request["evaluation_id"],
        "run_id": normalized_request["run_id"],
        "run_root": str(run_root),
        "workspace_root": str(workspace_root),
        "product_manual_ref": str(manual_path),
        **(
            {"final_effects_text_ref": str(normalized_request["final_effects_text_ref"])}
            if str(normalized_request.get("final_effects_text_ref") or "").strip()
            else {}
        ),
        "status": "ERROR",
        "role_runs": dict(role_runs),
        "notes": [str(item) for item in notes],
        "error": {
            "stage": stage,
            "kind": kind,
            "message": message,
            **(
                {"evidence_refs": [str(ref) for ref in evidence_refs if str(ref).strip()]}
                if evidence_refs
                else {}
            ),
            **(
                {"human_gate_reasons": [str(item) for item in human_gate_reasons if str(item).strip()]}
                if human_gate_reasons
                else {}
            ),
        },
    }
    if checker_result_report is not None:
        report["checker_result"] = dict(checker_result_report)
    if final_effect_requirements is not None:
        report["final_effect_requirements"] = list(final_effect_requirements)
    if requirement_graph is not None:
        report["requirement_graph"] = dict(requirement_graph)
    if ordinary_test_results is not None:
        report["ordinary_test_results"] = dict(ordinary_test_results)
    if ai_user_scheduler is not None:
        report["ai_user_scheduler"] = dict(ai_user_scheduler)
    if ai_user_runs is not None:
        report["ai_user_runs"] = list(ai_user_runs)
    _validate_with_schema(obj=report, schema_path=_REPORT_SCHEMA_PATH, label="prototype error report")
    _write_json(run_root / "EvaluationReport.json", report)
    return report


def _build_supervisor_report(
    *,
    normalized_request: Mapping[str, Any],
    run_root: Path,
    request_ref: Path,
    status: str,
    message: str,
    child_stdout_ref: Path,
    child_stderr_ref: Path,
    progress_log_ref: Path,
    poll_interval_s: float,
    no_progress_timeout_s: float,
    child_exit_code: int | None,
    terminal_evaluation_report_ref: Path | None = None,
    terminal_evaluation_status: str | None = None,
    notes: Sequence[str] | None = None,
) -> dict[str, Any]:
    report: dict[str, Any] = {
        "schema": "loop_product.loop_evaluator_prototype_supervisor_report",
        "schema_version": "1.0.0",
        "evaluation_id": str(normalized_request["evaluation_id"]),
        "run_id": str(normalized_request["run_id"]),
        "run_root": str(run_root),
        "request_ref": str(request_ref),
        "status": status,
        "message": message,
        "poll_interval_s": float(poll_interval_s),
        "no_progress_timeout_s": float(no_progress_timeout_s),
        "child_stdout_ref": str(child_stdout_ref),
        "child_stderr_ref": str(child_stderr_ref),
        "progress_log_ref": str(progress_log_ref),
        "notes": [str(item) for item in (notes or []) if str(item).strip()],
    }
    if child_exit_code is not None:
        report["child_exit_code"] = int(child_exit_code)
    if terminal_evaluation_report_ref is not None:
        report["terminal_evaluation_report_ref"] = str(terminal_evaluation_report_ref)
    if terminal_evaluation_status is not None:
        report["terminal_evaluation_status"] = str(terminal_evaluation_status)
    _validate_with_schema(
        obj=report,
        schema_path=_SUPERVISOR_SCHEMA_PATH,
        label="prototype supervisor report",
    )
    _write_json(run_root / "SupervisorReport.json", report)
    return report


def run_evaluator_prototype(*, request: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    request_path = Path(request).resolve() if isinstance(request, (str, Path)) else None
    request_obj = _load_json(Path(request)) if isinstance(request, (str, Path)) else dict(request)
    provisional = _provisional_error_context(request_obj, request_path=request_path)
    run_root = Path(str(provisional["run_root"]))
    run_root.mkdir(parents=True, exist_ok=True)
    _write_json(run_root / "input_snapshot.json", request_obj)

    normalized_request: dict[str, Any] | None = None
    workspace_root: Path | None = None
    manual_path: Path | None = None
    manual_text = ""

    stage = "preflight"
    role_runs: dict[str, Any] = {}
    checker_result_report: dict[str, Any] | None = None
    final_effect_requirements: list[dict[str, Any]] | None = None
    requirement_graph: dict[str, Any] | None = None
    ordinary_test_results: dict[str, Any] | None = None
    ai_user_scheduler: dict[str, Any] | None = None
    ai_user_runs: list[dict[str, Any]] | None = None
    runtime_assertions: list[dict[str, Any]] | None = None
    packaged_self_eval_guard_previous: object | str | None = _PACKAGED_SELF_EVAL_GUARD_UNCHANGED
    try:
        try:
            normalized_request = _normalize_request(request_obj, request_path=request_path)
            packaged_self_eval_guard_previous = _activate_packaged_self_eval_guard(request=normalized_request)
            workspace_root = Path(str(normalized_request["workspace_root"]))
            output_root = Path(str(normalized_request["output_root"]))
            manual_path, manual_text = _read_text_ref(
                workspace_root=workspace_root,
                text_ref=str(normalized_request["product_manual_ref"]),
                label="product_manual_ref",
            )
            run_root = output_root / str(normalized_request["evaluation_id"]) / str(normalized_request["run_id"])
            run_root.mkdir(parents=True, exist_ok=True)
            preserved_paths: list[Path] = []
            if request_path is not None and _is_within(request_path, run_root):
                preserved_paths.append(request_path)
                for relpath in (
                    Path("SupervisorProgress.log"),
                    Path("supervisor_child.stdout.txt"),
                    Path("supervisor_child.stderr.txt"),
                ):
                    candidate = run_root / relpath
                    if candidate.exists():
                        preserved_paths.append(candidate)
            _clear_run_root_artifacts(run_root=run_root, preserve_paths=preserved_paths)
            _write_json(run_root / "input_snapshot.json", normalized_request)

            if str(normalized_request.get("final_effects_text_ref") or "").strip():
                stage = "checker"
                goals_path, goals_text = _read_text_ref(
                    workspace_root=workspace_root,
                    text_ref=str(normalized_request["final_effects_text_ref"]),
                    label="final_effects_text_ref",
                )
                checker_role_dir = _default_role_runtime_root(run_root=run_root, role_id="checker")
                checker_artifact_root = _role_runtime_artifact_root(checker_role_dir, "checker")
                checker_workspace = _materialize_codex_role_workspace(
                    request=normalized_request,
                    run_root=run_root,
                    role_id="checker",
                    copy_source_workspace=False,
                )
                checker_result_raw, checker_run = _invoke_role(
                    role_id="checker",
                    request=normalized_request,
                    run_root=run_root,
                    workspace_root=checker_workspace["workspace_root"],
                    role_dir=checker_role_dir,
                    prompt_text=build_checker_prompt(
                        request=normalized_request,
                        final_effects_text=goals_text,
                        result_path=checker_artifact_root / "result.json",
                        response_path=checker_artifact_root / "response.md",
                    ),
                    context_obj={
                        "evaluation_id": normalized_request["evaluation_id"],
                        "role": "checker",
                        "workspace_root": str(Path(checker_workspace["workspace_root"]).resolve()),
                        "final_effects_text_ref": str(
                            _materialize_staged_role_context_ref(
                                staged_workspace_root=Path(checker_workspace["workspace_root"]),
                                source_workspace_root=workspace_root,
                                source_path=goals_path,
                                label="final_effects_text",
                            )
                        ),
                    },
                )
                role_runs["checker"] = checker_run
                checker_validated = _validate_checker_result(checker_result_raw)
                _materialize_validated_checker_artifacts(
                    role_dir=run_root / "roles" / "checker",
                    validated_checker_result=checker_validated,
                )
                final_effect_requirements = list(checker_validated["normalized_requirements"])
                requirement_graph = dict(checker_validated["requirement_graph"])
                checker_result_report = {
                    "result_ref": checker_run["result_ref"],
                    "response_ref": checker_run["response_ref"],
                    "normalized_requirements": final_effect_requirements,
                    "requirement_graph": requirement_graph,
                    "repair_actions": list(checker_validated["repair_actions"]),
                    "notes": list(checker_validated["notes"]),
                }
            else:
                final_effect_requirements = _validate_final_effect_requirements(
                    normalized_request.get("final_effect_requirements") or [],
                    label="prototype input final_effect_requirements",
                )
                requirement_graph = _build_singleton_requirement_graph(final_effect_requirements)

            stage = "test_designer"
            test_ai_role_dir = _default_role_runtime_root(run_root=run_root, role_id="test_ai")
            test_ai_artifact_root = _role_runtime_artifact_root(test_ai_role_dir, "test_ai")
            test_designer_workspace = _materialize_codex_role_workspace(
                request=normalized_request,
                run_root=run_root,
                role_id="test_ai",
                copy_source_workspace=True,
            )
            designer_result, designer_run = _invoke_role(
                role_id="test_designer",
                request=normalized_request,
                run_root=run_root,
                workspace_root=test_designer_workspace["workspace_root"],
                role_instance_id="test_ai",
                order_log_id="test_designer",
                role_dir=test_ai_role_dir,
                prompt_text=build_test_designer_prompt(
                    request=normalized_request,
                    manual_text=manual_text,
                    final_effect_requirements=final_effect_requirements,
                    result_path=test_ai_artifact_root / "result.json",
                    response_path=test_ai_artifact_root / "response.md",
                    ordinary_test_artifact_root=Path(test_designer_workspace["ordinary_test_artifact_root"]),
                ),
                context_obj={
                    "evaluation_id": normalized_request["evaluation_id"],
                    "role": "test_designer",
                    "workspace_root": str(Path(test_designer_workspace["workspace_root"]).resolve()),
                    "artifact_workspace_root": str(run_root),
                    "ordinary_test_artifact_root": str(
                        Path(test_designer_workspace["ordinary_test_artifact_root"]).resolve()
                    ),
                    "product_manual_ref": str(
                        _materialize_staged_role_context_ref(
                            staged_workspace_root=Path(test_designer_workspace["workspace_root"]),
                            source_workspace_root=workspace_root,
                            source_path=manual_path,
                            label="product_manual",
                        )
                    ),
                    "final_effect_requirements": final_effect_requirements,
                },
            )
            role_runs["test_designer"] = designer_run
            stage_visible_designer_dir = run_root / "roles" / "test_designer"
            stage_visible_designer_result_ref = stage_visible_designer_dir / "result.json"
            stage_visible_designer_response_ref = stage_visible_designer_dir / "response.md"
            _copy_file_if_needed(source=Path(str(designer_run["result_ref"])), dest=stage_visible_designer_result_ref)
            _copy_file_if_needed(source=Path(str(designer_run["response_ref"])), dest=stage_visible_designer_response_ref)
            designer_result = _remap_staged_ordinary_test_refs(
                result=designer_result,
                staged_artifact_root=Path(test_designer_workspace["ordinary_test_artifact_root"]),
                canonical_artifact_root=run_root / "ordinary_tests",
            )
            ordinary_test_results = _validate_test_designer_result(
                designer_result,
                request=normalized_request,
                workspace_root=workspace_root,
                run_root=run_root,
                result_ref=stage_visible_designer_result_ref,
            )

            stage = "ordinary_tests"
            _materialize_current_run_snapshot(
                run_root=run_root,
                snapshot_root=run_root / ".loop_evaluator_internal" / "current_run_snapshot",
            )

            stage = "ai_user"
            ai_user_runs, ai_user_scheduler = _run_ai_user_effects(
                request=normalized_request,
                run_root=run_root,
                manual_path=manual_path,
                manual_text=manual_text,
                final_effect_requirements=final_effect_requirements,
                requirement_graph=requirement_graph,
            )

            stage = "reviewer"
            reviewer_role_dir = _default_role_runtime_root(run_root=run_root, role_id="reviewer")
            reviewer_artifact_root = _role_runtime_artifact_root(reviewer_role_dir, "reviewer")
            reviewer_workspace = _materialize_codex_role_workspace(
                request=normalized_request,
                run_root=run_root,
                role_id="reviewer",
                copy_source_workspace=False,
            )
            reviewer_result_raw, reviewer_run = _invoke_role(
                role_id="reviewer",
                request=normalized_request,
                run_root=run_root,
                workspace_root=Path(reviewer_workspace["workspace_root"]),
                role_dir=reviewer_role_dir,
                prompt_text=build_reviewer_prompt(
                    request=normalized_request,
                    manual_text=manual_text,
                    final_effect_requirements=final_effect_requirements,
                    requirement_graph=requirement_graph,
                    ordinary_test_results=ordinary_test_results,
                    ai_user_results=ai_user_runs,
                    result_path=reviewer_artifact_root / "result.json",
                    response_path=reviewer_artifact_root / "response.md",
                    run_root=run_root,
                    workspace_root=workspace_root,
                ),
                context_obj={
                    "evaluation_id": normalized_request["evaluation_id"],
                    "role": "reviewer",
                    "workspace_root": str(Path(reviewer_workspace["workspace_root"]).resolve()),
                    "artifact_workspace_root": str(run_root),
                    "product_manual_ref": str(
                        _materialize_staged_role_context_ref(
                            staged_workspace_root=Path(reviewer_workspace["workspace_root"]),
                            source_workspace_root=workspace_root,
                            source_path=manual_path,
                            label="product_manual",
                        )
                    ),
                    "final_effect_requirements": final_effect_requirements,
                    "requirement_graph": requirement_graph,
                    "ordinary_test_results": ordinary_test_results,
                    "ai_user_results": ai_user_runs,
                },
            )
            role_runs["reviewer"] = reviewer_run
            reviewer_result = _validate_reviewer_result(
                result=reviewer_result_raw,
                final_effect_requirements=final_effect_requirements,
                result_ref=Path(str(reviewer_run["result_ref"])),
                workspace_root=workspace_root,
                run_root=run_root,
                product_manual_ref=manual_path,
                ai_user_results=ai_user_runs,
                ordinary_test_results=ordinary_test_results,
            )
        except Exception as exc:  # noqa: BLE001
            error_evidence_refs: list[str] = []
            error_kind = "RUNTIME_FAILURE"
            human_gate_reasons: list[str] = []
            if stage == "checker" and "checker" in role_runs:
                error_evidence_refs.extend(
                    [
                        str(role_runs["checker"]["result_ref"]),
                        str(role_runs["checker"]["response_ref"]),
                    ]
                )
            if stage == "reviewer" and "reviewer" in role_runs:
                error_evidence_refs.extend(
                    [
                        str(role_runs["reviewer"]["result_ref"]),
                        str(role_runs["reviewer"]["response_ref"]),
                    ]
                )
            if isinstance(exc, AIUserBatchError):
                ai_user_runs = list(exc.partial_runs)
                ai_user_scheduler = dict(exc.scheduler)
                if isinstance(exc.first_exception, ValueError):
                    error_kind = "CONTRACT_VALIDATION_FAILURE"
            elif isinstance(exc, CheckerHumanGateError):
                error_kind = "HUMAN_GATE"
                human_gate_reasons = list(exc.human_gate_reasons)
            elif isinstance(exc, ValueError):
                error_kind = "CONTRACT_VALIDATION_FAILURE"
            return _build_error_report(
                normalized_request=normalized_request
                or {
                    "evaluation_id": str(provisional["evaluation_id"]),
                    "run_id": str(provisional["run_id"]),
                    **(
                        {"final_effects_text_ref": str(provisional["final_effects_text_ref"])}
                        if str(provisional["final_effects_text_ref"])
                        else {}
                    ),
                },
                run_root=run_root,
                workspace_root=workspace_root or Path(str(provisional["workspace_root"])),
                manual_path=manual_path or Path(str(provisional["product_manual_ref"])),
                stage=stage,
                kind=error_kind,
                message=str(exc),
                role_runs=role_runs,
                notes=[str(exc)],
                evidence_refs=error_evidence_refs,
                human_gate_reasons=human_gate_reasons,
                checker_result_report=checker_result_report,
                final_effect_requirements=final_effect_requirements,
                requirement_graph=requirement_graph,
                ordinary_test_results=ordinary_test_results,
                ai_user_scheduler=ai_user_scheduler,
                ai_user_runs=ai_user_runs,
            )
    finally:
        _restore_packaged_self_eval_guard(packaged_self_eval_guard_previous)

    runtime_assertions = _evaluate_runtime_assertions(normalized_request.get("runtime_assertions") or [])
    runtime_status = _derive_runtime_status(
        final_effect_requirements=final_effect_requirements,
        effect_reviews=reviewer_result["effect_reviews"],
    )
    runtime_status = _fold_runtime_assertions_into_status(
        base_status=runtime_status,
        runtime_assertions=runtime_assertions,
    )
    report_notes: list[str] = []
    failed_runtime_assertions = [
        str(item.get("assertion_id") or "").strip()
        for item in runtime_assertions
        if bool(item.get("blocking", True)) and str(item.get("status") or "").strip().upper() != "PASS"
    ]
    if failed_runtime_assertions:
        report_notes.append(
            "blocking runtime assertions failed: " + ", ".join(sorted(failed_runtime_assertions))
        )
    report = {
        "schema": "loop_product.loop_evaluator_prototype_report",
        "schema_version": "1.0.0",
        "evaluation_id": normalized_request["evaluation_id"],
        "run_id": normalized_request["run_id"],
        "run_root": str(run_root),
        "workspace_root": str(workspace_root),
        "product_manual_ref": str(manual_path),
        **(
            {"final_effects_text_ref": str(normalized_request["final_effects_text_ref"])}
            if str(normalized_request.get("final_effects_text_ref") or "").strip()
            else {}
        ),
        "status": runtime_status,
        "final_effect_requirements": final_effect_requirements,
        "requirement_graph": requirement_graph,
        "role_runs": role_runs,
        **({"checker_result": checker_result_report} if checker_result_report is not None else {}),
        "ordinary_test_results": ordinary_test_results,
        "ai_user_scheduler": ai_user_scheduler,
        "ai_user_runs": ai_user_runs,
        "reviewer_result": {
            "result_ref": role_runs["reviewer"]["result_ref"],
            "response_ref": role_runs["reviewer"]["response_ref"],
            **reviewer_result,
        },
        **({"runtime_assertions": runtime_assertions} if runtime_assertions else {}),
        "notes": report_notes,
    }
    _validate_with_schema(obj=report, schema_path=_REPORT_SCHEMA_PATH, label="prototype report")
    _write_json(run_root / "EvaluationReport.json", report)
    return report


def run_evaluator_prototype_supervised(
    *,
    request: Mapping[str, Any] | str | Path,
    poll_interval_s: float = 0.5,
    no_progress_timeout_s: float = 7200.0,
    _child_argv_override: Sequence[str] | None = None,
) -> dict[str, Any]:
    if poll_interval_s <= 0:
        raise ValueError("poll_interval_s must be > 0")
    if no_progress_timeout_s <= 0:
        raise ValueError("no_progress_timeout_s must be > 0")
    request_obj = _load_json(Path(request)) if isinstance(request, (str, Path)) else dict(request)
    normalized_request = _normalize_request(request_obj)
    run_root = (
        Path(str(normalized_request["output_root"]))
        / str(normalized_request["evaluation_id"])
        / str(normalized_request["run_id"])
    )
    run_root.mkdir(parents=True, exist_ok=True)
    _clear_run_root_artifacts(run_root=run_root)

    request_ref = run_root / "SupervisorRequest.json"
    progress_log_ref = run_root / "SupervisorProgress.log"
    terminal_report_ref = run_root / "EvaluationReport.json"
    supervisor_report_ref = run_root / "SupervisorReport.json"

    _write_json(request_ref, normalized_request)
    _write_text(progress_log_ref, "")
    child_argv = (
        [str(item) for item in _child_argv_override]
        if _child_argv_override is not None
        else [
            sys.executable,
            "-m",
            "loop_product.evaluator.prototype",
            "--input",
            str(request_ref),
            "--mode",
            "raw",
        ]
    )
    notes: list[str] = []
    _append_supervisor_progress(progress_log_ref, f"launching child evaluator: {shlex.join(child_argv)}")
    start_mtime_ns = time.time_ns()
    child_pythonpath = str(_STANDALONE_PRODUCT_REPO_ROOT)
    existing_pythonpath = str(os.environ.get("PYTHONPATH") or "").strip()
    if existing_pythonpath:
        child_pythonpath = f"{child_pythonpath}{os.pathsep}{existing_pythonpath}"
    handle = start_cmd(
        cmd=child_argv,
        cwd=_STANDALONE_PRODUCT_REPO_ROOT,
        log_dir=run_root,
        label="supervisor_child",
        env={"PYTHONPATH": child_pythonpath},
    )
    child_stdout_ref = handle.stdout_path
    child_stderr_ref = handle.stderr_path
    last_snapshot = _supervisor_progress_snapshot(
        run_root=run_root,
        min_mtime_ns=start_mtime_ns,
        ignored_paths=[progress_log_ref, supervisor_report_ref],
    )
    last_progress_monotonic = time.monotonic()
    last_validation_error = ""

    def _read_terminal_report() -> tuple[dict[str, Any], str] | None:
        nonlocal last_validation_error
        if not terminal_report_ref.exists() or terminal_report_ref.stat().st_mtime_ns < start_mtime_ns:
            return None
        try:
            terminal_report = _load_json(terminal_report_ref)
            _validate_with_schema(
                obj=terminal_report,
                schema_path=_REPORT_SCHEMA_PATH,
                label="supervised terminal evaluator report",
            )
            terminal_status = str(terminal_report.get("status") or "").strip()
            if terminal_status in _TERMINAL_EVALUATION_STATUSES:
                return terminal_report, terminal_status
        except Exception as exc:  # noqa: BLE001
            last_validation_error = str(exc)
        return None

    while True:
        terminal_report_info = _read_terminal_report()
        if terminal_report_info is not None:
            _terminal_report, terminal_status = terminal_report_info
            _append_supervisor_progress(
                progress_log_ref,
                f"observed terminal evaluator report with status={terminal_status}",
            )
            if handle.proc.poll() is None:
                deadline = time.monotonic() + 1.0
                while time.monotonic() < deadline and handle.proc.poll() is None:
                    time.sleep(0.05)
                if handle.proc.poll() is None:
                    notes.append("child remained alive after terminal report; terminated residual process tree")
                    _append_supervisor_progress(
                        progress_log_ref,
                        "child remained alive after terminal report; terminating residual process tree",
                    )
                    terminate_running_cmd(handle)
            cmd_result = finish_running_cmd(handle)
            child_exit_code = int(cmd_result.span.get("exit_code", -1))
            return _build_supervisor_report(
                normalized_request=normalized_request,
                run_root=run_root,
                request_ref=request_ref,
                status="TERMINAL",
                message="supervisor observed a terminal evaluator report and waited until the run reached terminal state",
                child_stdout_ref=child_stdout_ref,
                child_stderr_ref=child_stderr_ref,
                progress_log_ref=progress_log_ref,
                poll_interval_s=poll_interval_s,
                no_progress_timeout_s=no_progress_timeout_s,
                child_exit_code=child_exit_code,
                terminal_evaluation_report_ref=terminal_report_ref,
                terminal_evaluation_status=terminal_status,
                notes=notes,
            )
        current_snapshot = _supervisor_progress_snapshot(
            run_root=run_root,
            min_mtime_ns=start_mtime_ns,
            ignored_paths=[progress_log_ref, supervisor_report_ref],
        )
        now = time.monotonic()
        if current_snapshot != last_snapshot:
            last_snapshot = current_snapshot
            last_progress_monotonic = now
            latest_mtime_ns, total_size, file_count = current_snapshot
            _append_supervisor_progress(
                progress_log_ref,
                f"observed child progress: file_count={file_count} total_size={total_size} latest_mtime_ns={latest_mtime_ns}",
            )
        child_exit_code = handle.proc.poll()
        if child_exit_code is not None:
            terminal_report_info = _read_terminal_report()
            if terminal_report_info is not None:
                _terminal_report, terminal_status = terminal_report_info
                _append_supervisor_progress(
                    progress_log_ref,
                    f"observed terminal evaluator report with status={terminal_status} after child exit",
                )
                cmd_result = finish_running_cmd(handle)
                return _build_supervisor_report(
                    normalized_request=normalized_request,
                    run_root=run_root,
                    request_ref=request_ref,
                    status="TERMINAL",
                    message=(
                        "supervisor observed a terminal evaluator report after the child exited "
                        "and accepted the run as terminal"
                    ),
                    child_stdout_ref=child_stdout_ref,
                    child_stderr_ref=child_stderr_ref,
                    progress_log_ref=progress_log_ref,
                    poll_interval_s=poll_interval_s,
                    no_progress_timeout_s=no_progress_timeout_s,
                    child_exit_code=int(cmd_result.span.get("exit_code", -1)),
                    terminal_evaluation_report_ref=terminal_report_ref,
                    terminal_evaluation_status=terminal_status,
                    notes=notes,
                )
            message = "child exited before a terminal evaluator report was observed"
            if last_validation_error:
                message = f"{message}; last report validation error: {last_validation_error}"
            _append_supervisor_progress(
                progress_log_ref,
                f"child exited without terminal evaluator report: exit_code={child_exit_code}",
            )
            cmd_result = finish_running_cmd(handle)
            return _build_supervisor_report(
                normalized_request=normalized_request,
                run_root=run_root,
                request_ref=request_ref,
                status="BUG",
                message=message,
                child_stdout_ref=child_stdout_ref,
                child_stderr_ref=child_stderr_ref,
                progress_log_ref=progress_log_ref,
                poll_interval_s=poll_interval_s,
                no_progress_timeout_s=no_progress_timeout_s,
                child_exit_code=int(cmd_result.span.get("exit_code", -1)),
                notes=notes,
            )
        if now - last_progress_monotonic > no_progress_timeout_s:
            _append_supervisor_progress(
                progress_log_ref,
                f"no progress observed for {no_progress_timeout_s:.2f}s; terminating child process tree",
            )
            terminate_running_cmd(handle)
            cmd_result = finish_running_cmd(handle, timed_out=True, timeout_kind="semantic")
            return _build_supervisor_report(
                normalized_request=normalized_request,
                run_root=run_root,
                request_ref=request_ref,
                status="STUCK",
                message=(
                    "child evaluator made no progress within the configured timeout; "
                    "the supervisor terminated the run as stuck"
                ),
                child_stdout_ref=child_stdout_ref,
                child_stderr_ref=child_stderr_ref,
                progress_log_ref=progress_log_ref,
                poll_interval_s=poll_interval_s,
                no_progress_timeout_s=no_progress_timeout_s,
                child_exit_code=int(cmd_result.span.get("exit_code", -1)),
                notes=notes,
            )
        time.sleep(poll_interval_s)


def build_endpoint_clarification_part1_prototype_input(
    *,
    workspace_root: str | Path,
    output_root: str | Path,
    product_manual_ref: str | Path,
    final_effects_text_ref: str | Path | None = None,
) -> dict[str, Any]:
    request: dict[str, Any] = {
        "schema": "loop_product.loop_evaluator_prototype_input",
        "schema_version": "1.0.0",
        "evaluation_id": "endpoint_clarification_part1",
        "workspace_root": str(Path(workspace_root)),
        "output_root": str(Path(output_root)),
        "product_manual_ref": str(Path(product_manual_ref)),
        "role_requirements": {
            "test_designer": "Generate only documented-surface ordinary tests for the endpoint-clarification behavior.",
            "ai_user": "Act like a real user trying to clarify a fuzzy endpoint while preserving a faithful visible log. When validating the Codex-host integration effect, behave like a conversation-facing host that proactively routes fuzzy turns through the host adapter entrypoint instead of asking the user to invoke explicit start/continue mode commands.",
            "reviewer": "Only affirm endpoint-clarification success or failure when the recorded interaction is reproducible from visible evidence.",
        },
        "agent_execution": {
            "default": {
                "agent_provider": "codex_cli",
                "sandbox_mode": "danger-full-access",
                "reasoning_effort": "high",
            },
            "reviewer": {
                "agent_provider": "codex_cli",
                "sandbox_mode": "danger-full-access",
                "reasoning_effort": "high",
            },
        },
    }
    if final_effects_text_ref is None:
        request["final_effect_requirements"] = [
            {
                "requirement_id": "stable_enter_or_bypass_decision",
                "description": "The system must stably decide whether to enter vision compiler or bypass it.",
                "blocking": True,
            },
            {
                "requirement_id": "ordinary_conversation_triggerable",
                "description": "Codex-host integration effect: the front-half must be triggerable from an ordinary conversation-style host turn without making the caller choose start versus continue explicitly.",
                "blocking": True,
            },
            {
                "requirement_id": "repo_independent_front_half_product",
                "description": "Final architecture effect: the front-half must be independent of the current LeanAtlas repo and treated as part of the same independent repo/product as the back-half evaluator, with LeanAtlas reduced to a host adapter or integration layer.",
                "blocking": True,
            },
            {
                "requirement_id": "vision_mode_stability_after_entry",
                "description": "After entry, the interaction must remain in endpoint-clarification mode rather than silently drifting back to ordinary execution.",
                "blocking": True,
            },
            {
                "requirement_id": "result_oriented_future_effect_focus",
                "description": "The dialogue must focus on the future effect the user wants rather than on implementation-path choices.",
                "blocking": True,
            },
            {
                "requirement_id": "natural_language_response_freedom",
                "description": "The user must be able to respond freely in natural language rather than being collapsed into a menu.",
                "blocking": True,
            },
            {
                "requirement_id": "single_high_information_question",
                "description": "Each round must ask only one high-information clarification question.",
                "blocking": True,
            },
            {
                "requirement_id": "task_scoped_endpoint_artifact_persisted",
                "description": "Confirmed and denied endpoint information must be preserved in a task-scoped artifact.",
                "blocking": True,
            },
            {
                "requirement_id": "artifact_location_disclosed",
                "description": "The system must tell the user where the endpoint artifact is stored.",
                "blocking": True,
            },
            {
                "requirement_id": "clarity_stop_boundary",
                "description": "Once endpoint clarity is sufficient, Part 1 must stop instead of drifting into downstream-layer questions.",
                "blocking": True,
            },
            {
                "requirement_id": "non_menu_path_questioning_blocked",
                "description": "Implementation-path interrogation must not replace endpoint clarification.",
                "blocking": True,
            },
            {
                "requirement_id": "iterative_node_not_single_turn",
                "description": "Vision compiler Part 1 must behave like an iterative node rather than a one-shot helper.",
                "blocking": True,
            },
        ]
    else:
        request["final_effects_text_ref"] = str(Path(final_effects_text_ref))
        request["role_requirements"]["checker"] = (
            "Repair endpoint-clarification final-effects text into one effect per item when the repair is obvious, and stop only at a true human gate."
        )
    return request


def build_endpoint_clarification_part1_input(
    *,
    workspace_root: str | Path,
    output_root: str | Path,
    product_manual_ref: str | Path,
    final_effects_text_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Formal packaged endpoint-clarification evaluator request alias."""

    return build_endpoint_clarification_part1_prototype_input(
        workspace_root=workspace_root,
        output_root=output_root,
        product_manual_ref=product_manual_ref,
        final_effects_text_ref=final_effects_text_ref,
    )


def build_evaluator_prototype_self_eval_input(
    *,
    workspace_root: str | Path,
    output_root: str | Path,
    fixture_request_ref: str | Path,
    final_effects_text_ref: str | Path | None = None,
    source_product_manual_ref: str | Path | None = None,
    host_adapter_root: str | Path | None = None,
) -> dict[str, Any]:
    workspace_root_path = Path(workspace_root)
    output_root_path = Path(output_root)
    fixture_request_path = Path(fixture_request_ref)
    self_eval_run_id = _fresh_session_run_id()
    requested_final_effects_path = (
        Path(final_effects_text_ref) if final_effects_text_ref is not None else _FINAL_EFFECTS_DOC_PATH
    ).resolve()
    source_manual_path = (
        Path(source_product_manual_ref) if source_product_manual_ref is not None else _PRODUCT_MANUAL_DOC_PATH
    ).resolve()
    generated_manual_path = (
        workspace_root_path
        / ".cache"
        / "loop_evaluator_self_eval"
        / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.generated.md"
    )
    generated_fixture_request_path = (
        workspace_root_path / ".cache" / "loop_evaluator_self_eval" / "fixture_request.generated.json"
    )
    generated_negative_fixture_request_path = (
        workspace_root_path
        / ".cache"
        / "loop_evaluator_self_eval"
        / "fixture_request.reviewer_missing_repro.generated.json"
    )
    generated_missing_evidence_fixture_request_path = (
        workspace_root_path
        / ".cache"
        / "loop_evaluator_self_eval"
        / "fixture_request.reviewer_missing_evidence.generated.json"
    )
    generated_bug_fixture_request_path = (
        workspace_root_path
        / ".cache"
        / "loop_evaluator_self_eval"
        / "fixture_request.supervisor_bug.generated.json"
    )
    generated_stuck_fixture_request_path = (
        workspace_root_path
        / ".cache"
        / "loop_evaluator_self_eval"
        / "fixture_request.supervisor_stuck.generated.json"
    )
    generated_fixture_support_dir = generated_fixture_request_path.parent / "inputs"
    generated_outer_final_effects_path = (
        generated_fixture_support_dir / "LOOP_EVALUATOR_PROTOTYPE_SELF_EVAL_OUTER_FINAL_EFFECTS.md"
    )
    generated_positive_fixture_final_effects_path = (
        generated_fixture_support_dir / "LOOP_EVALUATOR_PROTOTYPE_SELF_EVAL_POSITIVE_FIXTURE_FINAL_EFFECTS.md"
    )
    support_agent_path = generated_fixture_request_path.parent / "reviewer_missing_repro_agent.py"
    missing_evidence_agent_path = generated_fixture_request_path.parent / "reviewer_missing_evidence_agent.py"
    supervisor_bug_agent_path = generated_fixture_request_path.parent / "supervisor_bug_agent.py"
    supervisor_stuck_agent_path = generated_fixture_request_path.parent / "supervisor_stuck_agent.py"
    adapter_snapshot_dir = generated_fixture_support_dir / "leanatlas_adapter_snapshot"
    raw_fixture_request = _load_json(fixture_request_path)
    original_fixture_workspace_root = _resolve_path(
        _ROOT,
        str(raw_fixture_request.get("workspace_root") or workspace_root_path),
    )
    outer_final_effects_path = requested_final_effects_path
    if final_effects_text_ref is None and _is_canonical_evaluator_final_effects_path(requested_final_effects_path):
        outer_final_effects_path = _write_session_local_self_eval_text(
            generated_outer_final_effects_path,
            _DEFAULT_SELF_EVAL_OUTER_FINAL_EFFECTS_TEXT,
        )
    generated_fixture_request = json.loads(json.dumps(raw_fixture_request))
    generated_fixture_request["workspace_root"] = "."
    generated_fixture_request["output_root"] = str(
        Path(".cache") / "loop_evaluator_self_eval" / "fixture_output"
    )
    generated_fixture_request["run_id"] = self_eval_run_id
    generated_fixture_request["product_manual_ref"] = _materialize_self_eval_request_ref(
        raw_ref=str(raw_fixture_request.get("product_manual_ref") or ""),
        original_workspace_root=original_fixture_workspace_root,
        target_workspace_root=workspace_root_path,
        support_dir=generated_fixture_support_dir,
    )
    generated_fixture_request["ai_user_scheduler"] = {"max_parallel_runs": 1}
    final_effects_text_ref_raw = str(raw_fixture_request.get("final_effects_text_ref") or "").strip()
    fixture_final_effects_source = _resolve_path(
        original_fixture_workspace_root,
        final_effects_text_ref_raw or str(requested_final_effects_path),
    )
    if final_effects_text_ref is None and _is_canonical_evaluator_final_effects_path(fixture_final_effects_source):
        _write_session_local_self_eval_text(
            generated_positive_fixture_final_effects_path,
            _DEFAULT_SELF_EVAL_POSITIVE_FIXTURE_FINAL_EFFECTS_TEXT,
        )
        generated_fixture_request["final_effects_text_ref"] = str(
            generated_positive_fixture_final_effects_path.relative_to(workspace_root_path)
        )
        generated_fixture_role_requirements = dict(generated_fixture_request.get("role_requirements") or {})
        existing_ai_user_role_requirement = str(generated_fixture_role_requirements.get("ai_user") or "").strip()
        existing_test_designer_role_requirement = str(
            generated_fixture_role_requirements.get("test_designer") or ""
        ).strip()
        ai_user_guidance_suffix = (
            " Do not relaunch the positive-path packaged self-eval fixture through the evaluator entrypoint. "
            "Validate the positive-path acceptance surface from the current run's visible artifacts and documented non-recursive probes. "
            "Use the staged control files for the active AI-as-User unit as launch evidence for that unit when needed. "
            "Do not inspect source files when the current run artifacts already expose the acceptance surface."
        )
        generated_fixture_role_requirements["ai_user"] = (
            f"{existing_ai_user_role_requirement}{ai_user_guidance_suffix}".strip()
            if existing_ai_user_role_requirement
            else ai_user_guidance_suffix.strip()
        )
        test_designer_guidance_suffix = (
            " For this positive-path packaged self-eval fixture, do not require a terminal EvaluationReport.json from the current in-flight run. "
            "Audit only the stage-visible evidence that should exist before reviewer and terminal-report emission, and leave terminal PASS validation to later review. "
            "Active AI-as-User launch evidence comes from the staged control surface. "
            "Ordinary tests must not require run_root .loop/ai_user artifacts for the active unit during ordinary-test execution. "
            "Do not inspect source files when the current run artifacts already expose the acceptance surface."
        )
        generated_fixture_role_requirements["test_designer"] = (
            f"{existing_test_designer_role_requirement}{test_designer_guidance_suffix}".strip()
            if existing_test_designer_role_requirement
            else test_designer_guidance_suffix.strip()
        )
        generated_fixture_request["role_requirements"] = generated_fixture_role_requirements
    else:
        generated_fixture_request["final_effects_text_ref"] = _materialize_self_eval_request_ref(
            raw_ref=final_effects_text_ref_raw or str(requested_final_effects_path),
            original_workspace_root=original_fixture_workspace_root,
            target_workspace_root=workspace_root_path,
            support_dir=generated_fixture_support_dir,
        )
    generated_agent_execution = dict(generated_fixture_request.get("agent_execution") or {})
    for role_key, role_cfg_raw in list(generated_agent_execution.items()):
        role_cfg = dict(role_cfg_raw or {})
        agent_cmd = str(role_cfg.get("agent_cmd") or "").strip()
        if agent_cmd:
            _materialize_self_eval_agent_cmd_support_files(
                raw_cmd=agent_cmd,
                original_workspace_root=original_fixture_workspace_root,
                target_workspace_root=workspace_root_path,
                fixture_request_path=fixture_request_path,
            )
            role_cfg["agent_cmd"] = _rewrite_self_eval_agent_cmd(raw_cmd=agent_cmd)
        generated_agent_execution[role_key] = role_cfg
    if generated_agent_execution:
        generated_fixture_request["agent_execution"] = generated_agent_execution
    _write_json(generated_fixture_request_path, generated_fixture_request)
    _materialize_self_eval_support_agent(support_agent_path)
    _materialize_self_eval_missing_evidence_agent(missing_evidence_agent_path)
    _materialize_self_eval_supervisor_bug_agent(supervisor_bug_agent_path)
    _materialize_self_eval_supervisor_stuck_agent(supervisor_stuck_agent_path)
    if host_adapter_root is not None:
        _materialize_self_eval_adapter_snapshot(
            source_root=Path(host_adapter_root).resolve(),
            target_dir=adapter_snapshot_dir,
        )
    generated_negative_fixture_request = json.loads(json.dumps(generated_fixture_request))
    generated_negative_fixture_request["evaluation_id"] = (
        f"{str(generated_fixture_request.get('evaluation_id') or 'fixture_product')}_reviewer_missing_repro"
    )
    negative_fixture_run_id = f"{self_eval_run_id}__reviewer_missing_repro"
    generated_negative_fixture_request["run_id"] = negative_fixture_run_id
    negative_agent_execution = dict(generated_negative_fixture_request.get("agent_execution") or {})
    negative_reviewer_cfg = dict(negative_agent_execution.get("reviewer") or {})
    negative_reviewer_cfg["agent_cmd"] = _rewrite_self_eval_agent_cmd(
        raw_cmd=(
            f"{shlex.quote(sys.executable)} "
            ".cache/loop_evaluator_self_eval/reviewer_missing_repro_agent.py"
        )
    )
    negative_reviewer_cfg.pop("agent_provider", None)
    negative_reviewer_cfg.pop("agent_profile", None)
    negative_reviewer_cfg.setdefault("sandbox_mode", "danger-full-access")
    negative_agent_execution["reviewer"] = negative_reviewer_cfg
    generated_negative_fixture_request["agent_execution"] = negative_agent_execution
    _write_json(generated_negative_fixture_request_path, generated_negative_fixture_request)
    generated_missing_evidence_fixture_request = json.loads(json.dumps(generated_fixture_request))
    generated_missing_evidence_fixture_request["evaluation_id"] = (
        f"{str(generated_fixture_request.get('evaluation_id') or 'fixture_product')}_reviewer_missing_evidence"
    )
    missing_evidence_fixture_run_id = f"{self_eval_run_id}__reviewer_missing_evidence"
    generated_missing_evidence_fixture_request["run_id"] = missing_evidence_fixture_run_id
    missing_evidence_agent_execution = dict(generated_missing_evidence_fixture_request.get("agent_execution") or {})
    missing_evidence_reviewer_cfg = dict(missing_evidence_agent_execution.get("reviewer") or {})
    missing_evidence_reviewer_cfg["agent_cmd"] = _rewrite_self_eval_agent_cmd(
        raw_cmd=(
            f"{shlex.quote(sys.executable)} "
            ".cache/loop_evaluator_self_eval/reviewer_missing_evidence_agent.py"
        )
    )
    missing_evidence_reviewer_cfg.pop("agent_provider", None)
    missing_evidence_reviewer_cfg.pop("agent_profile", None)
    missing_evidence_reviewer_cfg.setdefault("sandbox_mode", "danger-full-access")
    missing_evidence_agent_execution["reviewer"] = missing_evidence_reviewer_cfg
    generated_missing_evidence_fixture_request["agent_execution"] = missing_evidence_agent_execution
    _write_json(generated_missing_evidence_fixture_request_path, generated_missing_evidence_fixture_request)
    generated_bug_fixture_request = json.loads(json.dumps(generated_fixture_request))
    generated_bug_fixture_request["evaluation_id"] = (
        f"{str(generated_fixture_request.get('evaluation_id') or 'fixture_product')}_supervisor_bug"
    )
    bug_fixture_run_id = f"{self_eval_run_id}__supervisor_bug"
    generated_bug_fixture_request["run_id"] = bug_fixture_run_id
    bug_agent_execution = dict(generated_bug_fixture_request.get("agent_execution") or {})
    bug_checker_cfg = dict(bug_agent_execution.get("checker") or bug_agent_execution.get("default") or {})
    bug_checker_cfg["agent_cmd"] = _rewrite_self_eval_agent_cmd(
        raw_cmd=(
            f"{shlex.quote(sys.executable)} "
            ".cache/loop_evaluator_self_eval/supervisor_bug_agent.py"
        )
    )
    bug_checker_cfg.pop("agent_provider", None)
    bug_checker_cfg.pop("agent_profile", None)
    bug_checker_cfg.setdefault("sandbox_mode", "danger-full-access")
    bug_agent_execution["checker"] = bug_checker_cfg
    generated_bug_fixture_request["agent_execution"] = bug_agent_execution
    _write_json(generated_bug_fixture_request_path, generated_bug_fixture_request)
    generated_stuck_fixture_request = json.loads(json.dumps(generated_fixture_request))
    generated_stuck_fixture_request["evaluation_id"] = (
        f"{str(generated_fixture_request.get('evaluation_id') or 'fixture_product')}_supervisor_stuck"
    )
    stuck_fixture_run_id = f"{self_eval_run_id}__supervisor_stuck"
    generated_stuck_fixture_request["run_id"] = stuck_fixture_run_id
    stuck_agent_execution = dict(generated_stuck_fixture_request.get("agent_execution") or {})
    stuck_checker_cfg = dict(stuck_agent_execution.get("checker") or stuck_agent_execution.get("default") or {})
    stuck_checker_cfg["agent_cmd"] = _rewrite_self_eval_agent_cmd(
        raw_cmd=(
            f"{shlex.quote(sys.executable)} "
            ".cache/loop_evaluator_self_eval/supervisor_stuck_agent.py"
        )
    )
    stuck_checker_cfg.pop("agent_provider", None)
    stuck_checker_cfg.pop("agent_profile", None)
    stuck_checker_cfg.setdefault("sandbox_mode", "danger-full-access")
    stuck_agent_execution["checker"] = stuck_checker_cfg
    generated_stuck_fixture_request["agent_execution"] = stuck_agent_execution
    _write_json(generated_stuck_fixture_request_path, generated_stuck_fixture_request)
    negative_fixture_report_rel = (
        Path(".cache")
        / "loop_evaluator_self_eval"
        / "fixture_output"
        / str(generated_negative_fixture_request["evaluation_id"])
        / negative_fixture_run_id
        / "EvaluationReport.json"
    )
    missing_evidence_fixture_report_rel = (
        Path(".cache")
        / "loop_evaluator_self_eval"
        / "fixture_output"
        / str(generated_missing_evidence_fixture_request["evaluation_id"])
        / missing_evidence_fixture_run_id
        / "EvaluationReport.json"
    )
    bug_fixture_supervisor_report_rel = (
        Path(".cache")
        / "loop_evaluator_self_eval"
        / "fixture_output"
        / str(generated_bug_fixture_request["evaluation_id"])
        / bug_fixture_run_id
        / "SupervisorReport.json"
    )
    stuck_fixture_supervisor_report_rel = (
        Path(".cache")
        / "loop_evaluator_self_eval"
        / "fixture_output"
        / str(generated_stuck_fixture_request["evaluation_id"])
        / stuck_fixture_run_id
        / "SupervisorReport.json"
    )
    positive_fixture_report_rel = (
        Path(".cache")
        / "loop_evaluator_self_eval"
        / "fixture_output"
        / str(generated_fixture_request["evaluation_id"])
        / str(generated_fixture_request["run_id"])
        / "EvaluationReport.json"
    )
    generated_fixture_request_rel = generated_fixture_request_path.relative_to(workspace_root_path)
    generated_negative_fixture_request_rel = generated_negative_fixture_request_path.relative_to(
        workspace_root_path
    )
    generated_missing_evidence_fixture_request_rel = generated_missing_evidence_fixture_request_path.relative_to(
        workspace_root_path
    )
    generated_bug_fixture_request_rel = generated_bug_fixture_request_path.relative_to(workspace_root_path)
    generated_stuck_fixture_request_rel = generated_stuck_fixture_request_path.relative_to(workspace_root_path)
    base_manual_text = source_manual_path.read_text(encoding="utf-8").rstrip()
    session_python_cmd = _preferred_workspace_python_command(workspace_root_path)
    session_direct_cmd = (
        f"{session_python_cmd} -m loop_product.evaluator.prototype --input {generated_fixture_request_rel} --mode supervised"
    )
    session_negative_direct_cmd = (
        f"{session_python_cmd} -m loop_product.evaluator.prototype --input "
        f"{generated_negative_fixture_request_rel} --mode supervised"
    )
    session_missing_evidence_direct_cmd = (
        f"{session_python_cmd} -m loop_product.evaluator.prototype --input "
        f"{generated_missing_evidence_fixture_request_rel} --mode supervised"
    )
    session_bug_direct_cmd = (
        f"{session_python_cmd} -m loop_product.evaluator.prototype --input "
        f"{generated_bug_fixture_request_rel} --mode supervised"
    )
    session_stuck_direct_cmd = (
        f"{session_python_cmd} -m loop_product.evaluator.prototype --input "
        f"{generated_stuck_fixture_request_rel} --mode supervised --no-progress-timeout-s 1.0"
    )
    session_manual = (
        f"{base_manual_text}\n\n"
        "## Session-Local Fixtures\n\n"
        "Positive-path prepared fixture request JSON for this self-eval run:\n\n"
        f"`{generated_fixture_request_rel}`\n\n"
        "Machine-checkable nested report path for the positive-path fixture in this session:\n\n"
        f"`{positive_fixture_report_rel}`\n\n"
        "Before you inspect the positive-path report path above, first execute the positive-path fixture command for this session.\n\n"
        "If that report is absent before the command has been executed, treat it as pre-execution state rather than product failure.\n\n"
        "Preferred caller-facing command for the positive-path self-eval session:\n\n"
        "```bash\n"
        f"{session_direct_cmd}\n"
        "```\n\n"
        "These session-local direct-interpreter commands stay runnable from staged workspaces: "
        "they use the current workspace's provisioned interpreter when `.venv` exists locally, "
        "and otherwise fall back to `${LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT}` for the same "
        "session's provisioned interpreter.\n\n"
        "Equivalent `uv` command for the same positive-path session:\n\n"
        "```bash\n"
        f"uv run python -m loop_product.evaluator.prototype --input {generated_fixture_request_rel} --mode supervised\n"
        "```\n\n"
        "When validating the packaged positive-path fixture from inside its own AI-as-User lane, do not relaunch that same packaged fixture through the evaluator entrypoint.\n\n"
        "Instead, validate the current run's visible artifacts plus only the repo-local probes that are actually needed for the requirement you are covering. "
        "This session manual intentionally does not prescribe a mandatory helper script for every lane.\n\n"
        "Supporting negative-path fixture request JSON for reviewer fail-closed verification:\n\n"
        f"`{generated_negative_fixture_request_rel}`\n\n"
        "Machine-checkable nested report path for the negative-path fixture in this session:\n\n"
        f"`{negative_fixture_report_rel}`\n\n"
        "Use this command when you need to verify that malformed reviewer claims are rejected fail-closed:\n\n"
        "```bash\n"
        f"{session_negative_direct_cmd}\n"
        "```\n\n"
        "Equivalent `uv` command for the negative-path fixture:\n\n"
        "```bash\n"
        f"uv run python -m loop_product.evaluator.prototype --input {generated_negative_fixture_request_rel} --mode supervised\n"
        "```\n\n"
        "Supporting negative-path fixture request JSON for reviewer missing-evidence fail-closed verification:\n\n"
        f"`{generated_missing_evidence_fixture_request_rel}`\n\n"
        "Machine-checkable nested report path for the missing-evidence fixture in this session:\n\n"
        f"`{missing_evidence_fixture_report_rel}`\n\n"
        "Use this command when you need to verify that missing reviewer evidence refs are rejected fail-closed:\n\n"
        "```bash\n"
        f"{session_missing_evidence_direct_cmd}\n"
        "```\n\n"
        "Equivalent `uv` command for the missing-evidence fixture:\n\n"
        "```bash\n"
        f"uv run python -m loop_product.evaluator.prototype --input {generated_missing_evidence_fixture_request_rel} --mode supervised\n"
        "```\n\n"
        "Supporting bug-path fixture request JSON for confirmed supervisor `BUG` verification:\n\n"
        f"`{generated_bug_fixture_request_rel}`\n\n"
        "Expected supervisor report path for the bug-path fixture in this session:\n\n"
        f"`{bug_fixture_supervisor_report_rel}`\n\n"
        "Use this command when you need to verify that the supervised entrypoint reports a confirmed bug instead of pretending completion:\n\n"
        "```bash\n"
        f"{session_bug_direct_cmd}\n"
        "```\n\n"
        "Equivalent `uv` command for the bug-path fixture:\n\n"
        "```bash\n"
        f"uv run python -m loop_product.evaluator.prototype --input {generated_bug_fixture_request_rel} --mode supervised\n"
        "```\n\n"
        "Supporting stuck-path fixture request JSON for confirmed supervisor `STUCK` verification:\n\n"
        f"`{generated_stuck_fixture_request_rel}`\n\n"
        "Expected supervisor report path for the stuck-path fixture in this session:\n\n"
        f"`{stuck_fixture_supervisor_report_rel}`\n\n"
        "Use this command when you need to verify that the supervised entrypoint reports a confirmed stuck condition instead of pretending completion:\n\n"
        "```bash\n"
        f"{session_stuck_direct_cmd}\n"
        "```\n\n"
        "Equivalent `uv` command for the stuck-path fixture:\n\n"
        "```bash\n"
        f"uv run python -m loop_product.evaluator.prototype --input {generated_stuck_fixture_request_rel} --mode supervised --no-progress-timeout-s 1.0\n"
        "```\n\n"
        "When validating shared-repo architecture or adapter-boundary claims, choose only the repo-local evidence or probes that are actually needed for that claim and keep the resulting evidence reviewer-visible. "
        "The session manual intentionally does not require a single helper script for those claims.\n\n"
        "For this self-eval run, do not report completion before a terminal evaluator result exists.\n"
        "Early return is acceptable only after a confirmed bug or stuck condition.\n"
    )
    _write_text(
        generated_manual_path,
        _sanitize_self_eval_manual_text(text=session_manual, workspace_root=workspace_root_path),
    )
    return {
        "schema": "loop_product.loop_evaluator_prototype_input",
        "schema_version": "1.0.0",
        "evaluation_id": "loop_evaluator_prototype_self_eval",
        "run_id": self_eval_run_id,
        "workspace_root": str(workspace_root_path),
        "output_root": str(output_root_path),
        "product_manual_ref": str(generated_manual_path),
        "final_effects_text_ref": str(outer_final_effects_path),
        "ai_user_scheduler": {"max_parallel_runs": 1},
        "runtime_assertions": [
            {
                "assertion_id": "positive_terminal_pass_fixture",
                "kind": "evaluation_report",
                "blocking": True,
                "report_ref": str(positive_fixture_report_rel),
                "expected_status": "PASS",
                "expected_evaluation_id": str(generated_fixture_request["evaluation_id"]),
                "expected_run_id": str(generated_fixture_request["run_id"]),
            },
            {
                "assertion_id": "reviewer_fail_closed_negative_fixture",
                "kind": "evaluation_report",
                "blocking": True,
                "report_ref": str(negative_fixture_report_rel),
                "expected_status": "ERROR",
                "expected_evaluation_id": str(generated_negative_fixture_request["evaluation_id"]),
                "expected_run_id": negative_fixture_run_id,
                "expected_error_stage": "reviewer",
                "expected_error_message_substring": "reproduction_steps",
            },
            {
                "assertion_id": "reviewer_fail_closed_missing_evidence_fixture",
                "kind": "evaluation_report",
                "blocking": True,
                "report_ref": str(missing_evidence_fixture_report_rel),
                "expected_status": "ERROR",
                "expected_evaluation_id": str(generated_missing_evidence_fixture_request["evaluation_id"]),
                "expected_run_id": missing_evidence_fixture_run_id,
                "expected_error_stage": "reviewer",
                "expected_error_message_substring": "evidence_refs",
            },
            {
                "assertion_id": "supervisor_bug_fixture",
                "kind": "supervisor_report",
                "blocking": True,
                "report_ref": str(bug_fixture_supervisor_report_rel),
                "expected_status": "BUG",
                "expected_evaluation_id": str(generated_bug_fixture_request["evaluation_id"]),
                "expected_run_id": bug_fixture_run_id,
                "expected_error_message_substring": "before a terminal evaluator report",
            },
            {
                "assertion_id": "supervisor_stuck_fixture",
                "kind": "supervisor_report",
                "blocking": True,
                "report_ref": str(stuck_fixture_supervisor_report_rel),
                "expected_status": "STUCK",
                "expected_evaluation_id": str(generated_stuck_fixture_request["evaluation_id"]),
                "expected_run_id": stuck_fixture_run_id,
                "expected_error_message_substring": "no progress",
            },
        ],
        "role_requirements": {
            "checker": (
                "Repair evaluator final-effects text into one effect per item when the repair is obvious, and stop only at a true human gate."
            ),
            "test_designer": (
                "Generate only documented-surface ordinary tests for the evaluator prototype. "
                "Use prepared fixture requests from the product manual only when they are relevant to the claim under test. "
                "If you rely on a dedicated fixture report, first execute the corresponding documented fixture command. "
                "Only inspect its machine-checkable report path after that command returns; the absence of that report before the command has run is not itself a product failure. "
                "Treat product-manual example commands as optional aids rather than a mandatory checklist, and do not run unrelated helper scripts or fixture examples just because they appear in the manual. "
                "Do not treat mid-run progress as completion; require a terminal evaluator result or a confirmed bug or stuck condition."
            ),
            "ai_user": (
                "Act like a real caller using the evaluator prototype via the documented CLI. "
                "Run one evaluation unit at a time during this live self-eval so nested Codex usage stays within documented capacity. "
                "Use only the documented commands, fixture requests, and visible evidence that are relevant to the current evaluation unit. "
                "Treat product-manual example commands as optional aids rather than a mandatory checklist, and ignore unrelated helper scripts or fixture examples. "
                "Do not treat progress as completion unless a confirmed bug or stuck condition is established."
            ),
            "reviewer": (
                "Only affirm evaluator success when the visible evidence shows the caller stayed attached until a terminal evaluator result, "
                "or correctly stopped because a confirmed bug or stuck condition existed."
            ),
        },
        "agent_execution": {
            "default": {
                "agent_provider": "codex_cli",
                "sandbox_mode": "danger-full-access",
                "reasoning_effort": "high",
            },
            "reviewer": {
                "agent_provider": "codex_cli",
                "sandbox_mode": "danger-full-access",
                "reasoning_effort": "high",
            },
        },
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to a prototype request JSON file.")
    parser.add_argument(
        "--mode",
        choices=("supervised", "raw"),
        default="supervised",
        help="Caller-facing supervised wait-until-terminal mode, or raw in-process evaluator mode for child dispatch.",
    )
    parser.add_argument(
        "--poll-interval-s",
        type=float,
        default=0.5,
        help="Supervisor polling interval in seconds when --mode=supervised.",
    )
    parser.add_argument(
        "--no-progress-timeout-s",
        type=float,
        default=7200.0,
        help="Supervisor no-progress timeout in seconds when --mode=supervised.",
    )
    args = parser.parse_args(argv)
    if args.mode == "raw":
        report = run_evaluator_prototype(request=Path(args.input))
        print(Path(str(report["run_root"])) / "EvaluationReport.json")
        return 0
    supervisor_report = run_evaluator_prototype_supervised(
        request=Path(args.input),
        poll_interval_s=args.poll_interval_s,
        no_progress_timeout_s=args.no_progress_timeout_s,
    )
    if supervisor_report["status"] == "TERMINAL":
        print(str(supervisor_report["terminal_evaluation_report_ref"]))
        return 0
    print(Path(str(supervisor_report["run_root"])) / "SupervisorReport.json")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
