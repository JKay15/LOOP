"""Launch-policy helpers for child nodes."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(slots=True)
class LaunchPolicy:
    """Minimal launch policy for a child lane."""

    agent_provider: str
    thinking_budget: str
    sandbox_mode: str
    retry_policy: str


_CODEX_REASONING_BY_BUDGET = {
    "low": "low",
    "medium": "medium",
    "high": "high",
    "xhigh": "xhigh",
}


def codex_reasoning_effort_for_thinking_budget(thinking_budget: str) -> str:
    normalized = str(thinking_budget or "").strip().lower()
    if not normalized:
        normalized = "medium"
    if normalized not in _CODEX_REASONING_BY_BUDGET:
        raise ValueError(f"unsupported thinking_budget for codex child launch: {thinking_budget!r}")
    return _CODEX_REASONING_BY_BUDGET[normalized]


def build_codex_cli_child_launch(
    *,
    workspace_root: str | Path,
    sandbox_mode: str,
    thinking_budget: str,
    prompt_path: str | Path,
) -> dict[str, Any]:
    resolved_workspace = Path(workspace_root).resolve()
    resolved_prompt = Path(prompt_path).resolve()
    resolved_codex_home = (Path.home() / ".codex").resolve()
    normalized_sandbox = str(sandbox_mode or "").strip() or "danger-full-access"
    if normalized_sandbox not in {"read-only", "workspace-write", "danger-full-access"}:
        raise ValueError(f"unsupported sandbox_mode for child launch: {sandbox_mode!r}")
    reasoning_effort = codex_reasoning_effort_for_thinking_budget(thinking_budget)
    return {
        "argv": [
            "codex",
            "exec",
            "-C",
            str(resolved_workspace),
            "--skip-git-repo-check",
            "-s",
            normalized_sandbox,
            "--add-dir",
            str(resolved_codex_home),
            "-c",
            f'model_reasoning_effort="{reasoning_effort}"',
            "-",
        ],
        "env": {},
        "cwd": str(resolved_workspace),
        "stdin_path": str(resolved_prompt),
    }
