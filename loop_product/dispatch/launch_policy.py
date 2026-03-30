"""Launch-policy helpers for child nodes."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loop_product.codex_home import default_codex_model

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

_TRANSPORT_ENV_KEY_PAIRS = (
    ("HTTP_PROXY", "http_proxy"),
    ("HTTPS_PROXY", "https_proxy"),
    ("ALL_PROXY", "all_proxy"),
    ("NO_PROXY", "no_proxy"),
)


def codex_reasoning_effort_for_thinking_budget(thinking_budget: str) -> str:
    normalized = str(thinking_budget or "").strip().lower()
    if not normalized:
        normalized = "medium"
    if normalized not in _CODEX_REASONING_BY_BUDGET:
        raise ValueError(f"unsupported thinking_budget for codex child launch: {thinking_budget!r}")
    return _CODEX_REASONING_BY_BUDGET[normalized]


def merge_safe_parent_transport_env(env: dict[str, Any] | None = None) -> dict[str, str]:
    merged = {str(key): str(value) for key, value in dict(env or {}).items() if str(value or "").strip()}
    for upper, lower in _TRANSPORT_ENV_KEY_PAIRS:
        explicit_upper = str(merged.get(upper) or "").strip()
        explicit_lower = str(merged.get(lower) or "").strip()
        parent_upper = str(os.environ.get(upper) or "").strip()
        parent_lower = str(os.environ.get(lower) or "").strip()
        chosen = explicit_upper or explicit_lower or parent_upper or parent_lower
        if not chosen:
            merged.pop(upper, None)
            merged.pop(lower, None)
            continue
        merged[upper] = chosen
        merged[lower] = chosen
    return merged


def _discover_runtime_transport_env(state_root: str | Path | None) -> dict[str, str]:
    if state_root is None:
        return {}
    runtime_root = Path(state_root).expanduser().resolve()
    launch_root = runtime_root / "artifacts" / "launches"
    if not launch_root.exists():
        return {}
    request_refs = sorted(
        launch_root.glob("*/attempt_*/ChildLaunchRequest.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    for request_ref in request_refs:
        try:
            payload = json.loads(request_ref.read_text(encoding="utf-8"))
        except Exception:
            continue
        launch_spec = dict(payload.get("launch_spec") or {})
        discovered = merge_safe_parent_transport_env(dict(launch_spec.get("env") or {}))
        if any(discovered.get(upper) for upper, _ in _TRANSPORT_ENV_KEY_PAIRS):
            return discovered
    return {}


def merge_runtime_transport_env(
    *,
    state_root: str | Path | None,
    env: dict[str, Any] | None = None,
) -> dict[str, str]:
    merged = {str(key): str(value) for key, value in dict(env or {}).items() if str(value or "").strip()}
    runtime_transport_env = _discover_runtime_transport_env(state_root)
    for upper, lower in _TRANSPORT_ENV_KEY_PAIRS:
        explicit_upper = str(merged.get(upper) or "").strip()
        explicit_lower = str(merged.get(lower) or "").strip()
        sticky_upper = str(runtime_transport_env.get(upper) or "").strip()
        sticky_lower = str(runtime_transport_env.get(lower) or "").strip()
        parent_upper = str(os.environ.get(upper) or "").strip()
        parent_lower = str(os.environ.get(lower) or "").strip()
        chosen = explicit_upper or explicit_lower or sticky_upper or sticky_lower or parent_upper or parent_lower
        if not chosen:
            merged.pop(upper, None)
            merged.pop(lower, None)
            continue
        merged[upper] = chosen
        merged[lower] = chosen
    return merged


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
            "-m",
            default_codex_model(),
            "-c",
            f'model_reasoning_effort="{reasoning_effort}"',
            "-",
        ],
        "env": merge_safe_parent_transport_env(),
        "cwd": str(resolved_workspace),
        "stdin_path": str(resolved_prompt),
    }
