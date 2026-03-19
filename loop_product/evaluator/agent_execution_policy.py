"""Fail-closed evaluator agent-execution policy."""

from __future__ import annotations

import shlex
from collections.abc import Mapping
from typing import Any

_ALLOWED_REPO_SHIPPED_ROLE_AGENT_MODULES = frozenset(
    {
        "loop_product.loop.fixture_role_agent",
        "loop_product.loop.smoke_role_agent",
    }
)


def allowed_repo_shipped_role_agent_modules() -> tuple[str, ...]:
    return tuple(sorted(_ALLOWED_REPO_SHIPPED_ROLE_AGENT_MODULES))


def _extract_python_module(agent_cmd: str) -> str | None:
    raw = str(agent_cmd or "").strip()
    if not raw:
        return None
    try:
        argv = shlex.split(raw)
    except ValueError:
        return None
    for index, token in enumerate(argv[:-1]):
        if token == "-m":
            module = str(argv[index + 1] or "").strip()
            if module:
                return module
    return None


def validate_repo_shipped_role_agent_cmd(agent_cmd: str, *, context: str) -> None:
    raw = str(agent_cmd or "").strip()
    if not raw:
        return
    module = _extract_python_module(raw)
    if module in _ALLOWED_REPO_SHIPPED_ROLE_AGENT_MODULES:
        return
    allowed = ", ".join(allowed_repo_shipped_role_agent_modules())
    raise ValueError(
        f"{context} must use provider/profile execution or a repo-shipped evaluator role agent only; "
        f"task-authored agent_cmd values are forbidden. Allowed repo-shipped modules: {allowed}. "
        f"Got: {raw!r}"
    )


def validate_evaluator_agent_execution(agent_execution: Mapping[str, Any], *, context: str) -> None:
    for role_id, role_cfg_raw in dict(agent_execution or {}).items():
        role_cfg = dict(role_cfg_raw or {})
        agent_cmd = str(role_cfg.get("agent_cmd") or "").strip()
        if agent_cmd:
            validate_repo_shipped_role_agent_cmd(agent_cmd, context=f"{context} role `{role_id}`")
