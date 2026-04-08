"""Router-local child launch policy builder."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from loop.events import ActorKind


@dataclass(frozen=True)
class RouterActorLaunchPlan:
    """Small immutable launch plan for one router-managed actor attempt."""

    argv: list[str]
    env: dict[str, str]
    cwd: Path
    stdin_path: Path | None
    start_new_session: bool
    bridge_mode_override: str | None


def build_router_actor_launch_plan(
    *,
    actor_kind: ActorKind,
    workspace_root: Path,
    prompt_path: Path,
    actor_env: dict[str, str],
    codex_home: Path,
) -> RouterActorLaunchPlan:
    """Return the committed Codex CLI launch shape for router-managed actors."""

    resolved_workspace = Path(workspace_root).expanduser().resolve()
    resolved_prompt = Path(prompt_path).expanduser().resolve()
    resolved_codex_home = Path(codex_home).expanduser().resolve()
    if actor_kind is ActorKind.EVALUATOR_TESTER:
        argv = [
            "codex",
            "exec",
            "-C",
            str(resolved_workspace),
            "review",
            "--dangerously-bypass-approvals-and-sandbox",
            "--skip-git-repo-check",
            "-c",
            'model_reasoning_effort="xhigh"',
            "-",
        ]
    else:
        argv = [
            "codex",
            "exec",
            "-C",
            str(resolved_workspace),
            "--skip-git-repo-check",
            "-s",
            "danger-full-access",
            "-c",
            'model_reasoning_effort="xhigh"',
            "-",
        ]
    return RouterActorLaunchPlan(
        argv=argv,
        env={
            **dict(actor_env),
            "CODEX_HOME": str(resolved_codex_home),
            "OTEL_SDK_DISABLED": "true",
            "OTEL_TRACES_EXPORTER": "none",
            "OTEL_METRICS_EXPORTER": "none",
            "OTEL_LOGS_EXPORTER": "none",
        },
        cwd=resolved_workspace,
        stdin_path=resolved_prompt,
        start_new_session=False,
        bridge_mode_override="direct",
    )
