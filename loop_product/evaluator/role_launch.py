"""Committed evaluator role launch helpers."""

from __future__ import annotations

import os
import re
import shlex
from pathlib import Path
from typing import Any, Mapping

from ..run_cmd import run_cmd

_ENV_ASSIGNMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=.*$")
_ENV_REF_RE = re.compile(r"\$(?:\{(?P<braced>[A-Za-z_][A-Za-z0-9_]*)\}|(?P<plain>[A-Za-z_][A-Za-z0-9_]*))")
_UNSUPPORTED_SHELL_TOKENS = {"|", "||", "&&", ";", "<", ">", ">>", "2>", "2>>", "<<", "<<<"}
_SUPPORTED_PROVIDER_IDS = {"codex_cli", "claude_code"}
_SUPPORTED_REASONING_EFFORTS = {"minimal", "low", "medium", "high", "xhigh"}


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


def _parse_explicit_agent_cmd(agent_cmd: str) -> tuple[dict[str, str], list[str]]:
    raw = str(agent_cmd or "").strip()
    if not raw:
        raise ValueError("explicit evaluator role agent_cmd cannot be empty")
    try:
        tokens = shlex.split(raw)
    except ValueError as exc:
        raise ValueError(f"explicit evaluator role agent_cmd is not shell-parseable: {raw!r}") from exc
    env_updates: dict[str, str] = {}
    while tokens and _ENV_ASSIGNMENT_RE.fullmatch(tokens[0] or ""):
        key, value = str(tokens.pop(0)).split("=", 1)
        env_updates[key] = os.path.expandvars(value)
    if not tokens:
        raise ValueError(f"explicit evaluator role agent_cmd must include an executable argv: {raw!r}")
    bad_tokens = [token for token in tokens if token in _UNSUPPORTED_SHELL_TOKENS]
    if bad_tokens:
        raise ValueError(
            "explicit evaluator role agent_cmd must be a direct argv command without shell operators; "
            f"got unsupported tokens: {', '.join(sorted(set(bad_tokens)))}"
        )
    return env_updates, [str(token) for token in tokens]


def _expand_env_refs(raw: str, env: Mapping[str, str]) -> str:
    merged = {str(k): str(v) for k, v in os.environ.items()}
    merged.update({str(k): str(v) for k, v in dict(env or {}).items()})

    def _replace(match: re.Match[str]) -> str:
        key = match.group("braced") or match.group("plain") or ""
        return merged.get(key, match.group(0))

    return _ENV_REF_RE.sub(_replace, str(raw))


def _build_provider_argv(
    *,
    provider_id: str,
    prompt_transport: str,
    prompt_arg: str,
    sandbox_mode: str,
    reasoning_effort: str,
    prompt_path: Path,
    workspace_root: Path,
    response_path: Path,
    output_schema_path: Path | None,
    raw_response_path: Path | None,
) -> tuple[list[str], str | None]:
    if provider_id not in _SUPPORTED_PROVIDER_IDS:
        raise ValueError(f"unsupported committed evaluator role provider: {provider_id}")
    if provider_id == "codex_cli":
        argv = [
            "codex",
            "exec",
            "-C",
            str(workspace_root.resolve()),
            "--skip-git-repo-check",
            "-s",
            str(sandbox_mode or "danger-full-access"),
        ]
        if output_schema_path is not None:
            argv.extend(["--output-schema", str(output_schema_path.resolve())])
        output_target = raw_response_path if raw_response_path is not None else response_path
        argv.extend(["-o", str(output_target.resolve())])
        if reasoning_effort in _SUPPORTED_REASONING_EFFORTS:
            argv.extend(["-c", f'model_reasoning_effort="{reasoning_effort}"'])
        if prompt_transport == "stdin":
            argv.append("-")
            return argv, str(prompt_path.resolve())
        if prompt_transport == "env_path":
            argv.append(str(prompt_path.resolve()))
            return argv, None
        if prompt_transport == "arg":
            argv.extend([prompt_arg or "--prompt-file", str(prompt_path.resolve())])
            return argv, None
        raise ValueError(f"unsupported prompt transport for committed evaluator role launch: {prompt_transport}")
    # Best-effort argv path for claude_code; currently unused in product tests.
    argv = ["claude", "exec"]
    if prompt_transport == "stdin":
        argv.append("-")
        return argv, str(prompt_path.resolve())
    if prompt_transport == "env_path":
        argv.append(str(prompt_path.resolve()))
        return argv, None
    if prompt_transport == "arg":
        argv.extend([prompt_arg or "--prompt-file", str(prompt_path.resolve())])
        return argv, None
    raise ValueError(f"unsupported prompt transport for committed evaluator role launch: {prompt_transport}")


def build_committed_role_launch_spec(
    *,
    resolved: Any,
    config: Mapping[str, Any],
    prompt_path: Path,
    workspace_root: Path,
    response_path: Path,
    base_env: Mapping[str, str],
    output_schema_path: Path | None = None,
    raw_response_path: Path | None = None,
) -> dict[str, Any]:
    prompt_path = Path(prompt_path).expanduser().resolve()
    workspace_root = Path(workspace_root).expanduser().resolve()
    response_path = Path(response_path).expanduser().resolve()
    merged_env = {str(k): str(v) for k, v in dict(base_env or {}).items()}
    source = str(getattr(resolved, "source", "") or "")
    provider_id = str(getattr(resolved, "provider_id", "") or "")
    if source == "cli.agent_cmd":
        parsed_env, argv = _parse_explicit_agent_cmd(str(getattr(resolved, "agent_cmd", "") or ""))
        expanded_parsed_env = {
            str(key): _expand_env_refs(str(value), {**merged_env, **parsed_env})
            for key, value in parsed_env.items()
        }
        merged_env.update(expanded_parsed_env)
        expanded_argv = [_expand_env_refs(token, merged_env) for token in argv]
        return {
            "argv": expanded_argv,
            "env": merged_env,
            "cwd": str(workspace_root),
            "stdin_path": "",
            "launch_mode": "parsed_agent_cmd",
            "provider_id": "",
        }

    prompt_transport = str(getattr(resolved, "prompt_transport", "") or "stdin").strip() or "stdin"
    prompt_arg = str(getattr(resolved, "prompt_arg", "") or "").strip()
    sandbox_mode = str(config.get("sandbox_mode") or "danger-full-access").strip() or "danger-full-access"
    reasoning_effort = str(config.get("reasoning_effort") or "").strip().lower()
    argv, stdin_path = _build_provider_argv(
        provider_id=provider_id,
        prompt_transport=prompt_transport,
        prompt_arg=prompt_arg,
        sandbox_mode=sandbox_mode,
        reasoning_effort=reasoning_effort,
        prompt_path=prompt_path,
        workspace_root=workspace_root,
        response_path=response_path,
        output_schema_path=output_schema_path,
        raw_response_path=raw_response_path,
    )
    return {
        "argv": argv,
        "env": merged_env,
        "cwd": str(workspace_root),
        "stdin_path": str(prompt_path) if stdin_path is not None else "",
        "launch_mode": "provider_argv",
        "provider_id": provider_id,
    }


def run_committed_role_launch(
    *,
    launch_spec: Mapping[str, Any],
    log_dir: Path,
    label: str,
    timeout_s: int,
    idle_timeout_s: int,
    terminal_success_paths: list[str] | tuple[str, ...] | None = None,
    terminal_success_stable_s: float | None = None,
    terminal_success_streams: list[str] | tuple[str, ...] | None = None,
    terminal_success_patterns: list[str] | tuple[str, ...] | None = None,
    reconnect_grace_s: int,
    reconnect_max_events: int,
) -> Any:
    return run_cmd(
        cmd=[str(item) for item in list(launch_spec.get("argv") or [])],
        cwd=Path(str(launch_spec.get("cwd") or "")).expanduser().resolve(),
        stdin_path=str(launch_spec.get("stdin_path") or ""),
        log_dir=log_dir,
        label=label,
        timeout_s=timeout_s,
        idle_timeout_s=idle_timeout_s,
        terminal_success_paths=terminal_success_paths,
        terminal_success_stable_s=terminal_success_stable_s,
        terminal_success_streams=terminal_success_streams,
        terminal_success_patterns=terminal_success_patterns,
        reconnect_grace_s=reconnect_grace_s,
        reconnect_max_events=reconnect_max_events,
        capture_text=False,
        env={str(k): str(v) for k, v in dict(launch_spec.get("env") or {}).items()},
    )
