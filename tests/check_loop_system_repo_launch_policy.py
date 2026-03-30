#!/usr/bin/env python3
"""Validate policy-preserving child launch helpers."""

from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-launch-policy][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        from loop_product.dispatch.launch_policy import (  # type: ignore
            build_codex_cli_child_launch,
            codex_reasoning_effort_for_thinking_budget,
        )
        from loop_product.codex_home import default_codex_model  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import launch-policy helpers: {exc}")

    expected = {
        "low": "low",
        "medium": "medium",
        "high": "high",
        "xhigh": "xhigh",
    }
    observed = {
        key: codex_reasoning_effort_for_thinking_budget(key)
        for key in expected
    }
    if observed != expected:
        return _fail(f"thinking-budget mapping must stay explicit and lossless, got {observed!r}")

    workspace_root = ROOT / "workspace" / "example-project"
    prompt_path = workspace_root / "CHILD_PROMPT.md"
    original_env = {key: os.environ.get(key) for key in ("http_proxy", "all_proxy", "NO_PROXY")}
    os.environ["http_proxy"] = "http://127.0.0.1:7890"
    os.environ["all_proxy"] = "socks5://127.0.0.1:7890"
    os.environ["NO_PROXY"] = "localhost,127.0.0.1"
    try:
        launch = build_codex_cli_child_launch(
            workspace_root=workspace_root,
            sandbox_mode="danger-full-access",
            thinking_budget="medium",
            prompt_path=prompt_path,
        )
    finally:
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    if str(launch.get("cwd") or "") != str(workspace_root.resolve()):
        return _fail("child launch helper must pin cwd to the node workspace root")
    if str(launch.get("stdin_path") or "") != str(prompt_path.resolve()):
        return _fail("child launch helper must preserve the prompt file as stdin_path")

    env = dict(launch.get("env") or {})
    if env.get("HTTP_PROXY") != "http://127.0.0.1:7890" or env.get("http_proxy") != "http://127.0.0.1:7890":
        return _fail(f"child launch helper must preserve parent HTTP proxy transport env, got {env!r}")
    if env.get("ALL_PROXY") != "socks5://127.0.0.1:7890" or env.get("all_proxy") != "socks5://127.0.0.1:7890":
        return _fail(f"child launch helper must preserve parent ALL_PROXY transport env, got {env!r}")
    if env.get("NO_PROXY") != "localhost,127.0.0.1" or env.get("no_proxy") != "localhost,127.0.0.1":
        return _fail(f"child launch helper must preserve parent NO_PROXY transport env, got {env!r}")
    if "HOME" in env or "CODEX_HOME" in env:
        return _fail(f"child launch helper must not override HOME or CODEX_HOME, got {env!r}")

    argv = list(launch.get("argv") or [])
    expected_codex_home = str((Path.home() / ".codex").resolve())
    expected_prefix = [
        "codex",
        "exec",
        "-C",
        str(workspace_root.resolve()),
        "--skip-git-repo-check",
        "-s",
        "danger-full-access",
        "--add-dir",
        expected_codex_home,
        "-m",
        default_codex_model(),
        "-c",
        'model_reasoning_effort="medium"',
        "-",
    ]
    if argv != expected_prefix:
        return _fail(f"child launch helper must preserve exact Codex launch policy, got {argv!r}")

    print("[loop-system-launch-policy] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
