#!/usr/bin/env python3
"""Validate policy-preserving child launch helpers."""

from __future__ import annotations

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
    launch = build_codex_cli_child_launch(
        workspace_root=workspace_root,
        sandbox_mode="danger-full-access",
        thinking_budget="medium",
        prompt_path=prompt_path,
    )
    if str(launch.get("cwd") or "") != str(workspace_root.resolve()):
        return _fail("child launch helper must pin cwd to the node workspace root")
    if str(launch.get("stdin_path") or "") != str(prompt_path.resolve()):
        return _fail("child launch helper must preserve the prompt file as stdin_path")

    env = dict(launch.get("env") or {})
    if env:
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
