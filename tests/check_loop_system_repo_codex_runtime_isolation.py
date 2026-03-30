#!/usr/bin/env python3
"""Validate minimal runtime-owned Codex home sharing and explicit model pinning."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-codex-runtime-isolation][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        import loop_product.codex_home as codex_home  # type: ignore
        from loop_product.dispatch.launch_policy import build_codex_cli_child_launch  # type: ignore
        from loop_product.evaluator.prototype import _normalize_agent_config  # type: ignore
        from loop_product.evaluator.role_launch import build_committed_role_launch_spec  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import Codex runtime helpers: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop-codex-home-") as tmpdir:
        tmp_root = Path(tmpdir).resolve()
        host_home = tmp_root / "host_codex"
        runtime_home = tmp_root / "runtime_codex"
        host_home.mkdir(parents=True, exist_ok=True)
        (host_home / "auth.json").write_text("{}", encoding="utf-8")
        (host_home / "config.toml").write_text('model = "gpt-5.4"\n', encoding="utf-8")
        (host_home / "version.json").write_text('{"version":"test"}', encoding="utf-8")
        (host_home / "models_cache.json").write_text("{}", encoding="utf-8")
        for dirname in ("plugins", "skills", "vendor_imports", "rules", "memories"):
            path = host_home / dirname
            path.mkdir(parents=True, exist_ok=True)
            (path / "marker.txt").write_text(dirname, encoding="utf-8")

        original_host_codex_home = codex_home.host_codex_home
        codex_home.host_codex_home = lambda: host_home  # type: ignore[assignment]
        try:
            prepared = codex_home.prepare_runtime_owned_codex_home(runtime_home=runtime_home)
        finally:
            codex_home.host_codex_home = original_host_codex_home  # type: ignore[assignment]

        if prepared != runtime_home:
            return _fail("prepare_runtime_owned_codex_home must return the resolved runtime home path")
        for relpath in ("auth.json", "version.json", "models_cache.json"):
            if not (runtime_home / relpath).exists():
                return _fail(f"runtime-owned CODEX_HOME must preserve minimal shared surface {relpath}")
        for relpath in ("config.toml", "plugins", "skills", "vendor_imports", "rules", "memories"):
            if (runtime_home / relpath).exists():
                return _fail(f"runtime-owned CODEX_HOME must not inherit host desktop surface {relpath}")

    normalized = _normalize_agent_config(
        {
            "agent_execution": {
                "default": {
                    "agent_provider": "codex_cli",
                    "sandbox_mode": "workspace-write",
                    "reasoning_effort": "high",
                }
            }
        },
        "checker",
    )
    if normalized.get("model") != codex_home.default_codex_model():
        return _fail("evaluator agent config must inject an explicit default Codex model")

    committed_default = build_committed_role_launch_spec(
        resolved=SimpleNamespace(
            source="provider_argv",
            provider_id="codex_cli",
            prompt_transport="stdin",
            prompt_arg="",
        ),
        config={
            "sandbox_mode": "workspace-write",
            "reasoning_effort": "high",
        },
        prompt_path=ROOT / ".tmp_prompt.md",
        workspace_root=ROOT,
        response_path=ROOT / ".tmp_response.json",
        base_env={},
    )
    committed_default_argv = [str(item) for item in list(committed_default.get("argv") or [])]
    if "-m" not in committed_default_argv:
        return _fail("committed evaluator role launch must pin an explicit default model")
    default_model_index = committed_default_argv.index("-m") + 1
    if default_model_index >= len(committed_default_argv) or committed_default_argv[default_model_index] != codex_home.default_codex_model():
        return _fail("committed evaluator role launch must pin the stable default Codex model")

    committed_explicit = build_committed_role_launch_spec(
        resolved=SimpleNamespace(
            source="provider_argv",
            provider_id="codex_cli",
            prompt_transport="stdin",
            prompt_arg="",
        ),
        config={
            "sandbox_mode": "workspace-write",
            "reasoning_effort": "high",
            "model": "gpt-5.4-mini",
        },
        prompt_path=ROOT / ".tmp_prompt.md",
        workspace_root=ROOT,
        response_path=ROOT / ".tmp_response.json",
        base_env={},
    )
    committed_explicit_argv = [str(item) for item in list(committed_explicit.get("argv") or [])]
    explicit_model_index = committed_explicit_argv.index("-m") + 1
    if committed_explicit_argv[explicit_model_index] != "gpt-5.4-mini":
        return _fail("committed evaluator role launch must preserve an explicit caller-specified model")

    child_launch = build_codex_cli_child_launch(
        workspace_root=ROOT / "workspace" / "example-project",
        sandbox_mode="danger-full-access",
        thinking_budget="medium",
        prompt_path=ROOT / "workspace" / "example-project" / "CHILD_PROMPT.md",
    )
    child_argv = [str(item) for item in list(child_launch.get("argv") or [])]
    if "-m" not in child_argv:
        return _fail("child Codex launch policy must pin an explicit model")
    child_model_index = child_argv.index("-m") + 1
    if child_argv[child_model_index] != codex_home.default_codex_model():
        return _fail("child Codex launch policy must pin the stable default Codex model")

    print("[loop-system-codex-runtime-isolation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
