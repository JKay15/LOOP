#!/usr/bin/env python3
"""Validate provider-capacity cooldown behavior without automatic checker downgrade."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-capacity-relief][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        from loop_product.loop.evaluator_supervision import (  # type: ignore
            _provider_capacity_recovery_cooldown_s,
        )
        from loop_product.evaluator.role_launch import build_committed_role_launch_spec  # type: ignore
        from loop_product.evaluator.prototype import _provider_retry_delay_s  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import evaluator capacity-relief helpers: {exc}")

    expected_cooldown = {
        0: 0.0,
        1: 30.0,
        3: 30.0,
        4: 30.0,
        7: 30.0,
        8: 30.0,
        15: 30.0,
        16: 30.0,
        31: 30.0,
        32: 30.0,
        99: 30.0,
    }
    for attempts, expected in expected_cooldown.items():
        observed = _provider_capacity_recovery_cooldown_s(prior_attempt_count=attempts)
        if observed != expected:
            return _fail(
                "provider-capacity cooldown must escalate to meaningful persisted waits, "
                f"got attempts={attempts} -> {observed}, expected {expected}"
            )

    if _provider_retry_delay_s("provider_capacity", failure_count=1) != 2.0:
        return _fail("provider-capacity must preserve one bounded role-local retry before outer cooldown takes over")
    if _provider_retry_delay_s("provider_capacity", failure_count=2) is not None:
        return _fail("provider-capacity must hand off to outer runtime cooldown after the first bounded role-local retry")
    if _provider_retry_delay_s("provider_transport", failure_count=1) != 1.0:
        return _fail("provider_transport delay schedule regressed unexpectedly")
    if _provider_retry_delay_s("provider_runtime", failure_count=2) != 2.0:
        return _fail("provider_runtime delay schedule regressed unexpectedly")

    launch_spec = build_committed_role_launch_spec(
        resolved=SimpleNamespace(
            source="provider_argv",
            provider_id="codex_cli",
            prompt_transport="stdin",
            prompt_arg="",
        ),
        config={
            "sandbox_mode": "workspace-write",
            "reasoning_effort": "minimal",
            "model": "gpt-5.4-mini",
        },
        prompt_path=ROOT / ".tmp_prompt.md",
        workspace_root=ROOT,
        response_path=ROOT / ".tmp_response.md",
        base_env={},
    )
    argv = [str(item) for item in list(launch_spec.get("argv") or [])]
    if "-m" not in argv or "gpt-5.4-mini" not in argv:
        return _fail("committed evaluator role launch must preserve explicit checker model selection")
    if "-c" not in argv:
        return _fail("committed evaluator role launch must preserve explicit checker reasoning_effort flag")
    effort_index = argv.index("-c") + 1
    if effort_index >= len(argv) or argv[effort_index] != 'model_reasoning_effort="minimal"':
        return _fail("committed evaluator role launch must preserve explicit checker reasoning_effort")

    print("[loop-system-evaluator-capacity-relief] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
