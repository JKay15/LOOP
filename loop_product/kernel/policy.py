"""Kernel execution policies for the wave-1 bootstrap."""

from __future__ import annotations


def kernel_execution_policy() -> dict[str, str]:
    return {
        "mode": "kernel",
        "agent_provider": "codex_cli",
        "sandbox_mode": "danger-full-access",
        "retry_policy": "supervisor_only",
    }


def kernel_reasoning_profile() -> dict[str, str]:
    return {"thinking_budget": "medium", "role": "kernel"}


def implementer_execution_policy() -> dict[str, str]:
    return {
        "agent_provider": "codex_cli",
        "sandbox_mode": "danger-full-access",
        "retry_policy": "single_retry",
    }


def implementer_reasoning_profile() -> dict[str, str]:
    return {"thinking_budget": "high", "role": "implementer"}


def implementer_budget_profile() -> dict[str, int]:
    return {"max_rounds": 2}
