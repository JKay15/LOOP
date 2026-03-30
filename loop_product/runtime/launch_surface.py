"""Unified trusted child-launch surface with committed supervision attach."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loop_product.dispatch.launch_runtime import launch_child_from_result_ref as dispatch_launch_child_from_result_ref
from loop_product.dispatch.launch_runtime import terminate_runtime_owned_launch_result_ref


def launch_child_from_result_ref(
    *,
    result_ref: str | Path,
    startup_probe_ms: int = 1500,
    startup_health_timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Launch a materialized child and attach committed supervision fail-closed."""

    launch_payload = dispatch_launch_child_from_result_ref(
        result_ref=result_ref,
        startup_probe_ms=startup_probe_ms,
        startup_health_timeout_ms=startup_health_timeout_ms,
    )
    launch_decision = str(launch_payload.get("launch_decision") or "").strip()
    if launch_decision not in {"started", "started_existing"}:
        return launch_payload
    launch_result_ref = str(launch_payload.get("launch_result_ref") or "").strip()
    if not launch_result_ref:
        raise ValueError("committed child launch did not return launch_result_ref")
    try:
        from loop_product.child_supervision_sidecar import ensure_child_supervision_running

        ensure_child_supervision_running(launch_result_ref=launch_result_ref)
    except Exception:
        try:
            terminate_runtime_owned_launch_result_ref(result_ref=launch_result_ref)
        except Exception:
            pass
        raise
    return launch_payload
