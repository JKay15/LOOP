#!/usr/bin/env python3
"""Validate repo-scoped supervision reactor idle backoff and runtime-marker throttling."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-supervision-reactor-idle-backoff][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        import loop_product.child_supervision_sidecar as sidecar_module  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import child supervision sidecar helpers: {exc}")

    base_poll = 0.05
    steady_sleep = sidecar_module._repo_supervision_reactor_sleep_s(  # type: ignore[attr-defined]
        base_poll_interval_s=base_poll,
        tracked_launch_refs=[
            "/tmp/launch-a.json",
            "/tmp/launch-b.json",
        ],
        previous_tracked_launch_refs=[
            "/tmp/launch-a.json",
            "/tmp/launch-b.json",
        ],
        worker_count=2,
        had_worker_state_change=False,
    )
    if steady_sleep <= 0.25:
        return _fail("steady repo-scoped reactor loops with unchanged tracked children must back off above the base poll interval")

    changed_sleep = sidecar_module._repo_supervision_reactor_sleep_s(  # type: ignore[attr-defined]
        base_poll_interval_s=base_poll,
        tracked_launch_refs=[
            "/tmp/launch-a.json",
            "/tmp/launch-c.json",
        ],
        previous_tracked_launch_refs=[
            "/tmp/launch-a.json",
            "/tmp/launch-b.json",
        ],
        worker_count=2,
        had_worker_state_change=False,
    )
    if abs(changed_sleep - base_poll) > 1e-9:
        return _fail("tracked-launch topology changes must keep the reactor on the short base poll interval")

    if not sidecar_module._should_refresh_repo_supervision_runtime(  # type: ignore[attr-defined]
        tracked_launch_refs=["/tmp/launch-a.json"],
        previous_tracked_launch_refs=["/tmp/launch-b.json"],
        had_worker_state_change=False,
        now_monotonic=10.0,
        last_runtime_update_monotonic=9.95,
        heartbeat_interval_s=1.0,
    ):
        return _fail("runtime marker must refresh immediately when the tracked child set changes")

    if sidecar_module._should_refresh_repo_supervision_runtime(  # type: ignore[attr-defined]
        tracked_launch_refs=["/tmp/launch-a.json"],
        previous_tracked_launch_refs=["/tmp/launch-a.json"],
        had_worker_state_change=False,
        now_monotonic=10.2,
        last_runtime_update_monotonic=10.0,
        heartbeat_interval_s=1.0,
    ):
        return _fail("runtime marker must not rewrite every loop while tracked launches stay unchanged inside the heartbeat window")

    if not sidecar_module._should_refresh_repo_supervision_runtime(  # type: ignore[attr-defined]
        tracked_launch_refs=["/tmp/launch-a.json"],
        previous_tracked_launch_refs=["/tmp/launch-a.json"],
        had_worker_state_change=False,
        now_monotonic=11.5,
        last_runtime_update_monotonic=10.0,
        heartbeat_interval_s=1.0,
    ):
        return _fail("runtime marker must still refresh periodically even when the tracked child set stays unchanged")

    print("[loop-system-supervision-reactor-idle-backoff] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
