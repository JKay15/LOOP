#!/usr/bin/env python3
"""Validate repo-scoped supervision reactor exit behavior once the registry drains."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-supervision-reactor-empty-registry-exit][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        import loop_product.child_supervision_sidecar as sidecar_module  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import child supervision sidecar helpers: {exc}")

    if sidecar_module._repo_supervision_reactor_should_exit(  # type: ignore[attr-defined]
        tracked_launch_refs=["/tmp/launch-a.json"],
        worker_count=1,
        empty_registry_since_monotonic=None,
        now_monotonic=10.0,
        orphan_worker_grace_s=1.0,
    ):
        return _fail("reactor must not exit while the registry still tracks launches")

    if sidecar_module._repo_supervision_reactor_should_exit(  # type: ignore[attr-defined]
        tracked_launch_refs=[],
        worker_count=1,
        empty_registry_since_monotonic=10.0,
        now_monotonic=10.2,
        orphan_worker_grace_s=1.0,
    ):
        return _fail("reactor must not exit immediately when an orphan worker might still be unwinding")

    if not sidecar_module._repo_supervision_reactor_should_exit(  # type: ignore[attr-defined]
        tracked_launch_refs=[],
        worker_count=1,
        empty_registry_since_monotonic=10.0,
        now_monotonic=11.5,
        orphan_worker_grace_s=1.0,
    ):
        return _fail("reactor must self-exit after the empty-registry grace window if only orphan workers remain")

    if not sidecar_module._repo_supervision_reactor_should_exit(  # type: ignore[attr-defined]
        tracked_launch_refs=[],
        worker_count=0,
        empty_registry_since_monotonic=10.0,
        now_monotonic=10.1,
        orphan_worker_grace_s=1.0,
    ):
        return _fail("reactor must exit immediately once both the registry and worker set are empty")

    print("[loop-system-supervision-reactor-empty-registry-exit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
