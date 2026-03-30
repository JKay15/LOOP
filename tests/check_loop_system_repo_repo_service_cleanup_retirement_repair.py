#!/usr/bin/env python3
"""Validate canonical repair of mistaken repo-service cleanup retirement."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-service-cleanup-retirement-repair][FAIL] {msg}", file=sys.stderr)
    return 2


def _all_blocked(payload: dict[str, object] | None) -> bool:
    bundle = dict(payload or {})
    for key in (
        "repo_reactor_residency_guard",
        "repo_reactor",
        "repo_control_plane",
        "host_child_launch_supervisor",
        "housekeeping_reap_controller",
    ):
        item = dict(bundle.get(key) or {})
        if str(item.get("bootstrap_status") or "") != "blocked_cleanup_committed":
            return False
    return True


def main() -> int:
    try:
        from loop_product.kernel.query import query_real_root_repo_service_cleanup_retirement_repair_trace_view
        from loop_product.runtime import cleanup_test_repo_services
        from loop_product.runtime.control_plane import ensure_repo_control_plane_services_running
        from loop_product.runtime.control_plane import (
            repo_control_plane_runtime_root,
            repo_reactor_residency_guard_runtime_root,
            repo_reactor_runtime_root,
        )
        from loop_product.runtime.rehearsal import repair_real_root_repo_service_cleanup_retirement
        from loop_product.runtime.gc import housekeeping_runtime_root
        from loop_product import host_child_launch_supervisor as supervisor_module
        from loop_product.runtime_identity import ensure_runtime_root_identity, runtime_root_identity_ref
        from loop_product.state_io import write_json_read_only
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_service_cleanup_retirement_repair_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"

        started = ensure_repo_control_plane_services_running(repo_root=repo_root)
        if int(dict(started.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
            return _fail("setup must start repo services before cleanup-retirement repair is exercised")

        service_roots = {
            "repo_reactor_residency_guard": repo_reactor_residency_guard_runtime_root(repo_root=repo_root),
            "repo_reactor": repo_reactor_runtime_root(repo_root=repo_root),
            "repo_control_plane": repo_control_plane_runtime_root(repo_root=repo_root),
            "host_child_launch_supervisor": supervisor_module.host_supervisor_runtime_root(repo_root=repo_root),
            "housekeeping_reap_controller": housekeeping_runtime_root(repo_root=repo_root),
        }
        original_identities = {
            service_kind: ensure_runtime_root_identity(state_root)
            for service_kind, state_root in service_roots.items()
        }

        cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root)
        if not bool(cleanup_receipt.get("quiesced")):
            return _fail(f"cleanup setup must quiesce repo services, got: {cleanup_receipt}")
        for service_kind, state_root in service_roots.items():
            original_identity = dict(original_identities.get(service_kind) or {})
            if not original_identity:
                return _fail(f"setup must capture original runtime identity for {service_kind}")
            write_json_read_only(runtime_root_identity_ref(state_root), original_identity)

        blocked = ensure_repo_control_plane_services_running(
            repo_root=repo_root,
            trigger_kind="test_preflight_after_cleanup",
            trigger_ref="cleanup_receipt",
        )
        if not _all_blocked(blocked):
            return _fail(f"cleanup-retired repo services must stay blocked before repair, got: {blocked}")

        receipt = repair_real_root_repo_service_cleanup_retirement(repo_root=repo_root)
        if str(receipt.get("status") or "") != "repaired":
            return _fail(f"repair helper must report repaired status for mistaken repo-service cleanup, got: {receipt}")
        if int(receipt.get("reseeded_service_count") or 0) < 5:
            return _fail(f"repair helper must reseed all blocked repo services, got: {receipt}")

        trace = query_real_root_repo_service_cleanup_retirement_repair_trace_view(repo_root / ".loop" / "housekeeping")
        if str(trace.get("status") or "") != "repaired":
            return _fail(f"repair trace must expose repaired status, got: {trace}")
        if int(trace.get("reseeded_service_count") or 0) != int(receipt.get("reseeded_service_count") or 0):
            return _fail("repair trace must preserve reseeded service count")

        restarted = ensure_repo_control_plane_services_running(
            repo_root=repo_root,
            trigger_kind="test_after_cleanup_repair",
            trigger_ref=str(receipt.get("repair_result_event_id") or ""),
        )
        if int(dict(restarted.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
            return _fail(f"repo control-plane must restart after cleanup-retirement repair, got: {restarted}")
        if str(dict(restarted.get("repo_control_plane") or {}).get("bootstrap_status") or "").strip() == "blocked_cleanup_committed":
            return _fail("repo control-plane must no longer be blocked by cleanup authority after repair")

    print("[loop-system-repo-service-cleanup-retirement-repair] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
