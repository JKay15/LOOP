#!/usr/bin/env python3
"""Validate read-only runtime-identity trace over canonical identity, supervision, and liveness facts."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-identity-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_runtime_identity_trace_view
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    helper_path = ROOT / "tests" / "check_loop_system_repo_lease_lineage_trace.py"
    helper_spec = importlib.util.spec_from_file_location("loop_system_repo_lease_lineage_trace_helpers", helper_path)
    if helper_spec is None or helper_spec.loader is None:
        return _fail("failed to load lease-lineage helper module")
    helper_module = importlib.util.module_from_spec(helper_spec)
    helper_spec.loader.exec_module(helper_module)
    _base_kernel_state = helper_module._base_kernel_state
    _commit_live_lease_lineage = helper_module._commit_live_lease_lineage
    _commit_missing_heartbeat_lineage = helper_module._commit_missing_heartbeat_lineage
    _persist_projection_bundle = helper_module._persist_projection_bundle

    with temporary_repo_root(prefix="loop_system_runtime_identity_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_live_lease_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_runtime_identity_trace_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("runtime-identity trace must stay read-only")
        if not bool(trace.get("read_only")):
            return _fail("runtime-identity trace must declare itself read-only")
        if str(trace.get("node_id") or "") != anchor["node_id"]:
            return _fail("runtime-identity trace must preserve node identity")
        if str(trace.get("launch_event_id") or "") != anchor["launch_event_id"]:
            return _fail("runtime-identity trace must preserve launch identity continuity")
        if str(trace.get("lease_owner_id") or "") != anchor["lease_owner_id"]:
            return _fail("runtime-identity trace must preserve lease-owner continuity")
        if int(trace.get("lease_epoch") or 0) != 3:
            return _fail("runtime-identity trace must preserve lease epoch continuity")
        if str(trace.get("launch_origin_kind") or "") != "launch_started":
            return _fail("runtime-identity trace must preserve launch-origin kind")
        if not bool(trace.get("process_identity_present")):
            return _fail("runtime-identity trace must report canonical process-identity evidence")
        if not bool(trace.get("supervision_attached_present")):
            return _fail("runtime-identity trace must report canonical supervision attachment evidence")
        if not bool(trace.get("heartbeat_observed_present")):
            return _fail("runtime-identity trace must report canonical heartbeat evidence for a live lease")
        process_identity_anchor = dict(trace.get("process_identity_anchor") or {})
        if int(process_identity_anchor.get("pid") or 0) != 43001:
            return _fail("runtime-identity trace must expose the canonical process-identity pid")
        if str(process_identity_anchor.get("process_fingerprint") or "") != "process:lease-lineage-live":
            return _fail("runtime-identity trace must expose the canonical process-identity fingerprint")
        supervision_identity_anchor = dict(trace.get("supervision_identity_anchor") or {})
        if str(supervision_identity_anchor.get("source_event_type") or "") != "heartbeat_observed":
            return _fail("runtime-identity trace must anchor live supervision identity on the heartbeat fact")
        if int(supervision_identity_anchor.get("pid") or 0) != 53001:
            return _fail("runtime-identity trace must expose the canonical supervision pid")
        current_runtime_identity = dict(trace.get("current_runtime_identity") or {})
        if int(current_runtime_identity.get("pid") or 0) != 43001:
            return _fail("runtime-identity trace must expose the currently visible runtime pid")
        if str(current_runtime_identity.get("process_fingerprint") or "") != "process:lease-lineage-live":
            return _fail("runtime-identity trace must expose the currently visible runtime fingerprint")
        if str(current_runtime_identity.get("effective_attachment_state") or "") != "ATTACHED":
            return _fail("runtime-identity trace must expose effective attachment truth")
        if str(trace.get("closure_kind") or ""):
            return _fail("live runtime-identity trace must not claim a closure kind")
        if list(trace.get("gaps") or []):
            return _fail("complete runtime-identity trace must not report causal gaps")

    with temporary_repo_root(prefix="loop_system_runtime_identity_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_missing_heartbeat_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_runtime_identity_trace_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("runtime-identity gap trace must stay read-only")
        gaps = [str(item) for item in list(trace.get("gaps") or [])]
        if "missing_canonical_heartbeat_observed_event" not in gaps:
            return _fail("runtime-identity trace must surface the missing canonical heartbeat gap")
        if bool(trace.get("heartbeat_observed_present")):
            return _fail("runtime-identity gap scenario must not pretend a heartbeat fact exists")
        supervision_identity_anchor = dict(trace.get("supervision_identity_anchor") or {})
        if str(supervision_identity_anchor.get("source_event_type") or "") != "supervision_attached":
            return _fail("runtime-identity gap must fall back to the supervision-attached anchor")

    print("[loop-system-runtime-identity-trace] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
