#!/usr/bin/env python3
"""Validate read-only node-lineage trace over ancestor chain plus nested launch/lease traces."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-node-lineage-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_node_lineage_trace_view
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

    with temporary_repo_root(prefix="loop_system_node_lineage_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_live_lease_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_node_lineage_trace_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("node-lineage trace must stay read-only")
        if not bool(trace.get("read_only")):
            return _fail("node-lineage trace must declare itself read-only")
        lineage_chain = [dict(item or {}) for item in list(trace.get("lineage_chain") or [])]
        lineage_node_ids = [str(item.get("node_id") or "") for item in lineage_chain]
        if lineage_node_ids != ["root-kernel", anchor["node_id"]]:
            return _fail("node-lineage trace must expose the visible root-to-node ancestor chain")
        if str(dict(trace.get("current_node") or {}).get("node_id") or "") != anchor["node_id"]:
            return _fail("node-lineage trace must expose the current node projection")
        if not bool(dict(trace.get("launch_lineage") or {}).get("read_only")):
            return _fail("node-lineage trace must reuse the nested launch-lineage surface")
        if not bool(dict(trace.get("lease_lineage") or {}).get("read_only")):
            return _fail("node-lineage trace must reuse the nested lease-lineage surface")
        if not bool(dict(trace.get("runtime_identity_trace") or {}).get("read_only")):
            return _fail("node-lineage trace must reuse the nested runtime-identity trace surface")
        if list(dict(trace.get("launch_lineage") or {}).get("gaps") or []):
            return _fail("complete node lineage must not carry nested launch-lineage gaps")
        if list(dict(trace.get("lease_lineage") or {}).get("gaps") or []):
            return _fail("complete node lineage must not carry nested lease-lineage gaps")
        if list(dict(trace.get("runtime_identity_trace") or {}).get("gaps") or []):
            return _fail("complete node lineage must not carry nested runtime-identity gaps")
        current_runtime_identity = dict(
            dict(trace.get("runtime_identity_trace") or {}).get("current_runtime_identity") or {}
        )
        if int(current_runtime_identity.get("pid") or 0) != 43001:
            return _fail("node-lineage trace must preserve the nested visible runtime pid")
        if str(current_runtime_identity.get("process_fingerprint") or "") != "process:lease-lineage-live":
            return _fail("node-lineage trace must preserve the nested visible runtime fingerprint")
        if list(trace.get("gaps") or []):
            return _fail("complete node lineage must not report aggregate causal gaps")

    with temporary_repo_root(prefix="loop_system_node_lineage_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_missing_heartbeat_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        trace = query_node_lineage_trace_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("gapped node-lineage trace must stay read-only")
        gaps = [str(item) for item in list(trace.get("gaps") or [])]
        if "lease_lineage_gaps_present" not in gaps:
            return _fail("node-lineage trace must surface nested lease-lineage gaps")
        if "runtime_identity_trace_gaps_present" not in gaps:
            return _fail("node-lineage trace must surface nested runtime-identity gaps")
        if bool(dict(trace.get("lease_lineage") or {}).get("heartbeat_observed_present")):
            return _fail("gapped node lineage must not pretend nested heartbeat evidence exists")
        runtime_trace_gaps = [str(item) for item in list(dict(trace.get("runtime_identity_trace") or {}).get("gaps") or [])]
        if "missing_canonical_heartbeat_observed_event" not in runtime_trace_gaps:
            return _fail("gapped node lineage must preserve the nested runtime-identity causal gap")

    print("[loop-system-node-lineage-trace] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
