#!/usr/bin/env python3
"""Validate read-only lineage parity debugging over node/launch/lease trace surfaces."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-lineage-parity-debugger][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_lineage_parity_debugger_view
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

    with temporary_repo_root(prefix="loop_system_lineage_parity_debugger_complete_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_live_lease_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        debug = query_lineage_parity_debugger_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("complete lineage parity debugger must stay read-only")
        if not bool(debug.get("read_only")):
            return _fail("lineage parity debugger must declare itself read-only")
        if str(debug.get("trace_surface") or "") != "node_lineage_trace":
            return _fail("lineage parity debugger must report node_lineage_trace as its backing surface")
        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(trusted.get("node_id") or "") != anchor["node_id"]:
            return _fail("trusted lineage parity view must anchor to the requested node id")
        if list(trusted.get("ancestor_node_ids") or []) != ["root-kernel"]:
            return _fail("trusted lineage parity view must expose the ordered ancestor chain")
        if list(projected.get("ancestor_node_ids") or []) != ["root-kernel"]:
            return _fail("projected lineage parity view must preserve the ordered ancestor chain")
        if str(projected.get("launch_event_id") or "") != anchor["launch_event_id"]:
            return _fail("projected lineage parity view must preserve the visible launch identity")
        if str(projected.get("lease_owner_id") or "") != anchor["lease_owner_id"]:
            return _fail("projected lineage parity view must preserve the visible lease owner")
        if int(projected.get("lease_epoch") or 0) != 3:
            return _fail("projected lineage parity view must preserve the visible lease epoch")
        if int(projected.get("runtime_pid") or 0) != 43001:
            return _fail("projected lineage parity view must preserve the visible runtime pid")
        if str(projected.get("runtime_process_fingerprint") or "") != "process:lease-lineage-live":
            return _fail("projected lineage parity view must preserve the visible runtime fingerprint")
        if str(projected.get("launch_trace_source_node_id") or "") != anchor["node_id"]:
            return _fail("launch lineage trace must stay anchored to the visible source node")
        if str(projected.get("lease_trace_source_node_id") or "") != anchor["node_id"]:
            return _fail("lease lineage trace must stay anchored to the visible source node")
        if str(projected.get("runtime_trace_node_id") or "") != anchor["node_id"]:
            return _fail("runtime-identity trace must stay anchored to the visible source node")
        if list(debug.get("trace_gaps") or []):
            return _fail("complete lineage parity debugger must not report trace gaps")
        if list(debug.get("mismatches") or []):
            return _fail("complete lineage parity debugger must not report mismatches")

    with temporary_repo_root(prefix="loop_system_lineage_parity_debugger_gap_") as repo_root:
        state_root = repo_root / ".loop"
        kernel_state = _base_kernel_state()
        anchor = _commit_missing_heartbeat_lineage(state_root, repo_root)
        _persist_projection_bundle(state_root, kernel_state)
        before = committed_event_count(state_root)
        debug = query_lineage_parity_debugger_view(state_root, node_id=anchor["node_id"])
        after = committed_event_count(state_root)
        if before != after:
            return _fail("gapped lineage parity debugger must stay read-only")
        trace_gaps = [str(item) for item in list(debug.get("trace_gaps") or [])]
        if "lease_lineage_gaps_present" not in trace_gaps:
            return _fail("gapped lineage parity debugger must surface nested lease-lineage gaps")
        if "runtime_identity_trace_gaps_present" not in trace_gaps:
            return _fail("gapped lineage parity debugger must surface nested runtime-identity gaps")
        mismatches = [str(item) for item in list(debug.get("mismatches") or [])]
        if "trace_gaps_present" not in mismatches:
            return _fail("gapped lineage parity debugger must report explicit trace-gaps parity")
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(projected.get("launch_event_id") or "") != anchor["launch_event_id"]:
            return _fail("gapped lineage parity debugger must still expose the visible launch identity")
        if str(projected.get("lease_owner_id") or "") != anchor["lease_owner_id"]:
            return _fail("gapped lineage parity debugger must still expose the visible lease owner")
        if int(projected.get("runtime_pid") or 0) != 43002:
            return _fail("gapped lineage parity debugger must still expose the visible runtime pid")

    print("[loop-system-lineage-parity-debugger] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
