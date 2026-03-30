#!/usr/bin/env python3
"""Validate projection manifest visibility across untracked, partial, and atomic states."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-manifest][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view, query_projection_consistency_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection manifest visibility",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="projection-manifest-child-001",
        node_kind="implementer",
        goal_slice="exercise projection manifest visibility",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-manifest-child-001.json",
        result_sink_ref="artifacts/projection-manifest-child-001/result.json",
        lineage_ref="root-kernel->projection-manifest-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_manifest_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-manifest",
            root_goal="validate projection manifest visibility",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        initial_view = query_projection_consistency_view(state_root)
        if str(initial_view.get("visibility_state") or "") != "untracked":
            return _fail("projection consistency must report untracked before a projection manifest exists")
        manifest = dict(initial_view.get("projection_manifest") or {})
        if bool(manifest.get("exists")):
            return _fail("projection manifest metadata must report missing manifest before first trusted rebuild")

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projection manifest dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before manifest self-heal")

        _ = query_authority_view(state_root)

        healed_view = query_projection_consistency_view(state_root)
        healed_manifest = dict(healed_view.get("projection_manifest") or {})
        if str(healed_view.get("visibility_state") or "") != "atomically_visible":
            return _fail("projection consistency must report atomically_visible after trusted rebuild writes a manifest")
        if not bool(healed_manifest.get("exists")):
            return _fail("trusted rebuild must materialize the projection manifest")
        if int(healed_manifest.get("last_applied_seq") or 0) != int(healed_view.get("committed_seq") or 0):
            return _fail("projection manifest must track the committed seq reached by trusted rebuild")
        if list(healed_manifest.get("covered_node_ids") or []) != [child_node.node_id, root_node.node_id]:
            return _fail("projection manifest must cover the complete sorted node set for the projection bundle")

        child_snapshot_ref = state_root / "state" / f"{child_node.node_id}.json"
        child_snapshot = json.loads(child_snapshot_ref.read_text(encoding="utf-8"))
        child_snapshot["last_applied_seq"] = 0
        child_snapshot["projection_updated_at"] = "2026-03-24T00:00:00Z"
        os.chmod(child_snapshot_ref, 0o644)
        child_snapshot_ref.write_text(json.dumps(child_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        partial_view = query_projection_consistency_view(state_root)
        if str(partial_view.get("visibility_state") or "") != "partial":
            return _fail("projection consistency must report partial when a covered node projection falls behind the manifest bundle")
        if bool(dict(partial_view.get("projection_manifest") or {}).get("exists")) is not True:
            return _fail("partial projection visibility must still expose the existing manifest metadata")

        _ = query_authority_view(state_root)
        re_healed_view = query_projection_consistency_view(state_root)
        if str(re_healed_view.get("visibility_state") or "") != "atomically_visible":
            return _fail("trusted self-heal must restore atomically_visible after repairing a partial bundle")

    print("[loop-system-event-journal-projection-manifest][OK] projection manifest tracks untracked, partial, and atomically-visible projection bundles")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
