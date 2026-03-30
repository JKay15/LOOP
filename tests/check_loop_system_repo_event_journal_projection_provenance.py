#!/usr/bin/env python3
"""Validate projection provenance refs back to journal and bundle manifest."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-projection-provenance][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.event_journal import event_journal_db_ref, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import (
            query_authority_view,
            query_projection_consistency_view,
            query_projection_explanation_view,
        )
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="validate projection provenance",
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
        node_id="projection-provenance-child-001",
        node_kind="implementer",
        goal_slice="exercise projection provenance",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/projection-provenance-child-001.json",
        result_sink_ref="artifacts/projection-provenance-child-001/result.json",
        lineage_ref="root-kernel->projection-provenance-child-001",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_event_journal_projection_provenance_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="event-journal-projection-provenance",
            root_goal="validate projection provenance",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, child_node, authority=kernel_internal_authority())

        expected_journal_ref = str(event_journal_db_ref(state_root))
        expected_manifest_ref = str((state_root / "state" / "ProjectionManifest.json").resolve())

        initial = query_projection_consistency_view(state_root)
        kernel_projection = dict(initial.get("kernel_projection") or {})
        child_projection = dict(dict(initial.get("node_projections") or {}).get(child_node.node_id) or {})
        if str(kernel_projection.get("source_event_journal_ref") or "") != expected_journal_ref:
            return _fail("kernel projection must carry the authoritative journal ref even before the first manifest exists")
        if str(child_projection.get("source_event_journal_ref") or "") != expected_journal_ref:
            return _fail("node projection must carry the authoritative journal ref even before the first manifest exists")
        if str(kernel_projection.get("projection_bundle_ref") or "") != expected_manifest_ref:
            return _fail("kernel projection must point at the eventual bundle manifest path")
        if str(child_projection.get("projection_bundle_ref") or "") != expected_manifest_ref:
            return _fail("node projection must point at the eventual bundle manifest path")

        accepted = accept_control_envelope(
            state_root,
            normalize_control_envelope(
                build_child_dispatch_status(
                    child_node,
                    note="projection provenance dispatch heartbeat",
                ).to_envelope()
            ),
        )
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("accepted dispatch envelope must mirror into the journal before provenance self-heal")

        _ = query_authority_view(state_root)
        healed = query_projection_consistency_view(state_root)
        alignment = dict(healed.get("bundle_alignment") or {})
        manifest = dict(healed.get("projection_manifest") or {})
        if bool(alignment.get("kernel_bundle_aligned")) is not True or bool(alignment.get("node_bundle_aligned")) is not True:
            return _fail("trusted self-heal must align kernel and node projections to the same bundle/journal provenance")
        if str(manifest.get("source_event_journal_ref") or "") != expected_journal_ref:
            return _fail("projection manifest must point back to the authoritative journal ref")
        if str(manifest.get("projection_bundle_ref") or "") != expected_manifest_ref:
            return _fail("projection manifest must self-identify as the current projection bundle ref")

        explanation = query_projection_explanation_view(state_root, node_id=child_node.node_id)
        if str(explanation.get("effective_source_event_journal_ref") or "") != expected_journal_ref:
            return _fail("projection explanation must expose the effective journal provenance for the visible bundle")
        if str(explanation.get("effective_projection_bundle_ref") or "") != expected_manifest_ref:
            return _fail("projection explanation must expose the effective bundle ref for the visible bundle")

        child_snapshot_ref = state_root / "state" / f"{child_node.node_id}.json"
        child_payload = json.loads(child_snapshot_ref.read_text(encoding="utf-8"))
        child_payload.pop("source_event_journal_ref", None)
        child_payload.pop("projection_bundle_ref", None)
        os.chmod(child_snapshot_ref, 0o644)
        child_snapshot_ref.write_text(json.dumps(child_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        _ = query_authority_view(state_root)
        repaired = query_projection_consistency_view(state_root)
        repaired_child = dict(dict(repaired.get("node_projections") or {}).get(child_node.node_id) or {})
        if str(repaired_child.get("source_event_journal_ref") or "") != expected_journal_ref:
            return _fail("trusted self-heal must restore missing node journal provenance")
        if str(repaired_child.get("projection_bundle_ref") or "") != expected_manifest_ref:
            return _fail("trusted self-heal must restore missing node bundle provenance")

    print("[loop-system-event-journal-projection-provenance][OK] projection provenance stays explicit and self-heals when refs drift")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
