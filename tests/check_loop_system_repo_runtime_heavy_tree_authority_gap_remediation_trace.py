#!/usr/bin/env python3
"""Validate runtime-heavy-tree remediation on the accepted discovery spine."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-heavy-tree-authority-gap-remediation-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-runtime-heavy-tree-authority-gap-remediation-trace",
        root_goal="validate runtime-heavy-tree remediation on the accepted discovery spine",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise runtime-heavy-tree remediation trace validation",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"thinking_budget": "medium", "role": "kernel"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/kernel.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    kernel_state.register_node(root_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())


def _write_runtime_heavy_tree(artifact_root: Path, *, label: str) -> Path:
    tree_root = artifact_root / ".lake"
    (tree_root / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
    (tree_root / "packages" / "mathlib" / "STATUS.txt").write_text(f"{label}: ready\n", encoding="utf-8")
    return tree_root.resolve()


def _committed_events_by_type(state_root: Path, event_type: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    return [
        dict(event)
        for event in iter_committed_events(state_root)
        if str(event.get("event_type") or "") == str(event_type)
    ]


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.query import (
            query_heavy_object_authority_gap_remediation_trace_view,
            query_heavy_object_reference_inventory_view,
            query_heavy_object_reference_trace_view,
        )
        from loop_product.kernel.submit import submit_heavy_object_authority_gap_remediation_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_runtime_heavy_tree_authority_gap_remediation_trace_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _write_runtime_heavy_tree(
            repo_root / "workspace" / "runtime-heavy-remediation" / "deliverables" / "primary_artifact",
            label="shared-runtime-heavy-tree",
        )
        staging_ref = _write_runtime_heavy_tree(
            repo_root
            / "workspace"
            / "runtime-heavy-remediation"
            / "deliverables"
            / ".primary_artifact.publish.remtrace"
            / "primary_artifact",
            label="shared-runtime-heavy-tree",
        )

        accepted = submit_heavy_object_authority_gap_remediation_request(
            state_root,
            object_ref=primary_ref,
            object_kind="runtime_heavy_tree",
            reason="authoritatively remediate one unmanaged runtime heavy tree",
            runtime_name="heavy-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("runtime-heavy-tree authority-gap remediation request must be accepted")

        trace = query_heavy_object_authority_gap_remediation_trace_view(
            state_root,
            object_ref=primary_ref,
            runtime_name="heavy-runtime",
            object_kind="runtime_heavy_tree",
            repo_root=repo_root,
        )
        if not bool(trace.get("read_only")):
            return _fail("runtime-heavy-tree remediation trace must stay read-only")
        if not bool(trace.get("remediation_request_present")):
            return _fail("runtime-heavy-tree remediation trace must expose the accepted remediation request")
        if not bool(trace.get("canonical_result_event_present")):
            return _fail("runtime-heavy-tree remediation trace must expose the canonical remediation result event")
        if str(trace.get("current_coverage_state") or "") != "MANAGED":
            return _fail("runtime-heavy-tree remediation trace must converge to coverage_state=MANAGED")
        if trace.get("gaps"):
            return _fail(f"runtime-heavy-tree remediation trace must not report gaps after convergence, got {trace.get('gaps')!r}")

        registrations = [
            dict(event)
            for event in _committed_events_by_type(state_root, "heavy_object_registered")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
        ]
        references = [
            dict(event)
            for event in _committed_events_by_type(state_root, "heavy_object_reference_attached")
            if str(dict(event.get("payload") or {}).get("object_kind") or "") == "runtime_heavy_tree"
        ]
        if len(registrations) != 1:
            return _fail("runtime-heavy-tree remediation must converge through exactly one canonical registration")
        if len(references) != 2:
            return _fail("runtime-heavy-tree remediation must converge through exactly two canonical references")

        object_id = str(dict(registrations[0].get("payload") or {}).get("object_id") or "")
        inventory = query_heavy_object_reference_inventory_view(
            state_root,
            object_id=object_id,
        )
        if int(inventory.get("active_reference_count") or 0) != 2:
            return _fail("runtime-heavy-tree reference inventory must report both active heavy-tree references for the canonical object identity")
        if {
            str(item)
            for item in list(inventory.get("active_reference_holder_kinds") or [])
        } != {"workspace_artifact_root", "workspace_publication_staging_root"}:
            return _fail("runtime-heavy-tree reference inventory must preserve both holder kinds")

        reference_payloads = [dict(event.get("payload") or {}) for event in references]
        if {
            str(payload.get("reference_ref") or "")
            for payload in reference_payloads
        } != {str(primary_ref), str(staging_ref)}:
            return _fail("runtime-heavy-tree remediation must preserve both discovered heavy-tree refs")

        reference_trace = query_heavy_object_reference_trace_view(
            state_root,
            envelope_id=str(reference_payloads[0].get("envelope_id") or ""),
        )
        referenced_object = dict(reference_trace.get("referenced_object") or {})
        if str(referenced_object.get("object_kind") or "") != "runtime_heavy_tree":
            return _fail("runtime-heavy-tree reference trace must preserve object_kind=runtime_heavy_tree")
        if str(referenced_object.get("reference_kind") or "") != "authority_gap_auto_reference":
            return _fail("runtime-heavy-tree reference trace must preserve authority_gap_auto_reference kind")
        if str(referenced_object.get("trigger_kind") or "") != "heavy_object_authority_gap_remediation":
            return _fail("runtime-heavy-tree reference trace must preserve the remediation trigger kind")

    print(
        "[loop-system-runtime-heavy-tree-authority-gap-remediation-trace][OK] "
        "runtime-heavy-tree remediation converges through accepted discovery, canonical registration, and canonical references"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
