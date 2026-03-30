#!/usr/bin/env python3
"""Validate heavy-object authority-gap remediation trace visibility for Milestone 5."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-remediation-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-remediation-trace",
        root_goal="validate heavy-object authority-gap remediation trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap remediation trace validation",
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


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> Path:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import sqlite3

        from loop_product.event_journal.store import event_journal_db_ref
        from loop_product.kernel.query import query_heavy_object_authority_gap_remediation_trace_view
        from loop_product.kernel.submit import submit_heavy_object_authority_gap_remediation_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/authority-gap-remediation.pack",
            b"heavy-object-authority-gap-remediation\n",
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/authority-gap-remediation.pack",
            b"heavy-object-authority-gap-remediation\n",
        )

        accepted = submit_heavy_object_authority_gap_remediation_request(
            state_root,
            object_ref=primary_ref,
            object_kind="mathlib_pack",
            reason="route unmanaged heavy-object candidate through the canonical discovery chain",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        trace = query_heavy_object_authority_gap_remediation_trace_view(
            state_root,
            object_ref=primary_ref,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if not bool(trace.get("read_only")):
            return _fail("authority-gap remediation trace must stay read-only")
        if not bool(trace.get("remediation_request_present")):
            return _fail("complete remediation trace must report a matching remediation request")
        if not bool(trace.get("canonical_result_event_present")):
            return _fail("complete remediation trace must report the canonical remediation settled fact")
        if str(trace.get("remediation_envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("remediation trace must expose the accepted remediation envelope id")
        if str(trace.get("current_coverage_state") or "") != "MANAGED":
            return _fail("complete remediation trace must report MANAGED coverage after remediation discovery")
        authority_gap_trace = dict(trace.get("authority_gap_trace") or {})
        if str(authority_gap_trace.get("coverage_state") or "") != "MANAGED":
            return _fail("nested authority-gap trace must show MANAGED coverage after remediation discovery")
        remediation_trace = dict(trace.get("remediation_trace") or {})
        if not remediation_trace:
            return _fail("complete remediation trace must include the matching discovery trace")
        if not bool(remediation_trace.get("canonical_request_event_present")):
            return _fail("remediation trace must include the canonical heavy-object discovery request fact")
        if not bool(remediation_trace.get("canonical_observation_event_present")):
            return _fail("remediation trace must include the canonical heavy-object observation fact")
        discovery_effect = dict(remediation_trace.get("discovery_effect") or {})
        if str(discovery_effect.get("discovery_kind") or "") != "authority_gap_remediation":
            return _fail("remediation trace must preserve authority_gap_remediation as discovery_kind")
        if [str(item) for item in list(discovery_effect.get("object_refs") or [])] != [
            str(duplicate_ref.resolve()),
            str(primary_ref.resolve()),
        ]:
            return _fail("remediation trace must expose the discovered duplicate refs")
        if int(discovery_effect.get("duplicate_count") or 0) != 2:
            return _fail("remediation trace must expose the discovered duplicate count")
        if list(trace.get("gaps") or []):
            return _fail(f"complete remediation trace must not report gaps, got {trace['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_trace_missing_settled_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/authority-gap-remediation-missing-settled.pack",
            b"heavy-object-authority-gap-remediation-missing-settled\n",
        )
        _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/authority-gap-remediation-missing-settled.pack",
            b"heavy-object-authority-gap-remediation-missing-settled\n",
        )
        submit_heavy_object_authority_gap_remediation_request(
            state_root,
            object_ref=primary_ref,
            object_kind="mathlib_pack",
            reason="materialize one remediation chain before deleting the canonical settled event",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        db_ref = event_journal_db_ref(state_root)
        with sqlite3.connect(db_ref) as conn:
            conn.execute(
                "DELETE FROM committed_events WHERE event_type = ?",
                ("heavy_object_authority_gap_remediation_settled",),
            )
            conn.commit()

        degraded_trace = query_heavy_object_authority_gap_remediation_trace_view(
            state_root,
            object_ref=primary_ref,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if bool(degraded_trace.get("canonical_result_event_present")):
            return _fail("degraded remediation trace must not invent the missing canonical remediation settled fact")
        gaps = [str(item) for item in list(degraded_trace.get("gaps") or [])]
        if "missing_canonical_heavy_object_authority_gap_remediation_settled_event" not in gaps:
            return _fail("degraded remediation trace must surface missing_canonical_heavy_object_authority_gap_remediation_settled_event")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_trace_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        unmanaged_ref = _make_pack(
            repo_root,
            "workspace/publish/authority-gap-remediation-gap.pack",
            b"heavy-object-authority-gap-remediation-gap\n",
        )

        gap_trace = query_heavy_object_authority_gap_remediation_trace_view(
            state_root,
            object_ref=unmanaged_ref,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if not bool(gap_trace.get("read_only")):
            return _fail("gap remediation trace must stay read-only")
        if bool(gap_trace.get("remediation_request_present")):
            return _fail("gap remediation trace must not invent a remediation request")
        if bool(gap_trace.get("canonical_result_event_present")):
            return _fail("gap remediation trace must not invent a remediation settled event")
        if str(gap_trace.get("current_coverage_state") or "") != "UNMANAGED":
            return _fail("gap remediation trace must keep the unmanaged candidate as UNMANAGED")
        gaps = [str(item) for item in list(gap_trace.get("gaps") or [])]
        if "missing_matching_heavy_object_authority_gap_remediation_request" not in gaps:
            return _fail("gap remediation trace must surface missing_matching_heavy_object_authority_gap_remediation_request")
        if "unmanaged_heavy_object_candidate_present" not in gaps:
            return _fail("gap remediation trace must preserve unmanaged_heavy_object_candidate_present")

    print("[loop-system-heavy-object-authority-gap-remediation-trace][OK] heavy-object authority-gap remediation trace stays read-only, requires a canonical remediation settled fact, and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
