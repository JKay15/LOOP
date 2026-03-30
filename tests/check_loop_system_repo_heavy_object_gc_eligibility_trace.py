#!/usr/bin/env python3
"""Validate read-only heavy-object GC-eligibility trace visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-gc-eligibility-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-gc-eligibility-trace",
        root_goal="validate read-only heavy-object gc-eligibility trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object gc-eligibility trace validation",
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


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.query import query_heavy_object_gc_eligibility_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_gc_eligibility_trace_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-old.pack"
        old_bytes = b"heavy-object-gc-eligibility-trace-old\n"
        old_ref.parent.mkdir(parents=True, exist_ok=True)
        old_ref.write_bytes(old_bytes)
        old_id = f"sha256:{hashlib.sha256(old_bytes).hexdigest()}"

        new_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-new.pack"
        new_bytes = b"heavy-object-gc-eligibility-trace-new\n"
        new_ref.write_bytes(new_bytes)
        new_id = f"sha256:{hashlib.sha256(new_bytes).hexdigest()}"

        old_registered = submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_ref.stat().st_size,
            reason="register the old heavy object before gc-eligibility trace exists",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register the replacement heavy object before gc-eligibility trace exists",
        )
        submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="replacement_pack",
            reason="replacement pack supersedes the old retained pack before gc eligibility",
            trigger_kind="heavy_object_reference_replaced",
            trigger_ref="manual-reference-event",
        )
        accepted = submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old pack is superseded and may now enter authoritative gc-eligibility state",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
            trigger_kind="heavy_object_auto_supersession",
            trigger_ref="manual-supersession-event",
        )

        trace = query_heavy_object_gc_eligibility_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if not bool(trace.get("read_only")):
            return _fail("heavy-object gc-eligibility trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("heavy-object gc-eligibility trace must expose accepted audit presence")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("heavy-object gc-eligibility trace must expose compatibility mirror presence")
        if not bool(trace.get("canonical_event_present")):
            return _fail("heavy-object gc-eligibility trace must expose canonical gc-eligibility fact presence")
        if not bool(trace.get("matching_registration_present")):
            return _fail("heavy-object gc-eligibility trace must expose matching registration presence")
        if not bool(trace.get("supporting_supersession_present")):
            return _fail("heavy-object gc-eligibility trace must expose supporting supersession presence")
        eligible_object = dict(trace.get("eligible_object") or {})
        if str(eligible_object.get("object_id") or "") != old_id:
            return _fail("heavy-object gc-eligibility trace must expose the eligible object id")
        if str(eligible_object.get("eligibility_kind") or "") != "superseded_reclamation_candidate":
            return _fail("heavy-object gc-eligibility trace must expose the eligibility kind")
        if str(eligible_object.get("superseded_by_object_id") or "") != new_id:
            return _fail("heavy-object gc-eligibility trace must expose the replacement object id")
        if str(eligible_object.get("trigger_kind") or "") != "heavy_object_auto_supersession":
            return _fail("heavy-object gc-eligibility trace must expose trigger_kind")
        if str(eligible_object.get("trigger_ref") or "") != "manual-supersession-event":
            return _fail("heavy-object gc-eligibility trace must expose trigger_ref")
        registration = dict(trace.get("latest_matching_registration") or {})
        if str(registration.get("event_type") or "") != "heavy_object_registered":
            return _fail("heavy-object gc-eligibility trace must surface the latest matching registration event summary")
        supersession = dict(trace.get("latest_supporting_supersession") or {})
        if str(supersession.get("event_type") or "") != "heavy_object_superseded":
            return _fail("heavy-object gc-eligibility trace must surface the latest supporting supersession event summary")
        if trace.get("gaps"):
            return _fail(f"complete heavy-object gc-eligibility trace must not report gaps, got {trace['gaps']!r}")

        missing = query_heavy_object_gc_eligibility_trace_view(state_root, envelope_id=str(old_registered.envelope_id or ""))
        gaps = set(str(item) for item in list(missing.get("gaps") or []))
        if "missing_canonical_heavy_object_gc_eligible_event" not in gaps:
            return _fail("gapped heavy-object gc-eligibility trace must report missing canonical eligibility fact")
        if bool(missing.get("canonical_event_present")):
            return _fail("gapped heavy-object gc-eligibility trace must not pretend a canonical eligibility fact exists")

    print("[loop-system-heavy-object-gc-eligibility-trace][OK] heavy-object gc-eligibility trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
