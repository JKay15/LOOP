#!/usr/bin/env python3
"""Validate read-only heavy-object retention visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-retention-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-retention-view",
        root_goal="validate read-only heavy-object retention visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object retention validation",
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


def _make_object(repo_root: Path, rel: str, payload: bytes) -> tuple[str, Path]:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}", path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, mirror_accepted_control_envelope_event
        from loop_product.gateway.accept import accept_control_envelope
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.query import query_heavy_object_retention_view
        from loop_product.kernel.submit import (
            submit_heavy_object_gc_eligibility_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_reclamation_request,
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_retention_view_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old.pack",
            b"heavy-object-retention-old-pack\n",
        )
        old_byte_size = old_ref.stat().st_size
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new.pack",
            b"heavy-object-retention-new-pack\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            byte_size=old_byte_size,
            reason="register old object before retention view exists",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            byte_size=new_ref.stat().st_size,
            reason="register replacement object before retention view exists",
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=new_ref,
            pin_holder_id="publish-staging",
            pin_kind="publish_pin",
            reason="pin the replacement object so it stays retained",
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
            reason="replacement pack supersedes the old retained pack",
        )
        submit_heavy_object_gc_eligibility_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            eligibility_kind="superseded_reclamation_candidate",
            reason="old pack becomes gc eligible",
            superseded_by_object_id=new_id,
            superseded_by_object_ref=new_ref,
        )
        accepted = submit_heavy_object_reclamation_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=old_ref,
            reclamation_kind="authoritative_gc_reclaim",
            reason="reclaim the superseded old pack",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("complete retention scenario requires an accepted heavy-object reclamation request")
        if old_ref.exists():
            return _fail("authoritative heavy-object reclamation setup must actually reclaim the old bytes")

        event_count_before = committed_event_count(state_root)
        old_view = query_heavy_object_retention_view(state_root, object_id=old_id)
        new_view = query_heavy_object_retention_view(state_root, object_id=new_id)
        event_count_after = committed_event_count(state_root)
        if event_count_before != event_count_after:
            return _fail("heavy-object retention view must stay read-only and not mutate committed event history")

        if not bool(old_view.get("read_only")) or not bool(new_view.get("read_only")):
            return _fail("heavy-object retention view must stay read-only")
        if str(old_view.get("retention_state") or "") != "RECLAIMED":
            return _fail("superseded and reclaimed object must report retention_state=RECLAIMED")
        if bool(old_view.get("reclaimable")):
            return _fail("already reclaimed object must not remain reclaimable")
        if not bool(old_view.get("registered_present")):
            return _fail("old object retention view must expose matching registration presence")
        if not bool(old_view.get("superseded_present")):
            return _fail("old object retention view must expose supersession presence")
        if not bool(old_view.get("gc_eligibility_present")):
            return _fail("old object retention view must expose gc-eligibility presence")
        if not bool(old_view.get("reclaimed_present")):
            return _fail("old object retention view must expose reclaimed presence")
        if int(old_view.get("active_pin_count") or 0) != 0:
            return _fail("reclaimed old object must not report active pins")
        latest_superseded_by = dict(old_view.get("latest_superseded_by") or {})
        if str(latest_superseded_by.get("replacement_object_id") or "") != new_id:
            return _fail("old object retention view must expose the replacement object anchor")
        if str(new_view.get("retention_state") or "") != "PINNED":
            return _fail("replacement object with an active pin must report retention_state=PINNED")
        if bool(new_view.get("reclaimable")):
            return _fail("pinned replacement object must not be reclaimable")
        if int(new_view.get("active_pin_count") or 0) != 1:
            return _fail("replacement object must expose one active pin")
        replacement_for = [str(item) for item in list(new_view.get("replacement_for_object_ids") or [])]
        if replacement_for != [old_id]:
            return _fail("replacement object must expose which old object it supersedes")
        latest_reclaimed = dict(old_view.get("latest_reclaimed") or {})
        if int(latest_reclaimed.get("byte_size_before") or 0) != old_byte_size:
            return _fail("reclaimed retention view must preserve byte_size_before from the canonical reclaimed event")
        if new_view.get("gaps"):
            return _fail(f"complete replacement retention view must not report gaps, got {new_view['gaps']!r}")
        if old_view.get("gaps"):
            return _fail(f"complete reclaimed retention view must not report gaps, got {old_view['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_retention_view_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)
        object_id, object_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-gap.pack",
            b"heavy-object-retention-gap-pack\n",
        )

        envelope = ControlEnvelope(
            source="root-kernel",
            envelope_type="heavy_object_registration_request",
            payload_type="local_control_decision",
            round_id="R0",
            generation=0,
            payload={
                "object_id": object_id,
                "object_kind": "mathlib_pack",
                "object_ref": str(object_ref.resolve()),
                "byte_size": object_ref.stat().st_size,
                "registration_kind": "heavy_object_registration",
            },
            status=EnvelopeStatus.PROPOSAL,
            note="accepted heavy-object registration missing canonical registration fact",
        )
        accepted = accept_control_envelope(state_root, normalize_control_envelope(envelope))
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("gap retention scenario requires an accepted heavy-object registration envelope")
        if not mirror_accepted_control_envelope_event(state_root, accepted):
            return _fail("gap retention scenario requires a compatibility mirror event")

        gap_view = query_heavy_object_retention_view(state_root, object_id=object_id, object_ref=object_ref)
        gaps = [str(item) for item in list(gap_view.get("gaps") or [])]
        if bool(gap_view.get("registered_present")):
            return _fail("gap retention view must not invent a canonical heavy-object registration fact")
        if str(gap_view.get("retention_state") or "") != "UNREGISTERED":
            return _fail("gap retention view without canonical registration must report retention_state=UNREGISTERED")
        if "missing_canonical_heavy_object_registered_event" not in gaps:
            return _fail("gap retention view must surface missing_canonical_heavy_object_registered_event")
        if bool(gap_view.get("reclaimable")):
            return _fail("gap retention view without canonical lifecycle must not report reclaimable=true")

    print("[loop-system-heavy-object-retention-view][OK] heavy-object retention view stays read-only and explains current committed lifecycle state")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
