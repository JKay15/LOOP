#!/usr/bin/env python3
"""Validate read-only heavy-object supersession trace visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-supersession-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-supersession-trace",
        root_goal="validate read-only heavy-object supersession trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object supersession trace validation",
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
        from loop_product.kernel.query import query_heavy_object_supersession_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_registration_request,
            submit_heavy_object_supersession_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_supersession_trace_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        old_id, old_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-old.pack",
            b"heavy-object-old-trace\n",
        )
        new_id, new_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/pack-new.pack",
            b"heavy-object-new-trace\n",
        )

        for object_id, object_ref in ((old_id, old_ref), (new_id, new_ref)):
            submit_heavy_object_registration_request(
                state_root,
                object_id=object_id,
                object_kind="mathlib_pack",
                object_ref=object_ref,
                byte_size=object_ref.stat().st_size,
                reason="register heavy objects before supersession trace exists",
            )

        accepted = submit_heavy_object_supersession_request(
            state_root,
            superseded_object_id=old_id,
            superseded_object_kind="mathlib_pack",
            superseded_object_ref=old_ref,
            replacement_object_id=new_id,
            replacement_object_kind="mathlib_pack",
            replacement_object_ref=new_ref,
            supersession_kind="publish_bundle_replacement",
            reason="replacement pack supersedes prior retained pack",
            trigger_kind="heavy_object_reference_replaced",
            trigger_ref="manual-reference-event",
        )

        trace = query_heavy_object_supersession_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if not bool(trace.get("read_only")):
            return _fail("heavy-object supersession trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("heavy-object supersession trace must expose accepted audit presence")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("heavy-object supersession trace must expose compatibility mirror presence")
        if not bool(trace.get("canonical_event_present")):
            return _fail("heavy-object supersession trace must expose canonical supersession fact presence")
        if not bool(trace.get("superseded_registration_present")):
            return _fail("heavy-object supersession trace must expose the superseded object registration")
        if not bool(trace.get("replacement_registration_present")):
            return _fail("heavy-object supersession trace must expose the replacement object registration")
        summary = dict(trace.get("supersession") or {})
        if str(summary.get("superseded_object_id") or "") != old_id:
            return _fail("heavy-object supersession trace must expose the superseded object id")
        if str(summary.get("replacement_object_id") or "") != new_id:
            return _fail("heavy-object supersession trace must expose the replacement object id")
        if str(summary.get("supersession_kind") or "") != "publish_bundle_replacement":
            return _fail("heavy-object supersession trace must expose the supersession kind")
        if str(summary.get("trigger_kind") or "") != "heavy_object_reference_replaced":
            return _fail("heavy-object supersession trace must expose trigger_kind")
        if str(summary.get("trigger_ref") or "") != "manual-reference-event":
            return _fail("heavy-object supersession trace must expose trigger_ref")
        if trace.get("gaps"):
            return _fail(f"complete heavy-object supersession trace must not report gaps, got {trace['gaps']!r}")

        missing = query_heavy_object_supersession_trace_view(state_root, envelope_id="missing-envelope")
        gaps = set(str(item) for item in list(missing.get("gaps") or []))
        expected = {
            "missing_accepted_audit_envelope",
            "missing_compatibility_envelope_mirror",
            "missing_canonical_heavy_object_superseded_event",
            "missing_superseded_heavy_object_registration",
            "missing_replacement_heavy_object_registration",
        }
        if not expected.issubset(gaps):
            return _fail(f"gapped heavy-object supersession trace must report the expected missing facts, got {sorted(gaps)!r}")

    print("[loop-system-heavy-object-supersession-trace][OK] heavy-object supersession trace stays read-only and exposes both complete and gapped causal histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
