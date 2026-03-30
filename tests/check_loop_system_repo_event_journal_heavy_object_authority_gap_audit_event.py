#!/usr/bin/env python3
"""Validate canonical accepted-event and receipt behavior for heavy-object authority-gap audits."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-audit-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-audit-event",
        root_goal="validate heavy-object authority-gap audit command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap audit event validation",
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


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> tuple[str, Path]:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    return f"sha256:{digest}", path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view
        from loop_product.kernel.submit import (
            submit_control_envelope,
            submit_heavy_object_authority_gap_audit_request,
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import heavy_object_authority_gap_audit_receipt_ref
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_audit_event_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_audit_event_runtime"
        _persist_anchor_state(state_root)

        managed_id, managed_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/audit-managed.pack",
            b"heavy-object-authority-gap-audit-managed\n",
        )
        _unused_id, unmanaged_a = _make_pack(
            repo_root,
            "workspace/publish/audit-unmanaged-a.pack",
            b"heavy-object-authority-gap-audit-unmanaged-a\n",
        )
        _unused_id_b, unmanaged_b = _make_pack(
            repo_root,
            "workspace/publish/audit-unmanaged-b.pack",
            b"heavy-object-authority-gap-audit-unmanaged-b\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            byte_size=managed_ref.stat().st_size,
            reason="register managed audit candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover managed audit candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        accepted = submit_heavy_object_authority_gap_audit_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="record one accepted heavy-object authority-gap audit command",
            runtime_name="publish-runtime",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object authority-gap audit request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object authority-gap audit request must preserve heavy_object classification")

        event_count_before_replay = committed_event_count(state_root)
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object authority-gap audit envelope must remain accepted")
        if committed_event_count(state_root) != event_count_before_replay:
            return _fail("replayed accepted heavy-object authority-gap audit envelope must not duplicate committed events")

        receipt_ref = heavy_object_authority_gap_audit_receipt_ref(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        if not receipt_ref.exists():
            return _fail("accepted heavy-object authority-gap audit command must materialize a runtime receipt")
        receipt = json.loads(receipt_ref.read_text(encoding="utf-8"))
        if str(receipt.get("schema") or "") != "loop_product.heavy_object_authority_gap_audit_receipt":
            return _fail("receipt schema must identify heavy-object authority-gap audits")
        if int(receipt.get("candidate_count") or 0) != 3:
            return _fail("audit receipt must report all three heavy-object candidates")
        if int(receipt.get("managed_candidate_count") or 0) != 1:
            return _fail("audit receipt must report one managed candidate")
        if int(receipt.get("unmanaged_candidate_count") or 0) != 2:
            return _fail("audit receipt must report two unmanaged candidates")
        if int(receipt.get("registered_candidate_count") or 0) != 1:
            return _fail("audit receipt must report one registered candidate")
        if int(receipt.get("discovery_covered_candidate_count") or 0) != 1:
            return _fail("audit receipt must report one discovery-covered candidate")
        managed_candidates = list(receipt.get("managed_candidates") or [])
        unmanaged_candidates = list(receipt.get("unmanaged_candidates") or [])
        if len(managed_candidates) != 1 or len(unmanaged_candidates) != 2:
            return _fail("audit receipt must expose managed and unmanaged candidate summaries")
        unmanaged_refs = {str(dict(item).get("object_ref") or "") for item in unmanaged_candidates}
        if unmanaged_refs != {str(unmanaged_a.resolve()), str(unmanaged_b.resolve())}:
            return _fail("audit receipt must preserve normalized unmanaged candidate refs")

        events = list(iter_committed_events(state_root))
        event_types = [str(event.get("event_type") or "") for event in events]
        if event_types.count("heavy_object_authority_gap_audit_requested") != 1:
            return _fail("accepted heavy-object authority-gap audit command must emit exactly one canonical heavy_object_authority_gap_audit_requested fact")
        canonical = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_authority_gap_audit_requested"),
            {},
        )
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_authority_gap_audit:{accepted.envelope_id}":
            return _fail("canonical heavy-object authority-gap audit event must use heavy_object_authority_gap_audit:<envelope_id> as command_id")
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object authority-gap audit event must preserve the accepted envelope identity")
        if str(payload.get("repo_root") or "") != str(repo_root.resolve()):
            return _fail("canonical heavy-object authority-gap audit event must preserve repo_root")
        if str(payload.get("object_kind") or "") != "mathlib_pack":
            return _fail("canonical heavy-object authority-gap audit event must preserve object_kind")
        if str(payload.get("runtime_name") or "") != "publish-runtime":
            return _fail("canonical heavy-object authority-gap audit event must preserve runtime_name")

        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(inventory.get("unmanaged_candidate_count") or 0) != 2:
            return _fail("audit command must not mutate authority-gap inventory by itself")

    print("[loop-system-heavy-object-authority-gap-audit-event][OK] accepted heavy-object authority-gap audit commands emit one canonical event and one deterministic receipt without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
