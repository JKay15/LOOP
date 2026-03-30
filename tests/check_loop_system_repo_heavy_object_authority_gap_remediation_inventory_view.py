#!/usr/bin/env python3
"""Validate read-only heavy-object authority-gap remediation inventory visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-remediation-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-remediation-inventory-view",
        root_goal="validate heavy-object authority-gap remediation inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap remediation inventory validation",
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


def _entry(entries: list[dict[str, object]], object_ref: Path) -> dict[str, object]:
    target = str(object_ref.resolve())
    for item in entries:
        if str(item.get("object_ref") or "") == target:
            return dict(item)
    return {}


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import sqlite3

        from loop_product.event_journal.store import event_journal_db_ref
        from loop_product.kernel.query import query_heavy_object_authority_gap_remediation_inventory_view
        from loop_product.kernel.submit import (
            submit_heavy_object_authority_gap_remediation_request,
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_inventory_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        managed_id, managed_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/remediation-inventory-managed.pack",
            b"heavy-object-authority-gap-remediation-inventory-managed\n",
        )
        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/remediation-inventory-primary.pack",
            b"heavy-object-authority-gap-remediation-inventory-remediated\n",
        )[1]
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/remediation-inventory-primary.pack",
            b"heavy-object-authority-gap-remediation-inventory-remediated\n",
        )[1]
        unmanaged_ref = _make_pack(
            repo_root,
            "workspace/publish/remediation-inventory-unmanaged.pack",
            b"heavy-object-authority-gap-remediation-inventory-unmanaged\n",
        )[1]

        submit_heavy_object_registration_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            byte_size=managed_ref.stat().st_size,
            reason="register managed remediation inventory candidate",
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
            reason="discover managed remediation inventory candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        remediated = submit_heavy_object_authority_gap_remediation_request(
            state_root,
            object_ref=primary_ref,
            object_kind="mathlib_pack",
            reason="route unmanaged heavy-object candidate through the canonical discovery chain",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        inventory = query_heavy_object_authority_gap_remediation_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("remediation inventory must stay read-only")
        if int(inventory.get("filesystem_candidate_count") or 0) != 4:
            return _fail("remediation inventory must report all four candidate files")
        if int(inventory.get("managed_candidate_count") or 0) != 3:
            return _fail("remediation inventory must report three managed candidates")
        if int(inventory.get("unmanaged_candidate_count") or 0) != 1:
            return _fail("remediation inventory must report one unmanaged candidate")
        if int(inventory.get("remediation_requested_candidate_count") or 0) != 1:
            return _fail("remediation inventory must report one candidate with a remediation request")
        if int(inventory.get("remediated_candidate_count") or 0) != 1:
            return _fail("remediation inventory must report one remediated candidate")
        if int(inventory.get("remediation_result_candidate_count") or 0) != 1:
            return _fail("remediation inventory must report one candidate with a canonical remediation settled fact")
        if int(inventory.get("pending_remediation_candidate_count") or 0) != 0:
            return _fail("complete remediation inventory must not report pending remediation candidates")
        if int(inventory.get("unrequested_unmanaged_candidate_count") or 0) != 1:
            return _fail("remediation inventory must report one unmanaged candidate without remediation")

        entries = list(inventory.get("candidates") or [])
        primary_entry = _entry(entries, primary_ref)
        if str(primary_entry.get("remediation_state") or "") != "REMEDIATED":
            return _fail("primary candidate must report remediation_state=REMEDIATED")
        if not bool(primary_entry.get("remediation_result_event_present")):
            return _fail("primary candidate must expose the canonical remediation settled fact")
        if str(primary_entry.get("remediation_envelope_id") or "") != str(remediated.envelope_id or ""):
            return _fail("primary candidate must expose the accepted remediation envelope id")

        duplicate_entry = _entry(entries, duplicate_ref)
        if str(duplicate_entry.get("coverage_state") or "") != "MANAGED":
            return _fail("duplicate candidate must report managed coverage after remediation discovery")
        if str(duplicate_entry.get("remediation_state") or "") != "NOT_REQUIRED":
            return _fail("duplicate candidate must remain NOT_REQUIRED because it was covered by discovery, not directly requested")

        managed_entry = _entry(entries, managed_ref)
        if str(managed_entry.get("remediation_state") or "") != "NOT_REQUIRED":
            return _fail("already managed candidate must report remediation_state=NOT_REQUIRED")

        unmanaged_entry = _entry(entries, unmanaged_ref)
        if str(unmanaged_entry.get("coverage_state") or "") != "UNMANAGED":
            return _fail("unmanaged candidate must stay UNMANAGED")
        if str(unmanaged_entry.get("remediation_state") or "") != "UNREQUESTED":
            return _fail("unmanaged candidate without remediation must report remediation_state=UNREQUESTED")

        gaps = [str(item) for item in list(inventory.get("gaps") or [])]
        if "unmanaged_heavy_object_candidates_without_remediation_request_present" not in gaps:
            return _fail("inventory must surface unmanaged_heavy_object_candidates_without_remediation_request_present")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_inventory_missing_settled_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/remediation-inventory-missing-settled.pack",
            b"heavy-object-authority-gap-remediation-inventory-missing-settled\n",
        )[1]
        _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/remediation-inventory-missing-settled.pack",
            b"heavy-object-authority-gap-remediation-inventory-missing-settled\n",
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

        degraded_inventory = query_heavy_object_authority_gap_remediation_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(degraded_inventory.get("remediated_candidate_count") or 0) != 0:
            return _fail("inventory must not treat a candidate as remediated when the canonical remediation settled fact is missing")
        if int(degraded_inventory.get("pending_remediation_candidate_count") or 0) != 1:
            return _fail("inventory must keep the candidate pending when the canonical remediation settled fact is missing")
        degraded_entry = _entry(list(degraded_inventory.get("candidates") or []), primary_ref)
        if str(degraded_entry.get("remediation_state") or "") != "REQUESTED":
            return _fail("candidate must fall back to remediation_state=REQUESTED when the canonical remediation settled fact is missing")
        if bool(degraded_entry.get("remediation_result_event_present")):
            return _fail("candidate must not invent a remediation settled event after it was deleted")
        gaps = [str(item) for item in list(degraded_inventory.get("gaps") or [])]
        if "missing_canonical_heavy_object_authority_gap_remediation_settled_event" not in gaps:
            return _fail("inventory must surface missing_canonical_heavy_object_authority_gap_remediation_settled_event when settlement is missing")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_inventory_empty_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        empty_inventory = query_heavy_object_authority_gap_remediation_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        gaps = [str(item) for item in list(empty_inventory.get("gaps") or [])]
        if "missing_matching_filesystem_heavy_object_candidate" not in gaps:
            return _fail("empty remediation inventory must surface missing_matching_filesystem_heavy_object_candidate")

    print("[loop-system-heavy-object-authority-gap-remediation-inventory-view][OK] heavy-object authority-gap remediation inventory stays read-only and distinguishes requested vs remediated candidates via the canonical remediation settled fact")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
