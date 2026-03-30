#!/usr/bin/env python3
"""Validate read-only heavy-object authority-gap inventory visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _expected_canonical_store_ref(repo_root: Path, *, object_kind: str, object_id: str, is_dir: bool, suffix: str = "") -> Path:
    digest = str(object_id or "").removeprefix("sha256:").strip()
    leaf = "tree" if is_dir else f"object{suffix}"
    return (repo_root / ".loop" / "heavy_objects" / object_kind / digest / leaf).resolve()


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-inventory-view",
        root_goal="validate read-only heavy-object authority-gap inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap inventory validation",
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
        from loop_product.kernel.query import query_heavy_object_authority_gap_inventory_view
        from loop_product.kernel.submit import (
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_inventory_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        managed_id, managed_candidate_ref = _make_pack(
            repo_root,
            "workspace/publish/managed.pack",
            b"heavy-object-authority-gap-managed\n",
        )
        managed_ref = _expected_canonical_store_ref(
            repo_root,
            object_kind="mathlib_pack",
            object_id=managed_id,
            is_dir=False,
            suffix=".pack",
        )
        managed_ref.parent.mkdir(parents=True, exist_ok=True)
        managed_ref.write_bytes(managed_candidate_ref.read_bytes())
        _gap_a_id, gap_a_ref = _make_pack(
            repo_root,
            "workspace/publish/gap-a.pack",
            b"heavy-object-authority-gap-a\n",
        )
        _gap_b_id, gap_b_ref = _make_pack(
            repo_root,
            "workspace/publish/gap-b.pack",
            b"heavy-object-authority-gap-b\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            byte_size=managed_ref.stat().st_size,
            reason="register managed pack",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_candidate_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover managed pack",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("authority-gap inventory must stay read-only")
        if int(inventory.get("filesystem_candidate_count") or 0) != 3:
            return _fail("authority-gap inventory must count only repo candidates, not canonical store entries")
        if int(inventory.get("managed_candidate_count") or 0) != 1:
            return _fail("authority-gap inventory must preserve managed candidate coverage")
        if int(inventory.get("unmanaged_candidate_count") or 0) != 2:
            return _fail("authority-gap inventory must expose unmanaged filesystem candidates")

        managed_candidates = list(inventory.get("managed_candidates") or [])
        unmanaged_candidates = list(inventory.get("unmanaged_candidates") or [])
        if len(managed_candidates) != 1 or len(unmanaged_candidates) != 2:
            return _fail("authority-gap inventory must split managed and unmanaged candidates")

        managed_summary = dict(managed_candidates[0] or {})
        if str(managed_summary.get("object_ref") or "") != str(managed_candidate_ref.resolve()):
            return _fail("managed candidate must preserve the discovered candidate ref")
        if not bool(managed_summary.get("registered_present")):
            return _fail("managed candidate must report committed registration coverage through object-id-first matching")
        if not bool(managed_summary.get("discovery_present")):
            return _fail("managed candidate must report committed discovery coverage")
        latest_registration = dict(managed_summary.get("latest_registration") or {})
        if str(dict(latest_registration.get("payload") or {}).get("object_ref") or "") != str(managed_ref.resolve()):
            return _fail("managed candidate must surface the canonical registration ref while keeping the candidate ref visible")

        unmanaged_refs = {str(item.get("object_ref") or "") for item in unmanaged_candidates}
        if unmanaged_refs != {str(gap_a_ref.resolve()), str(gap_b_ref.resolve())}:
            return _fail("authority-gap inventory must expose both unmanaged candidate refs")
        for summary in unmanaged_candidates:
            blob = dict(summary or {})
            if bool(blob.get("registered_present")) or bool(blob.get("discovery_present")):
                return _fail("unmanaged candidates must not claim committed lifecycle coverage")
            if str(blob.get("coverage_state") or "") != "UNMANAGED":
                return _fail("unmanaged candidates must report coverage_state=UNMANAGED")

        gaps = [str(item) for item in list(inventory.get("gaps") or [])]
        if "unmanaged_heavy_object_candidates_present" not in gaps:
            return _fail("authority-gap inventory must surface unmanaged_heavy_object_candidates_present when unmanaged candidates remain")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_inventory_missing_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        gap_inventory = query_heavy_object_authority_gap_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("filesystem_candidate_count") or 0) != 0:
            return _fail("missing-candidate authority-gap inventory must not invent filesystem candidates")
        gaps = [str(item) for item in list(gap_inventory.get("gaps") or [])]
        if "missing_matching_filesystem_heavy_object_candidate" not in gaps:
            return _fail("authority-gap inventory must surface missing_matching_filesystem_heavy_object_candidate when no pack candidates exist")

    print("[loop-system-heavy-object-authority-gap-inventory-view][OK] heavy-object authority-gap inventory stays read-only and exposes unmanaged filesystem candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
