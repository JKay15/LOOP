#!/usr/bin/env python3
"""Validate read-only heavy-object discovery inventory visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-discovery-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-discovery-inventory-view",
        root_goal="validate read-only heavy-object discovery inventory visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object discovery inventory validation",
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
        from loop_product.kernel.query import query_heavy_object_discovery_inventory_view
        from loop_product.kernel.submit import (
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_discovery_inventory_view_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        share_payload = b"heavy-object-discovery-inventory-share\n"
        share_id, share_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/discovery-share.pack",
            share_payload,
        )
        _share_dup_id, share_dup_ref = _make_object(
            repo_root,
            "workspace/publish/shareable/discovery-share.pack",
            share_payload,
        )

        solo_payload = b"heavy-object-discovery-inventory-solo\n"
        solo_id, solo_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/discovery-solo.pack",
            solo_payload,
        )

        other_payload = b"heavy-object-discovery-inventory-other\n"
        other_id, other_ref = _make_object(
            repo_root,
            "artifacts/heavy_objects/mathlib/discovery-other.pack",
            other_payload,
        )
        _other_dup_id, other_dup_ref = _make_object(
            repo_root,
            "workspace/publish/other/discovery-other.pack",
            other_payload,
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            byte_size=share_ref.stat().st_size,
            reason="register shareable discovery object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=solo_id,
            object_kind="mathlib_pack",
            object_ref=solo_ref,
            byte_size=solo_ref.stat().st_size,
            reason="register single-ref discovery object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=other_id,
            object_kind="mathlib_pack",
            object_ref=other_ref,
            byte_size=other_ref.stat().st_size,
            reason="register other-runtime discovery object",
            runtime_name="other-runtime",
            repo_root=repo_root,
        )

        submit_heavy_object_discovery_request(
            state_root,
            object_id=share_id,
            object_kind="mathlib_pack",
            object_ref=share_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate refs for the shareable object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=solo_id,
            object_kind="mathlib_pack",
            object_ref=solo_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover only the primary ref for the solo object",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        submit_heavy_object_discovery_request(
            state_root,
            object_id=other_id,
            object_kind="mathlib_pack",
            object_ref=other_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate refs for a different runtime",
            runtime_name="other-runtime",
            repo_root=repo_root,
        )

        inventory = query_heavy_object_discovery_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            discovery_kind="matching_filename_sha256_scan",
            repo_root=repo_root,
        )
        if not bool(inventory.get("read_only")):
            return _fail("heavy-object discovery inventory must stay read-only")
        if int(inventory.get("matching_discovery_observation_count") or 0) != 2:
            return _fail("discovery inventory must count matching committed discovery observations")
        if int(inventory.get("discovered_object_count") or 0) != 2:
            return _fail("discovery inventory must count unique discovered heavy-object identities")
        if int(inventory.get("shareable_object_count") or 0) != 1:
            return _fail("discovery inventory must report one currently shareable discovered object")
        if int(inventory.get("strongest_duplicate_count") or 0) != 2:
            return _fail("discovery inventory must preserve the strongest duplicate count")

        discovered_objects = list(inventory.get("discovered_objects") or [])
        if len(discovered_objects) != 2:
            return _fail("discovery inventory must expose both discovered object summaries")

        first = dict(discovered_objects[0] or {})
        second = dict(discovered_objects[1] or {})
        if str(first.get("object_id") or "") != share_id:
            return _fail("discovery inventory must sort the strongest duplicate candidate first")
        if not bool(first.get("shareable")) or str(first.get("sharing_state") or "") != "SHAREABLE":
            return _fail("shareable discovered object must preserve current sharing decision")
        if [str(item) for item in list(first.get("discovered_object_refs") or [])] != [
            str(share_ref.resolve()),
            str(share_dup_ref.resolve()),
        ]:
            return _fail("discovery inventory must expose normalized discovered refs for the shareable object")
        if str(first.get("discovery_kind") or "") != "matching_filename_sha256_scan":
            return _fail("discovery inventory must expose discovery_kind")

        if str(second.get("object_id") or "") != solo_id:
            return _fail("discovery inventory must preserve the single-ref discovered object")
        if bool(second.get("shareable")) or str(second.get("sharing_state") or "") != "SINGLE_REF":
            return _fail("single-ref discovered object must stay non-shareable")
        if int(second.get("duplicate_count") or 0) != 1:
            return _fail("single-ref discovered object must expose duplicate_count=1")

        if inventory.get("gaps"):
            return _fail(f"complete discovery inventory must not report gaps, got {inventory['gaps']!r}")

    with temporary_repo_root(prefix="loop_system_heavy_object_discovery_inventory_view_gap_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        gap_inventory = query_heavy_object_discovery_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            discovery_kind="matching_filename_sha256_scan",
            repo_root=repo_root,
        )
        if int(gap_inventory.get("matching_discovery_observation_count") or 0) != 0:
            return _fail("gap discovery inventory must not invent matching discovery observations")
        if int(gap_inventory.get("discovered_object_count") or 0) != 0:
            return _fail("gap discovery inventory must not invent discovered objects")
        gaps = [str(item) for item in list(gap_inventory.get("gaps") or [])]
        if "missing_matching_heavy_object_discovery_observation" not in gaps:
            return _fail("gap discovery inventory must surface missing_matching_heavy_object_discovery_observation")

    print("[loop-system-heavy-object-discovery-inventory-view][OK] heavy-object discovery inventory stays read-only and exposes committed discovery candidates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
