#!/usr/bin/env python3
"""Validate reference inventory and retention visibility after authoritative heavy-object reference release."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-reference-release-inventory-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-reference-release-inventory-view",
        root_goal="validate reference inventory and retention visibility after release",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object reference-release inventory validation",
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


def _make_ref(holder_root: Path, name: str, payload: bytes) -> Path:
    ref = holder_root / ".lake" / "packages" / "mathlib" / ".git" / "objects" / "pack" / name
    ref.parent.mkdir(parents=True, exist_ok=True)
    ref.write_bytes(payload)
    return ref.resolve()


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.query import (
            query_heavy_object_reference_inventory_view,
            query_heavy_object_retention_view,
        )
        from loop_product.kernel.submit import (
            submit_heavy_object_reference_release_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_reference_release_inventory_view_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_bytes = b"heavy-object-reference-release-inventory-view\n"
        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-reference-release-inventory.pack"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        artifact_holder_ref = repo_root / "workspace" / "inventory-runtime" / "deliverables" / "primary_artifact"
        publication_holder_ref = repo_root / "workspace" / "inventory-runtime" / "deliverables" / ".primary_artifact.publish.inventory"
        artifact_reference = _make_ref(artifact_holder_ref, object_ref.name, object_bytes)
        publication_reference = _make_ref(publication_holder_ref / "primary_artifact", object_ref.name, object_bytes)

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="register heavy object before reference release inventory exists",
        )
        submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=artifact_reference,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=artifact_holder_ref,
            reference_kind="mathlib_pack_runtime_reference",
            reason="attach runtime artifact reference",
        )
        released = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=publication_reference,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=publication_holder_ref,
            reference_kind="mathlib_pack_publish_reference",
            reason="attach publication staging reference",
        )

        submit_heavy_object_reference_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=publication_reference,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=publication_holder_ref,
            reference_kind="mathlib_pack_publish_reference",
            reason="release the publication staging reference while the runtime artifact ref remains active",
        )

        inventory = query_heavy_object_reference_inventory_view(
            state_root,
            object_id=object_id,
            object_ref=object_ref,
        )
        if not bool(inventory.get("read_only")):
            return _fail("heavy-object reference inventory must stay read-only")
        if int(inventory.get("active_reference_count") or 0) != 1:
            return _fail("reference inventory must drop the released reference from the active set")
        if int(inventory.get("released_reference_count") or 0) != 1:
            return _fail("reference inventory must expose one released reference summary")
        active_refs = [dict(item or {}) for item in list(inventory.get("active_references") or [])]
        if len(active_refs) != 1 or str(active_refs[0].get("reference_ref") or "") != str(artifact_reference):
            return _fail("reference inventory must keep only the unreleased runtime artifact reference active")
        released_refs = [dict(item or {}) for item in list(inventory.get("released_references") or [])]
        if len(released_refs) != 1 or str(released_refs[0].get("reference_ref") or "") != str(publication_reference):
            return _fail("reference inventory must preserve the released publication reference summary")
        if inventory.get("gaps"):
            return _fail(f"partially released reference inventory must not report gaps, got {inventory['gaps']!r}")

        retained = query_heavy_object_retention_view(state_root, object_id=object_id, object_ref=object_ref)
        if str(retained.get("retention_state") or "") != "REFERENCED":
            return _fail("while one authoritative reference remains, retention must stay REFERENCED")

        submit_heavy_object_reference_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=artifact_reference,
            reference_holder_kind="workspace_artifact_root",
            reference_holder_ref=artifact_holder_ref,
            reference_kind="mathlib_pack_runtime_reference",
            reason="release the final runtime artifact reference",
        )

        released_inventory = query_heavy_object_reference_inventory_view(
            state_root,
            object_id=object_id,
            object_ref=object_ref,
        )
        if int(released_inventory.get("active_reference_count") or 0) != 0:
            return _fail("after releasing all authoritative references, active_reference_count must converge to zero")
        if int(released_inventory.get("released_reference_count") or 0) != 2:
            return _fail("after releasing all authoritative references, released_reference_count must preserve both releases")
        if "missing_matching_heavy_object_reference" not in set(
            str(item) for item in list(released_inventory.get("gaps") or [])
        ):
            return _fail("released reference inventory must report the missing active reference gap once everything is released")

        registered_only = query_heavy_object_retention_view(state_root, object_id=object_id, object_ref=object_ref)
        if str(registered_only.get("retention_state") or "") != "REGISTERED":
            return _fail("after releasing all references without supersession or pinning, retention must converge to REGISTERED")

        missing = query_heavy_object_reference_inventory_view(
            state_root,
            object_id="sha256:missing",
            object_ref=repo_root / "missing.pack",
        )
        if "missing_matching_heavy_object_reference" not in set(str(item) for item in list(missing.get("gaps") or [])):
            return _fail("gapped heavy-object reference inventory must report missing matching reference support")

    print(
        "[loop-system-heavy-object-reference-release-inventory-view][OK] "
        "reference inventory and retention stay read-only and follow the latest authoritative attach-vs-release truth"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
