#!/usr/bin/env python3
"""Validate retention visibility after authoritative heavy-object pin release."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-pin-release-retention-view][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-pin-release-retention-view",
        root_goal="validate heavy-object retention visibility after pin release",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object pin-release retention validation",
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
        from loop_product.kernel.query import query_heavy_object_retention_view
        from loop_product.kernel.submit import (
            submit_heavy_object_pin_release_request,
            submit_heavy_object_pin_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_pin_release_retention_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        object_bytes = b"heavy-object-pin-release-retention\n"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="register object before pin-release retention visibility",
        )
        submit_heavy_object_pin_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="retain the registered heavy object",
        )

        pinned_view = query_heavy_object_retention_view(state_root, object_id=object_id, object_ref=object_ref)
        if str(pinned_view.get("retention_state") or "") != "PINNED":
            return _fail("retention view must report retention_state=PINNED while the authoritative pin is active")
        if int(pinned_view.get("active_pin_count") or 0) != 1:
            return _fail("retention view must report one active pin while the pin is active")

        submit_heavy_object_pin_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            pin_holder_id="publish:artifact-bundle",
            pin_kind="publish_retention_pin",
            reason="release the publish retention pin after the holder is done",
        )

        released_view = query_heavy_object_retention_view(state_root, object_id=object_id, object_ref=object_ref)
        if str(released_view.get("retention_state") or "") != "REGISTERED":
            return _fail("retention view must drop back to retention_state=REGISTERED once the only pin is released")
        if int(released_view.get("active_pin_count") or 0) != 0:
            return _fail("retention view must report zero active pins after authoritative pin release")
        if int(released_view.get("released_pin_count") or 0) != 1:
            return _fail("retention view must expose one released pin after authoritative pin release")
        released_pins = list(released_view.get("released_pins") or [])
        if len(released_pins) != 1:
            return _fail("retention view must expose one released pin summary")
        released_pin = dict(released_pins[0] or {})
        if str(released_pin.get("pin_holder_id") or "") != "publish:artifact-bundle":
            return _fail("released pin summary must preserve the pin holder id")
        if str(released_pin.get("pin_kind") or "") != "publish_retention_pin":
            return _fail("released pin summary must preserve the pin kind")
        if released_view.get("gaps"):
            return _fail(f"released retention view must not report gaps, got {released_view['gaps']!r}")

    print("[loop-system-heavy-object-pin-release-retention-view][OK] retention visibility follows authoritative pin release instead of historical pin append-only counts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
