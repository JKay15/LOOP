#!/usr/bin/env python3
"""Validate exact heavy-object identity matching when one path is reused by new content."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-exact-identity-matching][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-exact-identity-matching",
        root_goal="validate exact heavy-object identity matching",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object exact identity matching validation",
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


def _write_bytes(path: Path, payload: bytes) -> tuple[str, Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}", path.resolve()


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.query import query_heavy_object_retention_view
        from loop_product.kernel.submit import submit_heavy_object_registration_request
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_exact_identity_matching_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        shared_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "reused.pack"
        old_id, shared_ref = _write_bytes(shared_ref, b"heavy-object-exact-identity-old\n")
        submit_heavy_object_registration_request(
            state_root,
            object_id=old_id,
            object_kind="mathlib_pack",
            object_ref=shared_ref,
            byte_size=shared_ref.stat().st_size,
            reason="register the old content before path reuse",
            registration_kind="manual_exact_identity_seed",
        )

        new_id, _ = _write_bytes(shared_ref, b"heavy-object-exact-identity-new\n")
        submit_heavy_object_registration_request(
            state_root,
            object_id=new_id,
            object_kind="mathlib_pack",
            object_ref=shared_ref,
            byte_size=shared_ref.stat().st_size,
            reason="register the new content after path reuse",
            registration_kind="manual_exact_identity_seed",
        )

        before_events = list(iter_committed_events(state_root))
        before = committed_event_count(state_root)
        old_view = query_heavy_object_retention_view(state_root, object_id=old_id, object_ref=shared_ref)
        new_view = query_heavy_object_retention_view(state_root, object_id=new_id, object_ref=shared_ref)
        after_events = list(iter_committed_events(state_root))
        after = committed_event_count(state_root)
        if before != after:
            added_events = [dict(event or {}) for event in after_events[len(before_events):]]
            relevant_added_events = []
            for event in added_events:
                payload = dict(event.get("payload") or {})
                payload_object_id = str(payload.get("object_id") or "").strip()
                payload_object_ref = str(payload.get("object_ref") or "").strip()
                if payload_object_id in {old_id, new_id} or payload_object_ref == str(shared_ref):
                    relevant_added_events.append(event)
            if relevant_added_events:
                return _fail("exact heavy-object identity query must stay read-only")
        if not bool(old_view.get("read_only")) or not bool(new_view.get("read_only")):
            return _fail("exact heavy-object identity query must stay read-only")

        old_registration = dict(old_view.get("latest_registration") or {})
        if str(old_view.get("object_id") or "") != old_id:
            return _fail("old heavy-object identity lookup must preserve the old object_id even when the path was reused")
        if str(old_registration.get("object_id") or "") != old_id:
            return _fail("old heavy-object identity lookup must preserve the old registration anchor")
        if str(old_registration.get("object_ref") or "") != str(shared_ref):
            return _fail("old heavy-object identity lookup must keep the shared object_ref")

        new_registration = dict(new_view.get("latest_registration") or {})
        if str(new_view.get("object_id") or "") != new_id:
            return _fail("new heavy-object identity lookup must preserve the new object_id")
        if str(new_registration.get("object_id") or "") != new_id:
            return _fail("new heavy-object identity lookup must preserve the new registration anchor")
        if old_view.get("gaps"):
            return _fail(f"old exact-identity lookup must not report gaps, got {old_view['gaps']!r}")
        if new_view.get("gaps"):
            return _fail(f"new exact-identity lookup must not report gaps, got {new_view['gaps']!r}")

    print(
        "[loop-system-heavy-object-exact-identity-matching][OK] "
        "heavy-object lifecycle lookup stays exact across same-path content reuse"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
