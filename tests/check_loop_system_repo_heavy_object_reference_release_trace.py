#!/usr/bin/env python3
"""Validate read-only heavy-object reference-release trace visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-reference-release-trace][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-reference-release-trace",
        root_goal="validate read-only heavy-object reference-release trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object reference-release trace validation",
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
        from loop_product.kernel.query import query_heavy_object_reference_release_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_reference_release_request,
            submit_heavy_object_reference_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_reference_release_trace_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        object_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-a.pack"
        object_bytes = b"heavy-object-reference-release-trace\n"
        object_ref.parent.mkdir(parents=True, exist_ok=True)
        object_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        reference_holder_ref = repo_root / "workspace" / "reference-release-runtime" / "deliverables" / ".primary_artifact.publish.release"
        reference_ref = (
            reference_holder_ref
            / "primary_artifact"
            / ".lake"
            / "packages"
            / "mathlib"
            / ".git"
            / "objects"
            / "pack"
            / object_ref.name
        )
        reference_ref.parent.mkdir(parents=True, exist_ok=True)
        reference_ref.write_bytes(object_bytes)

        submit_heavy_object_registration_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            byte_size=len(object_bytes),
            reason="register one canonical heavy object before reference-release trace exists",
        )
        attached = submit_heavy_object_reference_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_publish_reference",
            reason="attach one authoritative publish reference before release",
        )
        accepted = submit_heavy_object_reference_release_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=object_ref,
            reference_ref=reference_ref,
            reference_holder_kind="workspace_publication_staging_root",
            reference_holder_ref=reference_holder_ref,
            reference_kind="mathlib_pack_publish_reference",
            reason="release the publish reference after the holder is done",
        )

        trace = query_heavy_object_reference_release_trace_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        if not bool(trace.get("read_only")):
            return _fail("heavy-object reference-release trace must stay read-only")
        if not bool(trace.get("accepted_audit_present")):
            return _fail("heavy-object reference-release trace must expose accepted audit presence")
        if not bool(trace.get("compatibility_event_present")):
            return _fail("heavy-object reference-release trace must expose compatibility mirror presence")
        if not bool(trace.get("canonical_event_present")):
            return _fail("heavy-object reference-release trace must expose canonical reference-release fact presence")
        if not bool(trace.get("matching_registration_present")):
            return _fail("heavy-object reference-release trace must expose matching registration presence")
        if not bool(trace.get("matching_reference_present")):
            return _fail("heavy-object reference-release trace must expose the released reference anchor")
        released_reference = dict(trace.get("released_reference") or {})
        if str(released_reference.get("object_id") or "") != object_id:
            return _fail("heavy-object reference-release trace must expose the released object id")
        if str(released_reference.get("reference_ref") or "") != str(reference_ref.resolve()):
            return _fail("heavy-object reference-release trace must expose the released reference ref")
        if str(released_reference.get("reference_holder_kind") or "") != "workspace_publication_staging_root":
            return _fail("heavy-object reference-release trace must expose the released holder kind")
        if str(released_reference.get("reference_holder_ref") or "") != str(reference_holder_ref.resolve()):
            return _fail("heavy-object reference-release trace must expose the released holder ref")
        if str(released_reference.get("reference_kind") or "") != "mathlib_pack_publish_reference":
            return _fail("heavy-object reference-release trace must expose the released reference kind")
        if trace.get("gaps"):
            return _fail(f"complete heavy-object reference-release trace must not report gaps, got {trace['gaps']!r}")

        missing = query_heavy_object_reference_release_trace_view(state_root, envelope_id=str(attached.envelope_id or ""))
        gaps = set(str(item) for item in list(missing.get("gaps") or []))
        if "missing_canonical_heavy_object_reference_released_event" not in gaps:
            return _fail("gapped heavy-object reference-release trace must report missing canonical release fact")
        if bool(missing.get("canonical_event_present")):
            return _fail("gapped heavy-object reference-release trace must not pretend a canonical release fact exists")

    print(
        "[loop-system-heavy-object-reference-release-trace][OK] "
        "heavy-object reference-release trace stays read-only and exposes both complete and gapped causal histories"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
