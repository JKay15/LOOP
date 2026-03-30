#!/usr/bin/env python3
"""Validate read-only heavy-object authority-gap trace visibility for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-trace-view][FAIL] {msg}", file=sys.stderr)
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
        task_id="milestone5-heavy-object-authority-gap-trace-view",
        root_goal="validate read-only heavy-object authority-gap trace visibility",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap trace validation",
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
        from loop_product.kernel.query import query_heavy_object_authority_gap_trace_view
        from loop_product.kernel.submit import (
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_trace_complete_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        managed_id, managed_candidate_ref = _make_pack(
            repo_root,
            "workspace/publish/trace-managed.pack",
            b"heavy-object-authority-gap-trace-managed\n",
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
        _gap_id, unmanaged_ref = _make_pack(
            repo_root,
            "workspace/publish/trace-gap.pack",
            b"heavy-object-authority-gap-trace-gap\n",
        )

        submit_heavy_object_registration_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            byte_size=managed_ref.stat().st_size,
            reason="register managed trace candidate",
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
            reason="discover managed trace candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        managed_trace = query_heavy_object_authority_gap_trace_view(
            state_root,
            object_ref=managed_candidate_ref,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if not bool(managed_trace.get("read_only")):
            return _fail("authority-gap trace must stay read-only")
        if str(managed_trace.get("coverage_state") or "") != "MANAGED":
            return _fail("managed candidate must report coverage_state=MANAGED")
        if not bool(managed_trace.get("filesystem_candidate_present")):
            return _fail("managed trace must report the candidate file as present")
        if not bool(managed_trace.get("registered_present")):
            return _fail("managed trace must report committed registration support through object-id-first matching")
        if not bool(managed_trace.get("discovery_present")):
            return _fail("managed trace must report committed discovery support")
        if managed_trace.get("gaps"):
            return _fail(f"managed trace must not report gaps, got {managed_trace['gaps']!r}")
        latest_registration = dict(managed_trace.get("latest_registration") or {})
        if str(dict(latest_registration.get("payload") or {}).get("object_ref") or "") != str(managed_ref.resolve()):
            return _fail("managed trace must preserve the canonical registration ref while tracing the candidate path")

        unmanaged_trace = query_heavy_object_authority_gap_trace_view(
            state_root,
            object_ref=unmanaged_ref,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if str(unmanaged_trace.get("coverage_state") or "") != "UNMANAGED":
            return _fail("unmanaged candidate must report coverage_state=UNMANAGED")
        if not bool(unmanaged_trace.get("filesystem_candidate_present")):
            return _fail("unmanaged trace must still report the filesystem candidate as present")
        if bool(unmanaged_trace.get("registered_present")) or bool(unmanaged_trace.get("discovery_present")):
            return _fail("unmanaged trace must not invent committed lifecycle support")
        gaps = [str(item) for item in list(unmanaged_trace.get("gaps") or [])]
        if "unmanaged_heavy_object_candidate_present" not in gaps:
            return _fail("unmanaged trace must surface unmanaged_heavy_object_candidate_present")

        missing_trace = query_heavy_object_authority_gap_trace_view(
            state_root,
            object_ref=repo_root / "workspace/publish/missing.pack",
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if bool(missing_trace.get("filesystem_candidate_present")):
            return _fail("missing trace must not invent a filesystem candidate")
        gaps = [str(item) for item in list(missing_trace.get("gaps") or [])]
        if "missing_matching_filesystem_heavy_object_candidate" not in gaps:
            return _fail("missing trace must surface missing_matching_filesystem_heavy_object_candidate")

    print("[loop-system-heavy-object-authority-gap-trace-view][OK] heavy-object authority-gap trace stays read-only and explains managed vs unmanaged candidate coverage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
