#!/usr/bin/env python3
"""Validate canonical accepted-event and receipt behavior for repo-root heavy-object remediation fanout."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-repo-remediation-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-repo-remediation-event",
        root_goal="validate repo-root heavy-object remediation fanout event and receipt",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise repo-root heavy-object remediation event validation",
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


def _make_pack(repo_root: Path, rel: str, payload: bytes) -> Path:
    path = repo_root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.query import query_heavy_object_authority_gap_remediation_inventory_view
        from loop_product.kernel.submit import submit_heavy_object_authority_gap_repo_remediation_request
        from loop_product.runtime import heavy_object_authority_gap_repo_remediation_receipt_ref
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_repo_remediation_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/repo-remediation-primary.pack",
            b"heavy-object-authority-gap-repo-remediation\n",
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/repo-remediation-primary.pack",
            b"heavy-object-authority-gap-repo-remediation\n",
        )

        accepted = submit_heavy_object_authority_gap_repo_remediation_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="fan out heavy-object authority-gap remediation across the repo root",
            runtime_name="publish-runtime",
        )
        if str(accepted.status.value) != "accepted":
            return _fail("repo remediation batch helper must return an accepted envelope")

        receipt_ref = heavy_object_authority_gap_repo_remediation_receipt_ref(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        if not receipt_ref.exists():
            return _fail("accepted repo remediation batch must materialize a runtime receipt")
        receipt = __import__("json").loads(receipt_ref.read_text(encoding="utf-8"))
        if str(receipt.get("schema") or "") != "loop_product.heavy_object_authority_gap_repo_remediation_receipt":
            return _fail("receipt schema must identify repo remediation fanout")
        if int(receipt.get("candidate_count") or 0) != 2:
            return _fail("receipt must report both unmanaged candidates in the batch scope")
        if int(receipt.get("requested_candidate_count") or 0) != 1:
            return _fail("duplicate pair must only emit one remediation request")
        if int(receipt.get("skipped_candidate_count") or 0) != 1:
            return _fail("duplicate pair must skip the second candidate after the first request covers it")
        request_entries = list(receipt.get("requested_candidates") or [])
        if len(request_entries) != 1:
            return _fail("receipt must expose exactly one requested candidate entry")
        if not str(dict(request_entries[0]).get("remediation_envelope_id") or ""):
            return _fail("requested candidate receipt must expose the remediation envelope id")
        skipped_entries = list(receipt.get("skipped_candidates") or [])
        if len(skipped_entries) != 1:
            return _fail("receipt must expose exactly one skipped candidate entry")
        if str(dict(skipped_entries[0]).get("skip_reason") or "") != "already_managed_after_prior_fanout_request":
            return _fail("skipped candidate receipt must explain the duplicate-skip reason")
        if not bool(receipt.get("fully_managed_after")):
            return _fail("receipt must report the repo-root candidate set as fully managed after fanout")

        inventory = query_heavy_object_authority_gap_remediation_inventory_view(
            state_root,
            runtime_name="publish-runtime",
            object_kind="mathlib_pack",
            repo_root=repo_root,
        )
        if int(inventory.get("unrequested_unmanaged_candidate_count") or 0) != 0:
            return _fail("repo remediation batch must leave no unrequested unmanaged candidates in this duplicate-pair setup")
        if int(inventory.get("remediated_candidate_count") or 0) != 1:
            return _fail("inventory must report one directly remediated candidate")

    print("[loop-system-heavy-object-authority-gap-repo-remediation-event][OK] repo-root remediation fanout emits one canonical batch request and one deterministic receipt over the existing discovery chain")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
