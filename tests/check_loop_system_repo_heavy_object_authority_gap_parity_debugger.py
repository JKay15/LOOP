#!/usr/bin/env python3
"""Validate command parity debugger coverage for heavy-object authority-gap commands."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-parity-debugger][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-parity-debugger",
        root_goal="validate heavy-object authority-gap parity debugger",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap parity debugger validation",
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
        from loop_product.event_journal import committed_event_count
        from loop_product.kernel.query import query_command_parity_debugger_view
        from loop_product.kernel.submit import (
            submit_heavy_object_authority_gap_audit_request,
            submit_heavy_object_authority_gap_repo_remediation_request,
            submit_heavy_object_discovery_request,
            submit_heavy_object_registration_request,
        )
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import (
            heavy_object_authority_gap_audit_receipt_ref,
            heavy_object_authority_gap_repo_remediation_receipt_ref,
        )
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_parity_audit_complete_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_parity_debugger_runtime"
        _persist_anchor_state(state_root)

        managed_id, managed_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/parity-audit-managed.pack",
            b"heavy-object-authority-gap-parity-audit-managed\n",
        )
        _unused_id, _unmanaged_ref = _make_pack(
            repo_root,
            "workspace/publish/parity-audit-unmanaged.pack",
            b"heavy-object-authority-gap-parity-audit-unmanaged\n",
        )
        submit_heavy_object_registration_request(
            state_root,
            object_id=managed_id,
            object_kind="mathlib_pack",
            object_ref=managed_ref,
            byte_size=managed_ref.stat().st_size,
            reason="register managed audit parity candidate",
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
            reason="discover managed audit parity candidate",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )

        accepted = submit_heavy_object_authority_gap_audit_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="exercise heavy-object authority-gap audit parity debugger",
            runtime_name="publish-runtime",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("audit parity scenario requires an accepted audit command")
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("heavy-object authority-gap audit parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "heavy_object":
            return _fail("heavy-object audit parity debugger must preserve heavy_object classification")
        if str(debug.get("trace_surface") or "") != "heavy_object_authority_gap_audit_trace":
            return _fail("heavy-object audit parity debugger must route through audit trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted heavy-object audit parity debugger scenario must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted heavy-object audit parity debugger scenario must not report mismatches")
        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(trusted.get("command_kind") or "") != "heavy_object_authority_gap_audit":
            return _fail("trusted expected effect must expose heavy_object_authority_gap_audit command kind")
        if int(trusted.get("candidate_count") or 0) != 2:
            return _fail("trusted expected effect must expose audit candidate_count")
        if int(projected.get("filesystem_candidate_count") or 0) != 2:
            return _fail("projected visible effect must expose final filesystem candidate count")
        if int(projected.get("managed_candidate_count") or 0) != 1:
            return _fail("projected visible effect must expose final managed candidate count")
        if int(projected.get("unmanaged_candidate_count") or 0) != 1:
            return _fail("projected visible effect must expose final unmanaged candidate count")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_parity_audit_gap_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_parity_debugger_runtime"
        _persist_anchor_state(state_root)
        _make_pack(
            repo_root,
            "workspace/publish/parity-audit-gap-unmanaged.pack",
            b"heavy-object-authority-gap-parity-audit-gap\n",
        )
        accepted = submit_heavy_object_authority_gap_audit_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="exercise missing audit receipt parity handling",
            runtime_name="publish-runtime",
        )
        receipt_ref = heavy_object_authority_gap_audit_receipt_ref(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        if not receipt_ref.exists():
            return _fail("audit parity gap scenario requires an initial audit receipt")
        receipt_ref.unlink()
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("gapped heavy-object audit parity debugger must stay read-only")
        if bool(debug.get("parity_ok")):
            return _fail("missing audit receipt must fail parity")
        mismatches = [str(item) for item in list(debug.get("mismatches") or [])]
        if "trace_gaps_present" not in mismatches:
            return _fail("missing audit receipt must surface trace_gaps_present")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_parity_repo_remediation_complete_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_parity_debugger_runtime"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/parity-repo-remediation-primary.pack",
            b"heavy-object-authority-gap-parity-repo-remediation\n",
        )[1]
        _duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/parity-repo-remediation-primary.pack",
            b"heavy-object-authority-gap-parity-repo-remediation\n",
        )[1]

        accepted = submit_heavy_object_authority_gap_repo_remediation_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="exercise heavy-object repo remediation parity debugger",
            runtime_name="publish-runtime",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail("repo remediation parity scenario requires an accepted remediation command")
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("heavy-object repo remediation parity debugger must stay read-only")
        if str(debug.get("classification") or "") != "heavy_object":
            return _fail("heavy-object repo remediation parity debugger must preserve heavy_object classification")
        if str(debug.get("trace_surface") or "") != "heavy_object_authority_gap_repo_remediation_trace":
            return _fail("heavy-object repo remediation parity debugger must route through repo remediation trace")
        if not bool(debug.get("parity_ok")):
            return _fail("accepted heavy-object repo remediation parity debugger scenario must report parity")
        if list(debug.get("mismatches") or []):
            return _fail("accepted heavy-object repo remediation parity debugger scenario must not report mismatches")
        trusted = dict(debug.get("trusted_expected_effect") or {})
        projected = dict(debug.get("projected_visible_effect") or {})
        if str(trusted.get("command_kind") or "") != "heavy_object_authority_gap_repo_remediation":
            return _fail("trusted expected effect must expose heavy_object_authority_gap_repo_remediation command kind")
        if int(trusted.get("candidate_count") or 0) != 2:
            return _fail("trusted expected effect must expose remediation candidate_count")
        if int(trusted.get("requested_resolution_candidate_count") or 0) != 1:
            return _fail("trusted expected effect must expose one requested-resolution candidate")
        if int(projected.get("filesystem_candidate_count") or 0) != 2:
            return _fail("projected visible effect must expose final remediation filesystem candidate count")
        if int(projected.get("unrequested_unmanaged_candidate_count") or 0) != 0:
            return _fail("projected visible effect must expose fully remediated unmanaged coverage")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_parity_repo_remediation_gap_") as repo_root:
        state_root = repo_root / ".loop" / "heavy_object_authority_gap_parity_debugger_runtime"
        _persist_anchor_state(state_root)
        _make_pack(
            repo_root,
            "workspace/publish/parity-repo-remediation-gap.pack",
            b"heavy-object-authority-gap-parity-repo-remediation-gap\n",
        )
        accepted = submit_heavy_object_authority_gap_repo_remediation_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="exercise missing repo remediation receipt parity handling",
            runtime_name="publish-runtime",
        )
        receipt_ref = heavy_object_authority_gap_repo_remediation_receipt_ref(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        if not receipt_ref.exists():
            return _fail("repo remediation parity gap scenario requires an initial remediation receipt")
        receipt_ref.unlink()
        before = committed_event_count(state_root)
        debug = query_command_parity_debugger_view(state_root, envelope_id=str(accepted.envelope_id or ""))
        after = committed_event_count(state_root)
        if before != after:
            return _fail("gapped heavy-object repo remediation parity debugger must stay read-only")
        if bool(debug.get("parity_ok")):
            return _fail("missing repo remediation receipt must fail parity")
        mismatches = [str(item) for item in list(debug.get("mismatches") or [])]
        if "trace_gaps_present" not in mismatches:
            return _fail("missing repo remediation receipt must surface trace_gaps_present")

    print("[loop-system-heavy-object-authority-gap-parity-debugger][OK] heavy-object authority-gap parity debugger stays read-only and reports both complete and gapped audit/remediation histories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
