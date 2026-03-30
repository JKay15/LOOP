#!/usr/bin/env python3
"""Validate canonical result-event behavior for repo-root heavy-object remediation fanout."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-repo-remediation-result-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-repo-remediation-result-event",
        root_goal="validate heavy-object authority-gap repo remediation result events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap repo remediation result validation",
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
        from loop_product.event_journal import committed_event_count, iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_heavy_object_authority_gap_repo_remediation_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import heavy_object_authority_gap_repo_remediation_receipt_ref
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_repo_remediation_result_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        _make_pack(
            repo_root,
            "workspace/publish/repo-remediation-result-primary.pack",
            b"heavy-object-authority-gap-repo-remediation-result\n",
        )
        _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/repo-remediation-result-primary.pack",
            b"heavy-object-authority-gap-repo-remediation-result\n",
        )

        accepted = submit_heavy_object_authority_gap_repo_remediation_request(
            state_root,
            repo_root=repo_root,
            object_kind="mathlib_pack",
            reason="record one accepted repo-root heavy-object remediation result command",
            runtime_name="publish-runtime",
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(
                f"heavy-object authority-gap repo remediation request must be accepted, got {accepted.status.value!r}"
            )

        event_count_before_replay = committed_event_count(state_root)
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object repo remediation envelope must remain accepted")
        if committed_event_count(state_root) != event_count_before_replay:
            return _fail("replayed accepted heavy-object repo remediation envelope must not duplicate committed events")

        receipt_ref = heavy_object_authority_gap_repo_remediation_receipt_ref(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        if not receipt_ref.exists():
            return _fail("accepted heavy-object repo remediation command must materialize a runtime receipt")
        receipt = json.loads(receipt_ref.read_text(encoding="utf-8"))

        events = list(iter_committed_events(state_root))
        requested_events = [
            dict(event)
            for event in events
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        ]
        result_events = [
            dict(event)
            for event in events
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_repo_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        ]
        if len(requested_events) != 1:
            return _fail("accepted heavy-object repo remediation command must emit exactly one matching requested event for its accepted envelope")
        if len(result_events) != 1:
            return _fail("accepted heavy-object repo remediation command must emit exactly one matching settled result event for its accepted envelope")

        canonical = result_events[0]
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_authority_gap_repo_remediation_result:{accepted.envelope_id}":
            return _fail(
                "canonical repo remediation result event must use heavy_object_authority_gap_repo_remediation_result:<envelope_id> as command_id"
            )
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical repo remediation result event must preserve the accepted envelope identity")
        if str(payload.get("receipt_ref") or "") != str(receipt_ref):
            return _fail("canonical repo remediation result event must point at the deterministic remediation receipt")
        for key in (
            "candidate_count",
            "requested_candidate_count",
            "skipped_candidate_count",
            "unrequested_unmanaged_candidate_count_after",
            "pending_remediation_candidate_count_after",
            "remediated_candidate_count_after",
        ):
            if int(payload.get(key) or 0) != int(receipt.get(key) or 0):
                return _fail(f"canonical repo remediation result event must preserve receipt field {key}")
        if bool(payload.get("fully_managed_after")) != bool(receipt.get("fully_managed_after")):
            return _fail("canonical repo remediation result event must preserve fully_managed_after from the receipt")
        if list(payload.get("failure_reasons") or []) != list(receipt.get("failure_reasons") or []):
            return _fail("canonical repo remediation result event must preserve failure_reasons from the receipt")

        result_requested_refs = {
            str(dict(item).get("object_ref") or "")
            for item in list(payload.get("requested_candidates") or [])
        }
        receipt_requested_refs = {
            str(dict(item).get("object_ref") or "")
            for item in list(receipt.get("requested_candidates") or [])
        }
        if result_requested_refs != receipt_requested_refs:
            return _fail("canonical repo remediation result event must preserve requested candidate refs from the receipt")

    print(
        "[loop-system-heavy-object-authority-gap-repo-remediation-result-event][OK] accepted heavy-object repo remediation commands emit one canonical settled result event tied to the deterministic receipt"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
