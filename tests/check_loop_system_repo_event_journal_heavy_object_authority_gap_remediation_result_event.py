#!/usr/bin/env python3
"""Validate canonical settled-event behavior for repo-local heavy-object authority-gap remediation."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-heavy-object-authority-gap-remediation-result-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-authority-gap-remediation-result-event",
        root_goal="validate heavy-object authority-gap remediation result events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object authority-gap remediation result validation",
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
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_heavy_object_authority_gap_remediation_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import heavy_object_discovery_receipt_ref
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_authority_gap_remediation_result_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = _make_pack(
            repo_root,
            "workspace/publish/authority-gap-remediation-result-primary.pack",
            b"heavy-object-authority-gap-remediation-result\n",
        )
        duplicate_ref = _make_pack(
            repo_root,
            "artifacts/heavy_objects/mathlib/authority-gap-remediation-result-primary.pack",
            b"heavy-object-authority-gap-remediation-result\n",
        )

        accepted = submit_heavy_object_authority_gap_remediation_request(
            state_root,
            object_ref=primary_ref,
            object_kind="mathlib_pack",
            reason="record one accepted repo-local heavy-object remediation result command",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(
                f"heavy-object authority-gap remediation request must be accepted, got {accepted.status.value!r}"
            )

        events_before_replay = list(iter_committed_events(state_root))
        requested_count_before_replay = sum(
            1
            for event in events_before_replay
            if str(event.get("event_type") or "") == "heavy_object_discovery_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        observed_count_before_replay = sum(
            1
            for event in events_before_replay
            if str(event.get("event_type") or "") == "heavy_object_observed"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        result_count_before_replay = sum(
            1
            for event in events_before_replay
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object remediation envelope must remain accepted")
        events_after_replay = list(iter_committed_events(state_root))
        requested_count_after_replay = sum(
            1
            for event in events_after_replay
            if str(event.get("event_type") or "") == "heavy_object_discovery_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        observed_count_after_replay = sum(
            1
            for event in events_after_replay
            if str(event.get("event_type") or "") == "heavy_object_observed"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        result_count_after_replay = sum(
            1
            for event in events_after_replay
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        )
        if (
            requested_count_after_replay != requested_count_before_replay
            or observed_count_after_replay != observed_count_before_replay
            or result_count_after_replay != result_count_before_replay
        ):
            return _fail("replayed accepted heavy-object remediation envelope must not duplicate its matching committed events")

        receipt_ref = heavy_object_discovery_receipt_ref(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        if not receipt_ref.exists():
            return _fail("accepted heavy-object remediation command must materialize a deterministic discovery receipt")
        receipt = json.loads(receipt_ref.read_text(encoding="utf-8"))

        events = events_after_replay
        requested_events = [
            dict(event)
            for event in events
            if str(event.get("event_type") or "") == "heavy_object_discovery_requested"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        ]
        observed_events = [
            dict(event)
            for event in events
            if str(event.get("event_type") or "") == "heavy_object_observed"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        ]
        result_events = [
            dict(event)
            for event in events
            if str(event.get("event_type") or "") == "heavy_object_authority_gap_remediation_settled"
            and str(dict(event.get("payload") or {}).get("envelope_id") or "") == str(accepted.envelope_id or "")
        ]
        if len(requested_events) != 1:
            return _fail("accepted heavy-object remediation command must emit exactly one requested event")
        if len(observed_events) != 1:
            return _fail("accepted heavy-object remediation command must emit exactly one observed event")
        if len(result_events) != 1:
            return _fail("accepted heavy-object remediation command must emit exactly one settled result event")

        canonical = result_events[0]
        payload = dict(canonical.get("payload") or {})
        if str(canonical.get("command_id") or "") != f"heavy_object_authority_gap_remediation_result:{accepted.envelope_id}":
            return _fail(
                "canonical remediation result event must use heavy_object_authority_gap_remediation_result:<envelope_id> as command_id"
            )
        if str(payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical remediation result event must preserve the accepted envelope identity")
        if str(payload.get("receipt_ref") or "") != str(receipt_ref):
            return _fail("canonical remediation result event must point at the deterministic discovery receipt")
        for key in (
            "object_id",
            "object_kind",
            "object_ref",
            "discovery_kind",
            "runtime_name",
            "repo_root",
            "discovery_root",
        ):
            if str(payload.get(key) or "") != str(receipt.get(key) or ""):
                return _fail(f"canonical remediation result event must preserve receipt field {key}")
        for key in ("duplicate_count", "total_bytes"):
            if int(payload.get(key) or 0) != int(receipt.get(key) or 0):
                return _fail(f"canonical remediation result event must preserve receipt field {key}")
        result_refs = [str(item) for item in list(payload.get("object_refs") or [])]
        receipt_refs = [str(item) for item in list(receipt.get("discovered_refs") or [])]
        if result_refs != receipt_refs:
            return _fail("canonical remediation result event must preserve discovered refs from the receipt")
        if result_refs != [str(duplicate_ref.resolve()), str(primary_ref.resolve())]:
            return _fail("canonical remediation result event must expose the duplicate refs in deterministic order")

    print(
        "[loop-system-heavy-object-authority-gap-remediation-result-event][OK] accepted heavy-object remediation commands emit one canonical settled result event tied to the deterministic discovery receipt"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
