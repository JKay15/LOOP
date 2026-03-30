#!/usr/bin/env python3
"""Validate canonical accepted heavy-object discovery events for Milestone 5."""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-event-journal-heavy-object-discovery-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _persist_anchor_state(state_root: Path) -> None:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    kernel_state = KernelState(
        task_id="milestone5-heavy-object-discovery-event",
        root_goal="validate canonical heavy-object discovery command events",
        root_node_id="root-kernel",
    )
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise heavy-object discovery event validation",
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


def _relevant_discovery_events(*, state_root: Path, envelope_id: str) -> list[dict]:
    from loop_product.event_journal import iter_committed_events

    relevant: list[dict] = []
    for event in iter_committed_events(state_root):
        event_dict = dict(event)
        payload = dict(event_dict.get("payload") or {})
        event_type = str(event_dict.get("event_type") or "")
        if event_type == "control_envelope_accepted" and str(payload.get("envelope_id") or "") == envelope_id:
            relevant.append(event_dict)
            continue
        if event_type in {"heavy_object_discovery_requested", "heavy_object_observed"} and str(
            payload.get("envelope_id") or ""
        ) == envelope_id:
            relevant.append(event_dict)
    return relevant


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.event_journal import iter_committed_events
        from loop_product.kernel.submit import submit_control_envelope, submit_heavy_object_discovery_request
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.runtime import heavy_object_discovery_receipt_ref
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    with temporary_repo_root(prefix="loop_system_heavy_object_discovery_event_") as repo_root:
        state_root = repo_root / ".loop"
        _persist_anchor_state(state_root)

        primary_ref = repo_root / "artifacts" / "heavy_objects" / "mathlib" / "pack-discovery.pack"
        duplicate_ref = repo_root / "workspace" / "publish" / "primary_artifact" / "pack-discovery.pack"
        object_bytes = b"heavy-object-discovery-foundation\n"
        primary_ref.parent.mkdir(parents=True, exist_ok=True)
        primary_ref.write_bytes(object_bytes)
        duplicate_ref.parent.mkdir(parents=True, exist_ok=True)
        duplicate_ref.write_bytes(object_bytes)
        object_id = f"sha256:{hashlib.sha256(object_bytes).hexdigest()}"

        accepted = submit_heavy_object_discovery_request(
            state_root,
            object_id=object_id,
            object_kind="mathlib_pack",
            object_ref=primary_ref,
            discovery_root=repo_root,
            discovery_kind="matching_filename_sha256_scan",
            reason="discover duplicate heavy-object refs through the authoritative runtime surface",
            runtime_name="publish-runtime",
            repo_root=repo_root,
        )
        if accepted.status is not EnvelopeStatus.ACCEPTED:
            return _fail(f"heavy-object discovery request must be accepted, got {accepted.status.value!r}")
        if str(accepted.classification or "") != "heavy_object":
            return _fail("accepted heavy-object discovery request must preserve heavy_object classification")

        relevant_before = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_discovery_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
            )
        )
        replay = submit_control_envelope(state_root, accepted)
        if replay.status is not EnvelopeStatus.ACCEPTED:
            return _fail("replayed accepted heavy-object discovery envelope must remain accepted")
        relevant_after = tuple(
            str(event.get("event_id") or "")
            for event in _relevant_discovery_events(
                state_root=state_root,
                envelope_id=str(accepted.envelope_id or ""),
            )
        )
        if relevant_after != relevant_before:
            return _fail("accepted heavy-object discovery replay must not append duplicate relevant committed facts")

        receipt_ref = heavy_object_discovery_receipt_ref(state_root=state_root, envelope_id=str(accepted.envelope_id or ""))
        if not receipt_ref.exists():
            return _fail("heavy-object discovery execution must persist a deterministic discovery receipt")

        events = list(iter_committed_events(state_root))
        relevant_events = _relevant_discovery_events(
            state_root=state_root,
            envelope_id=str(accepted.envelope_id or ""),
        )
        event_types = [str(event.get("event_type") or "") for event in relevant_events]
        if event_types.count("control_envelope_accepted") != 1:
            return _fail("heavy-object discovery command must retain exactly one compatibility envelope mirror")
        if event_types.count("heavy_object_discovery_requested") != 1:
            return _fail("heavy-object discovery command must emit exactly one canonical heavy_object_discovery_requested fact")
        if event_types.count("heavy_object_observed") != 1:
            return _fail("trusted heavy-object discovery execution must emit exactly one canonical heavy_object_observed fact")

        request_event = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_discovery_requested"),
            {},
        )
        request_payload = dict(request_event.get("payload") or {})
        if str(request_event.get("command_id") or "") != f"heavy_object_discovery:{accepted.envelope_id}":
            return _fail("canonical heavy-object discovery request event must use heavy_object_discovery:<envelope_id> as command_id")
        if str(request_payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object discovery request event must preserve the accepted envelope identity")
        if str(request_payload.get("object_id") or "") != object_id:
            return _fail("canonical heavy-object discovery request event must preserve object_id")
        if str(request_payload.get("object_ref") or "") != str(primary_ref.resolve()):
            return _fail("canonical heavy-object discovery request event must preserve the normalized primary object ref")
        if str(request_payload.get("discovery_root") or "") != str(repo_root.resolve()):
            return _fail("canonical heavy-object discovery request event must preserve the normalized discovery root")
        if str(request_payload.get("discovery_kind") or "") != "matching_filename_sha256_scan":
            return _fail("canonical heavy-object discovery request event must preserve discovery_kind")

        observed_event = next(
            (dict(event) for event in events if str(event.get("event_type") or "") == "heavy_object_observed"),
            {},
        )
        observed_payload = dict(observed_event.get("payload") or {})
        if str(observed_event.get("command_id") or "") != f"heavy_object_discovery_result:{accepted.envelope_id}":
            return _fail("canonical heavy-object discovery observation event must use heavy_object_discovery_result:<envelope_id> as command_id")
        if str(observed_payload.get("envelope_id") or "") != str(accepted.envelope_id or ""):
            return _fail("canonical heavy-object discovery observation event must preserve the accepted envelope identity")
        if str(observed_payload.get("object_id") or "") != object_id:
            return _fail("canonical heavy-object discovery observation event must preserve object_id")
        if str(observed_payload.get("object_ref") or "") != str(primary_ref.resolve()):
            return _fail("canonical heavy-object discovery observation event must preserve the normalized primary object ref")
        if [str(item) for item in list(observed_payload.get("object_refs") or [])] != [
            str(primary_ref.resolve()),
            str(duplicate_ref.resolve()),
        ]:
            return _fail("canonical heavy-object discovery observation event must preserve discovered refs")
        if int(observed_payload.get("duplicate_count") or 0) != 2:
            return _fail("canonical heavy-object discovery observation event must preserve duplicate_count")
        if int(observed_payload.get("total_bytes") or 0) != len(object_bytes) * 2:
            return _fail("canonical heavy-object discovery observation event must preserve total_bytes")
        if str(observed_payload.get("observation_kind") or "") != "matching_filename_sha256_scan":
            return _fail("canonical heavy-object discovery observation event must preserve observation_kind")
        if str(observed_payload.get("discovery_receipt_ref") or "") != str(receipt_ref):
            return _fail("canonical heavy-object discovery observation event must point at the deterministic discovery receipt")

    print("[loop-system-event-journal-heavy-object-discovery-event][OK] accepted heavy-object discovery commands emit canonical request and observed facts without replay duplication")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
