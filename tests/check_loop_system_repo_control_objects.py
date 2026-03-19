#!/usr/bin/env python3
"""Validate IF-4 structured control objects and unified envelope behavior."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator, validate


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-control-objects][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.gateway.normalize import normalize_control_envelope
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import (
            KernelState,
            ensure_runtime_tree,
            persist_kernel_state,
            query_kernel_state,
        )
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.loop.local_control import build_local_control_decision, decide_next_action
        from loop_product.protocols.control_envelope import EnvelopeStatus
        from loop_product.protocols.control_objects import (
            ChildDispatchStatus,
            LocalControlAction,
            LocalControlDecision,
            NodeTerminalOutcome,
            NodeTerminalResult,
        )
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
        from loop_product.runtime.lifecycle import build_node_terminal_result
    except Exception as exc:  # noqa: BLE001
        return _fail(f"loop system IF-4 imports failed: {exc}")

    try:
        envelope_schema = _load_schema("LoopSystemControlEnvelope.schema.json")
        topology_schema = _load_schema("LoopSystemTopologyMutation.schema.json")
        local_control_schema = _load_schema("LoopSystemLocalControlDecision.schema.json")
        dispatch_schema = _load_schema("LoopSystemDispatchStatus.schema.json")
        terminal_schema = _load_schema("LoopSystemNodeTerminalResult.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"missing IF-4 schema: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise IF-4 structured control object validation",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/control_object_summary.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="exercise structured control-object carriage",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/control_object_summary.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )

    topology_mutations = [
        TopologyMutation.split(child_node.node_id, ["child-a", "child-b"], reason="split accepted subgoals"),
        TopologyMutation.merge(child_node.node_id, ["child-a", "child-b"], reason="merge converged results"),
        TopologyMutation.resume(child_node.node_id, reason="resume after external unblock"),
        TopologyMutation.retry(child_node.node_id, reason="retry bounded local repair"),
        TopologyMutation.relaunch(child_node.node_id, reason="relaunch with refreshed staged workspace"),
    ]
    for mutation in topology_mutations:
        validate(mutation.to_dict(), topology_schema)

    evaluator_result = EvaluatorResult(
        verdict=EvaluatorVerdict.BUG,
        lane="reviewer",
        summary="The evaluator hit a retryable same-run interruption and should continue recovery.",
        evidence_refs=["reviewer:round-1"],
        retryable=True,
        diagnostics={"self_attribution": "", "self_repair": ""},
    )
    decision = build_local_control_decision(child_node, evaluator_result)
    dispatch_status = build_child_dispatch_status(child_node, note="child heartbeat still active")
    terminal_result = build_node_terminal_result(
        child_node,
        outcome=NodeTerminalOutcome.FAIL_CLOSED,
        summary="bounded local loop terminated fail-closed after exhausting retries",
        evidence_refs=["roundbook:child-implementer-001"],
    )

    if not isinstance(decision, LocalControlDecision):
        return _fail("build_local_control_decision must return LocalControlDecision")
    if decide_next_action(evaluator_result) != decision.action.value:
        return _fail("local control decision action must stay aligned with decide_next_action(...)")
    if not isinstance(dispatch_status, ChildDispatchStatus):
        return _fail("build_child_dispatch_status must return ChildDispatchStatus")
    if not isinstance(terminal_result, NodeTerminalResult):
        return _fail("build_node_terminal_result must return NodeTerminalResult")
    if decision.action is not LocalControlAction.RETRY:
        return _fail("retryable evaluator results should materialize a RETRY local control decision regardless of verdict kind")

    validate(decision.to_dict(), local_control_schema)
    validate(dispatch_status.to_dict(), dispatch_schema)
    validate(terminal_result.to_dict(), terminal_schema)

    normalized_envelopes = [
        normalize_control_envelope(
            mutation.to_envelope(round_id=child_node.round_id, generation=child_node.generation)
        )
        for mutation in topology_mutations
    ]
    normalized_envelopes.extend(
        [
            normalize_control_envelope(decision.to_envelope(round_id="R1.1", generation=child_node.generation)),
            normalize_control_envelope(dispatch_status.to_envelope(status=EnvelopeStatus.REPORT)),
            normalize_control_envelope(terminal_result.to_envelope(round_id="R1.2", generation=child_node.generation)),
            normalize_control_envelope(
                {
                    "source": child_node.node_id,
                    "envelope_type": "evaluator_result",
                    "payload_type": "evaluator_result",
                    "round_id": "R1.1",
                    "generation": child_node.generation,
                    "payload": evaluator_result.to_dict(),
                    "status": "report",
                    "note": evaluator_result.summary,
                }
            ),
        ]
    )
    topology_envelopes = normalized_envelopes[: len(topology_mutations)]
    report_envelopes = normalized_envelopes[len(topology_mutations) :]
    for envelope in normalized_envelopes:
        validate(envelope.to_dict(), envelope_schema)
        if not envelope.payload_type:
            return _fail("normalized envelopes must expose explicit payload_type")
        if envelope.envelope_type.endswith("_request") and envelope.status is not EnvelopeStatus.PROPOSAL:
            return _fail("request envelopes must remain proposals before kernel acceptance")
        if envelope.envelope_type in {"local_control_decision", "child_dispatch_status", "node_terminal_result", "evaluator_result"} and envelope.status is not EnvelopeStatus.REPORT:
            return _fail("report envelopes must remain reports before kernel acceptance")

    with tempfile.TemporaryDirectory(prefix="loop_system_control_objects_") as td:
        state_root = Path(td) / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="wave4-control-objects",
            root_goal="validate IF-4 structured control objects",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        baseline_query = query_kernel_state(state_root)
        if baseline_query["accepted_envelope_count"] != 0:
            return _fail("structured control objects must stay inert before kernel submission")

        for envelope in report_envelopes:
            accepted = submit_control_envelope(state_root, envelope)
            if accepted.status is not EnvelopeStatus.ACCEPTED:
                return _fail(f"kernel must accept valid structured control envelope {accepted.envelope_type}")
            if not accepted.payload_type:
                return _fail("accepted envelopes must preserve payload_type")

        direct_topology = submit_control_envelope(state_root, topology_envelopes[0])
        if direct_topology.status is not EnvelopeStatus.REJECTED:
            return _fail("generic topology submission must not bypass kernel review from a bare topology proposal")

        rejected = submit_control_envelope(
            state_root,
            {
                "source": "",
                "envelope_type": "relaunch_request",
                "payload_type": "topology_mutation",
                "round_id": "",
                "generation": -1,
                "payload": {},
                "status": "proposal",
            },
        )
        if rejected.status is not EnvelopeStatus.REJECTED:
            return _fail("malformed structured control envelope must fail closed")

        rejected_missing_required = submit_control_envelope(
            state_root,
            {
                "payload": {"summary": "missing required fields must fail closed"},
                "status": "report",
            },
        )
        if rejected_missing_required.status is not EnvelopeStatus.REJECTED:
            return _fail("missing required raw control-envelope fields must fail closed instead of being default-repaired")

        rejected_invalid_payload_type = submit_control_envelope(
            state_root,
            {
                "source": child_node.node_id,
                "envelope_type": "split_request",
                "payload_type": "not_a_real_payload_type",
                "round_id": "R1.3",
                "generation": child_node.generation,
                "payload": TopologyMutation.split(
                    child_node.node_id,
                    ["child-c", "child-d"],
                    reason="invalid raw payload_type must fail closed",
                ).to_dict(),
                "status": "proposal",
                "note": "bogus payload_type must not normalize into accepted state",
            },
        )
        if rejected_invalid_payload_type.status is not EnvelopeStatus.REJECTED:
            return _fail("raw control envelopes with invalid explicit payload_type must fail closed")

        query = query_kernel_state(state_root)
        if query["accepted_envelope_count"] != len(report_envelopes):
            return _fail("kernel query must report only report-surface accepted facts plus no unreviewed topology proposals")
        if child_node.node_id not in query["nodes"]:
            return _fail("kernel query must preserve node graph while accepting control objects")
        if query["nodes"][child_node.node_id]["status"] != NodeStatus.FAILED.value:
            return _fail("accepted node_terminal_result must update the authoritative node lifecycle state")
        accepted_types = {item["envelope_type"] for item in query["accepted_envelopes"]}
        for expected in {
            "local_control_decision",
            "child_dispatch_status",
            "node_terminal_result",
            "evaluator_result",
        }:
            if expected not in accepted_types:
                return _fail(f"accepted envelopes must preserve {expected}")
        if not all(str(item.get("payload_type") or "").strip() for item in query["accepted_envelopes"]):
            return _fail("query-visible accepted envelopes must preserve payload_type for read-only callers")

    print("[loop-system-control-objects] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
