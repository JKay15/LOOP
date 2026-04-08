#!/usr/bin/env python3
"""Validate minimal router v0 node-table object definitions."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-node-table][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.events import ActorKind, ActorRef
        from loop.node_table import (
            ComponentRuntimeState,
            ComponentStatus,
            NodeRuntimeRecord,
            component_key,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router node-table imports failed: {exc}")

    implementer = ComponentRuntimeState(
        status=ComponentStatus.RUNNING,
        attempt_count=3,
        pid=43210,
        process_birth_time=1711965600.0,
        session_ids=["session-1", "session-2", "session-3"],
        workspace_fingerprint_before="fingerprint-before-001",
        saw_output_in_attempt=True,
        consecutive_no_progress=1,
        consecutive_failed_exits=0,
    )
    reviewer = ComponentRuntimeState(
        status=ComponentStatus.INACTIVE,
        attempt_count=0,
        pid=None,
        process_birth_time=None,
        session_ids=[],
        workspace_fingerprint_before="",
        saw_output_in_attempt=False,
        consecutive_no_progress=0,
        consecutive_failed_exits=0,
    )
    record = NodeRuntimeRecord(
        node_id="node-001",
        parent_node_id="0",
        child_node_ids=["node-010", "node-011"],
        workspace_root="/tmp/workspace/node-001",
        final_effects_file="/tmp/workspace/node-001/FINAL_EFFECTS.md",
        split_request=0,
        split_approved=1,
        approved_split_request_seq=7,
        evaluator_phase="",
        checker_tasks_ref="",
        task_result_refs={"task-1": {"evaluator_tester": "/tmp/result-1.md"}},
        reviewer_verdict_kind="",
        reviewer_report_ref="",
        pending_prelude_lines=["Resume after incomplete completion signals."],
        current_components=[
            ActorRef(node_id="node-001", actor_kind=ActorKind.IMPLEMENTER, attempt_count=3)
        ],
        durable_commit="abc123",
        result_commit="def456",
        escalated_to_kernel=False,
        last_rejected_split_diff_fingerprint="diff-fingerprint-001",
        components={
            component_key(actor_kind=ActorKind.IMPLEMENTER): implementer,
            component_key(actor_kind=ActorKind.EVALUATOR_REVIEWER): reviewer,
        },
    )

    if record.node_id != "node-001":
        return _fail("node record must retain node identity")
    if record.parent_node_id != "0":
        return _fail("node record must retain parent_node_id")
    if record.child_node_ids != ["node-010", "node-011"]:
        return _fail("node record must retain ordered child_node_ids")
    if record.workspace_root != "/tmp/workspace/node-001":
        return _fail("node record must retain workspace_root")
    if record.final_effects_file != "/tmp/workspace/node-001/FINAL_EFFECTS.md":
        return _fail("node record must retain final_effects_file")
    if record.split_request != 0:
        return _fail("node record must retain split_request flag")
    if record.split_approved != 1:
        return _fail("node record must retain split_approved flag")
    if record.approved_split_request_seq != 7:
        return _fail("node record must retain approved split request seq")
    if len(record.current_components) != 1:
        return _fail("node record must retain one current actor")
    if record.current_components[0].actor_kind is not ActorKind.IMPLEMENTER:
        return _fail("node record must retain the implementer current actor")
    if record.task_result_refs != {"task-1": {"evaluator_tester": "/tmp/result-1.md"}}:
        return _fail("node record must retain task_result_refs")
    if record.pending_prelude_lines != ["Resume after incomplete completion signals."]:
        return _fail("node record must retain pending_prelude_lines")
    if record.durable_commit != "abc123":
        return _fail("node record must retain durable commit")
    if record.result_commit != "def456":
        return _fail("node record must retain durable result commit")
    if record.last_rejected_split_diff_fingerprint != "diff-fingerprint-001":
        return _fail("node record must retain the latest rejected split diff fingerprint")
    if record.escalated_to_kernel:
        return _fail("node record should preserve the escalation flag")
    if record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].status is not ComponentStatus.RUNNING:
        return _fail("implementer status must preserve RUNNING")
    if record.components[component_key(actor_kind=ActorKind.EVALUATOR_REVIEWER)].status is not ComponentStatus.INACTIVE:
        return _fail("reviewer status must preserve INACTIVE")
    if record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].attempt_count != 3:
        return _fail("component attempt_count must be preserved")
    if record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].pid != 43210:
        return _fail("component pid must be preserved")
    if record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].process_birth_time != 1711965600.0:
        return _fail("component process_birth_time must be preserved")
    if record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].session_ids != ["session-1", "session-2", "session-3"]:
        return _fail("component session_ids must preserve ordered attempt history")
    if record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].workspace_fingerprint_before != "fingerprint-before-001":
        return _fail("component launch snapshot fingerprint must be preserved")
    if not record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].saw_output_in_attempt:
        return _fail("component output-observed flag must be preserved")

    print("[loop-system-router-node-table][PASS] minimal router node-table objects import and instantiate cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
