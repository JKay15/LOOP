#!/usr/bin/env python3
"""Validate minimal router v0 SQLite store operations."""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-store][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.events import (
            ApproveSplit,
            ActorKind,
            ActorRef,
            KERNEL_ATTEMPT_COUNT,
            KERNEL_NODE_ID,
            OutputWindow,
            ProcessExitedObserved,
            RejectSplit,
            RequestSplit,
        )
        from loop.node_table import (
            ComponentRuntimeState,
            ComponentStatus,
            NodeRuntimeRecord,
            component_key,
        )
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router store imports failed: {exc}")

    now = datetime.now(timezone.utc)
    actor = ActorRef(
        node_id="node-001",
        actor_kind=ActorKind.IMPLEMENTER,
        attempt_count=2,
    )
    ai_user_actor = ActorRef(
        node_id="node-002",
        actor_kind=ActorKind.EVALUATOR_AI_USER,
        attempt_count=5,
        task_id="task-7",
    )
    kernel = ActorRef(
        node_id=KERNEL_NODE_ID,
        actor_kind=ActorKind.KERNEL,
        attempt_count=KERNEL_ATTEMPT_COUNT,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        store.initialize()

        first_seq = store.append_event(
            OutputWindow(
                actor=actor,
                had_output=True,
                observed_at=now,
            ),
            recorded_at=now,
        )
        second_seq = store.append_event(
            ApproveSplit(
                actor=kernel,
                target_node_id="node-001",
                request_seq=1,
                approved_at=now,
            ),
            recorded_at=now,
        )
        third_seq = store.append_event(
            ProcessExitedObserved(
                actor=actor,
                pid=43210,
                process_birth_time=1711965600.0,
                exit_code=0,
                signal_name=None,
                occurred_at=now,
            ),
            recorded_at=now,
        )
        fourth_seq = store.append_event(
            RequestSplit(
                actor=actor,
                split_bundle_ref="/tmp/split-bundle-002",
                durable_commit="abc123",
                diff_fingerprint="diff-fingerprint-002",
                requested_at=now,
            ),
            recorded_at=now,
        )
        fifth_seq = store.append_event(
            RejectSplit(
                actor=kernel,
                target_node_id="node-001",
                request_seq=4,
                rejected_at=now,
                reason_ref="/tmp/reject.md",
            ),
            recorded_at=now,
        )
        sixth_seq = store.append_event(
            ProcessExitedObserved(
                actor=ai_user_actor,
                pid=54321,
                process_birth_time=1711965602.0,
                exit_code=0,
                signal_name=None,
                occurred_at=now,
            ),
            recorded_at=now,
        )

        if (
            first_seq != 1
            or second_seq != 2
            or third_seq != 3
            or fourth_seq != 4
            or fifth_seq != 5
            or sixth_seq != 6
        ):
            return _fail("inbox sequence numbers must auto-increment from 1")

        events = store.list_events_after(0)
        if len(events) != 6:
            return _fail("store must return all appended inbox items")
        if events[0].event_type != "OutputWindow":
            return _fail("first stored inbox item must preserve event_type")
        if events[0].attempt_count != 2:
            return _fail("stored runtime event must preserve attempt_count")

        first_payload = json.loads(events[0].payload_json)
        if first_payload.get("had_output") is not True:
            return _fail("stored OutputWindow payload must preserve had_output")
        if first_payload.get("observed_at") is None:
            return _fail("stored OutputWindow payload must preserve observed_at")
        if events[1].event_type != "ApproveSplit":
            return _fail("second stored inbox item must preserve control message type")
        if events[1].actor_kind is not ActorKind.KERNEL:
            return _fail("kernel control messages must preserve kernel actor kind")
        if events[1].attempt_count != KERNEL_ATTEMPT_COUNT:
            return _fail("kernel control messages must preserve the kernel actor attempt_count")
        if events[1].node_id != KERNEL_NODE_ID:
            return _fail("kernel control messages must preserve kernel node_id 0")
        second_payload = json.loads(events[1].payload_json)
        if second_payload.get("request_seq") != 1:
            return _fail("stored ApproveSplit payload must preserve request_seq")
        if second_payload.get("target_node_id") != "node-001":
            return _fail("stored ApproveSplit payload must preserve target_node_id")
        fourth_payload = json.loads(events[3].payload_json)
        if fourth_payload.get("split_bundle_ref") != "/tmp/split-bundle-002":
            return _fail("stored RequestSplit payload must preserve split_bundle_ref")
        if fourth_payload.get("durable_commit") != "abc123":
            return _fail("stored RequestSplit payload must preserve durable_commit")
        if fourth_payload.get("diff_fingerprint") != "diff-fingerprint-002":
            return _fail("stored RequestSplit payload must preserve diff_fingerprint")
        sixth_payload = json.loads(events[5].payload_json)
        if sixth_payload.get("task_id") != "task-7":
            return _fail("stored runtime event payload must preserve AI-user task_id")
        loaded_fourth = store.load_event(fourth_seq)
        if loaded_fourth is None or loaded_fourth.event_type != "RequestSplit":
            return _fail("store.load_event must load one inbox row by seq")
        if store.load_event(9999) is not None:
            return _fail("store.load_event must return None for unknown seq")
        if store.latest_request_split_seq_for_node("node-001") != fourth_seq:
            return _fail("store must surface the latest RequestSplit seq for a node")

        record = NodeRuntimeRecord(
            node_id="node-001",
            parent_node_id="0",
            child_node_ids=["node-010", "node-011"],
            workspace_root="/tmp/workspace/node-001",
            final_effects_file="/tmp/workspace/node-001/FINAL_EFFECTS.md",
            split_request=1,
            split_approved=1,
            approved_split_request_seq=4,
            evaluator_phase="",
            checker_tasks_ref="",
            task_result_refs={"task-1": {"evaluator_tester": "/tmp/result-1.md"}},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            pending_prelude_lines=["Resume after incomplete completion signals."],
            current_components=[
                ActorRef(
                    node_id="node-001",
                    actor_kind=ActorKind.IMPLEMENTER,
                    attempt_count=2,
                )
            ],
            durable_commit="abc123",
            result_commit="def456",
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint="diff-fingerprint-001",
            components={
                component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                    status=ComponentStatus.RUNNING,
                    attempt_count=2,
                    pid=43210,
                    process_birth_time=1711965600.0,
                    session_ids=["session-1", "session-2"],
                    workspace_fingerprint_before="fingerprint-before-001",
                    saw_output_in_attempt=True,
                    consecutive_no_progress=0,
                    consecutive_failed_exits=0,
                ),
            },
        )
        store.upsert_node(record)
        loaded_record = store.load_node("node-001")
        if loaded_record is None:
            return _fail("stored node record must be loadable")
        if loaded_record.parent_node_id != "0":
            return _fail("stored node record must preserve parent_node_id")
        if loaded_record.child_node_ids != ["node-010", "node-011"]:
            return _fail("stored node record must preserve child_node_ids")
        if loaded_record.workspace_root != "/tmp/workspace/node-001":
            return _fail("stored node record must preserve workspace_root")
        if loaded_record.final_effects_file != "/tmp/workspace/node-001/FINAL_EFFECTS.md":
            return _fail("stored node record must preserve final_effects_file")
        if loaded_record.split_request != 1:
            return _fail("stored node record must preserve split_request flag")
        if loaded_record.split_approved != 1:
            return _fail("stored node record must preserve split_approved flag")
        if loaded_record.approved_split_request_seq != 4:
            return _fail("stored node record must preserve approved_split_request_seq")
        if loaded_record.task_result_refs != {"task-1": {"evaluator_tester": "/tmp/result-1.md"}}:
            return _fail("stored node record must preserve task_result_refs")
        if loaded_record.pending_prelude_lines != ["Resume after incomplete completion signals."]:
            return _fail("stored node record must preserve pending_prelude_lines")
        if loaded_record.durable_commit != "abc123":
            return _fail("stored node record must preserve durable_commit")
        if loaded_record.result_commit != "def456":
            return _fail("stored node record must preserve result_commit")
        if loaded_record.last_rejected_split_diff_fingerprint != "diff-fingerprint-001":
            return _fail("stored node record must preserve last_rejected_split_diff_fingerprint")
        if loaded_record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].pid != 43210:
            return _fail("stored node record must preserve component pid")
        if loaded_record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].process_birth_time != 1711965600.0:
            return _fail("stored node record must preserve component process_birth_time")
        if loaded_record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].session_ids != ["session-1", "session-2"]:
            return _fail("stored node record must preserve component session_ids")
        if loaded_record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)].workspace_fingerprint_before != "fingerprint-before-001":
            return _fail("stored node record must preserve component launch snapshot fingerprint")
        if len(loaded_record.current_components) != 1:
            return _fail("stored node record must preserve current_components")
        if loaded_record.current_components[0].actor_kind is not ActorKind.IMPLEMENTER:
            return _fail("stored current_components must preserve actor kind")
        if loaded_record.current_components[0].attempt_count != 2:
            return _fail("stored current_components must preserve attempt_count")
        ai_record = NodeRuntimeRecord(
            node_id="node-002",
            parent_node_id="0",
            child_node_ids=[],
            workspace_root="/tmp/workspace/node-002",
            final_effects_file="/tmp/workspace/node-002/FINAL_EFFECTS.md",
            split_request=0,
            split_approved=0,
            approved_split_request_seq=0,
            evaluator_phase="tasks",
            checker_tasks_ref="/tmp/workspace/node-002/checker/tasks.json",
            task_result_refs={},
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            pending_prelude_lines=[],
            current_components=[
                ActorRef(
                    node_id="node-002",
                    actor_kind=ActorKind.EVALUATOR_AI_USER,
                    attempt_count=5,
                    task_id="task-7",
                )
            ],
            durable_commit="ghi789",
            result_commit="",
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint="",
            components={
                component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-7"): ComponentRuntimeState(
                    status=ComponentStatus.RUNNING,
                    attempt_count=5,
                    task_id="task-7",
                    pid=54321,
                    process_birth_time=1711965602.0,
                    session_ids=["ai-session-1"],
                    workspace_fingerprint_before="fingerprint-before-ai-002",
                    saw_output_in_attempt=False,
                    consecutive_no_progress=0,
                    consecutive_failed_exits=0,
                ),
            },
        )
        store.upsert_node(ai_record)
        loaded_ai_record = store.load_node("node-002")
        if loaded_ai_record is None:
            return _fail("AI-user node record must be loadable")
        if loaded_ai_record.components[component_key(actor_kind=ActorKind.EVALUATOR_AI_USER, task_id="task-7")].task_id != "task-7":
            return _fail("stored node record must preserve AI-user component task_id")
        if len(loaded_ai_record.current_components) != 1:
            return _fail("stored AI-user node record must preserve current_components")
        if loaded_ai_record.current_components[0].task_id != "task-7":
            return _fail("stored current_components must preserve AI-user task_id")
        pending_reviews = store.list_pending_split_reviews()
        if pending_reviews != [("node-001", fourth_seq)]:
            pending_reviews = [
                (pending.node_id, pending.request_seq) for pending in pending_reviews
            ]
        if pending_reviews != [("node-001", fourth_seq)]:
            return _fail("store must rebuild pending split reviews from node_table + latest RequestSplit seq")

        if store.read_last_applied_seq() != 0:
            return _fail("fresh store cursor must start at 0")
        store.write_last_applied_seq(2)
        if store.read_last_applied_seq() != 2:
            return _fail("store cursor must persist updated sequence")
        if store.read_kernel_session_id() != "":
            return _fail("fresh store kernel_session_id must default to empty string")
        if store.read_kernel_rollout_path() != "":
            return _fail("fresh store kernel_rollout_path must default to empty string")
        if store.read_kernel_started_at() != "":
            return _fail("fresh store kernel_started_at must default to empty string")
        if store.read_router_status() != "":
            return _fail("fresh store router_status must default to empty string")
        if store.read_router_terminal_reason_json() != "":
            return _fail("fresh store router_terminal_reason_json must default to empty string")
        if store.read_router_terminal_at() != "":
            return _fail("fresh store router_terminal_at must default to empty string")
        if store.read_router_completed_result_commit() != "":
            return _fail("fresh store router_completed_result_commit must default to empty string")
        if store.read_router_completed_report_ref() != "":
            return _fail("fresh store router_completed_report_ref must default to empty string")
        if store.read_router_completed_at() != "":
            return _fail("fresh store router_completed_at must default to empty string")
        store.write_kernel_bootstrap_info(
            kernel_session_id="kernel-session-001",
            kernel_rollout_path="/tmp/kernel-rollout.jsonl",
            kernel_started_at="2026-04-03T00:30:00Z",
        )
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"majority_frontier_terminal_failed"}',
            router_terminal_at="2026-04-05T00:00:00+00:00",
        )
        if store.read_kernel_session_id() != "kernel-session-001":
            return _fail("store must persist kernel_session_id updates")
        if store.read_kernel_rollout_path() != "/tmp/kernel-rollout.jsonl":
            return _fail("store must persist kernel_rollout_path updates")
        if store.read_kernel_started_at() != "2026-04-03T00:30:00Z":
            return _fail("store must persist kernel_started_at updates")
        if store.read_router_status() != "terminal_failed":
            return _fail("store must persist router_status updates")
        if store.read_router_terminal_reason_json() != '{"reason":"majority_frontier_terminal_failed"}':
            return _fail("store must persist router_terminal_reason_json updates")
        if store.read_router_terminal_at() != "2026-04-05T00:00:00+00:00":
            return _fail("store must persist router_terminal_at updates")
        store.write_router_completed_state(
            router_status="completed",
            router_completed_result_commit="fedcba654321",
            router_completed_report_ref="/tmp/router-complete.md",
            router_completed_at="2026-04-06T00:00:00+00:00",
        )
        if store.read_router_status() != "completed":
            return _fail("store must persist completed router_status updates")
        if store.read_router_terminal_reason_json() != "":
            return _fail("completed router state must clear router_terminal_reason_json")
        if store.read_router_terminal_at() != "":
            return _fail("completed router state must clear router_terminal_at")
        if store.read_router_completed_result_commit() != "fedcba654321":
            return _fail("store must persist router_completed_result_commit updates")
        if store.read_router_completed_report_ref() != "/tmp/router-complete.md":
            return _fail("store must persist router_completed_report_ref updates")
        if store.read_router_completed_at() != "2026-04-06T00:00:00+00:00":
            return _fail("store must persist router_completed_at updates")

        reopened = RouterStore(db_path)
        if reopened.read_last_applied_seq() != 2:
            return _fail("reopened store must preserve cursor state")
        if reopened.read_kernel_session_id() != "kernel-session-001":
            return _fail("reopened store must preserve kernel_session_id")
        if reopened.read_kernel_rollout_path() != "/tmp/kernel-rollout.jsonl":
            return _fail("reopened store must preserve kernel_rollout_path")
        if reopened.read_kernel_started_at() != "2026-04-03T00:30:00Z":
            return _fail("reopened store must preserve kernel_started_at")
        if reopened.read_router_status() != "completed":
            return _fail("reopened store must preserve completed router_status")
        if reopened.read_router_completed_result_commit() != "fedcba654321":
            return _fail("reopened store must preserve router_completed_result_commit")
        if reopened.read_router_completed_report_ref() != "/tmp/router-complete.md":
            return _fail("reopened store must preserve router_completed_report_ref")
        if reopened.read_router_completed_at() != "2026-04-06T00:00:00+00:00":
            return _fail("reopened store must preserve router_completed_at")
        reopened_record = reopened.load_node("node-001")
        if reopened_record is None or len(reopened_record.current_components) != 1:
            return _fail("reopened store must preserve node current_components")
        if reopened_record.current_components[0].actor_kind is not ActorKind.IMPLEMENTER:
            return _fail("reopened store must preserve the implementer current actor")

    print("[loop-system-router-store][PASS] minimal router store persists mixed inbox items, node table, and cursor")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
