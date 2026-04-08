#!/usr/bin/env python3
"""Validate minimal router v0 event object definitions."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-events][FAIL] {msg}", file=sys.stderr)
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
            RequestSplit,
            RejectSplit,
            RouterControlMessage,
            RouterInboxItem,
            RouterEvent,
            TakeoverResolved,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router event imports failed: {exc}")

    now = datetime.now(timezone.utc)
    actor = ActorRef(
        node_id="node-001",
        actor_kind=ActorKind.EVALUATOR_REVIEWER,
        attempt_count=3,
    )
    ai_user_actor = ActorRef(
        node_id="node-002",
        actor_kind=ActorKind.EVALUATOR_AI_USER,
        attempt_count=4,
        task_id="task-7",
    )
    kernel = ActorRef(
        node_id=KERNEL_NODE_ID,
        actor_kind=ActorKind.KERNEL,
        attempt_count=KERNEL_ATTEMPT_COUNT,
    )
    kernel_checker = ActorRef(
        node_id=KERNEL_NODE_ID,
        actor_kind=ActorKind.EVALUATOR_CHECKER,
        attempt_count=1,
    )

    observed = OutputWindow(
        actor=actor,
        had_output=True,
        observed_at=now,
    )
    exited = ProcessExitedObserved(
        actor=actor,
        pid=43210,
        process_birth_time=1711965600.0,
        exit_code=0,
        signal_name=None,
        occurred_at=now,
    )
    request_split = RequestSplit(
        actor=actor,
        split_bundle_ref="/tmp/split-bundle",
        durable_commit="abc123",
        diff_fingerprint="diff-fingerprint-001",
        requested_at=now,
    )
    approve_split = ApproveSplit(
        actor=kernel,
        target_node_id="node-001",
        request_seq=17,
        approved_at=now,
    )
    reject_split = RejectSplit(
        actor=kernel,
        target_node_id="node-001",
        request_seq=18,
        rejected_at=now,
        reason_ref="/tmp/reject.md",
    )
    takeover_resolved = TakeoverResolved(
        actor=kernel,
        target_node_id="node-001",
        takeover_id="takeover-001",
        resolution_ref="/tmp/takeover-resolved.md",
        workspace_root="/tmp/workspace/node-001",
        final_effects_file="/tmp/workspace/node-001/FINAL_EFFECTS.md",
        resolved_at=now,
    )

    if actor.actor_kind is not ActorKind.EVALUATOR_REVIEWER:
        return _fail("actor kind must preserve the evaluator reviewer role")
    if ai_user_actor.task_id != "task-7":
        return _fail("AI-user actor identity must preserve task_id")
    if observed.actor.node_id != "node-001":
        return _fail("OutputWindow must retain actor node identity")
    if observed.had_output is not True:
        return _fail("OutputWindow must retain had_output")
    if observed.observed_at != now:
        return _fail("OutputWindow must retain observed_at")
    if exited.exit_code != 0:
        return _fail("ProcessExitedObserved must retain exit_code")
    if exited.process_birth_time != 1711965600.0:
        return _fail("ProcessExitedObserved must retain process_birth_time")
    if exited.actor.attempt_count != 3:
        return _fail("ProcessExitedObserved must retain attempt_count")
    if request_split.durable_commit != "abc123":
        return _fail("RequestSplit must retain durable_commit")
    if request_split.diff_fingerprint != "diff-fingerprint-001":
        return _fail("RequestSplit must retain diff_fingerprint")
    if request_split.split_bundle_ref != "/tmp/split-bundle":
        return _fail("RequestSplit must retain split_bundle_ref")
    if approve_split.actor.actor_kind is not ActorKind.KERNEL:
        return _fail("ApproveSplit must retain kernel actor identity")
    if approve_split.actor.node_id != KERNEL_NODE_ID:
        return _fail("kernel actor must use node_id 0")
    if approve_split.target_node_id != "node-001":
        return _fail("ApproveSplit must retain target_node_id")
    if approve_split.request_seq != 17:
        return _fail("ApproveSplit must retain request_seq")
    if kernel_checker.actor_kind is not ActorKind.EVALUATOR_CHECKER:
        return _fail("node 0 must allow evaluator actors after final kernel takeover")
    if takeover_resolved.resolution_ref != "/tmp/takeover-resolved.md":
        return _fail("TakeoverResolved must retain resolution_ref")

    event_count = sum(
        isinstance(item, RouterEvent)
        for item in (observed, exited)
    )
    if event_count != 2:
        return _fail("RouterEvent alias must cover both low-level event types")
    control_count = sum(
        isinstance(item, RouterControlMessage)
        for item in (request_split, approve_split, reject_split, takeover_resolved)
    )
    if control_count != 4:
        return _fail("RouterControlMessage alias must cover split/takeover control items")
    inbox_count = sum(
        isinstance(item, RouterInboxItem)
        for item in (
            observed,
            exited,
            request_split,
            approve_split,
            reject_split,
            takeover_resolved,
        )
    )
    if inbox_count != 6:
        return _fail("RouterInboxItem alias must cover runtime events and control messages")

    print("[loop-system-router-events][PASS] minimal router runtime events and control messages import and instantiate cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
