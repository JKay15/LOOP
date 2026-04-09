#!/usr/bin/env python3
"""Validate router core wakeup + split lifecycle focused closure."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import threading
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-core][FAIL] {msg}", file=sys.stderr)
    return 2


class _LiveProcess:
    def __init__(self, pid: int = 0) -> None:
        self.pid = int(pid)
        self.terminate_calls = 0
        self._done = threading.Event()

    def poll(self):
        return None

    def wait(self) -> int:
        self._done.wait()
        return 0

    def terminate(self) -> None:
        self.terminate_calls += 1


class _ExitedProcess:
    def __init__(self, pid: int = 0) -> None:
        self.pid = int(pid)

    def poll(self):
        return 1


class _FakeProcSupervisor:
    def __init__(self) -> None:
        self.started = 0
        self.stopped = 0
        self.registered: list[dict[str, object]] = []
        self.live_actors: set[object] = set()
        self.terminated: list[object] = []

    def start(self) -> None:
        self.started += 1

    def stop(self) -> None:
        self.stopped += 1

    def register_actor(
        self,
        actor,
        *,
        process,
        process_birth_time,
        session_id,
        rollout_path=None,
        now=None,
    ):
        self.registered.append(
            {
                "actor": actor,
                "process": process,
                "process_birth_time": process_birth_time,
                "session_id": session_id,
                "rollout_path": rollout_path,
                "now": now,
            }
        )
        self.live_actors.add(actor)
        return rollout_path

    def has_actor(self, actor) -> bool:
        return actor in self.live_actors

    def terminate_actor(self, actor) -> bool:
        self.terminated.append(actor)
        self.live_actors.discard(actor)
        return True

    def mark_live(self, actor) -> None:
        self.live_actors.add(actor)


class _HungThread:
    def __init__(self) -> None:
        self.join_calls: list[float | None] = []

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)

    def is_alive(self) -> bool:
        return True


def _write_final_effects(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path.resolve()


def _write_file(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path.resolve()


def _checker_manifest_payload(
    *,
    workspace_root: Path,
    final_effects_file: Path,
    snapshot_id: str,
    tasks: list[dict[str, object]],
    obligation_text: str | None = None,
) -> dict[str, object]:
    return {
        "version": 2,
        "snapshot_id": snapshot_id,
        "workspace_root": str(workspace_root.resolve()),
        "authoritative_final_effects_path": str(final_effects_file.resolve()),
        "obligations": [
            {
                "id": "req-1",
                "section": "Main",
                "requirement_text": obligation_text or final_effects_file.read_text(encoding="utf-8").strip(),
            }
        ],
        "tasks": tasks,
    }


def _checker_task_entry(
    task_id: str,
    *,
    goal: str | None = None,
    blocking_condition: str | None = None,
    task_instructions: str | None = None,
    required_evidence: list[str] | None = None,
    covered_requirement_ids: list[str] | None = None,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "covered_requirement_ids": list(covered_requirement_ids or ["req-1"]),
        "goal": goal or f"Validate {task_id}.",
        "blocking_condition": blocking_condition or f"Block if {task_id} fails.",
        "task_instructions": task_instructions or f"Inspect the current workspace and validate {task_id}.",
        "required_evidence": list(required_evidence or []),
    }


def _write_completion(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path.resolve()


def _run_git(cwd: Path, *args: str) -> str:
    proc = subprocess.run(
        ["git", "-C", str(cwd), *args],
        capture_output=True,
        text=True,
        env={
            **dict(os.environ),
            "GIT_AUTHOR_NAME": "Router Test",
            "GIT_AUTHOR_EMAIL": "router-test@example.com",
            "GIT_COMMITTER_NAME": "Router Test",
            "GIT_COMMITTER_EMAIL": "router-test@example.com",
        },
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed in {cwd}: {str(proc.stderr or '').strip() or 'unknown error'}"
        )
    return str(proc.stdout or "").strip()


def _init_git_repo(repo_root: Path) -> str:
    repo_root.mkdir(parents=True, exist_ok=True)
    _run_git(repo_root, "init")
    _run_git(repo_root, "config", "user.name", "Router Test")
    _run_git(repo_root, "config", "user.email", "router-test@example.com")
    (repo_root / "README.md").write_text("base\n", encoding="utf-8")
    _run_git(repo_root, "add", "README.md")
    _run_git(repo_root, "commit", "-m", "initial")
    return _run_git(repo_root, "rev-parse", "HEAD")


def _commit_in_detached_worktree(
    *,
    repo_root: Path,
    base_commit: str,
    worktree_root: Path,
    relpath: str,
    content: str,
    message: str,
) -> str:
    _run_git(repo_root, "worktree", "add", "--detach", str(worktree_root), str(base_commit))
    target = (worktree_root / relpath).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")
    _run_git(worktree_root, "add", relpath)
    _run_git(worktree_root, "commit", "-m", message)
    return _run_git(worktree_root, "rev-parse", "HEAD")


def _init_repo_with_final_effects(repo_root: Path, text: str) -> tuple[Path, str]:
    _init_git_repo(repo_root)
    final_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", text)
    _run_git(repo_root, "add", "FINAL_EFFECTS.md")
    _run_git(repo_root, "commit", "-m", "add final effects")
    return final_effects.resolve(), _run_git(repo_root, "rev-parse", "HEAD")


def _write_split_bundle(
    bundle_dir: Path,
    *,
    child_names: list[str],
    parent_after_merge: bool = False,
) -> Path:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    resolved_bundle_dir = bundle_dir.resolve()
    coverage_units: list[dict[str, str]] = []
    children: list[dict[str, object]] = []
    for index, child_name in enumerate(child_names, start=1):
        final_effects = _write_final_effects(
            resolved_bundle_dir / "children" / child_name / "FINAL_EFFECTS.md",
            f"Work for {child_name}.\n",
        )
        unit_id = f"u{index:02d}"
        coverage_units.append(
            {
                "id": unit_id,
                "requirement_text": f"Remaining work for {child_name}.",
            }
        )
        children.append(
            {
                "name": child_name,
                "final_effects_file": str(final_effects.relative_to(resolved_bundle_dir)),
                "covered_unit_ids": [unit_id],
            }
        )
    proposal: dict[str, object] = {
        "version": 2,
        "coverage_units": coverage_units,
        "children": children,
    }
    if parent_after_merge:
        residual_id = f"u{len(coverage_units) + 1:02d}"
        residual_effects = _write_final_effects(
            resolved_bundle_dir / "parent_after_merge" / "FINAL_EFFECTS.md",
            "Parent residual work after merged child outputs.\n",
        )
        coverage_units.append(
            {
                "id": residual_id,
                "requirement_text": "Residual parent work after merged child outputs.",
            }
        )
        proposal["parent_after_merge"] = {
            "final_effects_file": str(residual_effects.relative_to(resolved_bundle_dir)),
            "covered_unit_ids": [residual_id],
        }
    (resolved_bundle_dir / "proposal.json").write_text(json.dumps(proposal, indent=2, sort_keys=True), encoding="utf-8")
    return resolved_bundle_dir


def _upsert_running_node(
    *,
    store,
    node_id: str,
    parent_node_id: str,
    attempt_count: int,
    durable_commit: str,
    workspace_root: Path,
    final_effects_file: Path,
    actor_kind,
    pid: int,
    process_birth_time: float,
    session_ids: list[str],
    split_request: int = 0,
    split_approved: int = 0,
    approved_split_request_seq: int = 0,
    child_node_ids: list[str] | None = None,
    last_rejected_split_diff_fingerprint: str = "",
    result_commit: str = "",
    checker_tasks_ref: str = "",
    task_result_refs: dict[str, dict[str, str]] | None = None,
    pending_prelude_lines: list[str] | None = None,
    workspace_fingerprint_before: str = "fingerprint-before",
    task_id: str | None = None,
):
    from loop.events import ActorRef
    from loop.node_table import (
        ComponentRuntimeState,
        ComponentStatus,
        NodeRuntimeRecord,
        component_key,
    )

    store.upsert_node(
        NodeRuntimeRecord(
            node_id=node_id,
            parent_node_id=parent_node_id,
            child_node_ids=list(child_node_ids or []),
            workspace_root=str(workspace_root.resolve()),
            final_effects_file=str(final_effects_file.resolve()),
            split_request=int(split_request),
            split_approved=int(split_approved),
            approved_split_request_seq=int(approved_split_request_seq),
            evaluator_phase="",
            checker_tasks_ref=str(checker_tasks_ref),
            task_result_refs=dict(task_result_refs or {}),
            reviewer_verdict_kind="",
            reviewer_report_ref="",
            pending_prelude_lines=list(pending_prelude_lines or []),
            current_components=[
                ActorRef(
                    node_id=node_id,
                    actor_kind=actor_kind,
                    attempt_count=int(attempt_count),
                    task_id=str(task_id or "").strip() or None,
                )
            ],
            durable_commit=durable_commit,
            result_commit=result_commit,
            escalated_to_kernel=False,
            last_rejected_split_diff_fingerprint=last_rejected_split_diff_fingerprint,
            components={
                component_key(
                    actor_kind=actor_kind,
                    task_id=str(task_id or "").strip() or None,
                ): ComponentRuntimeState(
                    status=ComponentStatus.RUNNING,
                    attempt_count=int(attempt_count),
                    task_id=str(task_id or "").strip(),
                    pid=int(pid),
                    process_birth_time=float(process_birth_time),
                    session_ids=list(session_ids),
                    workspace_fingerprint_before=str(workspace_fingerprint_before),
                    saw_output_in_attempt=False,
                    consecutive_no_progress=0,
                    consecutive_failed_exits=0,
                )
            },
        )
    )


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.core import (
            ActorLaunchResult,
            RouterCore,
            notify_router_wakeup,
            router_wakeup_socket_path,
        )
        from loop.completion import actor_completion_record_path, workspace_output_fingerprint
        from loop.evaluator_runtime import (
            ai_user_current_result_path,
            ai_user_accepted_result_path,
            checker_current_tasks_path,
            checker_accepted_tasks_path,
            reviewer_accepted_feedback_path,
            reviewer_accepted_report_path,
            reviewer_current_feedback_path,
            reviewer_current_report_path,
            tester_accepted_result_path,
            tester_current_result_path,
        )
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
        from loop.node_table import component_key
        from loop.store import RouterStore, StoredRouterInboxItem
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router core imports failed: {exc}")

    now = datetime.now(timezone.utc)
    kernel = ActorRef(
        node_id=KERNEL_NODE_ID,
        actor_kind=ActorKind.KERNEL,
        attempt_count=KERNEL_ATTEMPT_COUNT,
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        prompt_effects = _write_final_effects(
            tmp / "workspace" / "prompt-node" / "FINAL_EFFECTS.md",
            "Prompt contract.\n",
        )

        def _unused_actor_launcher(spec) -> ActorLaunchResult:
            return ActorLaunchResult(
                process=_LiveProcess(pid=29999),
                process_birth_time=1712999900.0,
                session_id="prompt-session",
                rollout_path=(tmp / "runtime" / "prompt.jsonl").resolve(),
            )

        prompt_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=_unused_actor_launcher,
        )
        tester_prompt = prompt_core._build_actor_prompt_text(
            node_id="prompt-node",
            actor_kind=ActorKind.EVALUATOR_TESTER,
            workspace_root=prompt_effects.parent,
            final_effects_file=prompt_effects,
            prelude_lines=["Router evaluator tester dispatch."],
        )
        if "=== ROUTER FEEDBACK SLOT ===" not in tester_prompt:
            return _fail("tester prompt must include the router feedback slot")
        if "at most 200 changed lines" not in tester_prompt:
            return _fail("tester prompt must define a concrete block-size limit")
        if "Review each block with this fixed sequence" not in tester_prompt:
            return _fail("tester prompt must define a fixed deep-review workflow")
        if "owner must be one of:" not in tester_prompt:
            return _fail("tester prompt must require owner tagging for each finding")
        if "Artifact gate policy:" not in tester_prompt:
            return _fail("tester prompt must define an artifact gate policy")
        if "Missing weak-gate artifacts must not block by themselves." not in tester_prompt:
            return _fail("tester prompt must state that weak-gate artifacts are non-blocking by default")
        if "report that as a checker-side contract problem" not in tester_prompt:
            return _fail("tester prompt must reroute overweight artifact contracts to checker")

        fault_prelude = prompt_core._build_evaluator_fault_checker_prelude(
            report_ref="/tmp/reviewer_report.md",
            feedback_ref="/tmp/reviewer_feedback.json",
            prior_checker_tasks_ref="/tmp/prior_tasks.json",
        )
        if "Reviewer report reference: /tmp/reviewer_report.md" not in fault_prelude:
            return _fail("fault-aware checker prelude must include reviewer report reference")
        if "Reviewer feedback reference: /tmp/reviewer_feedback.json" not in fault_prelude:
            return _fail("fault-aware checker prelude must include reviewer feedback reference")
        if "Previous checker manifest reference: /tmp/prior_tasks.json" not in fault_prelude:
            return _fail("fault-aware checker prelude must include prior checker manifest reference")
        if "Previous evaluator wave ended in EVALUATOR_FAULT. Do not reproduce the prior checker manifest blindly." not in fault_prelude:
            return _fail("fault-aware checker prelude must explicitly forbid blind reruns")

        implementer_prompt = prompt_core._build_actor_prompt_text(
            node_id="prompt-node",
            actor_kind=ActorKind.IMPLEMENTER,
            workspace_root=prompt_effects.parent,
            final_effects_file=prompt_effects,
            prelude_lines=["Router resume after reviewer feedback."],
        )
        if "request-split --split-bundle-ref <bundle_dir>" not in implementer_prompt:
            return _fail("implementer prompt must continue documenting the split submission interface in the completion contract")
        if "Before declaring completion, check the cheapest concrete way the work could still be falsely 'done'." not in implementer_prompt:
            return _fail("implementer prompt must preserve the cheapest-failure-mode completion check")
        if "Do not treat helper materials, cached references, fetched examples, temporary scaffolds, or copied comparison artifacts as the finished result" not in implementer_prompt:
            return _fail("implementer prompt must forbid treating helper/reference material as the finished deliverable")
        if "Implementation integrity:" not in implementer_prompt:
            return _fail("implementer prompt must keep the approved generic implementation-integrity section")
        if "Split interface:" not in implementer_prompt or "`split` means proposing to kernel" not in implementer_prompt:
            return _fail("implementer prompt must define what split means before referencing the split interface")
        if "proposal.json` with `version = 2`" not in implementer_prompt or "one child FINAL_EFFECTS file for each proposed child" not in implementer_prompt:
            return _fail("implementer prompt must state the minimum split bundle contents")
        if "Split gate:" not in implementer_prompt:
            return _fail("implementer prompt must expose the approved split gate")
        if "Consider split only after you have already made real implementation progress in the current workspace." not in implementer_prompt:
            return _fail("implementer prompt must require real parent implementation progress before split is considered")
        if "zero semantic omission into multiple concrete child scopes that can each make meaningful parallel progress before final merge" not in implementer_prompt:
            return _fail("implementer prompt must relax split toward meaningful parallel progress while preserving zero semantic omission")
        if "Do not require near-total independence: shared later integration boundaries or a shared central theorem target do not by themselves make split inappropriate." not in implementer_prompt:
            return _fail("implementer prompt must state that shared later integration alone does not forbid split")
        if "/Users/xiongjiangkai/xjk_papers/LOOP/IMPLEMENTER_SPLIT_GUIDE.md" not in implementer_prompt:
            return _fail("implementer prompt must point implementers at the shared split guide path")
        if "you must read the shared split guide" not in implementer_prompt or "complete `proposal.json v2` schema" not in implementer_prompt:
            return _fail("implementer prompt must require reading the shared split guide before request-split because the full split schema lives there")
        if "Post-split behavior:" not in implementer_prompt:
            return _fail("implementer prompt must explain what happens after a split request is submitted")
        if "Router and kernel now own the split decision." not in implementer_prompt:
            return _fail("implementer prompt must tell implementers to stop the attempt after request-split succeeds")
        if "Do not submit another split request after rejection unless there is new effective implementation progress and a materially improved split case." not in implementer_prompt:
            return _fail("implementer prompt must prevent immediate repeat split submissions after rejection")
        if "finish any explicit `parent_after_merge` residual work" not in implementer_prompt:
            return _fail("implementer prompt must require explicit residual work to be finished after child merge")
        if "Authority-grounded rebuttal rule:" not in implementer_prompt or "routing inputs, not higher authority than FINAL_EFFECTS.md" not in implementer_prompt:
            return _fail("implementer prompt must explicitly subordinate routed feedback to FINAL_EFFECTS authority")
        if "Explain why that interpretation is a semantic narrowing rather than a necessary operational clarification." not in implementer_prompt:
            return _fail("implementer prompt must require explicit semantic-narrowing rebuttal reasoning")
        if "Split rule:" in implementer_prompt or "Completion rule:" in implementer_prompt:
            return _fail("implementer prompt must not add unapproved split/completion rule sections")

        kernel_prompt = prompt_core._build_actor_prompt_text(
            node_id="0",
            actor_kind=ActorKind.KERNEL,
            workspace_root=prompt_effects.parent,
            final_effects_file=prompt_effects,
            prelude_lines=[
                "Router kernel review dispatch.",
                "Split request seq: 7",
                f"Parent workspace root: {prompt_effects.parent}",
                f"Parent final effects file: {prompt_effects}",
                "Frozen split bundle snapshot: /tmp/frozen-bundle",
                "Parent durable commit baseline: abc123",
                "Active system node count: 12",
                "Open split-approved node count: 2",
            ],
            extra_env={"LOOP_REQUEST_SEQ": "7"},
        )
        if "Zero-omission rule:" not in kernel_prompt:
            return _fail("kernel prompt must define the zero-omission rule for split review")
        if "coverage_units must account for every still-unfinished requirement" not in kernel_prompt:
            return _fail("kernel prompt must concretize zero omission beyond headline tasks")
        if "Children must not have overlapping ownership of the same coverage unit." not in kernel_prompt:
            return _fail("kernel prompt must forbid overlapping child ownership in split review")
        if "Shared later integration boundaries or a shared central theorem target do not by themselves make a split invalid." not in kernel_prompt:
            return _fail("kernel prompt must preserve split viability despite shared later integration boundaries")
        if "Reject splits only when overlap or cross-child blocking dominates the normal success path strongly enough that likely integration cost outweighs the benefit of parallel progress." not in kernel_prompt:
            return _fail("kernel prompt must reject splits only when overlap dominates enough to erase parallel benefit")
        if "If active system node count is 100 or higher, reject by default." not in kernel_prompt:
            return _fail("kernel prompt must define a hard high-pressure split threshold")
        if "Parent workspace root:" not in kernel_prompt or "Frozen split bundle snapshot:" not in kernel_prompt:
            return _fail("kernel prompt must surface the injected parent workspace and frozen bundle paths")
        if "Current split depth" in kernel_prompt or "Existing child count for this parent" in kernel_prompt:
            return _fail("kernel prompt must not inject unapproved subtree metrics")

        active_effects = _write_final_effects(
            tmp / "workspace" / "active-node" / "FINAL_EFFECTS.md",
            "Active node count contract.\n",
        )
        pending_effects = _write_final_effects(
            tmp / "workspace" / "pending-split" / "FINAL_EFFECTS.md",
            "Pending split contract.\n",
        )
        approved_effects = _write_final_effects(
            tmp / "workspace" / "approved-split" / "FINAL_EFFECTS.md",
            "Approved split contract.\n",
        )
        completed_split_effects = _write_final_effects(
            tmp / "workspace" / "completed-approved-split" / "FINAL_EFFECTS.md",
            "Completed approved split contract.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="10",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="active-commit",
            workspace_root=active_effects.parent,
            final_effects_file=active_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=30101,
            process_birth_time=1713000101.0,
            session_ids=["active-session"],
        )
        _upsert_running_node(
            store=store,
            node_id="11",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="pending-commit",
            workspace_root=pending_effects.parent,
            final_effects_file=pending_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=30102,
            process_birth_time=1713000102.0,
            session_ids=["pending-session"],
            split_request=1,
        )
        pending_record = store.load_node("11")
        if pending_record is None:
            return _fail("pending split record must exist for active-node-count testing")
        store.upsert_node(replace(pending_record, current_components=[]))
        _upsert_running_node(
            store=store,
            node_id="12",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="approved-commit",
            workspace_root=approved_effects.parent,
            final_effects_file=approved_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=30103,
            process_birth_time=1713000103.0,
            session_ids=["approved-session"],
            split_approved=1,
        )
        approved_record = store.load_node("12")
        if approved_record is None:
            return _fail("approved split record must exist for active-node-count testing")
        store.upsert_node(replace(approved_record, current_components=[]))
        _upsert_running_node(
            store=store,
            node_id="13",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="completed-split-durable",
            workspace_root=completed_split_effects.parent,
            final_effects_file=completed_split_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=30104,
            process_birth_time=1713000104.0,
            session_ids=["completed-split-session"],
            split_approved=1,
            result_commit="completed-split-result",
        )
        completed_split_record = store.load_node("13")
        if completed_split_record is None:
            return _fail("completed split record must exist for active-node-count testing")
        store.upsert_node(replace(completed_split_record, current_components=[]))
        active_count_prelude = prompt_core._build_kernel_review_prelude(
            request_seq=9,
            source_record=store.load_node("10"),
            split_bundle_ref="/tmp/frozen-active-count-bundle",
            durable_commit="active-commit",
            diff_fingerprint="active-diff",
        )
        if "Active system node count: 2" not in active_count_prelude:
            return _fail("kernel split-review prelude must count only running nodes plus pending split-review nodes as active")
        if "Open split-approved node count: 1" not in active_count_prelude:
            return _fail("kernel split-review prelude must count open split-approved parents separately from active nodes")

        ai_prompt = prompt_core._build_actor_prompt_text(
            node_id="prompt-node",
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            workspace_root=prompt_effects.parent,
            final_effects_file=prompt_effects,
            prelude_lines=[
                "Router evaluator lane dispatch.",
                "Evaluator task id: task-1",
                "Current dispatched snapshot id: snapshot-prompt",
                "Authoritative checker manifest path: /tmp/tasks.json",
                "Your only durable result path: /tmp/result.md",
            ],
        )
        if "=== ROUTER FEEDBACK SLOT ===" not in ai_prompt:
            return _fail("AI-user prompt must include the router feedback slot")
        if "Add at most 3 implied checks." not in ai_prompt:
            return _fail("AI-user prompt must cap implied checks explicitly")
        if "direct checks performed" not in ai_prompt:
            return _fail("AI-user prompt must require reporting direct checks performed")
        if "owner must be one of:" not in ai_prompt:
            return _fail("AI-user prompt must require owner tagging for each finding")
        if "The routed task entry is the only task truth for this lane." not in ai_prompt:
            return _fail("AI-user prompt must make the inline routed task entry the only lane truth")
        if "Missing weak-gate artifacts must not block by themselves." not in ai_prompt:
            return _fail("AI-user prompt must keep weak-gate misses non-blocking by default")
        if "Current dispatched snapshot id: <current snapshot id>" not in ai_prompt:
            return _fail("AI-user prompt must require writing the current dispatched snapshot id")
        if "Split-lifecycle rule:" not in ai_prompt or "historical `parent_node_id` ancestry rows" not in ai_prompt:
            return _fail("AI-user prompt must forbid inferring split from historical node topology")
        if "Add at most 3 implied checks." not in ai_prompt:
            return _fail("AI-user prompt must preserve the cap on implied checks after gate-policy edits")

        checker_prompt = prompt_core._build_actor_prompt_text(
            node_id="prompt-node",
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            workspace_root=prompt_effects.parent,
            final_effects_file=prompt_effects,
            prelude_lines=["checker resume line"],
        )
        if "=== ROUTER FEEDBACK SLOT ===" not in checker_prompt:
            return _fail("checker prompt must include the router feedback slot")
        if "checker/tasks.json is the sole durable evaluator task truth for this wave." not in checker_prompt:
            return _fail("checker prompt must pin checker/tasks.json as the sole durable task truth")
        if "Do not create parallel authoritative files for obligations, coverage, routing, disposition, manifests, checklists, summaries, certifications, or per-task markdown." not in checker_prompt:
            return _fail("checker prompt must reject parallel evaluator sidecar truth")
        if "do not silently narrow or strengthen the meaning of FINAL_EFFECTS.md" not in checker_prompt:
            return _fail("checker prompt must distinguish operational clarification from semantic narrowing")
        if "checker-derived task interpretation over-tightens FINAL_EFFECTS.md" not in checker_prompt:
            return _fail("checker prompt must preserve implementer rebuttal against checker over-tightening")
        if "If router prelude says the previous evaluator wave ended in EVALUATOR_FAULT" not in checker_prompt:
            return _fail("checker prompt must instruct fault-aware reruns after EVALUATOR_FAULT")
        if "do not reproduce the prior checker manifest blindly" not in checker_prompt:
            return _fail("checker prompt must forbid blind checker-manifest regeneration after evaluator fault")
        if "required_evidence" not in checker_prompt or "task_instructions" not in checker_prompt:
            return _fail("checker prompt must expose inline task instructions and required_evidence in tasks.json")

        reviewer_prompt = prompt_core._build_actor_prompt_text(
            node_id="prompt-node",
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            workspace_root=prompt_effects.parent,
            final_effects_file=prompt_effects,
            prelude_lines=["Router evaluator reviewer dispatch."],
        )
        if "feedback.json is the primary routing artifact." not in reviewer_prompt:
            return _fail("reviewer prompt must prioritize feedback.json over the prose report")
        if '"implementer": {' not in reviewer_prompt or '"ai_user": [' not in reviewer_prompt:
            return _fail("reviewer prompt must include the actor-bucketed feedback.json template")
        if '"version": 2' not in reviewer_prompt or '"snapshot_id": "snapshot-abc123"' not in reviewer_prompt:
            return _fail("reviewer prompt must document the version=2 snapshot-aware feedback schema")
        if "--feedback-ref <path/to/feedback.json>" not in reviewer_prompt:
            return _fail("reviewer prompt must document the feedback-ref submission interface explicitly")
        if "Treat any routerctl rejection as a hard validation failure." not in reviewer_prompt:
            return _fail("reviewer prompt must require strict handling of routerctl submission rejection")
        if '"blocking": false' not in reviewer_prompt:
            return _fail("reviewer prompt must include a blocking=false feedback.json example")
        if "Missing weak-gate artifacts must not block by themselves." not in reviewer_prompt:
            return _fail("reviewer prompt must keep weak-gate artifacts non-blocking by default")
        if "Record weak-gate findings in feedback.json only when they are useful advisory context" not in reviewer_prompt:
            return _fail("reviewer prompt must explain when weak-gate findings belong in feedback.json")
        if "Split-lifecycle rule:" not in reviewer_prompt or "Resolve split-related findings against current-node split lifecycle evidence" not in reviewer_prompt:
            return _fail("reviewer prompt must forbid carrying forward split findings from historical node topology alone")
        if "If an implementer explicitly argues that a checker-derived task or active finding over-tightens FINAL_EFFECTS.md, you must adjudicate that rebuttal explicitly." not in reviewer_prompt:
            return _fail("reviewer prompt must require explicit adjudication of implementer rebuttals")
        if "Determine whether the checker/task interpretation is an operational clarification or a semantic narrowing." not in reviewer_prompt:
            return _fail("reviewer prompt must distinguish operational clarification from semantic narrowing")
        if "Do not reject or fault an implementation merely because it violates an over-tightened checker/task interpretation" not in reviewer_prompt:
            return _fail("reviewer prompt must not treat unsupported checker narrowing as binding")

        split_truth_record = replace(
            pending_record,
            child_node_ids=["21", "22"],
        )
        lane_split_prelude = prompt_core._build_serial_task_prelude(
            record=split_truth_record,
            task_entry={"task_id": "task-1"},
        )
        if "Current node split_request: 1" not in lane_split_prelude:
            return _fail("AI-user launch prelude must inject current-node split_request truth")
        if "Current node split_approved: 0" not in lane_split_prelude:
            return _fail("AI-user launch prelude must inject current-node split_approved truth")
        if 'Current node child_node_ids: ["21", "22"]' not in lane_split_prelude:
            return _fail("AI-user launch prelude must inject current-node child_node_ids truth")

        reviewer_split_prelude = prompt_core._build_reviewer_prelude(split_truth_record)
        if "Current node split_request: 1" not in reviewer_split_prelude:
            return _fail("reviewer prelude must inject current-node split_request truth")
        if "Current node split_approved: 0" not in reviewer_split_prelude:
            return _fail("reviewer prelude must inject current-node split_approved truth")
        if 'Current node child_node_ids: ["21", "22"]' not in reviewer_split_prelude:
            return _fail("reviewer prelude must inject current-node child_node_ids truth")

        feedback_record = replace(
            split_truth_record,
            workspace_root=str(prompt_effects.parent),
        )
        prompt_core._materialize_reviewer_feedback_ledger(
            record=feedback_record,
            actor_attempt_count=1,
            verdict_kind="IMPLEMENTER_ACTION_REQUIRED",
            report_ref="/tmp/report.md",
            feedback_submission_ref=Path("/tmp/feedback.json"),
            feedback_submission={
                "version": 2,
                "snapshot_id": "snapshot-prompt",
                "implementer": {
                    "findings": [
                        {
                            "blocking": True,
                            "summary": "check this finding against current concrete evidence",
                            "evidence_ref": "/tmp/evidence.md",
                        }
                    ]
                },
                "checker": {"findings": []},
                "tester": {"findings": []},
                "ai_user": [],
            },
        )
        implementer_feedback_prelude = prompt_core._build_actor_feedback_prelude(
            feedback_record,
            actor_kind=ActorKind.IMPLEMENTER,
        )
        if not any(
            "Active feedback is routing context, not authoritative proof." in line
            for line in implementer_feedback_prelude
        ):
            return _fail("feedback prelude must tell actors that active feedback is routing context rather than blind authority")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        expected_wakeup_root = (Path.home() / ".codex" / "router_ipc").resolve()
        if wakeup_path.parent != expected_wakeup_root:
            return _fail("router wakeup socket path must live under ~/.codex/router_ipc")
        if len(str(wakeup_path)) >= 100:
            return _fail("router wakeup socket path must stay short enough for AF_UNIX limits")
        store = RouterStore(db_path)
        store.write_kernel_session_id("kernel-session-001")

        node1_final_effects, node1_commit = _init_repo_with_final_effects(
            tmp / "workspace" / "node-1",
            "Parent node 1 work.\n",
        )
        node2_final_effects, node2_commit = _init_repo_with_final_effects(
            tmp / "workspace" / "node-2",
            "Parent node 2 work.\n",
        )
        bundle1 = _write_split_bundle(tmp / "bundles" / "split-1", child_names=["child_1"])
        bundle2 = _write_split_bundle(
            tmp / "bundles" / "split-2",
            child_names=["child_1", "child_2"],
        )

        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=3,
            durable_commit=node1_commit,
            workspace_root=node1_final_effects.parent,
            final_effects_file=node1_final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=11111,
            process_birth_time=1711965600.0,
            session_ids=["session-1"],
            split_request=1,
        )
        _upsert_running_node(
            store=store,
            node_id="2",
            parent_node_id="0",
            attempt_count=4,
            durable_commit=node2_commit,
            workspace_root=node2_final_effects.parent,
            final_effects_file=node2_final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=22222,
            process_birth_time=1711965700.0,
            session_ids=["session-2"],
            split_request=1,
        )

        launch_metas: list[dict[str, object]] = []
        actor_launch_specs: list[object] = []
        handled: list[tuple[int, str]] = []
        fake_proc = _FakeProcSupervisor()
        fake_proc.mark_live(
            ActorRef(node_id="1", actor_kind=ActorKind.IMPLEMENTER, attempt_count=3)
        )
        fake_proc.mark_live(
            ActorRef(node_id="2", actor_kind=ActorKind.IMPLEMENTER, attempt_count=4)
        )

        def _actor_launcher(spec) -> ActorLaunchResult:
            actor_launch_specs.append(spec)
            meta = {
                "actor_kind": spec.actor_kind,
                "node_id": spec.node_id,
                "attempt_count": spec.attempt_count,
                "pid": 30000 + len(actor_launch_specs),
                "process_birth_time": 1713000000.0 + len(actor_launch_specs),
            }
            launch_metas.append(meta)
            return ActorLaunchResult(
                process=_LiveProcess(pid=int(meta["pid"])),
                process_birth_time=float(meta["process_birth_time"]),
                session_id=f"child-session-{len(actor_launch_specs)}",
                rollout_path=(tmp / "runtime" / f"child-{spec.node_id}.jsonl").resolve(),
            )

        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            placeholder_observer=lambda item: handled.append((item.seq, item.event_type)),
            proc_supervisor=fake_proc,
            actor_launcher=_actor_launcher,
        )
        core.start()
        try:
            implementer1 = ActorRef(node_id="1", actor_kind=ActorKind.IMPLEMENTER, attempt_count=3)
            implementer2 = ActorRef(node_id="2", actor_kind=ActorKind.IMPLEMENTER, attempt_count=4)

            seq1 = store.append_event(
                RequestSplit(
                    actor=implementer1,
                    split_bundle_ref=str(bundle1),
                    durable_commit=node1_commit,
                    diff_fingerprint="fingerprint-001",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: len(
                    [spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.KERNEL]
                )
                == 1,
                timeout_seconds=2.0,
            ):
                return _fail("core must dispatch the first split review through the node-0 kernel actor")
            first_kernel_spec = next(
                spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.KERNEL
            )
            if first_kernel_spec.node_id != "0":
                return _fail("kernel review launch must use node 0")
            if str(first_kernel_spec.env.get("LOOP_REQUEST_SEQ") or "") != str(seq1):
                return _fail("kernel review launch must inject the pending request_seq into the kernel actor env")
            first_kernel_prompt = str(first_kernel_spec.prompt_text)
            if f"Parent workspace root: {node1_final_effects.parent.resolve()}" not in first_kernel_prompt:
                return _fail("kernel review launch must inject the concrete parent workspace root into the prompt")
            if f"Parent final effects file: {node1_final_effects.resolve()}" not in first_kernel_prompt:
                return _fail("kernel review launch must inject the concrete parent final effects file into the prompt")
            if "Active system node count: 2" not in first_kernel_prompt:
                return _fail("kernel review launch must inject the current active system node count into the prompt")
            if "Open split-approved node count: 0" not in first_kernel_prompt:
                return _fail("kernel review launch must inject the current open split-approved node count into the prompt")

            duplicate_seq1 = store.append_event(
                RequestSplit(
                    actor=implementer1,
                    split_bundle_ref=str(bundle1),
                    durable_commit=node1_commit,
                    diff_fingerprint="fingerprint-001-duplicate",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            time.sleep(0.05)
            if len([spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.KERNEL]) != 1:
                return _fail("core must ignore duplicate RequestSplit while the kernel actor is already running")
            if (store.load_node("1") or _sentinel()).split_request != 1:
                return _fail("duplicate RequestSplit must not disturb the existing pending split_request state")

            seq2 = store.append_event(
                RequestSplit(
                    actor=implementer2,
                    split_bundle_ref=str(bundle2),
                    durable_commit=node2_commit,
                    diff_fingerprint="fingerprint-002",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: (store.load_node("2") or _sentinel()).split_request == 1,
                timeout_seconds=2.0,
            ):
                return _fail("second RequestSplit must preserve split_request=1 while waiting in FIFO")
            if len([spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.KERNEL]) != 1:
                return _fail("core must keep one active kernel actor at a time")

            first_kernel_meta = next(
                meta for meta in launch_metas if meta["actor_kind"] is ActorKind.KERNEL
            )
            reject_reason = _write_file(tmp / "workspace" / "node-1" / "reject-001.md", "reject reason\n")
            _write_completion(
                actor_completion_record_path(node1_final_effects.parent, ActorKind.KERNEL),
                {
                    "version": 1,
                    "node_id": "0",
                    "actor_kind": ActorKind.KERNEL.value,
                    "attempt_count": int(first_kernel_meta["attempt_count"]),
                    "pid": int(first_kernel_meta["pid"]),
                    "process_birth_time": float(first_kernel_meta["process_birth_time"]),
                    "completed_at": now.isoformat(),
                    "request_seq": int(seq1),
                    "verdict_kind": "REJECT",
                    "reason_ref": str(reject_reason.resolve()),
                },
            )
            reject_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id=KERNEL_NODE_ID,
                        actor_kind=ActorKind.KERNEL,
                        attempt_count=int(first_kernel_meta["attempt_count"]),
                    ),
                    pid=int(first_kernel_meta["pid"]),
                    process_birth_time=float(first_kernel_meta["process_birth_time"]),
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: (store.load_node("1") or _sentinel()).split_request == 0,
                timeout_seconds=2.0,
            ):
                return _fail("kernel reject completion must clear node split_request")
            node1 = store.load_node("1")
            if node1 is None or node1.last_rejected_split_diff_fingerprint != "fingerprint-001":
                return _fail("kernel reject completion must write back the original RequestSplit diff_fingerprint")
            if not _wait_until(
                lambda: len(
                    [spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.KERNEL]
                )
                == 2,
                timeout_seconds=2.0,
            ):
                return _fail("kernel reject completion must free the lane and dispatch the next split")

            second_kernel_meta = [
                meta for meta in launch_metas if meta["actor_kind"] is ActorKind.KERNEL
            ][1]
            _write_completion(
                actor_completion_record_path(node2_final_effects.parent, ActorKind.KERNEL),
                {
                    "version": 1,
                    "node_id": "0",
                    "actor_kind": ActorKind.KERNEL.value,
                    "attempt_count": int(second_kernel_meta["attempt_count"]),
                    "pid": int(second_kernel_meta["pid"]),
                    "process_birth_time": float(second_kernel_meta["process_birth_time"]),
                    "completed_at": now.isoformat(),
                    "request_seq": int(seq2),
                    "verdict_kind": "APPROVE",
                },
            )
            approve_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id=KERNEL_NODE_ID,
                        actor_kind=ActorKind.KERNEL,
                        attempt_count=int(second_kernel_meta["attempt_count"]),
                    ),
                    pid=int(second_kernel_meta["pid"]),
                    process_birth_time=float(second_kernel_meta["process_birth_time"]),
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: (store.load_node("2") or _sentinel()).split_approved == 1,
                timeout_seconds=2.0,
            ):
                return _fail("kernel approve completion must set split_approved=1 on the parent node")
            node2 = store.load_node("2")
            if node2 is None:
                return _fail("approved parent node must still be loadable")
            if node2.child_node_ids != ["3", "4"]:
                return _fail("kernel approve completion must materialize sequential child node ids")
            child_launch_specs = [
                spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.IMPLEMENTER and spec.node_id in {"3", "4"}
            ]
            if not _wait_until(
                lambda: len(
                    [
                        spec
                        for spec in actor_launch_specs
                        if spec.actor_kind is ActorKind.IMPLEMENTER and spec.node_id in {"3", "4"}
                    ]
                )
                == 2,
                timeout_seconds=2.0,
            ):
                return _fail("kernel approve completion must launch one child implementer per bundle entry")
            child_launch_specs = [
                spec for spec in actor_launch_specs if spec.actor_kind is ActorKind.IMPLEMENTER and spec.node_id in {"3", "4"}
            ]
            if child_launch_specs[0].node_id != "3" or child_launch_specs[0].parent_node_id != "2":
                return _fail("first child launch must use parent node 2 and node id 3")
            if child_launch_specs[1].node_id != "4" or child_launch_specs[1].parent_node_id != "2":
                return _fail("second child launch must use parent node 2 and node id 4")
            expected_child3_effects = (
                Path(child_launch_specs[0].workspace_root) / "FINAL_EFFECTS.md"
            ).resolve()
            if child_launch_specs[0].final_effects_file != expected_child3_effects:
                return _fail("child launch must remap the child-specific final_effects_file into the child git worktree")
            child3 = store.load_node("3")
            child4 = store.load_node("4")
            if child3 is None or child4 is None:
                return _fail("ApproveSplit must persist materialized child nodes")
            if child3.parent_node_id != "2" or child4.parent_node_id != "2":
                return _fail("materialized child nodes must point back to the approved parent")
            expected_child3_effects = (Path(child3.workspace_root) / "children" / "child_1" / "FINAL_EFFECTS.md").resolve()
            expected_child4_effects = (Path(child4.workspace_root) / "children" / "child_2" / "FINAL_EFFECTS.md").resolve()
            if child3.final_effects_file != str(expected_child3_effects):
                return _fail("materialized child node must remap child final_effects_file into the child git worktree")
            if child4.final_effects_file != str(expected_child4_effects):
                return _fail("materialized child node must remap child final_effects_file into the child git worktree")
            if _run_git(Path(child3.workspace_root), "rev-parse", "HEAD") != node2_commit:
                return _fail("materialized child node must start from the approved parent's durable baseline commit")
            if _run_git(Path(child4.workspace_root), "rev-parse", "HEAD") != node2_commit:
                return _fail("materialized child node must start from the approved parent's durable baseline commit")

            out1 = store.append_event(
                OutputWindow(
                    actor=implementer2,
                    had_output=True,
                    observed_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            out2 = store.append_event(
                OutputWindow(
                    actor=implementer2,
                    had_output=True,
                    observed_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: any(
                    actor.node_id == "2"
                    and actor.actor_kind is ActorKind.IMPLEMENTER
                    and int(actor.attempt_count) == 4
                    for actor in fake_proc.terminated
                ),
                timeout_seconds=2.0,
            ):
                return _fail("split-approved parent must be force-terminated after two output windows")

            handled_event_types = [event_type for _seq, event_type in handled]
            if handled_event_types != [
                "RequestSplit",
                "RequestSplit",
                "RequestSplit",
                "ProcessExitedObserved",
                "RejectSplit",
                "ProcessExitedObserved",
                "ApproveSplit",
                "OutputWindow",
                "OutputWindow",
            ]:
                return _fail("core must still process split lifecycle items in durable seq order")
        finally:
            core.stop()
        if wakeup_path.exists():
            return _fail("core stop must clean up the wakeup socket path")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        no_progress_effects = _write_final_effects(
            tmp / "workspace" / "node-19" / "FINAL_EFFECTS.md",
            "No-progress node 19.\n",
        )
        fake_proc = _FakeProcSupervisor()
        fake_proc.mark_live(
            ActorRef(node_id="19", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )
        _upsert_running_node(
            store=store,
            node_id="19",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="np19",
            workspace_root=no_progress_effects.parent,
            final_effects_file=no_progress_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=19191,
            process_birth_time=1711966750.0,
            session_ids=["session-19"],
            workspace_fingerprint_before=workspace_output_fingerprint(no_progress_effects.parent),
        )
        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
        )
        core.start()
        try:
            actor19 = ActorRef(node_id="19", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
            last_seq = 0
            for _index in range(5):
                last_seq = store.append_event(
                    OutputWindow(
                        actor=actor19,
                        had_output=False,
                        observed_at=now,
                    ),
                    recorded_at=now,
                )
                notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: actor19 in fake_proc.terminated,
                timeout_seconds=2.0,
            ):
                return _fail("five consecutive no-progress windows must terminate the active actor")
            node19 = store.load_node("19")
            if node19 is None:
                return _fail("no-progress node must remain durable after termination request")
            component19 = node19.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
            if component19 is None or int(component19.consecutive_no_progress) != 5:
                return _fail("no-progress tracking must durably accumulate across OutputWindow events")
            if not _wait_until(lambda: store.read_last_applied_seq() == last_seq, timeout_seconds=2.0):
                return _fail("no-progress OutputWindow events must still advance last_applied_seq")
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        checker_final_effects = _write_final_effects(
            tmp / "workspace" / "node-12b" / "FINAL_EFFECTS.md",
            "Checker node 12b.\n",
        )
        duplicate_checker_tasks_ref = _write_file(
            checker_current_tasks_path(checker_final_effects.parent, "12b"),
            json.dumps(
                {
                    "version": 2,
                    "snapshot_id": "",
                    "workspace_root": str(checker_final_effects.parent),
                    "authoritative_final_effects_path": str(checker_final_effects),
                    "obligations": [
                        {
                            "id": "req-1",
                            "section": "Main",
                            "requirement_text": "Checker node 12b.",
                        }
                    ],
                    "tasks": [
                        {
                            "task_id": "dup-task",
                            "covered_requirement_ids": ["req-1"],
                            "goal": "First duplicate task",
                            "blocking_condition": "Block if duplicate A fails.",
                            "task_instructions": "Duplicate task A.",
                            "required_evidence": [],
                        },
                        {
                            "task_id": "dup-task",
                            "covered_requirement_ids": ["req-1"],
                            "goal": "Second duplicate task",
                            "blocking_condition": "Block if duplicate B fails.",
                            "task_instructions": "Duplicate task B.",
                            "required_evidence": [],
                        },
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _write_file(tmp / "workspace" / "node-12b" / "deliverable.md", "changed\n")
        _upsert_running_node(
            store=store,
            node_id="12b",
            parent_node_id="0",
            attempt_count=2,
            durable_commit="checker12b",
            workspace_root=checker_final_effects.parent,
            final_effects_file=checker_final_effects,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            pid=12122,
            process_birth_time=1711966001.0,
            session_ids=["checker-session-12b"],
            workspace_fingerprint_before="before-checker-12b",
        )
        checker_record = store.load_node("12b")
        if checker_record is None:
            return _fail("checker test node 12b must be loadable")
        store.upsert_node(
            replace(
                checker_record,
                current_components=[
                    ActorRef(
                        node_id="12b",
                        actor_kind=ActorKind.EVALUATOR_CHECKER,
                        attempt_count=2,
                    )
                ],
                evaluator_phase="checker",
            )
        )
        _write_file(
            actor_completion_record_path(
                checker_final_effects.parent,
                ActorKind.EVALUATOR_CHECKER,
                node_id="12b",
            ),
            json.dumps(
                {
                    "version": 1,
                    "node_id": "12b",
                    "actor_kind": "evaluator_checker",
                    "attempt_count": 2,
                    "pid": 12122,
                    "process_birth_time": 1711966001.0,
                    "completed_at": now.isoformat(),
                    "tasks_ref": str(duplicate_checker_tasks_ref.resolve()),
                },
                indent=2,
                sort_keys=True,
            ),
        )
        checker_launch_specs = []

        def _duplicate_checker_launcher(spec) -> ActorLaunchResult:
            checker_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=43002),
                process_birth_time=1714300001.0,
                session_id="checker-recovery-session-12b",
                rollout_path=(tmp / "runtime" / "checker-recovery-12b.jsonl").resolve(),
            )

        duplicate_proc = _FakeProcSupervisor()
        duplicate_proc.mark_live(
            ActorRef(node_id="12b", actor_kind=ActorKind.EVALUATOR_CHECKER, attempt_count=2)
        )
        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=duplicate_proc,
            actor_launcher=_duplicate_checker_launcher,
        )
        core.start()
        try:
            exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="12b",
                        actor_kind=ActorKind.EVALUATOR_CHECKER,
                        attempt_count=2,
                    ),
                    pid=12122,
                    process_birth_time=1711966001.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == exit_seq, timeout_seconds=2.0):
                return _fail("duplicate checker task ids must still advance last_applied_seq under the unified inbox boundary")
            if not _wait_until(lambda: len(checker_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("duplicate checker task ids must relaunch evaluator_checker")
            if "must not reuse task_id" not in checker_launch_specs[0].prompt_text:
                return _fail("checker recovery prompt must explain duplicate task_id rejection")
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        restart_effects = _write_final_effects(
            tmp / "workspace" / "node-20" / "FINAL_EFFECTS.md",
            "Restart node 20.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="20",
            parent_node_id="0",
            attempt_count=4,
            durable_commit="restart20",
            workspace_root=restart_effects.parent,
            final_effects_file=restart_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=20202,
            process_birth_time=1711965790.0,
            session_ids=["session-20"],
            workspace_fingerprint_before=workspace_output_fingerprint(restart_effects.parent),
        )
        fake_proc = _FakeProcSupervisor()
        recovered_specs: list[object] = []

        def _restart_recovery_launcher(spec) -> ActorLaunchResult:
            recovered_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=42020),
                process_birth_time=1714202000.0,
                session_id="restart-recovered-20",
                rollout_path=(tmp / "runtime" / "restart-20.jsonl").resolve(),
            )

        restart_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_restart_recovery_launcher,
        )
        restart_core.start()
        try:
            if not _wait_until(lambda: len(recovered_specs) == 1, timeout_seconds=2.0):
                return _fail("router startup must recover an unobserved RUNNING actor through the existing after-exit path")
            recovered_spec = recovered_specs[0]
            if recovered_spec.node_id != "20" or recovered_spec.attempt_count != 5:
                return _fail("startup recovery must relaunch the same node with incremented attempt_count")
            if "no longer under proc supervision" not in recovered_spec.prompt_text:
                return _fail("startup recovery prompt must explain why the previous RUNNING actor was recovered")
            node20 = store.load_node("20")
            if node20 is None:
                return _fail("startup recovery node must remain durable")
            component20 = node20.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
            if component20 is None or int(component20.attempt_count) != 5:
                return _fail("startup recovery must durably persist the relaunched implementer attempt")
        finally:
            restart_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        recover_final_effects = _write_final_effects(
            tmp / "workspace" / "node-9" / "FINAL_EFFECTS.md",
            "Recover node 9.\n",
        )
        fake_proc = _FakeProcSupervisor()
        recovered_specs: list[object] = []

        def _recover_launcher(spec) -> ActorLaunchResult:
            recovered_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=40001),
                process_birth_time=1714000000.0,
                session_id="recovered-session-001",
                rollout_path=(tmp / "runtime" / "recovered.jsonl").resolve(),
            )

        _upsert_running_node(
            store=store,
            node_id="9",
            parent_node_id="0",
            attempt_count=5,
            durable_commit="xyz999",
            workspace_root=recover_final_effects.parent,
            final_effects_file=recover_final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=99999,
            process_birth_time=1711965800.0,
            session_ids=["session-9"],
            split_request=1,
        )

        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_recover_launcher,
        )
        fake_proc.mark_live(
            ActorRef(node_id="9", actor_kind=ActorKind.IMPLEMENTER, attempt_count=5)
        )
        core.start()
        try:
            request_seq = store.append_event(
                RequestSplit(
                    actor=ActorRef(
                        node_id="9",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=5,
                    ),
                    split_bundle_ref=str(_write_split_bundle(tmp / "bundles" / "recover-split", child_names=["child_1"])),
                    durable_commit="xyz999",
                    diff_fingerprint="retry-fingerprint",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            fake_proc.live_actors.discard(
                ActorRef(node_id="9", actor_kind=ActorKind.IMPLEMENTER, attempt_count=5)
            )
            reject_seq = store.append_event(
                RejectSplit(
                    actor=kernel,
                    target_node_id="9",
                    request_seq=request_seq,
                    rejected_at=now,
                    reason_ref="/tmp/reject-recover.md",
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: len(recovered_specs) == 1, timeout_seconds=2.0):
                return _fail("RejectSplit must recover a dead implementer through the generic actor path")
            recovered_spec = recovered_specs[0]
            if recovered_spec.node_id != "9" or recovered_spec.attempt_count != 6:
                return _fail("recovered implementer must relaunch on the same node with incremented attempt_count")
            if "Do not request split again until there is new effective git diff progress." not in recovered_spec.prompt_text:
                return _fail("recovery prompt must remind the child not to repeat split without new progress")
            recovered_record = store.load_node("9")
            if recovered_record is None:
                return _fail("recovered node must remain loadable")
            recovered_component = recovered_record.components[component_key(actor_kind=ActorKind.IMPLEMENTER)]
            if recovered_component.attempt_count != 6:
                return _fail("recovered implementer component must persist the new attempt_count")
            if recovered_component.session_ids != ["session-9", "recovered-session-001"]:
                return _fail("recovered implementer component must append the new session id")
            if recovered_record.last_rejected_split_diff_fingerprint != "retry-fingerprint":
                return _fail("RejectSplit recovery path must still persist the rejected diff fingerprint")
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        retry_final_effects = _write_final_effects(
            Path(tmpdir) / "workspace" / "node-11" / "FINAL_EFFECTS.md",
            "Retry node 11.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="11",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="retry111",
            workspace_root=retry_final_effects.parent,
            final_effects_file=retry_final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=11111,
            process_birth_time=1711965900.0,
            session_ids=["session-11"],
            split_request=1,
        )

        retry_launch_specs: list[object] = []
        retry_launch_meta: list[dict[str, object]] = []

        def _retry_actor_launcher(spec) -> ActorLaunchResult:
            retry_launch_specs.append(spec)
            meta = {
                "actor_kind": spec.actor_kind,
                "attempt_count": spec.attempt_count,
                "pid": 41000 + len(retry_launch_specs),
                "process_birth_time": 1714100000.0 + len(retry_launch_specs),
            }
            retry_launch_meta.append(meta)
            return ActorLaunchResult(
                process=_LiveProcess(pid=int(meta["pid"])),
                process_birth_time=float(meta["process_birth_time"]),
                session_id=f"retry-session-{len(retry_launch_specs)}",
                rollout_path=(Path(tmpdir) / "runtime" / f"retry-{spec.node_id}-{spec.attempt_count}.jsonl").resolve(),
            )

        retry_proc = _FakeProcSupervisor()
        retry_proc.mark_live(
            ActorRef(node_id="11", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )
        retry_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=retry_proc,
            actor_launcher=_retry_actor_launcher,
        )
        retry_core.start()
        try:
            retry_seq = store.append_event(
                RequestSplit(
                    actor=ActorRef(
                        node_id="11",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    ),
                    split_bundle_ref=str(_write_split_bundle(Path(tmpdir) / "bundles" / "retry", child_names=["child_1"])),
                    durable_commit="retry111",
                    diff_fingerprint="retry-fingerprint-11",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: len([spec for spec in retry_launch_specs if spec.actor_kind is ActorKind.KERNEL]) == 1,
                timeout_seconds=2.0,
            ):
                return _fail("pending split review must launch the first kernel actor attempt")
            for expected_attempt in (1, 2, 3):
                kernel_meta = [
                    meta for meta in retry_launch_meta if meta["actor_kind"] is ActorKind.KERNEL
                ][expected_attempt - 1]
                exit_seq = store.append_event(
                    ProcessExitedObserved(
                        actor=ActorRef(
                            node_id=KERNEL_NODE_ID,
                            actor_kind=ActorKind.KERNEL,
                            attempt_count=int(kernel_meta["attempt_count"]),
                        ),
                        pid=int(kernel_meta["pid"]),
                        process_birth_time=float(kernel_meta["process_birth_time"]),
                        exit_code=1,
                        signal_name=None,
                        occurred_at=now,
                    ),
                    recorded_at=now,
                )
                notify_router_wakeup(wakeup_path)
                if not _wait_until(lambda: store.read_last_applied_seq() == exit_seq, timeout_seconds=2.0):
                    return _fail("kernel failed exits must still advance last_applied_seq")
                if expected_attempt < 3:
                    if not _wait_until(
                        lambda: len([spec for spec in retry_launch_specs if spec.actor_kind is ActorKind.KERNEL]) == expected_attempt + 1,
                        timeout_seconds=2.0,
                    ):
                        return _fail("kernel failed exits before threshold must relaunch the same pending request")
            if not _wait_until(
                lambda: (store.load_node("11") or _sentinel()).split_request == 0,
                timeout_seconds=2.0,
            ):
                return _fail("kernel failed exit exhaustion must eventually clear split_request")
        finally:
            retry_core.stop()
        if wakeup_path.exists():
            return _fail("retrying core stop must clean up the wakeup socket path")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        evaluation_effects = _write_final_effects(
            tmp / "workspace" / "node-14" / "FINAL_EFFECTS.md",
            "Evaluate node 14.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="14",
            parent_node_id="0",
            attempt_count=7,
            durable_commit="eval14",
            workspace_root=evaluation_effects.parent,
            final_effects_file=evaluation_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=14141,
            process_birth_time=1711966200.0,
            session_ids=["implementer-session-14"],
        )

        def _failing_checker_launcher(spec) -> ActorLaunchResult:
            if spec.actor_kind is ActorKind.EVALUATOR_CHECKER:
                raise RuntimeError("checker launch failed")
            return ActorLaunchResult(
                process=_LiveProcess(pid=41001),
                process_birth_time=1714100000.0,
                session_id="fallback-session-14",
                rollout_path=(tmp / "runtime" / "fallback-node-14.jsonl").resolve(),
            )

        pending_proc = _FakeProcSupervisor()
        pending_proc.mark_live(
            ActorRef(node_id="14", actor_kind=ActorKind.IMPLEMENTER, attempt_count=7)
        )
        pending_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=pending_proc,
            actor_launcher=_failing_checker_launcher,
        )
        pending_core.start()
        try:
            _write_file(
                evaluation_effects.parent / "deliverable.md",
                "node 14 implementer output\n",
            )
            _write_completion(
                actor_completion_record_path(
                    evaluation_effects.parent,
                    ActorKind.IMPLEMENTER,
                    node_id="14",
                ),
                {
                    "version": 1,
                    "node_id": "14",
                    "actor_kind": ActorKind.IMPLEMENTER.value,
                    "attempt_count": 7,
                    "pid": 14141,
                    "process_birth_time": 1711966200.0,
                    "completed_at": now.isoformat(),
                },
            )
            eval_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="14",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=7,
                    ),
                    pid=14141,
                    process_birth_time=1711966200.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == eval_seq, timeout_seconds=2.0):
                return _fail("implementer completion failure path must still advance last_applied_seq")
            pending_record = store.load_node("14")
            if pending_record is None:
                return _fail("failed checker auto-launch node must remain loadable")
            if pending_record.current_components:
                return _fail("failed checker auto-launch must leave current_components empty")
            if pending_record.evaluator_phase != "checker":
                return _fail("failed checker auto-launch must durably persist evaluator_phase=checker")
            implementer_component = pending_record.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
            if implementer_component is None or implementer_component.status.value != "completed":
                return _fail("failed checker auto-launch must durably mark implementer completed before checker launch")
        finally:
            pending_core.stop()

        reconcile_launch_specs: list[object] = []

        def _working_checker_launcher(spec) -> ActorLaunchResult:
            reconcile_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=41002),
                process_birth_time=1714100001.0,
                session_id=f"reconcile-{spec.actor_kind.value}-14",
                rollout_path=(tmp / "runtime" / f"reconcile-{spec.actor_kind.value}-14.jsonl").resolve(),
            )

        reconcile_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            actor_launcher=_working_checker_launcher,
        )
        reconcile_core.start()
        try:
            if not _wait_until(
                lambda: (
                    len((store.load_node("14") or _sentinel()).current_components) == 1
                    and (store.load_node("14") or _sentinel()).current_components[0].actor_kind
                    is ActorKind.EVALUATOR_CHECKER
                ),
                timeout_seconds=2.0,
            ):
                return _fail("core restart must reconcile a durably pending checker launch")
            if not reconcile_launch_specs:
                return _fail("checker reconcile launch must capture a launch spec")
            checker_prompt = str(reconcile_launch_specs[0].prompt_text)
            if "You are the LOOP evaluator_checker." not in checker_prompt:
                return _fail("checker prompt must use the checker-specific header")
            if "Do not use planning tools, todo tools, or progress-commentary updates." not in checker_prompt:
                return _fail("checker prompt must suppress generic planning/commentary behavior for checker")
            if "You are planning later evaluator work; you are not performing the substantive audit now." not in checker_prompt:
                return _fail("checker prompt must define checker as a planner rather than a full evaluator")
            if "Obligation extraction rule: each bullet or numbered item becomes one obligation; each prose paragraph outside lists becomes one obligation." not in checker_prompt:
                return _fail("checker prompt must make obligation extraction near-mechanical")
            if "If evidence is incomplete, encode the uncertainty inside a task goal or blocking_condition instead of broadening the scan." not in checker_prompt:
                return _fail("checker prompt must prefer tasking uncertainty over extra investigation")
            if "checker/tasks.json is the sole durable evaluator task truth for this wave." not in checker_prompt:
                return _fail("checker prompt must make checker/tasks.json the sole durable evaluator truth")
            if "Do not create parallel authoritative files for obligations, coverage, routing, disposition, manifests, checklists, summaries, certifications, or per-task markdown." not in checker_prompt:
                return _fail("checker prompt must reject parallel evaluator sidecar truth")
            if "do not silently narrow or strengthen the meaning of FINAL_EFFECTS.md" not in checker_prompt:
                return _fail("checker prompt must separate operational clarification from semantic narrowing")
            if "checker-derived task interpretation over-tightens FINAL_EFFECTS.md" not in checker_prompt:
                return _fail("checker prompt must preserve authority-grounded implementer rebuttals")
            if "Do not require an md/json pair by default." not in checker_prompt:
                return _fail("checker prompt must avoid default md/json artifact pairs")
            if "Do not use task_ref indirection." not in checker_prompt:
                return _fail("checker prompt must remove task_ref indirection")
            if "task_instructions" not in checker_prompt or "required_evidence" not in checker_prompt:
                return _fail("checker prompt must inline task instructions and required evidence into tasks.json")
            if "Prefer 4-12 tasks unless more are required to avoid semantic omission." not in checker_prompt:
                return _fail("checker prompt must bias checker away from both umbrella tasks and runaway task counts")
            if "If all finish-gate conditions hold, stop immediately." not in checker_prompt:
                return _fail("checker prompt must tell checker to stop immediately after passing the finish gate")
        finally:
            reconcile_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        parent_effects, parent_commit = _init_repo_with_final_effects(
            tmp / "workspace" / "node-15",
            "Parent node 15 split.\n",
        )
        split_bundle = _write_split_bundle(tmp / "bundles" / "split-15", child_names=["child_1"])
        _upsert_running_node(
            store=store,
            node_id="15",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=parent_commit,
            workspace_root=parent_effects.parent,
            final_effects_file=parent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=15151,
            process_birth_time=1711966300.0,
            session_ids=["implementer-session-15"],
            split_request=1,
        )

        def _failing_child_launcher(spec) -> ActorLaunchResult:
            if spec.node_id != "15":
                raise RuntimeError("child launch failed")
            return ActorLaunchResult(
                process=_LiveProcess(pid=42001),
                process_birth_time=1714200000.0,
                session_id="parent-session-15",
                rollout_path=(tmp / "runtime" / "parent-15.jsonl").resolve(),
            )

        child_fail_proc = _FakeProcSupervisor()
        child_fail_proc.mark_live(
            ActorRef(node_id="15", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )
        child_fail_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=child_fail_proc,
            actor_launcher=_failing_child_launcher,
        )
        child_fail_core.start()
        try:
            request_seq = store.append_event(
                RequestSplit(
                    actor=ActorRef(
                        node_id="15",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    ),
                    split_bundle_ref=str(split_bundle),
                    durable_commit=parent_commit,
                    diff_fingerprint="split15-fingerprint",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            approve_seq = store.append_event(
                ApproveSplit(
                    actor=kernel,
                    target_node_id="15",
                    request_seq=request_seq,
                    approved_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == approve_seq, timeout_seconds=2.0):
                return _fail("ApproveSplit failure path must still advance last_applied_seq")
            parent_record = store.load_node("15")
            if parent_record is None:
                return _fail("approved parent must remain loadable after child launch failure")
            if parent_record.split_approved != 1 or parent_record.child_node_ids != ["16"]:
                return _fail("ApproveSplit child-launch failure must still durably preserve approved parent + child ids")
            child_record = store.load_node("16")
            if child_record is None:
                return _fail("ApproveSplit child-launch failure must still durably materialize child node")
            if child_record.current_components:
                return _fail("child launch failure must leave child in pending state with no current actors")
            if child_record.components:
                return _fail("child launch failure must not fake a running component state")
            if child_record.durable_commit != parent_commit:
                return _fail("approved child node must inherit the parent's durable git baseline commit")
        finally:
            child_fail_core.stop()

        def _working_child_launcher(spec) -> ActorLaunchResult:
            return ActorLaunchResult(
                process=_LiveProcess(pid=42002),
                process_birth_time=1714200001.0,
                session_id=f"reconcile-{spec.node_id}",
                rollout_path=(tmp / "runtime" / f"child-{spec.node_id}.jsonl").resolve(),
            )

        child_reconcile_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            actor_launcher=_working_child_launcher,
        )
        child_reconcile_core.start()
        try:
            if not _wait_until(
                lambda: (
                    len((store.load_node("16") or _sentinel()).current_components) == 1
                    and (store.load_node("16") or _sentinel()).current_components[0].actor_kind
                    is ActorKind.IMPLEMENTER
                ),
                timeout_seconds=2.0,
            ):
                return _fail("core restart must reconcile a durably pending child implementer launch")
        finally:
            child_reconcile_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        parent_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Parent node 19.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add parent final effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="19",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=parent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=19001,
            process_birth_time=1711900000.0,
            session_ids=["session-19"],
            split_request=1,
        )
        split_bundle = _write_split_bundle(tmp / "bundles" / "git-child-19", child_names=["child_git"])
        request_seq = store.append_event(
            RequestSplit(
                actor=ActorRef(
                    node_id="19",
                    actor_kind=ActorKind.IMPLEMENTER,
                    attempt_count=1,
                ),
                split_bundle_ref=str(split_bundle),
                durable_commit=base_commit,
                diff_fingerprint="split19-fingerprint",
                requested_at=now,
            ),
            recorded_at=now,
        )
        materialize_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        parent_record = store.load_node("19")
        if parent_record is None:
            return _fail("git child materialization parent must remain loadable")
        updated_parent, new_child_records = materialize_core._materialize_split_children(
            parent_record=replace(
                parent_record,
                split_request=0,
                split_approved=1,
                approved_split_request_seq=request_seq,
            ),
            request_seq=request_seq,
        )
        if updated_parent.child_node_ids != ["20"]:
            return _fail("git child materialization must still allocate the next child id deterministically")
        if len(new_child_records) != 1:
            return _fail("git child materialization must create exactly one new child record")
        child_record = store.load_node("20")
        if child_record is None:
            return _fail("git child materialization must durably upsert the new child node")
        if child_record.workspace_root == str((split_bundle / "children" / "child_git").resolve()):
            return _fail("child workspace_root must not reuse the frozen split bundle directory as a mutable workspace")
        if _run_git(Path(child_record.workspace_root), "rev-parse", "HEAD") != base_commit:
            return _fail("child workspace_root must be a git worktree rooted at the parent durable baseline commit")
        expected_child_effects = (
            Path(child_record.workspace_root) / "children" / "child_git" / "FINAL_EFFECTS.md"
        ).resolve()
        if child_record.final_effects_file != str(expected_child_effects):
            return _fail("child final_effects_file must be remapped into the child git worktree")
        if expected_child_effects.read_text(encoding="utf-8") != (
            split_bundle / "children" / "child_git" / "FINAL_EFFECTS.md"
        ).read_text(encoding="utf-8"):
            return _fail("child git worktree must receive the child-local frozen handoff file content")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        approve_final_effects, approve_commit = _init_repo_with_final_effects(
            tmp / "workspace" / "node-21",
            "Approve node 21.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="21",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=approve_commit,
            workspace_root=approve_final_effects.parent,
            final_effects_file=approve_final_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=21001,
            process_birth_time=1712100000.0,
            session_ids=["session-21"],
            split_request=1,
        )
        request_seq = store.append_event(
            RequestSplit(
                actor=ActorRef(
                    node_id="21",
                    actor_kind=ActorKind.IMPLEMENTER,
                    attempt_count=1,
                ),
                split_bundle_ref=str(_write_split_bundle(tmp / "bundles" / "approve-21", child_names=["child_1"])),
                durable_commit=approve_commit,
                diff_fingerprint="approve-fingerprint-21",
                requested_at=now,
            ),
            recorded_at=now,
        )
        fake_proc = _FakeProcSupervisor()
        atomic_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
        )
        fake_proc.mark_live(
            ActorRef(node_id="21", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )
        atomic_core._materialize_split_children = lambda **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            RuntimeError("approve materialization exploded")
        )
        atomic_core.start()
        try:
            approve_seq = store.append_event(
                ApproveSplit(
                    actor=kernel,
                    target_node_id="21",
                    request_seq=request_seq,
                    approved_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == approve_seq, timeout_seconds=2.0):
                return _fail("ApproveSplit materialization failure must still advance last_applied_seq")
            parent_record = store.load_node("21")
            if parent_record is None:
                return _fail("ApproveSplit atomicity test parent must remain loadable")
            if parent_record.split_request != 1 or parent_record.split_approved != 0:
                return _fail("ApproveSplit materialization failure must leave the parent in pre-approval state")
            if parent_record.child_node_ids:
                return _fail("ApproveSplit materialization failure must not durably claim child ids")
        finally:
            atomic_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        parent_effects, parent_commit40 = _init_repo_with_final_effects(
            tmp / "workspace" / "node-40",
            "Parent node 40.\n",
        )
        approved_bundle = _write_split_bundle(tmp / "bundles" / "approved-40", child_names=["approved_child"])
        duplicate_bundle = _write_split_bundle(tmp / "bundles" / "duplicate-40", child_names=["duplicate_child"])
        approved_request_seq = store.append_event(
            RequestSplit(
                actor=ActorRef(
                    node_id="40",
                    actor_kind=ActorKind.IMPLEMENTER,
                    attempt_count=1,
                ),
                split_bundle_ref=str(approved_bundle),
                durable_commit=parent_commit40,
                diff_fingerprint="approve-fingerprint-40-a",
                requested_at=now,
            ),
            recorded_at=now,
        )
        store.append_event(
            RequestSplit(
                actor=ActorRef(
                    node_id="40",
                    actor_kind=ActorKind.IMPLEMENTER,
                    attempt_count=1,
                ),
                split_bundle_ref=str(duplicate_bundle),
                durable_commit=parent_commit40,
                diff_fingerprint="approve-fingerprint-40-b",
                requested_at=now,
            ),
            recorded_at=now,
        )
        _upsert_running_node(
            store=store,
            node_id="40",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=parent_commit40,
            workspace_root=parent_effects.parent,
            final_effects_file=parent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=40001,
            process_birth_time=1714000001.0,
            session_ids=["session-40"],
            split_approved=1,
            approved_split_request_seq=approved_request_seq,
        )
        reconcile_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
        )
        reconcile_core.start()
        try:
            if not _wait_until(lambda: store.load_node("41") is not None, timeout_seconds=2.0):
                return _fail("split-approved reconcile must materialize the missing child record")
            child_record = store.load_node("41")
            if child_record is None:
                return _fail("approved child record must be loadable after reconcile")
            expected_effects = (Path(child_record.workspace_root) / "children" / "approved_child" / "FINAL_EFFECTS.md").resolve()
            if child_record.final_effects_file != str(expected_effects):
                return _fail("split-approved reconcile must remap the approved request seq bundle into the child git worktree")
            if _run_git(Path(child_record.workspace_root), "rev-parse", "HEAD") != parent_commit40:
                return _fail("split-approved reconcile must still use the approved request seq baseline commit")
        finally:
            reconcile_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        checker_final_effects = _write_final_effects(
            tmp / "workspace" / "node-12" / "FINAL_EFFECTS.md",
            "Checker node 12.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="12",
            parent_node_id="0",
            attempt_count=2,
            durable_commit="checker12",
            workspace_root=checker_final_effects.parent,
            final_effects_file=checker_final_effects,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            pid=12121,
            process_birth_time=1711966000.0,
            session_ids=["checker-session-12"],
            workspace_fingerprint_before=workspace_output_fingerprint(checker_final_effects.parent),
        )
        checker_record = store.load_node("12")
        if checker_record is None:
            return _fail("checker test node must be loadable")
        store.upsert_node(
            replace(
                checker_record,
                current_components=[
                    ActorRef(
                        node_id="12",
                        actor_kind=ActorKind.EVALUATOR_CHECKER,
                        attempt_count=2,
                    )
                ],
                evaluator_phase="checker",
            )
        )
        checker_launch_specs: list[object] = []

        def _checker_recovery_launcher(spec) -> ActorLaunchResult:
            checker_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=43001),
                process_birth_time=1714300000.0,
                session_id="checker-recovery-session-12",
                rollout_path=(tmp / "runtime" / "checker-recovery-12.jsonl").resolve(),
            )

        checker_proc = _FakeProcSupervisor()
        checker_proc.mark_live(
            ActorRef(node_id="12", actor_kind=ActorKind.EVALUATOR_CHECKER, attempt_count=2)
        )
        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=checker_proc,
            actor_launcher=_checker_recovery_launcher,
        )
        core.start()
        try:
            poisoned_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="12",
                        actor_kind=ActorKind.EVALUATOR_CHECKER,
                        attempt_count=2,
                    ),
                    pid=12121,
                    process_birth_time=1711966000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == poisoned_seq, timeout_seconds=2.0):
                return _fail("incomplete checker exit must still advance last_applied_seq under the unified inbox boundary")
            if core._thread is None or not core._thread.is_alive():
                return _fail("incomplete checker exit must not kill the router core inbox thread")
            if not _wait_until(lambda: len(checker_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("missing checker completion signals must relaunch evaluator_checker")
            if checker_launch_specs[0].actor_kind is not ActorKind.EVALUATOR_CHECKER:
                return _fail("checker incomplete exit must relaunch evaluator_checker")
            if "Before exiting" not in checker_launch_specs[0].prompt_text or "complete --tasks-ref" not in checker_launch_specs[0].prompt_text:
                return _fail("checker recovery prompt must remind the checker completion interface usage")
            if "No completion record was written" not in checker_launch_specs[0].prompt_text:
                return _fail("checker recovery prompt must explain that the completion record was missing")
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        terminal_effects_1 = _write_final_effects(
            tmp / "workspace" / "node-30" / "FINAL_EFFECTS.md",
            "Terminal-failed node 30.\n",
        )
        terminal_effects_2 = _write_final_effects(
            tmp / "workspace" / "node-31" / "FINAL_EFFECTS.md",
            "Recoverable node 31.\n",
        )
        from loop.node_table import (
            ComponentRuntimeState,
            ComponentStatus,
            NodeRuntimeRecord,
        )

        store.upsert_node(
            NodeRuntimeRecord(
                node_id="30",
                parent_node_id="0",
                child_node_ids=[],
                workspace_root=str(terminal_effects_1.parent.resolve()),
                final_effects_file=str(terminal_effects_1.resolve()),
                split_request=0,
                split_approved=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                current_components=[
                    ActorRef(
                        node_id="30",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    )
                ],
                durable_commit="terminal30",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.TERMINAL_FAILED,
                        attempt_count=3,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["session-30"],
                        workspace_fingerprint_before="",
                        saw_output_in_attempt=False,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=3,
                    )
                },
            )
        )
        _upsert_running_node(
            store=store,
            node_id="31",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="terminal31",
            workspace_root=terminal_effects_2.parent,
            final_effects_file=terminal_effects_2,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=31111,
            process_birth_time=1711966760.0,
            session_ids=["session-31-1"],
            workspace_fingerprint_before=workspace_output_fingerprint(terminal_effects_2.parent),
        )
        relaunched_specs: list[object] = []

        def _relaunch_after_failure(spec) -> ActorLaunchResult:
            relaunched_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=51000 + len(relaunched_specs)),
                process_birth_time=1715100000.0 + len(relaunched_specs),
                session_id=f"relaunch-31-{len(relaunched_specs)}",
                rollout_path=(tmp / "runtime" / f"relaunch-31-{len(relaunched_specs)}.jsonl").resolve(),
            )

        fake_proc = _FakeProcSupervisor()
        fake_proc.mark_live(
            ActorRef(node_id="31", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )
        terminal_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_relaunch_after_failure,
        )
        terminal_core.start()
        try:
            actor31 = ActorRef(node_id="31", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
            failed_exit_seq_1 = store.append_event(
                ProcessExitedObserved(
                    actor=actor31,
                    pid=31111,
                    process_birth_time=1711966760.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: _component_attempt_count(store, "31", ActorKind.IMPLEMENTER) == 2,
                timeout_seconds=2.0,
            ):
                return _fail("first failed exit must relaunch the actor with attempt_count+1")
            if store.read_router_status() != "":
                return _fail("one terminal-failed frontier node out of two must not yet stop the router")
            failed_exit_seq_2 = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(node_id="31", actor_kind=ActorKind.IMPLEMENTER, attempt_count=2),
                    pid=51001,
                    process_birth_time=1715100001.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: _component_attempt_count(store, "31", ActorKind.IMPLEMENTER) == 3,
                timeout_seconds=2.0,
            ):
                return _fail("second failed exit must relaunch the actor again before terminal failure")
            if store.read_router_status() != "":
                return _fail("one terminal-failed frontier node out of two must still not stop the router after two failed exits")
            failed_exit_seq_3 = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(node_id="31", actor_kind=ActorKind.IMPLEMENTER, attempt_count=3),
                    pid=51002,
                    process_birth_time=1715100002.0,
                    exit_code=1,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_router_status() == "terminal_failed",
                timeout_seconds=2.0,
            ):
                return _fail("majority frontier terminal failure must write router terminal status")
            node31 = store.load_node("31")
            if node31 is None:
                return _fail("terminal-failed node must remain durable")
            component31 = node31.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
            if component31 is None or component31.status is not ComponentStatus.TERMINAL_FAILED:
                return _fail("third failed exit must mark the component TERMINAL_FAILED")
            if int(component31.consecutive_failed_exits) != 3:
                return _fail("third failed exit must durably count consecutive_failed_exits")
            if len(relaunched_specs) != 2:
                return _fail("terminal failure threshold must stop relaunching after the third failed exit")
            terminal_reason = json.loads(store.read_router_terminal_reason_json() or "{}")
            if terminal_reason.get("frontier_total") != 2 or terminal_reason.get("frontier_terminal_failed") != 2:
                return _fail("router terminal summary must record the frontier majority counts")
            if not _wait_until(
                lambda: terminal_core._thread is not None and not terminal_core._thread.is_alive(),
                timeout_seconds=2.0,
            ):
                return _fail("router must stop its core thread after majority frontier terminal failure")
            if store.read_last_applied_seq() != failed_exit_seq_3:
                return _fail("terminal-failure ProcessExitedObserved must still advance last_applied_seq")
        finally:
            terminal_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        reviewer_final_effects = _write_final_effects(
            tmp / "workspace" / "node-17" / "FINAL_EFFECTS.md",
            "Reviewer node 17.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="17",
            parent_node_id="0",
            attempt_count=3,
            durable_commit="reviewer17",
            workspace_root=reviewer_final_effects.parent,
            final_effects_file=reviewer_final_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=17171,
            process_birth_time=1711966400.0,
            session_ids=["reviewer-session-17"],
        )
        reviewer_record = store.load_node("17")
        if reviewer_record is None:
            return _fail("reviewer test node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                current_components=[
                    ActorRef(
                        node_id="17",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=3,
                    )
                ],
                evaluator_phase="reviewer",
            )
        )
        reviewer_launch_specs: list[object] = []

        def _reviewer_recovery_launcher(spec) -> ActorLaunchResult:
            reviewer_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=44001),
                process_birth_time=1714400000.0,
                session_id="reviewer-recovery-session-17",
                rollout_path=(tmp / "runtime" / "reviewer-recovery-17.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="17", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=3)
        )
        core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_reviewer_recovery_launcher,
        )
        core.start()
        try:
            _write_completion(
                actor_completion_record_path(
                    reviewer_final_effects.parent,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="17",
                ),
                {
                    "version": 1,
                    "node_id": "17",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 3,
                    "pid": 17171,
                    "process_birth_time": 1711966400.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "MAYBE",
                    "report_ref": "/tmp/reviewer-17.md",
                },
            )
            invalid_verdict_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="17",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=3,
                    ),
                    pid=17171,
                    process_birth_time=1711966400.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: store.read_last_applied_seq() == invalid_verdict_seq,
                timeout_seconds=2.0,
            ):
                return _fail("invalid reviewer completion must still advance last_applied_seq under the completion boundary")
            if core._thread is None or not core._thread.is_alive():
                return _fail("invalid reviewer completion must not kill the router core inbox thread")
            if not _wait_until(lambda: len(reviewer_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("invalid reviewer completion must relaunch reviewer instead of leaving the node dead")
            if reviewer_launch_specs[0].actor_kind is not ActorKind.EVALUATOR_REVIEWER:
                return _fail("invalid reviewer completion must relaunch evaluator_reviewer")
            if "verdict_kind" not in reviewer_launch_specs[0].prompt_text:
                return _fail("reviewer recovery prompt must explain the missing/invalid verdict signal")
        finally:
            core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 27.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        (repo_root / "merged.txt").write_text("merged result\n", encoding="utf-8")
        _run_git(repo_root, "add", "merged.txt")
        _run_git(repo_root, "commit", "-m", "final merged result")
        result_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="27",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=27001,
            process_birth_time=1712700000.0,
            session_ids=["reviewer-session-27"],
        )
        reviewer_record = store.load_node("27")
        if reviewer_record is None:
            return _fail("reviewer OK result_commit test node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                current_components=[
                    ActorRef(
                        node_id="27",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
                evaluator_phase="reviewer",
            )
        )
        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="27", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(
                reviewer_current_report_path(repo_root, "27"),
                "LGTM\n",
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="27",
                ),
                {
                    "version": 1,
                    "node_id": "27",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 27001,
                    "process_birth_time": 1712700000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "OK",
                    "report_ref": str(report_ref),
                },
            )
            complete_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="27",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=27001,
                    process_birth_time=1712700000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == complete_seq, timeout_seconds=2.0):
                return _fail("reviewer OK completion must still advance last_applied_seq")
            finalized = store.load_node("27")
            if finalized is None:
                return _fail("reviewer OK completion must preserve the durable node record")
            if finalized.result_commit != result_commit:
                return _fail("reviewer OK on a clean git workspace must persist the current HEAD as result_commit")
            if finalized.current_components or str(finalized.evaluator_phase or "").strip():
                return _fail("reviewer OK completion must leave the node inactive and evaluator-complete")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        from loop.feedback import active_feedback_path, feedback_history_path

        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 28b.\n")
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(repo_root, "28b"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=repo_root,
                    final_effects_file=reviewer_effects,
                    snapshot_id="snapshot-28b",
                    tasks=[_checker_task_entry("task-1", goal="task one")],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer feedback effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="28b",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=28101,
            process_birth_time=1712810000.0,
            session_ids=["reviewer-session-28b"],
            workspace_fingerprint_before=workspace_output_fingerprint(repo_root),
        )
        reviewer_record = store.load_node("28b")
        if reviewer_record is None:
            return _fail("reviewer feedback ledger node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                evaluator_phase="reviewer",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
                current_components=[
                    ActorRef(
                        node_id="28b",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
            )
        )
        implementer_launch_specs: list[object] = []

        def _implementer_feedback_launcher(spec) -> ActorLaunchResult:
            implementer_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=28102),
                process_birth_time=1712810001.0,
                session_id="implementer-session-28b",
                rollout_path=(tmp / "runtime" / "implementer-28b.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="28b", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_implementer_feedback_launcher,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(
                reviewer_current_report_path(repo_root, "28b"),
                "Current dispatched snapshot id: snapshot-28b\nneeds implementer fixes\n",
            )
            evidence_ref = _write_file(
                repo_root / ".loop" / "router_runtime" / "nodes" / "node-28b" / "reviewer" / "current" / "evidence.txt",
                "button alignment mismatch\n",
            )
            feedback_submission_ref = _write_file(
                reviewer_current_feedback_path(repo_root, "28b"),
                json.dumps(
                    {
                        "version": 2,
                        "snapshot_id": "snapshot-28b",
                        "implementer": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "login button fidelity still fails",
                                    "evidence_ref": str(evidence_ref.resolve()),
                                }
                            ],
                        },
                        "checker": {"findings": []},
                        "tester": {"findings": []},
                        "ai_user": [],
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="28b",
                ),
                {
                    "version": 1,
                    "node_id": "28b",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 28101,
                    "process_birth_time": 1712810000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "EVALUATOR_FAULT",
                    "report_ref": str(report_ref.resolve()),
                    "feedback_ref": str(feedback_submission_ref.resolve()),
                },
            )
            complete_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="28b",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=28101,
                    process_birth_time=1712810000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == complete_seq, timeout_seconds=2.0):
                return _fail("reviewer feedback completion must still advance last_applied_seq")
            if not _wait_until(lambda: len(implementer_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("reviewer feedback completion must still relaunch implementer under the current routing")
            active_path = active_feedback_path(repo_root, node_id="28b")
            if not active_path.is_file():
                return _fail("accepted reviewer feedback must materialize an active workspace feedback ledger")
            active_payload = json.loads(active_path.read_text(encoding="utf-8"))
            if active_payload.get("report_ref") != str(reviewer_accepted_report_path(repo_root, "28b", attempt_count=1)):
                return _fail("active feedback ledger must record reviewer report ref")
            if active_payload.get("feedback_submission_ref") != str(reviewer_accepted_feedback_path(repo_root, "28b", attempt_count=1)):
                return _fail("active feedback ledger must record reviewer feedback submission ref")
            implementer_findings = (
                active_payload.get("implementer", {}).get("findings", [])
                if isinstance(active_payload.get("implementer"), dict)
                else []
            )
            if len(implementer_findings) != 1:
                return _fail("active feedback ledger must preserve implementer findings")
            if implementer_findings[0].get("summary") != "login button fidelity still fails":
                return _fail("active feedback ledger must preserve finding summaries")
            history_path = feedback_history_path(repo_root, node_id="28b")
            history_lines = [
                line for line in history_path.read_text(encoding="utf-8").splitlines() if line.strip()
            ]
            if len(history_lines) != 1:
                return _fail("accepted reviewer feedback must append one history record")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 28bb.\n")
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(repo_root, "28bb"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=repo_root,
                    final_effects_file=reviewer_effects,
                    snapshot_id="snapshot-28bb",
                    tasks=[_checker_task_entry("task-1", goal="task one")],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer feedback priority effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="28bb",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=28121,
            process_birth_time=1712812000.0,
            session_ids=["reviewer-session-28bb"],
            workspace_fingerprint_before=workspace_output_fingerprint(repo_root),
        )
        reviewer_record = store.load_node("28bb")
        if reviewer_record is None:
            return _fail("reviewer feedback priority node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                evaluator_phase="reviewer",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
                current_components=[
                    ActorRef(
                        node_id="28bb",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
            )
        )
        launch_specs: list[object] = []

        def _priority_launcher(spec) -> ActorLaunchResult:
            launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=28122),
                process_birth_time=1712812001.0,
                session_id="implementer-session-28bb",
                rollout_path=(tmp / "runtime" / "implementer-28bb.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="28bb", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_priority_launcher,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(
                reviewer_current_report_path(repo_root, "28bb"),
                "Current dispatched snapshot id: snapshot-28bb\nmixed findings\n",
            )
            implementer_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28bb"
                / "reviewer"
                / "current"
                / "implementer_evidence.txt",
                "implementer evidence\n",
            )
            checker_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28bb"
                / "reviewer"
                / "current"
                / "checker_evidence.txt",
                "checker evidence\n",
            )
            tester_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28bb"
                / "reviewer"
                / "current"
                / "tester_evidence.txt",
                "tester evidence\n",
            )
            ai_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28bb"
                / "reviewer"
                / "current"
                / "ai_evidence.txt",
                "ai evidence\n",
            )
            feedback_submission_ref = _write_file(
                reviewer_current_feedback_path(repo_root, "28bb"),
                json.dumps(
                    {
                        "version": 2,
                        "snapshot_id": "snapshot-28bb",
                        "implementer": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "implementer issue summary",
                                    "evidence_ref": str(implementer_evidence_ref.resolve()),
                                }
                            ],
                        },
                        "checker": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "checker issue summary",
                                    "evidence_ref": str(checker_evidence_ref.resolve()),
                                }
                            ],
                        },
                        "tester": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "tester issue summary",
                                    "evidence_ref": str(tester_evidence_ref.resolve()),
                                }
                            ],
                        },
                        "ai_user": [
                            {
                                "task_id": "task-1",
                                "findings": [
                                    {
                                        "blocking": True,
                                        "summary": "ai issue summary",
                                        "evidence_ref": str(ai_evidence_ref.resolve()),
                                    }
                                ],
                            }
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="28bb",
                ),
                {
                    "version": 1,
                    "node_id": "28bb",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 28121,
                    "process_birth_time": 1712812000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "EVALUATOR_FAULT",
                    "report_ref": str(report_ref.resolve()),
                    "feedback_ref": str(feedback_submission_ref.resolve()),
                },
            )
            complete_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="28bb",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=28121,
                    process_birth_time=1712812000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == complete_seq, timeout_seconds=2.0):
                return _fail("reviewer feedback priority completion must advance last_applied_seq")
            if not _wait_until(lambda: len(launch_specs) == 1, timeout_seconds=2.0):
                return _fail("implementer-priority feedback should relaunch exactly one implementer actor")
            if launch_specs[0].actor_kind is not ActorKind.IMPLEMENTER:
                return _fail("implementer feedback must take restart priority over lower-level feedback")
            prompt_text = str(launch_specs[0].prompt_text)
            if str(feedback_history_path(repo_root, node_id="28bb")) not in prompt_text:
                return _fail("implementer prompt must include feedback history reference path")
            if "implementer issue summary" not in prompt_text:
                return _fail("implementer prompt must include implementer-targeted active feedback")
            if "checker issue summary" in prompt_text or "tester issue summary" in prompt_text or "ai issue summary" in prompt_text:
                return _fail("implementer prompt must not inline other actors' active feedback")
            if "Authority-grounded rebuttal rule:" not in prompt_text:
                return _fail("implementer feedback relaunch prompt must preserve authority-grounded rebuttal guidance")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 28bd.\n")
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(repo_root, "28bd"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=repo_root,
                    final_effects_file=reviewer_effects,
                    snapshot_id="snapshot-28bd",
                    tasks=[_checker_task_entry("task-1", goal="task one")],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer checker route effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="28bd",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=28131,
            process_birth_time=1712813000.0,
            session_ids=["reviewer-session-28bd"],
            workspace_fingerprint_before=workspace_output_fingerprint(repo_root),
        )
        reviewer_record = store.load_node("28bd")
        if reviewer_record is None:
            return _fail("reviewer checker-route node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                evaluator_phase="reviewer",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
                current_components=[
                    ActorRef(
                        node_id="28bd",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
            )
        )
        launch_specs = []

        def _checker_route_launcher(spec) -> ActorLaunchResult:
            launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=28132),
                process_birth_time=1712813001.0,
                session_id="checker-session-28bd",
                rollout_path=(tmp / "runtime" / "checker-28bd.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="28bd", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_checker_route_launcher,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(
                reviewer_current_report_path(repo_root, "28bd"),
                "Current dispatched snapshot id: snapshot-28bd\nchecker findings only\n",
            )
            checker_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28bd"
                / "reviewer"
                / "current"
                / "checker_evidence.txt",
                "checker evidence\n",
            )
            tester_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28bd"
                / "reviewer"
                / "current"
                / "tester_evidence.txt",
                "tester evidence\n",
            )
            feedback_submission_ref = _write_file(
                reviewer_current_feedback_path(repo_root, "28bd"),
                json.dumps(
                    {
                        "version": 2,
                        "snapshot_id": "snapshot-28bd",
                        "implementer": {"findings": []},
                        "checker": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "checker issue summary",
                                    "evidence_ref": str(checker_evidence_ref.resolve()),
                                }
                            ],
                        },
                        "tester": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "tester issue summary",
                                    "evidence_ref": str(tester_evidence_ref.resolve()),
                                }
                            ],
                        },
                        "ai_user": [],
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="28bd",
                ),
                {
                    "version": 1,
                    "node_id": "28bd",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 28131,
                    "process_birth_time": 1712813000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "IMPLEMENTER_ACTION_REQUIRED",
                    "report_ref": str(report_ref.resolve()),
                    "feedback_ref": str(feedback_submission_ref.resolve()),
                },
            )
            complete_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="28bd",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=28131,
                    process_birth_time=1712813000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == complete_seq, timeout_seconds=2.0):
                return _fail("reviewer checker-route completion must advance last_applied_seq")
            if not _wait_until(lambda: len(launch_specs) == 1, timeout_seconds=2.0):
                return _fail("checker-only feedback should relaunch exactly one checker actor")
            if launch_specs[0].actor_kind is not ActorKind.EVALUATOR_CHECKER:
                return _fail("checker feedback must restart from checker instead of implementer")
            prompt_text = str(launch_specs[0].prompt_text)
            if str(feedback_history_path(repo_root, node_id="28bd")) not in prompt_text:
                return _fail("checker prompt must include feedback history reference path")
            if "checker issue summary" not in prompt_text:
                return _fail("checker prompt must include checker-targeted active feedback")
            if "tester issue summary" in prompt_text:
                return _fail("checker prompt must not inline tester-targeted active feedback")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 28be.\n")
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(repo_root, "28be"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=repo_root,
                    final_effects_file=reviewer_effects,
                    snapshot_id="snapshot-28be",
                    tasks=[
                        _checker_task_entry("task-1", goal="task one"),
                        _checker_task_entry("task-2", goal="task two"),
                    ],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer tasks route effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="28be",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=28141,
            process_birth_time=1712814000.0,
            session_ids=["reviewer-session-28be"],
            workspace_fingerprint_before=workspace_output_fingerprint(repo_root),
            task_result_refs={
                "task-1": {
                    ActorKind.EVALUATOR_TESTER.value: "/tmp/tester-old.md",
                    ActorKind.EVALUATOR_AI_USER.value: "/tmp/ai-task-1-old.md",
                },
                "task-2": {
                    ActorKind.EVALUATOR_TESTER.value: "/tmp/tester-old.md",
                    ActorKind.EVALUATOR_AI_USER.value: "/tmp/ai-task-2-old.md",
                },
            },
        )
        reviewer_record = store.load_node("28be")
        if reviewer_record is None:
            return _fail("reviewer tasks-route node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                evaluator_phase="reviewer",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
                current_components=[
                    ActorRef(
                        node_id="28be",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
            )
        )
        launch_specs = []

        def _tasks_route_launcher(spec) -> ActorLaunchResult:
            launch_specs.append(spec)
            launched_task_id = str(spec.env.get("LOOP_TASK_ID") or "").strip()
            session_suffix = launched_task_id or spec.actor_kind.value
            return ActorLaunchResult(
                process=_LiveProcess(pid=28142 + len(launch_specs)),
                process_birth_time=1712814001.0 + len(launch_specs),
                session_id=f"tasks-route-{session_suffix}",
                rollout_path=(tmp / "runtime" / f"tasks-route-{session_suffix}.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="28be", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_tasks_route_launcher,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(
                reviewer_current_report_path(repo_root, "28be"),
                "Current dispatched snapshot id: snapshot-28be\ntester and task-1 only\n",
            )
            tester_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28be"
                / "reviewer"
                / "current"
                / "tester_evidence.txt",
                "tester evidence\n",
            )
            ai_evidence_ref = _write_file(
                repo_root
                / ".loop"
                / "router_runtime"
                / "nodes"
                / "node-28be"
                / "reviewer"
                / "current"
                / "ai_evidence_task_1.txt",
                "ai task 1 evidence\n",
            )
            feedback_submission_ref = _write_file(
                reviewer_current_feedback_path(repo_root, "28be"),
                json.dumps(
                    {
                        "version": 2,
                        "snapshot_id": "snapshot-28be",
                        "implementer": {"findings": []},
                        "checker": {"findings": []},
                        "tester": {
                            "findings": [
                                {
                                    "blocking": True,
                                    "summary": "tester issue summary",
                                    "evidence_ref": str(tester_evidence_ref.resolve()),
                                }
                            ],
                        },
                        "ai_user": [
                            {
                                "task_id": "task-1",
                                "findings": [
                                    {
                                        "blocking": True,
                                        "summary": "ai task-1 issue summary",
                                        "evidence_ref": str(ai_evidence_ref.resolve()),
                                    }
                                ],
                            }
                        ],
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="28be",
                ),
                {
                    "version": 1,
                    "node_id": "28be",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 28141,
                    "process_birth_time": 1712814000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "IMPLEMENTER_ACTION_REQUIRED",
                    "report_ref": str(report_ref.resolve()),
                    "feedback_ref": str(feedback_submission_ref.resolve()),
                },
            )
            complete_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="28be",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=28141,
                    process_birth_time=1712814000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == complete_seq, timeout_seconds=2.0):
                return _fail("reviewer tasks-route completion must advance last_applied_seq")
            if not _wait_until(lambda: len(launch_specs) == 2, timeout_seconds=2.0):
                return _fail("tasks-only feedback should relaunch tester plus exactly one ai_user lane")
            launched_kinds = [spec.actor_kind for spec in launch_specs]
            if launched_kinds.count(ActorKind.EVALUATOR_TESTER) != 1 or launched_kinds.count(ActorKind.EVALUATOR_AI_USER) != 1:
                return _fail("tasks-only feedback must relaunch one tester and one ai_user actor")
            ai_specs = [spec for spec in launch_specs if spec.actor_kind is ActorKind.EVALUATOR_AI_USER]
            if not ai_specs or str(ai_specs[0].env.get("LOOP_TASK_ID") or "").strip() != "task-1":
                return _fail("tasks-only feedback must relaunch only the ai_user task_id that still has active feedback")
            tester_prompt = str([spec for spec in launch_specs if spec.actor_kind is ActorKind.EVALUATOR_TESTER][0].prompt_text)
            ai_prompt = str(ai_specs[0].prompt_text)
            if str(feedback_history_path(repo_root, node_id="28be")) not in tester_prompt:
                return _fail("tester prompt must include feedback history reference path")
            if str(feedback_history_path(repo_root, node_id="28be")) not in ai_prompt:
                return _fail("ai_user prompt must include feedback history reference path")
            if "tester issue summary" not in tester_prompt or "ai task-1 issue summary" in tester_prompt:
                return _fail("tester prompt must include only tester-targeted active feedback")
            if "ai task-1 issue summary" not in ai_prompt or "tester issue summary" in ai_prompt:
                return _fail("ai_user prompt must include only its own task-targeted active feedback")
            persisted = store.load_node("28be")
            if persisted is None:
                return _fail("tasks-only feedback node must remain durable")
            task_1_results = persisted.task_result_refs.get("task-1", {})
            task_2_results = persisted.task_result_refs.get("task-2", {})
            if ActorKind.EVALUATOR_TESTER.value in task_1_results or ActorKind.EVALUATOR_TESTER.value in task_2_results:
                return _fail("tasks-only tester rerun must clear prior tester results across all tasks")
            if ActorKind.EVALUATOR_AI_USER.value in task_1_results:
                return _fail("tasks-only ai_user rerun must clear prior result for the targeted task")
            if task_2_results.get(ActorKind.EVALUATOR_AI_USER.value) != "/tmp/ai-task-2-old.md":
                return _fail("tasks-only ai_user rerun must preserve untouched ai_user lane results")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 28c.\n")
        checker_task_ref = _write_file(repo_root / "checker" / "task-1.md", "task one\n")
        checker_tasks_ref = _write_file(
            repo_root / "checker" / "tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [{"task_id": "task-1", "task_ref": str(checker_task_ref.resolve())}],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer invalid feedback effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="28c",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=28111,
            process_birth_time=1712811000.0,
            session_ids=["reviewer-session-28c"],
            workspace_fingerprint_before=workspace_output_fingerprint(repo_root),
        )
        reviewer_record = store.load_node("28c")
        if reviewer_record is None:
            return _fail("invalid reviewer feedback node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                evaluator_phase="reviewer",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
                current_components=[
                    ActorRef(
                        node_id="28c",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
            )
        )
        reviewer_recovery_specs: list[object] = []

        def _invalid_feedback_reviewer_launcher(spec) -> ActorLaunchResult:
            reviewer_recovery_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=28112),
                process_birth_time=1712811001.0,
                session_id="reviewer-recovery-session-28c",
                rollout_path=(tmp / "runtime" / "reviewer-recovery-28c.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="28c", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_invalid_feedback_reviewer_launcher,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(repo_root / "reviewer" / "report.md", "needs checker fixes\n")
            feedback_submission_ref = _write_file(
                repo_root / "reviewer" / "feedback.json",
                json.dumps(
                    {
                        "version": 1,
                        "implementer": {"feedback_ref": "", "findings": []},
                        "tester": {"feedback_ref": "", "findings": []},
                        "ai_user": [],
                    },
                    indent=2,
                    sort_keys=True,
                ),
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="28c",
                ),
                {
                    "version": 1,
                    "node_id": "28c",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 28111,
                    "process_birth_time": 1712811000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "EVALUATOR_FAULT",
                    "report_ref": str(report_ref.resolve()),
                    "feedback_ref": str(feedback_submission_ref.resolve()),
                },
            )
            invalid_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="28c",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=28111,
                    process_birth_time=1712811000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == invalid_seq, timeout_seconds=2.0):
                return _fail("invalid reviewer feedback completion must still advance last_applied_seq")
            if not _wait_until(lambda: len(reviewer_recovery_specs) == 1, timeout_seconds=2.0):
                return _fail("invalid reviewer feedback completion must relaunch reviewer")
            if "missing top-level bucket 'checker'" not in reviewer_recovery_specs[0].prompt_text:
                return _fail("reviewer recovery prompt must explain invalid feedback submission details")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        reviewer_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Reviewer node 28.\n")
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(repo_root, "28"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=repo_root,
                    final_effects_file=reviewer_effects,
                    snapshot_id="snapshot-reviewer-dirty-28",
                    tasks=[
                        _checker_task_entry(
                            "T28",
                            goal="Validate reviewer closeout persistence.",
                            task_instructions="Inspect reviewer OK closeout persistence behavior.",
                        )
                    ],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add reviewer effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        (repo_root / "dirty.txt").write_text("still dirty\n", encoding="utf-8")
        (repo_root / ".DS_Store").write_text("noise\n", encoding="utf-8")
        (repo_root / "target-task005").mkdir(parents=True, exist_ok=True)
        (repo_root / "target-task005" / "scratch.txt").write_text("noise\n", encoding="utf-8")
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="28",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=reviewer_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=28001,
            process_birth_time=1712800000.0,
            session_ids=["reviewer-session-28"],
            checker_tasks_ref=str(checker_tasks_ref),
        )
        reviewer_record = store.load_node("28")
        if reviewer_record is None:
            return _fail("dirty reviewer completion node must be loadable")
        store.upsert_node(
            replace(
                reviewer_record,
                current_components=[
                    ActorRef(
                        node_id="28",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    )
                ],
                evaluator_phase="reviewer",
            )
        )
        reviewer_recovery_specs: list[object] = []

        def _dirty_reviewer_launcher(spec) -> ActorLaunchResult:
            reviewer_recovery_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=28002),
                process_birth_time=1712800001.0,
                session_id="reviewer-recovery-session-28",
                rollout_path=(tmp / "runtime" / "reviewer-recovery-28.jsonl").resolve(),
            )

        reviewer_proc = _FakeProcSupervisor()
        reviewer_proc.mark_live(
            ActorRef(node_id="28", actor_kind=ActorKind.EVALUATOR_REVIEWER, attempt_count=1)
        )
        reviewer_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=reviewer_proc,
            actor_launcher=_dirty_reviewer_launcher,
        )
        reviewer_core.start()
        try:
            report_ref = _write_file(
                reviewer_current_report_path(repo_root, "28"),
                "\n".join(
                    [
                        "Current dispatched snapshot id: snapshot-reviewer-dirty-28",
                        "Blocking active findings: none.",
                        "",
                    ]
                ),
            )
            feedback_ref = _write_file(
                reviewer_current_feedback_path(repo_root, "28"),
                json.dumps(
                    {
                        "version": 2,
                        "snapshot_id": "snapshot-reviewer-dirty-28",
                        "implementer": {"findings": []},
                        "checker": {"findings": []},
                        "tester": {"findings": []},
                        "ai_user": [],
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
            )
            _write_completion(
                actor_completion_record_path(
                    repo_root,
                    ActorKind.EVALUATOR_REVIEWER,
                    node_id="28",
                ),
                {
                    "version": 1,
                    "node_id": "28",
                    "actor_kind": ActorKind.EVALUATOR_REVIEWER.value,
                    "attempt_count": 1,
                    "pid": 28001,
                    "process_birth_time": 1712800000.0,
                    "completed_at": now.isoformat(),
                    "verdict_kind": "OK",
                    "report_ref": str(report_ref),
                    "feedback_ref": str(feedback_ref),
                    "feedback_submission": {
                        "version": 2,
                        "snapshot_id": "snapshot-reviewer-dirty-28",
                        "implementer": {"findings": []},
                        "checker": {"findings": []},
                        "tester": {"findings": []},
                        "ai_user": [],
                    },
                },
            )
            invalid_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="28",
                        actor_kind=ActorKind.EVALUATOR_REVIEWER,
                        attempt_count=1,
                    ),
                    pid=28001,
                    process_birth_time=1712800000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == invalid_seq, timeout_seconds=2.0):
                return _fail("router-owned reviewer result commit must still advance last_applied_seq")
            settled_record = store.load_node("28")
            if settled_record is None:
                return _fail("router-owned reviewer result commit node must remain durable")
            if reviewer_recovery_specs:
                return _fail("reviewer OK with substantive dirty work must not relaunch reviewer once router can materialize the result commit")
            result_commit = str(settled_record.result_commit or "").strip()
            if not result_commit:
                return _fail("reviewer OK must persist a router-created result_commit")
            if result_commit == base_commit:
                return _fail("router-created result_commit must advance beyond the durable baseline when substantive work is dirty")
            if result_commit != _run_git(repo_root, "rev-parse", "HEAD"):
                return _fail("router-created reviewer result_commit must match workspace HEAD")
            committed_paths = set(
                filter(
                    None,
                    _run_git(repo_root, "show", "--pretty=format:", "--name-only", result_commit).splitlines(),
                )
            )
            if "dirty.txt" not in committed_paths:
                return _fail("router-created reviewer result_commit must include substantive workspace changes")
            if ".DS_Store" in committed_paths or any(path.startswith("target-task005/") for path in committed_paths):
                return _fail("router-created reviewer result_commit must exclude non-substantive workspace noise")
        finally:
            reviewer_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        parent_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Parent node 60.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add merge parent effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        child_a_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "child-a",
            relpath="feature_a.txt",
            content="feature A\n",
            message="child a result",
        )
        child_b_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "child-b",
            relpath="feature_b.txt",
            content="feature B\n",
            message="child b result",
        )
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="60",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=parent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=60001,
            process_birth_time=1716000000.0,
            session_ids=["parent-session-60"],
            split_approved=1,
            approved_split_request_seq=1,
            child_node_ids=["61", "62"],
        )
        parent_record = store.load_node("60")
        if parent_record is None:
            return _fail("auto-merge parent record must be loadable")
        store.upsert_node(replace(parent_record, current_components=[]))
        child_a_effects = _write_final_effects(tmp / "child-a-effects" / "FINAL_EFFECTS.md", "Child A.\n")
        child_b_effects = _write_final_effects(tmp / "child-b-effects" / "FINAL_EFFECTS.md", "Child B.\n")
        child_a_report = _write_file(child_a_effects.parent / "reviewer.md", "child a ok\n")
        child_b_report = _write_file(child_b_effects.parent / "reviewer.md", "child b ok\n")
        child_a_tester = _write_file(child_a_effects.parent / "tester.md", "child a tester\n")
        child_b_tester = _write_file(child_b_effects.parent / "tester.md", "child b tester\n")
        child_a_runtime = (tmp / "router" / "runtime" / "node-61" / "implementer" / "attempt-1").resolve()
        child_b_runtime = (tmp / "router" / "runtime" / "node-62" / "implementer" / "attempt-1").resolve()
        child_a_runtime.mkdir(parents=True, exist_ok=True)
        child_b_runtime.mkdir(parents=True, exist_ok=True)
        (child_a_runtime / "runtime.txt").write_text("child a runtime\n", encoding="utf-8")
        (child_b_runtime / "runtime.txt").write_text("child b runtime\n", encoding="utf-8")
        _upsert_running_node(
            store=store,
            node_id="61",
            parent_node_id="60",
            attempt_count=1,
            durable_commit=base_commit,
            result_commit=child_a_commit,
            workspace_root=child_a_effects.parent,
            final_effects_file=child_a_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=61001,
            process_birth_time=1716100000.0,
            session_ids=["child-session-61"],
        )
        _upsert_running_node(
            store=store,
            node_id="62",
            parent_node_id="60",
            attempt_count=1,
            durable_commit=base_commit,
            result_commit=child_b_commit,
            workspace_root=child_b_effects.parent,
            final_effects_file=child_b_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=62001,
            process_birth_time=1716200000.0,
            session_ids=["child-session-62"],
        )
        for child_id in ("61", "62"):
            child_record = store.load_node(child_id)
            if child_record is None:
                return _fail("auto-merge child node must be loadable before reconcile")
            store.upsert_node(
                replace(
                    child_record,
                    current_components=[],
                    reviewer_verdict_kind="OK",
                    reviewer_report_ref=str(
                        child_a_report.resolve() if child_id == "61" else child_b_report.resolve()
                    ),
                    task_result_refs={
                        "task-1": {
                            ActorKind.EVALUATOR_TESTER.value: str(
                                child_a_tester.resolve() if child_id == "61" else child_b_tester.resolve()
                            )
                        }
                    },
                )
            )
        merge_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
        )
        merge_core.start()
        try:
            if not _wait_until(
                lambda: bool(str((store.load_node("60") or _sentinel()).result_commit or "").strip()),
                timeout_seconds=2.0,
            ):
                return _fail("split-approved parent with all child result_commit values must auto-merge to a durable parent result_commit")
            merged_parent = store.load_node("60")
            if merged_parent is None:
                return _fail("auto-merged parent must remain durable")
            if str(merged_parent.result_commit or "").strip() != _run_git(Path(merged_parent.workspace_root), "rev-parse", "HEAD"):
                return _fail("auto-merged parent result_commit must match the HEAD of the durable parent integration workspace")
            if (Path(merged_parent.workspace_root) / "feature_a.txt").read_text(encoding="utf-8") != "feature A\n":
                return _fail("auto-merged parent workspace must contain child A changes")
            if (Path(merged_parent.workspace_root) / "feature_b.txt").read_text(encoding="utf-8") != "feature B\n":
                return _fail("auto-merged parent workspace must contain child B changes")
            if int(merged_parent.split_approved) != 0:
                return _fail("auto-merged parent must clear split_approved after convergence closes")
            for child_id, original_workspace in (
                ("61", child_a_effects.parent.resolve()),
                ("62", child_b_effects.parent.resolve()),
            ):
                archived_child = store.load_node(child_id)
                if archived_child is None:
                    return _fail("absorbed child must remain durable after parent convergence")
                if original_workspace.exists():
                    return _fail("absorbed child workspace must be removed immediately after parent convergence")
                if (tmp / "router" / "runtime" / f"node-{child_id}").exists():
                    return _fail("absorbed child runtime directory must be removed after parent convergence")
                if not str(archived_child.workspace_root or "").strip():
                    return _fail("absorbed child must keep a router-owned archive root for durable evidence refs")
                if "router/archive" not in str(archived_child.workspace_root):
                    return _fail("absorbed child archive root must move under router/archive")
                if str(archived_child.final_effects_file or "").strip():
                    return _fail("absorbed child must clear final_effects_file after convergence")
                if str(archived_child.checker_tasks_ref or "").strip():
                    return _fail("absorbed child must clear checker_tasks_ref after convergence")
                archived_report = Path(str(archived_child.reviewer_report_ref or "")).resolve()
                if not archived_report.exists():
                    return _fail("absorbed child must preserve reviewer_report_ref via archive copy")
                archived_tester = Path(
                    str(
                        archived_child.task_result_refs.get("task-1", {}).get(
                            ActorKind.EVALUATOR_TESTER.value,
                            "",
                        )
                    )
                ).resolve()
                if not archived_tester.exists():
                    return _fail("absorbed child must preserve task result refs via archive copy")
        finally:
            merge_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo-parent-residual").resolve()
        _init_git_repo(repo_root)
        parent_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Parent node 65.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add parent residual effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        child_a_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "residual-child-a",
            relpath="feature_a.txt",
            content="feature A\n",
            message="residual child a",
        )
        child_b_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "residual-child-b",
            relpath="feature_b.txt",
            content="feature B\n",
            message="residual child b",
        )
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        residual_bundle = _write_split_bundle(
            tmp / "residual-bundle",
            child_names=["child_a", "child_b"],
            parent_after_merge=True,
        )
        request_seq = store.append_event(
            RequestSplit(
                actor=ActorRef(node_id="65", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1),
                split_bundle_ref=str(residual_bundle),
                durable_commit=base_commit,
                diff_fingerprint="residual-fingerprint",
                requested_at=datetime.now(timezone.utc),
            ),
            recorded_at=datetime.now(timezone.utc),
        )
        _upsert_running_node(
            store=store,
            node_id="65",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=parent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=65001,
            process_birth_time=1716500000.0,
            session_ids=["parent-session-65"],
            split_approved=1,
            approved_split_request_seq=request_seq,
            child_node_ids=["66", "67"],
        )
        parent_record = store.load_node("65")
        if parent_record is None:
            return _fail("parent-after-merge parent record must be loadable")
        store.upsert_node(replace(parent_record, current_components=[]))
        for child_id, child_commit, effects_name, pid in (
            ("66", child_a_commit, "child-a/FINAL_EFFECTS.md", 66001),
            ("67", child_b_commit, "child-b/FINAL_EFFECTS.md", 67001),
        ):
            child_effects = _write_final_effects(tmp / effects_name, f"Child {child_id}.\n")
            _upsert_running_node(
                store=store,
                node_id=child_id,
                parent_node_id="65",
                attempt_count=1,
                durable_commit=base_commit,
                result_commit=child_commit,
                workspace_root=child_effects.parent,
                final_effects_file=child_effects,
                actor_kind=ActorKind.IMPLEMENTER,
                pid=pid,
                process_birth_time=float(pid),
                session_ids=[f"child-session-{child_id}"],
            )
            child_record = store.load_node(child_id)
            if child_record is None:
                return _fail("parent-after-merge child node must be loadable before reconcile")
            store.upsert_node(replace(child_record, current_components=[], reviewer_verdict_kind="OK"))
        residual_launch_specs: list[object] = []

        def _residual_launcher(spec) -> ActorLaunchResult:
            residual_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=65002),
                process_birth_time=1716500001.0,
                session_id="residual-session-65",
                rollout_path=(tmp / "runtime" / "residual-node-65.jsonl").resolve(),
            )

        residual_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            actor_launcher=_residual_launcher,
        )
        residual_core.start()
        try:
            if not _wait_until(lambda: len(residual_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("clean child merge with parent_after_merge must relaunch the parent implementer")
            residual_spec = residual_launch_specs[0]
            if residual_spec.actor_kind is not ActorKind.IMPLEMENTER or str(residual_spec.node_id) != "65":
                return _fail("parent-after-merge relaunch must target the parent implementer lane")
            expected_residual_ref = (residual_bundle / "parent_after_merge" / "FINAL_EFFECTS.md").resolve()
            if str(expected_residual_ref) not in str(residual_spec.prompt_text):
                return _fail("parent-after-merge relaunch prompt must inject the approved residual final effects reference")
            residual_parent = store.load_node("65")
            if residual_parent is None:
                return _fail("parent-after-merge parent must remain durable")
            if str(residual_parent.result_commit or "").strip():
                return _fail("parent-after-merge parent must not auto-persist result_commit before residual work is completed")
            if int(residual_parent.split_approved) != 0:
                return _fail("parent-after-merge relaunch must clear split_approved before handing control back to the parent implementer")
            merged_head = _run_git(Path(residual_parent.workspace_root), "rev-parse", "HEAD")
            if str(residual_parent.durable_commit or "").strip() != merged_head:
                return _fail("parent-after-merge relaunch must advance durable_commit to the clean merged child baseline")
            residual_final_effects = Path(residual_parent.final_effects_file)
            if not residual_final_effects.exists() or residual_final_effects.read_text(encoding="utf-8") != Path(parent_effects).read_text(encoding="utf-8"):
                return _fail("parent-after-merge relaunch must materialize the current parent FINAL_EFFECTS into the merged workspace")
            if (Path(residual_parent.workspace_root) / "feature_a.txt").read_text(encoding="utf-8") != "feature A\n":
                return _fail("parent-after-merge workspace must contain merged child A changes")
            if (Path(residual_parent.workspace_root) / "feature_b.txt").read_text(encoding="utf-8") != "feature B\n":
                return _fail("parent-after-merge workspace must contain merged child B changes")
        finally:
            residual_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo-cascade").resolve()
        _init_git_repo(repo_root)
        grandparent_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Grandparent node 79.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add cascade parent effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        child_a_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "cascade-child-a",
            relpath="feature_a.txt",
            content="feature A\n",
            message="cascade child a",
        )
        child_b_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "cascade-child-b",
            relpath="feature_b.txt",
            content="feature B\n",
            message="cascade child b",
        )
        child_c_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "cascade-child-c",
            relpath="feature_c.txt",
            content="feature C\n",
            message="cascade child c",
        )
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="79",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=grandparent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=79001,
            process_birth_time=1717900000.0,
            session_ids=["grandparent-session-79"],
            split_approved=1,
            approved_split_request_seq=1,
            child_node_ids=["80", "83"],
        )
        _upsert_running_node(
            store=store,
            node_id="80",
            parent_node_id="79",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=grandparent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=80001,
            process_birth_time=1718000000.0,
            session_ids=["parent-session-80"],
            split_approved=1,
            approved_split_request_seq=1,
            child_node_ids=["81", "82"],
        )
        for node_id in ("79", "80"):
            record = store.load_node(node_id)
            if record is None:
                return _fail("cascade parent records must be loadable")
            store.upsert_node(replace(record, current_components=[]))
        for child_id, parent_id, child_commit, effects_name, pid in (
            ("81", "80", child_a_commit, "child-a/FINAL_EFFECTS.md", 81001),
            ("82", "80", child_b_commit, "child-b/FINAL_EFFECTS.md", 82001),
            ("83", "79", child_c_commit, "child-c/FINAL_EFFECTS.md", 83001),
        ):
            child_effects = _write_final_effects(tmp / effects_name, f"Child {child_id}.\n")
            _upsert_running_node(
                store=store,
                node_id=child_id,
                parent_node_id=parent_id,
                attempt_count=1,
                durable_commit=base_commit,
                result_commit=child_commit,
                workspace_root=child_effects.parent,
                final_effects_file=child_effects,
                actor_kind=ActorKind.IMPLEMENTER,
                pid=pid,
                process_birth_time=float(pid),
                session_ids=[f"child-session-{child_id}"],
            )
            child_record = store.load_node(child_id)
            if child_record is None:
                return _fail("cascade child nodes must be loadable")
            store.upsert_node(
                replace(
                    child_record,
                    current_components=[],
                    reviewer_verdict_kind="OK",
                )
            )
        cascade_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
        )
        cascade_core.start()
        try:
            if not _wait_until(
                lambda: bool(str((store.load_node("80") or _sentinel()).result_commit or "").strip()),
                timeout_seconds=2.0,
            ):
                return _fail("child results must first converge into a durable parent result_commit before grandparent merge")
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: bool(str((store.load_node("79") or _sentinel()).result_commit or "").strip()),
                timeout_seconds=2.0,
            ):
                return _fail("parent result_commit must be able to cascade into a grandparent auto-merge on the next reconcile tick")
            cascaded_root = store.load_node("79")
            if cascaded_root is None:
                return _fail("cascaded grandparent must remain durable")
            for filename, expected in (
                ("feature_a.txt", "feature A\n"),
                ("feature_b.txt", "feature B\n"),
                ("feature_c.txt", "feature C\n"),
            ):
                if (Path(cascaded_root.workspace_root) / filename).read_text(encoding="utf-8") != expected:
                    return _fail("grandparent cascade merge must contain every converged child change")
        finally:
            cascade_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo").resolve()
        _init_git_repo(repo_root)
        parent_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Parent node 70.\n")
        (repo_root / "shared.txt").write_text("base\n", encoding="utf-8")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md", "shared.txt")
        _run_git(repo_root, "commit", "-m", "add conflict parent effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        child_a_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "conflict-child-a",
            relpath="shared.txt",
            content="child A\n",
            message="child a conflicting result",
        )
        child_b_commit = _commit_in_detached_worktree(
            repo_root=repo_root,
            base_commit=base_commit,
            worktree_root=tmp / "conflict-child-b",
            relpath="shared.txt",
            content="child B\n",
            message="child b conflicting result",
        )
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id="70",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=base_commit,
            workspace_root=repo_root,
            final_effects_file=parent_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=70001,
            process_birth_time=1717000000.0,
            session_ids=["parent-session-70"],
            split_approved=1,
            approved_split_request_seq=1,
            child_node_ids=["71", "72"],
        )
        parent_record = store.load_node("70")
        if parent_record is None:
            return _fail("takeover parent record must be loadable")
        store.upsert_node(replace(parent_record, current_components=[]))
        child_a_effects = _write_final_effects(tmp / "conflict-child-a-effects" / "FINAL_EFFECTS.md", "Child A.\n")
        child_b_effects = _write_final_effects(tmp / "conflict-child-b-effects" / "FINAL_EFFECTS.md", "Child B.\n")
        _upsert_running_node(
            store=store,
            node_id="71",
            parent_node_id="70",
            attempt_count=1,
            durable_commit=base_commit,
            result_commit=child_a_commit,
            workspace_root=child_a_effects.parent,
            final_effects_file=child_a_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=71001,
            process_birth_time=1717100000.0,
            session_ids=["child-session-71"],
        )
        _upsert_running_node(
            store=store,
            node_id="72",
            parent_node_id="70",
            attempt_count=1,
            durable_commit=base_commit,
            result_commit=child_b_commit,
            workspace_root=child_b_effects.parent,
            final_effects_file=child_b_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=72001,
            process_birth_time=1717200000.0,
            session_ids=["child-session-72"],
        )
        for child_id in ("71", "72"):
            child_record = store.load_node(child_id)
            if child_record is None:
                return _fail("takeover child node must be loadable before reconcile")
            store.upsert_node(
                replace(
                    child_record,
                    current_components=[],
                    reviewer_verdict_kind="OK",
                )
            )
        takeover_launch_specs: list[object] = []

        def _takeover_launcher(spec) -> ActorLaunchResult:
            takeover_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=70002),
                process_birth_time=1717000001.0,
                session_id="takeover-session-70",
                rollout_path=(tmp / "runtime" / "takeover-node-70.jsonl").resolve(),
            )

        takeover_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            actor_launcher=_takeover_launcher,
        )
        takeover_core.start()
        try:
            if not _wait_until(lambda: len(takeover_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("merge conflicts across child result commits must trigger a takeover relaunch of the parent implementer")
            takeover_spec = takeover_launch_specs[0]
            if takeover_spec.actor_kind is not ActorKind.IMPLEMENTER or str(takeover_spec.node_id) != "70":
                return _fail("takeover relaunch must target the parent implementer lane")
            if "conflict" not in str(takeover_spec.prompt_text).lower():
                return _fail("takeover relaunch prompt must explain that the parent is resuming only to resolve merge conflicts")
            takeover_parent = store.load_node("70")
            if takeover_parent is None:
                return _fail("takeover parent must remain durable")
            if str(takeover_parent.result_commit or "").strip():
                return _fail("conflicted takeover parent must not persist result_commit before conflicts are resolved")
            if int(takeover_parent.split_approved) != 0:
                return _fail("takeover parent must leave split_approved state once conflict resolution is handed back to the parent")
            unresolved = _run_git(Path(takeover_parent.workspace_root), "diff", "--name-only", "--diff-filter=U")
            if "shared.txt" not in unresolved.splitlines():
                return _fail("takeover workspace must preserve the unresolved git merge conflict for the parent to resolve")
        finally:
            takeover_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo-kernel-final").resolve()
        _init_git_repo(repo_root)
        root_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Root node 1.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add root final effects")
        root_commit = _run_git(repo_root, "rev-parse", "HEAD")
        reviewer_report = _write_file(tmp / "root-review.md", "root ok\n")
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        wakeup_path = router_wakeup_socket_path(db_path)
        _upsert_running_node(
            store=store,
            node_id="1",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=root_commit,
            workspace_root=repo_root,
            final_effects_file=root_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=91001,
            process_birth_time=1719100000.0,
            session_ids=["root-reviewer-session"],
        )
        root_record = store.load_node("1")
        if root_record is None:
            return _fail("root reviewer node must be loadable")
        root_record = replace(root_record, evaluator_phase="reviewer")
        store.upsert_node(root_record)
        final_launch_specs: list[object] = []

        def _final_kernel_launcher(spec) -> ActorLaunchResult:
            final_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=91002 + len(final_launch_specs)),
                process_birth_time=1719100001.0 + len(final_launch_specs),
                session_id=f"final-kernel-{len(final_launch_specs)}",
                rollout_path=(tmp / "runtime" / f"final-kernel-{len(final_launch_specs)}.jsonl").resolve(),
            )

        final_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            actor_launcher=_final_kernel_launcher,
        )
        final_core._advance_after_valid_actor_completion(
            record=root_record,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            task_id=None,
            completion_payload={
                "verdict_kind": "OK",
                "report_ref": str(reviewer_report.resolve()),
                "result_commit": root_commit,
            },
        )
        if len(final_launch_specs) != 1:
            return _fail("root reviewer OK must hand off final closeout to a node-0 kernel takeover launch")
        if final_launch_specs[0].actor_kind is not ActorKind.KERNEL or str(final_launch_specs[0].node_id) != KERNEL_NODE_ID:
            return _fail("final closeout takeover must launch the ordinary kernel actor on node 0")
        if str(final_launch_specs[0].env.get("LOOP_REQUEST_SEQ") or "").strip():
            return _fail("ordinary final kernel takeover must not carry split-review LOOP_REQUEST_SEQ metadata")
        if "final kernel takeover" not in str(final_launch_specs[0].prompt_text).lower():
            return _fail("final kernel takeover prompt must explain that node 0 is taking over final closeout")
        if "pending implementer split request" in str(final_launch_specs[0].prompt_text).lower() or "frozen proposal.json" in str(final_launch_specs[0].prompt_text).lower():
            return _fail("final kernel takeover must not reuse the split-review kernel prompt")
        kernel_record = store.load_node(KERNEL_NODE_ID)
        if kernel_record is None:
            return _fail("final kernel takeover must materialize node 0")
        if not kernel_record.escalated_to_kernel:
            return _fail("final kernel takeover must mark node 0 as escalated_to_kernel")
        if str(kernel_record.durable_commit or "").strip() != root_commit:
            return _fail("final kernel takeover must use the root result_commit as node 0 durable baseline")
        kernel_final_effects = Path(kernel_record.final_effects_file)
        if not kernel_final_effects.exists() or kernel_final_effects.read_text(encoding="utf-8") != Path(root_effects).read_text(encoding="utf-8"):
            return _fail("final kernel takeover must materialize the current root FINAL_EFFECTS into the kernel worktree")
        _write_file(Path(kernel_record.workspace_root) / "kernel-closeout.md", "closeout note\n")
        final_core._advance_after_valid_actor_completion(
            record=kernel_record,
            actor_kind=ActorKind.KERNEL,
            task_id=None,
            completion_payload={},
        )
        if len(final_launch_specs) != 2:
            return _fail("ordinary kernel completion during final closeout must continue into evaluator checker")
        if final_launch_specs[1].actor_kind is not ActorKind.EVALUATOR_CHECKER:
            return _fail("final kernel completion must relaunch node 0 through evaluator_checker")
        if str(final_launch_specs[1].node_id) != KERNEL_NODE_ID:
            return _fail("node 0 evaluator checker must stay on node 0 after kernel takeover")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo-kernel-complete").resolve()
        _init_git_repo(repo_root)
        kernel_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Kernel closeout.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add kernel closeout effects")
        kernel_commit = _run_git(repo_root, "rev-parse", "HEAD")
        reviewer_report = _write_file(tmp / "kernel-review.md", "kernel ok\n")
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        _upsert_running_node(
            store=store,
            node_id=KERNEL_NODE_ID,
            parent_node_id="1",
            attempt_count=1,
            durable_commit=kernel_commit,
            workspace_root=repo_root,
            final_effects_file=kernel_effects,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            pid=92001,
            process_birth_time=1719200000.0,
            session_ids=["kernel-reviewer-session"],
        )
        kernel_record = store.load_node(KERNEL_NODE_ID)
        if kernel_record is None:
            return _fail("node 0 reviewer completion test node must be loadable")
        kernel_record = replace(
            kernel_record,
            evaluator_phase="reviewer",
            escalated_to_kernel=True,
        )
        store.upsert_node(kernel_record)
        completed_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=fake_proc,
        )
        finalized = completed_core._advance_after_valid_actor_completion(
            record=kernel_record,
            actor_kind=ActorKind.EVALUATOR_REVIEWER,
            task_id=None,
            completion_payload={
                "verdict_kind": "OK",
                "report_ref": str(reviewer_report.resolve()),
                "result_commit": kernel_commit,
            },
        )
        if str(store.read_router_status() or "").strip() != "completed":
            return _fail("node 0 reviewer OK must write router completed status")
        if str(store.read_router_completed_result_commit() or "").strip() != kernel_commit:
            return _fail("node 0 reviewer OK must persist router_completed_result_commit")
        if str(store.read_router_completed_report_ref() or "").strip() != str(reviewer_report.resolve()):
            return _fail("node 0 reviewer OK must persist router_completed_report_ref")
        if not str(store.read_router_completed_at() or "").strip():
            return _fail("node 0 reviewer OK must persist router_completed_at")
        if not completed_core._stop_requested:
            return _fail("node 0 reviewer OK must request router self-stop after global success")
        if fake_proc.stopped != 1:
            return _fail("node 0 reviewer OK must stop proc supervision after global success")
        if str(finalized.result_commit or "").strip() != kernel_commit:
            return _fail("node 0 reviewer OK must preserve the final result_commit on the durable node")
        if finalized.current_components or str(finalized.evaluator_phase or "").strip():
            return _fail("node 0 reviewer OK must leave the node inactive and evaluator-complete")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "repo-kernel-noop-closeout").resolve()
        _init_git_repo(repo_root)
        kernel_effects = _write_final_effects(repo_root / "FINAL_EFFECTS.md", "Kernel noop closeout.\n")
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add kernel noop closeout effects")
        kernel_commit = _run_git(repo_root, "rev-parse", "HEAD")
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        _upsert_running_node(
            store=store,
            node_id=KERNEL_NODE_ID,
            parent_node_id="1",
            attempt_count=2,
            durable_commit=kernel_commit,
            workspace_root=repo_root,
            final_effects_file=kernel_effects,
            actor_kind=ActorKind.KERNEL,
            pid=93001,
            process_birth_time=1719300000.0,
            session_ids=["kernel-noop-session"],
            workspace_fingerprint_before=workspace_output_fingerprint(repo_root),
        )
        kernel_record = store.load_node(KERNEL_NODE_ID)
        if kernel_record is None:
            return _fail("node 0 kernel noop closeout test node must be loadable")
        kernel_record = replace(kernel_record, escalated_to_kernel=True)
        store.upsert_node(kernel_record)
        _write_completion(
            actor_completion_record_path(repo_root, ActorKind.KERNEL, node_id=KERNEL_NODE_ID),
            {
                "version": 1,
                "node_id": KERNEL_NODE_ID,
                "actor_kind": ActorKind.KERNEL.value,
                "attempt_count": 2,
                "pid": 93001,
                "process_birth_time": 1719300000.0,
                "completed_at": now.isoformat(),
            },
        )
        noop_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        noop_component = kernel_record.components.get(component_key(actor_kind=ActorKind.KERNEL))
        if noop_component is None:
            return _fail("node 0 kernel noop closeout test must preserve kernel component state")
        noop_check = noop_core._validate_actor_completion(
            record=kernel_record,
            item=StoredRouterInboxItem(
                seq=1,
                event_type="ProcessExitedObserved",
                node_id=KERNEL_NODE_ID,
                actor_kind=ActorKind.KERNEL,
                attempt_count=2,
                recorded_at=now,
                payload_json="{}",
            ),
            component=noop_component,
            payload={
                "exit_code": 0,
                "signal_name": None,
            },
        )
        if not noop_check.accepted:
            return _fail(
                "node 0 final-closeout kernel must be allowed to complete without substantive workspace changes "
                f"when the current takeover workspace already satisfies closeout: {noop_check.reason_lines}"
            )

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        task_effects = _write_final_effects(
            tmp / "workspace" / "node-18" / "FINAL_EFFECTS.md",
            "Task node 18.\n",
        )
        task_ref = _write_file(tmp / "workspace" / "node-18" / "task-1.md", "Task 1.\n")
        checker_tasks_ref = _write_file(
            tmp / "workspace" / "node-18" / "checker-tasks.json",
            json.dumps(
                {
                    "version": 1,
                    "tasks": [{"task_id": "task-1", "task_ref": str(task_ref.resolve())}],
                },
                indent=2,
                sort_keys=True,
            ),
        )
        _upsert_running_node(
            store=store,
            node_id="18",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="task18",
            workspace_root=task_effects.parent,
            final_effects_file=task_effects,
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            task_id="task-1",
            pid=18181,
            process_birth_time=1711800000.0,
            session_ids=["ai-user-session-18"],
            task_result_refs={"task-1": {ActorKind.EVALUATOR_TESTER.value: "/tmp/tester-18.md"}},
        )
        task_record = store.load_node("18")
        if task_record is None:
            return _fail("task node 18 must be loadable")
        task_record = replace(
            task_record,
            evaluator_phase="tasks",
            checker_tasks_ref=str(checker_tasks_ref.resolve()),
        )
        store.upsert_node(task_record)
        task_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        original_upsert_node = store.upsert_node

        def _raise_on_reviewer_upsert(record):
            if str(record.evaluator_phase or "").strip() == "reviewer":
                raise RuntimeError("reviewer upsert exploded")
            return original_upsert_node(record)

        store.upsert_node = _raise_on_reviewer_upsert  # type: ignore[method-assign]
        try:
            try:
                task_core._advance_after_valid_actor_completion(
                    record=task_record,
                    actor_kind=ActorKind.EVALUATOR_AI_USER,
                    task_id="task-1",
                    completion_payload={
                        "task_id": "task-1",
                        "result_ref": "/tmp/ai-user-18.md",
                    },
                )
            except RuntimeError:
                pass
            else:
                return _fail("task->reviewer atomicity test must force reviewer promotion failure")
        finally:
            store.upsert_node = original_upsert_node  # type: ignore[method-assign]
        persisted_record = store.load_node("18")
        if persisted_record is None:
            return _fail("task->reviewer atomicity test node must remain loadable")
        if str(persisted_record.evaluator_phase or "").strip() != "tasks":
            return _fail("task->reviewer promotion failure must leave the node in tasks phase for recovery")
        if persisted_record.current_components:
            return _fail("task->reviewer promotion failure must not leave a fake running actor behind")
        if not str(
            persisted_record.task_result_refs.get("task-1", {}).get(
                ActorKind.EVALUATOR_AI_USER.value,
                "",
            )
        ).strip():
            return _fail("task->reviewer promotion failure must preserve the completed AI-user result for retry")
        reviewer_recovery_specs: list[object] = []

        def _reviewer_recovery_launcher(spec) -> ActorLaunchResult:
            reviewer_recovery_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=41818),
                process_birth_time=1714181800.0,
                session_id=f"task18-{spec.actor_kind.value}",
                rollout_path=(tmp / "runtime" / f"task18-{spec.actor_kind.value}.jsonl").resolve(),
            )

        recovery_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=_FakeProcSupervisor(),
            actor_launcher=_reviewer_recovery_launcher,
        )
        recovered_record = recovery_core._continue_tasks_phase(persisted_record)
        if len(reviewer_recovery_specs) != 1 or reviewer_recovery_specs[0].actor_kind is not ActorKind.EVALUATOR_REVIEWER:
            return _fail("task->reviewer promotion failure must remain recoverable by relaunching reviewer")
        if len(recovered_record.current_components) != 1 or recovered_record.current_components[0].actor_kind is not ActorKind.EVALUATOR_REVIEWER:
            return _fail("reviewer recovery must materialize one running reviewer actor")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        task_effects = _write_final_effects(
            tmp / "workspace" / "node-23" / "FINAL_EFFECTS.md",
            "Task node 23.\n",
        )
        result_ref = _write_file(
            tmp / "workspace" / "node-23" / "ai-user-result.md",
            "AI user result.\n",
        )
        broken_tasks_ref = _write_file(
            tmp / "workspace" / "node-23" / "checker-tasks.json",
            "{not valid json",
        )
        _write_file(tmp / "workspace" / "node-23" / "deliverable.md", "changed\n")
        fake_proc = _FakeProcSupervisor()
        fake_proc.mark_live(
            ActorRef(
                node_id="23",
                actor_kind=ActorKind.EVALUATOR_AI_USER,
                attempt_count=2,
                task_id="task-1",
            )
        )
        _upsert_running_node(
            store=store,
            node_id="23",
            parent_node_id="0",
            attempt_count=2,
            durable_commit="task23",
            workspace_root=task_effects.parent,
            final_effects_file=task_effects,
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            task_id="task-1",
            pid=23232,
            process_birth_time=1712300000.0,
            session_ids=["ai-user-session-23"],
            task_result_refs={"task-1": {ActorKind.EVALUATOR_TESTER.value: "/tmp/tester-23.md"}},
            workspace_fingerprint_before="before-task-23",
        )
        task_record = store.load_node("23")
        if task_record is None:
            return _fail("task node 23 must be loadable")
        store.upsert_node(
            replace(
                task_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(broken_tasks_ref.resolve()),
            )
        )
        checker_launch_specs: list[object] = []

        def _checker_manifest_launcher(spec) -> ActorLaunchResult:
            checker_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=43023),
                process_birth_time=1714302300.0,
                session_id=f"task23-{spec.actor_kind.value}",
                rollout_path=(tmp / "runtime" / f"task23-{spec.actor_kind.value}.jsonl").resolve(),
            )

        task_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=fake_proc,
            actor_launcher=_checker_manifest_launcher,
        )
        task_core.start()
        try:
            _write_completion(
                actor_completion_record_path(
                    task_effects.parent,
                    ActorKind.EVALUATOR_AI_USER,
                    node_id="23",
                    task_id="task-1",
                ),
                {
                    "version": 1,
                    "node_id": "23",
                    "actor_kind": ActorKind.EVALUATOR_AI_USER.value,
                    "attempt_count": 2,
                    "pid": 23232,
                    "process_birth_time": 1712300000.0,
                    "completed_at": now.isoformat(),
                    "task_id": "task-1",
                    "result_ref": str(result_ref.resolve()),
                },
            )
            exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="23",
                        actor_kind=ActorKind.EVALUATOR_AI_USER,
                        attempt_count=2,
                        task_id="task-1",
                    ),
                    pid=23232,
                    process_birth_time=1712300000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == exit_seq, timeout_seconds=2.0):
                return _fail("bad checker task manifests must not poison ProcessExitedObserved handling")
            if task_core._thread is None or not task_core._thread.is_alive():
                return _fail("bad checker task manifests must not kill the router core thread")
            if not _wait_until(lambda: len(checker_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("bad checker task manifests must rerun checker instead of leaving the dead task actor RUNNING")
            if checker_launch_specs[0].actor_kind is not ActorKind.EVALUATOR_CHECKER:
                return _fail("bad checker task manifests must recover by relaunching evaluator_checker")
            if "checker task manifest" not in checker_launch_specs[0].prompt_text:
                return _fail("checker manifest recovery prompt must explain why checker is being rerun")
        finally:
            task_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        checker_effects = _write_final_effects(
            tmp / "workspace" / "node-24" / "FINAL_EFFECTS.md",
            "Checker node 24.\n",
        )
        _write_file(tmp / "workspace" / "node-24" / "deliverable.md", "changed\n")
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(checker_effects.parent, "24"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=checker_effects.parent,
                    final_effects_file=checker_effects,
                    snapshot_id="snapshot-24-live",
                    tasks=[
                        _checker_task_entry(
                            "task-1",
                            goal="Validate task one.",
                            task_instructions="Inspect the current workspace and validate task one.",
                            required_evidence=["screenshot diff"],
                        )
                    ],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _upsert_running_node(
            store=store,
            node_id="24",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="checker24",
            workspace_root=checker_effects.parent,
            final_effects_file=checker_effects,
            actor_kind=ActorKind.EVALUATOR_CHECKER,
            pid=24242,
            process_birth_time=1712400000.0,
            session_ids=["checker-session-24"],
            workspace_fingerprint_before="before-checker-24",
        )
        tester_launch_specs: list[object] = []
        checker_proc = _FakeProcSupervisor()
        checker_proc.mark_live(
            ActorRef(node_id="24", actor_kind=ActorKind.EVALUATOR_CHECKER, attempt_count=1)
        )

        def _checker_snapshot_launcher(spec) -> ActorLaunchResult:
            tester_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=44024),
                process_birth_time=1714402400.0,
                session_id=f"checker24-{spec.actor_kind.value}",
                rollout_path=(tmp / "runtime" / f"checker24-{spec.actor_kind.value}.jsonl").resolve(),
            )

        checker_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=checker_proc,
            actor_launcher=_checker_snapshot_launcher,
        )
        checker_core.start()
        try:
            _write_completion(
                actor_completion_record_path(
                    checker_effects.parent,
                    ActorKind.EVALUATOR_CHECKER,
                    node_id="24",
                ),
                {
                    "version": 1,
                    "node_id": "24",
                    "actor_kind": ActorKind.EVALUATOR_CHECKER.value,
                    "attempt_count": 1,
                    "pid": 24242,
                    "process_birth_time": 1712400000.0,
                    "completed_at": now.isoformat(),
                    "tasks_ref": str(checker_tasks_ref.resolve()),
                },
            )
            exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="24",
                        actor_kind=ActorKind.EVALUATOR_CHECKER,
                        attempt_count=1,
                    ),
                    pid=24242,
                    process_birth_time=1712400000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == exit_seq, timeout_seconds=2.0):
                return _fail("checker snapshot completion must advance the inbox")
            if not _wait_until(lambda: len(tester_launch_specs) == 2, timeout_seconds=2.0):
                return _fail("checker snapshot completion must launch tester plus the pending AI-user task")
            if tester_launch_specs[0].actor_kind is not ActorKind.EVALUATOR_TESTER:
                return _fail("checker snapshot completion must launch evaluator_tester first")
            if tester_launch_specs[1].actor_kind is not ActorKind.EVALUATOR_AI_USER:
                return _fail("checker snapshot completion must launch evaluator_ai_user after tester")
            if str(tester_launch_specs[1].env.get("LOOP_TASK_ID") or "") != "task-1":
                return _fail("checker snapshot completion must preserve the pending task_id when launching AI-user")
            persisted_record = store.load_node("24")
            if persisted_record is None:
                return _fail("checker snapshot node must remain loadable")
            frozen_tasks_ref = Path(str(persisted_record.checker_tasks_ref or "")).resolve()
            if frozen_tasks_ref == checker_tasks_ref.resolve():
                return _fail("checker completion must snapshot tasks_ref instead of preserving the live manifest path")
            if not frozen_tasks_ref.exists():
                return _fail("checker completion must persist an existing frozen task manifest")
            frozen_payload = json.loads(frozen_tasks_ref.read_text(encoding="utf-8"))
            if int(frozen_payload.get("version") or 0) != 2:
                return _fail("checker completion snapshot must preserve the version=2 manifest contract")
            if str(frozen_payload.get("snapshot_id") or "").strip() == "snapshot-24-live":
                return _fail("checker completion snapshot must assign a fresh accepted snapshot id")
            frozen_task = list(frozen_payload.get("tasks") or [{}])[0]
            if str(frozen_task.get("task_instructions") or "").strip() != "Inspect the current workspace and validate task one.":
                return _fail("checker completion snapshot must preserve inline task instructions")
            if list(frozen_task.get("required_evidence") or []) != ["screenshot diff"]:
                return _fail("checker completion snapshot must preserve required_evidence for accepted manifests")
        finally:
            checker_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        ai_effects = _write_final_effects(
            tmp / "workspace" / "node-25" / "FINAL_EFFECTS.md",
            "Task node 25.\n",
        )
        result_ref = _write_file(
            ai_user_current_result_path(ai_effects.parent, "25", task_id="task-1"),
            "Task id: wrong-task\nCurrent dispatched snapshot id: snapshot-25\nTask verdict: FAIL\n",
        )
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(ai_effects.parent, "25"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=ai_effects.parent,
                    final_effects_file=ai_effects,
                    snapshot_id="snapshot-25",
                    tasks=[_checker_task_entry("task-1", goal="Validate task one.")],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _write_file(tmp / "workspace" / "node-25" / "deliverable.md", "changed\n")
        _upsert_running_node(
            store=store,
            node_id="25",
            parent_node_id="0",
            attempt_count=3,
            durable_commit="task25",
            workspace_root=ai_effects.parent,
            final_effects_file=ai_effects,
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            task_id="task-1",
            pid=25252,
            process_birth_time=1712500000.0,
            session_ids=["ai-user-session-25"],
            task_result_refs={"task-1": {ActorKind.EVALUATOR_TESTER.value: "/tmp/tester-25.md"}},
            workspace_fingerprint_before="before-task-25",
        )
        ai_record = store.load_node("25")
        if ai_record is None:
            return _fail("task node 25 must be loadable")
        store.upsert_node(
            replace(
                ai_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
            )
        )
        ai_launch_specs: list[object] = []
        ai_proc = _FakeProcSupervisor()
        ai_proc.mark_live(
            ActorRef(
                node_id="25",
                actor_kind=ActorKind.EVALUATOR_AI_USER,
                attempt_count=3,
                task_id="task-1",
            )
        )

        def _ai_task_launcher(spec) -> ActorLaunchResult:
            ai_launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=45025),
                process_birth_time=1714502500.0,
                session_id=f"task25-{spec.actor_kind.value}",
                rollout_path=(tmp / "runtime" / f"task25-{spec.actor_kind.value}.jsonl").resolve(),
            )

        ai_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=ai_proc,
            actor_launcher=_ai_task_launcher,
        )
        ai_core.start()
        try:
            _write_completion(
                actor_completion_record_path(
                    ai_effects.parent,
                    ActorKind.EVALUATOR_AI_USER,
                    node_id="25",
                    task_id="task-1",
                ),
                {
                    "version": 1,
                    "node_id": "25",
                    "actor_kind": ActorKind.EVALUATOR_AI_USER.value,
                    "attempt_count": 3,
                    "pid": 25252,
                    "process_birth_time": 1712500000.0,
                    "completed_at": now.isoformat(),
                    "task_id": "wrong-task",
                    "result_ref": str(result_ref.resolve()),
                },
            )
            exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="25",
                        actor_kind=ActorKind.EVALUATOR_AI_USER,
                        attempt_count=3,
                        task_id="task-1",
                    ),
                    pid=25252,
                    process_birth_time=1712500000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == exit_seq, timeout_seconds=2.0):
                return _fail("invalid AI-user task_id must still advance the inbox")
            if not _wait_until(lambda: len(ai_launch_specs) == 1, timeout_seconds=2.0):
                return _fail("invalid AI-user task_id must relaunch evaluator_ai_user")
            if ai_launch_specs[0].actor_kind is not ActorKind.EVALUATOR_AI_USER:
                return _fail("invalid AI-user task_id must relaunch evaluator_ai_user")
            if "task_id" not in ai_launch_specs[0].prompt_text:
                return _fail("AI-user recovery prompt must explain the mismatched task_id")
        finally:
            ai_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        ai_effects = _write_final_effects(
            tmp / "workspace" / "node-26" / "FINAL_EFFECTS.md",
            "Task node 26.\n",
        )
        result_ref = _write_file(
            ai_user_current_result_path(ai_effects.parent, "26", task_id="task-1"),
            "Task id: task-1\nCurrent dispatched snapshot id: snapshot-26\nTask verdict: PASS\n",
        )
        checker_tasks_ref = _write_file(
            checker_current_tasks_path(ai_effects.parent, "26"),
            json.dumps(
                _checker_manifest_payload(
                    workspace_root=ai_effects.parent,
                    final_effects_file=ai_effects,
                    snapshot_id="snapshot-26",
                    tasks=[_checker_task_entry("task-1", goal="Validate task one.")],
                ),
                indent=2,
                sort_keys=True,
            ),
        )
        _write_file(tmp / "workspace" / "node-26" / "deliverable.md", "changed\n")
        _upsert_running_node(
            store=store,
            node_id="26",
            parent_node_id="0",
            attempt_count=1,
            durable_commit="task26",
            workspace_root=ai_effects.parent,
            final_effects_file=ai_effects,
            actor_kind=ActorKind.EVALUATOR_AI_USER,
            task_id="task-1",
            pid=26262,
            process_birth_time=1712600000.0,
            session_ids=["ai-user-session-26"],
            task_result_refs={},
            workspace_fingerprint_before="before-task-26",
        )
        ai_record = store.load_node("26")
        if ai_record is None:
            return _fail("task node 26 must be loadable")
        store.upsert_node(
            replace(
                ai_record,
                evaluator_phase="tasks",
                checker_tasks_ref=str(checker_tasks_ref.resolve()),
            )
        )
        launch_specs: list[object] = []
        ai_proc = _FakeProcSupervisor()
        ai_proc.mark_live(
            ActorRef(
                node_id="26",
                actor_kind=ActorKind.EVALUATOR_AI_USER,
                attempt_count=1,
                task_id="task-1",
            )
        )

        def _valid_ai_task_launcher(spec) -> ActorLaunchResult:
            launch_specs.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=46026),
                process_birth_time=1714602600.0,
                session_id=f"task26-{spec.actor_kind.value}",
                rollout_path=(tmp / "runtime" / f"task26-{spec.actor_kind.value}.jsonl").resolve(),
            )

        ai_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=ai_proc,
            actor_launcher=_valid_ai_task_launcher,
        )
        ai_core.start()
        try:
            _write_completion(
                actor_completion_record_path(
                    ai_effects.parent,
                    ActorKind.EVALUATOR_AI_USER,
                    node_id="26",
                    task_id="task-1",
                ),
                {
                    "version": 1,
                    "node_id": "26",
                    "actor_kind": ActorKind.EVALUATOR_AI_USER.value,
                    "attempt_count": 1,
                    "pid": 26262,
                    "process_birth_time": 1712600000.0,
                    "completed_at": now.isoformat(),
                    "task_id": "task-1",
                    "result_ref": str(result_ref.resolve()),
                },
            )
            exit_seq = store.append_event(
                ProcessExitedObserved(
                    actor=ActorRef(
                        node_id="26",
                        actor_kind=ActorKind.EVALUATOR_AI_USER,
                        attempt_count=1,
                        task_id="task-1",
                    ),
                    pid=26262,
                    process_birth_time=1712600000.0,
                    exit_code=0,
                    signal_name=None,
                    occurred_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(lambda: store.read_last_applied_seq() == exit_seq, timeout_seconds=2.0):
                return _fail("valid AI-user task_id from checker manifest must still advance the inbox")
            if not _wait_until(lambda: len(launch_specs) == 1, timeout_seconds=2.0):
                return _fail("valid AI-user completion should advance tasks phase instead of relaunching the same AI-user")
            if launch_specs[0].actor_kind is not ActorKind.EVALUATOR_TESTER:
                return _fail("accepted AI-user completion should continue tasks phase by launching evaluator_tester when tester is still pending")
            persisted = store.load_node("26")
            if persisted is None:
                return _fail("task node 26 must remain loadable after valid AI-user completion")
            if str(
                persisted.task_result_refs.get("task-1", {}).get(ActorKind.EVALUATOR_AI_USER.value, "")
            ) != str(ai_user_accepted_result_path(ai_effects.parent, "26", task_id="task-1", attempt_count=1)):
                return _fail("valid AI-user completion must persist the accepted AI-user result ref even when task_result_refs started empty")
        finally:
            ai_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        wakeup_path = router_wakeup_socket_path(db_path)
        store = RouterStore(db_path)
        failing_review_effects, review13_commit = _init_repo_with_final_effects(
            tmp / "workspace" / "node-13",
            "Kernel review failure node 13.\n",
        )
        _upsert_running_node(
            store=store,
            node_id="13",
            parent_node_id="0",
            attempt_count=1,
            durable_commit=review13_commit,
            workspace_root=failing_review_effects.parent,
            final_effects_file=failing_review_effects,
            actor_kind=ActorKind.IMPLEMENTER,
            pid=13131,
            process_birth_time=1711966100.0,
            session_ids=["implementer-session-13"],
            split_request=1,
        )

        failed_kernel_launches: list[object] = []

        def _failing_kernel_actor_launcher(spec) -> ActorLaunchResult:
            if spec.actor_kind is ActorKind.KERNEL:
                failed_kernel_launches.append(spec)
                raise RuntimeError("kernel actor launch exploded")
            return ActorLaunchResult(
                process=_LiveProcess(pid=43013),
                process_birth_time=1714301300.0,
                session_id="fallback-session-13",
                rollout_path=(tmp / "runtime" / "fallback-node-13.jsonl").resolve(),
            )

        review_proc = _FakeProcSupervisor()
        review_proc.mark_live(
            ActorRef(node_id="13", actor_kind=ActorKind.IMPLEMENTER, attempt_count=1)
        )
        review_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=review_proc,
            actor_launcher=_failing_kernel_actor_launcher,
        )
        review_core.start()
        try:
            request_seq = store.append_event(
                RequestSplit(
                    actor=ActorRef(
                        node_id="13",
                        actor_kind=ActorKind.IMPLEMENTER,
                        attempt_count=1,
                    ),
                    split_bundle_ref=str(_write_split_bundle(tmp / "bundles" / "review-fail", child_names=["child_1"])),
                    durable_commit=review13_commit,
                    diff_fingerprint="review-fingerprint-13",
                    requested_at=now,
                ),
                recorded_at=now,
            )
            notify_router_wakeup(wakeup_path)
            if not _wait_until(
                lambda: len(failed_kernel_launches) >= 1,
                timeout_seconds=2.0,
            ):
                return _fail("pending split review must still try to launch the node-0 kernel actor")
            if (store.load_node("13") or _sentinel()).split_request != 1:
                return _fail("kernel actor launch failure must leave the source node split_request pending for retry")
            if store.latest_request_split_seq_for_node("13") != request_seq:
                return _fail("kernel review boundary test must preserve the durable request seq")
        finally:
            review_core.stop()

        recovered_kernel_launches: list[object] = []

        def _working_kernel_actor_launcher(spec) -> ActorLaunchResult:
            recovered_kernel_launches.append(spec)
            return ActorLaunchResult(
                process=_LiveProcess(pid=43014),
                process_birth_time=1714301400.0,
                session_id=f"kernel-recovered-{spec.attempt_count}",
                rollout_path=(tmp / "runtime" / f"kernel-recovered-{spec.attempt_count}.jsonl").resolve(),
            )

        recovered_review_core = RouterCore(
            store=store,
            wakeup_socket_path=wakeup_path,
            proc_supervisor=review_proc,
            actor_launcher=_working_kernel_actor_launcher,
        )
        recovered_review_core.start()
        try:
            if not _wait_until(
                lambda: len(recovered_kernel_launches) == 1,
                timeout_seconds=2.0,
            ):
                return _fail("core restart must reconcile a pending split by relaunching the node-0 kernel actor")
            if recovered_kernel_launches[0].actor_kind is not ActorKind.KERNEL:
                return _fail("reconciled split review must relaunch kernel as the node-0 actor")
            if recovered_kernel_launches[0].node_id != KERNEL_NODE_ID:
                return _fail("reconciled split review must still run on node 0")
        finally:
            recovered_review_core.stop()

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        hung_core_thread = _HungThread()
        stop_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=fake_proc,
        )
        stop_core._thread = hung_core_thread
        stop_core.stop()
        if stop_core._thread is not hung_core_thread:
            return _fail("core.stop must preserve the core thread reference when join times out and the thread remains alive")
        if fake_proc.stopped != 1:
            return _fail("core.stop must still stop proc even if core thread joins time out")
        if hung_core_thread.join_calls != [2.0]:
            return _fail("core.stop must use the configured join timeout for the core thread")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        terminal_resume_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=fake_proc,
        )
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"test"}',
            router_terminal_at="2026-04-05T00:00:00+00:00",
        )
        terminal_resume_core._maybe_request_router_terminal_shutdown()
        if not terminal_resume_core._stop_requested:
            return _fail("existing terminal_failed router status must still request shutdown side effects")
        if fake_proc.stopped != 1:
            return _fail("existing terminal_failed router status must still stop proc when revisited")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"startup-test"}',
            router_terminal_at="2026-04-05T00:00:00+00:00",
        )
        fake_proc = _FakeProcSupervisor()
        startup_terminal_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=fake_proc,
        )
        sweep_calls = 0

        def _count_sweep() -> None:
            nonlocal sweep_calls
            sweep_calls += 1

        startup_terminal_core._startup_running_actor_sweep = _count_sweep  # type: ignore[method-assign]
        startup_terminal_core.start()
        if not _wait_until(
            lambda: startup_terminal_core._thread is not None
            and not startup_terminal_core._thread.is_alive(),
            timeout_seconds=2.0,
        ):
            return _fail("core.start must immediately self-stop when durable router_status is already terminal_failed")
        if sweep_calls != 0:
            return _fail("core.start must not run the startup actor sweep after durable terminal_failed was recorded")
        if fake_proc.started != 1 or fake_proc.stopped != 1:
            return _fail("core.start terminal_failed resume path must still start and then stop proc exactly once")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        fake_proc = _FakeProcSupervisor()
        completed_resume_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=fake_proc,
        )
        store.write_router_completed_state(
            router_status="completed",
            router_completed_result_commit="abc123complete",
            router_completed_report_ref="/tmp/router-complete.md",
            router_completed_at="2026-04-06T00:00:00+00:00",
        )
        completed_resume_core._maybe_request_router_terminal_shutdown()
        if not completed_resume_core._stop_requested:
            return _fail("existing completed router status must still request shutdown side effects")
        if fake_proc.stopped != 1:
            return _fail("existing completed router status must still stop proc when revisited")

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "router.sqlite3"
        store = RouterStore(db_path)
        store.write_router_completed_state(
            router_status="completed",
            router_completed_result_commit="abc123complete",
            router_completed_report_ref="/tmp/router-complete.md",
            router_completed_at="2026-04-06T00:00:00+00:00",
        )
        fake_proc = _FakeProcSupervisor()
        startup_completed_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
            proc_supervisor=fake_proc,
        )
        sweep_calls = 0

        def _count_completed_sweep() -> None:
            nonlocal sweep_calls
            sweep_calls += 1

        startup_completed_core._startup_running_actor_sweep = _count_completed_sweep  # type: ignore[method-assign]
        startup_completed_core.start()
        if not _wait_until(
            lambda: startup_completed_core._thread is not None
            and not startup_completed_core._thread.is_alive(),
            timeout_seconds=2.0,
        ):
            return _fail("core.start must immediately self-stop when durable router_status is already completed")
        if sweep_calls != 0:
            return _fail("core.start must not run the startup actor sweep after durable completed was recorded")
        if fake_proc.started != 1 or fake_proc.stopped != 1:
            return _fail("core.start completed resume path must still start and then stop proc exactly once")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "paused-append-router.sqlite3"
        store = RouterStore(db_path)
        paused_workspace = (tmp / "paused-root").resolve()
        paused_workspace.mkdir(parents=True, exist_ok=True)
        paused_final_effects = _write_final_effects(
            paused_workspace / "FINAL_EFFECTS.md",
            "Original paused obligations.\n",
        )
        paused_append_ref = _write_file(
            tmp / "paused-append-final-effects.md",
            "Paused appended obligations.\n",
        )
        child_ref = ActorRef(
            node_id="7",
            actor_kind=ActorKind.IMPLEMENTER,
            attempt_count=2,
        )
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="1",
                parent_node_id="0",
                child_node_ids=["7"],
                workspace_root=str(paused_workspace),
                final_effects_file=str(paused_final_effects),
                split_request=1,
                split_approved=1,
                approved_split_request_seq=9,
                evaluator_phase="tasks",
                checker_tasks_ref="/tmp/paused-checker.json",
                task_result_refs={"task-1": {"evaluator_ai_user": "/tmp/result.json"}},
                reviewer_verdict_kind="",
                reviewer_report_ref="",
                pending_prelude_lines=[],
                current_components=[child_ref],
                durable_commit="paused-root-durable",
                result_commit="",
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="paused-fingerprint",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.INACTIVE,
                        attempt_count=1,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["paused-root"],
                        workspace_fingerprint_before="paused-root-fp",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        store.write_router_paused_state(
            router_status="paused",
            router_paused_reason_json='{"reason":"manual pause"}',
            router_paused_at="2026-04-09T00:00:00+00:00",
        )
        paused_resume_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        paused_payload = paused_resume_core.prepare_resume(
            append_final_effects_ref=paused_append_ref,
        )
        if str(paused_payload.get("resume_mode") or "") != "paused_append":
            return _fail("paused append resume must report paused_append resume_mode")
        if str(store.read_router_status() or "").strip():
            return _fail("paused append resume must clear router_status before runtime restart")
        resumed_paused_root = store.load_node("1")
        if resumed_paused_root is None:
            return _fail("paused append resume must preserve the root node")
        if resumed_paused_root.child_node_ids != ["7"] or int(resumed_paused_root.split_request) != 1:
            return _fail("paused append resume must preserve the current root frontier state")
        if resumed_paused_root.current_components != [child_ref]:
            return _fail("paused append resume must not rewrite current frontier ownership")
        paused_merged_final_effects = paused_final_effects.read_text(encoding="utf-8")
        if "Original paused obligations." not in paused_merged_final_effects:
            return _fail("paused append resume must preserve the original root FINAL_EFFECTS text")
        if "## Appended Final Effects" not in paused_merged_final_effects or "Paused appended obligations." not in paused_merged_final_effects:
            return _fail("paused append resume must append the new obligations to the root FINAL_EFFECTS file")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "terminal-append-router.sqlite3"
        store = RouterStore(db_path)
        terminal_workspace = (tmp / "terminal-root").resolve()
        terminal_workspace.mkdir(parents=True, exist_ok=True)
        terminal_effects = _write_final_effects(
            terminal_workspace / "FINAL_EFFECTS.md",
            "Original terminal obligations.\n",
        )
        terminal_append_ref = _write_file(
            tmp / "terminal-append-final-effects.md",
            "Terminal appended obligations.\n",
        )
        child_workspace = (tmp / "terminal-child").resolve()
        child_workspace.mkdir(parents=True, exist_ok=True)
        child_effects = _write_final_effects(
            child_workspace / "FINAL_EFFECTS.md",
            "Child obligations.\n",
        )
        store.upsert_nodes(
            [
                NodeRuntimeRecord(
                    node_id="1",
                    parent_node_id="0",
                    child_node_ids=["71"],
                    workspace_root=str(terminal_workspace),
                    final_effects_file=str(terminal_effects),
                    split_request=1,
                    split_approved=1,
                    approved_split_request_seq=5,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[],
                    durable_commit="root-durable",
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.TERMINAL_FAILED,
                            attempt_count=1,
                            pid=0,
                            process_birth_time=None,
                            session_ids=["root-terminal"],
                            workspace_fingerprint_before="root-fp",
                            saw_output_in_attempt=True,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=1,
                        )
                    },
                ),
                NodeRuntimeRecord(
                    node_id="71",
                    parent_node_id="1",
                    child_node_ids=[],
                    workspace_root=str(child_workspace),
                    final_effects_file=str(child_effects),
                    split_request=0,
                    split_approved=0,
                    approved_split_request_seq=0,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[
                        ActorRef(
                            node_id="71",
                            actor_kind=ActorKind.IMPLEMENTER,
                            attempt_count=3,
                        )
                    ],
                    durable_commit="child-durable",
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.TERMINAL_FAILED,
                            attempt_count=3,
                            pid=7101,
                            process_birth_time=1713000100.0,
                            session_ids=["child-terminal"],
                            workspace_fingerprint_before="child-fp",
                            saw_output_in_attempt=True,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=3,
                        )
                    },
                ),
            ]
        )
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"majority_frontier_terminal_failed"}',
            router_terminal_at="2026-04-09T00:00:00+00:00",
        )
        terminal_resume_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        terminal_payload = terminal_resume_core.prepare_resume(
            append_final_effects_ref=terminal_append_ref,
        )
        if str(terminal_payload.get("resume_mode") or "") != "terminal_failed_append":
            return _fail("terminal_failed append resume must report terminal_failed_append resume_mode")
        if int(terminal_payload.get("recovered_components") or 0) != 1:
            return _fail("terminal_failed append resume must still recover only unfinished frontier components")
        if str(store.read_router_status() or "").strip():
            return _fail("terminal_failed append resume must clear router_status before runtime restart")
        resumed_terminal_root = store.load_node("1")
        resumed_terminal_child = store.load_node("71")
        if resumed_terminal_root is None or resumed_terminal_child is None:
            return _fail("terminal_failed append resume must preserve durable nodes")
        resumed_terminal_child_component = resumed_terminal_child.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
        if resumed_terminal_child_component is None or resumed_terminal_child_component.status is not ComponentStatus.INACTIVE:
            return _fail("terminal_failed append resume must still reactivate the unfinished child frontier")
        terminal_merged_final_effects = terminal_effects.read_text(encoding="utf-8")
        if "Original terminal obligations." not in terminal_merged_final_effects:
            return _fail("terminal_failed append resume must preserve the original root FINAL_EFFECTS text")
        if "## Appended Final Effects" not in terminal_merged_final_effects or "Terminal appended obligations." not in terminal_merged_final_effects:
            return _fail("terminal_failed append resume must append the new obligations to the root FINAL_EFFECTS file")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        db_path = tmp / "router.sqlite3"
        store = RouterStore(db_path)
        terminal_workspace = (tmp / "terminal-workspace").resolve()
        terminal_workspace.mkdir(parents=True, exist_ok=True)
        terminal_effects = _write_final_effects(
            terminal_workspace / "FINAL_EFFECTS.md",
            "Recover the child frontier.\n",
        )
        store.upsert_nodes(
            [
                NodeRuntimeRecord(
                    node_id="70",
                    parent_node_id="0",
                    child_node_ids=["71"],
                    workspace_root=str(terminal_workspace),
                    final_effects_file=str(terminal_effects),
                    split_request=0,
                    split_approved=1,
                    approved_split_request_seq=9,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[],
                    durable_commit="parent-durable",
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.TERMINAL_FAILED,
                            attempt_count=2,
                            pid=7001,
                            process_birth_time=1713000000.0,
                            session_ids=["parent-terminal"],
                            workspace_fingerprint_before="parent-fp",
                            saw_output_in_attempt=True,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=3,
                        )
                    },
                ),
                NodeRuntimeRecord(
                    node_id="71",
                    parent_node_id="70",
                    child_node_ids=[],
                    workspace_root=str(terminal_workspace),
                    final_effects_file=str(terminal_effects),
                    split_request=0,
                    split_approved=0,
                    approved_split_request_seq=0,
                    evaluator_phase="",
                    checker_tasks_ref="",
                    task_result_refs={},
                    reviewer_verdict_kind="",
                    reviewer_report_ref="",
                    pending_prelude_lines=[],
                    current_components=[
                        ActorRef(
                            node_id="71",
                            actor_kind=ActorKind.IMPLEMENTER,
                            attempt_count=3,
                        )
                    ],
                    durable_commit="child-durable",
                    result_commit="",
                    escalated_to_kernel=False,
                    last_rejected_split_diff_fingerprint="",
                    components={
                        component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                            status=ComponentStatus.TERMINAL_FAILED,
                            attempt_count=3,
                            pid=7101,
                            process_birth_time=1713000100.0,
                            session_ids=["child-terminal"],
                            workspace_fingerprint_before="child-fp",
                            saw_output_in_attempt=True,
                            consecutive_no_progress=0,
                            consecutive_failed_exits=3,
                        )
                    },
                ),
            ]
        )
        store.write_router_terminal_state(
            router_status="terminal_failed",
            router_terminal_reason_json='{"reason":"majority_frontier_terminal_failed"}',
            router_terminal_at="2026-04-09T00:00:00+00:00",
        )
        resume_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        resume_payload = resume_core.prepare_resume()
        if str(resume_payload.get("resume_mode") or "") != "terminal_failed":
            return _fail("terminal_failed resume must report terminal_failed resume_mode")
        if int(resume_payload.get("recovered_components") or 0) != 1:
            return _fail("terminal_failed resume must only recover unfinished frontier components")
        if str(store.read_router_status() or "").strip():
            return _fail("terminal_failed resume must clear router_status before runtime restart")
        resumed_parent = store.load_node("70")
        resumed_child = store.load_node("71")
        if resumed_parent is None or resumed_child is None:
            return _fail("terminal_failed resume must preserve durable nodes")
        parent_component = resumed_parent.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
        child_component = resumed_child.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
        if parent_component is None or child_component is None:
            return _fail("terminal_failed resume must preserve implementer component state")
        if parent_component.status is not ComponentStatus.TERMINAL_FAILED:
            return _fail("terminal_failed resume must not revive a split-approved parent that is waiting on child completion")
        if child_component.status is not ComponentStatus.INACTIVE:
            return _fail("terminal_failed resume must reactivate the child frontier by marking it inactive")
        if int(child_component.consecutive_failed_exits) != 0:
            return _fail("terminal_failed resume must reset failed exit counters for recovered components")
        if resumed_child.current_components:
            return _fail("terminal_failed resume must clear recovered current_components so reconcile can relaunch them cleanly")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        repo_root = (tmp / "completed-root-repo").resolve()
        _init_git_repo(repo_root)
        final_effects_file = _write_final_effects(
            repo_root / "FINAL_EFFECTS.md",
            "Original completed obligations.\n",
        )
        _run_git(repo_root, "add", "FINAL_EFFECTS.md")
        _run_git(repo_root, "commit", "-m", "add final effects")
        base_commit = _run_git(repo_root, "rev-parse", "HEAD")
        (repo_root / "deliverable.txt").write_text("earlier root output\n", encoding="utf-8")
        _run_git(repo_root, "add", "deliverable.txt")
        _run_git(repo_root, "commit", "-m", "complete earlier root wave")
        earlier_root_result_commit = _run_git(repo_root, "rev-parse", "HEAD")
        takeover_root = (tmp / "completed-takeover-repo").resolve()
        _init_git_repo(takeover_root)
        takeover_final_effects_file = _write_final_effects(
            takeover_root / "FINAL_EFFECTS.md",
            "Previously completed obligations.\n",
        )
        _run_git(takeover_root, "add", "FINAL_EFFECTS.md")
        _run_git(takeover_root, "commit", "-m", "add takeover final effects")
        takeover_base_commit = _run_git(takeover_root, "rev-parse", "HEAD")
        (takeover_root / "deliverable.txt").write_text("final integrated output\n", encoding="utf-8")
        _run_git(takeover_root, "add", "deliverable.txt")
        _run_git(takeover_root, "commit", "-m", "complete integrated wave")
        result_commit = _run_git(takeover_root, "rev-parse", "HEAD")
        append_ref = _write_file(
            tmp / "append-final-effects.md",
            "New appended obligations.\n",
        )
        db_path = tmp / "completed-router.sqlite3"
        store = RouterStore(db_path)
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="1",
                parent_node_id="0",
                child_node_ids=["71"],
                workspace_root=str(repo_root),
                final_effects_file=str(final_effects_file),
                split_request=1,
                split_approved=1,
                approved_split_request_seq=17,
                evaluator_phase="",
                checker_tasks_ref="/tmp/old-checker.json",
                task_result_refs={"task-1": {"evaluator_tester": "/tmp/old-result.md"}},
                reviewer_verdict_kind="OK",
                reviewer_report_ref="/tmp/old-reviewer.md",
                pending_prelude_lines=[],
                current_components=[],
                durable_commit=base_commit,
                result_commit=earlier_root_result_commit,
                escalated_to_kernel=False,
                last_rejected_split_diff_fingerprint="old-root-fingerprint",
                components={
                    component_key(actor_kind=ActorKind.IMPLEMENTER): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=4,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["old-implementer"],
                        workspace_fingerprint_before="completed-fp",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        store.upsert_node(
            NodeRuntimeRecord(
                node_id="0",
                parent_node_id="0",
                child_node_ids=[],
                workspace_root=str(takeover_root),
                final_effects_file=str(takeover_final_effects_file),
                split_request=0,
                split_approved=0,
                approved_split_request_seq=0,
                evaluator_phase="",
                checker_tasks_ref="",
                task_result_refs={},
                reviewer_verdict_kind="OK",
                reviewer_report_ref="/tmp/final-reviewer.md",
                pending_prelude_lines=[],
                current_components=[],
                durable_commit=takeover_base_commit,
                result_commit=result_commit,
                escalated_to_kernel=True,
                last_rejected_split_diff_fingerprint="kernel-fingerprint",
                components={
                    component_key(actor_kind=ActorKind.KERNEL): ComponentRuntimeState(
                        status=ComponentStatus.COMPLETED,
                        attempt_count=2,
                        pid=0,
                        process_birth_time=None,
                        session_ids=["old-kernel"],
                        workspace_fingerprint_before="kernel-fp",
                        saw_output_in_attempt=True,
                        consecutive_no_progress=0,
                        consecutive_failed_exits=0,
                    )
                },
            )
        )
        store.write_router_completed_state(
            router_status="completed",
            router_completed_result_commit=result_commit,
            router_completed_report_ref="/tmp/final-reviewer.md",
            router_completed_at="2026-04-09T01:00:00+00:00",
        )
        completed_resume_core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(db_path),
        )
        completed_payload = completed_resume_core.prepare_resume(
            append_final_effects_ref=append_ref,
        )
        if str(completed_payload.get("resume_mode") or "") != "completed_append":
            return _fail("completed resume must report completed_append resume_mode")
        resumed_root = store.load_node("1")
        if resumed_root is None:
            return _fail("completed resume must preserve the root node")
        if str(store.read_router_status() or "").strip():
            return _fail("completed resume must clear completed router_status before runtime restart")
        if str(resumed_root.workspace_root or "").strip() != str(takeover_root):
            return _fail("completed resume must reopen root from the final completed workspace")
        if str(Path(resumed_root.final_effects_file).resolve()) != str(takeover_final_effects_file.resolve()):
            return _fail("completed resume must reopen root against the final completed FINAL_EFFECTS file")
        if str(resumed_root.durable_commit or "").strip() != result_commit:
            return _fail("completed resume must promote the final completed result_commit into durable_commit")
        if str(resumed_root.result_commit or "").strip():
            return _fail("completed resume must clear result_commit so a new wave can run")
        if resumed_root.child_node_ids or int(resumed_root.split_request) != 0 or int(resumed_root.split_approved) != 0:
            return _fail("completed resume must reopen the root as a single unsplit node")
        if str(resumed_root.reviewer_verdict_kind or "").strip():
            return _fail("completed resume must clear reviewer verdict state")
        if str(resumed_root.checker_tasks_ref or "").strip():
            return _fail("completed resume must clear checker task graph state")
        if resumed_root.task_result_refs:
            return _fail("completed resume must clear task result refs before the next wave")
        if bool(resumed_root.escalated_to_kernel):
            return _fail("completed resume must clear root escalated_to_kernel state for the next wave")
        implementer_component = resumed_root.components.get(component_key(actor_kind=ActorKind.IMPLEMENTER))
        if implementer_component is None or implementer_component.status is not ComponentStatus.INACTIVE:
            return _fail("completed resume must leave the root implementer inactive so reconcile can relaunch it")
        if not resumed_root.pending_prelude_lines or "previously completed obligations" not in " ".join(resumed_root.pending_prelude_lines).lower():
            return _fail("completed resume must inject a prelude that explains the previously completed obligations")
        resumed_kernel = store.load_node("0")
        if resumed_kernel is None:
            return _fail("completed resume must preserve the historical kernel node record")
        if bool(resumed_kernel.escalated_to_kernel) or str(resumed_kernel.result_commit or "").strip():
            return _fail("completed resume must clear the historical kernel takeover state before the next wave")
        merged_final_effects = takeover_final_effects_file.read_text(encoding="utf-8")
        if "Previously completed obligations." not in merged_final_effects:
            return _fail("completed resume must preserve the final completed FINAL_EFFECTS text")
        if "## Appended Final Effects" not in merged_final_effects or "New appended obligations." not in merged_final_effects:
            return _fail("completed resume must append the new final effects block into the final completed FINAL_EFFECTS file")
        original_root_text = final_effects_file.read_text(encoding="utf-8")
        if "New appended obligations." in original_root_text:
            return _fail("completed resume must not mutate the stale pre-takeover root FINAL_EFFECTS file")

    print("[loop-system-router-core][PASS] core dispatches split reviews FIFO through the node-0 kernel actor, materializes approved children, recovers rejected dead actors, force-terminates approved parents, and retries pending kernel work through ordinary actor recovery")
    return 0


def _sentinel():
    class _Missing:
        split_request = -1
        split_approved = -1
        current_components = []

    return _Missing()


def _component_attempt_count(store, node_id: str, actor_kind) -> int:
    record = store.load_node(node_id)
    if record is None:
        return -1
    component = record.components.get(actor_kind)
    if component is None:
        return -1
    return int(component.attempt_count)


def _wait_until(predicate, *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


if __name__ == "__main__":
    raise SystemExit(main())
