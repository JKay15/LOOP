#!/usr/bin/env python3
"""Validate the committed root-side child supervision helper."""

from __future__ import annotations

import atexit
import argparse
import contextlib
import io
import json
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-child-supervision-helper][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict:
    path = ROOT / "docs" / "schemas" / name
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def _cleanup_runtime_tree(*, state_root: Path, workspace_root: Path) -> None:
    from loop_product.runtime import cleanup_test_runtime_root

    def _safe_rmtree(path: Path) -> None:
        if not path.exists():
            return

        def _onerror(func, target, exc_info):  # type: ignore[no-untyped-def]
            del exc_info
            try:
                os.chmod(target, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
            except OSError:
                return
            try:
                func(target)
            except OSError:
                return

        shutil.rmtree(path, ignore_errors=False, onerror=_onerror)

    try:
        if state_root.exists():
            cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
    except Exception:
        pass
    _safe_rmtree(workspace_root)
    _safe_rmtree(state_root)


def _cleanup_repo_services_for_root() -> None:
    from loop_product.runtime import cleanup_test_repo_services

    def _safe_rmtree(path: Path) -> None:
        if not path.exists():
            return

        def _onerror(func, target, exc_info):  # type: ignore[no-untyped-def]
            del exc_info
            try:
                os.chmod(target, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
            except OSError:
                return
            try:
                func(target)
            except OSError:
                return

        shutil.rmtree(path, ignore_errors=False, onerror=_onerror)

    try:
        cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass
    for base in (ROOT / ".loop", ROOT / "workspace"):
        for path in sorted(base.glob("test-child-supervision-*")):
            try:
                _safe_rmtree(path)
            except Exception:
                pass


def main() -> int:
    from loop_product.runtime import bootstrap_first_implementer_node
    from loop_product.dispatch.supervision import supervise_child_until_settled
    import loop_product.runtime.lifecycle as lifecycle_module

    atexit.register(_cleanup_repo_services_for_root)
    supervision_schema = _load_schema("LoopChildSupervisionResult.schema.json")

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_helper_") as td:
        temp_root = Path(td)
        endpoint = temp_root / "EndpointArtifact.json"
        endpoint.write_text(
            json.dumps(
                {
                    "version": "1",
                    "session_root": str((temp_root / "endpoint_session").resolve()),
                    "artifact_ref": str(endpoint.resolve()),
                    "latest_turn_ref": str((temp_root / "turns" / "0001" / "TurnResult.json").resolve()),
                    "mode": "VISION_COMPILER",
                    "status": "CLARIFIED",
                    "original_user_prompt": "Deliver one local Lean formalization artifact.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "formalization",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Deliver one local Lean formalization artifact.",
                        "final_effect": "Deliver one local Lean formalization artifact.",
                        "observable_success_criteria": ["A local Lean artifact exists."],
                        "hard_constraints": ["Stay local."],
                        "non_goals": [],
                        "relevant_context": [],
                        "open_questions": [],
                        "artifact_ready_for_persistence": True,
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        workspace_root = ROOT / "workspace" / "test-child-supervision-helper"
        state_root = ROOT / ".loop" / "test-child-supervision-helper"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-helper",
            root_goal="bootstrap one implementer node for child supervision helper validation",
            child_goal_slice="repair unmet evaluator requirements until the run settles",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        implementer_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "REPAIR_REQUIRED",
                    "evaluator_result": {
                        "verdict": "FAIL",
                        "retryable": True,
                        "summary": "simple evaluator reviewer verdict=FAIL",
                        "diagnostics": {
                            "self_attribution": "implementation_gap",
                            "self_repair": "repair the unmet in-scope requirements and rerun evaluator",
                        },
                    },
                    "evaluation_report_ref": str((temp_root / "EvaluationReport.json").resolve()),
                    "reviewer_response_ref": str((temp_root / "reviewer.txt").resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_2 = state_root / "artifacts" / "launches" / node_id / "attempt_002" / "ChildLaunchResult.json"
        launch_result_ref_2.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_2.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_2.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        status_calls = {"count": 0}
        recovery_calls: list[dict[str, object]] = []
        launch_calls: list[str] = []

        def _status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            status_calls["count"] += 1
            status_ref = temp_root / f"status-{status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": True,
                "stall_threshold_s": stall_threshold_s,
                "recovery_reason": "active_without_live_pid",
            }

        def _recovery_runner(**payload):
            recovery_calls.append(dict(payload))
            recovery_result_ref = state_root / "artifacts" / "recovery" / node_id / "OrphanedActiveRecoveryResult.json"
            recovery_result_ref.parent.mkdir(parents=True, exist_ok=True)
            recovery_payload = {
                "recovery_result_ref": str(recovery_result_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "recovery_decision": "accepted",
            }
            recovery_result_ref.write_text(json.dumps(recovery_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return recovery_payload

        def _launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            launch_calls.append(str(result_ref))
            implementer_result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": node_id,
                        "status": "COMPLETED",
                        "outcome": "COMPLETED",
                        "evaluator_result": {
                            "verdict": "PASS",
                            "retryable": False,
                            "summary": "simple evaluator reviewer verdict=PASS",
                            "diagnostics": {"self_attribution": "", "self_repair": ""},
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            return {"launch_result_ref": str(launch_result_ref_2.resolve())}

        result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=0.0,
            max_recoveries=2,
            max_wall_clock_s=0.0,
            runtime_status_reader=_status_reader,
            recovery_runner=_recovery_runner,
            launcher=_launcher,
            sleep_fn=lambda _: None,
        )
        Draft202012Validator(supervision_schema).validate(result)

        if str(result.get("settled_reason") or "") != "pass":
            return _fail("child supervision helper must settle as pass after successful repaired relaunch")
        if int(result.get("recoveries_used") or 0) != 1:
            return _fail("child supervision helper must count exactly one recover/relaunch cycle in the happy path")
        if len(recovery_calls) != 1:
            return _fail("child supervision helper must invoke same-node recovery once for a retryable terminal FAIL")
        if len(launch_calls) != 1:
            return _fail("child supervision helper must relaunch the same node once after accepted recovery")
        if str(recovery_calls[0].get("reason") or "") != "retryable_terminal_result_after_child_stop":
            return _fail("child supervision helper must classify retryable terminal FAILs with a generic retryable-terminal reason")
        if str(recovery_calls[0].get("self_repair") or "") == "":
            return _fail("child supervision helper must forward a non-empty self-repair hint into recovery")
        if str(recovery_calls[0].get("confirmed_launch_result_ref") or "") != str(launch_result_ref_1.resolve()):
            return _fail("child supervision helper must forward the exact confirmed launch result into recovery")
        if str(recovery_calls[0].get("confirmed_runtime_status_ref") or "") == "":
            return _fail("child supervision helper must forward the exact confirmed runtime status into recovery")
        if str(result.get("latest_launch_result_ref") or "") != str(launch_result_ref_2.resolve()):
            return _fail("child supervision helper must update the settled launch_result_ref after relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-recovery-resilience"
        state_root = ROOT / ".loop" / "test-child-supervision-recovery-resilience"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-recovery-resilience",
            root_goal="bootstrap one implementer node for recovery-exception resilience validation",
            child_goal_slice="keep supervising even if a recovery subcall transiently raises",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out-recovery-resilience.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        implementer_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "REPAIR_REQUIRED",
                    "evaluator_result": {
                        "verdict": "FAIL",
                        "retryable": True,
                        "summary": "simple evaluator reviewer verdict=FAIL",
                        "diagnostics": {
                            "self_attribution": "implementation_gap",
                            "self_repair": "repair the unmet in-scope requirements and rerun evaluator",
                        },
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_2 = state_root / "artifacts" / "launches" / node_id / "attempt_002" / "ChildLaunchResult.json"
        launch_result_ref_2.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_2.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_2.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        resilient_recovery_calls: list[dict[str, object]] = []
        resilient_launch_calls: list[str] = []

        def _resilient_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            return {
                "launch_result_ref": str(Path(result_ref).resolve()),
                "status_result_ref": str((temp_root / "resilient-status.json").resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": True,
                "stall_threshold_s": stall_threshold_s,
                "recovery_reason": "active_without_live_pid",
            }

        def _resilient_recovery_runner(**payload):
            resilient_recovery_calls.append(dict(payload))
            if len(resilient_recovery_calls) == 1:
                raise ValueError("stale confirmed runtime status rejected by recovery helper")
            recovery_result_ref = state_root / "artifacts" / "recovery" / node_id / "OrphanedActiveRecoveryResult.json"
            recovery_result_ref.parent.mkdir(parents=True, exist_ok=True)
            recovery_payload = {
                "recovery_result_ref": str(recovery_result_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "recovery_decision": "accepted",
            }
            recovery_result_ref.write_text(json.dumps(recovery_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return recovery_payload

        def _resilient_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            resilient_launch_calls.append(str(result_ref))
            implementer_result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": node_id,
                        "status": "COMPLETED",
                        "outcome": "COMPLETED",
                        "evaluator_result": {
                            "verdict": "PASS",
                            "retryable": False,
                            "summary": "simple evaluator reviewer verdict=PASS",
                            "diagnostics": {"self_attribution": "", "self_repair": ""},
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            return {"launch_result_ref": str(launch_result_ref_2.resolve())}

        resilient_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=0.0,
            max_recoveries=2,
            max_wall_clock_s=0.0,
            runtime_status_reader=_resilient_status_reader,
            recovery_runner=_resilient_recovery_runner,
            launcher=_resilient_launcher,
            sleep_fn=lambda _: None,
        )
        Draft202012Validator(supervision_schema).validate(resilient_result)

        if str(resilient_result.get("settled_reason") or "") != "pass":
            return _fail("child supervision helper must keep supervising after a transient recovery exception and eventually settle as pass")
        if len(resilient_recovery_calls) != 2:
            return _fail("child supervision helper must retry recovery after a transient recovery exception instead of crashing the sidecar loop")
        if len(resilient_launch_calls) != 1:
            return _fail("child supervision helper must only relaunch after the later accepted recovery succeeds")
        if int(resilient_result.get("recoveries_used") or 0) != 1:
            return _fail("child supervision helper must count only accepted recovery/relaunch cycles, not transient recovery exceptions")

        workspace_root = ROOT / "workspace" / "test-child-supervision-authoritative-blocked"
        state_root = ROOT / ".loop" / "test-child-supervision-authoritative-blocked"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-authoritative-blocked",
            root_goal="bootstrap one implementer node for authoritative blocked supervision validation",
            child_goal_slice="surface an authoritative blocked branch result without same-node recovery churn",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out-blocked.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        implementer_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "BLOCKED",
                    "outcome": "BLOCKED",
                    "summary": "authoritative blocked branch result",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_blocked = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_blocked.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_blocked.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_blocked.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        blocked_recovery_calls: list[dict[str, object]] = []
        blocked_launch_calls: list[str] = []

        def _blocked_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            return {
                "launch_result_ref": str(Path(result_ref).resolve()),
                "status_result_ref": str((temp_root / "blocked-status.json").resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "recovery_reason": "node_status_blocked",
                "lifecycle_status": "BLOCKED",
                "runtime_attachment_state": "TERMINAL",
            }

        def _blocked_recovery_runner(**payload):
            blocked_recovery_calls.append(dict(payload))
            raise AssertionError("authoritative blocked child result must not invoke same-node recovery")

        def _blocked_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            blocked_launch_calls.append(str(result_ref))
            raise AssertionError("authoritative blocked child result must not relaunch")

        blocked_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_blocked,
            poll_interval_s=0.0,
            stall_threshold_s=0.0,
            max_recoveries=1,
            max_wall_clock_s=0.0,
            runtime_status_reader=_blocked_status_reader,
            recovery_runner=_blocked_recovery_runner,
            launcher=_blocked_launcher,
            sleep_fn=lambda _: None,
        )
        Draft202012Validator(supervision_schema).validate(blocked_result)
        if str(blocked_result.get("settled_reason") or "") != "authoritative_result":
            return _fail("child supervision helper must settle authoritative blocked results without recovery churn")
        if blocked_recovery_calls or blocked_launch_calls:
            return _fail("authoritative blocked result must not trigger recovery or relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-split-continuation"
        state_root = ROOT / ".loop" / "test-child-supervision-split-continuation"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-split-continuation",
            root_goal="bootstrap one implementer node for split-continuation supervision validation",
            child_goal_slice="stop at an authoritative split-continuation result without attempting orphaned-active recovery",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-split-continue").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        implementer_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "IN_PROGRESS",
                    "outcome": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
                    "summary": "authoritative split-continuation result published a deferred split release point",
                    "split_state": {
                        "proposed": True,
                        "accepted": True,
                        "mode": "deferred",
                        "accepted_target_nodes": ["child-linear-001"],
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_split = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_split.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_split.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_split.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        split_continuation_recovery_calls: list[dict[str, object]] = []
        split_continuation_launch_calls: list[str] = []

        def _split_continuation_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            return {
                "launch_result_ref": str(Path(result_ref).resolve()),
                "status_result_ref": str((temp_root / "split-continuation-status.json").resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "ACTIVE",
                "runtime_attachment_state": "UNOBSERVED",
                "recovery_reason": "active_without_live_pid_unconfirmed",
            }

        def _split_continuation_recovery_runner(**payload):
            split_continuation_recovery_calls.append(dict(payload))
            raise AssertionError("split-continuation result must not route into orphaned-active recovery")

        def _split_continuation_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            split_continuation_launch_calls.append("called")
            raise AssertionError("split-continuation result must not relaunch")

        split_continuation_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_split,
            poll_interval_s=0.0,
            stall_threshold_s=0.0,
            max_recoveries=1,
            max_wall_clock_s=0.0,
            runtime_status_reader=_split_continuation_status_reader,
            recovery_runner=_split_continuation_recovery_runner,
            launcher=_split_continuation_launcher,
            sleep_fn=lambda _: None,
        )
        Draft202012Validator(supervision_schema).validate(split_continuation_result)
        if str(split_continuation_result.get("settled_reason") or "") != "authoritative_result":
            return _fail("split-continuation result must settle through authoritative-result handling")
        if split_continuation_recovery_calls or split_continuation_launch_calls:
            return _fail("split-continuation result must not trigger recovery or relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-provider-capacity"
        state_root = ROOT / ".loop" / "test-child-supervision-provider-capacity"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-provider-capacity",
            root_goal="bootstrap one implementer node for provider-capacity cooldown validation",
            child_goal_slice="continue after transient provider-capacity launch failure until the run settles",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out-capacity.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "retry_exhausted",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "attempt_count": 2,
                    "startup_retry_limit": 1,
                    "retryable_failure_kind": "provider_capacity",
                    "attempts": [
                        {"attempt_index": 1, "retryable_failure_kind": "provider_capacity", "exit_code": 1},
                        {"attempt_index": 2, "retryable_failure_kind": "provider_capacity", "exit_code": 1},
                    ],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_2 = state_root / "artifacts" / "launches" / node_id / "attempt_002" / "ChildLaunchResult.json"
        launch_result_ref_2.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_2.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_2.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        capacity_status_calls = {"count": 0}
        capacity_recovery_calls: list[dict[str, object]] = []
        capacity_launch_calls: list[str] = []
        capacity_sleep_calls: list[float] = []

        def _capacity_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            capacity_status_calls["count"] += 1
            status_ref = temp_root / f"capacity-status-{capacity_status_calls['count']}.json"
            if capacity_status_calls["count"] == 1:
                return {
                    "launch_result_ref": launch_result_ref,
                    "status_result_ref": str(status_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "pid_alive": False,
                    "recovery_eligible": True,
                    "stall_threshold_s": stall_threshold_s,
                    "recovery_reason": "active_without_live_pid",
                }
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "recovery_reason": "",
            }

        def _capacity_recovery_runner(**payload):
            capacity_recovery_calls.append(dict(payload))
            recovery_result_ref = state_root / "artifacts" / "recovery" / node_id / "OrphanedActiveRecoveryResult.json"
            recovery_result_ref.parent.mkdir(parents=True, exist_ok=True)
            recovery_payload = {
                "recovery_result_ref": str(recovery_result_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "recovery_decision": "accepted",
            }
            recovery_result_ref.write_text(json.dumps(recovery_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return recovery_payload

        def _capacity_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            capacity_launch_calls.append(str(result_ref))
            implementer_result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": node_id,
                        "status": "COMPLETED",
                        "outcome": "COMPLETED",
                        "evaluator_result": {
                            "verdict": "PASS",
                            "retryable": False,
                            "summary": "simple evaluator reviewer verdict=PASS",
                            "diagnostics": {"self_attribution": "", "self_repair": ""},
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            return {"launch_result_ref": str(launch_result_ref_2.resolve())}

        capacity_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=0.0,
            max_recoveries=2,
            max_wall_clock_s=0.0,
            runtime_status_reader=_capacity_status_reader,
            recovery_runner=_capacity_recovery_runner,
            launcher=_capacity_launcher,
            sleep_fn=capacity_sleep_calls.append,
        )
        Draft202012Validator(supervision_schema).validate(capacity_result)

        if str(capacity_result.get("settled_reason") or "") != "pass":
            return _fail("provider-capacity recovery path must still settle as pass after successful relaunch")
        if len(capacity_recovery_calls) != 1 or len(capacity_launch_calls) != 1:
            return _fail("provider-capacity path must still perform exactly one recovery/relaunch cycle in this fixture")
        if not capacity_sleep_calls:
            return _fail("child supervision helper must apply a bounded cooldown before recovering from provider-capacity launch churn")
        if abs(float(capacity_sleep_calls[0]) - 0.75) > 1e-9:
            return _fail("child supervision helper must apply the expected first provider-capacity cooldown before relaunch")
        if str(capacity_result.get("latest_launch_result_ref") or "") != str(launch_result_ref_2.resolve()):
            return _fail("provider-capacity recovery path must still update the settled launch_result_ref after relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-incomplete-terminal"
        state_root = ROOT / ".loop" / "test-child-supervision-incomplete-terminal"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-incomplete-terminal",
            root_goal="bootstrap one implementer node for incomplete-terminal-result supervision validation",
            child_goal_slice="recover when a stopped child leaves a completed-looking result without evaluator-backed closure",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out-incomplete.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        implementer_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "COMPLETED",
                    "delivered_artifact_exactly_evaluated": True,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        launch_result_ref_2 = state_root / "artifacts" / "launches" / node_id / "attempt_002" / "ChildLaunchResult.json"
        launch_result_ref_2.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_2.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_2.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        incomplete_status_calls = {"count": 0}
        incomplete_recovery_calls: list[dict[str, object]] = []
        incomplete_launch_calls: list[str] = []
        evaluation_report_ref = temp_root / "incomplete-terminal" / "EvaluationReport.json"
        evaluation_report_ref.parent.mkdir(parents=True, exist_ok=True)

        def _incomplete_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            incomplete_status_calls["count"] += 1
            status_ref = temp_root / f"incomplete-status-{incomplete_status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "COMPLETED",
                "recovery_reason": "authoritative_terminal_result",
            }

        def _incomplete_recovery_runner(**payload):
            incomplete_recovery_calls.append(dict(payload))
            recovery_result_ref = state_root / "artifacts" / "recovery" / node_id / "OrphanedActiveRecoveryResult.json"
            recovery_result_ref.parent.mkdir(parents=True, exist_ok=True)
            recovery_payload = {
                "recovery_result_ref": str(recovery_result_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "recovery_decision": "accepted",
            }
            recovery_result_ref.write_text(json.dumps(recovery_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            return recovery_payload

        def _incomplete_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del startup_probe_ms, startup_health_timeout_ms
            incomplete_launch_calls.append(str(result_ref))
            evaluation_report_ref.write_text(
                json.dumps({"status": "COMPLETED", "verdict": "PASS"}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            implementer_result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": node_id,
                        "status": "COMPLETED",
                        "outcome": "COMPLETED",
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "evaluator_result": {
                            "verdict": "PASS",
                            "retryable": False,
                            "summary": "simple evaluator reviewer verdict=PASS",
                            "diagnostics": {"self_attribution": "", "self_repair": ""},
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            return {"launch_result_ref": str(launch_result_ref_2.resolve())}

        incomplete_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=0.0,
            max_recoveries=2,
            max_wall_clock_s=0.01,
            runtime_status_reader=_incomplete_status_reader,
            recovery_runner=_incomplete_recovery_runner,
            launcher=_incomplete_launcher,
            sleep_fn=lambda _: None,
        )
        Draft202012Validator(supervision_schema).validate(incomplete_result)
        if str(incomplete_result.get("settled_reason") or "") != "pass":
            return _fail("stopped child with incomplete/manual implementer_result must recover and settle after a valid relaunch")
        if len(incomplete_recovery_calls) != 1 or len(incomplete_launch_calls) != 1:
            return _fail("incomplete/manual implementer_result must trigger exactly one recovery/relaunch cycle in this fixture")
        if str(incomplete_result.get("latest_launch_result_ref") or "") != str(launch_result_ref_2.resolve()):
            return _fail("incomplete/manual implementer_result recovery path must update the settled launch_result_ref after relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-no-substantive-progress"
        state_root = ROOT / ".loop" / "test-child-supervision-no-substantive-progress"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-no-substantive-progress",
            root_goal="bootstrap one implementer node for false-liveness supervision validation",
            child_goal_slice="stop truthfully when a live child keeps reporting only placeholder staged progress without any substantive change",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-no-progress").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        no_progress_status_calls = {"count": 0}
        no_progress_recovery_calls: list[dict[str, object]] = []
        no_progress_launch_calls: list[str] = []
        no_progress_terminator_calls: list[str] = []
        no_progress_now = {"t": 1000.0}
        no_progress_pid_alive = {"value": True}

        def _no_progress_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            no_progress_status_calls["count"] += 1
            status_ref = temp_root / f"no-progress-status-{no_progress_status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": bool(no_progress_pid_alive["value"]),
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "ACTIVE" if no_progress_pid_alive["value"] else "BLOCKED",
                "recovery_reason": "live_pid_still_attached",
                "latest_log_age_s": 0.1,
            }

        stagnant_snapshot = {
            "node_id": node_id,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "pid_alive": True,
            "lifecycle_status": "ACTIVE",
            "phase_hint": "IMPLEMENTING",
            "terminal_result_present": False,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "implementer_verdict": "",
            "implementer_retryable": False,
            "evaluator_status": "",
            "evaluator_running_unit_ids": [],
            "evaluator_completed_unit_ids": [],
            "evaluator_blocked_retryable_unit_ids": [],
            "evaluator_blocked_terminal_unit_ids": [],
            "evaluator_pending_unit_ids": [],
            "observed_doc_refs": [
                str((workspace_root / "deliverables" / "primary_artifact" / "README.md").resolve()),
                str((workspace_root / "deliverables" / "primary_artifact" / "TRACEABILITY.md").resolve()),
                str((workspace_root / "deliverables" / "primary_artifact" / "formalization" / "STATUS.md").resolve()),
            ],
            "recent_log_lines": [
                "Workspace-local artifact skeleton created.",
                "No split has been proposed yet.",
                "Evaluator has not been run yet for this node.",
            ],
        }

        def _no_progress_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            return dict(stagnant_snapshot)

        def _no_progress_recovery_runner(**payload):
            no_progress_recovery_calls.append(dict(payload))
            return {}

        def _no_progress_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            no_progress_launch_calls.append("unexpected")
            return {}

        def _no_progress_terminator(*, result_ref: str | Path):
            no_progress_terminator_calls.append(str(Path(result_ref).resolve()))
            no_progress_pid_alive["value"] = False

        no_progress_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=2,
            max_wall_clock_s=0.0,
            runtime_status_reader=_no_progress_status_reader,
            recovery_runner=_no_progress_recovery_runner,
            launcher=_no_progress_launcher,
            launch_terminator=_no_progress_terminator,
            progress_snapshot_reader=_no_progress_snapshot_reader,
            no_substantive_progress_window_s=5.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: no_progress_now["t"],
            sleep_fn=lambda seconds: no_progress_now.__setitem__("t", no_progress_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(no_progress_result)
        if str(no_progress_result.get("settled_reason") or "") != "no_substantive_progress":
            return _fail("supervision helper must settle to a truthful no_substantive_progress stop point when a live child shows unchanged placeholder-only progress")
        if not bool(no_progress_result.get("settled")):
            return _fail("no-substantive-progress stop points must be settled only after the live child has been stopped")
        if no_progress_recovery_calls:
            return _fail("no-substantive-progress stop points must not pretend the child is recovery-eligible")
        if no_progress_launch_calls:
            return _fail("no-substantive-progress stop points must not relaunch the child automatically")
        if no_progress_terminator_calls != [str(launch_result_ref_1.resolve())]:
            return _fail("no-substantive-progress stop points must terminate the live child before returning")

        workspace_root = ROOT / "workspace" / "test-child-supervision-empty-workspace-false-liveness"
        state_root = ROOT / ".loop" / "test-child-supervision-empty-workspace-false-liveness"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-empty-workspace-false-liveness",
            root_goal="bootstrap one implementer node for empty-workspace false-liveness validation",
            child_goal_slice="stop truthfully when a live child keeps promising the first artifact batch but never materializes anything under the frozen workspace mirror",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-empty-workspace").resolve()),
            context_refs=[],
            startup_required_output_paths=[
                "README.md",
                "WHOLE_PAPER_STATUS.json",
                "extraction/source_structure.json",
            ],
        )

        startup_live_root = Path(str(bootstrap.get("workspace_live_artifact_ref") or "")).resolve()
        startup_live_root.mkdir(parents=True, exist_ok=True)
        (startup_live_root / "notes.txt").write_text("unrelated placeholder\n", encoding="utf-8")

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        empty_workspace_status_calls = {"count": 0}
        empty_workspace_recovery_calls: list[dict[str, object]] = []
        empty_workspace_launch_calls: list[str] = []
        empty_workspace_terminator_calls: list[str] = []
        empty_workspace_now = {"t": 2000.0}
        empty_workspace_pid_alive = {"value": True}

        def _empty_workspace_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            empty_workspace_status_calls["count"] += 1
            status_ref = temp_root / f"empty-workspace-status-{empty_workspace_status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": bool(empty_workspace_pid_alive["value"]),
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "ACTIVE" if empty_workspace_pid_alive["value"] else "BLOCKED",
                "recovery_reason": "live_pid_still_attached",
                "latest_log_age_s": 0.1,
            }

        empty_workspace_snapshot = {
            "node_id": node_id,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "pid_alive": True,
            "lifecycle_status": "ACTIVE",
            "phase_hint": "IMPLEMENTING",
            "terminal_result_present": False,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "implementer_verdict": "",
            "implementer_retryable": False,
            "evaluator_status": "",
            "evaluator_running_unit_ids": [],
            "evaluator_completed_unit_ids": [],
            "evaluator_blocked_retryable_unit_ids": [],
            "evaluator_blocked_terminal_unit_ids": [],
            "evaluator_pending_unit_ids": [],
            "observed_doc_refs": [
                str((ROOT / "loop_product_repo" / ".agents" / "skills" / "loop-runner" / "SKILL.md").resolve()),
                str((ROOT / "loop_product_repo" / "workspace" / "AGENTS.md").resolve()),
                str((workspace_root / "FROZEN_HANDOFF.md").resolve()),
                str((ROOT / ".cache" / "leanatlas" / "tmp" / "arxiv_2602_11505v2" / "source" / "main.tex").resolve()),
                str((ROOT / "loop_product_repo" / ".agents" / "skills" / "evaluator-exec" / "SKILL.md").resolve()),
                str((ROOT / "loop_product_repo" / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()),
            ],
            "recent_log_lines": [
                "Plan update",
                "Inspect the frozen TeX source and extract theorem/definition/section structure needed for the first artifact batch",
                "Materialize the workspace mirror artifact with source-backed extraction outputs and whole-paper status evidence",
                "I’m writing the first real artifact batch now: a source-derived inventory of sections, theorem-like statements, labels, refs, and an initial whole-paper status file inside `deliverables/primary_artifact`.",
            ],
        }

        def _empty_workspace_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            return dict(empty_workspace_snapshot)

        def _empty_workspace_recovery_runner(**payload):
            empty_workspace_recovery_calls.append(dict(payload))
            return {}

        def _empty_workspace_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            empty_workspace_launch_calls.append("unexpected")
            return {}

        def _empty_workspace_terminator(*, result_ref: str | Path):
            empty_workspace_terminator_calls.append(str(Path(result_ref).resolve()))
            empty_workspace_pid_alive["value"] = False

        empty_workspace_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=2,
            max_wall_clock_s=6.0,
            runtime_status_reader=_empty_workspace_status_reader,
            recovery_runner=_empty_workspace_recovery_runner,
            launcher=_empty_workspace_launcher,
            launch_terminator=_empty_workspace_terminator,
            progress_snapshot_reader=_empty_workspace_snapshot_reader,
            no_substantive_progress_window_s=5.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: empty_workspace_now["t"],
            sleep_fn=lambda seconds: empty_workspace_now.__setitem__("t", empty_workspace_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(empty_workspace_result)
        if str(empty_workspace_result.get("settled_reason") or "") != "no_substantive_progress":
            return _fail(
                "supervision helper must stop a live child whose startup_required_output_paths remain missing even if unrelated files appear under the live artifact root"
            )
        if not bool(empty_workspace_result.get("settled")):
            return _fail("empty-workspace no-substantive-progress stop points must be settled only after the live child has been stopped")
        if empty_workspace_recovery_calls:
            return _fail("empty-workspace false-liveness stop points must not pretend the child is recovery-eligible")
        if empty_workspace_launch_calls:
            return _fail("empty-workspace false-liveness stop points must not relaunch the child automatically")
        if empty_workspace_terminator_calls != [str(launch_result_ref_1.resolve())]:
            return _fail("empty-workspace false-liveness stop points must terminate the live child before returning")

        workspace_root = ROOT / "workspace" / "test-child-supervision-xhigh-startup-grace"
        state_root = ROOT / ".loop" / "test-child-supervision-xhigh-startup-grace"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-xhigh-startup-grace",
            root_goal="bootstrap one implementer node for reasoning-aware startup grace validation",
            child_goal_slice="allow bounded xhigh startup reading before declaring empty-workspace no substantive progress",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-xhigh-startup").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        xhigh_status_calls = {"count": 0}
        xhigh_recovery_calls: list[dict[str, object]] = []
        xhigh_launch_calls: list[str] = []
        xhigh_terminator_calls: list[str] = []
        xhigh_now = {"t": 3000.0}
        xhigh_pid_alive = {"value": True}

        def _xhigh_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            xhigh_status_calls["count"] += 1
            status_ref = temp_root / f"xhigh-startup-status-{xhigh_status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": bool(xhigh_pid_alive["value"]),
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "ACTIVE" if xhigh_pid_alive["value"] else "BLOCKED",
                "recovery_reason": "live_pid_still_attached",
                "latest_log_age_s": 0.1,
            }

        xhigh_snapshot = {
            "node_id": node_id,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "pid_alive": True,
            "lifecycle_status": "ACTIVE",
            "phase_hint": "IMPLEMENTING",
            "terminal_result_present": False,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "implementer_verdict": "",
            "implementer_retryable": False,
            "evaluator_status": "",
            "evaluator_running_unit_ids": [],
            "evaluator_completed_unit_ids": [],
            "evaluator_blocked_retryable_unit_ids": [],
            "evaluator_blocked_terminal_unit_ids": [],
            "evaluator_pending_unit_ids": [],
            "observed_doc_refs": [
                str((workspace_root / "FROZEN_HANDOFF.md").resolve()),
                str((ROOT / ".cache" / "leanatlas" / "tmp" / "arxiv_2602_11505v2" / "source" / "main.tex").resolve()),
                str((ROOT / "loop_product_repo" / ".agents" / "skills" / "loop-runner" / "SKILL.md").resolve()),
                str((ROOT / "loop_product_repo" / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()),
            ],
            "recent_log_lines": [
                "Read the frozen handoff and the exact whole-paper benchmark docs before starting the first extraction batch.",
                "Inspect the whole-paper source and baseline refs needed for the first source-backed extraction artifact batch.",
                "I am still in the bounded xhigh startup pass and have not written the first artifact batch yet.",
            ],
        }

        def _xhigh_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            return dict(xhigh_snapshot)

        def _xhigh_recovery_runner(**payload):
            xhigh_recovery_calls.append(dict(payload))
            return {}

        def _xhigh_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            xhigh_launch_calls.append("unexpected")
            return {}

        def _xhigh_terminator(*, result_ref: str | Path):
            xhigh_terminator_calls.append(str(Path(result_ref).resolve()))
            xhigh_pid_alive["value"] = False

        xhigh_not_yet_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=10.0,
            stall_threshold_s=60.0,
            max_recoveries=2,
            max_wall_clock_s=70.0,
            runtime_status_reader=_xhigh_status_reader,
            recovery_runner=_xhigh_recovery_runner,
            launcher=_xhigh_launcher,
            launch_terminator=_xhigh_terminator,
            progress_snapshot_reader=_xhigh_snapshot_reader,
            no_substantive_progress_window_s=300.0,
            now_fn=lambda: xhigh_now["t"],
            sleep_fn=lambda seconds: xhigh_now.__setitem__("t", xhigh_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(xhigh_not_yet_result)
        if str(xhigh_not_yet_result.get("settled_reason") or "") != "timeout":
            return _fail("xhigh startup should keep a larger bounded grace window before empty-workspace no-substantive-progress fires")
        if xhigh_recovery_calls or xhigh_launch_calls or xhigh_terminator_calls:
            return _fail("xhigh startup grace should not trigger recovery, relaunch, or termination in the pre-grace fixture")

        xhigh_now["t"] = 3000.0
        xhigh_status_calls["count"] = 0
        xhigh_recovery_calls.clear()
        xhigh_launch_calls.clear()
        xhigh_terminator_calls.clear()
        xhigh_pid_alive["value"] = True
        xhigh_stop_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=10.0,
            stall_threshold_s=60.0,
            max_recoveries=2,
            max_wall_clock_s=220.0,
            runtime_status_reader=_xhigh_status_reader,
            recovery_runner=_xhigh_recovery_runner,
            launcher=_xhigh_launcher,
            launch_terminator=_xhigh_terminator,
            progress_snapshot_reader=_xhigh_snapshot_reader,
            no_substantive_progress_window_s=300.0,
            now_fn=lambda: xhigh_now["t"],
            sleep_fn=lambda seconds: xhigh_now.__setitem__("t", xhigh_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(xhigh_stop_result)
        if str(xhigh_stop_result.get("settled_reason") or "") != "no_substantive_progress":
            return _fail("xhigh startup must still settle to no_substantive_progress after the larger bounded grace window expires")
        if not bool(xhigh_stop_result.get("settled")):
            return _fail("xhigh startup no-substantive-progress stop points must be settled only after the live child has been stopped")
        if xhigh_recovery_calls or xhigh_launch_calls:
            return _fail("xhigh startup no-substantive-progress stop points must not pretend the child is recovery-eligible")
        if xhigh_terminator_calls != [str(launch_result_ref_1.resolve())]:
            return _fail("xhigh startup no-substantive-progress stop points must terminate the live child before returning")

        for checkpoint_scope, workflow_scope, artifact_scope, terminal_scope, required_status_name in (
            ("source", "generic", "task", "local", "IMPLEMENTING"),
            ("slice", "whole_paper_formalization", "slice", "local", "SLICE_STARTUP"),
        ):
            workspace_root = ROOT / "workspace" / f"test-child-supervision-xhigh-{checkpoint_scope}-checkpoint-grace"
            state_root = ROOT / ".loop" / f"test-child-supervision-xhigh-{checkpoint_scope}-checkpoint-grace"
            _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug=f"test-child-supervision-xhigh-{checkpoint_scope}-checkpoint-grace",
                root_goal=f"bootstrap one implementer node for reasoning-aware {checkpoint_scope} checkpoint grace validation",
                child_goal_slice=(
                    "treat sustained xhigh checkpoint-phase progress as bounded activity instead of immediately killing the child on the flat checkpoint window"
                ),
                workflow_scope=workflow_scope,
                artifact_scope=artifact_scope,
                terminal_authority_scope=terminal_scope,
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/primary_artifact",
                external_publish_target=str((temp_root / "Desktop" / f"out-xhigh-{checkpoint_scope}-checkpoint").resolve()),
                context_refs=[],
                startup_required_output_paths=[
                    "README.md",
                    "TRACEABILITY.md",
                    "analysis/STARTUP_BOUNDARY.json",
                ],
                progress_checkpoints=[
                    {
                        "checkpoint_id": f"{checkpoint_scope}_decision",
                        "description": "record the first machine-auditable execution decision after startup",
                        "required_any_of": [
                            "analysis/EXECUTION_DECISION.json",
                        ],
                        "window_s": 5.0,
                    }
                ],
            )

            node_id = str(bootstrap["node_id"])
            launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
            launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
            launch_result_ref_1.write_text(
                json.dumps(
                    {
                        "launch_result_ref": str(launch_result_ref_1.resolve()),
                        "launch_decision": "started",
                        "node_id": node_id,
                        "state_root": str(state_root.resolve()),
                        "workspace_root": str(workspace_root.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            checkpoint_live_root = Path(str(bootstrap.get("workspace_live_artifact_ref") or "")).resolve()
            checkpoint_live_root.mkdir(parents=True, exist_ok=True)
            (checkpoint_live_root / "README.md").write_text("# startup batch\n", encoding="utf-8")
            (checkpoint_live_root / "TRACEABILITY.md").write_text("| artifact | status |\n| --- | --- |\n", encoding="utf-8")
            (checkpoint_live_root / "analysis").mkdir(parents=True, exist_ok=True)
            (checkpoint_live_root / "analysis" / "STARTUP_BOUNDARY.json").write_text(
                json.dumps({"scope": checkpoint_scope}) + "\n",
                encoding="utf-8",
            )

            checkpoint_status_calls = {"count": 0}
            checkpoint_recovery_calls: list[dict[str, object]] = []
            checkpoint_launch_calls: list[str] = []
            checkpoint_terminator_calls: list[str] = []
            checkpoint_now = {"t": 4000.0}
            checkpoint_pid_alive = {"value": True}

            def _checkpoint_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                launch_result_ref = str(Path(result_ref).resolve())
                checkpoint_status_calls["count"] += 1
                status_ref = temp_root / f"xhigh-{checkpoint_scope}-checkpoint-status-{checkpoint_status_calls['count']}.json"
                return {
                    "launch_result_ref": launch_result_ref,
                    "status_result_ref": str(status_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "pid_alive": bool(checkpoint_pid_alive["value"]),
                    "recovery_eligible": False,
                    "stall_threshold_s": stall_threshold_s,
                    "lifecycle_status": "ACTIVE" if checkpoint_pid_alive["value"] else "BLOCKED",
                    "recovery_reason": "live_pid_still_attached",
                    "latest_log_age_s": 0.1,
                }

            def _checkpoint_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                del result_ref, stall_threshold_s
                tick = checkpoint_status_calls["count"]
                return {
                    "node_id": node_id,
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "pid_alive": bool(checkpoint_pid_alive["value"]),
                    "lifecycle_status": "ACTIVE",
                    "phase_hint": "IMPLEMENTING",
                    "terminal_result_present": False,
                    "implementer_result_ref": "",
                    "implementer_outcome": "",
                    "implementer_verdict": "",
                    "implementer_retryable": False,
                    "evaluator_status": "",
                    "evaluator_running_unit_ids": [],
                    "evaluator_completed_unit_ids": [],
                    "evaluator_blocked_retryable_unit_ids": [],
                    "evaluator_blocked_terminal_unit_ids": [],
                    "evaluator_pending_unit_ids": [],
                    "observed_doc_refs": [
                        str((checkpoint_live_root / "README.md").resolve()),
                        str((checkpoint_live_root / "TRACEABILITY.md").resolve()),
                        str((checkpoint_live_root / "analysis" / "STARTUP_BOUNDARY.json").resolve()),
                    ],
                    "recent_log_lines": [
                        f"{checkpoint_scope} child is still refining theorem dependencies before the first execution decision artifact (tick={tick}).",
                        f"{checkpoint_scope} child is actively reading source-backed evidence and scoping the next Lean package boundary (tick={tick}).",
                        f"{checkpoint_scope} child remains inside the bounded xhigh checkpoint phase and has not emitted analysis/EXECUTION_DECISION.json yet (tick={tick}).",
                    ],
                }

            def _checkpoint_recovery_runner(**payload):
                checkpoint_recovery_calls.append(dict(payload))
                return {}

            def _checkpoint_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
                del result_ref, startup_probe_ms, startup_health_timeout_ms
                checkpoint_launch_calls.append("unexpected")
                return {}

            def _checkpoint_terminator(*, result_ref: str | Path):
                checkpoint_terminator_calls.append(str(Path(result_ref).resolve()))
                checkpoint_pid_alive["value"] = False

            checkpoint_not_yet_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=480.0,
                runtime_status_reader=_checkpoint_status_reader,
                recovery_runner=_checkpoint_recovery_runner,
                launcher=_checkpoint_launcher,
                launch_terminator=_checkpoint_terminator,
                progress_snapshot_reader=_checkpoint_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: checkpoint_now["t"],
                sleep_fn=lambda seconds: checkpoint_now.__setitem__("t", checkpoint_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(checkpoint_not_yet_result)
            if str(checkpoint_not_yet_result.get("settled_reason") or "") != "timeout":
                return _fail(f"xhigh {checkpoint_scope} checkpoint grace should not kill the child before the larger checkpoint patience window expires")
            if checkpoint_recovery_calls or checkpoint_launch_calls or checkpoint_terminator_calls:
                return _fail(f"xhigh {checkpoint_scope} checkpoint grace should not trigger recovery, relaunch, or termination in the pre-grace fixture")

            checkpoint_now["t"] = 4000.0
            checkpoint_status_calls["count"] = 0
            checkpoint_recovery_calls.clear()
            checkpoint_launch_calls.clear()
            checkpoint_terminator_calls.clear()
            checkpoint_pid_alive["value"] = True

            checkpoint_stop_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=2040.0,
                runtime_status_reader=_checkpoint_status_reader,
                recovery_runner=_checkpoint_recovery_runner,
                launcher=_checkpoint_launcher,
                launch_terminator=_checkpoint_terminator,
                progress_snapshot_reader=_checkpoint_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: checkpoint_now["t"],
                sleep_fn=lambda seconds: checkpoint_now.__setitem__("t", checkpoint_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(checkpoint_stop_result)
            if str(checkpoint_stop_result.get("settled_reason") or "") != "timeout":
                return _fail(f"xhigh {checkpoint_scope} checkpoint activity must remain followable instead of settling to no_substantive_progress while logs keep evolving")
            if checkpoint_recovery_calls or checkpoint_launch_calls or checkpoint_terminator_calls:
                return _fail(f"xhigh {checkpoint_scope} checkpoint activity must not trigger recovery, relaunch, or termination while the child is still active")

        for checkpoint_scope, workflow_scope, artifact_scope, terminal_scope in (
            ("source", "generic", "task", "local"),
            ("slice", "whole_paper_formalization", "slice", "local"),
        ):
            workspace_root = ROOT / "workspace" / f"test-child-supervision-xhigh-{checkpoint_scope}-checkpoint-plateau"
            state_root = ROOT / ".loop" / f"test-child-supervision-xhigh-{checkpoint_scope}-checkpoint-plateau"
            _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug=f"test-child-supervision-xhigh-{checkpoint_scope}-checkpoint-plateau",
                root_goal=f"bootstrap one implementer node for reasoning-aware {checkpoint_scope} checkpoint plateau validation",
                child_goal_slice=(
                    "treat sustained xhigh checkpoint plateaus as bounded activity instead of killing the child on the flat no-substantive-progress timer"
                ),
                workflow_scope=workflow_scope,
                artifact_scope=artifact_scope,
                terminal_authority_scope=terminal_scope,
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/primary_artifact",
                external_publish_target=str((temp_root / "Desktop" / f"out-xhigh-{checkpoint_scope}-checkpoint-plateau").resolve()),
                context_refs=[],
                startup_required_output_paths=[
                    "README.md",
                    "TRACEABILITY.md",
                    "analysis/STARTUP_BOUNDARY.json",
                ],
                progress_checkpoints=[
                    {
                        "checkpoint_id": f"{checkpoint_scope}_decision",
                        "description": "record the first machine-auditable execution decision after startup",
                        "required_any_of": [
                            "analysis/EXECUTION_DECISION.json",
                        ],
                        "window_s": 5.0,
                    }
                ],
            )

            node_id = str(bootstrap["node_id"])
            launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
            launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
            launch_result_ref_1.write_text(
                json.dumps(
                    {
                        "launch_result_ref": str(launch_result_ref_1.resolve()),
                        "launch_decision": "started",
                        "node_id": node_id,
                        "state_root": str(state_root.resolve()),
                        "workspace_root": str(workspace_root.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            plateau_live_root = Path(str(bootstrap.get("workspace_live_artifact_ref") or "")).resolve()
            plateau_live_root.mkdir(parents=True, exist_ok=True)
            (plateau_live_root / "README.md").write_text("# startup batch\n", encoding="utf-8")
            (plateau_live_root / "TRACEABILITY.md").write_text("| artifact | status |\n| --- | --- |\n", encoding="utf-8")
            (plateau_live_root / "analysis").mkdir(parents=True, exist_ok=True)
            (plateau_live_root / "analysis" / "STARTUP_BOUNDARY.json").write_text(
                json.dumps({"scope": checkpoint_scope}) + "\n",
                encoding="utf-8",
            )

            plateau_status_calls = {"count": 0}
            plateau_recovery_calls: list[dict[str, object]] = []
            plateau_launch_calls: list[str] = []
            plateau_terminator_calls: list[str] = []
            plateau_now = {"t": 4500.0}
            plateau_pid_alive = {"value": True}

            def _plateau_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                launch_result_ref = str(Path(result_ref).resolve())
                plateau_status_calls["count"] += 1
                status_ref = temp_root / f"xhigh-{checkpoint_scope}-checkpoint-plateau-status-{plateau_status_calls['count']}.json"
                return {
                    "launch_result_ref": launch_result_ref,
                    "status_result_ref": str(status_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "pid_alive": bool(plateau_pid_alive["value"]),
                    "recovery_eligible": False,
                    "stall_threshold_s": stall_threshold_s,
                    "lifecycle_status": "ACTIVE" if plateau_pid_alive["value"] else "BLOCKED",
                    "recovery_reason": "live_pid_still_attached",
                    "latest_log_age_s": 0.1,
                }

            plateau_snapshot = {
                "node_id": node_id,
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "pid_alive": True,
                "lifecycle_status": "ACTIVE",
                "phase_hint": "IMPLEMENTING",
                "terminal_result_present": False,
                "implementer_result_ref": "",
                "implementer_outcome": "",
                "implementer_verdict": "",
                "implementer_retryable": False,
                "evaluator_status": "",
                "evaluator_running_unit_ids": [],
                "evaluator_completed_unit_ids": [],
                "evaluator_blocked_retryable_unit_ids": [],
                "evaluator_blocked_terminal_unit_ids": [],
                "evaluator_pending_unit_ids": [],
                "observed_doc_refs": [
                    str((plateau_live_root / "README.md").resolve()),
                    str((plateau_live_root / "TRACEABILITY.md").resolve()),
                    str((plateau_live_root / "analysis" / "STARTUP_BOUNDARY.json").resolve()),
                ],
                "recent_log_lines": [
                    "placeholder split-decision checkpoint remains pending while the child is still thinking.",
                    "todo: record the first execution decision once the plateau resolves.",
                    "planned outputs remain unchanged during this bounded xhigh checkpoint plateau.",
                ],
            }

            def _plateau_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                del result_ref, stall_threshold_s
                return dict(plateau_snapshot)

            def _plateau_recovery_runner(**payload):
                plateau_recovery_calls.append(dict(payload))
                return {}

            def _plateau_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
                del result_ref, startup_probe_ms, startup_health_timeout_ms
                plateau_launch_calls.append("unexpected")
                return {}

            def _plateau_terminator(*, result_ref: str | Path):
                plateau_terminator_calls.append(str(Path(result_ref).resolve()))
                plateau_pid_alive["value"] = False

            plateau_not_yet_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=480.0,
                runtime_status_reader=_plateau_status_reader,
                recovery_runner=_plateau_recovery_runner,
                launcher=_plateau_launcher,
                launch_terminator=_plateau_terminator,
                progress_snapshot_reader=_plateau_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: plateau_now["t"],
                sleep_fn=lambda seconds: plateau_now.__setitem__("t", plateau_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(plateau_not_yet_result)
            if str(plateau_not_yet_result.get("settled_reason") or "") != "timeout":
                return _fail(f"xhigh {checkpoint_scope} checkpoint plateau should not be cut by the old flat no-substantive-progress timer before the larger patience window expires")
            if plateau_recovery_calls or plateau_launch_calls or plateau_terminator_calls:
                return _fail(f"xhigh {checkpoint_scope} checkpoint plateau should not trigger recovery, relaunch, or termination in the pre-grace fixture")

            plateau_now["t"] = 4500.0
            plateau_status_calls["count"] = 0
            plateau_recovery_calls.clear()
            plateau_launch_calls.clear()
            plateau_terminator_calls.clear()
            plateau_pid_alive["value"] = True

            plateau_stop_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=2040.0,
                runtime_status_reader=_plateau_status_reader,
                recovery_runner=_plateau_recovery_runner,
                launcher=_plateau_launcher,
                launch_terminator=_plateau_terminator,
                progress_snapshot_reader=_plateau_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: plateau_now["t"],
                sleep_fn=lambda seconds: plateau_now.__setitem__("t", plateau_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(plateau_stop_result)
            if str(plateau_stop_result.get("settled_reason") or "") != "no_substantive_progress":
                return _fail(f"xhigh {checkpoint_scope} checkpoint plateau must still settle once the larger bounded patience window expires")
            if plateau_recovery_calls or plateau_launch_calls:
                return _fail(f"xhigh {checkpoint_scope} checkpoint plateau must not pretend the child is recovery-eligible")
            if plateau_terminator_calls != [str(launch_result_ref_1.resolve())]:
                return _fail(f"xhigh {checkpoint_scope} checkpoint plateau must terminate the live child before returning")

        for artifact_scope_name, workflow_scope, artifact_scope, terminal_scope in (
            ("source", "generic", "task", "local"),
            ("slice", "whole_paper_formalization", "slice", "local"),
        ):
            workspace_root = ROOT / "workspace" / f"test-child-supervision-xhigh-{artifact_scope_name}-artifact-plateau"
            state_root = ROOT / ".loop" / f"test-child-supervision-xhigh-{artifact_scope_name}-artifact-plateau"
            _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug=f"test-child-supervision-xhigh-{artifact_scope_name}-artifact-plateau",
                root_goal=f"bootstrap one implementer node for reasoning-aware {artifact_scope_name} post-startup artifact plateau validation",
                child_goal_slice=(
                    "allow a bounded xhigh post-startup artifact plateau instead of killing the child on the flat no-substantive-progress timer"
                ),
                workflow_scope=workflow_scope,
                artifact_scope=artifact_scope,
                terminal_authority_scope=terminal_scope,
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/primary_artifact",
                external_publish_target=str((temp_root / "Desktop" / f"out-xhigh-{artifact_scope_name}-artifact-plateau").resolve()),
                context_refs=[],
                startup_required_output_paths=[
                    "README.md",
                    "WHOLE_PAPER_STATUS.json",
                    "analysis/internal_dependency_graph.json",
                ],
            )

            node_id = str(bootstrap["node_id"])
            launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
            launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
            launch_result_ref_1.write_text(
                json.dumps(
                    {
                        "launch_result_ref": str(launch_result_ref_1.resolve()),
                        "launch_decision": "started",
                        "node_id": node_id,
                        "state_root": str(state_root.resolve()),
                        "workspace_root": str(workspace_root.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            artifact_plateau_live_root = Path(str(bootstrap.get("workspace_live_artifact_ref") or "")).resolve()
            artifact_plateau_live_root.mkdir(parents=True, exist_ok=True)
            (artifact_plateau_live_root / "README.md").write_text("# startup batch\n", encoding="utf-8")
            (artifact_plateau_live_root / "WHOLE_PAPER_STATUS.json").write_text('{"status":"STARTUP"}\n', encoding="utf-8")
            (artifact_plateau_live_root / "analysis").mkdir(parents=True, exist_ok=True)
            (artifact_plateau_live_root / "analysis" / "internal_dependency_graph.json").write_text(
                '{"nodes": [], "edges": []}\n',
                encoding="utf-8",
            )

            artifact_plateau_status_calls = {"count": 0}
            artifact_plateau_recovery_calls: list[dict[str, object]] = []
            artifact_plateau_launch_calls: list[str] = []
            artifact_plateau_terminator_calls: list[str] = []
            artifact_plateau_now = {"t": 5200.0}
            artifact_plateau_pid_alive = {"value": True}

            def _artifact_plateau_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                launch_result_ref = str(Path(result_ref).resolve())
                artifact_plateau_status_calls["count"] += 1
                status_ref = temp_root / f"xhigh-{artifact_scope_name}-artifact-plateau-status-{artifact_plateau_status_calls['count']}.json"
                return {
                    "launch_result_ref": launch_result_ref,
                    "status_result_ref": str(status_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "pid_alive": bool(artifact_plateau_pid_alive["value"]),
                    "recovery_eligible": False,
                    "stall_threshold_s": stall_threshold_s,
                    "lifecycle_status": "ACTIVE" if artifact_plateau_pid_alive["value"] else "BLOCKED",
                    "recovery_reason": "live_pid_still_attached",
                    "latest_log_age_s": 0.1,
                }

            artifact_plateau_snapshot = {
                "node_id": node_id,
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "pid_alive": True,
                "lifecycle_status": "ACTIVE",
                "phase_hint": "IMPLEMENTING",
                "terminal_result_present": False,
                "implementer_result_ref": "",
                "implementer_outcome": "",
                "implementer_verdict": "",
                "implementer_retryable": False,
                "evaluator_status": "",
                "evaluator_running_unit_ids": [],
                "evaluator_completed_unit_ids": [],
                "evaluator_blocked_retryable_unit_ids": [],
                "evaluator_blocked_terminal_unit_ids": [],
                "evaluator_pending_unit_ids": [],
                "observed_doc_refs": [
                    str((artifact_plateau_live_root / "README.md").resolve()),
                    str((artifact_plateau_live_root / "WHOLE_PAPER_STATUS.json").resolve()),
                    str((artifact_plateau_live_root / "analysis" / "internal_dependency_graph.json").resolve()),
                ],
            }

            def _artifact_plateau_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                del result_ref, stall_threshold_s
                tick = artifact_plateau_status_calls["count"]
                return {
                    **dict(artifact_plateau_snapshot),
                    "recent_log_lines": [
                        "The startup-required artifact batch is present and stable.",
                        f"The child is still reasoning about the next control decision before touching any new machine-auditable file (tick={tick}).",
                        f"No evaluator has started and no authoritative result exists yet (tick={tick}).",
                    ],
                }

            def _artifact_plateau_recovery_runner(**payload):
                artifact_plateau_recovery_calls.append(dict(payload))
                return {}

            def _artifact_plateau_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
                del result_ref, startup_probe_ms, startup_health_timeout_ms
                artifact_plateau_launch_calls.append("unexpected")
                return {}

            def _artifact_plateau_terminator(*, result_ref: str | Path):
                artifact_plateau_terminator_calls.append(str(Path(result_ref).resolve()))
                artifact_plateau_pid_alive["value"] = False

            artifact_plateau_not_yet_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=480.0,
                runtime_status_reader=_artifact_plateau_status_reader,
                recovery_runner=_artifact_plateau_recovery_runner,
                launcher=_artifact_plateau_launcher,
                launch_terminator=_artifact_plateau_terminator,
                progress_snapshot_reader=_artifact_plateau_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: artifact_plateau_now["t"],
                sleep_fn=lambda seconds: artifact_plateau_now.__setitem__("t", artifact_plateau_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(artifact_plateau_not_yet_result)
            if str(artifact_plateau_not_yet_result.get("settled_reason") or "") != "timeout":
                return _fail(f"xhigh {artifact_scope_name} post-startup artifact plateau should not be cut before the larger bounded patience window expires")
            if artifact_plateau_recovery_calls or artifact_plateau_launch_calls or artifact_plateau_terminator_calls:
                return _fail(f"xhigh {artifact_scope_name} post-startup artifact plateau should not trigger recovery, relaunch, or termination in the pre-grace fixture")

            artifact_plateau_now["t"] = 5200.0
            artifact_plateau_status_calls["count"] = 0
            artifact_plateau_recovery_calls.clear()
            artifact_plateau_launch_calls.clear()
            artifact_plateau_terminator_calls.clear()
            artifact_plateau_pid_alive["value"] = True

            artifact_plateau_stop_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=720.0,
                runtime_status_reader=_artifact_plateau_status_reader,
                recovery_runner=_artifact_plateau_recovery_runner,
                launcher=_artifact_plateau_launcher,
                launch_terminator=_artifact_plateau_terminator,
                progress_snapshot_reader=_artifact_plateau_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: artifact_plateau_now["t"],
                sleep_fn=lambda seconds: artifact_plateau_now.__setitem__("t", artifact_plateau_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(artifact_plateau_stop_result)
            if str(artifact_plateau_stop_result.get("settled_reason") or "") != "timeout":
                return _fail(f"xhigh {artifact_scope_name} post-startup artifact activity must remain followable instead of settling while logs keep evolving")
            if artifact_plateau_recovery_calls or artifact_plateau_launch_calls or artifact_plateau_terminator_calls:
                return _fail(f"xhigh {artifact_scope_name} post-startup artifact activity must not trigger recovery, relaunch, or termination while the child is still active")

            artifact_plateau_now["t"] = 5600.0
            artifact_plateau_status_calls["count"] = 0
            artifact_plateau_recovery_calls.clear()
            artifact_plateau_launch_calls.clear()
            artifact_plateau_terminator_calls.clear()
            artifact_plateau_pid_alive["value"] = True

            static_artifact_plateau_snapshot = dict(artifact_plateau_snapshot)
            static_artifact_plateau_snapshot["recent_log_lines"] = [
                "The startup-required artifact batch is present and stable.",
                "The child is stalled on the same control decision with no new evidence.",
                "No evaluator has started and no authoritative result exists yet.",
            ]

            def _static_artifact_plateau_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                del result_ref, stall_threshold_s
                return dict(static_artifact_plateau_snapshot)

            static_artifact_plateau_stop_result = supervise_child_until_settled(
                launch_result_ref=launch_result_ref_1,
                poll_interval_s=120.0,
                stall_threshold_s=60.0,
                max_recoveries=2,
                max_wall_clock_s=2040.0,
                runtime_status_reader=_artifact_plateau_status_reader,
                recovery_runner=_artifact_plateau_recovery_runner,
                launcher=_artifact_plateau_launcher,
                launch_terminator=_artifact_plateau_terminator,
                progress_snapshot_reader=_static_artifact_plateau_snapshot_reader,
                no_substantive_progress_window_s=120.0,
                now_fn=lambda: artifact_plateau_now["t"],
                sleep_fn=lambda seconds: artifact_plateau_now.__setitem__("t", artifact_plateau_now["t"] + float(seconds)),
            )
            Draft202012Validator(supervision_schema).validate(static_artifact_plateau_stop_result)
            if str(static_artifact_plateau_stop_result.get("settled_reason") or "") != "no_substantive_progress":
                return _fail(f"xhigh {artifact_scope_name} static post-startup artifact plateau must still settle once activity truly stops")
            if artifact_plateau_recovery_calls or artifact_plateau_launch_calls:
                return _fail(f"xhigh {artifact_scope_name} static post-startup artifact plateau must not pretend the child is recovery-eligible")
            if artifact_plateau_terminator_calls != [str(launch_result_ref_1.resolve())]:
                return _fail(f"xhigh {artifact_scope_name} static post-startup artifact plateau must terminate the live child before returning")

        workspace_root = ROOT / "workspace" / "test-child-supervision-live-root-progress"
        state_root = ROOT / ".loop" / "test-child-supervision-live-root-progress"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-live-root-progress",
            root_goal="bootstrap one implementer node for live-root progress validation",
            child_goal_slice="treat non-empty live artifact roots as substantive startup progress even before publication",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_live_artifact_relpath=".tmp_primary_artifact",
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-live-root").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        live_root = workspace_root / ".tmp_primary_artifact"
        live_root.mkdir(parents=True, exist_ok=True)
        (live_root / "README.md").write_text("# extraction batch\n", encoding="utf-8")

        live_root_status_calls = {"count": 0}
        live_root_recovery_calls: list[dict[str, object]] = []
        live_root_launch_calls: list[str] = []
        live_root_terminator_calls: list[str] = []
        live_root_now = {"t": 4000.0}
        live_root_pid_alive = {"value": True}

        def _live_root_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            live_root_status_calls["count"] += 1
            status_ref = temp_root / f"live-root-status-{live_root_status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": bool(live_root_pid_alive["value"]),
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "ACTIVE" if live_root_pid_alive["value"] else "BLOCKED",
                "recovery_reason": "live_pid_still_attached",
                "latest_log_age_s": 0.1,
            }

        live_root_snapshot = {
            "node_id": node_id,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "pid_alive": True,
            "lifecycle_status": "ACTIVE",
            "phase_hint": "IMPLEMENTING",
            "terminal_result_present": False,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "implementer_verdict": "",
            "implementer_retryable": False,
            "evaluator_status": "",
            "evaluator_running_unit_ids": [],
            "evaluator_completed_unit_ids": [],
            "evaluator_blocked_retryable_unit_ids": [],
            "evaluator_blocked_terminal_unit_ids": [],
            "evaluator_pending_unit_ids": [],
            "observed_doc_refs": [
                str((workspace_root / "FROZEN_HANDOFF.md").resolve()),
                str((ROOT / ".cache" / "leanatlas" / "tmp" / "arxiv_2602_11505v2" / "source" / "main.tex").resolve()),
            ],
            "recent_log_lines": [
                "I’m writing the first real source-backed extraction batch into the live artifact root now.",
                "The publish root is intentionally still empty because publication has not happened yet.",
                "The next step is to extend the extraction and decide whether a structured split proposal is warranted.",
            ],
        }

        def _live_root_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            return dict(live_root_snapshot)

        def _live_root_recovery_runner(**payload):
            live_root_recovery_calls.append(dict(payload))
            return {}

        def _live_root_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            live_root_launch_calls.append("unexpected")
            return {}

        def _live_root_terminator(*, result_ref: str | Path):
            live_root_terminator_calls.append(str(Path(result_ref).resolve()))
            live_root_pid_alive["value"] = False

        live_root_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=10.0,
            stall_threshold_s=60.0,
            max_recoveries=2,
            max_wall_clock_s=70.0,
            runtime_status_reader=_live_root_status_reader,
            recovery_runner=_live_root_recovery_runner,
            launcher=_live_root_launcher,
            launch_terminator=_live_root_terminator,
            progress_snapshot_reader=_live_root_snapshot_reader,
            no_substantive_progress_window_s=300.0,
            now_fn=lambda: live_root_now["t"],
            sleep_fn=lambda seconds: live_root_now.__setitem__("t", live_root_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(live_root_result)
        if str(live_root_result.get("settled_reason") or "") != "timeout":
            return _fail("non-empty live artifact roots must satisfy required-progress detection even while the publish root is still empty")
        if live_root_recovery_calls or live_root_launch_calls or live_root_terminator_calls:
            return _fail("live-root startup progress must not trigger recovery, relaunch, or termination before the bounded wall clock expires")

        workspace_root = ROOT / "workspace" / "test-child-supervision-post-startup-stagnation"
        state_root = ROOT / ".loop" / "test-child-supervision-post-startup-stagnation"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-post-startup-stagnation",
            root_goal="bootstrap one implementer node for post-startup stagnation validation",
            child_goal_slice="stop truthfully when startup-required outputs are present but the live artifact tree stops changing before any evaluator-backed progress",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-post-startup-stagnation").resolve()),
            context_refs=[],
            startup_required_output_paths=[
                "README.md",
                "WHOLE_PAPER_STATUS.json",
                "analysis/internal_dependency_graph.json",
            ],
            progress_checkpoints=[
                {
                    "checkpoint_id": "split_decision",
                    "description": "explicitly submit or decline split after startup artifacts are present",
                    "required_any_of": [
                        "split/SPLIT_REQUEST_SUBMISSION_RESULT.json",
                        "split/SPLIT_DECLINE_REASON.md",
                    ],
                    "window_s": 5.0,
                }
            ],
        )

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        stagnation_live_root = Path(str(bootstrap.get("workspace_live_artifact_ref") or "")).resolve()
        stagnation_live_root.mkdir(parents=True, exist_ok=True)
        (stagnation_live_root / "README.md").write_text("# startup batch\n", encoding="utf-8")
        (stagnation_live_root / "WHOLE_PAPER_STATUS.json").write_text('{"status":"STARTUP"}\n', encoding="utf-8")
        (stagnation_live_root / "analysis").mkdir(parents=True, exist_ok=True)
        (stagnation_live_root / "analysis" / "internal_dependency_graph.json").write_text(
            '{"nodes": [], "edges": []}\n',
            encoding="utf-8",
        )

        stagnation_status_calls = {"count": 0}
        stagnation_recovery_calls: list[dict[str, object]] = []
        stagnation_launch_calls: list[str] = []
        stagnation_terminator_calls: list[str] = []
        stagnation_now = {"t": 5000.0}
        stagnation_pid_alive = {"value": True}

        def _stagnation_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            launch_result_ref = str(Path(result_ref).resolve())
            stagnation_status_calls["count"] += 1
            status_ref = temp_root / f"post-startup-stagnation-status-{stagnation_status_calls['count']}.json"
            return {
                "launch_result_ref": launch_result_ref,
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": bool(stagnation_pid_alive["value"]),
                "recovery_eligible": False,
                "stall_threshold_s": stall_threshold_s,
                "lifecycle_status": "ACTIVE" if stagnation_pid_alive["value"] else "BLOCKED",
                "recovery_reason": "live_pid_still_attached",
                "latest_log_age_s": 0.1,
            }

        stagnation_snapshot = {
            "node_id": node_id,
            "workspace_root": str(workspace_root.resolve()),
            "state_root": str(state_root.resolve()),
            "pid_alive": True,
            "lifecycle_status": "ACTIVE",
            "phase_hint": "IMPLEMENTING",
            "terminal_result_present": False,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "implementer_verdict": "",
            "implementer_retryable": False,
            "evaluator_status": "",
            "evaluator_running_unit_ids": [],
            "evaluator_completed_unit_ids": [],
            "evaluator_blocked_retryable_unit_ids": [],
            "evaluator_blocked_terminal_unit_ids": [],
            "evaluator_pending_unit_ids": [],
            "observed_doc_refs": [
                str((stagnation_live_root / "README.md").resolve()),
                str((stagnation_live_root / "WHOLE_PAPER_STATUS.json").resolve()),
                str((stagnation_live_root / "analysis" / "internal_dependency_graph.json").resolve()),
            ],
            "recent_log_lines": [
                "The startup-required extraction batch is already present in the live artifact root.",
                "I am thinking through the next split boundary before touching any additional artifact files.",
                "No evaluator has started and no authoritative result exists yet.",
            ],
        }

        def _stagnation_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            changing_counter = stagnation_status_calls["count"]
            churn_path = stagnation_live_root / "analysis" / f"checkpoint_churn_{changing_counter}.json"
            churn_path.parent.mkdir(parents=True, exist_ok=True)
            churn_path.write_text(json.dumps({"tick": changing_counter}) + "\n", encoding="utf-8")
            return dict(stagnation_snapshot)

        def _stagnation_recovery_runner(**payload):
            stagnation_recovery_calls.append(dict(payload))
            return {}

        def _stagnation_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            stagnation_launch_calls.append("unexpected")
            return {}

        def _stagnation_terminator(*, result_ref: str | Path):
            stagnation_terminator_calls.append(str(Path(result_ref).resolve()))
            stagnation_pid_alive["value"] = False

        stagnation_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=10.0,
            stall_threshold_s=60.0,
            max_recoveries=2,
            max_wall_clock_s=30.0,
            runtime_status_reader=_stagnation_status_reader,
            recovery_runner=_stagnation_recovery_runner,
            launcher=_stagnation_launcher,
            launch_terminator=_stagnation_terminator,
            progress_snapshot_reader=_stagnation_snapshot_reader,
            no_substantive_progress_window_s=5.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: stagnation_now["t"],
            sleep_fn=lambda seconds: stagnation_now.__setitem__("t", stagnation_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(stagnation_result)
        if str(stagnation_result.get("settled_reason") or "") != "no_substantive_progress":
            return _fail(
                "supervision helper must stop a live child once the earliest incomplete progress checkpoint expires, even if unrelated live artifact churn continues"
            )
        if stagnation_recovery_calls or stagnation_launch_calls:
            return _fail("post-startup stagnation must not pretend the child is recovery-eligible or relaunch it automatically")
        if stagnation_terminator_calls != [str(launch_result_ref_1.resolve())]:
            return _fail("post-startup stagnation must terminate the live child before returning")

        workspace_root = ROOT / "workspace" / "test-child-supervision-runtime-status-observation-resilience"
        state_root = ROOT / ".loop" / "test-child-supervision-runtime-status-observation-resilience"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-runtime-status-observation-resilience",
            root_goal="bootstrap one implementer node for runtime-status observation resilience",
            child_goal_slice="survive transient runtime-status reader failures without crashing the supervision loop",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-runtime-status-observation-resilience").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        status_observation_report_ref = temp_root / "runtime-status-observation-resilience" / "EvaluationReport.json"
        status_observation_report_ref.parent.mkdir(parents=True, exist_ok=True)
        status_observation_report_ref.write_text(
            json.dumps({"status": "COMPLETED", "verdict": "PASS"}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        status_observation_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        status_observation_result_ref.parent.mkdir(parents=True, exist_ok=True)
        status_observation_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "PASS",
                    "summary": "runtime-status observation resilience fixture",
                    "evaluation_report_ref": str(status_observation_report_ref.resolve()),
                    "evaluator_result": {
                        "verdict": "PASS",
                        "retryable": False,
                        "summary": "simple evaluator reviewer verdict=PASS",
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        status_observation_calls = {"count": 0}
        status_observation_recovery_calls: list[dict[str, object]] = []
        status_observation_launch_calls: list[str] = []
        status_observation_now = {"t": 6000.0}

        def _status_observation_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            status_observation_calls["count"] += 1
            if status_observation_calls["count"] == 1:
                raise RuntimeError("transient runtime-status observation failure")
            status_ref = temp_root / f"runtime-status-observation-{status_observation_calls['count']}.json"
            return {
                "launch_result_ref": str(launch_result_ref_1.resolve()),
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": False,
                "stall_threshold_s": 60.0,
                "lifecycle_status": "COMPLETED",
                "recovery_reason": "node_status_completed",
                "latest_log_age_s": 0.1,
            }

        def _status_observation_recovery_runner(**payload):
            status_observation_recovery_calls.append(dict(payload))
            return {}

        def _status_observation_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            status_observation_launch_calls.append("unexpected")
            return {}

        status_observation_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=1,
            max_wall_clock_s=0.0,
            runtime_status_reader=_status_observation_reader,
            recovery_runner=_status_observation_recovery_runner,
            launcher=_status_observation_launcher,
            no_substantive_progress_window_s=5.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: status_observation_now["t"],
            sleep_fn=lambda seconds: status_observation_now.__setitem__("t", status_observation_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(status_observation_result)
        if str(status_observation_result.get("settled_reason") or "") != "pass":
            return _fail("supervision helper must survive transient runtime-status reader failures and continue to a later settled pass")
        if status_observation_recovery_calls or status_observation_launch_calls:
            return _fail("transient runtime-status observation failures must not trigger fake recovery or relaunch")

        superseded_workspace_root = ROOT / "workspace" / "test-child-supervision-superseded-attempt"
        superseded_state_root = ROOT / ".loop" / "test-child-supervision-superseded-attempt"
        shutil.rmtree(superseded_workspace_root, ignore_errors=True)
        shutil.rmtree(superseded_state_root, ignore_errors=True)
        superseded_bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-superseded-attempt",
            root_goal="bootstrap one implementer node for superseded-attempt supervision coverage",
            child_goal_slice="older attempts must settle as superseded when a newer committed attempt already exists",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(superseded_workspace_root),
            state_root=str(superseded_state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-superseded-attempt").resolve()),
            context_refs=[],
        )
        superseded_node_id = str(superseded_bootstrap["node_id"])
        superseded_attempt_1_ref = superseded_state_root / "artifacts" / "launches" / superseded_node_id / "attempt_001" / "ChildLaunchResult.json"
        superseded_attempt_1_ref.parent.mkdir(parents=True, exist_ok=True)
        superseded_attempt_1_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(superseded_attempt_1_ref.resolve()),
                    "launch_decision": "started_existing",
                    "node_id": superseded_node_id,
                    "state_root": str(superseded_state_root.resolve()),
                    "workspace_root": str(superseded_workspace_root.resolve()),
                    "pid": 11111,
                    "exit_code": None,
                    "reuse_launch_result_ref": str(
                        (superseded_state_root / "artifacts" / "launches" / superseded_node_id / "attempt_000" / "ChildLaunchResult.json").resolve()
                    ),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        superseded_attempt_2_ref = superseded_state_root / "artifacts" / "launches" / superseded_node_id / "attempt_002" / "ChildLaunchResult.json"
        superseded_attempt_2_ref.parent.mkdir(parents=True, exist_ok=True)
        superseded_attempt_2_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(superseded_attempt_2_ref.resolve()),
                    "launch_decision": "started_existing",
                    "node_id": superseded_node_id,
                    "state_root": str(superseded_state_root.resolve()),
                    "workspace_root": str(superseded_workspace_root.resolve()),
                    "pid": 11111,
                    "exit_code": None,
                    "reuse_launch_result_ref": str(
                        (superseded_state_root / "artifacts" / "launches" / superseded_node_id / "attempt_000" / "ChildLaunchResult.json").resolve()
                    ),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        superseded_attempt_2_ref.with_name("ChildSupervisionRuntime.json").write_text(
            json.dumps(
                {
                    "launch_result_ref": str(superseded_attempt_2_ref.resolve()),
                    "pid": 22222,
                    "status": "running",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        superseded_recovery_calls: list[dict[str, object]] = []
        superseded_launch_calls: list[str] = []

        def _superseded_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del stall_threshold_s
            return {
                "launch_result_ref": str(Path(result_ref).resolve()),
                "status_result_ref": str((temp_root / "superseded-attempt-status.json").resolve()),
                "node_id": superseded_node_id,
                "state_root": str(superseded_state_root.resolve()),
                "workspace_root": str(superseded_workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": True,
                "stall_threshold_s": 60.0,
                "lifecycle_status": "ACTIVE",
                "runtime_attachment_state": "LOST",
                "recovery_reason": "active_without_live_pid",
                "latest_log_age_s": 120.0,
            }

        def _superseded_recovery_runner(**payload):
            superseded_recovery_calls.append(dict(payload))
            return {}

        def _superseded_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            superseded_launch_calls.append("unexpected")
            return {}

        superseded_result = supervise_child_until_settled(
            launch_result_ref=superseded_attempt_1_ref,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=5,
            max_wall_clock_s=0.0,
            runtime_status_reader=_superseded_status_reader,
            recovery_runner=_superseded_recovery_runner,
            launcher=_superseded_launcher,
            no_substantive_progress_window_s=0.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: 8000.0,
            sleep_fn=lambda _seconds: None,
        )
        Draft202012Validator(supervision_schema).validate(superseded_result)
        if str(superseded_result.get("settled_reason") or "") != "superseded_attempt":
            return _fail("older attempt supervision must settle as superseded_attempt once a newer numeric attempt already exists")
        if superseded_recovery_calls or superseded_launch_calls:
            return _fail("superseded attempt supervision must not emit recovery or relaunch traffic")

        workspace_root = ROOT / "workspace" / "test-child-supervision-progress-snapshot-observation-resilience"
        state_root = ROOT / ".loop" / "test-child-supervision-progress-snapshot-observation-resilience"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-progress-snapshot-observation-resilience",
            root_goal="bootstrap one implementer node for progress-snapshot observation resilience",
            child_goal_slice="survive transient progress snapshot failures without crashing the supervision loop",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-progress-snapshot-observation-resilience").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        launch_result_ref_1 = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref_1.parent.mkdir(parents=True, exist_ok=True)
        launch_result_ref_1.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref_1.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        progress_snapshot_report_ref = temp_root / "progress-snapshot-observation-resilience" / "EvaluationReport.json"
        progress_snapshot_report_ref.parent.mkdir(parents=True, exist_ok=True)
        progress_snapshot_report_ref.write_text(
            json.dumps({"status": "COMPLETED", "verdict": "PASS"}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        progress_snapshot_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        progress_snapshot_result_ref.parent.mkdir(parents=True, exist_ok=True)

        progress_snapshot_status_calls = {"count": 0}
        progress_snapshot_recovery_calls: list[dict[str, object]] = []
        progress_snapshot_launch_calls: list[str] = []
        progress_snapshot_now = {"t": 7000.0}
        progress_snapshot_calls = {"count": 0}

        def _progress_snapshot_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            progress_snapshot_status_calls["count"] += 1
            status_ref = temp_root / f"progress-snapshot-observation-{progress_snapshot_status_calls['count']}.json"
            return {
                "launch_result_ref": str(launch_result_ref_1.resolve()),
                "status_result_ref": str(status_ref.resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": True,
                "recovery_eligible": False,
                "stall_threshold_s": 60.0,
                "lifecycle_status": "ACTIVE",
                "recovery_reason": "live_pid_still_attached",
                "latest_log_age_s": 0.1,
            }

        def _progress_snapshot_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            progress_snapshot_calls["count"] += 1
            if progress_snapshot_calls["count"] == 1:
                progress_snapshot_result_ref.write_text(
                    json.dumps(
                        {
                            "schema": "loop_product.implementer_result",
                            "schema_version": "0.1.0",
                            "node_id": node_id,
                            "status": "COMPLETED",
                            "outcome": "PASS",
                            "summary": "progress-snapshot observation resilience fixture",
                            "evaluation_report_ref": str(progress_snapshot_report_ref.resolve()),
                            "evaluator_result": {
                                "verdict": "PASS",
                                "retryable": False,
                                "summary": "simple evaluator reviewer verdict=PASS",
                            },
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                raise RuntimeError("transient progress snapshot failure")
            return {
                "node_id": node_id,
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "pid_alive": True,
                "lifecycle_status": "ACTIVE",
                "phase_hint": "IMPLEMENTING",
                "terminal_result_present": False,
                "implementer_result_ref": "",
                "implementer_outcome": "",
                "implementer_verdict": "",
                "implementer_retryable": False,
                "evaluator_status": "",
                "evaluator_running_unit_ids": [],
                "evaluator_completed_unit_ids": [],
                "evaluator_blocked_retryable_unit_ids": [],
                "evaluator_blocked_terminal_unit_ids": [],
                "evaluator_pending_unit_ids": [],
                "observed_doc_refs": [],
                "recent_log_lines": [],
            }

        def _progress_snapshot_recovery_runner(**payload):
            progress_snapshot_recovery_calls.append(dict(payload))
            return {}

        def _progress_snapshot_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            progress_snapshot_launch_calls.append("unexpected")
            return {}

        progress_snapshot_result = supervise_child_until_settled(
            launch_result_ref=launch_result_ref_1,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=1,
            max_wall_clock_s=0.0,
            runtime_status_reader=_progress_snapshot_status_reader,
            recovery_runner=_progress_snapshot_recovery_runner,
            launcher=_progress_snapshot_launcher,
            progress_snapshot_reader=_progress_snapshot_reader,
            no_substantive_progress_window_s=5.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: progress_snapshot_now["t"],
            sleep_fn=lambda seconds: progress_snapshot_now.__setitem__("t", progress_snapshot_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(progress_snapshot_result)
        if str(progress_snapshot_result.get("settled_reason") or "") != "pass":
            return _fail("supervision helper must survive transient progress snapshot failures and continue to a later settled pass")
        if progress_snapshot_recovery_calls or progress_snapshot_launch_calls:
            return _fail("transient progress snapshot failures must not trigger fake recovery or relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-authoritative-pass-fast-path"
        state_root = ROOT / ".loop" / "test-child-supervision-authoritative-pass-fast-path"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-authoritative-pass-fast-path",
            root_goal="bootstrap one implementer node for authoritative-pass fast-path coverage",
            child_goal_slice="settle supervision directly from an authoritative PASS result even if runtime-status observation is unavailable",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/primary_artifact",
            external_publish_target=str((temp_root / "Desktop" / "out-authoritative-pass-fast-path").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        authoritative_pass_launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        authoritative_pass_launch_ref.parent.mkdir(parents=True, exist_ok=True)
        authoritative_pass_launch_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(authoritative_pass_launch_ref.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        authoritative_pass_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        authoritative_pass_result_ref.parent.mkdir(parents=True, exist_ok=True)
        authoritative_pass_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "COMPLETED",
                    "summary": "authoritative pass fast-path fixture",
                    "evaluator_result": {
                        "verdict": "PASS",
                        "retryable": False,
                        "summary": "simple evaluator reviewer verdict=PASS",
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        authoritative_pass_status_calls = {"count": 0}

        def _authoritative_pass_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del result_ref, stall_threshold_s
            authoritative_pass_status_calls["count"] += 1
            raise AssertionError("authoritative PASS fast-path must settle before runtime-status observation")

        authoritative_pass_result = supervise_child_until_settled(
            launch_result_ref=authoritative_pass_launch_ref,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=1,
            max_wall_clock_s=0.0,
            runtime_status_reader=_authoritative_pass_status_reader,
            recovery_runner=lambda **_payload: (_ for _ in ()).throw(AssertionError("recovery must not run for authoritative PASS")),
            launcher=lambda **_payload: (_ for _ in ()).throw(AssertionError("relaunch must not run for authoritative PASS")),
            no_substantive_progress_window_s=0.0,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: 8100.0,
            sleep_fn=lambda _seconds: None,
        )
        Draft202012Validator(supervision_schema).validate(authoritative_pass_result)
        if str(authoritative_pass_result.get("settled_reason") or "") != "pass":
            return _fail("supervision helper must settle immediately from an authoritative PASS result even if runtime-status observation is unavailable")
        if authoritative_pass_status_calls["count"] != 0:
            return _fail("authoritative PASS fast-path must not invoke runtime-status observation")

        workspace_root = ROOT / "workspace" / "test-child-supervision-blocked-after-recovery-exception"
        state_root = ROOT / ".loop" / "test-child-supervision-blocked-after-recovery-exception"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-blocked-after-recovery-exception",
            root_goal="bootstrap one implementer node for non-active truthful-stop validation",
            child_goal_slice="stop supervising once the node leaves ACTIVE after a failed recovery attempt",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out-blocked-after-recovery-exception.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        blocked_after_recovery_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        blocked_after_recovery_result_ref.parent.mkdir(parents=True, exist_ok=True)
        blocked_after_recovery_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "REPAIR_REQUIRED",
                    "summary": "retryable evaluator fail already recorded before node left ACTIVE",
                    "evaluator_result": {
                        "verdict": "FAIL",
                        "retryable": True,
                        "summary": "simple evaluator reviewer verdict=FAIL",
                        "diagnostics": {
                            "self_attribution": "implementation_gap",
                            "self_repair": "repair locally and rerun evaluator",
                        },
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        blocked_after_recovery_launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        blocked_after_recovery_launch_ref.parent.mkdir(parents=True, exist_ok=True)
        blocked_after_recovery_launch_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(blocked_after_recovery_launch_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        blocked_after_recovery_calls: list[dict[str, object]] = []
        blocked_after_recovery_launch_calls: list[str] = []
        blocked_after_recovery_status = {"count": 0}
        blocked_after_recovery_now = {"t": 9100.0}

        def _blocked_after_recovery_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del stall_threshold_s
            blocked_after_recovery_status["count"] += 1
            if blocked_after_recovery_status["count"] == 1:
                lifecycle_status = "ACTIVE"
                attachment_state = "LOST"
                recovery_eligible = True
                recovery_reason = "active_without_live_pid"
            else:
                lifecycle_status = "BLOCKED"
                attachment_state = "TERMINAL"
                recovery_eligible = False
                recovery_reason = "node_status_blocked"
            return {
                "launch_result_ref": str(Path(result_ref).resolve()),
                "status_result_ref": str((temp_root / f"blocked-after-recovery-{blocked_after_recovery_status['count']}.json").resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": recovery_eligible,
                "stall_threshold_s": 60.0,
                "lifecycle_status": lifecycle_status,
                "runtime_attachment_state": attachment_state,
                "recovery_reason": recovery_reason,
            }

        def _blocked_after_recovery_runner(**payload):
            blocked_after_recovery_calls.append(dict(payload))
            raise ValueError("orphaned-active recovery requires an ACTIVE node, got 'BLOCKED'")

        def _blocked_after_recovery_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            blocked_after_recovery_launch_calls.append("unexpected")
            raise AssertionError("non-ACTIVE blocked node must not relaunch after recovery was rejected")

        blocked_after_recovery_result = supervise_child_until_settled(
            launch_result_ref=blocked_after_recovery_launch_ref,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=3,
            max_wall_clock_s=5.0,
            runtime_status_reader=_blocked_after_recovery_status_reader,
            recovery_runner=_blocked_after_recovery_runner,
            launcher=_blocked_after_recovery_launcher,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: blocked_after_recovery_now["t"],
            sleep_fn=lambda seconds: blocked_after_recovery_now.__setitem__(
                "t",
                blocked_after_recovery_now["t"] + float(seconds) + 1.0,
            ),
        )
        Draft202012Validator(supervision_schema).validate(blocked_after_recovery_result)
        if str(blocked_after_recovery_result.get("settled_reason") or "") != "authoritative_result":
            return _fail("supervision helper must settle to authoritative_result once a retryable FAIL node has already left ACTIVE")
        if len(blocked_after_recovery_calls) != 1:
            return _fail("supervision helper must stop after the first rejected recovery once the node becomes non-ACTIVE")
        if blocked_after_recovery_launch_calls:
            return _fail("blocked non-ACTIVE node must not relaunch")

        workspace_root = ROOT / "workspace" / "test-child-supervision-nonactive-without-result"
        state_root = ROOT / ".loop" / "test-child-supervision-nonactive-without-result"
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="test-child-supervision-nonactive-without-result",
            root_goal="bootstrap one implementer node for non-active no-result truthful-stop validation",
            child_goal_slice="settle once the node is blocked and no longer recoverable even without an authoritative result",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root),
            state_root=str(state_root),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out-nonactive-without-result.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        nonactive_without_result_launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        nonactive_without_result_launch_ref.parent.mkdir(parents=True, exist_ok=True)
        nonactive_without_result_launch_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(nonactive_without_result_launch_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        nonactive_without_result_recovery_calls: list[dict[str, object]] = []
        nonactive_without_result_launch_calls: list[str] = []
        nonactive_without_result_now = {"t": 9200.0}

        def _nonactive_without_result_status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
            del stall_threshold_s
            return {
                "launch_result_ref": str(Path(result_ref).resolve()),
                "status_result_ref": str((temp_root / "nonactive-without-result-status.json").resolve()),
                "node_id": node_id,
                "state_root": str(state_root.resolve()),
                "workspace_root": str(workspace_root.resolve()),
                "pid_alive": False,
                "recovery_eligible": False,
                "stall_threshold_s": 60.0,
                "lifecycle_status": "BLOCKED",
                "runtime_attachment_state": "TERMINAL",
                "recovery_reason": "node_status_blocked",
            }

        def _nonactive_without_result_recovery_runner(**payload):
            nonactive_without_result_recovery_calls.append(dict(payload))
            raise AssertionError("non-ACTIVE blocked node without a result must not attempt recovery")

        def _nonactive_without_result_launcher(*, result_ref: str | Path, startup_probe_ms: int = 1500, startup_health_timeout_ms: int = 12000):
            del result_ref, startup_probe_ms, startup_health_timeout_ms
            nonactive_without_result_launch_calls.append("unexpected")
            raise AssertionError("non-ACTIVE blocked node without a result must not relaunch")

        nonactive_without_result = supervise_child_until_settled(
            launch_result_ref=nonactive_without_result_launch_ref,
            poll_interval_s=0.0,
            stall_threshold_s=60.0,
            max_recoveries=3,
            max_wall_clock_s=2.0,
            runtime_status_reader=_nonactive_without_result_status_reader,
            recovery_runner=_nonactive_without_result_recovery_runner,
            launcher=_nonactive_without_result_launcher,
            apply_reasoning_budget_floor=False,
            now_fn=lambda: nonactive_without_result_now["t"],
            sleep_fn=lambda seconds: nonactive_without_result_now.__setitem__(
                "t",
                nonactive_without_result_now["t"] + float(seconds) + 1.0,
            ),
        )
        Draft202012Validator(supervision_schema).validate(nonactive_without_result)
        if str(nonactive_without_result.get("settled_reason") or "") != "retry_budget_exhausted":
            return _fail("supervision helper must truthfully stop once a non-ACTIVE node is no longer recoverable even without an authoritative result")
        if nonactive_without_result_recovery_calls or nonactive_without_result_launch_calls:
            return _fail("non-ACTIVE node without an authoritative result must not emit recovery or relaunch traffic")

        cli_launch_result_ref = state_root / "artifacts" / "launches" / node_id / "attempt_cli" / "ChildLaunchResult.json"
        cli_launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        cli_launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(cli_launch_result_ref.resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        cli_result_payload = {
            "launch_result_ref": str(cli_launch_result_ref.resolve()),
            "latest_launch_result_ref": str(cli_launch_result_ref.resolve()),
            "node_id": node_id,
            "state_root": str(state_root.resolve()),
            "workspace_root": str(workspace_root.resolve()),
            "settled": True,
            "settled_reason": "no_substantive_progress",
            "recoveries_used": 0,
            "implementer_result_ref": "",
            "implementer_outcome": "",
            "evaluator_verdict": "",
            "status_result_ref": str((temp_root / "cli-supervision-status.json").resolve()),
            "history": [],
        }
        cli_sync_calls: list[str] = []
        cli_stdout = io.StringIO()
        original_supervise = lifecycle_module.supervise_child_until_settled
        original_sync = lifecycle_module.synchronize_authoritative_state_from_launch_result_ref
        try:
            lifecycle_module.supervise_child_until_settled = lambda **_: dict(cli_result_payload)
            lifecycle_module.synchronize_authoritative_state_from_launch_result_ref = (
                lambda *, result_ref: cli_sync_calls.append(str(Path(result_ref).resolve())) or None
            )
            with contextlib.redirect_stdout(cli_stdout):
                cli_exit = lifecycle_module._cmd_supervise_child(
                    argparse.Namespace(
                        launch_result_ref=str(cli_launch_result_ref.resolve()),
                        poll_interval_s=2.0,
                        stall_threshold_s=60.0,
                        max_recoveries=1,
                        max_wall_clock_s=0.0,
                        no_substantive_progress_window_s=300.0,
                    )
                )
        finally:
            lifecycle_module.supervise_child_until_settled = original_supervise
            lifecycle_module.synchronize_authoritative_state_from_launch_result_ref = original_sync
        if cli_exit != 0:
            return _fail("runtime lifecycle supervise-child command must exit cleanly after writing a settled committed supervision result")
        cli_supervision_result_ref = cli_launch_result_ref.with_name("ChildSupervisionResult.json")
        if not cli_supervision_result_ref.exists():
            return _fail("runtime lifecycle supervise-child command must persist ChildSupervisionResult.json next to the launch result")
        persisted_cli_result = json.loads(cli_supervision_result_ref.read_text(encoding="utf-8"))
        Draft202012Validator(supervision_schema).validate(persisted_cli_result)
        if str(persisted_cli_result.get("settled_reason") or "") != "no_substantive_progress":
            return _fail("runtime lifecycle supervise-child command must persist the settled supervision payload verbatim")
        if cli_sync_calls != [str(cli_launch_result_ref.resolve())]:
            return _fail("runtime lifecycle supervise-child command must synchronize authoritative state from the committed launch result after persisting supervision output")
        printed_cli_result = json.loads(cli_stdout.getvalue())
        if printed_cli_result != cli_result_payload:
            return _fail("runtime lifecycle supervise-child command must still print the settled supervision payload to stdout after persisting it")

    print("[loop-system-child-supervision-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
