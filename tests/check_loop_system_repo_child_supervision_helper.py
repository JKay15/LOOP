#!/usr/bin/env python3
"""Validate the committed root-side child supervision helper."""

from __future__ import annotations

import json
import shutil
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


def main() -> int:
    from loop_product.runtime import bootstrap_first_implementer_node
    from loop_product.dispatch.supervision import supervise_child_until_settled

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
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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

        workspace_root = ROOT / "workspace" / "test-child-supervision-authoritative-blocked"
        state_root = ROOT / ".loop" / "test-child-supervision-authoritative-blocked"
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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

        workspace_root = ROOT / "workspace" / "test-child-supervision-provider-capacity"
        state_root = ROOT / ".loop" / "test-child-supervision-provider-capacity"
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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
            now_fn=lambda: empty_workspace_now["t"],
            sleep_fn=lambda seconds: empty_workspace_now.__setitem__("t", empty_workspace_now["t"] + float(seconds)),
        )
        Draft202012Validator(supervision_schema).validate(empty_workspace_result)
        if str(empty_workspace_result.get("settled_reason") or "") != "no_substantive_progress":
            return _fail("supervision helper must stop a live child that keeps the workspace mirror empty while repeatedly claiming it is writing the first artifact batch")
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
        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)
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

    print("[loop-system-child-supervision-helper] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
