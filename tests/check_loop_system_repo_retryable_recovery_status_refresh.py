#!/usr/bin/env python3
"""Validate stale terminal runtime status refresh for retryable ACTIVE repair lanes."""

from __future__ import annotations

import atexit
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
    print(f"[loop-system-retryable-recovery-status-refresh][FAIL] {msg}", file=sys.stderr)
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

    try:
        cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.runtime import bootstrap_first_implementer_node, cleanup_test_repo_services
    from loop_product.runtime.lifecycle import recover_orphaned_active_node
    from loop_product import child_supervision_sidecar as sidecar_module
    from loop_product.runtime_paths import node_machine_handoff_ref

    atexit.register(_cleanup_repo_services_for_root)
    result_schema = _load_schema("LoopOrphanedActiveRecoveryResult.schema.json")

    with tempfile.TemporaryDirectory(prefix="loop_system_retryable_recovery_status_refresh_") as td:
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

        workspace_root = ROOT / "workspace" / "test-retryable-recovery-status-refresh"
        state_root = ROOT / ".loop" / "test-retryable-recovery-status-refresh"
        initial_cleanup = cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
        if not bool(initial_cleanup.get("quiesced")):
            return _fail("repo service cleanup must quiesce before retryable recovery status refresh validation")
        _cleanup_runtime_tree(state_root=state_root, workspace_root=workspace_root)
        bootstrap = bootstrap_first_implementer_node(
            mode="fresh",
            task_slug="retryable-refresh",
            root_goal="bootstrap one implementer node for stale runtime status refresh validation",
            child_goal_slice="accept same-node retry after refreshing a stale terminal status for a retryable fail lane",
            endpoint_artifact_ref=str(endpoint.resolve()),
            workspace_root=str(workspace_root.resolve()),
            state_root=str(state_root.resolve()),
            workspace_mirror_relpath="deliverables/out.txt",
            external_publish_target=str((temp_root / "Desktop" / "out.txt").resolve()),
            context_refs=[],
        )

        node_id = str(bootstrap["node_id"])
        implementer_result_ref = state_root / "artifacts" / node_id / "implementer_result.json"
        implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
        evaluation_report_ref = temp_root / "EvaluationReport.json"
        evaluation_report_ref.write_text("{}\n", encoding="utf-8")
        implementer_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": node_id,
                    "status": "COMPLETED",
                    "outcome": "REPAIR_REQUIRED",
                    "summary": "simple evaluator reviewer verdict=FAIL",
                    "evaluator_result": {
                        "verdict": "FAIL",
                        "retryable": True,
                        "summary": "simple evaluator reviewer verdict=FAIL",
                        "diagnostics": {
                            "self_attribution": "implementation_gap",
                            "self_repair": "repair the unmet in-scope requirements and rerun evaluator",
                        },
                    },
                    "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        launch_result_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
        stdout_ref = launch_result_ref.with_name(f"{node_id}.stdout.txt")
        stderr_ref = launch_result_ref.with_name(f"{node_id}.stderr.txt")
        stdin_ref = workspace_root / "CHILD_PROMPT.md"
        launch_request_ref = launch_result_ref.with_name("ChildLaunchRequest.json")
        launch_log_dir = launch_result_ref.parent / "logs"
        launch_log_dir.mkdir(parents=True, exist_ok=True)
        stdout_ref.write_text("stale retryable lane output\n", encoding="utf-8")
        stderr_ref.write_text("", encoding="utf-8")
        launch_request_ref.write_text("{}\n", encoding="utf-8")
        launch_result_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "source_result_ref": str(Path(str(bootstrap["bootstrap_result_ref"])).resolve()),
                    "launch_decision": "started",
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "startup_health_timeout_ms": 250,
                    "startup_retry_limit": 0,
                    "attempt_count": 1,
                    "retryable_failure_kind": "",
                    "attempts": [
                        {
                            "attempt_index": 1,
                            "launch_decision": "started",
                            "launch_request_ref": str(launch_request_ref.resolve()),
                            "launch_log_dir": str(launch_log_dir.resolve()),
                            "stdout_ref": str(stdout_ref.resolve()),
                            "stderr_ref": str(stderr_ref.resolve()),
                            "stdin_ref": str(stdin_ref.resolve()),
                            "wrapped_argv": ["python3", "-c", "pass"],
                            "pid": 999999,
                            "exit_code": 1,
                            "retryable_failure_kind": "",
                        }
                    ],
                    "launch_request_ref": str(launch_request_ref.resolve()),
                    "launch_log_dir": str(launch_log_dir.resolve()),
                    "pid": 999999,
                    "exit_code": 1,
                    "stdout_ref": str(stdout_ref.resolve()),
                    "stderr_ref": str(stderr_ref.resolve()),
                    "stdin_ref": str(stdin_ref.resolve()),
                    "wrapped_argv": ["python3", "-c", "pass"],
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        stale_status_ref = launch_result_ref.with_name("stale-terminal-status.json")
        stale_status_ref.write_text(
            json.dumps(
                {
                    "launch_result_ref": str(launch_result_ref.resolve()),
                    "status_result_ref": str(stale_status_ref.resolve()),
                    "node_id": node_id,
                    "state_root": str(state_root.resolve()),
                    "workspace_root": str(workspace_root.resolve()),
                    "pid": 999999,
                    "pid_alive": False,
                    "exit_code": 1,
                    "lifecycle_status": "ACTIVE",
                    "runtime_attachment_state": "TERMINAL",
                    "runtime_observation_kind": "terminal_result",
                    "runtime_summary": "stale terminal view from an older runtime-status helper",
                    "runtime_evidence_refs": [str(implementer_result_ref.resolve())],
                    "latest_log_ref": str(stdout_ref.resolve()),
                    "latest_log_mtime": "",
                    "latest_log_age_s": 999.0,
                    "stdout_ref": str(stdout_ref.resolve()),
                    "stderr_ref": str(stderr_ref.resolve()),
                    "stalled_hint": False,
                    "recovery_eligible": False,
                    "recovery_reason": "authoritative_terminal_result",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        supervision_runtime_ref = launch_result_ref.with_name("ChildSupervisionRuntime.json")
        original_ensure_supervision_running = sidecar_module.ensure_child_supervision_running
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        try:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"

            def _fake_ensure_supervision_running(*, launch_result_ref: str | Path, **_kwargs: object) -> dict[str, object]:
                launch_path = Path(launch_result_ref).resolve()
                supervision_runtime_ref.write_text(
                    json.dumps(
                        {
                            "schema": "loop_product.child_supervision_runtime",
                            "launch_result_ref": str(launch_path),
                            "pid": 1,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                return {"launch_result_ref": str(launch_path), "pid": 1}

            sidecar_module.ensure_child_supervision_running = _fake_ensure_supervision_running
            recovery = recover_orphaned_active_node(
                state_root=str(state_root.resolve()),
                node_id=node_id,
                workspace_root=str(workspace_root.resolve()),
                confirmed_launch_result_ref=str(launch_result_ref.resolve()),
                confirmed_runtime_status_ref=str(stale_status_ref.resolve()),
                reason="retryable evaluator fail left the node active but the stale status ref still looked terminal",
                self_attribution="implementation_gap",
                self_repair="continue the same implementer task, repair the unmet in-scope requirements, and rerun evaluator",
                observation_kind="retryable_terminal_result",
                summary="stale runtime status must be refreshed before same-node recovery is judged",
                evidence_refs=[str(implementer_result_ref.resolve())],
            )
        finally:
            sidecar_module.ensure_child_supervision_running = original_ensure_supervision_running
            supervision_runtime_ref.unlink(missing_ok=True)
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
        Draft202012Validator(result_schema).validate(recovery)

        if str(recovery.get("recovery_decision") or "") != "accepted":
            return _fail("recovery helper must accept same-node retry after refreshing a stale terminal status for retryable FAIL")
        if str(recovery.get("confirmed_runtime_recovery_reason") or "") == "authoritative_terminal_result":
            return _fail("recovery helper must not keep using a stale authoritative_terminal_result status after refresh")
        refreshed_status_ref = Path(str(recovery.get("confirmed_runtime_status_ref") or ""))
        if not refreshed_status_ref.exists():
            return _fail("recovery helper must persist the refreshed runtime status it actually used")
        if refreshed_status_ref.resolve() == stale_status_ref.resolve():
            return _fail("recovery helper must replace the stale terminal status ref with a fresh trusted status")
        recovery_context_ref = Path(str(recovery.get("recovery_context_ref") or "")).resolve()
        if not recovery_context_ref.exists():
            return _fail("recovery helper must persist RECOVERY_CONTEXT.md for the accepted retryable relaunch")
        refreshed_handoff_ref = node_machine_handoff_ref(state_root=state_root, node_id=node_id)
        refreshed_handoff = json.loads(refreshed_handoff_ref.read_text(encoding="utf-8"))
        refreshed_context_refs = [str(item) for item in list(refreshed_handoff.get("context_refs") or [])]
        if str(recovery_context_ref) not in refreshed_context_refs:
            return _fail("accepted retryable recovery must inject RECOVERY_CONTEXT.md into the refreshed handoff context refs")
        if str(implementer_result_ref.resolve()) not in refreshed_context_refs:
            return _fail("accepted retryable recovery must preserve recovery evidence refs in the refreshed handoff context refs")

        shutil.rmtree(workspace_root, ignore_errors=True)
        shutil.rmtree(state_root, ignore_errors=True)

    print("[loop-system-retryable-recovery-status-refresh] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
