#!/usr/bin/env python3
"""Validate the committed child progress snapshot helper."""

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
    print(f"[loop-system-child-progress-snapshot][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict:
    path = ROOT / "docs" / "schemas" / name
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def main() -> int:
    from loop_product.dispatch.child_progress_snapshot import child_progress_snapshot_from_launch_result_ref

    snapshot_schema = _load_schema("LoopChildProgressSnapshot.schema.json")

    with tempfile.TemporaryDirectory(prefix="loop_system_child_progress_snapshot_") as td:
        temp_root = Path(td)
        state_root = ROOT / ".loop" / "test-child-progress-snapshot"
        workspace_root = ROOT / "workspace" / "test-child-progress-snapshot"
        shutil.rmtree(state_root, ignore_errors=True)
        shutil.rmtree(workspace_root, ignore_errors=True)
        try:
            launch_dir = state_root / "artifacts" / "launches" / "test-child-progress-snapshot" / "attempt_001"
            log_dir = launch_dir / "logs"
            log_dir.mkdir(parents=True, exist_ok=True)
            stdout_ref = log_dir / "stdout.txt"
            stderr_ref = log_dir / "stderr.txt"
            stdout_ref.write_text("", encoding="utf-8")
            stderr_ref.write_text(
                "\n".join(
                    [
                        "Read and follow:",
                        "- '/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/workspace/AGENTS.md'",
                        "- '/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/.agents/skills/evaluator-exec/SKILL.md'",
                        "LEAN_PATH=/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/workspace/test-child-progress-snapshot/.lake/build/lib/lean:/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/workspace/test-child-progress-snapshot/.lake/packages/mathlib/.lake/build/lib/lean",
                        'exec',
                        '/bin/zsh -lc "sed -n \'1,240p\' /Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md"',
                        '/bin/zsh -lc "sed -n \'1,240p\' /Users/xiongjiangkai/.codex/skills/lean4/SKILL.md"',
                        "codex",
                        "I’m repairing the artifact before rerunning evaluator.",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            launch_result_ref = launch_dir / "ChildLaunchResult.json"
            launch_result_ref.write_text(
                json.dumps(
                    {
                        "launch_decision": "started",
                        "node_id": "test-child-progress-snapshot",
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "launch_request_ref": str((launch_dir / "ChildLaunchRequest.json").resolve()),
                        "launch_result_ref": str(launch_result_ref.resolve()),
                        "launch_log_dir": str(log_dir.resolve()),
                        "stdout_ref": str(stdout_ref.resolve()),
                        "stderr_ref": str(stderr_ref.resolve()),
                        "stdin_ref": str((launch_dir / "stdin.txt").resolve()),
                        "wrapped_argv": ["codex", "exec", "-"],
                        "wrapper_cmd": "",
                        "pid": 12345,
                        "exit_code": None,
                        "startup_health_timeout_ms": 12000,
                        "startup_retry_limit": 1,
                        "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                        "attempt_count": 1,
                        "retryable_failure_kind": "",
                        "attempts": [],
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            implementer_result_ref = state_root / "artifacts" / "test-child-progress-snapshot" / "implementer_result.json"
            implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
            implementer_result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": "test-child-progress-snapshot",
                        "status": "ACTIVE",
                        "outcome": "REPAIR_REQUIRED",
                        "evaluator_result": {
                            "verdict": "FAIL",
                            "retryable": True,
                            "summary": "same-run evaluator recovery is still pending",
                            "diagnostics": {
                                "self_attribution": "product_gap",
                                "self_repair": "repair the unmet requirement and rerun evaluator",
                            },
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            run_root = (
                state_root
                / "artifacts"
                / "evaluator_runs"
                / "test-child-progress-snapshot"
                / "implementer_task_test-child-progress-snapshot"
                / "R1__test-child-progress-snapshot__evaluator"
            )
            run_root.mkdir(parents=True, exist_ok=True)
            evaluator_run_state_ref = run_root / "EvaluatorRunState.json"
            evaluator_run_state_ref.write_text(
                json.dumps(
                    {
                        "evaluation_id": "implementer_task_test-child-progress-snapshot",
                        "run_id": "R1__test-child-progress-snapshot__evaluator",
                        "status": "RECOVERY_REQUIRED",
                        "checker": {"status": "COMPLETED", "attempt_count": 1},
                        "lanes_by_unit_id": {
                            "EU-001": {
                                "status": "COMPLETED",
                                "lane_root": str((run_root / ".loop" / "lanes" / "001_EU-001").resolve()),
                                "target_evaluation_unit": {
                                    "unit_id": "EU-001",
                                    "requirement_ids": ["REQ-001"],
                                    "requirements": [{"requirement_id": "REQ-001", "description": "artifact exists"}],
                                },
                            },
                            "EU-002": {
                                "status": "BLOCKED_RETRYABLE",
                                "lane_root": str((run_root / ".loop" / "lanes" / "002_EU-002").resolve()),
                                "target_evaluation_unit": {
                                    "unit_id": "EU-002",
                                    "requirement_ids": ["REQ-002"],
                                    "requirements": [{"requirement_id": "REQ-002", "description": "artifact honest"}],
                                },
                            },
                            "EU-003": {
                                "status": "RUNNING",
                                "lane_root": str((run_root / ".loop" / "lanes" / "003_EU-003").resolve()),
                                "target_evaluation_unit": {
                                    "unit_id": "EU-003",
                                    "requirement_ids": ["REQ-003"],
                                    "requirements": [{"requirement_id": "REQ-003", "description": "terminal result exists"}],
                                },
                            },
                        },
                        "reviewer": {"status": "PENDING", "attempt_count": 0},
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            def _status_reader(*, result_ref: str | Path, stall_threshold_s: float = 60.0) -> dict[str, object]:
                status_result_ref = temp_root / "ChildRuntimeStatusResult.json"
                return {
                    "launch_result_ref": str(Path(result_ref).resolve()),
                    "status_result_ref": str(status_result_ref.resolve()),
                    "node_id": "test-child-progress-snapshot",
                    "workspace_root": str(workspace_root.resolve()),
                    "state_root": str(state_root.resolve()),
                    "node_ref": str((state_root / "state" / "test-child-progress-snapshot.json").resolve()),
                    "launch_decision": "started",
                    "pid": 12345,
                    "pid_alive": True,
                    "exit_code": None,
                    "lifecycle_status": "ACTIVE",
                    "runtime_attachment_state": "ATTACHED",
                    "runtime_observed_at": "2026-03-20T00:00:00Z",
                    "runtime_observation_kind": "direct_runtime_probe",
                    "runtime_summary": "child process is alive",
                    "runtime_evidence_refs": [str(stderr_ref.resolve())],
                    "stdout_ref": str(stdout_ref.resolve()),
                    "stderr_ref": str(stderr_ref.resolve()),
                    "latest_log_ref": str(stderr_ref.resolve()),
                    "latest_log_mtime": "2026-03-20T00:00:00Z",
                    "latest_log_age_s": 3.5,
                    "stall_threshold_s": float(stall_threshold_s),
                    "stalled_hint": False,
                    "recovery_eligible": False,
                    "recovery_reason": "",
                }

            snapshot = child_progress_snapshot_from_launch_result_ref(
                result_ref=str(launch_result_ref.resolve()),
                stall_threshold_s=17.5,
                runtime_status_reader=_status_reader,
            )
            Draft202012Validator(snapshot_schema).validate(snapshot)

            if str(snapshot.get("node_id") or "") != "test-child-progress-snapshot":
                return _fail("snapshot must preserve node_id")
            if str(snapshot.get("runtime_status_ref") or "") == "":
                return _fail("snapshot must expose the committed runtime status result ref")
            if bool(snapshot.get("pid_alive")) is not True:
                return _fail("snapshot must preserve pid_alive from runtime status")
            if float(snapshot.get("latest_log_age_s") or -1.0) != 3.5:
                return _fail("snapshot must preserve latest_log_age_s from runtime status")
            if bool(snapshot.get("terminal_result_present")) is not True:
                return _fail("snapshot must surface authoritative implementer_result presence")
            if str(snapshot.get("evaluator_status") or "") != "RECOVERY_REQUIRED":
                return _fail("snapshot must surface latest evaluator run status when present")
            if list(snapshot.get("evaluator_running_unit_ids") or []) != ["EU-003"]:
                return _fail("snapshot must identify running evaluator units")
            if list(snapshot.get("evaluator_blocked_retryable_unit_ids") or []) != ["EU-002"]:
                return _fail("snapshot must identify blocked retryable evaluator units")
            observed_repo_doc_refs = list(snapshot.get("observed_repo_doc_refs") or [])
            required_repo_refs = {
                "/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/workspace/AGENTS.md",
                "/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/.agents/skills/evaluator-exec/SKILL.md",
                "/Users/xiongjiangkai/xjk_papers/leanatlas/loop_product_repo/docs/contracts/LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
            }
            if not required_repo_refs.issubset(set(observed_repo_doc_refs)):
                return _fail("snapshot must extract repo-local document refs from committed logs")
            observed_external_doc_refs = list(snapshot.get("observed_external_doc_refs") or [])
            if "/Users/xiongjiangkai/.codex/skills/lean4/SKILL.md" not in observed_external_doc_refs:
                return _fail("snapshot must extract non-repo document refs from committed logs")
            if list(snapshot.get("recent_log_lines") or []) == []:
                return _fail("snapshot must preserve a compact recent log excerpt")
            snapshot_ref = Path(str(snapshot.get("snapshot_ref") or ""))
            if not snapshot_ref.exists():
                return _fail("snapshot helper must persist a snapshot artifact")
            persisted = json.loads(snapshot_ref.read_text(encoding="utf-8"))
            if persisted.get("snapshot_ref") != str(snapshot_ref.resolve()):
                return _fail("persisted snapshot must be self-identifying")
        finally:
            shutil.rmtree(state_root, ignore_errors=True)
            shutil.rmtree(workspace_root, ignore_errors=True)

    print("[loop-system-child-progress-snapshot] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
