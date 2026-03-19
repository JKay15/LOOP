#!/usr/bin/env python3
"""Run the first-wave LOOP system smoke path."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-smoke][FAIL] {msg}", file=sys.stderr)
    return 2


def _run_smoke_role_agent(
    *,
    role: str,
    mode: str,
    context: dict[str, object],
    operation_log_path: Path | None = None,
    work_root: Path,
) -> tuple[subprocess.CompletedProcess[str], Path, Path]:
    result_path = work_root / f"{role}_result.json"
    response_path = work_root / f"{role}_response.txt"
    context_path = work_root / f"{role}_context.json"
    context_path.write_text(json.dumps(context, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    env = dict(os.environ)
    env["LOOP_PRODUCT_EVAL_ROLE"] = role
    env["LOOP_PRODUCT_EVAL_RESULT_PATH"] = str(result_path)
    env["LOOP_PRODUCT_EVAL_RESPONSE_PATH"] = str(response_path)
    env["LOOP_PRODUCT_EVAL_CONTEXT_PATH"] = str(context_path)
    if operation_log_path is not None:
        env["LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH"] = str(operation_log_path)
    proc = subprocess.run(
        [sys.executable, "-m", "loop_product.loop.smoke_role_agent", "--mode", mode],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=env,
    )
    return proc, result_path, response_path


def _check_smoke_role_agent_truthfulness() -> None:
    with tempfile.TemporaryDirectory(prefix="loop_system_smoke_role_agent_") as td:
        temp_root = Path(td)
        current_run_root = temp_root / "current_run"
        checker_result_path = current_run_root / "roles" / "checker" / "result.json"
        checker_result_path.parent.mkdir(parents=True, exist_ok=True)
        checker_result_path.write_text(json.dumps({"status": "OK"}, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        ordinary_root = temp_root / "ordinary_tests"
        test_context = {
            "current_run_root": str(current_run_root),
            "workspace_root": str(ROOT),
            "ordinary_test_artifact_root": str(ordinary_root),
        }
        proc, result_path, _ = _run_smoke_role_agent(
            role="test_designer",
            mode="fail",
            context=test_context,
            work_root=temp_root,
        )
        if proc.returncode != 0:
            raise ValueError(f"smoke role agent test_designer failed: stdout={proc.stdout!r} stderr={proc.stderr!r}")
        test_result = json.loads(result_path.read_text(encoding="utf-8"))
        ordinary_test_results = dict(test_result.get("ordinary_test_results") or {})
        executed_tests = list(ordinary_test_results.get("executed_tests") or [])
        if not executed_tests:
            raise ValueError("smoke role agent test_designer must preserve a bounded executed probe")
        argv = [str(part) for part in (dict(executed_tests[0] or {}).get("argv") or [])]
        if "loop_product.kernel.entry" in argv or "--smoke" in argv:
            raise ValueError("smoke role agent test_designer must not claim it reran the smoke entrypoint")
        generated_tests_ref = Path(str(ordinary_test_results.get("generated_tests_ref") or ""))
        generated_tests = generated_tests_ref.read_text(encoding="utf-8")
        if "without relaunching the smoke path" not in generated_tests:
            raise ValueError("smoke role agent test_designer must record non-recursive smoke inspection")

        operation_log_path = temp_root / "ai_user" / "operation_log.md"
        ai_user_context = {
            "current_run_root": str(current_run_root),
            "workspace_root": str(ROOT),
        }
        proc, result_path, _ = _run_smoke_role_agent(
            role="ai_user",
            mode="fail",
            context=ai_user_context,
            operation_log_path=operation_log_path,
            work_root=temp_root,
        )
        if proc.returncode != 0:
            raise ValueError(f"smoke role agent ai_user failed: stdout={proc.stdout!r} stderr={proc.stderr!r}")
        ai_user_result = json.loads(result_path.read_text(encoding="utf-8"))
        operation_log_ref = Path(str(ai_user_result.get("operation_log_ref") or ""))
        operation_log = operation_log_ref.read_text(encoding="utf-8")
        if "Ran the documented smoke entrypoint" in operation_log:
            raise ValueError("smoke role agent ai_user must not claim it reran the smoke entrypoint")
        if "without relaunching the smoke entrypoint" not in operation_log:
            raise ValueError("smoke role agent ai_user must record non-recursive smoke inspection")


def main() -> int:
    smoke_script = ROOT / "scripts/smoke.sh"
    if not smoke_script.exists():
        return _fail("missing scripts/smoke.sh")

    with tempfile.TemporaryDirectory(prefix="loop_system_smoke_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        proc = subprocess.run(
            [str(smoke_script), str(state_root)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            return _fail(f"smoke.sh failed: stdout={proc.stdout!r} stderr={proc.stderr!r}")

        summary_path = state_root / "artifacts" / "smoke_summary.json"
        if not summary_path.exists():
            return _fail("smoke path must emit .loop/artifacts/smoke_summary.json")

        data = json.loads(summary_path.read_text(encoding="utf-8"))
        required_pairs = {
            "kernel_status": "ACTIVE",
            "child_status": "COMPLETED",
            "evaluator_status": "PASS_AFTER_RETRY",
            "smoke_status": "OK",
        }
        for key, expected in required_pairs.items():
            if data.get(key) != expected:
                return _fail(f"smoke summary {key!r} must be {expected!r}, got {data.get(key)!r}")

        if not data.get("roundbook"):
            return _fail("smoke summary must include roundbook entries")
        roundbook = list(data["roundbook"])
        if len(roundbook) != 2:
            return _fail(f"smoke roundbook must contain exactly fail/pass rounds, got {len(roundbook)}")
        expected_rounds = (
            ("FAIL", "RETRY"),
            ("PASS", "COMPLETE"),
        )
        for index, (entry, (expected_verdict, expected_action)) in enumerate(zip(roundbook, expected_rounds), start=1):
            if entry.get("round_index") != index:
                return _fail(f"smoke roundbook round {index} must preserve round_index={index}")
            if entry.get("verdict") != expected_verdict:
                return _fail(
                    f"smoke round {index} must have verdict {expected_verdict!r}, got {entry.get('verdict')!r}"
                )
            if entry.get("action") != expected_action:
                return _fail(
                    f"smoke round {index} must have action {expected_action!r}, got {entry.get('action')!r}"
                )
            for key in ("request_ref", "reviewer_response_ref", "evaluation_report_ref"):
                value = str(entry.get(key) or "").strip()
                if not value:
                    return _fail(f"smoke round {index} must include {key}")
                if not Path(value).exists():
                    return _fail(f"smoke round {index} {key} must point to an existing artifact")

        audit_dir = state_root / "audit"
        if not any(audit_dir.iterdir()):
            return _fail("smoke path must write at least one audit artifact")

        state_dir = state_root / "state"
        if not any(state_dir.iterdir()):
            return _fail("smoke path must write at least one state artifact")

        delegation_path = state_root / "state" / "delegations" / "child-implementer-001.json"
        if not delegation_path.exists():
            return _fail("smoke path must freeze child delegation under .loop/state/delegations/")

        kernel_state_path = state_root / "state" / "kernel_state.json"
        if not kernel_state_path.exists():
            return _fail("smoke path must persist authoritative kernel_state.json")
        kernel_state = json.loads(kernel_state_path.read_text(encoding="utf-8"))
        if not kernel_state.get("accepted_envelopes"):
            return _fail("kernel_state.json must record accepted control envelopes")
        if not kernel_state.get("active_evaluator_lanes"):
            return _fail("kernel_state.json must record accepted evaluator lanes")

        evaluator_node_dir = state_root / "artifacts" / "evaluator_nodes"
        if not evaluator_node_dir.exists() or not any(evaluator_node_dir.rglob("EvaluatorNodeRequest.json")):
            return _fail("smoke path must materialize evaluator-node request artifacts")
        evaluator_run_dir = state_root / "artifacts" / "evaluator_node_runs"
        if not evaluator_run_dir.exists():
            return _fail("smoke path must materialize evaluator-node run artifacts")
        for index, entry in enumerate(roundbook, start=1):
            run_root = Path(str(entry["evaluation_report_ref"])).parent
            if not run_root.exists():
                return _fail(f"smoke round {index} evaluator run root must exist")
            request_ref = Path(str(entry["request_ref"]))
            submission_path = request_ref.parent / "EvaluatorNodeSubmission.json"
            if not submission_path.is_file():
                return _fail(f"smoke round {index} must preserve EvaluatorNodeSubmission.json beside the request artifact")
            submission = json.loads(submission_path.read_text(encoding="utf-8"))
            context_refs = {str(item) for item in (submission.get("context_refs") or [])}
            expected_context_refs = {
                str((state_root / "state" / "child-implementer-001.json").resolve()),
                str((state_root / "state" / "delegations" / "child-implementer-001.json").resolve()),
            }
            if not expected_context_refs.issubset(context_refs):
                return _fail(
                    f"smoke round {index} evaluator submission must preserve frozen delegation and node-local context refs"
                )
            report_path = run_root / "EvaluationReport.json"
            if not report_path.is_file():
                return _fail(f"smoke round {index} missing evaluator runtime artifact EvaluationReport.json")
            report = json.loads(report_path.read_text(encoding="utf-8"))
            lanes = list(report.get("lanes") or [])
            if len(lanes) != 1:
                return _fail(f"smoke round {index} must preserve exactly one grouped evaluation-unit lane")
            lane = dict(lanes[0] or {})
            lane_root = Path(str(lane.get("lane_root") or ""))
            if not lane_root.is_dir():
                return _fail(f"smoke round {index} lane_root must point to an existing directory")
            target_unit = dict(lane.get("target_evaluation_unit") or {})
            if str(target_unit.get("unit_id") or "").strip() != "EU-001":
                return _fail(
                    f"smoke round {index} grouped lane must preserve the checker-declared unit_id, got {target_unit!r}"
                )
            if list(target_unit.get("requirement_ids") or []) != ["SMOKE-001"]:
                return _fail(
                    f"smoke round {index} grouped lane must preserve the smoke requirement ids, got {target_unit!r}"
                )
            required_files = (
                Path(".loop/checker/AGENTS.md"),
                Path(".loop/checker/AGENTS.override.md"),
                Path(".loop/checker/runs/checker/result.json"),
                Path(".loop/checker/runs/checker/invocation.json"),
                Path(".loop/checker/runs/checker/exec_span.json"),
                Path(".loop/test_ai/AGENTS.md"),
                Path(".loop/test_ai/AGENTS.override.md"),
                Path(".loop/test_ai/runs/test_ai__EU-001/result.json"),
                Path(".loop/test_ai/runs/test_ai__EU-001/invocation.json"),
                Path(".loop/test_ai/runs/test_ai__EU-001/exec_span.json"),
                Path(".loop/ai_user/AGENTS.md"),
                Path(".loop/ai_user/AGENTS.override.md"),
                Path(".loop/ai_user/runs/ai_user__EU-001/result.json"),
                Path(".loop/ai_user/runs/ai_user__EU-001/invocation.json"),
                Path(".loop/ai_user/runs/ai_user__EU-001/exec_span.json"),
                Path(".loop/reviewer/AGENTS.md"),
                Path(".loop/reviewer/AGENTS.override.md"),
                Path(".loop/reviewer/runs/reviewer/result.json"),
                Path(".loop/reviewer/runs/reviewer/invocation.json"),
                Path(".loop/reviewer/runs/reviewer/exec_span.json"),
                Path(".loop/reviewer/runs/reviewer/response.txt"),
                Path("EvaluationReport.json"),
            )
            for rel in required_files:
                path = run_root / rel
                if not path.is_file():
                    return _fail(f"smoke round {index} missing evaluator runtime artifact {rel}")
            expected_role_agents = {
                Path(".loop/checker/AGENTS.md"): (
                    "You are evaluator checker.",
                    "Do not act as test_ai, ai_user, or reviewer.",
                ),
                Path(".loop/test_ai/AGENTS.md"): (
                    "You are evaluator test_ai.",
                    "Do not rewrite final effects, checker grouping, or reviewer verdicts.",
                ),
                Path(".loop/ai_user/AGENTS.md"): (
                    "You are evaluator ai_user.",
                    "Do not redesign requirements, checker grouping, or reviewer verdicts.",
                ),
                Path(".loop/reviewer/AGENTS.md"): (
                    "You are evaluator reviewer.",
                    "Judge only from reviewer-visible artifacts.",
                ),
            }
            for rel, needles in expected_role_agents.items():
                text = (run_root / rel).read_text(encoding="utf-8")
                for needle in needles:
                    if needle not in text:
                        return _fail(f"smoke round {index} role home file {rel} must contain {needle!r}")
            for rel in (
                Path(".loop/checker/AGENTS.override.md"),
                Path(".loop/test_ai/AGENTS.override.md"),
                Path(".loop/ai_user/AGENTS.override.md"),
                Path(".loop/reviewer/AGENTS.override.md"),
            ):
                if (run_root / rel).read_text(encoding="utf-8").strip() != "":
                    return _fail(f"smoke round {index} role override file {rel} must stay empty")
            for rel in (
                Path(".loop/checker/config.toml"),
                Path(".loop/test_ai/config.toml"),
                Path(".loop/ai_user/config.toml"),
                Path(".loop/reviewer/config.toml"),
            ):
                if (run_root / rel).exists():
                    return _fail(f"smoke round {index} role home must not copy host config.toml wholesale into {rel}")

            invocation_expectations = (
                (
                    Path(".loop/checker/runs/checker/exec_span.json"),
                    Path(".loop/checker/workspaces/checker/workspace"),
                ),
                (
                    Path(".loop/test_ai/runs/test_ai__EU-001/exec_span.json"),
                    Path(".loop/test_ai/workspaces/test_ai__EU-001/workspace"),
                ),
                (
                    Path(".loop/ai_user/runs/ai_user__EU-001/exec_span.json"),
                    Path(".loop/ai_user/workspaces/ai_user__EU-001/workspace"),
                ),
                (
                    Path(".loop/reviewer/runs/reviewer/exec_span.json"),
                    Path(".loop/reviewer/workspaces/reviewer/workspace"),
                ),
            )
            for rel, expected_workspace_rel in invocation_expectations:
                exec_span = json.loads((run_root / rel).read_text(encoding="utf-8"))
                invocation = json.loads((run_root / rel.parent / "invocation.json").read_text(encoding="utf-8"))
                requested_agent_cmd = str(
                    (invocation.get("requested_config") or {}).get("agent_cmd") or invocation.get("effective_agent_cmd") or ""
                )
                if "loop_product.loop.smoke_role_agent" not in requested_agent_cmd:
                    return _fail(
                        f"smoke round {index} {rel} must run through the repo-local smoke role agent, got {requested_agent_cmd!r}"
                    )
                expected_workspace = str((run_root / expected_workspace_rel).resolve())
                if str(exec_span.get("cwd") or "") != expected_workspace:
                    return _fail(
                        f"smoke round {index} {rel} must pin cwd to {expected_workspace!r}, "
                        f"got {exec_span.get('cwd')!r}"
                    )

        try:
            _check_smoke_role_agent_truthfulness()
        except ValueError as exc:
            return _fail(str(exc))

        quarantine_dir = state_root / "quarantine"
        if not quarantine_dir.exists():
            return _fail("smoke path must materialize the quarantine directory even if unused")

    print("[loop-system-smoke] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
