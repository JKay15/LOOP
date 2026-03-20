"""Deterministic local role agent for the real evaluator-backed smoke path."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _expected_verdict(mode: str) -> str:
    return "FAIL" if mode == "fail" else "PASS"


def _run_artifact_probe(*, context: dict[str, object]) -> tuple[list[str], subprocess.CompletedProcess[str], int]:
    workspace_root = Path(str(context["workspace_root"])).resolve()
    current_run_root = Path(str(context["current_run_root"])).resolve()
    probe_script = """
from pathlib import Path
import json
import sys

workspace_root = Path(sys.argv[1]).resolve()
current_run_root = Path(sys.argv[2]).resolve()
required = [
    workspace_root / "scripts" / "smoke.sh",
    workspace_root / "loop_product" / "kernel" / "entry.py",
    workspace_root / "loop_product" / "loop" / "runner.py",
    workspace_root / "loop_product" / "loop" / "smoke_role_agent.py",
    current_run_root / "roles" / "checker" / "result.json",
]
missing = [str(path) for path in required if not path.exists()]
if missing:
    raise SystemExit("missing smoke surface(s): " + ", ".join(missing))
checker = json.loads((current_run_root / "roles" / "checker" / "result.json").read_text(encoding="utf-8"))
print("inspected smoke source surfaces and current-run checker artifact")
print("checker_status=" + str(checker.get("status")))
"""
    argv = [sys.executable, "-c", probe_script, str(workspace_root), str(current_run_root)]
    start_ns = time.time_ns()
    proc = subprocess.run(argv, cwd=str(workspace_root), capture_output=True, text=True)
    duration_ms = int((time.time_ns() - start_ns) / 1_000_000)
    return argv, proc, duration_ms


def _checker_result(*, mode: str) -> dict[str, object]:
    requirement = {
        "requirement_id": "SMOKE-001",
        "description": (
            "The bounded smoke round preserves real evaluator-node request/report artifacts and reaches the expected "
            f"reviewer verdict `{_expected_verdict(mode)}`."
        ),
        "blocking": True,
        "requirement_kind": "product_effect",
    }
    return {
        "status": "OK",
        "normalized_requirements": [requirement],
        "requirement_graph": {
            "requirements": [requirement],
            "evaluation_units": [{"unit_id": "EU-001", "requirement_ids": ["SMOKE-001"]}],
            "dependency_edges": [],
        },
        "repair_actions": [],
        "notes": [f"checker confirmed deterministic smoke mode `{mode}`"],
    }


def _test_designer_result(*, context: dict[str, object], mode: str) -> dict[str, object]:
    ordinary_root_raw = str(context.get("ordinary_test_artifact_root") or "").strip()
    if ordinary_root_raw:
        ordinary_root = Path(ordinary_root_raw)
        generated_tests_ref = ordinary_root / "generated_tests.md"
        stdout_ref = ordinary_root / "stdout.txt"
        stderr_ref = ordinary_root / "stderr.txt"
        exec_span_ref = ordinary_root / "exec_span.json"
        argv, probe_proc, duration_ms = _run_artifact_probe(context=context)
        _write_text(
            generated_tests_ref,
            (
                "# Smoke ordinary tests\n\n"
                "1. Inspect the documented smoke surfaces from repo root without relaunching the smoke path.\n"
                "2. Inspect the current-run checker artifact for the active smoke round.\n"
            ),
        )
        _write_text(stdout_ref, probe_proc.stdout)
        _write_text(stderr_ref, probe_proc.stderr)
        _write_json(
            exec_span_ref,
            {
                "cwd": str(Path(str(context["workspace_root"])).resolve()),
                "duration_ms": duration_ms,
                "exit_code": int(probe_proc.returncode),
            },
        )
        return {
            "status": "OK",
            "ordinary_test_results": {
                "all_passed": probe_proc.returncode == 0,
                "generated_tests_ref": str(generated_tests_ref),
                "executed_tests": [
                    {
                        "test_id": "OT-001",
                        "test_kind": "smoke_artifact_probe",
                        "argv": argv,
                        "passed": probe_proc.returncode == 0,
                        "exit_code": int(probe_proc.returncode),
                        "stdout_ref": str(stdout_ref),
                        "stderr_ref": str(stderr_ref),
                        "exec_span_ref": str(exec_span_ref),
                    }
                ],
            },
            "notes": [f"ordinary tests ran a bounded non-recursive artifact probe for smoke mode `{mode}`"],
        }
    return {
        "status": "OK",
        "notes": [f"simple evaluator delegated smoke test_ai output stayed deterministic for mode `{mode}`"],
    }


def _ai_user_result(*, context: dict[str, object], operation_log_path: Path, mode: str) -> dict[str, object]:
    outcome = "RETRY_REQUIRED" if mode == "fail" else "PASS"
    current_run_root = Path(str(context["current_run_root"])).resolve()
    _write_text(
        operation_log_path,
        (
            "1. Read the smoke manual.\n"
            "2. Inspected the documented smoke surfaces and current-run checker artifact without relaunching the smoke entrypoint.\n"
            f"3. Confirmed reviewer-visible evidence for mode={mode} under {current_run_root}.\n"
        ),
    )
    return {
        "status": "OK",
        "operation_log_ref": str(operation_log_path),
        "effect_results": [
            {
                "requirement_id": "SMOKE-001",
                "outcome": outcome,
                "summary": (
                    "Reviewer-visible smoke evidence requires a bounded retry."
                    if mode == "fail"
                    else "Reviewer-visible smoke evidence satisfied the acceptance surface."
                ),
                "evidence_refs": [str(operation_log_path)],
            }
        ],
        "notes": [f"ai-user preserved a faithful single-unit log for mode `{mode}`"],
    }


def _reviewer_result(*, mode: str) -> dict[str, object]:
    verdict = _expected_verdict(mode)
    summary = (
        "The first smoke round exposed a retryable gap, so the child should retry once."
        if mode == "fail"
        else "The retry round satisfied the smoke acceptance surface."
    )
    return {
        "status": "OK",
        "effect_reviews": [
            {
                "requirement_id": "SMOKE-001",
                "verdict": verdict,
                "summary": summary,
                "evidence_refs": ["run:.loop/ai_user/runs/ai_user__EU-001/operation_log.md"],
                "reproduction_steps": [
                    "1. Inspect the current round EvaluatorNodeRequest.json and role artifacts.",
                    "2. Confirm the reviewer verdict matches the deterministic smoke mode.",
                ],
            }
        ],
        "summary": summary,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Deterministic smoke role agent")
    parser.add_argument("--mode", choices=("fail", "pass"), required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    role = str(os.environ["LOOP_PRODUCT_EVAL_ROLE"])
    result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
    response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])
    context = json.loads(Path(os.environ["LOOP_PRODUCT_EVAL_CONTEXT_PATH"]).read_text(encoding="utf-8"))

    if role == "checker":
        result = _checker_result(mode=args.mode)
        _write_json(result_path, result)
        _write_text(response_path, f"# Checker\n\nSmoke checker finished with mode `{args.mode}`.\n")
        return 0
    if role == "test_designer":
        result = _test_designer_result(context=context, mode=args.mode)
        _write_json(result_path, result)
        _write_text(response_path, f"# Test Designer\n\nSmoke tests finished with mode `{args.mode}`.\n")
        return 0
    if role == "ai_user":
        operation_log_raw = os.environ.get("LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH", "").strip()
        operation_log_path = Path(operation_log_raw) if operation_log_raw else None
        if operation_log_path is not None:
            result = _ai_user_result(context=context, operation_log_path=operation_log_path, mode=args.mode)
        else:
            result = {
                "status": "OK",
                "operation_log_ref": "",
                "effect_results": [
                    {
                        "requirement_id": "SMOKE-001",
                        "outcome": "RETRY_REQUIRED" if args.mode == "fail" else "PASS",
                        "summary": "Simple evaluator smoke ai_user produced reviewer-visible raw output.",
                        "evidence_refs": [str(response_path)],
                    }
                ],
                "notes": [f"ai-user preserved a faithful single-unit log for mode `{args.mode}`"],
            }
        _write_json(result_path, result)
        _write_text(response_path, f"# AI-as-User\n\nSmoke user run finished with mode `{args.mode}`.\n")
        return 0
    if role == "reviewer":
        result = _reviewer_result(mode=args.mode)
        _write_json(result_path, result)
        _write_text(
            response_path,
            f"VERDICT: {_expected_verdict(args.mode)}\nsummary: smoke review finished with mode `{args.mode}`.\n",
        )
        return 0
    raise SystemExit(f"unexpected role: {role}")


if __name__ == "__main__":
    raise SystemExit(main())
