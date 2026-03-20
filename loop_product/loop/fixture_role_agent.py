"""Repo-shipped deterministic evaluator role agent for non-smoke test coverage."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import time
from pathlib import Path


def _write_json(path: Path, obj: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _counter_path(*, base_root: Path, label: str, scenario: str) -> Path:
    return base_root / ".loop_fixture_state" / f"{label}.{scenario}.count"


def _bump_counter(*, base_root: Path, label: str, scenario: str) -> int:
    path = _counter_path(base_root=base_root, label=label, scenario=scenario)
    current = 0
    if path.exists():
        current = int(path.read_text(encoding="utf-8").strip())
    current += 1
    _write_text(path, f"{current}\n")
    return current


def _target_unit_and_requirements(context: dict[str, object]) -> tuple[dict[str, object], list[dict[str, object]]]:
    target_unit = dict(context.get("target_evaluation_unit") or {})
    requirements = [dict(item) for item in (target_unit.get("requirements") or [])]
    if target_unit and requirements:
        return target_unit, requirements
    requirement = dict(context.get("requirement") or {})
    if requirement:
        rid = str(requirement.get("requirement_id") or "").strip()
        return (
            {
                "unit_id": f"EU-{rid}",
                "requirement_ids": [rid],
                "requirements": [requirement],
            },
            [requirement],
        )
    return {"unit_id": "EU-UNKNOWN", "requirement_ids": [], "requirements": []}, []


def _checker_endpoint_pass() -> dict[str, object]:
    requirements = [
        {
            "requirement_id": "REQ-001",
            "description": "The endpoint clarification product stays runnable and documented.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
        {
            "requirement_id": "REQ-002",
            "description": "The host adapter remains a host adapter rather than product core.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
    ]
    return {
        "status": "OK",
        "normalized_requirements": requirements,
        "requirement_graph": {
            "requirements": requirements,
            "evaluation_units": [{"unit_id": "EU-001", "requirement_ids": ["REQ-001", "REQ-002"]}],
            "dependency_edges": [],
        },
        "repair_actions": [],
        "human_gate_reasons": [],
        "notes": ["checker grouped both requirements into one co-testable unit"],
    }


def _test_designer_endpoint_pass(*, context: dict[str, object]) -> dict[str, object]:
    ordinary_root_raw = str(context.get("ordinary_test_artifact_root") or "").strip()
    if ordinary_root_raw:
        ordinary_root = Path(ordinary_root_raw)
        stdout_ref = ordinary_root / "stdout.txt"
        stderr_ref = ordinary_root / "stderr.txt"
        exec_span_ref = ordinary_root / "exec_span.json"
        _write_text(stdout_ref, "endpoint clarification help output\n")
        _write_text(stderr_ref, "")
        _write_json(exec_span_ref, {"cwd": str(Path(context["workspace_root"]).resolve()), "exit_code": 0})
        return {
            "status": "OK",
            "ordinary_test_results": {
                "all_passed": True,
                "executed_tests": [
                    {
                        "test_id": "OT-001",
                        "test_kind": "documented_cli_probe",
                        "argv": ["python", "-m", "loop_product.endpoint.clarification", "--help"],
                        "passed": True,
                        "exit_code": 0,
                        "stdout_ref": str(stdout_ref),
                        "stderr_ref": str(stderr_ref),
                        "exec_span_ref": str(exec_span_ref),
                    }
                ],
            },
            "notes": ["ordinary tests stayed on the documented surface"],
        }
    return {
        "status": "OK",
        "notes": ["simple evaluator delegated test_ai output stayed on the documented surface"],
    }


def _ai_user_endpoint_pass(*, response_path: Path, operation_log_path: Path | None) -> dict[str, object]:
    if operation_log_path is not None:
        _write_text(
            operation_log_path,
            "1. Read the manual.\n2. Ran the documented help surface.\n3. Confirmed host-adapter boundary evidence.\n",
        )
    evidence_ref = str(operation_log_path) if operation_log_path is not None else str(response_path)
    return {
        "status": "OK",
        "operation_log_ref": str(operation_log_path) if operation_log_path is not None else "",
        "effect_results": [
            {
                "requirement_id": "REQ-001",
                "outcome": "PASS",
                "summary": "Documented endpoint clarification surface behaved as expected.",
                "evidence_refs": [evidence_ref],
            },
            {
                "requirement_id": "REQ-002",
                "outcome": "PASS",
                "summary": "Host adapter evidence remained separate from product core.",
                "evidence_refs": [evidence_ref],
            },
        ],
        "notes": ["ai-user ran exactly one evaluation unit"],
    }


def _reviewer_endpoint_pass() -> dict[str, object]:
    return {
        "status": "OK",
        "effect_reviews": [
            {
                "requirement_id": "REQ-001",
                "verdict": "PASS",
                "evidence_refs": ["run:.loop/ai_user/runs/ai_user__EU-001/operation_log.md"],
                "reproduction_steps": [
                    "1. Run the documented endpoint clarification help surface.",
                    "2. Inspect the reviewer-visible ordinary-test and AI-user artifacts.",
                ],
            },
            {
                "requirement_id": "REQ-002",
                "verdict": "PASS",
                "evidence_refs": ["run:.loop/ai_user/runs/ai_user__EU-001/operation_log.md"],
                "reproduction_steps": [
                    "1. Inspect the repo-local manuals and contracts.",
                    "2. Confirm the host rule is separate from the core runtime contract.",
                ],
            },
        ],
    }


def _checker_single_requirement() -> dict[str, object]:
    requirement = {
        "requirement_id": "REQ-001",
        "description": "Synthetic failure classification probe.",
        "blocking": True,
        "requirement_kind": "product_effect",
    }
    return {
        "status": "OK",
        "normalized_requirements": [requirement],
        "requirement_graph": {
            "requirements": [requirement],
            "evaluation_units": [{"unit_id": "EU-001", "requirement_ids": ["REQ-001"]}],
            "dependency_edges": [],
        },
        "repair_actions": [],
        "human_gate_reasons": [],
        "notes": [],
    }


def _checker_simple_stdout_handoff() -> dict[str, object]:
    requirements = [
        {
            "requirement_id": "REQ-001",
            "description": "Observe the first split effect.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
        {
            "requirement_id": "REQ-002",
            "description": "Observe the second split effect.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
    ]
    return {
        "status": "OK",
        "normalized_requirements": requirements,
        "requirement_graph": {
            "requirements": requirements,
            "evaluation_units": [
                {
                    "unit_id": "EU-ONE-BUNDLE",
                    "requirement_ids": ["REQ-001", "REQ-002"],
                }
            ],
            "dependency_edges": [],
        },
        "repair_actions": [
            {
                "raw_item": "one merged item",
                "action": "split_merged_effects",
                "emitted_requirement_ids": ["REQ-001", "REQ-002"],
            }
        ],
        "notes": ["split a merged final effect into two requirements"],
    }


def _checker_lane_resume_same_run() -> dict[str, object]:
    requirements = [
        {
            "requirement_id": "REQ-001",
            "description": "Preserve completed first-lane work across same-run resume.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
        {
            "requirement_id": "REQ-002",
            "description": "Resume only the unfinished second lane.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
    ]
    return {
        "status": "OK",
        "normalized_requirements": requirements,
        "requirement_graph": {
            "requirements": requirements,
            "evaluation_units": [
                {
                    "unit_id": "EU-001",
                    "requirement_ids": ["REQ-001"],
                },
                {
                    "unit_id": "EU-002",
                    "requirement_ids": ["REQ-002"],
                },
            ],
            "dependency_edges": [],
        },
        "repair_actions": [],
        "human_gate_reasons": [],
        "notes": ["resume fixture split the request into two stable evaluation units"],
    }


def _checker_runtime_closure_requirements() -> dict[str, object]:
    requirements = [
        {
            "requirement_id": "REQ-001",
            "description": "The workspace mirror report exists and truthfully summarizes the formalization gap.",
            "blocking": True,
            "requirement_kind": "product_effect",
        },
        {
            "requirement_id": "REQ-002",
            "description": "The implementer run reaches and returns a terminal evaluator-backed result.",
            "blocking": True,
            "requirement_kind": "runtime_closure",
        },
    ]
    return {
        "status": "OK",
        "normalized_requirements": requirements,
        "requirement_graph": {
            "requirements": requirements,
            "evaluation_units": [
                {
                    "unit_id": "EU-MIXED",
                    "requirement_ids": ["REQ-001", "REQ-002"],
                }
            ],
            "dependency_edges": [],
        },
        "repair_actions": [],
        "human_gate_reasons": [],
        "notes": ["fixture mixes one product-facing effect with one runtime-closure effect"],
    }


def _checker_runtime_closure_only_requirements() -> dict[str, object]:
    requirements = [
        {
            "requirement_id": "REQ-RUNTIME-ONLY",
            "description": "The implementer run reaches and returns a terminal evaluator-backed result.",
            "blocking": True,
            "requirement_kind": "runtime_closure",
        }
    ]
    return {
        "status": "OK",
        "normalized_requirements": requirements,
        "requirement_graph": {
            "requirements": requirements,
            "evaluation_units": [],
            "dependency_edges": [],
        },
        "repair_actions": [],
        "human_gate_reasons": [],
        "notes": ["fixture emits only runtime-closure requirements so the evaluator must finish without delegated lanes"],
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Repo-shipped deterministic evaluator role agent")
    parser.add_argument(
        "--scenario",
        choices=(
            "endpoint_pass",
            "lane_failure",
            "transient_lane_failure_once",
            "lane_resume_same_run",
            "terminal_fail",
            "response_then_fail",
            "pass_with_error_words",
            "unknown_reviewer",
            "simple_stdout_handoff",
            "recursive_reviewer",
            "runtime_closure_requirements",
            "runtime_closure_only_requirements",
            "linger_after_response",
        ),
        required=True,
    )
    parser.add_argument("--failing-role", default="")
    parser.add_argument("--failure-message", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    role = str(os.environ["LOOP_PRODUCT_EVAL_ROLE"])
    result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
    response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])
    context = json.loads(Path(os.environ["LOOP_PRODUCT_EVAL_CONTEXT_PATH"]).read_text(encoding="utf-8"))
    operation_log_raw = str(os.environ.get("LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH") or "").strip()
    operation_log_path = Path(operation_log_raw) if operation_log_raw else None
    product_repo_root = Path(__file__).resolve().parents[2]

    if args.scenario == "endpoint_pass":
        if role == "checker":
            _write_json(result_path, _checker_endpoint_pass())
            _write_text(response_path, "# Checker\n\nFixture checker completed.\n")
            return 0
        if role == "test_designer":
            _write_json(result_path, _test_designer_endpoint_pass(context=context))
            _write_text(response_path, "# Test Designer\n\nFixture ordinary tests completed.\n")
            return 0
        if role == "ai_user":
            _write_json(result_path, _ai_user_endpoint_pass(response_path=response_path, operation_log_path=operation_log_path))
            _write_text(response_path, "# AI-as-User\n\nFixture user run completed.\n")
            return 0
        if role == "reviewer":
            _write_json(result_path, _reviewer_endpoint_pass())
            _write_text(response_path, "VERDICT: PASS\nsummary: fixture reviewer completed with reviewer-visible evidence.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "lane_failure":
        if args.failing_role and role == args.failing_role:
            print(args.failure_message or "synthetic evaluator lane failure", file=sys.stderr)
            return 3
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nSynthetic checker completed.\n")
            return 0
        if role == "test_designer":
            _write_json(result_path, {"status": "OK", "notes": ["ordinary test completed"]})
            _write_text(response_path, "# Test Designer\n\nSynthetic lane completed.\n")
            return 0
        if role == "ai_user":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "operation_log_ref": "",
                    "effect_results": [
                        {
                            "requirement_id": "REQ-001",
                            "outcome": "PASS",
                            "summary": "Synthetic user lane completed.",
                            "evidence_refs": [str(response_path)],
                        }
                    ],
                    "notes": [],
                },
            )
            _write_text(response_path, "# AI-as-User\n\nSynthetic lane completed.\n")
            return 0
        if role == "reviewer":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "effect_reviews": [
                        {
                            "requirement_id": "REQ-001",
                            "verdict": "PASS",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Synthetic reviewer completed."],
                        }
                    ],
                },
            )
            _write_text(response_path, "VERDICT: PASS\nsummary: synthetic reviewer completed.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "transient_lane_failure_once":
        marker = Path(context["workspace_root"]) / ".loop_fixture_state" / f"{role}.transient_lane_failure_once"
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nSynthetic checker completed.\n")
            return 0
        if role == "test_designer":
            if not marker.exists():
                _write_text(marker, "seen\n")
                print("We're currently experiencing high demand, which may cause temporary errors.", file=sys.stderr)
                return 3
            _write_json(result_path, {"status": "OK", "notes": ["ordinary test completed after transient provider retry"]})
            _write_text(response_path, "# Test Designer\n\nRecovered after transient provider retry.\n")
            return 0
        if role == "ai_user":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "operation_log_ref": "",
                    "effect_results": [
                        {
                            "requirement_id": "REQ-001",
                            "outcome": "PASS",
                            "summary": "Synthetic user lane completed after transient retry upstream.",
                            "evidence_refs": [str(response_path)],
                        }
                    ],
                    "notes": [],
                },
            )
            _write_text(response_path, "# AI-as-User\n\nSynthetic lane completed.\n")
            return 0
        if role == "reviewer":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "effect_reviews": [
                        {
                            "requirement_id": "REQ-001",
                            "verdict": "PASS",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Synthetic reviewer completed after transient lane retry."],
                        }
                    ],
                },
            )
            _write_text(response_path, "VERDICT: PASS\nsummary: synthetic reviewer completed after transient lane retry.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "lane_resume_same_run":
        role_instance = str(os.environ.get("LOOP_PRODUCT_EVAL_ROLE_INSTANCE") or role).strip() or role
        source_workspace_root = Path(
            str(os.environ.get("LOOP_PRODUCT_EVAL_SOURCE_WORKSPACE_ROOT") or context["workspace_root"])
        ).resolve()
        count = _bump_counter(
            base_root=source_workspace_root,
            label=role_instance,
            scenario="lane_resume_same_run",
        )
        if role == "checker":
            _write_json(result_path, _checker_lane_resume_same_run())
            _write_text(response_path, "# Checker\n\nFrozen lane-resume checker graph completed.\n")
            return 0
        if role == "test_designer":
            target_unit, _requirements = _target_unit_and_requirements(context)
            unit_id = str(target_unit.get("unit_id") or "EU-UNKNOWN")
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "notes": [f"test_ai completed for {unit_id} on attempt {count}"],
                },
            )
            _write_text(response_path, f"# Test Designer\n\nCompleted {unit_id}.\n")
            return 0
        if role == "ai_user":
            target_unit, requirements = _target_unit_and_requirements(context)
            unit_id = str(target_unit.get("unit_id") or "EU-UNKNOWN")
            if unit_id == "EU-002" and count <= 2:
                print("We're currently experiencing high demand, which may cause temporary errors.", file=sys.stderr)
                return 3
            requirement_id = str((requirements[0] if requirements else {}).get("requirement_id") or "REQ-UNKNOWN")
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "operation_log_ref": str(operation_log_path) if operation_log_path is not None else "",
                    "effect_results": [
                        {
                            "requirement_id": requirement_id,
                            "outcome": "PASS",
                            "summary": f"AI-as-User completed {unit_id} on attempt {count}.",
                            "evidence_refs": [str(response_path)],
                        }
                    ],
                    "notes": [f"ai_user completed {unit_id} on attempt {count}"],
                },
            )
            _write_text(response_path, f"# AI-as-User\n\nCompleted {unit_id} on attempt {count}.\n")
            return 0
        if role == "reviewer":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "effect_reviews": [
                        {
                            "requirement_id": "REQ-001",
                            "verdict": "PASS",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Resume preserved the completed first lane."],
                        },
                        {
                            "requirement_id": "REQ-002",
                            "verdict": "PASS",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Resume reran only the unfinished second lane."],
                        },
                    ],
                },
            )
            _write_text(response_path, "VERDICT: PASS\nsummary: same-run lane resume fixture reviewer completed.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "unknown_reviewer":
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nSynthetic checker completed.\n")
            return 0
        if role == "test_designer":
            _write_json(result_path, {"status": "OK", "notes": ["ordinary test completed"]})
            _write_text(response_path, "# Test Designer\n\nSynthetic lane completed.\n")
            return 0
        if role == "ai_user":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "operation_log_ref": "",
                    "effect_results": [
                        {
                            "requirement_id": "REQ-001",
                            "outcome": "PASS",
                            "summary": "Synthetic user lane completed.",
                            "evidence_refs": [str(response_path)],
                        }
                    ],
                    "notes": [],
                },
            )
            _write_text(response_path, "# AI-as-User\n\nSynthetic lane completed.\n")
            return 0
        if role == "reviewer":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "effect_reviews": [
                        {
                            "requirement_id": "REQ-001",
                            "verdict": "UNKNOWN",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Synthetic reviewer returned UNKNOWN."],
                        }
                    ],
                },
            )
            _write_text(response_path, "VERDICT: UNKNOWN\nsummary: synthetic reviewer returned unknown.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "pass_with_error_words":
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nSynthetic checker completed.\n")
            return 0
        if role == "test_designer":
            _write_json(result_path, {"status": "OK", "notes": ["ordinary test completed"]})
            _write_text(response_path, "# Test Designer\n\nSynthetic lane completed.\n")
            return 0
        if role == "ai_user":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "operation_log_ref": "",
                    "effect_results": [
                        {
                            "requirement_id": "REQ-001",
                            "outcome": "PASS",
                            "summary": "Synthetic user lane completed.",
                            "evidence_refs": [str(response_path)],
                        }
                    ],
                    "notes": [],
                },
            )
            _write_text(response_path, "# AI-as-User\n\nSynthetic lane completed.\n")
            return 0
        if role == "reviewer":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "effect_reviews": [
                        {
                            "requirement_id": "REQ-001",
                            "verdict": "PASS",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Synthetic reviewer returned PASS with bounded-gap wording."],
                        }
                    ],
                },
            )
            _write_text(
                response_path,
                "PASS.\n\nThe bounded proof uses an explicit coefficient-error hypothesis, but the reviewer verdict remains PASS.\n",
            )
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "terminal_fail":
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nSynthetic checker completed.\n")
            return 0
        if role == "test_designer":
            _write_json(result_path, {"status": "OK", "notes": ["ordinary test completed"]})
            _write_text(response_path, "# Test Designer\n\nSynthetic lane completed.\n")
            return 0
        if role == "ai_user":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "operation_log_ref": "",
                    "effect_results": [
                        {
                            "requirement_id": "REQ-001",
                            "outcome": "FAIL",
                            "summary": "Synthetic user lane exposed a product-facing failure.",
                            "evidence_refs": [str(response_path)],
                        }
                    ],
                    "notes": [],
                },
            )
            _write_text(response_path, "# AI-as-User\n\nSynthetic lane reported a product-facing failure.\n")
            return 0
        if role == "reviewer":
            _write_json(
                result_path,
                {
                    "status": "OK",
                    "effect_reviews": [
                        {
                            "requirement_id": "REQ-001",
                            "verdict": "FAIL",
                            "evidence_refs": [str(response_path)],
                            "reproduction_steps": ["Synthetic reviewer confirmed a product-facing failure."],
                        }
                    ],
                },
            )
            _write_text(response_path, "VERDICT: FAIL\nsummary: synthetic reviewer confirmed a product-facing failure.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "response_then_fail":
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nResponse-then-fail checker completed.\n")
            return 0
        if role == "test_designer":
            _write_text(response_path, "RESPONSE THEN FAIL :: EU-001\n")
            time.sleep(1.5)
            sys.stderr.write("synthetic post-response failure after terminal artifact write\n")
            sys.stderr.flush()
            return 23
        if role == "ai_user":
            _write_text(response_path, "AI USER RAW OUTPUT :: EU-001\nresult=response-then-fail\n")
            return 0
        if role == "reviewer":
            _write_text(response_path, "VERDICT: PASS\nsummary: response-then-fail reviewer should stay unreachable.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario == "linger_after_response":
        if role == "checker":
            _write_json(result_path, _checker_single_requirement())
            _write_text(response_path, "# Checker\n\nLinger-after-response checker completed.\n")
            return 0
        if role == "test_designer":
            _write_text(response_path, "LINGER AFTER RESPONSE :: EU-001\n")
            time.sleep(5.0)
            return 0
        if role == "ai_user":
            _write_text(response_path, "AI USER RAW OUTPUT :: EU-001\nresult=linger-after-response\n")
            return 0
        if role == "reviewer":
            _write_text(response_path, "VERDICT: PASS\nsummary: linger-after-response reviewer completed.\n")
            return 0
        raise SystemExit(f"unexpected role: {role}")

    if args.scenario in {
        "simple_stdout_handoff",
        "recursive_reviewer",
        "runtime_closure_requirements",
        "runtime_closure_only_requirements",
    }:
        if role == "checker":
            if args.scenario == "runtime_closure_requirements":
                _write_json(result_path, _checker_runtime_closure_requirements())
            elif args.scenario == "runtime_closure_only_requirements":
                _write_json(result_path, _checker_runtime_closure_only_requirements())
            else:
                _write_json(result_path, _checker_simple_stdout_handoff())
            _write_text(response_path, "# Checker\n\nNormalized the final effects.\n")
            return 0
        if role == "test_designer":
            target_unit, requirements = _target_unit_and_requirements(context)
            unit_id = str(target_unit.get("unit_id") or "EU-UNKNOWN")
            requirement_ids = ",".join(str(item.get("requirement_id") or "") for item in requirements)
            sys.stdout.write(
                '{"role":"test_ai","unit_id":"'
                + unit_id
                + '","requirement_ids":"'
                + requirement_ids
                + '","note":"intentionally malformed final text"\n'
            )
            return 0
        if role == "ai_user":
            target_unit, requirements = _target_unit_and_requirements(context)
            unit_id = str(target_unit.get("unit_id") or "EU-UNKNOWN")
            requirement_ids = ",".join(str(item.get("requirement_id") or "") for item in requirements)
            sys.stdout.write(
                "AI USER RAW OUTPUT :: " + unit_id + "\n"
                "requirements=" + requirement_ids + "\n"
                "step=opened-app\n"
                "result=plain-text-only\n"
            )
            return 0
        if role == "reviewer":
            if args.scenario == "recursive_reviewer":
                nested_root = Path(context["workspace_root"]) / "nested_self_eval"
                nested_root.mkdir(parents=True, exist_ok=True)
                manual_path = nested_root / "product_manual.md"
                final_effects_path = nested_root / "final_effects.md"
                _write_text(manual_path, "# Nested Manual\n")
                _write_text(final_effects_path, "1. nested effect\n")
                nested_request = {
                    "schema": "loop_product.loop_evaluator_prototype_input",
                    "schema_version": "1.0.0",
                    "evaluation_id": "nested_self_eval",
                    "workspace_root": str(nested_root),
                    "output_root": str(nested_root / ".cache" / "loop_evaluator_self_eval" / "fixture_output"),
                    "product_manual_ref": str(manual_path),
                    "final_effects_text_ref": str(final_effects_path),
                    "role_requirements": {
                        "checker": "Normalize nested effects.",
                        "test_designer": "nested test ai",
                        "ai_user": "nested ai user",
                        "reviewer": "nested reviewer",
                    },
                    "agent_execution": {
                        "default": {
                            "agent_cmd": (
                                f"PYTHONPATH={shlex.quote(str(product_repo_root))} "
                                f"{shlex.quote(sys.executable)} -m loop_product.loop.fixture_role_agent "
                                "--scenario recursive_reviewer"
                            ),
                        }
                    },
                }
                from loop_product.evaluator.simple import run_evaluator_simple_prototype

                nested_report = run_evaluator_simple_prototype(request=nested_request)
                sys.stdout.write(json.dumps(nested_report, sort_keys=True) + "\n")
                if str(nested_report.get("status") or "") != "ERROR":
                    raise SystemExit(19)
                if "simple evaluator recursion detected" not in str(dict(nested_report.get("error") or {}).get("message") or ""):
                    raise SystemExit(23)
                raise SystemExit(17)
            for lane in context["lanes"]:
                target_unit = dict(lane.get("target_evaluation_unit") or {})
                unit_id = str(target_unit.get("unit_id") or "")
                requirements = [dict(item) for item in (target_unit.get("requirements") or [])]
                requirement_ids = ",".join(str(item.get("requirement_id") or "") for item in requirements)
                test_ai_raw = str(((lane.get("test_ai") or {}).get("raw_output_text")) or "")
                ai_user_raw = str(((lane.get("ai_user") or {}).get("raw_output_text")) or "")
                sys.stdout.write(
                    "REVIEWER SAW " + unit_id + "\n"
                    "REQUIREMENTS=" + requirement_ids + "\n"
                    "TEST_AI=" + repr(test_ai_raw) + "\n"
                    "AI_USER=" + repr(ai_user_raw) + "\n"
                )
            return 0
        raise SystemExit(f"unexpected role: {role}")

    raise SystemExit(f"unexpected scenario: {args.scenario}")


if __name__ == "__main__":
    raise SystemExit(main())
