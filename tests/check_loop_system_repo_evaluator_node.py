#!/usr/bin/env python3
"""Validate IF-3 evaluator-as-node runtime with repo-shipped deterministic role agents."""

from __future__ import annotations

import json
import inspect
import os
import shlex
import subprocess
import sys
import tempfile
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

from jsonschema import Draft202012Validator, validate


ROOT = Path(__file__).resolve().parents[1]


def _fixture_role_agent_cmd(*, scenario: str, failing_role: str | None = None, failure_message: str | None = None) -> str:
    parts = [
        shlex.quote(sys.executable),
        "-m",
        "loop_product.loop.fixture_role_agent",
        "--scenario",
        shlex.quote(scenario),
    ]
    if failing_role:
        parts.extend(["--failing-role", shlex.quote(failing_role)])
    if failure_message:
        parts.extend(["--failure-message", shlex.quote(failure_message)])
    return " ".join(parts)


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-node][FAIL] {msg}", file=sys.stderr)
    return 2


def _read_counter(path: Path) -> int:
    return int(path.read_text(encoding="utf-8").strip())


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def _write_stub_agent(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            from __future__ import annotations

            import json
            import os
            from pathlib import Path


            def _write_json(path: Path, obj: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\\n", encoding="utf-8")


            def _write_text(path: Path, text: str) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")


            def main() -> int:
                role = str(os.environ["LOOP_PRODUCT_EVAL_ROLE"])
                result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
                response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])
                operation_log_path = Path(os.environ.get("LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH", ""))
                context = json.loads(Path(os.environ["LOOP_PRODUCT_EVAL_CONTEXT_PATH"]).read_text(encoding="utf-8"))
                if role == "checker":
                    result = {
                        "status": "OK",
                        "normalized_requirements": [
                            {
                                "requirement_id": "REQ-001",
                                "description": "The endpoint clarification product stays runnable and documented.",
                                "blocking": True,
                            },
                            {
                                "requirement_id": "REQ-002",
                                "description": "The host adapter remains a host adapter rather than product core.",
                                "blocking": True,
                            },
                        ],
                        "requirement_graph": {
                            "requirements": [
                                {
                                    "requirement_id": "REQ-001",
                                    "description": "The endpoint clarification product stays runnable and documented.",
                                    "blocking": True,
                                },
                                {
                                    "requirement_id": "REQ-002",
                                    "description": "The host adapter remains a host adapter rather than product core.",
                                    "blocking": True,
                                },
                            ],
                            "evaluation_units": [{"unit_id": "EU-001", "requirement_ids": ["REQ-001", "REQ-002"]}],
                            "dependency_edges": [],
                        },
                        "repair_actions": [],
                        "human_gate_reasons": [],
                        "notes": ["checker grouped both requirements into one co-testable unit"],
                    }
                    _write_json(result_path, result)
                    _write_text(response_path, "# Checker\\n\\nStub checker completed.\\n")
                    return 0
                if role == "test_designer":
                    ordinary_root_raw = str(context.get("ordinary_test_artifact_root") or "").strip()
                    if ordinary_root_raw:
                        ordinary_root = Path(ordinary_root_raw)
                        stdout_ref = ordinary_root / "stdout.txt"
                        stderr_ref = ordinary_root / "stderr.txt"
                        exec_span_ref = ordinary_root / "exec_span.json"
                        _write_text(stdout_ref, "endpoint clarification help output\\n")
                        _write_text(stderr_ref, "")
                        _write_json(exec_span_ref, {"cwd": str(Path(context["workspace_root"]).resolve()), "exit_code": 0})
                        result = {
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
                    else:
                        result = {
                            "status": "OK",
                            "notes": ["simple evaluator delegated test_ai output stayed on the documented surface"],
                        }
                    _write_json(result_path, result)
                    _write_text(response_path, "# Test Designer\\n\\nStub ordinary tests completed.\\n")
                    return 0
                if role == "ai_user":
                    operation_log_raw = os.environ.get("LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH", "").strip()
                    operation_log_path = Path(operation_log_raw) if operation_log_raw else None
                    if operation_log_path is not None:
                        _write_text(
                            operation_log_path,
                            "1. Read the manual.\\n2. Ran the documented help surface.\\n3. Confirmed host-adapter boundary evidence.\\n",
                        )
                    evidence_ref = str(operation_log_path) if operation_log_path is not None else str(response_path)
                    result = {
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
                    _write_json(result_path, result)
                    _write_text(response_path, "# AI-as-User\\n\\nStub user run completed.\\n")
                    return 0
                if role == "reviewer":
                    result = {
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
                    _write_json(result_path, result)
                    _write_text(
                        response_path,
                        "VERDICT: PASS\\nsummary: stub reviewer completed with reviewer-visible evidence.\\n",
                    )
                    return 0
                raise SystemExit(f"unexpected role: {role}")


            if __name__ == "__main__":
                raise SystemExit(main())
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_fake_codex_cli(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            from __future__ import annotations

            import json
            import os
            import sys
            import time
            from pathlib import Path


            def _write_json(path: Path, obj: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\\n", encoding="utf-8")


            def _write_text(path: Path, text: str) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")


            def main() -> int:
                argv = sys.argv[1:]
                if not argv or argv[0] != "exec":
                    raise SystemExit(f"unexpected argv: {argv!r}")

                role = str(os.environ["LOOP_PRODUCT_EVAL_ROLE"]).strip()
                result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
                response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])
                operation_log_raw = os.environ.get("LOOP_PRODUCT_EVAL_OPERATION_LOG_PATH", "").strip()
                operation_log_path = Path(operation_log_raw) if operation_log_raw else None
                test_designer_mode = str(os.environ.get("FAKE_CODEX_TEST_DESIGNER_MODE") or "").strip()

                if role == "checker":
                    requirement = {
                        "requirement_id": "REQ-001",
                        "description": "Synthetic provider linger coverage.",
                        "blocking": True,
                        "requirement_kind": "product_effect",
                    }
                    _write_json(
                        result_path,
                        {
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
                        },
                    )
                    _write_text(response_path, "# Checker\\n\\nProvider linger checker completed.\\n")
                    sys.stderr.write("tokens used: 21\\n")
                    sys.stderr.flush()
                    return 0

                if role == "test_designer":
                    _write_text(response_path, "PROVIDER LINGER AFTER RESPONSE :: EU-001\\n")
                    sys.stderr.write("tokens used: 42\\n")
                    sys.stderr.flush()
                    if test_designer_mode == "linger_after_response":
                        time.sleep(5.0)
                    return 0

                if role == "ai_user":
                    if operation_log_path is not None:
                        _write_text(operation_log_path, "1. Ran the provider linger fixture.\\n")
                    _write_text(response_path, "AI USER RAW OUTPUT :: EU-001\\nresult=provider-linger\\n")
                    sys.stderr.write("tokens used: 43\\n")
                    sys.stderr.flush()
                    return 0

                if role == "reviewer":
                    _write_text(response_path, "VERDICT: PASS\\nsummary: provider linger reviewer completed.\\n")
                    sys.stderr.write("tokens used: 44\\n")
                    sys.stderr.flush()
                    return 0

                raise SystemExit(f"unexpected role: {role}")


            if __name__ == "__main__":
                raise SystemExit(main())
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_lane_failure_agent(
    path: Path,
    *,
    failing_role: str,
    failure_message: str,
) -> None:
    path.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env python3
            from __future__ import annotations

            import json
            import os
            import sys
            from pathlib import Path


            def _write_json(path: Path, obj: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\\n", encoding="utf-8")


            def _write_text(path: Path, text: str) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")


            def main() -> int:
                role = str(os.environ["LOOP_PRODUCT_EVAL_ROLE"])
                result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
                response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])
                if role == "{failing_role}":
                    print({failure_message!r}, file=sys.stderr)
                    return 3
                if role == "checker":
                    _write_json(
                        result_path,
                        {{
                            "status": "OK",
                            "normalized_requirements": [
                                {{
                                    "requirement_id": "REQ-001",
                                    "description": "Synthetic failure classification probe.",
                                    "blocking": True,
                                }}
                            ],
                            "requirement_graph": {{
                                "requirements": [
                                    {{
                                        "requirement_id": "REQ-001",
                                        "description": "Synthetic failure classification probe.",
                                        "blocking": True,
                                    }}
                                ],
                                "evaluation_units": [{{"unit_id": "EU-001", "requirement_ids": ["REQ-001"]}}],
                                "dependency_edges": [],
                            }},
                            "repair_actions": [],
                            "human_gate_reasons": [],
                            "notes": [],
                        }},
                    )
                    _write_text(response_path, "# Checker\\n\\nSynthetic checker completed.\\n")
                    return 0
                if role == "test_designer":
                    _write_json(result_path, {{"status": "OK", "notes": ["ordinary test completed"]}})
                    _write_text(response_path, "# Test Designer\\n\\nSynthetic lane completed.\\n")
                    return 0
                if role == "ai_user":
                    _write_json(
                        result_path,
                        {{
                            "status": "OK",
                            "operation_log_ref": "",
                            "effect_results": [
                                {{
                                    "requirement_id": "REQ-001",
                                    "outcome": "PASS",
                                    "summary": "Synthetic user lane completed.",
                                    "evidence_refs": [str(response_path)],
                                }}
                            ],
                            "notes": [],
                        }},
                    )
                    _write_text(response_path, "# AI-as-User\\n\\nSynthetic lane completed.\\n")
                    return 0
                if role == "reviewer":
                    _write_json(
                        result_path,
                        {{
                            "status": "OK",
                            "effect_reviews": [
                                {{
                                    "requirement_id": "REQ-001",
                                    "verdict": "PASS",
                                    "evidence_refs": [str(response_path)],
                                    "reproduction_steps": ["Synthetic reviewer completed."],
                                }}
                            ],
                        }},
                    )
                    _write_text(response_path, "VERDICT: PASS\\nsummary: synthetic reviewer completed.\\n")
                    return 0
                raise SystemExit(f"unexpected role: {{role}}")


            if __name__ == "__main__":
                raise SystemExit(main())
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _write_unknown_reviewer_agent(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            from __future__ import annotations

            import json
            import os
            from pathlib import Path


            def _write_json(path: Path, obj: object) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\\n", encoding="utf-8")


            def _write_text(path: Path, text: str) -> None:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text, encoding="utf-8")


            def main() -> int:
                role = str(os.environ["LOOP_PRODUCT_EVAL_ROLE"])
                result_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESULT_PATH"])
                response_path = Path(os.environ["LOOP_PRODUCT_EVAL_RESPONSE_PATH"])
                if role == "checker":
                    _write_json(
                        result_path,
                        {
                            "status": "OK",
                            "normalized_requirements": [
                                {
                                    "requirement_id": "REQ-001",
                                    "description": "Synthetic unknown-reviewer probe.",
                                    "blocking": True,
                                }
                            ],
                            "requirement_graph": {
                                "requirements": [
                                    {
                                        "requirement_id": "REQ-001",
                                        "description": "Synthetic unknown-reviewer probe.",
                                        "blocking": True,
                                    }
                                ],
                                "evaluation_units": [{"unit_id": "EU-001", "requirement_ids": ["REQ-001"]}],
                                "dependency_edges": [],
                            },
                            "repair_actions": [],
                            "human_gate_reasons": [],
                            "notes": [],
                        },
                    )
                    _write_text(response_path, "# Checker\\n\\nSynthetic checker completed.\\n")
                    return 0
                if role == "test_designer":
                    _write_json(result_path, {"status": "OK", "notes": ["ordinary test completed"]})
                    _write_text(response_path, "# Test Designer\\n\\nSynthetic lane completed.\\n")
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
                    _write_text(response_path, "# AI-as-User\\n\\nSynthetic lane completed.\\n")
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
                    _write_text(response_path, "VERDICT: UNKNOWN\\nsummary: synthetic reviewer returned unknown.\\n")
                    return 0
                raise SystemExit(f"unexpected role: {role}")


            if __name__ == "__main__":
                raise SystemExit(main())
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import loop_product.loop.evaluator_client as evaluator_client_module
        import loop_product.loop.runner as loop_runner_module
        import loop_product.runtime_paths as runtime_paths_module
        import loop_product.evaluator.prototype as evaluator_prototype_module
        from loop_product import run_evaluator
        from loop_product.evaluator.prototype import build_test_designer_prompt
        from loop_product.evaluator.prototype import run_evaluator_prototype_supervised
        from loop_product.evaluator.prototype import _structured_output_pre_result_error
        from loop_product.evaluator.prototype import _validate_requirement_graph
        from loop_product.evaluator.prototype import _validate_ai_user_effect_result
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import ensure_runtime_tree, load_kernel_state, persist_kernel_state
        from loop_product.loop.evaluator_client import (
            build_evaluator_submission_for_frozen_task,
            build_evaluator_submission_for_endpoint_clarification,
            run_evaluator_node,
            run_evaluator_node_until_terminal,
        )
        from loop_product.protocols.evaluator import EvaluatorNodeSubmission, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
    except Exception as exc:  # noqa: BLE001
        return _fail(f"loop system evaluator-node imports failed: {exc}")

    module_source = inspect.getsource(evaluator_client_module)
    runner_source = inspect.getsource(loop_runner_module)
    runtime_paths_source = inspect.getsource(runtime_paths_module)
    prototype_source = inspect.getsource(evaluator_prototype_module)
    run_node_source = inspect.getsource(evaluator_client_module.run_evaluator_node)
    if "run_evaluator(" not in run_node_source:
        return _fail("run_evaluator_node must use the formal evaluator alias after productization")
    if "run_evaluator_simple_prototype" in run_node_source:
        return _fail("run_evaluator_node must stop depending on the prototype-named evaluator entry")
    if "run_evaluator_prototype_supervised" in run_node_source:
        return _fail("run_evaluator_node must not keep depending on the legacy supervised evaluator wrapper")
    if "build_endpoint_clarification_part1_input" not in module_source:
        return _fail("evaluator client must use the formal endpoint-clarification request builder alias")
    if "build_endpoint_clarification_part1_prototype_input" in module_source:
        return _fail("evaluator client must stop importing the prototype-named endpoint request builder directly")
    for helper_name in ("product_package_root", "product_repo_root", "product_contract_path", "product_schema_path"):
        if f"def {helper_name}" not in runtime_paths_source:
            return _fail(f"runtime_paths must expose the shared layout helper `{helper_name}`")
    if "Path(__file__).resolve().parents[2]" in module_source or "Path(__file__).resolve().parents[2]" in runner_source:
        return _fail("evaluator-node and child-runner surfaces must not infer repo root directly from __file__")
    if "sys.path.insert(0, str(_ROOT))" in prototype_source:
        return _fail("evaluator compatibility surface must not patch sys.path to recover repo-root layout")

    try:
        submission_schema = _load_schema("LoopSystemEvaluatorSubmission.schema.json")
        evaluator_result_schema = _load_schema("LoopSystemEvaluatorResult.schema.json")
        evaluator_run_state_schema = _load_schema("LoopEvaluatorRunState.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"missing evaluator submission schema: {exc}")
    if "lineage_ref" not in list(submission_schema.get("required") or []):
        return _fail("evaluator submission schema must require lineage_ref for the structured node-lineage claim")
    if "lineage_ref" not in dict(submission_schema.get("properties") or {}):
        return _fail("evaluator submission schema must expose a lineage_ref property")
    result_verdict_enum = list(((evaluator_result_schema.get("properties") or {}).get("verdict") or {}).get("enum") or [])
    for required_verdict in ("STRUCTURED_EXCEPTION", "ERROR"):
        if required_verdict not in result_verdict_enum:
            return _fail(f"evaluator result schema must expose `{required_verdict}` as a fail-closed terminal outcome")

    with tempfile.TemporaryDirectory(prefix="loop_system_evaluator_node_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)

        ai_user_operation_log = temp_root / "ai_user_operation_log.md"
        ai_user_operation_log.write_text(
            textwrap.dedent(
                """\
                1. Read the manual.
                2. Attempted a probe that failed for evaluator-side reasons.
                9. Repaired the probe by using bare `python -c '...'` to import `loop_product`.
                10. Confirmed the documented repo-root surfaces.
                """
            ),
            encoding="utf-8",
        )
        ai_user_result_ref = temp_root / "roles" / "ai_user" / "EU-001" / "result.json"
        ai_user_result_ref.parent.mkdir(parents=True, exist_ok=True)
        ai_user_result_ref.write_text("{}", encoding="utf-8")
        try:
            validated_ai_user_result = _validate_ai_user_effect_result(
                result={
                    "status": "OK",
                    "operation_log_ref": str(ai_user_operation_log),
                    "effect_results": [
                        {
                            "requirement_id": "REQ-001",
                            "outcome": "PASS",
                            "summary": "Natural-language evidence refs should resolve against operation-log steps.",
                            "evidence_refs": ["python import probe from operation 9"],
                        }
                    ],
                },
                target_unit={"unit_id": "EU-001", "requirement_ids": ["REQ-001"]},
                result_ref=ai_user_result_ref,
                workspace_root=workspace_root,
                run_root=temp_root,
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(f"AI-as-User operation-log evidence ref normalization regressed: {exc}")
        normalized_evidence_refs = validated_ai_user_result["effect_results"][0]["evidence_refs"]
        expected_op_anchor = f"{ai_user_operation_log.resolve()}#op9"
        if normalized_evidence_refs != [expected_op_anchor]:
            return _fail(
                "AI-as-User natural-language evidence refs must normalize to the referenced operation-log step"
            )

        ordinary_prompt = build_test_designer_prompt(
            request={
                "role_requirements": {
                    "test_designer": "Design your own ordinary tests against the documented current product surface."
                }
            },
            manual_text="# Manual\n\nUse documented surfaces only.\n",
            final_effect_requirements=[
                {
                    "requirement_id": "REQ-001",
                    "description": "Documented smoke and import surfaces stay usable.",
                    "blocking": True,
                }
            ],
            result_path=temp_root / "roles" / "test_designer" / "result.json",
            response_path=temp_root / "roles" / "test_designer" / "response.md",
            ordinary_test_artifact_root=temp_root / "ordinary_tests",
        )
        required_prompt_clauses = (
            "If `uv run --project .` starts creating a virtual environment, building the project, or reaching for network access in a staged workspace, treat that as evaluator-side bootstrap noise and immediately switch to the already-working staged-workspace `python` surface.",
            "Put explanatory notes in your final JSON or summary, not into a shell command.",
            "Once one successful smoke-style probe and one successful API or import probe already cover the documented surface, stop exploring and finalize.",
            "Prefer bounded non-interactive probes over headed or long-lived interactive sessions.",
            "Any auxiliary browser, server, watcher, or helper process you start must be short-lived and cleaned up before you return.",
            "Once decisive evidence exists for the current evaluation unit, stop and return immediately instead of hunting for nicer evidence.",
            "Do not inspect implementation source files to decide what to test unless a documented command or public import stayed ambiguous after one decisive probe.",
            "When you need reviewer-visible serialization of public Python objects, prefer stable helpers such as `dataclasses.asdict`, an object's own `to_dict()`, or explicit field projection; do not assume `model_dump()` or `vars()` exist.",
        )
        for clause in required_prompt_clauses:
            if clause not in ordinary_prompt:
                return _fail("ordinary-test prompt must enforce staged-workspace self-repair and prompt-level convergence")

        frozen_submission = build_evaluator_submission_for_frozen_task(
            target_node=NodeSpec(
                node_id="frozen-task-node",
                node_kind="implementer",
                goal_slice="deliver one frozen task",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1",
                execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "danger-full-access"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_runtime_s": 3600},
                allowed_actions=["implement", "evaluate", "report"],
                workspace_root=str(workspace_root.resolve()),
                codex_home=str(workspace_root.resolve()),
                lineage_ref="lineage.json",
                delegation_ref="delegation.json",
                status=NodeStatus.ACTIVE,
            ),
            workspace_root=workspace_root,
            output_root=temp_root / "evaluator_submission_output",
            implementation_package_ref=ROOT / "loop_product" / "kernel" / "entry.py",
            product_manual_ref=ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
            final_effect_requirements=[
                {
                    "requirement_id": "REQ-001",
                    "description": "one frozen generic evaluator submission",
                    "blocking": True,
                }
            ],
        )
        default_cfg = dict(frozen_submission.agent_execution.get("default") or {})
        reviewer_cfg = dict(frozen_submission.agent_execution.get("reviewer") or {})
        if str(default_cfg.get("reasoning_effort") or "") != "high":
            return _fail("generic evaluator-node submissions must default delegated evaluator roles to high reasoning")
        if str(reviewer_cfg.get("reasoning_effort") or "") != "high":
            return _fail("generic evaluator-node submissions must keep reviewer on the same high default reasoning")

        repaired_graph = _validate_requirement_graph(
            {
                "requirements": [
                    {"requirement_id": "REQ-001", "description": "repo layout", "blocking": True},
                    {"requirement_id": "REQ-002", "description": "authority query", "blocking": True},
                    {"requirement_id": "REQ-003", "description": "audit view", "blocking": True},
                ],
                "evaluation_units": [
                    {"unit_id": "EU-001", "requirement_ids": ["REQ-001"]},
                    {"unit_id": "EU-002", "requirement_ids": ["REQ-002"]},
                    {"unit_id": "EU-003", "requirement_ids": ["REQ-002", "REQ-003"]},
                ],
                "dependency_edges": [
                    {"from_unit_id": "EU-001", "to_unit_id": "EU-002"},
                    {"from_unit_id": "EU-002", "to_unit_id": "EU-003"},
                ],
            },
            label="checker requirement_graph",
        )
        repaired_units = repaired_graph["evaluation_units"]
        expected_units = [
            {"unit_id": "EU-001", "requirement_ids": ["REQ-001"]},
            {"unit_id": "EU-002", "requirement_ids": ["REQ-002"]},
            {"unit_id": "EU-003", "requirement_ids": ["REQ-003"]},
        ]
        if repaired_units != expected_units:
            return _fail("requirement-graph validation must deterministically repair later duplicate requirement assignments")

        fixture_agent_cmd = _fixture_role_agent_cmd(scenario="endpoint_pass")

        root_node = NodeSpec(
            node_id="root-kernel",
            node_kind="kernel",
            goal_slice="supervise evaluator-as-node integration",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/evaluator_node_summary.json",
            lineage_ref="root-kernel",
            status=NodeStatus.ACTIVE,
        )
        child_node = NodeSpec(
            node_id="child-implementer-001",
            node_kind="implementer",
            goal_slice="validate endpoint clarification section acceptance surface",
            parent_node_id=root_node.node_id,
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["evaluate", "report"],
            delegation_ref="state/delegations/child-implementer-001.json",
            result_sink_ref="artifacts/evaluator_node_summary.json",
            lineage_ref="root-kernel->child-implementer-001",
            status=NodeStatus.ACTIVE,
        )
        from loop_product.kernel.state import KernelState

        kernel_state = KernelState(
            task_id="wave3-evaluator-node",
            root_goal="exercise evaluator-as-node integration",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

        submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=child_node,
            workspace_root=ROOT,
            output_root=state_root / "artifacts" / "evaluator_node_runs",
            role_agent_cmd=fixture_agent_cmd,
        )
        if submission.to_dict().get("lineage_ref") != child_node.lineage_ref:
            return _fail("evaluator submissions must preserve the target node lineage_ref")
        validate(submission.to_dict(), submission_schema)
        for role_id, role_cfg in submission.agent_execution.items():
            if str(role_cfg.get("agent_cmd") or "") != fixture_agent_cmd:
                return _fail(f"explicit role_agent_cmd must override {role_id} agent execution")
            if any(str(role_cfg.get(key) or "").strip() for key in ("agent_provider", "agent_profile")):
                return _fail(f"explicit role_agent_cmd must remove provider/profile from {role_id} agent execution")

        evaluator_result, runtime_refs = run_evaluator_node(
            state_root=state_root,
            submission=submission,
        )

        if evaluator_result.verdict is not EvaluatorVerdict.PASS:
            return _fail(f"deterministic evaluator node should PASS, got {evaluator_result.verdict.value}")
        if evaluator_result.lane != "reviewer":
            return _fail("translated evaluator verdict lane must be `reviewer`")
        if not evaluator_result.evidence_refs:
            return _fail("translated evaluator verdict must expose reviewer-visible evidence refs")

        request_ref = Path(str(runtime_refs["request_ref"]))
        evaluation_ref = Path(str(runtime_refs["evaluation_report_ref"]))
        if "supervisor_report_ref" in runtime_refs:
            return _fail("simple-evaluator node runtime must not expose a legacy supervisor_report_ref")
        for path in (request_ref, evaluation_ref):
            if not path.exists():
                return _fail(f"evaluator node runtime must materialize {path.name}")
        submission_artifact = json.loads((request_ref.parent / "EvaluatorNodeSubmission.json").read_text(encoding="utf-8"))
        if submission_artifact.get("lineage_ref") != child_node.lineage_ref:
            return _fail("materialized evaluator submission artifacts must preserve lineage_ref")

        loaded_state = load_kernel_state(state_root)
        accepted_evaluator_envelope = next(
            (
                item
                for item in loaded_state.accepted_envelopes
                if item.get("source") == submission.evaluator_node_id and item.get("envelope_type") == "evaluator_result"
            ),
            None,
        )
        if accepted_evaluator_envelope is None:
            return _fail("run_evaluator_node must transport evaluator results through the gateway into accepted authoritative state")
        if not any(
            lane.get("node_id") == submission.evaluator_node_id and lane.get("verdict") == "PASS"
            for lane in loaded_state.active_evaluator_lanes
        ):
            return _fail("run_evaluator_node must record the accepted evaluator lane for the evaluator node")

        evaluator_run_root = evaluation_ref.parent
        expected_role_results = (
            evaluator_run_root / ".loop" / "checker" / "runs" / "checker" / "result.json",
            evaluator_run_root / ".loop" / "checker" / "runs" / "checker" / "exec_span.json",
            evaluator_run_root / ".loop" / "test_ai" / "runs" / "test_ai__EU-001" / "result.json",
            evaluator_run_root / ".loop" / "test_ai" / "runs" / "test_ai__EU-001" / "exec_span.json",
            evaluator_run_root / ".loop" / "ai_user" / "runs" / "ai_user__EU-001" / "result.json",
            evaluator_run_root / ".loop" / "ai_user" / "runs" / "ai_user__EU-001" / "exec_span.json",
            evaluator_run_root / ".loop" / "reviewer" / "runs" / "reviewer" / "response.txt",
            evaluator_run_root / ".loop" / "reviewer" / "runs" / "reviewer" / "exec_span.json",
        )
        for path in expected_role_results:
            if not path.exists():
                return _fail(f"evaluator node runtime must materialize lane artifact {path}")
        expected_role_prompts = {
            evaluator_run_root / ".loop" / "checker" / "runs" / "checker" / "invocation.json": (
                "# LOOP Evaluator Prototype: Checker",
                "Use only the final-effects text in this prompt; do not inspect prior runs, prior checker outputs, or unrelated workspace files.",
            ),
            evaluator_run_root / ".loop" / "test_ai" / "runs" / "test_ai__EU-001" / "invocation.json": (
                "# LOOP Evaluator Simple Prototype: Test AI",
                "Do not ask for confirmation, permission, or a follow-up choice such as `Proceed now?`",
            ),
            evaluator_run_root / ".loop" / "ai_user" / "runs" / "ai_user__EU-001" / "invocation.json": (
                "# LOOP Evaluator Simple Prototype: AI-as-User",
                "Use the product from its documented surface first.",
            ),
            evaluator_run_root / ".loop" / "reviewer" / "runs" / "reviewer" / "invocation.json": (
                "# LOOP Evaluator Simple Prototype: Reviewer",
                "Read the checker output and the delegated raw lane outputs.",
            ),
        }
        for path, needles in expected_role_prompts.items():
            invocation_obj = json.loads(path.read_text(encoding="utf-8"))
            prompt_ref = Path(str(invocation_obj.get("prompt_ref") or ""))
            if not prompt_ref.exists():
                return _fail(f"evaluator node runtime must materialize role prompt referenced by {path}")
            text = prompt_ref.read_text(encoding="utf-8")
            for needle in needles:
                if needle not in text:
                    return _fail(f"evaluator node runtime must preserve role guidance {needle!r} in prompt {prompt_ref}")
        for path in (
            evaluator_run_root / ".loop" / "checker" / "config.toml",
            evaluator_run_root / ".loop" / "test_ai" / "config.toml",
            evaluator_run_root / ".loop" / "ai_user" / "config.toml",
            evaluator_run_root / ".loop" / "reviewer" / "config.toml",
        ):
            if path.exists():
                return _fail(f"evaluator node runtime must not copy host config.toml wholesale into role home {path}")
        for path in (
            evaluator_run_root / ".loop" / "checker" / "AGENTS.md",
            evaluator_run_root / ".loop" / "checker" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "checker" / "workspaces" / "checker" / "workspace" / "AGENTS.md",
            evaluator_run_root / ".loop" / "checker" / "workspaces" / "checker" / "workspace" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "test_ai" / "AGENTS.md",
            evaluator_run_root / ".loop" / "test_ai" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "test_ai" / "workspaces" / "test_ai__EU-001" / "workspace" / "AGENTS.md",
            evaluator_run_root / ".loop" / "test_ai" / "workspaces" / "test_ai__EU-001" / "workspace" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "ai_user" / "AGENTS.md",
            evaluator_run_root / ".loop" / "ai_user" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "ai_user" / "workspaces" / "ai_user__EU-001" / "workspace" / "AGENTS.md",
            evaluator_run_root / ".loop" / "ai_user" / "workspaces" / "ai_user__EU-001" / "workspace" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "reviewer" / "AGENTS.md",
            evaluator_run_root / ".loop" / "reviewer" / "AGENTS.override.md",
            evaluator_run_root / ".loop" / "reviewer" / "workspaces" / "reviewer" / "workspace" / "AGENTS.md",
            evaluator_run_root / ".loop" / "reviewer" / "workspaces" / "reviewer" / "workspace" / "AGENTS.override.md",
        ):
            if path.exists():
                return _fail(f"evaluator node runtime must not create runtime AGENTS file {path}")

        fake_codex_root = temp_root / "fake_codex_bin"
        fake_codex_root.mkdir(parents=True, exist_ok=True)
        fake_codex = fake_codex_root / "codex"
        _write_fake_codex_cli(fake_codex)
        lingering_submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=child_node,
            workspace_root=ROOT,
            output_root=state_root / "artifacts" / "evaluator_node_lingering_runs",
        )
        previous_path = str(os.environ.get("PATH") or "")
        os.environ["PATH"] = str(fake_codex_root) + os.pathsep + previous_path if previous_path else str(fake_codex_root)
        os.environ["FAKE_CODEX_TEST_DESIGNER_MODE"] = "linger_after_response"
        lingering_started = time.monotonic()
        try:
            lingering_result, lingering_refs = run_evaluator_node(
                state_root=state_root,
                submission=lingering_submission,
            )
        finally:
            os.environ["PATH"] = previous_path
            os.environ.pop("FAKE_CODEX_TEST_DESIGNER_MODE", None)
        lingering_elapsed_s = time.monotonic() - lingering_started
        if lingering_result.verdict is not EvaluatorVerdict.PASS:
            return _fail("lingering delegated-response fixture must still translate to a terminal PASS")
        if lingering_elapsed_s >= 3.0:
            return _fail("evaluator node runtime must not wait for the full lingering provider exit after a delegated terminal response artifact already exists")
        lingering_report = json.loads(Path(str(lingering_refs["evaluation_report_ref"])).read_text(encoding="utf-8"))
        lingering_lanes = list(lingering_report.get("lanes") or [])
        if len(lingering_lanes) != 1:
            return _fail("lingering delegated-response fixture must preserve the single evaluation-unit lane")
        lingering_test_ai = dict(lingering_lanes[0].get("test_ai") or {})
        if str(lingering_test_ai.get("raw_output_source") or "") != "response_path":
            return _fail("evaluator node runtime must preserve that the lingering delegated role closed through its response artifact")
        if "PROVIDER LINGER AFTER RESPONSE" not in str(lingering_test_ai.get("raw_output_text") or ""):
            return _fail("evaluator node runtime must preserve the terminal response artifact written before the natural provider exit")
        lingering_exec_span = json.loads(Path(str(lingering_test_ai.get("exec_span_ref") or "")).read_text(encoding="utf-8"))
        if int(lingering_exec_span.get("duration_ms") or 0) >= 3000:
            return _fail("evaluator node delegated-role exec span must settle before the natural linger interval expires")
        if lingering_exec_span.get("terminal_success") is not True:
            return _fail("provider-backed lingering evaluator node lanes must record terminal_success in exec-span evidence")

        pass_with_error_words_submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=child_node,
            workspace_root=ROOT,
            output_root=state_root / "artifacts" / "evaluator_node_pass_with_error_words_runs",
            role_agent_cmd=_fixture_role_agent_cmd(scenario="pass_with_error_words"),
        )
        pass_with_error_words_result, pass_with_error_words_refs = run_evaluator_node(
            state_root=state_root,
            submission=pass_with_error_words_submission,
        )
        if pass_with_error_words_result.verdict is not EvaluatorVerdict.PASS:
            return _fail(
                "reviewer text that starts with PASS must not be downgraded to ERROR just because later prose mentions coefficient-error"
            )
        pass_with_error_words_impl_ref = Path(
            str(
                pass_with_error_words_refs.get("workspace_implementer_result_ref")
                or pass_with_error_words_refs.get("implementer_result_ref")
                or ""
            )
        )
        if not pass_with_error_words_impl_ref.exists():
            return _fail("terminal PASS-with-error-words fixture must materialize an authoritative implementer_result")
        pass_with_error_words_impl = json.loads(pass_with_error_words_impl_ref.read_text(encoding="utf-8"))
        if str(((pass_with_error_words_impl.get("evaluator_result") or {}).get("verdict")) or "") != "PASS":
            return _fail(
                "authoritative implementer_result must preserve PASS when reviewer prose contains bounded-gap phrases like coefficient-error"
            )

        response_then_fail_submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=child_node,
            workspace_root=ROOT,
            output_root=state_root / "artifacts" / "evaluator_node_response_then_fail_runs",
            role_agent_cmd=_fixture_role_agent_cmd(scenario="response_then_fail"),
        )
        response_then_fail_result, response_then_fail_refs = run_evaluator_node(
            state_root=state_root,
            submission=response_then_fail_submission,
        )
        if response_then_fail_result.verdict is not EvaluatorVerdict.ERROR:
            return _fail("delegated roles that fail after writing a terminal response artifact must translate to ERROR, not PASS")
        response_then_fail_report = json.loads(
            Path(str(response_then_fail_refs["evaluation_report_ref"])).read_text(encoding="utf-8")
        )
        if str(response_then_fail_report.get("status") or "") != "ERROR":
            return _fail("response-then-fail evaluator reports must stay terminal ERROR")

        retryable_fail_state_root = temp_root / ".loop" / "retryable_fail"
        ensure_runtime_tree(retryable_fail_state_root)
        retryable_fail_root_node = NodeSpec(
            node_id="retryable-fail-root",
            node_kind="kernel",
            goal_slice="exercise retryable evaluator FAIL closeout semantics",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/retryable_fail/root_summary.json",
            lineage_ref="retryable-fail-root",
            status=NodeStatus.ACTIVE,
        )
        retryable_fail_child_node = NodeSpec(
            node_id="retryable-fail-child",
            node_kind="implementer",
            goal_slice="validate retryable evaluator FAIL stays non-terminal",
            parent_node_id=retryable_fail_root_node.node_id,
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report"],
            delegation_ref="state/delegations/retryable-fail-child.json",
            result_sink_ref="artifacts/retryable_fail/implementer_result.json",
            lineage_ref="retryable-fail-root->retryable-fail-child",
            status=NodeStatus.ACTIVE,
        )
        retryable_fail_kernel_state = KernelState(
            task_id="retryable-fail-evaluator-node",
            root_goal="exercise retryable FAIL evaluator translation",
            root_node_id=retryable_fail_root_node.node_id,
        )
        retryable_fail_kernel_state.register_node(retryable_fail_root_node)
        retryable_fail_kernel_state.register_node(retryable_fail_child_node)
        persist_kernel_state(
            retryable_fail_state_root,
            retryable_fail_kernel_state,
            authority=kernel_internal_authority(),
        )
        retryable_fail_submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=retryable_fail_child_node,
            workspace_root=ROOT,
            output_root=retryable_fail_state_root / "artifacts" / "evaluator_node_retryable_fail_runs",
            role_agent_cmd=_fixture_role_agent_cmd(scenario="terminal_fail"),
        )
        retryable_fail_result, retryable_fail_refs = run_evaluator_node(
            state_root=retryable_fail_state_root,
            submission=retryable_fail_submission,
        )
        if retryable_fail_result.verdict is not EvaluatorVerdict.FAIL:
            return _fail("terminal-fail fixture must translate to FAIL")
        if retryable_fail_result.retryable is not True:
            return _fail("reviewer FAIL must remain retryable repair work by default")
        retryable_fail_impl_ref = Path(
            str(
                retryable_fail_refs.get("workspace_implementer_result_ref")
                or retryable_fail_refs.get("implementer_result_ref")
                or ""
            )
        )
        if not retryable_fail_impl_ref.exists():
            return _fail("retryable FAIL must still materialize an authoritative implementer_result")
        retryable_fail_impl = json.loads(retryable_fail_impl_ref.read_text(encoding="utf-8"))
        if str(retryable_fail_impl.get("outcome") or "") != "REPAIR_REQUIRED":
            return _fail("retryable FAIL implementer_result must preserve REPAIR_REQUIRED outcome")
        retryable_fail_state = load_kernel_state(retryable_fail_state_root)
        accepted_retryable_fail = [
            item
            for item in retryable_fail_state.accepted_envelopes
            if item.get("source") == retryable_fail_submission.evaluator_node_id
        ]
        if not any(item.get("envelope_type") == "evaluator_result" for item in accepted_retryable_fail):
            return _fail("retryable FAIL must still submit the evaluator_result envelope")
        if any(item.get("envelope_type") == "node_terminal_result" for item in accepted_retryable_fail):
            return _fail("retryable FAIL must not emit node_terminal_result before repair is attempted")
        retryable_fail_node = dict(retryable_fail_state.nodes.get(retryable_fail_child_node.node_id) or {})
        if str(retryable_fail_node.get("status") or "") != NodeStatus.ACTIVE.value:
            return _fail("retryable FAIL must leave the implementer node ACTIVE for repair work")

        whole_paper_gate_state_root = temp_root / ".loop" / "whole_paper_gate"
        whole_paper_gate_workspace_root = temp_root / "whole_paper_gate_workspace"
        whole_paper_gate_artifact_root = whole_paper_gate_workspace_root / "deliverables" / "primary_artifact"
        whole_paper_gate_artifact_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(whole_paper_gate_state_root)
        whole_paper_gate_root_node = NodeSpec(
            node_id="whole-paper-gate-root",
            node_kind="kernel",
            goal_slice="exercise whole-paper evaluator preflight gate",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/whole_paper_gate/root_summary.json",
            lineage_ref="whole-paper-gate-root",
            status=NodeStatus.ACTIVE,
        )
        whole_paper_gate_child_node = NodeSpec(
            node_id="whole-paper-gate-child",
            node_kind="implementer",
            goal_slice="ensure incomplete whole-paper artifacts cannot start evaluator",
            parent_node_id=whole_paper_gate_root_node.node_id,
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report", "split_request"],
            delegation_ref="state/delegations/whole-paper-gate-child.json",
            result_sink_ref="artifacts/whole_paper_gate/implementer_result.json",
            lineage_ref="whole-paper-gate-root->whole-paper-gate-child",
            status=NodeStatus.ACTIVE,
        )
        whole_paper_gate_kernel_state = KernelState(
            task_id="whole-paper-evaluator-gate",
            root_goal="prevent premature whole-paper evaluator attempts",
            root_node_id=whole_paper_gate_root_node.node_id,
        )
        whole_paper_gate_kernel_state.register_node(whole_paper_gate_root_node)
        whole_paper_gate_kernel_state.register_node(whole_paper_gate_child_node)
        persist_kernel_state(
            whole_paper_gate_state_root,
            whole_paper_gate_kernel_state,
            authority=kernel_internal_authority(),
        )
        (whole_paper_gate_artifact_root / "README.md").write_text("# README\n", encoding="utf-8")
        (whole_paper_gate_artifact_root / "TRACEABILITY.md").write_text(
            "# Traceability\n\nThis artifact does not yet justify any terminal whole-paper result.\n",
            encoding="utf-8",
        )
        (whole_paper_gate_artifact_root / "EXTRACTION_REPORT.md").write_text("# Extraction\n", encoding="utf-8")
        (whole_paper_gate_artifact_root / "PARTITION_PLAN.md").write_text(
            "# Partition\n\nSplit recommendation recorded only in prose.\n",
            encoding="utf-8",
        )
        manual_ref = whole_paper_gate_workspace_root / "PRODUCT_MANUAL.md"
        manual_ref.write_text("# Manual\n\nWhole-paper benchmark.\n", encoding="utf-8")
        final_effects_ref = whole_paper_gate_workspace_root / "FINAL_EFFECTS.md"
        final_effects_ref.write_text(
            "\n".join(
                [
                    "# Final Effects",
                    "",
                    "- whole-paper faithful complete formalization",
                    "- paper defect exposed",
                    "- external dependency blocked",
                    "- intermediate block success is not whole-paper final success",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        whole_paper_gate_submission = build_evaluator_submission_for_frozen_task(
            target_node=whole_paper_gate_child_node,
            workspace_root=whole_paper_gate_workspace_root,
            output_root=whole_paper_gate_state_root / "artifacts" / "evaluator_node_whole_paper_gate_runs",
            implementation_package_ref=whole_paper_gate_artifact_root,
            product_manual_ref=manual_ref,
            final_effects_text_ref=final_effects_ref,
            role_agent_cmd=_fixture_role_agent_cmd(scenario="wave_sequence"),
        )
        try:
            run_evaluator_node_until_terminal(
                state_root=whole_paper_gate_state_root,
                submission=whole_paper_gate_submission,
                max_same_run_recovery_attempts=0,
            )
        except ValueError as exc:
            msg = str(exc)
            if "WHOLE_PAPER_STATUS.json" not in msg or "whole-paper" not in msg:
                return _fail("whole-paper evaluator preflight rejection must explain the missing structured terminal status evidence")
        else:
            return _fail("whole-paper evaluator attempts must fail closed before evaluator launch when terminal status evidence is missing")

        whole_paper_dependency_gate_state_root = temp_root / ".loop" / "whole_paper_dependency_gate"
        whole_paper_dependency_gate_workspace_root = temp_root / "whole_paper_dependency_gate_workspace"
        whole_paper_dependency_gate_workspace_root.mkdir(parents=True, exist_ok=True)
        whole_paper_dependency_gate_artifact_root = whole_paper_dependency_gate_workspace_root / "deliverables" / "primary_artifact"
        whole_paper_dependency_gate_artifact_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(whole_paper_dependency_gate_state_root)
        whole_paper_dependency_gate_root_node = NodeSpec(
            node_id="whole-paper-dependency-gate-root",
            node_kind="kernel",
            goal_slice="exercise whole-paper dependency gate",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/whole_paper_dependency_gate/root_summary.json",
            lineage_ref="whole-paper-dependency-gate-root",
            status=NodeStatus.ACTIVE,
        )
        linear_dep_node = NodeSpec(
            node_id="whole-paper-linear-dep",
            node_kind="implementer",
            goal_slice="close the linear chain dependency",
            parent_node_id=whole_paper_dependency_gate_root_node.node_id,
            generation=1,
            round_id="R1.linear",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["implement", "report"],
            delegation_ref="state/delegations/whole-paper-linear-dep.json",
            result_sink_ref="artifacts/whole_paper_linear_dep/result.json",
            lineage_ref="whole-paper-dependency-gate-root->whole-paper-linear-dep",
            status=NodeStatus.ACTIVE,
        )
        final_integration_node = NodeSpec(
            node_id="whole-paper-final-integration",
            node_kind="implementer",
            goal_slice="final whole-paper integration after dependency closure",
            parent_node_id=whole_paper_dependency_gate_root_node.node_id,
            generation=1,
            round_id="R1.final",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["evaluate", "report"],
            depends_on_node_ids=[linear_dep_node.node_id],
            delegation_ref="state/delegations/whole-paper-final-integration.json",
            result_sink_ref="artifacts/whole_paper_final_integration/implementer_result.json",
            lineage_ref="whole-paper-dependency-gate-root->whole-paper-final-integration",
            status=NodeStatus.ACTIVE,
        )
        whole_paper_dependency_gate_kernel_state = KernelState(
            task_id="whole-paper-dependency-gate-evaluator-node",
            root_goal="reject whole-paper evaluator launch until dependency nodes reach terminal-ready outcomes",
            root_node_id=whole_paper_dependency_gate_root_node.node_id,
        )
        whole_paper_dependency_gate_kernel_state.register_node(whole_paper_dependency_gate_root_node)
        whole_paper_dependency_gate_kernel_state.register_node(linear_dep_node)
        whole_paper_dependency_gate_kernel_state.register_node(final_integration_node)
        persist_kernel_state(
            whole_paper_dependency_gate_state_root,
            whole_paper_dependency_gate_kernel_state,
            authority=kernel_internal_authority(),
        )
        (whole_paper_dependency_gate_artifact_root / "README.md").write_text("# README\n", encoding="utf-8")
        (whole_paper_dependency_gate_artifact_root / "TRACEABILITY.md").write_text("# Traceability\n", encoding="utf-8")
        (whole_paper_dependency_gate_artifact_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "status": "TERMINAL",
                    "terminal_classification": "paper defect exposed",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        linear_dep_result_ref = whole_paper_dependency_gate_state_root / "artifacts" / "whole_paper_linear_dep" / "result.json"
        linear_dep_result_ref.parent.mkdir(parents=True, exist_ok=True)
        linear_dep_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.child_result",
                    "node_id": linear_dep_node.node_id,
                    "status": "BLOCKED",
                    "outcome": "BLOCKED",
                    "summary": "dependency has not reached a terminal-consumable outcome yet",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        dependency_gate_manual_ref = whole_paper_dependency_gate_workspace_root / "PRODUCT_MANUAL.md"
        dependency_gate_manual_ref.write_text("# Manual\n\nWhole-paper final integration.\n", encoding="utf-8")
        dependency_gate_final_effects_ref = whole_paper_dependency_gate_workspace_root / "FINAL_EFFECTS.md"
        dependency_gate_final_effects_ref.write_text(
            "\n".join(
                [
                    "# Final Effects",
                    "",
                    "- whole-paper faithful complete formalization",
                    "- paper defect exposed",
                    "- external dependency blocked",
                    "- final integration may launch evaluator only after dependency nodes close to terminal-ready outcomes",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        whole_paper_dependency_gate_submission = build_evaluator_submission_for_frozen_task(
            target_node=final_integration_node,
            workspace_root=whole_paper_dependency_gate_workspace_root,
            output_root=whole_paper_dependency_gate_state_root / "artifacts" / "evaluator_node_whole_paper_dependency_gate_runs",
            implementation_package_ref=whole_paper_dependency_gate_artifact_root,
            product_manual_ref=dependency_gate_manual_ref,
            final_effects_text_ref=dependency_gate_final_effects_ref,
            role_agent_cmd=_fixture_role_agent_cmd(scenario="wave_sequence"),
        )
        try:
            run_evaluator_node_until_terminal(
                state_root=whole_paper_dependency_gate_state_root,
                submission=whole_paper_dependency_gate_submission,
                max_same_run_recovery_attempts=0,
            )
        except ValueError as exc:
            msg = str(exc)
            if "dependency" not in msg.lower() or linear_dep_node.node_id not in msg:
                return _fail("whole-paper evaluator dependency gate must explain which declared dependency is not terminal-ready")
        else:
            return _fail("whole-paper final integration must fail closed before evaluator launch when dependency nodes are not terminal-ready")

        split_child_required_output_gate_state_root = temp_root / ".loop" / "split_child_required_output_gate"
        split_child_required_output_gate_workspace_root = temp_root / "split_child_required_output_gate_workspace"
        split_child_required_output_gate_artifact_root = (
            split_child_required_output_gate_workspace_root / "deliverables" / "primary_artifact"
        )
        (split_child_required_output_gate_artifact_root / "PARTITION").mkdir(parents=True, exist_ok=True)
        (split_child_required_output_gate_artifact_root / "formalization").mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(split_child_required_output_gate_state_root)
        split_child_gate_root_node = NodeSpec(
            node_id="split-child-gate-root",
            node_kind="kernel",
            goal_slice="exercise split-child required output evaluator gate",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/split_child_gate/root_summary.json",
            lineage_ref="split-child-gate-root",
            status=NodeStatus.ACTIVE,
        )
        split_child_gate_node = NodeSpec(
            node_id="split-child-gate-appendix",
            node_kind="implementer",
            goal_slice="formalize appendix support slice faithfully and only report terminal status after required outputs exist",
            parent_node_id=split_child_gate_root_node.node_id,
            generation=1,
            round_id="R1.S1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report", "split_request"],
            delegation_ref="state/delegations/split-child-gate-appendix.json",
            result_sink_ref="artifacts/split_child_gate/implementer_result.json",
            lineage_ref="split-child-gate-root->split-child-gate-appendix",
            status=NodeStatus.ACTIVE,
        )
        split_child_gate_kernel_state = KernelState(
            task_id="split-child-required-output-gate",
            root_goal="reject split-child evaluator launch when declared required outputs are still missing",
            root_node_id=split_child_gate_root_node.node_id,
        )
        split_child_gate_kernel_state.register_node(split_child_gate_root_node)
        split_child_gate_kernel_state.register_node(split_child_gate_node)
        persist_kernel_state(
            split_child_required_output_gate_state_root,
            split_child_gate_kernel_state,
            authority=kernel_internal_authority(),
        )
        (split_child_required_output_gate_artifact_root / "README.md").write_text("# README\n", encoding="utf-8")
        (split_child_required_output_gate_artifact_root / "TRACEABILITY.md").write_text("# Traceability\n", encoding="utf-8")
        (
            split_child_required_output_gate_artifact_root / "formalization" / "appendix_claim_inventory.json"
        ).write_text("{\"count\": 22}\n", encoding="utf-8")
        (
            split_child_required_output_gate_artifact_root / "PARTITION" / "external_dependency_candidates.json"
        ).write_text("[]\n", encoding="utf-8")
        (
            split_child_required_output_gate_artifact_root / "PARTITION" / "partition_plan.json"
        ).write_text(
            json.dumps(
                {
                    "blocks": [
                        {
                            "block_id": "appendix_support_and_proofs",
                            "owner": split_child_gate_node.node_id,
                            "required_outputs": [
                                "formalization/appendix_claim_inventory.json",
                                "formalization/appendix_claims.lean",
                                "PARTITION/external_dependency_candidates.json",
                                "WHOLE_PAPER_STATUS.json",
                            ],
                        }
                    ]
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        (split_child_required_output_gate_artifact_root / "WHOLE_PAPER_STATUS.json").write_text(
            json.dumps(
                {
                    "status": "TERMINAL",
                    "terminal_classification": "external dependency blocked",
                    "implementation_progress": {
                        "appendix_claim_inventory": "complete",
                        "lean_formalization": "not_started_due_to_external_block",
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        split_child_gate_manual_ref = split_child_required_output_gate_workspace_root / "PRODUCT_MANUAL.md"
        split_child_gate_manual_ref.write_text("# Manual\n\nSplit child appendix lane.\n", encoding="utf-8")
        split_child_gate_final_effects_ref = split_child_required_output_gate_workspace_root / "FINAL_EFFECTS.md"
        split_child_gate_final_effects_ref.write_text(
            "\n".join(
                [
                    "# Final Effects",
                    "",
                    "- whole-paper faithful complete formalization",
                    "- paper defect exposed",
                    "- external dependency blocked",
                    "- split child lanes may not enter evaluator before their declared required outputs are satisfied",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        split_child_gate_submission = build_evaluator_submission_for_frozen_task(
            target_node=split_child_gate_node,
            workspace_root=split_child_required_output_gate_workspace_root,
            output_root=split_child_required_output_gate_state_root / "artifacts" / "evaluator_split_child_gate_runs",
            implementation_package_ref=split_child_required_output_gate_artifact_root,
            product_manual_ref=split_child_gate_manual_ref,
            final_effects_text_ref=split_child_gate_final_effects_ref,
            role_agent_cmd=_fixture_role_agent_cmd(scenario="wave_sequence"),
        )
        try:
            run_evaluator_node_until_terminal(
                state_root=split_child_required_output_gate_state_root,
                submission=split_child_gate_submission,
                max_same_run_recovery_attempts=0,
            )
        except ValueError as exc:
            msg = str(exc)
            if "required output" not in msg.lower() or "appendix_claims.lean" not in msg:
                return _fail("split-child evaluator preflight rejection must explain which required output is still missing")
        else:
            return _fail("split-child evaluator launch must fail closed when declared required outputs are still missing")

        runtime_closure_only_state_root = temp_root / ".loop" / "runtime_closure_only"
        runtime_closure_only_workspace_root = temp_root / "runtime_closure_only_workspace"
        runtime_closure_only_workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(runtime_closure_only_state_root)
        runtime_closure_only_root_node = NodeSpec(
            node_id="runtime-closure-root",
            node_kind="kernel",
            goal_slice="exercise runtime-closure-only evaluator node closure",
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy={"mode": "kernel"},
            reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/runtime_closure_only/root_summary.json",
            lineage_ref="runtime-closure-root",
            status=NodeStatus.ACTIVE,
        )
        runtime_closure_only_child_node = NodeSpec(
            node_id="runtime-closure-child",
            node_kind="implementer",
            goal_slice="runtime_closure-only evaluator completion",
            parent_node_id=runtime_closure_only_root_node.node_id,
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 1},
            allowed_actions=["evaluate", "report"],
            delegation_ref="state/delegations/runtime-closure-child.json",
            result_sink_ref="artifacts/runtime_closure_only/implementer_result.json",
            lineage_ref="runtime-closure-root->runtime-closure-child",
            status=NodeStatus.ACTIVE,
        )
        runtime_closure_only_kernel_state = KernelState(
            task_id="runtime-closure-only-evaluator-node",
            root_goal="exercise runtime-closure-only evaluator translation",
            root_node_id=runtime_closure_only_root_node.node_id,
        )
        runtime_closure_only_kernel_state.register_node(runtime_closure_only_root_node)
        runtime_closure_only_kernel_state.register_node(runtime_closure_only_child_node)
        persist_kernel_state(
            runtime_closure_only_state_root,
            runtime_closure_only_kernel_state,
            authority=kernel_internal_authority(),
        )
        runtime_closure_only_submission = build_evaluator_submission_for_frozen_task(
            target_node=runtime_closure_only_child_node,
            workspace_root=runtime_closure_only_workspace_root,
            output_root=runtime_closure_only_state_root / "artifacts" / "evaluator_node_runtime_closure_only_runs",
            implementation_package_ref=ROOT / "loop_product" / "kernel" / "entry.py",
            product_manual_ref=ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
            final_effect_requirements=[
                {
                    "requirement_id": "REQ-RUNTIME-ONLY",
                    "description": "Evaluator terminal completion is the only blocking requirement.",
                    "blocking": True,
                    "requirement_kind": "runtime_closure",
                }
            ],
        )
        runtime_closure_only_result, runtime_closure_only_refs = run_evaluator_node(
            state_root=runtime_closure_only_state_root,
            submission=runtime_closure_only_submission,
        )
        if runtime_closure_only_result.verdict is not EvaluatorVerdict.PASS:
            return _fail(
                "runtime-closure-only evaluator-node submissions must translate terminal completion into PASS without a delegated reviewer verdict"
            )
        if "reviewer_response_ref" in runtime_closure_only_refs:
            return _fail("runtime-closure-only evaluator-node submissions must not fabricate reviewer_response_ref")
        runtime_closure_only_report = json.loads(
            Path(str(runtime_closure_only_refs["evaluation_report_ref"])).read_text(encoding="utf-8")
        )
        if str(runtime_closure_only_report.get("status") or "") != "COMPLETED":
            return _fail("runtime-closure-only evaluator-node reports must remain terminal COMPLETED")
        if list(runtime_closure_only_report.get("lanes") or []):
            return _fail("runtime-closure-only evaluator-node reports must not materialize delegated lanes")
        runtime_closure_only_requirement_ids = [
            str(item.get("requirement_id") or "")
            for item in list(runtime_closure_only_report.get("runtime_closure_requirements") or [])
        ]
        if runtime_closure_only_requirement_ids != ["REQ-RUNTIME-ONLY"]:
            return _fail("runtime-closure-only evaluator-node reports must preserve runtime_closure_requirements")
        runtime_closure_only_impl_ref = Path(
            str(
                runtime_closure_only_refs.get("workspace_implementer_result_ref")
                or runtime_closure_only_refs.get("implementer_result_ref")
                or ""
            )
        )
        if not runtime_closure_only_impl_ref.exists():
            return _fail("runtime-closure-only evaluator-node runs must still materialize authoritative implementer_result")
        runtime_closure_only_impl = json.loads(runtime_closure_only_impl_ref.read_text(encoding="utf-8"))
        if str(((runtime_closure_only_impl.get("evaluator_result") or {}).get("verdict")) or "") != "PASS":
            return _fail("runtime-closure-only evaluator-node implementer_result must preserve PASS")

        invocation_expectations = (
            (
                evaluator_run_root / ".loop" / "checker" / "runs" / "checker" / "exec_span.json",
                evaluator_run_root / ".loop" / "checker" / "workspaces" / "checker" / "workspace",
            ),
            (
                evaluator_run_root / ".loop" / "test_ai" / "runs" / "test_ai__EU-001" / "exec_span.json",
                evaluator_run_root / ".loop" / "test_ai" / "workspaces" / "test_ai__EU-001" / "workspace",
            ),
            (
                evaluator_run_root / ".loop" / "ai_user" / "runs" / "ai_user__EU-001" / "exec_span.json",
                evaluator_run_root / ".loop" / "ai_user" / "workspaces" / "ai_user__EU-001" / "workspace",
            ),
            (
                evaluator_run_root / ".loop" / "reviewer" / "runs" / "reviewer" / "exec_span.json",
                evaluator_run_root / ".loop" / "reviewer" / "workspaces" / "reviewer" / "workspace",
            ),
        )
        for exec_span_path, expected_workspace in invocation_expectations:
            exec_span = json.loads(exec_span_path.read_text(encoding="utf-8"))
            if Path(str(exec_span.get("cwd") or "")).resolve() != expected_workspace.resolve():
                return _fail(
                    f"evaluator node runtime must pin {exec_span_path} cwd to {expected_workspace.resolve()}"
                )
        for expected_workspace in (
            evaluator_run_root / ".loop" / "test_ai" / "workspaces" / "test_ai__EU-001" / "workspace",
            evaluator_run_root / ".loop" / "ai_user" / "workspaces" / "ai_user__EU-001" / "workspace",
        ):
            for runtime_relpath in (Path(".loop"), Path("workspace"), Path(".uv-cache")):
                if (expected_workspace / runtime_relpath).exists():
                    return _fail(
                        f"evaluator role workspaces must exclude source runtime/generated directory {runtime_relpath}"
                    )

        ordinary_bootstrap_root = temp_root / ".loop" / "ordinary_child_eval"
        ordinary_result_path = temp_root / "ordinary_child_eval_result.json"
        ordinary_script = temp_root / "ordinary_child_eval.py"
        ordinary_script.write_text(
            textwrap.dedent(
                f"""\
                #!/usr/bin/env python3
                from __future__ import annotations

                import json
                import sys
                from pathlib import Path

                ROOT = Path({str(ROOT)!r})
                if str(ROOT) not in sys.path:
                    sys.path.insert(0, str(ROOT))

                from loop_product.dispatch.child_dispatch import load_child_runtime_context
                from loop_product.loop.evaluator_client import (
                    build_evaluator_submission_for_endpoint_clarification,
                    run_evaluator_node,
                )
                from loop_product.protocols.node import NodeSpec
                from loop_product.runtime import initialize_evaluator_runtime

                state_root = Path(sys.argv[1]).resolve()
                fixture_cmd = str(sys.argv[2])
                result_path = Path(sys.argv[3]).resolve()

                bootstrap = initialize_evaluator_runtime(
                    state_root=state_root,
                    task_id="ordinary-evaluator-bootstrap",
                    root_goal="bootstrap evaluator runtime for an ordinary caller",
                    child_goal_slice="exercise evaluator quick path through the public runtime bootstrap surface",
                    child_node_id="ordinary-child-001",
                    round_id="R1",
                )
                runtime_context = load_child_runtime_context(state_root, "ordinary-child-001")
                target_node = NodeSpec.from_dict(dict(runtime_context["node"]))
                submission = build_evaluator_submission_for_endpoint_clarification(
                    target_node=target_node,
                    workspace_root=ROOT,
                    output_root=state_root / "artifacts" / "ordinary_evaluator_runs",
                    role_agent_cmd=fixture_cmd,
                )
                evaluator_result, runtime_refs = run_evaluator_node(
                    state_root=state_root,
                    submission=submission,
                )
                result_path.write_text(
                    json.dumps(
                        {{
                            "bootstrap": bootstrap,
                            "verdict": evaluator_result.verdict.value,
                            "runtime_refs": runtime_refs,
                        }},
                        indent=2,
                        sort_keys=True,
                    )
                    + "\\n",
                    encoding="utf-8",
                )
                """
            ),
            encoding="utf-8",
        )
        ordinary_script.chmod(0o755)
        ordinary_eval_proc = subprocess.run(
            [sys.executable, str(ordinary_script), str(ordinary_bootstrap_root), fixture_agent_cmd, str(ordinary_result_path)],
            cwd=str(temp_root),
            text=True,
            capture_output=True,
        )
        if ordinary_eval_proc.returncode != 0:
            detail = ordinary_eval_proc.stderr.strip() or ordinary_eval_proc.stdout.strip()
            return _fail(
                "ordinary callers must be able to bootstrap evaluator runtime through the public surface: "
                f"{detail}"
            )
        ordinary_eval_result = json.loads(ordinary_result_path.read_text(encoding="utf-8"))
        if ordinary_eval_result.get("verdict") != "PASS":
            return _fail("ordinary caller evaluator bootstrap path must still reach PASS with the repo-shipped fixture role agent")
        ordinary_role_workspaces = (
            ordinary_bootstrap_root
            / "artifacts"
            / "ordinary_evaluator_runs"
            / "endpoint_clarification_part1_ordinary-child-001"
            / "R1__ordinary-child-001__evaluator"
            / ".loop"
        )
        for expected_workspace in (
            ordinary_role_workspaces / "test_ai" / "workspaces" / "test_ai__EU-001" / "workspace",
            ordinary_role_workspaces / "ai_user" / "workspaces" / "ai_user__EU-001" / "workspace",
        ):
            for runtime_relpath in (Path(".loop"), Path("workspace"), Path(".uv-cache")):
                if (expected_workspace / runtime_relpath).exists():
                    return _fail(
                        "ordinary-caller evaluator role workspaces must exclude source runtime/generated "
                        f"directory {runtime_relpath}"
                    )
        ordinary_request_ref = Path(str((ordinary_eval_result.get("runtime_refs") or {}).get("request_ref") or ""))
        ordinary_report_ref = Path(str((ordinary_eval_result.get("runtime_refs") or {}).get("evaluation_report_ref") or ""))
        ordinary_child_ref = Path(str((ordinary_eval_result.get("bootstrap") or {}).get("child_node_ref") or ""))
        ordinary_delegation_ref = Path(str((ordinary_eval_result.get("bootstrap") or {}).get("delegation_ref") or ""))
        for path in (ordinary_request_ref, ordinary_report_ref, ordinary_child_ref, ordinary_delegation_ref):
            if not path.exists():
                return _fail(f"public runtime bootstrap must materialize ordinary-caller artifact {path}")

        artifact_workspace_root = temp_root / "artifact_workspace"
        artifact_root = artifact_workspace_root / "deliverables" / "primary_artifact"
        (artifact_root / "LinearlyBiasedPredictorChain").mkdir(parents=True, exist_ok=True)
        (artifact_workspace_root / "artifacts").mkdir(parents=True, exist_ok=True)
        (artifact_workspace_root / "workspace").mkdir(parents=True, exist_ok=True)
        (artifact_root / "README.md").write_text("# README\n", encoding="utf-8")
        (artifact_root / "TRACEABILITY.md").write_text("# TRACEABILITY\n", encoding="utf-8")
        (artifact_root / "BUILD.sh").write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
        (artifact_root / "LinearlyBiasedPredictorChain" / "Core.lean").write_text("def stub : Nat := 0\n", encoding="utf-8")
        (artifact_root / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
        (artifact_root / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text("cached\n", encoding="utf-8")
        (artifact_root / ".git").mkdir(parents=True, exist_ok=True)
        (artifact_root / ".git" / "config").write_text("[core]\n\trepositoryformatversion = 0\n", encoding="utf-8")
        (artifact_root / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
        (artifact_root / ".venv" / "bin" / "python").write_text("# stub\n", encoding="utf-8")
        (artifact_root / ".uv-cache").mkdir(parents=True, exist_ok=True)
        (artifact_root / ".uv-cache" / "cache.txt").write_text("cache\n", encoding="utf-8")
        (artifact_root / "build" / "lib").mkdir(parents=True, exist_ok=True)
        (artifact_root / "build" / "lib" / "artifact.txt").write_text("build\n", encoding="utf-8")
        (artifact_root / "_lake_build" / "lib").mkdir(parents=True, exist_ok=True)
        (artifact_root / "_lake_build" / "lib" / "artifact.txt").write_text("lake-build\n", encoding="utf-8")
        artifact_manual_ref = artifact_workspace_root / "PRODUCT_MANUAL.md"
        artifact_manual_ref.write_text("# Manual\n\nInspect the staged deliverable only.\n", encoding="utf-8")
        if not (artifact_root / ".lake").exists():
            return _fail("nested-heavy-tree regression fixture must materialize source deliverable .lake")
        if not (artifact_root / ".git").exists():
            return _fail("nested-heavy-tree regression fixture must materialize source deliverable .git")

        artifact_kernel_state = load_kernel_state(state_root)
        artifact_child_node = NodeSpec(
            node_id="child-artifact-copy-001",
            node_kind="implementer",
            goal_slice="validate evaluator workspace staging excludes nested heavyweight trees",
            parent_node_id=root_node.node_id,
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["evaluate", "report"],
            delegation_ref="state/delegations/child-artifact-copy-001.json",
            result_sink_ref="artifacts/child-artifact-copy-001/implementer_result.json",
            lineage_ref="root-kernel->child-artifact-copy-001",
            status=NodeStatus.ACTIVE,
        )
        artifact_kernel_state.register_node(artifact_child_node)
        persist_kernel_state(state_root, artifact_kernel_state, authority=kernel_internal_authority())

        artifact_copy_submission = build_evaluator_submission_for_frozen_task(
            target_node=artifact_child_node,
            workspace_root=artifact_workspace_root,
            output_root=state_root / "artifacts" / "evaluator_node_artifact_copy_runs",
            implementation_package_ref=artifact_root,
            product_manual_ref=artifact_manual_ref,
            final_effect_requirements=[
                {
                    "requirement_id": "REQ-ARTIFACT",
                    "description": "Reviewer-visible artifact remains available from staged role workspaces.",
                    "blocking": True,
                }
            ],
            role_agent_cmd=fixture_agent_cmd,
        )
        artifact_copy_result, artifact_copy_refs = run_evaluator_node(
            state_root=state_root,
            submission=artifact_copy_submission,
        )
        if artifact_copy_result.verdict is not EvaluatorVerdict.PASS:
            return _fail("nested-heavy-tree fixture must still reach PASS through the evaluator-node path")
        for heavyweight_relpath in (
            Path(".lake"),
            Path(".git"),
            Path(".venv"),
            Path(".uv-cache"),
            Path("build"),
            Path("_lake_build"),
        ):
            if (artifact_root / heavyweight_relpath).exists():
                return _fail(
                    "evaluator-node runtime must canonicalize the source deliverable itself and prune "
                    f"runtime-owned heavy tree {heavyweight_relpath} before a successful evaluation"
                )
        artifact_copy_run_root = Path(str(artifact_copy_refs["evaluation_report_ref"])).resolve().parent
        for expected_workspace in (
            artifact_copy_run_root / ".loop" / "test_ai" / "workspaces" / "test_ai__EU-001" / "workspace",
            artifact_copy_run_root / ".loop" / "ai_user" / "workspaces" / "ai_user__EU-001" / "workspace",
        ):
            copied_artifact_root = expected_workspace / "deliverables" / "primary_artifact"
            for reviewer_visible_relpath in (
                Path("README.md"),
                Path("TRACEABILITY.md"),
                Path("BUILD.sh"),
                Path("LinearlyBiasedPredictorChain") / "Core.lean",
            ):
                if not (copied_artifact_root / reviewer_visible_relpath).exists():
                    return _fail(
                        "evaluator role workspace staging must preserve reviewer-visible deliverable files "
                        f"such as {reviewer_visible_relpath}"
                    )
            for heavyweight_relpath in (
                Path(".lake"),
                Path(".git"),
                Path(".venv"),
                Path(".uv-cache"),
                Path("build"),
                Path("_lake_build"),
            ):
                if (copied_artifact_root / heavyweight_relpath).exists():
                    return _fail(
                        "evaluator role workspace staging must exclude nested heavyweight deliverable tree "
                        f"{heavyweight_relpath}"
                    )

        failing_agent_cmd = _fixture_role_agent_cmd(scenario="lane_failure", failing_role="checker", failure_message="synthetic checker failure")
        bug_submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=child_node,
            workspace_root=ROOT,
            output_root=state_root / "artifacts" / "evaluator_node_bug_runs",
            role_agent_cmd=failing_agent_cmd,
        )
        bug_result, _bug_refs = run_evaluator_node(
            state_root=state_root,
            submission=bug_submission,
        )
        if bug_result.verdict is not EvaluatorVerdict.ERROR:
            return _fail(f"simple-evaluator delegated role failure must translate to ERROR, got {bug_result.verdict.value}")

        composite_provider_capacity_failure = "\n".join(
            [
                "2026-03-20T17:08:48.425852Z ERROR codex_core::models_manager::manager: failed to refresh available models: timeout waiting for child process to exit",
                "2026-03-20T17:08:48.437999Z WARN codex_state::runtime: failed to open state db at /Users/xiongjiangkai/.codex/state_5.sqlite: migration 20 was previously applied but is missing in the resolved migrations",
                "2026-03-20T17:08:48.438303Z WARN codex_core::state_db: failed to initialize state runtime at /Users/xiongjiangkai/.codex: migration 20 was previously applied but is missing in the resolved migrations",
                "2026-03-20T17:08:50.133306Z WARN codex_core::rollout::list: state db discrepancy during find_thread_path_by_id_str_in_subdir: falling_back",
                "2026-03-20T17:08:55.267441Z WARN codex_core::codex: stream disconnected - retrying sampling request (1/5 in 181ms)...",
                "Reconnecting... 1/5 (We're currently experiencing high demand, which may cause temporary errors.)",
            ]
        )
        issue_agents = {
            "uv": (
                "uv run --project . started creating a virtual environment and failed before touching the product surface",
                "uv",
                "STRUCTURED_EXCEPTION",
                False,
                "evaluator_exec_uv",
                "staged-workspace `python`",
            ),
            "path": (
                "python: can't open file '/tmp/missing_product_entry.py': [Errno 2] No such file or directory",
                "path",
                "STRUCTURED_EXCEPTION",
                False,
                "evaluator_exec_path",
                "correct the path",
            ),
            "repo_structure": (
                "pyproject.toml not found from the current repo root; workspace layout assumption was wrong",
                "repo_structure",
                "STRUCTURED_EXCEPTION",
                False,
                "evaluator_exec_repo_structure",
                "confirm the repo structure",
            ),
            "command_sequence": (
                "zsh:1: parse error near `fi' while executing the generated command sequence",
                "command_sequence",
                "STRUCTURED_EXCEPTION",
                False,
                "evaluator_exec_command_sequence",
                "repair the command sequence",
            ),
            "provider_transport": (
                "failed to connect to websocket: IO error: Connection reset by peer (os error 54)\nstream disconnected before completion",
                "provider_transport",
                "STUCK",
                True,
                "evaluator_exec_provider_transport",
                "retry the same evaluator lane with bounded backoff",
            ),
            "provider_capacity": (
                "We're currently experiencing high demand, which may cause temporary errors.",
                "provider_capacity",
                "STUCK",
                True,
                "evaluator_exec_provider_capacity",
                "retry the same evaluator lane after a short bounded backoff",
            ),
            "provider_quota": (
                "ERROR: You've hit your usage limit. Please try again later.",
                "provider_quota",
                "STUCK",
                True,
                "evaluator_exec_provider_quota",
                "retry after the provider quota window resets",
            ),
            "provider_runtime": (
                "thread 'main' panicked at system-configuration dynamic_store.rs: Attempted to create a NULL object.",
                "provider_runtime",
                "STUCK",
                True,
                "evaluator_exec_provider_runtime",
                "retry the same evaluator lane with bounded backoff",
            ),
            "provider_capacity_composite": (
                composite_provider_capacity_failure,
                "provider_capacity",
                "STUCK",
                True,
                "evaluator_exec_provider_capacity",
                "retry the same evaluator lane after a short bounded backoff",
            ),
        }
        for issue_name, (
            failure_message,
            expected_issue_kind,
            expected_verdict,
            expected_retryable,
            expected_self_attribution,
            expected_repair_snippet,
        ) in issue_agents.items():
            issue_agent_cmd = _fixture_role_agent_cmd(
                scenario="lane_failure",
                failing_role="test_designer",
                failure_message=failure_message,
            )
            issue_submission = build_evaluator_submission_for_endpoint_clarification(
                target_node=child_node,
                workspace_root=ROOT,
                output_root=state_root / "artifacts" / f"evaluator_node_{issue_name}_runs",
                role_agent_cmd=issue_agent_cmd,
            )
            issue_result, issue_refs = run_evaluator_node(
                state_root=state_root,
                submission=issue_submission,
            )
            if issue_result.verdict.value != expected_verdict:
                return _fail(
                    f"{issue_name} evaluator-exec failure must translate to {expected_verdict}, got {issue_result.verdict.value}"
                )
            if issue_result.retryable is not expected_retryable:
                return _fail(f"{issue_name} evaluator-exec failure must preserve retryable={expected_retryable}")
            if str(issue_result.diagnostics.get("self_attribution") or "") != expected_self_attribution:
                return _fail(
                    f"{issue_name} evaluator-exec failure must preserve self_attribution={expected_self_attribution!r}"
                )
            if expected_repair_snippet not in str(issue_result.diagnostics.get("self_repair") or ""):
                return _fail(f"{issue_name} evaluator-exec failure must preserve a specific self_repair hint")
            issue_report = json.loads(Path(str(issue_refs["evaluation_report_ref"])).read_text(encoding="utf-8"))
            issue_error = dict(issue_report.get("error") or {})
            if str(issue_error.get("kind") or "") != "STRUCTURED_EXCEPTION":
                return _fail("structured evaluator-exec failures must be persisted as structured exceptions")
            if str(issue_error.get("issue_kind") or "") != expected_issue_kind:
                return _fail(f"structured evaluator-exec failures must preserve issue_kind={expected_issue_kind!r}")
            if expected_issue_kind.startswith("provider_"):
                if str(issue_report.get("status") or "") != "RECOVERY_REQUIRED":
                    return _fail("retryable provider failures must surface RECOVERY_REQUIRED instead of terminal ERROR")
                issue_recovery = dict(issue_report.get("recovery") or {})
                if issue_recovery.get("required") is not True or str(issue_recovery.get("mode") or "") != "SAME_RUN_RESUME":
                    return _fail("retryable provider failures must expose same-run recovery metadata")
            validate(issue_result.to_dict(), evaluator_result_schema)

        with tempfile.TemporaryDirectory(prefix="loop_system_empty_envelope_provider_issue_") as td:
            temp_root = Path(td)
            raw_response_path = temp_root / "response.raw.json"
            raw_response_path.write_text("", encoding="utf-8")
            result_path = temp_root / "result.json"
            output_schema_path = temp_root / "output_schema.json"
            output_schema_path.write_text("{}", encoding="utf-8")
            stdout_path = temp_root / "stdout.txt"
            stdout_path.write_text("", encoding="utf-8")
            stderr_path = temp_root / "stderr.txt"
            stderr_path.write_text(
                "ERROR: We're currently experiencing high demand, which may cause temporary errors.\n",
                encoding="utf-8",
            )
            structured_error = _structured_output_pre_result_error(
                runtime_role_instance_id="checker",
                output_schema_path=output_schema_path,
                raw_response_path=raw_response_path,
                effective_result_path=result_path,
                exec_span={
                    "exit_code": 1,
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                },
                base_dir=temp_root,
            )
            if structured_error is None or "provider_capacity" not in str(structured_error):
                return _fail("empty structured-output envelopes with high-demand stderr must preserve provider_capacity classification")
            stderr_path.write_text("plain checker crash without provider hints\n", encoding="utf-8")
            plain_error = _structured_output_pre_result_error(
                runtime_role_instance_id="checker",
                output_schema_path=output_schema_path,
                raw_response_path=raw_response_path,
                effective_result_path=result_path,
                exec_span={
                    "exit_code": 1,
                    "stdout_path": str(stdout_path),
                    "stderr_path": str(stderr_path),
                },
                base_dir=temp_root,
            )
            if plain_error is None or "empty structured-output envelope" not in str(plain_error):
                return _fail("plain empty structured-output envelopes must still surface the dedicated empty-envelope error")

        resume_request = {
            "schema": "loop_product.loop_evaluator_prototype_input",
            "schema_version": "1.0.0",
            "evaluation_id": "same_run_lane_resume",
            "run_id": "current",
            "workspace_root": str(workspace_root),
            "output_root": str(temp_root / "same_run_resume_output"),
            "product_manual_ref": str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()
            ),
            "final_effects_text_ref": str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_FINAL_EFFECTS.md").resolve()
            ),
            "final_effect_requirements": [
                {
                    "requirement_id": "REQ-001",
                    "description": "resume must preserve completed first-lane work",
                    "blocking": True,
                },
                {
                    "requirement_id": "REQ-002",
                    "description": "resume must rerun only the unfinished second lane",
                    "blocking": True,
                },
            ],
            "role_requirements": {
                "checker": "Normalize into two evaluation units for same-run lane resume coverage.",
                "test_designer": "Stay on the documented evaluator surface.",
                "ai_user": "Behave like a faithful documented caller.",
                "reviewer": "Return a terminal reviewer verdict backed by reviewer-visible evidence.",
            },
            "agent_execution": {
                "default": {"agent_cmd": _fixture_role_agent_cmd(scenario="lane_resume_same_run")},
            },
        }
        first_resume_report = run_evaluator(request=resume_request)
        if str(first_resume_report.get("status") or "") != "RECOVERY_REQUIRED":
            return _fail("first same-run lane-resume evaluator attempt must stop in RECOVERY_REQUIRED after the unfinished lane fails")
        resume_run_root = Path(str(first_resume_report.get("run_root") or ""))
        resume_state_path = resume_run_root / "EvaluatorRunState.json"
        if not resume_state_path.exists():
            return _fail("same-run lane-resume evaluator must persist EvaluatorRunState.json after the failed first attempt")
        first_resume_recovery = dict(first_resume_report.get("recovery") or {})
        if first_resume_recovery.get("required") is not True or str(first_resume_recovery.get("mode") or "") != "SAME_RUN_RESUME":
            return _fail("first same-run lane-resume evaluator attempt must expose same-run recovery metadata")
        first_resume_state = json.loads(resume_state_path.read_text(encoding="utf-8"))
        validate(first_resume_state, evaluator_run_state_schema)
        if str(first_resume_state.get("status") or "") != "RECOVERY_REQUIRED":
            return _fail("same-run lane-resume run-state must preserve RECOVERY_REQUIRED until resume finishes")
        if str(dict(first_resume_state.get("checker") or {}).get("status") or "") != "COMPLETED":
            return _fail("run-state must preserve the completed checker stage before a later lane fails")
        if [str(item.get("unit_id") or "") for item in list(first_resume_state.get("lane_plan") or [])] != ["EU-001", "EU-002"]:
            return _fail("run-state must freeze the checker-produced lane plan in order")

        second_resume_report = run_evaluator(request=resume_request)
        if str(second_resume_report.get("status") or "") != "COMPLETED":
            return _fail("second same-run lane-resume evaluator attempt must resume and finish instead of restarting from scratch")
        second_resume_state = json.loads(resume_state_path.read_text(encoding="utf-8"))
        validate(second_resume_state, evaluator_run_state_schema)
        if int(dict(second_resume_state.get("checker") or {}).get("attempt_count") or 0) != 1:
            return _fail("same-run lane resume must not rerun checker once the checker graph is frozen")
        resumed_lanes = dict(second_resume_state.get("lanes_by_unit_id") or {})
        if str(dict(resumed_lanes.get("EU-001") or {}).get("status") or "") != "COMPLETED":
            return _fail("same-run lane resume must preserve the completed first lane")
        if str(dict(resumed_lanes.get("EU-002") or {}).get("status") or "") != "COMPLETED":
            return _fail("same-run lane resume must complete the unfinished second lane on resume")
        if str(dict(second_resume_state.get("reviewer") or {}).get("status") or "") != "COMPLETED":
            return _fail("reviewer must run only after all resumed lanes become terminal")
        counter_root = workspace_root / ".loop_fixture_state"
        if _read_counter(counter_root / "checker.lane_resume_same_run.count") != 1:
            return _fail("same-run lane resume must not rerun checker")
        if _read_counter(counter_root / "test_ai__EU-001.lane_resume_same_run.count") != 1:
            return _fail("same-run lane resume must not rerun completed first-lane test_ai work")
        if _read_counter(counter_root / "ai_user__EU-001.lane_resume_same_run.count") != 1:
            return _fail("same-run lane resume must not rerun completed first-lane ai_user work")
        if _read_counter(counter_root / "test_ai__EU-002.lane_resume_same_run.count") != 1:
            return _fail("same-run lane resume must preserve completed second-lane test_ai work")
        if _read_counter(counter_root / "ai_user__EU-002.lane_resume_same_run.count") != 3:
            return _fail("same-run lane resume must preserve first-run bounded retries and then rerun only the unfinished second-lane ai_user work")

        helper_workspace_root = temp_root / "same_run_resume_helper_workspace"
        helper_workspace_root.mkdir(parents=True, exist_ok=True)
        helper_state_root = temp_root / ".loop" / "same_run_resume_helper_state"
        ensure_runtime_tree(helper_state_root)
        helper_child_node = NodeSpec(
            node_id="child-resume-helper-001",
            node_kind="implementer",
            parent_node_id="root-kernel",
            goal_slice="exercise evaluator helper same-run resume",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["evaluate", "report"],
            delegation_ref="state/delegations/child-resume-helper-001.json",
            result_sink_ref="artifacts/evaluator_helper_summary.json",
            lineage_ref="root-kernel->child-resume-helper-001",
            status=NodeStatus.ACTIVE,
        )
        helper_kernel_state = KernelState(
            task_id="same-run-lane-resume-helper",
            root_goal="exercise evaluator-node same-run terminal helper",
            root_node_id=root_node.node_id,
        )
        helper_kernel_state.register_node(root_node)
        helper_kernel_state.register_node(helper_child_node)
        persist_kernel_state(helper_state_root, helper_kernel_state, authority=kernel_internal_authority())
        helper_submission = EvaluatorNodeSubmission(
            evaluation_id="same_run_lane_resume_helper",
            evaluator_node_id="child-resume-helper-001__evaluator",
            target_node_id=helper_child_node.node_id,
            parent_node_id=helper_child_node.parent_node_id,
            generation=helper_child_node.generation,
            round_id=helper_child_node.round_id,
            lineage_ref=helper_child_node.lineage_ref,
            goal_slice="resume the same evaluator run until terminal without restarting checker",
            workspace_root=str(helper_workspace_root),
            output_root=str(temp_root / "same_run_resume_helper_output"),
            implementation_package_ref=str((ROOT / "loop_product" / "loop" / "evaluator_client.py").resolve()),
            product_manual_ref=str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()
            ),
            final_effects_text_ref=str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_FINAL_EFFECTS.md").resolve()
            ),
            role_requirements={
                "checker": "Normalize into two evaluation units for helper same-run resume coverage.",
                "test_designer": "Stay on the documented evaluator surface.",
                "ai_user": "Behave like a faithful documented caller.",
                "reviewer": "Return a terminal reviewer verdict backed by reviewer-visible evidence.",
            },
            agent_execution={
                "default": {"agent_cmd": _fixture_role_agent_cmd(scenario="lane_resume_same_run")},
            },
        )
        helper_result, helper_runtime_refs = run_evaluator_node_until_terminal(
            state_root=helper_state_root,
            submission=helper_submission,
            max_same_run_recovery_attempts=3,
        )
        if helper_result.verdict is not EvaluatorVerdict.PASS:
            return _fail("run_evaluator_node_until_terminal must keep same-run recovery going until a terminal PASS lands")
        helper_report = json.loads(Path(helper_runtime_refs["evaluation_report_ref"]).read_text(encoding="utf-8"))
        if str(helper_report.get("status") or "") != "COMPLETED":
            return _fail("run_evaluator_node_until_terminal must leave a terminal completed evaluator report")
        helper_run_state = json.loads(
            (
                Path(str(helper_runtime_refs.get("run_state_ref") or ""))
                if str(helper_runtime_refs.get("run_state_ref") or "").strip()
                else Path(helper_runtime_refs["evaluation_report_ref"]).parent / "EvaluatorRunState.json"
            ).read_text(encoding="utf-8")
        )
        validate(helper_run_state, evaluator_run_state_schema)
        if str(helper_run_state.get("status") or "") != "COMPLETED":
            return _fail("run_evaluator_node_until_terminal must leave the same evaluator run in COMPLETED state")
        if int(dict(helper_run_state.get("checker") or {}).get("attempt_count") or 0) != 1:
            return _fail("run_evaluator_node_until_terminal must preserve the frozen checker graph during same-run recovery")
        helper_lanes = dict(helper_run_state.get("lanes_by_unit_id") or {})
        if str(dict(helper_lanes.get("EU-001") or {}).get("status") or "") != "COMPLETED":
            return _fail("run_evaluator_node_until_terminal must preserve completed first-lane work")
        if str(dict(helper_lanes.get("EU-002") or {}).get("status") or "") != "COMPLETED":
            return _fail("run_evaluator_node_until_terminal must finish the resumed second lane before returning")
        helper_kernel_result_ref = helper_state_root / helper_child_node.result_sink_ref
        helper_workspace_result_ref = helper_workspace_root / helper_child_node.result_sink_ref
        if not helper_kernel_result_ref.exists():
            return _fail("terminal evaluator completion must materialize the authoritative implementer_result sink")
        if not helper_workspace_result_ref.exists():
            return _fail("terminal evaluator completion must mirror implementer_result into the workspace-local sink")
        helper_result_payload = json.loads(helper_kernel_result_ref.read_text(encoding="utf-8"))
        if str(((helper_result_payload.get("evaluator_result") or {}).get("verdict")) or "") != "PASS":
            return _fail("materialized implementer_result must project the terminal evaluator PASS verdict")
        if str(helper_result_payload.get("workspace_mirror_ref") or "") != helper_submission.implementation_package_ref:
            return _fail("materialized implementer_result must preserve the evaluated workspace mirror ref")
        if str(helper_result_payload.get("request_ref") or "") != str(helper_runtime_refs.get("request_ref") or ""):
            return _fail("materialized implementer_result must preserve the evaluator request ref")
        if str(helper_result_payload.get("evaluation_report_ref") or "") != str(helper_runtime_refs.get("evaluation_report_ref") or ""):
            return _fail("materialized implementer_result must preserve the evaluator report ref")
        helper_kernel_state_after = load_kernel_state(helper_state_root)
        helper_kernel_node_after = dict(helper_kernel_state_after.nodes.get(helper_child_node.node_id) or {})
        if str(helper_kernel_node_after.get("status") or "") != "COMPLETED":
            return _fail("terminal evaluator PASS must normalize kernel node status to COMPLETED")
        if str(dict(helper_kernel_node_after.get("runtime_state") or {}).get("attachment_state") or "") != "TERMINAL":
            return _fail("terminal evaluator PASS must mark kernel runtime attachment as TERMINAL")
        helper_node_snapshot_ref = helper_state_root / "state" / f"{helper_child_node.node_id}.json"
        helper_node_snapshot = json.loads(helper_node_snapshot_ref.read_text(encoding="utf-8"))
        if str(helper_node_snapshot.get("status") or "") != "COMPLETED":
            return _fail("terminal evaluator PASS must persist the per-node snapshot as COMPLETED")
        if str(dict(helper_node_snapshot.get("runtime_state") or {}).get("attachment_state") or "") != "TERMINAL":
            return _fail("terminal evaluator PASS must persist terminal runtime attachment into the per-node snapshot")

        fail_workspace_root = temp_root / "terminal_fail_workspace"
        fail_workspace_root.mkdir(parents=True, exist_ok=True)
        fail_state_root = temp_root / ".loop" / "terminal_fail_state"
        ensure_runtime_tree(fail_state_root)
        fail_child_node = NodeSpec(
            node_id="child-terminal-fail-001",
            node_kind="implementer",
            parent_node_id="root-kernel",
            goal_slice="exercise implementer_result materialization on terminal evaluator FAIL",
            generation=1,
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access"},
            reasoning_profile={"role": "implementer", "thinking_budget": "medium"},
            budget_profile={"max_rounds": 2},
            allowed_actions=["evaluate", "report"],
            delegation_ref="state/delegations/child-terminal-fail-001.json",
            result_sink_ref="artifacts/child-terminal-fail-001/implementer_result.json",
            lineage_ref="root-kernel->child-terminal-fail-001",
            status=NodeStatus.ACTIVE,
        )
        fail_kernel_state = KernelState(
            task_id="terminal-fail-helper",
            root_goal="exercise terminal evaluator fail materialization",
            root_node_id=root_node.node_id,
        )
        fail_kernel_state.register_node(root_node)
        fail_kernel_state.register_node(fail_child_node)
        persist_kernel_state(fail_state_root, fail_kernel_state, authority=kernel_internal_authority())
        fail_submission = EvaluatorNodeSubmission(
            evaluation_id="terminal_fail_helper",
            evaluator_node_id="child-terminal-fail-001__evaluator",
            target_node_id=fail_child_node.node_id,
            parent_node_id=fail_child_node.parent_node_id,
            generation=fail_child_node.generation,
            round_id=fail_child_node.round_id,
            lineage_ref=fail_child_node.lineage_ref,
            goal_slice="materialize terminal FAIL results to committed sinks",
            workspace_root=str(fail_workspace_root),
            output_root=str(temp_root / "terminal_fail_output"),
            implementation_package_ref=str((ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()),
            product_manual_ref=str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()
            ),
            final_effects_text_ref=str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_FINAL_EFFECTS.md").resolve()
            ),
            role_requirements={
                "checker": "Normalize the bounded requirement for terminal FAIL sink coverage.",
                "test_designer": "Stay on the documented evaluator product surface.",
                "ai_user": "Behave like a faithful documented caller.",
                "reviewer": "Return a terminal FAIL verdict backed by reviewer-visible evidence.",
            },
            agent_execution={
                "default": {"agent_cmd": _fixture_role_agent_cmd(scenario="terminal_fail")},
            },
        )
        fail_result, fail_runtime_refs = run_evaluator_node_until_terminal(
            state_root=fail_state_root,
            submission=fail_submission,
            max_same_run_recovery_attempts=0,
        )
        if fail_result.verdict is not EvaluatorVerdict.FAIL:
            return _fail("terminal FAIL fixture must return a terminal evaluator FAIL verdict")
        fail_kernel_result_ref = fail_state_root / fail_child_node.result_sink_ref
        fail_workspace_result_ref = fail_workspace_root / fail_child_node.result_sink_ref
        if not fail_kernel_result_ref.exists():
            return _fail("terminal evaluator FAIL must still materialize the authoritative implementer_result sink")
        if not fail_workspace_result_ref.exists():
            return _fail("terminal evaluator FAIL must still mirror implementer_result into the workspace-local sink")
        fail_result_payload = json.loads(fail_kernel_result_ref.read_text(encoding="utf-8"))
        if str(((fail_result_payload.get("evaluator_result") or {}).get("verdict")) or "") != "FAIL":
            return _fail("materialized implementer_result must project the terminal evaluator FAIL verdict")
        if str(fail_result_payload.get("evaluation_report_ref") or "") != str(fail_runtime_refs.get("evaluation_report_ref") or ""):
            return _fail("terminal FAIL materialization must preserve the evaluator report ref")
        if str(fail_result_payload.get("status") or "") != "COMPLETED":
            return _fail("terminal FAIL materialization must still mark the implementer result as completed")
        if str(fail_result_payload.get("outcome") or "") != "REPAIR_REQUIRED":
            return _fail("retryable terminal FAIL materialization must mark the implementer result as unfinished repair work")
        fail_diagnostics = dict((fail_result_payload.get("evaluator_result") or {}).get("diagnostics") or {})
        if str(fail_diagnostics.get("self_repair") or "") == "":
            return _fail("retryable terminal FAIL materialization must preserve a non-empty self_repair hint")
        fail_kernel_state_after = load_kernel_state(fail_state_root)
        fail_kernel_node_after = dict(fail_kernel_state_after.nodes.get(fail_child_node.node_id) or {})
        if str(fail_kernel_node_after.get("status") or "") != "ACTIVE":
            return _fail("retryable terminal FAIL must leave kernel node status ACTIVE for repair work")
        if str(dict(fail_kernel_node_after.get("runtime_state") or {}).get("attachment_state") or "") == "TERMINAL":
            return _fail("retryable terminal FAIL must not mark kernel runtime attachment as TERMINAL")
        fail_node_snapshot_ref = fail_state_root / "state" / f"{fail_child_node.node_id}.json"
        if fail_node_snapshot_ref.exists():
            fail_node_snapshot = json.loads(fail_node_snapshot_ref.read_text(encoding="utf-8"))
            if str(fail_node_snapshot.get("status") or "") != "ACTIVE":
                return _fail("retryable terminal FAIL per-node snapshot must stay ACTIVE when it exists")
            if str(dict(fail_node_snapshot.get("runtime_state") or {}).get("attachment_state") or "") == "TERMINAL":
                return _fail("retryable terminal FAIL must not persist terminal runtime attachment into the per-node snapshot")

        rogue_agent = temp_root / "task_local_eval_agent.py"
        rogue_agent.write_text("#!/usr/bin/env python3\nprint('rogue')\n", encoding="utf-8")
        rogue_agent.chmod(0o755)
        try:
            build_evaluator_submission_for_endpoint_clarification(
                target_node=child_node,
                workspace_root=ROOT,
                output_root=state_root / "artifacts" / "evaluator_node_rogue_runs",
                role_agent_cmd=str(rogue_agent),
            )
        except ValueError as exc:
            if "repo-shipped evaluator role agent" not in str(exc):
                return _fail("custom evaluator role-agent rejection must explain the repo-shipped-only policy")
        else:
            return _fail("custom temp-script evaluator role agents must be rejected before execution")

        raw_request = {
            "schema": "loop_product.loop_evaluator_prototype_input",
            "schema_version": "1.0.0",
            "evaluation_id": "rogue_agent_raw_surface",
            "run_id": "current",
            "workspace_root": str(ROOT),
            "output_root": str(temp_root / "rogue_raw_output"),
            "product_manual_ref": str(
                (ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md").resolve()
            ),
            "final_effect_requirements": [
                {
                    "requirement_id": "REQ-001",
                    "description": "raw surface must reject custom temp-script evaluator agents",
                    "blocking": True,
                }
            ],
            "role_requirements": {
                "checker": "Normalize the single bounded requirement.",
                "test_designer": "Stay on the documented evaluator product surface.",
                "ai_user": "Behave like a faithful documented caller.",
                "reviewer": "Return a terminal verdict backed by reviewer-visible evidence.",
            },
            "agent_execution": {
                "default": {"agent_cmd": str(rogue_agent)},
            },
        }
        raw_report = run_evaluator(request=raw_request)
        if str(raw_report.get("status") or "") != "ERROR":
            return _fail("raw evaluator surface must fail closed when a rogue custom agent_cmd is supplied")
        raw_error = dict(raw_report.get("error") or {})
        if "repo-shipped evaluator role agent" not in str(raw_error.get("message") or ""):
            return _fail("raw evaluator surface must explain repo-shipped-only agent execution when rejecting a rogue script")

        unknown_reviewer_agent_cmd = _fixture_role_agent_cmd(scenario="unknown_reviewer")
        error_submission = build_evaluator_submission_for_endpoint_clarification(
            target_node=child_node,
            workspace_root=ROOT,
            output_root=state_root / "artifacts" / "evaluator_node_unknown_runs",
            role_agent_cmd=unknown_reviewer_agent_cmd,
        )
        error_result, _error_refs = run_evaluator_node(
            state_root=state_root,
            submission=error_submission,
        )
        if error_result.verdict is not EvaluatorVerdict.ERROR:
            return _fail(f"unknown reviewer verdict must translate to ERROR, got {error_result.verdict.value}")
        if str(error_result.diagnostics.get("self_attribution") or "") != "evaluator_unknown_terminal":
            return _fail("unknown reviewer verdict must preserve evaluator-side self-attribution")
        validate(error_result.to_dict(), evaluator_result_schema)

        # Regression: if a child writes a valid terminal report between the
        # supervisor's pre-poll check and the observed child exit, the
        # supervisor must still accept the terminal report instead of
        # misclassifying the run as BUG.
        import loop_product.evaluator.prototype as evaluator_prototype_module

        manual_ref = temp_root / "supervisor_race_manual.md"
        manual_ref.write_text("# Manual\n\nSynthetic supervisor race regression.\n", encoding="utf-8")
        supervised_output_root = temp_root / "supervisor_race_output"
        supervised_request = {
            "schema": "loop_product.loop_evaluator_prototype_input",
            "schema_version": "1.0.0",
            "evaluation_id": "supervisor_race_regression",
            "run_id": "current",
            "workspace_root": str(ROOT),
            "output_root": str(supervised_output_root),
            "product_manual_ref": str(manual_ref),
            "final_effect_requirements": [
                {
                    "requirement_id": "REQ-001",
                    "description": "Synthetic requirement for supervisor race regression.",
                    "blocking": True,
                }
            ],
            "role_requirements": {
                "test_designer": "Not used by this synthetic supervisor regression.",
                "ai_user": "Not used by this synthetic supervisor regression.",
                "reviewer": "Not used by this synthetic supervisor regression.",
            },
        }
        supervised_run_root = supervised_output_root / "supervisor_race_regression" / "current"
        terminal_report_ref = supervised_run_root / "EvaluationReport.json"
        stdout_ref = supervised_run_root / "supervisor_child.stdout.txt"
        stderr_ref = supervised_run_root / "supervisor_child.stderr.txt"
        snapshot_calls = {"count": 0}

        class _FakeProc:
            def poll(self) -> int:
                return 0

        def _write_terminal_report() -> None:
            terminal_report = {
                "schema": "loop_product.loop_evaluator_prototype_report",
                "schema_version": "1.0.0",
                "evaluation_id": "supervisor_race_regression",
                "run_id": "current",
                "run_root": str(supervised_run_root),
                "workspace_root": str(ROOT),
                "product_manual_ref": str(manual_ref),
                "status": "ERROR",
                "role_runs": {},
                "notes": ["synthetic terminal report written during supervisor race regression"],
                "error": {
                    "stage": "reviewer",
                    "kind": "RUNTIME_FAILURE",
                    "message": "synthetic terminal report",
                },
            }
            terminal_report_ref.parent.mkdir(parents=True, exist_ok=True)
            terminal_report_ref.write_text(json.dumps(terminal_report, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        original_start_cmd = evaluator_prototype_module.start_cmd
        original_finish_running_cmd = evaluator_prototype_module.finish_running_cmd
        original_progress_snapshot = evaluator_prototype_module._supervisor_progress_snapshot
        try:
            def _fake_start_cmd(*, cmd: object, cwd: object, log_dir: Path, label: str, env: object | None = None) -> SimpleNamespace:
                del cmd, cwd, label, env
                stdout_ref.parent.mkdir(parents=True, exist_ok=True)
                stdout_ref.write_text("", encoding="utf-8")
                stderr_ref.write_text("", encoding="utf-8")
                return SimpleNamespace(proc=_FakeProc(), stdout_path=stdout_ref, stderr_path=stderr_ref)

            def _fake_finish_running_cmd(handle: object, timed_out: bool = False, timeout_kind: str | None = None) -> SimpleNamespace:
                return SimpleNamespace(span={"exit_code": 0})

            def _fake_supervisor_progress_snapshot(
                *,
                run_root: Path,
                min_mtime_ns: int,
                ignored_paths: object,
            ) -> tuple[int, int, int]:
                snapshot_calls["count"] += 1
                if snapshot_calls["count"] == 2:
                    _write_terminal_report()
                    return (terminal_report_ref.stat().st_mtime_ns, terminal_report_ref.stat().st_size, 1)
                return (0, 0, 0)

            evaluator_prototype_module.start_cmd = _fake_start_cmd
            evaluator_prototype_module.finish_running_cmd = _fake_finish_running_cmd
            evaluator_prototype_module._supervisor_progress_snapshot = _fake_supervisor_progress_snapshot

            supervisor_report = run_evaluator_prototype_supervised(
                request=supervised_request,
                poll_interval_s=0.01,
                no_progress_timeout_s=1.0,
                _child_argv_override=[sys.executable, "-c", "raise SystemExit(0)"],
            )
        finally:
            evaluator_prototype_module.start_cmd = original_start_cmd
            evaluator_prototype_module.finish_running_cmd = original_finish_running_cmd
            evaluator_prototype_module._supervisor_progress_snapshot = original_progress_snapshot

        if supervisor_report["status"] != "TERMINAL":
            return _fail(
                "supervisor must accept a valid terminal evaluator report that appears just before child exit"
            )
        if supervisor_report.get("terminal_evaluation_status") != "ERROR":
            return _fail("supervisor regression run must preserve the observed terminal evaluation status")
        if Path(str(supervisor_report.get("terminal_evaluation_report_ref") or "")).resolve() != terminal_report_ref.resolve():
            return _fail("supervisor regression run must point at the observed terminal evaluation report")

    print("[loop-system-evaluator-node] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
