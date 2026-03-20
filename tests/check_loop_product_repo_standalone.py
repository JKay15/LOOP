#!/usr/bin/env python3
"""Standalone smoke check for the shared LOOP product repo."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-product-standalone][FAIL] {msg}", file=sys.stderr)
    return 2


def _run_help(module_name: str) -> tuple[int, str, str]:
    proc = subprocess.run(
        [sys.executable, "-m", module_name, "--help"],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def _write_fake_policy_script(path: Path) -> None:
    path.write_text(
        textwrap.dedent(
            """\
            #!/usr/bin/env python3
            import json
            import os
            import sys
            from pathlib import Path

            PRIMARY_QUESTION = "When this is successful, what will you be able to see or do that would make you say it is exactly right?"
            payload = json.loads(Path(os.environ["LOOP_ENDPOINT_POLICY_INPUT"]).read_text(encoding="utf-8"))
            action = str(payload.get("action") or "")
            if action == "START":
                result = {
                    "task_type": "other",
                    "sufficient": False,
                    "user_request_summary": str(payload.get("user_text") or "").strip(),
                    "final_effect": "",
                    "observable_success_criteria": [],
                    "hard_constraints": [],
                    "non_goals": [],
                    "relevant_context": [],
                    "open_questions": ["success_criteria_or_final_effect"],
                    "question_to_user": PRIMARY_QUESTION,
                    "confirmation_message": "",
                    "artifact_ready_for_persistence": False,
                }
            else:
                result = {
                    "task_type": "other",
                    "sufficient": True,
                    "user_request_summary": str(payload.get("user_text") or "").strip(),
                    "final_effect": str(payload.get("user_text") or "").strip(),
                    "observable_success_criteria": [str(payload.get("user_text") or "").strip()],
                    "hard_constraints": [],
                    "non_goals": [],
                    "relevant_context": [],
                    "open_questions": [],
                    "question_to_user": "",
                    "confirmation_message": "The information is sufficient for the next step. I understand the final effect. Should I proceed?",
                    "artifact_ready_for_persistence": True,
                }
            sys.stdout.write(json.dumps(result, ensure_ascii=False))
            """
        ),
        encoding="utf-8",
    )
    path.chmod(0o755)


def _run_front_half_without_jsonschema() -> tuple[int, str, str]:
    with tempfile.TemporaryDirectory(prefix="loop_product_front_half_") as td:
        temp_root = Path(td)
        session_root = temp_root / "session"
        blocker = temp_root / "jsonschema.py"
        fake_policy = temp_root / "fake_policy.py"
        blocker.write_text("raise ModuleNotFoundError('blocked jsonschema for front-half smoke')\n", encoding="utf-8")
        _write_fake_policy_script(fake_policy)
        env = dict(os.environ)
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(temp_root) if not existing else f"{temp_root}{os.pathsep}{existing}"
        env["LOOP_ENDPOINT_POLICY_CMD"] = f"{sys.executable} {fake_policy}"
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "loop_product.endpoint.clarification",
                "start",
                "--session-root",
                str(session_root),
                "--user-text",
                "I want a calm notes page that feels obvious immediately, and clutter would feel wrong.",
            ],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env=env,
        )
        return proc.returncode, proc.stdout, proc.stderr


def _run_evaluator_help_without_jsonschema() -> tuple[int, str, str]:
    with tempfile.TemporaryDirectory(prefix="loop_product_back_half_help_") as td:
        temp_root = Path(td)
        blocker = temp_root / "jsonschema.py"
        blocker.write_text("raise ModuleNotFoundError('blocked jsonschema for back-half help smoke')\n", encoding="utf-8")
        env = dict(os.environ)
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(temp_root) if not existing else f"{temp_root}{os.pathsep}{existing}"
        proc = subprocess.run(
            [sys.executable, "-m", "loop_product.evaluator.prototype", "--help"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env=env,
        )
        return proc.returncode, proc.stdout, proc.stderr


def _run_simple_evaluator_help_without_jsonschema() -> tuple[int, str, str]:
    with tempfile.TemporaryDirectory(prefix="loop_product_simple_eval_help_") as td:
        temp_root = Path(td)
        blocker = temp_root / "jsonschema.py"
        blocker.write_text("raise ModuleNotFoundError('blocked jsonschema for simple evaluator help')\n", encoding="utf-8")
        env = dict(os.environ)
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(temp_root) if not existing else f"{temp_root}{os.pathsep}{existing}"
        proc = subprocess.run(
            [sys.executable, "-m", "loop_product.evaluator.simple", "--help"],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            env=env,
        )
        return proc.returncode, proc.stdout, proc.stderr


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import loop_product as loop_product_package
        import loop_product.endpoint.clarification as endpoint_clarification
        import loop_product.evaluator.simple as evaluator_simple
        import loop_product.endpoint.policy as endpoint_policy
        import loop_product.evaluator.prototype as evaluator_prototype
        import loop_product.runtime.lifecycle as runtime_lifecycle
    except Exception as exc:  # noqa: BLE001
        return _fail(f"standalone imports failed: {exc}")

    for attr_name in (
        "run_evaluator",
        "build_endpoint_clarification_part1_input",
        "bootstrap_first_implementer_node",
        "bootstrap_first_implementer_from_endpoint",
        "child_runtime_status_from_launch_result_ref",
        "recover_orphaned_active_node",
        "supervise_child_until_settled",
    ):
        if not hasattr(loop_product_package, attr_name):
            return _fail(f"loop_product package root must expose formal alias `{attr_name}`")
    for rel_path in (
        ROOT / "loop_product" / "endpoint" / "__init__.py",
        ROOT / "loop_product" / "endpoint" / "clarification.py",
        ROOT / "loop_product" / "endpoint" / "policy.py",
        ROOT / "loop_product" / "evaluator" / "__init__.py",
        ROOT / "loop_product" / "evaluator" / "prototype.py",
        ROOT / "loop_product" / "evaluator" / "simple.py",
        ROOT / "loop_product" / "dispatch" / "bootstrap.py",
    ):
        if not rel_path.exists():
            return _fail(f"standalone repo must keep endpoint/simple implementations under dedicated subpackages: missing {rel_path}")

    for required_attr in (
        "evaluate_start_policy",
        "evaluate_continue_policy",
        "normalize_start_policy_decision",
        "normalize_continue_policy_decision",
    ):
        if not hasattr(endpoint_policy, required_attr):
            return _fail(f"loop_product.endpoint.policy must expose `{required_attr}`")
    if not hasattr(endpoint_clarification, "apply_endpoint_clarification_decision"):
        return _fail("loop_product.endpoint.clarification must expose `apply_endpoint_clarification_decision`")

    for module_name, module_obj in (
        ("loop_product.endpoint.clarification", endpoint_clarification),
        ("loop_product.evaluator.simple", evaluator_simple),
        ("loop_product.evaluator.prototype", evaluator_prototype),
        ("loop_product.runtime.lifecycle", runtime_lifecycle),
    ):
        if not hasattr(module_obj, "main"):
            return _fail(f"{module_name} must expose a main() entrypoint")
        rc, stdout, stderr = _run_help(module_name)
        if rc != 0:
            return _fail(f"`python -m {module_name} --help` must succeed; stderr={stderr.strip()!r}")
        if not stdout.strip():
            return _fail(f"`python -m {module_name} --help` must print usage text")

    rc, stdout, stderr = _run_front_half_without_jsonschema()
    if rc != 0:
        return _fail(
            "front-half CLI must stay runnable when evaluator-only jsonschema imports are blocked; "
            f"stderr={stderr.strip()!r}"
        )
    if "Endpoint artifact:" not in stdout:
        return _fail("front-half blocked-jsonschema smoke must still print the endpoint artifact location")

    rc, stdout, stderr = _run_evaluator_help_without_jsonschema()
    if rc != 0:
        return _fail(
            "back-half help must stay runnable when jsonschema is blocked until execution needs it; "
            f"stderr={stderr.strip()!r}"
        )
    if "loop_product.evaluator.prototype" not in stdout and "usage:" not in stdout.lower():
        return _fail("back-half blocked-jsonschema help smoke must still print usage text")

    rc, stdout, stderr = _run_simple_evaluator_help_without_jsonschema()
    if rc != 0:
        return _fail(
            "simple evaluator help must stay runnable when jsonschema is blocked until execution needs it; "
            f"stderr={stderr.strip()!r}"
        )
    if "loop_product.evaluator.simple" not in stdout and "usage:" not in stdout.lower():
        return _fail("simple evaluator blocked-jsonschema help smoke must still print usage text")
    if "--mode" in stdout:
        return _fail("simple evaluator help must not expose a raw|supervised compatibility split")
    if "prototype request JSON file" in stdout:
        return _fail("simple evaluator help must describe a formal evaluator request, not a prototype request")

    if (ROOT / "loop_product" / "evaluator_simple_prototype.py").exists():
        return _fail("standalone repo must remove the root simple-evaluator shim once subpackage cutover is complete")
    if (ROOT / "loop_product" / "endpoint_clarification.py").exists():
        return _fail("standalone repo must remove the root endpoint shim once endpoint subpackage cutover is complete")

    simple_source = (ROOT / "loop_product" / "evaluator" / "simple.py").read_text(encoding="utf-8")
    if "ThreadPoolExecutor" in simple_source or "_lane_parallelism" in simple_source:
        return _fail("simple evaluator runtime must stay sequential and avoid scheduler/executor scaffolding")
    endpoint_source = (ROOT / "loop_product" / "endpoint" / "clarification.py").read_text(encoding="utf-8")
    if "def handle_endpoint_clarification_turn" not in endpoint_source:
        return _fail("front-half implementation must live in loop_product.endpoint.clarification")

    pyproject_text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    if 'loop-evaluator = "loop_product.evaluator.simple:main"' not in pyproject_text:
        return _fail("standalone repo pyproject must expose the formal `loop-evaluator` console script")
    if 'loop-endpoint-clarification = "loop_product.endpoint.clarification:main"' not in pyproject_text:
        return _fail("standalone repo pyproject must expose the endpoint console script from the endpoint subpackage")
    if "loop_product.evaluator_simple_prototype:main" in pyproject_text:
        return _fail("standalone repo pyproject must stop targeting the removed root simple-evaluator module")
    if "loop_product.endpoint_clarification:main" in pyproject_text:
        return _fail("standalone repo pyproject must stop targeting the removed root endpoint module")

    print("[loop-product-standalone] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
