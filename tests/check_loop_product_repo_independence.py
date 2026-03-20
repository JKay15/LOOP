#!/usr/bin/env python3
"""Deterministic independence proof for the standalone LOOP product repo."""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _write_report(path: Path | None, report: dict[str, object]) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _ok(label: str, detail: str) -> dict[str, str]:
    return {"check_id": label, "status": "PASS", "detail": detail}


def _fail(label: str, detail: str) -> dict[str, str]:
    return {"check_id": label, "status": "FAIL", "detail": detail}


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(ROOT), text=True, capture_output=True)


def _scan_runtime_imports() -> list[dict[str, str]]:
    checks: list[dict[str, str]] = []
    disallowed_roots = {"leanatlas"}
    package_root = ROOT / "loop_product"
    for path in sorted(package_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    root = str(alias.name).split(".", 1)[0]
                    if root in disallowed_roots:
                        checks.append(
                            _fail(
                                "no_leanatlas_runtime_imports",
                                f"{path.relative_to(ROOT)} imports forbidden module root `{root}`",
                            )
                        )
                        return checks
            if isinstance(node, ast.ImportFrom):
                if node.level:
                    continue
                module = str(node.module or "")
                root = module.split(".", 1)[0]
                if root in disallowed_roots:
                    checks.append(
                        _fail(
                            "no_leanatlas_runtime_imports",
                            f"{path.relative_to(ROOT)} imports forbidden module root `{root}`",
                        )
                    )
                    return checks
        text = path.read_text(encoding="utf-8")
        if any(
            pattern in text
            for pattern in (
                '_SHARED_PRODUCT_REPO_DIRNAME = "loop_product_repo"',
                "_SHARED_PRODUCT_REPO_DIRNAME = 'loop_product_repo'",
                'Path("loop_product_repo")',
                "Path('loop_product_repo')",
            )
        ):
            checks.append(
                _fail(
                    "no_checkout_dir_coupling",
                    f"{path.relative_to(ROOT)} hardcodes `loop_product_repo` instead of staying repo-neutral",
                )
            )
            return checks
    checks.append(
        _ok(
            "no_leanatlas_runtime_imports",
            "Core loop_product package files do not require runtime imports from LeanAtlas internals.",
        )
    )
    return checks


def _scan_neutral_docs() -> list[dict[str, str]]:
    checks: list[dict[str, str]] = []
    targets = (
        ROOT / "docs" / "contracts" / "LOOP_ENDPOINT_CLARIFICATION_PART1_FINAL_EFFECTS.md",
        ROOT / "docs" / "contracts" / "LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_CONTRACT.md",
        ROOT / "docs" / "contracts" / "LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_PRODUCT_MANUAL.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_FINAL_EFFECTS.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_CONTRACT.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
        ROOT / "docs" / "schemas" / "LoopEvaluatorPrototypeInput.schema.json",
        ROOT / "docs" / "schemas" / "LoopEvaluatorPrototypeReport.schema.json",
        ROOT / "docs" / "schemas" / "LoopEvaluatorPrototypeSupervisorReport.schema.json",
    )
    forbidden = (
        "/Users/xiongjiangkai/xjk_papers/leanatlas",
        ".cache/leanatlas",
        "leanatlas.loop_evaluator_prototype",
        "https://leanatlas.dev/schemas/",
    )
    for path in targets:
        text = path.read_text(encoding="utf-8")
        for snippet in forbidden:
            if snippet in text:
                checks.append(
                    _fail(
                        "neutral_product_surfaces",
                        f"{path.relative_to(ROOT)} still contains forbidden LeanAtlas-specific snippet `{snippet}`",
                    )
                )
                return checks
    checks.append(
        _ok(
            "neutral_product_surfaces",
            "Standalone manuals, contracts, and schemas avoid LeanAtlas-specific absolute paths and schema ids.",
        )
    )
    return checks


def _check_required_surfaces() -> list[dict[str, str]]:
    required_paths = (
        ROOT / "pyproject.toml",
        ROOT / "uv.lock",
        ROOT / "README.md",
        ROOT / "AGENTS.md",
        ROOT / "loop_product" / "__init__.py",
        ROOT / "loop_product" / "endpoint" / "__init__.py",
        ROOT / "loop_product" / "endpoint" / "clarification.py",
        ROOT / "loop_product" / "endpoint" / "policy.py",
        ROOT / "loop_product" / "evaluator" / "__init__.py",
        ROOT / "loop_product" / "evaluator" / "prototype.py",
        ROOT / "loop_product" / "evaluator" / "simple.py",
        ROOT / "tests" / "check_loop_product_repo_standalone.py",
        ROOT / "docs" / "contracts" / "LOOP_ENDPOINT_CLARIFICATION_PART1_FINAL_EFFECTS.md",
        ROOT / "docs" / "contracts" / "LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_CONTRACT.md",
        ROOT / "docs" / "contracts" / "LOOP_ENDPOINT_CLARIFICATION_HOST_INTEGRATION_PRODUCT_MANUAL.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_FINAL_EFFECTS.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_CONTRACT.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_HOST_INTEGRATION_PRODUCT_MANUAL.md",
        ROOT / "docs" / "contracts" / "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
    )
    for path in required_paths:
        if not path.exists():
            return [
                _fail(
                    "shared_repo_surfaces",
                    f"Required standalone product surface missing: {path.relative_to(ROOT)}",
                )
            ]
    evaluator_removed_path = ROOT / "loop_product" / "evaluator_simple_prototype.py"
    if evaluator_removed_path.exists():
        return [
            _fail(
                "shared_repo_surfaces",
                f"Deprecated root shim must be absent after subpackage cutover: {evaluator_removed_path.relative_to(ROOT)}",
            )
        ]
    endpoint_shim = ROOT / "loop_product" / "endpoint_clarification.py"
    if endpoint_shim.exists():
        return [
            _fail(
                "shared_repo_surfaces",
                f"Deprecated root endpoint shim must be absent after subpackage cutover: {endpoint_shim.relative_to(ROOT)}",
            )
        ]
    return [
        _ok(
            "shared_repo_surfaces",
            "front-half and back-half product surfaces, manuals, tests, schemas, and host rules all live inside the same standalone repo.",
        )
    ]


def _check_formal_entry_aliases() -> list[dict[str, str]]:
    pyproject_text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    if 'loop-evaluator = "loop_product.evaluator.simple:main"' not in pyproject_text:
        return [
            _fail(
                "formal_backhalf_entry_aliases",
                "Standalone product repo must expose `loop-evaluator` as the formal thin-evaluator console script.",
            )
        ]
    if 'loop-endpoint-clarification = "loop_product.endpoint.clarification:main"' not in pyproject_text:
        return [
            _fail(
                "formal_backhalf_entry_aliases",
                "Standalone product repo must expose the formal endpoint console script from the endpoint subpackage.",
            )
        ]
    readme_text = (ROOT / "README.md").read_text(encoding="utf-8")
    if "run_evaluator" not in readme_text:
        return [
            _fail(
                "formal_backhalf_entry_aliases",
                "Standalone product repo README must expose the formal package-root evaluator alias.",
            )
        ]
    return [
        _ok(
            "formal_backhalf_entry_aliases",
            "The standalone repo exposes formal evaluator aliases while keeping prototype names only as compatibility surfaces.",
        )
    ]


def _check_root_agents_surface() -> list[dict[str, str]]:
    path = ROOT / "AGENTS.md"
    if not path.exists():
        return [
            _fail(
                "root_host_rules",
                "Standalone product repo must ship a root AGENTS.md so staged copies keep their Codex host routing rules.",
            )
        ]
    text = path.read_text(encoding="utf-8")
    for snippet in (
        "ordinary conversation",
        "sufficiently clear",
        "clarification agent before `kernel mode`",
        "must itself perform the clarification reasoning",
        "persist or update the clarification session",
        "`apply-decision`",
        "must not merely imitate clarification behavior in free-form conversation",
        "same user turn already explicitly authorizes proceeding now",
        "switch into `kernel mode`",
        "must explicitly say that it is now in `kernel mode`",
        "docs/contracts/LOOP_SYSTEM_KERNEL_CONTRACT.md",
        "docs/contracts/LOOP_SYSTEM_NODE_CONTRACT.md",
        ".agents/skills/child-dispatch/SKILL.md",
        "project folder under `workspace/`",
        "workspace_root",
        "current working directory",
        "must not directly implement",
        "must first materialize exactly one LOOP node",
        "terminal evaluator-backed result",
        "must not run its own acceptance, testing, inspection, or evaluator-like checking",
    ):
        if snippet not in text:
            return [
                _fail(
                    "root_host_rules",
                    f"Standalone product repo root AGENTS.md missing required reusable host-rule snippet `{snippet}`.",
                )
            ]
    return [
        _ok(
            "root_host_rules",
            "The standalone repo ships a reusable root AGENTS.md so staged copies preserve clarification gating and kernel-entry behavior.",
        )
    ]


def _check_standalone_smoke() -> list[dict[str, str]]:
    proc = _run([sys.executable, "tests/check_loop_product_repo_standalone.py"])
    if proc.returncode != 0:
        return [
            _fail(
                "standalone_run_test_reuse",
                f"Standalone smoke failed with rc={proc.returncode}: {proc.stderr.strip() or proc.stdout.strip()}",
            )
        ]
    return [
        _ok(
            "standalone_run_test_reuse",
            "The standalone repo can import and execute both front-half and back-half entrypoints without LeanAtlas runtime support.",
        )
    ]


def _check_adapter_snapshot(adapter_snapshot: Path | None) -> list[dict[str, str]]:
    if adapter_snapshot is None:
        return []
    evaluator_wrapper = adapter_snapshot / "evaluator_prototype.py"
    endpoint_wrapper = adapter_snapshot / "endpoint_clarification.py"
    for path in (evaluator_wrapper, endpoint_wrapper):
        if not path.exists():
            return [
                _fail(
                    "leanatlas_adapter_boundary",
                    f"Adapter snapshot is missing expected wrapper file {path.relative_to(ROOT if path.is_relative_to(ROOT) else adapter_snapshot)}",
                )
            ]
    evaluator_text = evaluator_wrapper.read_text(encoding="utf-8")
    endpoint_text = endpoint_wrapper.read_text(encoding="utf-8")
    for snippet in ("loop_product_repo", "loop_product.evaluator.prototype"):
        if snippet not in evaluator_text:
            return [
                _fail(
                    "leanatlas_adapter_boundary",
                    f"LeanAtlas evaluator adapter snapshot must delegate through `{snippet}`",
                )
            ]
    for snippet in ("loop_product_repo", "loop_product.endpoint.clarification"):
        if snippet not in endpoint_text:
            return [
                _fail(
                    "leanatlas_adapter_boundary",
                    f"LeanAtlas endpoint adapter snapshot must delegate through `{snippet}`",
                )
            ]
    return [
        _ok(
            "leanatlas_adapter_boundary",
            "The supplied LeanAtlas wrappers are thin adapters over the standalone product repo rather than the permanent implementation home.",
        )
    ]


def _check_self_eval_request(self_eval_request: Path | None) -> list[dict[str, str]]:
    if self_eval_request is None:
        return []
    request_path = self_eval_request.resolve()
    try:
        request_rel = request_path.relative_to(ROOT)
    except ValueError:
        return [
            _fail(
                "repo_shipped_self_eval_inputs",
                f"Self-eval request must live inside the standalone repo root; got {request_path}",
            )
        ]
    if not request_path.exists():
        return [
            _fail(
                "repo_shipped_self_eval_inputs",
                f"Self-eval request is missing: {request_rel}",
            )
        ]
    if tuple(request_rel.parts[:2]) != (".cache", "loop_evaluator_self_eval"):
        return [
            _fail(
                "repo_shipped_self_eval_inputs",
                f"Self-eval request must live under .cache/loop_evaluator_self_eval/**; got {request_rel}",
            )
        ]
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    resolved_refs: dict[str, Path] = {}
    for field in ("product_manual_ref", "final_effects_text_ref"):
        raw_ref = str(payload.get(field) or "").strip()
        if not raw_ref:
            return [
                _fail(
                    "repo_shipped_self_eval_inputs",
                    f"Self-eval request is missing `{field}`.",
                )
            ]
        ref_path = Path(raw_ref)
        if ref_path.is_absolute():
            return [
                _fail(
                    "repo_shipped_self_eval_inputs",
                    f"Self-eval request `{field}` must stay repo-relative, got absolute path `{raw_ref}`.",
                )
            ]
        ref_rel = Path(raw_ref)
        if tuple(ref_rel.parts[:3]) != (".cache", "loop_evaluator_self_eval", "inputs"):
            return [
                _fail(
                    "repo_shipped_self_eval_inputs",
                    f"Self-eval request `{field}` must resolve inside .cache/loop_evaluator_self_eval/inputs/**; got `{raw_ref}`.",
                )
            ]
        resolved_ref = (ROOT / ref_rel).resolve()
        if not resolved_ref.exists():
            return [
                _fail(
                    "repo_shipped_self_eval_inputs",
                    f"Self-eval request `{field}` points at a missing repo-shipped file: {ref_rel}",
                )
            ]
        resolved_refs[field] = resolved_ref
    return [
        _ok(
            "repo_shipped_self_eval_inputs",
            "The self-eval request plus its product_manual_ref and final_effects_text_ref resolve to repo-shipped .cache/loop_evaluator_self_eval/inputs/** files.",
        )
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report-path", help="Optional JSON report output path.")
    parser.add_argument("--adapter-snapshot", help="Optional session-local snapshot of LeanAtlas adapter wrappers.")
    parser.add_argument(
        "--self-eval-request",
        help="Optional session-local self-eval request JSON whose repo-shipped inputs should be verified.",
    )
    args = parser.parse_args()

    checks: list[dict[str, str]] = []
    checks.extend(_check_required_surfaces())
    checks.extend(_check_formal_entry_aliases())
    checks.extend(_check_root_agents_surface())
    checks.extend(_check_standalone_smoke())
    checks.extend(_scan_runtime_imports())
    checks.extend(_scan_neutral_docs())
    adapter_snapshot = Path(args.adapter_snapshot).resolve() if args.adapter_snapshot else None
    checks.extend(_check_adapter_snapshot(adapter_snapshot))
    self_eval_request = Path(args.self_eval_request).resolve() if args.self_eval_request else None
    checks.extend(_check_self_eval_request(self_eval_request))

    failing = [item for item in checks if item["status"] != "PASS"]
    report = {
        "schema": "loop_product.loop_product_repo_independence_report",
        "schema_version": "1.0.0",
        "status": "FAIL" if failing else "PASS",
        "repo_root": str(ROOT),
        "checks": checks,
    }
    _write_report(Path(args.report_path) if args.report_path else None, report)
    if failing:
        first = failing[0]
        print(f"[loop-product-independence][FAIL] {first['detail']}", file=sys.stderr)
        return 2
    print("[loop-product-independence] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
