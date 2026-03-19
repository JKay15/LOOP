#!/usr/bin/env python3
"""Validate the six minimal skill skeletons from handoff section 5."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SKILLS = ROOT / ".agents" / "skills"


def _fail(msg: str) -> int:
    print(f"[loop-system-skill-skeletons][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_skill(name: str) -> str:
    path = SKILLS / name / "SKILL.md"
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8")


def main() -> int:
    required = {
        "loop-runner": [
            "goal slice",
            "implementation",
            "evaluator",
            "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
            "EvaluatorNodeSubmission",
            "initialize_evaluator_runtime",
            "run_evaluator_node",
            "request_ref",
            "evaluation_report_ref",
            "verdict",
            "terminal evaluator result",
            "task-local evaluator role-agent scripts",
            "repo-shipped evaluator tools",
            "delivered_artifact_ref",
            "deterministic equality proof",
            "AI review status",
            "attempt evaluator",
            "request_ref: none",
            "evaluation_report_ref: none",
            "non-terminal",
            "fixable product defect",
            "external prerequisite blocker",
            "before claiming completion",
            "evaluator in flight",
            "short `wait_agent` timeout",
            "`wait_agent` timeout of at least 600000 ms",
            "10 minutes",
            "heartbeat window",
            "same evaluator attempt",
            "lower-fidelity fallback",
            "heartbeat ping",
            "phase",
            "`evaluator_state`",
            "`same_attempt`",
            "`stalled`",
            "`next_action`",
            "continue the same attempt",
            "local control",
            "Not responsible",
            "split",
        ],
        "child-dispatch": [
            "delegation",
            "execution policy",
            "child-local",
            "heartbeat",
            "status-only",
            "non-interrupting",
            "status packet",
            "phase",
            "`evaluator_state`",
            "`request_ref`",
            "`evaluation_report_ref`",
            "`same_attempt`",
            "`stalled`",
            "`next_action`",
            "whether evaluator has started",
            "terminal",
            "root kernel",
            "Not responsible",
        ],
        "split-review": [
            "split review",
            "split request",
            "S1",
            "S5",
            "Not responsible",
        ],
        "runtime-recovery": [
            "resume",
            "retry",
            "relaunch",
            "consistency",
            "Not responsible",
        ],
        "evaluator-exec": [
            "uv",
            "path",
            "repo structure",
            "implementation",
            "LOOP_EVALUATOR_PROTOTYPE_PRODUCT_MANUAL.md",
            "documented evaluator product surface",
            "initialize_evaluator_runtime",
            "run_evaluator_node",
            "request_ref",
            "evaluation_report_ref",
            "task-local evaluator role-agent scripts",
            "repo-shipped evaluator helpers",
            "evaluator in flight",
            "same evaluator attempt",
            "short `wait_agent` timeout",
            "`wait_agent` timeout of at least 600000 ms",
            "10 minutes",
            "heartbeat window",
            "lower-fidelity fallback",
            "AI plus rigid script",
            "Not responsible",
        ],
        "repo-hygiene": [
            "artifacts",
            "quarantine",
            "cleanup",
            "git",
            "experience assets",
            "cache",
            "durable state",
            "Not responsible",
        ],
    }

    try:
        shipped = {
            path.parent.name
            for path in SKILLS.glob("*/SKILL.md")
        }
        required_names = set(required)
        if shipped != required_names:
            extra = sorted(shipped - required_names)
            missing = sorted(required_names - shipped)
            raise ValueError(
                "skill set mismatch: "
                f"extra={extra or ['<none>']} missing={missing or ['<none>']}"
            )
        for skill_name, needles in required.items():
            text = _load_skill(skill_name)
            for needle in needles:
                if needle not in text:
                    raise ValueError(f"{skill_name}: {needle}")
    except (FileNotFoundError, ValueError) as exc:
        return _fail(f"skill skeleton missing required section-5 text: {exc}")

    print("[loop-system-skill-skeletons] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
