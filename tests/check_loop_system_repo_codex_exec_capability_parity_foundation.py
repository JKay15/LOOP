#!/usr/bin/env python3
"""Focused Milestone 3 `codex exec` capability-parity harness."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-codex-exec-capability-parity-foundation][FAIL] {msg}", file=sys.stderr)
    return 2


def _run(script: Path) -> tuple[int, str]:
    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
    )
    output = (result.stdout or "") + (result.stderr or "")
    return result.returncode, output.strip()


def _cleanup_repo_services() -> dict[str, object]:
    from loop_product.runtime import cleanup_test_repo_services

    return dict(
        cleanup_test_repo_services(
            repo_root=ROOT,
            settle_timeout_s=4.0,
            poll_interval_s=0.05,
            temp_test_repo_roots=[],
        )
        or {}
    )


def main() -> int:
    matrix_ref = ROOT / "docs" / "contracts" / "LOOP_SYSTEM_CODEX_EXEC_CAPABILITY_PARITY_MATRIX.md"
    matrix_text = matrix_ref.read_text(encoding="utf-8")
    for needle in (
        "CE-001",
        "CE-002",
        "CE-003",
        "CE-004",
        "CE-007",
    ):
        if needle not in matrix_text:
            return _fail(f"parity matrix must keep frozen scenario id {needle}")

    scenarios = [
        (
            "quota_resume",
            [
                ROOT / "tests" / "check_loop_system_repo_runtime_recovery.py",
                ROOT / "tests" / "check_loop_system_repo_retryable_recovery_status_refresh.py",
            ],
        ),
        (
            "sidecar_loss_and_truthful_downgrade",
            [
                ROOT / "tests" / "check_loop_system_repo_runtime_loss_confirmed_event.py",
                ROOT / "tests" / "check_loop_system_repo_host_child_runtime_status_supervisor.py",
            ],
        ),
        (
            "wrong_pid_and_identity_continuity",
            [
                ROOT / "tests" / "check_loop_system_repo_launch_reuse_pid_identity.py",
                ROOT / "tests" / "check_loop_system_repo_host_child_launch_supervisor.py",
            ],
        ),
        (
            "truthful_nonlive_without_exact_exit_observation",
            [
                ROOT / "tests" / "check_loop_system_repo_runtime_loss_confirmed_event.py",
                ROOT / "tests" / "check_loop_system_repo_host_child_runtime_status_supervisor.py",
            ],
        ),
    ]

    script_to_scenarios: dict[Path, list[str]] = {}
    for scenario_id, scripts in scenarios:
        for script in scripts:
            if not script.exists():
                return _fail(f"{scenario_id} evidence script missing: {script}")
            script_to_scenarios.setdefault(script, []).append(scenario_id)

    for script, scenario_ids in script_to_scenarios.items():
        initial_cleanup = _cleanup_repo_services()
        if not bool(initial_cleanup.get("quiesced")):
            joined = ", ".join(scenario_ids)
            return _fail(f"{joined} preflight cleanup must quiesce repo services before {script.name}: {initial_cleanup}")
        code, output = _run(script)
        if code != 0:
            joined = ", ".join(scenario_ids)
            return _fail(
                f"{joined} regressed via {script.name}: "
                f"{output or f'exit={code}'}"
            )

    print("[loop-system-codex-exec-capability-parity-foundation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
