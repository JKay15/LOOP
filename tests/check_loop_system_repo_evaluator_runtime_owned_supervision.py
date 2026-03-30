#!/usr/bin/env python3
"""Validate runtime-owned evaluator supervision semantics."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-runtime-owned-supervision][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_submission(path: Path, *, evaluator_node_id: str, target_node_id: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "evaluation_id": f"eval-{target_node_id}",
                "evaluator_node_id": evaluator_node_id,
                "target_node_id": target_node_id,
                "parent_node_id": "root-kernel",
                "generation": 1,
                "round_id": "R1",
                "lineage_ref": f"lineage::{target_node_id}",
                "goal_slice": f"goal::{target_node_id}",
                "workspace_root": str((path.parent / "workspace").resolve()),
                "output_root": str((path.parent / "output").resolve()),
                "implementation_package_ref": str((path.parent / "implementation").resolve()),
                "product_manual_ref": str((path.parent / "PRODUCT_MANUAL.md").resolve()),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def main() -> int:
    try:
        import loop_product.loop.evaluator_supervision as supervision_module  # type: ignore
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import evaluator supervision helpers: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_eval_runtime_supervision_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        submission_a = _write_submission(
            temp_root / "artifacts" / "evaluator_nodes" / "node-a" / "R1" / "EvaluatorNodeSubmission.json",
            evaluator_node_id="node-a__evaluator",
            target_node_id="node-a",
        )
        submission_b = _write_submission(
            temp_root / "artifacts" / "evaluator_nodes" / "node-b" / "R1" / "EvaluatorNodeSubmission.json",
            evaluator_node_id="node-b__evaluator",
            target_node_id="node-b",
        )

        original_run_until_terminal = supervision_module.run_evaluator_node_until_terminal
        original_load_submission = supervision_module._load_submission_ref
        original_sleep = supervision_module.time.sleep

        attempt_counter = {"count": 0}

        def _fake_load_submission(_submission_ref: str | Path):
            return object()

        def _fake_run_until_terminal(**kwargs):
            del kwargs
            attempt_counter["count"] += 1
            if attempt_counter["count"] == 1:
                return (
                    EvaluatorResult(
                        verdict=EvaluatorVerdict.STUCK,
                        lane="reviewer",
                        summary="synthetic retryable provider-capacity result",
                        retryable=True,
                        diagnostics={"issue_kind": "provider_capacity"},
                        recovery={"required": True, "mode": "SAME_RUN_RESUME"},
                    ),
                    {"run_state_ref": str((state_root / "synthetic" / "EvaluatorRunState.json").resolve())},
                )
            return (
                EvaluatorResult(
                    verdict=EvaluatorVerdict.PASS,
                    lane="reviewer",
                    summary="synthetic terminal pass",
                    retryable=False,
                    recovery={},
                ),
                {"run_state_ref": str((state_root / "synthetic" / "EvaluatorRunState.json").resolve())},
            )

        supervision_module._load_submission_ref = _fake_load_submission
        supervision_module.run_evaluator_node_until_terminal = _fake_run_until_terminal
        supervision_module.time.sleep = lambda _seconds: None
        try:
            result, _refs = supervision_module.run_submission_under_runtime_supervision(
                state_root=state_root,
                submission_ref=submission_a,
                max_same_run_recovery_attempts=2,
            )
        finally:
            supervision_module._load_submission_ref = original_load_submission
            supervision_module.run_evaluator_node_until_terminal = original_run_until_terminal
            supervision_module.time.sleep = original_sleep

        if result.verdict.value != "PASS":
            return _fail("runtime-owned evaluator supervision must keep retrying until a terminal PASS lands")
        if attempt_counter["count"] != 2:
            return _fail("runtime-owned evaluator supervision must re-arm the same submission after a retryable result")

        repo_supervision_root = supervision_module.repo_evaluator_supervision_root(state_root=state_root)
        runtime_ref = supervision_module.repo_evaluator_supervision_runtime_ref(
            repo_evaluator_supervision_root=repo_supervision_root
        )

        original_live_runtime = supervision_module.live_repo_evaluator_supervision_runtime
        original_popen = supervision_module.subprocess.Popen
        popen_argvs: list[list[str]] = []

        class _FakeProc:
            def poll(self) -> None:
                return None

        def _fake_live_runtime(*, repo_evaluator_supervision_root: str | Path):
            marker = supervision_module.repo_evaluator_supervision_runtime_ref(
                repo_evaluator_supervision_root=repo_evaluator_supervision_root
            )
            if marker.exists():
                return json.loads(marker.read_text(encoding="utf-8"))
            return None

        def _fake_popen(argv, **kwargs):
            del kwargs
            normalized_argv = [str(item) for item in argv]
            popen_argvs.append(normalized_argv)
            if "--run-repo-evaluator-supervision-reactor" not in normalized_argv:
                raise AssertionError(f"expected repo evaluator reactor launch argv, got {normalized_argv!r}")
            supervision_module._write_json(
                runtime_ref,
                {
                    "schema": "loop_product.repo_scoped_evaluator_supervision_runtime",
                    "runtime_kind": "repo_scoped_evaluator_supervision_reactor",
                    "repo_evaluator_supervision_root": str(repo_supervision_root),
                    "pid": os.getpid(),
                    "started_at_utc": "2026-03-29T00:00:00+00:00",
                    "tracked_submission_refs": [],
                },
            )
            return _FakeProc()

        supervision_module.live_repo_evaluator_supervision_runtime = _fake_live_runtime
        supervision_module.subprocess.Popen = _fake_popen
        try:
            payload_a = supervision_module.ensure_evaluator_supervision_running(
                state_root=state_root,
                submission_ref=submission_a,
                startup_timeout_s=1.0,
            )
            payload_b = supervision_module.ensure_evaluator_supervision_running(
                state_root=state_root,
                submission_ref=submission_b,
                startup_timeout_s=1.0,
            )
        finally:
            supervision_module.live_repo_evaluator_supervision_runtime = original_live_runtime
            supervision_module.subprocess.Popen = original_popen

        if len(popen_argvs) != 1:
            return _fail("two evaluator submissions in one repo root must spawn exactly one repo-scoped reactor")
        if str(payload_a.get("runtime_kind") or "") != "repo_scoped_evaluator_supervision":
            return _fail("first evaluator payload must advertise repo-scoped evaluator supervision")
        if str(payload_b.get("runtime_kind") or "") != "repo_scoped_evaluator_supervision":
            return _fail("second evaluator payload must advertise repo-scoped evaluator supervision")

        registry = supervision_module._load_repo_evaluator_supervision_registry(repo_supervision_root)
        tracked = sorted(str(item) for item in dict(registry.get("tracked_submissions") or {}).keys())
        expected = sorted([str(submission_a.resolve()), str(submission_b.resolve())])
        if tracked != expected:
            return _fail(f"repo-scoped evaluator registry must track both submissions, got {tracked!r}")

        original_run_until_terminal = supervision_module.run_evaluator_node_until_terminal
        original_load_submission = supervision_module._load_submission_ref
        original_load_latest_run_state = supervision_module.load_latest_evaluator_run_state
        original_now_utc = supervision_module._now_utc
        original_sleep = supervision_module.time.sleep
        prelaunch_sleep_calls: list[float] = []

        def _fake_load_submission_with_target(_submission_ref: str | Path):
            return SimpleNamespace(target_node_id="node-a")

        fixed_now = datetime(2026, 3, 29, 2, 40, 0, tzinfo=timezone.utc)

        def _fake_load_latest_run_state(*, state_root: Path, node_id: str):
            del state_root
            del node_id
            return (
                temp_root / ".loop" / "synthetic" / "EvaluatorRunState.json",
                {
                    "status": "RECOVERY_REQUIRED",
                    "checker": {
                        "attempt_count": 16,
                        "status": "BLOCKED_RETRYABLE",
                        "error": {
                            "issue_kind": "provider_capacity",
                            "message": "2026-03-29T02:40:00.000000Z ERROR codex_core::models_manager::manager: failed to refresh available models: We're currently experiencing high demand, which may cause temporary errors.",
                        },
                    },
                },
            )

        def _fake_run_until_terminal_after_cooldown(**kwargs):
            del kwargs
            return (
                EvaluatorResult(
                    verdict=EvaluatorVerdict.PASS,
                    lane="reviewer",
                    summary="terminal pass after prelaunch cooldown",
                    retryable=False,
                    recovery={},
                ),
                {"run_state_ref": str((state_root / "synthetic" / "EvaluatorRunState.json").resolve())},
            )

        supervision_module._load_submission_ref = _fake_load_submission_with_target
        supervision_module.load_latest_evaluator_run_state = _fake_load_latest_run_state
        supervision_module.run_evaluator_node_until_terminal = _fake_run_until_terminal_after_cooldown
        supervision_module._now_utc = lambda: fixed_now
        supervision_module.time.sleep = prelaunch_sleep_calls.append
        try:
            cooled_result, _refs = supervision_module.run_submission_under_runtime_supervision(
                state_root=state_root,
                submission_ref=submission_a,
                max_same_run_recovery_attempts=2,
            )
        finally:
            supervision_module._load_submission_ref = original_load_submission
            supervision_module.load_latest_evaluator_run_state = original_load_latest_run_state
            supervision_module.run_evaluator_node_until_terminal = original_run_until_terminal
            supervision_module._now_utc = original_now_utc
            supervision_module.time.sleep = original_sleep

        if cooled_result.verdict.value != "PASS":
            return _fail("provider-capacity cooldown path must still return the terminal evaluator result")
        if prelaunch_sleep_calls != [30.0]:
            return _fail(
                "runtime-owned evaluator supervision must honor persisted provider-capacity cooldown "
                f"before cold-starting again, got sleeps {prelaunch_sleep_calls!r}"
            )

    print("[loop-system-evaluator-runtime-owned-supervision] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
