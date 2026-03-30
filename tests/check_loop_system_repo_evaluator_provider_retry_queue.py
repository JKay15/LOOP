#!/usr/bin/env python3
"""Validate durable provider-capacity queue behavior for evaluator supervision."""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-provider-retry-queue][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_submission(path: Path, *, evaluator_node_id: str, target_node_id: str) -> Path:
    return _write_json(
        path,
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
    )


def main() -> int:
    try:
        import loop_product.loop.evaluator_supervision as supervision_module  # type: ignore
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import evaluator supervision helpers: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_eval_provider_retry_queue_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        target_node_id = "Proofs__and__Discussions_formalization__evaluator"
        submission_ref = _write_submission(
            temp_root / "artifacts" / "evaluator_nodes" / target_node_id / "R1" / "EvaluatorNodeSubmission.json",
            evaluator_node_id=target_node_id,
            target_node_id="Proofs__and__Discussions_formalization",
        )
        _write_json(
            state_root
            / "artifacts"
            / "evaluator_runs"
            / "Proofs__and__Discussions_formalization"
            / "implementer_task_Proofs__and__Discussions_formalization"
            / "R1.S4__Proofs__and__Discussions_formalization__evaluator"
            / "EvaluatorRunState.json",
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
                "reviewer": {"attempt_count": 0, "status": "PENDING"},
            },
        )

        queue_root = supervision_module.repo_evaluator_supervision_root(state_root=state_root)
        supervision_module.register_submission_for_repo_evaluator_supervision(
            state_root=state_root,
            submission_ref=submission_ref,
        )

        fixed_now = datetime(2026, 3, 29, 2, 40, 0, tzinfo=timezone.utc)
        queue_payload = supervision_module.ensure_submission_provider_retry_queued(
            state_root=state_root,
            submission_ref=submission_ref,
            repo_evaluator_supervision_root=queue_root,
            now_utc=fixed_now,
        )

        queue_entry = dict((queue_payload.get("queued_submissions") or {}).get(str(submission_ref.resolve())) or {})
        if not queue_entry:
            return _fail("provider-capacity submission must be seeded into the durable provider retry queue")
        if str(queue_entry.get("issue_kind") or "") != "provider_capacity":
            return _fail("queue entry must preserve provider_capacity issue kind")
        if str(queue_entry.get("ready_after_utc") or "") != "2026-03-29T02:40:30+00:00":
            return _fail(
                "provider-capacity retry must use the flat short cooldown window, "
                f"got ready_after_utc={queue_entry.get('ready_after_utc')!r}"
            )

        before_due = supervision_module.pop_due_provider_retry_submission_refs(
            repo_evaluator_supervision_root=queue_root,
            now_utc=fixed_now + timedelta(seconds=29),
        )
        if before_due:
            return _fail(f"submission must not launch before cooldown expiry, got {before_due!r}")

        after_due = supervision_module.pop_due_provider_retry_submission_refs(
            repo_evaluator_supervision_root=queue_root,
            now_utc=fixed_now + timedelta(seconds=30),
        )
        expected_submission = str(submission_ref.resolve())
        if after_due != [expected_submission]:
            return _fail(
                "submission must become launchable exactly when cooldown expires, "
                f"got {after_due!r}"
            )

        remaining_queue = supervision_module.load_provider_retry_queue(
            repo_evaluator_supervision_root=queue_root,
        )
        if dict(remaining_queue.get("queued_submissions") or {}):
            return _fail("due queue entry must be removed once handed off for relaunch")

        _write_json(
            state_root
            / "artifacts"
            / "evaluator_runs"
            / "Proofs__and__Discussions_formalization"
            / "implementer_task_Proofs__and__Discussions_formalization"
            / "R1.S4__Proofs__and__Discussions_formalization__evaluator"
            / "EvaluatorRunState.json",
            {
                "status": "RECOVERY_REQUIRED",
                "checker": {
                    "attempt_count": 16,
                    "status": "BLOCKED_RETRYABLE",
                    "error": {
                        "issue_kind": "provider_capacity",
                        "message": "provider capacity retryable handoff",
                    },
                },
                "reviewer": {"attempt_count": 0, "status": "PENDING"},
            },
        )

        original_run_until_terminal = supervision_module.run_evaluator_node_until_terminal
        original_load_submission = supervision_module._load_submission_ref
        call_args: list[int] = []

        def _fake_load_submission(_submission_ref: str | Path):
            return type("Submission", (), {"target_node_id": "Proofs__and__Discussions_formalization"})()

        def _fake_run_until_terminal(**kwargs):
            raw_attempts = kwargs.get("max_same_run_recovery_attempts")
            call_args.append(-1 if raw_attempts is None else int(raw_attempts))
            return (
                EvaluatorResult(
                    verdict=EvaluatorVerdict.STUCK,
                    lane="checker",
                    summary="provider capacity",
                    retryable=True,
                    diagnostics={"issue_kind": "provider_capacity"},
                    recovery={"required": True, "mode": "SAME_RUN_RESUME"},
                ),
                {},
            )

        supervision_module._load_submission_ref = _fake_load_submission
        supervision_module.run_evaluator_node_until_terminal = _fake_run_until_terminal
        try:
            worker_states: dict[str, str] = {}
            supervision_module._submission_worker(
                state_root=state_root,
                submission_ref=submission_ref,
                repo_evaluator_supervision_root=queue_root,
                max_same_run_recovery_attempts=40,
                worker_states=worker_states,
            )
        finally:
            supervision_module._load_submission_ref = original_load_submission
            supervision_module.run_evaluator_node_until_terminal = original_run_until_terminal

        if call_args != [0]:
            return _fail(
                "provider-capacity worker handoff must disable inner same-run recovery and let the durable queue own retries, "
                f"got max_same_run_recovery_attempts calls {call_args!r}"
            )
        seeded_queue = supervision_module.load_provider_retry_queue(
            repo_evaluator_supervision_root=queue_root,
        )
        if not dict(seeded_queue.get("queued_submissions") or {}):
            return _fail("provider-capacity worker handoff must seed the durable retry queue")
        if worker_states.get(str(submission_ref.resolve())) != "queued":
            return _fail("provider-capacity worker handoff must leave the submission in queued worker state")

    print("[loop-system-evaluator-provider-retry-queue] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
