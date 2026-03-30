#!/usr/bin/env python3
"""Validate repo-scoped evaluator supervision process bootstrap without control-plane import loops."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-supervision-process-bootstrap][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _write_submission(path: Path) -> Path:
    return _write_json(
        path,
        {
            "evaluation_id": "eval-proofs",
            "evaluator_node_id": "Proofs__and__Discussions_formalization__evaluator",
            "target_node_id": "Proofs__and__Discussions_formalization",
            "parent_node_id": "root-kernel",
            "generation": 1,
            "round_id": "R1",
            "lineage_ref": "lineage::proofs",
            "goal_slice": "goal::proofs",
            "workspace_root": str((path.parent / "workspace").resolve()),
            "output_root": str((path.parent / "output").resolve()),
            "implementation_package_ref": str((path.parent / "implementation").resolve()),
            "product_manual_ref": str((path.parent / "PRODUCT_MANUAL.md").resolve()),
        },
    )


def main() -> int:
    try:
        import loop_product.loop.evaluator_supervision as supervision_module  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import evaluator supervision module: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_eval_supervision_bootstrap_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        submission_ref = _write_submission(
            temp_root / "artifacts" / "evaluator_nodes" / "Proofs__and__Discussions_formalization__evaluator" / "R1" / "EvaluatorNodeSubmission.json"
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

        supervision_module.register_submission_for_repo_evaluator_supervision(
            state_root=state_root,
            submission_ref=submission_ref,
            max_same_run_recovery_attempts=40,
        )
        queue_root = supervision_module.repo_evaluator_supervision_root(state_root=state_root)
        supervision_module.ensure_submission_provider_retry_queued(
            state_root=state_root,
            submission_ref=submission_ref,
            repo_evaluator_supervision_root=queue_root,
        )

        log_ref = supervision_module.repo_evaluator_supervision_log_ref(
            repo_evaluator_supervision_root=queue_root,
        )
        runtime_ref = supervision_module.repo_evaluator_supervision_runtime_ref(
            repo_evaluator_supervision_root=queue_root,
        )

        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "loop_product.loop.evaluator_supervision",
                "--run-repo-evaluator-supervision-reactor",
                "--repo-evaluator-supervision-root",
                str(queue_root),
            ],
            cwd=str(state_root),
            stdout=open(log_ref, "ab", buffering=0),
            stderr=open(log_ref, "ab", buffering=0),
            start_new_session=True,
        )
        try:
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if runtime_ref.exists():
                    break
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
            if not runtime_ref.exists():
                stderr = log_ref.read_text(encoding="utf-8") if log_ref.exists() else ""
                return _fail(f"repo evaluator supervision reactor failed to publish runtime marker; log={stderr}")
            payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
            if int(payload.get("pid") or 0) <= 0:
                return _fail("runtime marker must publish the live reactor pid")
        finally:
            try:
                os.kill(proc.pid, 15)
            except OSError:
                pass
            try:
                proc.wait(timeout=2.0)
            except Exception:
                try:
                    os.kill(proc.pid, 9)
                except OSError:
                    pass

    print("[loop-system-evaluator-supervision-process-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
