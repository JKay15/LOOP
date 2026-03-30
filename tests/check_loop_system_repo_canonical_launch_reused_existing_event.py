#!/usr/bin/env python3
"""Validate canonical launch_reused_existing events from the trusted launch reuse surface."""

from __future__ import annotations

import json
import os
import shutil
import stat
import signal
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-canonical-launch-reused-existing-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _safe_rmtree(path: Path) -> None:
    def _onerror(func, target, exc_info):
        target_path = Path(target)
        try:
            target_path.chmod(target_path.stat().st_mode | stat.S_IWUSR)
        except OSError:
            pass
        try:
            func(target)
        except OSError:
            pass

    shutil.rmtree(path, onerror=_onerror)


def main() -> int:
    from loop_product.dispatch.launch_runtime import (
        launch_child_from_result_ref as trusted_launch_child_from_result_ref,
        terminate_runtime_owned_launch_result_ref,
    )
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        cleanup_test_repo_services,
        cleanup_test_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_canonical_launch_reused_existing_event_") as td:
        temp_root = Path(td)
        endpoint = temp_root / "EndpointArtifact.json"
        endpoint.write_text(
            json.dumps(
                {
                    "version": "1",
                    "session_root": str((temp_root / "endpoint_session").resolve()),
                    "artifact_ref": str(endpoint.resolve()),
                    "latest_turn_ref": str((temp_root / "turns" / "0001" / "TurnResult.json").resolve()),
                    "mode": "VISION_COMPILER",
                    "status": "CLARIFIED",
                    "original_user_prompt": "Bootstrap one launch-reuse test node.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "design",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Bootstrap one launch-reuse test node.",
                        "final_effect": "Bootstrap one launch-reuse test node.",
                        "observable_success_criteria": ["One live child can be launched and then reused."],
                        "hard_constraints": ["Use local temporary runtime roots."],
                        "non_goals": [],
                        "relevant_context": ["The task is already clarified."],
                        "open_questions": [],
                        "artifact_ready_for_persistence": True,
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        workspace_root = Path(
            tempfile.mkdtemp(prefix="test-canonical-launch-reused-existing-", dir=str(ROOT / "workspace"))
        ).resolve()
        state_root = Path(
            tempfile.mkdtemp(prefix="test-canonical-launch-reused-existing-", dir=str(ROOT / ".loop"))
        ).resolve()
        first_launch_result_ref: Path | None = None
        previous_launch_mode = os.environ.get("LOOP_CHILD_LAUNCH_MODE")
        try:
            os.environ["LOOP_CHILD_LAUNCH_MODE"] = "direct"
            initial_cleanup = cleanup_test_repo_services(
                repo_root=ROOT,
                settle_timeout_s=4.0,
                poll_interval_s=0.05,
                temp_test_repo_roots=[],
            )
            if not bool(initial_cleanup.get("quiesced")):
                return _fail("repo service cleanup must quiesce before canonical launch_reused_existing validation")
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-canonical-launch-reused-existing",
                root_goal="bootstrap one implementer node for canonical launch-reused-existing event coverage",
                child_goal_slice="prepare one live child and verify trusted reuse emits canonical launch_reused_existing",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            source_result = state_root / "artifacts" / "bootstrap" / "CanonicalLaunchReusedExistingSourceResult.json"
            marker_token = str((workspace_root / "canonical_launch_reused_existing.marker").resolve())
            source_result.write_text(
                json.dumps(
                    {
                        "node_id": str(bootstrap["node_id"]),
                        "workspace_root": str(workspace_root.resolve()),
                        "state_root": str(state_root.resolve()),
                        "launch_spec": {
                            "argv": [
                                sys.executable,
                                "-c",
                                "import time; time.sleep(30)",
                                marker_token,
                            ],
                            "env": {},
                            "cwd": str(workspace_root.resolve()),
                            "stdin_path": str((workspace_root / "CHILD_PROMPT.md").resolve()),
                        },
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

            first_launch = trusted_launch_child_from_result_ref(
                result_ref=str(source_result.resolve()),
                startup_probe_ms=50,
                startup_health_timeout_ms=250,
            )
            if str(first_launch.get("launch_decision") or "") != "started":
                return _fail("first trusted launch must produce a started child for reuse coverage")
            first_launch_result_ref = Path(str(first_launch.get("launch_result_ref") or "")).resolve()

            reused_launch = trusted_launch_child_from_result_ref(
                result_ref=str(source_result.resolve()),
                startup_probe_ms=50,
                startup_health_timeout_ms=250,
            )
            if str(reused_launch.get("launch_decision") or "") != "started_existing":
                return _fail("second trusted launch against a live child must produce started_existing")
            reused_launch_result_ref = Path(str(reused_launch.get("launch_result_ref") or "")).resolve()

            reuse_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "launch_reused_existing"
            ]
            if len(reuse_events) != 1:
                return _fail("trusted successful reuse must append exactly one canonical launch_reused_existing event")
            payload = dict(reuse_events[0].get("payload") or {})
            if str(payload.get("launch_event_id") or "") != str(reused_launch_result_ref):
                return _fail("launch_reused_existing must preserve the new alias launch identity")
            if str(payload.get("reused_launch_event_id") or "") != str(first_launch_result_ref):
                return _fail("launch_reused_existing must preserve the prior live launch identity it reuses")
            if str(payload.get("reuse_launch_result_ref") or "") != str(first_launch_result_ref):
                return _fail("launch_reused_existing must preserve reuse_launch_result_ref continuity")
            if str(payload.get("source_result_ref") or "") != str(source_result.resolve()):
                return _fail("launch_reused_existing must preserve source result provenance")
            if int(payload.get("pid") or 0) != int(first_launch.get("pid") or 0):
                return _fail("launch_reused_existing must preserve the reused live pid")
            if not str(payload.get("wrapped_argv_fingerprint") or "").strip():
                return _fail("launch_reused_existing must preserve expected wrapped argv fingerprint continuity")

            repeated_reuse = trusted_launch_child_from_result_ref(
                result_ref=str(source_result.resolve()),
                startup_probe_ms=50,
                startup_health_timeout_ms=250,
            )
            if str(repeated_reuse.get("launch_decision") or "") != "started_existing":
                return _fail("repeat trusted reuse against the same live child must stay on started_existing")
            repeated_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "launch_reused_existing"
            ]
            if len(repeated_events) != 1:
                return _fail("repeat trusted reuse against the same live child must not append duplicate launch_reused_existing events")
        finally:
            if previous_launch_mode is None:
                os.environ.pop("LOOP_CHILD_LAUNCH_MODE", None)
            else:
                os.environ["LOOP_CHILD_LAUNCH_MODE"] = previous_launch_mode
            if first_launch_result_ref is not None:
                try:
                    terminate_runtime_owned_launch_result_ref(result_ref=first_launch_result_ref)
                except Exception:
                    pass
            if state_root.exists():
                try:
                    cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
                except Exception:
                    pass
            try:
                cleanup_test_repo_services(
                    repo_root=ROOT,
                    settle_timeout_s=4.0,
                    poll_interval_s=0.05,
                    temp_test_repo_roots=[],
                )
            except Exception:
                pass
            if workspace_root.exists():
                _safe_rmtree(workspace_root)
            if state_root.exists():
                _safe_rmtree(state_root)

    print("[loop-system-canonical-launch-reused-existing-event] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
