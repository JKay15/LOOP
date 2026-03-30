#!/usr/bin/env python3
"""Validate canonical process_identity_confirmed events from trusted runtime-status observations."""

from __future__ import annotations

import json
import os
import shutil
import stat
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-canonical-process-identity-confirmed-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_dead(pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)


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
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        child_runtime_status_from_launch_result_ref as trusted_child_runtime_status_from_launch_result_ref,
        cleanup_test_repo_services,
        cleanup_test_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_canonical_process_identity_confirmed_event_") as td:
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
                    "original_user_prompt": "Bootstrap one process-identity-confirmed test node.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "design",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Bootstrap one process-identity-confirmed test node.",
                        "final_effect": "Bootstrap one process-identity-confirmed test node.",
                        "observable_success_criteria": ["One live child can be status-queried."],
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
            tempfile.mkdtemp(prefix="test-process-identity-confirmed_", dir=str(ROOT / "workspace"))
        ).resolve()
        state_root = Path(
            tempfile.mkdtemp(prefix="test-process-identity-confirmed_", dir=str(ROOT / ".loop"))
        ).resolve()
        live_pid = 0
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        try:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            initial_cleanup = cleanup_test_repo_services(
                repo_root=ROOT,
                settle_timeout_s=4.0,
                poll_interval_s=0.05,
                temp_test_repo_roots=[],
            )
            if not bool(initial_cleanup.get("quiesced")):
                return _fail("repo service cleanup must quiesce before canonical process_identity_confirmed validation")
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-process-identity-confirmed",
                root_goal="bootstrap one implementer node for canonical process_identity_confirmed coverage",
                child_goal_slice="prepare one live child and confirm runtime identity through trusted runtime status",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            source_result = state_root / "artifacts" / "bootstrap" / "ProcessIdentityConfirmedSourceResult.json"
            marker_token = str((workspace_root / "process_identity_confirmed.marker").resolve())
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

            launch_script = ROOT / "scripts" / "launch_child_from_result.sh"
            launch_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(source_result.resolve()),
                    "--startup-probe-ms",
                    "50",
                    "--startup-health-timeout-ms",
                    "250",
                ],
                cwd=str(ROOT),
                text=True,
                capture_output=True,
                env={**dict(os.environ), "LOOP_CHILD_LAUNCH_MODE": "direct"},
            )
            if launch_proc.returncode != 0:
                return _fail(f"trusted launch must succeed: {launch_proc.stderr or launch_proc.stdout}")
            launch = json.loads(launch_proc.stdout)
            launch_result_ref = Path(str(launch.get("launch_result_ref") or "")).resolve()
            live_pid = int(launch.get("pid") or 0)
            if live_pid <= 0 or not launch_result_ref.exists():
                return _fail("trusted launch must produce a live pid and launch result ref")

            first_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref),
                stall_threshold_s=0,
            )
            if str(first_status.get("runtime_attachment_state") or "") != "ATTACHED":
                return _fail("first trusted runtime-status call must report ATTACHED for the live child")
            if not bool(first_status.get("pid_alive")):
                return _fail("first trusted runtime-status call must confirm pid_alive for the live child")

            confirmed_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "process_identity_confirmed"
            ]
            if len(confirmed_events) != 1:
                return _fail("trusted live runtime-status must append exactly one canonical process_identity_confirmed event")
            payload = dict(confirmed_events[0].get("payload") or {})
            if str(payload.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
                return _fail("process_identity_confirmed must preserve launch identity continuity")
            if str(payload.get("status_result_ref") or "") != str(first_status.get("status_result_ref") or ""):
                return _fail("process_identity_confirmed must preserve status_result_ref evidence")
            if int(payload.get("pid") or 0) != int(live_pid):
                return _fail("process_identity_confirmed must preserve the launched pid")
            if not str(payload.get("process_fingerprint") or "").strip():
                return _fail("process_identity_confirmed must preserve process fingerprint continuity")

            second_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref),
                stall_threshold_s=0,
            )
            if str(second_status.get("runtime_attachment_state") or "") != "ATTACHED":
                return _fail("repeat trusted runtime-status call must still report ATTACHED for the same live child")
            repeated_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "process_identity_confirmed"
            ]
            if len(repeated_events) != 1:
                return _fail("repeat trusted live runtime-status must not append duplicate process_identity_confirmed events")
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if state_root.exists():
                try:
                    cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
                except Exception:
                    pass
            if live_pid:
                try:
                    os.kill(live_pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                except OSError:
                    pass
                _wait_dead(live_pid)
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

    print("[loop-system-canonical-process-identity-confirmed-event] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
