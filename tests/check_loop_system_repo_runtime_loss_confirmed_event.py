#!/usr/bin/env python3
"""Validate canonical runtime_loss_confirmed events from trusted runtime-status observations."""

from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-loss-confirmed-event][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_dead(pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)


def _purge_tree(path: Path, timeout_s: float = 2.0) -> None:
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

    deadline = time.time() + max(0.0, float(timeout_s))
    while path.exists():
        shutil.rmtree(path, onerror=_onerror)
        if not path.exists():
            return
        if time.time() >= deadline:
            raise OSError(f"failed to purge runtime-loss cleanup tree {path}")
        time.sleep(0.05)


def _purge_runtime_loss_roots() -> None:
    for base in (ROOT / ".loop", ROOT / "workspace"):
        for path in sorted(base.glob("test-runtime-loss-confirmed_*")):
            if path.exists():
                _purge_tree(path)


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        child_runtime_status_from_launch_result_ref as trusted_child_runtime_status_from_launch_result_ref,
        cleanup_test_repo_services,
        cleanup_test_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_loss_confirmed_event_") as td:
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
                    "original_user_prompt": "Bootstrap one runtime-loss-confirmed test node.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "design",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Bootstrap one runtime-loss-confirmed test node.",
                        "final_effect": "Bootstrap one runtime-loss-confirmed test node.",
                        "observable_success_criteria": ["One child can be launched and status-queried."],
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
            tempfile.mkdtemp(prefix="test-runtime-loss-confirmed_", dir=str(ROOT / "workspace"))
        ).resolve()
        state_root = Path(
            tempfile.mkdtemp(prefix="test-runtime-loss-confirmed_", dir=str(ROOT / ".loop"))
        ).resolve()
        live_pid = 0
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        try:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            _purge_runtime_loss_roots()
            initial_cleanup = cleanup_test_repo_services(
                repo_root=ROOT,
                settle_timeout_s=4.0,
                poll_interval_s=0.05,
                temp_test_repo_roots=[],
            )
            if not bool(initial_cleanup.get("quiesced")):
                return _fail("repo service cleanup must quiesce before runtime_loss_confirmed validation")
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-runtime-loss-confirmed",
                root_goal="bootstrap one implementer node for runtime-loss-confirmed event coverage",
                child_goal_slice="prepare one live child, kill it, and confirm runtime loss through trusted status",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            source_result = state_root / "artifacts" / "bootstrap" / "RuntimeLossConfirmedSourceResult.json"
            marker_token = str((workspace_root / "runtime_loss_confirmed.marker").resolve())
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
            stdout_ref = Path(str(launch.get("stdout_ref") or "")).resolve()
            stderr_ref = Path(str(launch.get("stderr_ref") or "")).resolve()
            live_pid = int(launch.get("pid") or 0)
            if live_pid <= 0 or not launch_result_ref.exists():
                return _fail("trusted launch must produce a live pid and launch result ref")

            os.kill(live_pid, signal.SIGTERM)
            _wait_dead(live_pid)

            stale_epoch = time.time() - 25.0
            for path in (launch_result_ref, stdout_ref, stderr_ref):
                os.utime(path, (stale_epoch, stale_epoch))

            confirmed_dead_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref),
                stall_threshold_s=0,
            )
            if not bool(confirmed_dead_status.get("recovery_eligible")):
                return _fail("trusted runtime-status must confirm recovery eligibility for a stale dead ACTIVE child")
            if str(confirmed_dead_status.get("recovery_reason") or "") != "active_without_live_pid":
                return _fail("confirmed dead ACTIVE child must explain recovery eligibility through active_without_live_pid")
            if str(confirmed_dead_status.get("runtime_attachment_state") or "") != "LOST":
                return _fail("confirmed dead ACTIVE child must downgrade runtime attachment to LOST")

            loss_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "runtime_loss_confirmed"
            ]
            if len(loss_events) != 1:
                return _fail("confirmed dead trusted runtime-status call must append exactly one canonical runtime_loss_confirmed event")
            payload = dict(loss_events[0].get("payload") or {})
            if str(payload.get("launch_event_id") or "") != str(launch_result_ref.resolve()):
                return _fail("runtime_loss_confirmed must preserve launch identity continuity")
            if str(payload.get("status_result_ref") or "") != str(confirmed_dead_status.get("status_result_ref") or ""):
                return _fail("runtime_loss_confirmed must preserve status_result_ref evidence")
            if str(payload.get("recovery_reason") or "") != "active_without_live_pid":
                return _fail("runtime_loss_confirmed must preserve recovery_reason continuity")
            if int(payload.get("pid") or 0) != int(live_pid):
                return _fail("runtime_loss_confirmed must preserve the launched pid")
            if not str(payload.get("process_fingerprint") or "").strip():
                return _fail("runtime_loss_confirmed must preserve process fingerprint continuity")

            repeated_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref),
                stall_threshold_s=0,
            )
            if str(repeated_status.get("recovery_reason") or "") != "active_without_live_pid":
                return _fail("repeated confirmed-loss status must stay on active_without_live_pid")
            repeated_loss_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "runtime_loss_confirmed"
            ]
            if len(repeated_loss_events) != 1:
                return _fail("repeated confirmed-loss status must not append duplicate runtime_loss_confirmed events")
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
                    os.kill(live_pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError:
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
            _purge_runtime_loss_roots()

    print("[loop-system-runtime-loss-confirmed-event] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
