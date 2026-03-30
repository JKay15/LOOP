#!/usr/bin/env python3
"""Validate trusted runtime-status compatibility mirroring into guarded liveness events."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-runtime-status-liveness-mirror][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_dead(pid: int, timeout_s: float = 5.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)


def _overwrite_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        os.chmod(path, 0o644)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(path, 0o444)


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        child_runtime_status_from_launch_result_ref as trusted_child_runtime_status_from_launch_result_ref,
        cleanup_test_runtime_root,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_runtime_status_liveness_mirror_") as td:
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
                    "original_user_prompt": "Create one local birthday poster HTML with offline music playback.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "design",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Deliver one local birthday poster with offline music playback.",
                        "final_effect": "Deliver one local birthday poster with offline music playback.",
                        "observable_success_criteria": [
                            "A local HTML birthday poster exists.",
                            "The page plays local music without external links."
                        ],
                        "hard_constraints": ["Output target is local."],
                        "non_goals": ["Do not use streaming embeds."],
                        "relevant_context": ["The task is already clarified."],
                        "open_questions": [],
                        "artifact_ready_for_persistence": True
                    }
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        workspace_root = Path(
            tempfile.mkdtemp(
                prefix="test-runtime-status-liveness-mirror_",
                dir=str(ROOT / "workspace"),
            )
        ).resolve()
        state_root = Path(
            tempfile.mkdtemp(
                prefix="test-runtime-status-liveness-mirror_",
                dir=str(ROOT / ".loop"),
            )
        ).resolve()
        live_pid = 0
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        try:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-runtime-status-liveness-mirror",
                root_goal="bootstrap one implementer node for runtime-status liveness journal mirroring",
                child_goal_slice="prepare one live child and mirror runtime status into guarded liveness events",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            source_result = state_root / "artifacts" / "bootstrap" / "RuntimeStatusMirrorSourceResult.json"
            marker_token = str((workspace_root / "runtime_status_mirror.marker").resolve())
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
            node_ref = state_root / "state" / f"{bootstrap['node_id']}.json"
            node_payload = json.loads(node_ref.read_text(encoding="utf-8"))
            runtime_state = dict(node_payload.get("runtime_state") or {})
            if str(runtime_state.get("attachment_state") or "") != "ATTACHED":
                return _fail("trusted runtime-status mirror must synchronize node snapshot attachment truth to ATTACHED")
            if not bool(runtime_state.get("live_process_trusted")):
                return _fail("trusted runtime-status mirror must mark the synchronized node snapshot as live_process_trusted")
            if not list(runtime_state.get("evidence_refs") or []):
                return _fail("trusted runtime-status mirror must retain runtime evidence refs on the node snapshot")
            first_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
            ]
            if not first_events:
                return _fail("first trusted runtime-status call must see at least one committed liveness event")
            if any(
                str(dict(event.get("payload") or {}).get("lease_owner_id") or "").startswith("status-query:")
                for event in first_events
            ):
                return _fail("trusted runtime-status mirror must not append a conflicting status-query lease when sidecar lease already owns ATTACHED truth")

            launch_payload = json.loads(launch_result_ref.read_text(encoding="utf-8"))
            launch_payload["wrapped_argv"] = [
                str(item) for item in list(launch_payload.get("wrapped_argv") or [])
            ]
            if len(launch_payload["wrapped_argv"]) < 2:
                return _fail("launch result must carry wrapped argv for runtime-status mirror coverage")
            launch_payload["wrapped_argv"][-1] = str((workspace_root / "runtime_status_mirror.other.marker").resolve())
            _overwrite_json(launch_result_ref, launch_payload)

            second_status = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref),
                stall_threshold_s=0,
            )
            if str(second_status.get("runtime_attachment_state") or "") != "ATTACHED":
                return _fail("second trusted runtime-status call must still report ATTACHED for the same live child")
            second_events = [
                event
                for event in iter_committed_events(state_root)
                if str(event.get("event_type") or "") == "runtime_liveness_observed"
            ]
            if any(
                str(dict(event.get("payload") or {}).get("lease_owner_id") or "").startswith("status-query:")
                for event in second_events
            ):
                return _fail("conflicting runtime-status mirror must be blocked by the guarded liveness writer")

        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if state_root.exists():
                cleanup_test_runtime_root(state_root=state_root, repo_root=ROOT)
            if live_pid > 0:
                try:
                    os.kill(live_pid, 15)
                except OSError:
                    pass
                _wait_dead(live_pid)
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)

    print("[loop-system-runtime-status-liveness-mirror] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
