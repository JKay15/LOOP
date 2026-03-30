#!/usr/bin/env python3
"""Validate cleanup-authority-driven detached sidecar retirement."""

from __future__ import annotations

import json
import os
import shutil
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
    print(f"[loop-system-sidecar-runtime-context-missing-exit][FAIL] {msg}", file=sys.stderr)
    return 2


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _wait_dead(pid: int, timeout_s: float = 5.0) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if not _pid_alive(pid):
            return True
        time.sleep(0.05)
    return not _pid_alive(pid)


def _wait_runtime_payload(runtime_ref: Path, timeout_s: float = 5.0) -> dict[str, object] | None:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if runtime_ref.exists():
            try:
                payload = json.loads(runtime_ref.read_text(encoding="utf-8"))
            except Exception:
                payload = None
            if isinstance(payload, dict) and int(payload.get("pid") or 0) > 0:
                return payload
        time.sleep(0.05)
    return None


def _command_tokens(command: str) -> list[str]:
    return [token for token in str(command or "").split() if token]


def _command_flag_value(command: str, flag: str) -> str:
    normalized_flag = str(flag or "").strip()
    if not normalized_flag:
        return ""
    tokens = _command_tokens(command)
    for index, token in enumerate(tokens):
        if token == normalized_flag:
            if index + 1 < len(tokens):
                return str(tokens[index + 1] or "").strip()
            return ""
        if token.startswith(f"{normalized_flag}="):
            return str(token.split("=", 1)[1] or "").strip()
    return ""


def _command_repo_root_matches(command: str, repo_root: Path) -> bool:
    raw_repo_root = _command_flag_value(command, "--repo-root")
    if not raw_repo_root:
        return False
    try:
        return Path(raw_repo_root).expanduser().resolve() == repo_root.resolve()
    except Exception:
        return False


def _repo_service_pids_for_repo(repo_root: Path) -> list[int]:
    proc = subprocess.run(
        ["ps", "-ax", "-o", "pid=,command="],
        text=True,
        capture_output=True,
        check=True,
    )
    result: list[int] = []
    resolved_repo_root = repo_root.resolve()
    for line in proc.stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        pid_text, _, command = raw.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if not _command_repo_root_matches(command, resolved_repo_root):
            continue
        if (
            "loop_product.runtime.control_plane" in command
            or "loop_product.host_child_launch_supervisor" in command
            or ("loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command)
        ):
            result.append(pid)
    return sorted(set(result))


def _wait_for_repo_service_pids_to_clear(repo_root: Path, *, timeout_s: float = 3.0) -> list[int]:
    deadline = time.time() + max(0.0, float(timeout_s))
    remaining = _repo_service_pids_for_repo(repo_root)
    while remaining and time.time() < deadline:
        time.sleep(0.05)
        remaining = _repo_service_pids_for_repo(repo_root)
    return remaining


def _cleanup_repo_services_strict(repo_root: Path) -> dict[str, object]:
    from loop_product.runtime import cleanup_test_repo_services

    receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=6.0, poll_interval_s=0.05)
    if not bool(receipt.get("quiesced")):
        time.sleep(0.25)
        receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=6.0, poll_interval_s=0.05)
    return dict(receipt)


def _write_source_result(path: Path, *, node_id: str, workspace_root: Path, state_root: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "node_id": node_id,
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "launch_spec": {
                    "argv": [sys.executable, "-c", "import time; time.sleep(12)"],
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


def main() -> int:
    from loop_product.kernel import query_operational_hygiene_view
    from loop_product.runtime import (
        bootstrap_first_implementer_node,
        child_runtime_status_from_launch_result_ref as trusted_child_runtime_status_from_launch_result_ref,
        cleanup_test_repo_services,
        cleanup_test_runtime_root,
    )
    from loop_product.runtime.gc import housekeeping_runtime_root

    with tempfile.TemporaryDirectory(prefix="loop_system_sidecar_runtime_context_missing_exit_") as td:
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
                            "The page plays local music without external links.",
                        ],
                        "hard_constraints": ["Output target is local."],
                        "non_goals": ["Do not use streaming embeds."],
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
            tempfile.mkdtemp(
                prefix="test-sidecar-runtime-context-missing-exit_",
                dir=str(ROOT / "workspace"),
            )
        ).resolve()
        state_root = Path(
            tempfile.mkdtemp(
                prefix="test-sidecar-runtime-context-missing-exit_",
                dir=str(ROOT / ".loop"),
            )
        ).resolve()

        live_pid = 0
        sidecar_pid = 0
        launch_result_ref = state_root / "artifacts" / "launches" / "unknown" / "attempt_001" / "ChildLaunchResult.json"
        previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
        try:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            initial_cleanup = cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(initial_cleanup.get("quiesced")):
                return _fail("repo service cleanup must quiesce before sidecar runtime-context-missing-exit validation")
            bootstrap = bootstrap_first_implementer_node(
                mode="fresh",
                task_slug=state_root.name,
                root_goal="bootstrap one implementer node for detached sidecar cleanup-authority coverage",
                child_goal_slice="prove that transient runtime loss does not force sidecar exit, while committed cleanup authority does",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )
            source_result_ref = state_root / "artifacts" / "bootstrap" / "SidecarRuntimeContextMissingExitSourceResult.json"
            _write_source_result(
                source_result_ref,
                node_id=str(bootstrap["node_id"]),
                workspace_root=workspace_root,
                state_root=state_root,
            )

            launch_script = ROOT / "scripts" / "launch_child_from_result.sh"
            launch_proc = subprocess.run(
                [
                    str(launch_script),
                    "--result-ref",
                    str(source_result_ref.resolve()),
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

            status_payload = trusted_child_runtime_status_from_launch_result_ref(
                result_ref=str(launch_result_ref),
                stall_threshold_s=0,
            )
            if str(status_payload.get("runtime_attachment_state") or "").strip().upper() != "ATTACHED":
                return _fail("trusted runtime status must report ATTACHED before cleanup-authority coverage")

            runtime_ref = launch_result_ref.with_name("ChildSupervisionRuntime.json")
            runtime_payload = _wait_runtime_payload(runtime_ref, timeout_s=5.0)
            if runtime_payload is None:
                return _fail("trusted runtime status must materialize a detached ChildSupervisionRuntime marker")
            sidecar_pid = int(runtime_payload.get("pid") or 0)
            if sidecar_pid <= 0 or not _pid_alive(sidecar_pid):
                return _fail("runtime marker must point at a live detached sidecar pid")

            transient_launch_shadow = launch_result_ref.with_name("ChildLaunchResult.shadow.json")
            launch_result_ref.rename(transient_launch_shadow)
            time.sleep(2.5)
            if launch_result_ref.exists():
                launch_result_ref.unlink()
            transient_launch_shadow.rename(launch_result_ref)
            if not _pid_alive(sidecar_pid):
                return _fail("transient runtime loss without cleanup authority must not force detached sidecar exit")

            cleanup_receipt = cleanup_test_runtime_root(
                state_root=state_root,
                repo_root=ROOT,
            )
            if not bool(cleanup_receipt.get("quiesced")):
                return _fail("trusted test cleanup helper must quiesce the runtime root")
            if not _wait_dead(sidecar_pid, timeout_s=6.0):
                return _fail("detached sidecar must retire after committed cleanup authority and runtime teardown")

            hygiene = query_operational_hygiene_view(
                housekeeping_runtime_root(repo_root=ROOT),
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("test_runtime_cleanup_committed_event_count") or 0) < 1:
                return _fail("cleanup helper must commit a canonical test_runtime_cleanup_committed fact")
            if int(hygiene.get("runtime_root_quiesced_event_count") or 0) < 1:
                return _fail("cleanup helper must commit a canonical runtime_root_quiesced fact")
            if int(hygiene.get("process_orphan_detected_event_count") or 0) < 1:
                return _fail("cleanup helper must commit at least one canonical process_orphan_detected fact")
        finally:
            if previous_runtime_status_mode is None:
                os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
            else:
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if live_pid > 0 and _pid_alive(live_pid):
                try:
                    os.kill(live_pid, signal.SIGTERM)
                except OSError:
                    pass
                _wait_dead(live_pid, timeout_s=2.0)
            if sidecar_pid > 0 and _pid_alive(sidecar_pid):
                try:
                    os.kill(sidecar_pid, signal.SIGTERM)
                except OSError:
                    pass
                if not _wait_dead(sidecar_pid, timeout_s=2.0):
                    try:
                        os.kill(sidecar_pid, signal.SIGKILL)
                    except OSError:
                        pass
                    _wait_dead(sidecar_pid, timeout_s=1.0)
            shutil.rmtree(workspace_root, ignore_errors=True)
            shutil.rmtree(state_root, ignore_errors=True)
            transient_launch_shadow = launch_result_ref.with_name("ChildLaunchResult.shadow.json")
            transient_launch_shadow.unlink(missing_ok=True)
            repo_cleanup_receipt = _cleanup_repo_services_strict(ROOT)
            if not bool(repo_cleanup_receipt.get("quiesced")):
                raise RuntimeError(
                    "sidecar runtime-context cleanup must quiesce repo-scope services, "
                    f"got: {repo_cleanup_receipt}"
                )
            if dict(repo_cleanup_receipt.get("remaining_live_services") or {}):
                raise RuntimeError(
                    "sidecar runtime-context cleanup must not leave live repo-scope services behind, "
                    f"got: {repo_cleanup_receipt}"
                )
            if dict(repo_cleanup_receipt.get("remaining_retired_pids") or {}):
                raise RuntimeError(
                    "sidecar runtime-context cleanup must not leave retired repo-scope pids behind, "
                    f"got: {repo_cleanup_receipt}"
                )
            remaining = _wait_for_repo_service_pids_to_clear(ROOT)
            if remaining:
                raise RuntimeError(
                    "sidecar runtime-context cleanup must not leave repo-scope service pids behind, "
                    f"got: {remaining}"
                )

    print("[loop-system-sidecar-runtime-context-missing-exit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
