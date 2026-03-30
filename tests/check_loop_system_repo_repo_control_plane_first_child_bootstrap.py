#!/usr/bin/env python3
"""Validate first-child bootstrap of the repo control-plane before explicit helper use."""

from __future__ import annotations

import json
import os
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-repo-control-plane-first-child-bootstrap][FAIL] {msg}", file=sys.stderr)
    return 2


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


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


def _process_alive_with_marker(pid: int, needle: str, *, repo_root: Path) -> bool:
    if not _pid_alive(pid):
        return False
    proc = subprocess.run(
        ["ps", "-ww", "-p", str(int(pid)), "-o", "command="],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return False
    command = str(proc.stdout or "").strip()
    return all(fragment in command for fragment in str(needle or "").split()) and str(repo_root.resolve()) in command


def _repo_control_plane_payload(*, control_plane_root: Path) -> dict[str, object]:
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime.control_plane import REPO_CONTROL_PLANE_NODE_ID

    effective = dict(
        dict(query_runtime_liveness_view(control_plane_root).get("effective_runtime_liveness_by_node") or {}).get(
            REPO_CONTROL_PLANE_NODE_ID
        )
        or {}
    )
    return dict(effective.get("payload") or {})


def _repo_service_pids_for_repo(repo_root: Path) -> list[int]:
    proc = subprocess.run(
        ["ps", "-ax", "-o", "pid=,command="],
        text=True,
        capture_output=True,
        check=True,
    )
    result: list[int] = []
    marker = str(repo_root.resolve())
    for line in proc.stdout.splitlines():
        raw = line.strip()
        if not raw:
            continue
        pid_text, _, command = raw.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if marker not in command:
            continue
        if (
            "loop_product.runtime.control_plane" in command
            or "loop_product.host_child_launch_supervisor" in command
            or ("loop_product.runtime.gc" in command and "--run-housekeeping-controller" in command)
        ):
            result.append(pid)
    return sorted(set(result))


def _safe_rmtree(path: Path) -> None:
    if not path.exists():
        return

    def _onerror(func, target, exc_info):  # type: ignore[no-untyped-def]
        del exc_info
        try:
            os.chmod(target, stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR)
        except OSError:
            return
        try:
            func(target)
        except OSError:
            return

    shutil.rmtree(path, ignore_errors=False, onerror=_onerror)


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(
            repo_root=repo_root,
            settle_timeout_s=4.0,
            poll_interval_s=0.05,
            temp_test_repo_roots=[],
        )
    except Exception:
        pass


def main() -> int:
    from loop_product.runtime import bootstrap_first_implementer_node, cleanup_test_repo_services
    from loop_product.runtime.control_plane import repo_control_plane_runtime_root

    with tempfile.TemporaryDirectory(prefix="loop_system_repo_control_plane_first_child_bootstrap_") as td:
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
                    "original_user_prompt": "Create one local birthday poster HTML.",
                    "confirmed_requirements": [],
                    "denied_requirements": [],
                    "question_history": [],
                    "turn_count": 1,
                    "requirement_artifact": {
                        "task_type": "design",
                        "workflow_scope": "generic",
                        "sufficient": True,
                        "user_request_summary": "Deliver one local birthday poster.",
                        "final_effect": "Deliver one local birthday poster.",
                        "observable_success_criteria": ["A local HTML birthday poster exists."],
                        "hard_constraints": ["Output target is local."],
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

        repo_root = ROOT
        workspace_root = ROOT / "workspace" / "test-repo-control-plane-first-child-bootstrap"
        state_root = ROOT / ".loop" / "test-repo-control-plane-first-child-bootstrap"
        _safe_rmtree(workspace_root)
        _safe_rmtree(state_root)
        control_plane_pid = 0
        try:
            initial_cleanup = cleanup_test_repo_services(
                repo_root=repo_root,
                settle_timeout_s=4.0,
                poll_interval_s=0.05,
                temp_test_repo_roots=[],
            )
            if not bool(initial_cleanup.get("quiesced")):
                return _fail("repo service cleanup must quiesce before first-child bootstrap validation")

            bootstrap_first_implementer_node(
                mode="fresh",
                task_slug="test-repo-control-plane-first-child-bootstrap",
                root_goal="bootstrap first child while also starting repo control-plane",
                child_goal_slice="start one first child bootstrap bundle",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            control_plane_root = repo_control_plane_runtime_root(repo_root=repo_root)
            if not _wait_until(lambda: bool(_repo_control_plane_payload(control_plane_root=control_plane_root)), timeout_s=4.0):
                return _fail("first-child bootstrap must bring up repo control-plane authority before explicit helper use")

            payload_a = _repo_control_plane_payload(control_plane_root=control_plane_root)
            control_plane_pid = int(payload_a.get("pid") or 0)
            if control_plane_pid <= 0:
                return _fail("first-child bootstrap must expose a live repo control-plane pid")
            if not _process_alive_with_marker(control_plane_pid, "loop_product.runtime.control_plane", repo_root=repo_root):
                return _fail("first-child bootstrap must start a live repo control-plane process")

            bootstrap_first_implementer_node(
                mode="continue_exact",
                task_slug="ignored",
                root_goal="ignored",
                child_goal_slice="ignored",
                endpoint_artifact_ref=str(endpoint.resolve()),
                workspace_root=str(workspace_root),
                state_root=str(state_root),
                node_id="test-repo-control-plane-first-child-bootstrap",
                workspace_mirror_relpath="deliverables/out.html",
                external_publish_target=str((temp_root / "Desktop" / "out.html").resolve()),
                context_refs=[],
            )

            payload_b = _repo_control_plane_payload(control_plane_root=control_plane_root)
            control_plane_pid_b = int(payload_b.get("pid") or 0)
            if control_plane_pid_b != control_plane_pid:
                return _fail("repeated first-child bootstrap must reuse the same repo control-plane")
        finally:
            _cleanup_repo_services(repo_root)
            _safe_rmtree(workspace_root)
            _safe_rmtree(state_root)

    print("[loop-system-repo-control-plane-first-child-bootstrap] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
