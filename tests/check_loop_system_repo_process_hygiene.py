#!/usr/bin/env python3
"""Guard supervisor/runtime hygiene after terminal child closeout."""

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
    print(f"[loop-system-process-hygiene][FAIL] {msg}", file=sys.stderr)
    return 2


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


def _wait_for_repo_service_pids_to_clear(repo_root: Path, *, timeout_s: float = 3.0) -> list[int]:
    deadline = time.time() + max(0.0, float(timeout_s))
    remaining = _repo_service_pids_for_repo(repo_root)
    while remaining and time.time() < deadline:
        time.sleep(0.05)
        remaining = _repo_service_pids_for_repo(repo_root)
    return remaining


def _query_direct_runtime_status_until(
    *,
    launch_runtime_module,
    result_ref: Path,
    pre_wait_s: float = 5.0,
    stall_threshold_s: float = 60.0,
) -> dict[str, object]:
    time.sleep(max(0.0, float(pre_wait_s)))
    previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
    os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
    try:
        return dict(
            launch_runtime_module.child_runtime_status_from_launch_result_ref(
                result_ref=result_ref,
                stall_threshold_s=stall_threshold_s,
            )
            or {}
        )
    finally:
        if previous_runtime_status_mode is None:
            os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
        else:
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode


def main() -> int:
    from loop_product.dispatch import launch_runtime as launch_runtime_module
    from loop_product.kernel.query import query_authority_view
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import (
        KernelState,
        ensure_runtime_tree,
        load_kernel_state,
        persist_kernel_state,
        persist_node_snapshot,
    )
    from loop_product.protocols.node import NodeSpec, NodeStatus
    from loop_product.runtime.gc import cleanup_test_runtime_root
    from loop_product.runtime import cleanup_test_repo_services
    from loop_product.runtime.control_plane import _repo_service_processes_by_kind
    from loop_product.runtime.control_plane import ensure_repo_control_plane_services_running
    from loop_product.runtime_paths import node_live_artifact_root

    with tempfile.TemporaryDirectory(prefix="loop_product_process_hygiene_") as td:
        temp_root = Path(td)
        idle_repo_root = temp_root / "idle_repo"
        idle_repo_root.mkdir(parents=True, exist_ok=True)
        supervisor_runtime_ref = idle_repo_root / ".loop" / "host_child_launch_supervisor" / "HostChildLaunchSupervisorRuntime.json"
        supervisor_proc = subprocess.Popen(
            [
                "uv",
                "run",
                "--locked",
                "python",
                "-m",
                "loop_product.host_child_launch_supervisor",
                "--repo-root",
                str(idle_repo_root),
                "--poll-interval-s",
                "0.05",
                "--idle-exit-after-s",
                "0.15",
            ],
            cwd=str(ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            supervisor_stdout, supervisor_stderr = supervisor_proc.communicate(timeout=8.0)
        except subprocess.TimeoutExpired:
            supervisor_proc.kill()
            supervisor_stdout, supervisor_stderr = supervisor_proc.communicate(timeout=3.0)
            return _fail("host child launch supervisor must exit when idle instead of lingering forever")
        if supervisor_proc.returncode != 0:
            detail = supervisor_stderr.strip() or supervisor_stdout.strip()
            return _fail(f"idle supervisor must exit cleanly: {detail}")
        if supervisor_runtime_ref.exists():
            return _fail("idle supervisor must clean its runtime marker on exit")

        with tempfile.TemporaryDirectory(prefix="loop_system_process_hygiene_foreign_repo_") as foreign_td:
            foreign_repo_root = (Path(foreign_td).resolve() / "loop_product_repo").resolve()
            foreign_started = ensure_repo_control_plane_services_running(repo_root=foreign_repo_root)
            if int(dict(foreign_started.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
                return _fail("setup must start temp foreign repo services before residue cleanup is exercised")
            if not _repo_service_pids_for_repo(foreign_repo_root):
                return _fail("setup must observe live temp foreign repo service pids before cleanup is exercised")
            foreign_cleanup_receipt = cleanup_test_repo_services(
                repo_root=temp_root,
                settle_timeout_s=4.0,
                poll_interval_s=0.05,
                temp_test_repo_roots=[foreign_repo_root],
            )
            temp_roots = set(str(item) for item in list(foreign_cleanup_receipt.get("temp_test_repo_roots") or []))
            if str(foreign_repo_root) not in temp_roots:
                return _fail(
                    "cleanup_test_repo_services(ROOT) must discover sibling temp test repo roots for residue cleanup"
                )
            temp_receipts = {
                str(item.get("repo_root") or ""): dict(item)
                for item in list(foreign_cleanup_receipt.get("temp_test_repo_receipts") or [])
                if str(item.get("repo_root") or "")
            }
            foreign_temp_receipt = dict(temp_receipts.get(str(foreign_repo_root)) or {})
            if not foreign_temp_receipt:
                return _fail("cleanup_test_repo_services(ROOT) must return the temp foreign repo cleanup receipt")
            if not bool(foreign_temp_receipt.get("quiesced")):
                return _fail(
                    "cleanup_test_repo_services(ROOT) must quiesce sibling temp test repo services, "
                    f"got: {foreign_temp_receipt}"
                )
            foreign_remaining = _wait_for_repo_service_pids_to_clear(foreign_repo_root, timeout_s=4.0)
            if foreign_remaining:
                return _fail(
                    "cleanup_test_repo_services(ROOT) must not leave sibling temp test repo service pids behind, "
                    f"got: {foreign_remaining}"
                )

        nested_repo_root = (ROOT / ".loop" / "test-process-hygiene-root-boundary").resolve()
        cleanup_test_repo_services(repo_root=nested_repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
        shutil.rmtree(nested_repo_root, ignore_errors=True)
        nested_repo_root.mkdir(parents=True, exist_ok=True)
        try:
            root_cleanup = cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(root_cleanup.get("quiesced")):
                return _fail("root cleanup must quiesce before nested repo-root boundary coverage")
            nested_started = ensure_repo_control_plane_services_running(repo_root=nested_repo_root)
            if int(dict(nested_started.get("repo_control_plane") or {}).get("pid") or 0) <= 0:
                return _fail("nested repo-root boundary setup must start repo control-plane services")
            nested_matches = _repo_service_processes_by_kind(repo_root=nested_repo_root)
            if not nested_matches:
                return _fail("nested repo-root boundary setup must observe nested repo services")
            root_matches = _repo_service_processes_by_kind(repo_root=ROOT)
            if root_matches:
                return _fail(
                    "root repo-service matcher must ignore nested fake repo roots under .loop, "
                    f"got: {root_matches}"
                )
            root_cleanup = cleanup_test_repo_services(repo_root=ROOT, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(root_cleanup.get("quiesced")):
                return _fail(
                    "cleanup_test_repo_services(ROOT) must ignore nested fake repo roots instead of stalling on them, "
                    f"got: {root_cleanup}"
                )
            nested_remaining = _repo_service_pids_for_repo(nested_repo_root)
            if not nested_remaining:
                return _fail(
                    "cleanup_test_repo_services(ROOT) must not reap nested fake repo services that belong to a "
                    f"different repo-root boundary, got: {root_cleanup}"
                )
        finally:
            nested_cleanup = cleanup_test_repo_services(repo_root=nested_repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            shutil.rmtree(nested_repo_root, ignore_errors=True)
            if not bool(nested_cleanup.get("quiesced")):
                return _fail(
                    "nested repo-root boundary cleanup must quiesce the nested fake repo services, "
                    f"got: {nested_cleanup}"
                )

        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)

        def _cleanup_runtime_root() -> None:
            cleanup_receipt = cleanup_test_runtime_root(state_root=state_root, repo_root=temp_root)
            if not bool(cleanup_receipt.get("quiesced")):
                raise RuntimeError(
                    "process hygiene fixture cleanup must quiesce the temporary runtime root, "
                    f"got: {cleanup_receipt}"
                )
            if list(cleanup_receipt.get("remaining_owned_processes") or []):
                raise RuntimeError(
                    "process hygiene fixture cleanup must not leave owned processes behind, "
                    f"got: {cleanup_receipt}"
                )
            repo_cleanup_receipt = cleanup_test_repo_services(repo_root=temp_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(repo_cleanup_receipt.get("quiesced")):
                raise RuntimeError(
                    "process hygiene fixture cleanup must quiesce repo-scope services, "
                    f"got: {repo_cleanup_receipt}"
                )
            if dict(repo_cleanup_receipt.get("remaining_live_services") or {}):
                raise RuntimeError(
                    "process hygiene fixture cleanup must not leave repo-scope services behind, "
                    f"got: {repo_cleanup_receipt}"
                )
            if dict(repo_cleanup_receipt.get("remaining_retired_pids") or {}):
                raise RuntimeError(
                    "process hygiene fixture cleanup must not leave retired repo-scope pids behind, "
                    f"got: {repo_cleanup_receipt}"
                )
            remaining = _wait_for_repo_service_pids_to_clear(temp_root)
            if remaining:
                raise RuntimeError(
                    "process hygiene fixture cleanup must not leave repo-scope service pids behind, "
                    f"got: {remaining}"
                )

        def _run_hygiene_body() -> int:
            node = NodeSpec(
                node_id="child-process-hygiene-001",
                node_kind="implementer",
                goal_slice="finish the exact same child without residual recovery churn",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"thinking_budget": "medium", "role": "implementer"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "evaluate", "report", "split_request"],
                delegation_ref="state/delegations/child-process-hygiene-001.json",
                result_sink_ref="artifacts/child-process-hygiene-001/implementer_result.json",
                lineage_ref="root-kernel->child-process-hygiene-001",
                status=NodeStatus.ACTIVE,
            )
            kernel_state = KernelState(
                task_id="process-hygiene",
                root_goal="settle terminal runtime status without stale active recovery",
                root_node_id="root-kernel",
            )
            kernel_state.register_node(
                NodeSpec(
                    node_id="root-kernel",
                    node_kind="kernel",
                    goal_slice="supervise child hygiene",
                    parent_node_id=None,
                    generation=0,
                    round_id="R0",
                    execution_policy={"mode": "kernel"},
                    reasoning_profile={"thinking_budget": "medium", "role": "kernel"},
                    budget_profile={"max_rounds": 1},
                    allowed_actions=["dispatch", "submit", "audit"],
                    delegation_ref="",
                    result_sink_ref="artifacts/kernel.json",
                    lineage_ref="root-kernel",
                    status=NodeStatus.ACTIVE,
                )
            )
            kernel_state.register_node(node)
            persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

            launch_result_ref = state_root / "artifacts" / "launches" / node.node_id / "attempt_001" / "ChildLaunchResult.json"
            stdout_ref = launch_result_ref.parent / "logs" / "child.stdout.txt"
            stderr_ref = launch_result_ref.parent / "logs" / "child.stderr.txt"
            launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
            stdout_ref.parent.mkdir(parents=True, exist_ok=True)
            stdout_ref.write_text("", encoding="utf-8")
            stderr_ref.write_text("", encoding="utf-8")
            (workspace_root / "CHILD_PROMPT.md").write_text("PROMPT\n", encoding="utf-8")
            launch_result_payload = {
                "launch_decision": "started",
                "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                "node_id": node.node_id,
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
                "startup_health_timeout_ms": 250,
                "startup_retry_limit": 0,
                "attempt_count": 1,
                "retryable_failure_kind": "",
                "attempts": [],
                "launch_request_ref": str((launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                "launch_result_ref": str(launch_result_ref.resolve()),
                "launch_log_dir": str((launch_result_ref.parent / "logs").resolve()),
                "stdout_ref": str(stdout_ref.resolve()),
                "stderr_ref": str(stderr_ref.resolve()),
                "stdin_ref": str((workspace_root / "CHILD_PROMPT.md").resolve()),
                "wrapped_argv": ["/bin/sh", "-lc", "printf 'terminal lane\\n'"],
                "wrapper_cmd": "",
                "pid": 999999,
                "exit_code": None,
            }
            launch_result_ref.write_text(json.dumps(launch_result_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            stale_epoch = time.time() - 25.0
            for path in (launch_result_ref, stdout_ref, stderr_ref):
                os.utime(path, (stale_epoch, stale_epoch))

            implementer_result_ref = state_root / "artifacts" / node.node_id / "implementer_result.json"
            implementer_result_ref.parent.mkdir(parents=True, exist_ok=True)
            incomplete_result_payload = {
                "schema": "loop_product.implementer_result",
                "schema_version": "0.1.0",
                "node_id": node.node_id,
                "status": "COMPLETED",
                "outcome": "COMPLETED",
                "delivered_artifact_exactly_evaluated": True,
            }
            implementer_result_ref.write_text(
                json.dumps(incomplete_result_payload, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
            )

            previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            try:
                status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                    result_ref=launch_result_ref,
                    stall_threshold_s=0.0,
                )
            finally:
                if previous_runtime_status_mode is None:
                    os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
                else:
                    os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if not bool(status_payload.get("recovery_eligible")):
                return _fail("completed implementer_result without evaluator-backed evidence must remain recovery-eligible")
            if str(status_payload.get("recovery_reason") or "") == "authoritative_terminal_result":
                return _fail("incomplete/manual implementer_result must not be accepted as authoritative terminal closure")
            if str(status_payload.get("lifecycle_status") or "") == "COMPLETED":
                return _fail("incomplete/manual implementer_result must not force lifecycle_status=COMPLETED")

            evaluation_report_ref = temp_root / "EvaluationReport.json"
            evaluation_report_ref.write_text(
                json.dumps({"status": "COMPLETED", "verdict": "PASS"}, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            implementer_result_ref.write_text(
                json.dumps(
                    {
                        **incomplete_result_payload,
                        "evaluation_report_ref": str(evaluation_report_ref.resolve()),
                        "evaluator_result": {"verdict": "PASS", "retryable": False},
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            try:
                status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                    result_ref=launch_result_ref,
                    stall_threshold_s=0.0,
                )
            finally:
                if previous_runtime_status_mode is None:
                    os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
                else:
                    os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
            if bool(status_payload.get("recovery_eligible")):
                return _fail("runtime status must not keep a node recovery-eligible once authoritative terminal result exists")
            if str(status_payload.get("recovery_reason") or "") != "authoritative_terminal_result":
                return _fail("runtime status must explain terminal closeout with authoritative_terminal_result")
            if str(status_payload.get("lifecycle_status") or "") != "COMPLETED":
                return _fail("runtime status must surface terminal lifecycle_status once authoritative terminal result exists")

            active_hygiene_workspace_root = workspace_root / "child-hygiene-live-001"
            active_hygiene_live_root = active_hygiene_workspace_root / ".tmp_primary_artifact"
            active_hygiene_artifact_root = active_hygiene_workspace_root / "deliverables" / "primary_artifact"
            active_hygiene_receipt_ref = (
                state_root / "artifacts" / "publication" / "child-hygiene-live-001" / "WorkspaceArtifactPublicationReceipt.json"
            )
            active_hygiene_workspace_root.mkdir(parents=True, exist_ok=True)
            (active_hygiene_workspace_root / "FROZEN_HANDOFF.json").write_text(
                json.dumps(
                    {
                        "node_id": "child-hygiene-live-001",
                        "workspace_live_artifact_ref": str(active_hygiene_live_root.resolve()),
                        "workspace_mirror_ref": str(active_hygiene_artifact_root.resolve()),
                        "artifact_publication_receipt_ref": str(active_hygiene_receipt_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            (active_hygiene_live_root / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
            (active_hygiene_live_root / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text("live-cache\n", encoding="utf-8")
            (active_hygiene_live_root / "build" / "lib").mkdir(parents=True, exist_ok=True)
            (active_hygiene_live_root / "build" / "lib" / "artifact.txt").write_text("live-build\n", encoding="utf-8")
            (active_hygiene_artifact_root / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
            (active_hygiene_artifact_root / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text("cached\n", encoding="utf-8")
            (active_hygiene_artifact_root / ".git").mkdir(parents=True, exist_ok=True)
            (active_hygiene_artifact_root / ".git" / "config").write_text("[core]\n\trepositoryformatversion = 0\n", encoding="utf-8")
            (active_hygiene_artifact_root / ".venv" / "bin").mkdir(parents=True, exist_ok=True)
            (active_hygiene_artifact_root / ".venv" / "bin" / "python").write_text("# stub\n", encoding="utf-8")
            (active_hygiene_artifact_root / ".uv-cache").mkdir(parents=True, exist_ok=True)
            (active_hygiene_artifact_root / ".uv-cache" / "cache.txt").write_text("cache\n", encoding="utf-8")
            (active_hygiene_artifact_root / "build" / "lib").mkdir(parents=True, exist_ok=True)
            (active_hygiene_artifact_root / "build" / "lib" / "artifact.txt").write_text("build\n", encoding="utf-8")
            (active_hygiene_artifact_root / "_lake_build" / "lib").mkdir(parents=True, exist_ok=True)
            (active_hygiene_artifact_root / "_lake_build" / "lib" / "artifact.txt").write_text("lake-build\n", encoding="utf-8")
            active_hygiene_kernel_state = load_kernel_state(state_root)
            active_hygiene_node = NodeSpec(
                node_id="child-hygiene-live-001",
                node_kind="implementer",
                goal_slice="keep runtime heavy trees out of live deliverables",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1.live",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "report"],
                workspace_root=str(active_hygiene_workspace_root.resolve()),
                delegation_ref="state/delegations/child-hygiene-live-001.json",
                result_sink_ref="artifacts/child-hygiene-live-001/result.json",
                lineage_ref="root-kernel->child-hygiene-live-001",
                status=NodeStatus.ACTIVE,
            )
            active_hygiene_kernel_state.register_node(active_hygiene_node)
            persist_kernel_state(state_root, active_hygiene_kernel_state, authority=kernel_internal_authority())
            _ = query_authority_view(state_root)
            for heavyweight_relpath in (".lake", ".git", ".venv", ".uv-cache", "build", "_lake_build"):
                if not (active_hygiene_artifact_root / heavyweight_relpath).exists():
                    return _fail(
                        "authority sync must not silently scrub the current publish root for an ACTIVE node; "
                        f"it unexpectedly removed {heavyweight_relpath}"
                    )
            for live_relpath in (".lake", "build"):
                if not (active_hygiene_live_root / live_relpath).exists():
                    return _fail(
                        "authority sync must leave the distinct live build root intact instead of canonicalizing it as a publish root, "
                        f"but removed {live_relpath} from .tmp_primary_artifact"
                    )

            active_runtime_hygiene_workspace_root = workspace_root / "child-hygiene-live-status-001"
            active_runtime_hygiene_live_root = active_runtime_hygiene_workspace_root / ".tmp_primary_artifact"
            active_runtime_hygiene_artifact_root = active_runtime_hygiene_workspace_root / "deliverables" / "primary_artifact"
            active_runtime_hygiene_receipt_ref = (
                state_root / "artifacts" / "publication" / "child-hygiene-live-status-001" / "WorkspaceArtifactPublicationReceipt.json"
            )
            active_runtime_hygiene_workspace_root.mkdir(parents=True, exist_ok=True)
            (active_runtime_hygiene_workspace_root / "FROZEN_HANDOFF.json").write_text(
                json.dumps(
                    {
                        "node_id": "child-hygiene-live-status-001",
                        "workspace_live_artifact_ref": str(active_runtime_hygiene_live_root.resolve()),
                        "workspace_mirror_ref": str(active_runtime_hygiene_artifact_root.resolve()),
                        "artifact_publication_receipt_ref": str(active_runtime_hygiene_receipt_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            (active_runtime_hygiene_live_root / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
            (active_runtime_hygiene_live_root / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text("live-cache\n", encoding="utf-8")
            (active_runtime_hygiene_live_root / "build" / "lib").mkdir(parents=True, exist_ok=True)
            (active_runtime_hygiene_live_root / "build" / "lib" / "artifact.txt").write_text("live-build\n", encoding="utf-8")
            (active_runtime_hygiene_artifact_root / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
            (active_runtime_hygiene_artifact_root / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text("cached\n", encoding="utf-8")
            (active_runtime_hygiene_artifact_root / "build" / "lib").mkdir(parents=True, exist_ok=True)
            (active_runtime_hygiene_artifact_root / "build" / "lib" / "artifact.txt").write_text("build\n", encoding="utf-8")
            runtime_hygiene_node = NodeSpec(
                node_id="child-hygiene-live-status-001",
                node_kind="implementer",
                goal_slice="runtime status polling must scrub heavy trees from active child deliverables",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1.status",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "report"],
                workspace_root=str(active_runtime_hygiene_workspace_root.resolve()),
                delegation_ref="state/delegations/child-hygiene-live-status-001.json",
                result_sink_ref="artifacts/child-hygiene-live-status-001/result.json",
                lineage_ref="root-kernel->child-hygiene-live-status-001",
                status=NodeStatus.ACTIVE,
            )
            active_runtime_kernel_state = load_kernel_state(state_root)
            active_runtime_kernel_state.register_node(runtime_hygiene_node)
            persist_kernel_state(state_root, active_runtime_kernel_state, authority=kernel_internal_authority())
            persist_node_snapshot(
                state_root,
                active_runtime_kernel_state.nodes[runtime_hygiene_node.node_id],
                authority=kernel_internal_authority(),
            )

            runtime_stdout_ref = state_root / "artifacts" / "launches" / runtime_hygiene_node.node_id / "attempt_001" / "logs" / "child.stdout.txt"
            runtime_stderr_ref = state_root / "artifacts" / "launches" / runtime_hygiene_node.node_id / "attempt_001" / "logs" / "child.stderr.txt"
            runtime_launch_result_ref = state_root / "artifacts" / "launches" / runtime_hygiene_node.node_id / "attempt_001" / "ChildLaunchResult.json"
            runtime_stdout_ref.parent.mkdir(parents=True, exist_ok=True)
            runtime_stdout_ref.write_text("writing first real artifact batch now\n", encoding="utf-8")
            runtime_stderr_ref.write_text("", encoding="utf-8")
            live_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=str(ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                runtime_launch_result_ref.write_text(
                    json.dumps(
                        {
                            "launch_decision": "started",
                            "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                            "node_id": runtime_hygiene_node.node_id,
                            "workspace_root": str(active_runtime_hygiene_workspace_root.resolve()),
                            "state_root": str(state_root.resolve()),
                            "startup_health_timeout_ms": 250,
                            "startup_retry_limit": 0,
                            "attempt_count": 1,
                            "retryable_failure_kind": "",
                            "attempts": [],
                            "launch_request_ref": str((runtime_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                            "launch_result_ref": str(runtime_launch_result_ref.resolve()),
                            "launch_log_dir": str(runtime_stdout_ref.parent.resolve()),
                            "stdout_ref": str(runtime_stdout_ref.resolve()),
                            "stderr_ref": str(runtime_stderr_ref.resolve()),
                            "stdin_ref": str((active_runtime_hygiene_workspace_root / "CHILD_PROMPT.md").resolve()),
                            "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(60)"],
                            "wrapper_cmd": "",
                            "pid": int(live_proc.pid),
                            "exit_code": None,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                status_payload = _query_direct_runtime_status_until(
                    launch_runtime_module=launch_runtime_module,
                    result_ref=runtime_launch_result_ref,
                    pre_wait_s=5.0,
                    stall_threshold_s=60.0,
                )
                if str(status_payload.get("lifecycle_status") or "") != "ACTIVE":
                    return _fail("live runtime-status hygiene fixture must stay ACTIVE while the child pid is still attached")
                if str(status_payload.get("recovery_reason") or "") != "publish_root_materialized_without_publication_receipt":
                    return _fail("runtime status polling must classify direct publish-root mutation as a publication violation")
                if not bool(status_payload.get("recovery_eligible")):
                    return _fail("publish-root publication violation must become recovery-eligible")
                if bool(status_payload.get("pid_alive")):
                    return _fail("runtime status polling must terminate the live child before surfacing publish-root violation recovery")
                if active_runtime_hygiene_artifact_root.exists():
                    return _fail("runtime status polling must clear the invalid publish root after publication-violation detection")
                for live_relpath in (".lake", "build"):
                    if not (active_runtime_hygiene_live_root / live_relpath).exists():
                        return _fail(
                            "runtime status polling must preserve the live build root while invalidating the publish root, "
                            f"but removed {live_relpath} from .tmp_primary_artifact"
                        )
            finally:
                live_proc.terminate()
                try:
                    live_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    live_proc.kill()
                    live_proc.wait(timeout=5.0)

            external_live_workspace_root = workspace_root / "child-external-live-hygiene-001"
            external_live_publish_root = external_live_workspace_root / "deliverables" / "primary_artifact"
            external_live_root = node_live_artifact_root(
                state_root=state_root,
                node_id="child-external-live-hygiene-001",
                workspace_mirror_relpath="deliverables/primary_artifact",
            )
            external_live_receipt_ref = (
                state_root / "artifacts" / "publication" / "child-external-live-hygiene-001" / "WorkspaceArtifactPublicationReceipt.json"
            )
            external_live_workspace_root.mkdir(parents=True, exist_ok=True)
            external_live_root.mkdir(parents=True, exist_ok=True)
            (external_live_root / "README.md").write_text("# external live root\n", encoding="utf-8")
            (external_live_workspace_root / "FROZEN_HANDOFF.json").write_text(
                json.dumps(
                    {
                        "node_id": "child-external-live-hygiene-001",
                        "workspace_live_artifact_ref": str(external_live_root.resolve()),
                        "workspace_mirror_ref": str(external_live_publish_root.resolve()),
                        "artifact_publication_receipt_ref": str(external_live_receipt_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            (external_live_workspace_root / ".tmp_primary_artifact" / ".lake" / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
            (external_live_workspace_root / ".tmp_primary_artifact" / ".lake" / "packages" / "mathlib" / "dummy.txt").write_text(
                "bad-local-cache\n",
                encoding="utf-8",
            )
            (external_live_workspace_root / ".tmp_primary_artifact" / "build" / "lib").mkdir(parents=True, exist_ok=True)
            (external_live_workspace_root / ".tmp_primary_artifact" / "build" / "lib" / "artifact.txt").write_text(
                "bad-local-build\n",
                encoding="utf-8",
            )
            external_live_node = NodeSpec(
                node_id="child-external-live-hygiene-001",
                node_kind="implementer",
                goal_slice="workspace-local heavy trees must fail closed when live root is external",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1.external",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "report"],
                workspace_root=str(external_live_workspace_root.resolve()),
                delegation_ref="state/delegations/child-external-live-hygiene-001.json",
                result_sink_ref="artifacts/child-external-live-hygiene-001/result.json",
                lineage_ref="root-kernel->child-external-live-hygiene-001",
                status=NodeStatus.ACTIVE,
            )
            external_live_kernel_state = load_kernel_state(state_root)
            external_live_kernel_state.register_node(external_live_node)
            persist_kernel_state(state_root, external_live_kernel_state, authority=kernel_internal_authority())
            persist_node_snapshot(
                state_root,
                external_live_kernel_state.nodes[external_live_node.node_id],
                authority=kernel_internal_authority(),
            )
            external_stdout_ref = (
                state_root / "artifacts" / "launches" / external_live_node.node_id / "attempt_001" / "logs" / "child.stdout.txt"
            )
            external_stderr_ref = (
                state_root / "artifacts" / "launches" / external_live_node.node_id / "attempt_001" / "logs" / "child.stderr.txt"
            )
            external_launch_result_ref = (
                state_root / "artifacts" / "launches" / external_live_node.node_id / "attempt_001" / "ChildLaunchResult.json"
            )
            external_stdout_ref.parent.mkdir(parents=True, exist_ok=True)
            external_stdout_ref.write_text("writing calibration branch package\n", encoding="utf-8")
            external_stderr_ref.write_text("", encoding="utf-8")
            external_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=str(ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                external_launch_result_ref.write_text(
                    json.dumps(
                        {
                            "launch_decision": "started",
                            "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                            "node_id": external_live_node.node_id,
                            "workspace_root": str(external_live_workspace_root.resolve()),
                            "state_root": str(state_root.resolve()),
                            "startup_health_timeout_ms": 250,
                            "startup_retry_limit": 0,
                            "attempt_count": 1,
                            "retryable_failure_kind": "",
                            "attempts": [],
                            "launch_request_ref": str((external_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                            "launch_result_ref": str(external_launch_result_ref.resolve()),
                            "launch_log_dir": str(external_stdout_ref.parent.resolve()),
                            "stdout_ref": str(external_stdout_ref.resolve()),
                            "stderr_ref": str(external_stderr_ref.resolve()),
                            "stdin_ref": str((external_live_workspace_root / "CHILD_PROMPT.md").resolve()),
                            "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(60)"],
                            "wrapper_cmd": "",
                            "pid": int(external_proc.pid),
                            "exit_code": None,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                external_status_payload = _query_direct_runtime_status_until(
                    launch_runtime_module=launch_runtime_module,
                    result_ref=external_launch_result_ref,
                    pre_wait_s=5.0,
                    stall_threshold_s=60.0,
                )
                if str(external_status_payload.get("recovery_reason") or "") != "workspace_contains_local_runtime_heavy_trees":
                    return _fail("external-live-root workspace pollution must surface workspace_contains_local_runtime_heavy_trees")
                if not bool(external_status_payload.get("recovery_eligible")):
                    return _fail("external-live-root workspace pollution must become recovery-eligible")
                if bool(external_status_payload.get("pid_alive")):
                    return _fail("runtime status polling must terminate the live child before surfacing external-live-root workspace pollution")
                if (external_live_workspace_root / ".tmp_primary_artifact").exists():
                    return _fail("runtime status polling must scrub invalid local .tmp_primary_artifact trees when live root is external")
                if not external_live_root.exists():
                    return _fail("runtime status polling must preserve the external live artifact root while scrubbing invalid workspace-local trees")
            finally:
                external_proc.terminate()
                try:
                    external_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    external_proc.kill()
                    external_proc.wait(timeout=5.0)

            live_root_symlink_workspace_root = workspace_root / "child-live-root-symlink-001"
            live_root_symlink_publish_root = live_root_symlink_workspace_root / "deliverables" / "primary_artifact"
            live_root_symlink_live_root = node_live_artifact_root(
                state_root=state_root,
                node_id="child-live-root-symlink-001",
                workspace_mirror_relpath="deliverables/primary_artifact",
            )
            live_root_symlink_receipt_ref = (
                state_root / "artifacts" / "publication" / "child-live-root-symlink-001" / "WorkspaceArtifactPublicationReceipt.json"
            )
            live_root_symlink_mount_root = (
                state_root / "artifacts" / "live_lake" / "child-live-root-symlink-001" / "primary_artifact"
            )
            live_root_symlink_workspace_root.mkdir(parents=True, exist_ok=True)
            live_root_symlink_live_root.mkdir(parents=True, exist_ok=True)
            live_root_symlink_mount_root.mkdir(parents=True, exist_ok=True)
            (live_root_symlink_mount_root / "packages" / "mathlib").mkdir(parents=True, exist_ok=True)
            (live_root_symlink_live_root / "README.md").write_text("# externalized live root\n", encoding="utf-8")
            (live_root_symlink_live_root / ".lake").symlink_to(live_root_symlink_mount_root, target_is_directory=True)
            (live_root_symlink_workspace_root / "FROZEN_HANDOFF.json").write_text(
                json.dumps(
                    {
                        "node_id": "child-live-root-symlink-001",
                        "workspace_live_artifact_ref": str(live_root_symlink_live_root.resolve()),
                        "workspace_mirror_ref": str(live_root_symlink_publish_root.resolve()),
                        "artifact_publication_receipt_ref": str(live_root_symlink_receipt_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            live_root_symlink_node = NodeSpec(
                node_id="child-live-root-symlink-001",
                node_kind="implementer",
                goal_slice="externalized .lake symlinks in the external live root must remain allowed",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1.live-symlink",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "report"],
                workspace_root=str(live_root_symlink_workspace_root.resolve()),
                delegation_ref="state/delegations/child-live-root-symlink-001.json",
                result_sink_ref="artifacts/child-live-root-symlink-001/result.json",
                lineage_ref="root-kernel->child-live-root-symlink-001",
                status=NodeStatus.ACTIVE,
            )
            live_root_symlink_kernel_state = load_kernel_state(state_root)
            live_root_symlink_kernel_state.register_node(live_root_symlink_node)
            persist_kernel_state(state_root, live_root_symlink_kernel_state, authority=kernel_internal_authority())
            persist_node_snapshot(
                state_root,
                live_root_symlink_kernel_state.nodes[live_root_symlink_node.node_id],
                authority=kernel_internal_authority(),
            )
            live_root_symlink_stdout_ref = (
                state_root / "artifacts" / "launches" / live_root_symlink_node.node_id / "attempt_001" / "logs" / "child.stdout.txt"
            )
            live_root_symlink_stderr_ref = (
                state_root / "artifacts" / "launches" / live_root_symlink_node.node_id / "attempt_001" / "logs" / "child.stderr.txt"
            )
            live_root_symlink_launch_result_ref = (
                state_root / "artifacts" / "launches" / live_root_symlink_node.node_id / "attempt_001" / "ChildLaunchResult.json"
            )
            live_root_symlink_stdout_ref.parent.mkdir(parents=True, exist_ok=True)
            live_root_symlink_stdout_ref.write_text("using externalized live-lake mount\n", encoding="utf-8")
            live_root_symlink_stderr_ref.write_text("", encoding="utf-8")
            live_root_symlink_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=str(ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                live_root_symlink_launch_result_ref.write_text(
                    json.dumps(
                        {
                            "launch_decision": "started",
                            "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                            "node_id": live_root_symlink_node.node_id,
                            "workspace_root": str(live_root_symlink_workspace_root.resolve()),
                            "state_root": str(state_root.resolve()),
                            "startup_health_timeout_ms": 250,
                            "startup_retry_limit": 0,
                            "attempt_count": 1,
                            "retryable_failure_kind": "",
                            "attempts": [],
                            "launch_request_ref": str((live_root_symlink_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                            "launch_result_ref": str(live_root_symlink_launch_result_ref.resolve()),
                            "launch_log_dir": str(live_root_symlink_stdout_ref.parent.resolve()),
                            "stdout_ref": str(live_root_symlink_stdout_ref.resolve()),
                            "stderr_ref": str(live_root_symlink_stderr_ref.resolve()),
                            "stdin_ref": str((live_root_symlink_workspace_root / "CHILD_PROMPT.md").resolve()),
                            "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(60)"],
                            "wrapper_cmd": "",
                            "pid": int(live_root_symlink_proc.pid),
                            "exit_code": None,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
                try:
                    live_root_symlink_status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                        result_ref=live_root_symlink_launch_result_ref,
                        stall_threshold_s=60.0,
                    )
                finally:
                    if previous_runtime_status_mode is None:
                        os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
                    else:
                        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
                if str(live_root_symlink_status_payload.get("recovery_reason") or "") == "live_artifact_root_contains_runtime_heavy_trees":
                    return _fail("externalized live-root .lake symlinks must not surface live_artifact_root_contains_runtime_heavy_trees")
                if bool(live_root_symlink_status_payload.get("recovery_eligible")):
                    return _fail("externalized live-root .lake symlinks must not become recovery-eligible")
                if not bool(live_root_symlink_status_payload.get("pid_alive")):
                    return _fail("externalized live-root .lake symlinks must not terminate an otherwise healthy live child")
                if not (live_root_symlink_live_root / ".lake").is_symlink():
                    return _fail("externalized live-root .lake mount must remain a symlink after runtime status polling")

                from loop_product.dispatch import publication as publication_module

                captured_copytree_kwargs: list[dict[str, object]] = []
                original_copytree = publication_module.shutil.copytree

                def _capturing_copytree(src, dst, *args, **kwargs):
                    captured_copytree_kwargs.append(dict(kwargs))
                    return original_copytree(src, dst, *args, **kwargs)

                publication_module.shutil.copytree = _capturing_copytree
                try:
                    publication_payload = publication_module.publish_workspace_artifact_snapshot(
                        node_id=live_root_symlink_node.node_id,
                        live_artifact_ref=live_root_symlink_live_root,
                        publish_artifact_ref=live_root_symlink_publish_root,
                        publication_receipt_ref=live_root_symlink_receipt_ref,
                    )
                finally:
                    publication_module.shutil.copytree = original_copytree
                if not captured_copytree_kwargs:
                    return _fail("workspace publication from an externalized live-root symlink fixture must call copytree")
                if captured_copytree_kwargs[-1].get("symlinks") is not True:
                    return _fail("workspace publication must preserve live-root heavy-tree symlinks during staging copy")
                if (live_root_symlink_publish_root / ".lake").exists():
                    return _fail("workspace publication must prune .lake from the published artifact root")
                removed_runtime_heavy_trees = [str(item) for item in list(publication_payload.get("removed_runtime_heavy_trees") or [])]
                if not any(item.endswith("/.lake") for item in removed_runtime_heavy_trees):
                    return _fail("workspace publication receipt must record that the staged .lake symlink was pruned")
            finally:
                live_root_symlink_proc.terminate()
                try:
                    live_root_symlink_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    live_root_symlink_proc.kill()
                    live_root_symlink_proc.wait(timeout=5.0)

            live_root_heavy_workspace_root = workspace_root / "child-live-root-heavy-001"
            live_root_heavy_publish_root = live_root_heavy_workspace_root / "deliverables" / "primary_artifact"
            live_root_heavy_live_root = node_live_artifact_root(
                state_root=state_root,
                node_id="child-live-root-heavy-001",
                workspace_mirror_relpath="deliverables/primary_artifact",
            )
            live_root_heavy_receipt_ref = (
                state_root / "artifacts" / "publication" / "child-live-root-heavy-001" / "WorkspaceArtifactPublicationReceipt.json"
            )
            live_root_heavy_workspace_root.mkdir(parents=True, exist_ok=True)
            live_root_heavy_live_root.mkdir(parents=True, exist_ok=True)
            (live_root_heavy_live_root / "README.md").write_text("# dirty live root\n", encoding="utf-8")
            (
                live_root_heavy_live_root / ".lake" / "packages" / "mathlib" / ".git"
            ).mkdir(parents=True, exist_ok=True)
            (
                live_root_heavy_live_root / ".lake" / "packages" / "mathlib" / ".git" / "config"
            ).write_text("[core]\n", encoding="utf-8")
            (live_root_heavy_workspace_root / "FROZEN_HANDOFF.json").write_text(
                json.dumps(
                    {
                        "node_id": "child-live-root-heavy-001",
                        "workspace_live_artifact_ref": str(live_root_heavy_live_root.resolve()),
                        "workspace_mirror_ref": str(live_root_heavy_publish_root.resolve()),
                        "artifact_publication_receipt_ref": str(live_root_heavy_receipt_ref.resolve()),
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            live_root_heavy_node = NodeSpec(
                node_id="child-live-root-heavy-001",
                node_kind="implementer",
                goal_slice="materialized .lake trees in the external live root must fail closed",
                parent_node_id="root-kernel",
                generation=1,
                round_id="R1.live-heavy",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"thinking_budget": "high", "role": "implementer"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "report"],
                workspace_root=str(live_root_heavy_workspace_root.resolve()),
                delegation_ref="state/delegations/child-live-root-heavy-001.json",
                result_sink_ref="artifacts/child-live-root-heavy-001/result.json",
                lineage_ref="root-kernel->child-live-root-heavy-001",
                status=NodeStatus.ACTIVE,
            )
            live_root_heavy_kernel_state = load_kernel_state(state_root)
            live_root_heavy_kernel_state.register_node(live_root_heavy_node)
            persist_kernel_state(state_root, live_root_heavy_kernel_state, authority=kernel_internal_authority())
            persist_node_snapshot(
                state_root,
                live_root_heavy_kernel_state.nodes[live_root_heavy_node.node_id],
                authority=kernel_internal_authority(),
            )
            live_root_heavy_stdout_ref = (
                state_root / "artifacts" / "launches" / live_root_heavy_node.node_id / "attempt_001" / "logs" / "child.stdout.txt"
            )
            live_root_heavy_stderr_ref = (
                state_root / "artifacts" / "launches" / live_root_heavy_node.node_id / "attempt_001" / "logs" / "child.stderr.txt"
            )
            live_root_heavy_launch_result_ref = (
                state_root / "artifacts" / "launches" / live_root_heavy_node.node_id / "attempt_001" / "ChildLaunchResult.json"
            )
            live_root_heavy_stdout_ref.parent.mkdir(parents=True, exist_ok=True)
            live_root_heavy_stdout_ref.write_text("building live root package\n", encoding="utf-8")
            live_root_heavy_stderr_ref.write_text("", encoding="utf-8")
            live_root_heavy_proc = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                cwd=str(ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            try:
                live_root_heavy_launch_result_ref.write_text(
                    json.dumps(
                        {
                            "launch_decision": "started",
                            "source_result_ref": str((state_root / "artifacts" / "bootstrap" / "FirstImplementerBootstrapResult.json").resolve()),
                            "node_id": live_root_heavy_node.node_id,
                            "workspace_root": str(live_root_heavy_workspace_root.resolve()),
                            "state_root": str(state_root.resolve()),
                            "startup_health_timeout_ms": 250,
                            "startup_retry_limit": 0,
                            "attempt_count": 1,
                            "retryable_failure_kind": "",
                            "attempts": [],
                            "launch_request_ref": str((live_root_heavy_launch_result_ref.parent / "ChildLaunchRequest.json").resolve()),
                            "launch_result_ref": str(live_root_heavy_launch_result_ref.resolve()),
                            "launch_log_dir": str(live_root_heavy_stdout_ref.parent.resolve()),
                            "stdout_ref": str(live_root_heavy_stdout_ref.resolve()),
                            "stderr_ref": str(live_root_heavy_stderr_ref.resolve()),
                            "stdin_ref": str((live_root_heavy_workspace_root / "CHILD_PROMPT.md").resolve()),
                            "wrapped_argv": [sys.executable, "-c", "import time; time.sleep(60)"],
                            "wrapper_cmd": "",
                            "pid": int(live_root_heavy_proc.pid),
                            "exit_code": None,
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                    encoding="utf-8",
                )
                previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
                os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
                try:
                    live_root_heavy_status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                        result_ref=live_root_heavy_launch_result_ref,
                        stall_threshold_s=60.0,
                    )
                finally:
                    if previous_runtime_status_mode is None:
                        os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
                    else:
                        os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
                if str(live_root_heavy_status_payload.get("recovery_reason") or "") != "live_artifact_root_contains_runtime_heavy_trees":
                    return _fail("materialized live-root .lake trees must surface live_artifact_root_contains_runtime_heavy_trees")
                if not bool(live_root_heavy_status_payload.get("recovery_eligible")):
                    return _fail("materialized live-root heavy trees must become recovery-eligible")
                if bool(live_root_heavy_status_payload.get("pid_alive")):
                    return _fail("runtime status polling must terminate the live child before surfacing live-root runtime heavy-tree violations")
                if (live_root_heavy_live_root / ".lake").exists():
                    return _fail("runtime status polling must scrub materialized live-root .lake trees after violation detection")
                if not live_root_heavy_live_root.exists():
                    return _fail("runtime status polling must preserve the external live artifact root itself while scrubbing heavy trees")
            finally:
                live_root_heavy_proc.terminate()
                try:
                    live_root_heavy_proc.wait(timeout=5.0)
                except subprocess.TimeoutExpired:
                    live_root_heavy_proc.kill()
                    live_root_heavy_proc.wait(timeout=5.0)

                print("[loop-system-process-hygiene] OK")
                return 0
        try:
            return _run_hygiene_body()
        finally:
            _cleanup_runtime_root()


if __name__ == "__main__":
    raise SystemExit(main())
