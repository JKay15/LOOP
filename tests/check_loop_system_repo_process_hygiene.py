#!/usr/bin/env python3
"""Guard supervisor/runtime hygiene after terminal child closeout."""

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
    print(f"[loop-system-process-hygiene][FAIL] {msg}", file=sys.stderr)
    return 2


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
            supervisor_stdout, supervisor_stderr = supervisor_proc.communicate(timeout=3.0)
        except subprocess.TimeoutExpired:
            supervisor_proc.kill()
            supervisor_stdout, supervisor_stderr = supervisor_proc.communicate(timeout=3.0)
            return _fail("host child launch supervisor must exit when idle instead of lingering forever")
        if supervisor_proc.returncode != 0:
            detail = supervisor_stderr.strip() or supervisor_stdout.strip()
            return _fail(f"idle supervisor must exit cleanly: {detail}")
        if supervisor_runtime_ref.exists():
            return _fail("idle supervisor must clean its runtime marker on exit")

        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        workspace_root.mkdir(parents=True, exist_ok=True)
        ensure_runtime_tree(state_root)
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
            previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            try:
                status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                    result_ref=runtime_launch_result_ref,
                    stall_threshold_s=60.0,
                )
            finally:
                if previous_runtime_status_mode is None:
                    os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
                else:
                    os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
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
            previous_runtime_status_mode = os.environ.get("LOOP_CHILD_RUNTIME_STATUS_MODE")
            os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = "direct"
            try:
                external_status_payload = launch_runtime_module.child_runtime_status_from_launch_result_ref(
                    result_ref=external_launch_result_ref,
                    stall_threshold_s=60.0,
                )
            finally:
                if previous_runtime_status_mode is None:
                    os.environ.pop("LOOP_CHILD_RUNTIME_STATUS_MODE", None)
                else:
                    os.environ["LOOP_CHILD_RUNTIME_STATUS_MODE"] = previous_runtime_status_mode
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

    print("[loop-system-process-hygiene] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
