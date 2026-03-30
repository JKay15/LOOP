#!/usr/bin/env python3
"""Validate IF-5 read-only authority query surface."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from jsonschema import Draft202012Validator, validate


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-state-query][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


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


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.dispatch.heartbeat import build_child_dispatch_status
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.query import query_authority_view
        from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.runtime import cleanup_test_repo_services
    except Exception as exc:  # noqa: BLE001
        return _fail(f"loop system IF-5 imports failed: {exc}")

    try:
        authority_schema = _load_schema("LoopSystemAuthorityView.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"missing IF-5 schema: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise IF-5 authority query validation",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/authority_view.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="exercise authority query visibility",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-implementer-001.json",
        result_sink_ref="artifacts/authority_view.json",
        lineage_ref="root-kernel->child-implementer-001",
        status=NodeStatus.ACTIVE,
    )

    with tempfile.TemporaryDirectory(prefix="loop_system_state_query_") as td:
        repo_root = Path(td)
        state_root = repo_root / ".loop"
        try:
            ensure_runtime_tree(state_root)
            kernel_state = KernelState(
                task_id="wave5-state-query",
                root_goal="validate IF-5 authority query",
                root_node_id=root_node.node_id,
            )
            kernel_state.register_node(root_node)
            kernel_state.register_node(child_node)
            persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())

            submit_control_envelope(
                state_root,
                build_child_dispatch_status(child_node, note="child remains active while evaluator feedback is pending").to_envelope(),
            )
            submit_control_envelope(
                state_root,
                {
                    "source": child_node.node_id,
                    "envelope_type": "evaluator_result",
                    "payload_type": "evaluator_result",
                    "round_id": "R1.1",
                    "generation": child_node.generation,
                    "payload": EvaluatorResult(
                        verdict=EvaluatorVerdict.STUCK,
                        lane="reviewer",
                        summary="The evaluator is stuck on its own missing prerequisite and must self-repair.",
                        evidence_refs=["reviewer:stuck"],
                        retryable=False,
                        diagnostics={
                            "self_attribution": "evaluator_environment",
                            "self_repair": "repair the missing prerequisite before blaming implementation",
                        },
                    ).to_dict(),
                    "status": "report",
                    "note": "stuck evaluator report",
                },
            )

            authority_view = query_authority_view(state_root)
            validate(authority_view, authority_schema)

            expected_keys = {
                "task_id",
                "root_goal",
                "root_node_id",
                "status",
                "node_graph",
                "node_lifecycle",
                "effective_requirements",
                "budget_view",
                "blocked_reasons",
                "delegation_map",
                "active_evaluator_lanes",
                "active_child_nodes",
                "truthful_active_child_nodes",
                "nonlive_active_child_nodes",
                "planned_child_nodes",
            }
            if set(authority_view) != expected_keys:
                return _fail("authority view must stay simple and explicit rather than exposing raw kernel internals")
            if child_node.node_id not in authority_view["active_child_nodes"]:
                return _fail("authority view must directly expose active child nodes")
            if authority_view["planned_child_nodes"] != []:
                return _fail("authority view must expose planned_child_nodes separately from active child nodes")
            if authority_view["node_lifecycle"].get(child_node.node_id) != NodeStatus.ACTIVE.value:
                return _fail("authority view must expose per-node lifecycle state directly")
            if authority_view["delegation_map"].get(child_node.node_id) != root_node.node_id:
                return _fail("authority view must expose delegation map directly")
            if child_node.goal_slice not in authority_view["effective_requirements"]:
                return _fail("authority view must expose effective requirements directly")
            if not isinstance(authority_view["blocked_reasons"], dict):
                return _fail("authority view must expose blocked reasons as a structured dictionary")
            if not any(
                lane["node_id"] == child_node.node_id and lane["verdict"] == "STUCK"
                for lane in authority_view["active_evaluator_lanes"]
            ):
                return _fail("authority view must expose active evaluator lanes directly")
            if not any(node["node_id"] == root_node.node_id for node in authority_view["node_graph"]):
                return _fail("authority view must expose the current authoritative node graph directly")
        finally:
            cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(cleanup_receipt.get("quiesced")):
                raise RuntimeError(
                    "state query fixture cleanup must quiesce repo-scope services, "
                    f"got: {cleanup_receipt}"
                )
            remaining = _wait_for_repo_service_pids_to_clear(repo_root)
            if remaining:
                raise RuntimeError(
                    "state query fixture cleanup must not leave repo-scope service pids behind, "
                    f"got: {remaining}"
                )

    with tempfile.TemporaryDirectory(prefix="loop_system_state_query_self_heal_") as td:
        repo_root = Path(td)
        state_root = repo_root / ".loop"
        try:
            ensure_runtime_tree(state_root)
            self_heal_child = NodeSpec(
                node_id="child-implementer-self-heal",
                node_kind="implementer",
                goal_slice="exercise authority query self-heal visibility",
                parent_node_id=root_node.node_id,
                generation=1,
                round_id="R1",
                execution_policy={"sandbox_mode": "workspace-write"},
                reasoning_profile={"role": "implementer", "thinking_budget": "high"},
                budget_profile={"max_rounds": 2},
                allowed_actions=["implement", "evaluate", "report"],
                delegation_ref="state/delegations/child-implementer-self-heal.json",
                result_sink_ref="artifacts/child-implementer-self-heal/result.json",
                lineage_ref="root-kernel->child-implementer-self-heal",
                status=NodeStatus.ACTIVE,
            )
            kernel_state = KernelState(
                task_id="wave5-state-query-self-heal",
                root_goal="validate query-time authoritative self-heal",
                root_node_id=root_node.node_id,
            )
            kernel_state.register_node(root_node)
            kernel_state.register_node(self_heal_child)
            persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
            stale_child = self_heal_child.to_dict()
            stale_child["status"] = "FAILED"
            stale_child["runtime_state"] = {
                "attachment_state": "TERMINAL",
                "observed_at": "2026-03-23T13:06:23Z",
                "observation_kind": "authoritative_result",
                "summary": "simple evaluator reviewer verdict=FAIL",
                "evidence_refs": [str((state_root / "artifacts" / "child-implementer-self-heal" / "result.json").resolve())],
            }
            persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
            persist_node_snapshot(state_root, stale_child, authority=kernel_internal_authority())
            result_ref = state_root / "artifacts" / "child-implementer-self-heal" / "result.json"
            eval_report_ref = state_root / "artifacts" / "child-implementer-self-heal" / "EvaluationReport.json"
            result_ref.parent.mkdir(parents=True, exist_ok=True)
            eval_report_ref.write_text("{}\n", encoding="utf-8")
            result_ref.write_text(
                json.dumps(
                    {
                        "schema": "loop_product.implementer_result",
                        "schema_version": "0.1.0",
                        "node_id": self_heal_child.node_id,
                        "status": "COMPLETED",
                        "outcome": "COMPLETED",
                        "summary": "simple evaluator reviewer verdict=PASS",
                        "evaluation_report_ref": str(eval_report_ref.resolve()),
                        "evaluator": {"verdict": "PASS"},
                        "evaluator_result": {
                            "verdict": "PASS",
                            "lane": "reviewer",
                            "summary": "simple evaluator reviewer verdict=PASS",
                            "retryable": False,
                            "evidence_refs": [],
                            "diagnostics": {},
                            "recovery": {},
                        },
                        "completed_at_utc": "2026-03-23T13:52:33Z",
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            healed_view = query_authority_view(state_root)
            if healed_view["node_lifecycle"].get(self_heal_child.node_id) != "COMPLETED":
                return _fail("query_authority_view must self-heal from authoritative PASS result instead of exposing stale FAILED node snapshot")
            healed_snapshot = json.loads((state_root / "state" / f"{self_heal_child.node_id}.json").read_text(encoding="utf-8"))
            healed_runtime = dict(healed_snapshot.get("runtime_state") or {})
            if str(healed_snapshot.get("status") or "") != "COMPLETED":
                return _fail("query_authority_view self-heal must refresh the cached per-node snapshot status")
            if str(healed_runtime.get("summary") or "") != "simple evaluator reviewer verdict=PASS":
                return _fail("query_authority_view self-heal must refresh the cached per-node runtime summary from authoritative PASS result")
        finally:
            cleanup_receipt = cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
            if not bool(cleanup_receipt.get("quiesced")):
                raise RuntimeError(
                    "state query self-heal fixture cleanup must quiesce repo-scope services, "
                    f"got: {cleanup_receipt}"
                )
            remaining = _wait_for_repo_service_pids_to_clear(repo_root)
            if remaining:
                raise RuntimeError(
                    "state query self-heal fixture cleanup must not leave repo-scope service pids behind, "
                    f"got: {remaining}"
                )

    print("[loop-system-state-query] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
