#!/usr/bin/env python3
"""Validate kernel authority sync for evaluator-backed implementer results."""

from __future__ import annotations

import json
import shlex
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from test_support import temporary_repo_tree_root


def _fail(msg: str) -> int:
    print(f"[loop-system-evaluator-authority-status-sync][FAIL] {msg}", file=sys.stderr)
    return 2


def _fixture_role_agent_cmd(
    *,
    scenario: str,
    failing_role: str | None = None,
    failure_message: str | None = None,
) -> str:
    parts = [
        shlex.quote(sys.executable),
        "-m",
        "loop_product.loop.fixture_role_agent",
        "--scenario",
        shlex.quote(scenario),
    ]
    if failing_role:
        parts.extend(["--failing-role", shlex.quote(failing_role)])
    if failure_message:
        parts.extend(["--failure-message", shlex.quote(failure_message)])
    return " ".join(parts)


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _target_node(*, workspace_root: Path):
    from loop_product.protocols.node import NodeSpec, NodeStatus

    return NodeSpec(
        node_id="authority-sync-node",
        node_kind="implementer",
        goal_slice="validate evaluator authority sync",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={"agent_provider": "codex_cli", "sandbox_mode": "danger-full-access"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_runtime_s": 1800},
        allowed_actions=["implement", "evaluate", "report", "resume_request", "retry_request", "relaunch_request"],
        delegation_ref="state/delegations/authority-sync-node.json",
        result_sink_ref="artifacts/authority_sync/result.json",
        workspace_root=str(workspace_root.resolve()),
        codex_home=str(workspace_root.resolve()),
        lineage_ref="root-kernel->authority-sync-node",
        status=NodeStatus.ACTIVE,
    )


def _base_state(*, state_root: Path, workspace_root: Path) -> object:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import KernelState, ensure_runtime_tree, persist_kernel_state, persist_node_snapshot
    from loop_product.protocols.node import NodeSpec, NodeStatus

    ensure_runtime_tree(state_root)
    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="root",
        goal_slice="root authority sync harness",
        parent_node_id="",
        generation=0,
        round_id="R0",
        execution_policy={},
        reasoning_profile={},
        budget_profile={},
        allowed_actions=["delegate", "evaluate", "report"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    target_node = _target_node(workspace_root=workspace_root)
    kernel_state = KernelState(
        task_id="evaluator-authority-status-sync",
        root_goal="validate evaluator authority sync",
        root_node_id=root_node.node_id,
    )
    kernel_state.register_node(root_node)
    kernel_state.register_node(target_node)
    persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
    persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())
    persist_node_snapshot(state_root, target_node, authority=kernel_internal_authority())
    return target_node


def _run_submission(*, temp_root: Path, case_name: str, role_agent_cmd: str):
    from loop_product.kernel.query import query_authority_view
    from loop_product.loop.evaluator_client import build_evaluator_submission_for_frozen_task, run_evaluator_node

    case_root = temp_root / case_name
    state_root = case_root / ".loop"
    workspace_root = case_root / "workspace"
    output_root = state_root / "artifacts" / "evaluator_runs"
    implementation_root = case_root / "implementation"
    workspace_root.mkdir(parents=True, exist_ok=True)
    implementation_root.mkdir(parents=True, exist_ok=True)
    target_node = _base_state(state_root=state_root, workspace_root=workspace_root)
    _write_text(implementation_root / "README.md", "# Implementation\n")
    manual_ref = _write_text(case_root / "PRODUCT_MANUAL.md", "# Manual\n\nUse only documented surfaces.\n")
    effects_ref = _write_text(case_root / "FINAL_EFFECTS.md", "# Final Effects\n\n- Keep authority lifecycle honest.\n")
    submission = build_evaluator_submission_for_frozen_task(
        target_node=target_node,
        workspace_root=workspace_root,
        output_root=output_root,
        implementation_package_ref=implementation_root,
        product_manual_ref=manual_ref,
        final_effects_text_ref=effects_ref,
        role_agent_cmd=role_agent_cmd,
        evaluation_id=f"evaluator_authority_status_sync_{case_name}",
    )
    result, refs = run_evaluator_node(state_root=state_root, submission=submission)
    authority = query_authority_view(state_root)
    return state_root, result, refs, authority


def _build_submission_for_target(*, case_root: Path, state_root: Path, workspace_root: Path):
    from loop_product.loop.evaluator_client import build_evaluator_submission_for_frozen_task

    implementation_root = case_root / "implementation"
    implementation_root.mkdir(parents=True, exist_ok=True)
    _write_text(implementation_root / "README.md", "# Implementation\n")
    manual_ref = _write_text(case_root / "PRODUCT_MANUAL.md", "# Manual\n\nUse only documented surfaces.\n")
    effects_ref = _write_text(case_root / "FINAL_EFFECTS.md", "# Final Effects\n\n- Keep authority lifecycle honest.\n")
    target_node = _target_node(workspace_root=workspace_root)
    return build_evaluator_submission_for_frozen_task(
        target_node=target_node,
        workspace_root=workspace_root,
        output_root=state_root / "artifacts" / "evaluator_runs",
        implementation_package_ref=implementation_root,
        product_manual_ref=manual_ref,
        final_effects_text_ref=effects_ref,
        evaluation_id=f"evaluator_authority_status_sync_{case_root.name}",
    )


def main() -> int:
    from loop_product.kernel.authority import kernel_internal_authority
    from loop_product.kernel.state import load_kernel_state, persist_kernel_state, persist_node_snapshot, synchronize_authoritative_node_results
    from loop_product.loop.evaluator_client import materialize_terminal_implementer_result
    from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
    from loop_product.runtime import backfill_authoritative_terminal_results
    from loop_product.state_io import is_user_writable

    with temporary_repo_tree_root(prefix="loop_system_eval_authority_sync_") as temp_root:

        error_state_root, error_result, error_refs, error_authority = _run_submission(
            temp_root=temp_root,
            case_name="structured_error",
            role_agent_cmd=_fixture_role_agent_cmd(
                scenario="lane_failure",
                failing_role="test_designer",
                failure_message="python: can't open file '/tmp/missing_product_entry.py': [Errno 2] No such file or directory",
            ),
        )
        error_lifecycle = dict(error_authority.get("node_lifecycle") or {}).get("authority-sync-node")
        if error_result.verdict.value != "STRUCTURED_EXCEPTION":
            return _fail(f"structured_error must still produce STRUCTURED_EXCEPTION, got {error_result.verdict.value}")
        if error_lifecycle != "FAILED":
            return _fail(
                "non-retryable evaluator terminal errors must keep authority lifecycle FAILED instead of being normalized to COMPLETED"
            )
        error_result_payload = json.loads(
            (error_state_root / "artifacts" / "authority_sync" / "result.json").read_text(encoding="utf-8")
        )
        if str(error_result_payload.get("status") or "") != "COMPLETED":
            return _fail("focused harness expects the implementer_result payload to still encode terminal completion bytes")

        fail_state_root, fail_result, _fail_refs, fail_authority = _run_submission(
            temp_root=temp_root,
            case_name="retryable_fail",
            role_agent_cmd=_fixture_role_agent_cmd(scenario="terminal_fail"),
        )
        fail_lifecycle = dict(fail_authority.get("node_lifecycle") or {}).get("authority-sync-node")
        if fail_result.verdict.value != "FAIL" or fail_result.retryable is not True:
            return _fail("retryable_fail must produce a retryable evaluator FAIL")
        if fail_lifecycle != "ACTIVE":
            return _fail(
                "retryable evaluator FAIL must remain ACTIVE instead of being normalized to COMPLETED by authoritative result sync"
            )
        fail_result_payload = json.loads(
            (fail_state_root / "artifacts" / "authority_sync" / "result.json").read_text(encoding="utf-8")
        )
        if str(dict(fail_result_payload.get("evaluator_result") or {}).get("retryable")).lower() != "true":
            return _fail("retryable_fail harness must preserve retryable evaluator_result evidence")
        fail_kernel_state = load_kernel_state(fail_state_root)
        fail_node = dict(fail_kernel_state.nodes.get("authority-sync-node") or {})
        fail_node["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "2026-03-23T08:00:00Z",
            "observation_kind": "authoritative_result",
            "summary": "stale terminal runtime attachment",
            "evidence_refs": [str((fail_state_root / "artifacts" / "authority_sync" / "result.json").resolve())],
        }
        fail_kernel_state.nodes["authority-sync-node"] = fail_node
        persist_node_snapshot(fail_state_root, fail_node, authority=kernel_internal_authority())
        persist_kernel_state(fail_state_root, fail_kernel_state, authority=kernel_internal_authority())
        refreshed = synchronize_authoritative_node_results(
            fail_state_root,
            continue_deferred=False,
            authority=kernel_internal_authority(),
        )
        refreshed_node = dict(refreshed.nodes.get("authority-sync-node") or {})
        refreshed_runtime = dict(refreshed_node.get("runtime_state") or {})
        if str(refreshed_node.get("status") or "") != "ACTIVE":
            return _fail("retryable FAIL authority sync must keep node ACTIVE after scrubbing stale terminal runtime state")
        if str(refreshed_runtime.get("attachment_state") or "") == "TERMINAL":
            return _fail("retryable FAIL authority sync must clear stale terminal runtime attachment instead of preserving it")
        fail_snapshot_payload = json.loads((fail_state_root / "state" / "authority-sync-node.json").read_text(encoding="utf-8"))
        if str(dict(fail_snapshot_payload.get("runtime_state") or {}).get("attachment_state") or "") == "TERMINAL":
            return _fail("retryable FAIL per-node snapshot must not persist stale terminal runtime attachment after authoritative sync")

        canonicalize_state_root = temp_root / "persist_node_snapshot_canonicalizes_pass" / ".loop"
        canonicalize_workspace_root = temp_root / "persist_node_snapshot_canonicalizes_pass" / "workspace"
        canonicalize_workspace_root.mkdir(parents=True, exist_ok=True)
        _base_state(state_root=canonicalize_state_root, workspace_root=canonicalize_workspace_root)
        canonicalize_result_ref = canonicalize_state_root / "artifacts" / "authority_sync" / "result.json"
        canonicalize_result_ref.parent.mkdir(parents=True, exist_ok=True)
        canonicalize_report_ref = temp_root / "pass-report.json"
        canonicalize_report_ref.write_text("{}\n", encoding="utf-8")
        canonicalize_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": "authority-sync-node",
                    "status": "COMPLETED",
                    "outcome": "COMPLETED",
                    "summary": "simple evaluator reviewer verdict=PASS",
                    "evaluator": {
                        "verdict": "PASS",
                    },
                    "evaluator_result": {
                        "verdict": "PASS",
                        "retryable": False,
                        "summary": "simple evaluator reviewer verdict=PASS",
                    },
                    "evaluation_report_ref": str(canonicalize_report_ref.resolve()),
                    "completed_at_utc": "2026-03-23T13:52:33Z",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        stale_pass_node = json.loads((canonicalize_state_root / "state" / "authority-sync-node.json").read_text(encoding="utf-8"))
        stale_pass_node["status"] = "FAILED"
        stale_pass_node["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "2026-03-23T13:06:23Z",
            "observation_kind": "authoritative_result",
            "summary": "simple evaluator reviewer verdict=FAIL",
            "evidence_refs": [str(canonicalize_result_ref.resolve())],
        }
        persist_node_snapshot(canonicalize_state_root, stale_pass_node, authority=kernel_internal_authority())
        refreshed_pass_snapshot = json.loads(
            (canonicalize_state_root / "state" / "authority-sync-node.json").read_text(encoding="utf-8")
        )
        if is_user_writable(canonicalize_state_root / "state" / "authority-sync-node.json"):
            return _fail("persist_node_snapshot must leave derived per-node snapshots read-only on disk")
        if str(refreshed_pass_snapshot.get("status") or "") != "COMPLETED":
            return _fail("persist_node_snapshot must canonicalize from fresher authoritative PASS result instead of preserving stale FAILED status")
        refreshed_pass_runtime = dict(refreshed_pass_snapshot.get("runtime_state") or {})
        if str(refreshed_pass_runtime.get("summary") or "") != "simple evaluator reviewer verdict=PASS":
            return _fail("persist_node_snapshot must rewrite stale runtime summary from fresher authoritative PASS result")
        if str(refreshed_pass_runtime.get("attachment_state") or "") != "TERMINAL":
            return _fail("persist_node_snapshot must keep authoritative PASS snapshots terminal")

        materialize_case_root = temp_root / "materialize_terminal_syncs_state"
        materialize_state_root = materialize_case_root / ".loop"
        materialize_workspace_root = materialize_case_root / "workspace"
        materialize_workspace_root.mkdir(parents=True, exist_ok=True)
        _base_state(state_root=materialize_state_root, workspace_root=materialize_workspace_root)
        materialize_kernel_state = load_kernel_state(materialize_state_root)
        materialize_node = dict(materialize_kernel_state.nodes.get("authority-sync-node") or {})
        materialize_node["status"] = "FAILED"
        materialize_node["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "2026-03-23T13:06:23Z",
            "observation_kind": "authoritative_result",
            "summary": "simple evaluator reviewer verdict=FAIL",
            "evidence_refs": ["stale-fail"],
        }
        materialize_kernel_state.nodes["authority-sync-node"] = materialize_node
        persist_node_snapshot(materialize_state_root, materialize_node, authority=kernel_internal_authority())
        persist_kernel_state(materialize_state_root, materialize_kernel_state, authority=kernel_internal_authority())
        submission = _build_submission_for_target(
            case_root=materialize_case_root,
            state_root=materialize_state_root,
            workspace_root=materialize_workspace_root,
        )
        stale_kernel_result_ref = materialize_state_root / "artifacts" / "authority_sync" / "result.json"
        stale_workspace_result_ref = materialize_workspace_root / "artifacts" / "authority_sync" / "result.json"
        stale_payload = {
            "schema": "loop_product.implementer_result",
            "schema_version": "0.1.0",
            "node_id": "authority-sync-node",
            "status": "COMPLETED",
            "outcome": "REPAIR_REQUIRED",
            "summary": "simple evaluator reviewer verdict=FAIL",
            "evaluator": {
                "verdict": "FAIL",
            },
            "evaluator_result": {
                "verdict": "FAIL",
                "retryable": True,
                "summary": "simple evaluator reviewer verdict=FAIL",
            },
            "evaluation_report_ref": str((materialize_case_root / "stale-fail-report.json").resolve()),
        }
        stale_kernel_result_ref.parent.mkdir(parents=True, exist_ok=True)
        stale_workspace_result_ref.parent.mkdir(parents=True, exist_ok=True)
        stale_kernel_result_ref.write_text(json.dumps(stale_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        stale_workspace_result_ref.write_text(json.dumps(stale_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        materialize_report_ref = materialize_case_root / "EvaluationReport.json"
        materialize_report_ref.write_text("{}\n", encoding="utf-8")
        materialize_terminal_implementer_result(
            state_root=materialize_state_root,
            submission=submission,
            evaluator_result=EvaluatorResult(
                verdict=EvaluatorVerdict.PASS,
                lane="reviewer",
                summary="simple evaluator reviewer verdict=PASS",
                evidence_refs=[],
                retryable=False,
                diagnostics={},
            ),
            runtime_refs={
                "request_ref": str((materialize_case_root / "EvaluatorNodeRequest.json").resolve()),
                "evaluation_report_ref": str(materialize_report_ref.resolve()),
                "reviewer_response_ref": "",
            },
        )
        materialized_node_snapshot = json.loads(
            (materialize_state_root / "state" / "authority-sync-node.json").read_text(encoding="utf-8")
        )
        if str(materialized_node_snapshot.get("status") or "") != "COMPLETED":
            return _fail("materialize_terminal_implementer_result must synchronize per-node snapshot immediately after writing authoritative PASS result")
        if (
            str(dict(materialized_node_snapshot.get("runtime_state") or {}).get("summary") or "")
            != "simple evaluator reviewer verdict=PASS"
        ):
            return _fail("materialize_terminal_implementer_result must refresh node snapshot summary instead of leaving stale FAIL bytes behind")
        materialized_kernel_result = json.loads(stale_kernel_result_ref.read_text(encoding="utf-8"))
        if is_user_writable(stale_kernel_result_ref):
            return _fail("materialize_terminal_implementer_result must leave authoritative result sinks read-only on disk")
        if is_user_writable(stale_workspace_result_ref):
            return _fail("materialize_terminal_implementer_result must leave workspace result mirrors read-only on disk")
        if str(materialized_kernel_result.get("summary") or "") != "simple evaluator reviewer verdict=PASS":
            return _fail("materialize_terminal_implementer_result must overwrite stale authoritative result summary instead of preserving older FAIL bytes")
        materialized_kernel_state = json.loads(
            (materialize_state_root / "state" / "kernel_state.json").read_text(encoding="utf-8")
        )
        if is_user_writable(materialize_state_root / "state" / "kernel_state.json"):
            return _fail("persist_kernel_state must leave kernel_state.json read-only on disk")
        if is_user_writable(materialize_state_root / "state" / "accepted_envelopes.json"):
            return _fail("persist_kernel_state must leave accepted_envelopes.json read-only on disk")
        kernel_node = dict(dict(materialized_kernel_state.get("nodes") or {}).get("authority-sync-node") or {})
        if str(kernel_node.get("status") or "") != "COMPLETED":
            return _fail("materialize_terminal_implementer_result must synchronize kernel_state node graph immediately after writing authoritative PASS result")

        backfill_case_root = temp_root / "backfill_historical_stale_result"
        backfill_state_root = backfill_case_root / ".loop"
        backfill_workspace_root = backfill_case_root / "workspace"
        backfill_workspace_root.mkdir(parents=True, exist_ok=True)
        _base_state(state_root=backfill_state_root, workspace_root=backfill_workspace_root)
        stale_backfill_payload = {
            "schema": "loop_product.implementer_result",
            "schema_version": "0.1.0",
            "node_id": "authority-sync-node",
            "status": "COMPLETED",
            "outcome": "COMPLETED",
            "summary": "simple evaluator reviewer verdict=FAIL",
            "completed_at_utc": "2026-03-23T13:52:33Z",
            "evaluator": {
                "verdict": "FAIL",
            },
            "evaluator_result": {
                "verdict": "PASS",
                "lane": "reviewer",
                "summary": "simple evaluator reviewer verdict=PASS",
                "retryable": False,
                "evidence_refs": [],
                "diagnostics": {},
                "recovery": {},
            },
            "runtime_refs": {
                "request_ref": str((backfill_case_root / "EvaluatorNodeRequest.json").resolve()),
                "evaluation_report_ref": str((backfill_case_root / "EvaluationReport.json").resolve()),
                "reviewer_response_ref": str((backfill_case_root / "reviewer_response.txt").resolve()),
            },
            "workspace_result_sink_ref": str((backfill_workspace_root / "artifacts" / "authority_sync" / "result.json").resolve()),
        }
        _write_text(backfill_case_root / "reviewer_response.txt", "VERDICT: PASS\n")
        _write_text(backfill_case_root / "EvaluationReport.json", "{}\n")
        _write_text(backfill_case_root / "EvaluatorNodeRequest.json", "{}\n")
        backfill_kernel_result_ref = backfill_state_root / "artifacts" / "authority_sync" / "result.json"
        backfill_workspace_result_ref = backfill_workspace_root / "artifacts" / "authority_sync" / "result.json"
        backfill_kernel_result_ref.parent.mkdir(parents=True, exist_ok=True)
        backfill_workspace_result_ref.parent.mkdir(parents=True, exist_ok=True)
        backfill_kernel_result_ref.write_text(
            json.dumps(stale_backfill_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        backfill_workspace_result_ref.write_text(
            json.dumps(stale_backfill_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        stale_backfill_node = json.loads((backfill_state_root / "state" / "authority-sync-node.json").read_text(encoding="utf-8"))
        stale_backfill_node["status"] = "FAILED"
        stale_backfill_node["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "2026-03-23T13:06:23Z",
            "observation_kind": "authoritative_result",
            "summary": "simple evaluator reviewer verdict=FAIL",
            "evidence_refs": [str(backfill_kernel_result_ref.resolve())],
        }
        backfill_kernel_state = load_kernel_state(backfill_state_root)
        backfill_kernel_state.nodes["authority-sync-node"] = stale_backfill_node
        persist_node_snapshot(backfill_state_root, stale_backfill_node, authority=kernel_internal_authority())
        persist_kernel_state(backfill_state_root, backfill_kernel_state, authority=kernel_internal_authority())
        repaired_nodes = backfill_authoritative_terminal_results(
            state_root=backfill_state_root,
            node_ids=["authority-sync-node"],
        )
        if repaired_nodes != ["authority-sync-node"]:
            return _fail(f"backfill_authoritative_terminal_results must report repaired node, got {repaired_nodes!r}")
        repaired_kernel_result = json.loads(backfill_kernel_result_ref.read_text(encoding="utf-8"))
        if is_user_writable(backfill_kernel_result_ref):
            return _fail("backfill_authoritative_terminal_results must leave repaired authoritative result sinks read-only on disk")
        if is_user_writable(backfill_workspace_result_ref):
            return _fail("backfill_authoritative_terminal_results must leave repaired workspace result mirrors read-only on disk")
        if str(repaired_kernel_result.get("summary") or "") != "simple evaluator reviewer verdict=PASS":
            return _fail("backfill_authoritative_terminal_results must repair stale authoritative top-level summary from nested PASS evaluator_result")
        repaired_workspace_result = json.loads(backfill_workspace_result_ref.read_text(encoding="utf-8"))
        if str(repaired_workspace_result.get("summary") or "") != "simple evaluator reviewer verdict=PASS":
            return _fail("backfill_authoritative_terminal_results must repair stale workspace result mirror from nested PASS evaluator_result")
        repaired_node_snapshot = json.loads((backfill_state_root / "state" / "authority-sync-node.json").read_text(encoding="utf-8"))
        if str(repaired_node_snapshot.get("status") or "") != "COMPLETED":
            return _fail("backfill_authoritative_terminal_results must synchronize repaired authoritative PASS result into per-node snapshot status")
        if str(dict(repaired_node_snapshot.get("runtime_state") or {}).get("summary") or "") != "simple evaluator reviewer verdict=PASS":
            return _fail("backfill_authoritative_terminal_results must synchronize repaired authoritative PASS summary into per-node snapshot")
        repaired_kernel_state_payload = json.loads((backfill_state_root / "state" / "kernel_state.json").read_text(encoding="utf-8"))
        if is_user_writable(backfill_state_root / "state" / "kernel_state.json"):
            return _fail("backfill_authoritative_terminal_results must keep derived kernel_state.json read-only after repair sync")
        if is_user_writable(backfill_state_root / "state" / "authority-sync-node.json"):
            return _fail("backfill_authoritative_terminal_results must keep derived per-node snapshot read-only after repair sync")
        repaired_kernel_node = dict(dict(repaired_kernel_state_payload.get("nodes") or {}).get("authority-sync-node") or {})
        if str(repaired_kernel_node.get("status") or "") != "COMPLETED":
            return _fail("backfill_authoritative_terminal_results must synchronize repaired authoritative PASS result into kernel_state")

    print("[loop-system-evaluator-authority-status-sync][OK] authority lifecycle stayed aligned with evaluator verdicts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
