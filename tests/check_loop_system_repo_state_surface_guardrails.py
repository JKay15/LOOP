#!/usr/bin/env python3
"""Guard remaining control-truth state surfaces against stale-cache regressions."""

from __future__ import annotations

import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LOOP_ROOT = ROOT / "loop_product"
SCRIPTS_ROOT = ROOT / "scripts"

ALLOWED_RAW_KERNEL_STATE_READERS = {
    "loop_product/kernel/entry.py",
    "loop_product/kernel/state.py",
    "loop_product/kernel/submit.py",
    "loop_product/runtime/lifecycle.py",
}


def _fail(msg: str) -> int:
    print(f"[loop-system-state-surface-guardrails][FAIL] {msg}", file=sys.stderr)
    return 2


def _iter_non_test_python() -> list[Path]:
    files = sorted(LOOP_ROOT.rglob("*.py"))
    files.extend(sorted(SCRIPTS_ROOT.rglob("*.py")))
    return [path for path in files if "/tests/" not in str(path)]


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT.resolve()))


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.requirements import route_user_requirements
        from loop_product.kernel.state import (
            KernelState,
            ensure_runtime_tree,
            persist_kernel_state,
            persist_node_snapshot,
            query_kernel_state_object,
        )
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from test_support import temporary_repo_root
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    raw_reader_offenders: list[str] = []
    for path in _iter_non_test_python():
        rel = _relative(path)
        text = path.read_text(encoding="utf-8")
        if "load_kernel_state(" in text and rel not in ALLOWED_RAW_KERNEL_STATE_READERS:
            raw_reader_offenders.append(rel)
    if raw_reader_offenders:
        return _fail(
            "disallowed raw load_kernel_state readers remain: "
            + ", ".join(sorted(raw_reader_offenders))
        )

    bootstrap_text = (LOOP_ROOT / "dispatch" / "bootstrap.py").read_text(encoding="utf-8")
    if 'state_root / "state" / f"{node_id}.json"' in bootstrap_text:
        return _fail("continue_exact must not re-read node snapshots directly from state/<node>.json")

    lifecycle_text = (LOOP_ROOT / "runtime" / "lifecycle.py").read_text(encoding="utf-8")
    if 'result_ref.write_text(json.dumps(result_payload' in lifecycle_text:
        return _fail("persist_committed_supervision_result must use the trusted read-only writer helper")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="guard state surfaces",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/runtime_bootstrap_summary.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    child_node = NodeSpec(
        node_id="child-state-guard",
        node_kind="implementer",
        goal_slice="verify writer-side self-heal",
        parent_node_id=root_node.node_id,
        generation=1,
        round_id="R1",
        execution_policy={"sandbox_mode": "workspace-write"},
        reasoning_profile={"role": "implementer", "thinking_budget": "high"},
        budget_profile={"max_rounds": 2},
        allowed_actions=["implement", "evaluate", "report"],
        delegation_ref="state/delegations/child-state-guard.json",
        result_sink_ref="artifacts/child-state-guard/result.json",
        lineage_ref="root-kernel->child-state-guard",
        status=NodeStatus.ACTIVE,
    )

    with temporary_repo_root(prefix="loop_system_state_surface_guardrails_") as repo_root:
        state_root = repo_root / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="state-surface-guardrails",
            root_goal="guard control-truth surfaces",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        kernel_state.register_node(child_node)
        persist_kernel_state(state_root, kernel_state, authority=kernel_internal_authority())
        persist_node_snapshot(state_root, root_node, authority=kernel_internal_authority())

        stale_child = child_node.to_dict()
        stale_child["status"] = "FAILED"
        stale_child["runtime_state"] = {
            "attachment_state": "TERMINAL",
            "observed_at": "2026-03-23T13:06:23Z",
            "observation_kind": "authoritative_result",
            "summary": "simple evaluator reviewer verdict=FAIL",
            "evidence_refs": [str((state_root / "artifacts" / child_node.node_id / "result.json").resolve())],
        }
        persist_node_snapshot(state_root, stale_child, authority=kernel_internal_authority())

        result_ref = state_root / "artifacts" / child_node.node_id / "result.json"
        eval_report_ref = state_root / "artifacts" / child_node.node_id / "EvaluationReport.json"
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        eval_report_ref.write_text("{}\n", encoding="utf-8")
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": child_node.node_id,
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
                        "diagnostics": {},
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        routed = route_user_requirements(state_root, "newly routed requirement")
        if routed != ["newly routed requirement"]:
            return _fail(f"unexpected routed requirements result: {routed!r}")

        healed = query_kernel_state_object(state_root, continue_deferred=False)
        healed_child = dict(healed.nodes.get(child_node.node_id) or {})
        healed_runtime = dict(healed_child.get("runtime_state") or {})
        if str(healed_child.get("status") or "") != "COMPLETED":
            return _fail("writer-side state update must preserve authoritative PASS rather than replay stale FAIL")
        if "PASS" not in str(healed_runtime.get("summary") or ""):
            return _fail("writer-side state update must keep the healed PASS summary visible after mutation")

    print("[loop-system-state-surface-guardrails][PASS] remaining state surfaces stay on trusted readers/writers")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
