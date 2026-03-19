#!/usr/bin/env python3
"""Regression check for concurrent smoke evaluator staging isolation."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-smoke-parallel-isolation][FAIL] {msg}", file=sys.stderr)
    return 2


def main() -> int:
    try:
        from loop_product.loop.evaluator_client import build_evaluator_submission_for_smoke_round
        from loop_product.protocols.node import NodeSpec, NodeStatus
    except Exception as exc:  # noqa: BLE001
        return _fail(f"imports failed: {exc}")

    node = NodeSpec(
        node_id="child-implementer-001",
        node_kind="implementer",
        goal_slice="exercise parallel smoke isolation",
        parent_node_id="root-kernel",
        generation=1,
        round_id="R1",
        execution_policy={},
        reasoning_profile={},
        budget_profile={},
        status=NodeStatus.ACTIVE,
    )
    with tempfile.TemporaryDirectory(prefix="loop_system_smoke_iso_a_") as tda, tempfile.TemporaryDirectory(
        prefix="loop_system_smoke_iso_b_"
    ) as tdb:
        state_root_a = Path(tda) / ".loop"
        state_root_b = Path(tdb) / ".loop"
        submission_a = build_evaluator_submission_for_smoke_round(
            target_node=node,
            state_root=state_root_a,
            workspace_root=ROOT,
            output_root=state_root_a / "artifacts" / "evaluator_node_runs",
            round_index=1,
            role_agent_cmd="python -m loop_product.loop.smoke_role_agent --mode fail",
        )
        submission_b = build_evaluator_submission_for_smoke_round(
            target_node=node,
            state_root=state_root_b,
            workspace_root=ROOT,
            output_root=state_root_b / "artifacts" / "evaluator_node_runs",
            round_index=1,
            role_agent_cmd="python -m loop_product.loop.smoke_role_agent --mode fail",
        )

    if submission_a.evaluation_id == submission_b.evaluation_id:
        return _fail("parallel smoke runs must get distinct evaluation_id values")
    if submission_a.round_id != submission_b.round_id:
        return _fail("parallel isolation must not perturb semantic round_id values")

    print("[loop-system-smoke-parallel-isolation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
