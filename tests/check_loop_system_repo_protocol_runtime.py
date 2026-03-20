#!/usr/bin/env python3
"""Validate wave-2 LOOP system protocol schemas and authoritative state runtime."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator, validate


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-protocol-runtime][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import loop_product.kernel as kernel_surface
        import loop_product.runtime as runtime_surface

        from loop_product.dispatch.child_dispatch import load_child_runtime_context, materialize_child
        from loop_product.dispatch.heartbeat import build_child_heartbeat
        from loop_product.kernel.authority import kernel_internal_authority
        from loop_product.kernel.state import (
            KernelState,
            ensure_runtime_tree,
            load_kernel_state,
            persist_kernel_state,
            query_kernel_state,
        )
        from loop_product.kernel.submit import submit_control_envelope
        from loop_product.protocols.control_envelope import ControlEnvelope, EnvelopeStatus
        from loop_product.protocols.evaluator import EvaluatorResult, EvaluatorVerdict
        from loop_product.protocols.node import NodeSpec, NodeStatus
        from loop_product.protocols.topology import TopologyMutation
    except Exception as exc:  # noqa: BLE001
        return _fail(f"loop system runtime imports failed: {exc}")

    kernel_entry_source = (ROOT / "loop_product" / "kernel" / "entry.py").read_text(encoding="utf-8")
    if "from loop_product.loop.runner import run_child_smoke_loop" in kernel_entry_source:
        return _fail("kernel smoke must not import the child loop directly after materialization")
    if "run_child_smoke_loop(state_root, child_node)" in kernel_entry_source:
        return _fail("kernel smoke must not continue child step-by-step execution in root control flow")

    runner_source = (ROOT / "loop_product" / "loop" / "runner.py").read_text(encoding="utf-8")
    if "def main(" not in runner_source or 'if __name__ == "__main__":' not in runner_source:
        return _fail("child loop runner must provide a standalone CLI entrypoint for child-local execution")

    try:
        node_schema = _load_schema("LoopSystemNodeSpec.schema.json")
        envelope_schema = _load_schema("LoopSystemControlEnvelope.schema.json")
        evaluator_schema = _load_schema("LoopSystemEvaluatorResult.schema.json")
        topology_schema = _load_schema("LoopSystemTopologyMutation.schema.json")
        kernel_state_schema = _load_schema("LoopSystemKernelState.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"missing or invalid LOOP system schema: {exc}")

    root_node = NodeSpec(
        node_id="root-kernel",
        node_kind="kernel",
        goal_slice="supervise a wave-2 runtime validation",
        parent_node_id=None,
        generation=0,
        round_id="R0",
        execution_policy={"mode": "kernel"},
        reasoning_profile={"role": "kernel", "thinking_budget": "medium"},
        budget_profile={"max_rounds": 1},
        allowed_actions=["dispatch", "submit", "audit"],
        delegation_ref="",
        result_sink_ref="artifacts/root_result.json",
        lineage_ref="root-kernel",
        status=NodeStatus.ACTIVE,
    )
    root_node_record = root_node.to_dict()
    validate(root_node_record, node_schema)
    if root_node_record["execution_policy"].get("agent_provider") != "codex_cli":
        return _fail("node execution_policy must serialize agent_provider instead of leaving provider selection implicit")
    if root_node_record["execution_policy"].get("sandbox_mode") != "danger-full-access":
        return _fail("node execution_policy must serialize sandbox_mode as node protocol data")
    if root_node_record["reasoning_profile"].get("thinking_budget") != "medium":
        return _fail("node reasoning_profile must serialize thinking_budget explicitly")
    runtime_state = dict(root_node_record.get("runtime_state") or {})
    if runtime_state.get("attachment_state") != "UNOBSERVED":
        return _fail("node protocol must serialize runtime_state with default attachment_state=UNOBSERVED")
    if root_node_record.get("depends_on_node_ids") != []:
        return _fail("ordinary node protocol must serialize default depends_on_node_ids=[]")
    if root_node_record.get("activation_condition") != "":
        return _fail("ordinary node protocol must serialize default activation_condition=''")

    evaluator_result = EvaluatorResult(
        verdict=EvaluatorVerdict.BUG,
        lane="ai-as-user",
        summary="The evaluator hit its own missing-tool issue before exercising the implementation.",
        evidence_refs=["ai-as-user:round-1"],
        retryable=True,
        diagnostics={
            "self_attribution": "evaluator_environment",
            "self_repair": "repair the command and retry before blaming implementation",
        },
    )
    validate(evaluator_result.to_dict(), evaluator_schema)

    heartbeat = build_child_heartbeat(root_node, note="kernel heartbeat test")
    validate(heartbeat.to_dict(), envelope_schema)

    mutation = TopologyMutation.split("root-kernel", ["child-a", "child-b"], reason="parallelize accepted work slices")
    validate(mutation.to_dict(), topology_schema)
    authority = kernel_internal_authority()

    with tempfile.TemporaryDirectory(prefix="loop_system_protocol_runtime_") as td:
        state_root = Path(td) / ".loop"
        ensure_runtime_tree(state_root)
        kernel_state = KernelState(
            task_id="wave2-runtime",
            root_goal="validate authoritative protocol runtime",
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        persist_kernel_state(state_root, kernel_state, authority=authority)

        child_node = materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id=root_node.node_id,
            node_id="child-implementer-001",
            goal_slice="test authoritative intake",
            round_id="R1",
            execution_policy={"sandbox_mode": "danger-full-access", "retry_policy": "single_retry"},
            reasoning_profile={"role": "implementer", "thinking_budget": "high"},
            budget_profile={"max_rounds": 3},
            authority=authority,
        )
        child_node_record = child_node.to_dict()
        validate(child_node_record, node_schema)
        if child_node_record["execution_policy"].get("agent_provider") != "codex_cli":
            return _fail("materialized child nodes must persist agent_provider in execution_policy")
        if child_node_record["execution_policy"].get("sandbox_mode") != "danger-full-access":
            return _fail("materialized child nodes must persist sandbox_mode in execution_policy")
        if child_node_record["reasoning_profile"].get("thinking_budget") != "high":
            return _fail("materialized child nodes must persist thinking_budget in reasoning_profile")
        expected_child_workspace_root = ROOT / "workspace" / child_node.node_id
        if Path(str(child_node_record.get("workspace_root") or "")).resolve() != expected_child_workspace_root.resolve():
            return _fail("materialized child nodes must persist an explicit implementer workspace under workspace/<node_id>")
        if str(child_node_record.get("codex_home") or "").strip():
            return _fail("materialized child nodes must not require a node-local CODEX_HOME override")
        if set(child_node_record.get("allowed_actions") or []) & {"retry_request", "relaunch_request"} != {"retry_request", "relaunch_request"}:
            return _fail("materialized implementer child nodes must carry retry_request and relaunch_request by default")
        child_runtime_state = dict(child_node_record.get("runtime_state") or {})
        if child_runtime_state.get("attachment_state") != "UNOBSERVED":
            return _fail("materialized child nodes must start with runtime attachment UNOBSERVED")
        if child_node_record.get("depends_on_node_ids") != []:
            return _fail("ordinary materialized child nodes must default depends_on_node_ids to []")
        if child_node_record.get("activation_condition") != "":
            return _fail("ordinary materialized child nodes must default activation_condition to empty")

        accepted_heartbeat = submit_control_envelope(
            state_root,
            build_child_heartbeat(child_node, note="child materialized with frozen delegation"),
        )
        if accepted_heartbeat.status is not EnvelopeStatus.ACCEPTED:
            return _fail("child heartbeat must be accepted into authoritative state")
        heartbeat_state = load_kernel_state(state_root)
        runtime_after_heartbeat = dict(heartbeat_state.nodes[child_node.node_id].get("runtime_state") or {})
        if runtime_after_heartbeat.get("attachment_state") != "ATTACHED":
            return _fail("accepted child heartbeat must mark the child runtime as ATTACHED")
        if not str(runtime_after_heartbeat.get("observed_at") or "").strip():
            return _fail("accepted child heartbeat must record when runtime attachment was observed")

        accepted_result = submit_control_envelope(
            state_root,
            ControlEnvelope(
                source=child_node.node_id,
                envelope_type="evaluator_result",
                round_id="R1.1",
                generation=child_node.generation,
                payload=evaluator_result.to_dict(),
                status=EnvelopeStatus.REPORT,
                note=evaluator_result.summary,
            ),
        )
        if accepted_result.status is not EnvelopeStatus.ACCEPTED:
            return _fail("structured evaluator result must be accepted into authoritative state")

        rejected = submit_control_envelope(
            state_root,
            {
                "source": "",
                "envelope_type": "resume_request",
                "round_id": "",
                "generation": -1,
                "payload": {},
                "status": "proposal",
            },
        )
        if rejected.status is not EnvelopeStatus.REJECTED:
            return _fail("malformed raw control envelope must fail closed")

        rejected_missing_required = submit_control_envelope(
            state_root,
            {
                "payload": {"x": 1},
                "status": "report",
            },
        )
        if rejected_missing_required.status is not EnvelopeStatus.REJECTED:
            return _fail("raw control envelopes missing required fields must fail closed instead of being default-repaired")

        blocked_state = load_kernel_state(state_root)
        blocked_state.nodes[child_node.node_id]["status"] = NodeStatus.BLOCKED.value
        blocked_state.blocked_reasons[child_node.node_id] = "waiting on a verified external prerequisite"
        persist_kernel_state(state_root, blocked_state, authority=authority)
        direct_resume = submit_control_envelope(
            state_root,
            {
                "source": child_node.node_id,
                "envelope_type": "resume_request",
                "payload_type": "topology_mutation",
                "round_id": "R1.2",
                "generation": child_node.generation,
                "payload": TopologyMutation.resume(
                    child_node.node_id,
                    reason="the same node can continue after the prerequisite is repaired",
                    payload={"consistency_signal": "dependency_unblocked"},
                ).to_dict(),
                "status": "proposal",
                "note": "direct topology submit must still enforce kernel review",
            },
        )
        if direct_resume.status is not EnvelopeStatus.ACCEPTED:
            return _fail("generic topology submission must still succeed when kernel review accepts it")
        review = dict(direct_resume.payload.get("review") or {})
        if str(review.get("decision") or "").upper() != "ACCEPT":
            return _fail("generic topology submission must attach a kernel review result before acceptance")

        rejected_object = submit_control_envelope(
            state_root,
            ControlEnvelope(
                source=child_node.node_id,
                envelope_type="local_control_decision",
                round_id="R1.invalid",
                generation=child_node.generation,
                payload={"summary": "bad status object should fail closed"},
                status="bogus",  # type: ignore[arg-type]
            ),
        )
        if rejected_object.status is not EnvelopeStatus.REJECTED:
            return _fail("malformed ControlEnvelope objects must fail closed instead of crashing")

        loaded_state = load_kernel_state(state_root)
        validate(loaded_state.to_dict(), kernel_state_schema)
        query = query_kernel_state(state_root)
        validate(query, kernel_state_schema)

        accepted_types = [item["envelope_type"] for item in query["accepted_envelopes"]]
        if query["accepted_envelope_count"] != 3:
            return _fail("only accepted heartbeat, evaluator_result, and kernel-reviewed resume_request facts should persist")
        if accepted_types.count("child_heartbeat") != 1 or accepted_types.count("evaluator_result") != 1:
            return _fail("authoritative state must record accepted heartbeat and evaluator_result envelopes exactly once")
        if accepted_types.count("resume_request") != 1:
            return _fail("kernel-reviewed topology acceptance must be persisted exactly once")
        if query["delegation_map"].get(child_node.node_id) != root_node.node_id:
            return _fail("delegation_map must expose child -> parent relationship")
        if child_node.node_id not in query["active_node_ids"]:
            return _fail("query must expose active child node ids")
        if child_node.node_id in query["blocked_reasons"]:
            return _fail("accepted kernel-reviewed resume_request must clear the blocked reason")
        if not any(
            lane["node_id"] == child_node.node_id and lane["lane"] == "ai-as-user"
            for lane in query["active_evaluator_lanes"]
        ):
            return _fail("query must expose active evaluator lanes for accepted evaluator results")
        if not any(
            asset.get("self_attribution") == "evaluator_environment"
            and asset.get("self_repair")
            for asset in query["experience_assets"]
        ):
            return _fail("self-attribution and self-repair evidence must be preserved in authoritative state")

        delegation_path = state_root / "state" / "delegations" / f"{child_node.node_id}.json"
        if not delegation_path.exists():
            return _fail("child-dispatch must materialize a frozen delegation artifact for each child")
        delegation_payload = json.loads(delegation_path.read_text(encoding="utf-8"))
        if delegation_payload.get("execution_policy", {}).get("agent_provider") != "codex_cli":
            return _fail("frozen delegation must preserve agent_provider inside execution_policy")
        runtime_context = load_child_runtime_context(state_root, child_node.node_id)
        if runtime_context.get("node", {}).get("node_id") != child_node.node_id:
            return _fail("child runtime context must expose the materialized node-local context")
        if runtime_context.get("delegation", {}).get("node_id") != child_node.node_id:
            return _fail("child runtime context must expose the frozen delegation payload")
        if runtime_context.get("delegation", {}).get("execution_policy", {}).get("sandbox_mode") != "danger-full-access":
            return _fail("child runtime context must preserve sandbox_mode from the frozen execution policy")
        if runtime_context.get("parent_transcript_ref"):
            return _fail("child runtime context must not depend on a raw parent transcript ref")
        runtime_workspace_root = Path(str(runtime_context.get("workspace_root") or ""))
        if runtime_workspace_root.resolve() != expected_child_workspace_root.resolve():
            return _fail("child runtime context must expose the explicit implementer project workspace for child-local execution")
        if str(runtime_context.get("codex_home") or "").strip():
            return _fail("child runtime context must not expose a required node-local CODEX_HOME override")

        journal_path = state_root / "state" / "accepted_envelopes.json"
        if not journal_path.exists():
            return _fail("accepted envelopes must be persisted under .loop/state/")

        try:
            persist_kernel_state(state_root, kernel_state)
        except PermissionError:
            pass
        else:
            return _fail("ordinary callers must not persist authoritative kernel state outside the exported kernel surface")

        try:
            materialize_child(
                state_root=state_root,
                kernel_state=kernel_state,
                parent_node_id=root_node.node_id,
                node_id="child-bypass-root",
                goal_slice="must not bypass the kernel surface",
                round_id="R-bypass",
                execution_policy={"sandbox_mode": "danger-full-access"},
                reasoning_profile={"role": "implementer", "thinking_budget": "medium"},
                budget_profile={"max_rounds": 1},
            )
        except PermissionError:
            pass
        else:
            return _fail("child-dispatch must reject direct graph mutation attempts outside kernel authority")

        ordinary_script = Path(td) / "ordinary_kernel_authority_probe.py"
        ordinary_script.write_text(
            "\n".join(
                [
                    "from __future__ import annotations",
                    "",
                    "import sys",
                    "from pathlib import Path",
                    "",
                    f"ROOT = Path({str(ROOT)!r})",
                    "if str(ROOT) not in sys.path:",
                    "    sys.path.insert(0, str(ROOT))",
                    "",
                    "from loop_product.kernel.authority import kernel_internal_authority",
                    "",
                    "try:",
                    "    kernel_internal_authority()",
                    "except PermissionError:",
                    "    raise SystemExit(0)",
                    "raise SystemExit(1)",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        ordinary_proc = subprocess.run(
            [sys.executable, str(ordinary_script)],
            cwd=str(Path(td)),
            capture_output=True,
            text=True,
        )
        if ordinary_proc.returncode != 0:
            return _fail("ordinary scripts outside trusted product surfaces must not obtain kernel mutation authority")

        required_kernel_surface = {
            "query_authority_view",
            "query_recent_audit_events",
            "route_user_requirements",
            "submit_control_envelope",
            "submit_topology_mutation",
        }
        if not required_kernel_surface.issubset(set(kernel_surface.__all__)):
            return _fail("loop_product.kernel must expose the documented minimum kernel surface directly")

        runtime_exports = set(runtime_surface.__all__)
        if "initialize_evaluator_runtime" not in runtime_exports:
            return _fail("loop_product.runtime must expose the public evaluator runtime bootstrap surface")
        if "bootstrap_first_implementer_node" not in runtime_exports:
            return _fail("loop_product.runtime must expose the public first-child bootstrap surface")
        if "bootstrap_first_implementer_from_endpoint" not in runtime_exports:
            return _fail("loop_product.runtime must expose the endpoint-driven first-child bootstrap surface")
        if "child_runtime_status_from_launch_result_ref" not in runtime_exports:
            return _fail("loop_product.runtime must expose the committed child-runtime status surface")
        if "supervise_child_until_settled" not in runtime_exports:
            return _fail("loop_product.runtime must expose the committed root-side child supervision surface")

        routed = kernel_surface.route_user_requirements(
            state_root,
            ["accept a newly routed user requirement", child_node.goal_slice],
        )
        if routed != ["accept a newly routed user requirement"]:
            return _fail("route_user_requirements must persist only newly routed requirements")

        routed_view = kernel_surface.query_authority_view(state_root)
        if "accept a newly routed user requirement" not in routed_view["effective_requirements"]:
            return _fail("route_user_requirements must update the authoritative effective requirement set")
        recent_audit = kernel_surface.query_recent_audit_events(state_root, limit=5)
        if not any(
            item.get("event_type") == "user_requirements_routed"
            and "accept a newly routed user requirement" in str(item.get("summary") or "")
            for item in recent_audit
        ):
            return _fail("kernel audit surface must expose routed user-requirement events")

        invalid_state_root = Path(td) / "source_tree_runtime"
        try:
            ensure_runtime_tree(invalid_state_root)
        except ValueError:
            pass
        else:
            return _fail("runtime helpers must reject state roots outside a .loop boundary")

        try:
            persist_kernel_state(invalid_state_root, kernel_state, authority=authority)
        except ValueError:
            pass
        else:
            return _fail("kernel state persistence must reject durable writes outside a .loop boundary")

        try:
            materialize_child(
                state_root=invalid_state_root,
                kernel_state=kernel_state,
                parent_node_id=root_node.node_id,
                node_id="child-invalid-root",
                goal_slice="must not materialize outside a .loop boundary",
                round_id="R-invalid",
                execution_policy={"sandbox_mode": "danger-full-access"},
                reasoning_profile={"role": "implementer", "thinking_budget": "medium"},
                budget_profile={"max_rounds": 1},
                authority=authority,
            )
        except ValueError:
            pass
        else:
            return _fail("child-dispatch must reject state roots outside a .loop boundary")

        try:
            submit_control_envelope(
                invalid_state_root,
                build_child_heartbeat(child_node, note="must not audit outside a .loop boundary"),
            )
        except ValueError:
            pass
        else:
            return _fail("control-envelope submission must reject state roots outside a .loop boundary")

        if invalid_state_root.exists():
            leaked_files = sorted(str(path.relative_to(Path(td))) for path in invalid_state_root.rglob("*") if path.is_file())
            if leaked_files:
                return _fail("runtime helpers must not leak durable files outside a .loop boundary")

    print("[loop-system-protocol-runtime] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
