"""Lifecycle helpers and public runtime bootstrap surfaces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from loop_product.dispatch.bootstrap import bootstrap_first_implementer_node as dispatch_bootstrap_first_implementer_node
from loop_product.dispatch.bootstrap import bootstrap_first_implementer_from_endpoint as dispatch_bootstrap_first_implementer_from_endpoint
from loop_product.dispatch.child_progress_snapshot import (
    child_progress_snapshot_from_launch_result_ref as dispatch_child_progress_snapshot_from_launch_result_ref,
)
from loop_product.dispatch.launch_runtime import child_runtime_status_from_launch_result_ref as dispatch_child_runtime_status_from_launch_result_ref
from loop_product.dispatch.launch_runtime import launch_child_from_result_ref as dispatch_launch_child_from_result_ref
from loop_product.dispatch.publication import publish_external_from_implementer_result as dispatch_publish_external_from_implementer_result
from loop_product.dispatch.recovery_bootstrap import recover_orphaned_active_node as dispatch_recover_orphaned_active_node
from loop_product.dispatch.supervision import supervise_child_until_settled as dispatch_supervise_child_until_settled
from loop_product.dispatch.child_dispatch import load_child_runtime_context, materialize_child
from loop_product.kernel.authority import kernel_internal_authority
from loop_product.kernel.policy import (
    implementer_budget_profile,
    implementer_execution_policy,
    implementer_reasoning_profile,
    kernel_execution_policy,
    kernel_reasoning_profile,
)
from loop_product.kernel.state import KernelState, ensure_runtime_tree, load_kernel_state, persist_kernel_state
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.control_objects import NodeTerminalOutcome, NodeTerminalResult
from loop_product.protocols.node import NodeStatus
from loop_product.runtime_paths import require_runtime_root


def node_status_for_outcome(outcome: NodeTerminalOutcome) -> str:
    if outcome is NodeTerminalOutcome.PASS:
        return "COMPLETED"
    if outcome is NodeTerminalOutcome.STUCK:
        return "BLOCKED"
    return "FAILED"


def lifecycle_status(node: NodeSpec) -> str:
    return node.status.value


def build_node_terminal_result(
    node: NodeSpec,
    *,
    outcome: NodeTerminalOutcome,
    summary: str,
    evidence_refs: list[str] | tuple[str, ...] = (),
    self_attribution: str = "",
    self_repair: str = "",
) -> NodeTerminalResult:
    """Create a structured node terminal result for gateway submission."""

    return NodeTerminalResult(
        node_id=node.node_id,
        round_id=node.round_id,
        generation=node.generation,
        outcome=outcome,
        node_status=node_status_for_outcome(outcome),
        summary=summary,
        evidence_refs=[str(item) for item in evidence_refs],
        self_attribution=self_attribution,
        self_repair=self_repair,
    )


def bootstrap_first_implementer_node(**payload: Any) -> dict[str, Any]:
    """Public trusted surface for first implementer bootstrap."""

    authority = kernel_internal_authority()
    return dispatch_bootstrap_first_implementer_node(authority=authority, **payload)


def bootstrap_first_implementer_from_endpoint(**payload: Any) -> dict[str, Any]:
    """Public trusted surface for endpoint-driven first implementer bootstrap."""

    authority = kernel_internal_authority()
    return dispatch_bootstrap_first_implementer_from_endpoint(authority=authority, **payload)


def recover_orphaned_active_node(**payload: Any) -> dict[str, Any]:
    """Public trusted surface for same-node orphaned-ACTIVE recovery."""

    return dispatch_recover_orphaned_active_node(**payload)


def launch_child_from_result_ref(
    *,
    result_ref: str | Path,
    startup_probe_ms: int = 1500,
    startup_health_timeout_ms: int = 12000,
) -> dict[str, Any]:
    """Public trusted surface for committed child launch from bootstrap/recovery output."""

    return dispatch_launch_child_from_result_ref(
        result_ref=result_ref,
        startup_probe_ms=startup_probe_ms,
        startup_health_timeout_ms=startup_health_timeout_ms,
    )


def child_runtime_status_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
) -> dict[str, Any]:
    """Public trusted surface for committed child-runtime status queries."""

    return dispatch_child_runtime_status_from_launch_result_ref(
        result_ref=result_ref,
        stall_threshold_s=stall_threshold_s,
    )


def child_progress_snapshot_from_launch_result_ref(
    *,
    result_ref: str | Path,
    stall_threshold_s: float = 60.0,
) -> dict[str, Any]:
    """Public trusted surface for committed child progress snapshots."""

    return dispatch_child_progress_snapshot_from_launch_result_ref(
        result_ref=result_ref,
        stall_threshold_s=stall_threshold_s,
    )


def publish_external_from_implementer_result(*, result_ref: str | Path) -> dict[str, Any]:
    """Public trusted surface for deterministic root-kernel external publication."""

    return dispatch_publish_external_from_implementer_result(result_ref=result_ref)


def supervise_child_until_settled(
    *,
    launch_result_ref: str | Path,
    poll_interval_s: float = 2.0,
    stall_threshold_s: float = 60.0,
    max_recoveries: int = 5,
    max_wall_clock_s: float = 0.0,
    no_substantive_progress_window_s: float = 300.0,
) -> dict[str, Any]:
    """Public trusted surface for committed root-side child supervision."""

    return dispatch_supervise_child_until_settled(
        launch_result_ref=launch_result_ref,
        poll_interval_s=poll_interval_s,
        stall_threshold_s=stall_threshold_s,
        max_recoveries=max_recoveries,
        max_wall_clock_s=max_wall_clock_s,
        no_substantive_progress_window_s=no_substantive_progress_window_s,
        runtime_status_reader=dispatch_child_runtime_status_from_launch_result_ref,
        recovery_runner=dispatch_recover_orphaned_active_node,
        launcher=dispatch_launch_child_from_result_ref,
    )


def initialize_evaluator_runtime(
    *,
    state_root: Path,
    task_id: str,
    root_goal: str,
    child_goal_slice: str,
    child_node_id: str = "child-implementer-001",
    round_id: str = "R1",
) -> dict[str, Any]:
    """Prepare a trusted `.loop` runtime tree for an ordinary implementer evaluator run."""

    authority = kernel_internal_authority()
    state_root = require_runtime_root(state_root)
    ensure_runtime_tree(state_root)

    kernel_state_path = state_root / "state" / "kernel_state.json"
    if kernel_state_path.exists():
        kernel_state = load_kernel_state(state_root)
    else:
        root_node = NodeSpec(
            node_id="root-kernel",
            node_kind="kernel",
            goal_slice=root_goal,
            parent_node_id=None,
            generation=0,
            round_id="R0",
            execution_policy=kernel_execution_policy(),
            reasoning_profile=kernel_reasoning_profile(),
            budget_profile={"max_rounds": 1},
            allowed_actions=["dispatch", "submit", "audit"],
            delegation_ref="",
            result_sink_ref="artifacts/runtime_bootstrap_summary.json",
            lineage_ref="root-kernel",
            status=NodeStatus.ACTIVE,
        )
        kernel_state = KernelState(
            task_id=task_id,
            root_goal=root_goal,
            root_node_id=root_node.node_id,
        )
        kernel_state.register_node(root_node)
        persist_kernel_state(state_root, kernel_state, authority=authority)

    root_node_id = str(kernel_state.root_node_id)
    if root_node_id not in kernel_state.nodes:
        raise ValueError(f"kernel state missing root node snapshot for {root_node_id!r}")

    if child_node_id not in kernel_state.nodes:
        materialize_child(
            state_root=state_root,
            kernel_state=kernel_state,
            parent_node_id=root_node_id,
            node_id=child_node_id,
            goal_slice=child_goal_slice,
            round_id=round_id,
            execution_policy=implementer_execution_policy(),
            reasoning_profile=implementer_reasoning_profile(),
            budget_profile=implementer_budget_profile(),
            authority=authority,
        )

    runtime_context = load_child_runtime_context(state_root, child_node_id)
    return {
        "state_root": str(state_root.resolve()),
        "kernel_state_ref": str((state_root / "state" / "kernel_state.json").resolve()),
        "child_node_id": child_node_id,
        "child_node_ref": str((state_root / "state" / f"{child_node_id}.json").resolve()),
        "delegation_ref": str((state_root / "state" / "delegations" / f"{child_node_id}.json").resolve()),
        "workspace_root": str(runtime_context.get("workspace_root") or ""),
        "codex_home": str(runtime_context.get("codex_home") or ""),
        "runtime_context": runtime_context,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="loop_product runtime lifecycle helpers")
    subparsers = parser.add_subparsers(dest="command")

    bootstrap = subparsers.add_parser(
        "bootstrap-first-implementer",
        help="Materialize the first implementer node and its frozen launch bundle",
    )
    bootstrap.add_argument("--request", required=True, help="JSON file following LoopFirstImplementerBootstrapRequest")
    bootstrap_from_endpoint = subparsers.add_parser(
        "bootstrap-first-implementer-from-endpoint",
        help="Materialize the first implementer node directly from a clarified endpoint artifact",
    )
    bootstrap_from_endpoint.add_argument("--endpoint-artifact-ref", required=True, help="Path to EndpointArtifact.json")
    bootstrap_from_endpoint.add_argument("--external-publish-target", default="", help="Optional publish target outside the workspace")
    bootstrap_from_endpoint.add_argument("--workspace-mirror-relpath", default="", help="Optional workspace mirror relpath override")
    bootstrap_from_endpoint.add_argument("--task-slug", default="", help="Optional task slug override")
    bootstrap_from_endpoint.add_argument("--workspace-root", default="", help="Optional exact workspace root for fresh bootstrap")
    bootstrap_from_endpoint.add_argument("--state-root", default="", help="Optional exact state root for fresh bootstrap")
    bootstrap_from_endpoint.add_argument("--node-id", default="", help="Optional exact node id override")
    bootstrap_from_endpoint.add_argument("--round-id", default="R1", help="Optional round id override")
    bootstrap_from_endpoint.add_argument("--context-ref", action="append", default=[], help="Optional extra context ref (repeatable)")
    bootstrap_from_endpoint.add_argument("--result-sink-ref", default="", help="Optional result sink ref override")
    recovery = subparsers.add_parser(
        "recover-orphaned-active",
        help="Accept same-node retry for an orphaned ACTIVE child and rebuild its canonical launch spec",
    )
    recovery.add_argument("--request", default="", help="Optional JSON file following LoopOrphanedActiveRecoveryRequest")
    recovery.add_argument("--state-root", default="", help="Runtime state root for direct structured recovery")
    recovery.add_argument("--node-id", default="", help="Exact ACTIVE node id for direct structured recovery")
    recovery.add_argument("--workspace-root", default="", help="Optional exact workspace root guard for direct structured recovery")
    recovery.add_argument("--confirmed-launch-result-ref", default="", help="Optional exact launch result ref already confirmed by the caller")
    recovery.add_argument("--confirmed-runtime-status-ref", default="", help="Optional exact runtime status ref already confirmed by the caller")
    recovery.add_argument("--reason", default="", help="Recovery reason for direct structured recovery")
    recovery.add_argument("--self-attribution", default="", help="Self-attribution for direct structured recovery")
    recovery.add_argument("--self-repair", default="", help="Self-repair for direct structured recovery")
    recovery.add_argument("--observation-kind", default="", help="Observation kind for runtime-loss evidence")
    recovery.add_argument("--summary", default="", help="Human-readable runtime-loss summary")
    recovery.add_argument("--evidence-ref", action="append", default=[], help="Repeatable evidence ref for runtime-loss evidence")
    launch = subparsers.add_parser(
        "launch-child",
        help="Launch a materialized child from a bootstrap/recovery result carrying launch_spec",
    )
    launch.add_argument("--result-ref", required=True, help="Path to a bootstrap/recovery result JSON containing launch_spec")
    launch.add_argument("--startup-probe-ms", type=int, default=1500, help="Milliseconds to wait before classifying launch as started/exited")
    launch.add_argument(
        "--startup-health-timeout-ms",
        type=int,
        default=12000,
        help="Additional milliseconds to wait for startup failure evidence to settle before treating the child as healthy",
    )
    runtime_status = subparsers.add_parser(
        "child-runtime-status",
        help="Summarize current runtime status for a committed ChildLaunchResult",
    )
    runtime_status.add_argument("--result-ref", required=True, help="Path to a ChildLaunchResult.json")
    runtime_status.add_argument(
        "--stall-threshold-s",
        type=float,
        default=60.0,
        help="Seconds of log silence before a still-live child is marked as stalled_hint",
    )
    progress_snapshot = subparsers.add_parser(
        "child-progress-snapshot",
        help="Summarize current child progress, evaluator state, and observed document refs",
    )
    progress_snapshot.add_argument("--launch-result-ref", required=True, help="Path to a ChildLaunchResult.json")
    progress_snapshot.add_argument(
        "--stall-threshold-s",
        type=float,
        default=60.0,
        help="Seconds of log silence before a still-live child is considered stalled in the underlying runtime status",
    )
    supervise = subparsers.add_parser(
        "supervise-child",
        help="Keep supervising a launched child until PASS, a confirmed non-retryable terminal result, or timeout",
    )
    supervise.add_argument("--launch-result-ref", required=True, help="Path to a ChildLaunchResult.json")
    supervise.add_argument("--poll-interval-s", type=float, default=2.0, help="Polling interval between supervision checks")
    supervise.add_argument("--stall-threshold-s", type=float, default=60.0, help="Seconds of log silence before a live child is considered stalled")
    supervise.add_argument("--max-recoveries", type=int, default=5, help="Maximum committed recover/relaunch cycles before settling exhausted")
    supervise.add_argument("--max-wall-clock-s", type=float, default=0.0, help="Optional supervision timeout in seconds; 0 disables timeout")
    supervise.add_argument(
        "--no-substantive-progress-window-s",
        type=float,
        default=300.0,
        help="Optional placeholder-only progress window in seconds before supervision returns a truthful no_substantive_progress stop point; 0 disables it",
    )
    publish = subparsers.add_parser(
        "publish-external-result",
        help="Publish the exact implementer workspace artifact to its frozen external target",
    )
    publish.add_argument("--result-ref", required=True, help="Path to the authoritative implementer_result.json")
    return parser


def _cmd_bootstrap_first_implementer(args: argparse.Namespace) -> int:
    request_path = Path(args.request).expanduser().resolve()
    payload = json.loads(request_path.read_text(encoding="utf-8"))
    result = bootstrap_first_implementer_node(**payload)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_bootstrap_first_implementer_from_endpoint(args: argparse.Namespace) -> int:
    result = bootstrap_first_implementer_from_endpoint(
        endpoint_artifact_ref=args.endpoint_artifact_ref,
        external_publish_target=args.external_publish_target,
        workspace_mirror_relpath=args.workspace_mirror_relpath,
        task_slug=args.task_slug,
        workspace_root=args.workspace_root,
        state_root=args.state_root,
        node_id=args.node_id,
        round_id=args.round_id,
        context_refs=list(args.context_ref or []),
        result_sink_ref=args.result_sink_ref,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_recover_orphaned_active(args: argparse.Namespace) -> int:
    if str(args.request or "").strip():
        request_path = Path(args.request).expanduser().resolve()
        payload = json.loads(request_path.read_text(encoding="utf-8"))
    else:
        payload = {
            "state_root": args.state_root,
            "node_id": args.node_id,
            "workspace_root": args.workspace_root,
            "confirmed_launch_result_ref": args.confirmed_launch_result_ref,
            "confirmed_runtime_status_ref": args.confirmed_runtime_status_ref,
            "reason": args.reason,
            "self_attribution": args.self_attribution,
            "self_repair": args.self_repair,
            "observation_kind": args.observation_kind,
            "summary": args.summary,
            "evidence_refs": list(args.evidence_ref or []),
        }
    result = recover_orphaned_active_node(**payload)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_publish_external_result(args: argparse.Namespace) -> int:
    result = publish_external_from_implementer_result(result_ref=args.result_ref)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_supervise_child(args: argparse.Namespace) -> int:
    result = supervise_child_until_settled(
        launch_result_ref=args.launch_result_ref,
        poll_interval_s=args.poll_interval_s,
        stall_threshold_s=args.stall_threshold_s,
        max_recoveries=args.max_recoveries,
        max_wall_clock_s=args.max_wall_clock_s,
        no_substantive_progress_window_s=args.no_substantive_progress_window_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_launch_child(args: argparse.Namespace) -> int:
    result = launch_child_from_result_ref(
        result_ref=args.result_ref,
        startup_probe_ms=args.startup_probe_ms,
        startup_health_timeout_ms=args.startup_health_timeout_ms,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_child_runtime_status(args: argparse.Namespace) -> int:
    result = child_runtime_status_from_launch_result_ref(
        result_ref=args.result_ref,
        stall_threshold_s=args.stall_threshold_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def _cmd_child_progress_snapshot(args: argparse.Namespace) -> int:
    result = child_progress_snapshot_from_launch_result_ref(
        result_ref=args.launch_result_ref,
        stall_threshold_s=args.stall_threshold_s,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if args.command == "bootstrap-first-implementer":
        return _cmd_bootstrap_first_implementer(args)
    if args.command == "bootstrap-first-implementer-from-endpoint":
        return _cmd_bootstrap_first_implementer_from_endpoint(args)
    if args.command == "recover-orphaned-active":
        return _cmd_recover_orphaned_active(args)
    if args.command == "launch-child":
        return _cmd_launch_child(args)
    if args.command == "child-runtime-status":
        return _cmd_child_runtime_status(args)
    if args.command == "child-progress-snapshot":
        return _cmd_child_progress_snapshot(args)
    if args.command == "supervise-child":
        return _cmd_supervise_child(args)
    if args.command == "publish-external-result":
        return _cmd_publish_external_result(args)
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
