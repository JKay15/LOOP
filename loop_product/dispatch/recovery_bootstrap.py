"""Deterministic recovery bootstrap for orphaned ACTIVE child nodes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loop_product.dispatch.launch_runtime import child_runtime_status_from_launch_result_ref
from loop_product.dispatch.launch_policy import build_codex_cli_child_launch
from loop_product.dispatch.launch_policy import merge_safe_parent_transport_env
from loop_product.evaluator_authority import authoritative_result_retryable_nonterminal
from loop_product.kernel.authority import KernelMutationAuthority
from loop_product.kernel.state import query_kernel_state_object
from loop_product.protocols.control_envelope import EnvelopeStatus
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime.liveness import build_runtime_loss_signal
from loop_product.runtime.recover import build_retry_request
from loop_product.runtime_paths import node_machine_handoff_ref, require_runtime_root


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _dedupe_refs(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        value = _nonempty(raw)
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _normalize_request(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = {
        "state_root": _nonempty(payload.get("state_root")),
        "node_id": _nonempty(payload.get("node_id")),
        "workspace_root": _nonempty(payload.get("workspace_root")),
        "confirmed_launch_result_ref": _nonempty(payload.get("confirmed_launch_result_ref")),
        "confirmed_runtime_status_ref": _nonempty(payload.get("confirmed_runtime_status_ref")),
        "reason": _nonempty(payload.get("reason")),
        "self_attribution": _nonempty(payload.get("self_attribution")),
        "self_repair": _nonempty(payload.get("self_repair")),
        "observation_kind": _nonempty(payload.get("observation_kind")),
        "summary": _nonempty(payload.get("summary")),
        "evidence_refs": [str(item) for item in (payload.get("evidence_refs") or [])],
    }
    validate_repo_object("LoopOrphanedActiveRecoveryRequest.schema.json", normalized)
    return normalized


def _persist_recovery_artifacts(*, state_root: Path, node_id: str, request_payload: dict[str, Any], result_payload: dict[str, Any]) -> tuple[Path, Path]:
    recovery_dir = state_root / "artifacts" / "recovery" / node_id
    request_path = recovery_dir / "OrphanedActiveRecoveryRequest.json"
    result_path = recovery_dir / "OrphanedActiveRecoveryResult.json"
    _write_json(request_path, request_payload)
    _write_json(result_path, result_payload)
    return request_path, result_path


def _latest_authoritative_result_ref(*, state_root: Path, node: NodeSpec) -> Path:
    result_sink_ref = str(node.result_sink_ref or f"artifacts/{node.node_id}/implementer_result.json").strip()
    return (state_root / result_sink_ref).resolve()


def _write_recovery_context(
    *,
    workspace_root: Path,
    request: dict[str, Any],
    latest_result_ref: Path | None,
    result_payload: dict[str, Any],
) -> Path:
    context_path = workspace_root / "RECOVERY_CONTEXT.md"
    lines = [
        "# Recovery Context",
        "",
        f"- node: `{result_payload['node_id']}`",
        f"- recovery_decision: `{result_payload['recovery_decision']}`",
        f"- reason: `{request['reason']}`",
        f"- self_attribution: `{request['self_attribution']}`",
        f"- self_repair: `{request['self_repair']}`",
        f"- confirmed_launch_result_ref: `{result_payload['confirmed_launch_result_ref']}`",
        f"- confirmed_runtime_status_ref: `{result_payload['confirmed_runtime_status_ref']}`",
    ]
    if latest_result_ref is not None and latest_result_ref.exists():
        lines.append(f"- latest_implementer_result_ref: `{latest_result_ref}`")
    evidence_refs = [str(item) for item in list(request.get("evidence_refs") or []) if str(item).strip()]
    if evidence_refs:
        lines.extend(["", "## Evidence Refs", ""])
        lines.extend([f"- `{item}`" for item in evidence_refs])
    context_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return context_path


def _refresh_continue_exact_bundle(
    *,
    authority: KernelMutationAuthority,
    state_root: Path,
    node: NodeSpec,
    workspace_root: Path,
    recovery_context_ref: Path | None = None,
    recovery_evidence_refs: list[str] | None = None,
) -> dict[str, Any]:
    from loop_product.dispatch.bootstrap import bootstrap_first_implementer_node

    handoff_path = node_machine_handoff_ref(state_root=state_root, node_id=node.node_id)
    if not handoff_path.exists():
        raise ValueError(f"same-node recovery requires an existing frozen handoff at {handoff_path}")
    handoff_payload = json.loads(handoff_path.read_text(encoding="utf-8"))
    kernel_state = query_kernel_state_object(state_root, continue_deferred=False)
    refreshed_context_refs = _dedupe_refs(
        [
            *[str(item) for item in list(handoff_payload.get("context_refs") or [])],
            str(recovery_context_ref.resolve()) if recovery_context_ref is not None else "",
            *[str(item) for item in list(recovery_evidence_refs or [])],
        ]
    )
    bootstrap_payload = {
        "mode": "continue_exact",
        "task_slug": str(kernel_state.task_id or state_root.name or node.node_id),
        "root_goal": _nonempty(handoff_payload.get("root_goal")) or str(kernel_state.root_goal or node.goal_slice),
        "child_goal_slice": _nonempty(handoff_payload.get("child_goal_slice")) or str(node.goal_slice or ""),
        "workflow_scope": _nonempty(handoff_payload.get("workflow_scope")) or str(node.workflow_scope or "generic"),
        "artifact_scope": _nonempty(handoff_payload.get("artifact_scope")) or str(node.artifact_scope or "task"),
        "terminal_authority_scope": _nonempty(handoff_payload.get("terminal_authority_scope"))
        or str(node.terminal_authority_scope or "local"),
        "endpoint_artifact_ref": _nonempty(handoff_payload.get("endpoint_artifact_ref")),
        "workspace_root": str(workspace_root.resolve()),
        "state_root": str(state_root.resolve()),
        "node_id": node.node_id,
        "round_id": str(node.round_id or handoff_payload.get("round_id") or ""),
        "workspace_mirror_relpath": _nonempty(handoff_payload.get("workspace_mirror_relpath")),
        "workspace_live_artifact_relpath": _nonempty(handoff_payload.get("workspace_live_artifact_relpath")),
        "external_publish_target": _nonempty(handoff_payload.get("external_publish_target")),
        "context_refs": refreshed_context_refs,
        "required_output_paths": [str(item).strip() for item in list(handoff_payload.get("required_output_paths") or []) if str(item).strip()],
        "startup_required_output_paths": [
            str(item).strip() for item in list(handoff_payload.get("startup_required_output_paths") or []) if str(item).strip()
        ],
        "progress_checkpoints": list(handoff_payload.get("progress_checkpoints") or []),
        "result_sink_ref": _nonempty(node.result_sink_ref) or _nonempty(handoff_payload.get("result_sink_ref")),
    }
    return bootstrap_first_implementer_node(authority=authority, **bootstrap_payload)


def _latest_launch_result_ref(*, state_root: Path, node_id: str, workspace_root: Path) -> Path:
    launch_root = state_root / "artifacts" / "launches" / node_id
    if not launch_root.exists():
        raise ValueError(f"orphaned-active recovery requires an existing committed launch result for {node_id!r}")
    for result_path in sorted(launch_root.glob("attempt_*/ChildLaunchResult.json"), reverse=True):
        try:
            payload = json.loads(result_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if str(payload.get("node_id") or "") != node_id:
            continue
        if Path(str(payload.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
            continue
        if Path(str(payload.get("state_root") or "")).expanduser().resolve() != state_root:
            continue
        return result_path.resolve()
    raise ValueError(
        f"orphaned-active recovery requires an exact committed launch result for {node_id!r} at {workspace_root}"
    )


def _confirmed_launch_result_ref(*, request: dict[str, Any], state_root: Path, node_id: str, workspace_root: Path) -> Path:
    raw = str(request.get("confirmed_launch_result_ref") or "").strip()
    if not raw:
        return _latest_launch_result_ref(state_root=state_root, node_id=node_id, workspace_root=workspace_root)
    confirmed_ref = Path(raw).expanduser().resolve()
    if not confirmed_ref.exists():
        raise ValueError(f"confirmed launch result ref does not exist: {confirmed_ref}")
    payload = json.loads(confirmed_ref.read_text(encoding="utf-8"))
    if str(payload.get("node_id") or "") != node_id:
        raise ValueError(f"confirmed launch result node_id mismatch for {node_id!r}: {confirmed_ref}")
    if Path(str(payload.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
        raise ValueError(f"confirmed launch result workspace_root mismatch for {node_id!r}: {confirmed_ref}")
    if Path(str(payload.get("state_root") or "")).expanduser().resolve() != state_root:
        raise ValueError(f"confirmed launch result state_root mismatch for {node_id!r}: {confirmed_ref}")
    return confirmed_ref


def _confirmed_runtime_status(
    *,
    request: dict[str, Any],
    confirmed_launch_result_ref: Path,
    node: NodeSpec,
    node_id: str,
    state_root: Path,
    workspace_root: Path,
) -> dict[str, Any]:
    def _validate_status_payload(status_payload: dict[str, Any]) -> dict[str, Any]:
        if str(status_payload.get("node_id") or "") != node_id:
            raise ValueError(f"confirmed runtime status node_id mismatch for {node_id!r}")
        if Path(str(status_payload.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
            raise ValueError(f"confirmed runtime status workspace_root mismatch for {node_id!r}")
        if Path(str(status_payload.get("state_root") or "")).expanduser().resolve() != state_root:
            raise ValueError(f"confirmed runtime status state_root mismatch for {node_id!r}")
        if Path(str(status_payload.get("launch_result_ref") or "")).expanduser().resolve() != confirmed_launch_result_ref:
            raise ValueError(
                f"confirmed runtime status launch_result_ref mismatch for {node_id!r}: "
                f"{status_payload.get('launch_result_ref')!r}"
            )
        return status_payload

    def _retryable_nonterminal_refresh_needed(status_payload: dict[str, Any]) -> bool:
        if bool(status_payload.get("recovery_eligible")):
            return False
        if bool(status_payload.get("pid_alive")):
            return False
        if str(status_payload.get("lifecycle_status") or "").strip().upper() not in {"", "ACTIVE"}:
            return False
        latest_result_ref = _latest_authoritative_result_ref(state_root=state_root, node=node)
        if not latest_result_ref.exists():
            return False
        try:
            latest_result_payload = json.loads(latest_result_ref.read_text(encoding="utf-8"))
        except Exception:
            return False
        return authoritative_result_retryable_nonterminal(latest_result_payload)

    raw = str(request.get("confirmed_runtime_status_ref") or "").strip()
    if raw:
        status_ref = Path(raw).expanduser().resolve()
        if not status_ref.exists():
            raise ValueError(f"confirmed runtime status ref does not exist: {status_ref}")
        confirmed_runtime_status = _validate_status_payload(json.loads(status_ref.read_text(encoding="utf-8")))
        if _retryable_nonterminal_refresh_needed(confirmed_runtime_status):
            confirmed_runtime_status = _validate_status_payload(
                child_runtime_status_from_launch_result_ref(
                    result_ref=confirmed_launch_result_ref,
                )
            )
    else:
        confirmed_runtime_status = _validate_status_payload(
            child_runtime_status_from_launch_result_ref(
                result_ref=confirmed_launch_result_ref,
            )
        )
    return confirmed_runtime_status


def recover_orphaned_active_node(*, authority: KernelMutationAuthority, **payload: Any) -> dict[str, Any]:
    """Accept a same-node retry for an orphaned ACTIVE child and rebuild its canonical launch spec."""

    from loop_product.kernel.submit import submit_topology_mutation

    request = _normalize_request(dict(payload))
    state_root = require_runtime_root(Path(request["state_root"]).expanduser().resolve())
    node_id = request["node_id"]
    kernel_state = query_kernel_state_object(state_root, continue_deferred=False)
    node_payload = dict(kernel_state.nodes.get(node_id) or {})
    if not node_payload:
        raise ValueError(f"orphaned-active recovery requires an exact existing node_id: {node_id!r}")
    node = NodeSpec.from_dict(node_payload)
    if node.status.value != "ACTIVE":
        raise ValueError(f"orphaned-active recovery requires an ACTIVE node, got {node.status.value!r}")
    workspace_root = Path(str(node.workspace_root or "")).expanduser().resolve()
    if not str(workspace_root):
        raise ValueError(f"node {node.node_id!r} is missing workspace_root")
    if request["workspace_root"]:
        expected_workspace = Path(request["workspace_root"]).expanduser().resolve()
        if expected_workspace != workspace_root:
            raise ValueError(
                f"exact workspace_root mismatch for orphaned-active recovery: expected {workspace_root}, got {expected_workspace}"
            )

    child_prompt_path = workspace_root / "CHILD_PROMPT.md"
    if not child_prompt_path.exists():
        raise ValueError(f"orphaned-active recovery requires an existing CHILD_PROMPT.md at {child_prompt_path}")

    confirmed_launch_result_ref = _confirmed_launch_result_ref(
        request=request,
        state_root=state_root,
        node_id=node_id,
        workspace_root=workspace_root,
    )
    confirmed_runtime_status = _confirmed_runtime_status(
        request=request,
        confirmed_launch_result_ref=confirmed_launch_result_ref,
        node=node,
        node_id=node_id,
        state_root=state_root,
        workspace_root=workspace_root,
    )
    if not bool(confirmed_runtime_status.get("recovery_eligible")):
        raise ValueError(
            "orphaned-active recovery requires a fresh committed runtime-loss confirmation "
            f"for {node_id!r}, got {confirmed_runtime_status.get('recovery_reason')!r}"
        )

    runtime_loss_signal = build_runtime_loss_signal(
        observation_kind=str(confirmed_runtime_status.get("runtime_observation_kind") or request["observation_kind"]),
        summary=str(confirmed_runtime_status.get("runtime_summary") or request["summary"]),
        evidence_refs=[
            str(confirmed_runtime_status.get("status_result_ref") or ""),
            *[str(item) for item in list(request["evidence_refs"])],
            *[str(item) for item in list(confirmed_runtime_status.get("runtime_evidence_refs") or [])],
        ],
        observed_at=str(confirmed_runtime_status.get("runtime_observed_at") or ""),
    )
    mutation = build_retry_request(
        node,
        reason=request["reason"],
        self_attribution=request["self_attribution"],
        self_repair=request["self_repair"],
        payload={"runtime_loss_signal": runtime_loss_signal},
    )
    envelope = submit_topology_mutation(
        state_root,
        mutation,
        round_id=node.round_id,
        generation=node.generation,
    )
    if envelope.status is not EnvelopeStatus.ACCEPTED:
        raise ValueError(f"orphaned-active recovery was not accepted for {node.node_id}: {envelope.note}")

    refreshed_state = query_kernel_state_object(state_root, continue_deferred=False)
    refreshed_payload = dict(refreshed_state.nodes.get(node_id) or {})
    refreshed_node = NodeSpec.from_dict(refreshed_payload)
    latest_result_ref = _latest_authoritative_result_ref(state_root=state_root, node=refreshed_node)
    preliminary_result_payload = {
        "recovery_decision": "accepted",
        "node_id": refreshed_node.node_id,
        "confirmed_launch_result_ref": str(confirmed_launch_result_ref),
        "confirmed_runtime_status_ref": str(confirmed_runtime_status.get("status_result_ref") or ""),
    }
    recovery_context_ref = _write_recovery_context(
        workspace_root=workspace_root,
        request=request,
        latest_result_ref=latest_result_ref if latest_result_ref.exists() else None,
        result_payload=preliminary_result_payload,
    )
    refreshed_bundle = _refresh_continue_exact_bundle(
        authority=authority,
        state_root=state_root,
        node=refreshed_node,
        workspace_root=workspace_root,
        recovery_context_ref=recovery_context_ref,
        recovery_evidence_refs=[str(item) for item in list(request.get("evidence_refs") or [])],
    )
    refreshed_child_prompt = Path(str(refreshed_bundle.get("child_prompt_ref") or child_prompt_path)).expanduser().resolve()
    if not refreshed_child_prompt.exists():
        raise ValueError(f"refreshed recovery bundle is missing CHILD_PROMPT.md at {refreshed_child_prompt}")
    child_prompt_path = refreshed_child_prompt
    launch_spec = dict(refreshed_bundle.get("launch_spec") or {})
    if not launch_spec:
        launch_spec = build_codex_cli_child_launch(
            workspace_root=workspace_root,
            sandbox_mode=str(refreshed_node.execution_policy.get("sandbox_mode") or "danger-full-access"),
            thinking_budget=str(refreshed_node.reasoning_profile.get("thinking_budget") or "medium"),
            prompt_path=child_prompt_path,
        )
    else:
        launch_spec["env"] = merge_safe_parent_transport_env(dict(launch_spec.get("env") or {}))

    result_payload = {
        "recovery_decision": "accepted",
        "node_id": refreshed_node.node_id,
        "workspace_root": str(workspace_root),
        "state_root": str(state_root),
        "node_ref": str((state_root / "state" / f"{refreshed_node.node_id}.json").resolve()),
        "delegation_ref": str((state_root / "state" / "delegations" / f"{refreshed_node.node_id}.json").resolve()),
        "child_prompt_ref": str(child_prompt_path.resolve()),
        "launch_spec": launch_spec,
        "confirmed_launch_result_ref": str(confirmed_launch_result_ref),
        "confirmed_runtime_status_ref": str(confirmed_runtime_status.get("status_result_ref") or ""),
        "confirmed_runtime_recovery_reason": str(confirmed_runtime_status.get("recovery_reason") or ""),
        "accepted_envelope_id": str(envelope.envelope_id or ""),
        "accepted_envelope_note": str(envelope.note or ""),
    }
    result_payload["recovery_context_ref"] = str(recovery_context_ref.resolve())
    if latest_result_ref.exists():
        result_payload["latest_implementer_result_ref"] = str(latest_result_ref)
    request_ref, result_ref = _persist_recovery_artifacts(
        state_root=state_root,
        node_id=node_id,
        request_payload=request,
        result_payload=result_payload,
    )
    result_payload["recovery_request_ref"] = str(request_ref.resolve())
    result_payload["recovery_result_ref"] = str(result_ref.resolve())
    validate_repo_object("LoopOrphanedActiveRecoveryResult.schema.json", result_payload)
    _write_json(result_ref, result_payload)
    return result_payload
