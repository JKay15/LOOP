"""Deterministic recovery bootstrap for orphaned ACTIVE child nodes."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from loop_product.dispatch.launch_runtime import child_runtime_status_from_launch_result_ref
from loop_product.dispatch.launch_policy import build_codex_cli_child_launch
from loop_product.kernel.state import load_kernel_state
from loop_product.protocols.control_envelope import EnvelopeStatus
from loop_product.protocols.node import NodeSpec
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime.liveness import build_runtime_loss_signal
from loop_product.runtime.recover import build_retry_request
from loop_product.runtime_paths import require_runtime_root


def _nonempty(value: Any) -> str:
    return str(value or "").strip()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
    node_id: str,
    state_root: Path,
    workspace_root: Path,
) -> dict[str, Any]:
    raw = str(request.get("confirmed_runtime_status_ref") or "").strip()
    if raw:
        status_ref = Path(raw).expanduser().resolve()
        if not status_ref.exists():
            raise ValueError(f"confirmed runtime status ref does not exist: {status_ref}")
        confirmed_runtime_status = json.loads(status_ref.read_text(encoding="utf-8"))
    else:
        confirmed_runtime_status = child_runtime_status_from_launch_result_ref(
            result_ref=confirmed_launch_result_ref,
        )
    if str(confirmed_runtime_status.get("node_id") or "") != node_id:
        raise ValueError(f"confirmed runtime status node_id mismatch for {node_id!r}")
    if Path(str(confirmed_runtime_status.get("workspace_root") or "")).expanduser().resolve() != workspace_root:
        raise ValueError(f"confirmed runtime status workspace_root mismatch for {node_id!r}")
    if Path(str(confirmed_runtime_status.get("state_root") or "")).expanduser().resolve() != state_root:
        raise ValueError(f"confirmed runtime status state_root mismatch for {node_id!r}")
    if Path(str(confirmed_runtime_status.get("launch_result_ref") or "")).expanduser().resolve() != confirmed_launch_result_ref:
        raise ValueError(
            f"confirmed runtime status launch_result_ref mismatch for {node_id!r}: "
            f"{confirmed_runtime_status.get('launch_result_ref')!r}"
        )
    return confirmed_runtime_status


def recover_orphaned_active_node(**payload: Any) -> dict[str, Any]:
    """Accept a same-node retry for an orphaned ACTIVE child and rebuild its canonical launch spec."""

    from loop_product.kernel.submit import submit_topology_mutation

    request = _normalize_request(dict(payload))
    state_root = require_runtime_root(Path(request["state_root"]).expanduser().resolve())
    node_id = request["node_id"]
    kernel_state = load_kernel_state(state_root)
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

    refreshed_state = load_kernel_state(state_root)
    refreshed_payload = dict(refreshed_state.nodes.get(node_id) or {})
    refreshed_node = NodeSpec.from_dict(refreshed_payload)
    latest_result_ref = _latest_authoritative_result_ref(state_root=state_root, node=refreshed_node)
    launch_spec = build_codex_cli_child_launch(
        workspace_root=workspace_root,
        sandbox_mode=str(refreshed_node.execution_policy.get("sandbox_mode") or "danger-full-access"),
        thinking_budget=str(refreshed_node.reasoning_profile.get("thinking_budget") or "medium"),
        prompt_path=child_prompt_path,
    )

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
    recovery_context_ref = _write_recovery_context(
        workspace_root=workspace_root,
        request=request,
        latest_result_ref=latest_result_ref if latest_result_ref.exists() else None,
        result_payload=result_payload,
    )
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
