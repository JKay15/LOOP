#!/usr/bin/env python3
"""Submit a kernel-reviewed split request from frozen handoff context."""

from __future__ import annotations

import argparse
from collections.abc import Sequence as SequenceABC
import json
from pathlib import Path
import re
from typing import Any

from loop_product.control_intent import (
    ARTIFACT_SCOPE_SPEC,
    TERMINAL_AUTHORITY_SCOPE_SPEC,
    WORKFLOW_SCOPE_SPEC,
    default_split_request_refs,
    default_progress_checkpoints,
    default_startup_required_output_paths,
    normalize_activation_condition,
    normalize_machine_choice,
)
from loop_product.kernel.state import query_kernel_state_object
from loop_product.kernel.submit import submit_topology_mutation
from loop_product.protocols.node import NodeSpec, normalize_progress_checkpoints
from loop_product.runtime_paths import node_machine_handoff_ref
from loop_product.topology.split_review import build_split_request, _supported_activation_condition


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def _extract_markdown_field(text: str, field_name: str) -> str:
    pattern = rf"^- {re.escape(field_name)}:\s+`([^`]+)`\s*$"
    for line in text.splitlines():
        match = re.match(pattern, line.strip())
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _resolve_handoff_json_ref(handoff_ref: Path) -> Path:
    if handoff_ref.suffix.lower() == ".json":
        return handoff_ref
    if handoff_ref.suffix.lower() != ".md":
        return handoff_ref
    sibling_json = handoff_ref.with_suffix(".json")
    if sibling_json.exists():
        return sibling_json
    markdown = handoff_ref.read_text(encoding="utf-8")
    state_root_raw = _extract_markdown_field(markdown, "state_root")
    node_id = _extract_markdown_field(markdown, "node_id")
    if state_root_raw and node_id:
        candidate = node_machine_handoff_ref(
            state_root=Path(state_root_raw).expanduser().resolve(),
            node_id=node_id,
        )
        if candidate.exists():
            return candidate
    raise ValueError(
        f"handoff_ref {handoff_ref} is markdown; could not resolve a canonical FROZEN_HANDOFF.json from sibling path or embedded state_root/node_id"
    )


def _normalize_target_nodes(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        raise ValueError("proposal.target_nodes must be a non-empty array")
    normalized: list[dict[str, Any]] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"proposal.target_nodes[{idx}] must be an object")
        node_id = str(item.get("node_id") or "").strip()
        goal_slice = str(item.get("goal_slice") or "").strip()
        if not node_id or not goal_slice:
            raise ValueError(f"proposal.target_nodes[{idx}] must include non-empty node_id and goal_slice")
        normalized_item = {
            "node_id": node_id,
            "goal_slice": goal_slice,
            "workflow_scope": normalize_machine_choice(item.get("workflow_scope"), WORKFLOW_SCOPE_SPEC),
            "artifact_scope": normalize_machine_choice(
                item.get("artifact_scope") if item.get("artifact_scope") not in (None, "") else "slice",
                ARTIFACT_SCOPE_SPEC,
            ),
            "terminal_authority_scope": normalize_machine_choice(
                item.get("terminal_authority_scope") if item.get("terminal_authority_scope") not in (None, "") else "local",
                TERMINAL_AUTHORITY_SCOPE_SPEC,
            ),
        }
        required_output_paths_raw = item.get("required_output_paths")
        startup_required_output_paths_raw = item.get("startup_required_output_paths")
        progress_checkpoints_raw = item.get("progress_checkpoints")
        if required_output_paths_raw in (None, ""):
            resolved_required_output_paths: list[str] = []
        elif isinstance(required_output_paths_raw, SequenceABC) and not isinstance(required_output_paths_raw, (str, bytes)):
            resolved_required_output_paths = [str(entry).strip() for entry in required_output_paths_raw if str(entry).strip()]
        else:
            raise ValueError(f"proposal.target_nodes[{idx}].required_output_paths must be a list of strings")
        if startup_required_output_paths_raw in (None, ""):
            resolved_startup_required_output_paths = default_startup_required_output_paths(
                workflow_scope=normalized_item["workflow_scope"],
                artifact_scope=normalized_item["artifact_scope"],
                terminal_authority_scope=normalized_item["terminal_authority_scope"],
            )
        elif isinstance(startup_required_output_paths_raw, SequenceABC) and not isinstance(
            startup_required_output_paths_raw,
            (str, bytes),
        ):
            resolved_startup_required_output_paths = [
                str(entry).strip() for entry in startup_required_output_paths_raw if str(entry).strip()
            ]
        else:
            raise ValueError(f"proposal.target_nodes[{idx}].startup_required_output_paths must be a list of strings")
        if progress_checkpoints_raw in (None, ""):
            resolved_progress_checkpoints = default_progress_checkpoints(
                workflow_scope=normalized_item["workflow_scope"],
                artifact_scope=normalized_item["artifact_scope"],
                terminal_authority_scope=normalized_item["terminal_authority_scope"],
            )
        elif isinstance(progress_checkpoints_raw, SequenceABC) and not isinstance(progress_checkpoints_raw, (str, bytes)):
            resolved_progress_checkpoints = normalize_progress_checkpoints(progress_checkpoints_raw)
            if len(resolved_progress_checkpoints) != len(list(progress_checkpoints_raw)):
                raise ValueError(
                    f"proposal.target_nodes[{idx}].progress_checkpoints must be a list of machine-readable checkpoint objects"
                )
        else:
            raise ValueError(f"proposal.target_nodes[{idx}].progress_checkpoints must be a list of checkpoint objects")
        if resolved_required_output_paths:
            normalized_item["required_output_paths"] = resolved_required_output_paths
        if resolved_startup_required_output_paths:
            normalized_item["startup_required_output_paths"] = resolved_startup_required_output_paths
        if resolved_progress_checkpoints:
            normalized_item["progress_checkpoints"] = resolved_progress_checkpoints
        depends_on = [str(entry).strip() for entry in list(item.get("depends_on_node_ids") or []) if str(entry).strip()]
        if depends_on:
            normalized_item["depends_on_node_ids"] = depends_on
        activation_condition_raw = str(item.get("activation_condition") or "").strip()
        activation_condition = normalize_activation_condition(activation_condition_raw)
        activation_rationale = str(item.get("activation_rationale") or "").strip()
        if activation_condition_raw:
            if _supported_activation_condition(activation_condition_raw):
                normalized_item["activation_condition"] = activation_condition
            else:
                if not activation_rationale:
                    activation_rationale = activation_condition_raw
                if not depends_on:
                    raise ValueError(
                        "proposal.target_nodes[{idx}].activation_condition must use supported "
                        "after:<node_id>:<requirement> syntax unless depends_on_node_ids already provide the machine gate; "
                        "put explanatory prose in activation_rationale".format(idx=idx)
                    )
        if activation_rationale:
            normalized_item["activation_rationale"] = activation_rationale
        normalized.append(normalized_item)
    return normalized


def _resolve_state_root(handoff: dict[str, Any]) -> Path:
    direct = str(handoff.get("state_root") or "").strip()
    if direct:
        return Path(direct).expanduser().resolve()

    kernel_result_sink_ref = str(handoff.get("kernel_result_sink_ref") or "").strip()
    if kernel_result_sink_ref:
        sink_path = Path(kernel_result_sink_ref).expanduser().resolve()
        for parent in sink_path.parents:
            if parent.name == "artifacts":
                return parent.parent.resolve()

    raise ValueError("handoff must include state_root or a kernel_result_sink_ref rooted under the runtime artifacts tree")


def _resolve_live_artifact_root(handoff: dict[str, Any]) -> Path:
    direct = str(handoff.get("workspace_live_artifact_ref") or "").strip()
    if direct:
        return Path(direct).expanduser().resolve()
    workspace_root = str(handoff.get("workspace_root") or "").strip()
    live_relpath = str(handoff.get("workspace_live_artifact_relpath") or "").strip()
    if workspace_root and live_relpath:
        return (Path(workspace_root).expanduser().resolve() / live_relpath).resolve()
    raise ValueError(
        "handoff must include workspace_live_artifact_ref or workspace_root + workspace_live_artifact_relpath "
        "to derive deterministic split proposal/result refs"
    )


def _build_result_payload(*, handoff_ref: Path, proposal_ref: Path, envelope: dict[str, Any]) -> dict[str, Any]:
    payload = dict(envelope)
    payload_status = str(payload.get("status") or "").strip().upper()
    return {
        "handoff_ref": str(handoff_ref.resolve()),
        "proposal_ref": str(proposal_ref.resolve()),
        "status": payload_status,
        "envelope": payload,
        "review": dict((payload.get("payload") or {}).get("review") or {}),
        "accepted": payload_status == "ACCEPTED",
        "envelope_id": str(payload.get("envelope_id") or ""),
    }


def _build_rejected_result_payload(*, handoff_ref: Path, proposal_ref: Path, error: Exception | str) -> dict[str, Any]:
    return {
        "handoff_ref": str(handoff_ref.resolve()),
        "proposal_ref": str(proposal_ref.resolve()),
        "status": "REJECTED",
        "accepted": False,
        "envelope": {},
        "review": {},
        "envelope_id": "",
        "error": str(error),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit a split request from frozen handoff context.")
    parser.add_argument("--handoff-ref", required=True)
    parser.add_argument("--proposal-ref")
    parser.add_argument("--result-ref")
    args = parser.parse_args()

    handoff_ref = Path(args.handoff_ref).expanduser().resolve()
    proposal_ref: Path | None = Path(args.proposal_ref).expanduser().resolve() if args.proposal_ref else None
    result_ref: Path | None = Path(args.result_ref).expanduser().resolve() if args.result_ref else None
    resolved_handoff_ref = handoff_ref

    try:
        resolved_handoff_ref = _resolve_handoff_json_ref(handoff_ref)
        handoff = _load_json(resolved_handoff_ref)
        if proposal_ref is None or result_ref is None:
            default_proposal_ref, default_result_ref = default_split_request_refs(
                _resolve_live_artifact_root(handoff),
                workspace_mirror_relpath=str(handoff.get("workspace_mirror_relpath") or ""),
            )
            proposal_ref = proposal_ref or default_proposal_ref
            result_ref = result_ref or default_result_ref
        proposal = _load_json(proposal_ref)

        state_root = _resolve_state_root(handoff)
        node_id = str(handoff.get("node_id") or "").strip()
        round_id = str(handoff.get("round_id") or "").strip()
        if not node_id or not round_id:
            raise ValueError("handoff must include non-empty node_id and round_id")

        kernel_state = query_kernel_state_object(state_root, continue_deferred=False)
        source_raw = dict(kernel_state.nodes.get(node_id) or {})
        if not source_raw:
            raise ValueError(f"source node not found in kernel state: {node_id}")
        source_node = NodeSpec.from_dict(source_raw)

        mutation = build_split_request(
            source_node=source_node,
            target_nodes=_normalize_target_nodes(proposal.get("target_nodes")),
            split_mode=str(proposal.get("split_mode") or "parallel"),
            completed_work=str(proposal.get("completed_work") or ""),
            remaining_work=str(proposal.get("remaining_work") or ""),
            reason=str(proposal.get("reason") or ""),
        )
        envelope = submit_topology_mutation(
            state_root,
            mutation,
            round_id=round_id,
            generation=source_node.generation,
            source=source_node.node_id,
        )
        result_payload = _build_result_payload(
            handoff_ref=resolved_handoff_ref,
            proposal_ref=proposal_ref,
            envelope=envelope.to_dict(),
        )
    except Exception as exc:
        result_payload = _build_rejected_result_payload(
            handoff_ref=resolved_handoff_ref,
            proposal_ref=proposal_ref or handoff_ref,
            error=exc,
        )

    assert result_ref is not None
    result_ref.parent.mkdir(parents=True, exist_ok=True)
    result_ref.write_text(json.dumps(result_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result_payload, indent=2, sort_keys=True))
    return 0 if result_payload["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
