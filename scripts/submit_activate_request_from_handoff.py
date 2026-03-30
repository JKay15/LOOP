#!/usr/bin/env python3
"""Submit a kernel-reviewed activate request from frozen handoff context."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any

from loop_product.control_intent import default_activate_request_refs
from loop_product.kernel.state import query_kernel_state_object
from loop_product.kernel.submit import submit_topology_mutation
from loop_product.protocols.node import NodeSpec
from loop_product.runtime_paths import node_machine_handoff_ref
from loop_product.topology.activate import build_activate_request


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
        "to derive deterministic activate proposal/result refs"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit an activate request from frozen handoff context.")
    parser.add_argument("--handoff-ref", required=True)
    parser.add_argument("--proposal-ref")
    parser.add_argument("--result-ref")
    args = parser.parse_args()

    handoff_ref = Path(args.handoff_ref).expanduser().resolve()
    proposal_ref: Path | None = Path(args.proposal_ref).expanduser().resolve() if args.proposal_ref else None
    result_ref: Path | None = Path(args.result_ref).expanduser().resolve() if args.result_ref else None

    resolved_handoff_ref = _resolve_handoff_json_ref(handoff_ref)
    handoff = _load_json(resolved_handoff_ref)
    if proposal_ref is None or result_ref is None:
        default_proposal_ref, default_result_ref = default_activate_request_refs(
            _resolve_live_artifact_root(handoff),
            workspace_mirror_relpath=str(handoff.get("workspace_mirror_relpath") or ""),
        )
        proposal_ref = proposal_ref or default_proposal_ref
        result_ref = result_ref or default_result_ref
    proposal = _load_json(proposal_ref)

    state_root = _resolve_state_root(handoff)
    current_node_id = str(handoff.get("node_id") or "").strip()
    round_id = str(handoff.get("round_id") or "").strip()
    if not current_node_id or not round_id:
        raise ValueError("handoff must include non-empty node_id and round_id")

    target_node_id = str(proposal.get("target_node_id") or "").strip()
    if not target_node_id:
        raise ValueError("proposal.target_node_id must be non-empty")

    kernel_state = query_kernel_state_object(state_root, continue_deferred=False)
    current_node_raw = dict(kernel_state.nodes.get(current_node_id) or {})
    if not current_node_raw:
        raise ValueError(f"source node not found in kernel state: {current_node_id}")
    target_node_raw = dict(kernel_state.nodes.get(target_node_id) or {})
    if not target_node_raw:
        raise ValueError(f"target planned node not found in kernel state: {target_node_id}")
    current_node = NodeSpec.from_dict(current_node_raw)

    mutation = build_activate_request(
        target_node_id,
        reason=str(proposal.get("reason") or ""),
        payload=dict(proposal.get("payload") or {}),
    )
    envelope = submit_topology_mutation(
        state_root,
        mutation,
        round_id=round_id,
        generation=current_node.generation,
        source=current_node.node_id,
    )
    result_payload = _build_result_payload(
        handoff_ref=resolved_handoff_ref,
        proposal_ref=proposal_ref,
        envelope=envelope.to_dict(),
    )
    assert result_ref is not None
    result_ref.parent.mkdir(parents=True, exist_ok=True)
    result_ref.write_text(json.dumps(result_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result_payload, indent=2, sort_keys=True))
    return 0 if result_payload["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
