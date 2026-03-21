#!/usr/bin/env python3
"""Submit a kernel-reviewed activate request from frozen handoff context."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from loop_product.kernel.state import load_kernel_state
from loop_product.kernel.submit import submit_topology_mutation
from loop_product.protocols.node import NodeSpec
from loop_product.topology.activate import build_activate_request


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit an activate request from frozen handoff context.")
    parser.add_argument("--handoff-ref", required=True)
    parser.add_argument("--proposal-ref", required=True)
    parser.add_argument("--result-ref", required=True)
    args = parser.parse_args()

    handoff_ref = Path(args.handoff_ref).expanduser().resolve()
    proposal_ref = Path(args.proposal_ref).expanduser().resolve()
    result_ref = Path(args.result_ref).expanduser().resolve()

    handoff = _load_json(handoff_ref)
    proposal = _load_json(proposal_ref)

    state_root = _resolve_state_root(handoff)
    current_node_id = str(handoff.get("node_id") or "").strip()
    round_id = str(handoff.get("round_id") or "").strip()
    if not current_node_id or not round_id:
        raise ValueError("handoff must include non-empty node_id and round_id")

    target_node_id = str(proposal.get("target_node_id") or "").strip()
    if not target_node_id:
        raise ValueError("proposal.target_node_id must be non-empty")

    kernel_state = load_kernel_state(state_root)
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
        handoff_ref=handoff_ref,
        proposal_ref=proposal_ref,
        envelope=envelope.to_dict(),
    )
    result_ref.parent.mkdir(parents=True, exist_ok=True)
    result_ref.write_text(json.dumps(result_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result_payload, indent=2, sort_keys=True))
    return 0 if result_payload["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
