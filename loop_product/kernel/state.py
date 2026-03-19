"""Kernel state helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from loop_product.gateway.classify import classify_envelope
from loop_product.kernel.authority import KernelMutationAuthority, require_kernel_authority
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.protocols.node import NodeSpec, NodeStatus, RuntimeAttachmentState, normalize_runtime_state
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_paths import require_runtime_root


NON_TERMINAL_NODE_STATUSES = {
    NodeStatus.PLANNED.value,
    NodeStatus.ACTIVE.value,
    NodeStatus.BLOCKED.value,
}

ACTIVE_NODE_STATUSES = {
    NodeStatus.ACTIVE.value,
    NodeStatus.BLOCKED.value,
}


@dataclass(slots=True)
class KernelState:
    """Authoritative state view for the wave-1 kernel."""

    task_id: str
    root_goal: str
    root_node_id: str
    status: str = "ACTIVE"
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    delegation_map: dict[str, str] = field(default_factory=dict)
    blocked_reasons: dict[str, str] = field(default_factory=dict)
    complexity_budget: dict[str, Any] = field(default_factory=lambda: {"max_active_nodes": 4})
    accepted_envelopes: list[dict[str, Any]] = field(default_factory=list)
    active_evaluator_lanes: list[dict[str, Any]] = field(default_factory=list)
    active_requirements: list[str] = field(default_factory=list)
    experience_assets: list[dict[str, Any]] = field(default_factory=list)

    def register_node(self, node: NodeSpec) -> None:
        self.nodes[node.node_id] = node.to_dict()
        if node.parent_node_id:
            self.delegation_map[node.node_id] = node.parent_node_id
        self._register_requirement(node.goal_slice)

    def unregister_node(self, node_id: str) -> dict[str, Any] | None:
        node = self.nodes.pop(node_id, None)
        if node is None:
            return None
        self.delegation_map.pop(node_id, None)
        self.blocked_reasons.pop(node_id, None)
        self.active_evaluator_lanes = [item for item in self.active_evaluator_lanes if item.get("node_id") != node_id]
        goal_slice = str(node.get("goal_slice") or "")
        if goal_slice and not any(str(other.get("goal_slice") or "") == goal_slice for other in self.nodes.values()):
            self.active_requirements = [item for item in self.active_requirements if item != goal_slice]
        return node

    def update_node_status(self, node_id: str, status: NodeStatus) -> None:
        node = self.nodes[node_id]
        node["status"] = status.value

    def apply_accepted_envelope(self, envelope: ControlEnvelope) -> None:
        record = envelope.to_dict()
        self.accepted_envelopes.append(record)
        classification = record["classification"] or classify_envelope(envelope)
        payload = record.get("payload") or {}

        if classification == "dispatch":
            node_id = str(payload.get("node_id") or envelope.source)
            status_value = str(payload.get("node_status") or payload.get("status") or "")
            if node_id in self.nodes and status_value:
                self.nodes[node_id]["status"] = status_value
            if node_id in self.nodes:
                dispatch_state = str(payload.get("dispatch_state") or "").strip().upper()
                attachment_state = RuntimeAttachmentState.ATTACHED
                if dispatch_state == "COMPLETED":
                    attachment_state = RuntimeAttachmentState.TERMINAL
                self._update_runtime_state(
                    node_id=node_id,
                    attachment_state=attachment_state,
                    observed_at=str(record.get("accepted_at") or ""),
                    observation_kind=f"dispatch:{dispatch_state.lower() or 'heartbeat'}",
                    summary=str(payload.get("summary") or envelope.note or ""),
                    evidence_refs=[f"control_envelope:{record.get('envelope_id') or ''}"],
                )
            self._record_self_diagnostic(
                node_id=node_id,
                round_id=envelope.round_id,
                lane="dispatch",
                payload=payload,
                summary=str(payload.get("summary") or envelope.note or ""),
            )
            if status_value == NodeStatus.BLOCKED.value and envelope.note:
                self.blocked_reasons[node_id] = envelope.note
        elif classification == "local_control":
            node_id = str(payload.get("node_id") or envelope.source)
            action = str(payload.get("action") or "")
            summary = str(payload.get("summary") or envelope.note or "")
            if action in {"ESCALATE_STUCK", "FAIL_CLOSED"} and summary:
                self.blocked_reasons[node_id] = summary
            self._record_self_diagnostic(
                node_id=node_id,
                round_id=envelope.round_id,
                lane="local_control",
                payload=payload,
                summary=summary,
            )
        elif classification == "evaluator":
            lane_record = {
                "node_id": envelope.source,
                "lane": str(payload.get("lane") or "unknown"),
                "verdict": str(payload.get("verdict") or "UNKNOWN"),
                "round_id": envelope.round_id,
                "summary": str(payload.get("summary") or ""),
            }
            self.active_evaluator_lanes.append(lane_record)
            diagnostics = dict(payload.get("diagnostics") or {})
            if diagnostics.get("self_attribution") or diagnostics.get("self_repair"):
                self.experience_assets.append(
                    {
                        "node_id": envelope.source,
                        "round_id": envelope.round_id,
                        "lane": lane_record["lane"],
                        "self_attribution": str(diagnostics.get("self_attribution") or ""),
                        "self_repair": str(diagnostics.get("self_repair") or ""),
                        "summary": lane_record["summary"],
                    }
                )
            if payload.get("verdict") == "STUCK":
                self.blocked_reasons[envelope.source] = lane_record["summary"] or "evaluator reported STUCK"
        elif classification == "terminal":
            node_id = str(payload.get("node_id") or envelope.source)
            node_status = str(payload.get("node_status") or "")
            if node_id in self.nodes and node_status:
                self.nodes[node_id]["status"] = node_status
            if node_id in self.nodes:
                self._update_runtime_state(
                    node_id=node_id,
                    attachment_state=RuntimeAttachmentState.TERMINAL,
                    observed_at=str(record.get("accepted_at") or ""),
                    observation_kind="terminal_result",
                    summary=str(payload.get("summary") or envelope.note or ""),
                    evidence_refs=[f"control_envelope:{record.get('envelope_id') or ''}"],
                )
            summary = str(payload.get("summary") or envelope.note or "")
            if node_status == NodeStatus.BLOCKED.value and summary:
                self.blocked_reasons[node_id] = summary
            self._record_self_diagnostic(
                node_id=node_id,
                round_id=envelope.round_id,
                lane="terminal",
                payload=payload,
                summary=summary,
            )
        elif classification == "topology":
            mutation = dict(payload.get("topology_mutation") or payload)
            review = dict(payload.get("review") or {})
            source_node_id = str(mutation.get("source_node_id") or envelope.source)
            summary = str(review.get("summary") or mutation.get("reason") or envelope.note or "")
            self._record_self_diagnostic(
                node_id=source_node_id,
                round_id=envelope.round_id,
                lane="topology",
                payload={
                    "self_attribution": str(review.get("self_attribution") or ""),
                    "self_repair": str(review.get("self_repair") or ""),
                },
                summary=summary,
            )

    def _register_requirement(self, requirement: str) -> None:
        if requirement and requirement not in self.active_requirements:
            self.active_requirements.append(requirement)

    def _record_self_diagnostic(
        self,
        *,
        node_id: str,
        round_id: str,
        lane: str,
        payload: dict[str, Any],
        summary: str,
    ) -> None:
        self_attribution = str(payload.get("self_attribution") or "").strip()
        self_repair = str(payload.get("self_repair") or "").strip()
        if not (self_attribution or self_repair):
            return
        self.experience_assets.append(
            {
                "node_id": node_id,
                "round_id": round_id,
                "lane": lane,
                "self_attribution": self_attribution,
                "self_repair": self_repair,
                "summary": summary,
            }
        )

    def _update_runtime_state(
        self,
        *,
        node_id: str,
        attachment_state: RuntimeAttachmentState,
        observed_at: str,
        observation_kind: str,
        summary: str,
        evidence_refs: list[str] | tuple[str, ...] = (),
    ) -> None:
        node = self.nodes.get(node_id)
        if node is None:
            return
        node["runtime_state"] = normalize_runtime_state(
            {
                "attachment_state": attachment_state.value,
                "observed_at": observed_at,
                "summary": summary,
                "observation_kind": observation_kind,
                "evidence_refs": [str(item) for item in evidence_refs],
            }
        )

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["active_node_ids"] = [
            node_id
            for node_id, node in self.nodes.items()
            if str(node.get("status")) in ACTIVE_NODE_STATUSES
        ]
        data["accepted_envelope_count"] = len(self.accepted_envelopes)
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "KernelState":
        return cls(
            task_id=str(data["task_id"]),
            root_goal=str(data["root_goal"]),
            root_node_id=str(data["root_node_id"]),
            status=str(data.get("status") or "ACTIVE"),
            nodes=dict(data.get("nodes") or {}),
            delegation_map=dict(data.get("delegation_map") or {}),
            blocked_reasons=dict(data.get("blocked_reasons") or {}),
            complexity_budget=dict(data.get("complexity_budget") or {"max_active_nodes": 4}),
            accepted_envelopes=list(data.get("accepted_envelopes") or []),
            active_evaluator_lanes=list(data.get("active_evaluator_lanes") or []),
            active_requirements=list(data.get("active_requirements") or []),
            experience_assets=list(data.get("experience_assets") or []),
        )


def ensure_runtime_tree(state_root: Path) -> None:
    """Create the repo-local `.loop` tree."""

    state_root = require_runtime_root(state_root)
    for rel in ("state", "cache", "audit", "artifacts", "quarantine"):
        (state_root / rel).mkdir(parents=True, exist_ok=True)


def persist_kernel_state(
    state_root: Path,
    kernel_state: KernelState,
    *,
    authority: KernelMutationAuthority | None = None,
) -> Path:
    """Write kernel state into the authority tree."""

    require_kernel_authority(authority, surface="persist_kernel_state")
    state_root = require_runtime_root(state_root)
    state_dir = state_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    data = kernel_state.to_dict()
    validate_repo_object("LoopSystemKernelState.schema.json", data)
    path = state_dir / "kernel_state.json"
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (state_dir / "accepted_envelopes.json").write_text(
        json.dumps(kernel_state.accepted_envelopes, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def persist_node_snapshot(
    state_root: Path,
    node: NodeSpec | dict[str, Any],
    *,
    authority: KernelMutationAuthority | None = None,
) -> Path:
    """Persist one node snapshot under the runtime state tree."""

    require_kernel_authority(authority, surface="persist_node_snapshot")
    state_root = require_runtime_root(state_root)
    record = node.to_dict() if isinstance(node, NodeSpec) else dict(node)
    node_id = str(record["node_id"])
    path = state_root / "state" / f"{node_id}.json"
    path.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_kernel_state(state_root: Path) -> KernelState:
    """Load authoritative kernel state from the runtime tree."""

    state_root = require_runtime_root(state_root)
    path = state_root / "state" / "kernel_state.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    validate_repo_object("LoopSystemKernelState.schema.json", data)
    return KernelState.from_dict(data)


def query_kernel_state(state_root: Path) -> dict[str, Any]:
    """Return a read-only authoritative state snapshot for kernel queries."""

    state_root = require_runtime_root(state_root)
    return load_kernel_state(state_root).to_dict()
