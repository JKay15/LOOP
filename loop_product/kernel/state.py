"""Kernel state helpers."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from loop_product.artifact_hygiene import canonicalize_workspace_artifact_heavy_trees
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
_NODE_STATUS_VALUES = {item.value for item in NodeStatus}
_TERMINAL_DEPENDENCY_NODE_STATUSES = {
    NodeStatus.BLOCKED.value,
    NodeStatus.COMPLETED.value,
    NodeStatus.FAILED.value,
}
_NON_READY_TERMINAL_OUTCOMES = {
    "BLOCKED",
    "REPAIR_REQUIRED",
    "SPLIT_ACCEPTED_AWAITING_CHILDREN",
}


def _default_complexity_budget() -> dict[str, Any]:
    from loop_product.topology.budget import default_complexity_budget

    return default_complexity_budget()


def _normalized_complexity_budget(raw: dict[str, Any] | None = None) -> dict[str, Any]:
    from loop_product.topology.budget import normalized_complexity_budget

    return normalized_complexity_budget(raw)


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
    complexity_budget: dict[str, Any] = field(default_factory=_default_complexity_budget)
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
            complexity_budget=_normalized_complexity_budget(dict(data.get("complexity_budget") or {})),
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


def _authoritative_result_ref_for_node(*, state_root: Path, node_id: str, node_payload: dict[str, Any]) -> Path:
    sink = str(node_payload.get("result_sink_ref") or "").strip()
    if not sink:
        sink = f"artifacts/{node_id}/result.json"
    return (state_root / sink).resolve()


def _accepted_deferred_split(payload: dict[str, Any]) -> bool:
    split = dict(payload.get("split") or payload.get("split_report") or {})
    deferred = dict(split.get("deferred_request") or {})
    return bool(deferred.get("accepted"))


def _normalized_status_from_authoritative_result(
    *,
    current_status: str,
    payload: dict[str, Any],
) -> str:
    outcome = str(payload.get("outcome") or "").strip().upper()
    raw_status = str(payload.get("status") or "").strip().upper()
    if outcome == "SPLIT_ACCEPTED_AWAITING_CHILDREN":
        return NodeStatus.BLOCKED.value
    if _accepted_deferred_split(payload):
        return NodeStatus.COMPLETED.value
    if raw_status in _NODE_STATUS_VALUES:
        return raw_status
    return current_status


def _synchronized_runtime_state(
    *,
    existing: dict[str, Any],
    desired_status: str,
    result_ref: Path,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    runtime_state = normalize_runtime_state(existing)
    if desired_status not in {NodeStatus.BLOCKED.value, NodeStatus.COMPLETED.value, NodeStatus.FAILED.value}:
        return runtime_state
    evidence_refs = [str(result_ref.resolve())]
    for item in list(runtime_state.get("evidence_refs") or []):
        item_text = str(item or "").strip()
        if item_text and item_text not in evidence_refs:
            evidence_refs.append(item_text)
    return normalize_runtime_state(
        {
            **runtime_state,
            "attachment_state": RuntimeAttachmentState.TERMINAL.value,
            "summary": str(result_payload.get("summary") or runtime_state.get("summary") or ""),
            "observation_kind": "authoritative_result",
            "evidence_refs": evidence_refs,
        }
    )


def _dependency_snapshot_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = payload.get("dependency_status")
    if isinstance(raw, dict):
        normalized: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            node_id = str(key or "").strip()
            if not node_id:
                continue
            normalized[node_id] = dict(value or {}) if isinstance(value, dict) else {"state_status": str(value or "")}
        return normalized
    if isinstance(raw, list):
        normalized = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            node_id = str(item.get("node_id") or item.get("dependency_node_id") or "").strip()
            if not node_id:
                continue
            normalized[node_id] = dict(item)
        return normalized
    return {}


def _dependency_snapshot_ready(snapshot: dict[str, Any]) -> bool:
    state_status = str(snapshot.get("state_status") or snapshot.get("status") or "").strip().upper()
    outcome = str(snapshot.get("outcome") or "").strip().upper()
    result_sink_present = snapshot.get("result_sink_present")
    if result_sink_present is False:
        return False
    if state_status not in _TERMINAL_DEPENDENCY_NODE_STATUSES:
        return False
    if state_status == NodeStatus.BLOCKED.value:
        return bool(outcome) and outcome not in _NON_READY_TERMINAL_OUTCOMES
    return outcome not in _NON_READY_TERMINAL_OUTCOMES


def authoritative_result_payload_dependency_ready(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or "").strip().upper()
    outcome = str(payload.get("outcome") or "").strip().upper()
    if _accepted_deferred_split(payload):
        return True
    if status not in _TERMINAL_DEPENDENCY_NODE_STATUSES:
        return False
    if status == NodeStatus.BLOCKED.value:
        return bool(outcome) and outcome not in _NON_READY_TERMINAL_OUTCOMES
    return outcome not in _NON_READY_TERMINAL_OUTCOMES


def authoritative_node_dependency_ready(*, state_root: Path, node_payload: dict[str, Any]) -> bool:
    status = str(node_payload.get("status") or "").strip().upper()
    if status not in _TERMINAL_DEPENDENCY_NODE_STATUSES:
        return False
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    if str(runtime_state.get("attachment_state") or "") != RuntimeAttachmentState.TERMINAL.value:
        return False
    result_ref = _authoritative_result_ref_for_node(
        state_root=state_root,
        node_id=str(node_payload.get("node_id") or ""),
        node_payload=dict(node_payload or {}),
    )
    if not result_ref.exists():
        return status in {NodeStatus.COMPLETED.value, NodeStatus.FAILED.value}
    try:
        result_payload = json.loads(result_ref.read_text(encoding="utf-8"))
    except Exception:
        return False
    return authoritative_result_payload_dependency_ready(dict(result_payload or {}))


def _workspace_artifact_hygiene_sync(*, kernel_state: KernelState) -> None:
    for node_payload in kernel_state.nodes.values():
        workspace_root = str(node_payload.get("workspace_root") or "").strip()
        if not workspace_root:
            continue
        artifact_workspace = Path(workspace_root).expanduser().resolve()
        if not artifact_workspace.exists() or not artifact_workspace.is_dir():
            continue
        canonicalize_workspace_artifact_heavy_trees(artifact_workspace)


def _dependency_unblocked_resume_candidates(*, state_root: Path, kernel_state: KernelState) -> list[str]:
    candidates: list[str] = []
    for node_id, node_payload in list(kernel_state.nodes.items()):
        if str(node_payload.get("status") or "").strip().upper() != NodeStatus.BLOCKED.value:
            continue
        allowed_actions = {str(item or "").strip() for item in list(node_payload.get("allowed_actions") or [])}
        if "resume_request" not in allowed_actions:
            continue
        depends_on = [str(item or "").strip() for item in list(node_payload.get("depends_on_node_ids") or []) if str(item or "").strip()]
        if not depends_on:
            continue
        runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
        if str(runtime_state.get("attachment_state") or "") not in {
            RuntimeAttachmentState.TERMINAL.value,
            RuntimeAttachmentState.LOST.value,
        }:
            continue
        result_ref = _authoritative_result_ref_for_node(
            state_root=state_root,
            node_id=node_id,
            node_payload=dict(node_payload or {}),
        )
        if not result_ref.exists():
            continue
        try:
            result_payload = json.loads(result_ref.read_text(encoding="utf-8"))
        except Exception:
            continue
        snapshot_map = _dependency_snapshot_map(dict(result_payload or {}))
        if not snapshot_map:
            continue
        any_snapshot_unready = False
        all_dependencies_ready_now = True
        for dependency_node_id in depends_on:
            snapshot = dict(snapshot_map.get(dependency_node_id) or {})
            if not _dependency_snapshot_ready(snapshot):
                any_snapshot_unready = True
            dependency_payload = dict(kernel_state.nodes.get(dependency_node_id) or {})
            if not dependency_payload or not authoritative_node_dependency_ready(
                state_root=state_root,
                node_payload=dependency_payload,
            ):
                all_dependencies_ready_now = False
                break
        if any_snapshot_unready and all_dependencies_ready_now:
            candidates.append(node_id)
    return candidates


def synchronize_authoritative_node_results(
    state_root: Path,
    *,
    continue_deferred: bool = True,
    authority: KernelMutationAuthority | None = None,
) -> KernelState:
    """Normalize durable node state from authoritative child result sinks."""

    require_kernel_authority(authority, surface="synchronize_authoritative_node_results")
    state_root = require_runtime_root(state_root)
    kernel_state = load_kernel_state(state_root)
    _workspace_artifact_hygiene_sync(kernel_state=kernel_state)
    dirty = False

    for node_id, node_payload in list(kernel_state.nodes.items()):
        result_ref = _authoritative_result_ref_for_node(
            state_root=state_root,
            node_id=node_id,
            node_payload=dict(node_payload or {}),
        )
        if not result_ref.exists():
            continue
        try:
            result_payload = json.loads(result_ref.read_text(encoding="utf-8"))
        except Exception:
            continue
        desired_status = _normalized_status_from_authoritative_result(
            current_status=str(node_payload.get("status") or ""),
            payload=result_payload,
        )
        if desired_status != str(node_payload.get("status") or ""):
            node_payload["status"] = desired_status
            dirty = True
        normalized_runtime_state = _synchronized_runtime_state(
            existing=dict(node_payload.get("runtime_state") or {}),
            desired_status=str(node_payload.get("status") or ""),
            result_ref=result_ref,
            result_payload=result_payload,
        )
        if normalized_runtime_state != normalize_runtime_state(dict(node_payload.get("runtime_state") or {})):
            node_payload["runtime_state"] = normalized_runtime_state
            dirty = True
        summary = str(result_payload.get("summary") or "").strip()
        if str(node_payload.get("status") or "") == NodeStatus.BLOCKED.value and summary:
            if kernel_state.blocked_reasons.get(node_id) != summary:
                kernel_state.blocked_reasons[node_id] = summary
                dirty = True
        elif node_id in kernel_state.blocked_reasons and str(node_payload.get("status") or "") != NodeStatus.BLOCKED.value:
            kernel_state.blocked_reasons.pop(node_id, None)
            dirty = True
    if dirty:
        for node in kernel_state.nodes.values():
            persist_node_snapshot(state_root, node, authority=authority)
        persist_kernel_state(state_root, kernel_state, authority=authority)

    if continue_deferred:
        from loop_product.kernel.submit import submit_topology_mutation
        from loop_product.topology.activate import build_activate_request, review_activate_request

        refreshed = load_kernel_state(state_root)
        while True:
            ready_planned: list[tuple[str, str, int]] = []
            for child_id, child_payload in list(refreshed.nodes.items()):
                if str(child_payload.get("status") or "") != NodeStatus.PLANNED.value:
                    continue
                mutation = build_activate_request(
                    child_id,
                    reason=f"authoritative dependency snapshot for {child_id} is now terminal-ready; continue the planned child",
                )
                review = review_activate_request(refreshed, mutation, state_root=state_root)
                if str(review.get("decision") or "").upper() != "ACCEPT":
                    continue
                ready_planned.append(
                    (
                        child_id,
                        str(child_payload.get("round_id") or refreshed.root_node_id),
                        int(child_payload.get("generation") or 0),
                    )
                )
            if not ready_planned:
                break
            for child_id, round_id, generation in ready_planned:
                submit_topology_mutation(
                    state_root,
                    build_activate_request(
                        child_id,
                        reason=f"authoritative dependency snapshot for {child_id} is now terminal-ready; continue the planned child",
                    ),
                    round_id=round_id,
                    generation=generation,
                )
                refreshed = load_kernel_state(state_root)
        kernel_state = refreshed

    if continue_deferred:
        from loop_product.kernel.submit import submit_topology_mutation
        from loop_product.runtime.recover import build_resume_request

        refreshed = load_kernel_state(state_root)
        for node_id in _dependency_unblocked_resume_candidates(state_root=state_root, kernel_state=refreshed):
            node_payload = dict(refreshed.nodes.get(node_id) or {})
            try:
                source_node = NodeSpec.from_dict(node_payload)
            except Exception:
                continue
            mutation = build_resume_request(
                source_node,
                reason="authoritative dependency snapshot is stale and declared dependencies are now durably ready",
                consistency_signal="dependency_results_now_ready_after_blocked_snapshot",
                payload={
                    "dependency_unblocked": True,
                    "self_attribution": "kernel detected that the authoritative blocked dependency snapshot is stale",
                    "self_repair": "resume the same logical node with an exact relaunch now that every declared dependency is durably ready",
                },
            )
            submit_topology_mutation(
                state_root,
                mutation,
                round_id=str(node_payload.get("round_id") or refreshed.root_node_id),
                generation=int(node_payload.get("generation") or 0),
            )
            refreshed = load_kernel_state(state_root)
        kernel_state = refreshed

    return kernel_state


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
    from loop_product.kernel.authority import kernel_internal_authority

    return synchronize_authoritative_node_results(
        state_root,
        continue_deferred=True,
        authority=kernel_internal_authority(),
    ).to_dict()
