"""Kernel state helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from loop_product.artifact_hygiene import canonicalize_workspace_artifact_heavy_trees
from loop_product.evaluator_authority import (
    authoritative_result_conflicts_with_inflight_evaluator,
    authoritative_result_has_evaluator_evidence,
    authoritative_result_retryable_nonterminal,
    evaluator_backed_terminal_closure,
)
from loop_product.gateway.classify import classify_envelope
from loop_product.kernel.authority import KernelMutationAuthority, require_kernel_authority
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.protocols.node import NodeSpec, NodeStatus, RuntimeAttachmentState, normalize_runtime_state
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_identity import ensure_runtime_root_identity
from loop_product.state_io import is_user_writable, write_json_read_only
from loop_product.runtime_paths import require_runtime_root
from loop_product.runtime_paths import node_machine_handoff_ref


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
_KERNEL_PROJECTION_NAME = "kernel_state"
_NODE_PROJECTION_NAME = "node_snapshot"
_PROJECTION_MANIFEST_NAME = "projection_manifest"
_AMBIENT_REPO_SERVICE_RUNTIME_NAMES = {
    "repo_control_plane",
    "repo_reactor",
    "repo_reactor_residency_guard",
    "housekeeping",
    "host_child_launch_supervisor",
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
    ensure_runtime_root_identity(state_root)
    anchor_root: Path | None = None
    parts = state_root.resolve().parts
    for idx, part in enumerate(parts):
        if part != ".loop":
            continue
        anchor_root = Path(*parts[: idx + 1]).resolve()
        break
    if (
        anchor_root is not None
        and anchor_root != state_root
        and state_root.name not in _AMBIENT_REPO_SERVICE_RUNTIME_NAMES
    ):
        ensure_runtime_tree(anchor_root)
    if state_root.name == ".loop":
        from loop_product.runtime.control_plane import ensure_repo_control_plane_service_for_runtime_root

        ensure_repo_control_plane_service_for_runtime_root(
            state_root=state_root,
            trigger_kind="runtime_tree_init",
        )


def _projection_updated_at_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _latest_committed_projection_seq(state_root: Path) -> int:
    from loop_product.event_journal import latest_projection_committed_seq

    return int(latest_projection_committed_seq(state_root))


def _annotate_projection_payload(
    payload: dict[str, Any],
    *,
    projection_name: str,
    last_applied_seq: int,
    source_event_journal_ref: str | None = None,
    projection_bundle_ref: str | None = None,
) -> dict[str, Any]:
    annotated = dict(payload or {})
    annotated["projection_name"] = str(projection_name)
    annotated["last_applied_seq"] = int(last_applied_seq)
    annotated["projection_updated_at"] = _projection_updated_at_utc()
    if source_event_journal_ref is not None:
        annotated["source_event_journal_ref"] = str(source_event_journal_ref)
    if projection_bundle_ref is not None:
        annotated["projection_bundle_ref"] = str(projection_bundle_ref)
    return annotated


def _projection_path_requires_refresh(
    path: Path,
    *,
    projection_name: str,
    last_applied_seq: int,
    source_event_journal_ref: str | None = None,
    projection_bundle_ref: str | None = None,
) -> bool:
    if not path.exists():
        return True
    if is_user_writable(path):
        return True
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return True
    if str(payload.get("projection_name") or "") != projection_name:
        return True
    try:
        existing_seq = int(payload.get("last_applied_seq"))
    except Exception:
        return True
    if existing_seq != int(last_applied_seq):
        return True
    if source_event_journal_ref is not None and str(payload.get("source_event_journal_ref") or "") != str(source_event_journal_ref):
        return True
    if projection_bundle_ref is not None and str(payload.get("projection_bundle_ref") or "") != str(projection_bundle_ref):
        return True
    return False


def _projection_source_event_journal_ref(state_root: Path) -> str:
    from loop_product.event_journal import event_journal_db_ref

    return str(event_journal_db_ref(state_root))


def _projection_manifest_path(state_root: Path) -> Path:
    return require_runtime_root(state_root) / "state" / "ProjectionManifest.json"


def _projection_manifest_payload(
    *,
    state_root: Path,
    kernel_state: KernelState,
    last_applied_seq: int,
) -> dict[str, Any]:
    covered_node_ids = sorted(str(node_id) for node_id in kernel_state.nodes)
    return _annotate_projection_payload(
        {
            "kernel_projection_name": _KERNEL_PROJECTION_NAME,
            "node_projection_name": _NODE_PROJECTION_NAME,
            "covered_node_ids": covered_node_ids,
            "covered_node_count": len(covered_node_ids),
        },
        projection_name=_PROJECTION_MANIFEST_NAME,
        last_applied_seq=last_applied_seq,
        source_event_journal_ref=_projection_source_event_journal_ref(state_root),
        projection_bundle_ref=str(_projection_manifest_path(state_root).resolve()),
    )


def _projection_manifest_requires_refresh(
    *,
    state_root: Path,
    kernel_state: KernelState,
    last_applied_seq: int,
) -> bool:
    path = _projection_manifest_path(state_root)
    if _projection_path_requires_refresh(
        path,
        projection_name=_PROJECTION_MANIFEST_NAME,
        last_applied_seq=last_applied_seq,
        source_event_journal_ref=_projection_source_event_journal_ref(state_root),
        projection_bundle_ref=str(path.resolve()),
    ):
        return True
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return True
    expected = _projection_manifest_payload(
        state_root=state_root,
        kernel_state=kernel_state,
        last_applied_seq=last_applied_seq,
    )
    if list(payload.get("covered_node_ids") or []) != list(expected.get("covered_node_ids") or []):
        return True
    if int(payload.get("covered_node_count") or -1) != int(expected.get("covered_node_count") or 0):
        return True
    if str(payload.get("kernel_projection_name") or "") != _KERNEL_PROJECTION_NAME:
        return True
    if str(payload.get("node_projection_name") or "") != _NODE_PROJECTION_NAME:
        return True
    return False


def _projection_surfaces_need_refresh(
    *,
    state_root: Path,
    kernel_state: KernelState,
    last_applied_seq: int,
) -> bool:
    state_dir = state_root / "state"
    if _projection_path_requires_refresh(
        state_dir / "kernel_state.json",
        projection_name=_KERNEL_PROJECTION_NAME,
        last_applied_seq=last_applied_seq,
        source_event_journal_ref=_projection_source_event_journal_ref(state_root),
        projection_bundle_ref=str(_projection_manifest_path(state_root).resolve()),
    ):
        return True
    for node_id in kernel_state.nodes:
        if _projection_path_requires_refresh(
            state_dir / f"{node_id}.json",
            projection_name=_NODE_PROJECTION_NAME,
            last_applied_seq=last_applied_seq,
            source_event_journal_ref=_projection_source_event_journal_ref(state_root),
            projection_bundle_ref=str(_projection_manifest_path(state_root).resolve()),
        ):
            return True
    if _projection_manifest_requires_refresh(
        state_root=state_root,
        kernel_state=kernel_state,
        last_applied_seq=last_applied_seq,
    ):
        return True
    return False


def _projection_floor_from_path(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return 0
    try:
        return int(payload.get("last_applied_seq") or 0)
    except Exception:
        return 0


def _reconcile_kernel_projection_from_journal(
    *,
    state_root: Path,
    kernel_state: KernelState,
    applied_seq: int,
) -> tuple[KernelState, int, bool]:
    from loop_product.event_journal import event_affects_projection, iter_committed_events_after_seq

    replay_seq = int(applied_seq or 0)
    seen_envelope_ids = {
        str(record.get("envelope_id") or "").strip()
        for record in list(kernel_state.accepted_envelopes)
        if str(record.get("envelope_id") or "").strip()
    }
    dirty = False
    for event in iter_committed_events_after_seq(state_root, after_seq=replay_seq):
        if not event_affects_projection(event):
            continue
        event_seq = int(event.get("seq") or replay_seq)
        payload = dict(event.get("payload") or {})
        envelope = ControlEnvelope.from_dict(payload)
        envelope_id = str(envelope.envelope_id or envelope.with_identity().envelope_id or "").strip()
        if envelope_id and envelope_id in seen_envelope_ids:
            replay_seq = event_seq
            continue
        if str(envelope.classification or "").strip().lower() == "topology":
            break
        if envelope_id and envelope_id in seen_envelope_ids:
            continue
        kernel_state.apply_accepted_envelope(envelope)
        if envelope_id:
            seen_envelope_ids.add(envelope_id)
        replay_seq = event_seq
        dirty = True
    return kernel_state, replay_seq, dirty


def adopt_historical_projection_bundle(
    state_root: Path,
    *,
    authority: KernelMutationAuthority | None = None,
) -> dict[str, Any]:
    """Adopt missing committed accepted-envelope history into a preserved projection bundle."""

    require_kernel_authority(authority, surface="adopt_historical_projection_bundle")
    state_root = require_runtime_root(state_root)
    from loop_product.event_journal import event_affects_projection, iter_committed_events

    kernel_state = load_kernel_state(state_root)
    before_kernel_last_applied_seq = _projection_floor_from_path(state_root / "state" / "kernel_state.json")
    before_projection_manifest_last_applied_seq = _projection_floor_from_path(_projection_manifest_path(state_root))
    previous_node_floors = {
        node_id: _projection_floor_from_path(state_root / "state" / f"{node_id}.json")
        for node_id in kernel_state.nodes
    }
    seen_envelope_ids = {
        str(record.get("envelope_id") or "").strip()
        for record in list(kernel_state.accepted_envelopes)
        if str(record.get("envelope_id") or "").strip()
    }
    missing_payloads: list[dict[str, Any]] = []
    missing_envelope_ids: list[str] = []
    for event in iter_committed_events(state_root):
        if not event_affects_projection(event):
            continue
        if str(event.get("event_type") or "") != "control_envelope_accepted":
            continue
        payload = dict(event.get("payload") or {})
        envelope_id = str(payload.get("envelope_id") or "").strip()
        if envelope_id and envelope_id in seen_envelope_ids:
            continue
        missing_payloads.append(payload)
        if envelope_id:
            seen_envelope_ids.add(envelope_id)
            missing_envelope_ids.append(envelope_id)

    projection_seq = _latest_committed_projection_seq(state_root)
    if not missing_payloads:
        node_projection_fresh = all(
            int(previous_node_floors.get(node_id, 0)) == int(projection_seq)
            for node_id in kernel_state.nodes
        )
        projections_fresh = bool(
            int(before_kernel_last_applied_seq) == int(projection_seq)
            and int(before_projection_manifest_last_applied_seq) == int(projection_seq)
            and node_projection_fresh
            and not _projection_surfaces_need_refresh(
                state_root=state_root,
                kernel_state=kernel_state,
                last_applied_seq=int(projection_seq),
            )
        )
        if projections_fresh:
            return {
                "runtime_root": str(state_root),
                "projection_committed_seq": int(projection_seq),
                "missing_accepted_envelope_count_before": 0,
                "missing_accepted_envelope_count_after": 0,
                "adopted_envelope_count": 0,
                "adopted_envelope_ids_sample": [],
                "before_kernel_last_applied_seq": int(before_kernel_last_applied_seq),
                "kernel_last_applied_seq": int(before_kernel_last_applied_seq),
                "before_projection_manifest_last_applied_seq": int(before_projection_manifest_last_applied_seq),
                "projection_manifest_last_applied_seq": int(before_projection_manifest_last_applied_seq),
                "updated_node_ids": [],
            }

    if missing_payloads:
        kernel_state.accepted_envelopes.extend(missing_payloads)

    persist_kernel_state(state_root, kernel_state, authority=authority, projection_seq=projection_seq)
    for node_payload in kernel_state.nodes.values():
        persist_node_snapshot(
            state_root,
            node_payload,
            authority=authority,
            projection_seq=projection_seq,
        )
    persist_projection_manifest(
        state_root,
        kernel_state,
        authority=authority,
        projection_seq=projection_seq,
    )

    after_kernel_last_applied_seq = _projection_floor_from_path(state_root / "state" / "kernel_state.json")
    after_projection_manifest_last_applied_seq = _projection_floor_from_path(_projection_manifest_path(state_root))
    updated_node_ids = [
        str(node_id)
        for node_id in sorted(kernel_state.nodes)
        if _projection_floor_from_path(state_root / "state" / f"{node_id}.json")
        != int(previous_node_floors.get(node_id, 0))
    ]
    return {
        "runtime_root": str(state_root),
        "projection_committed_seq": int(projection_seq),
        "missing_accepted_envelope_count_before": len(missing_payloads),
        "missing_accepted_envelope_count_after": 0,
        "adopted_envelope_count": len(missing_payloads),
        "adopted_envelope_ids_sample": missing_envelope_ids[:8],
        "before_kernel_last_applied_seq": int(before_kernel_last_applied_seq),
        "kernel_last_applied_seq": int(after_kernel_last_applied_seq),
        "before_projection_manifest_last_applied_seq": int(before_projection_manifest_last_applied_seq),
        "projection_manifest_last_applied_seq": int(after_projection_manifest_last_applied_seq),
        "updated_node_ids": updated_node_ids,
    }


def _record_projection_rebuild_result(
    *,
    state_root: Path,
    summary: dict[str, Any],
    producer: str,
) -> dict[str, Any]:
    from loop_product.event_journal import commit_projection_rebuild_performed_event, latest_committed_seq

    recorded = dict(summary or {})
    recorded["journal_committed_seq"] = int(latest_committed_seq(state_root))
    event = commit_projection_rebuild_performed_event(
        state_root,
        receipt=recorded,
        producer=producer,
    )
    if event is not None:
        recorded["rebuild_result_event_id"] = str(event.get("event_id") or "")
        recorded["rebuild_result_event_seq"] = int(event.get("seq") or 0)
    return recorded


def _authoritative_result_ref_for_node(*, state_root: Path, node_id: str, node_payload: dict[str, Any]) -> Path:
    sink = str(node_payload.get("result_sink_ref") or "").strip()
    if not sink:
        sink = f"artifacts/{node_id}/result.json"
    return (state_root / sink).resolve()


def _load_authoritative_result_payload(result_ref: Path) -> dict[str, Any]:
    if not result_ref.exists():
        return {}
    try:
        payload = json.loads(result_ref.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _whole_paper_terminal_status_from_result_payload(payload: dict[str, Any]) -> dict[str, Any]:
    from loop_product.dispatch.publication import _load_whole_paper_terminal_status

    artifact_ref = str(payload.get("workspace_mirror_ref") or payload.get("delivered_artifact_ref") or "").strip()
    if not artifact_ref:
        return {}
    try:
        return dict(_load_whole_paper_terminal_status(Path(artifact_ref).expanduser().resolve()) or {})
    except Exception:
        return {}


def _preferred_whole_paper_root_result_source(
    *,
    state_root: Path,
    kernel_state: KernelState,
    node_id: str,
    node_payload: dict[str, Any],
    direct_result_ref: Path,
    direct_result_payload: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    is_kernel_root = node_id == str(kernel_state.root_node_id or "")
    is_whole_paper_direct_root = str(node_payload.get("parent_node_id") or "").strip() == "root-kernel"
    if not is_kernel_root and not is_whole_paper_direct_root:
        return direct_result_ref, direct_result_payload
    if str(node_payload.get("workflow_scope") or "").strip() != "whole_paper_formalization":
        return direct_result_ref, direct_result_payload
    if str(node_payload.get("terminal_authority_scope") or "").strip() != "whole_paper":
        return direct_result_ref, direct_result_payload
    if evaluator_backed_terminal_closure(direct_result_payload) and _whole_paper_terminal_status_from_result_payload(
        direct_result_payload
    ):
        return direct_result_ref, direct_result_payload

    best_ref = direct_result_ref
    best_payload = direct_result_payload
    best_key: tuple[int, str, str] | None = None
    for child_id, child_payload in list(kernel_state.nodes.items()):
        child = dict(child_payload or {})
        if str(child.get("parent_node_id") or "").strip() != node_id:
            continue
        if str(child.get("terminal_authority_scope") or "").strip() != "whole_paper":
            continue
        child_ref = _authoritative_result_ref_for_node(state_root=state_root, node_id=child_id, node_payload=child)
        child_result_payload = _load_authoritative_result_payload(child_ref)
        if not evaluator_backed_terminal_closure(child_result_payload):
            continue
        if not _whole_paper_terminal_status_from_result_payload(child_result_payload):
            continue
        child_key = (
            int(child.get("generation") or 0),
            str(child.get("round_id") or ""),
            str(child_id),
        )
        if best_key is None or child_key > best_key:
            best_key = child_key
            best_ref = child_ref
            best_payload = child_result_payload
    return best_ref, best_payload


def _existing_machine_handoff_ref(*, state_root: Path, node_id: str) -> Path | None:
    candidate = node_machine_handoff_ref(state_root=state_root, node_id=node_id)
    if candidate.exists():
        return candidate
    return None


def _accepted_deferred_split(payload: dict[str, Any]) -> bool:
    split = dict(payload.get("split") or payload.get("split_report") or {})
    deferred = dict(split.get("deferred_request") or {})
    return bool(deferred.get("accepted"))


def _split_continuation_outcome(payload: dict[str, Any]) -> str:
    return str(payload.get("outcome") or "").strip().upper()


def _has_evaluator_backed_terminal_closure(payload: dict[str, Any]) -> bool:
    return evaluator_backed_terminal_closure(payload)


def _normalized_status_from_evaluator_result(payload: dict[str, Any]) -> str | None:
    evaluator_result = dict(payload.get("evaluator_result") or {})
    verdict = str(evaluator_result.get("verdict") or "").strip().upper()
    if not verdict:
        return None
    retryable = bool(evaluator_result.get("retryable"))
    if retryable and verdict != "PASS":
        return NodeStatus.ACTIVE.value
    if verdict == "PASS":
        return NodeStatus.COMPLETED.value
    if verdict == "STUCK":
        return NodeStatus.BLOCKED.value
    return NodeStatus.FAILED.value


def authoritative_result_payload_split_release_ready(payload: dict[str, Any]) -> bool:
    outcome = _split_continuation_outcome(payload)
    if outcome == "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
        split_state = dict(payload.get("split_state") or payload.get("split_report") or {})
        if split_state:
            accepted = bool(split_state.get("accepted"))
            if accepted:
                return True
            if list(split_state.get("accepted_target_nodes") or []):
                return True
        return True
    return False


def _normalized_status_from_authoritative_result(
    *,
    current_status: str,
    node_kind: str,
    payload: dict[str, Any],
) -> str:
    outcome = _split_continuation_outcome(payload)
    raw_status = str(payload.get("status") or "").strip().upper()
    if outcome == "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
        return NodeStatus.BLOCKED.value
    if outcome == "SPLIT_ACCEPTED_AWAITING_CHILDREN":
        return NodeStatus.BLOCKED.value
    if _accepted_deferred_split(payload):
        return NodeStatus.COMPLETED.value
    if raw_status == NodeStatus.COMPLETED.value and str(node_kind or "").strip() == "implementer":
        if not authoritative_result_has_evaluator_evidence(payload):
            return current_status
        evaluator_status = _normalized_status_from_evaluator_result(payload)
        if evaluator_status:
            return evaluator_status
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
    evidence_refs = [str(result_ref.resolve())]
    for item in list(runtime_state.get("evidence_refs") or []):
        item_text = str(item or "").strip()
        if item_text and item_text not in evidence_refs:
            evidence_refs.append(item_text)
    if authoritative_result_retryable_nonterminal(result_payload):
        if desired_status == NodeStatus.BLOCKED.value:
            attachment_state = str(runtime_state.get("attachment_state") or "").strip() or RuntimeAttachmentState.LOST.value
            if attachment_state == RuntimeAttachmentState.TERMINAL.value:
                attachment_state = RuntimeAttachmentState.LOST.value
            observation_kind = str(runtime_state.get("observation_kind") or "").strip()
            if not observation_kind or observation_kind == "authoritative_result":
                observation_kind = "authoritative_retryable_result"
            return normalize_runtime_state(
                {
                    **runtime_state,
                    "attachment_state": attachment_state,
                    "summary": str(runtime_state.get("summary") or result_payload.get("summary") or ""),
                    "observation_kind": observation_kind,
                    "evidence_refs": evidence_refs,
                    "live_process_trusted": False if attachment_state != RuntimeAttachmentState.ATTACHED.value else bool(runtime_state.get("live_process_trusted")),
                    "pid_alive": bool(runtime_state.get("pid_alive")) if attachment_state == RuntimeAttachmentState.ATTACHED.value else False,
                }
            )
        if str(runtime_state.get("attachment_state") or "") == RuntimeAttachmentState.TERMINAL.value:
            return normalize_runtime_state(
                {
                    **runtime_state,
                    "attachment_state": RuntimeAttachmentState.UNOBSERVED.value,
                    "summary": str(result_payload.get("summary") or runtime_state.get("summary") or ""),
                    "observation_kind": "authoritative_retryable_result",
                    "evidence_refs": evidence_refs,
                }
            )
    if desired_status not in {NodeStatus.BLOCKED.value, NodeStatus.COMPLETED.value, NodeStatus.FAILED.value}:
        return runtime_state
    return normalize_runtime_state(
        {
            **runtime_state,
            "attachment_state": RuntimeAttachmentState.TERMINAL.value,
            "summary": str(result_payload.get("summary") or runtime_state.get("summary") or ""),
            "observation_kind": "authoritative_result",
            "evidence_refs": evidence_refs,
        }
    )


def _authoritative_result_sync_candidate(
    *,
    kernel_state: KernelState | None,
    state_root: Path,
    node_id: str,
    node_payload: dict[str, Any],
) -> dict[str, Any] | None:
    from loop_product.dispatch.publication import workspace_publication_ready_for_terminal_state

    resolved_kernel_state = kernel_state
    if resolved_kernel_state is None:
        try:
            resolved_kernel_state = load_kernel_state(state_root)
        except FileNotFoundError:
            resolved_kernel_state = KernelState(
                task_id="authoritative-result-bootstrap",
                root_goal="bootstrap authoritative result sync before kernel_state.json exists",
                root_node_id=str(node_id),
            )
    direct_result_ref = _authoritative_result_ref_for_node(
        state_root=state_root,
        node_id=node_id,
        node_payload=node_payload,
    )
    direct_result_payload = _load_authoritative_result_payload(direct_result_ref)
    result_ref, result_payload = _preferred_whole_paper_root_result_source(
        state_root=state_root,
        kernel_state=resolved_kernel_state,
        node_id=node_id,
        node_payload=node_payload,
        direct_result_ref=direct_result_ref,
        direct_result_payload=direct_result_payload,
    )
    if not result_payload:
        return None
    if authoritative_result_conflicts_with_inflight_evaluator(
        state_root=state_root,
        node_id=node_id,
        payload=result_payload,
    ):
        return None
    desired_status = _normalized_status_from_authoritative_result(
        current_status=str(node_payload.get("status") or ""),
        node_kind=str(node_payload.get("node_kind") or ""),
        payload=result_payload,
    )
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    supervision_observation_kind = str(runtime_state.get("observation_kind") or "").strip().lower()
    if (
        desired_status == NodeStatus.ACTIVE.value
        and supervision_observation_kind in {
            "supervision:no_substantive_progress",
            "supervision:retry_budget_exhausted",
        }
    ):
        desired_status = NodeStatus.BLOCKED.value
    if desired_status in {
        NodeStatus.BLOCKED.value,
        NodeStatus.COMPLETED.value,
        NodeStatus.FAILED.value,
    }:
        workspace_root = str(node_payload.get("workspace_root") or "").strip()
        if workspace_root:
            publication_gate = workspace_publication_ready_for_terminal_state(
                workspace_root=workspace_root,
                required_output_paths=_required_output_paths_for_node_payload(node_payload),
            )
            if not bool(publication_gate.get("ready")):
                return None
    return {
        "result_ref": result_ref,
        "result_payload": result_payload,
        "desired_status": desired_status,
    }


def _canonicalized_node_payload_from_authoritative_result(
    *,
    kernel_state: KernelState | None = None,
    state_root: Path,
    node_id: str,
    node_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    canonical = dict(node_payload or {})
    sync_candidate = _authoritative_result_sync_candidate(
        kernel_state=kernel_state,
        state_root=state_root,
        node_id=node_id,
        node_payload=canonical,
    )
    if sync_candidate is None:
        return canonical, None
    result_payload = dict(sync_candidate["result_payload"] or {})
    runtime_refs = dict(result_payload.get("runtime_refs") or {})
    evaluator = dict(result_payload.get("evaluator") or {})
    evaluator_result = dict(result_payload.get("evaluator_result") or {})
    desired_status = str(sync_candidate["desired_status"])
    canonical["status"] = desired_status
    canonical["runtime_state"] = _synchronized_runtime_state(
        existing=dict(canonical.get("runtime_state") or {}),
        desired_status=desired_status,
        result_ref=Path(sync_candidate["result_ref"]),
        result_payload=result_payload,
    )
    canonical["authoritative_result_ref"] = str(Path(sync_candidate["result_ref"]).resolve())
    canonical["authoritative_result_status"] = str(result_payload.get("status") or "")
    canonical["authoritative_result_outcome"] = str(result_payload.get("outcome") or "")
    canonical["authoritative_result_summary"] = str(result_payload.get("summary") or "")
    canonical["authoritative_evaluator_verdict"] = str(
        evaluator_result.get("verdict") or evaluator.get("verdict") or ""
    )
    canonical["authoritative_evaluation_report_ref"] = str(
        result_payload.get("evaluation_report_ref")
        or runtime_refs.get("evaluation_report_ref")
        or evaluator.get("evaluation_report_ref")
        or ""
    )
    canonical["authoritative_reviewer_response_ref"] = str(
        result_payload.get("reviewer_response_ref")
        or runtime_refs.get("reviewer_response_ref")
        or evaluator.get("reviewer_response_ref")
        or ""
    )
    canonical["authoritative_workspace_result_sink_ref"] = str(result_payload.get("workspace_result_sink_ref") or "")
    canonical["authoritative_workspace_mirror_ref"] = str(
        result_payload.get("workspace_mirror_ref") or result_payload.get("delivered_artifact_ref") or ""
    )
    return canonical, sync_candidate


def _sync_blocked_reason_for_node(
    *,
    kernel_state: KernelState,
    node_id: str,
    node_payload: dict[str, Any],
    sync_candidate: dict[str, Any] | None,
) -> bool:
    dirty = False
    summary = ""
    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    runtime_observation_kind = str(runtime_state.get("observation_kind") or "").strip().lower()
    if runtime_observation_kind in {
        "supervision:no_substantive_progress",
        "supervision:retry_budget_exhausted",
    }:
        summary = str(runtime_state.get("summary") or "").strip()
    if sync_candidate is not None:
        summary = summary or str(dict(sync_candidate.get("result_payload") or {}).get("summary") or "").strip()
    if str(node_payload.get("status") or "") == NodeStatus.BLOCKED.value and summary:
        if kernel_state.blocked_reasons.get(node_id) != summary:
            kernel_state.blocked_reasons[node_id] = summary
            dirty = True
    elif node_id in kernel_state.blocked_reasons and str(node_payload.get("status") or "") != NodeStatus.BLOCKED.value:
        kernel_state.blocked_reasons.pop(node_id, None)
        dirty = True
    return dirty


def _supervision_result_ref_for_launch_result(*, launch_result_ref: Path) -> Path:
    return launch_result_ref.expanduser().resolve().with_name("ChildSupervisionResult.json")


def _path_mtime_utc(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return ""


def _supervision_stop_point_summary(reason: str) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized == "no_substantive_progress":
        return "child supervision settled to no_substantive_progress after terminating a live runtime that stopped making substantive artifact progress"
    if normalized == "retry_budget_exhausted":
        return "child supervision settled to retry_budget_exhausted after bounded recovery attempts were exhausted"
    return f"child supervision settled to {normalized or 'unknown'}"


def _normalized_supervision_stop_point(
    *,
    supervision_result_ref: Path,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    if not bool(payload.get("settled")):
        return None
    reason = str(payload.get("settled_reason") or "").strip().lower()
    if reason not in {"no_substantive_progress", "retry_budget_exhausted"}:
        return None
    evidence_refs: list[str] = [str(supervision_result_ref.resolve())]
    for key in ("launch_result_ref", "latest_launch_result_ref", "status_result_ref", "implementer_result_ref"):
        ref = str(payload.get(key) or "").strip()
        if ref and ref not in evidence_refs:
            evidence_refs.append(ref)
    return {
        "desired_status": NodeStatus.BLOCKED.value,
        "attachment_state": RuntimeAttachmentState.LOST.value,
        "observed_at": _path_mtime_utc(supervision_result_ref),
        "observation_kind": f"supervision:{reason}",
        "summary": _supervision_stop_point_summary(reason),
        "evidence_refs": evidence_refs,
    }


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


def _required_output_paths_for_node_payload(node_payload: dict[str, Any]) -> list[str]:
    return [
        str(item).strip()
        for item in list(node_payload.get("required_output_paths") or [])
        if str(item).strip()
    ]


def authoritative_result_payload_dependency_ready(payload: dict[str, Any]) -> bool:
    status = str(payload.get("status") or "").strip().upper()
    outcome = _split_continuation_outcome(payload)
    if authoritative_result_payload_split_release_ready(payload):
        return True
    if _accepted_deferred_split(payload):
        return True
    if status not in _TERMINAL_DEPENDENCY_NODE_STATUSES:
        return False
    if status == NodeStatus.BLOCKED.value:
        return bool(outcome) and outcome not in _NON_READY_TERMINAL_OUTCOMES
    return outcome not in _NON_READY_TERMINAL_OUTCOMES


def authoritative_node_dependency_ready(*, state_root: Path, node_payload: dict[str, Any]) -> bool:
    from loop_product.dispatch.publication import workspace_publication_ready_for_terminal_state

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
    workspace_root = str(node_payload.get("workspace_root") or "").strip()
    if workspace_root:
        publication_gate = workspace_publication_ready_for_terminal_state(
            workspace_root=workspace_root,
            machine_handoff_ref=node_machine_handoff_ref(
                state_root=state_root,
                node_id=str(node_payload.get("node_id") or ""),
            ),
            required_output_paths=_required_output_paths_for_node_payload(node_payload),
        )
        if not bool(publication_gate.get("ready")):
            return False
    return authoritative_result_payload_dependency_ready(dict(result_payload or {}))


def authoritative_node_split_release_ready(*, state_root: Path, node_payload: dict[str, Any]) -> bool:
    from loop_product.dispatch.publication import workspace_publication_ready_for_terminal_state

    runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    if str(runtime_state.get("attachment_state") or "") != RuntimeAttachmentState.TERMINAL.value:
        return False
    result_ref = _authoritative_result_ref_for_node(
        state_root=state_root,
        node_id=str(node_payload.get("node_id") or ""),
        node_payload=dict(node_payload or {}),
    )
    if not result_ref.exists():
        return False
    try:
        result_payload = json.loads(result_ref.read_text(encoding="utf-8"))
    except Exception:
        return False
    workspace_root = str(node_payload.get("workspace_root") or "").strip()
    if workspace_root:
        publication_gate = workspace_publication_ready_for_terminal_state(
            workspace_root=workspace_root,
            machine_handoff_ref=node_machine_handoff_ref(
                state_root=state_root,
                node_id=str(node_payload.get("node_id") or ""),
            ),
            required_output_paths=_required_output_paths_for_node_payload(node_payload),
        )
        if not bool(publication_gate.get("ready")):
            return False
    return authoritative_result_payload_split_release_ready(dict(result_payload or {}))


def _workspace_artifact_hygiene_sync(*, state_root: Path, kernel_state: KernelState) -> None:
    from loop_product.dispatch.publication import inspect_workspace_publication_state

    for node_payload in kernel_state.nodes.values():
        workspace_root = str(node_payload.get("workspace_root") or "").strip()
        if not workspace_root:
            continue
        artifact_workspace = Path(workspace_root).expanduser().resolve()
        if not artifact_workspace.exists() or not artifact_workspace.is_dir():
            continue
        publication_state = inspect_workspace_publication_state(
            workspace_root=artifact_workspace,
            machine_handoff_ref=_existing_machine_handoff_ref(
                state_root=state_root,
                node_id=str(node_payload.get("node_id") or ""),
            ),
        )
        excluded_roots: list[Path] = []
        publish_ref = str(publication_state.get("publish_artifact_ref") or "").strip()
        if publish_ref:
            excluded_roots.append(Path(publish_ref).expanduser().resolve())
        live_ref = str(publication_state.get("live_artifact_ref") or "").strip()
        if live_ref:
            excluded_roots.append(Path(live_ref).expanduser().resolve())
        canonicalize_workspace_artifact_heavy_trees(artifact_workspace, exclude_roots=excluded_roots)


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


def synchronize_supervision_stop_point_from_launch_result_ref(
    state_root: Path,
    *,
    launch_result_ref: str | Path,
    authority: KernelMutationAuthority | None = None,
) -> KernelState:
    """Normalize durable node state from a settled child supervision stop point."""

    require_kernel_authority(authority, surface="synchronize_supervision_stop_point_from_launch_result_ref")
    state_root = require_runtime_root(state_root)
    launch_path = Path(launch_result_ref).expanduser().resolve()
    if not launch_path.exists():
        return load_kernel_state(state_root)
    try:
        launch_payload = json.loads(launch_path.read_text(encoding="utf-8"))
    except Exception:
        return load_kernel_state(state_root)
    node_id = str(launch_payload.get("node_id") or "").strip()
    if not node_id:
        return load_kernel_state(state_root)
    supervision_result_ref = _supervision_result_ref_for_launch_result(launch_result_ref=launch_path)
    if not supervision_result_ref.exists():
        return load_kernel_state(state_root)
    try:
        supervision_payload = json.loads(supervision_result_ref.read_text(encoding="utf-8"))
    except Exception:
        return load_kernel_state(state_root)
    normalized = _normalized_supervision_stop_point(
        supervision_result_ref=supervision_result_ref,
        payload=supervision_payload,
    )
    if normalized is None:
        return load_kernel_state(state_root)

    kernel_state = load_kernel_state(state_root)
    node_payload = kernel_state.nodes.get(node_id)
    if not isinstance(node_payload, dict):
        return kernel_state

    existing_runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
    authoritative_result_ref = _authoritative_result_ref_for_node(
        state_root=state_root,
        node_id=node_id,
        node_payload=node_payload,
    )
    authoritative_result_payload = _load_authoritative_result_payload(authoritative_result_ref)
    existing_observation_kind = str(existing_runtime_state.get("observation_kind") or "").strip().lower()
    if str(node_payload.get("status") or "") in {NodeStatus.COMPLETED.value, NodeStatus.FAILED.value}:
        return kernel_state
    if (
        str(existing_runtime_state.get("attachment_state") or "") == RuntimeAttachmentState.TERMINAL.value
        and existing_observation_kind in {"authoritative_result", "terminal_result", "terminal_node_state"}
        and authoritative_result_payload
        and not authoritative_result_retryable_nonterminal(authoritative_result_payload)
    ):
        return kernel_state

    dirty = False
    desired_status = str(normalized.get("desired_status") or NodeStatus.BLOCKED.value)
    if str(node_payload.get("status") or "") != desired_status:
        node_payload["status"] = desired_status
        dirty = True
    merged_evidence_refs = [str(item) for item in list(normalized.get("evidence_refs") or []) if str(item).strip()]
    for item in list(existing_runtime_state.get("evidence_refs") or []):
        text = str(item or "").strip()
        if text and text not in merged_evidence_refs:
            merged_evidence_refs.append(text)
    next_runtime_state = normalize_runtime_state(
        {
            **existing_runtime_state,
            "attachment_state": str(normalized.get("attachment_state") or RuntimeAttachmentState.LOST.value),
            "observed_at": str(normalized.get("observed_at") or existing_runtime_state.get("observed_at") or ""),
            "summary": str(normalized.get("summary") or existing_runtime_state.get("summary") or ""),
            "observation_kind": str(normalized.get("observation_kind") or existing_runtime_state.get("observation_kind") or ""),
            "evidence_refs": merged_evidence_refs,
            "live_process_trusted": False,
            "pid_alive": False,
        }
    )
    if next_runtime_state != existing_runtime_state:
        node_payload["runtime_state"] = next_runtime_state
        dirty = True
    summary = str(normalized.get("summary") or "").strip()
    if summary and kernel_state.blocked_reasons.get(node_id) != summary:
        kernel_state.blocked_reasons[node_id] = summary
        dirty = True

    if dirty:
        persist_node_snapshot(state_root, node_payload, authority=authority)
        persist_kernel_state(state_root, kernel_state, authority=authority)
        return load_kernel_state(state_root)
    return kernel_state


def _apply_effective_runtime_liveness_projection(
    *,
    state_root: Path,
    kernel_state: KernelState,
) -> bool:
    from loop_product.event_journal import iter_committed_events
    from loop_product.runtime.liveness import select_effective_runtime_liveness_head, summarize_runtime_liveness_observation

    observations_by_node: dict[str, list[dict[str, Any]]] = {}
    for event in iter_committed_events(state_root):
        if str(event.get("event_type") or "") != "runtime_liveness_observed":
            continue
        node_id = str(event.get("node_id") or "").strip()
        if not node_id:
            continue
        payload = dict(event.get("payload") or {})
        summary = {
            "payload": payload,
            "observed_at": str(payload.get("observed_at") or event.get("recorded_at") or ""),
        }
        summary.update(
            summarize_runtime_liveness_observation(
                payload,
                recorded_at=str(event.get("recorded_at") or ""),
            )
        )
        observations_by_node.setdefault(node_id, []).append(summary)

    dirty = False
    for node_id, observations in observations_by_node.items():
        node_payload = kernel_state.nodes.get(node_id)
        if not isinstance(node_payload, dict):
            continue
        _raw_head, effective_head = select_effective_runtime_liveness_head(observations)
        effective_summary = dict(effective_head or {})
        effective_payload = dict(effective_summary.get("payload") or {})
        existing_runtime_state = normalize_runtime_state(dict(node_payload.get("runtime_state") or {}))
        effective_attachment_state = str(
            effective_summary.get("effective_attachment_state")
            or effective_summary.get("raw_attachment_state")
            or effective_payload.get("attachment_state")
            or existing_runtime_state.get("attachment_state")
            or RuntimeAttachmentState.UNOBSERVED.value
        ).strip() or RuntimeAttachmentState.UNOBSERVED.value
        merged_evidence_refs = [
            str(item) for item in list(effective_payload.get("evidence_refs") or []) if str(item).strip()
        ]
        for item in list(existing_runtime_state.get("evidence_refs") or []):
            text = str(item or "").strip()
            if text and text not in merged_evidence_refs:
                merged_evidence_refs.append(text)
        effective_live_process_trusted = bool(
            effective_attachment_state == RuntimeAttachmentState.ATTACHED.value
            and str(effective_summary.get("lease_freshness") or "").strip().lower() == "fresh"
            and bool(effective_payload.get("pid_alive"))
        )
        next_runtime_state = normalize_runtime_state(
            {
                **existing_runtime_state,
                "attachment_state": effective_attachment_state,
                "observed_at": str(
                    effective_summary.get("observed_at")
                    or effective_payload.get("observed_at")
                    or existing_runtime_state.get("observed_at")
                    or ""
                ),
                "summary": str(effective_payload.get("summary") or existing_runtime_state.get("summary") or ""),
                "observation_kind": str(
                    effective_payload.get("observation_kind") or existing_runtime_state.get("observation_kind") or ""
                ),
                "evidence_refs": merged_evidence_refs,
                "live_process_trusted": effective_live_process_trusted,
                "pid": int(effective_payload.get("pid") or existing_runtime_state.get("pid") or 0),
                "pid_alive": bool(effective_payload.get("pid_alive", existing_runtime_state.get("pid_alive"))),
                "lease_epoch": int(
                    effective_summary.get("lease_epoch")
                    or effective_payload.get("lease_epoch")
                    or existing_runtime_state.get("lease_epoch")
                    or 0
                ),
                "lease_owner_id": str(
                    effective_summary.get("lease_owner_id")
                    or effective_payload.get("lease_owner_id")
                    or existing_runtime_state.get("lease_owner_id")
                    or ""
                ),
                "lease_freshness": str(
                    effective_summary.get("lease_freshness") or existing_runtime_state.get("lease_freshness") or ""
                ),
                "lease_source": str(
                    effective_summary.get("lease_source") or existing_runtime_state.get("lease_source") or ""
                ),
                "lease_duration_s": (
                    effective_summary.get("lease_duration_s")
                    if effective_summary.get("lease_duration_s") is not None
                    else effective_payload.get("lease_duration_s", existing_runtime_state.get("lease_duration_s"))
                ),
                "lease_expires_at": str(
                    effective_summary.get("lease_expires_at") or existing_runtime_state.get("lease_expires_at") or ""
                ),
                "launch_event_id": str(
                    effective_payload.get("launch_event_id") or existing_runtime_state.get("launch_event_id") or ""
                ),
                "status_result_ref": str(
                    effective_payload.get("status_result_ref") or existing_runtime_state.get("status_result_ref") or ""
                ),
                "runtime_ref": str(effective_payload.get("runtime_ref") or existing_runtime_state.get("runtime_ref") or ""),
                "process_fingerprint": str(
                    effective_payload.get("process_fingerprint")
                    or existing_runtime_state.get("process_fingerprint")
                    or ""
                ),
                "process_started_at_utc": str(
                    effective_payload.get("process_started_at_utc")
                    or existing_runtime_state.get("process_started_at_utc")
                    or ""
                ),
            }
        )
        if next_runtime_state != existing_runtime_state:
            node_payload["runtime_state"] = next_runtime_state
            dirty = True
    return dirty


def synchronize_authoritative_node_results(
    state_root: Path,
    *,
    continue_deferred: bool = True,
    authority: KernelMutationAuthority | None = None,
) -> KernelState:
    """Normalize durable node state from authoritative child result sinks."""

    require_kernel_authority(authority, surface="synchronize_authoritative_node_results")
    state_root = require_runtime_root(state_root)
    kernel_projection_floor = _projection_floor_from_path(state_root / "state" / "kernel_state.json")
    kernel_state = load_kernel_state(state_root)
    kernel_state, projection_seq, replay_dirty = _reconcile_kernel_projection_from_journal(
        state_root=state_root,
        kernel_state=kernel_state,
        applied_seq=kernel_projection_floor,
    )
    _workspace_artifact_hygiene_sync(state_root=state_root, kernel_state=kernel_state)
    dirty = replay_dirty

    for node_id, node_payload in list(kernel_state.nodes.items()):
        canonical_node_payload, sync_candidate = _canonicalized_node_payload_from_authoritative_result(
            kernel_state=kernel_state,
            state_root=state_root,
            node_id=node_id,
            node_payload=dict(node_payload or {}),
        )
        if canonical_node_payload != dict(node_payload or {}):
            kernel_state.nodes[node_id] = canonical_node_payload
            dirty = True
        if _sync_blocked_reason_for_node(
            kernel_state=kernel_state,
            node_id=node_id,
            node_payload=kernel_state.nodes[node_id],
            sync_candidate=sync_candidate,
        ):
            dirty = True
    if _apply_effective_runtime_liveness_projection(state_root=state_root, kernel_state=kernel_state):
        dirty = True
    projection_dirty = _projection_surfaces_need_refresh(
        state_root=state_root,
        kernel_state=kernel_state,
        last_applied_seq=projection_seq,
    )
    if dirty or projection_dirty:
        for node in kernel_state.nodes.values():
            persist_node_snapshot(state_root, node, authority=authority, projection_seq=projection_seq)
        persist_kernel_state(state_root, kernel_state, authority=authority, projection_seq=projection_seq)
        persist_projection_manifest(state_root, kernel_state, authority=authority, projection_seq=projection_seq)

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


def rebuild_kernel_projections(
    state_root: Path,
    *,
    continue_deferred: bool = False,
    authority: KernelMutationAuthority | None = None,
) -> dict[str, Any]:
    """Explicitly rebuild durable kernel/node projections from current authoritative truth."""

    require_kernel_authority(authority, surface="rebuild_kernel_projections")
    state_root = require_runtime_root(state_root)
    before_kernel_state = load_kernel_state(state_root)
    previous_kernel_last_applied_seq = _projection_floor_from_path(state_root / "state" / "kernel_state.json")
    previous_projection_manifest_last_applied_seq = _projection_floor_from_path(_projection_manifest_path(state_root))
    previous_node_floors = {
        node_id: _projection_floor_from_path(state_root / "state" / f"{node_id}.json")
        for node_id in before_kernel_state.nodes
    }
    previous_accepted_envelope_count = len(before_kernel_state.accepted_envelopes)
    from loop_product.kernel.query import query_projection_consistency_view

    before_consistency = query_projection_consistency_view(state_root)
    before_replay_summary = dict(before_consistency.get("projection_replay_summary") or {})

    rebuilt_state = synchronize_authoritative_node_results(
        state_root,
        continue_deferred=continue_deferred,
        authority=authority,
    )

    committed_seq = _latest_committed_projection_seq(state_root)
    kernel_last_applied_seq = _projection_floor_from_path(state_root / "state" / "kernel_state.json")
    projection_manifest_last_applied_seq = _projection_floor_from_path(_projection_manifest_path(state_root))
    lagging_node_ids = [
        str(node_id)
        for node_id in sorted(rebuilt_state.nodes)
        if _projection_floor_from_path(state_root / "state" / f"{node_id}.json") < committed_seq
    ]
    if kernel_last_applied_seq < committed_seq:
        projection_state = "kernel_stale"
    elif lagging_node_ids:
        projection_state = "partially_projected"
    else:
        projection_state = "projected"
    updated_node_ids = [
        str(node_id)
        for node_id in sorted(rebuilt_state.nodes)
        if _projection_floor_from_path(state_root / "state" / f"{node_id}.json")
        != int(previous_node_floors.get(node_id, 0))
    ]
    replayed_event_count = max(len(rebuilt_state.accepted_envelopes) - previous_accepted_envelope_count, 0)
    after_consistency = query_projection_consistency_view(state_root)
    after_replay_summary = dict(after_consistency.get("projection_replay_summary") or {})
    pending_replay = dict(after_consistency.get("pending_replay") or {})
    status = "completed" if projection_state == "projected" else "blocked"
    summary = {
        "runtime_root": str(state_root),
        "rebuild_scope": "kernel",
        "committed_seq": committed_seq,
        "previous_kernel_last_applied_seq": previous_kernel_last_applied_seq,
        "kernel_last_applied_seq": kernel_last_applied_seq,
        "previous_projection_manifest_last_applied_seq": previous_projection_manifest_last_applied_seq,
        "projection_manifest_last_applied_seq": projection_manifest_last_applied_seq,
        "replayed_event_count": replayed_event_count,
        "updated_node_ids": updated_node_ids,
        "lagging_node_ids": lagging_node_ids,
        "projection_state": projection_state,
        "visibility_state": str(after_consistency.get("visibility_state") or ""),
        "status": status,
        "blocking_reason": str(dict(pending_replay.get("blocking_event") or {}).get("reason") or ""),
        "pending_replay": pending_replay,
        "repair_state_before": str(before_replay_summary.get("repair_state") or ""),
        "repair_scope_before": str(before_replay_summary.get("repair_scope") or ""),
        "repair_target_node_ids_before": [str(item) for item in list(before_replay_summary.get("repair_target_node_ids") or [])],
        "repairable_before": bool(before_replay_summary.get("repairable_now")),
        "repair_state_after": str(after_replay_summary.get("repair_state") or ""),
        "repair_scope_after": str(after_replay_summary.get("repair_scope") or ""),
        "repair_target_node_ids_after": [str(item) for item in list(after_replay_summary.get("repair_target_node_ids") or [])],
        "repairable_after": bool(after_replay_summary.get("repairable_now")),
        "repair_cleared": bool(before_replay_summary.get("repair_needed")) and not bool(after_replay_summary.get("repair_needed")),
    }
    return _record_projection_rebuild_result(
        state_root=state_root,
        summary=summary,
        producer="kernel.state.rebuild_kernel_projections",
    )


def rebuild_node_projection(
    state_root: Path,
    node_id: str,
    *,
    authority: KernelMutationAuthority | None = None,
) -> dict[str, Any]:
    """Explicitly rebuild one node projection when the kernel projection is already current."""

    require_kernel_authority(authority, surface="rebuild_node_projection")
    state_root = require_runtime_root(state_root)
    target_node_id = str(node_id or "").strip()
    if not target_node_id:
        raise ValueError("node_id must be non-empty")

    committed_seq = _latest_committed_projection_seq(state_root)
    kernel_last_applied_seq = _projection_floor_from_path(state_root / "state" / "kernel_state.json")
    node_path = state_root / "state" / f"{target_node_id}.json"
    previous_node_last_applied_seq = _projection_floor_from_path(node_path)
    previous_projection_manifest_last_applied_seq = _projection_floor_from_path(_projection_manifest_path(state_root))
    from loop_product.kernel.query import query_projection_consistency_view

    before_consistency = query_projection_consistency_view(state_root)
    before_replay_summary = dict(before_consistency.get("projection_replay_summary") or {})

    if kernel_last_applied_seq < committed_seq:
        return _record_projection_rebuild_result(
            state_root=state_root,
            summary={
                "runtime_root": str(state_root),
                "rebuild_scope": "node",
                "target_node_id": target_node_id,
                "committed_seq": committed_seq,
                "kernel_last_applied_seq": kernel_last_applied_seq,
                "previous_node_last_applied_seq": previous_node_last_applied_seq,
                "node_last_applied_seq": previous_node_last_applied_seq,
                "previous_projection_manifest_last_applied_seq": previous_projection_manifest_last_applied_seq,
                "projection_manifest_last_applied_seq": previous_projection_manifest_last_applied_seq,
                "status": "blocked",
                "projection_state": "kernel_stale",
                "visibility_state": str(before_consistency.get("visibility_state") or ""),
                "blocking_reason": "kernel_projection_stale",
                "pending_replay": dict(before_consistency.get("pending_replay") or {}),
                "repair_state_before": str(before_replay_summary.get("repair_state") or ""),
                "repair_scope_before": str(before_replay_summary.get("repair_scope") or ""),
                "repair_target_node_ids_before": [str(item) for item in list(before_replay_summary.get("repair_target_node_ids") or [])],
                "repairable_before": bool(before_replay_summary.get("repairable_now")),
                "repair_state_after": str(before_replay_summary.get("repair_state") or ""),
                "repair_scope_after": str(before_replay_summary.get("repair_scope") or ""),
                "repair_target_node_ids_after": [str(item) for item in list(before_replay_summary.get("repair_target_node_ids") or [])],
                "repairable_after": bool(before_replay_summary.get("repairable_now")),
                "repair_cleared": False,
            },
            producer="kernel.state.rebuild_node_projection",
        )

    kernel_state = load_kernel_state(state_root)
    existing_node_payload = dict(kernel_state.nodes.get(target_node_id) or {})
    if not existing_node_payload:
        return _record_projection_rebuild_result(
            state_root=state_root,
            summary={
                "runtime_root": str(state_root),
                "rebuild_scope": "node",
                "target_node_id": target_node_id,
                "committed_seq": committed_seq,
                "kernel_last_applied_seq": kernel_last_applied_seq,
                "previous_node_last_applied_seq": previous_node_last_applied_seq,
                "node_last_applied_seq": previous_node_last_applied_seq,
                "previous_projection_manifest_last_applied_seq": previous_projection_manifest_last_applied_seq,
                "projection_manifest_last_applied_seq": previous_projection_manifest_last_applied_seq,
                "status": "missing_node",
                "projection_state": "missing_node",
                "visibility_state": str(before_consistency.get("visibility_state") or ""),
                "blocking_reason": "unknown_node_id",
                "pending_replay": dict(before_consistency.get("pending_replay") or {}),
                "repair_state_before": str(before_replay_summary.get("repair_state") or ""),
                "repair_scope_before": str(before_replay_summary.get("repair_scope") or ""),
                "repair_target_node_ids_before": [str(item) for item in list(before_replay_summary.get("repair_target_node_ids") or [])],
                "repairable_before": bool(before_replay_summary.get("repairable_now")),
                "repair_state_after": str(before_replay_summary.get("repair_state") or ""),
                "repair_scope_after": str(before_replay_summary.get("repair_scope") or ""),
                "repair_target_node_ids_after": [str(item) for item in list(before_replay_summary.get("repair_target_node_ids") or [])],
                "repairable_after": bool(before_replay_summary.get("repairable_now")),
                "repair_cleared": False,
            },
            producer="kernel.state.rebuild_node_projection",
        )

    canonical_node_payload, sync_candidate = _canonicalized_node_payload_from_authoritative_result(
        kernel_state=kernel_state,
        state_root=state_root,
        node_id=target_node_id,
        node_payload=existing_node_payload,
    )
    kernel_dirty = False
    if canonical_node_payload != existing_node_payload:
        kernel_state.nodes[target_node_id] = canonical_node_payload
        kernel_dirty = True
    if _sync_blocked_reason_for_node(
        kernel_state=kernel_state,
        node_id=target_node_id,
        node_payload=kernel_state.nodes[target_node_id],
        sync_candidate=sync_candidate,
    ):
        kernel_dirty = True
    if kernel_dirty:
        persist_kernel_state(state_root, kernel_state, authority=authority, projection_seq=committed_seq)

    persist_node_snapshot(
        state_root,
        kernel_state.nodes[target_node_id],
        authority=authority,
        projection_seq=committed_seq,
    )

    all_node_floors_current = all(
        _projection_floor_from_path(state_root / "state" / f"{current_node_id}.json") >= committed_seq
        for current_node_id in kernel_state.nodes
    )
    if all_node_floors_current and _projection_floor_from_path(state_root / "state" / "kernel_state.json") >= committed_seq:
        persist_projection_manifest(
            state_root,
            kernel_state,
            authority=authority,
            projection_seq=committed_seq,
        )

    node_last_applied_seq = _projection_floor_from_path(node_path)
    projection_manifest_last_applied_seq = _projection_floor_from_path(_projection_manifest_path(state_root))
    consistency = query_projection_consistency_view(state_root)
    after_replay_summary = dict(consistency.get("projection_replay_summary") or {})
    status = "already_current"
    if previous_node_last_applied_seq < committed_seq or kernel_dirty:
        status = "rebuilt"
    summary = {
        "runtime_root": str(state_root),
        "rebuild_scope": "node",
        "target_node_id": target_node_id,
        "committed_seq": committed_seq,
        "kernel_last_applied_seq": _projection_floor_from_path(state_root / "state" / "kernel_state.json"),
        "previous_node_last_applied_seq": previous_node_last_applied_seq,
        "node_last_applied_seq": node_last_applied_seq,
        "previous_projection_manifest_last_applied_seq": previous_projection_manifest_last_applied_seq,
        "projection_manifest_last_applied_seq": projection_manifest_last_applied_seq,
        "status": status,
        "projection_state": "projected" if str(consistency.get("visibility_state") or "") == "atomically_visible" else "partial",
        "visibility_state": str(consistency.get("visibility_state") or ""),
        "blocking_reason": "",
        "pending_replay": dict(consistency.get("pending_replay") or {}),
        "repair_state_before": str(before_replay_summary.get("repair_state") or ""),
        "repair_scope_before": str(before_replay_summary.get("repair_scope") or ""),
        "repair_target_node_ids_before": [str(item) for item in list(before_replay_summary.get("repair_target_node_ids") or [])],
        "repairable_before": bool(before_replay_summary.get("repairable_now")),
        "repair_state_after": str(after_replay_summary.get("repair_state") or ""),
        "repair_scope_after": str(after_replay_summary.get("repair_scope") or ""),
        "repair_target_node_ids_after": [str(item) for item in list(after_replay_summary.get("repair_target_node_ids") or [])],
        "repairable_after": bool(after_replay_summary.get("repairable_now")),
        "repair_cleared": bool(before_replay_summary.get("repair_needed")) and not bool(after_replay_summary.get("repair_needed")),
    }
    return _record_projection_rebuild_result(
        state_root=state_root,
        summary=summary,
        producer="kernel.state.rebuild_node_projection",
    )


def persist_kernel_state(
    state_root: Path,
    kernel_state: KernelState,
    *,
    authority: KernelMutationAuthority | None = None,
    projection_seq: int | None = None,
) -> Path:
    """Write kernel state into the authority tree."""

    require_kernel_authority(authority, surface="persist_kernel_state")
    state_root = require_runtime_root(state_root)
    state_dir = state_root / "state"
    state_dir.mkdir(parents=True, exist_ok=True)
    for node_id, node_payload in list(kernel_state.nodes.items()):
        canonical_node_payload, sync_candidate = _canonicalized_node_payload_from_authoritative_result(
            kernel_state=kernel_state,
            state_root=state_root,
            node_id=node_id,
            node_payload=dict(node_payload or {}),
        )
        kernel_state.nodes[node_id] = canonical_node_payload
        _sync_blocked_reason_for_node(
            kernel_state=kernel_state,
            node_id=node_id,
            node_payload=canonical_node_payload,
            sync_candidate=sync_candidate,
        )
    projection_seq = _latest_committed_projection_seq(state_root) if projection_seq is None else int(projection_seq)
    data = _annotate_projection_payload(
        kernel_state.to_dict(),
        projection_name=_KERNEL_PROJECTION_NAME,
        last_applied_seq=projection_seq,
        source_event_journal_ref=_projection_source_event_journal_ref(state_root),
        projection_bundle_ref=str(_projection_manifest_path(state_root).resolve()),
    )
    validate_repo_object("LoopSystemKernelState.schema.json", data)
    path = state_dir / "kernel_state.json"
    write_json_read_only(path, data)
    write_json_read_only(state_dir / "accepted_envelopes.json", kernel_state.accepted_envelopes)
    return path


def persist_node_snapshot(
    state_root: Path,
    node: NodeSpec | dict[str, Any],
    *,
    authority: KernelMutationAuthority | None = None,
    projection_seq: int | None = None,
) -> Path:
    """Persist one node snapshot under the runtime state tree."""

    require_kernel_authority(authority, surface="persist_node_snapshot")
    state_root = require_runtime_root(state_root)
    record = node.to_dict() if isinstance(node, NodeSpec) else dict(node)
    node_id = str(record["node_id"])
    record, _sync_candidate = _canonicalized_node_payload_from_authoritative_result(
        state_root=state_root,
        node_id=node_id,
        node_payload=record,
    )
    record = _annotate_projection_payload(
        record,
        projection_name=_NODE_PROJECTION_NAME,
        last_applied_seq=(
            _latest_committed_projection_seq(state_root) if projection_seq is None else int(projection_seq)
        ),
        source_event_journal_ref=_projection_source_event_journal_ref(state_root),
        projection_bundle_ref=str(_projection_manifest_path(state_root).resolve()),
    )
    path = state_root / "state" / f"{node_id}.json"
    write_json_read_only(path, record)
    return path


def persist_projection_manifest(
    state_root: Path,
    kernel_state: KernelState,
    *,
    authority: KernelMutationAuthority | None = None,
    projection_seq: int | None = None,
) -> Path:
    """Persist one manifest describing the current complete projection bundle."""

    require_kernel_authority(authority, surface="persist_projection_manifest")
    state_root = require_runtime_root(state_root)
    projection_seq = _latest_committed_projection_seq(state_root) if projection_seq is None else int(projection_seq)
    path = _projection_manifest_path(state_root)
    write_json_read_only(
        path,
        _projection_manifest_payload(
            state_root=state_root,
            kernel_state=kernel_state,
            last_applied_seq=projection_seq,
        ),
    )
    return path


def load_kernel_state(state_root: Path) -> KernelState:
    """Load authoritative kernel state from the runtime tree."""

    state_root = require_runtime_root(state_root)
    path = state_root / "state" / "kernel_state.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    validate_repo_object("LoopSystemKernelState.schema.json", data)
    return KernelState.from_dict(data)


def query_kernel_state_object(state_root: Path, *, continue_deferred: bool = False) -> KernelState:
    """Return a self-healed kernel state object derived from authoritative sinks."""

    state_root = require_runtime_root(state_root)
    from loop_product.kernel.authority import kernel_internal_authority

    rebuild_kernel_projections(
        state_root,
        continue_deferred=continue_deferred,
        authority=kernel_internal_authority(),
    )
    return load_kernel_state(state_root)


def query_kernel_state(state_root: Path) -> dict[str, Any]:
    """Return a read-only authoritative state snapshot for kernel queries."""

    return query_kernel_state_object(state_root, continue_deferred=True).to_dict()
