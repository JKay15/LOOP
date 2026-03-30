"""Deterministic root-kernel publication helper for external publish targets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import shutil
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from loop_product.artifact_hygiene import (
    artifact_fingerprint,
    canonicalize_directory_artifact_heavy_trees,
    iter_materialized_runtime_heavy_tree_paths,
    iter_workspace_artifact_roots,
    iter_workspace_publication_staging_roots,
    iter_runtime_heavy_tree_paths,
    remove_runtime_heavy_tree,
)
from loop_product.protocols.schema import validate_repo_object

_TERMINAL_EVALUATOR_VERDICTS = {"PASS", "FAIL", "STRUCTURED_EXCEPTION", "BUG", "STUCK", "ERROR"}
_WHOLE_PAPER_TERMINAL_CLASSIFICATIONS = {
    "FULLY_FAITHFUL_COMPLETE",
    "PAPER_DEFECT_EXPOSED",
    "EXTERNAL_DEPENDENCY_BLOCKED",
}
_WHOLE_PAPER_TERMINAL_CLASSIFICATION_ALIASES = {
    "whole-paper faithful complete formalization": "FULLY_FAITHFUL_COMPLETE",
    "paper defect exposed": "PAPER_DEFECT_EXPOSED",
    "external dependency blocked": "EXTERNAL_DEPENDENCY_BLOCKED",
}


def _absolute(path: str | Path) -> Path:
    raw = Path(path).expanduser()
    if raw.is_absolute():
        return raw
    return (Path.cwd() / raw).absolute()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_local_ref(raw_ref: object, *, artifact_root: Path) -> str:
    ref = str(raw_ref or "").strip()
    if not ref:
        return ""
    candidate = Path(ref).expanduser()
    artifact_root = artifact_root.resolve()
    if candidate.is_absolute():
        try:
            relative = candidate.resolve().relative_to(artifact_root)
        except Exception:
            return ""
        return relative.as_posix()
    try:
        resolved = (artifact_root / candidate).resolve()
        resolved.relative_to(artifact_root)
    except Exception:
        return ""
    return candidate.as_posix()


def _compact_external_ref_label(raw_ref: object) -> str:
    ref = str(raw_ref or "").strip()
    if not ref:
        return ""
    path = Path(ref).expanduser()
    if not path.is_absolute():
        return Path(ref).as_posix()
    parts = [part for part in path.parts if part not in (os.sep, "")]
    if not parts:
        return ref
    if len(parts) >= 2 and parts[-1].lower() == "skill.md":
        return "/".join(parts[-2:])
    if len(parts) >= 2 and parts[-1].lower() in {"agenta.md", "agents.md"}:
        return "/".join(parts[-2:])
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return parts[-1]


def _sanitize_publication_payload(value: Any, *, artifact_root: Path) -> Any:
    if isinstance(value, Mapping):
        sanitized: dict[str, Any] = {}
        for key, raw_item in value.items():
            if key == "source_tex_ref":
                text = str(raw_item or "").strip()
                sanitized[key] = Path(text).name if text else ""
                continue
            if key == "docs_used":
                labels = _ordered_unique_texts(_compact_external_ref_label(item) for item in list(raw_item or []))
                sanitized[key] = labels
                continue
            if key.endswith("_ref"):
                local_ref = _artifact_local_ref(raw_item, artifact_root=artifact_root)
                if local_ref:
                    sanitized[key] = local_ref
                continue
            if key.endswith("_refs"):
                refs = _ordered_unique_texts(
                    _artifact_local_ref(item, artifact_root=artifact_root) for item in list(raw_item or [])
                )
                sanitized[key] = refs
                continue
            nested = _sanitize_publication_payload(raw_item, artifact_root=artifact_root)
            sanitized[key] = nested
        return sanitized
    if isinstance(value, list):
        return [_sanitize_publication_payload(item, artifact_root=artifact_root) for item in value]
    return value


def _rewrite_exact_refs(value: Any, *, replacements: Mapping[str, str]) -> Any:
    if isinstance(value, Mapping):
        return {key: _rewrite_exact_refs(raw_item, replacements=replacements) for key, raw_item in value.items()}
    if isinstance(value, list):
        return [_rewrite_exact_refs(item, replacements=replacements) for item in value]
    if isinstance(value, str):
        text = value.strip()
        if text and text in replacements:
            return replacements[text]
    return value


def _replace_source_tex_ref_in_textual_surface(*, artifact_root: Path, original_ref: str, sanitized_ref: str) -> None:
    if not original_ref or original_ref == sanitized_ref:
        return
    for relpath in ("README.md", "TRACEABILITY.md"):
        path = artifact_root / relpath
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        rewritten = text.replace(original_ref, sanitized_ref)
        if rewritten != text:
            _write_text(path, rewritten)


def _whole_paper_source_section_count(artifact_root: Path) -> int:
    payload = _load_optional_json(artifact_root / "extraction" / "source_structure.json")
    return int(payload.get("section_count") or 0)


def _normalize_whole_paper_terminal_classification(raw: object) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    upper_value = value.upper()
    if upper_value in _WHOLE_PAPER_TERMINAL_CLASSIFICATIONS:
        return upper_value
    return _WHOLE_PAPER_TERMINAL_CLASSIFICATION_ALIASES.get(value.lower(), "")


def _load_split_continuation_terminal_status(artifact_root: Path) -> dict[str, Any]:
    payload = _load_optional_json(artifact_root / "WHOLE_PAPER_STATUS.json")
    if not payload:
        return {}
    if str(payload.get("artifact_scope") or "").strip() != "slice":
        return {}
    if str(payload.get("status") or "").strip().upper() != "TERMINAL":
        return {}
    if str(payload.get("terminal_classification") or "").strip() != "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION":
        return {}
    if str(payload.get("terminal_authority_scope") or "").strip() != "local":
        return {}
    return payload


def _load_slice_local_completion_status(artifact_root: Path) -> dict[str, Any]:
    payload = _load_optional_json(artifact_root / "WHOLE_PAPER_STATUS.json")
    if not payload:
        return {}
    if str(payload.get("artifact_scope") or "").strip() != "slice":
        return {}
    if str(payload.get("classification") or "").strip() != "slice_local_formalization_complete":
        return {}
    if str(payload.get("status") or "").strip() != "LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION":
        return {}
    if str(payload.get("terminal_authority_scope") or "").strip() != "local":
        return {}
    return payload


def _load_whole_paper_terminal_status(artifact_root: Path) -> dict[str, Any]:
    payload = _load_optional_json(artifact_root / "WHOLE_PAPER_STATUS.json")
    if not payload:
        return {}
    if str(payload.get("status") or "").strip().upper() != "TERMINAL":
        return {}
    classification = _normalize_whole_paper_terminal_classification(
        payload.get("whole_paper_terminal_classification") or payload.get("terminal_classification")
    )
    if not classification:
        return {}
    normalized = dict(payload)
    normalized["whole_paper_terminal_classification"] = classification
    normalized["terminal_classification"] = classification
    if "whole_paper_terminal_authority" not in normalized:
        normalized["whole_paper_terminal_authority"] = True
    return normalized


def _harmonize_split_continuation_control_surface(artifact_root: Path) -> None:
    payload = _load_split_continuation_terminal_status(artifact_root)
    if not payload:
        return

    node_id = str(payload.get("node_id") or "").strip() or "unknown-node"
    round_id = str(payload.get("round_id") or "").strip() or "unresolved"
    goal_slice = str(payload.get("goal_slice") or "").strip()
    source_tex_ref = str(payload.get("source_tex_ref") or "").strip()
    summary = str(payload.get("summary") or "").strip()
    accepted_child_node_ids = [str(item).strip() for item in list(payload.get("accepted_child_node_ids") or []) if str(item).strip()]
    released_remaining_work = [dict(item) for item in list(payload.get("released_remaining_work") or []) if isinstance(item, dict)]

    accepted_child_lines = accepted_child_node_ids or [str(item.get("node_id") or "").strip() for item in released_remaining_work if str(item.get("node_id") or "").strip()]
    accepted_child_lines = [item for item in accepted_child_lines if item]
    remaining_work_lines = [
        f"- `{str(item.get('node_id') or '').strip()}`: {str(item.get('goal_slice') or '').strip()}"
        for item in released_remaining_work
        if str(item.get("node_id") or "").strip() and str(item.get("goal_slice") or "").strip()
    ]

    readme_lines = [
        "# Whole-Paper Slice Split-Continuation Artifact",
        "",
        f"- node_id: `{node_id}`",
        f"- round_id: `{round_id}`",
        f"- source_tex_ref: `{source_tex_ref}`" if source_tex_ref else "- source_tex_ref: `unresolved`",
        "",
        summary or "This slice settled locally and released its remaining work into accepted child lanes instead of overclaiming full slice completion.",
    ]
    if accepted_child_lines:
        readme_lines.extend(["", "Accepted child lanes:"])
        readme_lines.extend(f"- `{item}`" for item in accepted_child_lines)
    if remaining_work_lines:
        readme_lines.extend(["", "Released remaining work:"])
        readme_lines.extend(remaining_work_lines)

    traceability_lines = [
        "# Slice Split-Continuation Traceability",
        "",
        f"- node_id: `{node_id}`",
        f"- goal_slice: `{goal_slice}`" if goal_slice else "- goal_slice: `unresolved`",
        f"- source_tex_ref: `{source_tex_ref}`" if source_tex_ref else "- source_tex_ref: `unresolved`",
        "- local_terminal_classification: `SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION`",
        "- whole_paper_terminal_authority: `false`",
    ]
    if accepted_child_lines:
        traceability_lines.append(f"- accepted_child_node_ids: `{', '.join(accepted_child_lines)}`")
    if summary:
        traceability_lines.extend(["", summary])

    slice_status_payload = {
        "workflow_scope": "whole_paper_formalization",
        "artifact_scope": "slice",
        "terminal_authority_scope": "local",
        "node_id": node_id,
        "round_id": round_id,
        "status": "TERMINAL",
        "slice_terminal_classification": "SPLIT_ACCEPTED_CONTINUE_IMPLEMENTATION",
        "goal_slice": goal_slice,
        "source_tex_ref": source_tex_ref,
        "slice_startup_batch_materialized": True,
        "whole_paper_terminal_authority": False,
        "accepted_child_node_ids": accepted_child_node_ids,
        "released_remaining_work": released_remaining_work,
        "summary": summary,
    }
    boundary_payload = {
        "workflow_scope": "whole_paper_formalization",
        "artifact_scope": "slice",
        "terminal_authority_scope": "local",
        "node_id": node_id,
        "goal_slice": goal_slice,
        "source_tex_ref": source_tex_ref,
        "notes": [
            "This slice settled into truthful local split continuation.",
            "Remaining work was released into accepted child lanes instead of being overclaimed here.",
            *(
                [f"Accepted child nodes: {', '.join(accepted_child_lines)}."]
                if accepted_child_lines
                else []
            ),
            "Whole-paper terminal classification authority remains outside this slice node.",
        ],
    }

    _write_json(artifact_root / "SLICE_STATUS.json", slice_status_payload)
    boundary_path = artifact_root / "analysis" / "SLICE_BOUNDARY.json"
    existing_boundary = _load_optional_json(boundary_path)
    existing_notes = [str(item) for item in list(existing_boundary.get("notes") or [])]
    if (
        not existing_boundary
        or any("startup bundle" in item.lower() for item in existing_notes)
        or any("still owes substantive formalization" in item.lower() for item in existing_notes)
    ):
        _write_json(boundary_path, boundary_payload)

    readme_path = artifact_root / "README.md"
    existing_readme = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""
    if not existing_readme.strip() or "startup bundle" in existing_readme.lower():
        _write_text(readme_path, "\n".join(readme_lines))

    traceability_path = artifact_root / "TRACEABILITY.md"
    existing_traceability = traceability_path.read_text(encoding="utf-8") if traceability_path.exists() else ""
    if not existing_traceability.strip() or "startup traceability" in existing_traceability.lower():
        _write_text(traceability_path, "\n".join(traceability_lines))


def _harmonize_slice_local_completion_control_surface(artifact_root: Path) -> None:
    payload = _load_slice_local_completion_status(artifact_root)
    if not payload:
        return

    node_id = str(payload.get("node_id") or "").strip() or "unknown-node"
    round_id = str(payload.get("round_id") or "").strip() or "unresolved"
    goal_slice = str(payload.get("goal_slice") or "").strip()
    source_tex_ref = str(payload.get("source_tex_ref") or "").strip()
    summary = str(payload.get("summary") or "").strip()
    whole_paper_terminal_classification = str(payload.get("whole_paper_terminal_classification") or "").strip() or "NOT_OWNED_BY_SLICE"
    verification = dict(payload.get("verification") or {})
    split_status = str(payload.get("split_status") or "").strip()

    readme_lines = [
        "# Whole-Paper Slice Local Completion Artifact",
        "",
        f"- node_id: `{node_id}`",
        f"- round_id: `{round_id}`",
        f"- source_tex_ref: `{source_tex_ref}`" if source_tex_ref else "- source_tex_ref: `unresolved`",
        "",
        summary or "This slice-local artifact completed its narrowed formalization while leaving whole-paper closeout parent-owned.",
        "",
        "Whole-paper terminal classification remains parent-owned.",
    ]
    if split_status:
        readme_lines.append(f"Historical split status: `{split_status}`.")

    traceability_lines = [
        "# Slice Local Completion Traceability",
        "",
        f"- node_id: `{node_id}`",
        f"- goal_slice: `{goal_slice}`" if goal_slice else "- goal_slice: `unresolved`",
        f"- source_tex_ref: `{source_tex_ref}`" if source_tex_ref else "- source_tex_ref: `unresolved`",
        "- local_slice_completion_classification: `slice_local_formalization_complete`",
        "- local_slice_completion_status: `LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION`",
        f"- whole_paper_terminal_classification: `{whole_paper_terminal_classification}`",
        "- whole_paper_terminal_authority: `false`",
    ]
    if verification:
        traceability_lines.append(f"- verification: `{json.dumps(verification, sort_keys=True)}`")
    if summary:
        traceability_lines.extend(["", summary])

    slice_status_payload = {
        "workflow_scope": "whole_paper_formalization",
        "artifact_scope": "slice",
        "terminal_authority_scope": "local",
        "node_id": node_id,
        "round_id": round_id,
        "status": "LOCAL_SLICE_COMPLETE_PENDING_PARENT_INTEGRATION",
        "slice_completion_classification": "slice_local_formalization_complete",
        "goal_slice": goal_slice,
        "source_tex_ref": source_tex_ref,
        "whole_paper_terminal_authority": False,
        "whole_paper_terminal_classification": whole_paper_terminal_classification,
        "summary": summary,
        "verification": verification,
        "split_status": split_status,
    }
    boundary_payload = {
        "workflow_scope": "whole_paper_formalization",
        "artifact_scope": "slice",
        "terminal_authority_scope": "local",
        "node_id": node_id,
        "goal_slice": goal_slice,
        "source_tex_ref": source_tex_ref,
        "notes": [
            "This slice completed its local formalization and is no longer only a startup bundle.",
            "Whole-paper closeout remains parent-owned.",
            f"Whole-paper terminal classification remains `{whole_paper_terminal_classification}`.",
            *([f"Historical split status: {split_status}."] if split_status else []),
        ],
    }

    _write_json(artifact_root / "SLICE_STATUS.json", slice_status_payload)
    boundary_path = artifact_root / "analysis" / "SLICE_BOUNDARY.json"
    existing_boundary = _load_optional_json(boundary_path)
    existing_notes = [str(item) for item in list(existing_boundary.get("notes") or [])]
    if (
        not existing_boundary
        or any("startup bundle" in item.lower() for item in existing_notes)
        or any("still owes substantive formalization" in item.lower() for item in existing_notes)
    ):
        _write_json(boundary_path, boundary_payload)

    readme_path = artifact_root / "README.md"
    existing_readme = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""
    if not existing_readme.strip() or "startup bundle" in existing_readme.lower():
        _write_text(readme_path, "\n".join(readme_lines))

    traceability_path = artifact_root / "TRACEABILITY.md"
    existing_traceability = traceability_path.read_text(encoding="utf-8") if traceability_path.exists() else ""
    if not existing_traceability.strip() or "startup traceability" in existing_traceability.lower():
        _write_text(traceability_path, "\n".join(traceability_lines))


def _harmonize_whole_paper_terminal_control_surface(artifact_root: Path) -> None:
    payload = _load_whole_paper_terminal_status(artifact_root)
    if not payload:
        return

    source_section_count = _whole_paper_source_section_count(artifact_root)
    theorem_inventory_payload = _load_optional_json(artifact_root / "extraction" / "theorem_inventory.json")
    dependency_graph_payload = _load_optional_json(artifact_root / "analysis" / "internal_dependency_graph.json")
    coverage = dict(payload.get("coverage") or {})
    if coverage:
        theorem_like_count = int(
            theorem_inventory_payload.get("theorem_like_count")
            or coverage.get("theorem_like_count")
            or 0
        )
        edge_count = int(
            dependency_graph_payload.get("edge_count")
            or coverage.get("internal_dependency_edge_count")
            or 0
        )
        coverage.update(
            {
                "section_count": int(source_section_count or coverage.get("section_count") or 0),
                "theorem_like_count": theorem_like_count,
                "theorem_like_item_count_materialized": int(
                    coverage.get("theorem_like_item_count_materialized") or theorem_like_count
                ),
                "internal_dependency_edge_count": edge_count,
            }
        )
        payload["coverage"] = coverage

    node_id = str(payload.get("node_id") or "").strip() or "unknown-node"
    round_id = str(payload.get("round_id") or "").strip() or "unresolved"
    source_tex_ref = str(payload.get("source_tex_ref") or "").strip()
    classification = str(payload.get("whole_paper_terminal_classification") or "").strip()
    terminal_authority = bool(payload.get("whole_paper_terminal_authority", True))
    notes = [str(item).strip() for item in list(payload.get("notes") or []) if str(item).strip()]
    terminal_drivers = [
        {
            "label": str(item.get("label") or "").strip(),
            "kind": str(item.get("kind") or "").strip(),
            "summary": str(item.get("summary") or "").strip(),
        }
        for item in list(payload.get("terminal_drivers") or [])
        if isinstance(item, Mapping)
        and (str(item.get("label") or "").strip() or str(item.get("summary") or "").strip())
    ]

    readme_lines = [
        "# Whole-Paper Integration Closeout",
        "",
        f"- node_id: `{node_id}`",
        f"- round_id: `{round_id}`",
        f"- source_tex_ref: `{source_tex_ref}`" if source_tex_ref else "- source_tex_ref: `unresolved`",
        f"- whole_paper_terminal_classification: `{classification}`",
        "",
        f"This artifact records a truthful whole-paper terminal closeout with classification `{classification}`.",
    ]
    if terminal_drivers:
        readme_lines.extend(["", "Terminal drivers:"])
        for driver in terminal_drivers:
            label = driver["label"] or "unlabeled-driver"
            summary = driver["summary"] or "no summary recorded"
            readme_lines.append(f"- `{label}`: {summary}")
    if notes:
        readme_lines.extend(["", "Notes:"])
        readme_lines.extend(f"- {item}" for item in notes)

    traceability_lines = [
        "# Whole-Paper Terminal Traceability",
        "",
        f"- node_id: `{node_id}`",
        f"- round_id: `{round_id}`",
        f"- source_tex_ref: `{source_tex_ref}`" if source_tex_ref else "- source_tex_ref: `unresolved`",
        f"- whole-paper terminal classification: `{classification}`",
        f"- whole-paper terminal authority: `{str(terminal_authority).lower()}`",
    ]
    if terminal_drivers:
        traceability_lines.append("- terminal_drivers:")
        traceability_lines.extend(
            f"  - `{driver['label'] or 'unlabeled-driver'}` ({driver['kind'] or 'unspecified'}): {driver['summary'] or 'no summary recorded'}"
            for driver in terminal_drivers
        )
    if notes:
        traceability_lines.extend(["", *[f"- {item}" for item in notes]])

    _write_json(artifact_root / "WHOLE_PAPER_STATUS.json", payload)

    readme_path = artifact_root / "README.md"
    existing_readme = readme_path.read_text(encoding="utf-8") if readme_path.exists() else ""
    if (
        not existing_readme.strip()
        or "startup bundle" in existing_readme.lower()
        or "extraction batch" in existing_readme.lower()
    ):
        _write_text(readme_path, "\n".join(readme_lines))

    traceability_path = artifact_root / "TRACEABILITY.md"
    existing_traceability = traceability_path.read_text(encoding="utf-8") if traceability_path.exists() else ""
    if (
        not existing_traceability.strip()
        or "startup traceability" in existing_traceability.lower()
        or "startup bundle" in existing_traceability.lower()
    ):
        _write_text(traceability_path, "\n".join(traceability_lines))


def _ordered_unique_texts(items: list[object] | tuple[object, ...]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _merge_dependency_entry(existing: dict[str, Any], incoming: Mapping[str, Any], *, parent_root: Path) -> None:
    incoming_status = str(incoming.get("status") or "").strip()
    existing_status = str(existing.get("status") or "").strip()
    if incoming_status and (not existing_status or incoming_status.startswith("RESOLVED")):
        existing["status"] = incoming_status
    for key in ("proof_impact", "required_next_step_if_closure_is_desired", "child_result_ref", "child_evaluation_report_ref"):
        incoming_value = str(incoming.get(key) or "").strip()
        if incoming_value and not str(existing.get(key) or "").strip():
            existing[key] = incoming_value
    for key in ("used_by", "why_resolved", "why_blocking"):
        existing[key] = _ordered_unique_texts(
            [*list(existing.get(key) or []), *list(incoming.get(key) or [])]
        )
        if not existing[key]:
            existing.pop(key, None)
    evidence_refs: list[str] = []
    for raw_ref in [*list(existing.get("evidence_refs") or []), *list(incoming.get("evidence_refs") or [])]:
        ref = str(raw_ref or "").strip()
        if not ref:
            continue
        if Path(ref).is_absolute():
            evidence_refs.append(ref)
            continue
        if (parent_root / ref).exists():
            evidence_refs.append(ref)
    evidence_refs = _ordered_unique_texts(evidence_refs)
    if evidence_refs:
        existing["evidence_refs"] = evidence_refs
    else:
        existing.pop("evidence_refs", None)


def _slug_fragment(text: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")
    return normalized or "item"


def _guess_proof_relevant_content_kind(
    *,
    status: str = "",
    label: str = "",
    description: str = "",
    claim_id: str = "",
    env_name: str = "",
) -> str:
    joined = " ".join(
        part
        for part in (
            status.lower(),
            label.lower(),
            description.lower(),
            claim_id.lower(),
            env_name.lower(),
        )
        if part
    )
    if "implicit proof obligation" in joined or "proof obligation" in joined:
        return "implicit_proof_obligation"
    if "notation" in joined:
        return "notation"
    if label.startswith("assum:") or "assumption" in joined:
        return "assumption"
    if label.startswith("alg") or "algorithm" in joined:
        return "algorithm"
    if "program" in joined:
        return "optimization_program"
    if env_name or label.startswith(("thm:", "prop:", "lem:", "cor:", "eq:")):
        if env_name in {"theorem", "lemma", "corollary", "proposition", "equation", "remark", "example"}:
            return "theorem_like"
    return "definition"


def _append_proof_relevant_inventory_item(
    items: list[dict[str, Any]],
    *,
    seen_keys: set[str],
    item: Mapping[str, Any],
) -> None:
    payload = {str(key): value for key, value in dict(item).items() if value not in (None, "", [], {})}
    inventory_id = str(payload.get("inventory_id") or "").strip()
    content_kind = str(payload.get("content_kind") or "").strip()
    key = f"{content_kind}:{inventory_id or json.dumps(payload, sort_keys=True, ensure_ascii=False)}"
    if key in seen_keys:
        return
    seen_keys.add(key)
    items.append(payload)


def _read_optional_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _collect_theorem_inventory_items(
    *,
    artifact_root: Path,
    items: list[dict[str, Any]],
    seen_keys: set[str],
) -> None:
    payload = _load_optional_json(artifact_root / "extraction" / "theorem_inventory.json")
    for index, raw_item in enumerate(list(payload.get("items") or []), start=1):
        if not isinstance(raw_item, Mapping):
            continue
        env_name = str(raw_item.get("env_name") or "").strip()
        label = str(raw_item.get("label") or raw_item.get("node_id") or "").strip()
        _append_proof_relevant_inventory_item(
            items,
            seen_keys=seen_keys,
            item={
                "inventory_id": label or f"theorem_inventory_{index:03d}",
                "content_kind": _guess_proof_relevant_content_kind(
                    label=label,
                    env_name=env_name,
                    description=str(raw_item.get("section_title") or ""),
                ),
                "content_kind_label": "theorem-like item" if env_name else "proof-relevant item",
                "env_name": env_name,
                "section_title": str(raw_item.get("section_title") or "").strip(),
                "line_start": raw_item.get("line_start"),
                "line_end": raw_item.get("line_end"),
                "summary": str(raw_item.get("statement_tex") or "").strip()[:240],
                "deduplicated": True,
                "traceable": True,
            },
        )


def _collect_problem_setup_source_map_items(
    *,
    child_root: Path,
    child_node_id: str,
    items: list[dict[str, Any]],
    seen_keys: set[str],
) -> None:
    payload = _load_optional_json(child_root / "analysis" / "problem_setup_source_map.json")
    for raw_item in list(payload.get("items") or []):
        if not isinstance(raw_item, Mapping):
            continue
        item_id = str(raw_item.get("id") or "").strip()
        artifact_targets = [str(value) for value in list(raw_item.get("artifact_targets") or []) if str(value).strip()]
        lean_symbols = [str(value) for value in list(raw_item.get("lean_symbols") or []) if str(value).strip()]
        _append_proof_relevant_inventory_item(
            items,
            seen_keys=seen_keys,
            item={
                "inventory_id": item_id or f"{child_node_id}:definition",
                "child_node_id": child_node_id,
                "content_kind": "definition",
                "content_kind_label": "definition / model surface",
                "line_start": raw_item.get("line_start"),
                "line_end": raw_item.get("line_end"),
                "artifact_targets": artifact_targets,
                "lean_symbols": lean_symbols,
                "summary": f"{child_node_id} source map covers {item_id or 'source item'}",
                "deduplicated": True,
                "traceable": True,
            },
        )


def _collect_section_source_map_items(
    *,
    child_root: Path,
    child_node_id: str,
    items: list[dict[str, Any]],
    seen_keys: set[str],
) -> None:
    payload = _load_optional_json(child_root / "analysis" / "SECTION_SOURCE_MAP.json")
    for raw_target in list(payload.get("targets") or []):
        if not isinstance(raw_target, Mapping):
            continue
        anchor = dict(raw_target.get("paper_anchor") or {})
        label = str(anchor.get("label") or "").strip()
        description = str(anchor.get("description") or "").strip()
        status = str(raw_target.get("status") or "").strip()
        _append_proof_relevant_inventory_item(
            items,
            seen_keys=seen_keys,
            item={
                "inventory_id": label or f"{child_node_id}:{_slug_fragment(description)}",
                "child_node_id": child_node_id,
                "content_kind": _guess_proof_relevant_content_kind(
                    status=status,
                    label=label,
                    description=description,
                ),
                "content_kind_label": status or "source-mapped surface",
                "line_start": anchor.get("start_line"),
                "line_end": anchor.get("end_line"),
                "summary": description,
                "lean_counterpart": str(raw_target.get("lean_counterpart") or "").strip(),
                "deduplicated": True,
                "traceable": True,
            },
        )


def _collect_formalization_ledger_items(
    *,
    child_root: Path,
    child_node_id: str,
    items: list[dict[str, Any]],
    seen_keys: set[str],
) -> None:
    payload = _load_optional_json(child_root / "analysis" / "FORMALIZATION_LEDGER.json")
    for raw_binding in list(payload.get("formalization_bindings") or []):
        if not isinstance(raw_binding, Mapping):
            continue
        claim_id = str(raw_binding.get("claim_id") or "").strip()
        lean_target = dict(raw_binding.get("lean_target") or {})
        _append_proof_relevant_inventory_item(
            items,
            seen_keys=seen_keys,
            item={
                "inventory_id": claim_id or str(raw_binding.get("binding_id") or "").strip() or f"{child_node_id}:binding",
                "child_node_id": child_node_id,
                "content_kind": _guess_proof_relevant_content_kind(
                    claim_id=claim_id,
                    description=str(lean_target.get("declaration_name") or ""),
                ),
                "content_kind_label": "formalization binding",
                "summary": str(raw_binding.get("formalization_status") or "").strip() or "FORMALIZED",
                "lean_target": {
                    key: str(value)
                    for key, value in lean_target.items()
                    if str(value).strip()
                },
                "deduplicated": True,
                "traceable": True,
            },
        )


def _collect_notation_items_from_notes(
    *,
    child_root: Path,
    child_node_id: str,
    items: list[dict[str, Any]],
    seen_keys: set[str],
) -> None:
    notes_path = child_root / "analysis" / "FORMALIZATION_NOTES.md"
    text = _read_optional_text(notes_path)
    lowered = text.lower()
    if "notation" not in lowered and "certainty" not in lowered:
        return
    note_lines = [
        line.strip().lstrip("-").strip()
        for line in text.splitlines()
        if ("notation" in line.lower() or "certainty" in line.lower()) and line.strip()
    ]
    summary = " ".join(note_lines[:3]).strip() or "notation coverage preserved in child formalization notes"
    _append_proof_relevant_inventory_item(
        items,
        seen_keys=seen_keys,
        item={
            "inventory_id": f"{child_node_id}:notation",
            "child_node_id": child_node_id,
            "content_kind": "notation",
            "content_kind_label": "notation item",
            "summary": summary,
            "deduplicated": True,
            "traceable": True,
        },
    )


def _collect_obligation_items(
    *,
    child_root: Path,
    child_node_id: str,
    items: list[dict[str, Any]],
    seen_keys: set[str],
) -> None:
    analysis_root = child_root / "analysis"
    if not analysis_root.exists():
        return
    for obligation_path in sorted(analysis_root.glob("*OBLIGATION*.md")) + sorted(analysis_root.glob("*OBLIGATIONS*.md")):
        text = _read_optional_text(obligation_path)
        numbered_lines = [
            line.strip()
            for line in text.splitlines()
            if line.strip() and line.lstrip()[:2] in {f"{idx}." for idx in range(1, 10)}
        ]
        if not numbered_lines:
            numbered_lines = [line.strip() for line in text.splitlines() if "obligation" in line.lower()][:2]
        for index, line in enumerate(numbered_lines, start=1):
            summary = line.split(":", 1)[-1].strip() if ":" in line else line
            _append_proof_relevant_inventory_item(
                items,
                seen_keys=seen_keys,
                item={
                    "inventory_id": f"{child_node_id}:{obligation_path.stem.lower()}:{index:02d}",
                    "child_node_id": child_node_id,
                    "content_kind": "implicit_proof_obligation",
                    "content_kind_label": "implicit proof obligation",
                    "summary": summary,
                    "deduplicated": True,
                    "traceable": True,
                },
            )


def _harmonize_whole_paper_proof_relevant_inventory_surface(artifact_root: Path) -> None:
    payload = _load_whole_paper_terminal_status(artifact_root)
    if not payload:
        return
    summary_payload = _load_optional_json(artifact_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json")
    if not summary_payload:
        return

    items: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    _collect_theorem_inventory_items(artifact_root=artifact_root, items=items, seen_keys=seen_keys)

    for raw_child in list(summary_payload.get("integrated_children") or []):
        if not isinstance(raw_child, Mapping):
            continue
        child_node_id = str(raw_child.get("node_id") or "").strip() or "integrated-child"
        child_artifact_ref = str(raw_child.get("delivered_artifact_ref") or "").strip()
        if not child_artifact_ref:
            continue
        child_root = Path(child_artifact_ref).expanduser()
        if not child_root.exists():
            continue
        _collect_problem_setup_source_map_items(
            child_root=child_root,
            child_node_id=child_node_id,
            items=items,
            seen_keys=seen_keys,
        )
        _collect_section_source_map_items(
            child_root=child_root,
            child_node_id=child_node_id,
            items=items,
            seen_keys=seen_keys,
        )
        _collect_formalization_ledger_items(
            child_root=child_root,
            child_node_id=child_node_id,
            items=items,
            seen_keys=seen_keys,
        )
        _collect_notation_items_from_notes(
            child_root=child_root,
            child_node_id=child_node_id,
            items=items,
            seen_keys=seen_keys,
        )
        _collect_obligation_items(
            child_root=child_root,
            child_node_id=child_node_id,
            items=items,
            seen_keys=seen_keys,
        )

    def _count(kind: str) -> int:
        return sum(1 for item in items if str(item.get("content_kind") or "") == kind)

    inventory_path = artifact_root / "extraction" / "proof_relevant_inventory.json"
    inventory_payload = {
        "artifact_scope": "task",
        "workflow_scope": "whole_paper_formalization",
        "node_id": str(payload.get("node_id") or "").strip(),
        "source_tex_ref": str(payload.get("source_tex_ref") or "").strip(),
        "summary": {
            "deduplicated": True,
            "traceable": True,
            "item_count": len(items),
            "theorem_like_count": _count("theorem_like"),
            "definition_count": _count("definition"),
            "assumption_count": _count("assumption"),
            "algorithm_count": _count("algorithm"),
            "optimization_program_count": _count("optimization_program"),
            "notation_item_count": _count("notation"),
            "implicit_proof_obligation_count": _count("implicit_proof_obligation"),
        },
        "items": items,
    }
    _write_json(inventory_path, inventory_payload)

    status_path = artifact_root / "WHOLE_PAPER_STATUS.json"
    status_payload = _load_optional_json(status_path)
    if status_payload:
        coverage = dict(status_payload.get("coverage") or {})
        coverage.update(
            {
                "proof_relevant_inventory_ref": "extraction/proof_relevant_inventory.json",
                "proof_relevant_inventory_item_count": len(items),
                "definition_count": _count("definition"),
                "notation_item_count": _count("notation"),
                "implicit_proof_obligation_count": _count("implicit_proof_obligation"),
            }
        )
        status_payload["coverage"] = coverage
        _write_json(status_path, status_payload)

    coverage_summary = dict(summary_payload.get("coverage_summary") or {})
    coverage_summary.update(
        {
            "proof_relevant_inventory_ref": "extraction/proof_relevant_inventory.json",
            "proof_relevant_inventory_item_count": len(items),
            "definition_count": _count("definition"),
            "notation_item_count": _count("notation"),
            "implicit_proof_obligation_count": _count("implicit_proof_obligation"),
        }
    )
    summary_payload["coverage_summary"] = coverage_summary
    _write_json(artifact_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json", summary_payload)


def _harmonize_whole_paper_integration_summary_surface(artifact_root: Path) -> None:
    payload = _load_whole_paper_terminal_status(artifact_root)
    if not payload:
        return
    summary_path = artifact_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json"
    summary_payload = _load_optional_json(summary_path)
    if not summary_payload:
        return
    theorem_inventory_payload = _load_optional_json(artifact_root / "extraction" / "theorem_inventory.json")
    dependency_graph_payload = _load_optional_json(artifact_root / "analysis" / "internal_dependency_graph.json")
    source_structure_payload = _load_optional_json(artifact_root / "extraction" / "source_structure.json")
    status_coverage = dict(payload.get("coverage") or {})
    coverage_summary = dict(summary_payload.get("coverage_summary") or {})

    theorem_like_count = int(
        theorem_inventory_payload.get("theorem_like_count")
        or status_coverage.get("theorem_like_count")
        or coverage_summary.get("theorem_like_count")
        or 0
    )
    edge_count = int(
        dependency_graph_payload.get("edge_count")
        or status_coverage.get("internal_dependency_edge_count")
        or coverage_summary.get("internal_dependency_edge_count")
        or 0
    )
    coverage_summary.update(
        {
            "all_extracted_blocks_integrated": bool(
                status_coverage.get("all_extracted_blocks_integrated")
                if "all_extracted_blocks_integrated" in status_coverage
                else coverage_summary.get("all_extracted_blocks_integrated")
            ),
            "all_theorem_like_items_accounted_for": bool(
                status_coverage.get("all_theorem_like_items_accounted_for")
                if "all_theorem_like_items_accounted_for" in status_coverage
                else coverage_summary.get("all_theorem_like_items_accounted_for")
            ),
            "block_count": int(status_coverage.get("block_count") or coverage_summary.get("block_count") or 0),
            "formalizer_owned_internal_gaps": list(
                status_coverage.get("formalizer_owned_internal_gaps")
                or coverage_summary.get("formalizer_owned_internal_gaps")
                or []
            ),
            "internal_cross_references_closed_by_integrated_blocks": bool(
                status_coverage.get("internal_cross_references_closed")
                if "internal_cross_references_closed" in status_coverage
                else coverage_summary.get("internal_cross_references_closed_by_integrated_blocks")
            ),
            "internal_dependency_edge_count": edge_count,
            "internal_dependency_graph_ref": str(
                coverage_summary.get("internal_dependency_graph_ref") or "analysis/internal_dependency_graph.json"
            ),
            "inventory_ref": str(coverage_summary.get("inventory_ref") or "extraction/theorem_inventory.json"),
            "proof_relevant_inventory_ref": str(
                coverage_summary.get("proof_relevant_inventory_ref") or "extraction/proof_relevant_inventory.json"
            ),
            "open_internal_cross_references": list(
                status_coverage.get("open_internal_cross_references")
                or coverage_summary.get("open_internal_cross_references")
                or []
            ),
            "section_count": int(
                source_structure_payload.get("section_count")
                or status_coverage.get("section_count")
                or coverage_summary.get("section_count")
                or 0
            ),
            "theorem_like_count": theorem_like_count,
            "theorem_like_item_count_materialized": int(
                status_coverage.get("theorem_like_item_count_materialized")
                or coverage_summary.get("theorem_like_item_count_materialized")
                or theorem_like_count
            ),
            "proof_relevant_inventory_item_count": int(
                status_coverage.get("proof_relevant_inventory_item_count")
                or coverage_summary.get("proof_relevant_inventory_item_count")
                or 0
            ),
            "definition_count": int(
                status_coverage.get("definition_count") or coverage_summary.get("definition_count") or 0
            ),
            "notation_item_count": int(
                status_coverage.get("notation_item_count") or coverage_summary.get("notation_item_count") or 0
            ),
            "implicit_proof_obligation_count": int(
                status_coverage.get("implicit_proof_obligation_count")
                or coverage_summary.get("implicit_proof_obligation_count")
                or 0
            ),
        }
    )
    summary_payload["coverage_summary"] = coverage_summary
    summary_payload["all_extracted_blocks_integrated"] = bool(coverage_summary["all_extracted_blocks_integrated"])
    summary_payload["all_theorem_like_items_accounted_for"] = bool(
        coverage_summary["all_theorem_like_items_accounted_for"]
    )
    summary_payload["open_internal_cross_references"] = list(coverage_summary["open_internal_cross_references"])
    summary_payload["internal_dependency_edge_count"] = int(coverage_summary["internal_dependency_edge_count"])
    _write_json(summary_path, summary_payload)


def _harmonize_whole_paper_external_dependency_ledger(artifact_root: Path) -> None:
    payload = _load_whole_paper_terminal_status(artifact_root)
    if not payload:
        return
    proof_dependencies = dict(payload.get("proof_relevant_external_dependencies") or {})
    resolved_keys = _ordered_unique_texts(list(proof_dependencies.get("resolved") or []))
    blocked_keys = _ordered_unique_texts(list(proof_dependencies.get("blocked") or []))
    summary_path = artifact_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json"
    summary_payload = _load_optional_json(summary_path)
    integrated_children = [
        dict(item) for item in list(summary_payload.get("integrated_children") or []) if isinstance(item, Mapping)
    ]
    merged: dict[str, dict[str, Any]] = {}
    for child in integrated_children:
        child_artifact_ref = str(child.get("delivered_artifact_ref") or "").strip()
        if not child_artifact_ref:
            continue
        child_ledger = _load_optional_json(Path(child_artifact_ref) / "external_dependencies" / "EXTERNAL_DEPENDENCY_LEDGER.json")
        for dependency in list(child_ledger.get("dependencies") or []):
            if not isinstance(dependency, Mapping):
                continue
            citation_key = str(dependency.get("citation_key") or "").strip()
            if not citation_key:
                continue
            bucket = merged.setdefault(citation_key, {"citation_key": citation_key})
            _merge_dependency_entry(bucket, dependency, parent_root=artifact_root)

    for citation_key in resolved_keys:
        bucket = merged.setdefault(citation_key, {"citation_key": citation_key})
        bucket["status"] = "RESOLVED_SOURCE_EXTRACTED_AND_MAPPED"
        bucket.setdefault("proof_impact", "proof_relevant")
        bucket["why_resolved"] = _ordered_unique_texts(
            [*list(bucket.get("why_resolved") or []), "Whole-paper integration preserved this citation as resolved."]
        )
    for citation_key in blocked_keys:
        bucket = merged.setdefault(citation_key, {"citation_key": citation_key})
        if not str(bucket.get("status") or "").startswith("RESOLVED"):
            bucket["status"] = "UNRESOLVED_MISSING_LOCAL_SOURCE"
        bucket.setdefault("proof_impact", "proof_relevant")
        bucket["why_blocking"] = _ordered_unique_texts(
            [*list(bucket.get("why_blocking") or []), "Whole-paper integration preserved this proof-relevant citation as unresolved."]
        )
        if not str(bucket.get("required_next_step_if_closure_is_desired") or "").strip():
            bucket["required_next_step_if_closure_is_desired"] = "Provide a local copy of the cited paper or an exact locally stored theorem statement."

    ordered_keys = [*resolved_keys, *[key for key in blocked_keys if key not in resolved_keys]]
    ordered_keys.extend(sorted(key for key in merged if key not in ordered_keys))
    dependencies = [merged[key] for key in ordered_keys]
    ledger_payload = {
        "artifact_scope": "task",
        "workflow_scope": "whole_paper_formalization",
        "node_id": str(payload.get("node_id") or "").strip(),
        "source_tex_ref": str(payload.get("source_tex_ref") or "").strip(),
        "whole_paper_terminal_claim": str(payload.get("classification") or "").strip() or str(
            payload.get("whole_paper_terminal_classification") or ""
        ).strip().lower(),
        "summary": {
            "proof_relevant_dependency_count": len(dependencies),
            "resolved_dependency_count": len(resolved_keys),
            "blocking_dependency_count": len(blocked_keys),
            "resolved_citation_keys": resolved_keys,
            "blocking_citation_keys": blocked_keys,
        },
        "dependencies": dependencies,
    }
    _write_json(artifact_root / "external_dependencies" / "EXTERNAL_DEPENDENCY_LEDGER.json", ledger_payload)


def _sanitize_whole_paper_publication_surface(artifact_root: Path) -> None:
    payload = _load_whole_paper_terminal_status(artifact_root)
    if not payload:
        return

    status_path = artifact_root / "WHOLE_PAPER_STATUS.json"
    status_payload = _load_optional_json(status_path)
    original_source_ref = str(status_payload.get("source_tex_ref") or "").strip()
    if status_payload:
        sanitized_status = dict(_sanitize_publication_payload(status_payload, artifact_root=artifact_root))
        _write_json(status_path, sanitized_status)
        sanitized_source_ref = str(sanitized_status.get("source_tex_ref") or "").strip()
        _replace_source_tex_ref_in_textual_surface(
            artifact_root=artifact_root,
            original_ref=original_source_ref,
            sanitized_ref=sanitized_source_ref,
        )

    for relpath in (
        "analysis/WHOLE_PAPER_INTEGRATION_SUMMARY.json",
        "analysis/PAPER_DEFECT_EXPOSURE.json",
        "external_dependencies/EXTERNAL_DEPENDENCY_LEDGER.json",
        "extraction/proof_relevant_inventory.json",
    ):
        path = artifact_root / relpath
        payload = _load_optional_json(path)
        if not payload:
            continue
        sanitized_payload = dict(_sanitize_publication_payload(payload, artifact_root=artifact_root))
        _write_json(path, sanitized_payload)


def _materialize_whole_paper_integrated_child_evidence_surface(artifact_root: Path) -> None:
    payload = _load_whole_paper_terminal_status(artifact_root)
    if not payload:
        return
    summary_path = artifact_root / "analysis" / "WHOLE_PAPER_INTEGRATION_SUMMARY.json"
    summary_payload = _load_optional_json(summary_path)
    if not summary_payload:
        return
    status_path = artifact_root / "WHOLE_PAPER_STATUS.json"
    status_payload = _load_optional_json(status_path)
    integrated_children = [dict(item) for item in list(summary_payload.get("integrated_children") or []) if isinstance(item, Mapping)]
    if not integrated_children:
        return

    integrated_root = artifact_root / "integrated_children"
    if integrated_root.exists():
        shutil.rmtree(integrated_root, ignore_errors=True)
    integrated_root.mkdir(parents=True, exist_ok=True)

    replacements: dict[str, str] = {}
    materialized_children: list[dict[str, Any]] = []
    child_result_refs: list[str] = []
    child_evaluation_report_refs: list[str] = []

    for raw_child in integrated_children:
        child = dict(raw_child)
        node_id = str(child.get("node_id") or "").strip() or "integrated-child"
        child_root = integrated_root / _slug_fragment(node_id)
        child_root.mkdir(parents=True, exist_ok=True)

        delivered_artifact_ref = str(child.get("delivered_artifact_ref") or "").strip()
        result_ref = str(child.get("result_ref") or "").strip()
        evaluation_report_ref = str(child.get("evaluation_report_ref") or "").strip()
        request_ref = str(child.get("request_ref") or "").strip()
        traceability_ref = str(child.get("traceability_ref") or "").strip()

        delivered_artifact_path = Path(delivered_artifact_ref).expanduser() if delivered_artifact_ref else None
        local_artifact_root = child_root / "primary_artifact"
        local_result_path = child_root / "result.json"
        local_evaluation_report_path = child_root / "EvaluationReport.json"
        local_request_path = child_root / "EvaluatorNodeRequest.json"

        if delivered_artifact_path and delivered_artifact_path.exists():
            shutil.copytree(delivered_artifact_path, local_artifact_root, symlinks=True)
            replacements[delivered_artifact_ref] = str(local_artifact_root.resolve())
            child["delivered_artifact_ref"] = str(local_artifact_root.resolve())
            local_traceability = local_artifact_root / "TRACEABILITY.md"
            if local_traceability.exists():
                child["traceability_ref"] = str(local_traceability.resolve())
                if traceability_ref:
                    replacements[traceability_ref] = str(local_traceability.resolve())

        if result_ref:
            replacements[result_ref] = str(local_result_path.resolve())
            child["result_ref"] = str(local_result_path.resolve())
            child_result_refs.append(str(local_result_path.resolve()))

        if evaluation_report_ref:
            replacements[evaluation_report_ref] = str(local_evaluation_report_path.resolve())
            child["evaluation_report_ref"] = str(local_evaluation_report_path.resolve())
            child_evaluation_report_refs.append(str(local_evaluation_report_path.resolve()))

        if request_ref:
            replacements[request_ref] = str(local_request_path.resolve())
            child["request_ref"] = str(local_request_path.resolve())

        if result_ref:
            result_payload = _load_optional_json(Path(result_ref).expanduser())
            if result_payload:
                rewritten_result = _rewrite_exact_refs(result_payload, replacements=replacements)
                _write_json(
                    local_result_path,
                    dict(_sanitize_publication_payload(rewritten_result, artifact_root=artifact_root)),
                )

        if evaluation_report_ref:
            evaluation_report_payload = _load_optional_json(Path(evaluation_report_ref).expanduser())
            if evaluation_report_payload:
                rewritten_report = _rewrite_exact_refs(evaluation_report_payload, replacements=replacements)
                _write_json(
                    local_evaluation_report_path,
                    dict(_sanitize_publication_payload(rewritten_report, artifact_root=artifact_root)),
                )

        if request_ref:
            request_payload = _load_optional_json(Path(request_ref).expanduser())
            if request_payload:
                rewritten_request = _rewrite_exact_refs(request_payload, replacements=replacements)
                _write_json(
                    local_request_path,
                    dict(_sanitize_publication_payload(rewritten_request, artifact_root=artifact_root)),
                )

        materialized_children.append(child)

    summary_payload = dict(_rewrite_exact_refs(summary_payload, replacements=replacements))
    summary_payload["integrated_children"] = materialized_children
    _write_json(summary_path, summary_payload)

    if status_payload:
        status_payload = dict(_rewrite_exact_refs(status_payload, replacements=replacements))
        child_dependency_gate = dict(status_payload.get("child_dependency_gate") or {})
        if child_result_refs:
            child_dependency_gate["child_result_refs"] = child_result_refs
        if child_evaluation_report_refs:
            child_dependency_gate["child_evaluation_report_refs"] = child_evaluation_report_refs
        if "integrated_child_count" not in child_dependency_gate:
            child_dependency_gate["integrated_child_count"] = len(materialized_children)
        status_payload["child_dependency_gate"] = child_dependency_gate
        _write_json(status_path, status_payload)


def _artifact_root_has_materialized_content(path: Path) -> bool:
    if not path.exists():
        return False
    if path.is_file():
        try:
            return path.stat().st_size > 0
        except OSError:
            return False
    if not path.is_dir():
        return False
    try:
        for current_root, dirnames, filenames in os.walk(path):
            current_path = Path(current_root)
            if filenames:
                return True
            if any((current_path / dirname).is_symlink() for dirname in dirnames):
                return True
    except OSError:
        return False
    return False


def _terminal_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _derive_state_root_from_result_ref(result_ref: Path) -> Path:
    # expected canonical sink: <state_root>/artifacts/<node_id>/implementer_result.json
    if len(result_ref.parents) < 3:
        raise ValueError(f"implementer result ref is too shallow to derive state root: {result_ref}")
    artifacts_dir = result_ref.parents[1]
    if artifacts_dir.name != "artifacts":
        raise ValueError(f"implementer result ref must live under state_root/artifacts/<node_id>: {result_ref}")
    return result_ref.parents[2]


def _load_workspace_publication_context(workspace_root: Path, *, machine_handoff_ref: str | Path | None = None) -> dict[str, Any]:
    handoff_path = (
        _absolute(machine_handoff_ref)
        if machine_handoff_ref not in (None, "")
        else (workspace_root / "FROZEN_HANDOFF.json").resolve()
    )
    handoff = _load_optional_json(handoff_path)
    publish_ref = str(handoff.get("workspace_mirror_ref") or "").strip()
    live_ref = str(handoff.get("workspace_live_artifact_ref") or "").strip()
    receipt_ref = str(handoff.get("artifact_publication_receipt_ref") or "").strip()
    if not publish_ref or not live_ref or not receipt_ref:
        return {
            "applicable": False,
            "workspace_root": str(workspace_root.resolve()),
            "handoff_ref": str(handoff_path),
            "publish_artifact_ref": "",
            "live_artifact_ref": "",
            "publication_receipt_ref": "",
        }
    publish_path = _absolute(publish_ref)
    live_path = _absolute(live_ref)
    receipt_path = _absolute(receipt_ref)
    return {
        "applicable": True,
        "workspace_root": str(workspace_root.resolve()),
        "handoff_ref": str(handoff_path),
        "publish_artifact_ref": str(publish_path),
        "live_artifact_ref": str(live_path),
        "publication_receipt_ref": str(receipt_path),
        "publish_path": publish_path,
        "live_path": live_path,
        "receipt_path": receipt_path,
    }


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except Exception:
        return False


def _workspace_local_runtime_heavy_roots(*, workspace_root: Path, publish_path: Path, live_path: Path) -> list[Path]:
    workspace_root = workspace_root.resolve()
    publish_path = publish_path.resolve()
    live_path = live_path.resolve()
    if _is_relative_to(live_path, workspace_root):
        return []

    candidates: list[Path] = []
    for child in workspace_root.iterdir() if workspace_root.exists() else []:
        if child.name.startswith(".tmp_primary_artifact"):
            candidates.append(child.resolve())

    for heavy_name in (".lake", ".git", ".venv", ".uv-cache", "build", "_lake_build"):
        candidate = (workspace_root / heavy_name).resolve()
        if candidate.exists():
            candidates.append(candidate)

    for artifact_root in iter_workspace_artifact_roots(workspace_root):
        artifact_root = artifact_root.resolve()
        if artifact_root == publish_path:
            continue
        if any(iter_runtime_heavy_tree_paths(artifact_root)):
            candidates.append(artifact_root)

    return sorted({path for path in candidates if path.exists()}, key=lambda item: (len(item.parts), str(item)))


def inspect_workspace_local_runtime_state(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Describe whether a workspace-local node leaked runtime-owned heavy trees into the workspace.

    When the exact live artifact root is external to the workspace, any local
    `.tmp_primary_artifact*` tree or root-level runtime heavy directory under
    the workspace becomes a product defect rather than an acceptable scratch
    surface.
    """

    workspace_path = _absolute(workspace_root)
    context = _load_workspace_publication_context(workspace_path, machine_handoff_ref=machine_handoff_ref)
    if not bool(context.get("applicable")):
        return {
            **context,
            "live_artifact_root_is_external": False,
            "violation": False,
            "violation_reason": "",
            "violation_summary": "",
            "workspace_runtime_heavy_root_refs": [],
        }

    publish_path = Path(context["publish_path"]).resolve()
    live_path = Path(context["live_path"]).resolve()
    local_heavy_roots = _workspace_local_runtime_heavy_roots(
        workspace_root=workspace_path,
        publish_path=publish_path,
        live_path=live_path,
    )
    live_external = not _is_relative_to(live_path, workspace_path)
    violation_reason = ""
    violation_summary = ""
    if live_external and local_heavy_roots:
        violation_reason = "workspace_contains_local_runtime_heavy_trees"
        violation_summary = (
            "workspace-local runtime heavy trees appeared even though the frozen live artifact root is external; "
            "split-child publication/build roots are mixed again"
        )
    return {
        **{k: v for k, v in context.items() if not k.endswith("_path")},
        "live_artifact_root_is_external": live_external,
        "violation": bool(violation_reason),
        "violation_reason": violation_reason,
        "violation_summary": violation_summary,
        "workspace_runtime_heavy_root_refs": [str(item) for item in local_heavy_roots],
    }


def inspect_live_artifact_runtime_state(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Describe whether the runtime-owned live artifact root contains materialized heavy trees.

    Runtime-owned `.lake/.git/build/...` trees are only allowed as externalized
    symlink/mount surfaces. A real directory tree under the live artifact root
    means the child bypassed the runtime-owned cache contract.
    """

    workspace_path = _absolute(workspace_root)
    context = _load_workspace_publication_context(workspace_path, machine_handoff_ref=machine_handoff_ref)
    if not bool(context.get("applicable")):
        return {
            **context,
            "live_artifact_root_is_external": False,
            "violation": False,
            "violation_reason": "",
            "violation_summary": "",
            "live_exists": False,
            "live_runtime_heavy_tree_refs": [],
        }

    live_path = Path(context["live_path"]).resolve()
    live_exists = live_path.exists()
    live_external = not _is_relative_to(live_path, workspace_path)
    heavy_refs = [
        str(item)
        for item in iter_materialized_runtime_heavy_tree_paths(live_path)
    ] if live_exists and live_path.is_dir() else []
    violation_reason = ""
    violation_summary = ""
    if live_external and heavy_refs:
        violation_reason = "live_artifact_root_contains_runtime_heavy_trees"
        violation_summary = (
            "live artifact root contains materialized runtime-owned heavy trees; "
            "runtime-owned externalized cache/build mounts were bypassed"
        )
    return {
        **{k: v for k, v in context.items() if not k.endswith("_path")},
        "live_artifact_root_is_external": live_external,
        "violation": bool(violation_reason),
        "violation_reason": violation_reason,
        "violation_summary": violation_summary,
        "live_exists": live_exists,
        "live_runtime_heavy_tree_refs": heavy_refs,
    }


def inspect_workspace_publication_state(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Describe whether a workspace-local publish root has been mutated outside publication."""

    workspace_path = _absolute(workspace_root)
    context = _load_workspace_publication_context(workspace_path, machine_handoff_ref=machine_handoff_ref)
    if not bool(context.get("applicable")):
        return {
            **context,
            "violation": False,
            "violation_reason": "",
            "violation_summary": "",
            "publish_exists": False,
            "publish_has_materialized_content": False,
            "publish_runtime_heavy_tree_refs": [],
            "receipt_present": False,
        }

    publish_path = Path(context["publish_path"])
    receipt_path = Path(context["receipt_path"])
    publish_exists = publish_path.exists()
    publish_has_materialized_content = _artifact_root_has_materialized_content(publish_path)
    heavy_refs = [
        str(item)
        for item in iter_runtime_heavy_tree_paths(publish_path)
    ] if publish_exists and publish_path.is_dir() else []
    receipt_present = receipt_path.exists()
    violation_reason = ""
    violation_summary = ""

    if not receipt_present:
        if publish_has_materialized_content:
            violation_reason = "publish_root_materialized_without_publication_receipt"
            violation_summary = (
                "publish root gained materialized content before the runtime-owned publication runner wrote a receipt"
            )
    else:
        receipt_payload = _load_optional_json(receipt_path)
        try:
            validate_repo_object("LoopWorkspaceArtifactPublicationReceipt.schema.json", receipt_payload)
        except Exception as exc:
            violation_reason = "invalid_publication_receipt"
            violation_summary = f"publication receipt is invalid: {exc}"
        else:
            receipt_publish_ref = str(receipt_payload.get("publish_artifact_ref") or "").strip()
            if not publish_exists:
                violation_reason = "published_artifact_missing_after_receipt"
                violation_summary = "publication receipt exists but the published artifact root is missing"
            elif receipt_publish_ref and _absolute(receipt_publish_ref) != publish_path:
                violation_reason = "publication_receipt_publish_root_mismatch"
                violation_summary = "publication receipt does not match the current publish root"
            elif heavy_refs:
                violation_reason = "publish_root_contains_runtime_heavy_trees"
                violation_summary = "publish root contains runtime-owned heavy trees and is no longer a clean shipped artifact"
            else:
                publish_fingerprint = artifact_fingerprint(publish_path, ignore_runtime_heavy=False)
                if str(receipt_payload.get("publish_artifact_fingerprint") or "").strip() != str(
                    publish_fingerprint["fingerprint"]
                ):
                    violation_reason = "publish_root_drifted_after_publication"
                    violation_summary = "publish root changed after the last runtime-owned publication receipt"

    return {
        **{k: v for k, v in context.items() if not k.endswith("_path")},
        "violation": bool(violation_reason),
        "violation_reason": violation_reason,
        "violation_summary": violation_summary,
        "publish_exists": publish_exists,
        "publish_has_materialized_content": publish_has_materialized_content,
        "publish_runtime_heavy_tree_refs": heavy_refs,
        "receipt_present": receipt_present,
    }


def repair_publication_receipt_surface(
    *,
    publication_receipt_ref: str | Path,
    publish_artifact_ref: str | Path = "",
) -> dict[str, Any]:
    """Re-materialize a publish root from a valid live root + receipt pair when possible."""

    receipt_path = _absolute(publication_receipt_ref)
    requested_publish_ref = str(publish_artifact_ref or "").strip()
    requested_publish_path = _absolute(requested_publish_ref) if requested_publish_ref else None
    base_report: dict[str, Any] = {
        "publication_receipt_ref": str(receipt_path),
        "publish_artifact_ref": str(requested_publish_path or ""),
        "live_artifact_ref": "",
        "repair_attempted": False,
        "repair_performed": False,
        "repairable": False,
        "repair_reason": "",
        "repair_summary": "",
    }
    if not receipt_path.exists():
        return {
            **base_report,
            "repair_reason": "missing_publication_receipt",
            "repair_summary": "publication receipt is missing so publish-root repair cannot proceed",
        }
    receipt_payload = _load_optional_json(receipt_path)
    try:
        validate_repo_object("LoopWorkspaceArtifactPublicationReceipt.schema.json", receipt_payload)
    except Exception as exc:
        return {
            **base_report,
            "repair_reason": "invalid_publication_receipt",
            "repair_summary": f"publication receipt is invalid: {exc}",
        }

    receipt_publish_ref = str(receipt_payload.get("publish_artifact_ref") or "").strip()
    target_publish_path = requested_publish_path or (_absolute(receipt_publish_ref) if receipt_publish_ref else None)
    live_ref = str(receipt_payload.get("live_artifact_ref") or "").strip()
    live_path = _absolute(live_ref) if live_ref else None
    report = {
        **base_report,
        "node_id": str(receipt_payload.get("node_id") or ""),
        "publish_artifact_ref": str(target_publish_path or ""),
        "live_artifact_ref": str(live_path or ""),
    }
    if target_publish_path is None:
        return {
            **report,
            "repair_reason": "missing_publish_artifact_ref",
            "repair_summary": "publication receipt does not name a publish root to repair",
        }
    if live_path is None:
        return {
            **report,
            "repair_reason": "missing_live_artifact_ref",
            "repair_summary": "publication receipt does not name a live artifact root to repair from",
        }
    if not live_path.exists():
        return {
            **report,
            "repair_reason": "missing_live_artifact_root",
            "repair_summary": f"live artifact root is missing: {live_path}",
        }

    live_fingerprint = artifact_fingerprint(live_path, ignore_runtime_heavy=True)
    if str(receipt_payload.get("live_artifact_fingerprint") or "").strip() != str(live_fingerprint["fingerprint"]):
        return {
            **report,
            "repair_reason": "live_artifact_changed_without_republication",
            "repair_summary": "live artifact root no longer matches the publication receipt fingerprint",
        }

    publish_exists = target_publish_path.exists()
    publish_has_heavy = False
    publish_fingerprint_value = ""
    if publish_exists and target_publish_path.is_dir():
        publish_has_heavy = bool(list(iter_runtime_heavy_tree_paths(target_publish_path)))
        publish_fingerprint_value = str(
            artifact_fingerprint(target_publish_path, ignore_runtime_heavy=False).get("fingerprint") or ""
        )
    receipt_publish_fingerprint = str(receipt_payload.get("publish_artifact_fingerprint") or "").strip()
    needs_republish = (
        not publish_exists
        or publish_has_heavy
        or publish_fingerprint_value != receipt_publish_fingerprint
        or (receipt_publish_ref and _absolute(receipt_publish_ref) != target_publish_path)
    )
    if not needs_republish:
        return {
            **report,
            "repairable": True,
        }

    repair_reason = "published_artifact_missing_after_receipt"
    repair_summary = "publication receipt exists but the published artifact root is missing"
    if publish_has_heavy:
        repair_reason = "publish_root_contains_runtime_heavy_trees"
        repair_summary = "publish root contains runtime-owned heavy trees and must be republished"
    elif publish_exists and publish_fingerprint_value != receipt_publish_fingerprint:
        repair_reason = "publish_root_drifted_after_publication"
        repair_summary = "publish root changed after the last runtime-owned publication receipt"
    elif receipt_publish_ref and _absolute(receipt_publish_ref) != target_publish_path:
        repair_reason = "publication_receipt_publish_root_mismatch"
        repair_summary = "publication receipt publish root drifted from the current target publish root"

    publish_workspace_artifact_snapshot(
        node_id=str(receipt_payload.get("node_id") or target_publish_path.name),
        live_artifact_ref=live_path,
        publish_artifact_ref=target_publish_path,
        publication_receipt_ref=receipt_path,
    )
    repaired_receipt = _load_optional_json(receipt_path)
    return {
        **report,
        "repair_attempted": True,
        "repair_performed": True,
        "repairable": True,
        "repair_reason": repair_reason,
        "repair_summary": repair_summary,
        "publish_artifact_ref": str(_absolute(str(repaired_receipt.get("publish_artifact_ref") or target_publish_path))),
        "live_artifact_ref": str(_absolute(str(repaired_receipt.get("live_artifact_ref") or live_path))),
    }


def repair_workspace_publication_surface(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Repair workspace publication-surface drift when handoff + receipt still preserve truth."""

    report = inspect_workspace_publication_state(workspace_root=workspace_root, machine_handoff_ref=machine_handoff_ref)
    if not bool(report.get("applicable")):
        return {
            **report,
            "repair_attempted": False,
            "repair_performed": False,
            "repairable": False,
            "repair_reason": "",
            "repair_summary": "",
        }
    if not bool(report.get("violation")):
        return {
            **report,
            "repair_attempted": False,
            "repair_performed": False,
            "repairable": True,
            "repair_reason": "",
            "repair_summary": "",
        }
    repair_report = repair_publication_receipt_surface(
        publication_receipt_ref=str(report.get("publication_receipt_ref") or ""),
        publish_artifact_ref=str(report.get("publish_artifact_ref") or ""),
    )
    if bool(repair_report.get("repair_performed")):
        post_repair_report = inspect_workspace_publication_state(
            workspace_root=workspace_root,
            machine_handoff_ref=machine_handoff_ref,
        )
        return {
            **post_repair_report,
            "repair_attempted": True,
            "repair_performed": True,
            "repairable": True,
            "repair_reason": str(repair_report.get("repair_reason") or ""),
            "repair_summary": str(repair_report.get("repair_summary") or ""),
        }
    return {
        **report,
        "repair_attempted": bool(repair_report.get("repair_attempted")),
        "repair_performed": False,
        "repairable": bool(repair_report.get("repairable")),
        "repair_reason": str(repair_report.get("repair_reason") or report.get("violation_reason") or ""),
        "repair_summary": str(repair_report.get("repair_summary") or report.get("violation_summary") or ""),
    }


def workspace_publication_ready_for_terminal_state(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
    required_output_paths: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Return whether a workspace-local node may sync a terminal authoritative result."""

    report = repair_workspace_publication_surface(
        workspace_root=workspace_root,
        machine_handoff_ref=machine_handoff_ref,
    )
    local_runtime_report = inspect_workspace_local_runtime_state(
        workspace_root=workspace_root,
        machine_handoff_ref=machine_handoff_ref,
    )
    live_runtime_report = inspect_live_artifact_runtime_state(
        workspace_root=workspace_root,
        machine_handoff_ref=machine_handoff_ref,
    )
    if not bool(report.get("applicable")):
        return {
            **report,
            "local_runtime_violation": False,
            "local_runtime_violation_reason": "",
            "local_runtime_violation_summary": "",
            "workspace_runtime_heavy_root_refs": [],
            "live_runtime_violation": False,
            "live_runtime_violation_reason": "",
            "live_runtime_violation_summary": "",
            "live_runtime_heavy_tree_refs": [],
            "ready": True,
            "ready_reason": "",
            "ready_summary": "",
        }
    if bool(report.get("violation")):
        return {
            **report,
            "ready": False,
            "ready_reason": str(report.get("violation_reason") or ""),
            "ready_summary": str(report.get("violation_summary") or ""),
        }
    if bool(local_runtime_report.get("violation")):
        return {
            **report,
            "local_runtime_violation": True,
            "local_runtime_violation_reason": str(local_runtime_report.get("violation_reason") or ""),
            "local_runtime_violation_summary": str(local_runtime_report.get("violation_summary") or ""),
            "workspace_runtime_heavy_root_refs": list(local_runtime_report.get("workspace_runtime_heavy_root_refs") or []),
            "ready": False,
            "ready_reason": str(local_runtime_report.get("violation_reason") or ""),
            "ready_summary": str(local_runtime_report.get("violation_summary") or ""),
        }
    if bool(live_runtime_report.get("violation")):
        return {
            **report,
            "local_runtime_violation": False,
            "local_runtime_violation_reason": "",
            "local_runtime_violation_summary": "",
            "workspace_runtime_heavy_root_refs": list(local_runtime_report.get("workspace_runtime_heavy_root_refs") or []),
            "live_runtime_violation": True,
            "live_runtime_violation_reason": str(live_runtime_report.get("violation_reason") or ""),
            "live_runtime_violation_summary": str(live_runtime_report.get("violation_summary") or ""),
            "live_runtime_heavy_tree_refs": list(live_runtime_report.get("live_runtime_heavy_tree_refs") or []),
            "ready": False,
            "ready_reason": str(live_runtime_report.get("violation_reason") or ""),
            "ready_summary": str(live_runtime_report.get("violation_summary") or ""),
        }

    receipt_path = _absolute(str(report.get("publication_receipt_ref") or ""))
    receipt_payload = _load_optional_json(receipt_path)
    if not receipt_payload:
        return {
            **report,
            "ready": False,
            "ready_reason": "missing_publication_receipt",
            "ready_summary": "terminal state sync requires a runtime-owned publication receipt",
        }
    try:
        validate_repo_object("LoopWorkspaceArtifactPublicationReceipt.schema.json", receipt_payload)
    except Exception as exc:
        return {
            **report,
            "ready": False,
            "ready_reason": "invalid_publication_receipt",
            "ready_summary": f"terminal state sync requires a valid publication receipt: {exc}",
        }

    publish_path = _absolute(str(report.get("publish_artifact_ref") or ""))
    live_path = _absolute(str(report.get("live_artifact_ref") or ""))
    if not publish_path.exists():
        return {
            **report,
            "ready": False,
            "ready_reason": "missing_published_artifact",
            "ready_summary": "terminal state sync requires a published artifact root",
        }
    if not live_path.exists():
        return {
            **report,
            "ready": False,
            "ready_reason": "missing_live_artifact_root",
            "ready_summary": "terminal state sync requires the live artifact root to exist so publication freshness can be checked",
        }

    live_fingerprint = artifact_fingerprint(live_path, ignore_runtime_heavy=True)
    if str(receipt_payload.get("live_artifact_fingerprint") or "").strip() != str(live_fingerprint["fingerprint"]):
        return {
            **report,
            "ready": False,
            "ready_reason": "live_artifact_changed_without_republication",
            "ready_summary": "live artifact root changed after the last publication receipt; republish before terminalizing",
        }

    missing_required_outputs: list[str] = []
    for relpath in [str(item).strip() for item in required_output_paths if str(item).strip()]:
        if not (publish_path / relpath).exists():
            missing_required_outputs.append(relpath)
    if missing_required_outputs:
        return {
            **report,
            "ready": False,
            "ready_reason": "required_outputs_missing_from_published_artifact",
            "ready_summary": "terminal state sync requires declared required outputs in the published artifact",
            "missing_required_output_paths": missing_required_outputs,
        }

    return {
        **report,
        "local_runtime_violation": False,
        "local_runtime_violation_reason": "",
        "local_runtime_violation_summary": "",
        "workspace_runtime_heavy_root_refs": list(local_runtime_report.get("workspace_runtime_heavy_root_refs") or []),
        "live_runtime_violation": False,
        "live_runtime_violation_reason": "",
        "live_runtime_violation_summary": "",
        "live_runtime_heavy_tree_refs": list(live_runtime_report.get("live_runtime_heavy_tree_refs") or []),
        "ready": True,
        "ready_reason": "",
        "ready_summary": "",
    }


def reset_workspace_publish_root(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Remove an invalid published mirror so same-node recovery can restart from a clean publish root."""

    report = inspect_workspace_publication_state(workspace_root=workspace_root, machine_handoff_ref=machine_handoff_ref)
    publish_ref = str(report.get("publish_artifact_ref") or "").strip()
    receipt_ref = str(report.get("publication_receipt_ref") or "").strip()
    removed_publish_root = False
    removed_publication_receipt = False
    if publish_ref:
        publish_path = _absolute(publish_ref)
        if publish_path.exists():
            if publish_path.is_dir():
                shutil.rmtree(publish_path)
            else:
                publish_path.unlink()
            removed_publish_root = True
    if receipt_ref:
        receipt_path = _absolute(receipt_ref)
        if receipt_path.exists():
            receipt_path.unlink()
            removed_publication_receipt = True
    return {
        **report,
        "removed_publish_root": removed_publish_root,
        "removed_publication_receipt": removed_publication_receipt,
    }


def reset_workspace_local_runtime_heavy_roots(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Remove invalid workspace-local heavy trees when the frozen live root is external."""

    report = inspect_workspace_local_runtime_state(workspace_root=workspace_root, machine_handoff_ref=machine_handoff_ref)
    removed_roots: list[str] = []
    removed_staging_roots: list[str] = []
    workspace_path = _absolute(workspace_root)
    if bool(report.get("live_artifact_root_is_external")):
        for root_ref in list(report.get("workspace_runtime_heavy_root_refs") or []):
            candidate = _absolute(str(root_ref))
            if not candidate.exists():
                continue
            if candidate.is_dir():
                shutil.rmtree(candidate)
            else:
                candidate.unlink()
            removed_roots.append(str(candidate))
        for staging_root in iter_workspace_publication_staging_roots(workspace_path):
            if not staging_root.exists():
                continue
            shutil.rmtree(staging_root, ignore_errors=True)
            if not staging_root.exists():
                removed_staging_roots.append(str(staging_root))
    return {
        **report,
        "removed_workspace_runtime_heavy_root_refs": removed_roots,
        "removed_workspace_publication_staging_root_refs": removed_staging_roots,
    }


def reset_live_artifact_runtime_heavy_roots(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Remove materialized runtime heavy trees from the runtime-owned live artifact root."""

    report = inspect_live_artifact_runtime_state(workspace_root=workspace_root, machine_handoff_ref=machine_handoff_ref)
    removed_roots: list[str] = []
    for root_ref in list(report.get("live_runtime_heavy_tree_refs") or []):
        candidate = _absolute(str(root_ref))
        if not candidate.exists():
            continue
        remove_runtime_heavy_tree(candidate)
        removed_roots.append(str(candidate))
    return {
        **report,
        "removed_live_runtime_heavy_tree_refs": removed_roots,
    }


def _first_nonempty(*values: str) -> str:
    for value in values:
        stripped = str(value or "").strip()
        if stripped:
            return stripped
    return ""


def _extract_evaluator_projection(payload: dict[str, Any]) -> dict[str, Any]:
    projected = dict(payload.get("evaluator_result") or {})
    if projected:
        return projected
    nested = dict(payload.get("evaluator") or {})
    verdict = str(nested.get("verdict") or "").strip()
    if not verdict:
        return {}
    evidence_refs = [
        item
        for item in (
            str(nested.get("request_ref") or "").strip(),
            str(nested.get("evaluation_report_ref") or "").strip(),
            str(nested.get("reviewer_response_ref") or "").strip(),
        )
        if item
    ]
    projected = {"verdict": verdict}
    if evidence_refs:
        projected["evidence_refs"] = evidence_refs
    return projected


def _eligible_for_publication_ready(payload: dict[str, Any], *, source_ref: str, target_ref: str, owner: str) -> bool:
    if str(payload.get("status") or "").strip() != "COMPLETED":
        return False
    if owner != "root-kernel":
        return False
    if not source_ref or not target_ref:
        return False
    verdict = str(_extract_evaluator_projection(payload).get("verdict") or "").strip()
    if verdict not in _TERMINAL_EVALUATOR_VERDICTS:
        return False
    if bool(payload.get("delivered_artifact_exactly_evaluated")):
        return True
    workspace_mirror_ref = str(payload.get("workspace_mirror_ref") or "").strip()
    if workspace_mirror_ref and workspace_mirror_ref == source_ref:
        return True
    return False


def _normalize_implementer_result_for_publication(*, result_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    source_ref = _first_nonempty(
        str(normalized.get("delivered_artifact_ref") or ""),
        *[str(item) for item in list(normalized.get("publish_ready_artifact_refs") or [])],
        str(normalized.get("workspace_mirror_ref") or ""),
    )
    if source_ref and not str(normalized.get("delivered_artifact_ref") or "").strip():
        normalized["delivered_artifact_ref"] = source_ref
    if source_ref:
        ready_refs = [str(item).strip() for item in list(normalized.get("publish_ready_artifact_refs") or []) if str(item).strip()]
        if not ready_refs:
            normalized["publish_ready_artifact_refs"] = [source_ref]
    projected = _extract_evaluator_projection(normalized)
    if projected and not dict(normalized.get("evaluator_result") or {}):
        normalized["evaluator_result"] = projected
    if not str(normalized.get("result_ref") or "").strip():
        normalized["result_ref"] = str(result_path.resolve())
    if not str(normalized.get("result_kind") or "").strip():
        normalized["result_kind"] = "implementer_result"
    target_ref = str(normalized.get("external_publish_target") or "").strip()
    owner = str(normalized.get("external_publication_owner") or "").strip()
    if not str(normalized.get("outcome") or "").strip() and _eligible_for_publication_ready(
        normalized,
        source_ref=source_ref,
        target_ref=target_ref,
        owner=owner,
    ):
        normalized["outcome"] = "READY_FOR_PUBLICATION"
    if normalized != payload:
        _write_json(result_path, normalized)
    return normalized


def publish_external_from_implementer_result(*, result_ref: str | Path) -> dict[str, Any]:
    """Mechanically publish an implementer-owned workspace artifact to the frozen external target."""

    result_path = _absolute(result_ref)
    if not result_path.exists():
        raise ValueError(f"implementer result ref does not exist: {result_path}")
    payload = _normalize_implementer_result_for_publication(
        result_path=result_path,
        payload=json.loads(result_path.read_text(encoding="utf-8")),
    )

    node_id = str(payload.get("node_id") or "").strip()
    status = str(payload.get("status") or "").strip()
    outcome = str(payload.get("outcome") or "").strip()
    owner = str(payload.get("external_publication_owner") or "").strip()
    source_ref = str(payload.get("delivered_artifact_ref") or "").strip()
    if not source_ref:
        refs = [str(item).strip() for item in list(payload.get("publish_ready_artifact_refs") or []) if str(item).strip()]
        source_ref = refs[0] if refs else ""
    target_ref = str(payload.get("external_publish_target") or "").strip()

    if not node_id:
        raise ValueError("implementer result must carry node_id")
    if status != "COMPLETED":
        raise ValueError(f"external publication requires implementer_result status=COMPLETED, got {status!r}")
    if outcome != "READY_FOR_PUBLICATION":
        raise ValueError(
            f"external publication requires implementer_result outcome=READY_FOR_PUBLICATION, got {outcome!r}"
        )
    if owner != "root-kernel":
        raise ValueError(f"external publication requires external_publication_owner=root-kernel, got {owner!r}")
    if not source_ref or not target_ref:
        raise ValueError("external publication requires both delivered_artifact_ref and external_publish_target")

    source_path = _absolute(source_ref)
    target_path = _absolute(target_ref)
    if not source_path.exists():
        raise ValueError(f"publication source artifact does not exist: {source_path}")

    target_path.parent.mkdir(parents=True, exist_ok=True)
    with source_path.open("rb") as source_handle, tempfile.NamedTemporaryFile(
        "wb",
        dir=target_path.parent,
        prefix=f".{target_path.stem}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        byte_count = 0
        for chunk in iter(lambda: source_handle.read(1024 * 1024), b""):
            handle.write(chunk)
            byte_count += len(chunk)
        temp_path = Path(handle.name)
    os.replace(temp_path, target_path)

    source_sha256 = _sha256(source_path)
    target_sha256 = _sha256(target_path)
    if source_sha256 != target_sha256:
        raise ValueError("external publication copy integrity failed: source/target hashes differ")

    state_root = _derive_state_root_from_result_ref(result_path)
    publication_result_ref = state_root / "artifacts" / "publication" / node_id / "ExternalPublicationResult.json"
    publication_payload = {
        "node_id": node_id,
        "published": True,
        "source_ref": str(source_path),
        "target_ref": str(target_path),
        "source_sha256": source_sha256,
        "target_sha256": target_sha256,
        "bytes_copied": byte_count,
        "publication_result_ref": str(publication_result_ref),
    }
    validate_repo_object("LoopExternalPublicationResult.schema.json", publication_payload)
    _write_json(publication_result_ref, publication_payload)
    return publication_payload


def publish_workspace_artifact_snapshot(
    *,
    node_id: str,
    live_artifact_ref: str | Path,
    publish_artifact_ref: str | Path,
    publication_receipt_ref: str | Path,
) -> dict[str, Any]:
    """Publish a sanitized snapshot from a live build root into the shipped workspace artifact root."""

    node_id_text = str(node_id or "").strip()
    if not node_id_text:
        raise ValueError("workspace artifact publication requires node_id")
    live_path = _absolute(live_artifact_ref)
    publish_path = _absolute(publish_artifact_ref)
    receipt_path = _absolute(publication_receipt_ref)
    if not live_path.exists():
        raise ValueError(f"workspace artifact publication source does not exist: {live_path}")
    if live_path.is_dir():
        live_heavy_refs = [str(item) for item in iter_materialized_runtime_heavy_tree_paths(live_path)]
        live_parent = live_path.parent.resolve()
        live_is_workspace_local = publish_path.resolve().is_relative_to(live_parent)
        if live_heavy_refs and not live_is_workspace_local:
            raise ValueError(
                "workspace artifact publication source contains materialized runtime-owned heavy trees: "
                + ", ".join(live_heavy_refs)
            )
        _harmonize_whole_paper_terminal_control_surface(live_path)
        _harmonize_whole_paper_integration_summary_surface(live_path)
        _harmonize_whole_paper_proof_relevant_inventory_surface(live_path)
        _harmonize_whole_paper_external_dependency_ledger(live_path)
        _harmonize_split_continuation_control_surface(live_path)
        _harmonize_slice_local_completion_control_surface(live_path)

    publish_parent = publish_path.parent
    publish_parent.mkdir(parents=True, exist_ok=True)
    removed_heavy_trees: list[str] = []

    if live_path.is_dir():
        temp_dir = Path(
            tempfile.mkdtemp(
                dir=str(publish_parent),
                prefix=f".{publish_path.name}.publish.",
            )
        )
        try:
            staged_root = temp_dir / publish_path.name
            # Preserve externalized runtime-heavy symlink identity during the
            # staging copy so canonicalization can prune those mounts cheaply
            # instead of materializing multi-GB trees into the temp publish root.
            shutil.copytree(live_path, staged_root, symlinks=True)
            removed_heavy_trees = canonicalize_directory_artifact_heavy_trees(staged_root)
            _harmonize_whole_paper_terminal_control_surface(staged_root)
            _harmonize_whole_paper_integration_summary_surface(staged_root)
            _harmonize_whole_paper_proof_relevant_inventory_surface(staged_root)
            _harmonize_whole_paper_external_dependency_ledger(staged_root)
            _materialize_whole_paper_integrated_child_evidence_surface(staged_root)
            _sanitize_whole_paper_publication_surface(staged_root)
            _harmonize_split_continuation_control_surface(staged_root)
            _harmonize_slice_local_completion_control_surface(staged_root)
            if publish_path.exists():
                if publish_path.is_dir():
                    shutil.rmtree(publish_path)
                else:
                    publish_path.unlink()
            os.replace(staged_root, publish_path)
        finally:
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)
    else:
        with live_path.open("rb") as source_handle, tempfile.NamedTemporaryFile(
            "wb",
            dir=publish_parent,
            prefix=f".{publish_path.stem}.publish.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            for chunk in iter(lambda: source_handle.read(1024 * 1024), b""):
                handle.write(chunk)
            temp_path = Path(handle.name)
        os.replace(temp_path, publish_path)

    live_fingerprint = artifact_fingerprint(live_path, ignore_runtime_heavy=True)
    published_fingerprint = artifact_fingerprint(publish_path, ignore_runtime_heavy=False)
    publication_payload = {
        "node_id": node_id_text,
        "published_at_utc": _terminal_timestamp(),
        "live_artifact_ref": str(live_path),
        "publish_artifact_ref": str(publish_path),
        "live_artifact_kind": str(live_fingerprint["artifact_kind"]),
        "publish_artifact_kind": str(published_fingerprint["artifact_kind"]),
        "live_artifact_fingerprint": str(live_fingerprint["fingerprint"]),
        "publish_artifact_fingerprint": str(published_fingerprint["fingerprint"]),
        "removed_runtime_heavy_trees": [str(item) for item in removed_heavy_trees],
        "publication_receipt_ref": str(receipt_path),
    }
    validate_repo_object("LoopWorkspaceArtifactPublicationReceipt.schema.json", publication_payload)
    _write_json(receipt_path, publication_payload)
    return publication_payload
