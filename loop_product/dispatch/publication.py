"""Deterministic root-kernel publication helper for external publish targets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from loop_product.artifact_hygiene import (
    artifact_fingerprint,
    canonicalize_directory_artifact_heavy_trees,
    iter_workspace_artifact_roots,
    iter_runtime_heavy_tree_paths,
)
from loop_product.protocols.schema import validate_repo_object

_TERMINAL_EVALUATOR_VERDICTS = {"PASS", "FAIL", "STRUCTURED_EXCEPTION", "BUG", "STUCK", "ERROR"}


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


def _load_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


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


def workspace_publication_ready_for_terminal_state(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
    required_output_paths: list[str] | tuple[str, ...] = (),
) -> dict[str, Any]:
    """Return whether a workspace-local node may sync a terminal authoritative result."""

    report = inspect_workspace_publication_state(workspace_root=workspace_root, machine_handoff_ref=machine_handoff_ref)
    local_runtime_report = inspect_workspace_local_runtime_state(
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
    removed_publish_root = False
    if publish_ref:
        publish_path = _absolute(publish_ref)
        if publish_path.exists():
            if publish_path.is_dir():
                shutil.rmtree(publish_path)
            else:
                publish_path.unlink()
            removed_publish_root = True
    return {
        **report,
        "removed_publish_root": removed_publish_root,
    }


def reset_workspace_local_runtime_heavy_roots(
    *,
    workspace_root: str | Path,
    machine_handoff_ref: str | Path | None = None,
) -> dict[str, Any]:
    """Remove invalid workspace-local heavy trees when the frozen live root is external."""

    report = inspect_workspace_local_runtime_state(workspace_root=workspace_root, machine_handoff_ref=machine_handoff_ref)
    removed_roots: list[str] = []
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
    return {
        **report,
        "removed_workspace_runtime_heavy_root_refs": removed_roots,
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
            shutil.copytree(live_path, staged_root)
            removed_heavy_trees = canonicalize_directory_artifact_heavy_trees(staged_root)
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
