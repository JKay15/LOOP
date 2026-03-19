"""Deterministic root-kernel publication helper for external publish targets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

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


def _derive_state_root_from_result_ref(result_ref: Path) -> Path:
    # expected canonical sink: <state_root>/artifacts/<node_id>/implementer_result.json
    if len(result_ref.parents) < 3:
        raise ValueError(f"implementer result ref is too shallow to derive state root: {result_ref}")
    artifacts_dir = result_ref.parents[1]
    if artifacts_dir.name != "artifacts":
        raise ValueError(f"implementer result ref must live under state_root/artifacts/<node_id>: {result_ref}")
    return result_ref.parents[2]


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
