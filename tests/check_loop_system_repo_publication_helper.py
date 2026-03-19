#!/usr/bin/env python3
"""Validate the committed root-kernel external publication helper."""

from __future__ import annotations

import hashlib
import json
import shutil
import sys
import tempfile
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-publication-helper][FAIL] {msg}", file=sys.stderr)
    return 2


def _load_schema(name: str) -> dict[str, object]:
    path = ROOT / "docs" / "schemas" / name
    if not path.exists():
        raise FileNotFoundError(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(data)
    return data


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_publication_success(
    *,
    publish_external_from_implementer_result,
    result_ref: Path,
    source: Path,
    target: Path,
    result_schema: dict[str, object],
) -> int:
    published = publish_external_from_implementer_result(result_ref=result_ref)
    Draft202012Validator(result_schema).validate(published)

    if not target.exists():
        return _fail("publication helper must materialize the external target")
    if target.read_text(encoding="utf-8") != source.read_text(encoding="utf-8"):
        return _fail("publication helper must copy the exact source bytes to the external target")
    if published.get("published") is not True:
        return _fail("publication helper must report published=true on success")
    if str(published.get("source_ref") or "") != str(source.resolve()):
        return _fail("publication helper must report the exact source artifact ref")
    if str(published.get("target_ref") or "") != str(target.resolve()):
        return _fail("publication helper must report the exact external publish target")
    if str(published.get("source_sha256") or "") != _sha256(source):
        return _fail("publication helper must report the exact source sha256")
    if str(published.get("target_sha256") or "") != _sha256(target):
        return _fail("publication helper must report the exact target sha256")
    if str(published.get("source_sha256") or "") != str(published.get("target_sha256") or ""):
        return _fail("publication helper must verify source/target hashes match")

    publication_result_ref = Path(str(published.get("publication_result_ref") or ""))
    if not publication_result_ref.exists():
        return _fail("publication helper must persist a durable publication result artifact")
    return 0


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        import loop_product as loop_product_package
        import loop_product.runtime as runtime_surface
    except Exception as exc:  # noqa: BLE001
        return _fail(f"publication helper imports failed: {exc}")

    if not hasattr(runtime_surface, "publish_external_from_implementer_result"):
        return _fail("loop_product.runtime must expose publish_external_from_implementer_result")
    if not hasattr(loop_product_package, "publish_external_from_implementer_result"):
        return _fail("loop_product package root must expose publish_external_from_implementer_result")

    try:
        result_schema = _load_schema("LoopExternalPublicationResult.schema.json")
    except Exception as exc:  # noqa: BLE001
        return _fail(f"publication result schema missing or invalid: {exc}")

    publish_external_from_implementer_result = runtime_surface.publish_external_from_implementer_result

    with tempfile.TemporaryDirectory(prefix="loop_publication_helper_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop" / "demo"
        workspace_root = temp_root / "workspace" / "demo"
        source = workspace_root / "deliverables" / "poster.html"
        target = temp_root / "Desktop" / "poster.html"
        result_ref = state_root / "artifacts" / "demo-node" / "implementer_result.json"

        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("<html><body>poster</body></html>\n", encoding="utf-8")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("stale\n", encoding="utf-8")
        result_ref.parent.mkdir(parents=True, exist_ok=True)
        result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": "demo-node",
                    "status": "COMPLETED",
                    "outcome": "READY_FOR_PUBLICATION",
                    "delivered_artifact_ref": str(source.resolve()),
                    "publish_ready_artifact_refs": [str(source.resolve())],
                    "external_publish_target": str(target.resolve()),
                    "external_publication_owner": "root-kernel",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

        explicit_rc = _assert_publication_success(
            publish_external_from_implementer_result=publish_external_from_implementer_result,
            result_ref=result_ref,
            source=source,
            target=target,
            result_schema=result_schema,
        )
        if explicit_rc != 0:
            return explicit_rc
        publication_result_ref = state_root / "artifacts" / "publication" / "demo-node" / "ExternalPublicationResult.json"
        if state_root not in publication_result_ref.parents:
            return _fail("publication helper must persist its result under the owning state root")

        normalized_target = temp_root / "Desktop" / "poster-normalized.html"
        normalized_result_ref = state_root / "artifacts" / "normalized-node" / "implementer_result.json"
        normalized_result_ref.parent.mkdir(parents=True, exist_ok=True)
        normalized_result_ref.write_text(
            json.dumps(
                {
                    "schema": "loop_product.implementer_result",
                    "schema_version": "0.1.0",
                    "node_id": "normalized-node",
                    "status": "COMPLETED",
                    "summary": "Evaluator terminal verdict already exists in nested form.",
                    "delivered_artifact_ref": str(source.resolve()),
                    "workspace_mirror_ref": str(source.resolve()),
                    "delivered_artifact_exactly_evaluated": True,
                    "external_publish_target": str(normalized_target.resolve()),
                    "external_publication_owner": "root-kernel",
                    "evaluator": {
                        "verdict": "BUG",
                        "request_ref": str((temp_root / "req.json").resolve()),
                        "evaluation_report_ref": str((temp_root / "EvaluationReport.json").resolve()),
                        "reviewer_response_ref": str((temp_root / "reviewer.txt").resolve()),
                    },
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        normalized_rc = _assert_publication_success(
            publish_external_from_implementer_result=publish_external_from_implementer_result,
            result_ref=normalized_result_ref,
            source=source,
            target=normalized_target,
            result_schema=result_schema,
        )
        if normalized_rc != 0:
            return normalized_rc
        normalized_payload = json.loads(normalized_result_ref.read_text(encoding="utf-8"))
        if str(normalized_payload.get("outcome") or "") != "READY_FOR_PUBLICATION":
            return _fail("publication helper must normalize a terminal evaluator-backed implementer result to outcome=READY_FOR_PUBLICATION")
        ready_refs = [str(item) for item in list(normalized_payload.get("publish_ready_artifact_refs") or []) if str(item).strip()]
        if ready_refs != [str(source.resolve())]:
            return _fail("publication helper must persist publish_ready_artifact_refs when normalizing implementer results")
        projected = dict(normalized_payload.get("evaluator_result") or {})
        if str(projected.get("verdict") or "") != "BUG":
            return _fail("publication helper must project nested terminal evaluator verdicts into evaluator_result during normalization")
        if str(normalized_payload.get("result_ref") or "") != str(normalized_result_ref.resolve()):
            return _fail("publication helper must persist result_ref during normalization")
        if str(normalized_payload.get("result_kind") or "") != "implementer_result":
            return _fail("publication helper must persist result_kind during normalization")

        wrapper_script = ROOT / "scripts" / "publish_external_from_result.sh"
        if not wrapper_script.exists():
            return _fail("repo must ship a wrapper for the publication helper")
        wrapper_text = wrapper_script.read_text(encoding="utf-8")
        for snippet in (
            "uv run --locked python -m loop_product.runtime.lifecycle publish-external-result",
            "--result-ref",
        ):
            if snippet not in wrapper_text:
                return _fail(f"publication wrapper missing required snippet {snippet!r}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
