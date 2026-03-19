"""Accept normalized control envelopes into the audit trail."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import ValidationError

from loop_product.gateway.classify import classify_envelope
from loop_product.protocols.control_envelope import ControlEnvelope
from loop_product.protocols.schema import validate_repo_object
from loop_product.runtime_paths import require_runtime_root


def accept_control_envelope(state_root: Path, envelope: ControlEnvelope) -> ControlEnvelope:
    """Persist an accepted envelope into the audit tree."""

    state_root = require_runtime_root(state_root)
    if envelope.status.value == "rejected":
        rejected = replace(envelope.with_identity(), classification="rejected")
        _write_audit_record(state_root, rejected)
        return rejected

    accepted = replace(
        envelope.with_identity().accepted(),
        classification=envelope.classification or classify_envelope(envelope),
        accepted_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    )
    try:
        validate_repo_object("LoopSystemControlEnvelope.schema.json", accepted.to_dict())
    except ValidationError as exc:
        rejected = replace(
            envelope.rejected(f"control envelope validation failed during accept: {exc.message}").with_identity(),
            classification="rejected",
        )
        _write_audit_record(state_root, rejected)
        return rejected

    _write_audit_record(state_root, accepted)
    return accepted


def _write_audit_record(state_root: Path, envelope: ControlEnvelope) -> Path:
    state_root = require_runtime_root(state_root)
    audit_dir = state_root / "audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    path = audit_dir / f"envelope_{stamp}_{envelope.envelope_type}.json"
    path.write_text(json.dumps(envelope.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
