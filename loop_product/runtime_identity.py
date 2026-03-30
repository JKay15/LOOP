"""Durable per-runtime-root identity helpers."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from loop_product.runtime_paths import require_runtime_root
from loop_product.state_io import write_json_read_only


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def runtime_root_identity_ref(state_root: str | Path) -> Path:
    resolved = require_runtime_root(Path(state_root).expanduser().resolve())
    return (resolved / "state" / "RuntimeRootIdentity.json").resolve()


def read_runtime_root_identity(state_root: str | Path) -> dict[str, Any]:
    ref = runtime_root_identity_ref(state_root)
    if not ref.exists():
        return {}
    try:
        payload = json.loads(ref.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not str(payload.get("runtime_identity_token") or "").strip():
        return {}
    return dict(payload)


def ensure_runtime_root_identity(state_root: str | Path) -> dict[str, Any]:
    resolved = require_runtime_root(Path(state_root).expanduser().resolve())
    existing = read_runtime_root_identity(resolved)
    if existing:
        return existing
    payload = {
        "schema": "loop_product.runtime_root_identity",
        "state_root": str(resolved),
        "runtime_identity_token": uuid4().hex,
        "created_at_utc": _now_iso(),
    }
    ref = runtime_root_identity_ref(resolved)
    ref.parent.mkdir(parents=True, exist_ok=True)
    write_json_read_only(ref, payload)
    return payload


def reseed_runtime_root_identity(state_root: str | Path) -> dict[str, Any]:
    resolved = require_runtime_root(Path(state_root).expanduser().resolve())
    previous = read_runtime_root_identity(resolved)
    payload = {
        "schema": "loop_product.runtime_root_identity",
        "state_root": str(resolved),
        "runtime_identity_token": uuid4().hex,
        "created_at_utc": _now_iso(),
    }
    previous_token = str(previous.get("runtime_identity_token") or "").strip()
    if previous_token:
        payload["supersedes_runtime_identity_token"] = previous_token
    ref = runtime_root_identity_ref(resolved)
    ref.parent.mkdir(parents=True, exist_ok=True)
    write_json_read_only(ref, payload)
    return payload


def runtime_root_identity_token(state_root: str | Path, *, ensure: bool = False) -> str:
    payload = ensure_runtime_root_identity(state_root) if ensure else read_runtime_root_identity(state_root)
    return str(payload.get("runtime_identity_token") or "")
