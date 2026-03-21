#!/usr/bin/env python3
"""Publish a sanitized workspace artifact snapshot from frozen handoff context."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from loop_product.dispatch.publication import publish_workspace_artifact_snapshot


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object in {path}")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish the workspace artifact snapshot named in a frozen handoff.")
    parser.add_argument("--handoff-ref", required=True)
    args = parser.parse_args()

    handoff_ref = Path(args.handoff_ref).expanduser().resolve()
    handoff = _load_json(handoff_ref)
    node_id = str(handoff.get("node_id") or "").strip()
    live_ref = str(handoff.get("workspace_live_artifact_ref") or "").strip()
    publish_ref = str(handoff.get("workspace_mirror_ref") or "").strip()
    receipt_ref = str(handoff.get("artifact_publication_receipt_ref") or "").strip()
    if not node_id or not live_ref or not publish_ref or not receipt_ref:
        raise ValueError(
            "handoff must include node_id, workspace_live_artifact_ref, workspace_mirror_ref, and artifact_publication_receipt_ref"
        )
    payload = publish_workspace_artifact_snapshot(
        node_id=node_id,
        live_artifact_ref=live_ref,
        publish_artifact_ref=publish_ref,
        publication_receipt_ref=receipt_ref,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
