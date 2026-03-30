#!/usr/bin/env python3
"""Validate committed authority mirroring for the housekeeping reap controller."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-housekeeping-controller-authority-mirror][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _persist_node(state_root: Path, node_id: str, *, status: str, workspace_root: Path) -> None:
    _write_json(
        state_root / "state" / f"{node_id}.json",
        {
            "node_id": node_id,
            "status": status,
            "workspace_root": str(workspace_root.resolve()),
            "runtime_state": {},
        },
    )


def _wait_until(predicate, *, timeout_s: float, interval_s: float = 0.05) -> bool:
    deadline = time.time() + max(0.0, float(timeout_s))
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return bool(predicate())


def _cleanup_repo_services(repo_root: Path) -> None:
    from loop_product.runtime import cleanup_test_repo_services

    try:
        cleanup_test_repo_services(repo_root=repo_root, settle_timeout_s=4.0, poll_interval_s=0.05)
    except Exception:
        pass


def main() -> int:
    from loop_product.event_journal import iter_committed_events
    from loop_product.kernel import query_runtime_liveness_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_controller_authority_mirror_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "stopped-run"
        workspace_root = repo_root / "workspace" / "stopped-run"
        (state_root / "state").mkdir(parents=True, exist_ok=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(state_root, "stopped-run", status="BLOCKED", workspace_root=workspace_root)

        owned_proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(state_root.resolve()),
                str(workspace_root.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        controller_pid = 0
        try:
            request = gc_module.submit_stopped_run_reap_request(state_root=state_root, repo_root=repo_root)
            controller_pid = int(request.get("controller_pid") or 0)
            if controller_pid <= 0:
                return _fail("request-triggered startup must still produce a controller pid")

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            controller_node_id = gc_module.HOUSEKEEPING_REAP_CONTROLLER_NODE_ID

            if not _wait_until(
                lambda: bool(
                    dict(query_runtime_liveness_view(housekeeping_root).get("effective_runtime_liveness_by_node") or {}).get(
                        controller_node_id
                    )
                ),
                timeout_s=4.0,
            ):
                return _fail("housekeeping controller startup must become visible in committed runtime liveness authority")

            liveness_view = query_runtime_liveness_view(housekeeping_root)
            effective = dict(liveness_view.get("effective_runtime_liveness_by_node") or {}).get(controller_node_id) or {}
            if str(effective.get("effective_attachment_state") or "") != "ATTACHED":
                return _fail("housekeeping controller authority mirror must report ATTACHED while the controller is alive")
            if int(dict(effective.get("payload") or {}).get("pid") or 0) != controller_pid:
                return _fail("controller liveness payload must preserve controller pid identity")

            if not _wait_until(
                lambda: bool(
                    [
                        event
                        for event in iter_committed_events(housekeeping_root)
                        if str(event.get("event_type") or "") == "process_identity_confirmed"
                        and str(event.get("node_id") or "") == controller_node_id
                    ]
                ),
                timeout_s=3.0,
            ):
                return _fail("housekeeping controller startup must commit process_identity_confirmed")

            identity_events = [
                event
                for event in iter_committed_events(housekeeping_root)
                if str(event.get("event_type") or "") == "process_identity_confirmed"
                and str(event.get("node_id") or "") == controller_node_id
            ]
            if not identity_events:
                return _fail("housekeeping controller startup must commit process_identity_confirmed")
            identity_payload = dict(identity_events[-1].get("payload") or {})
            if int(identity_payload.get("pid") or 0) != controller_pid:
                return _fail("process_identity_confirmed must preserve controller pid identity")
            if not str(identity_payload.get("process_fingerprint") or ""):
                return _fail("process_identity_confirmed must preserve controller process fingerprint")

            baseline_count = int(liveness_view.get("runtime_liveness_event_count") or 0)
            if not _wait_until(
                lambda: int(query_runtime_liveness_view(housekeeping_root).get("runtime_liveness_event_count") or 0) > baseline_count,
                timeout_s=3.0,
            ):
                return _fail("live housekeeping controller must refresh committed liveness while it remains running")
        finally:
            _cleanup_repo_services(repo_root)
            if owned_proc.poll() is None:
                owned_proc.kill()
                owned_proc.wait(timeout=1.0)

    print("[loop-system-housekeeping-controller-authority-mirror] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
