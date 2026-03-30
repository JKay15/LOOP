#!/usr/bin/env python3
"""Validate the trusted reap reconciler consumes committed canonical reap facts."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-reap-reconciler-foundation][FAIL] {msg}", file=sys.stderr)
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


def main() -> int:
    from loop_product.event_journal import commit_reap_requested_event, commit_runtime_reap_requested_event
    from loop_product.kernel import query_operational_hygiene_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_reap_reconciler_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        stopped_state_root = repo_root / ".loop" / "stopped-run"
        stopped_workspace_root = repo_root / "workspace" / "stopped-run"
        (stopped_state_root / "state").mkdir(parents=True, exist_ok=True)
        stopped_workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(stopped_state_root, "stopped-run", status="BLOCKED", workspace_root=stopped_workspace_root)

        lingering_proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
                str(stopped_state_root.resolve()),
                str(stopped_workspace_root.resolve()),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        try:
            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            payload = {
                "runtime_name": "stopped-run",
                "state_root": str(stopped_state_root.resolve()),
                "workspace_root": str(stopped_workspace_root.resolve()),
                "owned_processes": [],
            }
            commit_reap_requested_event(
                housekeeping_root,
                request_id="stopped-run:req-001",
                node_id="stopped-run",
                payload=payload,
                producer="test.reap_reconciler_foundation",
            )
            commit_runtime_reap_requested_event(
                housekeeping_root,
                request_id="stopped-run:req-001",
                node_id="stopped-run",
                payload=payload,
                producer="test.reap_reconciler_foundation",
            )

            results = gc_module.reconcile_housekeeping_reap_requests(repo_root=repo_root, runtime_name="stopped-run")
            if len(results) != 1:
                return _fail("reconciler must report exactly one stopped-run result")
            result = dict(results[0])
            if str(result.get("execution_owner") or "") != "reap_reconciler":
                return _fail("reconciler result must record reap_reconciler as execution owner")
            if str(result.get("outcome") or "") != "confirmed":
                return _fail("reconciler must confirm a reapable stopped run")
            if not result.get("process_reap_confirmed_event_id"):
                return _fail("reconciler result must report canonical process_reap_confirmed event id")
            if not result.get("runtime_root_quiesced_event_id"):
                return _fail("reconciler result must report canonical runtime_root_quiesced event id")
            if stopped_state_root.exists():
                return _fail("confirmed reconcile must retire the stopped state root")

            hygiene = query_operational_hygiene_view(
                housekeeping_root,
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("reap_requested_event_count") or 0) != 1:
                return _fail("canonical reap_requested fact must remain visible after reconcile")
            if int(hygiene.get("process_reap_confirmed_event_count") or 0) != 1:
                return _fail("reconciler must commit one canonical process_reap_confirmed fact")
            if int(hygiene.get("runtime_root_quiesced_event_count") or 0) != 1:
                return _fail("reconciler must commit one canonical runtime_root_quiesced fact")

            deadline = time.time() + 3.0
            while time.time() < deadline and lingering_proc.poll() is None:
                time.sleep(0.05)
            if lingering_proc.poll() is None:
                return _fail("reconciler must terminate the lingering owned process")
        finally:
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

        blocked_state_root = repo_root / ".loop" / "live-run"
        blocked_workspace_root = repo_root / "workspace" / "live-run"
        (blocked_state_root / "state").mkdir(parents=True, exist_ok=True)
        blocked_workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(blocked_state_root, "live-run", status="ACTIVE", workspace_root=blocked_workspace_root)

        payload = {
            "runtime_name": "live-run",
            "state_root": str(blocked_state_root.resolve()),
            "workspace_root": str(blocked_workspace_root.resolve()),
            "owned_processes": [],
        }
        housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
        commit_reap_requested_event(
            housekeeping_root,
            request_id="live-run:req-001",
            node_id="live-run",
            payload=payload,
            producer="test.reap_reconciler_foundation",
        )
        commit_runtime_reap_requested_event(
            housekeeping_root,
            request_id="live-run:req-001",
            node_id="live-run",
            payload=payload,
            producer="test.reap_reconciler_foundation",
        )
        results = gc_module.reconcile_housekeeping_reap_requests(repo_root=repo_root, runtime_name="live-run")
        if len(results) != 1:
            return _fail("reconciler must report exactly one live-run result")
        result = dict(results[0])
        if str(result.get("outcome") or "") != "deferred":
            return _fail("reconciler must defer a blocked reap request")
        if not result.get("reap_deferred_event_id"):
            return _fail("deferred reconcile must report canonical reap_deferred event id")
        if not blocked_state_root.exists():
            return _fail("deferred reconcile must not delete the blocked runtime root")
        hygiene = query_operational_hygiene_view(
            housekeeping_root,
            include_heavy_object_summaries=False,
        )
        if int(hygiene.get("reap_deferred_event_count") or 0) < 1:
            return _fail("reconciler must commit canonical reap_deferred for blocked requests")

    print("[loop-system-reap-reconciler-foundation] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
