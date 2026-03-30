#!/usr/bin/env python3
"""Validate canonical Milestone 3 reap vocabulary is emitted alongside compatibility aliases."""

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
    print(f"[loop-system-canonical-reap-vocabulary][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _persist_node(state_root: Path, node_id: str, *, status: str, workspace_root: Path | None = None) -> None:
    _write_json(
        state_root / "state" / f"{node_id}.json",
        {
            "node_id": node_id,
            "status": status,
            "workspace_root": str(workspace_root.resolve()) if workspace_root is not None else "",
            "runtime_state": {},
        },
    )


def _persist_launch_result(state_root: Path, node_id: str, *, pid: int) -> None:
    launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
    _write_json(
        launch_ref,
        {
            "node_id": node_id,
            "pid": int(pid),
            "launch_result_ref": str(launch_ref.resolve()),
            "state_root": str(state_root.resolve()),
        },
    )


def main() -> int:
    from loop_product.kernel import query_operational_hygiene_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_canonical_reap_vocab_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_scope_root = repo_root / ".loop"
        workspace_scope_root = repo_root / "workspace"
        state_scope_root.mkdir(parents=True, exist_ok=True)
        workspace_scope_root.mkdir(parents=True, exist_ok=True)

        stopped_state_root = state_scope_root / "stopped-run"
        stopped_workspace_root = workspace_scope_root / "stopped-run"
        stopped_workspace_root.mkdir(parents=True, exist_ok=True)
        (stopped_state_root / "state").mkdir(parents=True, exist_ok=True)
        _persist_node(stopped_state_root, "stopped-run", status="BLOCKED", workspace_root=stopped_workspace_root)
        _persist_node(stopped_state_root, "child-terminal-001", status="COMPLETED", workspace_root=stopped_workspace_root)
        _persist_launch_result(stopped_state_root, "child-terminal-001", pid=424242)

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
            gc_module.reap_stopped_run(state_root=stopped_state_root, repo_root=repo_root)
            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            hygiene = query_operational_hygiene_view(
                housekeeping_root,
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("reap_requested_event_count") or 0) != 1:
                return _fail("reapable run must emit exactly one canonical reap_requested event")
            if int(hygiene.get("process_reap_confirmed_event_count") or 0) != 1:
                return _fail("reapable run must emit exactly one canonical process_reap_confirmed event")
            latest_requested = dict(hygiene.get("latest_reap_requested") or {})
            latest_confirmed = dict(hygiene.get("latest_process_reap_confirmed") or {})
            if str(dict(latest_requested.get("payload") or {}).get("runtime_name") or "") != "stopped-run":
                return _fail("canonical reap_requested payload must preserve the runtime name")
            if str(dict(latest_confirmed.get("payload") or {}).get("runtime_name") or "") != "stopped-run":
                return _fail("canonical process_reap_confirmed payload must preserve the runtime name")
            if int(hygiene.get("runtime_reap_requested_event_count") or 0) != 1:
                return _fail("compatibility runtime_reap_requested event must still be preserved")
            if int(hygiene.get("runtime_reap_confirmed_event_count") or 0) != 1:
                return _fail("compatibility runtime_reap_confirmed event must still be preserved")

            deadline = time.time() + 3.0
            while time.time() < deadline and lingering_proc.poll() is None:
                time.sleep(0.05)
            if lingering_proc.poll() is None:
                return _fail("canonical reap vocabulary must not regress actual stopped-run termination")
        finally:
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

        live_state_root = state_scope_root / "live-run"
        live_workspace_root = workspace_scope_root / "live-run"
        live_workspace_root.mkdir(parents=True, exist_ok=True)
        (live_state_root / "state").mkdir(parents=True, exist_ok=True)
        _persist_node(live_state_root, "live-run", status="ACTIVE", workspace_root=live_workspace_root)
        live_proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
            close_fds=True,
        )
        try:
            _persist_launch_result(live_state_root, "live-run", pid=live_proc.pid)
            try:
                gc_module.reap_stopped_run(state_root=live_state_root, repo_root=repo_root)
            except ValueError:
                pass
            else:
                return _fail("non-reapable live run must still reject reap")

            housekeeping_root = gc_module.housekeeping_runtime_root(repo_root=repo_root)
            hygiene = query_operational_hygiene_view(
                housekeeping_root,
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("reap_deferred_event_count") or 0) != 1:
                return _fail("blocked reap must emit exactly one canonical reap_deferred event")
            latest_deferred = dict(hygiene.get("latest_reap_deferred") or {})
            payload = dict(latest_deferred.get("payload") or {})
            if str(payload.get("runtime_name") or "") != "live-run":
                return _fail("canonical reap_deferred payload must preserve the blocked runtime name")
            if int(hygiene.get("runtime_reap_deferred_event_count") or 0) != 1:
                return _fail("compatibility runtime_reap_deferred event must still be preserved")
        finally:
            if live_proc.poll() is None:
                live_proc.terminate()
                try:
                    live_proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    live_proc.kill()
                    live_proc.wait(timeout=1.0)

    print("[loop-system-canonical-reap-vocabulary] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
