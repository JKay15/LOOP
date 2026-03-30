#!/usr/bin/env python3
"""Validate standalone housekeeping reap controller execution."""

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
    print(f"[loop-system-housekeeping-reap-controller][FAIL] {msg}", file=sys.stderr)
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
    from loop_product.kernel import query_operational_hygiene_view
    from loop_product.runtime import gc as gc_module

    with tempfile.TemporaryDirectory(prefix="loop_system_housekeeping_reap_controller_") as td:
        repo_root = Path(td).resolve() / "loop_product_repo"
        state_root = repo_root / ".loop" / "stopped-run"
        workspace_root = repo_root / "workspace" / "stopped-run"
        (state_root / "state").mkdir(parents=True, exist_ok=True)
        workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(state_root, "stopped-run", status="BLOCKED", workspace_root=workspace_root)

        lingering_proc = subprocess.Popen(
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
        try:
            request = gc_module.submit_stopped_run_reap_request(
                state_root=state_root,
                repo_root=repo_root,
                auto_bootstrap_controller=False,
            )
            if str(request.get("runtime_name") or "") != "stopped-run":
                return _fail("request helper must preserve runtime name")
            if not request.get("canonical_reap_requested_event_id"):
                return _fail("request helper must report canonical reap_requested event id")
            if not state_root.exists():
                return _fail("submitting reap intent alone must not immediately delete the runtime root")

            results = gc_module.run_housekeeping_reap_controller_once(repo_root=repo_root, runtime_name="stopped-run")
            if len(results) != 1:
                return _fail("controller run must settle exactly one stopped-run request")
            result = dict(results[0])
            if str(result.get("outcome") or "") != "confirmed":
                return _fail("controller must confirm a reapable stopped-run request")
            if str(result.get("controller_runner") or "") != "housekeeping_reap_controller":
                return _fail("controller result must record housekeeping_reap_controller ownership")
            if not result.get("process_reap_confirmed_event_id"):
                return _fail("controller result must report canonical process_reap_confirmed event id")
            if not result.get("runtime_root_quiesced_event_id"):
                return _fail("controller result must report canonical runtime_root_quiesced event id")
            if state_root.exists():
                return _fail("controller must retire the stopped runtime root after confirmation")

            hygiene = query_operational_hygiene_view(
                gc_module.housekeeping_runtime_root(repo_root=repo_root),
                include_heavy_object_summaries=False,
            )
            if int(hygiene.get("reap_requested_event_count") or 0) != 1:
                return _fail("controller scenario must preserve one canonical reap_requested fact")
            if int(hygiene.get("process_reap_confirmed_event_count") or 0) != 1:
                return _fail("controller scenario must emit one canonical process_reap_confirmed fact")
            if int(hygiene.get("runtime_root_quiesced_event_count") or 0) != 1:
                return _fail("controller scenario must emit one canonical runtime_root_quiesced fact")

            deadline = time.time() + 3.0
            while time.time() < deadline and lingering_proc.poll() is None:
                time.sleep(0.05)
            if lingering_proc.poll() is None:
                return _fail("controller must terminate lingering owned processes")
        finally:
            if lingering_proc.poll() is None:
                lingering_proc.kill()
                lingering_proc.wait(timeout=1.0)

        blocked_state_root = repo_root / ".loop" / "live-run"
        blocked_workspace_root = repo_root / "workspace" / "live-run"
        (blocked_state_root / "state").mkdir(parents=True, exist_ok=True)
        blocked_workspace_root.mkdir(parents=True, exist_ok=True)
        _persist_node(blocked_state_root, "live-run", status="ACTIVE", workspace_root=blocked_workspace_root)

        request = gc_module.submit_stopped_run_reap_request(
            state_root=blocked_state_root,
            repo_root=repo_root,
            auto_bootstrap_controller=False,
        )
        if str(request.get("runtime_name") or "") != "live-run":
            return _fail("blocked request helper must preserve runtime name")
        results = gc_module.run_housekeeping_reap_controller_once(repo_root=repo_root, runtime_name="live-run")
        if len(results) != 1:
            return _fail("controller run must settle exactly one live-run request")
        result = dict(results[0])
        if str(result.get("outcome") or "") != "deferred":
            return _fail("controller must defer blocked reap requests")
        if str(result.get("controller_runner") or "") != "housekeeping_reap_controller":
            return _fail("deferred controller result must record housekeeping_reap_controller ownership")
        if not result.get("reap_deferred_event_id"):
            return _fail("controller must report canonical reap_deferred event id")
        if not blocked_state_root.exists():
            return _fail("controller must not delete blocked runtime roots")

    print("[loop-system-housekeeping-reap-controller] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
