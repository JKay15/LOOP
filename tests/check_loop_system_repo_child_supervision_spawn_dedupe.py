#!/usr/bin/env python3
"""Validate repo-scoped child supervision spawn dedupe and lease fencing."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-child-supervision-spawn-dedupe][FAIL] {msg}", file=sys.stderr)
    return 2


def _write_launch_result(*, launch_result_ref: Path, node_id: str, workspace_root: Path, state_root: Path) -> None:
    launch_result_ref.parent.mkdir(parents=True, exist_ok=True)
    launch_result_ref.write_text(
        json.dumps(
            {
                "launch_result_ref": str(launch_result_ref.resolve()),
                "node_id": node_id,
                "workspace_root": str(workspace_root.resolve()),
                "state_root": str(state_root.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _install_fake_repo_reactor(sidecar_module, *, spawn_calls: list[list[str]]):
    original_live_child_runtime = sidecar_module.live_child_supervision_runtime
    original_live_repo_runtime = sidecar_module.live_repo_supervision_reactor_runtime
    original_popen = sidecar_module.subprocess.Popen
    original_pid_command_line = sidecar_module._pid_command_line
    reactor_command = {"value": "python unrelated-test-process"}

    class _FakeProc:
        def poll(self) -> None:
            return None

    def _fake_live_repo_runtime(*, repo_supervision_root: str | Path):
        runtime_ref = sidecar_module.repo_supervision_reactor_runtime_ref(
            repo_supervision_root=repo_supervision_root
        )
        if runtime_ref.exists():
            return json.loads(runtime_ref.read_text(encoding="utf-8"))
        return None

    def _fake_live_child_runtime(*, launch_result_ref: str | Path):
        launch_path = Path(launch_result_ref).expanduser().resolve()
        existing = original_live_child_runtime(launch_result_ref=launch_path)
        if existing is not None:
            return existing
        runtime_ref = sidecar_module.supervision_runtime_ref(launch_result_ref=launch_path)
        repo_root = sidecar_module.repo_supervision_reactor_root(launch_result_ref=launch_path)
        registry = sidecar_module._load_repo_supervision_registry(repo_root)
        tracked_launches = dict(registry.get("tracked_launches") or {})
        launch_key = str(launch_path)
        if launch_key not in tracked_launches:
            return None
        reactor_runtime_ref = sidecar_module.repo_supervision_reactor_runtime_ref(repo_supervision_root=repo_root)
        if not reactor_runtime_ref.exists():
            return None
        lease_epoch = sidecar_module.reserve_supervision_lease_epoch(launch_result_ref=launch_path)
        payload = {
            "schema": "loop_product.child_supervision_runtime",
            "launch_result_ref": launch_key,
            "runtime_kind": "repo_scoped_child_supervision",
            "repo_supervision_root": str(repo_root),
            "pid": os.getpid(),
            "log_ref": str(sidecar_module.repo_supervision_reactor_log_ref(repo_supervision_root=repo_root)),
            "result_ref": str(sidecar_module.supervision_result_ref(launch_result_ref=launch_path)),
            "started_at_utc": "2026-03-29T00:00:00+00:00",
            "poll_interval_s": float(tracked_launches[launch_key].get("poll_interval_s") or 2.0),
            "stall_threshold_s": float(tracked_launches[launch_key].get("stall_threshold_s") or 60.0),
            "max_recoveries": int(tracked_launches[launch_key].get("max_recoveries") or 5),
            "no_substantive_progress_window_s": float(
                tracked_launches[launch_key].get("no_substantive_progress_window_s") or 300.0
            ),
            "lease_epoch": int(lease_epoch),
        }
        sidecar_module._write_json(runtime_ref, payload)
        return payload

    def _fake_popen(argv, **kwargs):
        del kwargs
        normalized_argv = [str(item) for item in argv]
        if "--run-repo-supervision-reactor" not in normalized_argv:
            raise AssertionError(f"expected repo-scoped reactor spawn, got {normalized_argv!r}")
        spawn_calls.append(normalized_argv)
        try:
            repo_root = Path(normalized_argv[normalized_argv.index("--repo-supervision-root") + 1]).expanduser().resolve()
        except Exception as exc:  # pragma: no cover - explicit failure path
            raise AssertionError(f"repo-scoped reactor launch must carry --repo-supervision-root: {normalized_argv!r}") from exc
        reactor_command["value"] = (
            f"{sys.executable} -m loop_product.child_supervision_sidecar "
            f"--run-repo-supervision-reactor --repo-supervision-root {repo_root}"
        )
        sidecar_module._write_json(
            sidecar_module.repo_supervision_reactor_runtime_ref(repo_supervision_root=repo_root),
            {
                "schema": "loop_product.repo_scoped_child_supervision_reactor_runtime",
                "runtime_kind": "repo_scoped_child_supervision_reactor",
                "repo_supervision_root": str(repo_root),
                "pid": os.getpid(),
                "started_at_utc": "2026-03-29T00:00:00+00:00",
                "tracked_launch_refs": [],
            },
        )
        return _FakeProc()

    sidecar_module.live_child_supervision_runtime = _fake_live_child_runtime
    sidecar_module.live_repo_supervision_reactor_runtime = _fake_live_repo_runtime
    sidecar_module.subprocess.Popen = _fake_popen
    sidecar_module._pid_command_line = (
        lambda pid: reactor_command["value"] if int(pid) == int(os.getpid()) else "python unrelated-test-process"
    )
    return original_live_child_runtime, original_live_repo_runtime, original_popen, original_pid_command_line


def main() -> int:
    import loop_product.child_supervision_sidecar as sidecar_module

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_spawn_dedupe_") as td:
        temp_root = Path(td)
        launch_result_ref = temp_root / "ChildLaunchResult.json"
        _write_launch_result(
            launch_result_ref=launch_result_ref,
            node_id="child-supervision-spawn-dedupe-001",
            workspace_root=temp_root / "workspace",
            state_root=temp_root / ".loop",
        )

        spawn_calls: list[list[str]] = []
        original_live_child_runtime, original_live_repo_runtime, original_popen, original_pid_command_line = _install_fake_repo_reactor(
            sidecar_module,
            spawn_calls=spawn_calls,
        )
        start_barrier = threading.Barrier(2)
        errors: list[str] = []
        results: list[dict[str, object]] = []

        def _runner() -> None:
            try:
                start_barrier.wait()
                results.append(
                    sidecar_module.ensure_child_supervision_running(
                        launch_result_ref=launch_result_ref,
                        startup_timeout_s=1.0,
                    )
                )
            except Exception as exc:  # pragma: no cover - explicit failure path
                errors.append(str(exc))

        try:
            threads = [threading.Thread(target=_runner), threading.Thread(target=_runner)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5.0)
        finally:
            sidecar_module.live_child_supervision_runtime = original_live_child_runtime
            sidecar_module.live_repo_supervision_reactor_runtime = original_live_repo_runtime
            sidecar_module.subprocess.Popen = original_popen
            sidecar_module._pid_command_line = original_pid_command_line

        if errors:
            return _fail(f"concurrent ensure_child_supervision_running calls must not error: {errors}")
        if len(results) != 2:
            return _fail("both concurrent callers must receive a runtime payload")
        if len(spawn_calls) != 1:
            return _fail("concurrent ensure_child_supervision_running calls must spawn at most one repo-scoped reactor")
        if any(str(result.get("launch_result_ref") or "") != str(launch_result_ref.resolve()) for result in results):
            return _fail("all concurrent callers must converge on the same launch_result_ref")
        if any(int(result.get("lease_epoch") or 0) != 1 for result in results):
            return _fail("initial repo-scoped supervision registration must surface lease_epoch=1 to all concurrent callers")

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_lease_epoch_persistence_") as td:
        temp_root = Path(td)
        launch_result_ref = temp_root / "ChildLaunchResult.json"
        _write_launch_result(
            launch_result_ref=launch_result_ref,
            node_id="child-supervision-lease-epoch-persistence-001",
            workspace_root=temp_root / "workspace",
            state_root=temp_root / ".loop",
        )

        runtime_ref = sidecar_module.supervision_runtime_ref(launch_result_ref=launch_result_ref)
        lease_state_ref = sidecar_module.supervision_lease_state_ref(launch_result_ref=launch_result_ref)
        first_epoch = sidecar_module.reserve_supervision_lease_epoch(launch_result_ref=launch_result_ref)
        sidecar_module._write_json(
            runtime_ref,
            {
                "schema": "loop_product.child_supervision_runtime",
                "launch_result_ref": str(launch_result_ref.resolve()),
                "pid": os.getpid(),
                "lease_epoch": int(first_epoch),
            },
        )
        runtime_ref.unlink(missing_ok=True)
        second_epoch = sidecar_module.reserve_supervision_lease_epoch(launch_result_ref=launch_result_ref)
        if [first_epoch, second_epoch] != [1, 2]:
            return _fail("same-attempt child supervision lease epochs must stay monotonic across runtime-marker cleanup")
        if not lease_state_ref.exists():
            return _fail("lease-state receipt must persist the latest reserved epoch")
        lease_state_payload = json.loads(lease_state_ref.read_text(encoding="utf-8"))
        if int(lease_state_payload.get("last_reserved_lease_epoch") or 0) != 2:
            return _fail("lease-state receipt must remember the latest reserved lease_epoch")

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_cross_attempt_spawn_") as td:
        temp_root = Path(td)
        state_root = temp_root / ".loop"
        workspace_root = temp_root / "workspace"
        node_id = "child-supervision-cross-attempt-dedupe-001"
        older_launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_001" / "ChildLaunchResult.json"
        newer_launch_ref = state_root / "artifacts" / "launches" / node_id / "attempt_002" / "ChildLaunchResult.json"
        _write_launch_result(launch_result_ref=older_launch_ref, node_id=node_id, workspace_root=workspace_root, state_root=state_root)
        _write_launch_result(launch_result_ref=newer_launch_ref, node_id=node_id, workspace_root=workspace_root, state_root=state_root)
        sidecar_module._write_json(
            sidecar_module.supervision_runtime_ref(launch_result_ref=older_launch_ref),
            {
                "schema": "loop_product.child_supervision_runtime",
                "launch_result_ref": str(older_launch_ref.resolve()),
                "pid": os.getpid(),
                "lease_epoch": 1,
            },
        )

        cross_attempt_spawn_calls: list[list[str]] = []
        original_live_child_runtime, original_live_repo_runtime, original_popen, original_pid_command_line = _install_fake_repo_reactor(
            sidecar_module,
            spawn_calls=cross_attempt_spawn_calls,
        )
        try:
            refreshed_runtime = sidecar_module.ensure_child_supervision_running(
                launch_result_ref=newer_launch_ref,
                startup_timeout_s=1.0,
            )
        finally:
            sidecar_module.live_child_supervision_runtime = original_live_child_runtime
            sidecar_module.live_repo_supervision_reactor_runtime = original_live_repo_runtime
            sidecar_module.subprocess.Popen = original_popen
            sidecar_module._pid_command_line = original_pid_command_line

        if len(cross_attempt_spawn_calls) != 1:
            return _fail("newer started attempt must spawn repo-scoped supervision instead of reusing the older attempt runtime")
        if str(refreshed_runtime.get("launch_result_ref") or "") != str(newer_launch_ref.resolve()):
            return _fail("cross-attempt supervision must bind to the newer attempt launch_result_ref")
        if int(refreshed_runtime.get("lease_epoch") or 0) != 2:
            return _fail("newer cross-attempt supervision must advance lease_epoch past the older attempt owner")

    with tempfile.TemporaryDirectory(prefix="loop_system_child_supervision_stale_runtime_pid_") as td:
        temp_root = Path(td)
        launch_result_ref = temp_root / "ChildLaunchResult.json"
        _write_launch_result(
            launch_result_ref=launch_result_ref,
            node_id="child-supervision-stale-runtime-pid-001",
            workspace_root=temp_root / "workspace",
            state_root=temp_root / ".loop",
        )

        runtime_ref = sidecar_module.supervision_runtime_ref(launch_result_ref=launch_result_ref)
        sidecar_module._write_json(
            runtime_ref,
            {
                "schema": "loop_product.child_supervision_runtime",
                "launch_result_ref": str(launch_result_ref.resolve()),
                "pid": os.getpid(),
                "started_at_utc": "2026-03-23T00:00:00+00:00",
                "poll_interval_s": 2.0,
                "stall_threshold_s": 60.0,
                "max_recoveries": 5,
                "no_substantive_progress_window_s": 300.0,
            },
        )

        stale_runtime_spawn_calls: list[list[str]] = []
        original_live_child_runtime, original_live_repo_runtime, original_popen, original_pid_command_line = _install_fake_repo_reactor(
            sidecar_module,
            spawn_calls=stale_runtime_spawn_calls,
        )
        sidecar_module._pid_command_line = lambda pid: "python unrelated-test-process"
        try:
            refreshed_runtime = sidecar_module.ensure_child_supervision_running(
                launch_result_ref=launch_result_ref,
                startup_timeout_s=1.0,
            )
        finally:
            sidecar_module.live_child_supervision_runtime = original_live_child_runtime
            sidecar_module.live_repo_supervision_reactor_runtime = original_live_repo_runtime
            sidecar_module.subprocess.Popen = original_popen
            sidecar_module._pid_command_line = original_pid_command_line

        if len(stale_runtime_spawn_calls) != 1:
            return _fail("stale runtime markers must not suppress a fresh repo-scoped supervision spawn")
        if int(refreshed_runtime.get("lease_epoch") or 0) != 2:
            return _fail("fresh supervision after a stale pre-epoch runtime marker must bump lease_epoch to 2")

    print("[loop-system-child-supervision-spawn-dedupe] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
