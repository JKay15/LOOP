#!/usr/bin/env python3
"""Validate repo-scoped child supervision reactor registration and spawn dedupe."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fail(msg: str) -> int:
    print(f"[loop-system-supervision-reactor-registry][FAIL] {msg}", file=sys.stderr)
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


def main() -> int:
    try:
        import loop_product.child_supervision_sidecar as sidecar_module  # type: ignore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"failed to import child supervision sidecar helpers: {exc}")

    with tempfile.TemporaryDirectory(prefix="loop_system_supervision_reactor_registry_") as td:
        temp_root = Path(td)
        workspace_root = temp_root / "workspace"
        state_root = temp_root / ".loop"
        launch_root = state_root / "artifacts" / "launches"
        launch_a = launch_root / "child-a" / "attempt_001" / "ChildLaunchResult.json"
        launch_b = launch_root / "child-b" / "attempt_001" / "ChildLaunchResult.json"
        launch_c = launch_root / "child-c" / "attempt_001" / "ChildLaunchResult.json"
        _write_launch_result(launch_result_ref=launch_a, node_id="child-a", workspace_root=workspace_root, state_root=state_root)
        _write_launch_result(launch_result_ref=launch_b, node_id="child-b", workspace_root=workspace_root, state_root=state_root)
        _write_launch_result(launch_result_ref=launch_c, node_id="child-c", workspace_root=workspace_root, state_root=state_root)

        repo_supervision_root = sidecar_module.repo_supervision_reactor_root(launch_result_ref=launch_a)
        reactor_runtime_ref = sidecar_module.repo_supervision_reactor_runtime_ref(
            repo_supervision_root=repo_supervision_root
        )

        original_live_child_runtime = sidecar_module.live_child_supervision_runtime
        original_live_repo_runtime = sidecar_module.live_repo_supervision_reactor_runtime
        original_popen = sidecar_module.subprocess.Popen

        popen_argvs: list[list[str]] = []

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
            runtime_ref = sidecar_module.supervision_runtime_ref(launch_result_ref=launch_result_ref)
            if runtime_ref.exists():
                return json.loads(runtime_ref.read_text(encoding="utf-8"))
            registry = sidecar_module._load_repo_supervision_registry(repo_supervision_root)
            tracked_launches = dict(registry.get("tracked_launches") or {})
            launch_key = str(Path(launch_result_ref).expanduser().resolve())
            if not reactor_runtime_ref.exists() or launch_key not in tracked_launches:
                return None
            payload = {
                "schema": "loop_product.child_supervision_runtime",
                "launch_result_ref": launch_key,
                "runtime_kind": "repo_scoped_child_supervision",
                "repo_supervision_root": str(repo_supervision_root),
                "pid": os.getpid(),
                "poll_interval_s": float(tracked_launches[launch_key].get("poll_interval_s") or 2.0),
                "stall_threshold_s": float(tracked_launches[launch_key].get("stall_threshold_s") or 60.0),
                "max_recoveries": int(tracked_launches[launch_key].get("max_recoveries") or 5),
                "no_substantive_progress_window_s": float(
                    tracked_launches[launch_key].get("no_substantive_progress_window_s") or 300.0
                ),
                "lease_epoch": 1,
                "started_at_utc": "2026-03-29T00:00:00+00:00",
                "log_ref": str(sidecar_module.repo_supervision_reactor_log_ref(repo_supervision_root=repo_supervision_root)),
                "result_ref": str(sidecar_module.supervision_result_ref(launch_result_ref=launch_key)),
            }
            sidecar_module._write_json(runtime_ref, payload)
            return payload

        def _fake_popen(argv, **kwargs):
            del kwargs
            normalized_argv = [str(item) for item in argv]
            popen_argvs.append(normalized_argv)
            if "--run-repo-supervision-reactor" not in normalized_argv:
                raise AssertionError(f"expected repo-scoped reactor launch argv, got {normalized_argv!r}")
            payload = {
                "schema": "loop_product.repo_scoped_child_supervision_reactor_runtime",
                "runtime_kind": "repo_scoped_child_supervision_reactor",
                "repo_supervision_root": str(repo_supervision_root),
                "pid": os.getpid(),
                "started_at_utc": "2026-03-29T00:00:00+00:00",
                "tracked_launch_refs": [],
            }
            sidecar_module._write_json(reactor_runtime_ref, payload)
            return _FakeProc()

        sidecar_module.live_child_supervision_runtime = _fake_live_child_runtime
        sidecar_module.live_repo_supervision_reactor_runtime = _fake_live_repo_runtime
        sidecar_module.subprocess.Popen = _fake_popen
        try:
            payload_a = sidecar_module.ensure_child_supervision_running(launch_result_ref=launch_a, startup_timeout_s=1.0)
            payload_b = sidecar_module.ensure_child_supervision_running(launch_result_ref=launch_b, startup_timeout_s=1.0)
            if len(popen_argvs) != 1:
                return _fail("two active children in one repo root must spawn exactly one repo-scoped reactor process")
            if str(payload_a.get("runtime_kind") or "") != "repo_scoped_child_supervision":
                return _fail("first child runtime payload must advertise repo-scoped supervision")
            if str(payload_b.get("runtime_kind") or "") != "repo_scoped_child_supervision":
                return _fail("second child runtime payload must advertise repo-scoped supervision")

            registry_after_two = sidecar_module._load_repo_supervision_registry(repo_supervision_root)
            tracked_after_two = sorted(str(item) for item in dict(registry_after_two.get("tracked_launches") or {}).keys())
            expected_two = sorted([str(launch_a.resolve()), str(launch_b.resolve())])
            if tracked_after_two != expected_two:
                return _fail(f"reactor registry must track both active children, got {tracked_after_two!r}")

            sidecar_module._write_json(
                sidecar_module.supervision_result_ref(launch_result_ref=launch_a),
                {"schema": "loop_product.child_supervision_result", "status": "TERMINAL"},
            )
            registry_after_prune = sidecar_module.reconcile_repo_supervision_registry(
                repo_supervision_root=repo_supervision_root
            )
            tracked_after_prune = sorted(str(item) for item in dict(registry_after_prune.get("tracked_launches") or {}).keys())
            expected_pruned = [str(launch_b.resolve())]
            if tracked_after_prune != expected_pruned:
                return _fail(f"reactor registry must prune terminal children, got {tracked_after_prune!r}")

            payload_c = sidecar_module.ensure_child_supervision_running(launch_result_ref=launch_c, startup_timeout_s=1.0)
            if len(popen_argvs) != 1:
                return _fail("joining a new child under a live repo-scoped reactor must not spawn a second process")
            if str(payload_c.get("runtime_kind") or "") != "repo_scoped_child_supervision":
                return _fail("joined child runtime payload must advertise repo-scoped supervision")
            registry_after_join = sidecar_module._load_repo_supervision_registry(repo_supervision_root)
            tracked_after_join = sorted(str(item) for item in dict(registry_after_join.get("tracked_launches") or {}).keys())
            expected_join = sorted([str(launch_b.resolve()), str(launch_c.resolve())])
            if tracked_after_join != expected_join:
                return _fail(f"reactor registry must retain active children and admit new joins, got {tracked_after_join!r}")
        finally:
            sidecar_module.live_child_supervision_runtime = original_live_child_runtime
            sidecar_module.live_repo_supervision_reactor_runtime = original_live_repo_runtime
            sidecar_module.subprocess.Popen = original_popen

    print("[loop-system-supervision-reactor-registry] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
