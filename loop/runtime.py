"""Long-lived router runtime entrypoint for `router_api start`."""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from pathlib import Path

from loop.ai_launch import ai_launch_exit_code, start_ai_launch
from loop.codex_runtime import prepare_runtime_owned_codex_home
from loop.core import (
    ActorLaunchResult,
    ActorLaunchSpec,
    RouterCore,
    RouterStartError,
    router_wakeup_socket_path,
)
from loop.launch import build_router_actor_launch_plan
from loop.store import RouterStore


class _AiLaunchWaitableProcess:
    """Minimal wait/poll wrapper around an AiLaunchHandle."""

    def __init__(self, handle) -> None:
        self._handle = handle
        self.pid = int(getattr(handle, "pid", 0) or 0)

    def wait(self) -> int:
        while True:
            exit_code = ai_launch_exit_code(self._handle)
            if exit_code is not None:
                return int(exit_code)
            time.sleep(0.05)

    def poll(self) -> int | None:
        exit_code = ai_launch_exit_code(self._handle)
        if exit_code is None:
            return None
        return int(exit_code)


def _write_startup_result(path: Path, payload: dict[str, object]) -> None:
    path = Path(path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    tmp_path.replace(path)


def _parse_session_meta(rollout_path: Path) -> tuple[str, str]:
    first_line = ""
    with rollout_path.open("r", encoding="utf-8", errors="replace") as handle:
        first_line = str(handle.readline() or "").strip()
    if not first_line:
        return "", ""
    try:
        obj = json.loads(first_line)
    except json.JSONDecodeError:
        return "", ""
    payload = dict(obj.get("payload") or {})
    return str(payload.get("id") or ""), str(payload.get("timestamp") or "")


def _wait_for_rollout(runtime_codex_home: Path, *, timeout_seconds: float = 15.0) -> tuple[str, Path]:
    sessions_root = runtime_codex_home / "sessions"
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        matches = sorted(sessions_root.rglob("rollout-*.jsonl")) if sessions_root.exists() else []
        if matches:
            chosen = max(matches, key=lambda path: path.stat().st_mtime_ns)
            session_id, _started_at = _parse_session_meta(chosen)
            if session_id:
                return session_id, chosen.resolve()
        time.sleep(0.05)
    raise RouterStartError(
        reason_code="ROOT_ACTOR_LAUNCH_FAILED",
        message="router start rejected: launched root actor did not materialize a rollout session in time.",
    )


def build_actor_launcher(*, router_db_path: Path):
    repo_root = Path(__file__).resolve().parents[1]

    def _launch(request: ActorLaunchSpec) -> ActorLaunchResult:
        attempt_root = (
            router_db_path.parent
            / "router"
            / "runtime"
            / f"node-{request.node_id}"
            / request.actor_kind.value
            / f"attempt-{request.attempt_count}"
        ).resolve()
        attempt_root.mkdir(parents=True, exist_ok=True)
        runtime_codex_home = prepare_runtime_owned_codex_home(
            runtime_home=attempt_root / ".codex_runtime"
        )
        prompt_path = attempt_root / f"{request.actor_kind.value}.prompt.md"
        prompt_path.write_text(request.prompt_text, encoding="utf-8")
        launch_plan = build_router_actor_launch_plan(
            actor_kind=request.actor_kind,
            workspace_root=request.workspace_root,
            prompt_path=prompt_path,
            actor_env=request.env,
            codex_home=runtime_codex_home,
        )
        handle = start_ai_launch(
            cmd=launch_plan.argv,
            cwd=launch_plan.cwd,
            log_dir=attempt_root / "logs",
            label=(
                f"router-node-{request.node_id}-{request.actor_kind.value}"
                f"-attempt-{request.attempt_count}"
            ),
            env=launch_plan.env,
            stdin_path=launch_plan.stdin_path,
            start_new_session=launch_plan.start_new_session,
            bridge_mode_override=launch_plan.bridge_mode_override,
        )
        session_id, rollout_path = _wait_for_rollout(runtime_codex_home)
        return ActorLaunchResult(
            process=_AiLaunchWaitableProcess(handle),
            process_birth_time=time.time(),
            session_id=session_id,
            rollout_path=rollout_path,
        )

    return _launch


def run_router_runtime(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run one long-lived router runtime.")
    parser.add_argument("--router-db", required=True)
    parser.add_argument("--kernel-session-id")
    parser.add_argument("--kernel-rollout-path")
    parser.add_argument("--kernel-started-at")
    parser.add_argument("--final-effects-file")
    parser.add_argument("--startup-result-file", required=True)
    parser.add_argument("--resume-only", action="store_true")
    args = parser.parse_args(argv)

    store = RouterStore(Path(args.router_db).expanduser().resolve())
    core = RouterCore(
        store=store,
        wakeup_socket_path=router_wakeup_socket_path(store.db_path),
        actor_launcher=build_actor_launcher(router_db_path=store.db_path),
    )
    try:
        if bool(args.resume_only):
            core.start()
            payload = {
                "accepted": True,
                "status": "RESUMED",
                "message": "router resumed from the latest durable state",
            }
        else:
            payload = core.start_router(
                kernel_session_id=args.kernel_session_id,
                kernel_rollout_path=args.kernel_rollout_path,
                kernel_started_at=args.kernel_started_at,
                final_effects_file=args.final_effects_file,
            )
    except RouterStartError as exc:
        _write_startup_result(Path(args.startup_result_file), exc.to_payload())
        return 2
    except Exception as exc:  # noqa: BLE001
        _write_startup_result(
            Path(args.startup_result_file),
            {
                "accepted": False,
                "status": "REJECTED",
                "reason_code": "ROOT_ACTOR_LAUNCH_FAILED",
                "message": f"router start rejected: {exc}",
            },
        )
        return 2

    _write_startup_result(Path(args.startup_result_file), payload)
    stop_event = threading.Event()

    def _request_stop(_signum, _frame) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    try:
        stop_event.wait()
    finally:
        core.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    return int(run_router_runtime(argv) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
