#!/usr/bin/env python3
"""Validate minimal router proc observation runtime."""

from __future__ import annotations

import sys
import tempfile
import threading
import time
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _fail(msg: str) -> int:
    print(f"[loop-system-router-proc][FAIL] {msg}", file=sys.stderr)
    return 2


class _FakeProcess:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._done = threading.Event()

    def finish(self, returncode: int) -> None:
        self.returncode = returncode
        self._done.set()

    def wait(self) -> int:
        self._done.wait()
        if self.returncode is None:
            raise RuntimeError("fake process released without returncode")
        return self.returncode


class _HungThread:
    def __init__(self) -> None:
        self.join_calls: list[float | None] = []

    def join(self, timeout: float | None = None) -> None:
        self.join_calls.append(timeout)

    def is_alive(self) -> bool:
        return True


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    try:
        from loop.events import ActorKind, ActorRef
        from loop.core import RouterCore, notify_router_wakeup, router_wakeup_socket_path
        from loop.proc import ProcSupervisor, select_proc_backend
        from loop.store import RouterStore
    except Exception as exc:  # noqa: BLE001
        return _fail(f"router proc imports failed: {exc}")

    backend = select_proc_backend("darwin")
    if backend.platform_key != "darwin":
        return _fail("darwin backend selection must preserve platform key")

    try:
        select_proc_backend("linux")
    except RuntimeError:
        pass
    else:
        return _fail("unsupported backend selection must raise")

    session_id = "019d498e-44fe-7612-9307-476380906e34"
    rollout_name = f"rollout-2026-04-01T22-59-13-{session_id}.jsonl"
    actor = ActorRef(
        node_id="node-001",
        actor_kind=ActorKind.IMPLEMENTER,
        attempt_count=3,
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        codex_home = Path(tmpdir) / ".codex"
        rollout_path = codex_home / "sessions" / "2026" / "04" / "01" / rollout_name
        rollover_path = codex_home / "sessions" / "2026" / "04" / "02" / rollout_name
        rollout_path.parent.mkdir(parents=True, exist_ok=True)
        rollout_path.write_text('{"first":"line"}\n', encoding="utf-8")
        fake_process = _FakeProcess(pid=43210)
        store = RouterStore(Path(tmpdir) / "router.sqlite3")
        core = RouterCore(
            store=store,
            wakeup_socket_path=router_wakeup_socket_path(Path(tmpdir) / "router.sqlite3"),
        )
        supervisor = ProcSupervisor(
            codex_home=codex_home,
            backend=backend,
            output_window_seconds=0.2,
            event_sink=store.append_event,
            event_notifier=lambda: notify_router_wakeup(core.wakeup_socket_path),
        )
        core.start()
        try:
            supervisor.start()
            resolved = supervisor.register_actor(
                actor,
                process=fake_process,
                process_birth_time=1711965600.0,
                session_id=session_id,
            )
            if resolved is None or resolved.resolve() != rollout_path.resolve():
                return _fail("register_actor must resolve the rollout path from session_id")

            rollout_path.write_text('{"first":"line"}\n{"second":"line"}\n', encoding="utf-8")
            output_events = _wait_for_event_type_count(
                store,
                event_type="OutputWindow",
                expected_count=1,
                timeout_seconds=2.0,
            )
            if len(output_events) != 1:
                return _fail("supervisor must asynchronously emit one output event after file growth")
            output_event = output_events[0]
            if output_event.event_type != "OutputWindow":
                return _fail("first stored proc event must be OutputWindow")
            if output_event.node_id != actor.node_id or output_event.actor_kind is not actor.actor_kind:
                return _fail("first stored proc event must preserve actor identity columns")
            if output_event.attempt_count != actor.attempt_count:
                return _fail("first stored proc event must preserve attempt_count")
            if not _wait_until(lambda: store.read_last_applied_seq() == output_events[0].seq, timeout_seconds=2.0):
                return _fail("proc output event must wake the running core")
            output_payload = json.loads(output_event.payload_json)
            if output_payload.get("had_output") is not True or output_payload.get("observed_at") is None:
                return _fail("stored output event must preserve had_output and observed_at payload")

            rollout_path.write_text(
                '{"first":"line"}\n{"second":"line"}\n{"third":"line"}\n',
                encoding="utf-8",
            )
            repeated_events = _wait_for_event_type_count(
                store,
                event_type="OutputWindow",
                expected_count=2,
                timeout_seconds=2.0,
            )
            if len(repeated_events) != 2:
                return _fail("supervisor must keep running and emit repeated output events across windows")
            repeated_output_event = repeated_events[1]
            if repeated_output_event.event_type != "OutputWindow":
                return _fail("repeated proc event must still be OutputWindow")
            if repeated_output_event.node_id != actor.node_id or repeated_output_event.actor_kind is not actor.actor_kind:
                return _fail("repeated stored proc event must preserve actor identity columns")

            rollover_path.parent.mkdir(parents=True, exist_ok=True)
            rollover_path.write_text('{"fourth":"line"}\n', encoding="utf-8")
            rollover_events = _wait_for_event_type_count(
                store,
                event_type="OutputWindow",
                expected_count=3,
                timeout_seconds=2.0,
            )
            if len(rollover_events) != 3:
                return _fail("supervisor must follow the newest rollout path when a session continues into a new dated jsonl file")
            if supervisor.resolve_rollout_path(session_id) != rollover_path.resolve():
                return _fail("resolve_rollout_path must prefer the newest dated rollout path for one session id")

            rollover_path.write_text(
                '{"fourth":"line"}\n{"fifth":"line"}\n',
                encoding="utf-8",
            )
            rollover_growth_events = _wait_for_event_type_count(
                store,
                event_type="OutputWindow",
                expected_count=4,
                timeout_seconds=2.0,
            )
            if len(rollover_growth_events) != 4:
                return _fail("supervisor must continue emitting output events after switching to a newer dated rollout path")

            time.sleep(0.4)
            unchanged_events = _wait_for_event_type_count(
                store,
                event_type="OutputWindow",
                expected_count=5,
                timeout_seconds=2.0,
            )
            if len(unchanged_events) < 5:
                return _fail("each output window must emit OutputWindow even when no new output appears")
            silent_payloads = [json.loads(event.payload_json) for event in unchanged_events[4:]]
            if not silent_payloads or not any(payload.get("had_output") is False for payload in silent_payloads):
                return _fail("unchanged rollout size must emit OutputWindow with had_output=false")

            fake_process.finish(-9)
            exit_events = _wait_for_event_type_count(
                store,
                event_type="ProcessExitedObserved",
                expected_count=1,
                timeout_seconds=2.0,
            )
            all_events = store.list_events_after(0)
            if len(exit_events) != 1:
                return _fail("supervisor must asynchronously emit one exit event after process termination")
            exit_event = exit_events[0]
            if exit_event.event_type != "ProcessExitedObserved":
                return _fail("final stored proc event must be ProcessExitedObserved")
            exit_payload = json.loads(exit_event.payload_json)
            if exit_payload.get("pid") != 43210:
                return _fail("exit event must preserve pid from proc registry")
            if exit_payload.get("process_birth_time") != 1711965600.0:
                return _fail("exit event must preserve process_birth_time from proc registry")
            if exit_payload.get("signal_name") != "SIGKILL":
                return _fail("negative returncode must normalize into a signal name")
            if supervisor.has_actor(actor):
                return _fail("proc must drop actor observation state after exit")
            supervisor.stop()
            supervisor.start()
            supervisor.stop()
            if not _wait_until(lambda: store.read_last_applied_seq() == all_events[-1].seq, timeout_seconds=2.0):
                return _fail("proc exit event must wake the running core")
        finally:
            supervisor.stop()
            core.stop()

    hung_supervisor = ProcSupervisor(
        event_sink=lambda _event: None,
        event_notifier=None,
    )
    hung_thread = _HungThread()
    hung_supervisor._monitor_thread = hung_thread
    hung_supervisor.stop()
    if hung_supervisor._monitor_thread is not hung_thread:
        return _fail("proc.stop must preserve the monitor thread reference when join times out and the thread remains alive")
    if hung_thread.join_calls != [2.0]:
        return _fail("proc.stop must use the configured join timeout for the monitor thread")

    print("[loop-system-router-proc][PASS] minimal router proc waits in background, emits growth events, and normalizes exits")
    return 0


def _wait_for_event_count(store, *, expected_count: int, timeout_seconds: float):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        events = store.list_events_after(0)
        if len(events) >= expected_count:
            return events
        time.sleep(0.02)
    return store.list_events_after(0)


def _wait_for_event_type_count(store, *, event_type: str, expected_count: int, timeout_seconds: float):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        events = [event for event in store.list_events_after(0) if event.event_type == event_type]
        if len(events) >= expected_count:
            return events
        time.sleep(0.02)
    return [event for event in store.list_events_after(0) if event.event_type == event_type]


def _wait_until(predicate, *, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())


if __name__ == "__main__":
    raise SystemExit(main())
