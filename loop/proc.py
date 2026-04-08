"""Minimal background proc supervisor for router v0."""

from __future__ import annotations

import os
import signal
import sys
import threading
import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable, Protocol

from loop.events import (
    ActorRef,
    OutputWindow,
    ProcessExitedObserved,
    RouterEvent,
)
from loop.rollout import fallback_rollout_path_for_session


class ProcBackend(Protocol):
    """Platform-specific proc backend marker."""

    platform_key: str


@dataclass(frozen=True)
class DarwinProcBackend:
    """macOS proc backend marker for v0."""

    platform_key: str = "darwin"


def select_proc_backend(platform_key: str | None = None) -> ProcBackend:
    """Return the supported v0 proc backend for the current platform."""

    normalized = str(platform_key or sys.platform).strip().lower()
    if normalized == "darwin":
        return DarwinProcBackend()
    raise RuntimeError(f"unsupported proc backend platform: {normalized}")


class WaitableProcess(Protocol):
    """Minimal process handle needed by the proc supervisor."""

    pid: int

    def wait(self) -> int: ...


@dataclass
class _ObservedActor:
    """Non-durable proc observation state for one active actor."""

    actor: ActorRef
    process: WaitableProcess
    process_birth_time: float | None
    session_id: str
    rollout_path: Path | None
    last_seen_size: int
    next_window_deadline: datetime


PROC_THREAD_JOIN_TIMEOUT_SECONDS = 2.0
LOGGER = logging.getLogger(__name__)


class ProcSupervisor:
    """Observe Codex rollout growth and asynchronously emit router events."""

    def __init__(
        self,
        *,
        codex_home: str | Path | None = None,
        backend: ProcBackend | None = None,
        output_window_seconds: float = 60.0,
        event_sink: Callable[[RouterEvent], int | None],
        event_notifier: Callable[[], object] | None = None,
    ) -> None:
        self._codex_home = Path(codex_home or (Path.home() / ".codex")).expanduser().resolve()
        self._backend = backend or select_proc_backend()
        self._output_window = timedelta(seconds=float(output_window_seconds))
        self._event_sink = event_sink
        self._event_notifier = event_notifier
        self._observed: dict[ActorRef, _ObservedActor] = {}
        self._session_path_cache: dict[str, Path] = {}
        self._condition = threading.Condition()
        self._stop_requested = False
        self._monitor_thread: threading.Thread | None = None
        self._exit_threads: dict[ActorRef, threading.Thread] = {}

    def start(self) -> None:
        """Start the background observation loop once."""

        with self._condition:
            if self._monitor_thread is not None:
                return
            self._stop_requested = False
            self._monitor_thread = threading.Thread(
                target=self._run_monitor_loop,
                name="router-proc-supervisor",
            )
            self._monitor_thread.start()

    def stop(self) -> None:
        """Stop the background observation loop."""

        with self._condition:
            self._stop_requested = True
            self._condition.notify_all()
            monitor_thread = self._monitor_thread
        if monitor_thread is not None:
            monitor_thread.join(timeout=PROC_THREAD_JOIN_TIMEOUT_SECONDS)
        monitor_thread_alive = bool(monitor_thread is not None and monitor_thread.is_alive())
        if monitor_thread_alive:
            LOGGER.warning(
                "router proc monitor thread did not exit within %.1fs",
                PROC_THREAD_JOIN_TIMEOUT_SECONDS,
            )
        with self._condition:
            if not monitor_thread_alive:
                self._monitor_thread = None
                self._observed.clear()
                self._session_path_cache.clear()
                self._exit_threads.clear()

    def register_actor(
        self,
        actor: ActorRef,
        *,
        process: WaitableProcess,
        process_birth_time: float | None,
        session_id: str,
        rollout_path: str | Path | None = None,
        now: datetime | None = None,
    ) -> Path | None:
        """Register one actor attempt for rollout-growth observation."""

        observed_at = now or datetime.now(timezone.utc)
        resolved_rollout_path = (
            Path(rollout_path).expanduser().resolve()
            if rollout_path not in (None, "")
            else self.resolve_rollout_path(session_id)
        )
        state = _ObservedActor(
            actor=actor,
            process=process,
            process_birth_time=process_birth_time,
            session_id=str(session_id),
            rollout_path=resolved_rollout_path,
            last_seen_size=self._current_size(resolved_rollout_path),
            next_window_deadline=observed_at + self._output_window,
        )
        with self._condition:
            self._observed[actor] = state
            self._condition.notify_all()

        watcher = threading.Thread(
            target=self._wait_for_exit,
            args=(actor, process),
            name=f"router-proc-exit-{actor.node_id}-{actor.actor_kind.value}",
            daemon=True,
        )
        self._exit_threads[actor] = watcher
        watcher.start()
        return resolved_rollout_path

    def has_actor(self, actor: ActorRef) -> bool:
        """Return whether proc is still observing this actor."""

        with self._condition:
            return actor in self._observed

    def terminate_actor(self, actor: ActorRef) -> bool:
        """Best-effort terminate for one observed actor."""

        with self._condition:
            state = self._observed.get(actor)
        if state is None:
            return False
        process = state.process
        terminate = getattr(process, "terminate", None)
        if callable(terminate):
            try:
                terminate()
                return True
            except Exception:  # noqa: BLE001
                pass
        pid = int(getattr(process, "pid", 0) or 0)
        if pid <= 0:
            return False
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except OSError:
            return False

    def resolve_rollout_path(self, session_id: str) -> Path | None:
        """Find the Codex rollout JSONL for one session id."""

        chosen = fallback_rollout_path_for_session(
            session_id,
            codex_home=self._codex_home,
            preferred_paths=[self._session_path_cache.get(session_id)],
        )
        if chosen is None:
            return None
        self._session_path_cache[session_id] = chosen
        return chosen

    def _run_monitor_loop(self) -> None:
        while True:
            with self._condition:
                if self._stop_requested:
                    return
                timeout = self._seconds_until_next_deadline_locked()
                self._condition.wait(timeout=timeout)
                if self._stop_requested:
                    return
                due_states = self._due_states_locked(datetime.now(timezone.utc))

            observed_at = datetime.now(timezone.utc)
            for state in due_states:
                self._check_output_window(state, observed_at)

    def _seconds_until_next_deadline_locked(self) -> float | None:
        if not self._observed:
            return None
        now = datetime.now(timezone.utc)
        next_deadline = min(state.next_window_deadline for state in self._observed.values())
        delta = (next_deadline - now).total_seconds()
        return max(0.0, delta)

    def _due_states_locked(self, now: datetime) -> list[_ObservedActor]:
        return [
            state
            for state in self._observed.values()
            if now >= state.next_window_deadline
        ]

    def _check_output_window(self, state: _ObservedActor, observed_at: datetime) -> None:
        refreshed_rollout_path = state.rollout_path
        current_size = self._current_size(refreshed_rollout_path)
        path_changed = False
        if state.session_id and (
            refreshed_rollout_path is None or current_size <= state.last_seen_size
        ):
            fallback_rollout_path = self.resolve_rollout_path(state.session_id)
            if fallback_rollout_path is not None and fallback_rollout_path != refreshed_rollout_path:
                refreshed_rollout_path = fallback_rollout_path
                current_size = self._current_size(refreshed_rollout_path)
                path_changed = True
        had_output = bool(current_size > state.last_seen_size or (path_changed and current_size > 0))
        self._emit_event(
            OutputWindow(
                actor=state.actor,
                had_output=had_output,
                observed_at=observed_at,
            )
        )
        state.rollout_path = refreshed_rollout_path
        state.last_seen_size = current_size
        state.next_window_deadline = observed_at + self._output_window

    def _wait_for_exit(self, actor: ActorRef, process: WaitableProcess) -> None:
        returncode = process.wait()
        with self._condition:
            state = self._observed.pop(actor, None)
            self._condition.notify_all()
        if state is None:
            return
        exit_code, signal_name = self._normalize_returncode(returncode)
        self._emit_event(
            ProcessExitedObserved(
                actor=actor,
                pid=int(process.pid),
                process_birth_time=state.process_birth_time,
                exit_code=exit_code,
                signal_name=signal_name,
                occurred_at=datetime.now(timezone.utc),
            )
        )

    def _emit_event(self, event: RouterEvent) -> None:
        self._event_sink(event)
        if self._event_notifier is not None:
            self._event_notifier()

    def _current_size(self, rollout_path: Path | None) -> int:
        if rollout_path is None:
            return 0
        try:
            return int(rollout_path.stat().st_size)
        except FileNotFoundError:
            return 0

    def _normalize_returncode(self, returncode: int | None) -> tuple[int | None, str | None]:
        if returncode is None:
            return None, None
        if returncode < 0:
            signal_number = -int(returncode)
            try:
                signal_name = signal.Signals(signal_number).name
            except ValueError:
                signal_name = f"SIG{signal_number}"
            return None, signal_name
        return int(returncode), None
