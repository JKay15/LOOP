"""Committed AI launch helpers.

This module centralizes the decision of how repo-owned AI subprocesses are
started. The first consumer is committed child launch, where launching a
second `codex exec` directly from inside a live `codex exec` kernel session
can fail during startup. In those cases we prefer asking a host tmux server to
materialize the child command instead of nesting the AI subprocess directly in
the current process tree.
"""

from __future__ import annotations

import hashlib
import os
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from .run_cmd import RunCmdResult, RunningCmdHandle, finish_running_cmd, start_cmd, terminate_running_cmd

_SAFE_PARENT_ENV_KEYS = {
    "ALL_PROXY",
    "HOME",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "LOGNAME",
    "NO_COLOR",
    "NO_PROXY",
    "PATH",
    "SSH_AUTH_SOCK",
    "TMP",
    "TMPDIR",
    "TEMP",
    "USER",
    "UV_CACHE_DIR",
    "all_proxy",
    "https_proxy",
    "http_proxy",
    "no_proxy",
}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _ensure_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("", encoding="utf-8")


def _result_from_log_files(
    *,
    cmd: Sequence[str],
    cwd: str,
    log_dir: Path,
    stdout_path: Path,
    stderr_path: Path,
    exit_code: int,
    started_at_s: float,
    timed_out: bool = False,
    timeout_kind: str | None = None,
) -> RunCmdResult:
    _ensure_file(stdout_path)
    _ensure_file(stderr_path)
    out_sha = _sha256_file(stdout_path)
    err_sha = _sha256_file(stderr_path)
    duration_ms = max(0, int((time.time() - started_at_s) * 1000))
    base = log_dir.parent
    try:
        stdout_rel = stdout_path.relative_to(base).as_posix()
    except ValueError:
        stdout_rel = stdout_path.as_posix()
    try:
        stderr_rel = stderr_path.relative_to(base).as_posix()
    except ValueError:
        stderr_rel = stderr_path.as_posix()
    span: dict[str, Any] = {
        "cmd": list(cmd),
        "cwd": str(cwd),
        "exit_code": int(exit_code),
        "stdout_path": stdout_rel,
        "stderr_path": stderr_rel,
        "stdout_sha256": out_sha,
        "stderr_sha256": err_sha,
        "duration_ms": duration_ms,
    }
    if timed_out:
        span["timed_out"] = True
    if timeout_kind:
        span["timeout_kind"] = str(timeout_kind)
    return RunCmdResult(span=span, stdout_text=None, stderr_text=None)


@dataclass
class AiLaunchHandle:
    mode: str
    cmd: list[str]
    cwd: str
    log_dir: Path
    stdout_path: Path
    stderr_path: Path
    started_at_s: float
    pid: int | None = None
    direct_handle: RunningCmdHandle | None = None
    tmux_session_name: str = ""
    tmux_window_index: str = ""
    tmux_pane_id: str = ""
    tmux_pane_pid: int | None = None
    tmux_exit_code_path: Path | None = None
    tmux_script_path: Path | None = None


def _child_launch_bridge_enabled() -> bool:
    raw = str(os.environ.get("LOOP_CHILD_LAUNCH_BRIDGE_MODE") or "").strip().lower()
    if raw in {"direct", "off", "disabled"}:
        return False
    if raw in {"tmux", "on", "enabled"}:
        return True
    return bool(str(os.environ.get("CODEX_THREAD_ID") or "").strip())


def _tmux_available() -> bool:
    return shutil.which("tmux") is not None


def _should_use_tmux_bridge(cmd: Sequence[str]) -> bool:
    if len(cmd) < 2:
        return False
    if str(cmd[0]) != "codex" or str(cmd[1]) != "exec":
        return False
    if not _child_launch_bridge_enabled():
        return False
    return _tmux_available()


def _resolve_tmux_target_session() -> str:
    if str(os.environ.get("TMUX") or "").strip():
        probe = subprocess.run(
            ["tmux", "display-message", "-p", "#S"],
            text=True,
            capture_output=True,
            check=False,
        )
        session_name = str(probe.stdout or "").strip()
        if probe.returncode == 0 and session_name:
            return session_name
    fallback = str(os.environ.get("LOOP_AI_TMUX_SESSION") or "").strip()
    if fallback:
        return fallback
    probe = subprocess.run(
        ["tmux", "list-sessions", "-F", "#S"],
        text=True,
        capture_output=True,
        check=False,
    )
    if probe.returncode == 0:
        sessions = [str(line).strip() for line in str(probe.stdout or "").splitlines() if str(line).strip()]
        if "main" in sessions:
            return "main"
        if sessions:
            return sessions[0]
    return "loop-product-ai"


def _tmux_env(env: Mapping[str, str] | None) -> dict[str, str]:
    merged: dict[str, str] = {}
    for key in _SAFE_PARENT_ENV_KEYS:
        value = str(os.environ.get(key) or "").strip()
        if value:
            merged[key] = value
    for key, value in dict(env or {}).items():
        merged[str(key)] = str(value)
    for key in list(merged):
        if key.startswith("CODEX_"):
            merged.pop(key, None)
        if key.startswith("TMUX"):
            merged.pop(key, None)
    return merged


def _write_tmux_wrapper_script(
    *,
    script_path: Path,
    cwd: Path,
    cmd: Sequence[str],
    env: Mapping[str, str],
    stdout_path: Path,
    stderr_path: Path,
    exit_code_path: Path,
    stdin_path: Path | None,
) -> None:
    lines = ["#!/bin/sh", "set +e", f"cd {shlex.quote(str(cwd))} || exit 111"]
    for key, value in env.items():
        lines.append(f"export {key}={shlex.quote(str(value))}")
    cmd_text = shlex.join([str(item) for item in cmd])
    redirect = ""
    if stdin_path is not None and str(stdin_path):
        redirect += f" < {shlex.quote(str(stdin_path))}"
    redirect += f" > {shlex.quote(str(stdout_path))} 2> {shlex.quote(str(stderr_path))}"
    lines.append(f"{cmd_text}{redirect}")
    lines.append("code=$?")
    lines.append(f"printf '%s\\n' \"$code\" > {shlex.quote(str(exit_code_path))}")
    lines.append("exit \"$code\"")
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    script_path.chmod(0o755)


def _start_tmux_launch(
    *,
    cmd: Sequence[str],
    cwd: Path,
    log_dir: Path,
    label: str,
    env: Mapping[str, str] | None,
    stdin_path: str | Path | None,
) -> AiLaunchHandle:
    log_dir.mkdir(parents=True, exist_ok=True)
    safe_label = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in label).strip("_") or "ai"
    stdout_path = log_dir / f"{safe_label}.stdout.txt"
    stderr_path = log_dir / f"{safe_label}.stderr.txt"
    exit_code_path = log_dir / f"{safe_label}.exit_code.txt"
    wrapper_script = log_dir / f"{safe_label}.tmux-launch.sh"
    for path in (stdout_path, stderr_path, exit_code_path):
        if path.exists():
            path.unlink()
    _write_tmux_wrapper_script(
        script_path=wrapper_script,
        cwd=cwd,
        cmd=cmd,
        env=_tmux_env(env),
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        exit_code_path=exit_code_path,
        stdin_path=Path(stdin_path).expanduser().resolve() if stdin_path not in (None, "") else None,
    )
    session_name = _resolve_tmux_target_session()
    probe = subprocess.run(
        [
            "tmux",
            "new-window",
            "-d",
            "-P",
            "-F",
            "#{session_name} #{window_index} #{pane_id} #{pane_pid}",
            "-t",
            session_name,
            "-n",
            safe_label[:40],
            str(wrapper_script),
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if probe.returncode != 0:
        raise RuntimeError(f"tmux bridge failed to start window: {probe.stderr.strip() or probe.stdout.strip()}")
    parts = str(probe.stdout or "").strip().split()
    if len(parts) != 4:
        raise RuntimeError(f"tmux bridge returned unexpected window metadata: {probe.stdout!r}")
    out_session, window_index, pane_id, pane_pid = parts
    return AiLaunchHandle(
        mode="tmux",
        cmd=[str(item) for item in cmd],
        cwd=str(cwd),
        log_dir=log_dir,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        started_at_s=time.time(),
        pid=int(pane_pid),
        tmux_session_name=out_session,
        tmux_window_index=window_index,
        tmux_pane_id=pane_id,
        tmux_pane_pid=int(pane_pid),
        tmux_exit_code_path=exit_code_path,
        tmux_script_path=wrapper_script,
    )


def _start_direct_launch(
    *,
    cmd: Sequence[str],
    cwd: Path,
    log_dir: Path,
    label: str,
    env: Mapping[str, str] | None,
    stdin_path: str | Path | None,
    start_new_session: bool,
) -> AiLaunchHandle:
    handle = start_cmd(
        cmd=cmd,
        cwd=cwd,
        log_dir=log_dir,
        label=label,
        env=env,
        stdin_path=stdin_path,
        start_new_session=start_new_session,
    )
    return AiLaunchHandle(
        mode="direct",
        cmd=[str(item) for item in cmd],
        cwd=str(Path(cwd).resolve()),
        log_dir=log_dir,
        stdout_path=handle.stdout_path,
        stderr_path=handle.stderr_path,
        started_at_s=handle.started_at_s,
        pid=int(handle.proc.pid),
        direct_handle=handle,
    )


def start_ai_launch(
    *,
    cmd: Sequence[str],
    cwd: Path,
    log_dir: Path,
    label: str,
    env: Mapping[str, str] | None = None,
    stdin_path: str | Path | None = None,
    start_new_session: bool = False,
) -> AiLaunchHandle:
    if _should_use_tmux_bridge(cmd):
        return _start_tmux_launch(
            cmd=cmd,
            cwd=Path(cwd).expanduser().resolve(),
            log_dir=log_dir,
            label=label,
            env=env,
            stdin_path=stdin_path,
        )
    return _start_direct_launch(
        cmd=cmd,
        cwd=Path(cwd).expanduser().resolve(),
        log_dir=log_dir,
        label=label,
        env=env,
        stdin_path=stdin_path,
        start_new_session=start_new_session,
    )


def detach_ai_launch_handle(handle: AiLaunchHandle) -> None:
    if handle.mode != "direct" or handle.direct_handle is None:
        return
    for stream in (handle.direct_handle.stdout_handle, handle.direct_handle.stderr_handle):
        try:
            stream.close()
        except Exception:
            pass
    try:
        if handle.direct_handle.stdin_handle is not None:
            handle.direct_handle.stdin_handle.close()
    except Exception:
        pass


def ai_launch_exit_code(handle: AiLaunchHandle) -> int | None:
    if handle.mode == "direct":
        if handle.direct_handle is None:
            return None
        polled = handle.direct_handle.proc.poll()
        if polled is None:
            return None
        return int(polled)
    exit_code_path = handle.tmux_exit_code_path
    if exit_code_path is None or not exit_code_path.exists():
        return None
    try:
        return int(str(exit_code_path.read_text(encoding="utf-8")).strip())
    except Exception:
        return None


def terminate_ai_launch(handle: AiLaunchHandle, *, grace_s: float = 0.5) -> None:
    if handle.mode == "direct":
        if handle.direct_handle is not None:
            terminate_running_cmd(handle.direct_handle, grace_s=grace_s)
        return
    if not handle.tmux_pane_id:
        return
    subprocess.run(["tmux", "kill-pane", "-t", handle.tmux_pane_id], text=True, capture_output=True, check=False)
    deadline = time.monotonic() + max(0.1, grace_s)
    while time.monotonic() < deadline:
        if ai_launch_exit_code(handle) is not None:
            return
        time.sleep(0.05)


def finish_ai_launch(
    handle: AiLaunchHandle,
    *,
    timed_out: bool = False,
    timeout_kind: str | None = None,
) -> RunCmdResult:
    if handle.mode == "direct":
        if handle.direct_handle is None:
            raise RuntimeError("missing direct handle")
        return finish_running_cmd(handle.direct_handle, timed_out=timed_out, timeout_kind=timeout_kind, capture_text=False)
    exit_code = ai_launch_exit_code(handle)
    if exit_code is None:
        raise RuntimeError("tmux-backed AI launch has not exited yet")
    return _result_from_log_files(
        cmd=handle.cmd,
        cwd=handle.cwd,
        log_dir=handle.log_dir,
        stdout_path=handle.stdout_path,
        stderr_path=handle.stderr_path,
        exit_code=exit_code,
        started_at_s=handle.started_at_s,
        timed_out=timed_out,
        timeout_kind=timeout_kind,
    )
