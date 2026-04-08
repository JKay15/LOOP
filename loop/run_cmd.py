#!/usr/bin/env python3
"""Unified command runner with evidence capture.

Why this exists
--------------
Evidence-chain upgrades must not rely on Codex "describing" what it ran.
Instead, the runner captures command execution evidence in a deterministic,
structured form.

This wrapper is intentionally:
- stdlib-only
- non-shell (argv only)
- output-to-files first (so logs can be hashed and audited)

Contracts
---------
- docs/contracts/REPORTING_CONTRACT.md
- docs/contracts/RUNREPORT_CONTRACT.md
- docs/contracts/WORKFLOW_CONTRACT.md
"""

from __future__ import annotations

import hashlib
import os
import re
import signal
import subprocess
import time
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Union

from loop.runtime_noise import default_shared_uv_cache_dir

_ENV_UNSET_SENTINEL = "__LEANATLAS_ENV_UNSET__"
_DEFAULT_SEALED_PARENT_ENV_KEYS = (
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
    "TEMP",
    "TMP",
    "TMPDIR",
    "USER",
    "UV_CACHE_DIR",
    "all_proxy",
    "https_proxy",
    "http_proxy",
    "no_proxy",
)


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _ensure_log_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        return
    path.write_text("", encoding="utf-8")


def _sanitize_label(label: str) -> str:
    out = []
    for ch in label:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    s = "".join(out).strip("_")
    return s or "cmd"


@dataclass
class RunCmdResult:
    span: Dict[str, Any]
    stdout_text: Optional[str]
    stderr_text: Optional[str]


@dataclass
class RunningCmdHandle:
    proc: subprocess.Popen[Any]
    cmd: list[str]
    cwd: str
    label: str
    log_dir: Path
    stdout_path: Path
    stderr_path: Path
    started_at_s: float
    stdout_handle: Any
    stderr_handle: Any
    stdin_handle: Any = None
    started_new_session: bool = True


def _as_positive_timeout(value: Optional[int]) -> Optional[float]:
    if value is None:
        return None
    if value <= 0:
        return None
    return float(value)


def _as_nonnegative_int(value: int) -> int:
    return max(0, int(value))


def build_sealed_parent_env(
    *,
    env: Optional[Mapping[str, str]] = None,
    allowed_parent_env_keys: Optional[Sequence[str]] = None,
) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    for key in allowed_parent_env_keys or _DEFAULT_SEALED_PARENT_ENV_KEYS:
        normalized_key = str(key)
        inherited_value = os.environ.get(normalized_key)
        if inherited_value is None or inherited_value == "":
            continue
        merged[normalized_key] = str(inherited_value)
    for key, inherited_value in os.environ.items():
        if not str(key).startswith("FAKE_CODEX_"):
            continue
        if inherited_value is None or inherited_value == "":
            continue
        merged[str(key)] = str(inherited_value)
    if env is not None:
        for key, value in env.items():
            normalized_key = str(key)
            normalized_value = str(value)
            if normalized_value == _ENV_UNSET_SENTINEL:
                merged.pop(normalized_key, None)
                continue
            merged[normalized_key] = normalized_value
    return merged


def _prepare_env(
    cwd: Union[str, Path],
    env: Optional[Mapping[str, str]],
    *,
    inherit_parent_env: bool = True,
    allowed_parent_env_keys: Optional[Sequence[str]] = None,
) -> Dict[str, str]:
    merged = (
        dict(os.environ)
        if inherit_parent_env
        else build_sealed_parent_env(env=None, allowed_parent_env_keys=allowed_parent_env_keys)
    )
    if env is not None:
        for key, value in env.items():
            normalized_key = str(key)
            normalized_value = str(value)
            if normalized_value == _ENV_UNSET_SENTINEL:
                merged.pop(normalized_key, None)
                continue
            merged[normalized_key] = normalized_value

    uv_cache_dir = str(merged.get("UV_CACHE_DIR") or "").strip()
    if uv_cache_dir:
        try:
            Path(uv_cache_dir).mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        return merged

    cache_dir = default_shared_uv_cache_dir()
    cache_dir.mkdir(parents=True, exist_ok=True)
    merged["UV_CACHE_DIR"] = str(cache_dir)
    return merged


def _semantic_activity_marker(path: Path) -> tuple[str, int, int, int]:
    try:
        if not path.exists():
            return ("missing", 0, 0, 0)
        stat = path.stat()
        if not path.is_dir():
            return ("file", 1, int(stat.st_size), int(stat.st_mtime_ns))
        file_count = 0
        total_size = 0
        latest_mtime_ns = int(stat.st_mtime_ns)
        stack = [path]
        while stack:
            current = stack.pop()
            try:
                children = list(current.iterdir())
            except Exception:
                continue
            for child in children:
                try:
                    child_stat = child.stat()
                except FileNotFoundError:
                    continue
                latest_mtime_ns = max(latest_mtime_ns, int(child_stat.st_mtime_ns))
                if child.is_dir():
                    stack.append(child)
                    continue
                file_count += 1
                total_size += int(child_stat.st_size)
        return ("dir", file_count, total_size, latest_mtime_ns)
    except FileNotFoundError:
        return ("missing", 0, 0, 0)


def _terminal_file_marker(path: Path) -> tuple[str, int, int]:
    try:
        if not path.exists() or not path.is_file():
            return ("missing", 0, 0)
        stat = path.stat()
        return ("file", int(stat.st_size), int(stat.st_mtime_ns))
    except FileNotFoundError:
        return ("missing", 0, 0)


def _marker_has_nonempty_file(marker: tuple[str, int, int]) -> bool:
    return marker[0] == "file" and marker[1] > 0


def _terminate_process_tree(
    proc: subprocess.Popen[Any],
    *,
    grace_s: float = 0.5,
    use_process_group: bool = True,
) -> None:
    """Best-effort termination for proc + children in the same process group."""
    if proc.poll() is not None:
        return

    pgid: Optional[int]
    try:
        pgid = os.getpgid(proc.pid)
    except Exception:
        pgid = None

    if use_process_group and pgid is not None:
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            return
        except Exception:
            pass
    else:
        try:
            proc.terminate()
        except Exception:
            pass

    deadline = time.monotonic() + max(0.05, grace_s)
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.05)

    if use_process_group and pgid is not None:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            return
        except Exception:
            pass
    try:
        proc.kill()
    except Exception:
        pass
    try:
        proc.wait(timeout=1.0)
    except Exception:
        pass


def start_cmd(
    *,
    cmd: Sequence[str],
    cwd: Union[str, Path],
    log_dir: Path,
    label: str,
    env: Optional[Mapping[str, str]] = None,
    stdin_path: Optional[Union[str, Path]] = None,
    start_new_session: bool = True,
    inherit_parent_env: bool = True,
    allowed_parent_env_keys: Optional[Sequence[str]] = None,
) -> RunningCmdHandle:
    """Start a command with run_cmd-compatible logging, but do not wait for it."""

    if not cmd or not all(isinstance(x, str) and x for x in cmd):
        raise ValueError("cmd must be a non-empty sequence of non-empty strings")

    log_dir.mkdir(parents=True, exist_ok=True)
    safe = _sanitize_label(label)
    stdout_path = log_dir / f"{safe}.stdout.txt"
    stderr_path = log_dir / f"{safe}.stderr.txt"
    stdout_handle = stdout_path.open("w", encoding="utf-8", errors="replace")
    stderr_handle = stderr_path.open("w", encoding="utf-8", errors="replace")
    stdin_handle = None
    if stdin_path not in (None, ""):
        stdin_handle = Path(stdin_path).expanduser().resolve().open("r", encoding="utf-8", errors="replace")
    proc = subprocess.Popen(
        list(cmd),
        cwd=str(cwd),
        stdin=stdin_handle,
        stdout=stdout_handle,
        stderr=stderr_handle,
        text=True,
        env=_prepare_env(
            cwd,
            env,
            inherit_parent_env=inherit_parent_env,
            allowed_parent_env_keys=allowed_parent_env_keys,
        ),
        start_new_session=start_new_session,
    )
    return RunningCmdHandle(
        proc=proc,
        cmd=list(cmd),
        cwd=str(Path(cwd).resolve()),
        label=safe,
        log_dir=log_dir,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        started_at_s=time.time(),
        stdout_handle=stdout_handle,
        stderr_handle=stderr_handle,
        stdin_handle=stdin_handle,
        started_new_session=start_new_session,
    )


def terminate_running_cmd(handle: RunningCmdHandle, *, grace_s: float = 0.5) -> None:
    _terminate_process_tree(handle.proc, grace_s=grace_s, use_process_group=handle.started_new_session)


def finish_running_cmd(
    handle: RunningCmdHandle,
    *,
    timed_out: bool = False,
    timeout_kind: Optional[str] = None,
    capture_text: bool = False,
    max_capture_bytes: int = 2 * 1024 * 1024,
) -> RunCmdResult:
    """Finalize a command started by start_cmd() into a RunCmdResult."""

    if handle.proc.poll() is None:
        rc = int(handle.proc.wait())
    else:
        rc = int(handle.proc.poll())
    try:
        handle.stdout_handle.close()
    except Exception:
        pass
    try:
        handle.stderr_handle.close()
    except Exception:
        pass
    try:
        if handle.stdin_handle is not None:
            handle.stdin_handle.close()
    except Exception:
        pass

    dt_ms = int(round((time.time() - handle.started_at_s) * 1000.0))
    _ensure_log_file(handle.stdout_path)
    _ensure_log_file(handle.stderr_path)
    out_sha = _sha256_file(handle.stdout_path)
    err_sha = _sha256_file(handle.stderr_path)

    base = handle.log_dir.parent
    try:
        stdout_rel = handle.stdout_path.relative_to(base).as_posix()
    except Exception:
        stdout_rel = handle.stdout_path.as_posix()
    try:
        stderr_rel = handle.stderr_path.relative_to(base).as_posix()
    except Exception:
        stderr_rel = handle.stderr_path.as_posix()

    span: Dict[str, Any] = {
        "id": handle.label,
        "cmd": list(handle.cmd),
        "cwd": handle.cwd,
        "exit_code": rc,
        "stdout_path": stdout_rel,
        "stderr_path": stderr_rel,
        "stdout_sha256": out_sha,
        "stderr_sha256": err_sha,
        "duration_ms": dt_ms,
    }
    if timed_out:
        span["timed_out"] = True
        if timeout_kind:
            span["timeout_kind"] = timeout_kind

    stdout_text: Optional[str] = None
    stderr_text: Optional[str] = None
    if capture_text:
        _ensure_log_file(handle.stdout_path)
        _ensure_log_file(handle.stderr_path)
        with handle.stdout_path.open("rb") as f:
            stdout_text = f.read(max_capture_bytes).decode("utf-8", errors="replace")
        with handle.stderr_path.open("rb") as f:
            stderr_text = f.read(max_capture_bytes).decode("utf-8", errors="replace")
    return RunCmdResult(span=span, stdout_text=stdout_text, stderr_text=stderr_text)


def run_cmd(
    *,
    cmd: Sequence[str],
    cwd: Union[str, Path],
    log_dir: Path,
    label: str,
    timeout_s: Optional[int] = None,
    idle_timeout_s: Optional[int] = None,
    semantic_idle_timeout_s: Optional[int] = None,
    semantic_activity_streams: Optional[Sequence[str]] = None,
    semantic_activity_paths: Optional[Sequence[Union[str, Path]]] = None,
    terminal_success_paths: Optional[Sequence[Union[str, Path]]] = None,
    terminal_success_stable_s: Optional[float] = None,
    terminal_success_streams: Optional[Sequence[str]] = None,
    terminal_success_patterns: Optional[Sequence[str]] = None,
    reconnect_grace_s: Optional[int] = None,
    reconnect_max_events: int = 0,
    reconnect_pattern: str = r"\breconnect(?:ing|ed|ion)?\b",
    env: Optional[Mapping[str, str]] = None,
    stdin_path: Optional[Union[str, Path]] = None,
    capture_text: bool = False,
    max_capture_bytes: int = 2 * 1024 * 1024,
    inherit_parent_env: bool = True,
    allowed_parent_env_keys: Optional[Sequence[str]] = None,
) -> RunCmdResult:
    """Run a command and write stdout/stderr to files.

    Parameters
    ----------
    cmd:
      argv array (no shell).
    cwd:
      working directory.
    log_dir:
      where to write stdout/stderr files.
    label:
      filename prefix (caller-controlled; include stage/attempt index).
    timeout_s:
      optional timeout.
    idle_timeout_s:
      optional inactivity timeout in seconds (stdout/stderr unchanged).
    semantic_idle_timeout_s:
      optional timeout in seconds for lack of semantic progress on declared semantic streams/files.
    semantic_activity_streams:
      optional subset of {"stdout", "stderr"} counted as semantic progress when their files grow.
    semantic_activity_paths:
      optional extra file paths counted as semantic progress when their size increases.
    terminal_success_paths:
      optional file paths whose non-empty stable contents may authoritatively settle the command as success
      once an explicit terminal marker is also observed in the configured terminal_success_streams.
    terminal_success_stable_s:
      optional stability window in seconds required before a terminal_success_path can settle the command.
    terminal_success_streams:
      optional subset of {"stdout", "stderr"} searched for terminal_success_patterns.
    terminal_success_patterns:
      optional regex patterns that must be observed on terminal_success_streams before a terminal_success_path
      can settle the command.
    reconnect_grace_s:
      optional extra grace seconds granted when reconnect markers appear in output.
    reconnect_max_events:
      max reconnect-marker events that can grant grace.
    reconnect_pattern:
      regex pattern used to detect reconnect markers in incremental output.
    env:
      optional environment overrides.
    stdin_path:
      optional file path to feed to the child process on stdin.
    capture_text:
      if True, also return stdout/stderr strings (bounded by max_capture_bytes).
    max_capture_bytes:
      max bytes read back into memory when capture_text=True.

    Returns
    -------
    RunCmdResult(span=..., stdout_text=?, stderr_text=?)

    Span fields (minimum):
      id, cmd, cwd, exit_code, stdout_path, stderr_path, stdout_sha256, stderr_sha256, duration_ms
    """

    if not cmd or not all(isinstance(x, str) and x for x in cmd):
        raise ValueError("cmd must be a non-empty sequence of non-empty strings")

    log_dir.mkdir(parents=True, exist_ok=True)
    safe = _sanitize_label(label)

    stdout_path = log_dir / f"{safe}.stdout.txt"
    stderr_path = log_dir / f"{safe}.stderr.txt"

    t0 = time.time()
    hard_timeout_s = _as_positive_timeout(timeout_s)
    idle_timeout_val_s = _as_positive_timeout(idle_timeout_s)
    semantic_idle_timeout_val_s = _as_positive_timeout(semantic_idle_timeout_s)
    terminal_success_stable_val_s = _as_positive_timeout(terminal_success_stable_s)
    reconnect_grace_val_s = _as_positive_timeout(reconnect_grace_s)
    reconnect_max_events_val = _as_nonnegative_int(reconnect_max_events)
    reconnect_re: Optional[re.Pattern[str]] = None
    if reconnect_grace_val_s is not None and reconnect_max_events_val > 0:
        reconnect_re = re.compile(reconnect_pattern, flags=re.IGNORECASE)
    timed_out = False
    timeout_kind: Optional[str] = None
    cmd_env = _prepare_env(
        cwd,
        env,
        inherit_parent_env=inherit_parent_env,
        allowed_parent_env_keys=allowed_parent_env_keys,
    )
    semantic_streams = {str(name).strip().lower() for name in (semantic_activity_streams or ()) if str(name).strip()}
    bad_streams = sorted(stream for stream in semantic_streams if stream not in {"stdout", "stderr"})
    if bad_streams:
        raise ValueError(f"semantic_activity_streams must only contain stdout/stderr; got: {', '.join(bad_streams)}")
    terminal_streams = {
        str(name).strip().lower() for name in (terminal_success_streams or ("stdout", "stderr")) if str(name).strip()
    }
    bad_terminal_streams = sorted(stream for stream in terminal_streams if stream not in {"stdout", "stderr"})
    if bad_terminal_streams:
        raise ValueError(
            f"terminal_success_streams must only contain stdout/stderr; got: {', '.join(bad_terminal_streams)}"
        )
    terminal_success_res = [
        re.compile(str(pattern), flags=re.IGNORECASE)
        for pattern in (terminal_success_patterns or ())
        if str(pattern).strip()
    ]
    semantic_paths: list[Path] = []
    base_cwd = Path(cwd).resolve()
    for raw in semantic_activity_paths or ():
        p = Path(raw)
        if not p.is_absolute():
            p = base_cwd / p
        semantic_paths.append(p.resolve())
    terminal_paths: list[Path] = []
    for raw in terminal_success_paths or ():
        p = Path(raw)
        if not p.is_absolute():
            p = base_cwd / p
        terminal_paths.append(p.resolve())

    stdin_cm = (
        Path(stdin_path).expanduser().resolve().open("r", encoding="utf-8", errors="replace")
        if stdin_path not in (None, "")
        else nullcontext(None)
    )
    with stdout_path.open("w", encoding="utf-8", errors="replace") as out_f, stderr_path.open(
        "w", encoding="utf-8", errors="replace"
    ) as err_f, stdin_cm as in_f:
        p = subprocess.Popen(
            list(cmd),
            cwd=str(cwd),
            stdin=in_f,
            stdout=out_f,
            stderr=err_f,
            text=True,
            env=cmd_env,
            start_new_session=True,
        )
        start_mono = time.monotonic()
        hard_deadline_mono = (start_mono + hard_timeout_s) if hard_timeout_s is not None else None
        last_activity_mono = start_mono
        last_semantic_activity_mono = start_mono
        idle_extend_until_mono = start_mono
        last_stdout_size = 0
        last_stderr_size = 0
        semantic_stdout_size = 0
        semantic_stderr_size = 0
        semantic_path_markers = {
            path: _semantic_activity_marker(path)
            for path in semantic_paths
        }
        scan_stdout_pos = 0
        scan_stderr_pos = 0
        reconnect_events_applied = 0
        rc: Optional[int] = None
        terminal_success = False
        terminal_success_path: Optional[Path] = None
        terminal_success_observed_exit_code: Optional[int] = None
        terminal_path_markers = {
            path: _terminal_file_marker(path)
            for path in terminal_paths
        }
        terminal_path_ready_since = {
            path: None
            for path in terminal_paths
        }
        terminal_scan_stdout_pos = 0
        terminal_scan_stderr_pos = 0
        terminal_stream_match_seen = False
        terminal_success_enabled = bool(terminal_path_markers) and bool(terminal_success_res)

        while True:
            polled = p.poll()
            if polled is not None:
                rc = int(polled)
                break

            now = time.monotonic()
            stdout_size = stdout_path.stat().st_size
            stderr_size = stderr_path.stat().st_size
            should_timeout = False
            if hard_deadline_mono is not None and now >= hard_deadline_mono:
                should_timeout = True
            elif idle_timeout_val_s is not None:
                if stdout_size != last_stdout_size or stderr_size != last_stderr_size:
                    last_stdout_size = stdout_size
                    last_stderr_size = stderr_size
                    last_activity_mono = now
                    if reconnect_re is not None:
                        if stdout_size < scan_stdout_pos:
                            scan_stdout_pos = 0
                        if stderr_size < scan_stderr_pos:
                            scan_stderr_pos = 0

                        reconnect_hits = 0
                        if stdout_size > scan_stdout_pos:
                            read_start = scan_stdout_pos
                            if (stdout_size - scan_stdout_pos) > 64 * 1024:
                                read_start = stdout_size - 64 * 1024
                            with stdout_path.open("rb") as sf:
                                sf.seek(read_start)
                                chunk = sf.read(stdout_size - read_start)
                            scan_stdout_pos = stdout_size
                            reconnect_hits += len(reconnect_re.findall(chunk.decode("utf-8", errors="replace")))
                        if stderr_size > scan_stderr_pos:
                            read_start = scan_stderr_pos
                            if (stderr_size - scan_stderr_pos) > 64 * 1024:
                                read_start = stderr_size - 64 * 1024
                            with stderr_path.open("rb") as ef:
                                ef.seek(read_start)
                                chunk = ef.read(stderr_size - read_start)
                            scan_stderr_pos = stderr_size
                            reconnect_hits += len(reconnect_re.findall(chunk.decode("utf-8", errors="replace")))

                        while reconnect_hits > 0 and reconnect_events_applied < reconnect_max_events_val:
                            reconnect_hits -= 1
                            reconnect_events_applied += 1
                            if hard_deadline_mono is not None and reconnect_grace_val_s is not None:
                                hard_deadline_mono += reconnect_grace_val_s
                            if reconnect_grace_val_s is not None:
                                idle_extend_until_mono = max(idle_extend_until_mono, now + reconnect_grace_val_s)
                elif (now - last_activity_mono) >= idle_timeout_val_s:
                    if now >= idle_extend_until_mono:
                        should_timeout = True
                        timeout_kind = "transport"

            semantic_progress = False
            if "stdout" in semantic_streams and stdout_size > semantic_stdout_size:
                semantic_progress = True
            semantic_stdout_size = stdout_size
            if "stderr" in semantic_streams and stderr_size > semantic_stderr_size:
                semantic_progress = True
            semantic_stderr_size = stderr_size
            semantic_path_progress = False
            for path, previous_marker in tuple(semantic_path_markers.items()):
                current_marker = _semantic_activity_marker(path)
                if current_marker != previous_marker:
                    semantic_progress = True
                    semantic_path_progress = True
                semantic_path_markers[path] = current_marker
            if semantic_path_progress:
                last_activity_mono = now
            if semantic_progress:
                last_semantic_activity_mono = now

            if terminal_success_enabled:
                if "stdout" in terminal_streams:
                    if stdout_size < terminal_scan_stdout_pos:
                        terminal_scan_stdout_pos = 0
                    if stdout_size > terminal_scan_stdout_pos:
                        read_start = terminal_scan_stdout_pos
                        if (stdout_size - terminal_scan_stdout_pos) > 64 * 1024:
                            read_start = stdout_size - 64 * 1024
                        with stdout_path.open("rb") as sf:
                            sf.seek(read_start)
                            chunk = sf.read(stdout_size - read_start)
                        terminal_scan_stdout_pos = stdout_size
                        text = chunk.decode("utf-8", errors="replace")
                        if any(pattern.search(text) for pattern in terminal_success_res):
                            terminal_stream_match_seen = True
                if "stderr" in terminal_streams:
                    if stderr_size < terminal_scan_stderr_pos:
                        terminal_scan_stderr_pos = 0
                    if stderr_size > terminal_scan_stderr_pos:
                        read_start = terminal_scan_stderr_pos
                        if (stderr_size - terminal_scan_stderr_pos) > 64 * 1024:
                            read_start = stderr_size - 64 * 1024
                        with stderr_path.open("rb") as ef:
                            ef.seek(read_start)
                            chunk = ef.read(stderr_size - read_start)
                        terminal_scan_stderr_pos = stderr_size
                        text = chunk.decode("utf-8", errors="replace")
                        if any(pattern.search(text) for pattern in terminal_success_res):
                            terminal_stream_match_seen = True

            if terminal_success_enabled and terminal_success_stable_val_s is not None and terminal_stream_match_seen:
                for path, previous_marker in tuple(terminal_path_markers.items()):
                    current_marker = _terminal_file_marker(path)
                    if current_marker != previous_marker:
                        terminal_path_markers[path] = current_marker
                        terminal_path_ready_since[path] = now if _marker_has_nonempty_file(current_marker) else None
                    ready_since = terminal_path_ready_since.get(path)
                    if ready_since is None or not _marker_has_nonempty_file(current_marker):
                        continue
                    if (now - ready_since) < terminal_success_stable_val_s:
                        continue
                    if (now - last_activity_mono) < terminal_success_stable_val_s:
                        continue
                    pre_terminal_poll = p.poll()
                    if pre_terminal_poll is not None:
                        rc = int(pre_terminal_poll)
                        break
                    terminal_success = True
                    terminal_success_path = path
                    if p.poll() is None:
                        _terminate_process_tree(p)
                    observed_exit_code = p.poll()
                    terminal_success_observed_exit_code = (
                        int(observed_exit_code) if observed_exit_code is not None else None
                    )
                    rc = 0
                    break
                if terminal_success:
                    break
                if rc is not None:
                    break

            if not should_timeout and semantic_idle_timeout_val_s is not None:
                if (now - last_semantic_activity_mono) >= semantic_idle_timeout_val_s:
                    should_timeout = True
                    timeout_kind = "semantic"

            if should_timeout:
                timed_out = True
                _terminate_process_tree(p)
                rc = 124
                break

            time.sleep(0.05)

        if rc is None:
            rc = int(p.wait())

    dt_ms = int(round((time.time() - t0) * 1000.0))

    _ensure_log_file(stdout_path)
    _ensure_log_file(stderr_path)
    out_sha = _sha256_file(stdout_path)
    err_sha = _sha256_file(stderr_path)

    # Make paths portable: record relative to log_dir.parent when possible.
    base = log_dir.parent
    try:
        stdout_rel = stdout_path.relative_to(base).as_posix()
    except Exception:
        stdout_rel = stdout_path.as_posix()
    try:
        stderr_rel = stderr_path.relative_to(base).as_posix()
    except Exception:
        stderr_rel = stderr_path.as_posix()

    span: Dict[str, Any] = {
        "id": safe,
        "cmd": list(cmd),
        "cwd": str(Path(cwd).resolve()),
        "exit_code": rc,
        "stdout_path": stdout_rel,
        "stderr_path": stderr_rel,
        "stdout_sha256": out_sha,
        "stderr_sha256": err_sha,
        "duration_ms": dt_ms,
    }
    if timed_out:
        span["timed_out"] = True
    if timeout_kind is not None:
        span["timeout_kind"] = timeout_kind
    if terminal_success:
        span["terminal_success"] = True
        if terminal_success_path is not None:
            try:
                span["terminal_success_path"] = str(terminal_success_path.relative_to(base))
            except Exception:
                span["terminal_success_path"] = str(terminal_success_path)
        if terminal_success_observed_exit_code is not None:
            span["terminal_success_observed_exit_code"] = terminal_success_observed_exit_code
    uv_cache_dir = str(cmd_env.get("UV_CACHE_DIR") or "")
    if uv_cache_dir:
        span["uv_cache_dir"] = uv_cache_dir

    if not capture_text:
        return RunCmdResult(span=span, stdout_text=None, stderr_text=None)

    # Bounded read-back.
    _ensure_log_file(stdout_path)
    _ensure_log_file(stderr_path)
    out_bytes = stdout_path.read_bytes()[:max_capture_bytes]
    err_bytes = stderr_path.read_bytes()[:max_capture_bytes]
    stdout_text = out_bytes.decode("utf-8", errors="replace")
    stderr_text = err_bytes.decode("utf-8", errors="replace")
    return RunCmdResult(span=span, stdout_text=stdout_text, stderr_text=stderr_text)
