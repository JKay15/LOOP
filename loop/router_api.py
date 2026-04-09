"""Router control API entrypoint for operator-side router commands."""

from __future__ import annotations

import argparse
from pathlib import Path

from loop.agent_api import (
    _handle_resume,
    _handle_start,
    _handle_status,
    _print_payload,
    _resolve_router_db_path,
)
from loop.core import RouterCore, RouterPauseError, notify_router_wakeup, router_wakeup_socket_path
from loop.store import RouterStore


def _handle_pause(args: argparse.Namespace) -> int:
    router_db_path = _resolve_router_db_path(args.router_db)
    store = RouterStore(router_db_path)
    core = RouterCore(
        store=store,
        wakeup_socket_path=router_wakeup_socket_path(store.db_path),
    )
    try:
        payload = core.prepare_pause(reason=args.reason)
    except RouterPauseError as exc:
        rejected = exc.to_payload()
        rejected["exit_code"] = 2
        return _print_payload(rejected)
    notify_router_wakeup(router_wakeup_socket_path(store.db_path))
    payload["message"] = "router pause requested; runtime will stop after draining the current wake cycle"
    payload["exit_code"] = 0
    return _print_payload(payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Router control API entrypoint for operator-side router commands."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser(
        "status",
        help="Print the durable router status snapshot as stable JSON.",
    )
    status.add_argument("--router-db")
    status.set_defaults(handler=_handle_status)

    start = subparsers.add_parser(
        "start",
        help="Start one long-lived router runtime from kernel bootstrap inputs.",
    )
    start.add_argument("--router-db")
    start.add_argument("--kernel-session-id")
    start.add_argument("--kernel-rollout-path")
    start.add_argument("--kernel-started-at")
    start.add_argument("--final-effects-file")
    start.set_defaults(handler=_handle_start)

    resume = subparsers.add_parser(
        "resume",
        help="Resume a paused/terminal_failed run, or append final effects while resuming paused/terminal_failed/completed runs.",
    )
    resume.add_argument("--router-db")
    resume.add_argument("--append-final-effects-ref")
    resume.set_defaults(handler=_handle_resume)

    pause = subparsers.add_parser(
        "pause",
        help="Pause an active router run at its current durable frontier.",
    )
    pause.add_argument("--router-db")
    pause.add_argument("--reason")
    pause.set_defaults(handler=_handle_pause)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handler = getattr(args, "handler", None)
    if handler is None:
        raise SystemExit("router API command handler is not implemented")
    return int(handler(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
