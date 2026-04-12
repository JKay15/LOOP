"""Minimal SQLite persistence for router v0."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loop.events import (
    ActorRef,
    ApproveSplit,
    ActorKind,
    OutputWindow,
    ProcessExitedObserved,
    RejectSplit,
    RequestSplit,
    RouterInboxItem,
    TakeoverResolved,
)
from loop.node_table import (
    ComponentRuntimeState,
    ComponentStatus,
    NodeRuntimeRecord,
    component_key_for_actor,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _isoformat(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class StoredRouterInboxItem:
    """One durable router inbox row loaded back from SQLite."""

    seq: int
    event_type: str
    node_id: str
    actor_kind: ActorKind
    attempt_count: int
    recorded_at: datetime
    payload_json: str


@dataclass(frozen=True)
class PendingSplitReview:
    """One durable split request that still awaits kernel review dispatch."""

    node_id: str
    request_seq: int


class RouterStore:
    """Thin SQLite adapter for router events, node table, and cursor."""

    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)
        self.initialize()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def initialize(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_type TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    actor_kind TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL,
                    recorded_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS node_table (
                    node_id TEXT PRIMARY KEY,
                    parent_node_id TEXT NOT NULL,
                    child_node_ids_json TEXT NOT NULL,
                    workspace_root TEXT NOT NULL,
                    final_effects_file TEXT NOT NULL,
                    split_request INTEGER NOT NULL,
                    split_approved INTEGER NOT NULL,
                    approved_split_request_seq INTEGER NOT NULL DEFAULT 0,
                    evaluator_phase TEXT NOT NULL,
                    checker_tasks_ref TEXT NOT NULL,
                    task_result_refs_json TEXT NOT NULL,
                    reviewer_verdict_kind TEXT NOT NULL,
                    reviewer_report_ref TEXT NOT NULL,
                    pending_prelude_lines_json TEXT NOT NULL,
                    durable_commit TEXT NOT NULL,
                    result_commit TEXT NOT NULL DEFAULT '',
                    escalated_to_kernel INTEGER NOT NULL,
                    last_rejected_split_diff_fingerprint TEXT NOT NULL,
                    components_json TEXT NOT NULL,
                    current_components_json TEXT NOT NULL DEFAULT '[]'
                )
                """
            )
            self._ensure_node_table_column(
                conn,
                "current_components_json",
                "TEXT NOT NULL DEFAULT '[]'",
            )
            self._ensure_node_table_column(
                conn,
                "task_result_refs_json",
                "TEXT NOT NULL DEFAULT '{}'",
            )
            self._ensure_node_table_column(
                conn,
                "pending_prelude_lines_json",
                "TEXT NOT NULL DEFAULT '[]'",
            )
            self._ensure_node_table_column(
                conn,
                "approved_split_request_seq",
                "INTEGER NOT NULL DEFAULT 0",
            )
            self._ensure_node_table_column(
                conn,
                "result_commit",
                "TEXT NOT NULL DEFAULT ''",
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS router_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    last_applied_seq INTEGER NOT NULL,
                    kernel_session_id TEXT NOT NULL,
                    kernel_rollout_path TEXT NOT NULL,
                    kernel_started_at TEXT NOT NULL,
                    router_status TEXT NOT NULL DEFAULT '',
                    router_paused_reason_json TEXT NOT NULL DEFAULT '',
                    router_paused_at TEXT NOT NULL DEFAULT '',
                    router_terminal_reason_json TEXT NOT NULL DEFAULT '',
                    router_terminal_at TEXT NOT NULL DEFAULT '',
                    router_completed_result_commit TEXT NOT NULL DEFAULT '',
                    router_completed_report_ref TEXT NOT NULL DEFAULT '',
                    router_completed_at TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self._ensure_router_meta_column(
                conn,
                "router_status",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_paused_reason_json",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_paused_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_terminal_reason_json",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_terminal_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_completed_result_commit",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_completed_report_ref",
                "TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_router_meta_column(
                conn,
                "router_completed_at",
                "TEXT NOT NULL DEFAULT ''",
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO router_meta (
                    singleton,
                    last_applied_seq,
                    kernel_session_id,
                    kernel_rollout_path,
                    kernel_started_at,
                    router_status,
                    router_paused_reason_json,
                    router_paused_at,
                    router_terminal_reason_json,
                    router_terminal_at,
                    router_completed_result_commit,
                    router_completed_report_ref,
                    router_completed_at
                )
                VALUES (1, 0, '', '', '', '', '', '', '', '', '', '', '')
                """
            )
    def append_event(
        self,
        event: RouterInboxItem,
        *,
        recorded_at: datetime | None = None,
    ) -> int:
        with self._connect() as conn:
            return self._append_event_conn(
                conn,
                event,
                recorded_at=recorded_at,
            )

    def reserve_split_request_and_append_event(
        self,
        event: RequestSplit,
        *,
        recorded_at: datetime | None = None,
    ) -> int | None:
        actor = event.actor
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT split_request, split_approved, durable_commit, components_json, current_components_json
                FROM node_table
                WHERE node_id = ?
                """,
                (str(actor.node_id),),
            ).fetchone()
            if row is None:
                return None
            current_components = _deserialize_current_components(
                str(row["current_components_json"]),
                node_id=str(actor.node_id),
            )
            if actor not in current_components:
                return None
            if int(row["split_request"]) != 0 or int(row["split_approved"]) != 0:
                return None
            if str(row["durable_commit"]) != str(event.durable_commit):
                return None
            components = _deserialize_components(str(row["components_json"]))
            component = components.get(component_key_for_actor(actor))
            if (
                component is None
                or component.status is not ComponentStatus.RUNNING
                or int(component.attempt_count) != int(actor.attempt_count)
            ):
                return None
            conn.execute(
                """
                UPDATE node_table
                SET split_request = 1,
                    approved_split_request_seq = 0
                WHERE node_id = ?
                """,
                (str(actor.node_id),),
            )
            return self._append_event_conn(
                conn,
                event,
                recorded_at=recorded_at,
            )

    def list_events_after(
        self,
        seq: int,
        *,
        limit: int | None = None,
    ) -> list[StoredRouterInboxItem]:
        query = """
            SELECT seq, event_type, node_id, actor_kind, attempt_count, recorded_at, payload_json
            FROM events
            WHERE seq > ?
            ORDER BY seq ASC
        """
        params: list[Any] = [int(seq)]
        if limit is not None:
            query += " LIMIT ?"
            params.append(int(limit))
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            StoredRouterInboxItem(
                seq=int(row["seq"]),
                event_type=str(row["event_type"]),
                node_id=str(row["node_id"]),
                actor_kind=ActorKind(str(row["actor_kind"])),
                attempt_count=int(row["attempt_count"]),
                recorded_at=_parse_datetime(str(row["recorded_at"])),
                payload_json=str(row["payload_json"]),
            )
            for row in rows
        ]

    def load_event(self, seq: int) -> StoredRouterInboxItem | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT seq, event_type, node_id, actor_kind, attempt_count, recorded_at, payload_json
                FROM events
                WHERE seq = ?
                """,
                (int(seq),),
            ).fetchone()
        if row is None:
            return None
        return StoredRouterInboxItem(
            seq=int(row["seq"]),
            event_type=str(row["event_type"]),
            node_id=str(row["node_id"]),
            actor_kind=ActorKind(str(row["actor_kind"])),
            attempt_count=int(row["attempt_count"]),
            recorded_at=_parse_datetime(str(row["recorded_at"])),
            payload_json=str(row["payload_json"]),
        )

    def latest_request_split_seq_for_node(self, node_id: str) -> int | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(seq) AS latest_seq
                FROM events
                WHERE event_type = 'RequestSplit' AND node_id = ?
                """,
                (str(node_id),),
            ).fetchone()
        if row is None or row["latest_seq"] is None:
            return None
        return int(row["latest_seq"])

    def list_pending_split_reviews(self) -> list[PendingSplitReview]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT n.node_id AS node_id, MAX(e.seq) AS request_seq
                FROM node_table AS n
                JOIN events AS e
                  ON e.node_id = n.node_id
                 AND e.event_type = 'RequestSplit'
                WHERE n.split_request = 1
                GROUP BY n.node_id
                ORDER BY request_seq ASC
                """
            ).fetchall()
        return [
            PendingSplitReview(
                node_id=str(row["node_id"]),
                request_seq=int(row["request_seq"]),
            )
            for row in rows
        ]

    def upsert_node(self, record: NodeRuntimeRecord) -> None:
        with self._connect() as conn:
            self._upsert_node_conn(conn, record)

    def upsert_nodes(self, records: list[NodeRuntimeRecord]) -> None:
        if not records:
            return
        with self._connect() as conn:
            for record in records:
                self._upsert_node_conn(conn, record)

    def materialize_pause_barrier(self) -> list[ActorRef]:
        cleared: list[ActorRef] = []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT node_id, components_json, current_components_json
                FROM node_table
                """
            ).fetchall()
            for row in rows:
                node_id = str(row["node_id"])
                current_components = _deserialize_current_components(
                    str(row["current_components_json"]),
                    node_id=node_id,
                )
                if not current_components:
                    continue
                components = _deserialize_components(str(row["components_json"]))
                updated_components = dict(components)
                for actor in current_components:
                    cleared.append(actor)
                    component_state_key = component_key_for_actor(actor)
                    component = updated_components.get(component_state_key)
                    if component is None or component.status is not ComponentStatus.RUNNING:
                        continue
                    updated_components[component_state_key] = replace(
                        component,
                        status=ComponentStatus.INACTIVE,
                    )
                conn.execute(
                    """
                    UPDATE node_table
                    SET components_json = ?,
                        current_components_json = '[]'
                    WHERE node_id = ?
                    """,
                    (
                        _canonical_json(_serialize_components(updated_components)),
                        node_id,
                    ),
                )
        return cleared

    def load_node(self, node_id: str) -> NodeRuntimeRecord | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT node_id, parent_node_id, child_node_ids_json, workspace_root, final_effects_file, split_request, split_approved,
                       approved_split_request_seq,
                       evaluator_phase, checker_tasks_ref, task_result_refs_json, reviewer_verdict_kind, reviewer_report_ref,
                       pending_prelude_lines_json,
                       durable_commit, result_commit, escalated_to_kernel,
                       last_rejected_split_diff_fingerprint, components_json, current_components_json
                FROM node_table
                WHERE node_id = ?
                """,
                (node_id,),
            ).fetchone()
        if row is None:
            return None
        components = _deserialize_components(str(row["components_json"]))
        current_components = _deserialize_current_components(
            str(row["current_components_json"]),
            node_id=str(row["node_id"]),
        )
        return NodeRuntimeRecord(
            node_id=str(row["node_id"]),
            parent_node_id=str(row["parent_node_id"]),
            child_node_ids=[str(item) for item in json.loads(str(row["child_node_ids_json"]))],
            workspace_root=str(row["workspace_root"]),
            final_effects_file=str(row["final_effects_file"]),
            split_request=int(row["split_request"]),
            split_approved=int(row["split_approved"]),
            approved_split_request_seq=int(row["approved_split_request_seq"]),
            evaluator_phase=str(row["evaluator_phase"]),
            checker_tasks_ref=str(row["checker_tasks_ref"]),
            task_result_refs={
                str(task_id): {
                    str(actor_kind): str(result_ref)
                    for actor_kind, result_ref in dict(task_payload).items()
                }
                for task_id, task_payload in json.loads(str(row["task_result_refs_json"])).items()
            },
            reviewer_verdict_kind=str(row["reviewer_verdict_kind"]),
            reviewer_report_ref=str(row["reviewer_report_ref"]),
            pending_prelude_lines=[
                str(item) for item in json.loads(str(row["pending_prelude_lines_json"]))
            ],
            current_components=current_components,
            durable_commit=str(row["durable_commit"]),
            result_commit=str(row["result_commit"]),
            escalated_to_kernel=bool(int(row["escalated_to_kernel"])),
            last_rejected_split_diff_fingerprint=str(row["last_rejected_split_diff_fingerprint"]),
            components=components,
        )

    def list_nodes(self) -> list[NodeRuntimeRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT node_id, parent_node_id, child_node_ids_json, workspace_root, final_effects_file, split_request, split_approved,
                       approved_split_request_seq,
                       evaluator_phase, checker_tasks_ref, task_result_refs_json, reviewer_verdict_kind, reviewer_report_ref,
                       pending_prelude_lines_json,
                       durable_commit, result_commit, escalated_to_kernel,
                       last_rejected_split_diff_fingerprint, components_json, current_components_json
                FROM node_table
                ORDER BY CAST(node_id AS INTEGER), node_id
                """
            ).fetchall()
        records: list[NodeRuntimeRecord] = []
        for row in rows:
            components = _deserialize_components(str(row["components_json"]))
            current_components = _deserialize_current_components(
                str(row["current_components_json"]),
                node_id=str(row["node_id"]),
            )
            records.append(
                NodeRuntimeRecord(
                    node_id=str(row["node_id"]),
                    parent_node_id=str(row["parent_node_id"]),
                    child_node_ids=[str(item) for item in json.loads(str(row["child_node_ids_json"]))],
                    workspace_root=str(row["workspace_root"]),
                    final_effects_file=str(row["final_effects_file"]),
                    split_request=int(row["split_request"]),
                    split_approved=int(row["split_approved"]),
                    approved_split_request_seq=int(row["approved_split_request_seq"]),
                    evaluator_phase=str(row["evaluator_phase"]),
                    checker_tasks_ref=str(row["checker_tasks_ref"]),
                    task_result_refs={
                        str(task_id): {
                            str(actor_kind): str(result_ref)
                            for actor_kind, result_ref in dict(task_payload).items()
                        }
                        for task_id, task_payload in json.loads(str(row["task_result_refs_json"])).items()
                    },
                    reviewer_verdict_kind=str(row["reviewer_verdict_kind"]),
                    reviewer_report_ref=str(row["reviewer_report_ref"]),
                    pending_prelude_lines=[
                        str(item) for item in json.loads(str(row["pending_prelude_lines_json"]))
                    ],
                    current_components=current_components,
                    durable_commit=str(row["durable_commit"]),
                    result_commit=str(row["result_commit"]),
                    escalated_to_kernel=bool(int(row["escalated_to_kernel"])),
                    last_rejected_split_diff_fingerprint=str(row["last_rejected_split_diff_fingerprint"]),
                    components=components,
                )
            )
        return records

    def delete_node(self, node_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM node_table
                WHERE node_id = ?
                """,
                (str(node_id),),
            )

    def allocate_next_node_ids(self, count: int) -> list[str]:
        normalized = int(count)
        if normalized <= 0:
            return []
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT COALESCE(MAX(CAST(node_id AS INTEGER)), 0) AS max_node_id
                FROM node_table
                WHERE node_id GLOB '[0-9]*'
                """
            ).fetchone()
        start = int(row["max_node_id"] or 0) + 1 if row is not None else 1
        return [str(node_id) for node_id in range(start, start + normalized)]

    def read_last_applied_seq(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_applied_seq FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return 0
        return int(row["last_applied_seq"])

    def write_last_applied_seq(self, seq: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET last_applied_seq = ?
                WHERE singleton = 1
                """,
                (int(seq),),
            )

    def read_kernel_session_id(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT kernel_session_id FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["kernel_session_id"])

    def write_kernel_session_id(self, kernel_session_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET kernel_session_id = ?
                WHERE singleton = 1
                """,
                (str(kernel_session_id),),
            )

    def read_kernel_rollout_path(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT kernel_rollout_path FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["kernel_rollout_path"])

    def read_kernel_started_at(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT kernel_started_at FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["kernel_started_at"])

    def write_kernel_bootstrap_info(
        self,
        *,
        kernel_session_id: str,
        kernel_rollout_path: str,
        kernel_started_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET kernel_session_id = ?,
                    kernel_rollout_path = ?,
                    kernel_started_at = ?
                WHERE singleton = 1
                """,
                (
                    str(kernel_session_id),
                    str(kernel_rollout_path),
                    str(kernel_started_at),
                ),
            )

    def read_router_status(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_status FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_status"])

    def read_router_terminal_reason_json(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_terminal_reason_json FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_terminal_reason_json"])

    def read_router_terminal_at(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_terminal_at FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_terminal_at"])

    def read_router_paused_reason_json(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_paused_reason_json FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_paused_reason_json"])

    def read_router_paused_at(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_paused_at FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_paused_at"])

    def read_router_completed_result_commit(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_completed_result_commit FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_completed_result_commit"])

    def read_router_completed_report_ref(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_completed_report_ref FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_completed_report_ref"])

    def read_router_completed_at(self) -> str:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT router_completed_at FROM router_meta WHERE singleton = 1"
            ).fetchone()
        if row is None:
            return ""
        return str(row["router_completed_at"])

    def write_router_terminal_state(
        self,
        *,
        router_status: str,
        router_terminal_reason_json: str,
        router_terminal_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET router_status = ?,
                    router_paused_reason_json = '',
                    router_paused_at = '',
                    router_terminal_reason_json = ?,
                    router_terminal_at = ?
                WHERE singleton = 1
                """,
                (
                    str(router_status),
                    str(router_terminal_reason_json),
                    str(router_terminal_at),
                ),
            )

    def write_router_paused_state(
        self,
        *,
        router_status: str,
        router_paused_reason_json: str,
        router_paused_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET router_status = ?,
                    router_paused_reason_json = ?,
                    router_paused_at = ?,
                    router_terminal_reason_json = '',
                    router_terminal_at = '',
                    router_completed_result_commit = '',
                    router_completed_report_ref = '',
                    router_completed_at = ''
                WHERE singleton = 1
                """,
                (
                    str(router_status),
                    str(router_paused_reason_json),
                    str(router_paused_at),
                ),
            )

    def write_router_completed_state(
        self,
        *,
        router_status: str,
        router_completed_result_commit: str,
        router_completed_report_ref: str,
        router_completed_at: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET router_status = ?,
                    router_paused_reason_json = '',
                    router_paused_at = '',
                    router_terminal_reason_json = '',
                    router_terminal_at = '',
                    router_completed_result_commit = ?,
                    router_completed_report_ref = ?,
                    router_completed_at = ?
                WHERE singleton = 1
                """,
                (
                    str(router_status),
                    str(router_completed_result_commit),
                    str(router_completed_report_ref),
                    str(router_completed_at),
                ),
            )

    def clear_router_pause_state(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET router_status = '',
                    router_paused_reason_json = '',
                    router_paused_at = ''
                WHERE singleton = 1
                """
            )

    def clear_router_outcome_state(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE router_meta
                SET router_status = '',
                    router_paused_reason_json = '',
                    router_paused_at = '',
                    router_terminal_reason_json = '',
                    router_terminal_at = '',
                    router_completed_result_commit = '',
                    router_completed_report_ref = '',
                    router_completed_at = ''
                WHERE singleton = 1
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_node_table_column(
        self,
        conn: sqlite3.Connection,
        column_name: str,
        column_sql: str,
    ) -> None:
        row = conn.execute("PRAGMA table_info(node_table)").fetchall()
        known = {str(item["name"]) for item in row}
        if str(column_name) in known:
            return
        conn.execute(f"ALTER TABLE node_table ADD COLUMN {column_name} {column_sql}")

    def _ensure_router_meta_column(
        self,
        conn: sqlite3.Connection,
        column_name: str,
        column_sql: str,
    ) -> None:
        row = conn.execute("PRAGMA table_info(router_meta)").fetchall()
        known = {str(item["name"]) for item in row}
        if str(column_name) in known:
            return
        conn.execute(f"ALTER TABLE router_meta ADD COLUMN {column_name} {column_sql}")

    def _upsert_node_conn(self, conn: sqlite3.Connection, record: NodeRuntimeRecord) -> None:
        conn.execute(
            """
            INSERT INTO node_table (
                node_id,
                parent_node_id,
                child_node_ids_json,
                workspace_root,
                final_effects_file,
                split_request,
                split_approved,
                approved_split_request_seq,
                evaluator_phase,
                checker_tasks_ref,
                task_result_refs_json,
                reviewer_verdict_kind,
                reviewer_report_ref,
                pending_prelude_lines_json,
                durable_commit,
                result_commit,
                escalated_to_kernel,
                last_rejected_split_diff_fingerprint,
                components_json,
                current_components_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                parent_node_id = excluded.parent_node_id,
                child_node_ids_json = excluded.child_node_ids_json,
                workspace_root = excluded.workspace_root,
                final_effects_file = excluded.final_effects_file,
                split_request = excluded.split_request,
                split_approved = excluded.split_approved,
                approved_split_request_seq = excluded.approved_split_request_seq,
                evaluator_phase = excluded.evaluator_phase,
                checker_tasks_ref = excluded.checker_tasks_ref,
                task_result_refs_json = excluded.task_result_refs_json,
                reviewer_verdict_kind = excluded.reviewer_verdict_kind,
                reviewer_report_ref = excluded.reviewer_report_ref,
                pending_prelude_lines_json = excluded.pending_prelude_lines_json,
                durable_commit = excluded.durable_commit,
                result_commit = excluded.result_commit,
                escalated_to_kernel = excluded.escalated_to_kernel,
                last_rejected_split_diff_fingerprint = excluded.last_rejected_split_diff_fingerprint,
                components_json = excluded.components_json,
                current_components_json = excluded.current_components_json
            """,
            (
                record.node_id,
                record.parent_node_id,
                _canonical_json(record.child_node_ids),
                record.workspace_root,
                record.final_effects_file,
                int(record.split_request),
                int(record.split_approved),
                int(record.approved_split_request_seq),
                str(record.evaluator_phase),
                str(record.checker_tasks_ref),
                _canonical_json(record.task_result_refs),
                str(record.reviewer_verdict_kind),
                str(record.reviewer_report_ref),
                _canonical_json(record.pending_prelude_lines),
                record.durable_commit,
                record.result_commit,
                int(record.escalated_to_kernel),
                record.last_rejected_split_diff_fingerprint,
                _canonical_json(_serialize_components(record.components)),
                _canonical_json(_serialize_current_components(record.current_components)),
            ),
        )

    def _append_event_conn(
        self,
        conn: sqlite3.Connection,
        event: RouterInboxItem,
        *,
        recorded_at: datetime | None = None,
    ) -> int:
        payload_json = _canonical_json(_event_payload(event))
        actor_ref = _inbox_actor_ref(event)
        cursor = conn.execute(
            """
            INSERT INTO events (
                event_type,
                node_id,
                actor_kind,
                attempt_count,
                recorded_at,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                type(event).__name__,
                _inbox_node_id(event),
                actor_ref.actor_kind.value,
                int(actor_ref.attempt_count),
                _isoformat(recorded_at or _utc_now()),
                payload_json,
            ),
        )
        return int(cursor.lastrowid)


def _event_payload(event: RouterInboxItem) -> dict[str, Any]:
    actor_task_id = str(event.actor.task_id or "").strip()
    if isinstance(event, OutputWindow):
        payload = {
            "had_output": bool(event.had_output),
            "observed_at": _isoformat(event.observed_at),
        }
        if actor_task_id:
            payload["task_id"] = actor_task_id
        return payload
    if isinstance(event, ProcessExitedObserved):
        payload = {
            "pid": int(event.pid),
            "process_birth_time": event.process_birth_time,
            "exit_code": event.exit_code,
            "signal_name": event.signal_name,
            "occurred_at": _isoformat(event.occurred_at),
        }
        if actor_task_id:
            payload["task_id"] = actor_task_id
        return payload
    if isinstance(event, RequestSplit):
        payload = {
            "split_bundle_ref": event.split_bundle_ref,
            "durable_commit": event.durable_commit,
            "diff_fingerprint": event.diff_fingerprint,
            "requested_at": _isoformat(event.requested_at),
        }
        if actor_task_id:
            payload["task_id"] = actor_task_id
        return payload
    if isinstance(event, ApproveSplit):
        payload = {
            "target_node_id": event.target_node_id,
            "request_seq": int(event.request_seq),
            "approved_at": _isoformat(event.approved_at),
        }
        if actor_task_id:
            payload["task_id"] = actor_task_id
        return payload
    if isinstance(event, RejectSplit):
        payload = {
            "target_node_id": event.target_node_id,
            "request_seq": int(event.request_seq),
            "rejected_at": _isoformat(event.rejected_at),
            "reason_ref": event.reason_ref,
        }
        if actor_task_id:
            payload["task_id"] = actor_task_id
        return payload
    if isinstance(event, TakeoverResolved):
        payload = {
            "target_node_id": event.target_node_id,
            "takeover_id": event.takeover_id,
            "resolution_ref": event.resolution_ref,
            "workspace_root": event.workspace_root,
            "final_effects_file": event.final_effects_file,
            "resolved_at": _isoformat(event.resolved_at),
        }
        if actor_task_id:
            payload["task_id"] = actor_task_id
        return payload
    raise TypeError(f"unsupported router inbox payload type: {type(event)!r}")


def _inbox_actor_ref(event: RouterInboxItem) -> ActorRef:
    return event.actor


def _inbox_node_id(event: RouterInboxItem) -> str:
    return _inbox_actor_ref(event).node_id


def _serialize_components(
    components: dict[str, ComponentRuntimeState]
) -> dict[str, Any]:
    return {
        component_key: {
            "status": component.status.value,
            "attempt_count": int(component.attempt_count),
            "task_id": str(component.task_id or ""),
            "pid": component.pid,
            "process_birth_time": component.process_birth_time,
            "session_ids": list(component.session_ids),
            "workspace_fingerprint_before": str(component.workspace_fingerprint_before),
            "saw_output_in_attempt": bool(component.saw_output_in_attempt),
            "consecutive_no_progress": int(component.consecutive_no_progress),
            "consecutive_failed_exits": int(component.consecutive_failed_exits),
        }
        for component_key, component in components.items()
    }


def _serialize_current_components(current_components: list[ActorRef]) -> list[dict[str, Any]]:
    return [
        {
            "node_id": str(actor.node_id),
            "actor_kind": actor.actor_kind.value,
            "attempt_count": int(actor.attempt_count),
            "task_id": str(actor.task_id or ""),
        }
        for actor in current_components
    ]


def _deserialize_components(payload_json: str) -> dict[str, ComponentRuntimeState]:
    payload = json.loads(payload_json)
    return {
        str(component_key): ComponentRuntimeState(
            status=ComponentStatus(str(component["status"])),
            attempt_count=int(component["attempt_count"]),
            task_id=str(component.get("task_id") or ""),
            pid=component["pid"],
            process_birth_time=component["process_birth_time"],
            session_ids=[str(item) for item in component.get("session_ids", [])],
            workspace_fingerprint_before=str(component.get("workspace_fingerprint_before") or ""),
            saw_output_in_attempt=bool(component["saw_output_in_attempt"]),
            consecutive_no_progress=int(component["consecutive_no_progress"]),
            consecutive_failed_exits=int(
                component.get(
                    "consecutive_failed_exits",
                    component.get("consecutive_same_failure", 0),
                )
            ),
        )
        for component_key, component in payload.items()
    }


def _deserialize_current_components(
    payload_json: str,
    *,
    node_id: str,
) -> list[ActorRef]:
    payload = json.loads(payload_json or "[]")
    return [
        ActorRef(
            node_id=str(item.get("node_id") or node_id),
            actor_kind=ActorKind(str(item["actor_kind"])),
            attempt_count=int(item["attempt_count"]),
            task_id=str(item.get("task_id") or "").strip() or None,
        )
        for item in payload
    ]
