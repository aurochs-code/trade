"""
platform/events.py — append-only EventStore

业务事实只 INSERT 不 UPDATE/DELETE。
每个事件属于一个 stream，stream 内 version 自动递增。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _is_unique_conflict(exc: Exception) -> bool:
    text = str(exc).lower()
    return "unique" in text or "duplicate" in text


class EventRepository:
    """Append-only event log repository."""

    def __init__(self, conn: Any):
        self._conn = conn

    def append(
        self,
        stream: str,
        stream_type: str,
        event_type: str,
        payload: dict,
        metadata: Optional[dict] = None,
    ) -> str:
        """
        追加一条事件。自动递增 stream_version。
        通过 event_streams 分配每个 stream 的 next_version，并在同一事务中写入事件。
        MySQL 通过 event_streams 行锁保持一致性。

        Returns:
            event_id
        """
        metadata = metadata or {}

        for attempt in range(5):
            event_id = _new_id()
            self._conn.execute("BEGIN")
            try:
                next_version = self._next_stream_version(stream, stream_type)

                self._conn.execute(
                    """INSERT INTO event_log
                       (event_id, stream, stream_type, stream_version,
                        event_type, payload_json, metadata_json, occurred_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        event_id,
                        stream,
                        stream_type,
                        next_version,
                        event_type,
                        json.dumps(payload, ensure_ascii=False, default=str),
                        json.dumps(metadata, ensure_ascii=False, default=str),
                        _now_iso(),
                    ),
                )
                self._conn.execute(
                    "UPDATE event_streams SET next_version = ?, updated_at = ? WHERE stream = ?",
                    (next_version + 1, _now_iso(), stream),
                )
                self._conn.execute("COMMIT")
                break
            except Exception as exc:
                self._conn.execute("ROLLBACK")
                if attempt < 4 and _is_unique_conflict(exc):
                    self._repair_stream_version(stream)
                    continue
                raise
        return event_id

    def _next_stream_version(self, stream: str, stream_type: str) -> int:
        row = self._select_stream_row(stream, for_update=True)
        if not row:
            next_version = self._legacy_next_stream_version(stream)
            self._conn.execute(
                """INSERT IGNORE INTO event_streams
                   (stream, stream_type, next_version, updated_at)
                   VALUES (?, ?, ?, ?)""",
                (stream, stream_type, next_version, _now_iso()),
            )
            row = self._select_stream_row(stream, for_update=True)
        if not row:
            raise RuntimeError(f"Failed to allocate event stream version for {stream}")
        return int(row["next_version"])

    def _select_stream_row(self, stream: str, *, for_update: bool = False):
        sql = "SELECT stream, stream_type, next_version FROM event_streams WHERE stream = ?"
        if for_update and str(getattr(self._conn, "dialect", "")).startswith("mysql"):
            sql += " FOR UPDATE"
        return self._conn.execute(sql, (stream,)).fetchone()

    def _legacy_next_stream_version(self, stream: str) -> int:
        row = self._conn.execute(
            "SELECT MAX(stream_version) FROM event_log WHERE stream = ?",
            (stream,),
        ).fetchone()
        return (row[0] or 0) + 1 if row else 1

    def _repair_stream_version(self, stream: str) -> None:
        next_version = self._legacy_next_stream_version(stream)
        self._conn.execute("BEGIN")
        try:
            self._conn.execute(
                "UPDATE event_streams SET next_version = ?, updated_at = ? WHERE stream = ?",
                (next_version, _now_iso(), stream),
            )
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def query(
        self,
        stream: Optional[str] = None,
        stream_type: Optional[str] = None,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 1000,
        metadata_filter: Optional[dict] = None,
    ) -> list[dict]:
        """Query events with optional filters.

        Args:
            metadata_filter: 可选的 metadata 字段过滤，如 {"run_id": "xxx"}。
                使用 json_extract 在 SQL 层过滤，避免全量拉取后内存过滤。
        """
        clauses: list[str] = []
        params: list = []

        if stream:
            clauses.append("stream = ?")
            params.append(stream)
        if stream_type:
            clauses.append("stream_type = ?")
            params.append(stream_type)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if since:
            clauses.append("occurred_at >= ?")
            params.append(since)
        if until:
            clauses.append("occurred_at <= ?")
            params.append(until)
        if metadata_filter:
            for key, value in metadata_filter.items():
                clauses.append(f"json_extract(metadata_json, '$.{key}') = ?")
                params.append(value)

        where = " AND ".join(clauses) if clauses else "1=1"
        sql = f"""SELECT * FROM event_log
                  WHERE {where}
                  ORDER BY occurred_at, stream_version
                  LIMIT ?"""
        params.append(limit)

        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_stream(self, stream: str) -> list[dict]:
        """Get all events for a stream, ordered by version."""
        rows = self._conn.execute(
            "SELECT * FROM event_log WHERE stream = ? ORDER BY stream_version",
            (stream,),
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def count(
        self,
        event_type: Optional[str] = None,
        since: Optional[str] = None,
    ) -> int:
        """Count events matching filters."""
        clauses: list[str] = []
        params: list = []
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if since:
            clauses.append("occurred_at >= ?")
            params.append(since)
        where = " AND ".join(clauses) if clauses else "1=1"
        row = self._conn.execute(
            f"SELECT COUNT(*) FROM event_log WHERE {where}", params
        ).fetchone()
        return row[0]

    @staticmethod
    def _row_to_dict(row: Any) -> dict:
        d = dict(row)
        if "payload_json" in d:
            d["payload"] = _decode_json(d.pop("payload_json"))
        if "metadata_json" in d:
            d["metadata"] = _decode_json(d.pop("metadata_json"))
        return d


class EventStore(EventRepository):
    """Backward-compatible EventStore facade."""
