"""Event log CLI commands."""

from __future__ import annotations

from typing import Optional

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.evidence import backfill_legacy_evidence
from astock_trading.platform.events import EventStore


events_app = typer.Typer(name="events", help="事件查询")


def _query_evidence_events(conn, code: str, limit: int = 100) -> list[dict]:
    """按股票代码拉取一条可复盘证据链。"""
    rows = conn.execute(
        """SELECT * FROM event_log
           WHERE stream IN (?, ?, ?, ?)
              OR stream LIKE ?
              OR stream LIKE ?
              OR stream LIKE ?
           ORDER BY occurred_at, stream_version
           LIMIT ?""",
        (
            f"strategy:{code}",
            f"manual_trade:{code}",
            f"position:{code}",
            f"evidence:{code}",
            f"order:{code}:%",
            f"trade:{code}:%",
            f"paper:{code}",
            limit,
        ),
    ).fetchall()
    return [EventStore._row_to_dict(row) for row in rows]


@events_app.command("query")
def events_query(
    event_type: Optional[str] = typer.Option(None, "--type", help="事件类型"),
    stream: Optional[str] = typer.Option(None, help="stream 标识"),
    since: Optional[str] = typer.Option(None, help="起始时间 (ISO)"),
    limit: int = typer.Option(50, help="最大条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询事件"""
    conn = connect()
    try:
        store = EventStore(conn)
        events = store.query(stream=stream, event_type=event_type, since=since, limit=limit)
        if as_json:
            json_or_text(events, True)
        else:
            for e in events:
                typer.echo(
                    f"  [{e['occurred_at']}] {e['event_type']}  "
                    f"stream={e['stream']}  v{e['stream_version']}"
                )
    finally:
        conn.close()


@events_app.command("evidence")
def events_evidence(
    code: str = typer.Argument(..., help="股票代码，如 002138"),
    limit: int = typer.Option(100, help="最大条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """按股票代码查询评分、决策、人工确认、订单、持仓和复盘证据链。"""
    conn = connect()
    try:
        events = _query_evidence_events(conn, code, limit=limit)
        if as_json:
            json_or_text(events, True)
            return
        if not events:
            typer.echo(f"未找到 {code} 的证据事件")
            return
        for e in events:
            typer.echo(
                f"  [{e['occurred_at']}] {e['event_type']}  "
                f"stream={e['stream']}  v{e['stream_version']}"
            )
    finally:
        conn.close()


@events_app.command("backfill-evidence")
def events_backfill_evidence(
    code: str = typer.Option("", "--code", help="只回填某只股票代码"),
    apply: bool = typer.Option(False, "--apply", help="实际写入回填事件；不传则只预览"),
    limit: int = typer.Option(5000, help="最多扫描旧事件数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """为历史旧事件追加证据回填事件，不改写原始事件。"""
    conn = connect()
    try:
        payload = backfill_legacy_evidence(conn, code=code, apply=apply, limit=limit)
        json_or_text(payload, as_json)
    finally:
        conn.close()


@events_app.command("count")
def events_count(
    event_type: Optional[str] = typer.Option(None, "--type", help="事件类型"),
    since: Optional[str] = typer.Option(None, help="起始时间 (ISO)"),
):
    """统计事件数量"""
    conn = connect()
    try:
        store = EventStore(conn)
        n = store.count(event_type=event_type, since=since)
        typer.echo(f"Events: {n}")
    finally:
        conn.close()
