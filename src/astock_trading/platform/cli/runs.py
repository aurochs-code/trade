"""Run journal CLI commands."""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.runs import RunJournal
from astock_trading.platform.time import utc_now, utc_now_iso


runs_app = typer.Typer(name="runs", help="运行记录")


def _stale_running_rows(conn, older_than_hours: int) -> list[dict]:
    cutoff = (utc_now() - timedelta(hours=older_than_hours)).isoformat()
    rows = conn.execute(
        "SELECT run_id, run_type, started_at FROM run_log "
        "WHERE status = 'running' AND started_at < ? "
        "ORDER BY started_at DESC",
        (cutoff,),
    ).fetchall()
    return [dict(r) for r in rows]


@runs_app.command("list")
def runs_list(
    run_type: Optional[str] = typer.Option(None, help="过滤 run_type"),
    status: Optional[str] = typer.Option(None, help="过滤 status"),
    limit: int = typer.Option(20, help="显示条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看运行记录"""
    conn = connect()
    try:
        journal = RunJournal(conn)
        runs = journal.list_runs(run_type=run_type, status=status, limit=limit)
        if as_json:
            json_or_text(runs, True)
        else:
            for r in runs:
                status_icon = {"completed": "OK", "failed": "FAIL", "running": "RUN"}.get(
                    r["status"], "?"
                )
                typer.echo(
                    f"  {status_icon} {r['run_id']}  type={r['run_type']}  "
                    f"status={r['status']}  started={r['started_at']}"
                )
    finally:
        conn.close()


@runs_app.command("failed")
def runs_failed(
    days: int = typer.Option(7, help="查看最近 N 天"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查看近期失败的运行"""
    conn = connect()
    try:
        journal = RunJournal(conn)
        failed = journal.get_failed_runs(days=days)
        if as_json:
            json_or_text(failed, True)
        elif not failed:
            typer.echo("无失败记录")
        else:
            for r in failed:
                typer.echo(
                    f"  FAIL {r['run_id']}  type={r['run_type']}  "
                    f"error={r.get('error_message', '')[:80]}"
                )
    finally:
        conn.close()


@runs_app.command("cleanup-stale")
def runs_cleanup_stale(
    older_than_hours: int = typer.Option(6, "--older-than-hours", help="清理超过 N 小时的 running run"),
    yes: bool = typer.Option(False, "--yes", help="确认写入：将 stale running 标记为 failed"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """清理历史卡住的 running 记录；默认 dry-run。"""
    conn = connect()
    try:
        stale = _stale_running_rows(conn, older_than_hours)
        if yes and stale:
            now = utc_now_iso()
            message = f"stale running cleaned up after {older_than_hours}h"
            conn.executemany(
                "UPDATE run_log SET status = 'failed', finished_at = ?, error_message = ? "
                "WHERE run_id = ? AND status = 'running'",
                [(now, message, row["run_id"]) for row in stale],
            )
        result = {
            "dry_run": not yes,
            "older_than_hours": older_than_hours,
            "count": len(stale),
            "runs": stale,
        }
        if as_json:
            json_or_text(result, True)
        else:
            action = "将清理" if not yes else "已清理"
            typer.echo(f"{action} {len(stale)} 条 stale running run")
            for row in stale[:20]:
                typer.echo(f"  {row['run_id']} {row['run_type']} {row['started_at']}")
            if stale and not yes:
                typer.echo("添加 --yes 确认写入")
    finally:
        conn.close()
