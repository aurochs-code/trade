"""Dashboard CLI."""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.dashboard import build_dashboard_snapshot
from astock_trading.platform.db import connect, init_db


dashboard_app = typer.Typer(name="dashboard", help="只读仪表盘数据")


@dashboard_app.command("snapshot")
def dashboard_snapshot(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """输出 Web / 手机仪表盘可消费的只读状态快照。"""
    init_db()
    conn = connect()
    try:
        payload = build_dashboard_snapshot(conn)
        json_or_text(payload, as_json)
    finally:
        conn.close()
