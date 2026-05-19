"""Hermes 轻量查询命令。"""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.hermes_commands import (
    build_digest,
    build_explanation,
    build_suggestion,
)


def register_hermes_commands(app: typer.Typer) -> None:
    @app.command("digest")
    def digest(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """一句话总结今日交易系统状态。"""
        init_db()
        conn = connect()
        try:
            json_or_text(build_digest(conn), as_json)
        finally:
            conn.close()

    @app.command("suggest")
    def suggest(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """基于当前状态输出下一步建议，不执行交易。"""
        init_db()
        conn = connect()
        try:
            json_or_text(build_suggestion(conn), as_json)
        finally:
            conn.close()

    @app.command("explain")
    def explain(
        code: str = typer.Argument(..., help="股票代码"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """解释某只股票最近评分和决策逻辑。"""
        init_db()
        conn = connect()
        try:
            json_or_text(build_explanation(conn, code), as_json)
        finally:
            conn.close()
