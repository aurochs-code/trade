"""Single-stock analysis CLI commands."""

from __future__ import annotations

import asyncio

import typer

from astock_trading.pipeline.context import build_context
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.stock_analysis import StockAnalysisError, analyze_stock


stock_app = typer.Typer(name="stock", help="个股分析")


@stock_app.command("analyze")
def stock_analyze(
    identifier: str = typer.Argument(..., help="股票代码或名称"),
    history_days: int = typer.Option(7, "--history-days", help="历史评分记录条数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
) -> None:
    """分析单只股票的评分、买入门控、候选池和历史记录，不执行交易。"""
    ctx = build_context()
    try:
        try:
            payload = asyncio.run(
                analyze_stock(identifier, ctx, history_days=history_days)
            )
        except StockAnalysisError as exc:
            payload = {
                "analysis": "stock",
                "status": "failed",
                "identifier": identifier,
                "error": str(exc),
                "execution_allowed": False,
            }
            json_or_text(payload, as_json)
            raise typer.Exit(1) from exc
        json_or_text(payload, as_json)
    finally:
        ctx.conn.close()
