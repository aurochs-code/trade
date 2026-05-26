"""交易后复盘 CLI。"""

from __future__ import annotations

import typer

from astock_trading.execution.reconciliation import TradeReconciliationService
from astock_trading.execution.review import TradeReviewService
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore


review_app = typer.Typer(name="review", help="交易后复盘")


@review_app.command("trades")
def review_trades(
    code: str = typer.Option("", "--code", help="只复盘某只股票代码"),
    as_of: str = typer.Option("", "--as-of", help="复盘日期 YYYY-MM-DD，默认今天"),
    record: bool = typer.Option(False, "--record", help="写入 trade.review.recorded；不传则只预览"),
    limit: int = typer.Option(500, help="最多扫描交易假设事件数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """交易后复盘：到期后计算 MFE/MAE 并验证交易前假设。"""
    init_db()
    conn = connect()
    try:
        payload = TradeReviewService(EventStore(conn), conn).review_due_trades(
            as_of=as_of or None,
            code=code,
            record=record,
            limit=limit,
        )
        json_or_text(payload, as_json)
    finally:
        conn.close()


@review_app.command("shadow")
def review_shadow(
    date: str = typer.Option("", "--date", help="对账日期 YYYY-MM-DD，默认今天"),
    record: bool = typer.Option(False, "--record", help="写入 rule_deviation.recorded；不传则只预览"),
    slippage_bps: int = typer.Option(50, "--slippage-bps", help="价格偏离阈值，单位 bps"),
    limit: int = typer.Option(1000, help="最多扫描事件数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘 vs 实盘逐笔对账。"""
    init_db()
    conn = connect()
    try:
        payload = TradeReconciliationService(EventStore(conn)).reconcile(
            date=date or None,
            record=record,
            slippage_bps=slippage_bps,
            limit=limit,
        )
        json_or_text(payload, as_json)
    finally:
        conn.close()


@review_app.command("manual-followup")
def review_manual_followup(
    skip_account: bool = typer.Option(False, "--skip-account", help="不请求 MX 模拟盘账户，只检查本地配置和事件证据"),
    limit: int = typer.Option(100, "--limit", min=1, max=1000, help="最多扫描影子试运行事件数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """汇总人工复核清单；只读，不写状态、不下单。"""
    from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
    from astock_trading.pipeline.context import build_context
    from astock_trading.platform.manual_followup import build_manual_followup_report

    ctx = build_context()
    try:
        auto_readiness = build_auto_trade_readiness(ctx, include_account=not skip_account)
        payload = build_manual_followup_report(
            ctx.conn,
            auto_readiness=auto_readiness,
            limit=limit,
        )
    finally:
        ctx.conn.close()
    json_or_text(payload, as_json)
