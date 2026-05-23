"""Paper trading CLI commands."""

from __future__ import annotations

import asyncio

import typer

from astock_trading.platform.cli.common import json_or_text


paper_app = typer.Typer(name="paper", help="模拟盘")


@paper_app.command("trial-plan")
def paper_trial_plan(
    limit: int = typer.Option(10, "--limit", min=1, max=20, help="返回候选数量"),
    record: bool = typer.Option(False, "--record", help="写入 paper.trial.recorded 影子试运行事件；不下单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """生成模拟盘影子试运行计划；只读，不下单。"""
    from astock_trading.pipeline.context import build_context
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.paper_trial import build_paper_trial_plan

    ctx = build_context()
    try:
        payload = build_paper_trial_plan(
            ctx.conn,
            event_store=EventStore(ctx.conn),
            limit=limit,
            record=record,
        )
    finally:
        ctx.conn.close()
    json_or_text(payload, as_json)


@paper_app.command("trial-review")
def paper_trial_review(
    trial_date: str = typer.Option("", "--trial-date", help="只复盘某个试运行日期 YYYY-MM-DD"),
    as_of: str = typer.Option("", "--as-of", help="复盘日期 YYYY-MM-DD，默认今天"),
    min_age_days: int = typer.Option(1, "--min-age-days", min=0, help="最少观察天数"),
    record: bool = typer.Option(False, "--record", help="写入 paper.trial.reviewed 影子复盘事件；不下单"),
    limit: int = typer.Option(100, "--limit", min=1, max=1000, help="最多扫描试运行事件数"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """复盘模拟盘影子试运行候选；只读，不下单。"""
    from astock_trading.pipeline.context import build_context
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.paper_trial import build_paper_trial_review

    ctx = build_context()
    try:
        event_store = EventStore(ctx.conn)
        payload = build_paper_trial_review(
            ctx.conn,
            event_store,
            trial_date=trial_date,
            as_of=as_of,
            min_age_days=min_age_days,
            record=record,
            limit=limit,
        )
    finally:
        ctx.conn.close()
    json_or_text(payload, as_json)


@paper_app.command("auto-readiness")
def paper_auto_readiness(
    skip_account: bool = typer.Option(False, "--skip-account", help="不请求 MX 模拟盘账户，只检查本地配置和事件证据"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查 auto_trade 是否会提交 MX 模拟盘委托；只读，不下单。"""
    from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
    from astock_trading.pipeline.context import build_context

    ctx = build_context()
    try:
        payload = build_auto_trade_readiness(ctx, include_account=not skip_account)
    finally:
        ctx.conn.close()
    json_or_text(payload, as_json)


@paper_app.command("status")
def paper_status(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询模拟盘持仓和资金。"""
    from astock_trading.pipeline.paper_account import PaperAccount

    paper = PaperAccount()
    positions = paper.get_positions()
    balance = paper.get_balance()
    payload = {
        "positions": [p.__dict__ for p in positions],
        "balance": balance.__dict__,
    }
    json_or_text(payload, as_json)


@paper_app.command("orders")
def paper_orders(
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """查询模拟盘委托。"""
    from astock_trading.pipeline.paper_account import _mx_call

    result = asyncio.run(_mx_call(lambda c: c.mock_orders()))
    json_or_text(result, as_json)


@paper_app.command("buy")
def paper_buy(
    code: str = typer.Argument(..., help="股票代码"),
    shares: int = typer.Argument(..., help="股数，必须为 100 的整数倍"),
    price: float = typer.Option(0, "--price", help="限价；0 表示市价"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认下单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘买入。"""
    from astock_trading.pipeline.paper_account import PaperAccount

    if not yes:
        raise typer.BadParameter("paper buy requires --yes")
    result = PaperAccount().buy(code, shares, price)
    json_or_text(result.__dict__, as_json)


@paper_app.command("sell")
def paper_sell(
    code: str = typer.Argument(..., help="股票代码"),
    shares: int = typer.Argument(..., help="股数，必须为 100 的整数倍"),
    price: float = typer.Option(0, "--price", help="限价；0 表示市价"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认下单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘卖出。"""
    from astock_trading.pipeline.paper_account import PaperAccount

    if not yes:
        raise typer.BadParameter("paper sell requires --yes")
    result = PaperAccount().sell(code, shares, price)
    json_or_text(result.__dict__, as_json)


@paper_app.command("cancel")
def paper_cancel(
    order_id: str = typer.Option("", "--order-id", help="委托 ID；空则撤全部"),
    yes: bool = typer.Option(False, "--yes", "-y", help="确认撤单"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """模拟盘撤单。"""
    from astock_trading.pipeline.paper_account import _mx_call

    if not yes:
        raise typer.BadParameter("paper cancel requires --yes")
    cancel_all = not order_id.strip()
    result = asyncio.run(_mx_call(lambda c: c.mock_cancel(order_id or None, cancel_all)))
    json_or_text(result, as_json)
