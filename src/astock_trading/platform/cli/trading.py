"""Portfolio and manual execution CLI commands."""

from __future__ import annotations

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore


def _position_dict(position):
    return position.to_dict() if position else None


def _manual_trade_payload(
    *,
    side: str,
    code: str,
    shares: int,
    price_cents: int,
    fee_cents: int,
    reason: str,
    order,
    audit: dict,
    position_before,
    position_after,
) -> dict:
    return {
        "status": "recorded",
        "side": side,
        "code": code,
        "shares": shares,
        "price_cents": price_cents,
        "fee_cents": fee_cents,
        "reason": reason,
        "order_id": order.order_id,
        "order": order.to_dict(),
        "audit": audit,
        "position_before": _position_dict(position_before),
        "position_after": _position_dict(position_after),
    }


def register_trading_commands(app: typer.Typer) -> None:
    @app.command("status")
    def portfolio_status(
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """持仓概览"""
        conn = connect()
        try:
            store = EventStore(conn)
            from astock_trading.execution.service import ExecutionService

            svc = ExecutionService(store, conn)
            portfolio = svc.get_portfolio()
            if as_json:
                json_or_text(portfolio, True)
                return
            positions = portfolio.get("positions", [])
            if not positions:
                typer.echo("当前无持仓")
                return
            typer.echo(f"持仓 {portfolio['holding_count']} 只:")
            for p in positions:
                cost = p["avg_cost_cents"] / 100
                typer.echo(f"  {p['code']} {p['name']}  {p['shares']}股  成本{cost:.2f}  风格={p['style']}")
        finally:
            conn.close()

    @app.command("record-sell")
    def record_sell(
        code: str = typer.Argument(..., help="股票代码，如 002261"),
        shares: int = typer.Argument(..., help="卖出股数（当前必须等于持仓数量）"),
        price: float = typer.Argument(..., help="成交价，如 34.52"),
        fee: float = typer.Option(0, "--fee", help="手续费（元），默认 0"),
        reason: str = typer.Option("manual", "--reason", help="卖出原因"),
        yes: bool = typer.Option(False, "--yes", "-y", help="确认执行（必填）"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """录入已在券商 App 成交的卖出记录（手动补录，不调 broker）。"""
        from astock_trading.execution.service import ExecutionService

        conn = connect()
        try:
            store = EventStore(conn)
            svc = ExecutionService(store, conn)
            price_cents = int(price * 100)
            fee_cents = int(fee * 100)

            pos = svc.get_position(code)
            if not pos:
                raise ValueError(f"未找到持仓：{code}")
            position_before = pos

            proceeds = price_cents * shares - fee_cents
            pnl = (price_cents - pos.avg_cost_cents) * shares
            pnl_pct = (price - pos.avg_cost) / pos.avg_cost * 100

            if not as_json:
                typer.echo("═" * 50)
                typer.echo("  卖出录入预览")
                typer.echo("─" * 50)
                typer.echo(f"  股票       {code}  {pos.name}")
                typer.echo(f"  持仓       {pos.shares} 股")
                typer.echo(f"  卖出       {shares} 股")
                typer.echo(f"  成交价     ¥{price}")
                typer.echo(f"  成交额     ¥{proceeds / 100:,.2f}")
                typer.echo(f"  手续费     ¥{fee_cents / 100:.2f}")
                typer.echo(f"  成本       ¥{pos.avg_cost:.2f}")
                typer.echo(f"  盈亏       ¥{pnl / 100:+,.2f}  ({pnl_pct:+.1f}%)")
                typer.echo("─" * 50)

            if shares != pos.shares:
                raise ValueError(
                    f"部分卖出暂不支持。当前持仓 {pos.shares} 股，传入了 {shares} 股。"
                    f"\n如需卖出，请传入 --shares {pos.shares}"
                )

            if not yes:
                if as_json:
                    json_or_text({"status": "confirmation_required", "side": "sell", "code": code}, True)
                else:
                    typer.echo("添加 --yes 确认执行")
                raise typer.Abort()

            order = svc.record_sell(
                code=code,
                shares=shares,
                price_cents=price_cents,
                fee_cents=fee_cents,
                reason=reason,
            )

            conn.commit()
            audit = svc.audit_manual_trade_consistency(order.order_id)
            payload = _manual_trade_payload(
                side="sell",
                code=code,
                shares=shares,
                price_cents=price_cents,
                fee_cents=fee_cents,
                reason=reason,
                order=order,
                audit=audit,
                position_before=position_before,
                position_after=svc.get_position(code),
            )
            if as_json:
                json_or_text(payload, True)
                return
            typer.echo(f"已录入卖出：{code} {shares}股 @{price}")
            typer.echo(f"   订单ID：{order.order_id}")
            if audit["ok"]:
                typer.echo("   一致性校验：通过")
            else:
                typer.echo(f"   一致性校验：异常 {','.join(audit['issues'])}")
        except Exception as e:
            typer.secho(f"{e}", fg="red")
            raise typer.Abort()
        finally:
            conn.close()

    @app.command("record-buy")
    def record_buy(
        code: str = typer.Argument(..., help="股票代码，如 002261"),
        shares: int = typer.Argument(..., help="买入股数"),
        price: float = typer.Argument(..., help="成交价，如 39.91"),
        fee: float = typer.Option(0, "--fee", help="手续费（元），默认 0"),
        reason: str = typer.Option("manual", "--reason", help="买入原因"),
        name: str = typer.Option("", "--name", help="股票名称（可选）"),
        style: str = typer.Option("growth", "--style", help="风格：growth / momentum / slow_bull"),
        yes: bool = typer.Option(False, "--yes", "-y", help="确认执行（必填）"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """录入已在券商 App 成交的买入记录（手动补录，不调 broker）。"""
        from astock_trading.execution.service import ExecutionService

        conn = connect()
        try:
            store = EventStore(conn)
            svc = ExecutionService(store, conn)
            price_cents = int(price * 100)
            fee_cents = int(fee * 100)
            total_cost = price_cents * shares + fee_cents
            position_before = svc.get_position(code)

            if not as_json:
                typer.echo("═" * 50)
                typer.echo("  买入录入预览")
                typer.echo("─" * 50)
                typer.echo(f"  股票       {code}")
                typer.echo(f"  名称       {name or '(未填)'}")
                typer.echo(f"  风格       {style}")
                typer.echo(f"  买入       {shares} 股")
                typer.echo(f"  成交价     ¥{price}")
                typer.echo(f"  成交额     ¥{price_cents * shares / 100:,.2f}")
                typer.echo(f"  手续费     ¥{fee_cents / 100:.2f}")
                typer.echo(f"  总成本     ¥{total_cost / 100:,.2f}")
                typer.echo("─" * 50)

            if not yes:
                if as_json:
                    json_or_text({"status": "confirmation_required", "side": "buy", "code": code}, True)
                else:
                    typer.echo("添加 --yes 确认执行")
                raise typer.Abort()

            order = svc.record_buy(
                code=code,
                shares=shares,
                price_cents=price_cents,
                fee_cents=fee_cents,
                reason=reason,
                name=name,
                style=style,
            )

            conn.commit()
            audit = svc.audit_manual_trade_consistency(order.order_id)
            payload = _manual_trade_payload(
                side="buy",
                code=code,
                shares=shares,
                price_cents=price_cents,
                fee_cents=fee_cents,
                reason=reason,
                order=order,
                audit=audit,
                position_before=position_before,
                position_after=svc.get_position(code),
            )
            if as_json:
                json_or_text(payload, True)
                return
            typer.echo(f"已录入买入：{code} {shares}股 @{price}")
            typer.echo(f"   订单ID：{order.order_id}")
            if audit["ok"]:
                typer.echo("   一致性校验：通过")
            else:
                typer.echo(f"   一致性校验：异常 {','.join(audit['issues'])}")
        except Exception as e:
            typer.secho(f"{e}", fg="red")
            raise typer.Abort()
        finally:
            conn.close()
