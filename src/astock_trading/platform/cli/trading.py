"""Portfolio and manual execution CLI commands."""

from __future__ import annotations

import typer

from astock_trading.execution.positions import allocate_cost_basis_cents
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect
from astock_trading.platform.events import EventStore


def _position_dict(position):
    return position.to_dict() if position else None


def _money_to_cents(amount: float) -> int:
    return int(round(amount * 100))


def _cost_basis_from_inputs(
    *,
    shares: int,
    price_cents: int,
    fee_cents: int = 0,
    cost_price: float = 0,
    cost_basis: float = 0,
) -> int:
    if cost_basis > 0:
        return _money_to_cents(cost_basis)
    if cost_price > 0:
        return int(round(cost_price * shares * 100))
    return price_cents * shares + fee_cents


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
    evidence: dict | None = None,
    realized_pnl_cents: int | None = None,
    sold_cost_basis_cents: int | None = None,
) -> dict:
    payload = {
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
        "evidence": evidence or {},
    }
    if realized_pnl_cents is not None:
        payload["realized_pnl_cents"] = realized_pnl_cents
    if sold_cost_basis_cents is not None:
        payload["sold_cost_basis_cents"] = sold_cost_basis_cents
    return payload


def _build_hypothesis_payload(
    hypothesis: str = "",
    invalidation: str = "",
    manual_reason: str = "",
    review_after_days: int = 0,
) -> dict:
    payload = {}
    if hypothesis:
        payload["thesis"] = hypothesis
    if invalidation:
        payload["invalidation"] = invalidation
    if manual_reason:
        payload["manual_reason"] = manual_reason
    if review_after_days > 0:
        payload["review_after_days"] = review_after_days
    return payload


def _trade_evidence_payload(store: EventStore, code: str, order_id: str) -> dict:
    events = store.query(stream=f"trade:{code}:{order_id}")
    event_ids = {event["event_type"]: event["event_id"] for event in events}
    return {
        "stream": f"trade:{code}:{order_id}",
        "event_count": len(events),
        "hypothesis_event_id": event_ids.get("trade.hypothesis.recorded", ""),
        "outcome_event_id": event_ids.get("trade.outcome.recorded", ""),
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
                cost = p.get("cost_price") or p["avg_cost_cents"] / 100
                typer.echo(f"  {p['code']} {p['name']}  {p['shares']}股  成本{cost:.3f}  风格={p['style']}")
        finally:
            conn.close()

    @app.command("record-sell")
    def record_sell(
        code: str = typer.Argument(..., help="股票代码，如 002261"),
        shares: int = typer.Argument(..., help="卖出股数（支持部分卖出）"),
        price: float = typer.Argument(..., help="成交价，如 34.52"),
        fee: float = typer.Option(0, "--fee", help="手续费（元），默认 0"),
        reason: str = typer.Option("manual", "--reason", help="卖出原因"),
        source_event_id: str = typer.Option("", "--source-event-id", help="来源决策/人工确认事件 ID"),
        source_score_event_id: str = typer.Option("", "--source-score-event-id", help="来源评分事件 ID"),
        decision_id: str = typer.Option("", "--decision-id", help="来源决策事件 ID；等同 --source-event-id"),
        signal_id: str = typer.Option("", "--signal-id", help="来源评分/信号事件 ID；等同 --source-score-event-id"),
        manual_reason: str = typer.Option("", "--manual-reason", help="人工确认原因；会写入交易假设证据"),
        hypothesis: str = typer.Option("", "--hypothesis", help="交易前假设或卖出假设"),
        invalidation: str = typer.Option("", "--invalidation", help="假设失效条件"),
        review_after_days: int = typer.Option(0, "--review-after-days", help="几天后复盘，0 表示不设置"),
        yes: bool = typer.Option(False, "--yes", "-y", help="确认执行（必填）"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """录入已在券商 App 成交的卖出记录（手动补录，不调 broker）。"""
        from astock_trading.execution.service import ExecutionService

        conn = connect()
        try:
            store = EventStore(conn)
            svc = ExecutionService(store, conn)
            price_cents = _money_to_cents(price)
            fee_cents = _money_to_cents(fee)

            pos = svc.get_position(code)
            if not pos:
                raise ValueError(f"未找到持仓：{code}")
            position_before = pos
            if shares <= 0:
                raise ValueError(f"shares 必须 > 0，当前为 {shares}")
            if shares > pos.shares:
                raise ValueError(f"卖出股数不能超过当前持仓：{shares} > {pos.shares}")

            proceeds = price_cents * shares - fee_cents
            sold_cost_basis_cents = allocate_cost_basis_cents(
                pos.effective_cost_basis_cents,
                pos.shares,
                shares,
            )
            pnl = proceeds - sold_cost_basis_cents
            pnl_pct = pnl / sold_cost_basis_cents * 100 if sold_cost_basis_cents else 0.0
            remaining_shares = pos.shares - shares
            remaining_cost_basis_cents = pos.effective_cost_basis_cents - sold_cost_basis_cents

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
                typer.echo(f"  成本       ¥{pos.avg_cost:.3f}")
                typer.echo(f"  盈亏       ¥{pnl / 100:+,.2f}  ({pnl_pct:+.1f}%)")
                if remaining_shares > 0:
                    typer.echo(
                        f"  剩余       {remaining_shares} 股，成本金额 ¥{remaining_cost_basis_cents / 100:,.2f}"
                    )
                typer.echo("─" * 50)

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
                source_event_id=source_event_id or decision_id,
                source_score_event_id=source_score_event_id or signal_id,
                hypothesis=_build_hypothesis_payload(
                    hypothesis=hypothesis,
                    invalidation=invalidation,
                    manual_reason=manual_reason,
                    review_after_days=review_after_days,
                ),
            )

            conn.commit()
            audit = svc.audit_manual_trade_consistency(order.order_id)
            evidence = _trade_evidence_payload(store, code, order.order_id)
            position_after = svc.get_position(code)
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
                position_after=position_after,
                evidence=evidence,
                realized_pnl_cents=pnl,
                sold_cost_basis_cents=sold_cost_basis_cents,
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
        cost_price: float = typer.Option(0, "--cost-price", help="券商显示的成本价（元/股），优先于 --fee 推导总成本"),
        cost_basis: float = typer.Option(0, "--cost-basis", help="券商显示的总成本（元），优先于 --cost-price"),
        reason: str = typer.Option("manual", "--reason", help="买入原因"),
        name: str = typer.Option("", "--name", help="股票名称（可选）"),
        style: str = typer.Option("growth", "--style", help="风格：growth / momentum / slow_bull"),
        source_event_id: str = typer.Option("", "--source-event-id", help="来源决策/人工确认事件 ID"),
        source_score_event_id: str = typer.Option("", "--source-score-event-id", help="来源评分事件 ID"),
        decision_id: str = typer.Option("", "--decision-id", help="来源决策事件 ID；等同 --source-event-id"),
        signal_id: str = typer.Option("", "--signal-id", help="来源评分/信号事件 ID；等同 --source-score-event-id"),
        manual_reason: str = typer.Option("", "--manual-reason", help="人工确认原因；会写入交易假设证据"),
        hypothesis: str = typer.Option("", "--hypothesis", help="交易前假设"),
        invalidation: str = typer.Option("", "--invalidation", help="假设失效条件"),
        review_after_days: int = typer.Option(0, "--review-after-days", help="几天后复盘，0 表示不设置"),
        yes: bool = typer.Option(False, "--yes", "-y", help="确认执行（必填）"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """录入已在券商 App 成交的买入记录（手动补录，不调 broker）。"""
        from astock_trading.execution.service import ExecutionService

        conn = connect()
        try:
            store = EventStore(conn)
            svc = ExecutionService(store, conn)
            price_cents = _money_to_cents(price)
            fee_cents = _money_to_cents(fee)
            total_cost = _cost_basis_from_inputs(
                shares=shares,
                price_cents=price_cents,
                fee_cents=fee_cents,
                cost_price=cost_price,
                cost_basis=cost_basis,
            )
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
                typer.echo(f"  成本价     ¥{total_cost / shares / 100:.3f}")
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
                cost_basis_cents=total_cost,
                reason=reason,
                name=name,
                style=style,
                source_event_id=source_event_id or decision_id,
                source_score_event_id=source_score_event_id or signal_id,
                hypothesis=_build_hypothesis_payload(
                    hypothesis=hypothesis,
                    invalidation=invalidation,
                    manual_reason=manual_reason,
                    review_after_days=review_after_days,
                ),
            )

            conn.commit()
            audit = svc.audit_manual_trade_consistency(order.order_id)
            evidence = _trade_evidence_payload(store, code, order.order_id)
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
                evidence=evidence,
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

    @app.command("adjust-position-cost")
    def adjust_position_cost(
        code: str = typer.Argument(..., help="股票代码，如 002156"),
        cost_price: float = typer.Option(0, "--cost-price", help="券商显示的成本价（元/股）"),
        cost_basis: float = typer.Option(0, "--cost-basis", help="券商显示的总成本（元）"),
        reason: str = typer.Option("manual_cost_adjustment", "--reason", help="调整原因"),
        yes: bool = typer.Option(False, "--yes", "-y", help="确认写入（必填）"),
        as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
    ):
        """按券商 App 的成本价校准本地持仓成本（不下单）。"""
        from astock_trading.execution.service import ExecutionService

        conn = connect()
        try:
            store = EventStore(conn)
            svc = ExecutionService(store, conn)
            position_before = svc.get_position(code)
            if position_before is None:
                raise ValueError(f"未找到持仓：{code}")
            if cost_price <= 0 and cost_basis <= 0:
                raise ValueError("必须提供 --cost-price 或 --cost-basis")

            total_cost = _cost_basis_from_inputs(
                shares=position_before.shares,
                price_cents=position_before.avg_cost_cents,
                cost_price=cost_price,
                cost_basis=cost_basis,
            )

            if not as_json:
                typer.echo("═" * 50)
                typer.echo("  持仓成本校准预览")
                typer.echo("─" * 50)
                typer.echo(f"  股票       {code}  {position_before.name}")
                typer.echo(f"  持仓       {position_before.shares} 股")
                typer.echo(f"  原成本价   ¥{position_before.avg_cost:.3f}")
                typer.echo(f"  新成本价   ¥{total_cost / position_before.shares / 100:.3f}")
                typer.echo(f"  原总成本   ¥{position_before.effective_cost_basis_cents / 100:,.2f}")
                typer.echo(f"  新总成本   ¥{total_cost / 100:,.2f}")
                typer.echo("─" * 50)

            if not yes:
                if as_json:
                    json_or_text({"status": "confirmation_required", "code": code}, True)
                else:
                    typer.echo("添加 --yes 确认写入")
                raise typer.Abort()

            result = svc.adjust_position_cost_basis(
                code=code,
                cost_basis_cents=total_cost,
                reason=reason,
            )
            conn.commit()
            payload = {
                "status": "adjusted",
                "code": code,
                "reason": reason,
                "cost_basis_cents": total_cost,
                "cost_price": total_cost / position_before.shares / 100,
                "event_id": result["event_id"],
                "run_id": result["run_id"],
                "position_before": _position_dict(result["position_before"]),
                "position_after": _position_dict(result["position_after"]),
            }
            if as_json:
                json_or_text(payload, True)
                return
            typer.echo(f"已校准持仓成本：{code} 成本价 ¥{payload['cost_price']:.3f}")
            typer.echo(f"   事件ID：{result['event_id']}")
        except Exception as e:
            typer.secho(f"{e}", fg="red")
            raise typer.Abort()
        finally:
            conn.close()
