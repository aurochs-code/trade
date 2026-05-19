"""风控和仓位计算 CLI 命令。"""

from __future__ import annotations

from datetime import datetime

import typer

from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_today
from astock_trading.risk.rules import check_exit_signals, check_portfolio_risk, get_risk_params
from astock_trading.risk.sizing import calc_position_size
from astock_trading.strategy.models import Style


risk_app = typer.Typer(name="risk", help="风控检查和仓位计算")


def _strategy_config() -> dict:
    data, _errors = ConfigRegistry().load_and_validate()
    return data.get("strategy", {})


def _position_limits(strategy: dict) -> dict:
    return strategy.get("risk", {}).get("position", {})


def _risk_limits(strategy: dict) -> dict:
    return strategy.get("risk", {}).get("portfolio", strategy.get("risk", {}))


def _position_style(style: str) -> Style:
    if style == Style.SLOW_BULL.value:
        return Style.SLOW_BULL
    if style == Style.MOMENTUM.value:
        return Style.MOMENTUM
    return Style.UNKNOWN


def _risk_signal_payload(signal) -> dict:
    return {
        "signal_type": signal.signal_type,
        "trigger_price": signal.trigger_price,
        "current_price": signal.current_price,
        "description": signal.description,
        "urgency": signal.urgency,
    }


@risk_app.command("position")
def risk_position(
    code: str = typer.Argument(..., help="股票代码"),
    score: float = typer.Argument(..., help="评分，用于记录本次仓位建议的依据"),
    price: float = typer.Argument(..., help="当前价格"),
    capital: float | None = typer.Option(None, "--capital", help="总资金；默认读取 strategy.capital"),
    current_exposure_pct: float = typer.Option(0.0, "--current-exposure-pct", help="当前总仓位占比"),
    market_multiplier: float = typer.Option(1.0, "--market-multiplier", help="大盘仓位系数"),
    single_max_pct: float | None = typer.Option(None, "--single-max-pct", help="单股仓位上限"),
    total_max_pct: float | None = typer.Option(None, "--total-max-pct", help="总仓位上限"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """计算建议仓位；只读，不写入数据库。"""
    strategy = _strategy_config()
    position_cfg = _position_limits(strategy)
    total_capital = capital if capital is not None else float(strategy.get("capital", 500000))
    single_max = single_max_pct if single_max_pct is not None else float(position_cfg.get("single_max", 0.20))
    total_max = total_max_pct if total_max_pct is not None else float(position_cfg.get("total_max", 0.60))

    size = calc_position_size(
        total_capital=total_capital,
        current_exposure_pct=current_exposure_pct,
        price=price,
        market_multiplier=market_multiplier,
        single_max_pct=single_max,
        total_max_pct=total_max,
    )
    payload = {
        "code": code,
        "score": score,
        "price": price,
        "capital": total_capital,
        "current_exposure_pct": current_exposure_pct,
        "market_multiplier": market_multiplier,
        "single_max_pct": single_max,
        "total_max_pct": total_max,
        "shares": size.shares,
        "amount": size.amount,
        "pct": size.pct,
    }
    json_or_text(payload, as_json)


@risk_app.command("check")
def risk_check(
    code: str = typer.Argument(..., help="股票代码"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查单只本地持仓的离场风控信号。"""
    init_db()
    conn = connect()
    try:
        from astock_trading.execution.service import ExecutionService

        svc = ExecutionService(EventStore(conn), conn)
        pos = svc.get_position(code)
        if not pos:
            json_or_text({"status": "not_held", "code": code, "signals": []}, as_json)
            return

        style = _position_style(pos.style)
        risk_cfg = _strategy_config().get("risk", {})
        params = get_risk_params(style, risk_cfg)
        today = local_today()
        try:
            entry_date = datetime.strptime(pos.entry_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            entry_date = today

        signals = check_exit_signals(
            code=code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price or pos.avg_cost,
            entry_date=entry_date,
            today=today,
            highest_since_entry=(
                pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost
            ),
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            params=params,
        )
        payload = {
            "status": "ok",
            "code": code,
            "position": pos.to_dict(),
            "signals": [_risk_signal_payload(signal) for signal in signals],
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()


@risk_app.command("portfolio")
def risk_portfolio(
    daily_pnl_pct: float = typer.Option(0.0, "--daily-pnl-pct", help="单日收益率，用小数表示"),
    consecutive_loss_days: int = typer.Option(0, "--consecutive-loss-days", help="连续亏损天数"),
    max_sector_exposure_pct: float = typer.Option(0.0, "--max-sector-exposure-pct", help="最大行业仓位占比"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """检查组合级风控限制；只读。"""
    init_db()
    conn = connect()
    try:
        from astock_trading.execution.service import ExecutionService

        portfolio = ExecutionService(EventStore(conn), conn).get_portfolio()
        positions = portfolio.get("positions", [])
        total_market = portfolio.get("total_market_cents", 0) or 0
        if total_market > 0:
            max_single_exposure_pct = max(
                ((item.get("current_price_cents") or item.get("avg_cost_cents") or 0) * item.get("shares", 0))
                / total_market
                for item in positions
            )
        else:
            max_single_exposure_pct = 0.0

        limits = _risk_limits(_strategy_config())
        breaches = check_portfolio_risk(
            daily_pnl_pct=daily_pnl_pct,
            consecutive_loss_days=consecutive_loss_days,
            max_single_exposure_pct=max_single_exposure_pct,
            max_sector_exposure_pct=max_sector_exposure_pct,
            limits=limits,
        )
        payload = {
            "status": "breached" if breaches else "ok",
            "inputs": {
                "daily_pnl_pct": daily_pnl_pct,
                "consecutive_loss_days": consecutive_loss_days,
                "max_single_exposure_pct": max_single_exposure_pct,
                "max_sector_exposure_pct": max_sector_exposure_pct,
            },
            "breaches": [
                {
                    "rule": breach.rule,
                    "current_value": breach.current_value,
                    "limit_value": breach.limit_value,
                    "description": breach.description,
                }
                for breach in breaches
            ],
        }
        json_or_text(payload, as_json)
    finally:
        conn.close()
