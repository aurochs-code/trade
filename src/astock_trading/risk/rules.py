"""
risk/rules.py — 风控规则（纯函数）

不做任何 IO。输入持仓 + 快照 + 参数，输出离场信号。
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from astock_trading.risk.models import ExitSignal, RiskParams, RiskBreach
from astock_trading.strategy.models import Style


def _distance_pct(current_price: float, trigger_price: float | None) -> float | None:
    if trigger_price is None or trigger_price <= 0:
        return None
    return round((current_price - trigger_price) / trigger_price, 4)


def build_threshold_snapshot(
    code: str,
    avg_cost: float,
    current_price: float,
    entry_date: date,
    today: date,
    highest_since_entry: float,
    entry_day_low: float,
    params: RiskParams,
    ma20: float = 0,
    ma60: float = 0,
    signals: list[ExitSignal] | None = None,
) -> dict:
    """构造单票风控阈值快照，未触发时也保留各条风险线。"""
    signals = signals or []
    triggered_signal_types = [signal.signal_type for signal in signals]
    holding_days = (today - entry_date).days
    pnl_pct = (current_price - avg_cost) / avg_cost if avg_cost > 0 else 0.0

    stop_price = round(avg_cost * (1 - params.stop_loss), 2)
    trail_price = (
        round(highest_since_entry * (1 - params.trailing_stop), 2)
        if params.trailing_stop and highest_since_entry > 0
        else None
    )
    ma_exit_price = ma20 if params.exit_ma == 20 and ma20 > 0 else None
    absolute_stop_price = (
        round(ma60 * 0.98, 2)
        if params.absolute_stop_ma == 60 and ma60 > 0
        else None
    )

    def _signal_triggered(signal_type: str, trigger_price: float | None = None) -> bool:
        for signal in signals:
            if signal.signal_type != signal_type:
                continue
            if trigger_price is None or round(signal.trigger_price, 2) == round(trigger_price, 2):
                return True
        return False

    style = params.style.value if hasattr(params.style, "value") else str(params.style)
    return {
        "code": code,
        "style": style,
        "avg_cost": round(avg_cost, 4),
        "current_price": round(current_price, 4),
        "entry_date": entry_date.isoformat(),
        "today": today.isoformat(),
        "holding_days": holding_days,
        "highest_since_entry": round(highest_since_entry, 4),
        "entry_day_low": round(entry_day_low, 4),
        "pnl_pct": round(pnl_pct, 4),
        "triggered_signal_types": triggered_signal_types,
        "thresholds": {
            "stop_loss": {
                "enabled": True,
                "trigger_price": stop_price,
                "distance_pct": _distance_pct(current_price, stop_price),
                "triggered": _signal_triggered("stop_loss", stop_price),
            },
            "trailing_stop": {
                "enabled": trail_price is not None,
                "trigger_price": trail_price,
                "distance_pct": _distance_pct(current_price, trail_price),
                "triggered": _signal_triggered("trailing_stop", trail_price),
            },
            "time_stop": {
                "enabled": params.time_stop_days > 0,
                "days_limit": params.time_stop_days,
                "days_remaining": max(params.time_stop_days - holding_days, 0),
                "triggered": _signal_triggered("time_stop"),
            },
            "ma_exit": {
                "enabled": ma_exit_price is not None,
                "ma": params.exit_ma,
                "trigger_price": ma_exit_price,
                "distance_pct": _distance_pct(current_price, ma_exit_price),
                "triggered": _signal_triggered("ma_exit", ma_exit_price),
            },
            "absolute_stop": {
                "enabled": absolute_stop_price is not None,
                "ma": params.absolute_stop_ma,
                "trigger_price": absolute_stop_price,
                "distance_pct": _distance_pct(current_price, absolute_stop_price),
                "triggered": _signal_triggered("stop_loss", absolute_stop_price),
            },
        },
    }


def get_risk_params(style: Style, risk_cfg: Optional[dict] = None) -> RiskParams:
    """根据风格返回对应的风控参数。"""
    risk_cfg = risk_cfg or {}
    if style == Style.SLOW_BULL:
        sb = risk_cfg.get("slow_bull", {})
        return RiskParams(
            style=style,
            stop_loss=sb.get("stop_loss", 0.08),
            absolute_stop_ma=sb.get("absolute_stop_ma", 60),
            trailing_stop=None,
            exit_ma=sb.get("exit_ma", 20),
            time_stop_days=sb.get("time_stop_days", 30),
        )
    else:
        mm = risk_cfg.get("momentum", {})
        return RiskParams(
            style=style,
            stop_loss=mm.get("stop_loss", 0.08),
            trailing_stop=mm.get("trailing_stop", 0.10),
            exit_ma=mm.get("exit_ma", 20),
            time_stop_days=mm.get("time_stop_days", 15),
            stop_loss_anchor=mm.get("stop_loss_anchor", "entry_day_low"),
        )


def check_exit_signals(
    code: str,
    avg_cost: float,
    current_price: float,
    entry_date: date,
    today: date,
    highest_since_entry: float,
    entry_day_low: float,
    params: RiskParams,
    ma20: float = 0,
    ma60: float = 0,
) -> list[ExitSignal]:
    """
    纯函数：检查所有离场信号。

    Returns:
        触发的离场信号列表（可能为空）
    """
    signals: list[ExitSignal] = []

    # 固定止损
    stop_price = round(avg_cost * (1 - params.stop_loss), 2)
    if current_price <= stop_price:
        signals.append(ExitSignal(
            code=code,
            signal_type="stop_loss",
            trigger_price=stop_price,
            current_price=current_price,
            description=f"跌破止损线 {stop_price}（成本 {avg_cost} × {1-params.stop_loss:.0%}）",
            urgency="immediate",
        ))

    # 移动止盈（题材股）
    if params.trailing_stop and highest_since_entry > 0:
        trail_price = round(highest_since_entry * (1 - params.trailing_stop), 2)
        if current_price <= trail_price:
            drawdown = (highest_since_entry - current_price) / highest_since_entry
            signals.append(ExitSignal(
                code=code,
                signal_type="trailing_stop",
                trigger_price=trail_price,
                current_price=current_price,
                description=f"最高 {highest_since_entry} 回撤 {drawdown:.1%} > {params.trailing_stop:.0%}",
                urgency="immediate",
            ))

    # 时间止损
    holding_days = (today - entry_date).days
    if holding_days >= params.time_stop_days:
        # 只有没涨的情况才触发
        pnl_pct = (current_price - avg_cost) / avg_cost if avg_cost > 0 else 0
        if pnl_pct <= 0.05:  # 涨幅不足 5%
            signals.append(ExitSignal(
                code=code,
                signal_type="time_stop",
                trigger_price=avg_cost,
                current_price=current_price,
                description=f"持仓 {holding_days} 天 >= {params.time_stop_days} 天，涨幅仅 {pnl_pct:.1%}",
                urgency="advisory",
            ))

    # MA 跌破离场
    if params.exit_ma == 20 and ma20 > 0 and current_price < ma20:
        signals.append(ExitSignal(
            code=code,
            signal_type="ma_exit",
            trigger_price=ma20,
            current_price=current_price,
            description=f"收盘价 {current_price} < MA20 {ma20}",
            urgency="end_of_day",
        ))

    # 慢牛：跌破 MA60 绝对止损
    if params.absolute_stop_ma == 60 and ma60 > 0 and current_price < ma60 * 0.98:
        signals.append(ExitSignal(
            code=code,
            signal_type="stop_loss",
            trigger_price=round(ma60 * 0.98, 2),
            current_price=current_price,
            description=f"跌破 MA60 绝对止损线 {ma60 * 0.98:.2f}",
            urgency="immediate",
        ))

    return signals


def check_portfolio_risk(
    daily_pnl_pct: float,
    consecutive_loss_days: int,
    max_single_exposure_pct: float,
    max_sector_exposure_pct: float,
    limits: dict,
) -> list[RiskBreach]:
    """纯函数：检查组合级风控。"""
    breaches: list[RiskBreach] = []

    daily_limit = limits.get("daily_loss_limit_pct", 0.03)
    if daily_pnl_pct < -daily_limit:
        breaches.append(RiskBreach(
            rule="daily_loss_limit",
            current_value=daily_pnl_pct,
            limit_value=-daily_limit,
            description=f"单日亏损 {daily_pnl_pct:.1%} 超过限制 {daily_limit:.0%}",
        ))

    consec_limit = limits.get("consecutive_loss_days_limit", 2)
    if consecutive_loss_days >= consec_limit:
        breaches.append(RiskBreach(
            rule="consecutive_loss_days",
            current_value=consecutive_loss_days,
            limit_value=consec_limit,
            description=f"连续亏损 {consecutive_loss_days} 天 >= {consec_limit} 天",
        ))

    single_warn = limits.get("max_single_position_warn_pct", 0.25)
    if max_single_exposure_pct > single_warn:
        breaches.append(RiskBreach(
            rule="single_position_concentration",
            current_value=max_single_exposure_pct,
            limit_value=single_warn,
            description=f"单股仓位 {max_single_exposure_pct:.0%} > {single_warn:.0%}",
        ))

    # 行业集中度检查
    sector_warn = limits.get("max_sector_exposure_warn_pct", 0.40)
    if max_sector_exposure_pct > sector_warn:
        breaches.append(RiskBreach(
            rule="sector_concentration",
            current_value=max_sector_exposure_pct,
            limit_value=sector_warn,
            description=f"行业仓位 {max_sector_exposure_pct:.0%} > {sector_warn:.0%}",
        ))

    return breaches
