"""
risk/service.py — 风控服务层

编排风控检查，结果写入 event_log。
这是 risk context 唯一允许做 IO（写事件）的地方。
"""

from __future__ import annotations

from datetime import date

from astock_trading.platform.events import EventStore
from astock_trading.risk.models import (
    ExitSignal,
    PortfolioLimits,
    PositionSize,
    RiskBreach,
    RiskParams,
)
from astock_trading.risk.rules import check_exit_signals, check_portfolio_risk
from astock_trading.risk.sizing import calc_position_size


class RiskService:
    """编排风控检查，结果追加到 event_log。"""

    def __init__(self, event_store: EventStore):
        self._event_store = event_store

    def assess_position(
        self,
        code: str,
        avg_cost: float,
        current_price: float,
        entry_date: date,
        today: date,
        highest_since_entry: float,
        entry_day_low: float,
        risk_params: RiskParams,
        run_id: str,
        ma20: float = 0,
        ma60: float = 0,
    ) -> list[ExitSignal]:
        """
        单票风控检查，结果追加到 event_log。

        Returns:
            触发的离场信号列表
        """
        signals = check_exit_signals(
            code=code,
            avg_cost=avg_cost,
            current_price=current_price,
            entry_date=entry_date,
            today=today,
            highest_since_entry=highest_since_entry,
            entry_day_low=entry_day_low,
            params=risk_params,
            ma20=ma20,
            ma60=ma60,
        )

        metadata = {"run_id": run_id}

        for sig in signals:
            self._event_store.append(
                stream=f"risk:{code}",
                stream_type="risk",
                event_type=f"risk.{sig.signal_type}_triggered",
                payload={
                    "code": sig.code,
                    "signal_type": sig.signal_type,
                    "trigger_price": sig.trigger_price,
                    "current_price": sig.current_price,
                    "description": sig.description,
                    "urgency": sig.urgency,
                },
                metadata=metadata,
            )

        return signals

    def assess_portfolio(
        self,
        daily_pnl_pct: float,
        consecutive_loss_days: int,
        max_single_exposure_pct: float,
        max_sector_exposure_pct: float,
        limits: PortfolioLimits,
        run_id: str,
    ) -> list[RiskBreach]:
        """
        组合风控检查，结果追加到 event_log。
        """
        breaches = check_portfolio_risk(
            daily_pnl_pct=daily_pnl_pct,
            consecutive_loss_days=consecutive_loss_days,
            max_single_exposure_pct=max_single_exposure_pct,
            max_sector_exposure_pct=max_sector_exposure_pct,
            limits=limits.to_dict(),
        )

        metadata = {"run_id": run_id}

        for breach in breaches:
            self._event_store.append(
                stream="risk:portfolio",
                stream_type="risk",
                event_type="risk.portfolio_breach",
                payload={
                    "rule": breach.rule,
                    "current_value": breach.current_value,
                    "limit_value": breach.limit_value,
                    "description": breach.description,
                },
                metadata=metadata,
            )

        return breaches

    def calc_and_record_position(
        self,
        code: str,
        total_capital: float,
        current_exposure_pct: float,
        price: float,
        market_multiplier: float,
        run_id: str,
        single_max_pct: float = 0.20,
        total_max_pct: float = 0.60,
    ) -> PositionSize:
        """
        仓位计算，结果追加到 event_log。
        """
        ps = calc_position_size(
            total_capital=total_capital,
            current_exposure_pct=current_exposure_pct,
            price=price,
            market_multiplier=market_multiplier,
            single_max_pct=single_max_pct,
            total_max_pct=total_max_pct,
        )

        self._event_store.append(
            stream=f"risk:{code}",
            stream_type="risk",
            event_type="risk.position_sized",
            payload={
                "code": code,
                "shares": ps.shares,
                "amount": ps.amount,
                "pct": ps.pct,
                "market_multiplier": ps.market_multiplier,
            },
            metadata={"run_id": run_id},
        )

        return ps
