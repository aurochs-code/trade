"""
risk/models.py — 风控领域模型
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from astock_trading.strategy.models import Style


@dataclass(frozen=True)
class RiskParams:
    style: Style
    stop_loss: float = 0.08
    trailing_stop: Optional[float] = None
    exit_ma: int = 20
    time_stop_days: int = 15
    absolute_stop_ma: Optional[int] = None
    stop_loss_anchor: Optional[str] = None


@dataclass(frozen=True)
class ExitSignal:
    code: str
    signal_type: str  # stop_loss / trailing_stop / time_stop / ma_exit / style_switch
    trigger_price: float
    current_price: float
    description: str
    urgency: str = "end_of_day"  # immediate / end_of_day / advisory


@dataclass(frozen=True)
class PositionSize:
    shares: int
    amount: float
    pct: float
    market_multiplier: float = 1.0


@dataclass(frozen=True)
class RiskBreach:
    rule: str
    current_value: float
    limit_value: float
    description: str


@dataclass(frozen=True)
class PortfolioLimits:
    """组合风控阈值。"""
    daily_loss_limit_pct: float = 0.03
    consecutive_loss_days_limit: int = 2
    cooldown_days: int = 2
    max_single_position_warn_pct: float = 0.25
    max_sector_exposure_warn_pct: float = 0.40

    def to_dict(self) -> dict:
        return {
            "daily_loss_limit_pct": self.daily_loss_limit_pct,
            "consecutive_loss_days_limit": self.consecutive_loss_days_limit,
            "cooldown_days": self.cooldown_days,
            "max_single_position_warn_pct": self.max_single_position_warn_pct,
            "max_sector_exposure_warn_pct": self.max_sector_exposure_warn_pct,
        }


@dataclass(frozen=True)
class RiskAssessment:
    """风控评估结果。"""
    code: str
    exit_signals: list[ExitSignal] = field(default_factory=list)
    portfolio_breaches: list[RiskBreach] = field(default_factory=list)
    position_size: Optional[PositionSize] = None
    blocked: bool = False
    block_reason: str = ""
