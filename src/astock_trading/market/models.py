"""
market/models.py — 市场数据模型

由 market context 的 adapters 产出，strategy/risk context 消费。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class StockQuote:
    code: str
    name: str
    price: float
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float
    change_pct: float
    timestamp: Optional[datetime] = None


@dataclass(frozen=True)
class TechnicalIndicators:
    ma5: float = 0.0
    ma10: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    above_ma20: bool = False
    volume_ratio: float = 1.0
    rsi: float = 50.0
    golden_cross: bool = False
    ma20_slope: float = 0.0
    momentum_5d: float = 0.0
    daily_volatility: float = 0.0
    deviation_rate: float = 0.0
    change_pct: float = 0.0


@dataclass(frozen=True)
class FinancialReport:
    roe: Optional[float] = None
    roe_3y_ago: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_profit_growth: Optional[float] = None
    operating_cash_flow: Optional[float] = None
    pe_ttm: Optional[float] = None
    pb: Optional[float] = None
    debt_ratio: Optional[float] = None


@dataclass(frozen=True)
class FundFlow:
    net_inflow_1d: float = 0.0
    net_inflow_5d: float = 0.0
    main_force_ratio: float = 0.0
    northbound_net: float = 0.0
    northbound_net_positive: bool = False
    consecutive_outflow_days: int = 0


@dataclass(frozen=True)
class SentimentData:
    score: float = 1.5
    news_count: int = 0
    positive_ratio: float = 0.5
    detail: str = ""
    key_events: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SectorContext:
    """Sector strength context used by route-specific scoring."""

    industry_name: str = ""
    industry_rank: Optional[int] = None
    industry_change_pct: float = 0.0
    turnover_yi: Optional[float] = None
    leader: str = ""
    relative_strength_pct: float = 0.0
    confirmed: bool = False


@dataclass(frozen=True)
class StockSnapshot:
    """评分所需的全部数据。由 MarketService 组装，Scorer 消费。"""
    code: str
    name: str
    observation_id: Optional[str] = None
    quote: Optional[StockQuote] = None
    technical: Optional[TechnicalIndicators] = None
    financial: Optional[FinancialReport] = None
    flow: Optional[FundFlow] = None
    sentiment: Optional[SentimentData] = None
    sector: Optional[SectorContext] = None
    kline: Optional[pd.DataFrame] = None


@dataclass(frozen=True)
class IndexQuote:
    """指数行情。"""
    symbol: str
    name: str
    price: float = 0.0
    change_pct: float = 0.0
    ma20: float = 0.0
    ma60: float = 0.0
    above_ma20: bool = False
    below_ma60_days: int = 0
    timestamp: Optional[datetime] = None
