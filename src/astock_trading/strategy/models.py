"""
strategy/models.py — 策略领域模型

所有模型为 frozen dataclass，不可变。
金额用 float（领域层），持久化时转为 cents 整数。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class Style(str, Enum):
    SLOW_BULL = "slow_bull"
    MOMENTUM = "momentum"
    UNKNOWN = "unknown"


class Action(str, Enum):
    BUY = "BUY"
    TRIAL_BUY = "TRIAL_BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    WATCH = "WATCH"
    CLEAR = "CLEAR"


class MarketSignal(str, Enum):
    GREEN = "GREEN"
    YELLOW = "YELLOW"
    RED = "RED"
    CLEAR = "CLEAR"


class DataQuality(str, Enum):
    OK = "ok"
    DEGRADED = "degraded"
    ERROR = "error"


@dataclass(frozen=True)
class ScoringWeights:
    technical: float = 3.0
    fundamental: float = 2.0
    flow: float = 2.0
    sentiment: float = 3.0

    @property
    def total(self) -> float:
        return self.technical + self.fundamental + self.flow + self.sentiment


@dataclass(frozen=True)
class DimensionScore:
    name: str
    score: float
    max_score: float
    detail: str
    raw_data: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "score": self.score,
            "max_score": self.max_score,
            "detail": self.detail,
            "raw_data": self.raw_data,
        }


@dataclass(frozen=True)
class StrategyRouteEvidence:
    """Deterministic evidence that a stock matches a known strategy route."""

    route: str
    display_name: str
    family: str
    confidence: float
    evidence: dict = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    entry_signal: bool = False
    status: str = ""
    route_score: float = 0.0
    matched_conditions: list[str] = field(default_factory=list)
    missing_conditions: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.status:
            object.__setattr__(self, "status", "entry" if self.entry_signal else "watch")
        if self.route_score <= 0 and self.entry_signal:
            object.__setattr__(self, "route_score", 1.0)

    def to_dict(self) -> dict:
        return {
            "route": self.route,
            "display_name": self.display_name,
            "family": self.family,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "notes": self.notes,
            "entry_signal": self.entry_signal,
            "status": self.status,
            "route_score": self.route_score,
            "matched_conditions": self.matched_conditions,
            "missing_conditions": self.missing_conditions,
        }


@dataclass(frozen=True)
class StrategyRouteDiagnostic:
    """Condition-level route diagnosis for matched and near-matched routes."""

    route: str
    display_name: str
    family: str
    status: str
    route_score: float
    matched_conditions: list[str] = field(default_factory=list)
    missing_conditions: list[str] = field(default_factory=list)
    entry_signal: bool = False
    confidence: float = 0.0
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "route": self.route,
            "display_name": self.display_name,
            "family": self.family,
            "status": self.status,
            "route_score": self.route_score,
            "matched_conditions": self.matched_conditions,
            "missing_conditions": self.missing_conditions,
            "entry_signal": self.entry_signal,
            "confidence": self.confidence,
            "notes": self.notes,
        }


@dataclass(frozen=True)
class ScoreResult:
    code: str
    name: str
    total: float
    dimensions: list[DimensionScore] = field(default_factory=list)
    veto_signals: list[str] = field(default_factory=list)
    hard_veto: list[str] = field(default_factory=list)
    warning_signals: list[str] = field(default_factory=list)
    veto_triggered: bool = False
    entry_signal: bool = False
    style: Style = Style.UNKNOWN
    style_confidence: float = 0.0
    data_quality: DataQuality = DataQuality.OK
    data_missing_fields: list[str] = field(default_factory=list)
    strategy_routes: list[StrategyRouteEvidence] = field(default_factory=list)
    route_diagnostics: list[StrategyRouteDiagnostic] = field(default_factory=list)
    primary_strategy_route: Optional[str] = None
    scored_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "name": self.name,
            "total_score": self.total,
            "technical_score": self._dim_score("technical"),
            "fundamental_score": self._dim_score("fundamental"),
            "flow_score": self._dim_score("flow"),
            "sentiment_score": self._dim_score("sentiment"),
            "technical_detail": self._dim_detail("technical"),
            "fundamental_detail": self._dim_detail("fundamental"),
            "flow_detail": self._dim_detail("flow"),
            "sentiment_detail": self._dim_detail("sentiment"),
            "veto_signals": self.veto_signals,
            "hard_veto_signals": self.hard_veto,
            "warning_signals": self.warning_signals,
            "veto_triggered": self.veto_triggered,
            "entry_signal": self.entry_signal,
            "style": self.style.value,
            "style_confidence": self.style_confidence,
            "data_quality": self.data_quality.value,
            "data_missing_fields": self.data_missing_fields,
            "dimensions": [dimension.to_dict() for dimension in self.dimensions],
            "strategy_routes": [route.to_dict() for route in self.strategy_routes],
            "route_diagnostics": [
                diagnostic.to_dict() for diagnostic in self.route_diagnostics
            ],
            "primary_strategy_route": self.primary_strategy_route,
        }

    def _dim_score(self, name: str) -> float:
        for d in self.dimensions:
            if d.name == name:
                return d.score
        return 0.0

    def _dim_detail(self, name: str) -> str:
        for d in self.dimensions:
            if d.name == name:
                return d.detail
        return ""


@dataclass(frozen=True)
class MarketState:
    signal: MarketSignal = MarketSignal.CLEAR
    multiplier: float = 0.0
    detail: dict = field(default_factory=dict)


@dataclass(frozen=True)
class StyleResult:
    """风格判定结果。"""
    style: Style
    confidence: float
    metrics: dict = field(default_factory=dict)
    reason: str = ""


@dataclass(frozen=True)
class EntrySignal:
    """入场信号。"""
    code: str
    signal_type: str  # "golden_cross" | "pullback" | "trend"
    triggered: bool = False
    detail: str = ""


@dataclass(frozen=True)
class SwitchResult:
    """风格切换检测结果。"""
    should_switch: bool = False
    trigger: str = ""
    new_style: Style = Style.SLOW_BULL


@dataclass(frozen=True)
class DecisionIntent:
    code: str
    name: str
    action: Action
    confidence: float
    score: float
    position_pct: float = 0.0
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None
    market_signal: MarketSignal = MarketSignal.CLEAR
    market_multiplier: float = 0.0
    veto_reasons: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
