"""
strategy/decider.py — 综合决策引擎（纯函数）

不做任何 IO。输入 ScoreResult + MarketState，输出 DecisionIntent。
"""

from __future__ import annotations


from astock_trading.strategy.models import (
    Action,
    DataQuality,
    DecisionIntent,
    MarketSignal,
    MarketState,
    ScoreResult,
)


_DATA_QUALITY_RANK = {
    DataQuality.ERROR.value: 0,
    DataQuality.DEGRADED.value: 1,
    DataQuality.OK.value: 2,
}


class Decider:
    """综合决策 — 纯函数，无副作用。"""

    def __init__(
        self,
        buy_threshold: float = 6.5,
        watch_threshold: float = 5.0,
        reject_threshold: float = 4.0,
        single_max_pct: float = 0.20,
        total_max_pct: float = 0.60,
        weekly_max: int = 2,
        require_entry_signal_for_buy: bool = False,
        min_data_quality_for_buy: str | DataQuality = DataQuality.DEGRADED,
        max_missing_fields_for_buy: int | None = None,
        critical_missing_fields_for_buy: list[str] | tuple[str, ...] | None = None,
        min_position_pct_for_buy: float = 0.01,
    ):
        self.buy_threshold = buy_threshold
        self.watch_threshold = watch_threshold
        self.reject_threshold = reject_threshold
        self.single_max_pct = single_max_pct
        self.total_max_pct = total_max_pct
        self.weekly_max = weekly_max
        self.require_entry_signal_for_buy = require_entry_signal_for_buy
        self.min_data_quality_for_buy = _quality_value(min_data_quality_for_buy)
        self.max_missing_fields_for_buy = max_missing_fields_for_buy
        self.critical_missing_fields_for_buy = set(critical_missing_fields_for_buy or [])
        self.min_position_pct_for_buy = min_position_pct_for_buy

    def decide(
        self,
        score: ScoreResult,
        market: MarketState,
        current_exposure_pct: float = 0.0,
        weekly_buy_count: int = 0,
    ) -> DecisionIntent:
        notes: list[str] = []
        veto_reasons: list[str] = []

        # Veto check
        if score.veto_triggered:
            veto_reasons = list(score.hard_veto)
            return DecisionIntent(
                code=score.code, name=score.name,
                action=Action.CLEAR, confidence=0,
                score=score.total,
                market_signal=market.signal,
                market_multiplier=market.multiplier,
                veto_reasons=veto_reasons,
                notes=["一票否决"],
            )

        # Market signal block
        if market.signal in (MarketSignal.RED, MarketSignal.CLEAR):
            notes.append(f"大盘 {market.signal.value}，禁止新开仓")
            return DecisionIntent(
                code=score.code, name=score.name,
                action=Action.WATCH, confidence=score.total,
                score=score.total,
                market_signal=market.signal,
                market_multiplier=0.0,
                notes=notes,
            )

        # Weekly limit
        if weekly_buy_count >= self.weekly_max:
            notes.append(f"本周已买 {weekly_buy_count}/{self.weekly_max}")

        # Score-based decision
        if score.total >= self.buy_threshold and weekly_buy_count < self.weekly_max:
            position_pct = self.single_max_pct * market.multiplier
            remaining = max(0, self.total_max_pct - current_exposure_pct)
            position_pct = min(position_pct, remaining)

            buy_blocks = self._buy_block_reasons(score, position_pct)
            if buy_blocks:
                return DecisionIntent(
                    code=score.code, name=score.name,
                    action=Action.WATCH,
                    confidence=score.total,
                    score=score.total,
                    position_pct=0.0,
                    market_signal=market.signal,
                    market_multiplier=market.multiplier,
                    notes=notes + buy_blocks,
                )

            return DecisionIntent(
                code=score.code, name=score.name,
                action=Action.BUY,
                confidence=score.total,
                score=score.total,
                position_pct=position_pct,
                market_signal=market.signal,
                market_multiplier=market.multiplier,
                notes=notes,
            )
        elif score.total >= self.watch_threshold:
            return DecisionIntent(
                code=score.code, name=score.name,
                action=Action.WATCH,
                confidence=score.total,
                score=score.total,
                market_signal=market.signal,
                market_multiplier=market.multiplier,
                notes=notes,
            )
        else:
            return DecisionIntent(
                code=score.code, name=score.name,
                action=Action.CLEAR,
                confidence=score.total,
                score=score.total,
                market_signal=market.signal,
                market_multiplier=market.multiplier,
                notes=["评分过低"],
            )

    def decide_batch(
        self,
        scores: list[ScoreResult],
        market: MarketState,
        current_exposure_pct: float = 0.0,
        weekly_buy_count: int = 0,
    ) -> list[DecisionIntent]:
        return [
            self.decide(s, market, current_exposure_pct, weekly_buy_count)
            for s in scores
        ]

    def _buy_block_reasons(self, score: ScoreResult, position_pct: float) -> list[str]:
        reasons: list[str] = []

        if position_pct < self.min_position_pct_for_buy:
            reasons.append(
                f"仓位空间不足：建议仓位 {position_pct:.1%} "
                f"< {self.min_position_pct_for_buy:.1%}"
            )

        if self.require_entry_signal_for_buy and not score.entry_signal:
            reasons.append("入场信号未触发")

        score_quality = _quality_value(score.data_quality)
        if _quality_rank(score_quality) < _quality_rank(self.min_data_quality_for_buy):
            reasons.append(
                f"数据质量 {score_quality} 低于要求 {self.min_data_quality_for_buy}"
            )

        missing = list(score.data_missing_fields or [])
        if (
            self.max_missing_fields_for_buy is not None
            and len(missing) > self.max_missing_fields_for_buy
        ):
            reasons.append(
                f"关键数据缺失过多：{len(missing)} "
                f"> {self.max_missing_fields_for_buy}"
            )

        critical_missing = sorted(self.critical_missing_fields_for_buy.intersection(missing))
        if critical_missing:
            reasons.append("关键字段缺失：" + ",".join(critical_missing))

        return reasons


def _quality_value(value: str | DataQuality) -> str:
    if isinstance(value, DataQuality):
        return value.value
    normalized = str(value or DataQuality.DEGRADED.value).lower()
    if normalized not in _DATA_QUALITY_RANK:
        return DataQuality.DEGRADED.value
    return normalized


def _quality_rank(value: str | DataQuality) -> int:
    return _DATA_QUALITY_RANK.get(_quality_value(value), _DATA_QUALITY_RANK[DataQuality.DEGRADED.value])


def build_decider_from_config(cfg: dict) -> Decider:
    """Build Decider from strategy config, including optional buy-side gates."""
    scoring_cfg = cfg.get("scoring", {})
    thresholds = scoring_cfg.get("thresholds", {})
    gates = scoring_cfg.get("decision_gates", {})
    pos_cfg = cfg.get("risk", {}).get("position", {})
    return Decider(
        buy_threshold=thresholds.get("buy", 5.5),
        watch_threshold=thresholds.get("watch", 5.0),
        reject_threshold=thresholds.get("reject", 4.0),
        single_max_pct=pos_cfg.get("single_max", 0.20),
        total_max_pct=pos_cfg.get("total_max", 0.60),
        weekly_max=pos_cfg.get("weekly_max", 2),
        require_entry_signal_for_buy=bool(gates.get("require_entry_signal_for_buy", False)),
        min_data_quality_for_buy=gates.get("min_data_quality_for_buy", DataQuality.DEGRADED.value),
        max_missing_fields_for_buy=gates.get("max_missing_fields_for_buy"),
        critical_missing_fields_for_buy=gates.get("critical_missing_fields_for_buy", []),
        min_position_pct_for_buy=gates.get("min_position_pct_for_buy", 0.01),
    )
