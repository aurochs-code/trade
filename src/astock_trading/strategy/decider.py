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
        trial_buy_threshold: float | None = None,
        trial_buy_entry_signal_threshold: float | None = None,
    ):
        self.buy_threshold = buy_threshold
        self.watch_threshold = watch_threshold
        self.reject_threshold = reject_threshold
        self.trial_buy_threshold = (
            buy_threshold if trial_buy_threshold is None else trial_buy_threshold
        )
        self.trial_buy_entry_signal_threshold = (
            max(watch_threshold, buy_threshold - 0.5)
            if trial_buy_entry_signal_threshold is None
            else trial_buy_entry_signal_threshold
        )
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
            if self._trial_buy_allowed(score):
                notes.append("评分和数据质量支持小仓试买意向，但不形成可自动承接买入意向")
                return DecisionIntent(
                    code=score.code, name=score.name,
                    action=Action.TRIAL_BUY, confidence=score.total,
                    score=score.total,
                    position_pct=0.0,
                    market_signal=market.signal,
                    market_multiplier=0.0,
                    notes=notes,
                )
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
                if self._trial_buy_allowed(score, buy_blocks=buy_blocks):
                    return DecisionIntent(
                        code=score.code, name=score.name,
                        action=Action.TRIAL_BUY,
                        confidence=score.total,
                        score=score.total,
                        position_pct=0.0,
                        market_signal=market.signal,
                        market_multiplier=market.multiplier,
                        notes=notes + buy_blocks + [
                            "评分和数据质量支持小仓试买意向，但不形成可自动承接买入意向"
                        ],
                    )
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
            if weekly_buy_count < self.weekly_max and self._trial_buy_allowed(score):
                if score.entry_signal and score.total < self.buy_threshold:
                    trial_reason = "入场信号已触发但评分未达到正式买入线，列为试买意向"
                elif self._watch_route_reaches_trial_line(score):
                    trial_reason = "观察路线证据接近入场要求但尚未触发入场信号，列为试买意向"
                else:
                    trial_reason = "评分达到试买线但未达到正式买入线，列为试买意向"
                return DecisionIntent(
                    code=score.code, name=score.name,
                    action=Action.TRIAL_BUY,
                    confidence=score.total,
                    score=score.total,
                    position_pct=0.0,
                    market_signal=market.signal,
                    market_multiplier=market.multiplier,
                    notes=notes + [trial_reason, "试买意向不形成可自动承接买入意向"],
                )
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

    def _trial_buy_allowed(
        self,
        score: ScoreResult,
        *,
        buy_blocks: list[str] | None = None,
    ) -> bool:
        """Return true for soft setups that deserve a non-executable trial signal."""
        score_reaches_trial_line = score.total >= self.trial_buy_threshold
        entry_signal_reaches_trial_line = (
            score.entry_signal
            and score.total >= self.trial_buy_entry_signal_threshold
        )
        watch_route_reaches_trial_line = self._watch_route_reaches_trial_line(score)
        if (
            not score_reaches_trial_line
            and not entry_signal_reaches_trial_line
            and not watch_route_reaches_trial_line
        ):
            return False

        score_quality = _quality_value(score.data_quality)
        if _quality_rank(score_quality) < _quality_rank(self.min_data_quality_for_buy):
            return False

        missing = list(score.data_missing_fields or [])
        if (
            self.max_missing_fields_for_buy is not None
            and len(missing) > self.max_missing_fields_for_buy
        ):
            return False

        if self.critical_missing_fields_for_buy.intersection(missing):
            return False

        blocks = buy_blocks or []
        if not blocks:
            return True
        return all("入场信号未触发" in reason for reason in blocks)

    def _watch_route_reaches_trial_line(self, score: ScoreResult) -> bool:
        if score.total < self.trial_buy_entry_signal_threshold:
            return False
        return any(_is_trial_watch_route(route) for route in score.strategy_routes)


def _quality_value(value: str | DataQuality) -> str:
    if isinstance(value, DataQuality):
        return value.value
    normalized = str(value or DataQuality.DEGRADED.value).lower()
    if normalized not in _DATA_QUALITY_RANK:
        return DataQuality.DEGRADED.value
    return normalized


def _quality_rank(value: str | DataQuality) -> int:
    return _DATA_QUALITY_RANK.get(_quality_value(value), _DATA_QUALITY_RANK[DataQuality.DEGRADED.value])


def _route_value(route: object, field: str, default: object = None) -> object:
    if isinstance(route, dict):
        return route.get(field, default)
    return getattr(route, field, default)


def _is_trial_watch_route(route: object) -> bool:
    status = str(_route_value(route, "status", "") or "")
    try:
        route_score = float(_route_value(route, "route_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        route_score = 0.0
    return status == "watch" and route_score >= 0.6


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
        trial_buy_threshold=gates.get("trial_buy_threshold"),
        trial_buy_entry_signal_threshold=gates.get("trial_buy_entry_signal_threshold"),
    )
