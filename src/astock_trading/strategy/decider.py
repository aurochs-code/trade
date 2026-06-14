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
        market_regime_overlays: dict | None = None,
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
        self.market_regime_overlays = market_regime_overlays or {}

    def decide(
        self,
        score: ScoreResult,
        market: MarketState,
        current_exposure_pct: float = 0.0,
        weekly_buy_count: int = 0,
    ) -> DecisionIntent:
        notes: list[str] = []
        veto_reasons: list[str] = []
        regime = self._regime_rules(market.signal)
        buy_threshold = float(regime.get("buy_threshold", self.buy_threshold))
        watch_threshold = float(regime.get("watch_threshold", self.watch_threshold))
        trial_buy_threshold = float(regime.get("trial_buy_threshold", self.trial_buy_threshold))
        trial_buy_entry_signal_threshold = float(
            regime.get("trial_buy_entry_signal_threshold", self.trial_buy_entry_signal_threshold)
        )
        allow_trial_buy = bool(regime.get("allow_trial_buy", True))
        enabled_trial_routes = tuple(str(item) for item in regime.get("enabled_trial_routes", []) or [])
        disabled_trial_routes = tuple(str(item) for item in regime.get("disabled_trial_routes", []) or [])
        if buy_threshold != self.buy_threshold:
            notes.append(f"市场制度 {market.signal.value} 买入线 {buy_threshold:.1f}")
        if enabled_trial_routes and not _trial_route_enabled(score, enabled_trial_routes):
            notes.append(f"市场制度只允许试买路线：{','.join(enabled_trial_routes)}")
        if disabled_trial_routes and _trial_route_disabled(score, disabled_trial_routes):
            notes.append(f"市场制度禁用试买路线：{','.join(disabled_trial_routes)}")

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
            if self._trial_buy_allowed(
                score,
                trial_buy_threshold=trial_buy_threshold,
                trial_buy_entry_signal_threshold=trial_buy_entry_signal_threshold,
                allow_trial_buy=allow_trial_buy,
                enabled_trial_routes=enabled_trial_routes,
                disabled_trial_routes=disabled_trial_routes,
            ):
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
            if not allow_trial_buy:
                notes.append("市场制度阻断观察：弱市只记录研究信号，不形成试买意向")
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
        if score.total >= buy_threshold and weekly_buy_count < self.weekly_max:
            position_pct = self.single_max_pct * market.multiplier
            remaining = max(0, self.total_max_pct - current_exposure_pct)
            position_pct = min(position_pct, remaining)

            buy_blocks = self._buy_block_reasons(score, position_pct)
            if buy_blocks:
                if self._trial_buy_allowed(
                    score,
                    buy_blocks=buy_blocks,
                    trial_buy_threshold=trial_buy_threshold,
                    trial_buy_entry_signal_threshold=trial_buy_entry_signal_threshold,
                    allow_trial_buy=allow_trial_buy,
                    enabled_trial_routes=enabled_trial_routes,
                    disabled_trial_routes=disabled_trial_routes,
                ):
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
        elif score.total >= watch_threshold:
            if weekly_buy_count < self.weekly_max and self._trial_buy_allowed(
                score,
                trial_buy_threshold=trial_buy_threshold,
                trial_buy_entry_signal_threshold=trial_buy_entry_signal_threshold,
                allow_trial_buy=allow_trial_buy,
                enabled_trial_routes=enabled_trial_routes,
                disabled_trial_routes=disabled_trial_routes,
            ):
                if score.entry_signal and score.total < buy_threshold:
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
        trial_buy_threshold: float | None = None,
        trial_buy_entry_signal_threshold: float | None = None,
        allow_trial_buy: bool = True,
        enabled_trial_routes: tuple[str, ...] = (),
        disabled_trial_routes: tuple[str, ...] = (),
    ) -> bool:
        """Return true for soft setups that deserve a non-executable trial signal."""
        if not allow_trial_buy:
            return False
        if enabled_trial_routes and not _trial_route_enabled(score, enabled_trial_routes):
            return False
        if disabled_trial_routes and _trial_route_disabled(score, disabled_trial_routes):
            return False
        effective_trial_buy = (
            self.trial_buy_threshold if trial_buy_threshold is None else trial_buy_threshold
        )
        effective_trial_entry = (
            self.trial_buy_entry_signal_threshold
            if trial_buy_entry_signal_threshold is None
            else trial_buy_entry_signal_threshold
        )
        score_reaches_trial_line = score.total >= effective_trial_buy
        entry_signal_reaches_trial_line = (
            score.entry_signal
            and score.total >= effective_trial_entry
        )
        watch_route_reaches_trial_line = self._watch_route_reaches_trial_line(
            score,
            trial_buy_entry_signal_threshold=effective_trial_entry,
        )
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

    def _watch_route_reaches_trial_line(
        self,
        score: ScoreResult,
        *,
        trial_buy_entry_signal_threshold: float | None = None,
    ) -> bool:
        threshold = (
            self.trial_buy_entry_signal_threshold
            if trial_buy_entry_signal_threshold is None
            else trial_buy_entry_signal_threshold
        )
        if score.total < threshold:
            return False
        return any(_is_trial_watch_route(route) for route in score.strategy_routes)

    def _regime_rules(self, signal: MarketSignal) -> dict:
        return dict(self.market_regime_overlays.get(signal.value) or {})


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


def _trial_route_disabled(score: ScoreResult, disabled_routes: tuple[str, ...]) -> bool:
    disabled = {str(route) for route in disabled_routes}
    primary = _primary_route_name(score)
    if primary and primary in disabled:
        return True
    active_routes = _active_route_names(score)
    return bool(active_routes) and all(route in disabled for route in active_routes)


def _trial_route_enabled(score: ScoreResult, enabled_routes: tuple[str, ...]) -> bool:
    enabled = {str(route) for route in enabled_routes}
    primary = _primary_route_name(score)
    if primary:
        return primary in enabled
    return any(route in enabled for route in _active_route_names(score))


def _primary_route_name(score: ScoreResult) -> str:
    return str(score.primary_strategy_route or "")


def _active_route_names(score: ScoreResult) -> list[str]:
    return [
        str(_route_value(route, "route", "") or "")
        for route in score.strategy_routes
        if (
            bool(_route_value(route, "entry_signal", False))
            or str(_route_value(route, "status", "") or "") == "watch"
        )
    ]


def build_decider_from_config(cfg: dict) -> Decider:
    """Build Decider from strategy config, including optional buy-side gates."""
    scoring_cfg = cfg.get("scoring", {})
    thresholds = scoring_cfg.get("thresholds", {})
    gates = scoring_cfg.get("decision_gates", {})
    pos_cfg = cfg.get("risk", {}).get("position", {})
    regime_overlays = scoring_cfg.get("market_regime_overlays", {})
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
        market_regime_overlays=regime_overlays,
    )
