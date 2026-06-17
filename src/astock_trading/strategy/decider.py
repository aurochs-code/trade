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
from astock_trading.strategy.route_policy import iter_route_policy_entries


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
        route_execution_policy: dict | None = None,
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
        self.route_execution_policy = route_execution_policy or {}

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

        route_policy_name, route_policy = self._route_policy_for_score(
            market.signal,
            score,
            market,
        )
        route_policy_allows_market = _route_policy_allows_market_block(
            route_policy,
            market.signal,
        )
        if route_policy:
            policy_score_min = route_policy.get("score_min")
            if policy_score_min is not None:
                route_buy_threshold = float(policy_score_min or buy_threshold)
                if route_buy_threshold < buy_threshold:
                    buy_threshold = route_buy_threshold
                    notes.append(f"路线 {route_policy_name} 买入线 {buy_threshold:.1f}")

        # Market signal block
        if market.signal in (MarketSignal.RED, MarketSignal.CLEAR) and not route_policy_allows_market:
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

        if route_policy_allows_market:
            notes.append(f"显式路线策略允许 {market.signal.value} 小仓正式买入")

        # Weekly limit
        if weekly_buy_count >= self.weekly_max:
            notes.append(f"本周已买 {weekly_buy_count}/{self.weekly_max}")

        # Score-based decision
        if score.total >= buy_threshold and weekly_buy_count < self.weekly_max:
            if route_policy and route_policy.get("position_pct") is not None:
                position_pct = min(float(route_policy.get("position_pct") or 0.0), self.single_max_pct)
            else:
                position_pct = self.single_max_pct * market.multiplier
            remaining = max(0, self.total_max_pct - current_exposure_pct)
            position_pct = min(position_pct, remaining)

            require_entry_signal = _route_policy_requires_entry_signal(
                route_policy,
                default=self.require_entry_signal_for_buy,
            )
            buy_blocks = self._buy_block_reasons(
                score,
                position_pct,
                require_entry_signal=require_entry_signal,
                route_policy=route_policy,
            )
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

    def build_buy_funnel(
        self,
        score: ScoreResult,
        market: MarketState,
        *,
        decision: DecisionIntent | None = None,
        current_exposure_pct: float = 0.0,
        weekly_buy_count: int = 0,
    ) -> dict:
        """Build replayable BUY-gate diagnostics without changing the decision."""
        regime = self._regime_rules(market.signal)
        buy_threshold = float(regime.get("buy_threshold", self.buy_threshold))
        watch_threshold = float(regime.get("watch_threshold", self.watch_threshold))
        route_policy_route, route_policy_key, route_policy = self._route_policy_match(
            market.signal,
            score,
            market,
        )
        if route_policy and route_policy.get("score_min") is not None:
            route_buy_threshold = float(route_policy.get("score_min") or buy_threshold)
            if route_buy_threshold < buy_threshold:
                buy_threshold = route_buy_threshold

        if route_policy and route_policy.get("position_pct") is not None:
            raw_position_pct = min(
                float(route_policy.get("position_pct") or 0.0),
                self.single_max_pct,
            )
        else:
            raw_position_pct = self.single_max_pct * float(market.multiplier or 0.0)
        remaining_pct = max(0.0, self.total_max_pct - current_exposure_pct)
        position_pct = min(raw_position_pct, remaining_pct)

        reason_keys: list[str] = []
        gates: dict[str, dict] = {}

        market_blocked = (
            market.signal in (MarketSignal.RED, MarketSignal.CLEAR)
            and not _route_policy_allows_market_block(route_policy, market.signal)
        )
        gates["market_regime"] = {
            "status": "blocked" if market_blocked else "pass",
            "signal": market.signal.value,
            "multiplier": market.multiplier,
            "reason_key": "market_blocks_new_positions" if market_blocked else None,
        }
        if market_blocked:
            reason_keys.append("market_blocks_new_positions")

        hard_veto_reasons = list(score.hard_veto or [])
        gates["hard_veto"] = {
            "status": "blocked" if score.veto_triggered else "pass",
            "reasons": hard_veto_reasons,
        }
        if score.veto_triggered:
            reason_keys.extend(hard_veto_reasons or ["veto"])

        score_status = "pass" if score.total >= buy_threshold else (
            "watch" if score.total >= watch_threshold else "blocked"
        )
        gates["score"] = {
            "status": score_status,
            "score": score.total,
            "buy_threshold": buy_threshold,
            "watch_threshold": watch_threshold,
            "reason_key": None if score.total >= buy_threshold else "score_below_buy_line",
        }
        if score.total < buy_threshold:
            reason_keys.append("score_below_buy_line" if score.total >= watch_threshold else "score_too_low")

        require_entry_signal = _route_policy_requires_entry_signal(
            route_policy,
            default=self.require_entry_signal_for_buy,
        )
        entry_blocked = require_entry_signal and not score.entry_signal
        gates["entry_signal"] = {
            "status": "blocked" if entry_blocked else "pass",
            "required": require_entry_signal,
            "triggered": bool(score.entry_signal),
            "reason_key": "entry_signal_missing" if entry_blocked else None,
        }
        if entry_blocked:
            reason_keys.append("entry_signal_missing")

        min_data_quality_for_buy = _route_policy_min_data_quality(
            route_policy,
            self.min_data_quality_for_buy,
        )
        score_quality = _quality_value(score.data_quality)
        quality_blocked = (
            _quality_rank(score_quality) < _quality_rank(min_data_quality_for_buy)
        )
        gates["data_quality"] = {
            "status": "blocked" if quality_blocked else "pass",
            "value": score_quality,
            "minimum": min_data_quality_for_buy,
            "reason_key": "data_quality_below_min" if quality_blocked else None,
        }
        if quality_blocked:
            reason_keys.append("data_quality_below_min")

        missing = list(score.data_missing_fields or [])
        max_missing_fields_for_buy = _route_policy_max_missing_fields(
            route_policy,
            self.max_missing_fields_for_buy,
        )
        missing_too_many = (
            max_missing_fields_for_buy is not None
            and len(missing) > max_missing_fields_for_buy
        )
        critical_missing = sorted(self.critical_missing_fields_for_buy.intersection(missing))
        gates["missing_fields"] = {
            "status": "blocked" if missing_too_many or critical_missing else "pass",
            "fields": missing,
            "max_allowed": max_missing_fields_for_buy,
            "critical_missing": critical_missing,
        }
        if missing_too_many:
            reason_keys.append("too_many_missing_fields")
        if critical_missing:
            reason_keys.append("critical_missing_fields")

        weekly_blocked = weekly_buy_count >= self.weekly_max
        gates["weekly_limit"] = {
            "status": "blocked" if weekly_blocked else "pass",
            "weekly_buy_count": weekly_buy_count,
            "weekly_max": self.weekly_max,
            "reason_key": "weekly_limit_decision" if weekly_blocked else None,
        }
        if weekly_blocked:
            reason_keys.append("weekly_limit_decision")

        position_blocked = position_pct < self.min_position_pct_for_buy
        gates["position_space"] = {
            "status": "blocked" if position_blocked else "pass",
            "raw_position_pct": raw_position_pct,
            "remaining_pct": remaining_pct,
            "position_pct": position_pct,
            "min_position_pct": self.min_position_pct_for_buy,
            "reason_key": "position_space_insufficient" if position_blocked else None,
        }
        if position_blocked:
            reason_keys.append("position_space_insufficient")

        route_policy_status = "no_route"
        primary_route = _primary_route_name(score)
        active_routes = _active_route_names(score)
        if route_policy:
            route_policy_status = "matched"
        elif primary_route or active_routes:
            route_policy_status = "not_matched"
            reason_keys.append("route_policy_not_matched")
        gates["route_policy"] = {
            "status": route_policy_status,
            "market_signal": market.signal.value,
            "primary_route": primary_route or None,
            "active_routes": active_routes,
            "matched_route": route_policy_route or None,
            "matched_key": route_policy_key or None,
            "policy": route_policy or {},
        }

        action = decision.action.value if decision else None
        status = {
            "BUY": "executable_buy",
            "TRIAL_BUY": "trial_only",
            "WATCH": "watch",
            "CLEAR": "blocked",
        }.get(str(action or ""), "unknown")
        return {
            "version": 1,
            "status": status,
            "action": action,
            "decision_reason_keys": _dedupe(reason_keys),
            "gates": gates,
        }

    def _buy_block_reasons(
        self,
        score: ScoreResult,
        position_pct: float,
        *,
        require_entry_signal: bool | None = None,
        route_policy: dict | None = None,
    ) -> list[str]:
        reasons: list[str] = []

        if position_pct < self.min_position_pct_for_buy:
            reasons.append(
                f"仓位空间不足：建议仓位 {position_pct:.1%} "
                f"< {self.min_position_pct_for_buy:.1%}"
            )

        entry_required = self.require_entry_signal_for_buy if require_entry_signal is None else require_entry_signal
        if entry_required and not score.entry_signal:
            reasons.append("入场信号未触发")

        min_data_quality_for_buy = _route_policy_min_data_quality(
            route_policy,
            self.min_data_quality_for_buy,
        )
        score_quality = _quality_value(score.data_quality)
        if _quality_rank(score_quality) < _quality_rank(min_data_quality_for_buy):
            reasons.append(
                f"数据质量 {score_quality} 低于要求 {min_data_quality_for_buy}"
            )

        missing = list(score.data_missing_fields or [])
        max_missing_fields_for_buy = _route_policy_max_missing_fields(
            route_policy,
            self.max_missing_fields_for_buy,
        )
        if (
            max_missing_fields_for_buy is not None
            and len(missing) > max_missing_fields_for_buy
        ):
            reasons.append(
                f"关键数据缺失过多：{len(missing)} "
                f"> {max_missing_fields_for_buy}"
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

    def _route_policy_for_score(
        self,
        signal: MarketSignal,
        score: ScoreResult,
        market: MarketState | None = None,
    ) -> tuple[str, dict]:
        route, _key, policy = self._route_policy_match(signal, score, market)
        return route, policy

    def _route_policy_match(
        self,
        signal: MarketSignal,
        score: ScoreResult,
        market: MarketState | None = None,
    ) -> tuple[str, str, dict]:
        route_names = []
        primary = _primary_route_name(score)
        if primary:
            route_names.append(primary)
        route_names.extend(route for route in _active_route_names(score) if route and route not in route_names)
        for route in route_names:
            for key, policy in iter_route_policy_entries(self.route_execution_policy, signal.value, route):
                if (
                    _route_policy_allows_action(policy, "BUY")
                    and _route_policy_matches_score(policy, score.total)
                    and _route_policy_matches_phase(policy, market)
                ):
                    return route, key, policy
        return "", "", {}


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


def _route_policy_allows_action(policy: dict, action: str) -> bool:
    actions = policy.get("actions")
    if actions is None:
        return action == "BUY"
    return action in {str(item) for item in actions or () if str(item)}


def _route_policy_matches_score(policy: dict, score_total: float) -> bool:
    try:
        score = float(score_total or 0.0)
    except (TypeError, ValueError):
        score = 0.0
    score_min = policy.get("score_min")
    if score_min is not None and score < float(score_min or 0.0):
        return False
    score_max = policy.get("score_max")
    if score_max is not None and score > float(score_max or 0.0):
        return False
    return True


def _route_policy_matches_phase(policy: dict, market: MarketState | None) -> bool:
    detail = getattr(market, "detail", {}) if market else {}
    if not isinstance(detail, dict):
        detail = {}
    if policy.get("require_above_ma120") is not None:
        if bool(detail.get("above_ma120")) is not bool(policy.get("require_above_ma120")):
            return False
    min_ma120_slope = policy.get("min_index_ma120_slope_20d_pct")
    if min_ma120_slope is not None:
        slope = _float_or_none(detail.get("index_ma120_slope_20d_pct"))
        if slope is None or slope < float(min_ma120_slope):
            return False
    phases = {str(item) for item in policy.get("phase_buckets", []) or [] if str(item)}
    if not phases:
        return True
    phase = _market_phase_bucket_from_detail(detail)
    return phase in phases


def _route_policy_requires_entry_signal(policy: dict, *, default: bool) -> bool:
    if policy and policy.get("require_entry_signal") is not None:
        return bool(policy.get("require_entry_signal"))
    return bool(default)


def _route_policy_min_data_quality(policy: dict | None, default: str | DataQuality) -> str:
    if policy and policy.get("min_data_quality_for_buy") is not None:
        return _quality_value(policy.get("min_data_quality_for_buy"))
    return _quality_value(default)


def _route_policy_max_missing_fields(policy: dict | None, default: int | None) -> int | None:
    if policy and "max_missing_fields_for_buy" in policy:
        value = policy.get("max_missing_fields_for_buy")
        return None if value is None else int(value)
    return default


def _route_policy_allows_market_block(policy: dict, signal: MarketSignal) -> bool:
    if signal != MarketSignal.RED:
        return False
    return bool(policy and policy.get("allow_market_blocked"))


def _market_phase_bucket_from_detail(detail: dict) -> str:
    if not isinstance(detail, dict):
        return "unknown"
    explicit = str(detail.get("market_phase_bucket") or "")
    if explicit:
        return explicit

    deviation = _float_or_none(detail.get("index_ma20_deviation_pct"))
    if deviation is None:
        price = _float_or_none(detail.get("price") or detail.get("index_price"))
        ma20 = _float_or_none(detail.get("ma20") or detail.get("index_ma20"))
        if price is not None and ma20 and ma20 > 0:
            deviation = (price / ma20 - 1) * 100
    slope = _float_or_none(
        detail.get("index_ma20_slope_5d_pct")
        or detail.get("ma20_slope_5d_pct")
        or detail.get("slope_5d_pct")
    )
    if deviation is None or slope is None:
        return "unknown"

    if deviation < -5:
        distance = "deep_below_ma20"
    elif deviation < 0:
        distance = "below_ma20"
    elif deviation <= 3:
        distance = "near_ma20"
    else:
        distance = "extended_above_ma20"

    if slope >= 0.5:
        trend = "slope_up"
    elif slope <= -0.5:
        trend = "slope_down"
    else:
        trend = "slope_flat"
    return f"{distance}_{trend}"


def _float_or_none(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _dedupe(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


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
        route_execution_policy=scoring_cfg.get("route_execution_policy", {}),
    )
