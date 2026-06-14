"""
strategy/scorer.py — 四维评分引擎（纯函数）

不做任何 IO。输入 StockSnapshot，输出 ScoreResult。
回测和实盘共用同一份代码。
"""

from __future__ import annotations

from dataclasses import fields
from typing import Optional

from astock_trading.market.models import StockSnapshot, TechnicalIndicators
from astock_trading.strategy.continuation_filters import ContinuationQualifier
from astock_trading.strategy.continuation_models import ContinuationFilterConfig, ContinuationScoreConfig
from astock_trading.strategy.continuation_scorer import ContinuationScorer
from astock_trading.strategy.models import (
    DataQuality,
    DimensionScore,
    ScoreResult,
    ScoringWeights,
    StrategyRouteDiagnostic,
    StrategyRouteEvidence,
    Style,
)

WARNING_ONLY_SIGNALS = frozenset({"consecutive_outflow_warn"})


class Scorer:
    """四维评分器 — 纯函数，无副作用，无 IO。"""

    def __init__(
        self,
        weights: ScoringWeights,
        veto_rules: list[str],
        entry_cfg: Optional[dict] = None,
        continuation_cfg: Optional[dict] = None,
        score_adjustments: Optional[dict] = None,
    ):
        self.weights = weights
        self.veto_rules = set(veto_rules)
        self.entry_cfg = entry_cfg or {}
        self.score_adjustments = score_adjustments or {}
        continuation_cfg = continuation_cfg or {}
        self.continuation_filter_cfg = _dataclass_from_config(
            ContinuationFilterConfig,
            continuation_cfg.get("filters", {}),
        )
        self.continuation_score_cfg = _dataclass_from_config(
            ContinuationScoreConfig,
            continuation_cfg.get("scoring", {}),
        )

    def score(self, snapshot: StockSnapshot) -> ScoreResult:
        tech = self._score_technical(snapshot)
        fund = self._score_fundamental(snapshot)
        flow = self._score_flow(snapshot)
        sent = self._score_sentiment(snapshot)
        flow = self._apply_score_adjustments(tech, flow)

        w = self.weights
        raw = (
            tech.score * w.technical / tech.max_score
            + fund.score * w.fundamental / fund.max_score
            + flow.score * w.flow / flow.max_score
            + sent.score * w.sentiment / sent.max_score
        )

        veto_signals = self._check_veto(snapshot)
        hard_veto, warnings = split_veto_signals(veto_signals)
        veto_triggered = len(hard_veto) > 0

        if veto_triggered:
            total = 0.0
        else:
            total = round(raw, 1)
            if "consecutive_outflow_warn" in veto_signals:
                total = max(0, round(total - 2.0, 1))

        style, style_conf = self._classify_style(snapshot)
        data_quality, missing = self._assess_quality(snapshot, fund)
        strategy_routes, route_diagnostics = self._detect_strategy_routes(snapshot)
        generic_entry_signal = self._check_entry(snapshot, tech)
        entry_signal = generic_entry_signal or any(
            route.entry_signal for route in strategy_routes
        )
        if not strategy_routes:
            fallback_route = self._fallback_strategy_route(
                snapshot,
                total=total,
                generic_entry_signal=generic_entry_signal,
            )
            if fallback_route is not None:
                strategy_routes = [fallback_route]
                route_diagnostics = [_route_diagnostic(fallback_route), *route_diagnostics]

        return ScoreResult(
            code=snapshot.code,
            name=snapshot.name,
            total=total,
            dimensions=[tech, fund, flow, sent],
            veto_signals=veto_signals,
            hard_veto=hard_veto,
            warning_signals=warnings,
            veto_triggered=veto_triggered,
            entry_signal=entry_signal,
            style=style,
            style_confidence=style_conf,
            data_quality=data_quality,
            data_missing_fields=missing,
            strategy_routes=strategy_routes,
            route_diagnostics=route_diagnostics,
            primary_strategy_route=strategy_routes[0].route if strategy_routes else None,
        )

    def _fallback_strategy_route(
        self,
        s: StockSnapshot,
        *,
        total: float,
        generic_entry_signal: bool,
    ) -> StrategyRouteEvidence | None:
        t = s.technical
        evidence = {
            "score": round(float(total), 2),
        }
        if t is not None:
            evidence.update({
                "above_ma20": t.above_ma20,
                "golden_cross": t.golden_cross,
                "volume_ratio": round(t.volume_ratio, 2),
                "rsi": round(t.rsi, 2),
                "ma20_slope": round(t.ma20_slope, 4),
                "momentum_5d": round(t.momentum_5d, 2),
                "deviation_rate": round(t.deviation_rate, 2),
                "change_pct": round(t.change_pct, 2),
            })
        if t is not None and total >= 6.0:
            pullback_setup_conditions = {
                "above_ma20": t.above_ma20,
                "near_ma20": -2.0 <= t.deviation_rate <= 6.0,
                "rsi_watch_range": 35.0 <= t.rsi <= 72.0,
                "ma20_slope": t.ma20_slope >= 0.005,
                "constructive_momentum": t.momentum_5d >= 0.0,
                "volume_observed": t.volume_ratio > 0,
                "not_chasing": t.change_pct <= 5.0,
            }
            pullback_setup_matched, pullback_setup_missing = _condition_lists(pullback_setup_conditions)
            pullback_setup_score = _route_score(pullback_setup_matched, pullback_setup_conditions)
            if (
                pullback_setup_conditions["above_ma20"]
                and pullback_setup_conditions["near_ma20"]
                and pullback_setup_conditions["rsi_watch_range"]
                and pullback_setup_conditions["ma20_slope"]
                and pullback_setup_conditions["constructive_momentum"]
                and pullback_setup_conditions["volume_observed"]
                and pullback_setup_score >= 0.85
            ):
                return StrategyRouteEvidence(
                    route="pullback_setup_watch",
                    display_name="回踩待确认",
                    family="trend_swing",
                    confidence=0.63,
                    entry_signal=False,
                    status="watch",
                    route_score=pullback_setup_score,
                    matched_conditions=pullback_setup_matched,
                    missing_conditions=pullback_setup_missing,
                    evidence=evidence,
                    notes=[
                        "missing_flow_or_volume_confirmation",
                        "watch_only_wait_for_pullback_confirmation",
                    ],
                )

            trend_structure_conditions = {
                "above_ma20": t.above_ma20,
                "trend_hint": (
                    t.golden_cross
                    or t.ma20_slope >= 0.003
                    or t.momentum_5d >= 2.0
                ),
                "rsi_watch_range": 40.0 <= t.rsi <= 82.0,
                "deviation_watch_range": -2.0 <= t.deviation_rate <= 25.0,
                "volume_observed": t.volume_ratio > 0,
                "not_limit_chase": t.change_pct < 9.9,
            }
            trend_structure_matched, trend_structure_missing = _condition_lists(trend_structure_conditions)
            trend_structure_score = _route_score(trend_structure_matched, trend_structure_conditions)
            if (
                trend_structure_conditions["above_ma20"]
                and trend_structure_conditions["trend_hint"]
                and trend_structure_conditions["rsi_watch_range"]
                and trend_structure_conditions["deviation_watch_range"]
                and trend_structure_conditions["volume_observed"]
            ):
                return StrategyRouteEvidence(
                    route="trend_structure_watch",
                    display_name="趋势结构观察",
                    family="trend_swing",
                    confidence=0.56,
                    entry_signal=False,
                    status="watch",
                    route_score=trend_structure_score,
                    matched_conditions=trend_structure_matched,
                    missing_conditions=trend_structure_missing,
                    evidence=evidence,
                    notes=[
                        "trend_structure_present_but_entry_confirmation_missing",
                        "watch_only_low_priority_route",
                    ],
                )

        if generic_entry_signal:
            return StrategyRouteEvidence(
                route="generic_entry_signal_watch",
                display_name="通用入场观察",
                family="score_watch",
                confidence=0.52,
                entry_signal=False,
                status="watch",
                route_score=1.0,
                matched_conditions=["generic_entry_signal"],
                missing_conditions=["named_strategy_route"],
                evidence=evidence,
                notes=[
                    "generic_entry_signal_without_named_route",
                    "watch_only_until_named_route_confirms",
                ],
            )
        if total >= 6.0:
            return StrategyRouteEvidence(
                route="score_strength_watch",
                display_name="评分支撑观察",
                family="score_watch",
                confidence=0.5,
                entry_signal=False,
                status="watch",
                route_score=1.0,
                matched_conditions=["score_floor"],
                missing_conditions=["technical_route_confirmation"],
                evidence=evidence,
                notes=[
                    "high_score_without_named_entry_route",
                    "watch_only_prevents_unexplained_unknown_route",
                ],
            )
        return None

    def score_batch(self, snapshots: list[StockSnapshot]) -> list[ScoreResult]:
        results = [self.score(s) for s in snapshots]
        results.sort(key=lambda r: r.total, reverse=True)
        return results

    # ------------------------------------------------------------------
    # 技术面 (满分 3)
    # ------------------------------------------------------------------

    def _score_technical(self, s: StockSnapshot) -> DimensionScore:
        t = s.technical
        if t is None:
            return DimensionScore("technical", 0, 3.0, "数据缺失")

        rsi_max = self.entry_cfg.get("rsi_max", 70)
        vol_ratio_min = self.entry_cfg.get("volume_ratio_min", 1.5)

        # 金叉 (1)
        cross_score = 1.0 if t.golden_cross else (0.5 if t.ma10 > t.ma20 > 0 else 0)

        # 量比 (0.5)
        if t.volume_ratio >= vol_ratio_min:
            vol_score = 0.5
        elif t.volume_ratio >= 1.0:
            vol_score = 0.2
        else:
            vol_score = 0

        # RSI (0.5)
        if t.rsi < rsi_max and t.rsi >= 30:
            rsi_score = 0.5
        elif t.rsi < 30:
            rsi_score = 0.3
        else:
            rsi_score = 0

        # 均线排列 (0.5)
        arr_score = 0
        if t.ma5 > 0 and t.ma20 > 0 and t.ma60 > 0:
            if t.ma5 > t.ma20 > t.ma60:
                arr_score = 0.5
            elif t.ma20 > t.ma60:
                arr_score = 0.3

        # 动量 (0.5)
        if t.momentum_5d >= 5:
            mom_score = 0.5
        elif t.momentum_5d >= 2:
            mom_score = 0.3
        elif t.momentum_5d >= 0:
            mom_score = 0.1
        else:
            mom_score = 0

        total = round(min(cross_score + vol_score + rsi_score + arr_score + mom_score, 3.0), 1)
        detail = (
            f"金叉:{cross_score}/1{'✓' if t.golden_cross else ''} "
            f"量比:{vol_score}/0.5({t.volume_ratio:.1f}) "
            f"RSI:{rsi_score}/0.5({t.rsi:.0f}) "
            f"排列:{arr_score}/0.5 动量:{mom_score}/0.5"
        )
        return DimensionScore("technical", total, 3.0, detail, {
            "above_ma20": t.above_ma20,
            "rsi": t.rsi,
            "golden_cross": t.golden_cross,
            "volume_ratio": t.volume_ratio,
            "ma20_slope": t.ma20_slope,
            "momentum_5d": t.momentum_5d,
            "deviation_rate": t.deviation_rate,
            "change_pct": t.change_pct,
        })

    # ------------------------------------------------------------------
    # 基本面 (满分 3)
    # ------------------------------------------------------------------

    def _score_fundamental(self, s: StockSnapshot) -> DimensionScore:
        f = s.financial
        if f is None:
            return DimensionScore(
                "fundamental",
                0,
                3.0,
                "数据缺失",
                {"data_quality": "error", "missing_fields": ["基本面"]},
            )

        missing: list[str] = []
        if f.roe is None:
            missing.append("ROE")
        if f.revenue_growth is None:
            missing.append("营收")
        if f.operating_cash_flow is None:
            missing.append("现金流")

        roe = f.roe or 0
        roe_score = 1.0 if roe >= 15 else (0.7 if roe >= 10 else (0.4 if roe >= 5 else 0))
        roe_trend = None
        roe_trend_score = 0.0
        if f.roe is not None and f.roe_3y_ago is not None:
            roe_trend = round(f.roe - f.roe_3y_ago, 2)
            if roe_trend >= 6:
                roe_trend_score = 0.4
            elif roe_trend >= 3:
                roe_trend_score = 0.2

        rev = f.revenue_growth or 0
        rev_score = 1.0 if rev >= 20 else (0.7 if rev >= 10 else (0.3 if rev >= 0 else 0))

        cf_score = 0.5 if (f.operating_cash_flow or 0) > 0 else 0

        total = round(min(roe_score + roe_trend_score + rev_score + cf_score, 3.0), 1)
        detail = f"ROE:{roe_score:.1f}/1 营收:{rev_score:.1f}/1 现金流:{cf_score:.1f}/1"
        if roe_trend_score:
            detail += f" ROE改善:{roe_trend_score:.1f}/0.4({roe_trend:+.1f}pct)"
        if missing:
            detail += f" ⚠️缺失:{','.join(missing)}"

        dq = "ok" if not missing else "degraded"
        return DimensionScore("fundamental", total, 3.0, detail, {
            "data_quality": dq, "missing_fields": missing, "roe_trend": roe_trend,
        })

    # ------------------------------------------------------------------
    # 资金流 (满分 2)
    # ------------------------------------------------------------------

    def _score_flow(self, s: StockSnapshot) -> DimensionScore:
        fl = s.flow
        if fl is None:
            return DimensionScore("flow", 0, 2.0, "数据缺失")

        main_net = fl.net_inflow_1d or 0
        if main_net > 1e9:
            main_score = 1.0
        elif main_net > 5e8:
            main_score = 0.7
        elif main_net > 0:
            main_score = 0.4
        else:
            main_score = 0

        north_score = 1.0 if fl.northbound_net_positive else 0.5

        total = round(min(main_score + north_score, 2.0), 1)
        detail = f"主力:{main_score}/1.0 北向:{north_score}/1.0"
        return DimensionScore("flow", total, 2.0, detail, {
            "main_net_inflow": main_net,
        })

    def _apply_score_adjustments(
        self,
        tech: DimensionScore,
        flow: DimensionScore,
    ) -> DimensionScore:
        cfg = self.score_adjustments.get("tech_flow_correlation") or {}
        if not cfg.get("enabled", False):
            return flow
        tech_threshold = float(cfg.get("tech_threshold", 2.5))
        flow_threshold = float(cfg.get("flow_threshold", 1.5))
        if tech.score < tech_threshold or flow.score < flow_threshold:
            return flow
        discount = float(cfg.get("flow_discount", 0.7))
        adjusted = round(flow.score * discount, 1)
        return DimensionScore(
            flow.name,
            adjusted,
            flow.max_score,
            f"{flow.detail} 重复动量折扣:{discount:.0%}",
            {
                **flow.raw_data,
                "pre_correlation_discount_score": flow.score,
                "correlation_discount": discount,
            },
        )

    # ------------------------------------------------------------------
    # 舆情 (满分 3)
    # ------------------------------------------------------------------

    def _score_sentiment(self, s: StockSnapshot) -> DimensionScore:
        se = s.sentiment
        if se is None:
            return DimensionScore("sentiment", 1.5, 3.0, "无数据，默认1.5")

        total = round(max(0, min(se.score, 3.0)), 1)
        detail = se.detail or f"舆情评分:{total}"
        return DimensionScore("sentiment", total, 3.0, detail)

    # ------------------------------------------------------------------
    # 一票否决
    # ------------------------------------------------------------------

    def _check_veto(self, s: StockSnapshot) -> list[str]:
        signals: list[str] = []
        t = s.technical

        if t and "below_ma20" in self.veto_rules and not t.above_ma20:
            signals.append("below_ma20")

        if t and "limit_up_today" in self.veto_rules:
            # 涨跌停判断：科创板(688)为20%，其他为10%
            threshold = 19.9 if s.code.startswith("688") else 9.9
            if abs(t.change_pct) >= threshold:
                signals.append("limit_up_today")

        if s.flow and "consecutive_outflow" in self.veto_rules:
            if s.flow.consecutive_outflow_days >= 3:
                if t and t.above_ma20 and (s.quote and s.quote.amount > 5e8):
                    signals.append("consecutive_outflow_warn")
                else:
                    signals.append("consecutive_outflow")

        if "ma20_trend_down" in self.veto_rules and t:
            if t.ma20_slope < -0.02 and not t.above_ma20:
                signals.append("ma20_trend_down")

        return signals

    # ------------------------------------------------------------------
    # 入场信号
    # ------------------------------------------------------------------

    def _check_entry(self, s: StockSnapshot, tech_dim: DimensionScore) -> bool:
        t = s.technical
        if not t:
            return False
        rsi_max = self.entry_cfg.get("rsi_max", 70)
        vol_min = self.entry_cfg.get("volume_ratio_min", 1.5)
        return t.golden_cross and t.volume_ratio >= vol_min and t.rsi < rsi_max

    # ------------------------------------------------------------------
    # 策略路线证据
    # ------------------------------------------------------------------

    def _detect_strategy_routes(
        self,
        s: StockSnapshot,
    ) -> tuple[list[StrategyRouteEvidence], list[StrategyRouteDiagnostic]]:
        """Map deterministic indicator patterns to reusable strategy routes."""
        t = s.technical
        if not t:
            return [], []

        routes: list[StrategyRouteEvidence] = []
        diagnostics: list[StrategyRouteDiagnostic] = []
        rsi_max = float(self.entry_cfg.get("rsi_max", 70))
        close_near_high = _close_near_high(s)
        liquidity_amount = float(s.quote.amount if s.quote else 0.0)
        continuation_filter = ContinuationQualifier(self.continuation_filter_cfg).qualify(s)
        continuation_score = ContinuationScorer(self.continuation_score_cfg).score(
            s,
            continuation_filter,
        )

        if continuation_score.qualified and continuation_score.total_score >= 2.5:
            routes.append(StrategyRouteEvidence(
                route="short_continuation",
                display_name="短续接力",
                family="short_continuation",
                confidence=min(0.9, 0.7 + continuation_score.total_score / 20.0),
                entry_signal=True,
                evidence={
                    **continuation_score.component_breakdown,
                    "continuation_score": continuation_score.total_score,
                    "close_near_high": continuation_filter.close_near_high,
                    "intraday_retrace": continuation_filter.intraday_retrace,
                },
                notes=[
                    "continuation_qualifier_passed",
                    *continuation_score.notes,
                ],
            ))

        flow_net = float(s.flow.net_inflow_1d or 0.0) if s.flow else 0.0
        flow_amount_ratio = flow_net / liquidity_amount if liquidity_amount > 0 else 0.0
        volume_confirm_min = float(self.entry_cfg.get("volume_ratio_min", 1.5))
        flow_conditions = {
            "recent_golden_cross": _recent_golden_cross(t),
            "above_ma20": t.above_ma20,
            "relative_volume_pullback": 0.8 <= t.volume_ratio < volume_confirm_min,
            "rsi_range": 30 <= t.rsi < rsi_max,
            "ma20_slope": t.ma20_slope >= 0.01,
            "momentum_5d": t.momentum_5d >= 5.0,
            "deviation_risk": t.deviation_rate <= 10.0,
            "change_pct_risk": t.change_pct < 8.0,
            "liquidity": liquidity_amount >= 5e8,
            "flow_strength": _flow_strength_confirmed(flow_net, liquidity_amount),
        }
        flow_matched, flow_missing = _condition_lists(flow_conditions)
        flow_route_score = _route_score(flow_matched, flow_conditions)
        flow_critical_ok = all(
            flow_conditions[name]
            for name in (
                "recent_golden_cross",
                "above_ma20",
                "relative_volume_pullback",
                "ma20_slope",
                "flow_strength",
            )
        )
        flow_entry_signal = flow_critical_ok and not flow_missing
        flow_watch_signal = flow_critical_ok and len(flow_missing) <= 1 and flow_route_score >= 0.6
        if flow_entry_signal or flow_watch_signal:
            routes.append(StrategyRouteEvidence(
                route="flow_confirmed_trend",
                display_name="资金趋势确认",
                family="trend_swing",
                confidence=0.88 if flow_entry_signal else 0.66,
                entry_signal=flow_entry_signal,
                status="entry" if flow_entry_signal else "watch",
                route_score=flow_route_score,
                matched_conditions=flow_matched,
                missing_conditions=flow_missing,
                evidence={
                    "golden_cross": t.golden_cross,
                    "recent_golden_cross": flow_conditions["recent_golden_cross"],
                    "volume_ratio": round(t.volume_ratio, 2),
                    "volume_ratio_required": round(volume_confirm_min, 2),
                    "main_net_inflow": round(flow_net, 2),
                    "flow_amount_ratio": round(flow_amount_ratio, 4),
                    "amount": round(liquidity_amount, 2),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "rsi": round(t.rsi, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                    "deviation_rate": round(t.deviation_rate, 2),
                    "change_pct": round(t.change_pct, 2),
                },
                notes=[
                    "relative_volume_below_entry_min_but_absolute_flow_confirms",
                    "shadow_and_paper_execution_still_require_pool_and_profile_gates",
                ],
            ))

        if (
            t.above_ma20
            and t.volume_ratio >= 2.0
            and t.momentum_5d >= 3.0
            and 0 <= t.deviation_rate <= 7.0
            and t.rsi < max(rsi_max, 72)
            and close_near_high
        ):
            routes.append(StrategyRouteEvidence(
                route="volume_breakout",
                display_name="放量突破",
                family="short_continuation",
                confidence=0.96,
                entry_signal=t.golden_cross and t.rsi < rsi_max,
                evidence={
                    "volume_ratio": round(t.volume_ratio, 2),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "deviation_rate": round(t.deviation_rate, 2),
                    "close_near_high": close_near_high,
                    "above_ma20": t.above_ma20,
                },
                notes=["borrowed_from_daily_stock_analysis:volume_breakout"],
            ))

        pullback_volume_max = float(self.entry_cfg.get("pullback_volume_ratio_max", 1.6))
        pullback_rsi_max = min(rsi_max, float(self.entry_cfg.get("pullback_rsi_max", 68)))
        if (
            s.flow is not None
            and t.above_ma20
            and t.ma5 >= t.ma20 > t.ma60 > 0
            and 0.8 <= t.volume_ratio <= pullback_volume_max
            and 40 <= t.rsi <= pullback_rsi_max
            and -1.0 <= t.deviation_rate <= 4.5
            and t.ma20_slope >= 0.005
            and t.momentum_5d >= 0.0
            and -0.5 <= t.change_pct <= 4.0
            and liquidity_amount >= 3e8
            and flow_net >= 0
        ):
            routes.append(StrategyRouteEvidence(
                route="pullback_to_ma20",
                display_name="均线回踩转强",
                family="trend_swing",
                confidence=0.91,
                entry_signal=True,
                evidence={
                    "volume_ratio": round(t.volume_ratio, 2),
                    "volume_ratio_max": round(pullback_volume_max, 2),
                    "rsi": round(t.rsi, 2),
                    "deviation_rate": round(t.deviation_rate, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "change_pct": round(t.change_pct, 2),
                    "amount": round(liquidity_amount, 2),
                    "main_net_inflow": round(flow_net, 2),
                    "ma_relation": "ma5>=ma20>ma60",
                },
                notes=[
                    "trend_pullback_recovered_near_ma20",
                    "paper_execution_still_requires_core_buy_window_and_risk_gates",
                ],
            ))

        overheat_conditions = {
            "above_ma20": t.above_ma20,
            "ma20_slope": t.ma20_slope >= 0.005,
            "momentum_5d": t.momentum_5d >= 5.0,
            "overheat_zone": (
                t.rsi > rsi_max
                or t.deviation_rate > 8.0
                or t.change_pct >= 6.0
            ),
            "not_limit_up_chase": t.change_pct < 9.0,
            "liquidity": liquidity_amount >= 3e8,
        }
        overheat_matched, overheat_missing = _condition_lists(overheat_conditions)
        overheat_score = _route_score(overheat_matched, overheat_conditions)
        if all(overheat_conditions.values()):
            routes.append(StrategyRouteEvidence(
                route="relative_strength_overheat",
                display_name="强势过热观察",
                family="trend_swing",
                confidence=0.68,
                entry_signal=False,
                status="watch",
                route_score=overheat_score,
                matched_conditions=overheat_matched,
                missing_conditions=overheat_missing,
                evidence={
                    "volume_ratio": round(t.volume_ratio, 2),
                    "rsi": round(t.rsi, 2),
                    "rsi_max": round(rsi_max, 2),
                    "deviation_rate": round(t.deviation_rate, 2),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                    "change_pct": round(t.change_pct, 2),
                    "amount": round(liquidity_amount, 2),
                },
                notes=[
                    "relative_strength_survived_overheat_filters",
                    "watch_only_until_market_regime_execution_backtest_passes",
                ],
            ))

        if (
            t.above_ma20
            and t.ma5 >= t.ma10 >= t.ma20 > 0
            and t.ma20 > t.ma60 > 0
            and 0 < t.volume_ratio <= 1.2
            and 40 <= t.rsi <= 65
            and -1.5 <= t.deviation_rate <= 3.0
            and t.ma20_slope >= 0.003
        ):
            routes.append(StrategyRouteEvidence(
                route="shrink_pullback",
                display_name="缩量回踩",
                family="trend_swing",
                confidence=0.84,
                entry_signal=t.rsi < rsi_max,
                evidence={
                    "volume_ratio": round(t.volume_ratio, 2),
                    "rsi": round(t.rsi, 2),
                    "deviation_rate": round(t.deviation_rate, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                    "ma_order": "ma5>=ma10>=ma20>ma60",
                },
                notes=["borrowed_from_daily_stock_analysis:shrink_pullback"],
            ))

        cooling_conditions = {
            "above_ma20": t.above_ma20,
            "trend_structure": _recent_golden_cross(t),
            "quiet_volume": 0.5 <= t.volume_ratio <= 1.2,
            "rsi_cooling_zone": 50 <= t.rsi <= rsi_max + 5,
            "deviation_watch_zone": 2.0 <= t.deviation_rate <= 12.0,
            "ma20_slope": t.ma20_slope >= 0,
            "flow_not_out": s.flow is not None and flow_net >= 0,
        }
        cooling_matched, cooling_missing = _condition_lists(cooling_conditions)
        cooling_score = _route_score(cooling_matched, cooling_conditions)
        if (
            cooling_conditions["above_ma20"]
            and cooling_conditions["trend_structure"]
            and cooling_conditions["quiet_volume"]
            and cooling_conditions["rsi_cooling_zone"]
            and cooling_conditions["deviation_watch_zone"]
            and cooling_conditions["flow_not_out"]
        ):
            routes.append(StrategyRouteEvidence(
                route="trend_cooling_off",
                display_name="趋势冷却观察",
                family="trend_swing",
                confidence=0.7,
                entry_signal=False,
                status="watch",
                route_score=cooling_score,
                matched_conditions=cooling_matched,
                missing_conditions=cooling_missing,
                evidence={
                    "golden_cross": t.golden_cross,
                    "volume_ratio": round(t.volume_ratio, 2),
                    "rsi": round(t.rsi, 2),
                    "rsi_max": round(rsi_max, 2),
                    "deviation_rate": round(t.deviation_rate, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "main_net_inflow": round(flow_net, 2),
                },
                notes=[
                    "rsi_cooling_watch",
                    "watch_only_wait_for_pullback_or_fresh_entry",
                ],
            ))

        if (
            t.golden_cross
            and t.above_ma20
            and t.volume_ratio >= 1.2
            and 30 <= t.rsi < rsi_max
            and t.ma20_slope >= 0
        ):
            routes.append(StrategyRouteEvidence(
                route="ma_golden_cross",
                display_name="均线金叉",
                family="trend_swing",
                confidence=0.78,
                entry_signal=t.volume_ratio >= float(self.entry_cfg.get("volume_ratio_min", 1.5)),
                evidence={
                    "golden_cross": t.golden_cross,
                    "volume_ratio": round(t.volume_ratio, 2),
                    "rsi": round(t.rsi, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                },
                notes=["borrowed_from_daily_stock_analysis:ma_golden_cross"],
            ))

        volume_missing = t.volume_ratio <= 0
        if (
            volume_missing
            and t.above_ma20
            and t.ma20 > t.ma60 > 0
            and (t.golden_cross or t.ma5 >= t.ma10 >= t.ma20)
            and t.ma20_slope >= 0.01
            and t.momentum_5d >= 3.0
            and 40 <= t.rsi < rsi_max
            and t.deviation_rate <= 10.0
        ):
            routes.append(StrategyRouteEvidence(
                route="trend_watch",
                display_name="趋势观察",
                family="trend_swing",
                confidence=0.62,
                entry_signal=False,
                status="watch",
                route_score=0.88,
                matched_conditions=[
                    "above_ma20",
                    "ma20_above_ma60",
                    "trend_structure",
                    "ma20_slope",
                    "momentum_5d",
                    "rsi_range",
                    "deviation_risk",
                ],
                missing_conditions=["volume_ratio"],
                evidence={
                    "golden_cross": t.golden_cross,
                    "volume_ratio": round(t.volume_ratio, 2),
                    "rsi": round(t.rsi, 2),
                    "ma20_slope": round(t.ma20_slope, 4),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "deviation_rate": round(t.deviation_rate, 2),
                },
                notes=[
                    "volume_ratio_missing_blocks_entry",
                    "shadow_watch_only",
                ],
            ))

        dragon_conditions = {
            "above_ma20": t.above_ma20,
            "liquidity": liquidity_amount >= 5e8,
            "volume_ratio": t.volume_ratio >= 1.5,
            "momentum_5d": t.momentum_5d >= 5.0,
            "change_pct": t.change_pct >= 3.0,
            "deviation_risk": t.deviation_rate <= 10.0,
            "sector_strength": bool(s.sector and s.sector.confirmed),
        }
        dragon_matched, dragon_missing = _condition_lists(dragon_conditions)
        dragon_shape_ok = all(
            dragon_conditions[name]
            for name in (
                "above_ma20",
                "liquidity",
                "volume_ratio",
                "momentum_5d",
                "change_pct",
                "deviation_risk",
            )
        )
        if dragon_shape_ok:
            sector = s.sector
            sector_confirmed = bool(sector and sector.confirmed)
            notes = ["borrowed_from_daily_stock_analysis:dragon_head"]
            if sector is None:
                notes.append("sector_data_missing_downgrades_route")
            elif not sector_confirmed:
                notes.append("requires_sector_strength_confirmation")
            dragon_status = "entry" if sector_confirmed else ("watch" if sector is None else "blocked")
            dragon_confidence = 0.9 if sector_confirmed else (0.6 if sector is None else 0.0)
            dragon_entry_signal = sector_confirmed
            dragon_route = StrategyRouteEvidence(
                route="dragon_head",
                display_name="龙头策略",
                family="sector_momentum",
                confidence=dragon_confidence,
                entry_signal=dragon_entry_signal,
                status=dragon_status,
                route_score=_route_score(dragon_matched, dragon_conditions),
                matched_conditions=dragon_matched,
                missing_conditions=dragon_missing,
                evidence={
                    "amount": round(liquidity_amount, 2),
                    "volume_ratio": round(t.volume_ratio, 2),
                    "momentum_5d": round(t.momentum_5d, 2),
                    "change_pct": round(t.change_pct, 2),
                    "sector_confirmation": "confirmed" if sector_confirmed else "unavailable",
                    "industry_name": sector.industry_name if sector else "",
                    "industry_rank": sector.industry_rank if sector else None,
                    "industry_change_pct": sector.industry_change_pct if sector else 0.0,
                    "leader": sector.leader if sector else "",
                    "relative_strength_pct": sector.relative_strength_pct if sector else 0.0,
                },
                notes=notes,
            )
            if dragon_status != "blocked":
                routes.append(dragon_route)
            else:
                diagnostics.append(_route_diagnostic(dragon_route))

        routes.sort(key=lambda route: route.confidence, reverse=True)
        return routes, [_route_diagnostic(route) for route in routes] + diagnostics

    # ------------------------------------------------------------------
    # 风格判定
    # ------------------------------------------------------------------

    def _classify_style(self, s: StockSnapshot) -> tuple[Style, float]:
        t = s.technical
        if not t:
            return Style.UNKNOWN, 0.0

        sb_score = 0
        mm_score = 0

        if t.daily_volatility <= 0.02:
            sb_score += 1
        if t.daily_volatility >= 0.03:
            mm_score += 1

        if 50 <= t.rsi <= 65:
            sb_score += 1
        if t.rsi >= 75:
            mm_score += 1

        if t.ma20_slope >= 0.005:
            sb_score += 1
        if t.ma20_slope >= 0.02:
            mm_score += 1

        if sb_score >= 2 and sb_score > mm_score:
            return Style.SLOW_BULL, round(sb_score / 3, 2)
        elif mm_score >= 2:
            return Style.MOMENTUM, round(mm_score / 3, 2)
        elif t.daily_volatility >= 0.03:
            return Style.MOMENTUM, 0.5
        else:
            return Style.SLOW_BULL, 0.5

    # ------------------------------------------------------------------
    # 数据质量
    # ------------------------------------------------------------------

    def _assess_quality(
        self, s: StockSnapshot, fund_dim: DimensionScore
    ) -> tuple[DataQuality, list[str]]:
        dq = fund_dim.raw_data.get("data_quality", "ok")
        missing = list(fund_dim.raw_data.get("missing_fields", []))
        if s.quote is None:
            missing.append("行情")
        if s.technical is None:
            missing.append("技术指标")
        if s.flow is None:
            missing.append("资金流")
        if dq == "error":
            return DataQuality.ERROR, missing
        if dq == "degraded" or missing:
            return DataQuality.DEGRADED, missing
        return DataQuality.OK, []


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def split_veto_signals(signals: list[str]) -> tuple[list[str], list[str]]:
    hard = [s for s in signals if s not in WARNING_ONLY_SIGNALS]
    warn = [s for s in signals if s in WARNING_ONLY_SIGNALS]
    return hard, warn


def _close_near_high(s: StockSnapshot) -> bool:
    q = s.quote
    if not q or q.high <= 0:
        return False
    return q.close >= q.high * 0.98


def _recent_golden_cross(t: TechnicalIndicators) -> bool:
    return bool(t.golden_cross or (t.ma5 >= t.ma10 >= t.ma20 > 0))


def _flow_strength_confirmed(flow_net: float, amount: float) -> bool:
    if flow_net >= 3e8:
        return True
    if amount <= 0:
        return False
    return flow_net >= 1e8 and flow_net / amount >= 0.05


def _condition_lists(conditions: dict[str, bool]) -> tuple[list[str], list[str]]:
    matched = [name for name, passed in conditions.items() if passed]
    missing = [name for name, passed in conditions.items() if not passed]
    return matched, missing


def _route_score(matched_conditions: list[str], conditions: dict[str, bool]) -> float:
    if not conditions:
        return 0.0
    return round(len(matched_conditions) / len(conditions), 2)


def _route_diagnostic(route: StrategyRouteEvidence) -> StrategyRouteDiagnostic:
    return StrategyRouteDiagnostic(
        route=route.route,
        display_name=route.display_name,
        family=route.family,
        status=route.status,
        route_score=route.route_score,
        matched_conditions=list(route.matched_conditions),
        missing_conditions=list(route.missing_conditions),
        entry_signal=route.entry_signal,
        confidence=route.confidence,
        notes=list(route.notes),
    )


def _dataclass_from_config(cls, values: dict | None):
    allowed = {field.name for field in fields(cls)}
    filtered = {
        key: value
        for key, value in (values or {}).items()
        if key in allowed
    }
    return cls(**filtered)
