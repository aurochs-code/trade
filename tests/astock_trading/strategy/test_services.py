"""Tests for strategy/service.py and risk/service.py — service layer with event_log"""

import pytest
from dataclasses import replace
from datetime import date

from astock_trading.platform.events import EventStore
from astock_trading.market.models import (
    FinancialReport,
    FundFlow,
    SentimentData,
    StockQuote,
    StockSnapshot,
    TechnicalIndicators,
)
from astock_trading.strategy.models import (
    Action,
    DataQuality,
    DimensionScore,
    MarketSignal,
    MarketState,
    ScoreResult,
    ScoringWeights,
    StrategyRouteEvidence,
)
from astock_trading.strategy.scorer import Scorer
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.service import StrategyService
from astock_trading.risk.models import PortfolioLimits, RiskParams
from astock_trading.risk.service import RiskService
from astock_trading.strategy.models import Style


@pytest.fixture
def db(mysql_conn):
    yield mysql_conn


@pytest.fixture
def event_store(db):
    return EventStore(db)


def _make_snapshot(code="002138", name="双环传动") -> StockSnapshot:
    return StockSnapshot(
        code=code, name=name,
        quote=StockQuote(
            code=code, name=name, price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=1.5,
        ),
        technical=TechnicalIndicators(
            ma5=15.0, ma10=14.5, ma20=14.0, ma60=13.0,
            above_ma20=True, volume_ratio=1.8, rsi=55.0,
            golden_cross=True, ma20_slope=0.01,
            momentum_5d=3.0, daily_volatility=0.025,
        ),
        financial=FinancialReport(roe=12.0, revenue_growth=15.0, operating_cash_flow=1e8),
        flow=FundFlow(net_inflow_1d=6e8, northbound_net_positive=True),
        sentiment=SentimentData(score=2.0, detail="研报3篇"),
    )


class TestStrategyService:
    def test_evaluate_writes_events(self, event_store):
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=["below_ma20"],
        )
        decider = Decider(buy_threshold=6.5, watch_threshold=5.0)
        svc = StrategyService(scorer, decider, event_store)

        snapshots = [_make_snapshot("001", "股票A"), _make_snapshot("002", "股票B")]
        market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

        decisions = svc.evaluate(
            snapshots, market,
            run_id="run_test_001", config_version="v_test",
        )

        assert len(decisions) == 2

        # 验证 score.calculated 事件写入
        score_events = event_store.query(event_type="score.calculated")
        assert len(score_events) == 2

        # 验证 decision.suggested 事件写入
        decision_events = event_store.query(event_type="decision.suggested")
        assert len(decision_events) == 2

        # 验证 metadata
        for ev in score_events:
            assert ev["metadata"]["run_id"] == "run_test_001"
            assert ev["metadata"]["config_version"] == "v_test"

    def test_evaluate_persists_raw_scoring_and_decision_evidence(self, event_store):
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=[],
        )
        decider = Decider(buy_threshold=6.5, watch_threshold=5.0)
        svc = StrategyService(scorer, decider, event_store)

        market = MarketState(
            signal=MarketSignal.GREEN,
            multiplier=0.5,
            detail={"indices": {"沪深300": {"signal": "GREEN", "change_pct": 1.2}}},
        )
        snapshot = replace(
            _make_snapshot("002138", "双环传动"),
            observation_id="obs_snapshot_1",
        )
        svc.evaluate(
            [snapshot],
            market,
            run_id="run_evidence",
            config_version="v_test",
            current_exposure_pct=0.12,
            weekly_buy_count=1,
        )

        score_event = event_store.query(event_type="score.calculated")[0]
        dimensions = {
            item["name"]: item for item in score_event["payload"]["dimensions"]
        }
        assert dimensions["technical"]["raw_data"]["rsi"] == 55.0
        assert dimensions["technical"]["raw_data"]["volume_ratio"] == 1.8
        assert dimensions["flow"]["raw_data"]["main_net_inflow"] == 6e8
        assert score_event["payload"]["source_observation_id"] == "obs_snapshot_1"

        decision_event = event_store.query(event_type="decision.suggested")[0]
        decision_payload = decision_event["payload"]
        assert decision_payload["source_score_event_id"] == score_event["event_id"]
        assert decision_payload["decision_inputs"] == {
            "current_exposure_pct": 0.12,
            "weekly_buy_count": 1,
        }
        assert decision_payload["market_state"] == {
            "signal": "GREEN",
            "multiplier": 0.5,
            "detail": market.detail,
        }
        assert decision_payload["decision_rules"]["buy_threshold"] == 6.5
        assert decision_payload["decision_rules"]["watch_threshold"] == 5.0

        manual_event = event_store.query(event_type="manual_trade.requested")[0]
        assert manual_event["payload"]["source_event_id"] == decision_event["event_id"]
        assert manual_event["payload"]["source_score_event_id"] == score_event["event_id"]

    def test_buy_decision_persists_entry_route_evidence(self, event_store):
        score = ScoreResult(
            code="002384",
            name="东山精密",
            total=7.0,
            dimensions=[
                DimensionScore("technical", 2.5, 3.0, "金叉成立，资金确认"),
                DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
                DimensionScore("flow", 1.5, 2.0, "资金较强"),
                DimensionScore("sentiment", 1.8, 2.0, "偏强"),
            ],
            entry_signal=True,
            style=Style.MOMENTUM,
            style_confidence=0.88,
            data_quality=DataQuality.OK,
            strategy_routes=[
                StrategyRouteEvidence(
                    route="flow_confirmed_trend",
                    display_name="资金趋势确认",
                    family="trend_swing",
                    confidence=0.88,
                    evidence={"main_net_inflow": 3330975094.0},
                    entry_signal=True,
                )
            ],
            primary_strategy_route="flow_confirmed_trend",
        )

        class StaticScorer:
            def score_batch(self, snapshots):
                return [score]

        decider = Decider(buy_threshold=6.5, watch_threshold=5.0, require_entry_signal_for_buy=True)
        svc = StrategyService(StaticScorer(), decider, event_store)

        svc.evaluate(
            [_make_snapshot("002384", "东山精密")],
            MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
            run_id="run_entry_route_evidence",
            config_version="v_test",
        )

        decision_payload = event_store.query(event_type="decision.suggested")[0]["payload"]
        assert decision_payload["action"] == "BUY"
        assert decision_payload["entry_signal"] is True
        assert decision_payload["primary_strategy_route"] == "flow_confirmed_trend"
        assert decision_payload["primary_strategy_route_label"] == "资金趋势确认"
        assert decision_payload["strategy_routes"][0]["display_name"] == "资金趋势确认"
        assert decision_payload["technical_detail"] == "金叉成立，资金确认"
        assert decision_payload["data_quality"] == "ok"

        manual_payload = event_store.query(event_type="manual_trade.requested")[0]["payload"]
        assert manual_payload["entry_signal"] is True
        assert manual_payload["primary_strategy_route"] == "flow_confirmed_trend"
        assert manual_payload["primary_strategy_route_label"] == "资金趋势确认"

    def test_decision_persists_structured_buy_funnel(self, event_store):
        score = ScoreResult(
            code="002384",
            name="东山精密",
            total=6.8,
            dimensions=[
                DimensionScore("technical", 2.4, 3.0, "趋势偏强"),
                DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
                DimensionScore("flow", 1.6, 2.0, "资金较强"),
                DimensionScore("sentiment", 1.6, 2.0, "情绪偏强"),
            ],
            entry_signal=False,
            style=Style.MOMENTUM,
            style_confidence=0.8,
            data_quality=DataQuality.OK,
            strategy_routes=[
                StrategyRouteEvidence(
                    route="trend_cooling_off",
                    display_name="趋势冷却观察",
                    family="trend_swing",
                    confidence=0.7,
                    entry_signal=False,
                    status="watch",
                    route_score=0.7,
                )
            ],
            primary_strategy_route="trend_cooling_off",
        )

        class StaticScorer:
            def score_batch(self, snapshots):
                return [score]

        decider = Decider(
            buy_threshold=6.5,
            watch_threshold=5.0,
            require_entry_signal_for_buy=True,
            min_data_quality_for_buy="ok",
            route_execution_policy={
                "GREEN:short_continuation": {
                    "score_min": 6.0,
                    "position_pct": 0.22,
                    "priority": 70,
                }
            },
        )
        svc = StrategyService(StaticScorer(), decider, event_store)

        svc.evaluate(
            [_make_snapshot("002384", "东山精密")],
            MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
            run_id="run_buy_funnel",
            config_version="v_test",
        )

        decision_payload = event_store.query(event_type="decision.suggested")[0]["payload"]
        funnel = decision_payload["buy_funnel"]
        assert funnel["status"] == "trial_only"
        assert funnel["decision_reason_keys"] == ["entry_signal_missing", "route_policy_not_matched"]
        assert funnel["gates"]["entry_signal"] == {
            "status": "blocked",
            "required": True,
            "triggered": False,
            "reason_key": "entry_signal_missing",
        }
        assert funnel["gates"]["route_policy"]["status"] == "not_matched"
        assert funnel["gates"]["route_policy"]["primary_route"] == "trend_cooling_off"
        assert funnel["gates"]["market_regime"]["status"] == "pass"
        assert event_store.query(event_type="manual_trade.requested") == []

    def test_trial_buy_decision_does_not_request_manual_trade(self, event_store):
        score = ScoreResult(
            code="002384",
            name="东山精密",
            total=6.2,
            dimensions=[
                DimensionScore("technical", 2.0, 3.0, "技术偏强"),
                DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
                DimensionScore("flow", 1.5, 2.0, "资金较强"),
                DimensionScore("sentiment", 1.5, 2.0, "情绪中性"),
            ],
            entry_signal=False,
            style=Style.MOMENTUM,
            style_confidence=0.8,
            data_quality=DataQuality.OK,
        )

        class StaticScorer:
            def score_batch(self, snapshots):
                return [score]

        decider = Decider(
            buy_threshold=6.0,
            watch_threshold=5.0,
            require_entry_signal_for_buy=True,
        )
        svc = StrategyService(StaticScorer(), decider, event_store)

        decisions = svc.evaluate(
            [_make_snapshot("002384", "东山精密")],
            MarketState(signal=MarketSignal.RED, multiplier=0.0),
            run_id="run_trial_buy",
            config_version="v_test",
        )

        assert decisions[0].action == Action.TRIAL_BUY
        decision_payload = event_store.query(event_type="decision.suggested")[0]["payload"]
        assert decision_payload["action"] == "TRIAL_BUY"
        assert decision_payload["position_pct"] == 0.0
        assert event_store.query(event_type="manual_trade.requested") == []

    def test_recent_false_breakouts_cool_entry_signal_to_watch(self, event_store):
        score = ScoreResult(
            code="002384",
            name="东山精密",
            total=7.1,
            dimensions=[
                DimensionScore("technical", 2.6, 3.0, "趋势成立"),
                DimensionScore("fundamental", 1.2, 3.0, "基本面可用"),
                DimensionScore("flow", 1.5, 2.0, "资金较强"),
                DimensionScore("sentiment", 1.5, 2.0, "情绪中性"),
            ],
            entry_signal=True,
            style=Style.MOMENTUM,
            style_confidence=0.8,
            data_quality=DataQuality.OK,
            strategy_routes=[
                StrategyRouteEvidence(
                    route="ma_golden_cross",
                    display_name="均线金叉",
                    family="trend_swing",
                    confidence=0.8,
                    entry_signal=True,
                )
            ],
            primary_strategy_route="ma_golden_cross",
        )
        for idx, review_as_of in enumerate(("2026-06-03", "2026-06-09"), start=1):
            event_store.append(
                stream=f"trade:002384:false_break_{idx}",
                stream_type="trade",
                event_type="trade.review.recorded",
                payload={
                    "code": "002384",
                    "name": "东山精密",
                    "entry_date": "2026-06-01",
                    "review_as_of": review_as_of,
                    "mae_pct": -0.06,
                    "latest_return_pct": -0.04,
                    "hypothesis_validation": {"status": "invalidation_possible"},
                },
                metadata={"source": "test"},
            )

        class StaticScorer:
            def score_batch(self, snapshots):
                return [score]

        decider = Decider(
            buy_threshold=6.0,
            watch_threshold=5.0,
            require_entry_signal_for_buy=True,
        )
        svc = StrategyService(
            StaticScorer(),
            decider,
            event_store,
            false_breakout_cfg={
                "enabled": True,
                "lookback_days": 30,
                "failure_threshold": 2,
                "cooldown_days": 10,
                "as_of": "2026-06-13",
            },
        )

        decisions = svc.evaluate(
            [_make_snapshot("002384", "东山精密")],
            MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
            run_id="run_false_breakout_cooldown",
            config_version="v_test",
        )

        assert decisions[0].action == Action.WATCH
        assert "假突破冷却" in " ".join(decisions[0].notes)
        decision_payload = event_store.query(event_type="decision.suggested")[-1]["payload"]
        assert decision_payload["entry_signal"] is False
        assert decision_payload["false_breakout_cooldown"]["active"] is True
        assert decision_payload["strategy_routes"][0]["status"] == "watch"
        assert event_store.query(event_type="manual_trade.requested") == []

    def test_degraded_score_keeps_previous_valid_score_as_reference_only(self, event_store):
        previous_event_id = event_store.append(
            stream="strategy:002138",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "002138",
                "name": "双环传动",
                "total_score": 7.2,
                "data_quality": "ok",
            },
            metadata={"run_id": "run_previous", "config_version": "v_old"},
        )
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=[],
        )
        decider = Decider(buy_threshold=6.5, watch_threshold=5.0)
        svc = StrategyService(scorer, decider, event_store)
        degraded_snapshot = replace(
            _make_snapshot("002138", "双环传动"),
            financial=FinancialReport(),
        )

        svc.evaluate(
            [degraded_snapshot],
            MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
            run_id="run_degraded",
            config_version="v_new",
        )

        score_events = event_store.query(event_type="score.calculated")
        current_payload = score_events[-1]["payload"]
        assert current_payload["data_quality"] == DataQuality.DEGRADED.value
        assert current_payload["total_score"] != 7.2
        assert current_payload["previous_valid_score"] == {
            "event_id": previous_event_id,
            "occurred_at": score_events[0]["occurred_at"],
            "run_id": "run_previous",
            "config_version": "v_old",
            "total_score": 7.2,
            "data_quality": "ok",
            "reference_only": True,
            "note": "当前评分数据质量降级时仅作参考，不替代本次评分。",
        }

    def test_evaluate_creates_pending_manual_trade_for_buy_decision(self, event_store):
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=[],
        )
        decider = Decider(buy_threshold=6.5, watch_threshold=5.0)
        svc = StrategyService(scorer, decider, event_store)

        market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
        decisions = svc.evaluate(
            [_make_snapshot("002138", "双环传动")],
            market,
            run_id="run_manual_intent",
            config_version="v_test",
        )

        assert decisions[0].action == Action.BUY
        intent_events = event_store.query(event_type="manual_trade.requested")
        assert len(intent_events) == 1
        intent = intent_events[0]
        assert intent["stream"] == "manual_trade:002138"
        assert intent["payload"]["status"] == "pending"
        assert intent["payload"]["side"] == "buy"
        assert intent["payload"]["code"] == "002138"
        assert intent["payload"]["suggested_price"] == 15.0
        assert intent["payload"]["position_pct"] > 0
        assert intent["metadata"]["run_id"] == "run_manual_intent"

    def test_evaluate_invokes_manual_trade_notifier_after_buy_decision(self, event_store):
        notifications = []
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=[],
        )
        decider = Decider(buy_threshold=6.5, watch_threshold=5.0)
        svc = StrategyService(
            scorer,
            decider,
            event_store,
            manual_trade_notifier=notifications.append,
        )

        svc.evaluate(
            [_make_snapshot("002138", "双环传动")],
            MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
            run_id="run_manual_notify",
            config_version="v_test",
        )

        intent_events = event_store.query(event_type="manual_trade.requested")
        assert len(intent_events) == 1
        assert len(notifications) == 1
        notification = notifications[0]
        assert notification["event_id"] == intent_events[0]["event_id"]
        assert notification["event_type"] == "manual_trade.requested"
        assert notification["manual_trade"]["code"] == "002138"
        assert notification["manual_trade"]["suggested_price"] == 15.0
        assert notification["metadata"]["execution"] == "manual"
        assert notification["score_result"].code == "002138"
        assert notification["decision"].action == Action.BUY
        assert notification["snapshot"].quote.close == 15.0

    def test_score_single_writes_event(self, event_store):
        scorer = Scorer(
            weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
            veto_rules=[],
        )
        decider = Decider()
        svc = StrategyService(scorer, decider, event_store)

        result = svc.score_single(
            _make_snapshot(), run_id="run_single", config_version="v1",
        )

        assert result.code == "002138"
        events = event_store.query(event_type="score.calculated")
        assert len(events) == 1
        assert events[0]["payload"]["code"] == "002138"


class TestRiskService:
    def test_assess_position_writes_events(self, event_store):
        svc = RiskService(event_store)

        signals = svc.assess_position(
            code="002138",
            avg_cost=50.0,
            current_price=45.0,
            entry_date=date(2026, 4, 1),
            today=date(2026, 4, 10),
            highest_since_entry=52.0,
            entry_day_low=49.0,
            risk_params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
            run_id="run_risk_001",
        )

        assert len(signals) >= 1
        risk_events = event_store.query(stream="risk:002138")
        assert len(risk_events) >= 1

    def test_assess_portfolio_writes_events(self, event_store):
        svc = RiskService(event_store)

        breaches = svc.assess_portfolio(
            daily_pnl_pct=-0.04,
            consecutive_loss_days=1,
            max_single_exposure_pct=0.15,
            max_sector_exposure_pct=0.30,
            limits=PortfolioLimits(daily_loss_limit_pct=0.03),
            run_id="run_risk_002",
        )

        assert any(b.rule == "daily_loss_limit" for b in breaches)
        events = event_store.query(event_type="risk.portfolio_breach")
        assert len(events) >= 1

    def test_calc_position_writes_event(self, event_store):
        svc = RiskService(event_store)

        ps = svc.calc_and_record_position(
            code="002138",
            total_capital=450000,
            current_exposure_pct=0.2,
            price=15.0,
            market_multiplier=1.0,
            run_id="run_risk_003",
        )

        assert ps.shares > 0
        events = event_store.query(event_type="risk.position_sized")
        assert len(events) == 1
        assert events[0]["payload"]["code"] == "002138"
