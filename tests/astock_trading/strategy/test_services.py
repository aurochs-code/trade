"""Tests for strategy/service.py and risk/service.py — service layer with event_log"""

import pytest
from dataclasses import replace
from datetime import date

from astock_trading.platform.db import init_db, connect
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
    MarketSignal,
    MarketState,
    ScoringWeights,
)
from astock_trading.strategy.scorer import Scorer
from astock_trading.strategy.decider import Decider
from astock_trading.strategy.service import StrategyService
from astock_trading.risk.models import PortfolioLimits, RiskParams
from astock_trading.risk.service import RiskService
from astock_trading.strategy.models import Style


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    yield conn
    conn.close()


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
