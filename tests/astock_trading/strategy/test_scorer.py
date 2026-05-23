"""Tests for strategy/scorer.py — pure function scoring"""

import pytest

from astock_trading.market.models import (
    FinancialReport,
    FundFlow,
    SectorContext,
    SentimentData,
    StockQuote,
    StockSnapshot,
    TechnicalIndicators,
)
from astock_trading.strategy.models import DataQuality, ScoringWeights, Style
from astock_trading.strategy.scorer import Scorer


def _make_snapshot(**overrides) -> StockSnapshot:
    """Build a StockSnapshot with sensible defaults."""
    tech = TechnicalIndicators(
        ma5=15.0, ma10=14.5, ma20=14.0, ma60=13.0,
        above_ma20=True, volume_ratio=1.8, rsi=55.0,
        golden_cross=True, ma20_slope=0.01,
        momentum_5d=3.0, daily_volatility=0.025,
        deviation_rate=2.0, change_pct=1.5,
    )
    quote = StockQuote(
        code="002138", name="双环传动", price=15.0,
        open=14.8, high=15.2, low=14.7, close=15.0,
        volume=5000000, amount=7.5e8, change_pct=1.5,
    )
    fin = FinancialReport(roe=12.0, revenue_growth=15.0,
                          operating_cash_flow=1e8, pe_ttm=25.0)
    flow = FundFlow(net_inflow_1d=6e8, northbound_net_positive=True)
    sent = SentimentData(score=2.0, detail="研报3篇")

    defaults = dict(
        code="002138", name="双环传动",
        quote=quote, technical=tech, financial=fin,
        flow=flow, sentiment=sent,
    )
    defaults.update(overrides)
    return StockSnapshot(**defaults)


@pytest.fixture
def scorer():
    return Scorer(
        weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
        veto_rules=["below_ma20", "limit_up_today", "consecutive_outflow", "ma20_trend_down"],
        entry_cfg={"rsi_max": 70, "volume_ratio_min": 1.5},
    )


def test_basic_score(scorer):
    s = _make_snapshot()
    result = scorer.score(s)

    assert result.code == "002138"
    assert result.total > 0
    assert len(result.dimensions) == 4
    assert not result.veto_triggered
    assert result.entry_signal is True  # golden_cross + vol_ratio + rsi ok


def test_detects_volume_breakout_strategy_route(scorer):
    s = _make_snapshot(
        quote=StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.3, high=15.1, low=14.2, close=15.0,
            volume=8000000, amount=1.1e9, change_pct=4.8,
        ),
        technical=TechnicalIndicators(
            ma5=15.0, ma10=14.4, ma20=13.8, ma60=12.9,
            above_ma20=True, volume_ratio=2.4, rsi=62.0,
            golden_cross=True, ma20_slope=0.018,
            momentum_5d=6.2, daily_volatility=0.03,
            deviation_rate=4.8, change_pct=4.8,
        ),
    )

    result = scorer.score(s)

    assert result.primary_strategy_route == "volume_breakout"
    assert result.strategy_routes[0].route == "volume_breakout"
    assert result.strategy_routes[0].family == "short_continuation"
    payload = result.to_dict()
    assert payload["primary_strategy_route"] == "volume_breakout"
    assert payload["strategy_routes"][0]["display_name"] == "放量突破"
    assert payload["strategy_routes"][0]["evidence"]["volume_ratio"] == 2.4


def test_detects_short_continuation_entry_route_without_golden_cross(scorer):
    s = _make_snapshot(
        name="短续候选",
        quote=StockQuote(
            code="300611", name="短续候选", price=28.96,
            open=28.0, high=29.18, low=27.63, close=28.96,
            volume=12000000, amount=3.4e8, change_pct=4.0,
        ),
        technical=TechnicalIndicators(
            ma5=27.4, ma10=26.3, ma20=25.9, ma60=24.5,
            above_ma20=True, volume_ratio=1.35, rsi=66.0,
            golden_cross=False, ma20_slope=0.018,
            momentum_5d=7.2, daily_volatility=0.035,
            deviation_rate=5.2, change_pct=4.0,
        ),
    )

    result = scorer.score(s)

    assert result.entry_signal is True
    assert result.primary_strategy_route == "short_continuation"
    route = result.strategy_routes[0]
    assert route.display_name == "短续接力"
    assert route.family == "short_continuation"
    assert route.entry_signal is True
    assert route.evidence["continuation_score"] >= 2.5
    assert route.evidence["close_near_high"] >= 0.75


def test_detects_flow_confirmed_trend_when_relative_volume_is_low(scorer):
    s = _make_snapshot(
        name="资金趋势候选",
        quote=StockQuote(
            code="002384", name="资金趋势候选", price=220.55,
            open=210.92, high=221.73, low=210.92, close=220.55,
            volume=84361695, amount=18_374_458_405.73, change_pct=6.56,
        ),
        technical=TechnicalIndicators(
            ma5=216.77, ma10=216.06, ma20=203.87, ma60=143.58,
            above_ma20=True, volume_ratio=0.92, rsi=67.6,
            golden_cross=True, ma20_slope=0.061,
            momentum_5d=8.46, daily_volatility=0.049,
            deviation_rate=8.18, change_pct=6.56,
        ),
        flow=FundFlow(net_inflow_1d=3_330_975_094, northbound_net_positive=False),
    )

    result = scorer.score(s)

    assert result.entry_signal is True
    assert result.primary_strategy_route == "flow_confirmed_trend"
    route = result.strategy_routes[0]
    assert route.display_name == "资金趋势确认"
    assert route.family == "trend_swing"
    assert route.entry_signal is True
    assert route.evidence["volume_ratio"] == 0.92
    assert route.evidence["main_net_inflow"] == 3_330_975_094


def test_detects_shrink_pullback_strategy_route(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(
            ma5=14.2, ma10=14.0, ma20=13.8, ma60=13.0,
            above_ma20=True, volume_ratio=0.85, rsi=52.0,
            golden_cross=False, ma20_slope=0.009,
            momentum_5d=1.2, daily_volatility=0.014,
            deviation_rate=1.1, change_pct=-0.4,
        ),
    )

    result = scorer.score(s)

    routes = {route.route: route for route in result.strategy_routes}
    assert "shrink_pullback" in routes
    assert routes["shrink_pullback"].family == "trend_swing"
    assert routes["shrink_pullback"].evidence["volume_ratio"] == 0.85


def test_detects_trend_watch_route_when_volume_ratio_is_missing(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(
            ma5=123.3, ma10=122.1, ma20=118.2, ma60=108.8,
            above_ma20=True, volume_ratio=0.0, rsi=62.8,
            golden_cross=True, ma20_slope=0.033,
            momentum_5d=7.9, daily_volatility=0.08,
            deviation_rate=7.6, change_pct=0.1,
        ),
    )

    result = scorer.score(s)

    assert result.entry_signal is False
    assert result.primary_strategy_route == "trend_watch"
    route = result.strategy_routes[0]
    assert route.display_name == "趋势观察"
    assert route.entry_signal is False
    assert route.evidence["volume_ratio"] == 0.0
    assert "volume_ratio_missing_blocks_entry" in route.notes


def test_configured_volume_floor_allows_trend_golden_cross_entry():
    scorer = Scorer(
        weights=ScoringWeights(technical=3, fundamental=2, flow=2, sentiment=3),
        veto_rules=["below_ma20", "limit_up_today", "consecutive_outflow", "ma20_trend_down"],
        entry_cfg={"rsi_max": 70, "volume_ratio_min": 1.2},
    )
    s = _make_snapshot(
        technical=TechnicalIndicators(
            ma5=123.3, ma10=122.1, ma20=118.2, ma60=108.8,
            above_ma20=True, volume_ratio=1.26, rsi=62.8,
            golden_cross=True, ma20_slope=0.033,
            momentum_5d=7.9, daily_volatility=0.08,
            deviation_rate=7.6, change_pct=0.1,
        ),
    )

    result = scorer.score(s)

    assert result.entry_signal is True
    route = next(route for route in result.strategy_routes if route.route == "ma_golden_cross")
    assert route.entry_signal is True


def test_dragon_head_route_uses_confirmed_sector_strength(scorer):
    s = _make_snapshot(
        name="强势龙头",
        quote=StockQuote(
            code="300001", name="强势龙头", price=20.0,
            open=18.8, high=20.1, low=18.7, close=20.0,
            volume=12000000, amount=1.8e9, change_pct=7.5,
        ),
        technical=TechnicalIndicators(
            ma5=20.0, ma10=18.8, ma20=17.5, ma60=15.0,
            above_ma20=True, volume_ratio=2.2, rsi=68.0,
            golden_cross=True, ma20_slope=0.025,
            momentum_5d=9.0, daily_volatility=0.04,
            deviation_rate=8.0, change_pct=7.5,
        ),
        sector=SectorContext(
            industry_name="机器人",
            industry_rank=2,
            industry_change_pct=3.0,
            leader="强势龙头",
            relative_strength_pct=4.5,
            confirmed=True,
        ),
    )

    result = scorer.score(s)
    dragon = next(route for route in result.strategy_routes if route.route == "dragon_head")

    assert dragon.confidence == 0.9
    assert dragon.entry_signal is True
    assert "requires_sector_strength_confirmation" not in dragon.notes
    assert dragon.evidence["sector_confirmation"] == "confirmed"
    assert dragon.evidence["industry_name"] == "机器人"
    assert dragon.evidence["relative_strength_pct"] == 4.5


def test_veto_below_ma20(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(above_ma20=False, rsi=55, volume_ratio=1.8),
    )
    result = scorer.score(s)

    assert result.veto_triggered
    assert "below_ma20" in result.hard_veto
    assert result.total == 0.0


def test_veto_limit_up(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(
            above_ma20=True, change_pct=10.0, rsi=55, volume_ratio=1.8,
        ),
    )
    result = scorer.score(s)

    assert "limit_up_today" in result.hard_veto
    assert result.total == 0.0


def test_no_entry_signal_when_rsi_high(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(
            ma5=15, ma10=14.5, ma20=14, ma60=13,
            above_ma20=True, volume_ratio=2.0, rsi=75.0,
            golden_cross=True, ma20_slope=0.01,
            momentum_5d=3.0, daily_volatility=0.025,
        ),
    )
    result = scorer.score(s)

    assert result.entry_signal is False  # RSI too high


def test_style_classification_momentum(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(
            above_ma20=True, rsi=78, volume_ratio=2.0,
            daily_volatility=0.04, ma20_slope=0.03,
            golden_cross=True,
        ),
    )
    result = scorer.score(s)
    assert result.style == Style.MOMENTUM


def test_style_classification_slow_bull(scorer):
    s = _make_snapshot(
        technical=TechnicalIndicators(
            above_ma20=True, rsi=58, volume_ratio=1.5,
            daily_volatility=0.015, ma20_slope=0.008,
            golden_cross=False, ma5=15, ma10=14.5, ma20=14, ma60=13,
        ),
    )
    result = scorer.score(s)
    assert result.style == Style.SLOW_BULL


def test_degraded_data_quality(scorer):
    s = _make_snapshot(
        financial=FinancialReport(roe=10.0),  # missing revenue_growth and cash_flow
    )
    result = scorer.score(s)
    assert result.data_quality == DataQuality.DEGRADED
    assert len(result.data_missing_fields) > 0


def test_missing_flow_degrades_data_quality(scorer):
    s = _make_snapshot(flow=None)

    result = scorer.score(s)

    assert result.data_quality == DataQuality.DEGRADED
    assert "资金流" in result.data_missing_fields


def test_batch_score_sorted(scorer):
    s1 = _make_snapshot(code="001", name="高分股",
                        technical=TechnicalIndicators(
                            above_ma20=True, rsi=55, volume_ratio=2.0,
                            golden_cross=True, ma5=15, ma10=14.5, ma20=14, ma60=13,
                            momentum_5d=5, ma20_slope=0.01,
                        ))
    s2 = _make_snapshot(code="002", name="低分股",
                        technical=TechnicalIndicators(above_ma20=True, rsi=55))

    results = scorer.score_batch([s2, s1])
    assert results[0].code == "001"  # higher score first


def test_consecutive_outflow_warn(scorer):
    """consecutive_outflow with above_ma20 + high amount → warn only, not hard veto"""
    s = _make_snapshot(
        technical=TechnicalIndicators(above_ma20=True, rsi=55, volume_ratio=1.8),
        flow=FundFlow(consecutive_outflow_days=3, northbound_net_positive=True),
        quote=StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=6e8, change_pct=1.5,
        ),
    )
    result = scorer.score(s)
    assert "consecutive_outflow_warn" in result.warning_signals
    assert not result.veto_triggered
    assert result.total > 0  # reduced but not zero


def test_none_dimensions_handled(scorer):
    """Snapshot with all None data should not crash."""
    s = StockSnapshot(code="999", name="空数据")
    result = scorer.score(s)
    assert result.total == 0.0 or result.total >= 0
    assert result.data_quality in (DataQuality.OK, DataQuality.ERROR, DataQuality.DEGRADED)
