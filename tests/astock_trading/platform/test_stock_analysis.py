from __future__ import annotations

import asyncio

from astock_trading.market.models import StockQuote, StockSnapshot, TechnicalIndicators
from astock_trading.platform.stock_analysis import (
    build_stock_analysis_payload,
    resolve_stock_identifier,
)
from astock_trading.strategy.models import (
    Action,
    DataQuality,
    DecisionIntent,
    DimensionScore,
    MarketSignal,
    MarketState,
    ScoreResult,
    Style,
)


def test_resolve_stock_identifier_supports_chinese_name():
    async def resolver(query: str) -> list[dict]:
        assert query == "三安光电"
        return [{"代码": "600703", "名称": "三安光电"}]

    result = asyncio.run(resolve_stock_identifier("三安光电", resolver=resolver))

    assert result == {"code": "600703", "name": "三安光电", "source": "screener"}


def test_build_stock_analysis_payload_marks_report_non_executable():
    snapshot = StockSnapshot(
        code="600703",
        name="三安光电",
        quote=StockQuote(
            code="600703",
            name="三安光电",
            price=12.3,
            open=12.0,
            high=12.5,
            low=11.9,
            close=12.3,
            volume=1000000,
            amount=12300000,
            change_pct=1.2,
        ),
        technical=TechnicalIndicators(
            ma5=12.0,
            ma10=11.8,
            ma20=11.5,
            ma60=10.8,
            above_ma20=True,
            volume_ratio=1.8,
            rsi=58,
            golden_cross=True,
            ma20_slope=0.01,
            momentum_5d=3.0,
            daily_volatility=0.02,
            deviation_rate=4.0,
            change_pct=1.2,
        ),
    )
    score = ScoreResult(
        code="600703",
        name="三安光电",
        total=6.3,
        dimensions=[
            DimensionScore("technical", 2.4, 3.0, "技术达标"),
            DimensionScore("fundamental", 1.4, 3.0, "基本面可用"),
            DimensionScore("flow", 1.0, 2.0, "资金一般"),
            DimensionScore("sentiment", 1.5, 3.0, "中性"),
        ],
        entry_signal=True,
        style=Style.MOMENTUM,
        style_confidence=0.67,
        data_quality=DataQuality.OK,
    )
    decision = DecisionIntent(
        code="600703",
        name="三安光电",
        action=Action.BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.16,
        market_signal=MarketSignal.GREEN,
        market_multiplier=0.8,
    )

    payload = build_stock_analysis_payload(
        identifier="三安光电",
        resolved={"code": "600703", "name": "三安光电", "source": "screener"},
        snapshot=snapshot,
        score=score,
        decision=decision,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=0.8, detail={"沪深300": "ok"}),
        profile="trend_swing",
        config_version="v1",
        candidate_pool={"pool_tier": "watch", "score": 6.1},
        history=[{"date": "2026-05-16", "total_score": 6.1}],
    )

    assert payload["analysis"] == "stock"
    assert payload["execution_allowed"] is False
    assert payload["resolved"]["code"] == "600703"
    assert payload["score"]["total_score"] == 6.3
    assert payload["decision"]["action"] == "BUY"
    assert payload["market"]["signal"] == "GREEN"
    assert payload["candidate_pool"]["pool_tier"] == "watch"
    assert payload["history"][0]["total_score"] == 6.1
    assert "manual confirmation" in payload["recommendations"][0]
