from __future__ import annotations

import asyncio
import json
import sqlite3

import astock_trading.platform.stock_analysis as stock_analysis
from astock_trading.market.models import StockQuote, StockSnapshot, TechnicalIndicators
from astock_trading.platform.stock_analysis import (
    build_stock_analysis_payload,
    lookup_stock_identifier_from_db,
    resolve_stock_identifier,
    _with_resolved_snapshot_name,
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


def test_resolve_stock_identifier_backfills_name_for_input_code():
    result = asyncio.run(
        resolve_stock_identifier(
            "600703",
            name_lookup=lambda value: {"code": value, "name": "三安光电", "source": "local_cache"},
        )
    )

    assert result == {"code": "600703", "name": "三安光电", "source": "local_cache"}


def test_resolve_stock_identifier_uses_screener_for_input_code_when_local_cache_misses():
    async def resolver(query: str) -> list[dict]:
        assert query == "600703"
        return [{"代码": "600703", "名称": "三安光电"}]

    result = asyncio.run(
        resolve_stock_identifier("600703", resolver=resolver, name_lookup=lambda value: None)
    )

    assert result == {"code": "600703", "name": "三安光电", "source": "screener"}


def test_resolve_stock_identifier_uses_basic_info_for_input_code_when_screener_misses(monkeypatch):
    async def resolver(query: str) -> list[dict]:
        assert query == "600519"
        return []

    async def basic_info_lookup(code: str) -> str | None:
        assert code == "600519"
        return "贵州茅台"

    monkeypatch.setattr(stock_analysis, "_lookup_stock_name_from_basic_info", basic_info_lookup)

    result = asyncio.run(
        resolve_stock_identifier("600519", resolver=resolver, name_lookup=lambda value: None)
    )

    assert result == {"code": "600519", "name": "贵州茅台", "source": "basic_info"}


def test_resolve_stock_identifier_uses_spot_snapshot_for_name_when_screener_misses(monkeypatch):
    async def resolver(query: str) -> list[dict]:
        assert query == "三安光电"
        return []

    async def spot_lookup(identifier: str) -> dict | None:
        assert identifier == "三安光电"
        return {"code": "600703", "name": "三安光电", "source": "spot"}

    monkeypatch.setattr(stock_analysis, "_lookup_stock_from_spot", spot_lookup)

    result = asyncio.run(
        resolve_stock_identifier("三安光电", resolver=resolver, name_lookup=lambda value: None)
    )

    assert result == {"code": "600703", "name": "三安光电", "source": "spot"}


def test_lookup_stock_identifier_from_db_resolves_recent_observation_name():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE projection_candidate_pool (
            code TEXT,
            pool_tier TEXT,
            name TEXT,
            score REAL,
            added_at TEXT,
            last_scored_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE market_observations (
            observation_id TEXT,
            source TEXT,
            kind TEXT,
            symbol TEXT,
            observed_at TEXT,
            run_id TEXT,
            payload_json TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO market_observations
           (observation_id, source, kind, symbol, observed_at, payload_json)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "obs-1",
            "test",
            "stock_news",
            "600703",
            "2026-05-16T00:00:00+00:00",
            json.dumps({"quote": {"代码": "600703", "名称": "三安光电"}}, ensure_ascii=False),
        ),
    )

    assert lookup_stock_identifier_from_db(conn, "三安光电") == {
        "code": "600703",
        "name": "三安光电",
        "source": "local_cache",
    }
    assert lookup_stock_identifier_from_db(conn, "600703") == {
        "code": "600703",
        "name": "三安光电",
        "source": "local_cache",
    }


def test_with_resolved_snapshot_name_updates_quote_name_when_provider_returns_code():
    snapshot = StockSnapshot(
        code="600703",
        name="600703",
        quote=StockQuote(
            code="600703",
            name="600703",
            price=12.3,
            open=12.0,
            high=12.5,
            low=11.9,
            close=12.3,
            volume=1000000,
            amount=12300000,
            change_pct=1.2,
        ),
    )

    result = _with_resolved_snapshot_name(snapshot, "三安光电")

    assert result.name == "三安光电"
    assert result.quote.name == "三安光电"


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
