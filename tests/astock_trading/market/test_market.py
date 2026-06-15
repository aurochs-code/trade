"""Tests for market/store.py and market/service.py"""

import asyncio
from datetime import datetime, timezone
import json
import logging
import subprocess
import sys
from types import SimpleNamespace
import warnings
import numpy as np
import pandas as pd
import pytest

from astock_trading.market import service as market_service_module
from astock_trading.market.adapters import AkShareHKMarketAdapter, OpenCliFinanceAdapter, OpenCliXueqiuAdapter
from astock_trading.market.akshare_adapters import MXMarketAdapter
from astock_trading.market.models import (
    FinancialReport,
    FundFlow,
    IndexQuote,
    SentimentData,
    StockQuote,
    StockSnapshot,
    TechnicalIndicators,
)
from astock_trading.market.store import MarketStore
from astock_trading.market.service import MarketService
from astock_trading.market.source_router import SourceRouteOptions


@pytest.fixture
def db(mysql_conn):
    yield mysql_conn


@pytest.fixture
def store(db):
    return MarketStore(db)


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def test_fetch_sina_intraday_suppresses_akshare_stderr(monkeypatch, capfd):
    class FakeAkshare:
        def stock_intraday_sina(self, symbol, date):
            print("0%| mock intraday progress", file=sys.stderr)
            return pd.DataFrame({
                "price": [14.8, 15.0],
                "volume": [1000, 2000],
                "name": ["双环传动", "双环传动"],
            })

        def stock_zh_a_daily(self, symbol, adjust):
            print("0%| mock daily progress", file=sys.stderr)
            return pd.DataFrame({"close": [14.5, 14.8]})

    monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())

    quotes = market_service_module._fetch_sina_intraday(["002138"])

    captured = capfd.readouterr()
    assert quotes["002138"] is not None
    assert quotes["002138"].price == 15.0
    assert captured.err == ""


def test_collect_intraday_batch_suppresses_akshare_spot_stderr(monkeypatch, store, capfd):
    class FakeAkshare:
        def stock_zh_a_spot_em(self):
            print("0%| mock spot progress", file=sys.stderr)
            return pd.DataFrame([{
                "代码": "002138",
                "名称": "双环传动",
                "最新价": 15.0,
                "今开": 14.8,
                "最高": 15.2,
                "最低": 14.7,
                "成交量": 5000000,
                "成交额": 750000000.0,
                "涨跌幅": 1.5,
            }])

    monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())
    svc = MarketService(market_providers=[], store=store)

    snapshots = _run_async(
        svc.collect_intraday_batch([{"code": "002138", "name": "双环传动"}], run_id="run_spot")
    )

    captured = capfd.readouterr()
    assert snapshots[0].quote is not None
    assert snapshots[0].quote.price == 15.0
    assert captured.err == ""


def test_get_quote_rejects_provider_price_that_diverges_from_daily_kline():
    class BadRealtimeProvider:
        async def get_realtime(self, codes):
            return {
                "600584": StockQuote(
                    code="600584",
                    name="长电科技",
                    price=177.75,
                    open=190.82,
                    high=192.99,
                    low=176.66,
                    close=177.75,
                    volume=43_493_461,
                    amount=8_113_828_937.0,
                    change_pct=-4.64,
                )
            }

        async def get_kline(self, code, period="daily", count=120):
            assert code == "600584"
            return pd.DataFrame({
                "date": ["2026-05-21", "2026-05-22"],
                "open": [65.0, 66.88],
                "high": [67.0, 71.28],
                "low": [64.0, 65.67],
                "close": [66.0, 66.84],
                "volume": [100_000_000, 312_505_756],
                "amount": [6_600_000_000.0, 21_324_471_319.4],
            })

    class FallbackRealtimeProvider:
        async def get_realtime(self, codes):
            return {
                "600584": StockQuote(
                    code="600584",
                    name="长电科技",
                    price=66.84,
                    open=66.88,
                    high=71.28,
                    low=65.67,
                    close=66.84,
                    volume=312_505_756,
                    amount=21_324_471_319.4,
                    change_pct=0.94,
                )
            }

        async def get_kline(self, code, period="daily", count=120):
            return None

    svc = MarketService(market_providers=[BadRealtimeProvider(), FallbackRealtimeProvider()])

    quote = _run_async(svc._get_quote("600584"))

    assert quote is not None
    assert quote.price == 66.84


def test_collect_batch_times_out_single_snapshot_and_continues():
    class SlowMarketService(MarketService):
        async def collect_snapshot(self, code, name="", run_id=None):
            await asyncio.sleep(1)
            return SimpleNamespace(code=code, name=name)

    svc = SlowMarketService(market_providers=[])

    snapshots = _run_async(
        svc.collect_batch(
            [{"code": "002138", "name": "双环传动"}],
            run_id="run-timeout",
            per_snapshot_timeout_seconds=0.01,
        )
    )

    assert len(snapshots) == 1
    assert snapshots[0].code == "002138"
    assert snapshots[0].name == "双环传动"
    assert snapshots[0].quote is None


def test_collect_batch_timeout_does_not_expire_queued_snapshots():
    class SlowButValidMarketService(MarketService):
        async def collect_snapshot(self, code, name="", run_id=None):
            async with self._sem:
                await asyncio.sleep(0.03)
                quote = StockQuote(
                    code=code,
                    name=name,
                    price=10.0,
                    open=10.0,
                    high=10.0,
                    low=10.0,
                    close=10.0,
                    volume=100,
                    amount=1000.0,
                    change_pct=1.0,
                )
                return StockSnapshot(code=code, name=name, quote=quote)

    svc = SlowButValidMarketService(market_providers=[], concurrency=1)

    snapshots = _run_async(
        svc.collect_batch(
            [
                {"code": "002138", "name": "双环传动"},
                {"code": "688981", "name": "中芯国际"},
            ],
            run_id="run-queued-timeout",
            per_snapshot_timeout_seconds=0.05,
        )
    )

    assert [snapshot.code for snapshot in snapshots] == ["002138", "688981"]
    assert all(snapshot.quote is not None for snapshot in snapshots)


def test_collect_batch_times_out_sector_context_and_keeps_snapshots():
    class SlowSectorMarketService(MarketService):
        async def collect_snapshot(self, code, name="", run_id=None):
            quote = StockQuote(
                code=code,
                name=name,
                price=10.0,
                open=10.0,
                high=10.0,
                low=10.0,
                close=10.0,
                volume=100,
                amount=1000.0,
                change_pct=1.0,
            )
            return StockSnapshot(code=code, name=name, quote=quote)

        async def _attach_sector_context(self, snapshots, run_id=None):
            await asyncio.sleep(1)
            return []

    svc = SlowSectorMarketService(market_providers=[])

    snapshots = _run_async(
        svc.collect_batch(
            [{"code": "002138", "name": "双环传动"}],
            run_id="run-sector-timeout",
            include_sector_context=True,
            sector_context_timeout_seconds=0.01,
        )
    )

    assert len(snapshots) == 1
    assert snapshots[0].code == "002138"
    assert snapshots[0].quote is not None
    assert snapshots[0].sector is None


# ---------------------------------------------------------------------------
# MarketStore tests
# ---------------------------------------------------------------------------

class TestMarketStore:
    def test_save_and_get_observation(self, store):
        store.save_observation("test", "quote", "002138", {"price": 15.0})
        result = store.get_latest_observation("002138", "quote")
        assert result is not None
        assert result["price"] == 15.0

    def test_ttl_expired(self, store):
        store.save_observation("test", "quote", "002138", {"price": 15.0})
        # TTL=0 means always expired
        result = store.get_latest_observation("002138", "quote", max_age_seconds=0)
        assert result is None

    def test_ttl_valid(self, store):
        store.save_observation("test", "quote", "002138", {"price": 15.0})
        result = store.get_latest_observation("002138", "quote", max_age_seconds=3600)
        assert result is not None

    def test_get_cached(self, store):
        store.save_observation("test", "financial", "002138", {"roe": 12.0})
        result = store.get_cached("002138", "financial")
        assert result is not None
        assert result["roe"] == 12.0

    def test_no_observation(self, store):
        result = store.get_latest_observation("999999", "quote")
        assert result is None

    def test_save_provider_failure_observation(self, store):
        observation_id = store.save_provider_failure(
            source="BaiduFundFlowAdapter",
            target_kind="fund_flow",
            symbol="000858",
            status="parse_error",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            run_id="run_provider_failure",
        )

        result = store.get_latest_observation("000858", "provider_failure")

        assert observation_id
        assert result["source"] == "BaiduFundFlowAdapter"
        assert result["target_kind"] == "fund_flow"
        assert result["status"] == "parse_error"
        assert result["error_type"] == "JSONDecodeError"
        assert result["error_message"] == "Expecting value"

    def test_save_and_get_price_bars_preserves_adjustflag(self, store):
        bars = pd.DataFrame({
            "日期": ["2024-01-02", "2024-01-03"],
            "开盘": [10.0, 10.2],
            "最高": [10.5, 10.6],
            "最低": [9.8, 10.0],
            "收盘": [10.2, 10.4],
            "成交量": [1000000, 1100000],
            "成交额": [10000000, 11000000],
            "涨跌幅": [1.0, 1.96],
        })

        saved = store.save_price_bars("600036", bars, source="baostock", adjustflag="2")
        missing = store.get_price_bars("600036", "2024-01-01", "2024-01-31", adjustflag="3")
        loaded = store.get_price_bars("600036", "2024-01-01", "2024-01-31", adjustflag="2")

        assert saved == 2
        assert missing.empty
        assert loaded["日期"].tolist() == ["2024-01-02", "2024-01-03"]
        assert loaded["收盘"].tolist() == [10.2, 10.4]
        assert loaded["涨跌幅"].tolist() == [1.0, 1.96]

    def test_financial_snapshots_are_loaded_by_available_date(self, store):
        store.save_financial_snapshot(
            "600036",
            report_year=2024,
            report_quarter=1,
            report_date="2024-03-31",
            available_date="2024-04-30",
            payload={
                "roe": 12.3,
                "roe_3y_ago": 6.1,
                "revenue_growth": 8.8,
                "operating_cash_flow": 0.2,
            },
            source="baostock",
        )
        store.save_financial_snapshot(
            "600036",
            report_year=2024,
            report_quarter=2,
            report_date="2024-06-30",
            available_date="2024-08-31",
            payload={"roe": 14.0, "operating_cash_flow": 0.3},
            source="baostock",
        )

        early = store.get_financial_snapshot("600036", as_of_date="2024-04-15")
        q1 = store.get_financial_snapshot("600036", as_of_date="2024-05-01")
        q2 = store.get_financial_snapshot("600036", as_of_date="2024-09-01")

        assert early is None
        assert q1 is not None
        assert q1["report_quarter"] == 1
        assert q1["roe"] == 12.3
        assert q2 is not None
        assert q2["report_quarter"] == 2
        assert q2["roe"] == 14.0


# ---------------------------------------------------------------------------
# Mock providers for MarketService tests
# ---------------------------------------------------------------------------

class MockMarketProvider:
    def __init__(self, quotes=None):
        self._quotes = quotes or {}

    async def get_realtime(self, codes):
        return {c: self._quotes[c] for c in codes if c in self._quotes}

    async def get_kline(self, code, period="daily", count=120):
        return None

    async def get_index(self, symbols):
        return {}


class MockSectorProvider(MockMarketProvider):
    async def get_concept_blocks(self, code):
        return {
            "industry": [{"name": "机器人", "change_pct": "3.0"}],
            "concept": [],
            "region": [],
            "concept_tags": [],
        }

    async def get_industry_comparison(self, top_n=20):
        return {
            "top": [
                {
                    "rank": 2,
                    "name": "机器人",
                    "change_pct": 3.0,
                    "turnover_yi": 88.0,
                    "leader": "强势龙头",
                }
            ],
            "bottom": [],
            "total": 30,
        }


class NoIndexProvider:
    async def get_realtime(self, codes):
        return {}

    async def get_kline(self, code, period="daily", count=120):
        return None


class TrackingIndexProvider(MockMarketProvider):
    def __init__(self):
        super().__init__()
        self.index_calls = []

    async def get_index(self, symbols):
        self.index_calls.append(symbols)
        return {
            "sh000001": IndexQuote(
                symbol="sh000001",
                name="上证指数",
                price=3100.0,
                change_pct=0.5,
                ma20=3000.0,
                ma60=2950.0,
                above_ma20=True,
            )
        }


class TrackingAShareKlineProvider(MockMarketProvider):
    def __init__(self, kline=None):
        super().__init__()
        self.calls = []
        self._kline = kline if kline is not None else pd.DataFrame({"close": [1.0]})

    async def get_kline(self, code, period="daily", count=120):
        self.calls.append(code)
        return self._kline


class TrackingHKKlineProvider(AkShareHKMarketAdapter):
    def __init__(self, kline=None):
        self.calls = []
        self._kline = kline if kline is not None else pd.DataFrame({"close": [1.0]})

    async def get_realtime(self, codes):
        return {}

    async def get_kline(self, code, period="daily", count=120):
        self.calls.append(code)
        return self._kline


class MockFinancialProvider:
    def __init__(self, data=None):
        self._data = data or {}

    async def get_financial(self, code):
        return self._data.get(code)


class MockFlowProvider:
    def __init__(self, data=None):
        self._data = data or {}

    async def get_fund_flow(self, code, days=5):
        return self._data.get(code)


class SlowFlowProvider:
    def __init__(self, flow):
        self._flow = flow
        self.calls = 0

    async def get_fund_flow(self, code, days=5):
        self.calls += 1
        await asyncio.sleep(1)
        return self._flow


class TrackingFailingFlowProvider:
    def __init__(self):
        self.calls = 0

    async def get_fund_flow(self, code, days=5):
        self.calls += 1
        raise ConnectionError("mock fail")


class MockSentimentProvider:
    async def search_news(self, query):
        return SentimentData(score=2.0, detail="mock")


class FailingProvider:
    """Always raises."""
    async def get_realtime(self, codes):
        raise ConnectionError("mock fail")

    async def get_kline(self, code, period="daily", count=120):
        raise ConnectionError("mock fail")

    async def get_index(self, symbols):
        raise ConnectionError("mock fail")

    async def get_financial(self, code):
        raise ConnectionError("mock fail")

    async def get_fund_flow(self, code, days=5):
        raise ConnectionError("mock fail")

    async def search_news(self, query):
        raise ConnectionError("mock fail")


# ---------------------------------------------------------------------------
# MarketService tests
# ---------------------------------------------------------------------------

class TestMarketService:
    def test_get_flow_saves_fund_flow_observation(self, store):
        flow = FundFlow(net_inflow_1d=100.0, main_force_ratio=1.2)
        svc = MarketService(flow_providers=[MockFlowProvider({"000858": flow})], store=store)

        result = asyncio.get_event_loop().run_until_complete(svc._get_flow("000858"))

        cached = store.get_latest_observation("000858", "fund_flow")
        assert result == flow
        assert cached["net_inflow_1d"] == 100.0
        assert cached["main_force_ratio"] == 1.2

    def test_get_flow_saves_success_observation_run_id(self, store):
        flow = FundFlow(net_inflow_1d=100.0, main_force_ratio=1.2)
        svc = MarketService(flow_providers=[MockFlowProvider({"000858": flow})], store=store)

        asyncio.get_event_loop().run_until_complete(
            svc._get_flow("000858", run_id="run_flow_success")
        )

        row = store._conn.execute(
            """SELECT run_id
               FROM market_observations
               WHERE symbol = ? AND kind = ?
               ORDER BY observed_at DESC
               LIMIT 1""",
            ("000858", "fund_flow"),
        ).fetchone()
        assert row["run_id"] == "run_flow_success"

    def test_get_flow_records_provider_failure(self, store):
        svc = MarketService(flow_providers=[FailingProvider()], store=store)

        result = asyncio.get_event_loop().run_until_complete(svc._get_flow("000858"))

        failure = store.get_latest_observation("000858", "provider_failure")
        assert result is None
        assert failure["source"] == "FailingProvider"
        assert failure["target_kind"] == "fund_flow"
        assert failure["status"] == "provider_error"
        assert failure["error_type"] == "ConnectionError"
        assert "mock fail" in failure["error_message"]

    def test_get_flow_times_out_slow_provider_and_falls_back(self, store):
        slow = SlowFlowProvider(FundFlow(net_inflow_1d=1.0))
        flow = FundFlow(net_inflow_1d=100.0, main_force_ratio=1.2)
        svc = MarketService(
            flow_providers=[slow, MockFlowProvider({"000858": flow})],
            store=store,
            source_route_options={
                "fund_flow": SourceRouteOptions(timeout_seconds=0.01),
            },
        )

        result = asyncio.get_event_loop().run_until_complete(
            svc._get_flow("000858", run_id="run_flow_timeout")
        )

        failure = store.get_latest_observation("000858", "provider_failure")
        cached = store.get_latest_observation("000858", "fund_flow")
        assert result == flow
        assert slow.calls == 1
        assert failure["source"] == "SlowFlowProvider"
        assert failure["status"] == "timeout"
        assert failure["error_type"] == "TimeoutError"
        assert failure["details"]["attempt"] == 1
        assert cached["net_inflow_1d"] == 100.0

    def test_get_flow_skips_circuit_open_provider(self, store):
        bad = TrackingFailingFlowProvider()
        flow = FundFlow(net_inflow_1d=100.0)
        svc = MarketService(
            flow_providers=[bad, MockFlowProvider({"000858": flow})],
            store=store,
            source_route_options={
                "fund_flow": SourceRouteOptions(
                    timeout_seconds=0.1,
                    max_failures=1,
                    cooldown_seconds=60,
                ),
            },
        )

        first = asyncio.get_event_loop().run_until_complete(
            svc._get_flow("000858", run_id="run_flow_first")
        )
        second = asyncio.get_event_loop().run_until_complete(
            svc._get_flow("000858", run_id="run_flow_second")
        )

        circuit_row = store._conn.execute(
            """SELECT payload_json
               FROM market_observations
               WHERE kind = 'provider_failure' AND run_id = ?
               ORDER BY observed_at DESC
               LIMIT 1""",
            ("run_flow_second",),
        ).fetchone()
        circuit = json.loads(circuit_row["payload_json"])
        assert first == flow
        assert second == flow
        assert bad.calls == 1
        assert circuit["source"] == "TrackingFailingFlowProvider"
        assert circuit["status"] == "circuit_open"
        assert circuit["error_type"] == "CircuitOpen"

    def test_get_flow_serializes_numpy_scalars(self, store):
        flow = FundFlow(net_inflow_1d=np.int64(100), main_force_ratio=np.float64(1.2))
        svc = MarketService(flow_providers=[MockFlowProvider({"000858": flow})], store=store)

        result = asyncio.get_event_loop().run_until_complete(svc._get_flow("000858"))

        cached = store.get_latest_observation("000858", "fund_flow")
        failure = store.get_latest_observation("000858", "provider_failure")
        assert result == flow
        assert cached["net_inflow_1d"] == 100
        assert cached["main_force_ratio"] == 1.2
        assert failure is None

    def test_collect_signal_data_saves_observation(self, store):
        class SignalProvider:
            async def get_concept_blocks(self, code):
                return {"concept_tags": ["白酒"], "industry": [], "concept": [], "region": []}

        svc = MarketService(market_providers=[SignalProvider()], store=store)

        result = asyncio.get_event_loop().run_until_complete(
            svc.collect_concept_blocks("000858", run_id="run_signal")
        )

        cached = store.get_latest_observation("000858", "concept_blocks")
        assert result["concept_tags"] == ["白酒"]
        assert cached["concept_tags"] == ["白酒"]

    def test_collect_signal_records_provider_failure(self, store):
        class FailingSignalProvider:
            async def get_industry_comparison(self, top_n):
                raise RuntimeError("industry source unavailable")

        svc = MarketService(market_providers=[FailingSignalProvider()], store=store)

        result = asyncio.get_event_loop().run_until_complete(
            svc.collect_industry_comparison(5, run_id="run_industry_failure")
        )

        failure = store.get_latest_observation("cn_a", "provider_failure")
        assert result == {"top": [], "bottom": [], "total": 0}
        assert failure["source"] == "FailingSignalProvider"
        assert failure["target_kind"] == "industry_comparison"
        assert failure["status"] == "provider_error"
        assert failure["error_type"] == "RuntimeError"
        assert "industry source unavailable" in failure["error_message"]

    def test_collect_xueqiu_hot_stocks_saves_observation(self, store):
        class XueqiuProvider:
            async def get_xueqiu_hot_stocks(self, limit=10, list_type="10"):
                return [{"rank": 1, "code": "300274", "name": "阳光电源", "heat": 2785}]

        svc = MarketService(market_providers=[XueqiuProvider()], store=store)

        result = asyncio.get_event_loop().run_until_complete(
            svc.collect_xueqiu_hot_stocks(limit=5, run_id="run_xueqiu")
        )

        cached = store.get_latest_observation("type_10", "xueqiu_hot_stocks")
        assert result[0]["name"] == "阳光电源"
        assert cached["items"][0]["code"] == "300274"

    def test_collect_opencli_finance_signals_saves_observations(self, store):
        class FinanceProvider:
            async def get_cross_platform_hot_stocks(self, limit=10):
                return [{
                    "rank": 1,
                    "code": "300274",
                    "name": "阳光电源",
                    "source_count": 3,
                    "sources": ["xueqiu", "eastmoney", "sinafinance"],
                }]

            async def get_finance_flash(self, limit=10):
                return [{"time": "09:10", "title": "商务部回应关税安排", "source": "sinafinance"}]

            async def get_global_risk_news(self, limit=10):
                return [{"title": "US-China trade talks continue", "source": "bloomberg"}]

            async def get_market_announcements(self, limit=10):
                return [{"code": "603311", "name": "金海高科", "title": "复牌公告", "source": "eastmoney"}]

        svc = MarketService(market_providers=[FinanceProvider()], store=store)

        hot = asyncio.get_event_loop().run_until_complete(
            svc.collect_cross_platform_hot_stocks(limit=5, run_id="run_opencli")
        )
        flash = asyncio.get_event_loop().run_until_complete(
            svc.collect_finance_flash(limit=5, run_id="run_opencli")
        )
        global_news = asyncio.get_event_loop().run_until_complete(
            svc.collect_global_risk_news(limit=5, run_id="run_opencli")
        )
        announcements = asyncio.get_event_loop().run_until_complete(
            svc.collect_market_announcements(limit=5, run_id="run_opencli")
        )

        assert hot[0]["source_count"] == 3
        assert flash[0]["source"] == "sinafinance"
        assert global_news[0]["source"] == "bloomberg"
        assert announcements[0]["title"] == "复牌公告"
        assert store.get_latest_observation("cn_a", "cross_platform_hot_stocks")["items"][0]["code"] == "300274"
        assert store.get_latest_observation("cn_a", "finance_flash")["items"][0]["title"] == "商务部回应关税安排"
        assert store.get_latest_observation("global", "global_risk_news")["items"][0]["source"] == "bloomberg"
        assert store.get_latest_observation("cn_a", "market_announcements")["items"][0]["code"] == "603311"

    def test_collect_sector_heatmap_saves_observation(self, store):
        class SectorHeatmapProvider:
            async def get_sector_heatmap(self):
                return [{"name": "机器人", "change_pct": 3.2, "amount": 1200000000}]

        svc = MarketService(market_providers=[SectorHeatmapProvider()], store=store)

        result = asyncio.get_event_loop().run_until_complete(
            svc.collect_sector_heatmap(run_id="run_sector")
        )

        cached = store.get_latest_observation("cn_a", "sector_heatmap")
        assert result[0]["name"] == "机器人"
        assert cached["items"][0]["change_pct"] == 3.2

    def test_collect_intraday_batch_skips_scoring_dimensions(self, store):
        quote = StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=-1.5,
        )
        financial = MockFinancialProvider({"002138": FinancialReport(roe=12.0)})
        flow = MockFlowProvider({"002138": FundFlow(net_inflow_1d=6e8)})
        sentiment = MockSentimentProvider()
        svc = MarketService(
            market_providers=[MockMarketProvider({"002138": quote})],
            financial_providers=[financial],
            flow_providers=[flow],
            sentiment_providers=[sentiment],
            store=store,
        )

        loop = asyncio.new_event_loop()
        try:
            snap = loop.run_until_complete(
                svc.collect_intraday_batch([{"code": "002138", "name": "双环传动"}], run_id="run_intraday")
            )[0]
        finally:
            loop.close()

        assert snap.quote is not None
        assert snap.quote.price == 15.0
        assert snap.financial is None
        assert snap.flow is None
        assert snap.sentiment is None

    def test_collect_snapshot(self, store):
        quote = StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=1.5,
        )
        svc = MarketService(
            market_providers=[MockMarketProvider({"002138": quote})],
            financial_providers=[MockFinancialProvider({"002138": FinancialReport(roe=12.0)})],
            flow_providers=[MockFlowProvider({"002138": FundFlow(net_inflow_1d=6e8)})],
            sentiment_providers=[MockSentimentProvider()],
            store=store,
        )

        snap = asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动", run_id="run_test")
        )

        assert snap.code == "002138"
        assert snap.quote is not None
        assert snap.quote.price == 15.0
        assert snap.financial is not None
        assert snap.financial.roe == 12.0
        assert snap.flow is not None
        assert snap.sentiment is not None
        obs = store.get_latest_observation("002138", "snapshot")
        assert obs["quote"]["close"] == 15.0
        assert obs["financial"]["roe"] == 12.0
        assert obs["flow"]["net_inflow_1d"] == 6e8
        assert obs["completeness"]["has_quote"] is True
        row = store._conn.execute(
            "SELECT observation_id FROM market_observations WHERE symbol = ? AND kind = ?",
            ("002138", "snapshot"),
        ).fetchone()
        assert snap.observation_id == row["observation_id"]

    def test_get_financial_merges_partial_provider_reports(self, store):
        valuation = MockFinancialProvider({
            "002138": FinancialReport(pe_ttm=25.0, pb=3.2),
        })
        fundamentals = MockFinancialProvider({
            "002138": FinancialReport(
                roe=12.0,
                revenue_growth=15.0,
                operating_cash_flow=1e8,
            ),
        })
        svc = MarketService(
            financial_providers=[valuation, fundamentals],
            store=store,
        )

        report = asyncio.get_event_loop().run_until_complete(svc._get_financial("002138"))

        assert report is not None
        assert report.pe_ttm == 25.0
        assert report.pb == 3.2
        assert report.roe == 12.0
        assert report.revenue_growth == 15.0
        assert report.operating_cash_flow == 1e8

        cached = store.get_latest_observation("002138", "financial")
        assert cached["pe_ttm"] == 25.0
        assert cached["roe"] == 12.0

    def test_collect_batch(self, store):
        q1 = StockQuote(code="001", name="A", price=10.0, open=10, high=10, low=10, close=10, volume=1000, amount=1e7, change_pct=0)
        q2 = StockQuote(code="002", name="B", price=20.0, open=20, high=20, low=20, close=20, volume=2000, amount=2e7, change_pct=0)

        svc = MarketService(
            market_providers=[MockMarketProvider({"001": q1, "002": q2})],
            store=store,
        )

        snaps = asyncio.get_event_loop().run_until_complete(
            svc.collect_batch([{"code": "001", "name": "A"}, {"code": "002", "name": "B"}])
        )

        assert len(snaps) == 2
        assert snaps[0].code == "001"
        assert snaps[1].code == "002"

    def test_collect_batch_can_include_sector_context(self, store):
        quote = StockQuote(
            code="300001", name="强势龙头", price=20.0,
            open=18.8, high=20.1, low=18.7, close=20.0,
            volume=12000000, amount=1.8e9, change_pct=7.5,
        )
        svc = MarketService(
            market_providers=[MockSectorProvider({"300001": quote})],
            store=store,
        )

        snaps = asyncio.get_event_loop().run_until_complete(
            svc.collect_batch(
                [{"code": "300001", "name": "强势龙头"}],
                run_id="run_sector",
                include_sector_context=True,
            )
        )

        assert snaps[0].sector is not None
        assert snaps[0].sector.industry_name == "机器人"
        assert snaps[0].sector.industry_rank == 2
        assert snaps[0].sector.industry_change_pct == 3.0
        assert snaps[0].sector.relative_strength_pct == 4.5
        assert snaps[0].sector.confirmed is True

    def test_collect_batch_paid_sector_context_skips_free_provider(self, store):
        quote = StockQuote(
            code="300001", name="强势龙头", price=20.0,
            open=18.8, high=20.1, low=18.7, close=20.0,
            volume=12000000, amount=1.8e9, change_pct=7.5,
        )

        class TusharePaidSectorProvider(MockMarketProvider):
            async def get_concept_blocks(self, code):
                return {
                    "industry": [{"name": "机器人", "change_pct": "3.0"}],
                    "concept": [],
                    "region": [],
                    "concept_tags": [],
                }

            async def get_industry_comparison(self, top_n=20):
                return {
                    "top": [{"rank": 2, "name": "机器人", "change_pct": 3.0, "leader": "强势龙头"}],
                    "bottom": [],
                    "total": 1,
                }

        class FreeCrashingSectorProvider(MockMarketProvider):
            async def get_concept_blocks(self, code):
                raise RuntimeError("free provider should be isolated")

            async def get_industry_comparison(self, top_n=20):
                raise RuntimeError("free provider should be isolated")

        svc = MarketService(
            market_providers=[
                TusharePaidSectorProvider({"300001": quote}),
                FreeCrashingSectorProvider({"300001": quote}),
            ],
            store=store,
        )

        snaps = asyncio.get_event_loop().run_until_complete(
            svc.collect_batch(
                [{"code": "300001", "name": "强势龙头"}],
                run_id="run_paid_sector",
                include_sector_context=True,
                paid_sector_context_only=True,
            )
        )

        assert snaps[0].sector is not None
        assert snaps[0].sector.industry_name == "机器人"
        assert snaps[0].sector.confirmed is True

    def test_fallback_on_failure(self, store):
        """First provider fails, second succeeds."""
        quote = StockQuote(
            code="002138", name="双环传动", price=15.0,
            open=14.8, high=15.2, low=14.7, close=15.0,
            volume=5000000, amount=7.5e8, change_pct=1.5,
        )
        svc = MarketService(
            market_providers=[FailingProvider(), MockMarketProvider({"002138": quote})],
            financial_providers=[FailingProvider(), MockFinancialProvider({"002138": FinancialReport(roe=10.0)})],
            store=store,
        )

        snap = asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动")
        )

        assert snap.quote is not None
        assert snap.quote.price == 15.0
        assert snap.financial is not None

    def test_market_state_falls_back_to_cached_signal_when_index_providers_fail(self, store):
        store.save_observation(
            "market_service",
            "market_state",
            "market",
            {"signal": "YELLOW", "multiplier": 0.5},
            run_id="previous_market_signal",
        )
        svc = MarketService(market_providers=[FailingProvider()], store=store)

        state, index_data = _run_async(
            svc.collect_market_state(run_id="run_provider_failed")
        )

        assert state.signal.value == "YELLOW"
        assert state.multiplier == 0.5
        assert state.detail["reason"] == "指数数据源无有效数据，沿用最近市场信号"
        assert index_data == {}

    def test_market_state_uses_projection_when_live_index_payload_is_invalid(self, store):
        class InvalidIndexProvider:
            async def get_index(self, symbols):
                return {
                    symbol: IndexQuote(
                        symbol=symbol,
                        name=name,
                        price=0.0,
                        change_pct=0.0,
                        ma20=0.0,
                        ma60=0.0,
                        above_ma20=False,
                    )
                    for symbol, name in {
                        "sh000001": "上证指数",
                        "sz399001": "深证成指",
                        "sz399006": "创业板指",
                    }.items()
                }

        now = datetime.now(timezone.utc).isoformat()
        for symbol, name, ma20_pct in (
            ("sh000001", "上证指数", 2.0),
            ("sz399001", "深证成指", 1.5),
            ("sz399006", "创业板指", 3.0),
        ):
            store._conn.execute(
                """INSERT INTO projection_market_state
                   (index_symbol, name, signal, price_cents, change_pct, ma20_pct, ma60_pct, updated_at)
                   VALUES (?, ?, '', ?, ?, ?, ?, ?)""",
                (symbol, name, 310000, 0.8, ma20_pct, 4.0, now),
            )

        svc = MarketService(market_providers=[InvalidIndexProvider()], store=store)

        state, index_data = _run_async(
            svc.collect_market_state(run_id="run_invalid_live_index")
        )

        assert state.signal.value == "GREEN"
        assert state.multiplier == 1.0
        assert state.detail["reason"] == "指数数据源无有效数据，使用最近市场投影"
        assert state.detail["fallback_source"] == "projection_market_state"
        assert index_data["上证指数"]["symbol"] == "sh000001"

    def test_market_state_projection_query_quotes_signal_for_mysql_reserved_column(self):
        class InvalidIndexProvider:
            async def get_index(self, symbols):
                return {
                    symbol: IndexQuote(
                        symbol=symbol,
                        name=name,
                        price=0.0,
                        change_pct=1.0,
                        ma20=0.0,
                        ma60=0.0,
                        above_ma20=False,
                    )
                    for symbol, name in {
                        "sh000001": "上证指数",
                        "sz399001": "深证成指",
                        "sz399006": "创业板指",
                    }.items()
                }

        class FakeMySQLProjectionConn:
            def __init__(self):
                self.queries: list[str] = []

            def execute(self, sql, params=None):
                self.queries.append(sql)
                if " signal," in sql and "`signal`" not in sql:
                    raise RuntimeError("SQL syntax error near reserved word signal")
                now = datetime.now(timezone.utc).isoformat()
                rows = [
                    {
                        "index_symbol": "sh000001",
                        "name": "上证指数",
                        "signal": "",
                        "price_cents": 411289,
                        "change_pct": 0.87,
                        "ma20_pct": -0.69,
                        "ma60_pct": 1.17,
                        "updated_at": now,
                    },
                    {
                        "index_symbol": "sz399001",
                        "name": "深证成指",
                        "signal": "",
                        "price_cents": 1559729,
                        "change_pct": 2.3,
                        "ma20_pct": 1.11,
                        "ma60_pct": 7.13,
                        "updated_at": now,
                    },
                    {
                        "index_symbol": "sz399006",
                        "name": "创业板指",
                        "signal": "",
                        "price_cents": 393850,
                        "change_pct": 2.84,
                        "ma20_pct": 3.03,
                        "ma60_pct": 12.57,
                        "updated_at": now,
                    },
                ]
                return SimpleNamespace(fetchall=lambda: rows)

        fake_conn = FakeMySQLProjectionConn()
        store = SimpleNamespace(_conn=fake_conn, get_cached=lambda *args, **kwargs: None)
        svc = MarketService(market_providers=[InvalidIndexProvider()], store=store)

        state, index_data = _run_async(svc.collect_market_state())

        assert state.signal.value == "GREEN"
        assert state.detail["fallback_source"] == "projection_market_state"
        assert index_data["深证成指"]["source"] == "projection_market_state"
        assert any("`signal`" in query for query in fake_conn.queries)

    def test_akshare_flow_tick_suppresses_download_warning(self, monkeypatch):
        from astock_trading.market.akshare_adapters import AkShareFlowAdapter

        def fake_tick(symbol):
            warnings.warn("正在下载数据，请稍等", UserWarning)
            return pd.DataFrame({
                "性质": ["买盘", "卖盘"],
                "成交金额": [100.0, 20.0],
            })

        monkeypatch.setitem(
            sys.modules,
            "akshare",
            SimpleNamespace(stock_zh_a_tick_tx_js=fake_tick),
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = AkShareFlowAdapter()._get_flow_from_tx_tick("000001")

        assert result is not None
        assert result.net_inflow_1d == 80.0
        assert not any("正在下载数据" in str(item.message) for item in caught)

    def test_mx_index_fallback_suppresses_akshare_progress_stderr(self, monkeypatch, capsys):
        from astock_trading.market.mx import realtime

        def fake_market_index_mx():
            return {
                "上证指数": {"price": 3100.0, "change_pct": 0.5},
                "深证成指": {"price": 10500.0, "change_pct": 0.8},
                "创业板指": {"price": 2200.0, "change_pct": 1.1},
                "科创50": {"price": 1000.0, "change_pct": 0.6},
            }

        class FakeAkshare:
            def stock_zh_index_daily(self, symbol):
                print("0%| mock akshare progress", file=sys.stderr)
                return pd.DataFrame({
                    "date": pd.date_range("2026-01-01", periods=80).astype(str),
                    "close": [1000.0 + i for i in range(80)],
                })

        monkeypatch.setattr(realtime, "get_market_index_mx", fake_market_index_mx)
        monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())

        result = MXMarketAdapter()._get_index_sync()

        captured = capsys.readouterr()
        assert result["上证指数"].price == 3100.0
        assert "mock akshare progress" not in captured.err

    def test_mx_kline_uses_akshare_when_mx_volume_is_missing(self, monkeypatch, capsys):
        dates = pd.date_range("2026-04-01", periods=30, freq="B").strftime("%Y-%m-%d")
        mx_df = pd.DataFrame({
            "date": dates,
            "open": [10 + i * 0.1 for i in range(30)],
            "close": [10.1 + i * 0.1 for i in range(30)],
            "high": [10.2 + i * 0.1 for i in range(30)],
            "low": [9.9 + i * 0.1 for i in range(30)],
            "volume": [0] * 30,
            "涨跌幅": [0] * 30,
        })
        ak_df = pd.DataFrame({
            "date": dates,
            "open": [10 + i * 0.1 for i in range(30)],
            "close": [10.1 + i * 0.1 for i in range(30)],
            "high": [10.2 + i * 0.1 for i in range(30)],
            "low": [9.9 + i * 0.1 for i in range(30)],
            "volume": [1_000_000 + i * 10_000 for i in range(30)],
            "amount": [10_000_000 + i * 100_000 for i in range(30)],
        })

        class FakeAkshare:
            def stock_zh_a_daily(self, symbol, adjust):
                print("0%| mock akshare progress", file=sys.stderr)
                assert symbol == "sh600066"
                assert adjust == "qfq"
                return ak_df

        adapter = MXMarketAdapter()
        monkeypatch.setattr(adapter, "_get_kline_from_mx", lambda code, count: mx_df)
        monkeypatch.setitem(sys.modules, "akshare", FakeAkshare())

        result = adapter._get_kline_sync("600066", "daily", 20)

        assert result is not None
        assert result["volume"].sum() > 0
        assert "amount" in result.columns
        captured = capsys.readouterr()
        assert "mock akshare progress" not in captured.err

    def test_akshare_flow_records_internal_subsource_errors(self, monkeypatch, store):
        from astock_trading.market.akshare_adapters import AkShareFlowAdapter

        class BrokenAkshare:
            def stock_individual_fund_flow(self, stock, market):
                raise RuntimeError("eastmoney fund flow unavailable")

            def stock_zh_a_tick_tx_js(self, symbol):
                raise ValueError("tencent tick unavailable")

        adapter = AkShareFlowAdapter()
        monkeypatch.setitem(sys.modules, "akshare", BrokenAkshare())
        svc = MarketService(flow_providers=[adapter], store=store)

        result = asyncio.get_event_loop().run_until_complete(
            svc._get_flow("000001", run_id="run_akshare_subsource_failure")
        )

        failure = store.get_latest_observation("000001", "provider_failure")
        diagnostic = failure["details"]["provider_diagnostic"]
        assert result is None
        assert failure["source"] == "AkShareFlowAdapter"
        assert failure["target_kind"] == "fund_flow"
        assert failure["status"] == "provider_error"
        assert diagnostic["error_type"] == "AkShareFlowSubsourcesFailed"
        assert [item["status"] for item in diagnostic["subsource_errors"]] == [
            "em_fund_flow_failed",
            "tx_tick_failed",
        ]
        assert diagnostic["subsource_errors"][0]["subsource"] == "eastmoney_fund_flow"
        assert diagnostic["subsource_errors"][1]["subsource"] == "tencent_tick"

    def test_baidu_fund_flow_parse_error_does_not_warn(self, caplog):
        from astock_trading.market.a_stock_adapters import BaiduFundFlowAdapter

        class Response:
            def json(self):
                raise json.JSONDecodeError("Expecting value", "", 0)

        adapter = BaiduFundFlowAdapter(request_get=lambda *args, **kwargs: Response())

        with caplog.at_level(logging.WARNING):
            rows = adapter._fund_flow_history("000001")

        assert rows == []
        assert adapter.get_last_error("000001")["status"] == "parse_error"
        assert not caplog.records

    def test_astock_signal_industry_fallback_does_not_warn(self, caplog):
        from astock_trading.market.a_stock_adapters import AStockSignalAdapter

        class BrokenAkshare:
            def stock_board_industry_name_em(self):
                raise RuntimeError("eastmoney unavailable")

        adapter = AStockSignalAdapter()
        adapter._akshare = lambda: BrokenAkshare()

        with caplog.at_level(logging.WARNING):
            rows = adapter._get_industry_comparison_em()

        assert rows == []
        assert not caplog.records

    def test_all_providers_fail(self, store):
        """All providers fail → snapshot with None fields."""
        svc = MarketService(
            market_providers=[FailingProvider()],
            financial_providers=[FailingProvider()],
            flow_providers=[FailingProvider()],
            sentiment_providers=[FailingProvider()],
            store=store,
        )

        snap = asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动")
        )

        assert snap.code == "002138"
        assert snap.quote is None
        assert snap.financial is None

    def test_collect_market_state_skips_provider_without_get_index(self):
        provider = TrackingIndexProvider()
        svc = MarketService(market_providers=[NoIndexProvider(), provider])

        state, index_data = asyncio.get_event_loop().run_until_complete(
            svc.collect_market_state("run_test")
        )

        assert state.signal.value in {"GREEN", "YELLOW", "RED", "CLEAR"}
        assert provider.index_calls == [["sh000001", "sz399001", "sz399006"]]
        assert index_data["上证指数"]["symbol"] == "sh000001"

    def test_observation_saved(self, store):
        svc = MarketService(
            market_providers=[MockMarketProvider({})],
            store=store,
        )

        asyncio.get_event_loop().run_until_complete(
            svc.collect_snapshot("002138", "双环传动", run_id="run_obs")
        )

        obs = store.get_latest_observation("002138", "snapshot")
        assert obs is not None

    def test_hk_technical_uses_only_hk_provider(self, store, monkeypatch):
        a_provider = TrackingAShareKlineProvider()
        hk_provider = TrackingHKKlineProvider()
        monkeypatch.setattr(
            market_service_module,
            "compute_technical_indicators",
            lambda kline, quote: TechnicalIndicators(ma20=81.4),
        )

        svc = MarketService(
            market_providers=[a_provider, hk_provider],
            store=store,
        )

        technical = asyncio.get_event_loop().run_until_complete(
            svc._get_technical("09927", None)
        )

        assert technical is not None
        assert technical.ma20 == 81.4
        assert a_provider.calls == []
        assert hk_provider.calls == ["09927"]

    def test_a_share_technical_skips_hk_provider(self, store, monkeypatch):
        a_provider = TrackingAShareKlineProvider()
        hk_provider = TrackingHKKlineProvider()
        monkeypatch.setattr(
            market_service_module,
            "compute_technical_indicators",
            lambda kline, quote: TechnicalIndicators(ma20=15.0),
        )

        svc = MarketService(
            market_providers=[hk_provider, a_provider],
            store=store,
        )

        technical = asyncio.get_event_loop().run_until_complete(
            svc._get_technical("600066", None)
        )

        assert technical is not None
        assert technical.ma20 == 15.0
        assert hk_provider.calls == []
        assert a_provider.calls == ["600066"]

    def test_technical_continues_when_first_kline_has_missing_volume(self, store):
        dates = pd.date_range("2026-04-01", periods=30, freq="B").strftime("%Y-%m-%d")
        price_only = pd.DataFrame({
            "date": dates,
            "open": [10 + i * 0.1 for i in range(30)],
            "close": [10.1 + i * 0.1 for i in range(30)],
            "high": [10.2 + i * 0.1 for i in range(30)],
            "low": [9.9 + i * 0.1 for i in range(30)],
            "volume": [0] * 30,
        })
        with_volume = price_only.copy()
        with_volume["volume"] = [1_000_000 + i * 50_000 for i in range(30)]

        first_provider = TrackingAShareKlineProvider(price_only)
        second_provider = TrackingAShareKlineProvider(with_volume)
        svc = MarketService(
            market_providers=[first_provider, second_provider],
            store=store,
        )

        technical = asyncio.get_event_loop().run_until_complete(
            svc._get_technical("600066", None)
        )

        assert technical is not None
        assert technical.volume_ratio > 0
        assert first_provider.calls == ["600066"]
        assert second_provider.calls == ["600066"]

    def test_hk_quote_falls_back_to_hk_kline_only(self, store):
        a_provider = TrackingAShareKlineProvider(
            pd.DataFrame([{"close": 88.9, "open": 88.0, "high": 89.5, "low": 87.8, "volume": 1, "amount": 1}])
        )
        hk_provider = TrackingHKKlineProvider(
            pd.DataFrame([{
                "date": "2026-04-16",
                "open": 80.2,
                "high": 82.0,
                "low": 79.8,
                "close": 81.4,
                "volume": 470600,
                "amount": 37750650,
                "涨跌幅": 1.12,
                "名称": "赛力斯(港股)",
            }])
        )

        svc = MarketService(
            market_providers=[a_provider, hk_provider],
            store=store,
        )

        quote = asyncio.get_event_loop().run_until_complete(
            svc._get_quote("09927")
        )

        assert quote is not None
        assert quote.close == 81.4
        assert quote.name == "赛力斯(港股)"
        assert a_provider.calls == []
        assert hk_provider.calls == ["09927"]


def test_opencli_xueqiu_adapter_normalizes_hot_stock_payload(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return subprocess.CompletedProcess(
            cmd,
            0,
            stdout=(
                '[{"rank":1,"symbol":"SZ300274","name":"阳光电源",'
                '"price":152.95,"changePercent":"13.27%","heat":2785}]'
            ),
            stderr="",
        )

    monkeypatch.setattr("astock_trading.market.adapters.shutil.which", lambda name: "/bin/opencli")
    monkeypatch.setattr("astock_trading.market.adapters.subprocess.run", fake_run)

    rows = asyncio.get_event_loop().run_until_complete(
        OpenCliXueqiuAdapter().get_xueqiu_hot_stocks(limit=5)
    )

    assert rows == [{
        "rank": 1,
        "symbol": "SZ300274",
        "code": "300274",
        "name": "阳光电源",
        "price": 152.95,
        "change_pct": 13.27,
        "heat": 2785,
        "heat_text": "2785",
        "tags": [],
        "url": "",
        "source": "xueqiu",
    }]
    assert calls[0][0][:4] == ["/bin/opencli", "xueqiu", "hot-stock", "--limit"]
    assert calls[0][1]["timeout"] == 45


def test_opencli_finance_adapter_aggregates_cross_platform_hot_stocks(monkeypatch):
    def fake_run(cmd, **kwargs):
        site = cmd[1]
        command = cmd[2]
        payloads = {
            ("xueqiu", "hot-stock"): (
                '[{"rank":1,"symbol":"SZ300274","name":"阳光电源",'
                '"price":152.95,"changePercent":"13.27%","heat":2785}]'
            ),
            ("eastmoney", "hot-rank"): (
                '[{"rank":4,"symbol":"300274","name":"阳光电源",'
                '"price":"152.95","changePercent":"13.27%","heat":"74.53% 25.47%"}]'
            ),
            ("sinafinance", "stock-rank"): (
                '[{"rank":"5","symbol":"sz300274","name":"阳光电源",'
                '"price":"152.95","change":"+13.27%","market":"A股"}]'
            ),
            ("ths", "hot-rank"): (
                '[{"rank":"8","name":"阳光电源","changePercent":"+13.27%",'
                '"heat":"12.1万热度","tags":"300274,光伏,储能"}]'
            ),
            ("tdx", "hot-rank"): (
                '[{"rank":1,"symbol":"002407","name":"多氟多","changePercent":"10.02%",'
                '"heat":"143.16万人气","tags":"锂电池概念"}]'
            ),
        }
        return subprocess.CompletedProcess(cmd, 0, stdout=payloads.get((site, command), "[]"), stderr="")

    monkeypatch.setattr("astock_trading.market.adapters.shutil.which", lambda name: "/bin/opencli")
    monkeypatch.setattr("astock_trading.market.adapters.subprocess.run", fake_run)

    rows = asyncio.get_event_loop().run_until_complete(
        OpenCliFinanceAdapter().get_cross_platform_hot_stocks(limit=5)
    )

    assert rows[0]["code"] == "300274"
    assert rows[0]["name"] == "阳光电源"
    assert rows[0]["source_count"] == 4
    assert rows[0]["best_rank"] == 1
    assert rows[0]["sources"] == ["xueqiu", "eastmoney", "sinafinance", "ths"]
    assert rows[0]["source_ranks"]["sinafinance"] == 5
    assert rows[1]["code"] == "002407"
    assert rows[1]["sources"] == ["tdx"]


def test_opencli_finance_adapter_collects_hot_sectors(monkeypatch):
    adapter = OpenCliFinanceAdapter()
    calls = []

    def fake_run(site, command, *positionals, options=None, browser=False, timeout_seconds=None):
        calls.append((site, command, options, browser))
        return [{
            "rank": "1",
            "code": "BK1234",
            "name": "机器人",
            "price": "1280.4",
            "changePercent": "3.21",
            "mainNet": "123456789",
            "leadStock": "双环传动",
            "leadChangePercent": "6.18",
            "upCount": "42",
            "downCount": "3",
        }]

    monkeypatch.setattr(adapter, "_run_opencli", fake_run)

    rows = asyncio.get_event_loop().run_until_complete(
        adapter.get_hot_sectors(limit=5, sector_type="concept", sort="money-flow")
    )

    assert calls == [("eastmoney", "sectors", {"--type": "concept", "--sort": "money-flow", "--limit": 5}, False)]
    assert rows == [{
        "rank": 1,
        "code": "BK1234",
        "name": "机器人",
        "price": 1280.4,
        "change_pct": 3.21,
        "main_net": 123456789.0,
        "lead_stock": "双环传动",
        "lead_change_pct": 6.18,
        "up_count": 42,
        "down_count": 3,
        "type": "concept",
        "sort": "money-flow",
        "source": "eastmoney",
    }]


def test_baostock_login_suppresses_stdout(monkeypatch, capsys):
    from astock_trading.market import baostock_adapters

    class LoginResult:
        error_code = "0"
        error_msg = ""

    class FakeBaostock:
        @staticmethod
        def login():
            print("login success!")
            return LoginResult()

    monkeypatch.setattr(baostock_adapters, "_bs_logged_in", False)
    monkeypatch.setitem(sys.modules, "baostock", FakeBaostock())

    try:
        baostock_adapters._bs_ensure_login()
        captured = capsys.readouterr()
        assert "login success" not in captured.out
    finally:
        monkeypatch.setattr(baostock_adapters, "_bs_logged_in", False)


def test_opencli_finance_adapter_searches_market_news(monkeypatch):
    adapter = OpenCliFinanceAdapter()

    def fake_run(site, command, *positionals, options=None, browser=False, timeout_seconds=None):
        if (site, command) == ("eastmoney", "kuaixun"):
            return [
                {"time": "09:01", "title": "机器人板块走强", "summary": "机器人概念早盘活跃"},
                {"time": "09:00", "title": "白酒板块调整", "summary": "消费股回落"},
            ]
        if (site, command) == ("sinafinance", "news"):
            return [
                {"id": "1", "time": "09:02", "content": "【机器人】机器人产业链延续强势", "views": "10"},
                {"id": "2", "time": "08:59", "content": "【地产】地产股震荡", "views": "20"},
            ]
        if (site, command) == ("reuters", "search"):
            return [{"rank": 1, "title": "Robotics stocks rally in China", "date": "2026-05-16", "url": "https://example.com"}]
        return []

    monkeypatch.setattr(adapter, "_run_opencli", fake_run)

    rows = asyncio.get_event_loop().run_until_complete(adapter.search_market_news("机器人", limit=5))

    assert [row["title"] for row in rows] == ["机器人", "机器人板块走强"]
    assert {row["source"] for row in rows} == {"sinafinance", "eastmoney"}


def test_format_market_signals_markdown():
    from astock_trading.reporting.market_formatters import format_market_signals_markdown

    lines = format_market_signals_markdown(
        hot_stocks=[{"name": "五粮液", "code": "000858", "change_pct": 5.5, "reason": "白酒+消费复苏"}],
        xueqiu_hot_stocks=[{"rank": 1, "name": "阳光电源", "code": "300274", "change_pct": 13.27, "heat": 2785}],
        cross_platform_hot_stocks=[{
            "rank": 1,
            "name": "阳光电源",
            "code": "300274",
            "change_pct": 13.27,
            "source_count": 3,
            "sources": ["xueqiu", "eastmoney", "sinafinance"],
        }],
        finance_flash=[{
            "time": "09:10",
            "title": "商务部回应关税安排",
            "summary": "中美经贸磋商形成积极共识，双方讨论有关产品降税安排。",
            "source": "sinafinance",
        }],
        global_risk_news=[{
            "title": "Fed rate cut expectations fade",
            "summary": "Treasury yields rise as inflation remains sticky.",
            "source": "bloomberg",
        }],
        market_announcements=[{"code": "603311", "name": "金海高科", "title": "复牌公告", "category": "复牌公告"}],
        northbound=[{"time": "15:00", "hgt_yi": 2.2, "sgt_yi": 0.4}],
        dragon_tiger={"stocks": [{"name": "五粮液", "code": "000858", "net_buy_wan": 12345.7, "reason": "偏离值"}]},
        lockup={"upcoming": [{"date": "2026-06-01", "type": "定增", "float_ratio": 0.5}]},
    )

    text = "\n".join(lines)
    assert "市场信号" in text
    assert "五粮液(000858)" in text
    assert "雪球热搜" in text
    assert "#1 阳光电源(300274)" in text
    assert "跨平台热度" in text
    assert "雪球/东财/新浪" in text
    assert "财经快讯" in text
    assert "海外风险" in text
    assert "公告提示" in text
    assert "影响: 宏观/出口链/人民币风险" in text
    assert "影响: 利率/成长股估值" in text
    assert "动作:" in text
    assert "北向资金" in text
    assert "龙虎榜" in text
    assert "解禁预警" in text
