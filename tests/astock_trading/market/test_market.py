"""Tests for market/store.py and market/service.py"""

import asyncio
import subprocess
import pandas as pd
import pytest

from astock_trading.market import service as market_service_module
from astock_trading.market.adapters import AkShareHKMarketAdapter, OpenCliFinanceAdapter, OpenCliXueqiuAdapter
from astock_trading.market.models import (
    FinancialReport,
    FundFlow,
    IndexQuote,
    SentimentData,
    StockQuote,
    TechnicalIndicators,
)
from astock_trading.market.store import MarketStore
from astock_trading.market.service import MarketService
from astock_trading.platform.db import init_db, connect


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def store(db):
    return MarketStore(db)


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
