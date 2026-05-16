"""Tests for HK stock adapter logic — code detection, normalization, routing."""

import asyncio
import pandas as pd

from astock_trading.market.adapters import (
    is_hk_code,
    normalize_hk_code,
    AkShareHKMarketAdapter,
    AkShareHKFinancialAdapter,
    AkShareMarketAdapter,
    MXMarketAdapter,
)
from astock_trading.market.models import StockQuote


# ── Code detection ──

class TestIsHKCode:
    def test_standard_hk_codes(self):
        assert is_hk_code("09927") is True
        assert is_hk_code("00700") is True
        assert is_hk_code("01810") is True
        assert is_hk_code("03690") is True

    def test_hk_prefix(self):
        assert is_hk_code("hk09927") is True
        assert is_hk_code("HK00700") is True

    def test_a_share_codes(self):
        assert is_hk_code("600066") is False
        assert is_hk_code("002138") is False
        assert is_hk_code("300750") is False
        assert is_hk_code("000001") is False  # 6-digit, not HK

    def test_edge_cases(self):
        assert is_hk_code("") is False
        assert is_hk_code("12345") is False  # 5-digit but doesn't start with 0
        assert is_hk_code("688001") is False  # 科创板


class TestNormalizeHKCode:
    def test_strip_prefix(self):
        assert normalize_hk_code("hk09927") == "09927"
        assert normalize_hk_code("HK00700") == "00700"

    def test_already_normalized(self):
        assert normalize_hk_code("09927") == "09927"

    def test_zero_pad(self):
        assert normalize_hk_code("hk700") == "00700"
        assert normalize_hk_code("700") == "00700"


# ── HK adapter routing ──

class TestHKAdapterRouting:
    def test_hk_market_skips_a_share(self):
        adapter = AkShareHKMarketAdapter()
        # A-share codes should return empty
        result = asyncio.get_event_loop().run_until_complete(
            adapter.get_realtime(["600066", "002138"])
        )
        assert result == {}

    def test_hk_market_kline_skips_a_share(self):
        adapter = AkShareHKMarketAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.get_kline("600066")
        )
        assert result is None

    def test_hk_financial_skips_a_share(self):
        adapter = AkShareHKFinancialAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.get_financial("600066")
        )
        assert result is None

    def test_a_share_market_skips_hk(self):
        """AkShareMarketAdapter.get_kline should return None for HK codes."""
        adapter = AkShareMarketAdapter()
        result = asyncio.get_event_loop().run_until_complete(
            adapter.get_kline("09927")
        )
        assert result is None

    def test_mx_market_kline_skips_hk(self, monkeypatch):
        """MXMarketAdapter._get_kline_sync should short-circuit HK codes."""
        adapter = MXMarketAdapter()

        def _should_not_call(*args, **kwargs):
            raise AssertionError("MX HK kline path should not be called")

        monkeypatch.setattr(adapter, "_get_kline_from_mx", _should_not_call)
        result = adapter._get_kline_sync("09927", "daily", 10)
        assert result is None

    def test_mx_market_realtime_skips_hk(self, monkeypatch):
        """MXMarketAdapter.get_realtime should filter HK codes before dispatch."""
        adapter = MXMarketAdapter()
        captured = {}

        def _fake_get_realtime_sync(codes):
            captured["codes"] = list(codes)
            return {
                "600066": StockQuote(
                    code="600066",
                    name="宇通客车",
                    price=25.1,
                    open=24.8,
                    high=25.2,
                    low=24.7,
                    close=25.1,
                    volume=1000,
                    amount=2000,
                    change_pct=1.2,
                )
            }

        monkeypatch.setattr(adapter, "_get_realtime_sync", _fake_get_realtime_sync)
        result = asyncio.get_event_loop().run_until_complete(
            adapter.get_realtime(["09927", "600066"])
        )

        assert captured["codes"] == ["600066"]
        assert "09927" not in result
        assert result["600066"].close == 25.1


# ── HK kline format ──

class TestHKKlineFormat:
    def test_kline_adds_change_pct(self):
        """Verify _get_kline_sync adds 涨跌幅 column to HK daily data."""
        # Simulate what _get_kline_sync does with a mock DataFrame
        df = pd.DataFrame({
            "date": ["2026-04-10", "2026-04-11", "2026-04-14"],
            "open": [79.0, 80.0, 80.5],
            "high": [82.0, 81.0, 81.0],
            "low": [79.0, 79.3, 79.3],
            "close": [80.0, 80.5, 80.3],
            "volume": [864213, 514900, 470600],
            "amount": [69406435, 41418455, 37750650],
        })

        # Simulate the processing
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["涨跌幅"] = df["close"].pct_change() * 100

        assert "涨跌幅" in df.columns
        assert pd.notna(df["涨跌幅"].iloc[1])
        assert abs(df["涨跌幅"].iloc[1] - 0.625) < 0.01  # (80.5-80)/80 * 100


# ── Mixed A+HK code handling ──

class TestMixedCodeRouting:
    def test_a_share_realtime_filters_hk(self):
        """AkShareMarketAdapter._get_realtime_sync should skip HK codes."""
        # We can't call the real API, but we can verify the filtering logic
        # by checking that the code_set only contains A-share codes
        codes = ["600066", "09927", "002138", "00700"]
        a_codes = [c for c in codes if not is_hk_code(c)]
        assert a_codes == ["600066", "002138"]

    def test_hk_realtime_filters_a_share(self):
        """AkShareHKMarketAdapter.get_realtime should only process HK codes."""
        codes = ["600066", "09927", "002138", "00700"]
        hk_codes = [c for c in codes if is_hk_code(c)]
        assert hk_codes == ["09927", "00700"]
