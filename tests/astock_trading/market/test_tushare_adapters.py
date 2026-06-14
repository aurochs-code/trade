from __future__ import annotations

import asyncio


class FakeTushareClient:
    def __init__(self, responses: dict[str, list[dict]]):
        self.responses = responses
        self.calls: list[tuple[str, dict, str]] = []
        self.enabled = True
        self.token_present = True

    def query(self, api_name: str, *, params: dict | None = None, fields: str = "") -> list[dict]:
        self.calls.append((api_name, params or {}, fields))
        return self.responses.get(api_name, [])

    def pro_bar(self, *, ts_code: str, start_date: str, end_date: str, count: int) -> list[dict]:
        self.calls.append(("pro_bar", {
            "ts_code": ts_code,
            "start_date": start_date,
            "end_date": end_date,
            "count": count,
        }, ""))
        return self.responses.get("pro_bar", [])


def test_tushare_client_from_env_prefers_astock_token_and_redacts_value(monkeypatch):
    from astock_trading.market.tushare_adapters import TushareClient

    monkeypatch.setenv("ASTOCK_TUSHARE_TOKEN", "secret-primary-token")
    monkeypatch.setenv("TUSHARE_TOKEN", "secret-fallback-token")

    client = TushareClient.from_env()

    assert client.enabled is True
    diagnostic = client.diagnostic()
    assert diagnostic["token_present"] is True
    assert diagnostic["token_source"] == "ASTOCK_TUSHARE_TOKEN"
    assert "moneyflow" in diagnostic["configured_regular_interfaces"]
    assert "pro_bar" in diagnostic["configured_regular_interfaces"]
    assert "minute" in diagnostic["not_assumed_interfaces"]
    assert "secret-primary-token" not in str(diagnostic)
    assert "secret-fallback-token" not in str(diagnostic)


def test_tushare_client_uses_official_sdk_query_and_pro_bar():
    import pandas as pd

    from astock_trading.market.tushare_adapters import TushareClient

    class FakePro:
        def __init__(self):
            self.calls = []

        def query(self, api_name, fields="", **params):
            self.calls.append((api_name, fields, params))
            return pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.5}])

    class FakeSDK:
        def __init__(self):
            self.pro = FakePro()
            self.pro_api_token = ""
            self.pro_bar_calls = []

        def pro_api(self, token):
            self.pro_api_token = token
            return self.pro

        def pro_bar(self, **kwargs):
            self.pro_bar_calls.append(kwargs)
            return pd.DataFrame([{"ts_code": "000001.SZ", "close": 10.5}])

    sdk = FakeSDK()
    client = TushareClient("secret-sdk-token", token_source="test", sdk_module=sdk)

    rows = client.query("daily", params={"ts_code": "000001.SZ"}, fields="ts_code,close")
    bars = client.pro_bar(ts_code="000001.SZ", start_date="20260601", end_date="20260612", count=20)

    assert sdk.pro_api_token == "secret-sdk-token"
    assert sdk.pro.calls == [("daily", "ts_code,close", {"ts_code": "000001.SZ"})]
    assert sdk.pro_bar_calls[0]["ts_code"] == "000001.SZ"
    assert rows == [{"ts_code": "000001.SZ", "close": 10.5}]
    assert bars == [{"ts_code": "000001.SZ", "close": 10.5}]


def test_tushare_market_adapter_maps_daily_data_to_quote_and_kline():
    from astock_trading.market.tushare_adapters import TushareMarketAdapter

    adapter = TushareMarketAdapter(client=FakeTushareClient({
        "daily": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260612",
                "open": 10.0,
                "high": 10.8,
                "low": 9.8,
                "close": 10.5,
                "pct_chg": 2.3,
                "vol": 1234.0,
                "amount": 5678.0,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260611",
                "open": 9.9,
                "high": 10.1,
                "low": 9.7,
                "close": 10.0,
                "pct_chg": 0.5,
                "vol": 1000.0,
                "amount": 4000.0,
            },
        ],
        "pro_bar": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260611",
                "open": 9.9,
                "high": 10.1,
                "low": 9.7,
                "close": 10.0,
                "pct_chg": 0.5,
                "vol": 1000.0,
                "amount": 4000.0,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260612",
                "open": 10.0,
                "high": 10.8,
                "low": 9.8,
                "close": 10.5,
                "pct_chg": 2.3,
                "vol": 1234.0,
                "amount": 5678.0,
            },
        ],
    }))

    quotes = asyncio.run(adapter.get_realtime(["000001"]))
    kline = asyncio.run(adapter.get_kline("000001", "daily", 2))

    assert quotes["000001"].price == 10.5
    assert quotes["000001"].amount == 5_678_000.0
    assert list(kline["close"]) == [10.0, 10.5]
    assert list(kline["volume"]) == [100_000, 123_400]


def test_tushare_market_adapter_fetches_trade_calendar_and_full_market_daily():
    from astock_trading.market.tushare_adapters import TushareMarketAdapter

    client = FakeTushareClient({
        "trade_cal": [
            {"cal_date": "20260611"},
            {"cal_date": "20260612"},
        ],
        "daily": [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20260612",
                "open": 10.0,
                "high": 10.8,
                "low": 9.8,
                "close": 10.5,
                "pct_chg": 2.3,
                "vol": 1234.0,
                "amount": 5678.0,
            },
            {
                "ts_code": "600036.SH",
                "trade_date": "20260612",
                "open": 35.0,
                "high": 36.0,
                "low": 34.5,
                "close": 35.8,
                "pct_chg": 1.7,
                "vol": 4321.0,
                "amount": 8765.0,
            },
        ],
    })
    adapter = TushareMarketAdapter(client=client)

    dates = asyncio.run(adapter.get_trade_dates("2026-06-11", "2026-06-12"))
    bars = asyncio.run(adapter.get_daily_market_bars("2026-06-12"))

    assert dates == ["20260611", "20260612"]
    assert list(bars["symbol"]) == ["000001", "600036"]
    assert list(bars["date"]) == ["20260612", "20260612"]
    assert list(bars["close"]) == [10.5, 35.8]
    assert list(bars["volume"]) == [123_400, 432_100]
    assert [call[0] for call in client.calls] == ["trade_cal", "daily"]
    assert client.calls[0][1]["is_open"] == "1"
    assert client.calls[1][1]["trade_date"] == "20260612"


def test_tushare_financial_and_flow_adapters_use_regular_6000_point_interfaces():
    from astock_trading.market.tushare_adapters import TushareFinancialAdapter, TushareFlowAdapter

    client = FakeTushareClient({
        "daily_basic": [
            {"trade_date": "20260612", "pe_ttm": 18.5, "pb": 2.1},
        ],
        "fina_indicator": [
            {"end_date": "20230331", "roe": 6.1},
            {
                "end_date": "20260331",
                "roe": 12.3,
                "or_yoy": 16.8,
                "netprofit_yoy": 21.5,
                "ocf_to_or": 0.17,
                "debt_to_assets": 42.0,
            },
        ],
        "moneyflow": [
            {"trade_date": "20260612", "net_mf_amount": 1200.0},
            {"trade_date": "20260611", "net_mf_amount": -200.0},
            {"trade_date": "20260610", "net_mf_amount": 300.0},
        ],
    })

    financial = asyncio.run(TushareFinancialAdapter(client=client).get_financial("000001"))
    flow = asyncio.run(TushareFlowAdapter(client=client).get_fund_flow("000001", days=3))
    default_flow = asyncio.run(TushareFlowAdapter(client=client).get_fund_flow("000001"))

    assert financial.roe == 12.3
    assert financial.roe_3y_ago == 6.1
    assert financial.revenue_growth == 16.8
    assert financial.net_profit_growth == 21.5
    assert financial.operating_cash_flow == 0.17
    assert financial.pe_ttm == 18.5
    assert financial.pb == 2.1
    assert financial.debt_ratio == 42.0
    assert flow.net_inflow_1d == 12_000_000.0
    assert flow.net_inflow_5d == 13_000_000.0
    assert flow.main_force_ratio > 0
    assert default_flow.net_inflow_1d == 12_000_000.0
    assert [call[0] for call in client.calls] == [
        "daily_basic",
        "fina_indicator",
        "moneyflow",
        "moneyflow",
    ]


def test_tushare_market_adapter_uses_regular_signal_interfaces():
    from astock_trading.market.tushare_adapters import TushareMarketAdapter

    adapter = TushareMarketAdapter(client=FakeTushareClient({
        "stock_basic": [
            {
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "name": "平安银行",
                "area": "深圳",
                "industry": "银行",
                "market": "主板",
                "list_date": "19910403",
            }
        ],
        "top_list": [
            {
                "trade_date": "20260612",
                "ts_code": "000001.SZ",
                "name": "平安银行",
                "close": 10.5,
                "pct_change": 2.1,
                "turnover_rate": 3.2,
                "amount": 3500.0,
                "net_amount": 680.0,
                "reason": "日涨幅偏离值达7%",
            }
        ],
        "share_float": [
            {
                "ts_code": "000001.SZ",
                "float_date": "20260715",
                "float_share": 1500.0,
                "float_ratio": 0.8,
                "holder_name": "测试股东",
                "share_type": "首发原股东限售股份",
            }
        ],
        "hk_hold": [
            {
                "trade_date": "20260612",
                "ts_code": "000001.SZ",
                "name": "平安银行",
                "vol": 1000000,
                "ratio": 2.5,
                "exchange": "深股通",
            }
        ],
    }))

    basic = asyncio.run(adapter.get_basic_info("000001"))
    daily_lhb = asyncio.run(adapter.get_daily_dragon_tiger("2026-06-12"))
    lhb = asyncio.run(adapter.get_dragon_tiger("000001", "2026-06-12", look_back=5))
    lockup = asyncio.run(adapter.get_lockup_expiry("000001", "2026-06-12", forward_days=45))
    northbound = asyncio.run(adapter.get_northbound_realtime())

    assert basic["name"] == "平安银行"
    assert daily_lhb["total_records"] == 1
    assert daily_lhb["stocks"][0]["net_buy_wan"] == 680.0
    assert lhb["records"][0]["code"] == "000001"
    assert lockup["upcoming"][0]["float_date"] == "2026-07-15"
    assert northbound[0]["exchange"] == "深股通"
