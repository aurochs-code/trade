"""Tests for adapters sourced from a-stock-data endpoint patterns."""

from __future__ import annotations

import asyncio
import sys

import pandas as pd
import pytest


class FakeTencentResponse:
    def __init__(self, text: str):
        self._text = text

    def read(self) -> bytes:
        return self._text.encode("gbk")


def test_tencent_financial_adapter_maps_pe_and_pb(monkeypatch):
    from astock_trading.market.adapters import TencentFinancialAdapter

    fields = [""] * 60
    fields[1] = "绿的谐波"
    fields[3] = "224.12"
    fields[39] = "300.45"
    fields[46] = "11.51"
    payload = 'v_sh688017="' + "~".join(fields) + '";'

    def fake_urlopen(req, timeout=10):
        assert "qt.gtimg.cn" in req.full_url
        assert timeout == 10
        return FakeTencentResponse(payload)

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    adapter = TencentFinancialAdapter()
    report = asyncio.get_event_loop().run_until_complete(adapter.get_financial("688017"))

    assert report is not None
    assert report.pe_ttm == 300.45
    assert report.pb == 11.51


def test_akshare_financial_adapter_maps_scoring_fields(monkeypatch):
    import akshare as ak

    from astock_trading.market.adapters import AkShareFinancialAdapter

    monkeypatch.setattr(
        ak,
        "stock_financial_analysis_indicator",
        lambda symbol, start_year: pd.DataFrame([
            {
                "日期": "2026-03-31",
                "净资产收益率(%)": "12.5",
                "主营业务收入增长率(%)": "18.6",
                "每股经营性现金流(元)": "0.91",
            },
            {
                "日期": "2023-03-31",
                "净资产收益率(%)": "6.4",
                "主营业务收入增长率(%)": "4.2",
                "每股经营性现金流(元)": "0.31",
            },
        ]),
    )

    adapter = AkShareFinancialAdapter()
    report = asyncio.get_event_loop().run_until_complete(adapter.get_financial("002384"))

    assert report is not None
    assert report.roe == 12.5
    assert report.roe_3y_ago == 6.4
    assert report.revenue_growth == 18.6
    assert report.operating_cash_flow == 0.91


def test_akshare_financial_adapter_uses_ths_summary_fallback(monkeypatch):
    import akshare as ak

    from astock_trading.market.adapters import AkShareFinancialAdapter

    monkeypatch.setattr(
        ak,
        "stock_financial_analysis_indicator",
        lambda symbol, start_year: pd.DataFrame([{"日期": "2026-03-31"}]),
    )
    monkeypatch.setattr(
        ak,
        "stock_financial_abstract_ths",
        lambda symbol: pd.DataFrame([
            {
                "报告期": "2025-12-31",
                "营业总收入同比增长率": "9.5%",
                "每股经营现金流": "0.42",
                "净资产收益率": "8.1%",
            },
            {
                "报告期": "2026-03-31",
                "营业总收入同比增长率": "21.3%",
                "每股经营现金流": "0.88",
                "净资产收益率": "13.2%",
            },
        ]),
    )

    adapter = AkShareFinancialAdapter()
    report = asyncio.get_event_loop().run_until_complete(adapter.get_financial("002384"))

    assert report is not None
    assert report.roe == 13.2
    assert report.revenue_growth == 21.3
    assert report.operating_cash_flow == 0.88


def test_akshare_financial_adapter_suppresses_stderr_noise(monkeypatch, capfd):
    import akshare as ak

    from astock_trading.market.adapters import AkShareFinancialAdapter

    def noisy_financial_indicator(symbol, start_year):
        print("正在下载数据，请稍等", file=sys.stderr)
        return pd.DataFrame([{
            "净资产收益率(%)": "12.5",
            "主营业务收入增长率(%)": "18.6",
            "每股经营性现金流(元)": "0.91",
        }])

    monkeypatch.setattr(ak, "stock_financial_analysis_indicator", noisy_financial_indicator)

    adapter = AkShareFinancialAdapter()
    report = asyncio.get_event_loop().run_until_complete(adapter.get_financial("002384"))

    assert report is not None
    assert capfd.readouterr().err == ""


def test_akshare_flow_adapter_suppresses_stderr_noise(monkeypatch, capfd):
    import akshare as ak

    from astock_trading.market.adapters import AkShareFlowAdapter

    def noisy_fund_flow(stock, market):
        print("正在下载数据，请稍等", file=sys.stderr)
        return pd.DataFrame([
            {"日期": "2026-05-20", "主力净流入-净额": 1_000_000},
            {"日期": "2026-05-21", "主力净流入-净额": -2_000_000},
        ])

    monkeypatch.setattr(ak, "stock_individual_fund_flow", noisy_fund_flow)

    adapter = AkShareFlowAdapter()
    flow = asyncio.get_event_loop().run_until_complete(adapter.get_fund_flow("002384", days=2))

    assert flow is not None
    assert flow.net_inflow_1d == -1_000_000
    assert capfd.readouterr().err == ""


class FakeMootdxClient:
    def quotes(self, symbol):
        assert symbol == ["688017"]
        return pd.DataFrame([{
            "code": "688017",
            "name": "绿的谐波",
            "price": 224.12,
            "open": 214.10,
            "high": 229.62,
            "low": 214.10,
            "last_close": 215.01,
            "vol": 100000,
            "amount": 1870400000.0,
        }])

    def bars(self, symbol, category, offset):
        assert symbol == "688017"
        assert category == 4
        assert offset == 3
        return pd.DataFrame([
            {"datetime": "2026-05-11", "open": 10.0, "close": 10.5, "high": 10.8, "low": 9.9, "vol": 100, "amount": 1050},
            {"datetime": "2026-05-12", "open": 10.5, "close": 10.8, "high": 11.0, "low": 10.4, "vol": 120, "amount": 1296},
            {"datetime": "2026-05-13", "open": 10.8, "close": 11.0, "high": 11.2, "low": 10.7, "vol": 130, "amount": 1430},
        ])


def test_mootdx_market_adapter_maps_realtime_and_kline():
    from astock_trading.market.adapters import MootdxMarketAdapter

    adapter = MootdxMarketAdapter(client_factory=lambda: FakeMootdxClient())

    quotes = asyncio.get_event_loop().run_until_complete(adapter.get_realtime(["688017"]))
    kline = asyncio.get_event_loop().run_until_complete(adapter.get_kline("688017", "daily", 3))

    quote = quotes["688017"]
    assert quote.name == "绿的谐波"
    assert quote.price == 224.12
    assert quote.change_pct == 4.24
    assert quote.volume == 100000
    assert quote.amount == 1870400000.0

    assert list(kline.columns) == ["date", "open", "close", "high", "low", "volume", "amount", "pct_change"]
    assert kline["close"].tolist() == [10.5, 10.8, 11.0]
    assert kline["pct_change"].iloc[0] == 0


class FakeJsonResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def test_baidu_flow_adapter_maps_history_to_fund_flow():
    from astock_trading.market.adapters import BaiduFundFlowAdapter

    calls = []

    def fake_get(url, headers=None, timeout=10):
        calls.append(url)
        return FakeJsonResponse({
            "ResultCode": "0",
            "Result": {
                "list": [
                    {"showtime": "2026-05-13", "extMainIn": "-100", "superNetIn": "-50"},
                    {"showtime": "2026-05-14", "extMainIn": "200", "superNetIn": "80"},
                    {"showtime": "2026-05-15", "extMainIn": "300", "superNetIn": "120"},
                ]
            },
        })

    adapter = BaiduFundFlowAdapter(request_get=fake_get)
    flow = asyncio.get_event_loop().run_until_complete(adapter.get_fund_flow("000858", days=3))

    assert flow is not None
    assert "fundsortlist" in calls[0]
    assert flow.net_inflow_1d == 3_000_000
    assert flow.net_inflow_5d == 4_000_000
    assert flow.main_force_ratio == pytest.approx(2.5)
    assert flow.consecutive_outflow_days == 0


def test_signal_adapter_maps_http_endpoints():
    from astock_trading.market.adapters import AStockSignalAdapter

    def fake_get(url, params=None, headers=None, timeout=10):
        if "getharden" in url:
            return FakeJsonResponse({
                "errocode": 0,
                "data": [{
                    "code": "000858", "name": "五粮液", "reason": "白酒+消费复苏",
                    "zhangfu": "5.5", "close": "151.2", "huanshou": "2.3",
                }],
            })
        if "getrelatedblock" in url:
            return FakeJsonResponse({
                "ResultCode": 0,
                "Result": [
                    {"type": "行业", "list": [{"name": "白酒", "increase": "1.2", "desc": "食品饮料"}]},
                    {"type": "概念", "list": [{"name": "高端消费", "increase": "2.5", "desc": ""}]},
                    {"type": "地域", "list": [{"name": "四川", "increase": "0.8", "desc": ""}]},
                ],
            })
        if "dayChart" in url:
            return FakeJsonResponse({"time": ["09:30", "15:00"], "hgt": [1.1, 2.2], "sgt": [-0.5, 0.4]})
        if "datacenter-web.eastmoney.com" in url:
            return FakeJsonResponse({
                "success": True,
                "result": {"data": [{
                    "TRADE_DATE": "2026-05-15 00:00:00",
                    "SECURITY_CODE": "000858",
                    "SECURITY_NAME_ABBR": "五粮液",
                    "EXPLANATION": "日涨幅偏离值达7%",
                    "CLOSE_PRICE": 151.2,
                    "CHANGE_RATE": 7.21,
                    "BILLBOARD_NET_AMT": 123456789,
                    "BILLBOARD_BUY_AMT": 223456789,
                    "BILLBOARD_SELL_AMT": 100000000,
                    "TURNOVERRATE": 9.87,
                }]},
            })
        raise AssertionError(url)

    adapter = AStockSignalAdapter(request_get=fake_get)

    hot = asyncio.get_event_loop().run_until_complete(adapter.get_hot_stocks("2026-05-15"))
    concepts = asyncio.get_event_loop().run_until_complete(adapter.get_concept_blocks("000858"))
    northbound = asyncio.get_event_loop().run_until_complete(adapter.get_northbound_realtime())
    dragon = asyncio.get_event_loop().run_until_complete(adapter.get_daily_dragon_tiger("2026-05-15"))

    assert hot[0]["reason_tags"] == ["白酒", "消费复苏"]
    assert concepts["concept_tags"] == ["高端消费"]
    assert northbound[-1] == {"time": "15:00", "hgt_yi": 2.2, "sgt_yi": 0.4}
    assert dragon["stocks"][0]["net_buy_wan"] == 12345.7


class FakeAkModule:
    def stock_lhb_detail_em(self, start_date, end_date):
        return pd.DataFrame([{"日期": "2026-05-15", "代码": "000858", "解读": "上榜", "龙虎榜净买额": 100, "换手率": 8.8}])

    def stock_lhb_stock_detail_em(self, symbol, date, flag):
        return pd.DataFrame([{"营业部名称": f"{flag}营业部", "买入额": 10, "卖出额": 3, "净额": 7}])

    def stock_lhb_jgmmtj_em(self, symbol):
        return pd.DataFrame([{"买入机构数": 2, "卖出机构数": 1, "机构净买入额": 9}])

    def stock_restricted_release_queue_em(self, symbol):
        return pd.DataFrame([{"解禁时间": "2025-01-01", "限售股类型": "首发", "解禁数量": 1000, "实际解禁市值占总市值比例": 1.2}])

    def stock_restricted_release_detail_em(self, date):
        return pd.DataFrame([{"股票代码": "000858", "解禁日期": "2026-06-01", "限售股类型": "定增", "解禁数量": 2000, "占流通股比例": 0.5}])

    def stock_board_industry_summary_ths(self):
        return pd.DataFrame([
            {"板块": "白酒", "涨跌幅": 2.1, "总成交额": 100, "净流入": 8, "上涨家数": 10, "下跌家数": 2, "领涨股": "五粮液"},
            {"板块": "银行", "涨跌幅": -1.1, "总成交额": 80, "净流入": -3, "上涨家数": 3, "下跌家数": 9, "领涨股": "平安银行"},
        ])

    def stock_zh_a_disclosure_report_cninfo(self, symbol, market):
        return pd.DataFrame([{"公告标题": "年度报告", "公告类型": "定期报告", "公告日期": "2026-05-15", "公告链接": "https://example.com/a.pdf"}])


def test_signal_adapter_maps_akshare_endpoints():
    from astock_trading.market.adapters import AStockSignalAdapter

    adapter = AStockSignalAdapter(ak_module=FakeAkModule())

    lhb = asyncio.get_event_loop().run_until_complete(adapter.get_dragon_tiger("000858", "2026-05-15"))
    lockup = asyncio.get_event_loop().run_until_complete(adapter.get_lockup_expiry("000858", "2026-05-15"))
    industry = asyncio.get_event_loop().run_until_complete(adapter.get_industry_comparison(top_n=1))
    announcements = asyncio.get_event_loop().run_until_complete(adapter.get_announcements("000858"))

    assert lhb["records"][0]["reason"] == "上榜"
    assert lhb["seats"]["buy"][0]["name"] == "买入营业部"
    assert lockup["upcoming"][0]["type"] == "定增"
    assert industry["top"][0]["name"] == "白酒"
    assert industry["bottom"][0]["name"] == "银行"
    assert announcements[0]["title"] == "年度报告"


def test_signal_adapter_prefers_eastmoney_industry_comparison():
    from astock_trading.market.adapters import AStockSignalAdapter

    class FakeEastmoneyIndustryAk:
        def stock_board_industry_name_em(self):
            return pd.DataFrame([
                {"板块名称": "机器人", "涨跌幅": "3.2", "成交额": 2500000000, "上涨家数": 35, "下跌家数": 5, "领涨股票": "绿的谐波"},
                {"板块名称": "银行", "涨跌幅": "-0.8", "成交额": 1800000000, "上涨家数": 4, "下跌家数": 38, "领涨股票": "平安银行"},
            ])

    adapter = AStockSignalAdapter(ak_module=FakeEastmoneyIndustryAk())

    industry = asyncio.get_event_loop().run_until_complete(adapter.get_industry_comparison(top_n=1))

    assert industry["total"] == 2
    assert industry["top"][0]["name"] == "机器人"
    assert industry["top"][0]["turnover_yi"] == 25
    assert industry["top"][0]["leader"] == "绿的谐波"
    assert industry["bottom"][0]["name"] == "银行"


def test_signal_adapter_industry_comparison_suppresses_stderr_noise(capsys):
    from astock_trading.market.adapters import AStockSignalAdapter

    class NoisyIndustryAk:
        def stock_board_industry_name_em(self):
            print("0%| mock industry progress", file=sys.stderr)
            return pd.DataFrame([
                {"板块名称": "机器人", "涨跌幅": "3.2", "成交额": 2500000000, "上涨家数": 35, "下跌家数": 5, "领涨股票": "绿的谐波"},
            ])

    adapter = AStockSignalAdapter(ak_module=NoisyIndustryAk())

    industry = asyncio.get_event_loop().run_until_complete(adapter.get_industry_comparison(top_n=1))

    captured = capsys.readouterr()
    assert industry["top"][0]["name"] == "机器人"
    assert "mock industry progress" not in captured.err


def test_signal_adapter_announcements_suppresses_stderr_noise(capsys):
    from astock_trading.market.adapters import AStockSignalAdapter

    class NoisyAnnouncementsAk:
        def stock_zh_a_disclosure_report_cninfo(self, symbol, market):
            print("0%| mock announcements progress", file=sys.stderr)
            return pd.DataFrame([
                {
                    "公告标题": "复牌公告",
                    "公告类型": "交易提示",
                    "公告日期": "2026-05-22",
                    "公告链接": "https://example.com",
                }
            ])

    adapter = AStockSignalAdapter(ak_module=NoisyAnnouncementsAk())

    rows = asyncio.get_event_loop().run_until_complete(adapter.get_announcements("000858", limit=1))

    captured = capsys.readouterr()
    assert rows[0]["title"] == "复牌公告"
    assert "mock announcements progress" not in captured.err
