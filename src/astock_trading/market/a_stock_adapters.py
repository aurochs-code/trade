"""A-share adapters backed by Tencent, Baidu, THS, Eastmoney, and mootdx."""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import Optional

import pandas as pd

from astock_trading.market.models import FinancialReport, FundFlow, IndexQuote, StockQuote
from astock_trading.platform.time import local_today

from .adapter_utils import (
    _a_stock_prefix,
    _normalize_a_stock_code,
    _to_float,
    _to_int,
    is_hk_code,
)

_logger = logging.getLogger(__name__)

class TencentFinancialAdapter:
    """腾讯财经估值 adapter（PE/PB）。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        import urllib.request

        normalized = _normalize_a_stock_code(code)
        try:
            req = urllib.request.Request(
                f"https://qt.gtimg.cn/q={_a_stock_prefix(normalized)}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp = urllib.request.urlopen(req, timeout=10)
            parsed = _parse_tencent_quote_payload(resp.read().decode("gbk", errors="ignore"))
            item = parsed.get(normalized)
            if not item:
                return None
            return FinancialReport(pe_ttm=item.get("pe_ttm"), pb=item.get("pb"))
        except Exception as e:
            _logger.warning(f"[TencentFinancial] {code} 获取失败: {e}")
            return None


_BAIDU_PAE_HEADERS = {
    "Host": "finance.pae.baidu.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/117.0.0.0",
    "Accept": "application/vnd.finance-web.v1+json",
    "Origin": "https://gushitong.baidu.com",
    "Referer": "https://gushitong.baidu.com/",
}


_THS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "Chrome/117.0.0.0 Safari/537.36"
    )
}


_HSGT_HEADERS = {
    **_THS_HEADERS,
    "Host": "data.hexin.cn",
    "Referer": "https://data.hexin.cn/",
}


class BaiduFundFlowAdapter:
    """百度股市通资金流向 adapter。"""

    def __init__(self, request_get=None):
        self._request_get = request_get
        self._last_errors: dict[str, dict] = {}

    async def get_fund_flow(self, code: str, days: int = 5) -> Optional[FundFlow]:
        if is_hk_code(code):
            return None
        self._last_errors.pop(code, None)
        return await asyncio.to_thread(self._get_fund_flow_sync, code, days)

    def get_last_error(self, code: str) -> dict | None:
        return self._last_errors.get(code)

    def _get(self, url: str, headers: dict, timeout: int = 10):
        if self._request_get is not None:
            return self._request_get(url, headers=headers, timeout=timeout)
        import requests
        return requests.get(url, headers=headers, timeout=timeout)

    def _get_fund_flow_sync(self, code: str, days: int) -> Optional[FundFlow]:
        rows = self._fund_flow_history(code, days)
        if not rows:
            return None

        recent = sorted(rows, key=lambda r: str(r.get("date", "")), reverse=True)[:days]
        main_values = [_to_float(r.get("mainIn")) for r in recent]
        super_values = [_to_float(r.get("superNetIn")) for r in recent]
        latest_main = main_values[0] if main_values else 0.0
        total_main = sum(main_values)
        latest_super = super_values[0] if super_values else 0.0
        main_force_ratio = latest_main / latest_super if latest_super else 0.0

        outflow_streak = 0
        for value in main_values:
            if value < 0:
                outflow_streak += 1
            else:
                break

        return FundFlow(
            net_inflow_1d=latest_main * 10000,
            net_inflow_5d=total_main * 10000,
            main_force_ratio=main_force_ratio,
            consecutive_outflow_days=outflow_streak,
        )

    def _fund_flow_history(self, code: str, days: int = 20) -> list[dict]:
        url = (
            f"https://finance.pae.baidu.com/vapi/v1/fundsortlist"
            f"?code={_normalize_a_stock_code(code)}&market=ab&pn=0&rn={days}"
            f"&finClientType=pc"
        )
        try:
            d = self._get(url, headers=_BAIDU_PAE_HEADERS, timeout=10).json()
            result_code = str(d.get("ResultCode", -1))
            if result_code != "0":
                self._last_errors[code] = {
                    "status": "provider_error",
                    "error_type": "ResultCode",
                    "error_message": f"ResultCode={result_code}",
                }
                return []
            result = d.get("Result") or {}
            return [{
                "date": item.get("showtime", ""),
                "close": item.get("closepx", ""),
                "change_pct": item.get("ratio", ""),
                "superNetIn": item.get("superNetIn", ""),
                "largeNetIn": item.get("largeNetIn", ""),
                "mediumNetIn": item.get("mediumNetIn", ""),
                "littleNetIn": item.get("littleNetIn", ""),
                "mainIn": item.get("extMainIn", ""),
            } for item in result.get("list", [])]
        except Exception as e:
            self._last_errors[code] = {
                "status": "parse_error" if "Expecting value" in str(e) else "provider_error",
                "error_type": e.__class__.__name__,
                "error_message": str(e),
            }
            _logger.debug(f"[BaiduFundFlow] {code} 历史资金流获取失败: {e}")
            return []

    def get_fund_flow_realtime_sync(self, code: str, trade_date: str) -> list[dict]:
        compact = trade_date.replace("-", "")
        url = (
            f"https://finance.pae.baidu.com/vapi/v1/fundflow"
            f"?code={_normalize_a_stock_code(code)}&market=ab&date={compact}"
            f"&finClientType=pc"
        )
        try:
            d = self._get(url, headers=_BAIDU_PAE_HEADERS, timeout=10).json()
            if str(d.get("ResultCode", -1)) != "0":
                return []
            raw = d.get("Result", {}).get("update_data", "")
            if not raw:
                return []
            rows = []
            for segment in raw.split(";"):
                parts = segment.split(",")
                if len(parts) >= 9:
                    rows.append({
                        "time": parts[0],
                        "mainForce": _to_float(parts[2]),
                        "retail": _to_float(parts[3]),
                        "super": _to_float(parts[4]),
                        "large": _to_float(parts[5]),
                        "price": _to_float(parts[8]),
                    })
            return rows
        except Exception as e:
            _logger.debug(f"[BaiduFundFlow] {code} 实时资金流获取失败: {e}")
            return []


class AStockSignalAdapter:
    """A 股信号层 adapter：同花顺、百度、东财、akshare 聚合端点。"""

    def __init__(self, request_get=None, ak_module=None):
        self._request_get = request_get
        self._ak = ak_module

    def _get(self, url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 10):
        if self._request_get is not None:
            return self._request_get(url, params=params, headers=headers, timeout=timeout)
        import requests
        return requests.get(url, params=params, headers=headers, timeout=timeout)

    def _akshare(self):
        if self._ak is not None:
            return self._ak
        import akshare as ak
        return ak

    async def get_research_reports(self, code: str, max_pages: int = 2) -> list[dict]:
        return await asyncio.to_thread(self._get_research_reports_sync, code, max_pages)

    def _get_research_reports_sync(self, code: str, max_pages: int = 2) -> list[dict]:
        records: list[dict] = []
        for page in range(1, max_pages + 1):
            params = {
                "industryCode": "*",
                "pageSize": "100",
                "industry": "*",
                "rating": "*",
                "ratingChange": "*",
                "beginTime": "2000-01-01",
                "endTime": "2030-01-01",
                "pageNo": str(page),
                "fields": "",
                "qType": "0",
                "orgCode": "",
                "code": _normalize_a_stock_code(code),
                "rcode": "",
                "p": str(page),
                "pageNum": str(page),
                "pageNumber": str(page),
            }
            try:
                d = self._get(
                    "https://reportapi.eastmoney.com/report/list",
                    params=params,
                    headers={"User-Agent": _THS_HEADERS["User-Agent"], "Referer": "https://data.eastmoney.com/"},
                    timeout=30,
                ).json()
                rows = d.get("data") or []
                if not rows:
                    break
                for row in rows:
                    info_code = row.get("infoCode", "")
                    records.append({
                        "title": row.get("title", ""),
                        "publish_date": (row.get("publishDate") or "")[:10],
                        "org": row.get("orgSName", ""),
                        "rating": row.get("emRatingName", ""),
                        "industry": row.get("indvInduName", ""),
                        "eps_this_year": row.get("predictThisYearEps"),
                        "eps_next_year": row.get("predictNextYearEps"),
                        "eps_next_two_year": row.get("predictNextTwoYearEps"),
                        "info_code": info_code,
                        "pdf_url": f"https://pdf.dfcfw.com/pdf/H3_{info_code}_1.pdf" if info_code else "",
                    })
                if page >= (d.get("TotalPage", 1) or 1):
                    break
            except Exception as e:
                _logger.warning(f"[AStockSignal] 东财研报获取失败: {code} {e}")
                break
        return records

    async def get_stock_news(self, code: str, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_stock_news_sync, code, limit)

    def _get_stock_news_sync(self, code: str, limit: int = 20) -> list[dict]:
        try:
            df = self._akshare().stock_news_em(symbol=_normalize_a_stock_code(code))
            if df is None or df.empty:
                return []
            rows = []
            for _, row in df.head(limit).iterrows():
                rows.append({
                    "title": row.get("新闻标题", row.get("标题", "")),
                    "content": row.get("新闻内容", row.get("内容", "")),
                    "time": str(row.get("发布时间", "")),
                    "source": row.get("文章来源", ""),
                    "url": row.get("新闻链接", row.get("链接", "")),
                })
            return rows
        except Exception as e:
            _logger.warning(f"[AStockSignal] 个股新闻获取失败: {code} {e}")
            return []

    async def get_cls_flash(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_cls_flash_sync, limit)

    def _get_cls_flash_sync(self, limit: int = 20) -> list[dict]:
        try:
            df = self._akshare().stock_info_global_cls()
            if df is None or df.empty:
                return []
            rows = []
            for _, row in df.head(limit).iterrows():
                rows.append({
                    "title": row.get("标题", ""),
                    "content": row.get("内容", ""),
                    "time": str(row.get("发布时间", "")),
                })
            return rows
        except Exception as e:
            _logger.warning(f"[AStockSignal] 财联社快讯获取失败: {e}")
            return []

    async def get_global_news(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_global_news_sync, limit)

    def _get_global_news_sync(self, limit: int = 20) -> list[dict]:
        try:
            df = self._akshare().stock_info_global_em()
            if df is None or df.empty:
                return []
            rows = []
            for _, row in df.head(limit).iterrows():
                rows.append({
                    "title": row.get("标题", ""),
                    "summary": row.get("摘要", ""),
                    "time": str(row.get("发布时间", "")),
                    "url": row.get("链接", ""),
                })
            return rows
        except Exception as e:
            _logger.warning(f"[AStockSignal] 东财全球资讯获取失败: {e}")
            return []

    async def get_basic_info(self, code: str) -> dict:
        return await asyncio.to_thread(self._get_basic_info_sync, code)

    def _get_basic_info_sync(self, code: str) -> dict:
        try:
            df = self._akshare().stock_individual_info_em(symbol=_normalize_a_stock_code(code))
            if df is None or df.empty:
                return {}
            return {
                str(row.get("item", row.iloc[0])): row.get("value", row.iloc[1] if len(row) > 1 else "")
                for _, row in df.iterrows()
            }
        except Exception as e:
            _logger.warning(f"[AStockSignal] 个股基本面获取失败: {code} {e}")
            return {}

    async def get_f10(self, code: str, category: str = "最新提示") -> str:
        return await asyncio.to_thread(self._get_f10_sync, code, category)

    def _get_f10_sync(self, code: str, category: str = "最新提示") -> str:
        try:
            from mootdx.quotes import Quotes
            client = Quotes.factory(market="std")
            text = client.F10(symbol=_normalize_a_stock_code(code), name=category)
            return str(text or "")
        except ImportError:
            return ""
        except Exception as e:
            _logger.warning(f"[AStockSignal] F10 获取失败: {code} {category} {e}")
            return ""

    async def get_hot_stocks(self, trade_date: str | None = None) -> list[dict]:
        return await asyncio.to_thread(self._get_hot_stocks_sync, trade_date)

    def _get_hot_stocks_sync(self, trade_date: str | None = None) -> list[dict]:
        if trade_date is None:
            trade_date = local_today().isoformat()
        url = (
            f"http://zx.10jqka.com.cn/event/api/getharden/"
            f"date/{trade_date}/orderby/date/orderway/desc/charset/GBK/"
        )
        try:
            d = self._get(url, headers=_THS_HEADERS, timeout=10).json()
            if d.get("errocode", 0) != 0:
                return []
            rows = []
            for item in d.get("data") or []:
                reason = str(item.get("reason", "") or "")
                rows.append({
                    "code": _normalize_a_stock_code(str(item.get("code", ""))),
                    "name": item.get("name", ""),
                    "reason": reason,
                    "reason_tags": [t.strip() for t in reason.split("+") if t.strip()],
                    "close": _to_float(item.get("close")),
                    "change_pct": _to_float(item.get("zhangfu")),
                    "turnover_pct": _to_float(item.get("huanshou")),
                    "amount": _to_float(item.get("chengjiaoe")),
                    "volume": _to_int(item.get("chengjiaoliang")),
                    "large_order_net": _to_float(item.get("ddejingliang")),
                    "market": item.get("market", ""),
                })
            return rows
        except Exception as e:
            _logger.warning(f"[AStockSignal] 同花顺热点获取失败: {e}")
            return []

    async def get_concept_blocks(self, code: str) -> dict:
        return await asyncio.to_thread(self._get_concept_blocks_sync, code)

    def _get_concept_blocks_sync(self, code: str) -> dict:
        url = (
            f"https://finance.pae.baidu.com/api/getrelatedblock"
            f"?code={_normalize_a_stock_code(code)}&market=ab"
            f"&typeCode=all&finClientType=pc"
        )
        empty = {"industry": [], "concept": [], "region": [], "concept_tags": []}
        try:
            d = self._get(url, headers=_BAIDU_PAE_HEADERS, timeout=10).json()
            if str(d.get("ResultCode", -1)) != "0":
                return empty
            result = {"industry": [], "concept": [], "region": [], "concept_tags": []}
            for block in d.get("Result", []):
                block_type = str(block.get("type", ""))
                for item in block.get("list", []):
                    entry = {
                        "name": item.get("name", ""),
                        "change_pct": item.get("increase", ""),
                        "desc": item.get("desc", ""),
                    }
                    if "行业" in block_type:
                        result["industry"].append(entry)
                    elif "概念" in block_type:
                        result["concept"].append(entry)
                        result["concept_tags"].append(entry["name"])
                    elif "地域" in block_type:
                        result["region"].append(entry)
            return result
        except Exception as e:
            _logger.warning(f"[AStockSignal] 百度概念获取失败: {code} {e}")
            return empty

    async def get_northbound_realtime(self) -> list[dict]:
        return await asyncio.to_thread(self._get_northbound_realtime_sync)

    def _get_northbound_realtime_sync(self) -> list[dict]:
        try:
            d = self._get(
                "https://data.hexin.cn/market/hsgtApi/method/dayChart/",
                headers=_HSGT_HEADERS,
                timeout=10,
            ).json()
            times = d.get("time", [])
            hgt = d.get("hgt", [])
            sgt = d.get("sgt", [])
            return [{
                "time": t,
                "hgt_yi": hgt[i] if i < len(hgt) else None,
                "sgt_yi": sgt[i] if i < len(sgt) else None,
            } for i, t in enumerate(times)]
        except Exception as e:
            _logger.warning(f"[AStockSignal] 北向资金获取失败: {e}")
            return []

    async def get_daily_dragon_tiger(self, trade_date: str | None = None, min_net_buy: float | None = None) -> dict:
        return await asyncio.to_thread(self._get_daily_dragon_tiger_sync, trade_date, min_net_buy)

    def _get_daily_dragon_tiger_sync(self, trade_date: str | None = None, min_net_buy: float | None = None) -> dict:
        if trade_date is None:
            trade_date = local_today().isoformat()
        params = {
            "reportName": "RPT_DAILYBILLBOARD_DETAILSNEW",
            "columns": "ALL",
            "filter": f"(TRADE_DATE>='{trade_date}')(TRADE_DATE<='{trade_date}')",
            "pageNumber": "1",
            "pageSize": "500",
            "sortTypes": "-1",
            "sortColumns": "BILLBOARD_NET_AMT",
            "source": "WEB",
            "client": "WEB",
        }
        try:
            d = self._get(
                "https://datacenter-web.eastmoney.com/api/data/v1/get",
                params=params,
                headers={"User-Agent": _THS_HEADERS["User-Agent"], "Referer": "https://data.eastmoney.com/"},
                timeout=15,
            ).json()
            data = (d.get("result") or {}).get("data") or []
            stocks = []
            for row in data:
                net_buy = _to_float(row.get("BILLBOARD_NET_AMT")) / 10000
                if min_net_buy is not None and net_buy < min_net_buy:
                    continue
                stocks.append({
                    "code": row.get("SECURITY_CODE", ""),
                    "name": row.get("SECURITY_NAME_ABBR", ""),
                    "reason": row.get("EXPLANATION", ""),
                    "close": row.get("CLOSE_PRICE") or 0,
                    "change_pct": round(_to_float(row.get("CHANGE_RATE")), 2),
                    "net_buy_wan": round(net_buy, 1),
                    "buy_wan": round(_to_float(row.get("BILLBOARD_BUY_AMT")) / 10000, 1),
                    "sell_wan": round(_to_float(row.get("BILLBOARD_SELL_AMT")) / 10000, 1),
                    "turnover_pct": round(_to_float(row.get("TURNOVERRATE")), 2),
                })
            actual_date = data[0].get("TRADE_DATE", "")[:10] if data else trade_date
            return {"date": actual_date, "total_records": len(stocks), "stocks": stocks}
        except Exception as e:
            _logger.warning(f"[AStockSignal] 全市场龙虎榜获取失败: {e}")
            return {"date": trade_date, "total_records": 0, "stocks": []}

    async def get_dragon_tiger(self, code: str, trade_date: str, look_back: int = 30) -> dict:
        return await asyncio.to_thread(self._get_dragon_tiger_sync, code, trade_date, look_back)

    def _get_dragon_tiger_sync(self, code: str, trade_date: str, look_back: int = 30) -> dict:
        from datetime import datetime, timedelta

        ak = self._akshare()
        normalized = _normalize_a_stock_code(code)
        start = datetime.strptime(trade_date, "%Y-%m-%d") - timedelta(days=look_back)
        start_str = start.strftime("%Y%m%d")
        end_str = trade_date.replace("-", "")
        records = []
        try:
            df = ak.stock_lhb_detail_em(start_date=start_str, end_date=end_str)
            if df is not None and not df.empty:
                for _, row in df[df["代码"] == normalized].iterrows():
                    records.append({
                        "date": str(row.get("日期", "")),
                        "reason": row.get("解读", ""),
                        "net_buy": row.get("龙虎榜净买额", 0),
                        "turnover": row.get("换手率", 0),
                    })
        except Exception:
            pass

        seats = {"buy": [], "sell": []}
        if records:
            latest_date = records[0]["date"].replace("-", "")[:8]
            for flag, key in (("买入", "buy"), ("卖出", "sell")):
                try:
                    df_detail = ak.stock_lhb_stock_detail_em(symbol=normalized, date=latest_date, flag=flag)
                    if df_detail is not None and not df_detail.empty:
                        for _, row in df_detail.head(5).iterrows():
                            seats[key].append({
                                "name": row.get("营业部名称", ""),
                                "buy_amt": row.get("买入额", 0),
                                "sell_amt": row.get("卖出额", 0),
                                "net": row.get("净额", 0),
                            })
                except Exception:
                    pass

        institution = {}
        try:
            df_inst = ak.stock_lhb_jgmmtj_em(symbol=normalized)
            if df_inst is not None and not df_inst.empty:
                row = df_inst.iloc[0]
                institution = {
                    "buy_count": row.get("买入机构数", 0),
                    "sell_count": row.get("卖出机构数", 0),
                    "net_amount": row.get("机构净买入额", 0),
                }
        except Exception:
            pass
        return {"records": records, "seats": seats, "institution": institution}

    async def get_lockup_expiry(self, code: str, trade_date: str, forward_days: int = 90) -> dict:
        return await asyncio.to_thread(self._get_lockup_expiry_sync, code, trade_date, forward_days)

    def _get_lockup_expiry_sync(self, code: str, trade_date: str, forward_days: int = 90) -> dict:
        ak = self._akshare()
        normalized = _normalize_a_stock_code(code)
        history = []
        upcoming = []
        try:
            df = ak.stock_restricted_release_queue_em(symbol=normalized)
            if df is not None and not df.empty:
                for _, row in df.head(15).iterrows():
                    history.append({
                        "date": str(row.get("解禁时间", "")),
                        "type": row.get("限售股类型", ""),
                        "shares": row.get("解禁数量", 0),
                        "ratio": row.get("实际解禁市值占总市值比例", 0),
                    })
        except Exception:
            pass
        try:
            df = ak.stock_restricted_release_detail_em(date=trade_date.replace("-", ""))
            if df is not None and not df.empty:
                for _, row in df[df["股票代码"] == normalized].iterrows():
                    upcoming.append({
                        "date": str(row.get("解禁日期", "")),
                        "type": row.get("限售股类型", ""),
                        "shares": row.get("解禁数量", 0),
                        "float_ratio": row.get("占流通股比例", 0),
                    })
        except Exception:
            pass
        return {"history": history, "upcoming": upcoming}

    async def get_industry_comparison(self, top_n: int = 20) -> dict:
        return await asyncio.to_thread(self._get_industry_comparison_sync, top_n)

    def _get_industry_comparison_sync(self, top_n: int = 20) -> dict:
        rows = self._get_industry_comparison_em()
        if rows:
            return {"top": rows[:top_n], "bottom": rows[-top_n:], "total": len(rows)}

        rows = self._get_industry_comparison_sina()
        if rows:
            return {"top": rows[:top_n], "bottom": rows[-top_n:], "total": len(rows)}

        return self._get_industry_comparison_ths(top_n)

    def _get_industry_comparison_em(self) -> list[dict]:
        try:
            df = self._akshare().stock_board_industry_name_em()
            if df is None or df.empty:
                return []
            rows = []
            for i, row in df.sort_values("涨跌幅", ascending=False).reset_index(drop=True).iterrows():
                rows.append({
                    "rank": i + 1,
                    "name": row.get("板块名称", row.get("名称", "")),
                    "change_pct": _to_float(row.get("涨跌幅")),
                    "turnover_yi": _to_float(row.get("成交额")) / 100000000,
                    "net_inflow_yi": _to_float(row.get("净流入")) / 100000000 if "净流入" in df.columns else None,
                    "up_count": _to_int(row.get("上涨家数")),
                    "down_count": _to_int(row.get("下跌家数")),
                    "leader": row.get("领涨股票", row.get("领涨股", "")),
                })
            return rows
        except Exception as e:
            _logger.debug(f"[AStockSignal] 东财行业对比获取失败: {e}")
            return []

    def _get_industry_comparison_sina(self) -> list[dict]:
        try:
            df = self._akshare().stock_sector_spot(indicator="行业")
            if df is None or df.empty:
                return []
            rows = []
            sorted_df = df.copy()
            sorted_df["涨跌幅"] = sorted_df["涨跌幅"].map(_to_float)
            sorted_df = sorted_df.sort_values("涨跌幅", ascending=False).reset_index(drop=True)
            for i, row in sorted_df.iterrows():
                rows.append({
                    "rank": i + 1,
                    "name": row.get("板块", ""),
                    "change_pct": _to_float(row.get("涨跌幅")),
                    "turnover_yi": _to_float(row.get("总成交额")) / 100000000,
                    "net_inflow_yi": None,
                    "up_count": 0,
                    "down_count": 0,
                    "leader": row.get("名称", row.get("领涨股", "")),
                })
            return rows
        except Exception as e:
            _logger.debug(f"[AStockSignal] 新浪行业对比获取失败: {e}")
            return []

    def _get_industry_comparison_ths(self, top_n: int = 20) -> dict:
        try:
            df = self._akshare().stock_board_industry_summary_ths()
            if df is None or df.empty:
                return {"top": [], "bottom": [], "total": 0}
            rows = []
            for i, row in df.iterrows():
                rows.append({
                    "rank": i + 1,
                    "name": row.get("板块", ""),
                    "change_pct": row.get("涨跌幅", 0),
                    "turnover_yi": row.get("总成交额", 0),
                    "net_inflow_yi": row.get("净流入", 0) if "净流入" in df.columns else None,
                    "up_count": row.get("上涨家数", 0),
                    "down_count": row.get("下跌家数", 0),
                    "leader": row.get("领涨股", ""),
                })
            return {"top": rows[:top_n], "bottom": rows[-top_n:], "total": len(rows)}
        except Exception as e:
            _logger.debug(f"[AStockSignal] 行业对比获取失败: {e}")
            return {"top": [], "bottom": [], "total": 0}

    async def get_announcements(self, code: str, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_announcements_sync, code, limit)

    def _get_announcements_sync(self, code: str, limit: int = 20) -> list[dict]:
        ak = self._akshare()
        normalized = _normalize_a_stock_code(code)
        try:
            df = ak.stock_zh_a_disclosure_report_cninfo(symbol=normalized, market="沪深京")
            if df is None or df.empty:
                return []
            rows = []
            for _, row in df.head(limit).iterrows():
                rows.append({
                    "title": row.get("公告标题", ""),
                    "type": row.get("公告类型", ""),
                    "date": str(row.get("公告日期", row.get("公告时间", "")))[:10],
                    "url": row.get("公告链接", ""),
                })
            return rows
        except Exception as e:
            _logger.warning(f"[AStockSignal] 巨潮公告获取失败: {code} {e}")
            return []


class MootdxMarketAdapter:
    """mootdx 行情 adapter（通达信 TCP）。"""

    def __init__(self, client_factory=None):
        self._client_factory = client_factory
        self._local = threading.local()

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        a_codes = [_normalize_a_stock_code(c) for c in codes if not is_hk_code(c)]
        if not a_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, a_codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(
            self._get_kline_sync, _normalize_a_stock_code(code), period, count
        )

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        return {}

    def _client(self):
        if getattr(self._local, "client", None) is None:
            if self._client_factory is not None:
                self._local.client = self._client_factory()
            else:
                from mootdx.quotes import Quotes
                self._local.client = Quotes.factory(market="std")
        return self._local.client

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            df = self._client().quotes(symbol=codes)
            if df is None or df.empty:
                return {}
            result: dict[str, StockQuote] = {}
            for _, row in df.iterrows():
                code = _normalize_a_stock_code(str(row.get("code", row.get("symbol", ""))))
                if not code or code not in codes:
                    continue
                price = _to_float(row.get("price"))
                last_close = _to_float(row.get("last_close") or row.get("pre_close"))
                change_pct = ((price - last_close) / last_close * 100) if last_close > 0 else 0.0
                result[code] = StockQuote(
                    code=code,
                    name=str(row.get("name", code)),
                    price=price,
                    open=_to_float(row.get("open")),
                    high=_to_float(row.get("high")),
                    low=_to_float(row.get("low")),
                    close=price,
                    volume=_to_int(row.get("vol") or row.get("volume")),
                    amount=_to_float(row.get("amount")),
                    change_pct=round(change_pct, 2),
                )
            return result
        except ImportError:
            _logger.info("[MootdxMarket] mootdx 未安装，跳过")
            return {}
        except Exception as e:
            _logger.warning(f"[MootdxMarket] 实时行情获取失败: {e}")
            return {}

    def _get_kline_sync(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        category_map = {
            "daily": 4, "d": 4,
            "weekly": 5, "w": 5,
            "monthly": 6, "m": 6,
            "1": 7, "1m": 7,
            "5": 8, "5m": 8,
            "15": 9, "15m": 9,
            "30": 10, "30m": 10,
            "60": 11, "60m": 11,
        }
        category = category_map.get(str(period).lower())
        if category is None:
            return None
        try:
            df = self._client().bars(symbol=code, category=category, offset=count)
            if df is None or df.empty:
                return None
            out = pd.DataFrame({
                "date": df["datetime"].astype(str) if "datetime" in df.columns else df.index.astype(str),
                "open": pd.to_numeric(df["open"], errors="coerce").fillna(0.0),
                "close": pd.to_numeric(df["close"], errors="coerce").fillna(0.0),
                "high": pd.to_numeric(df["high"], errors="coerce").fillna(0.0),
                "low": pd.to_numeric(df["low"], errors="coerce").fillna(0.0),
                "volume": pd.to_numeric(df.get("vol", df.get("volume", 0)), errors="coerce").fillna(0).astype(int),
                "amount": pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0.0),
            })
            out["pct_change"] = out["close"].pct_change().fillna(0.0) * 100
            return out.tail(count).reset_index(drop=True)
        except ImportError:
            _logger.info("[MootdxMarket] mootdx 未安装，跳过")
            return None
        except Exception as e:
            _logger.warning(f"[MootdxMarket] K线获取失败: {code} {e}")
            return None


def _parse_tencent_quote_payload(data: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for line in data.strip().split(";"):
        if not line.strip() or "=" not in line or '"' not in line:
            continue
        key = line.split("=", 1)[0].split("_")[-1]
        vals = line.split('"')[1].split("~")
        if len(vals) < 53:
            continue
        code = _normalize_a_stock_code(key)
        result[code] = {
            "name": vals[1],
            "price": _to_float(vals[3]),
            "pe_ttm": _to_float(vals[39]) or None,
            "pb": _to_float(vals[46]) or None,
        }
    return result
