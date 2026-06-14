"""Tushare Pro adapters.

Tushare is used as a paid, stable source for regular point-based A-share data.
Minute, news, announcement and feature-data endpoints still require separate
permissions, so this module only wires regular daily, index, financial and
money-flow APIs.
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
import os
from typing import Any, Callable, Optional

import pandas as pd

from astock_trading.market.adapter_utils import _normalize_a_stock_code, _to_float
from astock_trading.market.models import FinancialReport, FundFlow, IndexQuote, StockQuote

_ASTOCK_TOKEN_ENV = "ASTOCK_TUSHARE_TOKEN"
_FALLBACK_TOKEN_ENV = "TUSHARE_TOKEN"


class TushareAPIError(RuntimeError):
    """Raised when Tushare returns a non-zero API response."""

    def __init__(self, code: int | str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"Tushare API failed: code={code}, message={message}")


class TushareClient:
    """Small HTTP client for Tushare Pro without adding SDK dependency."""

    def __init__(
        self,
        token: str = "",
        *,
        token_source: str = "",
        sdk_module: Any | None = None,
        api_url: str = "http://api.tushare.pro",
        timeout: float = 30.0,
        http_post: Callable[..., Any] | None = None,
    ):
        self._token = token.strip()
        self._token_source = token_source
        self._sdk = sdk_module
        self._pro = None
        self._api_url = api_url
        self._timeout = timeout
        self._http_post = http_post

    @classmethod
    def from_env(cls) -> "TushareClient":
        primary = os.environ.get(_ASTOCK_TOKEN_ENV, "").strip()
        if primary:
            return cls(primary, token_source=_ASTOCK_TOKEN_ENV)
        fallback = os.environ.get(_FALLBACK_TOKEN_ENV, "").strip()
        if fallback:
            return cls(fallback, token_source=_FALLBACK_TOKEN_ENV)
        return cls("", token_source="")

    @property
    def enabled(self) -> bool:
        return bool(self._token)

    @property
    def token_present(self) -> bool:
        return bool(self._token)

    @property
    def token_source(self) -> str:
        return self._token_source

    def diagnostic(self) -> dict[str, Any]:
        return {
            "provider": "tushare",
            "enabled": self.enabled,
            "token_present": self.token_present,
            "token_source": self._token_source if self._token else "",
            "sdk": "tushare",
            "query_transport": "http",
            "configured_regular_interfaces": [
                "daily",
                "pro_bar",
                "index_daily",
                "daily_basic",
                "fina_indicator",
                "moneyflow",
                "stock_basic",
                "top_list",
                "share_float",
                "hk_hold",
            ],
            "not_assumed_interfaces": {
                "minute": "分钟数据为独立权限，不按积分默认开放",
                "news": "新闻资讯为独立权限，不按积分默认开放",
                "announcements": "公告信息为独立权限，不按积分默认开放",
                "feature_data": "特色数据通常需要 10000+ 或专属权限",
            },
            "permission_note": "按当前 token 实测接口结果为准，分钟/新闻/公告/特色数据不按积分自动开放。",
        }

    def _sdk_module(self):
        if self._sdk is None:
            import tushare as ts

            self._sdk = ts
        return self._sdk

    def _pro_api(self):
        if self._pro is None:
            self._pro = self._sdk_module().pro_api(self._token)
        return self._pro

    def query(self, api_name: str, *, params: dict | None = None, fields: str = "") -> list[dict]:
        if not self._token:
            return []
        if self._sdk is None:
            return self._http_query(api_name, params=params, fields=fields)
        try:
            frame = self._pro_api().query(api_name, fields=fields, **(params or {}))
        except Exception as exc:
            raise TushareAPIError("sdk_query_failed", str(exc)) from exc
        return _frame_to_records(frame)

    def _http_query(self, api_name: str, *, params: dict | None = None, fields: str = "") -> list[dict]:
        try:
            post = self._http_post
            if post is None:
                import requests

                post = requests.post
            response = post(
                self._api_url,
                json={
                    "api_name": api_name,
                    "token": self._token,
                    "params": params or {},
                    "fields": fields,
                },
                timeout=self._timeout,
            )
            raise_for_status = getattr(response, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            payload = response.json()
        except TushareAPIError:
            raise
        except Exception as exc:
            raise TushareAPIError("http_query_failed", str(exc)) from exc

        if not isinstance(payload, dict):
            raise TushareAPIError("invalid_response", "Tushare HTTP response is not a JSON object")
        code = payload.get("code", 0)
        if code not in (0, "0", None):
            raise TushareAPIError(code, str(payload.get("msg") or ""))
        data = payload.get("data") or {}
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if not isinstance(data, dict):
            return []
        records = data.get("items") or []
        response_fields = data.get("fields") or []
        if not isinstance(records, list) or not isinstance(response_fields, list):
            return []
        rows: list[dict] = []
        for item in records:
            if not isinstance(item, (list, tuple)):
                continue
            rows.append({str(field): value for field, value in zip(response_fields, item)})
        return rows

    def pro_bar(self, *, ts_code: str, start_date: str, end_date: str, count: int) -> list[dict]:
        if not self._token:
            return []
        try:
            frame = self._sdk_module().pro_bar(
                ts_code=ts_code,
                api=self._pro_api(),
                start_date=start_date,
                end_date=end_date,
                adj="qfq",
                freq="D",
                ma=[5, 10, 20, 60],
                factors=["vr", "tor"],
            )
        except TypeError:
            frame = self._sdk_module().pro_bar(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                adj="qfq",
                freq="D",
                ma=[5, 10, 20, 60],
                factors=["vr", "tor"],
            )
        except Exception as exc:
            raise TushareAPIError("sdk_pro_bar_failed", str(exc)) from exc
        records = _frame_to_records(frame)
        return records[-count:] if count > 0 else records


def _client_or_default(client: TushareClient | None) -> TushareClient:
    return client or TushareClient.from_env()


def _frame_to_records(frame: Any) -> list[dict]:
    if frame is None:
        return []
    if isinstance(frame, list):
        return [item for item in frame if isinstance(item, dict)]
    if isinstance(frame, dict):
        return [frame]
    if isinstance(frame, pd.DataFrame):
        if frame.empty:
            return []
        return frame.where(pd.notna(frame), None).to_dict("records")
    to_dict = getattr(frame, "to_dict", None)
    if callable(to_dict):
        try:
            records = to_dict("records")
            if isinstance(records, list):
                return [item for item in records if isinstance(item, dict)]
        except TypeError:
            pass
    return []


def _compact_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _recent_start_date(days: int) -> str:
    return _compact_date(date.today() - timedelta(days=max(days, 1)))


def _normalize_trade_date(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return _compact_date(date.today())
    return text.replace("-", "")[:8]


def _date_from_any(value: object) -> date:
    text = _normalize_trade_date(value)
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def _iso_date(value: object) -> str:
    text = _normalize_trade_date(value)
    if len(text) != 8 or not text.isdigit():
        return str(value or "")
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _to_ts_code(code: str) -> str:
    raw = str(code or "").strip().upper()
    if "." in raw:
        symbol, suffix = raw.split(".", 1)
        return f"{symbol[-6:]}.{suffix[:2]}"
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")) and len(raw) >= 8:
        prefix = lower[:2]
        symbol = raw[2:8]
        return f"{symbol}.{prefix.upper()}"
    normalized = _normalize_a_stock_code(raw)
    if not normalized:
        return raw
    if normalized.startswith(("6", "9")):
        suffix = "SH"
    elif normalized.startswith(("4", "8")):
        suffix = "BJ"
    else:
        suffix = "SZ"
    return f"{normalized}.{suffix}"


def _from_ts_code(ts_code: str) -> str:
    return str(ts_code or "").split(".", 1)[0]


def _sort_by_trade_date(rows: list[dict]) -> list[dict]:
    return sorted(rows, key=lambda item: str(item.get("trade_date") or item.get("end_date") or ""))


def _daily_amount_to_yuan(value: object) -> float:
    return _to_float(value) * 1000


def _daily_volume_to_shares(value: object) -> int:
    return int(_to_float(value) * 100)


def _moneyflow_to_yuan(value: object) -> float:
    return _to_float(value) * 10000


def _dragon_tiger_stock(row: dict) -> dict:
    return {
        "code": _from_ts_code(str(row.get("ts_code") or "")),
        "name": row.get("name", ""),
        "date": _iso_date(row.get("trade_date")),
        "reason": row.get("reason", ""),
        "close": _to_float(row.get("close")),
        "change_pct": round(_to_float(row.get("pct_change")), 2),
        "turnover_pct": round(_to_float(row.get("turnover_rate")), 2),
        "amount_wan": round(_to_float(row.get("amount")), 1),
        "net_buy_wan": round(_to_float(row.get("net_amount")), 1),
        "source": "tushare",
    }


class TushareMarketAdapter:
    """A-share daily quote, K-line and index data from Tushare Pro."""

    def __init__(self, client: TushareClient | None = None):
        self._client = _client_or_default(client)

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        if not self._client.enabled:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, codes)

    async def get_kline(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        if period != "daily" or not self._client.enabled:
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, count)

    async def get_trade_dates(self, start: str, end: str) -> list[str]:
        if not self._client.enabled:
            return []
        return await asyncio.to_thread(self._get_trade_dates_sync, start, end)

    async def get_daily_market_bars(self, trade_date: str) -> pd.DataFrame:
        if not self._client.enabled:
            return pd.DataFrame()
        return await asyncio.to_thread(self._get_daily_market_bars_sync, trade_date)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        if not self._client.enabled:
            return {}
        return await asyncio.to_thread(self._get_index_sync, symbols)

    async def get_basic_info(self, code: str) -> dict:
        if not self._client.enabled:
            return {}
        return await asyncio.to_thread(self._get_basic_info_sync, code)

    async def get_daily_dragon_tiger(
        self,
        trade_date: str | None = None,
        min_net_buy: float | None = None,
    ) -> dict:
        if not self._client.enabled:
            return {"date": trade_date or "", "total_records": 0, "stocks": []}
        return await asyncio.to_thread(self._get_daily_dragon_tiger_sync, trade_date, min_net_buy)

    async def get_dragon_tiger(self, code: str, trade_date: str, look_back: int = 30) -> dict:
        if not self._client.enabled:
            return {"records": [], "seats": {"buy": [], "sell": []}, "institution": {}}
        return await asyncio.to_thread(self._get_dragon_tiger_sync, code, trade_date, look_back)

    async def get_lockup_expiry(
        self,
        code: str,
        trade_date: str,
        forward_days: int = 90,
    ) -> dict:
        if not self._client.enabled:
            return {"history": [], "upcoming": []}
        return await asyncio.to_thread(self._get_lockup_expiry_sync, code, trade_date, forward_days)

    async def get_northbound_realtime(self) -> list[dict]:
        if not self._client.enabled:
            return []
        return await asyncio.to_thread(self._get_northbound_realtime_sync)

    def _query_daily(self, code: str, count: int) -> list[dict]:
        lookback_days = max(count * 2 + 30, 45)
        return self._client.query(
            "daily",
            params={
                "ts_code": _to_ts_code(code),
                "start_date": _recent_start_date(lookback_days),
                "end_date": _compact_date(date.today()),
            },
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )

    def _get_trade_dates_sync(self, start: str, end: str) -> list[str]:
        rows = self._client.query(
            "trade_cal",
            params={
                "exchange": "SSE",
                "is_open": "1",
                "start_date": _normalize_trade_date(start),
                "end_date": _normalize_trade_date(end),
            },
            fields="cal_date",
        )
        dates = {
            str(row.get("cal_date") or "").strip()
            for row in rows
            if str(row.get("cal_date") or "").strip()
        }
        return sorted(dates)

    def _get_daily_market_bars_sync(self, trade_date: str) -> pd.DataFrame:
        compact = _normalize_trade_date(trade_date)
        rows = self._client.query(
            "daily",
            params={"trade_date": compact},
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        )
        if not rows:
            return pd.DataFrame()
        rows = sorted(rows, key=lambda item: str(item.get("ts_code") or ""))
        return pd.DataFrame({
            "symbol": [_from_ts_code(str(row.get("ts_code") or "")) for row in rows],
            "date": [str(row.get("trade_date") or compact) for row in rows],
            "open": [_to_float(row.get("open")) for row in rows],
            "high": [_to_float(row.get("high")) for row in rows],
            "low": [_to_float(row.get("low")) for row in rows],
            "close": [_to_float(row.get("close")) for row in rows],
            "volume": [_daily_volume_to_shares(row.get("vol")) for row in rows],
            "amount": [_daily_amount_to_yuan(row.get("amount")) for row in rows],
            "涨跌幅": [_to_float(row.get("pct_chg")) for row in rows],
        })

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        quotes: dict[str, StockQuote] = {}
        for code in codes:
            rows = _sort_by_trade_date(self._query_daily(code, 10))
            if not rows:
                continue
            row = rows[-1]
            normalized = _from_ts_code(str(row.get("ts_code") or _to_ts_code(code))) or code
            close = _to_float(row.get("close"))
            quotes[normalized] = StockQuote(
                code=normalized,
                name=normalized,
                price=close,
                open=_to_float(row.get("open"), close),
                high=_to_float(row.get("high"), close),
                low=_to_float(row.get("low"), close),
                close=close,
                volume=_daily_volume_to_shares(row.get("vol")),
                amount=_daily_amount_to_yuan(row.get("amount")),
                change_pct=_to_float(row.get("pct_chg")),
            )
        return quotes

    def _get_kline_sync(self, code: str, count: int) -> Optional[pd.DataFrame]:
        lookback_days = max(count * 2 + 30, 45)
        rows = _sort_by_trade_date(self._client.pro_bar(
            ts_code=_to_ts_code(code),
            start_date=_recent_start_date(lookback_days),
            end_date=_compact_date(date.today()),
            count=count,
        ))
        if not rows:
            rows = _sort_by_trade_date(self._query_daily(code, count))
        if not rows:
            return None
        selected = rows[-count:]
        frame = pd.DataFrame({
            "date": [str(row.get("trade_date") or "") for row in selected],
            "open": [_to_float(row.get("open")) for row in selected],
            "high": [_to_float(row.get("high")) for row in selected],
            "low": [_to_float(row.get("low")) for row in selected],
            "close": [_to_float(row.get("close")) for row in selected],
            "volume": [_daily_volume_to_shares(row.get("vol")) for row in selected],
            "amount": [_daily_amount_to_yuan(row.get("amount")) for row in selected],
            "涨跌幅": [_to_float(row.get("pct_chg")) for row in selected],
        })
        return frame

    def _get_index_sync(self, symbols: list[str]) -> dict[str, IndexQuote]:
        quotes: dict[str, IndexQuote] = {}
        for symbol in symbols:
            ts_code = _index_ts_code(symbol)
            if not ts_code:
                continue
            rows = _sort_by_trade_date(self._client.query(
                "index_daily",
                params={
                    "ts_code": ts_code,
                    "start_date": _recent_start_date(180),
                    "end_date": _compact_date(date.today()),
                },
                fields="ts_code,trade_date,close,pct_chg",
            ))
            if not rows:
                continue
            closes = pd.Series([_to_float(row.get("close")) for row in rows])
            latest = rows[-1]
            ma20 = float(closes.rolling(20).mean().iloc[-1]) if len(closes) >= 20 else 0.0
            ma60 = float(closes.rolling(60).mean().iloc[-1]) if len(closes) >= 60 else 0.0
            close = _to_float(latest.get("close"))
            below_ma60_days = 0
            if ma60 > 0:
                for value in reversed(closes.tolist()):
                    if value < ma60:
                        below_ma60_days += 1
                    else:
                        break
            quotes[symbol] = IndexQuote(
                symbol=symbol,
                name=_index_name(symbol),
                price=close,
                change_pct=_to_float(latest.get("pct_chg")),
                ma20=ma20,
                ma60=ma60,
                above_ma20=bool(close >= ma20) if ma20 > 0 else False,
                below_ma60_days=below_ma60_days,
            )
        return quotes

    def _get_basic_info_sync(self, code: str) -> dict:
        rows = self._client.query(
            "stock_basic",
            params={"ts_code": _to_ts_code(code), "list_status": "L"},
            fields="ts_code,symbol,name,area,industry,market,list_date,fullname,enname,exchange,list_status",
        )
        if not rows:
            return {}
        row = rows[0]
        return {
            "ts_code": row.get("ts_code", ""),
            "symbol": row.get("symbol") or _from_ts_code(str(row.get("ts_code") or "")),
            "name": row.get("name", ""),
            "area": row.get("area", ""),
            "industry": row.get("industry", ""),
            "market": row.get("market", ""),
            "list_date": _iso_date(row.get("list_date")),
            "exchange": row.get("exchange", ""),
            "list_status": row.get("list_status", ""),
            "source": "tushare",
        }

    def _get_daily_dragon_tiger_sync(
        self,
        trade_date: str | None = None,
        min_net_buy: float | None = None,
    ) -> dict:
        compact = _normalize_trade_date(trade_date or _compact_date(date.today()))
        rows = self._client.query(
            "top_list",
            params={"trade_date": compact},
            fields=(
                "trade_date,ts_code,name,close,pct_change,turnover_rate,amount,"
                "net_amount,reason"
            ),
        )
        stocks = [_dragon_tiger_stock(row) for row in rows]
        if min_net_buy is not None:
            stocks = [item for item in stocks if item["net_buy_wan"] >= min_net_buy]
        return {
            "date": _iso_date(compact),
            "total_records": len(stocks),
            "stocks": stocks,
        }

    def _get_dragon_tiger_sync(self, code: str, trade_date: str, look_back: int = 30) -> dict:
        end = _date_from_any(trade_date)
        start = end - timedelta(days=max(look_back, 1))
        ts_code = _to_ts_code(code)
        rows: list[dict] = []
        current = start
        while current <= end:
            for row in self._client.query(
                "top_list",
                params={"trade_date": _compact_date(current)},
                fields=(
                    "trade_date,ts_code,name,close,pct_change,turnover_rate,amount,"
                    "net_amount,reason"
                ),
            ):
                if str(row.get("ts_code") or "").upper() == ts_code:
                    rows.append(row)
            current += timedelta(days=1)
        records = [_dragon_tiger_stock(row) for row in _sort_by_trade_date(rows)]
        return {
            "records": records,
            "seats": {"buy": [], "sell": []},
            "institution": {},
            "source": "tushare",
        }

    def _get_lockup_expiry_sync(self, code: str, trade_date: str, forward_days: int = 90) -> dict:
        start = _date_from_any(trade_date)
        end = start + timedelta(days=max(forward_days, 1))
        rows = self._client.query(
            "share_float",
            params={
                "ts_code": _to_ts_code(code),
                "start_date": _compact_date(start),
                "end_date": _compact_date(end),
            },
            fields="ts_code,float_date,float_share,float_ratio,holder_name,share_type",
        )
        upcoming = [
            {
                "code": _from_ts_code(str(row.get("ts_code") or "")),
                "float_date": _iso_date(row.get("float_date")),
                "float_share": _to_float(row.get("float_share")),
                "float_ratio": _to_float(row.get("float_ratio")),
                "holder_name": row.get("holder_name", ""),
                "share_type": row.get("share_type", ""),
                "source": "tushare",
            }
            for row in _sort_by_trade_date(rows)
        ]
        return {"history": [], "upcoming": upcoming}

    def _get_northbound_realtime_sync(self) -> list[dict]:
        rows = self._client.query(
            "hk_hold",
            params={},
            fields="trade_date,ts_code,name,vol,ratio,exchange",
        )
        return [
            {
                "trade_date": _iso_date(row.get("trade_date")),
                "code": _from_ts_code(str(row.get("ts_code") or "")),
                "name": row.get("name", ""),
                "vol": _to_float(row.get("vol")),
                "ratio": _to_float(row.get("ratio")),
                "exchange": row.get("exchange", ""),
                "source": "tushare_hk_hold",
            }
            for row in rows
        ]


class TushareFinancialAdapter:
    """Financial and daily-basic data from Tushare Pro."""

    def __init__(self, client: TushareClient | None = None):
        self._client = _client_or_default(client)

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        if not self._client.enabled:
            return None
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        ts_code = _to_ts_code(code)
        daily_basic = _sort_by_trade_date(self._client.query(
            "daily_basic",
            params={
                "ts_code": ts_code,
                "start_date": _recent_start_date(45),
                "end_date": _compact_date(date.today()),
            },
            fields="ts_code,trade_date,pe_ttm,pb,total_mv,circ_mv,turnover_rate",
        ))
        indicators = _sort_by_trade_date(self._client.query(
            "fina_indicator",
            params={"ts_code": ts_code},
            fields=(
                "ts_code,end_date,roe,or_yoy,netprofit_yoy,ocf_to_or,"
                "debt_to_assets"
            ),
        ))
        latest_basic = daily_basic[-1] if daily_basic else {}
        latest_indicator = indicators[-1] if indicators else {}
        if not latest_basic and not latest_indicator:
            return None
        return FinancialReport(
            roe=_optional_float(latest_indicator.get("roe")),
            roe_3y_ago=_indicator_roe_3y_ago(indicators),
            revenue_growth=_optional_float(latest_indicator.get("or_yoy")),
            net_profit_growth=_optional_float(latest_indicator.get("netprofit_yoy")),
            operating_cash_flow=_optional_float(latest_indicator.get("ocf_to_or")),
            pe_ttm=_optional_float(latest_basic.get("pe_ttm")),
            pb=_optional_float(latest_basic.get("pb")),
            debt_ratio=_optional_float(latest_indicator.get("debt_to_assets")),
        )


def _indicator_roe_3y_ago(indicators: list[dict[str, Any]]) -> Optional[float]:
    if not indicators:
        return None
    latest_year = _indicator_year(indicators[-1])
    if latest_year is None:
        return None
    target_year = latest_year - 3
    for row in reversed(indicators):
        if _indicator_year(row) == target_year:
            return _optional_float(row.get("roe"))
    return None


def _indicator_year(row: dict[str, Any]) -> Optional[int]:
    text = str(row.get("end_date") or "")
    if len(text) < 4:
        return None
    try:
        return int(text[:4])
    except ValueError:
        return None


class TushareFlowAdapter:
    """Individual A-share money-flow data from Tushare Pro."""

    def __init__(self, client: TushareClient | None = None):
        self._client = _client_or_default(client)

    async def get_fund_flow(self, code: str, days: int = 5) -> Optional[FundFlow]:
        if not self._client.enabled:
            return None
        return await asyncio.to_thread(self._get_fund_flow_sync, code, days)

    def _get_fund_flow_sync(self, code: str, days: int) -> Optional[FundFlow]:
        rows = _sort_by_trade_date(self._client.query(
            "moneyflow",
            params={
                "ts_code": _to_ts_code(code),
                "start_date": _recent_start_date(max(days * 3, 20)),
                "end_date": _compact_date(date.today()),
            },
            fields=(
                "ts_code,trade_date,buy_sm_amount,sell_sm_amount,buy_md_amount,"
                "sell_md_amount,buy_lg_amount,sell_lg_amount,buy_elg_amount,"
                "sell_elg_amount,net_mf_amount"
            ),
        ))
        if not rows:
            return None
        selected = rows[-max(days, 1):]
        latest = selected[-1]
        net_values = [_moneyflow_to_yuan(row.get("net_mf_amount")) for row in selected]
        net_1d = _moneyflow_to_yuan(latest.get("net_mf_amount"))
        net_5d = sum(net_values)
        large_buy = _moneyflow_to_yuan(latest.get("buy_lg_amount")) + _moneyflow_to_yuan(
            latest.get("buy_elg_amount")
        )
        large_sell = _moneyflow_to_yuan(latest.get("sell_lg_amount")) + _moneyflow_to_yuan(
            latest.get("sell_elg_amount")
        )
        denominator = abs(large_buy) + abs(large_sell)
        if denominator:
            main_force_ratio = (large_buy - large_sell) / denominator
        elif net_1d > 0:
            main_force_ratio = 1.0
        elif net_1d < 0:
            main_force_ratio = -1.0
        else:
            main_force_ratio = 0.0
        consecutive_outflow = 0
        for value in reversed(net_values):
            if value < 0:
                consecutive_outflow += 1
            else:
                break
        return FundFlow(
            net_inflow_1d=net_1d,
            net_inflow_5d=net_5d,
            main_force_ratio=round(main_force_ratio, 4),
            northbound_net=0.0,
            northbound_net_positive=net_1d > 0,
            consecutive_outflow_days=consecutive_outflow,
        )


def _optional_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return _to_float(value, default=None)


def _index_ts_code(symbol: str) -> str:
    raw = str(symbol or "").strip().lower()
    mapping = {
        "sh000001": "000001.SH",
        "000001.sh": "000001.SH",
        "000001": "000001.SH",
        "sz399001": "399001.SZ",
        "399001.sz": "399001.SZ",
        "399001": "399001.SZ",
        "sz399006": "399006.SZ",
        "399006.sz": "399006.SZ",
        "399006": "399006.SZ",
        "sh000688": "000688.SH",
        "000688.sh": "000688.SH",
        "000688": "000688.SH",
    }
    return mapping.get(raw, _to_ts_code(raw))


def _index_name(symbol: str) -> str:
    raw = str(symbol or "").strip().lower()
    if raw in {"sh000001", "000001.sh", "000001"}:
        return "上证指数"
    if raw in {"sz399001", "399001.sz", "399001"}:
        return "深证成指"
    if raw in {"sz399006", "399006.sz", "399006"}:
        return "创业板指"
    if raw in {"sh000688", "000688.sh", "000688"}:
        return "科创50"
    return symbol


def tushare_provider_diagnostic() -> dict[str, Any]:
    return TushareClient.from_env().diagnostic()
