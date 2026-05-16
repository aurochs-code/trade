"""
market/adapters.py — 数据源 Protocol + adapter 实现

Protocol 定义标准接口，adapter 实现具体数据源。
所有 adapter 返回标准化的 dataclass，不暴露数据源细节。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import shutil
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Protocol, runtime_checkable

import pandas as pd

from astock_trading.platform.time import local_now, local_today
from astock_trading.market.models import (
    FinancialReport,
    FundFlow,
    IndexQuote,
    SentimentData,
    StockQuote,
)

_logger = logging.getLogger(__name__)


def _patch_py_mini_racer_destructor() -> None:
    """Guard AkShare's optional py_mini_racer dependency against noisy partial init cleanup."""
    try:
        from py_mini_racer import py_mini_racer
    except Exception:
        return

    cls = getattr(py_mini_racer, "MiniRacer", None)
    if cls is None or getattr(cls, "_astock_safe_del", False):
        return

    def _safe_del(self) -> None:
        ext = getattr(self, "ext", None)
        ctx = getattr(self, "ctx", None)
        free_context = getattr(ext, "mr_free_context", None) if ext is not None else None
        if ctx is None or free_context is None:
            return
        try:
            free_context(ctx)
        except Exception:
            return

    cls.__del__ = _safe_del
    cls._astock_safe_del = True


_patch_py_mini_racer_destructor()


# ---------------------------------------------------------------------------
# Protocol 接口
# ---------------------------------------------------------------------------

@runtime_checkable
class MarketDataProvider(Protocol):
    """行情数据源。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        """批量获取实时行情。"""
        ...

    async def get_kline(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        """获取 K 线数据。"""
        ...

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        """获取指数行情。"""
        ...


@runtime_checkable
class FinancialDataProvider(Protocol):
    """财务数据源。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        """获取财务数据。"""
        ...


@runtime_checkable
class FlowDataProvider(Protocol):
    """资金流向数据源。"""

    async def get_fund_flow(self, code: str, days: int) -> Optional[FundFlow]:
        """获取资金流向。"""
        ...


@runtime_checkable
class SentimentProvider(Protocol):
    """舆情数据源。"""

    async def search_news(self, query: str) -> Optional[SentimentData]:
        """搜索新闻/研报。"""
        ...


@runtime_checkable
class ScreenerProvider(Protocol):
    """选股数据源。"""

    async def search_stocks(self, query: str) -> list[dict]:
        """选股筛选。"""
        ...


# ---------------------------------------------------------------------------
# AkShare Adapters（同步 akshare 用 asyncio.to_thread 包装）
# ---------------------------------------------------------------------------

def _to_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        if isinstance(value, str):
            value = value.strip().replace("%", "").replace(",", "")
            if value in {"", "-", "nan", "None"}:
                return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _normalize_a_stock_code(code: str) -> str:
    code = code.strip().lower()
    if code.endswith((".sh", ".sz", ".bj")):
        return code[:6]
    if code.startswith(("sh", "sz", "bj")):
        return code[2:]
    return code


def _normalize_xueqiu_symbol(symbol: str) -> str:
    raw = symbol.strip()
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")) and lower[2:].isdigit():
        return lower[2:]
    return raw


def _extract_a_stock_code(value: object) -> str:
    text = str(value or "")
    match = re.search(r"(?<!\d)(\d{6})(?!\d)", text)
    return match.group(1) if match else ""


def _normalize_opencli_a_stock_symbol(symbol: object, tags: object = "") -> str:
    raw = str(symbol or "").strip()
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")) and lower[2:].isdigit():
        return lower[2:8]
    if raw.isdigit() and len(raw) >= 6:
        return raw[:6]
    return _extract_a_stock_code(tags)


def _xueqiu_symbol(code_or_symbol: str) -> str:
    raw = str(code_or_symbol or "").strip()
    lower = raw.lower()
    if lower.startswith(("sh", "sz", "bj")):
        return raw.upper()
    code = _normalize_a_stock_code(raw)
    if code.isdigit() and len(code) == 6:
        if code.startswith(("6", "9")):
            return f"SH{code}"
        if code.startswith("8"):
            return f"BJ{code}"
        return f"SZ{code}"
    return raw


def _split_tags(value: object) -> list[str]:
    tags = []
    for item in re.split(r"[,，/、\s]+", str(value or "")):
        tag = item.strip()
        if not tag or re.fullmatch(r"\d{6}", tag):
            continue
        tags.append(tag)
    return tags


def _parse_heat_value(value: object) -> int:
    text = str(value or "").replace(",", "").strip()
    if not text:
        return 0
    match = re.search(r"([+-]?\d+(?:\.\d+)?)\s*(亿|万)?", text)
    if not match:
        return _to_int(value)
    number = float(match.group(1))
    unit = match.group(2)
    if unit == "亿":
        number *= 100000000
    elif unit == "万":
        number *= 10000
    return int(number)


def _a_stock_prefix(code: str) -> str:
    code = _normalize_a_stock_code(code)
    if code.startswith(("6", "9")):
        return f"sh{code}"
    if code.startswith("8"):
        return f"bj{code}"
    return f"sz{code}"


_OPENCLI_HOT_SOURCES = ("xueqiu", "eastmoney", "sinafinance", "ths", "tdx")


class OpenCliFinanceAdapter:
    """Use local opencli to fetch optional market-intel signals."""

    def __init__(
        self,
        executable: str | None = None,
        timeout_seconds: int = 45,
        window: str = "background",
        site_session: str = "persistent",
    ):
        self._executable = executable
        self._timeout_seconds = timeout_seconds
        self._window = window
        self._site_session = site_session

    async def get_xueqiu_hot_stocks(self, limit: int = 10, list_type: str = "10") -> list[dict]:
        return await asyncio.to_thread(self._get_xueqiu_hot_stocks_sync, limit, list_type)

    async def get_cross_platform_hot_stocks(self, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._get_cross_platform_hot_stocks_sync, limit)

    async def get_finance_flash(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_finance_flash_sync, limit)

    async def get_global_risk_news(self, limit: int = 12) -> list[dict]:
        return await asyncio.to_thread(self._get_global_risk_news_sync, limit)

    async def get_market_announcements(self, limit: int = 20) -> list[dict]:
        return await asyncio.to_thread(self._get_market_announcements_sync, limit)

    async def get_xueqiu_comments(self, symbol: str, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._get_xueqiu_comments_sync, symbol, limit)

    async def get_daily_dragon_tiger(self, trade_date: str | None = None, min_net_buy: float | None = None) -> dict:
        return await asyncio.to_thread(self._get_daily_dragon_tiger_sync, trade_date, min_net_buy)

    async def get_northbound_realtime(self) -> list[dict]:
        return await asyncio.to_thread(self._get_northbound_realtime_sync)

    async def get_hot_sectors(
        self,
        limit: int = 10,
        sector_type: str = "industry",
        sort: str = "change",
    ) -> list[dict]:
        return await asyncio.to_thread(self._get_hot_sectors_sync, limit, sector_type, sort)

    async def search_market_news(self, query: str, limit: int = 10) -> list[dict]:
        return await asyncio.to_thread(self._search_market_news_sync, query, limit)

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        return {}

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        return None

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        return {}

    def _run_opencli(
        self,
        site: str,
        command: str,
        *positionals: str,
        options: dict[str, object] | None = None,
        browser: bool = False,
        timeout_seconds: int | None = None,
    ) -> list[dict]:
        executable = self._executable or shutil.which("opencli")
        if not executable:
            _logger.info("[OpenCliFinance] opencli 未安装，跳过 %s %s", site, command)
            return []

        cmd = [executable, site, command]
        cmd.extend(str(p) for p in positionals if str(p or ""))
        for flag, value in (options or {}).items():
            if value is None or value == "":
                continue
            cmd.extend([flag, str(value)])
        cmd.extend(["-f", "json"])
        if browser:
            cmd.extend(["--window", self._window, "--site-session", self._site_session])

        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds or self._timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            _logger.warning("[OpenCliFinance] %s %s 获取超时", site, command)
            return []
        except OSError as e:
            _logger.warning(f"[OpenCliFinance] opencli 执行失败: {e}")
            return []

        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "").strip()
            _logger.warning("[OpenCliFinance] %s %s 返回失败: %s", site, command, detail[:240])
            return []

        try:
            payload = json.loads(completed.stdout or "[]")
        except json.JSONDecodeError as e:
            _logger.warning("[OpenCliFinance] %s %s JSON 解析失败: %s", site, command, e)
            return []

        if isinstance(payload, dict) and payload.get("ok") is False:
            _logger.warning("[OpenCliFinance] %s %s 返回错误: %s", site, command, payload.get("error", "unknown"))
            return []
        rows = payload
        if isinstance(payload, dict):
            rows = payload.get("items") or payload.get("data") or payload.get("rows") or []
        if not isinstance(rows, list):
            return []
        return [item for item in rows if isinstance(item, dict)]

    def _get_xueqiu_hot_stocks_sync(self, limit: int = 10, list_type: str = "10") -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 50))
        rows = self._run_opencli(
            "xueqiu",
            "hot-stock",
            options={"--limit": safe_limit, "--type": str(list_type or "10")},
            browser=True,
        )
        results = []
        for item in rows:
            results.append(self._normalize_hot_stock_item("xueqiu", item))
        return results

    def _get_cross_platform_hot_stocks_sync(self, limit: int = 10) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 50))
        all_items: list[dict] = []
        source_specs = (
            ("xueqiu", "hot-stock", {"--limit": safe_limit, "--type": "10"}),
            ("eastmoney", "hot-rank", {"--limit": safe_limit}),
            ("sinafinance", "stock-rank", {"--market": "cn"}),
            ("ths", "hot-rank", {"--limit": safe_limit}),
            ("tdx", "hot-rank", {"--limit": safe_limit}),
        )
        for source, command, options in source_specs:
            rows = self._run_opencli(source, command, options=options, browser=True)
            for item in rows:
                normalized = self._normalize_hot_stock_item(source, item)
                if normalized.get("code") or normalized.get("name"):
                    all_items.append(normalized)
        return self._aggregate_hot_stocks(all_items, safe_limit)

    def _normalize_hot_stock_item(self, source: str, item: dict) -> dict:
        raw_symbol = str(item.get("symbol") or "").strip()
        tags_raw = item.get("tags", "")
        code = _normalize_opencli_a_stock_symbol(raw_symbol, tags_raw)
        if source == "xueqiu" and not code:
            code = _normalize_xueqiu_symbol(raw_symbol)
        name = str(item.get("name") or code or raw_symbol).strip()
        heat_raw = item.get("heat", "")
        return {
            "rank": _to_int(item.get("rank")),
            "symbol": raw_symbol,
            "code": code,
            "name": name,
            "price": _to_float(item.get("price")),
            "change_pct": _to_float(item.get("changePercent", item.get("change_pct", item.get("change")))),
            "heat": _parse_heat_value(heat_raw),
            "heat_text": str(heat_raw or ""),
            "tags": _split_tags(tags_raw),
            "url": item.get("url", ""),
            "source": source,
        }

    def _aggregate_hot_stocks(self, items: list[dict], limit: int) -> list[dict]:
        groups: dict[str, dict] = {}
        source_order = {name: i for i, name in enumerate(_OPENCLI_HOT_SOURCES)}
        for item in items:
            key = item.get("code") or item.get("name")
            if not key:
                continue
            source = item.get("source", "")
            rank = _to_int(item.get("rank"), 999)
            entry = groups.setdefault(
                key,
                {
                    "code": item.get("code", ""),
                    "name": item.get("name") or key,
                    "price": item.get("price", 0),
                    "change_pct": item.get("change_pct", 0),
                    "best_rank": rank,
                    "score": 0,
                    "sources": [],
                    "source_ranks": {},
                    "tags": [],
                    "items": [],
                },
            )
            if source and source not in entry["sources"]:
                entry["sources"].append(source)
            if source:
                prev_rank = entry["source_ranks"].get(source, 999)
                entry["source_ranks"][source] = min(prev_rank, rank)
            entry["best_rank"] = min(entry["best_rank"], rank)
            entry["score"] += max(1, 60 - rank)
            if not entry.get("price") and item.get("price"):
                entry["price"] = item["price"]
            if item.get("change_pct"):
                entry["change_pct"] = item["change_pct"]
            for tag in item.get("tags", []):
                if tag not in entry["tags"]:
                    entry["tags"].append(tag)
            entry["items"].append(item)

        rows = []
        for entry in groups.values():
            entry["sources"].sort(key=lambda s: source_order.get(s, 99))
            entry["source_count"] = len(entry["sources"])
            rows.append(entry)
        rows.sort(key=lambda x: (-x["source_count"], -x["score"], x["best_rank"], x["name"]))
        for idx, row in enumerate(rows[:limit], start=1):
            row["rank"] = idx
        return rows[:limit]

    def _get_finance_flash_sync(self, limit: int = 20) -> list[dict]:
        safe_limit = max(1, min(int(limit or 20), 50))
        items: list[dict] = []
        for row in self._run_opencli("eastmoney", "kuaixun", options={"--limit": safe_limit}, browser=False):
            title = str(row.get("title") or "").strip()
            summary = str(row.get("summary") or "").strip()
            items.append({
                "time": row.get("time", ""),
                "title": title or summary[:80],
                "summary": summary,
                "content": summary,
                "stocks": row.get("stocks", ""),
                "source": "eastmoney",
            })
        for row in self._run_opencli("sinafinance", "news", options={"--limit": safe_limit}, browser=False):
            content = str(row.get("content") or "").strip()
            title = self._flash_title(content)
            items.append({
                "id": row.get("id"),
                "time": row.get("time", ""),
                "title": title,
                "summary": content,
                "content": content,
                "views": row.get("views", ""),
                "source": "sinafinance",
            })
        return self._dedupe_news_items(items, safe_limit)

    def _flash_title(self, content: str) -> str:
        text = content.strip()
        if text.startswith("【") and "】" in text:
            return text[1:text.index("】")].strip()
        return text[:60]

    def _get_global_risk_news_sync(self, limit: int = 12) -> list[dict]:
        safe_limit = max(1, min(int(limit or 12), 30))
        items: list[dict] = []
        bloomberg_limit = min(5, safe_limit)
        for feed in ("markets", "economics"):
            for row in self._run_opencli("bloomberg", feed, options={"--limit": bloomberg_limit}, browser=False):
                items.append({
                    "title": row.get("title", ""),
                    "summary": row.get("summary", ""),
                    "url": row.get("link", ""),
                    "source": "bloomberg",
                    "channel": feed,
                })
        reuters_limit = min(5, safe_limit)
        for row in self._run_opencli(
            "reuters",
            "search",
            "China markets",
            options={"--limit": reuters_limit},
            browser=True,
            timeout_seconds=min(self._timeout_seconds, 20),
        ):
            items.append({
                "title": row.get("title", ""),
                "summary": "",
                "time": row.get("date", ""),
                "url": row.get("url", ""),
                "source": "reuters",
                "channel": row.get("section", "") or row.get("section_path", ""),
            })
        return self._dedupe_news_items(items, safe_limit)

    def _get_market_announcements_sync(self, limit: int = 20) -> list[dict]:
        safe_limit = max(1, min(int(limit or 20), 100))
        items = []
        for row in self._run_opencli("eastmoney", "announcement", options={"--limit": safe_limit}, browser=False):
            items.append({
                "time": row.get("time", ""),
                "code": _normalize_a_stock_code(str(row.get("code", ""))),
                "name": row.get("name", ""),
                "title": row.get("title", ""),
                "category": row.get("category", ""),
                "url": row.get("url", ""),
                "source": "eastmoney",
            })
        return items[:safe_limit]

    def _get_xueqiu_comments_sync(self, symbol: str, limit: int = 10) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 50))
        xq_symbol = _xueqiu_symbol(symbol)
        items = []
        for row in self._run_opencli(
            "xueqiu",
            "comments",
            xq_symbol,
            options={"--limit": safe_limit},
            browser=True,
            timeout_seconds=min(self._timeout_seconds, 30),
        ):
            items.append({
                "author": row.get("author", ""),
                "text": row.get("text", ""),
                "likes": _to_int(row.get("likes")),
                "replies": _to_int(row.get("replies")),
                "retweets": _to_int(row.get("retweets")),
                "created_at": row.get("created_at", ""),
                "url": row.get("url", ""),
                "source": "xueqiu",
            })
        return items

    def _get_daily_dragon_tiger_sync(self, trade_date: str | None = None, min_net_buy: float | None = None) -> dict:
        options: dict[str, object] = {"--limit": 100}
        if trade_date:
            options["--date"] = trade_date
        stocks = []
        for row in self._run_opencli("eastmoney", "longhu", options=options, browser=False):
            net_buy_wan = _to_float(row.get("netAmt")) / 10000
            if min_net_buy is not None and net_buy_wan < min_net_buy:
                continue
            stocks.append({
                "code": _normalize_a_stock_code(str(row.get("code", ""))),
                "name": row.get("name", ""),
                "reason": row.get("reason", ""),
                "close": _to_float(row.get("closePrice")),
                "change_pct": round(_to_float(row.get("changeRate")), 2),
                "net_buy_wan": round(net_buy_wan, 1),
                "buy_wan": round(_to_float(row.get("buyAmt")) / 10000, 1),
                "sell_wan": round(_to_float(row.get("sellAmt")) / 10000, 1),
                "turnover": _to_float(row.get("turnover")),
                "market": row.get("market", ""),
                "trade_date": row.get("tradeDate", ""),
            })
        actual_date = stocks[0].get("trade_date", "") if stocks else trade_date
        return {"date": actual_date, "total_records": len(stocks), "stocks": stocks}

    def _get_northbound_realtime_sync(self) -> list[dict]:
        rows = []
        for row in self._run_opencli("eastmoney", "northbound", options={"--limit": 10}, browser=False):
            rows.append({
                "time": row.get("time", ""),
                "cumulative_net_yi": _to_float(row.get("cumulativeNetYi")),
                "minute_net_yi": _to_float(row.get("minuteNetYi")),
                "total_net_yi": _to_float(row.get("totalNetYi")),
                "source": "eastmoney",
            })
        return rows

    def _get_hot_sectors_sync(
        self,
        limit: int = 10,
        sector_type: str = "industry",
        sort: str = "change",
    ) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 100))
        normalized_type = sector_type if sector_type in {"industry", "concept", "region"} else "industry"
        normalized_sort = sort if sort in {"change", "drop", "money-flow", "out-flow", "turnover"} else "change"
        rows = []
        for row in self._run_opencli(
            "eastmoney",
            "sectors",
            options={"--type": normalized_type, "--sort": normalized_sort, "--limit": safe_limit},
            browser=False,
        ):
            rows.append({
                "rank": _to_int(row.get("rank")),
                "code": str(row.get("code") or "").strip(),
                "name": str(row.get("name") or "").strip(),
                "price": _to_float(row.get("price")),
                "change_pct": _to_float(row.get("changePercent", row.get("change_pct"))),
                "main_net": _to_float(row.get("mainNet", row.get("main_net"))),
                "lead_stock": str(row.get("leadStock") or row.get("lead_stock") or "").strip(),
                "lead_change_pct": _to_float(row.get("leadChangePercent", row.get("lead_change_pct"))),
                "up_count": _to_int(row.get("upCount", row.get("up_count"))),
                "down_count": _to_int(row.get("downCount", row.get("down_count"))),
                "type": normalized_type,
                "sort": normalized_sort,
                "source": "eastmoney",
            })
        return rows[:safe_limit]

    def _search_market_news_sync(self, query: str, limit: int = 10) -> list[dict]:
        safe_limit = max(1, min(int(limit or 10), 40))
        query_text = str(query or "").strip()
        tokens = self._market_news_tokens(query_text)
        flash = self._get_finance_flash_sync(limit=max(safe_limit * 3, 20))
        if tokens:
            flash = [item for item in flash if self._news_matches_tokens(item, tokens)]

        items = flash[:safe_limit]
        if self._query_wants_global(query_text):
            reuters_query = query_text if re.search(r"[A-Za-z]", query_text) else "China markets"
            for row in self._run_opencli(
                "reuters",
                "search",
                reuters_query,
                options={"--limit": min(10, safe_limit)},
                browser=True,
                timeout_seconds=min(self._timeout_seconds, 20),
            ):
                items.append({
                    "rank": _to_int(row.get("rank")),
                    "title": row.get("title", ""),
                    "summary": "",
                    "time": row.get("date", ""),
                    "url": row.get("url", ""),
                    "source": "reuters",
                    "channel": row.get("section", "") or row.get("section_path", ""),
                })
        return self._dedupe_news_items(items, safe_limit)

    def _market_news_tokens(self, query: str) -> list[str]:
        text = str(query or "").lower()
        generic_terms = (
            "今天", "今日", "有没有", "什么", "哪些", "那个", "几个", "热点", "新闻", "快讯",
            "板块", "最强", "强势", "财经", "市场", "一下", "看看", "了吗", "是否", "和",
        )
        for term in generic_terms:
            text = text.replace(term, " ")
        tokens = re.findall(r"[a-z0-9][a-z0-9._-]+|[\u4e00-\u9fff]{2,}", text)
        seen = set()
        result = []
        for token in tokens:
            if token not in seen:
                seen.add(token)
                result.append(token)
        return result

    def _query_wants_global(self, query: str) -> bool:
        lowered = str(query or "").lower()
        global_terms = (
            "海外", "全球", "国际", "美国", "美联储", "fed", "rate", "inflation", "china",
            "reuters", "bloomberg", "tariff", "oil", "yield", "macro",
        )
        return bool(re.search(r"[A-Za-z]", lowered)) or any(term in lowered for term in global_terms)

    def _news_matches_tokens(self, item: dict, tokens: list[str]) -> bool:
        haystack = " ".join(
            str(item.get(key) or "").lower()
            for key in ("title", "summary", "content", "stocks", "channel")
        )
        return any(token in haystack for token in tokens)

    def _dedupe_news_items(self, items: list[dict], limit: int) -> list[dict]:
        seen = set()
        deduped = []
        for item in items:
            title = str(item.get("title") or item.get("summary") or "").strip()
            if not title:
                continue
            key = re.sub(r"\s+", "", title)[:80]
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        deduped.sort(key=lambda x: str(x.get("time", "")), reverse=True)
        return deduped[:limit]


class OpenCliXueqiuAdapter(OpenCliFinanceAdapter):
    """Backward-compatible name for the opencli Xueqiu/finance adapter."""


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

    async def get_fund_flow(self, code: str, days: int = 5) -> Optional[FundFlow]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_fund_flow_sync, code, days)

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
            if str(d.get("ResultCode", -1)) != "0":
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
            _logger.warning(f"[BaiduFundFlow] {code} 历史资金流获取失败: {e}")
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
            _logger.warning(f"[BaiduFundFlow] {code} 实时资金流获取失败: {e}")
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
            _logger.warning(f"[AStockSignal] 东财行业对比获取失败: {e}")
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
            _logger.warning(f"[AStockSignal] 新浪行业对比获取失败: {e}")
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
            _logger.warning(f"[AStockSignal] 行业对比获取失败: {e}")
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


class AkShareMarketAdapter:
    """AkShare 行情 adapter（仅 A 股）。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        # 港股代码不处理（避免把 09927 当成 A 股 601127）
        cn_codes = [c for c in codes if not is_hk_code(c)]
        if not cn_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, cn_codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # AkShare 不负责指数行情，指数由 MXMarketAdapter 提供（见 _get_index_sync stub）
        return await asyncio.to_thread(self._get_index_sync, symbols)

    async def get_sector_heatmap(self) -> list[dict]:
        """获取行业板块热力图数据。

        优先东财（stock_board_industry_name_em），失败则降级新浪行业
        （stock_sector_spot），两者独立数据源。
        返回字段：板块名称、涨跌幅、成交额、上涨家数、下跌家数。
        按涨跌幅降序排列。
        """
        return await asyncio.to_thread(self._get_sector_heatmap_sync)

    async def get_market_stats(self) -> dict:
        """获取全市场涨跌家数统计（新浪A股批量并发拉取）。

        Returns:
            {"up": int, "down": int, "flat": int, "total": int}
        """
        return await asyncio.to_thread(self._get_market_stats_sync)

    def _get_sector_heatmap_sync(self) -> list[dict]:
        """同步实现：拉东财行业板块行情，失败则降级到新浪行业。"""
        # 方式1：东财行业板块
        df = self._fetch_em_industry()
        if df is not None:
            return df

        # 方式2：新浪行业（独立数据源）
        df2 = self._fetch_sina_industry()
        if df2 is not None:
            return df2

        _logger.warning("[AkShareMarket] 行业板块：东财和新浪均获取失败")
        return []

    def _fetch_em_industry(self) -> Optional[list[dict]]:
        """拉东财行业板块（stock_board_industry_name_em）。"""
        try:
            import akshare as ak
            df = ak.stock_board_industry_name_em()
            if df is None or df.empty:
                return None
            rename = {
                "板块名称": "name",
                "涨跌幅": "change_pct",
                "成交额": "amount",
                "上涨家数": "up_count",
                "下跌家数": "down_count",
            }
            col_map = {}
            for old, new in rename.items():
                for col in df.columns:
                    if old in col:
                        col_map[col] = new
                        break
            if not col_map:
                return None
            df = df.rename(columns=col_map)
            keep = ["name", "change_pct", "amount", "up_count", "down_count"]
            df = df[[c for c in keep if c in df.columns]]
            df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
            records = df.to_dict("records")
            for r in records:
                r["change_pct"] = float(r.get("change_pct", 0) or 0)
                r["amount"] = float(r.get("amount", 0) or 0)
                r["up_count"] = int(r.get("up_count", 0) or 0)
                r["down_count"] = int(r.get("down_count", 0) or 0)
            return records
        except Exception as e:
            _logger.warning(f"[AkShareMarket] 东财行业板块获取失败: {e}")
            return None

    def _fetch_sina_industry(self) -> Optional[list[dict]]:
        """拉新浪行业板块（stock_sector_spot indicator=行业），与东财独立。

        注意：stock_sector_spot 返回的是个股数据，需按板块聚合后才能得到板块行情。
        原始字段：板块、涨跌幅、总成交额 等。
        """
        try:
            import akshare as ak

            df = ak.stock_sector_spot(indicator="行业")
            if df is None or df.empty:
                return None

            # 转换为数值（有些行可能是字符串或带百分号）
            def _to_float(val):
                try:
                    s = str(val).strip().replace("%", "").replace(",", "")
                    return float(s) if s not in ("-", "", "nan") else 0.0
                except (ValueError, TypeError):
                    return 0.0

            df["涨跌幅"] = df["涨跌幅"].apply(_to_float)
            df["总成交额"] = pd.to_numeric(df["总成交额"], errors="coerce").fillna(0)

            # 按板块聚合：涨跌幅取均值，成交额取总和
            # 注意：stock_sector_spot 每个板块只返回1只代表性股票，
            # up_count/down_count 实际意义有限，仅展示代表性股票方向
            agg = (
                df.groupby("板块", as_index=False)
                .agg(
                    change_pct=("涨跌幅", "mean"),
                    amount=("总成交额", "sum"),
                    _rep_direction=("涨跌幅", lambda x: "up" if x.iloc[0] > 0 else "down"),
                )
                .rename(columns={"板块": "name"})
                .sort_values("change_pct", ascending=False)
                .reset_index(drop=True)
            )

            records = agg.to_dict("records")
            for r in records:
                r["change_pct"] = float(r.get("change_pct", 0) or 0)
                r["amount"] = float(r.get("amount", 0) or 0)
                # stock_sector_spot 每个板块仅1只代表性股票，涨跌家数无意义，置0
                r["up_count"] = 0
                r["down_count"] = 0
                r.pop("_rep_direction", None)

            return records
        except Exception as e:
            _logger.warning(f"[AkShareMarket] 新浪行业板块获取失败: {e}")
            return None

    def _get_market_stats_sync(self) -> dict:
        """并发拉取新浪A股全量数据，统计全市场升降家数。

        约5500只A股，每页100只，并发10线程，预期3-5s内完成。
        """
        import requests
        from concurrent.futures import ThreadPoolExecutor, as_completed

        SINA_BASE = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        HEADERS = {"Referer": "https://finance.sina.com.cn"}

        def fetch_page(page: int) -> list:
            params = {
                "page": page, "num": 100, "sort": "symbol",
                "asc": 1, "node": "hs_a", "_s_r_a": "page",
            }
            try:
                r = requests.get(SINA_BASE, params=params, headers=HEADERS, timeout=15)
                data = r.json()
                return data if isinstance(data, list) else []
            except Exception:
                return []

        # 先拿一页确认总数
        first_page = fetch_page(1)
        if not first_page:
            return {"up": 0, "down": 0, "flat": 0, "total": 0}

        all_data = list(first_page)
        total_approx = 5600  # 保守估计
        pages_needed = min((total_approx // 100) + 1, 60)

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(fetch_page, p) for p in range(2, pages_needed + 1)]
            for fut in as_completed(futures):
                for item in fut.result():
                    all_data.append(item)

        up = sum(1 for d in all_data if float(d.get("changepercent") or 0) > 0)
        down = sum(1 for d in all_data if float(d.get("changepercent") or 0) < 0)
        flat = len(all_data) - up - down
        return {"up": up, "down": down, "flat": flat, "total": len(all_data)}

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        """主入口：优先东财全量，失败则降级到新浪分时。"""
        try:
            import akshare as ak

            # 过滤掉港股代码
            a_codes = [c for c in codes if not is_hk_code(c)]
            if not a_codes:
                return {}

            df = ak.stock_zh_a_spot_em()
            if df is not None and not df.empty:
                code_set = set(a_codes)
                result = {}
                for _, row in df.iterrows():
                    code = str(row.get("代码", "")).strip()
                    if code not in code_set:
                        continue
                    result[code] = StockQuote(
                        code=code,
                        name=str(row.get("名称", "")),
                        price=float(row.get("最新价", 0) or 0),
                        open=float(row.get("今开", 0) or 0),
                        high=float(row.get("最高", 0) or 0),
                        low=float(row.get("最低", 0) or 0),
                        close=float(row.get("最新价", 0) or 0),
                        volume=int(row.get("成交量", 0) or 0),
                        amount=float(row.get("成交额", 0) or 0),
                        change_pct=float(row.get("涨跌幅", 0) or 0),
                    )
                if result:
                    return result
        except Exception:
            pass

        # 东财断线，降级到新浪分时（收盘后场景，每只股票单独请求）
        return self._get_realtime_from_sina_tick(codes)

    def _get_realtime_from_sina_tick(self, codes: list[str]) -> dict[str, StockQuote]:
        """新浪分时降级：用当日最后一笔的价格和成交额估算实时行情。

        仅在东财 stock_zh_a_spot_em 断线时使用。
        收盘后可用，日内数据有限（无盘前集合竞价价格）。
        """

        result = {}
        today = local_today().strftime("%Y%m%d")

        for code in codes:
            if is_hk_code(code):
                continue
            try:
                import akshare as ak

                # 标准化新浪格式
                symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
                df = ak.stock_intraday_sina(symbol=symbol, date=today)
                if df is None or df.empty:
                    continue

                last_row = df.iloc[-1]
                price = float(last_row["price"])
                prev_price = float(last_row["prev_price"]) if last_row["prev_price"] else price
                change_pct = ((price - prev_price) / prev_price * 100) if prev_price > 0 else 0.0

                # 成交额 = sum(price * volume)，新浪分时 volume 是累计成交量
                total_volume = float(df["volume"].sum()) if "volume" in df.columns else 0.0
                amount = float(last_row["volume"]) * price if "volume" in df.columns else 0.0

                result[code] = StockQuote(
                    code=code,
                    name=str(df.iloc[0]["name"]) if "name" in df.columns else code,
                    price=price,
                    open=0.0,  # 新浪分时无开盘价
                    high=float(df["price"].max()),
                    low=float(df["price"].min()),
                    close=price,
                    volume=int(total_volume),
                    amount=amount,
                    change_pct=round(change_pct, 2),
                )
            except Exception:
                continue

        return result

    def _get_kline_sync(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        """主入口：优先东财日线，失败则降级到腾讯日线。"""
        result = self._get_kline_from_em(code, count)
        if result is not None and not result.empty:
            return result
        return self._get_kline_from_tx(code, count)

    def _get_kline_from_em(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """东财日线（复权）。"""
        try:
            import akshare as ak

            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None

            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)
            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_kline_from_tx(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """腾讯日线降级：无成交量（volume=0），其他字段齐全。

        适用：东财 stock_zh_a_daily 断线时。
        """
        try:
            import akshare as ak
            from datetime import datetime, timedelta

            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            end = datetime.today().strftime("%Y%m%d")
            start = (datetime.today() - timedelta(days=count * 4)).strftime("%Y%m%d")
            df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start, end_date=end)
            if df is None or df.empty:
                return None

            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)

            # 腾讯日线无 volume，用 amount/close 估算成交量（单位一致时有效）
            # 保留 amount 列，volume 置 0（不影响技术指标计算，仅影响 amount 相关维度）
            if "volume" not in df.columns:
                df["volume"] = 0
            if "amount" not in df.columns:
                df["amount"] = 0.0

            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_index_sync(self, symbols: list[str]) -> dict[str, IndexQuote]:
        # 指数行情未实现，由 MXMarketAdapter 提供，此处仅作兜底
        _logger.debug("[AkShareMarket] get_index 未实现，返回空")
        return {}


class AkShareFinancialAdapter:
    """AkShare 财务 adapter。"""

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        try:
            if is_hk_code(code):
                return None
            import akshare as ak
            df = ak.stock_financial_analysis_indicator(symbol=code, start_year="2024")
            if df is None or df.empty:
                return None

            def _latest(col_name_pattern: str) -> Optional[float]:
                col = next((c for c in df.columns if col_name_pattern in str(c)), None)
                if not col:
                    return None
                vals = df[col].dropna().head(4).tolist()
                return round(float(vals[0]), 2) if vals else None

            roe = _latest("净资产收益率")  # 取加权净资产收益率更准确
            if roe is None:
                roe = _latest("总资产净利润率")

            # 营收增长：主营业务收入增长率（最新一期）
            rev_growth = _latest("主营业务收入增长率")

            # 现金流：每股经营性现金流
            cash_flow = _latest("每股经营性现金流")

            # 也尝试总资产净利润率作为备选
            if roe is None:
                roe = _latest("总资产净利润率")

            return FinancialReport(
                roe=roe,
                revenue_growth=rev_growth,
                operating_cash_flow=cash_flow,
            )
        except Exception:
            return None


class AkShareFlowAdapter:
    """AkShare 资金流向 adapter。"""

    async def get_fund_flow(self, code: str, days: int = 5) -> Optional[FundFlow]:
        return await asyncio.to_thread(self._get_flow_sync, code, days)

    def _get_flow_sync(self, code: str, days: int) -> Optional[FundFlow]:
        """主入口：优先东财，失败则降级到腾讯 tick。"""
        try:
            if is_hk_code(code):
                return None
            # 1. 优先东财资金流
            result = self._get_flow_from_em(code, days)
            if result is not None:
                return result
            # 2. 东财断线，降级到腾讯分笔
            result = self._get_flow_from_tx_tick(code)
            if result is not None:
                return result
            return None
        except Exception:
            return None

    def _get_flow_from_em(self, code: str, days: int) -> Optional[FundFlow]:
        """东财资金流接口，失败时返回 None（ caller 会尝试腾讯降级）。"""
        try:
            import akshare as ak
            market = "sh" if code.startswith(("6", "9")) else "sz"
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            if df is None or df.empty:
                return None

            recent = df.tail(days)
            total_net = 0
            outflow_streak = 0
            for _, row in recent.iterrows():
                main_net = 0
                for col in row.index:
                    if "主力" in str(col) and "净" in str(col):
                        main_net = float(row[col]) if pd.notna(row[col]) else 0
                        break
                total_net += main_net
                if main_net < -5_000_000:
                    outflow_streak += 1

            return FundFlow(
                net_inflow_1d=total_net,
                consecutive_outflow_days=outflow_streak,
            )
        except Exception:
            return None

    def _get_flow_from_tx_tick(self, code: str) -> Optional[FundFlow]:
        """腾讯分笔成交降级：按买盘/卖盘汇总估算主力净流入。

        适用场景：东财 stock_individual_fund_flow 接口断线时。
        注意：腾讯 tick 只有当日数据，days 参数被忽略。
        """
        try:
            import akshare as ak

            # 标准化为腾讯格式：sh600415 / sz000001
            if code.startswith(("6", "9")):
                symbol = f"sh{code}"
            else:
                symbol = f"sz{code}"

            df = ak.stock_zh_a_tick_tx_js(symbol=symbol)
            if df is None or df.empty:
                return None

            buy_mask = df["性质"].str.contains("买", na=False)
            sell_mask = df["性质"].str.contains("卖", na=False)

            buy_amount = df.loc[buy_mask, "成交金额"].sum()
            sell_amount = df.loc[sell_mask, "成交金额"].sum()
            total_amount = buy_amount + sell_amount

            net_inflow = buy_amount - sell_amount  # 正=净流入，负=净流出

            # 主买占比（主力参与度）
            main_force_ratio = buy_amount / total_amount if total_amount > 0 else 0.0

            # 连续流出判断：今日净流出且金额 > 5000万 → outflow_streak=1（腾讯tick只当日）
            # 明日需要重新获取才知道是否连续，所以最多记1天
            outflow_streak = 1 if (net_inflow < -5_000_000) else 0

            return FundFlow(
                net_inflow_1d=net_inflow,
                net_inflow_5d=net_inflow,  # 腾讯tick仅当日，退化为单日
                main_force_ratio=main_force_ratio,
                consecutive_outflow_days=outflow_streak,
            )
        except Exception:
            return None


# ---------------------------------------------------------------------------
# MX Adapters
# ---------------------------------------------------------------------------

class MXSentimentAdapter:
    """妙想舆情 adapter。"""

    async def search_news(self, query: str) -> Optional[SentimentData]:
        return await asyncio.to_thread(self._sync, query)

    def _sync(self, query: str) -> Optional[SentimentData]:
        try:
            from astock_trading.market.mx.search import MXSearch
            mx = MXSearch()
            result = mx.search(query)

            data = result.get("data", {})
            inner = data.get("data", {})
            search_resp = inner.get("llmSearchResponse", {})
            items = search_resp.get("data", [])

            if not items:
                return SentimentData(score=1.5, detail="无相关资讯")

            report_count = 0
            positive_count = 0
            negative_count = 0
            for item in items:
                info_type = item.get("informationType", "")
                rating = str(item.get("rating", "")).lower()
                if info_type == "REPORT":
                    report_count += 1
                if any(w in rating for w in ["买入", "增持", "推荐"]):
                    positive_count += 1
                elif any(w in rating for w in ["减持", "卖出"]):
                    negative_count += 1

            score = 1.5
            if report_count >= 5:
                score += 0.5
            elif report_count >= 2:
                score += 0.3
            if positive_count >= 2:
                score += 0.5
            elif positive_count >= 1:
                score += 0.3
            if negative_count >= 2:
                score -= 0.5
            score = max(0, min(score, 3.0))

            return SentimentData(
                score=round(score, 1),
                news_count=len(items),
                positive_ratio=positive_count / max(len(items), 1),
                detail=f"研报{report_count}篇 买入{positive_count} 减持{negative_count}",
            )
        except Exception:
            return None


class MXScreenerAdapter:
    """妙想选股 adapter。"""

    async def search_stocks(self, query: str) -> list[dict]:
        return await asyncio.to_thread(self._sync, query)

    def _sync(self, query: str) -> list[dict]:
        try:
            from astock_trading.market.mx.xuangu import MXXuangu
            mx = MXXuangu()
            result = mx.search(query)
            rows, _, err = mx.extract_data(result)
            if err:
                return []
            return rows
        except Exception:
            return []


class MXMarketAdapter:
    """妙想行情 adapter。"""

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        a_codes = [code for code in codes if not is_hk_code(code)]
        if not a_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, a_codes)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        """获取 A 股指数行情（内部 hardcode 四大指数，无视 symbols 参数）。"""
        return await asyncio.to_thread(self._get_index_sync)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    def _get_kline_sync(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        """从 MX finskillshub API 获取日K线历史数据。

        策略：query= "code 近N个交易日 日K线"，DTO 0 含：
          - headName[]    → 日期列（最新→最旧，降序）
          - 100000000017969 → 开盘价
          - 100000000017975 → 收盘价
          - 100000000019180 → 是否涨停（辅助）
        数据只有开/收，没有 high/low/vol，compute_technical_indicators
        用收盘价计算均线/R SI/MACD，只需 close 列存在即可（其他列填0）。

        兜底：AkShare 东财日线。
        """
        if is_hk_code(code):
            _logger.debug(f"[MXMarket] get_kline 跳过港股代码: {code}")
            return None

        if period != "daily":
            _logger.warning(f"[MXMarket] get_kline 不支持 period={period}，仅支持 daily")
            return None

        import akshare as ak

        # 先试 MX
        mx_df = self._get_kline_from_mx(code, count)
        if mx_df is not None and not mx_df.empty:
            return mx_df

        # MX 失败 → AkShare 东财
        try:
            symbol = f"sh{code}" if code.startswith(("6", "9")) else f"sz{code}"
            df = ak.stock_zh_a_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None
            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)
            df["涨跌幅"] = df["close"].pct_change() * 100
            return df
        except Exception:
            return None

    def _get_kline_from_mx(self, code: str, count: int) -> Optional[pd.DataFrame]:
        """调用 MX finskillshub query 接口拉日K线。"""
        try:
            import httpx
        except ImportError:
            return None

        # 加载 MX API key（与 realtime.py 相同逻辑）
        import os
        from pathlib import Path

        def _load_key():
            key = os.environ.get("MX_APIKEY", "").strip()
            if key:
                return key
            p = Path(__file__).resolve().parent.parent.parent.parent / ".env"
            if p.exists():
                for line in open(p, encoding="utf-8"):
                    line = line.strip()
                    if line.startswith("MX_APIKEY=") and len(line) > 10:
                        return line.split("=", 1)[1].strip()
            return ""

        api_key = _load_key()
        if not api_key:
            return None

        # MX finnhub 返回结构：
        # DTO 0: headName=日期[], 100000000017969=开盘[], 100000000017975=收盘[]
        # 日期列从新→旧排列，需要反转
        _FID_OPEN = "100000000017969"
        _FID_CLOSE = "100000000017975"

        try:
            resp = httpx.post(
                "https://mkapi2.dfcfs.com/finskillshub/api/claw/query",
                json={"toolQuery": f"{code} 近{count}个交易日 日K线"},
                headers={"Content-Type": "application/json", "apikey": api_key},
                timeout=15.0,
            )
            data = resp.json()
        except Exception:
            return None

        try:
            inner = data.get("data", {}).get("data", {})
            dto_list = inner.get("searchDataResultDTO", {}).get("dataTableDTOList", [])
        except Exception:
            return None

        dto = None
        for d in dto_list:
            raw = d.get("rawTable", {})
            if _FID_OPEN in raw and _FID_CLOSE in raw:
                dto = d
                break
        if dto is None:
            return None

        raw = dto.get("rawTable", {})
        head = raw.get("headName", [])

        if not isinstance(head, list) or len(head) < 2:
            return None

        dates = list(reversed(head))            # 新→旧 → 旧→新
        opens = list(reversed(raw.get(_FID_OPEN, [])))
        closes = list(reversed(raw.get(_FID_CLOSE, [])))

        rows = min(len(dates), count * 2)
        rows = max(rows, 5)

        import pandas as pd
        df = pd.DataFrame({
            "date":   dates[-rows:],
            "open":   [float(o) for o in opens[-rows:]],
            "close":  [float(c) for c in closes[-rows:]],
            # MX 没有这些字段，akshare adapter 的 compute_technical_indicators
            # 只强制要求 close，其余列填 0 或 NaN
            "high":   [float(c) for c in closes[-rows:]],   # 近似
            "low":    [float(c) for c in closes[-rows:]],   # 近似
            "volume": [0] * rows,
        })
        df["涨跌幅"] = df["close"].pct_change() * 100
        return df

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            codes = [code for code in codes if not is_hk_code(code)]
            if not codes:
                return {}
            from astock_trading.market.mx.realtime import get_realtime_mx
            raw = get_realtime_mx(codes)
            result = {}
            for code, data in raw.items():
                if "error" in data:
                    continue
                result[code] = StockQuote(
                    code=code,
                    name=data.get("name", code),
                    price=data.get("price", 0),
                    open=data.get("open", 0),
                    high=data.get("high", 0),
                    low=data.get("low", 0),
                    close=data.get("price", 0),
                    volume=int(data.get("volume", 0)),
                    amount=data.get("amount", 0),
                    change_pct=data.get("change_pct", 0),
                )
            return result
        except Exception:
            return {}

    def _get_index_sync(self) -> dict[str, IndexQuote]:
        """获取指数行情，优先 MX，失败则用 akshare 兜底。
        均线/above_ma20/below_ma60_days 由 akshare 日线数据计算。"""
        import akshare as ak
        import pandas as pd

        # 日线代码映射
        code_map = {
            "上证指数": "sh000001",
            "深证成指": "sz399001",
            "创业板指": "sz399006",
            "科创50": "sh000688",
        }

        def _compute_ma(symbol: str) -> tuple[float, float, bool, int]:
            """计算 MA20、MA60、above_ma20、below_ma60_days。"""
            try:
                df = ak.stock_zh_index_daily(symbol=symbol)
                df = df.sort_values("date")
                close = df["close"].astype(float)
                ma20_val = close.rolling(20).mean().iloc[-1] if len(close) >= 20 else 0
                ma60_val = close.rolling(60).mean().iloc[-1] if len(close) >= 60 else 0
                latest_price = close.iloc[-1]
                above = bool(latest_price > ma20_val > 0)
                # below_ma60_days：最近多少个交易日连续低于 MA60
                below_ma60 = (close < ma60_val).iloc[-20:] if ma60_val > 0 else pd.Series(False, index=close.index[-20:])
                count = 0
                for v in reversed(below_ma60.tolist()):
                    if v:
                        count += 1
                    else:
                        break
                return float(ma20_val), float(ma60_val), above, count
            except Exception:
                return 0.0, 0.0, False, 0

        result = {}

        # 优先 MX（获取实时价格/涨跌幅）
        try:
            from astock_trading.market.mx.realtime import get_market_index_mx
            raw = get_market_index_mx()
            for name, data in raw.items():
                if "error" in data:
                    continue
                symbol = code_map.get(name, name)
                ma20, ma60, above_ma20, below_days = _compute_ma(symbol)
                result[name] = IndexQuote(
                    symbol=symbol,
                    name=name,
                    price=data.get("close") or data.get("price", 0) or 0,
                    change_pct=data.get("change_pct", 0) or 0,
                    ma20=ma20,
                    ma60=ma60,
                    above_ma20=above_ma20,
                    below_ma60_days=below_days,
                )
            if result and any(v.price > 0 for v in result.values()):
                # MX 可能返回完整指数名称但部分价格为 null/0（深证/创业板/科创常见）。
                # 只有全部指数都有有效价格时才直接返回；否则继续走 akshare 日线兜底，
                # 补齐缺失或无效价格，避免 market_state 出现 price=0。
                missing_or_invalid = [
                    n for n in code_map
                    if n not in result or (result[n].price or 0) <= 0
                ]
                if not missing_or_invalid:
                    return result
                # 有缺失或价格无效，继续走 akshare 兜底补全
        except Exception as e:
            _logger.warning(f"[MXMarket] MX 指数行情获取失败: {e}")

        # akshare 兜底（价格 + 均线）
        # 注意：stock_zh_index_spot_sina 在批量请求时对部分指数（深证/创业板）返回 0，
        # 改用 stock_zh_index_daily 的最新收盘价作为价格来源，更稳定
        try:
            # Phase 1: 并行拉取所有指数日线数据
            def _fetch_daily(code):
                try:
                    return code, ak.stock_zh_index_daily(symbol=code)
                except Exception:
                    return code, None

            codes = list(code_map.values())
            with ThreadPoolExecutor(max_workers=min(len(codes), 4)) as executor:
                daily_map: dict[str, Optional[pd.DataFrame]] = {
                    code: df for code, df in executor.map(_fetch_daily, codes)
                }

            # Phase 2: 主线程更新 result（已有 MX 数据的补充价格/MA，没有的创建新记录）
            for name, code in code_map.items():
                daily_df = daily_map.get(code)
                if name in result:
                    # MX 已返回数据，用日线补充/替换价格（如果 MX 价格无效）和 MA
                    # 注意：IndexQuote 是 frozen dataclass，不能原地修改，需重建对象
                    existing = result[name]
                    price = existing.price
                    change_pct = existing.change_pct
                    ma20 = existing.ma20
                    ma60 = existing.ma60
                    above_ma20 = existing.above_ma20
                    below_ma60_days = existing.below_ma60_days
                    if daily_df is not None and not daily_df.empty:
                        latest_close = float(daily_df["close"].iloc[-1])
                        prev_close = float(daily_df["close"].iloc[-2]) if len(daily_df) >= 2 else latest_close
                        if (price or 0) <= 0 or price != latest_close:
                            price = latest_close
                        daily_chg = ((latest_close - prev_close) / prev_close * 100) if prev_close > 0 else 0
                        if abs(change_pct) < 0.01 and abs(daily_chg) > 0.01:
                            change_pct = daily_chg
                    # 补全 MA（如果 MX 没有的话）
                    if not ma20 and not ma60:
                        ma20, ma60, above_ma20, below_days = _compute_ma(code)
                        below_ma60_days = below_days
                    # 重建 IndexQuote（替换整个对象）
                    result[name] = IndexQuote(
                        symbol=existing.symbol,
                        name=name,
                        price=price,
                        change_pct=change_pct,
                        ma20=ma20,
                        ma60=ma60,
                        above_ma20=above_ma20,
                        below_ma60_days=below_ma60_days,
                        timestamp=existing.timestamp,
                    )
                else:
                    # 完全没有数据，用日线构建完整记录
                    price_val = 0.0
                    change_val = 0.0
                    if daily_df is not None and not daily_df.empty:
                        latest_close = float(daily_df["close"].iloc[-1])
                        prev_close = float(daily_df["close"].iloc[-2]) if len(daily_df) >= 2 else latest_close
                        price_val = latest_close
                        change_val = ((latest_close - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
                    ma20, ma60, above_ma20, below_days = _compute_ma(code)
                    result[name] = IndexQuote(
                        symbol=code,
                        name=name,
                        price=price_val,
                        change_pct=change_val,
                        ma20=ma20,
                        ma60=ma60,
                        above_ma20=above_ma20,
                        below_ma60_days=below_days,
                    )
            return result
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# BaoStock Adapters
# ---------------------------------------------------------------------------

# 模块级 session 管理，避免频繁 login/logout
_bs_lock = threading.RLock()
_bs_logged_in = False


def _bs_ensure_login():
    """确保已登录 baostock（线程安全）。"""
    global _bs_logged_in
    with _bs_lock:
        if not _bs_logged_in:
            import baostock as bs
            lg = bs.login()
            if lg.error_code != "0":
                raise RuntimeError(f"baostock login failed: {lg.error_msg} {lg.error_msg}")
            _bs_logged_in = True


def _bs_logout():
    """登出 baostock。"""
    global _bs_logged_in
    with _bs_lock:
        if _bs_logged_in:
            import baostock as bs
            bs.logout()
            _bs_logged_in = False


def _normalize_baostock_code(code: str) -> str:
    """将纯数字 A 股代码标准化为 baostock 格式（sh.600000 / sz.000001）。

    baostock 代码规则：
    - 沪市：sh.6xxxxx
    - 深市：sz.0xxxxx / sz.3xxxxx（创业板）
    - 北交所：bj.8xxxxx
    """
    code = code.strip()
    if "." in code:
        return code.lower()
    if code.startswith(("6", "9")):
        return f"sh.{code}"
    if code.startswith(("0", "3")):
        return f"sz.{code}"
    if code.startswith("8"):
        return f"bj.{code}"
    # 兜底：加 sh
    return f"sh.{code}"


def _to_baostock_code(code: str) -> str:
    """兼容处理：code 可能已经是 sh.600000 格式或纯数字。"""
    return _normalize_baostock_code(code)


class BaoStockMarketAdapter:
    """BaoStock 历史数据 adapter（主要用于回测场景）。

    支持：
    - 日/周/月 K 线（前复权、后复权、不复权）
    - 5/15/30/60 分钟 K 线
    - 指数 K 线

    注意：baostock 不提供实时行情，get_realtime() 返回最新一日快照
    （等同于当日收盘价），仅作兜底使用。
    """

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        # baostock 无真正实时，此处用最新日K 快照兜底
        result = {}
        for code in codes:
            if is_hk_code(code):
                continue
            bs_code = _to_baostock_code(code)
            df = await self.get_kline(bs_code, period="daily", count=1)
            if df is not None and not df.empty:
                row = df.iloc[-1]
                result[code] = StockQuote(
                    code=code,
                    name=str(row.get("名称", code)),
                    price=float(row.get("收盘", 0)),
                    open=float(row.get("开盘", 0)),
                    high=float(row.get("最高", 0)),
                    low=float(row.get("最低", 0)),
                    close=float(row.get("收盘", 0)),
                    volume=int(float(row.get("成交量", 0))),
                    amount=float(row.get("成交额", 0)),
                    change_pct=float(row.get("涨跌幅", 0) or 0),
                )
        return result

    async def get_kline(
        self,
        code: str,
        period: str = "daily",
        count: int = 120,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        adjustflag: str = "2",
    ) -> Optional[pd.DataFrame]:
        """获取 K 线数据。

        Args:
            code: 股票代码（支持 sh.600000 / sz.000001 / 600000 等格式）
            period: 日线周期
                - "daily" / "d": 日 K（默认）
                - "weekly" / "w": 周 K
                - "monthly" / "m": 月 K
                - "5"/"15"/"30"/"60": 分钟 K
            count: 获取最近 N 条（与 start_date 二选一）
            start_date: 开始日期 "YYYY-MM-DD"（与 count 二选一）
            end_date: 结束日期 "YYYY-MM-DD"
            adjustflag: 复权类型
                - "3": 不复权（默认）
                - "2": 前复权（回测推荐）
                - "1": 后复权

        Returns:
            DataFrame，列名对齐 MarketStore.save_bars()：
            日期, 开盘, 最高, 最低, 收盘, 成交量, 成交额, 涨跌幅, 证券名称
        """
        return await asyncio.to_thread(
            self._get_kline_sync, code, period, count, start_date, end_date, adjustflag
        )

    def _get_kline_sync(
        self,
        code: str,
        period: str,
        count: int,
        start_date: Optional[str],
        end_date: Optional[str],
        adjustflag: str,
    ) -> Optional[pd.DataFrame]:
        try:
            import baostock as bs

            bs_code = _to_baostock_code(code)
            _bs_ensure_login()

            # period 标准化
            freq_map = {"daily": "d", "d": "d", "weekly": "w", "w": "w", "monthly": "m", "m": "m",
                        "5": "5", "15": "15", "30": "30", "60": "60"}
            freq = freq_map.get(period, "d")

            # 字段列表
            # 注意：分钟线不支持 pctChg 和 turn
            is_minute = freq not in ("d", "w", "m")
            fields = "date,code,open,high,low,close,volume,amount,adjustflag"
            if not is_minute:
                fields += ",pctChg"
            if freq in ("w", "m"):
                fields += ",turn"
            if is_minute:
                fields += ",time"

            # 计算日期范围（当指定 count 而非起止日期时）
            if start_date is None and count > 0:
                from datetime import timedelta
                end_dt = local_now().replace(tzinfo=None)
                if freq == "d":
                    days = min(count * 3, 5000)
                elif freq in ("w", "m"):
                    # 周/月线：每单元跨度大
                    weeks_per_bar = 1 if freq == "w" else 4
                    days = min(count * weeks_per_bar * 14, 5000)
                else:
                    # 分钟线：每交易日约 240 条，估算所需交易日
                    trading_days = (count // 240) + 2
                    days = trading_days * 7 + 7
                start_dt = end_dt - timedelta(days=days)
                start_date = start_dt.strftime("%Y-%m-%d")
                end_date = end_dt.strftime("%Y-%m-%d")

            rs = bs.query_history_k_data_plus(
                bs_code,
                fields,
                start_date=start_date or "",
                end_date=end_date or "",
                frequency=freq,
                adjustflag=adjustflag,
            )

            if rs.error_code != "0":
                return None

            data_list = []
            while rs.error_code == "0" and rs.next():
                data_list.append(rs.get_row_data())

            if not data_list:
                return None

            df = pd.DataFrame(data_list, columns=rs.fields)

            # 取最近 count 条
            if count > 0 and len(df) > count:
                df = df.tail(count).reset_index(drop=True)

            # 列名对齐 MarketStore.save_bars()
            rename = {
                "date": "日期",
                "open": "开盘",
                "high": "最高",
                "low": "最低",
                "close": "收盘",
                "volume": "成交量",
                "amount": "成交额",
                "pctChg": "涨跌幅",
                "adjustflag": "复权类型",
                "code": "证券代码",
                "turn": "换手率",
            }
            df.rename(columns=rename, inplace=True)

            # 分钟数据：组合 date + time → 日期（baostock 分钟 time=YYYYMMDDHHMMSSmmm）
            if is_minute and "time" in df.columns:
                def _combine_dt(row):
                    t = str(row.get("time", ""))
                    if len(t) >= 14:
                        return f"{t[0:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"
                    return str(row.get("日期", ""))
                df["日期"] = df.apply(_combine_dt, axis=1)
                df.drop(columns=["time"], inplace=True)

            # 数值类型转换（baostock 全返回字符串）
            numeric_cols = ["开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅", "换手率"]
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

            # 获取证券名称
            name = self._get_stock_name(bs_code)
            df["证券名称"] = name
            df["名称"] = name

            return df

        except Exception:
            return None

    def _get_stock_name(self, bs_code: str) -> str:
        """通过 query_trade_dates 获取证券简称（近似）。"""
        try:
            import baostock as bs

            _bs_ensure_login()
            rs = bs.query_stock_basic(code=bs_code)
            if rs.error_code == "0":
                while rs.next():
                    row = rs.get_row_data()
                    if len(row) >= 3:
                        return row[3] or row[1] or bs_code
            return bs_code
        except Exception:
            return bs_code

    def __del__(self):
        """全局 session 不在此关闭，由模块级函数统一管理。"""
        pass


# ---------------------------------------------------------------------------
# 港股代码识别
# ---------------------------------------------------------------------------

def is_hk_code(code: str) -> bool:
    """判断是否为港股代码。

    港股代码规则：
    - 5 位纯数字且以 0 开头（如 09927, 00700, 01810）
    - 或显式带 hk 前缀（如 hk09927）
    """
    code = code.strip().lower()
    if code.startswith("hk"):
        return True
    # 5 位数字且以 0 开头 → 港股
    if len(code) == 5 and code.isdigit() and code.startswith("0"):
        # 排除 A 股深市 00xxx 开头的（深市主板 000xxx 是 6 位）
        # 5 位且 0 开头 → 港股
        return True
    return False


def normalize_hk_code(code: str) -> str:
    """标准化港股代码为 5 位纯数字（去掉 hk 前缀）。"""
    code = code.strip().lower()
    if code.startswith("hk"):
        code = code[2:]
    return code.zfill(5)


# ---------------------------------------------------------------------------
# AkShare 港股 Adapters
# ---------------------------------------------------------------------------

class AkShareHKMarketAdapter:
    """AkShare 港股行情 adapter。

    数据源：
    - 实时行情：stock_hk_spot_em()（东财港股全市场快照）
    - K 线：stock_hk_daily(symbol, adjust='qfq')
    """

    async def get_realtime(self, codes: list[str]) -> dict[str, StockQuote]:
        hk_codes = [c for c in codes if is_hk_code(c)]
        if not hk_codes:
            return {}
        return await asyncio.to_thread(self._get_realtime_sync, hk_codes)

    async def get_kline(self, code: str, period: str = "daily", count: int = 120) -> Optional[pd.DataFrame]:
        if not is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_kline_sync, code, period, count)

    async def get_index(self, symbols: list[str]) -> dict[str, IndexQuote]:
        _logger.debug("[AkShareHKMarket] 港股指数暂不支持")
        return {}  # 港股指数暂不支持

    def _get_realtime_sync(self, codes: list[str]) -> dict[str, StockQuote]:
        try:
            import akshare as ak
            df = ak.stock_hk_spot_em()
            if df is None or df.empty:
                return {}

            # 标准化待查代码
            lookup = {}
            for c in codes:
                norm = normalize_hk_code(c)
                lookup[norm] = c  # norm → original code

            result = {}
            for _, row in df.iterrows():
                raw_code = str(row.get("代码", "")).strip()
                # stock_hk_spot_em 的代码列可能是 "09927" 或 "9927"
                norm = raw_code.zfill(5)
                if norm not in lookup:
                    continue
                original = lookup[norm]
                result[original] = StockQuote(
                    code=original,
                    name=str(row.get("名称", "")),
                    price=float(row.get("最新价", 0) or 0),
                    open=float(row.get("今开", 0) or 0),
                    high=float(row.get("最高", 0) or 0),
                    low=float(row.get("最低", 0) or 0),
                    close=float(row.get("最新价", 0) or 0),
                    volume=int(row.get("成交量", 0) or 0),
                    amount=float(row.get("成交额", 0) or 0),
                    change_pct=float(row.get("涨跌幅", 0) or 0),
                )
            return result
        except Exception:
            return {}

    def _get_kline_sync(self, code: str, period: str, count: int) -> Optional[pd.DataFrame]:
        try:
            import akshare as ak

            symbol = normalize_hk_code(code)
            df = ak.stock_hk_daily(symbol=symbol, adjust="qfq")
            if df is None or df.empty:
                return None

            # stock_hk_daily 返回英文列名：date, open, high, low, close, volume, amount
            df = df.sort_values("date").tail(count * 2).reset_index(drop=True)

            # 添加涨跌幅列
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df["涨跌幅"] = df["close"].pct_change() * 100

            return df
        except Exception:
            return None


class AkShareHKFinancialAdapter:
    """港股财务 adapter — 使用 akshare 港股财务接口。

    akshare 港股财务数据有限，尽力获取，获取不到返回 None。
    """

    async def get_financial(self, code: str) -> Optional[FinancialReport]:
        if not is_hk_code(code):
            return None
        return await asyncio.to_thread(self._get_financial_sync, code)

    def _get_financial_sync(self, code: str) -> Optional[FinancialReport]:
        try:
            import akshare as ak
            symbol = normalize_hk_code(code)

            # 尝试 stock_hk_valuation_baidu（百度港股估值）
            try:
                df = ak.stock_hk_valuation_baidu(symbol=symbol, indicator="总市值", period="近一年")
                if df is not None and not df.empty:
                    # 只能拿到估值，没有 ROE 等
                    return FinancialReport()
            except Exception as e:
                _logger.debug(f"[AkShareHKFinancial] {code} 百度估值接口失败: {e}")

            # 港股财务数据有限，返回空报告（不阻塞评分，降级处理）
            _logger.info(f"[AkShareHKFinancial] {code} 港股财务数据有限，降级返回空报告")
            return FinancialReport()
        except Exception as e:
            _logger.warning(f"[AkShareHKFinancial] {code} 财务数据获取异常: {e}")
            return None
