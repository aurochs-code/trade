"""
market/store.py — 市场观察存储

追加到 market_observations / market_bars，提供 TTL 缓存检查。
"""

from __future__ import annotations

import json
import uuid
import hashlib
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd


# TTL 配置（秒）
TTL_CONFIG = {
    "quote": 30,
    "technical": 300,
    "financial": 86400,
    "flow": 600,
    "sentiment": 1800,
    "index": 60,
    "market_state": 1800,
}


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _jsonable_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _jsonable_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item") and value.__class__.__module__.startswith("numpy"):
        return value.item()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _float_or_none(value: Any) -> float | None:
    if value in (None, "", "None"):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    if value in (None, "", "None"):
        return 0
    try:
        if pd.isna(value):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _cents(value: Any) -> int:
    number = _float_or_none(value)
    return int(round((number or 0.0) * 100))


class MarketRepository:
    """Repository for market observations and bars."""

    def __init__(self, conn: Any):
        self._conn = conn

    def save_observation(
        self,
        source: str,
        kind: str,
        symbol: str,
        payload: dict,
        run_id: Optional[str] = None,
    ) -> str:
        """追加到 market_observations，返回 observation_id。"""
        obs_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        self._conn.execute(
            """INSERT OR REPLACE INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                obs_id,
                source,
                kind,
                symbol,
                now,
                run_id,
                json.dumps(_jsonable_value(payload), ensure_ascii=False),
            ),
        )
        return obs_id

    def save_provider_failure(
        self,
        source: str,
        target_kind: str,
        symbol: str,
        status: str,
        error_type: str,
        error_message: str,
        run_id: Optional[str] = None,
        details: Optional[dict] = None,
    ) -> str:
        """记录 provider 失败，不计入目标数据源的成功观测。"""
        payload = {
            "source": source,
            "target_kind": target_kind,
            "symbol": symbol,
            "status": status,
            "error_type": error_type,
            "error_message": error_message,
        }
        if details:
            payload["details"] = details
        return self.save_observation(
            source=source,
            kind="provider_failure",
            symbol=symbol,
            payload=payload,
            run_id=run_id,
        )

    def get_latest_observation(
        self,
        symbol: str,
        kind: str,
        max_age_seconds: Optional[int] = None,
    ) -> Optional[dict]:
        """获取最新观测，可选 TTL 检查。"""
        row = self._conn.execute(
            """SELECT payload_json, observed_at FROM market_observations
               WHERE symbol = ? AND kind = ?
               ORDER BY observed_at DESC LIMIT 1""",
            (symbol, kind),
        ).fetchone()

        if not row:
            return None

        if max_age_seconds is not None:
            observed = datetime.fromisoformat(row["observed_at"])
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - observed).total_seconds()
            if age > max_age_seconds:
                return None  # TTL 过期

        return _decode_json(row["payload_json"])

    def get_cached(self, symbol: str, kind: str) -> Optional[dict]:
        """TTL 缓存检查，使用默认 TTL。返回 None 如果缓存数据不完整。"""
        ttl = TTL_CONFIG.get(kind, 300)
        data = self.get_latest_observation(symbol, kind, max_age_seconds=ttl)
        if data is None:
            return None
        # 校验字段完整性：旧缓存（kind='quote' 只存了 close/name）不完整，拒绝复用
        if kind == "quote":
            from dataclasses import fields as dc_fields
            from astock_trading.market.models import StockQuote
            stored_keys = set(data.keys())
            required_keys = {f.name for f in dc_fields(StockQuote)}
            if not required_keys.issubset(stored_keys):
                return None
        return data

    def save_bars(self, symbol: str, bars_df: pd.DataFrame, source: str = "akshare") -> int:
        """追加到 market_bars（金额存分），返回写入行数。"""
        now = datetime.now(timezone.utc).isoformat()
        count = 0

        for _, row in bars_df.iterrows():
            try:
                self._conn.execute(
                    """INSERT OR REPLACE INTO market_bars
                       (symbol, bar_date, period, open_cents, high_cents, low_cents,
                        close_cents, volume, amount_cents, source, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        symbol,
                        str(row.get("日期", row.get("date", ""))),
                        "daily",
                        int(float(row.get("开盘", row.get("open", 0))) * 100),
                        int(float(row.get("最高", row.get("high", 0))) * 100),
                        int(float(row.get("最低", row.get("low", 0))) * 100),
                        int(float(row.get("收盘", row.get("close", 0))) * 100),
                        int(row.get("成交量", row.get("volume", 0))),
                        int(float(row.get("成交额", row.get("amount", 0))) * 100),
                        source,
                        now,
                    ),
                )
                count += 1
            except (ValueError, TypeError):
                continue

        return count

    def get_bars(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        """从 market_bars 读取 K 线，金额从分转回元。"""
        query = "SELECT * FROM market_bars WHERE symbol = ?"
        params: list = [symbol]

        if start:
            query += " AND bar_date >= ?"
            params.append(start)
        if end:
            query += " AND bar_date <= ?"
            params.append(end)

        query += " ORDER BY bar_date"

        rows = self._conn.execute(query, params).fetchall()
        if not rows:
            return pd.DataFrame()

        data = []
        for r in rows:
            data.append({
                "日期": r["bar_date"],
                "开盘": r["open_cents"] / 100,
                "最高": r["high_cents"] / 100,
                "最低": r["low_cents"] / 100,
                "收盘": r["close_cents"] / 100,
                "成交量": r["volume"],
                "成交额": r["amount_cents"] / 100,
            })

        return pd.DataFrame(data)

    def save_price_bars(
        self,
        symbol: str,
        bars_df: pd.DataFrame,
        source: str = "baostock",
        period: str = "daily",
        adjustflag: str = "2",
    ) -> int:
        """写入回测专用 K 线表，按复权参数隔离缓存。"""
        now = datetime.now(timezone.utc).isoformat()
        count = 0
        for _, row in bars_df.iterrows():
            bar_date = str(row.get("日期", row.get("date", "")) or "")
            if not bar_date:
                continue
            try:
                self._conn.execute(
                    """INSERT OR REPLACE INTO market_price_bars
                       (symbol, bar_date, period, adjustflag, source,
                        open_cents, high_cents, low_cents, close_cents,
                        volume, amount_cents, change_pct, fetched_at, raw_json)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        symbol,
                        bar_date,
                        period,
                        str(adjustflag),
                        source,
                        _cents(row.get("开盘", row.get("open", 0))),
                        _cents(row.get("最高", row.get("high", 0))),
                        _cents(row.get("最低", row.get("low", 0))),
                        _cents(row.get("收盘", row.get("close", 0))),
                        _int_or_zero(row.get("成交量", row.get("volume", 0))),
                        _cents(row.get("成交额", row.get("amount", 0))),
                        _float_or_none(row.get("涨跌幅", row.get("change_pct"))),
                        now,
                        json.dumps(_jsonable_value(row.to_dict()), ensure_ascii=False),
                    ),
                )
                count += 1
            except (ValueError, TypeError):
                continue
        if count:
            self.save_data_coverage(
                domain="price_bars",
                symbol=symbol,
                start_date=str(bars_df.get("日期", bars_df.get("date", pd.Series(dtype=str))).min()),
                end_date=str(bars_df.get("日期", bars_df.get("date", pd.Series(dtype=str))).max()),
                source=source,
                period=period,
                adjustflag=str(adjustflag),
                row_count=count,
                status="ok",
            )
        return count

    def get_price_bars(
        self,
        symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        *,
        period: str = "daily",
        adjustflag: str = "2",
        source: str | None = None,
    ) -> pd.DataFrame:
        """从回测专用 K 线表读取数据，金额从分转回元。"""
        query = (
            "SELECT * FROM market_price_bars "
            "WHERE symbol = ? AND period = ? AND adjustflag = ?"
        )
        params: list[Any] = [symbol, period, str(adjustflag)]
        if source:
            query += " AND source = ?"
            params.append(source)
        if start:
            query += " AND bar_date >= ?"
            params.append(start)
        if end:
            query += " AND bar_date <= ?"
            params.append(end)
        query += " ORDER BY bar_date"

        rows = self._conn.execute(query, params).fetchall()
        if not rows:
            return pd.DataFrame()

        data = []
        for row in rows:
            data.append({
                "日期": row["bar_date"],
                "开盘": row["open_cents"] / 100,
                "最高": row["high_cents"] / 100,
                "最低": row["low_cents"] / 100,
                "收盘": row["close_cents"] / 100,
                "成交量": row["volume"],
                "成交额": row["amount_cents"] / 100,
                "涨跌幅": row["change_pct"] if row["change_pct"] is not None else 0.0,
            })
        return pd.DataFrame(data)

    def save_price_bar_records(
        self,
        rows: list[dict],
        *,
        source: str,
        period: str = "daily",
        adjustflag: str = "3",
    ) -> int:
        """批量写入回测 K 线表，用于全市场日线落库。"""
        now = datetime.now(timezone.utc).isoformat()
        params = []
        for row in rows:
            symbol = str(row.get("symbol") or row.get("code") or "").strip()
            bar_date = str(row.get("日期", row.get("date", "")) or "").strip()
            if not symbol or not bar_date:
                continue
            params.append((
                symbol,
                bar_date,
                period,
                str(adjustflag),
                source,
                _cents(row.get("开盘", row.get("open", 0))),
                _cents(row.get("最高", row.get("high", 0))),
                _cents(row.get("最低", row.get("low", 0))),
                _cents(row.get("收盘", row.get("close", 0))),
                _int_or_zero(row.get("成交量", row.get("volume", 0))),
                _cents(row.get("成交额", row.get("amount", 0))),
                _float_or_none(row.get("涨跌幅", row.get("change_pct"))),
                now,
                json.dumps(_jsonable_value(row), ensure_ascii=False),
            ))
        if not params:
            return 0
        self._conn.executemany(
            """INSERT OR REPLACE INTO market_price_bars
               (symbol, bar_date, period, adjustflag, source,
                open_cents, high_cents, low_cents, close_cents,
                volume, amount_cents, change_pct, fetched_at, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            params,
        )
        return len(params)

    def save_financial_snapshot(
        self,
        symbol: str,
        *,
        report_year: int,
        report_quarter: int,
        report_date: str,
        available_date: str,
        payload: dict,
        source: str = "baostock",
    ) -> None:
        """写入财务快照。available_date 用于回测避免提前使用未披露数据。"""
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """INSERT OR REPLACE INTO market_financials
               (symbol, report_year, report_quarter, source, report_date, available_date,
                roe, roe_3y_ago, revenue_growth, net_profit_growth,
                operating_cash_flow, pe_ttm, pb, debt_ratio, fetched_at, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                symbol,
                int(report_year),
                int(report_quarter),
                source,
                report_date,
                available_date,
                _float_or_none(payload.get("roe")),
                _float_or_none(payload.get("roe_3y_ago")),
                _float_or_none(payload.get("revenue_growth")),
                _float_or_none(payload.get("net_profit_growth")),
                _float_or_none(payload.get("operating_cash_flow")),
                _float_or_none(payload.get("pe_ttm")),
                _float_or_none(payload.get("pb")),
                _float_or_none(payload.get("debt_ratio")),
                now,
                json.dumps(_jsonable_value(payload), ensure_ascii=False),
            ),
        )

    def get_financial_snapshot(
        self,
        symbol: str,
        *,
        as_of_date: str | None = None,
        report_year: int | None = None,
        report_quarter: int | None = None,
        source: str | None = None,
    ) -> dict | None:
        """读取财务快照；传 as_of_date 时返回当日已可用的最近一期。"""
        params: list[Any] = [symbol]
        query = "SELECT * FROM market_financials WHERE symbol = ?"
        if source:
            query += " AND source = ?"
            params.append(source)
        if report_year is not None:
            query += " AND report_year = ?"
            params.append(int(report_year))
        if report_quarter is not None:
            query += " AND report_quarter = ?"
            params.append(int(report_quarter))
        if as_of_date:
            query += " AND available_date <= ?"
            params.append(as_of_date)
            query += " ORDER BY available_date DESC, report_year DESC, report_quarter DESC LIMIT 1"
        else:
            query += " ORDER BY report_year DESC, report_quarter DESC LIMIT 1"

        row = self._conn.execute(query, params).fetchone()
        if not row:
            return None
        return {
            "symbol": row["symbol"],
            "report_year": row["report_year"],
            "report_quarter": row["report_quarter"],
            "report_date": row["report_date"],
            "available_date": row["available_date"],
            "roe": row["roe"],
            "roe_3y_ago": row["roe_3y_ago"],
            "revenue_growth": row["revenue_growth"],
            "net_profit_growth": row["net_profit_growth"],
            "operating_cash_flow": row["operating_cash_flow"],
            "pe_ttm": row["pe_ttm"],
            "pb": row["pb"],
            "debt_ratio": row["debt_ratio"],
            "source": row["source"],
        }

    def get_financial_snapshots(
        self,
        symbol: str,
        *,
        start_available: str | None = None,
        end_available: str | None = None,
        source: str | None = None,
    ) -> list[dict]:
        query = "SELECT * FROM market_financials WHERE symbol = ?"
        params: list[Any] = [symbol]
        if source:
            query += " AND source = ?"
            params.append(source)
        if start_available:
            query += " AND available_date >= ?"
            params.append(start_available)
        if end_available:
            query += " AND available_date <= ?"
            params.append(end_available)
        query += " ORDER BY available_date, report_year, report_quarter"
        rows = self._conn.execute(query, params).fetchall()
        return [
            {
                "symbol": row["symbol"],
                "report_year": row["report_year"],
                "report_quarter": row["report_quarter"],
                "report_date": row["report_date"],
                "available_date": row["available_date"],
                "roe": row["roe"],
                "roe_3y_ago": row["roe_3y_ago"],
                "revenue_growth": row["revenue_growth"],
                "net_profit_growth": row["net_profit_growth"],
                "operating_cash_flow": row["operating_cash_flow"],
                "pe_ttm": row["pe_ttm"],
                "pb": row["pb"],
                "debt_ratio": row["debt_ratio"],
                "source": row["source"],
            }
            for row in rows
        ]

    def save_fund_flow_snapshot(
        self,
        symbol: str,
        trade_date: str,
        payload: dict,
        *,
        source: str = "baostock",
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self._conn.execute(
            """INSERT OR REPLACE INTO market_fund_flows
               (symbol, trade_date, source, net_inflow_1d, net_inflow_5d,
                main_force_ratio, northbound_net, consecutive_outflow_days,
                fetched_at, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                symbol,
                trade_date,
                source,
                _float_or_none(payload.get("net_inflow_1d")),
                _float_or_none(payload.get("net_inflow_5d")),
                _float_or_none(payload.get("main_force_ratio")),
                _float_or_none(payload.get("northbound_net")),
                _int_or_zero(payload.get("consecutive_outflow_days")),
                now,
                json.dumps(_jsonable_value(payload), ensure_ascii=False),
            ),
        )

    def save_data_coverage(
        self,
        *,
        domain: str,
        symbol: str,
        start_date: str | None,
        end_date: str | None,
        source: str,
        period: str | None = None,
        adjustflag: str | None = None,
        row_count: int = 0,
        status: str = "ok",
        error: dict | None = None,
    ) -> str:
        now = datetime.now(timezone.utc).isoformat()
        key_src = "|".join([
            domain,
            symbol,
            start_date or "",
            end_date or "",
            period or "",
            adjustflag or "",
            source,
        ])
        coverage_key = hashlib.sha1(key_src.encode()).hexdigest()
        self._conn.execute(
            """INSERT OR REPLACE INTO market_data_coverage
               (coverage_key, domain, symbol, start_date, end_date, period,
                adjustflag, source, row_count, status, fetched_at, error_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                coverage_key,
                domain,
                symbol,
                start_date,
                end_date,
                period,
                adjustflag,
                source,
                int(row_count),
                status,
                now,
                json.dumps(_jsonable_value(error), ensure_ascii=False) if error else None,
            ),
        )
        return coverage_key


class MarketStore(MarketRepository):
    """Backward-compatible market store facade."""
