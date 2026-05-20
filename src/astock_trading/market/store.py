"""
market/store.py — 市场观察存储

追加到 market_observations / market_bars，提供 TTL 缓存检查。
"""

from __future__ import annotations

import json
import uuid
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


class MarketStore(MarketRepository):
    """Backward-compatible market store facade."""
