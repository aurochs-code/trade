"""交易后复盘服务。

从交易前假设出发，到期后用 market_bars 计算 MFE/MAE，并追加复盘证据。
"""

from __future__ import annotations

import datetime as dt
from typing import Any

from astock_trading.platform.domain_events import (
    TRADE_HYPOTHESIS_RECORDED,
    TRADE_REVIEW_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_today_str


class TradeReviewService:
    """基于交易证据链生成到期复盘。"""

    def __init__(self, event_store: EventStore, conn: Any):
        self._events = event_store
        self._conn = conn

    def review_due_trades(
        self,
        *,
        as_of: str | None = None,
        code: str = "",
        record: bool = False,
        limit: int = 500,
    ) -> dict:
        """扫描到期买入假设，必要时追加 `trade.review.recorded`。"""
        review_as_of = as_of or local_today_str()
        items: list[dict] = []
        reviewed_count = 0

        hypotheses = self._events.query(event_type=TRADE_HYPOTHESIS_RECORDED, limit=limit)
        for event in hypotheses:
            payload = event.get("payload", {}) or {}
            if payload.get("side") != "buy":
                continue
            if code and payload.get("code") != code:
                continue

            review_after_days = _positive_int((payload.get("hypothesis") or {}).get("review_after_days"))
            if review_after_days <= 0:
                continue

            item = self._build_review_item(event, review_as_of, review_after_days)
            if item is None:
                continue
            if item["status"] == "reviewed" and record and not self._review_exists(
                item["stream"],
                item["source_hypothesis_event_id"],
                review_as_of,
            ):
                review_event_id = self._events.append(
                    stream=item["stream"],
                    stream_type="trade",
                    event_type=TRADE_REVIEW_RECORDED,
                    payload=item["payload"],
                    metadata={"source": "trade_review", "account": "main"},
                )
                item["review_event_id"] = review_event_id
                reviewed_count += 1
            elif item["status"] == "reviewed" and self._review_exists(
                item["stream"],
                item["source_hypothesis_event_id"],
                review_as_of,
            ):
                item["status"] = "already_reviewed"
            items.append(_public_review_item(item))

        return {
            "status": "applied" if record else "dry_run",
            "review_as_of": review_as_of,
            "record": record,
            "reviewed_count": reviewed_count,
            "items": items,
        }

    def _build_review_item(
        self,
        hypothesis_event: dict,
        review_as_of: str,
        review_after_days: int,
    ) -> dict | None:
        payload = hypothesis_event.get("payload", {}) or {}
        code = str(payload.get("code") or "")
        order_id = str(payload.get("order_id") or "")
        if not code or not order_id:
            return None

        entry_date = self._entry_date(order_id, hypothesis_event)
        if _date(review_as_of) < _date(entry_date) + dt.timedelta(days=review_after_days):
            return {
                "status": "not_due",
                "stream": hypothesis_event["stream"],
                "source_hypothesis_event_id": hypothesis_event["event_id"],
                "payload": {
                    "order_id": order_id,
                    "code": code,
                    "entry_date": entry_date,
                    "review_as_of": review_as_of,
                    "review_after_days": review_after_days,
                },
            }

        bars = self._bars(code, start=entry_date, end=review_as_of)
        if not bars:
            return {
                "status": "insufficient_market_bars",
                "stream": hypothesis_event["stream"],
                "source_hypothesis_event_id": hypothesis_event["event_id"],
                "payload": {
                    "order_id": order_id,
                    "code": code,
                    "entry_date": entry_date,
                    "review_as_of": review_as_of,
                    "review_after_days": review_after_days,
                },
            }

        shares = int(payload.get("shares") or 0)
        entry_price_cents = int(payload.get("price_cents") or 0)
        if entry_price_cents <= 0:
            return None

        max_bar = max(bars, key=lambda item: int(item["high_cents"]))
        min_bar = min(bars, key=lambda item: int(item["low_cents"]))
        latest_bar = bars[-1]
        mfe_cents = (int(max_bar["high_cents"]) - entry_price_cents) * shares
        mae_cents = (int(min_bar["low_cents"]) - entry_price_cents) * shares
        mfe_pct = round((int(max_bar["high_cents"]) / entry_price_cents) - 1.0, 4)
        mae_pct = round((int(min_bar["low_cents"]) / entry_price_cents) - 1.0, 4)
        latest_return_pct = round((int(latest_bar["close_cents"]) / entry_price_cents) - 1.0, 4)

        source_outcome_event_id = self._source_outcome_event_id(hypothesis_event["stream"])
        review_payload = {
            "order_id": order_id,
            "code": code,
            "name": payload.get("name") or code,
            "entry_date": entry_date,
            "review_as_of": review_as_of,
            "review_after_days": review_after_days,
            "shares": shares,
            "entry_price_cents": entry_price_cents,
            "latest_close_cents": int(latest_bar["close_cents"]),
            "mfe_cents": mfe_cents,
            "mae_cents": mae_cents,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "latest_return_pct": latest_return_pct,
            "source_hypothesis_event_id": hypothesis_event["event_id"],
            "source_outcome_event_id": source_outcome_event_id,
            "hypothesis": payload.get("hypothesis") or {},
            "hypothesis_validation": _validate_hypothesis(
                hypothesis=payload.get("hypothesis") or {},
                latest_return_pct=latest_return_pct,
                mae_pct=mae_pct,
                mfe_pct=mfe_pct,
            ),
            "review_evidence": {
                "bar_count": len(bars),
                "bars": bars,
                "max_high_bar": max_bar,
                "min_low_bar": min_bar,
                "latest_bar": latest_bar,
            },
        }
        return {
            "status": "reviewed",
            "stream": hypothesis_event["stream"],
            "source_hypothesis_event_id": hypothesis_event["event_id"],
            "payload": review_payload,
        }

    def _entry_date(self, order_id: str, hypothesis_event: dict) -> str:
        row = self._conn.execute(
            "SELECT filled_at, created_at FROM projection_orders WHERE order_id = ?",
            (order_id,),
        ).fetchone()
        if row:
            for field in ("filled_at", "created_at"):
                value = row[field]
                if value:
                    return str(value)[:10]
        return str(hypothesis_event.get("occurred_at") or "")[:10]

    def _bars(self, code: str, *, start: str, end: str) -> list[dict]:
        rows = self._conn.execute(
            """SELECT bar_date, open_cents, high_cents, low_cents, close_cents, volume, amount_cents, source
               FROM market_bars
               WHERE symbol = ? AND period = 'daily' AND bar_date >= ? AND bar_date <= ?
               ORDER BY bar_date""",
            (code, start, end),
        ).fetchall()
        return [
            {
                "bar_date": row["bar_date"],
                "open_cents": int(row["open_cents"]),
                "high_cents": int(row["high_cents"]),
                "low_cents": int(row["low_cents"]),
                "close_cents": int(row["close_cents"]),
                "volume": int(row["volume"]),
                "amount_cents": int(row["amount_cents"]),
                "source": row["source"],
            }
            for row in rows
        ]

    def _source_outcome_event_id(self, stream: str) -> str:
        for event in self._events.query(stream=stream, event_type="trade.outcome.recorded", limit=20):
            return str(event.get("event_id") or "")
        return ""

    def _review_exists(self, stream: str, source_hypothesis_event_id: str, review_as_of: str) -> bool:
        for event in self._events.query(stream=stream, event_type=TRADE_REVIEW_RECORDED, limit=100):
            payload = event.get("payload", {}) or {}
            if (
                payload.get("source_hypothesis_event_id") == source_hypothesis_event_id
                and payload.get("review_as_of") == review_as_of
            ):
                return True
        return False


def _validate_hypothesis(
    *,
    hypothesis: dict,
    latest_return_pct: float,
    mae_pct: float,
    mfe_pct: float,
) -> dict:
    invalidation = str(hypothesis.get("invalidation") or "")
    if mae_pct <= -0.05:
        return {
            "status": "invalidation_possible",
            "reason": "区间最低价较买入价回撤超过 5%，需要人工核对失效条件。",
            "invalidation": invalidation,
        }
    if latest_return_pct >= 0 and mfe_pct > 0:
        return {
            "status": "supported",
            "reason": "复盘日收盘价不低于买入价，且持有期出现正向 MFE。",
            "invalidation": invalidation,
        }
    if latest_return_pct < 0:
        return {
            "status": "weakened",
            "reason": "复盘日收盘价低于买入价，假设需要降级或人工复核。",
            "invalidation": invalidation,
        }
    return {
        "status": "inconclusive",
        "reason": "价格证据不足以支持或否定假设，需要人工继续观察。",
        "invalidation": invalidation,
    }


def _positive_int(value: Any) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return 0
    return max(result, 0)


def _date(value: str) -> dt.date:
    return dt.date.fromisoformat(str(value)[:10])


def _public_review_item(item: dict) -> dict:
    payload = item.get("payload", {}) or {}
    result = {
        "status": item["status"],
        "stream": item["stream"],
        "order_id": payload.get("order_id", ""),
        "code": payload.get("code", ""),
        "entry_date": payload.get("entry_date", ""),
        "review_as_of": payload.get("review_as_of", ""),
        "source_hypothesis_event_id": item.get("source_hypothesis_event_id", ""),
    }
    if item.get("review_event_id"):
        result["review_event_id"] = item["review_event_id"]
    for key in ("mfe_cents", "mae_cents", "mfe_pct", "mae_pct", "latest_return_pct", "hypothesis_validation"):
        if key in payload:
            result[key] = payload[key]
    return result
