"""市场数据补水和覆盖率诊断。"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from astock_trading.market.store import MarketStore
from astock_trading.market.tushare_adapters import TushareClient, TushareMarketAdapter


def _compact_date(value: str) -> str:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 8 or not text.isdigit():
        raise ValueError(f"日期格式无效: {value}")
    return text


def _iso_date(value: str) -> str:
    text = _compact_date(value)
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError("市场数据补水不能在已有事件循环中同步调用")


def hydrate_tushare_daily_market_bars(
    conn,
    *,
    client: TushareClient | Any | None = None,
    start: str,
    end: str,
    adjustflag: str = "3",
    dry_run: bool = False,
    limit_dates: int | None = None,
) -> dict:
    """按交易日拉取 Tushare 全市场普通日线并写入 market_price_bars。

    Tushare daily 是非复权日线，只能写入 adjustflag=3。前复权回测数据仍应使用
    Baostock 或 Tushare pro_bar/复权因子路径，不能混用。
    """
    if str(adjustflag) != "3":
        raise ValueError(
            "Tushare daily 是非复权全市场日线，只能落库为 adjustflag=3；"
            "前复权历史请使用 baostock/pro_bar 或复权因子路径。"
        )

    actual_client = client or TushareClient.from_env()
    if not getattr(actual_client, "enabled", False):
        return {
            "status": "failed",
            "error": "ASTOCK_TUSHARE_TOKEN 未配置或 Tushare client 不可用",
            "source": "tushare",
            "adjustflag": str(adjustflag),
            "start": _iso_date(start),
            "end": _iso_date(end),
            "trade_dates": [],
            "planned_rows": 0,
            "rows_written": 0,
        }

    adapter = TushareMarketAdapter(client=actual_client)
    trade_dates = _run_async(adapter.get_trade_dates(start, end))
    if limit_dates is not None and limit_dates > 0:
        trade_dates = trade_dates[:limit_dates]

    store = MarketStore(conn)
    rows_seen = 0
    rows_written = 0
    empty_dates: list[str] = []
    written_dates: list[dict] = []

    for trade_date in trade_dates:
        frame = _run_async(adapter.get_daily_market_bars(trade_date))
        if frame.empty:
            empty_dates.append(trade_date)
            continue
        records = [
            {**record, "date": _iso_date(str(record.get("date") or trade_date))}
            for record in frame.to_dict("records")
        ]
        rows_seen += len(records)
        if dry_run:
            written_dates.append({
                "trade_date": _iso_date(trade_date),
                "planned_rows": len(records),
                "rows_written": 0,
            })
            continue

        saved = store.save_price_bar_records(
            records,
            source="tushare",
            period="daily",
            adjustflag=str(adjustflag),
        )
        store.save_data_coverage(
            domain="price_bars_daily",
            symbol="*",
            start_date=_iso_date(trade_date),
            end_date=_iso_date(trade_date),
            source="tushare",
            period="daily",
            adjustflag=str(adjustflag),
            row_count=saved,
            status="ok",
        )
        rows_written += saved
        written_dates.append({
            "trade_date": _iso_date(trade_date),
            "planned_rows": len(records),
            "rows_written": saved,
        })
        if hasattr(conn, "commit"):
            conn.commit()

    status = "dry_run" if dry_run else "ok"
    if not trade_dates:
        status = "empty"
    return {
        "status": status,
        "source": "tushare",
        "adjustflag": str(adjustflag),
        "start": _iso_date(start),
        "end": _iso_date(end),
        "trade_dates": trade_dates,
        "trade_date_count": len(trade_dates),
        "planned_rows": rows_seen,
        "rows_written": rows_written,
        "empty_dates": empty_dates,
        "dates": written_dates,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "notes": [
            "Tushare daily 为非复权日线，已按 adjustflag=3 落库。",
            "前复权回测历史不要和本数据直接混用，应继续使用 adjustflag=2 数据源。",
        ],
    }


def summarize_price_bar_coverage(
    conn,
    *,
    source: str | None = None,
    adjustflag: str | None = None,
    start: str | None = None,
    end: str | None = None,
) -> dict:
    """汇总 market_price_bars 的真实覆盖率。"""
    query = (
        "SELECT source, adjustflag, period, "
        "MIN(bar_date) AS start_date, MAX(bar_date) AS end_date, "
        "COUNT(*) AS row_count, COUNT(DISTINCT symbol) AS symbol_count, "
        "COUNT(DISTINCT bar_date) AS trading_day_count "
        "FROM market_price_bars WHERE 1 = 1"
    )
    params: list[Any] = []
    if source:
        query += " AND source = ?"
        params.append(source)
    if adjustflag:
        query += " AND adjustflag = ?"
        params.append(str(adjustflag))
    if start:
        query += " AND bar_date >= ?"
        params.append(_iso_date(start))
    if end:
        query += " AND bar_date <= ?"
        params.append(_iso_date(end))
    query += " GROUP BY source, adjustflag, period ORDER BY source, adjustflag, period"

    rows = conn.execute(query, params).fetchall()
    items = []
    for row in rows:
        row_count = int(row["row_count"] or 0)
        symbol_count = int(row["symbol_count"] or 0)
        trading_day_count = int(row["trading_day_count"] or 0)
        items.append({
            "source": row["source"],
            "adjustflag": row["adjustflag"],
            "period": row["period"],
            "start_date": row["start_date"],
            "end_date": row["end_date"],
            "row_count": row_count,
            "symbol_count": symbol_count,
            "trading_day_count": trading_day_count,
            "avg_rows_per_symbol": round(row_count / symbol_count, 2) if symbol_count else 0.0,
            "avg_symbols_per_day": round(row_count / trading_day_count, 2) if trading_day_count else 0.0,
        })

    return {
        "diagnostic": "market_data_coverage",
        "status": "ok" if items else "empty",
        "filters": {
            "source": source or "",
            "adjustflag": str(adjustflag or ""),
            "start": _iso_date(start) if start else "",
            "end": _iso_date(end) if end else "",
        },
        "price_bars": items,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
