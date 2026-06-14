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


def _is_supported_a_share_symbol(symbol: str) -> bool:
    text = str(symbol or "").strip()
    if len(text) != 6 or not text.isdigit():
        return False
    return text.startswith(("0", "3", "6", "4", "8"))


def select_backtest_universe(
    conn,
    *,
    source: str = "tushare",
    adjustflag: str = "3",
    start: str,
    end: str,
    min_trading_days: int = 240,
    min_avg_amount_yuan: float = 200_000_000.0,
    min_avg_amplitude_pct: float = 3.0,
    min_price: float = 5.0,
    max_price: float = 200.0,
    limit: int = 300,
) -> dict:
    """从已落库全市场日线中筛出适合动量/回踩路线的研究股票池。"""
    start_iso = _iso_date(start)
    end_iso = _iso_date(end)
    params = [
        str(source),
        str(adjustflag),
        start_iso,
        end_iso,
        str(source),
        str(adjustflag),
        start_iso,
        end_iso,
    ]
    rows = conn.execute(
        """
        WITH base AS (
            SELECT
                symbol,
                MIN(bar_date) AS start_date,
                MAX(bar_date) AS end_date,
                COUNT(DISTINCT bar_date) AS trading_day_count,
                AVG(amount_cents) / 100.0 AS avg_amount_yuan,
                AVG(CASE
                    WHEN close_cents > 0
                    THEN ((high_cents - low_cents) * 100.0 / close_cents)
                    ELSE NULL
                END) AS avg_amplitude_pct
            FROM market_price_bars
            WHERE source = ?
              AND adjustflag = ?
              AND period = 'daily'
              AND bar_date >= ?
              AND bar_date <= ?
            GROUP BY symbol
        ),
        latest AS (
            SELECT b.symbol, b.close_cents / 100.0 AS latest_close
            FROM market_price_bars b
            JOIN base ON base.symbol = b.symbol AND base.end_date = b.bar_date
            WHERE b.source = ?
              AND b.adjustflag = ?
              AND b.period = 'daily'
              AND b.bar_date >= ?
              AND b.bar_date <= ?
        )
        SELECT
            base.symbol,
            base.start_date,
            base.end_date,
            base.trading_day_count,
            base.avg_amount_yuan,
            base.avg_amplitude_pct,
            latest.latest_close
        FROM base
        JOIN latest ON latest.symbol = base.symbol
        """,
        params,
    ).fetchall()

    selected: list[dict[str, Any]] = []
    excluded = {
        "unsupported_symbol": 0,
        "insufficient_history": 0,
        "low_liquidity": 0,
        "low_volatility": 0,
        "price_out_of_range": 0,
    }
    for row in rows:
        symbol = str(row["symbol"])
        if not _is_supported_a_share_symbol(symbol):
            excluded["unsupported_symbol"] += 1
            continue
        trading_days = int(row["trading_day_count"] or 0)
        avg_amount = float(row["avg_amount_yuan"] or 0.0)
        avg_amplitude = float(row["avg_amplitude_pct"] or 0.0)
        latest_close = float(row["latest_close"] or 0.0)
        if trading_days < int(min_trading_days):
            excluded["insufficient_history"] += 1
            continue
        if avg_amount < float(min_avg_amount_yuan):
            excluded["low_liquidity"] += 1
            continue
        if avg_amplitude < float(min_avg_amplitude_pct):
            excluded["low_volatility"] += 1
            continue
        if latest_close < float(min_price) or latest_close > float(max_price):
            excluded["price_out_of_range"] += 1
            continue

        liquidity_score = avg_amount / 100_000_000.0
        fitness_score = round(liquidity_score * avg_amplitude, 4)
        selected.append({
            "code": symbol,
            "start_date": row["start_date"],
            "end_date": row["end_date"],
            "trading_day_count": trading_days,
            "avg_amount_yuan": round(avg_amount, 2),
            "avg_amplitude_pct": round(avg_amplitude, 4),
            "latest_close": round(latest_close, 4),
            "fitness_score": fitness_score,
        })

    selected.sort(key=lambda item: (-item["fitness_score"], item["code"]))
    limited = selected[: max(int(limit), 0)]
    codes = [item["code"] for item in limited]
    status = "ok" if limited else "empty"
    return {
        "diagnostic": "backtest_universe_selection",
        "status": status,
        "source": str(source),
        "adjustflag": str(adjustflag),
        "start": start_iso,
        "end": end_iso,
        "filters": {
            "min_trading_days": int(min_trading_days),
            "min_avg_amount_yuan": float(min_avg_amount_yuan),
            "min_avg_amplitude_pct": float(min_avg_amplitude_pct),
            "min_price": float(min_price),
            "max_price": float(max_price),
            "limit": int(limit),
        },
        "scanned_count": len(rows),
        "eligible_count": len(selected),
        "selected_count": len(limited),
        "excluded": excluded,
        "codes": codes,
        "codes_csv": ",".join(codes),
        "selected": limited,
        "backtest_batch_command": (
            f"atrade backtest-batch {','.join(codes)} {start_iso} {end_iso} "
            f"--use-stored-data --use-market-bars --adjustflag {adjustflag} --json"
            if codes else ""
        ),
        "warnings": [
            "当前选择基于 market_price_bars 的成交额、振幅、价格和历史覆盖，只用于生成研究股票池；不是买入建议。",
            "adjustflag=3 为非复权口径，适合全市场初筛；严肃收益回测仍应优先使用前复权 adjustflag=2。",
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
