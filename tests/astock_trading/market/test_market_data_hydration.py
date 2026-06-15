from __future__ import annotations

import pytest



class FakeTushareClient:
    def __init__(self, responses: dict[str, list[dict]]):
        self.responses = responses
        self.calls: list[tuple[str, dict, str]] = []
        self.enabled = True

    def query(self, api_name: str, *, params: dict | None = None, fields: str = "") -> list[dict]:
        self.calls.append((api_name, params or {}, fields))
        if api_name == "daily":
            trade_date = (params or {}).get("trade_date")
            return self.responses.get(f"daily:{trade_date}", self.responses.get("daily", []))
        return self.responses.get(api_name, [])


def test_hydrate_tushare_daily_market_bars_writes_raw_daily_rows(mysql_conn):
    from astock_trading.market.data_hydration import (
        hydrate_tushare_daily_market_bars,
        summarize_price_bar_coverage,
    )

    conn = mysql_conn
    client = FakeTushareClient({
        "trade_cal": [{"cal_date": "20260612"}],
        "daily:20260612": [
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
    try:
        result = hydrate_tushare_daily_market_bars(
            conn,
            client=client,
            start="2026-06-12",
            end="2026-06-12",
            adjustflag="3",
        )

        assert result["status"] == "ok"
        assert result["source"] == "tushare"
        assert result["adjustflag"] == "3"
        assert result["trade_dates"] == ["20260612"]
        assert result["rows_written"] == 2
        rows = conn.execute(
            """SELECT symbol, bar_date, source, adjustflag, close_cents, volume, amount_cents
               FROM market_price_bars
               ORDER BY symbol"""
        ).fetchall()
        assert [(row["symbol"], row["bar_date"], row["source"], row["adjustflag"]) for row in rows] == [
            ("000001", "2026-06-12", "tushare", "3"),
            ("600036", "2026-06-12", "tushare", "3"),
        ]
        assert rows[0]["close_cents"] == 1050
        assert rows[0]["volume"] == 123400
        assert rows[0]["amount_cents"] == 567800000

        coverage = summarize_price_bar_coverage(conn)
        assert coverage["status"] == "ok"
        assert coverage["price_bars"][0]["source"] == "tushare"
        assert coverage["price_bars"][0]["adjustflag"] == "3"
        assert coverage["price_bars"][0]["row_count"] == 2
        assert coverage["price_bars"][0]["symbol_count"] == 2
        assert coverage["price_bars"][0]["start_date"] == "2026-06-12"
        assert coverage["price_bars"][0]["end_date"] == "2026-06-12"
    finally:
        conn.close()


def test_hydrate_tushare_daily_market_bars_dry_run_does_not_write(mysql_conn):
    from astock_trading.market.data_hydration import hydrate_tushare_daily_market_bars

    conn = mysql_conn
    client = FakeTushareClient({
        "trade_cal": [{"cal_date": "20260612"}],
        "daily:20260612": [{"ts_code": "000001.SZ", "trade_date": "20260612", "close": 10.5}],
    })
    try:
        result = hydrate_tushare_daily_market_bars(
            conn,
            client=client,
            start="2026-06-12",
            end="2026-06-12",
            dry_run=True,
        )

        assert result["status"] == "dry_run"
        assert result["planned_rows"] == 1
        assert result["rows_written"] == 0
        row = conn.execute("SELECT COUNT(*) AS count FROM market_price_bars").fetchone()
        assert row["count"] == 0
    finally:
        conn.close()


def test_hydrate_tushare_daily_market_bars_rejects_qfq_adjustflag(mysql_conn):
    from astock_trading.market.data_hydration import hydrate_tushare_daily_market_bars

    conn = mysql_conn
    try:
        with pytest.raises(ValueError, match="Tushare daily"):
            hydrate_tushare_daily_market_bars(
                conn,
                client=FakeTushareClient({}),
                start="2026-06-12",
                end="2026-06-12",
                adjustflag="2",
            )
    finally:
        conn.close()


def test_select_backtest_universe_filters_for_liquid_volatile_codes(mysql_conn):
    from astock_trading.market.data_hydration import select_backtest_universe
    from astock_trading.market.store import MarketStore

    conn = mysql_conn
    store = MarketStore(conn)
    try:
        rows = [
            # 高成交额、高振幅、价格正常，应入选。
            {"symbol": "300001", "date": "2026-06-10", "open": 10, "high": 11, "low": 9, "close": 10, "amount": 260_000_000, "volume": 1000},
            {"symbol": "300001", "date": "2026-06-11", "open": 10, "high": 10.8, "low": 9.4, "close": 10.2, "amount": 280_000_000, "volume": 1000},
            {"symbol": "300001", "date": "2026-06-12", "open": 10.2, "high": 11.2, "low": 9.8, "close": 10.8, "amount": 300_000_000, "volume": 1000},
            # 成交额够但振幅不够，应过滤。
            {"symbol": "600001", "date": "2026-06-10", "open": 20, "high": 20.2, "low": 19.9, "close": 20, "amount": 500_000_000, "volume": 1000},
            {"symbol": "600001", "date": "2026-06-11", "open": 20, "high": 20.1, "low": 19.8, "close": 20, "amount": 520_000_000, "volume": 1000},
            {"symbol": "600001", "date": "2026-06-12", "open": 20, "high": 20.2, "low": 19.9, "close": 20, "amount": 510_000_000, "volume": 1000},
            # 振幅够但成交额不够，应过滤。
            {"symbol": "002001", "date": "2026-06-10", "open": 8, "high": 8.8, "low": 7.4, "close": 8, "amount": 30_000_000, "volume": 1000},
            {"symbol": "002001", "date": "2026-06-11", "open": 8, "high": 8.6, "low": 7.6, "close": 8.1, "amount": 35_000_000, "volume": 1000},
            {"symbol": "002001", "date": "2026-06-12", "open": 8.1, "high": 8.9, "low": 7.7, "close": 8.5, "amount": 40_000_000, "volume": 1000},
            # 价格过高，应过滤。
            {"symbol": "688001", "date": "2026-06-10", "open": 260, "high": 285, "low": 240, "close": 260, "amount": 500_000_000, "volume": 1000},
            {"symbol": "688001", "date": "2026-06-11", "open": 260, "high": 285, "low": 240, "close": 262, "amount": 520_000_000, "volume": 1000},
            {"symbol": "688001", "date": "2026-06-12", "open": 262, "high": 290, "low": 245, "close": 265, "amount": 540_000_000, "volume": 1000},
        ]
        store.save_price_bar_records(rows, source="tushare", adjustflag="3")

        result = select_backtest_universe(
            conn,
            source="tushare",
            adjustflag="3",
            start="2026-06-10",
            end="2026-06-12",
            min_trading_days=3,
            min_avg_amount_yuan=200_000_000,
            min_avg_amplitude_pct=5.0,
            min_price=5.0,
            max_price=200.0,
            limit=10,
        )

        assert result["status"] == "ok"
        assert result["selected_count"] == 1
        assert result["codes"] == ["300001"]
        selected = result["selected"][0]
        assert selected["code"] == "300001"
        assert selected["avg_amount_yuan"] == 280_000_000.0
        assert selected["avg_amplitude_pct"] > 10
        assert selected["latest_close"] == 10.8
        assert result["backtest_batch_command"].startswith(
            "atrade backtest-batch 300001 2026-06-10 2026-06-12"
        )
        assert result["warnings"][0].startswith("当前选择基于 market_price_bars")
    finally:
        conn.close()
