from __future__ import annotations

import pytest

from astock_trading.platform.db import connect, init_db


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


def test_hydrate_tushare_daily_market_bars_writes_raw_daily_rows(tmp_path):
    from astock_trading.market.data_hydration import (
        hydrate_tushare_daily_market_bars,
        summarize_price_bar_coverage,
    )

    db_path = tmp_path / "hydrate.db"
    init_db(db_path)
    conn = connect(db_path)
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


def test_hydrate_tushare_daily_market_bars_dry_run_does_not_write(tmp_path):
    from astock_trading.market.data_hydration import hydrate_tushare_daily_market_bars

    db_path = tmp_path / "dry-run.db"
    init_db(db_path)
    conn = connect(db_path)
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


def test_hydrate_tushare_daily_market_bars_rejects_qfq_adjustflag(tmp_path):
    from astock_trading.market.data_hydration import hydrate_tushare_daily_market_bars

    db_path = tmp_path / "qfq.db"
    init_db(db_path)
    conn = connect(db_path)
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
