import json

import pandas as pd

from astock_trading.backtest.continuation_backtest import run_continuation_backtest
from astock_trading.market.store import MarketStore


def test_continuation_backtest_returns_hold_window_metrics(tmp_path):
    trades = [
        {
            "trade_date": "2026-01-02",
            "code": "600036",
            "hold_days": 2,
            "return_pct": 1.8,
            "entry_mode": "open",
            "top_n": 2,
        },
        {
            "trade_date": "2026-01-03",
            "code": "000001",
            "hold_days": 2,
            "return_pct": -0.4,
            "entry_mode": "open",
            "top_n": 2,
        },
    ]
    (tmp_path / "continuation_trades.json").write_text(json.dumps(trades), encoding="utf-8")

    result = run_continuation_backtest(
        codes=["600036", "000001"],
        start="2026-01-01",
        end="2026-03-31",
        hold_days=2,
        top_n=2,
        data_dir=tmp_path,
    )

    assert result["hold_days"] == 2
    assert "total_return_pct" in result
    assert "win_rate_pct" in result
    assert "trades" in result
    assert len(result["trades"]) == 2


def test_continuation_backtest_uses_market_bars_when_available(mysql_conn):
    conn = mysql_conn
    try:
        store = MarketStore(conn)
        dates = [f"2026-01-0{i}" for i in range(1, 9)]
        strong = pd.DataFrame(
            {
                "日期": dates,
                "开盘": [10.0, 10.05, 10.12, 10.2, 10.4, 10.7, 10.95, 11.15],
                "最高": [10.08, 10.15, 10.25, 10.38, 10.72, 11.0, 11.25, 11.45],
                "最低": [9.96, 10.0, 10.08, 10.16, 10.35, 10.62, 10.9, 11.08],
                "收盘": [10.04, 10.12, 10.22, 10.34, 10.68, 10.95, 11.2, 11.38],
                "成交量": [80_000_000, 82_000_000, 85_000_000, 88_000_000, 150_000_000, 160_000_000, 170_000_000, 175_000_000],
                "成交额": [8.0e8, 8.3e8, 8.7e8, 9.1e8, 1.6e9, 1.75e9, 1.9e9, 2.0e9],
            }
        )
        store.save_bars("600036", strong, source="test")
    finally:
        conn.close()

    result = run_continuation_backtest(
        codes=["600036"],
        start="2026-01-05",
        end="2026-01-06",
        hold_days=2,
        top_n=1,
    )

    assert result["trades"]
    assert result["total_return_pct"] != 0
