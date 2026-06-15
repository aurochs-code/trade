"""Backtest persistence behavior."""

from __future__ import annotations


class FakeConn:
    def __init__(self):
        self.executed: list[tuple[str, tuple]] = []
        self.many: list[tuple[str, list[tuple]]] = []

    def execute(self, sql: str, params=None):
        self.executed.append((sql, tuple(params or ())))

    def executemany(self, sql: str, rows):
        self.many.append((sql, [tuple(row) for row in rows]))


def test_save_backtest_result_writes_run_full_trades_and_equity_curve():
    from astock_trading.backtest.persistence import save_backtest_result

    conn = FakeConn()
    result = {
        "preset": "验证A",
        "initial_cash": 100000.0,
        "final_value": 103000.0,
        "total_return_pct": 3.0,
        "annual_return_pct": 12.0,
        "max_drawdown_pct": 4.0,
        "win_rate_pct": 50.0,
        "sharpe_ratio": 1.2,
        "calmar_ratio": 3.0,
        "trade_log": [
            {"date": "2026-01-02", "code": "600000", "side": "buy", "price": 10.0, "shares": 100},
            {"date": "2026-01-03", "code": "600000", "side": "sell", "price": 10.5, "shares": 100},
            {"date": "2026-01-04", "code": "600519", "side": "buy", "price": 100.0, "shares": 100},
        ],
        "equity_curve": [
            {"date": "2026-01-02", "equity": 100000.0, "cash": 99000.0, "positions": 1},
            {"date": "2026-01-03", "equity": 100500.0, "cash": 100500.0, "positions": 0},
        ],
    }

    payload = save_backtest_result(
        conn,
        result,
        request={
            "codes": ["600000", "600519"],
            "start": "2026-01-02",
            "end": "2026-01-04",
        },
    )

    assert payload["status"] == "recorded"
    assert payload["trade_count"] == 3
    assert payload["equity_curve_points"] == 2
    assert any("backtest_runs" in sql for sql, _ in conn.executed)
    trade_sql, trade_rows = conn.many[0]
    equity_sql, equity_rows = conn.many[1]
    assert "backtest_trades" in trade_sql
    assert "backtest_equity_curve" in equity_sql
    assert len(trade_rows) == 3
    assert len(equity_rows) == 2
