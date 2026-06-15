"""P6-1 自适应风控建议测试。"""

from __future__ import annotations

from datetime import date, timedelta

from astock_trading.pipeline.adaptive_risk import run_adaptive_risk
from astock_trading.platform.events import EventStore


def _seed_market_bars(conn, *, symbol: str = "000001", days: int = 20, intraday_range_pct: float = 0.06) -> None:
    start = date(2026, 5, 1)
    close_cents = 300000
    half_range = intraday_range_pct / 2
    for offset in range(days):
        current = start + timedelta(days=offset)
        high_cents = int(close_cents * (1 + half_range))
        low_cents = int(close_cents * (1 - half_range))
        conn.execute(
            """REPLACE INTO market_bars
               (symbol, bar_date, period, open_cents, high_cents, low_cents, close_cents,
                volume, amount_cents, source, fetched_at)
               VALUES (?, ?, 'daily', ?, ?, ?, ?, 1000, 100000, 'test', ?)""",
            (
                symbol,
                current.isoformat(),
                close_cents,
                high_cents,
                low_cents,
                close_cents,
                f"{current.isoformat()}T00:00:00+00:00",
            ),
        )
        close_cents += 100


def _append_balance(store: EventStore, *, total_asset_cents: int, loss_days: int) -> None:
    store.append(
        "balance:main",
        "balance",
        "balance.updated",
        {
            "scope": "main",
            "cash_cents": total_asset_cents,
            "total_asset_cents": total_asset_cents,
            "weekly_buy_count": 0,
            "daily_pnl_cents": -1000 if loss_days else 1000,
            "consecutive_loss_days": loss_days,
            "updated_at": "2026-05-19T00:00:00+08:00",
        },
    )


def test_adaptive_risk_suggests_wider_stop_lower_position_and_higher_buy_threshold(mysql_conn):
    conn = mysql_conn
    try:
        store = EventStore(conn)
        _seed_market_bars(conn, intraday_range_pct=0.07)
        _append_balance(store, total_asset_cents=1_000_000, loss_days=0)
        _append_balance(store, total_asset_cents=940_000, loss_days=1)
        _append_balance(store, total_asset_cents=900_000, loss_days=3)

        payload = run_adaptive_risk(conn, lookback_days=20, record=True, config_version="v_test")
        events = store.query(event_type="risk.adaptive_suggestion.proposed")
    finally:
        conn.close()

    assert payload["analysis"] == "adaptive_risk"
    assert payload["status"] == "ok"
    assert payload["inputs"]["market_volatility"]["sample_count"] == 20
    assert payload["inputs"]["market_volatility"]["average_intraday_range_pct"] >= 0.06
    assert payload["inputs"]["equity_curve"]["max_drawdown_pct"] >= 0.09
    assert payload["inputs"]["loss_streak"]["consecutive_loss_days"] == 3
    assert payload["suggestions"]["stop_loss_adjustment"]["action"] == "widen"
    assert payload["suggestions"]["position_limit_adjustment"]["action"] == "reduce"
    assert payload["suggestions"]["buy_threshold_adjustment"]["action"] == "raise"
    assert payload["guardrails"]["auto_apply"] is False
    assert payload["recorded_event_id"]
    assert events[0]["payload"]["guardrails"]["manual_confirmation_required"] is True


def test_adaptive_risk_reports_insufficient_data_without_guessing(mysql_conn):
    conn = mysql_conn
    try:
        payload = run_adaptive_risk(conn, lookback_days=20, record=False, config_version="v_test")
    finally:
        conn.close()

    assert payload["status"] == "insufficient_data"
    assert payload["suggestions"]["stop_loss_adjustment"]["action"] == "hold"
    assert payload["suggestions"]["position_limit_adjustment"]["action"] == "hold"
    assert payload["suggestions"]["buy_threshold_adjustment"]["action"] == "hold"
    assert payload["guardrails"]["auto_apply"] is False
    assert payload["evidence_gaps"]
