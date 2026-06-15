"""交易后复盘：到期持仓 MFE/MAE 与假设验证。"""

from __future__ import annotations

from astock_trading.execution.review import TradeReviewService
from astock_trading.execution.service import ExecutionService, SimulatedBroker
from astock_trading.platform.events import EventStore


def _insert_bar(conn, date: str, open_: int, high: int, low: int, close: int) -> None:
    conn.execute(
        """REPLACE INTO market_bars
           (symbol, bar_date, period, open_cents, high_cents, low_cents,
            close_cents, volume, amount_cents, source, fetched_at)
           VALUES (?, ?, 'daily', ?, ?, ?, ?, 1000, 1000000, 'test', ?)""",
        ("002138", date, open_, high, low, close, f"{date}T15:00:00+08:00"),
    )


def test_review_due_trade_records_mfe_mae_and_hypothesis_validation(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)
    svc = ExecutionService(store, conn, broker=SimulatedBroker())

    order = svc.record_buy(
        code="002138",
        name="双环传动",
        shares=100,
        price_cents=1000,
        fee_cents=3,
        reason="突破后回踩不破",
        run_id="manual_buy_review",
        source_event_id="decision_evt_1",
        source_score_event_id="score_evt_1",
        hypothesis={
            "thesis": "突破后回踩不破，三日内维持强势",
            "invalidation": "跌破买入价 5% 需要复核",
            "review_after_days": 3,
        },
    )
    conn.execute(
        "UPDATE event_log SET occurred_at = ? WHERE stream = ?",
        ("2026-05-10T10:00:00+08:00", f"trade:002138:{order.order_id}"),
    )
    conn.execute(
        "UPDATE projection_orders SET filled_at = ?, created_at = ?, updated_at = ? WHERE order_id = ?",
        (
            "2026-05-10T10:00:00+08:00",
            "2026-05-10T10:00:00+08:00",
            "2026-05-10T10:00:00+08:00",
            order.order_id,
        ),
    )
    _insert_bar(conn, "2026-05-10", 1000, 1030, 990, 1010)
    _insert_bar(conn, "2026-05-11", 1010, 1100, 1005, 1080)
    _insert_bar(conn, "2026-05-12", 1080, 1090, 960, 980)
    _insert_bar(conn, "2026-05-13", 980, 1060, 970, 1040)

    result = TradeReviewService(store, conn).review_due_trades(as_of="2026-05-13", record=True)
    second = TradeReviewService(store, conn).review_due_trades(as_of="2026-05-13", record=True)
    review_events = store.query(stream=f"trade:002138:{order.order_id}", event_type="trade.review.recorded")

    assert result["status"] == "applied"
    assert result["reviewed_count"] == 1
    assert second["reviewed_count"] == 0
    assert len(review_events) == 1
    review = review_events[0]["payload"]
    assert review["order_id"] == order.order_id
    assert review["review_as_of"] == "2026-05-13"
    assert review["entry_price_cents"] == 1000
    assert review["mfe_cents"] == 10000
    assert review["mae_cents"] == -4000
    assert review["mfe_pct"] == 0.1
    assert review["mae_pct"] == -0.04
    assert review["hypothesis_validation"]["status"] == "supported"
    assert review["review_evidence"]["bars"][0]["bar_date"] == "2026-05-10"


def test_review_due_trade_reports_missing_market_bars_without_recording(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)
    svc = ExecutionService(store, conn, broker=SimulatedBroker())

    order = svc.record_buy(
        code="002138",
        name="双环传动",
        shares=100,
        price_cents=1000,
        run_id="manual_buy_review",
        hypothesis={"thesis": "三日后复盘", "review_after_days": 3},
    )
    conn.execute(
        "UPDATE event_log SET occurred_at = ? WHERE stream = ?",
        ("2026-05-10T10:00:00+08:00", f"trade:002138:{order.order_id}"),
    )
    conn.execute(
        "UPDATE projection_orders SET filled_at = ?, created_at = ?, updated_at = ? WHERE order_id = ?",
        (
            "2026-05-10T10:00:00+08:00",
            "2026-05-10T10:00:00+08:00",
            "2026-05-10T10:00:00+08:00",
            order.order_id,
        ),
    )

    result = TradeReviewService(store, conn).review_due_trades(as_of="2026-05-13", record=False)
    review_events = store.query(stream=f"trade:002138:{order.order_id}", event_type="trade.review.recorded")

    assert result["status"] == "dry_run"
    assert result["items"][0]["status"] == "insufficient_market_bars"
    assert review_events == []
