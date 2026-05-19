"""Tests for event evidence query helpers."""

from __future__ import annotations

from astock_trading.platform.cli import events as events_cli
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.platform.evidence import backfill_legacy_evidence


def test_query_evidence_events_returns_stock_evidence_chain(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    store = EventStore(conn)

    try:
        store.append("strategy:002138", "strategy", "score.calculated", {"code": "002138"})
        store.append("strategy:002138", "strategy", "decision.suggested", {"code": "002138"})
        store.append("manual_trade:002138", "manual_trade", "manual_trade.requested", {"code": "002138"})
        store.append("order:002138:ord_1", "order", "order.filled", {"code": "002138"})
        store.append("position:002138", "position", "position.opened", {"code": "002138"})
        store.append("trade:002138:ord_1", "trade", "trade.hypothesis.recorded", {"code": "002138"})
        store.append("strategy:000001", "strategy", "score.calculated", {"code": "000001"})

        assert hasattr(events_cli, "_query_evidence_events")
        events = events_cli._query_evidence_events(conn, "002138", limit=20)
    finally:
        conn.close()

    assert [event["event_type"] for event in events] == [
        "score.calculated",
        "decision.suggested",
        "manual_trade.requested",
        "order.filled",
        "position.opened",
        "trade.hypothesis.recorded",
    ]


def test_backfill_legacy_evidence_appends_partial_evidence_once(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    store = EventStore(conn)

    try:
        score_event_id = store.append(
            "strategy:002138",
            "strategy",
            "score.calculated",
            {"code": "002138", "total_score": 7.1},
        )
        decision_event_id = store.append(
            "strategy:002138",
            "strategy",
            "decision.suggested",
            {"code": "002138", "action": "BUY"},
        )
        order_created_id = store.append(
            "order:002138:ord_legacy",
            "order",
            "order.created",
            {
                "order_id": "ord_legacy",
                "code": "002138",
                "name": "双环传动",
                "side": "buy",
                "shares": 100,
                "price_cents": 1500,
                "broker": "manual",
            },
        )
        order_filled_id = store.append(
            "order:002138:ord_legacy",
            "order",
            "order.filled",
            {
                "order_id": "ord_legacy",
                "code": "002138",
                "side": "buy",
                "shares": 100,
                "fill_price_cents": 1510,
                "fee_cents": 5,
            },
        )

        first = backfill_legacy_evidence(conn, apply=True)
        second = backfill_legacy_evidence(conn, apply=True)
        events = events_cli._query_evidence_events(conn, "002138", limit=50)
    finally:
        conn.close()

    assert first["status"] == "applied"
    assert first["created_count"] == 4
    assert second["created_count"] == 0
    assert {item["source_event_id"] for item in first["created"]} == {
        score_event_id,
        decision_event_id,
        order_created_id,
        order_filled_id,
    }
    backfilled = [event for event in events if event["event_type"] == "evidence.backfilled"]
    assert len(backfilled) == 2
    assert backfilled[0]["payload"]["evidence_status"] == "legacy_partial"
    assert backfilled[0]["payload"]["legacy_payload"]["code"] == "002138"
    trade_types = [event["event_type"] for event in events if event["stream_type"] == "trade"]
    assert trade_types == ["trade.hypothesis.recorded", "trade.outcome.recorded"]
    outcome = [event for event in events if event["event_type"] == "trade.outcome.recorded"][0]
    assert outcome["payload"]["source_event_id"] == order_filled_id
    assert outcome["payload"]["fill_price_cents"] == 1510
