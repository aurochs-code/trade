"""Tests for event evidence query helpers."""

from __future__ import annotations

from astock_trading.platform.cli import events as events_cli
from astock_trading.platform.events import EventStore
from astock_trading.platform.evidence import backfill_legacy_evidence


def test_query_evidence_events_returns_stock_evidence_chain(mysql_conn):
    conn = mysql_conn
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


def test_query_events_defaults_to_newest_first_and_supports_ascending(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)

    try:
        old_id = store.append("paper:summary", "paper_trade", "auto_trade.summary", {"date": "2026-05-21"})
        new_id = store.append("paper:summary", "paper_trade", "auto_trade.summary", {"date": "2026-05-22"})
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-21T06:00:00+00:00", old_id),
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T06:00:00+00:00", new_id),
        )

        newest_first = events_cli._query_events(conn, event_type="auto_trade.summary", limit=2)
        oldest_first = events_cli._query_events(
            conn,
            event_type="auto_trade.summary",
            limit=2,
            order="asc",
        )
    finally:
        conn.close()

    assert [event["payload"]["date"] for event in newest_first] == ["2026-05-22", "2026-05-21"]
    assert [event["payload"]["date"] for event in oldest_first] == ["2026-05-21", "2026-05-22"]


def test_backfill_legacy_evidence_appends_partial_evidence_once(mysql_conn):
    conn = mysql_conn
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


def test_backfill_legacy_evidence_recovers_buy_entry_route_from_source_score(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)

    try:
        score_event_id = store.append(
            "strategy:002384",
            "strategy",
            "score.calculated",
            {
                "code": "002384",
                "name": "东山精密",
                "total_score": 7.0,
                "entry_signal": True,
                "primary_strategy_route": "flow_confirmed_trend",
                "strategy_routes": [
                    {
                        "route": "flow_confirmed_trend",
                        "display_name": "资金趋势确认",
                        "entry_signal": True,
                    }
                ],
                "technical_detail": "金叉成立，资金确认",
                "data_quality": "ok",
                "dimensions": [{"name": "technical", "score": 2.5, "max_score": 3.0}],
                "source_observation_id": "obs-002384",
            },
        )
        decision_event_id = store.append(
            "strategy:002384",
            "strategy",
            "decision.suggested",
            {
                "code": "002384",
                "name": "东山精密",
                "action": "BUY",
                "score": 7.0,
                "source_score_event_id": score_event_id,
                "decision_inputs": {"weekly_buy_count": 0},
                "decision_rules": {"buy_threshold": 6.5},
            },
        )
        manual_event_id = store.append(
            "manual_trade:002384",
            "manual_trade",
            "manual_trade.requested",
            {
                "status": "pending",
                "side": "buy",
                "code": "002384",
                "name": "东山精密",
                "score": 7.0,
                "source_event_id": decision_event_id,
                "source_score_event_id": score_event_id,
            },
        )

        dry_run = backfill_legacy_evidence(conn, code="002384", apply=False)
        applied = backfill_legacy_evidence(conn, code="002384", apply=True)
        second = backfill_legacy_evidence(conn, code="002384", apply=True)
        events = events_cli._query_evidence_events(conn, "002384", limit=50)
    finally:
        conn.close()

    assert dry_run["status"] == "dry_run"
    assert dry_run["planned_count"] == 2
    assert {item["source_event_id"] for item in dry_run["planned"]} == {
        decision_event_id,
        manual_event_id,
    }
    assert applied["created_count"] == 2
    assert second["created_count"] == 0

    recovered = [
        event for event in events
        if event["event_type"] == "evidence.backfilled"
        and event["payload"]["evidence_status"] == "recovered_signal_evidence"
    ]
    assert {event["payload"]["source_event_id"] for event in recovered} == {
        decision_event_id,
        manual_event_id,
    }
    first_payload = recovered[0]["payload"]
    assert first_payload["source_score_event_id"] == score_event_id
    assert first_payload["recovered_evidence"] == {
        "entry_signal": True,
        "primary_strategy_route": "flow_confirmed_trend",
        "primary_strategy_route_label": "资金趋势确认",
        "strategy_routes": [
            {
                "route": "flow_confirmed_trend",
                "display_name": "资金趋势确认",
                "entry_signal": True,
            }
        ],
        "technical_detail": "金叉成立，资金确认",
        "data_quality": "ok",
    }
