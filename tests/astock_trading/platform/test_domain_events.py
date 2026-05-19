"""Domain event contract tests."""

from __future__ import annotations

from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore


def test_domain_event_names_are_stable():
    from astock_trading.platform import domain_events as events

    assert events.SCORE_CALCULATED == "score.calculated"
    assert events.DECISION_SUGGESTED == "decision.suggested"
    assert events.STRATEGY_CALIBRATION_PROPOSED == "strategy.calibration.proposed"
    assert events.STRATEGY_PROFILE_COMPARISON_PROPOSED == "strategy.profile_comparison.proposed"
    assert events.STRATEGY_CAPITAL_ALLOCATION_PROPOSED == "strategy.capital_allocation.proposed"
    assert events.STRATEGY_HEALTH_REPORT_PROPOSED == "strategy.health_report.proposed"
    assert events.RISK_ADAPTIVE_SUGGESTION_PROPOSED == "risk.adaptive_suggestion.proposed"
    assert events.MANUAL_TRADE_REQUESTED == "manual_trade.requested"
    assert events.TRADE_HYPOTHESIS_RECORDED == "trade.hypothesis.recorded"
    assert events.TRADE_OUTCOME_RECORDED == "trade.outcome.recorded"
    assert events.TRADE_REVIEW_RECORDED == "trade.review.recorded"
    assert events.EVIDENCE_BACKFILLED == "evidence.backfilled"
    assert events.AUTO_TRADE_EXECUTED == "auto_trade.executed"
    assert events.AUTO_TRADE_SUMMARY == "auto_trade.summary"
    assert events.CANDIDATE_ADDED == "candidate.added"


def test_domain_event_publisher_appends_event(tmp_path):
    from astock_trading.platform.domain_events import DomainEvent, DomainEventPublisher, SCORE_CALCULATED

    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    store = EventStore(conn)
    publisher = DomainEventPublisher(store)

    try:
        event_id = publisher.publish(
            DomainEvent(
                stream="strategy:002138",
                stream_type="strategy",
                event_type=SCORE_CALCULATED,
                payload={"code": "002138", "total_score": 7.1},
                metadata={"run_id": "run_1"},
            )
        )
        rows = store.query(event_type=SCORE_CALCULATED)
    finally:
        conn.close()

    assert event_id
    assert rows[0]["payload"]["code"] == "002138"
    assert rows[0]["metadata"]["run_id"] == "run_1"
