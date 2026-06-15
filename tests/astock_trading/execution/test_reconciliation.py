"""模拟盘 vs 实盘逐笔对账。"""

from __future__ import annotations

from astock_trading.execution.reconciliation import TradeReconciliationService
from astock_trading.execution.service import ExecutionService, SimulatedBroker
from astock_trading.platform.domain_events import AUTO_TRADE_EXECUTED
from astock_trading.platform.events import EventStore


def _paper_buy(store: EventStore, *, code: str, signal_id: str, shares: int, price: float) -> str:
    event_id = store.append(
        stream=f"paper:{code}",
        stream_type="paper_trade",
        event_type=AUTO_TRADE_EXECUTED,
        payload={
            "side": "buy",
            "code": code,
            "name": code,
            "shares": shares,
            "price": price,
            "status": "filled",
            "order_id": f"paper_{signal_id}",
            "source_score_event_id": signal_id,
        },
        metadata={"run_id": "paper_reconcile", "account": "paper"},
    )
    return event_id


def _set_event_time(conn, event_id: str, occurred_at: str) -> None:
    conn.execute("UPDATE event_log SET occurred_at = ? WHERE event_id = ?", (occurred_at, event_id))


def _set_stream_time(conn, stream: str, occurred_at: str) -> None:
    conn.execute("UPDATE event_log SET occurred_at = ? WHERE stream = ?", (occurred_at, stream))


def test_shadow_reconcile_records_structured_deviations_idempotently(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)
    exec_svc = ExecutionService(store, conn, broker=SimulatedBroker())

    matched_paper = _paper_buy(store, code="002138", signal_id="score_match", shares=100, price=10.0)
    missing_paper = _paper_buy(store, code="300558", signal_id="score_missing", shares=100, price=20.0)
    partial_paper = _paper_buy(store, code="600703", signal_id="score_partial", shares=200, price=11.0)
    for event_id in (matched_paper, missing_paper, partial_paper):
        _set_event_time(conn, event_id, "2026-05-18T10:00:00+08:00")

    matched_order = exec_svc.record_buy(
        code="002138",
        name="双环传动",
        shares=100,
        price_cents=1000,
        run_id="manual_match",
        source_score_event_id="score_match",
        hypothesis={"manual_reason": "人工确认执行"},
    )
    partial_order = exec_svc.record_buy(
        code="600703",
        name="三安光电",
        shares=100,
        price_cents=1110,
        run_id="manual_partial",
        source_score_event_id="score_partial",
        hypothesis={"manual_reason": "人工只执行半仓"},
    )
    extra_order = exec_svc.record_buy(
        code="000001",
        name="平安银行",
        shares=100,
        price_cents=1200,
        run_id="manual_extra",
        source_score_event_id="score_extra",
        hypothesis={"manual_reason": "人工额外买入"},
    )
    for order in (matched_order, partial_order, extra_order):
        _set_stream_time(conn, f"trade:{order.code}:{order.order_id}", "2026-05-18T10:05:00+08:00")

    result = TradeReconciliationService(store).reconcile(date="2026-05-18", record=True)
    second = TradeReconciliationService(store).reconcile(date="2026-05-18", record=True)
    deviation_events = store.query(event_type="rule_deviation.recorded")

    assert result["status"] == "applied"
    assert result["summary"]["paper_trades"] == 3
    assert result["summary"]["real_trades"] == 3
    assert result["summary"]["matched"] == 1
    assert result["summary"]["deviation_count"] == 3
    assert result["summary"]["deviation_types"] == {
        "extra_real_trade": 1,
        "not_executed": 1,
        "partial_fill": 1,
    }
    by_type = {item["deviation_type"]: item for item in result["items"] if item["deviation_type"] != "matched"}
    assert by_type["not_executed"]["rule_deviation"] == "shadow_divergence"
    assert by_type["extra_real_trade"]["real"]["order_id"] == extra_order.order_id
    assert by_type["partial_fill"]["paper"]["shares"] == 200
    assert by_type["partial_fill"]["real"]["shares"] == 100
    assert result["recorded_count"] == 3
    assert second["recorded_count"] == 0
    assert len(deviation_events) == 3
    assert {event["payload"]["rule_deviation"] for event in deviation_events} == {"shadow_divergence"}
