"""Tests for execution context — orders, positions, projections, service."""

import pytest

from astock_trading.execution.models import OrderSide, OrderStatus
from astock_trading.execution.orders import OrderManager
from astock_trading.execution.positions import PositionManager, PositionProjector
from astock_trading.execution.service import ExecutionService, SimulatedBroker
from astock_trading.platform.db import init_db, connect
from astock_trading.platform.events import EventStore


@pytest.fixture
def db(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    yield conn
    conn.close()


@pytest.fixture
def event_store(db):
    return EventStore(db)


@pytest.fixture
def order_mgr(event_store, db):
    return OrderManager(event_store, db)


@pytest.fixture
def pos_mgr(event_store, db):
    return PositionManager(event_store, db)


@pytest.fixture
def svc(event_store, db):
    return ExecutionService(event_store, db, broker=SimulatedBroker())


# ---------------------------------------------------------------------------
# Order tests
# ---------------------------------------------------------------------------

class TestOrders:
    def test_create_order(self, order_mgr):
        order = order_mgr.create_order(
            code="002138", name="双环传动", side=OrderSide.BUY,
            shares=100, price_cents=1500, run_id="run_1",
        )
        assert order.order_id.startswith("ord_")
        assert order.status == OrderStatus.PENDING
        assert order.shares == 100

    def test_fill_order(self, order_mgr):
        order = order_mgr.create_order(
            code="002138", name="双环传动", side=OrderSide.BUY,
            shares=100, price_cents=1500, run_id="run_1",
        )
        order_mgr.fill_order(order.order_id, fill_price_cents=1505, fee_cents=5, run_id="run_1")

        filled = order_mgr.get_order(order.order_id)
        assert filled.status == OrderStatus.FILLED

    def test_cancel_order(self, order_mgr):
        order = order_mgr.create_order(
            code="002138", name="双环传动", side=OrderSide.BUY,
            shares=100, price_cents=1500, run_id="run_1",
        )
        order_mgr.cancel_order(order.order_id, "test_cancel", run_id="run_1")

        cancelled = order_mgr.get_order(order.order_id)
        assert cancelled.status == OrderStatus.CANCELLED

    def test_fill_already_filled_raises(self, order_mgr):
        order = order_mgr.create_order(
            code="002138", name="双环传动", side=OrderSide.BUY,
            shares=100, price_cents=1500, run_id="run_1",
        )
        order_mgr.fill_order(order.order_id, 1500, 0, "run_1")

        with pytest.raises(ValueError, match="already filled"):
            order_mgr.fill_order(order.order_id, 1500, 0, "run_1")

    def test_events_written(self, order_mgr, event_store):
        order = order_mgr.create_order(
            code="002138", name="双环传动", side=OrderSide.BUY,
            shares=100, price_cents=1500, run_id="run_1",
        )
        order_mgr.fill_order(order.order_id, 1500, 5, "run_1")

        events = event_store.query(stream_type="order")
        assert len(events) == 2
        assert events[0]["event_type"] == "order.created"
        assert events[1]["event_type"] == "order.filled"


# ---------------------------------------------------------------------------
# Position tests
# ---------------------------------------------------------------------------

class TestPositions:
    def test_open_position(self, pos_mgr):
        pos = pos_mgr.open_position(
            code="002138", name="双环传动", shares=100,
            avg_cost_cents=1500, style="momentum", run_id="run_1",
        )
        assert pos.code == "002138"
        assert pos.shares == 100
        assert pos.avg_cost == 15.0

    def test_close_position(self, pos_mgr):
        pos_mgr.open_position(
            code="002138", name="双环传动", shares=100,
            avg_cost_cents=1500, style="momentum", run_id="run_1",
        )
        pnl = pos_mgr.close_position("002138", 100, 1600, "run_1", reason="take_profit")
        assert pnl == (1600 - 1500) * 100  # 10000 cents = 100 yuan

        # Position should be gone
        assert pos_mgr.get_position("002138") is None

    def test_get_positions(self, pos_mgr):
        pos_mgr.open_position("001", "A", 100, 1000, "slow_bull", "run_1")
        pos_mgr.open_position("002", "B", 200, 2000, "momentum", "run_1")

        positions = pos_mgr.get_positions()
        assert len(positions) == 2

    def test_close_nonexistent_raises(self, pos_mgr):
        with pytest.raises(ValueError, match="not found"):
            pos_mgr.close_position("999999", 100, 1000, "run_1")


# ---------------------------------------------------------------------------
# Projection rebuild tests
# ---------------------------------------------------------------------------

class TestProjectionRebuild:
    def test_rebuild_from_events(self, pos_mgr, event_store, db):
        # Open two positions
        pos_mgr.open_position("001", "A", 100, 1000, "slow_bull", "run_1")
        pos_mgr.open_position("002", "B", 200, 2000, "momentum", "run_1")
        # Close one
        pos_mgr.close_position("001", 100, 1200, "run_1")

        # Verify current state
        assert len(pos_mgr.get_positions()) == 1

        # Delete projection and rebuild
        projector = PositionProjector(event_store, db)
        rebuilt = projector.rebuild()

        assert len(rebuilt) == 1
        assert rebuilt[0].code == "002"
        assert rebuilt[0].shares == 200

    def test_rebuild_empty(self, event_store, db):
        projector = PositionProjector(event_store, db)
        rebuilt = projector.rebuild()
        assert len(rebuilt) == 0


# ---------------------------------------------------------------------------
# ExecutionService tests
# ---------------------------------------------------------------------------

class TestExecutionService:
    def test_buy_flow(self, svc):
        order = svc.execute_buy(
            code="002138", name="双环传动", shares=100,
            price_cents=1500, style="momentum", run_id="run_1",
        )
        assert order.order_id.startswith("ord_")

        # Position should exist
        pos = svc.get_position("002138")
        assert pos is not None
        assert pos.shares == 100

        # Portfolio should reflect
        portfolio = svc.get_portfolio()
        assert portfolio["holding_count"] == 1

    def test_sell_flow(self, svc):
        svc.execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")
        svc.execute_sell("002138", 100, 1600, "run_1", reason="take_profit")

        assert svc.get_position("002138") is None
        assert svc.get_portfolio()["holding_count"] == 0

    def test_rebuild_matches_current(self, svc):
        svc.execute_buy("001", "A", 100, 1000, "slow_bull", "run_1")
        svc.execute_buy("002", "B", 200, 2000, "momentum", "run_1")
        svc.execute_sell("001", 100, 1200, "run_1")

        # Current state
        before = svc.get_positions()
        assert len(before) == 1

        # Rebuild
        rebuilt = svc.rebuild_projections()
        assert len(rebuilt) == 1
        assert rebuilt[0].code == before[0].code
        assert rebuilt[0].shares == before[0].shares
        assert rebuilt[0].avg_cost_cents == before[0].avg_cost_cents

    def test_full_audit_trail(self, svc, event_store):
        svc.execute_buy("002138", "双环传动", 100, 1500, "momentum", "run_1")
        svc.execute_sell("002138", 100, 1600, "run_1", reason="take_profit")

        # Should have: order.created, order.filled, position.opened,
        #              order.created, order.filled, position.closed
        all_events = event_store.query()
        order_events = [e for e in all_events if e["stream_type"] == "order"]
        pos_events = [e for e in all_events if e["stream_type"] == "position"]

        assert len(order_events) == 4  # 2 creates + 2 fills
        assert len(pos_events) == 2    # 1 opened + 1 closed

    def test_manual_buy_audit_passes_after_record_buy(self, svc):
        order = svc.record_buy(
            code="002138",
            name="双环传动",
            shares=100,
            price_cents=1500,
            fee_cents=5,
            reason="manual_confirmed",
            run_id="manual_buy_test",
        )

        audit = svc.audit_manual_trade_consistency(order.order_id)

        assert audit["ok"] is True
        assert audit["issues"] == []
        assert audit["order"]["status"] == "filled"
        assert audit["position"]["shares"] == 100

    def test_manual_buy_records_hypothesis_and_outcome_evidence(self, svc, event_store):
        order = svc.record_buy(
            code="002138",
            name="双环传动",
            shares=100,
            price_cents=1500,
            fee_cents=5,
            reason="人工确认突破后回踩不破",
            run_id="manual_buy_evidence",
            source_event_id="decision_evt_1",
            source_score_event_id="score_evt_1",
            hypothesis={
                "thesis": "突破后回踩不破，资金流仍为正",
                "invalidation": "跌破 MA20 或主力连续流出",
                "review_after_days": 3,
            },
        )

        trade_events = event_store.query(stream=f"trade:002138:{order.order_id}")
        event_types = [event["event_type"] for event in trade_events]
        assert event_types == ["trade.hypothesis.recorded", "trade.outcome.recorded"]

        hypothesis = trade_events[0]["payload"]
        assert hypothesis["order_id"] == order.order_id
        assert hypothesis["side"] == "buy"
        assert hypothesis["source_event_id"] == "decision_evt_1"
        assert hypothesis["source_score_event_id"] == "score_evt_1"
        assert hypothesis["hypothesis"]["thesis"] == "突破后回踩不破，资金流仍为正"
        assert hypothesis["hypothesis"]["manual_reason"] == "人工确认突破后回踩不破"

        outcome = trade_events[1]["payload"]
        assert outcome["order_id"] == order.order_id
        assert outcome["status"] == "filled"
        assert outcome["fill_price_cents"] == 1500
        assert outcome["position_after"]["shares"] == 100
        assert trade_events[1]["metadata"]["run_id"] == "manual_buy_evidence"

    def test_manual_buy_audit_detects_position_drift(self, svc, db):
        order = svc.record_buy(
            code="002138",
            name="双环传动",
            shares=100,
            price_cents=1500,
            run_id="manual_buy_test",
        )
        db.execute(
            "UPDATE projection_positions SET shares = ? WHERE code = ?",
            (200, "002138"),
        )

        audit = svc.audit_manual_trade_consistency(order.order_id)

        assert audit["ok"] is False
        assert "position_shares_mismatch" in audit["issues"]
