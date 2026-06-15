"""RiskService 事件持久化测试。"""

from datetime import date

from astock_trading.risk.models import RiskParams
from astock_trading.risk.service import RiskService
from astock_trading.strategy.models import Style


class FakeEventStore:
    def __init__(self):
        self.events = []

    def append(self, stream, stream_type, event_type, payload, metadata=None):
        self.events.append({
            "stream": stream,
            "stream_type": stream_type,
            "event_type": event_type,
            "payload": payload,
            "metadata": metadata or {},
        })
        return f"evt_{len(self.events)}"


def test_assess_position_records_threshold_snapshot_without_trigger():
    store = FakeEventStore()
    svc = RiskService(store)

    signals = svc.assess_position(
        code="002138",
        avg_cost=10.0,
        current_price=11.0,
        entry_date=date(2026, 4, 1),
        today=date(2026, 4, 6),
        highest_since_entry=12.0,
        entry_day_low=9.8,
        risk_params=RiskParams(
            style=Style.MOMENTUM,
            stop_loss=0.08,
            trailing_stop=0.10,
            time_stop_days=15,
            exit_ma=20,
        ),
        run_id="run_threshold_snapshot",
        ma20=10.5,
    )

    assert signals == []
    assert [event["event_type"] for event in store.events] == ["risk.threshold_snapshot"]
    event = store.events[0]
    assert event["stream"] == "risk:002138"
    assert event["metadata"] == {"run_id": "run_threshold_snapshot"}
    assert event["payload"]["thresholds"]["stop_loss"]["trigger_price"] == 9.2
    assert event["payload"]["thresholds"]["trailing_stop"]["trigger_price"] == 10.8
    assert event["payload"]["triggered_signal_types"] == []


def test_assess_position_records_snapshot_before_trigger_event():
    store = FakeEventStore()
    svc = RiskService(store)

    signals = svc.assess_position(
        code="002138",
        avg_cost=10.0,
        current_price=9.0,
        entry_date=date(2026, 4, 1),
        today=date(2026, 4, 6),
        highest_since_entry=12.0,
        entry_day_low=9.8,
        risk_params=RiskParams(style=Style.MOMENTUM, stop_loss=0.08),
        run_id="run_stop_loss",
    )

    assert [signal.signal_type for signal in signals] == ["stop_loss"]
    assert [event["event_type"] for event in store.events] == [
        "risk.threshold_snapshot",
        "risk.stop_loss_triggered",
    ]
    assert store.events[0]["payload"]["triggered_signal_types"] == ["stop_loss"]
