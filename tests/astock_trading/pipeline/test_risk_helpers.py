"""Pipeline 风控 helper 测试。"""

from types import SimpleNamespace

from astock_trading.execution.models import Position
from astock_trading.pipeline.helpers import check_position_risks
from astock_trading.risk.models import ExitSignal


class FakeMarketService:
    async def collect_batch(self, stock_list, run_id):
        assert stock_list == [{"code": "002138", "name": "双环传动"}]
        assert run_id == "run_helper"
        return [
            SimpleNamespace(
                code="002138",
                quote=None,
                technical=SimpleNamespace(ma20=10.5, ma60=9.5),
            )
        ]


class FakeRiskService:
    def __init__(self):
        self.calls = []

    def assess_position(self, **kwargs):
        self.calls.append(kwargs)
        return [
            ExitSignal(
                code=kwargs["code"],
                signal_type="fake_service_signal",
                trigger_price=1.0,
                current_price=kwargs["current_price"],
                description="来自服务层",
                urgency="immediate",
            )
        ]


def test_check_position_risks_routes_through_risk_service():
    risk_svc = FakeRiskService()
    ctx = SimpleNamespace(
        cfg={"risk": {"momentum": {"stop_loss": 0.08}}},
        market_svc=FakeMarketService(),
        risk_svc=risk_svc,
    )
    position = Position(
        code="002138",
        name="双环传动",
        style="momentum",
        shares=100,
        avg_cost_cents=1000,
        entry_date="2026-04-01",
        highest_since_entry_cents=1200,
        entry_day_low_cents=980,
        current_price_cents=1100,
    )

    results = check_position_risks(ctx, [position], "run_helper")

    assert results[0][0] is position
    assert [signal.signal_type for signal in results[0][1]] == ["fake_service_signal"]
    assert len(risk_svc.calls) == 1
    call = risk_svc.calls[0]
    assert call["code"] == "002138"
    assert call["run_id"] == "run_helper"
    assert call["ma20"] == 10.5
    assert call["ma60"] == 9.5


class FakeExecService:
    def __init__(self, positions):
        self._positions = positions

    def get_positions(self):
        return self._positions


class FakeIntradayMarketService:
    async def collect_intraday_batch(self, stock_list, run_id):
        assert stock_list == [{"code": "002138", "name": "双环传动"}]
        assert run_id == "run_intraday"
        return [
            SimpleNamespace(
                code="002138",
                quote=None,
                technical=SimpleNamespace(ma20=10.5, ma60=9.5),
            )
        ]


class FakeEventStore:
    def __init__(self):
        self.appended = []

    def query(self, **kwargs):
        return []

    def append(self, stream, stream_type, event_type, payload, metadata=None):
        self.appended.append({
            "stream": stream,
            "stream_type": stream_type,
            "event_type": event_type,
            "payload": payload,
            "metadata": metadata or {},
        })


class FakeConn:
    def __init__(self):
        self.committed = False

    def commit(self):
        self.committed = True


def test_intraday_monitor_routes_position_risk_through_service(monkeypatch):
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, None),
    )
    risk_svc = FakeRiskService()
    event_store = FakeEventStore()
    position = Position(
        code="002138",
        name="双环传动",
        style="momentum",
        shares=100,
        avg_cost_cents=1000,
        entry_date="2026-04-01",
        highest_since_entry_cents=1200,
        entry_day_low_cents=980,
        current_price_cents=1100,
    )
    ctx = SimpleNamespace(
        cfg={"risk": {"momentum": {"stop_loss": 0.08}}},
        exec_svc=FakeExecService([position]),
        market_svc=FakeIntradayMarketService(),
        risk_svc=risk_svc,
        event_store=event_store,
        conn=FakeConn(),
    )

    from astock_trading.pipeline.intraday_monitor import run

    result = run(ctx, "run_intraday")

    assert [alert["signal_type"] for alert in result["alerts"]] == ["fake_service_signal"]
    assert len(risk_svc.calls) == 1
    assert risk_svc.calls[0]["code"] == "002138"
    assert risk_svc.calls[0]["run_id"] == "run_intraday"
    assert risk_svc.calls[0]["ma20"] == 10.5
    assert ctx.conn.committed is True
