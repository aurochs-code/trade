"""周度表现快照持久化测试。"""

from types import SimpleNamespace

from astock_trading.pipeline.weekly import run


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConnection:
    def execute(self, sql, params=()):
        if "FROM projection_orders" in sql:
            return FakeResult([
                {
                    "order_id": "buy-1",
                    "code": "002138",
                    "side": "buy",
                    "shares": 100,
                    "price_cents": 1000,
                    "filled_at": "2026-06-15T02:00:00+00:00",
                },
                {
                    "order_id": "sell-1",
                    "code": "002138",
                    "side": "sell",
                    "shares": 100,
                    "price_cents": 1100,
                    "filled_at": "2026-06-16T02:00:00+00:00",
                },
            ])
        if "FROM projection_candidate_pool" in sql:
            return FakeResult([])
        raise AssertionError(sql)


class FakeEventStore:
    def __init__(self):
        self.appended = []

    def query(self, event_type=None, **kwargs):
        if event_type == "position.closed":
            return [{"payload": {"code": "002138", "realized_pnl_cents": 10000}}]
        return []

    def append(self, stream, stream_type, event_type, payload, metadata=None):
        self.appended.append({
            "stream": stream,
            "stream_type": stream_type,
            "event_type": event_type,
            "payload": payload,
            "metadata": metadata or {},
        })
        return f"evt_{len(self.appended)}"


class FakeObsidian:
    def __init__(self):
        self.weekly_review = None

    def write_weekly_review(self, stats):
        self.weekly_review = stats

    def write_daily_log(self, run_id, content):
        return None


def test_weekly_pipeline_records_performance_snapshot(monkeypatch):
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, None),
    )
    event_store = FakeEventStore()
    obsidian = FakeObsidian()
    ctx = SimpleNamespace(
        conn=FakeConnection(),
        event_store=event_store,
        exec_svc=SimpleNamespace(get_positions=lambda: []),
        reporter=SimpleNamespace(generate_weekly_report=lambda week: "# 周报"),
        obsidian=obsidian,
        cfg={"risk": {"position": {"weekly_max": 2}}},
    )

    result = run(ctx, "run_weekly_snapshot")

    events = [event for event in event_store.appended if event["event_type"] == "performance.weekly_snapshot"]
    assert len(events) == 1
    event = events[0]
    assert event["stream"] == "performance:weekly"
    assert event["stream_type"] == "performance"
    assert event["metadata"] == {"run_id": "run_weekly_snapshot"}
    assert event["payload"]["week_str"] == result["week"]
    assert event["payload"]["buy_count"] == 1
    assert event["payload"]["sell_count"] == 1
    assert event["payload"]["net_pnl_cents"] == 10000
    assert obsidian.weekly_review is event["payload"]
