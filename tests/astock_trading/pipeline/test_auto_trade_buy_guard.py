from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from astock_trading.pipeline.auto_trade import run
from astock_trading.pipeline.paper_account import PaperBalance
from astock_trading.platform.db import connect, init_db
from astock_trading.strategy.models import MarketSignal, MarketState


class FakePaperAccount:
    def __init__(self):
        self.buy_calls: list[tuple[str, int]] = []

    def get_positions(self):
        return []

    def get_balance(self):
        return PaperBalance(total_asset=100_000, available_cash=100_000, market_value=0)

    def get_exposure(self):
        return 0.0, 100_000

    def buy(self, code, shares):
        self.buy_calls.append((code, shares))
        return SimpleNamespace(success=True, order_id="order-1", error="")


class FakeEventStore:
    def __init__(self, decision_events=None, portfolio_breach_events=None):
        self.decision_events = decision_events or []
        self.portfolio_breach_events = portfolio_breach_events or []
        self.appended: list[dict] = []

    def query(self, **kwargs):
        if kwargs.get("event_type") == "decision.suggested":
            return self.decision_events[: kwargs.get("limit", len(self.decision_events))]
        if kwargs.get("event_type") == "risk.portfolio_breach":
            return self.portfolio_breach_events[: kwargs.get("limit", len(self.portfolio_breach_events))]
        return []

    def append(self, stream, stream_type, event_type, payload, metadata=None):
        self.appended.append(
            {
                "stream": stream,
                "stream_type": stream_type,
                "event_type": event_type,
                "payload": payload,
                "metadata": metadata or {},
            }
        )
        return f"event-{len(self.appended)}"


class FakeMarketService:
    async def collect_market_state(self, run_id):
        return MarketState(signal=MarketSignal.GREEN, multiplier=1.0), []

    async def collect_batch(self, stock_list, run_id):
        return [
            SimpleNamespace(code=item["code"], quote=SimpleNamespace(close=10.0))
            for item in stock_list
        ]


class FakeRunJournal:
    def __init__(self, failed_runs=None):
        self.failed_runs = failed_runs or []

    def get_failed_runs(self, days=1):
        return self.failed_runs


class FakeObsidian:
    def write_paper_report(self, **kwargs):
        pass

    def append_paper_trade_log(self, trade_rows):
        pass

    def write_daily_output_index(self, run_id):
        pass

    def write_daily_log(self, run_id, content):
        pass


@pytest.fixture
def auto_trade_ctx(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    ctx = SimpleNamespace(
        conn=conn,
        event_store=FakeEventStore(),
        run_journal=FakeRunJournal(),
        cfg={
            "auto_trade": {
                "enabled": True,
                "dry_run": True,
                "buy_guard": {"max_age_hours": 24},
            },
            "risk": {
                "position": {
                    "total_max": 0.60,
                    "single_max": 0.20,
                    "weekly_max": 2,
                }
            },
            "scoring": {"thresholds": {"buy": 6.5}},
        },
        market_svc=FakeMarketService(),
        projector=SimpleNamespace(sync_market_state=lambda index_data: None),
        obsidian=FakeObsidian(),
    )
    try:
        yield ctx
    finally:
        conn.close()


def _seed_core_candidate(conn, *, scored_at: datetime):
    conn.execute(
        """INSERT INTO projection_candidate_pool
           (code, pool_tier, name, score, added_at, last_scored_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("002138", "core", "双环传动", 7.5, scored_at.isoformat(), scored_at.isoformat()),
    )


def test_run_skips_buy_and_returns_diagnostic_without_fresh_decision_events(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    _seed_core_candidate(
        auto_trade_ctx.conn,
        scored_at=datetime.now(timezone.utc) - timedelta(hours=1),
    )

    result = run(auto_trade_ctx, "run-no-fresh-decision")

    assert result["buys"] == []
    assert result["diagnostics"][0]["reason"] == "no_fresh_decision_events"
    assert paper.buy_calls == []
    diagnostic_events = [
        event for event in auto_trade_ctx.event_store.appended
        if event["event_type"] == "auto_trade.diagnostic"
    ]
    assert diagnostic_events
    assert diagnostic_events[0]["metadata"] == {
        "run_id": "run-no-fresh-decision",
        "account": "paper",
    }


def test_run_skips_buy_when_new_trade_guard_blocks_failed_runs(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    now = datetime.now(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.run_journal = FakeRunJournal(
        failed_runs=[{"run_id": "run_failed", "run_type": "evening"}]
    )
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-new-trade-guard")

    assert result["buys"] == []
    assert result["diagnostics"][0]["reason"] == "new_trade_guard_blocked"
    assert result["diagnostics"][0]["details"]["blockers"][0]["reason"] == "recent_failed_pipeline"
    assert paper.buy_calls == []


def test_run_skips_buy_and_returns_diagnostic_when_core_pool_is_empty(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    now = datetime.now(timezone.utc)
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-empty-core")

    assert result["buys"] == []
    assert result["diagnostics"][0]["reason"] == "core_pool_empty"
    assert paper.buy_calls == []


def test_run_skips_buy_and_returns_diagnostic_when_scoring_inputs_are_stale(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    now = datetime.now(timezone.utc)
    _seed_core_candidate(
        auto_trade_ctx.conn,
        scored_at=now - timedelta(days=3),
    )
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-stale-scoring")

    assert result["buys"] == []
    assert result["diagnostics"][0]["reason"] == "scoring_inputs_stale"
    assert result["diagnostics"][0]["details"]["age_hours"] > 24
    assert paper.buy_calls == []


def test_run_does_not_push_discord_when_no_trade_actions(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    calls = []
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: calls.append((args, kwargs)) or (True, ""),
    )

    result = run(auto_trade_ctx, "run-no-actions")

    assert result["buys"] == []
    assert result["sells"] == []
    assert result["discord_embed"] is None
    assert calls == []


def test_run_pushes_discord_when_auto_trade_has_actions(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    calls = []
    sell_action = {
        "side": "sell",
        "code": "002138",
        "name": "双环传动",
        "shares": 100,
        "price": 10.0,
        "reason": "stop_loss",
        "status": "dry_run",
    }
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.pipeline.auto_trade._check_and_sell",
        lambda *args, **kwargs: [sell_action],
    )
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: calls.append((args, kwargs)) or (True, ""),
    )

    result = run(auto_trade_ctx, "run-with-actions")

    assert result["sells"] == [sell_action]
    assert result["discord_embed"] is not None
    assert len(calls) == 1
