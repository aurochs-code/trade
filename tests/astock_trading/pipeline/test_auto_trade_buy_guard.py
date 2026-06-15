from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from astock_trading.market.models import TechnicalIndicators
from astock_trading.pipeline.auto_trade import (
    _check_and_sell,
    _get_buy_candidates,
    _get_highest_since_entry,
    _score_and_buy,
    build_auto_trade_readiness,
    run,
)
from astock_trading.pipeline.paper_account import PaperBalance, PaperPosition
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import MARKET_TZ
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
    def __init__(
        self,
        decision_events=None,
        portfolio_breach_events=None,
        auto_trade_events=None,
        profile_activation_events=None,
    ):
        self.decision_events = decision_events or []
        self.portfolio_breach_events = portfolio_breach_events or []
        self.auto_trade_events = auto_trade_events or []
        self.profile_activation_events = profile_activation_events or []
        self.appended: list[dict] = []

    def query(self, **kwargs):
        if kwargs.get("event_type") == "decision.suggested":
            return self.decision_events[: kwargs.get("limit", len(self.decision_events))]
        if kwargs.get("event_type") == "risk.portfolio_breach":
            return self.portfolio_breach_events[: kwargs.get("limit", len(self.portfolio_breach_events))]
        if kwargs.get("event_type") == "auto_trade.executed":
            return self.auto_trade_events[: kwargs.get("limit", len(self.auto_trade_events))]
        if kwargs.get("event_type") == "strategy.profile_activation.requested":
            return self.profile_activation_events[: kwargs.get("limit", len(self.profile_activation_events))]
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


class LightweightRiskMarketService:
    def __init__(self):
        self.collect_batch_calls = 0
        self.collect_intraday_calls = 0

    async def collect_batch(self, stock_list, run_id):
        self.collect_batch_calls += 1
        raise AssertionError("卖出风控不应拉取完整个股快照")

    async def collect_intraday_batch(self, stock_list, run_id):
        self.collect_intraday_calls += 1
        return [
            SimpleNamespace(
                code=item["code"],
                technical=TechnicalIndicators(ma20=9.0, ma60=8.0),
            )
            for item in stock_list
        ]


class NoPriceMarketService(FakeMarketService):
    async def collect_batch(self, stock_list, run_id):
        return [
            SimpleNamespace(code=item["code"], quote=SimpleNamespace(close=0.0))
            for item in stock_list
        ]


class FakeRunJournal:
    def __init__(self, failed_runs=None, successful_runs=None):
        self.failed_runs = failed_runs or []
        self.successful_runs = successful_runs or []

    def get_failed_runs(self, days=1):
        return self.failed_runs

    def list_runs(self, run_type=None, status=None, limit=20):
        rows = self.successful_runs
        if run_type:
            rows = [row for row in rows if row.get("run_type") == run_type]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        return rows[:limit]


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
def auto_trade_ctx(mysql_conn):
    conn = mysql_conn
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


def _set_trading_now(monkeypatch, *, hour: int = 10, minute: int = 0) -> datetime:
    trade_time = datetime(2026, 5, 22, hour, minute, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    return trade_time.astimezone(timezone.utc)


def test_highest_since_entry_uses_local_market_bars(mysql_conn):
    conn = mysql_conn
    try:
        conn.execute(
            """INSERT INTO market_bars
               (symbol, bar_date, period, open_cents, high_cents, low_cents, close_cents,
                volume, amount_cents, source, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "002138",
                "2026-05-20",
                "daily",
                1000,
                1280,
                990,
                1200,
                1000,
                100000,
                "test",
                "2026-05-20T15:00:00+08:00",
            ),
        )

        highest = _get_highest_since_entry(
            "002138",
            datetime(2026, 5, 19, tzinfo=timezone.utc).date(),
            current_price=12.1,
            conn=conn,
        )

        assert highest == 12.8
    finally:
        conn.close()


def test_sell_risk_uses_lightweight_intraday_snapshots(auto_trade_ctx):
    market = LightweightRiskMarketService()
    auto_trade_ctx.market_svc = market
    position = PaperPosition(
        code="002138",
        name="双环传动",
        shares=100,
        avg_cost=10.0,
        current_price=10.2,
        market_value=1020.0,
        pnl=20.0,
        pnl_pct=0.02,
    )

    sells = _check_and_sell(
        auto_trade_ctx,
        FakePaperAccount(),
        [position],
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
        "run-lightweight-risk",
        auto_trade_ctx.cfg["auto_trade"],
        dry_run=True,
    )

    assert sells == []
    assert market.collect_intraday_calls == 1
    assert market.collect_batch_calls == 0


def test_sell_event_inherits_latest_paper_buy_route(auto_trade_ctx):
    position = PaperPosition(
        code="002138",
        name="双环传动",
        shares=100,
        avg_cost=10.0,
        current_price=11.0,
        market_value=1100.0,
        pnl=100.0,
        pnl_pct=0.1,
    )
    auto_trade_ctx.event_store = FakeEventStore(
        auto_trade_events=[
            {
                "event_type": "auto_trade.executed",
                "occurred_at": "2026-05-21T02:00:00+00:00",
                "payload": {
                    "side": "buy",
                    "code": "002138",
                    "status": "filled",
                    "primary_strategy_route": "flow_confirmed_trend",
                    "primary_strategy_route_label": "资金趋势确认",
                },
                "metadata": {"account": "paper"},
            }
        ]
    )

    sells = _check_and_sell(
        auto_trade_ctx,
        FakePaperAccount(),
        [position],
        MarketState(signal=MarketSignal.CLEAR, multiplier=0.0),
        "run-sell-route",
        auto_trade_ctx.cfg["auto_trade"],
        dry_run=True,
    )

    assert sells[0]["primary_strategy_route"] == "flow_confirmed_trend"
    assert sells[0]["primary_strategy_route_label"] == "资金趋势确认"
    event_payload = auto_trade_ctx.event_store.appended[-1]["payload"]
    assert event_payload["primary_strategy_route_label"] == "资金趋势确认"


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
    now = _set_trading_now(monkeypatch)
    _seed_core_candidate(
        auto_trade_ctx.conn,
        scored_at=now - timedelta(hours=1),
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
    now = _set_trading_now(monkeypatch)
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


def test_run_allows_buy_when_failed_auto_trade_has_recovered(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    now = _set_trading_now(monkeypatch)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.run_journal = FakeRunJournal(
        failed_runs=[
            {
                "run_id": "run_auto_trade_failed",
                "run_type": "auto_trade",
                "started_at": (now - timedelta(minutes=30)).isoformat(),
                "error_message": "stale running cleaned up after 0h",
            }
        ],
        successful_runs=[
            {
                "run_id": "run_auto_trade_recovered",
                "run_type": "auto_trade",
                "status": "completed",
                "started_at": (now - timedelta(minutes=5)).isoformat(),
            }
        ],
    )
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-1",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-1",
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-recovered-failed-auto-trade")

    assert result["diagnostics"] == []
    assert result["buys"][0]["code"] == "002138"
    assert result["buys"][0]["status"] == "dry_run"


def test_run_submits_dry_run_buy_inside_configured_buy_window(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-window",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-window",
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-window-dry-buy")

    assert result["window_state"]["buy_open"] is True
    assert result["buys"][0]["code"] == "002138"
    assert result["buys"][0]["status"] == "dry_run"
    assert result["buys"][0]["shares"] == 2000
    assert paper.buy_calls == []
    executed_events = [
        event for event in auto_trade_ctx.event_store.appended
        if event["event_type"] == "auto_trade.executed"
    ]
    assert executed_events[0]["payload"]["source_score_event_id"] == "score-window"


def test_run_submits_mx_paper_buy_when_dry_run_disabled(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-live-paper",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-live-paper",
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-window-paper-buy")

    assert result["dry_run"] is False
    assert result["buys"][0]["status"] == "filled"
    assert result["buys"][0]["order_id"] == "order-1"
    assert paper.buy_calls == [("002138", 2000)]
    executed_events = [
        event for event in auto_trade_ctx.event_store.appended
        if event["event_type"] == "auto_trade.executed"
    ]
    assert executed_events[0]["payload"]["dry_run"] is False
    assert executed_events[0]["payload"]["source_score_event_id"] == "score-live-paper"


def test_run_blocks_buy_when_default_profile_requires_manual_activation(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg.update({
        "continuation": {"filters": {"amount_min": 200000000}},
        "backtest_presets": {"aggressive_high_return": {"buy_threshold": 6.0}},
    })
    auto_trade_ctx.cfg["auto_trade"].update({
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-default-profile-run",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-default-profile-run",
                },
                "metadata": {},
            }
        ],
        profile_activation_events=[
            {
                "event_id": "activation-default-profile-run",
                "event_type": "strategy.profile_activation.requested",
                "occurred_at": (now - timedelta(minutes=5)).isoformat(),
                "payload": {
                    "status": "requires_manual_confirmation",
                    "current_profile": "default",
                    "target_profile": "trend_swing",
                    "activation": {"auto_apply": False},
                    "guardrails": {"manual_approval_required": True},
                },
                "metadata": {},
            }
        ],
    )

    result = run(auto_trade_ctx, "run-profile-review-required")

    assert result["buys"] == []
    assert paper.buy_calls == []
    assert result["diagnostics"][0]["reason"] == "profile_review_required"
    assert result["diagnostics"][0]["details"]["recommended_profile"] == "trend_swing"
    assert result["no_trade_summary"]["reason"] == "profile_review_required"
    assert "profile" in result["no_trade_summary"]["message"]
    executed_events = [
        event for event in auto_trade_ctx.event_store.appended
        if event["event_type"] == "auto_trade.executed"
    ]
    diagnostic_events = [
        event for event in auto_trade_ctx.event_store.appended
        if event["event_type"] == "auto_trade.diagnostic"
    ]
    assert executed_events == []
    assert diagnostic_events[0]["payload"]["reason"] == "profile_review_required"


def test_run_uses_latest_buy_decision_when_event_store_returns_ascending(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-old",
                "event_type": "decision.suggested",
                "occurred_at": (now - timedelta(minutes=20)).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 6.1,
                    "source_score_event_id": "score-old",
                },
                "metadata": {},
            },
            {
                "event_id": "decision-new",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-new",
                },
                "metadata": {},
            },
        ]
    )

    result = run(auto_trade_ctx, "run-latest-decision")

    assert result["buys"][0]["source_score_event_id"] == "score-new"
    assert result["buys"][0]["score"] == 7.5


def test_build_auto_trade_readiness_reports_paper_order_mode(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-readiness",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-readiness",
                    "entry_signal": True,
                    "primary_strategy_route": "flow_confirmed_trend",
                    "primary_strategy_route_label": "资金趋势确认",
                    "technical_detail": "金叉成立，资金确认",
                    "data_quality": "ok",
                },
                "metadata": {},
            }
        ]
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["mode"] == "mx_paper_order"
    assert payload["paper_order_submission_enabled"] is True
    assert payload["buy_side"]["status"] == "ready"
    assert payload["buy_side"]["top_signal"]["code"] == "002138"
    assert payload["buy_side"]["top_signal"]["entry_signal"] is True
    assert payload["buy_side"]["top_signal"]["primary_strategy_route_label"] == "资金趋势确认"
    assert payload["buy_side"]["top_signal"]["technical_detail"] == "金叉成立，资金确认"
    assert payload["paper_account"]["status"] == "ok"
    assert payload["next_action"]["command"] == "atrade run-pipeline auto_trade --json"
    assert payload["next_action"]["command_contract_id"] == "run_pipeline_auto_trade"
    assert payload["next_action"]["risk_level"] == "paper_order_execution"
    assert payload["next_action"]["writes_state"] is True
    assert payload["next_action"]["writes_order"] is True
    assert payload["next_action"]["requires_user_approval"] is True


def test_build_auto_trade_readiness_blocks_default_mixed_profile_before_auto_apply(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg.update({
        "continuation": {"filters": {"amount_min": 200000000}},
        "backtest_presets": {"aggressive_high_return": {"buy_threshold": 6.0}},
    })
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-default-profile",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-default-profile",
                },
                "metadata": {},
            }
        ]
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["status"] == "profile_review_required"
    assert payload["buy_side"]["ready"] is False
    assert payload["execution_profile"]["current_profile"] == "default"
    assert payload["execution_profile"]["status"] == "review_required"
    assert payload["execution_profile"]["recommended_profile"] == "trend_swing"
    assert {item["reason"] for item in payload["blockers"]} == {"profile_review_required"}
    assert payload["next_action"] == {
        "type": "confirm_strategy_profile",
        "label": "人工确认执行 profile",
        "command": "确认后设置 ASTOCK_CONFIG_PROFILE=trend_swing 再运行 atrade paper auto-readiness --json",
        "safe_to_auto_apply": False,
    }


def test_build_auto_trade_readiness_treats_recovered_failure_across_local_midnight_as_ok(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    current_time = datetime(2026, 5, 23, 0, 30, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: current_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    auto_trade_ctx.run_journal = FakeRunJournal(
        failed_runs=[
            {
                "run_id": "run_auto_trade_20260522_142240_aa6612",
                "run_type": "auto_trade",
                "status": "failed",
                "started_at": "2026-05-22T06:22:40.012852+00:00",
                "error_message": "stale running cleaned up after 0h",
            }
        ],
        successful_runs=[
            {
                "run_id": "run_auto_trade_20260522_220258_15a5e5",
                "run_type": "auto_trade",
                "status": "completed",
                "started_at": "2026-05-22T14:02:58.053742+00:00",
            }
        ],
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=False,
    )

    assert payload["new_trade_guard"]["status"] == "ok"
    assert payload["new_trade_guard"]["allow_new_trades"] is True
    assert payload["new_trade_guard"]["blockers"] == []
    assert "new_trade_guard_blocked" not in {item["reason"] for item in payload["blockers"]}


def test_build_auto_trade_readiness_surfaces_recorded_profile_activation_request(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg.update({
        "continuation": {"filters": {"amount_min": 200000000}},
        "backtest_presets": {"aggressive_high_return": {"buy_threshold": 6.0}},
    })
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-default-profile",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-default-profile",
                },
                "metadata": {},
            }
        ],
        profile_activation_events=[
            {
                "event_id": "activation-trend-swing",
                "event_type": "strategy.profile_activation.requested",
                "occurred_at": (now - timedelta(minutes=5)).isoformat(),
                "payload": {
                    "status": "requires_manual_confirmation",
                    "current_profile": "default",
                    "target_profile": "trend_swing",
                    "activation": {
                        "auto_apply": False,
                        "manual_confirmation_required": True,
                        "export_command": "export ASTOCK_CONFIG_PROFILE=trend_swing",
                        "verify_command": (
                            "ASTOCK_CONFIG_PROFILE=trend_swing "
                            "atrade paper auto-readiness --json"
                        ),
                    },
                    "guardrails": {
                        "auto_apply": False,
                        "manual_approval_required": True,
                    },
                },
                "metadata": {},
            }
        ],
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    request = payload["execution_profile"]["latest_activation_request"]
    assert payload["status"] == "profile_review_required"
    assert payload["execution_profile"]["activation_request_status"] == "recorded"
    assert request["event_id"] == "activation-trend-swing"
    assert request["target_profile"] == "trend_swing"
    assert request["activation"]["auto_apply"] is False
    assert request["activation"]["verify_command"] == (
        "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json"
    )
    assert payload["next_action"] == {
        "type": "review_recorded_profile_activation",
        "label": "复核已记录的 profile 激活计划",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "command_contract_id": "strategy_profile_activation_review",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
    }


def test_build_auto_trade_readiness_prioritizes_recorded_profile_activation_when_window_closed(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 14, 31, tzinfo=MARKET_TZ)
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg.update({
        "continuation": {"filters": {"amount_min": 200000000}},
        "backtest_presets": {"aggressive_high_return": {"buy_threshold": 6.0}},
    })
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-window-profile",
                "event_type": "decision.suggested",
                "occurred_at": (trade_time - timedelta(minutes=5)).astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-window-profile",
                },
                "metadata": {},
            }
        ],
        profile_activation_events=[
            {
                "event_id": "activation-window-profile",
                "event_type": "strategy.profile_activation.requested",
                "occurred_at": (now - timedelta(minutes=5)).isoformat(),
                "payload": {
                    "status": "requires_manual_confirmation",
                    "current_profile": "default",
                    "target_profile": "trend_swing",
                    "activation": {"auto_apply": False},
                    "guardrails": {"manual_approval_required": True},
                },
                "metadata": {},
            }
        ],
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["status"] == "waiting_window"
    assert "已记录待人工确认的 trend_swing profile 激活计划" in payload["summary"]
    assert "当前不在模拟买入窗口" in payload["summary"]
    assert {item["reason"] for item in payload["blockers"]} == {
        "profile_review_required",
        "buy_window_closed",
    }
    assert payload["next_action"] == {
        "type": "review_recorded_profile_activation",
        "label": "复核已记录的 profile 激活计划",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "command_contract_id": "strategy_profile_activation_review",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
    }


def test_build_auto_trade_readiness_prioritizes_profile_review_over_generic_blocked(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 22, 0, tzinfo=MARKET_TZ)
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg.update({
        "continuation": {"filters": {"amount_min": 200000000}},
        "backtest_presets": {"aggressive_high_return": {"buy_threshold": 6.0}},
    })
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        profile_activation_events=[
            {
                "event_id": "activation-trend-swing",
                "event_type": "strategy.profile_activation.requested",
                "occurred_at": (now - timedelta(minutes=5)).isoformat(),
                "payload": {
                    "status": "requires_manual_confirmation",
                    "current_profile": "default",
                    "target_profile": "trend_swing",
                    "activation": {"auto_apply": False},
                    "guardrails": {"manual_approval_required": True},
                },
                "metadata": {},
            }
        ],
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=False,
    )

    assert payload["status"] == "profile_review_required"
    assert payload["buy_side"]["status"] == "blocked"
    assert {item["reason"] for item in payload["blockers"]} == {
        "profile_review_required",
        "buy_window_closed",
        "no_fresh_buy_signal",
    }
    assert payload["next_action"] == {
        "type": "review_recorded_profile_activation",
        "label": "复核已记录的 profile 激活计划",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "command_contract_id": "strategy_profile_activation_review",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
    }


def test_build_auto_trade_readiness_surfaces_current_entry_signal_gap(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 22, 0, tzinfo=MARKET_TZ)
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg.update({
        "continuation": {"filters": {"amount_min": 200000000}},
        "backtest_presets": {"aggressive_high_return": {"buy_threshold": 6.0}},
    })
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    EventStore(auto_trade_ctx.conn).append(
        "strategy:002138",
        "strategy",
        "score.calculated",
        {
            "code": "002138",
            "name": "双环传动",
            "total_score": 7.5,
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
        },
    )
    auto_trade_ctx.event_store = FakeEventStore(
        profile_activation_events=[
            {
                "event_id": "activation-trend-swing",
                "event_type": "strategy.profile_activation.requested",
                "occurred_at": (now - timedelta(minutes=5)).isoformat(),
                "payload": {
                    "status": "requires_manual_confirmation",
                    "current_profile": "default",
                    "target_profile": "trend_swing",
                    "activation": {"auto_apply": False},
                    "guardrails": {"manual_approval_required": True},
                },
                "metadata": {},
            }
        ],
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=False,
    )

    assert payload["status"] == "profile_review_required"
    assert payload["buy_side"]["status"] == "blocked"
    assert {item["reason"] for item in payload["buy_side"]["blockers"]} == {
        "buy_window_closed",
        "no_fresh_buy_signal",
    }
    assert payload["buy_side"]["current_entry_signals"][0] == {
        "code": "002138",
        "name": "双环传动",
        "pool_tier": "core",
        "pool_tier_label": "核心",
        "score": 7.5,
        "entry_signal": True,
        "primary_strategy_route": "flow_confirmed_trend",
        "primary_strategy_route_label": "资金趋势确认",
        "technical_detail": "金叉成立，资金确认",
        "data_quality": "ok",
        "review_command": "atrade stock analyze 002138 --json",
    }
    assert payload["buy_side"]["signal_gap"] == {
        "status": "entry_signal_without_fresh_buy_intent",
        "summary": (
            "当前核心候选已有入场信号，但没有同日新鲜买入意向；"
            "先复核单票，再等待下一次评分决策链路生成同日买入意向。"
        ),
        "next_action": {
            "type": "review_current_entry_signal",
            "label": "复核当前核心入场信号",
            "command": "atrade stock analyze 002138 --json",
            "safe_to_auto_apply": True,
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
            "command_contract_id": "stock_analyze",
        },
        "guardrails": {
            "entry_signal_is_buy_intent": False,
            "places_order": False,
            "requires_same_day_buy_decision": True,
        },
    }


def test_build_auto_trade_readiness_reports_waiting_window_with_buy_signal(
    auto_trade_ctx,
    monkeypatch,
):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 14, 31, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-after-window",
                "event_type": "decision.suggested",
                "occurred_at": (trade_time - timedelta(minutes=5)).astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-after-window",
                },
                "metadata": {},
            }
        ]
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["status"] == "waiting_window"
    assert payload["summary"] == (
        "已有新鲜买入意向 1 条，但当前不在模拟买入窗口；"
        "最高分为 双环传动(002138) 7.5 分，本轮不会提交模拟买入。"
    )
    assert payload["buy_side"]["status"] == "waiting_window"
    assert {item["reason"] for item in payload["blockers"]} == {"buy_window_closed"}
    assert payload["next_action"]["type"] == "inspect_blockers"
    assert payload["next_action"]["safe_to_auto_apply"] is False
    assert payload["next_action"]["command_contract_id"] == "paper_auto_readiness"
    assert payload["next_action"]["risk_level"] == "read_only"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False


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
    now = _set_trading_now(monkeypatch)
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
    auto_trade_ctx.conn.execute(
        """INSERT INTO projection_candidate_pool
           (code, pool_tier, name, score, added_at, last_scored_at)
           VALUES
           (?, ?, ?, ?, ?, ?),
           (?, ?, ?, ?, ?, ?)""",
        (
            "002384", "watch", "东山精密", 5.5, now.isoformat(), now.isoformat(),
            "300475", "radar", "香农芯创", 4.9, now.isoformat(), now.isoformat(),
        ),
    )

    result = run(auto_trade_ctx, "run-empty-core")

    assert result["buys"] == []
    assert result["diagnostics"][0]["reason"] == "core_pool_empty"
    assert result["diagnostics"][0]["details"]["watch_count"] == 1
    assert result["diagnostics"][0]["details"]["radar_count"] == 1
    assert result["no_trade_summary"]["reason"] == "core_pool_empty"
    assert result["no_trade_summary"]["message"] == "核心候选池为空；当前观察候选 1 只、强势观察 1 只，只跟踪不自动买入。"
    summary_events = [
        event for event in auto_trade_ctx.event_store.appended
        if event["event_type"] == "auto_trade.summary"
    ]
    assert summary_events[0]["payload"]["no_trade_summary"]["reason"] == "core_pool_empty"
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
    now = _set_trading_now(monkeypatch)
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


def test_run_does_not_repeat_existing_paper_buy_signal(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    now = _set_trading_now(monkeypatch)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-1",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-1",
                },
                "metadata": {},
            }
        ],
        auto_trade_events=[
            {
                "event_type": "auto_trade.executed",
                "occurred_at": now.isoformat(),
                "payload": {
                    "side": "buy",
                    "code": "002138",
                    "status": "dry_run",
                    "source_event_id": "decision-1",
                    "source_score_event_id": "score-1",
                },
                "metadata": {"account": "paper"},
            }
        ],
    )

    result = run(auto_trade_ctx, "run-duplicate-signal")

    assert result["buys"] == []
    assert paper.buy_calls == []


def test_run_explains_buy_candidate_price_unavailable(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 14, 20, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr(
        "astock_trading.reporting.discord_sender.send_embed",
        lambda *args, **kwargs: (True, ""),
    )
    auto_trade_ctx.market_svc = NoPriceMarketService()
    auto_trade_ctx.cfg["auto_trade"].update({
        "enabled": True,
        "dry_run": True,
        "buy_window": {"start": "09:45", "end": "14:30"},
    })
    now = trade_time.astimezone(timezone.utc)
    _seed_core_candidate(auto_trade_ctx.conn, scored_at=now - timedelta(hours=1))
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-no-price",
                "event_type": "decision.suggested",
                "occurred_at": now.isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "002138",
                    "name": "双环传动",
                    "score": 7.5,
                    "source_score_event_id": "score-no-price",
                },
                "metadata": {},
            }
        ]
    )

    result = run(auto_trade_ctx, "run-no-price")

    assert result["buys"] == []
    assert result["diagnostics"][0]["reason"] == "buy_candidate_price_unavailable"
    assert result["diagnostics"][0]["details"]["codes"] == ["002138"]
    assert result["no_trade_summary"]["reason"] == "buy_candidate_price_unavailable"
    assert result["no_trade_summary"]["message"] == "买入候选缺少有效价格，未提交模拟买入。"
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


def test_run_explains_fresh_buy_signal_after_buy_window_closed(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 14, 31, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.PaperAccount", lambda: paper)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-buy-late",
                "occurred_at": (trade_time - timedelta(minutes=5)).astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "688981",
                    "name": "中芯国际",
                    "score": 6.4,
                    "position_pct": 0.1,
                    "market_signal": "YELLOW",
                    "source_score_event_id": "score-buy-late",
                },
            }
        ]
    )

    result = run(auto_trade_ctx, "run-buy-window-closed")

    assert result["buys"] == []
    assert result["no_trade_summary"]["reason"] == "buy_window_closed_with_signal"
    assert result["no_trade_summary"]["details"]["pending_buy_signal"]["count"] == 1
    assert result["no_trade_summary"]["details"]["pending_buy_signal"]["top"]["code"] == "688981"


def test_buy_candidates_sort_by_route_policy_before_raw_score(auto_trade_ctx, monkeypatch):
    from astock_trading.pipeline import auto_trade as auto_trade_module

    now = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: now)
    monkeypatch.setattr(
        auto_trade_module,
        "_usable_buy_decision_events",
        lambda ctx, cfg, now, max_age_hours: [
            {
                "event_id": "decision-high-score",
                "occurred_at": now.astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "600000",
                    "name": "高分普通",
                    "score": 7.2,
                    "market_signal": "GREEN",
                    "primary_strategy_route": "relative_strength_overheat",
                },
            },
            {
                "event_id": "decision-route-priority",
                "occurred_at": now.astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "600001",
                    "name": "路线优先",
                    "score": 6.1,
                    "market_signal": "GREEN",
                    "primary_strategy_route": "volume_breakout",
                },
            },
        ],
    )
    monkeypatch.setattr(
        auto_trade_module,
        "_recent_paper_buy_keys",
        lambda ctx, since: {"codes": set(), "signal_ids": set()},
    )
    auto_trade_ctx.cfg["scoring"]["route_execution_policy"] = {
        "GREEN:volume_breakout": {"priority": 80, "position_pct": 0.22, "score_min": 6.0}
    }

    candidates = _get_buy_candidates(
        auto_trade_ctx,
        "run-route-sort",
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
        exposure_pct=0.0,
        weekly_buy_count=0,
        cfg=auto_trade_ctx.cfg["auto_trade"],
        max_age_hours=24,
    )

    assert [item["code"] for item in candidates] == ["600001", "600000"]


def test_score_and_buy_uses_route_policy_position_for_formal_buy(auto_trade_ctx, monkeypatch):
    from astock_trading.pipeline import auto_trade as auto_trade_module

    auto_trade_ctx.cfg["scoring"]["route_execution_policy"] = {
        "GREEN:volume_breakout": {"priority": 80, "position_pct": 0.11, "score_min": 6.0}
    }
    monkeypatch.setattr(auto_trade_module, "_buy_side_diagnostics", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        auto_trade_module,
        "_get_buy_candidates",
        lambda *args, **kwargs: [
            {
                "code": "600001",
                "name": "路线仓位",
                "score": 6.5,
                "price": 10.0,
                "market_signal": "GREEN",
                "primary_strategy_route": "volume_breakout",
                "source_event_id": "decision-route-position",
                "source_score_event_id": "score-route-position",
            }
        ],
    )

    buys = _score_and_buy(
        auto_trade_ctx,
        FakePaperAccount(),
        PaperBalance(total_asset=100_000, available_cash=100_000, market_value=0),
        exposure_pct=0.0,
        available_cash=100_000,
        market_state=MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
        run_id="run-route-position",
        cfg=auto_trade_ctx.cfg["auto_trade"],
        dry_run=True,
        max_trades=1,
    )

    assert buys[0]["position_pct"] == 0.11
    assert buys[0]["shares"] == 1100
    event_payload = auto_trade_ctx.event_store.appended[-1]["payload"]
    assert event_payload["primary_strategy_route"] == "volume_breakout"
    assert event_payload["primary_strategy_route_label"] == "放量突破"


def test_readiness_ignores_previous_day_buy_signal_even_within_max_age(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    trade_time = datetime(2026, 5, 22, 10, 0, tzinfo=MARKET_TZ)
    previous_day_signal = datetime(2026, 5, 21, 15, 25, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: trade_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    _seed_core_candidate(
        auto_trade_ctx.conn,
        scored_at=trade_time.astimezone(timezone.utc) - timedelta(hours=1),
    )
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-after-window-yesterday",
                "event_type": "decision.suggested",
                "occurred_at": previous_day_signal.astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "688981",
                    "name": "中芯国际",
                    "score": 6.4,
                    "source_score_event_id": "score-after-window-yesterday",
                },
                "metadata": {},
            }
        ]
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["fresh_buy_signal"]["count"] == 0
    assert payload["buy_side"]["status"] == "blocked"
    assert {item["reason"] for item in payload["buy_side"]["blockers"]} == {"no_fresh_buy_signal"}


def test_readiness_ignores_same_date_buy_signal_on_non_trading_day(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    weekend_time = datetime(2026, 5, 23, 10, 0, tzinfo=MARKET_TZ)
    weekend_signal = datetime(2026, 5, 23, 1, 44, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: weekend_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    _seed_core_candidate(
        auto_trade_ctx.conn,
        scored_at=weekend_time.astimezone(timezone.utc) - timedelta(hours=1),
    )
    source_score_event_id = EventStore(auto_trade_ctx.conn).append(
        "strategy:688981",
        "strategy",
        "score.calculated",
        {
            "code": "688981",
            "name": "中芯国际",
            "total_score": 6.4,
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
        },
        metadata={"run_id": "weekend-score"},
    )
    auto_trade_ctx.event_store = FakeEventStore(
        decision_events=[
            {
                "event_id": "decision-weekend-buy",
                "event_type": "decision.suggested",
                "occurred_at": weekend_signal.astimezone(timezone.utc).isoformat(),
                "payload": {
                    "action": "BUY",
                    "code": "688981",
                    "name": "中芯国际",
                    "score": 6.4,
                    "source_score_event_id": source_score_event_id,
                },
                "metadata": {},
            }
        ]
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["window_state"]["trading_day"] is False
    assert payload["fresh_buy_signal"]["count"] == 0
    assert "近期买入意向 1 条不可承接" in payload["summary"]
    assert "买入意向发生日或当前检查日不是交易日" in payload["summary"]
    assert payload["recent_unusable_buy_signal"] == {
        "count": 1,
        "max_age_hours": 24,
        "top": {
            "event_id": "decision-weekend-buy",
            "occurred_at": weekend_signal.astimezone(timezone.utc).isoformat(),
            "code": "688981",
            "name": "中芯国际",
            "score": 6.4,
            "position_pct": 0,
            "market_signal": "",
            "source_score_event_id": source_score_event_id,
            "entry_signal": True,
            "primary_strategy_route": "flow_confirmed_trend",
            "primary_strategy_route_label": "资金趋势确认",
            "technical_detail": "金叉成立，资金确认",
            "data_quality": "ok",
            "unusable_reason": "non_trading_day",
            "unusable_reason_label": "买入意向发生日或当前检查日不是交易日",
            "carries_to_current_window": False,
        },
    }
    assert payload["buy_side"]["status"] == "blocked"
    assert {item["reason"] for item in payload["buy_side"]["blockers"]} == {"buy_window_closed", "no_fresh_buy_signal"}


def test_readiness_labels_weekend_stale_pool_as_next_window_refresh(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    weekend_time = datetime(2026, 5, 23, 10, 0, tzinfo=MARKET_TZ)
    monkeypatch.setattr("astock_trading.pipeline.auto_trade.local_now", lambda: weekend_time)
    auto_trade_ctx.cfg["auto_trade"].update({
        "dry_run": False,
        "buy_window": {"start": "09:45", "end": "14:30"},
        "sell_window": {"start": "09:35", "end": "14:50"},
    })
    _seed_core_candidate(
        auto_trade_ctx.conn,
        scored_at=weekend_time.astimezone(timezone.utc) - timedelta(hours=25),
    )

    payload = build_auto_trade_readiness(
        auto_trade_ctx,
        paper_factory=lambda: paper,
        include_account=True,
    )

    assert payload["window_state"]["trading_day"] is False
    assert payload["candidate_pool"]["fresh"] is False
    assert payload["candidate_pool"]["freshness_status"] == "refresh_required_before_next_window"
    assert payload["candidate_pool"]["refresh_required_before_next_window"] is True
    assert {item["reason"] for item in payload["buy_side"]["blockers"]} == {
        "buy_window_closed",
        "candidate_refresh_required_before_next_window",
        "no_fresh_buy_signal",
    }
    assert "候选池评分已过期" not in payload["summary"]
    assert "下个买入窗口前需要重新刷新候选评分" in payload["summary"]


def test_run_pushes_discord_when_auto_trade_has_actions(auto_trade_ctx, monkeypatch):
    paper = FakePaperAccount()
    calls = []
    _set_trading_now(monkeypatch)
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
