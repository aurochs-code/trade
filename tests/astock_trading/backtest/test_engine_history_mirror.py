"""回测引擎优先读取历史信号镜像。"""

from __future__ import annotations

import pandas as pd

from astock_trading.backtest.engine import (
    BacktestConfig,
    BacktestEngine,
    _cache_covers_backtest_range,
    _cache_covers_end_date,
)
from astock_trading.platform.history_mirror import archive_signal_history
from astock_trading.strategy.models import Action, DecisionIntent, MarketSignal, MarketState, ScoreResult


def test_market_bar_cache_can_cover_backtest_without_warmup_prefix():
    cached = pd.DataFrame({"日期": ["2019-01-02", "2026-06-15"]})
    late_listing_cached = pd.DataFrame({"日期": ["2022-03-15", "2026-06-15"]})

    assert _cache_covers_backtest_range(cached, "2019-01-01", "2026-06-15") is True
    assert _cache_covers_backtest_range(cached, "2018-04-16", "2026-06-15") is False
    assert _cache_covers_backtest_range(late_listing_cached, "2019-01-01", "2026-06-15") is False
    assert _cache_covers_end_date(late_listing_cached, "2026-06-15") is True


def test_backtest_engine_uses_signal_history_before_proxy_replay(mysql_conn):
    conn = mysql_conn
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-01-05",
            history_group_id="hist_20260105_screener",
            run_id="screener_101500",
            phase="screener",
            market={"signal": "GREEN", "multiplier": 1.0, "detail": {"source": "test"}},
            candidates=[{"code": "600036", "name": "招商银行", "total_score": 7.2}],
            decisions=[{"code": "600036", "name": "招商银行", "action": "BUY", "score": 7.2}],
        )

        engine = BacktestEngine(BacktestConfig(), history_conn=conn)
        engine._bars = {
            "600036": pd.DataFrame({"日期": ["2026-01-05"], "收盘": [10.0]}),
        }
        fallback_market = MarketState(signal=MarketSignal.RED, multiplier=0.0)

        replay = engine._mirror_replay_for_date("2026-01-05", fallback_market)
    finally:
        conn.close()

    assert replay is not None
    assert replay["source"] == "history_mirror"
    assert replay["history_group_id"] == "hist_20260105_screener"
    assert replay["market"].signal == MarketSignal.GREEN
    score, intent = replay["intents"][0]
    assert score.code == "600036"
    assert score.total == 7.2
    assert intent.action.value == "BUY"


def test_pool_only_history_mirror_is_discovery_evidence_not_strategy_intents(monkeypatch):
    def fake_load_signal_history_bundle(conn, *, snapshot_date, history_group_id="", phases=("screener", "scoring")):
        assert "historical_discovery" in phases
        return {
            "history_group_id": "hist_20260105_discovery",
            "sections": {
                "market": {"signal": "GREEN", "multiplier": 1.0},
                "pool": [{"code": "600036", "name": "招商银行", "pool_tier": "historical_discovery"}],
                "candidates": [],
                "decision": [],
            },
        }

    monkeypatch.setattr(
        "astock_trading.platform.history_mirror.load_signal_history_bundle",
        fake_load_signal_history_bundle,
    )

    engine = BacktestEngine(BacktestConfig(), history_conn=object())
    engine._bars = {
        "600036": pd.DataFrame({"日期": ["2026-01-05"], "收盘": [10.0]}),
    }
    fallback_market = MarketState(signal=MarketSignal.RED, multiplier=0.0)

    replay = engine._mirror_replay_for_date("2026-01-05", fallback_market)

    assert replay is not None
    assert replay["source"] == "history_mirror"
    assert replay["history_group_id"] == "hist_20260105_discovery"
    assert replay["intents"] == []
    assert replay["has_strategy_intents"] is False
    assert replay["discovery_sources"] == {"600036": ["pool"]}


def test_discovery_only_history_mirror_preserves_fallback_market_detail(monkeypatch):
    def fake_load_signal_history_bundle(conn, *, snapshot_date, history_group_id="", phases=("screener", "scoring")):
        return {
            "history_group_id": "hist_20260105_discovery",
            "sections": {
                "market": {},
                "pool": [{"code": "600036", "pool_tier": "historical_discovery"}],
                "candidates": [],
                "decision": [],
            },
        }

    monkeypatch.setattr(
        "astock_trading.platform.history_mirror.load_signal_history_bundle",
        fake_load_signal_history_bundle,
    )

    engine = BacktestEngine(BacktestConfig(), history_conn=object())
    engine._bars = {
        "600036": pd.DataFrame({"日期": ["2026-01-05"], "收盘": [10.0]}),
    }
    fallback_market = MarketState(
        signal=MarketSignal.YELLOW,
        multiplier=0.5,
        detail={"price": 3200.0, "above_ma120": True, "index_ma20_slope_5d_pct": 1.2},
    )

    replay = engine._mirror_replay_for_date("2026-01-05", fallback_market)

    assert replay is not None
    assert replay["market"].signal == MarketSignal.YELLOW
    assert replay["market"].multiplier == 0.5
    assert replay["market"].detail["price"] == 3200.0
    assert replay["market"].detail["above_ma120"] is True
    assert replay["market"].detail["index_ma20_slope_5d_pct"] == 1.2


def test_backtest_engine_uses_preloaded_history_mirror_cache(monkeypatch):
    bulk_calls = []

    def fake_load_signal_history_bundles(conn, *, snapshot_dates, phases=("screener", "scoring")):
        bulk_calls.append({
            "conn": conn,
            "snapshot_dates": list(snapshot_dates),
            "phases": phases,
        })
        return {
            "2026-01-05": {
                "history_group_id": "hist_20260105_discovery",
                "sections": {
                    "market": {"signal": "GREEN", "multiplier": 1.0},
                    "pool": [{"code": "600036", "pool_tier": "historical_discovery"}],
                    "candidates": [],
                    "decision": [],
                },
            }
        }

    monkeypatch.setattr(
        "astock_trading.platform.history_mirror.load_signal_history_bundles",
        fake_load_signal_history_bundles,
    )
    monkeypatch.setattr(
        "astock_trading.platform.history_mirror.load_signal_history_bundle",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("应使用预加载缓存，不应逐日查询")),
    )

    history_conn = object()
    engine = BacktestEngine(BacktestConfig(), history_conn=history_conn)
    engine._bars = {
        "600036": pd.DataFrame({"日期": ["2026-01-05"], "收盘": [10.0]}),
    }
    engine._sorted_dates = ["2026-01-05"]
    fallback_market = MarketState(signal=MarketSignal.RED, multiplier=0.0)

    engine._preload_history_mirror_cache()
    replay = engine._mirror_replay_for_date("2026-01-05", fallback_market)

    assert bulk_calls == [
        {
                "conn": history_conn,
                "snapshot_dates": ["2026-01-05"],
                "phases": ("historical_discovery", "screener", "scoring"),
            }
        ]
    assert replay is not None
    assert replay["history_group_id"] == "hist_20260105_discovery"
    assert replay["discovery_sources"] == {"600036": ["pool"]}


def test_backtest_engine_skips_bulk_history_mirror_cache_for_long_period(monkeypatch):
    def fail_bulk_loader(*args, **kwargs):
        raise AssertionError("长周期回测不应使用大 IN 批量预载历史镜像")

    monkeypatch.setattr(
        "astock_trading.platform.history_mirror.load_signal_history_bundles",
        fail_bulk_loader,
    )

    engine = BacktestEngine(BacktestConfig(), history_conn=object())
    engine._sorted_dates = [f"2026-01-{(idx % 28) + 1:02d}" for idx in range(251)]

    engine._preload_history_mirror_cache()

    assert engine._history_mirror_cache is None


def test_reachable_only_allows_codes_seen_in_signal_history(mysql_conn):
    conn = mysql_conn
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-01-05",
            history_group_id="hist_20260105_screener",
            run_id="screener_101500",
            phase="screener",
            market={"signal": "GREEN", "multiplier": 1.0, "detail": {"source": "test"}},
            pool=[{"code": "600036", "name": "招商银行", "pool_tier": "core"}],
            candidates=[{"code": "600036", "name": "招商银行", "total_score": 7.2}],
            decisions=[{
                "code": "600036",
                "name": "招商银行",
                "action": "BUY",
                "score": 7.2,
                "position_pct": 0.2,
            }],
        )

        engine = BacktestEngine(
            BacktestConfig(
                initial_cash=100000.0,
                require_reachable_candidate_for_buy=True,
                reachable_lookback_days=0,
            ),
            history_conn=conn,
        )
        engine._bars = {
            "600036": pd.DataFrame({"日期": ["2026-01-05"], "收盘": [10.0]}),
        }
        engine._sorted_dates = ["2026-01-05"]

        report = engine.run()
    finally:
        conn.close()

    assert report["buy_trades"] == 1
    reachability = report["discovery_reachability"]
    assert reachability["enabled"] is True
    assert reachability["candidate_checks"] == 1
    assert reachability["reachable_candidates"] == 1
    assert reachability["blocked_candidates"] == 0
    assert reachability["reachable_buy_rate_pct"] == 100.0
    assert reachability["discovery_sources"]["pool"] == 1


def test_reachable_only_blocks_buy_intent_not_seen_in_signal_history():
    engine = BacktestEngine(
        BacktestConfig(
            require_reachable_candidate_for_buy=True,
            reachable_lookback_days=0,
        )
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)
    score = ScoreResult(code="300750", name="宁德时代", total=7.5)
    intent = DecisionIntent(
        code="300750",
        name="宁德时代",
        action=Action.BUY,
        confidence=0.9,
        score=7.5,
        position_pct=0.2,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    status = engine._reachability_status("2026-01-05", score.code)
    if not status["reachable"]:
        engine._record_execution_funnel_skip("not_discovered_by_screener", score, intent, market)

    assert status == {
        "reachable": False,
        "reason": "not_discovered_by_screener",
        "sources": [],
        "last_seen_date": "",
    }
    assert engine._execution_funnel["skip_reasons"]["not_discovered_by_screener"] == 1


def test_reachability_queries_history_discovery_on_demand():
    class FakeResult:
        def fetchall(self):
            return [
                {"snapshot_date": "2026-01-04", "source": "pool"},
                {"snapshot_date": "2026-01-04", "source": "candidates"},
            ]

    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=()):
            self.calls.append((sql, params))
            return FakeResult()

    conn = FakeConn()
    engine = BacktestEngine(
        BacktestConfig(
            require_reachable_candidate_for_buy=True,
            reachable_lookback_days=5,
        ),
        history_conn=conn,
    )

    status = engine._reachability_status("2026-01-05", "600036")

    assert status == {
        "reachable": True,
        "reason": "discovered_by_screener",
        "sources": ["candidates", "pool"],
        "last_seen_date": "2026-01-04",
    }
    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert "FROM signal_history_discoveries" in sql
    assert "payload_json LIKE" not in sql
    assert params == ("600036", "2025-12-31", "2026-01-05")
