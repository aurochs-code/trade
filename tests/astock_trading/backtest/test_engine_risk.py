"""Backtest risk management behavior."""

from pathlib import Path

import pandas as pd
import yaml

import astock_trading.market.adapters as market_adapters
import astock_trading.backtest.engine as backtest_engine_module
from astock_trading.backtest.engine import (
    BacktestConfig,
    BacktestEngine,
    Position,
    _date_ranges,
    _financial_periods,
    _score_weights_for_mode,
    load_config,
    run_backtest,
)
from astock_trading.market.store import MarketStore
from astock_trading.platform.db import connect, init_db
from astock_trading.strategy.models import Action, DecisionIntent, MarketSignal, MarketState, ScoreResult


def _strategy_config(path: str) -> dict:
    repo_root = Path(__file__).resolve().parents[3]
    return yaml.safe_load((repo_root / path).read_text(encoding="utf-8"))


def test_risk_check_triggers_trailing_stop_from_high_watermark():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            stop_loss=0.08,
            trailing_stop=0.10,
            time_stop_days=30,
        )
    )
    dates = ["2026-01-02", "2026-01-05", "2026-01-06"]
    engine._sorted_dates = dates
    engine._cash = 90000.0
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 12.0, 10.5],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=1000,
            entry_price=10.0,
            entry_date=dates[0],
            high_water=12.0,
        )
    }

    engine._risk_check(dates[2], 2)

    assert "002138" not in engine._positions
    assert engine._trades[-1]["reason"] == "追踪止损"
    assert engine._trades[-1]["return_pct"] == 5.0


def test_runtime_base_config_matches_final_route_candidate_risk_profile():
    for config_path in (
        "config/strategy.yaml",
        "src/astock_trading/templates/config/strategy.yaml",
    ):
        cfg = _strategy_config(config_path)
        final_candidate = cfg["backtest_presets"]["攻_C_route_policy_v3_weekly5_trail18"]
        momentum = cfg["risk"]["momentum"]
        position = cfg["risk"]["position"]

        assert momentum["trailing_stop"] == final_candidate["momentum_trailing_stop"] == 0.18
        assert momentum["time_stop_days"] == final_candidate["momentum_time_stop_days"] == 30
        assert position["single_max"] == final_candidate["single_max_pct"] == 0.22
        assert position["total_max"] == final_candidate["total_max_pct"] == 0.67
        assert position["weekly_max"] == final_candidate["weekly_max"] == 5
        assert position["holding_max"] == final_candidate["holding_max"] == 5


def test_backtest_buy_uses_decision_position_pct():
    engine = BacktestEngine(BacktestConfig(single_max_pct=0.20))
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.BUY,
        confidence=7.0,
        score=7.0,
        position_pct=0.10,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )

    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.YELLOW, multiplier=0.5),
    ) == 0.10


def test_backtest_can_counterfactually_execute_red_trial_buy_at_small_size():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_trial_buy_market_signals=("RED",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
    )

    assert engine._intent_executable_for_backtest(intent, "002138") is True
    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.RED, multiplier=0.3),
    ) == 0.06


def test_backtest_can_filter_counterfactual_trial_buy_by_route():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_trial_buy_market_signals=("RED",),
            execute_trial_buy_routes=("pullback_to_ma20",),
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
    )

    assert engine._intent_executable_for_backtest(intent, "002138", "pullback_to_ma20") is True
    assert engine._intent_executable_for_backtest(intent, "002138", "trend_cooling_off") is False


def test_backtest_can_counterfactually_execute_watch_route_with_score_floor():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_watch_trial_market_signals=("GREEN",),
            execute_watch_trial_routes=("relative_strength_overheat",),
            execute_watch_trial_score_min=6.0,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "relative_strength_overheat",
        score_total=6.2,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "relative_strength_overheat",
        score_total=5.9,
    ) is False
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "pullback_to_ma20",
        score_total=6.2,
    ) is False
    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == 0.20


def test_backtest_can_filter_counterfactual_watch_route_by_market_route_pair():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.20,
            execute_watch_trial_pairs=("GREEN:relative_strength_overheat",),
            execute_watch_trial_score_min=5.0,
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=5.3,
        score=5.3,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "relative_strength_overheat",
        score_total=5.3,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "pullback_to_ma20",
        score_total=5.3,
    ) is False
    assert engine._execution_position_pct(
        intent,
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    ) == 0.20


def test_score_weights_for_tech_fundamental_mode_rescales_to_full_score_budget():
    weights = _score_weights_for_mode(
        {"technical": 3.0, "fundamental": 2.0, "flow": 2.0, "sentiment": 3.0},
        "tech_fundamental",
    )

    assert weights == {
        "technical": 6.0,
        "fundamental": 4.0,
        "flow": 0.0,
        "sentiment": 0.0,
    }


def test_backtest_config_loads_strategy_overlays_and_score_adjustments():
    cfg = load_config("aggressive_score_capture")

    assert cfg.market_regime_overlays["YELLOW"]["allow_trial_buy"] is False
    assert cfg.market_regime_overlays["YELLOW"]["buy_threshold"] == 6.5
    assert cfg.market_regime_overlays["RED"]["allow_trial_buy"] is True
    assert cfg.score_adjustments["tech_flow_correlation"]["enabled"] is True


def test_backtest_config_loads_position_limits_from_preset():
    cfg = load_config("攻_C_route_policy_v3_weekly5")

    assert cfg.single_max_pct == 0.22
    assert cfg.total_max_pct == 0.67
    assert cfg.weekly_max == 5


def test_backtest_config_keeps_weekly5_without_market_specific_override():
    cfg = load_config("攻_C_route_policy_v3_weekly5")

    assert cfg.weekly_max == 5
    assert cfg.weekly_max_by_market == {}


def test_backtest_config_loads_route_execution_policy_from_preset():
    cfg = load_config("攻_C_route_policy_v3")

    assert cfg.route_execution_policy["YELLOW:shrink_pullback"]["score_min"] == 6.0
    assert cfg.route_execution_policy["YELLOW:shrink_pullback"]["position_pct"] == 0.11
    assert cfg.route_execution_policy["YELLOW:shrink_pullback"]["priority"] == 35
    assert cfg.daily_max_buys == 3
    assert cfg.holding_max == 5


def test_backtest_config_loads_route_execution_policy_v3_from_preset():
    cfg = load_config("攻_C_route_policy_v3")

    assert cfg.route_execution_policy["GREEN:dragon_head"]["priority"] == 85
    assert cfg.route_execution_policy["RED:volume_breakout"]["position_pct"] == 0.066
    assert "RED:relative_strength_overheat" not in cfg.route_execution_policy


def test_backtest_config_loads_route_policy_v3_weekly5_trail18_from_preset():
    cfg = load_config("攻_C_route_policy_v3_weekly5_trail18")

    assert cfg.weekly_max == 5
    assert cfg.daily_max_buys == 3
    assert cfg.holding_max == 5
    assert cfg.trailing_stop == 0.18
    assert cfg.time_stop_days == 30
    assert cfg.route_execution_policy["GREEN:dragon_head"]["priority"] == 85
    assert cfg.route_execution_policy["RED:volume_breakout"]["position_pct"] == 0.066


def test_run_backtest_can_override_trailing_stop_without_new_preset(monkeypatch):
    captured = {}

    class FakeEngine:
        def __init__(self, cfg, history_conn=None, market_conn=None):
            captured["cfg"] = cfg

        def load_data(self, code_list, start, end, pre_start):
            return {}

        def run(self):
            return {"trailing_stop": captured["cfg"].trailing_stop}

    monkeypatch.setattr(backtest_engine_module, "BacktestEngine", FakeEngine)
    monkeypatch.setattr(backtest_engine_module, "_open_history_connection", lambda *args, **kwargs: None)
    monkeypatch.setattr(backtest_engine_module, "_open_market_data_connection", lambda **kwargs: None)

    result = run_backtest(
        "002138",
        "2025-01-01",
        "2025-01-31",
        preset="攻_C_route_policy_v3",
        use_history_mirror=False,
        trailing_stop=0.18,
    )

    assert result["trailing_stop"] == 0.18


def test_backtest_uses_market_specific_weekly_limit_when_present():
    engine = BacktestEngine(
        BacktestConfig(
            weekly_max=2,
            weekly_max_by_market={"GREEN": 4},
        )
    )

    assert engine._weekly_max_for_market(MarketState(signal=MarketSignal.GREEN, multiplier=1.0)) == 4
    assert engine._weekly_max_for_market(MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)) == 2


def test_backtest_route_policy_can_execute_watch_pair_with_own_score_floor_and_size():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "YELLOW:shrink_pullback": {
                    "score_min": 5.8,
                    "position_pct": 0.11,
                    "priority": 35,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=5.9,
        score=5.9,
        position_pct=0.0,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )
    market = MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)

    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.9,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "shrink_pullback",
        score_total=5.7,
    ) is False
    assert engine._intent_executable_for_backtest(
        intent,
        "002138",
        "short_continuation",
        score_total=6.5,
    ) is False
    assert engine._execution_position_pct(
        intent,
        market,
        route="shrink_pullback",
    ) == 0.11


def test_backtest_route_policy_can_execute_trial_buy_with_own_score_floor_and_size():
    engine = BacktestEngine(
        BacktestConfig(
            single_max_pct=0.22,
            route_execution_policy={
                "GREEN:relative_strength_overheat": {
                    "actions": ["TRIAL_BUY"],
                    "score_min": 6.2,
                    "position_pct": 0.11,
                    "priority": 45,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )
    market = MarketState(signal=MarketSignal.GREEN, multiplier=1.0)

    assert engine._intent_executable_for_backtest(
        intent,
        "300475",
        "relative_strength_overheat",
        score_total=6.3,
    ) is True
    assert engine._intent_executable_for_backtest(
        intent,
        "300475",
        "relative_strength_overheat",
        score_total=6.1,
    ) is False
    assert engine._execution_position_pct(
        intent,
        market,
        route="relative_strength_overheat",
    ) == 0.11


def test_backtest_route_policy_defaults_to_watch_only_for_trial_buy():
    engine = BacktestEngine(
        BacktestConfig(
            route_execution_policy={
                "GREEN:relative_strength_overheat": {
                    "score_min": 6.2,
                    "position_pct": 0.11,
                    "priority": 45,
                }
            },
        )
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.TRIAL_BUY,
        confidence=6.3,
        score=6.3,
        position_pct=0.0,
        market_signal=MarketSignal.GREEN,
        market_multiplier=1.0,
    )

    assert engine._intent_executable_for_backtest(
        intent,
        "300475",
        "relative_strength_overheat",
        score_total=6.3,
    ) is False


def test_backtest_route_policy_priority_can_outrank_raw_score():
    engine = BacktestEngine(
        BacktestConfig(
            route_execution_policy={
                "YELLOW:shrink_pullback": {"score_min": 6.0, "priority": 35},
                "YELLOW:short_continuation": {"score_min": 6.0, "priority": 10},
            },
        )
    )
    market = MarketState(signal=MarketSignal.YELLOW, multiplier=0.5)
    shrink_intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=6.0,
        score=6.0,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )
    short_intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.WATCH,
        confidence=7.0,
        score=7.0,
        market_signal=MarketSignal.YELLOW,
        market_multiplier=0.5,
    )

    assert engine._buy_candidate_sort_key(
        shrink_intent,
        "shrink_pullback",
        market,
        score_total=6.0,
    ) > engine._buy_candidate_sort_key(
        short_intent,
        "short_continuation",
        market,
        score_total=7.0,
    )


def test_backtest_execution_funnel_reports_zero_position_skip_by_market_route():
    engine = BacktestEngine(BacktestConfig())
    market = MarketState(signal=MarketSignal.RED, multiplier=0.0)
    score = ScoreResult(
        code="300475",
        name="香农芯创",
        total=6.7,
        entry_signal=False,
        primary_strategy_route="relative_strength_overheat",
    )
    intent = DecisionIntent(
        code="300475",
        name="香农芯创",
        action=Action.WATCH,
        confidence=6.7,
        score=6.7,
        position_pct=0.0,
        market_signal=MarketSignal.RED,
        market_multiplier=0.0,
    )

    engine._record_execution_funnel_intents("2026-01-02", [(score, intent)], market)
    engine._record_execution_funnel_skip("zero_position_pct", score, intent, market)

    report = engine._build_report()

    assert report["execution_funnel"]["signals_total"] == 1
    assert report["execution_funnel"]["actions"]["WATCH"] == 1
    assert report["execution_funnel"]["skip_reasons"]["zero_position_pct"] == 1
    assert (
        report["execution_funnel"]["by_market_route"]["RED:relative_strength_overheat"]["skip_reasons"][
            "zero_position_pct"
        ]
        == 1
    )


def test_backtest_report_can_skip_signal_alpha_summary_for_fast_execution_checks():
    engine = BacktestEngine(BacktestConfig(include_signal_alpha=False))
    engine._signal_records = [{"code": "300475", "primary_strategy_route": "relative_strength_overheat"}]

    report = engine._build_report()

    assert report["signal_alpha"] == {"skipped": True, "sample_size": 1}
    assert report["signal_validation"]["sample_size"] == 1
    assert "execution_funnel" in report


def test_backtest_allocation_respects_total_position_limit():
    engine = BacktestEngine(
        BacktestConfig(
            initial_cash=100000.0,
            single_max_pct=0.20,
            total_max_pct=0.30,
        )
    )
    trade_date = "2026-01-02"
    engine._cash = 75000.0
    engine._sorted_dates = [trade_date]
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": [trade_date],
            "收盘": [10.0],
        })
    }
    engine._positions = {
        "002138": Position(
            code="002138",
            shares=2500,
            entry_price=10.0,
            entry_date=trade_date,
            high_water=10.0,
        )
    }

    assert engine._allocation_budget_for_position(trade_date, 0.20) == 5000.0


def test_backtest_load_data_can_skip_slow_financial_fetch(monkeypatch):
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    kline = pd.DataFrame({
        "日期": dates,
        "开盘": [10.0, 10.2, 10.3],
        "最高": [10.5, 10.6, 10.7],
        "最低": [9.8, 10.0, 10.1],
        "收盘": [10.2, 10.3, 10.4],
        "成交量": [1000000, 1100000, 1200000],
        "成交额": [10000000, 11000000, 12000000],
        "涨跌幅": [1.0, 0.98, 0.97],
        "证券名称": ["样本"] * 3,
        "名称": ["样本"] * 3,
    })

    class FakeBaoStockMarketAdapter:
        async def get_kline(self, *args, **kwargs):
            return kline.copy()

    monkeypatch.setattr(
        market_adapters,
        "BaoStockMarketAdapter",
        lambda: FakeBaoStockMarketAdapter(),
    )
    engine = BacktestEngine(BacktestConfig(load_financials=False))
    called = False

    def fail_if_called(codes, end_date):
        nonlocal called
        called = True

    monkeypatch.setattr(engine, "_load_financials", fail_if_called)

    result = engine.load_data(["600036"], "2024-01-02", "2024-01-04", "2023-10-01")

    assert result["loaded"] == 1
    assert called is False


def test_backtest_progress_log_writes_to_stderr(capsys):
    engine = BacktestEngine(BacktestConfig(progress_log=True))

    engine._log_progress("kline_start", code="600036", batch="1/2")

    captured = capsys.readouterr()
    assert "backtest_progress" in captured.err
    assert "kline_start" in captured.err
    assert "600036" in captured.err
    assert captured.out == ""


def test_backtest_date_ranges_cover_requested_end_date():
    ranges = _date_ranges("2023-10-03", "2025-12-31", months_per_batch=6)

    assert ranges[0][0] == "2023-10-03"
    assert ranges[-1][1] == "2025-12-31"
    assert len(ranges) == 5
    assert all(prev[1] == nxt[0] for prev, nxt in zip(ranges, ranges[1:]))


def test_financial_periods_only_include_reports_available_by_end_date():
    periods = _financial_periods("2024-01-01", "2025-12-31")

    assert (2025, 3) in periods
    assert (2025, 4) not in periods


def test_backtest_load_data_fetches_final_date_range_with_no_row_cap(monkeypatch):
    calls: list[dict] = []
    kline = pd.DataFrame({
        "日期": ["2024-01-02", "2025-12-31"],
        "开盘": [10.0, 10.2],
        "最高": [10.5, 10.6],
        "最低": [9.8, 10.0],
        "收盘": [10.2, 10.3],
        "成交量": [1000000, 1100000],
        "成交额": [10000000, 11000000],
        "涨跌幅": [1.0, 0.98],
        "证券名称": ["样本", "样本"],
        "名称": ["样本", "样本"],
    })

    class FakeBaoStockMarketAdapter:
        async def get_kline(self, code, **kwargs):
            calls.append({"code": code, **kwargs})
            return kline.copy()

    monkeypatch.setattr(
        market_adapters,
        "BaoStockMarketAdapter",
        lambda: FakeBaoStockMarketAdapter(),
    )
    engine = BacktestEngine(BacktestConfig(load_financials=False))

    result = engine.load_data(["600036"], "2024-01-01", "2025-12-31", "2023-10-03")

    assert result["loaded"] == 1
    stock_calls = [item for item in calls if item["code"] == "600036"]
    assert stock_calls[-1]["end_date"] == "2025-12-31"
    assert all(item["count"] == 0 for item in stock_calls)


def test_backtest_load_data_uses_market_bars_cache_when_available(tmp_path, monkeypatch):
    db_path = tmp_path / "astock_trading.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        cached = pd.DataFrame({
            "日期": ["2023-10-04", "2024-01-02", "2025-12-31"],
            "开盘": [10.0, 10.1, 10.2],
            "最高": [10.5, 10.6, 10.7],
            "最低": [9.8, 9.9, 10.0],
            "收盘": [10.2, 10.3, 10.4],
            "成交量": [1000000, 1100000, 1200000],
            "成交额": [10000000, 11000000, 12000000],
        })
        index = cached.assign(收盘=[3000.0, 3010.0, 3020.0])
        store.save_price_bars("600036", cached, source="test", adjustflag="2")
        store.save_price_bars("000001", index, source="test", adjustflag="2")

        class FailingBaoStockMarketAdapter:
            async def get_kline(self, *args, **kwargs):
                raise AssertionError("cache hit should not call baostock")

        monkeypatch.setattr(
            market_adapters,
            "BaoStockMarketAdapter",
            lambda: FailingBaoStockMarketAdapter(),
        )
        engine = BacktestEngine(
            BacktestConfig(use_market_bars=True, load_financials=False),
            market_conn=conn,
        )

        result = engine.load_data(["600036"], "2024-01-01", "2025-12-31", "2023-10-03")

        assert result["loaded"] == 1
        assert engine._bars["600036"]["日期"].iloc[-1] == "2025-12-31"
    finally:
        conn.close()


def test_backtest_load_data_hydrates_market_bars_after_remote_fetch(tmp_path, monkeypatch):
    db_path = tmp_path / "astock_trading.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        kline = pd.DataFrame({
            "日期": ["2024-01-02", "2025-12-31"],
            "开盘": [10.0, 10.2],
            "最高": [10.5, 10.6],
            "最低": [9.8, 10.0],
            "收盘": [10.2, 10.3],
            "成交量": [1000000, 1100000],
            "成交额": [10000000, 11000000],
            "涨跌幅": [1.0, 0.98],
            "证券名称": ["样本", "样本"],
            "名称": ["样本", "样本"],
        })

        class FakeBaoStockMarketAdapter:
            async def get_kline(self, *args, **kwargs):
                return kline.copy()

        monkeypatch.setattr(
            market_adapters,
            "BaoStockMarketAdapter",
            lambda: FakeBaoStockMarketAdapter(),
        )
        engine = BacktestEngine(
            BacktestConfig(hydrate_market_bars=True, load_financials=False),
            market_conn=conn,
        )

        result = engine.load_data(["600036"], "2024-01-01", "2025-12-31", "2023-10-03")
        stored = MarketStore(conn).get_price_bars(
            "600036",
            start="2024-01-01",
            end="2025-12-31",
            adjustflag="2",
        )

        assert result["loaded"] == 1
        assert not stored.empty
        assert stored["日期"].iloc[-1] == "2025-12-31"
    finally:
        conn.close()


def test_backtest_load_financials_uses_cached_snapshot(tmp_path):
    db_path = tmp_path / "astock_trading.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_financial_snapshot(
            "600036",
            report_year=2025,
            report_quarter=4,
            report_date="2025-12-31",
            available_date="2026-04-30",
            payload={
                "roe": 12.3,
                "roe_3y_ago": 6.1,
                "revenue_growth": 8.8,
                "operating_cash_flow": 0.2,
            },
            source="baostock",
        )
        engine = BacktestEngine(
            BacktestConfig(use_financial_cache=True),
            market_conn=conn,
        )

        engine._load_financials(["600036"], "2025-01-01", "2026-06-01")

        fin = engine._financial_for_date("600036", "2026-05-01")
        assert fin["roe"] == 12.3
        assert fin["roe_3y_ago"] == 6.1
    finally:
        conn.close()
