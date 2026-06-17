"""信号 Alpha 与优化前后对比。"""

import json

import pandas as pd

from astock_trading.backtest.engine import BacktestConfig, BacktestEngine
from astock_trading.backtest.signal_analysis import (
    compare_backtest_signal_reports,
    signal_alpha_summary,
)
from astock_trading.strategy.models import (
    Action,
    DecisionIntent,
    DimensionScore,
    MarketSignal,
    MarketState,
    ScoreResult,
)


def test_signal_alpha_summary_groups_by_route_and_market_regime():
    signals = [
        {
            "code": "002384",
            "signal_date": "2026-01-02",
            "action": "TRIAL_BUY",
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "forward_returns": {"5d": 0.04, "10d": 0.07},
        },
        {
            "code": "688498",
            "signal_date": "2026-01-03",
            "action": "TRIAL_BUY",
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "forward_returns": {"5d": -0.03, "10d": -0.02},
        },
        {
            "code": "300750",
            "signal_date": "2026-01-04",
            "action": "WATCH",
            "primary_strategy_route": "pullback_to_ma20",
            "market_signal": "YELLOW",
            "forward_returns": {"5d": 0.01, "10d": 0.03},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("5d", "10d"))

    cooling = report["by_route"]["trend_cooling_off"]
    assert cooling["sample_size"] == 2
    assert cooling["horizons"]["5d"]["win_rate_pct"] == 50.0
    assert cooling["horizons"]["5d"]["avg_return_pct"] == 0.5
    green = report["by_market_signal"]["GREEN"]
    assert green["sample_size"] == 2
    assert report["overall"]["sample_size"] == 3


def test_signal_alpha_summary_includes_distribution_bootstrap_and_market_route_slice():
    signals = [
        {
            "code": "A",
            "primary_strategy_route": "volume_breakout",
            "market_signal": "RED",
            "forward_returns": {"20d": 0.12},
        },
        {
            "code": "B",
            "primary_strategy_route": "volume_breakout",
            "market_signal": "RED",
            "forward_returns": {"20d": -0.03},
        },
        {
            "code": "C",
            "primary_strategy_route": "pullback_to_ma20",
            "market_signal": "GREEN",
            "forward_returns": {"20d": 0.04},
        },
        {
            "code": "D",
            "primary_strategy_route": "pullback_to_ma20",
            "market_signal": "GREEN",
            "forward_returns": {"20d": 0.01},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("20d",), bootstrap_iterations=120, bootstrap_seed=7)

    horizon = report["overall"]["horizons"]["20d"]
    assert horizon["median_return_pct"] == 2.5
    assert horizon["p25_return_pct"] == 0.0
    assert horizon["p75_return_pct"] == 6.0
    assert horizon["max_loss_pct"] == 3.0
    assert horizon["avg_return_ci_low_pct"] <= horizon["avg_return_pct"]
    assert horizon["avg_return_ci_high_pct"] >= horizon["avg_return_pct"]
    assert horizon["bootstrap_iterations"] == 120
    assert report["by_market_route"]["RED"]["volume_breakout"]["sample_size"] == 2
    assert report["by_market_route"]["GREEN"]["pullback_to_ma20"]["sample_size"] == 2
    concentration = report["by_market_route"]["RED"]["volume_breakout"]["concentration"]
    assert concentration["unique_codes"] == 2
    assert concentration["top_code_sample_pct"] == 50.0


def test_signal_alpha_summary_splits_unknown_route_by_bucket_and_market():
    signals = [
        {
            "code": "A",
            "primary_strategy_route": "unknown",
            "unknown_bucket": "near_pullback_missing_confirm",
            "market_signal": "RED",
            "forward_returns": {"20d": 0.08},
        },
        {
            "code": "B",
            "primary_strategy_route": "unknown",
            "unknown_bucket": "score_only_no_route",
            "market_signal": "RED",
            "forward_returns": {"20d": -0.02},
        },
        {
            "code": "C",
            "primary_strategy_route": "pullback_to_ma20",
            "market_signal": "RED",
            "forward_returns": {"20d": 0.04},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("20d",), bootstrap_iterations=40)

    assert report["by_unknown_bucket"]["near_pullback_missing_confirm"]["sample_size"] == 1
    assert report["by_unknown_bucket"]["score_only_no_route"]["sample_size"] == 1
    assert "pullback_to_ma20" not in report["by_unknown_bucket"]
    assert report["by_market_unknown_bucket"]["RED"]["near_pullback_missing_confirm"]["sample_size"] == 1


def test_signal_alpha_summary_groups_by_decision_and_veto_reasons():
    signals = [
        {
            "code": "A",
            "signal_date": "2025-01-01",
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "RED",
            "decision_reasons": ["veto", "market_blocks_new_positions"],
            "veto_reasons": ["below_ma20"],
            "forward_returns": {"20d": 0.06},
        },
        {
            "code": "B",
            "signal_date": "2025-01-02",
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "RED",
            "decision_reasons": ["veto"],
            "veto_reasons": ["below_ma20", "ma20_trend_down"],
            "forward_returns": {"20d": -0.02},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("20d",), bootstrap_iterations=40)

    below_ma20 = report["by_veto_reason"]["below_ma20"]
    assert below_ma20["sample_size"] == 2
    assert below_ma20["horizons"]["20d"]["avg_return_pct"] == 2.0
    assert report["by_veto_reason"]["ma20_trend_down"]["sample_size"] == 1
    assert report["by_decision_reason"]["market_blocks_new_positions"]["sample_size"] == 1


def test_signal_alpha_summary_reports_code_and_date_cluster_concentration():
    signals = [
        {
            "code": "A",
            "signal_date": "2025-01-01",
            "primary_strategy_route": "relative_strength_overheat",
            "market_signal": "RED",
            "forward_returns": {"20d": 0.04},
        },
        {
            "code": "A",
            "signal_date": "2025-01-03",
            "primary_strategy_route": "relative_strength_overheat",
            "market_signal": "RED",
            "forward_returns": {"20d": 0.06},
        },
        {
            "code": "B",
            "signal_date": "2025-01-10",
            "primary_strategy_route": "relative_strength_overheat",
            "market_signal": "RED",
            "forward_returns": {"20d": -0.01},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("20d",))

    concentration = report["by_route"]["relative_strength_overheat"]["concentration"]
    assert concentration["unique_codes"] == 2
    assert concentration["top_code"] == "A"
    assert concentration["top_code_sample_pct"] == 66.67
    assert concentration["date_cluster_count"] == 2


def test_signal_alpha_summary_groups_by_score_bucket_inside_market_route():
    signals = [
        {
            "code": "A",
            "signal_date": "2025-01-01",
            "score": 4.7,
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "forward_returns": {"20d": 0.08},
        },
        {
            "code": "B",
            "signal_date": "2025-01-02",
            "score": 4.9,
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "forward_returns": {"20d": 0.04},
        },
        {
            "code": "C",
            "signal_date": "2025-01-03",
            "score": 5.2,
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "forward_returns": {"20d": -0.03},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("20d",), bootstrap_iterations=40)

    assert report["by_score_bucket"]["4.5-5.0"]["sample_size"] == 2
    assert report["by_score_bucket"]["5.0-5.5"]["sample_size"] == 1
    route_bucket = report["by_route_score_bucket"]["trend_cooling_off"]["4.5-5.0"]
    assert route_bucket["horizons"]["20d"]["avg_return_pct"] == 6.0
    market_route_bucket = report["by_market_route_score_bucket"]["GREEN"]["trend_cooling_off"]["5.0-5.5"]
    assert market_route_bucket["sample_size"] == 1
    assert market_route_bucket["horizons"]["20d"]["avg_return_pct"] == -3.0


def test_signal_alpha_summary_groups_by_market_phase_route_score_bucket():
    signals = [
        {
            "code": "A",
            "signal_date": "2025-01-01",
            "score": 4.2,
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "market_context": {
                "index_ma20_deviation_pct": 1.2,
                "index_ma20_slope_5d_pct": 0.8,
            },
            "forward_returns": {"20d": 0.08},
        },
        {
            "code": "B",
            "signal_date": "2025-01-02",
            "score": 4.4,
            "primary_strategy_route": "trend_cooling_off",
            "market_signal": "GREEN",
            "market_context": {
                "index_ma20_deviation_pct": 1.8,
                "index_ma20_slope_5d_pct": 0.6,
            },
            "forward_returns": {"20d": 0.04},
        },
    ]

    report = signal_alpha_summary(signals, horizons=("20d",), bootstrap_iterations=40)

    bucket = report["by_market_phase_route_score_bucket"]["near_ma20_slope_up"]["trend_cooling_off"]["<4.5"]
    assert bucket["sample_size"] == 2
    assert bucket["horizons"]["20d"]["avg_return_pct"] == 6.0


def test_compare_backtest_signal_reports_surfaces_win_rate_and_trade_delta():
    baseline = {
        "total_return_pct": 1.2,
        "max_drawdown_pct": 8.0,
        "win_rate_pct": 42.0,
        "buy_trades": 5,
        "signal_alpha": {"overall": {"sample_size": 5}},
    }
    candidate = {
        "total_return_pct": 3.7,
        "max_drawdown_pct": 6.5,
        "win_rate_pct": 55.0,
        "buy_trades": 9,
        "signal_alpha": {"overall": {"sample_size": 9}},
    }

    comparison = compare_backtest_signal_reports(baseline, candidate)

    assert comparison["total_return_delta_pct"] == 2.5
    assert comparison["win_rate_delta_pct"] == 13.0
    assert comparison["max_drawdown_delta_pct"] == -1.5
    assert comparison["buy_trade_delta"] == 4
    assert comparison["signal_sample_delta"] == 4
    assert "增加了可验证信号样本" in comparison["interpretation"]


def test_backtest_report_includes_signal_alpha_for_recorded_routes():
    engine = BacktestEngine(BacktestConfig())
    dates = [
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
        "2026-01-09",
    ]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 10.2, 10.5, 11.0, 12.0, 13.0],
        })
    }
    engine._portfolio_value_series = [
        {"date": item, "equity": 100000.0}
        for item in dates
    ]

    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=6.2,
        entry_signal=False,
        primary_strategy_route="trend_cooling_off",
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        market_signal=MarketSignal.GREEN,
    )

    engine._record_signal_validation_rows(
        dates[0],
        [(score, intent)],
        MarketState(
            signal=MarketSignal.GREEN,
            multiplier=1.0,
            detail={
                "index_ma20_deviation_pct": 1.2,
                "index_ma20_slope_5d_pct": 0.8,
                "above_ma20_days": 6,
                "below_ma20_days": 0,
            },
        ),
    )
    report = engine._build_report()

    assert report["signal_alpha"]["overall"]["sample_size"] == 1
    assert report["signal_alpha"]["by_route"]["trend_cooling_off"]["sample_size"] == 1
    assert report["signal_alpha"]["overall"]["horizons"]["5d"]["avg_return_pct"] == 30.0
    context = report["signal_validation"]["signals"][0]["market_context"]
    assert context["index_ma20_deviation_pct"] == 1.2
    assert context["market_phase_bucket"] == "near_ma20_slope_up"
    assert report["calmar_ratio"] == 0.0


def test_backtest_report_samples_generic_entry_signals_for_classification():
    engine = BacktestEngine(BacktestConfig())
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    engine._sorted_dates = dates
    engine._bars = {
        "002138": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 10.2, 10.5, 11.0, 12.0, 13.0],
        })
    }
    engine._portfolio_value_series = [{"date": item, "equity": 100000.0} for item in dates]

    score = ScoreResult(
        code="002138",
        name="双环传动",
        total=6.2,
        entry_signal=True,
        primary_strategy_route=None,
    )
    intent = DecisionIntent(
        code="002138",
        name="双环传动",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        market_signal=MarketSignal.GREEN,
    )

    engine._record_signal_validation_rows(
        dates[0],
        [(score, intent)],
        MarketState(signal=MarketSignal.GREEN, multiplier=1.0),
    )
    report = engine._build_report()

    assert report["signal_validation"]["unknown_route_count"] == 0
    assert report["signal_validation"]["generic_entry_signal_count"] == 1
    assert report["signal_validation"]["generic_entry_signal_samples"][0]["code"] == "002138"


def test_backtest_report_keeps_high_score_watch_without_route_as_no_entry_signal():
    engine = BacktestEngine(BacktestConfig(decision_gates={"trial_buy_threshold": 6.0}))
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    engine._sorted_dates = dates
    engine._bars = {
        "600276": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 10.2, 10.5, 11.0, 12.0, 13.0],
        })
    }
    engine._portfolio_value_series = [{"date": item, "equity": 100000.0} for item in dates]

    score = ScoreResult(
        code="600276",
        name="恒瑞医药",
        total=6.2,
        entry_signal=False,
        primary_strategy_route=None,
    )
    intent = DecisionIntent(
        code="600276",
        name="恒瑞医药",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        market_signal=MarketSignal.RED,
    )

    engine._record_signal_validation_rows(
        dates[0],
        [(score, intent)],
        MarketState(signal=MarketSignal.RED, multiplier=0.0),
    )
    report = engine._build_report()

    assert report["signal_validation"]["unknown_route_count"] == 0
    assert report["signal_validation"]["no_entry_route_count"] == 1
    assert report["signal_validation"]["no_entry_route_samples"][0]["action"] == "WATCH"


def test_backtest_report_classifies_no_entry_route_bucket_from_technical_snapshot():
    engine = BacktestEngine(BacktestConfig(decision_gates={"trial_buy_threshold": 6.0}))
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    engine._sorted_dates = dates
    engine._bars = {
        "600276": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 10.2, 10.5, 11.0, 12.0, 13.0],
        })
    }
    engine._portfolio_value_series = [{"date": item, "equity": 100000.0} for item in dates]
    score = ScoreResult(
        code="600276",
        name="恒瑞医药",
        total=6.2,
        entry_signal=False,
        primary_strategy_route=None,
        dimensions=[
            DimensionScore("technical", 2.1, 3.0, "", {
                "above_ma20": True,
                "ma20_slope": 0.01,
                "momentum_5d": 1.2,
                "deviation_rate": 2.3,
                "volume_ratio": 1.7,
                "rsi": 55.0,
            })
        ],
    )
    intent = DecisionIntent(
        code="600276",
        name="恒瑞医药",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        market_signal=MarketSignal.RED,
    )

    engine._record_signal_validation_rows(
        dates[0],
        [(score, intent)],
        MarketState(signal=MarketSignal.RED, multiplier=0.0),
    )
    report = engine._build_report()
    sample = report["signal_validation"]["no_entry_route_samples"][0]

    assert sample["unknown_bucket"] == "near_pullback_missing_confirm"
    assert sample["technical_snapshot"]["volume_ratio"] == 1.7
    assert report["signal_alpha"]["by_market_unknown_bucket"]["RED"]["near_pullback_missing_confirm"]["sample_size"] == 1


def test_backtest_unknown_signal_payload_is_json_serializable_with_pandas_scalars():
    engine = BacktestEngine(BacktestConfig(decision_gates={"trial_buy_threshold": 6.0}))
    dates = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09"]
    engine._sorted_dates = dates
    engine._bars = {
        "600276": pd.DataFrame({
            "日期": dates,
            "收盘": [10.0, 10.2, 10.5, 11.0, 12.0, 13.0],
        })
    }
    engine._portfolio_value_series = [{"date": item, "equity": 100000.0} for item in dates]
    score = ScoreResult(
        code="600276",
        name="恒瑞医药",
        total=6.2,
        dimensions=[
            DimensionScore("technical", 2.1, 3.0, "", {
                "above_ma20": pd.Series([True]).iloc[0],
                "volume_ratio": pd.Series([1.7]).iloc[0],
            })
        ],
    )
    intent = DecisionIntent(
        code="600276",
        name="恒瑞医药",
        action=Action.WATCH,
        confidence=6.2,
        score=6.2,
        market_signal=MarketSignal.RED,
    )

    engine._record_signal_validation_rows(
        dates[0],
        [(score, intent)],
        MarketState(signal=MarketSignal.RED, multiplier=0.0),
    )

    json.dumps(engine._build_report(), ensure_ascii=False)


def test_backtest_report_can_return_all_signal_records_for_batch_aggregation():
    engine = BacktestEngine(BacktestConfig(signal_record_limit=None))
    engine._sorted_dates = ["2026-01-02"]
    engine._cash = 100000.0
    engine._portfolio_value_series = [{"date": "2026-01-02", "equity": 100000.0}]
    engine._signal_records = [
        {
            "code": f"CODE{i}",
            "primary_strategy_route": "volume_breakout",
            "market_signal": "GREEN",
            "forward_returns": {"10d": 0.01},
        }
        for i in range(60)
    ]

    report = engine._build_report()

    assert report["signal_validation"]["sample_size"] == 60
    assert len(report["signal_validation"]["signals"]) == 60
