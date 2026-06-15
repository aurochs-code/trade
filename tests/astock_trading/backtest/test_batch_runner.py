"""Batched backtest runner behavior."""

from __future__ import annotations

from astock_trading.backtest.batch_runner import BacktestBatchConfig, run_batched_backtest


def test_batched_backtest_splits_timeout_batch_to_preserve_samples():
    calls: list[tuple[str, ...]] = []

    def runner(codes: list[str], **kwargs):
        calls.append(tuple(codes))
        if "BAD" in codes:
            raise TimeoutError("provider stalled")
        return {
            "total_return_pct": 1.0,
            "annual_return_pct": 3.0,
            "max_drawdown_pct": 2.0,
            "win_rate_pct": 50.0,
            "sharpe_ratio": 0.8,
            "calmar_ratio": 1.5,
            "buy_trades": 1,
            "sell_trades": 1,
            "signal_validation": {
                "sample_size": 1,
                "signals": [
                    {
                        "code": codes[0],
                        "primary_strategy_route": "volume_breakout",
                        "market_signal": "GREEN",
                        "forward_returns": {"10d": 0.03},
                    }
                ],
                "unknown_route_count": 0,
                "unknown_route_samples": [],
            },
        }

    report = run_batched_backtest(
        ["GOOD1", "BAD", "GOOD2"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=3, batch_timeout_seconds=1.0),
        batch_runner=runner,
    )

    assert calls == [
        ("GOOD1", "BAD", "GOOD2"),
        ("GOOD1",),
        ("BAD",),
        ("GOOD2",),
    ]
    assert report["coverage"]["requested_codes"] == 3
    assert report["coverage"]["completed_codes"] == 2
    assert report["coverage"]["failed_codes"] == ["BAD"]
    assert report["signal_validation"]["sample_size"] == 2
    assert report["signal_alpha"]["overall"]["horizons"]["10d"]["avg_return_pct"] == 3.0
    assert report["portfolio_summary"]["avg_sharpe_ratio"] == 0.8
    assert report["portfolio_summary"]["avg_calmar_ratio"] == 1.5


def test_batched_backtest_progress_log_writes_to_stderr(capsys):
    def runner(codes: list[str], **kwargs):
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "signal_validation": {"sample_size": 0, "signals": []},
        }

    run_batched_backtest(
        ["600036"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=1, progress_log=True),
        batch_runner=runner,
    )

    captured = capsys.readouterr()
    assert "backtest_batch_progress" in captured.err
    assert "batch_start" in captured.err
    assert "batch_done" in captured.err
    assert captured.out == ""


def test_batched_backtest_passes_score_dimension_mode_to_runner():
    captured: dict[str, object] = {}

    def runner(codes: list[str], **kwargs):
        captured["config"] = kwargs["config"]
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "signal_validation": {"sample_size": 0, "signals": []},
        }

    report = run_batched_backtest(
        ["600036"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=1, score_dimension_mode="tech_fundamental"),
        batch_runner=runner,
    )

    assert captured["config"].score_dimension_mode == "tech_fundamental"
    assert report["config"]["score_dimension_mode"] == "tech_fundamental"


def test_batched_backtest_passes_watch_trial_config_to_runner():
    captured: dict[str, object] = {}

    def runner(codes: list[str], **kwargs):
        captured["config"] = kwargs["config"]
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "signal_validation": {"sample_size": 0, "signals": []},
        }

    report = run_batched_backtest(
        ["002138"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(
            batch_size=1,
            disable_market_reduce_sell=True,
            watch_loss_cooldown_days=5,
            watch_loss_cooldown_phases=("below_ma20_slope_up",),
            execute_watch_trial_markets=("GREEN", "YELLOW"),
            execute_watch_trial_routes=("relative_strength_overheat",),
            execute_watch_trial_pairs=("GREEN:relative_strength_overheat",),
            execute_buy_phases=("extended_above_ma20_slope_up",),
            execute_watch_trial_score_min=6.2,
            execute_watch_trial_score_max=6.8,
            execute_watch_trial_position_pct=0.08,
            execute_watch_trial_phases=("below_ma20_slope_up",),
            execute_watch_trial_min_above_ma20_days=6,
            execute_watch_trial_min_above_ma20_days_phases=("extended_above_ma20_slope_up",),
            execute_watch_trial_require_above_ma60_phases=("extended_above_ma20_slope_up",),
            execute_watch_trial_require_above_ma120_phases=("extended_above_ma20_slope_up",),
        ),
        batch_runner=runner,
    )

    config = captured["config"]
    assert config.disable_market_reduce_sell is True
    assert config.watch_loss_cooldown_days == 5
    assert config.watch_loss_cooldown_phases == ("below_ma20_slope_up",)
    assert config.execute_watch_trial_markets == ("GREEN", "YELLOW")
    assert config.execute_watch_trial_routes == ("relative_strength_overheat",)
    assert config.execute_watch_trial_pairs == ("GREEN:relative_strength_overheat",)
    assert config.execute_buy_phases == ("extended_above_ma20_slope_up",)
    assert config.execute_watch_trial_score_min == 6.2
    assert config.execute_watch_trial_score_max == 6.8
    assert config.execute_watch_trial_position_pct == 0.08
    assert config.execute_watch_trial_phases == ("below_ma20_slope_up",)
    assert config.execute_watch_trial_min_above_ma20_days == 6
    assert config.execute_watch_trial_min_above_ma20_days_phases == ("extended_above_ma20_slope_up",)
    assert config.execute_watch_trial_require_above_ma60_phases == ("extended_above_ma20_slope_up",)
    assert config.execute_watch_trial_require_above_ma120_phases == ("extended_above_ma20_slope_up",)
    assert report["config"]["execute_watch_trial_markets"] == ["GREEN", "YELLOW"]
    assert report["config"]["execute_watch_trial_routes"] == ["relative_strength_overheat"]
    assert report["config"]["execute_watch_trial_pairs"] == ["GREEN:relative_strength_overheat"]
    assert report["config"]["execute_buy_phases"] == ["extended_above_ma20_slope_up"]
    assert report["config"]["disable_market_reduce_sell"] is True
    assert report["config"]["watch_loss_cooldown_days"] == 5
    assert report["config"]["watch_loss_cooldown_phases"] == ["below_ma20_slope_up"]
    assert report["config"]["execute_watch_trial_score_min"] == 6.2
    assert report["config"]["execute_watch_trial_score_max"] == 6.8
    assert report["config"]["execute_watch_trial_position_pct"] == 0.08
    assert report["config"]["execute_watch_trial_phases"] == ["below_ma20_slope_up"]
    assert report["config"]["execute_watch_trial_min_above_ma20_days"] == 6
    assert report["config"]["execute_watch_trial_min_above_ma20_days_phases"] == [
        "extended_above_ma20_slope_up"
    ]
    assert report["config"]["execute_watch_trial_require_above_ma60_phases"] == [
        "extended_above_ma20_slope_up"
    ]
    assert report["config"]["execute_watch_trial_require_above_ma120_phases"] == [
        "extended_above_ma20_slope_up"
    ]
    assert report["execution_semantics"]["mode"] == "research_what_if"
    assert report["execution_semantics"]["watch_trial_enabled"] is True
    assert report["execution_semantics"]["watch_loss_cooldown_days"] == 5
    assert report["execution_semantics"]["watch_loss_cooldown_phases"] == ["below_ma20_slope_up"]


def test_batched_backtest_defaults_to_production_buy_only_semantics():
    def runner(codes: list[str], **kwargs):
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "signal_validation": {"sample_size": 0, "signals": []},
        }

    report = run_batched_backtest(
        ["002138"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=1),
        batch_runner=runner,
    )

    assert report["execution_semantics"]["mode"] == "production_buy_only"
    assert report["execution_semantics"]["buy_only"] is True
    assert report["execution_semantics"]["route_policy_default_actions"] == ["BUY"]


def test_batched_backtest_compact_report_includes_buy_route_counts():
    def runner(codes: list[str], **kwargs):
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "buy_trades": 2,
            "sell_trades": 0,
            "trades": [
                {
                    "side": "buy",
                    "source_action": "WATCH",
                    "source_route": "relative_strength_overheat",
                },
                {
                    "side": "buy",
                    "source_action": "TRIAL_BUY",
                    "source_route": "pullback_to_ma20",
                },
                {"side": "sell", "source_route": "pullback_to_ma20"},
            ],
            "signal_validation": {"sample_size": 0, "signals": []},
        }

    report = run_batched_backtest(
        ["002138"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=1),
        batch_runner=runner,
    )

    assert report["batch_reports"][0]["buy_route_counts"] == {
        "TRIAL_BUY|pullback_to_ma20": 1,
        "WATCH|relative_strength_overheat": 1,
    }


def test_batched_backtest_respects_signal_output_limit_for_full_unknown_review():
    def runner(codes: list[str], **kwargs):
        return {
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "signal_validation": {
                "sample_size": 2,
                "signals": [
                    {
                        "code": f"{codes[0]}A",
                        "primary_strategy_route": "unknown",
                        "market_signal": "RED",
                        "forward_returns": {"20d": 0.01},
                    },
                    {
                        "code": f"{codes[0]}B",
                        "primary_strategy_route": "unknown",
                        "market_signal": "RED",
                        "forward_returns": {"20d": 0.02},
                    },
                ],
            },
        }

    report = run_batched_backtest(
        ["600036", "300750"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=1, signal_output_limit=0),
        batch_runner=runner,
    )
    assert report["signal_validation"]["sample_size"] == 4
    assert report["signal_validation"]["signals"] == []
    assert report["signal_validation"]["unknown_route_count"] == 4

    report = run_batched_backtest(
        ["600036", "300750"],
        "2024-01-01",
        "2024-12-31",
        BacktestBatchConfig(batch_size=1, signal_output_limit=4),
        batch_runner=runner,
    )
    assert len(report["signal_validation"]["signals"]) == 4
