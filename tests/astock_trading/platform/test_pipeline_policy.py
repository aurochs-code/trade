"""Pipeline run policy tests."""

from __future__ import annotations


def test_intraday_monitor_can_run_more_than_once_on_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "intraday_monitor",
        is_trading_day=True,
        is_completed_today=True,
    )

    assert decision is None


def test_auto_trade_can_run_more_than_once_on_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "auto_trade",
        is_trading_day=True,
        is_completed_today=True,
    )

    assert decision is None


def test_weekly_can_run_on_non_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "weekly",
        is_trading_day=False,
        is_completed_today=False,
    )

    assert decision is None


def test_trading_day_pipeline_skips_on_non_trading_day():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "morning",
        is_trading_day=False,
        is_completed_today=False,
    )

    assert decision == "non_trading_day"


def test_daily_pipeline_skips_after_successful_run():
    from astock_trading.platform.pipeline_policy import should_skip_pipeline

    decision = should_skip_pipeline(
        "scoring",
        is_trading_day=True,
        is_completed_today=True,
    )

    assert decision == "completed_today"


def test_market_data_pipeline_is_blocked_when_core_data_sources_failed():
    from astock_trading.platform.pipeline_policy import data_source_gate_decision

    decision = data_source_gate_decision(
        "morning",
        {"status": "failed", "required_missing": ["baidu_fund_flow"], "optional_missing": []},
    )

    assert decision == "failed"


def test_market_data_pipeline_continues_when_only_optional_sources_degraded():
    from astock_trading.platform.pipeline_policy import data_source_gate_decision

    decision = data_source_gate_decision(
        "evening",
        {"status": "warning", "required_missing": [], "optional_missing": ["industry_comparison"]},
    )

    assert decision == "warning"


def test_weekly_pipeline_ignores_market_data_source_gate():
    from astock_trading.platform.pipeline_policy import data_source_gate_decision

    decision = data_source_gate_decision(
        "weekly",
        {"status": "failed", "required_missing": ["hot_stocks"], "optional_missing": []},
    )

    assert decision is None


def test_new_trade_guard_blocks_failed_runs_data_sources_and_portfolio_breach():
    from astock_trading.platform.pipeline_policy import new_trade_guard_decision

    decision = new_trade_guard_decision(
        failed_runs=[{"run_id": "run_failed", "run_type": "evening"}],
        data_source_health={"status": "failed", "required_missing": ["baidu_fund_flow"]},
        portfolio_breaches=[
            {"payload": {"rule": "daily_loss_limit", "description": "单日亏损超限"}}
        ],
    )

    assert decision["status"] == "blocked"
    assert decision["allow_new_trades"] is False
    assert [item["reason"] for item in decision["blockers"]] == [
        "recent_failed_pipeline",
        "data_source_health_failed",
        "portfolio_risk_block",
    ]


def test_new_trade_guard_ignores_failed_run_after_same_pipeline_recovers():
    from astock_trading.platform.pipeline_policy import new_trade_guard_decision

    decision = new_trade_guard_decision(
        failed_runs=[
            {
                "run_id": "run_auto_trade_failed",
                "run_type": "auto_trade",
                "started_at": "2026-05-22T06:22:40+00:00",
                "error_message": "stale running cleaned up after 0h",
            }
        ],
        successful_runs=[
            {
                "run_id": "run_auto_trade_recovered",
                "run_type": "auto_trade",
                "started_at": "2026-05-22T06:42:04+00:00",
            }
        ],
    )

    assert decision["status"] == "ok"
    assert decision["allow_new_trades"] is True
    assert decision["blockers"] == []
