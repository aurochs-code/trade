"""Research CLI behavior."""

from __future__ import annotations

import json

from typer.testing import CliRunner


def test_backtest_record_run_json_persists_result(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.backtest.engine as engine_module
    import astock_trading.backtest.persistence as persistence_module
    import astock_trading.platform.db as db_module

    saved: dict = {}

    class FakeConn:
        def close(self):
            saved["closed"] = True

    def fake_run_backtest(**kwargs):
        saved["run_kwargs"] = kwargs
        return {
            "preset": kwargs["preset"],
            "initial_cash": kwargs["initial_cash"],
            "final_value": 101000.0,
            "total_return_pct": 1.0,
            "annual_return_pct": 10.0,
            "max_drawdown_pct": 2.0,
            "win_rate_pct": 50.0,
            "total_trades": 1,
            "buy_trades": 1,
            "sell_trades": 0,
            "positions_open": 1,
            "trade_log": [
                {"date": "2026-01-02", "code": "600000", "side": "buy", "price": 10.0, "shares": 100}
            ],
            "trades": [
                {"date": "2026-01-02", "code": "600000", "side": "buy", "price": 10.0, "shares": 100}
            ],
            "equity_curve": [
                {"date": "2026-01-02", "equity": 101000.0, "cash": 99000.0, "positions": 1}
            ],
        }

    def fake_save(conn, result, *, request):
        saved["conn"] = conn
        saved["result"] = result
        saved["request"] = request
        return {
            "status": "recorded",
            "run_id": "bt_test",
            "trade_count": len(result["trade_log"]),
            "equity_curve_points": len(result["equity_curve"]),
        }

    monkeypatch.setattr(engine_module, "run_backtest", fake_run_backtest)
    monkeypatch.setattr(db_module, "connect", lambda: FakeConn())
    monkeypatch.setattr(persistence_module, "save_backtest_result", fake_save)

    result = CliRunner().invoke(
        app,
        [
            "backtest",
            "600000",
            "2026-01-02",
            "2026-01-05",
            "--watch-trial-position-pct",
            "0.06",
            "--watch-trial-phases",
            "below_ma20_slope_up",
            "--buy-phases",
            "extended_above_ma20_slope_up",
            "--disable-market-reduce-sell",
            "--holding-max",
            "8",
            "--time-stop-days",
            "20",
            "--stop-loss",
            "0.06",
            "--watch-loss-cooldown-days",
            "5",
            "--watch-loss-cooldown-phases",
            "below_ma20_slope_up",
            "--watch-trial-min-above-ma20-days",
            "6",
            "--watch-trial-min-above-ma20-days-phases",
            "extended_above_ma20_slope_up",
            "--watch-trial-require-above-ma60-phases",
            "extended_above_ma20_slope_up",
            "--watch-trial-require-above-ma120-phases",
            "extended_above_ma20_slope_up",
            "--scale-in",
            "--scale-in-profit-threshold",
            "0.12",
            "--scale-in-step-position-pct",
            "0.08",
            "--scale-in-max-position-pct",
            "0.30",
            "--scale-in-max-adds",
            "2",
            "--scale-in-min-days-between",
            "4",
            "--scale-in-routes",
            "short_continuation,volume_breakout",
            "--scale-in-markets",
            "GREEN,YELLOW",
            "--scale-in-actions",
            "BUY,WATCH,TRIAL_BUY",
            "--scale-in-no-require-entry-signal",
            "--scale-in-score-min",
            "5.5",
            "--scale-in-aggressive-max-position-pct",
            "0.30",
            "--scale-in-aggressive-step-position-pct",
            "0.08",
            "--scale-in-aggressive-markets",
            "GREEN,YELLOW",
            "--scale-in-aggressive-routes",
            "short_continuation,volume_breakout",
            "--scale-in-aggressive-phases",
            "extended_above_ma20_slope_up,below_ma20_slope_up",
            "--trade-output-limit",
            "-1",
            "--record-run",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["recorded_run"] == {
        "status": "recorded",
        "run_id": "bt_test",
        "trade_count": 1,
        "equity_curve_points": 1,
    }
    assert saved["request"]["codes"] == ["600000"]
    assert saved["request"]["start"] == "2026-01-02"
    assert saved["request"]["end"] == "2026-01-05"
    assert saved["run_kwargs"]["execute_watch_trial_position_pct"] == 0.06
    assert saved["run_kwargs"]["execute_watch_trial_phases"] == ("below_ma20_slope_up",)
    assert saved["run_kwargs"]["execute_buy_phases"] == ("extended_above_ma20_slope_up",)
    assert saved["run_kwargs"]["disable_market_reduce_sell"] is True
    assert saved["run_kwargs"]["holding_max"] == 8
    assert saved["run_kwargs"]["time_stop_days"] == 20
    assert saved["run_kwargs"]["stop_loss"] == 0.06
    assert saved["run_kwargs"]["watch_loss_cooldown_days"] == 5
    assert saved["run_kwargs"]["watch_loss_cooldown_phases"] == ("below_ma20_slope_up",)
    assert saved["run_kwargs"]["execute_watch_trial_min_above_ma20_days"] == 6
    assert saved["run_kwargs"]["execute_watch_trial_min_above_ma20_days_phases"] == (
        "extended_above_ma20_slope_up",
    )
    assert saved["run_kwargs"]["execute_watch_trial_require_above_ma60_phases"] == (
        "extended_above_ma20_slope_up",
    )
    assert saved["run_kwargs"]["execute_watch_trial_require_above_ma120_phases"] == (
        "extended_above_ma20_slope_up",
    )
    assert saved["run_kwargs"]["scale_in_enabled"] is True
    assert saved["run_kwargs"]["scale_in_profit_threshold"] == 0.12
    assert saved["run_kwargs"]["scale_in_step_position_pct"] == 0.08
    assert saved["run_kwargs"]["scale_in_max_position_pct"] == 0.30
    assert saved["run_kwargs"]["scale_in_max_adds"] == 2
    assert saved["run_kwargs"]["scale_in_min_days_between"] == 4
    assert saved["run_kwargs"]["scale_in_routes"] == ("short_continuation", "volume_breakout")
    assert saved["run_kwargs"]["scale_in_market_signals"] == ("GREEN", "YELLOW")
    assert saved["run_kwargs"]["scale_in_actions"] == ("BUY", "WATCH", "TRIAL_BUY")
    assert saved["run_kwargs"]["scale_in_require_entry_signal"] is False
    assert saved["run_kwargs"]["scale_in_score_min"] == 5.5
    assert saved["run_kwargs"]["scale_in_aggressive_max_position_pct"] == 0.30
    assert saved["run_kwargs"]["scale_in_aggressive_step_position_pct"] == 0.08
    assert saved["run_kwargs"]["scale_in_aggressive_market_signals"] == ("GREEN", "YELLOW")
    assert saved["run_kwargs"]["scale_in_aggressive_routes"] == ("short_continuation", "volume_breakout")
    assert saved["run_kwargs"]["scale_in_aggressive_phase_buckets"] == (
        "extended_above_ma20_slope_up",
        "below_ma20_slope_up",
    )
    assert saved["run_kwargs"]["trade_record_limit"] is None
    assert saved["request"]["watch_trial_position_pct"] == 0.06
    assert saved["request"]["watch_trial_phases"] == ("below_ma20_slope_up",)
    assert saved["request"]["buy_phases"] == ("extended_above_ma20_slope_up",)
    assert saved["request"]["disable_market_reduce_sell"] is True
    assert saved["request"]["holding_max"] == 8
    assert saved["request"]["time_stop_days"] == 20
    assert saved["request"]["stop_loss"] == 0.06
    assert saved["request"]["watch_loss_cooldown_days"] == 5
    assert saved["request"]["watch_loss_cooldown_phases"] == ("below_ma20_slope_up",)
    assert saved["request"]["watch_trial_min_above_ma20_days"] == 6
    assert saved["request"]["watch_trial_min_above_ma20_days_phases"] == (
        "extended_above_ma20_slope_up",
    )
    assert saved["request"]["watch_trial_require_above_ma60_phases"] == (
        "extended_above_ma20_slope_up",
    )
    assert saved["request"]["watch_trial_require_above_ma120_phases"] == (
        "extended_above_ma20_slope_up",
    )
    assert saved["request"]["scale_in_enabled"] is True
    assert saved["request"]["scale_in_profit_threshold"] == 0.12
    assert saved["request"]["scale_in_step_position_pct"] == 0.08
    assert saved["request"]["scale_in_max_position_pct"] == 0.30
    assert saved["request"]["scale_in_max_adds"] == 2
    assert saved["request"]["scale_in_min_days_between"] == 4
    assert saved["request"]["scale_in_routes"] == ("short_continuation", "volume_breakout")
    assert saved["request"]["scale_in_market_signals"] == ("GREEN", "YELLOW")
    assert saved["request"]["scale_in_actions"] == ("BUY", "WATCH", "TRIAL_BUY")
    assert saved["request"]["scale_in_require_entry_signal"] is False
    assert saved["request"]["scale_in_score_min"] == 5.5
    assert saved["request"]["scale_in_aggressive_max_position_pct"] == 0.30
    assert saved["request"]["scale_in_aggressive_step_position_pct"] == 0.08
    assert saved["request"]["scale_in_aggressive_market_signals"] == ("GREEN", "YELLOW")
    assert saved["request"]["scale_in_aggressive_routes"] == ("short_continuation", "volume_breakout")
    assert saved["request"]["scale_in_aggressive_phase_buckets"] == (
        "extended_above_ma20_slope_up",
        "below_ma20_slope_up",
    )
    assert saved["request"]["trade_output_limit"] == -1
    assert saved["closed"] is True


def test_backtest_cli_preserves_preset_research_fields_when_not_overridden(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.backtest.engine as engine_module

    saved: dict = {}

    def fake_run_backtest(**kwargs):
        saved["run_kwargs"] = kwargs
        return {
            "preset": kwargs["preset"],
            "initial_cash": kwargs["initial_cash"],
            "final_value": 100000.0,
            "total_return_pct": 0.0,
            "annual_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "total_trades": 0,
            "buy_trades": 0,
            "sell_trades": 0,
            "positions_open": 0,
            "trade_log": [],
            "trades": [],
            "equity_curve": [],
        }

    monkeypatch.setattr(engine_module, "run_backtest", fake_run_backtest)

    result = CliRunner().invoke(
        app,
        [
            "backtest",
            "600000",
            "2026-01-02",
            "2026-01-05",
            "--preset",
            "攻_C_phase_filtered_watch08",
            "--json",
        ],
    )

    assert result.exit_code == 0, result.stdout
    assert saved["run_kwargs"]["execute_buy_phases"] is None
    assert saved["run_kwargs"]["execute_watch_trial_pairs"] is None
    assert saved["run_kwargs"]["execute_watch_trial_phases"] is None
    assert saved["run_kwargs"]["execute_watch_trial_score_min"] is None
    assert saved["run_kwargs"]["execute_watch_trial_score_max"] is None
    assert saved["run_kwargs"]["execute_watch_trial_position_pct"] is None


def test_command_catalog_marks_backtest_record_run_as_state_write():
    from astock_trading.platform.cli.agent import _command_catalog

    commands = {entry["id"]: entry for entry in _command_catalog()["commands"]}

    assert commands["backtest"]["writes_state"] is False
    assert commands["backtest"]["writes_order"] is False
    assert commands["backtest_record_run"]["writes_state"] is True
    assert commands["backtest_record_run"]["writes_order"] is False
    assert commands["backtest_record_run"]["state_events"] == [
        "backtest_runs",
        "backtest_trades",
        "backtest_equity_curve",
    ]
    assert commands["backtest_runs"]["writes_state"] is False
    assert commands["backtest_runs"]["writes_order"] is False


def test_backtest_runs_json_lists_recorded_runs(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.db as db_module

    class FakeResult:
        def fetchall(self):
            return [
                {
                    "run_id": "bt_1",
                    "preset": "验证A",
                    "codes_json": '["600000"]',
                    "start_date": "2026-01-02",
                    "end_date": "2026-01-05",
                    "initial_cash": 100000.0,
                    "final_value": 101000.0,
                    "metrics_json": '{"annual_return_pct": 12.0, "max_drawdown_pct": 3.0}',
                    "created_at": "2026-01-05T08:00:00+00:00",
                }
            ]

    class FakeConn:
        def execute(self, sql, params=None):
            assert "FROM backtest_runs" in sql
            assert params == (20,)
            return FakeResult()

        def close(self):
            pass

    monkeypatch.setattr(db_module, "connect", lambda: FakeConn())

    result = CliRunner().invoke(app, ["backtest-runs", "--json"])

    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["count"] == 1
    assert payload["runs"][0]["run_id"] == "bt_1"
    assert payload["runs"][0]["codes"] == ["600000"]
    assert payload["runs"][0]["metrics"]["annual_return_pct"] == 12.0
