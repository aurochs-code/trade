"""Smoke tests for the real CLI entrypoint."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from typer.testing import CliRunner


def _cli_env(tmp_path: Path) -> dict:
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{tmp_path / 'runtime.db'}"
    return env


def test_doctor_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "doctor", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["db"]["schema_version"] == 3
    assert payload["config"]["version"].startswith("v")
    assert "installed" in payload["mcp"]
    assert payload["timezone"] == "Asia/Shanghai"


def test_doctor_json_fails_without_database_url():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = os.environ.copy()
    env.pop("ASTOCK_DATABASE_URL", None)
    env["ASTOCK_NO_ENV_FILE"] = "1"

    result = subprocess.run(
        [str(cli), "doctor", "--json"],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert "ASTOCK_DATABASE_URL is required" in payload["error"]


def test_continuation_validate_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-validate", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "Top N" in result.stdout
    assert "--start" in result.stdout
    assert "--end" in result.stdout


def test_continuation_backtest_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-backtest", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--hold-days" in result.stdout
    assert "--top-n" in result.stdout


def test_continuation_study_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "continuation-study", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--top-ns" in result.stdout
    assert "--hold-days" in result.stdout


def test_stock_analyze_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "stock", "analyze", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "股票代码或名称" in result.stdout
    assert "--json" in result.stdout
    assert "--history-days" in result.stdout


def test_health_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "health", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] in {"ok", "warning", "failed"}
    assert "db" in payload
    assert "runs" in payload
    assert "data_sources" in payload
    assert "status" in payload["data_sources"]
    assert "checks" in payload["data_sources"]


def test_health_diagnostics_mask_database_password():
    from astock_trading.platform.cli.health import _diagnostic_database_url

    url = "mysql+pymysql://root:123456@127.0.0.1:33306/astock_trading?charset=utf8mb4"

    masked = _diagnostic_database_url(url)

    assert "123456" not in masked
    assert masked == "mysql+pymysql://root:***@127.0.0.1:33306/astock_trading?charset=utf8mb4"


def test_data_sources_status_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "data-sources", "status", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--max-age-hours" in result.stdout
    assert "--json" in result.stdout


def test_mcp_help_uses_stable_entrypoint():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "mcp", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "atrade mcp" in result.stdout
    assert "python -m astock_trading" not in result.stdout


def test_run_pipeline_help_includes_data_source_health_override():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "run-pipeline", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--ignore-data-source-health" in result.stdout
    assert "--json" in result.stdout
    for pipeline in [
        "morning",
        "noon",
        "intraday_monitor",
        "evening",
        "scoring",
        "weekly",
        "monthly",
        "sentiment",
        "auto_trade",
    ]:
        assert pipeline in result.stdout


def test_run_pipeline_json_reports_skip_without_text(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.pipelines as pipelines_cli
    import astock_trading.pipeline.context as pipeline_context

    class FakeRunJournal:
        def is_completed_today(self, pipeline_type):
            return False

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        run_journal = FakeRunJournal()
        conn = FakeConn()
        config_version = "test"

    monkeypatch.setattr(pipelines_cli, "is_trading_day", lambda: False)
    monkeypatch.setattr(pipeline_context, "build_context", lambda: FakeContext())

    result = CliRunner().invoke(app, ["run-pipeline", "morning", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "skipped"
    assert payload["pipeline"] == "morning"
    assert payload["reason"] == "non_trading_day"
    assert result.stderr == ""


def test_db_maintenance_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    for args, expected in [
        (["db", "backup", "--help"], "--output"),
        (["db", "tables", "--help"], "MySQL"),
        (["db", "check", "--help"], "CHECK TABLE"),
        (["db", "optimize", "--help"], "OPTIMIZE TABLE"),
    ]:
        result = subprocess.run(
            [str(cli), *args],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        assert expected in result.stdout


def test_db_status_initializes_schema_version_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "db", "status", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 3


def test_removed_sqlite_maintenance_commands_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    for command in ["vacuum", "integrity", "audit-projections", "rebuild-projections"]:
        result = subprocess.run(
            [str(cli), "db", command, "--help"],
            cwd=root,
            capture_output=True,
            text=True,
        )
        assert result.returncode != 0


def test_runs_cleanup_stale_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "runs", "cleanup-stale", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--older-than-hours" in result.stdout
    assert "--yes" in result.stdout


def test_agent_context_json_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "agent-context", "--json"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert "bin/trade" in payload["safe_entrypoints"]
    assert "src/astock_trading/**/*.py" in payload["forbidden_entrypoints"]


def test_machine_readable_runtime_commands_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = _cli_env(tmp_path)

    for args in [
        ["events", "query", "--json"],
        ["runs", "list", "--json"],
        ["manual-trades", "list", "--json"],
    ]:
        result = subprocess.run(
            [str(cli), *args],
            cwd=root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        assert json.loads(result.stdout) == []


def test_screener_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "screener", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "选股" in result.stdout
    assert "run" in result.stdout
    assert "score" in result.stdout
    assert "candidates" in result.stdout
    assert "promote" in result.stdout
    assert "reject" in result.stdout


def test_screener_candidates_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "screener", "candidates", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == []


def test_screener_promote_updates_candidates_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = _cli_env(tmp_path)

    promoted = subprocess.run(
        [
            str(cli),
            "screener",
            "promote",
            "002138",
            "--name",
            "双环传动",
            "--score",
            "7.2",
            "--to",
            "core",
            "--json",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(promoted.stdout)
    assert payload["status"] == "promoted"
    assert payload["code"] == "002138"
    assert payload["pool_tier"] == "core"

    listed = subprocess.run(
        [str(cli), "screener", "candidates", "--tier", "core", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    candidates = json.loads(listed.stdout)
    assert candidates == [
        {
            "code": "002138",
            "pool_tier": "core",
            "name": "双环传动",
            "score": 7.2,
            "added_at": candidates[0]["added_at"],
            "last_scored_at": candidates[0]["last_scored_at"],
            "streak_days": 0,
            "note": "manual_promote",
        }
    ]


def test_portfolio_status_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "status", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload == {
        "holding_count": 0,
        "total_cost_cents": 0,
        "total_market_cents": 0,
        "unrealized_pnl_cents": 0,
        "positions": [],
    }


def test_record_buy_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [
            str(cli),
            "record-buy",
            "002138",
            "100",
            "15.00",
            "--name",
            "双环传动",
            "--style",
            "momentum",
            "--reason",
            "manual_test",
            "--yes",
            "--json",
        ],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "recorded"
    assert payload["side"] == "buy"
    assert payload["code"] == "002138"
    assert payload["shares"] == 100
    assert payload["price_cents"] == 1500
    assert payload["fee_cents"] == 0
    assert payload["order"]["broker"] == "manual"
    assert payload["audit"]["ok"] is True
    assert payload["position_before"] is None
    assert payload["position_after"]["code"] == "002138"


def test_record_sell_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = _cli_env(tmp_path)

    subprocess.run(
        [
            str(cli),
            "record-buy",
            "002138",
            "100",
            "15.00",
            "--name",
            "双环传动",
            "--style",
            "momentum",
            "--yes",
            "--json",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    result = subprocess.run(
        [
            str(cli),
            "record-sell",
            "002138",
            "100",
            "16.00",
            "--reason",
            "manual_exit",
            "--yes",
            "--json",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "recorded"
    assert payload["side"] == "sell"
    assert payload["code"] == "002138"
    assert payload["shares"] == 100
    assert payload["price_cents"] == 1600
    assert payload["order"]["broker"] == "manual"
    assert payload["audit"]["ok"] is True
    assert payload["position_before"]["code"] == "002138"
    assert payload["position_after"] is None


def test_sqlite_to_mysql_migration_dry_run_json_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "db", "migrate-sqlite-to-mysql", "--dry-run", "--json"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert "event_log" in payload["source_counts"]
    assert payload["target"] == "not_written"
