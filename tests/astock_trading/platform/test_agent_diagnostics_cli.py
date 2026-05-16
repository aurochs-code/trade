"""Agent diagnostics CLI contract tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from astock_trading.platform.agent_diagnostics import diagnose_health
from astock_trading.platform.db import connect, init_db


def _cli_env(tmp_path: Path) -> dict:
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{tmp_path / 'runtime.db'}"
    return env


def test_diagnose_health_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "diagnose", "health", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["diagnostic"] == "health"
    assert payload["status"] in {"ok", "warning", "failed"}
    assert "findings" in payload
    assert "recommendations" in payload
    assert "data_sources" in payload["inputs"]


def test_diagnose_health_treats_old_failed_runs_as_historical(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "old_failed",
                "evening",
                "cn_a",
                "v_test",
                "failed",
                "2026-01-01T00:00:00+00:00",
                "old failure",
            ),
        )

        payload = diagnose_health(conn)

        assert payload["inputs"]["failed_runs"] == []
        assert payload["inputs"]["historical_failed_runs"][0]["run_id"] == "old_failed"
        assert "failed runs require review" not in " ".join(payload["findings"])
    finally:
        conn.close()


def test_explain_run_missing_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "explain-run", "missing-run-id", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload == {
        "status": "not_found",
        "run_id": "missing-run-id",
        "findings": ["run_id not found"],
    }


def test_propose_plan_json_is_non_executing_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "propose-plan", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "proposed"
    assert payload["execution_allowed"] is False
    assert payload["plan_type"] == "agent_trade_plan"
    assert "diagnostics" in payload
    assert "actions" in payload


def test_notify_propose_plan_dry_run_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "notify", "propose-plan", "--dry-run", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["notification"]["target"] == "discord"
    assert "交易计划" in payload["embed"]["title"]
    assert payload["plan"]["execution_allowed"] is False


def test_diagnose_strategy_json_reports_parameter_profile_need(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "diagnose", "strategy", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["diagnostic"] == "strategy"
    assert payload["status"] in {"ok", "warning"}
    assert "findings" in payload
    assert "recommendations" in payload
    assert "decision_gates" in payload["inputs"]
    assert payload["parameter_profiles"]["need_multiple_profiles"] is True
    assert {item["name"] for item in payload["parameter_profiles"]["suggested"]} >= {
        "trend_swing",
        "short_continuation",
        "defensive_watch",
    }
