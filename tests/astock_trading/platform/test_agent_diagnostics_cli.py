"""Agent diagnostics CLI contract tests."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

from astock_trading.market.store import MarketStore
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


def test_llm_context_json_runs_from_outside_checkout(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "llm-context", "--mode", "close", "--json"],
        cwd=tmp_path,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["context_type"] == "llm_summary_context"
    assert payload["mode"] == "close"
    assert payload["execution_allowed"] is False
    assert "record-buy" in " ".join(payload["guardrails"])
    assert "diagnostics" in payload["sections"]
    assert "trade_plan" in payload["sections"]
    assert payload["term_glossary"]


def test_llm_context_markdown_localizes_internal_terms(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "llm-context", "--mode", "morning"],
        cwd=tmp_path,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    text = result.stdout
    assert "自动执行：禁止" in text
    assert "计划已生成但不可执行" in text
    assert "候选池新鲜度" in text
    assert "核心池" in text
    assert "是否允许自动执行" in text
    assert "execution_allowed=false" not in text
    assert "status: `" not in text


def test_llm_context_close_includes_market_intel_comparison(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        for run_id, run_type, started_at in (
            ("morning_run", "morning", "2026-05-18T09:15:00+08:00"),
            ("evening_run", "evening", "2026-05-18T15:35:00+08:00"),
        ):
            conn.execute(
                """INSERT INTO run_log
                   (run_id, run_type, scope, config_version, status, started_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (run_id, run_type, "cn_a", "test", "ok", started_at),
            )

        store = MarketStore(conn)
        store.save_observation(
            "test",
            "sector_heatmap",
            "cn_a",
            {"items": [
                {"name": "机器人", "change_pct": 3.2, "amount": 1200000000},
                {"name": "光伏", "change_pct": 2.1, "amount": 800000000},
            ]},
            run_id="morning_run",
        )
        store.save_observation(
            "test",
            "sector_heatmap",
            "cn_a",
            {"items": [
                {"name": "机器人", "change_pct": 4.1, "amount": 1600000000},
                {"name": "算力", "change_pct": 3.5, "amount": 1800000000},
            ]},
            run_id="evening_run",
        )
        store.save_observation(
            "test",
            "cross_platform_hot_stocks",
            "cn_a",
            {"items": [{"code": "300001", "name": "早盘热股", "source_count": 2}]},
            run_id="morning_run",
        )
        store.save_observation(
            "test",
            "cross_platform_hot_stocks",
            "cn_a",
            {"items": [
                {"code": "300001", "name": "早盘热股", "source_count": 3},
                {"code": "300002", "name": "收盘热股", "source_count": 2},
            ]},
            run_id="evening_run",
        )
        store.save_observation(
            "test",
            "finance_flash",
            "cn_a",
            {"items": [{"title": "早盘政策新闻", "source": "eastmoney"}]},
            run_id="morning_run",
        )
        store.save_observation(
            "test",
            "finance_flash",
            "cn_a",
            {"items": [
                {"title": "早盘政策新闻", "source": "eastmoney"},
                {"title": "收盘新增新闻", "source": "sinafinance"},
            ]},
            run_id="evening_run",
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "llm-context", "--mode", "close", "--json"],
        cwd=tmp_path,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    intel = payload["sections"]["market_intel"]["data"]
    assert intel["comparison"]["available"] is True
    assert intel["morning"]["sectors"][0]["name"] == "机器人"
    assert intel["close"]["sectors"][1]["name"] == "算力"
    assert intel["comparison"]["sectors"]["persistent"][0]["name"] == "机器人"
    assert intel["comparison"]["sectors"]["new"][0]["name"] == "算力"
    assert intel["comparison"]["sectors"]["faded"][0]["name"] == "光伏"
    assert intel["comparison"]["hot_stocks"]["new"][0]["name"] == "收盘热股"
    assert intel["comparison"]["news"]["new"][0]["title"] == "收盘新增新闻"


def test_llm_context_morning_falls_back_to_latest_market_intel_cache(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation(
            "test",
            "sector_heatmap",
            "cn_a",
            {"items": [{"name": "AI算力", "change_pct": 2.8}]},
            run_id="market_intel_manual",
        )
        store.save_observation(
            "test",
            "finance_flash",
            "cn_a",
            {"items": [{"title": "周末财经新闻", "source": "sinafinance"}]},
            run_id="market_intel_manual",
        )
        store.save_observation(
            "test",
            "cross_platform_hot_stocks",
            "cn_a",
            {"items": [{"code": "300001", "name": "周末热股"}]},
            run_id="market_intel_manual",
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "llm-context", "--mode", "morning", "--json"],
        cwd=tmp_path,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    current = payload["sections"]["market_intel"]["data"]["current"]
    assert current["available"] is True
    assert current["fallback_used"] is True
    assert current["sectors"][0]["name"] == "AI算力"
    assert current["news"][0]["title"] == "周末财经新闻"
    assert current["hot_stocks"][0]["name"] == "周末热股"


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
