"""Smoke tests for the real CLI entrypoint."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from astock_trading.platform.cli.screener import _scan_limit


def _cli_env(tmp_path: Path) -> dict:
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{tmp_path / 'runtime.db'}"
    return env


def test_screener_limit_defaults_to_configured_market_scan_limit():
    assert _scan_limit({"market_scan_limit": 300}, None) == 300
    assert _scan_limit({"market_scan_limit": 300}, 25) == 25
    assert _scan_limit({}, None) == 30


def test_source_quality_summary_reports_per_stock_coverage():
    import astock_trading.platform.cli.screener as screener_cli
    from astock_trading.market.models import (
        FinancialReport,
        FundFlow,
        SectorContext,
        SentimentData,
        StockQuote,
        StockSnapshot,
        TechnicalIndicators,
    )

    quote = StockQuote(
        code="002138",
        name="双环传动",
        price=35.0,
        open=34.0,
        high=36.0,
        low=33.0,
        close=35.0,
        volume=100000,
        amount=3500000.0,
        change_pct=2.1,
    )
    full_snapshot = StockSnapshot(
        code="002138",
        name="双环传动",
        quote=quote,
        technical=TechnicalIndicators(above_ma20=True),
        financial=FinancialReport(roe=12.0, revenue_growth=18.0, operating_cash_flow=1.0),
        flow=FundFlow(net_inflow_1d=500000.0),
        sentiment=SentimentData(score=1.8),
        sector=SectorContext(industry_name="汽车零部件", confirmed=True),
    )
    missing_flow = StockSnapshot(
        code="603215",
        name="比依股份",
        quote=quote,
        technical=TechnicalIndicators(),
        financial=FinancialReport(roe=8.0, revenue_growth=5.0, operating_cash_flow=1.0),
    )

    summary = screener_cli._build_source_quality_summary(
        [full_snapshot, missing_flow],
        [
            {"code": "002138", "data_quality": "ok", "data_missing_fields": []},
            {"code": "603215", "data_quality": "degraded", "data_missing_fields": ["资金流"]},
        ],
    )

    assert summary["status"] == "warning"
    assert summary["sample_size"] == 2
    assert summary["coverage"]["quote"]["available"] == 2
    assert summary["coverage"]["flow"] == {
        "label": "资金流",
        "layer": "L1",
        "available": 1,
        "missing": 1,
        "total": 2,
        "rate": 0.5,
    }
    assert summary["score_quality_counts"] == {"degraded": 1, "ok": 1}
    assert summary["missing_fields"][0] == {"field": "资金流", "count": 1}
    assert any("逐票资金流覆盖率" in item for item in summary["warnings"])


def test_screener_run_archives_history_snapshot(monkeypatch, tmp_path):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.screener as screener_cli
    from astock_trading.market.models import StockQuote, StockSnapshot, TechnicalIndicators
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    class NonClosingConn:
        def __init__(self, conn):
            self._conn = conn

        def execute(self, *args, **kwargs):
            return self._conn.execute(*args, **kwargs)

        def close(self):
            pass

    class FakeObsidian:
        def write_screening_result(self, *args, **kwargs):
            pass

    def fake_score_stock_batch(ctx, stock_list, run_id):
        ctx.event_store.append(
            "strategy:002138",
            "strategy",
            "decision.suggested",
            {
                "code": "002138",
                "name": "双环传动",
                "action": "WATCH",
                "market_signal": "YELLOW",
                "notes": ["缺少入场信号"],
            },
            metadata={"run_id": run_id},
        )
        quote = StockQuote(
            code="002138",
            name="双环传动",
            price=35.0,
            open=34.0,
            high=36.0,
            low=33.0,
            close=35.0,
            volume=100000,
            amount=3500000.0,
            change_pct=2.1,
        )
        return {
            "scores": [
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 5.8,
                    "entry_signal": False,
                    "data_quality": "ok",
                }
            ],
            "snapshots": [
                StockSnapshot(
                    code="002138",
                    name="双环传动",
                    quote=quote,
                    technical=TechnicalIndicators(above_ma20=True),
                )
            ],
        }

    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    raw_conn = connect(db_path)
    conn = NonClosingConn(raw_conn)
    event_store = EventStore(conn)
    ctx = SimpleNamespace(
        cfg={
            "screening": {"market_scan_limit": 1},
            "pool_management": {"watch_min_score": 5.0},
            "scoring": {"thresholds": {"buy": 6.5, "watch": 5.0, "reject": 4.0}},
        },
        conn=conn,
        event_store=event_store,
        projector=ProjectionUpdater(event_store, conn),
        obsidian=FakeObsidian(),
    )
    monkeypatch.setattr(screener_cli, "build_context", lambda: ctx)
    monkeypatch.setattr(
        screener_cli,
        "_search_screener_results",
        lambda query, timeout_seconds: [{"code": "002138", "name": "双环传动"}],
    )
    monkeypatch.setattr(screener_cli, "_score_stock_batch", fake_score_stock_batch)

    try:
        result = CliRunner().invoke(
            app,
            ["screener", "run", "--query", "强势股", "--json"],
        )

        assert result.exit_code == 0
        payload = json.loads(result.stdout)
        assert payload["history_group_id"].endswith(payload["run_id"])
        assert payload["source_quality"]["sample_size"] == 1
        assert payload["source_quality"]["coverage"]["quote"]["available"] == 1
        assert payload["source_quality"]["coverage"]["flow"]["missing"] == 1
        rows = raw_conn.execute(
            "SELECT phase, snapshot_type FROM signal_history_snapshots WHERE history_group_id = ?",
            (payload["history_group_id"],),
        ).fetchall()
        assert {row["snapshot_type"] for row in rows} == {"market", "pool", "candidates", "decision"}
        assert {row["phase"] for row in rows} == {"screener"}
    finally:
        raw_conn.close()


def test_screener_refresh_json_returns_timeout_payload_when_search_hangs(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.screener as screener_cli

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        cfg = {
            "screening": {
                "mx_query": "强势股",
                "screener_query_timeout_seconds": 0.01,
            }
        }
        conn = FakeConn()

    def timeout_search(query, timeout_seconds):
        raise screener_cli.ScreenerSearchTimeout(query, timeout_seconds)

    monkeypatch.setattr(screener_cli, "build_context", lambda: FakeContext())
    monkeypatch.setattr(screener_cli, "_search_screener_results", timeout_search)

    result = CliRunner().invoke(app, ["screener", "refresh", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload == {
        "command": "screener refresh",
        "status": "failed",
        "reason": "screener_search_timeout",
        "query": "强势股",
        "timeout_seconds": 0.01,
        "execution_allowed": False,
        "writes_state": False,
        "summary": "选股粗筛源超时，候选池未刷新；先诊断数据源或调小刷新范围。",
        "next_action": {
            "type": "diagnose_data_sources",
            "label": "诊断数据源",
            "command": "atrade data-sources diagnose --json",
            "safe_to_auto_apply": True,
        },
    }


def test_screener_refresh_json_returns_failed_payload_when_search_worker_crashes(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.screener as screener_cli

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        cfg = {"screening": {"mx_query": "强势股"}}
        conn = FakeConn()

    def failed_search(query, timeout_seconds):
        raise screener_cli.ScreenerSearchFailed(query, returncode=-6, stderr_tail="libmini_racer fatal")

    monkeypatch.setattr(screener_cli, "build_context", lambda: FakeContext())
    monkeypatch.setattr(screener_cli, "_search_screener_results", failed_search)

    result = CliRunner().invoke(app, ["screener", "refresh", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["command"] == "screener refresh"
    assert payload["status"] == "failed"
    assert payload["reason"] == "screener_search_failed"
    assert payload["query"] == "强势股"
    assert payload["execution_allowed"] is False
    assert payload["writes_state"] is False
    assert payload["diagnostic"]["returncode"] == -6
    assert payload["diagnostic"]["stderr_tail"] == "libmini_racer fatal"
    assert payload["next_action"]["command"] == "atrade data-sources diagnose --json"


def test_screener_refresh_json_returns_timeout_payload_when_scoring_hangs(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.screener as screener_cli

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        cfg = {
            "screening": {
                "mx_query": "强势股",
                "screener_scoring_timeout_seconds": 0.01,
            }
        }
        conn = FakeConn()

    def fake_search(query, timeout_seconds):
        return [{"code": "002138", "name": "双环传动"}]

    def timeout_score_batch(ctx, stock_list, run_id, *, query, timeout_seconds):
        raise screener_cli.ScreenerScoringTimeout(query, timeout_seconds)

    monkeypatch.setattr(screener_cli, "build_context", lambda: FakeContext())
    monkeypatch.setattr(screener_cli, "_search_screener_results", fake_search)
    monkeypatch.setattr(screener_cli, "_hot_recall_candidates", lambda conn, *, limit: [])
    monkeypatch.setattr(screener_cli, "_candidate_rows", lambda conn, tier="all", limit=1000: [])
    monkeypatch.setattr(screener_cli, "_score_stock_batch_with_timeout", timeout_score_batch)

    result = CliRunner().invoke(app, ["screener", "refresh", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload == {
        "command": "screener refresh",
        "status": "failed",
        "reason": "screener_scoring_timeout",
        "query": "强势股",
        "timeout_seconds": 0.01,
        "execution_allowed": False,
        "candidate_pool_refreshed": False,
        "may_have_partial_score_events": True,
        "summary": "逐票评分或行情采集超时，候选池未刷新；先诊断数据源或调小 --limit。",
        "next_action": {
            "type": "diagnose_data_sources",
            "label": "诊断数据源",
            "command": "atrade data-sources diagnose --json",
            "safe_to_auto_apply": True,
        },
    }


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
    assert payload["db"]["schema_version"] == 4
    assert payload["config"]["version"].startswith("v")
    assert "installed" in payload["mcp"]
    assert payload["timezone"] == "Asia/Shanghai"


def test_hermes_opportunity_watch_alerts_when_pool_turns_non_empty(monkeypatch, tmp_path):
    from astock_trading.platform.cli import app
    from astock_trading.platform.db import connect, init_db

    db_path = tmp_path / "runtime.db"
    state_file = tmp_path / "opportunity-watch.json"
    monkeypatch.setenv("ASTOCK_DATABASE_URL", f"sqlite:///{db_path}")

    first = CliRunner().invoke(
        app,
        ["opportunity-watch", "--state-file", str(state_file), "--json"],
    )
    assert first.exit_code == 0
    first_payload = json.loads(first.stdout)
    assert first_payload["status"] == "baseline_recorded"
    assert first_payload["should_notify"] is False
    assert first_payload["current_counts"]["all_candidates"] == 0

    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "300558",
                "watch",
                "贝达药业",
                6.2,
                "2026-05-21T10:00:00+08:00",
                "2026-05-21T10:00:00+08:00",
                1,
                "screener_refresh:requires_entry_strategy_route",
            ),
        )
    finally:
        conn.close()

    second = CliRunner().invoke(
        app,
        ["opportunity-watch", "--state-file", str(state_file), "--json"],
    )

    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    assert payload["status"] == "changed"
    assert payload["should_notify"] is True
    assert payload["execution_allowed"] is False
    assert payload["manual_confirmation_required"] is True
    assert "candidate_pool_activated" in payload["change_types"]
    assert "new_watch_candidates" in payload["change_types"]
    assert payload["previous_counts"]["all_candidates"] == 0
    assert payload["counts"] == payload["current_counts"]
    assert payload["current_counts"]["watch_candidates"] == 1
    assert payload["candidate_summary"]["total"] == 1
    assert payload["candidate_summary"]["watch_count"] == 1
    assert payload["current_action"]["command"] == payload["next_action"]["command"]
    assert payload["current_action"]["safe_to_auto_apply"] == payload["next_action"]["safe_to_auto_apply"]
    assert payload["new_candidates"][0]["code"] == "300558"
    assert payload["new_candidates"][0]["pool_tier_label"] == "观察"
    assert "主动提醒" in payload["summary"]


def test_hermes_opportunity_watch_alerts_on_new_radar_candidate(monkeypatch, tmp_path):
    from astock_trading.platform.cli import app
    from astock_trading.platform.db import connect, init_db

    db_path = tmp_path / "runtime.db"
    state_file = tmp_path / "opportunity-watch.json"
    monkeypatch.setenv("ASTOCK_DATABASE_URL", f"sqlite:///{db_path}")

    first = CliRunner().invoke(
        app,
        ["opportunity-watch", "--state-file", str(state_file), "--json"],
    )
    assert first.exit_code == 0

    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "603376",
                "radar",
                "大明电子",
                4.8,
                "2026-05-22T13:30:00+08:00",
                "2026-05-22T13:30:00+08:00",
                0,
                "screener_refresh:below_watch_retained",
            ),
        )
    finally:
        conn.close()

    second = CliRunner().invoke(
        app,
        ["opportunity-watch", "--state-file", str(state_file), "--json"],
    )

    assert second.exit_code == 0
    payload = json.loads(second.stdout)
    assert payload["status"] == "changed"
    assert payload["should_notify"] is True
    assert "new_radar_candidates" in payload["change_types"]
    assert payload["current_counts"]["radar_candidates"] == 1
    assert payload["new_candidates"][0]["pool_tier_label"] == "强势观察"
    assert "新强势观察候选" in payload["summary"]


def test_hermes_opportunity_watch_alerts_when_evidence_action_appears(monkeypatch, tmp_path):
    import astock_trading.platform.hermes_commands as hermes_commands
    from astock_trading.platform.db import connect, init_db

    db_path = tmp_path / "runtime.db"
    state_file = tmp_path / "opportunity-watch.json"
    init_db(db_path)
    conn = connect(db_path)
    previous_attention_key = "|".join([
        "profile_review_required",
        "review_runtime_profile_activation",
        "atrade strategy profile-activation --target trend_swing --json",
        "true",
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json",
    ])
    state_file.write_text(
        json.dumps({
            "snapshot": {
                "date": "2026-05-22",
                "counts": {
                    "buy_intents": 0,
                    "core_candidates": 1,
                    "watch_candidates": 0,
                    "radar_candidates": 0,
                    "all_candidates": 1,
                },
                "candidate_keys": ["core:688981"],
                "core_keys": ["core:688981"],
                "watch_keys": [],
                "radar_keys": [],
                "candidates": [{"code": "688981", "pool_tier": "core", "score": 6.4}],
                "attention_key": previous_attention_key,
                "attention": {"key": previous_attention_key},
            }
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        hermes_commands,
        "_candidate_pool_items",
        lambda _conn, limit=1000: [
            {"code": "688981", "name": "中芯国际", "pool_tier": "core", "score": 6.4}
        ],
    )
    monkeypatch.setattr(
        hermes_commands,
        "build_opportunity_card",
        lambda _conn, limit=5: {
            "date": "2026-05-22",
            "status": "profile_review_required",
            "summary": "核心候选 1 只；模拟承接前先复核运行 profile。",
            "decision_brief": "已有影子复盘证据可记录。",
            "counts": {
                "buy_intents": 0,
                "core_candidates": 1,
                "watch_candidates": 0,
                "radar_candidates": 0,
                "all_candidates": 1,
            },
            "next_action": {
                "type": "review_runtime_profile_activation",
                "label": "复核运行 profile 激活",
                "command": "atrade strategy profile-activation --target trend_swing --json",
                "reason": "运行环境仍会使用 default。",
                "safe_to_auto_apply": False,
            },
            "approval_gate": {
                "required": True,
                "apply_command": (
                    "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
                ),
            },
            "evidence_actions": [
                {
                    "type": "record_positive_trial_review",
                    "label": "记录影子试运行复盘",
                    "command": "atrade paper trial-review --min-age-days 0 --record --json",
                    "reason": "可先写入影子复盘证据，不提交模拟盘订单。",
                    "safe_to_auto_apply": True,
                    "writes_state": True,
                }
            ],
        },
    )
    try:
        payload = hermes_commands.build_opportunity_watch(
            conn,
            state_file=state_file,
            update_state=False,
        )
    finally:
        conn.close()

    assert payload["status"] == "changed"
    assert payload["should_notify"] is True
    assert "operator_action_required" in payload["change_types"]
    assert payload["opportunity"]["evidence_actions"][0]["command"] == (
        "atrade paper trial-review --min-age-days 0 --record --json"
    )
    assert "record_positive_trial_review" in payload["snapshot"]["attention_key"]


def test_notify_opportunity_watch_dry_run_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = _cli_env(tmp_path)
    state_file = tmp_path / "opportunity-watch.json"

    subprocess.run(
        [str(cli), "opportunity-watch", "--state-file", str(state_file), "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    from astock_trading.platform.db import connect, init_db

    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "002138",
                "watch",
                "双环传动",
                6.4,
                "2026-05-21T10:05:00+08:00",
                "2026-05-21T10:05:00+08:00",
                1,
                "screener_refresh:requires_entry_strategy_route",
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [
            str(cli),
            "notify",
            "opportunity-watch",
            "--state-file",
            str(state_file),
            "--dry-run",
            "--json",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["notification"]["skipped"] is False
    assert payload["monitor"]["should_notify"] is True
    assert "新观察候选" in payload["embed"]["title"]
    values = "\n".join(field["value"] for field in payload["embed"]["fields"])
    assert "双环传动(002138)" in values
    assert "禁止自动执行" in values


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


def test_backtest_help_includes_history_mirror_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "backtest", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "--history-mirror" in result.stdout
    assert "--no-history-mirror" in result.stdout


def test_calibrate_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "calibrate", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "param_calibration"
    assert payload["status"] == "insufficient_data"
    assert payload["guardrails"]["auto_apply"] is False
    assert result.stderr == ""


def test_risk_adaptive_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "risk", "adaptive", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "adaptive_risk"
    assert payload["status"] == "insufficient_data"
    assert payload["guardrails"]["auto_apply"] is False
    assert result.stderr == ""


def test_strategy_profiles_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "strategy", "profiles", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "strategy_profile_comparison"
    assert payload["guardrails"]["auto_switch_profile"] is False
    assert {item["name"] for item in payload["profiles"]} >= {
        "trend_swing",
        "short_continuation",
        "defensive_watch",
    }
    short_profile = next(item for item in payload["profiles"] if item["name"] == "short_continuation")
    assert short_profile["key_parameters"]["auto_trade_dry_run"] is True
    trend_profile = next(item for item in payload["profiles"] if item["name"] == "trend_swing")
    assert trend_profile["key_parameters"]["auto_trade_dry_run"] is False
    assert result.stderr == ""


def test_strategy_profile_activation_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "strategy", "profile-activation", "--target", "trend_swing", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "strategy_profile_activation_plan"
    assert payload["status"] in {"requires_manual_confirmation", "already_active"}
    assert payload["target_profile"] == "trend_swing"
    assert payload["summary"]
    assert payload["activation"]["export_command"] == "export ASTOCK_CONFIG_PROFILE=trend_swing"
    assert payload["activation"]["verify_command"] == (
        "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json"
    )
    assert payload["approval_gate"]["apply_command"] == (
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
    )
    assert payload["approval_gate"]["apply_command_contract"]["writes_environment"] is True
    assert payload["approval_gate"]["apply_command_contract"]["requires_user_approval"] is True
    assert payload["post_approval_checklist"]["steps"][0]["command"] == (
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
    )
    assert payload["post_approval_checklist"]["steps"][1]["command"] == "atrade diagnose schedule --json"
    assert payload["post_approval_checklist"]["steps"][2]["command"] == "atrade paper auto-readiness --json"
    assert payload["post_approval_checklist"]["paper_order_execution"]["command"] == (
        "atrade run-pipeline auto_trade --json"
    )
    assert payload["post_approval_checklist"]["paper_order_execution"]["requires_separate_user_approval"] is True
    assert payload["post_approval_checklist"]["paper_order_execution"]["command_contract"]["writes_order"] is True
    after_approval_preview = payload["after_approval_preview"]
    if payload["approval_gate"].get("required"):
        assert after_approval_preview["available"] is True
        assert after_approval_preview["target_profile"] == "trend_swing"
        assert after_approval_preview["preview_command"] == (
            "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --skip-account --json"
        )
        assert after_approval_preview["post_approval_verify_command"] == (
            "atrade paper auto-readiness --json"
        )
        assert after_approval_preview["schedule_verify_command"] == "atrade diagnose schedule --json"
        assert isinstance(after_approval_preview["remaining_blockers_from_current_readiness"], list)
        assert after_approval_preview["writes_environment"] is False
        assert after_approval_preview["places_order"] is False
    else:
        assert after_approval_preview["available"] is False
    assert payload["next_action"]["command"] in {
        "atrade diagnose schedule --json",
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json",
    }
    if payload["next_action"]["command"].endswith("--apply-env --yes --json"):
        assert payload["next_action"]["writes_environment"] is True
        assert payload["next_action"]["writes_state"] is True
        assert payload["next_action"]["writes_order"] is False
        assert payload["next_action"]["requires_user_approval"] is True
        assert payload["next_action"]["risk_level"] == "environment_write"
        assert payload["next_action"]["command_contract_id"] == "strategy_profile_activation_apply"
    assert payload["guardrails"]["auto_switch_profile"] is False
    assert payload["guardrails"]["modifies_environment"] is False
    assert result.stderr == ""


def test_strategy_profile_activation_apply_env_requires_yes_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"ASTOCK_DATABASE_URL=sqlite:///{tmp_path / 'runtime.db'}\n",
        encoding="utf-8",
    )
    env = _cli_env(tmp_path)
    env["ASTOCK_ENV_FILE"] = str(env_file)

    result = subprocess.run(
        [
            str(cli),
            "strategy",
            "profile-activation",
            "--target",
            "trend_swing",
            "--apply-env",
            "--env-file",
            str(env_file),
            "--json",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "confirmation_required"
    assert payload["guardrails"]["modifies_environment"] is False
    assert payload["next_action"]["writes_environment"] is True
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is True
    assert payload["next_action"]["risk_level"] == "environment_write"
    assert payload["next_action"]["command_contract_id"] == "strategy_profile_activation_apply"
    assert "ASTOCK_CONFIG_PROFILE" not in env_file.read_text(encoding="utf-8")


def test_strategy_profile_activation_already_active_next_action_is_read_only(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = _cli_env(tmp_path)
    env["ASTOCK_CONFIG_PROFILE"] = "trend_swing"

    result = subprocess.run(
        [str(cli), "strategy", "profile-activation", "--target", "trend_swing", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "already_active"
    assert payload["next_action"]["command"] == "atrade diagnose schedule --json"
    assert payload["next_action"]["command_contract_id"] == "diagnose_schedule"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_environment"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False
    assert payload["next_action"]["risk_level"] == "read_only"


def test_strategy_profile_activation_apply_env_yes_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env_file = tmp_path / ".env"
    env_file.write_text(
        f"ASTOCK_DATABASE_URL=sqlite:///{tmp_path / 'runtime.db'}\n",
        encoding="utf-8",
    )
    env = _cli_env(tmp_path)
    env["ASTOCK_ENV_FILE"] = str(env_file)

    result = subprocess.run(
        [
            str(cli),
            "strategy",
            "profile-activation",
            "--target",
            "trend_swing",
            "--apply-env",
            "--yes",
            "--env-file",
            str(env_file),
            "--json",
        ],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "applied"
    assert payload["runtime_env"]["env_file"] == str(env_file)
    assert payload["runtime_env"]["after_profile"] == "trend_swing"
    assert payload["recorded_event_id"]
    assert "ASTOCK_CONFIG_PROFILE=trend_swing" in env_file.read_text(encoding="utf-8")
    assert payload["next_action"]["command"] == "atrade diagnose schedule --json"
    assert payload["next_action"]["command_contract_id"] == "diagnose_schedule"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_environment"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False
    assert payload["next_action"]["risk_level"] == "read_only"
    assert result.stderr == ""


def test_strategy_allocation_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "strategy", "allocation", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "strategy_capital_allocation"
    assert payload["guardrails"]["auto_apply"] is False
    assert payload["guardrails"]["manual_approval_required"] is True
    assert result.stderr == ""


def test_strategy_health_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "strategy", "health", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "strategy_health_review"
    assert payload["status"] == "insufficient_data"
    assert payload["guardrails"]["auto_apply"] is False
    assert result.stderr == ""


def test_dashboard_snapshot_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "dashboard", "snapshot", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["analysis"] == "dashboard_snapshot"
    assert payload["guardrails"]["read_only"] is True
    assert payload["guardrails"]["trading_actions_enabled"] is False
    assert result.stderr == ""


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


def test_screener_explain_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "screener", "explain", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "解释近期为什么没有合适候选" in result.stdout
    assert "--near-miss-margin" in result.stdout
    assert "--follow-up-limit" in result.stdout
    assert "--json" in result.stdout


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


def test_health_json_treats_recovered_failed_runs_as_non_actionable(tmp_path):
    from astock_trading.platform.db import connect, init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{db_path}"

    init_db(db_path)
    conn = connect(db_path)
    try:
        rows = [
            (
                "auto_trade_failed",
                "auto_trade",
                "cn_a",
                "v_test",
                "failed",
                "2026-05-22T06:22:40+00:00",
                "2026-05-22T06:26:10+00:00",
                "stale running cleaned up after 0h",
            ),
            (
                "auto_trade_recovered",
                "auto_trade",
                "cn_a",
                "v_test",
                "completed",
                "2026-05-22T06:42:04+00:00",
                "2026-05-22T06:42:07+00:00",
                None,
            ),
        ]
        conn.executemany(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, finished_at, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "health", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["runs"]["failed_3d"] == []
    assert payload["runs"]["recovered_failed_3d"][0]["run_id"] == "auto_trade_failed"


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


def test_data_sources_diagnose_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "data-sources", "diagnose", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "诊断数据源" in result.stdout
    assert "--max-age-hours" in result.stdout
    assert "--json" in result.stdout


def test_data_sources_diagnose_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "data-sources", "diagnose", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["diagnostic"] == "data_sources"
    assert payload["status"] in {"ok", "warning", "failed"}
    assert "health" in payload
    assert "provider_failures" in payload
    assert "latest_screener_source_quality" in payload


def test_review_trades_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "review", "trades", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "交易后复盘" in result.stdout
    assert "--record" in result.stdout
    assert "--as-of" in result.stdout
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
    assert payload["schema_version"] == 4


def test_history_signal_json_via_bin_trade(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.history_mirror import archive_signal_history

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-05-19",
            history_group_id="hist_cli_1",
            run_id="screener_cli",
            phase="screener",
            market={"signal": "GREEN"},
            pool=[],
            candidates=[{"code": "300558", "name": "贝达药业", "total_score": 5.6}],
            decisions=[{"code": "300558", "name": "贝达药业", "action": "WATCH", "score": 5.6}],
        )
    finally:
        conn.close()

    result = subprocess.run(
        [
            str(cli),
            "history",
            "signal",
            "--date",
            "2026-05-19",
            "--history-group-id",
            "hist_cli_1",
            "--code",
            "300558",
            "--json",
        ],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["history_group_id"] == "hist_cli_1"
    assert payload["sections"]["candidates"][0]["code"] == "300558"
    assert payload["code_analysis"]["decision_action"] == "WATCH"
    assert "观察" in payload["code_analysis"]["miss_reason"]


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


def test_runs_failed_json_outputs_recent_failures(tmp_path):
    from astock_trading.platform.db import connect, init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, finished_at, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "run_failed_json",
                "auto_trade",
                "cn_a",
                "v_test",
                "failed",
                "2026-05-22T06:22:40+00:00",
                "2026-05-22T06:26:10+00:00",
                "stale running cleaned up after 0h",
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "runs", "failed", "--days", "30", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload[0]["run_id"] == "run_failed_json"
    assert payload[0]["run_type"] == "auto_trade"
    assert payload[0]["error_message"] == "stale running cleaned up after 0h"


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
    assert payload["recommended_commands"]["commands"] == "atrade commands --json"
    assert payload["recommended_commands"]["opportunity"] == "atrade opportunity --json"
    assert payload["recommended_commands"]["opportunity_watch"] == "atrade opportunity-watch --json"
    assert payload["recommended_commands"]["screener_explain"] == "atrade screener explain --json"
    assert payload["recommended_commands"]["paper_auto_readiness"] == "atrade paper auto-readiness --json"
    assert payload["recommended_commands"]["risk_trial_guard"] == "atrade risk trial-guard --json"
    assert payload["recommended_commands"]["paper_trial_plan"] == "atrade paper trial-plan --json"
    assert payload["recommended_commands"]["paper_trial_review"] == "atrade paper trial-review --json"
    assert payload["recommended_commands"]["events_backfill_evidence"] == "atrade events backfill-evidence --json"
    assert payload["recommended_commands"]["diagnose_flow"] == "atrade diagnose flow --json"
    assert payload["recommended_commands"]["diagnose_schedule"] == "atrade diagnose schedule --json"
    assert payload["recommended_commands"]["llm_context_close"] == "atrade llm-context --mode close --json"
    assert (
        payload["recommended_commands"]["strategy_profile_activation"]
        == "atrade strategy profile-activation --target trend_swing --json"
    )


def test_agent_command_catalog_json_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    env = os.environ.copy()
    env.pop("ASTOCK_DATABASE_URL", None)
    env["ASTOCK_NO_ENV_FILE"] = "1"

    result = subprocess.run(
        [str(cli), "commands", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    context_result = subprocess.run(
        [str(cli), "agent-context", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    context_payload = json.loads(context_result.stdout)
    assert payload["command"] == "commands"
    assert payload["status"] == "ok"
    assert payload["catalog_version"] == 1
    assert payload["guardrails"]["stable_entrypoints"] == ["atrade", "bin/trade"]
    assert payload["guardrails"]["json_required_for_automation"] is True

    by_id = {item["id"]: item for item in payload["commands"]}
    by_command = {item["command"]: item for item in payload["commands"]}
    missing_contracts = {
        name: command
        for name, command in context_payload["recommended_commands"].items()
        if command not in by_command
    }
    assert missing_contracts == {}
    assert by_id["doctor"]["command"] == "atrade doctor --json"
    assert by_id["doctor"]["risk_level"] == "read_only"
    assert by_id["doctor"]["writes_state"] is False
    assert by_id["doctor"]["writes_environment"] is False
    assert by_id["doctor"]["writes_order"] is False
    assert by_id["doctor"]["requires_user_approval"] is False
    assert by_id["diagnose_flow"]["argv"] == ["atrade", "diagnose", "flow", "--json"]
    assert by_id["diagnose_flow"]["writes_state"] is False
    assert by_id["diagnose_flow"]["risk_level"] == "read_only"
    assert by_id["diagnose_flow"]["options"]["--include-account"]["default"] is False
    assert by_id["diagnose_strategy"]["command"] == "atrade diagnose strategy --json"
    assert by_id["diagnose_strategy"]["risk_level"] == "read_only"
    assert by_id["data_sources_diagnose"]["command"] == "atrade data-sources diagnose --json"
    assert by_id["data_sources_diagnose"]["risk_level"] == "read_only"
    assert by_id["digest"]["command"] == "atrade digest --json"
    assert by_id["digest"]["risk_level"] == "read_only"
    assert by_id["llm_context_close"]["command"] == "atrade llm-context --mode close --json"
    assert by_id["llm_context_close"]["risk_level"] == "read_only"
    assert by_id["llm_context_close"]["options"]["--mode"]["allowed_values"] == [
        "morning",
        "close",
        "weekly",
    ]
    assert by_id["screener_candidates"]["command"] == "atrade screener candidates --json"
    assert by_id["screener_candidates"]["risk_level"] == "read_only"
    assert by_id["stock_analyze"]["arguments"]["CODE_OR_NAME"]["description"] == "股票代码或名称"
    assert by_id["stock_analyze"]["risk_level"] == "read_only"
    assert by_id["risk_trial_guard"]["risk_level"] == "read_only"
    assert "候选池" in by_id["risk_trial_guard"]["description"]
    assert "profile" in by_id["risk_trial_guard"]["description"]
    assert by_id["screener_refresh"]["writes_state"] is True
    assert by_id["screener_refresh"]["state_events"] == [
        "candidate.added",
        "candidate.updated",
        "candidate.promoted",
        "candidate.rejected",
        "pool.demoted",
    ]
    assert by_id["opportunity_watch"]["options"]["--no-write"]["effect"] == "只比较，不更新机会监控状态文件"
    assert by_id["paper_trial_plan_record"]["writes_state"] is True
    assert by_id["paper_trial_plan_record"]["writes_order"] is False
    assert by_id["paper_trial_plan_record"]["state_events"] == ["paper.trial.recorded"]
    assert by_id["paper_trial_review_record"]["writes_state"] is True
    assert by_id["paper_trial_review_record"]["state_events"] == ["paper.trial.reviewed"]
    assert by_id["strategy_profile_activation_review"]["writes_state"] is False
    assert by_id["strategy_profile_activation_apply"]["writes_state"] is True
    assert by_id["strategy_profile_activation_apply"]["requires_user_approval"] is True
    assert by_id["strategy_profile_activation_apply"]["options"]["--apply-env"]["writes_environment"] is True
    assert by_id["run_pipeline_auto_trade"]["writes_order"] is True
    assert by_id["run_pipeline_auto_trade"]["requires_user_approval"] is True
    assert by_id["events_backfill_evidence_preview"]["command"] == "atrade events backfill-evidence --json"
    assert by_id["events_backfill_evidence_preview"]["writes_state"] is False
    assert by_id["events_backfill_evidence_preview"]["risk_level"] == "read_only"
    assert by_id["events_backfill_evidence_preview"]["options"]["--limit"]["default"] == 5000
    assert (
        by_id["events_backfill_evidence_apply"]["command"]
        == "atrade events backfill-evidence --apply --json"
    )
    assert by_id["events_backfill_evidence_apply"]["writes_state"] is True
    assert by_id["events_backfill_evidence_apply"]["writes_order"] is False
    assert by_id["events_backfill_evidence_apply"]["requires_user_approval"] is True
    assert by_id["events_backfill_evidence_apply"]["state_events"] == ["evidence.backfilled"]
    assert by_id["manual_trades_list"]["writes_state"] is False
    assert by_id["manual_trades_stale"]["command"] == "atrade manual-trades list --status stale --json"
    assert by_id["manual_trades_expire_stale"]["writes_state"] is True
    assert by_id["manual_trades_expire_stale"]["writes_order"] is False
    assert by_id["manual_trades_expire_stale"]["requires_user_approval"] is True
    assert by_id["manual_trades_expire_stale"]["state_events"] == ["manual_trade.expired"]
    assert by_id["record_buy"]["writes_state"] is True
    assert by_id["record_buy"]["requires_user_approval"] is True


def test_notify_manual_confirmation_dry_run_json(tmp_path):
    from astock_trading.platform.cli import app

    payload_path = tmp_path / "analysis.json"
    payload_path.write_text(json.dumps({
        "analysis": "stock",
        "status": "ok",
        "execution_allowed": False,
        "resolved": {"code": "600703", "name": "三安光电"},
        "quote": {"price": 12.3, "change_pct": 1.2},
        "score": {
            "total_score": 6.3,
            "data_quality": "ok",
            "entry_signal": True,
            "strategy_routes": [
                {"display_name": "放量突破", "confidence": 0.92, "entry_signal": True}
            ],
        },
        "decision": {
            "action": "BUY",
            "confidence": 6.3,
            "position_pct": 0.16,
            "market_signal": "GREEN",
        },
        "recommendations": [
            "manual confirmation required before any order; this report never executes trades"
        ],
    }, ensure_ascii=False), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "notify",
            "manual-confirmation",
            "--payload",
            str(payload_path),
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["notification"]["target"] == "discord"
    assert "人工确认" in payload["embed"]["title"]
    assert payload["analysis"]["resolved"]["code"] == "600703"


def test_notify_llm_summary_card_dry_run_json(tmp_path):
    from astock_trading.platform.cli import app

    payload_path = tmp_path / "llm-summary.md"
    payload_path.write_text("""## A股收盘复盘｜2026-05-17 15:55

**今日闭环：部分完成**
自动执行：禁止

### 1. 系统与数据质量
- 数据质量：降级（evidence_id: evt_data_1）

### 4. 盘前 vs 收盘
- 对比只用于复盘早盘判断质量，不作为自动交易依据（evidence_id: evt_compare_1）
""", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "notify",
            "llm-summary-card",
            "--mode",
            "close",
            "--payload",
            str(payload_path),
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["embed"]["title"] == "A股收盘复盘｜2026-05-17 15:55"
    assert payload["embed"]["fields"][0]["name"] == "今日闭环"
    assert payload["embed"]["fields"][2]["name"] == "🛡️ 系统与数据质量"
    assert payload["notification"]["target"] == "discord"


def test_notify_llm_summary_card_accepts_chinese_evidence_label(tmp_path):
    from astock_trading.platform.cli import app

    payload_path = tmp_path / "llm-summary.md"
    payload_path.write_text("""## A股收盘复盘｜2026-05-17 15:55

**今日闭环：部分完成**
自动执行：禁止

### 1. 系统与数据质量
- 数据质量：降级（证据编号：evt_data_1）

### 4. 盘前 vs 收盘
- 对比只用于复盘早盘判断质量，不作为自动交易依据（证据编号：evt_compare_1）
""", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "notify",
            "llm-summary-card",
            "--mode",
            "close",
            "--payload",
            str(payload_path),
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["evidence_validation"]["evidence_ids"] == ["evt_data_1", "evt_compare_1"]


def test_notify_llm_summary_card_accepts_unavailable_evidence_marker(tmp_path):
    from astock_trading.platform.cli import app

    payload_path = tmp_path / "llm-summary.md"
    payload_path.write_text("""## A股收盘复盘｜2026-05-17 15:55

**今日闭环：部分完成**
自动执行：禁止

### 1. 系统与数据质量
- 数据质量：降级
- 证据编号：暂无可用数据

### 4. 盘前 vs 收盘
- 对比只用于复盘早盘判断质量，不作为自动交易依据（证据编号：evt_compare_1）
""", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "notify",
            "llm-summary-card",
            "--mode",
            "close",
            "--payload",
            str(payload_path),
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["evidence_validation"]["evidence_ids"] == ["evt_compare_1"]
    assert payload["evidence_validation"]["unavailable_sections"] == ["系统与数据质量"]


def test_notify_llm_summary_card_rejects_missing_evidence_id(tmp_path):
    from astock_trading.platform.cli import app

    payload_path = tmp_path / "llm-summary.md"
    payload_path.write_text("""## A股收盘复盘｜2026-05-17 15:55

**今日闭环：部分完成**
自动执行：禁止

### 1. 系统与数据质量
- 数据质量：降级
""", encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "notify",
            "llm-summary-card",
            "--mode",
            "close",
            "--payload",
            str(payload_path),
            "--dry-run",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "failed"
    assert payload["evidence_validation"]["ok"] is False
    assert "缺少 evidence_id" in payload["error"]


def test_llm_context_markdown_includes_evidence_registry(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.llm_context import build_llm_context, render_llm_context_markdown

    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        event_id = EventStore(conn).append(
            "strategy:002138",
            "strategy",
            "decision.suggested",
            {"code": "002138", "action": "WATCH", "summary": "观察"},
        )

        payload = build_llm_context(conn, mode="close")
        markdown = render_llm_context_markdown(payload)
    finally:
        conn.close()

    assert "## 证据编号清单" in markdown
    assert f"evidence_id: {event_id}" in markdown
    assert "每个判断段落必须引用 evidence_id" in markdown


def test_daily_inspection_summary_keeps_pending_manual_trade_items():
    from astock_trading.platform.cli.notifications import _build_daily_inspection_summary

    summary = _build_daily_inspection_summary({
        "date": "2026-05-16",
        "results": [
            {
                "name": "manual_trades",
                "returncode": 0,
                "json": [
                    {
                        "status": "pending",
                        "side": "BUY",
                        "code": "600703",
                        "name": "三安光电",
                        "score": 6.3,
                        "position_pct": 0.16,
                    }
                ],
            }
        ],
        "route_blocked_watch_candidates": [
            {
                "code": "300558",
                "name": "贝达药业",
                "score": 6.2,
                "note": "screener_refresh:requires_entry_strategy_route",
            }
        ],
    })

    assert summary["pending_manual_trades"] == 1
    assert summary["pending_manual_trade_items"][0]["code"] == "600703"
    assert summary["route_blocked_watch_candidates"][0]["code"] == "300558"


def test_daily_inspection_summary_includes_opportunity_card():
    from astock_trading.platform.cli.notifications import _build_daily_inspection_summary

    summary = _build_daily_inspection_summary({
        "date": "2026-05-21",
        "results": [
            {
                "name": "opportunity",
                "returncode": 0,
                "json": {
                    "status": "needs_health_check",
                    "summary": "先修运行/数据问题，暂停新增交易判断。",
                    "decision_brief": "买入意向 0，核心候选 0，观察候选 0。",
                    "counts": {
                        "buy_intents": 0,
                        "watch_candidates": 0,
                        "core_candidates": 0,
                    },
                    "blockers": ["候选池为空", "核心池为空"],
                    "next_action": {
                        "label": "检查运行失败",
                        "command": "atrade health --json",
                    },
                },
            }
        ],
    })

    assert summary["opportunity_status"] == "needs_health_check"
    assert summary["opportunity_summary"] == "先修运行/数据问题，暂停新增交易判断。"
    assert summary["opportunity_counts"]["buy_intents"] == 0
    assert summary["opportunity_blockers"] == ["候选池为空", "核心池为空"]
    assert summary["opportunity_next_action"]["command"] == "atrade health --json"


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


def test_market_intel_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "market-intel", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "市场新闻" in result.stdout
    assert "brief" in result.stdout
    assert "search" in result.stdout
    assert "hot-stocks" in result.stdout
    assert "northbound" in result.stdout
    assert "fund-flow" in result.stdout
    assert "watchlist" in result.stdout


def test_risk_position_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "risk", "position", "002138", "7.5", "15.00", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["code"] == "002138"
    assert payload["score"] == 7.5
    assert payload["price"] == 15.0
    assert payload["shares"] > 0
    assert payload["shares"] % 100 == 0


def test_risk_trial_guard_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "risk", "trial-guard", "--capital", "500000", "--amount", "60000", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "breached"
    assert payload["manual_confirmation_required"] is True
    assert payload["real_broker_integration"] == "disabled"
    assert payload["trial_position_cap"]["cap_pct"] == 0.1
    assert payload["trial_position_cap"]["cap_amount"] == 50000
    assert payload["checked_order"]["within_cap"] is False


def test_risk_trial_guard_surfaces_candidate_flow_and_profile_gate(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    env_file = tmp_path / ".env"
    env_file.write_text(f"ASTOCK_DATABASE_URL=sqlite:///{db_path}\n", encoding="utf-8")
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        EventStore(conn).append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "data_quality": "ok",
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
            },
            metadata={"run_id": "risk_trial_context"},
        )
        EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            payload={
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "manual_confirmation_required": True,
                    "export_command": "export ASTOCK_CONFIG_PROFILE=trend_swing",
                    "verify_command": "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json",
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                    "modifies_environment": False,
                },
            },
            metadata={"source": "test"},
        )
    finally:
        conn.close()

    env = _cli_env(tmp_path)
    env["ASTOCK_ENV_FILE"] = str(env_file)
    env.pop("ASTOCK_CONFIG_PROFILE", None)
    result = subprocess.run(
        [str(cli), "risk", "trial-guard", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "profile_review_required"
    assert payload["summary"] == (
        "试运行护栏未通过：运行 profile 仍需人工确认；"
        "候选池 1 只：核心 1、观察 0、强势观察 0；当前入场信号 1 只。"
    )
    assert payload["candidate_summary"] == payload["candidate_flow"]["candidate_summary"]
    assert payload["current_entry_signals"] == payload["candidate_flow"]["current_entry_signals"]
    assert payload["candidate_flow"]["candidate_summary"] == {
        "total": 1,
        "core_count": 1,
        "watch_count": 0,
        "radar_count": 0,
        "entry_signal_count": 1,
        "summary": "候选池 1 只：核心 1、观察 0、强势观察 0；当前入场信号 1 只。",
    }
    assert payload["candidate_flow"]["current_entry_signals"] == [
        {
            "code": "688981",
            "name": "中芯国际",
            "pool_tier": "core",
            "pool_tier_label": "核心",
            "score": 6.4,
            "entry_signal": True,
            "primary_strategy_route": "flow_confirmed_trend",
            "primary_strategy_route_label": "资金趋势确认",
            "data_quality": "ok",
            "review_command": "atrade stock analyze 688981 --json",
        }
    ]
    assert payload["execution_profile"]["status"] == "review_required"
    assert payload["execution_profile"]["effective_profile"] == "default"
    assert payload["blockers"] == [
        {
            "reason": "profile_review_required",
            "label": "运行 profile 仍需人工确认",
            "command": "atrade strategy profile-activation --target trend_swing --json",
        }
    ]
    assert payload["next_action"]["command"] == "atrade strategy profile-activation --target trend_swing --json"
    assert payload["next_action"]["command_contract_id"] == "strategy_profile_activation_review"
    assert payload["next_action"]["writes_order"] is False


def test_risk_check_json_reports_missing_position_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "risk", "check", "002138", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload == {"status": "not_held", "code": "002138", "signals": []}


def test_market_intel_hot_stocks_json(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.market_intel as market_intel_cli

    class FakeMarketService:
        async def collect_hot_stocks(self, trade_date=None, run_id=None):
            return [{"code": "002138", "name": "双环传动", "reason": "机器人"}]

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        market_svc = FakeMarketService()
        conn = FakeConn()

    monkeypatch.setattr(market_intel_cli, "build_context", lambda: FakeContext())

    result = CliRunner().invoke(
        app,
        ["market-intel", "hot-stocks", "--trade-date", "2026-05-17", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["trade_date"] == "2026-05-17"
    assert payload["count"] == 1
    assert payload["stocks"][0]["code"] == "002138"


def test_market_intel_brief_json(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.market_intel as market_intel_cli

    class FakeMarketService:
        async def collect_finance_flash(self, limit=20, run_id=None):
            return [{"time": "09:01", "title": "机器人板块走强", "source": "eastmoney"}]

        async def collect_global_risk_news(self, limit=12, run_id=None):
            return [{"title": "Fed rate cut expectations fade", "source": "bloomberg"}]

        async def collect_cross_platform_hot_stocks(self, limit=10, run_id=None):
            return [{"rank": 1, "name": "双环传动", "code": "002472", "source_count": 3}]

        async def collect_hot_sectors(self, limit=10, sector_type="industry", sort="change", run_id=None):
            return [{
                "rank": 1,
                "name": "机器人",
                "type": sector_type,
                "sort": sort,
                "change_pct": 3.21,
                "lead_stock": "双环传动",
            }]

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        market_svc = FakeMarketService()
        conn = FakeConn()

    monkeypatch.setattr(market_intel_cli, "build_context", lambda: FakeContext())

    result = CliRunner().invoke(
        app,
        ["market-intel", "brief", "--query", "今天热点新闻和强势板块", "--limit", "2", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["query"] == "今天热点新闻和强势板块"
    assert payload["finance_flash"][0]["title"] == "机器人板块走强"
    assert payload["hot_stocks"][0]["code"] == "002472"
    assert payload["strong_sectors"][0]["name"] == "机器人"
    assert payload["money_flow_sectors"][0]["sort"] == "money-flow"


def test_market_intel_brief_falls_back_to_sector_heatmap(monkeypatch):
    from astock_trading.platform.cli import app
    import astock_trading.platform.cli.market_intel as market_intel_cli

    class FakeMarketService:
        async def collect_finance_flash(self, limit=20, run_id=None):
            return []

        async def collect_global_risk_news(self, limit=12, run_id=None):
            return []

        async def collect_cross_platform_hot_stocks(self, limit=10, run_id=None):
            return []

        async def collect_hot_sectors(self, limit=10, sector_type="industry", sort="change", run_id=None):
            return []

        async def collect_sector_heatmap(self, run_id=None):
            return [{"name": "机器人", "change_pct": 3.21, "amount": 123000000, "up_count": 42, "down_count": 3}]

    class FakeConn:
        def close(self):
            pass

    class FakeContext:
        market_svc = FakeMarketService()
        conn = FakeConn()

    monkeypatch.setattr(market_intel_cli, "build_context", lambda: FakeContext())

    result = CliRunner().invoke(app, ["market-intel", "brief", "--limit", "2", "--no-global", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["strong_sectors"][0]["name"] == "机器人"
    assert payload["strong_sectors"][0]["source"] == "sector_heatmap"


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
    assert len(candidates) == 1
    assert {
        "code": candidates[0]["code"],
        "pool_tier": candidates[0]["pool_tier"],
        "name": candidates[0]["name"],
        "score": candidates[0]["score"],
        "streak_days": candidates[0]["streak_days"],
        "note": candidates[0]["note"],
    } == {
        "code": "002138",
        "pool_tier": "core",
        "name": "双环传动",
        "score": 7.2,
        "streak_days": 0,
        "note": "manual_promote",
    }
    assert "entry_signal" in candidates[0]
    assert "primary_strategy_route_label" in candidates[0]


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


def test_record_buy_json_accepts_decision_signal_and_manual_reason_aliases(tmp_path):
    from astock_trading.platform.db import connect
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"

    result = subprocess.run(
        [
            str(cli),
            "record-buy",
            "002138",
            "100",
            "15.00",
            "--name",
            "双环传动",
            "--decision-id",
            "decision_evt_1",
            "--signal-id",
            "score_evt_1",
            "--manual-reason",
            "人工确认突破后回踩不破",
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
    conn = connect(db_path)
    try:
        events = EventStore(conn).query(stream=f"trade:002138:{payload['order_id']}")
    finally:
        conn.close()
    hypothesis = next(event["payload"] for event in events if event["event_type"] == "trade.hypothesis.recorded")
    assert hypothesis["source_event_id"] == "decision_evt_1"
    assert hypothesis["source_score_event_id"] == "score_evt_1"
    assert hypothesis["hypothesis"]["manual_reason"] == "人工确认突破后回踩不破"


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


def test_review_shadow_json_reports_paper_real_deviation_via_bin_trade(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.domain_events import AUTO_TRADE_EXECUTED
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        event_id = EventStore(conn).append(
            stream="paper:002138",
            stream_type="paper_trade",
            event_type=AUTO_TRADE_EXECUTED,
            payload={
                "side": "buy",
                "code": "002138",
                "name": "双环传动",
                "shares": 100,
                "price": 10.0,
                "status": "filled",
                "source_score_event_id": "score_cli_1",
            },
            metadata={"run_id": "paper_cli", "account": "paper"},
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-18T10:00:00+08:00", event_id),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "review", "shadow", "--date", "2026-05-18", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["summary"]["paper_trades"] == 1
    assert payload["summary"]["real_trades"] == 0
    assert payload["summary"]["deviation_types"] == {"not_executed": 1}
    assert payload["items"][0]["join_key"]["signal_id"] == "score_cli_1"
    assert payload["items"][0]["rule_deviation"] == "shadow_divergence"


def test_hermes_digest_suggest_explain_json_via_bin_trade(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        score_event_id = store.append(
            stream="score:002138",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "002138",
                "name": "双环传动",
                "total_score": 7.8,
                "technical_score": 8.1,
                "fundamental_score": 7.2,
                "flow_score": 7.5,
                "sentiment_score": 6.8,
                "data_quality": "ok",
                "entry_signal": True,
                "veto_triggered": False,
            },
            metadata={"run_id": "scoring_cli"},
        )
        decision_event_id = store.append(
            stream="decision:002138",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "002138",
                "name": "双环传动",
                "action": "BUY",
                "score": 7.8,
                "confidence": 0.76,
                "source_score_event_id": score_event_id,
                "veto_reasons": [],
                "notes": ["入场信号成立"],
            },
            metadata={"run_id": "scoring_cli"},
        )
        manual_event_id = store.append(
            stream="manual_trade:002138",
            stream_type="manual_trade",
            event_type="manual_trade.requested",
            payload={
                "status": "pending",
                "side": "buy",
                "code": "002138",
                "name": "双环传动",
                "score": 7.8,
                "source_event_id": decision_event_id,
                "source_score_event_id": score_event_id,
            },
            metadata={"run_id": "scoring_cli", "account": "main", "execution": "manual"},
        )
        conn.executemany(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            [
                ("2026-05-22T05:59:58+00:00", score_event_id),
                ("2026-05-22T05:59:59+00:00", decision_event_id),
                ("2026-05-22T06:00:00+00:00", manual_event_id),
            ],
        )
    finally:
        conn.close()

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "strategy.yaml").write_text(
        """
scoring:
  weights:
    technical: 4.0
    fundamental: 2.5
    flow: 3.0
    sentiment: 0.5
  thresholds:
    buy: 6.0
    watch: 5.0
    reject: 4.0
manual_confirmation:
  pending_max_age_hours: 24
  buy_window:
    end: "23:59"
""",
        encoding="utf-8",
    )
    env = _cli_env(tmp_path)
    env["ASTOCK_CONFIG_DIR"] = str(config_dir)
    env["ASTOCK_TEST_NOW"] = "2026-05-22T14:05:00+08:00"
    digest = subprocess.run(
        [str(cli), "digest", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    suggest = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    explain = subprocess.run(
        [str(cli), "explain", "002138", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    digest_payload = json.loads(digest.stdout)
    suggest_payload = json.loads(suggest.stdout)
    explain_payload = json.loads(explain.stdout)
    assert digest_payload["status"] == "needs_manual_confirmation"
    assert "待人工确认 1" in digest_payload["summary"]
    assert suggest_payload["next_action"]["command"] == "atrade manual-trades list --json"
    assert suggest_payload["next_action"]["command_contract_id"] == "manual_trades_list"
    assert suggest_payload["next_action"]["writes_state"] is False
    assert suggest_payload["next_action"]["writes_order"] is False
    assert suggest_payload["next_action"]["risk_level"] == "read_only"
    assert suggest_payload["execution_allowed"] is False
    assert explain_payload["code"] == "002138"
    assert explain_payload["latest_decision"]["action"] == "BUY"
    assert "买入意向" in explain_payload["summary"]
    assert explain_payload["next_action"]["command"] == "atrade manual-trades list --json"
    assert explain_payload["next_action"]["command_contract_id"] == "manual_trades_list"
    assert explain_payload["next_action"]["writes_state"] is False
    assert explain_payload["next_action"]["writes_order"] is False
    assert explain_payload["next_action"]["risk_level"] == "read_only"


def test_hermes_suggest_pauses_new_trade_judgment_when_l1_coverage_degraded(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.history_mirror import archive_signal_history

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-20", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002138", {"items": [1]})
        store.save_observation(
            "market_service",
            "snapshot",
            "002138",
            {
                "code": "002138",
                "name": "双环传动",
                "completeness": {
                    "has_quote": True,
                    "has_technical": True,
                    "has_financial": True,
                    "has_flow": False,
                    "has_sentiment": True,
                    "has_sector": True,
                },
            },
            run_id="screener_l1_degraded",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-20",
            history_group_id="hist_l1_degraded",
            run_id="screener_l1_degraded",
            phase="screener",
            candidates=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 7.8,
                    "data_quality": "degraded",
                    "data_missing_fields": ["资金流"],
                }
            ],
            decisions=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "action": "BUY",
                    "score": 7.8,
                }
            ],
        )
        EventStore(conn).append(
            stream="decision:002138",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={"code": "002138", "name": "双环传动", "action": "BUY", "score": 7.8},
            metadata={"run_id": "screener_l1_degraded"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "needs_health_check"
    assert payload["recommendation"] == "先修运行/数据问题，暂停新增交易判断。"
    assert payload["next_action"]["command"] == "atrade data-sources diagnose --json"
    assert payload["next_action"]["command_contract_id"] == "data_sources_diagnose"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["risk_level"] == "read_only"
    assert payload["data_source_blockers"][0]["reason"] == "latest_screener_l1_coverage_degraded"
    assert payload["execution_allowed"] is False


def test_hermes_suggest_reports_score_quality_blocker_separately(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.history_mirror import archive_signal_history

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-20", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002384", {"items": [1]})
        store.save_observation(
            "market_service",
            "snapshot",
            "002384",
            {
                "code": "002384",
                "name": "东山精密",
                "completeness": {
                    "has_quote": True,
                    "has_technical": True,
                    "has_financial": True,
                    "has_flow": True,
                    "has_sentiment": True,
                    "has_sector": True,
                },
            },
            run_id="screener_score_degraded",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-20",
            history_group_id="hist_score_degraded",
            run_id="screener_score_degraded",
            phase="screener",
            candidates=[
                {
                    "code": "002384",
                    "name": "东山精密",
                    "total_score": 6.2,
                    "data_quality": "degraded",
                    "data_missing_fields": ["ROE", "营收", "现金流"],
                }
            ],
            decisions=[
                {
                    "code": "002384",
                    "name": "东山精密",
                    "action": "BUY",
                    "score": 6.2,
                }
            ],
        )
        EventStore(conn).append(
            stream="decision:002384",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={"code": "002384", "name": "东山精密", "action": "BUY", "score": 6.2},
            metadata={"run_id": "screener_score_degraded"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "needs_health_check"
    assert payload["data_source_blockers"][0]["reason"] == "latest_screener_score_quality_degraded"
    assert payload["next_action"]["reason"] == "最近筛选评分数据质量降级，先诊断数据源再看新增交易。"
    assert payload["execution_allowed"] is False


def test_hermes_suggest_reports_empty_pool_as_no_qualified_candidates(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.history_mirror import archive_signal_history

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-20", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002138", {"items": [1]})
        store.save_observation(
            "market_service",
            "snapshot",
            "002138",
            {
                "code": "002138",
                "name": "双环传动",
                "completeness": {
                    "has_quote": True,
                    "has_technical": True,
                    "has_financial": True,
                    "has_flow": True,
                    "has_sentiment": True,
                    "has_sector": True,
                },
            },
            run_id="screener_no_qualified",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-20",
            history_group_id="hist_no_qualified",
            run_id="screener_no_qualified",
            phase="screener",
            candidates=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 4.1,
                    "data_quality": "ok",
                    "data_missing_fields": [],
                    "entry_signal": False,
                }
            ],
        )
        EventStore(conn).append(
            stream="score:002138",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "002138",
                "name": "双环传动",
                "total_score": 4.1,
                "data_quality": "ok",
                "entry_signal": False,
            },
            metadata={"run_id": "screener_no_qualified"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "wait_no_qualified_candidates"
    assert payload["recommendation"] == "核心数据源可用，候选池为空；继续观察，不降低买入线。"
    assert payload["next_action"]["type"] == "observe_no_qualified_candidates"
    assert "暂无合格候选" in payload["next_action"]["label"]
    assert "不是行情没数据" in payload["next_action"]["reason"]
    assert payload["next_action"]["command_contract_id"] == "screener_explain"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["risk_level"] == "read_only"


def test_hermes_opportunity_json_reports_no_qualified_candidates(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.history_mirror import archive_signal_history

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-20", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002138", {"items": [1]})
        archive_signal_history(
            conn,
            snapshot_date="2026-05-20",
            history_group_id="hist_opportunity_empty",
            run_id="screener_opportunity_empty",
            phase="screener",
            candidates=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 4.1,
                    "data_quality": "ok",
                    "entry_signal": False,
                }
            ],
        )
        EventStore(conn).append(
            stream="score:002138",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "002138",
                "name": "双环传动",
                "total_score": 4.1,
                "data_quality": "ok",
                "entry_signal": False,
            },
            metadata={"run_id": "screener_opportunity_empty"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["command"] == "opportunity"
    assert payload["status"] == "wait_no_qualified_candidates"
    assert payload["execution_allowed"] is False
    assert payload["manual_confirmation_required"] is True
    assert payload["counts"]["buy_intents"] == 0
    assert payload["counts"]["watch_candidates"] == 0
    assert payload["watch_candidates"] == []
    assert "暂无合格候选" in payload["summary"]
    assert "不降低买入线" in payload["decision_brief"]


def test_hermes_opportunity_does_not_block_on_provider_failures_outside_active_candidates(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("688981", "core", "中芯国际", 6.4, "2026-05-22", "2026-05-22", 3, ""),
        )
        store.save_provider_failure(
            source="AkShareFlowAdapter",
            target_kind="fund_flow",
            symbol="300475",
            status="timeout",
            error_type="TimeoutError",
            error_message="provider 超时",
            run_id="screener_other_symbol",
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    blockers = " ".join(payload["blockers"])
    assert payload["status"] == "wait"
    assert payload["summary"] == "核心候选 1 只；等待入场信号，不自动买入。"
    assert payload["next_action"]["type"] == "paper_trial_plan"
    assert payload["next_action"]["command"] == "atrade paper trial-plan --json"
    assert payload["next_action"]["command_contract_id"] == "paper_trial_plan"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["risk_level"] == "read_only"
    assert "provider 失败" not in blockers
    assert "L1 数据源失败未补齐" not in blockers
    assert all(
        "provider 失败未被 fallback 补齐" not in finding
        for finding in payload["diagnostics"]["data_sources"]["findings"]
    )


def test_hermes_opportunity_uses_chinese_label_for_active_l1_provider_blocker(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("300475", "watch", "香农芯创", 5.3, "2026-05-22", "2026-05-22", 1, ""),
        )
        store.save_provider_failure(
            source="AkShareFlowAdapter",
            target_kind="fund_flow",
            symbol="300475",
            status="timeout",
            error_type="TimeoutError",
            error_message="provider 超时",
            run_id="screener_active_symbol",
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    blockers = " ".join(payload["blockers"])
    assert payload["status"] == "needs_health_check"
    assert "L1 数据源失败未补齐" in blockers
    assert "unresolved_l1_provider_failures" not in blockers


def test_hermes_suggest_ignores_historical_failed_runs_for_current_opportunity(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.history_mirror import archive_signal_history

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, finished_at, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "old_failed_run",
                "evening",
                "cn_a",
                "test",
                "failed",
                "2000-01-01T00:00:00+00:00",
                "2000-01-01T00:01:00+00:00",
                "历史失败，不应阻断当前机会判断",
            ),
        )
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "603376", {"items": [1]})
        archive_signal_history(
            conn,
            snapshot_date="2026-05-22",
            history_group_id="hist_current_empty",
            run_id="screener_current_empty",
            phase="screener",
            candidates=[
                {
                    "code": "603376",
                    "name": "大明电子",
                    "total_score": 4.1,
                    "data_quality": "ok",
                    "entry_signal": False,
                }
            ],
        )
        EventStore(conn).append(
            stream="score:603376",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "603376",
                "name": "大明电子",
                "total_score": 4.1,
                "data_quality": "ok",
                "entry_signal": False,
            },
            metadata={"run_id": "screener_current_empty"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "wait_no_qualified_candidates"
    assert payload["digest"]["failed_runs"] == []


def test_hermes_suggest_ignores_recovered_failed_runs_for_current_opportunity(tmp_path):
    from astock_trading.platform.db import connect, init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        rows = [
            (
                "auto_trade_failed",
                "auto_trade",
                "cn_a",
                "test",
                "failed",
                "2026-05-22T06:22:40+00:00",
                "2026-05-22T06:26:10+00:00",
                "stale running cleaned up after 0h",
            ),
            (
                "auto_trade_recovered",
                "auto_trade",
                "cn_a",
                "test",
                "completed",
                "2026-05-22T06:42:04+00:00",
                "2026-05-22T06:42:07+00:00",
                None,
            ),
        ]
        conn.executemany(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, finished_at, error_message)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        conn.commit()
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["digest"]["failed_runs"] == []
    assert "失败运行 0" in payload["digest"]["summary"]


def test_hermes_opportunity_json_highlights_radar_candidates(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "603376", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "002384",
                "name": "东山精密",
                "pool_tier": "watch",
                "score": 5.5,
                "note": "screener_refresh",
            },
            {
                "code": "603376",
                "name": "大明电子",
                "pool_tier": "radar",
                "score": 4.8,
                "note": "screener_refresh:below_watch_retained",
            }
        ])
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["counts"]["radar_candidates"] == 1
    assert payload["counts"]["watch_candidates"] == 1
    assert payload["watch_candidates"][0]["note_label"] == "筛选刷新入池"
    assert payload["radar_candidates"][0]["code"] == "603376"
    assert payload["radar_candidates"][0]["pool_tier_label"] == "强势观察"
    assert payload["radar_candidates"][0]["note_label"] == "低于观察线，保留跟踪"
    assert payload["summary"] == "观察候选 1 只，强势观察 1 只；等待入场信号，不自动买入。"
    assert "强势观察 1" in payload["decision_brief"]
    assert payload["execution_allowed"] is False
    assert payload["next_action"]["type"] == "paper_trial_plan"
    assert payload["next_action"]["command"] == "atrade paper trial-plan --json"


def test_hermes_opportunity_json_surfaces_core_candidates_in_summary(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
            {
                "code": "688372",
                "name": "伟测科技",
                "pool_tier": "watch",
                "score": 5.7,
                "note": "screener_refresh",
            },
        ])
        EventStore(conn).append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "data_quality": "ok",
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
            },
            metadata={"run_id": "score_entry_signal"},
        )
        event_id = EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            payload={
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "manual_confirmation_required": True,
                    "export_command": "export ASTOCK_CONFIG_PROFILE=trend_swing",
                    "verify_command": "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json",
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                    "modifies_environment": False,
                },
            },
            metadata={"source": "test"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )
    suggest_result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    suggest_payload = json.loads(suggest_result.stdout)
    assert payload["counts"]["core_candidates"] == 1
    assert payload["counts"]["watch_candidates"] == 1
    assert payload["candidate_summary"]["summary"] == (
        "候选池 2 只：核心 1、观察 1、强势观察 0；当前入场信号 1 只。"
    )
    assert payload["candidate_summary"]["top_core_candidate"]["code"] == "688981"
    assert payload["candidate_summary"]["top_core_candidate"]["entry_signal"] is True
    assert payload["candidate_summary"]["top_watch_candidate"]["code"] == "688372"
    assert payload["candidate_summary"]["top_radar_candidate"] == {}
    assert payload["summary"] == "核心候选 1 只，观察候选 1 只；等待入场信号，不自动买入。"
    assert payload["watch_candidates"][0]["pool_tier_label"] == "核心"
    assert payload["watch_candidates"][0]["entry_signal"] is True
    assert payload["watch_candidates"][0]["primary_strategy_route"] == "flow_confirmed_trend"
    assert payload["watch_candidates"][0]["primary_strategy_route_label"] == "资金趋势确认"
    assert payload["watch_candidates"][0]["technical_detail"] == "金叉成立，资金确认"
    assert payload["watch_candidates"][1]["pool_tier_label"] == "观察"
    profile_activation = payload["diagnostics"]["profile_activation"]
    assert profile_activation["latest_request"]["event_id"] == event_id
    assert profile_activation["latest_request"]["target_profile"] == "trend_swing"
    assert "已记录待人工确认的 trend_swing profile 激活计划" in payload["blockers"]
    assert suggest_payload["candidate_summary"]["summary"] == payload["candidate_summary"]["summary"]
    assert suggest_payload["current_entry_signals"] == [
        payload["candidate_summary"]["top_core_candidate"]
    ]


def test_hermes_opportunity_prioritizes_runtime_profile_review_for_stale_core_buy_signal(
    tmp_path,
    monkeypatch,
):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    env_file = tmp_path / ".env"
    jobs_path = tmp_path / "jobs.json"
    scripts_dir = tmp_path / "scripts"
    monkeypatch.setenv("ASTOCK_TEST_NOW", "2026-05-23T10:00:00+08:00")
    env_file.write_text(f"ASTOCK_DATABASE_URL=sqlite:///{db_path}\n", encoding="utf-8")
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    scripts_dir.mkdir(parents=True)
    (scripts_dir / "a_stock_screener_refresh_intraday_silent.sh").write_text(
        '#!/usr/bin/env bash\n"$(dirname "$0")/a_stock_screener_refresh_silent.sh"\n',
        encoding="utf-8",
    )
    (scripts_dir / "a_stock_screener_refresh_silent.sh").write_text(
        "#!/usr/bin/env bash\natrade screener refresh --json\n",
        encoding="utf-8",
    )
    (scripts_dir / "a_stock_pipeline_auto_trade_silent.sh").write_text(
        "#!/usr/bin/env bash\natrade run-pipeline auto_trade --json\n",
        encoding="utf-8",
    )
    (scripts_dir / "a_stock_intraday_execution_cycle_silent.sh").write_text(
        (
            "#!/usr/bin/env bash\n"
            "atrade paper auto-readiness --json\n"
            '"$(dirname "$0")/a_stock_pipeline_auto_trade_silent.sh"\n'
        ),
        encoding="utf-8",
    )
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        event_store = EventStore(conn)
        score_event_id = event_store.append(
            stream="score:688981",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "data_quality": "ok",
                "entry_signal": True,
                "veto_triggered": False,
            },
            metadata={"run_id": "old_manual"},
        )
        decision_event_id = event_store.append(
            stream="decision:688981",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "source_score_event_id": score_event_id,
                "veto_reasons": [],
                "notes": ["入场信号成立"],
            },
            metadata={"run_id": "old_manual"},
        )
        manual_event_id = event_store.append(
            stream="manual_trade:688981",
            stream_type="manual_trade",
            event_type="manual_trade.requested",
            payload={
                "status": "pending",
                "side": "buy",
                "code": "688981",
                "name": "中芯国际",
                "score": 6.4,
                "source_event_id": decision_event_id,
                "source_score_event_id": score_event_id,
            },
            metadata={"run_id": "old_manual", "account": "main", "execution": "manual"},
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-01T01:00:00+00:00", manual_event_id),
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-23T01:30:00+00:00", decision_event_id),
        )
        event_store.append(
            stream="paper_trial:2026-05-22:688981",
            stream_type="paper_trial",
            event_type="paper.trial.recorded",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "pool_tier_label": "核心",
                "trial_date": "2026-05-22",
                "trial_start_price": 10.0,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-plan", "shadow_only": True},
        )
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-profile-gate-positive",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
        event_store.append(
            stream="strategy:profile_activation",
            stream_type="strategy",
            event_type="strategy.profile_activation.requested",
            payload={
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "manual_confirmation_required": True,
                    "export_command": "export ASTOCK_CONFIG_PROFILE=trend_swing",
                    "verify_command": "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json",
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                    "modifies_environment": False,
                },
            },
            metadata={"source": "test"},
        )
    finally:
        conn.close()

    env = _cli_env(tmp_path)
    env["ASTOCK_ENV_FILE"] = str(env_file)
    env["ASTOCK_HERMES_JOBS_PATH"] = str(jobs_path)
    env.pop("ASTOCK_CONFIG_PROFILE", None)
    watch_state_file = tmp_path / "opportunity-watch-state.json"
    watch_state_file.write_text(
        json.dumps({
            "snapshot": {
                "date": "2026-05-22",
                "counts": {
                    "buy_intents": 0,
                    "core_candidates": 1,
                    "watch_candidates": 0,
                    "radar_candidates": 0,
                    "all_candidates": 1,
                },
                "candidate_keys": ["core:688981"],
                "core_keys": ["core:688981"],
                "watch_keys": [],
                "radar_keys": [],
                "candidates": [
                    {
                        "code": "688981",
                        "name": "中芯国际",
                        "pool_tier": "core",
                        "score": 6.4,
                    }
                ],
            }
        }),
        encoding="utf-8",
    )
    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    digest_result = subprocess.run(
        [str(cli), "digest", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    suggest_result = subprocess.run(
        [str(cli), "suggest", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    agent_context_result = subprocess.run(
        [str(cli), "agent-context", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    watch_result = subprocess.run(
        [str(cli), "opportunity-watch", "--state-file", str(watch_state_file), "--no-write", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    digest_payload = json.loads(digest_result.stdout)
    suggest_payload = json.loads(suggest_result.stdout)
    agent_context_payload = json.loads(agent_context_result.stdout)
    watch_payload = json.loads(watch_result.stdout)
    assert digest_payload["status"] == "profile_review_required"
    assert digest_payload["attention"] == {
        "status": "profile_review_required",
        "label": "复核运行 profile 激活",
        "summary": "已有核心候选和过期买入意向；模拟承接前先复核运行 profile。",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }
    assert "先复核运行 profile" in digest_payload["summary"]
    assert payload["status"] == "profile_review_required"
    assert payload["counts"]["buy_intents"] == 0
    assert payload["counts"]["stale_buy_intents"] == 1
    assert payload["counts"]["recent_unusable_buy_signals"] == 1
    assert payload["summary"] == (
        "核心候选 1 只，近期买入意向 1 条不可承接；最高分为 中芯国际(688981) 6.4 分，"
        "原因：买入意向发生日或当前检查日不是交易日；模拟承接前先复核运行 profile。"
    )
    assert payload["current_entry_signals"] == [
        {
            "code": "688981",
            "name": "中芯国际",
            "pool_tier": "core",
            "pool_tier_label": "核心",
            "score": 6.4,
            "entry_signal": True,
            "primary_strategy_route_label": None,
            "technical_detail": "",
            "review_command": "atrade stock analyze 688981 --json",
        }
    ]
    assert payload["recent_unusable_buy_signal"]["top"]["code"] == "688981"
    assert payload["after_approval_preview"]["available"] is True
    assert payload["after_approval_preview"]["target_profile"] == "trend_swing"
    assert payload["after_approval_preview"]["preview_command"] == (
        "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --skip-account --json"
    )
    assert payload["after_approval_preview"]["post_approval_verify_command"] == (
        "atrade paper auto-readiness --json"
    )
    assert payload["after_approval_preview"]["schedule_verify_command"] == "atrade diagnose schedule --json"
    assert "当前核心候选已有入场信号" in payload["after_approval_preview"]["summary"]
    assert payload["after_approval_preview"]["signal_gap"]["status"] == (
        "entry_signal_without_fresh_buy_intent"
    )
    assert payload["after_approval_preview"]["signal_gap"]["next_action"]["command"] == (
        "atrade stock analyze 688981 --json"
    )
    assert payload["after_approval_preview"]["recent_unusable_buy_signal"]["top"]["code"] == "688981"
    assert payload["after_approval_preview"]["writes_environment"] is False
    assert payload["after_approval_preview"]["places_order"] is False
    assert "已有买入意向；" not in payload["summary"]
    assert payload["next_action"] == {
        "type": "review_runtime_profile_activation",
        "label": "复核运行 profile 激活",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "reason": "已有核心候选和过期买入意向，但运行环境仍会使用 default；先人工确认 trend_swing profile。",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }
    assert payload["evidence_actions"] == [
        {
            "type": "record_positive_trial_review",
            "label": "记录影子试运行复盘",
            "command": "atrade paper trial-review --min-age-days 0 --record --json",
            "reason": "有 1 只影子试运行表现为正但尚未记录复盘；可先写入影子复盘证据，不提交模拟盘订单。",
            "safe_to_auto_apply": True,
            "writes_state": True,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "state_write",
            "command_contract_id": "paper_trial_review_record",
        }
    ]
    assert payload["diagnostics"]["schedule"]["runtime_profile"]["status"] == "review_required"
    assert payload["diagnostics"]["schedule"]["runtime_profile"]["effective_profile"] == "default"
    assert any("运行环境仍会使用 default" in item for item in payload["blockers"])
    assert payload["approval_gate"] == {
        "required": True,
        "type": "profile_activation_apply",
        "label": "人工确认写入运行 profile",
        "reason": "当前 default 混合配置阻断自动模拟；需要人工批准后写入 ASTOCK_CONFIG_PROFILE=trend_swing。",
        "target_profile": "trend_swing",
        "review_command": "atrade strategy profile-activation --target trend_swing --json",
        "apply_command": "atrade strategy profile-activation --target trend_swing --apply-env --yes --json",
        "verify_command": "atrade diagnose schedule --json",
        "safe_to_auto_apply": False,
        "modifies_environment_after_approval": True,
        "review_command_contract_id": "strategy_profile_activation_review",
        "review_command_contract": {
            "id": "strategy_profile_activation_review",
            "risk_level": "read_only",
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "state_events": [],
        },
        "apply_command_contract_id": "strategy_profile_activation_apply",
        "apply_command_contract": {
            "id": "strategy_profile_activation_apply",
            "risk_level": "environment_write",
            "writes_state": True,
            "writes_environment": True,
            "writes_order": False,
            "requires_user_approval": True,
            "state_events": ["strategy.profile_activation.applied"],
        },
        "verify_command_contract_id": "diagnose_schedule",
        "verify_command_contract": {
            "id": "diagnose_schedule",
            "risk_level": "read_only",
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "state_events": [],
        },
    }
    assert payload["next_window_plan"]["status"] == "requires_profile_approval_before_next_window"
    assert payload["next_window_plan"]["current_signal"]["code"] == "688981"
    assert payload["next_window_plan"]["current_signal"]["carries_to_next_window"] is False
    assert payload["next_window_plan"]["next_window_requires_fresh_buy_signal"] is True
    assert [item["script"] for item in payload["next_window_plan"]["scheduled_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert [item["pending_first_run"] for item in payload["next_window_plan"]["scheduled_steps"]] == [
        True,
        True,
        True,
    ]
    assert [
        item["critical_for_intraday_simulation"]
        for item in payload["next_window_plan"]["scheduled_steps"]
    ] == [False, True, True]
    assert payload["next_window_plan"]["first_run_verification"]["required"] is True
    assert payload["next_window_plan"]["first_run_verification"]["critical_required"] is True
    assert payload["next_window_plan"]["first_run_verification"]["verify_command_contract_id"] == "diagnose_schedule"
    assert payload["next_window_plan"]["first_run_verification"]["verify_command_contract"] == {
        "id": "diagnose_schedule",
        "risk_level": "read_only",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "state_events": [],
    }
    assert [item["script"] for item in payload["next_window_plan"]["first_run_verification"]["pending_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert payload["next_window_plan"]["next_action"]["command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert suggest_payload["approval_gate"] == payload["approval_gate"]
    assert suggest_payload["next_window_plan"]["status"] == "requires_profile_approval_before_next_window"
    assert suggest_payload["next_window_plan"]["current_signal"]["code"] == "688981"
    assert suggest_payload["next_window_plan"]["first_run_verification"]["verify_command_contract_id"] == (
        "diagnose_schedule"
    )
    operator_attention = agent_context_payload["operator_attention"]
    assert operator_attention["status"] == "profile_review_required"
    assert operator_attention["current_action"]["type"] == "review_runtime_profile_activation"
    assert operator_attention["current_action"]["label"] == "复核运行 profile 激活"
    assert operator_attention["current_action"]["command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert operator_attention["summary"] == payload["summary"]
    assert operator_attention["current_action"]["safe_to_auto_apply"] is False
    assert operator_attention["current_action"]["writes_state"] is False
    assert operator_attention["current_action"]["writes_environment"] is False
    assert operator_attention["current_action"]["writes_order"] is False
    assert operator_attention["current_action"]["requires_user_approval"] is False
    assert operator_attention["current_action"]["risk_level"] == "read_only"
    assert operator_attention["current_action"]["command_contract_id"] == "strategy_profile_activation_review"
    assert operator_attention["current_action"]["command_contract"] == {
        "id": "strategy_profile_activation_review",
        "risk_level": "read_only",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "state_events": [],
    }
    assert operator_attention["approval_gate"]["apply_command"] == (
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
    )
    assert operator_attention["approval_gate"]["review_command_contract_id"] == "strategy_profile_activation_review"
    assert operator_attention["approval_gate"]["review_command_contract"]["risk_level"] == "read_only"
    assert operator_attention["approval_gate"]["review_command_contract"]["writes_environment"] is False
    assert operator_attention["approval_gate"]["apply_command_contract_id"] == "strategy_profile_activation_apply"
    assert operator_attention["approval_gate"]["apply_command_contract"]["risk_level"] == "environment_write"
    assert operator_attention["approval_gate"]["apply_command_contract"]["writes_environment"] is True
    assert operator_attention["approval_gate"]["apply_command_contract"]["requires_user_approval"] is True
    assert operator_attention["approval_gate"]["safe_to_auto_apply"] is False
    assert operator_attention["after_approval_preview"]["available"] is True
    assert operator_attention["after_approval_preview"]["target_profile"] == "trend_swing"
    assert operator_attention["after_approval_preview"]["preview_command"] == (
        "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --skip-account --json"
    )
    assert "当前核心候选已有入场信号" in operator_attention["after_approval_preview"]["summary"]
    assert operator_attention["after_approval_preview"]["recent_unusable_buy_signal"]["top"]["code"] == (
        "688981"
    )
    assert operator_attention["after_approval_preview"]["signal_gap"]["status"] == (
        "entry_signal_without_fresh_buy_intent"
    )
    assert operator_attention["after_approval_preview"]["signal_gap"]["next_action"]["command"] == (
        "atrade stock analyze 688981 --json"
    )
    assert operator_attention["after_approval_preview"]["writes_environment"] is False
    assert operator_attention["after_approval_preview"]["places_order"] is False
    assert operator_attention["evidence_actions"][0]["command"] == (
        "atrade paper trial-review --min-age-days 0 --record --json"
    )
    assert operator_attention["evidence_actions"][0]["command_contract_id"] == "paper_trial_review_record"
    assert operator_attention["evidence_actions"][0]["command_contract"]["risk_level"] == "state_write"
    assert operator_attention["next_window_plan"]["current_signal"]["carries_to_next_window"] is False
    assert operator_attention["next_window_plan"]["next_window_requires_fresh_buy_signal"] is True
    assert (
        operator_attention["next_window_plan"]["first_run_verification"]["verify_command_contract_id"]
        == "diagnose_schedule"
    )
    assert operator_attention["next_window_plan"]["first_run_verification"]["verify_command_contract"] == {
        "id": "diagnose_schedule",
        "risk_level": "read_only",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "state_events": [],
    }
    assert operator_attention["runtime_contract"]["status"] == "ok"
    assert operator_attention["runtime_contract"]["scope"] == "next_window_simulation_scripts"
    assert operator_attention["runtime_contract"]["blocking_issues"] == []
    assert [
        item["script"] for item in operator_attention["runtime_contract"]["script_checks"]
    ] == [
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
        "a_stock_screener_refresh_intraday_silent.sh",
    ]
    assert operator_attention["follow_up_commands"] == [
        "atrade diagnose flow --json",
        "atrade opportunity --json",
        "atrade digest --json",
        "atrade paper auto-readiness --json",
        "atrade risk trial-guard --json",
    ]
    assert any("--apply-env --yes" in item for item in operator_attention["guardrails"])
    assert watch_payload["status"] == "changed"
    assert watch_payload["should_notify"] is True
    assert "operator_action_required" in watch_payload["change_types"]
    assert "当前动作需要处理" in watch_payload["change_labels"]
    assert watch_payload["next_action"] == {
        "label": "复核运行 profile 激活",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "reason": "已有核心候选和过期买入意向，但运行环境仍会使用 default；先人工确认 trend_swing profile。",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }
    assert watch_payload["opportunity"]["next_window_plan"]["status"] == (
        "requires_profile_approval_before_next_window"
    )
    assert watch_payload["opportunity"]["after_approval_preview"]["available"] is True
    assert "当前核心候选已有入场信号" in watch_payload["opportunity"]["after_approval_preview"]["summary"]
    assert watch_payload["opportunity"]["after_approval_preview"]["schedule_verify_command"] == (
        "atrade diagnose schedule --json"
    )
    assert watch_payload["opportunity"]["after_approval_preview"]["writes_environment"] is False
    assert watch_payload["opportunity"]["after_approval_preview"]["places_order"] is False
    assert watch_payload["opportunity"]["next_window_plan"]["current_signal"]["carries_to_next_window"] is False
    assert watch_payload["opportunity"]["recent_unusable_buy_signal"]["top"]["code"] == "688981"
    assert watch_payload["snapshot"]["attention_key"].startswith("profile_review_required|")


def test_hermes_surfaces_recent_unusable_buy_signal_in_digest_and_opportunity(tmp_path, monkeypatch):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.hermes_commands import build_digest, build_opportunity_card
    from astock_trading.reporting.projectors import ProjectionUpdater

    monkeypatch.setenv("ASTOCK_TEST_NOW", "2026-05-23T10:00:00+08:00")
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        score_event_id = events.append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            {
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "data_quality": "ok",
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
            },
            metadata={"run_id": "weekend-score"},
        )
        decision_event_id = events.append(
            "strategy:688981",
            "strategy",
            "decision.suggested",
            {
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "source_score_event_id": score_event_id,
            },
            metadata={"run_id": "weekend-score"},
        )
        manual_event_id = events.append(
            "manual_trade:688981",
            "manual_trade",
            "manual_trade.requested",
            {
                "status": "pending",
                "side": "buy",
                "code": "688981",
                "name": "中芯国际",
                "score": 6.4,
                "source_event_id": decision_event_id,
                "source_score_event_id": score_event_id,
            },
            metadata={"run_id": "weekend-score", "execution": "manual"},
        )
        weekend_buy_at = "2026-05-22T17:44:18+00:00"
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id IN (?, ?)",
            (weekend_buy_at, decision_event_id, manual_event_id),
        )
        events.append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {"auto_apply": False},
                "guardrails": {"auto_apply": False, "manual_approval_required": True},
            },
            metadata={"source": "test"},
        )

        digest = build_digest(conn)
        opportunity = build_opportunity_card(conn)
    finally:
        conn.close()

    recent = digest["recent_unusable_buy_signal"]
    assert recent["count"] == 1
    assert recent["top"]["code"] == "688981"
    assert recent["top"]["entry_signal"] is True
    assert recent["top"]["primary_strategy_route_label"] == "资金趋势确认"
    assert recent["top"]["unusable_reason"] == "non_trading_day"
    assert "近期买入意向 1 条不可承接" in digest["summary"]
    assert "中芯国际(688981) 6.4 分" in digest["summary"]
    assert "买入意向发生日或当前检查日不是交易日" in digest["summary"]
    assert opportunity["recent_unusable_buy_signal"] == recent
    assert opportunity["counts"]["recent_unusable_buy_signals"] == 1
    assert "近期买入意向 1 条不可承接" in opportunity["summary"]
    assert "买入意向发生日或当前检查日不是交易日" in opportunity["summary"]


def test_hermes_opportunity_prioritizes_runtime_profile_review_for_pending_core_buy_signal(
    tmp_path,
):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    env_file = tmp_path / ".env"
    config_dir = tmp_path / "config"
    jobs_path = tmp_path / "jobs.json"
    today = local_today_str()
    env_file.write_text(f"ASTOCK_DATABASE_URL=sqlite:///{db_path}\n", encoding="utf-8")
    config_dir.mkdir()
    (config_dir / "strategy.yaml").write_text(
        """
scoring:
  weights:
    technical: 4.0
    fundamental: 2.5
    flow: 3.0
    sentiment: 0.5
  thresholds:
    buy: 6.0
    watch: 5.0
    reject: 4.0
manual_confirmation:
  pending_max_age_hours: 24
  buy_window:
    end: "23:59"
""",
        encoding="utf-8",
    )
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", today, {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        event_store = EventStore(conn)
        score_event_id = event_store.append(
            stream="score:688981",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "data_quality": "ok",
                "entry_signal": True,
                "veto_triggered": False,
            },
            metadata={"run_id": "pending_manual"},
        )
        decision_event_id = event_store.append(
            stream="decision:688981",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "source_score_event_id": score_event_id,
                "veto_reasons": [],
                "notes": ["入场信号成立"],
            },
            metadata={"run_id": "pending_manual"},
        )
        manual_event_id = event_store.append(
            stream="manual_trade:688981",
            stream_type="manual_trade",
            event_type="manual_trade.requested",
            payload={
                "status": "pending",
                "side": "buy",
                "code": "688981",
                "name": "中芯国际",
                "score": 6.4,
                "source_event_id": decision_event_id,
                "source_score_event_id": score_event_id,
            },
            metadata={"run_id": "pending_manual", "account": "main", "execution": "manual"},
        )
        conn.executemany(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            [
                ("2026-05-22T05:59:58+00:00", score_event_id),
                ("2026-05-22T05:59:59+00:00", decision_event_id),
                ("2026-05-22T06:00:00+00:00", manual_event_id),
            ],
        )
        event_store.append(
            stream="strategy:profile_activation",
            stream_type="strategy",
            event_type="strategy.profile_activation.requested",
            payload={
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "manual_confirmation_required": True,
                    "export_command": "export ASTOCK_CONFIG_PROFILE=trend_swing",
                    "verify_command": "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json",
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                    "modifies_environment": False,
                },
            },
            metadata={"source": "test"},
        )
    finally:
        conn.close()

    env = _cli_env(tmp_path)
    env["ASTOCK_CONFIG_DIR"] = str(config_dir)
    env["ASTOCK_ENV_FILE"] = str(env_file)
    env["ASTOCK_HERMES_JOBS_PATH"] = str(jobs_path)
    env["ASTOCK_TEST_NOW"] = "2026-05-22T14:05:00+08:00"
    env.pop("ASTOCK_CONFIG_PROFILE", None)

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    digest_result = subprocess.run(
        [str(cli), "digest", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    agent_context_result = subprocess.run(
        [str(cli), "agent-context", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    digest_payload = json.loads(digest_result.stdout)
    agent_context_payload = json.loads(agent_context_result.stdout)
    assert digest_payload["pending_manual_trades"] == 1
    assert digest_payload["stale_manual_trades"] == 0
    assert digest_payload["status"] == "profile_review_required"
    assert digest_payload["attention"]["command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert payload["status"] == "profile_review_required"
    assert payload["counts"]["buy_intents"] == 1
    assert payload["summary"] == "核心候选 1 只，已有买入意向；模拟承接前先复核运行 profile。"
    assert payload["next_action"]["type"] == "review_runtime_profile_activation"
    assert payload["approval_gate"]["required"] is True
    assert payload["next_window_plan"]["status"] == "requires_profile_approval_before_next_window"
    assert payload["next_window_plan"]["next_window_requires_fresh_buy_signal"] is True
    operator_attention = agent_context_payload["operator_attention"]
    assert operator_attention["status"] == "profile_review_required"
    assert operator_attention["current_action"]["command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert operator_attention["approval_gate"]["apply_command_contract_id"] == (
        "strategy_profile_activation_apply"
    )


def test_hermes_opportunity_json_surfaces_positive_shadow_trials(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    today = local_today_str()
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "600584", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        EventStore(conn).append(
            stream=f"paper_trial_review:{today}:600584",
            stream_type="paper_trial",
            event_type="paper.trial.reviewed",
            payload={
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "radar",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 9.04,
                "trial_start_price": 66.84,
                "current_price": 72.88,
                "current_pool_tier": "core",
                "current_pool_tier_label": "核心",
                "current_score": 6.4,
                "candidate_state_changed": True,
                "candidate_state_change_label": "观察 -> 核心",
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-review", "shadow_only": True},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "review_positive_trial"
    assert payload["counts"]["positive_trial_candidates"] == 1
    assert payload["positive_trial_candidates"][0]["code"] == "600584"
    assert payload["positive_trial_candidates"][0]["return_pct"] == 9.04
    assert payload["positive_trial_candidates"][0]["current_pool_tier"] == "core"
    assert payload["positive_trial_candidates"][0]["current_score"] == 6.4
    assert payload["positive_trial_candidates"][0]["candidate_state_change_label"] == "强势观察 -> 核心"
    assert payload["next_action"]["type"] == "review_positive_trial"
    assert payload["next_action"]["command"] == "atrade stock analyze 600584 --json"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_environment"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False
    assert payload["next_action"]["risk_level"] == "read_only"
    assert payload["next_action"]["command_contract_id"] == "stock_analyze"
    assert "影子试运行表现为正" in payload["summary"]


def test_hermes_opportunity_prioritizes_actionable_positive_shadow_trials(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    today = local_today_str()
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        updater = ProjectionUpdater(None, conn)
        updater.sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.9,
                "note": "screener_refresh",
            },
        ])
        event_store = EventStore(conn)
        event_store.append(
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
                "technical_detail": "资金趋势确认",
                "data_quality": "ok",
            },
        )
        event_store.append(
            f"paper_trial_review:{today}:688981",
            "paper_trial",
            "paper.trial.reviewed",
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 6.0,
                "trial_start_price": 10.0,
                "current_price": 10.6,
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
        )
        event_store.append(
            f"paper_trial_review:{today}:600584",
            "paper_trial",
            "paper.trial.reviewed",
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 12.0,
                "trial_start_price": 10.0,
                "current_price": 11.2,
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "review_positive_trial"
    assert [item["code"] for item in payload["positive_trial_candidates"][:2]] == [
        "688981",
        "600584",
    ]
    assert payload["positive_trial_candidates"][0]["current_pool_tier"] == "core"
    assert payload["positive_trial_candidates"][0]["current_entry_signal"] is True
    assert payload["positive_trial_candidates"][1]["return_pct"] == 12.0
    assert payload["next_action"]["command"] == "atrade stock analyze 688981 --json"


def test_hermes_opportunity_refreshes_recorded_positive_trial_from_current_pool(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    today = local_today_str()
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "600584", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.9,
                "note": "screener_refresh",
            },
        ])
        EventStore(conn).append(
            stream=f"paper_trial_review:{today}:600584",
            stream_type="paper_trial",
            event_type="paper.trial.reviewed",
            payload={
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "radar",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 9.04,
                "trial_start_price": 66.84,
                "current_price": 72.88,
                "current_pool_tier": "core",
                "current_pool_tier_label": "核心",
                "current_score": 6.4,
                "candidate_state_changed": True,
                "candidate_state_change_label": "强势观察 -> 核心",
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-review", "shadow_only": True},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    item = payload["positive_trial_candidates"][0]
    assert item["code"] == "600584"
    assert item["current_pool_tier"] == "watch"
    assert item["current_pool_tier_label"] == "观察"
    assert item["current_score"] == 5.9
    assert item["active_candidate"] is True
    assert item["candidate_state_changed"] is True
    assert item["candidate_state_change_label"] == "强势观察 -> 观察"


def test_hermes_opportunity_positive_shadow_trial_exposes_profile_gate_and_next_window(
    tmp_path,
):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    env_file = tmp_path / ".env"
    jobs_path = tmp_path / "jobs.json"
    today = local_today_str()
    env_file.write_text(f"ASTOCK_DATABASE_URL=sqlite:///{db_path}\n", encoding="utf-8")
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "600584", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.8,
                "note": "screener_refresh",
            },
        ])
        event_store = EventStore(conn)
        event_store.append(
            stream=f"paper_trial_review:{today}:600584",
            stream_type="paper_trial",
            event_type="paper.trial.reviewed",
            payload={
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "radar",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 9.04,
                "trial_start_price": 66.84,
                "current_price": 72.88,
                "current_pool_tier": "watch",
                "current_pool_tier_label": "观察",
                "current_score": 5.8,
                "candidate_state_changed": True,
                "candidate_state_change_label": "强势观察 -> 观察",
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-review", "shadow_only": True},
        )
        event_store.append(
            stream="strategy:profile_activation",
            stream_type="strategy",
            event_type="strategy.profile_activation.requested",
            payload={
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "manual_confirmation_required": True,
                    "export_command": "export ASTOCK_CONFIG_PROFILE=trend_swing",
                    "verify_command": "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json",
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                    "modifies_environment": False,
                },
            },
            metadata={"source": "test"},
        )
    finally:
        conn.close()

    env = _cli_env(tmp_path)
    env["ASTOCK_ENV_FILE"] = str(env_file)
    env["ASTOCK_HERMES_JOBS_PATH"] = str(jobs_path)
    env.pop("ASTOCK_CONFIG_PROFILE", None)
    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    agent_context_result = subprocess.run(
        [str(cli), "agent-context", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    agent_context = json.loads(agent_context_result.stdout)
    assert payload["status"] == "review_positive_trial"
    assert payload["approval_gate"]["required"] is True
    assert payload["approval_gate"]["review_command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert payload["next_window_plan"]["status"] == "requires_profile_approval_before_next_window"
    assert payload["next_window_plan"]["current_signal"] == {}
    assert payload["next_window_plan"]["next_window_requires_fresh_buy_signal"] is True
    assert [item["script"] for item in payload["next_window_plan"]["scheduled_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert payload["next_window_plan"]["first_run_verification"]["required"] is True
    assert payload["next_window_plan"]["first_run_verification"]["critical_required"] is True
    assert payload["next_window_plan"]["first_run_verification"]["verify_command_contract_id"] == "diagnose_schedule"
    assert payload["next_window_plan"]["first_run_verification"]["verify_command_contract"] == {
        "id": "diagnose_schedule",
        "risk_level": "read_only",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "state_events": [],
    }
    assert [item["script"] for item in payload["next_window_plan"]["first_run_verification"]["pending_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    current_action = agent_context["operator_attention"]["current_action"]
    assert current_action["command"] == "atrade stock analyze 600584 --json"
    assert current_action["command_contract_id"] == "stock_analyze"
    assert current_action["writes_state"] is False
    assert current_action["writes_environment"] is False
    assert current_action["writes_order"] is False
    assert current_action["requires_user_approval"] is False
    assert current_action["risk_level"] == "read_only"
    assert current_action["command_contract"]["risk_level"] == "read_only"
    assert current_action["command_contract"]["requires_user_approval"] is False
    assert agent_context["operator_attention"]["approval_gate"]["apply_command_contract_id"] == (
        "strategy_profile_activation_apply"
    )


def test_hermes_opportunity_previews_unrecorded_positive_shadow_trials(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
        assert EventStore(conn).query(event_type="paper.trial.reviewed") == []
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "review_positive_trial"
    assert payload["counts"]["positive_trial_candidates"] == 1
    assert payload["counts"]["active_positive_trial_candidates"] == 1
    assert payload["positive_trial_candidates"][0]["code"] == "688981"
    assert payload["positive_trial_candidates"][0]["return_pct"] == 8.0
    assert payload["positive_trial_candidates"][0]["review_recorded"] is False
    assert payload["next_action"] == {
        "type": "record_positive_trial_review",
        "label": "记录影子试运行复盘",
        "command": "atrade paper trial-review --min-age-days 0 --record --json",
        "reason": "只读复盘已发现表现为正的影子候选；先写入复盘证据，再人工复核，不自动晋级或下单。",
        "safe_to_auto_apply": True,
        "writes_state": True,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "state_write",
        "command_contract_id": "paper_trial_review_record",
    }


def test_hermes_opportunity_merges_recorded_and_preview_positive_shadow_trials(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    today = local_today_str()
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "600584", {"items": [1]})
        updater = ProjectionUpdater(None, conn)
        updater.sync_candidate_pool([
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.9,
                "note": "screener_refresh",
            },
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.8,
                "note": "screener_refresh",
            },
        ])
        event_store = EventStore(conn)
        event_store.append(
            stream="strategy:600584",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "600584",
                "name": "长电科技",
                "total_score": 5.9,
                "entry_signal": True,
                "primary_strategy_route": "ma_golden_cross",
                "strategy_routes": [
                    {
                        "route": "ma_golden_cross",
                        "display_name": "均线金叉",
                        "family": "trend_swing",
                        "confidence": 0.72,
                        "entry_signal": True,
                    }
                ],
                "technical_detail": "金叉:1.0/1",
                "data_quality": "ok",
            },
            metadata={"source": "test"},
        )
        event_store.append(
            stream="strategy:688981",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.8,
                "entry_signal": True,
                "primary_strategy_route": "flow_confirmed_trend",
                "strategy_routes": [
                    {
                        "route": "flow_confirmed_trend",
                        "display_name": "资金趋势确认",
                        "family": "trend_swing",
                        "confidence": 0.81,
                        "entry_signal": True,
                    }
                ],
                "technical_detail": "资金趋势确认",
                "data_quality": "ok",
            },
            metadata={"source": "test"},
        )
        event_store.append(
            stream=f"paper_trial_review:{today}:600584",
            stream_type="paper_trial",
            event_type="paper.trial.reviewed",
            payload={
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "radar",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 9.04,
                "trial_start_price": 66.84,
                "current_price": 72.88,
                "current_pool_tier": "watch",
                "current_pool_tier_label": "观察",
                "current_score": 5.9,
                "candidate_state_changed": True,
                "candidate_state_change_label": "强势观察 -> 观察",
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-review", "shadow_only": True},
        )
        event_store.append(
            stream=f"paper_trial:{today}:688981",
            stream_type="paper_trial",
            event_type="paper.trial.recorded",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "pool_tier_label": "核心",
                "score": 6.8,
                "trial_date": today,
                "trial_start_price": 10.0,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-plan", "shadow_only": True},
        )
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-preview-current",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 11.2, "close": 11.2}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "review_positive_trial"
    assert payload["counts"]["positive_trial_candidates"] == 2
    assert payload["counts"]["active_positive_trial_candidates"] == 2
    assert [item["code"] for item in payload["positive_trial_candidates"]] == ["688981", "600584"]
    assert payload["positive_trial_candidates"][0]["review_recorded"] is False
    assert payload["positive_trial_candidates"][0]["review_source"] == "paper.trial-review.preview"
    assert payload["positive_trial_candidates"][0]["current_entry_signal"] is True
    assert payload["positive_trial_candidates"][0]["current_primary_strategy_route"] == "flow_confirmed_trend"
    assert payload["positive_trial_candidates"][0]["current_primary_strategy_route_label"] == "资金趋势确认"
    assert payload["positive_trial_candidates"][1]["review_recorded"] is True
    assert payload["positive_trial_candidates"][1]["current_entry_signal"] is True
    assert payload["positive_trial_candidates"][1]["current_primary_strategy_route"] == "ma_golden_cross"
    assert payload["positive_trial_candidates"][1]["current_primary_strategy_route_label"] == "均线金叉"
    assert payload["next_action"]["type"] == "record_positive_trial_review"
    assert payload["next_action"]["writes_state"] is True
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["risk_level"] == "state_write"
    assert payload["next_action"]["command_contract_id"] == "paper_trial_review_record"


def test_hermes_opportunity_does_not_prioritize_removed_positive_trial(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    today = local_today_str()
    init_db(db_path)
    conn = connect(db_path)
    try:
        market_store = MarketStore(conn)
        market_store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        market_store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        market_store.save_observation("AkShareFlowAdapter", "fund_flow", "600584", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        event_store = EventStore(conn)
        score_event_id = event_store.append(
            stream="score:688981",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "data_quality": "ok",
                "entry_signal": True,
                "veto_triggered": False,
            },
            metadata={"run_id": "old_manual"},
        )
        decision_event_id = event_store.append(
            stream="decision:688981",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "source_score_event_id": score_event_id,
                "veto_reasons": [],
                "notes": ["入场信号成立"],
            },
            metadata={"run_id": "old_manual"},
        )
        manual_event_id = event_store.append(
            stream="manual_trade:688981",
            stream_type="manual_trade",
            event_type="manual_trade.requested",
            payload={
                "status": "pending",
                "side": "buy",
                "code": "688981",
                "name": "中芯国际",
                "score": 6.4,
                "source_event_id": decision_event_id,
                "source_score_event_id": score_event_id,
            },
            metadata={"run_id": "old_manual", "account": "main", "execution": "manual"},
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-01T01:00:00+00:00", manual_event_id),
        )
        event_store.append(
            stream="decision:300475",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "300475",
                "name": "香农芯创",
                "action": "CLEAR",
                "score": 4.8,
                "veto_reasons": ["未达买入线"],
                "notes": ["等待下一轮入场信号"],
            },
            metadata={"run_id": "later_clear"},
        )
        event_store.append(
            stream=f"paper_trial_review:{today}:600584",
            stream_type="paper_trial",
            event_type="paper.trial.reviewed",
            payload={
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "radar",
                "trial_date": today,
                "review_date": today,
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 9.04,
                "trial_start_price": 66.84,
                "current_price": 72.88,
                "price_anomaly": False,
                "paper_order_submitted": False,
            },
            metadata={"source": "paper.trial-review", "shadow_only": True},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "opportunity", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "paper_auto_readiness"
    assert payload["summary"] == "核心候选 1 只，已有买入意向；先检查模拟盘自动交易预检。"
    assert payload["counts"]["buy_intents"] == 0
    assert payload["counts"]["stale_buy_intents"] == 1
    assert payload["counts"]["active_positive_trial_candidates"] == 0
    assert payload["counts"]["inactive_positive_trial_candidates"] == 1
    assert payload["stale_buy_intents"][0]["code"] == "688981"
    assert payload["positive_trial_candidates"][0]["candidate_state_changed"] is True
    assert payload["positive_trial_candidates"][0]["candidate_state_change_label"] == "强势观察 -> 已移出候选池"
    assert payload["next_action"]["type"] == "paper_auto_readiness"
    assert payload["next_action"]["command"] == "atrade paper auto-readiness --json"
    assert "模拟盘自动交易预检" in payload["next_action"]["reason"]
    assert any("已移出候选池" in item for item in payload["blockers"])
    assert any("过期" in item or "错过" in item for item in payload["blockers"])


def test_manual_trades_expire_stale_appends_auditable_resolution(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        event_store = EventStore(conn)
        manual_event_id = event_store.append(
            stream="manual_trade:688981",
            stream_type="manual_trade",
            event_type="manual_trade.requested",
            payload={
                "status": "pending",
                "side": "buy",
                "code": "688981",
                "name": "中芯国际",
                "score": 6.4,
            },
            metadata={"run_id": "old_manual", "account": "main", "execution": "manual"},
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-01T01:00:00+00:00", manual_event_id),
        )
    finally:
        conn.close()

    expired = subprocess.run(
        [str(cli), "manual-trades", "expire-stale", "--max-age-hours", "4", "--yes", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )
    pending = subprocess.run(
        [str(cli), "manual-trades", "list", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )
    all_items = subprocess.run(
        [str(cli), "manual-trades", "list", "--status", "all", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    expired_payload = json.loads(expired.stdout)
    all_payload = json.loads(all_items.stdout)
    assert expired_payload["status"] == "success"
    assert expired_payload["expired_count"] == 1
    assert expired_payload["expired"][0]["code"] == "688981"
    assert json.loads(pending.stdout) == []
    assert all_payload[0]["status"] == "expired"
    assert all_payload[0]["resolution"]["reason"] == "stale_confirmation"


def test_paper_trial_plan_json_surfaces_watch_and_radar_candidates(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
            {
                "code": "300475",
                "name": "香农芯创",
                "pool_tier": "radar",
                "score": 4.9,
                "note": "screener_refresh:below_watch_retained",
            },
        ])
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-plan", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["command"] == "paper trial-plan"
    assert payload["status"] == "ready"
    assert payload["execution_allowed"] is False
    assert payload["manual_confirmation_required"] is True
    assert payload["counts"]["trial_candidates"] == 2
    assert payload["candidate_summary"]["total"] == 2
    assert payload["candidate_summary"]["core_count"] == 0
    assert payload["candidate_summary"]["watch_count"] == 1
    assert payload["candidate_summary"]["radar_count"] == 1
    assert payload["candidate_summary"]["entry_signal_count"] == 0
    assert payload["candidate_summary"]["top_watch_candidate"]["code"] == "688981"
    assert payload["candidate_summary"]["top_radar_candidate"]["code"] == "300475"
    assert payload["current_entry_signals"] == []
    assert payload["candidates"][0]["code"] == "688981"
    assert payload["candidates"][0]["pool_tier_label"] == "观察"
    assert payload["candidates"][0]["trial_mode"] == "影子试运行"
    assert payload["candidates"][0]["review_command"] == "atrade stock analyze 688981 --json"
    assert payload["candidates"][1]["pool_tier_label"] == "强势观察"
    assert payload["next_action"]["command"] == "atrade stock analyze 688981 --json"
    assert payload["next_action"]["command_contract_id"] == "stock_analyze"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_environment"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False
    assert payload["next_action"]["risk_level"] == "read_only"


def test_paper_trial_plan_empty_next_action_marks_screener_refresh_as_state_write(tmp_path):
    from astock_trading.platform.db import init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)

    result = subprocess.run(
        [str(cli), "paper", "trial-plan", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "empty"
    assert payload["next_action"]["command"] == "atrade screener refresh --json"
    assert payload["next_action"]["command_contract_id"] == "screener_refresh"
    assert payload["next_action"]["writes_state"] is True
    assert payload["next_action"]["writes_environment"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False
    assert payload["next_action"]["risk_level"] == "state_write"


def test_paper_auto_readiness_json_reports_paper_order_mode(tmp_path):
    from astock_trading.platform.db import init_db

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    init_db(db_path)
    (config_dir / "strategy.yaml").write_text(
        """
auto_trade:
  enabled: true
  dry_run: false
  buy_window:
    start: "09:45"
    end: "14:30"
  sell_window:
    start: "09:35"
    end: "14:50"
risk:
  position:
    total_max: 0.60
    single_max: 0.20
    weekly_max: 2
scoring:
  thresholds:
    buy: 6.0
    watch: 5.0
    reject: 4.0
""",
        encoding="utf-8",
    )
    env = _cli_env(tmp_path)
    env["ASTOCK_CONFIG_DIR"] = str(config_dir)

    result = subprocess.run(
        [str(cli), "paper", "auto-readiness", "--skip-account", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["command"] == "paper auto-readiness"
    assert payload["mode"] == "mx_paper_order"
    assert payload["paper_order_submission_enabled"] is True
    assert payload["paper_account"]["status"] == "skipped"
    assert payload["guardrails"]["real_order_auto_execution_allowed"] is False


def test_paper_trial_plan_json_includes_entry_signal_evidence(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        event_store = EventStore(conn)
        ProjectionUpdater(event_store, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
        ])
        event_store.append(
            stream="strategy:688981",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "688981",
                "total_score": 6.4,
                "entry_signal": True,
                "primary_strategy_route": "ma_golden_cross",
                "strategy_routes": [
                    {
                        "route": "ma_golden_cross",
                        "display_name": "均线金叉",
                        "family": "trend_swing",
                        "confidence": 0.78,
                        "entry_signal": True,
                    },
                ],
                "technical_detail": "金叉:1.0/1 量比:0.5/0.5(1.3)",
                "data_quality": "ok",
            },
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-plan", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    candidate = payload["candidates"][0]
    assert candidate["entry_signal"] is True
    assert candidate["primary_strategy_route"] == "ma_golden_cross"
    assert candidate["primary_strategy_route_label"] == "均线金叉"
    assert candidate["technical_detail"].startswith("金叉")
    assert candidate["trial_reason"] == "核心候选，已有入场信号：均线金叉；本计划仍不自动下单。"
    assert payload["candidate_summary"]["entry_signal_count"] == 1
    assert payload["candidate_summary"]["top_core_candidate"]["code"] == "688981"
    assert payload["current_entry_signals"] == [payload["candidate_summary"]["top_core_candidate"]]


def test_paper_trial_plan_record_writes_shadow_trial_events(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["recorded_count"] == 1
    assert payload["guardrails"]["paper_order_submitted"] is False

    conn = connect(db_path)
    try:
        events = EventStore(conn).query(event_type="paper.trial.recorded")
    finally:
        conn.close()
    assert len(events) == 1
    assert events[0]["stream"] == f"paper_trial:{payload['date']}:688981"
    assert events[0]["payload"]["code"] == "688981"
    assert events[0]["payload"]["paper_order_allowed"] is False
    assert events[0]["metadata"]["source"] == "paper.trial-plan"


def test_paper_trial_plan_record_includes_start_price_from_latest_snapshot(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "snapshot",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"quote": {"price": 10.0, "close": 10.0}}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["candidates"][0]["trial_start_price"] == 10.0
    assert payload["candidates"][0]["trial_start_price_source"] == "market_observations.snapshot"

    conn = connect(db_path)
    try:
        events = EventStore(conn).query(event_type="paper.trial.recorded")
    finally:
        conn.close()
    assert events[0]["payload"]["trial_start_price"] == 10.0
    assert events[0]["payload"]["trial_start_price_source"] == "market_observations.snapshot"


def test_paper_trial_review_json_reports_shadow_candidate_outcome(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["execution_allowed"] is False
    assert payload["recorded_count"] == 1
    assert payload["summary"]["trial_count"] == 1
    assert payload["summary"]["positive_count"] == 1
    assert payload["review_summary"] == payload["summary"]
    assert len(payload["positive_reviews"]) == 1
    assert payload["positive_reviews"][0]["code"] == "688981"
    assert payload["positive_reviews"][0]["review_status"] == "positive"
    assert payload["positive_reviews"][0]["next_action"] == "atrade stock analyze 688981 --json"
    assert payload["positive_reviews"][0]["paper_order_submitted"] is False
    assert payload["items"][0]["code"] == "688981"
    assert payload["items"][0]["trial_start_price"] == 10.0
    assert payload["items"][0]["current_price"] == 10.8
    assert payload["items"][0]["return_pct"] == 8.0
    assert payload["items"][0]["review_status"] == "positive"
    assert payload["items"][0]["paper_order_submitted"] is False
    assert payload["next_action"]["command"] == "atrade stock analyze 688981 --json"
    assert payload["next_action"]["command_contract_id"] == "stock_analyze"
    assert payload["next_action"]["writes_state"] is False
    assert payload["next_action"]["writes_environment"] is False
    assert payload["next_action"]["writes_order"] is False
    assert payload["next_action"]["requires_user_approval"] is False
    assert payload["next_action"]["risk_level"] == "read_only"

    conn = connect(db_path)
    try:
        events = EventStore(conn).query(event_type="paper.trial.reviewed")
    finally:
        conn.close()
    assert len(events) == 1
    assert events[0]["payload"]["review_status"] == "positive"


def test_paper_trial_review_reports_current_candidate_state_after_promotion(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        updater = ProjectionUpdater(None, conn)
        updater.sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "promoted_after_entry_signal",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    item = json.loads(result.stdout)["items"][0]
    assert item["pool_tier"] == "watch"
    assert item["score"] == 5.6
    assert item["current_pool_tier"] == "core"
    assert item["current_pool_tier_label"] == "核心"
    assert item["current_score"] == 6.4
    assert item["candidate_state_changed"] is True
    assert item["candidate_state_change_label"] == "观察 -> 核心"


def test_paper_trial_review_marks_removed_candidate_as_state_change(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-removed-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute("DELETE FROM projection_candidate_pool WHERE code = ?", ("688981",))
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-removed-trial-current",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    item = json.loads(result.stdout)["items"][0]
    assert item["pool_tier"] == "watch"
    assert item["current_pool_tier"] is None
    assert item["current_pool_tier_label"] == "已移出候选池"
    assert item["current_score"] is None
    assert item["candidate_state_changed"] is True
    assert item["candidate_state_change_label"] == "观察 -> 已移出候选池"


def test_paper_trial_review_marks_same_day_impossible_return_as_price_anomaly(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 30.0, "close": 30.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["summary"]["positive_count"] == 0
    assert payload["summary"]["price_anomaly_count"] == 1
    assert payload["items"][0]["return_pct"] == 200.0
    assert payload["items"][0]["review_status"] == "price_anomaly"
    assert payload["items"][0]["price_anomaly"] is True
    assert "超过 40%" in payload["items"][0]["price_anomaly_reason"]
    assert payload["next_action"]["type"] == "inspect_price_anomaly"


def test_paper_trial_review_skips_latest_outlier_when_recent_stable_price_exists(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current-stable",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:00:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current-outlier",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 30.0, "close": 30.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["summary"]["price_anomaly_count"] == 0
    assert payload["summary"]["positive_count"] == 1
    assert payload["items"][0]["current_price"] == 10.8
    assert payload["items"][0]["return_pct"] == 8.0
    assert payload["items"][0]["review_status"] == "positive"


def test_paper_trial_review_record_appends_correction_when_status_changes(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current-ok",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:00:00+08:00",
                "run_current",
                json.dumps({"price": 10.8, "close": 10.8}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-current-weak",
                "test",
                "quote",
                "688981",
                "2026-05-22T14:55:00+08:00",
                "run_current",
                json.dumps({"price": 9.0, "close": 9.0}, ensure_ascii=False),
            ),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-review", "--min-age-days", "0", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["recorded_count"] == 1
    conn = connect(db_path)
    try:
        events = EventStore(conn).query(
            stream=f"paper_trial_review:{payload['date']}:688981",
            event_type="paper.trial.reviewed",
        )
    finally:
        conn.close()
    assert len(events) == 2
    assert events[0]["payload"]["review_status"] == "positive"
    assert events[-1]["payload"]["review_status"] == "negative"
    assert events[-1]["payload"]["review_corrected"] is True
    assert events[-1]["payload"]["previous_review_status"] == "positive"
    assert events[-1]["payload"]["previous_event_id"] == events[0]["event_id"]


def test_paper_trial_plan_record_supplements_legacy_trial_event_with_start_price(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.time import local_today_str
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    trial_date = local_today_str()
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        conn.execute(
            """INSERT INTO market_observations
               (observation_id, source, kind, symbol, observed_at, run_id, payload_json)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                "obs-trial-start",
                "test",
                "quote",
                "688981",
                "2026-05-22T09:30:00+08:00",
                "run_start",
                json.dumps({"price": 10.0, "close": 10.0}, ensure_ascii=False),
            ),
        )
        EventStore(conn).append(
            stream=f"paper_trial:{trial_date}:688981",
            stream_type="paper_trial",
            event_type="paper.trial.recorded",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "trial_date": trial_date,
                "paper_order_submitted": False,
            },
            metadata={"source": "legacy-test"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "paper", "trial-plan", "--record", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["recorded_count"] == 1

    conn = connect(db_path)
    try:
        events = EventStore(conn).query(
            stream=f"paper_trial:{trial_date}:688981",
            event_type="paper.trial.recorded",
        )
    finally:
        conn.close()
    assert len(events) == 2
    assert events[-1]["payload"]["trial_start_price"] == 10.0
    assert events[-1]["payload"]["baseline_supplemented"] is True


def test_notify_opportunity_dry_run_json_via_bin_trade(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "300558",
                "name": "贝达药业",
                "pool_tier": "watch",
                "score": 6.2,
                "note": "requires_entry_strategy_route",
            }
        ])
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "notify", "opportunity", "--dry-run", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["status"] == "dry_run"
    assert payload["notification"]["target"] == "discord"
    assert "今日机会卡" in payload["embed"]["title"]
    assert payload["opportunity"]["execution_allowed"] is False
    values = "\n".join(field["value"] for field in payload["embed"]["fields"])
    assert "观察候选" in values
    assert "贝达药业(300558)" in values
    assert "不自动交易" in values


def test_notify_opportunity_embed_labels_candidate_pool_when_core_present(tmp_path):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform.db import connect, init_db
    from astock_trading.reporting.projectors import ProjectionUpdater

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "core",
                "score": 6.4,
                "note": "screener_refresh",
            },
            {
                "code": "688372",
                "name": "伟测科技",
                "pool_tier": "watch",
                "score": 5.7,
                "note": "screener_refresh",
            },
        ])
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "notify", "opportunity", "--dry-run", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["opportunity"]["summary"] == "核心候选 1 只，观察候选 1 只；等待入场信号，不自动买入。"
    field_names = [field["name"] for field in payload["embed"]["fields"]]
    assert "候选池（核心 1 / 观察 1）" in field_names
    values = "\n".join(field["value"] for field in payload["embed"]["fields"])
    assert "中芯国际(688981) 核心" in values
    assert "伟测科技(688372) 观察" in values


def test_hermes_digest_localizes_clear_action(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        EventStore(conn).append(
            stream="decision:603215",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "603215",
                "name": "比依股份",
                "action": "CLEAR",
                "score": 4.4,
                "confidence": 4.4,
                "notes": ["评分过低"],
            },
            metadata={"run_id": "scoring_cli"},
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "digest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["latest_decision"]["action"] == "CLEAR"
    assert payload["latest_decision"]["action_label"] == "观望"
    assert "603215 观望" in payload["summary"]


def test_hermes_digest_uses_latest_decision_beyond_default_event_page(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        for index in range(205):
            code = f"60{index:04d}"
            event_id = store.append(
                stream=f"decision:{code}",
                stream_type="strategy",
                event_type="decision.suggested",
                payload={
                    "code": code,
                    "name": f"旧候选{index}",
                    "action": "CLEAR",
                    "score": 4.0,
                    "notes": ["历史观望"],
                },
                metadata={"run_id": f"old_page_{index}"},
            )
            conn.execute(
                "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
                (f"2026-04-01T00:{index % 60:02d}:00+00:00", event_id),
            )
        latest_event_id = store.append(
            stream="decision:688981",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "notes": ["入场信号成立"],
            },
            metadata={"run_id": "latest_buy"},
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T06:30:00+00:00", latest_event_id),
        )
    finally:
        conn.close()

    result = subprocess.run(
        [str(cli), "digest", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["latest_decision"]["code"] == "688981"
    assert payload["latest_decision"]["action"] == "BUY"
    assert "688981 买入意向" in payload["summary"]


def test_hermes_digest_prioritizes_pending_buy_intent_over_later_clear_decision(tmp_path):
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore

    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        buy_decision_id = store.append(
            stream="decision:688981",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "notes": ["入场信号成立"],
            },
            metadata={"run_id": "pending_buy"},
        )
        manual_event_id = store.append(
            stream="manual_trade:688981",
            stream_type="manual_trade",
            event_type="manual_trade.requested",
            payload={
                "status": "pending",
                "side": "buy",
                "code": "688981",
                "name": "中芯国际",
                "score": 6.4,
                "source_event_id": buy_decision_id,
            },
            metadata={"run_id": "pending_buy"},
        )
        clear_decision_id = store.append(
            stream="decision:002342",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "002342",
                "name": "巨力索具",
                "action": "CLEAR",
                "score": 0.0,
                "notes": ["一票否决"],
            },
            metadata={"run_id": "later_clear"},
        )
        conn.executemany(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            [
                ("2026-05-22T05:59:59+00:00", buy_decision_id),
                ("2026-05-22T06:00:00+00:00", manual_event_id),
                ("2026-05-22T06:01:00+00:00", clear_decision_id),
            ],
        )
    finally:
        conn.close()

    env = _cli_env(tmp_path)
    env["ASTOCK_TEST_NOW"] = "2026-05-22T14:05:00+08:00"
    result = subprocess.run(
        [str(cli), "digest", "--json"],
        cwd=root,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["latest_decision"]["code"] == "002342"
    assert payload["latest_decision"]["action"] == "CLEAR"
    assert payload["signal_focus"]["type"] == "pending_manual_trade"
    assert payload["signal_focus"]["code"] == "688981"
    assert payload["signal_focus"]["action_label"] == "买入意向"
    assert "当前重点 688981 中芯国际 买入意向 6.4 分" in payload["summary"]
    assert "最新决策 002342 观望" not in payload["summary"]


def test_sqlite_to_mysql_migration_dry_run_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    sqlite_path = tmp_path / "archived_astock_trading.db"

    result = subprocess.run(
        [
            str(cli),
            "db",
            "migrate-sqlite-to-mysql",
            "--sqlite-path",
            str(sqlite_path),
            "--dry-run",
            "--json",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["dry_run"] is True
    assert "event_log" in payload["source_counts"]
    assert payload["target"] == "not_written"
