"""Smoke tests for the real CLI entrypoint."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

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
    assert payload["recommended_commands"]["screener_explain"] == "atrade screener explain --json"


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
