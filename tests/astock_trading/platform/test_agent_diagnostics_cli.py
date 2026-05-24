"""Agent diagnostics CLI contract tests."""

from __future__ import annotations

import json
import os
import subprocess
from datetime import date, datetime, time, timezone
from pathlib import Path

from astock_trading.market.store import MarketStore
from astock_trading.platform.agent_diagnostics import (
    _next_window_date_from_schedule,
    diagnose_flow,
    diagnose_health,
    diagnose_schedule,
    diagnose_strategy,
    propose_agent_trade_plan,
)
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.reporting.projectors import ProjectionUpdater


def _cli_env(tmp_path: Path) -> dict:
    env = os.environ.copy()
    env["ASTOCK_DATABASE_URL"] = f"sqlite:///{tmp_path / 'runtime.db'}"
    return env


def test_next_window_date_uses_scheduled_trading_day_on_weekend_before_buy_window():
    next_date = _next_window_date_from_schedule(
        current=datetime.fromisoformat("2026-05-23T00:26:00+08:00"),
        start_time=time(9, 45),
        end_time=time(14, 30),
        scheduled_steps=[
            {"next_run_at": "2026-05-25T13:40:00+08:00"},
            {"next_run_at": "2026-05-25T14:00:00+08:00"},
        ],
    )

    assert next_date == date(2026, 5, 25)


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


def test_diagnose_flow_json_via_bin_trade(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "diagnose", "flow", "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["diagnostic"] == "candidate_flow"
    assert payload["guardrails"]["read_only"] is True
    assert payload["guardrails"]["real_order_auto_execution_allowed"] is False
    assert "candidate_pool" in payload
    assert "strategy" in payload
    assert "opportunity" in payload
    assert "auto_readiness" in payload


def test_diagnose_flow_exposes_top_level_candidate_summary_and_entry_signals(tmp_path, monkeypatch):
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        events = EventStore(conn)
        ProjectionUpdater(events, conn).sync_candidate_pool([
            {
                "code": "002384",
                "name": "东山精密",
                "pool_tier": "core",
                "score": 7.0,
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
        events.append(
            "strategy:002384",
            "strategy",
            "score.calculated",
            {
                "code": "002384",
                "name": "东山精密",
                "total_score": 7.0,
                "entry_signal": True,
                "primary_strategy_route": "flow_confirmed_trend",
                "technical_detail": "金叉成立，资金趋势确认",
                "data_quality": "ok",
            },
        )

        payload = diagnose_flow(conn)
    finally:
        conn.close()

    assert payload["candidate_summary"] == {
        "total": 2,
        "core_count": 1,
        "watch_count": 1,
        "radar_count": 0,
        "entry_signal_count": 1,
        "latest_scored_at": payload["candidate_summary"]["latest_scored_at"],
        "summary": "候选池 2 只：核心 1、观察 1、强势观察 0；当前入场信号 1 只。",
        "top_core_candidate": {
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "pool_tier_label": "核心",
            "score": 7.0,
            "entry_signal": True,
            "primary_strategy_route": "flow_confirmed_trend",
            "primary_strategy_route_label": "资金趋势确认",
            "technical_detail": "金叉成立，资金趋势确认",
            "review_command": "atrade stock analyze 002384 --json",
        },
        "top_watch_candidate": {
            "code": "600584",
            "name": "长电科技",
            "pool_tier": "watch",
            "pool_tier_label": "观察",
            "score": 5.9,
            "entry_signal": None,
            "primary_strategy_route": None,
            "primary_strategy_route_label": None,
            "technical_detail": "",
            "review_command": "atrade stock analyze 600584 --json",
        },
        "top_radar_candidate": {},
    }
    assert payload["current_entry_signals"] == [
        {
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "pool_tier_label": "核心",
            "score": 7.0,
            "entry_signal": True,
            "primary_strategy_route": "flow_confirmed_trend",
            "primary_strategy_route_label": "资金趋势确认",
            "technical_detail": "金叉成立，资金趋势确认",
            "data_quality": "ok",
            "review_command": "atrade stock analyze 002384 --json",
        }
    ]
    assert payload["strategy"]["candidate_flow"]["candidate_summary"] == payload["candidate_summary"]


def test_diagnose_flow_paper_trial_summary_exposes_review_outcome(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        store.append(
            "paper_trial:2026-05-22:688981",
            "paper_trial",
            "paper.trial.recorded",
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.8,
                "trial_date": "2026-05-22",
                "trial_start_price": 10.0,
                "paper_order_submitted": False,
            },
        )
        store.append(
            "paper_trial_review:2026-05-23:688981",
            "paper_trial",
            "paper.trial.reviewed",
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 5.8,
                "trial_date": "2026-05-22",
                "review_date": "2026-05-23",
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 8.0,
                "current_pool_tier": "core",
                "current_pool_tier_label": "核心",
                "current_entry_signal": True,
                "current_primary_strategy_route": "flow_confirmed_trend",
                "current_primary_strategy_route_label": "资金趋势确认",
                "candidate_state_changed": True,
                "candidate_state_change_label": "观察 -> 核心",
                "paper_order_submitted": False,
            },
        )
        store.append(
            "paper_trial_review:2026-05-23:600584",
            "paper_trial",
            "paper.trial.reviewed",
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "radar",
                "score": 4.8,
                "trial_date": "2026-05-22",
                "review_date": "2026-05-23",
                "review_status": "positive",
                "review_status_label": "表现为正",
                "return_pct": 9.04,
                "current_pool_tier": "watch",
                "current_pool_tier_label": "观察",
                "current_entry_signal": False,
                "candidate_state_changed": True,
                "candidate_state_change_label": "强势观察 -> 观察",
                "paper_order_submitted": False,
            },
        )

        payload = diagnose_flow(conn, opportunity={}, auto_readiness={})
    finally:
        conn.close()

    paper_trial = payload["automation"]["paper_trial"]
    assert paper_trial["recorded_count"] == 1
    assert paper_trial["reviewed_count"] == 2
    assert paper_trial["latest_recorded"]["status"] == "recorded"
    assert paper_trial["latest_recorded"]["status_label"] == "已记录"
    assert paper_trial["latest_reviewed"]["status"] == "positive"
    assert paper_trial["latest_reviewed"]["status_label"] == "表现为正"
    assert paper_trial["latest_reviewed"]["code"] == "600584"
    assert paper_trial["latest_reviewed"]["return_pct"] == 9.04
    assert paper_trial["latest_reviewed"]["current_pool_tier"] == "watch"
    assert paper_trial["latest_reviewed"]["current_pool_tier_label"] == "观察"
    assert paper_trial["latest_reviewed"]["candidate_state_change_label"] == "强势观察 -> 观察"
    assert paper_trial["review_summary"]["positive_count"] == 2
    assert paper_trial["positive_reviews"][0] == {
        "event_id": paper_trial["positive_reviews"][0]["event_id"],
        "evidence_id": paper_trial["positive_reviews"][0]["event_id"],
        "event_type": "paper.trial.reviewed",
        "occurred_at": paper_trial["positive_reviews"][0]["occurred_at"],
        "code": "688981",
        "name": "中芯国际",
        "status": "positive",
        "status_label": "表现为正",
        "pool_tier": "watch",
        "score": 5.8,
        "trial_date": "2026-05-22",
        "review_date": "2026-05-23",
        "return_pct": 8.0,
        "current_pool_tier": "core",
        "current_pool_tier_label": "核心",
        "current_entry_signal": True,
        "current_primary_strategy_route": "flow_confirmed_trend",
        "current_primary_strategy_route_label": "资金趋势确认",
        "candidate_state_changed": True,
        "candidate_state_change_label": "观察 -> 核心",
        "paper_order_submitted": False,
        "review_command": "atrade stock analyze 688981 --json",
    }
    assert paper_trial["next_action"] == {
        "type": "review_positive_trial",
        "label": "复核表现为正的影子候选",
        "command": "atrade stock analyze 688981 --json",
        "reason": "影子试运行收益为正，只能进入人工复核，不能自动晋级或下单。",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "stock_analyze",
    }


def test_diagnose_flow_paper_trial_counts_are_not_capped_to_recent_page(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        for index in range(21):
            code = f"68{index:04d}"
            store.append(
                f"paper_trial_review:2026-05-23:{code}",
                "paper_trial",
                "paper.trial.reviewed",
                {
                    "code": code,
                    "name": f"影子候选{index}",
                    "pool_tier": "watch",
                    "trial_date": "2026-05-22",
                    "review_date": "2026-05-23",
                    "review_status": "flat",
                    "review_status_label": "横盘观察",
                    "return_pct": 0.0,
                    "paper_order_submitted": False,
                },
            )

        payload = diagnose_flow(conn, opportunity={}, auto_readiness={})
    finally:
        conn.close()

    assert payload["automation"]["paper_trial"]["reviewed_count"] == 21


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


def test_diagnose_health_treats_recovered_failed_runs_as_non_actionable(tmp_path):
    db_path = tmp_path / "runtime.db"
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

        payload = diagnose_health(conn)

        assert payload["inputs"]["failed_runs"] == []
        assert payload["inputs"]["recovered_failed_runs"][0]["run_id"] == "auto_trade_failed"
        assert "failed runs require review" not in " ".join(payload["findings"])
    finally:
        conn.close()


def test_diagnose_health_distinguishes_empty_pool_from_missing_market_data(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-18", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("akshare", "fund_flow", "000858", {"items": [1]})

        payload = diagnose_health(conn)

        assert payload["inputs"]["data_sources"]["required_missing"] == []
        assert (
            "candidate pool is empty; required data sources are available, "
            "so treat this as no qualified candidates after screening"
        ) in payload["findings"]
        assert (
            "refresh candidates if needed; if it stays empty, report it as no qualified candidates, not missing market data"
        ) in payload["recommendations"]
    finally:
        conn.close()


def test_diagnose_health_reports_unresolved_provider_failures(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-18", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("akshare", "fund_flow", "000858", {"items": [1]})
        store.save_provider_failure(
            source="BaiduFundFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="parse_error",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            run_id="run_unresolved_provider_failure",
        )

        payload = diagnose_health(conn)

        assert payload["status"] == "warning"
        assert payload["inputs"]["data_sources"]["provider_failures"]["unresolved_recent"] == 1
        assert "1 个 provider 失败未被 fallback 补齐" in payload["findings"]
        assert "查看 data_sources.provider_failures.unresolved，先修未补齐的数据源再扩大交易判断" in payload["recommendations"]
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


def test_propose_plan_inspects_data_sources_when_latest_l1_coverage_is_degraded(tmp_path):
    from astock_trading.platform.history_mirror import archive_signal_history
    from astock_trading.platform.time import utc_now_iso

    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-20", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002138", {"items": [1]})
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("002138", "core", "双环传动", 7.1, utc_now_iso(), utc_now_iso(), 2, "测试核心候选"),
        )
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
                    "total_score": 7.1,
                    "data_quality": "degraded",
                    "data_missing_fields": ["资金流"],
                }
            ],
        )

        payload = propose_agent_trade_plan(conn)

        assert payload["execution_allowed"] is False
        assert payload["data_source_blockers"][0]["reason"] == "latest_screener_l1_coverage_degraded"
        assert payload["actions"][0]["type"] == "inspect_data_sources"
        assert "逐票 L1 覆盖不足" in payload["actions"][0]["reason"]
    finally:
        conn.close()


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


def test_llm_context_morning_markdown_includes_discord_card_contract(tmp_path):
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
    assert "## Discord 卡片输出模板" in text
    assert "## A股盘前摘要｜YYYY-MM-DD 09:20" in text
    assert "### 1. 系统与数据质量" in text
    assert "### 2. 今日动作" in text
    assert "### 3. 市场热点" in text
    assert "### 6. 今日纪律" in text
    assert "数据降级时，信心也要降级" in text
    assert "热点只作为市场背景和复盘线索，不作为买入依据" in text


def test_llm_context_close_markdown_includes_discord_card_contract(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "llm-context", "--mode", "close"],
        cwd=tmp_path,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    text = result.stdout
    assert "## Discord 卡片输出模板" in text
    assert "## A股收盘复盘｜YYYY-MM-DD 15:55" in text
    assert "### 1. 系统与数据质量" in text
    assert "### 3. 收盘市场热点" in text
    assert "### 4. 盘前 vs 收盘" in text
    assert "### 7. 明日清单" in text
    assert "计划外的交易，先当风险处理" in text
    assert "对比只用于复盘早盘判断质量，不作为自动交易依据" in text


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


def test_llm_context_morning_marks_hot_stock_change_as_non_realtime_and_labels_evidence(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("morning_run", "morning", "cn_a", "test", "completed", "2026-05-22T09:15:00+08:00"),
        )
        store = MarketStore(conn)
        store.save_observation(
            "test",
            "xueqiu_hot_stocks",
            "cn_a",
            {"items": [{"code": "000725", "name": "京东方A", "rank": 3, "change_pct": 10.02}]},
            run_id="morning_run",
        )

        from astock_trading.platform.llm_context import build_llm_context

        payload = build_llm_context(conn, mode="morning")
    finally:
        conn.close()

    current = payload["sections"]["market_intel"]["data"]["current"]
    stock = current["hot_stocks"][0]
    observation = current["observations"]["xueqiu_hot_stocks"]
    assert stock["change_pct_context"] == "热榜口径，非实时行情"
    assert stock["is_realtime_quote"] is False
    assert stock["evidence_id"] == observation["observation_id"]
    assert observation["kind"] == "xueqiu_hot_stocks"
    evidence = next(item for item in payload["evidence_registry"] if item["evidence_id"] == observation["observation_id"])
    assert evidence["label"] == "xueqiu_hot_stocks"


def test_llm_context_morning_omits_directionless_sector_heatmap(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("morning_run", "morning", "cn_a", "test", "completed", "2026-05-22T09:15:00+08:00"),
        )
        store = MarketStore(conn)
        store.save_observation(
            "test",
            "sector_heatmap",
            "cn_a",
            {"items": [
                {"name": "专业技术服务业", "change_pct": 0, "amount": 0, "up_count": 0, "down_count": 0},
            ]},
            run_id="morning_run",
        )

        from astock_trading.platform.llm_context import build_llm_context

        payload = build_llm_context(conn, mode="morning")
    finally:
        conn.close()

    current = payload["sections"]["market_intel"]["data"]["current"]
    assert current["sectors"] == []
    assert "不能当作热门板块" in current["data_notes"][0]
    assert current["observations"]["sector_heatmap"]["kind"] == "sector_heatmap"


def test_llm_context_close_includes_operable_review_diagnostics(tmp_path):
    from astock_trading.platform.events import EventStore

    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "cn_a", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("akshare", "fund_flow", "000858", {"items": [1]})
        store.save_provider_failure(
            source="BaiduFundFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="parse_error",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            run_id="evening_run",
            details={
                "provider_diagnostic": {
                    "subsource_errors": {
                        "em_fund_flow": "empty response",
                        "tx_tick": "timeout",
                    }
                }
            },
        )
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("evening_run", "evening", "cn_a", "test", "completed", "2026-05-21T15:35:00+08:00"),
        )
        store.save_observation(
            "test",
            "cross_platform_hot_stocks",
            "cn_a",
            {"items": [
                {"code": "603215", "name": "比依股份", "source_count": 3},
                {"code": "002138", "name": "双环传动", "source_count": 2},
            ]},
            run_id="evening_run",
        )

        events = EventStore(conn)
        events.append(
            "strategy:603215",
            "strategy",
            "score.calculated",
            {
                "code": "603215",
                "name": "比依股份",
                "total_score": 4.2,
                "entry_signal": False,
                "data_quality": "degraded",
                "data_missing_fields": ["资金流"],
                "hard_veto_signals": ["below_ma20"],
            },
        )
        events.append(
            "strategy:603215",
            "strategy",
            "decision.suggested",
            {
                "code": "603215",
                "name": "比依股份",
                "action": "CLEAR",
                "score": 4.2,
                "veto_reasons": ["below_ma20", "consecutive_outflow"],
            },
        )

        from astock_trading.platform.llm_context import build_llm_context

        payload = build_llm_context(conn, mode="close")
    finally:
        conn.close()

    review = payload["sections"]["close_review"]["data"]
    assert review["data_source_failures"]["unresolved_count"] == 1
    failure = review["data_source_failures"]["unresolved"][0]
    assert failure["source"] == "BaiduFundFlowAdapter"
    assert failure["subsource_errors"]["tx_tick"] == "timeout"
    assert review["candidate_funnel"]["pool"]["core_count"] == 0
    assert review["candidate_funnel"]["scores"]["total"] == 1
    assert review["candidate_funnel"]["scores"]["entry_signal"]["missing"] == 1
    assert review["candidate_funnel"]["decisions"]["actions"][0]["label"] == "观望"
    assert review["candidate_funnel"]["blockers"]["decision_veto_reasons"][0]["label"] == "跌破 MA20"
    assert review["hot_stock_pool_bridge"]["not_in_pool"][0]["code"] == "603215"
    assert "只作为召回线索" in review["hot_stock_pool_bridge"]["rule"]
    assert review["comparison_readiness"]["available"] is False
    assert "盘前热点数据" in review["comparison_readiness"]["missing_inputs"]
    commands = [item["command"] for item in review["tomorrow_checklist"]]
    assert "atrade data-sources diagnose --json" in commands
    assert "atrade screener explain --json" in commands
    assert any(item["kind"] == "market_observation" for item in payload["evidence_registry"])


def test_llm_context_close_includes_simulation_flow_gate(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_TEST_NOW", "2026-05-23T10:00:00+08:00")
    jobs_path = tmp_path / "jobs.json"
    scripts_dir = tmp_path / "scripts"
    env_file = tmp_path / ".env"
    env_file.write_text(f"ASTOCK_DATABASE_URL=sqlite:///{db_path}\n", encoding="utf-8")
    monkeypatch.setenv("ASTOCK_ENV_FILE", str(env_file))
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(jobs_path))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    scripts_dir.mkdir(parents=True)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "模拟盘自动交易",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "0 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:01:31+08:00",
                    "last_status": "ok",
                    "next_run_at": "2026-05-25T14:00:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
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
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "002384",
                "name": "东山精密",
                "pool_tier": "core",
                "score": 7.0,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        score_event_id = events.append(
            "strategy:002384",
            "strategy",
            "score.calculated",
            {
                "code": "002384",
                "name": "东山精密",
                "total_score": 7.0,
                "entry_signal": True,
                "primary_strategy_route": "flow_confirmed_trend",
                "strategy_routes": [
                    {
                        "route": "flow_confirmed_trend",
                        "display_name": "资金趋势确认",
                        "entry_signal": True,
                    }
                ],
                "data_quality": "ok",
            },
        )
        decision_event_id = events.append(
            "strategy:002384",
            "strategy",
            "decision.suggested",
            {
                "code": "002384",
                "name": "东山精密",
                "action": "BUY",
                "score": 7.0,
                "position_pct": 0.2,
                "market_signal": "GREEN",
                "source_score_event_id": score_event_id,
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-23T01:30:00+00:00", decision_event_id),
        )
        activation_event_id = events.append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                },
            },
        )
        paper_recorded_event_id = events.append(
            "paper_trial:2026-05-22:002384",
            "paper_trial",
            "paper.trial.recorded",
            {
                "code": "002384",
                "name": "东山精密",
                "pool_tier": "core",
                "score": 7.0,
                "trial_date": "2026-05-22",
                "paper_order_submitted": False,
            },
        )

        from astock_trading.platform.llm_context import build_llm_context

        payload = build_llm_context(conn, mode="close")
    finally:
        conn.close()

    review = payload["sections"]["close_review"]["data"]
    simulation = review["simulation_flow"]
    assert simulation["status"] == "profile_review_required"
    assert simulation["approval_gate"]["required"] is True
    assert simulation["approval_gate"]["review_command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert simulation["approval_gate"]["safe_to_auto_apply"] is False
    assert simulation["approval_gate"]["review_command_contract"]["risk_level"] == "read_only"
    assert simulation["approval_gate"]["apply_command_contract"]["risk_level"] == "environment_write"
    assert simulation["approval_gate"]["apply_command_contract"]["writes_environment"] is True
    for action_path in [
        ("flow_stage",),
        ("opportunity",),
        ("auto_readiness",),
        ("next_window_plan",),
    ]:
        action = simulation[action_path[0]]["next_action"]
        assert action["risk_level"] == "read_only"
        assert action["writes_state"] is False
        assert action["writes_environment"] is False
        assert action["writes_order"] is False
        assert action["requires_user_approval"] is False
        assert action["command_contract_id"]
    assert simulation["flow_stage"]["next_action"]["command_contract_id"] == (
        "strategy_profile_activation_review"
    )
    assert simulation["auto_readiness"]["next_action"]["command_contract_id"] == (
        "strategy_profile_activation_review"
    )
    assert simulation["next_window_plan"]["next_action"]["command_contract_id"] == (
        "strategy_profile_activation_review"
    )
    assert simulation["runtime_contract"]["status"] == "ok"
    assert simulation["runtime_contract"]["scope"] == "next_window_simulation_scripts"
    schedule = simulation["automation_schedule"]
    assert schedule["status"] == "warning"
    assert schedule["runtime_profile"]["status"] == "review_required"
    assert schedule["runtime_contract"]["status"] == "ok"
    assert schedule["runtime_contract"]["scope"] == "next_window_simulation_scripts"
    assert schedule["runtime_contract"]["blocking_issues"] == []
    assert schedule["runtime_contract"]["script_checks"] == [
        {
            "script": "a_stock_intraday_execution_cycle_silent.sh",
            "profile_env_file_loading_possible": True,
            "issues": [],
        },
        {
            "script": "a_stock_pipeline_auto_trade_silent.sh",
            "profile_env_file_loading_possible": True,
            "issues": [],
        },
        {
            "script": "a_stock_screener_refresh_intraday_silent.sh",
            "profile_env_file_loading_possible": True,
            "issues": [],
        },
    ]
    assert schedule["intraday_simulation"]["status"] == "profile_review_required"
    assert schedule["intraday_simulation"]["runtime_contract"]["status"] == "ok"
    assert schedule["intraday_simulation"]["profile_ready"] is False
    assert schedule["intraday_simulation"]["ready_for_next_window"] is False
    assert schedule["intraday_simulation"]["critical_job_count"] == 3
    assert schedule["intraday_simulation"]["pending_first_run_critical_count"] == 2
    assert [item["script"] for item in schedule["intraday_simulation"]["scheduled_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert schedule["intraday_simulation"]["first_run_verification"]["critical_required"] is True
    assert simulation["flow_stage"]["latest_activation_request"]["event_id"] == activation_event_id
    assert simulation["flow_stage"]["latest_activation_request"]["evidence_id"] == activation_event_id
    assert simulation["flow_stage"]["recent_unusable_buy_signal"]["count"] == 1
    assert simulation["auto_readiness"]["recent_unusable_buy_signal"]["top"]["code"] == "002384"
    assert simulation["auto_readiness"]["recent_unusable_buy_signal"]["top"]["entry_signal"] is True
    activation_evidence = next(
        item for item in payload["evidence_registry"]
        if item["evidence_id"] == activation_event_id
    )
    assert activation_evidence["kind"] == "strategy.profile_activation.requested"
    assert activation_evidence["label"] == "trend_swing"
    assert simulation["auto_readiness"]["status"]
    assert any(
        item["reason"] == "profile_review_required"
        for item in simulation["auto_readiness"]["blockers"]
    )
    assert simulation["paper_trial"]["recorded_count"] == 1
    assert simulation["paper_trial"]["latest_recorded"]["code"] == "002384"
    assert simulation["paper_trial"]["latest_recorded"]["event_id"] == paper_recorded_event_id
    assert simulation["paper_trial"]["latest_recorded"]["evidence_id"] == paper_recorded_event_id
    assert simulation["paper_trial"]["latest_recorded"]["event_type"] == "paper.trial.recorded"
    assert simulation["paper_trial"]["next_action"]["writes_order"] is False
    assert simulation["paper_trial"]["next_action"]["risk_level"] == "read_only"
    assert simulation["paper_trial"]["next_action"]["command_contract_id"] in {
        "stock_analyze",
        "paper_trial_review",
        "paper_trial_plan",
    }
    assert simulation["recommended_commands"]["risk_trial_guard"] == "atrade risk trial-guard --json"
    assert simulation["guardrails"]["places_paper_order"] is False
    commands = [item["command"] for item in review["tomorrow_checklist"]]
    assert "atrade strategy profile-activation --target trend_swing --json" in commands
    approval_item = next(
        item for item in review["tomorrow_checklist"]
        if item["command"] == "atrade strategy profile-activation --target trend_swing --json"
    )
    assert approval_item["requires_user_approval"] is True
    assert approval_item["safe_to_auto_apply"] is False
    assert approval_item["apply_command_after_approval"] == (
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
    )
    assert approval_item["verify_command"] == "atrade diagnose schedule --json"
    assert approval_item["writes_environment_after_approval"] is True
    assert approval_item["review_command_contract"]["risk_level"] == "read_only"
    assert approval_item["apply_command_contract"]["risk_level"] == "environment_write"
    assert approval_item["apply_command_contract"]["writes_environment"] is True
    assert any("模拟承接链路" in item for item in review["summary_requirements"])


def test_close_review_checklist_does_not_prioritize_non_actionable_provider_incidents():
    from astock_trading.platform.llm_context import _provider_failure_context, _tomorrow_checklist

    data_source_failures = _provider_failure_context(
        {
            "data_source_diagnosis": {
                "provider_failures": {
                    "unresolved_recent": 1,
                    "resolved_recent": 0,
                    "by_unresolved_source": {"OpenCliFinanceAdapter": 1},
                    "unresolved": [
                        {
                            "source": "OpenCliFinanceAdapter",
                            "target_kind": "market_news_search",
                            "symbol": "A股大盘指数状态",
                            "status": "empty",
                            "error_type": "EmptyResult",
                            "error_message": "provider 返回空结果",
                        }
                    ],
                },
                "provider_incidents": {
                    "actionable_unresolved_recent": 0,
                    "non_actionable_unresolved_recent": 1,
                },
            },
            "data_source_blockers": [],
        },
        {},
    )

    checklist = _tomorrow_checklist(
        data_source_failures=data_source_failures,
        candidate_funnel={"pool": {"total": 7, "core_count": 1}},
        hot_bridge={},
        comparison={"available": True},
        simulation_flow={
            "approval_gate": {
                "required": True,
                "label": "人工确认写入运行 profile",
                "review_command": "atrade strategy profile-activation --target trend_swing --json",
                "apply_command": (
                    "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
                ),
                "verify_command": "atrade diagnose schedule --json",
                "modifies_environment_after_approval": True,
            }
        },
    )

    assert data_source_failures["actionable_unresolved_count"] == 0
    assert data_source_failures["non_actionable_unresolved_count"] == 1
    assert checklist[0]["command"] == "atrade strategy profile-activation --target trend_swing --json"
    data_source_item = next(
        item for item in checklist
        if item["command"] == "atrade data-sources diagnose --json"
    )
    assert data_source_item["priority"] == "normal"
    assert "不阻断候选或模拟承接" in data_source_item["reason"]


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
    assert payload["parameter_profiles"]["profiles_available"] is True
    assert {item["name"] for item in payload["parameter_profiles"]["available_profiles"]} >= {
        "trend_swing",
        "short_continuation",
        "defensive_watch",
    }
    assert "profile 已存在" in " ".join(payload["recommendations"])
    assert "split operating parameters into explicit profiles" not in payload["recommendations"]
    assert {item["name"] for item in payload["parameter_profiles"]["suggested"]} >= {
        "trend_swing",
        "short_continuation",
        "defensive_watch",
    }


def test_diagnose_strategy_reports_candidate_flow_and_simulation_blocker(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.setenv("ASTOCK_CONFIG_PROFILE", "trend_swing")
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
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        events.append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            {
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "entry_signal": True,
                "primary_strategy_route": "ma_golden_cross",
                "data_quality": "ok",
            },
        )
        events.append(
            "strategy:600584",
            "strategy",
            "score.calculated",
            {
                "code": "600584",
                "name": "长电科技",
                "total_score": 5.6,
                "entry_signal": False,
                "data_quality": "ok",
            },
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
                "position_pct": 0.1,
                "market_signal": "YELLOW",
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T06:21:32+00:00", decision_event_id),
        )
        events.append(
            "auto_trade:summary",
            "auto_trade",
            "auto_trade.summary",
            {
                "date": "2026-05-22",
                "dry_run": False,
                "buy_count": 0,
                "sell_count": 0,
                "no_trade_summary": {
                    "reason": "buy_window_closed_with_signal",
                    "message": "已有新鲜买入意向 1 条，但当前不在模拟买入窗口。",
                },
            },
        )

        payload = diagnose_strategy(conn)
    finally:
        conn.close()

    flow = payload["candidate_flow"]
    assert flow["pool"]["core_count"] == 1
    assert flow["pool"]["watch_count"] == 1
    assert flow["scores"]["unique_scores"] == 2
    assert flow["scores"]["entry_signal"]["triggered"] == 1
    assert flow["scores"]["entry_signal"]["missing"] == 1
    assert flow["decisions"]["action_counts"]["BUY"] == 1
    assert flow["decisions"]["usable_buy_signal_count"] == 1
    assert flow["decisions"]["latest_buy_signal"]["code"] == "688981"
    assert flow["decisions"]["latest_usable_buy_signal"]["code"] == "688981"
    assert flow["automation"]["latest_summary"]["no_trade_reason"] == "buy_window_closed_with_signal"
    assert payload["actionable_state"]["status"] == "buy_signal_waiting_window"
    assert payload["actionable_state"]["next_action"]["command"] == "atrade paper auto-readiness --json"


def test_diagnose_strategy_ranks_top_scores_and_decisions_by_actionability(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.setenv("ASTOCK_CONFIG_PROFILE", "trend_swing")
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
            {
                "code": "002342",
                "name": "巨力索具",
                "pool_tier": "radar",
                "score": 0.0,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        buy_score_id = events.append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            {
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "entry_signal": True,
                "data_quality": "ok",
            },
        )
        buy_decision_id = events.append(
            "strategy:688981",
            "strategy",
            "decision.suggested",
            {
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "position_pct": 0.1,
                "market_signal": "GREEN",
            },
        )
        clear_score_id = events.append(
            "strategy:002342",
            "strategy",
            "score.calculated",
            {
                "code": "002342",
                "name": "巨力索具",
                "total_score": 0.0,
                "entry_signal": False,
                "data_quality": "ok",
                "veto_triggered": True,
            },
        )
        clear_decision_id = events.append(
            "strategy:002342",
            "strategy",
            "decision.suggested",
            {
                "code": "002342",
                "name": "巨力索具",
                "action": "CLEAR",
                "score": 0.0,
                "position_pct": 0.0,
                "market_signal": "GREEN",
                "veto_reasons": ["below_ma20"],
            },
        )
        conn.executemany(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            [
                ("2026-05-22T06:00:00+00:00", buy_score_id),
                ("2026-05-22T06:00:01+00:00", buy_decision_id),
                ("2026-05-22T06:01:00+00:00", clear_score_id),
                ("2026-05-22T06:01:01+00:00", clear_decision_id),
            ],
        )

        payload = diagnose_strategy(conn)
    finally:
        conn.close()

    scores = payload["candidate_flow"]["scores"]["top_scores"]
    decisions = payload["candidate_flow"]["decisions"]["top_decisions"]
    assert scores[0]["code"] == "688981"
    assert scores[0]["score"] == 6.4
    assert scores[0]["entry_signal"] is True
    assert decisions[0]["code"] == "688981"
    assert decisions[0]["action"] == "BUY"


def test_diagnose_strategy_ignores_stale_entry_signal_outside_current_pool(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.setenv("ASTOCK_CONFIG_PROFILE", "trend_swing")
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.8,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        events.append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            {
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "entry_signal": True,
                "data_quality": "ok",
            },
        )
        events.append(
            "strategy:600584",
            "strategy",
            "score.calculated",
            {
                "code": "600584",
                "name": "长电科技",
                "total_score": 5.8,
                "entry_signal": False,
                "data_quality": "ok",
            },
        )

        payload = diagnose_strategy(conn)
    finally:
        conn.close()

    flow = payload["candidate_flow"]
    assert flow["scores"]["entry_signal"]["triggered"] == 0
    assert flow["scores"]["entry_signal"]["missing"] == 1
    assert flow["scores"]["entry_signal"]["latest_scores_triggered"] == 1
    assert payload["actionable_state"]["status"] == "observable_candidates"
    assert "入场信号不足" in payload["actionable_state"]["summary"]


def test_diagnose_strategy_ignores_buy_signal_on_non_trading_day(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.setenv("ASTOCK_CONFIG_PROFILE", "trend_swing")
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "002384",
                "name": "东山精密",
                "pool_tier": "core",
                "score": 7.0,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        events.append(
            "strategy:002384",
            "strategy",
            "score.calculated",
            {
                "code": "002384",
                "name": "东山精密",
                "total_score": 7.0,
                "entry_signal": True,
                "primary_strategy_route": "flow_confirmed_trend",
                "data_quality": "ok",
            },
        )
        decision_event_id = events.append(
            "strategy:002384",
            "strategy",
            "decision.suggested",
            {
                "code": "002384",
                "name": "东山精密",
                "action": "BUY",
                "score": 7.0,
                "position_pct": 0.2,
                "market_signal": "GREEN",
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T17:44:18+00:00", decision_event_id),
        )

        payload = diagnose_strategy(conn)
    finally:
        conn.close()

    flow = payload["candidate_flow"]
    assert flow["decisions"]["latest_buy_signal"]["code"] == "002384"
    assert flow["decisions"]["usable_buy_signal_count"] == 0
    assert flow["decisions"]["latest_usable_buy_signal"] is None
    assert flow["current_entry_signals"] == [
        {
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "pool_tier_label": "核心",
            "score": 7.0,
            "entry_signal": True,
            "primary_strategy_route": "flow_confirmed_trend",
            "primary_strategy_route_label": "资金趋势确认",
            "technical_detail": "",
            "data_quality": "ok",
            "review_command": "atrade stock analyze 002384 --json",
        }
    ]
    assert payload["actionable_state"]["status"] == "entry_signal_observable"
    assert "已有过期待复核买入意向" in payload["actionable_state"]["summary"]
    assert "没有新鲜可承接买入意向" in payload["actionable_state"]["summary"]
    assert payload["summary"] == (
        "候选池 1 只（核心 1、观察 0、强势观察 0）；"
        "已有过期待复核买入意向，但没有新鲜可承接买入意向；先保持影子试运行和单票复核。"
    )


def test_diagnose_flow_profile_review_summary_uses_current_signal_state(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
            {
                "code": "600584",
                "name": "长电科技",
                "pool_tier": "watch",
                "score": 5.8,
                "note": "screener_refresh",
            },
        ])
        EventStore(conn).append(
            "strategy:600584",
            "strategy",
            "score.calculated",
            {
                "code": "600584",
                "name": "长电科技",
                "total_score": 5.8,
                "entry_signal": False,
                "data_quality": "ok",
            },
        )

        payload = diagnose_flow(
            conn,
            opportunity={
                "status": "trial_plan_ready",
                "summary": "已有观察候选。",
                "counts": {"watch_candidates": 1, "buy_intents": 0},
                "next_action": {
                    "type": "paper_trial_plan",
                    "label": "生成影子试运行计划",
                    "command": "atrade paper trial-plan --json",
                    "safe_to_auto_apply": True,
                },
            },
        )
    finally:
        conn.close()

    assert payload["flow_stage"]["status"] == "profile_review_required"
    assert payload["strategy"]["actionable_state"]["status"] == "observable_candidates"
    assert "当前没有入场信号或新鲜买入意向" in payload["flow_stage"]["summary"]
    assert "买入意向已经进入可观察链路" not in payload["summary"]


def test_diagnose_flow_profile_review_summary_surfaces_recent_unusable_buy_signal(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
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
        activation_event_id = events.append(
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
        )

        payload = diagnose_flow(
            conn,
            opportunity={
                "status": "profile_review_required",
                "summary": "已有核心候选和过期买入意向。",
                "counts": {"core_candidates": 1, "buy_intents": 0},
                "next_action": {
                    "type": "review_runtime_profile_activation",
                    "label": "复核运行 profile 激活",
                    "command": "atrade strategy profile-activation --target trend_swing --json",
                    "safe_to_auto_apply": False,
                },
            },
            auto_readiness={
                "status": "profile_review_required",
                "summary": "当前仍在 default 混合配置；没有新鲜买入意向。",
                "execution_profile": {
                    "current_profile": "default",
                    "status": "review_required",
                    "safe_to_auto_apply": False,
                    "recommended_profile": "trend_swing",
                    "activation_request_status": "recorded",
                    "latest_activation_request": {"event_id": activation_event_id},
                },
                "fresh_buy_signal": {"count": 0, "top": {}},
                "recent_unusable_buy_signal": {
                    "count": 1,
                    "max_age_hours": 24,
                    "top": {
                        "event_id": "decision-weekend-buy",
                        "occurred_at": "2026-05-22T17:44:18+00:00",
                        "code": "688981",
                        "name": "中芯国际",
                        "score": 6.4,
                        "entry_signal": True,
                        "primary_strategy_route_label": "资金趋势确认",
                        "unusable_reason": "non_trading_day",
                        "unusable_reason_label": "买入意向发生日或当前检查日不是交易日",
                        "carries_to_current_window": False,
                    },
                },
                "buy_side": {
                    "status": "blocked",
                    "ready": False,
                    "blockers": [{"reason": "no_fresh_buy_signal", "label": "没有新鲜买入意向"}],
                },
                "blockers": [
                    {"reason": "profile_review_required", "label": "执行 profile 待确认"},
                    {"reason": "no_fresh_buy_signal", "label": "没有新鲜买入意向"},
                ],
            },
        )
    finally:
        conn.close()

    assert payload["flow_stage"]["status"] == "profile_review_required"
    assert payload["flow_stage"]["recent_unusable_buy_signal"]["count"] == 1
    assert "近期买入意向 1 条不可承接" in payload["flow_stage"]["summary"]
    assert "中芯国际(688981) 6.4 分" in payload["summary"]
    assert "买入意向发生日或当前检查日不是交易日" in payload["summary"]


def test_diagnose_flow_next_window_falls_back_to_latest_buy_signal(tmp_path, monkeypatch):
    db_path = tmp_path / "runtime.db"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(tmp_path / "missing-jobs.json"))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
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
        decision_event_id = events.append(
            "strategy:688981",
            "strategy",
            "decision.suggested",
            {
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "position_pct": 0.1,
                "market_signal": "YELLOW",
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T07:25:53+00:00", decision_event_id),
        )

        payload = diagnose_flow(
            conn,
            opportunity={
                "status": "profile_review_required",
                "summary": "已有核心候选和过期买入意向。",
                "counts": {"core_candidates": 1, "buy_intents": 0},
                "next_action": {},
            },
            auto_readiness={
                "status": "waiting_window",
                "checked_at": "2026-05-23T00:57:39+08:00",
                "buy_side": {
                    "status": "waiting_window",
                    "blockers": [{"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"}],
                },
                "blockers": [{"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"}],
            },
        )
    finally:
        conn.close()

    assert payload["next_window_plan"]["available"] is True
    assert payload["next_window_plan"]["current_signal"] == {
        "code": "688981",
        "name": "中芯国际",
        "occurred_at": "2026-05-22T07:25:53+00:00",
        "score": 6.4,
        "carries_to_next_window": False,
        "expires_reason": "买入意向只在产生当日且不晚于买入窗口结束时可被 auto_trade 承接",
    }
    assert payload["next_window_plan"]["next_window_requires_fresh_buy_signal"] is True


def test_diagnose_strategy_prioritizes_profile_activation_before_next_buy_window(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(jobs_path))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr(
        "astock_trading.platform.agent_diagnostics.utc_now",
        lambda: datetime(2026, 5, 22, 8, 10, tzinfo=timezone.utc),
    )
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "模拟盘自动交易",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "0 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:01:31+08:00",
                    "last_status": "ok",
                    "next_run_at": "2026-05-25T14:00:00+08:00",
                },
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T15:21:35+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T16:46:05+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T20:03:15+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
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
        decision_event_id = events.append(
            "strategy:688981",
            "strategy",
            "decision.suggested",
            {
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "position_pct": 0.1,
                "market_signal": "YELLOW",
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T06:21:32+00:00", decision_event_id),
        )
        events.append(
            "auto_trade:summary",
            "auto_trade",
            "auto_trade.summary",
            {
                "date": "2026-05-22",
                "dry_run": False,
                "buy_count": 0,
                "sell_count": 0,
                "no_trade_summary": {
                    "reason": "buy_window_closed_with_signal",
                    "message": "已有新鲜买入意向 1 条，但当前不在模拟买入窗口。",
                },
            },
        )
        activation_event_id = events.append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "verify_command": (
                        "ASTOCK_CONFIG_PROFILE=trend_swing "
                        "atrade paper auto-readiness --json"
                    ),
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                },
            },
        )

        payload = diagnose_strategy(conn)
    finally:
        conn.close()

    assert payload["actionable_state"]["execution_profile"]["status"] == "review_required"
    assert payload["actionable_state"]["execution_profile"]["activation_request_status"] == "recorded"
    assert (
        payload["actionable_state"]["execution_profile"]["latest_activation_request"]["event_id"]
        == activation_event_id
    )
    assert payload["actionable_state"]["next_action"] == {
        "type": "review_recorded_profile_activation",
        "label": "复核已记录的 profile 激活计划",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }
    assert "当前仍在 default 混合配置；自动模拟前需要人工确认执行 profile" in payload["summary"]
    assert "。；" not in payload["summary"]
    assert "先复核已记录的 trend_swing profile 激活计划，再等下个买入窗口预检" in payload[
        "recommendations"
    ]


def test_diagnose_flow_joins_candidate_signal_and_simulation_blockers(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(jobs_path))
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    monkeypatch.setattr(
        "astock_trading.platform.agent_diagnostics.utc_now",
        lambda: datetime(2026, 5, 22, 8, 10, tzinfo=timezone.utc),
    )
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "模拟盘自动交易",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "0 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:01:31+08:00",
                    "last_status": "ok",
                    "next_run_at": "2026-05-25T14:00:00+08:00",
                },
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T15:21:35+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T16:46:05+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T20:03:15+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    try:
        ProjectionUpdater(None, conn).sync_candidate_pool([
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
                "score": 5.6,
                "note": "screener_refresh",
            },
        ])
        events = EventStore(conn)
        events.append(
            "strategy:688981",
            "strategy",
            "score.calculated",
            {
                "code": "688981",
                "name": "中芯国际",
                "total_score": 6.4,
                "entry_signal": True,
                "data_quality": "ok",
            },
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
                "position_pct": 0.1,
                "market_signal": "YELLOW",
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T06:21:32+00:00", decision_event_id),
        )
        events.append(
            "auto_trade:summary",
            "auto_trade",
            "auto_trade.summary",
            {
                "date": "2026-05-22",
                "dry_run": False,
                "buy_count": 0,
                "sell_count": 0,
                "no_trade_summary": {
                    "reason": "buy_window_closed_with_signal",
                    "message": "已有新鲜买入意向 1 条，但当前不在模拟买入窗口。",
                },
            },
        )
        activation_event_id = events.append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "verify_command": (
                        "ASTOCK_CONFIG_PROFILE=trend_swing "
                        "atrade paper auto-readiness --json"
                    ),
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                },
            },
        )

        payload = diagnose_flow(
            conn,
            opportunity={
                "status": "profile_review_required",
                "summary": "已有核心候选和买入意向；模拟承接前先复核运行 profile 激活。",
                "counts": {
                    "core_candidates": 1,
                    "watch_candidates": 1,
                    "buy_intents": 0,
                },
                "next_action": {
                    "type": "review_runtime_profile_activation",
                    "label": "复核运行 profile 激活",
                    "command": "atrade strategy profile-activation --target trend_swing --json",
                    "safe_to_auto_apply": False,
                },
            },
            auto_readiness={
                "status": "profile_review_required",
                "summary": "当前仍在 default 混合配置；已记录待人工确认的 trend_swing profile 激活计划。",
                "execution_profile": {
                    "current_profile": "default",
                    "status": "review_required",
                    "safe_to_auto_apply": False,
                    "recommended_profile": "trend_swing",
                    "activation_request_status": "recorded",
                    "latest_activation_request": {"event_id": activation_event_id},
                },
                "candidate_pool": {"total_count": 2, "core_count": 1, "watch_count": 1},
                "fresh_buy_signal": {
                    "count": 1,
                    "top": {
                        "code": "688981",
                        "name": "中芯国际",
                        "occurred_at": "2026-05-22T06:21:32+00:00",
                        "score": 6.4,
                    },
                },
                "checked_at": "2026-05-22T16:10:00+08:00",
                "window_state": {
                    "buy_open": False,
                    "sell_open": False,
                    "checked_at": "2026-05-22T16:10:00+08:00",
                },
                "buy_side": {
                    "status": "waiting_window",
                    "ready": False,
                    "blockers": [{"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"}],
                    "top_signal": {
                        "code": "688981",
                        "name": "中芯国际",
                        "occurred_at": "2026-05-22T06:21:32+00:00",
                        "score": 6.4,
                    },
                },
                "blockers": [
                    {"reason": "profile_review_required", "label": "执行 profile 待确认"},
                    {"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"},
                ],
                "next_action": {
                    "type": "review_recorded_profile_activation",
                    "label": "复核已记录的 profile 激活计划",
                    "command": "atrade strategy profile-activation --target trend_swing --json",
                    "safe_to_auto_apply": False,
                },
            },
        )
    finally:
        conn.close()

    assert payload["diagnostic"] == "candidate_flow"
    assert payload["status"] == "warning"
    assert payload["flow_stage"]["status"] == "profile_review_required"
    assert payload["flow_stage"]["latest_activation_request"]["event_id"] == activation_event_id
    assert payload["next_action"]["command"] == "atrade strategy profile-activation --target trend_swing --json"
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
    assert payload["after_approval_preview"] == {
        "available": True,
        "target_profile": "trend_swing",
        "summary": (
            "人工批准并写入 trend_swing 后，按当前只读预判还剩 1 个非 profile 阻断："
            "当前不在模拟买入窗口。"
        ),
        "preview_command": (
            "ASTOCK_CONFIG_PROFILE=trend_swing "
            "atrade paper auto-readiness --skip-account --json"
        ),
        "post_approval_verify_command": "atrade paper auto-readiness --json",
        "schedule_verify_command": "atrade diagnose schedule --json",
        "remaining_blockers_from_current_readiness": [
            {"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"}
        ],
        "safe_to_auto_apply": True,
        "writes_environment": False,
        "places_order": False,
        "note": "这是基于当前 readiness 的只读预判，不会写 .env，也不会提交模拟委托。",
    }
    assert payload["next_window_plan"]["available"] is True
    assert payload["next_window_plan"]["status"] == "requires_profile_approval_before_next_window"
    assert payload["next_window_plan"]["next_buy_window"] == {
        "start": "2026-05-25T09:45:00+08:00",
        "end": "2026-05-25T14:30:00+08:00",
        "source": "auto_trade.buy_window",
    }
    assert payload["next_window_plan"]["current_signal"] == {
        "code": "688981",
        "name": "中芯国际",
        "occurred_at": "2026-05-22T06:21:32+00:00",
        "score": 6.4,
        "carries_to_next_window": False,
        "expires_reason": "买入意向只在产生当日且不晚于买入窗口结束时可被 auto_trade 承接",
    }
    assert payload["next_window_plan"]["next_window_requires_fresh_buy_signal"] is True
    assert [item["script"] for item in payload["next_window_plan"]["scheduled_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    steps_by_next_run = {
        item["next_run_at"]: item
        for item in payload["next_window_plan"]["scheduled_steps"]
    }
    assert steps_by_next_run["2026-05-25T13:40:00+08:00"]["pending_first_run"] is True
    assert steps_by_next_run["2026-05-25T13:40:00+08:00"]["critical_for_intraday_simulation"] is False
    assert steps_by_next_run["2026-05-25T14:00:00+08:00"]["pending_first_run"] is False
    assert steps_by_next_run["2026-05-25T14:00:00+08:00"]["critical_for_intraday_simulation"] is True
    assert steps_by_next_run["2026-05-25T14:12:00+08:00"]["pending_first_run"] is True
    assert steps_by_next_run["2026-05-25T14:12:00+08:00"]["critical_for_intraday_simulation"] is True
    assert steps_by_next_run["2026-05-25T14:24:00+08:00"]["pending_first_run"] is True
    assert steps_by_next_run["2026-05-25T14:24:00+08:00"]["critical_for_intraday_simulation"] is True
    first_run = payload["next_window_plan"]["first_run_verification"]
    assert first_run["required"] is True
    assert first_run["critical_required"] is True
    assert first_run["verify_command"] == "atrade diagnose schedule --json"
    assert first_run["safe_to_auto_apply"] is True
    assert first_run["verify_command_contract_id"] == "diagnose_schedule"
    assert first_run["verify_command_contract"] == {
        "id": "diagnose_schedule",
        "risk_level": "read_only",
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "state_events": [],
    }
    assert "3 个首次运行任务" in first_run["summary"]
    assert [item["script"] for item in first_run["pending_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert [item["critical_for_intraday_simulation"] for item in first_run["pending_steps"]] == [
        False,
        True,
        True,
    ]
    assert payload["next_window_plan"]["next_action"] == {
        "type": "review_runtime_profile_activation",
        "label": "先复核运行 profile 激活",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }
    assert payload["next_window_plan"]["guardrails"] == {
        "read_only": True,
        "writes_environment": False,
        "places_order": False,
        "old_signal_auto_carryover": False,
    }
    assert payload["candidate_pool"]["core_count"] == 1
    assert payload["candidate_pool"]["watch_count"] == 1
    assert payload["strategy"]["candidate_flow"]["decisions"]["usable_buy_signal_count"] == 1
    assert payload["automation"]["latest_auto_trade_summary"]["no_trade_reason"] == (
        "buy_window_closed_with_signal"
    )
    assert payload["opportunity"]["status"] == "profile_review_required"
    assert payload["auto_readiness"]["status"] == "profile_review_required"
    assert payload["guardrails"]["real_order_auto_execution_allowed"] is False
    assert "下次买入窗口内运行 atrade paper auto-readiness --json 核查是否可提交 MX 模拟委托" not in payload[
        "recommendations"
    ]


def test_diagnose_schedule_reports_missed_intraday_catchup_jobs(tmp_path):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                },
            ],
        }),
        encoding="utf-8",
    )
    try:
        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            now=datetime(2026, 5, 22, 15, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    assert payload["diagnostic"] == "schedule"
    assert payload["status"] == "warning"
    assert payload["source"]["jobs_path"] == str(jobs_path)
    assert {item["name"] for item in payload["missed_jobs"]} == {
        "A股盘中候选-模拟闭环",
        "A股盘中模拟买入兜底",
    }
    assert payload["next_action"] == {
        "type": "inspect_hermes_trading_profile",
        "label": "检查 Hermes trading 调度",
        "command": "atrade diagnose schedule --json",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "diagnose_schedule",
    }


def test_diagnose_schedule_does_not_mark_jobs_created_after_schedule_as_missed(tmp_path):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T16:46:05+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "created_at": "2026-05-22T20:03:15+08:00",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    try:
        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            now=datetime(2026, 5, 22, 13, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    assert payload["status"] == "ok"
    assert payload["missed_jobs"] == []
    assert len(payload["pending_first_run_jobs"]) == 2
    assert all(item["pending_first_run"] for item in payload["tracked_jobs"])
    assert payload["tracked_jobs"][0]["created_at"] == "2026-05-22T16:46:05+08:00"
    assert payload["tracked_jobs"][0]["missed_times"] == []


def test_diagnose_schedule_surfaces_intraday_simulation_verification_plan(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    env_file = tmp_path / ".env"
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "模拟盘自动交易",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "0 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:01:31+08:00",
                    "last_status": "ok",
                    "next_run_at": "2026-05-25T14:00:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    env_file.write_text("ASTOCK_DATABASE_URL=sqlite:///runtime.db\n", encoding="utf-8")
    try:
        EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "verify_command": (
                        "ASTOCK_CONFIG_PROFILE=trend_swing "
                        "atrade paper auto-readiness --json"
                    ),
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                },
            },
        )

        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            env_file=env_file,
            now=datetime(2026, 5, 23, 0, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    simulation = payload["intraday_simulation"]
    assert simulation["status"] == "profile_review_required"
    assert simulation["profile_ready"] is False
    assert simulation["ready_for_next_window"] is False
    assert simulation["critical_job_count"] == 3
    assert simulation["pending_first_run_critical_count"] == 2
    assert "2 个关键模拟承接任务等待首次运行" in simulation["summary"]
    assert [item["script"] for item in simulation["scheduled_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert [item["role"] for item in simulation["scheduled_steps"]] == [
        "refresh_candidates",
        "auto_trade_check_or_submit_paper_order",
        "refresh_and_auto_trade_cycle",
        "auto_trade_check_or_submit_paper_order",
    ]
    assert simulation["first_run_verification"]["critical_required"] is True
    assert [item["script"] for item in simulation["first_run_verification"]["pending_steps"]] == [
        "a_stock_screener_refresh_intraday_silent.sh",
        "a_stock_intraday_execution_cycle_silent.sh",
        "a_stock_pipeline_auto_trade_silent.sh",
    ]
    assert simulation["next_action"]["command"] == (
        "atrade strategy profile-activation --target trend_swing --json"
    )
    assert simulation["guardrails"] == {
        "read_only": True,
        "runs_jobs": False,
        "places_order": False,
        "writes_environment": False,
        "old_signal_auto_carryover": False,
    }


def test_diagnose_schedule_reports_script_runtime_contract_for_next_window_jobs(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "profile" / "cron" / "jobs.json"
    scripts_dir = tmp_path / "profile" / "scripts"
    env_file = tmp_path / ".env"
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.parent.mkdir(parents=True)
    scripts_dir.mkdir(parents=True)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选池轻量刷新",
                    "script": "a_stock_screener_refresh_intraday_silent.sh",
                    "schedule": {"display": "40 13 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T13:40:00+08:00",
                },
                {
                    "name": "模拟盘自动交易",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "0 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:01:31+08:00",
                    "last_status": "ok",
                    "next_run_at": "2026-05-25T14:00:00+08:00",
                },
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
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
    env_file.write_text(
        "ASTOCK_DATABASE_URL=sqlite:///runtime.db\nASTOCK_CONFIG_PROFILE=trend_swing\n",
        encoding="utf-8",
    )
    try:
        EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "guardrails": {"manual_approval_required": True},
            },
        )

        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            env_file=env_file,
            now=datetime(2026, 5, 23, 0, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    contract = payload["runtime_contract"]
    assert contract["status"] == "ok"
    assert contract["script_dir"] == str(scripts_dir)
    assert contract["env_loader"] == {
        "entrypoint": "atrade",
        "loads_env_file": True,
        "respects_astock_no_env_file": True,
        "source": "astock_trading.platform.runtime_env.load_runtime_env",
    }
    assert contract["blocking_issues"] == []
    assert all(item["profile_env_file_loading_possible"] for item in contract["script_checks"])
    assert payload["intraday_simulation"]["runtime_contract"]["status"] == "ok"
    assert payload["intraday_simulation"]["ready_for_next_window"] is True


def test_diagnose_schedule_runtime_contract_ignores_non_simulation_llm_scripts(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "profile" / "cron" / "jobs.json"
    scripts_dir = tmp_path / "profile" / "scripts"
    env_file = tmp_path / ".env"
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.parent.mkdir(parents=True)
    scripts_dir.mkdir(parents=True)
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
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T14:24:00+08:00",
                },
                {
                    "name": "A股收盘 LLM 复盘",
                    "script": "a_stock_llm_close_embed.sh",
                    "schedule": {"display": "50 15 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "next_run_at": "2026-05-25T15:50:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    (scripts_dir / "a_stock_screener_refresh_intraday_silent.sh").write_text(
        "#!/usr/bin/env bash\natrade screener refresh --json\n",
        encoding="utf-8",
    )
    (scripts_dir / "a_stock_pipeline_auto_trade_silent.sh").write_text(
        "#!/usr/bin/env bash\natrade run-pipeline auto_trade --json\n",
        encoding="utf-8",
    )
    (scripts_dir / "a_stock_llm_close_embed.sh").write_text(
        "#!/usr/bin/env bash\nhermes --profile trading -z prompt\n",
        encoding="utf-8",
    )
    env_file.write_text(
        "ASTOCK_DATABASE_URL=sqlite:///runtime.db\nASTOCK_CONFIG_PROFILE=trend_swing\n",
        encoding="utf-8",
    )
    try:
        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            env_file=env_file,
            now=datetime(2026, 5, 23, 0, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    assert payload["runtime_contract"]["status"] == "ok"
    assert payload["runtime_contract"]["blocking_issues"] == []
    assert {item["script"] for item in payload["runtime_contract"]["script_checks"]} == {
        "a_stock_pipeline_auto_trade_silent.sh",
        "a_stock_screener_refresh_intraday_silent.sh",
    }
    assert payload["intraday_simulation"]["status"] in {"ready", "pending_first_run_verification"}


def test_diagnose_schedule_surfaces_intraday_simulation_when_jobs_missing(tmp_path):
    db_path = tmp_path / "runtime.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        payload = diagnose_schedule(
            conn,
            jobs_path=tmp_path / "missing-jobs.json",
            now=datetime(2026, 5, 23, 0, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    assert payload["status"] == "unknown"
    assert payload["intraday_simulation"]["status"] == "schedule_attention_required"
    assert payload["intraday_simulation"]["scheduled_step_count"] == 0
    assert payload["intraday_simulation"]["summary"] == (
        "未找到下个窗口的盘中候选刷新/模拟承接任务；需要先核查 Hermes 调度。"
    )
    assert payload["intraday_simulation"]["guardrails"]["runs_jobs"] is False


def test_diagnose_schedule_warns_when_recorded_profile_not_in_runtime_env(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    env_file = tmp_path / ".env"
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:12:40+08:00",
                    "last_status": "success",
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    env_file.write_text("ASTOCK_DATABASE_URL=sqlite:///runtime.db\n", encoding="utf-8")
    try:
        EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "verify_command": (
                        "ASTOCK_CONFIG_PROFILE=trend_swing "
                        "atrade paper auto-readiness --json"
                    ),
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                },
            },
        )

        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            env_file=env_file,
            now=datetime(2026, 5, 22, 7, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    assert payload["status"] == "warning"
    assert payload["missed_jobs"] == []
    assert payload["runtime_profile"]["status"] == "review_required"
    assert payload["runtime_profile"]["effective_profile"] == "default"
    assert payload["runtime_profile"]["env_profile"] is None
    assert payload["runtime_profile"]["source"]["profile_key_present"] is False
    assert payload["runtime_profile"]["activation_request_status"] == "recorded"
    assert payload["next_action"] == {
        "type": "review_runtime_profile_activation",
        "label": "复核运行 profile 激活",
        "command": "atrade strategy profile-activation --target trend_swing --json",
        "safe_to_auto_apply": False,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "strategy_profile_activation_review",
    }


def test_diagnose_schedule_accepts_runtime_env_profile_after_recorded_activation(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    env_file = tmp_path / ".env"
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    init_db(db_path)
    conn = connect(db_path)
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": "2026-05-22T14:12:40+08:00",
                    "last_status": "success",
                    "next_run_at": "2026-05-25T14:12:00+08:00",
                },
            ],
        }),
        encoding="utf-8",
    )
    env_file.write_text(
        "ASTOCK_DATABASE_URL=sqlite:///runtime.db\nASTOCK_CONFIG_PROFILE=trend_swing\n",
        encoding="utf-8",
    )
    try:
        EventStore(conn).append(
            "strategy:profile_activation",
            "strategy",
            "strategy.profile_activation.requested",
            {
                "status": "requires_manual_confirmation",
                "current_profile": "default",
                "target_profile": "trend_swing",
                "activation": {
                    "auto_apply": False,
                    "verify_command": (
                        "ASTOCK_CONFIG_PROFILE=trend_swing "
                        "atrade paper auto-readiness --json"
                    ),
                },
                "guardrails": {
                    "auto_apply": False,
                    "manual_approval_required": True,
                },
            },
        )

        payload = diagnose_schedule(
            conn,
            jobs_path=jobs_path,
            env_file=env_file,
            now=datetime(2026, 5, 22, 7, 0, tzinfo=timezone.utc),
        )
    finally:
        conn.close()

    assert payload["status"] == "ok"
    assert payload["runtime_profile"]["status"] == "ok"
    assert payload["runtime_profile"]["effective_profile"] == "trend_swing"
    assert payload["runtime_profile"]["source"]["profile_key_present"] is True


def test_diagnose_schedule_json_via_bin_trade_with_jobs_path(tmp_path):
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"
    jobs_path = tmp_path / "jobs.json"
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                },
            ],
        }),
        encoding="utf-8",
    )

    result = subprocess.run(
        [str(cli), "diagnose", "schedule", "--jobs-path", str(jobs_path), "--json"],
        cwd=root,
        env=_cli_env(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )

    payload = json.loads(result.stdout)
    assert payload["diagnostic"] == "schedule"
    assert payload["source"]["jobs_path"] == str(jobs_path)
    assert payload["tracked_jobs"][0]["name"] == "A股盘中模拟买入兜底"


def test_diagnose_strategy_points_to_schedule_when_buy_signal_missed_catchup(
    tmp_path,
    monkeypatch,
):
    db_path = tmp_path / "runtime.db"
    jobs_path = tmp_path / "jobs.json"
    init_db(db_path)
    conn = connect(db_path)
    monkeypatch.setenv("ASTOCK_HERMES_JOBS_PATH", str(jobs_path))
    monkeypatch.setattr(
        "astock_trading.platform.agent_diagnostics.utc_now",
        lambda: datetime(2026, 5, 22, 13, 0, tzinfo=timezone.utc),
    )
    jobs_path.write_text(
        json.dumps({
            "jobs": [
                {
                    "name": "A股盘中候选-模拟闭环",
                    "script": "a_stock_intraday_execution_cycle_silent.sh",
                    "schedule": {"display": "12 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                },
                {
                    "name": "A股盘中模拟买入兜底",
                    "script": "a_stock_pipeline_auto_trade_silent.sh",
                    "schedule": {"display": "24 14 * * 1-5"},
                    "enabled": True,
                    "state": "scheduled",
                    "last_run_at": None,
                    "last_status": None,
                },
            ],
        }),
        encoding="utf-8",
    )
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
        decision_event_id = events.append(
            "strategy:688981",
            "strategy",
            "decision.suggested",
            {
                "code": "688981",
                "name": "中芯国际",
                "action": "BUY",
                "score": 6.4,
                "position_pct": 0.1,
                "market_signal": "YELLOW",
            },
        )
        conn.execute(
            "UPDATE event_log SET occurred_at = ? WHERE event_id = ?",
            ("2026-05-22T06:21:32+00:00", decision_event_id),
        )
        events.append(
            "auto_trade:summary",
            "auto_trade",
            "auto_trade.summary",
            {
                "date": "2026-05-22",
                "dry_run": False,
                "buy_count": 0,
                "sell_count": 0,
                "no_trade_summary": {
                    "reason": "buy_window_closed_with_signal",
                    "message": "已有新鲜买入意向 1 条，但当前不在模拟买入窗口。",
                },
            },
        )

        payload = diagnose_strategy(conn)
    finally:
        conn.close()

    assert payload["candidate_flow"]["automation"]["schedule"]["status"] == "warning"
    assert payload["actionable_state"]["status"] == "buy_signal_waiting_window"
    assert payload["actionable_state"]["schedule_gap"]["status"] == "warning"
    assert payload["actionable_state"]["next_action"]["command"] == "atrade diagnose schedule --json"
