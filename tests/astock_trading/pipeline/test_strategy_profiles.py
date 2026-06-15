"""P6-2 多策略 profile 对比测试。"""

from __future__ import annotations

import json
from pathlib import Path

from astock_trading.pipeline.strategy_profiles import (
    apply_strategy_profile_activation,
    build_strategy_profile_activation_plan,
    compare_strategy_profiles,
    profile_config_hash,
    propose_strategy_allocation,
)
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.events import EventStore


def _write_profile_config(tmp_path) -> None:
    (tmp_path / "profiles").mkdir()
    (tmp_path / "strategy.yaml").write_text(
        """
strategy:
  scoring:
    weights:
      technical: 4
      fundamental: 2
      flow: 3
      sentiment: 1
    thresholds:
      buy: 6.2
      watch: 5.0
      reject: 4.0
    decision_gates:
      require_entry_signal_for_buy: true
      min_data_quality_for_buy: degraded
      max_missing_fields_for_buy: 1
  risk:
    position:
      single_max: 0.2
      total_max: 0.6
      weekly_max: 2
  auto_trade:
    enabled: true
    dry_run: true
""",
        encoding="utf-8",
    )
    (tmp_path / "profiles" / "trend_swing.yaml").write_text(
        """
strategy:
  scoring:
    thresholds:
      buy: 6.0
      watch: 5.0
      reject: 4.0
""",
        encoding="utf-8",
    )
    (tmp_path / "profiles" / "short_continuation.yaml").write_text(
        """
strategy:
  scoring:
    thresholds:
      buy: 6.1
      watch: 5.2
      reject: 4.0
  continuation:
    scoring:
      top_n: 3
      hold_days: [1, 2, 3]
""",
        encoding="utf-8",
    )
    (tmp_path / "profiles" / "defensive_watch.yaml").write_text(
        """
strategy:
  scoring:
    thresholds:
      buy: 6.8
      watch: 5.2
      reject: 4.0
    decision_gates:
      min_data_quality_for_buy: ok
      max_missing_fields_for_buy: 0
""",
        encoding="utf-8",
    )


def test_compare_strategy_profiles_reports_profile_evidence_and_records_event(tmp_path, mysql_conn):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    conn = mysql_conn
    try:
        store = EventStore(conn)
        trend_config, _ = ConfigRegistry(config_dir=config_dir, profile="trend_swing").load_and_validate()
        trend_hash = profile_config_hash(trend_config)
        conn.execute(
            """INSERT INTO config_versions
               (config_version, config_hash, config_json, created_at)
               VALUES (?, ?, ?, ?)""",
            ("v_trend", trend_hash, json.dumps(trend_config, ensure_ascii=False), "2026-05-19T00:00:00+00:00"),
        )
        conn.execute(
            """INSERT INTO run_log
               (run_id, run_type, scope, config_version, status, started_at, finished_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            ("run_trend", "scoring", "cn_a", "v_trend", "completed", "2026-05-19T00:00:00+00:00", "2026-05-19T00:01:00+00:00"),
        )
        store.append(
            "strategy:600703",
            "strategy",
            "decision.suggested",
            {"code": "600703", "action": "BUY", "score": 6.7},
            metadata={"config_version": "v_trend", "run_id": "run_trend"},
        )
        store.append(
            "trade:600703:order1",
            "trade",
            "trade.review.recorded",
            {"code": "600703", "latest_return_pct": 0.04},
            metadata={"config_version": "v_trend"},
        )

        payload = compare_strategy_profiles(
            conn,
            config_dir=config_dir,
            profiles=("trend_swing", "short_continuation", "defensive_watch"),
            record=True,
        )
        events = store.query(event_type="strategy.profile_comparison.proposed")
    finally:
        conn.close()

    trend = next(item for item in payload["profiles"] if item["name"] == "trend_swing")
    defensive = next(item for item in payload["profiles"] if item["name"] == "defensive_watch")
    assert payload["analysis"] == "strategy_profile_comparison"
    assert payload["guardrails"]["auto_switch_profile"] is False
    assert trend["evidence_status"] == "has_profile_runs"
    assert trend["run_count"] == 1
    assert trend["decision_counts"]["BUY"] == 1
    assert trend["trade_review"]["sample_count"] == 1
    assert trend["trade_review"]["avg_return_pct"] == 0.04
    assert trend["key_parameters"]["trial_buy_threshold"] == 6.0
    assert trend["key_parameters"]["trial_buy_entry_signal_threshold"] == 5.5
    assert defensive["key_parameters"]["buy_threshold"] == 6.8
    assert defensive["key_parameters"]["trial_buy_threshold"] == 6.8
    assert defensive["key_parameters"]["trial_buy_entry_signal_threshold"] == 6.3
    assert defensive["evidence_status"] == "no_profile_runs"
    assert payload["recorded_event_id"]
    assert events[0]["payload"]["guardrails"]["auto_switch_profile"] is False


def test_compare_strategy_profiles_without_runs_marks_shadow_validation_needed(tmp_path, mysql_conn):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    conn = mysql_conn
    try:
        payload = compare_strategy_profiles(conn, config_dir=config_dir, profiles=("trend_swing",), record=False)
    finally:
        conn.close()

    assert payload["status"] == "needs_shadow_validation"
    assert payload["profiles"][0]["evidence_status"] == "no_profile_runs"
    assert "先做影子运行" in payload["recommendations"][0]


def test_build_strategy_profile_activation_plan_requires_manual_confirmation(tmp_path, monkeypatch, mysql_conn):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    conn = mysql_conn
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    try:
        payload = build_strategy_profile_activation_plan(
            conn,
            config_dir=config_dir,
            target_profile="trend_swing",
            record=True,
        )
        events = EventStore(conn).query(event_type="strategy.profile_activation.requested")
    finally:
        conn.close()

    assert payload["analysis"] == "strategy_profile_activation_plan"
    assert payload["status"] == "requires_manual_confirmation"
    assert payload["current_profile"] == "default"
    assert payload["target_profile"] == "trend_swing"
    assert payload["activation"]["export_command"] == "export ASTOCK_CONFIG_PROFILE=trend_swing"
    assert payload["activation"]["verify_command"] == (
        "ASTOCK_CONFIG_PROFILE=trend_swing atrade paper auto-readiness --json"
    )
    assert payload["activation"]["run_command_requires_user_approval"] is True
    assert payload["activation"]["run_command_contract_id"] == "run_pipeline_auto_trade"
    assert payload["activation"]["auto_apply"] is False
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
    assert payload["post_approval_checklist"]["status"] == "waiting_manual_approval"
    assert payload["post_approval_checklist"]["summary"] == (
        "人工批准写入 trend_swing 后，先运行只读预检和调度核查；"
        "确认同日买入意向、买入窗口和调度首跑后，auto_trade 才能单独审批执行。"
    )
    assert [item["command"] for item in payload["post_approval_checklist"]["steps"]] == [
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json",
        "atrade diagnose schedule --json",
        "atrade paper auto-readiness --json",
        "atrade risk trial-guard --json",
    ]
    assert all(
        item["command_contract"]["writes_order"] is False
        for item in payload["post_approval_checklist"]["steps"]
    )
    assert payload["post_approval_checklist"]["paper_order_execution"] == {
        "command": "atrade run-pipeline auto_trade --json",
        "allowed_only_after": [
            "运行 profile 已确认",
            "调度核查通过",
            "同日新鲜买入意向已形成",
            "买入窗口打开",
            "模拟预检和试运行护栏通过",
        ],
        "requires_separate_user_approval": True,
        "command_contract": {
            "id": "run_pipeline_auto_trade",
            "risk_level": "paper_order_execution",
            "writes_state": True,
            "writes_environment": False,
            "writes_order": True,
            "requires_user_approval": True,
            "state_events": ["auto_trade.diagnostic", "auto_trade.summary", "paper.order.submitted"],
        },
    }
    assert payload["guardrails"]["manual_approval_required"] is True
    assert payload["guardrails"]["modifies_environment"] is False
    assert payload["summary"] == "当前执行 profile 为 default；目标 trend_swing 需要人工确认后才能写入运行环境。"
    assert payload["next_action"] == {
        "type": "confirm_profile_activation_apply",
        "label": "确认写入运行 profile",
        "command": "atrade strategy profile-activation --target trend_swing --apply-env --yes --json",
        "safe_to_auto_apply": False,
        "writes_state": True,
        "writes_environment": True,
        "writes_order": False,
        "requires_user_approval": True,
        "risk_level": "environment_write",
        "command_contract_id": "strategy_profile_activation_apply",
    }
    assert payload["recorded_event_id"]
    assert events[0]["payload"]["target_profile"] == "trend_swing"
    assert events[0]["payload"]["next_action"]["command"] == (
        "atrade strategy profile-activation --target trend_swing --apply-env --yes --json"
    )


def test_apply_strategy_profile_activation_requires_explicit_confirmation(
    tmp_path,
    monkeypatch,
    mysql_conn,
):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "ASTOCK_DATABASE_URL=mysql+pymysql://user:pass@127.0.0.1:3306/runtime\n",
        encoding="utf-8",
    )
    conn = mysql_conn
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    try:
        payload = apply_strategy_profile_activation(
            conn,
            config_dir=config_dir,
            target_profile="trend_swing",
            env_file=env_file,
            confirm=False,
        )
        events = EventStore(conn).query(event_type="strategy.profile_activation.applied")
    finally:
        conn.close()

    assert payload["status"] == "confirmation_required"
    assert payload["guardrails"]["modifies_environment"] is False
    assert payload["runtime_env"]["before_profile"] is None
    assert "ASTOCK_CONFIG_PROFILE" not in env_file.read_text(encoding="utf-8")
    assert events == []


def test_apply_strategy_profile_activation_updates_env_and_records_event(
    tmp_path,
    monkeypatch,
    mysql_conn,
):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "ASTOCK_DATABASE_URL=mysql+pymysql://user:pass@127.0.0.1:3306/runtime\n"
        "ASTOCK_CONFIG_PROFILE=default\n",
        encoding="utf-8",
    )
    conn = mysql_conn
    monkeypatch.delenv("ASTOCK_CONFIG_PROFILE", raising=False)
    try:
        payload = apply_strategy_profile_activation(
            conn,
            config_dir=config_dir,
            target_profile="trend_swing",
            env_file=env_file,
            confirm=True,
        )
        events = EventStore(conn).query(event_type="strategy.profile_activation.applied")
    finally:
        conn.close()

    assert payload["status"] == "applied"
    assert payload["guardrails"]["modifies_environment"] is True
    assert payload["guardrails"]["manual_approval_required"] is True
    assert payload["runtime_env"]["before_profile"] == "default"
    assert payload["runtime_env"]["after_profile"] == "trend_swing"
    assert payload["runtime_env"]["backup_path"]
    assert Path(payload["runtime_env"]["backup_path"]).exists()
    assert "ASTOCK_CONFIG_PROFILE=trend_swing" in env_file.read_text(encoding="utf-8")
    assert events[0]["payload"]["target_profile"] == "trend_swing"
    assert events[0]["payload"]["runtime_env"]["after_profile"] == "trend_swing"


def _insert_profile_version(conn, config_dir, profile: str, version: str) -> str:
    config, _ = ConfigRegistry(config_dir=config_dir, profile=profile).load_and_validate()
    config_hash = profile_config_hash(config)
    conn.execute(
        """INSERT INTO config_versions
           (config_version, config_hash, config_json, created_at)
           VALUES (?, ?, ?, ?)""",
        (version, config_hash, json.dumps(config, ensure_ascii=False), "2026-05-19T00:00:00+00:00"),
    )
    conn.execute(
        """INSERT INTO run_log
           (run_id, run_type, scope, config_version, status, started_at, finished_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"run_{profile}",
            "scoring",
            "cn_a",
            version,
            "completed",
            "2026-05-19T00:00:00+00:00",
            "2026-05-19T00:01:00+00:00",
        ),
    )
    return version


def _append_reviews(store: EventStore, *, profile_version: str, profile: str, returns: list[float]) -> None:
    for index, return_pct in enumerate(returns, start=1):
        store.append(
            f"trade:{profile}:order{index}",
            "trade",
            "trade.review.recorded",
            {"code": f"60070{index}", "latest_return_pct": return_pct},
            metadata={"config_version": profile_version},
        )


def test_propose_strategy_allocation_isolates_capital_and_flags_weak_profiles(tmp_path, mysql_conn):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    conn = mysql_conn
    try:
        store = EventStore(conn)
        trend_version = _insert_profile_version(conn, config_dir, "trend_swing", "v_trend")
        short_version = _insert_profile_version(conn, config_dir, "short_continuation", "v_short")
        _append_reviews(store, profile_version=trend_version, profile="trend", returns=[0.04, 0.03, -0.01])
        _append_reviews(store, profile_version=short_version, profile="short", returns=[-0.03, -0.02, -0.01])

        payload = propose_strategy_allocation(
            conn,
            config_dir=config_dir,
            profiles=("trend_swing", "short_continuation", "defensive_watch"),
            total_capital=500000,
            min_samples=3,
            record=True,
        )
        events = store.query(event_type="strategy.capital_allocation.proposed")
    finally:
        conn.close()

    trend = next(item for item in payload["capital_buckets"] if item["profile"] == "trend_swing")
    short = next(item for item in payload["capital_buckets"] if item["profile"] == "short_continuation")
    defensive = next(item for item in payload["capital_buckets"] if item["profile"] == "defensive_watch")
    assert payload["analysis"] == "strategy_capital_allocation"
    assert payload["guardrails"]["auto_apply"] is False
    assert trend["scope"] == "strategy_trend_swing"
    assert trend["action"] == "activate_candidate"
    assert trend["suggested_capital_cents"] > 0
    assert short["action"] == "pause_candidate"
    assert short["suggested_capital_cents"] == 0
    assert defensive["action"] == "shadow_validate"
    assert payload["weak_strategy_review"]["pause_candidates"] == ["short_continuation"]
    assert payload["recorded_event_id"]
    assert events[0]["payload"]["guardrails"]["auto_apply"] is False


def test_propose_strategy_allocation_requires_shadow_data_before_allocating(tmp_path, mysql_conn):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    conn = mysql_conn
    try:
        payload = propose_strategy_allocation(
            conn,
            config_dir=config_dir,
            profiles=("trend_swing",),
            total_capital=500000,
            min_samples=3,
            record=False,
        )
    finally:
        conn.close()

    assert payload["status"] == "needs_shadow_validation"
    assert payload["capital_buckets"][0]["action"] == "shadow_validate"
    assert payload["capital_buckets"][0]["suggested_capital_cents"] == 0
