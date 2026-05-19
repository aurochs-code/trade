"""P6-2 多策略 profile 对比测试。"""

from __future__ import annotations

import json

from astock_trading.pipeline.strategy_profiles import compare_strategy_profiles, profile_config_hash
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.db import connect, init_db
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


def test_compare_strategy_profiles_reports_profile_evidence_and_records_event(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    db_path = tmp_path / "profiles.db"
    init_db(db_path)
    conn = connect(db_path)
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
    assert defensive["key_parameters"]["buy_threshold"] == 6.8
    assert defensive["evidence_status"] == "no_profile_runs"
    assert payload["recorded_event_id"]
    assert events[0]["payload"]["guardrails"]["auto_switch_profile"] is False


def test_compare_strategy_profiles_without_runs_marks_shadow_validation_needed(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    _write_profile_config(config_dir)
    db_path = tmp_path / "empty.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        payload = compare_strategy_profiles(conn, config_dir=config_dir, profiles=("trend_swing",), record=False)
    finally:
        conn.close()

    assert payload["status"] == "needs_shadow_validation"
    assert payload["profiles"][0]["evidence_status"] == "no_profile_runs"
    assert "先做影子运行" in payload["recommendations"][0]
