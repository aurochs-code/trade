from __future__ import annotations

from datetime import datetime, timezone

from astock_trading.market.store import MarketStore
from astock_trading.platform.cli.data_sources import (
    _latest_screener_source_quality,
    build_data_source_diagnosis,
)
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.data_source_diagnostics import data_source_blockers_for_new_trades
from astock_trading.platform.history_mirror import archive_signal_history


def test_build_data_source_diagnosis_includes_latest_screener_source_quality(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-20", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002138", {"net_inflow_1d": 1})
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
                    "has_sector": False,
                },
            },
            run_id="screener_101500",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-20",
            history_group_id="hist_diag_1",
            run_id="screener_101500",
            phase="screener",
            candidates=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 4.2,
                    "data_quality": "degraded",
                    "data_missing_fields": ["资金流"],
                }
            ],
        )

        payload = build_data_source_diagnosis(
            conn,
            now=datetime(2026, 5, 20, 3, 0, tzinfo=timezone.utc),
        )

        quality = payload["latest_screener_source_quality"]
        assert payload["diagnostic"] == "data_sources"
        assert quality["status"] == "warning"
        assert quality["run_id"] == "screener_101500"
        assert quality["history_group_id"] == "hist_diag_1"
        assert quality["coverage"]["quote"]["available"] == 1
        assert quality["coverage"]["flow"]["missing"] == 1
        assert quality["missing_fields"] == [{"field": "资金流", "count": 1}]
    finally:
        conn.close()


def test_data_source_diagnosis_does_not_block_new_trades_for_weekend_stale_gate_sources(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "latest", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "002384", {"items": [1]})
        conn.execute(
            "UPDATE market_observations SET observed_at = ?",
            ("2026-05-22T07:36:00+00:00",),
        )

        payload = build_data_source_diagnosis(
            conn,
            now=datetime(2026, 5, 24, 0, 34, tzinfo=timezone.utc),
        )
        blockers = data_source_blockers_for_new_trades(payload)

        assert payload["status"] == "warning"
        assert payload["health"]["required_missing"] == []
        assert payload["health"]["deferred_required"] == [
            "hot_stocks",
            "northbound_realtime",
            "baidu_fund_flow",
        ]
        assert blockers == []
        assert any("非交易日核心源自然过期" in item for item in payload["findings"])
    finally:
        conn.close()


def test_latest_screener_source_quality_ignores_newer_market_signal_snapshots(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"net_inflow_1d": 1})
        store.save_observation(
            "market_service",
            "snapshot",
            "688981",
            {
                "code": "688981",
                "name": "中芯国际",
                "completeness": {
                    "has_quote": True,
                    "has_technical": True,
                    "has_financial": True,
                    "has_flow": True,
                    "has_sentiment": True,
                    "has_sector": True,
                },
            },
            run_id="screener_intraday",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-22",
            history_group_id="hist_screener_intraday",
            run_id="screener_intraday",
            phase="screener",
            candidates=[
                {
                    "code": "688981",
                    "name": "中芯国际",
                    "total_score": 6.4,
                    "data_quality": "ok",
                }
            ],
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-22",
            history_group_id="hist_evening_empty",
            run_id="run_evening_latest",
            phase="evening",
            candidates=[],
        )
        conn.execute(
            "UPDATE signal_history_snapshots SET created_at = ? WHERE history_group_id = ?",
            ("2026-05-22T07:32:00+00:00", "hist_screener_intraday"),
        )
        conn.execute(
            "UPDATE signal_history_snapshots SET created_at = ? WHERE history_group_id = ?",
            ("2026-05-22T07:35:00+00:00", "hist_evening_empty"),
        )

        quality = _latest_screener_source_quality(conn)

        assert quality["run_id"] == "screener_intraday"
        assert quality["phase"] == "screener"
        assert quality["sample_size"] == 1
        assert quality["coverage"]["flow"]["available"] == 1
        assert quality["warnings"] == []
    finally:
        conn.close()


def test_new_trade_blockers_ignore_l1_failures_outside_active_candidate_pool(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"net_inflow_1d": 1})
        store.save_observation("astock_signal", "industry_comparison", "cn_a", {"items": [1]})
        store.save_observation("opencli", "market_announcements", "cn_a", {"items": [1]})
        store.save_observation("astock_signal", "research_reports", "688981", {"items": [1]})
        store.save_observation("astock_signal", "stock_news", "688981", {"items": [1]})
        store.save_observation("astock_signal", "basic_info", "688981", {"items": [1]})
        store.save_observation("market_service", "financial", "688981", {"roe": 10})
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

        diagnosis = build_data_source_diagnosis(conn)
        blockers = data_source_blockers_for_new_trades(diagnosis)

        assert diagnosis["provider_failures"]["unresolved_recent"] == 1
        assert [item["reason"] for item in blockers] == []
    finally:
        conn.close()


def test_build_data_source_diagnosis_keeps_non_active_provider_failures_non_actionable(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"net_inflow_1d": 1})
        store.save_observation("astock_signal", "industry_comparison", "cn_a", {"items": [1]})
        store.save_observation("opencli", "market_announcements", "cn_a", {"items": [1]})
        store.save_observation("astock_signal", "research_reports", "688981", {"items": [1]})
        store.save_observation("astock_signal", "stock_news", "688981", {"items": [1]})
        store.save_observation("astock_signal", "basic_info", "688981", {"items": [1]})
        store.save_observation("market_service", "financial", "688981", {"roe": 10})
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

        payload = build_data_source_diagnosis(
            conn,
            now=datetime(2026, 5, 22, 8, 0, tzinfo=timezone.utc),
        )

        assert payload["status"] == "ok"
        assert payload["findings"] == []
        assert payload["recommendations"] == []
        assert payload["provider_failures"]["unresolved_recent"] == 1
        assert payload["provider_incidents"] == {
            "non_actionable_unresolved_recent": 1,
            "actionable_unresolved_recent": 0,
        }
    finally:
        conn.close()


def test_new_trade_blockers_keep_l1_failures_for_active_candidate_pool(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"net_inflow_1d": 1})
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

        diagnosis = build_data_source_diagnosis(conn)
        blockers = data_source_blockers_for_new_trades(diagnosis)

        assert blockers[0]["reason"] == "unresolved_l1_provider_failures"
        assert blockers[0]["count"] == 1
        assert blockers[0]["items"][0]["symbol"] == "300475"
    finally:
        conn.close()


def test_new_trade_blockers_ignore_l1_coverage_gaps_outside_active_candidate_pool(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"net_inflow_1d": 1})
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("688981", "core", "中芯国际", 6.4, "2026-05-22", "2026-05-22", 3, ""),
        )
        store.save_observation(
            "market_service",
            "snapshot",
            "300475",
            {
                "code": "300475",
                "name": "香农芯创",
                "completeness": {
                    "has_quote": True,
                    "has_technical": True,
                    "has_financial": True,
                    "has_flow": False,
                    "has_sentiment": True,
                    "has_sector": True,
                },
            },
            run_id="screener_non_active_degraded",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-22",
            history_group_id="hist_non_active_degraded",
            run_id="screener_non_active_degraded",
            phase="screener",
            candidates=[
                {
                    "code": "300475",
                    "name": "香农芯创",
                    "total_score": 5.3,
                    "data_quality": "degraded",
                    "data_missing_fields": ["资金流"],
                }
            ],
        )

        diagnosis = build_data_source_diagnosis(conn)
        blockers = data_source_blockers_for_new_trades(diagnosis)

        assert diagnosis["latest_screener_source_quality"]["coverage"]["flow"]["missing"] == 1
        assert [item["reason"] for item in blockers] == []
    finally:
        conn.close()


def test_data_source_diagnosis_does_not_warn_for_tiny_failed_scan_outside_active_pool(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"net_inflow_1d": 1})
        store.save_observation("astock_signal", "industry_comparison", "cn_a", {"items": [1]})
        store.save_observation("opencli", "market_announcements", "cn_a", {"items": [1]})
        store.save_observation("astock_signal", "research_reports", "688981", {"items": [1]})
        store.save_observation("astock_signal", "stock_news", "688981", {"items": [1]})
        store.save_observation("astock_signal", "basic_info", "688981", {"items": [1]})
        store.save_observation("market_service", "financial", "688981", {"roe": 10})
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("688981", "core", "中芯国际", 6.4, "2026-05-22", "2026-05-22", 3, ""),
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-22",
            history_group_id="hist_tiny_failed_scan",
            run_id="screener_tiny_failed_scan",
            phase="screener",
            candidates=[
                {
                    "code": "300475",
                    "name": "香农芯创",
                    "total_score": 0,
                    "data_quality": "error",
                    "data_missing_fields": ["行情", "技术指标", "基本面", "资金流"],
                }
            ],
        )

        payload = build_data_source_diagnosis(
            conn,
            now=datetime(2026, 5, 22, 8, 0, tzinfo=timezone.utc),
        )
        blockers = data_source_blockers_for_new_trades(payload)

        assert payload["status"] == "ok"
        assert payload["findings"] == []
        assert payload["recommendations"] == []
        assert payload["latest_screener_source_quality"]["status"] == "warning"
        assert payload["latest_screener_source_quality"]["actionable"] is False
        assert payload["latest_screener_source_quality"]["score_quality_items"][0]["code"] == "300475"
        assert blockers == []
    finally:
        conn.close()


def test_latest_screener_source_quality_treats_later_flow_observation_as_repaired(tmp_path):
    db_path = tmp_path / "diagnose.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-22", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("AkShareFlowAdapter", "fund_flow", "688981", {"items": [1]})
        store.save_observation(
            "market_service",
            "snapshot",
            "600584",
            {
                "code": "600584",
                "name": "长电科技",
                "completeness": {
                    "has_quote": True,
                    "has_technical": True,
                    "has_financial": True,
                    "has_flow": False,
                    "has_sentiment": True,
                    "has_sector": True,
                },
            },
            run_id="screener_missing_flow",
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-05-22",
            history_group_id="hist_missing_flow",
            run_id="screener_missing_flow",
            phase="screener",
            candidates=[
                {
                    "code": "600584",
                    "name": "长电科技",
                    "total_score": 5.5,
                    "data_quality": "degraded",
                    "data_missing_fields": ["资金流"],
                }
            ],
        )
        conn.execute(
            "UPDATE signal_history_snapshots SET created_at = ? WHERE history_group_id = ?",
            ("2026-05-22T07:33:00+00:00", "hist_missing_flow"),
        )
        store.save_observation(
            "AkShareFlowAdapter",
            "fund_flow",
            "600584",
            {"net_inflow_1d": 1, "main_force_ratio": 0.5},
            run_id="stock_analyze_repair",
        )
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?
               WHERE kind = 'fund_flow' AND symbol = '600584'""",
            ("2026-05-22T07:50:00+00:00",),
        )

        payload = build_data_source_diagnosis(
            conn,
            now=datetime(2026, 5, 22, 8, 0, tzinfo=timezone.utc),
        )

        flow_coverage = payload["latest_screener_source_quality"]["coverage"]["flow"]
        assert flow_coverage["missing"] == 0
        assert flow_coverage["repaired_symbols"] == ["600584"]
        assert flow_coverage["raw_missing_symbols"] == ["600584"]
        assert not payload["latest_screener_source_quality"]["warnings"]
        assert "最近筛选逐票资金流覆盖率" not in " ".join(payload["findings"])
    finally:
        conn.close()


def test_latest_screener_source_quality_avoids_unbounded_created_at_sort():
    class Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return self._rows

    class GuardedConn:
        def execute(self, sql, params=None):
            normalized = " ".join(sql.split())
            if (
                "FROM signal_history_snapshots" in normalized
                and "ORDER BY created_at DESC" in normalized
                and "snapshot_date =" not in normalized
            ):
                raise AssertionError("不能按 created_at 对全部历史信号镜像做无界排序")
            if "MAX(snapshot_date)" in normalized:
                return Result([{"snapshot_date": None}])
            return Result([])

    result = _latest_screener_source_quality(GuardedConn())

    assert result["status"] == "empty"
