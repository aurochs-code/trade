from __future__ import annotations

from datetime import datetime, timedelta, timezone

from astock_trading.market.health import evaluate_data_source_health
from astock_trading.market.store import MarketStore
from astock_trading.platform.db import connect, init_db


def test_evaluate_data_source_health_marks_missing_required_as_failed(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        MarketStore(conn).save_observation(
            "astock_signal",
            "hot_stocks",
            "2026-05-15",
            {"items": [1, 2, 3]},
        )

        result = evaluate_data_source_health(conn, now=now)

        assert result["status"] == "failed"
        assert "northbound_realtime" in result["required_missing"]
        assert result["checks"]["hot_stocks"]["status"] == "healthy"
    finally:
        conn.close()


def test_evaluate_data_source_health_marks_stale_optional_as_warning(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "flow", "000858", {"main_net_inflow": 1})
        store.save_observation("astock_signal", "announcements", "000858", {"items": []})
        stale_time = (now - timedelta(days=10)).isoformat()
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'announcements'",
            (stale_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        assert result["status"] == "warning"
        assert result["required_missing"] == []
        assert "announcements" in result["optional_missing"]
        assert result["checks"]["announcements"]["status"] == "degraded"
    finally:
        conn.close()


def test_evaluate_data_source_health_marks_empty_payload_as_degraded(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_observation("astock_signal", "industry_comparison", "cn_a", {"items": []})

        result = evaluate_data_source_health(conn, now=now)

        assert result["status"] == "warning"
        assert result["checks"]["industry_comparison"]["status"] == "degraded"
        assert "industry_comparison" in result["optional_missing"]
    finally:
        conn.close()


def test_evaluate_data_source_health_tracks_financial_observations(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_observation("MarketService", "financial", "000858", {"roe": 12.0})

        result = evaluate_data_source_health(conn, now=now)

        assert result["checks"]["financial"]["status"] == "healthy"
        assert result["checks"]["financial"]["symbol"] == "000858"
        assert "financial" not in result["optional_missing"]
    finally:
        conn.close()


def test_evaluate_data_source_health_includes_recent_provider_failures(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_provider_failure(
            source="BaiduFundFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="parse_error",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            run_id="run_flow_failure",
            details={
                "provider_diagnostic": {
                    "subsource_errors": [
                        {"subsource": "eastmoney_fund_flow", "status": "em_fund_flow_failed"}
                    ]
                }
            },
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["total_recent"] == 1
        assert failures["unresolved_recent"] == 1
        assert failures["resolved_recent"] == 0
        assert failures["by_source"] == {"BaiduFundFlowAdapter": 1}
        assert failures["by_target_kind"] == {"fund_flow": 1}
        assert failures["by_unresolved_source"] == {"BaiduFundFlowAdapter": 1}
        assert failures["recent"][0]["source"] == "BaiduFundFlowAdapter"
        assert failures["recent"][0]["target_kind"] == "fund_flow"
        assert failures["recent"][0]["status"] == "parse_error"
        assert failures["recent"][0]["symbol"] == "603215"
        assert failures["recent"][0]["details"]["provider_diagnostic"]["subsource_errors"] == [
            {"subsource": "eastmoney_fund_flow", "status": "em_fund_flow_failed"}
        ]
        assert failures["recent"][0]["resolved_by_fallback"] is False
        assert failures["unresolved"][0]["symbol"] == "603215"
    finally:
        conn.close()


def test_evaluate_data_source_health_marks_provider_failure_resolved_by_fallback(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_provider_failure(
            source="BaiduFundFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="parse_error",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            run_id="run_flow_fallback",
        )
        store.save_observation(
            "AkShareFlowAdapter",
            "fund_flow",
            "603215",
            {"net_inflow_1d": 1, "main_force_ratio": 0.2},
            run_id="run_flow_fallback",
        )
        failure_time = (now - timedelta(minutes=2)).isoformat()
        success_time = (now - timedelta(minutes=1)).isoformat()
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'provider_failure'",
            (failure_time,),
        )
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?
               WHERE kind = 'fund_flow' AND symbol = '603215'""",
            (success_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["total_recent"] == 1
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 1
        assert failures["by_unresolved_source"] == {}
        assert failures["unresolved"] == []
        assert failures["recent"][0]["resolved_by_fallback"] is True
        assert failures["recent"][0]["resolved_source"] == "AkShareFlowAdapter"
        assert failures["recent"][0]["resolved_observed_at"] == success_time
    finally:
        conn.close()


def test_evaluate_data_source_health_resolves_legacy_success_without_run_id(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_provider_failure(
            source="BaiduFundFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="parse_error",
            error_type="JSONDecodeError",
            error_message="Expecting value",
            run_id="run_legacy_fallback",
        )
        store.save_observation(
            "AkShareFlowAdapter",
            "fund_flow",
            "603215",
            {"net_inflow_1d": 1, "main_force_ratio": 0.2},
        )
        failure_time = (now - timedelta(minutes=2)).isoformat()
        success_time = (now - timedelta(minutes=1)).isoformat()
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'provider_failure'",
            (failure_time,),
        )
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?, run_id = NULL
               WHERE kind = 'fund_flow' AND symbol = '603215'""",
            (success_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 1
        assert failures["recent"][0]["resolved_by_fallback"] is True
        assert failures["recent"][0]["resolved_source"] == "AkShareFlowAdapter"
    finally:
        conn.close()


def test_evaluate_data_source_health_resolves_later_success_from_same_provider(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_provider_failure(
            source="AkShareFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="provider_error",
            error_type="TypeError",
            error_message="Object of type int64 is not JSON serializable",
            run_id="run_same_provider_recovered",
        )
        store.save_observation(
            "AkShareFlowAdapter",
            "fund_flow",
            "603215",
            {"net_inflow_1d": 1, "main_force_ratio": 0.2},
        )
        failure_time = (now - timedelta(minutes=2)).isoformat()
        success_time = (now - timedelta(minutes=1)).isoformat()
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'provider_failure'",
            (failure_time,),
        )
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?, run_id = NULL
               WHERE kind = 'fund_flow' AND symbol = '603215'""",
            (success_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 1
        assert failures["recent"][0]["resolved_by_fallback"] is True
        assert failures["recent"][0]["resolved_source"] == "AkShareFlowAdapter"
    finally:
        conn.close()


def test_evaluate_data_source_health_resolves_later_success_from_new_run(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        store.save_provider_failure(
            source="AkShareFlowAdapter",
            target_kind="fund_flow",
            symbol="603215",
            status="timeout",
            error_type="TimeoutError",
            error_message="provider 超时",
            run_id="run_before_fix",
        )
        store.save_observation(
            "AkShareFlowAdapter",
            "fund_flow",
            "603215",
            {"net_inflow_1d": 1, "main_force_ratio": 0.2},
            run_id="run_after_fix",
        )
        failure_time = (now - timedelta(minutes=2)).isoformat()
        success_time = (now - timedelta(minutes=1)).isoformat()
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'provider_failure'",
            (failure_time,),
        )
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?
               WHERE kind = 'fund_flow' AND symbol = '603215'""",
            (success_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 1
        assert failures["recent"][0]["resolved_by_fallback"] is True
        assert failures["recent"][0]["resolved_source"] == "AkShareFlowAdapter"
    finally:
        conn.close()


def test_evaluate_data_source_health_warns_on_stale_candidate_pool(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        stale_time = (now - timedelta(days=3)).isoformat()
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("002138", "core", "双环传动", 7.5, stale_time, stale_time),
        )

        result = evaluate_data_source_health(conn, now=now)

        assert result["status"] == "warning"
        assert result["checks"]["candidate_pool_freshness"]["status"] == "degraded"
        assert result["checks"]["candidate_pool_freshness"]["core_count"] == 1
        assert "candidate_pool_freshness" in result["optional_missing"]
    finally:
        conn.close()


def test_evaluate_data_source_health_warns_when_core_pool_is_empty(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"net_inflow_1d": 1})
        fresh_time = (now - timedelta(hours=1)).isoformat()
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            ("002138", "watch", "双环传动", 7.5, fresh_time, fresh_time),
        )

        result = evaluate_data_source_health(conn, now=now)

        assert result["status"] == "warning"
        assert result["checks"]["core_pool"]["status"] == "empty"
        assert result["checks"]["core_pool"]["core_count"] == 0
        assert "core_pool" in result["optional_missing"]
    finally:
        conn.close()
