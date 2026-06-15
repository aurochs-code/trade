from __future__ import annotations

from datetime import datetime, timedelta, timezone

from astock_trading.market.health import evaluate_data_source_health
from astock_trading.market import health as health_module
from astock_trading.market.store import MarketStore


def test_evaluate_data_source_health_marks_missing_required_as_failed(mysql_conn):
    conn = mysql_conn
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


def test_latest_for_kinds_queries_each_kind_separately_to_avoid_large_sort():
    class FakeResult:
        def __init__(self, row):
            self._row = row

        def fetchone(self):
            return self._row

    class RecordingConnection:
        def __init__(self):
            self.queries = []
            self.rows = {
                "flow": {
                    "source": "baidu",
                    "kind": "flow",
                    "symbol": "000001",
                    "observed_at": "2026-06-12T02:00:00+00:00",
                    "payload_json": "{}",
                },
                "fund_flow": {
                    "source": "tushare",
                    "kind": "fund_flow",
                    "symbol": "000001",
                    "observed_at": "2026-06-12T03:00:00+00:00",
                    "payload_json": "{}",
                },
            }

        def execute(self, sql, params=()):
            normalized_sql = " ".join(sql.split())
            assert "WHERE kind = ?" in normalized_sql
            assert "kind IN" not in normalized_sql
            self.queries.append((normalized_sql, tuple(params)))
            return FakeResult(self.rows.get(params[0]))

    conn = RecordingConnection()

    latest = health_module._latest_for_kinds(conn, ("flow", "fund_flow"))

    assert latest["kind"] == "fund_flow"
    assert [params for _, params in conn.queries] == [("flow",), ("fund_flow",)]


def test_market_observations_has_kind_observed_index(mysql_conn):
    conn = mysql_conn
    try:
        rows = conn.execute(
            "SHOW INDEX FROM market_observations WHERE Key_name = 'idx_market_obs_kind_observed'"
        ).fetchall()
        index_names = {row["Key_name"] for row in rows}

        assert "idx_market_obs_kind_observed" in index_names
    finally:
        conn.close()


def test_evaluate_data_source_health_marks_stale_optional_as_warning(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_defers_stale_required_sources_on_weekend(mysql_conn):
    conn = mysql_conn
    try:
        now = datetime(2026, 5, 24, 0, 34, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "latest", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"main_net_inflow": 1})
        friday_close = "2026-05-22T07:36:00+00:00"
        conn.execute(
            "UPDATE market_observations SET observed_at = ?",
            (friday_close,),
        )

        result = evaluate_data_source_health(conn, now=now)

        assert result["status"] == "warning"
        assert result["required_missing"] == []
        assert result["deferred_required"] == [
            "hot_stocks",
            "northbound_realtime",
            "baidu_fund_flow",
        ]
        assert result["calendar_context"]["market_weekday"] is False
        assert result["checks"]["hot_stocks"]["stale_reason"] == "non_trading_day"
        assert result["checks"]["hot_stocks"]["next_refresh_required_before_next_window"] is True
    finally:
        conn.close()


def test_evaluate_data_source_health_accepts_market_announcements_alias(mysql_conn):
    conn = mysql_conn
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "hot_stocks", "2026-05-15", {"items": [1]})
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation("baidu", "fund_flow", "000858", {"main_net_inflow": 1})
        store.save_observation(
            "opencli",
            "market_announcements",
            "cn_a",
            {"items": [{"code": "603311", "title": "复牌公告"}]},
        )

        result = evaluate_data_source_health(conn, now=now)

        assert result["checks"]["announcements"]["status"] == "healthy"
        assert result["checks"]["announcements"]["kind"] == "market_announcements"
        assert "announcements" not in result["optional_missing"]
    finally:
        conn.close()


def test_evaluate_data_source_health_marks_empty_payload_as_degraded(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_tracks_financial_observations(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_includes_recent_provider_failures(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_tracks_circuit_open_as_skipped_not_unresolved(mysql_conn):
    conn = mysql_conn
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
            status="circuit_open",
            error_type="CircuitOpen",
            error_message="provider 熔断中，跳过本次调用",
            run_id="run_flow_circuit_open",
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["total_recent"] == 1
        assert failures["skipped_recent"] == 1
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 0
        assert failures["by_skipped_source"] == {"AkShareFlowAdapter": 1}
        assert failures["by_unresolved_source"] == {}
        assert failures["recent"][0]["skipped_by_circuit"] is True
        assert failures["unresolved"] == []
    finally:
        conn.close()


def test_evaluate_data_source_health_resolves_cross_platform_hot_stocks_with_hot_stocks_fallback(mysql_conn):
    conn = mysql_conn
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_provider_failure(
            source="OpenCliFinanceAdapter",
            target_kind="cross_platform_hot_stocks",
            symbol="cn_a",
            status="timeout",
            error_type="TimeoutError",
            error_message="provider 超时",
            run_id="run_opencli_timeout",
        )
        store.save_observation("AStockSignalAdapter", "hot_stocks", "latest", {"items": [1]})
        failure_time = (now - timedelta(minutes=2)).isoformat()
        success_time = (now - timedelta(minutes=1)).isoformat()
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'provider_failure'",
            (failure_time,),
        )
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?
               WHERE kind = 'hot_stocks' AND symbol = 'latest'""",
            (success_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 1
        assert failures["recent"][0]["resolved_by_fallback"] is True
        assert failures["recent"][0]["resolved_source"] == "AStockSignalAdapter"
        assert failures["unresolved"] == []
    finally:
        conn.close()


def test_evaluate_data_source_health_resolves_cross_platform_hot_stocks_with_nearby_prior_hot_stocks(mysql_conn):
    conn = mysql_conn
    try:
        now = datetime(2026, 5, 15, 3, 0, tzinfo=timezone.utc)
        store = MarketStore(conn)
        store.save_observation("astock_signal", "northbound_realtime", "cn_a", {"items": [1]})
        store.save_observation(
            "AStockSignalAdapter",
            "hot_stocks",
            "latest",
            {"items": [{"code": "688981"}]},
            run_id="run_evening",
        )
        store.save_provider_failure(
            source="OpenCliFinanceAdapter",
            target_kind="cross_platform_hot_stocks",
            symbol="cn_a",
            status="timeout",
            error_type="TimeoutError",
            error_message="provider 超时",
            run_id="run_evening",
        )
        success_time = (now - timedelta(seconds=30)).isoformat()
        failure_time = (now - timedelta(seconds=20)).isoformat()
        conn.execute(
            """UPDATE market_observations
               SET observed_at = ?
               WHERE kind = 'hot_stocks' AND symbol = 'latest'""",
            (success_time,),
        )
        conn.execute(
            "UPDATE market_observations SET observed_at = ? WHERE kind = 'provider_failure'",
            (failure_time,),
        )

        result = evaluate_data_source_health(conn, now=now)

        failures = result["provider_failures"]
        assert failures["unresolved_recent"] == 0
        assert failures["resolved_recent"] == 1
        assert failures["recent"][0]["resolved_by_fallback"] is True
        assert failures["recent"][0]["resolved_source"] == "AStockSignalAdapter"
        assert failures["recent"][0]["resolved_observed_at"] == success_time
    finally:
        conn.close()


def test_evaluate_data_source_health_marks_provider_failure_resolved_by_fallback(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_resolves_legacy_success_without_run_id(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_resolves_later_success_from_same_provider(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_resolves_later_success_from_new_run(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_warns_on_stale_candidate_pool(mysql_conn):
    conn = mysql_conn
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


def test_evaluate_data_source_health_warns_when_core_pool_is_empty(mysql_conn):
    conn = mysql_conn
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
