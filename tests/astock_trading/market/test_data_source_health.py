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
