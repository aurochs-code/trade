from __future__ import annotations

from datetime import datetime, timezone

from astock_trading.market.store import MarketStore
from astock_trading.platform.cli.data_sources import (
    _latest_screener_source_quality,
    build_data_source_diagnosis,
)
from astock_trading.platform.db import connect, init_db
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
