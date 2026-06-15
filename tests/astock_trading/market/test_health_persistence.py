"""数据源健康快照持久化测试。"""

import json
from datetime import datetime, timezone

from astock_trading.market.health import record_data_source_health_snapshot


class RecordingConnection:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((" ".join(sql.split()), params))


def test_record_data_source_health_snapshot_writes_market_observation():
    conn = RecordingConnection()
    health = {
        "status": "warning",
        "required_missing": [],
        "optional_missing": ["core_pool"],
        "checks": {"core_pool": {"status": "empty"}},
        "provider_failures": {"unresolved_recent": 0},
    }

    observation_id = record_data_source_health_snapshot(
        conn,
        health,
        run_id="run_health",
        observed_at=datetime(2026, 6, 15, 1, 30, tzinfo=timezone.utc),
    )

    assert observation_id
    sql, params = conn.calls[0]
    assert "market_observations" in sql
    assert params[1:6] == (
        "market_health",
        "data_source_health",
        "cn_a",
        "2026-06-15T01:30:00+00:00",
        "run_health",
    )
    payload = json.loads(params[6])
    assert payload["status"] == "warning"
    assert payload["optional_missing"] == ["core_pool"]
    assert payload["recorded_at"] == "2026-06-15T01:30:00+00:00"
