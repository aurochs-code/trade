"""历史信号镜像归档与诊断。"""

from __future__ import annotations

import json

from astock_trading.platform.history_mirror import (
    archive_signal_history,
    diagnose_signal_history,
    load_signal_history_bundles,
    rebuild_signal_history_discovery_index,
    _market_snapshot,
)


def test_archive_and_diagnose_signal_history_bundle(mysql_conn):
    conn = mysql_conn
    try:
        group_id = archive_signal_history(
            conn,
            snapshot_date="2026-05-19",
            history_group_id="hist_20260519_1",
            run_id="screener_101500",
            phase="screener",
            market={
                "signal": "YELLOW",
                "indices": {"上证指数": {"change_pct": 0.2}},
            },
            pool=[
                {"code": "002138", "name": "双环传动", "pool_tier": "watch", "score": 5.8},
            ],
            candidates=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 5.8,
                    "entry_signal": False,
                    "data_quality": "ok",
                    "hard_veto_signals": [],
                },
            ],
            decisions=[
                {
                    "code": "002138",
                    "name": "双环传动",
                    "action": "WATCH",
                    "score": 5.8,
                    "notes": ["缺少入场信号"],
                },
            ],
        )

        payload = diagnose_signal_history(
            conn,
            snapshot_date="2026-05-19",
            history_group_id=group_id,
            code="002138",
        )
    finally:
        conn.close()

    assert group_id == "hist_20260519_1"
    assert payload["status"] == "ok"
    assert payload["snapshot_date"] == "2026-05-19"
    assert payload["history_group_id"] == "hist_20260519_1"
    assert payload["sections"]["market"]["signal"] == "YELLOW"
    assert payload["sections"]["pool"][0]["code"] == "002138"
    assert payload["sections"]["candidates"][0]["total_score"] == 5.8
    assert payload["sections"]["decision"][0]["action"] == "WATCH"
    assert payload["code_analysis"]["code"] == "002138"
    assert payload["code_analysis"]["decision_action"] == "WATCH"
    assert "观察" in payload["code_analysis"]["miss_reason"]


def test_archive_signal_history_writes_discovery_index(mysql_conn):
    conn = mysql_conn
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-05-20",
            history_group_id="hist_20260520_index",
            run_id="screener_index",
            phase="screener",
            market={},
            pool=[{"code": "600036"}, {"code": "300750"}],
            candidates=[{"code": "600036"}],
            decisions=[{"code": "600036", "action": "BUY"}],
        )
        rows = conn.execute(
            """SELECT code, source
               FROM signal_history_discoveries
               WHERE snapshot_date = ? AND history_group_id = ?
               ORDER BY code, source""",
            ("2026-05-20", "hist_20260520_index"),
        ).fetchall()
    finally:
        conn.close()

    assert [dict(row) for row in rows] == [
        {"code": "300750", "source": "pool"},
        {"code": "600036", "source": "candidates"},
        {"code": "600036", "source": "decision"},
        {"code": "600036", "source": "pool"},
    ]


def test_archive_signal_history_replaces_discovery_index_by_date_and_group():
    class FakeConn:
        def __init__(self):
            self.executed: list[tuple[str, tuple]] = []
            self.executemany_calls: list[tuple[str, list[tuple]]] = []

        def execute(self, sql, params=()):
            self.executed.append((sql, tuple(params or ())))

        def executemany(self, sql, params):
            self.executemany_calls.append((sql, list(params)))

    conn = FakeConn()

    archive_signal_history(
        conn,
        snapshot_date="2026-05-20",
        history_group_id="hist_20260520_index",
        run_id="screener_index",
        phase="historical_discovery",
        market={},
        pool=[{"code": "600036"}],
        candidates=[],
        decisions=[],
    )

    delete_calls = [item for item in conn.executed if "DELETE FROM signal_history_discoveries" in item[0]]
    assert delete_calls == [
        (
            "DELETE FROM signal_history_discoveries WHERE snapshot_date = ? AND history_group_id = ?",
            ("2026-05-20", "hist_20260520_index"),
        )
    ]
    assert conn.executemany_calls


def test_rebuild_signal_history_discovery_index_from_snapshots(mysql_conn):
    conn = mysql_conn
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-05-21",
            history_group_id="hist_20260521_rebuild",
            run_id="rebuild",
            phase="historical_discovery",
            market={},
            pool=[{"code": "002475"}],
            candidates=[],
            decisions=[],
        )
        conn.execute(
            "DELETE FROM signal_history_discoveries WHERE history_group_id = ?",
            ("hist_20260521_rebuild",),
        )

        dry_run = rebuild_signal_history_discovery_index(
            conn,
            start="2026-05-21",
            end="2026-05-21",
            write=False,
        )
        write_result = rebuild_signal_history_discovery_index(
            conn,
            start="2026-05-21",
            end="2026-05-21",
            write=True,
        )
        row = conn.execute(
            """SELECT code, source
               FROM signal_history_discoveries
               WHERE history_group_id = ?""",
            ("hist_20260521_rebuild",),
        ).fetchone()
    finally:
        conn.close()

    assert dry_run["status"] == "dry_run"
    assert dry_run["discovery_row_count"] == 1
    assert write_result["status"] == "ok"
    assert dict(row) == {"code": "002475", "source": "pool"}


def test_market_snapshot_quotes_signal_reserved_word():
    class FakeResult:
        def fetchall(self):
            return [
                {
                    "index_symbol": "000001",
                    "name": "上证指数",
                    "signal": "GREEN",
                    "price_cents": 310000,
                    "change_pct": 0.5,
                    "ma20_pct": 1.2,
                    "ma60_pct": 2.3,
                    "updated_at": "2026-05-19T15:00:00+08:00",
                },
            ]

    class FakeConn:
        def execute(self, sql, params=None):
            assert "`signal`" in sql
            return FakeResult()

    payload = _market_snapshot(FakeConn())

    assert payload["indices"][0]["signal"] == "GREEN"
    assert payload["signal"] == "GREEN"


def test_load_signal_history_bundles_reads_latest_group_for_many_dates(mysql_conn):
    conn = mysql_conn
    try:
        archive_signal_history(
            conn,
            snapshot_date="2026-01-05",
            history_group_id="hist_20260105_old",
            run_id="old",
            phase="screener",
            market={"signal": "RED"},
            pool=[{"code": "600000"}],
            candidates=[],
            decisions=[],
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-01-05",
            history_group_id="hist_20260105_new",
            run_id="new",
            phase="historical_discovery",
            market={"signal": "GREEN"},
            pool=[{"code": "600036"}],
            candidates=[],
            decisions=[],
        )
        archive_signal_history(
            conn,
            snapshot_date="2026-01-06",
            history_group_id="hist_20260106",
            run_id="next",
            phase="historical_discovery",
            market={"signal": "YELLOW"},
            pool=[{"code": "300750"}],
            candidates=[],
            decisions=[],
        )

        bundles = load_signal_history_bundles(
            conn,
            snapshot_dates=["2026-01-05", "2026-01-06", "2026-01-07"],
            phases=("screener", "historical_discovery"),
        )
    finally:
        conn.close()

    assert set(bundles) == {"2026-01-05", "2026-01-06"}
    assert bundles["2026-01-05"]["history_group_id"] == "hist_20260105_new"
    assert bundles["2026-01-05"]["sections"]["market"]["signal"] == "GREEN"
    assert bundles["2026-01-05"]["sections"]["pool"][0]["code"] == "600036"
    assert bundles["2026-01-06"]["history_group_id"] == "hist_20260106"
    assert bundles["2026-01-06"]["sections"]["pool"][0]["code"] == "300750"


def test_load_signal_history_bundles_uses_single_bulk_query_with_fake_conn():
    class FakeResult:
        def fetchall(self):
            return [
                {
                    "snapshot_date": "2026-01-05",
                    "history_group_id": "hist_old",
                    "run_id": "old",
                    "phase": "screener",
                    "snapshot_type": "market",
                    "payload_json": json.dumps({"signal": "RED"}),
                    "created_at": "2026-01-05T01:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-01-05",
                    "history_group_id": "hist_new",
                    "run_id": "new",
                    "phase": "historical_discovery",
                    "snapshot_type": "market",
                    "payload_json": json.dumps({"signal": "GREEN"}),
                    "created_at": "2026-01-05T02:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-01-05",
                    "history_group_id": "hist_new",
                    "run_id": "new",
                    "phase": "historical_discovery",
                    "snapshot_type": "pool",
                    "payload_json": json.dumps([{"code": "600036"}]),
                    "created_at": "2026-01-05T02:00:00+00:00",
                },
                {
                    "snapshot_date": "2026-01-06",
                    "history_group_id": "hist_next",
                    "run_id": "next",
                    "phase": "historical_discovery",
                    "snapshot_type": "pool",
                    "payload_json": json.dumps([{"code": "300750"}]),
                    "created_at": "2026-01-06T02:00:00+00:00",
                },
            ]

    class FakeConn:
        def __init__(self):
            self.calls = []

        def execute(self, sql, params=()):
            self.calls.append((sql, params))
            return FakeResult()

    conn = FakeConn()

    bundles = load_signal_history_bundles(
        conn,
        snapshot_dates=["2026-01-05", "2026-01-06"],
        phases=("screener", "historical_discovery"),
    )

    assert len(conn.calls) == 1
    assert "snapshot_date IN (?,?)" in conn.calls[0][0]
    assert "phase IN (?,?)" in conn.calls[0][0]
    assert conn.calls[0][1] == (
        "2026-01-05",
        "2026-01-06",
        "screener",
        "historical_discovery",
    )
    assert bundles["2026-01-05"]["history_group_id"] == "hist_new"
    assert bundles["2026-01-05"]["sections"]["market"]["signal"] == "GREEN"
    assert bundles["2026-01-05"]["sections"]["pool"][0]["code"] == "600036"
    assert bundles["2026-01-06"]["sections"]["pool"][0]["code"] == "300750"


def test_diagnose_signal_history_payload_query_avoids_created_at_sort():
    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class FakeConn:
        def execute(self, sql, params=None):
            if "GROUP BY history_group_id" in sql:
                return FakeResult(
                    [
                        {
                            "history_group_id": "hist_1",
                            "run_id": "screener_210412",
                            "phase": "screener",
                            "created_at": "2026-05-19T13:10:52+00:00",
                            "section_count": 4,
                        },
                    ]
                )

            assert "payload_json" in sql
            assert "ORDER BY snapshot_type" in sql
            assert "ORDER BY created_at, snapshot_type" not in sql
            return FakeResult(
                [
                    {
                        "snapshot_type": "market",
                        "payload_json": json.dumps({"signal": "GREEN"}),
                        "run_id": "screener_210412",
                        "phase": "screener",
                        "created_at": "2026-05-19T13:10:52+00:00",
                    },
                ]
            )

    payload = diagnose_signal_history(FakeConn(), snapshot_date="2026-05-19")

    assert payload["status"] == "ok"
    assert payload["sections"]["market"]["signal"] == "GREEN"
