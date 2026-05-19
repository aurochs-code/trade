"""Screener governance behavior tests."""

from __future__ import annotations

from types import SimpleNamespace

from astock_trading.platform.cli.screener import (
    _add_watch_candidates,
    _apply_candidate_pool_refresh,
    _build_screener_explanation,
    _watch_threshold,
)
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore
from astock_trading.reporting.projectors import ProjectionUpdater


def _entry_route(route: str = "ma_golden_cross") -> dict:
    return {
        "route": route,
        "display_name": "均线金叉",
        "family": "trend_swing",
        "confidence": 0.82,
        "entry_signal": True,
    }


def test_watch_threshold_defaults_to_strategy_config():
    ctx = SimpleNamespace(
        cfg={
            "scoring": {"thresholds": {"buy": 5.5}},
            "pool_management": {"promote_min_score": 5.8},
        }
    )

    assert _watch_threshold(ctx, None) == 5.8
    assert _watch_threshold(ctx, 6.2) == 6.2


def test_build_screener_explanation_summarizes_blockers_and_near_misses():
    scores = [
        {
            "code": "001",
            "name": "临界股",
            "total_score": 5.7,
            "data_quality": "ok",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
        },
        {
            "code": "002",
            "name": "跌破均线",
            "total_score": 0.0,
            "data_quality": "degraded",
            "entry_signal": False,
            "veto_triggered": True,
            "hard_veto_signals": ["below_ma20"],
            "data_missing_fields": ["ROE"],
        },
        {
            "code": "003",
            "name": "评分过低",
            "total_score": 3.8,
            "data_quality": "ok",
            "entry_signal": True,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
        },
    ]
    decisions = [
        {
            "action": "CLEAR",
            "score": 5.7,
            "market_signal": "GREEN",
            "notes": ["评分过低"],
            "veto_reasons": [],
        },
        {
            "action": "CLEAR",
            "score": 0.0,
            "market_signal": "GREEN",
            "notes": ["一票否决"],
            "veto_reasons": ["below_ma20"],
        },
    ]

    payload = _build_screener_explanation(
        scores,
        decisions,
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-19T00:00:00+08:00",
        run_id="screener_test",
    )

    assert payload["diagnostic"] == "screener_explain"
    assert payload["score_buckets"]["near_buy"] == 1
    assert payload["score_buckets"]["below_reject"] == 2
    assert payload["blockers"]["hard_veto_reasons"][0] == {
        "reason": "below_ma20",
        "label": "跌破 MA20",
        "count": 1,
    }
    assert payload["blockers"]["decision_veto_reasons"][0] == {
        "reason": "below_ma20",
        "label": "跌破 MA20",
        "count": 1,
    }
    assert payload["blockers"]["data_quality"][1] == {
        "quality": "degraded",
        "label": "降级",
        "count": 1,
    }
    assert payload["near_misses"][0]["code"] == "001"
    assert "缺少入场信号" in payload["near_misses"][0]["blockers"]
    assert "临界候选" in payload["summary"]


def test_build_screener_explanation_returns_follow_up_candidate_layers():
    scores = [
        {
            "code": "001",
            "name": "观察候选",
            "total_score": 5.3,
            "data_quality": "ok",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
        },
        {
            "code": "002",
            "name": "临界观察",
            "total_score": 4.6,
            "data_quality": "ok",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
        },
        {
            "code": "003",
            "name": "高分被挡",
            "total_score": 6.2,
            "data_quality": "ok",
            "entry_signal": True,
            "veto_triggered": True,
            "hard_veto_signals": ["below_ma20"],
            "data_missing_fields": [],
        },
        {
            "code": "004",
            "name": "待补数据",
            "total_score": 4.8,
            "data_quality": "degraded",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": ["ROE", "现金流"],
        },
    ]

    payload = _build_screener_explanation(
        scores,
        [],
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-19T00:00:00+08:00",
    )

    assert payload["follow_up"]["watch_candidates"][0]["code"] == "001"
    assert payload["follow_up"]["near_watch_candidates"][0]["code"] == "004"
    assert payload["follow_up"]["near_watch_candidates"][1]["code"] == "002"
    assert payload["follow_up"]["blocked_high_scores"][0]["code"] == "003"
    assert payload["follow_up"]["data_repair_candidates"][0]["code"] == "004"
    assert payload["next_actions"][0] == {
        "type": "stock_analysis",
        "label": "复核观察候选",
        "command": "atrade stock analyze 001 --json",
    }


def test_add_watch_candidates_records_candidate_event(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        ctx = SimpleNamespace(
            conn=conn,
            event_store=store,
            projector=ProjectionUpdater(store, conn),
        )

        added = _add_watch_candidates(
            ctx,
            [
                {
                    "code": "002138",
                    "name": "双环传动",
                    "total_score": 5.9,
                    "veto_triggered": False,
                }
            ],
            threshold=5.5,
            run_id="screener_test",
        )

        assert added == [{"code": "002138", "name": "双环传动", "score": 5.9}]
        events = store.query(event_type="candidate.added")
        assert len(events) == 1
        assert events[0]["stream"] == "candidate:002138"
        assert events[0]["payload"]["pool_tier"] == "watch"
        assert events[0]["metadata"] == {"source": "cli.screener", "run_id": "screener_test"}
    finally:
        conn.close()


def test_refresh_replays_existing_candidates_into_governed_pool(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        projector = ProjectionUpdater(store, conn)
        ctx = SimpleNamespace(
            cfg={
                "scoring": {"thresholds": {"buy": 5.5, "watch": 5.0, "reject": 4.0}},
                "pool_management": {
                    "promote_min_score": 5.5,
                    "promote_streak_days": 1,
                    "watch_min_score": 5.0,
                    "remove_max_score": 4.0,
                },
            },
            conn=conn,
            event_store=store,
            projector=projector,
        )
        projector.sync_candidate_pool(
            [
                {
                    "code": "001",
                    "name": "A",
                    "pool_tier": "watch",
                    "score": 5.0,
                    "added_at": "2026-04-01",
                    "last_scored_at": "2026-04-01",
                },
                {
                    "code": "002",
                    "name": "B",
                    "pool_tier": "core",
                    "score": 6.0,
                    "added_at": "2026-04-01",
                    "last_scored_at": "2026-04-01",
                },
                {
                    "code": "003",
                    "name": "C",
                    "pool_tier": "watch",
                    "score": 4.5,
                    "added_at": "2026-04-01",
                    "last_scored_at": "2026-04-01",
                },
            ]
        )

        changes = _apply_candidate_pool_refresh(
            ctx,
            [
                {
                    "code": "001",
                    "name": "A",
                    "total_score": 6.0,
                    "veto_triggered": False,
                    "strategy_routes": [_entry_route()],
                },
                {"code": "002", "name": "B", "total_score": 4.7, "veto_triggered": False},
                {"code": "003", "name": "C", "total_score": 3.5, "veto_triggered": False},
            ],
            run_id="screener_refresh_test",
        )

        rows = conn.execute(
            """SELECT code, pool_tier, score, last_scored_at
               FROM projection_candidate_pool
               ORDER BY code"""
        ).fetchall()
        assert [(row["code"], row["pool_tier"], row["score"]) for row in rows] == [
            ("001", "core", 6.0),
        ]
        assert all(row["last_scored_at"] != "2026-04-01" for row in rows)
        assert changes["promoted"] == [
            {"code": "001", "name": "A", "score": 6.0, "from": "watch", "to": "core"}
        ]
        assert changes["rejected"] == [
            {"code": "002", "name": "B", "score": 4.7, "reason": "score<5.0"},
            {"code": "003", "name": "C", "score": 3.5, "reason": "score<5.0"},
        ]

        event_types = [event["event_type"] for event in store.query(limit=10)]
        assert "candidate.promoted" in event_types
        assert "candidate.rejected" in event_types
    finally:
        conn.close()


def test_refresh_requires_promote_streak_before_core_promotion(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        projector = ProjectionUpdater(store, conn)
        ctx = SimpleNamespace(
            cfg={
                "scoring": {"thresholds": {"buy": 5.5, "watch": 5.0, "reject": 4.0}},
                "pool_management": {
                    "promote_min_score": 5.5,
                    "promote_streak_days": 2,
                    "watch_min_score": 5.0,
                    "remove_max_score": 4.0,
                },
            },
            conn=conn,
            event_store=store,
            projector=projector,
        )
        projector.sync_candidate_pool(
            [
                {
                    "code": "001",
                    "name": "A",
                    "pool_tier": "watch",
                    "score": 5.2,
                    "streak_days": 0,
                },
                {
                    "code": "002",
                    "name": "B",
                    "pool_tier": "watch",
                    "score": 5.6,
                    "streak_days": 1,
                },
            ]
        )

        changes = _apply_candidate_pool_refresh(
            ctx,
            [
                {
                    "code": "001",
                    "name": "A",
                    "total_score": 5.8,
                    "veto_triggered": False,
                    "strategy_routes": [_entry_route()],
                },
                {
                    "code": "002",
                    "name": "B",
                    "total_score": 5.9,
                    "veto_triggered": False,
                    "strategy_routes": [_entry_route("volume_breakout")],
                },
            ],
            run_id="screener_refresh_test",
        )

        rows = conn.execute(
            """SELECT code, pool_tier, score, streak_days
               FROM projection_candidate_pool
               ORDER BY code"""
        ).fetchall()
        assert [(row["code"], row["pool_tier"], row["streak_days"]) for row in rows] == [
            ("001", "watch", 1),
            ("002", "core", 2),
        ]
        assert changes["promoted"] == [
            {"code": "002", "name": "B", "score": 5.9, "from": "watch", "to": "core"}
        ]
        assert changes["watched"] == [
            {"code": "001", "name": "A", "score": 5.8, "from": "watch", "to": "watch"}
        ]
    finally:
        conn.close()


def test_refresh_requires_entry_strategy_route_before_core_promotion(tmp_path):
    db_path = tmp_path / "test.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        projector = ProjectionUpdater(store, conn)
        ctx = SimpleNamespace(
            cfg={
                "scoring": {"thresholds": {"buy": 5.5, "watch": 5.0, "reject": 4.0}},
                "pool_management": {
                    "promote_min_score": 5.5,
                    "promote_streak_days": 1,
                    "watch_min_score": 5.0,
                    "remove_max_score": 4.0,
                },
            },
            conn=conn,
            event_store=store,
            projector=projector,
        )

        changes = _apply_candidate_pool_refresh(
            ctx,
            [
                {
                    "code": "001",
                    "name": "A",
                    "total_score": 6.2,
                    "veto_triggered": False,
                    "strategy_routes": [],
                },
                {
                    "code": "002",
                    "name": "B",
                    "total_score": 6.1,
                    "veto_triggered": False,
                    "strategy_routes": [
                        {
                            **_entry_route("dragon_head"),
                            "display_name": "龙头策略",
                            "family": "sector_momentum",
                            "entry_signal": False,
                        }
                    ],
                },
            ],
            run_id="screener_refresh_test",
        )

        rows = conn.execute(
            """SELECT code, pool_tier, score, note
               FROM projection_candidate_pool
               ORDER BY code"""
        ).fetchall()
        assert [(row["code"], row["pool_tier"], row["score"]) for row in rows] == [
            ("001", "watch", 6.2),
            ("002", "watch", 6.1),
        ]
        assert {row["note"] for row in rows} == {
            "screener_refresh:requires_entry_strategy_route"
        }
        assert changes["promoted"] == []
        assert changes["watched"] == [
            {
                "code": "001",
                "name": "A",
                "score": 6.2,
                "from": None,
                "to": "watch",
                "reason": "requires_entry_strategy_route",
            },
            {
                "code": "002",
                "name": "B",
                "score": 6.1,
                "from": None,
                "to": "watch",
                "reason": "requires_entry_strategy_route",
            },
        ]
    finally:
        conn.close()
