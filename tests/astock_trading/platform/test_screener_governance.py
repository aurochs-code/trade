"""Screener governance behavior tests."""

from __future__ import annotations

from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

from astock_trading.platform.cli.screener import (
    _add_watch_candidates,
    _apply_candidate_pool_refresh,
    _build_scoring_candidates,
    _candidate_rows,
    _build_screener_iteration_plan,
    _build_screener_explanation,
    _hot_recall_candidates,
    _recent_signal_recall_candidates,
    _record_screener_iteration,
    _run_screener,
    _scan_limit,
    _watch_threshold,
)
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


def test_watch_threshold_defaults_to_observation_line_not_promotion_line():
    ctx = SimpleNamespace(
        cfg={
            "scoring": {"thresholds": {"buy": 6.0, "watch": 5.0}},
            "pool_management": {"promote_min_score": 6.0, "watch_min_score": 5.0},
        }
    )

    assert _watch_threshold(ctx, None) == 5.0
    assert _watch_threshold(ctx, 6.2) == 6.2


def test_refresh_scan_limit_uses_operational_budget_without_narrowing_run_scan():
    cfg = {"market_scan_limit": 300, "refresh_scan_limit": 80}

    assert _scan_limit(cfg, None, refresh_pool=False) == 300
    assert _scan_limit(cfg, None, refresh_pool=True) == 80
    assert _scan_limit(cfg, 120, refresh_pool=True) == 120
    assert _scan_limit({"market_scan_limit": 300}, None, refresh_pool=True) == 10


def test_candidate_rows_include_latest_entry_signal_and_strategy_route(mysql_conn):
    conn = mysql_conn
    try:
        event_store = EventStore(conn)
        ProjectionUpdater(event_store, conn).sync_candidate_pool([
            {
                "code": "688981",
                "name": "中芯国际",
                "pool_tier": "watch",
                "score": 6.2,
                "note": "screener_refresh",
            }
        ])
        event_store.append(
            stream="strategy:688981",
            stream_type="strategy",
            event_type="score.calculated",
            payload={
                "code": "688981",
                "total_score": 6.4,
                "entry_signal": True,
                "primary_strategy_route": "ma_golden_cross",
                "strategy_routes": [_entry_route()],
                "technical_detail": "金叉:1.0/1 量比:0.5/0.5(1.3)",
                "data_quality": "ok",
            },
        )

        rows = _candidate_rows(conn)

        assert rows[0]["code"] == "688981"
        assert rows[0]["entry_signal"] is True
        assert rows[0]["primary_strategy_route"] == "ma_golden_cross"
        assert rows[0]["primary_strategy_route_label"] == "均线金叉"
        assert rows[0]["strategy_routes"][0]["entry_signal"] is True
        assert rows[0]["technical_detail"].startswith("金叉")
    finally:
        conn.close()


def test_refresh_applies_score_limit_after_hot_and_existing_recall(mysql_conn, monkeypatch):
    from astock_trading.market.store import MarketStore
    from astock_trading.platform import cli as cli_package

    conn = mysql_conn
    store = MarketStore(conn)
    store.save_observation(
        "astock_signal",
        "hot_stocks",
        "latest",
        {
            "items": [
                {"code": "300001", "name": "热股一"},
                {"code": "300002", "name": "热股二"},
                {"code": "300003", "name": "热股三"},
            ]
        },
    )
    ProjectionUpdater(EventStore(conn), conn).sync_candidate_pool(
        [
            {"code": "600001", "name": "存量一", "pool_tier": "watch", "score": 5.1},
            {"code": "600002", "name": "存量二", "pool_tier": "radar", "score": 4.8},
            {"code": "600003", "name": "存量三", "pool_tier": "core", "score": 6.2},
        ]
    )
    captured: dict[str, list[dict]] = {}

    def fake_score_stock_batch(_ctx, stock_list, _run_id):
        captured["stock_list"] = stock_list
        raise RuntimeError("stop-before-live-scoring")

    monkeypatch.setattr(
        cli_package.screener,
        "_search_screener_results",
        lambda query, timeout_seconds: [
            {"code": f"00000{index}", "name": f"主召回{index}"}
            for index in range(1, 7)
        ],
    )
    monkeypatch.setattr(cli_package.screener, "_score_stock_batch", fake_score_stock_batch)
    monkeypatch.setattr(
        cli_package.screener,
        "build_context",
        lambda: SimpleNamespace(
            cfg={
                "screening": {
                    "mx_query": "测试查询",
                    "market_scan_limit": 300,
                    "refresh_scan_limit": 80,
                    "include_hot_recall": True,
                    "hot_recall_limit": 10,
                },
            },
            conn=conn,
        ),
    )

    with pytest.raises(RuntimeError, match="stop-before-live-scoring"):
        _run_screener("", 5, None, True, refresh_pool=True)

    assert len(captured["stock_list"]) <= 5
    assert [item["code"] for item in captured["stock_list"]] == [
        "600003",
        "600002",
        "600001",
        "300001",
        "000001",
    ]


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


def test_build_screener_explanation_uses_latest_score_per_code_for_candidate_counts():
    scores = [
        {
            "code": "688981",
            "name": "中芯国际",
            "total_score": 5.1,
            "data_quality": "degraded",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": ["ROE"],
        },
        {
            "code": "688981",
            "name": "中芯国际",
            "total_score": 5.6,
            "data_quality": "ok",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
        },
    ]

    payload = _build_screener_explanation(
        scores,
        [],
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-22T00:00:00+08:00",
    )

    assert payload["scope"]["score_events"] == 2
    assert payload["scope"]["unique_scores"] == 1
    assert payload["score_buckets"]["near_buy"] == 1
    assert payload["follow_up_counts"]["watch_candidates"] == 1
    assert payload["follow_up_counts"]["data_repair_candidates"] == 0
    assert payload["near_misses"] == [
        {
            "code": "688981",
            "name": "中芯国际",
            "score": 5.6,
            "data_quality": "ok",
            "entry_signal": False,
            "blockers": ["缺少入场信号", "分数低于买入线 6.0"],
        }
    ]


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
        follow_up_limit=1,
    )

    assert payload["follow_up"]["watch_candidates"][0]["code"] == "001"
    assert len(payload["follow_up"]["near_watch_candidates"]) == 1
    assert payload["follow_up"]["near_watch_candidates"][0]["code"] == "004"
    assert payload["follow_up"]["blocked_high_scores"][0]["code"] == "003"
    assert payload["follow_up"]["data_repair_candidates"][0]["code"] == "004"
    assert payload["follow_up_counts"] == {
        "watch_candidates": 1,
        "near_watch_candidates": 2,
        "blocked_high_scores": 1,
        "data_repair_candidates": 1,
    }
    assert payload["next_actions"][0] == {
        "type": "stock_analysis",
        "label": "复核观察候选",
        "command": "atrade stock analyze 001 --json",
    }


def test_build_screener_explanation_uses_current_candidate_pool_for_follow_up():
    scores = [
        {
            "code": "600584",
            "name": "长电科技",
            "total_score": 5.5,
            "data_quality": "ok",
            "entry_signal": False,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
        }
    ]
    current_candidates = [
        {
            "code": "600584",
            "name": "长电科技",
            "pool_tier": "watch",
            "score": 5.8,
            "entry_signal": False,
            "data_quality": "ok",
        },
        {
            "code": "603376",
            "name": "大明电子",
            "pool_tier": "radar",
            "score": 4.8,
            "entry_signal": False,
            "data_quality": "ok",
        },
    ]

    payload = _build_screener_explanation(
        scores,
        [],
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-22T00:00:00+08:00",
        current_candidates=current_candidates,
    )

    assert payload["scope"]["current_candidate_pool"] == 2
    assert payload["current_candidate_pool"]["counts"] == {"core": 0, "watch": 1, "radar": 1}
    assert payload["follow_up"]["watch_candidates"][0] | {
        "code": "600584",
        "score": 5.8,
        "score_source": "current_candidate_pool",
        "pool_tier": "watch",
        "pool_tier_label": "观察",
    } == payload["follow_up"]["watch_candidates"][0]
    assert payload["follow_up"]["near_watch_candidates"][0] | {
        "code": "603376",
        "score": 4.8,
        "score_source": "current_candidate_pool",
        "pool_tier": "radar",
        "pool_tier_label": "强势观察",
    } == payload["follow_up"]["near_watch_candidates"][0]


def test_build_screener_explanation_marks_historical_entry_signal_for_refresh_recall():
    scores = [
        {
            "code": "300611",
            "name": "美力科技",
            "total_score": 4.8,
            "data_quality": "ok",
            "entry_signal": True,
            "veto_triggered": False,
            "hard_veto_signals": [],
            "data_missing_fields": [],
            "score_source": "score_event",
            "score_event_id": "score-300611-old",
            "scored_at": "2026-05-20T07:12:58+00:00",
        }
    ]

    payload = _build_screener_explanation(
        scores,
        [],
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-19T00:00:00+08:00",
    )

    candidate = payload["follow_up"]["near_watch_candidates"][0]
    assert candidate["code"] == "300611"
    assert candidate["score_source"] == "score_event"
    assert candidate["score_event_id"] == "score-300611-old"
    assert candidate["scored_at"] == "2026-05-20T07:12:58+00:00"
    assert candidate["recall_hint"] == {
        "type": "recent_entry_signal_recall",
        "label": "历史入场信号需重新评分入池",
        "command": "atrade screener refresh --json",
        "safe_to_auto_apply": True,
        "reason": "该票来自历史评分事件，不在当前候选池；先通过刷新召回重新评分，不直接当成当前可模拟候选。",
    }
    assert payload["next_actions"][0] == {
        "type": "refresh_recent_signal_recall",
        "label": "刷新历史入场信号候选",
        "command": "atrade screener refresh --json",
        "safe_to_auto_apply": True,
    }
    assert payload["next_actions"][1] == {
        "type": "near_watch_review",
        "label": "复核临界观察候选",
        "command": "atrade stock analyze 300611 --json",
    }


def test_build_screener_iteration_plan_refreshes_historical_entry_signal_before_review():
    explanation = _build_screener_explanation(
        [
            {
                "code": "300611",
                "name": "美力科技",
                "total_score": 4.8,
                "data_quality": "ok",
                "entry_signal": True,
                "veto_triggered": False,
                "hard_veto_signals": [],
                "data_missing_fields": [],
                "score_source": "score_event",
                "score_event_id": "score-300611-old",
                "scored_at": "2026-05-20T07:12:58+00:00",
            }
        ],
        [],
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-19T00:00:00+08:00",
    )

    payload = _build_screener_iteration_plan(explanation, record=False)

    assert payload["iteration_plan"][0] == {
        "type": "recent_signal_recall_refresh",
        "label": "刷新历史入场信号候选",
        "command": "atrade screener refresh --json",
        "rationale": "历史评分曾出现入场信号，但不在当前候选池；先重新评分入池，再判断是否进入观察或核心。",
        "safe_to_auto_apply": True,
    }
    assert payload["iteration_plan"][1]["type"] == "near_watch_review"
    assert payload["iteration_plan"][1]["rationale"] == (
        "该票来自历史评分事件，刷新后如果仍接近观察线再单票复核。"
    )


def test_build_screener_explanation_summarizes_core_entry_candidate_from_current_pool():
    current_candidates = [
        {
            "code": "002384",
            "name": "东山精密",
            "pool_tier": "core",
            "score": 7.0,
            "entry_signal": True,
            "data_quality": "ok",
        },
        {
            "code": "600584",
            "name": "长电科技",
            "pool_tier": "watch",
            "score": 5.9,
            "entry_signal": False,
            "data_quality": "ok",
        },
    ]

    payload = _build_screener_explanation(
        [],
        [],
        thresholds={"buy": 6.0, "watch": 5.0, "reject": 4.0},
        since="2026-05-22T00:00:00+08:00",
        current_candidates=current_candidates,
    )

    assert payload["status"] == "ok"
    assert "核心候选" in payload["summary"]
    assert "入场信号" in payload["summary"]
    assert "不适合直接当作买入意向" not in payload["summary"]
    assert payload["top_scores"][0] | {
        "code": "002384",
        "score": 7.0,
        "entry_signal": True,
        "pool_tier": "core",
        "pool_tier_label": "核心",
    } == payload["top_scores"][0]
    assert payload["next_actions"][0] == {
        "type": "paper_auto_readiness",
        "label": "复核模拟盘承接",
        "command": "atrade paper auto-readiness --json",
    }


def test_build_screener_iteration_plan_keeps_guardrails_and_next_command():
    explanation = {
        "summary": "近期候选整体评分不足，当前不应通过降低买入线来制造交易。",
        "scope": {"score_events": 170, "decision_events": 170},
        "score_buckets": {
            "buy_ready_raw": 0,
            "near_buy": 0,
            "watch_band": 0,
            "reject_band": 1,
            "below_reject": 169,
        },
        "follow_up_counts": {
            "watch_candidates": 0,
            "near_watch_candidates": 1,
            "blocked_high_scores": 0,
            "data_repair_candidates": 136,
        },
        "follow_up": {
            "watch_candidates": [],
            "near_watch_candidates": [{"code": "301338", "name": "凯格精机", "score": 4.3}],
            "blocked_high_scores": [],
            "data_repair_candidates": [{"code": "002387", "name": "维信诺", "score": 2.0}],
        },
        "next_actions": [
            {
                "type": "near_watch_review",
                "label": "复核临界观察候选",
                "command": "atrade stock analyze 301338 --json",
            }
        ],
    }

    payload = _build_screener_iteration_plan(explanation, record=True)

    assert payload["diagnostic"] == "screener_iteration"
    assert payload["status"] == "needs_action"
    assert payload["closed_loop"]["next_command"] == "atrade stock analyze 301338 --json"
    assert payload["iteration_plan"][0]["type"] == "near_watch_review"
    assert payload["iteration_plan"][1]["type"] == "data_repair"
    assert payload["guardrails"]["blocked_auto_adjustments"][0]["type"] == "lower_buy_threshold"
    assert payload["guardrails"]["manual_confirmation_required"] is True


def test_record_screener_iteration_appends_strategy_event(mysql_conn):
    conn = mysql_conn
    try:
        store = EventStore(conn)
        ctx = SimpleNamespace(event_store=store)
        payload = {
            "diagnostic": "screener_iteration",
            "status": "needs_action",
            "iteration_plan": [{"type": "refresh_scores"}],
        }

        event_id = _record_screener_iteration(ctx, payload, run_id="iter_test")

        events = store.query(event_type="strategy.iteration.proposed")
        assert events[0]["event_id"] == event_id
        assert events[0]["stream"] == "strategy:iteration"
        assert events[0]["payload"] == payload
        assert events[0]["metadata"] == {"source": "cli.screener.iterate", "run_id": "iter_test"}
    finally:
        conn.close()


def test_screener_iterate_help_via_bin_trade():
    root = Path(__file__).resolve().parents[3]
    cli = root / "bin" / "trade"

    result = subprocess.run(
        [str(cli), "screener", "iterate", "--help"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "生成选股自我迭代计划" in result.stdout
    assert "--no-record" in result.stdout
    assert "--json" in result.stdout


def test_add_watch_candidates_records_candidate_event(mysql_conn):
    conn = mysql_conn
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


def test_refresh_replays_existing_candidates_into_governed_pool(mysql_conn):
    conn = mysql_conn
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
            ("002", "radar", 4.7),
        ]
        assert all(row["last_scored_at"] != "2026-04-01" for row in rows)
        assert changes["promoted"] == [
            {"code": "001", "name": "A", "score": 6.0, "from": "watch", "to": "core"}
        ]
        assert changes["radar"] == [
            {
                "code": "002",
                "name": "B",
                "score": 4.7,
                "from": "core",
                "to": "radar",
                "reason": "below_watch_retained",
            }
        ]
        assert changes["rejected"] == [
            {"code": "003", "name": "C", "score": 3.5, "reason": "score<4.5"},
        ]

        event_types = [event["event_type"] for event in store.query(limit=10)]
        assert "candidate.promoted" in event_types
        assert "pool.demoted" in event_types
        assert "candidate.rejected" in event_types
    finally:
        conn.close()


def test_refresh_adds_near_watch_score_to_radar_pool(mysql_conn):
    conn = mysql_conn
    try:
        store = EventStore(conn)
        ctx = SimpleNamespace(
            cfg={
                "scoring": {"thresholds": {"buy": 6.0, "watch": 5.0, "reject": 4.0}},
                "pool_management": {
                    "promote_min_score": 6.0,
                    "promote_streak_days": 2,
                    "watch_min_score": 5.0,
                    "radar_min_score": 4.5,
                    "remove_max_score": 4.0,
                },
            },
            conn=conn,
            event_store=store,
            projector=ProjectionUpdater(store, conn),
        )

        changes = _apply_candidate_pool_refresh(
            ctx,
            [
                {
                    "code": "603376",
                    "name": "大明电子",
                    "total_score": 4.8,
                    "veto_triggered": False,
                    "entry_signal": False,
                    "strategy_routes": [],
                }
            ],
            run_id="screener_refresh_test",
        )

        rows = conn.execute(
            """SELECT code, pool_tier, score, note
               FROM projection_candidate_pool
               ORDER BY code"""
        ).fetchall()
        assert [(row["code"], row["pool_tier"], row["score"]) for row in rows] == [
            ("603376", "radar", 4.8),
        ]
        assert rows[0]["note"] == "screener_refresh:below_watch_retained"
        assert changes["radar"] == [
            {
                "code": "603376",
                "name": "大明电子",
                "score": 4.8,
                "from": None,
                "to": "radar",
                "reason": "below_watch_retained",
            }
        ]
        events = store.query(event_type="candidate.added")
        assert events[0]["payload"]["pool_tier"] == "radar"
        assert events[0]["payload"]["note"] == "screener_refresh:below_watch_retained"
    finally:
        conn.close()


def test_refresh_scoring_candidates_prioritizes_existing_pool_when_limited():
    raw_candidates = [
        {"code": "600100", "name": "粗筛一"},
        {"code": "600101", "name": "粗筛二"},
        {"code": "600102", "name": "粗筛三"},
    ]
    hot_candidates = [
        {"code": "300100", "name": "热榜一"},
        {"code": "300101", "name": "热榜二"},
        {"code": "300102", "name": "热榜三"},
    ]
    existing_candidates = [
        {"code": "002384", "name": "东山精密"},
        {"code": "688981", "name": "中芯国际"},
        {"code": "600584", "name": "长电科技"},
    ]

    result = _build_scoring_candidates(
        raw_candidates,
        hot_candidates,
        [],
        existing_candidates,
        score_limit=5,
        refresh_pool=True,
    )

    assert [item["code"] for item in result["stock_list"]] == [
        "002384",
        "688981",
        "600584",
        "300100",
        "600100",
    ]
    assert result["source_counts"] == {"existing_pool": 3, "hot_stocks": 1, "mx": 1}


def test_refresh_scoring_candidates_can_recall_recent_entry_signal():
    raw_candidates = [
        {"code": "600100", "name": "粗筛一"},
        {"code": "600101", "name": "粗筛二"},
    ]
    hot_candidates = [{"code": "300100", "name": "热榜一"}]
    recent_signal_candidates = [{"code": "300611", "name": "美力科技"}]
    existing_candidates = [
        {"code": "002384", "name": "东山精密"},
        {"code": "688981", "name": "中芯国际"},
        {"code": "600584", "name": "长电科技"},
    ]

    result = _build_scoring_candidates(
        raw_candidates,
        hot_candidates,
        recent_signal_candidates,
        existing_candidates,
        score_limit=5,
        refresh_pool=True,
    )

    assert [item["code"] for item in result["stock_list"]] == [
        "002384",
        "688981",
        "600584",
        "300611",
        "300100",
    ]
    assert result["source_counts"] == {
        "existing_pool": 3,
        "recent_signals": 1,
        "hot_stocks": 1,
    }


def test_hot_recall_candidates_reads_recent_hot_stock_observations(mysql_conn):
    from astock_trading.market.store import MarketStore

    conn = mysql_conn
    try:
        store = MarketStore(conn)
        store.save_observation(
            "astock_signal",
            "hot_stocks",
            "latest",
            {
                "items": [
                    {"code": "603376", "name": "大明电子", "reason": "机器人+汽车电子"},
                    {"code": "SZ002245", "name": "蔚蓝锂芯", "reason": "锂电+机器人"},
                    {"code": "002496", "name": "*ST辉丰", "reason": "ST摘帽预期"},
                    {"code": "NVDA", "name": "英伟达", "reason": "海外芯片"},
                ]
            },
        )
        store.save_observation(
            "OpenCliFinanceAdapter",
            "cross_platform_hot_stocks",
            "cn_a",
            {
                "stocks": [
                    {"代码": "600032", "名称": "浙江新能"},
                    {"code": "603376", "name": "大明电子"},
                    {"code": "00700", "name": "腾讯控股"},
                ]
            },
        )

        candidates = _hot_recall_candidates(conn, limit=10)

        assert candidates == [
            {"code": "600032", "name": "浙江新能", "recall_source": "cross_platform_hot_stocks"},
            {"code": "603376", "name": "大明电子", "recall_source": "cross_platform_hot_stocks"},
            {"code": "002245", "name": "蔚蓝锂芯", "recall_source": "hot_stocks"},
        ]
    finally:
        conn.close()


def test_recent_signal_recall_candidates_reads_entry_and_near_watch_scores(mysql_conn):
    conn = mysql_conn
    try:
        store = EventStore(conn)
        store.append(
            "strategy:300611",
            "strategy",
            "score.calculated",
            {
                "code": "300611",
                "name": "美力科技",
                "total_score": 4.8,
                "entry_signal": True,
                "veto_triggered": False,
                "data_quality": "ok",
            },
        )
        store.append(
            "strategy:600584",
            "strategy",
            "score.calculated",
            {
                "code": "600584",
                "name": "长电科技",
                "total_score": 5.9,
                "entry_signal": False,
                "veto_triggered": False,
                "data_quality": "ok",
            },
        )
        store.append(
            "strategy:688001",
            "strategy",
            "score.calculated",
            {
                "code": "688001",
                "name": "硬否决",
                "total_score": 6.1,
                "entry_signal": True,
                "veto_triggered": True,
                "data_quality": "ok",
            },
        )
        store.append(
            "strategy:000001",
            "strategy",
            "score.calculated",
            {
                "code": "000001",
                "name": "分数不足",
                "total_score": 4.0,
                "entry_signal": False,
                "veto_triggered": False,
                "data_quality": "ok",
            },
        )

        candidates = _recent_signal_recall_candidates(
            conn,
            min_score=4.5,
            watch_score=5.0,
            limit=10,
        )

        assert candidates == [
            {
                "code": "600584",
                "name": "长电科技",
                "recall_source": "recent_signal_score",
                "score": 5.9,
                "entry_signal": False,
            },
            {
                "code": "300611",
                "name": "美力科技",
                "recall_source": "recent_entry_signal",
                "score": 4.8,
                "entry_signal": True,
            },
        ]
    finally:
        conn.close()


def test_refresh_requires_promote_streak_before_core_promotion(mysql_conn):
    conn = mysql_conn
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


def test_refresh_requires_entry_strategy_route_before_core_promotion(mysql_conn):
    conn = mysql_conn
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


def test_refresh_can_promote_entry_route_with_shorter_entry_streak(mysql_conn):
    conn = mysql_conn
    try:
        store = EventStore(conn)
        projector = ProjectionUpdater(store, conn)
        ctx = SimpleNamespace(
            cfg={
                "scoring": {"thresholds": {"buy": 6.0, "watch": 5.0, "reject": 4.0}},
                "pool_management": {
                    "promote_min_score": 6.0,
                    "promote_streak_days": 2,
                    "entry_signal_promote_streak_days": 1,
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
                    "code": "002384",
                    "name": "东山精密",
                    "pool_tier": "watch",
                    "score": 5.5,
                    "streak_days": 0,
                },
            ]
        )

        changes = _apply_candidate_pool_refresh(
            ctx,
            [
                {
                    "code": "002384",
                    "name": "东山精密",
                    "total_score": 7.0,
                    "veto_triggered": False,
                    "strategy_routes": [_entry_route("flow_confirmed_trend")],
                },
            ],
            run_id="screener_refresh_test",
        )

        row = conn.execute(
            "SELECT code, pool_tier, score, streak_days FROM projection_candidate_pool WHERE code = ?",
            ("002384",),
        ).fetchone()
        assert dict(row) == {
            "code": "002384",
            "pool_tier": "core",
            "score": 7.0,
            "streak_days": 1,
        }
        assert changes["promoted"] == [
            {"code": "002384", "name": "东山精密", "score": 7.0, "from": "watch", "to": "core"}
        ]
    finally:
        conn.close()
