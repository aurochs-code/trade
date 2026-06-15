"""推荐诊断入口。"""

from astock_trading.platform.events import EventStore
from astock_trading.platform.recommendation_diagnostics import diagnose_recommendations


def test_diagnose_recommendations_separates_formal_buy_from_watch_layers(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)
    try:
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "688498",
                "watch",
                "源杰科技",
                5.8,
                "2026-06-13T09:30:00+08:00",
                "2026-06-13T14:30:00+08:00",
                1,
                "trend_cooling_off",
            ),
        )
        store.append(
            stream="strategy:688498",
            stream_type="strategy",
            event_type="decision.suggested",
            payload={
                "code": "688498",
                "name": "源杰科技",
                "action": "TRIAL_BUY",
                "score": 5.8,
                "entry_signal": False,
                "market_signal": "GREEN",
                "primary_strategy_route": "trend_cooling_off",
                "primary_strategy_route_label": "趋势冷却观察",
                "strategy_routes": [
                    {
                        "route": "trend_cooling_off",
                        "display_name": "趋势冷却观察",
                        "status": "watch",
                        "entry_signal": False,
                        "route_score": 0.86,
                    }
                ],
            },
            metadata={"run_id": "test_recommendation"},
        )

        payload = diagnose_recommendations(conn)
    finally:
        conn.close()

    assert payload["diagnostic"] == "recommendations"
    assert payload["actionability"]["formal_buy_ready"] is False
    assert payload["actionability"]["trial_tracking_available"] is True
    assert payload["tiers"]["trial_buy_watch"][0]["code"] == "688498"
    assert payload["tiers"]["trial_buy_watch"][0]["route_label"] == "趋势冷却观察"
    assert payload["root_causes"][0]["type"] == "core_pool_empty"
    assert payload["next_actions"][0]["risk_level"] == "read_only"


def test_positive_review_uses_current_candidate_pool_state_not_stale_payload(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)
    try:
        conn.execute(
            """INSERT INTO projection_candidate_pool
               (code, pool_tier, name, score, added_at, last_scored_at, streak_days, note)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "002384",
                "watch",
                "东山精密",
                6.2,
                "2026-06-13T09:30:00+08:00",
                "2026-06-13T14:30:00+08:00",
                1,
                "screener_refresh:requires_entry_strategy_route",
            ),
        )
        store.append(
            stream="paper:002384",
            stream_type="paper",
            event_type="paper.trial.reviewed",
            payload={
                "code": "002384",
                "name": "东山精密",
                "review_status": "positive",
                "return_pct": 6.56,
                "current_pool_tier": "core",
                "current_entry_signal": True,
            },
            metadata={"run_id": "test_review"},
        )

        payload = diagnose_recommendations(conn)
    finally:
        conn.close()

    assert payload["candidate_pool"]["core_count"] == 0
    item = payload["tiers"]["positive_review_watch"][0]
    assert item["code"] == "002384"
    assert item["active_in_current_pool"] is True
    assert item["current_pool_tier"] == "watch"
    assert item["current_entry_signal"] is None
    assert item["stale_pool_evidence"] is True
    assert item["stale_payload"]["current_pool_tier"] == "core"


def test_diagnose_recommendations_reports_yield_target_evidence_state(mysql_conn):
    conn = mysql_conn
    store = EventStore(conn)
    try:
        for code, status, return_pct in [
            ("002384", "positive", 6.0),
            ("600584", "negative", -2.0),
        ]:
            store.append(
                stream=f"paper:{code}",
                stream_type="paper",
                event_type="paper.trial.reviewed",
                payload={
                    "code": code,
                    "name": code,
                    "review_status": status,
                    "return_pct": return_pct,
                },
                metadata={"run_id": "test_yield"},
            )

        payload = diagnose_recommendations(conn)
    finally:
        conn.close()

    target = payload["yield_target"]
    assert target["target_band"]["realistic_annual_return_pct"] == "20-25%"
    assert target["sample"]["reviewed_count"] == 2
    assert target["sample"]["avg_return_pct"] == 2.0
    assert target["sample"]["win_rate_pct"] == 0.5
    assert target["status"] == "insufficient_sample"
    assert "不能证明现实目标已经达成" in target["summary"]
