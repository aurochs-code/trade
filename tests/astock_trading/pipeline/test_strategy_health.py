"""P6-3 策略体检 / 深度归因测试。"""

from __future__ import annotations

from astock_trading.pipeline.strategy_health import run_strategy_health_review
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.events import EventStore


def _seed_review_sample(
    store: EventStore,
    *,
    code: str,
    return_pct: float,
    industry: str,
    market_cap_yuan: float,
    route: str,
    entry_date: str,
    review_as_of: str,
) -> None:
    score_event_id = store.append(
        f"strategy:{code}",
        "strategy",
        "score.calculated",
        {
            "code": code,
            "name": f"样本{code}",
            "total_score": 6.6,
            "industry_name": industry,
            "market_cap_yuan": market_cap_yuan,
            "entry_signal": True,
            "primary_strategy_route": route,
            "strategy_routes": [{"route": route, "family": "trend_swing", "entry_signal": True}],
        },
    )
    hypothesis_event_id = store.append(
        f"trade:{code}:order_{code}",
        "trade",
        "trade.hypothesis.recorded",
        {
            "order_id": f"order_{code}",
            "side": "buy",
            "code": code,
            "source_score_event_id": score_event_id,
            "hypothesis": {"review_after_days": 5, "entry_signal_type": route},
        },
    )
    store.append(
        f"trade:{code}:order_{code}",
        "trade",
        "trade.review.recorded",
        {
            "order_id": f"order_{code}",
            "code": code,
            "entry_date": entry_date,
            "review_as_of": review_as_of,
            "review_after_days": 5,
            "mfe_pct": max(return_pct + 0.03, 0),
            "mae_pct": min(return_pct - 0.02, 0),
            "latest_return_pct": return_pct,
            "source_hypothesis_event_id": hypothesis_event_id,
        },
    )


def test_strategy_health_review_groups_returns_and_records_event(tmp_path):
    db_path = tmp_path / "health.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        store = EventStore(conn)
        _seed_review_sample(
            store,
            code="600703",
            return_pct=0.06,
            industry="机器人",
            market_cap_yuan=30_000_000_000,
            route="pullback_confirm",
            entry_date="2026-05-18",
            review_as_of="2026-05-23",
        )
        _seed_review_sample(
            store,
            code="002138",
            return_pct=0.03,
            industry="机器人",
            market_cap_yuan=35_000_000_000,
            route="pullback_confirm",
            entry_date="2026-05-19",
            review_as_of="2026-05-24",
        )
        _seed_review_sample(
            store,
            code="000858",
            return_pct=-0.04,
            industry="白酒",
            market_cap_yuan=600_000_000_000,
            route="breakout",
            entry_date="2026-05-20",
            review_as_of="2026-05-25",
        )

        payload = run_strategy_health_review(conn, min_samples=3, record=True)
        events = store.query(event_type="strategy.health_report.proposed")
    finally:
        conn.close()

    industry_groups = {item["bucket"]: item for item in payload["group_attribution"]["by_industry"]}
    market_cap_groups = {item["bucket"]: item for item in payload["group_attribution"]["by_market_cap"]}
    route_groups = {item["bucket"]: item for item in payload["group_attribution"]["by_entry_signal_type"]}
    assert payload["analysis"] == "strategy_health_review"
    assert payload["status"] == "ok"
    assert industry_groups["机器人"]["sample_count"] == 2
    assert industry_groups["机器人"]["avg_return_pct"] == 0.045
    assert industry_groups["白酒"]["avg_return_pct"] == -0.04
    assert market_cap_groups["中市值"]["sample_count"] == 2
    assert route_groups["pullback_confirm"]["win_rate_pct"] == 1.0
    assert payload["competence_circle"]["strengths"][0]["bucket"] == "机器人"
    assert payload["competence_circle"]["weaknesses"][0]["bucket"] == "白酒"
    assert payload["time_analysis"]["by_entry_weekday"]
    assert payload["guardrails"]["auto_apply"] is False
    assert payload["recorded_event_id"]
    assert events[0]["payload"]["analysis"] == "strategy_health_review"


def test_strategy_health_review_reports_insufficient_samples(tmp_path):
    db_path = tmp_path / "empty.db"
    init_db(db_path)
    conn = connect(db_path)
    try:
        payload = run_strategy_health_review(conn, min_samples=3, record=False)
    finally:
        conn.close()

    assert payload["status"] == "insufficient_data"
    assert payload["sample"]["closed_trade_reviews"] == 0
    assert payload["competence_circle"]["strengths"] == []
    assert "至少需要 3 笔" in payload["evidence_gaps"][0]
