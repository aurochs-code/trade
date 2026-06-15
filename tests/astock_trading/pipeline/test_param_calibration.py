"""P5 参数校准：只输出建议，不自动改策略配置。"""

from __future__ import annotations

from astock_trading.pipeline.param_calibration import run_calibration
from astock_trading.platform.events import EventStore


def _seed_review_sample(store: EventStore, conn, *, code: str, return_pct: float, mfe_pct: float, mae_pct: float) -> None:
    score_event_id = store.append(
        f"strategy:{code}",
        "strategy",
        "score.calculated",
        {
            "code": code,
            "name": f"样本{code}",
            "total_score": 6.8,
            "technical_score": 2.5,
            "fundamental_score": 1.2,
            "flow_score": 1.8,
            "sentiment_score": 1.3,
            "hard_veto_signals": [],
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
            "name": f"样本{code}",
            "source_score_event_id": score_event_id,
            "hypothesis": {"review_after_days": 5},
        },
    )
    store.append(
        f"trade:{code}:order_{code}",
        "trade",
        "trade.review.recorded",
        {
            "order_id": f"order_{code}",
            "code": code,
            "name": f"样本{code}",
            "entry_date": "2026-01-02",
            "review_as_of": "2026-01-07",
            "review_after_days": 5,
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "latest_return_pct": return_pct,
            "source_hypothesis_event_id": hypothesis_event_id,
        },
    )
    conn.execute(
        """INSERT INTO market_bars
           (symbol, bar_date, period, open_cents, high_cents, low_cents, close_cents, volume, amount_cents, source, fetched_at)
           VALUES (?, ?, 'daily', ?, ?, ?, ?, 1000, 100000, 'test', '2026-01-07T00:00:00+00:00')""",
        (code, "2026-01-02", 1000, 1010, 990, 1000),
    )
    conn.execute(
        """INSERT INTO market_bars
           (symbol, bar_date, period, open_cents, high_cents, low_cents, close_cents, volume, amount_cents, source, fetched_at)
           VALUES (?, ?, 'daily', ?, ?, ?, ?, 1000, 100000, 'test', '2026-01-07T00:00:00+00:00')""",
        (code, "2026-01-07", 1050, 1120, 1030, int(1000 * (1 + return_pct))),
    )


def test_run_calibration_outputs_p5_suggestions_and_records_event(mysql_conn):
    conn = mysql_conn
    try:
        store = EventStore(conn)
        _seed_review_sample(store, conn, code="600703", return_pct=0.06, mfe_pct=0.11, mae_pct=-0.03)
        _seed_review_sample(store, conn, code="002138", return_pct=-0.02, mfe_pct=0.04, mae_pct=-0.07)
        store.append(
            "candidate:600703",
            "candidate",
            "candidate.added",
            {"code": "600703", "name": "三安光电", "pool_tier": "core", "added_at": "2026-01-02"},
        )
        store.append(
            "strategy:300558",
            "strategy",
            "score.calculated",
            {"code": "300558", "total_score": 6.7, "hard_veto_signals": ["below_ma20"], "veto_triggered": True},
        )

        payload = run_calibration(conn, min_samples=2, record=True, config_version="v_test")
    finally:
        conn.close()

    assert payload["status"] == "ok"
    assert payload["sample"]["closed_trade_reviews"] == 2
    assert payload["parameter_calibration"]["suggestions"]["stop_loss"]["proposed"] > 0
    assert payload["parameter_calibration"]["suggestions"]["time_stop_days"]["proposed"] >= 5
    assert payload["weight_optimization"]["dimension_correlations"]
    assert payload["selection_optimization"]["candidate_performance"]["sample_count"] == 1
    assert payload["selection_optimization"]["veto_rules"][0]["rule"] == "below_ma20"
    assert payload["recorded_event_id"]


def test_run_calibration_reports_insufficient_data_without_guessing(mysql_conn):
    conn = mysql_conn
    try:
        payload = run_calibration(conn, min_samples=20, record=False, config_version="v_test")
    finally:
        conn.close()

    assert payload["status"] == "insufficient_data"
    assert payload["parameter_calibration"]["suggestions"] == {}
    assert payload["guardrails"]["auto_apply"] is False
    assert "至少需要 20 笔闭合交易复盘" in payload["evidence_gaps"][0]
