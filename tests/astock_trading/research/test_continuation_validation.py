import json

import pandas as pd
import pytest

from astock_trading.research.continuation_validation import (
    build_score_bucket_report,
    build_top_n_report,
    run_continuation_validation,
)
from astock_trading.market.store import MarketStore
from astock_trading.strategy.continuation_models import ContinuationScoreResult


def test_score_bucket_report_groups_candidates_by_score():
    rows = [
        ContinuationScoreResult(code="A", name="A", qualified=True, strength_score=2.0, continuity_score=1.0, quality_score=1.0),
        ContinuationScoreResult(code="B", name="B", qualified=True, strength_score=0.5, continuity_score=0.3, quality_score=0.2),
    ]
    forward_returns = pd.DataFrame(
        [
            {"code": "A", "t1_return": 0.03, "t2_return": 0.05, "t3_return": 0.04},
            {"code": "B", "t1_return": -0.01, "t2_return": 0.00, "t3_return": -0.02},
        ]
    )

    report = build_score_bucket_report(rows, forward_returns, bucket_count=2)

    assert len(report) == 2
    assert report[0]["sample_count"] == 1
    assert "t1_win_rate" in report[0]


def test_top_n_report_aggregates_daily_ranked_returns():
    ranked = pd.DataFrame(
        [
            {"trade_date": "2026-04-01", "code": "A", "rank": 1, "t1_return": 0.03},
            {"trade_date": "2026-04-01", "code": "B", "rank": 2, "t1_return": 0.01},
            {"trade_date": "2026-04-02", "code": "C", "rank": 1, "t1_return": 0.02},
        ]
    )

    report = build_top_n_report(ranked, top_ns=(1, 2))

    assert report[0]["top_n"] == 1
    assert report[0]["avg_t1_return"] > 0


def test_run_continuation_validation_returns_bucket_and_top_n_sections(tmp_path):
    ranked = pd.DataFrame(
        [
            {
                "trade_date": "2026-01-01",
                "code": "600036",
                "rank": 1,
                "t1_return": 0.02,
                "open_t1_return": 0.02,
                "vwap_30m_t1_return": 0.018,
                "open_not_chase_t1_return": 0.015,
            }
        ]
    )
    forward = pd.DataFrame(
        [{"code": "600036", "t1_return": 0.02, "t2_return": 0.03, "t3_return": 0.01}]
    )
    results = [
        {
            "code": "600036",
            "name": "招商银行",
            "qualified": True,
            "strength_score": 1.8,
            "continuity_score": 1.0,
            "quality_score": 1.2,
            "flow_score": 0.4,
            "stability_score": 0.7,
            "overheat_penalty": 0.0,
            "notes": [],
        }
    ]
    ranked.to_csv(tmp_path / "ranked_returns.csv", index=False)
    forward.to_csv(tmp_path / "forward_returns.csv", index=False)
    (tmp_path / "results.json").write_text(json.dumps(results), encoding="utf-8")

    result = run_continuation_validation(
        codes=["600036", "000001"],
        start="2026-01-01",
        end="2026-02-28",
        top_n=2,
        data_dir=tmp_path,
    )

    assert result["top_n"] == 2
    assert "score_bucket_report" in result
    assert "top_n_report" in result
    assert "execution_report" in result
    assert "top_candidates" in result
    assert "candidate_report" in result
    assert "strength_score" in result["top_candidates"][0]
    assert "component_breakdown" in result["top_candidates"][0]
    assert "scores" in result["candidate_report"][0]
    assert "metrics" in result["candidate_report"][0]
    assert result["candidate_report"][0]["score"] == pytest.approx(5.1)


def test_run_continuation_validation_uses_market_bars_when_available(mysql_conn):
    conn = mysql_conn
    try:
        store = MarketStore(conn)
        dates = [f"2026-01-0{i}" for i in range(1, 9)]
        strong = pd.DataFrame(
            {
                "日期": dates,
                "开盘": [10.0, 10.05, 10.12, 10.2, 10.4, 10.7, 10.95, 11.15],
                "最高": [10.08, 10.15, 10.25, 10.38, 10.72, 11.0, 11.25, 11.45],
                "最低": [9.96, 10.0, 10.08, 10.16, 10.35, 10.62, 10.9, 11.08],
                "收盘": [10.04, 10.12, 10.22, 10.34, 10.68, 10.95, 11.2, 11.38],
                "成交量": [80_000_000, 82_000_000, 85_000_000, 88_000_000, 150_000_000, 160_000_000, 170_000_000, 175_000_000],
                "成交额": [8.0e8, 8.3e8, 8.7e8, 9.1e8, 1.6e9, 1.75e9, 1.9e9, 2.0e9],
            }
        )
        weak = pd.DataFrame(
            {
                "日期": dates,
                "开盘": [10.0, 10.0, 10.01, 10.02, 10.02, 10.03, 10.03, 10.04],
                "最高": [10.02, 10.03, 10.04, 10.05, 10.06, 10.06, 10.07, 10.08],
                "最低": [9.98, 9.99, 10.0, 10.01, 10.01, 10.02, 10.02, 10.03],
                "收盘": [10.0, 10.01, 10.02, 10.03, 10.04, 10.05, 10.06, 10.07],
                "成交量": [40_000_000] * 8,
                "成交额": [4.0e8] * 8,
            }
        )
        store.save_bars("600036", strong, source="test")
        store.save_bars("000001", weak, source="test")
    finally:
        conn.close()

    result = run_continuation_validation(
        codes=["600036", "000001"],
        start="2026-01-05",
        end="2026-01-06",
        top_n=2,
    )

    assert result["score_bucket_report"]
    assert result["top_n_report"]
    assert result["ranked_returns"]
    assert result["top_candidates"]
    assert "component_breakdown" in result["top_candidates"][0]


def test_run_continuation_validation_uses_config_defaults_when_top_n_not_given(tmp_path):
    ranked = pd.DataFrame(
        [
            {
                "trade_date": "2026-01-01",
                "code": "600036",
                "rank": 1,
                "score": 5.2,
                "t1_return": 0.02,
                "open_t1_return": 0.02,
                "vwap_30m_t1_return": 0.018,
                "open_not_chase_t1_return": 0.015,
            }
        ]
    )
    forward = pd.DataFrame(
        [{"code": "600036", "trade_date": "2026-01-01", "t1_return": 0.02, "t2_return": 0.03, "t3_return": 0.01}]
    )
    results = [
        {
            "code": "600036",
            "name": "招商银行",
            "qualified": True,
            "trade_date": "2026-01-01",
            "strength_score": 1.8,
            "continuity_score": 1.0,
            "quality_score": 1.2,
            "flow_score": 0.4,
            "stability_score": 0.7,
            "overheat_penalty": 0.0,
            "notes": [],
        }
    ]
    ranked.to_csv(tmp_path / "ranked_returns.csv", index=False)
    forward.to_csv(tmp_path / "forward_returns.csv", index=False)
    (tmp_path / "results.json").write_text(json.dumps(results), encoding="utf-8")

    result = run_continuation_validation(
        codes=["600036"],
        start="2026-01-01",
        end="2026-02-28",
        data_dir=tmp_path,
    )

    assert result["top_n"] == 3
    assert result["top_candidates"][0]["code"] == "600036"
    assert result["candidate_report"][0]["code"] == "600036"
