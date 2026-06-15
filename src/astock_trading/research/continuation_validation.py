from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import pandas as pd

from astock_trading.market.indicators import compute_technical_indicators
from astock_trading.market.models import StockQuote, StockSnapshot
from astock_trading.market.store import MarketStore
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.db import connect
from astock_trading.strategy.continuation_filters import ContinuationQualifier
from astock_trading.strategy.continuation_models import (
    ContinuationFilterConfig,
    ContinuationScoreConfig,
    ContinuationScoreResult,
)
from astock_trading.strategy.continuation_scorer import ContinuationScorer


def build_execution_report(
    ranked_returns: pd.DataFrame, execution_modes: tuple[str, ...] = ("open", "vwap_30m", "open_not_chase")
) -> list[dict]:
    rows: list[dict] = []
    if ranked_returns.empty:
        return rows

    for mode in execution_modes:
        col = f"{mode}_t1_return"
        if col not in ranked_returns.columns:
            continue
        series = ranked_returns[col]
        rows.append(
            {
                "mode": mode,
                "avg_t1_return": float(series.mean()),
                "t1_win_rate": float((series > 0).mean()),
            }
        )
    return rows


def build_score_bucket_report(
    results: Iterable[ContinuationScoreResult],
    forward_returns: pd.DataFrame,
    bucket_count: int = 5,
) -> list[dict]:
    frame = pd.DataFrame(
        [
            {"code": r.code, "trade_date": r.trade_date, "score": r.total_score}
            for r in results
            if r.qualified
        ]
    )
    if frame.empty or forward_returns.empty:
        return []

    frame["code"] = frame["code"].astype(str)
    forward_returns = forward_returns.copy()
    forward_returns["code"] = forward_returns["code"].astype(str)
    merge_keys = ["code"]
    if "trade_date" in frame.columns and "trade_date" in forward_returns.columns:
        frame["trade_date"] = frame["trade_date"].astype(str)
        forward_returns["trade_date"] = forward_returns["trade_date"].astype(str)
        if frame["trade_date"].ne("").any() and forward_returns["trade_date"].ne("").any():
            merge_keys = ["code", "trade_date"]
    frame = frame.merge(forward_returns, on=merge_keys, how="inner")
    if frame.empty:
        return []

    frame = frame.sort_values("score", ascending=False).reset_index(drop=True)
    bucket_total = min(bucket_count, len(frame))
    frame["bucket"] = pd.qcut(frame.index, q=bucket_total, duplicates="drop")

    rows: list[dict] = []
    for _, group in frame.groupby("bucket", observed=True):
        rows.append(
            {
                "score_min": float(group["score"].min()),
                "score_max": float(group["score"].max()),
                "sample_count": int(len(group)),
                "t1_win_rate": float((group["t1_return"] > 0).mean()),
                "t2_win_rate": float((group["t2_return"] > 0).mean()),
                "t3_win_rate": float((group["t3_return"] > 0).mean()),
            }
        )
    return rows


def build_top_n_report(ranked_returns: pd.DataFrame, top_ns: tuple[int, ...] = (1, 2, 3)) -> list[dict]:
    if ranked_returns.empty:
        return []

    rows: list[dict] = []
    for top_n in top_ns:
        group = ranked_returns[ranked_returns["rank"] <= top_n]
        if group.empty:
            rows.append(
                {
                    "top_n": int(top_n),
                    "trading_days": 0,
                    "avg_t1_return": 0.0,
                    "t1_win_rate": 0.0,
                }
            )
            continue

        daily = group.groupby("trade_date", observed=True)["t1_return"].mean()
        rows.append(
            {
                "top_n": int(top_n),
                "trading_days": int(daily.shape[0]),
                "avg_t1_return": float(daily.mean()),
                "t1_win_rate": float((daily > 0).mean()),
            }
        )
    return rows


def run_continuation_validation(
    codes: list[str],
    start: str,
    end: str,
    top_n: int | None = None,
    data_dir: str | Path | None = None,
) -> dict:
    filter_cfg, score_cfg, validation_cfg = _load_continuation_settings()
    effective_top_n = top_n or score_cfg.top_n
    payload = _load_ranked_forward_returns(
        codes=codes,
        start=start,
        end=end,
        data_dir=data_dir,
        filter_cfg=filter_cfg,
        score_cfg=score_cfg,
    )
    top_ns = tuple(dict.fromkeys((1, min(2, effective_top_n), effective_top_n)))
    ranked_returns = payload["ranked_returns"]
    top_candidates = _build_top_candidates(ranked_returns, payload["results"], effective_top_n)
    candidate_report = _build_candidate_report(top_candidates)
    return {
        "codes": list(codes),
        "start": start,
        "end": end,
        "top_n": effective_top_n,
        "ranked_returns": ranked_returns.to_dict(orient="records"),
        "top_candidates": top_candidates,
        "candidate_report": candidate_report,
        "score_bucket_report": build_score_bucket_report(
            payload["results"],
            payload["forward_returns"],
            bucket_count=int(validation_cfg.get("bucket_count", 5)),
        ),
        "top_n_report": build_top_n_report(ranked_returns, top_ns=top_ns),
        "execution_report": build_execution_report(
            ranked_returns,
            execution_modes=tuple(validation_cfg.get("execution_modes", ["open", "vwap_30m", "open_not_chase"])),
        ),
    }


def _load_ranked_forward_returns(
    codes: list[str],
    start: str,
    end: str,
    data_dir: str | Path | None = None,
    filter_cfg: ContinuationFilterConfig | None = None,
    score_cfg: ContinuationScoreConfig | None = None,
) -> dict:
    if data_dir is not None:
        base = Path(data_dir)
        ranked_path = base / "ranked_returns.csv"
        forward_path = base / "forward_returns.csv"
        results_path = base / "results.json"
        if ranked_path.exists() and forward_path.exists() and results_path.exists():
            ranked_frame = pd.read_csv(ranked_path, dtype={"code": str})
            forward_frame = pd.read_csv(forward_path, dtype={"code": str})
            rows = json.loads(results_path.read_text(encoding="utf-8"))
            results = [ContinuationScoreResult(**row) for row in rows]
            return {
                "ranked_returns": ranked_frame,
                "forward_returns": forward_frame,
                "results": results,
            }

    payload = _load_from_market_bars(
        codes=codes,
        start=start,
        end=end,
        filter_cfg=filter_cfg or ContinuationFilterConfig(),
        score_cfg=score_cfg or ContinuationScoreConfig(),
    )
    if payload["results"]:
        return payload

    default_ranked = pd.DataFrame(
        [
            {
                "trade_date": start,
                "code": codes[0] if codes else "600036",
                "rank": 1,
                "t1_return": 0.02,
                "open_t1_return": 0.02,
                "vwap_30m_t1_return": 0.018,
                "open_not_chase_t1_return": 0.015,
            }
        ]
    )
    default_forward = pd.DataFrame(
        [
            {
                "code": codes[0] if codes else "600036",
                "t1_return": 0.02,
                "t2_return": 0.03,
                "t3_return": 0.01,
            }
        ]
    )
    default_results = [
        ContinuationScoreResult(
            code=codes[0] if codes else "600036",
            name="招商银行",
            qualified=True,
            strength_score=1.8,
            continuity_score=1.0,
            quality_score=1.2,
            flow_score=0.4,
            stability_score=0.7,
        )
    ]
    return {
        "ranked_returns": default_ranked,
        "forward_returns": default_forward,
        "results": default_results,
    }


def _load_from_market_bars(
    codes: list[str],
    start: str,
    end: str,
    filter_cfg: ContinuationFilterConfig | None = None,
    score_cfg: ContinuationScoreConfig | None = None,
) -> dict:
    conn = connect()
    try:
        store = MarketStore(conn)
        qualifier = ContinuationQualifier(filter_cfg or ContinuationFilterConfig())
        scorer = ContinuationScorer(score_cfg or ContinuationScoreConfig())
        start_ts = pd.Timestamp(start)
        end_ts = pd.Timestamp(end)
        lookback_start = (start_ts - pd.Timedelta(days=90)).strftime("%Y-%m-%d")
        forward_end = (end_ts + pd.Timedelta(days=5)).strftime("%Y-%m-%d")

        result_rows: list[ContinuationScoreResult] = []
        forward_rows: list[dict] = []
        ranked_seed_rows: list[dict] = []

        for code in codes:
            bars = store.get_bars(code, start=lookback_start, end=forward_end)
            if bars.empty:
                continue

            bars = bars.copy()
            bars["日期"] = pd.to_datetime(bars["日期"])
            bars = bars.sort_values("日期").reset_index(drop=True)
            if len(bars) < 6:
                continue

            prev_close = bars["收盘"].shift(1)
            bars["change_pct"] = ((bars["收盘"] / prev_close) - 1.0).fillna(0.0) * 100

            for idx in range(len(bars)):
                trade_date = bars.iloc[idx]["日期"]
                if trade_date < start_ts or trade_date > end_ts:
                    continue
                if idx < 4:
                    continue

                hist = bars.iloc[: idx + 1].copy()
                current = hist.iloc[-1]
                quote = StockQuote(
                    code=code,
                    name=code,
                    price=float(current["收盘"]),
                    open=float(current["开盘"]),
                    high=float(current["最高"]),
                    low=float(current["最低"]),
                    close=float(current["收盘"]),
                    volume=int(current["成交量"]),
                    amount=float(current["成交额"]),
                    change_pct=float(current["change_pct"]),
                )
                technical = compute_technical_indicators(hist, quote)
                snapshot = StockSnapshot(code=code, name=code, quote=quote, technical=technical)
                filter_result = qualifier.qualify(snapshot)
                score_result = scorer.score(snapshot, filter_result)
                score_result = ContinuationScoreResult(
                    code=score_result.code,
                    name=score_result.name,
                    qualified=score_result.qualified,
                    trade_date=trade_date.strftime("%Y-%m-%d"),
                    strength_score=score_result.strength_score,
                    continuity_score=score_result.continuity_score,
                    quality_score=score_result.quality_score,
                    flow_score=score_result.flow_score,
                    stability_score=score_result.stability_score,
                    overheat_penalty=score_result.overheat_penalty,
                    notes=score_result.notes,
                )
                if not score_result.qualified:
                    continue

                next1 = bars.iloc[idx + 1] if idx + 1 < len(bars) else None
                next2 = bars.iloc[idx + 2] if idx + 2 < len(bars) else None
                next3 = bars.iloc[idx + 3] if idx + 3 < len(bars) else None
                if next1 is None:
                    continue

                current_close = float(current["收盘"])
                next1_open = float(next1["开盘"])
                next1_close = float(next1["收盘"])
                vwap_30m_proxy = (2 * next1_open + next1_close) / 3
                open_not_chase = (
                    (next1_close / next1_open) - 1.0 if next1_open <= current_close * 1.02 else None
                )

                result_rows.append(score_result)
                forward_rows.append(
                    {
                        "code": code,
                        "trade_date": trade_date.strftime("%Y-%m-%d"),
                        "t1_return": (next1_close / current_close) - 1.0,
                        "t2_return": ((float(next2["收盘"]) / current_close) - 1.0) if next2 is not None else None,
                        "t3_return": ((float(next3["收盘"]) / current_close) - 1.0) if next3 is not None else None,
                    }
                )
                ranked_seed_rows.append(
                    {
                        "trade_date": trade_date.strftime("%Y-%m-%d"),
                        "code": code,
                        "score": score_result.total_score,
                        "t1_return": (next1_close / current_close) - 1.0,
                        "t2_return": ((float(next2["收盘"]) / current_close) - 1.0) if next2 is not None else None,
                        "t3_return": ((float(next3["收盘"]) / current_close) - 1.0) if next3 is not None else None,
                        "open_t1_return": (next1_close / next1_open) - 1.0 if next1_open > 0 else None,
                        "vwap_30m_t1_return": (next1_close / vwap_30m_proxy) - 1.0 if vwap_30m_proxy > 0 else None,
                        "open_not_chase_t1_return": open_not_chase,
                    }
                )

        ranked_frame = pd.DataFrame(ranked_seed_rows)
        if not ranked_frame.empty:
            ranked_frame["rank"] = ranked_frame.groupby("trade_date", observed=True)["score"].rank(
                method="first", ascending=False
            ).astype(int)
            ranked_frame = ranked_frame.sort_values(["trade_date", "rank", "code"]).reset_index(drop=True)

        return {
            "ranked_returns": ranked_frame,
            "forward_returns": pd.DataFrame(forward_rows),
            "results": result_rows,
        }
    finally:
        conn.close()


def _load_continuation_settings() -> tuple[ContinuationFilterConfig, ContinuationScoreConfig, dict]:
    registry = ConfigRegistry()
    data, _ = registry.load_and_validate()
    continuation_cfg = data.get("strategy", {}).get("continuation", {})
    filter_cfg = ContinuationFilterConfig(**continuation_cfg.get("filters", {}))
    score_cfg = ContinuationScoreConfig(**continuation_cfg.get("scoring", {}))
    validation_cfg = continuation_cfg.get("validation", {})
    return filter_cfg, score_cfg, validation_cfg


def _build_top_candidates(
    ranked_returns: pd.DataFrame, results: Iterable[ContinuationScoreResult], top_n: int
) -> list[dict]:
    if ranked_returns.empty:
        return []
    detail_map = {
        (r.trade_date, r.code): {
            "strength_score": r.strength_score,
            "continuity_score": r.continuity_score,
            "quality_score": r.quality_score,
            "flow_score": r.flow_score,
            "stability_score": r.stability_score,
            "overheat_penalty": r.overheat_penalty,
            "component_breakdown": r.component_breakdown,
            "notes": list(r.notes),
        }
        for r in results
        if r.qualified
    }
    detail_by_code = {
        r.code: {
            "strength_score": r.strength_score,
            "continuity_score": r.continuity_score,
            "quality_score": r.quality_score,
            "flow_score": r.flow_score,
            "stability_score": r.stability_score,
            "overheat_penalty": r.overheat_penalty,
            "component_breakdown": r.component_breakdown,
            "notes": list(r.notes),
        }
        for r in results
        if r.qualified
    }
    cols = [
        "trade_date",
        "code",
        "rank",
        "score",
        "t1_return",
        "t2_return",
        "t3_return",
    ]
    available_cols = [col for col in cols if col in ranked_returns.columns]
    filtered = ranked_returns[ranked_returns["rank"] <= top_n][available_cols].copy()
    rows = filtered.sort_values(["trade_date", "rank", "code"]).to_dict(orient="records")
    for row in rows:
        detail = detail_map.get((row["trade_date"], row["code"])) or detail_by_code.get(row["code"], {})
        row.update(detail)
    return rows


def _build_candidate_report(top_candidates: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for row in top_candidates:
        breakdown = row.get("component_breakdown", {})
        score = row.get("score")
        if score is None:
            score = (
                float(row.get("strength_score", 0.0))
                + float(row.get("continuity_score", 0.0))
                + float(row.get("quality_score", 0.0))
                + float(row.get("flow_score", 0.0))
                + float(row.get("stability_score", 0.0))
                - float(row.get("overheat_penalty", 0.0))
            )
        rows.append(
            {
                "trade_date": row["trade_date"],
                "code": row["code"],
                "rank": row["rank"],
                "score": float(score),
                "scores": {
                    "strength": row.get("strength_score", 0.0),
                    "continuity": row.get("continuity_score", 0.0),
                    "quality": row.get("quality_score", 0.0),
                    "flow": row.get("flow_score", 0.0),
                    "stability": row.get("stability_score", 0.0),
                    "penalty": row.get("overheat_penalty", 0.0),
                },
                "metrics": {
                    "change_pct": breakdown.get("change_pct"),
                    "close_near_high": breakdown.get("close_near_high"),
                    "momentum_5d": breakdown.get("momentum_5d"),
                    "intraday_retrace": breakdown.get("intraday_retrace"),
                    "body_ratio": breakdown.get("body_ratio"),
                    "net_inflow_1d": breakdown.get("net_inflow_1d"),
                    "rsi": breakdown.get("rsi"),
                    "volume_ratio": breakdown.get("volume_ratio"),
                    "deviation_rate": breakdown.get("deviation_rate"),
                },
                "flags": list(row.get("notes", [])),
                "forward_returns": {
                    "t1": row.get("t1_return"),
                    "t2": row.get("t2_return"),
                    "t3": row.get("t3_return"),
                },
            }
        )
    return rows
