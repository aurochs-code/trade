"""Signal alpha summaries for backtest and shadow-signal validation."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date
import random
from statistics import mean, stdev
from typing import Any, Iterable


def signal_alpha_summary(
    signals: Iterable[dict[str, Any]],
    *,
    horizons: tuple[str, ...] = ("5d", "10d", "20d"),
    bootstrap_iterations: int = 500,
    bootstrap_confidence: float = 0.95,
    bootstrap_seed: int = 20260613,
) -> dict[str, Any]:
    """Summarize forward-return quality for recorded strategy signals."""
    rows = [dict(item) for item in signals]
    scored_rows = _rows_with_score_bucket(rows)
    phased_rows = _rows_with_market_phase(scored_rows)
    return {
        "overall": _group_summary(
            rows,
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_route": _summaries_by_key(
            rows,
            "primary_strategy_route",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_market_signal": _summaries_by_key(
            rows,
            "market_signal",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_market_route": _nested_summaries_by_keys(
            rows,
            "market_signal",
            "primary_strategy_route",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_decision_reason": _multi_value_summaries_by_key(
            rows,
            "decision_reasons",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_veto_reason": _multi_value_summaries_by_key(
            rows,
            "veto_reasons",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_score_bucket": _summaries_by_key(
            scored_rows,
            "_score_bucket",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_route_score_bucket": _nested_summaries_by_keys(
            scored_rows,
            "primary_strategy_route",
            "_score_bucket",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_market_route_score_bucket": _market_route_score_bucket_summary(
            scored_rows,
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_market_phase_route_score_bucket": _market_phase_route_score_bucket_summary(
            phased_rows,
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_unknown_bucket": _summaries_by_key(
            _unknown_rows(rows),
            "unknown_bucket",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
        "by_market_unknown_bucket": _nested_summaries_by_keys(
            _unknown_rows(rows),
            "market_signal",
            "unknown_bucket",
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        ),
    }


def _multi_value_summaries_by_key(
    rows: list[dict[str, Any]],
    key: str,
    *,
    horizons: tuple[str, ...],
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        values = row.get(key) or []
        if isinstance(values, str):
            values = [values]
        if not isinstance(values, Iterable):
            continue
        for value in values:
            label = str(value or "").strip()
            if not label:
                continue
            groups[label].append(row)
    return {
        group: _group_summary(
            items,
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        )
        for group, items in sorted(groups.items())
    }


def compare_backtest_signal_reports(
    baseline: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    """Compare two backtest reports without claiming causality."""
    baseline_sample = _signal_sample_size(baseline)
    candidate_sample = _signal_sample_size(candidate)
    return {
        "total_return_delta_pct": _round_delta(candidate, baseline, "total_return_pct"),
        "win_rate_delta_pct": _round_delta(candidate, baseline, "win_rate_pct"),
        "max_drawdown_delta_pct": _round_delta(candidate, baseline, "max_drawdown_pct"),
        "buy_trade_delta": int(candidate.get("buy_trades", 0) or 0) - int(baseline.get("buy_trades", 0) or 0),
        "signal_sample_delta": candidate_sample - baseline_sample,
        "baseline": _compact_report_metrics(baseline),
        "candidate": _compact_report_metrics(candidate),
        "interpretation": _comparison_interpretation(baseline_sample, candidate_sample),
    }


def _summaries_by_key(
    rows: list[dict[str, Any]],
    key: str,
    *,
    horizons: tuple[str, ...],
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(key) or "unknown")].append(row)
    return {
        group: _group_summary(
            items,
            horizons=horizons,
            bootstrap_iterations=bootstrap_iterations,
            bootstrap_confidence=bootstrap_confidence,
            bootstrap_seed=bootstrap_seed,
        )
        for group, items in sorted(groups.items())
    }


def _unknown_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    route_gap_labels = {"unknown", "no_entry_route", "generic_entry_signal_watch"}
    return [
        row
        for row in rows
        if str(row.get("primary_strategy_route") or "unknown") in route_gap_labels
    ]


def _nested_summaries_by_keys(
    rows: list[dict[str, Any]],
    outer_key: str,
    inner_key: str,
    *,
    horizons: tuple[str, ...],
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    groups: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for row in rows:
        outer = str(row.get(outer_key) or "unknown")
        inner = str(row.get(inner_key) or "unknown")
        groups[outer][inner].append(row)
    return {
        outer: {
            inner: _group_summary(
                items,
                horizons=horizons,
                bootstrap_iterations=bootstrap_iterations,
                bootstrap_confidence=bootstrap_confidence,
                bootstrap_seed=bootstrap_seed,
            )
            for inner, items in sorted(inner_groups.items())
        }
        for outer, inner_groups in sorted(groups.items())
    }


def _market_route_score_bucket_summary(
    rows: list[dict[str, Any]],
    *,
    horizons: tuple[str, ...],
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    groups: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for row in rows:
        market = str(row.get("market_signal") or "unknown")
        route = str(row.get("primary_strategy_route") or "unknown")
        score_bucket = str(row.get("_score_bucket") or "unknown")
        groups[market][route][score_bucket].append(row)
    return {
        market: {
            route: {
                score_bucket: _group_summary(
                    items,
                    horizons=horizons,
                    bootstrap_iterations=bootstrap_iterations,
                    bootstrap_confidence=bootstrap_confidence,
                    bootstrap_seed=bootstrap_seed,
                )
                for score_bucket, items in sorted(score_groups.items())
            }
            for route, score_groups in sorted(route_groups.items())
        }
        for market, route_groups in sorted(groups.items())
    }


def _market_phase_route_score_bucket_summary(
    rows: list[dict[str, Any]],
    *,
    horizons: tuple[str, ...],
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    groups: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for row in rows:
        phase = str(row.get("_market_phase_bucket") or "unknown")
        route = str(row.get("primary_strategy_route") or "unknown")
        score_bucket = str(row.get("_score_bucket") or "unknown")
        groups[phase][route][score_bucket].append(row)
    return {
        phase: {
            route: {
                score_bucket: _group_summary(
                    items,
                    horizons=horizons,
                    bootstrap_iterations=bootstrap_iterations,
                    bootstrap_confidence=bootstrap_confidence,
                    bootstrap_seed=bootstrap_seed,
                )
                for score_bucket, items in sorted(score_groups.items())
            }
            for route, score_groups in sorted(route_groups.items())
        }
        for phase, route_groups in sorted(groups.items())
    }


def _rows_with_score_bucket(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored_rows: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["_score_bucket"] = _score_bucket(item.get("score"))
        scored_rows.append(item)
    return scored_rows


def _rows_with_market_phase(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    phased_rows: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["_market_phase_bucket"] = market_phase_bucket(item.get("market_context"))
        phased_rows.append(item)
    return phased_rows


def market_phase_bucket(context: Any) -> str:
    if not isinstance(context, dict):
        return "unknown"
    deviation = _float_or_none(context.get("index_ma20_deviation_pct"))
    slope = _float_or_none(context.get("index_ma20_slope_5d_pct"))
    if deviation is None or slope is None:
        return "unknown"

    if deviation < -5:
        distance = "deep_below_ma20"
    elif deviation < 0:
        distance = "below_ma20"
    elif deviation <= 3:
        distance = "near_ma20"
    else:
        distance = "extended_above_ma20"

    if slope >= 0.5:
        trend = "slope_up"
    elif slope <= -0.5:
        trend = "slope_down"
    else:
        trend = "slope_flat"
    return f"{distance}_{trend}"


def _score_bucket(raw_score: Any) -> str:
    try:
        score = float(raw_score)
    except (TypeError, ValueError):
        return "unknown"
    if score < 4.5:
        return "<4.5"
    if score < 5.0:
        return "4.5-5.0"
    if score < 5.5:
        return "5.0-5.5"
    if score < 6.0:
        return "5.5-6.0"
    if score < 6.5:
        return "6.0-6.5"
    return ">=6.5"


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _group_summary(
    rows: list[dict[str, Any]],
    *,
    horizons: tuple[str, ...],
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    return {
        "sample_size": len(rows),
        "concentration": _group_concentration(rows),
        "horizons": {
            horizon: _horizon_summary(
                rows,
                horizon,
                bootstrap_iterations=bootstrap_iterations,
                bootstrap_confidence=bootstrap_confidence,
                bootstrap_seed=bootstrap_seed,
            )
            for horizon in horizons
        },
    }


def _group_concentration(rows: list[dict[str, Any]]) -> dict[str, Any]:
    codes = [str(row.get("code") or "") for row in rows if row.get("code")]
    if not rows:
        return {
            "unique_codes": 0,
            "top_code": "",
            "top_code_sample_size": 0,
            "top_code_sample_pct": 0.0,
            "date_cluster_count": 0,
        }
    counts = Counter(codes)
    top_code = ""
    top_count = 0
    if counts:
        top_code, top_count = counts.most_common(1)[0]
    return {
        "unique_codes": len(counts),
        "top_code": top_code,
        "top_code_sample_size": top_count,
        "top_code_sample_pct": round(top_count / len(rows) * 100, 2) if rows else 0.0,
        "date_cluster_count": _date_cluster_count(rows),
    }


def _date_cluster_count(rows: list[dict[str, Any]], *, cluster_days: int = 5) -> int:
    dates_by_code: dict[str, list[date]] = defaultdict(list)
    undated = 0
    for row in rows:
        code = str(row.get("code") or "")
        raw_date = str(row.get("signal_date") or "")
        if not code or not raw_date:
            undated += 1
            continue
        try:
            dates_by_code[code].append(date.fromisoformat(raw_date))
        except ValueError:
            undated += 1
    clusters = undated
    for dates in dates_by_code.values():
        previous: date | None = None
        for item in sorted(dates):
            if previous is None or (item - previous).days > cluster_days:
                clusters += 1
            previous = item
    return clusters


def _horizon_summary(
    rows: list[dict[str, Any]],
    horizon: str,
    *,
    bootstrap_iterations: int,
    bootstrap_confidence: float,
    bootstrap_seed: int,
) -> dict[str, Any]:
    values = _horizon_values(rows, horizon)
    if not values:
        return {
            "sample_size": 0,
            "win_rate_pct": 0.0,
            "avg_return_pct": 0.0,
            "median_return_pct": 0.0,
            "p10_return_pct": 0.0,
            "p25_return_pct": 0.0,
            "p75_return_pct": 0.0,
            "p90_return_pct": 0.0,
            "min_return_pct": 0.0,
            "max_return_pct": 0.0,
            "std_return_pct": 0.0,
            "downside_rate_pct": 0.0,
            "avg_loss_pct": 0.0,
            "max_loss_pct": 0.0,
            "return_sharpe": 0.0,
            "profit_factor": 0.0,
            "avg_return_ci_low_pct": 0.0,
            "avg_return_ci_high_pct": 0.0,
            "bootstrap_iterations": 0,
        }
    wins = sum(1 for value in values if value > 0)
    losses = [value for value in values if value < 0]
    gains = [value for value in values if value > 0]
    std_value = stdev(values) if len(values) > 1 else 0.0
    ci_low, ci_high = _bootstrap_mean_ci(
        values,
        iterations=bootstrap_iterations,
        confidence=bootstrap_confidence,
        seed=bootstrap_seed,
    )
    return {
        "sample_size": len(values),
        "win_rate_pct": round(wins / len(values) * 100, 2),
        "avg_return_pct": round(mean(values) * 100, 2),
        "median_return_pct": round(_percentile(values, 0.50) * 100, 2),
        "p10_return_pct": round(_percentile(values, 0.10) * 100, 2),
        "p25_return_pct": round(_percentile(values, 0.25) * 100, 2),
        "p75_return_pct": round(_percentile(values, 0.75) * 100, 2),
        "p90_return_pct": round(_percentile(values, 0.90) * 100, 2),
        "min_return_pct": round(min(values) * 100, 2),
        "max_return_pct": round(max(values) * 100, 2),
        "std_return_pct": round(std_value * 100, 2),
        "downside_rate_pct": round(len(losses) / len(values) * 100, 2),
        "avg_loss_pct": round(mean(losses) * 100, 2) if losses else 0.0,
        "max_loss_pct": round(abs(min(losses)) * 100, 2) if losses else 0.0,
        "return_sharpe": round(mean(values) / std_value, 2) if std_value > 0 else 0.0,
        "profit_factor": _profit_factor(gains, losses),
        "avg_return_ci_low_pct": round(ci_low * 100, 2),
        "avg_return_ci_high_pct": round(ci_high * 100, 2),
        "bootstrap_iterations": max(int(bootstrap_iterations), 0) if len(values) > 1 else 0,
    }


def _horizon_values(rows: list[dict[str, Any]], horizon: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        raw = (row.get("forward_returns") or {}).get(horizon)
        if raw is None:
            continue
        try:
            values.append(float(raw))
        except (TypeError, ValueError):
            continue
    return values


def _percentile(values: list[float], pct: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = max(0.0, min(1.0, pct)) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def _bootstrap_mean_ci(
    values: list[float],
    *,
    iterations: int,
    confidence: float,
    seed: int,
) -> tuple[float, float]:
    if len(values) <= 1 or iterations <= 0:
        avg = mean(values) if values else 0.0
        return avg, avg
    rng = random.Random(seed)
    sample_size = len(values)
    boot_means = [
        mean(values[rng.randrange(sample_size)] for _ in range(sample_size))
        for _ in range(int(iterations))
    ]
    tail = max(0.0, min(1.0, (1.0 - confidence) / 2))
    return (
        _percentile(boot_means, tail),
        _percentile(boot_means, 1.0 - tail),
    )


def _profit_factor(gains: list[float], losses: list[float]) -> float:
    if not losses:
        return 0.0
    return round(sum(gains) / abs(sum(losses)), 2)


def _round_delta(candidate: dict[str, Any], baseline: dict[str, Any], key: str) -> float:
    return round(float(candidate.get(key, 0) or 0) - float(baseline.get(key, 0) or 0), 2)


def _signal_sample_size(report: dict[str, Any]) -> int:
    signal_alpha = report.get("signal_alpha") or {}
    overall = signal_alpha.get("overall") or {}
    return int(overall.get("sample_size") or report.get("buy_trades") or 0)


def _compact_report_metrics(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "total_return_pct": report.get("total_return_pct", 0),
        "max_drawdown_pct": report.get("max_drawdown_pct", 0),
        "sharpe_ratio": report.get("sharpe_ratio", 0),
        "calmar_ratio": report.get("calmar_ratio", 0),
        "win_rate_pct": report.get("win_rate_pct", 0),
        "buy_trades": report.get("buy_trades", 0),
        "signal_sample_size": _signal_sample_size(report),
    }


def _comparison_interpretation(baseline_sample: int, candidate_sample: int) -> str:
    if candidate_sample <= baseline_sample:
        return "候选配置没有扩大可验证信号样本；优先检查漏判是否仍存在。"
    return "候选配置增加了可验证信号样本；胜率和回撤需结合样本质量继续验证。"
