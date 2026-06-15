"""P5 参数校准建议。

只读取真实交易复盘和评分证据，输出建议并可选记录事件；不自动修改策略配置。
"""

from __future__ import annotations

import datetime as dt
from statistics import mean
from typing import Any

from astock_trading.platform.domain_events import (
    SCORE_CALCULATED,
    STRATEGY_CALIBRATION_PROPOSED,
    TRADE_HYPOTHESIS_RECORDED,
    TRADE_REVIEW_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import utc_now_iso

DIMENSIONS = ("technical", "fundamental", "flow", "sentiment")


def run_calibration(
    conn: Any,
    *,
    min_samples: int = 20,
    window_days: int = 365,
    record: bool = False,
    config_version: str = "unknown",
) -> dict:
    """生成 P5 参数/权重/选股条件校准建议。"""
    store = EventStore(conn)
    samples = _trade_review_samples(store, window_days=window_days)
    sample_count = len(samples)
    status = "ok" if sample_count >= min_samples else "insufficient_data"

    payload = {
        "analysis": "param_calibration",
        "status": status,
        "generated_at": utc_now_iso(),
        "config_version": config_version,
        "sample": {
            "closed_trade_reviews": sample_count,
            "min_required": min_samples,
            "window_days": window_days,
        },
        "parameter_calibration": _parameter_calibration(samples, min_samples=min_samples),
        "weight_optimization": _weight_optimization(samples, min_samples=min_samples),
        "selection_optimization": _selection_optimization(store, conn, min_samples=min_samples),
        "walk_forward_validation": _walk_forward_validation(samples, min_samples=min_samples),
        "evidence_gaps": _evidence_gaps(samples, min_samples=min_samples),
        "guardrails": {
            "auto_apply": False,
            "reason": "P5 只输出参数建议和证据留痕，不自动修改 strategy.yaml。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_calibration_report(payload)

    if record:
        event_id = store.append(
            "strategy:calibration",
            "strategy",
            STRATEGY_CALIBRATION_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "param_calibration", "config_version": config_version},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(conn, event_id, payload["report_markdown"])

    return payload


def render_calibration_report(payload: dict) -> str:
    """渲染给 Obsidian/报告层可读的中文校准报告。"""
    sample = payload.get("sample", {})
    lines = [
        "# P5 参数校准建议",
        "",
        f"- 状态：{payload.get('status')}",
        f"- 闭合交易复盘样本：{sample.get('closed_trade_reviews', 0)} / {sample.get('min_required', 0)}",
        "- 自动修改配置：否",
        "",
        "## 参数建议",
    ]
    suggestions = (payload.get("parameter_calibration") or {}).get("suggestions") or {}
    if suggestions:
        for key, item in suggestions.items():
            lines.append(f"- {key}: 建议 {item.get('proposed')}（{item.get('basis', '')}）")
    else:
        lines.append("- 样本不足，暂不输出可执行参数建议。")

    lines.extend(["", "## 权重建议"])
    weight = payload.get("weight_optimization") or {}
    for item in weight.get("dimension_correlations", []):
        lines.append(f"- {item['dimension']}: corr={item['correlation']:.3f}，方向={item['suggested_direction']}")
    if not weight.get("dimension_correlations"):
        lines.append("- 样本不足，暂不调整评分权重。")

    return "\n".join(lines)


def _trade_review_samples(store: EventStore, *, window_days: int) -> list[dict]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=max(window_days, 1))
    reviews = [
        event
        for event in store.query(event_type=TRADE_REVIEW_RECORDED, limit=5000)
        if _event_dt(event) >= cutoff
    ]
    hypotheses = {
        event["event_id"]: event
        for event in store.query(event_type=TRADE_HYPOTHESIS_RECORDED, limit=5000)
    }
    scores = {
        event["event_id"]: event
        for event in store.query(event_type=SCORE_CALCULATED, limit=5000)
    }
    samples = []
    for event in reviews:
        payload = event.get("payload") or {}
        hypothesis = hypotheses.get(str(payload.get("source_hypothesis_event_id") or ""))
        hypothesis_payload = (hypothesis or {}).get("payload") or {}
        source_score_event_id = (
            payload.get("source_score_event_id")
            or hypothesis_payload.get("source_score_event_id")
            or ""
        )
        score_payload = (scores.get(str(source_score_event_id)) or {}).get("payload") or {}
        sample = {
            "event_id": event["event_id"],
            "code": str(payload.get("code") or hypothesis_payload.get("code") or ""),
            "entry_date": str(payload.get("entry_date") or ""),
            "review_as_of": str(payload.get("review_as_of") or ""),
            "holding_days": _holding_days(payload),
            "mfe_pct": _float(payload.get("mfe_pct")),
            "mae_pct": _float(payload.get("mae_pct")),
            "latest_return_pct": _float(payload.get("latest_return_pct")),
            "source_score_event_id": str(source_score_event_id),
            "score": score_payload,
        }
        samples.append(sample)
    samples.sort(key=lambda item: (item["entry_date"], item["event_id"]))
    return samples


def _parameter_calibration(samples: list[dict], *, min_samples: int) -> dict:
    if len(samples) < min_samples:
        return {
            "status": "insufficient_data",
            "suggestions": {},
            "reason": f"至少需要 {min_samples} 笔闭合交易复盘后才给出参数建议。",
        }
    mfe = [item["mfe_pct"] for item in samples]
    mae = [item["mae_pct"] for item in samples]
    holding_days = [item["holding_days"] for item in samples if item["holding_days"] > 0]
    return {
        "status": "ok",
        "distribution": {
            "mfe_median": _quantile(mfe, 0.5),
            "mfe_p75": _quantile(mfe, 0.75),
            "mae_p25": _quantile(mae, 0.25),
            "mae_p10": _quantile(mae, 0.10),
            "holding_days_median": _quantile(holding_days, 0.5) if holding_days else 0,
        },
        "suggestions": {
            "t1_pct": {
                "proposed": round(_clamp(_quantile(mfe, 0.5), 0.03, 0.12), 4),
                "basis": "取 MFE 中位数，限制在 3%-12%。",
            },
            "t2_pct": {
                "proposed": round(_clamp(_quantile(mfe, 0.75), 0.05, 0.20), 4),
                "basis": "取 MFE 75 分位，限制在 5%-20%。",
            },
            "t1_drawdown": {
                "proposed": round(_clamp(_quantile(mfe, 0.5) * 0.45, 0.02, 0.08), 4),
                "basis": "按第一止盈目标约 45% 回撤容忍度估算。",
            },
            "t2_drawdown": {
                "proposed": round(_clamp(_quantile(mfe, 0.75) * 0.40, 0.03, 0.10), 4),
                "basis": "按第二止盈目标约 40% 回撤容忍度估算。",
            },
            "stop_loss": {
                "proposed": round(_clamp(abs(_quantile(mae, 0.25)), 0.04, 0.12), 4),
                "basis": "用 MAE 25 分位估算常规止损。",
            },
            "absolute_stop": {
                "proposed": round(_clamp(abs(_quantile(mae, 0.10)), 0.06, 0.18), 4),
                "basis": "用 MAE 10 分位估算极端止损。",
            },
            "time_stop_days": {
                "proposed": int(round(_clamp(_quantile(holding_days, 0.5) if holding_days else 15, 3, 30))),
                "basis": "取持仓天数中位数，限制在 3-30 天。",
            },
        },
    }


def _weight_optimization(samples: list[dict], *, min_samples: int) -> dict:
    if len(samples) < min_samples:
        return {
            "status": "insufficient_data",
            "dimension_correlations": [],
            "recommendations": ["样本不足，暂不调整评分权重。"],
        }
    returns = [item["latest_return_pct"] for item in samples]
    rows = []
    for dim in DIMENSIONS:
        values = [_float((item.get("score") or {}).get(f"{dim}_score")) for item in samples]
        corr = _correlation(values, returns)
        direction = "hold"
        if corr >= 0.2:
            direction = "increase"
        elif corr <= -0.2:
            direction = "decrease"
        rows.append({
            "dimension": dim,
            "correlation": corr,
            "suggested_direction": direction,
        })
    rows.sort(key=lambda item: abs(item["correlation"]), reverse=True)
    return {
        "status": "ok",
        "dimension_correlations": rows,
        "recommendations": _weight_recommendations(rows),
    }


def _selection_optimization(store: EventStore, conn: Any, *, min_samples: int) -> dict:
    score_events = store.query(event_type=SCORE_CALCULATED, limit=5000)
    veto_counter: dict[str, int] = {}
    for event in score_events:
        payload = event.get("payload") or {}
        for rule in payload.get("hard_veto_signals") or payload.get("veto_reasons") or []:
            veto_counter[str(rule)] = veto_counter.get(str(rule), 0) + 1

    candidate_returns = _candidate_forward_returns(store, conn, horizon_days=5)
    return {
        "status": "ok" if len(candidate_returns) >= min_samples else "needs_more_forward_samples",
        "candidate_performance": {
            "horizon_days": 5,
            "sample_count": len(candidate_returns),
            "avg_return_pct": round(mean(candidate_returns), 4) if candidate_returns else 0.0,
        },
        "veto_rules": [
            {"rule": rule, "blocked_count": count}
            for rule, count in sorted(veto_counter.items(), key=lambda item: (-item[1], item[0]))
        ],
        "recommendations": _selection_recommendations(candidate_returns, veto_counter, min_samples=min_samples),
    }


def _candidate_forward_returns(store: EventStore, conn: Any, *, horizon_days: int) -> list[float]:
    events = [
        *store.query(event_type="candidate.added", limit=5000),
        *store.query(event_type="candidate.promoted", limit=5000),
    ]
    returns: list[float] = []
    for event in events:
        payload = event.get("payload") or {}
        code = str(payload.get("code") or "")
        if not code:
            continue
        start_date = str(payload.get("added_at") or event.get("occurred_at") or "")[:10]
        rows = conn.execute(
            """SELECT bar_date, close_cents
               FROM market_bars
               WHERE symbol = ? AND period = 'daily' AND bar_date >= ?
               ORDER BY bar_date
               LIMIT ?""",
            (code, start_date, horizon_days + 1),
        ).fetchall()
        if len(rows) < 2:
            continue
        start = int(rows[0]["close_cents"])
        end = int(rows[-1]["close_cents"])
        if start > 0:
            returns.append(round((end / start) - 1.0, 4))
    return returns


def _walk_forward_validation(samples: list[dict], *, min_samples: int) -> dict:
    if len(samples) < min_samples:
        return {
            "status": "insufficient_data",
            "train_count": 0,
            "eval_count": 0,
            "independent_eval_window": False,
        }
    split = max(1, int(len(samples) * 0.7))
    train = samples[:split]
    eval_rows = samples[split:]
    return {
        "status": "ok" if eval_rows else "needs_eval_window",
        "train_count": len(train),
        "eval_count": len(eval_rows),
        "independent_eval_window": bool(eval_rows),
        "train_avg_return_pct": round(mean(item["latest_return_pct"] for item in train), 4),
        "eval_avg_return_pct": round(mean(item["latest_return_pct"] for item in eval_rows), 4) if eval_rows else 0.0,
    }


def _evidence_gaps(samples: list[dict], *, min_samples: int) -> list[str]:
    gaps = []
    if len(samples) < min_samples:
        gaps.append(f"至少需要 {min_samples} 笔闭合交易复盘，目前只有 {len(samples)} 笔。")
    missing_score = len([item for item in samples if not item.get("score")])
    if missing_score:
        gaps.append(f"{missing_score} 笔复盘缺少来源评分事件，无法纳入权重优化。")
    return gaps


def _write_report_artifact(conn: Any, event_id: str, markdown: str) -> None:
    conn.execute(
        """REPLACE INTO report_artifacts
           (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"calibration_{event_id}",
            event_id,
            "param_calibration",
            "markdown",
            markdown,
            "local",
            utc_now_iso(),
        ),
    )


def _event_dt(event: dict) -> dt.datetime:
    value = str(event.get("occurred_at") or "")
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _holding_days(payload: dict) -> int:
    if payload.get("review_after_days"):
        return max(int(payload.get("review_after_days") or 0), 0)
    entry = str(payload.get("entry_date") or "")
    review = str(payload.get("review_as_of") or "")
    if not entry or not review:
        return 0
    try:
        return (dt.date.fromisoformat(review[:10]) - dt.date.fromisoformat(entry[:10])).days
    except ValueError:
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _quantile(values: list[float] | list[int], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 4)


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(float(value), low), high)


def _correlation(values: list[float], returns: list[float]) -> float:
    if len(values) != len(returns) or len(values) < 2:
        return 0.0
    avg_x = mean(values)
    avg_y = mean(returns)
    numerator = sum((x - avg_x) * (y - avg_y) for x, y in zip(values, returns))
    denom_x = sum((x - avg_x) ** 2 for x in values) ** 0.5
    denom_y = sum((y - avg_y) ** 2 for y in returns) ** 0.5
    if denom_x == 0 or denom_y == 0:
        return 0.0
    return round(numerator / (denom_x * denom_y), 4)


def _weight_recommendations(rows: list[dict]) -> list[str]:
    changes = [row for row in rows if row["suggested_direction"] != "hold"]
    if not changes:
        return ["当前样本下四维评分预测力差异不明显，暂不调整权重。"]
    return [
        f"{row['dimension']} 相关性 {row['correlation']:.3f}，建议方向：{row['suggested_direction']}。"
        for row in changes
    ]


def _selection_recommendations(candidate_returns: list[float], veto_counter: dict[str, int], *, min_samples: int) -> list[str]:
    recommendations = []
    if len(candidate_returns) < min_samples:
        recommendations.append("入池后表现样本不足，先继续积累核心池 forward return。")
    elif mean(candidate_returns) < 0:
        recommendations.append("入池后 5 日平均收益为负，优先收紧 screening.mx_query 或提高观察池门槛。")
    else:
        recommendations.append("入池后 5 日平均收益为正，筛选召回可以保持，后续重点看 veto 误杀。")
    if veto_counter:
        top_rule = max(veto_counter.items(), key=lambda item: item[1])[0]
        recommendations.append(f"当前最高频 veto 是 {top_rule}，应优先复核它的误杀率和拦截亏损率。")
    return recommendations
