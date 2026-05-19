"""P6-3 策略体检和深度归因。

只消费已记录的交易复盘、交易前假设和来源评分证据；不生成事后伪证据。
"""

from __future__ import annotations

import datetime as dt
from statistics import mean
from typing import Any

from astock_trading.platform.domain_events import (
    SCORE_CALCULATED,
    STRATEGY_HEALTH_REPORT_PROPOSED,
    TRADE_HYPOTHESIS_RECORDED,
    TRADE_REVIEW_RECORDED,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.time import utc_now_iso


def run_strategy_health_review(
    conn: Any,
    *,
    min_samples: int = 10,
    window_days: int = 365,
    record: bool = False,
) -> dict:
    """输出策略体检报告，按多个维度归因闭合交易复盘。"""
    store = EventStore(conn)
    samples = _review_samples(store, window_days=window_days)
    sample_count = len(samples)
    status = "ok" if sample_count >= min_samples else "insufficient_data"
    group_attribution = {
        "by_industry": _group_stats(samples, "industry"),
        "by_market_cap": _group_stats(samples, "market_cap_bucket"),
        "by_holding_days": _group_stats(samples, "holding_days_bucket"),
        "by_entry_signal_type": _group_stats(samples, "entry_signal_type"),
    }
    payload = {
        "analysis": "strategy_health_review",
        "status": status,
        "generated_at": utc_now_iso(),
        "sample": {
            "closed_trade_reviews": sample_count,
            "min_required": min_samples,
            "window_days": window_days,
        },
        "group_attribution": group_attribution,
        "competence_circle": _competence_circle(group_attribution),
        "time_analysis": {
            "by_entry_weekday": _group_stats(samples, "entry_weekday"),
            "by_entry_month": _group_stats(samples, "entry_month"),
        },
        "evidence_gaps": _evidence_gaps(samples, min_samples=min_samples),
        "guardrails": {
            "auto_apply": False,
            "manual_approval_required": True,
            "reason": "策略体检只输出归因和能力圈建议，不自动修改策略参数、profile 或仓位。",
        },
        "recorded_event_id": "",
    }
    payload["report_markdown"] = render_strategy_health_report(payload)

    if record:
        event_id = store.append(
            "strategy:health",
            "strategy",
            STRATEGY_HEALTH_REPORT_PROPOSED,
            payload={key: value for key, value in payload.items() if key != "recorded_event_id"},
            metadata={"source": "strategy_health"},
        )
        payload["recorded_event_id"] = event_id
        _write_report_artifact(conn, event_id, payload["report_markdown"])

    return payload


def render_strategy_health_report(payload: dict) -> str:
    """渲染中文策略体检报告。"""
    status_label = {"ok": "可参考", "insufficient_data": "证据不足"}.get(
        str(payload.get("status") or ""),
        str(payload.get("status") or ""),
    )
    sample = payload.get("sample") or {}
    lines = [
        "# P6-3 策略体检报告",
        "",
        f"- 状态：{status_label}",
        f"- 闭合复盘样本：{sample.get('closed_trade_reviews', 0)} / {sample.get('min_required', 0)}",
        "- 自动调整策略：否",
        "",
        "## 能力圈",
    ]
    circle = payload.get("competence_circle") or {}
    strengths = circle.get("strengths") or []
    weaknesses = circle.get("weaknesses") or []
    lines.append("- 强项：" + ("；".join(_group_sentence(item) for item in strengths) if strengths else "暂无"))
    lines.append("- 弱项：" + ("；".join(_group_sentence(item) for item in weaknesses) if weaknesses else "暂无"))
    lines.extend(["", "## 主要归因"])
    for title, key in [
        ("行业", "by_industry"),
        ("市值", "by_market_cap"),
        ("持仓天数", "by_holding_days"),
        ("入场信号", "by_entry_signal_type"),
    ]:
        lines.append(f"### {title}")
        groups = (payload.get("group_attribution") or {}).get(key) or []
        if not groups:
            lines.append("- 暂无样本")
        for item in groups[:5]:
            lines.append(f"- {_group_sentence(item)}")
    gaps = payload.get("evidence_gaps") or []
    if gaps:
        lines.extend(["", "## 证据缺口"])
        lines.extend(f"- {gap}" for gap in gaps)
    return "\n".join(lines)


def _review_samples(store: EventStore, *, window_days: int) -> list[dict]:
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
        holding_days = _holding_days(payload)
        entry_date = str(payload.get("entry_date") or "")[:10]
        samples.append({
            "event_id": event.get("event_id"),
            "code": str(payload.get("code") or hypothesis_payload.get("code") or ""),
            "latest_return_pct": _float(payload.get("latest_return_pct")),
            "mfe_pct": _float(payload.get("mfe_pct")),
            "mae_pct": _float(payload.get("mae_pct")),
            "holding_days": holding_days,
            "holding_days_bucket": _holding_days_bucket(holding_days),
            "industry": _industry(score_payload),
            "market_cap_bucket": _market_cap_bucket(score_payload),
            "entry_signal_type": _entry_signal_type(score_payload, hypothesis_payload),
            "entry_weekday": _entry_weekday(entry_date),
            "entry_month": entry_date[:7] if entry_date else "未知月份",
        })
    return samples


def _group_stats(samples: list[dict], key: str) -> list[dict]:
    buckets: dict[str, list[dict]] = {}
    for sample in samples:
        bucket = str(sample.get(key) or "未知")
        buckets.setdefault(bucket, []).append(sample)
    rows = []
    for bucket, items in buckets.items():
        returns = [item["latest_return_pct"] for item in items]
        mfe = [item["mfe_pct"] for item in items]
        mae = [item["mae_pct"] for item in items]
        rows.append({
            "bucket": bucket,
            "sample_count": len(items),
            "avg_return_pct": round(mean(returns), 4) if returns else 0.0,
            "win_rate_pct": round(sum(1 for value in returns if value > 0) / len(returns), 4) if returns else 0.0,
            "avg_mfe_pct": round(mean(mfe), 4) if mfe else 0.0,
            "avg_mae_pct": round(mean(mae), 4) if mae else 0.0,
        })
    rows.sort(key=lambda item: (item["sample_count"], item["avg_return_pct"]), reverse=True)
    return rows


def _competence_circle(group_attribution: dict) -> dict:
    all_groups = []
    for dimension, groups in group_attribution.items():
        for item in groups:
            all_groups.append({**item, "dimension": dimension})
    strengths = [
        item for item in all_groups
        if item["sample_count"] >= 2 and item["avg_return_pct"] > 0 and item["win_rate_pct"] >= 0.5
    ]
    weaknesses = [
        item for item in all_groups
        if item["sample_count"] >= 1 and (item["avg_return_pct"] < 0 or item["win_rate_pct"] < 0.4)
    ]
    strengths.sort(key=lambda item: (item["avg_return_pct"], item["sample_count"]), reverse=True)
    weaknesses.sort(key=lambda item: (item["avg_return_pct"], -item["sample_count"]))
    return {
        "strengths": strengths[:5],
        "weaknesses": weaknesses[:5],
    }


def _evidence_gaps(samples: list[dict], *, min_samples: int) -> list[str]:
    gaps = []
    if len(samples) < min_samples:
        gaps.append(f"至少需要 {min_samples} 笔闭合交易复盘，目前只有 {len(samples)} 笔。")
    missing_industry = sum(1 for item in samples if item["industry"] == "未知行业")
    missing_market_cap = sum(1 for item in samples if item["market_cap_bucket"] == "未知市值")
    missing_entry = sum(1 for item in samples if item["entry_signal_type"] == "unknown")
    if missing_industry:
        gaps.append(f"{missing_industry} 笔复盘缺少行业证据。")
    if missing_market_cap:
        gaps.append(f"{missing_market_cap} 笔复盘缺少市值证据。")
    if missing_entry:
        gaps.append(f"{missing_entry} 笔复盘缺少入场信号类型。")
    return gaps


def _industry(score_payload: dict) -> str:
    if score_payload.get("industry_name"):
        return str(score_payload["industry_name"])
    for route in score_payload.get("strategy_routes") or []:
        evidence = route.get("evidence") or {}
        if evidence.get("industry_name"):
            return str(evidence["industry_name"])
    return "未知行业"


def _market_cap_bucket(score_payload: dict) -> str:
    value = _float(
        score_payload.get("market_cap_yuan")
        or score_payload.get("market_cap")
        or score_payload.get("total_market_cap")
    )
    if value <= 0:
        return "未知市值"
    if value < 10_000_000_000:
        return "小市值"
    if value < 100_000_000_000:
        return "中市值"
    return "大市值"


def _entry_signal_type(score_payload: dict, hypothesis_payload: dict) -> str:
    if score_payload.get("primary_strategy_route"):
        return str(score_payload["primary_strategy_route"])
    routes = score_payload.get("strategy_routes") or []
    if routes:
        return str(routes[0].get("route") or "unknown")
    hypothesis = hypothesis_payload.get("hypothesis") or {}
    return str(hypothesis.get("entry_signal_type") or "unknown")


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


def _holding_days_bucket(days: int) -> str:
    if days <= 0:
        return "未知持仓"
    if days <= 3:
        return "1-3天"
    if days <= 10:
        return "4-10天"
    return "10天以上"


def _entry_weekday(entry_date: str) -> str:
    labels = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
    try:
        return labels[dt.date.fromisoformat(entry_date).weekday()]
    except ValueError:
        return "未知星期"


def _write_report_artifact(conn: Any, event_id: str, markdown: str) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO report_artifacts
           (artifact_id, run_id, report_type, format, content, delivered_to, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            f"strategy_health_{event_id}",
            event_id,
            "strategy_health_review",
            "markdown",
            markdown,
            "local",
            utc_now_iso(),
        ),
    )


def _group_sentence(item: dict) -> str:
    return (
        f"{item.get('bucket')}：样本 {item.get('sample_count')}，"
        f"均值 {item.get('avg_return_pct', 0):.2%}，胜率 {item.get('win_rate_pct', 0):.2%}"
    )


def _event_dt(event: dict) -> dt.datetime:
    value = str(event.get("occurred_at") or "")
    try:
        parsed = dt.datetime.fromisoformat(value)
    except ValueError:
        return dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
