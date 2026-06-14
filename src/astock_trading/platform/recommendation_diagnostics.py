"""Read-only recommendation diagnostics.

The report separates human-review recommendations from formal buy readiness.
"""

from __future__ import annotations

from typing import Any

from astock_trading.platform.agent_diagnostics import candidate_pool_summary
from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores
from astock_trading.platform.events import EventStore


def diagnose_recommendations(conn: Any, *, limit: int = 20) -> dict[str, Any]:
    """Explain whether current evidence is actionable, observable, or blocked."""
    pool = candidate_pool_summary(conn)
    decisions = _recent_decisions(conn, limit=limit)
    candidates = _candidate_rows(conn, limit=limit)

    formal = [_decision_item(item) for item in decisions if item.get("action") == "BUY"]
    trial = [_decision_item(item) for item in decisions if item.get("action") == "TRIAL_BUY"]
    strong_watch = _strong_watch_items(candidates)
    positive_reviews = _positive_review_items(conn, limit=limit)

    root_causes = _root_causes(pool=pool, decisions=decisions, candidates=candidates)
    actionability = {
        "formal_buy_ready": bool(formal),
        "trial_tracking_available": bool(trial),
        "watch_review_available": bool(strong_watch or positive_reviews),
    }
    status = "ready" if formal else ("watch" if trial or strong_watch or positive_reviews else "blocked")
    return {
        "diagnostic": "recommendations",
        "status": status,
        "summary": _summary(pool, actionability),
        "root_causes": root_causes,
        "actionability": actionability,
        "tiers": {
            "formal_buy_ready": formal,
            "trial_buy_watch": trial,
            "strong_watch": strong_watch,
            "positive_review_watch": positive_reviews,
        },
        "candidate_pool": pool,
        "yield_target": _yield_target_assessment(conn, limit=limit),
        "next_actions": _next_actions(actionability),
        "guardrails": {
            "read_only": True,
            "places_paper_order": False,
            "promotes_candidate": False,
            "manual_confirmation_required_for_real_trade": True,
        },
    }


def _recent_decisions(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    events = EventStore(conn).query(event_type="decision.suggested", limit=max(limit, 1))
    return [
        {
            **(event.get("payload") or {}),
            "event_id": event.get("event_id"),
            "occurred_at": event.get("occurred_at"),
        }
        for event in reversed(events)
    ]


def _candidate_rows(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
           FROM projection_candidate_pool
           ORDER BY CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
                    score DESC,
                    last_scored_at DESC,
                    code
           LIMIT ?""",
        (max(limit, 1),),
    ).fetchall()
    return [dict(row) for row in rows]


def _decision_item(payload: dict[str, Any]) -> dict[str, Any]:
    code = str(payload.get("code") or "")
    return {
        "code": code,
        "name": payload.get("name") or code,
        "action": payload.get("action"),
        "score": payload.get("score", payload.get("confidence", 0)),
        "entry_signal": bool(payload.get("entry_signal")),
        "market_signal": payload.get("market_signal"),
        "route": payload.get("primary_strategy_route"),
        "route_label": payload.get("primary_strategy_route_label") or _route_label(payload),
        "review_command": f"atrade stock analyze {code} --json" if code else "",
        "event_id": payload.get("event_id"),
        "occurred_at": payload.get("occurred_at"),
    }


def _route_label(payload: dict[str, Any]) -> str:
    route = payload.get("primary_strategy_route")
    for item in payload.get("strategy_routes") or []:
        if item.get("route") == route:
            return str(item.get("display_name") or route or "")
    return str(route or "")


def _strong_watch_items(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for row in candidates:
        code = str(row.get("code") or "")
        tier = str(row.get("pool_tier") or "")
        score = float(row.get("score") or 0.0)
        if tier not in {"watch", "radar", "core"} or score < 5.0:
            continue
        result.append({
            "code": code,
            "name": row.get("name") or code,
            "pool_tier": tier,
            "score": score,
            "note": row.get("note") or "",
            "review_command": f"atrade stock analyze {code} --json" if code else "",
        })
    return result


def _positive_review_items(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    events = EventStore(conn).query(event_type="paper.trial.reviewed", limit=max(limit, 1))
    positive_events = []
    reviewed_codes = []
    for event in reversed(events):
        payload = event.get("payload") or {}
        if str(payload.get("review_status") or payload.get("status") or "") != "positive":
            continue
        positive_events.append(event)
        code = str(payload.get("code") or "")
        if code:
            reviewed_codes.append(code)

    current_by_code = _current_candidates_by_code(conn, reviewed_codes)
    items = []
    for event in positive_events:
        payload = event.get("payload") or {}
        code = str(payload.get("code") or "")
        current = current_by_code.get(code)
        current_tier = current.get("pool_tier") if current else None
        current_entry_signal = current.get("entry_signal") if current else None
        stale_payload = _stale_positive_review_payload(payload, current)
        item = {
            "code": code,
            "name": (current or {}).get("name") or payload.get("name") or code,
            "return_pct": payload.get("return_pct"),
            "active_in_current_pool": bool(current),
            "current_pool_tier": current_tier,
            "current_entry_signal": current_entry_signal,
            "current_score": (current or {}).get("score"),
            "current_note": (current or {}).get("note"),
            "stale_pool_evidence": bool(stale_payload),
            "review_command": f"atrade stock analyze {code} --json" if code else "",
        }
        if stale_payload:
            item["stale_payload"] = stale_payload
        items.append(item)
    return items


def _current_candidates_by_code(conn: Any, codes: list[str]) -> dict[str, dict[str, Any]]:
    unique_codes = sorted({code for code in codes if code})
    if not unique_codes:
        return {}
    placeholders = ",".join("?" for _ in unique_codes)
    rows = conn.execute(
        f"""SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
            FROM projection_candidate_pool
            WHERE code IN ({placeholders})""",
        tuple(unique_codes),
    ).fetchall()
    candidates = [dict(row) for row in rows]
    enrich_candidate_rows_with_latest_scores(conn, candidates)
    return {str(item.get("code") or ""): item for item in candidates}


def _stale_positive_review_payload(
    payload: dict[str, Any],
    current: dict[str, Any] | None,
) -> dict[str, Any]:
    stale: dict[str, Any] = {}
    payload_tier = payload.get("current_pool_tier")
    current_tier = current.get("pool_tier") if current else None
    if payload_tier is not None and payload_tier != current_tier:
        stale["current_pool_tier"] = payload_tier
    payload_entry = payload.get("current_entry_signal")
    current_entry = current.get("entry_signal") if current else None
    if payload_entry is not None and (
        current is None or (current_entry is not None and _truthy(payload_entry) != _truthy(current_entry))
    ):
        stale["current_entry_signal"] = payload_entry
    return stale


def _root_causes(
    *,
    pool: dict[str, Any],
    decisions: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> list[dict[str, str]]:
    causes: list[dict[str, str]] = []
    if int(pool.get("core_count") or 0) == 0:
        causes.append({
            "type": "core_pool_empty",
            "severity": "high",
            "summary": "核心候选池为空，正式买入无法承接。",
        })
    has_entry = any(_truthy(item.get("entry_signal")) for item in decisions)
    if decisions and not has_entry:
        causes.append({
            "type": "entry_signal_insufficient",
            "severity": "high",
            "summary": "近期决策暂无正式入场信号。",
        })
    if not decisions and not candidates:
        causes.append({
            "type": "candidate_flow_empty",
            "severity": "high",
            "summary": "暂无候选和近期决策证据。",
        })
    if pool.get("stale"):
        causes.append({
            "type": "candidate_refresh_required_before_next_window",
            "severity": "medium",
            "summary": "下个买入窗口前需要重新刷新候选评分。",
        })
    return causes


def _yield_target_assessment(conn: Any, *, limit: int) -> dict[str, Any]:
    events = EventStore(conn).query(event_type="paper.trial.reviewed", limit=max(limit, 20))
    returns = [
        _return_pct((event.get("payload") or {}).get("return_pct"))
        for event in events
        if (event.get("payload") or {}).get("return_pct") is not None
    ]
    reviewed_count = len(returns)
    wins = sum(1 for value in returns if value > 0)
    avg_return = round(sum(returns) / reviewed_count, 2) if reviewed_count else 0.0
    win_rate = round(wins / reviewed_count, 4) if reviewed_count else 0.0

    if reviewed_count < 10:
        status = "insufficient_sample"
        summary = (
            "影子/复盘样本不足，不能证明现实目标已经达成；先积累至少 10 笔闭合复盘。"
        )
    elif avg_return > 0 and win_rate >= 0.5:
        status = "directionally_positive"
        summary = "单票复盘为正，收益目标仍需信号数量、资金利用率和回撤共同验证。"
    else:
        status = "needs_signal_or_exit_optimization"
        summary = "当前复盘收益或胜率不足，优先优化路线适配、卖出和风控归因。"

    return {
        "status": status,
        "summary": summary,
        "target_band": {
            "conservative_annual_return_pct": "12-15%",
            "realistic_annual_return_pct": "20-25%",
            "realistic_max_drawdown_pct": "<10%",
            "guardrail": "不通过放松买入门槛来追目标；先验证信号数量、资金利用率和回撤。",
        },
        "sample": {
            "reviewed_count": reviewed_count,
            "min_required": 10,
            "avg_return_pct": avg_return,
            "win_rate_pct": win_rate,
        },
        "next_evidence": [
            "按路线统计闭合复盘收益和胜率",
            "跟踪候选池刷新是否提高买入意向数量",
            "用回撤和资金利用率验证年化目标是否可承接",
        ],
    }


def _summary(pool: dict[str, Any], actionability: dict[str, bool]) -> str:
    if actionability["formal_buy_ready"]:
        return "已有正式买入候选；仍需买入窗口、风控和人工确认。"
    if actionability["trial_tracking_available"] or actionability["watch_review_available"]:
        return (
            f"暂无正式买入；候选池 {pool.get('total', 0)} 只，"
            "已有可人工复核的观察/试买线索。"
        )
    return "暂无正式买入或可复核观察线索；先刷新候选和评分。"


def _return_pct(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    if abs(number) <= 1:
        return number * 100
    return number


def _next_actions(actionability: dict[str, bool]) -> list[dict[str, Any]]:
    if actionability["formal_buy_ready"]:
        return [{
            "type": "paper_auto_readiness",
            "command": "atrade paper auto-readiness --json",
            "risk_level": "read_only",
        }]
    if actionability["trial_tracking_available"] or actionability["watch_review_available"]:
        return [{
            "type": "review_watch_candidates",
            "command": "atrade opportunity --json",
            "risk_level": "read_only",
        }]
    return [{
        "type": "refresh_candidates",
        "command": "atrade screener refresh --json",
        "risk_level": "read_only",
    }]


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "y"}
    return bool(value)
