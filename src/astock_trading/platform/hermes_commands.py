"""Hermes 友好的只读交易摘要与解释。"""

from __future__ import annotations

from typing import Any

from astock_trading.platform.events import EventStore
from astock_trading.platform.time import local_today_str

ACTION_LABELS = {
    "BUY": "买入意向",
    "SELL": "卖出意向",
    "WATCH": "观察",
    "NO_TRADE": "不操作",
}


def build_digest(conn: Any) -> dict[str, Any]:
    """一句话总结当前状态。"""
    store = EventStore(conn)
    pending_manual = _pending_manual_trades(store)
    latest_decision = _latest_event_payload(store, "decision.suggested")
    latest_score = _latest_event_payload(store, "score.calculated")
    positions = _positions(conn)
    failed_runs = _recent_failed_runs(conn)
    status = _overall_status(
        pending_manual_count=len(pending_manual),
        failed_run_count=len(failed_runs),
    )

    decision_label = _action_label(latest_decision.get("action", ""))
    decision_text = (
        f"{latest_decision.get('code', '')} {decision_label}"
        if latest_decision
        else "暂无决策"
    )
    summary = (
        f"今日状态：待人工确认 {len(pending_manual)}，"
        f"持仓 {len(positions)}，失败运行 {len(failed_runs)}，"
        f"最新决策 {decision_text}。"
    )

    return {
        "command": "digest",
        "status": status,
        "date": local_today_str(),
        "summary": summary,
        "pending_manual_trades": len(pending_manual),
        "pending_manual_trade_items": pending_manual[:5],
        "positions": {
            "count": len(positions),
            "items": positions[:5],
        },
        "failed_runs": failed_runs[:5],
        "latest_decision": _decision_payload(latest_decision),
        "latest_score": _score_payload(latest_score),
    }


def build_suggestion(conn: Any) -> dict[str, Any]:
    """基于当前状态输出下一步建议，不执行交易。"""
    digest = build_digest(conn)
    pending = digest["pending_manual_trade_items"]
    failed_runs = digest["failed_runs"]
    latest_decision = digest.get("latest_decision") or {}
    latest_score = digest.get("latest_score") or {}

    if pending:
        action = {
            "type": "manual_confirmation",
            "label": "处理人工确认",
            "command": "atrade manual-trades list --json",
            "reason": "存在待人工确认的买入意向，真实交易必须由人工确认。",
            "safe_to_auto_apply": False,
        }
        recommendation = "先复核待确认项；不自动下单。"
        status = "needs_manual_confirmation"
    elif failed_runs:
        action = {
            "type": "inspect_health",
            "label": "检查运行失败",
            "command": "atrade health --json",
            "reason": "近期 pipeline 有失败记录，先确认数据链路健康再看新交易。",
            "safe_to_auto_apply": True,
        }
        recommendation = "先修运行/数据问题，暂停新增交易判断。"
        status = "needs_health_check"
    elif latest_decision.get("action") == "BUY":
        code = latest_decision.get("code", "")
        action = {
            "type": "explain_buy_intent",
            "label": "解释买入意向",
            "command": f"atrade explain {code} --json" if code else "atrade screener explain --json",
            "reason": "存在最新买入意向，但仍要看评分、否决和人工确认链路。",
            "safe_to_auto_apply": False,
        }
        recommendation = "有买入意向，先解释证据，再走人工确认。"
        status = "review_buy_intent"
    elif latest_score:
        action = {
            "type": "wait_or_review_candidates",
            "label": "等待或复核候选",
            "command": "atrade screener explain --json",
            "reason": "已有评分证据但没有待确认买入，适合复核候选漏斗或继续等待。",
            "safe_to_auto_apply": True,
        }
        recommendation = "当前以等待和复核为主，不主动降低买入门槛。"
        status = "wait"
    else:
        action = {
            "type": "refresh_scores",
            "label": "刷新评分证据",
            "command": "atrade screener refresh --json",
            "reason": "缺少近期评分/决策证据，先刷新再判断策略是否过严。",
            "safe_to_auto_apply": True,
        }
        recommendation = "先刷新证据，不凭空给交易建议。"
        status = "needs_evidence"

    return {
        "command": "suggest",
        "status": status,
        "summary": recommendation,
        "recommendation": recommendation,
        "execution_allowed": False,
        "next_action": action,
        "digest": digest,
        "guardrails": {
            "manual_confirmation_required": True,
            "no_broker_api": True,
            "auto_threshold_change_allowed": False,
        },
    }


def build_explanation(conn: Any, code: str) -> dict[str, Any]:
    """解释单只股票最近评分和决策。"""
    store = EventStore(conn)
    score_event = _latest_code_event(store, "score.calculated", code)
    decision_event = _latest_code_event(store, "decision.suggested", code)
    score = score_event.get("payload", {}) if score_event else {}
    decision = decision_event.get("payload", {}) if decision_event else {}
    action = str(decision.get("action", ""))
    label = _action_label(action)

    if not score and not decision:
        summary = f"{code} 暂无本地评分/决策证据；先运行单股分析或刷新评分。"
        status = "warning"
        next_command = f"atrade stock analyze {code} --json"
    elif action == "BUY":
        summary = f"{code} 最新为{label}，仍需人工确认；评分 {decision.get('score', score.get('total_score', 0))}。"
        status = "buy_intent"
        next_command = "atrade manual-trades list --json"
    elif action:
        summary = f"{code} 最新决策为{label}；先按当前证据等待或观察。"
        status = "ok"
        next_command = f"atrade stock analyze {code} --json"
    else:
        summary = f"{code} 有评分但暂无决策事件；建议补跑评分或查看单股分析。"
        status = "warning"
        next_command = f"atrade stock analyze {code} --json"

    return {
        "command": "explain",
        "status": status,
        "code": code,
        "summary": summary,
        "latest_score": _score_payload(score, event=score_event),
        "latest_decision": _decision_payload(decision, event=decision_event),
        "blockers": {
            "data_quality": score.get("data_quality", "unknown") if score else "missing",
            "entry_signal": bool(score.get("entry_signal")) if score else False,
            "veto_triggered": bool(score.get("veto_triggered")) if score else False,
            "veto_reasons": decision.get("veto_reasons", []) or score.get("hard_veto_signals", []),
        },
        "next_action": {
            "command": next_command,
            "reason": "只读解释，不执行交易。",
            "safe_to_auto_apply": False,
        },
        "execution_allowed": False,
    }


def _pending_manual_trades(store: EventStore) -> list[dict[str, Any]]:
    events = store.query(stream_type="manual_trade", limit=200)
    by_stream: dict[str, dict[str, Any]] = {}
    for event in events:
        payload = event.get("payload", {})
        stream = event.get("stream", "")
        current = by_stream.get(stream, {})
        if event["event_type"] == "manual_trade.requested":
            current = {
                **payload,
                "requested_event_id": event["event_id"],
                "requested_at": event["occurred_at"],
                "updated_at": event["occurred_at"],
            }
        elif current:
            current["status"] = payload.get("status") or event["event_type"].removeprefix("manual_trade.")
            current["updated_at"] = event["occurred_at"]
        if current:
            by_stream[stream] = current
    pending = [item for item in by_stream.values() if item.get("status", "pending") == "pending"]
    return sorted(pending, key=lambda item: item.get("updated_at", ""), reverse=True)


def _latest_event_payload(store: EventStore, event_type: str) -> dict[str, Any]:
    events = store.query(event_type=event_type, limit=200)
    return events[-1].get("payload", {}) if events else {}


def _latest_code_event(store: EventStore, event_type: str, code: str) -> dict[str, Any]:
    events = store.query(event_type=event_type, limit=500)
    matched = [event for event in events if str(event.get("payload", {}).get("code", "")) == code]
    return matched[-1] if matched else {}


def _positions(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT code, name, shares, avg_cost_cents, current_price_cents, unrealized_pnl_cents "
        "FROM projection_positions ORDER BY updated_at DESC LIMIT 20"
    ).fetchall()
    return [dict(row) for row in rows]


def _recent_failed_runs(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT run_id, run_type, started_at, error_message "
        "FROM run_log WHERE status = 'failed' ORDER BY started_at DESC LIMIT 20"
    ).fetchall()
    return [dict(row) for row in rows]


def _overall_status(*, pending_manual_count: int, failed_run_count: int) -> str:
    if pending_manual_count:
        return "needs_manual_confirmation"
    if failed_run_count:
        return "warning"
    return "ok"


def _action_label(action: str) -> str:
    return ACTION_LABELS.get(action, action or "无决策")


def _score_payload(score: dict[str, Any], event: dict[str, Any] | None = None) -> dict[str, Any]:
    if not score:
        return {}
    payload = {
        "code": score.get("code", ""),
        "name": score.get("name", ""),
        "total_score": score.get("total_score", score.get("score", 0)),
        "technical_score": score.get("technical_score", 0),
        "fundamental_score": score.get("fundamental_score", 0),
        "flow_score": score.get("flow_score", 0),
        "sentiment_score": score.get("sentiment_score", 0),
        "data_quality": score.get("data_quality", "unknown"),
        "entry_signal": bool(score.get("entry_signal")),
        "veto_triggered": bool(score.get("veto_triggered")),
    }
    if event:
        payload["event_id"] = event.get("event_id", "")
        payload["occurred_at"] = event.get("occurred_at", "")
    return payload


def _decision_payload(decision: dict[str, Any], event: dict[str, Any] | None = None) -> dict[str, Any]:
    if not decision:
        return {}
    action = str(decision.get("action", ""))
    payload = {
        "code": decision.get("code", ""),
        "name": decision.get("name", ""),
        "action": action,
        "action_label": _action_label(action),
        "score": decision.get("score", 0),
        "confidence": decision.get("confidence", 0),
        "source_score_event_id": decision.get("source_score_event_id", ""),
        "veto_reasons": decision.get("veto_reasons", []) or [],
        "notes": decision.get("notes", []) or [],
    }
    if event:
        payload["event_id"] = event.get("event_id", "")
        payload["occurred_at"] = event.get("occurred_at", "")
    return payload
