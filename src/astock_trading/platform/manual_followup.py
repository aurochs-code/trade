"""人工复核自动汇总。

该模块只聚合机会卡、影子试运行复盘和模拟承接预检，不写状态、不提交委托。
"""

from __future__ import annotations

from typing import Any

from astock_trading.platform.time import local_now_iso, local_today_str


SOURCE_COMMANDS = [
    "atrade opportunity --json",
    "atrade paper trial-review --json",
    "atrade paper auto-readiness --json",
    "atrade risk trial-guard --json",
]

READ_ONLY_GUARDRAILS = {
    "read_only": True,
    "writes_state": False,
    "writes_environment": False,
    "writes_order": False,
    "manual_confirmation_required_for_trade": True,
}


def build_manual_followup_report(
    conn: Any,
    *,
    opportunity: dict[str, Any] | None = None,
    trial_review: dict[str, Any] | None = None,
    auto_readiness: dict[str, Any] | None = None,
    risk_guard: dict[str, Any] | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """从运行库生成只读人工复核汇总。"""
    if opportunity is None:
        from astock_trading.platform.hermes_commands import build_opportunity_card

        opportunity = build_opportunity_card(conn)
    if trial_review is None:
        from astock_trading.platform.events import EventStore
        from astock_trading.platform.paper_trial import build_paper_trial_review

        trial_review = build_paper_trial_review(
            conn,
            EventStore(conn),
            min_age_days=0,
            record=False,
            limit=limit,
        )
    return build_manual_followup_payload(
        opportunity=opportunity,
        trial_review=trial_review,
        auto_readiness=auto_readiness,
        risk_guard=risk_guard,
    )


def build_manual_followup_payload(
    *,
    opportunity: dict[str, Any],
    trial_review: dict[str, Any] | None = None,
    auto_readiness: dict[str, Any] | None = None,
    risk_guard: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """生成 Hermes/Discord 可消费的人工复核自动汇总。"""
    trial_review = trial_review or {}
    auto_readiness = auto_readiness or {}
    risk_guard = risk_guard or {}
    candidates = _positive_trial_candidates(opportunity, trial_review)
    candidate_reviews = [
        _candidate_review(item, auto_readiness=auto_readiness)
        for item in candidates
    ]
    manual_actions = _manual_actions(opportunity, auto_readiness=auto_readiness)
    status = _manual_followup_status(
        opportunity=opportunity,
        candidate_reviews=candidate_reviews,
        manual_actions=manual_actions,
        auto_readiness=auto_readiness,
    )
    return {
        "command": "review manual-followup",
        "status": status,
        "date": _payload_date(opportunity, trial_review),
        "captured_at": local_now_iso(),
        "summary": _manual_followup_summary(
            status=status,
            opportunity=opportunity,
            candidate_reviews=candidate_reviews,
            manual_actions=manual_actions,
        ),
        "candidate_summary": opportunity.get("candidate_summary", {}) or {},
        "counts": _counts(
            opportunity=opportunity,
            candidates=candidates,
            manual_actions=manual_actions,
        ),
        "candidate_reviews": candidate_reviews,
        "manual_actions": manual_actions,
        "next_action": _next_action(
            status=status,
            opportunity=opportunity,
            auto_readiness=auto_readiness,
            candidate_reviews=candidate_reviews,
            manual_actions=manual_actions,
        ),
        "auto_readiness": _compact_auto_readiness(auto_readiness),
        "risk_guard": _compact_risk_guard(risk_guard),
        "source_commands": SOURCE_COMMANDS,
        "guardrails": dict(READ_ONLY_GUARDRAILS),
    }


def _payload_date(opportunity: dict[str, Any], trial_review: dict[str, Any]) -> str:
    return str(opportunity.get("date") or trial_review.get("date") or local_today_str())


def _positive_trial_candidates(
    opportunity: dict[str, Any],
    trial_review: dict[str, Any],
) -> list[dict[str, Any]]:
    for key in (
        "active_positive_trial_candidates",
        "positive_trial_candidates",
    ):
        items = opportunity.get(key)
        if isinstance(items, list) and items:
            return [_normalize_candidate(item) for item in items if isinstance(item, dict)]
    reviews = trial_review.get("positive_reviews")
    if isinstance(reviews, list):
        return [_normalize_candidate(item) for item in reviews if isinstance(item, dict)]
    return []


def _normalize_candidate(item: dict[str, Any]) -> dict[str, Any]:
    code = str(item.get("code") or "")
    normalized = dict(item)
    normalized.setdefault("name", code)
    normalized.setdefault("review_command", f"atrade stock analyze {code} --json")
    if "current_pool_tier" not in normalized and "pool_tier" in normalized:
        normalized["current_pool_tier"] = normalized.get("pool_tier")
    if "current_pool_tier_label" not in normalized:
        normalized["current_pool_tier_label"] = normalized.get("pool_tier_label") or _tier_label(
            normalized.get("current_pool_tier")
        )
    if "current_score" not in normalized and "score" in normalized:
        normalized["current_score"] = normalized.get("score")
    if "current_entry_signal" not in normalized and "entry_signal" in normalized:
        normalized["current_entry_signal"] = normalized.get("entry_signal")
    return normalized


def _manual_followup_status(
    *,
    opportunity: dict[str, Any],
    candidate_reviews: list[dict[str, Any]],
    manual_actions: list[dict[str, Any]],
    auto_readiness: dict[str, Any],
) -> str:
    if _paper_order_approval_ready(auto_readiness):
        return "approval_required"
    if str(opportunity.get("status") or "") == "needs_health_check":
        return "needs_health_check"
    if candidate_reviews:
        return "review_candidates"
    if manual_actions:
        return "manual_review"
    return str(opportunity.get("status") or "watching")


def _manual_followup_summary(
    *,
    status: str,
    opportunity: dict[str, Any],
    candidate_reviews: list[dict[str, Any]],
    manual_actions: list[dict[str, Any]],
) -> str:
    if status == "needs_health_check":
        return str(opportunity.get("summary") or "先修运行/数据问题，暂停新增交易判断。")
    if status == "approval_required":
        return "模拟承接预检已通过；是否运行模拟盘自动交易必须由你明确确认。"
    if candidate_reviews:
        return f"有 {len(candidate_reviews)} 只影子试运行候选需要人工复核；系统不会自动晋级或下单。"
    if manual_actions:
        return f"有 {len(manual_actions)} 项人工动作待处理；系统不会自动执行。"
    return str(opportunity.get("summary") or "暂无需要人工处理的复核项。")


def _counts(
    *,
    opportunity: dict[str, Any],
    candidates: list[dict[str, Any]],
    manual_actions: list[dict[str, Any]],
) -> dict[str, Any]:
    source_counts = opportunity.get("counts", {}) or {}
    stale_items = opportunity.get("stale_buy_intents")
    buy_items = opportunity.get("buy_intents")
    return {
        **source_counts,
        "buy_intents": _item_count(buy_items, source_counts.get("buy_intents")),
        "stale_buy_intents": _item_count(stale_items, source_counts.get("stale_buy_intents")),
        "positive_trial_candidates": len(candidates),
        "manual_actions": len(manual_actions),
    }


def _item_count(items: Any, fallback: Any) -> int:
    if isinstance(items, list):
        return len(items)
    return _to_int(fallback)


def _candidate_review(item: dict[str, Any], *, auto_readiness: dict[str, Any]) -> dict[str, Any]:
    classification = _candidate_classification(item, auto_readiness=auto_readiness)
    code = str(item.get("code") or "")
    command = str(item.get("review_command") or f"atrade stock analyze {code} --json")
    next_action = {
        "type": "review_candidate",
        "label": "复核个股",
        "command": command,
        "reason": "只读复核个股证据，不自动交易。",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "stock_analyze",
    }
    if classification["classification"] == "requires_paper_order_approval":
        next_action = {
            "type": "review_paper_auto_readiness",
            "label": "复核模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "reason": "模拟盘委托动作必须先复核预检结果，再由你明确确认。",
            "safe_to_auto_apply": True,
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
            "command_contract_id": "paper_auto_readiness",
        }
    return {
        "code": code,
        "name": item.get("name") or code,
        "return_pct": _to_float(item.get("return_pct")),
        "current_pool_tier": item.get("current_pool_tier") or "",
        "current_pool_tier_label": item.get("current_pool_tier_label") or _tier_label(
            item.get("current_pool_tier")
        ),
        "current_score": _to_float(item.get("current_score")),
        "current_entry_signal": _truthy(item.get("current_entry_signal")),
        "current_primary_strategy_route_label": item.get("current_primary_strategy_route_label") or "",
        "current_data_quality": item.get("current_data_quality") or "",
        "classification": classification["classification"],
        "classification_label": classification["classification_label"],
        "reason": classification["reason"],
        "next_action": next_action,
        "source": item.get("review_source") or item.get("source") or "",
        "source_event_id": item.get("source_event_id") or "",
    }


def _candidate_classification(
    item: dict[str, Any],
    *,
    auto_readiness: dict[str, Any],
) -> dict[str, str]:
    tier = str(item.get("current_pool_tier") or "")
    entry_signal = _truthy(item.get("current_entry_signal"))
    if item.get("active_candidate") is False or not tier:
        return {
            "classification": "inactive",
            "classification_label": "仅留证据",
            "reason": "已移出当前候选池，只保留影子复盘证据。",
        }
    if tier == "core" and entry_signal and _paper_order_approval_ready(auto_readiness):
        return {
            "classification": "requires_paper_order_approval",
            "classification_label": "需要你确认",
            "reason": "核心候选已有入场信号，模拟承接预检已通过；提交 MX 模拟盘委托前必须人工确认。",
        }
    if tier == "core" and entry_signal:
        return {
            "classification": "wait_auto_readiness",
            "classification_label": "等待自动承接",
            "reason": "核心候选已有入场信号，先复核模拟承接预检。",
        }
    if tier == "core":
        return {
            "classification": "review_core",
            "classification_label": "复核核心",
            "reason": "已在核心池，但当前入场信号未触发。",
        }
    if tier in {"watch", "radar"} and not entry_signal:
        return {
            "classification": "continue_observe",
            "classification_label": "继续观察",
            "reason": "仍在观察层，且没有当前入场信号。",
        }
    return {
        "classification": "review_entry_signal",
        "classification_label": "复核入场",
        "reason": "候选已有当前入场信号，先只读复核证据和风险。",
    }


def _manual_actions(
    opportunity: dict[str, Any],
    *,
    auto_readiness: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if _paper_order_approval_ready(auto_readiness):
        actions.append({
            "type": "approve_paper_auto_trade",
            "label": "确认是否运行模拟盘自动交易",
            "command": "atrade run-pipeline auto_trade --json",
            "reason": "模拟承接预检已通过，但该命令可能提交 MX 模拟盘委托，必须由你明确批准。",
            "safe_to_auto_apply": False,
            "writes_state": True,
            "writes_environment": False,
            "writes_order": True,
            "requires_user_approval": True,
            "risk_level": "paper_order_execution",
        })
    if _item_count(opportunity.get("stale_buy_intents"), (opportunity.get("counts") or {}).get("stale_buy_intents")):
        actions.append({
            "type": "review_stale_manual_confirmation",
            "label": "复核过期买入意向",
            "command": "atrade manual-trades list --status stale --json",
            "reason": "有买入意向已过期，需要决定是否结案。",
            "safe_to_auto_apply": True,
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
            "command_contract_id": "manual_trades_stale",
        })
    for action in opportunity.get("evidence_actions", []) or []:
        if not isinstance(action, dict):
            continue
        actions.append(dict(action))
    return actions


def _next_action(
    *,
    status: str,
    opportunity: dict[str, Any],
    auto_readiness: dict[str, Any],
    candidate_reviews: list[dict[str, Any]],
    manual_actions: list[dict[str, Any]],
) -> dict[str, Any]:
    if status == "approval_required":
        return {
            "type": "review_paper_auto_readiness",
            "label": "复核模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "reason": "模拟承接预检已通过；提交委托前仍需你明确确认。",
            "safe_to_auto_apply": True,
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "requires_user_approval": False,
            "risk_level": "read_only",
            "command_contract_id": "paper_auto_readiness",
        }
    next_action = opportunity.get("next_action")
    if isinstance(next_action, dict) and next_action:
        return next_action
    if candidate_reviews:
        return candidate_reviews[0].get("next_action", {}) or {}
    if manual_actions:
        return manual_actions[0]
    readiness_action = auto_readiness.get("next_action")
    if isinstance(readiness_action, dict) and readiness_action:
        return readiness_action
    return {
        "type": "monitor",
        "label": "继续观察",
        "command": "atrade opportunity --json",
        "reason": "暂无需要人工处理的复核项。",
        "safe_to_auto_apply": True,
        "writes_state": False,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": "read_only",
        "command_contract_id": "opportunity",
    }


def _paper_order_approval_ready(auto_readiness: dict[str, Any]) -> bool:
    buy_side = auto_readiness.get("buy_side", {}) or {}
    next_action = auto_readiness.get("next_action", {}) or {}
    return bool(
        buy_side.get("ready")
        and next_action.get("writes_order")
        and next_action.get("requires_user_approval")
    )


def _compact_auto_readiness(auto_readiness: dict[str, Any]) -> dict[str, Any]:
    if not auto_readiness:
        return {}
    buy_side = auto_readiness.get("buy_side", {}) or {}
    return {
        "status": auto_readiness.get("status", ""),
        "summary": auto_readiness.get("summary", ""),
        "buy_side": {
            "ready": bool(buy_side.get("ready")),
            "blockers": buy_side.get("blockers", []) or [],
        },
        "next_action": auto_readiness.get("next_action", {}) or {},
    }


def _compact_risk_guard(risk_guard: dict[str, Any]) -> dict[str, Any]:
    if not risk_guard:
        return {}
    return {
        "status": risk_guard.get("status", ""),
        "summary": risk_guard.get("summary", ""),
        "candidate_summary": risk_guard.get("candidate_summary", {}) or {},
        "next_action": risk_guard.get("next_action", {}) or {},
    }


def _tier_label(value: Any) -> str:
    return {
        "core": "核心",
        "watch": "观察",
        "radar": "强势观察",
    }.get(str(value or ""), str(value or ""))


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on", "buy"}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
