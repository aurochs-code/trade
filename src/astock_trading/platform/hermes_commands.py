"""Hermes 友好的只读交易摘要与解释。"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from astock_trading.pipeline.strategy_profiles import latest_strategy_profile_activation_request
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.agent_diagnostics import (
    build_next_window_first_run_verification,
    candidate_pool_summary,
    diagnose_schedule,
)
from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores
from astock_trading.platform.data_source_diagnostics import (
    build_data_source_diagnosis,
    data_source_blocker_summary,
    data_source_blockers_for_new_trades,
)
from astock_trading.platform.events import EventStore
from astock_trading.platform.manual_trade_state import (
    actionable_pending_manual_trades,
    load_manual_confirmation_policy,
    manual_trade_states,
    stale_pending_manual_trades,
)
from astock_trading.platform.paths import default_state_dir
from astock_trading.platform.pipeline_policy import filter_unrecovered_failed_runs
from astock_trading.platform.time import MARKET_TZ, local_now, local_now_iso, local_today_str, utc_now

ACTION_LABELS = {
    "BUY": "买入意向",
    "TRIAL_BUY": "试买意向",
    "SELL": "卖出意向",
    "WATCH": "观察",
    "CLEAR": "观望",
    "NO_TRADE": "不操作",
}
NEXT_WINDOW_STEP_SCRIPTS = {
    "a_stock_screener_refresh_intraday_silent.sh",
    "a_stock_intraday_execution_cycle_silent.sh",
    "a_stock_pipeline_auto_trade_silent.sh",
}
OPPORTUNITY_WATCH_ACTION_STATUSES = {
    "needs_manual_confirmation",
    "needs_health_check",
    "review_buy_intent",
    "review_positive_trial",
    "profile_review_required",
    "paper_auto_readiness",
    "review_stale_manual_confirmation",
    "needs_evidence",
}


def build_digest(conn: Any) -> dict[str, Any]:
    """一句话总结当前状态。"""
    store = EventStore(conn)
    manual_states = _manual_trade_states(store)
    pending_manual = actionable_pending_manual_trades(manual_states)
    stale_manual = stale_pending_manual_trades(manual_states)
    latest_decision_event = _latest_event(store, "decision.suggested")
    latest_score_event = _latest_event(store, "score.calculated")
    latest_decision = latest_decision_event.get("payload", {}) if latest_decision_event else {}
    latest_score = latest_score_event.get("payload", {}) if latest_score_event else {}
    positions = _positions(conn)
    failed_runs = _recent_failed_runs(conn)
    recent_unusable_buy_signal = _recent_unusable_buy_signal(conn)
    attention = _digest_attention(
        conn,
        stale_manual=stale_manual,
        pending_manual=pending_manual,
        failed_run_count=len(failed_runs),
    )
    status = attention.get("status") or _overall_status(
        pending_manual_count=len(pending_manual),
        failed_run_count=len(failed_runs),
    )

    signal_focus = _digest_signal_focus(
        pending_manual=pending_manual,
        stale_manual=stale_manual,
        latest_decision=latest_decision,
    )
    summary = (
        f"今日状态：待人工确认 {len(pending_manual)}，"
        f"过期待复核 {len(stale_manual)}，"
        f"持仓 {len(positions)}，失败运行 {len(failed_runs)}，"
        f"{signal_focus.get('summary_label', '最新决策')} {signal_focus.get('text', '暂无决策')}。"
    )
    if attention:
        summary = f"{summary} 注意：{attention.get('summary', '')}"
    recent_unusable_text = _recent_unusable_buy_signal_text(recent_unusable_buy_signal)
    if recent_unusable_text:
        summary = f"{summary} {recent_unusable_text}"

    return {
        "command": "digest",
        "status": status,
        "date": local_today_str(),
        "summary": summary,
        "attention": attention,
        "pending_manual_trades": len(pending_manual),
        "pending_manual_trade_items": pending_manual[:5],
        "stale_manual_trades": len(stale_manual),
        "stale_manual_trade_items": stale_manual[:5],
        "recent_unusable_buy_signal": recent_unusable_buy_signal,
        "signal_focus": signal_focus,
        "positions": {
            "count": len(positions),
            "items": positions[:5],
        },
        "failed_runs": failed_runs[:5],
        "latest_decision": _decision_payload(latest_decision, event=latest_decision_event),
        "latest_score": _score_payload(latest_score, event=latest_score_event),
    }


def _digest_signal_focus(
    *,
    pending_manual: list[dict[str, Any]],
    stale_manual: list[dict[str, Any]],
    latest_decision: dict[str, Any],
) -> dict[str, Any]:
    if pending_manual:
        return _manual_trade_signal_focus("pending_manual_trade", "当前重点", pending_manual)
    if stale_manual:
        return _manual_trade_signal_focus("stale_manual_trade", "过期待复核", stale_manual)
    if latest_decision:
        action = str(latest_decision.get("action", ""))
        return {
            "type": "latest_decision",
            "summary_label": "最新决策",
            "code": latest_decision.get("code", ""),
            "name": latest_decision.get("name", ""),
            "action": action,
            "action_label": _action_label(action),
            "score": latest_decision.get("score", 0),
            "text": f"{latest_decision.get('code', '')} {_action_label(action)}",
        }
    return {
        "type": "none",
        "summary_label": "最新决策",
        "text": "暂无决策",
    }


def _manual_trade_signal_focus(
    focus_type: str,
    summary_label: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    item = max(items, key=lambda row: _manual_trade_focus_score(row)) if items else {}
    side = str(item.get("side") or "buy").lower()
    action = "SELL" if side == "sell" else "BUY"
    score = item.get("score", item.get("confidence"))
    code = str(item.get("code", ""))
    name = str(item.get("name", ""))
    action_label = _action_label(action)
    score_part = f" {_score_text(score)} 分" if score is not None else ""
    name_part = f" {name}" if name else ""
    return {
        "type": focus_type,
        "summary_label": summary_label,
        "code": code,
        "name": name,
        "side": side,
        "action": action,
        "action_label": action_label,
        "score": score,
        "text": f"{code}{name_part} {action_label}{score_part}".strip(),
    }


def _manual_trade_focus_score(item: dict[str, Any]) -> tuple[float, str]:
    try:
        score = float(item.get("score", item.get("confidence", 0)) or 0)
    except (TypeError, ValueError):
        score = 0.0
    return score, str(item.get("updated_at") or item.get("requested_at") or "")


def _digest_attention(
    conn: Any,
    *,
    stale_manual: list[dict[str, Any]],
    pending_manual: list[dict[str, Any]],
    failed_run_count: int,
) -> dict[str, Any]:
    if failed_run_count:
        return {}
    core_buy_intents = [*pending_manual, *stale_manual]
    has_core_buy_signal = _core_buy_signal_needs_auto_readiness(conn, core_buy_intents)
    if has_core_buy_signal:
        schedule_diagnosis = diagnose_schedule(conn)
        runtime_profile = schedule_diagnosis.get("runtime_profile", {}) or {}
        if _runtime_profile_blocks_simulation(runtime_profile):
            recommended_profile = runtime_profile.get("recommended_profile") or "trend_swing"
            summary = (
                "已有核心候选和过期买入意向；模拟承接前先复核运行 profile。"
                if stale_manual and not pending_manual
                else "已有核心候选和买入意向；模拟承接前先复核运行 profile。"
            )
            return {
                "status": "profile_review_required",
                "label": "复核运行 profile 激活",
                "summary": summary,
                "command": f"atrade strategy profile-activation --target {recommended_profile} --json",
                "safe_to_auto_apply": False,
                **_action_contract("strategy_profile_activation_review"),
            }
    if pending_manual:
        return {}
    if not stale_manual:
        return {}
    if not has_core_buy_signal:
        return {
            "status": "review_stale_manual_confirmation",
            "label": "复核过期买入意向",
            "summary": "有买入意向已过期或错过窗口；先复核或显式过期处理。",
            "command": "atrade manual-trades list --status stale --json",
            "safe_to_auto_apply": False,
            **_action_contract("manual_trades_stale"),
        }

    return {
        "status": "paper_auto_readiness",
        "label": "检查模拟盘自动交易预检",
        "summary": "已有核心候选和过期买入意向；先检查模拟盘自动交易预检。",
        "command": "atrade paper auto-readiness --json",
        "safe_to_auto_apply": True,
        **_action_contract("paper_auto_readiness"),
    }


def _recent_unusable_buy_signal(conn: Any) -> dict[str, Any]:
    """读取近期已产生但不能被当前模拟买入窗口承接的买入意向。"""
    try:
        from astock_trading.pipeline.auto_trade import (
            _buy_signal_event_summary,
            _buy_signal_unusable_reason,
        )
    except Exception:
        return {"count": 0, "max_age_hours": 24, "top": {}}

    try:
        data, _errors = ConfigRegistry().load_and_validate()
    except Exception:
        data = {}
    strategy = data.get("strategy", {}) if isinstance(data, dict) else {}
    cfg = (strategy.get("auto_trade", {}) or {}) if isinstance(strategy, dict) else {}
    max_age_hours = _recent_buy_signal_max_age_hours(strategy, cfg)
    current = local_now()
    current_utc = current.astimezone(timezone.utc) if current.tzinfo else current.replace(tzinfo=timezone.utc)
    since = (current_utc - timedelta(hours=max_age_hours)).isoformat()
    try:
        rows = conn.execute(
            """SELECT * FROM event_log
               WHERE event_type = ? AND occurred_at >= ?
               ORDER BY occurred_at DESC, stream_version DESC
               LIMIT 500""",
            ("decision.suggested", since),
        ).fetchall()
    except Exception:
        return {"count": 0, "max_age_hours": max_age_hours, "top": {}}

    ctx = SimpleNamespace(conn=conn)
    buy_events: list[dict[str, Any]] = []
    for row in rows:
        event = EventStore._row_to_dict(row)
        payload = event.get("payload", {}) or {}
        if payload.get("action") != "BUY":
            continue
        occurred = _parse_dt(event.get("occurred_at"))
        if occurred is None:
            continue
        occurred_utc = occurred.astimezone(timezone.utc) if occurred.tzinfo else occurred.replace(tzinfo=timezone.utc)
        if not (current_utc - timedelta(hours=max_age_hours) <= occurred_utc <= current_utc):
            continue
        reason = _buy_signal_unusable_reason(event, cfg, current_utc)
        if reason is None:
            continue
        reason_code, reason_label = reason
        item = _buy_signal_event_summary(event, ctx=ctx)
        item["unusable_reason"] = reason_code
        item["unusable_reason_label"] = reason_label
        item["carries_to_current_window"] = False
        buy_events.append(item)

    buy_events.sort(key=lambda item: (item.get("score") or 0, item.get("occurred_at") or ""), reverse=True)
    return {
        "count": len(buy_events),
        "max_age_hours": max_age_hours,
        "top": buy_events[0] if buy_events else {},
    }


def _recent_buy_signal_max_age_hours(strategy: dict[str, Any], cfg: dict[str, Any]) -> int:
    scoring_cfg = strategy.get("scoring", {}) or {}
    guard_cfg = cfg.get("buy_guard", {}) or {}
    for value in (
        guard_cfg.get("max_age_hours"),
        cfg.get("candidate_pool_max_age_hours"),
        scoring_cfg.get("max_age_hours"),
        scoring_cfg.get("freshness_max_age_hours"),
    ):
        if value:
            return int(value)
    return 24


def _recent_unusable_buy_signal_text(signal: dict[str, Any] | None) -> str:
    if not signal or int(signal.get("count") or 0) <= 0:
        return ""
    top = signal.get("top", {}) or {}
    code = str(top.get("code") or "")
    name = str(top.get("name") or code)
    score = _recent_signal_score_text(top.get("score", 0))
    reason = top.get("unusable_reason_label") or top.get("unusable_reason") or "不满足当前承接窗口"
    return f"近期买入意向 {signal.get('count')} 条不可承接；最高分为 {name}({code}) {score} 分，原因：{reason}。"


def _recent_signal_score_text(value: Any) -> str:
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return str(value)


def build_suggestion(conn: Any) -> dict[str, Any]:
    """基于当前状态输出下一步建议，不执行交易。"""
    digest = build_digest(conn)
    pending = digest["pending_manual_trade_items"]
    stale_pending = digest.get("stale_manual_trade_items", [])
    failed_runs = digest["failed_runs"]
    latest_decision = digest.get("latest_decision") or {}
    latest_score = digest.get("latest_score") or {}
    data_source_diagnosis = build_data_source_diagnosis(conn)
    candidate_pool = candidate_pool_summary(conn)
    candidate_items = _candidate_pool_items(conn, limit=1000)
    candidate_summary = _opportunity_candidate_summary(candidate_pool, candidate_items)
    current_entry_signals = [
        _compact_candidate_for_summary(item)
        for item in candidate_items
        if _candidate_has_entry_signal(item)
    ]
    profile_activation = _profile_activation_diagnostics(conn)
    schedule_diagnosis = diagnose_schedule(conn)
    runtime_profile = (schedule_diagnosis.get("runtime_profile", {}) or {})
    data_source_blockers = data_source_blockers_for_new_trades(data_source_diagnosis)
    recorded_positive_trials = _positive_trial_candidates(conn)
    preview_positive_trials = _positive_trial_preview_candidates(conn)
    positive_trials = _merge_positive_trial_candidates(
        recorded_positive_trials,
        preview_positive_trials,
    )
    active_positive_trials = [item for item in positive_trials if item.get("active_candidate")]
    inactive_positive_trials = [item for item in positive_trials if not item.get("active_candidate")]
    evidence_actions = _positive_trial_evidence_actions(active_positive_trials)
    buy_intents_for_auto_readiness = [*pending, *stale_pending]
    recent_unusable_buy_signal = digest.get("recent_unusable_buy_signal", {}) or {}
    has_pending_core_buy_intent = _core_buy_signal_needs_auto_readiness(conn, pending)
    has_core_buy_intent = _core_buy_signal_needs_auto_readiness(conn, buy_intents_for_auto_readiness)
    has_stale_core_buy_intent = _core_buy_signal_needs_auto_readiness(conn, stale_pending)

    if _runtime_profile_blocks_simulation(runtime_profile) and has_core_buy_intent:
        recommended_profile = runtime_profile.get("recommended_profile") or "trend_swing"
        effective_profile = runtime_profile.get("effective_profile") or "default"
        intent_text = "买入意向" if has_pending_core_buy_intent else "过期买入意向"
        action = {
            "type": "review_runtime_profile_activation",
            "label": "复核运行 profile 激活",
            "command": f"atrade strategy profile-activation --target {recommended_profile} --json",
            "reason": (
                f"已有核心候选和{intent_text}，但运行环境仍会使用 {effective_profile}；"
                f"先人工确认 {recommended_profile} profile。"
            ),
            "safe_to_auto_apply": False,
            **_action_contract("strategy_profile_activation_review"),
        }
        recommendation = f"已有核心候选和{intent_text}；模拟承接前先复核运行 profile 激活。"
        status = "profile_review_required"
    elif pending:
        action = {
            "type": "manual_confirmation",
            "label": "处理人工确认",
            "command": "atrade manual-trades list --json",
            "reason": "存在待人工确认的买入意向，真实交易必须由人工确认。",
            "safe_to_auto_apply": False,
            **_action_contract("manual_trades_list"),
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
            **_action_contract("health"),
        }
        recommendation = "先修运行/数据问题，暂停新增交易判断。"
        status = "needs_health_check"
    elif data_source_blockers:
        action = {
            "type": "inspect_data_sources",
            "label": "检查数据覆盖",
            "command": "atrade data-sources diagnose --json",
            "reason": data_source_blocker_summary(data_source_blockers),
            "safe_to_auto_apply": True,
            **_action_contract("data_sources_diagnose"),
        }
        recommendation = "先修运行/数据问题，暂停新增交易判断。"
        status = "needs_health_check"
    elif latest_decision.get("action") == "BUY" and not _buy_intent_is_stale(latest_decision, stale_pending):
        code = latest_decision.get("code", "")
        action = {
            "type": "explain_buy_intent",
            "label": "解释买入意向",
            "command": f"atrade explain {code} --json" if code else "atrade screener explain --json",
            "reason": "存在最新买入意向，但仍要看评分、否决和人工确认链路。",
            "safe_to_auto_apply": False,
            **_action_contract("explain" if code else "screener_explain"),
        }
        recommendation = "有买入意向，先解释证据，再走人工确认。"
        status = "review_buy_intent"
    elif has_stale_core_buy_intent:
        action = {
            "type": "paper_auto_readiness",
            "label": "检查模拟盘自动交易预检",
            "command": "atrade paper auto-readiness --json",
            "reason": "已有核心候选和买入意向，但人工确认已过期或可能错过买入窗口；先检查模拟盘自动交易预检。",
            "safe_to_auto_apply": True,
            **_action_contract("paper_auto_readiness"),
        }
        recommendation = "已有核心候选和买入意向；先检查模拟盘自动交易预检。"
        status = "paper_auto_readiness"
    elif active_positive_trials:
        first = active_positive_trials[0]
        if first.get("review_recorded") is False:
            action = {
                "type": "record_positive_trial_review",
                "label": "记录影子试运行复盘",
                "command": "atrade paper trial-review --min-age-days 0 --record --json",
                "reason": "只读复盘已发现表现为正的影子候选；先写入复盘证据，再人工复核，不自动晋级或下单。",
                "safe_to_auto_apply": True,
                **_action_contract(
                    "paper_trial_review_record",
                    writes_state=True,
                    risk_level="state_write",
                ),
            }
            recommendation = (
                f"有 {len(active_positive_trials)} 只仍在候选池内的影子试运行表现为正；"
                "先记录复盘证据，再人工复核，不自动晋级或下单。"
            )
        else:
            action = {
                "type": "review_positive_trial",
                "label": "复核表现为正的影子候选",
                "command": first["review_command"],
                "reason": "影子试运行表现为正，只能进入人工复核，不能自动晋级或下单。",
                "safe_to_auto_apply": True,
                **_action_contract("stock_analyze"),
            }
            recommendation = f"有 {len(active_positive_trials)} 只仍在候选池内的影子试运行表现为正；先人工复核，不自动晋级或下单。"
        status = "review_positive_trial"
    elif _has_observable_candidates(candidate_pool):
        action = {
            "type": "paper_trial_plan",
            "label": "生成模拟盘试运行计划",
            "command": "atrade paper trial-plan --json",
            "reason": "已有观察候选或强势观察候选，先生成只读影子试运行计划，不自动下单。",
            "safe_to_auto_apply": True,
            **_action_contract("paper_trial_plan"),
        }
        recommendation = "已有观察候选；生成模拟盘试运行计划，不主动降低买入门槛。"
        status = "wait"
    elif stale_pending:
        action = {
            "type": "expire_stale_manual_confirmation",
            "label": "复核过期人工确认",
            "command": "atrade manual-trades list --status stale --json",
            "reason": "旧买入意向已过期或错过买入窗口，只能复核或显式过期处理。",
            "safe_to_auto_apply": False,
            **_action_contract("manual_trades_stale"),
        }
        recommendation = f"有 {len(stale_pending)} 条买入意向已过期或错过买入窗口；先复核，不压住新候选观察。"
        status = "review_stale_manual_confirmation"
    elif _is_no_qualified_candidate_state(candidate_pool, data_source_diagnosis, latest_score):
        action = {
            "type": "observe_no_qualified_candidates",
            "label": "暂无合格候选",
            "command": "atrade screener explain --json",
            "reason": "核心数据源可用，但候选池为空，应视为筛选后暂无合格候选，不是行情没数据。",
            "safe_to_auto_apply": True,
            **_action_contract("screener_explain"),
        }
        recommendation = "核心数据源可用，候选池为空；继续观察，不降低买入线。"
        status = "wait_no_qualified_candidates"
    elif latest_score:
        action = {
            "type": "wait_or_review_candidates",
            "label": "等待或复核候选",
            "command": "atrade screener explain --json",
            "reason": "已有评分证据但没有待确认买入，适合复核候选漏斗或继续等待。",
            "safe_to_auto_apply": True,
            **_action_contract("screener_explain"),
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
            **_action_contract("screener_refresh", writes_state=True, risk_level="state_write"),
        }
        recommendation = "先刷新证据，不凭空给交易建议。"
        status = "needs_evidence"

    approval_gate = _opportunity_approval_gate(status, schedule_diagnosis)
    next_window_plan = _opportunity_next_window_plan(
        status=status,
        buy_signal_intents=buy_intents_for_auto_readiness,
        schedule_diagnosis=schedule_diagnosis,
        approval_gate=approval_gate,
    )

    return {
        "command": "suggest",
        "status": status,
        "summary": recommendation,
        "recommendation": recommendation,
        "execution_allowed": False,
        "next_action": action,
        "candidate_summary": candidate_summary,
        "current_entry_signals": current_entry_signals,
        "recent_unusable_buy_signal": recent_unusable_buy_signal,
        "approval_gate": approval_gate,
        "next_window_plan": next_window_plan,
        "digest": digest,
        "data_source_blockers": data_source_blockers,
        "positive_trial_candidates": positive_trials,
        "active_positive_trial_candidates": active_positive_trials,
        "inactive_positive_trial_candidates": inactive_positive_trials,
        "evidence_actions": evidence_actions,
        "diagnostics": {
            "data_sources": data_source_diagnosis,
            "candidate_pool": candidate_pool,
            "profile_activation": profile_activation,
            "schedule": schedule_diagnosis,
        },
        "guardrails": {
            "manual_confirmation_required": True,
            "no_broker_api": True,
            "auto_threshold_change_allowed": False,
        },
    }


def build_opportunity_card(conn: Any, *, limit: int = 5) -> dict[str, Any]:
    """生成主动推送用的今日机会卡，不执行交易。"""
    suggestion = build_suggestion(conn)
    digest = suggestion.get("digest", {}) or {}
    candidate_pool = (suggestion.get("diagnostics", {}) or {}).get("candidate_pool", {}) or {}
    data_sources = (suggestion.get("diagnostics", {}) or {}).get("data_sources", {}) or {}
    profile_activation = (suggestion.get("diagnostics", {}) or {}).get("profile_activation", {}) or {}
    schedule_diagnosis = (suggestion.get("diagnostics", {}) or {}).get("schedule", {}) or {}
    buy_intents = list(digest.get("pending_manual_trade_items", []) or [])[:limit]
    stale_buy_intents = list(digest.get("stale_manual_trade_items", []) or [])[:limit]
    all_candidate_items = _candidate_pool_items(conn, limit=1000)
    core_candidates = [item for item in all_candidate_items if item.get("pool_tier") == "core"][:limit]
    watch_only_candidates = [item for item in all_candidate_items if item.get("pool_tier") == "watch"][:limit]
    candidates = [item for item in all_candidate_items if item.get("pool_tier") != "radar"][:limit]
    radar_candidates = [item for item in all_candidate_items if item.get("pool_tier") == "radar"][:limit]
    current_entry_signals = [
        _compact_candidate_for_summary(item)
        for item in all_candidate_items
        if _candidate_has_entry_signal(item)
    ]
    positive_trials = list(suggestion.get("positive_trial_candidates", []) or [])[:limit]
    active_positive_trials = list(suggestion.get("active_positive_trial_candidates", []) or [])[:limit]
    inactive_positive_trials = list(suggestion.get("inactive_positive_trial_candidates", []) or [])[:limit]
    evidence_actions = list(suggestion.get("evidence_actions", []) or [])
    recent_unusable_buy_signal = suggestion.get("recent_unusable_buy_signal", {}) or {}
    status = str(suggestion.get("status", "unknown"))
    candidate_summary = _opportunity_candidate_summary(candidate_pool, all_candidate_items)
    blockers = _opportunity_blockers(
        suggestion,
        candidate_pool,
        data_sources,
        profile_activation,
        schedule_diagnosis,
    )
    approval_gate = _opportunity_approval_gate(status, schedule_diagnosis)
    after_approval_preview = _opportunity_after_approval_preview(conn, approval_gate)
    next_window_plan = _opportunity_next_window_plan(
        status=status,
        buy_signal_intents=[*buy_intents, *stale_buy_intents],
        schedule_diagnosis=schedule_diagnosis,
        approval_gate=approval_gate,
    )

    counts = {
        "buy_intents": len(buy_intents),
        "stale_buy_intents": len(stale_buy_intents),
        "watch_candidates": int(candidate_pool.get("watch_count", 0) or 0),
        "core_candidates": int(candidate_pool.get("core_count", 0) or 0),
        "radar_candidates": int(candidate_pool.get("radar_count", 0) or 0),
        "positive_trial_candidates": len(positive_trials),
        "active_positive_trial_candidates": len(active_positive_trials),
        "inactive_positive_trial_candidates": len(inactive_positive_trials),
        "recent_unusable_buy_signals": int(recent_unusable_buy_signal.get("count") or 0),
        "all_candidates": int(candidate_pool.get("total", 0) or 0),
        "positions": int((digest.get("positions", {}) or {}).get("count", 0) or 0),
        "failed_runs": len(digest.get("failed_runs", []) or []),
    }
    summary = _opportunity_summary(
        status,
        counts,
        recent_unusable_buy_signal=recent_unusable_buy_signal,
    )

    return {
        "command": "opportunity",
        "status": status,
        "date": local_today_str(),
        "summary": summary,
        "decision_brief": _opportunity_decision_brief(status, counts),
        "execution_allowed": False,
        "manual_confirmation_required": True,
        "counts": counts,
        "candidate_summary": candidate_summary,
        "current_entry_signals": current_entry_signals,
        "buy_intents": buy_intents,
        "stale_buy_intents": stale_buy_intents,
        "core_candidates": core_candidates,
        "watch_candidates": candidates,
        "watch_only_candidates": watch_only_candidates,
        "radar_candidates": radar_candidates,
        "positive_trial_candidates": positive_trials,
        "active_positive_trial_candidates": active_positive_trials,
        "inactive_positive_trial_candidates": inactive_positive_trials,
        "recent_unusable_buy_signal": recent_unusable_buy_signal,
        "evidence_actions": evidence_actions,
        "blockers": blockers,
        "approval_gate": approval_gate,
        "after_approval_preview": after_approval_preview,
        "next_window_plan": next_window_plan,
        "next_action": suggestion.get("next_action", {}),
        "suggestion": {
            "status": suggestion.get("status", ""),
            "recommendation": suggestion.get("recommendation", ""),
            "data_source_blockers": suggestion.get("data_source_blockers", []),
        },
        "diagnostics": {
            "candidate_pool": candidate_pool,
            "data_sources": {
                "status": data_sources.get("status", "unknown"),
                "findings": data_sources.get("findings", []) or [],
                "recommendations": data_sources.get("recommendations", []) or [],
            },
            "profile_activation": profile_activation,
            "schedule": {
                "status": schedule_diagnosis.get("status", "unknown"),
                "summary": schedule_diagnosis.get("summary", ""),
                "runtime_profile": schedule_diagnosis.get("runtime_profile", {}) or {},
                "next_action": schedule_diagnosis.get("next_action", {}) or {},
            },
        },
        "guardrails": {
            "manual_confirmation_required": True,
            "no_broker_api": True,
            "auto_threshold_change_allowed": False,
        },
    }


def build_opportunity_watch(
    conn: Any,
    *,
    state_file: Path | None = None,
    limit: int = 5,
    update_state: bool = True,
    reset_state: bool = False,
) -> dict[str, Any]:
    """检测今日机会是否出现新增候选；只提醒，不执行交易。"""
    resolved_state_file = _resolve_opportunity_watch_state_file(state_file)
    opportunity = build_opportunity_card(conn, limit=limit)
    all_candidates = _candidate_pool_items(conn, limit=1000)
    snapshot = _opportunity_watch_snapshot(opportunity, all_candidates)
    previous_snapshot = None if reset_state else _read_opportunity_watch_snapshot(resolved_state_file, snapshot["date"])
    payload = _opportunity_watch_payload(
        opportunity=opportunity,
        snapshot=snapshot,
        previous_snapshot=previous_snapshot,
        state_file=resolved_state_file,
        limit=limit,
    )
    if update_state:
        write_opportunity_watch_state(payload, resolved_state_file)
        payload["state_updated"] = True
    else:
        payload["state_updated"] = False
    return payload


def write_opportunity_watch_state(payload: dict[str, Any], state_file: Path | None = None) -> None:
    """写入机会变化监控基线，用于下次去重。"""
    snapshot = payload.get("snapshot", {}) or {}
    if not snapshot:
        return
    resolved_state_file = _resolve_opportunity_watch_state_file(state_file)
    resolved_state_file.parent.mkdir(parents=True, exist_ok=True)
    resolved_state_file.write_text(
        json.dumps({"snapshot": snapshot}, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


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
            **_explain_next_action_contract(next_command),
        },
        "execution_allowed": False,
    }


def _explain_next_action_contract(command: str) -> dict[str, Any]:
    if command == "atrade manual-trades list --json":
        return _action_contract("manual_trades_list")
    return _action_contract("stock_analyze")


def _candidate_pool_items(conn: Any, *, limit: int | None) -> list[dict[str, Any]]:
    sql = """SELECT code, pool_tier, name, score, added_at, last_scored_at, streak_days, note
             FROM projection_candidate_pool
             ORDER BY
               CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
               score DESC"""
    params: tuple[Any, ...] = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (limit,)
    rows = conn.execute(sql, params).fetchall()
    result = [dict(row) for row in rows]
    enrich_candidate_rows_with_latest_scores(conn, result)
    return [_candidate_payload(row) for row in result]


def _positive_trial_candidates(conn: Any, *, limit: int = 5) -> list[dict[str, Any]]:
    today = local_today_str()
    rows = conn.execute(
        """SELECT event_id, occurred_at, payload_json
           FROM event_log
           WHERE event_type = 'paper.trial.reviewed'
           ORDER BY occurred_at DESC, stream_version DESC
           LIMIT 200"""
    ).fetchall()
    by_code: dict[str, dict[str, Any]] = {}
    for row in rows:
        payload = _json_payload(row["payload_json"])
        code = str(payload.get("code") or "")
        if not code or code in by_code:
            continue
        if str(payload.get("review_date") or "") != today:
            continue
        if payload.get("review_status") != "positive":
            continue
        if payload.get("price_anomaly"):
            continue
        original_tier = str(payload.get("pool_tier") or "")
        current_state = _current_candidate_state_for_positive_trial(
            conn,
            code,
            original_tier=original_tier,
            recorded_current_score=payload.get("current_score"),
            fallback_current_pool_tier=payload.get("current_pool_tier"),
            fallback_current_pool_tier_label=payload.get("current_pool_tier_label"),
            fallback_candidate_state_changed=payload.get("candidate_state_changed"),
            fallback_candidate_state_change_label=str(payload.get("candidate_state_change_label") or ""),
            fallback_current_entry_signal=payload.get("current_entry_signal"),
            fallback_current_primary_strategy_route=payload.get("current_primary_strategy_route"),
            fallback_current_primary_strategy_route_label=payload.get("current_primary_strategy_route_label"),
            fallback_current_strategy_routes=payload.get("current_strategy_routes"),
            fallback_current_technical_detail=payload.get("current_technical_detail"),
            fallback_current_data_quality=payload.get("current_data_quality"),
        )
        by_code[code] = {
            "code": code,
            "name": payload.get("name") or code,
            "pool_tier": original_tier,
            "pool_tier_label": _pool_tier_label(original_tier),
            "trial_date": payload.get("trial_date"),
            "review_date": payload.get("review_date"),
            "review_status": payload.get("review_status"),
            "review_status_label": payload.get("review_status_label") or "表现为正",
            "return_pct": payload.get("return_pct"),
            "trial_start_price": payload.get("trial_start_price"),
            "current_price": payload.get("current_price"),
            **current_state,
            "paper_order_submitted": bool(payload.get("paper_order_submitted")),
            "source_event_id": row["event_id"],
            "reviewed_at": row["occurred_at"],
            "review_recorded": True,
            "review_source": "paper.trial.reviewed",
            "review_command": f"atrade stock analyze {code} --json",
        }
    return sorted(
        by_code.values(),
        key=_positive_trial_priority,
    )[:limit]


def _positive_trial_evidence_actions(positive_trials: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unrecorded = [
        item
        for item in positive_trials
        if item.get("active_candidate") and item.get("review_recorded") is False
    ]
    if not unrecorded:
        return []

    count = len(unrecorded)
    return [
        {
            "type": "record_positive_trial_review",
            "label": "记录影子试运行复盘",
            "command": "atrade paper trial-review --min-age-days 0 --record --json",
            "reason": (
                f"有 {count} 只影子试运行表现为正但尚未记录复盘；"
                "可先写入影子复盘证据，不提交模拟盘订单。"
            ),
            "safe_to_auto_apply": True,
            **_action_contract(
                "paper_trial_review_record",
                writes_state=True,
                risk_level="state_write",
            ),
        }
    ]


def _current_candidate_state_for_positive_trial(
    conn: Any,
    code: str,
    *,
    original_tier: str,
    recorded_current_score: Any,
    fallback_current_pool_tier: Any,
    fallback_current_pool_tier_label: Any,
    fallback_candidate_state_changed: Any,
    fallback_candidate_state_change_label: str,
    fallback_current_entry_signal: Any = None,
    fallback_current_primary_strategy_route: Any = None,
    fallback_current_primary_strategy_route_label: Any = None,
    fallback_current_strategy_routes: Any = None,
    fallback_current_technical_detail: Any = "",
    fallback_current_data_quality: Any = "",
) -> dict[str, Any]:
    row = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                  streak_days, note
           FROM projection_candidate_pool
           WHERE code = ?
           LIMIT 1""",
        (code,),
    ).fetchone()
    if row:
        current = dict(row)
        enrich_candidate_rows_with_latest_scores(conn, [current])
        current_pool_tier = str(current.get("pool_tier") or "")
        current_score = current.get("score")
        state_changed = current_pool_tier != original_tier
        state_change_label = (
            f"{_pool_tier_label(original_tier)} -> {_pool_tier_label(current_pool_tier)}"
            if state_changed
            else ""
        )
        if not state_changed and _score_changed(recorded_current_score, current_score):
            state_changed = True
            state_change_label = f"评分 {_score_text(recorded_current_score)} -> {_score_text(current_score)}"
        return {
            "current_pool_tier": current_pool_tier,
            "current_pool_tier_label": _pool_tier_label(current_pool_tier),
            "current_score": current_score,
            "active_candidate": bool(current_pool_tier),
            "candidate_state_changed": state_changed,
            "candidate_state_change_label": state_change_label,
            "current_entry_signal": current.get("entry_signal"),
            "current_primary_strategy_route": current.get("primary_strategy_route"),
            "current_primary_strategy_route_label": current.get("primary_strategy_route_label"),
            "current_strategy_routes": current.get("strategy_routes") or [],
            "current_technical_detail": current.get("technical_detail") or "",
            "current_data_quality": current.get("data_quality") or "",
        }

    previous_tier = original_tier or str(fallback_current_pool_tier or "")
    current_pool_tier_label = "已移出候选池"
    state_changed = True if previous_tier else bool(fallback_candidate_state_changed)
    state_change_label = (
        f"{_pool_tier_label(previous_tier)} -> 已移出候选池"
        if previous_tier
        else fallback_candidate_state_change_label
    )
    return {
        "current_pool_tier": None,
        "current_pool_tier_label": current_pool_tier_label,
        "current_score": None,
        "active_candidate": False,
        "candidate_state_changed": bool(state_changed),
        "candidate_state_change_label": state_change_label,
        "current_entry_signal": fallback_current_entry_signal,
        "current_primary_strategy_route": fallback_current_primary_strategy_route,
        "current_primary_strategy_route_label": fallback_current_primary_strategy_route_label,
        "current_strategy_routes": fallback_current_strategy_routes or [],
        "current_technical_detail": fallback_current_technical_detail or "",
        "current_data_quality": fallback_current_data_quality or "",
    }


def _score_changed(left: Any, right: Any) -> bool:
    try:
        return round(float(left), 2) != round(float(right), 2)
    except (TypeError, ValueError):
        return left != right


def _score_text(value: Any) -> str:
    try:
        return f"{float(value):g}"
    except (TypeError, ValueError):
        return str(value)


def _positive_trial_preview_candidates(conn: Any, *, limit: int = 5) -> list[dict[str, Any]]:
    from astock_trading.platform.paper_trial import build_paper_trial_review

    review = build_paper_trial_review(
        conn,
        EventStore(conn),
        min_age_days=0,
        record=False,
        limit=200,
    )
    by_code: dict[str, dict[str, Any]] = {}
    for item in review.get("items", []) or []:
        code = str(item.get("code") or "")
        if not code or code in by_code:
            continue
        if item.get("review_status") != "positive":
            continue
        if item.get("price_anomaly"):
            continue
        current_pool_tier = item.get("current_pool_tier")
        current_pool_tier_label = item.get("current_pool_tier_label")
        state_changed = item.get("candidate_state_changed")
        state_change_label = str(item.get("candidate_state_change_label") or "")
        original_tier = str(item.get("pool_tier") or "")
        if current_pool_tier in (None, "") and original_tier:
            current_pool_tier_label = current_pool_tier_label or "已移出候选池"
            state_changed = True if state_changed is None else state_changed
            state_change_label = state_change_label or f"{_pool_tier_label(original_tier)} -> 已移出候选池"
        active_candidate = current_pool_tier not in (None, "")
        by_code[code] = {
            "code": code,
            "name": item.get("name") or code,
            "pool_tier": original_tier,
            "pool_tier_label": _pool_tier_label(original_tier),
            "trial_date": item.get("trial_date"),
            "review_date": item.get("review_date"),
            "review_status": item.get("review_status"),
            "review_status_label": item.get("review_status_label") or "表现为正",
            "return_pct": item.get("return_pct"),
            "trial_start_price": item.get("trial_start_price"),
            "current_price": item.get("current_price"),
            "current_pool_tier": current_pool_tier,
            "current_pool_tier_label": current_pool_tier_label,
            "current_score": item.get("current_score"),
            "current_entry_signal": item.get("current_entry_signal"),
            "current_primary_strategy_route": item.get("current_primary_strategy_route"),
            "current_primary_strategy_route_label": item.get("current_primary_strategy_route_label"),
            "current_strategy_routes": item.get("current_strategy_routes") or [],
            "current_technical_detail": item.get("current_technical_detail") or "",
            "current_data_quality": item.get("current_data_quality") or "",
            "active_candidate": active_candidate,
            "candidate_state_changed": bool(state_changed),
            "candidate_state_change_label": state_change_label,
            "paper_order_submitted": bool(item.get("paper_order_submitted")),
            "source_event_id": None,
            "reviewed_at": None,
            "review_recorded": False,
            "review_source": "paper.trial-review.preview",
            "review_command": f"atrade stock analyze {code} --json",
        }
    return sorted(
        by_code.values(),
        key=_positive_trial_priority,
    )[:limit]


def _merge_positive_trial_candidates(
    recorded: list[dict[str, Any]],
    preview: list[dict[str, Any]],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    by_code: dict[str, dict[str, Any]] = {}
    for item in [*recorded, *preview]:
        code = str(item.get("code") or "")
        if not code:
            continue
        existing = by_code.get(code)
        if existing is None or (item.get("review_recorded") is True and existing.get("review_recorded") is False):
            by_code[code] = item
    return sorted(
        by_code.values(),
        key=_positive_trial_priority,
    )[:limit]


def _positive_trial_priority(item: dict[str, Any]) -> tuple[int, int, float, str]:
    tier_rank = {
        "core": 0,
        "watch": 1,
        "radar": 2,
    }.get(str(item.get("current_pool_tier") or ""), 3)
    entry_rank = 0 if _truthy_value(item.get("current_entry_signal")) else 1
    try:
        return_pct = float(item.get("return_pct") or 0)
    except (TypeError, ValueError):
        return_pct = 0.0
    return (tier_rank, entry_rank, -return_pct, str(item.get("code") or ""))


def _profile_activation_diagnostics(conn: Any) -> dict[str, Any]:
    latest_request = latest_strategy_profile_activation_request(conn)
    if not latest_request:
        return {
            "status": "missing",
            "latest_request": {},
            "message": "暂无已记录的执行 profile 激活请求。",
        }
    if latest_request.get("status") == "requires_manual_confirmation":
        status = "pending_manual_confirmation"
        message = "已有待人工确认的执行 profile 激活计划。"
    else:
        status = "recorded"
        message = "已有已记录的执行 profile 激活计划。"
    return {
        "status": status,
        "latest_request": latest_request,
        "message": message,
    }


def _resolve_opportunity_watch_state_file(state_file: Path | None) -> Path:
    if state_file is not None:
        return state_file.expanduser()
    return default_state_dir() / "opportunity_watch" / "state.json"


def _read_opportunity_watch_snapshot(state_file: Path, _today: str) -> dict[str, Any] | None:
    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    snapshot = raw.get("snapshot", {}) if isinstance(raw, dict) else {}
    if not isinstance(snapshot, dict):
        return None
    return snapshot


def _opportunity_watch_snapshot(
    opportunity: dict[str, Any],
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = opportunity.get("counts", {}) or {}
    candidate_keys = [_candidate_key(item) for item in candidates]
    watch_keys = [_candidate_key(item) for item in candidates if item.get("pool_tier") == "watch"]
    core_keys = [_candidate_key(item) for item in candidates if item.get("pool_tier") == "core"]
    radar_keys = [_candidate_key(item) for item in candidates if item.get("pool_tier") == "radar"]
    attention = _opportunity_watch_attention(opportunity)
    return {
        "date": opportunity.get("date", local_today_str()),
        "captured_at": local_now_iso(),
        "counts": {
            "buy_intents": int(counts.get("buy_intents", 0) or 0),
            "core_candidates": int(counts.get("core_candidates", 0) or 0),
            "watch_candidates": int(counts.get("watch_candidates", 0) or 0),
            "radar_candidates": int(counts.get("radar_candidates", 0) or 0),
            "all_candidates": int(counts.get("all_candidates", 0) or 0),
        },
        "candidate_keys": sorted(candidate_keys),
        "watch_keys": sorted(watch_keys),
        "core_keys": sorted(core_keys),
        "radar_keys": sorted(radar_keys),
        "candidates": candidates,
        "attention_key": attention["key"],
        "attention": attention,
    }


def _opportunity_watch_attention(opportunity: dict[str, Any]) -> dict[str, Any]:
    status = str(opportunity.get("status") or "")
    next_action = opportunity.get("next_action", {}) or {}
    approval_gate = opportunity.get("approval_gate", {}) or {}
    evidence_actions = [
        action for action in opportunity.get("evidence_actions", []) or []
        if isinstance(action, dict)
    ]
    command = str(next_action.get("command") or approval_gate.get("review_command") or "")
    action_type = str(next_action.get("type") or status)
    approval_required = approval_gate.get("required") is True
    evidence_key = ";".join(
        "|".join([
            str(action.get("type") or ""),
            str(action.get("command") or ""),
            str(bool(action.get("writes_state", False))).lower(),
        ])
        for action in evidence_actions
    )
    tracks_action = approval_required or bool(evidence_key) or status in OPPORTUNITY_WATCH_ACTION_STATUSES
    key = ""
    if tracks_action:
        key = "|".join([
            status,
            action_type,
            command,
            str(approval_required).lower(),
            str(approval_gate.get("apply_command") or ""),
            evidence_key,
        ])
    return {
        "key": key,
        "status": status,
        "type": action_type,
        "label": str(next_action.get("label") or ""),
        "command": command,
        "reason": str(next_action.get("reason") or opportunity.get("decision_brief") or ""),
        "safe_to_auto_apply": bool(next_action.get("safe_to_auto_apply", False)),
        "approval_required": approval_required,
        "evidence_action_key": evidence_key,
    }


def _opportunity_watch_payload(
    *,
    opportunity: dict[str, Any],
    snapshot: dict[str, Any],
    previous_snapshot: dict[str, Any] | None,
    state_file: Path,
    limit: int,
) -> dict[str, Any]:
    current_counts = snapshot["counts"]
    previous_counts = _snapshot_counts(previous_snapshot)
    candidates_by_key = {_candidate_key(item): item for item in snapshot.get("candidates", []) or []}
    current_attention_key = str(snapshot.get("attention_key") or "")
    previous_attention = (previous_snapshot or {}).get("attention", {}) or {}
    previous_attention_key = str((previous_snapshot or {}).get("attention_key") or previous_attention.get("key") or "")
    change_types: list[str] = []
    change_labels: list[str] = []
    new_keys: list[str] = []

    if previous_snapshot:
        if previous_counts["all_candidates"] == 0 and current_counts["all_candidates"] > 0:
            change_types.append("candidate_pool_activated")
            change_labels.append("候选池从空变为非空")
        new_watch_keys = sorted(set(snapshot.get("watch_keys", [])) - set(previous_snapshot.get("watch_keys", [])))
        new_core_keys = sorted(set(snapshot.get("core_keys", [])) - set(previous_snapshot.get("core_keys", [])))
        new_radar_keys = sorted(set(snapshot.get("radar_keys", [])) - set(previous_snapshot.get("radar_keys", [])))
        if new_watch_keys:
            change_types.append("new_watch_candidates")
            change_labels.append("新观察候选")
            new_keys.extend(new_watch_keys)
        if new_core_keys:
            change_types.append("new_core_candidates")
            change_labels.append("新核心候选")
            new_keys.extend(new_core_keys)
        if new_radar_keys:
            change_types.append("new_radar_candidates")
            change_labels.append("新强势观察候选")
            new_keys.extend(new_radar_keys)
        if current_attention_key and current_attention_key != previous_attention_key:
            change_types.append("operator_action_required")
            change_labels.append("当前动作需要处理")
        status = "changed" if change_types else "unchanged"
    else:
        status = "baseline_recorded"

    new_candidates = [
        candidates_by_key[key] for key in _sorted_candidate_keys(new_keys, candidates_by_key)
    ][:limit]
    should_notify = status == "changed"
    summary = _opportunity_watch_summary(
        status=status,
        current_counts=current_counts,
        previous_counts=previous_counts,
        change_types=change_types,
        new_candidates=new_candidates,
    )
    next_action = _opportunity_watch_next_action(opportunity, change_types)
    candidate_summary = opportunity.get("candidate_summary", {}) or _opportunity_candidate_summary(
        {
            "total": current_counts["all_candidates"],
            "core_count": current_counts["core_candidates"],
            "watch_count": current_counts["watch_candidates"],
            "radar_count": current_counts["radar_candidates"],
        },
        snapshot.get("candidates", []) or [],
    )
    return {
        "command": "opportunity-watch",
        "status": status,
        "date": snapshot["date"],
        "summary": summary,
        "should_notify": should_notify,
        "change_types": change_types,
        "change_labels": change_labels,
        "previous_counts": previous_counts,
        "current_counts": current_counts,
        "counts": current_counts,
        "candidate_summary": candidate_summary,
        "current_action": next_action,
        "new_candidates": new_candidates,
        "opportunity": {
            "status": opportunity.get("status", ""),
            "summary": opportunity.get("summary", ""),
            "decision_brief": opportunity.get("decision_brief", ""),
            "counts": opportunity.get("counts", {}) or {},
            "blockers": opportunity.get("blockers", []) or [],
            "next_action": opportunity.get("next_action", {}) or {},
            "evidence_actions": opportunity.get("evidence_actions", []) or [],
            "recent_unusable_buy_signal": opportunity.get("recent_unusable_buy_signal", {}) or {},
            "approval_gate": opportunity.get("approval_gate", {}) or {},
            "after_approval_preview": opportunity.get("after_approval_preview", {}) or {},
            "next_window_plan": opportunity.get("next_window_plan", {}) or {},
        },
        "next_action": next_action,
        "execution_allowed": False,
        "manual_confirmation_required": True,
        "state_file": str(state_file),
        "snapshot": snapshot,
        "guardrails": {
            "manual_confirmation_required": True,
            "no_broker_api": True,
            "auto_threshold_change_allowed": False,
        },
    }


def _snapshot_counts(snapshot: dict[str, Any] | None) -> dict[str, int]:
    counts = (snapshot or {}).get("counts", {}) or {}
    return {
        "buy_intents": int(counts.get("buy_intents", 0) or 0),
        "core_candidates": int(counts.get("core_candidates", 0) or 0),
        "watch_candidates": int(counts.get("watch_candidates", 0) or 0),
        "radar_candidates": int(counts.get("radar_candidates", 0) or 0),
        "all_candidates": int(counts.get("all_candidates", 0) or 0),
    }


def _candidate_key(item: dict[str, Any]) -> str:
    return f"{item.get('pool_tier', '')}:{item.get('code', '')}"


def _sorted_candidate_keys(keys: list[str], candidates_by_key: dict[str, dict[str, Any]]) -> list[str]:
    unique = list(dict.fromkeys(keys))
    return sorted(
        unique,
        key=lambda key: (
            {
                "core": 0,
                "watch": 1,
                "radar": 2,
            }.get(candidates_by_key.get(key, {}).get("pool_tier"), 3),
            -float(candidates_by_key.get(key, {}).get("score", 0) or 0),
            key,
        ),
    )


def _opportunity_watch_summary(
    *,
    status: str,
    current_counts: dict[str, int],
    previous_counts: dict[str, int],
    change_types: list[str],
    new_candidates: list[dict[str, Any]],
) -> str:
    if status == "baseline_recorded":
        if current_counts["all_candidates"]:
            return (
                f"已记录今日机会监控基线，当前候选池 {current_counts['all_candidates']} 只；"
                "后续新增观察/核心候选再主动提醒。"
            )
        return "已记录今日机会监控基线，候选池仍为空；后续从 0 变为非空时主动提醒。"
    if status == "unchanged":
        return "候选池无新增观察/核心候选，保持静默。"

    parts: list[str] = []
    if "candidate_pool_activated" in change_types:
        parts.append(
            f"候选池从 {previous_counts['all_candidates']} 变为 {current_counts['all_candidates']}"
        )
    watch_count = sum(1 for item in new_candidates if item.get("pool_tier") == "watch")
    core_count = sum(1 for item in new_candidates if item.get("pool_tier") == "core")
    radar_count = sum(1 for item in new_candidates if item.get("pool_tier") == "radar")
    if watch_count:
        parts.append(f"出现 {watch_count} 只新观察候选")
    if core_count:
        parts.append(f"出现 {core_count} 只新核心候选")
    if radar_count:
        parts.append(f"出现 {radar_count} 只新强势观察候选")
    if "operator_action_required" in change_types:
        parts.append("当前机会状态需要处理")
    return "，".join(parts or ["候选池发生变化"]) + "，已触发主动提醒。"


def _opportunity_watch_next_action(
    opportunity: dict[str, Any],
    change_types: list[str],
) -> dict[str, Any]:
    if "operator_action_required" in change_types:
        action = opportunity.get("next_action", {}) or {}
        if action.get("command"):
            result = {
                "label": str(action.get("label") or "复核当前机会状态"),
                "command": str(action.get("command")),
                "reason": str(action.get("reason") or opportunity.get("decision_brief") or "机会状态需要处理。"),
                "safe_to_auto_apply": bool(action.get("safe_to_auto_apply", False)),
            }
            for key in (
                "writes_state",
                "writes_environment",
                "writes_order",
                "requires_user_approval",
                "risk_level",
                "command_contract_id",
            ):
                if key in action:
                    result[key] = action[key]
            return result
    return {
        "label": "查看今日机会卡",
        "command": "atrade opportunity --json",
        "reason": "只读复核，不自动交易。",
        "safe_to_auto_apply": True,
        **_action_contract("opportunity"),
    }


def _candidate_payload(row: dict[str, Any]) -> dict[str, Any]:
    note = str(row.get("note", "") or "")
    tier = str(row.get("pool_tier", "") or "")
    return {
        "code": row.get("code", ""),
        "name": row.get("name", ""),
        "pool_tier": tier,
        "pool_tier_label": _pool_tier_label(tier),
        "score": row.get("score", 0) or 0,
        "added_at": row.get("added_at", ""),
        "last_scored_at": row.get("last_scored_at", ""),
        "streak_days": row.get("streak_days", 0) or 0,
        "note": note,
        "note_label": _candidate_note_label(note),
        "entry_signal": row.get("entry_signal"),
        "primary_strategy_route": row.get("primary_strategy_route"),
        "primary_strategy_route_label": row.get("primary_strategy_route_label"),
        "strategy_routes": row.get("strategy_routes") or [],
        "technical_detail": row.get("technical_detail", ""),
        "data_quality": row.get("data_quality", ""),
    }


def _opportunity_candidate_summary(candidate_pool: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    core_count = int(candidate_pool.get("core_count", 0) or 0)
    watch_count = int(candidate_pool.get("watch_count", 0) or 0)
    radar_count = int(candidate_pool.get("radar_count", 0) or 0)
    total = int(candidate_pool.get("total", 0) or len(candidates))
    entry_signal_count = sum(1 for item in candidates if _candidate_has_entry_signal(item))
    return {
        "total": total,
        "core_count": core_count,
        "watch_count": watch_count,
        "radar_count": radar_count,
        "entry_signal_count": entry_signal_count,
        "latest_scored_at": candidate_pool.get("latest_scored_at"),
        "summary": (
            f"候选池 {total} 只：核心 {core_count}、观察 {watch_count}、强势观察 {radar_count}；"
            f"当前入场信号 {entry_signal_count} 只。"
        ),
        "top_core_candidate": _first_candidate_for_tier(candidates, "core"),
        "top_watch_candidate": _first_candidate_for_tier(candidates, "watch"),
        "top_radar_candidate": _first_candidate_for_tier(candidates, "radar"),
    }


def _candidate_has_entry_signal(item: dict[str, Any]) -> bool:
    return _truthy_value(item.get("entry_signal"))


def _truthy_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "是", "有"}
    return bool(value)


def _first_candidate_for_tier(candidates: list[dict[str, Any]], tier: str) -> dict[str, Any]:
    for item in candidates:
        if str(item.get("pool_tier") or "") == tier:
            return _compact_candidate_for_summary(item)
    return {}


def _compact_candidate_for_summary(item: dict[str, Any]) -> dict[str, Any]:
    code = str(item.get("code") or "")
    return {
        "code": code,
        "name": item.get("name", ""),
        "pool_tier": item.get("pool_tier", ""),
        "pool_tier_label": item.get("pool_tier_label", ""),
        "score": item.get("score", 0) or 0,
        "entry_signal": item.get("entry_signal"),
        "primary_strategy_route_label": item.get("primary_strategy_route_label"),
        "technical_detail": item.get("technical_detail", ""),
        "review_command": f"atrade stock analyze {code} --json" if code else "",
    }


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _opportunity_summary(
    status: str,
    counts: dict[str, int],
    *,
    recent_unusable_buy_signal: dict[str, Any] | None = None,
) -> str:
    recent_unusable_text = _recent_unusable_buy_signal_text(recent_unusable_buy_signal).rstrip("。")
    if status == "review_positive_trial":
        count = counts.get("active_positive_trial_candidates", counts.get("positive_trial_candidates", 0))
        return f"有 {count} 只仍在候选池内的影子试运行表现为正；先人工复核，不自动晋级或下单。"
    if status == "review_stale_manual_confirmation":
        return f"有 {counts.get('stale_buy_intents', 0)} 条买入意向已过期或错过买入窗口；先复核，不压住新候选观察。"
    if status == "profile_review_required":
        if recent_unusable_text:
            return (
                f"核心候选 {counts.get('core_candidates', 0)} 只，"
                f"{recent_unusable_text}；模拟承接前先复核运行 profile。"
            )
        if counts.get("buy_intents", 0):
            return f"核心候选 {counts.get('core_candidates', 0)} 只，已有买入意向；模拟承接前先复核运行 profile。"
        if counts.get("stale_buy_intents", 0):
            return (
                f"核心候选 {counts.get('core_candidates', 0)} 只，"
                f"过期待复核买入意向 {counts.get('stale_buy_intents', 0)} 条；"
                "模拟承接前先复核运行 profile。"
            )
        return f"核心候选 {counts.get('core_candidates', 0)} 只，运行 profile 待复核；等待新鲜买入意向。"
    if status == "needs_health_check":
        if recent_unusable_text:
            return f"{recent_unusable_text}；同时先修运行/数据问题，暂停新增交易判断。"
        return "先修运行/数据问题，暂停新增交易判断。"
    if status == "wait_no_qualified_candidates":
        return "暂无合格候选；继续观察，不降低买入线。"
    if status == "review_buy_intent":
        return "有买入意向，先解释证据，再走人工确认。"
    if status == "paper_auto_readiness":
        return f"核心候选 {counts.get('core_candidates', 0)} 只，已有买入意向；先检查模拟盘自动交易预检。"
    if counts["buy_intents"]:
        return f"有 {counts['buy_intents']} 条买入意向等待人工确认；系统不会自动下单。"
    if counts.get("core_candidates", 0) or counts.get("watch_candidates", 0):
        return f"{_candidate_count_text(counts)}；等待入场信号，不自动买入。"
    if counts.get("radar_candidates", 0):
        return f"出现 {counts['radar_candidates']} 只强势观察候选；先跟踪，不自动买入。"
    if counts["all_candidates"]:
        return "当前没有买入意向，保留观察候选等待入场信号。"
    return "暂无买入意向；先刷新证据或继续只读观察。"


def _candidate_count_text(counts: dict[str, int]) -> str:
    parts: list[str] = []
    core_count = int(counts.get("core_candidates", 0) or 0)
    watch_count = int(counts.get("watch_candidates", 0) or 0)
    radar_count = int(counts.get("radar_candidates", 0) or 0)
    if core_count:
        parts.append(f"核心候选 {core_count} 只")
    if watch_count:
        parts.append(f"观察候选 {watch_count} 只")
    if radar_count:
        parts.append(f"强势观察 {radar_count} 只")
    return "，".join(parts or ["暂无合格候选"])


def _opportunity_decision_brief(status: str, counts: dict[str, int]) -> str:
    stale_suffix = f"，过期待复核 {counts.get('stale_buy_intents', 0)}" if counts.get("stale_buy_intents") else ""
    prefix = (
        f"买入意向 {counts['buy_intents']}，"
        f"核心候选 {counts['core_candidates']}，"
        f"观察候选 {counts['watch_candidates']}，"
        f"强势观察 {counts.get('radar_candidates', 0)}{stale_suffix}。"
    )
    if status == "wait_no_qualified_candidates":
        return prefix + "暂无合格候选，继续观察，不降低买入线。"
    if status == "needs_health_check":
        return prefix + "运行或数据质量优先于新增交易判断。"
    if status == "review_positive_trial":
        count = counts.get("active_positive_trial_candidates", counts.get("positive_trial_candidates", 0))
        return prefix + f"仍在候选池内的影子试运行表现为正 {count} 只，先人工复核。"
    if status == "review_stale_manual_confirmation":
        return prefix + "旧买入意向只做复核或过期处理，不阻断新候选观察。"
    if status == "profile_review_required":
        if counts.get("buy_intents", 0):
            return prefix + "已有买入意向证据，但运行 profile 未完成确认。"
        if counts.get("stale_buy_intents", 0):
            return prefix + "只有过期待复核买入意向；运行 profile 未完成确认，下个窗口需重新形成同日信号。"
        return prefix + "运行 profile 未完成确认；等待新鲜买入意向。"
    if status == "paper_auto_readiness":
        return prefix + "核心候选仍有买入意向证据，先检查模拟盘自动交易预检。"
    if counts["buy_intents"]:
        return prefix + "真实交易必须等待人工确认。"
    return prefix + "没有待确认买入，当前只读复核。"


def _opportunity_blockers(
    suggestion: dict[str, Any],
    candidate_pool: dict[str, Any],
    data_sources: dict[str, Any],
    profile_activation: dict[str, Any] | None = None,
    schedule_diagnosis: dict[str, Any] | None = None,
) -> list[str]:
    blockers: list[str] = []
    if suggestion.get("recommendation"):
        blockers.append(_display_text(str(suggestion["recommendation"])))
    latest_profile_request = (profile_activation or {}).get("latest_request", {}) or {}
    if latest_profile_request.get("status") == "requires_manual_confirmation":
        target_profile = latest_profile_request.get("target_profile") or "目标"
        blockers.append(f"已记录待人工确认的 {target_profile} profile 激活计划")
    runtime_profile = (schedule_diagnosis or {}).get("runtime_profile", {}) or {}
    if _runtime_profile_blocks_simulation(runtime_profile):
        effective_profile = runtime_profile.get("effective_profile") or "default"
        recommended_profile = runtime_profile.get("recommended_profile") or "trend_swing"
        blockers.append(f"运行环境仍会使用 {effective_profile}，需人工确认 {recommended_profile} profile")
    stale_count = int(((suggestion.get("digest", {}) or {}).get("stale_manual_trades", 0)) or 0)
    if stale_count:
        blockers.append(f"有 {stale_count} 条买入意向已过期或错过窗口，需复核或过期处理")
    inactive_positive_count = len(suggestion.get("inactive_positive_trial_candidates", []) or [])
    if inactive_positive_count:
        blockers.append(f"有 {inactive_positive_count} 只影子正收益已移出候选池，仅作复核证据")
    if int(candidate_pool.get("total", 0) or 0) == 0:
        blockers.append("候选池为空")
    if int(candidate_pool.get("core_count", 0) or 0) == 0:
        blockers.append("核心池为空")
    for item in suggestion.get("data_source_blockers", []) or []:
        reason = _display_text(
            str(item.get("description") or item.get("label") or item.get("reason") or "数据覆盖不足")
        )
        blockers.append(reason)
    return _dedupe(blockers)[:6]


def _runtime_profile_blocks_simulation(runtime_profile: dict[str, Any]) -> bool:
    return (
        runtime_profile.get("status") == "review_required"
        and runtime_profile.get("activation_request_status") == "recorded"
    )


def _display_text(text: str) -> str:
    replacements = {
        "candidate_pool_freshness": "候选池新鲜度",
        "core_pool": "核心池",
        "latest_screener_l1_coverage_degraded": "最近筛选逐票数据覆盖降级",
        "latest_screener_score_quality_degraded": "最近筛选评分数据质量降级",
        "unresolved_l1_provider_failures": "L1 数据源失败未补齐",
        "screener_refresh": "筛选刷新",
        "requires_entry_strategy_route": "缺少有效策略路线",
        "entry_signal": "入场信号",
        "BUY": "买入意向",
        "TRIAL_BUY": "试买意向",
        "WATCH": "观察",
        "CLEAR": "观望",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = value.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _pool_tier_label(tier: str) -> str:
    return {"core": "核心", "watch": "观察", "radar": "强势观察"}.get(tier, tier or "未分层")


def _candidate_note_label(note: str) -> str:
    if "requires_entry_strategy_route" in note:
        return "缺少有效策略路线"
    if "below_watch_retained" in note:
        return "低于观察线，保留跟踪"
    if note == "screener_refresh":
        return "筛选刷新入池"
    return _display_text(note) if note else "待复核"


def _is_no_qualified_candidate_state(
    candidate_pool: dict[str, Any],
    data_source_diagnosis: dict[str, Any],
    latest_score: dict[str, Any],
) -> bool:
    if int(candidate_pool.get("total", 0) or 0) != 0:
        return False
    health = data_source_diagnosis.get("health", {}) or {}
    if health.get("required_missing"):
        return False
    source_quality = data_source_diagnosis.get("latest_screener_source_quality", {}) or {}
    has_screener_evidence = source_quality.get("status") not in {"", "empty", None}
    return bool(latest_score or has_screener_evidence)


def _has_observable_candidates(candidate_pool: dict[str, Any]) -> bool:
    return int(candidate_pool.get("total", 0) or 0) > 0


def _core_buy_signal_needs_auto_readiness(
    conn: Any,
    buy_intents: list[dict[str, Any]],
) -> bool:
    for item in buy_intents:
        if str(item.get("side", "")).lower() != "buy":
            continue
        code = str(item.get("code") or "")
        if not code:
            continue
        row = conn.execute(
            "SELECT pool_tier FROM projection_candidate_pool WHERE code = ? LIMIT 1",
            (code,),
        ).fetchone()
        if row and str(row["pool_tier"] or "") == "core":
            return True
    return False


def _stale_core_buy_signal_needs_auto_readiness(
    conn: Any,
    stale_pending: list[dict[str, Any]],
) -> bool:
    return _core_buy_signal_needs_auto_readiness(conn, stale_pending)


def _opportunity_approval_gate(status: str, schedule_diagnosis: dict[str, Any]) -> dict[str, Any]:
    runtime_profile = schedule_diagnosis.get("runtime_profile", {}) or {}
    profile_gate_statuses = {
        "profile_review_required",
        "paper_auto_readiness",
        "review_positive_trial",
    }
    if status not in profile_gate_statuses or not _runtime_profile_blocks_simulation(runtime_profile):
        return {"required": False}
    target_profile = str(runtime_profile.get("recommended_profile") or "trend_swing")
    effective_profile = str(runtime_profile.get("effective_profile") or "default")
    return {
        "required": True,
        "type": "profile_activation_apply",
        "label": "人工确认写入运行 profile",
        "reason": (
            f"当前 {effective_profile} 混合配置阻断自动模拟；"
            f"需要人工批准后写入 ASTOCK_CONFIG_PROFILE={target_profile}。"
        ),
        "target_profile": target_profile,
        "review_command": f"atrade strategy profile-activation --target {target_profile} --json",
        "apply_command": (
            f"atrade strategy profile-activation --target {target_profile} --apply-env --yes --json"
        ),
        "verify_command": "atrade diagnose schedule --json",
        "safe_to_auto_apply": False,
        "modifies_environment_after_approval": True,
        "review_command_contract_id": "strategy_profile_activation_review",
        "review_command_contract": _command_contract("strategy_profile_activation_review"),
        "apply_command_contract_id": "strategy_profile_activation_apply",
        "apply_command_contract": _command_contract(
            "strategy_profile_activation_apply",
            writes_state=True,
            writes_environment=True,
            requires_user_approval=True,
            risk_level="environment_write",
            state_events=["strategy.profile_activation.applied"],
        ),
        "verify_command_contract_id": "diagnose_schedule",
        "verify_command_contract": _command_contract("diagnose_schedule"),
    }


def _opportunity_after_approval_preview(conn: Any, approval_gate: dict[str, Any]) -> dict[str, Any]:
    """机会卡的审批后只读预演；不查外部账户、不写环境、不提交委托。"""
    if approval_gate.get("required") is not True:
        return {"available": False}
    try:
        from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
        from astock_trading.platform.agent_diagnostics import _candidate_flow_after_approval_preview
        from astock_trading.platform.runs import RunJournal

        data, _errors = ConfigRegistry().load_and_validate()
        strategy_cfg = (data.get("strategy", {}) or {}) if isinstance(data, dict) else {}
        ctx = SimpleNamespace(
            conn=conn,
            cfg=strategy_cfg,
            event_store=EventStore(conn),
            run_journal=RunJournal(conn),
        )
        auto_readiness = build_auto_trade_readiness(ctx, include_account=False)
        return _candidate_flow_after_approval_preview(
            approval_gate=approval_gate,
            auto_readiness=auto_readiness,
        )
    except Exception as exc:
        return {
            "available": False,
            "status": "unavailable",
            "summary": f"审批后只读预演读取失败：{exc}",
            "recommended_command": "atrade diagnose flow --json",
            "safe_to_auto_apply": True,
            "writes_environment": False,
            "places_order": False,
        }


def _opportunity_next_window_plan(
    *,
    status: str,
    buy_signal_intents: list[dict[str, Any]],
    schedule_diagnosis: dict[str, Any],
    approval_gate: dict[str, Any],
) -> dict[str, Any]:
    plan_statuses = {
        "profile_review_required",
        "paper_auto_readiness",
        "review_positive_trial",
    }
    if status not in plan_statuses:
        return {"available": False}
    buy_window = _opportunity_buy_window()
    start_time = _parse_hhmm(buy_window.get("start", "09:45")) or time(9, 45)
    end_time = _parse_hhmm(buy_window.get("end", "14:30")) or time(14, 30)
    scheduled_steps = _opportunity_next_window_steps(schedule_diagnosis)
    approval_required = bool(approval_gate.get("required"))
    if not buy_signal_intents and not approval_required and not scheduled_steps:
        return {"available": False}
    next_window_date = _opportunity_next_window_date(scheduled_steps)
    window_start = datetime.combine(next_window_date, start_time, tzinfo=MARKET_TZ)
    window_end = datetime.combine(next_window_date, end_time, tzinfo=MARKET_TZ)
    current_signal = (
        _opportunity_current_signal(
            buy_signal_intents[0],
            next_window_date=next_window_date,
            end_time=end_time,
        )
        if buy_signal_intents
        else {}
    )
    carries_signal = bool(current_signal.get("carries_to_next_window"))
    if approval_required:
        plan_status = "requires_profile_approval_before_next_window"
        signal_text = (
            "当前买入意向不会跨日自动提交；"
            if buy_signal_intents
            else "当前没有可跨日自动承接的买入意向；"
        )
        summary = signal_text + "下个买入窗口前先人工确认 profile，再等待盘中重新形成同日买入意向。"
        next_action = {
            "type": "review_runtime_profile_activation",
            "label": "先复核运行 profile 激活",
            "command": approval_gate.get("review_command")
            or "atrade strategy profile-activation --target trend_swing --json",
            "safe_to_auto_apply": False,
            **_action_contract("strategy_profile_activation_review"),
        }
    elif not scheduled_steps:
        plan_status = "schedule_attention_required"
        summary = "当前买入意向已过期或错过窗口；未看到下个窗口的盘中刷新/模拟承接任务。"
        next_action = {
            "type": "inspect_schedule",
            "label": "检查 Hermes trading 调度",
            "command": "atrade diagnose schedule --json",
            "safe_to_auto_apply": True,
            **_action_contract("diagnose_schedule"),
        }
    else:
        plan_status = "waiting_scheduled_next_window"
        summary = "当前买入意向已过期或错过窗口；等待下个窗口重新刷新并形成同日买入意向。"
        next_action = {
            "type": "paper_auto_readiness",
            "label": "下个窗口前复核模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "safe_to_auto_apply": True,
            **_action_contract("paper_auto_readiness"),
        }

    return {
        "available": True,
        "status": plan_status,
        "summary": summary,
        "next_buy_window": {
            "start": window_start.isoformat(),
            "end": window_end.isoformat(),
            "source": "auto_trade.buy_window",
        },
        "current_signal": current_signal,
        "next_window_requires_fresh_buy_signal": not carries_signal,
        "scheduled_steps": scheduled_steps,
        "first_run_verification": build_next_window_first_run_verification(scheduled_steps),
        "next_action": next_action,
        "guardrails": {
            "read_only": True,
            "writes_environment": False,
            "places_order": False,
            "old_signal_auto_carryover": False,
        },
    }


def _action_contract(
    command_contract_id: str,
    *,
    writes_state: bool = False,
    risk_level: str = "read_only",
) -> dict[str, Any]:
    return {
        "writes_state": writes_state,
        "writes_environment": False,
        "writes_order": False,
        "requires_user_approval": False,
        "risk_level": risk_level,
        "command_contract_id": command_contract_id,
    }


def _command_contract(
    contract_id: str,
    *,
    writes_state: bool = False,
    writes_environment: bool = False,
    writes_order: bool = False,
    requires_user_approval: bool = False,
    risk_level: str = "read_only",
    state_events: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": contract_id,
        "risk_level": risk_level,
        "writes_state": writes_state,
        "writes_environment": writes_environment,
        "writes_order": writes_order,
        "requires_user_approval": requires_user_approval,
        "state_events": state_events or [],
    }


def _opportunity_buy_window() -> dict[str, Any]:
    try:
        data, _errors = ConfigRegistry().load_and_validate()
    except Exception:
        return {"start": "09:45", "end": "14:30"}
    return ((data.get("strategy", {}) or {}).get("auto_trade", {}) or {}).get("buy_window", {}) or {
        "start": "09:45",
        "end": "14:30",
    }


def _opportunity_next_window_steps(schedule_diagnosis: dict[str, Any]) -> list[dict[str, Any]]:
    steps = []
    for job in schedule_diagnosis.get("tracked_jobs", []) or []:
        script = str(job.get("script") or "")
        if script not in NEXT_WINDOW_STEP_SCRIPTS:
            continue
        if not job.get("enabled", False) or str(job.get("state") or "") == "paused":
            continue
        next_run = _parse_dt(job.get("next_run_at"))
        next_run_local = next_run.astimezone(MARKET_TZ) if next_run and next_run.tzinfo else next_run
        steps.append({
            "name": job.get("name", ""),
            "script": script,
            "role": _opportunity_next_window_step_role(script),
            "schedule": job.get("schedule", ""),
            "next_run_at": next_run_local.isoformat() if next_run_local else job.get("next_run_at"),
            "last_run_at": job.get("last_run_at"),
            "last_status": job.get("last_status"),
            "pending_first_run": bool(job.get("pending_first_run")),
            "critical_for_intraday_simulation": bool(job.get("critical_for_intraday_simulation")),
        })
    return sorted(steps, key=lambda item: item.get("next_run_at") or "")


def _opportunity_next_window_step_role(script: str) -> str:
    roles = {
        "a_stock_screener_refresh_intraday_silent.sh": "refresh_candidates",
        "a_stock_intraday_execution_cycle_silent.sh": "refresh_and_auto_trade_cycle",
        "a_stock_pipeline_auto_trade_silent.sh": "auto_trade_check_or_submit_paper_order",
    }
    return roles.get(script, "scheduled_step")


def _opportunity_next_window_date(scheduled_steps: list[dict[str, Any]]) -> date:
    future_dates = []
    for step in scheduled_steps:
        next_run = _parse_dt(step.get("next_run_at"))
        if next_run is None:
            continue
        next_run_local = next_run.astimezone(MARKET_TZ) if next_run.tzinfo else next_run.replace(tzinfo=MARKET_TZ)
        future_dates.append(next_run_local.date())
    if future_dates:
        return min(future_dates)
    current = local_now().date()
    candidate = current + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _opportunity_current_signal(
    item: dict[str, Any],
    *,
    next_window_date: date,
    end_time: time,
) -> dict[str, Any]:
    occurred_at = str(item.get("requested_at") or item.get("updated_at") or "")
    occurred = _parse_dt(occurred_at)
    carries = False
    if occurred is not None:
        occurred_local = occurred.astimezone(MARKET_TZ) if occurred.tzinfo else occurred.replace(tzinfo=MARKET_TZ)
        carries = (
            occurred_local.date() == next_window_date
            and occurred_local.replace(second=0, microsecond=0).time() <= end_time
            and not bool(item.get("stale"))
        )
    return {
        "code": item.get("code", ""),
        "name": item.get("name") or item.get("code", ""),
        "occurred_at": occurred_at,
        "score": item.get("score", item.get("confidence")),
        "carries_to_next_window": carries,
        "expires_reason": "买入意向只在产生当日且不晚于买入窗口结束时可被 auto_trade 承接",
    }


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=MARKET_TZ)
    return dt


def _parse_hhmm(value: Any) -> time | None:
    try:
        hour, minute = str(value).split(":", 1)
        return time(int(hour), int(minute))
    except (TypeError, ValueError):
        return None


def _buy_intent_is_stale(latest_decision: dict[str, Any], stale_pending: list[dict[str, Any]]) -> bool:
    code = str(latest_decision.get("code", ""))
    event_id = str(latest_decision.get("event_id", ""))
    for item in stale_pending:
        if event_id and item.get("source_event_id") == event_id:
            return True
        if code and item.get("code") == code and not event_id:
            return True
    return False


def _manual_trade_states(store: EventStore) -> list[dict[str, Any]]:
    return manual_trade_states(
        store.query(stream_type="manual_trade", limit=500),
        policy=load_manual_confirmation_policy(),
    )


def _pending_manual_trades(store: EventStore) -> list[dict[str, Any]]:
    return actionable_pending_manual_trades(_manual_trade_states(store))


def _latest_event(store: EventStore, event_type: str) -> dict[str, Any]:
    row = store._conn.execute(
        """SELECT * FROM event_log
           WHERE event_type = ?
           ORDER BY occurred_at DESC, stream_version DESC
           LIMIT 1""",
        (event_type,),
    ).fetchone()
    return EventStore._row_to_dict(row) if row else {}


def _latest_event_payload(store: EventStore, event_type: str) -> dict[str, Any]:
    return _latest_event(store, event_type).get("payload", {})


def _latest_code_event(store: EventStore, event_type: str, code: str) -> dict[str, Any]:
    events = store.query(event_type=event_type, limit=500)
    matched = [event for event in events if str(event.get("payload", {}).get("code", "")) == code]
    return matched[-1] if matched else {}


def _positions(conn: Any) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT code, name, shares, avg_cost_cents, cost_basis_cents, current_price_cents, unrealized_pnl_cents "
        "FROM projection_positions ORDER BY updated_at DESC LIMIT 20"
    ).fetchall()
    return [dict(row) for row in rows]


def _recent_failed_runs(conn: Any, *, days: int = 3) -> list[dict[str, Any]]:
    cutoff = (utc_now() - timedelta(days=days)).isoformat()
    failed_rows = conn.execute(
        "SELECT run_id, run_type, started_at, error_message "
        "FROM run_log WHERE status = 'failed' AND started_at >= ? "
        "ORDER BY started_at DESC LIMIT 20",
        (cutoff,),
    ).fetchall()
    successful_rows = conn.execute(
        "SELECT run_id, run_type, started_at "
        "FROM run_log WHERE status = 'completed' "
        "ORDER BY started_at DESC LIMIT 200"
    ).fetchall()
    return filter_unrecovered_failed_runs(
        [dict(row) for row in failed_rows],
        [dict(row) for row in successful_rows],
    )


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
