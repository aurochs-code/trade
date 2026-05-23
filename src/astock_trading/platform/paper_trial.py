"""模拟盘试运行候选计划。"""

from __future__ import annotations

from datetime import date
import json
from statistics import median
from typing import Any

from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores
from astock_trading.platform.domain_events import PAPER_TRIAL_RECORDED, PAPER_TRIAL_REVIEWED
from astock_trading.platform.time import local_today_str


def build_paper_trial_plan(
    conn: Any,
    *,
    event_store: Any | None = None,
    limit: int = 10,
    record: bool = False,
) -> dict[str, Any]:
    """从候选池生成只读试运行计划，不下单。"""
    candidates = [_trial_candidate(conn, row) for row in _candidate_rows(conn, limit=limit)]
    first = candidates[0] if candidates else None
    status = "ready" if candidates else "empty"
    date = local_today_str()
    recorded_count = _record_trial_candidates(event_store, candidates, date=date) if record else 0
    candidate_summary = _trial_candidate_summary(candidates)
    summary = (
        f"生成 {len(candidates)} 只模拟盘试运行候选；只做影子观察，不自动下单。"
        if candidates
        else "暂无可试运行候选；先刷新筛选和评分证据。"
    )
    return {
        "command": "paper trial-plan",
        "status": status,
        "date": date,
        "summary": summary,
        "execution_allowed": False,
        "manual_confirmation_required": True,
        "recorded_count": recorded_count,
        "counts": {
            "trial_candidates": len(candidates),
            "core_candidates": sum(1 for item in candidates if item["pool_tier"] == "core"),
            "watch_candidates": sum(1 for item in candidates if item["pool_tier"] == "watch"),
            "radar_candidates": sum(1 for item in candidates if item["pool_tier"] == "radar"),
        },
        "candidate_summary": candidate_summary,
        "current_entry_signals": _current_entry_signals(candidates),
        "candidates": candidates,
        "next_action": _next_action(first),
        "guardrails": {
            "shadow_only": True,
            "paper_order_submitted": False,
            "manual_confirmation_required": True,
            "auto_threshold_change_allowed": False,
        },
    }


def build_paper_trial_review(
    conn: Any,
    event_store: Any,
    *,
    trial_date: str = "",
    as_of: str = "",
    min_age_days: int = 1,
    record: bool = False,
    limit: int = 100,
) -> dict[str, Any]:
    """复盘 paper.trial.recorded 影子候选，不下单。"""
    review_date = as_of or local_today_str()
    events = event_store.query(
        event_type=PAPER_TRIAL_RECORDED,
        limit=max(int(limit or 1), 1),
    )
    latest_events: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        payload = event.get("payload") or {}
        key = (str(payload.get("trial_date") or ""), str(payload.get("code") or ""))
        latest_events[key] = event

    items = []
    for event in latest_events.values():
        payload = event.get("payload") or {}
        if trial_date and payload.get("trial_date") != trial_date:
            continue
        items.append(_trial_review_item(conn, event, review_date, min_age_days=min_age_days))

    recorded_count = _record_trial_reviews(event_store, items, review_date=review_date) if record else 0
    status = "ok" if items else "empty"
    summary = _trial_review_summary(items)
    positive_reviews = _positive_trial_reviews(items)
    return {
        "command": "paper trial-review",
        "status": status,
        "date": review_date,
        "trial_date": trial_date or None,
        "summary": summary,
        "review_summary": summary,
        "positive_reviews": positive_reviews,
        "execution_allowed": False,
        "manual_confirmation_required": True,
        "recorded_count": recorded_count,
        "items": items,
        "next_action": _review_next_action(items, positive_reviews=positive_reviews),
        "guardrails": {
            "shadow_only": True,
            "paper_order_submitted": False,
            "manual_confirmation_required": True,
            "auto_promotion_allowed": False,
        },
    }


def _record_trial_candidates(event_store: Any | None, candidates: list[dict[str, Any]], *, date: str) -> int:
    if event_store is None:
        return 0

    recorded = 0
    for candidate in candidates:
        code = candidate["code"]
        stream = f"paper_trial:{date}:{code}"
        existing = event_store.query(stream=stream, event_type=PAPER_TRIAL_RECORDED, limit=20)
        if any(_to_float((event.get("payload") or {}).get("trial_start_price")) for event in existing):
            continue
        if existing and candidate.get("trial_start_price") is None:
            continue
        payload = {
            **candidate,
            "trial_date": date,
            "paper_order_submitted": False,
        }
        if existing:
            payload["baseline_supplemented"] = True
            payload["previous_event_id"] = existing[-1].get("event_id")
        event_store.append(
            stream=stream,
            stream_type="paper_trial",
            event_type=PAPER_TRIAL_RECORDED,
            payload=payload,
            metadata={
                "source": "paper.trial-plan",
                "account": "paper",
                "shadow_only": True,
            },
        )
        recorded += 1
    return recorded


def _record_trial_reviews(event_store: Any, items: list[dict[str, Any]], *, review_date: str) -> int:
    recorded = 0
    for item in items:
        code = item["code"]
        stream = f"paper_trial_review:{review_date}:{code}"
        existing = event_store.query(stream=stream, event_type=PAPER_TRIAL_REVIEWED, limit=20)
        latest = existing[-1] if existing else None
        if latest and _same_review_payload(latest.get("payload") or {}, item):
            continue
        payload = dict(item)
        if latest:
            previous = latest.get("payload") or {}
            payload["review_corrected"] = True
            payload["previous_event_id"] = latest.get("event_id")
            payload["previous_review_status"] = previous.get("review_status")
        event_store.append(
            stream=stream,
            stream_type="paper_trial",
            event_type=PAPER_TRIAL_REVIEWED,
            payload=payload,
            metadata={
                "source": "paper.trial-review",
                "account": "paper",
                "shadow_only": True,
            },
        )
        recorded += 1
    return recorded


def _same_review_payload(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    keys = (
        "code",
        "trial_date",
        "review_date",
        "review_status",
        "current_pool_tier",
        "current_entry_signal",
        "current_primary_strategy_route",
        "candidate_state_changed",
        "candidate_state_change_label",
    )
    for key in keys:
        if previous.get(key) != current.get(key):
            return False
    if str(previous.get("price_anomaly_reason") or "") != str(current.get("price_anomaly_reason") or ""):
        return False
    for key in ("trial_start_price", "current_price", "return_pct", "current_score"):
        if _to_float(previous.get(key)) != _to_float(current.get(key)):
            return False
    return True


def _candidate_rows(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                  streak_days, note
           FROM projection_candidate_pool
           WHERE pool_tier IN ('core', 'watch', 'radar')
           ORDER BY CASE pool_tier WHEN 'core' THEN 0 WHEN 'watch' THEN 1 ELSE 2 END,
                    score DESC,
                    last_scored_at DESC,
                    code
           LIMIT ?""",
        (max(int(limit or 1), 1),),
    ).fetchall()
    result = [dict(row) for row in rows]
    enrich_candidate_rows_with_latest_scores(conn, result)
    return result


def _trial_candidate(conn: Any, row: dict[str, Any]) -> dict[str, Any]:
    code = str(row.get("code") or "")
    tier = str(row.get("pool_tier") or "")
    note = str(row.get("note") or "")
    price = _latest_market_price(conn, code)
    entry_signal = row.get("entry_signal")
    route_label = row.get("primary_strategy_route_label")
    return {
        "code": code,
        "name": row.get("name") or code,
        "pool_tier": tier,
        "pool_tier_label": _tier_label(tier),
        "score": float(row.get("score") or 0),
        "streak_days": int(row.get("streak_days") or 0),
        "note": note,
        "note_label": _note_label(note),
        "entry_signal": entry_signal,
        "primary_strategy_route": row.get("primary_strategy_route"),
        "primary_strategy_route_label": route_label,
        "strategy_routes": row.get("strategy_routes") or [],
        "technical_detail": row.get("technical_detail") or "",
        "data_quality": row.get("data_quality") or "",
        "trial_mode": "影子试运行",
        "trial_reason": _trial_reason(tier, note, entry_signal=entry_signal, route_label=route_label),
        "review_command": f"atrade stock analyze {code} --json",
        "risk_command": "atrade risk trial-guard --json",
        "paper_order_allowed": False,
        "trial_start_price": price["price"] if price else None,
        "trial_start_price_source": price["source"] if price else None,
        "trial_start_observed_at": price["observed_at"] if price else None,
    }


def _trial_candidate_summary(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(candidates)
    core_count = sum(1 for item in candidates if item.get("pool_tier") == "core")
    watch_count = sum(1 for item in candidates if item.get("pool_tier") == "watch")
    radar_count = sum(1 for item in candidates if item.get("pool_tier") == "radar")
    entry_signal_count = sum(1 for item in candidates if _truthy(item.get("entry_signal")))
    return {
        "total": total,
        "core_count": core_count,
        "watch_count": watch_count,
        "radar_count": radar_count,
        "entry_signal_count": entry_signal_count,
        "summary": (
            f"影子试运行候选 {total} 只：核心 {core_count}、观察 {watch_count}、"
            f"强势观察 {radar_count}；当前入场信号 {entry_signal_count} 只。"
        ),
        "top_core_candidate": _first_trial_candidate_for_tier(candidates, "core"),
        "top_watch_candidate": _first_trial_candidate_for_tier(candidates, "watch"),
        "top_radar_candidate": _first_trial_candidate_for_tier(candidates, "radar"),
    }


def _current_entry_signals(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        _compact_trial_candidate(item)
        for item in candidates
        if _truthy(item.get("entry_signal"))
    ]


def _first_trial_candidate_for_tier(candidates: list[dict[str, Any]], tier: str) -> dict[str, Any]:
    for item in candidates:
        if item.get("pool_tier") == tier:
            return _compact_trial_candidate(item)
    return {}


def _compact_trial_candidate(item: dict[str, Any]) -> dict[str, Any]:
    code = str(item.get("code") or "")
    return {
        "code": code,
        "name": item.get("name") or code,
        "pool_tier": item.get("pool_tier"),
        "pool_tier_label": item.get("pool_tier_label"),
        "score": item.get("score", 0) or 0,
        "entry_signal": item.get("entry_signal"),
        "primary_strategy_route": item.get("primary_strategy_route"),
        "primary_strategy_route_label": item.get("primary_strategy_route_label"),
        "technical_detail": item.get("technical_detail", ""),
        "review_command": item.get("review_command") or (f"atrade stock analyze {code} --json" if code else ""),
        "risk_command": item.get("risk_command") or "atrade risk trial-guard --json",
    }


def _current_candidate_state(conn: Any, code: str) -> dict[str, Any] | None:
    if not code:
        return None
    row = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                  streak_days, note
           FROM projection_candidate_pool
           WHERE code = ?
           LIMIT 1""",
        (code,),
    ).fetchone()
    if not row:
        return None

    rows = [dict(row)]
    enrich_candidate_rows_with_latest_scores(conn, rows)
    current = rows[0]
    tier = str(current.get("pool_tier") or "")
    note = str(current.get("note") or "")
    return {
        "current_pool_tier": tier,
        "current_pool_tier_label": _tier_label(tier),
        "current_score": float(current.get("score") or 0),
        "current_note": note,
        "current_note_label": _note_label(note),
        "current_entry_signal": current.get("entry_signal"),
        "current_primary_strategy_route": current.get("primary_strategy_route"),
        "current_primary_strategy_route_label": current.get("primary_strategy_route_label"),
        "current_strategy_routes": current.get("strategy_routes") or [],
        "current_technical_detail": current.get("technical_detail") or "",
        "current_data_quality": current.get("data_quality") or "",
    }


def _candidate_state_changed(
    trial_payload: dict[str, Any],
    current_candidate: dict[str, Any],
) -> bool:
    if str(trial_payload.get("pool_tier") or "") != str(current_candidate.get("current_pool_tier") or ""):
        return True

    trial_score = _to_float(trial_payload.get("score"))
    current_score = _to_float(current_candidate.get("current_score"))
    if trial_score is None and current_score is None:
        return False
    if trial_score is None or current_score is None:
        return True
    if round(trial_score, 4) != round(current_score, 4):
        return True

    if _truthy(trial_payload.get("entry_signal")) != _truthy(current_candidate.get("current_entry_signal")):
        return True

    return False


def _candidate_state_change_label(
    trial_payload: dict[str, Any],
    current_candidate: dict[str, Any],
) -> str:
    trial_tier = str(trial_payload.get("pool_tier") or "")
    current_tier = str(current_candidate.get("current_pool_tier") or "")
    if trial_tier != current_tier:
        return f"{_tier_label(trial_tier)} -> {_tier_label(current_tier)}"

    trial_score = _to_float(trial_payload.get("score"))
    current_score = _to_float(current_candidate.get("current_score"))
    if trial_score is not None and current_score is not None and round(trial_score, 4) != round(current_score, 4):
        return f"评分 {trial_score:.1f} -> {current_score:.1f}"

    if _truthy(trial_payload.get("entry_signal")) != _truthy(current_candidate.get("current_entry_signal")):
        return "入场信号：无 -> 有" if _truthy(current_candidate.get("current_entry_signal")) else "入场信号：有 -> 无"

    return ""


def _next_action(first: dict[str, Any] | None) -> dict[str, Any]:
    if not first:
        return {
            "type": "refresh_scores",
            "label": "刷新筛选评分",
            "command": "atrade screener refresh --json",
            "reason": "当前没有候选可做模拟盘影子观察。",
            "safe_to_auto_apply": True,
            **_action_contract("screener_refresh", writes_state=True, risk_level="state_write"),
        }
    return {
        "type": "review_trial_candidate",
        "label": "复核首个试运行候选",
        "command": first["review_command"],
        "reason": "先看评分、入场信号和否决原因，再决定是否人工加入模拟盘。",
        "safe_to_auto_apply": True,
        **_action_contract("stock_analyze"),
    }


def _action_contract(
    command_contract_id: str,
    *,
    writes_state: bool = False,
    writes_environment: bool = False,
    writes_order: bool = False,
    requires_user_approval: bool = False,
    risk_level: str = "read_only",
) -> dict[str, Any]:
    return {
        "writes_state": writes_state,
        "writes_environment": writes_environment,
        "writes_order": writes_order,
        "requires_user_approval": requires_user_approval,
        "risk_level": risk_level,
        "command_contract_id": command_contract_id,
    }


def _tier_label(tier: str) -> str:
    return {"core": "核心", "watch": "观察", "radar": "强势观察"}.get(tier, tier or "未分层")


def _note_label(note: str) -> str:
    if "requires_entry_strategy_route" in note:
        return "缺少可执行策略路线"
    if "below_watch_retained" in note:
        return "低于观察线，保留跟踪"
    if note == "screener_refresh":
        return "筛选刷新入池"
    if note == "screener_auto_watch":
        return "自动加入观察池"
    return note or "未注明"


def _trial_reason(
    tier: str,
    note: str,
    *,
    entry_signal: object = None,
    route_label: object = None,
) -> str:
    if _truthy(entry_signal):
        label = str(route_label or "可执行策略路线")
        if tier == "core":
            return f"核心候选，已有入场信号：{label}；本计划仍不自动下单。"
        return f"已有入场信号：{label}；先做影子试运行并等待人工复核。"
    if tier == "core":
        return "核心候选，可进入正式买入意向复核；本计划仍不自动下单。"
    if "requires_entry_strategy_route" in note:
        return "分数接近核心线，但缺少可执行入场路线，只做影子观察。"
    if tier == "radar":
        return "强势观察候选，先跟踪强度变化，不进入自动买入。"
    return "观察候选，等待入场信号或连续评分确认。"


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _trial_review_item(
    conn: Any,
    event: dict[str, Any],
    review_date: str,
    *,
    min_age_days: int,
) -> dict[str, Any]:
    payload = event.get("payload") or {}
    code = str(payload.get("code") or "")
    trial_date = str(payload.get("trial_date") or "")
    start_price = _to_float(payload.get("trial_start_price"))
    start_source = payload.get("trial_start_price_source")
    start_observed_at = payload.get("trial_start_observed_at")
    if start_price is None:
        fallback = _first_market_price_after(conn, code, event.get("occurred_at", ""))
        if fallback:
            start_price = fallback["price"]
            start_source = fallback["source"]
            start_observed_at = fallback["observed_at"]

    current = _latest_market_price(conn, code)
    current_candidate = _current_candidate_state(conn, code)
    current_price = current["price"] if current else None
    return_pct = _return_pct(start_price, current_price)
    age_days = _age_days(trial_date, review_date)
    price_anomaly_reason = _price_anomaly_reason(age_days=age_days, return_pct=return_pct)
    review_status = _review_status(
        age_days=age_days,
        min_age_days=min_age_days,
        start_price=start_price,
        current_price=current_price,
        return_pct=return_pct,
        price_anomaly_reason=price_anomaly_reason,
    )
    item = {
        "code": code,
        "name": payload.get("name") or code,
        "trial_date": trial_date,
        "review_date": review_date,
        "age_days": age_days,
        "pool_tier": payload.get("pool_tier"),
        "pool_tier_label": payload.get("pool_tier_label") or _tier_label(str(payload.get("pool_tier") or "")),
        "score": payload.get("score"),
        "trial_start_price": start_price,
        "trial_start_price_source": start_source,
        "trial_start_observed_at": start_observed_at,
        "current_price": current_price,
        "current_price_source": current["source"] if current else None,
        "current_observed_at": current["observed_at"] if current else None,
        "return_pct": return_pct,
        "price_anomaly": bool(price_anomaly_reason),
        "price_anomaly_reason": price_anomaly_reason,
        "review_status": review_status,
        "review_status_label": _review_status_label(review_status),
        "paper_order_submitted": False,
        "next_action": _item_next_action(code, review_status),
    }
    if current_candidate:
        item.update(current_candidate)
        item["candidate_state_changed"] = _candidate_state_changed(payload, current_candidate)
        item["candidate_state_change_label"] = _candidate_state_change_label(payload, current_candidate)
    else:
        item.update(_removed_candidate_state(payload))
    return item


def _removed_candidate_state(trial_payload: dict[str, Any]) -> dict[str, Any]:
    trial_tier = str(trial_payload.get("pool_tier") or "")
    change_label = "已移出候选池"
    if trial_tier:
        change_label = f"{_tier_label(trial_tier)} -> 已移出候选池"
    return {
        "current_pool_tier": None,
        "current_pool_tier_label": "已移出候选池",
        "current_score": None,
        "current_note": "",
        "current_note_label": "",
        "current_entry_signal": None,
        "current_primary_strategy_route": None,
        "current_primary_strategy_route_label": None,
        "current_strategy_routes": [],
        "current_technical_detail": "",
        "current_data_quality": "",
        "candidate_state_changed": True,
        "candidate_state_change_label": change_label,
    }


def _trial_review_summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {
        "trial_count": len(items),
        "positive_count": 0,
        "flat_count": 0,
        "negative_count": 0,
        "pending_count": 0,
        "insufficient_price_count": 0,
        "price_anomaly_count": 0,
    }
    for item in items:
        status = item.get("review_status")
        if status == "positive":
            counts["positive_count"] += 1
        elif status == "flat":
            counts["flat_count"] += 1
        elif status == "negative":
            counts["negative_count"] += 1
        elif status == "pending":
            counts["pending_count"] += 1
        elif status == "insufficient_price":
            counts["insufficient_price_count"] += 1
        elif status == "price_anomaly":
            counts["price_anomaly_count"] += 1
    return counts


def _positive_trial_reviews(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        dict(item)
        for item in sorted(items, key=_trial_review_priority)
        if item.get("review_status") == "positive"
    ][:5]


def _trial_review_priority(item: dict[str, Any]) -> tuple[int, int, float, str]:
    tier_rank = {
        "core": 0,
        "watch": 1,
        "radar": 2,
    }.get(str(item.get("current_pool_tier") or ""), 3)
    entry_rank = 0 if _truthy(item.get("current_entry_signal")) else 1
    return_pct = _to_float(item.get("return_pct")) or 0.0
    return (tier_rank, entry_rank, -return_pct, str(item.get("code") or ""))


def _review_next_action(
    items: list[dict[str, Any]],
    *,
    positive_reviews: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    anomaly = next((item for item in items if item.get("review_status") == "price_anomaly"), None)
    if anomaly:
        return {
            "type": "inspect_price_anomaly",
            "label": "核查价格异常",
            "command": f"atrade stock analyze {anomaly['code']} --json",
            "reason": "影子复盘发现异常价格跳变，先核查行情证据，不能按正收益处理。",
            "safe_to_auto_apply": True,
            **_action_contract("stock_analyze"),
        }
    positive = (positive_reviews if positive_reviews is not None else _positive_trial_reviews(items))
    positive = positive[0] if positive else None
    if positive:
        return {
            "type": "review_positive_trial",
            "label": "复核表现为正的影子候选",
            "command": f"atrade stock analyze {positive['code']} --json",
            "reason": "影子试运行收益为正，只能进入人工复核，不能自动晋级或下单。",
            "safe_to_auto_apply": True,
            **_action_contract("stock_analyze"),
        }
    return {
        "type": "paper_trial_plan",
        "label": "刷新影子试运行计划",
        "command": "atrade paper trial-plan --json",
        "reason": "继续跟踪观察候选，等待价格和入场证据确认。",
        "safe_to_auto_apply": True,
        **_action_contract("paper_trial_plan"),
    }


def _item_next_action(code: str, review_status: str) -> str:
    if review_status == "positive":
        return f"atrade stock analyze {code} --json"
    if review_status == "price_anomaly":
        return f"atrade stock analyze {code} --json"
    if review_status == "negative":
        return f"atrade explain {code} --json"
    return "atrade paper trial-review --json"


def _review_status(
    *,
    age_days: int | None,
    min_age_days: int,
    start_price: float | None,
    current_price: float | None,
    return_pct: float | None,
    price_anomaly_reason: str = "",
) -> str:
    if age_days is not None and age_days < min_age_days:
        return "pending"
    if start_price is None or current_price is None or return_pct is None:
        return "insufficient_price"
    if price_anomaly_reason:
        return "price_anomaly"
    if return_pct >= 3:
        return "positive"
    if return_pct <= -3:
        return "negative"
    return "flat"


def _review_status_label(status: str) -> str:
    return {
        "positive": "表现为正",
        "flat": "横盘观察",
        "negative": "表现转弱",
        "pending": "观察期不足",
        "insufficient_price": "价格证据不足",
        "price_anomaly": "价格异常",
    }.get(status, status)


def _price_anomaly_reason(*, age_days: int | None, return_pct: float | None) -> str:
    if return_pct is None:
        return ""
    days = age_days if age_days is not None else 0
    if days <= 1:
        threshold = 40
    elif days <= 5:
        threshold = 120
    else:
        threshold = 300
    if abs(return_pct) <= threshold:
        return ""
    return f"{days} 天价格变动 {return_pct:.2f}% 超过 {threshold}% 护栏，疑似行情快照异常。"


def _latest_market_price(conn: Any, code: str) -> dict[str, Any] | None:
    return _market_price_row(
        conn,
        code,
        order="DESC",
    )


def _first_market_price_after(conn: Any, code: str, observed_at: str) -> dict[str, Any] | None:
    return _market_price_row(
        conn,
        code,
        order="ASC",
        since=observed_at,
    )


def _market_price_row(
    conn: Any,
    code: str,
    *,
    order: str,
    since: str | None = None,
) -> dict[str, Any] | None:
    if not code:
        return None
    clauses = "symbol = ? AND kind IN ('quote', 'snapshot')"
    params: list[Any] = [code]
    if since:
        clauses += " AND observed_at >= ?"
        params.append(since)
    rows = conn.execute(
        f"""SELECT kind, observed_at, payload_json
            FROM market_observations
            WHERE {clauses}
            ORDER BY observed_at {order}
            LIMIT 20""",
        params,
    ).fetchall()
    candidates: list[dict[str, Any]] = []
    for row in rows:
        payload = _json_payload(row["payload_json"])
        price = _price_from_payload(payload)
        if price is None:
            continue
        candidates.append({
            "price": price,
            "source": f"market_observations.{row['kind']}",
            "observed_at": row["observed_at"],
        })
    if not candidates:
        return None
    if order.upper() == "DESC":
        return _first_non_outlier_price(candidates)
    return candidates[0]


def _first_non_outlier_price(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    for index, candidate in enumerate(candidates):
        older_prices = [
            float(item["price"])
            for item in candidates[index + 1 : index + 8]
            if _to_float(item.get("price")) is not None and float(item["price"]) > 0
        ]
        price = _to_float(candidate.get("price"))
        if price is None or price <= 0:
            continue
        if len(older_prices) >= 2:
            reference = median(older_prices)
            if reference > 0 and abs(price / reference - 1) * 100 > 40:
                continue
        return candidate
    return candidates[0]


def _json_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
            return decoded if isinstance(decoded, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _price_from_payload(payload: dict[str, Any]) -> float | None:
    quote = payload.get("quote") if isinstance(payload.get("quote"), dict) else payload
    for key in ("price", "close", "current_price"):
        price = _to_float(quote.get(key))
        if price is not None and price > 0:
            return price
    return None


def _return_pct(start_price: float | None, current_price: float | None) -> float | None:
    if start_price is None or current_price is None or start_price <= 0:
        return None
    return round((current_price / start_price - 1) * 100, 2)


def _age_days(trial_date: str, review_date: str) -> int | None:
    try:
        start = date.fromisoformat(trial_date)
        end = date.fromisoformat(review_date)
    except ValueError:
        return None
    return (end - start).days


def _to_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
