"""人工确认单状态归并与过期判定。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from astock_trading.platform.time import MARKET_TZ, is_market_weekday, local_now

DEFAULT_PENDING_MAX_AGE_HOURS = 4
DEFAULT_BUY_WINDOW = {"start": "09:45", "end": "14:30"}


def load_manual_confirmation_policy(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """读取人工确认有效期策略；读取失败时使用保守默认值。"""
    if config is None:
        try:
            from astock_trading.platform.config import ConfigRegistry

            config, _errors = ConfigRegistry().load_and_validate()
        except Exception:
            config = {}

    strategy = config.get("strategy", config) if isinstance(config, dict) else {}
    manual_cfg = strategy.get("manual_confirmation", {}) or {}
    auto_trade_cfg = strategy.get("auto_trade", {}) or {}
    max_age = manual_cfg.get("pending_max_age_hours") or manual_cfg.get("max_age_hours")
    return {
        "pending_max_age_hours": _positive_int(max_age, DEFAULT_PENDING_MAX_AGE_HOURS),
        "buy_window": manual_cfg.get("buy_window") or auto_trade_cfg.get("buy_window") or DEFAULT_BUY_WINDOW,
    }


def manual_trade_states(
    events: list[dict[str, Any]],
    *,
    policy: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """从 append-only 事件归并出人工确认单当前状态。"""
    effective_policy = policy or load_manual_confirmation_policy()
    current_time = now or local_now()
    by_stream: dict[str, dict[str, Any]] = {}
    for event in events:
        payload = event.get("payload", {}) or {}
        stream = event.get("stream", "")
        current = by_stream.get(stream, {})
        if event.get("event_type") == "manual_trade.requested":
            current = {
                **payload,
                "stream": stream,
                "requested_event_id": event.get("event_id", ""),
                "requested_at": event.get("occurred_at", ""),
                "updated_at": event.get("occurred_at", ""),
            }
        elif current:
            status = payload.get("status") or str(event.get("event_type", "")).removeprefix("manual_trade.")
            current.update(
                {
                    "status": status,
                    "updated_at": event.get("occurred_at", ""),
                    "resolution_event_id": event.get("event_id", ""),
                    "resolution": payload,
                }
            )
        if current:
            by_stream[stream] = _annotate_manual_trade(current, effective_policy, current_time)
    return sorted(by_stream.values(), key=lambda item: item.get("updated_at", ""), reverse=True)


def actionable_pending_manual_trades(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in states
        if item.get("status", "pending") == "pending" and not item.get("stale")
    ]


def stale_pending_manual_trades(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        item
        for item in states
        if item.get("status", "pending") == "pending" and item.get("stale")
    ]


def _annotate_manual_trade(
    item: dict[str, Any],
    policy: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    annotated = dict(item)
    requested = _parse_iso(str(annotated.get("requested_at", "")))
    max_age_hours = _positive_int(
        policy.get("pending_max_age_hours"),
        DEFAULT_PENDING_MAX_AGE_HOURS,
    )
    annotated["max_age_hours"] = max_age_hours
    if requested:
        now_utc = _ensure_tz(now).astimezone(timezone.utc)
        requested_utc = requested.astimezone(timezone.utc)
        age_hours = max((now_utc - requested_utc).total_seconds() / 3600, 0)
        annotated["age_hours"] = round(age_hours, 2)
        annotated["expires_at"] = (requested_utc + timedelta(hours=max_age_hours)).isoformat()
    else:
        annotated["age_hours"] = None
        annotated["expires_at"] = ""

    if annotated.get("status", "pending") != "pending":
        annotated["stale"] = False
        annotated["actionable"] = False
        return annotated

    reason = _stale_reason(annotated, requested, policy, now)
    annotated["stale"] = bool(reason)
    annotated["actionable"] = not reason
    if reason:
        annotated["stale_reason"] = reason
        annotated["stale_reason_label"] = _stale_reason_label(reason)
    return annotated


def _stale_reason(
    item: dict[str, Any],
    requested: datetime | None,
    policy: dict[str, Any],
    now: datetime,
) -> str:
    if requested is None:
        return "invalid_requested_at"
    max_age_hours = _positive_int(policy.get("pending_max_age_hours"), DEFAULT_PENDING_MAX_AGE_HOURS)
    if item.get("age_hours") is not None and float(item["age_hours"]) > max_age_hours:
        return "age_exceeded"

    if str(item.get("side", "")).lower() != "buy":
        return ""
    end_time = _window_end(policy.get("buy_window"))
    if end_time is None:
        return ""

    requested_local = requested.astimezone(MARKET_TZ)
    now_local = _ensure_tz(now).astimezone(MARKET_TZ)
    if requested_local.date() < now_local.date():
        return "trading_day_changed"
    if not is_market_weekday(now_local):
        return "non_trading_day"
    if not is_market_weekday(requested_local):
        return "non_trading_day"
    requested_minute = requested_local.replace(second=0, microsecond=0).time()
    now_minute = now_local.replace(second=0, microsecond=0).time()
    if requested_minute > end_time:
        return "requested_after_buy_window"
    if requested_local.date() == now_local.date() and now_minute > end_time:
        return "buy_window_closed"
    return ""


def _stale_reason_label(reason: str) -> str:
    return {
        "age_exceeded": "超过确认有效期",
        "trading_day_changed": "已跨交易日",
        "non_trading_day": "当前非交易日",
        "requested_after_buy_window": "信号产生时已错过买入窗口",
        "buy_window_closed": "买入窗口已关闭",
        "invalid_requested_at": "确认时间无法解析",
    }.get(reason, "已过期")


def _window_end(window: Any) -> Any:
    if not isinstance(window, dict):
        return None
    return _parse_hhmm(str(window.get("end", "")))


def _parse_hhmm(value: str):
    try:
        return datetime.strptime(value, "%H:%M").time()
    except (TypeError, ValueError):
        return None


def _parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _ensure_tz(parsed)


def _ensure_tz(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _positive_int(value: Any, fallback: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return fallback
    return number if number > 0 else fallback
