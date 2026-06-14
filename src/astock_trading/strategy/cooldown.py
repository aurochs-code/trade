"""Strategy cooldown helpers backed by persistent events."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any

from astock_trading.platform.domain_events import PAPER_TRIAL_REVIEWED, TRADE_REVIEW_RECORDED
from astock_trading.platform.time import local_today_str


def false_breakout_cooldown(
    event_store: Any,
    code: str,
    cfg: dict | None = None,
) -> dict[str, Any]:
    """Return active cooldown state after repeated failed entry attempts."""
    cfg = cfg or {}
    if not cfg.get("enabled", False) or not code:
        return {"active": False, "enabled": bool(cfg.get("enabled", False))}

    lookback_days = int(cfg.get("lookback_days", 30) or 30)
    failure_threshold = int(cfg.get("failure_threshold", 2) or 2)
    cooldown_days = int(cfg.get("cooldown_days", 10) or 10)
    as_of = _to_date(str(cfg.get("as_of") or local_today_str()))
    cutoff = as_of - timedelta(days=lookback_days)

    failures = [
        item for item in _failure_events(event_store, code)
        if cutoff <= item["failure_date"] <= as_of
    ]
    latest_failure_date = max((item["failure_date"] for item in failures), default=None)
    cooling_until = (
        latest_failure_date + timedelta(days=cooldown_days)
        if latest_failure_date is not None
        else None
    )
    active = (
        len(failures) >= failure_threshold
        and cooling_until is not None
        and as_of <= cooling_until
    )
    return {
        "active": active,
        "enabled": True,
        "code": code,
        "failure_count": len(failures),
        "failure_threshold": failure_threshold,
        "lookback_days": lookback_days,
        "cooldown_days": cooldown_days,
        "as_of": as_of.isoformat(),
        "cooling_until": cooling_until.isoformat() if cooling_until else None,
        "failures": [
            {
                **{k: v for k, v in item.items() if k != "failure_date"},
                "failure_date": item["failure_date"].isoformat(),
            }
            for item in failures
        ],
    }


def _failure_events(event_store: Any, code: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for event in event_store.query(event_type=TRADE_REVIEW_RECORDED, limit=1000):
        payload = event.get("payload") or {}
        if str(payload.get("code") or "") != code:
            continue
        if _trade_review_failed(payload):
            result.append(_failure_item(event, payload, "trade_review"))

    for event in event_store.query(event_type=PAPER_TRIAL_REVIEWED, limit=1000):
        payload = event.get("payload") or {}
        if str(payload.get("code") or "") != code:
            continue
        if _paper_review_failed(payload):
            result.append(_failure_item(event, payload, "paper_trial_review"))
    return result


def _trade_review_failed(payload: dict[str, Any]) -> bool:
    validation = payload.get("hypothesis_validation") or {}
    status = str(validation.get("status") or "")
    if status in {"invalidation_possible", "weakened"}:
        return True
    return _to_float(payload.get("mae_pct")) <= -0.05 and _to_float(payload.get("latest_return_pct")) < 0


def _paper_review_failed(payload: dict[str, Any]) -> bool:
    status = str(payload.get("review_status") or payload.get("status") or "")
    return status == "negative" or _to_float(payload.get("return_pct")) < 0


def _failure_item(event: dict[str, Any], payload: dict[str, Any], source: str) -> dict[str, Any]:
    failure_date = _to_date(
        str(
            payload.get("review_as_of")
            or payload.get("review_date")
            or event.get("occurred_at")
            or local_today_str()
        )[:10]
    )
    return {
        "event_id": event.get("event_id"),
        "event_type": event.get("event_type"),
        "source": source,
        "failure_date": failure_date,
        "mae_pct": payload.get("mae_pct"),
        "latest_return_pct": payload.get("latest_return_pct"),
        "return_pct": payload.get("return_pct"),
    }


def _to_date(value: str) -> date:
    return datetime.fromisoformat(value[:10]).date()


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
