"""Manual confirmation notification helpers."""

from __future__ import annotations

import logging
from dataclasses import asdict, is_dataclass
from enum import Enum
from typing import Any

from astock_trading.reporting.discord import format_manual_confirmation_embed
from astock_trading.reporting.discord_sender import send_embed

_logger = logging.getLogger(__name__)


def notify_manual_confirmation_requested(notification: dict[str, Any]) -> None:
    """Send a Discord card for a newly requested manual trade confirmation."""
    analysis = build_manual_confirmation_analysis(notification)
    embed = format_manual_confirmation_embed(analysis)
    ok, error = send_embed(embed, content="A股人工确认")
    if not ok:
        _logger.warning("[manual_confirmation] Discord 推送失败: %s", error)


def build_manual_confirmation_analysis(notification: dict[str, Any]) -> dict[str, Any]:
    """Build the formatter payload from StrategyService notification context."""
    manual_trade = notification.get("manual_trade", {}) or {}
    score_result = notification.get("score_result")
    decision = notification.get("decision")
    snapshot = notification.get("snapshot")
    market_state = notification.get("market_state")
    metadata = notification.get("metadata", {}) or {}

    score_payload = score_result.to_dict() if hasattr(score_result, "to_dict") else {}
    decision_payload = _decision_payload(decision)

    return {
        "analysis": "manual_confirmation",
        "status": "ok",
        "execution_allowed": False,
        "manual_trade": {
            **manual_trade,
            "event_id": notification.get("event_id", ""),
            "event_type": notification.get("event_type", "manual_trade.requested"),
        },
        "resolved": {
            "code": manual_trade.get("code", ""),
            "name": manual_trade.get("name", manual_trade.get("code", "")),
        },
        "profile": metadata.get("config_version", ""),
        "market": _market_payload(market_state),
        "quote": _jsonable(getattr(snapshot, "quote", None)) or {},
        "technical": _jsonable(getattr(snapshot, "technical", None)) or {},
        "fundamental": _jsonable(getattr(snapshot, "financial", None)) or {},
        "flow": _jsonable(getattr(snapshot, "flow", None)) or {},
        "sentiment": _jsonable(getattr(snapshot, "sentiment", None)) or {},
        "score": score_payload,
        "decision": decision_payload,
        "findings": _findings(score_payload, decision_payload),
        "recommendations": [
            "manual confirmation required before any order; this report never executes trades",
            "verify price, liquidity, position size, risk alerts, and catalysts before record-buy",
        ],
    }


def _decision_payload(decision: Any) -> dict[str, Any]:
    if decision is None:
        return {}
    return {
        "action": _enum_value(getattr(decision, "action", "")),
        "confidence": getattr(decision, "confidence", 0),
        "score": getattr(decision, "score", 0),
        "position_pct": getattr(decision, "position_pct", 0),
        "market_signal": _enum_value(getattr(decision, "market_signal", "")),
        "market_multiplier": getattr(decision, "market_multiplier", 0),
        "veto_reasons": list(getattr(decision, "veto_reasons", []) or []),
        "notes": list(getattr(decision, "notes", []) or []),
    }


def _market_payload(market_state: Any) -> dict[str, Any]:
    if market_state is None:
        return {}
    return {
        "signal": _enum_value(getattr(market_state, "signal", "")),
        "multiplier": getattr(market_state, "multiplier", 0),
        "detail": _jsonable(getattr(market_state, "detail", {})) or {},
    }


def _findings(score: dict[str, Any], decision: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    if score.get("veto_triggered"):
        findings.append("hard veto triggered: " + ",".join(score.get("hard_veto", []) or []))
    if score.get("warning_signals"):
        findings.append("warning signals: " + ",".join(score.get("warning_signals", []) or []))
    if not score.get("entry_signal"):
        findings.append("entry signal not triggered")
    if score.get("data_missing_fields"):
        findings.append("missing data fields: " + ",".join(score.get("data_missing_fields", []) or []))
    findings.extend(decision.get("notes", []) or [])
    return findings


def _jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value):
        return {key: _jsonable(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    return value


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value
