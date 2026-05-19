"""Shared policy for deciding whether a pipeline run should be skipped."""

from __future__ import annotations

from typing import Literal

SkipReason = Literal["non_trading_day", "completed_today"]
DataSourceGateDecision = Literal["failed", "warning"]

MULTI_RUN_PIPELINES = frozenset({"sentiment", "intraday_monitor"})
NON_TRADING_DAY_PIPELINES = frozenset({"sentiment", "weekly", "monthly"})
MARKET_DATA_GATED_PIPELINES = frozenset({
    "morning",
    "noon",
    "intraday_monitor",
    "evening",
    "scoring",
    "auto_trade",
})


def should_skip_pipeline(
    pipeline_type: str,
    *,
    is_trading_day: bool,
    is_completed_today: bool,
) -> SkipReason | None:
    """Return a skip reason for pipeline execution, or None when it may run."""
    if not is_trading_day and pipeline_type not in NON_TRADING_DAY_PIPELINES:
        return "non_trading_day"
    if is_completed_today and pipeline_type not in MULTI_RUN_PIPELINES:
        return "completed_today"
    return None


def data_source_gate_decision(
    pipeline_type: str,
    data_source_health: dict,
) -> DataSourceGateDecision | None:
    """Return data-source gate decision for pipelines that depend on market data."""
    if pipeline_type not in MARKET_DATA_GATED_PIPELINES:
        return None
    status = data_source_health.get("status")
    if status == "failed":
        return "failed"
    if status == "warning":
        return "warning"
    return None


def new_trade_guard_decision(
    *,
    failed_runs: list[dict] | None = None,
    data_source_health: dict | None = None,
    portfolio_breaches: list[dict] | None = None,
) -> dict:
    """Return whether the system may open new trades under daily anomaly guards."""
    blockers = []
    failed_runs = failed_runs or []
    portfolio_breaches = portfolio_breaches or []
    data_source_health = data_source_health or {}

    if failed_runs:
        blockers.append({
            "reason": "recent_failed_pipeline",
            "label": "近期 pipeline 失败",
            "count": len(failed_runs),
            "runs": [
                {
                    "run_id": item.get("run_id"),
                    "run_type": item.get("run_type"),
                    "error_message": item.get("error_message", ""),
                }
                for item in failed_runs[:5]
            ],
        })

    if data_source_health.get("status") == "failed":
        blockers.append({
            "reason": "data_source_health_failed",
            "label": "关键数据源异常",
            "required_missing": data_source_health.get("required_missing", []),
        })

    if portfolio_breaches:
        blockers.append({
            "reason": "portfolio_risk_block",
            "label": "组合风控触发",
            "count": len(portfolio_breaches),
            "breaches": [
                _portfolio_breach_payload(item)
                for item in portfolio_breaches[:5]
            ],
        })

    return {
        "status": "blocked" if blockers else "ok",
        "allow_new_trades": not blockers,
        "blockers": blockers,
    }


def _portfolio_breach_payload(event: dict) -> dict:
    payload = event.get("payload") or event
    return {
        "rule": payload.get("rule"),
        "description": payload.get("description", ""),
        "current_value": payload.get("current_value"),
        "limit_value": payload.get("limit_value"),
    }
