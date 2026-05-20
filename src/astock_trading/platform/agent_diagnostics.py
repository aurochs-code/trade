"""Read-only diagnostics used by Agent-facing CLI and MCP tools."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from astock_trading.market.health import evaluate_data_source_health
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.data_source_diagnostics import (
    build_data_source_diagnosis,
    data_source_blocker_summary,
    data_source_blockers_for_new_trades,
)
from astock_trading.platform.time import utc_now


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _decode_json(value: Any) -> Any:
    if not value:
        return {}
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return {}


def candidate_pool_summary(conn: Any, *, now: datetime | None = None, max_age_days: int = 3) -> dict:
    """Return a small candidate-pool freshness summary."""
    now = now or utc_now()
    rows = conn.execute(
        """SELECT pool_tier, COUNT(*) AS count, MAX(last_scored_at) AS last_scored_at
           FROM projection_candidate_pool
           GROUP BY pool_tier"""
    ).fetchall()
    tiers = {
        row["pool_tier"]: {
            "count": row["count"],
            "last_scored_at": row["last_scored_at"],
        }
        for row in rows
    }
    total = sum(item["count"] for item in tiers.values())
    latest_scored_at = None
    for item in tiers.values():
        dt = _parse_dt(item.get("last_scored_at"))
        if dt and (latest_scored_at is None or dt > latest_scored_at):
            latest_scored_at = dt

    age_days = None
    stale = False
    if latest_scored_at:
        age_days = round((now - latest_scored_at).total_seconds() / 86400, 2)
        stale = age_days > max_age_days

    core_count = tiers.get("core", {}).get("count", 0)
    status = "warning" if total == 0 or core_count == 0 or stale else "ok"
    return {
        "status": status,
        "total": total,
        "core_count": core_count,
        "watch_count": tiers.get("watch", {}).get("count", 0),
        "latest_scored_at": latest_scored_at.isoformat() if latest_scored_at else None,
        "age_days": age_days,
        "max_age_days": max_age_days,
        "stale": stale,
    }


def diagnose_health(conn: Any) -> dict:
    """Build a read-only health diagnosis for Agent orchestration."""
    now = utc_now()
    recent_failed_cutoff = (now.replace(microsecond=0) - timedelta(days=3)).isoformat()
    data_sources = evaluate_data_source_health(conn)
    candidate_pool = candidate_pool_summary(conn)
    failed_runs = conn.execute(
        """SELECT run_id, run_type, started_at, error_message
           FROM run_log
           WHERE status = 'failed'
             AND started_at >= ?
           ORDER BY started_at DESC
           LIMIT 10""",
        (recent_failed_cutoff,),
    ).fetchall()
    historical_failed_runs = conn.execute(
        """SELECT run_id, run_type, started_at, error_message
           FROM run_log
           WHERE status = 'failed'
             AND (started_at < ? OR started_at IS NULL)
           ORDER BY started_at DESC
           LIMIT 10""",
        (recent_failed_cutoff,),
    ).fetchall()
    running_runs = conn.execute(
        """SELECT run_id, run_type, started_at
           FROM run_log
           WHERE status = 'running'
           ORDER BY started_at DESC
           LIMIT 10"""
    ).fetchall()

    findings: list[str] = []
    recommendations: list[str] = []
    if data_sources["status"] == "failed":
        missing = ", ".join(data_sources.get("required_missing", []))
        findings.append(f"required data sources unavailable: {missing}")
        recommendations.append("refresh required market data sources before scoring or auto_trade")
    elif data_sources["status"] == "warning":
        missing = ", ".join(data_sources.get("optional_missing", []))
        findings.append(f"optional data sources degraded: {missing}")
        recommendations.append("continue read-only analysis, but avoid expanding execution confidence")

    provider_failures = data_sources.get("provider_failures", {}) or {}
    unresolved_provider_failures = int(provider_failures.get("unresolved_recent", 0) or 0)
    if unresolved_provider_failures:
        findings.append(f"{unresolved_provider_failures} 个 provider 失败未被 fallback 补齐")
        recommendations.append("查看 data_sources.provider_failures.unresolved，先修未补齐的数据源再扩大交易判断")

    if candidate_pool["total"] == 0:
        if not data_sources.get("required_missing"):
            findings.append(
                "candidate pool is empty; required data sources are available, "
                "so treat this as no qualified candidates after screening"
            )
            recommendations.append(
                "refresh candidates if needed; if it stays empty, report it as no qualified candidates, not missing market data"
            )
        else:
            findings.append("candidate pool is empty")
            recommendations.append("run screener refresh before scoring")
    elif candidate_pool["core_count"] == 0:
        findings.append("candidate core pool is empty")
        recommendations.append("promote fresh high-score candidates before auto_trade buy-side decisions")
    if candidate_pool["stale"]:
        findings.append(
            f"candidate pool scores are stale: {candidate_pool['age_days']}d "
            f"> {candidate_pool['max_age_days']}d"
        )
        recommendations.append("refresh candidate scores before generating a trade plan")

    if failed_runs:
        findings.append(f"{len(failed_runs)} failed runs require review")
        recommendations.append("inspect failed run errors with explain-run")
    if running_runs:
        findings.append(f"{len(running_runs)} runs are still marked running")
        recommendations.append("review running runs before scheduling more pipelines")

    status = "failed" if data_sources["status"] == "failed" else "warning" if findings else "ok"
    return {
        "diagnostic": "health",
        "status": status,
        "findings": findings,
        "recommendations": recommendations,
        "inputs": {
            "data_sources": data_sources,
            "candidate_pool": candidate_pool,
            "failed_runs": [dict(row) for row in failed_runs],
            "historical_failed_runs": [dict(row) for row in historical_failed_runs],
            "running_runs": [dict(row) for row in running_runs],
        },
    }


def diagnose_strategy(conn: Any) -> dict:
    """Assess strategy parameters and whether the system should use multiple profiles."""
    data, config_errors = ConfigRegistry().load_and_validate()
    strategy = data.get("strategy", {})
    scoring = strategy.get("scoring", {})
    weights = scoring.get("weights", {})
    thresholds = scoring.get("thresholds", {})
    gates = scoring.get("decision_gates", {})
    screening = strategy.get("screening", {})
    pool = strategy.get("pool_management", {})
    continuation = strategy.get("continuation", {})
    backtest_presets = strategy.get("backtest_presets", {})
    auto_trade = strategy.get("auto_trade", {})
    candidate_pool = candidate_pool_summary(conn)

    findings: list[str] = []
    recommendations: list[str] = []

    if config_errors:
        findings.extend(config_errors)
        recommendations.append("fix config validation warnings before changing thresholds")

    if weights:
        total_weight = sum(float(v or 0) for v in weights.values())
        if total_weight != 10:
            findings.append(f"scoring weights sum to {total_weight}, expected 10")
        if float(weights.get("sentiment", 0) or 0) >= float(weights.get("technical", 0) or 0):
            findings.append("sentiment weight is as high as technical weight")
            recommendations.append("keep sentiment as a confidence modifier unless its forward value is validated")

    buy_threshold = float(thresholds.get("buy", 0) or 0)
    watch_threshold = float(thresholds.get("watch", 0) or 0)
    if buy_threshold and buy_threshold <= 5.5:
        findings.append(f"buy threshold is permissive: {buy_threshold:.1f}")
        recommendations.append("require entry/data-quality gates when buy threshold is <= 5.5")
    if buy_threshold and watch_threshold and buy_threshold - watch_threshold < 0.7:
        findings.append("buy/watch thresholds are close; core promotion may be noisy")
        recommendations.append("use streak-based promotion or widen the buy/watch gap")

    if not gates.get("require_entry_signal_for_buy", False):
        findings.append("BUY decisions do not require entry_signal")
        recommendations.append("enable scoring.decision_gates.require_entry_signal_for_buy")
    if gates.get("max_missing_fields_for_buy") is None:
        findings.append("BUY decisions do not cap missing data fields")
        recommendations.append("set max_missing_fields_for_buy to 0 or 1")

    scan_limit = int(screening.get("market_scan_limit", 0) or 0)
    if scan_limit and scan_limit < 100:
        findings.append(f"candidate scan limit is narrow: {scan_limit}")
        recommendations.append("use multiple candidate sources or raise scan coverage before ranking")

    promote_streak = int(pool.get("promote_streak_days", 0) or 0)
    if promote_streak >= 2:
        recommendations.append("enforce promote_streak_days in candidate refresh before core promotion")

    if auto_trade.get("enabled") and not auto_trade.get("dry_run", True):
        findings.append("auto_trade is enabled with dry_run=false")
        recommendations.append("keep execution boundary explicit: paper account only unless manually confirmed")

    need_multiple_profiles = bool(continuation and backtest_presets)
    if need_multiple_profiles:
        findings.append("strategy config mixes swing, continuation, and backtest presets")
        recommendations.append("split operating parameters into explicit profiles")

    status = "warning" if findings else "ok"
    return {
        "diagnostic": "strategy",
        "status": status,
        "findings": findings,
        "recommendations": _dedupe(recommendations),
        "inputs": {
            "weights": weights,
            "thresholds": thresholds,
            "decision_gates": gates,
            "screening": screening,
            "pool_management": pool,
            "auto_trade": auto_trade,
            "candidate_pool": candidate_pool,
            "config_errors": config_errors,
        },
        "parameter_profiles": {
            "current_profile": os.getenv("ASTOCK_CONFIG_PROFILE", "default"),
            "need_multiple_profiles": need_multiple_profiles,
            "reason": (
                "current config contains both medium-term scoring/backtest presets "
                "and short-continuation research parameters"
            ),
            "suggested": [
                {
                    "name": "trend_swing",
                    "purpose": "5-20 trading-day trend swing candidates",
                    "use_when": "market signal is GREEN/YELLOW and candidate has confirmed entry signal",
                    "key_parameters": {
                        "buy_threshold": 5.8,
                        "require_entry_signal_for_buy": True,
                        "max_missing_fields_for_buy": 1,
                        "promote_streak_days": 2,
                    },
                },
                {
                    "name": "short_continuation",
                    "purpose": "T+1 to T+3 momentum continuation research and paper validation",
                    "use_when": "strong tape, high amount, close near high, no overheat lock",
                    "key_parameters": {
                        "amount_min": continuation.get("filters", {}).get("amount_min", 2e8),
                        "top_n": continuation.get("scoring", {}).get("top_n", 3),
                        "hold_days": continuation.get("scoring", {}).get("hold_days", [1, 2, 3]),
                    },
                },
                {
                    "name": "defensive_watch",
                    "purpose": "weak market observation-only mode",
                    "use_when": "market signal is RED/CLEAR or core pool is empty",
                    "key_parameters": {
                        "execution_allowed": False,
                        "buy_threshold": 6.5,
                        "watch_threshold": 5.0,
                    },
                },
            ],
        },
    }


def explain_run(conn: Any, run_id: str) -> dict:
    """Explain one run using run_log plus events tied by metadata.run_id."""
    row = conn.execute("SELECT * FROM run_log WHERE run_id = ?", (run_id,)).fetchone()
    if not row:
        return {"status": "not_found", "run_id": run_id, "findings": ["run_id not found"]}

    run = dict(row)
    run["artifacts"] = _decode_json(run.pop("artifacts_json", None))
    events = conn.execute(
        """SELECT event_id, stream, stream_type, event_type, occurred_at, payload_json, metadata_json
           FROM event_log
           WHERE json_extract(metadata_json, '$.run_id') = ?
           ORDER BY occurred_at, stream_version
           LIMIT 200""",
        (run_id,),
    ).fetchall()
    event_items = []
    for event in events:
        item = dict(event)
        item["payload"] = _decode_json(item.pop("payload_json", None))
        item["metadata"] = _decode_json(item.pop("metadata_json", None))
        event_items.append(item)

    findings = []
    if run.get("status") == "failed":
        findings.append(run.get("error_message") or "run failed without an error message")
    elif run.get("status") == "running":
        findings.append("run is still marked running")
    elif run.get("status") == "completed":
        findings.append("run completed")
    else:
        findings.append(f"run status is {run.get('status')}")

    return {
        "status": "explained",
        "run_id": run_id,
        "run": run,
        "events": event_items,
        "findings": findings,
    }


def propose_agent_trade_plan(conn: Any) -> dict:
    """Create a non-executing Agent trade plan from current diagnostics."""
    diagnostics = diagnose_health(conn)
    data_source_diagnosis = build_data_source_diagnosis(conn)
    data_source_blockers = data_source_blockers_for_new_trades(data_source_diagnosis)
    actions: list[dict] = []

    data_sources = diagnostics["inputs"]["data_sources"]
    pool = diagnostics["inputs"]["candidate_pool"]
    if data_sources["status"] == "failed":
        actions.append({
            "type": "refresh_data_sources",
            "priority": "high",
            "reason": "required market data sources are unavailable",
        })
    non_required_data_blockers = [
        item
        for item in data_source_blockers
        if item.get("reason") != "required_data_sources_unavailable"
    ]
    if non_required_data_blockers:
        actions.append({
            "type": "inspect_data_sources",
            "priority": "high",
            "reason": data_source_blocker_summary(non_required_data_blockers),
        })
    if pool["total"] == 0 or pool["stale"]:
        actions.append({
            "type": "refresh_candidates",
            "priority": "high",
            "reason": "candidate pool is empty or stale",
        })
    if pool["core_count"] == 0:
        actions.append({
            "type": "review_core_pool",
            "priority": "high",
            "reason": "auto_trade buy-side requires fresh core candidates",
        })
    if not actions:
        actions.append({
            "type": "run_scoring_review",
            "priority": "normal",
            "reason": "inputs are available for read-only decision review",
        })

    return {
        "status": "proposed",
        "plan_type": "agent_trade_plan",
        "execution_allowed": False,
        "diagnostics": diagnostics,
        "data_source_diagnosis": data_source_diagnosis,
        "data_source_blockers": data_source_blockers,
        "actions": actions,
        "guardrails": [
            "do not place real-money orders",
            "use bin/trade or bin/trade mcp only",
            "require confirmation for state-changing MCP tools",
        ],
    }


def _dedupe(items: list[str]) -> list[str]:
    seen = set()
    result = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result
