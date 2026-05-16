"""Read-only diagnostics used by Agent-facing CLI and MCP tools."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from astock_trading.market.health import evaluate_data_source_health
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
    data_sources = evaluate_data_source_health(conn)
    candidate_pool = candidate_pool_summary(conn)
    failed_runs = conn.execute(
        """SELECT run_id, run_type, started_at, error_message
           FROM run_log
           WHERE status = 'failed'
           ORDER BY started_at DESC
           LIMIT 10"""
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

    if candidate_pool["total"] == 0:
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
            "running_runs": [dict(row) for row in running_runs],
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
    actions: list[dict] = []

    data_sources = diagnostics["inputs"]["data_sources"]
    pool = diagnostics["inputs"]["candidate_pool"]
    if data_sources["status"] == "failed":
        actions.append({
            "type": "refresh_data_sources",
            "priority": "high",
            "reason": "required market data sources are unavailable",
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
        "actions": actions,
        "guardrails": [
            "do not place real-money orders",
            "use bin/trade or bin/trade mcp only",
            "require confirmation for state-changing MCP tools",
        ],
    }
