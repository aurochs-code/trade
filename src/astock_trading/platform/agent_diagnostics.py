"""Read-only diagnostics used by Agent-facing CLI and MCP tools."""

from __future__ import annotations

from collections import Counter
import json
import os
import re
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from astock_trading.market.health import evaluate_data_source_health
from astock_trading.pipeline.strategy_profiles import latest_strategy_profile_activation_request
from astock_trading.platform.candidate_evidence import enrich_candidate_rows_with_latest_scores
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.data_source_diagnostics import (
    build_data_source_diagnosis,
    data_source_blocker_summary,
    data_source_blockers_for_new_trades,
)
from astock_trading.platform.pipeline_policy import filter_unrecovered_failed_runs
from astock_trading.platform.paths import resolve_config_dir
from astock_trading.platform.runtime_env import candidate_env_files, parse_env_file
from astock_trading.platform.time import MARKET_TZ, is_market_weekday, utc_now


INTRADAY_CATCHUP_SCRIPTS = {
    "a_stock_intraday_execution_cycle_silent.sh",
    "a_stock_pipeline_auto_trade_silent.sh",
}
TRACKED_A_STOCK_SCRIPTS = INTRADAY_CATCHUP_SCRIPTS | {
    "a_stock_screener_refresh_intraday_silent.sh",
    "a_stock_paper_trial_cycle_silent.sh",
    "a_stock_pipeline_auto_trade_silent.sh",
}
NEXT_WINDOW_STEP_SCRIPTS = {
    "a_stock_screener_refresh_intraday_silent.sh",
    "a_stock_pipeline_auto_trade_silent.sh",
    "a_stock_intraday_execution_cycle_silent.sh",
}
NEXT_WINDOW_FIRST_RUN_LOG_PATHS = [
    "~/Documents/a-stock-trading/logs/cron/",
    "~/.hermes/profiles/trading/logs/errors.log",
]


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


def candidate_pool_summary(
    conn: Any,
    *,
    now: datetime | None = None,
    max_age_days: int = 3,
    max_execution_age_hours: int = 24,
) -> dict:
    """Return a small candidate-pool freshness summary."""
    now = now or utc_now()
    rows = conn.execute(
        """SELECT pool_tier,
                  COUNT(*) AS count,
                  MAX(COALESCE(NULLIF(last_scored_at, ''), added_at)) AS last_scored_at
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

    execution_freshness = _candidate_pool_execution_freshness(
        latest_scored_at=latest_scored_at,
        raw_latest_scored_at=latest_scored_at.isoformat() if latest_scored_at else None,
        now=now,
        max_age_hours=max_execution_age_hours,
    )
    core_count = tiers.get("core", {}).get("count", 0)
    radar_count = tiers.get("radar", {}).get("count", 0)
    status = "warning" if total == 0 or core_count == 0 or stale or not execution_freshness["fresh"] else "ok"
    return {
        "status": status,
        "total": total,
        "core_count": core_count,
        "watch_count": tiers.get("watch", {}).get("count", 0),
        "radar_count": radar_count,
        "tier_counts": {tier: item["count"] for tier, item in tiers.items()},
        "latest_scored_at": latest_scored_at.isoformat() if latest_scored_at else None,
        "age_days": age_days,
        "max_age_days": max_age_days,
        "stale": stale,
        "execution_freshness": execution_freshness,
    }


def _candidate_pool_execution_freshness(
    *,
    latest_scored_at: datetime | None,
    raw_latest_scored_at: str | None,
    now: datetime,
    max_age_hours: int,
) -> dict:
    age_hours = (now - latest_scored_at).total_seconds() / 3600 if latest_scored_at else None
    fresh = age_hours is not None and age_hours <= max_age_hours
    blocker = {}
    if not fresh:
        blocker = {
            "reason": "scoring_inputs_stale",
            "label": "候选池评分已过期",
        }
    return {
        "scope": "paper_auto_readiness",
        "latest_scored_at": raw_latest_scored_at,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "max_age_hours": max_age_hours,
        "fresh": fresh,
        "freshness_status": "fresh" if fresh else "stale",
        "blocker": blocker,
    }


def _candidate_pool_execution_max_age_hours(strategy: dict[str, Any] | None) -> int:
    strategy = strategy or {}
    auto_trade = strategy.get("auto_trade", {}) or {}
    guard_cfg = auto_trade.get("buy_guard", {}) or {}
    scoring_cfg = strategy.get("scoring", {}) or {}
    for value in (
        guard_cfg.get("max_age_hours"),
        auto_trade.get("candidate_pool_max_age_hours"),
        scoring_cfg.get("max_age_hours"),
        scoring_cfg.get("freshness_max_age_hours"),
    ):
        if value:
            return int(value)
    return 24


def diagnose_health(conn: Any) -> dict:
    """Build a read-only health diagnosis for Agent orchestration."""
    now = utc_now()
    recent_failed_cutoff = (now.replace(microsecond=0) - timedelta(days=3)).isoformat()
    data_sources = evaluate_data_source_health(conn)
    candidate_pool = candidate_pool_summary(conn)
    failed_run_rows = conn.execute(
        """SELECT run_id, run_type, started_at, error_message
           FROM run_log
           WHERE status = 'failed'
             AND started_at >= ?
           ORDER BY started_at DESC
           LIMIT 10""",
        (recent_failed_cutoff,),
    ).fetchall()
    successful_run_rows = conn.execute(
        """SELECT run_id, run_type, started_at
           FROM run_log
           WHERE status = 'completed'
             AND started_at >= ?
           ORDER BY started_at DESC
           LIMIT 50""",
        (recent_failed_cutoff,),
    ).fetchall()
    recent_failed_runs = [dict(row) for row in failed_run_rows]
    successful_runs = [dict(row) for row in successful_run_rows]
    failed_runs = filter_unrecovered_failed_runs(recent_failed_runs, successful_runs)
    current_failed_ids = {run.get("run_id") for run in failed_runs}
    recovered_failed_runs = [
        run for run in recent_failed_runs
        if run.get("run_id") not in current_failed_ids
    ]
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
            "failed_runs": failed_runs,
            "recovered_failed_runs": recovered_failed_runs,
            "historical_failed_runs": [dict(row) for row in historical_failed_runs],
            "running_runs": [dict(row) for row in running_runs],
        },
    }


def diagnose_schedule(
    conn: Any,
    *,
    jobs_path: Path | str | None = None,
    env_file: Path | str | None = None,
    now: datetime | None = None,
) -> dict:
    """诊断 Hermes trading profile 关键盘中调度是否按预期运行；只读。"""
    current = now or utc_now()
    current_local = current.astimezone(MARKET_TZ) if current.tzinfo else current.replace(tzinfo=MARKET_TZ)
    resolved_jobs_path = _resolve_hermes_jobs_path(jobs_path)
    runtime_profile = _schedule_runtime_profile_state(conn, env_file=env_file)
    runtime_contract = _schedule_runtime_contract(resolved_jobs_path, [])
    next_action = {
        "type": "inspect_hermes_trading_profile",
        "label": "检查 Hermes trading 调度",
        "command": "atrade diagnose schedule --json",
        "safe_to_auto_apply": True,
        **_action_contract("diagnose_schedule"),
    }
    if runtime_profile.get("status") == "review_required":
        next_action = _runtime_profile_next_action(runtime_profile)

    if resolved_jobs_path is None or not resolved_jobs_path.exists():
        intraday_simulation = _schedule_intraday_simulation_state(
            tracked_jobs=[],
            runtime_profile=runtime_profile,
            runtime_contract=runtime_contract,
        )
        return {
            "diagnostic": "schedule",
            "status": "unknown",
            "summary": "未找到 Hermes trading 调度配置，无法确认盘中兜底任务是否运行。",
            "checked_at": current_local.isoformat(),
            "source": {"jobs_path": str(resolved_jobs_path or "")},
            "runtime_profile": runtime_profile,
            "runtime_contract": runtime_contract,
            "tracked_jobs": [],
            "missed_jobs": [],
            "failed_jobs": [],
            "disabled_jobs": [],
            "pending_first_run_jobs": [],
            "intraday_simulation": intraday_simulation,
            "next_action": next_action,
            "guardrails": {
                "read_only": True,
                "modifies_schedule": False,
                "runs_jobs": False,
            },
        }

    try:
        raw = json.loads(resolved_jobs_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        intraday_simulation = _schedule_intraday_simulation_state(
            tracked_jobs=[],
            runtime_profile=runtime_profile,
            runtime_contract=runtime_contract,
        )
        return {
            "diagnostic": "schedule",
            "status": "failed",
            "summary": f"Hermes 调度配置读取失败: {exc}",
            "checked_at": current_local.isoformat(),
            "source": {"jobs_path": str(resolved_jobs_path)},
            "runtime_profile": runtime_profile,
            "runtime_contract": runtime_contract,
            "tracked_jobs": [],
            "missed_jobs": [],
            "failed_jobs": [],
            "disabled_jobs": [],
            "pending_first_run_jobs": [],
            "intraday_simulation": intraday_simulation,
            "next_action": next_action,
            "guardrails": {
                "read_only": True,
                "modifies_schedule": False,
                "runs_jobs": False,
            },
        }

    jobs = raw.get("jobs", []) if isinstance(raw, dict) else []
    tracked_jobs = [
        _schedule_job_state(job, now=current_local)
        for job in jobs
        if _is_tracked_a_stock_job(job)
    ]
    runtime_contract = _schedule_runtime_contract(resolved_jobs_path, tracked_jobs)
    missed_jobs = [job for job in tracked_jobs if job.get("missed_today")]
    failed_jobs = [job for job in tracked_jobs if _schedule_job_failed(job)]
    disabled_jobs = [job for job in tracked_jobs if not job.get("enabled") or job.get("state") == "paused"]
    pending_first_run_jobs = [job for job in tracked_jobs if job.get("pending_first_run")]
    intraday_simulation = _schedule_intraday_simulation_state(
        tracked_jobs=tracked_jobs,
        runtime_profile=runtime_profile,
        runtime_contract=runtime_contract,
    )
    runtime_profile_requires_review = runtime_profile.get("status") == "review_required"
    runtime_contract_requires_review = runtime_contract.get("status") == "warning"
    status = "warning" if (
        missed_jobs
        or failed_jobs
        or disabled_jobs
        or runtime_profile_requires_review
        or runtime_contract_requires_review
    ) else "ok"
    if missed_jobs:
        summary = f"发现 {len(missed_jobs)} 个 A 股关键盘中调度今天应运行但未运行。"
    elif failed_jobs:
        summary = f"发现 {len(failed_jobs)} 个 A 股关键调度最近运行失败。"
    elif disabled_jobs:
        summary = f"发现 {len(disabled_jobs)} 个 A 股关键盘中调度未启用或已暂停。"
    elif runtime_contract_requires_review:
        summary = runtime_contract.get("summary", "Hermes trading 脚本运行合约需要复核。")
    elif runtime_profile_requires_review:
        summary = runtime_profile.get(
            "message",
            "Hermes trading 调度存在运行 profile 待人工复核。",
        )
    elif pending_first_run_jobs:
        summary = f"Hermes trading 关键盘中调度未发现漏跑；{len(pending_first_run_jobs)} 个任务等待首次运行。"
    else:
        summary = "Hermes trading 关键盘中调度未发现漏跑。"

    return {
        "diagnostic": "schedule",
        "status": status,
        "summary": summary,
        "checked_at": current_local.isoformat(),
        "source": {
            "jobs_path": str(resolved_jobs_path),
            "profile": "trading",
        },
        "runtime_profile": runtime_profile,
        "runtime_contract": runtime_contract,
        "tracked_jobs": tracked_jobs,
        "missed_jobs": missed_jobs,
        "failed_jobs": failed_jobs,
        "disabled_jobs": disabled_jobs,
        "pending_first_run_jobs": pending_first_run_jobs,
        "intraday_simulation": intraday_simulation,
        "next_action": next_action,
        "guardrails": {
            "read_only": True,
            "modifies_schedule": False,
            "runs_jobs": False,
        },
    }


def _schedule_intraday_simulation_state(
    *,
    tracked_jobs: list[dict[str, Any]],
    runtime_profile: dict[str, Any],
    runtime_contract: dict[str, Any] | None = None,
) -> dict[str, Any]:
    runtime_contract = runtime_contract or _unknown_runtime_contract("未检查脚本运行合约。")
    scheduled_steps = _next_window_scheduled_steps({"tracked_jobs": tracked_jobs})
    first_run_verification = build_next_window_first_run_verification(scheduled_steps)
    step_scripts = set(NEXT_WINDOW_STEP_SCRIPTS)
    step_jobs = [job for job in tracked_jobs if str(job.get("script") or "") in step_scripts]
    disabled_step_jobs = [
        job for job in step_jobs if not job.get("enabled") or str(job.get("state") or "") == "paused"
    ]
    missed_critical_jobs = [
        job for job in step_jobs if job.get("critical_for_intraday_simulation") and job.get("missed_today")
    ]
    failed_critical_jobs = [
        job for job in step_jobs if job.get("critical_for_intraday_simulation") and _schedule_job_failed(job)
    ]
    critical_job_count = sum(1 for step in scheduled_steps if step.get("critical_for_intraday_simulation"))
    pending_first_run_critical_count = sum(
        1
        for step in scheduled_steps
        if step.get("pending_first_run") and step.get("critical_for_intraday_simulation")
    )
    profile_ready = runtime_profile.get("status") != "review_required"

    if not scheduled_steps or disabled_step_jobs or runtime_contract.get("status") == "warning":
        status = "schedule_attention_required"
    elif failed_critical_jobs:
        status = "critical_jobs_failed"
    elif runtime_profile.get("status") == "review_required":
        status = "profile_review_required"
    elif missed_critical_jobs:
        status = "critical_jobs_missed"
    elif pending_first_run_critical_count:
        status = "pending_first_run_verification"
    else:
        status = "ready"

    ready_for_next_window = status in {"ready", "pending_first_run_verification"}
    summary = _schedule_intraday_simulation_summary(
        status=status,
        scheduled_steps=scheduled_steps,
        critical_job_count=critical_job_count,
        pending_first_run_critical_count=pending_first_run_critical_count,
        failed_critical_count=len(failed_critical_jobs),
    )
    next_action = _schedule_intraday_simulation_next_action(status, runtime_profile)
    return {
        "status": status,
        "summary": summary,
        "profile_ready": profile_ready,
        "ready_for_next_window": ready_for_next_window,
        "scheduled_step_count": len(scheduled_steps),
        "critical_job_count": critical_job_count,
        "pending_first_run_critical_count": pending_first_run_critical_count,
        "scheduled_steps": scheduled_steps,
        "first_run_verification": first_run_verification,
        "runtime_contract": runtime_contract,
        "missed_critical_jobs": missed_critical_jobs,
        "failed_critical_jobs": failed_critical_jobs,
        "disabled_step_jobs": disabled_step_jobs,
        "next_action": next_action,
        "guardrails": {
            "read_only": True,
            "runs_jobs": False,
            "places_order": False,
            "writes_environment": False,
            "old_signal_auto_carryover": False,
        },
    }


def _schedule_intraday_simulation_summary(
    *,
    status: str,
    scheduled_steps: list[dict[str, Any]],
    critical_job_count: int,
    pending_first_run_critical_count: int,
    failed_critical_count: int = 0,
) -> str:
    if not scheduled_steps:
        return "未找到下个窗口的盘中候选刷新/模拟承接任务；需要先核查 Hermes 调度。"
    base = (
        f"下个窗口已排入 {len(scheduled_steps)} 个盘中候选/模拟承接步骤，"
        f"其中 {critical_job_count} 个会影响模拟承接"
    )
    if pending_first_run_critical_count:
        base = f"{base}，{pending_first_run_critical_count} 个关键模拟承接任务等待首次运行"
    if status == "profile_review_required":
        return f"{base}；运行 profile 仍需人工确认。"
    if status == "schedule_attention_required":
        return f"{base}；存在调度缺失、停用或暂停，需要先修调度。"
    if status == "critical_jobs_failed":
        return f"{base}；{failed_critical_count} 个关键模拟承接任务最近运行失败，需要先核查日志。"
    if status == "critical_jobs_missed":
        return f"{base}；已有关键模拟承接任务漏跑，需要核查日志。"
    if status == "pending_first_run_verification":
        return f"{base}；下个窗口后需要复核首次运行结果。"
    return f"{base}；调度侧已具备下个窗口承接条件。"


def _schedule_intraday_simulation_next_action(
    status: str,
    runtime_profile: dict[str, Any],
) -> dict[str, Any]:
    if status == "profile_review_required":
        return _runtime_profile_next_action(runtime_profile)
    if status in {
        "schedule_attention_required",
        "critical_jobs_failed",
        "critical_jobs_missed",
        "pending_first_run_verification",
    }:
        return {
            "type": "inspect_hermes_trading_profile",
            "label": "检查 Hermes trading 调度",
            "command": "atrade diagnose schedule --json",
            "safe_to_auto_apply": True,
            **_action_contract("diagnose_schedule"),
        }
    return {
        "type": "paper_auto_readiness",
        "label": "复核模拟承接预检",
        "command": "atrade paper auto-readiness --json",
        "safe_to_auto_apply": True,
        **_action_contract("paper_auto_readiness"),
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
    candidate_pool = candidate_pool_summary(
        conn,
        max_execution_age_hours=_candidate_pool_execution_max_age_hours(strategy),
    )
    candidate_flow = _strategy_candidate_flow(
        conn,
        thresholds=thresholds,
        candidate_pool=candidate_pool,
        auto_trade=auto_trade,
    )
    actionable_state = _strategy_actionable_state(candidate_flow)

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
    if promote_streak <= 0:
        findings.append("core promotion does not require a positive streak")
        recommendations.append("enable promote_streak_days before core promotion")

    if auto_trade.get("enabled") and not auto_trade.get("dry_run", True):
        recommendations.append("auto_trade 已处于 MX 模拟盘委托模式；真实交易仍需人工确认")

    flow_status = actionable_state.get("status")
    if flow_status == "no_candidate_flow":
        findings.append("当前缺少可观察候选流")
        recommendations.append("先运行 atrade screener refresh --json 生成候选、评分和决策证据")
    elif flow_status == "buy_signal_waiting_window":
        findings.append("已有买入意向，但自动模拟被买入窗口拦截")
        if actionable_state.get("schedule_gap"):
            findings.append("盘中候选-模拟兜底调度今天未实际运行")
            recommendations.append("先运行 atrade diagnose schedule --json 核查 14:12/14:24 兜底任务")
        else:
            recommendations.append("下次买入窗口内运行 atrade paper auto-readiness --json 核查是否可提交 MX 模拟委托")
    elif flow_status == "buy_signal_ready_for_auto_trade_check":
        recommendations.append("已有买入意向，先用 atrade paper auto-readiness --json 检查模拟盘窗口和风控状态")
    elif flow_status in {"entry_signal_observable", "observable_candidates"}:
        recommendations.append("已有可观察候选，使用 atrade paper trial-plan --json 保持影子试运行链路")

    need_multiple_profiles = bool(continuation and backtest_presets)
    current_profile = os.getenv("ASTOCK_CONFIG_PROFILE", "default")
    available_profiles = _available_strategy_profiles()
    required_profiles = {
        "trend_swing",
        "short_continuation",
        "weak_sideways",
        "defensive_watch",
    }
    profiles_available = required_profiles.issubset({item["name"] for item in available_profiles})
    market_regime_profile_guidance = _market_regime_profile_guidance(
        conn,
        current_profile=current_profile,
    )
    execution_profile = _strategy_execution_profile_state(
        conn,
        current_profile=current_profile,
        need_multiple_profiles=need_multiple_profiles,
        profiles_available=profiles_available,
        target_profile=str(market_regime_profile_guidance.get("activation_profile") or "trend_swing"),
    )
    actionable_state = _actionable_state_with_execution_profile(actionable_state, execution_profile)
    recommendations = _recommendations_with_execution_profile(recommendations, execution_profile)
    if need_multiple_profiles:
        if profiles_available:
            if current_profile == "default":
                findings.append("当前执行 profile 仍混合趋势波段、短线延续和回测预设")
                recommendations.append(
                    "profile 已存在；执行任务如需切到 trend_swing，必须先人工确认 ASTOCK_CONFIG_PROFILE"
                )
            else:
                recommendations.append("profile 已存在；继续用 strategy profiles 对比证据，不自动切换")
        else:
            findings.append("strategy config mixes swing, continuation, and backtest presets")
            recommendations.append("split operating parameters into explicit profiles")

    status = "warning" if findings else "ok"
    return {
        "diagnostic": "strategy",
        "status": status,
        "summary": _strategy_diagnosis_summary(
            candidate_flow=candidate_flow,
            actionable_state=actionable_state,
            execution_profile=execution_profile,
        ),
        "findings": findings,
        "recommendations": _dedupe(recommendations),
        "candidate_flow": candidate_flow,
        "market_regime_profile_guidance": market_regime_profile_guidance,
        "actionable_state": actionable_state,
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
        "execution_profile": execution_profile,
        "parameter_profiles": {
            "current_profile": current_profile,
            "need_multiple_profiles": need_multiple_profiles,
            "profiles_available": profiles_available,
            "available_profiles": available_profiles,
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
                    "name": "weak_sideways",
                    "purpose": "weak or sideways regime route-gated small-position validation",
                    "use_when": "confirmed YELLOW/RED market snapshot after intraday or close refresh",
                    "key_parameters": {
                        "execution_allowed": "route-gated only",
                        "yellow_routes": ["pullback_to_ma20", "volume_breakout"],
                        "red_routes": ["pullback_to_ma20"],
                        "auto_trade_dry_run": True,
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


_CONFIRMED_PROFILE_SWITCH_PHASES = {
    "noon",
    "intraday_monitor",
    "scoring",
    "evening",
    "auto_trade",
}


def _market_regime_profile_guidance(conn: Any, *, current_profile: str) -> dict[str, Any]:
    snapshot = _latest_market_history_snapshot(conn)
    market_signal = str((snapshot.get("payload") or {}).get("signal") or "")
    if not market_signal:
        market_signal = _dominant_projection_market_signal(conn)
    candidate_profile = _profile_for_market_signal(market_signal)
    phase = str(snapshot.get("phase") or "")
    switch_allowed = bool(
        market_signal
        and phase in _CONFIRMED_PROFILE_SWITCH_PHASES
        and candidate_profile != "default"
    )
    if phase == "morning":
        reason_key = "pre_market_reference_only"
        reason = "盘前快照只代表前收/缓存参考，不允许据此切换弱市/震荡策略。"
    elif not market_signal:
        reason_key = "market_signal_missing"
        reason = "暂无可确认的大盘综合信号，不生成策略切换建议。"
    elif switch_allowed:
        reason_key = "confirmed_market_snapshot"
        reason = "已取得盘中或收盘确认快照，可作为人工复核 profile 切换依据。"
    else:
        reason_key = "confirmed_snapshot_required"
        reason = "需要午间、盘中、评分或收盘快照确认后，才建议切换执行 profile。"

    fallback_activation = current_profile if current_profile != "default" else "trend_swing"
    activation_profile = candidate_profile if switch_allowed else fallback_activation
    return {
        "status": "ready" if switch_allowed else "reference_only",
        "market_signal": market_signal or "UNKNOWN",
        "snapshot_phase": phase or "",
        "snapshot_created_at": snapshot.get("created_at") or "",
        "candidate_profile": candidate_profile,
        "activation_profile": activation_profile,
        "current_profile": current_profile,
        "switch_allowed": switch_allowed,
        "reason_key": reason_key,
        "reason": reason,
        "guardrails": {
            "auto_switch_profile": False,
            "manual_approval_required": True,
            "pre_market_snapshot_can_switch": False,
        },
    }


def _latest_market_history_snapshot(conn: Any) -> dict[str, Any]:
    try:
        row = conn.execute(
            """SELECT phase, payload_json, created_at
               FROM signal_history_snapshots
               WHERE snapshot_type = 'market'
               ORDER BY created_at DESC
               LIMIT 1"""
        ).fetchone()
    except Exception:
        return {}
    if not row:
        return {}
    return {
        "phase": row["phase"],
        "created_at": row["created_at"],
        "payload": _decode_json(row["payload_json"]),
    }


def _dominant_projection_market_signal(conn: Any) -> str:
    try:
        rows = conn.execute(
            """SELECT `signal`, COUNT(*) AS count
               FROM projection_market_state
               WHERE `signal` IS NOT NULL AND `signal` != ''
               GROUP BY `signal`
               ORDER BY count DESC
               LIMIT 1"""
        ).fetchall()
    except Exception:
        return ""
    if not rows:
        return ""
    return str(rows[0]["signal"] or "")


def _profile_for_market_signal(signal: str) -> str:
    return {
        "GREEN": "trend_swing",
        "YELLOW": "weak_sideways",
        "RED": "weak_sideways",
        "CLEAR": "defensive_watch",
    }.get(str(signal or ""), "trend_swing")


def _strategy_diagnosis_summary(
    *,
    candidate_flow: dict[str, Any],
    actionable_state: dict[str, Any],
    execution_profile: dict[str, Any],
) -> str:
    pool = candidate_flow.get("pool", {}) or {}
    total = int(pool.get("total", 0) or 0)
    core = int(pool.get("core_count", 0) or 0)
    watch = int(pool.get("watch_count", 0) or 0)
    radar = int(pool.get("radar_count", 0) or 0)
    flow_summary = actionable_state.get("summary") or "策略候选流状态已汇总。"
    summary = f"候选池 {total} 只（核心 {core}、观察 {watch}、强势观察 {radar}）；{flow_summary}"
    if execution_profile.get("status") == "review_required" and execution_profile.get("message"):
        summary = f"{_strip_cn_period(summary)}；{execution_profile['message']}"
    return summary


def _strip_cn_period(value: str) -> str:
    return value.rstrip("。")


def diagnose_flow(
    conn: Any,
    *,
    opportunity: dict[str, Any] | None = None,
    auto_readiness: dict[str, Any] | None = None,
) -> dict:
    """汇总候选召回、策略闸门和模拟承接状态；只读，不下单。"""
    strategy = diagnose_strategy(conn)
    candidate_flow = strategy.get("candidate_flow", {}) or {}
    schedule = (candidate_flow.get("automation", {}) or {}).get("schedule") or diagnose_schedule(conn)
    if opportunity is None:
        opportunity = _build_flow_opportunity_card(conn)

    flow_stage = _diagnose_candidate_flow_stage(
        strategy=strategy,
        opportunity=opportunity,
        auto_readiness=auto_readiness or {},
    )
    status = _candidate_flow_status(flow_stage, opportunity)
    next_action = flow_stage.get("next_action") or (strategy.get("actionable_state", {}) or {}).get(
        "next_action",
        {},
    )
    pool = candidate_flow.get("pool", {}) or {}
    approval_gate = _candidate_flow_approval_gate(flow_stage)
    after_approval_preview = _candidate_flow_after_approval_preview(
        approval_gate=approval_gate,
        auto_readiness=auto_readiness or {},
    )
    next_window_plan = _candidate_flow_next_window_plan(
        strategy=strategy,
        schedule=schedule,
        auto_readiness=auto_readiness or {},
        approval_gate=approval_gate,
    )

    return {
        "diagnostic": "candidate_flow",
        "status": status,
        "summary": _candidate_flow_summary(
            flow_stage=flow_stage,
            candidate_pool=pool,
            opportunity=opportunity,
        ),
        "checked_at": utc_now().isoformat(),
        "findings": strategy.get("findings", []) or [],
        "recommendations": strategy.get("recommendations", []) or [],
        "flow_stage": flow_stage,
        "next_action": next_action,
        "approval_gate": approval_gate,
        "after_approval_preview": after_approval_preview,
        "next_window_plan": next_window_plan,
        "candidate_summary": candidate_flow.get("candidate_summary", {}) or {},
        "current_entry_signals": candidate_flow.get("current_entry_signals", []) or [],
        "candidate_pool": {
            **pool,
            "current_candidates": candidate_flow.get("current_candidates", []) or [],
            "current_entry_signals": candidate_flow.get("current_entry_signals", []) or [],
        },
        "strategy": {
            "status": strategy.get("status", "unknown"),
            "findings": strategy.get("findings", []) or [],
            "recommendations": strategy.get("recommendations", []) or [],
            "candidate_flow": candidate_flow,
            "actionable_state": strategy.get("actionable_state", {}) or {},
            "execution_profile": strategy.get("execution_profile", {}) or {},
        },
        "opportunity": opportunity,
        "auto_readiness": auto_readiness or {},
        "automation": {
            "schedule": schedule,
            "latest_auto_trade_summary": (candidate_flow.get("automation", {}) or {}).get("latest_summary")
            or {},
            "paper_trial": _paper_trial_flow(conn),
        },
        "guardrails": {
            "read_only": True,
            "runs_pipeline": False,
            "places_paper_order": False,
            "real_order_auto_execution_allowed": False,
            "manual_confirmation_required_for_real_trade": True,
        },
    }


def _available_strategy_profiles() -> list[dict[str, str]]:
    profile_dir = resolve_config_dir() / "profiles"
    if not profile_dir.exists():
        return []
    profiles = []
    for path in sorted(profile_dir.glob("*.yaml")):
        profiles.append({
            "name": path.stem,
            "path": str(path),
        })
    return profiles


def _schedule_runtime_profile_state(
    conn: Any,
    *,
    env_file: Path | str | None = None,
) -> dict[str, Any]:
    target_profile = "trend_swing"
    process_profile = os.getenv("ASTOCK_CONFIG_PROFILE", "default") or "default"
    env_profile_state = _runtime_env_profile(env_file)
    env_profile = env_profile_state.get("env_profile")
    effective_profile = process_profile if process_profile != "default" else env_profile or process_profile
    source_type = "process" if process_profile != "default" else env_profile_state.get("source_type", "default")
    latest_request = latest_strategy_profile_activation_request(conn, target_profile=target_profile)
    activation_status = "recorded" if latest_request else "missing"
    needs_review = bool(latest_request) and effective_profile != target_profile
    if needs_review:
        status = "review_required"
        message = (
            f"已记录 {target_profile} profile 激活计划，但 Hermes/atrade 运行环境当前仍会使用 "
            f"{effective_profile}；下个盘中模拟前需要人工确认 ASTOCK_CONFIG_PROFILE。"
        )
    elif latest_request:
        status = "ok"
        message = f"已记录 {target_profile} profile 激活计划，运行环境会使用该 profile。"
    else:
        status = "not_requested" if effective_profile == "default" else "ok"
        message = (
            "尚未记录 profile 激活计划，调度按 default profile 执行。"
            if status == "not_requested"
            else "运行环境已显式设置执行 profile。"
        )
    return {
        "status": status,
        "safe_to_auto_apply": not needs_review,
        "current_process_profile": process_profile,
        "env_profile": env_profile,
        "effective_profile": effective_profile,
        "effective_profile_source": source_type,
        "recommended_profile": target_profile,
        "activation_request_status": activation_status,
        "latest_activation_request": latest_request,
        "source": env_profile_state.get("source", {}),
        "message": message,
    }


def _runtime_env_profile(env_file: Path | str | None = None) -> dict[str, Any]:
    if env_file:
        candidates = [Path(env_file).expanduser()]
    else:
        candidates = candidate_env_files()
    selected = next((candidate for candidate in candidates if candidate.exists()), candidates[0] if candidates else None)
    source = {
        "env_file": str(selected or ""),
        "env_file_exists": bool(selected and selected.exists()),
        "profile_key_present": False,
    }
    if selected is None or not selected.exists():
        return {
            "env_profile": None,
            "source_type": "default",
            "source": source,
        }
    try:
        values = parse_env_file(selected)
    except OSError as exc:
        source["read_error"] = str(exc)
        return {
            "env_profile": None,
            "source_type": "default",
            "source": source,
        }
    profile = values.get("ASTOCK_CONFIG_PROFILE") or None
    source["profile_key_present"] = "ASTOCK_CONFIG_PROFILE" in values
    return {
        "env_profile": profile,
        "source_type": "env_file" if profile else "default",
        "source": source,
    }


def _runtime_profile_next_action(runtime_profile: dict[str, Any]) -> dict[str, Any]:
    recommended_profile = runtime_profile.get("recommended_profile") or "trend_swing"
    return {
        "type": "review_runtime_profile_activation",
        "label": "复核运行 profile 激活",
        "command": f"atrade strategy profile-activation --target {recommended_profile} --json",
        "safe_to_auto_apply": False,
        **_action_contract("strategy_profile_activation_review"),
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


def _strategy_execution_profile_state(
    conn: Any,
    *,
    current_profile: str,
    need_multiple_profiles: bool,
    profiles_available: bool,
    target_profile: str = "trend_swing",
) -> dict[str, Any]:
    if current_profile == "default" and need_multiple_profiles and profiles_available:
        latest_request = latest_strategy_profile_activation_request(
            conn,
            target_profile=target_profile,
        )
        return {
            "current_profile": current_profile,
            "status": "review_required",
            "safe_to_auto_apply": False,
            "recommended_profile": target_profile,
            "activation_request_status": "recorded" if latest_request else "missing",
            "latest_activation_request": latest_request,
            "message": "当前仍在 default 混合配置；自动模拟前需要人工确认执行 profile。",
        }
    return {
        "current_profile": current_profile,
        "status": "ok",
        "safe_to_auto_apply": True,
        "recommended_profile": current_profile,
        "activation_request_status": "not_required",
        "latest_activation_request": {},
        "message": "执行 profile 已明确。",
    }


def _actionable_state_with_execution_profile(
    actionable_state: dict[str, Any],
    execution_profile: dict[str, Any],
) -> dict[str, Any]:
    if execution_profile.get("status") != "review_required":
        return actionable_state
    result = dict(actionable_state)
    result["execution_profile"] = execution_profile
    if result.get("schedule_gap"):
        return result
    recommended_profile = execution_profile.get("recommended_profile") or "trend_swing"
    if execution_profile.get("latest_activation_request"):
        result["next_action"] = {
            "type": "review_recorded_profile_activation",
            "label": "复核已记录的 profile 激活计划",
            "command": f"atrade strategy profile-activation --target {recommended_profile} --json",
            "safe_to_auto_apply": False,
            **_action_contract("strategy_profile_activation_review"),
        }
    else:
        result["next_action"] = {
            "type": "generate_profile_activation_plan",
            "label": "生成执行 profile 激活计划",
            "command": f"atrade strategy profile-activation --target {recommended_profile} --json",
            "safe_to_auto_apply": False,
            **_action_contract("strategy_profile_activation_review"),
        }
    return result


def _recommendations_with_execution_profile(
    recommendations: list[str],
    execution_profile: dict[str, Any],
) -> list[str]:
    if execution_profile.get("status") != "review_required":
        return recommendations
    filtered = [
        item for item in recommendations
        if item != "下次买入窗口内运行 atrade paper auto-readiness --json 核查是否可提交 MX 模拟委托"
    ]
    recommended_profile = execution_profile.get("recommended_profile") or "trend_swing"
    if execution_profile.get("latest_activation_request"):
        filtered.append(f"先复核已记录的 {recommended_profile} profile 激活计划，再等下个买入窗口预检")
    else:
        filtered.append(f"先生成并人工确认 {recommended_profile} profile 激活计划，再等下个买入窗口预检")
    return filtered


def _build_flow_opportunity_card(conn: Any) -> dict[str, Any]:
    try:
        from astock_trading.platform.hermes_commands import build_opportunity_card

        return build_opportunity_card(conn)
    except Exception as exc:
        return {
            "status": "unavailable",
            "summary": f"机会卡读取失败：{exc}",
            "counts": {},
            "blockers": ["机会卡读取失败"],
            "next_action": {
                "type": "inspect_strategy",
                "label": "检查策略诊断",
                "command": "atrade diagnose strategy --json",
                "safe_to_auto_apply": True,
            },
        }


def _diagnose_candidate_flow_stage(
    *,
    strategy: dict[str, Any],
    opportunity: dict[str, Any],
    auto_readiness: dict[str, Any],
) -> dict[str, Any]:
    actionable_state = strategy.get("actionable_state", {}) or {}
    execution_profile = actionable_state.get("execution_profile") or strategy.get("execution_profile") or {}
    if execution_profile.get("status") == "review_required":
        recommended = execution_profile.get("recommended_profile") or "trend_swing"
        signal_status = str(actionable_state.get("status") or "")
        recent_unusable_buy_signal = auto_readiness.get("recent_unusable_buy_signal", {}) or {}
        return {
            "status": "profile_review_required",
            "label": "执行 profile 待人工确认",
            "summary": _profile_review_stage_summary(
                signal_status,
                recommended,
                recent_unusable_buy_signal=recent_unusable_buy_signal,
            ),
            "signal_status": signal_status,
            "recent_unusable_buy_signal": recent_unusable_buy_signal,
            "latest_activation_request": execution_profile.get("latest_activation_request") or {},
            "next_action": actionable_state.get("next_action") or {
                "type": "review_runtime_profile_activation",
                "label": "复核运行 profile 激活",
                "command": f"atrade strategy profile-activation --target {recommended} --json",
                "safe_to_auto_apply": False,
                **_action_contract("strategy_profile_activation_review"),
            },
        }

    schedule_gap = actionable_state.get("schedule_gap") or {}
    if schedule_gap:
        return {
            "status": "schedule_gap",
            "label": "盘中模拟承接调度缺口",
            "summary": actionable_state.get("summary", "") or "盘中候选和模拟兜底任务存在漏跑风险。",
            "schedule_gap": schedule_gap,
            "next_action": actionable_state.get("next_action") or {
                "type": "inspect_schedule",
                "label": "检查盘中模拟兜底调度",
                "command": "atrade diagnose schedule --json",
                "safe_to_auto_apply": True,
                **_action_contract("diagnose_schedule"),
            },
        }

    readiness_status = str(auto_readiness.get("status") or "")
    if readiness_status:
        return {
            "status": readiness_status,
            "label": _auto_readiness_stage_label(readiness_status),
            "summary": auto_readiness.get("summary") or actionable_state.get("summary", ""),
            "next_action": auto_readiness.get("next_action") or actionable_state.get("next_action") or {},
            "blockers": auto_readiness.get("blockers", []) or [],
        }

    action_status = str(actionable_state.get("status") or "")
    if action_status:
        return {
            "status": action_status,
            "label": _actionable_stage_label(action_status),
            "summary": actionable_state.get("summary", ""),
            "next_action": actionable_state.get("next_action") or {},
        }

    opportunity_status = str(opportunity.get("status") or "unknown")
    return {
        "status": opportunity_status,
        "label": "机会卡状态",
        "summary": opportunity.get("summary", ""),
        "next_action": opportunity.get("next_action") or {},
        }


def _profile_review_stage_summary(
    signal_status: str,
    recommended_profile: str,
    *,
    recent_unusable_buy_signal: dict[str, Any] | None = None,
) -> str:
    if signal_status in {"buy_signal_waiting_window", "buy_signal_ready_for_auto_trade_check"}:
        prefix = "候选和当日买入意向已经进入可观察链路"
    elif signal_status == "entry_signal_observable":
        prefix = "候选和入场信号已进入可观察链路，但尚未形成新鲜买入意向"
    elif signal_status == "observable_candidates":
        prefix = "候选池已进入可观察链路，但当前没有入场信号或新鲜买入意向"
    elif signal_status == "no_candidate_flow":
        prefix = "当前候选流尚未恢复"
    else:
        prefix = "候选流状态已汇总"
    recent_unusable_text = _recent_unusable_buy_signal_text(recent_unusable_buy_signal).rstrip("。")
    if recent_unusable_text:
        prefix = f"{prefix}；{recent_unusable_text}"
    return (
        f"{prefix}；当前 default 混合配置会阻断自动模拟；"
        f"先复核 {recommended_profile} profile 激活。"
    )


def _recent_unusable_buy_signal_text(signal: dict[str, Any] | None) -> str:
    if not signal or int(signal.get("count") or 0) <= 0:
        return ""
    top = signal.get("top", {}) or {}
    code = str(top.get("code") or "")
    name = str(top.get("name") or code)
    score = _format_score_text(top.get("score", 0))
    reason = top.get("unusable_reason_label") or top.get("unusable_reason") or "不满足当前承接窗口"
    return f"近期买入意向 {signal.get('count')} 条不可承接；最高分为 {name}({code}) {score} 分，原因：{reason}。"


def _format_score_text(value: Any) -> str:
    try:
        return f"{float(value):.1f}"
    except (TypeError, ValueError):
        return str(value)


def _auto_readiness_stage_label(status: str) -> str:
    labels = {
        "ready": "模拟承接预检通过",
        "waiting_window": "等待模拟买入窗口",
        "profile_review_required": "执行 profile 待人工确认",
        "blocked": "模拟承接仍被阻断",
        "shadow": "影子试运行模式",
        "disabled": "自动模拟未启用",
    }
    return labels.get(status, "模拟承接预检状态")


def _actionable_stage_label(status: str) -> str:
    labels = {
        "buy_signal_waiting_window": "买入意向等待窗口承接",
        "buy_signal_ready_for_auto_trade_check": "买入意向待模拟预检",
        "entry_signal_observable": "入场信号可观察",
        "observable_candidates": "候选池可观察",
        "no_candidate_flow": "暂无候选流",
    }
    return labels.get(status, "候选流状态")


def _candidate_flow_status(flow_stage: dict[str, Any], opportunity: dict[str, Any]) -> str:
    stage = str(flow_stage.get("status") or "")
    opportunity_status = str(opportunity.get("status") or "")
    if opportunity_status == "needs_health_check":
        return "failed"
    if stage in {
        "profile_review_required",
        "schedule_gap",
        "waiting_window",
        "blocked",
        "disabled",
        "no_candidate_flow",
        "buy_signal_waiting_window",
    }:
        return "warning"
    return "ok"


def _candidate_flow_summary(
    *,
    flow_stage: dict[str, Any],
    candidate_pool: dict[str, Any],
    opportunity: dict[str, Any],
) -> str:
    total = int(candidate_pool.get("total", 0) or 0)
    core = int(candidate_pool.get("core_count", 0) or 0)
    watch = int(candidate_pool.get("watch_count", 0) or 0)
    radar = int(candidate_pool.get("radar_count", 0) or 0)
    stage_summary = flow_stage.get("summary") or opportunity.get("summary") or "候选流状态已汇总。"
    return (
        f"候选池 {total} 只（核心 {core}、观察 {watch}、强势观察 {radar}）；"
        f"{stage_summary}"
    )


def _candidate_flow_approval_gate(flow_stage: dict[str, Any]) -> dict[str, Any]:
    if flow_stage.get("status") != "profile_review_required":
        return {"required": False}
    target_profile = "trend_swing"
    latest_request = flow_stage.get("latest_activation_request") or {}
    if latest_request.get("target_profile"):
        target_profile = str(latest_request.get("target_profile") or target_profile)
    return {
        "required": True,
        "type": "profile_activation_apply",
        "label": "人工确认写入运行 profile",
        "reason": (
            "当前 default 混合配置阻断自动模拟；"
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


def _candidate_flow_after_approval_preview(
    *,
    approval_gate: dict[str, Any],
    auto_readiness: dict[str, Any],
) -> dict[str, Any]:
    if not approval_gate.get("required"):
        return {"available": False}
    target_profile = str(approval_gate.get("target_profile") or "trend_swing")
    remaining_blockers = _remaining_readiness_blockers_after_profile(auto_readiness)
    if remaining_blockers:
        labels = "、".join(str(item.get("label") or item.get("reason") or "未知阻断") for item in remaining_blockers)
        summary = (
            f"人工批准并写入 {target_profile} 后，按当前只读预判还剩 {len(remaining_blockers)} "
            f"个非 profile 阻断：{labels}。"
        )
    else:
        summary = (
            f"人工批准并写入 {target_profile} 后，当前 readiness 没有显示额外非 profile 阻断；"
            "批准后仍需重新运行预检确认。"
        )
    buy_side = auto_readiness.get("buy_side", {}) or {}
    signal_gap = buy_side.get("signal_gap", {}) or {}
    current_entry_signals = buy_side.get("current_entry_signals", []) or []
    recent_unusable_buy_signal = auto_readiness.get("recent_unusable_buy_signal", {}) or {}
    extra_summary_parts: list[str] = []
    if signal_gap.get("summary"):
        extra_summary_parts.append(str(signal_gap["summary"]).rstrip("。"))
    recent_unusable_text = _recent_unusable_buy_signal_text(recent_unusable_buy_signal).rstrip("。")
    if recent_unusable_text:
        extra_summary_parts.append(recent_unusable_text)
    if extra_summary_parts:
        summary = f"{summary.rstrip('。')}；{'；'.join(extra_summary_parts)}。"

    payload = {
        "available": True,
        "target_profile": target_profile,
        "summary": summary,
        "preview_command": (
            f"ASTOCK_CONFIG_PROFILE={target_profile} "
            "atrade paper auto-readiness --skip-account --json"
        ),
        "post_approval_verify_command": "atrade paper auto-readiness --json",
        "schedule_verify_command": approval_gate.get("verify_command") or "atrade diagnose schedule --json",
        "remaining_blockers_from_current_readiness": remaining_blockers,
        "safe_to_auto_apply": True,
        "writes_environment": False,
        "places_order": False,
        "note": "这是基于当前 readiness 的只读预判，不会写 .env，也不会提交模拟委托。",
    }
    if current_entry_signals:
        payload["current_entry_signals"] = current_entry_signals
    if signal_gap:
        payload["signal_gap"] = signal_gap
    if recent_unusable_buy_signal:
        payload["recent_unusable_buy_signal"] = recent_unusable_buy_signal
    return payload


def _remaining_readiness_blockers_after_profile(auto_readiness: dict[str, Any]) -> list[dict[str, str]]:
    profile_blocker_reasons = {
        "profile_review_required",
        "execution_profile_review_required",
        "runtime_profile_review_required",
    }
    remaining: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def add_blocker(item: Any) -> None:
        if not isinstance(item, dict):
            return
        reason = str(item.get("reason") or "")
        if reason in profile_blocker_reasons:
            return
        label = str(item.get("label") or reason or "未知阻断")
        key = (reason, label)
        if key in seen:
            return
        seen.add(key)
        remaining.append({"reason": reason, "label": label})

    for item in auto_readiness.get("blockers", []) or []:
        add_blocker(item)

    buy_side = auto_readiness.get("buy_side", {}) or {}
    for item in buy_side.get("blockers", []) or []:
        add_blocker(item)
    if buy_side.get("status") == "waiting_window":
        add_blocker({"reason": "buy_window_closed", "label": "当前不在模拟买入窗口"})

    return remaining


def _candidate_flow_next_window_plan(
    *,
    strategy: dict[str, Any],
    schedule: dict[str, Any],
    auto_readiness: dict[str, Any],
    approval_gate: dict[str, Any],
) -> dict[str, Any]:
    remaining_reasons = {
        str(item.get("reason") or "")
        for item in _remaining_readiness_blockers_after_profile(auto_readiness)
    }
    buy_side = auto_readiness.get("buy_side", {}) or {}
    if buy_side.get("status") != "waiting_window" and "buy_window_closed" not in remaining_reasons:
        return {"available": False}

    auto_trade_cfg = ((strategy.get("inputs", {}) or {}).get("auto_trade", {}) or {})
    buy_window = auto_trade_cfg.get("buy_window") or {}
    start_time = _parse_hhmm(buy_window.get("start", "09:45")) or time(9, 45)
    end_time = _parse_hhmm(buy_window.get("end", "14:30")) or time(14, 30)
    current = _diagnostic_checked_at(auto_readiness)
    scheduled_steps = _next_window_scheduled_steps(schedule)
    next_window_date = _next_window_date_from_schedule(
        current=current,
        start_time=start_time,
        end_time=end_time,
        scheduled_steps=scheduled_steps,
    )
    window_start = datetime.combine(next_window_date, start_time, tzinfo=MARKET_TZ)
    window_end = datetime.combine(next_window_date, end_time, tzinfo=MARKET_TZ)

    decisions = ((strategy.get("candidate_flow", {}) or {}).get("decisions", {}) or {})
    current_signal = _next_window_current_signal(
        auto_readiness,
        fallback_signal=decisions.get("latest_usable_buy_signal") or decisions.get("latest_buy_signal") or {},
        next_window_date=next_window_date,
        end_time=end_time,
    )
    carries_signal = bool(current_signal.get("carries_to_next_window"))
    approval_required = bool(approval_gate.get("required"))
    if approval_required:
        status = "requires_profile_approval_before_next_window"
    elif not scheduled_steps:
        status = "schedule_attention_required"
    else:
        status = "waiting_scheduled_next_window"

    target_profile = str(approval_gate.get("target_profile") or "trend_swing")
    if approval_required:
        summary = (
            "当前买入窗口已关闭；今日买入意向不会跨日自动提交。"
            f"下个窗口前需要先人工确认 {target_profile} profile，并依赖盘中刷新和模拟承接任务"
            "重新形成同日信号。"
        )
        next_action = {
            "type": "review_runtime_profile_activation",
            "label": "先复核运行 profile 激活",
            "command": approval_gate.get("review_command")
            or f"atrade strategy profile-activation --target {target_profile} --json",
            "safe_to_auto_apply": False,
            **_action_contract("strategy_profile_activation_review"),
        }
    elif not scheduled_steps:
        summary = "当前买入窗口已关闭；未看到下个窗口的盘中刷新/模拟承接任务，需要先核查调度。"
        next_action = {
            "type": "inspect_schedule",
            "label": "检查 Hermes trading 调度",
            "command": "atrade diagnose schedule --json",
            "safe_to_auto_apply": True,
            **_action_contract("diagnose_schedule"),
        }
    else:
        summary = "当前买入窗口已关闭；等待下个买入窗口内重新刷新候选、形成同日买入意向并承接。"
        next_action = {
            "type": "paper_auto_readiness",
            "label": "下个窗口前复核模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "safe_to_auto_apply": True,
            **_action_contract("paper_auto_readiness"),
        }

    return {
        "available": True,
        "status": status,
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


def _diagnostic_checked_at(auto_readiness: dict[str, Any]) -> datetime:
    checked_at = _parse_dt(
        auto_readiness.get("checked_at")
        or (auto_readiness.get("window_state", {}) or {}).get("checked_at")
    )
    if checked_at is None:
        checked_at = utc_now()
    return checked_at.astimezone(MARKET_TZ) if checked_at.tzinfo else checked_at.replace(tzinfo=MARKET_TZ)


def _next_window_scheduled_steps(schedule: dict[str, Any]) -> list[dict[str, Any]]:
    steps = []
    for job in schedule.get("tracked_jobs", []) or []:
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
            "role": _next_window_step_role(script),
            "schedule": job.get("schedule", ""),
            "next_run_at": next_run_local.isoformat() if next_run_local else job.get("next_run_at"),
            "last_run_at": job.get("last_run_at"),
            "last_status": job.get("last_status"),
            "failure_diagnosis": job.get("failure_diagnosis") or {},
            "last_error_summary": job.get("last_error_summary", ""),
            "pending_first_run": bool(job.get("pending_first_run")),
            "critical_for_intraday_simulation": bool(job.get("critical_for_intraday_simulation")),
        })
    return sorted(steps, key=lambda item: item.get("next_run_at") or "")


def build_next_window_first_run_verification(scheduled_steps: list[dict[str, Any]]) -> dict[str, Any]:
    pending_steps = [
        {
            "name": step.get("name", ""),
            "script": step.get("script", ""),
            "role": step.get("role", ""),
            "next_run_at": step.get("next_run_at"),
            "critical_for_intraday_simulation": bool(step.get("critical_for_intraday_simulation")),
        }
        for step in scheduled_steps
        if step.get("pending_first_run")
    ]
    critical_count = sum(
        1
        for step in pending_steps
        if step.get("critical_for_intraday_simulation")
    )
    if pending_steps:
        summary = f"下个窗口后需要核查 {len(pending_steps)} 个首次运行任务"
        if critical_count:
            summary = f"{summary}，其中 {critical_count} 个会影响模拟承接。"
        else:
            summary = f"{summary}。"
    else:
        summary = "下个窗口盘中任务已有运行记录；继续按调度验证。"

    return {
        "required": bool(pending_steps),
        "critical_required": bool(critical_count),
        "summary": summary,
        "pending_steps": pending_steps,
        "verify_command": "atrade diagnose schedule --json",
        "verify_command_contract_id": "diagnose_schedule",
        "verify_command_contract": _command_contract("diagnose_schedule"),
        "safe_to_auto_apply": True,
        "log_paths": NEXT_WINDOW_FIRST_RUN_LOG_PATHS,
    }


def _next_window_step_role(script: str) -> str:
    roles = {
        "a_stock_screener_refresh_intraday_silent.sh": "refresh_candidates",
        "a_stock_pipeline_auto_trade_silent.sh": "auto_trade_check_or_submit_paper_order",
        "a_stock_intraday_execution_cycle_silent.sh": "refresh_and_auto_trade_cycle",
    }
    return roles.get(script, "scheduled_step")


def _next_window_date_from_schedule(
    *,
    current: datetime,
    start_time: time,
    end_time: time,
    scheduled_steps: list[dict[str, Any]],
) -> date:
    current_local = current.astimezone(MARKET_TZ) if current.tzinfo else current.replace(tzinfo=MARKET_TZ)
    future_dates: list[date] = []
    for step in scheduled_steps:
        next_run = _parse_dt(step.get("next_run_at"))
        if next_run is None:
            continue
        next_run_local = next_run.astimezone(MARKET_TZ) if next_run.tzinfo else next_run.replace(tzinfo=MARKET_TZ)
        if next_run_local.date() == current_local.date() and current_local.time() <= end_time:
            future_dates.append(current_local.date())
        elif next_run_local > current_local:
            future_dates.append(next_run_local.date())

    if current_local.time() < start_time:
        if current_local.weekday() < 5 and (
            not future_dates or current_local.date() in future_dates
        ):
            return current_local.date()
        if future_dates:
            return min(future_dates)
        return _next_weekday(current_local.date())

    if future_dates:
        return min(future_dates)
    return _next_weekday(current_local.date())


def _next_weekday(current_date: date) -> date:
    candidate = current_date + timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def _next_window_current_signal(
    auto_readiness: dict[str, Any],
    *,
    fallback_signal: dict[str, Any] | None = None,
    next_window_date: date,
    end_time: time,
) -> dict[str, Any]:
    buy_side = auto_readiness.get("buy_side", {}) or {}
    signal = (
        buy_side.get("top_signal")
        or (auto_readiness.get("fresh_buy_signal", {}) or {}).get("top")
        or fallback_signal
        or {}
    )
    if not signal:
        return {}
    occurred_at = str(signal.get("occurred_at") or "")
    occurred = _parse_dt(occurred_at)
    carries = False
    if occurred is not None:
        occurred_local = occurred.astimezone(MARKET_TZ) if occurred.tzinfo else occurred.replace(tzinfo=MARKET_TZ)
        carries = (
            occurred_local.date() == next_window_date
            and occurred_local.replace(second=0, microsecond=0).time() <= end_time
        )
    return {
        "code": signal.get("code", ""),
        "name": signal.get("name") or signal.get("code", ""),
        "occurred_at": occurred_at,
        "score": signal.get("score"),
        "carries_to_next_window": carries,
        "expires_reason": "买入意向只在产生当日且不晚于买入窗口结束时可被 auto_trade 承接",
    }


def _paper_trial_flow(conn: Any) -> dict[str, Any]:
    recorded = _recent_events(conn, "paper.trial.recorded", limit=20)
    reviewed = _recent_events(conn, "paper.trial.reviewed", limit=20)
    latest_reviews = _latest_payloads_by_code(reviewed)
    positive_reviews = [
        _paper_trial_event_summary(event)
        for event in sorted(latest_reviews, key=_paper_trial_review_priority)
        if _paper_trial_event_status(event) == "positive"
    ][:5]
    return {
        "recorded_count": _event_count(conn, "paper.trial.recorded"),
        "reviewed_count": _event_count(conn, "paper.trial.reviewed"),
        "latest_recorded": _paper_trial_event_summary(recorded[0]) if recorded else {},
        "latest_reviewed": _paper_trial_event_summary(reviewed[0]) if reviewed else {},
        "review_summary": _paper_trial_review_summary(latest_reviews),
        "positive_reviews": positive_reviews,
        "next_action": _paper_trial_next_action(recorded=recorded, positive_reviews=positive_reviews),
    }


def _event_count(conn: Any, event_type: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS count FROM event_log WHERE event_type = ?",
        (event_type,),
    ).fetchone()
    if not row:
        return 0
    try:
        return int(row["count"])
    except (KeyError, TypeError, ValueError):
        return int(row[0] or 0)


def _paper_trial_event_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    event_type = str(event.get("event_type") or "")
    status = payload.get("status") or payload.get("review_status")
    status_label = payload.get("status_label") or payload.get("review_status_label")
    if not status and event_type == "paper.trial.recorded":
        status = "recorded"
        status_label = "已记录"
    code = payload.get("code")
    event_id = event.get("event_id")
    result = {
        "event_id": event_id,
        "evidence_id": event_id,
        "event_type": event_type,
        "occurred_at": event.get("occurred_at"),
        "code": code,
        "name": payload.get("name"),
        "status": status,
        "status_label": status_label,
        "pool_tier": payload.get("pool_tier"),
        "score": _score_value(payload),
        "trial_date": payload.get("trial_date"),
        "review_date": payload.get("review_date"),
        "return_pct": payload.get("return_pct"),
        "current_pool_tier": payload.get("current_pool_tier"),
        "current_pool_tier_label": payload.get("current_pool_tier_label"),
        "current_entry_signal": payload.get("current_entry_signal"),
        "current_primary_strategy_route": payload.get("current_primary_strategy_route"),
        "current_primary_strategy_route_label": payload.get("current_primary_strategy_route_label"),
        "candidate_state_changed": payload.get("candidate_state_changed"),
        "candidate_state_change_label": payload.get("candidate_state_change_label"),
        "paper_order_submitted": bool(payload.get("paper_order_submitted")),
    }
    if code:
        result["review_command"] = f"atrade stock analyze {code} --json"
    return result


def _paper_trial_event_status(event: dict[str, Any]) -> str:
    payload = event.get("payload") or {}
    return str(payload.get("review_status") or payload.get("status") or "")


def _paper_trial_review_priority(event: dict[str, Any]) -> tuple[int, int, float, str]:
    payload = event.get("payload") or {}
    tier_rank = {
        "core": 0,
        "watch": 1,
        "radar": 2,
    }.get(str(payload.get("current_pool_tier") or ""), 3)
    entry_rank = 0 if _truthy(payload.get("current_entry_signal")) else 1
    return_pct = _score_value({"score": payload.get("return_pct")})
    return (tier_rank, entry_rank, -return_pct, str(payload.get("code") or ""))


def _paper_trial_review_summary(events: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "reviewed_candidate_count": len(events),
        "positive_count": 0,
        "flat_count": 0,
        "negative_count": 0,
        "pending_count": 0,
        "insufficient_price_count": 0,
        "price_anomaly_count": 0,
    }
    for event in events:
        status = _paper_trial_event_status(event)
        key = f"{status}_count"
        if key in counts:
            counts[key] += 1
    return counts


def _paper_trial_next_action(
    *,
    recorded: list[dict[str, Any]],
    positive_reviews: list[dict[str, Any]],
) -> dict[str, Any]:
    if positive_reviews:
        code = positive_reviews[0].get("code") or ""
        return {
            "type": "review_positive_trial",
            "label": "复核表现为正的影子候选",
            "command": f"atrade stock analyze {code} --json" if code else "atrade paper trial-review --json",
            "reason": "影子试运行收益为正，只能进入人工复核，不能自动晋级或下单。",
            "safe_to_auto_apply": True,
            **_action_contract("stock_analyze" if code else "paper_trial_review"),
        }
    command_contract_id = "paper_trial_review" if recorded else "paper_trial_plan"
    return {
        "type": "paper_trial_review" if recorded else "paper_trial_plan",
        "label": "复盘影子试运行" if recorded else "生成影子试运行计划",
        "command": "atrade paper trial-review --json" if recorded else "atrade paper trial-plan --json",
        "safe_to_auto_apply": True,
        **_action_contract(command_contract_id),
    }


def _resolve_hermes_jobs_path(jobs_path: Path | str | None = None) -> Path | None:
    if jobs_path:
        return Path(jobs_path).expanduser()
    env_path = os.getenv("ASTOCK_HERMES_JOBS_PATH")
    if env_path:
        return Path(env_path).expanduser()
    trading_path = Path.home() / ".hermes" / "profiles" / "trading" / "cron" / "jobs.json"
    if trading_path.exists():
        return trading_path
    default_path = Path.home() / ".hermes" / "cron" / "jobs.json"
    return default_path if default_path.exists() else trading_path


def _schedule_runtime_contract(jobs_path: Path | None, tracked_jobs: list[dict[str, Any]]) -> dict[str, Any]:
    env_loader = {
        "entrypoint": "atrade",
        "loads_env_file": True,
        "respects_astock_no_env_file": True,
        "source": "astock_trading.platform.runtime_env.load_runtime_env",
    }
    if jobs_path is None:
        return _unknown_runtime_contract("未找到 Hermes jobs.json，无法确认脚本运行合约。", env_loader=env_loader)

    script_dir = _hermes_script_dir_for_jobs_path(jobs_path)
    if not script_dir.exists():
        payload = _unknown_runtime_contract(
            "未找到 Hermes trading scripts 目录，无法确认脚本是否通过 atrade 加载运行 .env。",
            env_loader=env_loader,
        )
        payload["script_dir"] = str(script_dir)
        payload["script_dir_exists"] = False
        return payload

    script_names = sorted({
        str(job.get("script") or "")
        for job in tracked_jobs
        if str(job.get("script") or "") in NEXT_WINDOW_STEP_SCRIPTS
    })
    checks = [
        _schedule_script_runtime_check(script_dir, script_name, seen=set())
        for script_name in script_names
    ]
    blocking_issues = [
        {
            "script": check.get("script", ""),
            "reason": issue,
        }
        for check in checks
        for issue in check.get("issues", []) or []
    ]
    status = "warning" if blocking_issues else "ok"
    summary = (
        "Hermes trading 脚本会通过 atrade 入口加载运行 .env；profile 写入后可被后续调度读取。"
        if status == "ok"
        else f"发现 {len(blocking_issues)} 个 Hermes trading 脚本运行合约问题；先修脚本再判断 profile 能否承接。"
    )
    return {
        "status": status,
        "summary": summary,
        "script_dir": str(script_dir),
        "script_dir_exists": True,
        "scope": "next_window_simulation_scripts",
        "env_loader": env_loader,
        "script_checks": checks,
        "blocking_issues": blocking_issues,
        "guardrails": {
            "read_only": True,
            "modifies_scripts": False,
            "runs_jobs": False,
        },
    }


def _unknown_runtime_contract(message: str, *, env_loader: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "status": "unknown",
        "summary": message,
        "script_dir": "",
        "script_dir_exists": False,
        "env_loader": env_loader or {},
        "script_checks": [],
        "blocking_issues": [],
        "guardrails": {
            "read_only": True,
            "modifies_scripts": False,
            "runs_jobs": False,
        },
    }


def _hermes_script_dir_for_jobs_path(jobs_path: Path) -> Path:
    path = Path(jobs_path).expanduser()
    if path.parent.name == "cron":
        return path.parent.parent / "scripts"
    return path.parent / "scripts"


def _schedule_script_runtime_check(
    script_dir: Path,
    script_name: str,
    *,
    seen: set[str],
) -> dict[str, Any]:
    path = script_dir / script_name
    if script_name in seen:
        return {
            "script": script_name,
            "path": str(path),
            "exists": path.exists(),
            "uses_atrade_entrypoint": False,
            "delegated_scripts": [],
            "profile_env_file_loading_possible": True,
            "disables_env_file": False,
            "issues": [],
        }
    seen.add(script_name)
    if not path.exists():
        return {
            "script": script_name,
            "path": str(path),
            "exists": False,
            "uses_atrade_entrypoint": False,
            "delegated_scripts": [],
            "profile_env_file_loading_possible": False,
            "disables_env_file": False,
            "issues": ["script_missing"],
        }

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        return {
            "script": script_name,
            "path": str(path),
            "exists": True,
            "uses_atrade_entrypoint": False,
            "delegated_scripts": [],
            "profile_env_file_loading_possible": False,
            "disables_env_file": False,
            "issues": [f"script_unreadable:{exc}"],
        }

    delegated_scripts = sorted({
        match
        for match in re.findall(r"\ba_stock_[A-Za-z0-9_]+_silent\.sh\b", content)
        if match != script_name
    })
    delegated_checks = [
        _schedule_script_runtime_check(script_dir, delegated, seen=seen.copy())
        for delegated in delegated_scripts
    ]
    uses_atrade = bool(re.search(r"(^|[\s\"'(&;|])atrade([\s\"')]|$)", content))
    delegated_uses_atrade = any(check.get("uses_atrade_entrypoint") for check in delegated_checks)
    disables_env_file = bool(re.search(r"ASTOCK_NO_ENV_FILE\s*=\s*(1|true|yes)", content, re.IGNORECASE))
    delegated_issues = [
        f"{check.get('script')}:{issue}"
        for check in delegated_checks
        for issue in check.get("issues", []) or []
    ]
    issues = list(delegated_issues)
    if not uses_atrade and not delegated_uses_atrade:
        issues.append("atrade_entrypoint_not_found")
    if disables_env_file:
        issues.append("astock_no_env_file_set")
    profile_env_file_loading_possible = (
        bool(uses_atrade or delegated_uses_atrade)
        and not disables_env_file
        and not delegated_issues
    )
    return {
        "script": script_name,
        "path": str(path),
        "exists": True,
        "uses_atrade_entrypoint": bool(uses_atrade or delegated_uses_atrade),
        "delegated_scripts": delegated_scripts,
        "profile_env_file_loading_possible": profile_env_file_loading_possible,
        "disables_env_file": disables_env_file,
        "issues": issues,
    }


def _is_tracked_a_stock_job(job: dict[str, Any]) -> bool:
    script = str(job.get("script") or "")
    name = str(job.get("name") or "")
    return script in TRACKED_A_STOCK_SCRIPTS or name.startswith("A股")


def _schedule_job_state(job: dict[str, Any], *, now: datetime) -> dict[str, Any]:
    schedule_display = _schedule_display(job)
    expected_times = _expected_schedule_times_today(schedule_display, now=now)
    expected_passed = [item for item in expected_times if item <= now]
    last_run = _parse_dt(job.get("last_run_at"))
    last_run_local = last_run.astimezone(MARKET_TZ) if last_run and last_run.tzinfo else last_run
    created_at = _parse_dt(job.get("created_at"))
    created_at_local = created_at.astimezone(MARKET_TZ) if created_at and created_at.tzinfo else created_at
    ran_today = bool(last_run_local and last_run_local.date() == now.date())
    missed_times = [
        item.isoformat()
        for item in expected_passed
        if (created_at_local is None or item >= created_at_local)
        if not ran_today or (last_run_local and last_run_local < item)
    ]
    enabled = bool(job.get("enabled"))
    state = str(job.get("state") or "")
    missed_today = enabled and state != "paused" and bool(missed_times)
    pending_first_run = enabled and state != "paused" and last_run_local is None and not missed_today
    failure_diagnosis = _schedule_failure_diagnosis(job)
    return {
        "name": job.get("name", ""),
        "script": job.get("script", ""),
        "schedule": schedule_display,
        "enabled": enabled,
        "state": state,
        "created_at": job.get("created_at"),
        "updated_at": job.get("updated_at"),
        "paused_at": job.get("paused_at"),
        "last_run_at": job.get("last_run_at"),
        "last_status": job.get("last_status"),
        "failure_diagnosis": failure_diagnosis if _schedule_job_failed(job) else {},
        "last_error_summary": failure_diagnosis.get("summary", ""),
        "next_run_at": job.get("next_run_at"),
        "expected_times_passed": [item.isoformat() for item in expected_passed],
        "missed_times": missed_times,
        "missed_today": missed_today,
        "pending_first_run": pending_first_run,
        "critical_for_intraday_simulation": str(job.get("script") or "") in INTRADAY_CATCHUP_SCRIPTS,
    }


def _schedule_job_failed(job: dict[str, Any]) -> bool:
    status = str(job.get("last_status") or "").strip().lower()
    return status in {"error", "failed", "failure", "timeout", "cancelled"}


def _schedule_failure_diagnosis(job: dict[str, Any]) -> dict[str, Any]:
    raw_error = str(job.get("last_error") or "").strip()
    script = str(job.get("script") or "")
    name = str(job.get("name") or "")
    status = str(job.get("last_status") or "").strip().lower()
    summary = _schedule_error_summary(raw_error)
    exit_code = _schedule_error_exit_code(raw_error)
    log_path = _schedule_error_log_path(raw_error)
    error_type = _schedule_error_type(raw_error, status=status)
    return {
        "status": status or "unknown",
        "summary": summary,
        "exit_code": exit_code,
        "error_type": error_type,
        "log_path": log_path,
        "root_cause_hint": _schedule_error_root_cause_hint(
            error_type=error_type,
            script=script,
            name=name,
        ),
        "recovery_action": _schedule_failure_recovery_action(script=script, name=name),
    }


def _schedule_error_summary(raw_error: str, *, max_length: int = 220) -> str:
    if not raw_error:
        return ""
    lines = [
        line.strip()
        for line in raw_error.splitlines()
        if line.strip()
        and not line.strip().startswith("#")
        and "resource_tracker:" not in line
    ]
    summary = " | ".join(lines[:3]) if lines else raw_error.strip()
    return summary[:max_length].rstrip()


def _schedule_error_exit_code(raw_error: str) -> int | None:
    if not raw_error:
        return None
    match = re.search(r"\b(?:code|exit)=?\s*(\d{1,3})\b", raw_error, re.IGNORECASE)
    return int(match.group(1)) if match else None


def _schedule_error_log_path(raw_error: str) -> str:
    if not raw_error:
        return ""
    match = re.search(r"\blog=([^\s]+)", raw_error)
    return match.group(1) if match else ""


def _schedule_error_type(raw_error: str, *, status: str) -> str:
    lowered = raw_error.lower()
    if any(
        marker in lowered
        for marker in (
            "libmini_racer",
            "trace/bpt trap",
            "address_pool_manager",
            "check failed",
        )
    ):
        return "native_runtime_crash"
    if status == "timeout" or "timeout" in lowered or "timed out" in lowered:
        return "timeout"
    if "traceback" in lowered or "exception" in lowered:
        return "python_exception"
    if "script exited with code" in lowered or " exit=" in lowered:
        return "script_failure"
    return status or "unknown"


def _schedule_error_root_cause_hint(*, error_type: str, script: str, name: str) -> str:
    job_label = f"{script} {name}".lower()
    if error_type == "native_runtime_crash":
        if "screener_refresh" in job_label or "候选" in name:
            return (
                "候选池刷新进程在 libmini_racer 原生层崩溃，候选评分/投影可能没有完成写入；"
                "先重跑候选池刷新并查看日志。"
            )
        return "调度脚本在 libmini_racer 原生层崩溃；先查看日志并隔离触发该原生依赖的步骤。"
    if error_type == "timeout":
        return "调度脚本超时，结果可能未完整写入；先缩小任务规模重跑，再复核下游证据是否刷新。"
    if error_type == "python_exception":
        return "调度脚本抛出 Python 异常；先查看日志中的 traceback，再复核对应数据写入是否完成。"
    if error_type == "script_failure":
        return "调度脚本非零退出；先查看日志和命令输出，确认候选、评分或承接证据是否写入。"
    return "调度脚本失败原因未分类；先查看日志和 last_error，再判断是否需要重跑对应命令。"


def _schedule_failure_recovery_action(*, script: str, name: str) -> dict[str, Any]:
    job_label = f"{script} {name}".lower()
    if "screener_refresh" in job_label or "候选" in name:
        return {
            "type": "refresh_candidates",
            "label": "重新刷新候选和评分",
            "command": "atrade screener refresh --json",
            "safe_to_auto_apply": True,
            **_action_contract("screener_refresh", writes_state=True, risk_level="state_write"),
        }
    if "auto_trade" in job_label or "模拟" in name:
        return {
            "type": "check_paper_auto_readiness",
            "label": "复核模拟承接预检",
            "command": "atrade paper auto-readiness --json",
            "safe_to_auto_apply": True,
            **_action_contract("paper_auto_readiness"),
        }
    return {
        "type": "inspect_schedule",
        "label": "复核调度失败",
        "command": "atrade diagnose schedule --json",
        "safe_to_auto_apply": True,
        **_action_contract("diagnose_schedule"),
    }


def _schedule_display(job: dict[str, Any]) -> str:
    schedule = job.get("schedule") or {}
    if isinstance(schedule, dict):
        return str(schedule.get("display") or schedule.get("expr") or "")
    return str(job.get("schedule_display") or "")


def _expected_schedule_times_today(schedule_display: str, *, now: datetime) -> list[datetime]:
    parts = schedule_display.split()
    if len(parts) < 5:
        return []
    minute_values = _cron_fixed_values(parts[0], minimum=0, maximum=59)
    hour_values = _cron_fixed_values(parts[1], minimum=0, maximum=23)
    weekday_values = _cron_weekday_values(parts[4])
    if minute_values is None or hour_values is None:
        return []
    if weekday_values is not None and now.weekday() not in weekday_values:
        return []
    return sorted(
        now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        for hour in hour_values
        for minute in minute_values
    )


def _cron_fixed_values(value: str, *, minimum: int, maximum: int) -> list[int] | None:
    if value == "*":
        return list(range(minimum, maximum + 1))
    values: list[int] = []
    for part in value.split(","):
        if not part.isdigit():
            return None
        number = int(part)
        if number < minimum or number > maximum:
            return None
        values.append(number)
    return sorted(set(values))


def _cron_weekday_values(value: str) -> set[int] | None:
    if value == "*":
        return None
    result: set[int] = set()
    for part in value.split(","):
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            if not start_text.isdigit() or not end_text.isdigit():
                return None
            start = int(start_text)
            end = int(end_text)
            result.update(_cron_weekday_to_python(day) for day in range(start, end + 1))
            continue
        if not part.isdigit():
            return None
        result.add(_cron_weekday_to_python(int(part)))
    return result


def _cron_weekday_to_python(value: int) -> int:
    return 6 if value in {0, 7} else value - 1


def _strategy_candidate_flow(
    conn: Any,
    *,
    thresholds: dict[str, Any],
    candidate_pool: dict[str, Any],
    auto_trade: dict[str, Any],
) -> dict[str, Any]:
    score_events = _recent_events(conn, "score.calculated", limit=500)
    decision_events = _recent_events(conn, "decision.suggested", limit=500)
    latest_scores = _latest_payloads_by_code(score_events)
    latest_decisions = _latest_payloads_by_code(decision_events)
    usable_buy_events = _usable_buy_signal_events(decision_events, auto_trade)
    latest_usable_buy_signals = _latest_payloads_by_code(usable_buy_events)
    buy_threshold = float(thresholds.get("buy", 0) or 6.0)
    watch_threshold = float(thresholds.get("watch", 0) or 5.0)
    current_candidates = _current_candidate_rows(conn, limit=10)
    current_entry_signals = [
        _current_entry_signal_summary(item)
        for item in current_candidates
        if _truthy(item.get("entry_signal"))
    ]
    candidate_summary = _current_candidate_summary(candidate_pool, current_candidates)

    latest_entry_signal_count = sum(1 for item in latest_scores if _truthy(item["payload"].get("entry_signal")))
    current_entry_signal_count = sum(1 for item in current_candidates if _truthy(item.get("entry_signal")))
    raw_buy_ready = sum(1 for item in latest_scores if _score_value(item["payload"]) >= buy_threshold)
    watch_or_better = sum(1 for item in latest_scores if _score_value(item["payload"]) >= watch_threshold)
    action_counts = Counter(str(item["payload"].get("action") or "unknown") for item in latest_decisions)
    quality_counts = Counter(str(item["payload"].get("data_quality") or "unknown") for item in latest_scores)
    hard_veto_counts: Counter[str] = Counter()
    decision_veto_counts: Counter[str] = Counter()
    missing_field_counts: Counter[str] = Counter()
    for item in latest_scores:
        payload = item["payload"]
        hard_veto_counts.update(str(reason) for reason in (payload.get("hard_veto_signals") or []))
        missing_field_counts.update(str(field) for field in (payload.get("data_missing_fields") or []))
    for item in latest_decisions:
        decision_veto_counts.update(str(reason) for reason in (item["payload"].get("veto_reasons") or []))
    buy_funnel = _buy_funnel_summary(latest_scores, latest_decisions)

    return {
        "pool": candidate_pool,
        "candidate_summary": candidate_summary,
        "current_candidates": current_candidates,
        "current_entry_signals": current_entry_signals,
        "scores": {
            "raw_events": len(score_events),
            "unique_scores": len(latest_scores),
            "buy_ready_raw": raw_buy_ready,
            "watch_or_better": watch_or_better,
            "entry_signal": {
                "triggered": current_entry_signal_count,
                "missing": max(len(current_candidates) - current_entry_signal_count, 0),
                "scope": "current_candidate_pool",
                "latest_scores_triggered": latest_entry_signal_count,
                "latest_scores_missing": max(len(latest_scores) - latest_entry_signal_count, 0),
            },
            "data_quality_counts": dict(sorted(quality_counts.items())),
            "hard_veto_counts": dict(hard_veto_counts.most_common()),
            "missing_field_counts": dict(missing_field_counts.most_common()),
            "top_scores": [_score_summary(item) for item in _ranked_score_events(latest_scores)[:10]],
        },
        "decisions": {
            "raw_events": len(decision_events),
            "unique_decisions": len(latest_decisions),
            "action_counts": dict(sorted(action_counts.items())),
            "usable_buy_signal_count": len(latest_usable_buy_signals),
            "decision_veto_counts": dict(decision_veto_counts.most_common()),
            "latest_buy_signal": _latest_decision_with_action(decision_events, "BUY"),
            "latest_usable_buy_signal": _latest_decision_with_action(usable_buy_events, "BUY"),
            "top_decisions": [_decision_summary(item) for item in _ranked_decision_events(latest_decisions)[:10]],
        },
        "buy_funnel": buy_funnel,
        "automation": {
            "latest_summary": _latest_auto_trade_summary(conn),
            "schedule": diagnose_schedule(conn),
        },
    }


def _buy_funnel_summary(
    latest_scores: list[dict[str, Any]],
    latest_decisions: list[dict[str, Any]],
) -> dict[str, Any]:
    scores_by_code = {
        str((event.get("payload") or {}).get("code") or ""): event.get("payload") or {}
        for event in latest_scores
        if (event.get("payload") or {}).get("code")
    }
    reason_counts: Counter[str] = Counter()
    gate_status_counts: dict[str, Counter[str]] = {}
    route_policy_counts: Counter[str] = Counter()
    market_regime_counts: Counter[str] = Counter()
    top_blocked: list[dict[str, Any]] = []

    for event in latest_decisions:
        payload = event.get("payload") or {}
        code = str(payload.get("code") or "")
        funnel = payload.get("buy_funnel") or _fallback_buy_funnel(payload, scores_by_code.get(code, {}))
        for reason in funnel.get("decision_reason_keys") or []:
            reason_counts.update([str(reason)])
        gates = funnel.get("gates") or {}
        for gate_name, gate in gates.items():
            if not isinstance(gate, dict):
                continue
            status = str(gate.get("status") or "unknown")
            gate_status_counts.setdefault(str(gate_name), Counter()).update([status])
            if gate_name == "route_policy":
                route_policy_counts.update([status])
            if gate_name == "market_regime":
                market_regime_counts.update([status])
        if str(payload.get("action") or "") != "BUY":
            blocked = _decision_summary(event)
            blocked["buy_funnel"] = funnel
            top_blocked.append(blocked)

    top_blocked = sorted(
        top_blocked,
        key=lambda item: (_score_value(item), str(item.get("occurred_at") or "")),
        reverse=True,
    )[:10]
    return {
        "reason_counts": dict(reason_counts.most_common()),
        "gate_status_counts": {
            gate: dict(counter.most_common())
            for gate, counter in sorted(gate_status_counts.items())
        },
        "route_policy_counts": dict(route_policy_counts.most_common()),
        "market_regime_counts": dict(market_regime_counts.most_common()),
        "top_blocked": top_blocked,
    }


def _fallback_buy_funnel(decision_payload: dict[str, Any], score_payload: dict[str, Any]) -> dict[str, Any]:
    reason_keys: list[str] = []
    veto_reasons = list(decision_payload.get("veto_reasons") or [])
    hard_veto = list(score_payload.get("hard_veto_signals") or [])
    reason_keys.extend(veto_reasons or hard_veto)
    if not _truthy(score_payload.get("entry_signal")):
        reason_keys.append("entry_signal_missing")
    market_signal = str(decision_payload.get("market_signal") or "")
    if market_signal in {"RED", "CLEAR"}:
        reason_keys.append("market_blocks_new_positions")
    primary_route = (
        decision_payload.get("primary_strategy_route")
        or score_payload.get("primary_strategy_route")
    )
    if primary_route:
        reason_keys.append("route_policy_unknown")
    return {
        "version": 0,
        "status": "executable_buy" if decision_payload.get("action") == "BUY" else "unknown",
        "action": decision_payload.get("action"),
        "decision_reason_keys": _dedupe_strings(reason_keys),
        "gates": {
            "hard_veto": {
                "status": "blocked" if (veto_reasons or hard_veto) else "unknown",
                "reasons": veto_reasons or hard_veto,
            },
            "entry_signal": {
                "status": "pass" if _truthy(score_payload.get("entry_signal")) else "unknown",
                "triggered": _truthy(score_payload.get("entry_signal")),
            },
            "market_regime": {
                "status": "blocked" if market_signal in {"RED", "CLEAR"} else "unknown",
                "signal": market_signal,
            },
            "route_policy": {
                "status": "unknown" if primary_route else "no_route",
                "primary_route": primary_route,
            },
        },
    }


def _dedupe_strings(values: list[Any]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "")
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _strategy_actionable_state(candidate_flow: dict[str, Any]) -> dict[str, Any]:
    pool = candidate_flow.get("pool", {}) or {}
    scores = candidate_flow.get("scores", {}) or {}
    decisions = candidate_flow.get("decisions", {}) or {}
    automation = candidate_flow.get("automation", {}) or {}
    action_counts = decisions.get("action_counts", {}) or {}
    latest_summary = automation.get("latest_summary") or {}
    buy_count = int(decisions.get("usable_buy_signal_count", action_counts.get("BUY", 0)) or 0)
    entry_count = int((scores.get("entry_signal", {}) or {}).get("triggered", 0) or 0)

    if buy_count and latest_summary.get("no_trade_reason") == "buy_window_closed_with_signal":
        schedule_gap = _intraday_simulation_schedule_gap(automation.get("schedule") or {})
        if schedule_gap:
            return {
                "status": "buy_signal_waiting_window",
                "summary": "已有买入意向，但盘中候选-模拟兜底调度今天未实际运行，导致模拟承接错过买入窗口。",
                "schedule_gap": schedule_gap,
                "next_action": {
                    "type": "inspect_schedule",
                    "label": "检查盘中模拟兜底调度",
                    "command": "atrade diagnose schedule --json",
                    "safe_to_auto_apply": True,
                    **_action_contract("diagnose_schedule"),
                },
            }
        return {
            "status": "buy_signal_waiting_window",
            "summary": "已有买入意向，但最近一次自动模拟因买入窗口关闭未提交委托。",
            "next_action": {
                "type": "paper_auto_readiness",
                "label": "检查模拟盘自动交易预检",
                "command": "atrade paper auto-readiness --json",
                "safe_to_auto_apply": True,
                **_action_contract("paper_auto_readiness"),
            },
        }
    if buy_count:
        return {
            "status": "buy_signal_ready_for_auto_trade_check",
            "summary": "已有买入意向，下一步核查模拟盘窗口、账户和风控是否允许提交委托。",
            "next_action": {
                "type": "paper_auto_readiness",
                "label": "检查模拟盘自动交易预检",
                "command": "atrade paper auto-readiness --json",
                "safe_to_auto_apply": True,
                **_action_contract("paper_auto_readiness"),
            },
        }
    if entry_count:
        latest_buy_signal = decisions.get("latest_buy_signal") or {}
        summary = "已有入场信号候选，但尚未形成买入意向；先保持影子试运行和单票复核。"
        if latest_buy_signal:
            summary = "已有过期待复核买入意向，但没有新鲜可承接买入意向；先保持影子试运行和单票复核。"
        return {
            "status": "entry_signal_observable",
            "summary": summary,
            "next_action": {
                "type": "paper_trial_plan",
                "label": "生成影子试运行计划",
                "command": "atrade paper trial-plan --json",
                "safe_to_auto_apply": True,
                **_action_contract("paper_trial_plan"),
            },
        }
    if int(pool.get("total", 0) or 0):
        return {
            "status": "observable_candidates",
            "summary": "候选池已有观察对象，但入场信号不足；继续跟踪评分、资金流和数据质量。",
            "next_action": {
                "type": "paper_trial_plan",
                "label": "生成影子试运行计划",
                "command": "atrade paper trial-plan --json",
                "safe_to_auto_apply": True,
                **_action_contract("paper_trial_plan"),
            },
        }
    return {
        "status": "no_candidate_flow",
        "summary": "当前没有候选池、评分或买入意向可串成候选流。",
        "next_action": {
            "type": "refresh_candidates",
            "label": "刷新候选和评分",
            "command": "atrade screener refresh --json",
            "safe_to_auto_apply": True,
            **_action_contract("screener_refresh", writes_state=True, risk_level="state_write"),
        },
    }


def _intraday_simulation_schedule_gap(schedule: dict[str, Any]) -> dict[str, Any]:
    missed_jobs = [
        item for item in schedule.get("missed_jobs", []) or []
        if item.get("critical_for_intraday_simulation")
    ]
    if not missed_jobs:
        return {}
    return {
        "status": schedule.get("status", "warning"),
        "summary": schedule.get("summary", ""),
        "missed_jobs": missed_jobs,
    }


def _recent_events(conn: Any, event_type: str, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT event_id, occurred_at, payload_json
           FROM event_log
           WHERE event_type = ?
           ORDER BY occurred_at DESC, stream_version DESC
           LIMIT ?""",
        (event_type, max(int(limit or 1), 1)),
    ).fetchall()
    return [
        {
            "event_id": row["event_id"],
            "occurred_at": row["occurred_at"],
            "event_type": event_type,
            "payload": _decode_json(row["payload_json"]),
        }
        for row in rows
    ]


def _latest_payloads_by_code(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: list[dict[str, Any]] = []
    seen: set[str] = set()
    for event in events:
        payload = event.get("payload") or {}
        code = str(payload.get("code") or "")
        if not code or code in seen:
            continue
        seen.add(code)
        latest.append(event)
    return latest


def _ranked_score_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        events,
        key=lambda event: (
            _score_value(event.get("payload") or {}),
            1 if _truthy((event.get("payload") or {}).get("entry_signal")) else 0,
            _event_timestamp(event),
        ),
        reverse=True,
    )


def _ranked_decision_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        events,
        key=lambda event: (
            _decision_action_priority(str((event.get("payload") or {}).get("action") or "")),
            _score_value(event.get("payload") or {}),
            _event_timestamp(event),
        ),
        reverse=True,
    )


def _decision_action_priority(action: str) -> int:
    return {
        "BUY": 4,
        "SELL": 3,
        "WATCH": 2,
        "CLEAR": 1,
    }.get(action, 0)


def _event_timestamp(event: dict[str, Any]) -> float:
    occurred = _parse_dt(event.get("occurred_at"))
    return occurred.timestamp() if occurred else 0.0


def _usable_buy_signal_events(events: list[dict[str, Any]], auto_trade_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    current = _latest_event_time(events) or utc_now()
    return [
        event
        for event in events
        if (event.get("payload") or {}).get("action") == "BUY"
        and _event_matches_current_buy_session(event, auto_trade_cfg, current)
    ]


def _latest_event_time(events: list[dict[str, Any]]) -> datetime | None:
    occurred = [
        parsed
        for parsed in (_parse_dt(event.get("occurred_at")) for event in events)
        if parsed is not None
    ]
    return max(occurred) if occurred else None


def _event_matches_current_buy_session(
    event: dict[str, Any],
    auto_trade_cfg: dict[str, Any],
    current: datetime,
) -> bool:
    occurred = _parse_dt(event.get("occurred_at"))
    if occurred is None:
        return False
    current_local = current.astimezone(MARKET_TZ) if current.tzinfo else current.replace(tzinfo=MARKET_TZ)
    occurred_local = occurred.astimezone(MARKET_TZ) if occurred.tzinfo else occurred.replace(tzinfo=MARKET_TZ)
    if occurred_local.date() != current_local.date():
        return False
    if not is_market_weekday(current_local) or not is_market_weekday(occurred_local):
        return False

    end = _parse_hhmm((auto_trade_cfg.get("buy_window") or {}).get("end", ""))
    if end is None:
        return True
    return occurred_local.replace(second=0, microsecond=0).time() <= end


def _parse_hhmm(value: str) -> Any:
    try:
        hour, minute = value.split(":", 1)
        return datetime.min.time().replace(hour=int(hour), minute=int(minute))
    except (AttributeError, TypeError, ValueError):
        return None


def _current_candidate_rows(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT code, pool_tier, name, score, added_at, last_scored_at,
                  streak_days, note
           FROM projection_candidate_pool
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


def _current_candidate_summary(candidate_pool: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    total = int(candidate_pool.get("total", 0) or len(candidates))
    core_count = int(candidate_pool.get("core_count", 0) or 0)
    watch_count = int(candidate_pool.get("watch_count", 0) or 0)
    radar_count = int(candidate_pool.get("radar_count", 0) or 0)
    entry_signal_count = sum(1 for item in candidates if _truthy(item.get("entry_signal")))
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
        "top_core_candidate": _first_current_candidate_for_tier(candidates, "core"),
        "top_watch_candidate": _first_current_candidate_for_tier(candidates, "watch"),
        "top_radar_candidate": _first_current_candidate_for_tier(candidates, "radar"),
    }


def _first_current_candidate_for_tier(candidates: list[dict[str, Any]], tier: str) -> dict[str, Any]:
    for item in candidates:
        if str(item.get("pool_tier") or "") == tier:
            return _compact_current_candidate(item)
    return {}


def _compact_current_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    code = str(candidate.get("code") or "")
    tier = str(candidate.get("pool_tier") or "")
    route = candidate.get("primary_strategy_route")
    return {
        "code": code,
        "name": candidate.get("name", ""),
        "pool_tier": tier,
        "pool_tier_label": _pool_tier_label(tier),
        "score": candidate.get("score", 0) or 0,
        "entry_signal": candidate.get("entry_signal"),
        "primary_strategy_route": route,
        "primary_strategy_route_label": (
            candidate.get("primary_strategy_route_label") or _strategy_route_label(route)
        ),
        "technical_detail": candidate.get("technical_detail", ""),
        "review_command": f"atrade stock analyze {code} --json" if code else "",
    }


def _current_entry_signal_summary(candidate: dict[str, Any]) -> dict[str, Any]:
    code = str(candidate.get("code") or "")
    tier = str(candidate.get("pool_tier") or "")
    route = candidate.get("primary_strategy_route")
    return {
        "code": code,
        "name": candidate.get("name", ""),
        "pool_tier": tier,
        "pool_tier_label": _pool_tier_label(tier),
        "score": candidate.get("score", 0) or 0,
        "entry_signal": True,
        "primary_strategy_route": route,
        "primary_strategy_route_label": (
            candidate.get("primary_strategy_route_label") or _strategy_route_label(route)
        ),
        "technical_detail": candidate.get("technical_detail", ""),
        "data_quality": candidate.get("data_quality", ""),
        "review_command": f"atrade stock analyze {code} --json" if code else "",
    }


def _pool_tier_label(tier: str) -> str:
    return {"core": "核心", "watch": "观察", "radar": "强势观察"}.get(tier, tier or "未分层")


def _strategy_route_label(route: Any) -> str | None:
    labels = {
        "short_continuation": "短续接力",
        "flow_confirmed_trend": "资金趋势确认",
        "volume_breakout": "放量突破",
        "shrink_pullback": "缩量回踩",
        "ma_golden_cross": "均线金叉",
        "trend_watch": "趋势观察",
        "dragon_head": "龙头策略",
    }
    return labels.get(str(route)) if route else None


def _latest_decision_with_action(events: list[dict[str, Any]], action: str) -> dict[str, Any] | None:
    for event in events:
        payload = event.get("payload") or {}
        if payload.get("action") == action:
            return _decision_summary(event)
    return None


def _latest_auto_trade_summary(conn: Any) -> dict[str, Any] | None:
    events = _recent_events(conn, "auto_trade.summary", limit=1)
    if not events:
        return None
    event = events[0]
    payload = event.get("payload") or {}
    no_trade = payload.get("no_trade_summary") or {}
    return {
        "event_id": event.get("event_id"),
        "occurred_at": event.get("occurred_at"),
        "dry_run": bool(payload.get("dry_run")),
        "buy_count": int(payload.get("buy_count", 0) or 0),
        "sell_count": int(payload.get("sell_count", 0) or 0),
        "no_trade_reason": no_trade.get("reason"),
        "no_trade_message": no_trade.get("message"),
    }


def _score_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    return {
        "event_id": event.get("event_id"),
        "occurred_at": event.get("occurred_at"),
        "code": payload.get("code"),
        "name": payload.get("name"),
        "score": _score_value(payload),
        "entry_signal": _truthy(payload.get("entry_signal")),
        "primary_strategy_route": payload.get("primary_strategy_route"),
        "data_quality": payload.get("data_quality"),
        "veto_triggered": bool(payload.get("veto_triggered")),
    }


def _decision_summary(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    return {
        "event_id": event.get("event_id"),
        "occurred_at": event.get("occurred_at"),
        "code": payload.get("code"),
        "name": payload.get("name"),
        "action": payload.get("action"),
        "score": _score_value(payload),
        "position_pct": payload.get("position_pct"),
        "market_signal": payload.get("market_signal"),
        "veto_reasons": payload.get("veto_reasons") or [],
    }


def _score_value(payload: dict[str, Any]) -> float:
    for key in ("total_score", "score"):
        try:
            if payload.get(key) is not None:
                return float(payload.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0.0


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


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
