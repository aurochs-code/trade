"""系统化运维 watchdog 聚合诊断。"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any


_SEVERITY_RANK = {"info": 0, "warning": 1, "critical": 2}
_STATUS_BY_SEVERITY = {"critical": "critical", "warning": "warning", "info": "ok"}


def build_ops_watchdog_report(
    *,
    schedule: dict[str, Any],
    health: dict[str, Any],
    data_sources: dict[str, Any],
    flow: dict[str, Any],
    checked_at: datetime | None = None,
) -> dict[str, Any]:
    """把现有诊断面聚合成一份只读运维断点报告。"""
    current = checked_at or datetime.now(timezone.utc)
    incidents: list[dict[str, Any]] = []

    incidents.extend(_schedule_incidents(schedule))
    incidents.extend(_candidate_pool_incidents(_candidate_pool_from(health, flow)))
    incidents.extend(_data_source_incidents(data_sources))
    incidents.extend(_auto_readiness_incidents(flow))

    incidents = _dedupe_incidents(incidents)
    incidents.sort(key=lambda item: _SEVERITY_RANK.get(item.get("severity", "info"), 0), reverse=True)

    status = _report_status(incidents)
    return {
        "command": "ops watchdog",
        "diagnostic": "ops_watchdog",
        "status": status,
        "checked_at": current.isoformat(),
        "summary": _summary(status, incidents),
        "incidents": incidents,
        "next_actions": _next_actions(incidents),
        "components": {
            "schedule": _component_summary(schedule),
            "health": _component_summary(health),
            "data_sources": _component_summary(data_sources),
            "flow": _component_summary(flow),
        },
        "guardrails": {
            "read_only": True,
            "writes_state": False,
            "writes_environment": False,
            "writes_order": False,
            "runs_pipeline": False,
        },
    }


def build_ops_watchdog(
    ctx: Any,
    *,
    include_account: bool = False,
    jobs_path: Path | None = None,
    env_file: Path | None = None,
) -> dict[str, Any]:
    """采集当前系统只读诊断并生成 watchdog 报告。"""
    from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
    from astock_trading.platform.agent_diagnostics import (
        diagnose_flow,
        diagnose_health,
        diagnose_schedule,
    )
    from astock_trading.platform.data_source_diagnostics import build_data_source_diagnosis

    auto_readiness = build_auto_trade_readiness(ctx, include_account=include_account)
    schedule = diagnose_schedule(ctx.conn, jobs_path=jobs_path, env_file=env_file)
    health = diagnose_health(ctx.conn)
    data_sources = build_data_source_diagnosis(ctx.conn)
    flow = diagnose_flow(ctx.conn, auto_readiness=auto_readiness)
    return build_ops_watchdog_report(
        schedule=schedule,
        health=health,
        data_sources=data_sources,
        flow=flow,
    )


def build_ops_watchdog_context() -> Any:
    """构造 watchdog 只读所需的轻量上下文，避免加载完整行情/provider 服务。"""
    from astock_trading.platform.config import ConfigRegistry
    from astock_trading.platform.db import connect, init_db
    from astock_trading.platform.events import EventStore
    from astock_trading.platform.runs import RunJournal

    init_db()
    conn = connect()
    data, _ = ConfigRegistry().load_and_validate()
    return SimpleNamespace(
        conn=conn,
        cfg=data.get("strategy", {}) or {},
        event_store=EventStore(conn),
        run_journal=RunJournal(conn),
    )


def build_ops_watchdog_monitor(
    report: dict[str, Any],
    *,
    previous_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    """根据上次快照判断 watchdog 是否需要主动通知。"""
    snapshot = _ops_watchdog_snapshot(report)
    previous_status = str((previous_snapshot or {}).get("status") or "")
    previous_fingerprint = str((previous_snapshot or {}).get("fingerprint") or "")
    current_status = snapshot["status"]
    current_fingerprint = snapshot["fingerprint"]
    change_types: list[str] = []

    if not previous_snapshot:
        status = "changed" if current_status != "ok" else "baseline_recorded"
        if current_status != "ok":
            change_types.append("new_ops_incident")
    elif current_fingerprint != previous_fingerprint:
        status = "changed"
        if current_status == "ok":
            change_types.append("ops_recovered")
        elif previous_status == "ok":
            change_types.append("new_ops_incident")
        else:
            change_types.append("ops_incident_changed")
    else:
        status = "unchanged"

    should_notify = status == "changed"
    return {
        "command": "ops-watchdog-monitor",
        "status": status,
        "summary": _monitor_summary(status, report, previous_status, current_status),
        "should_notify": should_notify,
        "change_types": change_types,
        "previous_status": previous_status or "none",
        "current_status": current_status,
        "previous_snapshot": previous_snapshot or {},
        "snapshot": snapshot,
        "report": report,
    }


def resolve_ops_watchdog_state_file(state_file: Path | None = None) -> Path:
    if state_file is not None:
        return state_file.expanduser()
    from astock_trading.platform.paths import default_state_dir

    return default_state_dir() / "ops_watchdog" / "state.json"


def read_ops_watchdog_snapshot(state_file: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(state_file.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    snapshot = raw.get("snapshot", {}) if isinstance(raw, dict) else {}
    return snapshot if isinstance(snapshot, dict) else None


def write_ops_watchdog_snapshot(monitor: dict[str, Any], state_file: Path) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "snapshot": monitor.get("snapshot", {}) or {},
    }
    state_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def _schedule_incidents(schedule: dict[str, Any]) -> list[dict[str, Any]]:
    incidents: list[dict[str, Any]] = []
    for job in schedule.get("failed_jobs", []) or []:
        if not isinstance(job, dict):
            continue
        diagnosis = job.get("failure_diagnosis", {}) or {}
        incidents.append({
            "severity": "critical",
            "component": "schedule",
            "reason": "scheduled_job_failed",
            "label": "调度失败",
            "summary": f"{job.get('name', '调度任务')} 最近运行失败。",
            "evidence": {
                "name": job.get("name", ""),
                "last_status": job.get("last_status", ""),
                "error_type": diagnosis.get("error_type", ""),
                "exit_code": diagnosis.get("exit_code"),
                "log_path": diagnosis.get("log_path", ""),
            },
            "next_action": _action(
                diagnosis.get("recovery_action", {}) or {},
                command="atrade diagnose schedule --json",
                label="查看调度失败并执行恢复动作",
            ),
        })
    for job in schedule.get("missed_jobs", []) or []:
        if not isinstance(job, dict):
            continue
        incidents.append({
            "severity": "warning",
            "component": "schedule",
            "reason": "scheduled_job_missed",
            "label": "调度漏跑",
            "summary": f"{job.get('name', '调度任务')} 最近没有按预期运行。",
            "evidence": {
                "name": job.get("name", ""),
                "next_run_at": job.get("next_run_at", ""),
                "last_run_at": job.get("last_run_at", ""),
            },
            "next_action": _action(
                command="atrade diagnose schedule --json",
                label="复核调度漏跑原因",
            ),
        })
    return incidents


def _candidate_pool_from(health: dict[str, Any], flow: dict[str, Any]) -> dict[str, Any]:
    health_pool = ((health.get("inputs", {}) or {}).get("candidate_pool", {}) or {})
    if health_pool:
        return health_pool
    return flow.get("candidate_pool", {}) or {}


def _candidate_pool_incidents(pool: dict[str, Any]) -> list[dict[str, Any]]:
    incidents: list[dict[str, Any]] = []
    freshness = pool.get("execution_freshness", {}) or {}
    if freshness and freshness.get("fresh") is False:
        incidents.append({
            "severity": "warning",
            "component": "candidate_pool",
            "reason": "candidate_scoring_stale",
            "label": "候选评分过期",
            "summary": "候选池评分输入超过执行窗口允许时长。",
            "evidence": {
                "age_hours": freshness.get("age_hours"),
                "max_age_hours": freshness.get("max_age_hours"),
                "blocker": freshness.get("blocker", ""),
                "latest_scored_at": pool.get("latest_scored_at", ""),
            },
            "next_action": _action(
                command="atrade screener refresh --json",
                label="刷新候选池评分",
                writes_state=True,
            ),
        })
    if int(pool.get("core_count") or 0) <= 0:
        incidents.append({
            "severity": "warning",
            "component": "candidate_pool",
            "reason": "core_pool_empty",
            "label": "核心候选池为空",
            "summary": "当前没有可承接的 core 候选，买入侧会被阻断。",
            "evidence": {
                "total": pool.get("total", pool.get("total_count", 0)),
                "core_count": pool.get("core_count", 0),
                "watch_count": pool.get("watch_count", 0),
            },
            "next_action": _action(
                command="atrade screener refresh --json",
                label="刷新并重建候选池",
                writes_state=True,
            ),
        })
    return incidents


def _data_source_incidents(data_sources: dict[str, Any]) -> list[dict[str, Any]]:
    incidents: list[dict[str, Any]] = []
    blockers = data_sources.get("data_source_blockers", []) or []
    for blocker in blockers:
        if not isinstance(blocker, dict):
            continue
        reason = str(blocker.get("reason") or "data_source_blocker")
        incidents.append({
            "severity": "critical" if reason == "required_data_sources_unavailable" else "warning",
            "component": "data_sources",
            "reason": reason,
            "label": blocker.get("label") or "数据源阻断",
            "summary": blocker.get("summary") or blocker.get("label") or "数据源存在阻断项。",
            "evidence": blocker,
            "next_action": _action(
                command="atrade data-sources diagnose --json",
                label="查看数据源诊断",
            ),
        })

    incidents_payload = data_sources.get("provider_incidents", {}) or {}
    actionable = int(incidents_payload.get("actionable_unresolved_recent") or 0)
    if actionable > 0:
        incidents.append({
            "severity": "warning",
            "component": "data_sources",
            "reason": "actionable_provider_failures",
            "label": "候选相关数据源失败未补齐",
            "summary": f"{actionable} 个候选相关 provider 失败仍未补齐。",
            "evidence": incidents_payload,
            "next_action": _action(
                command="atrade data-sources diagnose --json",
                label="查看未补齐 provider 失败",
            ),
        })
    return incidents


def _auto_readiness_incidents(flow: dict[str, Any]) -> list[dict[str, Any]]:
    stage = flow.get("flow_stage", {}) or {}
    readiness = stage.get("auto_readiness", {}) or flow.get("auto_readiness", {}) or {}
    incidents: list[dict[str, Any]] = []
    for blocker in readiness.get("blockers", []) or []:
        if not isinstance(blocker, dict):
            continue
        reason = str(blocker.get("reason") or "")
        if reason not in {"core_pool_empty", "scoring_inputs_stale", "candidate_pool_stale"}:
            continue
        incidents.append({
            "severity": "warning",
            "component": "paper_auto_readiness",
            "reason": reason,
            "label": blocker.get("label") or "模拟承接阻断",
            "summary": blocker.get("label") or "模拟承接链路存在阻断项。",
            "evidence": blocker,
            "next_action": _action(
                command="atrade diagnose flow --json",
                label="复核候选流和模拟承接",
            ),
        })
    return incidents


def _action(
    source: dict[str, Any] | None = None,
    *,
    command: str,
    label: str,
    writes_state: bool = False,
) -> dict[str, Any]:
    payload = dict(source or {})
    payload.setdefault("command", command)
    payload.setdefault("label", label)
    payload.setdefault("writes_state", writes_state)
    payload.setdefault("writes_environment", False)
    payload.setdefault("writes_order", False)
    payload.setdefault("requires_user_approval", False)
    return payload


def _dedupe_incidents(incidents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in incidents:
        key = (str(item.get("component") or ""), str(item.get("reason") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _report_status(incidents: list[dict[str, Any]]) -> str:
    if not incidents:
        return "ok"
    severity = max(
        (_SEVERITY_RANK.get(str(item.get("severity") or "info"), 0), str(item.get("severity") or "info"))
        for item in incidents
    )[1]
    return _STATUS_BY_SEVERITY.get(severity, "warning")


def _summary(status: str, incidents: list[dict[str, Any]]) -> str:
    if status == "ok":
        return "运维 watchdog 未发现流程断层。"
    critical_count = sum(1 for item in incidents if item.get("severity") == "critical")
    warning_count = sum(1 for item in incidents if item.get("severity") == "warning")
    labels = "、".join(str(item.get("label") or item.get("reason") or "") for item in incidents[:3])
    if critical_count:
        return f"发现 {critical_count} 个关键运维断点：{labels}。"
    return f"发现 {warning_count} 个需处理的运维预警：{labels}。"


def _next_actions(incidents: list[dict[str, Any]]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    seen: set[str] = set()
    for incident in incidents:
        action = incident.get("next_action", {}) or {}
        command = str(action.get("command") or "")
        if not command or command in seen:
            continue
        seen.add(command)
        actions.append(action)
    return actions


def _component_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": payload.get("status", "unknown"),
        "summary": payload.get("summary", ""),
    }


def _ops_watchdog_snapshot(report: dict[str, Any]) -> dict[str, Any]:
    incident_keys = sorted(
        "|".join([
            str(item.get("severity") or ""),
            str(item.get("component") or ""),
            str(item.get("reason") or ""),
            str((item.get("evidence", {}) or {}).get("name") or ""),
            str((item.get("evidence", {}) or {}).get("error_type") or ""),
            str((item.get("evidence", {}) or {}).get("blocker") or ""),
        ])
        for item in report.get("incidents", []) or []
        if isinstance(item, dict)
    )
    status = str(report.get("status") or "unknown")
    fingerprint = "||".join([status, *incident_keys])
    return {
        "status": status,
        "fingerprint": fingerprint,
        "incident_count": len(incident_keys),
        "incident_keys": incident_keys,
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def _monitor_summary(
    status: str,
    report: dict[str, Any],
    previous_status: str,
    current_status: str,
) -> str:
    if status == "unchanged":
        return "运维 watchdog 状态未变化，已静默。"
    if status == "baseline_recorded":
        return "运维 watchdog 已记录正常基线。"
    if current_status == "ok" and previous_status and previous_status != "ok":
        return "运维 watchdog 已恢复正常。"
    return str(report.get("summary") or "运维 watchdog 发现新状态变化。")
