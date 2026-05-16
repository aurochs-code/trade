"""Discord notification CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import typer

from astock_trading.platform.agent_diagnostics import propose_agent_trade_plan
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db
from astock_trading.reporting.discord import (
    format_daily_inspection_embed,
    format_manual_confirmation_embed,
    format_propose_plan_embed,
)
from astock_trading.reporting.discord_sender import send_embed


notify_app = typer.Typer(name="notify", help="Discord 通知")


def _notification_payload(
    *,
    embed: dict,
    dry_run: bool,
    ok: bool,
    error: str,
    extra: dict[str, Any],
) -> dict:
    status = "dry_run" if dry_run else ("sent" if ok else "failed")
    return {
        "status": status,
        "notification": {
            "target": "discord",
            "ok": ok,
            "error": error,
        },
        "embed": embed,
        **extra,
    }


def _send_or_dry_run(embed: dict, content: str, dry_run: bool) -> tuple[bool, str]:
    if dry_run:
        return True, ""
    return send_embed(embed, content=content)


def _result_json(results_by_name: dict[str, dict], name: str) -> Any:
    item = results_by_name.get(name, {})
    return item.get("json")


def _status_from_json(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("status", "unknown"))
    return "unknown"


def _build_daily_inspection_summary(payload: dict, report_path: str = "") -> dict:
    results = payload.get("results", []) or []
    results_by_name = {item.get("name", ""): item for item in results}

    doctor = _result_json(results_by_name, "doctor") or {}
    health = _result_json(results_by_name, "health") or {}
    diagnose = _result_json(results_by_name, "diagnose_health") or {}
    manual_trades = _result_json(results_by_name, "manual_trades") or []
    paper = _result_json(results_by_name, "paper_status") or {}
    plan = _result_json(results_by_name, "propose_plan") or {}

    data_sources = (
        (diagnose.get("inputs", {}) or {}).get("data_sources")
        or health.get("data_sources")
        or {}
    )
    candidate_pool = (diagnose.get("inputs", {}) or {}).get("candidate_pool") or {}
    runs = health.get("runs", {}) or {}
    paper_balance = paper.get("balance", {}) if isinstance(paper, dict) else {}

    return {
        "date": payload.get("date") or "",
        "report_path": report_path or payload.get("report_path") or "",
        "failed_commands": [
            {"name": item.get("name", ""), "returncode": item.get("returncode")}
            for item in results
            if item.get("returncode") != 0
        ],
        "doctor_status": _status_from_json(doctor),
        "health_status": _status_from_json(health),
        "diagnose_health_status": _status_from_json(diagnose),
        "data_source_status": data_sources.get("status", "unknown"),
        "required_missing": data_sources.get("required_missing", []) or [],
        "optional_missing": data_sources.get("optional_missing", []) or [],
        "candidate_pool": candidate_pool,
        "failed_runs_count": len(runs.get("failed_3d", []) or (diagnose.get("inputs", {}) or {}).get("failed_runs", []) or []),
        "running_runs_count": len(runs.get("running", []) or (diagnose.get("inputs", {}) or {}).get("running_runs", []) or []),
        "pending_manual_trades": len(manual_trades) if isinstance(manual_trades, list) else 0,
        "pending_manual_trade_items": _pending_manual_trade_items(manual_trades),
        "route_blocked_watch_candidates": _route_blocked_watch_candidates(payload, results_by_name),
        "paper_positions": len(paper.get("positions", []) or []) if isinstance(paper, dict) else 0,
        "paper_total_asset": paper_balance.get("total_asset", 0) or 0,
        "plan_execution_allowed": bool(plan.get("execution_allowed")) if isinstance(plan, dict) else False,
        "plan_actions": plan.get("actions", []) if isinstance(plan, dict) else [],
    }


def _pending_manual_trade_items(manual_trades: Any) -> list[dict]:
    if not isinstance(manual_trades, list):
        return []
    items = []
    for trade in manual_trades:
        if not isinstance(trade, dict):
            continue
        if trade.get("status", "pending") != "pending":
            continue
        items.append({
            "code": trade.get("code", ""),
            "name": trade.get("name", ""),
            "side": trade.get("side", ""),
            "score": trade.get("score", trade.get("confidence", 0)),
            "confidence": trade.get("confidence", trade.get("score", 0)),
            "position_pct": trade.get("position_pct", 0),
            "requested_at": trade.get("requested_at", ""),
        })
    return items[:5]


def _route_blocked_watch_candidates(payload: dict, results_by_name: dict[str, dict]) -> list[dict]:
    direct = payload.get("route_blocked_watch_candidates") or []
    if isinstance(direct, list) and direct:
        return direct[:5]

    rows: list[dict] = []
    for name in ("screener_candidates", "candidate_pool", "candidate_pool_items"):
        value = _result_json(results_by_name, name)
        if isinstance(value, list):
            rows.extend(item for item in value if isinstance(item, dict))
        elif isinstance(value, dict):
            for key in ("candidates", "items", "rows"):
                nested = value.get(key)
                if isinstance(nested, list):
                    rows.extend(item for item in nested if isinstance(item, dict))

    blocked = [
        item for item in rows
        if "requires_entry_strategy_route" in str(item.get("note", ""))
    ]
    blocked.sort(key=lambda item: float(item.get("score", 0) or 0), reverse=True)
    return blocked[:5]


@notify_app.command("propose-plan")
def notify_propose_plan(
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """生成交易计划摘要并推送 Discord。"""
    init_db()
    conn = connect()
    try:
        plan = propose_agent_trade_plan(conn)
    finally:
        conn.close()

    embed = format_propose_plan_embed(plan)
    ok, error = _send_or_dry_run(embed, "A股交易计划", dry_run)
    payload = _notification_payload(
        embed=embed,
        dry_run=dry_run,
        ok=ok,
        error=error,
        extra={"plan": plan},
    )
    json_or_text(payload, as_json)
    if not dry_run and not ok:
        raise typer.Exit(1)


@notify_app.command("daily-inspection")
def notify_daily_inspection(
    payload_file: Path = typer.Option(..., "--payload", help="每日巡检 JSON payload 文件"),
    report_path: str = typer.Option("", "--report-path", help="巡检 Markdown 报告路径"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """从每日巡检 payload 生成摘要并推送 Discord。"""
    payload = json.loads(payload_file.read_text(encoding="utf-8"))
    summary = _build_daily_inspection_summary(payload, report_path)
    embed = format_daily_inspection_embed(summary)
    ok, error = _send_or_dry_run(embed, "A股每日巡检", dry_run)
    result = _notification_payload(
        embed=embed,
        dry_run=dry_run,
        ok=ok,
        error=error,
        extra={"summary": summary},
    )
    json_or_text(result, as_json)
    if not dry_run and not ok:
        raise typer.Exit(1)


@notify_app.command("manual-confirmation")
def notify_manual_confirmation(
    payload_file: Path = typer.Option(..., "--payload", help="stock analyze --json 生成的 payload 文件"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """从个股分析 payload 生成人工确认卡并推送 Discord。"""
    analysis = json.loads(payload_file.read_text(encoding="utf-8"))
    embed = format_manual_confirmation_embed(analysis)
    ok, error = _send_or_dry_run(embed, "A股人工确认", dry_run)
    result = _notification_payload(
        embed=embed,
        dry_run=dry_run,
        ok=ok,
        error=error,
        extra={"analysis": analysis},
    )
    json_or_text(result, as_json)
    if not dry_run and not ok:
        raise typer.Exit(1)
