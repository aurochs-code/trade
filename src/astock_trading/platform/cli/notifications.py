"""Discord notification CLI commands."""

from __future__ import annotations

import json
from pathlib import Path
import re
import signal
from typing import Any

import typer

from astock_trading.platform.agent_diagnostics import propose_agent_trade_plan
from astock_trading.platform.cli.common import json_or_text
from astock_trading.platform.db import connect, init_db
from astock_trading.platform.hermes_commands import (
    build_opportunity_card,
    build_opportunity_watch,
    write_opportunity_watch_state,
)
from astock_trading.platform.ops_watchdog import (
    build_ops_watchdog,
    build_ops_watchdog_monitor,
    build_ops_watchdog_context,
    read_ops_watchdog_snapshot,
    resolve_ops_watchdog_state_file,
    write_ops_watchdog_snapshot,
)
from astock_trading.reporting.discord import (
    format_daily_inspection_embed,
    format_llm_summary_embed,
    format_manual_confirmation_embed,
    format_manual_followup_embed,
    format_ops_watchdog_embed,
    format_opportunity_embed,
    format_opportunity_watch_embed,
    format_propose_plan_embed,
)
from astock_trading.reporting.discord_sender import send_embed


notify_app = typer.Typer(name="notify", help="Discord 通知")
build_context = build_ops_watchdog_context

_EVIDENCE_ID_RE = re.compile(
    r"(?:\bevidence_ids?\s*[:：]\s*|证据编号\s*[:：]\s*)([-A-Za-z0-9_.,:; ]+)"
)
_UNAVAILABLE_EVIDENCE_RE = re.compile(
    r"(?:\bevidence_ids?\s*[:：]\s*|证据编号\s*[:：]\s*)(?:暂无可用数据|无|none|n/a)",
    re.IGNORECASE,
)
_SECTION_RE = re.compile(r"(?m)^###\s+\d+[.、]?\s+(.+)$")


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


def _ops_watchdog_timeout_handler(signum, frame):
    raise TimeoutError("ops-watchdog 通知超过最大运行时限")


def validate_llm_summary_evidence(summary: str) -> dict:
    """校验 LLM 最终摘要是否按章节附带 evidence_id。"""
    evidence_ids = _extract_evidence_ids(summary)
    missing_sections: list[str] = []
    unavailable_sections: list[str] = []
    matches = list(_SECTION_RE.finditer(summary))
    if not matches:
        return {
            "ok": bool(evidence_ids),
            "evidence_ids": evidence_ids,
            "missing_sections": [] if evidence_ids else ["全文"],
            "unavailable_sections": [],
        }

    for idx, match in enumerate(matches):
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(summary)
        title = match.group(1).strip()
        body = summary[start:end].strip()
        if not _section_has_claim(body):
            continue
        if not _EVIDENCE_ID_RE.search(body):
            if _UNAVAILABLE_EVIDENCE_RE.search(body):
                unavailable_sections.append(title)
                continue
            missing_sections.append(title)
    return {
        "ok": not missing_sections,
        "evidence_ids": evidence_ids,
        "missing_sections": missing_sections,
        "unavailable_sections": unavailable_sections,
    }


def normalize_llm_summary_evidence(summary: str) -> tuple[str, list[str]]:
    """把缺少合法证据编号的判断章节显式标为暂无可用数据。"""
    matches = list(_SECTION_RE.finditer(summary))
    if not matches:
        return summary, []

    parts: list[str] = []
    pos = 0
    autofilled_sections: list[str] = []
    for idx, match in enumerate(matches):
        body_start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(summary)
        title = match.group(1).strip()
        parts.append(summary[pos:body_start])
        body = summary[body_start:end]
        if (
            _section_has_claim(body)
            and not _EVIDENCE_ID_RE.search(body)
            and not _UNAVAILABLE_EVIDENCE_RE.search(body)
        ):
            sep = "" if body.endswith("\n") else "\n"
            body = f"{body}{sep}- 证据编号：暂无可用数据\n"
            autofilled_sections.append(title)
        parts.append(body)
        pos = end
    parts.append(summary[pos:])
    return "".join(parts), autofilled_sections


def _extract_evidence_ids(summary: str) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for match in _EVIDENCE_ID_RE.finditer(summary):
        raw = match.group(1)
        for part in re.split(r"[,，、\s]+", raw):
            evidence_id = part.strip(" .;；()（）[]【】")
            if evidence_id and evidence_id not in seen:
                seen.add(evidence_id)
                ids.append(evidence_id)
    return ids


def _section_has_claim(body: str) -> bool:
    for line in body.splitlines():
        text = line.strip()
        if not text.startswith(("-", "•")):
            continue
        if "暂无可用数据" in text and not _EVIDENCE_ID_RE.search(text):
            continue
        return True
    return False


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
    opportunity = _result_json(results_by_name, "opportunity") or {}

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
        "opportunity_status": _status_from_json(opportunity),
        "opportunity_summary": opportunity.get("summary", "") if isinstance(opportunity, dict) else "",
        "opportunity_decision_brief": opportunity.get("decision_brief", "") if isinstance(opportunity, dict) else "",
        "opportunity_counts": opportunity.get("counts", {}) if isinstance(opportunity, dict) else {},
        "opportunity_blockers": opportunity.get("blockers", []) if isinstance(opportunity, dict) else [],
        "opportunity_next_action": opportunity.get("next_action", {}) if isinstance(opportunity, dict) else {},
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


@notify_app.command("opportunity")
def notify_opportunity(
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """生成今日机会卡并推送 Discord。"""
    init_db()
    conn = connect()
    try:
        opportunity = build_opportunity_card(conn)
    finally:
        conn.close()

    embed = format_opportunity_embed(opportunity)
    ok, error = _send_or_dry_run(embed, "A股今日机会卡", dry_run)
    payload = _notification_payload(
        embed=embed,
        dry_run=dry_run,
        ok=ok,
        error=error,
        extra={"opportunity": opportunity},
    )
    json_or_text(payload, as_json)
    if not dry_run and not ok:
        raise typer.Exit(1)


@notify_app.command("manual-followup")
def notify_manual_followup(
    skip_account: bool = typer.Option(False, "--skip-account", help="不请求 MX 模拟盘账户，只检查本地配置和事件证据"),
    limit: int = typer.Option(100, "--limit", min=1, max=1000, help="最多扫描影子试运行事件数"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """生成只读人工复核自动汇总并推送 Discord。"""
    from astock_trading.pipeline.auto_trade import build_auto_trade_readiness
    from astock_trading.pipeline.context import build_context
    from astock_trading.platform.manual_followup import build_manual_followup_report

    ctx = build_context()
    try:
        auto_readiness = build_auto_trade_readiness(ctx, include_account=not skip_account)
        manual_followup = build_manual_followup_report(
            ctx.conn,
            auto_readiness=auto_readiness,
            limit=limit,
        )
    finally:
        ctx.conn.close()

    embed = format_manual_followup_embed(manual_followup)
    ok, error = _send_or_dry_run(embed, "A股人工复核", dry_run)
    payload = _notification_payload(
        embed=embed,
        dry_run=dry_run,
        ok=ok,
        error=error,
        extra={"manual_followup": manual_followup},
    )
    json_or_text(payload, as_json)
    if not dry_run and not ok:
        raise typer.Exit(1)


@notify_app.command("opportunity-watch")
def notify_opportunity_watch(
    state_file: Path | None = typer.Option(None, "--state-file", help="机会变化状态文件"),
    reset_state: bool = typer.Option(False, "--reset-state", help="忽略旧状态并重建今日基线"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord，也不更新状态"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """候选池出现新增机会时推送 Discord；无变化时静默。"""
    init_db()
    conn = connect()
    try:
        monitor = build_opportunity_watch(
            conn,
            state_file=state_file,
            update_state=False,
            reset_state=reset_state,
        )
    finally:
        conn.close()

    should_notify = bool(monitor.get("should_notify"))
    embed = format_opportunity_watch_embed(monitor) if should_notify else {}
    if should_notify:
        ok, error = _send_or_dry_run(embed, "A股机会变化提醒", dry_run)
    else:
        ok, error = True, ""

    if not dry_run and (ok or not should_notify):
        write_opportunity_watch_state(monitor, state_file)
        monitor["state_updated"] = True

    status = "dry_run" if dry_run else ("sent" if should_notify and ok else ("failed" if should_notify else "silent"))
    result = {
        "status": status,
        "notification": {
            "target": "discord",
            "ok": ok,
            "error": error,
            "skipped": not should_notify,
            "reason": "" if should_notify else monitor.get("summary", ""),
        },
        "embed": embed,
        "monitor": monitor,
    }
    json_or_text(result, as_json)
    if not dry_run and should_notify and not ok:
        raise typer.Exit(1)


@notify_app.command("ops-watchdog")
def notify_ops_watchdog(
    include_account: bool = typer.Option(False, "--include-account", help="读取 MX 模拟盘账户；默认只查本地证据"),
    jobs_path: Path | None = typer.Option(None, "--jobs-path", help="Hermes jobs.json 路径"),
    env_file: Path | None = typer.Option(None, "--env-file", help="atrade 运行 .env 路径"),
    state_file: Path | None = typer.Option(None, "--state-file", help="watchdog 状态文件"),
    reset_state: bool = typer.Option(False, "--reset-state", help="忽略旧状态并重建当前基线"),
    max_runtime_seconds: int = typer.Option(45, "--max-runtime-seconds", min=5, help="watchdog 总运行时限，避免通知卡住"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord，也不更新状态"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """运维 watchdog 状态变化时推送 Discord；无变化时静默。"""
    previous_handler = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, _ops_watchdog_timeout_handler)
    signal.alarm(max_runtime_seconds)
    try:
        resolved_state_file = resolve_ops_watchdog_state_file(state_file)
        ctx = build_context()
        try:
            report = build_ops_watchdog(
                ctx,
                include_account=include_account,
                jobs_path=jobs_path,
                env_file=env_file,
            )
        finally:
            ctx.conn.close()

        previous = None if reset_state else read_ops_watchdog_snapshot(resolved_state_file)
        monitor = build_ops_watchdog_monitor(report, previous_snapshot=previous)
        monitor["state_file"] = str(resolved_state_file)
        should_notify = bool(monitor.get("should_notify"))
        embed = format_ops_watchdog_embed(monitor) if should_notify else {}
        if should_notify:
            ok, error = _send_or_dry_run(embed, "A股运维 watchdog", dry_run)
        else:
            ok, error = True, ""

        if not dry_run and (ok or not should_notify):
            write_ops_watchdog_snapshot(monitor, resolved_state_file)
            monitor["state_updated"] = True
        else:
            monitor["state_updated"] = False

        status = "dry_run" if dry_run else ("sent" if should_notify and ok else ("failed" if should_notify else "silent"))
        result = {
            "status": status,
            "notification": {
                "target": "discord",
                "ok": ok,
                "error": error,
                "skipped": not should_notify,
                "reason": "" if should_notify else monitor.get("summary", ""),
            },
            "embed": embed,
            "monitor": monitor,
        }
        json_or_text(result, as_json)
        if not dry_run and should_notify and not ok:
            raise typer.Exit(1)
    except TimeoutError as exc:
        result = {
            "status": "failed",
            "notification": {
                "target": "discord",
                "ok": False,
                "error": str(exc),
                "skipped": False,
                "reason": "ops-watchdog 通知超时",
            },
            "embed": {},
            "monitor": {"state_updated": False},
        }
        json_or_text(result, as_json)
        raise typer.Exit(1) from exc
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


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


@notify_app.command("llm-summary-card")
def notify_llm_summary_card(
    payload_file: Path = typer.Option(..., "--payload", help="LLM Markdown 摘要文件"),
    mode: str = typer.Option(..., "--mode", help="morning / close / weekly"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只生成卡片，不发送 Discord"),
    allow_missing_evidence: bool = typer.Option(False, "--allow-missing-evidence", help="应急放行缺少 evidence_id 的摘要"),
    fill_missing_evidence: bool = typer.Option(False, "--fill-missing-evidence", help="将缺少合法证据编号的判断章节标为暂无可用数据"),
    as_json: bool = typer.Option(False, "--json", help="JSON 输出"),
):
    """从 Hermes LLM Markdown 摘要生成 Discord Rich Embed。"""
    if mode not in {"morning", "close", "weekly"}:
        raise typer.BadParameter("--mode must be morning, close, or weekly")
    summary = payload_file.read_text(encoding="utf-8")
    evidence_normalization = {"enabled": fill_missing_evidence, "autofilled_sections": []}
    if fill_missing_evidence:
        summary, autofilled_sections = normalize_llm_summary_evidence(summary)
        evidence_normalization["autofilled_sections"] = autofilled_sections
    evidence_validation = validate_llm_summary_evidence(summary)
    if not allow_missing_evidence and not evidence_validation["ok"]:
        payload = {
            "status": "failed",
            "mode": mode,
            "error": "LLM 摘要缺少 evidence_id，已拒绝发送。",
            "evidence_validation": evidence_validation,
        }
        if fill_missing_evidence:
            payload["evidence_normalization"] = evidence_normalization
        json_or_text(payload, as_json)
        raise typer.Exit(1)
    embed = format_llm_summary_embed(mode, summary)
    content = {
        "morning": "A股 LLM 盘前摘要",
        "close": "A股 LLM 收盘复盘",
        "weekly": "A股 LLM 周复盘补充",
    }[mode]
    ok, error = _send_or_dry_run(embed, content, dry_run)
    result = _notification_payload(
        embed=embed,
        dry_run=dry_run,
        ok=ok,
        error=error,
        extra={
            "mode": mode,
            "evidence_validation": evidence_validation,
            **({"evidence_normalization": evidence_normalization} if fill_missing_evidence else {}),
        },
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
