"""只读 LLM 摘要上下文。

该模块把 Hermes/agent 需要的摘要材料收敛到稳定 CLI 表面：
`atrade llm-context --mode ...`。外部调度器不需要进入源码目录，也不需要读取
checkout 内脚本。
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

from astock_trading.execution.service import ExecutionService
from astock_trading.platform.agent_diagnostics import diagnose_health, propose_agent_trade_plan
from astock_trading.platform.events import EventStore
from astock_trading.platform.runs import RunJournal
from astock_trading.platform.service_factory import resolve_vault_path
from astock_trading.platform.time import local_now_str

MAX_DOC_CHARS = 3500


def _today() -> dt.date:
    return dt.datetime.now().date()


def _today_iso() -> str:
    return _today().isoformat()


def _week_start_iso() -> str:
    today = _today()
    monday = today - dt.timedelta(days=today.weekday())
    return f"{monday.isoformat()}T00:00:00"


def _today_start_iso() -> str:
    return f"{_today_iso()}T00:00:00"


def _iso_week() -> str:
    year, week, _weekday = _today().isocalendar()
    return f"{year}-W{week:02d}"


def _truncate(text: str, limit: int = MAX_DOC_CHARS) -> str:
    if len(text) <= limit:
        return text
    return f"{text[:limit]}\n... <已截断 {len(text) - limit} 字符>"


def _safe_section(name: str, fn) -> dict:
    try:
        return {"status": "ok", "data": fn()}
    except Exception as exc:  # pragma: no cover - defensive boundary
        return {"status": "failed", "error": f"{name}: {exc}"}


def _candidate_rows(conn: Any, *, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        """SELECT *
           FROM projection_candidate_pool
           ORDER BY COALESCE(score, 0) DESC, COALESCE(last_scored_at, '') DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def _manual_trade_state(events: list[dict]) -> list[dict]:
    by_stream: dict[str, dict] = {}
    for event in events:
        payload = event.get("payload", {}) or {}
        stream = event.get("stream", "")
        current = by_stream.get(stream, {})
        status = payload.get("status")
        if event["event_type"] == "manual_trade.requested":
            current = {
                **payload,
                "stream": stream,
                "requested_event_id": event["event_id"],
                "requested_at": event["occurred_at"],
                "updated_at": event["occurred_at"],
            }
        elif current:
            current.update(
                {
                    "status": status or event["event_type"].removeprefix("manual_trade."),
                    "updated_at": event["occurred_at"],
                    "resolution_event_id": event["event_id"],
                    "resolution": payload,
                }
            )
        if current:
            by_stream[stream] = current
    return sorted(by_stream.values(), key=lambda item: item.get("updated_at", ""), reverse=True)


def _report_paths(mode: str) -> list[tuple[str, str]]:
    paths = [
        ("今日决策", "04-决策/今日决策.md"),
        ("持仓概览", "01-状态/持仓/持仓概览.md"),
        ("候选池总览", "04-决策/候选池/候选池总览.md"),
        ("最新评分", "04-决策/候选池/最新评分.md"),
    ]
    if mode == "close":
        paths.append(("今日巡检", f"02-巡检/{_today_iso()}.md"))
    if mode == "weekly":
        paths.append(("本周复盘", f"03-分析/周复盘/{_iso_week()}.md"))
    return paths


def _read_report_docs(mode: str) -> dict:
    vault = resolve_vault_path()
    docs = []
    for label, relative in _report_paths(mode):
        item = {
            "name": label,
            "relative_path": relative,
            "exists": False,
            "content": "",
        }
        if vault:
            path = Path(vault) / relative
            item["path"] = str(path)
            if path.exists():
                item["exists"] = True
                item["content"] = _truncate(path.read_text(encoding="utf-8").strip())
        docs.append(item)
    return {
        "vault_path": vault or "",
        "docs": docs,
    }


def build_llm_context(conn: Any, *, mode: str) -> dict:
    """生成 Hermes/LLM 摘要用的只读上下文。"""
    if mode not in {"morning", "close", "weekly"}:
        raise ValueError("mode must be morning, close, or weekly")

    store = EventStore(conn)
    run_limit = 60 if mode == "weekly" else 40
    event_limit = 80 if mode == "weekly" else 60
    event_since = _week_start_iso() if mode == "weekly" else _today_start_iso()

    sections = {
        "diagnostics": _safe_section("diagnostics", lambda: diagnose_health(conn)),
        "trade_plan": _safe_section("trade_plan", lambda: propose_agent_trade_plan(conn)),
        "portfolio": _safe_section(
            "portfolio",
            lambda: ExecutionService(store, conn).get_portfolio(),
        ),
        "manual_trades": _safe_section(
            "manual_trades",
            lambda: _manual_trade_state(store.query(stream_type="manual_trade", limit=100)),
        ),
        "candidates": _safe_section("candidates", lambda: _candidate_rows(conn, limit=30)),
        "runs": _safe_section(
            "runs",
            lambda: RunJournal(conn).list_runs(limit=run_limit),
        ),
        "events": _safe_section(
            "events",
            lambda: store.query(since=event_since, limit=event_limit),
        ),
        "reports": _safe_section("reports", lambda: _read_report_docs(mode)),
    }

    failed_sections = [name for name, value in sections.items() if value.get("status") != "ok"]
    return {
        "status": "warning" if failed_sections else "ok",
        "context_type": "llm_summary_context",
        "mode": mode,
        "generated_at": local_now_str("%Y-%m-%dT%H:%M:%S%z"),
        "execution_allowed": False,
        "failed_sections": failed_sections,
        "guardrails": [
            "只基于本上下文总结，不要臆测外部事实。",
            "不要调用、建议自动调用或伪造 record-buy / record-sell。",
            "明确区分：观察、核心池、买入意向；观察不等于买入。",
            "数据质量降级时，不要提高执行信心。",
            "最终输出必须是简体中文，面向人工确认。",
        ],
        "sections": sections,
    }


def render_llm_context_markdown(payload: dict) -> str:
    """把上下文渲染为 Hermes cron 适合注入 LLM 的 Markdown。"""
    lines = [
        f"# A股 LLM 摘要上下文：{payload.get('mode')}",
        "",
        f"- status: `{payload.get('status')}`",
        f"- generated_at: `{payload.get('generated_at')}`",
        f"- execution_allowed: `{str(payload.get('execution_allowed')).lower()}`",
        f"- failed_sections: `{len(payload.get('failed_sections', []))}`",
        "",
        "## LLM 使用边界",
        "",
    ]
    for guardrail in payload.get("guardrails", []):
        lines.append(f"- {guardrail}")

    for name, section in payload.get("sections", {}).items():
        lines.extend(["", f"## {name}", ""])
        lines.append(f"- status: `{section.get('status')}`")
        if section.get("error"):
            lines.append(f"- error: `{section.get('error')}`")
        lines.extend(["", "```json"])
        lines.append(_truncate(_json_text(section.get("data", section)), 5000))
        lines.append("```")
    return "\n".join(lines)


def _json_text(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, indent=2, default=str)
