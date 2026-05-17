"""只读 LLM 摘要上下文。

该模块把 Hermes/agent 需要的摘要材料收敛到稳定 CLI 表面：
`atrade llm-context --mode ...`。外部调度器不需要进入源码目录，也不需要读取
checkout 内脚本。
"""

from __future__ import annotations

import datetime as dt
import re
from pathlib import Path
from typing import Any

from astock_trading.execution.service import ExecutionService
from astock_trading.platform.agent_diagnostics import diagnose_health, propose_agent_trade_plan
from astock_trading.platform.events import EventStore
from astock_trading.platform.runs import RunJournal
from astock_trading.platform.service_factory import resolve_vault_path
from astock_trading.platform.time import local_now_str

MAX_DOC_CHARS = 3500

MODE_CN = {
    "morning": "盘前摘要",
    "close": "收盘复盘",
    "weekly": "周复盘补充",
}

SECTION_CN = {
    "diagnostics": "健康诊断",
    "trade_plan": "交易计划",
    "portfolio": "持仓与资金",
    "manual_trades": "人工确认",
    "candidates": "候选池",
    "runs": "流水运行记录",
    "events": "事件记录",
    "reports": "报告片段",
}

TERM_CN = {
    "execution_allowed": "是否允许自动执行",
    "failed_sections": "失败的数据段",
    "context_type": "上下文类型",
    "diagnostic": "诊断类型",
    "inputs": "输入数据",
    "checks": "检查项",
    "required": "是否必需",
    "latest_observed_at": "最近观测时间",
    "age_hours": "数据年龄小时数",
    "max_age_hours": "最大允许小时数",
    "source": "数据来源",
    "kind": "数据类型",
    "symbol": "标的代码",
    "payload_count": "数据条数",
    "min_payload_count": "最低数据条数",
    "total_count": "总数",
    "total": "总数",
    "score": "评分",
    "name": "名称",
    "code": "代码",
    "note": "备注",
    "added_at": "加入时间",
    "updated_at": "更新时间",
    "requested_at": "请求时间",
    "latest": "最新记录",
    "llm_summary_context": "LLM 摘要上下文",
    "proposed": "计划已生成但不可执行",
    "ok": "正常",
    "warning": "警告",
    "failed": "失败",
    "error": "错误",
    "degraded": "降级",
    "unknown": "未知",
    "diagnostics": "健康诊断",
    "health": "健康检查",
    "trade_plan": "交易计划",
    "portfolio": "持仓与资金",
    "manual_trades": "人工确认",
    "candidate_pool": "候选池",
    "candidate_pool_freshness": "候选池新鲜度",
    "projection_candidate_pool": "候选池投影表",
    "core_pool": "核心池",
    "core_count": "核心池数量",
    "watch_count": "观察池数量",
    "pool_tier": "池子层级",
    "watch": "观察",
    "core": "核心池",
    "screener_refresh": "筛选器刷新",
    "holding_count": "持仓数量",
    "manual trade": "人工确认交易",
    "manual trades": "人工确认交易",
    "record-buy": "买入记录命令",
    "record-sell": "卖出记录命令",
    "BUY": "买入意向",
    "SELL": "卖出意向",
    "WATCH": "观察",
    "NO_TRADE": "不操作",
    "BUY_ALLOWED": "可买入",
    "REDUCED_BUY": "减量买入",
    "GREEN": "偏强",
    "YELLOW": "震荡",
    "RED": "转弱",
    "CLEAR": "观望",
    "entry_signal": "入场信号",
    "veto": "否决",
    "hard_veto": "硬否决",
    "warning_signals": "预警信号",
    "data_quality": "数据质量",
    "data_sources": "数据源",
    "hot_stocks": "热股榜",
    "northbound_realtime": "北向实时资金",
    "baidu_fund_flow": "百度资金流",
    "industry_comparison": "行业对比",
    "announcements": "公告",
    "research_reports": "研报",
    "stock_news": "个股新闻",
    "basic_info": "基础信息",
    "financial": "财务数据",
    "fund_flow": "资金流",
    "flow": "资金流",
    "down": "不可用",
    "empty": "为空",
    "healthy": "健康",
    "optional_missing": "辅助源缺失",
    "required_missing": "核心源缺失",
    "review_core_pool": "复核核心池",
    "latest_scored_at": "最近评分时间",
    "stale": "是否陈旧",
    "run_log": "流水运行日志",
    "run_type": "流水类型",
    "run_id": "运行编号",
    "type": "类型",
    "status": "状态",
    "actions": "建议动作",
    "findings": "诊断发现",
    "recommendations": "处理建议",
    "required data sources unavailable": "核心数据源不可用",
    "candidate pool is empty": "候选池为空",
    "candidate core pool is empty": "核心候选池为空",
    "refresh required market data sources before scoring or auto_trade": "评分或模拟交易前需要先刷新核心市场数据源",
    "run screener refresh before scoring": "评分前先刷新筛选器",
}

TEXT_REPLACEMENTS = {
    "execution_allowed=false": "系统禁止自动执行",
    "execution_allowed=true": "系统允许自动执行",
    "optional data sources degraded": "辅助数据源降级",
    "candidate core pool is empty": "核心候选池为空",
    "continue read-only analysis, but avoid expanding execution confidence": "继续只读分析，但不要提高执行信心",
    "promote fresh high-score candidates before auto_trade buy-side decisions": "模拟买入决策前先把新鲜高分候选标的提升到核心池",
    "health/diagnostics": "健康诊断",
    "manual trades": "人工确认交易",
    "manual trade": "人工确认交易",
    "portfolio": "持仓与资金",
    "Markdown": "报告片段",
}

GLOSSARY_ORDER = [
    "execution_allowed",
    "proposed",
    "warning",
    "degraded",
    "candidate_pool_freshness",
    "core_pool",
    "candidate_pool",
    "watch",
    "screener_refresh",
    "holding_count",
    "manual_trades",
    "portfolio",
    "record-buy",
    "record-sell",
]


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


def _term_label(term: object) -> str:
    text = str(term)
    return TERM_CN.get(text) or TERM_CN.get(text.lower()) or text


def _translate_text(text: str) -> str:
    translated = text
    for source, target in TEXT_REPLACEMENTS.items():
        translated = translated.replace(source, target)
    for source, target in sorted(TERM_CN.items(), key=lambda item: len(item[0]), reverse=True):
        pattern = re.compile(rf"(?<![A-Za-z0-9_-]){re.escape(source)}(?![A-Za-z0-9_-])")
        translated = pattern.sub(target, translated)
    return translated


def _display_value(value: Any) -> Any:
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, str):
        return _translate_text(value)
    return value


def _display_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {_term_label(key): _display_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_display_payload(item) for item in value]
    if isinstance(value, tuple):
        return [_display_payload(item) for item in value]
    return _display_value(value)


def _term_glossary() -> list[dict]:
    return [{"term": term, "display": TERM_CN[term]} for term in GLOSSARY_ORDER if term in TERM_CN]


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
        "term_policy": {
            "language": "简体中文",
            "rule": "Discord 最终输出不要裸露内部字段名、枚举值或 JSON 路径；必须改写成中文业务含义。",
            "first_mention": "如确需保留协议名，格式为：中文释义（内部字段：protocol_name）。",
        },
        "term_glossary": _term_glossary(),
        "failed_sections": failed_sections,
        "guardrails": [
            "只基于本上下文总结，不要臆测外部事实。",
            "不要调用、建议自动调用或伪造 record-buy / record-sell。",
            "明确区分：观察、核心池、买入意向；观察不等于买入。",
            "数据质量降级时，不要提高执行信心。",
            "最终输出必须是简体中文，面向人工确认；不要裸露内部字段名、枚举值或 JSON 路径。",
        ],
        "sections": sections,
    }


def render_llm_context_markdown(payload: dict) -> str:
    """把上下文渲染为 Hermes cron 适合注入 LLM 的 Markdown。"""
    mode = str(payload.get("mode") or "")
    status_label = _term_label(payload.get("status"))
    execution_label = "允许" if payload.get("execution_allowed") else "禁止"
    lines = [
        f"# A股 LLM 摘要上下文：{MODE_CN.get(mode, mode)}",
        "",
        f"- 上下文状态：{status_label}",
        f"- 生成时间：`{payload.get('generated_at')}`",
        f"- 自动执行：{execution_label}",
        f"- 失败的数据段：{len(payload.get('failed_sections', []))}",
        "",
        "## 输出要求",
        "",
        "- 最终发到 Discord 的内容必须使用中文业务表述，不要直接展示内部字段名、枚举值或 JSON 路径。",
        "- 如果必须保留协议名，第一次出现写成“中文释义（内部字段：protocol_name）”，不要单独裸露英文术语。",
        "- 下方代码块是取数依据，不是最终报告模板；请先转义后再总结。",
    ]
    for guardrail in payload.get("guardrails", []):
        lines.append(f"- {_translate_text(guardrail)}")

    lines.extend(["", "## 内部术语表", "", "以下只用于理解上下文，最终摘要不要复述术语表或裸露内部字段名。"])
    for item in payload.get("term_glossary", []):
        lines.append(f"- `{item['term']}`：{item['display']}")

    for name, section in payload.get("sections", {}).items():
        lines.extend(["", f"## {SECTION_CN.get(name, name)}", ""])
        lines.append(f"- 状态：{_term_label(section.get('status'))}")
        if section.get("error"):
            lines.append(f"- 错误：`{section.get('error')}`")
        lines.extend(["", "```json"])
        lines.append(_truncate(_json_text(_display_payload(section.get("data", section))), 5000))
        lines.append("```")
    return "\n".join(lines)


def _json_text(value: Any) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, indent=2, default=str)
