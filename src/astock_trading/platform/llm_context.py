"""只读 LLM 摘要上下文。

该模块把 Hermes/agent 需要的摘要材料收敛到稳定 CLI 表面：
`atrade llm-context --mode ...`。外部调度器不需要进入源码目录，也不需要读取
checkout 内脚本。
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from astock_trading.execution.service import ExecutionService
from astock_trading.platform.agent_diagnostics import diagnose_flow, diagnose_health, propose_agent_trade_plan
from astock_trading.platform.config import ConfigRegistry
from astock_trading.platform.events import EventStore
from astock_trading.platform.runs import RunJournal
from astock_trading.platform.service_factory import resolve_vault_path
from astock_trading.platform.time import local_date_bounds_utc, local_now_str, local_today

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
    "market_intel": "热点与新闻",
    "close_review": "收盘复盘诊断",
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
    "market_intel": "热点与新闻",
    "sectors": "热门板块",
    "hot_stocks": "热门股",
    "news": "热门新闻",
    "comparison": "盘前收盘对比",
    "morning": "盘前",
    "evening": "收盘",
    "close": "收盘",
    "sector_heatmap": "行业热力图",
    "hot_sectors_industry_change": "强势行业板块",
    "hot_sectors_concept_change": "强势概念板块",
    "hot_sectors_industry_money-flow": "资金流行业板块",
    "cross_platform_hot_stocks": "跨平台热股",
    "xueqiu_hot_stocks": "雪球热搜",
    "finance_flash": "财经快讯",
    "global_risk_news": "海外风险新闻",
    "market_announcements": "公告提示",
    "close_review": "收盘复盘诊断",
    "candidate_funnel": "候选漏斗",
    "simulation_flow": "模拟承接链路",
    "flow_stage": "候选流阶段",
    "approval_gate": "人工审批门",
    "next_window_plan": "下个买入窗口计划",
    "paper_trial": "影子试运行",
    "hot_stock_pool_bridge": "热点入池关系",
    "comparison_readiness": "盘前收盘对比可用性",
    "tomorrow_checklist": "明日复核清单",
    "unresolved_count": "未补齐数量",
    "unresolved_l1_provider_failures": "L1 数据源失败未补齐",
    "new": "新增",
    "persistent": "延续",
    "faded": "降温",
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
    "candidate pool is empty; required data sources are available, so treat this as no qualified candidates after screening": "候选池为空；核心数据源可用，应表述为筛选后没有合格候选，不是市场数据缺失",
    "candidate core pool is empty": "核心候选池为空",
    "refresh required market data sources before scoring or auto_trade": "评分或模拟交易前需要先刷新核心市场数据源",
    "run screener refresh before scoring": "评分前先刷新筛选器",
    "refresh candidates if needed; if it stays empty, report it as no qualified candidates, not missing market data": "必要时刷新候选池；若仍为空，应报告为没有合格候选，不要写成市场数据缺失",
}

TEXT_REPLACEMENTS = {
    "execution_allowed=false": "系统禁止自动执行",
    "execution_allowed=true": "系统允许自动执行",
    "optional data sources degraded": "辅助数据源降级",
    "candidate core pool is empty": "核心候选池为空",
    "candidate pool is empty; required data sources are available, so treat this as no qualified candidates after screening": "候选池为空；核心数据源可用，应表述为筛选后没有合格候选，不是市场数据缺失",
    "continue read-only analysis, but avoid expanding execution confidence": "继续只读分析，但不要提高执行信心",
    "promote fresh high-score candidates before auto_trade buy-side decisions": "模拟买入决策前先把新鲜高分候选标的提升到核心池",
    "refresh candidates if needed; if it stays empty, report it as no qualified candidates, not missing market data": "必要时刷新候选池；若仍为空，应报告为没有合格候选，不要写成市场数据缺失",
    "health/diagnostics": "健康诊断",
    "manual trades": "人工确认交易",
    "manual trade": "人工确认交易",
    "portfolio": "持仓与资金",
    "Markdown": "报告片段",
}

ACTION_CN = {
    "BUY": "买入意向",
    "SELL": "卖出意向",
    "WATCH": "观察",
    "NO_TRADE": "不操作",
    "CLEAR": "观望",
    "HOLD": "持有",
}

BLOCKER_CN = {
    "below_ma20": "跌破 MA20",
    "consecutive_outflow": "连续资金流出",
    "ma20_trend_down": "MA20 趋势向下",
    "limit_up_today": "当日涨停不追",
    "requires_entry_strategy_route": "缺少有效策略路线",
    "entry_signal": "入场信号不足",
    "no_entry_signal": "入场信号不足",
    "data_quality_degraded": "数据质量降级",
    "candidate_pool_empty": "候选池为空",
    "core_pool_empty": "核心池为空",
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
    "market_intel",
    "sector_heatmap",
    "cross_platform_hot_stocks",
    "finance_flash",
    "global_risk_news",
]

DISCIPLINE_LINES = [
    "先保本金，再谈收益。",
    "看不懂的时候，不操作也是动作。",
    "观察不等于买入，热度不等于确定性。",
    "数据降级时，信心也要降级。",
    "计划外的交易，先当风险处理。",
    "错过机会可以复盘，突破纪律必须停止。",
]

DISCORD_CARD_TEMPLATES = {
    "morning": """## A股盘前摘要｜YYYY-MM-DD 09:20

**今日结论：不操作 / 观察 / 待人工复核**
自动执行：禁止
数据来源：09:15 盘前流水 / 最新缓存（非正式盘前数据）

### 1. 系统与数据质量
- 系统状态：正常 / 警告 / 失败
- 数据质量：正常 / 降级 / 失败
- 缺失或异常：候选池新鲜度、核心池、行情源、新闻源等
- 对交易判断的影响：可参考 / 只能观察 / 不建议提高信心

### 2. 今日动作
- 默认动作：不操作 / 只读观察 / 等待人工确认
- 买入意向：无 / 有但需人工确认
- 自动扩仓：禁止
- 主要阻断原因：数据质量、核心池为空、入场信号不足等

### 3. 市场热点
**热门板块**
- 板块A：一句话原因

**热门新闻**
- 新闻A：影响方向

**热门股**
- 股票A：热度来源 / 涨跌幅 / 只作观察

> 热点只作为市场背景和复盘线索，不作为买入依据。

### 4. 候选池
- 核心池：N 只
- 观察池：N 只
- 买入意向：N 只
- 主要阻断原因：数据质量 / 入场信号不足 / 核心池为空 / 新鲜度不足

### 5. 持仓与风险
- 当前持仓：空仓 / N 只
- 待人工确认事项：无 / N 条
- 风险提醒：止损、数据异常、报告滞后、不一致项

### 6. 今日纪律
- 禁止自动买入或卖出
- 禁止把观察名单当作买入意向
- 禁止在数据降级时提高执行信心
- 风控短句：从下方“风控短句候选”选择 1 句""",
    "close": """## A股收盘复盘｜YYYY-MM-DD 15:55

**今日闭环：完成 / 部分完成 / 异常**
自动执行：禁止
今日交易动作：无自动交易 / 有人工确认事项 / 有待复核事项

### 1. 系统与数据质量
- 盘前流水：完成 / 缺失 / 异常
- 盘中风控：完成 / 告警 / 异常
- 收盘流水：完成 / 缺失 / 异常
- 数据质量：正常 / 降级 / 失败
- 未补齐数据源：列出源、标的、错误类型和 fallback 状态
- 对复盘结论的影响：可信 / 仅供参考 / 需要人工复核

### 2. 今日闭环
- 今日交易动作：无 / 有人工确认 / 有待复核
- 人工确认事项：无 / N 条
- 最终动作：不操作 / 继续观察 / 等待人工确认

### 3. 收盘市场热点
**热门板块**
- 板块A：收盘表现与原因

**热门新闻**
- 新闻A：收盘后影响

**热门股**
- 股票A：热度来源 / 涨跌幅 / 是否进入观察
- 若热门股未进入候选池，写明“仅为召回线索，未通过候选漏斗”

### 4. 盘前 vs 收盘
- 延续热点：盘前出现且收盘仍强的板块 / 个股
- 新增热点：盘前没有、收盘出现的板块 / 个股
- 降温热点：盘前热、收盘弱化的板块 / 个股
- 判断质量：盘前判断有效 / 偏弱 / 数据不足无法判断
- 对比只用于复盘早盘判断质量，不作为自动交易依据

### 5. 候选池变化
- 核心池：早盘 N → 收盘 N
- 观察池：早盘 N → 收盘 N
- 候选漏斗：评分 N → 入场信号 N → 观察 N → 核心 N → 买入意向 N
- 主要否决原因：按数量列出 top3
- 新增观察：股票A、股票B
- 移出/降级：股票C，原因
- 仍不可买原因：入场信号不足 / 风控否决 / 数据质量不足

### 6. 持仓与风险
- 当前持仓：空仓 / N 只
- 今日浮盈亏：如有则展示
- 风控事项：止损、移动止盈、异常波动、数据不一致
- 需人工复核：具体事项

### 7. 明日清单
- 明日继续观察：股票 / 板块
- 明日优先复核：数据源、候选池、人工确认
- 明日可执行检查：列出 1-3 条 atrade ... --json 命令
- 明日禁止事项：继续禁止自动执行，观察不等于买入
- 风控短句：从下方“风控短句候选”选择 1 句""",
}


def _today() -> dt.date:
    return local_today()


def _today_iso() -> str:
    return _today().isoformat()


def _week_start_iso() -> str:
    today = _today()
    monday = today - dt.timedelta(days=today.weekday())
    start_utc, _ = local_date_bounds_utc(monday)
    return start_utc


def _today_start_iso() -> str:
    start_utc, _ = local_date_bounds_utc(_today())
    return start_utc


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


def _discord_card_contract(mode: str) -> dict:
    template = DISCORD_CARD_TEMPLATES.get(mode, "")
    if not template:
        return {}
    rules = [
        "最终回复必须按模板结构输出，保留标题和章节顺序。",
        "不要输出原始 JSON、内部字段名、枚举值或 JSON 路径。",
        "缺少数据时写“暂无可用数据”，不要臆测外部事实。",
        "系统与数据质量必须作为第 1 区块，并决定后续结论可信度。",
        "热点只作为市场背景和复盘线索，不作为买入依据。",
        "末尾只选择 1 句风控短句，不要堆砌多句。",
    ]
    if mode == "close":
        rules.insert(
            -1,
            "收盘复盘必须使用“收盘复盘诊断”里的数据源失败、候选漏斗、热点入池关系和明日复核清单。",
        )
    return {
        "format": "Discord Markdown card",
        "template": template,
        "rules": rules,
        "discipline_lines": DISCIPLINE_LINES,
    }


def _summary_guardrails(mode: str) -> list[str]:
    guardrails = [
        "只基于本上下文总结，不要臆测外部事实。",
        "每个判断段落必须引用 evidence_id；没有证据编号的内容只能写“暂无可用数据”。",
        "证据编号必须引用同一数据段或同一标的的编号，不要跨段借用。",
        "不要调用、建议自动调用或伪造 record-buy / record-sell。",
        "明确区分：观察、核心池、买入意向；观察不等于买入。",
        "热门板块、热门新闻、热门股只作为市场背景和复盘线索，不等于买入依据。",
        "数据质量降级时，不要提高执行信心。",
        "最终输出必须是简体中文，面向人工确认；不要裸露内部字段名、枚举值或 JSON 路径。",
    ]
    if mode == "morning":
        guardrails.append("盘前摘要要说明热点数据来自盘前流水还是最新缓存。")
    elif mode == "close":
        guardrails.append("收盘复盘要对比盘前与收盘热点变化，用于评估早盘判断质量。")
        guardrails.append("收盘复盘必须写清楚候选漏斗：评分数、入场信号、观察池、核心池、买入意向和主要否决原因。")
        guardrails.append("收盘复盘如发现热点未入池，只能写成召回线索或明日复核项，不得写成买入依据。")
    elif mode == "weekly":
        guardrails.append("周复盘只提炼仍有解释价值的运行质量、交易质量和信号质量问题。")
    return guardrails


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


def _decode_payload(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, dict | list):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return {"raw": str(value)}


def _latest_market_observation(
    conn: Any,
    kind: str,
    *,
    run_type: str | None = None,
    fallback_to_latest: bool = False,
) -> dict | None:
    params: list[Any] = [kind, _today_start_iso()]
    run_filter = ""
    if run_type:
        run_filter = " AND r.run_type = ?"
        params.append(run_type)
    row = conn.execute(
        f"""SELECT m.observation_id, m.source, m.kind, m.symbol, m.observed_at, m.run_id, m.payload_json,
                  r.run_type
             FROM market_observations m
             LEFT JOIN run_log r ON m.run_id = r.run_id
            WHERE m.kind = ? AND m.observed_at >= ?{run_filter}
            ORDER BY m.observed_at DESC
            LIMIT 1""",
        tuple(params),
    ).fetchone()
    if row is None and run_type is not None:
        fallback = _latest_market_observation(conn, kind)
        if fallback and (fallback_to_latest or _observation_run_matches(fallback.get("run_id", ""), run_type)):
            fallback["requested_run_type"] = run_type
            fallback["fallback_used"] = True
            if _observation_run_matches(fallback.get("run_id", ""), run_type):
                fallback["run_type"] = run_type
            return fallback
        return None
    if row is None:
        return None
    return {
        "observation_id": row["observation_id"],
        "source": row["source"],
        "kind": row["kind"],
        "symbol": row["symbol"],
        "observed_at": row["observed_at"],
        "run_id": row["run_id"],
        "run_type": row["run_type"],
        "requested_run_type": run_type or "",
        "fallback_used": False,
        "payload": _decode_payload(row["payload_json"]),
    }


def _observation_run_matches(run_id: object, run_type: str) -> bool:
    text = str(run_id or "").lower()
    aliases = {"evening": ("evening", "close"), "morning": ("morning",), "noon": ("noon",)}
    return any(alias in text for alias in aliases.get(run_type, (run_type,)))


def _payload_items(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("items", "stocks", "top", "news", "announcements"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _compact_sector(item: dict, *, evidence_id: str = "", observed_at: str = "", source_kind: str = "") -> dict:
    return {
        "name": item.get("name") or item.get("板块") or "",
        "change_pct": item.get("change_pct", item.get("涨跌幅", 0)) or 0,
        "amount": item.get("amount", item.get("成交额", item.get("main_net", 0))) or 0,
        "lead_stock": item.get("lead_stock") or item.get("leader") or "",
        "up_count": item.get("up_count", 0) or 0,
        "down_count": item.get("down_count", 0) or 0,
        "source": item.get("source", ""),
        "source_kind": source_kind,
        "observed_at": observed_at,
        "evidence_id": evidence_id,
    }


def _hot_stock_change_context(source_label: str) -> str:
    if source_label in {"跨平台热股", "雪球热搜"}:
        return "热榜口径，非实时行情"
    return "题材热榜口径，非实时行情"


def _compact_stock(
    item: dict,
    source_label: str,
    *,
    evidence_id: str = "",
    observed_at: str = "",
    source_kind: str = "",
) -> dict:
    return {
        "code": item.get("code") or item.get("symbol") or "",
        "name": item.get("name") or item.get("symbol") or item.get("code") or "",
        "rank": item.get("rank", ""),
        "change_pct": item.get("change_pct", 0) or 0,
        "change_pct_context": _hot_stock_change_context(source_label),
        "is_realtime_quote": False,
        "heat": item.get("heat", 0) or 0,
        "source_count": item.get("source_count", ""),
        "sources": item.get("sources", []),
        "reason": item.get("reason", ""),
        "source": source_label,
        "source_kind": source_kind,
        "observed_at": observed_at,
        "evidence_id": evidence_id,
    }


def _compact_news(
    item: dict,
    source_label: str,
    *,
    evidence_id: str = "",
    observed_at: str = "",
    source_kind: str = "",
) -> dict:
    return {
        "title": item.get("title") or item.get("content") or item.get("text") or "",
        "time": item.get("time", ""),
        "source": item.get("source") or source_label,
        "summary": item.get("summary") or item.get("content") or "",
        "code": item.get("code", ""),
        "name": item.get("name", ""),
        "category": item.get("category", ""),
        "kind": source_label,
        "source_kind": source_kind,
        "observed_at": observed_at,
        "evidence_id": evidence_id,
    }


def _dedupe(items: list[dict], key_fn, *, limit: int) -> list[dict]:
    seen = set()
    result = []
    for item in items:
        key = key_fn(item)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(item)
        if len(result) >= limit:
            break
    return result


def _as_float(value: object) -> float:
    try:
        return float(str(value or 0).replace("%", "").replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _sector_has_effective_direction(item: dict) -> bool:
    return bool(
        abs(_as_float(item.get("change_pct", 0))) > 0
        or _as_float(item.get("amount", 0)) > 0
        or _as_float(item.get("up_count", 0)) > 0
        or _as_float(item.get("down_count", 0)) > 0
        or str(item.get("lead_stock") or "").strip()
    )


def _market_snapshot(conn: Any, *, run_type: str | None, fallback_to_latest: bool = False) -> dict:
    sector_kinds = ("sector_heatmap", "hot_sectors_industry_change", "hot_sectors_concept_change")
    stock_kinds = ("cross_platform_hot_stocks", "xueqiu_hot_stocks", "hot_stocks")
    news_kinds = ("finance_flash", "market_announcements", "global_risk_news")
    observations: dict[str, dict] = {}
    fallback_used = False
    data_notes: list[str] = []

    def latest(kind: str) -> dict | None:
        obs = _latest_market_observation(conn, kind, run_type=run_type, fallback_to_latest=fallback_to_latest)
        if obs:
            nonlocal fallback_used
            fallback_used = fallback_used or bool(obs.get("fallback_used"))
            observations[kind] = {
                "observation_id": obs["observation_id"],
                "kind": kind,
                "label": TERM_CN.get(kind, kind),
                "observed_at": obs["observed_at"],
                "run_id": obs["run_id"],
                "run_type": obs.get("run_type") or "",
                "requested_run_type": obs.get("requested_run_type") or "",
                "fallback_used": bool(obs.get("fallback_used")),
                "source": obs["source"],
                "count": len(_payload_items(obs["payload"])),
            }
        return obs

    sectors: list[dict] = []
    for kind in sector_kinds:
        obs = latest(kind)
        if not obs:
            continue
        source_label = TERM_CN.get(kind, kind)
        evidence_id = str(obs.get("observation_id") or "")
        observed_at = str(obs.get("observed_at") or "")
        for item in _payload_items(obs["payload"]):
            compacted = _compact_sector(
                item,
                evidence_id=evidence_id,
                observed_at=observed_at,
                source_kind=kind,
            )
            compacted["source"] = compacted.get("source") or source_label
            sectors.append(compacted)
    sectors = _dedupe(sectors, lambda item: item.get("name"), limit=8)
    sector_count_before_direction_filter = len(sectors)
    sectors = [item for item in sectors if _sector_has_effective_direction(item)]
    if sector_count_before_direction_filter and not sectors:
        data_notes.append("盘前行业热力图已有观测，但涨跌、成交和涨跌家数均未形成有效方向，不能当作热门板块。")

    stocks: list[dict] = []
    for kind in stock_kinds:
        obs = latest(kind)
        if not obs:
            continue
        source_label = TERM_CN.get(kind, kind)
        evidence_id = str(obs.get("observation_id") or "")
        observed_at = str(obs.get("observed_at") or "")
        stocks.extend(
            _compact_stock(
                item,
                source_label,
                evidence_id=evidence_id,
                observed_at=observed_at,
                source_kind=kind,
            )
            for item in _payload_items(obs["payload"])
        )
    stocks = _dedupe(stocks, lambda item: item.get("code") or item.get("name"), limit=10)
    if stocks:
        data_notes.append("热门股涨跌幅来自热榜或题材榜口径，不是 A 股实时行情；如需实时价格必须另查个股行情。")

    news: list[dict] = []
    for kind in news_kinds:
        obs = latest(kind)
        if not obs:
            continue
        source_label = TERM_CN.get(kind, kind)
        evidence_id = str(obs.get("observation_id") or "")
        observed_at = str(obs.get("observed_at") or "")
        news.extend(
            _compact_news(
                item,
                source_label,
                evidence_id=evidence_id,
                observed_at=observed_at,
                source_kind=kind,
            )
            for item in _payload_items(obs["payload"])
        )
    news = _dedupe(news, lambda item: item.get("title"), limit=10)

    return {
        "phase": TERM_CN.get(run_type or "latest", run_type or "latest"),
        "available": bool(sectors or stocks or news),
        "fallback_used": fallback_used,
        "fallback_note": "未找到指定流水的热点缓存，已退回最新可用热点缓存。" if fallback_used else "",
        "sectors": sectors,
        "hot_stocks": stocks,
        "news": news,
        "data_notes": data_notes,
        "observations": observations,
    }


def _item_key(item: dict, *fields: str) -> str:
    for field in fields:
        value = str(item.get(field) or "").strip()
        if value:
            return value
    return ""


def _compare_items(before: list[dict], after: list[dict], *, key_fields: tuple[str, ...], limit: int = 5) -> dict:
    before_keys = {_item_key(item, *key_fields) for item in before if _item_key(item, *key_fields)}
    after_keys = {_item_key(item, *key_fields) for item in after if _item_key(item, *key_fields)}
    return {
        "persistent": [item for item in after if _item_key(item, *key_fields) in before_keys][:limit],
        "new": [item for item in after if _item_key(item, *key_fields) not in before_keys][:limit],
        "faded": [item for item in before if _item_key(item, *key_fields) not in after_keys][:limit],
    }


def _market_comparison(morning: dict, close: dict) -> dict:
    if not morning.get("available") or not close.get("available"):
        return {
            "available": False,
            "reason": "盘前或收盘热点数据不足，暂不做强对比。",
        }
    return {
        "available": True,
        "sectors": _compare_items(morning["sectors"], close["sectors"], key_fields=("name",)),
        "hot_stocks": _compare_items(morning["hot_stocks"], close["hot_stocks"], key_fields=("code", "name")),
        "news": _compare_items(morning["news"], close["news"], key_fields=("title",)),
        "interpretation": [
            "延续热点代表盘前线索被收盘验证，但仍需价格、资金和风控确认。",
            "新增热点只作为复盘线索，不等于次日买入意向。",
            "降温热点用于检查盘前叙事是否失效。",
        ],
    }


def _market_intel_context(conn: Any, *, mode: str) -> dict:
    morning = _market_snapshot(conn, run_type="morning", fallback_to_latest=(mode == "morning"))
    if mode == "morning":
        return {
            "mode": mode,
            "current": morning,
            "summary_requirements": [
                "盘前摘要必须列出热门板块、热门新闻、热门股；没有数据时明确说数据不足。",
                "如果使用的是最新缓存而非盘前流水，必须说明这是非交易日/手动运行的参考数据。",
                "热点只作为观察线索，不得直接升级为买入意向。",
            ],
        }

    close = _market_snapshot(conn, run_type="evening")
    if mode == "close":
        return {
            "mode": mode,
            "morning": morning,
            "close": close,
            "comparison": _market_comparison(morning, close),
            "summary_requirements": [
                "收盘复盘必须列出收盘热门板块、热门新闻、热门股。",
                "收盘复盘必须对比盘前与收盘：延续、新增、降温分别说明。",
                "对比只用于复盘早盘判断质量，不作为自动交易依据。",
            ],
        }

    latest = _market_snapshot(conn, run_type=None)
    return {
        "mode": mode,
        "latest": latest,
        "summary_requirements": [
            "周复盘只总结一周内仍有解释价值的热点线索，不重复日内噪音。",
        ],
    }


def _score_value(payload: dict) -> float:
    for key in ("total_score", "score", "total"):
        try:
            return float(payload.get(key) or 0)
        except (TypeError, ValueError):
            continue
    return 0.0


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "是", "有"}
    return bool(value)


def _counter_rows(counter: Counter, *, labels: dict[str, str] | None = None, limit: int = 8) -> list[dict]:
    label_map = labels or {}
    return [
        {
            "value": key,
            "label": label_map.get(key) or _term_label(key),
            "count": count,
        }
        for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))[:limit]
        if key
    ]


def _compact_score_event(event: dict) -> dict:
    payload = event.get("payload", {}) or {}
    hard_veto = [str(item) for item in (payload.get("hard_veto_signals") or payload.get("hard_veto") or [])]
    return {
        "event_id": event.get("event_id", ""),
        "occurred_at": event.get("occurred_at", ""),
        "code": payload.get("code", ""),
        "name": payload.get("name", ""),
        "score": _score_value(payload),
        "data_quality": payload.get("data_quality", "unknown"),
        "entry_signal": _truthy(payload.get("entry_signal")),
        "hard_veto_signals": hard_veto,
        "hard_veto_labels": [BLOCKER_CN.get(item, _term_label(item)) for item in hard_veto],
        "data_missing_fields": [str(item) for item in (payload.get("data_missing_fields") or [])],
    }


def _candidate_funnel_context(conn: Any, store: EventStore, *, since: str) -> dict:
    pool_rows = _candidate_rows(conn, limit=200)
    pool_counter: Counter = Counter(str(row.get("pool_tier") or "unknown") for row in pool_rows)
    latest_scored_at = max(
        (str(row.get("last_scored_at") or row.get("added_at") or "") for row in pool_rows),
        default="",
    )

    score_events = store.query(event_type="score.calculated", since=since, limit=500)
    decision_events = store.query(event_type="decision.suggested", since=since, limit=500)
    score_payloads = [event.get("payload", {}) or {} for event in score_events]
    decision_payloads = [event.get("payload", {}) or {} for event in decision_events]

    entry_signal_count = sum(1 for payload in score_payloads if _truthy(payload.get("entry_signal")))
    quality_counter: Counter = Counter(str(payload.get("data_quality", "unknown")) for payload in score_payloads)
    missing_counter: Counter = Counter()
    hard_veto_counter: Counter = Counter()
    decision_veto_counter: Counter = Counter()
    action_counter: Counter = Counter()

    for payload in score_payloads:
        missing_counter.update(str(item) for item in (payload.get("data_missing_fields") or []))
        hard_veto_counter.update(
            str(item) for item in (payload.get("hard_veto_signals") or payload.get("hard_veto") or [])
        )
    for payload in decision_payloads:
        action_counter.update([str(payload.get("action") or "unknown")])
        decision_veto_counter.update(str(item) for item in (payload.get("veto_reasons") or []))

    top_scores = sorted((_compact_score_event(event) for event in score_events), key=lambda item: item["score"], reverse=True)[:10]
    buy_intent_count = action_counter.get("BUY", 0)
    explanation = _empty_pool_attribution(
        pool_rows=pool_rows,
        score_count=len(score_payloads),
        entry_signal_count=entry_signal_count,
        hard_veto_counter=hard_veto_counter,
        decision_veto_counter=decision_veto_counter,
    )
    return {
        "pool": {
            "total": len(pool_rows),
            "core_count": pool_counter.get("core", 0),
            "watch_count": pool_counter.get("watch", 0),
            "latest_scored_at": latest_scored_at,
            "rows": pool_rows[:20],
        },
        "scores": {
            "total": len(score_payloads),
            "entry_signal": {
                "triggered": entry_signal_count,
                "missing": max(len(score_payloads) - entry_signal_count, 0),
            },
            "data_quality": _counter_rows(quality_counter, limit=6),
            "missing_fields": _counter_rows(missing_counter, limit=8),
            "top_scores": top_scores,
        },
        "decisions": {
            "total": len(decision_payloads),
            "buy_intents": buy_intent_count,
            "actions": _counter_rows(action_counter, labels=ACTION_CN, limit=8),
        },
        "blockers": {
            "hard_veto_reasons": _counter_rows(hard_veto_counter, labels=BLOCKER_CN, limit=8),
            "decision_veto_reasons": _counter_rows(decision_veto_counter, labels=BLOCKER_CN, limit=8),
        },
        "empty_or_core_gap_explanation": explanation,
        "rule": "候选池为空或核心池为空时，只能说明暂无合格候选或需要复核候选漏斗，不得直接放宽买入线。",
    }


def _empty_pool_attribution(
    *,
    pool_rows: list[dict],
    score_count: int,
    entry_signal_count: int,
    hard_veto_counter: Counter,
    decision_veto_counter: Counter,
) -> str:
    core_count = sum(1 for row in pool_rows if str(row.get("pool_tier")) == "core")
    if pool_rows and core_count:
        return "候选池和核心池都有数据，重点复核是否有买入意向和人工确认事项。"
    if pool_rows:
        return "候选池有观察标的但核心池为空；应复核晋级规则、连续评分和入场信号，不要把观察直接当买入意向。"
    if score_count == 0:
        return "候选池为空，且今日没有评分事件；先确认 screener refresh/scoring 是否执行并写入历史镜像。"
    if entry_signal_count == 0:
        return "候选池为空，今日已有评分事件，但入场信号全部缺失；优先复核策略路线和入场触发条件。"
    if decision_veto_counter:
        reason = _counter_rows(decision_veto_counter, labels=BLOCKER_CN, limit=1)[0]["label"]
        return f"候选池为空，今日已有评分和决策事件，主要决策否决原因是：{reason}。"
    if hard_veto_counter:
        reason = _counter_rows(hard_veto_counter, labels=BLOCKER_CN, limit=1)[0]["label"]
        return f"候选池为空，今日已有评分事件，主要硬否决原因是：{reason}。"
    return "候选池为空，但已有评分事件；应复核候选提升、投影重建和评分阈值是否一致。"


def _provider_failure_context(trade_plan: dict, diagnostics: dict) -> dict:
    diagnosis = trade_plan.get("data_source_diagnosis", {}) or {}
    provider_failures = diagnosis.get("provider_failures")
    if not provider_failures:
        provider_failures = ((diagnostics.get("inputs", {}) or {}).get("data_sources", {}) or {}).get(
            "provider_failures",
            {},
        )
    unresolved = provider_failures.get("unresolved", []) or []
    provider_incidents = diagnosis.get("provider_incidents") or {}
    unresolved_count = int(provider_failures.get("unresolved_recent", len(unresolved)) or 0)
    data_source_blockers = trade_plan.get("data_source_blockers", []) or []
    actionable_count = int(
        provider_incidents.get("actionable_unresolved_recent")
        if provider_incidents
        else unresolved_count if data_source_blockers else 0
    )
    non_actionable_count = int(
        provider_incidents.get("non_actionable_unresolved_recent")
        if provider_incidents
        else max(unresolved_count - actionable_count, 0)
    )
    return {
        "unresolved_count": unresolved_count,
        "actionable_unresolved_count": actionable_count,
        "non_actionable_unresolved_count": non_actionable_count,
        "resolved_count": int(provider_failures.get("resolved_recent", 0) or 0),
        "by_unresolved_source": provider_failures.get("by_unresolved_source", {}) or {},
        "unresolved": [_compact_provider_failure(item) for item in unresolved[:8]],
        "data_source_blockers": data_source_blockers,
        "recommended_command": "atrade data-sources diagnose --json",
    }


def _compact_provider_failure(item: dict) -> dict:
    details = item.get("details") if isinstance(item.get("details"), dict) else {}
    provider_diagnostic = details.get("provider_diagnostic") if isinstance(details, dict) else {}
    subsource_errors = {}
    if isinstance(provider_diagnostic, dict):
        raw_errors = provider_diagnostic.get("subsource_errors")
        if isinstance(raw_errors, dict):
            subsource_errors = {str(key): str(value) for key, value in raw_errors.items()}
    return {
        "source": item.get("source", ""),
        "target_kind": item.get("target_kind", ""),
        "symbol": item.get("symbol", ""),
        "status": item.get("status", ""),
        "error_type": item.get("error_type", ""),
        "error_message": item.get("error_message", ""),
        "observed_at": item.get("observed_at", ""),
        "age_hours": item.get("age_hours"),
        "run_id": item.get("run_id", ""),
        "resolved_by_fallback": bool(item.get("resolved_by_fallback")),
        "resolved_source": item.get("resolved_source", ""),
        "subsource_errors": subsource_errors,
    }


def _hot_stock_pool_bridge(market_intel: dict, pool_rows: list[dict]) -> dict:
    close_snapshot = market_intel.get("close") or market_intel.get("current") or {}
    hot_stocks = close_snapshot.get("hot_stocks", []) or []
    pool_by_code = {str(row.get("code") or ""): row for row in pool_rows if row.get("code")}
    pool_by_name = {str(row.get("name") or ""): row for row in pool_rows if row.get("name")}
    matched = []
    not_in_pool = []
    for item in hot_stocks[:10]:
        code = str(item.get("code") or "")
        name = str(item.get("name") or "")
        pool_item = pool_by_code.get(code) or pool_by_name.get(name)
        compacted = {
            "code": code,
            "name": name,
            "source": item.get("source", ""),
            "rank": item.get("rank", ""),
            "source_count": item.get("source_count", ""),
            "heat": item.get("heat", 0),
        }
        if pool_item:
            matched.append({**compacted, "pool_tier": pool_item.get("pool_tier", ""), "score": pool_item.get("score")})
        else:
            not_in_pool.append(compacted)
    return {
        "hot_stock_count": len(hot_stocks),
        "matched_pool": matched,
        "not_in_pool": not_in_pool,
        "rule": "热门股只作为召回线索；未进入候选池表示尚未通过行情、资金、评分或风控漏斗。",
        "recommended_command": "atrade market-intel hot-stocks --json",
    }


def _comparison_readiness(market_intel: dict) -> dict:
    comparison = market_intel.get("comparison", {}) or {}
    morning = market_intel.get("morning", {}) or {}
    close = market_intel.get("close", {}) or {}
    missing = []
    if not morning.get("available"):
        missing.append("盘前热点数据")
    if not close.get("available"):
        missing.append("收盘热点数据")
    return {
        "available": bool(comparison.get("available")),
        "missing_inputs": missing,
        "reason": comparison.get("reason", ""),
        "fallback_used": bool(morning.get("fallback_used") or close.get("fallback_used")),
        "recommended_command": f"atrade history signal --date {_today_iso()} --json",
    }


def _compact_action(action: dict | None) -> dict:
    if not isinstance(action, dict):
        return {}
    keys = (
        "type",
        "label",
        "command",
        "reason",
        "safe_to_auto_apply",
        "writes_state",
        "writes_environment",
        "writes_order",
        "requires_user_approval",
        "risk_level",
        "command_contract_id",
    )
    compact = {key: action.get(key) for key in keys if key in action}
    contract = _compact_action_contract(
        command=str(compact.get("command") or ""),
        action_type=str(compact.get("type") or ""),
    )
    for key, value in contract.items():
        compact.setdefault(key, value)
    return compact


def _compact_action_contract(*, command: str, action_type: str = "") -> dict:
    if not command:
        return {}
    if "strategy profile-activation" in command and "--apply-env" in command:
        return _flat_action_contract(
            "strategy_profile_activation_apply",
            writes_state=True,
            writes_environment=True,
            requires_user_approval=True,
            risk_level="environment_write",
        )
    if "strategy profile-activation" in command or action_type in {
        "review_runtime_profile_activation",
        "review_recorded_profile_activation",
    }:
        return _flat_action_contract("strategy_profile_activation_review")
    if command == "atrade diagnose schedule --json":
        return _flat_action_contract("diagnose_schedule")
    if command == "atrade paper auto-readiness --json":
        return _flat_action_contract("paper_auto_readiness")
    if command == "atrade paper trial-plan --json":
        return _flat_action_contract("paper_trial_plan")
    if command == "atrade paper trial-review --min-age-days 0 --record --json":
        return _flat_action_contract(
            "paper_trial_review_record",
            writes_state=True,
            risk_level="state_write",
        )
    if command.startswith("atrade stock analyze "):
        return _flat_action_contract("stock_analyze")
    if command == "atrade screener refresh --json":
        return _flat_action_contract("screener_refresh", writes_state=True, risk_level="state_write")
    if command in {
        "atrade diagnose flow --json",
        "atrade data-sources diagnose --json",
        "atrade health --json",
        "atrade screener explain --json",
        "atrade opportunity --json",
        "atrade suggest --json",
    }:
        command_contract_id = command.replace("atrade ", "").replace(" --json", "").replace(" ", "_").replace("-", "_")
        return _flat_action_contract(command_contract_id)
    return {}


def _flat_action_contract(
    command_contract_id: str,
    *,
    writes_state: bool = False,
    writes_environment: bool = False,
    writes_order: bool = False,
    requires_user_approval: bool = False,
    risk_level: str = "read_only",
) -> dict:
    return {
        "writes_state": writes_state,
        "writes_environment": writes_environment,
        "writes_order": writes_order,
        "requires_user_approval": requires_user_approval,
        "risk_level": risk_level,
        "command_contract_id": command_contract_id,
    }


def _compact_event_reference(item: dict | None, *, event_type: str = "", label: str = "") -> dict:
    if not isinstance(item, dict) or not item.get("event_id"):
        return {}
    return {
        "event_id": item.get("event_id", ""),
        "evidence_id": item.get("event_id", ""),
        "occurred_at": item.get("occurred_at", ""),
        "event_type": item.get("event_type") or event_type,
        "label": label,
    }


def _compact_blockers(items: list | None, *, limit: int = 8) -> list[dict]:
    rows = []
    for item in (items or [])[:limit]:
        if not isinstance(item, dict):
            continue
        rows.append({
            "reason": item.get("reason", ""),
            "label": item.get("label") or _term_label(item.get("reason", "")),
        })
    return rows


def _auto_readiness_for_llm(conn: Any) -> dict:
    """用现有 auto_trade 预检口径构建只读摘要，默认跳过外部模拟账户读取。"""
    try:
        from astock_trading.pipeline.auto_trade import build_auto_trade_readiness

        data, _ = ConfigRegistry().load_and_validate()
        ctx = SimpleNamespace(
            conn=conn,
            cfg=data.get("strategy", {}) or {},
            event_store=EventStore(conn),
            run_journal=RunJournal(conn),
        )
        return build_auto_trade_readiness(ctx, include_account=False)
    except Exception as exc:
        return {
            "status": "unavailable",
            "summary": f"模拟承接预检读取失败：{exc}",
            "blockers": [{
                "reason": "auto_readiness_unavailable",
                "label": "模拟承接预检读取失败",
            }],
            "next_action": {
                "type": "paper_auto_readiness",
                "label": "检查模拟承接预检",
                "command": "atrade paper auto-readiness --json",
                "safe_to_auto_apply": True,
            },
        }


def _simulation_flow_context(conn: Any) -> dict:
    """收盘复盘专用的模拟承接链路摘要；只读，不运行 pipeline。"""
    auto_readiness = _auto_readiness_for_llm(conn)
    try:
        flow = diagnose_flow(conn, auto_readiness=auto_readiness)
    except Exception as exc:
        return {
            "status": "unavailable",
            "summary": f"模拟承接链路读取失败：{exc}",
            "recommended_command": "atrade diagnose flow --json",
            "guardrails": {
                "read_only": True,
                "runs_pipeline": False,
                "places_paper_order": False,
            },
        }

    opportunity = flow.get("opportunity", {}) or {}
    auto_readiness = flow.get("auto_readiness", {}) or {}
    automation = flow.get("automation", {}) or {}
    flow_stage = flow.get("flow_stage", {}) or {}
    candidate_pool = flow.get("candidate_pool", {}) or {}
    next_window_plan = flow.get("next_window_plan", {}) or {}
    approval_gate = flow.get("approval_gate", {}) or {}
    execution_profile = auto_readiness.get("execution_profile", {}) or {}
    schedule_profile = (
        (((automation.get("schedule", {}) or {}).get("runtime_profile", {}) or {}).get("latest_activation_request"))
        or {}
    )
    activation_request = (
        flow_stage.get("latest_activation_request")
        or execution_profile.get("latest_activation_request")
        or schedule_profile
        or {}
    )

    stage_status = flow_stage.get("status") or flow.get("status", "unknown")
    automation_schedule = _automation_schedule_context(automation.get("schedule", {}) or {})
    return {
        "status": stage_status,
        "diagnostic_status": flow.get("status", "unknown"),
        "summary": flow.get("summary", ""),
        "flow_stage": {
            "status": flow_stage.get("status", ""),
            "label": flow_stage.get("label", ""),
            "summary": flow_stage.get("summary", ""),
            "next_action": _compact_action(flow_stage.get("next_action")),
            "recent_unusable_buy_signal": flow_stage.get("recent_unusable_buy_signal", {}) or {},
            "latest_activation_request": _compact_event_reference(
                activation_request,
                event_type="strategy.profile_activation.requested",
                label=str(approval_gate.get("target_profile") or activation_request.get("target_profile") or ""),
            ),
        },
        "candidate_pool": {
            "total": candidate_pool.get("total", candidate_pool.get("total_count", 0)),
            "core_count": candidate_pool.get("core_count", 0),
            "watch_count": candidate_pool.get("watch_count", 0),
            "radar_count": candidate_pool.get("radar_count", 0),
            "latest_scored_at": candidate_pool.get("latest_scored_at"),
        },
        "opportunity": {
            "status": opportunity.get("status", ""),
            "summary": opportunity.get("summary", ""),
            "counts": opportunity.get("counts", {}) or {},
            "next_action": _compact_action(opportunity.get("next_action")),
            "recent_unusable_buy_signal": opportunity.get("recent_unusable_buy_signal", {}) or {},
            "evidence_actions": [
                _compact_action(item)
                for item in (opportunity.get("evidence_actions", []) or [])[:3]
                if isinstance(item, dict)
            ],
        },
        "auto_readiness": {
            "status": auto_readiness.get("status", ""),
            "summary": auto_readiness.get("summary", ""),
            "blockers": _compact_blockers(auto_readiness.get("blockers")),
            "recent_unusable_buy_signal": auto_readiness.get("recent_unusable_buy_signal", {}) or {},
            "buy_side": {
                "status": (auto_readiness.get("buy_side", {}) or {}).get("status", ""),
                "ready": bool((auto_readiness.get("buy_side", {}) or {}).get("ready", False)),
                "blockers": _compact_blockers((auto_readiness.get("buy_side", {}) or {}).get("blockers")),
            },
            "next_action": _compact_action(auto_readiness.get("next_action")),
        },
        "approval_gate": approval_gate,
        "after_approval_preview": flow.get("after_approval_preview", {}) or {},
        "next_window_plan": {
            "available": bool(next_window_plan.get("available", False)),
            "status": next_window_plan.get("status", ""),
            "summary": next_window_plan.get("summary", ""),
            "next_buy_window": next_window_plan.get("next_buy_window", {}) or {},
            "current_signal": next_window_plan.get("current_signal", {}) or {},
            "next_window_requires_fresh_buy_signal": bool(
                next_window_plan.get("next_window_requires_fresh_buy_signal", False)
            ),
            "scheduled_steps": [
                {
                    "name": item.get("name", ""),
                    "script": item.get("script", ""),
                    "role": item.get("role", ""),
                    "next_run_at": item.get("next_run_at", ""),
                    "last_status": item.get("last_status"),
                    "pending_first_run": bool(item.get("pending_first_run", False)),
                    "critical_for_intraday_simulation": bool(
                        item.get("critical_for_intraday_simulation", False)
                    ),
                }
                for item in (next_window_plan.get("scheduled_steps", []) or [])[:5]
                if isinstance(item, dict)
            ],
            "first_run_verification": next_window_plan.get("first_run_verification", {}) or {},
            "next_action": _compact_action(next_window_plan.get("next_action")),
            "guardrails": next_window_plan.get("guardrails", {}) or {},
        },
        "runtime_contract": automation_schedule.get("runtime_contract", {}) or {},
        "automation_schedule": automation_schedule,
        "latest_auto_trade_summary": automation.get("latest_auto_trade_summary", {}) or {},
        "paper_trial": automation.get("paper_trial", {}) or {},
        "recommended_commands": {
            "diagnose_flow": "atrade diagnose flow --json",
            "opportunity": "atrade opportunity --json",
            "paper_auto_readiness": "atrade paper auto-readiness --json",
            "risk_trial_guard": "atrade risk trial-guard --json",
        },
        "guardrails": flow.get("guardrails", {}) or {
            "read_only": True,
            "runs_pipeline": False,
            "places_paper_order": False,
        },
    }


def _automation_schedule_context(schedule: dict) -> dict:
    """压缩 Hermes 调度诊断，供收盘 LLM 复盘引用。"""
    if not isinstance(schedule, dict) or not schedule:
        return {}
    runtime_profile = schedule.get("runtime_profile", {}) or {}
    runtime_contract = schedule.get("runtime_contract", {}) or {}
    intraday = schedule.get("intraday_simulation", {}) or {}
    intraday_runtime_contract = intraday.get("runtime_contract", {}) or runtime_contract
    return {
        "status": schedule.get("status", ""),
        "summary": schedule.get("summary", ""),
        "runtime_profile": {
            "status": runtime_profile.get("status", ""),
            "effective_profile": runtime_profile.get("effective_profile", ""),
            "recommended_profile": runtime_profile.get("recommended_profile", ""),
            "activation_request_status": runtime_profile.get("activation_request_status", ""),
            "safe_to_auto_apply": bool(runtime_profile.get("safe_to_auto_apply", False)),
            "message": runtime_profile.get("message", ""),
        },
        "runtime_contract": _compact_schedule_runtime_contract(runtime_contract),
        "intraday_simulation": {
            "status": intraday.get("status", ""),
            "summary": intraday.get("summary", ""),
            "profile_ready": bool(intraday.get("profile_ready", False)),
            "ready_for_next_window": bool(intraday.get("ready_for_next_window", False)),
            "scheduled_step_count": int(intraday.get("scheduled_step_count") or 0),
            "critical_job_count": int(intraday.get("critical_job_count") or 0),
            "pending_first_run_critical_count": int(
                intraday.get("pending_first_run_critical_count") or 0
            ),
            "scheduled_steps": [
                {
                    "name": item.get("name", ""),
                    "script": item.get("script", ""),
                    "role": item.get("role", ""),
                    "next_run_at": item.get("next_run_at", ""),
                    "last_status": item.get("last_status"),
                    "pending_first_run": bool(item.get("pending_first_run", False)),
                    "critical_for_intraday_simulation": bool(
                        item.get("critical_for_intraday_simulation", False)
                    ),
                }
                for item in (intraday.get("scheduled_steps", []) or [])[:5]
                if isinstance(item, dict)
            ],
            "first_run_verification": intraday.get("first_run_verification", {}) or {},
            "runtime_contract": _compact_schedule_runtime_contract(intraday_runtime_contract),
            "next_action": _compact_action(intraday.get("next_action")),
            "guardrails": intraday.get("guardrails", {}) or {},
        },
        "next_action": _compact_action(schedule.get("next_action")),
        "guardrails": schedule.get("guardrails", {}) or {},
    }


def _compact_schedule_runtime_contract(contract: dict) -> dict:
    """保留调度脚本运行契约的操作结论，避免收盘上下文丢失 profile 可承接性。"""
    if not isinstance(contract, dict) or not contract:
        return {}
    return {
        "status": contract.get("status", ""),
        "summary": contract.get("summary", ""),
        "scope": contract.get("scope", ""),
        "script_dir_exists": bool(contract.get("script_dir_exists", False)),
        "env_loader": contract.get("env_loader", {}) or {},
        "script_checks": [
            {
                "script": item.get("script", ""),
                "profile_env_file_loading_possible": bool(
                    item.get("profile_env_file_loading_possible", False)
                ),
                "issues": item.get("issues", []) or [],
            }
            for item in (contract.get("script_checks", []) or [])
            if isinstance(item, dict)
        ],
        "blocking_issues": contract.get("blocking_issues", []) or [],
        "guardrails": contract.get("guardrails", {}) or {},
    }


def _tomorrow_checklist(
    *,
    data_source_failures: dict,
    candidate_funnel: dict,
    hot_bridge: dict,
    comparison: dict,
    simulation_flow: dict | None = None,
) -> list[dict]:
    items: list[dict] = []
    simulation_flow = simulation_flow or {}

    actionable_data_issue = bool(
        data_source_failures.get("data_source_blockers")
        or int(data_source_failures.get("actionable_unresolved_count") or 0) > 0
    )
    if actionable_data_issue:
        items.append({
            "priority": "high",
            "label": "复核未补齐数据源",
            "command": "atrade data-sources diagnose --json",
            "reason": "存在 provider 失败未被 fallback 补齐，不能提高执行信心。",
        })

    pool = candidate_funnel.get("pool", {}) or {}
    if not pool.get("total") or not pool.get("core_count"):
        items.append({
            "priority": "high",
            "label": "复核候选漏斗",
            "command": "atrade screener explain --json",
            "reason": candidate_funnel.get("empty_or_core_gap_explanation", "核心池为空，需要拆解主要否决原因。"),
        })

    approval_gate = simulation_flow.get("approval_gate", {}) or {}
    if approval_gate.get("required"):
        items.append({
            "priority": "high",
            "label": approval_gate.get("label", "人工复核运行 profile"),
            "command": approval_gate.get("review_command") or "atrade diagnose flow --json",
            "reason": approval_gate.get("reason") or "模拟承接链路仍有人工审批门，不能让 LLM 只写成没有交易。",
            "requires_user_approval": True,
            "safe_to_auto_apply": False,
            "apply_command_after_approval": approval_gate.get("apply_command") or "",
            "verify_command": approval_gate.get("verify_command") or "atrade diagnose flow --json",
            "writes_environment_after_approval": bool(
                approval_gate.get("modifies_environment_after_approval")
            ),
            "review_command_contract": approval_gate.get("review_command_contract") or {},
            "apply_command_contract": approval_gate.get("apply_command_contract") or {},
            "verify_command_contract": approval_gate.get("verify_command_contract") or {},
        })
    else:
        next_action = (simulation_flow.get("flow_stage", {}) or {}).get("next_action", {}) or {}
        if simulation_flow.get("status") in {"warning", "profile_review_required", "blocked", "waiting_window"} and next_action:
            items.append({
                "priority": "high",
                "label": next_action.get("label", "复核模拟承接链路"),
                "command": next_action.get("command") or "atrade diagnose flow --json",
                "reason": simulation_flow.get("summary", "收盘后需要明确候选流卡点和下一步。"),
            })

    evidence_actions = (simulation_flow.get("opportunity", {}) or {}).get("evidence_actions", []) or []
    if evidence_actions:
        action = evidence_actions[0]
        items.append({
            "priority": "normal",
            "label": action.get("label", "补记录证据动作"),
            "command": action.get("command") or "atrade opportunity --json",
            "reason": action.get("reason") or "存在可安全补记的证据动作，用于让候选流后续可复盘。",
        })

    if data_source_failures.get("unresolved_count") and not actionable_data_issue:
        items.append({
            "priority": "normal",
            "label": "复核非关键数据源事件",
            "command": "atrade data-sources diagnose --json",
            "reason": "存在非关键 provider 事件，但当前不阻断候选或模拟承接；完成 profile 和候选链路复核后再处理。",
        })

    if hot_bridge.get("not_in_pool"):
        items.append({
            "priority": "normal",
            "label": "复核热点召回",
            "command": "atrade market-intel hot-stocks --json",
            "reason": "热门股未进入候选池，只能作为次日召回线索。",
        })

    if not comparison.get("available"):
        items.append({
            "priority": "normal",
            "label": "补齐盘前收盘对比证据",
            "command": comparison.get("recommended_command", f"atrade history signal --date {_today_iso()} --json"),
            "reason": "盘前或收盘快照不足时，不能评价早盘判断质量。",
        })

    items.append({
        "priority": "discipline",
        "label": "保持执行边界",
        "command": "atrade suggest --json",
        "reason": "继续禁止自动执行；观察不等于买入意向，买入意向也必须人工确认。",
    })
    return items[:6]


def _close_review_context(
    conn: Any,
    store: EventStore,
    *,
    market_intel: dict,
    diagnostics: dict,
    trade_plan: dict,
) -> dict:
    candidate_funnel = _candidate_funnel_context(conn, store, since=_today_start_iso())
    data_source_failures = _provider_failure_context(trade_plan, diagnostics)
    hot_bridge = _hot_stock_pool_bridge(market_intel, candidate_funnel["pool"]["rows"])
    comparison = _comparison_readiness(market_intel)
    simulation_flow = _simulation_flow_context(conn)
    checklist = _tomorrow_checklist(
        data_source_failures=data_source_failures,
        candidate_funnel=candidate_funnel,
        hot_bridge=hot_bridge,
        comparison=comparison,
        simulation_flow=simulation_flow,
    )
    return {
        "date": _today_iso(),
        "data_source_failures": data_source_failures,
        "candidate_funnel": candidate_funnel,
        "simulation_flow": simulation_flow,
        "hot_stock_pool_bridge": hot_bridge,
        "comparison_readiness": comparison,
        "tomorrow_checklist": checklist,
        "summary_requirements": [
            "收盘复盘必须明确未补齐的数据源，不要只写“数据降级”。",
            "候选池为空时必须引用候选漏斗和主要否决原因，区分暂无合格候选与行情缺失。",
            "必须说明模拟承接链路当前卡点：profile 审批、买入窗口、新鲜买入意向、影子试运行，不能只写“没有交易”。",
            "热门股若未入池，只能写为召回线索或明日复核，不得升级为买入意向。",
            "盘前 vs 收盘数据不足时，写清缺失输入，不要强行做有效性判断。",
        ],
    }


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


def _evidence_registry(sections: dict) -> list[dict]:
    """从上下文中抽取 LLM 必须引用的证据编号。"""
    registry: list[dict] = []
    seen: set[str] = set()

    def evidence_label(value: dict) -> str:
        event_type = str(value.get("event_type") or "")
        payload = value.get("payload") if isinstance(value.get("payload"), dict) else {}
        if event_type == "strategy.profile_activation.requested":
            return str(payload.get("target_profile") or value.get("target_profile") or value.get("label") or "")
        return str(value.get("stream") or value.get("code") or value.get("label") or "")

    def add(evidence_id: object, *, source_section: str, kind: str = "", label: str = "") -> None:
        eid = str(evidence_id or "").strip()
        if not eid or eid in seen:
            return
        seen.add(eid)
        registry.append({
            "evidence_id": eid,
            "source_section": source_section,
            "kind": kind,
            "label": label,
        })

    def walk(value: Any, source_section: str) -> None:
        if isinstance(value, dict):
            if value.get("event_id"):
                add(
                    value.get("event_id"),
                    source_section=source_section,
                    kind=str(value.get("event_type") or "event"),
                    label=evidence_label(value),
                )
            if value.get("requested_event_id"):
                add(
                    value.get("requested_event_id"),
                    source_section=source_section,
                    kind="manual_trade.requested",
                    label=str(value.get("code") or value.get("stream") or ""),
                )
            if value.get("resolution_event_id"):
                add(
                    value.get("resolution_event_id"),
                    source_section=source_section,
                    kind="manual_trade.resolution",
                    label=str(value.get("code") or value.get("stream") or ""),
                )
            if value.get("observation_id"):
                add(
                    value.get("observation_id"),
                    source_section=source_section,
                    kind="market_observation",
                    label=str(value.get("kind") or value.get("symbol") or ""),
                )
            for item in value.values():
                walk(item, source_section)
        elif isinstance(value, list):
            for item in value:
                walk(item, source_section)

    for name, section in sections.items():
        walk(section.get("data", section), name)
    return registry[:120]


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

    diagnostics_section = _safe_section("diagnostics", lambda: diagnose_health(conn))
    trade_plan_section = _safe_section("trade_plan", lambda: propose_agent_trade_plan(conn))
    market_intel_section = _safe_section("market_intel", lambda: _market_intel_context(conn, mode=mode))
    sections = {
        "diagnostics": diagnostics_section,
        "trade_plan": trade_plan_section,
        "market_intel": market_intel_section,
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
    if mode == "close":
        sections["close_review"] = _safe_section(
            "close_review",
            lambda: _close_review_context(
                conn,
                store,
                market_intel=market_intel_section.get("data", {}) or {},
                diagnostics=diagnostics_section.get("data", {}) or {},
                trade_plan=trade_plan_section.get("data", {}) or {},
            ),
        )

    failed_sections = [name for name, value in sections.items() if value.get("status") != "ok"]
    evidence_registry = _evidence_registry(sections)
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
        "discord_card_contract": _discord_card_contract(mode),
        "term_glossary": _term_glossary(),
        "failed_sections": failed_sections,
        "guardrails": _summary_guardrails(mode),
        "evidence_contract": {
            "required": True,
            "field_name": "evidence_id",
            "rule": "最终摘要中每个判断段落必须引用同一数据段或同一标的的 evidence_id；没有对应编号的内容只能写“暂无可用数据”。",
        },
        "evidence_registry": evidence_registry,
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
        "- 每个判断段落必须引用 evidence_id；没有证据编号的内容只能写“暂无可用数据”。",
        "- 如果必须保留协议名，第一次出现写成“中文释义（内部字段：protocol_name）”，不要单独裸露英文术语。",
        "- 下方代码块是取数依据，不是最终报告模板；请先转义后再总结。",
    ]
    for guardrail in payload.get("guardrails", []):
        lines.append(f"- {_translate_text(guardrail)}")

    card_contract = payload.get("discord_card_contract") or {}
    if card_contract:
        lines.extend([
            "",
            "## Discord 卡片输出模板",
            "",
            "- 最终回复必须按下面模板生成一张 Discord Markdown 卡片。",
            "- 保留标题和章节顺序；没有数据时写“暂无可用数据”。",
            "- 系统与数据质量必须作为第 1 区块，后续结论不能越过这个可信度闸门。",
            "- 不要输出原始 JSON、代码块、内部字段名、枚举值或 JSON 路径。",
            "",
            card_contract["template"],
            "",
            "### 风控短句候选",
            "",
        ])
        for line in card_contract.get("discipline_lines", []):
            lines.append(f"- {line}")
        lines.extend(["", "只选择 1 句放入最终卡片末尾，不要把候选列表全部输出。"])

    lines.extend(["", "## 内部术语表", "", "以下只用于理解上下文，最终摘要不要复述术语表或裸露内部字段名。"])
    for item in payload.get("term_glossary", []):
        lines.append(f"- `{item['term']}`：{item['display']}")

    lines.extend(["", "## 证据编号清单", ""])
    registry = payload.get("evidence_registry", []) or []
    if registry:
        lines.append("最终摘要引用事实时使用 `evidence_id: ...`，可用编号如下：")
        for item in registry:
            lines.append(
                "- "
                f"evidence_id: {item.get('evidence_id')} | "
                f"来源: {SECTION_CN.get(item.get('source_section'), item.get('source_section'))} | "
                f"类型: {_term_label(item.get('kind'))} | "
                f"线索: {item.get('label') or '无'}"
            )
    else:
        lines.append("暂无事件证据编号；最终摘要只能写“暂无可用数据”，不能补写判断。")

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
