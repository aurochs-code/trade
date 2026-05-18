"""只读 LLM 摘要上下文。

该模块把 Hermes/agent 需要的摘要材料收敛到稳定 CLI 表面：
`atrade llm-context --mode ...`。外部调度器不需要进入源码目录，也不需要读取
checkout 内脚本。
"""

from __future__ import annotations

import datetime as dt
import json
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
    "market_intel": "热点与新闻",
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

### 4. 盘前 vs 收盘
- 延续热点：盘前出现且收盘仍强的板块 / 个股
- 新增热点：盘前没有、收盘出现的板块 / 个股
- 降温热点：盘前热、收盘弱化的板块 / 个股
- 判断质量：盘前判断有效 / 偏弱 / 数据不足无法判断
- 对比只用于复盘早盘判断质量，不作为自动交易依据

### 5. 候选池变化
- 核心池：早盘 N → 收盘 N
- 观察池：早盘 N → 收盘 N
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
- 明日禁止事项：继续禁止自动执行，观察不等于买入
- 风控短句：从下方“风控短句候选”选择 1 句""",
}


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


def _discord_card_contract(mode: str) -> dict:
    template = DISCORD_CARD_TEMPLATES.get(mode, "")
    if not template:
        return {}
    return {
        "format": "Discord Markdown card",
        "template": template,
        "rules": [
            "最终回复必须按模板结构输出，保留标题和章节顺序。",
            "不要输出原始 JSON、内部字段名、枚举值或 JSON 路径。",
            "缺少数据时写“暂无可用数据”，不要臆测外部事实。",
            "系统与数据质量必须作为第 1 区块，并决定后续结论可信度。",
            "热点只作为市场背景和复盘线索，不作为买入依据。",
            "末尾只选择 1 句风控短句，不要堆砌多句。",
        ],
        "discipline_lines": DISCIPLINE_LINES,
    }


def _summary_guardrails(mode: str) -> list[str]:
    guardrails = [
        "只基于本上下文总结，不要臆测外部事实。",
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
        f"""SELECT m.source, m.kind, m.symbol, m.observed_at, m.run_id, m.payload_json,
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


def _compact_sector(item: dict) -> dict:
    return {
        "name": item.get("name") or item.get("板块") or "",
        "change_pct": item.get("change_pct", item.get("涨跌幅", 0)) or 0,
        "amount": item.get("amount", item.get("成交额", item.get("main_net", 0))) or 0,
        "lead_stock": item.get("lead_stock") or item.get("leader") or "",
        "up_count": item.get("up_count", 0) or 0,
        "down_count": item.get("down_count", 0) or 0,
        "source": item.get("source", ""),
    }


def _compact_stock(item: dict, source_label: str) -> dict:
    return {
        "code": item.get("code") or item.get("symbol") or "",
        "name": item.get("name") or item.get("symbol") or item.get("code") or "",
        "rank": item.get("rank", ""),
        "change_pct": item.get("change_pct", 0) or 0,
        "heat": item.get("heat", 0) or 0,
        "source_count": item.get("source_count", ""),
        "sources": item.get("sources", []),
        "reason": item.get("reason", ""),
        "source": source_label,
    }


def _compact_news(item: dict, source_label: str) -> dict:
    return {
        "title": item.get("title") or item.get("content") or item.get("text") or "",
        "time": item.get("time", ""),
        "source": item.get("source") or source_label,
        "summary": item.get("summary") or item.get("content") or "",
        "code": item.get("code", ""),
        "name": item.get("name", ""),
        "category": item.get("category", ""),
        "kind": source_label,
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


def _market_snapshot(conn: Any, *, run_type: str | None, fallback_to_latest: bool = False) -> dict:
    sector_kinds = ("sector_heatmap", "hot_sectors_industry_change", "hot_sectors_concept_change")
    stock_kinds = ("cross_platform_hot_stocks", "xueqiu_hot_stocks", "hot_stocks")
    news_kinds = ("finance_flash", "market_announcements", "global_risk_news")
    observations: dict[str, dict] = {}
    fallback_used = False

    def latest(kind: str) -> dict | None:
        obs = _latest_market_observation(conn, kind, run_type=run_type, fallback_to_latest=fallback_to_latest)
        if obs:
            nonlocal fallback_used
            fallback_used = fallback_used or bool(obs.get("fallback_used"))
            observations[kind] = {
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
        for item in _payload_items(obs["payload"]):
            compacted = _compact_sector(item)
            compacted["source"] = compacted.get("source") or source_label
            sectors.append(compacted)
    sectors = _dedupe(sectors, lambda item: item.get("name"), limit=8)

    stocks: list[dict] = []
    for kind in stock_kinds:
        obs = latest(kind)
        if not obs:
            continue
        source_label = TERM_CN.get(kind, kind)
        stocks.extend(_compact_stock(item, source_label) for item in _payload_items(obs["payload"]))
    stocks = _dedupe(stocks, lambda item: item.get("code") or item.get("name"), limit=10)

    news: list[dict] = []
    for kind in news_kinds:
        obs = latest(kind)
        if not obs:
            continue
        source_label = TERM_CN.get(kind, kind)
        news.extend(_compact_news(item, source_label) for item in _payload_items(obs["payload"]))
    news = _dedupe(news, lambda item: item.get("title"), limit=10)

    return {
        "phase": TERM_CN.get(run_type or "latest", run_type or "latest"),
        "available": bool(sectors or stocks or news),
        "fallback_used": fallback_used,
        "fallback_note": "未找到指定流水的热点缓存，已退回最新可用热点缓存。" if fallback_used else "",
        "sectors": sectors,
        "hot_stocks": stocks,
        "news": news,
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
        "market_intel": _safe_section("market_intel", lambda: _market_intel_context(conn, mode=mode)),
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
        "discord_card_contract": _discord_card_contract(mode),
        "term_glossary": _term_glossary(),
        "failed_sections": failed_sections,
        "guardrails": _summary_guardrails(mode),
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
