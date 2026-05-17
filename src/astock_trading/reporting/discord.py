"""
reporting/discord.py — Discord 消息格式化

只负责格式化，不负责发送。实际发送由 Agent Gateway 或 V1 discord_push 处理。
reporting 不反写任何业务表。
"""

from __future__ import annotations

import re

from astock_trading.platform.time import local_now_iso, local_now_str, local_today_str
from astock_trading.reporting.market_formatters import (
    _format_stock_label,
    _source_list_label,
    format_announcement_intel_line,
    format_market_intel_line,
    top_sector_movers,
)


# Discord 品牌色
COLORS = {
    "morning": 0x1E88E5,
    "noon": 0xFB8C00,
    "evening": 0x7B1FA2,
    "scoring": 0x00838F,
    "weekly": 0x00695C,
    "stop_alert": 0xC62828,
    "profit_alert": 0x2E7D32,
    "sentiment": 0xFF6F00,
    "info": 0x37474F,
    "manual_confirmation": 0x5D4037,
}

SIGNAL_EMOJI = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}
SIGNAL_CN = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}
ACTION_CN = {
    "BUY": "买入意向",
    "SELL": "卖出意向",
    "WATCH": "观察",
    "CLEAR": "清仓",
    "BUY_ALLOWED": "可买入",
    "REDUCED_BUY": "减量买入",
    "NO_TRADE": "不操作",
}
STATUS_CN = {
    "ok": "正常",
    "warning": "警告",
    "failed": "失败",
    "error": "错误",
    "running": "运行中",
    "unknown": "未知",
    "degraded": "降级",
    "high": "高",
    "medium": "中",
    "low": "低",
}
DATA_QUALITY_CN = {
    "ok": "正常",
    "degraded": "降级",
    "error": "错误",
    "-": "-",
}

SIGNAL_TYPE_CN = {
    "stop_loss": "止损",
    "trailing_stop": "移动止盈",
    "time_stop": "时间止损",
    "ma_exit": "MA 跌破离场",
    "daily_loss": "单日浮亏",
    "style_switch": "风格切换",
}
URGENCY_CN = {
    "immediate": "立即处理",
    "end_of_day": "收盘前处理",
    "advisory": "提醒",
}
INTERNAL_LABEL_CN = {
    "above_ma20": "站上 MA20",
    "below_ma20": "跌破 MA20",
    "limit_up_today": "当日涨停",
    "consecutive_outflow": "连续资金流出",
    "consecutive_outflow_warn": "连续资金流出预警",
    "ma20_trend_down": "MA20 趋势下行",
    "turnover_spike": "换手异常放大",
    "requires_entry_strategy_route": "缺少有效策略路线",
    "entry_signal": "入场信号",
    "hard_veto": "硬否决",
    "warning_signals": "预警信号",
    "data_quality": "数据质量",
    "data_missing_fields": "缺失数据字段",
    "required_missing": "核心源缺失",
    "optional_missing": "辅助源缺失",
    "core_pool": "核心池",
    "candidate_pool_freshness": "候选池新鲜度",
    "industry_comparison": "行业对比",
    "financial": "财务数据",
    "announcements": "公告",
    "research_reports": "研报",
    "news": "新闻",
    "hot_stocks": "热股",
    "northbound_realtime": "北向实时资金",
    "baidu_fund_flow": "百度资金流",
    "review_core_pool": "复核核心池",
    "candidate core pool is empty": "核心候选池为空",
    "auto_trade buy-side requires fresh core candidates": "模拟买入侧需要新鲜核心候选",
}
MAX_EMBED_FIELDS = 25


def _field(name: str, value: str, inline: bool = True) -> dict:
    return {"name": name[:256], "value": str(value)[:1024], "inline": inline}


def _embed(
    title: str, color: int, fields: list[dict],
    description: str = "", footer: str = "",
) -> dict:
    e: dict = {"title": title[:256], "color": color}
    if description:
        e["description"] = description[:4096]
    if fields:
        e["fields"] = fields
    if footer:
        e["footer"] = {"text": footer}
    e["timestamp"] = local_now_iso()
    return e


def _markdown_heading_text(line: str) -> str:
    text = re.sub(r"^#+\s*", "", line.strip()).strip()
    return re.sub(r"^\d+[.、]\s*", "", text).strip()


def _clean_llm_summary_line(line: str) -> str:
    text = line.rstrip()
    if text.strip().startswith("```"):
        return ""
    return text


def _strip_markdown_emphasis(text: str) -> str:
    return text.replace("**", "").replace("__", "").strip()


def _llm_summary_value_after_colon(line: str, label: str) -> str:
    text = _strip_markdown_emphasis(line)
    pattern = rf"^{re.escape(label)}[:：]\s*(.+)$"
    match = re.match(pattern, text)
    return match.group(1).strip() if match else ""


def _split_llm_summary_markdown(markdown: str) -> tuple[str, str, list[tuple[str, str]]]:
    title = ""
    description_lines: list[str] = []
    sections: list[tuple[str, list[str]]] = []
    current_name = ""
    current_lines: list[str] = []
    seen_section = False

    for raw_line in markdown.splitlines():
        line = _clean_llm_summary_line(raw_line)
        if not line and not current_name:
            continue
        stripped = line.strip()
        if stripped.startswith("# ") or stripped.startswith("## "):
            if not title:
                title = _markdown_heading_text(stripped)
            continue
        if stripped.startswith("### "):
            if current_name:
                sections.append((current_name, current_lines))
            current_name = _markdown_heading_text(stripped)
            current_lines = []
            seen_section = True
            continue
        if seen_section:
            current_lines.append(line)
        elif stripped:
            description_lines.append(line)

    if current_name:
        sections.append((current_name, current_lines))

    compact_sections = []
    for name, lines in sections:
        value = "\n".join(line for line in lines if line.strip()).strip()
        if value:
            compact_sections.append((name, value))
    return title, "\n".join(description_lines).strip(), compact_sections


def _llm_summary_chips(description: str) -> list[dict]:
    chips = []
    for line in description.splitlines():
        for label in ("今日结论", "今日闭环", "自动执行", "数据来源", "今日交易动作"):
            value = _llm_summary_value_after_colon(line, label)
            if value:
                chips.append(_field(label, value, inline=True))
                break
    return chips[:3]


def _llm_section_name(name: str) -> str:
    icons = {
        "系统与数据质量": "🛡️",
        "今日动作": "🎯",
        "市场热点": "🔥",
        "收盘市场热点": "🔥",
        "候选池": "📌",
        "持仓与风险": "💼",
        "今日纪律": "📏",
        "今日闭环": "✅",
        "盘前 vs 收盘": "🔁",
        "候选池变化": "📌",
        "明日清单": "📝",
    }
    return f"{icons.get(name, '•')} {name}"


def _polish_llm_field_value(value: str) -> str:
    lines = []
    for raw_line in value.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("- "):
            line = "• " + line[2:].strip()
        lines.append(line)
    return "\n".join(lines)


def _append_llm_summary_fields(fields: list[dict], name: str, value: str) -> None:
    polished = _polish_llm_field_value(value)
    if len(polished) <= 960:
        fields.append(_field(_llm_section_name(name), polished, inline=False))
        return

    chunk = ""
    part = 1
    for line in polished.splitlines():
        candidate = f"{chunk}\n{line}".strip() if chunk else line
        if len(candidate) > 960 and chunk:
            suffix = "" if part == 1 else f"（续 {part}）"
            fields.append(_field(f"{_llm_section_name(name)}{suffix}", chunk, inline=False))
            part += 1
            chunk = line
        else:
            chunk = candidate
    if chunk:
        suffix = "" if part == 1 else f"（续 {part}）"
        fields.append(_field(f"{_llm_section_name(name)}{suffix}", chunk, inline=False))


def _llm_summary_color(mode: str, text: str) -> int:
    if any(token in text for token in ("失败", "错误", "严重异常")):
        return COLORS["stop_alert"]
    if any(token in text for token in ("警告", "降级", "待人工复核")):
        return COLORS["noon"]
    if mode == "morning":
        return COLORS["morning"]
    if mode == "close":
        return COLORS["evening"]
    return COLORS["weekly"]


def format_llm_summary_embed(mode: str, markdown: str) -> dict:
    """把 Hermes LLM Markdown 摘要转换成 Discord Rich Embed。"""
    title, description, sections = _split_llm_summary_markdown(markdown)
    mode_label = {"morning": "盘前摘要", "close": "收盘复盘", "weekly": "周复盘"}.get(mode, "LLM 摘要")
    if not title:
        title = f"A股 LLM {mode_label}"

    fields = _llm_summary_chips(description)
    for name, value in sections:
        if len(fields) >= MAX_EMBED_FIELDS:
            break
        _append_llm_summary_fields(fields, name, value)

    embed = _embed(
        title=title,
        description="**只读摘要** · 人工确认前参考 · 不构成自动交易指令",
        color=_llm_summary_color(mode, markdown),
        fields=fields,
        footer=f"A-Stock Trading · LLM {mode_label} · Rich Embed · 非交易指令",
    )
    embed["author"] = {"name": f"A-Stock Trading · LLM {mode_label}"}
    return embed


def _score_emoji(score: float) -> str:
    if score >= 7:
        return "✅"
    if score >= 5:
        return "🟡"
    return "❌"


def _pnl_emoji(v: float) -> str:
    return "🟢" if v >= 0 else "🔴"


def _label_cn(value: object) -> str:
    text = str(value or "")
    return (
        ACTION_CN.get(text)
        or SIGNAL_CN.get(text)
        or STATUS_CN.get(text)
        or DATA_QUALITY_CN.get(text)
        or SIGNAL_TYPE_CN.get(text)
        or INTERNAL_LABEL_CN.get(text)
        or text
    )


def _labels_cn(values: list | tuple | set) -> str:
    return ",".join(_label_cn(item) for item in values) or "-"


def _finding_cn(value: object) -> str:
    text = str(value)
    replacements = {
        "hard veto triggered:": "触发硬否决：",
        "warning signals:": "预警信号：",
        "entry signal not triggered": "入场信号未触发",
        "missing data fields:": "缺失数据字段：",
        "market gate ok": "大盘门控通过",
        "candidate core pool is empty": "核心候选池为空",
        "auto_trade buy-side requires fresh core candidates": "模拟买入侧需要新鲜核心候选",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    for source, target in INTERNAL_LABEL_CN.items():
        text = text.replace(source, target)
    return text


def _append_market_intel_fields(fields: list[dict], data: dict) -> None:
    cross_hot = data.get("cross_platform_hot_stocks", []) or []
    if cross_hot:
        lines = []
        for item in cross_hot[:5]:
            pct = item.get("change_pct", 0) or 0
            source_count = item.get("source_count", len(item.get("sources", [])) or 1)
            sources = _source_list_label(item.get("sources", []))
            source_text = f" · {source_count}源 {sources}" if sources else f" · {source_count}源"
            lines.append(f"{_format_stock_label(item)} `{pct:+.2f}%`{source_text}")
        fields.append(_field("跨平台热度", "\n".join(lines), inline=False))

    finance_flash = data.get("finance_flash", []) or []
    if finance_flash:
        lines = []
        for item in finance_flash[:5]:
            lines.append(format_market_intel_line(item, "finance_flash"))
        fields.append(_field("财经快讯", "\n".join(lines), inline=False))

    global_risk = data.get("global_risk_news", []) or []
    if global_risk:
        lines = []
        for item in global_risk[:5]:
            lines.append(format_market_intel_line(item, "global_risk"))
        fields.append(_field("海外风险", "\n".join(lines), inline=False))

    announcements = data.get("market_announcements", []) or []
    if announcements:
        lines = []
        for item in announcements[:5]:
            lines.append(format_announcement_intel_line(item))
        fields.append(_field("公告提示", "\n".join(lines), inline=False))


# ---------------------------------------------------------------------------
# 格式化函数
# ---------------------------------------------------------------------------

def format_morning_embed(data: dict) -> dict:
    """盘前摘要 → Discord embed dict。"""
    date_str = data.get("date", local_today_str())
    signal = data.get("market_signal", "")
    sig_tag = f"{SIGNAL_EMOJI.get(signal, '')} {SIGNAL_CN.get(signal, signal)}"

    fields = []

    # 大盘指数
    for name, info in data.get("market", {}).items():
        price = info.get("price", 0) or 0
        chg = info.get("chg_pct", info.get("change_pct", 0)) or 0
        fields.append(_field(name, f"`{price:.2f}` ({chg:+.2f}%)", inline=True))

    # 持仓
    positions = data.get("positions", [])
    if positions:
        fields.append(_field("\u200b", "**💼 持仓**", inline=False))
        for pos in positions:
            currency = pos.get("currency", "CNY")
            sym = "HK$" if currency == "HKD" else "¥"
            fields.append(_field(
                pos.get("name", ""),
                f"{pos.get('shares', 0)} 股 @ `{sym}{pos.get('price', 0):.2f}`",
            ))
    else:
        fields.append(_field("💼 持仓", "空仓", inline=False))

    # 今日决策
    decision = data.get("decision", {})
    if decision:
        action = decision.get("action", "")
        action_map = {
            "BUY_ALLOWED": "✅ 可买入",
            "REDUCED_BUY": "🟡 减量买入",
            "NO_TRADE": "🚫 不操作",
        }
        action_label = action_map.get(action, action)
        mult = decision.get("multiplier", 0)
        alerts = decision.get("risk_alerts", [])
        decision_lines = [f"仓位系数 `{mult:.2f}`"]
        if alerts:
            decision_lines.extend([f"⚠️ {a}" for a in alerts])
        fields.append(_field("📋 今日决策", f"**{action_label}**\n" + "\n".join(decision_lines), inline=False))

    # 止损挂单提醒：从风控信号中提取止损触发价，明确提示用户挂单
    stop_loss_reminders = data.get("stop_loss_reminders", [])
    if stop_loss_reminders:
        reminder_lines = []
        for r in stop_loss_reminders:
            reminder_lines.append(
                f"• {r['name']}({r['code']}): 立即挂 **卖出** 止损单 `@ {r['trigger_price']:.2f}`"
            )
        fields.append(_field("🔴 止损挂单提醒", "\n".join(reminder_lines), inline=False))

    # 核心池
    core = data.get("core_pool", [])
    if core:
        fields.append(_field("\u200b", "**🎯 核心池**", inline=False))
        for s in core:
            score = s.get("score", 0)
            score_label = s.get("score_label") or "上次评分"
            scored_at = f"\n日期 `{s['last_scored_at']}`" if s.get("last_scored_at") else ""
            fields.append(
                _field(s.get("name", ""), f"{_score_emoji(score)} {score_label} **{score:.1f}**{scored_at}")
            )

    xueqiu_hot = data.get("xueqiu_hot_stocks", [])
    if xueqiu_hot:
        hot_lines = []
        for item in xueqiu_hot[:5]:
            rank = item.get("rank") or ""
            rank_text = f"#{rank} " if rank else ""
            code = item.get("code") or item.get("symbol", "")
            name = item.get("name") or code
            pct = item.get("change_pct", 0) or 0
            heat = item.get("heat", 0) or 0
            label = f"{name}({code})" if code and code != name else name
            heat_text = f" · 热度 {heat}" if heat else ""
            hot_lines.append(f"{rank_text}{label} `{pct:+.2f}%`{heat_text}")
        fields.append(_field("雪球热搜", "\n".join(hot_lines), inline=False))

    _append_market_intel_fields(fields, data)

    return _embed(
        title=f"📊 盘前摘要 — {date_str}",
        description=f"综合信号 **{sig_tag}**",
        color=COLORS["morning"],
        fields=fields,
        footer="A-Stock Trading · morning_brief",
    )


def format_evening_embed(data: dict) -> dict:
    """收盘报告 → Discord embed dict。"""
    date_str = data.get("date", local_today_str())

    fields = []

    for name, info in data.get("market", {}).items():
        price = info.get("price", 0) or 0
        chg = info.get("change_pct", info.get("chg_pct", 0)) or 0
        fields.append(_field(name, f"`{price:.2f}` ({chg:+.2f}%)"))

    # 全市场升降家数
    stats = data.get("market_stats", {})
    if stats and stats.get("total", 0) > 0:
        up = stats.get("up", 0)
        down = stats.get("down", 0)
        flat = stats.get("flat", 0)
        fields.append(_field(
            "全市场",
            f"🔺 {up} | 🔻 {down} | ⚪ {flat}",
            inline=False,
        ))

    positions = data.get("positions", [])
    if positions:
        fields.append(_field("\u200b", "**💼 持仓**", inline=False))
        for pos in positions:
            pnl = pos.get("pnl_pct", 0)
            fields.append(_field(
                f"{_pnl_emoji(pnl)} {pos.get('name', '')}",
                f"{pos.get('shares', 0)} 股 · 盈亏 **{pnl:+.2f}%**",
            ))

    alerts = data.get("alerts", [])
    if alerts:
        fields.append(_field(
            f"⚠️ 触发事项（{len(alerts)}）",
            "\n".join(f"• {a}" for a in alerts),
            inline=False,
        ))

    _append_market_intel_fields(fields, data)

    return _embed(
        title=f"📈 收盘报告 — {date_str}",
        color=COLORS["evening"],
        fields=fields,
        footer="A-Stock Trading · close_review",
    )


def format_scoring_embed(scores: list[dict], date_str: str = "") -> dict:
    """评分报告 → Discord embed dict。"""
    if not date_str:
        date_str = local_today_str()

    fields = []
    for s in scores[:15]:  # Discord 最多 25 fields
        score = float(s.get("total_score", s.get("total", 0)) or 0)
        name = s.get("name", s.get("code", ""))
        veto = s.get("veto_triggered", False)
        emoji = "❌" if veto else _score_emoji(score)
        # 从 dimensions 列表取分项
        if "dimensions" in s:
            dim_map = {d["name"]: d for d in s["dimensions"]}
            tech = dim_map.get("technical", {}).get("score", 0)
            fund = dim_map.get("fundamental", {}).get("score", 0)
            flow = dim_map.get("flow", {}).get("score", 0)
            sent = dim_map.get("sentiment", {}).get("score", 0)
            detail = f"技{tech:.1f} 基{fund:.1f} 资{flow:.1f} 舆{sent:.1f}"
        else:
            detail = f"技{int(s.get('technical_score', 0))} 基{int(s.get('fundamental_score', 0))} " \
                     f"资{int(s.get('flow_score', 0))} 舆{int(s.get('sentiment_score', 0))}"
        lines = [f"**{score:.1f}** · {detail}"]
        if route_summary := _score_route_summary(s):
            lines.append(route_summary)
        if blocker_summary := _score_blocker_summary(s):
            lines.append(blocker_summary)
        fields.append(_field(f"{emoji} {name}", "\n".join(lines)))

    return _embed(
        title=f"🎯 核心池评分 — {date_str}",
        description=f"共 {len(scores)} 只",
        color=COLORS["scoring"],
        fields=fields,
        footer="A-Stock Trading · scoring",
    )


def _score_route_summary(score: dict) -> str:
    routes = score.get("strategy_routes", []) or []
    if not routes:
        return ""
    parts = []
    fallback_entry = bool(score.get("entry_signal"))
    for route in routes[:2]:
        name = route.get("display_name") or route.get("route", "")
        if not name:
            continue
        confidence = _to_float(route.get("confidence"))
        entry_signal = route.get("entry_signal", fallback_entry)
        state = "入场" if entry_signal else "观察"
        parts.append(f"{name} {confidence:.0%} {state}")
    return "路线 " + " / ".join(parts) if parts else ""


def _score_blocker_summary(score: dict) -> str:
    blockers = set(score.get("promotion_blockers", []) or [])
    note = str(score.get("note", ""))
    if "requires_entry_strategy_route" in note:
        blockers.add("requires_entry_strategy_route")

    parts = [_label_cn(item) for item in blockers]
    return "阻断 " + " / ".join(parts) if parts else ""


def format_manual_confirmation_embed(analysis: dict) -> dict:
    """人工确认摘要 → Discord embed dict。"""
    resolved = analysis.get("resolved", {}) or {}
    code = resolved.get("code") or analysis.get("code", "")
    name = resolved.get("name") or analysis.get("name") or code
    label = f"{name}({code})" if code and code != name else str(name or code)
    quote = analysis.get("quote", {}) or {}
    technical = analysis.get("technical", {}) or {}
    score = analysis.get("score", {}) or {}
    decision = analysis.get("decision", {}) or {}

    action = str(decision.get("action", "WATCH") or "WATCH")
    action_cn = _label_cn(action)
    confidence = _to_float(decision.get("confidence", decision.get("score")))
    position_pct = _to_float(decision.get("position_pct"))
    market_signal = decision.get("market_signal") or (analysis.get("market", {}) or {}).get("signal", "-")
    market_signal_cn = _label_cn(market_signal)
    total_score = _to_float(score.get("total_score", score.get("total")))
    data_quality = _label_cn(score.get("data_quality", "-"))

    fields = [
        _field(
            "核心结论",
            "\n".join([
                f"动作 **{action_cn}** · 市场 **{_signal_tag(market_signal, market_signal_cn)}**",
                f"置信度 `{confidence:.1f}` · 不自动下单",
                "需人工确认后才允许记录成交",
            ]),
            inline=False,
        ),
        _field(
            "评分",
            f"总分 **{total_score:.1f}** · 数据质量：{data_quality}\n{_dimension_summary(score)}",
            inline=False,
        ),
        _field(
            "趋势/路线",
            "\n".join(_trend_route_lines(score, technical, quote)),
            inline=False,
        ),
        _field(
            "买卖点",
            "\n".join([
                f"现价 {_price_text(quote.get('price'))}",
                f"建议仓位 {_pct_text(position_pct)}",
                "买点/卖点以人工盘口确认和风控规则为准",
            ]),
            inline=False,
        ),
        _field(
            "风险警报",
            "\n".join(_risk_lines(analysis, score, decision)) or "暂无显性风险；仍需人工复核",
            inline=False,
        ),
        _field(
            "催化因素",
            "\n".join(_catalyst_lines(analysis)) or "暂无明确催化",
            inline=False,
        ),
        _field(
            "操作检查清单",
            "\n".join([
                "确认价格/流动性/仓位/止损",
                "确认公告、舆情和盘面没有新增负面",
                "确认后再用 record-buy / record-sell 记录人工成交",
            ]),
            inline=False,
        ),
    ]

    return _embed(
        title=f"人工确认 — {local_today_str()}",
        description=f"{label} · {action_cn} · 不自动下单",
        color=COLORS["manual_confirmation"],
        fields=fields,
        footer="A-Stock Trading · manual_confirmation",
    )


def _signal_tag(raw: object, label: str) -> str:
    emoji = SIGNAL_EMOJI.get(str(raw), "")
    return f"{emoji} {label}".strip()


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _price_text(value: object) -> str:
    return f"{_to_float(value):.2f}"


def _pct_text(value: object) -> str:
    pct = _to_float(value)
    if abs(pct) <= 1:
        pct *= 100
    return f"{pct:.0f}%"


def _dimension_summary(score: dict) -> str:
    labels = {
        "technical": "技",
        "fundamental": "基",
        "flow": "资",
        "sentiment": "舆",
    }
    if "dimensions" in score:
        parts = []
        for item in score.get("dimensions", []) or []:
            name = item.get("name", "")
            if name in labels:
                parts.append(f"{labels[name]}{_to_float(item.get('score')):.1f}")
        if parts:
            return " ".join(parts)
    return (
        f"技{_to_float(score.get('technical_score')):.1f} "
        f"基{_to_float(score.get('fundamental_score')):.1f} "
        f"资{_to_float(score.get('flow_score')):.1f} "
        f"舆{_to_float(score.get('sentiment_score')):.1f}"
    )


def _trend_route_lines(score: dict, technical: dict, quote: dict) -> list[str]:
    routes = score.get("strategy_routes", []) or []
    route_lines = []
    for route in routes[:3]:
        name = route.get("display_name") or route.get("route", "")
        confidence = _to_float(route.get("confidence"))
        entry = "入场" if route.get("entry_signal") else "观察"
        route_lines.append(f"{name} `{confidence:.0%}` · {entry}")
    if not route_lines:
        route_lines.append("无有效策略路线")

    trend = [
        f"现价 {_price_text(quote.get('price'))} / 涨跌 {_to_float(quote.get('change_pct')):+.2f}%",
        "MA5/20/60 {ma5:.2f}/{ma20:.2f}/{ma60:.2f}".format(
            ma5=_to_float(technical.get("ma5")),
            ma20=_to_float(technical.get("ma20")),
            ma60=_to_float(technical.get("ma60")),
        ),
        "RSI {rsi:.1f} · 量比 {volume_ratio:.1f} · 5日动量 {momentum:+.1f}%".format(
            rsi=_to_float(technical.get("rsi")),
            volume_ratio=_to_float(technical.get("volume_ratio")),
            momentum=_to_float(technical.get("momentum_5d")),
        ),
    ]
    flags = []
    if technical.get("above_ma20"):
        flags.append("站上MA20")
    if technical.get("golden_cross"):
        flags.append("均线金叉")
    if score.get("entry_signal"):
        flags.append("入场信号")
    if flags:
        trend.append(" / ".join(flags))
    return route_lines + trend


def _risk_lines(analysis: dict, score: dict, decision: dict) -> list[str]:
    lines: list[str] = []
    for item in score.get("hard_veto", []) or []:
        lines.append(f"硬否决：{_label_cn(item)}")
    for item in decision.get("veto_reasons", []) or []:
        lines.append(f"门控：{_label_cn(item)}")
    for item in score.get("warning_signals", []) or []:
        lines.append(f"预警：{_label_cn(item)}")
    for item in analysis.get("findings", []) or []:
        if "warning" in str(item).lower() or "risk" in str(item).lower() or "veto" in str(item).lower():
            lines.append(_finding_cn(item))
    for item in decision.get("notes", []) or []:
        lines.append(_finding_cn(item))
    return lines[:6]


def _catalyst_lines(analysis: dict) -> list[str]:
    sentiment = analysis.get("sentiment", {}) or {}
    candidates = []
    for key in ("catalysts", "news", "announcements", "events"):
        value = analysis.get(key) or sentiment.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    lines = []
    for item in candidates:
        if isinstance(item, dict):
            text = item.get("title") or item.get("summary") or item.get("brief") or item.get("name")
        else:
            text = str(item)
        if text:
            lines.append(str(text))
    return lines[:5]


def format_stop_alert_embed(signal: dict) -> dict:
    """止损/止盈告警 → Discord embed dict。"""
    code = signal.get("code", "")
    signal_type = signal.get("signal_type", "")
    desc = signal.get("description", "")
    urgency = signal.get("urgency", "")

    color = COLORS["stop_alert"] if "stop" in signal_type else COLORS["profit_alert"]
    title_map = {
        "stop_loss": "🔴 止损触发",
        "trailing_stop": "🟠 移动止盈触发",
        "time_stop": "⏰ 时间止损",
        "ma_exit": "📉 MA 跌破离场",
    }
    title = title_map.get(signal_type, f"⚠️ {signal_type}")

    fields = [
        _field("代码", code),
        _field("类型", SIGNAL_TYPE_CN.get(signal_type, signal_type)),
        _field("紧急度", URGENCY_CN.get(urgency, urgency)),
        _field("说明", desc, inline=False),
    ]

    return _embed(title=title, color=color, fields=fields, footer="A-Stock Trading · risk_alert")


def format_combined_stop_alert_embed(signals: list[dict]) -> dict:
    """合并风控告警 → 单张 Discord embed dict。"""
    if not signals:
        return _embed(title="⚠️ 风控告警", color=COLORS["stop_alert"], fields=[])

    # 按紧急度排序：immediate > end_of_day > advisory
    urgency_order = {"immediate": 0, "end_of_day": 1, "advisory": 2}
    sorted_signals = sorted(signals, key=lambda s: urgency_order.get(s.get("urgency", ""), 9))

    title_map = {
        "stop_loss": "🔴 止损触发",
        "trailing_stop": "🟠 移动止盈触发",
        "time_stop": "⏰ 时间止损",
        "ma_exit": "📉 MA 跌破离场",
    }

    fields = []
    max_signal_fields = MAX_EMBED_FIELDS
    if len(sorted_signals) > MAX_EMBED_FIELDS:
        max_signal_fields = MAX_EMBED_FIELDS - 1

    for s in sorted_signals[:max_signal_fields]:
        code = s.get("code", "")
        signal_type = s.get("signal_type", "")
        urgency = s.get("urgency", "")
        desc = s.get("description", "")
        emoji = title_map.get(signal_type, "⚠️").split()[0]
        type_cn = SIGNAL_TYPE_CN.get(signal_type, signal_type)
        urgency_cn = URGENCY_CN.get(urgency, urgency)
        fields.append(_field(
            f"{emoji} {code}",
            f"{type_cn} · {urgency_cn}\n{desc}",
            inline=False,
        ))

    remaining = len(sorted_signals) - max_signal_fields
    if remaining > 0:
        fields.append(_field(
            "其余告警",
            f"还有 **{remaining}** 条未展开，请查看日志或后续处理。",
            inline=False,
        ))

    # 统计摘要
    immediate_count = sum(1 for s in signals if s.get("urgency") == "immediate")
    eod_count = sum(1 for s in signals if s.get("urgency") == "end_of_day")
    summary_parts = []
    if immediate_count:
        summary_parts.append(f"🔴 {immediate_count} 立即处理")
    if eod_count:
        summary_parts.append(f"📉 {eod_count} 收盘前处理")

    desc = " | ".join(summary_parts) if summary_parts else f"共 {len(signals)} 条风控触发"

    return _embed(
        title=f"⚠️ 风控告警（{len(signals)}）",
        description=desc,
        color=COLORS["stop_alert"],
        fields=fields,
        footer="A-Stock Trading · risk_alert",
    )


def format_intraday_monitor_embed(data: dict) -> dict:
    """盘中持仓风控轮巡 → Discord embed dict。"""
    time_str = data.get("time", local_now_str())
    alerts = data.get("alerts", [])
    positions = data.get("positions", [])

    title_map = {
        "daily_loss": "🔴 单日浮亏",
        "stop_loss": "🔴 止损触发",
        "trailing_stop": "🟠 移动止盈",
        "ma_exit": "📉 MA 离场",
    }

    fields = []
    for alert in alerts[:MAX_EMBED_FIELDS - 1]:
        signal_type = alert.get("signal_type", "")
        label = title_map.get(signal_type, f"⚠️ {signal_type}")
        code = alert.get("code", "")
        name = alert.get("name", code)
        current = alert.get("current_price", 0)
        desc = alert.get("description", "")
        fields.append(_field(
            f"{label} · {name}({code})",
            f"现价 {current:.2f}\n{desc}",
            inline=False,
        ))

    if len(alerts) > MAX_EMBED_FIELDS - 1:
        fields.append(_field(
            "其余告警",
            f"还有 **{len(alerts) - (MAX_EMBED_FIELDS - 1)}** 条未展开。",
            inline=False,
        ))

    if not fields and positions:
        summary = []
        for p in positions[:8]:
            summary.append(f"{p['name']}({p['code']}) {p['price']:.2f} / {p['change_pct']:+.2f}%")
        fields.append(_field("持仓快照", "\n".join(summary), inline=False))

    return _embed(
        title=f"⚠️ 盘中风控监控 — {time_str}",
        description=f"触发 {len(alerts)} 条告警",
        color=COLORS["stop_alert"] if alerts else COLORS["info"],
        fields=fields,
        footer="A-Stock Trading · intraday_monitor",
    )


def format_weekly_embed(data: dict) -> dict:
    """周报 → Discord embed dict。"""
    week = data.get("week", "")
    buy_count = data.get("buy_count", 0)
    sell_count = data.get("sell_count", 0)
    win_rate = data.get("win_rate", 0)
    profit_loss_ratio = data.get("profit_loss_ratio", 0)
    net_pnl = data.get("net_pnl_cents", 0) / 100
    positions = data.get("positions", [])

    pnl_emoji = "🟢" if net_pnl >= 0 else "🔴"
    sign = "+" if net_pnl >= 0 else ""

    fields = [
        _field("本周收益", f"{pnl_emoji} **{sign}¥{net_pnl:,.0f}**", inline=False),
        _field("买入", f"`{buy_count} 笔`"),
        _field("卖出", f"`{sell_count} 笔`"),
        _field("胜率", f"`{win_rate:.0%}`"),
        _field("盈亏比", f"`{profit_loss_ratio:.2f}`"),
    ]

    if positions:
        pos_lines = []
        for p in positions:
            pos_lines.append(f"• {p.get('name', '')}({p.get('code', '')}) {p.get('shares', 0)}股")
        fields.append(_field("当前持仓", "\n".join(pos_lines) or "空仓", inline=False))
    else:
        fields.append(_field("当前持仓", "空仓", inline=False))

    return _embed(
        title=f"📋 周报 — {week}",
        color=COLORS["weekly"],
        fields=fields,
        footer="A-Stock Trading · weekly_review",
    )


def format_sentiment_embed(alerts: list[dict]) -> dict:
    """舆情告警 → Discord embed dict。"""
    now = local_now_str("%H:%M")
    fields = []

    # 按 level 分组：negative 优先
    order = {"negative": 0, "event": 1, "positive": 2}
    sorted_alerts = sorted(alerts, key=lambda a: order.get(a.get("level", ""), 9))

    for a in sorted_alerts[:20]:  # Discord 最多 25 fields
        emoji = a.get("emoji", "📰")
        name = a.get("name", "")
        code = a.get("code", "")
        summary = a.get("summary", "")
        brief = a.get("brief", "")
        date = a.get("date", "")

        value_parts = [f"**{summary}**"]
        if brief:
            value_parts.append(brief)
        if date:
            value_parts.append(f"_{date}_")

        fields.append(_field(
            f"{emoji} {name}({code})",
            "\n".join(value_parts),
            inline=False,
        ))

    neg = sum(1 for a in alerts if a.get("level") == "negative")
    pos = sum(1 for a in alerts if a.get("level") == "positive")
    evt = sum(1 for a in alerts if a.get("level") == "event")
    desc_parts = []
    if neg:
        desc_parts.append(f"🔴 {neg} 负面")
    if pos:
        desc_parts.append(f"🟢 {pos} 正面")
    if evt:
        desc_parts.append(f"📢 {evt} 事件")

    return _embed(
        title=f"📰 舆情速报 — {now}",
        description=" · ".join(desc_parts) or f"共 {len(alerts)} 条",
        color=COLORS["sentiment"],
        fields=fields,
        footer="A-Stock Trading · sentiment_monitor",
    )


def format_propose_plan_embed(plan: dict) -> dict:
    """交易计划摘要 → Discord embed dict。"""
    diagnostics = plan.get("diagnostics", {}) or {}
    inputs = diagnostics.get("inputs", {}) or {}
    pool = inputs.get("candidate_pool", {}) or {}
    data_sources = inputs.get("data_sources", {}) or {}
    actions = plan.get("actions", []) or []
    findings = diagnostics.get("findings", []) or []

    execution_allowed = bool(plan.get("execution_allowed"))
    execution_text = "允许自动执行" if execution_allowed else "禁止自动执行"
    color = COLORS["profit_alert"] if execution_allowed else COLORS["info"]

    fields = [
        _field("执行状态", execution_text),
        _field(
            "健康状态",
            "诊断={diagnostics} / 数据源={data_sources}".format(
                diagnostics=_label_cn(diagnostics.get("status", "unknown")),
                data_sources=_label_cn(data_sources.get("status", "unknown")),
            ),
        ),
        _field(
            "候选池",
            "总数={total} 核心={core} 观察={watch}\n最近评分={latest}".format(
                total=pool.get("total", 0),
                core=pool.get("core_count", 0),
                watch=pool.get("watch_count", 0),
                latest=pool.get("latest_scored_at", "-"),
            ),
            inline=False,
        ),
    ]

    if findings:
        fields.append(_field("阻断/发现", "\n".join(f"- {_finding_cn(item)}" for item in findings[:5]), inline=False))

    if actions:
        action_lines = [
            f"- [{_label_cn(item.get('priority', '-'))}] {_label_cn(item.get('type', '-'))}：{_finding_cn(item.get('reason', ''))}"
            for item in actions[:5]
        ]
        fields.append(_field("建议动作", "\n".join(action_lines), inline=False))
    else:
        fields.append(_field("建议动作", "暂无", inline=False))

    required_missing = data_sources.get("required_missing", []) or []
    optional_missing = data_sources.get("optional_missing", []) or []
    if required_missing or optional_missing:
        fields.append(_field(
            "数据源缺口",
            f"核心源={_labels_cn(required_missing)}\n辅助源={_labels_cn(optional_missing)}",
            inline=False,
        ))

    return _embed(
        title=f"交易计划 — {local_today_str()}",
        color=color,
        fields=fields,
        footer="A-Stock Trading · propose_plan",
    )


def format_daily_inspection_embed(summary: dict) -> dict:
    """每日巡检摘要 → Discord embed dict。"""
    failed_commands = summary.get("failed_commands", []) or []
    required_missing = summary.get("required_missing", []) or []
    optional_missing = summary.get("optional_missing", []) or []
    pool = summary.get("candidate_pool", {}) or {}
    plan_actions = summary.get("plan_actions", []) or []

    has_problem = (
        bool(failed_commands)
        or summary.get("health_status") == "failed"
        or summary.get("diagnose_health_status") == "failed"
        or bool(required_missing)
    )
    color = COLORS["stop_alert"] if has_problem else COLORS["info"]

    fields = [
        _field(
            "系统状态",
            "doctor={doctor}\nhealth={health}\ndiagnose={diagnose}".format(
                doctor=_label_cn(summary.get("doctor_status", "unknown")),
                health=_label_cn(summary.get("health_status", "unknown")),
                diagnose=_label_cn(summary.get("diagnose_health_status", "unknown")),
            ),
        ),
        _field(
            "运行状态",
            "失败运行={failed}\n运行中={running}".format(
                failed=summary.get("failed_runs_count", 0),
                running=summary.get("running_runs_count", 0),
            ),
        ),
        _field(
            "数据源",
            "状态={status}\n核心源={required}\n辅助源={optional}".format(
                status=_label_cn(summary.get("data_source_status", "unknown")),
                required=_labels_cn(required_missing),
                optional=_labels_cn(optional_missing),
            ),
            inline=False,
        ),
        _field(
            "候选池",
            "总数={total} 核心={core} 观察={watch}".format(
                total=pool.get("total", pool.get("total_count", 0)),
                core=pool.get("core_count", 0),
                watch=pool.get("watch_count", 0),
            ),
        ),
        _field("人工确认", f"待确认 {summary.get('pending_manual_trades', 0)}"),
        _field(
            "模拟盘",
            f"持仓 {summary.get('paper_positions', 0)} / 资产 ¥{summary.get('paper_total_asset', 0):,.0f}",
        ),
    ]

    if failed_commands:
        fields.append(_field(
            "命令失败",
            "\n".join(f"- {item.get('name')} exit={item.get('returncode')}" for item in failed_commands[:5]),
            inline=False,
        ))

    manual_items = summary.get("pending_manual_trade_items", []) or []
    if manual_items:
        fields.append(_field(
            "人工确认明细",
            "\n".join(_manual_trade_lines(manual_items[:5])),
            inline=False,
        ))

    route_blockers = summary.get("route_blocked_watch_candidates", []) or []
    if route_blockers:
        fields.append(_field(
            "观察池阻断",
            "\n".join(_route_blocker_lines(route_blockers[:5])),
            inline=False,
        ))

    if plan_actions:
        action_lines = [
            f"- [{_label_cn(item.get('priority', '-'))}] {_label_cn(item.get('type', '-'))}"
            for item in plan_actions[:5]
        ]
        fields.append(_field("交易计划", "\n".join(action_lines), inline=False))
    else:
        fields.append(_field("交易计划", "暂无动作", inline=False))

    report_path = _short_report_path(str(summary.get("report_path", "")))
    if report_path:
        fields.append(_field("报告", report_path, inline=False))

    return _embed(
        title=f"每日巡检 — {summary.get('date', local_today_str())}",
        color=color,
        fields=fields,
        footer="A-Stock Trading · daily_inspection",
    )


def _short_report_path(path: str) -> str:
    marker = "trade-vault/"
    if marker in path:
        return path[path.index(marker):]
    return path


def _manual_trade_lines(items: list[dict]) -> list[str]:
    lines = []
    for item in items:
        code = item.get("code", "")
        name = item.get("name") or code
        side = _label_cn(item.get("side", "-"))
        score = _to_float(item.get("score", item.get("confidence")))
        position = _pct_text(item.get("position_pct", 0))
        lines.append(f"{name}({code}) {side} · 评分 {score:.1f} · 仓位 {position}")
    return lines


def _route_blocker_lines(items: list[dict]) -> list[str]:
    lines = []
    for item in items:
        code = item.get("code", "")
        name = item.get("name") or code
        score = _to_float(item.get("score"))
        reason = _candidate_note_label(str(item.get("note", "")))
        lines.append(f"{name}({code}) {score:.1f} · {reason}")
    return lines


def _candidate_note_label(note: str) -> str:
    if "requires_entry_strategy_route" in note:
        return "缺少有效策略路线"
    return note or "待复核"


def format_sector_heatmap_embed(sectors: list[dict], title: str = "") -> dict:
    """行业热力图 → Discord embed dict。

    Args:
        sectors: get_sector_heatmap() 返回的板块列表（已按涨跌幅降序）
        title: 自定义标题前缀（如 "盘前" / "收盘"）
    """
    if not sectors:
        return _embed(
            title="🏭 行业热力图",
            description="板块数据获取失败",
            color=COLORS["info"],
            fields=[],
            footer="A-Stock Trading · sector_heatmap",
        )

    # 涨幅前 5 / 跌幅前 5
    gainers, losers = top_sector_movers(sectors, limit=5)

    fields = []

    # 涨幅前排
    if gainers:
        gainer_lines = []
        for s in gainers:
            pct = s.get("change_pct", 0)
            amount = s.get("amount", 0)
            amount_str = f"¥{amount / 1e8:.1f}亿" if amount >= 1e8 else f"¥{amount / 1e4:.0f}万"
            gainer_lines.append(f"🔺 {s.get('name', '')} `{pct:+.2f}%` {amount_str}")
        fields.append(_field("\u200b", "**🔥 涨幅前 5**", inline=False))
        for line in gainer_lines:
            fields.append(_field("\u200b", line, inline=False))

    # 跌幅前排
    if losers:
        loser_lines = []
        for s in losers:
            pct = s.get("change_pct", 0)
            amount = s.get("amount", 0)
            amount_str = f"¥{amount / 1e8:.1f}亿" if amount >= 1e8 else f"¥{amount / 1e4:.0f}万"
            loser_lines.append(f"🔻 {s.get('name', '')} `{pct:+.2f}%` {amount_str}")
        fields.append(_field("\u200b", "**❄️ 跌幅前 5**", inline=False))
        for line in loser_lines:
            fields.append(_field("\u200b", line, inline=False))

    # 成交额 top 3（主线板块）
    by_amount = sorted(sectors, key=lambda s: s.get("amount", 0), reverse=True)[:3]
    if by_amount:
        amount_lines = []
        for s in by_amount:
            pct = s.get("change_pct", 0)
            amount = s.get("amount", 0)
            amount_str = f"¥{amount / 1e8:.1f}亿"
            sign = "+" if pct >= 0 else ""
            amount_lines.append(f"  {s.get('name', '')} {sign}{pct:.2f}% · {amount_str}")
        fields.append(_field("\u200b", "**📊 成交额前 3（主线）**", inline=False))
        for line in amount_lines:
            fields.append(_field("\u200b", line, inline=False))

    # 注：板块 up_count 之和 ≠ 全市场涨跌家数（一只股票属多个行业，重复计算）
    fields.append(_field(
        "板块统计",
        f"共 **{len(sectors)}** 个行业板块（涨跌家数仅供板块内参考）",
        inline=False,
    ))

    prefix = f"{title} · " if title else ""
    return _embed(
        title=f"{prefix}🏭 行业热力图",
        description=f"共 {len(sectors)} 个行业板块",
        color=COLORS["info"],
        fields=fields,
        footer="A-Stock Trading · sector_heatmap",
    )
