"""
reporting/discord.py — Discord 消息格式化

只负责格式化，不负责发送。实际发送由 Agent Gateway 或 V1 discord_push 处理。
reporting 不反写任何业务表。
"""

from __future__ import annotations


from astock_trading.platform.time import local_now_iso, local_now_str, local_today_str
from astock_trading.reporting.market_formatters import top_sector_movers


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
}

SIGNAL_EMOJI = {"GREEN": "🟢", "YELLOW": "🟡", "RED": "🔴", "CLEAR": "⚪"}
SIGNAL_CN = {"GREEN": "偏强", "YELLOW": "震荡", "RED": "转弱", "CLEAR": "观望"}

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


def _score_emoji(score: float) -> str:
    if score >= 7:
        return "✅"
    if score >= 5:
        return "🟡"
    return "❌"


def _pnl_emoji(v: float) -> str:
    return "🟢" if v >= 0 else "🔴"


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
            fields.append(_field(s.get("name", ""), f"{_score_emoji(score)} **{score:.1f}**"))

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
        fields.append(_field(f"{emoji} {name}", f"**{score:.1f}** · {detail}"))

    return _embed(
        title=f"🎯 核心池评分 — {date_str}",
        description=f"共 {len(scores)} 只",
        color=COLORS["scoring"],
        fields=fields,
        footer="A-Stock Trading · scoring",
    )


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
