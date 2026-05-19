"""
reporting/market_formatters.py — 市场数据格式化辅助

抽离可复用的热力图展示逻辑，避免 pipeline 之间互相依赖私有 helper。
"""

from __future__ import annotations

import math


def _to_float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def _format_amount_short(amount: float) -> str:
    if amount >= 1e8:
        return f"{amount / 1e8:.1f}亿"
    return f"{amount / 1e4:.0f}万"


def _format_stock_label(item: dict) -> str:
    name = item.get("name") or item.get("code") or item.get("symbol", "")
    code = item.get("code") or item.get("symbol", "")
    if code and code != name:
        return f"{name}({code})"
    return str(name)


_SOURCE_LABELS = {
    "xueqiu": "雪球",
    "eastmoney": "东财",
    "sinafinance": "新浪",
    "ths": "同花顺",
    "tdx": "通达信",
    "bloomberg": "彭博",
    "reuters": "路透",
}


def _source_label(source: str) -> str:
    return _SOURCE_LABELS.get(source, source)


def _source_list_label(sources: list[str]) -> str:
    return "/".join(_source_label(s) for s in sources if s)


def format_hot_stock_change_context(item: dict) -> str:
    """格式化热榜个股涨跌信息，避免把热榜口径误当实时行情。"""
    realtime_pct = _to_float_or_none(item.get("realtime_change_pct"))
    realtime_price = _to_float_or_none(item.get("realtime_price"))
    hot_list_pct = _to_float_or_none(item.get("change_pct"))

    parts = []
    if realtime_pct is not None:
        if realtime_price is not None and realtime_price > 0:
            parts.append(f"现价 `{realtime_price:.2f}`")
        parts.append(f"现涨 `{realtime_pct:+.2f}%`")
        if hot_list_pct is not None:
            parts.append(f"热榜口径 `{hot_list_pct:+.2f}%`")
    elif hot_list_pct is not None:
        parts.append(f"热榜口径 `{hot_list_pct:+.2f}%`(非实时)")

    return " · ".join(parts) if parts else "热榜关注"


def _brief_text(item: dict) -> str:
    return str(item.get("summary") or item.get("content") or item.get("text") or "").strip()


def _shorten(text: str, max_len: int = 72) -> str:
    text = " ".join(str(text or "").split())
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."


def _infer_market_impact(item: dict, kind: str) -> str:
    text = f"{item.get('title', '')} {_brief_text(item)} {item.get('category', '')}".lower()
    if any(k in text for k in ("中美", "关税", "贸易", "经贸", "tariff", "trade talks", "trade war")):
        return "宏观/出口链/人民币风险"
    if any(k in text for k in ("fed", "rate", "inflation", "yield", "ecb", "美联储", "利率", "通胀", "收益率", "央行")):
        return "利率/成长股估值"
    if any(k in text for k in ("伊朗", "霍尔木兹", "原油", "油价", "oil", "geopolitical", "地缘", "战争")):
        return "能源/航运/避险情绪"
    if any(k in text for k in ("ai", "算力", "芯片", "半导体", "服务器", "数据中心")):
        return "AI算力/半导体链"
    if any(k in text for k in ("光伏", "储能", "锂电", "新能源", "电池")):
        return "新能源链"
    if kind == "announcement" or any(k in text for k in ("复牌", "控制权", "立案", "处罚", "退市", "减持", "业绩预告")):
        return "个股事件风险"
    if kind == "global_risk":
        return "海外风险偏好"
    return "题材/风险背景"


def _suggest_market_action(kind: str, impact: str) -> str:
    if kind == "announcement":
        return "持仓/核心池命中才人工复核，不因公告追高"
    if kind == "global_risk":
        return "盘前看风险偏好，新增开仓先降档"
    if impact in {"宏观/出口链/人民币风险", "利率/成长股估值", "能源/航运/避险情绪"}:
        return "作为仓位背景，等指数和资金确认"
    return "作为观察线索，等价格和资金确认"


def format_market_intel_line(item: dict, kind: str) -> str:
    """Format opencli news into a trading-facing one-line note."""
    source = _source_label(item.get("source", ""))
    time = item.get("time", "")
    title = str(item.get("title") or _brief_text(item) or "").strip()
    prefix = f"{time} " if time else ""
    summary = _brief_text(item)
    summary_text = ""
    if summary and summary != title:
        summary_text = f" | 摘要: {_shorten(summary)}"
    impact = _infer_market_impact(item, kind)
    action = _suggest_market_action(kind, impact)
    return f"{prefix}{title} ({source}){summary_text} | 影响: {impact} | 动作: {action}"


def format_announcement_intel_line(item: dict) -> str:
    label = _format_stock_label(item)
    category = item.get("category", "")
    suffix = f" [{category}]" if category else ""
    impact = _infer_market_impact(item, "announcement")
    action = _suggest_market_action("announcement", impact)
    return f"{label}: {item.get('title', '')}{suffix} | 影响: {impact} | 动作: {action}"


def top_sector_movers(sectors: list[dict], limit: int = 5) -> tuple[list[dict], list[dict]]:
    """返回涨幅前 N 和跌幅前 N。

    `sectors` 默认已按涨跌幅降序排列。跌幅榜需要单独按升序取最小值，
    否则会拿到最接近 0 的下跌板块。
    """
    gainers = [s for s in sectors if s.get("change_pct", 0) > 0][:limit]
    losers = sorted(
        (s for s in sectors if s.get("change_pct", 0) < 0),
        key=lambda s: s.get("change_pct", 0),
    )[:limit]
    return gainers, losers


def format_sector_heatmap_markdown(sectors: list[dict], market_stats: dict = None) -> list[str]:
    """把板块数据格式化为 markdown 表格。

    Args:
        sectors: 板块列表
        market_stats: 全市场升降家数 {"up": int, "down": int, "flat": int, "total": int}
    """
    if not sectors:
        return ["数据获取失败"]

    lines = []
    gainers, losers = top_sector_movers(sectors)

    if gainers:
        lines.append("| 板块 | 涨跌幅 | 成交额 |")
        lines.append("|------|--------|--------|")
        for sector in gainers:
            pct = sector.get("change_pct", 0)
            amount = _format_amount_short(sector.get("amount", 0))
            lines.append(f"| 🔺 {sector.get('name', '')} | `{pct:+.2f}%` | {amount} |")

    if losers:
        lines.append("")
        lines.append("| 板块 | 涨跌幅 | 成交额 |")
        lines.append("|------|--------|--------|")
        for sector in losers:
            pct = sector.get("change_pct", 0)
            amount = _format_amount_short(sector.get("amount", 0))
            lines.append(f"| 🔻 {sector.get('name', '')} | `{pct:+.2f}%` | {amount} |")

    if market_stats and market_stats.get("total", 0) > 0:
        lines.append("")
        lines.append(f"*全市场：🔺 **{market_stats['up']}** | 🔻 **{market_stats['down']}** | ⚪ **{market_stats['flat']}** ({market_stats['total']} 只)*")
    else:
        lines.append("")
        lines.append(f"*共 {len(sectors)} 个板块，涨跌数据仅供板块内参考*")
    return lines


def format_market_signals_markdown(
    hot_stocks: list[dict] | None = None,
    xueqiu_hot_stocks: list[dict] | None = None,
    cross_platform_hot_stocks: list[dict] | None = None,
    finance_flash: list[dict] | None = None,
    global_risk_news: list[dict] | None = None,
    market_announcements: list[dict] | None = None,
    northbound: list[dict] | None = None,
    dragon_tiger: dict | None = None,
    lockup: dict | None = None,
) -> list[str]:
    """把事件型市场信号格式化为日志 markdown。"""
    lines = ["### 市场信号"]

    hot_stocks = hot_stocks or []
    if hot_stocks:
        lines.append("")
        lines.append("**热点题材**")
        for item in hot_stocks[:5]:
            name = item.get("name") or item.get("code", "")
            code = item.get("code", "")
            pct = item.get("change_pct", 0) or 0
            reason = item.get("reason", "")
            lines.append(f"- {name}({code}) `{pct:+.2f}%` {reason}")

    xueqiu_hot_stocks = xueqiu_hot_stocks or []
    if xueqiu_hot_stocks:
        lines.append("")
        lines.append("**雪球热搜**")
        for item in xueqiu_hot_stocks[:5]:
            rank = item.get("rank") or ""
            rank_text = f"#{rank} " if rank else ""
            pct = item.get("change_pct", 0) or 0
            heat = item.get("heat", 0) or 0
            heat_text = f"热度 {heat}" if heat else ""
            lines.append(f"- {rank_text}{_format_stock_label(item)} `{pct:+.2f}%` {heat_text}".rstrip())

    cross_platform_hot_stocks = cross_platform_hot_stocks or []
    if cross_platform_hot_stocks:
        lines.append("")
        lines.append("**跨平台热度**")
        for item in cross_platform_hot_stocks[:5]:
            sources = _source_list_label(item.get("sources", []))
            source_count = item.get("source_count", len(item.get("sources", [])) or 1)
            change_text = format_hot_stock_change_context(item)
            lines.append(f"- {_format_stock_label(item)} {change_text} · {source_count}源共振 {sources}".rstrip())

    market_announcements = market_announcements or []
    if market_announcements:
        lines.append("")
        lines.append("**公告提示**")
        for item in market_announcements[:5]:
            lines.append(f"- {format_announcement_intel_line(item)}")

    finance_flash = finance_flash or []
    if finance_flash:
        lines.append("")
        lines.append("**财经快讯**")
        for item in finance_flash[:5]:
            lines.append(f"- {format_market_intel_line(item, 'finance_flash')}")

    global_risk_news = global_risk_news or []
    if global_risk_news:
        lines.append("")
        lines.append("**海外风险**")
        for item in global_risk_news[:5]:
            lines.append(f"- {format_market_intel_line(item, 'global_risk')}")

    northbound = northbound or []
    if northbound:
        last = northbound[-1]
        hgt = last.get("hgt_yi")
        sgt = last.get("sgt_yi")
        lines.append("")
        if hgt is not None or sgt is not None:
            lines.append(f"**北向资金** {last.get('time', '')}: 沪股通 {hgt}亿 / 深股通 {sgt}亿")
        else:
            total = last.get("total_net_yi", last.get("totalNetYi"))
            cumulative = last.get("cumulative_net_yi", last.get("cumulativeNetYi"))
            lines.append(f"**北向资金** {last.get('time', '')}: 当日 {total}亿 / 累计 {cumulative}亿")

    stocks = (dragon_tiger or {}).get("stocks", [])
    if stocks:
        lines.append("")
        lines.append("**龙虎榜净买入**")
        for item in stocks[:5]:
            name = item.get("name") or item.get("code", "")
            code = item.get("code", "")
            net = item.get("net_buy_wan", 0) or 0
            reason = item.get("reason", "")
            lines.append(f"- {name}({code}) 净买入 {net:,.0f}万 {reason}")

    upcoming = (lockup or {}).get("upcoming", [])
    if upcoming:
        lines.append("")
        lines.append("**解禁预警**")
        for item in upcoming[:5]:
            ratio = item.get("float_ratio", item.get("ratio", 0)) or 0
            lines.append(f"- {item.get('date', '')} {item.get('type', '')} 占流通股 {ratio}%")

    if len(lines) == 1:
        return []
    return lines
