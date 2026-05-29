"""
pipeline/noon.py — 午休检查

流程：
1. 读持仓 + 当前价格
2. 检查风控（止损/止盈是否接近触发）
3. 检查是否有加仓机会（核心池高分 + 大盘 GREEN）
4. 写 Obsidian 日志
5. 格式化 Discord embed 推送
"""

from __future__ import annotations

import asyncio
import logging

from astock_trading.pipeline.context import PipelineContext
from astock_trading.pipeline.helpers import check_position_risks
from astock_trading.pipeline.notification_policy import should_push_sector_heatmap
from astock_trading.platform.history_mirror import archive_market_signal_snapshot
from astock_trading.platform.time import local_today_str
from astock_trading.reporting.discord import _embed, _field, COLORS
from astock_trading.reporting.market_formatters import (
    _format_stock_label,
    _source_label,
    _source_list_label,
    format_hot_stock_change_context,
    format_market_signals_markdown,
    format_sector_heatmap_markdown,
)

_logger = logging.getLogger(__name__)


def _normal_a_stock_code(item: dict) -> str:
    code = str(item.get("code") or item.get("symbol") or "").strip()
    return code if code.isdigit() and len(code) == 6 else ""


async def _attach_realtime_hot_stock_quotes(market_svc, hot_stocks: list[dict], run_id: str) -> list[dict]:
    """给热榜个股补实时行情字段；失败时保留原始热榜口径。"""
    if not hot_stocks:
        return []

    enriched = [dict(item) for item in hot_stocks]
    quote_targets = []
    seen_codes = set()
    for item in enriched[:5]:
        code = _normal_a_stock_code(item)
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        quote_targets.append({"code": code, "name": item.get("name") or code})

    if not quote_targets:
        return enriched

    try:
        snapshots = await market_svc.collect_intraday_batch(quote_targets, run_id=run_id)
    except Exception as exc:
        _logger.warning("[noon] 热榜实时行情校验失败，保留热榜口径: %s", exc)
        return enriched

    quotes_by_code = {}
    for snapshot in snapshots:
        quote = getattr(snapshot, "quote", None)
        if quote is None:
            continue
        code = getattr(snapshot, "code", "") or getattr(quote, "code", "")
        if code:
            quotes_by_code[str(code)] = quote

    for item in enriched:
        quote = quotes_by_code.get(_normal_a_stock_code(item))
        if quote is None:
            continue
        item["realtime_price"] = quote.price
        item["realtime_change_pct"] = quote.change_pct
        item["realtime_high"] = quote.high
        item["realtime_amount"] = quote.amount
    return enriched


def _format_noon_embed(data: dict) -> dict:
    """午休检查 → Discord embed。"""
    date_str = data.get("date", "")
    fields = []

    # 大盘
    signal = data.get("signal", "")
    fields.append(_field("大盘信号", f"**{signal}**"))

    # 持仓
    positions = data.get("positions", [])
    if positions:
        for p in positions:
            pnl = p.get("pnl_pct", 0)
            emoji = "🟢" if pnl >= 0 else "🔴"
            fields.append(_field(
                f"{emoji} {p['name']}",
                f"{p['shares']}股 · 成本 ¥{p['cost']:.2f}\n现价 ¥{p['price']:.2f} · 盈亏 **{pnl:+.2f}%**",
            ))
    else:
        fields.append(_field("持仓", "空仓"))

    # 风控
    alerts = data.get("alerts", [])
    if alerts:
        fields.append(_field(f"⚠️ 风控提示（{len(alerts)}）", "\n".join(f"• {a}" for a in alerts), inline=False))

    # 提示
    tips = data.get("tips", [])
    if tips:
        fields.append(_field("📋 提示", "\n".join(f"• {t}" for t in tips), inline=False))

    cross_hot = data.get("cross_platform_hot_stocks", []) or []
    finance_flash = data.get("finance_flash", []) or []
    info_lines = []
    for item in cross_hot[:3]:
        sources = _source_list_label(item.get("sources", []))
        change_text = format_hot_stock_change_context(item)
        info_lines.append(f"{_format_stock_label(item)} {change_text} · {sources}".rstrip(" ·"))
    for item in finance_flash[:3]:
        source = _source_label(item.get("source", ""))
        time = item.get("time", "")
        prefix = f"{time} " if time else ""
        info_lines.append(f"{prefix}{item.get('title', '')} ({source})")
    if info_lines:
        fields.append(_field("午间信息", "\n".join(info_lines), inline=False))

    return _embed(
        title=f"☀️ 午休检查 — {date_str}",
        color=COLORS["noon"],
        fields=fields,
        footer="A-Stock Trading · noon_check",
    )


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行午休检查 pipeline。"""

    # 0. 始终刷新持仓价格（不受幂等影响），日报发送前确保数据最新
    from astock_trading.pipeline.helpers import refresh_position_prices
    refresh_position_prices(ctx, run_id=run_id)

    # 1. 大盘
    market_state, index_data = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal.value

    # 同步指数数据到 projection_market_state 表
    if index_data:
        ctx.projector.sync_market_state(index_data)
    history_group_id = archive_market_signal_snapshot(
        ctx.conn,
        run_id=run_id,
        phase="noon",
        market_state=market_state,
        index_data=index_data,
    )

    # 2. 持仓 + 风控（带 MA 数据 + 配置文件参数）
    positions = ctx.exec_svc.get_positions()
    risk_results = check_position_risks(ctx, positions, run_id)
    alerts = []
    pos_data = []

    for pos, signals in risk_results:
        current = pos.current_price or pos.avg_cost
        pnl_pct = (current - pos.avg_cost) / pos.avg_cost * 100 if pos.avg_cost else 0

        for s in signals:
            alerts.append(f"{pos.name}({pos.code}): {s.description}")

        pos_data.append({
            "name": pos.name, "code": pos.code, "shares": pos.shares,
            "cost": pos.avg_cost, "price": current, "pnl_pct": pnl_pct,
            "currency": getattr(pos, "currency", "CNY"),
        })

    # 3. 加仓提示
    tips = []
    if signal in ("GREEN", "YELLOW"):
        pool_rows = ctx.conn.execute(
            "SELECT code, name, score FROM projection_candidate_pool WHERE pool_tier = 'core' AND score >= 7 ORDER BY score DESC LIMIT 3"
        ).fetchall()
        for r in pool_rows:
            tips.append(f"{r['name']}({r['code']}) 评分 {r['score']:.1f}，可关注加仓")
    if signal in ("RED", "CLEAR"):
        tips.append(f"大盘 {signal}，不建议操作")

    # 4. Obsidian 日志
    log_lines = ["## 午休检查", "", f"大盘: **{signal}**", ""]
    if pos_data:
        for p in pos_data:
            emoji = "🟢" if p["pnl_pct"] >= 0 else "🔴"
            log_lines.append(f"- {emoji} {p['name']} {p['pnl_pct']:+.1f}%")
    if alerts:
        log_lines.extend(["", "### 风控提示"] + [f"- ⚠️ {a}" for a in alerts])
    if tips:
        log_lines.extend(["", "### 操作提示"] + [f"- {t}" for t in tips])

    cross_platform_hot_stocks = asyncio.run(ctx.market_svc.collect_cross_platform_hot_stocks(run_id=run_id))
    cross_platform_hot_stocks = asyncio.run(
        _attach_realtime_hot_stock_quotes(ctx.market_svc, cross_platform_hot_stocks, run_id)
    )
    finance_flash = asyncio.run(ctx.market_svc.collect_finance_flash(limit=5, run_id=run_id))
    signal_lines = format_market_signals_markdown(
        cross_platform_hot_stocks=cross_platform_hot_stocks,
        finance_flash=finance_flash,
    )
    if signal_lines:
        log_lines.extend([""] + signal_lines)

    # 行业热力图
    heatmap_sectors = asyncio.run(ctx.market_svc.collect_sector_heatmap(run_id=run_id))
    _logger.info(f"[noon] 行业热力图: {len(heatmap_sectors)} 个板块")
    if heatmap_sectors:
        log_lines.extend(["", "### 行业热力图"] + format_sector_heatmap_markdown(heatmap_sectors))
    else:
        log_lines.extend(["", "### 行业热力图", "数据获取失败"])

    ctx.obsidian.write_daily_log(run_id, "\n".join(log_lines))

    # 刷新每日巡检报告
    ctx.obsidian.write_daily_output_index(run_id)

    # 5. Discord
    embed = _format_noon_embed({
        "date": local_today_str(), "signal": signal,
        "positions": pos_data, "alerts": alerts, "tips": tips,
        "cross_platform_hot_stocks": cross_platform_hot_stocks[:5],
        "finance_flash": finance_flash[:5],
    })

    sector_heatmap_pushed = False
    try:
        from astock_trading.reporting.discord import format_sector_heatmap_embed
        from astock_trading.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[noon] Discord 推送失败: {err}")
        if should_push_sector_heatmap(heatmap_sectors, phase="noon"):
            heatmap_embed = format_sector_heatmap_embed(heatmap_sectors, title="午休")
            ok2, err2 = send_embed(heatmap_embed)
            sector_heatmap_pushed = bool(ok2)
            if not ok2:
                _logger.warning(f"[noon] 热力图 Discord 推送失败: {err2}")
        else:
            _logger.info("[noon] 行业热力图无明显异动，跳过 Discord 单独推送")
    except Exception as e:
        _logger.warning(f"[noon] Discord 推送异常: {e}")

    _logger.info(f"[noon] 完成: {signal}, {len(positions)} 持仓, {len(alerts)} 风控")

    return {
        "signal": signal, "positions": len(positions),
        "alerts": alerts, "tips": tips, "discord_embed": embed,
        "cross_platform_hot_stocks": len(cross_platform_hot_stocks),
        "finance_flash": len(finance_flash),
        "sector_heatmap_pushed": sector_heatmap_pushed,
        "history_group_id": history_group_id,
    }
