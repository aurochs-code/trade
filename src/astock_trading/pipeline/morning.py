"""
pipeline/morning.py — 盘前摘要

流程：
1. 抓大盘信号
2. 读持仓 + 检查风控
3. 读核心池状态
4. 生成今日决策
5. 生成盘前报告 → report_artifacts
6. 写 Obsidian（日志 + 今日决策 + 持仓概览）
7. 格式化 Discord embed
"""

from __future__ import annotations

import asyncio
import logging

from astock_trading.pipeline.context import PipelineContext
from astock_trading.pipeline.helpers import check_position_risks
from astock_trading.pipeline.notification_policy import should_push_sector_heatmap
from astock_trading.platform.time import local_today_str
from astock_trading.reporting.discord import format_morning_embed
from astock_trading.reporting.market_formatters import (
    format_market_signals_markdown,
    format_sector_heatmap_markdown,
)

_logger = logging.getLogger(__name__)


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行盘前摘要 pipeline。"""

    # 1. 大盘信号
    market_state, index_data = asyncio.run(ctx.market_svc.collect_market_state(run_id))
    signal = market_state.signal.value
    multiplier = market_state.multiplier
    _logger.info(f"[morning] 大盘信号: {signal} (multiplier={multiplier})")

    # 同步指数数据到 projection_market_state 表
    if index_data:
        ctx.projector.sync_market_state(index_data)

    # 2. 持仓 + 风控（带 MA 数据 + 配置文件参数）
    # 先刷新持仓实时价格（缓存优先，盘中不重复请求）
    from astock_trading.pipeline.helpers import refresh_position_prices
    refresh_position_prices(ctx)

    positions = ctx.exec_svc.get_positions()
    risk_results = check_position_risks(ctx, positions, run_id)
    risk_alerts = []
    stop_loss_reminders = []
    # 区分 immediate 和 advisory 级别的风控信号
    has_immediate_risk = False
    for pos, signals in risk_results:
        for s in signals:
            risk_alerts.append(f"⚠️ {pos.name}({pos.code}): {s.description} [{s.urgency}]")
            if s.urgency == "immediate":
                has_immediate_risk = True
            # 提取止损挂单提醒
            if s.signal_type in ("stop_loss", "ma_exit", "trailing_stop"):
                stop_loss_reminders.append({
                    "name": pos.name,
                    "code": pos.code,
                    "signal_type": s.signal_type,
                    "trigger_price": s.trigger_price,
                    "description": s.description,
                })

    # 3. 核心池
    pool_rows = ctx.conn.execute(
        """SELECT code, name, score, last_scored_at
           FROM projection_candidate_pool
           WHERE pool_tier = 'core'
           ORDER BY score DESC"""
    ).fetchall()
    core_pool = [
        {
            "name": r["name"] or r["code"],
            "code": r["code"],
            "score": r["score"] or 0,
            "last_scored_at": r["last_scored_at"],
            "score_label": "上次评分",
        }
        for r in pool_rows
    ]

    # 4. 今日决策
    # 只有 immediate 级别的风控信号才阻止买入，advisory（如时间止损）不阻止
    can_buy = signal in ("GREEN", "YELLOW") and not has_immediate_risk
    reasons = [f"market_signal={signal}"]
    if risk_alerts:
        reasons.extend(risk_alerts)

    if signal in ("RED", "CLEAR"):
        decision_action = "NO_TRADE"
    elif not can_buy:
        decision_action = "NO_TRADE"
    elif signal == "YELLOW":
        decision_action = "REDUCED_BUY"
    else:
        decision_action = "BUY_ALLOWED"

    ctx.obsidian.write_today_decision(
        market_signal=signal, multiplier=multiplier,
        can_buy=can_buy, holding_count=len(positions),
        exposure_pct=0.0, reasons=reasons,
    )

    # 5. 盘前报告
    ctx.reporter.generate_morning_report(run_id)

    # 6. Obsidian
    ctx.obsidian.write_portfolio_status()

    # 盘前巡检信号摘要
    ctx.obsidian.write_signal_snapshot(
        run_id=run_id,
        market_state_detail=market_state.detail,
        market_signal=signal,
        decision={"action": decision_action},
    )

    log_lines = ["## 盘前摘要", "", f"大盘信号: **{signal}** (仓位系数 {multiplier})", ""]
    if positions:
        log_lines.append(f"持仓 {len(positions)} 只")
        for p in positions:
            sym = "HK$" if getattr(p, "currency", "CNY") == "HKD" else "¥"
            log_lines.append(f"- {p.name}({p.code}) {p.shares}股 成本{sym}{p.avg_cost:.2f}")
    else:
        log_lines.append("当前空仓")
    if risk_alerts:
        log_lines.extend(["", "### 风控预警"] + risk_alerts)
    if core_pool:
        log_lines.extend(["", "### 核心池"])
        for s in core_pool[:5]:
            emoji = "✅" if s["score"] >= 7 else ("🟡" if s["score"] >= 5 else "❌")
            scored_at = f"（{s['last_scored_at']}）" if s.get("last_scored_at") else ""
            log_lines.append(f"- {s['name']} {emoji} 上次评分 {s['score']:.1f}{scored_at}")

    hot_stocks = asyncio.run(ctx.market_svc.collect_hot_stocks(run_id=run_id))
    xueqiu_hot_stocks = asyncio.run(ctx.market_svc.collect_xueqiu_hot_stocks(run_id=run_id))
    cross_platform_hot_stocks = asyncio.run(ctx.market_svc.collect_cross_platform_hot_stocks(run_id=run_id))
    finance_flash = asyncio.run(ctx.market_svc.collect_finance_flash(limit=5, run_id=run_id))
    global_risk_news = asyncio.run(ctx.market_svc.collect_global_risk_news(limit=5, run_id=run_id))
    market_announcements = asyncio.run(ctx.market_svc.collect_market_announcements(limit=5, run_id=run_id))
    northbound = asyncio.run(ctx.market_svc.collect_northbound_realtime(run_id=run_id))
    signal_lines = format_market_signals_markdown(
        hot_stocks=hot_stocks,
        xueqiu_hot_stocks=xueqiu_hot_stocks,
        cross_platform_hot_stocks=cross_platform_hot_stocks,
        finance_flash=finance_flash,
        global_risk_news=global_risk_news,
        market_announcements=market_announcements,
        northbound=northbound,
    )
    if signal_lines:
        log_lines.extend([""] + signal_lines)

    # 行业热力图
    heatmap_sectors = asyncio.run(ctx.market_svc.collect_sector_heatmap(run_id=run_id))
    _logger.info(f"[morning] 行业热力图: {len(heatmap_sectors)} 个板块")
    if heatmap_sectors:
        log_lines.extend(["", "### 行业热力图"] + format_sector_heatmap_markdown(heatmap_sectors))
    else:
        log_lines.extend(["", "### 行业热力图", "数据获取失败"])

    ctx.obsidian.write_daily_log(run_id, "\n".join(log_lines))

    # 刷新每日巡检报告
    ctx.obsidian.write_daily_output_index(run_id)

    # 7. Discord embed
    discord_data = {
        "date": local_today_str(),
        "market_signal": signal,
        "market": market_state.detail.get("indices", {}),
        "positions": [{"name": p.name, "shares": p.shares, "price": p.current_price or p.avg_cost, "currency": getattr(p, "currency", "CNY")} for p in positions],
        "core_pool": core_pool[:5],
        "decision": {
            "action": decision_action,
            "multiplier": multiplier,
            "holding_count": len(positions),
            "risk_alerts": risk_alerts,
        },
        "stop_loss_reminders": stop_loss_reminders,
        "xueqiu_hot_stocks": xueqiu_hot_stocks[:5],
        "cross_platform_hot_stocks": cross_platform_hot_stocks[:5],
        "finance_flash": finance_flash[:5],
        "global_risk_news": global_risk_news[:5],
        "market_announcements": market_announcements[:5],
    }
    embed = format_morning_embed(discord_data)

    _logger.info(f"[morning] 完成: {len(positions)} 持仓, {len(core_pool)} 核心池, {len(risk_alerts)} 风控预警")

    # 8. Discord 推送
    sector_heatmap_pushed = False
    try:
        from astock_trading.reporting.discord import format_sector_heatmap_embed
        from astock_trading.reporting.discord_sender import send_embed
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[morning] Discord 推送失败: {err}")
        if should_push_sector_heatmap(heatmap_sectors, phase="morning"):
            heatmap_embed = format_sector_heatmap_embed(heatmap_sectors, title="盘前")
            ok2, err2 = send_embed(heatmap_embed)
            sector_heatmap_pushed = bool(ok2)
            if not ok2:
                _logger.warning(f"[morning] 热力图 Discord 推送失败: {err2}")
        else:
            _logger.info("[morning] 行业热力图无明显异动，跳过 Discord 单独推送")
    except Exception as e:
        _logger.warning(f"[morning] Discord 推送异常: {e}")

    return {
        "signal": signal, "multiplier": multiplier,
        "positions": len(positions), "core_pool": len(core_pool),
        "risk_alerts": risk_alerts, "discord_embed": embed,
        "hot_stocks": len(hot_stocks),
        "xueqiu_hot_stocks": len(xueqiu_hot_stocks),
        "cross_platform_hot_stocks": len(cross_platform_hot_stocks),
        "finance_flash": len(finance_flash),
        "global_risk_news": len(global_risk_news),
        "market_announcements": len(market_announcements),
        "sector_heatmap_pushed": sector_heatmap_pushed,
    }
