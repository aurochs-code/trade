"""
pipeline/weekly.py — 周报

流程：
1. 统计本周交易（买入/卖出/盈亏）
2. 统计胜率和盈亏比
3. 收集交易明细 + 池子变动
4. 收集模拟盘统计
5. 生成周报 → report_artifacts
6. 写 Obsidian 周复盘（自动数据 + 手动填写区）
7. 格式化 Discord embed
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from astock_trading.pipeline.context import PipelineContext
from astock_trading.platform.time import iso_to_local, local_date_bounds_utc, local_now

_logger = logging.getLogger(__name__)


def _query_filled_orders_this_week(conn, week_start_utc: str, week_after_utc: str):
    """从 projection_orders 表查询本周成交订单（兼容有无 event_store 两种情况）。"""
    rows = conn.execute(
        """
        SELECT order_id, code, side, shares, price_cents, filled_at
        FROM projection_orders
        WHERE status = 'filled'
          AND filled_at >= ?
          AND filled_at < ?
        ORDER BY filled_at ASC
        """,
        (week_start_utc, week_after_utc),
    ).fetchall()
    return [dict(r) for r in rows]


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行周报 pipeline。"""

    # 1. 本周时间范围
    now = local_now()
    week_start_date = now.date() - timedelta(days=now.weekday())
    week_end_date = week_start_date + timedelta(days=6)
    week_start_display = week_start_date.strftime("%m/%d")
    week_end_display = week_end_date.strftime("%m/%d")

    week_start_utc, _ = local_date_bounds_utc(week_start_date)
    week_after_utc, _ = local_date_bounds_utc(week_end_date + timedelta(days=1))

    # 2. 实盘交易统计（从 projection_orders 直接查，绕过 event_store 依赖）
    week_orders = _query_filled_orders_this_week(ctx.conn, week_start_utc, week_after_utc)

    # 平仓盈亏需从 event_store 的 position.closed 事件获取，event_store 不可用时置0
    closed_pnl_by_code: dict[str, list[int]] = {}
    try:
        closed_events = ctx.event_store.query(event_type="position.closed", since=week_start_utc)
        for e in closed_events:
            p = e["payload"]
            code = p.get("code", "")
            closed_pnl_by_code.setdefault(code, []).append(p.get("realized_pnl_cents", 0))
    except Exception:
        pass  # event_store 不存在时，平仓盈亏无法计算

    buy_count = sum(1 for o in week_orders if o["side"] == "buy")
    sell_count = sum(1 for o in week_orders if o["side"] == "sell")

    # 3. 胜率和盈亏比（仅统计有平仓盈亏记录的卖出）
    wins = 0
    losses = 0
    total_profit = 0
    total_loss = 0

    for pnls in closed_pnl_by_code.values():
        for pnl in pnls:
            if pnl > 0:
                wins += 1
                total_profit += pnl
            elif pnl < 0:
                losses += 1
                total_loss += abs(pnl)

    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0
    profit_loss_ratio = (total_profit / total_loss) if total_loss > 0 else float("inf") if total_profit > 0 else 0
    net_pnl_cents = total_profit - total_loss

    # 4. 交易明细
    trades = []
    remaining_closed_pnl_by_code = {
        code: list(pnls) for code, pnls in closed_pnl_by_code.items()
    }
    for o in week_orders:
        pnl = 0
        if o["side"] == "sell":
            pnls = remaining_closed_pnl_by_code.get(o["code"], [])
            pnl = pnls.pop(0) if pnls else 0
        trades.append({
            "date": o["filled_at"][:10],
            "code": o["code"],
            "name": "",
            "side": o["side"],
            "price": o["price_cents"] / 100,
            "shares": o["shares"],
            "pnl_cents": pnl,
            "note": "",
        })

    # 5. 池子变动（event_store 不可用时静默返回空）
    pool_changes = []
    try:
        pool_demoted = ctx.event_store.query(event_type="pool.demoted", since=week_start_utc)
        pool_removed = ctx.event_store.query(event_type="pool.removed", since=week_start_utc)
        for e in pool_demoted:
            p = e["payload"]
            pool_changes.append({
                "code": p.get("code", ""), "name": p.get("name", ""),
                "change_type": "demoted", "reason": f"降级: {p.get('reason', '')}",
            })
        for e in pool_removed:
            p = e["payload"]
            pool_changes.append({
                "code": p.get("code", ""), "name": p.get("name", ""),
                "change_type": "removed", "reason": f"移出: 评分 {p.get('score', 0)}",
            })
    except Exception:
        pass

    # 6. 当前持仓 + 核心池
    positions = ctx.exec_svc.get_positions()
    pos_data = [{"code": p.code, "name": p.name, "shares": p.shares,
                 "avg_cost": p.avg_cost, "style": p.style} for p in positions]

    pool_rows = ctx.conn.execute(
        "SELECT code, name, score FROM projection_candidate_pool "
        "WHERE pool_tier = 'core' ORDER BY score DESC"
    ).fetchall()
    core_pool = [{"code": r["code"], "name": r["name"] or "", "score": r["score"] or 0}
                 for r in pool_rows]

    # 7. 模拟盘统计（event_store 不可用时静默返回 None）
    paper_stats = None
    try:
        paper_events = ctx.event_store.query(
            event_type="auto_trade.executed",
            since=week_start_utc,
            until=week_after_utc,
        )
        paper_stats = _paper_stats_from_events(paper_events)
    except Exception:
        pass

    # 8. 周报
    week_str = now.strftime("%Y-W%W")
    ctx.reporter.generate_weekly_report(week_str)

    week_stats = {
        "week_str": week_str,
        "week_start": week_start_display,
        "week_end": week_end_display,
        "week_start_utc": week_start_utc,
        "week_after_utc": week_after_utc,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "profit_loss_ratio": profit_loss_ratio,
        "net_pnl_cents": net_pnl_cents,
        "total_profit_cents": total_profit,
        "total_loss_cents": total_loss,
        "trades": trades,
        "positions": pos_data,
        "core_pool": core_pool,
        "pool_changes": pool_changes,
        "paper_stats": paper_stats,
    }
    _record_weekly_performance_snapshot(ctx, run_id, week_stats)

    # 9. Obsidian 周复盘
    ctx.obsidian.write_weekly_review(week_stats)

    # 日志追加
    ctx.obsidian.write_daily_log(
        run_id,
        f"## 周报生成\n\n{week_str} 周报已生成。"
        f"{buy_count}买 {sell_count}卖 净盈亏¥{net_pnl_cents/100:+,.0f}",
    )

    _logger.info(
        f"[weekly] 完成: {buy_count}买 {sell_count}卖 "
        f"胜率{win_rate:.0%} 净盈亏¥{net_pnl_cents/100:+,.0f}"
    )

    # 10. Discord 推送
    try:
        from astock_trading.reporting.discord import format_weekly_embed
        from astock_trading.reporting.discord_sender import send_embed
        embed = format_weekly_embed({
            "week": week_str,
            "buy_count": buy_count, "sell_count": sell_count,
            "win_rate": win_rate, "profit_loss_ratio": profit_loss_ratio,
            "net_pnl_cents": net_pnl_cents,
            "paper_stats": paper_stats,
            "positions": [{"name": p.name, "code": p.code, "shares": p.shares}
                          for p in positions],
        })
        ok, err = send_embed(embed)
        if not ok:
            _logger.warning(f"[weekly] Discord 推送失败: {err}")
    except Exception as e:
        _logger.warning(f"[weekly] Discord 推送异常: {e}")

    # 11. 月末自动生成月复盘
    _maybe_generate_monthly(ctx, run_id, now)

    return {
        "week": week_str,
        "buy_count": buy_count, "sell_count": sell_count,
        "win_rate": win_rate, "profit_loss_ratio": round(profit_loss_ratio, 2),
        "net_pnl_cents": net_pnl_cents,
        "paper_stats": paper_stats,
    }


def _record_weekly_performance_snapshot(ctx: PipelineContext, run_id: str, week_stats: dict) -> str:
    return ctx.event_store.append(
        stream="performance:weekly",
        stream_type="performance",
        event_type="performance.weekly_snapshot",
        payload=week_stats,
        metadata={"run_id": run_id},
    )


def _maybe_generate_monthly(ctx: PipelineContext, run_id: str, now: datetime):
    """如果是月末最后一周，自动生成月复盘。"""
    next_week = now + timedelta(days=7)
    if next_week.month != now.month:
        # 本周是本月最后一周，生成月复盘
        _generate_monthly_review(ctx, run_id, now)


def _generate_monthly_review(ctx: PipelineContext, run_id: str, now: datetime):
    """生成月复盘。"""
    month_str = now.strftime("%Y-%m")
    month_start = now.replace(day=1).strftime("%Y-%m-%d")
    month_start_utc, _ = local_date_bounds_utc(month_start)

    # 实盘统计
    filled_events = ctx.event_store.query(event_type="order.filled", since=month_start_utc)
    closed_events = ctx.event_store.query(event_type="position.closed", since=month_start_utc)

    buy_count = sum(1 for e in filled_events if e["payload"].get("side") == "buy")
    sell_count = sum(1 for e in filled_events if e["payload"].get("side") == "sell")

    wins = 0
    losses = 0
    total_profit = 0
    total_loss = 0
    worst_trades = []

    for e in closed_events:
        pnl = e["payload"].get("realized_pnl_cents", 0)
        if pnl > 0:
            wins += 1
            total_profit += pnl
        elif pnl < 0:
            losses += 1
            total_loss += abs(pnl)
            worst_trades.append({
                "code": e["payload"].get("code", ""),
                "name": e["payload"].get("name", ""),
                "pnl_cents": pnl,
                "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
            })

    worst_trades.sort(key=lambda x: x["pnl_cents"])  # 最亏的排前面

    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0
    plr = (total_profit / total_loss) if total_loss > 0 else (
        float("inf") if total_profit > 0 else 0
    )
    net_pnl_cents = total_profit - total_loss
    avg_profit = total_profit // wins if wins > 0 else 0
    avg_loss = total_loss // losses if losses > 0 else 0

    # 周度汇总（按 ISO 周分组）
    weekly_map: dict[str, dict] = {}
    for e in filled_events:
        try:
            d = iso_to_local(e["occurred_at"])
            wk = d.strftime("%Y-W%W")
        except Exception:
            continue
        if wk not in weekly_map:
            weekly_map[wk] = {"week": wk, "pnl_cents": 0, "buy_count": 0,
                              "sell_count": 0, "wins": 0, "losses": 0}
        side = e["payload"].get("side", "")
        if side == "buy":
            weekly_map[wk]["buy_count"] += 1
        elif side == "sell":
            weekly_map[wk]["sell_count"] += 1

    for e in closed_events:
        try:
            d = iso_to_local(e["occurred_at"])
            wk = d.strftime("%Y-W%W")
        except Exception:
            continue
        if wk not in weekly_map:
            weekly_map[wk] = {"week": wk, "pnl_cents": 0, "buy_count": 0,
                              "sell_count": 0, "wins": 0, "losses": 0}
        pnl = e["payload"].get("realized_pnl_cents", 0)
        weekly_map[wk]["pnl_cents"] += pnl
        if pnl > 0:
            weekly_map[wk]["wins"] += 1
        elif pnl < 0:
            weekly_map[wk]["losses"] += 1

    weekly_summaries = sorted(weekly_map.values(), key=lambda x: x["week"])

    # 池子变动
    pool_demoted = ctx.event_store.query(event_type="pool.demoted", since=month_start_utc)
    pool_removed = ctx.event_store.query(event_type="pool.removed", since=month_start_utc)
    pool_changes = []
    for e in pool_demoted:
        p = e["payload"]
        pool_changes.append({
            "code": p.get("code", ""), "name": p.get("name", ""),
            "change_type": "demoted",
            "reason": f"降级: {p.get('reason', '')}",
            "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
        })
    for e in pool_removed:
        p = e["payload"]
        pool_changes.append({
            "code": p.get("code", ""), "name": p.get("name", ""),
            "change_type": "removed",
            "reason": f"移出: 评分 {p.get('score', 0)}",
            "date": iso_to_local(e.get("occurred_at", "")).date().isoformat(),
        })

    # 模拟盘统计
    paper_events = ctx.event_store.query(event_type="auto_trade.executed", since=month_start_utc)
    paper_stats = _paper_stats_from_events(paper_events)

    # 风控参数
    cfg = ctx.cfg
    risk_cfg = cfg.get("risk", {})
    pos_cfg = risk_cfg.get("position", {})
    momentum_cfg = risk_cfg.get("momentum", {})
    risk_params = {
        "stop_loss": f"{momentum_cfg.get('stop_loss', 0.08):.0%}",
        "trailing_stop": f"{momentum_cfg.get('trailing_stop', 0.10):.0%}",
        "time_stop_days": momentum_cfg.get("time_stop_days", 15),
        "weekly_max": pos_cfg.get("weekly_max", 2),
        "total_max": f"{pos_cfg.get('total_max', 0.60):.0%}",
        "single_max": f"{pos_cfg.get('single_max', 0.20):.0%}",
    }

    # 估算交易日数（工作日）
    month_start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    trading_days = sum(
        1 for i in range((now - month_start_dt).days + 1)
        if (month_start_dt + timedelta(days=i)).weekday() < 5
    )

    try:
        ctx.obsidian.write_monthly_review({
            "month_str": month_str,
            "trading_days": trading_days,
            "buy_count": buy_count,
            "sell_count": sell_count,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "profit_loss_ratio": plr,
            "net_pnl_cents": net_pnl_cents,
            "total_profit_cents": total_profit,
            "total_loss_cents": total_loss,
            "max_drawdown_cents": 0,  # TODO: 从每日快照计算
            "avg_profit_cents": avg_profit,
            "avg_loss_cents": avg_loss,
            "weekly_summaries": weekly_summaries,
            "worst_trades": worst_trades,
            "pool_changes": pool_changes,
            "paper_stats": paper_stats,
            "risk_params": risk_params,
        })
        _logger.info(f"[weekly] 月复盘已生成: {month_str}")
    except Exception as e:
        _logger.warning(f"[weekly] 月复盘生成失败: {e}")


def _paper_stats_from_events(events: list[dict]) -> dict | None:
    paper_events = [
        event for event in events
        if event.get("metadata", {}).get("account") == "paper"
        and event.get("payload", {}).get("side") in {"buy", "sell"}
        and event.get("payload", {}).get("status") in {"filled", "dry_run"}
    ]
    if not paper_events:
        return None

    paper_events = sorted(paper_events, key=lambda event: event.get("occurred_at", ""))
    route_by_code: dict[str, str] = {}
    by_route: dict[str, dict] = {}

    def route_bucket(route: str) -> dict:
        return by_route.setdefault(route, {
            "route": route,
            "buy_count": 0,
            "sell_count": 0,
            "net_pnl_cents": 0,
            "win_count": 0,
            "loss_count": 0,
            "win_rate": 0.0,
            "pnl_data_quality": "ok",
        })

    buy_count = sum(1 for event in paper_events if event.get("payload", {}).get("side") == "buy")
    sell_events = [event for event in paper_events if event.get("payload", {}).get("side") == "sell"]
    realized_values = [
        int(event.get("payload", {}).get("realized_pnl_cents") or 0)
        for event in sell_events
        if "realized_pnl_cents" in event.get("payload", {})
    ]
    for event in paper_events:
        payload = event.get("payload", {}) or {}
        side = payload.get("side")
        code = str(payload.get("code") or "")
        route = _paper_event_route(payload)
        if side == "buy":
            if code:
                route_by_code[code] = route
            route_bucket(route)["buy_count"] += 1
            continue

        if side != "sell":
            continue
        if route == "未知路线" and code:
            route = route_by_code.get(code, route)
        bucket = route_bucket(route)
        bucket["sell_count"] += 1
        if "realized_pnl_cents" not in payload:
            bucket["pnl_data_quality"] = "missing_realized_pnl"
            continue
        pnl = int(payload.get("realized_pnl_cents") or 0)
        bucket["net_pnl_cents"] += pnl
        if pnl > 0:
            bucket["win_count"] += 1
        elif pnl < 0:
            bucket["loss_count"] += 1

    route_rows = []
    for bucket in by_route.values():
        closed = bucket["win_count"] + bucket["loss_count"]
        bucket["win_rate"] = bucket["win_count"] / closed if closed else 0.0
        route_rows.append(bucket)
    route_rows.sort(
        key=lambda item: (
            item["net_pnl_cents"],
            item["sell_count"],
            item["buy_count"],
        ),
        reverse=True,
    )

    return {
        "buy_count": buy_count,
        "sell_count": len(sell_events),
        "net_pnl_cents": sum(realized_values),
        "by_route": route_rows,
        "pnl_data_quality": (
            "ok" if len(realized_values) == len(sell_events) else "missing_realized_pnl"
        ),
    }


def _paper_event_route(payload: dict) -> str:
    label = payload.get("primary_strategy_route_label")
    if label:
        return str(label)
    route = str(payload.get("primary_strategy_route") or "")
    if not route:
        return "未知路线"
    return _ROUTE_LABELS.get(route, route)


_ROUTE_LABELS = {
    "flow_confirmed_trend": "资金趋势确认",
    "volume_breakout": "放量突破",
    "pullback_to_ma20": "均线回踩转强",
    "short_continuation": "短续接力",
    "ma_golden_cross": "均线金叉",
    "dragon_head": "龙头策略",
    "shrink_pullback": "缩量回踩",
    "relative_strength_overheat": "强势过热观察",
    "trend_cooling_off": "趋势冷却观察",
    "trend_watch": "趋势观察",
}
