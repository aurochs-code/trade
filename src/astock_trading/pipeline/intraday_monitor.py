"""
pipeline/intraday_monitor.py — 盘中持仓风控轮巡

轻量级 pipeline：只刷新持仓行情、检查盘中风险、触发 Discord 告警。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from astock_trading.execution.models import Position
from astock_trading.pipeline.context import PipelineContext
from astock_trading.pipeline.helpers import _update_position_price
from astock_trading.platform.time import local_date_bounds_utc, local_now_str, local_today
from astock_trading.risk.models import ExitSignal
from astock_trading.risk.rules import get_risk_params
from astock_trading.strategy.models import Style

_logger = logging.getLogger(__name__)

DEFAULT_DAILY_LOSS_ALERT_PCT = 0.05
ALERT_EVENT_TYPE = "risk.intraday_alert"


def _risk_cfg(ctx: PipelineContext) -> dict:
    return ctx.cfg.get("risk", {})


def _monitor_cfg(ctx: PipelineContext) -> dict:
    return _risk_cfg(ctx).get("intraday_monitor", {})


def _daily_loss_threshold(ctx: PipelineContext) -> float:
    raw = _monitor_cfg(ctx).get("daily_loss_alert_pct", DEFAULT_DAILY_LOSS_ALERT_PCT)
    try:
        return abs(float(raw))
    except (TypeError, ValueError):
        return DEFAULT_DAILY_LOSS_ALERT_PCT


def _position_style(pos: Position) -> Style:
    return Style(pos.style) if pos.style in ("slow_bull", "momentum") else Style.UNKNOWN


def _entry_date(pos: Position) -> date:
    try:
        return date.fromisoformat(pos.entry_date) if pos.entry_date else local_today()
    except ValueError:
        return local_today()


def _alert_key(alert: dict) -> tuple[str, str]:
    return alert.get("code", ""), alert.get("signal_type", "")


def _already_alerted_today(ctx: PipelineContext, alert: dict) -> bool:
    since, until = local_date_bounds_utc()
    events = ctx.event_store.query(
        stream=f"risk:{alert['code']}",
        event_type=ALERT_EVENT_TYPE,
        since=since,
        until=until,
        limit=200,
    )
    key = _alert_key(alert)
    return any(_alert_key(e.get("payload", {})) == key for e in events)


def _record_alert(ctx: PipelineContext, run_id: str, alert: dict) -> None:
    ctx.event_store.append(
        stream=f"risk:{alert['code']}",
        stream_type="risk",
        event_type=ALERT_EVENT_TYPE,
        payload=alert,
        metadata={"run_id": run_id, "pipeline": "intraday_monitor"},
    )


def _signal_to_alert(pos: Position, signal: ExitSignal) -> dict:
    return {
        "code": pos.code,
        "name": pos.name,
        "signal_type": signal.signal_type,
        "trigger_price": signal.trigger_price,
        "current_price": signal.current_price,
        "description": signal.description,
        "urgency": signal.urgency,
    }


def _daily_loss_alert(pos: Position, current_price: float, change_pct: float, threshold: float) -> dict | None:
    if change_pct > -threshold * 100:
        return None
    return {
        "code": pos.code,
        "name": pos.name,
        "signal_type": "daily_loss",
        "trigger_price": round(current_price * (1 + threshold), 2),
        "current_price": current_price,
        "description": f"盘中单日跌幅 {change_pct:.2f}% >= {threshold:.0%}",
        "urgency": "immediate",
    }


def run(ctx: PipelineContext, run_id: str) -> dict:
    """执行盘中持仓风控轮巡。"""
    positions = ctx.exec_svc.get_positions()
    if not positions:
        _logger.info("[intraday_monitor] 当前空仓，跳过")
        return {"positions": 0, "alerts": [], "deduped": 0, "discord_embed": None}

    threshold = _daily_loss_threshold(ctx)
    stock_list = [{"code": p.code, "name": p.name} for p in positions]
    try:
        snapshots = asyncio.run(ctx.market_svc.collect_intraday_batch(stock_list, run_id))
        snapshots_by_code = {s.code: s for s in snapshots}
    except Exception:
        _logger.warning("[intraday_monitor] 行情获取失败（东财接口断开），使用缓存价格")
        snapshots_by_code = {}

    risk_cfg = _risk_cfg(ctx)
    alerts: list[dict] = []
    position_rows: list[dict] = []

    for pos in positions:
        snap = snapshots_by_code.get(pos.code)
        quote = snap.quote if snap else None
        technical = snap.technical if snap else None
        current_price = quote.close if quote and quote.close > 0 else (pos.current_price or pos.avg_cost)
        change_pct = quote.change_pct if quote else 0.0

        if quote and quote.close > 0:
            _update_position_price(ctx, pos.code, quote.close)

        position_rows.append({
            "code": pos.code,
            "name": pos.name,
            "shares": pos.shares,
            "price": current_price,
            "change_pct": change_pct,
        })

        daily_alert = _daily_loss_alert(pos, current_price, change_pct, threshold)
        if daily_alert:
            alerts.append(daily_alert)

        params = get_risk_params(_position_style(pos), risk_cfg)
        signals = ctx.risk_svc.assess_position(
            code=pos.code,
            avg_cost=pos.avg_cost,
            current_price=current_price,
            entry_date=_entry_date(pos),
            today=local_today(),
            highest_since_entry=pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost,
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            risk_params=params,
            run_id=run_id,
            ma20=technical.ma20 if technical else 0,
            ma60=technical.ma60 if technical else 0,
        )
        for signal in signals:
            if signal.signal_type == "ma_exit" or signal.urgency == "immediate":
                alerts.append(_signal_to_alert(pos, signal))

    new_alerts = []
    deduped = 0
    seen: set[tuple[str, str]] = set()
    for alert in alerts:
        key = _alert_key(alert)
        if key in seen or _already_alerted_today(ctx, alert):
            deduped += 1
            continue
        seen.add(key)
        _record_alert(ctx, run_id, alert)
        new_alerts.append(alert)

    ctx.conn.commit()

    embed = None
    if new_alerts:
        from astock_trading.reporting.discord import format_intraday_monitor_embed
        from astock_trading.reporting.discord_sender import send_embed

        embed = format_intraday_monitor_embed({
            "time": local_now_str(),
            "positions": position_rows,
            "alerts": new_alerts,
        })
        ok, err = send_embed(embed, content="⚠️ 盘中风控告警")
        if not ok:
            _logger.warning(f"[intraday_monitor] Discord 推送失败: {err}")

    _logger.info(
        "[intraday_monitor] 完成: %s 持仓, %s 新告警, %s 去重",
        len(positions), len(new_alerts), deduped,
    )
    return {
        "positions": len(positions),
        "alerts": new_alerts,
        "deduped": deduped,
        "discord_embed": embed,
    }
