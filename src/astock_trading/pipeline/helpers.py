"""
pipeline/helpers.py — Pipeline 共享工具函数

提供持仓风控检查等跨 pipeline 复用的逻辑。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from astock_trading.pipeline.context import PipelineContext
from astock_trading.execution.models import Position
from astock_trading.platform.time import local_date_bounds_utc, local_now, local_today, utc_now_iso
from astock_trading.risk.models import ExitSignal
from astock_trading.risk.rules import get_risk_params
from astock_trading.strategy.models import Style

_logger = logging.getLogger(__name__)


def _get_risk_cfg(ctx: PipelineContext) -> dict:
    """从配置中读取风控参数段。"""
    return ctx.cfg.get("risk", {})


def check_position_risks(
    ctx: PipelineContext,
    positions: list[Position],
    run_id: str,
) -> list[tuple[Position, list[ExitSignal]]]:
    """
    对持仓列表做风控检查，自动获取 MA 数据。

    Returns:
        [(position, [ExitSignal, ...]), ...]
    """
    if not positions:
        return []

    risk_cfg = _get_risk_cfg(ctx)

    # 批量获取持仓的技术指标（MA20/MA60）
    stock_list = [{"code": p.code, "name": p.name} for p in positions]
    try:
        snapshots = asyncio.run(ctx.market_svc.collect_batch(stock_list, run_id))
        ma_data = {}
        for snap in snapshots:
            if snap.technical:
                ma_data[snap.code] = {
                    "ma20": snap.technical.ma20,
                    "ma60": snap.technical.ma60,
                }
            # 同时更新持仓的 current_price 和 highest_since_entry
            if snap.quote and snap.quote.close > 0:
                _update_position_price(ctx, snap.code, snap.quote.close)
    except Exception as e:
        _logger.warning(f"[helpers] 批量获取 MA 数据失败: {e}")
        ma_data = {}

    results = []
    for pos in positions:
        style = Style(pos.style) if pos.style in ("slow_bull", "momentum") else Style.UNKNOWN
        params = get_risk_params(style, risk_cfg)
        try:
            entry_date = date.fromisoformat(pos.entry_date) if pos.entry_date else local_today()
        except ValueError:
            entry_date = local_today()

        ma_info = ma_data.get(pos.code, {})

        signals = ctx.risk_svc.assess_position(
            code=pos.code,
            avg_cost=pos.avg_cost,
            current_price=pos.current_price or pos.avg_cost,
            entry_date=entry_date,
            today=local_today(),
            highest_since_entry=pos.highest_since_entry_cents / 100 if pos.highest_since_entry_cents else pos.avg_cost,
            entry_day_low=pos.entry_day_low_cents / 100 if pos.entry_day_low_cents else pos.avg_cost,
            risk_params=params,
            run_id=run_id,
            ma20=ma_info.get("ma20", 0),
            ma60=ma_info.get("ma60", 0),
        )
        results.append((pos, signals))

    return results


def _update_position_price(ctx: PipelineContext, code: str, price: float) -> bool:
    """更新持仓的 current_price 和 highest_since_entry。"""
    try:
        price_cents = int(round(price * 100))
        row = ctx.conn.execute(
            """SELECT highest_since_entry_cents, current_price_cents,
                      avg_cost_cents, cost_basis_cents, shares
               FROM projection_positions
               WHERE code = ?""",
            (code,),
        ).fetchone()
        if not row:
            return False
        old_highest = row["highest_since_entry_cents"] or 0
        new_highest = max(old_highest, price_cents)
        cost_basis_cents = row["cost_basis_cents"] if "cost_basis_cents" in row.keys() else None
        if not cost_basis_cents:
            cost_basis_cents = row["avg_cost_cents"] * row["shares"]
        unrealized = price_cents * row["shares"] - cost_basis_cents

        ctx.conn.execute(
            """UPDATE projection_positions
               SET current_price_cents = ?,
                   highest_since_entry_cents = ?,
                   unrealized_pnl_cents = ?,
                   updated_at = ?
               WHERE code = ?""",
            (price_cents, new_highest, unrealized, utc_now_iso(), code),
        )
        return True
    except Exception as e:
        _logger.warning(f"[helpers] 更新持仓价格失败 {code}: {e}")
        return False


def _snapshot_quote_price(snapshot) -> float | None:
    quote = getattr(snapshot, "quote", None)
    if quote is None:
        return None
    for attr in ("close", "price"):
        try:
            value = float(getattr(quote, attr, 0) or 0)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    return None


def _apply_position_price_snapshots(
    ctx: PipelineContext,
    snapshots,
    refreshed: dict[str, float],
) -> None:
    for snap in snapshots:
        price = _snapshot_quote_price(snap)
        if price is None:
            continue
        if _update_position_price(ctx, snap.code, price):
            refreshed[snap.code] = price


def refresh_position_prices(ctx: PipelineContext, run_id: str | None = None) -> dict[str, float]:
    """
    刷新所有持仓的实时价格，写入 projection_positions。

    优先走持仓专用轻量行情链路；缺失时再降级到完整单股快照。
    返回刷新后的价格字典 {code: price}。
    """
    positions = ctx.exec_svc.get_positions()
    if not positions:
        return {}

    refreshed = {}
    stock_list = [{"code": pos.code, "name": pos.name} for pos in positions]

    try:
        snapshots = asyncio.run(ctx.market_svc.collect_intraday_batch(stock_list, run_id))
        _apply_position_price_snapshots(ctx, snapshots, refreshed)
    except AttributeError:
        _logger.info("[helpers] market_svc 不支持轻量持仓行情，降级到完整快照")
    except Exception as e:
        _logger.warning(f"[helpers] 轻量刷新持仓价格失败: {e}")

    missing = [item for item in stock_list if item["code"] not in refreshed]
    if missing:
        try:
            snapshots = asyncio.run(ctx.market_svc.collect_batch(missing, run_id))
            _apply_position_price_snapshots(ctx, snapshots, refreshed)
        except Exception as e:
            _logger.warning(f"[helpers] 批量刷新持仓价格失败: {e}")

    ctx.conn.commit()
    _logger.info(f"[helpers] 刷新持仓价格: {len(refreshed)} 只")
    return refreshed


def get_current_exposure(ctx: PipelineContext) -> tuple[float, int]:
    """
    计算当前仓位占比和本周买入次数。

    Returns:
        (current_exposure_pct, weekly_buy_count)
    """
    positions = ctx.exec_svc.get_positions()
    capital = ctx.capital

    total_market = sum(
        (p.current_price_cents or p.avg_cost_cents) * p.shares
        for p in positions
    )
    exposure_pct = total_market / (capital * 100) if capital > 0 else 0.0

    # 本周买入次数：查 event_log
    today = local_now()
    monday = today.date() - timedelta(days=today.weekday())
    since, _ = local_date_bounds_utc(monday)

    buy_count = ctx.event_store.count(
        event_type="position.opened",
        since=since,
    )

    return exposure_pct, buy_count
