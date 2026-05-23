"""
strategy/timer.py — 大盘择时（纯函数）

不做任何 IO。输入指数数据 dict，输出 MarketState。
数据获取由 market context 负责，这里只做信号计算。
"""

from __future__ import annotations

import logging
from typing import Optional

from astock_trading.strategy.models import MarketSignal, MarketState

_logger = logging.getLogger(__name__)

# 模块级缓存：上一次有效的大盘信号（provider 全挂时 fallback）
_last_valid_state: Optional[MarketState] = None


def compute_market_signal(
    index_data: dict[str, dict],
    config: Optional[dict] = None,
) -> MarketState:
    """
    纯函数：根据指数数据计算大盘信号。

    当所有 provider 都失败（total == 0）时，fallback 到上一次有效信号，
    而不是直接返回 CLEAR（避免误触发清仓）。

    Args:
        index_data: {
            "上证指数": {"above_ma20": bool, "below_ma60_days": int, "change_pct": float, ...},
            "深证成指": {...},
            "创业板指": {...},
        }
        config: market_timer 配置段（可选）

    Returns:
        MarketState(signal, multiplier, detail)
    """
    global _last_valid_state

    config = config or {}
    clear_days = config.get("clear_days_ma60", 15)

    green_count = 0
    red_count = 0
    clear_count = 0
    fallback_intraday_count = 0
    total = 0

    for name, data in index_data.items():
        if "error" in data:
            continue

        above_ma20 = data.get("above_ma20", False)
        below_ma60_days = data.get("below_ma60_days", 0)
        ma20_missing = "ma20" in data and not _positive_number(data.get("ma20"))

        if isinstance(above_ma20, bool) and not ma20_missing:
            total += 1
            if above_ma20:
                green_count += 1
            else:
                red_count += 1
        else:
            if "price" in data and not _positive_number(data.get("price")):
                continue
            change_pct = _to_float(data.get("change_pct"))
            if change_pct is None:
                continue
            total += 1
            fallback_intraday_count += 1
            if change_pct >= 0:
                green_count += 1
            else:
                red_count += 1

        if below_ma60_days >= clear_days:
            clear_count += 1

    if total == 0:
        # Fallback 到上一次有效信号，避免 provider 临时故障误触发 CLEAR
        if _last_valid_state is not None:
            _logger.warning("[timer] 无有效指数数据，沿用上次信号: %s", _last_valid_state.signal.value)
            return MarketState(
                signal=_last_valid_state.signal,
                multiplier=_last_valid_state.multiplier,
                detail={
                    "reason": "无有效指数数据，沿用上次信号",
                    "fallback_signal": _last_valid_state.signal.value,
                    "indices": index_data,
                },
            )
        # 首次运行且无数据 → 保守返回 RED（禁止新开仓但不触发清仓）
        _logger.warning("[timer] 无有效指数数据且无历史信号，返回 RED")
        return MarketState(
            signal=MarketSignal.RED,
            multiplier=0.0,
            detail={"reason": "无有效指数数据且无历史信号", "indices": index_data},
        )

    green_pct = green_count / total
    clear_pct = clear_count / total

    # 优先级：CLEAR > RED > YELLOW > GREEN
    if clear_pct >= 0.6:
        signal = MarketSignal.CLEAR
    elif green_pct >= 0.6:
        signal = MarketSignal.GREEN
    elif green_pct >= 0.3:
        signal = MarketSignal.YELLOW
    else:
        signal = MarketSignal.RED

    reason = None
    if fallback_intraday_count and fallback_intraday_count == total and signal == MarketSignal.GREEN:
        signal = MarketSignal.YELLOW
        reason = "MA 数据缺失，使用日内涨跌幅保守判断"

    multiplier = _signal_to_multiplier(signal)

    state = MarketState(
        signal=signal,
        multiplier=multiplier,
        detail={
            **({"reason": reason} if reason else {}),
            "green_count": green_count,
            "red_count": red_count,
            "clear_count": clear_count,
            "fallback_intraday_count": fallback_intraday_count,
            "total": total,
            "green_pct": round(green_pct, 2),
            "clear_pct": round(clear_pct, 2),
            "indices": index_data,
        },
    )

    # 缓存有效信号
    _last_valid_state = state
    return state


def _signal_to_multiplier(signal: MarketSignal) -> float:
    return {
        MarketSignal.GREEN: 1.0,
        MarketSignal.YELLOW: 0.5,
        MarketSignal.RED: 0.0,
        MarketSignal.CLEAR: 0.0,
    }.get(signal, 0.0)


def _positive_number(value: object) -> bool:
    number = _to_float(value)
    return number is not None and number > 0


def _to_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
