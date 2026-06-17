"""
backtest/engine.py — 生产级回测引擎

接入真实的 Scorer（四维评分）和 Decider（综合决策），与实盘共用同一套信号逻辑。

数据流：
  baostock K线
      ↓
  TechnicalIndicators（从 K 线实时计算）
      ↓
  StockSnapshot → Scorer.score() → ScoreResult
      ↓
  Decider.decide() → DecisionIntent
      ↓
  SimulatedBroker.submit_order()（按 A 股 T+1、涨跌停和成本模型撮合）
      ↓
  持仓管理 + 风控检查
      ↓
  绩效报告
"""

from __future__ import annotations

import asyncio
import io
import math
import signal
import sys
import time
from contextlib import redirect_stdout
from dataclasses import dataclass, field
from datetime import date as date_type, timedelta
from pathlib import Path
from typing import Any, TYPE_CHECKING, Optional

import pandas as pd
import yaml

from astock_trading.backtest.signal_analysis import market_phase_bucket, signal_alpha_summary
from astock_trading.strategy.route_policy import iter_route_policy_entries, route_policy_values

if TYPE_CHECKING:
    from astock_trading.strategy.models import MarketState


# ---------------------------------------------------------------------------
# Indicator 计算（纯函数）
# ---------------------------------------------------------------------------

def _date_ranges(
    start_date: str,
    end_date: str,
    *,
    months_per_batch: int = 6,
) -> list[tuple[str, str]]:
    """Split an inclusive date range into month-sized chunks that reach end_date."""
    start = date_type.fromisoformat(start_date)
    end = date_type.fromisoformat(end_date)
    if start >= end:
        return [(start.isoformat(), end.isoformat())]

    ranges: list[tuple[str, str]] = []
    current = start
    while current < end:
        next_date = min(_add_months(current, months_per_batch), end)
        ranges.append((current.isoformat(), next_date.isoformat()))
        current = next_date
    return ranges


def _add_months(value: date_type, months: int) -> date_type:
    month_index = value.month - 1 + max(int(months), 1)
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, _days_in_month(year, month))
    return date_type(year, month, day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        return 31
    first_next = date_type(year + (month // 12), (month % 12) + 1, 1)
    return (first_next - date_type(year, month, 1)).days


def _cache_covers_range(df: pd.DataFrame, pre_start: str, end_date: str) -> bool:
    if df.empty or "日期" not in df.columns:
        return False
    dates = pd.to_datetime(df["日期"], errors="coerce").dropna()
    if dates.empty:
        return False
    start_target = pd.Timestamp(pre_start) + pd.Timedelta(days=10)
    end_target = pd.Timestamp(end_date) - pd.Timedelta(days=10)
    return dates.min() <= start_target and dates.max() >= end_target


def _cache_covers_backtest_range(df: pd.DataFrame, start_date: str, end_date: str) -> bool:
    """判断缓存是否覆盖正式回测区间；不要求覆盖 MA warmup 的前置窗口。"""
    return _cache_covers_range(df, start_date, end_date)


def _cache_covers_end_date(df: pd.DataFrame, end_date: str) -> bool:
    """判断缓存尾部是否覆盖回测结束；允许股票在区间中途上市。"""
    if df.empty or "日期" not in df.columns:
        return False
    dates = pd.to_datetime(df["日期"], errors="coerce").dropna()
    if dates.empty:
        return False
    end_target = pd.Timestamp(end_date) - pd.Timedelta(days=10)
    return dates.max() >= end_target


def _missing_ranges_for_cache(
    cached: pd.DataFrame | None,
    pre_start: str,
    end_date: str,
) -> list[tuple[str, str]]:
    if cached is None or cached.empty or "日期" not in cached.columns:
        return _date_ranges(pre_start, end_date, months_per_batch=6)
    dates = pd.to_datetime(cached["日期"], errors="coerce").dropna()
    if dates.empty:
        return _date_ranges(pre_start, end_date, months_per_batch=6)
    missing: list[tuple[str, str]] = []
    min_date = dates.min().date()
    max_date = dates.max().date()
    start = date_type.fromisoformat(pre_start)
    end = date_type.fromisoformat(end_date)
    if min_date > start:
        missing.extend(_date_ranges(start.isoformat(), (min_date - timedelta(days=1)).isoformat()))
    if max_date < end:
        missing.extend(_date_ranges((max_date + timedelta(days=1)).isoformat(), end.isoformat()))
    return missing


def _financial_periods(start_date: str, end_date: str) -> list[tuple[int, int]]:
    start_year = date_type.fromisoformat(start_date).year - 1
    end_year = date_type.fromisoformat(end_date).year
    periods = []
    for year in range(start_year, end_year + 1):
        for quarter in range(1, 5):
            if _available_date(year, quarter) <= end_date:
                periods.append((year, quarter))
    return periods


def _score_weights_for_mode(weights: dict[str, float], mode: str) -> dict[str, float]:
    """Return scoring weights for a backtest dimension-ablation mode."""
    normalized = (mode or "full").strip().lower().replace("-", "_")
    base = {
        "technical": float(weights.get("technical", 0.0) or 0.0),
        "fundamental": float(weights.get("fundamental", 0.0) or 0.0),
        "flow": float(weights.get("flow", 0.0) or 0.0),
        "sentiment": float(weights.get("sentiment", 0.0) or 0.0),
    }
    if normalized in {"full", "all"}:
        return base
    if normalized not in {"tech_fundamental", "technical_fundamental"}:
        return base

    included = ("technical", "fundamental")
    included_sum = sum(base[key] for key in included)
    total_budget = sum(base.values())
    if included_sum <= 0 or total_budget <= 0:
        return {key: 0.0 for key in base}
    scale = total_budget / included_sum
    return {
        "technical": round(base["technical"] * scale, 4),
        "fundamental": round(base["fundamental"] * scale, 4),
        "flow": 0.0,
        "sentiment": 0.0,
    }


def _report_date(year: int, quarter: int) -> str:
    month_day = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}[int(quarter)]
    return f"{int(year)}-{month_day}"


def _available_date(year: int, quarter: int) -> str:
    # 用常见法定披露截止日近似可用日，避免回测提前读取未披露财报。
    quarter = int(quarter)
    if quarter == 1:
        return f"{int(year)}-04-30"
    if quarter == 2:
        return f"{int(year)}-08-31"
    if quarter == 3:
        return f"{int(year)}-10-31"
    return f"{int(year) + 1}-04-30"


def _call_with_timeout(fn, timeout_seconds: float):
    if timeout_seconds <= 0 or not hasattr(signal, "setitimer"):
        return fn()
    previous_handler = signal.getsignal(signal.SIGALRM)

    def _handle_timeout(signum, frame):  # noqa: ARG001
        raise TimeoutError(f"baostock 财务查询超过 {timeout_seconds:.0f}s")

    signal.signal(signal.SIGALRM, _handle_timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        return fn()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _signal_technical_snapshot(score: Any) -> dict[str, Any]:
    for dim in getattr(score, "dimensions", []) or []:
        if str(getattr(dim, "name", "") or "") != "technical":
            continue
        raw = dict(getattr(dim, "raw_data", {}) or {})
        keys = (
            "above_ma20",
            "golden_cross",
            "volume_ratio",
            "rsi",
            "ma20_slope",
            "momentum_5d",
            "deviation_rate",
            "change_pct",
        )
        return {key: _json_safe_value(raw.get(key)) for key in keys if key in raw}
    return {}


def _route_diagnostics_payload(score: Any) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for item in getattr(score, "route_diagnostics", []) or []:
        if hasattr(item, "to_dict"):
            payload.append(_json_safe_value(item.to_dict()))
        elif isinstance(item, dict):
            payload.append(_json_safe_value(dict(item)))
    return payload


def _unknown_signal_bucket(
    *,
    route: str,
    entry_signal: bool,
    action: str,
    technical: dict[str, Any],
    diagnostics: list[dict[str, Any]],
) -> str:
    if route:
        return ""
    best_diag = _best_route_diagnostic(diagnostics)
    if best_diag and float(best_diag.get("route_score", 0.0) or 0.0) >= 0.6:
        return f"near_{best_diag.get('route', 'route')}"
    if _near_pullback_missing_confirm(technical):
        return "near_pullback_missing_confirm"
    if _overheated_trend(technical):
        return "overheated_trend_no_entry"
    if _trend_structure_gap(technical):
        return "trend_structure_gap"
    if entry_signal:
        return "generic_entry_signal"
    if action == "TRIAL_BUY":
        return "score_trial_no_route"
    return "score_only_no_route"


def _best_route_diagnostic(diagnostics: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not diagnostics:
        return None
    return max(
        diagnostics,
        key=lambda item: float(item.get("route_score", 0.0) or 0.0),
    )


def _near_pullback_missing_confirm(technical: dict[str, Any]) -> bool:
    return (
        bool(technical.get("above_ma20"))
        and _float_value(technical.get("ma20_slope")) >= 0.005
        and _float_value(technical.get("momentum_5d")) >= 0.0
        and -2.0 <= _float_value(technical.get("deviation_rate")) <= 6.0
        and 35.0 <= _float_value(technical.get("rsi")) <= 72.0
    )


def _overheated_trend(technical: dict[str, Any]) -> bool:
    return (
        bool(technical.get("above_ma20"))
        and _float_value(technical.get("momentum_5d")) >= 5.0
        and (
            _float_value(technical.get("rsi")) > 70.0
            or _float_value(technical.get("deviation_rate")) > 8.0
            or _float_value(technical.get("change_pct")) >= 6.0
        )
    )


def _trend_structure_gap(technical: dict[str, Any]) -> bool:
    return (
        bool(technical.get("above_ma20"))
        and (
            bool(technical.get("golden_cross"))
            or _float_value(technical.get("ma20_slope")) >= 0.003
            or _float_value(technical.get("momentum_5d")) >= 2.0
        )
    )


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _rsi(closes: pd.Series, period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    deltas = closes.diff()
    gain = deltas.clip(lower=0).rolling(period).mean().iloc[-1]
    loss = (-deltas.clip(upper=0)).rolling(period).mean().iloc[-1]
    if loss == 0:
        return 100.0
    rs = gain / loss
    return round(100 - 100 / (1 + rs), 2)


def _compute_indicators(df: pd.DataFrame, as_of_date: str) -> Optional[dict]:
    """从历史 K 线计算技术指标（截至 as_of_date）。

    Returns dict compatible with TechnicalIndicators fields.
    Returns None if数据不足。
    """
    hist = df[df["日期"] <= as_of_date].copy()
    if len(hist) < 5:
        return None

    closes = hist["收盘"].astype(float)
    volumes = hist["成交量"].astype(float)
    today = hist[hist["日期"] == as_of_date]
    if today.empty:
        return None
    trow = today.iloc[0]
    price = float(trow["收盘"])
    change_pct = float(trow.get("涨跌幅", 0) or 0)

    # MA
    ma5 = float(closes.iloc[-5:].mean()) if len(closes) >= 5 else 0.0
    ma10 = float(closes.iloc[-10:].mean()) if len(closes) >= 10 else 0.0
    ma20 = float(closes.iloc[-20:].mean()) if len(closes) >= 20 else 0.0
    ma60 = float(closes.iloc[-60:].mean()) if len(closes) >= 60 else 0.0

    # Golden cross: MA5 crosses above MA20 in last 2 days
    golden_cross = False
    if len(closes) >= 21:
        ma5_prev = float(closes.iloc[-6:-1].mean()) if len(closes) >= 6 else 0
        ma20_prev = float(closes.iloc[-21:-1].mean()) if len(closes) >= 21 else 0
        if ma5_prev <= ma20_prev and ma5 > ma20 and ma20 > 0:
            golden_cross = True

    # Volume ratio: today / avg(last 20)
    vol_avg20 = float(volumes.iloc[-20:].mean()) if len(volumes) >= 20 else float(volumes.mean())
    volume_ratio = float(trow["成交量"]) / vol_avg20 if vol_avg20 > 0 else 1.0

    # Momentum 5d
    if len(closes) >= 5:
        mom5 = (float(closes.iloc[-1]) - float(closes.iloc[-5])) / float(closes.iloc[-5]) * 100
    else:
        mom5 = 0.0

    # Daily volatility (20d std of returns)
    if len(closes) >= 21:
        ret20 = closes.iloc[-20:].pct_change().std()
        daily_volatility = float(ret20) if not math.isnan(ret20) else 0.0
    else:
        daily_volatility = 0.0

    # MA20 slope: (ma20_today - ma20_5d_ago) / ma20_5d_ago
    ma20_slope = 0.0
    if len(closes) >= 25 and ma20 > 0:
        ma20_5d_ago = float(closes.iloc[-25:-5].mean()) if len(closes) >= 25 else ma20
        if ma20_5d_ago > 0:
            ma20_slope = (ma20 - ma20_5d_ago) / ma20_5d_ago

    above_ma20 = price > ma20 > 0
    return {
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "above_ma20": above_ma20,
        "volume_ratio": round(volume_ratio, 2),
        "rsi": _rsi(closes),
        "golden_cross": golden_cross,
        "ma20_slope": round(ma20_slope, 6),
        "momentum_5d": round(mom5, 2),
        "daily_volatility": round(daily_volatility, 6),
        "deviation_rate": round((price - ma20) / ma20 * 100, 2) if ma20 > 0 else 0.0,
        "change_pct": change_pct,
    }


def _market_state_from_index(
    index_df: pd.DataFrame, as_of_date: str, config: dict
) -> "MarketState":
    """从指数历史数据计算大盘信号。

    回测场景下（数据通常只有 120 天），优先用 MA20 判断：
    - 有 MA60 数据：标准三档（GREEN / YELLOW / RED / CLEAR）
    - 无 MA60 数据（<60天）：用 MA20 替代 MA60 的判断
    - 数据极少（<20天）：保守返回 GREEN（不阻止交易）
    """
    from astock_trading.strategy.models import MarketSignal, MarketState

    hist = index_df[index_df["日期"] <= as_of_date].copy()
    closes = hist["收盘"].astype(float)

    if len(closes) < 20:
        # 数据极少，保守返回 GREEN（让个股信号主导）
        return MarketState(signal=MarketSignal.GREEN, multiplier=1.0, detail={"reason": "数据不足，默认GREEN"})

    price = float(closes.iloc[-1])
    ma20_idx = float(closes.iloc[-20:].mean())
    ma60_idx = float(closes.iloc[-60:].mean()) if len(closes) >= 60 else 0.0
    ma120_idx = float(closes.iloc[-120:].mean()) if len(closes) >= 120 else 0.0
    ma_idx = ma60_idx if ma60_idx > 0 else ma20_idx  # 降级用 MA20

    above_ma20 = price > ma20_idx > 0
    above_ma = price > ma_idx > 0
    above_ma120 = price > ma120_idx > 0 if ma120_idx > 0 else None
    index_ma20_deviation_pct = ((price - ma20_idx) / ma20_idx * 100) if ma20_idx > 0 else 0.0
    index_ma20_slope_5d_pct = 0.0
    if len(closes) >= 25 and ma20_idx > 0:
        ma20_5d_ago = float(closes.iloc[-25:-5].mean())
        if ma20_5d_ago > 0:
            index_ma20_slope_5d_pct = (ma20_idx - ma20_5d_ago) / ma20_5d_ago * 100
    index_ma120_slope_20d_pct = 0.0
    if len(closes) >= 140 and ma120_idx > 0:
        ma120_20d_ago = float(closes.iloc[-140:-20].mean())
        if ma120_20d_ago > 0:
            index_ma120_slope_20d_pct = (ma120_idx - ma120_20d_ago) / ma120_20d_ago * 100

    # below_ma60_days（不足 60 天时，统计低于 MA20 的天数代替）
    below_days = 0
    below_ma20_days = 0
    above_ma20_days = 0
    lookback = min(20, len(closes) - 1)
    if ma_idx > 0:
        for p in reversed(closes.iloc[-lookback:].tolist()):
            if p < ma_idx:
                below_days += 1
            else:
                break
    if ma20_idx > 0:
        for p in reversed(closes.iloc[-lookback:].tolist()):
            if p < ma20_idx:
                below_ma20_days += 1
            else:
                break
        for p in reversed(closes.iloc[-lookback:].tolist()):
            if p > ma20_idx:
                above_ma20_days += 1
            else:
                break

    clear_days = config.get("clear_days_ma60", 15)

    # 多档判断
    if below_days >= clear_days:
        signal = MarketSignal.CLEAR
    elif above_ma20:
        signal = MarketSignal.GREEN
    elif above_ma:
        signal = MarketSignal.YELLOW
    else:
        signal = MarketSignal.RED

    multipliers = {
        MarketSignal.GREEN: 1.0,
        MarketSignal.YELLOW: 0.5,
        MarketSignal.RED: 0.0,
        MarketSignal.CLEAR: 0.0,
    }
    for key, value in (config.get("market_multipliers") or {}).items():
        if key in MarketSignal._value2member_map_:
            multipliers[MarketSignal(key)] = float(value)
    multiplier = multipliers.get(signal, 0.0)

    return MarketState(
        signal=signal,
        multiplier=multiplier,
        detail={
            "index": "上证指数",
            "price": round(price, 2),
            "ma20": round(ma20_idx, 2),
            "ma60": round(ma60_idx, 2) if ma60_idx > 0 else None,
            "ma120": round(ma120_idx, 2) if ma120_idx > 0 else None,
            "above_ma20": above_ma20,
            "above_ma60": bool(price > ma60_idx > 0) if ma60_idx > 0 else None,
            "above_ma120": above_ma120,
            "below_ma_days": below_days,
            "below_ma20_days": below_ma20_days,
            "above_ma20_days": above_ma20_days,
            "index_ma20_deviation_pct": round(index_ma20_deviation_pct, 2),
            "index_ma20_slope_5d_pct": round(index_ma20_slope_5d_pct, 2),
            "index_ma120_slope_20d_pct": round(index_ma120_slope_20d_pct, 2),
        },
    )


def _market_state_from_history_bundle(payload: dict, fallback: "MarketState") -> "MarketState":
    from astock_trading.strategy.models import MarketSignal, MarketState

    signal_value = str(payload.get("signal") or fallback.signal.value)
    signal = MarketSignal(signal_value) if signal_value in MarketSignal._value2member_map_ else fallback.signal
    detail = payload.get("detail")
    if not isinstance(detail, dict) or not detail:
        detail = dict(getattr(fallback, "detail", {}) or {})
        detail.setdefault("source", "history_mirror")
    return MarketState(
        signal=signal,
        multiplier=float(payload.get("multiplier", fallback.multiplier) or 0.0),
        detail=detail,
    )


def _history_score_value(candidate: dict, decision: dict) -> float:
    value = (
        candidate.get("total_score")
        or candidate.get("total")
        or candidate.get("score")
        or decision.get("score")
        or decision.get("confidence")
        or 0.0
    )
    return float(value or 0.0)


def _data_quality_from_history(value: object, data_quality_enum):
    quality = str(value or data_quality_enum.OK.value)
    return data_quality_enum(quality) if quality in data_quality_enum._value2member_map_ else data_quality_enum.OK


def _counter_inc(counter: dict[str, int], key: str, amount: int = 1) -> None:
    counter[key] = int(counter.get(key, 0)) + int(amount)


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _market_signal_value(market: object) -> str:
    return str(getattr(getattr(market, "signal", None), "value", "") or "unknown")


def _market_context_payload(market: object) -> dict[str, Any]:
    detail = getattr(market, "detail", {}) or {}
    if not isinstance(detail, dict):
        detail = {}
    payload = {
        "price": detail.get("price"),
        "ma20": detail.get("ma20"),
        "ma60": detail.get("ma60"),
        "ma120": detail.get("ma120"),
        "above_ma60": detail.get("above_ma60"),
        "above_ma120": detail.get("above_ma120"),
        "index_ma20_deviation_pct": detail.get("index_ma20_deviation_pct"),
        "index_ma20_slope_5d_pct": detail.get("index_ma20_slope_5d_pct"),
        "index_ma120_slope_20d_pct": detail.get("index_ma120_slope_20d_pct"),
        "above_ma20_days": detail.get("above_ma20_days"),
        "below_ma20_days": detail.get("below_ma20_days"),
        "below_ma_days": detail.get("below_ma_days"),
    }
    payload["market_phase_bucket"] = market_phase_bucket(payload)
    return payload


def _is_market_reduce_signal(market: object) -> bool:
    return _market_signal_value(market) in {"RED", "CLEAR"}


def _score_route_label(score: object) -> str:
    route = str(getattr(score, "primary_strategy_route", "") or "")
    if route:
        return route
    if bool(getattr(score, "entry_signal", False)):
        return "generic_entry_signal_watch"
    return "no_entry_route"


def _history_discovery_sources(sections: dict[str, Any]) -> dict[str, list[str]]:
    sources_by_code: dict[str, set[str]] = {}
    for source_name in ("pool", "candidates", "decision"):
        payload = sections.get(source_name) or []
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            code = str(item.get("code") or "").strip()
            if not code:
                continue
            sources_by_code.setdefault(code, set()).add(source_name)
    return {
        code: sorted(sources)
        for code, sources in sorted(sources_by_code.items())
    }


def _decision_reason_keys(intent: object) -> list[str]:
    notes = [str(item) for item in (getattr(intent, "notes", []) or []) if str(item)]
    action = str(getattr(getattr(intent, "action", None), "value", "") or "")
    reasons: list[str] = []
    for note in notes:
        if "一票否决" in note:
            reasons.append("veto")
        if "禁止新开仓" in note:
            reasons.append("market_blocks_new_positions")
        if "入场信号未触发" in note:
            reasons.append("entry_signal_missing")
        if "入场信号已触发" in note and "正式买入线" in note:
            reasons.append("entry_signal_below_buy_line")
        if "观察路线" in note and "试买意向" in note:
            reasons.append("watch_route_near_trial_line")
        if "数据质量" in note and "低于要求" in note:
            reasons.append("data_quality_below_min")
        if "关键数据缺失过多" in note:
            reasons.append("too_many_missing_fields")
        if "关键字段缺失" in note:
            reasons.append("critical_missing_fields")
        if "仓位空间不足" in note:
            reasons.append("position_space_insufficient")
        if "本周已买" in note:
            reasons.append("weekly_limit_decision")
        if "市场制度阻断观察" in note:
            reasons.append("regime_trial_disabled")
        if "只允许试买路线" in note:
            reasons.append("trial_route_not_enabled_by_regime")
        if "禁用试买路线" in note:
            reasons.append("trial_route_disabled_by_regime")
        if "评分过低" in note:
            reasons.append("score_too_low")
        if note.startswith("路线 ") and "买入线" in note:
            reasons.append("route_policy_buy_line")
    if not reasons:
        if action == "WATCH":
            reasons.append("score_between_watch_and_buy")
        elif action == "CLEAR":
            reasons.append("clear_without_detail")
        elif action == "TRIAL_BUY":
            reasons.append("trial_buy_soft_signal")
        elif action == "BUY":
            reasons.append("buy_signal")
    return list(dict.fromkeys(reasons))


def _veto_reason_keys(score: object, intent: object | None = None) -> list[str]:
    reasons: list[str] = []
    sources = [getattr(score, "hard_veto", []) or []]
    if intent is not None:
        sources.append(getattr(intent, "veto_reasons", []) or [])
    for source in sources:
        for item in source:
            reason = str(item or "").strip()
            if reason and reason not in reasons:
                reasons.append(reason)
    return reasons


# ---------------------------------------------------------------------------
# BacktestEngine
# ---------------------------------------------------------------------------

@dataclass
class BacktestConfig:
    preset_name: str = "保守验证C"
    initial_cash: float = 100000.0
    adjustflag: str = "2"
    pnl_adjustflag: str = "1"
    # 风控参数（来自 preset）
    trailing_stop: float = 0.10
    stop_loss: float = 0.08
    time_stop_days: int = 15
    buy_threshold: float = 6.5
    single_max_pct: float = 0.20
    total_max_pct: float = 0.60
    weekly_max: int = 2
    weekly_max_by_market: dict = field(default_factory=dict)
    daily_max_buys: int = 2
    holding_max: int = 5
    route_execution_policy: dict = field(default_factory=dict)
    execute_buy_phase_buckets: tuple[str, ...] = ()
    # 评分权重
    weights: dict = field(default_factory=lambda: {
        "technical": 3.0, "fundamental": 2.0, "flow": 2.0, "sentiment": 3.0
    })
    veto_rules: list = field(default_factory=lambda: [
        "below_ma20", "limit_up_today", "consecutive_outflow", "red_market", "ma20_trend_down"
    ])
    decision_gates: dict = field(default_factory=dict)
    market_regime_overlays: dict = field(default_factory=dict)
    score_adjustments: dict = field(default_factory=dict)
    score_dimension_mode: str = "full"
    market_multipliers: dict = field(default_factory=dict)
    disable_market_reduce_sell: bool = False
    watch_loss_cooldown_days: int = 0
    watch_loss_cooldown_phase_buckets: tuple[str, ...] = ()
    execute_trial_buy_market_signals: tuple[str, ...] = ()
    execute_trial_buy_routes: tuple[str, ...] = ()
    execute_watch_trial_market_signals: tuple[str, ...] = ()
    execute_watch_trial_routes: tuple[str, ...] = ()
    execute_watch_trial_pairs: tuple[str, ...] = ()
    execute_watch_trial_score_min: float = 6.0
    execute_watch_trial_score_max: float | None = None
    execute_watch_trial_position_pct: float | None = None
    execute_watch_trial_phase_buckets: tuple[str, ...] = ()
    execute_watch_trial_min_above_ma20_days: int = 0
    execute_watch_trial_min_above_ma20_days_phase_buckets: tuple[str, ...] = ()
    execute_watch_trial_require_above_ma60_phase_buckets: tuple[str, ...] = ()
    execute_watch_trial_require_above_ma120_phase_buckets: tuple[str, ...] = ()
    scale_in_enabled: bool = False
    scale_in_profit_threshold: float = 0.10
    scale_in_step_position_pct: float = 0.075
    scale_in_max_position_pct: float | None = None
    scale_in_max_adds: int = 2
    scale_in_min_days_between: int = 5
    scale_in_routes: tuple[str, ...] = ()
    scale_in_market_signals: tuple[str, ...] = ()
    scale_in_actions: tuple[str, ...] = ("BUY", "WATCH")
    scale_in_require_entry_signal: bool = True
    scale_in_score_min: float = 5.0
    scale_in_reset_time_stop: bool = True
    scale_in_aggressive_max_position_pct: float | None = None
    scale_in_aggressive_step_position_pct: float | None = None
    scale_in_aggressive_market_signals: tuple[str, ...] = ()
    scale_in_aggressive_routes: tuple[str, ...] = ()
    scale_in_aggressive_phase_buckets: tuple[str, ...] = ()
    trade_record_limit: int | None = 50
    signal_record_limit: int | None = 50
    signal_slices: tuple[str, ...] = ()
    include_signal_alpha: bool = True
    load_financials: bool = True
    progress_log: bool = False
    use_market_bars: bool = False
    hydrate_market_bars: bool = False
    use_financial_cache: bool = False
    hydrate_financial_cache: bool = False
    financial_query_timeout_seconds: float = 45.0
    require_reachable_candidate_for_buy: bool = False
    reachable_lookback_days: int = 5
    commission_bps: float = 2.5
    min_commission: float = 5.0
    stamp_tax_bps: float = 5.0
    transfer_fee_bps: float = 0.1
    slippage_bps: float = 5.0


@dataclass
class Position:
    code: str
    shares: int
    entry_price: float
    entry_date: str
    high_water: float
    pnl_units: float = 0.0
    pnl_entry_price: float = 0.0
    pnl_high_water: float = 0.0
    market_reduced: bool = False  # 是否已因大盘CLEAR减过仓
    position_pct: float = 0.0
    entry_route: str = ""
    entry_market_signal: str = ""
    market_reduce_exempt: bool = False
    add_count: int = 0
    last_add_date: str = ""
    cost_basis: float = 0.0
    pending_exit_reason: str = ""
    pending_exit_trigger_date: str = ""
    pending_exit_shares: int = 0


@dataclass
class PendingBuyOrder:
    signal_date: str
    execution_date: str
    score: Any
    intent: Any
    market: Any
    position_pct: float
    kind: str = "buy"


class BacktestEngine:
    """生产级回测引擎 — 复用 Scorer + Decider。"""

    def __init__(
        self,
        config: BacktestConfig,
        history_conn: Any | None = None,
        market_conn: Any | None = None,
    ):
        self.cfg = config
        self._history_conn = history_conn
        self._market_conn = market_conn
        self._market_store = None
        if market_conn is not None:
            from astock_trading.market.store import MarketStore

            self._market_store = MarketStore(market_conn)
        self._scorer = None
        self._decider = None
        self._bars: dict[str, pd.DataFrame] = {}       # code -> df
        self._pnl_bars: dict[str, pd.DataFrame] = {}   # code -> 后复权/收益口径 df
        self._index_df: Optional[pd.DataFrame] = None  # 上证指数
        self._sorted_dates: list[str] = []
        self._portfolio_value_series: list[dict] = []
        self._trades: list[dict] = []
        self._positions: dict[str, Position] = {}
        self._pending_buy_orders: list[PendingBuyOrder] = []
        self._cash: float = config.initial_cash
        self._weekly_buy_count: int = 0
        self._last_week: str = ""
        self._last_index_date: str = ""
        self._financial_cache: dict[str, list[dict]] = {}  # code -> point-in-time snapshots
        self._history_mirror_dates: list[str] = []
        self._history_mirror_cache: dict[str, dict[str, Any]] | None = None
        self._proxy_replay_dates: list[str] = []
        self._signal_records: list[dict[str, Any]] = []
        self._execution_funnel: dict[str, Any] = {
            "signals_total": 0,
            "entry_signal_total": 0,
            "executable_candidates": 0,
            "executed_buys": 0,
            "actions": {},
            "market_signals": {},
            "routes": {},
            "market_routes": {},
            "skip_reasons": {},
            "decision_reasons": {},
            "veto_reasons": {},
            "by_market_route": {},
        }
        self._recent_discoveries: dict[str, dict[str, Any]] = {}
        self._discovery_reachability: dict[str, Any] = {
            "candidate_checks": 0,
            "reachable_candidates": 0,
            "blocked_candidates": 0,
            "discovery_sources": {},
            "blocked_reasons": {},
            "blocked_codes": {},
        }
        self._requested_codes: list[str] = []
        self._loaded_codes: list[str] = []
        self._current_date_index: int = 0
        self._watch_loss_cooldown_until_index: int = -1
        self._execution_constraints: dict[str, Any] = {
            "t_plus_one": True,
            "t_plus_one_blocked_sells": 0,
            "buy_signals": 0,
            "buy_orders_scheduled": 0,
            "buy_orders_expired": 0,
            "buy_untradable": 0,
            "sell_triggers": 0,
            "sell_orders_pending": 0,
            "sell_untradable": 0,
            "pnl_price_missing_risk_skips": 0,
            "untradable_reasons": {},
        }
        self._cost_totals: dict[str, float] = {
            "commission": 0.0,
            "stamp_tax": 0.0,
            "transfer_fee": 0.0,
            "fee_total": 0.0,
            "slippage_cost": 0.0,
            "total_cost": 0.0,
        }

    def _log_progress(self, event: str, **fields: Any) -> None:
        if not self.cfg.progress_log:
            return
        chunks = [f"event={event}"]
        for key, value in fields.items():
            chunks.append(f"{key}={value}")
        print("[backtest_progress] " + " ".join(chunks), file=sys.stderr, flush=True)

    def load_data(
        self,
        codes: list[str],
        start_date: str,
        end_date: str,
        pre_start: str,
    ) -> dict:
        """从 baostock 加载股票和指数数据。

        Args:
            codes: 股票代码列表
            start_date: 回测开始日
            end_date: 回测结束日
            pre_start: 向前多拉的历史数据起点（用于 MA 计算）
        """
        from astock_trading.market.adapters import BaoStockMarketAdapter

        adapter = BaoStockMarketAdapter()
        self._requested_codes = list(codes)
        self._log_progress(
            "load_data_start",
            codes=len(codes),
            start=start_date,
            end=end_date,
            pre_start=pre_start,
        )
        bulk_market_bars: dict[str, pd.DataFrame] | None = None
        if self.cfg.use_market_bars and not self.cfg.hydrate_market_bars and self._market_store is not None:
            bulk_started_at = time.monotonic()
            bulk_codes = sorted({*codes, "000001"})
            self._log_progress("kline_cache_bulk_start", codes=len(bulk_codes))
            bulk_market_bars = self._market_store.get_price_bars_bulk(
                bulk_codes,
                start=pre_start,
                end=end_date,
                adjustflag=self.cfg.adjustflag,
                chunk_size=100,
            )
            self._log_progress(
                "kline_cache_bulk_done",
                codes=len(bulk_market_bars),
                rows=sum(len(frame) for frame in bulk_market_bars.values()),
                seconds=round(time.monotonic() - bulk_started_at, 2),
            )

        # 加载股票数据（baostock 单次最多返回 ~120 条，分多批取再合并）
        def _fetch_code(code: str, *, adjustflag: str | None = None) -> Optional[pd.DataFrame]:
            bar_adjustflag = str(adjustflag or self.cfg.adjustflag)
            code_started_at = time.monotonic()
            self._log_progress("kline_start", code=code, adjustflag=bar_adjustflag)
            cached: pd.DataFrame | None = None
            if self.cfg.use_market_bars:
                if bulk_market_bars is not None and bar_adjustflag == str(self.cfg.adjustflag):
                    cached = self._normalize_market_bars_cache(code, bulk_market_bars.get(code))
                else:
                    cached = self._load_market_bars_cache(
                        code,
                        pre_start,
                        end_date,
                        require_full=False,
                        adjustflag=bar_adjustflag,
                    )
                if cached is not None and _cache_covers_range(cached, pre_start, end_date):
                    self._log_progress(
                        "kline_cache_hit",
                        code=code,
                        adjustflag=bar_adjustflag,
                        rows=len(cached),
                        seconds=round(time.monotonic() - code_started_at, 2),
                    )
                    return cached
                if (
                    cached is not None
                    and not self.cfg.hydrate_market_bars
                    and (
                        _cache_covers_backtest_range(cached, start_date, end_date)
                        or _cache_covers_end_date(cached, end_date)
                    )
                ):
                    self._log_progress(
                        "kline_cache_hit_backtest_range",
                        code=code,
                        adjustflag=bar_adjustflag,
                        rows=len(cached),
                        seconds=round(time.monotonic() - code_started_at, 2),
                    )
                    return cached
                self._log_progress(
                    "kline_cache_partial" if cached is not None and not cached.empty else "kline_cache_miss",
                    code=code,
                    adjustflag=bar_adjustflag,
                    cached_rows=0 if cached is None else len(cached),
                )
                if cached is not None and not cached.empty and not self.cfg.hydrate_market_bars:
                    self._log_progress(
                        "kline_cache_hit_partial_no_hydrate",
                        code=code,
                        adjustflag=bar_adjustflag,
                        rows=len(cached),
                        seconds=round(time.monotonic() - code_started_at, 2),
                    )
                    return cached
            ranges = _missing_ranges_for_cache(cached, pre_start, end_date)

            dfs = []
            for i, (segment_start, segment_end) in enumerate(ranges):
                segment_started_at = time.monotonic()
                self._log_progress(
                    "kline_segment_start",
                    code=code,
                    adjustflag=bar_adjustflag,
                    segment=f"{i + 1}/{len(ranges)}",
                    start=segment_start,
                    end=segment_end,
                )
                df = asyncio.run(adapter.get_kline(
                    code, period="daily",
                    count=0,
                    start_date=segment_start, end_date=segment_end,
                    adjustflag=bar_adjustflag,
                ))
                self._log_progress(
                    "kline_segment_done",
                    code=code,
                    adjustflag=bar_adjustflag,
                    segment=f"{i + 1}/{len(ranges)}",
                    rows=0 if df is None else len(df),
                    seconds=round(time.monotonic() - segment_started_at, 2),
                )
                if df is not None and not df.empty:
                    dfs.append(df)

            if cached is not None and not cached.empty:
                dfs.insert(0, cached)
            if not dfs:
                self._log_progress(
                    "kline_empty",
                    code=code,
                    seconds=round(time.monotonic() - code_started_at, 2),
                )
                return None
            combined = pd.concat(dfs, ignore_index=True)
            combined = combined.drop_duplicates(subset=["日期"]).sort_values("日期").reset_index(drop=True)
            if self.cfg.hydrate_market_bars and self._market_store is not None:
                saved = self._market_store.save_price_bars(
                    code,
                    combined,
                    source="baostock",
                    adjustflag=bar_adjustflag,
                )
                if self._market_conn is not None:
                    self._market_conn.commit()
                self._log_progress("kline_cache_saved", code=code, rows=saved)
            self._log_progress(
                "kline_done",
                code=code,
                adjustflag=bar_adjustflag,
                rows=len(combined),
                seconds=round(time.monotonic() - code_started_at, 2),
            )
            return combined

        for code in codes:
            df = _fetch_code(code)
            if df is not None and not df.empty:
                self._bars[code] = df
        self._loaded_codes = sorted(self._bars)

        if not self._bars:
            return {"error": "所有股票均无法获取数据", "codes": codes}

        if self._uses_separate_pnl_bars():
            self._log_progress(
                "pnl_kline_start",
                codes=len(self._bars),
                adjustflag=self.cfg.pnl_adjustflag,
            )
            for code in list(self._bars.keys()):
                pnl_df = _fetch_code(code, adjustflag=self.cfg.pnl_adjustflag)
                if pnl_df is not None and not pnl_df.empty:
                    self._pnl_bars[code] = pnl_df
            self._log_progress("pnl_kline_done", loaded=len(self._pnl_bars))

        # 加载上证指数数据（同样需要分批，绕过 120 条限制）
        self._log_progress("index_start", code="000001")
        idx_df = _fetch_code("000001")
        if idx_df is not None and not idx_df.empty:
            self._index_df = idx_df
        self._log_progress("index_done", rows=0 if idx_df is None else len(idx_df))

        # 预加载财务快照，打分时按 as_of_date 选择最近已披露一期。
        if self.cfg.load_financials:
            self._log_progress("financials_start", codes=len(self._bars))
            self._load_financials(list(self._bars.keys()), start_date, end_date)
            self._log_progress("financials_done", codes=len(self._financial_cache))
        else:
            self._log_progress("financials_skipped")

        # 回测交易日应由市场日历驱动，不能取所有标的日期交集。
        # 否则池子里只要有一只较晚上市或短历史股票，就会裁掉整个组合的早期样本。
        if self._index_df is not None and not self._index_df.empty:
            calendar_dates = set(self._index_df["日期"].tolist())
        else:
            calendar_dates: set[str] = set()
            for df in self._bars.values():
                calendar_dates.update(str(item) for item in df["日期"].tolist())
        self._sorted_dates = sorted(d for d in calendar_dates if start_date <= d <= end_date)

        if not self._sorted_dates:
            return {"error": f"无交易日（区间 {start_date}~{end_date}）"}

        self._log_progress(
            "load_data_done",
            loaded=len(self._bars),
            trading_days=len(self._sorted_dates),
        )
        return {"loaded": len(self._bars), "trading_days": len(self._sorted_dates)}

    def _load_market_bars_cache(
        self,
        code: str,
        pre_start: str,
        end_date: str,
        *,
        require_full: bool = True,
        adjustflag: str | None = None,
    ) -> pd.DataFrame | None:
        if self._market_store is None:
            return None
        cached = self._market_store.get_price_bars(
            code,
            start=pre_start,
            end=end_date,
            adjustflag=str(adjustflag or self.cfg.adjustflag),
        )
        if cached.empty or (require_full and not _cache_covers_range(cached, pre_start, end_date)):
            return None
        return self._normalize_market_bars_cache(code, cached)

    def _normalize_market_bars_cache(self, code: str, cached: pd.DataFrame | None) -> pd.DataFrame | None:
        if cached is None or cached.empty:
            return None
        cached = cached.copy().drop_duplicates(subset=["日期"]).sort_values("日期").reset_index(drop=True)
        cached["证券名称"] = code
        cached["名称"] = code
        if "涨跌幅" not in cached.columns:
            prev_close = cached["收盘"].shift(1)
            cached["涨跌幅"] = ((cached["收盘"] / prev_close) - 1.0).fillna(0.0) * 100
        return cached

    def run(self) -> dict:
        """执行回测，返回完整报告。"""
        if not self._bars or not self._sorted_dates:
            return {"error": "请先调用 load_data()"}

        # 初始化 Scorer 和 Decider
        from astock_trading.strategy.models import (
            ScoringWeights,
        )
        from astock_trading.strategy.scorer import Scorer
        from astock_trading.strategy.decider import Decider

        w = _score_weights_for_mode(self.cfg.weights, self.cfg.score_dimension_mode)
        self._scorer = Scorer(
            weights=ScoringWeights(
                technical=w.get("technical", 3),
                fundamental=w.get("fundamental", 2),
                flow=w.get("flow", 2),
                sentiment=w.get("sentiment", 3),
            ),
            veto_rules=self.cfg.veto_rules,
            entry_cfg={
                "rsi_max": 70,
                "volume_ratio_min": 1.5,
                "deviation_max": self.cfg.preset_name == "保守验证C" and 10.0 or 12.0,
            },
            score_adjustments=self.cfg.score_adjustments,
        )
        gates = self.cfg.decision_gates or {}
        self._decider = Decider(
            # 回测场景无真实资金流数据，分数上限约 6.0，将阈值适当降低
            # 实盘使用 preset 的原始 buy_threshold
            buy_threshold=max(5.0, self.cfg.buy_threshold - 1.0),
            watch_threshold=4.0,
            single_max_pct=self.cfg.single_max_pct,
            total_max_pct=self.cfg.total_max_pct,
            weekly_max=self.cfg.weekly_max,
            require_entry_signal_for_buy=bool(gates.get("require_entry_signal_for_buy", False)),
            min_data_quality_for_buy=gates.get("min_data_quality_for_buy", "degraded"),
            max_missing_fields_for_buy=gates.get("max_missing_fields_for_buy"),
            critical_missing_fields_for_buy=gates.get("critical_missing_fields_for_buy", []),
            min_position_pct_for_buy=gates.get("min_position_pct_for_buy", 0.01),
            trial_buy_threshold=gates.get("trial_buy_threshold"),
            trial_buy_entry_signal_threshold=gates.get("trial_buy_entry_signal_threshold"),
            market_regime_overlays=self.cfg.market_regime_overlays,
            route_execution_policy=self.cfg.route_execution_policy,
        )

        index_config = {
            "clear_days_ma60": 15,
            "market_multipliers": self.cfg.market_multipliers,
        }
        self._preload_history_mirror_cache()

        for i, d in enumerate(self._sorted_dates):
            self._current_date_index = i
            if i == 0 or (i + 1) % 50 == 0 or i == len(self._sorted_dates) - 1:
                self._log_progress(
                    "simulation_day",
                    date=d,
                    index=i + 1,
                    total=len(self._sorted_dates),
                    positions=len(self._positions),
                )
            self._check_week_reset(d)

            # ── 1. 大盘信号 ──────────────────────────────────────────
            if self._index_df is not None and d != self._last_index_date:
                self._market_state = _market_state_from_index(self._index_df, d, index_config)
                self._last_index_date = d
            elif not hasattr(self, "_market_state"):
                from astock_trading.strategy.models import MarketSignal, MarketState
                self._market_state = MarketState(signal=MarketSignal.CLEAR, multiplier=0.0)

            market = self._market_state
            mirror_replay = self._mirror_replay_for_date(d, market)
            if mirror_replay is not None:
                market = mirror_replay["market"]
                self._market_state = market
                self._history_mirror_dates.append(d)
                self._record_discoveries_for_date(d, mirror_replay)
            else:
                self._proxy_replay_dates.append(d)

            # ── 2. 撮合前一交易日产生的买入意图 ───────────────────────────
            # 信号由前一交易日收盘后生成，最早只能在当前交易日开盘撮合。
            self._execute_pending_buy_orders(d)

            # ── 2. 持仓权益 ──────────────────────────────────────────
            portfolio_value = self._cash + self._positions_market_value(d)

            # ── 3. 风控检查（止损/止盈/到期）─────────────────────────
            self._risk_check(d, i)
            portfolio_value = self._cash + self._positions_market_value(d)

            # ── 4. 评分 + 决策 ───────────────────────────────────────
            current_exposure = (portfolio_value - self._cash) / portfolio_value if portfolio_value > 0 else 0.0
            if mirror_replay is not None and mirror_replay.get("has_strategy_intents", bool(mirror_replay["intents"])):
                intents = mirror_replay["intents"]
            else:
                if mirror_replay is not None:
                    self._proxy_replay_dates.append(d)
                intents = []
                candidate_codes = self._candidate_codes_for_date(d)
                for code in candidate_codes:
                    snapshot = self._build_snapshot(code, d)
                    if snapshot is None:
                        continue

                    score = self._scorer.score(snapshot)
                    intent = self._decider.decide(score, market, current_exposure, self._weekly_buy_count)
                    intents.append((score, intent))
            self._record_signal_validation_rows(d, intents, market)
            self._record_execution_funnel_intents(d, intents, market)

            # ── 5. 执行 SELL 信号 ───────────────────────────────────
            # 区分大盘 CLEAR（减仓50%）和个股分数低（不等强制卖，等止损）
            # intent.notes 里有 "大盘" 的是市场原因，否则是个股原因
            for score, intent in intents:
                if score.code not in self._positions:
                    continue

                is_market_clear = self._should_market_reduce_position(market)
                is_individual_clear = intent.action.value == "CLEAR"

                if not (is_market_clear or is_individual_clear):
                    continue

                pos = self._positions[score.code]
                if pos.pending_exit_reason:
                    continue

                if self._should_reduce_position_for_market(pos, market):
                    # 大盘 CLEAR/RED → 下一交易日减仓 50%（每个持仓只减一次）
                    self._execution_constraints["sell_triggers"] += 1
                    sell_shares = pos.shares // 2
                    pos.market_reduced = True
                    if sell_shares <= 0:
                        # 不足2手则全部清仓
                        sell_shares = pos.shares
                    reason = f"大盘{market.signal.value}减仓"
                    self._queue_exit(pos, reason=reason, trigger_date=d, shares=sell_shares)
                else:
                    # 个股分数低 → 不强制卖，等止损/时间止损自然退出
                    continue

            # ── 6. 趋势加仓（研究 what-if）────────────────────────────
            self._scale_in_positions(
                trade_date=d,
                day_index=i,
                intents=intents,
                market=market,
            )

            # ── 6. 执行 BUY 信号 ────────────────────────────────────
            if len(self._positions) < int(self.cfg.holding_max or 5) and self._cash > self.cfg.initial_cash * 0.05:
                buy_candidates = []
                for score, intent in intents:
                    route = getattr(score, "primary_strategy_route", None)
                    execution_status = self._intent_execution_status(
                        intent,
                        score.code,
                        route,
                        score_total=score.total,
                        market=market,
                    )
                    if not execution_status["executable"]:
                        self._record_execution_funnel_skip(execution_status["reason"], score, intent, market)
                        continue
                    reachability_status = self._reachability_status(d, score.code)
                    self._record_reachability_check(reachability_status, score.code)
                    if not reachability_status["reachable"]:
                        self._record_execution_funnel_skip(reachability_status["reason"], score, intent, market)
                        continue
                    if score.code in self._positions:
                        self._record_execution_funnel_skip("already_held", score, intent, market)
                        continue
                    if self._has_pending_buy_order(score.code):
                        self._record_execution_funnel_skip("buy_order_pending", score, intent, market)
                        continue
                    self._record_execution_funnel_executable(score, intent, market)
                    buy_candidates.append((score, intent))
                buy_candidates.sort(
                    key=lambda x: self._buy_candidate_sort_key(
                        x[1],
                        getattr(x[0], "primary_strategy_route", None),
                        market,
                        score_total=x[0].total,
                    ),
                    reverse=True,
                )

                execution_date = self._next_trade_date(d)
                daily_remaining = max(
                    0,
                    int(self.cfg.daily_max_buys or 2)
                    - self._pending_buy_order_count(execution_date=execution_date, include_scale_in=False),
                )
                weekly_remaining = max(
                    0,
                    self._weekly_max_for_market(market)
                    - self._weekly_buy_count
                    - self._pending_buy_order_count(include_scale_in=False),
                )
                for score, intent in buy_candidates:
                    if daily_remaining <= 0:
                        self._record_execution_funnel_skip("daily_limit", score, intent, market)
                        continue
                    if score.code in self._positions:
                        self._record_execution_funnel_skip("already_held", score, intent, market)
                        continue
                    if self._has_pending_buy_order(score.code):
                        self._record_execution_funnel_skip("buy_order_pending", score, intent, market)
                        continue
                    if weekly_remaining <= 0:
                        self._record_execution_funnel_skip("weekly_limit", score, intent, market)
                        continue
                    self._execution_constraints["buy_signals"] += 1
                    position_pct = self._execution_position_pct(
                        intent,
                        market,
                        route=getattr(score, "primary_strategy_route", None),
                    )
                    if position_pct <= 0:
                        self._record_execution_funnel_skip("zero_position_pct", score, intent, market)
                        continue
                    if not execution_date:
                        self._record_execution_funnel_skip("no_next_trade_date", score, intent, market)
                        continue
                    self._pending_buy_orders.append(PendingBuyOrder(
                        signal_date=d,
                        execution_date=execution_date,
                        score=score,
                        intent=intent,
                        market=market,
                        position_pct=position_pct,
                    ))
                    self._execution_constraints["buy_orders_scheduled"] += 1
                    daily_remaining -= 1
                    weekly_remaining -= 1
            else:
                block_reason = "holding_max" if len(self._positions) >= int(self.cfg.holding_max or 5) else "cash_floor"
                for score, intent in intents:
                    route = getattr(score, "primary_strategy_route", None)
                    if score.code in self._positions:
                        continue
                    if self._intent_executable_for_backtest(
                        intent,
                        score.code,
                        route,
                        score_total=score.total,
                        market=market,
                    ):
                        reachability_status = self._reachability_status(d, score.code)
                        self._record_reachability_check(reachability_status, score.code)
                        self._record_execution_funnel_skip(
                            reachability_status["reason"] if not reachability_status["reachable"] else block_reason,
                            score,
                            intent,
                            market,
                        )

            # ── 7. 记录权益曲线 ─────────────────────────────────────
            portfolio_value = self._cash + self._positions_market_value(d)
            self._portfolio_value_series.append({
                "date": d,
                "equity": round(portfolio_value, 2),
                "cash": round(self._cash, 2),
                "positions": len(self._positions),
            })

        return self._build_report()

    def _record_execution_funnel_intents(
        self,
        trade_date: str,
        intents: list[tuple[Any, Any]],
        market: "MarketState",
    ) -> None:
        del trade_date  # 当前只做聚合统计，保留参数方便后续扩展按日漏斗。
        signal = _market_signal_value(market)
        for score, intent in intents:
            action = str(getattr(getattr(intent, "action", None), "value", "") or "UNKNOWN")
            route = _score_route_label(score)
            key = f"{signal}:{route}"
            self._execution_funnel["signals_total"] += 1
            if bool(getattr(score, "entry_signal", False)):
                self._execution_funnel["entry_signal_total"] += 1
            _counter_inc(self._execution_funnel["actions"], action)
            _counter_inc(self._execution_funnel["market_signals"], signal)
            _counter_inc(self._execution_funnel["routes"], route)
            _counter_inc(self._execution_funnel["market_routes"], key)

            bucket = self._execution_funnel_bucket(signal, route)
            bucket["signals"] += 1
            if bool(getattr(score, "entry_signal", False)):
                bucket["entry_signal_total"] += 1
            _counter_inc(bucket["actions"], action)
            for reason in _decision_reason_keys(intent):
                _counter_inc(self._execution_funnel["decision_reasons"], reason)
                _counter_inc(bucket["decision_reasons"], reason)
            for veto_key in _veto_reason_keys(score, intent):
                _counter_inc(self._execution_funnel["veto_reasons"], veto_key)
                _counter_inc(bucket["veto_reasons"], veto_key)

    def _record_execution_funnel_executable(
        self,
        score: Any,
        intent: Any,
        market: "MarketState",
    ) -> None:
        self._execution_funnel["executable_candidates"] += 1
        bucket = self._execution_funnel_bucket(
            _market_signal_value(market),
            _score_route_label(score),
        )
        bucket["executable_candidates"] += 1
        action = str(getattr(getattr(intent, "action", None), "value", "") or "UNKNOWN")
        _counter_inc(bucket["executable_actions"], action)

    def _record_execution_funnel_skip(
        self,
        reason: str,
        score: Any,
        intent: Any,
        market: "MarketState",
    ) -> None:
        _counter_inc(self._execution_funnel["skip_reasons"], reason)
        bucket = self._execution_funnel_bucket(
            _market_signal_value(market),
            _score_route_label(score),
        )
        _counter_inc(bucket["skip_reasons"], reason)
        action = str(getattr(getattr(intent, "action", None), "value", "") or "UNKNOWN")
        _counter_inc(bucket["skipped_actions"], action)

    def _record_execution_funnel_buy(
        self,
        score: Any,
        intent: Any,
        market: "MarketState",
    ) -> None:
        self._execution_funnel["executed_buys"] += 1
        bucket = self._execution_funnel_bucket(
            _market_signal_value(market),
            _score_route_label(score),
        )
        bucket["executed_buys"] += 1
        action = str(getattr(getattr(intent, "action", None), "value", "") or "UNKNOWN")
        _counter_inc(bucket["executed_actions"], action)

    def _should_market_reduce_position(self, market: "MarketState") -> bool:
        return (not self.cfg.disable_market_reduce_sell) and _is_market_reduce_signal(market)

    def _uses_separate_pnl_bars(self) -> bool:
        return str(self.cfg.adjustflag) == "3" and str(self.cfg.pnl_adjustflag or "") not in {"", "3"}

    def _pnl_df_for_code(self, code: str) -> pd.DataFrame | None:
        pnl_df = self._pnl_bars.get(code)
        if pnl_df is not None:
            return pnl_df
        return self._bars.get(code)

    def _pnl_price_on(self, code: str, trade_date: str, *, field: str = "收盘") -> float | None:
        return self._price_on(self._pnl_df_for_code(code), trade_date, field=field)

    def _pnl_price_on_or_before(self, code: str, trade_date: str, *, field: str = "收盘") -> float | None:
        return self._price_on_or_before(self._pnl_df_for_code(code), trade_date, field=field)

    def _pnl_execution_price(self, code: str, trade_date: str, *, field: str, side: str) -> float | None:
        base_price = self._pnl_price_on(code, trade_date, field=field)
        if base_price is None:
            return None
        direction = 1.0 if str(side).lower() == "buy" else -1.0
        return base_price * (1.0 + direction * float(self.cfg.slippage_bps or 0.0) / 10000.0)

    def _next_trade_date(self, trade_date: str) -> str:
        try:
            idx = self._sorted_dates.index(trade_date)
        except ValueError:
            return ""
        next_idx = idx + 1
        if next_idx >= len(self._sorted_dates):
            return ""
        return self._sorted_dates[next_idx]

    def _has_pending_buy_order(self, code: str) -> bool:
        normalized = str(code or "").strip()
        return any(str(getattr(order.score, "code", "") or "").strip() == normalized for order in self._pending_buy_orders)

    def _pending_buy_order_count(self, *, execution_date: str | None = None, include_scale_in: bool = True) -> int:
        count = 0
        for order in self._pending_buy_orders:
            if execution_date is not None and order.execution_date != execution_date:
                continue
            if not include_scale_in and order.kind == "scale_in":
                continue
            count += 1
        return count

    def _execute_pending_buy_orders(self, trade_date: str) -> None:
        if not self._pending_buy_orders:
            return
        remaining: list[PendingBuyOrder] = []
        for order in self._pending_buy_orders:
            if order.execution_date != trade_date:
                remaining.append(order)
                continue
            executed = self._execute_pending_buy_order(order, trade_date)
            if not executed:
                self._execution_constraints["buy_orders_expired"] += 1
        self._pending_buy_orders = remaining

    def _execute_pending_buy_order(self, order: PendingBuyOrder, trade_date: str) -> bool:
        score = order.score
        intent = order.intent
        market = order.market
        code = str(getattr(score, "code", "") or "")
        if not code:
            return False
        if self._weekly_buy_count >= self._weekly_max_for_market(market):
            self._record_execution_funnel_skip("weekly_limit_at_execution", score, intent, market)
            return False
        df = self._bars.get(code)
        if df is None:
            self._record_execution_funnel_skip("missing_price_row_at_execution", score, intent, market)
            return False
        price = self._price_on(df, trade_date, field="开盘")
        if price is None or price <= 0:
            self._record_execution_funnel_skip("missing_open_price_at_execution", score, intent, market)
            return False
        tradeability = self._tradeability_status(
            df,
            trade_date,
            side="buy",
            code=code,
            name=getattr(score, "name", ""),
            price_field="开盘",
        )
        if not tradeability["tradable"]:
            self._record_untradable(tradeability["reason"], side="buy")
            self._record_execution_funnel_skip(tradeability["reason"], score, intent, market)
            return False

        if order.kind == "scale_in":
            return self._execute_pending_scale_in_order(order, trade_date, price)

        if len(self._positions) >= int(self.cfg.holding_max or 5):
            self._record_execution_funnel_skip("holding_max_at_execution", score, intent, market)
            return False
        if self._cash <= self.cfg.initial_cash * 0.05:
            self._record_execution_funnel_skip("cash_floor_at_execution", score, intent, market)
            return False
        if code in self._positions:
            self._record_execution_funnel_skip("already_held_at_execution", score, intent, market)
            return False

        allocate = self._allocation_budget_for_position(trade_date, order.position_pct, price_field="开盘")
        shares = self._shares_for_buy_budget(budget=allocate, price=price)
        if shares <= 0:
            self._record_execution_funnel_skip("shares_zero_at_execution", score, intent, market)
            return False

        buy_costs = self._trade_costs_for_order(side="buy", price=price, shares=shares)
        pnl_execution_price = self._pnl_execution_price(code, trade_date, field="开盘", side="buy")
        if pnl_execution_price is None or pnl_execution_price <= 0:
            pnl_execution_price = buy_costs["execution_price"]
        pnl_units = float(shares)
        self._cash -= buy_costs["cash_effect"]
        self._record_trade_costs(buy_costs)
        route = getattr(score, "primary_strategy_route", None)
        entry_policy = self._route_execution_policy(
            _market_signal_value(market),
            route,
            action=str(getattr(getattr(intent, "action", None), "value", "") or ""),
            score_total=float(getattr(score, "total", 0.0) or 0.0),
            market=market,
        )
        self._positions[code] = Position(
            code=code,
            shares=shares,
            entry_price=buy_costs["execution_price"],
            entry_date=trade_date,
            high_water=buy_costs["execution_price"],
            pnl_units=pnl_units,
            pnl_entry_price=pnl_execution_price,
            pnl_high_water=pnl_execution_price,
            position_pct=order.position_pct,
            entry_route=str(route or ""),
            entry_market_signal=_market_signal_value(market),
            market_reduce_exempt=bool(entry_policy.get("market_reduce_exempt")),
            cost_basis=buy_costs["cash_effect"],
        )
        self._weekly_buy_count += 1
        trade = self._buy_trade_record(
            trade_date=trade_date,
            signal_date=order.signal_date,
            score=score,
            intent=intent,
            market=market,
            price=buy_costs["execution_price"],
            shares=shares,
            position_pct=order.position_pct,
        )
        trade["signal_price"] = round(price, 4)
        trade["trade_cost"] = buy_costs
        trade["cash_outlay"] = round(buy_costs["cash_effect"], 2)
        self._trades.append(trade)
        self._record_execution_funnel_buy(score, intent, market)
        return True

    def _execute_pending_scale_in_order(self, order: PendingBuyOrder, trade_date: str, price: float) -> bool:
        score = order.score
        intent = order.intent
        market = order.market
        code = str(getattr(score, "code", "") or "")
        pos = self._positions.get(code)
        if pos is None:
            return False
        if pos.pending_exit_reason:
            return False
        allocate = self._allocation_budget_to_target_position(
            trade_date,
            code,
            order.position_pct,
            price_field="开盘",
        )
        shares = self._shares_for_buy_budget(budget=allocate, price=price)
        if shares <= 0:
            return False

        buy_costs = self._trade_costs_for_order(side="buy", price=price, shares=shares)
        pnl_execution_price = self._pnl_execution_price(code, trade_date, field="开盘", side="buy")
        if pnl_execution_price is None or pnl_execution_price <= 0:
            pnl_execution_price = buy_costs["execution_price"]
        added_pnl_units = float(shares)
        old_shares = pos.shares
        old_cost = pos.entry_price * old_shares
        new_cost = buy_costs["execution_price"] * shares
        pos.shares = old_shares + shares
        pos.entry_price = (old_cost + new_cost) / pos.shares
        pos.high_water = max(pos.high_water, buy_costs["execution_price"])
        pos.pnl_units = float(pos.pnl_units or old_shares) + added_pnl_units
        pos.pnl_entry_price = (
            (float(pos.pnl_entry_price or 0.0) * old_shares + pnl_execution_price * shares) / pos.pnl_units
            if pos.pnl_units > 0
            else pnl_execution_price
        )
        pos.pnl_high_water = max(float(pos.pnl_high_water or pos.pnl_entry_price), pnl_execution_price)
        pos.position_pct = round(order.position_pct, 4)
        pos.add_count += 1
        pos.last_add_date = trade_date
        pos.cost_basis = float(pos.cost_basis or old_cost) + buy_costs["cash_effect"]
        if self.cfg.scale_in_reset_time_stop:
            pos.entry_date = trade_date
        if not self._should_market_reduce_position(market):
            pos.market_reduced = False

        self._cash -= buy_costs["cash_effect"]
        self._record_trade_costs(buy_costs)
        self._weekly_buy_count += 1
        trade = self._scale_in_trade_record(
            trade_date=trade_date,
            signal_date=order.signal_date,
            score=score,
            intent=intent,
            market=market,
            price=buy_costs["execution_price"],
            shares=shares,
            position_pct=order.position_pct,
        )
        trade["signal_date"] = order.signal_date
        trade["execution_date"] = trade_date
        trade["signal_price"] = round(price, 4)
        trade["trade_cost"] = buy_costs
        trade["cash_outlay"] = round(buy_costs["cash_effect"], 2)
        self._trades.append(trade)
        self._record_execution_funnel_buy(score, intent, market)
        return True

    def _register_loss_cooldown(self, trade: dict[str, Any], date_index: int) -> None:
        days = int(self.cfg.watch_loss_cooldown_days or 0)
        if days <= 0:
            return
        if str(trade.get("side") or "") != "sell":
            return
        if float(trade.get("pnl") or 0.0) >= 0:
            return
        self._watch_loss_cooldown_until_index = max(
            self._watch_loss_cooldown_until_index,
            int(date_index) + days,
        )

    def _watch_loss_cooldown_status(self, market: "MarketState" | None = None) -> str:
        if int(self.cfg.watch_loss_cooldown_days or 0) <= 0:
            return ""
        if self._current_date_index <= self._watch_loss_cooldown_until_index:
            allowed_phases = {str(item) for item in self.cfg.watch_loss_cooldown_phase_buckets if str(item)}
            if allowed_phases:
                if market is None:
                    return ""
                phase = _market_context_payload(market).get("market_phase_bucket")
                if phase not in allowed_phases:
                    return ""
            return "watch_loss_cooldown"
        return ""

    def _record_discoveries_for_date(self, trade_date: str, mirror_replay: dict[str, Any]) -> None:
        """记录历史运行当天实盘发现链路实际看见过的股票。"""
        sources_by_code = mirror_replay.get("discovery_sources") or {}
        for code, sources in sources_by_code.items():
            normalized = str(code or "").strip()
            if not normalized:
                continue
            source_list = sorted({str(source) for source in (sources or []) if str(source)})
            self._recent_discoveries[normalized] = {
                "last_seen_date": trade_date,
                "sources": source_list,
            }

    def _candidate_codes_for_date(self, trade_date: str) -> list[str]:
        """返回当日策略层允许评分的股票池；reachable-only 时先由发现层收窄。"""
        if not self.cfg.require_reachable_candidate_for_buy:
            return list(self._bars.keys())
        lookback_days = max(int(self.cfg.reachable_lookback_days or 0), 0)
        try:
            current = date_type.fromisoformat(trade_date)
        except ValueError:
            return []
        codes: list[str] = []
        for code, info in self._recent_discoveries.items():
            normalized = str(code or "").strip()
            if normalized not in self._bars:
                continue
            last_seen = str((info or {}).get("last_seen_date") or "")
            try:
                age_days = (current - date_type.fromisoformat(last_seen)).days
            except ValueError:
                continue
            if 0 <= age_days <= lookback_days:
                codes.append(normalized)
        return sorted(set(codes))

    def _reachability_status(self, trade_date: str, code: str) -> dict[str, Any]:
        """判断买入候选是否在近期被实盘发现链路看见过。"""
        if not self.cfg.require_reachable_candidate_for_buy:
            return {
                "reachable": True,
                "reason": "reachability_disabled",
                "sources": [],
                "last_seen_date": "",
            }
        normalized = str(code or "").strip()
        info = self._recent_discoveries.get(normalized)
        if not info:
            info = self._lookup_recent_discovery_from_history(trade_date, normalized)
        if not info:
            return {
                "reachable": False,
                "reason": "not_discovered_by_screener",
                "sources": [],
                "last_seen_date": "",
            }
        last_seen = str(info.get("last_seen_date") or "")
        lookback_days = max(int(self.cfg.reachable_lookback_days or 0), 0)
        try:
            age_days = (date_type.fromisoformat(trade_date) - date_type.fromisoformat(last_seen)).days
        except ValueError:
            age_days = lookback_days + 1
        if age_days < 0 or age_days > lookback_days:
            refreshed_info = self._lookup_recent_discovery_from_history(trade_date, normalized)
            if refreshed_info:
                last_seen = str(refreshed_info.get("last_seen_date") or "")
                try:
                    age_days = (date_type.fromisoformat(trade_date) - date_type.fromisoformat(last_seen)).days
                except ValueError:
                    age_days = lookback_days + 1
                if 0 <= age_days <= lookback_days:
                    return {
                        "reachable": True,
                        "reason": "discovered_by_screener",
                        "sources": list(refreshed_info.get("sources") or []),
                        "last_seen_date": last_seen,
                    }
            return {
                "reachable": False,
                "reason": "discovery_stale",
                "sources": list(info.get("sources") or []),
                "last_seen_date": last_seen,
            }
        return {
            "reachable": True,
            "reason": "discovered_by_screener",
            "sources": list(info.get("sources") or []),
            "last_seen_date": last_seen,
        }

    def _lookup_recent_discovery_from_history(self, trade_date: str, code: str) -> dict[str, Any] | None:
        """按需查询历史发现池，避免全周期预加载大体量 pool JSON。"""
        normalized = str(code or "").strip()
        if self._history_conn is None or not normalized:
            return None
        lookback_days = max(int(self.cfg.reachable_lookback_days or 0), 0)
        try:
            end_date = date_type.fromisoformat(trade_date)
        except ValueError:
            return None
        start_date = (end_date - timedelta(days=lookback_days)).isoformat()
        rows = self._history_conn.execute(
            """SELECT snapshot_date, source
               FROM signal_history_discoveries
               WHERE code = ?
                 AND snapshot_date >= ?
                 AND snapshot_date <= ?
               ORDER BY snapshot_date DESC, source""",
            (normalized, start_date, trade_date),
        ).fetchall()
        latest_date = ""
        sources: set[str] = set()
        for row in rows:
            item = dict(getattr(row, "_mapping", row))
            snapshot_date = str(item.get("snapshot_date") or "")
            if not snapshot_date:
                continue
            if not latest_date:
                latest_date = snapshot_date
            if snapshot_date != latest_date:
                continue
            source = str(item.get("source") or "")
            if source in {"pool", "candidates", "decision"}:
                sources.add(source)
        if not latest_date:
            return None
        info = {
            "last_seen_date": latest_date,
            "sources": sorted(sources),
        }
        self._recent_discoveries[normalized] = info
        return info

    def _record_reachability_check(self, status: dict[str, Any], code: str) -> None:
        if not self.cfg.require_reachable_candidate_for_buy:
            return
        self._discovery_reachability["candidate_checks"] += 1
        if status.get("reachable"):
            self._discovery_reachability["reachable_candidates"] += 1
            for source in status.get("sources") or ["unknown"]:
                _counter_inc(self._discovery_reachability["discovery_sources"], str(source))
            return
        self._discovery_reachability["blocked_candidates"] += 1
        _counter_inc(self._discovery_reachability["blocked_reasons"], str(status.get("reason") or "unknown"))
        _counter_inc(self._discovery_reachability["blocked_codes"], str(code or "unknown"))

    def _preload_history_mirror_cache(self) -> None:
        """小日期集批量预载历史信号镜像；长周期避免 MySQL 大 IN 查询。"""
        self._history_mirror_cache = None
        if self._history_conn is None or not self._sorted_dates:
            return
        if len(self._sorted_dates) > 250:
            return
        from astock_trading.platform.history_mirror import load_signal_history_bundles

        self._history_mirror_cache = load_signal_history_bundles(
            self._history_conn,
            snapshot_dates=self._sorted_dates,
            phases=("historical_discovery", "screener", "scoring"),
        )

    def _buy_trade_record(
        self,
        *,
        trade_date: str,
        signal_date: str | None = None,
        score: Any,
        intent: Any,
        market: "MarketState",
        price: float,
        shares: int,
        position_pct: float,
    ) -> dict[str, Any]:
        market_context = _market_context_payload(market)
        action = str(getattr(getattr(intent, "action", None), "value", "") or "UNKNOWN")
        return {
            "date": trade_date,
            "signal_date": signal_date or trade_date,
            "execution_date": trade_date,
            "code": getattr(score, "code", ""),
            "name": getattr(score, "name", ""),
            "side": "buy",
            "price": price,
            "shares": shares,
            "score": round(float(getattr(score, "total", 0.0) or 0.0), 1),
            "source_action": action,
            "source_route": _score_route_label(score),
            "position_pct": round(position_pct, 4),
            "market_signal": _market_signal_value(market),
            "market_phase_bucket": market_context["market_phase_bucket"],
            "market_context": market_context,
            "pnl": 0,
            "return_pct": 0,
        }

    def _execution_funnel_bucket(self, signal: str, route: str) -> dict[str, Any]:
        key = f"{signal}:{route}"
        bucket = self._execution_funnel["by_market_route"].setdefault(
            key,
            {
                "signals": 0,
                "entry_signal_total": 0,
                "executable_candidates": 0,
                "executed_buys": 0,
                "actions": {},
                "executable_actions": {},
                "executed_actions": {},
                "skipped_actions": {},
                "skip_reasons": {},
                "decision_reasons": {},
                "veto_reasons": {},
            },
        )
        return bucket

    def _intent_executable_for_backtest(
        self,
        intent: Any,
        code: str,
        route: str | None = None,
        *,
        score_total: float | None = None,
        market: "MarketState" | None = None,
    ) -> bool:
        return bool(
            self._intent_execution_status(
                intent,
                code,
                route,
                score_total=score_total,
                market=market,
            )["executable"]
        )

    def _intent_execution_status(
        self,
        intent: Any,
        code: str,
        route: str | None = None,
        *,
        score_total: float | None = None,
        market: "MarketState" | None = None,
    ) -> dict[str, Any]:
        action = str(getattr(getattr(intent, "action", None), "value", "") or "")
        if action == "BUY":
            phase_status = self._buy_phase_status(market)
            if phase_status:
                return {"executable": False, "reason": phase_status}
            return {"executable": True, "reason": "buy"}
        signal = str(getattr(getattr(intent, "market_signal", None), "value", "") or "")
        route_name = str(route or "unknown")
        total = float(score_total if score_total is not None else getattr(intent, "score", 0.0) or 0.0)

        if action == "TRIAL_BUY":
            cooldown_status = self._watch_loss_cooldown_status(market)
            if cooldown_status:
                return {"executable": False, "reason": cooldown_status}
            allowed_routes = {str(item) for item in self.cfg.execute_trial_buy_routes if str(item)}
            if allowed_routes and route_name not in allowed_routes:
                return {"executable": False, "reason": "trial_route_not_allowed"}
            if signal in set(self.cfg.execute_trial_buy_market_signals or ()):
                return (
                    {"executable": False, "reason": "already_held"}
                    if code in self._positions
                    else {"executable": True, "reason": "trial_buy_market"}
                )
            policy = self._route_execution_policy(
                signal,
                route_name,
                action=action,
                score_total=total,
                market=market,
            )
            if policy and self._route_policy_allows_action(policy, action):
                score_min = float(policy.get("score_min", self.cfg.execute_watch_trial_score_min or 0.0) or 0.0)
                if total < score_min:
                    return {"executable": False, "reason": "route_policy_score_min"}
                return (
                    {"executable": False, "reason": "already_held"}
                    if code in self._positions
                    else {"executable": True, "reason": "route_policy_trial_buy"}
                )
            if policy:
                return {"executable": False, "reason": "route_policy_action_not_allowed"}
            return {"executable": False, "reason": "trial_buy_not_enabled"}

        if action != "WATCH":
            return {"executable": False, "reason": "action_not_executable"}

        cooldown_status = self._watch_loss_cooldown_status(market)
        if cooldown_status:
            return {"executable": False, "reason": cooldown_status}

        allowed_pairs = {str(item).strip() for item in self.cfg.execute_watch_trial_pairs if str(item).strip()}
        if allowed_pairs:
            if f"{signal}:{route_name}" not in allowed_pairs:
                return {"executable": False, "reason": "watch_pair_not_allowed"}
            score_status = self._watch_trial_score_status(total)
            if score_status:
                return {"executable": False, "reason": score_status}
            phase_status = self._watch_trial_phase_status(market)
            if phase_status:
                return {"executable": False, "reason": phase_status}
            return (
                {"executable": False, "reason": "already_held"}
                if code in self._positions
                else {"executable": True, "reason": "watch_trial_pair"}
            )

        policy = self._route_execution_policy(
            signal,
            route_name,
            action=action,
            score_total=total,
            market=market,
        )
        if policy and self._route_policy_allows_action(policy, action):
            score_min = float(policy.get("score_min", self.cfg.execute_watch_trial_score_min or 0.0) or 0.0)
            if total < score_min:
                return {"executable": False, "reason": "route_policy_score_min"}
            score_max = policy.get("score_max", self.cfg.execute_watch_trial_score_max)
            if score_max is not None and total >= float(score_max):
                return {"executable": False, "reason": "route_policy_score_max"}
            phase_status = self._watch_trial_phase_status(market)
            if phase_status:
                return {"executable": False, "reason": phase_status}
            return (
                {"executable": False, "reason": "already_held"}
                if code in self._positions
                else {"executable": True, "reason": "route_policy_watch"}
            )

        if policy:
            return {"executable": False, "reason": "watch_not_enabled"}

        if not (
            self.cfg.execute_watch_trial_market_signals
            or self.cfg.execute_watch_trial_routes
        ):
            return {"executable": False, "reason": "watch_not_enabled"}

        allowed_watch_signals = set(self.cfg.execute_watch_trial_market_signals or ())
        if allowed_watch_signals and signal not in allowed_watch_signals:
            return {"executable": False, "reason": "watch_market_not_allowed"}

        allowed_watch_routes = {str(item) for item in self.cfg.execute_watch_trial_routes if str(item)}
        if allowed_watch_routes and route_name not in allowed_watch_routes:
            return {"executable": False, "reason": "watch_route_not_allowed"}

        score_status = self._watch_trial_score_status(total)
        if score_status:
            return {"executable": False, "reason": score_status}
        phase_status = self._watch_trial_phase_status(market)
        if phase_status:
            return {"executable": False, "reason": phase_status}

        return (
            {"executable": False, "reason": "already_held"}
            if code in self._positions
            else {"executable": True, "reason": "watch_trial_market_route"}
        )

    def _watch_trial_phase_status(self, market: "MarketState" | None) -> str:
        allowed_phases = {str(item) for item in self.cfg.execute_watch_trial_phase_buckets if str(item)}
        min_above_days = int(self.cfg.execute_watch_trial_min_above_ma20_days or 0)
        min_above_phases = {
            str(item) for item in self.cfg.execute_watch_trial_min_above_ma20_days_phase_buckets if str(item)
        }
        require_ma60_phases = {
            str(item) for item in self.cfg.execute_watch_trial_require_above_ma60_phase_buckets if str(item)
        }
        require_ma120_phases = {
            str(item) for item in self.cfg.execute_watch_trial_require_above_ma120_phase_buckets if str(item)
        }
        if not allowed_phases and min_above_days <= 0 and not require_ma60_phases and not require_ma120_phases:
            return ""
        if market is None:
            return "watch_phase_missing"
        market_context = _market_context_payload(market)
        phase = market_context.get("market_phase_bucket")
        if allowed_phases and phase not in allowed_phases:
            return "watch_phase_not_allowed"
        if min_above_days > 0 and (not min_above_phases or phase in min_above_phases):
            above_days = int(market_context.get("above_ma20_days") or 0)
            if above_days < min_above_days:
                return "watch_above_ma20_days_below_min"
        if require_ma60_phases and phase in require_ma60_phases:
            above_ma60 = market_context.get("above_ma60")
            if above_ma60 is None:
                price = market_context.get("price")
                ma60 = market_context.get("ma60")
                try:
                    above_ma60 = float(price) > float(ma60) > 0
                except (TypeError, ValueError):
                    above_ma60 = False
            if not above_ma60:
                return "watch_above_ma60_required"
        if require_ma120_phases and phase in require_ma120_phases:
            above_ma120 = market_context.get("above_ma120")
            if above_ma120 is None:
                price = market_context.get("price")
                ma120 = market_context.get("ma120")
                try:
                    above_ma120 = float(price) > float(ma120) > 0
                except (TypeError, ValueError):
                    above_ma120 = False
            if not above_ma120:
                return "watch_above_ma120_required"
        return ""

    def _buy_phase_status(self, market: "MarketState" | None) -> str:
        allowed_phases = {str(item) for item in self.cfg.execute_buy_phase_buckets if str(item)}
        if not allowed_phases:
            return ""
        if market is None:
            return "buy_phase_missing"
        phase = _market_context_payload(market).get("market_phase_bucket")
        if phase not in allowed_phases:
            return "buy_phase_not_allowed"
        return ""

    def _watch_trial_score_status(self, score_total: float) -> str:
        if score_total < float(self.cfg.execute_watch_trial_score_min or 0.0):
            return "watch_score_below_min"
        score_max = self.cfg.execute_watch_trial_score_max
        if score_max is not None and score_total >= float(score_max):
            return "watch_score_above_max"
        return ""

    def _execution_position_pct(self, intent: Any, market: "MarketState", route: str | None = None) -> float:
        action = str(getattr(getattr(intent, "action", None), "value", "") or "")
        signal = str(getattr(getattr(market, "signal", None), "value", "") or "")
        policy = self._route_execution_policy(
            signal,
            route,
            action=action,
            score_total=float(getattr(intent, "score", 0.0) or 0.0),
            market=market,
        )
        if (
            action in {"BUY", "WATCH", "TRIAL_BUY"}
            and policy
            and self._route_policy_allows_action(policy, action)
            and policy.get("position_pct") is not None
        ):
            return round(min(float(policy.get("position_pct") or 0.0), self.cfg.single_max_pct), 4)
        if action == "BUY":
            pct = float(getattr(intent, "position_pct", 0.0) or 0.0)
            if pct <= 0:
                pct = self.cfg.single_max_pct * float(getattr(market, "multiplier", 0.0) or 0.0)
            return round(min(pct, self.cfg.single_max_pct), 4)
        if action == "TRIAL_BUY" and signal in set(self.cfg.execute_trial_buy_market_signals or ()):
            return round(self.cfg.single_max_pct * float(getattr(market, "multiplier", 0.0) or 0.0), 4)
        if action == "WATCH" and (
            self.cfg.execute_watch_trial_pairs
            or signal in set(self.cfg.execute_watch_trial_market_signals or ())
        ):
            if self.cfg.execute_watch_trial_position_pct is not None:
                return round(min(float(self.cfg.execute_watch_trial_position_pct or 0.0), self.cfg.single_max_pct), 4)
            return round(self.cfg.single_max_pct * float(getattr(market, "multiplier", 0.0) or 0.0), 4)
        return 0.0

    def _route_execution_policy(
        self,
        signal: str,
        route: str | None,
        *,
        action: str | None = None,
        score_total: float | None = None,
        market: "MarketState" | None = None,
    ) -> dict:
        for _key, policy in iter_route_policy_entries(self.cfg.route_execution_policy or {}, signal, route):
            if action is not None and not self._route_policy_allows_action(policy, action):
                continue
            if score_total is not None and not self._route_policy_matches_score(policy, score_total):
                continue
            if market is not None and not self._route_policy_matches_phase(policy, market):
                continue
            return policy
        return {}

    def _route_policy_allows_action(self, policy: dict, action: str) -> bool:
        if not policy:
            return False
        actions = policy.get("actions")
        if actions is None:
            return action == "BUY"
        allowed = {str(item) for item in actions or () if str(item)}
        return action in allowed

    def _route_policy_matches_score(self, policy: dict, score_total: float) -> bool:
        try:
            score = float(score_total or 0.0)
        except (TypeError, ValueError):
            score = 0.0
        score_min = policy.get("score_min")
        if score_min is not None and score < float(score_min or 0.0):
            return False
        score_max = policy.get("score_max")
        if score_max is not None and score > float(score_max or 0.0):
            return False
        return True

    def _route_policy_matches_phase(self, policy: dict, market: "MarketState") -> bool:
        market_context = _market_context_payload(market)
        if policy.get("require_above_ma120") is not None:
            if bool(market_context.get("above_ma120")) is not bool(policy.get("require_above_ma120")):
                return False
        min_ma120_slope = policy.get("min_index_ma120_slope_20d_pct")
        if min_ma120_slope is not None:
            slope = market_context.get("index_ma120_slope_20d_pct")
            if slope is None or float(slope) < float(min_ma120_slope):
                return False
        phases = {str(item) for item in policy.get("phase_buckets", []) or [] if str(item)}
        if not phases:
            return True
        return market_context.get("market_phase_bucket") in phases

    def _buy_candidate_sort_key(
        self,
        intent: Any,
        route: str | None,
        market: "MarketState",
        *,
        score_total: float,
    ) -> tuple[float, float, float]:
        action = str(getattr(getattr(intent, "action", None), "value", "") or "")
        signal = str(getattr(getattr(market, "signal", None), "value", "") or "")
        action_priority = {"BUY": 1000.0, "TRIAL_BUY": 500.0, "WATCH": 0.0}.get(action, -1000.0)
        policy = self._route_execution_policy(
            signal,
            route,
            action=action,
            score_total=score_total,
            market=market,
        )
        route_priority = float(policy.get("priority", 0.0) or 0.0) if self._route_policy_allows_action(policy, action) else 0.0
        return (action_priority + route_priority, float(score_total or 0.0), float(getattr(intent, "confidence", 0.0) or 0.0))

    def _weekly_max_for_market(self, market: "MarketState") -> int:
        signal = str(getattr(getattr(market, "signal", None), "value", "") or "")
        override = (self.cfg.weekly_max_by_market or {}).get(signal)
        if override is None:
            return int(self.cfg.weekly_max)
        return int(override)

    def _should_reduce_position_for_market(self, pos: Position, market: "MarketState") -> bool:
        if pos.market_reduce_exempt:
            return False
        return self._should_market_reduce_position(market) and not pos.market_reduced

    def _positions_market_value(self, trade_date: str, *, price_field: str = "收盘") -> float:
        value = 0.0
        for code, pos in self._positions.items():
            close = self._price_on_or_before(self._bars.get(code), trade_date, field=price_field)
            if close is None or close <= 0:
                continue
            value += close * float(pos.shares or 0.0)
        return value

    def _allocation_budget_for_position(self, trade_date: str, position_pct: float, *, price_field: str = "收盘") -> float:
        position_value = self._positions_market_value(trade_date, price_field=price_field)
        portfolio_value = self._cash + position_value
        if portfolio_value <= 0:
            return 0.0
        single_budget = portfolio_value * max(0.0, float(position_pct or 0.0))
        total_budget = portfolio_value * float(self.cfg.total_max_pct or 0.0)
        remaining_total_budget = max(0.0, total_budget - position_value)
        return round(max(0.0, min(self._cash, single_budget, remaining_total_budget)), 2)

    def _position_market_value(self, trade_date: str, code: str, *, price_field: str = "收盘") -> float:
        pos = self._positions.get(code)
        if pos is None:
            return 0.0
        close = self._price_on_or_before(self._bars.get(code), trade_date, field=price_field)
        if close is None or close <= 0:
            return 0.0
        return close * float(pos.shares or 0.0)

    def _allocation_budget_to_target_position(
        self,
        trade_date: str,
        code: str,
        target_pct: float,
        *,
        price_field: str = "收盘",
    ) -> float:
        position_value = self._positions_market_value(trade_date, price_field=price_field)
        portfolio_value = self._cash + position_value
        if portfolio_value <= 0:
            return 0.0
        current_value = self._position_market_value(trade_date, code, price_field=price_field)
        target_value = portfolio_value * max(0.0, float(target_pct or 0.0))
        add_budget = max(0.0, target_value - current_value)
        total_budget = portfolio_value * float(self.cfg.total_max_pct or 0.0)
        remaining_total_budget = max(0.0, total_budget - position_value)
        return round(max(0.0, min(self._cash, add_budget, remaining_total_budget)), 2)

    def _scale_in_positions(
        self,
        *,
        trade_date: str,
        day_index: int,
        intents: list[tuple[Any, Any]],
        market: "MarketState",
    ) -> None:
        if not self.cfg.scale_in_enabled:
            return
        if self._cash <= self.cfg.initial_cash * 0.05:
            return

        held_candidates: list[tuple[Any, Any]] = []
        allowed_actions = {str(item) for item in self.cfg.scale_in_actions if str(item)}
        for score, intent in intents:
            if getattr(score, "code", "") not in self._positions:
                continue
            action = str(getattr(getattr(intent, "action", None), "value", "") or "")
            if allowed_actions and action not in allowed_actions:
                continue
            held_candidates.append((score, intent))

        held_candidates.sort(
            key=lambda x: self._buy_candidate_sort_key(
                x[1],
                getattr(x[0], "primary_strategy_route", None),
                market,
                score_total=x[0].total,
            ),
            reverse=True,
        )

        for score, intent in held_candidates:
            if (
                self._weekly_buy_count + self._pending_buy_order_count()
                >= self._weekly_max_for_market(market)
            ):
                break
            code = getattr(score, "code", "")
            df = self._bars.get(code)
            if df is None:
                continue
            row = df[df["日期"] == trade_date]
            if row.empty:
                continue
            price = float(row["收盘"].iloc[0])
            pnl_price = self._pnl_price_on(code, trade_date) or price
            status = self._scale_in_execution_status(score, market, price=pnl_price, day_index=day_index)
            if not status["executable"]:
                continue
            target_pct = self._scale_in_target_position_pct(trade_date, code, score=score, market=market)
            execution_date = self._next_trade_date(trade_date)
            if not execution_date:
                continue
            if self._has_pending_buy_order(code):
                continue
            self._pending_buy_orders.append(PendingBuyOrder(
                signal_date=trade_date,
                execution_date=execution_date,
                score=score,
                intent=intent,
                market=market,
                position_pct=target_pct,
                kind="scale_in",
            ))
            self._execution_constraints["buy_orders_scheduled"] += 1

    def _scale_in_execution_status(
        self,
        score: Any,
        market: "MarketState",
        *,
        price: float,
        day_index: int,
    ) -> dict[str, Any]:
        if not self.cfg.scale_in_enabled:
            return {"executable": False, "reason": "scale_in_disabled"}
        code = getattr(score, "code", "")
        pos = self._positions.get(code)
        if pos is None:
            return {"executable": False, "reason": "scale_in_no_position"}
        if pos.pending_exit_reason:
            return {"executable": False, "reason": "scale_in_pending_exit"}
        signal = _market_signal_value(market)
        allowed_signals = {str(item) for item in self.cfg.scale_in_market_signals if str(item)}
        if allowed_signals and signal not in allowed_signals:
            return {"executable": False, "reason": "scale_in_market_blocked"}
        route = str(getattr(score, "primary_strategy_route", "") or "unknown")
        allowed_routes = {str(item) for item in self.cfg.scale_in_routes if str(item)}
        if allowed_routes and route not in allowed_routes:
            return {"executable": False, "reason": "scale_in_route_blocked"}
        if float(getattr(score, "total", 0.0) or 0.0) < float(self.cfg.scale_in_score_min or 0.0):
            return {"executable": False, "reason": "scale_in_score_below_min"}
        if self.cfg.scale_in_require_entry_signal and not bool(getattr(score, "entry_signal", False)):
            return {"executable": False, "reason": "scale_in_entry_signal_missing"}
        if pos.add_count >= int(self.cfg.scale_in_max_adds or 0):
            return {"executable": False, "reason": "scale_in_max_adds"}
        if pos.last_add_date and pos.last_add_date in self._sorted_dates:
            last_index = self._sorted_dates.index(pos.last_add_date)
            if day_index - last_index < int(self.cfg.scale_in_min_days_between or 0):
                return {"executable": False, "reason": "scale_in_too_soon"}
        entry_price = float(pos.pnl_entry_price or pos.entry_price or 0.0)
        if price <= 0 or entry_price <= 0:
            return {"executable": False, "reason": "scale_in_missing_price"}
        unrealized_return = (price - entry_price) / entry_price
        if unrealized_return < float(self.cfg.scale_in_profit_threshold or 0.0):
            return {"executable": False, "reason": "scale_in_profit_below_threshold"}
        trade_date = self._sorted_dates[day_index] if 0 <= day_index < len(self._sorted_dates) else ""
        target_pct = self._scale_in_target_position_pct(trade_date, code, score=score, market=market)
        current_pct = self._current_position_pct(trade_date, code)
        if trade_date and target_pct <= current_pct:
            return {"executable": False, "reason": "scale_in_target_reached"}
        return {"executable": True, "reason": "scale_in_trend_confirmed"}

    def _scale_in_target_position_pct(
        self,
        trade_date: str,
        code: str,
        *,
        score: Any | None = None,
        market: "MarketState" | None = None,
    ) -> float:
        pos = self._positions.get(code)
        if pos is None:
            return 0.0
        max_pct, step_pct = self._scale_in_position_limits(score=score, market=market)
        if max_pct is None:
            max_pct = self.cfg.scale_in_max_position_pct
        if max_pct is None:
            max_pct = self.cfg.single_max_pct
        base_pct = max(float(pos.position_pct or 0.0), self._current_position_pct(trade_date, code))
        if base_pct <= 0:
            base_pct = float(self.cfg.single_max_pct or 0.0)
        target_pct = base_pct + float(step_pct or 0.0)
        return round(min(float(max_pct or 0.0), target_pct), 4)

    def _scale_in_position_limits(
        self,
        *,
        score: Any | None,
        market: "MarketState" | None,
    ) -> tuple[float | None, float]:
        if self._scale_in_aggressive_context(score=score, market=market):
            max_pct = self.cfg.scale_in_aggressive_max_position_pct
            step_pct = self.cfg.scale_in_aggressive_step_position_pct
            if max_pct is not None:
                return max_pct, float(step_pct if step_pct is not None else self.cfg.scale_in_step_position_pct)
        return self.cfg.scale_in_max_position_pct, float(self.cfg.scale_in_step_position_pct or 0.0)

    def _scale_in_aggressive_context(self, *, score: Any | None, market: "MarketState" | None) -> bool:
        if score is None or market is None:
            return False
        if self.cfg.scale_in_aggressive_max_position_pct is None:
            return False
        signal = _market_signal_value(market)
        allowed_signals = {str(item) for item in self.cfg.scale_in_aggressive_market_signals if str(item)}
        if allowed_signals and signal not in allowed_signals:
            return False
        route = str(getattr(score, "primary_strategy_route", "") or "unknown")
        allowed_routes = {str(item) for item in self.cfg.scale_in_aggressive_routes if str(item)}
        if allowed_routes and route not in allowed_routes:
            return False
        allowed_phases = {str(item) for item in self.cfg.scale_in_aggressive_phase_buckets if str(item)}
        if allowed_phases:
            phase = _market_context_payload(market).get("market_phase_bucket")
            if phase not in allowed_phases:
                return False
        return True

    def _current_position_pct(self, trade_date: str, code: str) -> float:
        if not trade_date:
            return 0.0
        position_value = self._positions_market_value(trade_date)
        portfolio_value = self._cash + position_value
        if portfolio_value <= 0:
            return 0.0
        return self._position_market_value(trade_date, code) / portfolio_value

    def _scale_in_trade_record(
        self,
        *,
        trade_date: str,
        signal_date: str | None = None,
        score: Any,
        intent: Any,
        market: "MarketState",
        price: float,
        shares: int,
        position_pct: float,
    ) -> dict[str, Any]:
        trade = self._buy_trade_record(
            trade_date=trade_date,
            signal_date=signal_date or trade_date,
            score=score,
            intent=intent,
            market=market,
            price=price,
            shares=shares,
            position_pct=position_pct,
        )
        trade["source_action"] = "SCALE_IN"
        trade["reason"] = "趋势加仓"
        return trade

    def _mirror_replay_for_date(self, trade_date: str, fallback_market: "MarketState") -> dict | None:
        """优先读取真实历史信号镜像；没有镜像时让调用方回退到 proxy replay。"""
        if self._history_conn is None:
            return None

        from astock_trading.strategy.models import (
            Action,
            DataQuality,
            DecisionIntent,
            ScoreResult,
        )

        if self._history_mirror_cache is not None:
            bundle = self._history_mirror_cache.get(trade_date)
        else:
            from astock_trading.platform.history_mirror import load_signal_history_bundle

            bundle = load_signal_history_bundle(
                self._history_conn,
                snapshot_date=trade_date,
                phases=("historical_discovery", "screener", "scoring"),
            )
        if not bundle:
            return None

        sections = bundle.get("sections") or {}
        market = _market_state_from_history_bundle(sections.get("market") or {}, fallback_market)
        discovery_sources = _history_discovery_sources(sections)
        candidates = {
            str(item.get("code", "")): item
            for item in sections.get("candidates", [])
            if isinstance(item, dict) and item.get("code")
        }
        decisions = {
            str(item.get("code", "")): item
            for item in sections.get("decision", [])
            if isinstance(item, dict) and item.get("code")
        }
        codes = [code for code in sorted(set(candidates) | set(decisions)) if code in self._bars]
        intents = []
        for code in codes:
            candidate = candidates.get(code, {})
            decision = decisions.get(code, {})
            score_value = _history_score_value(candidate, decision)
            score = ScoreResult(
                code=code,
                name=str(candidate.get("name") or decision.get("name") or code),
                total=score_value,
                hard_veto=[str(item) for item in candidate.get("hard_veto_signals", [])],
                veto_triggered=bool(candidate.get("veto_triggered", False)),
                entry_signal=bool(candidate.get("entry_signal", False)),
                data_quality=_data_quality_from_history(candidate.get("data_quality"), DataQuality),
                data_missing_fields=[str(item) for item in candidate.get("data_missing_fields", [])],
            )
            action_value = str(decision.get("action") or "WATCH")
            action = Action(action_value) if action_value in Action._value2member_map_ else Action.WATCH
            intent = DecisionIntent(
                code=score.code,
                name=score.name,
                action=action,
                confidence=float(decision.get("confidence", decision.get("score", score.total)) or 0.0),
                score=score.total,
                position_pct=float(decision.get("position_pct", 0.0) or 0.0),
                market_signal=market.signal,
                market_multiplier=market.multiplier,
                veto_reasons=[str(item) for item in decision.get("veto_reasons", [])],
                notes=[str(item) for item in decision.get("notes", [])],
            )
            intents.append((score, intent))

        return {
            "source": "history_mirror",
            "history_group_id": bundle.get("history_group_id", ""),
            "market": market,
            "intents": intents,
            "has_strategy_intents": bool(candidates or decisions),
            "discovery_sources": discovery_sources,
        }

    def _record_signal_validation_rows(
        self,
        trade_date: str,
        intents: list[tuple[Any, Any]],
        market: "MarketState",
    ) -> None:
        """Record strategy signals with forward returns for later alpha review."""
        for score, intent in intents:
            route = str(getattr(score, "primary_strategy_route", "") or "")
            action = str(getattr(getattr(intent, "action", None), "value", "") or "")
            entry_signal = bool(getattr(score, "entry_signal", False))
            high_score_watch = (
                action == "WATCH"
                and float(getattr(score, "total", 0.0) or 0.0) >= self._signal_validation_score_floor()
            )
            if not (route or entry_signal or action in {"BUY", "TRIAL_BUY"} or high_score_watch):
                continue
            technical_snapshot = _signal_technical_snapshot(score)
            decision_reasons = _decision_reason_keys(intent)
            veto_reasons = _veto_reason_keys(score, intent)
            self._signal_records.append({
                "code": getattr(score, "code", ""),
                "name": getattr(score, "name", ""),
                "signal_date": trade_date,
                "action": action or "WATCH",
                "score": round(float(getattr(score, "total", 0.0) or 0.0), 2),
                "entry_signal": entry_signal,
                "primary_strategy_route": _score_route_label(score),
                "decision_reasons": decision_reasons,
                "veto_reasons": veto_reasons,
                "unknown_bucket": _unknown_signal_bucket(
                    route=route,
                    entry_signal=entry_signal,
                    action=action,
                    technical=technical_snapshot,
                    diagnostics=_route_diagnostics_payload(score),
                ),
                "technical_snapshot": technical_snapshot,
                "route_diagnostics": _route_diagnostics_payload(score),
                "market_signal": getattr(getattr(market, "signal", None), "value", "unknown"),
                "market_context": _market_context_payload(market),
                "forward_returns": self._forward_returns(getattr(score, "code", ""), trade_date),
            })

    def _signal_validation_score_floor(self) -> float:
        gates = self.cfg.decision_gates or {}
        if gates.get("trial_buy_threshold") is not None:
            return float(gates.get("trial_buy_threshold") or 0.0)
        return max(5.0, float(self.cfg.buy_threshold or 6.0) - 0.5)

    def _forward_returns(
        self,
        code: str,
        trade_date: str,
        *,
        horizons: tuple[int, ...] = (5, 10, 20),
    ) -> dict[str, float]:
        df = self._pnl_df_for_code(code)
        if df is None:
            return {}
        current_close = self._price_on(df, trade_date)
        if current_close is None or current_close <= 0:
            return {}
        code_dates = [str(item) for item in df["日期"].tolist()]
        if trade_date not in code_dates:
            return {}
        start_idx = code_dates.index(trade_date)
        returns: dict[str, float] = {}
        for horizon in horizons:
            target_idx = start_idx + horizon
            if target_idx >= len(code_dates):
                continue
            target_close = self._close_on(df, code_dates[target_idx])
            if target_close is None:
                continue
            returns[f"{horizon}d"] = round((target_close - current_close) / current_close, 6)
        return returns

    @staticmethod
    def _close_on(df: pd.DataFrame, trade_date: str) -> float | None:
        return BacktestEngine._price_on(df, trade_date, field="收盘")

    @staticmethod
    def _close_on_or_before(df: pd.DataFrame | None, trade_date: str) -> float | None:
        return BacktestEngine._price_on_or_before(df, trade_date, field="收盘")

    @staticmethod
    def _price_on(df: pd.DataFrame | None, trade_date: str, *, field: str = "收盘") -> float | None:
        if df is None or df.empty:
            return None
        row = df[df["日期"] == trade_date]
        if row.empty:
            return None
        series = row.iloc[0]
        value = series.get(field)
        if value in (None, "") and field != "收盘":
            value = series.get("收盘")
        try:
            price = float(value)
        except (TypeError, ValueError):
            return None
        return price if price > 0 else None

    @staticmethod
    def _price_on_or_before(df: pd.DataFrame | None, trade_date: str, *, field: str = "收盘") -> float | None:
        if df is None or df.empty:
            return None
        rows = df[df["日期"] <= trade_date]
        if rows.empty:
            return None
        row = rows.iloc[-1]
        if str(row.get("日期") or "") == str(trade_date):
            value = row.get(field)
            if value not in (None, ""):
                try:
                    price = float(value)
                    if price > 0:
                        return price
                except (TypeError, ValueError):
                    pass
        try:
            price = float(row.get("收盘"))
        except (TypeError, ValueError):
            return None
        return price if price > 0 else None

    @staticmethod
    def _row_on(df: pd.DataFrame | None, trade_date: str) -> pd.Series | None:
        if df is None or df.empty:
            return None
        row = df[df["日期"] == trade_date]
        if row.empty:
            return None
        return row.iloc[0]

    def _tradeability_status(
        self,
        df: pd.DataFrame | None,
        trade_date: str,
        *,
        side: str,
        code: str = "",
        name: str = "",
        price_field: str = "收盘",
    ) -> dict[str, Any]:
        row = self._row_on(df, trade_date)
        if row is None:
            return {"tradable": False, "reason": "missing_price_row"}
        volume = _safe_float(row.get("成交量"), 1.0)
        if volume <= 0:
            return {"tradable": False, "reason": "zero_volume"}
        close = _safe_float(row.get("收盘"), 0.0)
        open_price = _safe_float(row.get("开盘"), close)
        high = _safe_float(row.get("最高"), close)
        low = _safe_float(row.get("最低"), close)
        if min(open_price, high, low, close) <= 0:
            return {"tradable": False, "reason": "invalid_price"}
        execution_price = _safe_float(row.get(price_field), close)
        if execution_price <= 0:
            return {"tradable": False, "reason": "invalid_price"}
        change_pct = self._row_change_pct(df, trade_date, row, price=execution_price)
        limit_pct = self._limit_pct_for_code(code, name or str(row.get("证券名称", row.get("名称", ""))))
        tolerance = 0.2
        if side == "buy" and change_pct >= limit_pct - tolerance:
            return {"tradable": False, "reason": "limit_up_locked"}
        if side == "sell" and change_pct <= -limit_pct + tolerance:
            return {"tradable": False, "reason": "limit_down_locked"}
        return {"tradable": True, "reason": "tradable"}

    def _row_change_pct(
        self,
        df: pd.DataFrame | None,
        trade_date: str,
        row: pd.Series,
        *,
        price: float | None = None,
    ) -> float:
        close = _safe_float(row.get("收盘"), 0.0)
        if price is None or abs(float(price or 0.0) - close) < 1e-8:
            value = row.get("涨跌幅")
            if value not in (None, ""):
                try:
                    return float(value)
                except (TypeError, ValueError):
                    pass
        execution_price = _safe_float(price, 0.0) if price is not None else close
        if execution_price <= 0:
            return 0.0
        if df is None or df.empty:
            return 0.0
        rows = df[df["日期"] < trade_date].sort_values("日期")
        if rows.empty:
            return 0.0
        prev_close = _safe_float(rows.iloc[-1].get("收盘"), 0.0)
        if prev_close <= 0:
            return 0.0
        return (execution_price / prev_close - 1.0) * 100.0

    @staticmethod
    def _limit_pct_for_code(code: str, name: str = "") -> float:
        normalized = str(code or "")
        display_name = str(name or "").strip().upper().replace(" ", "")
        if display_name.startswith(("*ST", "ST", "退市")):
            return 5.0
        if normalized.startswith(("300", "301", "688", "689", "787")):
            return 20.0
        if normalized.startswith(("8", "4", "92")):
            return 30.0
        return 10.0

    def _trade_costs_for_order(self, *, side: str, price: float, shares: int) -> dict[str, float]:
        side = str(side or "").lower()
        base_price = max(float(price or 0.0), 0.0)
        direction = 1.0 if side == "buy" else -1.0
        execution_price = base_price * (1.0 + direction * float(self.cfg.slippage_bps or 0.0) / 10000.0)
        notional = execution_price * max(int(shares or 0), 0)
        if notional <= 0:
            return {
                "notional": 0.0,
                "commission": 0.0,
                "stamp_tax": 0.0,
                "transfer_fee": 0.0,
                "slippage_cost": 0.0,
                "fee_total": 0.0,
                "total_cost": 0.0,
                "cash_effect": 0.0,
                "execution_price": round(execution_price, 4),
            }
        commission = max(float(self.cfg.min_commission or 0.0), notional * float(self.cfg.commission_bps or 0.0) / 10000.0)
        stamp_tax = notional * float(self.cfg.stamp_tax_bps or 0.0) / 10000.0 if side == "sell" else 0.0
        transfer_fee = notional * float(self.cfg.transfer_fee_bps or 0.0) / 10000.0
        slippage_cost = abs(execution_price - base_price) * max(int(shares or 0), 0)
        fee_total = commission + stamp_tax + transfer_fee
        total_cost = fee_total + slippage_cost
        cash_effect = notional + fee_total if side == "buy" else notional - fee_total
        return {
            "notional": round(notional, 2),
            "commission": round(commission, 2),
            "stamp_tax": round(stamp_tax, 2),
            "transfer_fee": round(transfer_fee, 2),
            "slippage_cost": round(slippage_cost, 2),
            "fee_total": round(fee_total, 2),
            "total_cost": round(total_cost, 2),
            "cash_effect": round(cash_effect, 2),
            "execution_price": round(execution_price, 4),
        }

    def _record_trade_costs(self, costs: dict[str, float]) -> None:
        for key in ("commission", "stamp_tax", "transfer_fee", "fee_total", "slippage_cost", "total_cost"):
            self._cost_totals[key] = round(float(self._cost_totals.get(key, 0.0) or 0.0) + float(costs.get(key, 0.0) or 0.0), 2)

    def _shares_for_buy_budget(self, *, budget: float, price: float) -> int:
        shares = int(max(float(budget or 0.0), 0.0) / max(float(price or 0.0), 0.0001) / 100) * 100
        while shares > 0:
            costs = self._trade_costs_for_order(side="buy", price=price, shares=shares)
            if costs["cash_effect"] <= budget + 1e-6:
                return shares
            shares -= 100
        return 0

    def _record_untradable(self, reason: str, *, side: str) -> None:
        if side == "buy":
            self._execution_constraints["buy_untradable"] += 1
        else:
            self._execution_constraints["sell_untradable"] += 1
        _counter_inc(self._execution_constraints["untradable_reasons"], str(reason or "unknown"))

    def _build_snapshot(self, code: str, as_of_date: str):
        """从历史数据构建 StockSnapshot。"""
        from astock_trading.market.models import (
            FinancialReport, FundFlow, SentimentData,
            StockQuote, StockSnapshot, TechnicalIndicators,
        )

        df = self._bars.get(code)
        if df is None:
            return None

        hist = df[df["日期"] <= as_of_date].copy()
        if len(hist) < 5:
            return None

        today_row = hist[hist["日期"] == as_of_date]
        if today_row.empty:
            return None
        row = today_row.iloc[0]

        indicators = _compute_indicators(df, as_of_date)
        if indicators is None:
            return None

        tech = TechnicalIndicators(
            ma5=indicators["ma5"],
            ma10=indicators["ma10"],
            ma20=indicators["ma20"],
            ma60=indicators["ma60"],
            above_ma20=indicators["above_ma20"],
            volume_ratio=indicators["volume_ratio"],
            rsi=indicators["rsi"],
            golden_cross=indicators["golden_cross"],
            ma20_slope=indicators["ma20_slope"],
            momentum_5d=indicators["momentum_5d"],
            daily_volatility=indicators["daily_volatility"],
            deviation_rate=indicators["deviation_rate"],
            change_pct=indicators["change_pct"],
        )

        name = str(row.get("证券名称", row.get("名称", code)))
        fin = self._financial_for_date(code, as_of_date)
        return StockSnapshot(
            code=code,
            name=name,
            quote=StockQuote(
                code=code, name=name,
                price=float(row["收盘"]),
                open=float(row["开盘"]),
                high=float(row["最高"]),
                low=float(row["最低"]),
                close=float(row["收盘"]),
                volume=int(float(row["成交量"])),
                amount=float(row.get("成交额", 0)),
                change_pct=indicators["change_pct"],
            ),
            technical=tech,
            # 回测场景：使用截至当日已可用的最近一期财务快照。
            financial=FinancialReport(
                roe=fin.get("roe"),                          # 真实 ROE（百分数，如 12.0）
                roe_3y_ago=fin.get("roe_3y_ago"),
                revenue_growth=fin.get("revenue_growth"),     # 真实增速（百分数）
                net_profit_growth=fin.get("revenue_growth"),
                operating_cash_flow=fin.get("operating_cash_flow", 0.0),
            ),
            flow=FundFlow(
                net_inflow_1d=0,       # 未知，填 0
                net_inflow_5d=0,
                main_force_ratio=0.5, # 未知，填中性 0.5
                northbound_net=0,
                northbound_net_positive=True,  # 假设北向中性偏好
                consecutive_outflow_days=0,
            ),
            sentiment=SentimentData(score=1.5, news_count=0, positive_ratio=0.5),
            kline=hist,
        )

    def _queue_exit(self, pos: Position, *, reason: str, trigger_date: str, shares: int = 0) -> None:
        if pos.pending_exit_reason:
            return
        pos.pending_exit_reason = str(reason or "")
        pos.pending_exit_trigger_date = str(trigger_date or "")
        pos.pending_exit_shares = max(int(shares or 0), 0)
        self._execution_constraints["sell_orders_pending"] += 1

    def _execute_pending_exit(self, code: str, pos: Position, trade_date: str, day_idx: int) -> bool:
        if not pos.pending_exit_reason:
            return False
        if pos.pending_exit_trigger_date == trade_date:
            return False
        df = self._bars.get(code)
        if df is None:
            return False
        if self._sell_block_reason(pos, trade_date):
            self._execution_constraints["t_plus_one_blocked_sells"] += 1
            return False
        tradeability = self._tradeability_status(df, trade_date, side="sell", code=code, price_field="开盘")
        if not tradeability["tradable"]:
            self._record_untradable(tradeability["reason"], side="sell")
            return False
        price = self._price_on(df, trade_date, field="开盘")
        if price is None:
            return False
        reason = pos.pending_exit_reason
        trigger_date = pos.pending_exit_trigger_date or trade_date
        shares = pos.pending_exit_shares or pos.shares
        trade = self._close_position(
            trade_date=trade_date,
            code=code,
            price=price,
            shares=shares,
            reason=reason,
            score=0,
            trigger_date=trigger_date,
        )
        self._trades.append(trade)
        self._register_loss_cooldown(trade, day_idx)
        return True

    def _risk_check(self, d: str, day_idx: int):
        """风控检查：止损/追踪止损/时间止损。"""
        for code, pos in list(self._positions.items()):
            if self._execute_pending_exit(code, pos, d, day_idx):
                continue
            df = self._bars.get(code)
            if df is None:
                continue
            row = self._row_on(df, d)
            if row is None:
                continue

            raw_price = float(row["收盘"])
            pnl_price = self._pnl_price_on(code, d)
            if pnl_price is None and self._uses_separate_pnl_bars():
                self._execution_constraints["pnl_price_missing_risk_skips"] += 1
                continue
            price = pnl_price or raw_price
            entry_price = float(pos.pnl_entry_price or pos.entry_price or 0.0)
            if entry_price <= 0:
                continue
            ret = (price - entry_price) / entry_price

            # 时间止损
            entry_idx = self._sorted_dates.index(pos.entry_date) if pos.entry_date in self._sorted_dates else 0
            days_held = day_idx - entry_idx

            # 追踪止损
            pos.pnl_high_water = max(float(pos.pnl_high_water or pos.high_water or entry_price), price)
            pos.high_water = max(pos.high_water, raw_price)
            trail_drawdown = (price - pos.pnl_high_water) / pos.pnl_high_water if pos.pnl_high_water > 0 else 0.0

            stop_loss_triggered = ret <= -self.cfg.stop_loss
            trail_stop_triggered = trail_drawdown <= -self.cfg.trailing_stop
            time_stop_triggered = days_held >= self.cfg.time_stop_days

            reason = ""
            trigger_date = d
            if stop_loss_triggered or trail_stop_triggered or time_stop_triggered:
                if stop_loss_triggered:
                    reason = "止损"
                elif trail_stop_triggered:
                    reason = "追踪止损"
                else:
                    reason = "到期"

            if not reason:
                continue

            self._execution_constraints["sell_triggers"] += 1
            self._queue_exit(pos, reason=reason, trigger_date=trigger_date, shares=pos.shares)

    @staticmethod
    def _sell_block_reason(pos: Position, trade_date: str) -> str:
        if pos.entry_date == trade_date:
            return "t_plus_one"
        return ""

    def _close_position(
        self,
        *,
        trade_date: str,
        code: str,
        price: float,
        shares: int,
        reason: str,
        score: float,
        trigger_date: str | None = None,
    ) -> dict[str, Any]:
        pos = self._positions[code]
        sell_shares = min(max(int(shares or 0), 0), pos.shares)
        sell_ratio = sell_shares / pos.shares if pos.shares > 0 else 0.0
        pnl_units = float(pos.pnl_units or 0.0)
        sold_pnl_units = pnl_units * sell_ratio if pnl_units > 0 else 0.0
        costs = self._trade_costs_for_order(side="sell", price=price, shares=sell_shares)
        gross_proceeds = costs["notional"]
        net_proceeds = costs["cash_effect"]
        position_cost_basis = float(pos.cost_basis or (pos.entry_price * pos.shares))
        sold_cost_basis = position_cost_basis * (sell_shares / pos.shares) if pos.shares > 0 else 0.0
        pnl = net_proceeds - sold_cost_basis
        self._cash += net_proceeds
        self._record_trade_costs(costs)
        if sell_shares >= pos.shares:
            self._positions.pop(code, None)
        else:
            pos.shares -= sell_shares
            if pnl_units > 0:
                pos.pnl_units = max(0.0, pnl_units - sold_pnl_units)
            pos.cost_basis = max(0.0, position_cost_basis - sold_cost_basis)
            pos.pending_exit_reason = ""
            pos.pending_exit_trigger_date = ""
            pos.pending_exit_shares = 0
        return_pct = pnl / sold_cost_basis * 100 if sold_cost_basis > 0 else 0.0
        return {
            "date": trade_date,
            "trigger_date": trigger_date or trade_date,
            "execution_date": trade_date,
            "code": code,
            "side": "sell",
            "price": costs["execution_price"],
            "signal_price": round(float(price or 0.0), 4),
            "shares": sell_shares,
            "entry_price": pos.entry_price,
            "pnl": round(pnl, 2),
            "gross_proceeds": round(gross_proceeds, 2),
            "cost_basis": round(sold_cost_basis, 2),
            "trade_cost": costs,
            "return_pct": round(return_pct, 2),
            "reason": reason,
            "score": round(float(score or 0.0), 1),
            "source_route": pos.entry_route,
            "market_signal": pos.entry_market_signal,
        }

    def _load_financials(self, codes: list[str], start_date: str, end_date: str):
        """从 baostock 拉取区间内可用的季度财务快照并缓存。

        字段来源（均返回小数，如 0.128902 = 12.89%）：
        - roeAvg        → query_profit_data
        - YOYNI         → query_growth_data
        - CFOToOR       → query_cash_flow_data
        """
        if self.cfg.use_financial_cache and not self.cfg.hydrate_financial_cache and self._market_store is not None:
            started_at = time.monotonic()
            self._log_progress("financial_cache_bulk_start", codes=len(codes))
            snapshots_by_code = self._market_store.get_financial_snapshots_bulk(
                codes,
                end_available=end_date,
                source="baostock",
            )
            snapshot_count = 0
            for code in codes:
                snapshots = sorted(
                    snapshots_by_code.get(code) or [],
                    key=lambda item: (
                        str(item.get("available_date", "")),
                        int(item.get("report_year", 0) or 0),
                        int(item.get("report_quarter", 0) or 0),
                    ),
                )
                self._financial_cache[code] = snapshots
                snapshot_count += len(snapshots)
            self._log_progress(
                "financial_cache_bulk_done",
                codes=len(codes),
                snapshots=snapshot_count,
                seconds=round(time.monotonic() - started_at, 2),
            )
            return

        import baostock as bs

        periods = _financial_periods(start_date, end_date)

        def _fetch(rs):
            rows, fields = [], []
            while rs.next():
                if not fields:
                    fields = list(rs.fields)
                rows.append(list(rs.get_row_data()))
            return pd.DataFrame(rows, columns=fields) if rows else pd.DataFrame()

        def _fetch_query(kind: str, code: str, year: int, quarter: int, query_fn) -> tuple[pd.DataFrame, bool]:
            try:
                rs = _call_with_timeout(query_fn, self.cfg.financial_query_timeout_seconds)
                return _fetch(rs), False
            except Exception as exc:
                self._log_progress(
                    "financial_query_failed",
                    code=code,
                    year=year,
                    quarter=quarter,
                    kind=kind,
                    error=exc.__class__.__name__,
                    message=str(exc).replace(" ", "_")[:120],
                )
                if self._market_store is not None:
                    self._market_store.save_data_coverage(
                        domain="financials",
                        symbol=code,
                        start_date=_report_date(year, quarter),
                        end_date=_report_date(year, quarter),
                        source="baostock",
                        period=f"Q{quarter}",
                        row_count=0,
                        status="failed",
                        error={
                            "kind": kind,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc),
                        },
                    )
                    if self._market_conn is not None:
                        self._market_conn.commit()
                return pd.DataFrame(), True

        remote_enabled = not (self.cfg.use_financial_cache and not self.cfg.hydrate_financial_cache)
        if remote_enabled:
            with redirect_stdout(io.StringIO()):
                bs.login()
        try:
            for code in codes:
                started_at = time.monotonic()
                self._log_progress("financial_start", code=code)
                snapshots: list[dict] = []
                bs_code = self._bs_code(code)
                for year, quarter in periods:
                    period_started_at = time.monotonic()
                    self._log_progress("financial_period_start", code=code, year=year, quarter=quarter)
                    cached = self._load_financial_cache(code, year, quarter)
                    if cached is not None:
                        snapshots.append(cached)
                        self._log_progress(
                            "financial_period_cache_hit",
                            code=code,
                            year=year,
                            quarter=quarter,
                            seconds=round(time.monotonic() - period_started_at, 2),
                        )
                        continue
                    if self.cfg.use_financial_cache and not self.cfg.hydrate_financial_cache:
                        self._log_progress(
                            "financial_period_cache_miss",
                            code=code,
                            year=year,
                            quarter=quarter,
                        )
                        continue

                    q_str = str(quarter)
                    roe = None
                    roe_3y_ago = None
                    rev_growth = None
                    ocf = 0.0
                    raw: dict[str, Any] = {}

                    # ROE：来自 query_profit_data 的 roeAvg
                    query_failed = False
                    df, failed = _fetch_query(
                        "profit",
                        code,
                        year,
                        quarter,
                        lambda: bs.query_profit_data(bs_code, year, q_str),
                    )
                    query_failed = query_failed or failed
                    if not df.empty:
                        raw["profit"] = df.iloc[0].to_dict()
                        val = str(df.iloc[0].get("roeAvg", ""))
                        if val and val not in ("", "None"):
                            try:
                                roe = float(val) * 100
                            except ValueError:
                                pass

                    # 净利润增速：YoY
                    df, failed = _fetch_query(
                        "growth",
                        code,
                        year,
                        quarter,
                        lambda: bs.query_growth_data(bs_code, year, q_str),
                    )
                    query_failed = query_failed or failed
                    if not df.empty:
                        raw["growth"] = df.iloc[0].to_dict()
                        val = str(df.iloc[0].get("YOYNI", ""))
                        if val and val not in ("", "None"):
                            try:
                                rev_growth = float(val) * 100
                            except ValueError:
                                pass

                    # 现金流比率
                    df, failed = _fetch_query(
                        "cash_flow",
                        code,
                        year,
                        quarter,
                        lambda: bs.query_cash_flow_data(bs_code, year, q_str),
                    )
                    query_failed = query_failed or failed
                    if not df.empty:
                        raw["cash_flow"] = df.iloc[0].to_dict()
                        val = str(df.iloc[0].get("CFOToOR", ""))
                        if val and val not in ("", "None"):
                            try:
                                ocf = float(val)
                            except ValueError:
                                pass

                    if year > 2003:
                        df, failed = _fetch_query(
                            "profit_3y_ago",
                            code,
                            year,
                            quarter,
                            lambda: bs.query_profit_data(bs_code, year - 3, q_str),
                        )
                        query_failed = query_failed or failed
                        if df.empty:
                            df, failed = _fetch_query(
                                "profit_3y_ago_fallback",
                                code,
                                year,
                                quarter,
                                lambda: bs.query_profit_data(bs_code, year - 3, "4"),
                            )
                            query_failed = query_failed or failed
                        if not df.empty:
                            raw["profit_3y_ago"] = df.iloc[0].to_dict()
                            val = str(df.iloc[0].get("roeAvg", ""))
                            if val and val not in ("", "None"):
                                try:
                                    roe_3y_ago = float(val) * 100
                                except ValueError:
                                    pass

                    snapshot = {
                        "symbol": code,
                        "report_year": year,
                        "report_quarter": quarter,
                        "report_date": _report_date(year, quarter),
                        "available_date": _available_date(year, quarter),
                        "roe": roe,
                        "roe_3y_ago": roe_3y_ago,
                        "revenue_growth": rev_growth,
                        "net_profit_growth": rev_growth,
                        "operating_cash_flow": ocf,
                    }
                    has_financial_data = any((
                        roe is not None,
                        roe_3y_ago is not None,
                        rev_growth is not None,
                        ocf not in (None, 0.0),
                    ))
                    if has_financial_data or not query_failed:
                        self._save_financial_cache(code, snapshot, raw)
                        snapshots.append(snapshot)
                    self._log_progress(
                        "financial_period_done",
                        code=code,
                        year=year,
                        quarter=quarter,
                        has_roe=roe is not None,
                        has_roe_3y=roe_3y_ago is not None,
                        query_failed=query_failed,
                        seconds=round(time.monotonic() - period_started_at, 2),
                    )

                self._financial_cache[code] = sorted(
                    snapshots,
                    key=lambda item: (str(item.get("available_date", "")), int(item.get("report_year", 0)), int(item.get("report_quarter", 0))),
                )
                self._log_progress(
                    "financial_done",
                    code=code,
                    snapshots=len(self._financial_cache.get(code, [])),
                    seconds=round(time.monotonic() - started_at, 2),
                )
        finally:
            if remote_enabled:
                with redirect_stdout(io.StringIO()):
                    bs.logout()

    def _financial_for_date(self, code: str, as_of_date: str) -> dict:
        snapshots = self._financial_cache.get(code) or []
        chosen = None
        for snapshot in snapshots:
            available_date = str(snapshot.get("available_date") or "")
            if available_date and available_date <= as_of_date:
                chosen = snapshot
            elif available_date > as_of_date:
                break
        return dict(chosen or {})

    def _load_financial_cache(self, code: str, year: int, quarter: int) -> dict | None:
        if not self.cfg.use_financial_cache or self._market_store is None:
            return None
        payload = self._market_store.get_financial_snapshot(
            code,
            report_year=year,
            report_quarter=quarter,
            source="baostock",
        )
        if not payload:
            return None
        return {
            "symbol": code,
            "report_year": payload.get("report_year"),
            "report_quarter": payload.get("report_quarter"),
            "report_date": payload.get("report_date"),
            "available_date": payload.get("available_date"),
            "roe": payload.get("roe"),
            "roe_3y_ago": payload.get("roe_3y_ago"),
            "revenue_growth": payload.get("revenue_growth"),
            "net_profit_growth": payload.get("net_profit_growth"),
            "operating_cash_flow": payload.get("operating_cash_flow", 0.0),
        }

    def _save_financial_cache(self, code: str, payload: dict, raw: dict | None = None) -> None:
        if not self.cfg.hydrate_financial_cache or self._market_store is None:
            return
        self._market_store.save_financial_snapshot(
            code,
            report_year=int(payload["report_year"]),
            report_quarter=int(payload["report_quarter"]),
            report_date=str(payload["report_date"]),
            available_date=str(payload["available_date"]),
            payload={**payload, "raw": raw or {}},
            source="baostock",
        )
        if self._market_conn is not None:
            self._market_conn.commit()

    @staticmethod
    def _bs_code(code: str) -> str:
        """将股票代码标准化为 baostock 格式（sh.600036 / sz.000001）。"""
        code = code.strip()
        if "." in code:
            return code.lower()
        if code.startswith(("6", "9")):
            return f"sh.{code}"
        if code.startswith(("0", "3")):
            return f"sz.{code}"
        if code.startswith("8"):
            return f"bj.{code}"
        return f"sh.{code}"

    def _check_week_reset(self, d: str):
        """每周一重置周内买入计数。"""
        week = d[:7]  # YYYY-MM
        if week != self._last_week:
            self._weekly_buy_count = 0
            self._last_week = week

    def _build_report(self) -> dict:
        last_date = self._sorted_dates[-1] if self._sorted_dates else ""

        # 计算最终权益
        final_value = self._cash
        for code, pos in self._positions.items():
            price = self._price_on_or_before(self._bars.get(code), last_date)
            if price is not None:
                final_value += price * float(pos.shares or 0.0)

        total_return = (final_value - self.cfg.initial_cash) / self.cfg.initial_cash * 100
        total_cost = float(self._cost_totals.get("total_cost", 0.0) or 0.0)
        fee_total = float(self._cost_totals.get("fee_total", 0.0) or 0.0)
        before_fees_final_value = final_value + fee_total
        before_fees_return = (before_fees_final_value - self.cfg.initial_cash) / self.cfg.initial_cash * 100
        before_costs_final_value = final_value + total_cost
        before_costs_return = (before_costs_final_value - self.cfg.initial_cash) / self.cfg.initial_cash * 100
        gross_final_value = before_fees_final_value
        gross_return = before_fees_return

        sells = [t for t in self._trades if t["side"] == "sell"]
        wins = [t for t in sells if t.get("pnl", 0) > 0]
        win_rate = len(wins) / max(len(sells), 1) * 100

        # 最大回撤
        equity_series = [e["equity"] for e in self._portfolio_value_series]
        peak, max_dd = equity_series[0] if equity_series else 0, 0.0
        for v in equity_series:
            if v > peak:
                peak = v
            dd = (peak - v) / peak * 100 if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

        years = max(len(self._sorted_dates) / 252, 0.01)
        if final_value > 0 and self.cfg.initial_cash > 0:
            ann_return = ((final_value / self.cfg.initial_cash) ** (1 / years) - 1) * 100
        else:
            ann_return = -100.0
        if gross_final_value > 0 and self.cfg.initial_cash > 0:
            gross_ann_return = ((gross_final_value / self.cfg.initial_cash) ** (1 / years) - 1) * 100
        else:
            gross_ann_return = -100.0
        if before_fees_final_value > 0 and self.cfg.initial_cash > 0:
            before_fees_ann_return = ((before_fees_final_value / self.cfg.initial_cash) ** (1 / years) - 1) * 100
        else:
            before_fees_ann_return = -100.0
        if before_costs_final_value > 0 and self.cfg.initial_cash > 0:
            before_costs_ann_return = ((before_costs_final_value / self.cfg.initial_cash) ** (1 / years) - 1) * 100
        else:
            before_costs_ann_return = -100.0

        # 夏普比率：净值日收益均值 / 日波动，rf 目前按 0 处理。
        if len(equity_series) > 2:
            rets = pd.Series(equity_series).pct_change().dropna()
            ann_ret = rets.mean() * 252 if len(rets) > 0 else 0
            ann_vol = rets.std() * math.sqrt(252) if len(rets) > 1 else 1
            sharpe = (ann_ret / ann_vol) if ann_vol > 0 else 0
        else:
            sharpe = 0.0

        calmar = ann_return / max_dd if max_dd > 0 else 0.0
        unknown_route_signals = [
            item for item in self._signal_records
            if item.get("primary_strategy_route") == "unknown"
        ]
        no_entry_route_signals = [
            item for item in self._signal_records
            if item.get("primary_strategy_route") == "no_entry_route"
        ]
        generic_entry_signal_signals = [
            item for item in self._signal_records
            if item.get("primary_strategy_route") == "generic_entry_signal_watch"
        ]
        if self.cfg.signal_record_limit is None:
            signal_rows = list(self._signal_records)
        elif self.cfg.signal_record_limit <= 0:
            signal_rows = []
        else:
            signal_rows = self._signal_records[-int(self.cfg.signal_record_limit):]
        if self.cfg.trade_record_limit is None:
            trade_rows = list(self._trades)
        elif self.cfg.trade_record_limit <= 0:
            trade_rows = []
        else:
            trade_rows = self._trades[-int(self.cfg.trade_record_limit):]

        return {
            "preset": self.cfg.preset_name,
            "initial_cash": self.cfg.initial_cash,
            "final_value": round(final_value, 2),
            "before_fees_final_value": round(before_fees_final_value, 2),
            "before_costs_final_value": round(before_costs_final_value, 2),
            "gross_final_value": round(gross_final_value, 2),
            "total_return_pct": round(total_return, 2),
            "before_fees_total_return_pct": round(before_fees_return, 2),
            "before_costs_total_return_pct": round(before_costs_return, 2),
            "gross_total_return_pct": round(gross_return, 2),
            "annual_return_pct": round(ann_return, 2),
            "before_fees_annual_return_pct": round(before_fees_ann_return, 2),
            "before_costs_annual_return_pct": round(before_costs_ann_return, 2),
            "gross_annual_return_pct": round(gross_ann_return, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "win_rate_pct": round(win_rate, 1),
            "sharpe_ratio": round(sharpe, 2),
            "calmar_ratio": round(calmar, 2),
            "score_dimension_mode": self.cfg.score_dimension_mode,
            "total_trades": len(self._trades),
            "buy_trades": len([t for t in self._trades if t["side"] == "buy"]),
            "sell_trades": len(sells),
            "winning_trades": len(wins),
            "losing_trades": len(sells) - len(wins),
            "positions_open": len(self._positions),
            "signal_source": {
                "history_mirror_days": len(self._history_mirror_dates),
                "proxy_replay_days": len(self._proxy_replay_dates),
            },
            "discovery_reachability": self._discovery_reachability_summary(),
            "data_coverage": {
                "requested_codes": len(self._requested_codes),
                "loaded_codes": len(self._loaded_codes),
                "loaded_code_list": self._loaded_codes,
                "pnl_bar_coverage": self._pnl_bar_coverage_summary(),
            },
            "execution_semantics": self._execution_semantics(),
            "execution_constraints": self._execution_constraints_summary(),
            "cost_model": self._cost_model_summary(),
            "trade_performance": self._trade_performance_summary(),
            "performance_metric_notes": {
                "annual_return_pct": "CAGR based on net equity curve final value and trading_days/252.",
                "sharpe_ratio": "Uses arithmetic daily net returns annualized by 252 with rf=0.",
                "gross_total_return_pct": "Alias of before_fees_total_return_pct; slippage remains reflected in execution prices.",
                "before_costs_total_return_pct": "Theoretical reference with fees and slippage added back.",
            },
            "execution_funnel": self._execution_funnel,
            "signal_alpha": (
                signal_alpha_summary(self._signal_records, signal_slices=self.cfg.signal_slices)
                if self.cfg.include_signal_alpha
                else {"skipped": True, "sample_size": len(self._signal_records)}
            ),
            "signal_validation": {
                "sample_size": len(self._signal_records),
                "signals": signal_rows,
                "unknown_route_count": len(unknown_route_signals),
                "unknown_route_samples": unknown_route_signals[-20:],
                "no_entry_route_count": len(no_entry_route_signals),
                "no_entry_route_samples": no_entry_route_signals[-20:],
                "generic_entry_signal_count": len(generic_entry_signal_signals),
                "generic_entry_signal_samples": generic_entry_signal_signals[-20:],
            },
            "equity_curve": self._portfolio_value_series,
            "trade_log": self._trades,
            "trades": trade_rows,
        }

    def _execution_semantics(self) -> dict[str, Any]:
        policy_values = list(route_policy_values(self.cfg.route_execution_policy or {}))
        policy_watch_enabled = any(
            "WATCH" in {str(item) for item in (policy.get("actions") or [])}
            for policy in policy_values
        )
        policy_trial_enabled = any(
            "TRIAL_BUY" in {str(item) for item in (policy.get("actions") or [])}
            for policy in policy_values
        )
        watch_trial_enabled = bool(
            self.cfg.execute_watch_trial_pairs
            or self.cfg.execute_watch_trial_market_signals
            or self.cfg.execute_watch_trial_routes
            or policy_watch_enabled
        )
        trial_buy_enabled = bool(
            self.cfg.execute_trial_buy_market_signals
            or self.cfg.execute_trial_buy_routes
            or policy_trial_enabled
        )
        loss_cooldown_enabled = int(self.cfg.watch_loss_cooldown_days or 0) > 0
        scale_in_enabled = bool(self.cfg.scale_in_enabled)
        research_enabled = bool(watch_trial_enabled or trial_buy_enabled or loss_cooldown_enabled)
        notes = [
            "默认只执行正式 BUY；route_execution_policy 可用于 BUY 排序、仓位覆盖和显式路线正式化。",
        ]
        if research_enabled:
            notes.append("本次回测包含显式研究 what-if：允许部分 WATCH/TRIAL_BUY 按规则模拟成交。")
        if loss_cooldown_enabled:
            notes.append("本次回测包含观察层亏损冷却：亏损卖出后暂停观察/试买模拟成交。")
        if scale_in_enabled:
            notes.append("本次回测包含生产支持的趋势加仓：盈利持仓在指定市场制度和路线重新确认后补仓。")
        if self.cfg.require_reachable_candidate_for_buy:
            notes.append("本次回测启用可发现性闸门：买入候选必须在历史候选池、评分候选或决策镜像中出现过。")
        return {
            "mode": "research_what_if" if research_enabled else "production_buy_only",
            "buy_only": not research_enabled,
            "t_plus_one": True,
            "signal_execution_lag": "next_trading_day_open",
            "limit_price_model": "execution_price_near_limit_blocked",
            "cost_model": "commission_stamp_transfer_slippage",
            "pnl_price_model": (
                f"adjustflag_{self.cfg.pnl_adjustflag}"
                if self._uses_separate_pnl_bars()
                else f"signal_adjustflag_{self.cfg.adjustflag}"
            ),
            "reachable_only": bool(self.cfg.require_reachable_candidate_for_buy),
            "reachable_lookback_days": int(self.cfg.reachable_lookback_days or 0),
            "watch_trial_enabled": watch_trial_enabled,
            "trial_buy_enabled": trial_buy_enabled,
            "scale_in_enabled": scale_in_enabled,
            "scale_in_profit_threshold": float(self.cfg.scale_in_profit_threshold or 0.0),
            "scale_in_step_position_pct": float(self.cfg.scale_in_step_position_pct or 0.0),
            "scale_in_max_position_pct": self.cfg.scale_in_max_position_pct,
            "scale_in_max_adds": int(self.cfg.scale_in_max_adds or 0),
            "scale_in_markets": list(self.cfg.scale_in_market_signals or ()),
            "scale_in_routes": list(self.cfg.scale_in_routes or ()),
            "scale_in_actions": list(self.cfg.scale_in_actions or ()),
            "scale_in_require_entry_signal": bool(self.cfg.scale_in_require_entry_signal),
            "scale_in_aggressive_max_position_pct": self.cfg.scale_in_aggressive_max_position_pct,
            "scale_in_aggressive_step_position_pct": self.cfg.scale_in_aggressive_step_position_pct,
            "scale_in_aggressive_markets": list(self.cfg.scale_in_aggressive_market_signals or ()),
            "scale_in_aggressive_routes": list(self.cfg.scale_in_aggressive_routes or ()),
            "scale_in_aggressive_phases": list(self.cfg.scale_in_aggressive_phase_buckets or ()),
            "watch_loss_cooldown_days": int(self.cfg.watch_loss_cooldown_days or 0),
            "watch_loss_cooldown_phases": list(self.cfg.watch_loss_cooldown_phase_buckets or ()),
            "watch_trial_min_above_ma20_days": int(
                self.cfg.execute_watch_trial_min_above_ma20_days or 0
            ),
            "watch_trial_min_above_ma20_days_phases": list(
                self.cfg.execute_watch_trial_min_above_ma20_days_phase_buckets or ()
            ),
            "watch_trial_require_above_ma60_phases": list(
                self.cfg.execute_watch_trial_require_above_ma60_phase_buckets or ()
            ),
            "watch_trial_require_above_ma120_phases": list(
                self.cfg.execute_watch_trial_require_above_ma120_phase_buckets or ()
            ),
            "route_policy_default_actions": ["BUY"],
            "notes": notes,
        }

    def _execution_constraints_summary(self) -> dict[str, Any]:
        return {
            "t_plus_one": True,
            "t_plus_one_blocked_sells": int(self._execution_constraints.get("t_plus_one_blocked_sells") or 0),
            "buy_signals": int(self._execution_constraints.get("buy_signals") or 0),
            "buy_orders_scheduled": int(self._execution_constraints.get("buy_orders_scheduled") or 0),
            "buy_orders_expired": int(self._execution_constraints.get("buy_orders_expired") or 0),
            "buy_untradable": int(self._execution_constraints.get("buy_untradable") or 0),
            "sell_triggers": int(self._execution_constraints.get("sell_triggers") or 0),
            "sell_orders_pending": int(self._execution_constraints.get("sell_orders_pending") or 0),
            "sell_untradable": int(self._execution_constraints.get("sell_untradable") or 0),
            "pnl_price_missing_risk_skips": int(
                self._execution_constraints.get("pnl_price_missing_risk_skips") or 0
            ),
            "untradable_reasons": dict(self._execution_constraints.get("untradable_reasons") or {}),
        }

    def _pnl_bar_coverage_summary(self) -> dict[str, Any]:
        if not self._uses_separate_pnl_bars():
            return {
                "enabled": False,
                "adjustflag": str(self.cfg.adjustflag),
                "pnl_adjustflag": str(self.cfg.pnl_adjustflag or ""),
            }

        checked_codes = 0
        missing_code_count = 0
        missing_date_count = 0
        sample_codes: dict[str, Any] = {}
        for code, raw_df in sorted(self._bars.items()):
            if raw_df is None or raw_df.empty or "日期" not in raw_df.columns:
                continue
            checked_codes += 1
            raw_dates = {str(item) for item in raw_df["日期"].dropna().tolist()}
            pnl_df = self._pnl_bars.get(code)
            pnl_dates = (
                {str(item) for item in pnl_df["日期"].dropna().tolist()}
                if pnl_df is not None and not pnl_df.empty and "日期" in pnl_df.columns
                else set()
            )
            missing_dates = sorted(raw_dates - pnl_dates)
            if not missing_dates:
                continue
            missing_code_count += 1
            missing_date_count += len(missing_dates)
            if len(sample_codes) < 20:
                sample_codes[code] = {
                    "missing_date_count": len(missing_dates),
                    "sample_dates": missing_dates[:5],
                }

        return {
            "enabled": True,
            "adjustflag": str(self.cfg.adjustflag),
            "pnl_adjustflag": str(self.cfg.pnl_adjustflag or ""),
            "checked_codes": checked_codes,
            "pnl_loaded_codes": len(self._pnl_bars),
            "missing_code_count": missing_code_count,
            "missing_date_count": missing_date_count,
            "sample_codes": sample_codes,
        }

    def _cost_model_summary(self) -> dict[str, Any]:
        buy_trades = len([item for item in self._trades if item.get("side") in {"buy", "scale_in"}])
        sell_trades = len([item for item in self._trades if item.get("side") == "sell"])
        total_trades = buy_trades + sell_trades
        total_cost = float(self._cost_totals.get("total_cost", 0.0) or 0.0)
        return {
            "commission_bps": float(self.cfg.commission_bps or 0.0),
            "min_commission": float(self.cfg.min_commission or 0.0),
            "stamp_tax_bps": float(self.cfg.stamp_tax_bps or 0.0),
            "transfer_fee_bps": float(self.cfg.transfer_fee_bps or 0.0),
            "slippage_bps": float(self.cfg.slippage_bps or 0.0),
            "totals": dict(self._cost_totals),
            "avg_cost_per_trade": round(total_cost / total_trades, 2) if total_trades else 0.0,
            "cost_drag_pct": round(total_cost / self.cfg.initial_cash * 100, 4) if self.cfg.initial_cash else 0.0,
            "gross_return_definition": "gross_* equals before-fees return; slippage remains in execution prices.",
            "before_costs_definition": "before_costs_* adds back both fees and slippage as a theoretical zero-cost reference.",
        }

    def _trade_performance_summary(self) -> dict[str, Any]:
        """按真实已平仓交易聚合路线表现，避免把未平仓浮盈算进路线胜率。"""

        def empty_bucket() -> dict[str, Any]:
            return {
                "buy_trades": 0,
                "scale_in_trades": 0,
                "sell_trades": 0,
                "winning_sells": 0,
                "losing_sells": 0,
                "pnl": 0.0,
                "return_pct_sum": 0.0,
                "trade_cost": 0.0,
            }

        def add_trade(target: dict[str, dict[str, Any]], key: str, trade: dict[str, Any]) -> None:
            bucket = target.setdefault(key, empty_bucket())
            side = str(trade.get("side") or "")
            if side == "buy":
                bucket["buy_trades"] += 1
            elif side == "scale_in":
                bucket["scale_in_trades"] += 1
            elif side == "sell":
                bucket["sell_trades"] += 1
                pnl = float(trade.get("pnl") or 0.0)
                return_pct = float(trade.get("return_pct") or 0.0)
                bucket["pnl"] += pnl
                bucket["return_pct_sum"] += return_pct
                if return_pct > 0:
                    bucket["winning_sells"] += 1
                else:
                    bucket["losing_sells"] += 1
            trade_cost = trade.get("trade_cost") or {}
            bucket["trade_cost"] += float(trade_cost.get("total_cost") or 0.0)

        by_market_route: dict[str, dict[str, Any]] = {}
        by_route: dict[str, dict[str, Any]] = {}
        by_market_signal: dict[str, dict[str, Any]] = {}
        for trade in self._trades:
            route = str(trade.get("source_route") or "no_entry_route")
            market_signal = str(trade.get("market_signal") or "UNKNOWN")
            add_trade(by_market_route, f"{market_signal}:{route}", trade)
            add_trade(by_route, route, trade)
            add_trade(by_market_signal, market_signal, trade)

        def finalize(source: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
            rows: list[tuple[str, dict[str, Any]]] = []
            for key, bucket in source.items():
                sell_trades = int(bucket["sell_trades"])
                rows.append((
                    key,
                    {
                        "buy_trades": int(bucket["buy_trades"]),
                        "scale_in_trades": int(bucket["scale_in_trades"]),
                        "sell_trades": sell_trades,
                        "winning_sells": int(bucket["winning_sells"]),
                        "losing_sells": int(bucket["losing_sells"]),
                        "win_rate_pct": round(bucket["winning_sells"] / sell_trades * 100, 2)
                        if sell_trades else 0.0,
                        "avg_return_pct": round(bucket["return_pct_sum"] / sell_trades, 2)
                        if sell_trades else 0.0,
                        "pnl": round(float(bucket["pnl"]), 2),
                        "trade_cost": round(float(bucket["trade_cost"]), 2),
                    },
                ))
            rows.sort(key=lambda item: (item[1]["pnl"], item[1]["sell_trades"]), reverse=True)
            return {key: value for key, value in rows}

        return {
            "by_market_route": finalize(by_market_route),
            "by_route": finalize(by_route),
            "by_market_signal": finalize(by_market_signal),
            "notes": {
                "sell_trades": "胜率、平均收益和 PnL 只统计已平仓卖出交易。",
                "buy_trades": "买入和加仓仅用于频次与成本归因，未平仓浮动收益不计入路线胜率。",
            },
        }

    def _discovery_reachability_summary(self) -> dict[str, Any]:
        checks = int(self._discovery_reachability.get("candidate_checks") or 0)
        reachable = int(self._discovery_reachability.get("reachable_candidates") or 0)
        blocked = int(self._discovery_reachability.get("blocked_candidates") or 0)
        return {
            "enabled": bool(self.cfg.require_reachable_candidate_for_buy),
            "lookback_days": int(self.cfg.reachable_lookback_days or 0),
            "candidate_checks": checks,
            "reachable_candidates": reachable,
            "blocked_candidates": blocked,
            "reachable_buy_rate_pct": round(reachable / checks * 100, 2) if checks else 0.0,
            "discovery_sources": dict(self._discovery_reachability.get("discovery_sources") or {}),
            "blocked_reasons": dict(self._discovery_reachability.get("blocked_reasons") or {}),
            "blocked_codes": dict(self._discovery_reachability.get("blocked_codes") or {}),
        }


# ---------------------------------------------------------------------------
# 工厂函数（MCP/CLI 直接调用）
# ---------------------------------------------------------------------------

def load_config(preset_name: str) -> BacktestConfig:
    """从 strategy.yaml 加载 preset 配置。"""
    cfg_path = Path(__file__).parent.parent.parent.parent / "config" / "strategy.yaml"
    presets = {}
    weights = {"technical": 3.0, "fundamental": 2.0, "flow": 2.0, "sentiment": 3.0}
    veto_rules = ["below_ma20", "limit_up_today", "consecutive_outflow", "red_market", "ma20_trend_down"]
    decision_gates: dict[str, Any] = {}
    market_regime_overlays: dict[str, Any] = {}
    score_adjustments: dict[str, Any] = {}

    if cfg_path.exists():
        with open(cfg_path) as f:
            full = yaml.safe_load(f) or {}
        presets = full.get("backtest_presets", {})
        sc = full.get("scoring", {})
        weights_cfg = sc.get("weights", {})
        if weights_cfg:
            weights = weights_cfg
        veto_rules = sc.get("veto", veto_rules)
        decision_gates = sc.get("decision_gates", {})
        market_regime_overlays = sc.get("market_regime_overlays", {})
        score_adjustments = sc.get("score_adjustments", {})

    p = presets.get(preset_name, presets.get("保守验证C", {}))

    def _preset_tuple(key: str) -> tuple[str, ...]:
        value = p.get(key, ())
        if isinstance(value, str):
            value = [part.strip() for part in value.split(",")]
        return tuple(str(item) for item in (value or ()) if str(item))

    def _merge_mapping_by_key(base: dict[str, Any], override: Any) -> dict[str, Any]:
        if not isinstance(override, dict):
            return dict(base or {})
        merged: dict[str, Any] = {
            str(key): dict(value) if isinstance(value, dict) else value
            for key, value in (base or {}).items()
        }
        for key, value in override.items():
            key_str = str(key)
            if isinstance(value, dict) and isinstance(merged.get(key_str), dict):
                item = dict(merged[key_str])
                item.update(value)
                merged[key_str] = item
            else:
                merged[key_str] = value
        return merged

    # 融合 preset 和评分配置
    return BacktestConfig(
        preset_name=preset_name,
        trailing_stop=p.get("momentum_trailing_stop", 0.10),
        stop_loss=p.get("momentum_stop_loss", 0.08),
        time_stop_days=p.get("momentum_time_stop_days", 15),
        buy_threshold=p.get("buy_threshold", 6.5),
        single_max_pct=p.get("single_max_pct", 0.20),
        total_max_pct=p.get("total_max_pct", 0.60),
        weekly_max=p.get("weekly_max", 2),
        weekly_max_by_market=p.get("weekly_max_by_market", {}),
        daily_max_buys=p.get("daily_max_buys", 2),
        holding_max=p.get("holding_max", 5),
        route_execution_policy=p.get("route_execution_policy", {}),
        watch_loss_cooldown_days=int(p.get("watch_loss_cooldown_days", 0) or 0),
        watch_loss_cooldown_phase_buckets=_preset_tuple("watch_loss_cooldown_phases"),
        execute_buy_phase_buckets=_preset_tuple("execute_buy_phases"),
        execute_trial_buy_routes=_preset_tuple("execute_trial_buy_routes"),
        execute_watch_trial_market_signals=_preset_tuple("execute_watch_trial_markets"),
        execute_watch_trial_routes=_preset_tuple("execute_watch_trial_routes"),
        execute_watch_trial_pairs=_preset_tuple("execute_watch_trial_pairs"),
        execute_watch_trial_score_min=float(p.get("execute_watch_trial_score_min", 6.0) or 0.0),
        execute_watch_trial_score_max=(
            None
            if p.get("execute_watch_trial_score_max") is None
            else float(p.get("execute_watch_trial_score_max"))
        ),
        execute_watch_trial_position_pct=(
            None
            if p.get("execute_watch_trial_position_pct") is None
            else float(p.get("execute_watch_trial_position_pct"))
        ),
        execute_watch_trial_phase_buckets=_preset_tuple("execute_watch_trial_phases"),
        execute_watch_trial_min_above_ma20_days=int(
            p.get("execute_watch_trial_min_above_ma20_days", 0) or 0
        ),
        execute_watch_trial_min_above_ma20_days_phase_buckets=_preset_tuple(
            "execute_watch_trial_min_above_ma20_days_phases"
        ),
        execute_watch_trial_require_above_ma60_phase_buckets=_preset_tuple(
            "execute_watch_trial_require_above_ma60_phases"
        ),
        execute_watch_trial_require_above_ma120_phase_buckets=_preset_tuple(
            "execute_watch_trial_require_above_ma120_phases"
        ),
        scale_in_enabled=bool(p.get("scale_in_enabled", False)),
        scale_in_profit_threshold=float(p.get("scale_in_profit_threshold", 0.10) or 0.0),
        scale_in_step_position_pct=float(p.get("scale_in_step_position_pct", 0.075) or 0.0),
        scale_in_max_position_pct=(
            None
            if p.get("scale_in_max_position_pct") is None
            else float(p.get("scale_in_max_position_pct"))
        ),
        scale_in_max_adds=int(p.get("scale_in_max_adds", 2) or 0),
        scale_in_min_days_between=int(p.get("scale_in_min_days_between", 5) or 0),
        scale_in_routes=_preset_tuple("scale_in_routes"),
        scale_in_market_signals=_preset_tuple("scale_in_markets"),
        scale_in_actions=_preset_tuple("scale_in_actions") or ("BUY", "WATCH"),
        scale_in_require_entry_signal=bool(p.get("scale_in_require_entry_signal", True)),
        scale_in_score_min=float(p.get("scale_in_score_min", 5.0) or 0.0),
        scale_in_reset_time_stop=bool(p.get("scale_in_reset_time_stop", True)),
        scale_in_aggressive_max_position_pct=(
            None
            if p.get("scale_in_aggressive_max_position_pct") is None
            else float(p.get("scale_in_aggressive_max_position_pct"))
        ),
        scale_in_aggressive_step_position_pct=(
            None
            if p.get("scale_in_aggressive_step_position_pct") is None
            else float(p.get("scale_in_aggressive_step_position_pct"))
        ),
        scale_in_aggressive_market_signals=_preset_tuple("scale_in_aggressive_markets"),
        scale_in_aggressive_routes=_preset_tuple("scale_in_aggressive_routes"),
        scale_in_aggressive_phase_buckets=_preset_tuple("scale_in_aggressive_phases"),
        weights=weights,
        veto_rules=veto_rules,
        decision_gates=decision_gates,
        market_regime_overlays=_merge_mapping_by_key(
            market_regime_overlays,
            p.get("market_regime_overlays"),
        ),
        score_adjustments=score_adjustments,
        market_multipliers=p.get("market_multipliers", {}),
        pnl_adjustflag=str(p.get("pnl_adjustflag", "1") or "1"),
        commission_bps=float(p.get("commission_bps", 2.5) or 0.0),
        min_commission=float(p.get("min_commission", 5.0) or 0.0),
        stamp_tax_bps=float(p.get("stamp_tax_bps", 5.0) or 0.0),
        transfer_fee_bps=float(p.get("transfer_fee_bps", 0.1) or 0.0),
        slippage_bps=float(p.get("slippage_bps", 5.0) or 0.0),
    )


def run_backtest(
    codes: str,
    start: str,
    end: str,
    preset: str = "保守验证C",
    initial_cash: float = 100000.0,
    adjustflag: str = "2",
    pnl_adjustflag: str = "1",
    use_history_mirror: bool = True,
    red_multiplier: float | None = None,
    disable_market_reduce_sell: bool = False,
    execute_red_trial_buy: bool = False,
    execute_trial_buy_routes: tuple[str, ...] | list[str] | None = None,
    execute_buy_phases: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_markets: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_routes: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_pairs: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_score_min: float | None = None,
    execute_watch_trial_score_max: float | None = None,
    execute_watch_trial_position_pct: float | None = None,
    execute_watch_trial_phases: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_min_above_ma20_days: int | None = None,
    execute_watch_trial_min_above_ma20_days_phases: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_require_above_ma60_phases: tuple[str, ...] | list[str] | None = None,
    execute_watch_trial_require_above_ma120_phases: tuple[str, ...] | list[str] | None = None,
    holding_max: int | None = None,
    trailing_stop: float | None = None,
    time_stop_days: int | None = None,
    stop_loss: float | None = None,
    watch_loss_cooldown_days: int | None = None,
    watch_loss_cooldown_phases: tuple[str, ...] | list[str] | None = None,
    scale_in_enabled: bool | None = None,
    scale_in_profit_threshold: float | None = None,
    scale_in_step_position_pct: float | None = None,
    scale_in_max_position_pct: float | None = None,
    scale_in_max_adds: int | None = None,
    scale_in_min_days_between: int | None = None,
    scale_in_routes: tuple[str, ...] | list[str] | None = None,
    scale_in_market_signals: tuple[str, ...] | list[str] | None = None,
    scale_in_actions: tuple[str, ...] | list[str] | None = None,
    scale_in_require_entry_signal: bool | None = None,
    scale_in_score_min: float | None = None,
    scale_in_reset_time_stop: bool | None = None,
    scale_in_aggressive_max_position_pct: float | None = None,
    scale_in_aggressive_step_position_pct: float | None = None,
    scale_in_aggressive_market_signals: tuple[str, ...] | list[str] | None = None,
    scale_in_aggressive_routes: tuple[str, ...] | list[str] | None = None,
    scale_in_aggressive_phase_buckets: tuple[str, ...] | list[str] | None = None,
    trade_record_limit: int | None = 50,
    signal_record_limit: int | None = 50,
    signal_slices: tuple[str, ...] | list[str] | None = None,
    include_signal_alpha: bool = True,
    load_financials: bool = True,
    reachable_only: bool = False,
    reachable_lookback_days: int = 5,
    progress_log: bool = False,
    use_stored_data: bool = False,
    hydrate_data: bool = False,
    use_market_bars: bool = False,
    hydrate_market_bars: bool = False,
    score_dimension_mode: str = "full",
    commission_bps: float | None = None,
    min_commission: float | None = None,
    stamp_tax_bps: float | None = None,
    transfer_fee_bps: float | None = None,
    slippage_bps: float | None = None,
) -> dict:
    """执行回测的主入口函数（MCP 和 CLI 共用）。

    Returns:
        回测报告 dict（包含 trades, equity_curve, metrics）
    """
    code_list = [c.strip() for c in codes.split(",") if c.strip()]
    if not code_list:
        return {"error": "股票代码列表为空"}

    # 加载配置
    cfg = load_config(preset)
    cfg.initial_cash = initial_cash
    cfg.adjustflag = adjustflag
    cfg.pnl_adjustflag = str(pnl_adjustflag or cfg.pnl_adjustflag or "1")
    if trailing_stop is not None:
        cfg.trailing_stop = float(trailing_stop)
    if time_stop_days is not None:
        cfg.time_stop_days = int(time_stop_days)
    if stop_loss is not None:
        cfg.stop_loss = float(stop_loss)
    if watch_loss_cooldown_days is not None:
        cfg.watch_loss_cooldown_days = int(watch_loss_cooldown_days)
    if watch_loss_cooldown_phases is not None:
        cfg.watch_loss_cooldown_phase_buckets = tuple(
            str(item) for item in watch_loss_cooldown_phases if str(item)
        )
    if scale_in_enabled is not None:
        cfg.scale_in_enabled = bool(scale_in_enabled)
    if scale_in_profit_threshold is not None:
        cfg.scale_in_profit_threshold = float(scale_in_profit_threshold)
    if scale_in_step_position_pct is not None:
        cfg.scale_in_step_position_pct = float(scale_in_step_position_pct)
    if scale_in_max_position_pct is not None:
        cfg.scale_in_max_position_pct = float(scale_in_max_position_pct)
    if scale_in_max_adds is not None:
        cfg.scale_in_max_adds = int(scale_in_max_adds)
    if scale_in_min_days_between is not None:
        cfg.scale_in_min_days_between = int(scale_in_min_days_between)
    if scale_in_routes is not None:
        cfg.scale_in_routes = tuple(str(item) for item in scale_in_routes if str(item))
    if scale_in_market_signals is not None:
        cfg.scale_in_market_signals = tuple(str(item) for item in scale_in_market_signals if str(item))
    if scale_in_actions is not None:
        cfg.scale_in_actions = tuple(str(item) for item in scale_in_actions if str(item))
    if scale_in_require_entry_signal is not None:
        cfg.scale_in_require_entry_signal = bool(scale_in_require_entry_signal)
    if scale_in_score_min is not None:
        cfg.scale_in_score_min = float(scale_in_score_min)
    if scale_in_reset_time_stop is not None:
        cfg.scale_in_reset_time_stop = bool(scale_in_reset_time_stop)
    if scale_in_aggressive_max_position_pct is not None:
        cfg.scale_in_aggressive_max_position_pct = float(scale_in_aggressive_max_position_pct)
    if scale_in_aggressive_step_position_pct is not None:
        cfg.scale_in_aggressive_step_position_pct = float(scale_in_aggressive_step_position_pct)
    if scale_in_aggressive_market_signals is not None:
        cfg.scale_in_aggressive_market_signals = tuple(
            str(item) for item in scale_in_aggressive_market_signals if str(item)
        )
    if scale_in_aggressive_routes is not None:
        cfg.scale_in_aggressive_routes = tuple(str(item) for item in scale_in_aggressive_routes if str(item))
    if scale_in_aggressive_phase_buckets is not None:
        cfg.scale_in_aggressive_phase_buckets = tuple(
            str(item) for item in scale_in_aggressive_phase_buckets if str(item)
        )
    if holding_max is not None:
        cfg.holding_max = int(holding_max)
    if red_multiplier is not None:
        cfg.market_multipliers = {**cfg.market_multipliers, "RED": red_multiplier}
    cfg.disable_market_reduce_sell = bool(disable_market_reduce_sell)
    if execute_red_trial_buy:
        cfg.execute_trial_buy_market_signals = tuple(
            dict.fromkeys([*cfg.execute_trial_buy_market_signals, "RED"])
        )
    if execute_trial_buy_routes is not None:
        cfg.execute_trial_buy_routes = tuple(str(item) for item in execute_trial_buy_routes if str(item))
    if execute_buy_phases is not None:
        cfg.execute_buy_phase_buckets = tuple(str(item) for item in execute_buy_phases if str(item))
    if execute_watch_trial_markets is not None:
        cfg.execute_watch_trial_market_signals = tuple(
            str(item) for item in execute_watch_trial_markets if str(item)
        )
    if execute_watch_trial_routes is not None:
        cfg.execute_watch_trial_routes = tuple(str(item) for item in execute_watch_trial_routes if str(item))
    if execute_watch_trial_pairs is not None:
        cfg.execute_watch_trial_pairs = tuple(str(item) for item in execute_watch_trial_pairs if str(item))
    if execute_watch_trial_phases is not None:
        cfg.execute_watch_trial_phase_buckets = tuple(
            str(item) for item in execute_watch_trial_phases if str(item)
        )
    if execute_watch_trial_min_above_ma20_days is not None:
        cfg.execute_watch_trial_min_above_ma20_days = int(execute_watch_trial_min_above_ma20_days or 0)
    if execute_watch_trial_min_above_ma20_days_phases is not None:
        cfg.execute_watch_trial_min_above_ma20_days_phase_buckets = tuple(
            str(item) for item in execute_watch_trial_min_above_ma20_days_phases if str(item)
        )
    if execute_watch_trial_require_above_ma60_phases is not None:
        cfg.execute_watch_trial_require_above_ma60_phase_buckets = tuple(
            str(item) for item in execute_watch_trial_require_above_ma60_phases if str(item)
        )
    if execute_watch_trial_require_above_ma120_phases is not None:
        cfg.execute_watch_trial_require_above_ma120_phase_buckets = tuple(
            str(item) for item in execute_watch_trial_require_above_ma120_phases if str(item)
        )
    if execute_watch_trial_score_min is not None:
        cfg.execute_watch_trial_score_min = float(execute_watch_trial_score_min or 0.0)
    if execute_watch_trial_score_max is not None:
        cfg.execute_watch_trial_score_max = float(execute_watch_trial_score_max)
    if execute_watch_trial_position_pct is not None:
        cfg.execute_watch_trial_position_pct = float(execute_watch_trial_position_pct)
    cfg.trade_record_limit = trade_record_limit
    cfg.signal_record_limit = signal_record_limit
    if signal_slices is not None:
        cfg.signal_slices = tuple(str(item) for item in signal_slices if str(item))
    cfg.include_signal_alpha = bool(include_signal_alpha)
    cfg.score_dimension_mode = score_dimension_mode
    if commission_bps is not None:
        cfg.commission_bps = float(commission_bps)
    if min_commission is not None:
        cfg.min_commission = float(min_commission)
    if stamp_tax_bps is not None:
        cfg.stamp_tax_bps = float(stamp_tax_bps)
    if transfer_fee_bps is not None:
        cfg.transfer_fee_bps = float(transfer_fee_bps)
    if slippage_bps is not None:
        cfg.slippage_bps = float(slippage_bps)
    cfg.load_financials = load_financials
    cfg.require_reachable_candidate_for_buy = bool(reachable_only)
    cfg.reachable_lookback_days = max(int(reachable_lookback_days or 0), 0)
    cfg.progress_log = progress_log
    cfg.use_market_bars = use_market_bars or use_stored_data or hydrate_data
    cfg.hydrate_market_bars = hydrate_market_bars or hydrate_data
    cfg.use_financial_cache = use_stored_data or hydrate_data
    cfg.hydrate_financial_cache = hydrate_data

    history_conn = _open_history_connection(use_history_mirror)
    market_conn = _open_market_data_connection(
        use_market_bars=cfg.use_market_bars,
        hydrate_market_bars=cfg.hydrate_market_bars,
        use_financial_cache=cfg.use_financial_cache,
        hydrate_financial_cache=cfg.hydrate_financial_cache,
    )
    # 初始化引擎
    engine = BacktestEngine(cfg, history_conn=history_conn, market_conn=market_conn)
    try:
        # 向前多拉 260 天，支撑 MA120 与 MA120 斜率等长周期市场制度字段。
        from datetime import date as date_type, timedelta as td
        pre_start = (date_type.fromisoformat(start) - td(days=260)).isoformat()

        load_result = engine.load_data(code_list, start, end, pre_start)
        if "error" in load_result:
            return load_result

        return engine.run()
    finally:
        if history_conn is not None:
            history_conn.close()
        if market_conn is not None:
            market_conn.close()


def _open_history_connection(use_history_mirror: bool):
    if not use_history_mirror:
        return None
    try:
        from astock_trading.platform.db import connect

        return connect()
    except Exception:
        return None


def _open_market_data_connection(
    *,
    use_market_bars: bool,
    hydrate_market_bars: bool,
    use_financial_cache: bool,
    hydrate_financial_cache: bool,
):
    if not any((
        use_market_bars,
        hydrate_market_bars,
        use_financial_cache,
        hydrate_financial_cache,
    )):
        return None
    from astock_trading.platform.db import connect

    return connect()
